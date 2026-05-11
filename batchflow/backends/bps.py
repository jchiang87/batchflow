"""
backends/bps.py — SubmissionBackend ABC and concrete implementations.

BpsHtcondorBackend — shells out to ``bps submit`` (HTCondor plugin),
                     parses the cluster ID, and returns it.

MockBackend        — in-process fake for unit tests.
"""
from __future__ import annotations

import asyncio
import logging
import re
import subprocess
from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ..graph import PipelineNode

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Submission result
# ---------------------------------------------------------------------------

@dataclass
class SubmissionResult:
    """
    Returned by SubmissionBackend.submit().

    Attributes
    ----------
    submit_id : str
        Opaque identifier for the submitted job (e.g. an HTCondor cluster
        ID or a UUID).  Used by the backend to reconnect on resume/restart.
    submit_location : str
        Backend-specific location hint (e.g. schedd FQDN for HTCondor,
        ``"parsl"`` for Parsl).  Empty string when not applicable.
    """
    submit_id:       str
    submit_location: str


# ---------------------------------------------------------------------------
# Abstract interface
# ---------------------------------------------------------------------------

class SubmissionBackend(ABC):
    """
    Submits a single pipeline node and returns a SubmissionResult.
    """

    @abstractmethod
    async def submit(
        self,
        node: "PipelineNode",
        *,
        log_dir: Path | None = None,
    ) -> SubmissionResult:
        """
        Submit the job described by *node*.

        Parameters
        ----------
        node : PipelineNode
            The node to submit.  Backends extract whatever they need
            (e.g. ``node.bps_yaml`` / ``node.overrides`` for BPS backends,
            ``node.command`` for shell backends).
        log_dir : Path | None
            If given, stdout+stderr are captured here.

        Returns
        -------
        SubmissionResult
            Dataclass with submit_id and submit_location.
        """

    @abstractmethod
    async def release_held(self, submit_id: str) -> None:
        """Release a paused/held job identified by *submit_id*."""

    @abstractmethod
    async def remove(self, submit_id: str) -> None:
        """Abort all jobs associated with *submit_id*."""

    @abstractmethod
    async def restart(self, submit_id: str) -> SubmissionResult:
        """Restart a failed/held job and return the new SubmissionResult."""


# ---------------------------------------------------------------------------
# BPS implementation
# ---------------------------------------------------------------------------

class BpsHtcondorBackend(SubmissionBackend):
    """
    Submits via ``bps submit`` (HTCondor plugin) and parses the cluster ID
    from its stdout.

    BPS failures are detected by checking for ``RuntimeError`` in the
    captured log (the same approach as the original code, since piping
    through ``tee`` swallows the non-zero exit code).  Once BPS exposes
    a machine-readable exit code this check can be simplified.

    Parameters
    ----------
    bps_dir : Path
        Directory that contains the BPS YAML files (the local ./bps/).
    """

    def __init__(self, bps_dir: Path) -> None:
        self._bps_dir = bps_dir

    async def submit(
        self,
        node: "PipelineNode",
        *,
        log_dir: Path | None = None,
    ) -> SubmissionResult:
        bps_yaml = node.bps_yaml
        if not bps_yaml:
            raise ValueError(f"Node {node.node_id!r} has no bps_yaml set")
        yaml_path = self._bps_dir / bps_yaml
        if not yaml_path.exists():
            raise FileNotFoundError(f"BPS YAML not found: {yaml_path}")

        cmd = ["bps", "submit", str(yaml_path)]
        for key, val in (node.overrides or {}).items():
            cmd += ["--override", f"{key}={val}"]

        log.info("Submitting: %s", " ".join(cmd))

        loop = asyncio.get_running_loop()
        stdout, stderr = await loop.run_in_executor(
            None, self._run_bps, cmd
        )

        if log_dir:
            log_dir.mkdir(parents=True, exist_ok=True)
            log_file = log_dir / bps_yaml.replace(".yaml", ".log")
            log_file.write_text(stdout + stderr)

        # Detect BPS-level failures (exit code is unreliable through tee).
        if "RuntimeError" in stdout or "RuntimeError" in stderr:
            raise RuntimeError(
                f"bps submit reported RuntimeError for {bps_yaml}:\n"
                + stdout[-2000:]  # tail to keep logs manageable
            )

        submit_id = self._parse_cluster_id(stdout)

        # Capture the local schedd FQDN so the monitor can reconnect to it
        # when running on a different node.  The FQDN lives in the alias
        # field of the DaemonLocation address string.
        try:
            import htcondor
            m = re.search(r'alias=([^>&]+)', htcondor.Schedd().location.address)
            submit_location = m.group(1) if m else ""
        except Exception:
            submit_location = ""

        log.info("Submitted %s → cluster %s on schedd %s",
                 bps_yaml, submit_id, submit_location)
        return SubmissionResult(submit_id=submit_id, submit_location=submit_location)

    @staticmethod
    def _run_bps(cmd: list[str]) -> tuple[str, str]:
        result = subprocess.run(
            cmd, capture_output=True, text=True
        )
        return result.stdout, result.stderr

    @staticmethod
    def _parse_cluster_id(output: str) -> str:
        """
        Extract the HTCondor cluster ID from bps submit stdout.

        bps typically prints lines like:
            Submit dir: /path/to/submit/dir
            Run Id: 31931.0
            Run Name: u_operator_payload_name_timestamp
        We try a few patterns in order of specificity.
        """
        patterns = [
            r"Run Id:\s*(\d+)",
        ]
        for pat in patterns:
            m = re.search(pat, output, re.IGNORECASE)
            if m:
                return m.group(1)
        raise RuntimeError(
            "Could not parse HTCondor cluster ID from bps output.\n"
            "Output was:\n" + output[-1000:]
        )

    async def release_held(self, submit_id: str) -> None:
        await self._run_condor(["condor_release", submit_id])

    async def remove(self, submit_id: str) -> None:
        await self._run_condor(["condor_rm", submit_id])

    async def restart(self, submit_id: str) -> SubmissionResult:
        cmd = ["bps", "restart", "--id", submit_id]
        log.info("Restarting: %s", " ".join(cmd))
        loop = asyncio.get_running_loop()
        stdout, stderr = await loop.run_in_executor(None, self._run_bps, cmd)
        if "RuntimeError" in stdout or "RuntimeError" in stderr:
            raise RuntimeError(
                f"bps restart reported RuntimeError for {submit_id}:\n"
                + stdout[-2000:]
            )
        new_submit_id = self._parse_cluster_id(stdout)
        try:
            import htcondor
            m = re.search(r'alias=([^>&]+)', htcondor.Schedd().location.address)
            submit_location = m.group(1) if m else ""
        except Exception:
            submit_location = ""
        log.info("Restarted %s → new cluster %s on schedd %s",
                 submit_id, new_submit_id, submit_location)
        return SubmissionResult(submit_id=new_submit_id, submit_location=submit_location)

    @staticmethod
    async def _run_condor(cmd: list[str]) -> None:
        loop = asyncio.get_running_loop()
        def _exec():
            result = subprocess.run(cmd, capture_output=True, text=True)
            if result.returncode != 0:
                raise subprocess.CalledProcessError(
                    result.returncode, cmd, result.stdout, result.stderr
                )
        await loop.run_in_executor(None, _exec)


# ---------------------------------------------------------------------------
# Mock implementation — for tests and dry-runs
# ---------------------------------------------------------------------------

BpsBackend = BpsHtcondorBackend  # backward-compatibility alias


class MockBackend(SubmissionBackend):
    """
    In-process backend that never touches HTCondor.

    Each submit() call returns a synthetic cluster ID and records the
    call for later inspection.  Useful for unit tests and ``--dry-run``.
    """

    def __init__(self) -> None:
        self._next_id = 1000
        self.submitted: list[dict] = []

    async def submit(
        self,
        node: "PipelineNode",
        *,
        log_dir: Path | None = None,
    ) -> SubmissionResult:
        submit_id = str(self._next_id)
        self._next_id += 1
        record = {
            "submit_id":       submit_id,
            "submit_location": "mock-schedd",
            "node_id":         node.node_id,
            "node_type":       node.node_type,
            "overrides":       node.overrides,
        }
        self.submitted.append(record)
        log.info("MockBackend: submit %r → %s", node.node_id, submit_id)
        return SubmissionResult(submit_id=submit_id, submit_location="mock-schedd")

    async def release_held(self, submit_id: str) -> None:
        log.info("MockBackend: release_held %s", submit_id)

    async def remove(self, submit_id: str) -> None:
        log.info("MockBackend: remove %s", submit_id)

    async def restart(self, submit_id: str) -> SubmissionResult:
        new_submit_id = str(self._next_id)
        self._next_id += 1
        record = {
            "submit_id":       new_submit_id,
            "submit_location": "mock-schedd",
            "restarted_from":  submit_id,
        }
        self.submitted.append(record)
        log.info("MockBackend: restart %s → %s", submit_id, new_submit_id)
        return SubmissionResult(submit_id=new_submit_id, submit_location="mock-schedd")
