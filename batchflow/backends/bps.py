"""
backends/bps.py — SubmissionBackend ABC and concrete implementations.

BpsBackend    — shells out to ``bps submit``, parses the cluster ID,
                and returns it.  This replaces the subprocess logic
                scattered across EoPipelines and CpPipelines.

MockBackend   — in-process fake for unit tests; never touches HTCondor.
"""
from __future__ import annotations

import asyncio
import logging
import re
import subprocess
from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path

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
    cluster_id : str
        The HTCondor cluster ID, e.g. ``"12345"``.
    schedd_name : str
        FQDN of the schedd that accepted the submission (e.g.
        ``"sdfiana032.sdf.slac.stanford.edu"``).  Empty string when
        the schedd name could not be determined.
    """
    cluster_id:  str
    schedd_name: str


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
        bps_yaml: str,
        bps_dir: Path,
        overrides: dict[str, str] | None = None,
        log_dir: Path | None = None,
    ) -> SubmissionResult:
        """
        Submit the pipeline described by *bps_yaml*.

        Parameters
        ----------
        bps_yaml : str
            Filename within *bps_dir*, e.g. ``"bps_eoReadNoise.yaml"``.
        bps_dir : Path
            Directory that contains the BPS YAML files (the local ./bps/).
        overrides : dict | None
            Optional key=value pairs forwarded to bps as
            ``--override key=value`` arguments.
        log_dir : Path | None
            If given, stdout+stderr of bps submit are captured here.

        Returns
        -------
        SubmissionResult
            Dataclass with cluster_id and schedd_name.
        """

    @abstractmethod
    async def release_held(self, cluster_id: str) -> None:
        """Run ``condor_release <cluster_id>`` to un-hold held jobs."""

    @abstractmethod
    async def remove(self, cluster_id: str) -> None:
        """Run ``condor_rm <cluster_id>`` to abort all jobs in the cluster."""


# ---------------------------------------------------------------------------
# BPS implementation
# ---------------------------------------------------------------------------

class BpsBackend(SubmissionBackend):
    """
    Submits via ``bps submit`` and parses the cluster / submit-dir from
    its stdout.

    BPS failures are detected by checking for ``RuntimeError`` in the
    captured log (the same approach as the original code, since piping
    through ``tee`` swallows the non-zero exit code).  Once BPS exposes
    a machine-readable exit code this check can be simplified.
    """

    async def submit(
        self,
        bps_yaml: str,
        bps_dir: Path,
        overrides: dict[str, str] | None = None,
        log_dir: Path | None = None,
    ) -> SubmissionResult:
        yaml_path = bps_dir / bps_yaml
        if not yaml_path.exists():
            raise FileNotFoundError(f"BPS YAML not found: {yaml_path}")

        cmd = ["bps", "submit", str(yaml_path)]
        for key, val in (overrides or {}).items():
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

        cluster_id = self._parse_cluster_id(stdout)

        # Capture the local schedd FQDN so the monitor can reconnect to it
        # when running on a different node.  The FQDN lives in the alias
        # field of the DaemonLocation address string.
        try:
            import htcondor
            m = re.search(r'alias=([^>&]+)', htcondor.Schedd().location.address)
            schedd_name = m.group(1) if m else ""
        except Exception:
            schedd_name = ""

        log.info("Submitted %s → cluster %s on schedd %s",
                 bps_yaml, cluster_id, schedd_name)
        return SubmissionResult(cluster_id=cluster_id, schedd_name=schedd_name)

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
            Run Id: 20240101T120000Z
            HTCondor cluster id: 12345
        We try a few patterns in order of specificity.
        """
        patterns = [
            r"HTCondor cluster id:\s*(\d+)",
            r"cluster[_\s]id[:\s]+(\d+)",
            r"Cluster\s+(\d+)\s+submitted",
        ]
        for pat in patterns:
            m = re.search(pat, output, re.IGNORECASE)
            if m:
                return m.group(1)
        raise RuntimeError(
            "Could not parse HTCondor cluster ID from bps output.\n"
            "Output was:\n" + output[-1000:]
        )

    async def release_held(self, cluster_id: str) -> None:
        await self._run_condor(["condor_release", cluster_id])

    async def remove(self, cluster_id: str) -> None:
        await self._run_condor(["condor_rm", cluster_id])

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
        bps_yaml: str,
        bps_dir: Path,
        overrides: dict[str, str] | None = None,
        log_dir: Path | None = None,
    ) -> SubmissionResult:
        cluster_id = str(self._next_id)
        self._next_id += 1
        record = {
            "cluster_id":  cluster_id,
            "schedd_name": "mock-schedd",
            "bps_yaml":    bps_yaml,
            "overrides":   overrides or {},
        }
        self.submitted.append(record)
        log.info("MockBackend: submit %s → cluster %s", bps_yaml, cluster_id)
        return SubmissionResult(cluster_id=cluster_id, schedd_name="mock-schedd")

    async def release_held(self, cluster_id: str) -> None:
        log.info("MockBackend: release_held %s", cluster_id)

    async def remove(self, cluster_id: str) -> None:
        log.info("MockBackend: remove %s", cluster_id)
