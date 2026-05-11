"""
backends/bps_parsl.py — BpsParslBackend + BlockingNodeRunner

BpsParslBackend wraps `bps submit` for the Parsl plugin, which blocks until
the workflow completes.  Each submit spawns an async subprocess and returns
a UUID placeholder as the submit_id.  The real BPS run_id is parsed from the
output on completion and backfilled into the node via the cluster_id field
of the NODE_COMPLETE / NODE_FAILED event.
"""
from __future__ import annotations

import asyncio
import logging
import re
import uuid
from pathlib import Path
from typing import TYPE_CHECKING

from .bps import SubmissionBackend, SubmissionResult
from ..bus import EventBus, EventType, JobEvent
from ..monitor import AbstractNodeRunner

if TYPE_CHECKING:
    from ..graph import PipelineNode

log = logging.getLogger(__name__)


def _parse_run_id(output: str) -> str | None:
    m = re.search(r"Run Id:\s*(\S+)", output, re.IGNORECASE)
    return m.group(1) if m else None


async def _terminate_proc(proc: asyncio.subprocess.Process) -> None:
    """SIGTERM, then SIGKILL after 10 s if still alive."""
    try:
        proc.terminate()
        await asyncio.wait_for(proc.wait(), timeout=10.0)
    except (asyncio.TimeoutError, ProcessLookupError):
        try:
            proc.kill()
        except ProcessLookupError:
            pass


class BlockingNodeRunner(AbstractNodeRunner):
    """
    Awaits a blocking subprocess (bps submit with Parsl plugin) and
    publishes the terminal node event when it finishes.
    """

    def __init__(
        self,
        proc:        asyncio.subprocess.Process,
        workflow_id: str,
        node_id:     str,
        bus:         EventBus,
        node_label:  str,
        log_dir:     Path | None = None,
    ) -> None:
        self._proc        = proc
        self._workflow_id = workflow_id
        self._node_id     = node_id
        self._bus         = bus
        self._node_label  = node_label
        self._log_dir     = log_dir

    async def run(self) -> None:
        log.info(
            "BlockingNodeRunner[%s/%s]: waiting for subprocess (pid=%s)",
            self._workflow_id, self._node_id, self._proc.pid,
        )
        await self._bus.publish(JobEvent(
            event_type  = EventType.JOB_RUNNING,
            workflow_id = self._workflow_id,
            node_id     = self._node_id,
        ))

        try:
            stdout_b, stderr_b = await self._proc.communicate()
        except asyncio.CancelledError:
            log.warning(
                "BlockingNodeRunner[%s/%s]: cancelled — terminating subprocess",
                self._workflow_id, self._node_id,
            )
            await _terminate_proc(self._proc)
            raise

        rc       = self._proc.returncode
        stdout   = stdout_b.decode(errors="replace") if stdout_b else ""
        stderr   = stderr_b.decode(errors="replace") if stderr_b else ""
        combined = stdout + stderr

        if self._log_dir:
            self._log_dir.mkdir(parents=True, exist_ok=True)
            (self._log_dir / f"{self._node_label}.log").write_text(combined)

        run_id = _parse_run_id(combined) or ""

        if rc == 0 and "RuntimeError" not in combined:
            log.info(
                "BlockingNodeRunner[%s/%s]: succeeded (run_id=%s)",
                self._workflow_id, self._node_id, run_id,
            )
            await self._bus.publish(JobEvent(
                event_type  = EventType.NODE_COMPLETE,
                workflow_id = self._workflow_id,
                node_id     = self._node_id,
                cluster_id  = run_id,
            ))
        else:
            hold_reasons = (f"bps RuntimeError (exit {rc})",) if "RuntimeError" in combined else ()
            log.warning(
                "BlockingNodeRunner[%s/%s]: failed (rc=%s, run_id=%s)",
                self._workflow_id, self._node_id, rc, run_id,
            )
            await self._bus.publish(JobEvent(
                event_type   = EventType.NODE_FAILED,
                workflow_id  = self._workflow_id,
                node_id      = self._node_id,
                cluster_id   = run_id,
                exit_code    = rc,
                hold_reasons = hold_reasons,
            ))


class BpsParslBackend(SubmissionBackend):
    """
    Submits via ``bps submit`` (Parsl plugin), which blocks until complete.

    submit_id is a UUID placeholder; the real BPS run_id is parsed from
    stdout on completion and backfilled via WorkflowRunner._handle_event.

    Parameters
    ----------
    bps_dir : Path
        Directory that contains the BPS YAML files.
    """

    def __init__(self, bps_dir: Path) -> None:
        self._bps_dir   = bps_dir
        self._procs:    dict[str, asyncio.subprocess.Process] = {}
        self._log_dirs: dict[str, Path | None]                = {}

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

        log.info("Submitting (Parsl): %s", " ".join(cmd))

        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        submit_id = str(uuid.uuid4())
        self._procs[submit_id]    = proc
        self._log_dirs[submit_id] = log_dir

        log.info("Node %r → placeholder %s (pid=%s)", node.node_id, submit_id, proc.pid)
        return SubmissionResult(submit_id=submit_id, submit_location="parsl")

    def make_node_runner(
        self,
        node: "PipelineNode",
        bus: "EventBus",
        stop_event: asyncio.Event,
        *,
        workflow_id: str = "",
        wake_strategy: object = None,
    ) -> AbstractNodeRunner:
        submit_id = node.submit_id
        proc = self._procs.get(submit_id)
        if proc is None:
            raise KeyError(
                f"No process found for submit_id {submit_id!r}. "
                "Was submit() called before make_node_runner()?"
            )
        return BlockingNodeRunner(
            proc        = proc,
            workflow_id = workflow_id,
            node_id     = node.node_id,
            bus         = bus,
            node_label  = node.node_id,
            log_dir     = self._log_dirs.get(submit_id),
        )

    async def release_held(self, submit_id: str) -> None:
        log.warning(
            "BpsParslBackend.release_held(%r): no HTCondor hold concept; use restart",
            submit_id,
        )

    async def remove(self, submit_id: str) -> None:
        proc = self._procs.get(submit_id)
        if proc is not None and proc.returncode is None:
            log.info("BpsParslBackend.remove: terminating pid=%s", proc.pid)
            await _terminate_proc(proc)
        else:
            log.info("BpsParslBackend.remove(%r): no active process", submit_id)

    async def restart(self, submit_id: str) -> SubmissionResult:
        """submit_id here is the real BPS run_id (backfilled after the terminal event)."""
        cmd = ["bps", "restart", "--id", submit_id]
        log.info("Restarting (Parsl): %s", " ".join(cmd))

        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        new_submit_id = str(uuid.uuid4())
        self._procs[new_submit_id]    = proc
        self._log_dirs[new_submit_id] = None

        log.info("Restart of %r → placeholder %s (pid=%s)", submit_id, new_submit_id, proc.pid)
        return SubmissionResult(submit_id=new_submit_id, submit_location="parsl")
