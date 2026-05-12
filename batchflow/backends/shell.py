"""
backends/shell.py — ShellBackend + ShellNodeRunner

ShellBackend runs arbitrary blocking shell commands as workflow nodes.
Each submit() starts an async subprocess and returns a UUID submit_id.
restart() re-runs the same command (user is expected to have modified
the script externally before requesting a restart).

Subprocess output is redirected to a log file (not a PIPE) so that
proc.wait() returns as soon as the shell process exits, regardless of
whether any child processes have inherited the write-end of a pipe.
"""
from __future__ import annotations

import asyncio
import logging
import os
import shlex
import tempfile
import uuid
from pathlib import Path
from typing import TYPE_CHECKING

from .bps import SubmissionBackend, SubmissionResult
from ..bus import EventBus, EventType, JobEvent
from ..monitor import AbstractNodeRunner

if TYPE_CHECKING:
    from ..graph import PipelineNode

log = logging.getLogger(__name__)


def _make_log_path(node_id: str, log_dir: Path | None) -> Path:
    if log_dir is not None:
        log_dir.mkdir(parents=True, exist_ok=True)
        return log_dir / f"{node_id}.log"
    fd, path = tempfile.mkstemp(suffix=f"_{node_id}.log", prefix="batchflow_")
    os.close(fd)
    return Path(path)


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


class ShellNodeRunner(AbstractNodeRunner):
    """
    Awaits a blocking shell subprocess and publishes the terminal node event.

    Uses proc.wait() rather than proc.communicate() so that the runner
    returns as soon as the shell process exits, regardless of whether child
    processes have inherited the subprocess's file descriptors.
    """

    def __init__(
        self,
        proc:        asyncio.subprocess.Process,
        workflow_id: str,
        node_id:     str,
        bus:         EventBus,
        node_label:  str,
        log_path:    Path | None = None,
    ) -> None:
        self._proc        = proc
        self._workflow_id = workflow_id
        self._node_id     = node_id
        self._bus         = bus
        self._node_label  = node_label
        self._log_path    = log_path

    async def run(self) -> None:
        log.info(
            "ShellNodeRunner[%s/%s]: waiting for subprocess (pid=%s)",
            self._workflow_id, self._node_id, self._proc.pid,
        )
        await self._bus.publish(JobEvent(
            event_type  = EventType.JOB_RUNNING,
            workflow_id = self._workflow_id,
            node_id     = self._node_id,
        ))

        try:
            await self._proc.wait()
        except asyncio.CancelledError:
            log.warning(
                "ShellNodeRunner[%s/%s]: cancelled — terminating subprocess",
                self._workflow_id, self._node_id,
            )
            await _terminate_proc(self._proc)
            raise

        rc = self._proc.returncode

        if rc == 0:
            log.info(
                "ShellNodeRunner[%s/%s]: succeeded",
                self._workflow_id, self._node_id,
            )
            await self._bus.publish(JobEvent(
                event_type  = EventType.NODE_COMPLETE,
                workflow_id = self._workflow_id,
                node_id     = self._node_id,
            ))
        else:
            log.warning(
                "ShellNodeRunner[%s/%s]: failed (rc=%s)",
                self._workflow_id, self._node_id, rc,
            )
            await self._bus.publish(JobEvent(
                event_type  = EventType.NODE_FAILED,
                workflow_id = self._workflow_id,
                node_id     = self._node_id,
                exit_code   = rc,
            ))


class ShellBackend(SubmissionBackend):
    """
    Runs arbitrary blocking shell commands as workflow nodes.

    node.command is split via shlex and run as a subprocess.  The process
    blocks until the script exits.  restart() re-runs the same command,
    allowing the user to modify the script externally between attempts.
    """

    def __init__(self) -> None:
        self._procs:     dict[str, asyncio.subprocess.Process] = {}
        self._log_paths: dict[str, Path]                       = {}
        self._log_dirs:  dict[str, Path | None]                = {}
        self._commands:  dict[str, str]                        = {}
        self._node_ids:  dict[str, str]                        = {}

    async def submit(
        self,
        node: "PipelineNode",
        *,
        log_dir: Path | None = None,
    ) -> SubmissionResult:
        command = node.command
        if not command:
            raise ValueError(f"Node {node.node_id!r} has no command set")

        cmd = shlex.split(command)
        log.info("Submitting (shell): %s", command)

        log_path = _make_log_path(node.node_id, log_dir)
        log_f    = open(log_path, "wb")
        proc     = await asyncio.create_subprocess_exec(*cmd, stdout=log_f, stderr=log_f)
        log_f.close()

        submit_id = str(uuid.uuid4())
        self._procs[submit_id]     = proc
        self._log_paths[submit_id] = log_path
        self._log_dirs[submit_id]  = log_dir
        self._commands[submit_id]  = command
        self._node_ids[submit_id]  = node.node_id

        log.info("Node %r → %s (pid=%s)", node.node_id, submit_id, proc.pid)
        return SubmissionResult(submit_id=submit_id, submit_location="shell")

    def make_node_runner(
        self,
        node: "PipelineNode",
        bus: "EventBus",
        stop_event: asyncio.Event,
        *,
        workflow_id: str = "",
        wake_strategy: object = None,
    ) -> ShellNodeRunner:
        submit_id = node.submit_id
        proc = self._procs.get(submit_id)
        if proc is None:
            raise KeyError(
                f"No process found for submit_id {submit_id!r}. "
                "Was submit() called before make_node_runner()?"
            )
        return ShellNodeRunner(
            proc        = proc,
            workflow_id = workflow_id,
            node_id     = node.node_id,
            bus         = bus,
            node_label  = node.node_id,
            log_path    = self._log_paths.get(submit_id),
        )

    async def release_held(self, submit_id: str) -> None:
        log.warning(
            "ShellBackend.release_held(%r): no hold concept for shell nodes; use restart",
            submit_id,
        )

    async def remove(self, submit_id: str) -> None:
        proc = self._procs.get(submit_id)
        if proc is not None and proc.returncode is None:
            log.info("ShellBackend.remove: terminating pid=%s", proc.pid)
            await _terminate_proc(proc)
        else:
            log.info("ShellBackend.remove(%r): no active process", submit_id)

    async def restart(self, submit_id: str) -> SubmissionResult:
        """Re-run the same command (user modifies the script externally first)."""
        command = self._commands.get(submit_id)
        if not command:
            raise KeyError(f"No command found for submit_id {submit_id!r}")

        node_id = self._node_ids.get(submit_id, submit_id)
        log_dir = self._log_dirs.get(submit_id)
        cmd     = shlex.split(command)
        log.info("Restarting (shell): %s", command)

        log_path = _make_log_path(node_id, log_dir)
        log_f    = open(log_path, "wb")
        proc     = await asyncio.create_subprocess_exec(*cmd, stdout=log_f, stderr=log_f)
        log_f.close()

        new_submit_id = str(uuid.uuid4())
        self._procs[new_submit_id]     = proc
        self._log_paths[new_submit_id] = log_path
        self._log_dirs[new_submit_id]  = log_dir
        self._commands[new_submit_id]  = command
        self._node_ids[new_submit_id]  = node_id

        log.info("Restart → %s (pid=%s)", new_submit_id, proc.pid)
        return SubmissionResult(submit_id=new_submit_id, submit_location="shell")
