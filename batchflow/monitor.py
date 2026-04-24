"""
monitor.py — HTCondorMonitor with pluggable wake strategy.

The monitor runs as a long-lived asyncio task.  It watches one HTCondor
cluster (one bps submission) and emits JobEvents onto the EventBus
whenever a job's state changes.

Wake strategies
---------------
TimerWakeStrategy   — sleeps for a fixed interval, then queries condor_q.
                      Used now; works on any filesystem.
InotifyWakeStrategy — wakes immediately when the DAGMan event log is
                      written.  Suitable once inotify is available on
                      the shared filesystem.  Swap in by passing a
                      different strategy to HTCondorMonitor.

Both strategies implement the same one-method interface so the monitor
is completely unaware of which one is active.
"""
from __future__ import annotations

import asyncio
import json
import logging
import subprocess
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import AsyncIterator

from .bus import EventBus, EventType, JobEvent

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# HTCondor job status codes from ClassAds
# ---------------------------------------------------------------------------

_CONDOR_STATUS = {
    1: "Idle",
    2: "Running",
    3: "Removed",
    4: "Completed",
    5: "Held",
    6: "Transferring",
    7: "Suspended",
}

# Map HTCondor JobStatus to our EventType.
# These are *job-level* events; node-level synthesis happens in the runner.
_STATUS_TO_EVENT: dict[int, EventType] = {
    1: EventType.JOB_SUBMITTED,
    2: EventType.JOB_RUNNING,
    4: EventType.JOB_SUCCEEDED,
    5: EventType.JOB_HELD,
}


# ---------------------------------------------------------------------------
# Wake strategy interface
# ---------------------------------------------------------------------------

class WakeStrategy(ABC):
    """
    Controls *when* the monitor wakes to query condor_q.
    Implementations yield once per desired wake cycle.
    """

    @abstractmethod
    async def wake_cycles(self) -> AsyncIterator[None]:
        """Async generator; yield to trigger a condor_q query."""
        ...  # pragma: no cover
        yield  # make it a generator for type checkers


class TimerWakeStrategy(WakeStrategy):
    """
    Wake on a fixed interval.  Safe on any filesystem.

    Parameters
    ----------
    interval : float
        Seconds between condor_q polls.  Default 60 s is a reasonable
        balance between freshness and condor_q load for multi-day runs.
    """

    def __init__(self, interval: float = 60.0) -> None:
        self.interval = interval

    async def wake_cycles(self) -> AsyncIterator[None]:
        while True:
            yield
            await asyncio.sleep(self.interval)


class InotifyWakeStrategy(WakeStrategy):
    """
    Wake when the DAGMan event log file is written.

    Requires the ``watchfiles`` package (``pip install watchfiles``) and
    an inotify-capable filesystem.  Falls back to a 10 s timer if
    watchfiles is not installed.

    Parameters
    ----------
    log_path : Path
        Path to the DAGMan ``*.dag.dagman.log`` file.
    fallback_interval : float
        Timer interval used if watchfiles is unavailable.
    """

    def __init__(self, log_path: Path, fallback_interval: float = 10.0) -> None:
        self.log_path = log_path
        self.fallback_interval = fallback_interval

    async def wake_cycles(self) -> AsyncIterator[None]:
        try:
            from watchfiles import awatch  # type: ignore
            async for _ in awatch(self.log_path):
                yield
        except ImportError:
            log.warning(
                "watchfiles not installed; falling back to %ss timer",
                self.fallback_interval,
            )
            strategy = TimerWakeStrategy(self.fallback_interval)
            async for _ in strategy.wake_cycles():
                yield


# ---------------------------------------------------------------------------
# Condor query helpers (run in executor to avoid blocking the event loop)
# ---------------------------------------------------------------------------

@dataclass
class _CondorJobSummary:
    cluster_id:   str
    job_status:   int         # raw HTCondor status code
    hold_reason:  str
    exit_code:    int | None
    proc_ids:     list[str]   # list of "cluster.proc" strings


def _query_condor_q(cluster_id: str) -> list[_CondorJobSummary]:
    """
    Run ``condor_q -json <cluster_id>`` synchronously.
    Returns one summary per proc in the cluster.
    Raises subprocess.CalledProcessError on condor_q failure.
    """
    result = subprocess.run(
        ["condor_q", "-json", str(cluster_id)],
        capture_output=True, text=True, check=True,
    )
    ads = json.loads(result.stdout or "[]")
    summaries = []
    for ad in ads:
        cid = f"{ad.get('ClusterId', cluster_id)}.{ad.get('ProcId', 0)}"
        summaries.append(_CondorJobSummary(
            cluster_id  = str(ad.get("ClusterId", cluster_id)),
            job_status  = int(ad.get("JobStatus", 0)),
            hold_reason = ad.get("HoldReason", ""),
            exit_code   = ad.get("ExitCode"),
            proc_ids    = [cid],
        ))
    return summaries


def _query_condor_history(cluster_id: str) -> list[_CondorJobSummary]:
    """
    Query condor_history for jobs no longer in the queue (completed/failed).
    """
    result = subprocess.run(
        ["condor_history", "-json", str(cluster_id)],
        capture_output=True, text=True, check=True,
    )
    ads = json.loads(result.stdout or "[]")
    summaries = []
    for ad in ads:
        cid = f"{ad.get('ClusterId', cluster_id)}.{ad.get('ProcId', 0)}"
        summaries.append(_CondorJobSummary(
            cluster_id  = str(ad.get("ClusterId", cluster_id)),
            job_status  = int(ad.get("JobStatus", 4)),   # 4 = Completed
            hold_reason = ad.get("HoldReason", ""),
            exit_code   = ad.get("ExitCode"),
            proc_ids    = [cid],
        ))
    return summaries


# ---------------------------------------------------------------------------
# Monitor
# ---------------------------------------------------------------------------

class HTCondorMonitor:
    """
    Watches one HTCondor cluster and publishes JobEvents to the EventBus.

    Parameters
    ----------
    workflow_id : str
    node_id : str
        The PipelineNode this cluster belongs to.
    cluster_id : str
        The HTCondor cluster ID returned by bps submit.
    bus : EventBus
    wake_strategy : WakeStrategy
        Defaults to TimerWakeStrategy(60).
    stop_event : asyncio.Event
        Set this to stop the monitor task cleanly.
    """

    def __init__(
        self,
        workflow_id:   str,
        node_id:       str,
        cluster_id:    str,
        bus:           EventBus,
        wake_strategy: WakeStrategy | None = None,
        stop_event:    asyncio.Event | None = None,
    ) -> None:
        self.workflow_id   = workflow_id
        self.node_id       = node_id
        self.cluster_id    = cluster_id
        self.bus           = bus
        self.wake_strategy = wake_strategy or TimerWakeStrategy()
        self.stop_event    = stop_event or asyncio.Event()

        # Track last-seen per-proc status to emit only on transitions.
        self._last_status: dict[str, int] = {}
        self._node_done = False

    async def run(self) -> None:
        """
        Main monitor loop.  Run as an asyncio Task:
        ``asyncio.create_task(monitor.run())``.
        """
        log.info(
            "Monitor[%s/%s]: watching cluster %s",
            self.workflow_id, self.node_id, self.cluster_id,
        )
        async for _ in self.wake_strategy.wake_cycles():
            if self.stop_event.is_set() or self._node_done:
                break
            await self._poll()

    async def _poll(self) -> None:
        loop = asyncio.get_running_loop()
        try:
            summaries = await loop.run_in_executor(
                None, _query_condor_q, self.cluster_id
            )
        except FileNotFoundError:
            log.warning("condor_q not found — is HTCondor installed and on PATH?")
            return
        except subprocess.CalledProcessError as exc:
            log.warning("condor_q failed for cluster %s: %s",
                        self.cluster_id, exc)
            return
        except json.JSONDecodeError as exc:
            log.warning("condor_q JSON parse error for cluster %s: %s",
                        self.cluster_id, exc)
            return

        if not summaries:
            # Jobs no longer in queue — check history.
            try:
                summaries = await loop.run_in_executor(
                    None, _query_condor_history, self.cluster_id
                )
            except Exception as exc:
                log.warning("condor_history failed: %s", exc)
                return

        await self._process_summaries(summaries)

    async def _process_summaries(
        self, summaries: list[_CondorJobSummary]
    ) -> None:
        held_reasons: list[str] = []
        all_done = True
        any_failed = False
        any_held = False

        for s in summaries:
            for proc_id in s.proc_ids:
                prev = self._last_status.get(proc_id)
                if prev == s.job_status:
                    continue  # no transition

                self._last_status[proc_id] = s.job_status
                event_type = _STATUS_TO_EVENT.get(s.job_status)
                if event_type is None:
                    continue

                await self.bus.publish(JobEvent(
                    event_type   = event_type,
                    workflow_id  = self.workflow_id,
                    node_id      = self.node_id,
                    cluster_id   = s.cluster_id,
                    hold_reasons = (s.hold_reason,) if s.hold_reason else (),
                    exit_code    = s.exit_code,
                ))

                if s.hold_reason:
                    held_reasons.append(s.hold_reason)

            # Node-level bookkeeping.
            if s.job_status == 5:   # Held
                any_held = True
            if s.job_status not in (3, 4):  # not Removed/Completed
                all_done = False
            if s.exit_code is not None and s.exit_code != 0:
                any_failed = True

        # Synthesise node-level events once all procs have terminal status.
        if all_done and not self._node_done:
            self._node_done = True
            if any_held:
                node_event = EventType.NODE_HELD
            elif any_failed:
                node_event = EventType.NODE_FAILED
            else:
                node_event = EventType.NODE_COMPLETE

            await self.bus.publish(JobEvent(
                event_type   = node_event,
                workflow_id  = self.workflow_id,
                node_id      = self.node_id,
                cluster_id   = self.cluster_id,
                hold_reasons = tuple(held_reasons),
            ))
            log.info("Monitor[%s/%s]: node terminal → %s",
                     self.workflow_id, self.node_id, node_event.value)
