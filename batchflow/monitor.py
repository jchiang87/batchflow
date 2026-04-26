"""
monitor.py — HTCondorMonitor with pluggable wake strategy.

The monitor runs as a long-lived asyncio task.  It watches one HTCondor
cluster (one bps submission) and emits JobEvents onto the EventBus
whenever a job's state changes.

Wake strategies
---------------
TimerWakeStrategy   — sleeps for a fixed interval, then queries the schedd.
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
import functools
import logging
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
    Controls *when* the monitor wakes to query the schedd.
    Implementations yield once per desired wake cycle.
    """

    @abstractmethod
    async def wake_cycles(self) -> AsyncIterator[None]:
        """Async generator; yield to trigger a schedd query."""
        ...  # pragma: no cover
        yield  # make it a generator for type checkers


class TimerWakeStrategy(WakeStrategy):
    """
    Wake on a fixed interval.  Safe on any filesystem.

    Parameters
    ----------
    interval : float
        Seconds between schedd polls.  Default 60 s is a reasonable
        balance between freshness and schedd load for multi-day runs.
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


def _get_schedd(htcondor_mod, schedd_name: str | None):
    """
    Return an htcondor.Schedd connected to *schedd_name*.
    Falls back to the local schedd if schedd_name is None/empty or if the
    Collector lookup fails.
    """
    if not schedd_name:
        return htcondor_mod.Schedd()
    try:
        coll = htcondor_mod.Collector()
        schedd_ad = coll.locate(htcondor_mod.DaemonTypes.Schedd, schedd_name)
        return htcondor_mod.Schedd(schedd_ad)
    except Exception as exc:
        log.warning(
            "Could not locate schedd %r via Collector (%s); "
            "falling back to local schedd", schedd_name, exc,
        )
        return htcondor_mod.Schedd()


def _query_condor_q(
    cluster_id: str, schedd_name: str | None
) -> list[_CondorJobSummary]:
    """
    Query active HTCondor jobs for *cluster_id* using the Python bindings.
    Runs synchronously; call via run_in_executor.
    """
    import htcondor
    schedd = _get_schedd(htcondor, schedd_name)
    ads = list(schedd.query(
        constraint=f"ClusterId == {cluster_id}",
        projection=["ClusterId", "ProcId", "JobStatus", "ExitCode", "HoldReason"],
    ))
    summaries = []
    for ad in ads:
        cid = f"{ad.get('ClusterId', cluster_id)}.{ad.get('ProcId', 0)}"
        summaries.append(_CondorJobSummary(
            cluster_id  = str(ad.get("ClusterId", cluster_id)),
            job_status  = int(ad.get("JobStatus", 0)),
            hold_reason = ad.get("HoldReason", "") or "",
            exit_code   = ad.get("ExitCode"),
            proc_ids    = [cid],
        ))
    return summaries


def _query_condor_history(
    cluster_id: str, schedd_name: str | None
) -> list[_CondorJobSummary]:
    """
    Query completed/removed jobs for *cluster_id* using the Python bindings.
    Runs synchronously; call via run_in_executor.
    """
    import htcondor
    schedd = _get_schedd(htcondor, schedd_name)
    ads = list(schedd.history(
        constraint=f"ClusterId == {cluster_id}",
        projection=["ClusterId", "ProcId", "JobStatus", "ExitCode", "HoldReason"],
    ))
    summaries = []
    for ad in ads:
        cid = f"{ad.get('ClusterId', cluster_id)}.{ad.get('ProcId', 0)}"
        summaries.append(_CondorJobSummary(
            cluster_id  = str(ad.get("ClusterId", cluster_id)),
            job_status  = int(ad.get("JobStatus", 4)),
            hold_reason = ad.get("HoldReason", "") or "",
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
    schedd_name : str | None
        FQDN of the schedd that owns this cluster (e.g.
        ``"sdfiana032.sdf.slac.stanford.edu"``).  None falls back to the
        local schedd.
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
        schedd_name:   str | None = None,
        wake_strategy: WakeStrategy | None = None,
        stop_event:    asyncio.Event | None = None,
    ) -> None:
        self.workflow_id   = workflow_id
        self.node_id       = node_id
        self.cluster_id    = cluster_id
        self.bus           = bus
        self.schedd_name   = schedd_name
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
            "Monitor[%s/%s]: watching cluster %s on schedd %s",
            self.workflow_id, self.node_id, self.cluster_id,
            self.schedd_name or "(local)",
        )
        async for _ in self.wake_strategy.wake_cycles():
            if self.stop_event.is_set() or self._node_done:
                break
            await self._poll()

    async def _poll(self) -> None:
        loop = asyncio.get_running_loop()
        try:
            summaries = await loop.run_in_executor(
                None,
                functools.partial(_query_condor_q, self.cluster_id, self.schedd_name),
            )
        except ImportError as exc:
            log.warning("htcondor bindings not available: %s", exc)
            return
        except Exception as exc:
            log.warning("condor query failed for cluster %s: %s",
                        self.cluster_id, exc)
            return

        if not summaries:
            # Jobs no longer in queue — check history.
            try:
                summaries = await loop.run_in_executor(
                    None,
                    functools.partial(
                        _query_condor_history, self.cluster_id, self.schedd_name
                    ),
                )
            except Exception as exc:
                log.warning("condor history failed: %s", exc)
                return

        if not summaries:
            # Cluster absent from both condor_q and condor_history.  This can
            # happen if the schedd evicts old history entries under memory
            # pressure before our poll cycle fires.  Only treat this as a
            # completion if we previously saw the cluster (non-empty
            # _last_status), meaning the jobs ran and finished before being
            # evicted.  If we have never seen it, log a warning and keep
            # polling — it may simply not have appeared yet.
            if self._last_status:
                log.warning(
                    "Monitor[%s/%s]: cluster %s absent from condor_q and "
                    "condor_history; assuming completed",
                    self.workflow_id, self.node_id, self.cluster_id,
                )
                await self._process_summaries([])
            else:
                log.warning(
                    "Monitor[%s/%s]: cluster %s not yet visible in condor_q; "
                    "will retry next poll",
                    self.workflow_id, self.node_id, self.cluster_id,
                )
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
