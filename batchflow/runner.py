"""
runner.py — WorkflowRunner: async DAG scheduler.

Stall handling
--------------
When the workflow stalls (nothing running, something blocked), the
scheduler publishes WORKFLOW_STALLED then waits on ``_intervention_event``
— an asyncio.Event set by any INTERVENTION_* event.  A stalled multi-day
run consumes no CPU until the agent acts.

``run()`` accepts a ``stall_timeout`` constructor parameter:
  - None (default): wait indefinitely — correct for production.
  - float: give up after N seconds — useful in tests.

Outcome
-------
``run()`` returns a ``RunOutcome`` so callers distinguish completion from
stall without inspecting the graph.
"""
from __future__ import annotations

import asyncio
import logging
from enum import Enum
from pathlib import Path

from .agent import AgentHandler
from .backends.bps import SubmissionBackend, SubmissionResult
from .bus import EventBus, EventType, JobEvent
from .graph import NodeState, PipelineNode, WorkflowGraph
from .monitor import HTCondorMonitor, WakeStrategy
from .state import StateStore

log = logging.getLogger(__name__)


class RunOutcome(str, Enum):
    COMPLETE  = "COMPLETE"
    STALLED   = "STALLED"
    CANCELLED = "CANCELLED"


class WorkflowRunner:
    """
    Async DAG runner.

    Parameters
    ----------
    graph : WorkflowGraph
    backend : SubmissionBackend
    store : StateStore
    bus : EventBus
    bps_dir : Path
    log_dir : Path
    wake_strategy : WakeStrategy | None
    agent_handler : AgentHandler | None
    stall_timeout : float | None
        Seconds to wait for intervention after a stall before returning
        RunOutcome.STALLED.  None = wait forever (production default).
    """

    def __init__(
        self,
        graph:          WorkflowGraph,
        backend:        SubmissionBackend,
        store:          StateStore,
        bus:            EventBus,
        bps_dir:        Path,
        log_dir:        Path = Path("./logs"),
        wake_strategy:  WakeStrategy | None = None,
        agent_handler:  AgentHandler | None = None,
        stall_timeout:  float | None = None,
    ) -> None:
        self._graph         = graph
        self._backend       = backend
        self._store         = store
        self._bus           = bus
        self._bps_dir       = bps_dir
        self._log_dir       = log_dir
        self._wake_strategy = wake_strategy
        self._agent_handler = agent_handler
        self._stall_timeout = stall_timeout

        self._runner_queue       = bus.subscribe("runner")
        self._store_queue        = bus.subscribe("state_store")
        self._stop_event         = asyncio.Event()
        self._intervention_event = asyncio.Event()

        self._monitor_tasks: dict[str, asyncio.Task] = {}

    # ------------------------------------------------------------------
    # Public entry point
    # ------------------------------------------------------------------

    async def run(self) -> RunOutcome:
        await self._maybe_resume()

        background: list[asyncio.Task] = [
            asyncio.create_task(self._store_loop(), name="runner-store"),
        ]
        if self._agent_handler:
            background.append(
                asyncio.create_task(self._agent_handler.run(), name="agent-handler")
            )

        outcome = RunOutcome.CANCELLED
        try:
            await self._submit_ready_nodes()
            outcome = await self._scheduler_loop()
        except asyncio.CancelledError:
            log.info("WorkflowRunner: cancelled")
            outcome = RunOutcome.CANCELLED
        finally:
            self._stop_event.set()
            for t in background:
                t.cancel()
            await asyncio.gather(*background, return_exceptions=True)

        log.info("WorkflowRunner: outcome=%s", outcome.value)
        return outcome

    # ------------------------------------------------------------------
    # Resume
    # ------------------------------------------------------------------

    async def _maybe_resume(self) -> None:
        saved = await self._store.load_workflow(self._graph.workflow_id)
        if saved is None:
            await self._store.save_workflow(self._graph)
            return

        log.info("Resuming workflow %r from saved state", self._graph.workflow_id)
        for node in saved.nodes:
            live = self._graph._nodes.get(node.node_id)
            if live is None:
                continue
            live.state         = node.state
            live.restart_count = node.restart_count
            live.submit_id     = node.submit_id
            live.schedd_name   = node.schedd_name

        in_flight = {NodeState.SUBMITTED, NodeState.RUNNING}
        for node in self._graph.nodes:
            if node.state in in_flight and node.submit_id:
                log.info("Re-attaching monitor for %r (cluster %s)",
                         node.node_id, node.submit_id)
                self._spawn_monitor(node)

    # ------------------------------------------------------------------
    # Scheduler loop
    # ------------------------------------------------------------------

    async def _scheduler_loop(self) -> RunOutcome:
        while True:
            await self._drain_queue()

            if self._graph.is_complete():
                await self._bus.publish(JobEvent(
                    event_type  = EventType.WORKFLOW_COMPLETE,
                    workflow_id = self._graph.workflow_id,
                    node_id     = "__workflow__",
                ))
                return RunOutcome.COMPLETE

            if self._graph.is_stalled():
                log.warning("Workflow %r stalled", self._graph.workflow_id)
                await self._bus.publish(JobEvent(
                    event_type  = EventType.WORKFLOW_STALLED,
                    workflow_id = self._graph.workflow_id,
                    node_id     = "__workflow__",
                ))
                resumed = await self._wait_for_intervention()
                if not resumed:
                    return RunOutcome.STALLED
                self._intervention_event.clear()
                continue

            # Not stalled — wait briefly for the next event.
            try:
                event = await asyncio.wait_for(
                    self._runner_queue.get(), timeout=1.0
                )
                await self._handle_event(event)
            except asyncio.TimeoutError:
                pass

    async def _drain_queue(self) -> None:
        """Process all events currently queued, without blocking."""
        while True:
            try:
                event = self._runner_queue.get_nowait()
                await self._handle_event(event)
            except asyncio.QueueEmpty:
                break

    async def _wait_for_intervention(self) -> bool:
        """
        Park until an intervention event wakes us, or stall_timeout expires.
        While parked we keep draining the runner queue so monitor events
        (e.g. a node that was running when the stall was declared finally
        finishing) are not lost.
        Returns True if intervention arrived, False if timed out.
        """
        self._intervention_event.clear()

        async def _park() -> bool:
            while not self._intervention_event.is_set():
                try:
                    event = await asyncio.wait_for(
                        self._runner_queue.get(), timeout=5.0
                    )
                    await self._handle_event(event)
                except asyncio.TimeoutError:
                    pass
            return True

        if self._stall_timeout is not None:
            try:
                return await asyncio.wait_for(_park(), timeout=self._stall_timeout)
            except asyncio.TimeoutError:
                return False
        else:
            return await _park()

    # ------------------------------------------------------------------
    # Event handling
    # ------------------------------------------------------------------

    async def _handle_event(self, event: JobEvent) -> None:
        node_id = event.node_id
        if node_id == "__workflow__":
            return

        node = self._graph._nodes.get(node_id)
        if node is None:
            return

        if event.event_type == EventType.NODE_COMPLETE:
            node.state = NodeState.SUCCEEDED
            log.info("Node %r succeeded", node_id)
            await self._store.save_workflow(self._graph)
            await self._submit_ready_nodes()

        elif event.event_type in {EventType.NODE_FAILED, EventType.NODE_HELD}:
            node.state = (
                NodeState.HELD
                if event.event_type == EventType.NODE_HELD
                else NodeState.FAILED
            )
            log.warning("Node %r -> %s", node_id, node.state.value)
            await self._store.save_workflow(self._graph)

        elif event.event_type == EventType.JOB_RUNNING:
            if node.state != NodeState.SUCCEEDED:
                node.state = NodeState.RUNNING
                await self._store.save_workflow(self._graph)

        elif event.event_type in {
            EventType.INTERVENTION_RESTART,
            EventType.INTERVENTION_SKIP,
            EventType.INTERVENTION_MODIFY,
            EventType.INTERVENTION_ABORT,
        }:
            self._intervention_event.set()
            if event.event_type != EventType.INTERVENTION_ABORT:
                await self._submit_ready_nodes()

    # ------------------------------------------------------------------
    # Store loop
    # ------------------------------------------------------------------

    async def _store_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                event = await asyncio.wait_for(
                    self._store_queue.get(), timeout=1.0
                )
            except asyncio.TimeoutError:
                continue
            try:
                await self._store.record_event(event)
            except Exception as exc:
                log.error("StateStore.record_event failed: %s", exc)

    # ------------------------------------------------------------------
    # Submission and monitor spawning
    # ------------------------------------------------------------------

    async def _submit_ready_nodes(self) -> None:
        ready = list(self._graph.ready_nodes())
        if not ready:
            return

        log.info("Submitting %d ready node(s): %s",
                 len(ready), [n.node_id for n in ready])

        for node in ready:
            self._graph.mark_ready(node.node_id)

        results = await asyncio.gather(
            *[self._submit_node(node) for node in ready],
            return_exceptions=True,
        )
        for node, result in zip(ready, results):
            if isinstance(result, Exception):
                log.error("Submission failed for %r: %s", node.node_id, result)
                node.state = NodeState.FAILED
                await self._bus.publish(JobEvent(
                    event_type   = EventType.NODE_FAILED,
                    workflow_id  = self._graph.workflow_id,
                    node_id      = node.node_id,
                    hold_reasons = ("Submission error: " + str(result),),
                ))

        await self._store.save_workflow(self._graph)

    async def _submit_node(self, node: PipelineNode) -> None:
        result: SubmissionResult = await self._backend.submit(
            node.bps_yaml,
            self._bps_dir,
            overrides=node.bps_overrides or None,
            log_dir=self._log_dir,
        )
        node.submit_id   = result.cluster_id
        node.schedd_name = result.schedd_name
        node.state       = NodeState.SUBMITTED

        await self._bus.publish(JobEvent(
            event_type  = EventType.JOB_SUBMITTED,
            workflow_id = self._graph.workflow_id,
            node_id     = node.node_id,
            cluster_id  = result.cluster_id,
        ))

        self._spawn_monitor(node)

    def _spawn_monitor(self, node: PipelineNode) -> None:
        monitor = HTCondorMonitor(
            workflow_id   = self._graph.workflow_id,
            node_id       = node.node_id,
            cluster_id    = node.submit_id,
            bus           = self._bus,
            schedd_name   = node.schedd_name,
            wake_strategy = self._wake_strategy,
            stop_event    = self._stop_event,
        )
        task = asyncio.create_task(
            monitor.run(), name=f"monitor-{node.node_id}"
        )
        self._monitor_tasks[node.node_id] = task
