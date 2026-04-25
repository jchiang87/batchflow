"""
agent.py — AgentHandler, AgentNotification, and InterventionActions.

AgentHandler
    Subscribes to the EventBus and, on failure/held events, pushes a
    structured AgentNotification to one or more registered transports.
    Transports are pluggable (webhook, stdout, custom callables).

AgentNotification
    The payload sent to the agent.  Designed to be self-contained: the
    agent should be able to decide what to do without any follow-up
    queries.

InterventionActions
    Called by the agent (or autonomously by the handler based on
    classifier output) to restart, abort, skip, or modify a node.
    Every action is recorded in the StateStore for audit.
"""
from __future__ import annotations

import asyncio
import json
import logging
from abc import ABC, abstractmethod
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Coroutine

from .bus import EventBus, EventType, JobEvent
from .classifier import Classification, ErrorClassifier
from .graph import NodeState, WorkflowGraph

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Notification payload
# ---------------------------------------------------------------------------

@dataclass
class AgentNotification:
    """
    Everything an AI agent needs to decide on an intervention.

    Fields
    ------
    event_type : str
        The triggering event (e.g. ``"NODE_HELD"``).
    workflow_id, node_id : str
    timestamp : datetime
    hold_reasons : list[str]
        Raw HTCondor HoldReason strings.
    classification : Classification
        Classifier output — verdict, confidence, suggested_action.
    restart_count : int
        How many times this node has already been restarted.
    max_restarts : int
    dag_context : dict[str, str]
        Maps node_id → state.value for all nodes in the workflow,
        so the agent can see what is blocked downstream.
    bps_yaml : str
        The BPS file this node uses (useful for crafting modified retries).
    metadata : dict
        Arbitrary metadata from the PipelineNode definition.
    """
    event_type:      str
    workflow_id:     str
    node_id:         str
    timestamp:       datetime
    hold_reasons:    list[str]
    classification:  Classification
    restart_count:   int
    max_restarts:    int
    dag_context:     dict[str, str]
    bps_yaml:        str
    metadata:        dict = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        d["timestamp"] = self.timestamp.isoformat()
        d["classification"] = asdict(self.classification)
        return d

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), indent=2)


# ---------------------------------------------------------------------------
# Notification transports
# ---------------------------------------------------------------------------

class NotificationTransport(ABC):
    """
    Delivers an AgentNotification to some external destination.
    Implement this to add webhooks, message queues, email, etc.
    """

    @abstractmethod
    async def send(self, notification: AgentNotification) -> None:
        ...


class StdoutTransport(NotificationTransport):
    """
    Prints notifications as JSON to stdout.
    Useful for development, piping to a monitoring process, or tests.
    """

    async def send(self, notification: AgentNotification) -> None:
        print(notification.to_json(), flush=True)


class WebhookTransport(NotificationTransport):
    """
    POSTs notifications as JSON to an HTTP endpoint.

    Requires ``aiohttp`` (``pip install aiohttp``).

    Parameters
    ----------
    url : str
        The endpoint URL.
    headers : dict | None
        Optional extra headers (e.g. Authorization).
    timeout : float
        Request timeout in seconds.
    """

    def __init__(
        self,
        url: str,
        headers: dict[str, str] | None = None,
        timeout: float = 30.0,
    ) -> None:
        self.url = url
        self.headers = {"Content-Type": "application/json", **(headers or {})}
        self.timeout = timeout

    async def send(self, notification: AgentNotification) -> None:
        try:
            import aiohttp  # type: ignore
        except ImportError:
            raise ImportError(
                "WebhookTransport requires aiohttp: pip install aiohttp"
            )
        async with aiohttp.ClientSession() as session:
            async with session.post(
                self.url,
                data=notification.to_json(),
                headers=self.headers,
                timeout=aiohttp.ClientTimeout(total=self.timeout),
            ) as resp:
                if not resp.ok:
                    log.warning(
                        "Webhook POST to %s returned %d", self.url, resp.status
                    )


class CallbackTransport(NotificationTransport):
    """
    Calls an async Python callable — useful for tight in-process agents.

    Parameters
    ----------
    callback : async callable(AgentNotification) → None
    """

    def __init__(
        self,
        callback: Callable[[AgentNotification], Coroutine],
    ) -> None:
        self._callback = callback

    async def send(self, notification: AgentNotification) -> None:
        await self._callback(notification)


# ---------------------------------------------------------------------------
# AgentHandler
# ---------------------------------------------------------------------------

class AgentHandler:
    """
    Subscribes to EventBus events and pushes AgentNotifications on
    failure/held/stalled conditions.

    Parameters
    ----------
    bus : EventBus
    graph : WorkflowGraph
    classifier : ErrorClassifier
    transports : list[NotificationTransport]
        One or more destinations for notifications.
    auto_restart : bool
        If True, automatically call InterventionActions.restart_node()
        when the classifier returns ``"transient"`` with confidence ≥
        auto_restart_threshold.  Set False to leave all decisions to the
        external agent.
    auto_restart_threshold : float
        Minimum classifier confidence for auto-restart.
    """

    NOTIFY_ON = {
        EventType.NODE_HELD,
        EventType.NODE_FAILED,
        EventType.WORKFLOW_STALLED,
        EventType.WORKFLOW_COMPLETE,
    }

    def __init__(
        self,
        bus: EventBus,
        graph: WorkflowGraph,
        classifier: ErrorClassifier,
        transports: list[NotificationTransport],
        interventions: "InterventionActions",
        auto_restart: bool = False,
        auto_restart_threshold: float = 0.8,
        stop_event: asyncio.Event | None = None,
    ) -> None:
        self._graph = graph
        self._classifier = classifier
        self._transports = transports
        self._interventions = interventions
        self._auto_restart = auto_restart
        self._auto_restart_threshold = auto_restart_threshold
        self._stop_event = stop_event or asyncio.Event()
        self._queue = bus.subscribe("agent_handler")

    async def run(self) -> None:
        """
        Consume events from the bus and dispatch notifications.
        Run as an asyncio Task.
        """
        from .bus import EventBus  # local import to avoid circular
        log.info("AgentHandler: running")
        while not self._stop_event.is_set():
            try:
                event = await asyncio.wait_for(self._queue.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            await self._handle(event)

    async def _handle(self, event: JobEvent) -> None:
        if event.event_type not in self.NOTIFY_ON:
            return

        node = self._graph._nodes.get(event.node_id)
        clf = self._classifier.classify(list(event.hold_reasons))

        notification = AgentNotification(
            event_type     = event.event_type.value,
            workflow_id    = event.workflow_id,
            node_id        = event.node_id,
            timestamp      = event.timestamp,
            hold_reasons   = list(event.hold_reasons),
            classification = clf,
            restart_count  = node.restart_count if node else 0,
            max_restarts   = node.max_restarts if node else 0,
            dag_context    = {
                n.node_id: n.state.value
                for n in self._graph.nodes
            },
            bps_yaml       = node.bps_yaml if node else "",
            metadata       = node.metadata if node else {},
        )

        # Fan out to all transports concurrently.
        await asyncio.gather(
            *[t.send(notification) for t in self._transports],
            return_exceptions=True,
        )

        # Optionally act autonomously on transient failures.
        if (
            self._auto_restart
            and event.event_type in {EventType.NODE_HELD, EventType.NODE_FAILED}
            and clf.verdict == "transient"
            and clf.confidence >= self._auto_restart_threshold
            and node is not None
            and node.is_restartable
        ):
            log.info(
                "AgentHandler: auto-restarting %r (confidence=%.2f)",
                event.node_id, clf.confidence,
            )
            await self._interventions.restart_node(
                event.node_id,
                reason=f"auto-restart: {clf.matched_patterns}",
                actor="agent_handler",
            )


# ---------------------------------------------------------------------------
# InterventionActions
# ---------------------------------------------------------------------------

class InterventionActions:
    """
    Callable interventions available to an agent or to AgentHandler's
    auto-restart logic.

    Every action is idempotent where possible and is recorded in the
    StateStore with a timestamp, actor, and reason for post-run audit.

    Parameters
    ----------
    graph : WorkflowGraph
    backend : SubmissionBackend
    bus : EventBus
    store : StateStore
    bps_dir : Path
    """

    def __init__(
        self,
        graph: "WorkflowGraph",
        backend: "SubmissionBackend",
        bus: EventBus,
        store: "StateStore",
        bps_dir: "Path",
    ) -> None:
        from .backends.bps import SubmissionBackend  # avoid top-level circular
        self._graph   = graph
        self._backend = backend
        self._bus     = bus
        self._store   = store
        self._bps_dir = bps_dir

    async def restart_node(
        self,
        node_id: str,
        reason: str = "",
        actor: str = "agent",
        bps_overrides: dict | None = None,
    ) -> None:
        """
        Release held jobs or re-submit if the node has fully failed.
        Increments restart_count; refuses if max_restarts exceeded.
        """
        node = self._graph.node(node_id)
        if not node.is_restartable:
            raise RuntimeError(
                f"Node {node_id!r} is not restartable "
                f"(state={node.state.value}, "
                f"restarts={node.restart_count}/{node.max_restarts})"
            )

        node.restart_count += 1
        log.info("Restarting node %r (attempt %d/%d): %s",
                 node_id, node.restart_count, node.max_restarts, reason)

        if node.state == NodeState.HELD and node.submit_id:
            await self._backend.release_held(node.submit_id)
        else:
            # Re-submit from scratch.
            overrides = {**node.bps_overrides, **(bps_overrides or {})}
            result = await self._backend.submit(
                node.bps_yaml, self._bps_dir, overrides=overrides or None
            )
            node.submit_id   = result.cluster_id
            node.schedd_name = result.schedd_name

        node.state = NodeState.SUBMITTED
        event = JobEvent(
            event_type  = EventType.INTERVENTION_RESTART,
            workflow_id = self._graph.workflow_id,
            node_id     = node_id,
            cluster_id  = node.submit_id,
            actor       = actor,
            reason      = reason,
        )
        await self._store.save_workflow(self._graph)
        await self._bus.publish(event)
        await self._store.record_event(event)

    async def abort_node(
        self,
        node_id: str,
        reason: str = "",
        actor: str = "agent",
    ) -> None:
        """
        Cancel all HTCondor jobs for this node and mark it FAILED.
        Dependents remain PENDING (workflow is stalled until skipped or
        the node is restarted manually).
        """
        node = self._graph.node(node_id)
        if node.submit_id:
            await self._backend.remove(node.submit_id)
        node.state = NodeState.FAILED
        event = JobEvent(
            event_type  = EventType.INTERVENTION_ABORT,
            workflow_id = self._graph.workflow_id,
            node_id     = node_id,
            actor       = actor,
            reason      = reason,
        )
        await self._bus.publish(event)
        await self._store.record_event(event)
        await self._store.save_workflow(self._graph)

    async def skip_node(
        self,
        node_id: str,
        reason: str = "",
        actor: str = "agent",
    ) -> None:
        """
        Mark a node SKIPPED so its dependents are unblocked.
        If HTCondor jobs exist they are cancelled first.
        """
        node = self._graph.node(node_id)
        if node.submit_id:
            try:
                await self._backend.remove(node.submit_id)
            except Exception as exc:
                log.warning("Could not remove cluster %s: %s",
                            node.submit_id, exc)
        node.state = NodeState.SKIPPED
        event = JobEvent(
            event_type  = EventType.INTERVENTION_SKIP,
            workflow_id = self._graph.workflow_id,
            node_id     = node_id,
            actor       = actor,
            reason      = reason,
        )
        await self._bus.publish(event)
        await self._store.record_event(event)
        await self._store.save_workflow(self._graph)

    async def modify_node(
        self,
        node_id: str,
        bps_overrides: dict[str, str],
        reason: str = "",
        actor: str = "agent",
    ) -> None:
        """
        Abort the current submission and re-submit with different BPS
        parameters (e.g. more memory, different queue).
        """
        node = self._graph.node(node_id)
        if node.submit_id:
            await self._backend.remove(node.submit_id)
        node.bps_overrides.update(bps_overrides)
        result = await self._backend.submit(
            node.bps_yaml, self._bps_dir, overrides=node.bps_overrides
        )
        node.submit_id   = result.cluster_id
        node.schedd_name = result.schedd_name
        node.state = NodeState.SUBMITTED
        event = JobEvent(
            event_type  = EventType.INTERVENTION_MODIFY,
            workflow_id = self._graph.workflow_id,
            node_id     = node_id,
            cluster_id  = result.cluster_id,
            actor       = actor,
            reason      = reason,
            extra       = {"bps_overrides": bps_overrides},
        )
        await self._bus.publish(event)
        await self._store.record_event(event)
        await self._store.save_workflow(self._graph)
