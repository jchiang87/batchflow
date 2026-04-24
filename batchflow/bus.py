"""
bus.py — EventBus and JobEvent types.

The EventBus is the single communication channel between the HTCondor
monitor and all consumers (WorkflowRunner, StateStore, AgentHandler).
Each subscriber gets its own asyncio.Queue so a slow consumer never
stalls a fast one.

Design notes
------------
- Events are immutable dataclasses; consumers may not mutate them.
- The bus is intentionally dumb — it routes, never filters or transforms.
- Queue maxsize=0 means unbounded.  For multi-day runs this is fine; the
  queue will rarely hold more than a handful of events at once.
"""
from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import AsyncIterator

log = logging.getLogger(__name__)


class EventType(str, Enum):
    # HTCondor-level transitions
    JOB_SUBMITTED  = "JOB_SUBMITTED"
    JOB_RUNNING    = "JOB_RUNNING"
    JOB_HELD       = "JOB_HELD"
    JOB_RELEASED   = "JOB_RELEASED"
    JOB_FAILED     = "JOB_FAILED"
    JOB_SUCCEEDED  = "JOB_SUCCEEDED"
    JOB_ABORTED    = "JOB_ABORTED"

    # Node-level (derived by the runner from job-level events)
    NODE_COMPLETE   = "NODE_COMPLETE"
    NODE_FAILED     = "NODE_FAILED"
    NODE_HELD       = "NODE_HELD"
    NODE_SKIPPED    = "NODE_SKIPPED"

    # Workflow-level
    WORKFLOW_COMPLETE = "WORKFLOW_COMPLETE"
    WORKFLOW_STALLED  = "WORKFLOW_STALLED"

    # Agent actions (published so StateStore can log them)
    INTERVENTION_RESTART  = "INTERVENTION_RESTART"
    INTERVENTION_ABORT    = "INTERVENTION_ABORT"
    INTERVENTION_SKIP     = "INTERVENTION_SKIP"
    INTERVENTION_MODIFY   = "INTERVENTION_MODIFY"


@dataclass(frozen=True)
class JobEvent:
    """
    An immutable event emitted by the monitor or runner.

    Parameters
    ----------
    event_type : EventType
    workflow_id : str
    node_id : str
        The PipelineNode this event concerns.
    cluster_id : str | None
        HTCondor cluster ID (``"1234.0"`` format).  None for synthetic
        workflow-level events.
    timestamp : datetime
        UTC timestamp; defaults to now.
    hold_reasons : tuple[str, ...]
        Non-empty only for JOB_HELD / NODE_HELD events.
    exit_code : int | None
        Set for JOB_SUCCEEDED / JOB_FAILED.
    extra : dict
        Arbitrary additional metadata (e.g. condor ClassAd fields).
    actor : str
        For INTERVENTION_* events: who triggered the action.
    reason : str
        For INTERVENTION_* events: human-readable rationale.
    """
    event_type:   EventType
    workflow_id:  str
    node_id:      str
    cluster_id:   str | None              = None
    timestamp:    datetime                = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )
    hold_reasons: tuple[str, ...]         = field(default_factory=tuple)
    exit_code:    int | None              = None
    extra:        dict                    = field(default_factory=dict)
    actor:        str                     = "system"
    reason:       str                     = ""


class EventBus:
    """
    Async pub/sub bus backed by per-subscriber asyncio.Queue instances.

    Typical usage
    -------------
    >>> bus = EventBus()
    >>> q = bus.subscribe("runner")
    >>> await bus.publish(event)          # non-blocking fan-out
    >>> event = await q.get()
    """

    def __init__(self) -> None:
        self._queues: dict[str, asyncio.Queue[JobEvent]] = {}
        self._lock = asyncio.Lock()

    def subscribe(self, subscriber_id: str) -> asyncio.Queue[JobEvent]:
        """
        Register a subscriber and return its dedicated queue.
        Call before the event loop starts producing events.
        """
        if subscriber_id in self._queues:
            raise ValueError(f"Subscriber {subscriber_id!r} already registered")
        q: asyncio.Queue[JobEvent] = asyncio.Queue()
        self._queues[subscriber_id] = q
        log.debug("EventBus: subscriber %r registered", subscriber_id)
        return q

    async def publish(self, event: JobEvent) -> None:
        """Fan event out to all subscriber queues (never blocks)."""
        log.debug("EventBus: publishing %s for node %r",
                  event.event_type.value, event.node_id)
        for sid, q in self._queues.items():
            await q.put(event)

    async def events(
        self,
        queue: asyncio.Queue[JobEvent],
        *,
        stop_event: asyncio.Event | None = None,
    ) -> AsyncIterator[JobEvent]:
        """
        Async generator that yields events from *queue* until *stop_event*
        is set (or forever if stop_event is None).

        Usage
        -----
        >>> async for event in bus.events(my_queue, stop_event=done):
        ...     handle(event)
        """
        while True:
            if stop_event is not None and stop_event.is_set():
                break
            try:
                event = await asyncio.wait_for(queue.get(), timeout=1.0)
                yield event
            except asyncio.TimeoutError:
                continue
