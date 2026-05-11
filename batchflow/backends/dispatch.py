"""
backends/dispatch.py — DispatchBackend

Routes every backend call to the correct sub-backend based on node.node_type.

Usage
-----
    backend = DispatchBackend({
        "bps":   BpsHtcondorBackend(bps_dir=ws.bps_dir),
        "shell": ShellBackend(),
    })
    runner = WorkflowRunner(graph=g, backend=backend, ...)

make_node_runner() dispatches by node.node_type and also populates the
internal submit_id → node_type map, so release_held / remove / restart
work correctly even after a resume (where _owners would otherwise be empty).
"""
from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING

from .bps import SubmissionBackend, SubmissionResult

if TYPE_CHECKING:
    from ..graph import PipelineNode
    from ..bus import EventBus
    from ..monitor import AbstractNodeRunner

log = logging.getLogger(__name__)


class DispatchBackend(SubmissionBackend):
    """
    Fan-out backend that routes to sub-backends by node_type.

    Parameters
    ----------
    backends : dict[str, SubmissionBackend]
        Mapping of node_type string to the backend that handles it.
        E.g. ``{"bps": BpsHtcondorBackend(...), "shell": ShellBackend()}``.
    """

    def __init__(self, backends: dict[str, SubmissionBackend]) -> None:
        self._backends = backends
        self._owners: dict[str, str] = {}  # submit_id → node_type

    def _get(self, node_type: str) -> SubmissionBackend:
        backend = self._backends.get(node_type)
        if backend is None:
            raise KeyError(
                f"No backend registered for node_type {node_type!r}. "
                f"Registered types: {list(self._backends)}"
            )
        return backend

    def _owner(self, submit_id: str) -> SubmissionBackend:
        node_type = self._owners.get(submit_id)
        if node_type is None:
            raise KeyError(
                f"No backend owner found for submit_id {submit_id!r}. "
                "The submit_id may belong to a previous session; call "
                "make_node_runner() first to re-register it."
            )
        return self._get(node_type)

    async def submit(
        self,
        node: "PipelineNode",
        *,
        log_dir=None,
    ) -> SubmissionResult:
        backend = self._get(node.node_type)
        result  = await backend.submit(node, log_dir=log_dir)
        self._owners[result.submit_id] = node.node_type
        return result

    def make_node_runner(
        self,
        node: "PipelineNode",
        bus: "EventBus",
        stop_event: asyncio.Event,
        *,
        workflow_id: str = "",
        wake_strategy: object = None,
    ) -> "AbstractNodeRunner":
        # Populate _owners here so that release_held/remove/restart work
        # correctly after a resume (when submit() was not called this session).
        if node.submit_id:
            self._owners[node.submit_id] = node.node_type
        return self._get(node.node_type).make_node_runner(
            node, bus, stop_event,
            workflow_id   = workflow_id,
            wake_strategy = wake_strategy,
        )

    async def release_held(self, submit_id: str) -> None:
        await self._owner(submit_id).release_held(submit_id)

    async def remove(self, submit_id: str) -> None:
        await self._owner(submit_id).remove(submit_id)

    async def restart(self, submit_id: str) -> SubmissionResult:
        backend  = self._owner(submit_id)
        result   = await backend.restart(submit_id)
        # Register the new submit_id under the same node_type.
        self._owners[result.submit_id] = self._owners[submit_id]
        return result
