"""
graph.py — WorkflowGraph and PipelineNode.

A workflow is a DAG of PipelineNodes.  Each node names a BPS YAML file
and declares which other nodes must complete before it may run.  The
graph resolves the execution order and exposes ready/blocked state that
the runner consults after every job-completion event.
"""
from __future__ import annotations

import re
from collections import deque
from dataclasses import dataclass, field
from enum import Enum
from typing import Iterable


class NodeState(str, Enum):
    PENDING   = "PENDING"    # waiting on dependencies
    READY     = "READY"      # dependencies met, not yet submitted
    SUBMITTED = "SUBMITTED"  # bps submit called, awaiting HTCondor
    RUNNING   = "RUNNING"    # at least one HTCondor job active
    SUCCEEDED = "SUCCEEDED"
    FAILED    = "FAILED"
    HELD      = "HELD"       # one or more jobs held in HTCondor
    SKIPPED   = "SKIPPED"    # intentionally bypassed by agent


# States that count as "done" for dependency resolution.
TERMINAL_STATES = {NodeState.SUCCEEDED, NodeState.SKIPPED}
# States that block dependents indefinitely without intervention.
BLOCKED_STATES  = {NodeState.FAILED, NodeState.HELD}


@dataclass
class PipelineNode:
    """
    A single pipeline step within a workflow.

    Parameters
    ----------
    node_id : str
        Unique identifier within the workflow, e.g. ``"read_noise"``.
    bps_yaml : str
        Filename of the BPS submission YAML (relative to the local bps/
        directory), e.g. ``"bps_eoReadNoise.yaml"``.
    depends_on : list[str]
        IDs of nodes that must reach a terminal state before this node
        becomes READY.  An empty list means the node is a root and will
        be submitted immediately.
    bps_overrides : dict
        Key/value pairs that the submission backend may merge into the
        BPS YAML at submit time (useful for agent-driven retries with
        different resource requests).
    max_restarts : int
        Maximum number of agent-initiated restarts before the node is
        considered permanently failed.
    metadata : dict
        Arbitrary user data carried through to AgentNotifications.
    """
    node_id:       str
    bps_yaml:      str
    depends_on:    list[str]  = field(default_factory=list)
    bps_overrides: dict       = field(default_factory=dict)
    max_restarts:  int        = 3
    metadata:      dict       = field(default_factory=dict)

    # Runtime state — managed by WorkflowRunner, not set by users.
    state:          NodeState = field(default=NodeState.PENDING, init=False)
    restart_count:  int       = field(default=0, init=False)
    submit_id:      str | None = field(default=None, init=False)  # HTCondor cluster id
    schedd_name:    str | None = field(default=None, init=False)  # FQDN of submission schedd

    def __post_init__(self):
        if not re.match(r'^[A-Za-z0-9_\-]+$', self.node_id):
            raise ValueError(
                f"node_id {self.node_id!r} must be alphanumeric/underscore/hyphen"
            )

    @property
    def is_terminal(self) -> bool:
        return self.state in TERMINAL_STATES

    @property
    def is_restartable(self) -> bool:
        return (self.state in BLOCKED_STATES
                and self.restart_count < self.max_restarts)


class WorkflowGraph:
    """
    A validated DAG of PipelineNodes with topological scheduling support.

    Usage
    -----
    >>> g = WorkflowGraph("b_protocol")
    >>> g.add_node(PipelineNode("read_noise", "bps_eoReadNoise.yaml"))
    >>> g.add_node(PipelineNode("ptc", "bps_cpPtc.yaml",
    ...                         depends_on=["read_noise"]))
    >>> g.validate()
    >>> list(g.ready_nodes())
    [PipelineNode(node_id='read_noise', ...)]
    """

    def __init__(self, workflow_id: str):
        self.workflow_id = workflow_id
        self._nodes: dict[str, PipelineNode] = {}

    # ------------------------------------------------------------------
    # Graph construction
    # ------------------------------------------------------------------

    def add_node(self, node: PipelineNode) -> None:
        if node.node_id in self._nodes:
            raise ValueError(f"Duplicate node_id: {node.node_id!r}")
        self._nodes[node.node_id] = node

    def node(self, node_id: str) -> PipelineNode:
        try:
            return self._nodes[node_id]
        except KeyError:
            raise KeyError(f"No node with id {node_id!r}") from None

    @property
    def nodes(self) -> list[PipelineNode]:
        return list(self._nodes.values())

    # ------------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------------

    def validate(self) -> None:
        """
        Raise ValueError if the graph has unknown dependency references
        or contains a cycle.
        """
        for node in self._nodes.values():
            for dep in node.depends_on:
                if dep not in self._nodes:
                    raise ValueError(
                        f"Node {node.node_id!r} depends on unknown node {dep!r}"
                    )
        if self._has_cycle():
            raise ValueError("Workflow graph contains a cycle")

    def _has_cycle(self) -> bool:
        """Kahn's algorithm — returns True if a cycle exists."""
        in_degree = {nid: 0 for nid in self._nodes}
        for node in self._nodes.values():
            for dep in node.depends_on:
                in_degree[node.node_id] += 1

        queue = deque(nid for nid, d in in_degree.items() if d == 0)
        visited = 0
        while queue:
            nid = queue.popleft()
            visited += 1
            for other in self._nodes.values():
                if nid in other.depends_on:
                    in_degree[other.node_id] -= 1
                    if in_degree[other.node_id] == 0:
                        queue.append(other.node_id)
        return visited != len(self._nodes)

    # ------------------------------------------------------------------
    # Runtime scheduling queries
    # ------------------------------------------------------------------

    def ready_nodes(self) -> Iterable[PipelineNode]:
        """
        Yield nodes whose dependencies are all terminal and which are
        currently in PENDING state (i.e. not yet submitted or running).
        """
        for node in self._nodes.values():
            if node.state != NodeState.PENDING:
                continue
            if all(
                self._nodes[dep].state in TERMINAL_STATES
                for dep in node.depends_on
            ):
                yield node

    def mark_ready(self, node_id: str) -> None:
        self._nodes[node_id].state = NodeState.READY

    def is_complete(self) -> bool:
        return all(n.is_terminal for n in self._nodes.values())

    def is_stalled(self) -> bool:
        """
        True when no node is running/submitted and at least one non-terminal
        node exists — meaning nothing can make forward progress without
        intervention.
        """
        active = {NodeState.SUBMITTED, NodeState.RUNNING}
        has_active = any(n.state in active for n in self._nodes.values())
        has_incomplete = any(not n.is_terminal for n in self._nodes.values())
        return has_incomplete and not has_active

    def summary(self) -> dict[str, int]:
        """Return a count of nodes in each state."""
        counts: dict[str, int] = {}
        for n in self._nodes.values():
            counts[n.state.value] = counts.get(n.state.value, 0) + 1
        return counts
