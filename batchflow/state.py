"""
state.py — StateStore ABC and SQLite implementation.

Stores the full history of node state transitions and agent interventions.
Survives process restarts so workflows can be resumed after a crash or
deliberate pause.

Swapping to Postgres later means implementing StateStore with asyncpg;
the rest of the codebase is unaffected.
"""
from __future__ import annotations

import asyncio
import json
import logging
import sqlite3
from abc import ABC, abstractmethod
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .bus import JobEvent
from .graph import NodeState, PipelineNode, WorkflowGraph

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Abstract interface
# ---------------------------------------------------------------------------

class StateStore(ABC):
    """
    Persistent record of workflow execution history.

    All methods are async so implementations may use async DB drivers
    (asyncpg, aiosqlite) without requiring a shim.
    """

    @abstractmethod
    async def init(self) -> None:
        """Create schema / open connections.  Call once at startup."""

    @abstractmethod
    async def close(self) -> None:
        """Flush and close.  Call at shutdown."""

    @abstractmethod
    async def save_workflow(self, graph: WorkflowGraph) -> None:
        """Persist (or update) the full workflow definition."""

    @abstractmethod
    async def load_workflow(self, workflow_id: str) -> WorkflowGraph | None:
        """Reload a workflow graph including all node states."""

    @abstractmethod
    async def record_event(self, event: JobEvent) -> None:
        """Append an event to the immutable event log."""

    @abstractmethod
    async def get_events(
        self,
        workflow_id: str,
        node_id: str | None = None,
    ) -> list[JobEvent]:
        """Retrieve events, optionally filtered to one node."""

    @abstractmethod
    async def list_workflows(self) -> list[dict[str, Any]]:
        """Return summary rows for all known workflows."""


# ---------------------------------------------------------------------------
# SQLite implementation
# ---------------------------------------------------------------------------

_SCHEMA = """
CREATE TABLE IF NOT EXISTS workflows (
    workflow_id  TEXT PRIMARY KEY,
    graph_json   TEXT NOT NULL,
    created_at   TEXT NOT NULL,
    updated_at   TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS events (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    workflow_id  TEXT    NOT NULL,
    node_id      TEXT    NOT NULL,
    event_type   TEXT    NOT NULL,
    cluster_id   TEXT,
    hold_reasons TEXT,   -- JSON array
    exit_code    INTEGER,
    actor        TEXT    NOT NULL DEFAULT 'system',
    reason       TEXT    NOT NULL DEFAULT '',
    extra        TEXT,   -- JSON object
    timestamp    TEXT    NOT NULL,
    FOREIGN KEY (workflow_id) REFERENCES workflows(workflow_id)
);

CREATE INDEX IF NOT EXISTS idx_events_workflow ON events(workflow_id);
CREATE INDEX IF NOT EXISTS idx_events_node     ON events(workflow_id, node_id);
"""


class SqliteStateStore(StateStore):
    """
    SQLite-backed state store.

    SQLite's WAL mode gives safe concurrent readers; writes are
    serialised through an asyncio.Lock so multiple coroutines can
    call record_event without stepping on each other.

    Parameters
    ----------
    db_path : str | Path
        Path to the SQLite file, e.g. ``"./run/state.db"``.
        Use ``":memory:"`` for testing.
    """

    def __init__(self, db_path: str | Path = "./batchflow_state.db") -> None:
        self._path = str(db_path)
        self._conn: sqlite3.Connection | None = None
        self._write_lock = asyncio.Lock()

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def init(self) -> None:
        loop = asyncio.get_running_loop()
        self._conn = await loop.run_in_executor(None, self._open_connection)
        log.info("StateStore: opened SQLite at %s", self._path)

    def _open_connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self._path, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        conn.executescript(_SCHEMA)
        conn.commit()
        return conn

    async def close(self) -> None:
        if self._conn:
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, self._conn.close)
            self._conn = None

    # ------------------------------------------------------------------
    # Workflow persistence
    # ------------------------------------------------------------------

    async def save_workflow(self, graph: WorkflowGraph) -> None:
        now = _now()
        graph_json = _serialize_graph(graph)
        async with self._write_lock:
            await self._run(
                """INSERT INTO workflows (workflow_id, graph_json, created_at, updated_at)
                   VALUES (?, ?, ?, ?)
                   ON CONFLICT(workflow_id) DO UPDATE
                   SET graph_json=excluded.graph_json, updated_at=excluded.updated_at""",
                (graph.workflow_id, graph_json, now, now),
                commit=True,
            )

    async def load_workflow(self, workflow_id: str) -> WorkflowGraph | None:
        row = await self._fetch_one(
            "SELECT graph_json FROM workflows WHERE workflow_id=?",
            (workflow_id,),
        )
        if row is None:
            return None
        return _deserialize_graph(row["graph_json"])

    async def list_workflows(self) -> list[dict[str, Any]]:
        rows = await self._fetch_all(
            "SELECT workflow_id, created_at, updated_at FROM workflows"
            " ORDER BY created_at DESC"
        )
        return [dict(r) for r in rows]

    # ------------------------------------------------------------------
    # Event log
    # ------------------------------------------------------------------

    async def record_event(self, event: JobEvent) -> None:
        async with self._write_lock:
            await self._run(
                """INSERT INTO events
                   (workflow_id, node_id, event_type, cluster_id,
                    hold_reasons, exit_code, actor, reason, extra, timestamp)
                   VALUES (?,?,?,?,?,?,?,?,?,?)""",
                (
                    event.workflow_id,
                    event.node_id,
                    event.event_type.value,
                    event.cluster_id,
                    json.dumps(list(event.hold_reasons)),
                    event.exit_code,
                    event.actor,
                    event.reason,
                    json.dumps(event.extra),
                    event.timestamp.isoformat(),
                ),
                commit=True,
            )

    async def get_events(
        self,
        workflow_id: str,
        node_id: str | None = None,
    ) -> list[JobEvent]:
        if node_id:
            rows = await self._fetch_all(
                "SELECT * FROM events WHERE workflow_id=? AND node_id=?"
                " ORDER BY id",
                (workflow_id, node_id),
            )
        else:
            rows = await self._fetch_all(
                "SELECT * FROM events WHERE workflow_id=? ORDER BY id",
                (workflow_id,),
            )
        return [_row_to_event(r) for r in rows]

    # ------------------------------------------------------------------
    # Internal helpers — run SQLite in a thread executor so we don't
    # block the event loop.
    # ------------------------------------------------------------------

    async def _run(self, sql: str, params=(), *, commit=False) -> None:
        loop = asyncio.get_running_loop()
        conn = self._conn
        def _exec():
            conn.execute(sql, params)
            if commit:
                conn.commit()
        await loop.run_in_executor(None, _exec)

    async def _fetch_one(self, sql: str, params=()) -> sqlite3.Row | None:
        loop = asyncio.get_running_loop()
        conn = self._conn
        def _exec():
            return conn.execute(sql, params).fetchone()
        return await loop.run_in_executor(None, _exec)

    async def _fetch_all(self, sql: str, params=()) -> list[sqlite3.Row]:
        loop = asyncio.get_running_loop()
        conn = self._conn
        def _exec():
            return conn.execute(sql, params).fetchall()
        return await loop.run_in_executor(None, _exec)


# ---------------------------------------------------------------------------
# Serialisation helpers
# ---------------------------------------------------------------------------

def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _serialize_graph(graph: WorkflowGraph) -> str:
    data = {
        "workflow_id": graph.workflow_id,
        "nodes": [
            {
                "node_id":       n.node_id,
                "bps_yaml":      n.bps_yaml,
                "depends_on":    n.depends_on,
                "bps_overrides": n.bps_overrides,
                "max_restarts":  n.max_restarts,
                "metadata":      n.metadata,
                "state":         n.state.value,
                "restart_count": n.restart_count,
                "submit_id":     n.submit_id,
                "schedd_name":   n.schedd_name,
            }
            for n in graph.nodes
        ],
    }
    return json.dumps(data)


def _deserialize_graph(json_str: str) -> WorkflowGraph:
    data = json.loads(json_str)
    graph = WorkflowGraph(data["workflow_id"])
    for nd in data["nodes"]:
        node = PipelineNode(
            node_id       = nd["node_id"],
            bps_yaml      = nd["bps_yaml"],
            depends_on    = nd["depends_on"],
            bps_overrides = nd["bps_overrides"],
            max_restarts  = nd["max_restarts"],
            metadata      = nd["metadata"],
        )
        node.state         = NodeState(nd["state"])
        node.restart_count = nd["restart_count"]
        node.submit_id     = nd["submit_id"]
        node.schedd_name   = nd.get("schedd_name")
        graph.add_node(node)
    return graph


def _row_to_event(row: sqlite3.Row) -> JobEvent:
    from .bus import EventType  # avoid circular at module level
    return JobEvent(
        event_type   = EventType(row["event_type"]),
        workflow_id  = row["workflow_id"],
        node_id      = row["node_id"],
        cluster_id   = row["cluster_id"],
        timestamp    = datetime.fromisoformat(row["timestamp"]),
        hold_reasons = tuple(json.loads(row["hold_reasons"] or "[]")),
        exit_code    = row["exit_code"],
        extra        = json.loads(row["extra"] or "{}"),
        actor        = row["actor"],
        reason       = row["reason"],
    )
