"""
batchflow/backends/postgres.py — PostgreSQL StateStore implementation.

Drop-in replacement for SqliteStateStore when the deployment scales to
multiple simultaneous users or requires shared access to workflow state
across machines.

Requirements
------------
    pip install batchflow[postgres]   # installs asyncpg

Schema is identical to the SQLite version so migration is:
    pg_dump <sqlite schema> | psql <postgres url>
    # or use alembic with the schema in state.py as the source of truth.

Usage
-----
    from batchflow.backends.postgres import PostgresStateStore

    store = PostgresStateStore(
        dsn="postgresql://user:pass@host:5432/batchflow"
    )
    await store.init()
    # ... use identically to SqliteStateStore ...
    await store.close()

Connection pooling
------------------
asyncpg maintains a pool internally.  The default min/max pool sizes are
conservative (1/5); tune ``min_size`` and ``max_size`` for your load.
High-concurrency deployments (many simultaneous workflows) should increase
``max_size`` to avoid pool exhaustion during burst submission windows.

Transactions
------------
``record_event`` and ``save_workflow`` each run in their own transaction.
For multi-step operations (e.g. save workflow + record event atomically)
use the ``transaction()`` context manager exposed on the pool.

Notes on differences from SQLite
---------------------------------
- asyncpg uses ``$1, $2, ...`` placeholders instead of ``?``.
- asyncpg returns ``asyncpg.Record`` objects (dict-like) instead of
  ``sqlite3.Row``; the serialisation helpers handle both.
- ``ON CONFLICT ... DO UPDATE`` syntax is standard SQL and works the same.
- There is no ``PRAGMA journal_mode`` equivalent; Postgres handles
  concurrent access natively.
- AUTOINCREMENT → SERIAL (or BIGSERIAL for very large event logs).
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any

from ..bus import EventType, JobEvent
from ..graph import NodeState, PipelineNode, WorkflowGraph
from ..state import StateStore, _now, _serialize_graph, _deserialize_graph

log = logging.getLogger(__name__)

_SCHEMA = """
CREATE TABLE IF NOT EXISTS workflows (
    workflow_id  TEXT PRIMARY KEY,
    graph_json   TEXT NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS events (
    id           BIGSERIAL PRIMARY KEY,
    workflow_id  TEXT        NOT NULL REFERENCES workflows(workflow_id),
    node_id      TEXT        NOT NULL,
    event_type   TEXT        NOT NULL,
    cluster_id   TEXT,
    hold_reasons JSONB,
    exit_code    INTEGER,
    actor        TEXT        NOT NULL DEFAULT 'system',
    reason       TEXT        NOT NULL DEFAULT '',
    extra        JSONB,
    timestamp    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_events_workflow ON events(workflow_id);
CREATE INDEX IF NOT EXISTS idx_events_node     ON events(workflow_id, node_id);
"""


class PostgresStateStore(StateStore):
    """
    PostgreSQL-backed StateStore using asyncpg.

    Parameters
    ----------
    dsn : str
        asyncpg connection string, e.g.
        ``"postgresql://user:pass@localhost/batchflow"``.
    min_size : int
        Minimum pool connections (default 1).
    max_size : int
        Maximum pool connections (default 5).
    """

    def __init__(
        self,
        dsn: str,
        min_size: int = 1,
        max_size: int = 5,
    ) -> None:
        self._dsn      = dsn
        self._min_size = min_size
        self._max_size = max_size
        self._pool     = None   # asyncpg.Pool, set in init()

    async def init(self) -> None:
        try:
            import asyncpg  # type: ignore
        except ImportError:
            raise ImportError(
                "PostgresStateStore requires asyncpg: "
                "pip install batchflow[postgres]"
            )
        self._pool = await asyncpg.create_pool(
            self._dsn,
            min_size=self._min_size,
            max_size=self._max_size,
        )
        async with self._pool.acquire() as conn:
            await conn.execute(_SCHEMA)
        log.info("PostgresStateStore: connected to %s", self._dsn)

    async def close(self) -> None:
        if self._pool:
            await self._pool.close()
            self._pool = None

    # ------------------------------------------------------------------
    # Workflow persistence
    # ------------------------------------------------------------------

    async def save_workflow(self, graph: WorkflowGraph) -> None:
        now = datetime.now(timezone.utc)
        graph_json = _serialize_graph(graph)
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO workflows (workflow_id, graph_json, created_at, updated_at)
                VALUES ($1, $2, $3, $3)
                ON CONFLICT (workflow_id) DO UPDATE
                SET graph_json = EXCLUDED.graph_json,
                    updated_at = EXCLUDED.updated_at
                """,
                graph.workflow_id, graph_json, now,
            )

    async def load_workflow(self, workflow_id: str) -> WorkflowGraph | None:
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT graph_json FROM workflows WHERE workflow_id = $1",
                workflow_id,
            )
        if row is None:
            return None
        return _deserialize_graph(row["graph_json"])

    async def list_workflows(self) -> list[dict[str, Any]]:
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT workflow_id, created_at, updated_at "
                "FROM workflows ORDER BY created_at DESC"
            )
        return [
            {
                "workflow_id": r["workflow_id"],
                "created_at":  r["created_at"].isoformat(),
                "updated_at":  r["updated_at"].isoformat(),
            }
            for r in rows
        ]

    # ------------------------------------------------------------------
    # Event log
    # ------------------------------------------------------------------

    async def record_event(self, event: JobEvent) -> None:
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO events
                  (workflow_id, node_id, event_type, cluster_id,
                   hold_reasons, exit_code, actor, reason, extra, timestamp)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
                """,
                event.workflow_id,
                event.node_id,
                event.event_type.value,
                event.cluster_id,
                json.dumps(list(event.hold_reasons)),   # stored as JSONB
                event.exit_code,
                event.actor,
                event.reason,
                json.dumps(event.extra),
                event.timestamp,
            )

    async def get_events(
        self,
        workflow_id: str,
        node_id: str | None = None,
    ) -> list[JobEvent]:
        async with self._pool.acquire() as conn:
            if node_id:
                rows = await conn.fetch(
                    "SELECT * FROM events "
                    "WHERE workflow_id=$1 AND node_id=$2 ORDER BY id",
                    workflow_id, node_id,
                )
            else:
                rows = await conn.fetch(
                    "SELECT * FROM events WHERE workflow_id=$1 ORDER BY id",
                    workflow_id,
                )
        return [_pg_row_to_event(r) for r in rows]


def _pg_row_to_event(row) -> JobEvent:
    hold_reasons = row["hold_reasons"]
    if isinstance(hold_reasons, str):
        hold_reasons = json.loads(hold_reasons)
    hold_reasons = hold_reasons or []

    extra = row["extra"]
    if isinstance(extra, str):
        extra = json.loads(extra)
    extra = extra or {}

    ts = row["timestamp"]
    if isinstance(ts, str):
        ts = datetime.fromisoformat(ts)

    return JobEvent(
        event_type   = EventType(row["event_type"]),
        workflow_id  = row["workflow_id"],
        node_id      = row["node_id"],
        cluster_id   = row["cluster_id"],
        timestamp    = ts,
        hold_reasons = tuple(hold_reasons),
        exit_code    = row["exit_code"],
        extra        = extra,
        actor        = row["actor"],
        reason       = row["reason"],
    )
