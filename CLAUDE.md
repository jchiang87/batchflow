# CLAUDE.md

This file provides guidance to Claude Code when working with the **batchflow**
repository.

## Overview

**batchflow** is a generic async workflow orchestrator for HTCondor/BPS batch
systems.  It was extracted from the LSST Camera team's `eo_pipe` package and
generalised for any workflow that submits jobs via `bps submit` and monitors
them through HTCondor.

The `eo_pipe_adapter/` sub-package is a thin shim that translates the legacy
`eo_pipelines_config.yaml` format into batchflow's native `WorkflowGraph`.

## Repository Structure

```
batchflow/
├── batchflow/                    # Main package
│   ├── graph.py                  # WorkflowGraph, PipelineNode, NodeState
│   ├── loader.py                 # YAML workflow loader
│   ├── bus.py                    # EventBus, JobEvent — async pub/sub
│   ├── state.py                  # StateStore ABC + SqliteStateStore
│   ├── monitor.py                # HTCondorMonitor, WakeStrategy ABC
│   ├── classifier.py             # ErrorClassifier — transient/fatal heuristics
│   ├── agent.py                  # AgentHandler, InterventionActions, transports
│   ├── runner.py                 # WorkflowRunner — async DAG scheduler
│   ├── workspace.py              # WorkspaceManager — versioned bps/ copy
│   ├── cli.py                    # batchflow CLI (submit/status/resume/intervene)
│   └── backends/
│       ├── bps.py                # SubmissionBackend ABC, BpsBackend, MockBackend
│       └── postgres.py           # PostgresStateStore (requires asyncpg extra)
├── eo_pipe_adapter/              # LSST eo_pipe legacy config adapter
│   ├── adapter.py                # load_eo_workflow(), LSST error patterns
│   └── cli.py                    # eo-pipe CLI (submit/resume/status/intervene)
├── tests/
│   └── test_batchflow.py         # 50 tests; no HTCondor required
├── examples/
│   ├── b_protocol.yaml           # Native batchflow DAG for B-protocol
│   └── b_protocol_dag.yaml       # Same in eo_pipe_adapter extended format
└── pyproject.toml
```

## Installation

```bash
pip install -e ".[dev]"           # core + test dependencies
pip install -e ".[dev,webhook]"   # add aiohttp for WebhookTransport
pip install -e ".[dev,postgres]"  # add asyncpg for PostgresStateStore
pip install -e ".[dev,inotify]"   # add watchfiles for InotifyWakeStrategy
```

## Running Tests

```bash
python -m pytest tests/ -v          # all 50 tests (~2s)
python -m pytest tests/ -k runner   # just runner integration tests
python -m pytest tests/ -k adapter  # just eo_pipe adapter tests
```

**All tests run without HTCondor.** The `MockBackend` handles submission;
events are injected directly onto the `EventBus`.

## Architecture: Key Concepts

### Event-driven execution

The runner never polls graph state on a timer.  Instead:

1. `HTCondorMonitor` wakes on `TimerWakeStrategy` (or `InotifyWakeStrategy`
   when inotify is available), queries `condor_q`, and publishes `JobEvent`
   objects to the `EventBus`.
2. The `EventBus` fans each event out to per-subscriber `asyncio.Queue`
   instances — the runner, state store, and agent handler each get their own
   queue, so a slow agent webhook never stalls the scheduler.
3. `WorkflowRunner._scheduler_loop` consumes its queue and transitions graph
   state.  After each `NODE_COMPLETE` it calls `_submit_ready_nodes()`, which
   queries `WorkflowGraph.ready_nodes()` to find newly-unblocked nodes.

### Stall handling

When `WorkflowGraph.is_stalled()` is true (no nodes running, some blocked),
the runner publishes `WORKFLOW_STALLED` and parks on `_intervention_event` —
an `asyncio.Event` set by any `INTERVENTION_*` event.  A stalled production
run consumes no CPU until the agent calls `InterventionActions`.

Use `stall_timeout=N` in tests to make the runner return `RunOutcome.STALLED`
after N seconds rather than waiting forever.

### Resume

`WorkflowRunner._maybe_resume()` checks the `StateStore` for a saved graph
on startup.  If found, node states are merged back into the live graph and
monitors are re-attached to any clusters that were in-flight.  The `resume`
CLI command does nothing special — it just calls `runner.run()` with the
reloaded graph.

### Pluggable backends

| Concern | Current | Future |
|---|---|---|
| State persistence | `SqliteStateStore` | `PostgresStateStore` (see `backends/postgres.py`) |
| Monitor wake | `TimerWakeStrategy` (60s) | `InotifyWakeStrategy` (watchfiles) |
| Submission | `BpsBackend` | Any `SubmissionBackend` subclass |
| Notifications | `StdoutTransport` | `WebhookTransport`, `CallbackTransport` |

Swap any backend by passing a different instance to `WorkflowRunner` or
`AgentHandler`.  The rest of the codebase is unaffected.

## Adding a New Pipeline to eo_pipe

With the **native batchflow format** (`examples/b_protocol.yaml`):

```yaml
nodes:
  - id: my_new_task
    bps_yaml: bps_eoMyNewTask.yaml
    depends_on: [ptc]          # or [] for a root node
    max_restarts: 3
```

With the **legacy adapter format** (`examples/b_protocol_dag.yaml`):

Add the YAML filename to the `pipelines` list and, if it depends on other
nodes, add an entry to the `dependencies` dict.  `load_eo_workflow()` will
convert the filename to a node_id automatically
(`bps_eoMyNewTask.yaml` → `eo_my_new_task`).

## Adding LSST-Specific Error Patterns

Extend `eo_pipe_adapter/adapter.py`:

```python
LSST_TRANSIENT_PATTERNS = [
    ...
    r"MyNewTransientError",
]
LSST_FATAL_PATTERNS = [
    ...
    r"MyNewFatalError",
]
```

Or pass a site-specific `error_patterns.yaml` to `ErrorClassifier`:

```yaml
transient:
  - pattern: "MyNewTransientError"
fatal:
  - pattern: "MyNewFatalError"
```

## CLI Quick Reference

```bash
# Native batchflow
batchflow submit workflow.yaml --work-dir ./run_001
batchflow status --work-dir ./run_001
batchflow resume --work-dir ./run_001 --auto-restart
batchflow intervene restart <node_id> --work-dir ./run_001
batchflow intervene skip   <node_id> --reason "known bad data"
batchflow intervene abort  <node_id>

# eo_pipe legacy adapter
eo-pipe submit b_protocol --config data/eo_pipelines_config.yaml
eo-pipe status  --work-dir ./run_001
eo-pipe resume  --work-dir ./run_001
eo-pipe intervene restart cp_ptc --work-dir ./run_001
```

## Environment Variables (for eo_pipe runs)

| Variable | Description |
|---|---|
| `BUTLER_CONFIG` | Path to Butler configuration |
| `INSTRUMENT_NAME` | `LSSTCam`, `LSST-TS8`, or `LATISS` |
| `EO_PIPE_DIR` | eo_pipe installation directory (source of bps/) |
| `EO_PIPE_INCOLLECTION` | Input Butler collection |
| `DATASET_LABEL` | Label for output datasets |
| `BATCHFLOW_WEBHOOK` | Optional URL for agent notifications |

## Key Design Decisions

**Why asyncio over threads?**
The fan-out/join DAG scheduling and monitor wake cycle are naturally expressed
as coroutines.  `asyncio.gather` handles parallel node submission cleanly;
`asyncio.Event` gives zero-cost stall waiting.

**Why push notifications over pull?**
Runs span multiple days.  A pull model (agent polls `status()` every N
minutes) is wasteful and adds latency to failure response.  The push model
via `AgentHandler` + `NotificationTransport` delivers events within one
monitor wake cycle (~60s by default).

**Why SQLite now, Postgres later?**
SQLite is zero-config and sufficient for single-user proof-of-concept.
`StateStore` is an ABC; `PostgresStateStore` in `backends/postgres.py` is a
complete implementation requiring only `asyncpg`.  Switching is one
constructor argument.

**Why is stall_timeout a constructor parameter, not a class default?**
Production runs should never time out waiting for agent intervention — the
workflow should park indefinitely.  Tests need deterministic completion.
Making it explicit prevents accidental timeout in production due to a
forgotten default.
