# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

**batchflow** is a generic async workflow orchestrator for HTCondor/BPS batch systems. It submits jobs via `bps submit` and monitors them through HTCondor.

## Installation

```bash
pip install -e ".[dev]"           # core + test dependencies
pip install -e ".[dev,webhook]"   # add aiohttp for WebhookTransport
pip install -e ".[dev,postgres]"  # add asyncpg for PostgresStateStore
pip install -e ".[dev,inotify]"   # add watchfiles for InotifyWakeStrategy
```

## Running Tests

```bash
python -m pytest tests/ -v                        # all tests (~2s)
python -m pytest tests/ -k runner                 # runner integration tests
python -m pytest tests/ -k "test_simple_linear"   # single test by name
```

`asyncio_mode = "auto"` is set in `pyproject.toml`, so async test functions require no `@pytest.mark.asyncio` decorator. **All tests run without HTCondor** — `MockBackend` handles submission and events are injected directly onto the `EventBus`. There is no linting configuration in this repo.

## Architecture: Key Concepts

### Event-driven execution

The runner never polls graph state on a timer. Instead:

1. `HTCondorMonitor` wakes on `TimerWakeStrategy` (or `InotifyWakeStrategy` when available), queries `condor_q`, and publishes `JobEvent` objects to the `EventBus`.
2. The `EventBus` fans each event out to per-subscriber `asyncio.Queue` instances — the runner, state store, and agent handler each get their own queue, so a slow agent webhook never stalls the scheduler.
3. `WorkflowRunner._scheduler_loop` consumes its queue and transitions graph state. After each `NODE_COMPLETE` it calls `_submit_ready_nodes()`, which queries `WorkflowGraph.ready_nodes()` to find newly-unblocked nodes.

### Node state machine

`NodeState` transitions: `PENDING` → `READY` → `SUBMITTED` → `RUNNING` → `SUCCEEDED` | `FAILED` | `HELD` | `SKIPPED`.

`TERMINAL_STATES = {SUCCEEDED, SKIPPED}` — counts as done for dependency resolution. `BLOCKED_STATES = {FAILED, HELD}` — blocks dependents until agent intervention.

### Stall handling

When `WorkflowGraph.is_stalled()` is true (nothing running, something blocked), the runner publishes `WORKFLOW_STALLED` and parks on `_intervention_event` — an `asyncio.Event` set by any `INTERVENTION_*` event. A stalled production run consumes no CPU until the agent calls `InterventionActions`.

Use `stall_timeout=N` in tests to make the runner return `RunOutcome.STALLED` after N seconds rather than waiting forever.

### Resume

`WorkflowRunner._maybe_resume()` checks the `StateStore` for a saved graph on startup. If found, node states are merged back into the live graph and monitors are re-attached to any in-flight clusters. The `resume` CLI command does nothing special — it just calls `runner.run()` with the reloaded graph.

### Pluggable backends

| Concern | Default | Alternative |
|---|---|---|
| State persistence | `SqliteStateStore` | `PostgresStateStore` (`backends/postgres.py`) |
| Monitor wake | `TimerWakeStrategy` (60s) | `InotifyWakeStrategy` (watchfiles) |
| Submission | `BpsBackend` | Any `SubmissionBackend` subclass |
| Notifications | `StdoutTransport` | `WebhookTransport`, `CallbackTransport` |

Swap any backend by passing a different instance to `WorkflowRunner` or `AgentHandler`.

## Adding a New Pipeline Node

```yaml
nodes:
  - id: my_new_task
    bps_yaml: bps_myNewTask.yaml
    depends_on: [some_node]    # or [] for a root node
    max_restarts: 3
```

## Adding Custom Error Patterns

Pass a site-specific `error_patterns.yaml` to `ErrorClassifier`:

```yaml
transient:
  - pattern: "MyNewTransientError"
fatal:
  - pattern: "MyNewFatalError"
```

## CLI Quick Reference

```bash
batchflow submit workflow.yaml --work-dir ./run_001
batchflow status --work-dir ./run_001
batchflow resume --work-dir ./run_001 --auto-restart
batchflow intervene restart <node_id> --work-dir ./run_001
batchflow intervene skip   <node_id> --reason "known bad data"
batchflow intervene abort  <node_id>
```

## Environment Variables

| Variable | Description |
|---|---|
| `BATCHFLOW_WEBHOOK` | Optional URL for agent notifications |

## Key Design Decisions

**Why asyncio over threads?** The fan-out/join DAG scheduling and monitor wake cycle are naturally expressed as coroutines. `asyncio.gather` handles parallel node submission cleanly; `asyncio.Event` gives zero-cost stall waiting.

**Why push notifications over pull?** Runs span multiple days. A pull model wastes resources and adds latency to failure response. The push model via `AgentHandler` + `NotificationTransport` delivers events within one monitor wake cycle (~60s).

**Why SQLite now, Postgres later?** SQLite is zero-config for single-user use. `StateStore` is an ABC; switching to `PostgresStateStore` requires only changing one constructor argument.

**Why is `stall_timeout` a constructor parameter, not a class default?** Production runs should never time out waiting for agent intervention. Tests need deterministic completion. Making it explicit prevents accidental timeout in production.
