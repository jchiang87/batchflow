# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

**batchflow** is a generic async workflow orchestrator supporting multiple backends: HTCondor/BPS (`BpsHtcondorBackend`), Parsl-based BPS (`BpsParslBackend`), and arbitrary blocking shell scripts (`ShellBackend`). Mixed workflows are supported via `DispatchBackend`.

## Installation

```bash
pip install -e ".[dev]"           # core + test dependencies (includes htcondor)
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

1. `HTCondorNodeRunner` (formerly `HTCondorMonitor`) wakes on `TimerWakeStrategy` (or `InotifyWakeStrategy` when available), queries the schedd via the `htcondor` Python bindings, and publishes `JobEvent` objects to the `EventBus`.
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
| Submission (HTCondor) | `BpsHtcondorBackend` (alias: `BpsBackend`) | — |
| Submission (Parsl) | `BpsParslBackend` | — |
| Submission (shell) | `ShellBackend` | — |
| Submission (mixed) | `DispatchBackend({...})` | Any `SubmissionBackend` subclass |
| Notifications | `StdoutTransport` | `WebhookTransport`, `CallbackTransport` |
| Agent callback | `make_code_agent_callback` (smolagents `CodeAgent`) | `CodeAgentRunner(use_agent=False)` for direct tool dispatch |

Swap any backend by passing a different instance to `WorkflowRunner` or `AgentHandler`.

### Multi-schedd support

The cluster has one HTCondor schedd per interactive node (e.g. `sdfiana011`–`sdfiana033`). `BpsHtcondorBackend.submit()` captures the submission schedd's FQDN from `htcondor.Schedd().location.address` and stores it in `PipelineNode.submit_location`. `HTCondorNodeRunner` uses `htcondor.Collector().locate()` to reconnect to the correct schedd, so the agent can run on any node regardless of where `bps submit` was called. `submit_location` is persisted in the `StateStore` so resume works correctly after restart.

## Adding a New Pipeline Node

BPS node (default `node_type: bps`):
```yaml
workflow: my_workflow
bps_backend: htcondor   # "htcondor" (default) or "parsl"

nodes:
  - id: my_new_task
    bps_yaml: bps_myNewTask.yaml
    depends_on: [some_node]    # or [] for a root node
    max_restarts: 3
```

Shell node:
```yaml
workflow: my_workflow
nodes:
  - id: prep
    node_type: shell
    command: /path/to/prep.sh
    depends_on: []
```

Mixed workflow (shell + BPS via Parsl):
```yaml
workflow: my_workflow
bps_backend: parsl

nodes:
  - id: prep
    node_type: shell
    command: /path/to/prep.sh
  - id: process
    bps_yaml: bps_process.yaml
    depends_on: [prep]
```

The CLI always uses `DispatchBackend` internally, routing `node_type: bps` nodes to the selected BPS backend and `node_type: shell` nodes to `ShellBackend`.

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
| `DONT_USE_AGENT` | Set to any value to disable the smolagents `CodeAgent` and call `RestartNodeTool` directly (useful for testing) |
| `ANTHROPIC_DEFAULT_MODEL` | Override the default model ID (`claude-sonnet-4-6`) used by `CodeAgentRunner` |
| `ANTHROPIC_DEFAULT_SONNET_MODEL` | Fine-grained override for the Sonnet model (takes precedence over `ANTHROPIC_DEFAULT_MODEL` if set in `~/.claude/settings.json` env) |

## Key Design Decisions

**Why asyncio over threads?** The fan-out/join DAG scheduling and monitor wake cycle are naturally expressed as coroutines. `asyncio.gather` handles parallel node submission cleanly; `asyncio.Event` gives zero-cost stall waiting.

**Why push notifications over pull?** Runs span multiple days. A pull model wastes resources and adds latency to failure response. The push model via `AgentHandler` + `NotificationTransport` delivers events within one monitor wake cycle (~60s).

**Why SQLite now, Postgres later?** SQLite is zero-config for single-user use. `StateStore` is an ABC; switching to `PostgresStateStore` requires only changing one constructor argument.

**Why is `stall_timeout` a constructor parameter, not a class default?** Production runs should never time out waiting for agent intervention. Tests need deterministic completion. Making it explicit prevents accidental timeout in production.

**Why htcondor Python bindings instead of subprocess?** The `htcondor` package provides a typed API with proper exception types (`htcondor.HTCondorException`), no JSON parsing, and — critically — `htcondor.Collector().locate()` enables connecting to a named schedd on any node. The subprocess approach only worked when the agent ran on the same node as the submission.

**Why is `bps_backend` a workflow-level YAML field, not a CLI flag?** The backend choice is a property of the workflow (Parsl workflows require different infrastructure than HTCondor workflows), so it belongs with the workflow definition. It is also persisted in the `StateStore` so `batchflow resume` uses the correct backend automatically without the user needing to re-specify it.

**Why store `submit_location` on `PipelineNode`?** The schedd is determined at submit time (when we know which node we're on). Capturing it then and persisting it with the graph state means the monitor always reconnects to the right schedd, even after a resume on a different node.

**Why does `CodeAgentRunner` use `asyncio.to_thread` + `asyncio.wait_for`?** `smolagents.CodeAgent.run()` is a blocking call (synchronous HTTP to the model API). Running it in a thread executor keeps the asyncio event loop free. `wait_for` imposes a hard deadline (`agent_timeout`, default 300 s) so a hung model call cannot stall the workflow indefinitely.

**Why is `node_id == "__workflow__"` skipped in `CodeAgentRunner.run()`?** `WORKFLOW_COMPLETE` and `WORKFLOW_STALLED` events use `"__workflow__"` as a sentinel node ID. These are informational — there is no node to restart — so the runner returns early rather than attempting a graph lookup that would raise `KeyError`.
