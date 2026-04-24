# batchflow Design Document

*Async DAG Workflow Orchestration for HTCondor/BPS*

---

## 1. Motivation

Scientific computing workflows in high-energy physics and astronomy increasingly
involve multi-step pipelines submitted to HTCondor clusters via the LSST Batch
Processing Service (BPS). These runs routinely span multiple days, involve dozens
of interdependent pipeline stages, and are vulnerable to transient infrastructure
failures — network timeouts, worker evictions, disk quota violations — that do not
reflect problems with the science code itself.

The operational pattern that emerges in practice is: submit a set of jobs, wait,
discover that some subset has failed or been held, diagnose whether the failure is
transient or substantive, and either restart or escalate. As workflows grow in
complexity this manual loop becomes a bottleneck and a source of human error.

batchflow addresses this with three concrete capabilities:

- **DAG-structured execution** — pipelines are nodes in a directed acyclic graph
  with explicit dependency declarations. Parallel stages run concurrently;
  downstream stages are submitted automatically when their dependencies complete.
  This replaces both flat parallel submission and hard-coded sequential ordering.

- **Reliable status tracking** — job state is read directly from HTCondor via the
  `htcondor` Python bindings rather than inferred from log file scraping. Every
  transition is persisted to a local database so a process crash does not lose
  knowledge of in-flight work.

- **AI agent monitoring** — a push-based notification system delivers structured,
  self-contained failure reports to an agent or monitoring process. The agent can
  inspect failure context and call well-defined intervention actions (restart, skip,
  abort, modify) without needing to query the cluster directly.

The initial target deployment is single-user, proof-of-concept scale on the SLAC
SDF cluster. The design anticipates growth to multi-user, multi-workflow operation
by hiding all infrastructure concerns behind abstract interfaces with concrete
upgrade paths.

---

## 2. Design Principles

### Push over pull for agent notification

Runs span multiple days. A polling model — where an agent checks status every N
minutes — adds latency to failure response, wastes resources during quiet periods,
and requires the agent to maintain state about what it last observed. A push model
delivers events within one monitor wake cycle (60 seconds by default) with no
agent-side polling. The cost is a somewhat more complex notification path, which is
paid once at design time.

### asyncio throughout

Fan-out/join DAG scheduling and non-blocking monitor wakeup are naturally expressed
as coroutines. `asyncio.gather` handles concurrent node submission cleanly;
`asyncio.Event` gives zero-cost stall waiting. The alternative — a thread pool with
shared state — would require explicit locking around every graph mutation and would
not reduce complexity.

### htcondor Python bindings over subprocess

The `htcondor` Python package provides a typed API with proper exception types
(`htcondor.HTCondorException`), structured ClassAd access without JSON parsing, and
— critically — `htcondor.Collector().locate()` for connecting to a named schedd by
hostname. This last capability is essential on the SDF cluster, where each
interactive node runs its own schedd. A subprocess approach using `condor_q` could
only query the schedd on the node where the command was run; the Python bindings
allow the monitoring process to reconnect to the correct schedd regardless of which
node it is currently executing on.

### Pluggable backends with concrete upgrade paths

The proof-of-concept uses SQLite for state persistence and a timer-based monitor
wake strategy. Both choices are appropriate now and insufficient later. Rather than
revisiting the architecture at scale, each concern is expressed as an abstract
interface (`StateStore`, `WakeStrategy`, `SubmissionBackend`, `NotificationTransport`)
with the production implementation already written and tested — it just is not the
default.

### Stall is a first-class state, not an error

When a node fails and its dependents cannot proceed, the workflow is stalled rather
than aborted. The runner parks efficiently (consuming no CPU) and waits for an
intervention event. This is the correct model for long-running scientific workflows
where a transient infrastructure failure should pause, not terminate, hours of
accumulated computation.

---

## 3. Architecture

### 3.1 Module Overview

| Module | Responsibility |
|---|---|
| `graph.py` | `WorkflowGraph`, `PipelineNode`, `NodeState` — DAG definition, cycle detection, topological scheduling queries |
| `loader.py` | Loads `WorkflowGraph` from YAML workflow definition files |
| `bus.py` | `EventBus` and `JobEvent` — async pub/sub with per-subscriber `asyncio.Queue` |
| `state.py` | `StateStore` ABC + `SqliteStateStore` — persistent event log and graph serialisation |
| `monitor.py` | `HTCondorMonitor` — queries the schedd via `htcondor` Python bindings and publishes `JobEvent`s; pluggable `WakeStrategy` ABC |
| `classifier.py` | `ErrorClassifier` — classifies HTCondor `HoldReason` strings as transient or fatal using an extensible pattern registry |
| `agent.py` | `AgentHandler`, `AgentNotification`, `InterventionActions`, pluggable notification transports |
| `runner.py` | `WorkflowRunner` — async DAG scheduler, stall detection and parking, resume logic, `RunOutcome` |
| `workspace.py` | `WorkspaceManager` — versioned local copy of BPS YAML files with hash-based stale detection |
| `cli.py` | `batchflow` CLI: `submit`, `status`, `resume`, `intervene` |
| `backends/bps.py` | `SubmissionBackend` ABC; `BpsBackend` (submits via `htcondor` bindings); `MockBackend` for testing |
| `backends/postgres.py` | `PostgresStateStore` — drop-in Postgres replacement for `SqliteStateStore` (requires `asyncpg`) |

### 3.2 Event-Driven Execution

The `EventBus` is the communication backbone. Producers (`HTCondorMonitor`
instances, one per submitted node) publish `JobEvent` objects; consumers
(`WorkflowRunner`, `SqliteStateStore`, `AgentHandler`) each receive every event on
their own independent `asyncio.Queue`. This decoupling means a slow consumer — for
example, an agent webhook timing out — cannot delay the scheduler seeing a
`NODE_COMPLETE` event.

`HTCondorMonitor` wakes on its configured `WakeStrategy` and queries the schedd via
the `htcondor` Python bindings. It publishes events only for status transitions
observed since the last wake. When a cluster is no longer in the queue it queries
the schedd's history. Node-level events (`NODE_COMPLETE`, `NODE_HELD`,
`NODE_FAILED`) are synthesised from the aggregate state of all procs in the cluster
once all procs have reached a terminal HTCondor status.

`WorkflowRunner` maintains three concurrent asyncio tasks: a scheduler loop that
consumes its event queue and manages DAG state transitions; a store loop that drains
a separate queue and persists every event durably; and optionally the
`AgentHandler`. The scheduler submits all nodes whose dependencies are satisfied
after each `NODE_COMPLETE` event, naturally implementing both fan-out (multiple
dependents of one node) and fan-in (a node that waits for multiple predecessors).

### 3.3 Multi-Schedd Support

The SDF cluster runs one HTCondor schedd per interactive node (e.g. `sdfiana011`
through `sdfiana033`). A monitoring process running on a different node than the one
used for submission cannot query the correct schedd without knowing its hostname.

batchflow handles this as follows:

- At submit time, `BpsBackend` captures the FQDN of the submitting schedd via
  `htcondor.Schedd().location.address` and stores it in `PipelineNode.schedd_name`.
- `schedd_name` is persisted in the `StateStore` alongside the rest of the graph
  state, so it survives process restarts.
- `HTCondorMonitor` uses `htcondor.Collector().locate()` with the stored
  `schedd_name` to reconnect to the correct schedd on each wake cycle, regardless of
  which node the monitoring process is currently running on.
- The `batchflow resume` command therefore works correctly after a restart on any
  node — it reloads `schedd_name` from the database and the monitor reconnects
  transparently.

### 3.4 Workflow Definition

A workflow is a YAML file listing pipeline nodes with optional dependency
declarations:

```yaml
workflow: my_workflow
nodes:
  - id: stage_a
    bps_yaml: bps_stageA.yaml

  - id: stage_b
    bps_yaml: bps_stageB.yaml

  - id: stage_c              # fan-in: waits for both a and b
    bps_yaml: bps_stageC.yaml
    depends_on: [stage_a, stage_b]
    max_restarts: 5

  - id: stage_d
    bps_yaml: bps_stageD.yaml
    depends_on: [stage_c]
    bps_overrides:
      requestMemory: "16G"
```

Nodes with no `depends_on` are roots and are submitted immediately and concurrently.
A node becomes `READY` when all its predecessors have reached a terminal state
(`SUCCEEDED` or `SKIPPED`). `WorkflowGraph.validate()` checks for unknown dependency
references and cycles before any submission occurs.

### 3.5 Node State Machine

```
PENDING → READY → SUBMITTED → RUNNING → SUCCEEDED
                                       → FAILED
                                       → HELD
                                       → SKIPPED   (agent intervention)
```

`TERMINAL_STATES = {SUCCEEDED, SKIPPED}` — counts as done for dependency
resolution. `BLOCKED_STATES = {FAILED, HELD}` — blocks dependents until agent
intervention. The distinction between `SKIPPED` and `FAILED` allows an agent to
intentionally bypass a node and unblock its dependents without that node having
produced output.

### 3.6 Stall Handling and Agent Interface

When no nodes are running and at least one non-terminal node exists, the workflow is
stalled. The runner publishes a `WORKFLOW_STALLED` event, then parks on an
`asyncio.Event` that is set by any subsequent `INTERVENTION_*` event. While parked
it continues draining its queue so that events arriving from monitors are not lost.

`AgentHandler` subscribes to the bus and on `NODE_HELD` or `NODE_FAILED` events
constructs an `AgentNotification` and delivers it to all registered transports. The
notification is designed to be self-contained:

| Field | Content |
|---|---|
| `event_type` | The triggering event (e.g. `NODE_HELD`, `NODE_FAILED`, `WORKFLOW_STALLED`) |
| `hold_reasons` | Raw HTCondor `HoldReason` strings from the affected cluster |
| `classification` | Classifier verdict (`transient` / `fatal` / `unknown`), confidence score, and suggested action |
| `dag_context` | Current state of every node in the workflow — shows what is blocked downstream |
| `restart_count` / `max_restarts` | How many restarts have already been attempted and the per-node limit |
| `bps_yaml` | The BPS YAML file for the affected node — useful for crafting a modified retry |
| `metadata` | Arbitrary user-defined data from the node definition |

`InterventionActions` exposes four operations, each async, idempotent where
possible, and recorded in the `StateStore` with actor identity, timestamp, and
reason:

| Action | Effect |
|---|---|
| `restart_node()` | Releases held HTCondor jobs or re-submits if fully failed. Refuses if `max_restarts` is exceeded. |
| `skip_node()` | Marks the node `SKIPPED` — a terminal state — unblocking dependent nodes without the node having succeeded. |
| `abort_node()` | Cancels all HTCondor jobs and marks the node `FAILED`. |
| `modify_node(bps_overrides)` | Aborts and re-submits with altered BPS parameters (e.g. more memory or a different queue). |

Notification transports are pluggable. `StdoutTransport` (development and testing),
`WebhookTransport` (HTTP POST, requires `aiohttp`), and `CallbackTransport`
(in-process async callable) are provided. Multiple transports can be active
simultaneously. Custom transports implement a single `async send(notification)`
method.

---

## 4. Pluggable Backends

Each infrastructure dependency is expressed as an abstract base class. The
proof-of-concept implementation is the default; the production implementation is
already written and requires only a constructor argument change to activate.

| Concern | Now | Later |
|---|---|---|
| State persistence | `SqliteStateStore`: zero-config, single file, WAL mode for concurrent reads | `PostgresStateStore` (`asyncpg`): connection pool, shared access across machines; schema is identical so migration is a straight dump-and-restore |
| Monitor wake strategy | `TimerWakeStrategy`: wakes every N seconds (default 60), works on any filesystem | `InotifyWakeStrategy` (`watchfiles`): wakes immediately on DAGMan event log writes; falls back gracefully to timer if `watchfiles` is unavailable |
| Job submission | `BpsBackend`: submits via `htcondor` Python bindings, captures schedd FQDN at submit time; `MockBackend` for testing | Any `SubmissionBackend` subclass |
| Agent notifications | `StdoutTransport`: JSON to stdout | `WebhookTransport` (HTTP POST), `CallbackTransport` (in-process callable), or any `NotificationTransport` subclass |

---

## 5. Workspace Management

`WorkspaceManager` handles the local working directory for a workflow run. Its
primary responsibility is maintaining a local copy of the BPS YAML directory tree
that `bps submit` reads. It computes a SHA-256 hash over the sorted names and
contents of all files in the source tree and stores it in `workspace.json` alongside
the copy.

On each `prepare()` call it compares the stored hash against the current source
hash. If they differ it raises `WorkspaceError` rather than silently proceeding with
stale YAML files. The `force_refresh=True` flag is the explicit opt-in to overwrite.
This design catches the class of operational error where a configuration update to
the source tree is not propagated to an active run directory.

The workspace also owns the log directory for per-node submission output and the
path to the state database, giving each run a self-contained directory that can be
archived or inspected after completion.

---

## 6. Error Classification

`ErrorClassifier` maintains two lists of compiled regular expressions — transient
patterns and fatal patterns — and matches them against the HTCondor `HoldReason`
strings associated with a failure. Built-in patterns cover common cluster
infrastructure failures:

- **Transient**: network timeouts, worker evictions, shadow exceptions, disk quota
  exceedances, Docker pull failures, connection resets
- **Fatal**: missing input datasets, permission errors, Python exceptions
  (`ValueError`, `KeyError`, `ImportError`), segmentation faults

Site-specific patterns are loaded from an optional `error_patterns.yaml` file in
the run directory, or added programmatically via `add_transient()` and
`add_fatal()`. This allows domain-specific failure modes to be registered without
modifying the package.

Classification returns a verdict (`transient` / `fatal` / `unknown`), a confidence
score (fraction of hold reasons that matched the verdict bucket), a suggested action
(`restart` / `abort` / `investigate`), and the list of matched patterns for audit
logging. Mixed cases — where some reasons are transient and others fatal — resolve
conservatively to `fatal`.

---

## 7. Testing

The test suite runs without an HTCondor connection. `MockBackend` handles submission
and returns synthetic cluster IDs; events are injected directly onto the `EventBus`
to simulate monitor output. `asyncio_mode = "auto"` is set in `pyproject.toml` so
async test functions require no decorator. The `stall_timeout` constructor parameter
on `WorkflowRunner` controls how long the runner waits after a stall before
returning `RunOutcome.STALLED`, enabling deterministic stall tests without external
timeouts.

| Test class | Coverage |
|---|---|
| `TestWorkflowGraph` | DAG construction, cycle detection, fan-in readiness, stall and completion detection, node ID validation |
| `TestEventBus` | Fan-out to multiple subscribers, duplicate subscriber rejection, async generator stop-on-event |
| `TestSqliteStateStore` | Save/load round-trip with state preservation, event log append and retrieval, intervention audit trail |
| `TestErrorClassifier` | Transient/fatal/unknown classification, mixed-reason conservatism, YAML pattern file loading, runtime pattern addition |
| `TestWorkflowRunner` | Linear execution, parallel fan-out and fan-in, stall returning `RunOutcome.STALLED`, intervention restart resuming to completion |
| `TestInterventionActions` | Restart increments count and refuses at max, skip unblocks dependents, abort marks node failed |
| `TestLoader` | Valid YAML parse, missing file, missing required fields |
| `TestWorkspaceManager` | Copy, idempotency, stale detection and error, force refresh, missing source directory |
| `TestPostgresBackendInterface` | ABC conformance, clean `ImportError` without `asyncpg` installed, serialisation round-trip shared with SQLite |

---

## 8. Command-Line Interface

| Command | Description |
|---|---|
| `batchflow submit <workflow.yaml>` | Validate the workflow, print the node list, confirm, and begin execution. Supports `--dry-run`, `--auto-restart`, `--webhook`, and `--poll-interval`. |
| `batchflow status` | Print colour-coded node states, restart counts, and HTCondor cluster IDs from the `StateStore`. |
| `batchflow resume` | Reload saved graph from the `StateStore`, re-attach monitors to any in-flight clusters, and continue from the current state. |
| `batchflow intervene restart <node>` | Restart a held or failed node. Accepts `--override key=value` for BPS parameter changes on the retry. |
| `batchflow intervene skip <node>` | Mark a node `SKIPPED`, unblocking its dependents without it having succeeded. |
| `batchflow intervene abort <node>` | Cancel HTCondor jobs and mark the node `FAILED`. |
| `batchflow workflows` | List all workflow runs recorded in the `StateStore` with creation and update timestamps. |

The `--auto-restart` flag enables `AgentHandler`'s autonomous restart mode: when
`ErrorClassifier` returns a transient verdict with confidence at or above a
configurable threshold (default 0.8), the handler calls `restart_node()` without
waiting for external agent input. This is appropriate for environments where
transient failures are common and human review of each restart would be impractical.

---

## 9. Outstanding Work

- End-to-end validation on SDF with a single-node workflow, confirming that
  `BpsBackend` correctly captures the schedd FQDN at submit time and that
  `HTCondorMonitor` can reconnect to it from a different node.
- Validate `BpsBackend` against actual `bps submit` behaviour — in particular,
  confirm that the `htcondor` Python bindings return the cluster ID in the expected
  form after submission.
- Exercise the resume path after a deliberate process restart on a different
  interactive node, verifying that `schedd_name` is correctly reloaded from the
  `StateStore` and the monitor reconnects.
- Expose `batchflow intervene modify` in the CLI. The underlying
  `InterventionActions.modify_node()` is fully implemented; only the CLI command is
  missing.

---

## 10. Deployment

```bash
pip install -e /path/to/batchflow           # core package (includes htcondor)
pip install -e /path/to/batchflow[webhook]  # add WebhookTransport (aiohttp)
pip install -e /path/to/batchflow[inotify]  # add InotifyWakeStrategy (watchfiles)
pip install -e /path/to/batchflow[postgres] # add PostgresStateStore (asyncpg)
```

A minimal workflow run:

```bash
# 1. Define the workflow and submit
batchflow submit my_workflow.yaml --work-dir ./run_001

# 2. Check status at any time, from any node
batchflow status --work-dir ./run_001

# 3. If the process is interrupted, resume from any node
batchflow resume --work-dir ./run_001

# 4. Intervene on a stalled node
batchflow intervene restart stage_c --work-dir ./run_001
batchflow intervene skip stage_c --reason "known bad input data"
```

The run directory (`--work-dir`) contains the local `bps/` copy, per-node
submission logs, and the state database (`batchflow.db`). It is self-contained and
can be archived for post-run diagnosis. The optional `BATCHFLOW_WEBHOOK` environment
variable specifies a URL to receive `AgentNotification` JSON payloads.
