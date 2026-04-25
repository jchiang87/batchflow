"""
tests/test_batchflow.py

Unit tests for batchflow core logic.  No HTCondor connection required;
the MockBackend is used throughout and monitor tasks are replaced with
direct event injection.
"""
from __future__ import annotations

import asyncio
import json
from datetime import timezone
from pathlib import Path

import pytest

from batchflow import (
    WorkflowGraph, PipelineNode, NodeState,
    EventBus, EventType, JobEvent,
    SqliteStateStore,
    ErrorClassifier,
    AgentHandler, AgentNotification, InterventionActions,
    CallbackTransport, StdoutTransport,
    WorkflowRunner, MockBackend,
    load_workflow,
)
from batchflow.graph import TERMINAL_STATES


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_graph(workflow_id="test_wf") -> WorkflowGraph:
    g = WorkflowGraph(workflow_id)
    g.add_node(PipelineNode("a", "bps_a.yaml"))
    g.add_node(PipelineNode("b", "bps_b.yaml"))
    g.add_node(PipelineNode("c", "bps_c.yaml", depends_on=["a", "b"]))
    g.add_node(PipelineNode("d", "bps_d.yaml", depends_on=["c"]))
    g.validate()
    return g


# ---------------------------------------------------------------------------
# Graph tests
# ---------------------------------------------------------------------------

class TestWorkflowGraph:

    def test_ready_roots(self):
        g = make_graph()
        ready = list(g.ready_nodes())
        assert {n.node_id for n in ready} == {"a", "b"}

    def test_fan_in_blocked_until_both_parents_done(self):
        g = make_graph()
        g.node("a").state = NodeState.SUCCEEDED
        ready = list(g.ready_nodes())
        # b is still PENDING, so c must wait
        assert {n.node_id for n in ready} == {"b"}

    def test_fan_in_unblocked_when_both_parents_done(self):
        g = make_graph()
        g.node("a").state = NodeState.SUCCEEDED
        g.node("b").state = NodeState.SUCCEEDED
        ready = list(g.ready_nodes())
        assert {n.node_id for n in ready} == {"c"}

    def test_cycle_detection(self):
        g = WorkflowGraph("cycle")
        g.add_node(PipelineNode("x", "x.yaml", depends_on=["y"]))
        g.add_node(PipelineNode("y", "y.yaml", depends_on=["x"]))
        with pytest.raises(ValueError, match="cycle"):
            g.validate()

    def test_unknown_dependency(self):
        g = WorkflowGraph("bad")
        g.add_node(PipelineNode("x", "x.yaml", depends_on=["ghost"]))
        with pytest.raises(ValueError, match="unknown node"):
            g.validate()

    def test_is_complete(self):
        g = make_graph()
        assert not g.is_complete()
        for n in g.nodes:
            n.state = NodeState.SUCCEEDED
        assert g.is_complete()

    def test_is_stalled(self):
        g = make_graph()
        g.node("a").state = NodeState.FAILED
        g.node("b").state = NodeState.SUCCEEDED
        assert g.is_stalled()

    def test_skipped_counts_as_terminal(self):
        g = make_graph()
        g.node("a").state = NodeState.SKIPPED
        g.node("b").state = NodeState.SKIPPED
        ready = list(g.ready_nodes())
        assert {n.node_id for n in ready} == {"c"}

    def test_summary(self):
        g = make_graph()
        g.node("a").state = NodeState.SUCCEEDED
        s = g.summary()
        assert s["SUCCEEDED"] == 1
        assert s["PENDING"] == 3

    def test_invalid_node_id(self):
        with pytest.raises(ValueError):
            PipelineNode("bad id!", "x.yaml")


# ---------------------------------------------------------------------------
# EventBus tests
# ---------------------------------------------------------------------------

class TestEventBus:

    @pytest.mark.asyncio
    async def test_fan_out_to_multiple_subscribers(self):
        bus = EventBus()
        q1 = bus.subscribe("s1")
        q2 = bus.subscribe("s2")
        event = JobEvent(
            event_type=EventType.NODE_COMPLETE,
            workflow_id="wf",
            node_id="a",
        )
        await bus.publish(event)
        assert await q1.get() == event
        assert await q2.get() == event

    @pytest.mark.asyncio
    async def test_duplicate_subscriber_raises(self):
        bus = EventBus()
        bus.subscribe("s1")
        with pytest.raises(ValueError):
            bus.subscribe("s1")

    @pytest.mark.asyncio
    async def test_events_generator_stops_on_stop_event(self):
        """
        events() yields items until stop_event is set.  We publish one
        event, consume it, then set the stop event and verify the loop
        exits cleanly.
        """
        bus = EventBus()
        q = bus.subscribe("gen")
        stop = asyncio.Event()
        event = JobEvent(
            event_type=EventType.JOB_SUBMITTED,
            workflow_id="wf",
            node_id="x",
        )
        received = []

        async def consume():
            async for e in bus.events(q, stop_event=stop):
                received.append(e)
                stop.set()  # stop after first event

        await bus.publish(event)
        await asyncio.wait_for(consume(), timeout=3.0)
        assert len(received) == 1
        assert received[0].event_type == EventType.JOB_SUBMITTED


# ---------------------------------------------------------------------------
# StateStore tests
# ---------------------------------------------------------------------------

class TestSqliteStateStore:

    @pytest.mark.asyncio
    async def test_save_and_load_workflow(self):
        store = SqliteStateStore(":memory:")
        await store.init()
        g = make_graph()
        g.node("a").state = NodeState.SUCCEEDED
        await store.save_workflow(g)
        loaded = await store.load_workflow("test_wf")
        assert loaded is not None
        assert loaded.node("a").state == NodeState.SUCCEEDED
        assert loaded.node("b").state == NodeState.PENDING
        await store.close()

    @pytest.mark.asyncio
    async def test_load_nonexistent_returns_none(self):
        store = SqliteStateStore(":memory:")
        await store.init()
        result = await store.load_workflow("does_not_exist")
        assert result is None
        await store.close()

    @pytest.mark.asyncio
    async def test_record_and_retrieve_events(self):
        store = SqliteStateStore(":memory:")
        await store.init()
        g = make_graph()
        await store.save_workflow(g)
        event = JobEvent(
            event_type=EventType.NODE_COMPLETE,
            workflow_id="test_wf",
            node_id="a",
            hold_reasons=(),
        )
        await store.record_event(event)
        events = await store.get_events("test_wf", node_id="a")
        assert len(events) == 1
        assert events[0].event_type == EventType.NODE_COMPLETE
        await store.close()

    @pytest.mark.asyncio
    async def test_save_updates_state(self):
        store = SqliteStateStore(":memory:")
        await store.init()
        g = make_graph()
        await store.save_workflow(g)
        g.node("b").state = NodeState.RUNNING
        await store.save_workflow(g)
        loaded = await store.load_workflow("test_wf")
        assert loaded.node("b").state == NodeState.RUNNING
        await store.close()

    @pytest.mark.asyncio
    async def test_intervention_audit_trail(self):
        store = SqliteStateStore(":memory:")
        await store.init()
        g = make_graph()
        await store.save_workflow(g)
        event = JobEvent(
            event_type=EventType.INTERVENTION_RESTART,
            workflow_id="test_wf",
            node_id="a",
            actor="agent_007",
            reason="transient network failure",
        )
        await store.record_event(event)
        events = await store.get_events("test_wf")
        restart_events = [e for e in events
                          if e.event_type == EventType.INTERVENTION_RESTART]
        assert len(restart_events) == 1
        assert restart_events[0].actor == "agent_007"
        assert restart_events[0].reason == "transient network failure"
        await store.close()


# ---------------------------------------------------------------------------
# ErrorClassifier tests
# ---------------------------------------------------------------------------

class TestErrorClassifier:

    def test_known_transient(self):
        clf = ErrorClassifier()
        result = clf.classify(["Failed to send file to remote host"])
        assert result.verdict == "transient"
        assert result.suggested_action == "restart"
        assert result.confidence > 0

    def test_known_fatal(self):
        clf = ErrorClassifier()
        result = clf.classify(["dataset not found in Butler registry"])
        assert result.verdict == "fatal"
        assert result.suggested_action == "abort"

    def test_unknown(self):
        clf = ErrorClassifier()
        result = clf.classify(["some completely novel error"])
        assert result.verdict == "unknown"
        assert result.suggested_action == "investigate"

    def test_empty_reasons(self):
        clf = ErrorClassifier()
        result = clf.classify([])
        assert result.verdict == "unknown"
        assert result.confidence == 0.0

    def test_mixed_prefers_fatal(self):
        clf = ErrorClassifier()
        # One transient, one fatal → conservative → fatal
        result = clf.classify([
            "Network timeout",
            "Permission denied on output repository",
        ])
        assert result.verdict == "fatal"

    def test_add_custom_pattern(self):
        clf = ErrorClassifier()
        clf.add_transient("custom transient error XYZ")
        result = clf.classify(["custom transient error XYZ encountered"])
        assert result.verdict == "transient"

    def test_load_patterns_file(self, tmp_path):
        patterns = tmp_path / "patterns.yaml"
        patterns.write_text("""
transient:
  - pattern: "site specific timeout"
fatal:
  - pattern: "site specific fatal"
""")
        clf = ErrorClassifier(patterns_file=patterns)
        assert clf.classify(["site specific timeout"]).verdict == "transient"
        assert clf.classify(["site specific fatal"]).verdict == "fatal"


# ---------------------------------------------------------------------------
# WorkflowRunner (end-to-end with MockBackend, no HTCondor)
# ---------------------------------------------------------------------------

class TestWorkflowRunner:

    @pytest.mark.asyncio
    async def test_simple_linear_workflow(self):
        """a -> b -> c all succeed; returns RunOutcome.COMPLETE."""
        from batchflow.runner import RunOutcome
        g = WorkflowGraph("linear")
        g.add_node(PipelineNode("a", "a.yaml"))
        g.add_node(PipelineNode("b", "b.yaml", depends_on=["a"]))
        g.add_node(PipelineNode("c", "c.yaml", depends_on=["b"]))
        g.validate()

        bus = EventBus()
        backend = MockBackend()
        store = SqliteStateStore(":memory:")
        await store.init()

        runner = WorkflowRunner(
            graph=g, backend=backend, store=store,
            bus=bus, bps_dir=Path("/fake"),
        )

        async def inject_completions():
            for node_id in ["a", "b", "c"]:
                for _ in range(100):
                    if g._nodes[node_id].submit_id is not None:
                        break
                    await asyncio.sleep(0.02)
                await bus.publish(JobEvent(
                    event_type=EventType.NODE_COMPLETE,
                    workflow_id="linear", node_id=node_id,
                ))

        results = await asyncio.wait_for(
            asyncio.gather(runner.run(), inject_completions()),
            timeout=10.0,
        )
        assert results[0] == RunOutcome.COMPLETE
        assert g.is_complete()
        assert g.node("c").state == NodeState.SUCCEEDED
        await store.close()

    @pytest.mark.asyncio
    async def test_parallel_fan_out_and_join(self):
        """a, b run in parallel; c waits for both; d waits for c."""
        from batchflow.runner import RunOutcome
        g = make_graph()

        bus = EventBus()
        backend = MockBackend()
        store = SqliteStateStore(":memory:")
        await store.init()

        runner = WorkflowRunner(
            graph=g, backend=backend, store=store,
            bus=bus, bps_dir=Path("/fake"),
        )

        async def wait_submitted(node_id):
            for _ in range(100):
                if g._nodes[node_id].submit_id:
                    return
                await asyncio.sleep(0.02)

        async def inject():
            await asyncio.gather(wait_submitted("a"), wait_submitted("b"))
            await bus.publish(JobEvent(event_type=EventType.NODE_COMPLETE,
                                       workflow_id="test_wf", node_id="a"))
            await bus.publish(JobEvent(event_type=EventType.NODE_COMPLETE,
                                       workflow_id="test_wf", node_id="b"))
            await wait_submitted("c")
            await bus.publish(JobEvent(event_type=EventType.NODE_COMPLETE,
                                       workflow_id="test_wf", node_id="c"))
            await wait_submitted("d")
            await bus.publish(JobEvent(event_type=EventType.NODE_COMPLETE,
                                       workflow_id="test_wf", node_id="d"))

        results = await asyncio.wait_for(
            asyncio.gather(runner.run(), inject()),
            timeout=10.0,
        )
        assert results[0] == RunOutcome.COMPLETE
        assert g.is_complete()
        await store.close()

    @pytest.mark.asyncio
    async def test_stalled_returns_outcome(self):
        """Node failure with stall_timeout returns RunOutcome.STALLED quickly."""
        from batchflow.runner import RunOutcome
        g = WorkflowGraph("stall_wf")
        g.add_node(PipelineNode("x", "x.yaml"))
        g.add_node(PipelineNode("y", "y.yaml", depends_on=["x"]))
        g.validate()

        bus = EventBus()
        stall_q = bus.subscribe("stall_watcher")
        backend = MockBackend()
        store = SqliteStateStore(":memory:")
        await store.init()

        stall_received = asyncio.Event()

        # stall_timeout=0.5: runner exits cleanly after 0.5s stall
        runner = WorkflowRunner(
            graph=g, backend=backend, store=store,
            bus=bus, bps_dir=Path("/fake"),
            stall_timeout=0.5,
        )

        async def watch_stall():
            async for event in bus.events(stall_q, stop_event=stall_received):
                if event.event_type == EventType.WORKFLOW_STALLED:
                    stall_received.set()

        async def inject():
            for _ in range(100):
                if g._nodes["x"].submit_id:
                    break
                await asyncio.sleep(0.02)
            await bus.publish(JobEvent(
                event_type=EventType.NODE_FAILED,
                workflow_id="stall_wf", node_id="x"))

        results = await asyncio.wait_for(
            asyncio.gather(runner.run(), inject(), watch_stall()),
            timeout=8.0,
        )
        assert results[0] == RunOutcome.STALLED
        assert stall_received.is_set()
        assert g.node("x").state == NodeState.FAILED
        await store.close()

    @pytest.mark.asyncio
    async def test_intervention_restarts_stalled_workflow(self):
        """Agent restart after stall resumes the workflow to completion."""
        from batchflow.runner import RunOutcome
        g = WorkflowGraph("restart_wf")
        g.add_node(PipelineNode("p", "p.yaml", max_restarts=2))
        g.add_node(PipelineNode("q", "q.yaml", depends_on=["p"]))
        g.validate()

        bus = EventBus()
        _ = bus.subscribe("sink")
        backend = MockBackend()
        store = SqliteStateStore(":memory:")
        await store.init()
        await store.save_workflow(g)

        runner = WorkflowRunner(
            graph=g, backend=backend, store=store,
            bus=bus, bps_dir=Path("/fake"),
            stall_timeout=5.0,
        )
        actions = InterventionActions(
            graph=g, backend=backend, bus=bus,
            store=store, bps_dir=Path("/fake"),
        )

        async def wait_submitted(node_id):
            for _ in range(100):
                if g._nodes[node_id].submit_id:
                    return
                await asyncio.sleep(0.02)

        async def inject():
            await wait_submitted("p")
            # First attempt fails -> stall
            await bus.publish(JobEvent(event_type=EventType.NODE_FAILED,
                                       workflow_id="restart_wf", node_id="p"))
            await asyncio.sleep(0.1)
            # Agent restarts
            await actions.restart_node("p", reason="test restart")
            # Second attempt succeeds
            await wait_submitted("p")
            await bus.publish(JobEvent(event_type=EventType.NODE_COMPLETE,
                                       workflow_id="restart_wf", node_id="p"))
            await wait_submitted("q")
            await bus.publish(JobEvent(event_type=EventType.NODE_COMPLETE,
                                       workflow_id="restart_wf", node_id="q"))

        results = await asyncio.wait_for(
            asyncio.gather(runner.run(), inject()),
            timeout=15.0,
        )
        assert results[0] == RunOutcome.COMPLETE
        assert g.node("p").restart_count == 1
        await store.close()

    @pytest.mark.asyncio
    async def test_restart_failed_node_spawns_monitor(self):
        """Restarting a FAILED node must spawn a monitor for the new cluster."""
        from batchflow.runner import RunOutcome
        g = WorkflowGraph("restart_monitor_wf")
        g.add_node(PipelineNode("a", "a.yaml", max_restarts=1))
        g.add_node(PipelineNode("b", "b.yaml", depends_on=["a"]))
        g.validate()

        bus = EventBus()
        _ = bus.subscribe("sink")
        backend = MockBackend()
        store = SqliteStateStore(":memory:")
        await store.init()
        await store.save_workflow(g)

        runner = WorkflowRunner(
            graph=g, backend=backend, store=store,
            bus=bus, bps_dir=Path("/fake"),
            stall_timeout=5.0,
        )
        actions = InterventionActions(
            graph=g, backend=backend, bus=bus,
            store=store, bps_dir=Path("/fake"),
        )

        monitor_spawned = asyncio.Event()

        async def wait_submitted(node_id):
            for _ in range(100):
                if g._nodes[node_id].submit_id:
                    return
                await asyncio.sleep(0.02)

        async def inject():
            await wait_submitted("a")
            first_cluster = g.node("a").submit_id

            # First attempt fails
            await bus.publish(JobEvent(event_type=EventType.NODE_FAILED,
                                       workflow_id="restart_monitor_wf", node_id="a"))
            await asyncio.sleep(0.1)

            # Agent restarts — this calls bps restart for the new cluster
            await actions.restart_node("a", reason="test")
            await asyncio.sleep(0.1)

            # A new cluster_id must have been assigned
            assert g.node("a").submit_id != first_cluster, \
                "restart_node should have submitted a new cluster"

            # A monitor task must now exist for the new cluster
            task = runner._monitor_tasks.get("a")
            assert task is not None and not task.done(), \
                "no live monitor task after restarting a FAILED node"
            monitor_spawned.set()

            # Drive the restarted node to completion via direct injection
            await bus.publish(JobEvent(event_type=EventType.NODE_COMPLETE,
                                       workflow_id="restart_monitor_wf", node_id="a"))
            await wait_submitted("b")
            await bus.publish(JobEvent(event_type=EventType.NODE_COMPLETE,
                                       workflow_id="restart_monitor_wf", node_id="b"))

        results = await asyncio.wait_for(
            asyncio.gather(runner.run(), inject()),
            timeout=15.0,
        )
        assert monitor_spawned.is_set()
        assert results[0] == RunOutcome.COMPLETE
        assert g.node("a").restart_count == 1
        assert g.node("b").state == NodeState.SUCCEEDED
        await store.close()

# ---------------------------------------------------------------------------
# Loader tests
# ---------------------------------------------------------------------------

class TestLoader:

    def test_load_valid_yaml(self, tmp_path):
        wf_file = tmp_path / "workflow.yaml"
        wf_file.write_text("""
workflow: test_load
nodes:
  - id: step1
    bps_yaml: bps_step1.yaml
  - id: step2
    bps_yaml: bps_step2.yaml
    depends_on: [step1]
    max_restarts: 5
    metadata:
      tag: demo
""")
        g = load_workflow(wf_file)
        assert g.workflow_id == "test_load"
        assert len(g.nodes) == 2
        assert g.node("step2").depends_on == ["step1"]
        assert g.node("step2").max_restarts == 5
        assert g.node("step2").metadata == {"tag": "demo"}

    def test_load_missing_file(self):
        with pytest.raises(FileNotFoundError):
            load_workflow("/no/such/file.yaml")

    def test_load_missing_bps_yaml_key(self, tmp_path):
        wf_file = tmp_path / "bad.yaml"
        wf_file.write_text("""
workflow: bad
nodes:
  - id: oops
""")
        with pytest.raises(ValueError, match="bps_yaml"):
            load_workflow(wf_file)


# ---------------------------------------------------------------------------
# InterventionActions tests
# ---------------------------------------------------------------------------

class TestInterventionActions:

    @pytest.mark.asyncio
    async def test_restart_increments_count(self):
        g = make_graph()
        g.node("a").state = NodeState.HELD
        g.node("a").submit_id = "999"

        bus = EventBus()
        _ = bus.subscribe("sink")  # prevent publish blocking
        backend = MockBackend()
        store = SqliteStateStore(":memory:")
        await store.init()
        await store.save_workflow(g)

        actions = InterventionActions(
            graph=g, backend=backend, bus=bus,
            store=store, bps_dir=Path("/fake"),
        )
        await actions.restart_node("a", reason="test restart")
        assert g.node("a").restart_count == 1
        assert g.node("a").state == NodeState.SUBMITTED
        await store.close()

    @pytest.mark.asyncio
    async def test_restart_refuses_when_max_reached(self):
        g = make_graph()
        g.node("a").state = NodeState.FAILED
        g.node("a").restart_count = 3  # at max_restarts default

        bus = EventBus()
        _ = bus.subscribe("sink")
        backend = MockBackend()
        store = SqliteStateStore(":memory:")
        await store.init()
        await store.save_workflow(g)

        actions = InterventionActions(
            graph=g, backend=backend, bus=bus,
            store=store, bps_dir=Path("/fake"),
        )
        with pytest.raises(RuntimeError, match="not restartable"):
            await actions.restart_node("a")
        await store.close()

    @pytest.mark.asyncio
    async def test_skip_unblocks_dependents(self):
        g = make_graph()
        g.node("a").state = NodeState.FAILED
        g.node("b").state = NodeState.SUCCEEDED

        bus = EventBus()
        _ = bus.subscribe("sink")
        backend = MockBackend()
        store = SqliteStateStore(":memory:")
        await store.init()
        await store.save_workflow(g)

        actions = InterventionActions(
            graph=g, backend=backend, bus=bus,
            store=store, bps_dir=Path("/fake"),
        )
        await actions.skip_node("a", reason="skipping for test")
        assert g.node("a").state == NodeState.SKIPPED
        # With a=SKIPPED and b=SUCCEEDED, c should now be ready.
        ready = list(g.ready_nodes())
        assert any(n.node_id == "c" for n in ready)
        await store.close()

    @pytest.mark.asyncio
    async def test_abort_marks_failed(self):
        g = make_graph()
        g.node("a").state = NodeState.RUNNING
        g.node("a").submit_id = "888"

        bus = EventBus()
        _ = bus.subscribe("sink")
        backend = MockBackend()
        store = SqliteStateStore(":memory:")
        await store.init()
        await store.save_workflow(g)

        actions = InterventionActions(
            graph=g, backend=backend, bus=bus,
            store=store, bps_dir=Path("/fake"),
        )
        await actions.abort_node("a", reason="testing abort")
        assert g.node("a").state == NodeState.FAILED
        await store.close()


# ---------------------------------------------------------------------------
# AgentNotification serialisation
# ---------------------------------------------------------------------------

class TestAgentNotification:

    def test_to_json_roundtrip(self):
        from datetime import datetime, timezone
        from batchflow.classifier import Classification

        clf = Classification(
            verdict="transient",
            confidence=0.9,
            suggested_action="restart",
            matched_patterns=["Network timeout"],
        )
        notif = AgentNotification(
            event_type="NODE_HELD",
            workflow_id="wf",
            node_id="a",
            timestamp=datetime(2024, 1, 1, tzinfo=timezone.utc),
            hold_reasons=["Network timeout"],
            classification=clf,
            restart_count=1,
            max_restarts=3,
            dag_context={"a": "HELD", "b": "PENDING"},
            bps_yaml="bps_a.yaml",
            metadata={},
        )
        raw = notif.to_json()
        data = json.loads(raw)
        assert data["classification"]["verdict"] == "transient"
        assert data["node_id"] == "a"
        assert data["restart_count"] == 1


# ---------------------------------------------------------------------------
# WorkspaceManager tests
# ---------------------------------------------------------------------------

class TestWorkspaceManager:

    def test_prepare_copies_bps(self, tmp_path):
        from batchflow.workspace import WorkspaceManager

        # Create a fake source bps dir with one file.
        src = tmp_path / "pkg" / "bps"
        src.mkdir(parents=True)
        (src / "bps_test.yaml").write_text("key: value")

        ws = WorkspaceManager(
            source_bps_dir=src,
            instrument="LSSTCam",
            work_dir=tmp_path / "run",
        )
        ws.prepare()

        assert (ws.bps_dir / "bps_test.yaml").exists()
        assert ws.log_dir.exists()
        assert ws.db_path.parent.exists()

    def test_second_prepare_is_idempotent(self, tmp_path):
        from batchflow.workspace import WorkspaceManager

        src = tmp_path / "bps"
        src.mkdir()
        (src / "bps_a.yaml").write_text("a: 1")

        ws = WorkspaceManager(source_bps_dir=src,
                              instrument="LSSTCam",
                              work_dir=tmp_path / "run")
        ws.prepare()
        ws.prepare()  # should not raise

    def test_stale_copy_raises_without_force(self, tmp_path):
        from batchflow.workspace import WorkspaceManager, WorkspaceError

        src = tmp_path / "bps"
        src.mkdir()
        (src / "bps_a.yaml").write_text("a: 1")

        ws = WorkspaceManager(source_bps_dir=src,
                              instrument="LSSTCam",
                              work_dir=tmp_path / "run")
        ws.prepare()

        # Modify source to make local copy stale.
        (src / "bps_b.yaml").write_text("b: 2")

        with pytest.raises(WorkspaceError, match="stale"):
            ws.prepare()

    def test_force_refresh_updates_stale_copy(self, tmp_path):
        from batchflow.workspace import WorkspaceManager

        src = tmp_path / "bps"
        src.mkdir()
        (src / "bps_a.yaml").write_text("a: 1")

        ws = WorkspaceManager(source_bps_dir=src,
                              instrument="LSSTCam",
                              work_dir=tmp_path / "run")
        ws.prepare()
        (src / "bps_b.yaml").write_text("b: 2")

        ws2 = WorkspaceManager(source_bps_dir=src,
                               instrument="LSSTCam",
                               work_dir=tmp_path / "run",
                               force_refresh=True)
        ws2.prepare()
        assert (ws2.bps_dir / "bps_b.yaml").exists()

    def test_missing_source_raises(self, tmp_path):
        from batchflow.workspace import WorkspaceManager, WorkspaceError

        ws = WorkspaceManager(
            source_bps_dir=tmp_path / "nonexistent",
            instrument="LSSTCam",
            work_dir=tmp_path / "run",
        )
        with pytest.raises(WorkspaceError, match="not found"):
            ws.prepare()


# ---------------------------------------------------------------------------
# Postgres backend interface tests (no real Postgres required)
# ---------------------------------------------------------------------------

class TestPostgresBackendInterface:
    """
    Verify the PostgresStateStore satisfies the StateStore ABC and that
    its import failure (no asyncpg) raises a clear message.
    """

    def test_postgres_store_is_statestore_subclass(self):
        from batchflow.backends.postgres import PostgresStateStore
        from batchflow.state import StateStore as SS
        assert issubclass(PostgresStateStore, SS)

    def test_postgres_init_fails_cleanly_without_asyncpg(self, monkeypatch):
        """If asyncpg is not installed, init() should raise ImportError with help text."""
        import sys
        import importlib
        from batchflow.backends.postgres import PostgresStateStore

        # Temporarily hide asyncpg from the import system.
        original = sys.modules.pop("asyncpg", None)
        try:
            store = PostgresStateStore("postgresql://fake/db")
            with pytest.raises(ImportError, match="asyncpg"):
                asyncio.run(store.init())
        finally:
            if original is not None:
                sys.modules["asyncpg"] = original

    def test_postgres_and_sqlite_share_serialisation_helpers(self):
        """
        _serialize_graph / _deserialize_graph are shared between backends.
        Round-trip through both directions to confirm no divergence.
        """
        from batchflow.state import _serialize_graph, _deserialize_graph

        g = make_graph()
        g.node("a").state = NodeState.SUCCEEDED
        g.node("a").submit_id = "12345"
        g.node("a").restart_count = 2

        serialised = _serialize_graph(g)
        restored   = _deserialize_graph(serialised)

        assert restored.node("a").state         == NodeState.SUCCEEDED
        assert restored.node("a").submit_id     == "12345"
        assert restored.node("a").restart_count == 2
        assert restored.node("b").state         == NodeState.PENDING
