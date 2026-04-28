"""
tests/test_code_agent_callback.py

Exercises CodeAgentRunner, RestartNodeTool, and make_code_agent_callback
without the full WorkflowRunner.  AgentHandler is driven via direct event
injection.
"""
from __future__ import annotations

import asyncio
import logging
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from batchflow import (
    WorkflowGraph, PipelineNode, NodeState,
    EventBus, EventType, JobEvent,
    SqliteStateStore,
    ErrorClassifier,
    AgentHandler, InterventionActions,
    CallbackTransport,
    MockBackend,
)
from batchflow.code_agent import (
    BatchflowTool, RestartNodeTool, CodeAgentRunner, make_code_agent_callback,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

async def _make_fixtures(tmp_path, node_id="a", node_state=NodeState.HELD,
                         submit_id="999", max_restarts=2):
    g = WorkflowGraph("cb_test")
    g.add_node(PipelineNode(node_id, f"{node_id}.yaml", max_restarts=max_restarts))
    g.node(node_id).state = node_state
    g.node(node_id).submit_id = submit_id

    bus = EventBus()
    backend = MockBackend()
    store = SqliteStateStore(tmp_path / "test.db")
    await store.init()

    interventions = InterventionActions(
        graph=g, backend=backend, bus=bus,
        store=store, bps_dir=Path("/fake"),
    )
    return g, bus, backend, store, interventions


async def _run_handler_until(agent_handler, condition, timeout=10.0):
    """Run agent_handler.run() as a background task until condition() is true."""
    handler_task = asyncio.create_task(agent_handler.run())
    try:
        deadline = asyncio.get_event_loop().time() + timeout
        while asyncio.get_event_loop().time() < deadline:
            await asyncio.sleep(0.05)
            if condition():
                return
        raise AssertionError("Condition never became true within timeout")
    finally:
        handler_task.cancel()
        await asyncio.gather(handler_task, return_exceptions=True)


# ---------------------------------------------------------------------------
# RestartNodeTool construction and validation
# ---------------------------------------------------------------------------

async def test_restart_tool_initializes():
    """RestartNodeTool must pass smolagents validation: inputs match forward()."""
    interventions = AsyncMock(spec=InterventionActions)
    tool = RestartNodeTool(interventions)

    assert tool.name == "restart_node"
    assert tool.output_type == "null"
    assert set(tool.inputs.keys()) == {"node_id", "reason", "actor"}
    for key, spec in tool.inputs.items():
        assert "type" in spec
        assert "description" in spec


# ---------------------------------------------------------------------------
# RestartNodeTool.forward
# ---------------------------------------------------------------------------

async def test_forward_calls_restart_node():
    """forward() must invoke interventions.restart_node with correct args."""
    loop = asyncio.get_running_loop()
    interventions = AsyncMock(spec=InterventionActions)
    tool = RestartNodeTool(interventions)
    tool.set_loop(loop)

    await asyncio.to_thread(tool.forward, "stage_1a", "disk failure", "test_actor")

    interventions.restart_node.assert_awaited_once_with(
        "stage_1a", "disk failure", "test_actor"
    )


async def test_forward_propagates_exception():
    """forward() must not swallow exceptions from restart_node."""
    loop = asyncio.get_running_loop()
    interventions = AsyncMock(spec=InterventionActions)
    interventions.restart_node.side_effect = RuntimeError("not restartable")
    tool = RestartNodeTool(interventions)
    tool.set_loop(loop)

    with pytest.raises(RuntimeError, match="not restartable"):
        await asyncio.to_thread(tool.forward, "stage_1a")


# ---------------------------------------------------------------------------
# CodeAgentRunner — happy paths
# ---------------------------------------------------------------------------

async def test_code_agent_runner_restarts_held_node(tmp_path):
    """CodeAgentRunner.run triggered by NODE_HELD should set node to SUBMITTED."""
    g, bus, backend, store, interventions = await _make_fixtures(
        tmp_path, node_state=NodeState.HELD
    )
    runner = CodeAgentRunner([RestartNodeTool(interventions)], use_agent=False)
    agent_handler = AgentHandler(
        bus=bus, graph=g, classifier=ErrorClassifier(),
        transports=[CallbackTransport(runner.run)],
        interventions=interventions,
    )

    await bus.publish(JobEvent(
        event_type=EventType.NODE_HELD, workflow_id="cb_test", node_id="a",
    ))
    await _run_handler_until(agent_handler, lambda: g.node("a").state == NodeState.SUBMITTED)

    assert g.node("a").state == NodeState.SUBMITTED


async def test_code_agent_runner_restarts_failed_node(tmp_path):
    """CodeAgentRunner.run triggered by NODE_FAILED should call backend.restart."""
    g, bus, backend, store, interventions = await _make_fixtures(
        tmp_path, node_state=NodeState.FAILED
    )
    runner = CodeAgentRunner([RestartNodeTool(interventions)], use_agent=False)
    agent_handler = AgentHandler(
        bus=bus, graph=g, classifier=ErrorClassifier(),
        transports=[CallbackTransport(runner.run)],
        interventions=interventions,
    )

    await bus.publish(JobEvent(
        event_type=EventType.NODE_FAILED, workflow_id="cb_test", node_id="a",
    ))
    await _run_handler_until(agent_handler, lambda: g.node("a").state == NodeState.SUBMITTED)

    assert g.node("a").state == NodeState.SUBMITTED
    assert any("restarted_from" in r for r in backend.submitted)


# ---------------------------------------------------------------------------
# CodeAgentRunner — error handling
# ---------------------------------------------------------------------------

async def test_code_agent_runner_logs_error_on_non_restartable_node(tmp_path, caplog):
    """Runner on a node at max_restarts should log an error, not raise."""
    g, bus, backend, store, interventions = await _make_fixtures(
        tmp_path, node_state=NodeState.FAILED, max_restarts=1
    )
    g.node("a").restart_count = 1  # already at max

    runner = CodeAgentRunner([RestartNodeTool(interventions)], use_agent=False)
    agent_handler = AgentHandler(
        bus=bus, graph=g, classifier=ErrorClassifier(),
        transports=[CallbackTransport(runner.run)],
        interventions=interventions,
    )

    await bus.publish(JobEvent(
        event_type=EventType.NODE_FAILED, workflow_id="cb_test", node_id="a",
    ))

    with caplog.at_level(logging.ERROR, logger="batchflow.code_agent"):
        handler_task = asyncio.create_task(agent_handler.run())
        await asyncio.sleep(1.0)
        handler_task.cancel()
        await asyncio.gather(handler_task, return_exceptions=True)

    assert any("restart failed" in r.message.lower() for r in caplog.records)
    assert g.node("a").state == NodeState.FAILED  # unchanged


# ---------------------------------------------------------------------------
# CodeAgentRunner — semaphore concurrency limit
# ---------------------------------------------------------------------------

async def test_code_agent_runner_concurrent_limit(tmp_path):
    """At most 3 restart_node calls should be in-flight simultaneously."""
    g = WorkflowGraph("sem_test")
    for i in range(4):
        g.add_node(PipelineNode(f"n{i}", f"n{i}.yaml", max_restarts=2))
        g.node(f"n{i}").state = NodeState.HELD
        g.node(f"n{i}").submit_id = str(900 + i)

    bus = EventBus()
    backend = MockBackend()
    store = SqliteStateStore(tmp_path / "test.db")
    await store.init()

    in_flight = 0
    max_in_flight = 0
    gate = asyncio.Event()

    async def slow_restart(node_id, reason="", actor="agent"):
        nonlocal in_flight, max_in_flight
        in_flight += 1
        max_in_flight = max(max_in_flight, in_flight)
        await gate.wait()
        in_flight -= 1
        g.node(node_id).state = NodeState.SUBMITTED

    interventions = InterventionActions(
        graph=g, backend=backend, bus=bus,
        store=store, bps_dir=Path("/fake"),
    )
    interventions.restart_node = slow_restart

    # Fresh CodeAgentRunner instance — no shared global semaphore to reset.
    runner = CodeAgentRunner([RestartNodeTool(interventions)], use_agent=False)
    agent_handler = AgentHandler(
        bus=bus, graph=g, classifier=ErrorClassifier(),
        transports=[CallbackTransport(runner.run)],
        interventions=interventions,
    )

    handler_task = asyncio.create_task(agent_handler.run())
    try:
        for i in range(4):
            await bus.publish(JobEvent(
                event_type=EventType.NODE_HELD,
                workflow_id="sem_test",
                node_id=f"n{i}",
            ))

        await asyncio.sleep(0.5)
        assert max_in_flight <= 3, f"Expected ≤3 concurrent calls, got {max_in_flight}"

        gate.set()
        await asyncio.sleep(0.5)
    finally:
        handler_task.cancel()
        await asyncio.gather(handler_task, return_exceptions=True)


# ---------------------------------------------------------------------------
# CodeAgentRunner — workflow-level sentinel
# ---------------------------------------------------------------------------

async def test_code_agent_runner_ignores_workflow_sentinel(tmp_path):
    """WORKFLOW_COMPLETE/STALLED carry node_id='__workflow__'; runner must skip them."""
    g, bus, backend, store, interventions = await _make_fixtures(tmp_path)
    interventions.restart_node = AsyncMock()
    runner = CodeAgentRunner([RestartNodeTool(interventions)], use_agent=False)

    from batchflow.agent import AgentNotification
    from batchflow.classifier import Classification
    from datetime import datetime, timezone

    notification = AgentNotification(
        event_type="WORKFLOW_COMPLETE",
        workflow_id="cb_test",
        node_id="__workflow__",
        timestamp=datetime.now(timezone.utc),
        hold_reasons=[],
        classification=Classification("unknown", 0.0, [], None),
        restart_count=0,
        max_restarts=0,
        dag_context={},
        bps_yaml="",
        metadata={},
    )
    await runner.run(notification)
    await asyncio.sleep(0.1)
    interventions.restart_node.assert_not_awaited()


# ---------------------------------------------------------------------------
# make_code_agent_callback shim
# ---------------------------------------------------------------------------

async def test_make_code_agent_callback_shim(tmp_path):
    """make_code_agent_callback returns a CodeAgentRunner.run bound to interventions."""
    # Make sure DONT_USE_AGENT env var is not set.
    import os
    if "DONT_USE_AGENT" in os.environ:
        del os.environ["DONT_USE_AGENT"]

    g, bus, backend, store, interventions = await _make_fixtures(
        tmp_path, node_state=NodeState.HELD
    )
    # Patch _get_model so no live API key is needed; _build_agent returns a mock agent.
    mock_agent = MagicMock()
    mock_agent.run.return_value = None

    with patch("batchflow.code_agent.CodeAgentRunner._build_agent",
               return_value=mock_agent):
        callback = make_code_agent_callback(interventions)
        agent_handler = AgentHandler(
            bus=bus, graph=g, classifier=ErrorClassifier(),
            transports=[CallbackTransport(callback)],
            interventions=interventions,
        )

        await bus.publish(JobEvent(
            event_type=EventType.NODE_HELD, workflow_id="cb_test", node_id="a",
        ))
        await _run_handler_until(
            agent_handler, lambda: mock_agent.run.called, timeout=5.0
        )

    mock_agent.run.assert_called_once()
    call_arg = mock_agent.run.call_args[0][0]
    assert call_arg.startswith("Please handle ")
    assert '"node_id"' in call_arg


# ---------------------------------------------------------------------------
# CodeAgentRunner — use_agent=True path
# ---------------------------------------------------------------------------

async def test_code_agent_runner_uses_agent(tmp_path):
    """With use_agent=True, runner calls agent.run() with the serialised notification."""
    g, bus, backend, store, interventions = await _make_fixtures(
        tmp_path, node_state=NodeState.HELD
    )
    runner = CodeAgentRunner([RestartNodeTool(interventions)], use_agent=True)

    mock_agent = MagicMock()
    mock_agent.run.return_value = None

    agent_handler = AgentHandler(
        bus=bus, graph=g, classifier=ErrorClassifier(),
        transports=[CallbackTransport(runner.run)],
        interventions=interventions,
    )

    with patch.object(runner, "_build_agent", return_value=mock_agent):
        await bus.publish(JobEvent(
            event_type=EventType.NODE_HELD, workflow_id="cb_test", node_id="a",
        ))
        await _run_handler_until(
            agent_handler, lambda: mock_agent.run.called, timeout=5.0
        )

    mock_agent.run.assert_called_once()
    call_arg = mock_agent.run.call_args[0][0]
    assert call_arg.startswith("Please handle ")
    assert '"node_id"' in call_arg


# ---------------------------------------------------------------------------
# CodeAgentRunner — timeout
# ---------------------------------------------------------------------------

async def test_code_agent_runner_timeout(tmp_path, caplog):
    """agent_timeout fires when agent.run() hangs; error is logged, no exception raised."""
    import time
    g, bus, backend, store, interventions = await _make_fixtures(
        tmp_path, node_state=NodeState.HELD
    )
    runner = CodeAgentRunner(
        [RestartNodeTool(interventions)], use_agent=True, agent_timeout=0.1
    )

    mock_agent = MagicMock()
    mock_agent.run.side_effect = lambda _prompt: time.sleep(10)

    agent_handler = AgentHandler(
        bus=bus, graph=g, classifier=ErrorClassifier(),
        transports=[CallbackTransport(runner.run)],
        interventions=interventions,
    )

    with patch.object(runner, "_build_agent", return_value=mock_agent):
        with caplog.at_level(logging.ERROR, logger="batchflow.code_agent"):
            await bus.publish(JobEvent(
                event_type=EventType.NODE_HELD, workflow_id="cb_test", node_id="a",
            ))
            await _run_handler_until(
                agent_handler,
                lambda: any("timed out" in r.message for r in caplog.records),
                timeout=5.0,
            )

    assert any("timed out" in r.message for r in caplog.records)
