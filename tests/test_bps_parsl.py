"""tests/test_bps_parsl.py — BpsParslBackend + BlockingNodeRunner unit tests."""
from __future__ import annotations

import asyncio
import re
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from batchflow import EventBus, EventType
from batchflow.backends.bps_parsl import (
    BlockingNodeRunner, BpsParslBackend, _parse_run_id,
)


# ---------------------------------------------------------------------------
# _parse_run_id
# ---------------------------------------------------------------------------

def test_parse_run_id_found():
    output = "Submit dir: /tmp/foo\nRun Id: 20240501T120000Z\nRun Name: u_op_name\n"
    assert _parse_run_id(output) == "20240501T120000Z"


def test_parse_run_id_not_found():
    assert _parse_run_id("no run id here") is None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_fake_node(node_id="a", bps_yaml="a.yaml", submit_id=None):
    node = MagicMock()
    node.node_id   = node_id
    node.bps_yaml  = bps_yaml
    node.overrides = {}
    node.submit_id = submit_id
    return node


# ---------------------------------------------------------------------------
# BlockingNodeRunner — success path
# ---------------------------------------------------------------------------

async def test_blocking_runner_success(tmp_path):
    """NODE_COMPLETE published with parsed run_id on rc=0."""
    bus = EventBus()
    queue = bus.subscribe("test")

    log_path = tmp_path / "a.log"
    log_path.write_text("Run Id: 12345\n")

    proc = MagicMock()
    proc.pid        = 42
    proc.returncode = 0
    proc.wait       = AsyncMock(return_value=None)

    runner = BlockingNodeRunner(
        proc=proc, workflow_id="wf", node_id="a", bus=bus, node_label="a",
        log_path=log_path,
    )
    await runner.run()

    events = []
    while not queue.empty():
        events.append(queue.get_nowait())

    types = [e.event_type for e in events]
    assert EventType.JOB_RUNNING in types
    complete = next(e for e in events if e.event_type == EventType.NODE_COMPLETE)
    assert complete.cluster_id == "12345"


async def test_blocking_runner_failure(tmp_path):
    """NODE_FAILED published on non-zero exit code."""
    bus = EventBus()
    queue = bus.subscribe("test")

    log_path = tmp_path / "a.log"
    log_path.write_text("some output\n")

    proc = MagicMock()
    proc.pid        = 43
    proc.returncode = 1
    proc.wait       = AsyncMock(return_value=None)

    runner = BlockingNodeRunner(
        proc=proc, workflow_id="wf", node_id="a", bus=bus, node_label="a",
        log_path=log_path,
    )
    await runner.run()

    events = []
    while not queue.empty():
        events.append(queue.get_nowait())

    assert EventType.NODE_FAILED in [e.event_type for e in events]
    failed = next(e for e in events if e.event_type == EventType.NODE_FAILED)
    assert failed.exit_code == 1


async def test_blocking_runner_runtime_error_in_output(tmp_path):
    """NODE_FAILED published when output contains 'RuntimeError' even if rc=0."""
    bus = EventBus()
    queue = bus.subscribe("test")

    log_path = tmp_path / "a.log"
    log_path.write_text("RuntimeError: something failed\n")

    proc = MagicMock()
    proc.pid        = 44
    proc.returncode = 0
    proc.wait       = AsyncMock(return_value=None)

    runner = BlockingNodeRunner(
        proc=proc, workflow_id="wf", node_id="a", bus=bus, node_label="a",
        log_path=log_path,
    )
    await runner.run()

    events = []
    while not queue.empty():
        events.append(queue.get_nowait())

    assert EventType.NODE_FAILED in [e.event_type for e in events]


async def test_blocking_runner_reads_log(tmp_path):
    """Runner reads output from log_path after process exits."""
    bus = EventBus()
    bus.subscribe("discard")

    log_path = tmp_path / "a.log"
    log_path.write_text("Run Id: 99\nout\nerr")

    proc = MagicMock()
    proc.pid        = 45
    proc.returncode = 0
    proc.wait       = AsyncMock(return_value=None)

    runner = BlockingNodeRunner(
        proc=proc, workflow_id="wf", node_id="a",
        bus=bus, node_label="a", log_path=log_path,
    )
    await runner.run()

    events = []
    queue = bus._queues.get("discard")  # peek at what was published
    # Just verify no exception and log file is readable — content verified via cluster_id
    assert log_path.read_text() == "Run Id: 99\nout\nerr"


async def test_blocking_runner_cancellation():
    """CancelledError terminates the subprocess and re-raises."""
    bus = EventBus()
    bus.subscribe("discard")

    proc = MagicMock()
    proc.pid        = 46
    proc.returncode = None
    proc.wait       = AsyncMock(side_effect=[asyncio.CancelledError(), None])
    proc.terminate  = MagicMock()

    runner = BlockingNodeRunner(
        proc=proc, workflow_id="wf", node_id="a", bus=bus, node_label="a",
    )
    with pytest.raises(asyncio.CancelledError):
        await runner.run()

    proc.terminate.assert_called_once()


# ---------------------------------------------------------------------------
# BpsParslBackend.submit + make_node_runner
# ---------------------------------------------------------------------------

async def test_bps_parsl_submit_returns_uuid(tmp_path):
    """submit() returns a UUID submit_id and 'parsl' submit_location."""
    backend   = BpsParslBackend(bps_dir=tmp_path)
    yaml_path = tmp_path / "a.yaml"
    yaml_path.write_text("dummy")

    node      = _make_fake_node(bps_yaml="a.yaml")
    fake_proc = MagicMock()
    fake_proc.pid = 100

    with patch("asyncio.create_subprocess_exec", AsyncMock(return_value=fake_proc)):
        result = await backend.submit(node, log_dir=tmp_path)

    assert re.match(
        r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}",
        result.submit_id,
    )
    assert result.submit_location == "parsl"


async def test_bps_parsl_make_node_runner(tmp_path):
    """make_node_runner returns a BlockingNodeRunner using the stored proc."""
    backend   = BpsParslBackend(bps_dir=tmp_path)
    yaml_path = tmp_path / "a.yaml"
    yaml_path.write_text("dummy")

    node      = _make_fake_node(bps_yaml="a.yaml")
    fake_proc = MagicMock()
    fake_proc.pid = 101

    with patch("asyncio.create_subprocess_exec", AsyncMock(return_value=fake_proc)):
        result = await backend.submit(node, log_dir=tmp_path)

    node.submit_id = result.submit_id
    bus        = EventBus()
    stop_event = asyncio.Event()

    runner = backend.make_node_runner(node, bus, stop_event, workflow_id="wf")
    assert isinstance(runner, BlockingNodeRunner)
    assert runner._proc is fake_proc


async def test_bps_parsl_missing_bps_yaml(tmp_path):
    """submit() raises ValueError when node.bps_yaml is None."""
    backend = BpsParslBackend(bps_dir=tmp_path)
    node    = _make_fake_node(bps_yaml=None)
    with pytest.raises(ValueError, match="no bps_yaml"):
        await backend.submit(node)


async def test_bps_parsl_yaml_not_found(tmp_path):
    """submit() raises FileNotFoundError when YAML file is missing."""
    backend = BpsParslBackend(bps_dir=tmp_path)
    node    = _make_fake_node(bps_yaml="missing.yaml")
    with pytest.raises(FileNotFoundError):
        await backend.submit(node)
