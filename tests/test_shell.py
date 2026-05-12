"""tests/test_shell.py — ShellBackend + ShellNodeRunner unit tests."""
from __future__ import annotations

import asyncio
import re
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from batchflow import EventBus, EventType
from batchflow.backends.shell import ShellBackend, ShellNodeRunner


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_fake_node(node_id="a", command="/bin/true", submit_id=None):
    node = MagicMock()
    node.node_id   = node_id
    node.command   = command
    node.submit_id = submit_id
    return node


# ---------------------------------------------------------------------------
# ShellNodeRunner — success path
# ---------------------------------------------------------------------------

async def test_shell_runner_success():
    """NODE_COMPLETE published on rc=0."""
    bus   = EventBus()
    queue = bus.subscribe("test")

    proc = MagicMock()
    proc.pid        = 42
    proc.returncode = 0
    proc.wait       = AsyncMock(return_value=None)

    runner = ShellNodeRunner(
        proc=proc, workflow_id="wf", node_id="a", bus=bus, node_label="a",
    )
    await runner.run()

    events = []
    while not queue.empty():
        events.append(queue.get_nowait())

    types = [e.event_type for e in events]
    assert EventType.JOB_RUNNING in types
    assert EventType.NODE_COMPLETE in types
    assert EventType.NODE_FAILED not in types


async def test_shell_runner_failure():
    """NODE_FAILED published on non-zero exit code."""
    bus   = EventBus()
    queue = bus.subscribe("test")

    proc = MagicMock()
    proc.pid        = 43
    proc.returncode = 2
    proc.wait       = AsyncMock(return_value=None)

    runner = ShellNodeRunner(
        proc=proc, workflow_id="wf", node_id="a", bus=bus, node_label="a",
    )
    await runner.run()

    events = []
    while not queue.empty():
        events.append(queue.get_nowait())

    assert EventType.NODE_FAILED in [e.event_type for e in events]
    failed = next(e for e in events if e.event_type == EventType.NODE_FAILED)
    assert failed.exit_code == 2


async def test_shell_runner_log_written_by_subprocess(tmp_path):
    """Log file written by the subprocess is accessible after the runner completes."""
    bus = EventBus()
    bus.subscribe("discard")

    log_path = tmp_path / "a.log"
    log_path.write_bytes(b"out\nerr\n")  # simulate subprocess writing to the file

    proc = MagicMock()
    proc.pid        = 44
    proc.returncode = 0
    proc.wait       = AsyncMock(return_value=None)

    runner = ShellNodeRunner(
        proc=proc, workflow_id="wf", node_id="a",
        bus=bus, node_label="a", log_path=log_path,
    )
    await runner.run()

    assert log_path.read_text() == "out\nerr\n"


async def test_shell_runner_cancellation():
    """CancelledError terminates the subprocess and re-raises."""
    bus = EventBus()
    bus.subscribe("discard")

    proc = MagicMock()
    proc.pid        = 45
    proc.returncode = None
    proc.wait       = AsyncMock(side_effect=[asyncio.CancelledError(), None])
    proc.terminate  = MagicMock()

    runner = ShellNodeRunner(
        proc=proc, workflow_id="wf", node_id="a", bus=bus, node_label="a",
    )
    with pytest.raises(asyncio.CancelledError):
        await runner.run()

    proc.terminate.assert_called_once()


# ---------------------------------------------------------------------------
# ShellBackend.submit + make_node_runner
# ---------------------------------------------------------------------------

async def test_shell_backend_submit_returns_uuid(tmp_path):
    """submit() returns a UUID submit_id and 'shell' submit_location."""
    backend   = ShellBackend()
    node      = _make_fake_node(command="/bin/echo hello")
    fake_proc = MagicMock()
    fake_proc.pid = 100

    with patch("asyncio.create_subprocess_exec", AsyncMock(return_value=fake_proc)):
        result = await backend.submit(node, log_dir=tmp_path)

    assert re.match(
        r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}",
        result.submit_id,
    )
    assert result.submit_location == "shell"


async def test_shell_backend_make_node_runner(tmp_path):
    """make_node_runner returns a ShellNodeRunner using the stored proc."""
    backend   = ShellBackend()
    node      = _make_fake_node(command="/bin/echo hello")
    fake_proc = MagicMock()
    fake_proc.pid = 101

    with patch("asyncio.create_subprocess_exec", AsyncMock(return_value=fake_proc)):
        result = await backend.submit(node, log_dir=tmp_path)

    node.submit_id = result.submit_id
    bus        = EventBus()
    stop_event = asyncio.Event()

    runner = backend.make_node_runner(node, bus, stop_event, workflow_id="wf")
    assert isinstance(runner, ShellNodeRunner)
    assert runner._proc is fake_proc


async def test_shell_backend_missing_command():
    """submit() raises ValueError when node.command is None."""
    backend = ShellBackend()
    node    = _make_fake_node(command=None)
    with pytest.raises(ValueError, match="no command"):
        await backend.submit(node)


# ---------------------------------------------------------------------------
# ShellBackend.restart
# ---------------------------------------------------------------------------

async def test_shell_backend_restart_reruns_same_command(tmp_path):
    """restart() starts a new subprocess with the same command."""
    backend   = ShellBackend()
    node      = _make_fake_node(command="/bin/echo hello")
    fake_proc = MagicMock()
    fake_proc.pid = 102

    with patch("asyncio.create_subprocess_exec", AsyncMock(return_value=fake_proc)):
        result = await backend.submit(node, log_dir=tmp_path)
        old_submit_id = result.submit_id

    fake_proc2 = MagicMock()
    fake_proc2.pid = 103

    with patch("asyncio.create_subprocess_exec", AsyncMock(return_value=fake_proc2)) as mock_exec:
        result2 = await backend.restart(old_submit_id)
        # Verify shlex-split command was passed
        args = mock_exec.call_args[0]
        assert args == ("/bin/echo", "hello")

    assert result2.submit_id != old_submit_id
    assert result2.submit_location == "shell"
    # New submit_id must be resolvable by make_node_runner
    assert backend._procs.get(result2.submit_id) is fake_proc2


async def test_shell_backend_restart_unknown_id():
    """restart() raises KeyError for an unrecognised submit_id."""
    backend = ShellBackend()
    with pytest.raises(KeyError, match="No command found"):
        await backend.restart("nonexistent-id")


# ---------------------------------------------------------------------------
# ShellBackend.remove
# ---------------------------------------------------------------------------

async def test_shell_backend_remove_terminates_running_proc(tmp_path):
    """remove() terminates a still-running process."""
    backend   = ShellBackend()
    node      = _make_fake_node(command="/bin/sleep 100")
    fake_proc = MagicMock()
    fake_proc.pid        = 104
    fake_proc.returncode = None
    fake_proc.terminate  = MagicMock()
    fake_proc.wait       = AsyncMock(return_value=None)

    with patch("asyncio.create_subprocess_exec", AsyncMock(return_value=fake_proc)):
        result = await backend.submit(node, log_dir=tmp_path)

    await backend.remove(result.submit_id)
    fake_proc.terminate.assert_called_once()


# ---------------------------------------------------------------------------
# End-to-end: real subprocess (echo)
# ---------------------------------------------------------------------------

async def test_shell_runner_real_subprocess(tmp_path):
    """Integration: run a real /bin/echo subprocess through ShellNodeRunner."""
    log_path = tmp_path / "echo.log"
    log_f    = open(log_path, "wb")
    proc     = await asyncio.create_subprocess_exec(
        "/bin/echo", "hello",
        stdout=log_f,
        stderr=log_f,
    )
    log_f.close()

    bus   = EventBus()
    queue = bus.subscribe("test")

    runner = ShellNodeRunner(
        proc=proc, workflow_id="wf", node_id="echo", bus=bus, node_label="echo",
        log_path=log_path,
    )
    await runner.run()

    events = []
    while not queue.empty():
        events.append(queue.get_nowait())

    assert EventType.NODE_COMPLETE in [e.event_type for e in events]
    assert "hello" in log_path.read_text()
