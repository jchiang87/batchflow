"""tests/test_dispatch.py — DispatchBackend + updated loader tests."""
from __future__ import annotations

import asyncio
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

from batchflow import EventBus, NodeState
from batchflow.backends.bps import MockBackend, SubmissionResult
from batchflow.backends.dispatch import DispatchBackend
from batchflow.loader import load_workflow


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_node(node_id="a", node_type="bps", submit_id=None):
    node = MagicMock()
    node.node_id   = node_id
    node.node_type = node_type
    node.submit_id = submit_id
    return node


def _mock_backend(submit_id="42", submit_location="mock"):
    backend = MagicMock(spec=MockBackend)
    backend.submit       = AsyncMock(return_value=SubmissionResult(submit_id, submit_location))
    backend.restart      = AsyncMock(return_value=SubmissionResult(submit_id + "_r", submit_location))
    backend.release_held = AsyncMock()
    backend.remove       = AsyncMock()
    backend.make_node_runner = MagicMock(return_value=MagicMock())
    return backend


# ---------------------------------------------------------------------------
# DispatchBackend routing
# ---------------------------------------------------------------------------

async def test_dispatch_submit_routes_to_correct_backend():
    """submit() calls the backend registered for node.node_type."""
    bps_b   = _mock_backend("100", "htcondor")
    shell_b = _mock_backend("200", "shell")
    dispatch = DispatchBackend({"bps": bps_b, "shell": shell_b})

    bps_node   = _make_node(node_type="bps")
    shell_node = _make_node(node_type="shell")

    await dispatch.submit(bps_node)
    await dispatch.submit(shell_node)

    bps_b.submit.assert_awaited_once_with(bps_node, log_dir=None)
    shell_b.submit.assert_awaited_once_with(shell_node, log_dir=None)


async def test_dispatch_submit_registers_owner():
    """submit() registers the returned submit_id → node_type in _owners."""
    bps_b    = _mock_backend("100")
    dispatch = DispatchBackend({"bps": bps_b})

    node = _make_node(node_type="bps")
    await dispatch.submit(node)

    assert dispatch._owners.get("100") == "bps"


async def test_dispatch_make_node_runner_routes_and_registers():
    """make_node_runner dispatches by node.node_type and populates _owners."""
    bps_b    = _mock_backend()
    dispatch = DispatchBackend({"bps": bps_b})

    node = _make_node(node_type="bps", submit_id="99")
    bus  = EventBus()

    dispatch.make_node_runner(node, bus, asyncio.Event(), workflow_id="wf")

    bps_b.make_node_runner.assert_called_once()
    assert dispatch._owners.get("99") == "bps"


async def test_dispatch_release_held_routes():
    """release_held routes to the backend that owns the submit_id."""
    shell_b  = _mock_backend("200", "shell")
    dispatch = DispatchBackend({"shell": shell_b})
    dispatch._owners["200"] = "shell"

    await dispatch.release_held("200")
    shell_b.release_held.assert_awaited_once_with("200")


async def test_dispatch_remove_routes():
    """remove routes to the backend that owns the submit_id."""
    bps_b    = _mock_backend("100")
    dispatch = DispatchBackend({"bps": bps_b})
    dispatch._owners["100"] = "bps"

    await dispatch.remove("100")
    bps_b.remove.assert_awaited_once_with("100")


async def test_dispatch_restart_routes_and_reregisters():
    """restart routes to the correct backend and registers the new submit_id."""
    bps_b    = _mock_backend("100")
    dispatch = DispatchBackend({"bps": bps_b})
    dispatch._owners["100"] = "bps"

    result = await dispatch.restart("100")

    bps_b.restart.assert_awaited_once_with("100")
    assert dispatch._owners.get(result.submit_id) == "bps"


async def test_dispatch_unknown_node_type_raises():
    """submit() raises KeyError for an unregistered node_type."""
    dispatch = DispatchBackend({"bps": _mock_backend()})
    node = _make_node(node_type="parsl")

    with pytest.raises(KeyError, match="parsl"):
        await dispatch.submit(node)


async def test_dispatch_unknown_submit_id_raises():
    """release_held/remove/restart raise KeyError for an unregistered submit_id."""
    dispatch = DispatchBackend({"bps": _mock_backend()})

    with pytest.raises(KeyError):
        await dispatch.remove("unknown-id")


async def test_dispatch_make_node_runner_populates_owners_for_resume():
    """make_node_runner registers submit_id even when submit() was not called
    this session (resume scenario)."""
    bps_b    = _mock_backend()
    dispatch = DispatchBackend({"bps": bps_b})
    assert "resume-id" not in dispatch._owners

    node = _make_node(node_type="bps", submit_id="resume-id")
    dispatch.make_node_runner(node, EventBus(), asyncio.Event())

    assert dispatch._owners["resume-id"] == "bps"


# ---------------------------------------------------------------------------
# Loader — shell node support
# ---------------------------------------------------------------------------

def test_load_shell_node(tmp_path):
    """loader accepts node_type: shell with a command field."""
    wf_file = tmp_path / "wf.yaml"
    wf_file.write_text("""
workflow: mixed
nodes:
  - id: prep
    node_type: shell
    command: /bin/echo hello
  - id: process
    bps_yaml: bps_process.yaml
    depends_on: [prep]
""")
    g = load_workflow(wf_file)
    assert g.node("prep").node_type == "shell"
    assert g.node("prep").command   == "/bin/echo hello"
    assert g.node("prep").bps_yaml  is None
    assert g.node("process").node_type == "bps"
    assert g.node("process").bps_yaml  == "bps_process.yaml"


def test_load_shell_node_missing_command(tmp_path):
    """loader raises ValueError when a shell node has no command."""
    wf_file = tmp_path / "bad.yaml"
    wf_file.write_text("""
workflow: bad
nodes:
  - id: oops
    node_type: shell
""")
    with pytest.raises(ValueError, match="command"):
        load_workflow(wf_file)


def test_load_bps_node_missing_bps_yaml(tmp_path):
    """loader raises ValueError when a bps node has no bps_yaml (unchanged)."""
    wf_file = tmp_path / "bad.yaml"
    wf_file.write_text("""
workflow: bad
nodes:
  - id: oops
""")
    with pytest.raises(ValueError, match="bps_yaml"):
        load_workflow(wf_file)


def test_load_overrides_key(tmp_path):
    """loader accepts 'overrides' as well as legacy 'bps_overrides'."""
    wf_file = tmp_path / "wf.yaml"
    wf_file.write_text("""
workflow: ov_test
nodes:
  - id: a
    bps_yaml: a.yaml
    overrides:
      requestMemory: "8G"
  - id: b
    bps_yaml: b.yaml
    bps_overrides:
      requestMemory: "4G"
""")
    g = load_workflow(wf_file)
    assert g.node("a").overrides == {"requestMemory": "8G"}
    assert g.node("b").overrides == {"requestMemory": "4G"}
