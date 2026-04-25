"""
cli.py — Command-line interface for batchflow.

Commands
--------
submit      Load a workflow YAML and start a run.
status      Print current node states from the StateStore.
intervene   Trigger a restart / skip / abort on a specific node.
workflows   List all known workflows in the StateStore.
"""
from __future__ import annotations

import asyncio
import os
import sys
from pathlib import Path

import click

from .loader import load_workflow
from .state import SqliteStateStore


def _db_from_workdir(work_dir: Path) -> Path:
    return work_dir / "batchflow.db"


def _require_env(*names: str) -> None:
    missing = [n for n in names if not os.environ.get(n)]
    if missing:
        click.echo(click.style(f"Missing env vars: {missing}", fg="red"), err=True)
        sys.exit(1)


@click.group()
@click.version_option()
def cli():
    """batchflow — async DAG workflow runner for HTCondor/BPS."""


# ---------------------------------------------------------------------------
# submit
# ---------------------------------------------------------------------------

@cli.command()
@click.argument("workflow_yaml", type=click.Path(exists=True))
@click.option("--work-dir", "-w", default=".", show_default=True)
@click.option("--bps-source", envvar="EO_PIPE_DIR")
@click.option("--instrument", envvar="INSTRUMENT_NAME", default="LSSTCam",
              show_default=True)
@click.option("--poll-interval", default=60.0, show_default=True,
              help="Seconds between condor_q polls.")
@click.option("--dry-run", is_flag=True)
@click.option("--no-confirm", is_flag=True)
@click.option("--auto-restart", is_flag=True,
              help="Automatically restart transient failures.")
@click.option("--webhook", default=None, envvar="BATCHFLOW_WEBHOOK")
def submit(workflow_yaml, work_dir, bps_source, instrument,
           poll_interval, dry_run, no_confirm, auto_restart, webhook):
    """Load WORKFLOW_YAML and execute the workflow."""
    work_dir = Path(work_dir)
    graph = load_workflow(workflow_yaml)

    click.echo(f"\nWorkflow:   {graph.workflow_id}")
    click.echo(f"Work dir:   {work_dir}")
    click.echo(f"Instrument: {instrument}")
    click.echo(f"\nNodes ({len(graph.nodes)}):")
    for node in graph.nodes:
        deps = f"  <- {node.depends_on}" if node.depends_on else ""
        click.echo(f"  {node.node_id:<30}  {node.bps_yaml}{deps}")
    click.echo()

    if dry_run:
        click.echo(click.style("Dry run — not submitting.", fg="yellow"))
        return

    if not no_confirm and not click.confirm("Proceed?", default=True):
        click.echo("Aborted.")
        return

    asyncio.run(_run_workflow(
        graph=graph,
        work_dir=work_dir,
        bps_source=bps_source,
        instrument=instrument,
        poll_interval=poll_interval,
        auto_restart=auto_restart,
        webhook=webhook,
    ))


async def _run_workflow(graph, work_dir, bps_source, instrument,
                        poll_interval, auto_restart, webhook):
    from .agent import (AgentHandler, InterventionActions,
                        StdoutTransport, WebhookTransport)
    from .backends.bps import BpsBackend
    from .bus import EventBus
    from .classifier import ErrorClassifier
    from .monitor import TimerWakeStrategy
    from .runner import WorkflowRunner
    from .workspace import WorkspaceManager

    bps_source_dir = (
        Path(bps_source) / "bps" if bps_source else Path("./bps")
    )
    ws = WorkspaceManager(
        source_bps_dir=bps_source_dir,
        instrument=instrument,
        work_dir=work_dir,
    )
    ws.prepare()

    bus     = EventBus()
    backend = BpsBackend()
    store   = SqliteStateStore(ws.db_path)
    await store.init()

    patterns_file = work_dir / "error_patterns.yaml"
    classifier = ErrorClassifier(
        patterns_file=patterns_file if patterns_file.exists() else None
    )

    transports = [StdoutTransport()]
    if webhook:
        transports.append(WebhookTransport(webhook))

    interventions = InterventionActions(
        graph=graph, backend=backend, bus=bus,
        store=store, bps_dir=ws.bps_dir,
    )
    agent_handler = AgentHandler(
        bus=bus, graph=graph, classifier=classifier,
        transports=transports, interventions=interventions,
        auto_restart=auto_restart, auto_restart_threshold=0.8,
    )
    runner = WorkflowRunner(
        graph=graph, backend=backend, store=store, bus=bus,
        bps_dir=ws.bps_dir, log_dir=ws.log_dir,
        wake_strategy=TimerWakeStrategy(interval=poll_interval),
        agent_handler=agent_handler,
    )

    try:
        await runner.run()
    finally:
        await store.close()


# ---------------------------------------------------------------------------
# status
# ---------------------------------------------------------------------------

@cli.command()
@click.option("--work-dir", "-w", default=".", show_default=True)
@click.option("--workflow-id", default=None)
def status(work_dir, workflow_id):
    """Print current node states from the StateStore."""
    asyncio.run(_print_status(Path(work_dir), workflow_id))


async def _print_status(work_dir: Path, workflow_id: str | None):
    db = _db_from_workdir(work_dir)
    if not db.exists():
        click.echo(f"No state database at {db}", err=True)
        sys.exit(1)

    store = SqliteStateStore(db)
    await store.init()

    if workflow_id is None:
        rows = await store.list_workflows()
        if not rows:
            click.echo("No workflows found.")
            await store.close()
            return
        workflow_id = rows[0]["workflow_id"]

    graph = await store.load_workflow(workflow_id)
    if graph is None:
        click.echo(f"Workflow {workflow_id!r} not found.", err=True)
        await store.close()
        sys.exit(1)

    _COLORS = {
        "SUCCEEDED": "green", "FAILED": "red", "HELD": "yellow",
        "RUNNING": "cyan",    "SUBMITTED": "blue", "SKIPPED": "magenta",
    }

    click.echo(f"\nWorkflow: {graph.workflow_id}")
    click.echo(f"{'Node':<30} {'State':<12} {'Restarts':<10} Cluster")
    click.echo("-" * 68)
    for node in graph.nodes:
        color = _COLORS.get(node.state.value)
        state_s = (click.style(f"{node.state.value:<12}", fg=color)
                   if color else f"{node.state.value:<12}")
        restarts = f"{node.restart_count}/{node.max_restarts}"
        cluster  = node.submit_id or "-"
        click.echo(f"{node.node_id:<30} {state_s} {restarts:<10} {cluster}")

    click.echo()
    for state, count in sorted(graph.summary().items()):
        click.echo(f"  {state}: {count}")

    await store.close()


# ---------------------------------------------------------------------------
# intervene
# ---------------------------------------------------------------------------

@cli.group()
def intervene():
    """Trigger agent interventions on a running or stalled workflow."""


@intervene.command("restart")
@click.argument("node_id")
@click.option("--work-dir", "-w", default=".", show_default=True)
@click.option("--workflow-id", default=None)
@click.option("--reason", default="manual restart via CLI")
def intervene_restart(node_id, work_dir, workflow_id, reason):
    """Restart a held or failed node via bps restart."""
    asyncio.run(_intervene(Path(work_dir), workflow_id,
                           "restart", node_id, reason))


@intervene.command("resubmit")
@click.argument("node_id")
@click.option("--work-dir", "-w", default=".", show_default=True)
@click.option("--workflow-id", default=None)
@click.option("--reason", default="manual resubmit via CLI")
@click.option("--override", "-o", multiple=True,
              help="BPS override as key=value.")
def intervene_resubmit(node_id, work_dir, workflow_id, reason, override):
    """Submit a failed node from scratch via bps submit."""
    overrides = dict(kv.split("=", 1) for kv in override)
    asyncio.run(_intervene(Path(work_dir), workflow_id,
                           "resubmit", node_id, reason, overrides))


@intervene.command("skip")
@click.argument("node_id")
@click.option("--work-dir", "-w", default=".", show_default=True)
@click.option("--workflow-id", default=None)
@click.option("--reason", default="manual skip via CLI")
def intervene_skip(node_id, work_dir, workflow_id, reason):
    """Skip a node and unblock its dependents."""
    asyncio.run(_intervene(Path(work_dir), workflow_id,
                           "skip", node_id, reason))


@intervene.command("abort")
@click.argument("node_id")
@click.option("--work-dir", "-w", default=".", show_default=True)
@click.option("--workflow-id", default=None)
@click.option("--reason", default="manual abort via CLI")
def intervene_abort(node_id, work_dir, workflow_id, reason):
    """Abort a node's HTCondor jobs and mark it failed."""
    asyncio.run(_intervene(Path(work_dir), workflow_id,
                           "abort", node_id, reason))


async def _intervene(work_dir, workflow_id, action, node_id,
                     reason="", bps_overrides=None):
    from .agent import InterventionActions
    from .backends.bps import BpsBackend
    from .bus import EventBus

    db = _db_from_workdir(work_dir)
    store = SqliteStateStore(db)
    await store.init()

    if workflow_id is None:
        rows = await store.list_workflows()
        if not rows:
            click.echo("No workflows found.", err=True)
            sys.exit(1)
        workflow_id = rows[0]["workflow_id"]

    graph = await store.load_workflow(workflow_id)
    if graph is None:
        click.echo(f"Workflow {workflow_id!r} not found.", err=True)
        sys.exit(1)

    bus     = EventBus()
    _sink   = bus.subscribe("cli_sink")
    backend = BpsBackend()
    actions = InterventionActions(
        graph=graph, backend=backend, bus=bus,
        store=store, bps_dir=work_dir / "bps",
    )

    try:
        if action == "restart":
            await actions.restart_node(node_id, reason=reason, actor="cli")
        elif action == "resubmit":
            await actions.resubmit_node(node_id, reason=reason, actor="cli",
                                        bps_overrides=bps_overrides)
        elif action == "skip":
            await actions.skip_node(node_id, reason=reason, actor="cli")
        elif action == "abort":
            await actions.abort_node(node_id, reason=reason, actor="cli")
        click.echo(
            click.style(f"'{action}' applied to {node_id!r}.", fg="green")
        )
    except RuntimeError as exc:
        click.echo(click.style(str(exc), fg="red"), err=True)
        sys.exit(1)
    finally:
        await store.close()


# ---------------------------------------------------------------------------
# workflows
# ---------------------------------------------------------------------------

@cli.command()
@click.option("--work-dir", "-w", default=".", show_default=True)
def workflows(work_dir):
    """List all workflows in the StateStore."""
    asyncio.run(_list_workflows(Path(work_dir)))


async def _list_workflows(work_dir: Path):
    db = _db_from_workdir(work_dir)
    if not db.exists():
        click.echo(f"No state database at {db}", err=True)
        sys.exit(1)
    store = SqliteStateStore(db)
    await store.init()
    rows = await store.list_workflows()
    if not rows:
        click.echo("No workflows recorded.")
    else:
        click.echo(f"\n{'Workflow ID':<30} {'Created':<32} Updated")
        click.echo("-" * 80)
        for row in rows:
            click.echo(
                f"{row['workflow_id']:<30} "
                f"{row['created_at']:<32} {row['updated_at']}"
            )
    await store.close()


def main():
    cli()


if __name__ == "__main__":
    main()


# ---------------------------------------------------------------------------
# resume  (appended)
# ---------------------------------------------------------------------------

@cli.command()
@click.option("--work-dir", "-w", default=".", show_default=True,
              help="Working directory of the original run.")
@click.option("--workflow-id", default=None,
              help="Workflow ID to resume (defaults to most recent).")
@click.option("--poll-interval", default=60.0, show_default=True)
@click.option("--auto-restart", is_flag=True)
@click.option("--webhook", default=None, envvar="BATCHFLOW_WEBHOOK")
def resume(work_dir, workflow_id, poll_interval, auto_restart, webhook):
    """
    Re-attach to an interrupted workflow run.

    Reloads graph state from the StateStore, re-spawns monitors for
    any nodes that were SUBMITTED or RUNNING when the process died,
    and continues the DAG from where it left off.

    SUCCEEDED nodes are not re-submitted.  HELD/FAILED nodes remain in
    that state until the agent intervenes (or --auto-restart is set).
    """
    work_dir = Path(work_dir)
    db = _db_from_workdir(work_dir)
    if not db.exists():
        click.echo(click.style(f"No state database at {db}", fg="red"), err=True)
        sys.exit(1)
    asyncio.run(_resume_workflow(
        work_dir=work_dir,
        workflow_id=workflow_id,
        poll_interval=poll_interval,
        auto_restart=auto_restart,
        webhook=webhook,
    ))


async def _resume_workflow(work_dir, workflow_id, poll_interval,
                           auto_restart, webhook):
    from .agent import (AgentHandler, InterventionActions,
                        StdoutTransport, WebhookTransport)
    from .backends.bps import BpsBackend
    from .bus import EventBus
    from .classifier import ErrorClassifier
    from .monitor import TimerWakeStrategy
    from .runner import WorkflowRunner

    store = SqliteStateStore(_db_from_workdir(work_dir))
    await store.init()

    if workflow_id is None:
        rows = await store.list_workflows()
        if not rows:
            click.echo("No workflows in StateStore.", err=True)
            sys.exit(1)
        workflow_id = rows[0]["workflow_id"]

    graph = await store.load_workflow(workflow_id)
    if graph is None:
        click.echo(f"Workflow {workflow_id!r} not found.", err=True)
        sys.exit(1)

    click.echo(f"\nResuming workflow: {graph.workflow_id}")
    for state, count in sorted(graph.summary().items()):
        click.echo(f"  {state}: {count}")
    click.echo()

    bus     = EventBus()
    backend = BpsBackend()
    patterns_file = work_dir / "error_patterns.yaml"
    classifier = ErrorClassifier(
        patterns_file=patterns_file if patterns_file.exists() else None
    )
    transports = [StdoutTransport()]
    if webhook:
        transports.append(WebhookTransport(webhook))

    interventions = InterventionActions(
        graph=graph, backend=backend, bus=bus,
        store=store, bps_dir=work_dir / "bps",
    )
    agent_handler = AgentHandler(
        bus=bus, graph=graph, classifier=classifier,
        transports=transports, interventions=interventions,
        auto_restart=auto_restart, auto_restart_threshold=0.8,
    )
    runner = WorkflowRunner(
        graph=graph, backend=backend, store=store, bus=bus,
        bps_dir=work_dir / "bps", log_dir=work_dir / "logs",
        wake_strategy=TimerWakeStrategy(interval=poll_interval),
        agent_handler=agent_handler,
    )
    try:
        outcome = await runner.run()
        click.echo(f"\nWorkflow finished: {outcome.value}")
    finally:
        await store.close()
