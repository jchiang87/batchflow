"""
eo_pipe_adapter/cli.py — drop-in CLI replacement for EoPipelines/CpPipelines.

Usage
-----
    # equivalent to: EoPipelines(config).submit("b_protocol")
    eo-pipe submit b_protocol --config data/eo_pipelines_config.yaml

    # check status of a running/completed run
    eo-pipe status --work-dir ./run_001

    # resume after a crash
    eo-pipe resume --work-dir ./run_001

    # agent intervention
    eo-pipe intervene restart cp_ptc --work-dir ./run_001
"""
from __future__ import annotations

import asyncio
import sys
from pathlib import Path

import click

from batchflow import (
    EventBus, SqliteStateStore,
    ErrorClassifier,
)
from batchflow.agent import (
    AgentHandler, InterventionActions,
    StdoutTransport, WebhookTransport,
)
from batchflow.backends.bps import BpsBackend
from batchflow.monitor import TimerWakeStrategy
from batchflow.runner import WorkflowRunner
from batchflow.workspace import WorkspaceManager

from .adapter import (
    load_eo_workflow, check_env_vars, register_lsst_patterns
)


@click.group()
@click.version_option()
def cli():
    """eo-pipe — LSST EO pipeline runner (powered by batchflow)."""


# ---------------------------------------------------------------------------
# submit
# ---------------------------------------------------------------------------

@cli.command()
@click.argument("run_type")
@click.option("--config", "-c",
              default="data/eo_pipelines_config.yaml",
              show_default=True,
              help="Path to eo_pipelines_config.yaml.")
@click.option("--work-dir", "-w", default=".", show_default=True)
@click.option("--poll-interval", default=60.0, show_default=True)
@click.option("--dry-run", is_flag=True)
@click.option("--no-confirm", is_flag=True)
@click.option("--auto-restart", is_flag=True,
              help="Automatically restart transient failures.")
@click.option("--webhook", default=None, envvar="BATCHFLOW_WEBHOOK")
@click.option("--max-restarts", default=3, show_default=True)
def submit(run_type, config, work_dir, poll_interval,
           dry_run, no_confirm, auto_restart, webhook, max_restarts):
    """
    Submit all pipelines for RUN_TYPE (e.g. b_protocol, ptc).

    Equivalent to: EoPipelines(config).submit(run_type)
    """
    missing = check_env_vars(config, run_type)
    if missing:
        click.echo(
            click.style(f"Missing environment variables: {missing}", fg="red"),
            err=True,
        )
        sys.exit(1)

    graph = load_eo_workflow(config, run_type, max_restarts=max_restarts)
    work_dir = Path(work_dir)

    click.echo(f"\nRun type:  {run_type}")
    click.echo(f"Work dir:  {work_dir}")
    click.echo(f"\nPipelines ({len(graph.nodes)}):")
    for node in graph.nodes:
        deps = f"  <- {node.depends_on}" if node.depends_on else "  (no deps)"
        click.echo(f"  {node.node_id:<35} {node.bps_yaml}{deps}")
    click.echo()

    if dry_run:
        click.echo(click.style("Dry run — not submitting.", fg="yellow"))
        return

    if not no_confirm and not click.confirm("Proceed?", default=True):
        click.echo("Aborted.")
        return

    asyncio.run(_run(
        graph=graph,
        work_dir=work_dir,
        poll_interval=poll_interval,
        auto_restart=auto_restart,
        webhook=webhook,
    ))


async def _run(graph, work_dir, poll_interval, auto_restart, webhook,
               from_resume=False):
    import os
    bps_source = Path(os.environ["EO_PIPE_DIR"]) / "bps"
    instrument = os.environ.get("INSTRUMENT_NAME", "LSSTCam")

    if not from_resume:
        ws = WorkspaceManager(
            source_bps_dir=bps_source,
            instrument=instrument,
            work_dir=work_dir,
        )
        ws.prepare()
        bps_dir = ws.bps_dir
        log_dir = ws.log_dir
        db_path = ws.db_path
    else:
        bps_dir = work_dir / "bps"
        log_dir = work_dir / "logs"
        db_path = work_dir / "batchflow.db"

    bus     = EventBus()
    backend = BpsBackend()
    store   = SqliteStateStore(db_path)
    await store.init()

    clf = ErrorClassifier(
        patterns_file=(work_dir / "error_patterns.yaml")
        if (work_dir / "error_patterns.yaml").exists() else None
    )
    register_lsst_patterns(clf)

    transports = [StdoutTransport()]
    if webhook:
        transports.append(WebhookTransport(webhook))

    interventions = InterventionActions(
        graph=graph, backend=backend, bus=bus,
        store=store, bps_dir=bps_dir,
    )
    agent_handler = AgentHandler(
        bus=bus, graph=graph, classifier=clf,
        transports=transports, interventions=interventions,
        auto_restart=auto_restart, auto_restart_threshold=0.8,
    )
    runner = WorkflowRunner(
        graph=graph, backend=backend, store=store, bus=bus,
        bps_dir=bps_dir, log_dir=log_dir,
        wake_strategy=TimerWakeStrategy(interval=poll_interval),
        agent_handler=agent_handler,
    )
    try:
        outcome = await runner.run()
        click.echo(f"\nRun finished: {outcome.value}")
    finally:
        await store.close()


# ---------------------------------------------------------------------------
# resume
# ---------------------------------------------------------------------------

@cli.command()
@click.option("--work-dir", "-w", default=".", show_default=True)
@click.option("--config", "-c",
              default="data/eo_pipelines_config.yaml",
              show_default=True)
@click.option("--run-type", default=None,
              help="Required when the StateStore has multiple workflows.")
@click.option("--poll-interval", default=60.0, show_default=True)
@click.option("--auto-restart", is_flag=True)
@click.option("--webhook", default=None, envvar="BATCHFLOW_WEBHOOK")
def resume(work_dir, config, run_type, poll_interval, auto_restart, webhook):
    """Re-attach to an interrupted run."""
    work_dir = Path(work_dir)
    db = work_dir / "batchflow.db"
    if not db.exists():
        click.echo(click.style(f"No state database at {db}", fg="red"), err=True)
        sys.exit(1)

    asyncio.run(_resume(work_dir, config, run_type, poll_interval,
                        auto_restart, webhook))


async def _resume(work_dir, config, run_type, poll_interval,
                  auto_restart, webhook):
    store = SqliteStateStore(work_dir / "batchflow.db")
    await store.init()

    rows = await store.list_workflows()
    if not rows:
        click.echo("No workflows in StateStore.", err=True)
        sys.exit(1)

    wf_id = run_type or rows[0]["workflow_id"]
    graph = await store.load_workflow(wf_id)
    await store.close()

    if graph is None:
        click.echo(f"Workflow {wf_id!r} not found.", err=True)
        sys.exit(1)

    click.echo(f"\nResuming: {graph.workflow_id}")
    for state, count in sorted(graph.summary().items()):
        click.echo(f"  {state}: {count}")
    click.echo()

    await _run(
        graph=graph, work_dir=work_dir,
        poll_interval=poll_interval,
        auto_restart=auto_restart,
        webhook=webhook,
        from_resume=True,
    )


# ---------------------------------------------------------------------------
# status / intervene  (delegates to batchflow CLI helpers)
# ---------------------------------------------------------------------------

@cli.command()
@click.option("--work-dir", "-w", default=".", show_default=True)
def status(work_dir):
    """Print current pipeline states."""
    from batchflow.cli import _print_status
    asyncio.run(_print_status(Path(work_dir), workflow_id=None))


@cli.group()
def intervene():
    """Restart, skip, or abort a pipeline."""


@intervene.command("restart")
@click.argument("node_id")
@click.option("--work-dir", "-w", default=".", show_default=True)
@click.option("--reason", default="manual restart")
@click.option("--override", "-o", multiple=True)
def intervene_restart(node_id, work_dir, reason, override):
    """Restart a held or failed pipeline node."""
    from batchflow.cli import _intervene
    overrides = dict(kv.split("=", 1) for kv in override)
    asyncio.run(_intervene(Path(work_dir), None, "restart",
                           node_id, reason, overrides))


@intervene.command("skip")
@click.argument("node_id")
@click.option("--work-dir", "-w", default=".", show_default=True)
@click.option("--reason", default="manual skip")
def intervene_skip(node_id, work_dir, reason):
    """Skip a pipeline node and unblock its dependents."""
    from batchflow.cli import _intervene
    asyncio.run(_intervene(Path(work_dir), None, "skip", node_id, reason))


@intervene.command("abort")
@click.argument("node_id")
@click.option("--work-dir", "-w", default=".", show_default=True)
@click.option("--reason", default="manual abort")
def intervene_abort(node_id, work_dir, reason):
    """Abort a pipeline node."""
    from batchflow.cli import _intervene
    asyncio.run(_intervene(Path(work_dir), None, "abort", node_id, reason))


def main():
    cli()


if __name__ == "__main__":
    main()
