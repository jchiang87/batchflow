"""
eo_pipe_adapter/adapter.py

Thin adapter between the legacy eo_pipe pipeline configuration format
(``data/eo_pipelines_config.yaml``) and batchflow's WorkflowGraph.

The legacy format lists pipeline YAML filenames per run type with no
explicit dependency information — the original code submitted them
sequentially for cp_pipe or all-at-once for eo_pipe.  This adapter
preserves the flat structure as a default but allows an optional
``dependencies`` section to be added to the config file to express
the real DAG relationships.

Legacy format (eo_pipelines_config.yaml)
-----------------------------------------
    baseline:
      env_vars: [BUTLER_CONFIG, INSTRUMENT_NAME, ...]

    b_protocol:
      env_vars: []
      pipelines:
        - bps_eoReadNoise.yaml
        - bps_eoBiasStability.yaml
        - bps_cpPtc.yaml
        ...

Extended format (add a ``dependencies`` key to any run type)
-------------------------------------------------------------
    b_protocol:
      pipelines:
        - bps_eoReadNoise.yaml
        - bps_eoBiasStability.yaml
        - bps_cpPtc.yaml
      dependencies:
        bps_cpPtc.yaml:
          - bps_eoReadNoise.yaml
        bps_eoDarkDefects.yaml:
          - bps_eoReadNoise.yaml
          - bps_eoBiasStability.yaml

If no ``dependencies`` key is present the run type is treated as fully
parallel (all pipelines submitted simultaneously, no ordering imposed)
— matching the original EoPipelines behaviour.

Usage
-----
    from eo_pipe_adapter import load_eo_workflow

    graph = load_eo_workflow(
        config_path="data/eo_pipelines_config.yaml",
        run_type="b_protocol",
    )
    # graph is a validated batchflow.WorkflowGraph ready to hand to
    # WorkflowRunner.
"""
from __future__ import annotations

import os
import re
from pathlib import Path

import yaml

from batchflow import PipelineNode, WorkflowGraph


# ---------------------------------------------------------------------------
# LSST-specific error patterns to register with ErrorClassifier
# ---------------------------------------------------------------------------

LSST_TRANSIENT_PATTERNS = [
    r"Butler datastore.*timeout",
    r"Registry.*connection refused",
    r"S3.*connection reset",
    r"S3.*read timeout",
    r"posix.*Stale file handle",
    r"HTCondor shadow.*lost connection",
]

LSST_FATAL_PATTERNS = [
    r"DatasetNotFoundError",
    r"No datasets.*found for",
    r"IncompatibleDatasetTypeError",
    r"Butler.*unknown collection",
    r"lsst\.obs\.lsst.*not found",
    r"camera.*not recognised",
]

# Required environment variables for LSST runs.
REQUIRED_ENV_VARS = [
    "BUTLER_CONFIG",
    "INSTRUMENT_NAME",
    "EO_PIPE_DIR",
    "EO_PIPE_INCOLLECTION",
    "DATASET_LABEL",
]


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def load_eo_workflow(
    config_path: str | Path,
    run_type: str,
    workflow_id: str | None = None,
    max_restarts: int = 3,
) -> WorkflowGraph:
    """
    Load an eo_pipe run-type config and return a batchflow WorkflowGraph.

    Parameters
    ----------
    config_path : str | Path
        Path to ``eo_pipelines_config.yaml`` (or equivalent).
    run_type : str
        Run type key in the config, e.g. ``"b_protocol"`` or ``"ptc"``.
    workflow_id : str | None
        Override the workflow ID.  Defaults to ``run_type``.
    max_restarts : int
        Default max_restarts for all nodes.

    Returns
    -------
    WorkflowGraph
        Validated graph; immediately ready to pass to WorkflowRunner.

    Raises
    ------
    KeyError
        If *run_type* is not in the config.
    ValueError
        If the resulting graph has a cycle or unknown dependency references.
    """
    config_path = Path(config_path)
    with open(config_path) as fh:
        config = yaml.safe_load(fh)

    if run_type not in config:
        available = [k for k in config if k != "baseline"]
        raise KeyError(
            f"Run type {run_type!r} not in config.  "
            f"Available: {available}"
        )

    run_cfg = config[run_type]
    pipelines: list[str] = run_cfg.get("pipelines", [])
    if not pipelines:
        raise ValueError(f"Run type {run_type!r} has no pipelines defined.")

    dependencies: dict[str, list[str]] = run_cfg.get("dependencies", {})

    wf_id = workflow_id or run_type
    graph = WorkflowGraph(wf_id)

    for bps_yaml in pipelines:
        node_id = _yaml_to_node_id(bps_yaml)
        raw_deps = dependencies.get(bps_yaml, [])
        dep_ids  = [_yaml_to_node_id(d) for d in raw_deps]
        graph.add_node(PipelineNode(
            node_id      = node_id,
            bps_yaml     = bps_yaml,
            depends_on   = dep_ids,
            max_restarts = max_restarts,
            metadata     = {"run_type": run_type, "source_config": str(config_path)},
        ))

    graph.validate()
    return graph


def check_env_vars(
    config_path: str | Path,
    run_type: str,
) -> list[str]:
    """
    Return a list of missing environment variables for this run type.
    Empty list means all required variables are set.
    """
    config_path = Path(config_path)
    with open(config_path) as fh:
        config = yaml.safe_load(fh)

    baseline_vars: list[str] = config.get("baseline", {}).get("env_vars", [])
    run_vars:      list[str] = config.get(run_type, {}).get("env_vars", [])
    required = list(dict.fromkeys(baseline_vars + run_vars))  # deduplicate, preserve order
    return [v for v in required if not os.environ.get(v)]


def register_lsst_patterns(classifier) -> None:
    """
    Add LSST-specific HoldReason patterns to an ErrorClassifier instance.

    Call this after constructing the classifier:

        clf = ErrorClassifier()
        register_lsst_patterns(clf)
    """
    for pat in LSST_TRANSIENT_PATTERNS:
        classifier.add_transient(pat)
    for pat in LSST_FATAL_PATTERNS:
        classifier.add_fatal(pat)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _yaml_to_node_id(bps_yaml: str) -> str:
    """
    Convert a BPS YAML filename to a valid node_id.

    ``bps_eoReadNoise.yaml``  →  ``eo_read_noise``
    ``bps_cpPtc.yaml``        →  ``cp_ptc``
    """
    name = Path(bps_yaml).stem          # strip .yaml
    name = re.sub(r"^bps_", "", name)   # strip leading bps_
    # Insert underscores before upper-case runs (CamelCase → snake_case).
    name = re.sub(r"([a-z])([A-Z])", r"\1_\2", name)
    name = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", name)
    return name.lower()
