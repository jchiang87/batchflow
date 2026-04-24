"""
loader.py — Load WorkflowGraph from YAML or build programmatically.

YAML format
-----------
    workflow: b_protocol

    nodes:
      - id: read_noise
        bps_yaml: bps_eoReadNoise.yaml
        max_restarts: 5             # optional, default 3

      - id: ptc
        bps_yaml: bps_cpPtc.yaml
        depends_on: [read_noise]

      - id: linearity
        bps_yaml: bps_eoLinearity.yaml
        depends_on: [ptc]
        bps_overrides:              # optional key=value pairs for bps
          requestMemory: "8G"
        metadata:                   # arbitrary user data
          description: "Linearity curves from flat pairs"
"""
from __future__ import annotations

from pathlib import Path

import yaml

from .graph import PipelineNode, WorkflowGraph


def load_workflow(path: str | Path) -> WorkflowGraph:
    """
    Parse a workflow YAML file and return a validated WorkflowGraph.

    Raises
    ------
    ValueError
        If the YAML is missing required fields or the graph has cycles.
    FileNotFoundError
        If *path* does not exist.
    """
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Workflow file not found: {path}")

    with open(path) as fh:
        data = yaml.safe_load(fh)

    if "workflow" not in data:
        raise ValueError("Workflow YAML must contain a 'workflow' key")
    if "nodes" not in data or not data["nodes"]:
        raise ValueError("Workflow YAML must contain at least one node")

    graph = WorkflowGraph(data["workflow"])

    for nd in data["nodes"]:
        if "id" not in nd:
            raise ValueError(f"Node entry missing 'id': {nd}")
        if "bps_yaml" not in nd:
            raise ValueError(f"Node {nd['id']!r} missing 'bps_yaml'")

        node = PipelineNode(
            node_id       = nd["id"],
            bps_yaml      = nd["bps_yaml"],
            depends_on    = nd.get("depends_on", []),
            bps_overrides = nd.get("bps_overrides", {}),
            max_restarts  = nd.get("max_restarts", 3),
            metadata      = nd.get("metadata", {}),
        )
        graph.add_node(node)

    graph.validate()
    return graph
