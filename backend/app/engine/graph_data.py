"""Canonical conversion from stored graph-version JSON to executor inputs."""

from typing import Any


def unpack_version_data(
    nodes_data: dict | None,
    edges_data: dict | list | None,
) -> tuple[dict[str, dict[str, Any]], list[dict[str, Any]]]:
    if isinstance(nodes_data, dict) and "nodes" in nodes_data:
        nodes_list = nodes_data["nodes"]
        if isinstance(nodes_list, list):
            nodes_data = {
                str(node.get("id", node.get("node_id"))): node
                for node in nodes_list
                if isinstance(node, dict)
            }

    if isinstance(edges_data, dict) and "edges" in edges_data:
        edges_data = edges_data["edges"]

    normalized_edges: list[dict[str, Any]] = []
    if isinstance(edges_data, list):
        normalized_edges = [
            {
                **edge,
                "source_port": edge.get("source_port") or edge.get("sourceHandle") or "output",
                "target_port": edge.get("target_port") or edge.get("targetHandle") or "input",
            }
            for edge in edges_data
            if isinstance(edge, dict)
        ]

    normalized_nodes = nodes_data if isinstance(nodes_data, dict) else {}
    return normalized_nodes, normalized_edges
