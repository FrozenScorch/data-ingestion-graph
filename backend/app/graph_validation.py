"""Validation for Studio graph edges against the live node registry."""

from copy import deepcopy
from typing import Any

from app.nodes.registry import get_node


def sanitize_node_configs(
    nodes_data: dict[str, Any] | None,
    node_configs: dict[str, Any] | None,
) -> dict[str, dict[str, Any]]:
    """Drop fields removed from current node contracts when saving a new version."""
    node_types = {
        item.get("id"): item.get("type") for item in (nodes_data or {}).get("nodes", [])
    }
    sanitized: dict[str, dict[str, Any]] = {}
    for node_id, raw_config in (node_configs or {}).items():
        definition = get_node(node_types.get(node_id))
        if definition is None or not isinstance(raw_config, dict):
            sanitized[node_id] = {}
            continue
        allowed = set(definition.config_schema.get("properties", {}))
        sanitized[node_id] = {
            key: value for key, value in raw_config.items() if key in allowed
        }
    return sanitized


def strip_embedded_node_configs(nodes_data: dict[str, Any] | None) -> dict[str, Any] | None:
    """Normalize legacy nodes that duplicated config inside visual node data."""
    if nodes_data is None:
        return None
    normalized = deepcopy(nodes_data)
    for item in normalized.get("nodes", []):
        data = item.get("data")
        if isinstance(data, dict):
            data["config"] = {}
    return normalized


def validate_graph_edges(
    nodes_data: dict[str, Any] | None,
    edges_data: dict[str, Any] | None,
) -> None:
    """Reject edges whose endpoints or ports violate registered node contracts.

    Draft graphs may remain incomplete, so this validates only edges that exist;
    required inputs are enforced when a pipeline is executed.
    """
    raw_nodes = (nodes_data or {}).get("nodes", [])
    raw_edges = (edges_data or {}).get("edges", [])
    definitions: dict[str, Any] = {}

    for item in raw_nodes:
        node_id = item.get("id")
        node_type = item.get("type")
        definition = get_node(node_type) if isinstance(node_type, str) else None
        if not isinstance(node_id, str) or definition is None:
            raise ValueError(f"Unknown node type {node_type!r} for node {node_id!r}")
        definitions[node_id] = definition

    for edge in raw_edges:
        edge_id = edge.get("id", "<unknown>")
        source_id = edge.get("source")
        target_id = edge.get("target")
        if source_id not in definitions or target_id not in definitions:
            raise ValueError(f"Edge {edge_id!r} references an unknown node")

        source_name = edge.get("source_port") or edge.get("sourceHandle") or "output"
        target_name = edge.get("target_port") or edge.get("targetHandle") or "input"
        source_port = next(
            (port for port in definitions[source_id].outputs if port.name == source_name),
            None,
        )
        target_port = next(
            (port for port in definitions[target_id].inputs if port.name == target_name),
            None,
        )
        if source_port is None or target_port is None:
            raise ValueError(f"Edge {edge_id!r} references an unknown port")
        if target_port.data_type.value != "any" and source_port.data_type != target_port.data_type:
            raise ValueError(
                f"Edge {edge_id!r} cannot connect {source_port.data_type.value} "
                f"to {target_port.data_type.value}"
            )
