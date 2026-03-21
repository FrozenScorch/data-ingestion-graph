"""
DAG scheduler: Kahn's algorithm topological sort and parallel grouping.
"""
from collections import defaultdict, deque
from typing import Any


def topological_sort(nodes: dict[str, dict], edges: list[dict]) -> list[list[str]]:
    """
    Perform topological sort using Kahn's algorithm and group nodes by depth level.

    Args:
        nodes: Dict mapping node_id -> node definition (must contain 'id')
        edges: List of edge dicts with 'source' and 'target' node IDs

    Returns:
        List of lists, where each inner list contains node IDs that can run in parallel.
        Nodes in earlier lists must complete before later lists can start.

    Raises:
        ValueError: If the graph contains a cycle.
    """
    # Build adjacency list and in-degree count
    in_degree: dict[str, int] = {node_id: 0 for node_id in nodes}
    adjacency: dict[str, list[str]] = defaultdict(list)

    for edge in edges:
        source = edge.get("source", edge.get("source_id", edge.get("from")))
        target = edge.get("target", edge.get("target_id", edge.get("to")))
        if source in nodes and target in nodes:
            adjacency[source].append(target)
            in_degree[target] = in_degree.get(target, 0) + 1

    # Start with all nodes that have no incoming edges
    queue = deque([node_id for node_id, degree in in_degree.items() if degree == 0])
    levels: list[list[str]] = []

    visited_count = 0

    while queue:
        # All nodes at current depth can run in parallel
        current_level = list(queue)
        levels.append(current_level)
        visited_count += len(current_level)

        # Process edges from current level nodes
        next_queue: deque[str] = deque()
        for node_id in current_level:
            for neighbor in adjacency[node_id]:
                in_degree[neighbor] -= 1
                if in_degree[neighbor] == 0:
                    next_queue.append(neighbor)

        queue = next_queue

    if visited_count != len(nodes):
        raise ValueError("Graph contains a cycle and cannot be executed")

    return levels


def validate_dag(nodes: dict[str, dict], edges: list[dict]) -> list[str]:
    """
    Validate that a graph is a valid DAG. Returns list of error messages (empty if valid).

    Checks:
    1. No cycles
    2. All edge references point to valid nodes
    """
    errors: list[str] = []

    # Check edge references
    node_ids = set(nodes.keys())
    for i, edge in enumerate(edges):
        source = edge.get("source", edge.get("source_id", edge.get("from")))
        target = edge.get("target", edge.get("target_id", edge.get("to")))
        if source not in node_ids:
            errors.append(f"Edge {i}: source node '{source}' not found")
        if target not in node_ids:
            errors.append(f"Edge {i}: target node '{target}' not found")

    # Check for cycles
    try:
        topological_sort(nodes, edges)
    except ValueError as e:
        errors.append(str(e))

    return errors
