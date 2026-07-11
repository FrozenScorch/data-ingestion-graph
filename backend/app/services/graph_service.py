"""
Graph CRUD service and version management.
"""

import hashlib
import json
from typing import Optional
from uuid import UUID

from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.models.graph import Graph, GraphVersion, GraphStatus
from app.schemas.graph import _is_secret_key


def _assert_no_inline_secrets(value: object, path: str = "node_configs") -> None:
    """Reject credentials in graph JSON; secrets belong in encrypted Connections."""
    if isinstance(value, dict):
        for key, item in value.items():
            key_text = str(key)
            if _is_secret_key(key_text):
                raise ValueError(
                    f"Inline secret at {path}.{key_text} is not allowed; use connection_id"
                )
            _assert_no_inline_secrets(item, f"{path}.{key_text}")
    elif isinstance(value, list):
        for index, item in enumerate(value):
            _assert_no_inline_secrets(item, f"{path}[{index}]")


async def list_graphs(
    db: AsyncSession,
    owner_id: Optional[UUID] = None,
    status: Optional[str] = None,
    offset: int = 0,
    limit: int = 50,
) -> tuple[list[Graph], int]:
    """List graphs with optional filtering. Returns (graphs, total_count)."""
    query = select(Graph)
    count_query = select(func.count()).select_from(Graph)

    if owner_id:
        query = query.where(Graph.owner_id == owner_id)
        count_query = count_query.where(Graph.owner_id == owner_id)
    if status:
        query = query.where(Graph.status == status)
        count_query = count_query.where(Graph.status == status)

    query = query.order_by(Graph.updated_at.desc()).offset(offset).limit(limit)

    total_result = await db.execute(count_query)
    total = total_result.scalar() or 0

    result = await db.execute(query)
    graphs = list(result.scalars().all())
    return graphs, total


async def get_graph(db: AsyncSession, graph_id: UUID) -> Optional[Graph]:
    """Get a single graph by ID, with its latest version eagerly loaded."""
    result = await db.execute(
        select(Graph).where(Graph.id == graph_id).options(selectinload(Graph.versions))
    )
    graph = result.scalar_one_or_none()
    if graph is not None:
        # Attach the latest version for serialization
        graph._latest_version = graph.versions[0] if graph.versions else None
    return graph


async def create_graph(
    db: AsyncSession,
    name: str,
    owner_id: UUID,
    description: Optional[str] = None,
    tags: Optional[list[str]] = None,
) -> Graph:
    """Create a new graph."""
    graph = Graph(
        name=name,
        description=description,
        owner_id=owner_id,
        status=GraphStatus.DRAFT.value,
        tags=tags or [],
    )
    db.add(graph)
    await db.commit()
    await db.refresh(graph)
    return graph


async def update_graph(
    db: AsyncSession,
    graph_id: UUID,
    name: Optional[str] = None,
    description: Optional[str] = None,
    status: Optional[str] = None,
    tags: Optional[list[str]] = None,
) -> Optional[Graph]:
    """Update a graph's metadata."""
    graph = await get_graph(db, graph_id)
    if not graph:
        return None

    if name is not None:
        graph.name = name
    if description is not None:
        graph.description = description
    if status is not None:
        graph.status = status
    if tags is not None:
        graph.tags = tags

    await db.commit()
    await db.refresh(graph)
    return graph


async def archive_graph(db: AsyncSession, graph_id: UUID) -> Optional[Graph]:
    """Archive a graph (soft delete)."""
    return await update_graph(db, graph_id, status=GraphStatus.ARCHIVED.value)


def compute_checksum(data: dict) -> str:
    """Compute SHA-256 checksum of graph data."""
    serialized = json.dumps(data, sort_keys=True, default=str)
    return hashlib.sha256(serialized.encode()).hexdigest()


async def save_graph_version(
    db: AsyncSession,
    graph_id: UUID,
    nodes_data: Optional[dict] = None,
    edges_data: Optional[dict] = None,
    node_configs: Optional[dict] = None,
) -> Optional[GraphVersion]:
    """Save a new version of a graph.

    Uses SELECT ... FOR UPDATE to prevent concurrent version number collisions.
    Locks the graph row so that only one save_version call can increment the
    version at a time for a given graph.
    """
    from sqlalchemy import select as sa_select

    from app.graph_validation import (
        sanitize_node_configs,
        strip_embedded_node_configs,
        validate_graph_edges,
    )

    node_configs = sanitize_node_configs(nodes_data, node_configs)
    nodes_data = strip_embedded_node_configs(nodes_data)
    _assert_no_inline_secrets(node_configs)
    _assert_no_inline_secrets(nodes_data, "nodes_data")
    validate_graph_edges(nodes_data, edges_data)

    # Lock the graph row with FOR UPDATE to serialize concurrent version saves
    lock_result = await db.execute(sa_select(Graph).where(Graph.id == graph_id).with_for_update())
    graph = lock_result.scalar_one_or_none()
    if not graph:
        return None

    # Get the next version number (safe within the locked transaction)
    version_result = await db.execute(
        select(func.coalesce(func.max(GraphVersion.version_number), 0)).where(
            GraphVersion.graph_id == graph_id
        )
    )
    next_version = (version_result.scalar() or 0) + 1

    combined_data = {"nodes": nodes_data, "edges": edges_data, "configs": node_configs}
    checksum = compute_checksum(combined_data)

    version = GraphVersion(
        graph_id=graph_id,
        version_number=next_version,
        nodes_data=nodes_data,
        edges_data=edges_data,
        node_configs=node_configs,
        checksum=checksum,
    )
    db.add(version)
    await db.commit()
    await db.refresh(version)
    return version


async def get_graph_versions(
    db: AsyncSession,
    graph_id: UUID,
    limit: int = 20,
) -> list[GraphVersion]:
    """Get version history for a graph."""
    result = await db.execute(
        select(GraphVersion)
        .where(GraphVersion.graph_id == graph_id)
        .order_by(GraphVersion.version_number.desc())
        .limit(limit)
    )
    return list(result.scalars().all())
