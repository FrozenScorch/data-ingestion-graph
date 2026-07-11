"""
Graph API routes: CRUD and version management.
"""

from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.session import get_session
from app.middleware.auth import get_current_user
from app.schemas.graph import (
    GraphCreate,
    GraphUpdate,
    GraphResponse,
    GraphDetailResponse,
    GraphListResponse,
    GraphVersionSave,
    GraphVersionResponse,
    ConnectionCreate,
    ConnectionUpdate,
    ConnectionResponse,
)
from app.services.graph_service import (
    list_graphs,
    get_graph,
    create_graph,
    update_graph,
    archive_graph,
    save_graph_version,
    get_graph_versions,
)

router = APIRouter(prefix="/api/graphs", tags=["graphs"])


@router.get("", response_model=GraphListResponse)
async def list_graphs_endpoint(
    offset: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=100),
    status_filter: str | None = Query(None, alias="status"),
    db: AsyncSession = Depends(get_session),
    current_user: dict = Depends(get_current_user),
):
    """List all graphs accessible to the current user."""
    graphs, total = await list_graphs(
        db,
        owner_id=current_user["user_id"],
        status=status_filter,
        offset=offset,
        limit=limit,
    )
    return GraphListResponse(graphs=graphs, total=total)


@router.post("", response_model=GraphResponse, status_code=status.HTTP_201_CREATED)
async def create_graph_endpoint(
    request: GraphCreate,
    db: AsyncSession = Depends(get_session),
    current_user: dict = Depends(get_current_user),
):
    """Create a new graph."""
    if request.template_id:
        from app.graph_templates import create_graph_from_template

        try:
            graph = await create_graph_from_template(
                db,
                template_id=request.template_id,
                name=request.name,
                owner_id=current_user["user_id"],
                description=request.description,
                tags=request.tags,
            )
        except KeyError as exc:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Graph template not found",
            ) from exc
    else:
        graph = await create_graph(
            db,
            name=request.name,
            owner_id=current_user["user_id"],
            description=request.description,
            tags=request.tags,
        )
    return graph


@router.get("/{graph_id}", response_model=GraphDetailResponse)
async def get_graph_endpoint(
    graph_id: UUID,
    db: AsyncSession = Depends(get_session),
    current_user: dict = Depends(get_current_user),
):
    """Get a graph by ID with its latest version."""
    graph = await get_graph(db, graph_id)
    if not graph:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Graph not found")
    _check_graph_owner(graph, current_user)
    return graph


@router.put("/{graph_id}", response_model=GraphResponse)
async def update_graph_endpoint(
    graph_id: UUID,
    request: GraphUpdate,
    db: AsyncSession = Depends(get_session),
    current_user: dict = Depends(get_current_user),
):
    """Update a graph's metadata."""
    existing_graph = await get_graph(db, graph_id)
    if not existing_graph:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Graph not found")
    _check_graph_owner(existing_graph, current_user)

    graph = await update_graph(
        db,
        graph_id,
        name=request.name,
        description=request.description,
        status=request.status,
        tags=request.tags,
    )
    if not graph:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Graph not found")
    return graph


@router.delete("/{graph_id}", status_code=status.HTTP_204_NO_CONTENT)
async def archive_graph_endpoint(
    graph_id: UUID,
    db: AsyncSession = Depends(get_session),
    current_user: dict = Depends(get_current_user),
):
    """Archive a graph (soft delete)."""
    existing_graph = await get_graph(db, graph_id)
    if not existing_graph:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Graph not found")
    _check_graph_owner(existing_graph, current_user)

    graph = await archive_graph(db, graph_id)
    if not graph:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Graph not found")


@router.post("/{graph_id}/save", response_model=GraphVersionResponse)
async def save_graph_version_endpoint(
    graph_id: UUID,
    request: GraphVersionSave,
    db: AsyncSession = Depends(get_session),
    current_user: dict = Depends(get_current_user),
):
    """Save a new version of a graph."""
    existing_graph = await get_graph(db, graph_id)
    if not existing_graph:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Graph not found")
    _check_graph_owner(existing_graph, current_user)

    try:
        version = await save_graph_version(
            db,
            graph_id,
            nodes_data=request.nodes_data,
            edges_data=request.edges_data,
            node_configs=request.node_configs,
        )
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(exc),
        ) from exc
    if not version:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Graph not found")
    return version


@router.get("/{graph_id}/versions", response_model=list[GraphVersionResponse])
async def get_graph_versions_endpoint(
    graph_id: UUID,
    limit: int = Query(20, ge=1, le=100),
    db: AsyncSession = Depends(get_session),
    current_user: dict = Depends(get_current_user),
):
    """Get version history for a graph."""
    existing_graph = await get_graph(db, graph_id)
    if not existing_graph:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Graph not found")
    _check_graph_owner(existing_graph, current_user)

    versions = await get_graph_versions(db, graph_id, limit=limit)
    return versions


def _check_graph_owner(graph, current_user: dict) -> None:
    if current_user["role"] != "admin" and str(graph.owner_id) != str(current_user["user_id"]):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="You do not have permission to access this graph",
        )
