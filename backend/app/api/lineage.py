"""
Lineage API routes: query data lineage and provenance.
"""
from uuid import UUID

from app.db.session import get_session
from app.middleware.auth import get_current_user
from app.services.graph_service import get_graph
from app.services.lineage_service import (
    get_lineage_for_graph,
    get_lineage_for_run,
    get_lineage_for_source,
    get_provenance_for_run,
)
from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession


async def _check_lineage_run_access(run_id, current_user, db):
    from app.models.execution import Run
    result = await db.execute(select(Run).where(Run.id == run_id))
    run = result.scalar_one_or_none()
    if not run:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Run not found")
    graph = await get_graph(db, run.graph_id)
    if not graph:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Parent graph not found")
    if current_user["role"] != "admin" and str(graph.owner_id) != str(current_user["user_id"]):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="No permission")

router = APIRouter(prefix="/api/lineage", tags=["lineage"])


@router.get("/run/{run_id}")
async def list_lineage_for_run(
    run_id: UUID,
    db: AsyncSession = Depends(get_session),
    current_user: dict = Depends(get_current_user),
):
    """Get all lineage entries for a specific run."""
    await _check_lineage_run_access(run_id, current_user, db)
    entries = await get_lineage_for_run(db, run_id)
    return {
        "run_id": str(run_id),
        "lineage": [
            {
                "id": str(e.id),
                "source_node_id": e.source_node_id,
                "target_node_id": e.target_node_id,
                "source_port": e.source_port,
                "target_port": e.target_port,
                "items_count": e.items_count,
                "items_sample": e.items_sample,
                "bytes_transferred": e.bytes_transferred,
                "created_at": e.created_at.isoformat() if e.created_at else None,
            }
            for e in entries
        ],
        "total": len(entries),
    }


@router.get("/graph/{graph_id}")
async def list_lineage_for_graph(
    graph_id: UUID,
    limit: int = Query(100, ge=1, le=1000),
    db: AsyncSession = Depends(get_session),
    current_user: dict = Depends(get_current_user),
):
    """Get lineage entries across all runs for a graph."""
    graph = await get_graph(db, graph_id)
    if not graph:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Graph not found")
    if current_user["role"] != "admin" and str(graph.owner_id) != str(current_user["user_id"]):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="No permission")
    entries = await get_lineage_for_graph(db, graph_id, limit=limit)
    return {
        "graph_id": str(graph_id),
        "lineage": [
            {
                "id": str(e.id),
                "run_id": str(e.run_id),
                "source_node_id": e.source_node_id,
                "target_node_id": e.target_node_id,
                "source_port": e.source_port,
                "target_port": e.target_port,
                "items_count": e.items_count,
                "items_sample": e.items_sample,
                "bytes_transferred": e.bytes_transferred,
                "created_at": e.created_at.isoformat() if e.created_at else None,
            }
            for e in entries
        ],
        "total": len(entries),
    }


@router.get("/source/{source_ref:path}")
async def list_lineage_for_source(
    source_ref: str,
    db: AsyncSession = Depends(get_session),
    current_user: dict = Depends(get_current_user),
):
    """Find all runs that consumed a specific source."""
    owner_id = None if current_user["role"] == "admin" else current_user["user_id"]
    results = await get_lineage_for_source(db, source_ref, owner_id=owner_id)
    return {
        "source_ref": source_ref,
        "results": [
            {
                "provenance": {
                    "id": str(r["provenance"].id),
                    "run_id": str(r["provenance"].run_id),
                    "source_type": r["provenance"].source_type,
                    "source_ref": r["provenance"].source_ref,
                    "output_target": r["provenance"].output_target,
                    "records_affected": r["provenance"].records_affected,
                },
                "lineage": [
                    {
                        "id": str(l.id),
                        "source_node_id": l.source_node_id,
                        "target_node_id": l.target_node_id,
                        "items_count": l.items_count,
                    }
                    for l in r["lineage"]
                ],
            }
            for r in results
        ],
        "total": len(results),
    }


@router.get("/provenance/run/{run_id}")
async def list_provenance_for_run(
    run_id: UUID,
    db: AsyncSession = Depends(get_session),
    current_user: dict = Depends(get_current_user),
):
    """Get provenance records for a specific run."""
    await _check_lineage_run_access(run_id, current_user, db)
    entries = await get_provenance_for_run(db, run_id)
    return {
        "run_id": str(run_id),
        "provenance": [
            {
                "id": str(e.id),
                "source_type": e.source_type,
                "source_ref": e.source_ref,
                "output_target": e.output_target,
                "records_affected": e.records_affected,
                "metadata": e.metadata_,
                "created_at": e.created_at.isoformat() if e.created_at else None,
            }
            for e in entries
        ],
        "total": len(entries),
    }
