"""Authenticated query surface for SDK-backed pipeline test output."""

from pathlib import Path
from uuid import UUID

from app.config import settings
from app.db.session import get_session
from app.middleware.auth import get_current_user
from app.models.graph import Graph
from app.services.execution_service import get_run
from fastapi import APIRouter, Depends, HTTPException, Query, status
from ingestion_graph.destinations import SQLiteCollection
from ingestion_graph.query import QueryRequest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

router = APIRouter(prefix="/api/executions", tags=["query"])


@router.get("/{run_id}/query")
async def query_run_output(
    run_id: UUID,
    q: str | None = Query(default=None, max_length=500),
    source: str | None = Query(default=None, max_length=255),
    stream: str | None = Query(default=None, max_length=255),
    limit: int = Query(default=20, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
    db: AsyncSession = Depends(get_session),
    current_user: dict = Depends(get_current_user),
) -> dict:
    """Search the materialized current view produced by Queryable Test Store."""
    run = await get_run(db, run_id)
    if not run:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Run not found")
    owner_result = await db.execute(select(Graph.owner_id).where(Graph.id == run.graph_id))
    owner_id = owner_result.scalar_one_or_none()
    if owner_id is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Graph not found")
    if current_user["role"] != "admin" and str(owner_id) != str(current_user["user_id"]):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="No permission")

    store_path = Path(settings.temp_dir) / "query" / f"{run_id}.db"
    if not store_path.is_file():
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="This run has no queryable output; add a Queryable Test Store node",
        )

    store = SQLiteCollection(store_path)
    try:
        hits = await store.query(
            QueryRequest(text=q, source=source, stream=stream, limit=limit, offset=offset)
        )
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail=str(exc),
        ) from exc
    finally:
        await store.close()
    return {
        "run_id": str(run_id),
        "query": q,
        "count": len(hits),
        "hits": [{"score": hit.score, "envelope": hit.envelope.to_dict()} for hit in hits],
    }
