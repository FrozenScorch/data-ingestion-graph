"""
Execution API routes: run creation, management, and retrieval.
"""

from uuid import UUID

from app.db.session import get_session
from app.engine.graph_data import unpack_version_data
from app.middleware.auth import get_current_user
from app.models.execution import RunJobType
from app.schemas.execution import (
    RunCreate,
    RunDetailResponse,
    RunListResponse,
    RunNodeResponse,
    RunResponse,
)
from app.services.execution_service import (
    cancel_run,
    create_run,
    get_run,
    list_runs,
    pause_run,
    resume_run,
)
from app.services.graph_service import get_graph, get_graph_versions
from app.services.run_queue_service import enqueue_run_job
from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

router = APIRouter(tags=["executions"])


async def _check_run_access(run, current_user: dict, db: AsyncSession) -> None:
    """Verify the current user has access to the run's graph."""
    if current_user["role"] == "admin":
        return
    from app.models.graph import Graph

    result = await db.execute(select(Graph.owner_id).where(Graph.id == run.graph_id))
    owner_id = result.scalar_one_or_none()
    if owner_id is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Associated graph not found"
        )
    if str(owner_id) != str(current_user["user_id"]):
        # Avoid confirming another tenant's run exists.
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Run not found")


_unpack_version_data = unpack_version_data


@router.post(
    "/api/graphs/{graph_id}/run", response_model=RunResponse, status_code=status.HTTP_201_CREATED
)
async def start_run(
    graph_id: UUID,
    request: RunCreate | None = None,
    db: AsyncSession = Depends(get_session),
    current_user: dict = Depends(get_current_user),
):
    """Start a new execution run for a graph."""
    # Verify graph exists and user has access
    graph = await get_graph(db, graph_id)
    if not graph:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Graph not found")
    if current_user["role"] != "admin" and str(graph.owner_id) != str(current_user["user_id"]):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="No permission")

    # Get latest version if not specified
    graph_version_id = request.graph_version_id if request else None
    if graph_version_id:
        from app.models.graph import GraphVersion

        version_result = await db.execute(
            select(GraphVersion.id).where(
                GraphVersion.id == graph_version_id,
                GraphVersion.graph_id == graph_id,
            )
        )
        if version_result.scalar_one_or_none() is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Graph version not found",
            )
    else:
        versions = await get_graph_versions(db, graph_id, limit=1)
        if versions:
            graph_version_id = versions[0].id
    if graph_version_id is None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Graph has no saved version to execute",
        )

    trigger_type = request.trigger_type if request else "manual"

    run = await create_run(
        db,
        graph_id=graph_id,
        triggered_by=current_user["user_id"],
        trigger_type=trigger_type,
        graph_version_id=graph_version_id,
        enqueue_job_type=RunJobType.FULL.value,
    )
    return run


@router.get("/api/executions", response_model=RunListResponse)
async def list_executions(
    graph_id: UUID | None = Query(None),
    status_filter: str | None = Query(None, alias="status"),
    offset: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=100),
    db: AsyncSession = Depends(get_session),
    current_user: dict = Depends(get_current_user),
):
    """List all execution runs."""
    runs, total = await list_runs(
        db,
        graph_id=graph_id,
        status=status_filter,
        offset=offset,
        limit=limit,
        owner_id=None if current_user["role"] == "admin" else current_user["user_id"],
    )
    return RunListResponse(runs=runs, total=total)


@router.get("/api/executions/{run_id}", response_model=RunDetailResponse)
async def get_run_detail(
    run_id: UUID,
    db: AsyncSession = Depends(get_session),
    current_user: dict = Depends(get_current_user),
):
    """Get detailed run information including node statuses."""
    run = await get_run(db, run_id, load_nodes=True)
    if not run:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Run not found")
    await _check_run_access(run, current_user, db)

    run_nodes = [RunNodeResponse.model_validate(node) for node in run.run_nodes]
    return RunDetailResponse(
        run=RunResponse.model_validate(run),
        run_nodes=run_nodes,
    )


@router.post("/api/executions/{run_id}/cancel", response_model=RunResponse)
async def cancel_run_endpoint(
    run_id: UUID,
    db: AsyncSession = Depends(get_session),
    current_user: dict = Depends(get_current_user),
):
    """Cancel a running execution."""
    run = await get_run(db, run_id)
    if not run:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Run not found")
    await _check_run_access(run, current_user, db)
    run = await cancel_run(db, run_id)
    return run


@router.post("/api/executions/{run_id}/pause", response_model=RunResponse)
async def pause_run_endpoint(
    run_id: UUID,
    db: AsyncSession = Depends(get_session),
    current_user: dict = Depends(get_current_user),
):
    """Pause a running execution."""
    run = await get_run(db, run_id)
    if not run:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Run not found")
    await _check_run_access(run, current_user, db)
    run = await pause_run(db, run_id)
    return run


@router.post("/api/executions/{run_id}/resume", response_model=RunResponse)
async def resume_run_endpoint(
    run_id: UUID,
    db: AsyncSession = Depends(get_session),
    current_user: dict = Depends(get_current_user),
):
    """Resume a paused execution."""
    run = await get_run(db, run_id)
    if not run:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Run not found")
    await _check_run_access(run, current_user, db)
    run = await resume_run(db, run_id)
    return run


@router.post("/api/executions/{run_id}/retry", response_model=RunResponse)
async def retry_failed_run(
    run_id: UUID,
    db: AsyncSession = Depends(get_session),
    current_user: dict = Depends(get_current_user),
):
    """
    Retry only the failed nodes of a run, using checkpointed data from
    successful nodes. The original run_id is reused and status is reset.
    """
    run = await get_run(db, run_id)
    if not run:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Run not found")
    await _check_run_access(run, current_user, db)

    if run.status != "failed":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=(
                f"Cannot retry run in '{run.status}' status. "
                "Only failed runs can retry failed nodes; replay a cancelled run instead."
            ),
        )

    if run.graph_version_id is None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Run has no graph version, cannot retry",
        )

    run.status = "pending"
    run.error_message = None
    await enqueue_run_job(
        db,
        run.id,
        job_type=RunJobType.RETRY_FAILED.value,
        commit=False,
    )
    await db.commit()
    await db.refresh(run)
    return run


@router.post(
    "/api/executions/{run_id}/replay",
    response_model=RunResponse,
    status_code=status.HTTP_201_CREATED,
)
async def replay_run(
    run_id: UUID,
    db: AsyncSession = Depends(get_session),
    current_user: dict = Depends(get_current_user),
):
    """
    Full replay of a run from the beginning.
    Creates a new run with a new run_id, using the same graph version.
    """
    run = await get_run(db, run_id)
    if not run:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Run not found")
    await _check_run_access(run, current_user, db)

    if run.graph_version_id is None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Run has no graph version, cannot replay",
        )

    new_run = await create_run(
        db,
        graph_id=run.graph_id,
        triggered_by=current_user["user_id"],
        trigger_type="manual",
        graph_version_id=run.graph_version_id,
        enqueue_job_type=RunJobType.FULL.value,
    )
    return new_run
