"""
Execution service: run creation, management, and control.
"""

import logging
from uuid import UUID

from app.models.execution import Run, RunStatus, TriggerType
from app.models.graph import Graph
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

logger = logging.getLogger(__name__)


async def create_run(
    db: AsyncSession,
    graph_id: UUID,
    triggered_by: UUID,
    trigger_type: str = TriggerType.MANUAL.value,
    graph_version_id: UUID | None = None,
) -> Run:
    """Create a new run for a graph."""
    run = Run(
        graph_id=graph_id,
        graph_version_id=graph_version_id,
        trigger_type=trigger_type,
        triggered_by=triggered_by,
        status=RunStatus.PENDING.value,
    )
    db.add(run)
    await db.commit()
    await db.refresh(run)
    return run


async def get_run(
    db: AsyncSession,
    run_id: UUID,
    *,
    load_nodes: bool = False,
) -> Run | None:
    """Get a run by ID with optional relationship loading.

    Args:
        db: Async database session.
        run_id: UUID of the run.
        load_nodes: If True, eagerly load run_nodes (used by detail endpoints).
    """
    stmt = select(Run).where(Run.id == run_id)
    if load_nodes:
        stmt = stmt.options(selectinload(Run.run_nodes))
    result = await db.execute(stmt)
    return result.scalar_one_or_none()


async def list_runs(
    db: AsyncSession,
    graph_id: UUID | None = None,
    status: str | None = None,
    offset: int = 0,
    limit: int = 50,
    owner_id: UUID | None = None,
) -> tuple[list[Run], int]:
    """List runs with optional filtering."""
    query = select(Run)
    count_query = select(func.count()).select_from(Run)

    if owner_id is not None:
        query = query.join(Graph, Graph.id == Run.graph_id).where(Graph.owner_id == owner_id)
        count_query = count_query.join(Graph, Graph.id == Run.graph_id).where(
            Graph.owner_id == owner_id
        )

    if graph_id:
        query = query.where(Run.graph_id == graph_id)
        count_query = count_query.where(Run.graph_id == graph_id)
    if status:
        query = query.where(Run.status == status)
        count_query = count_query.where(Run.status == status)

    query = query.order_by(Run.created_at.desc()).offset(offset).limit(limit)

    total_result = await db.execute(count_query)
    total = total_result.scalar() or 0

    result = await db.execute(query)
    runs = list(result.scalars().all())
    return runs, total


async def update_run_status(
    db: AsyncSession,
    run_id: UUID,
    new_status: str,
) -> Run | None:
    """Update a run's status."""
    run = await get_run(db, run_id)
    if not run:
        return None

    from app.engine.state import can_transition

    if not can_transition(run.status, new_status):
        raise ValueError(f"Invalid status transition: {run.status} -> {new_status}")

    run.status = new_status
    await db.commit()
    await db.refresh(run)
    return run


async def cancel_run(db: AsyncSession, run_id: UUID) -> Run | None:
    """Cancel a run."""
    return await update_run_status(db, run_id, RunStatus.CANCELLED.value)


async def pause_run(db: AsyncSession, run_id: UUID) -> Run | None:
    """Pause a running run."""
    return await update_run_status(db, run_id, RunStatus.PAUSED.value)


async def resume_run(db: AsyncSession, run_id: UUID) -> Run | None:
    """Resume a paused run."""
    return await update_run_status(db, run_id, RunStatus.RUNNING.value)
