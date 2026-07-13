"""
Execution service: run creation, management, and control.
"""

import logging
from datetime import UTC, datetime
from uuid import UUID

from app.models.execution import (
    Run,
    RunJob,
    RunJobStatus,
    RunJobType,
    RunStatus,
    TriggerType,
)
from app.models.graph import Graph
from app.models.sdk_source_state import SDKSourceStateCandidate
from sqlalchemy import delete, func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

logger = logging.getLogger(__name__)


class RunFailureLeaseError(RuntimeError):
    """The worker no longer owns the lease required to fail a run."""


async def create_run(
    db: AsyncSession,
    graph_id: UUID,
    triggered_by: UUID,
    trigger_type: str = TriggerType.MANUAL.value,
    graph_version_id: UUID | None = None,
    enqueue_job_type: str | None = None,
) -> Run:
    """Create a new run, optionally with an atomic durable dispatch row."""
    if enqueue_job_type == RunJobType.FULL.value:
        await _supersede_inactive_failed_runs(db, graph_id)
    run = Run(
        graph_id=graph_id,
        graph_version_id=graph_version_id,
        trigger_type=trigger_type,
        triggered_by=triggered_by,
        status=RunStatus.PENDING.value,
    )
    db.add(run)
    if enqueue_job_type is not None:
        from app.services.run_queue_service import enqueue_run_job

        await db.flush()
        await enqueue_run_job(
            db,
            run.id,
            job_type=enqueue_job_type or RunJobType.FULL.value,
            commit=False,
        )
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
    """Update a run's status while serializing against final acknowledgement."""
    result = await db.execute(
        select(Run)
        .where(Run.id == run_id)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    run = result.scalar_one_or_none()
    if not run:
        return None

    from app.engine.state import can_transition

    if not can_transition(run.status, new_status):
        raise ValueError(f"Invalid status transition: {run.status} -> {new_status}")

    run.status = new_status
    if new_status == RunStatus.CANCELLED.value:
        await _delete_run_source_state_candidates(db, run)
    await db.commit()
    await db.refresh(run)
    return run


async def fail_run_if_running(
    db: AsyncSession,
    run_id: UUID,
    error_message: str,
    *,
    job_id: UUID | None = None,
    lease_owner: str | None = None,
) -> tuple[Run | None, bool]:
    """Fail a still-running run, fencing durable workers by their current lease."""
    job: RunJob | None = None
    if job_id is not None:
        job_result = await db.execute(
            select(RunJob)
            .where(RunJob.id == job_id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
        job = job_result.scalar_one_or_none()
        now = datetime.now(UTC)
        if (
            job is None
            or lease_owner is None
            or job.id != job_id
            or job.run_id != run_id
            or job.status != RunJobStatus.LEASED.value
            or job.lease_owner != lease_owner
            or job.lease_expires_at is None
            or job.lease_expires_at <= now
        ):
            await db.rollback()
            raise RunFailureLeaseError("Run job lease was lost before failure transition")
    elif lease_owner is not None:
        await db.rollback()
        raise RunFailureLeaseError("Run failure lease owner requires a job id")

    result = await db.execute(
        select(Run)
        .where(Run.id == run_id)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    run = result.scalar_one_or_none()
    if run is None:
        await db.rollback()
        return None, False
    if job_id is not None and not _owned_unexpired_job(
        job,
        job_id=job_id,
        run_id=run_id,
        lease_owner=lease_owner,
    ):
        await db.rollback()
        raise RunFailureLeaseError("Run job lease was lost after failure acquired the run")
    transitioned = run.status == RunStatus.RUNNING.value
    if transitioned:
        run.status = RunStatus.FAILED.value
        run.error_message = error_message
    if job_id is not None and not _owned_unexpired_job(
        job,
        job_id=job_id,
        run_id=run_id,
        lease_owner=lease_owner,
    ):
        await db.rollback()
        raise RunFailureLeaseError("Run job lease was lost before failure commit")
    await db.commit()
    if transitioned:
        await db.refresh(run)
    return run, transitioned


def _owned_unexpired_job(
    job: RunJob | None,
    *,
    job_id: UUID,
    run_id: UUID,
    lease_owner: str | None,
) -> bool:
    return bool(
        job is not None
        and job.id == job_id
        and job.run_id == run_id
        and job.status == RunJobStatus.LEASED.value
        and lease_owner is not None
        and job.lease_owner == lease_owner
        and job.lease_expires_at is not None
        and job.lease_expires_at > datetime.now(UTC)
    )


async def cancel_run(db: AsyncSession, run_id: UUID) -> Run | None:
    """Cancel a run."""
    return await update_run_status(db, run_id, RunStatus.CANCELLED.value)


async def pause_run(db: AsyncSession, run_id: UUID) -> Run | None:
    """Pause a run while serializing with its durable dispatch row."""
    await _lock_control_job(db, run_id)
    run = await _lock_control_run(db, run_id)
    if run is None:
        await db.rollback()
        return None

    from app.engine.state import can_transition

    if not can_transition(run.status, RunStatus.PAUSED.value):
        await db.rollback()
        raise ValueError(f"Invalid status transition: {run.status} -> paused")
    run.status = RunStatus.PAUSED.value
    await db.commit()
    await db.refresh(run)
    return run


async def resume_run(db: AsyncSession, run_id: UUID) -> Run | None:
    """Resume a paused run and invalidate/requeue its prior worker lease."""
    job = await _lock_control_job(db, run_id)
    run = await _lock_control_run(db, run_id)
    if run is None:
        await db.rollback()
        return None

    from app.engine.state import can_transition

    if not can_transition(run.status, RunStatus.RUNNING.value):
        await db.rollback()
        raise ValueError(f"Invalid status transition: {run.status} -> running")

    run.status = RunStatus.RUNNING.value
    if job is not None:
        job.status = RunJobStatus.QUEUED.value
        job.available_at = datetime.now(UTC)
        job.lease_owner = None
        job.lease_expires_at = None
        job.heartbeat_at = None
        job.last_error = None
    await db.commit()
    await db.refresh(run)
    return run


async def _lock_control_job(db: AsyncSession, run_id: UUID) -> RunJob | None:
    result = await db.execute(
        select(RunJob)
        .where(RunJob.run_id == run_id)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    return result.scalar_one_or_none()


async def _lock_control_run(db: AsyncSession, run_id: UUID) -> Run | None:
    result = await db.execute(
        select(Run)
        .where(Run.id == run_id)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    return result.scalar_one_or_none()


def _graph_owner_id(graph_id: UUID):
    return select(Graph.owner_id).where(Graph.id == graph_id).scalar_subquery()


async def _delete_run_source_state_candidates(db: AsyncSession, run: Run) -> None:
    await db.execute(
        delete(SDKSourceStateCandidate).where(
            SDKSourceStateCandidate.run_id == run.id,
            SDKSourceStateCandidate.graph_id == run.graph_id,
            SDKSourceStateCandidate.owner_id == _graph_owner_id(run.graph_id),
        )
    )


async def _supersede_inactive_failed_runs(db: AsyncSession, graph_id: UUID) -> None:
    candidate_result = await db.execute(
        select(Run.id)
        .where(
            Run.graph_id == graph_id,
            Run.status == RunStatus.FAILED.value,
        )
        .order_by(Run.id.asc())
    )
    candidate_run_ids = list(candidate_result.scalars().all())
    if not candidate_run_ids:
        return

    job_result = await db.execute(
        select(RunJob)
        .where(RunJob.run_id.in_(candidate_run_ids))
        .order_by(RunJob.run_id.asc(), RunJob.id.asc())
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    jobs_by_run_id = {job.run_id: job for job in job_result.scalars().all()}

    run_result = await db.execute(
        select(Run)
        .where(
            Run.graph_id == graph_id,
            Run.id.in_(candidate_run_ids),
        )
        .order_by(Run.id.asc())
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    failed_runs = list(run_result.scalars().all())
    active_job_statuses = {RunJobStatus.QUEUED.value, RunJobStatus.LEASED.value}
    superseded_run_ids: list[UUID] = []
    for run in failed_runs:
        if run.status != RunStatus.FAILED.value:
            continue
        job = jobs_by_run_id.get(run.id)
        if job is not None and job.status in active_job_statuses:
            continue
        run.status = RunStatus.SUPERSEDED.value
        superseded_run_ids.append(run.id)

    if not superseded_run_ids:
        return
    await db.execute(
        delete(SDKSourceStateCandidate).where(
            SDKSourceStateCandidate.graph_id == graph_id,
            SDKSourceStateCandidate.owner_id == _graph_owner_id(graph_id),
            SDKSourceStateCandidate.run_id.in_(superseded_run_ids),
        )
    )
