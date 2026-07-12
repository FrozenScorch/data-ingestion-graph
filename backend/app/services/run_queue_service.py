"""Database-backed run dispatch with expiring worker leases."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from uuid import UUID

from app.models.execution import Run, RunJob, RunJobStatus, RunJobType, RunStatus
from sqlalchemy import and_, or_, select, update
from sqlalchemy.ext.asyncio import AsyncSession


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


async def recover_orphaned_runs(db: AsyncSession) -> int:
    """Queue pending/running runs left behind before durable dispatch existed."""
    result = await db.execute(
        select(Run)
        .outerjoin(RunJob, RunJob.run_id == Run.id)
        .where(
            RunJob.id.is_(None),
            Run.graph_version_id.is_not(None),
            Run.status.in_((RunStatus.PENDING.value, RunStatus.RUNNING.value)),
        )
        .with_for_update(skip_locked=True)
    )
    runs = list(result.scalars().all())
    for run in runs:
        run.status = RunStatus.PENDING.value
        run.error_message = None
        db.add(
            RunJob(
                run_id=run.id,
                job_type=RunJobType.FULL.value,
                status=RunJobStatus.QUEUED.value,
            )
        )
    await db.commit()
    return len(runs)


async def enqueue_run_job(
    db: AsyncSession,
    run_id: UUID,
    *,
    job_type: str = RunJobType.FULL.value,
    commit: bool = True,
) -> RunJob:
    """Create or reset the single durable dispatch row for a run."""
    result = await db.execute(select(RunJob).where(RunJob.run_id == run_id).with_for_update())
    job = result.scalar_one_or_none()
    if job is None:
        job = RunJob(run_id=run_id, job_type=job_type, status=RunJobStatus.QUEUED.value)
        db.add(job)
    else:
        job.job_type = job_type
        job.status = RunJobStatus.QUEUED.value
        job.available_at = utc_now()
        job.lease_owner = None
        job.lease_expires_at = None
        job.heartbeat_at = None
        job.last_error = None
    if commit:
        await db.commit()
        await db.refresh(job)
    else:
        await db.flush()
    return job


async def claim_run_job(
    db: AsyncSession,
    *,
    worker_id: str,
    lease_seconds: int,
    now: datetime | None = None,
) -> RunJob | None:
    """Atomically claim one queued job or reclaim an expired lease."""
    claimed_at = now or utc_now()
    claimable = or_(
        and_(
            RunJob.status == RunJobStatus.QUEUED.value,
            RunJob.available_at <= claimed_at,
        ),
        and_(
            RunJob.status == RunJobStatus.LEASED.value,
            RunJob.lease_expires_at.is_not(None),
            RunJob.lease_expires_at <= claimed_at,
        ),
    )
    result = await db.execute(
        select(RunJob)
        .where(claimable)
        .order_by(RunJob.available_at.asc(), RunJob.created_at.asc())
        .with_for_update(skip_locked=True)
        .limit(1)
    )
    job = result.scalar_one_or_none()
    if job is None:
        await db.rollback()
        return None

    job.status = RunJobStatus.LEASED.value
    job.lease_owner = worker_id
    job.heartbeat_at = claimed_at
    job.lease_expires_at = claimed_at + timedelta(seconds=lease_seconds)
    job.attempt_count += 1
    await db.commit()
    await db.refresh(job)
    return job


async def heartbeat_run_job(
    db: AsyncSession,
    *,
    job_id: UUID,
    worker_id: str,
    lease_seconds: int,
    now: datetime | None = None,
) -> bool:
    """Extend a lease only while it is still owned by this worker."""
    heartbeat_at = now or utc_now()
    result = await db.execute(
        update(RunJob)
        .where(
            RunJob.id == job_id,
            RunJob.status == RunJobStatus.LEASED.value,
            RunJob.lease_owner == worker_id,
        )
        .values(
            heartbeat_at=heartbeat_at,
            lease_expires_at=heartbeat_at + timedelta(seconds=lease_seconds),
        )
    )
    await db.commit()
    return bool(result.rowcount)


async def finish_run_job(
    db: AsyncSession,
    *,
    job_id: UUID,
    worker_id: str,
    error: str | None = None,
) -> bool:
    """Complete a job only if the caller still owns its lease."""
    values = {
        "status": RunJobStatus.FAILED.value if error else RunJobStatus.COMPLETED.value,
        "lease_owner": None,
        "lease_expires_at": None,
        "heartbeat_at": utc_now(),
        "last_error": error,
    }
    result = await db.execute(
        update(RunJob)
        .where(
            RunJob.id == job_id,
            RunJob.status == RunJobStatus.LEASED.value,
            RunJob.lease_owner == worker_id,
        )
        .values(**values)
    )
    await db.commit()
    return bool(result.rowcount)


async def release_run_job(
    db: AsyncSession,
    *,
    job_id: UUID,
    worker_id: str,
) -> bool:
    """Return an owned lease to the queue during graceful worker shutdown."""
    result = await db.execute(
        update(RunJob)
        .where(
            RunJob.id == job_id,
            RunJob.status == RunJobStatus.LEASED.value,
            RunJob.lease_owner == worker_id,
        )
        .values(
            status=RunJobStatus.QUEUED.value,
            available_at=utc_now(),
            lease_owner=None,
            lease_expires_at=None,
            heartbeat_at=None,
        )
    )
    await db.commit()
    return bool(result.rowcount)
