"""Durable dispatch, lease, heartbeat, and recovery contract tests."""

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest
from app.models.execution import Run, RunJob, RunJobStatus, RunJobType
from app.services.run_queue_service import (
    claim_run_job,
    enqueue_run_job,
    finish_run_job,
    heartbeat_run_job,
    mark_run_failed_if_owned,
    recover_orphaned_runs,
    release_run_job,
)


def scalar_result(value):
    result = MagicMock()
    result.scalar_one_or_none.return_value = value
    return result


@pytest.mark.asyncio
async def test_enqueue_resets_existing_job_without_creating_duplicate():
    job = RunJob(
        run_id=uuid4(),
        job_type=RunJobType.FULL.value,
        status=RunJobStatus.FAILED.value,
        lease_owner="dead-worker",
        lease_expires_at=datetime.now(timezone.utc),
        heartbeat_at=datetime.now(timezone.utc),
        last_error="crashed",
    )
    db = AsyncMock()
    db.execute = AsyncMock(return_value=scalar_result(job))

    returned = await enqueue_run_job(
        db,
        job.run_id,
        job_type=RunJobType.RETRY_FAILED.value,
        commit=False,
    )

    assert returned is job
    assert job.job_type == RunJobType.RETRY_FAILED.value
    assert job.status == RunJobStatus.QUEUED.value
    assert job.lease_owner is None
    assert job.lease_expires_at is None
    assert job.last_error is None
    db.add.assert_not_called()
    db.flush.assert_awaited_once()


@pytest.mark.asyncio
async def test_claim_assigns_expiring_lease_and_increments_attempt():
    now = datetime(2026, 7, 12, tzinfo=timezone.utc)
    job = RunJob(
        run_id=uuid4(),
        status=RunJobStatus.QUEUED.value,
        available_at=now - timedelta(seconds=1),
        attempt_count=2,
    )
    db = AsyncMock()
    db.execute = AsyncMock(return_value=scalar_result(job))

    claimed = await claim_run_job(
        db,
        worker_id="worker-1",
        lease_seconds=60,
        now=now,
    )

    assert claimed is job
    assert job.status == RunJobStatus.LEASED.value
    assert job.lease_owner == "worker-1"
    assert job.heartbeat_at == now
    assert job.lease_expires_at == now + timedelta(seconds=60)
    assert job.attempt_count == 3
    db.commit.assert_awaited_once()
    db.refresh.assert_awaited_once_with(job)


@pytest.mark.asyncio
async def test_claim_query_includes_expired_lease_recovery_and_skip_locked():
    db = AsyncMock()
    db.execute = AsyncMock(return_value=scalar_result(None))

    assert (
        await claim_run_job(
            db,
            worker_id="worker-1",
            lease_seconds=60,
            now=datetime(2026, 7, 12, tzinfo=timezone.utc),
        )
        is None
    )

    query = db.execute.await_args.args[0]
    sql = str(query)
    assert "run_jobs.lease_expires_at" in sql
    assert "FOR UPDATE" in sql
    db.rollback.assert_awaited_once()


@pytest.mark.asyncio
async def test_heartbeat_and_finish_are_fenced_by_lease_owner():
    heartbeat_result = SimpleNamespace(rowcount=1)
    finish_result = SimpleNamespace(rowcount=0)
    db = AsyncMock()
    db.execute = AsyncMock(side_effect=[heartbeat_result, finish_result])
    job_id = uuid4()

    renewed = await heartbeat_run_job(
        db,
        job_id=job_id,
        worker_id="worker-1",
        lease_seconds=60,
        now=datetime(2026, 7, 12, tzinfo=timezone.utc),
    )
    finished = await finish_run_job(
        db,
        job_id=job_id,
        worker_id="stale-worker",
    )

    assert renewed is True
    assert finished is False
    assert db.commit.await_count == 2


@pytest.mark.asyncio
async def test_mark_run_failed_rejects_an_expired_lease():
    job = RunJob(
        run_id=uuid4(),
        status=RunJobStatus.LEASED.value,
        lease_owner="stale-worker",
        lease_expires_at=datetime.now(timezone.utc) - timedelta(seconds=1),
    )
    db = AsyncMock()
    db.execute = AsyncMock(return_value=scalar_result(job))

    marked = await mark_run_failed_if_owned(
        db,
        job_id=job.id,
        run_id=job.run_id,
        worker_id="stale-worker",
        error="late failure",
    )

    assert marked is False
    db.rollback.assert_awaited_once()
    db.commit.assert_not_awaited()


@pytest.mark.asyncio
async def test_mark_run_failed_locks_job_and_run_before_commit():
    run = Run(id=uuid4(), graph_id=uuid4(), status="running")
    job = RunJob(
        run_id=run.id,
        status=RunJobStatus.LEASED.value,
        lease_owner="worker-1",
        lease_expires_at=datetime.now(timezone.utc) + timedelta(seconds=60),
    )
    db = AsyncMock()
    db.execute = AsyncMock(side_effect=[scalar_result(job), scalar_result(run)])

    marked = await mark_run_failed_if_owned(
        db,
        job_id=job.id,
        run_id=run.id,
        worker_id="worker-1",
        error="connector failed",
    )

    assert marked is True
    assert run.status == "failed"
    assert run.error_message == "connector failed"
    assert all("FOR UPDATE" in str(call.args[0]) for call in db.execute.await_args_list)
    db.commit.assert_awaited_once()


@pytest.mark.asyncio
async def test_graceful_release_is_fenced_by_lease_owner():
    db = AsyncMock()
    db.execute = AsyncMock(return_value=SimpleNamespace(rowcount=1))

    released = await release_run_job(
        db,
        job_id=uuid4(),
        worker_id="worker-1",
    )

    assert released is True
    db.commit.assert_awaited_once()


def test_worker_configuration_keeps_heartbeat_below_half_lease():
    from app.config import Settings

    with pytest.raises(ValueError, match="heartbeat must be less than half"):
        Settings(
            run_worker_lease_seconds=30,
            run_worker_heartbeat_seconds=15,
        )


@pytest.mark.asyncio
async def test_reclaimed_job_does_not_reexecute_completed_run(monkeypatch):
    from app.engine import run_job_executor

    run = SimpleNamespace(status="completed")
    job = SimpleNamespace(run_id=uuid4(), job_type=RunJobType.FULL.value)
    db = AsyncMock()
    db.get = AsyncMock(return_value=run)
    execute_full = AsyncMock()
    monkeypatch.setattr(run_job_executor, "_execute_full_run", execute_full)

    await run_job_executor.execute_run_job(db, job)

    execute_full.assert_not_awaited()


@pytest.mark.asyncio
async def test_recover_orphaned_runs_queues_existing_pending_and_running_runs():
    pending = Run(id=uuid4(), graph_id=uuid4(), graph_version_id=uuid4(), status="pending")
    running = Run(id=uuid4(), graph_id=uuid4(), graph_version_id=uuid4(), status="running")
    db = AsyncMock()
    result = MagicMock()
    result.scalars.return_value.all.return_value = [pending, running]
    db.execute = AsyncMock(return_value=result)
    db.add = MagicMock()

    recovered = await recover_orphaned_runs(db)

    assert recovered == 2
    assert pending.status == running.status == "pending"
    assert db.add.call_count == 2
    queued_jobs = [call.args[0] for call in db.add.call_args_list]
    assert {job.run_id for job in queued_jobs} == {pending.id, running.id}
    assert all(job.status == RunJobStatus.QUEUED.value for job in queued_jobs)
    db.commit.assert_awaited_once()
