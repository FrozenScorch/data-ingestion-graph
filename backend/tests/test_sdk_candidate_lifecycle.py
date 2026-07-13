"""Run-state and candidate lifecycle regressions for SDK acknowledgement."""

from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest
from app.api.executions import retry_failed_run
from app.models.execution import Run, RunJob, RunJobStatus, RunJobType, RunStatus
from app.services.execution_service import (
    cancel_run,
    create_run,
    fail_run_if_running,
    update_run_status,
)
from fastapi import HTTPException
from sqlalchemy.dialects import postgresql


def _scalar_result(value):
    result = MagicMock()
    result.scalar_one_or_none.return_value = value
    return result


@pytest.mark.asyncio
async def test_stale_cancel_refreshes_locked_identity_and_cannot_overwrite_completion():
    cached_run = Run(id=uuid4(), graph_id=uuid4(), status="running")
    db = AsyncMock()

    async def locked_reload(statement):
        assert "FOR UPDATE" in str(statement)
        assert statement.get_execution_options()["populate_existing"] is True
        # Model another session completing after this session cached RUNNING.
        cached_run.status = "completed"
        return _scalar_result(cached_run)

    db.execute.side_effect = locked_reload

    with pytest.raises(ValueError, match="completed -> cancelled"):
        await cancel_run(db, cached_run.id)

    assert cached_run.status == "completed"
    db.commit.assert_not_awaited()
    assert db.execute.await_count == 1


@pytest.mark.asyncio
async def test_cancel_deletes_only_that_tenant_graph_run_candidates_before_commit():
    run = Run(id=uuid4(), graph_id=uuid4(), status="running")
    db = AsyncMock()
    db.execute.side_effect = [_scalar_result(run), SimpleNamespace(rowcount=2)]

    returned = await cancel_run(db, run.id)

    assert returned is run
    assert run.status == "cancelled"
    delete_statement = db.execute.await_args_list[1].args[0]
    sql = str(delete_statement)
    params = set(delete_statement.compile().params.values())
    assert "DELETE FROM sdk_source_state_candidates" in sql
    assert "graphs.owner_id" in sql
    assert run.id in params
    assert run.graph_id in params
    db.commit.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("initial_status", "target_status"),
    [("failed", "running"), ("paused", "running")],
)
async def test_retryable_and_paused_runs_keep_candidates(initial_status, target_status):
    run = Run(id=uuid4(), graph_id=uuid4(), status=initial_status)
    db = AsyncMock()
    db.execute.return_value = _scalar_result(run)

    await update_run_status(db, run.id, target_status)

    assert db.execute.await_count == 1
    db.commit.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("current_status", ["cancelled", "paused", "completed", "superseded"])
async def test_failure_transition_forced_refresh_preserves_non_running_status(current_status):
    cached_run = Run(id=uuid4(), graph_id=uuid4(), status="running")
    db = AsyncMock()

    async def locked_reload(statement):
        assert "FOR UPDATE" in str(statement)
        assert statement.get_execution_options()["populate_existing"] is True
        cached_run.status = current_status
        return _scalar_result(cached_run)

    db.execute.side_effect = locked_reload

    returned, transitioned = await fail_run_if_running(db, cached_run.id, "late failure")

    assert returned is cached_run
    assert transitioned is False
    assert cached_run.status == current_status
    assert cached_run.error_message is None
    db.commit.assert_awaited_once()


@pytest.mark.asyncio
async def test_failure_transition_changes_only_locked_running_run():
    run = Run(id=uuid4(), graph_id=uuid4(), status="running")
    db = AsyncMock()
    db.execute.return_value = _scalar_result(run)

    returned, transitioned = await fail_run_if_running(db, run.id, "node failed")

    assert returned is run
    assert transitioned is True
    assert run.status == "failed"
    assert run.error_message == "node failed"
    db.commit.assert_awaited_once()
    db.refresh.assert_awaited_once_with(run)


@pytest.mark.asyncio
async def test_new_full_run_supersedes_only_inactive_locked_failed_runs():
    graph_id = uuid4()
    no_job = Run(id=uuid4(), graph_id=graph_id, status=RunStatus.FAILED.value)
    failed_job_run = Run(id=uuid4(), graph_id=graph_id, status=RunStatus.FAILED.value)
    queued_run = Run(id=uuid4(), graph_id=graph_id, status=RunStatus.FAILED.value)
    leased_run = Run(id=uuid4(), graph_id=graph_id, status=RunStatus.FAILED.value)
    failed_job = RunJob(
        id=uuid4(),
        run_id=failed_job_run.id,
        status=RunJobStatus.FAILED.value,
    )
    queued_job = RunJob(
        id=uuid4(),
        run_id=queued_run.id,
        status=RunJobStatus.QUEUED.value,
    )
    leased_job = RunJob(
        id=uuid4(),
        run_id=leased_run.id,
        status=RunJobStatus.LEASED.value,
    )
    run_result = MagicMock()
    run_result.scalars.return_value.all.return_value = [
        no_job,
        failed_job_run,
        queued_run,
        leased_run,
    ]
    job_result = MagicMock()
    job_result.scalars.return_value.all.return_value = [failed_job, queued_job, leased_job]
    db = AsyncMock()
    db.add = MagicMock()
    db.execute.side_effect = [run_result, job_result, SimpleNamespace(rowcount=2)]

    async def assign_run_id():
        db.add.call_args.args[0].id = uuid4()

    db.flush.side_effect = assign_run_id
    with patch(
        "app.services.run_queue_service.enqueue_run_job",
        new=AsyncMock(),
    ):
        await create_run(
            db,
            graph_id=graph_id,
            triggered_by=uuid4(),
            graph_version_id=uuid4(),
            enqueue_job_type=RunJobType.FULL.value,
        )

    run_lock = db.execute.await_args_list[0].args[0]
    job_lock = db.execute.await_args_list[1].args[0]
    delete_statement = db.execute.await_args_list[2].args[0]
    assert "FOR UPDATE" in str(run_lock)
    assert "ORDER BY runs.id" in str(run_lock)
    assert run_lock.get_execution_options()["populate_existing"] is True
    assert "FOR UPDATE" in str(job_lock)
    assert "ORDER BY run_jobs.run_id ASC, run_jobs.id ASC" in str(job_lock)
    assert job_lock.get_execution_options()["populate_existing"] is True
    raw_params = delete_statement.compile().params.values()
    params = {
        item
        for value in raw_params
        for item in (value if isinstance(value, (list, tuple)) else (value,))
    }
    assert "DELETE FROM sdk_source_state_candidates" in str(delete_statement)
    assert "graphs.owner_id" in str(delete_statement)
    assert graph_id in params
    assert {no_job.id, failed_job_run.id} <= params
    assert queued_run.id not in params
    assert leased_run.id not in params
    assert no_job.status == failed_job_run.status == RunStatus.SUPERSEDED.value
    assert queued_run.status == leased_run.status == RunStatus.FAILED.value
    db.commit.assert_awaited_once()


@pytest.mark.asyncio
async def test_retry_locked_refresh_wins_and_queues_in_same_transaction():
    run = Run(
        id=uuid4(),
        graph_id=uuid4(),
        graph_version_id=uuid4(),
        status=RunStatus.FAILED.value,
    )
    db = AsyncMock()
    db.execute.return_value = _scalar_result(run)
    with (
        patch("app.api.executions.get_run", new=AsyncMock(return_value=run)),
        patch("app.api.executions.enqueue_run_job", new=AsyncMock()) as enqueue,
    ):
        returned = await retry_failed_run(
            run_id=run.id,
            db=db,
            current_user={"user_id": uuid4(), "role": "admin"},
        )

    lock_statement = db.execute.await_args.args[0]
    assert "FOR UPDATE" in str(lock_statement)
    assert lock_statement.get_execution_options()["populate_existing"] is True
    assert returned.status == RunStatus.PENDING.value
    enqueue.assert_awaited_once_with(
        db,
        run.id,
        job_type=RunJobType.RETRY_FAILED.value,
        commit=False,
    )
    db.commit.assert_awaited_once()


@pytest.mark.asyncio
async def test_new_full_run_wins_then_locked_retry_rejects_superseded_run():
    stale_access_run = Run(
        id=uuid4(),
        graph_id=uuid4(),
        graph_version_id=uuid4(),
        status=RunStatus.FAILED.value,
    )
    locked_run = Run(
        id=stale_access_run.id,
        graph_id=stale_access_run.graph_id,
        graph_version_id=stale_access_run.graph_version_id,
        status=RunStatus.SUPERSEDED.value,
    )
    db = AsyncMock()
    db.execute.return_value = _scalar_result(locked_run)
    with (
        patch("app.api.executions.get_run", new=AsyncMock(return_value=stale_access_run)),
        patch("app.api.executions.enqueue_run_job", new=AsyncMock()) as enqueue,
        pytest.raises(HTTPException, match="superseded") as exc_info,
    ):
        await retry_failed_run(
            run_id=locked_run.id,
            db=db,
            current_user={"user_id": uuid4(), "role": "admin"},
        )

    assert exc_info.value.status_code == 400
    enqueue.assert_not_awaited()
    db.rollback.assert_awaited_once()
    db.commit.assert_not_awaited()


def test_0004_downgrade_deletes_tombstones_before_dropping_marker_column():
    migration_path = (
        Path(__file__).resolve().parents[1]
        / "alembic"
        / "versions"
        / "0004_sdk_source_state_candidates.py"
    )
    spec = spec_from_file_location("sdk_state_migration_0004", migration_path)
    assert spec is not None and spec.loader is not None
    module = module_from_spec(spec)
    spec.loader.exec_module(module)
    fake_op = MagicMock()
    module.op = fake_op

    module.downgrade()

    method_names = [call[0] for call in fake_op.method_calls]
    execute_index = method_names.index("execute")
    drop_deleted_index = next(
        index
        for index, call in enumerate(fake_op.method_calls)
        if call[0] == "drop_column" and call.args == ("sdk_source_states", "is_deleted")
    )
    statement = fake_op.execute.call_args.args[0]
    sql = str(statement.compile(dialect=postgresql.dialect()))
    assert "DELETE FROM sdk_source_states" in sql
    assert "is_deleted IS true" in sql
    assert execute_index < drop_deleted_index
