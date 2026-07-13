from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest
from app.models.execution import Run, RunJob, RunJobStatus, RunStatus
from app.models.sdk_source_state import SDKSourceState, SDKSourceStateCandidate
from app.services.sdk_source_state_service import (
    RunCompletionLeaseError,
    SDKSourceStateLeaseError,
    StudioSDKSourceStateStore,
    complete_run_with_source_state_promotion,
)


def _scalar_result(value):
    result = MagicMock()
    result.scalar_one_or_none.return_value = value
    return result


def _advisory_result(acquired: bool = True):
    result = MagicMock()
    result.scalar_one.return_value = acquired
    return result


def test_state_pipeline_scope_includes_owner_graph_and_node():
    owner_a, owner_b, graph_id, run_id = uuid4(), uuid4(), uuid4(), uuid4()
    first = StudioSDKSourceStateStore(
        AsyncMock(),
        run_id=run_id,
        owner_id=owner_a,
        graph_id=graph_id,
        node_id="documents",
    )
    second = StudioSDKSourceStateStore(
        AsyncMock(),
        run_id=run_id,
        owner_id=owner_b,
        graph_id=graph_id,
        node_id="documents",
    )
    assert first.pipeline_key != second.pipeline_key
    with pytest.raises(ValueError, match="outside"):
        first._require_pipeline(second.pipeline_key)


@pytest.mark.asyncio
async def test_state_load_query_contains_every_owner_scope_dimension():
    owner_id, graph_id = uuid4(), uuid4()
    result = MagicMock()
    result.scalar_one_or_none.return_value = None
    session = AsyncMock()
    session.execute.return_value = result
    store = StudioSDKSourceStateStore(
        session,
        run_id=uuid4(),
        owner_id=owner_id,
        graph_id=graph_id,
        node_id="documents",
    )

    assert await store.load(store.pipeline_key, "local_documents", "upload-artifact") == {}
    statement = session.execute.await_args.args[0]
    parameter_values = set(statement.compile().params.values())
    assert owner_id in parameter_values
    assert graph_id in parameter_values
    assert "documents" in parameter_values
    assert "local_documents" in parameter_values
    assert "upload-artifact" in parameter_values


@pytest.mark.asyncio
async def test_state_lock_wait_is_bounded():
    owner_id, graph_id = uuid4(), uuid4()
    result = MagicMock()
    result.scalar_one.return_value = False
    session = AsyncMock()
    session.execute.return_value = result
    store = StudioSDKSourceStateStore(
        session,
        run_id=uuid4(),
        owner_id=owner_id,
        graph_id=graph_id,
        node_id="documents",
        lock_timeout_seconds=0,
    )

    with pytest.raises(TimeoutError, match="Timed out"):
        await store.acquire_lock()


@pytest.mark.asyncio
async def test_save_stages_run_candidate_without_mutating_committed_row():
    owner_id, graph_id, run_id = uuid4(), uuid4(), uuid4()
    committed = SDKSourceState(
        owner_id=owner_id,
        graph_id=graph_id,
        node_id="documents",
        source="local_documents",
        stream="upload-1",
        state_data={"cursor": 1},
        revision=4,
        is_deleted=False,
    )
    session = AsyncMock()
    session.add = MagicMock()
    run = Run(id=run_id, graph_id=graph_id, status="running")
    session.execute.side_effect = [
        _advisory_result(),
        _advisory_result(),
        _scalar_result(run),
        _scalar_result(None),
        _scalar_result(committed),
    ]
    store = StudioSDKSourceStateStore(
        session,
        run_id=run_id,
        owner_id=owner_id,
        graph_id=graph_id,
        node_id="documents",
    )

    await store.save(store.pipeline_key, "local_documents", "upload-1", {"cursor": 2})

    candidate = session.add.call_args.args[0]
    assert isinstance(candidate, SDKSourceStateCandidate)
    assert candidate.run_id == run_id
    assert candidate.base_state_data == {"cursor": 1}
    assert candidate.base_revision == 4
    assert candidate.state_data == {"cursor": 2}
    assert committed.state_data == {"cursor": 1}
    session.commit.assert_not_awaited()


@pytest.mark.asyncio
async def test_completion_cancellation_wins_before_candidate_promotion():
    run = Run(id=uuid4(), graph_id=uuid4(), status="cancelled")
    session = AsyncMock()
    session.execute.return_value = _scalar_result(run)

    assert await complete_run_with_source_state_promotion(session, run.id) is False
    session.rollback.assert_awaited_once()
    session.commit.assert_not_awaited()
    assert session.execute.await_count == 3


@pytest.mark.asyncio
async def test_cancelled_run_cannot_stage_a_late_candidate():
    run_id, owner_id, graph_id = uuid4(), uuid4(), uuid4()
    session = AsyncMock()
    session.execute.return_value = _scalar_result(
        Run(id=run_id, graph_id=graph_id, status="cancelled")
    )
    store = StudioSDKSourceStateStore(
        session,
        run_id=run_id,
        owner_id=owner_id,
        graph_id=graph_id,
        node_id="documents",
    )

    with pytest.raises(RuntimeError, match="inactive run"):
        await store.save(store.pipeline_key, "local_documents", "upload-1", {"cursor": 2})
    session.add.assert_not_called()


@pytest.mark.asyncio
async def test_completion_lost_lease_fails_before_candidate_promotion():
    run_id = uuid4()
    job = RunJob(
        id=uuid4(),
        run_id=run_id,
        status=RunJobStatus.LEASED.value,
        lease_owner="expired-worker",
        lease_expires_at=datetime.now(UTC) - timedelta(seconds=1),
    )
    session = AsyncMock()
    session.execute.return_value = _scalar_result(job)

    with pytest.raises(RunCompletionLeaseError, match="lease was lost"):
        await complete_run_with_source_state_promotion(
            session,
            run_id,
            job_id=job.id,
            lease_owner="expired-worker",
        )
    session.rollback.assert_awaited_once()
    session.commit.assert_not_awaited()
    assert session.execute.await_count == 3
    statements = [call.args[0] for call in session.execute.await_args_list]
    assert "pg_advisory_xact_lock" in str(statements[0])
    assert "sdk_source_state_candidates" in str(statements[1])
    assert "run_jobs" in str(statements[2])
    assert "FOR UPDATE" in str(statements[2])


@pytest.mark.asyncio
async def test_replaced_worker_after_long_read_cannot_stage_candidate():
    run_id, owner_id, graph_id, job_id = uuid4(), uuid4(), uuid4(), uuid4()
    job = RunJob(
        id=job_id,
        run_id=run_id,
        status=RunJobStatus.LEASED.value,
        lease_owner="stale-worker",
        lease_expires_at=datetime.now(UTC) + timedelta(minutes=5),
    )
    session = AsyncMock()

    async def execute(statement):
        index = session.execute.await_count
        if index in (2, 3):
            return _advisory_result()
        if index == 5:
            job.lease_owner = "replacement-worker"
        return _scalar_result(job)

    session.execute.side_effect = execute
    store = StudioSDKSourceStateStore(
        session,
        run_id=run_id,
        owner_id=owner_id,
        graph_id=graph_id,
        node_id="documents",
        job_id=job_id,
        lease_owner="stale-worker",
    )

    with pytest.raises(SDKSourceStateLeaseError, match="lease was lost"):
        await store.save(store.pipeline_key, "local_documents", "upload-1", {"cursor": 2})

    assert session.execute.await_count == 5
    statements = [call.args[0] for call in session.execute.await_args_list]
    assert "run_jobs" in str(statements[0]) and "FOR UPDATE" not in str(statements[0])
    assert "pg_try_advisory_xact_lock" in str(statements[1])
    assert "pg_try_advisory_xact_lock" in str(statements[2])
    assert "run_jobs" in str(statements[3]) and "FOR UPDATE" not in str(statements[3])
    assert "run_jobs" in str(statements[4]) and "FOR UPDATE" in str(statements[4])
    session.rollback.assert_awaited_once()
    session.add.assert_not_called()


@pytest.mark.asyncio
async def test_long_read_uses_only_advisories_then_staging_locks_job_before_run():
    run_id, owner_id, graph_id, job_id = uuid4(), uuid4(), uuid4(), uuid4()
    job = RunJob(
        id=job_id,
        run_id=run_id,
        status=RunJobStatus.LEASED.value,
        lease_owner="worker-1",
        lease_expires_at=datetime.now(UTC) + timedelta(minutes=5),
    )
    run = Run(id=run_id, graph_id=graph_id, status=RunStatus.RUNNING.value)
    session = AsyncMock()
    session.add = MagicMock()
    session.execute.side_effect = [
        _scalar_result(job),
        _advisory_result(),
        _advisory_result(),
        _scalar_result(job),
        _scalar_result(job),
        _scalar_result(run),
        _scalar_result(job),
        _scalar_result(None),
        _scalar_result(None),
        _scalar_result(job),
    ]
    store = StudioSDKSourceStateStore(
        session,
        run_id=run_id,
        owner_id=owner_id,
        graph_id=graph_id,
        node_id="documents",
        job_id=job_id,
        lease_owner="worker-1",
    )

    await store.acquire_lock()

    statements = [call.args[0] for call in session.execute.await_args_list]
    assert "run_jobs" in str(statements[0])
    assert "FOR UPDATE" not in str(statements[0])
    assert "pg_try_advisory_xact_lock" in str(statements[1])
    assert "pg_try_advisory_xact_lock" in str(statements[2])
    assert "run_jobs" in str(statements[3])
    assert "FOR UPDATE" not in str(statements[3])

    await store.save(store.pipeline_key, "local_documents", "upload-1", {"cursor": 1})

    statements = [call.args[0] for call in session.execute.await_args_list]
    assert "run_jobs" in str(statements[4]) and "FOR UPDATE" in str(statements[4])
    assert "runs" in str(statements[5]) and "run_jobs" not in str(statements[5])
    assert "FOR UPDATE" in str(statements[5])
    assert "run_jobs" in str(statements[6]) and "FOR UPDATE" in str(statements[6])
    assert isinstance(session.add.call_args.args[0], SDKSourceStateCandidate)


@pytest.mark.asyncio
async def test_lease_expiry_after_staging_wait_rolls_back_before_mutation():
    run_id, owner_id, graph_id, job_id = uuid4(), uuid4(), uuid4(), uuid4()
    job = RunJob(
        id=job_id,
        run_id=run_id,
        status=RunJobStatus.LEASED.value,
        lease_owner="worker-1",
        lease_expires_at=datetime.now(UTC) + timedelta(minutes=5),
    )
    run = Run(id=run_id, graph_id=graph_id, status=RunStatus.RUNNING.value)
    session = AsyncMock()

    async def execute(statement):
        index = session.execute.await_count
        if index == 1:
            return _scalar_result(job)
        if index in (2, 3):
            return _advisory_result()
        if index == 4:
            return _scalar_result(job)
        if index == 5:
            return _scalar_result(job)
        if index == 6:
            job.lease_expires_at = datetime.now(UTC) - timedelta(seconds=1)
            return _scalar_result(run)
        return _scalar_result(job)

    session.execute.side_effect = execute
    store = StudioSDKSourceStateStore(
        session,
        run_id=run_id,
        owner_id=owner_id,
        graph_id=graph_id,
        node_id="documents",
        job_id=job_id,
        lease_owner="worker-1",
    )

    with pytest.raises(SDKSourceStateLeaseError, match="lease was lost"):
        await store.save(store.pipeline_key, "local_documents", "upload-1", {"cursor": 2})

    session.rollback.assert_awaited_once()
    session.add.assert_not_called()


@pytest.mark.asyncio
async def test_completion_uses_run_scope_job_run_lock_order():
    run_id, graph_id, job_id = uuid4(), uuid4(), uuid4()
    job = RunJob(
        id=job_id,
        run_id=run_id,
        status=RunJobStatus.LEASED.value,
        lease_owner="worker-1",
        lease_expires_at=datetime.now(UTC) + timedelta(minutes=5),
    )
    run = Run(id=run_id, graph_id=graph_id, status=RunStatus.RUNNING.value)
    scopes = MagicMock()
    scopes.all.return_value = []
    candidates = MagicMock()
    candidates.scalars.return_value.all.return_value = []
    session = AsyncMock()
    session.execute.side_effect = [
        MagicMock(),
        scopes,
        _scalar_result(job),
        _scalar_result(run),
        _scalar_result(job),
        candidates,
        _scalar_result(job),
    ]

    assert await complete_run_with_source_state_promotion(
        session,
        run_id,
        job_id=job_id,
        lease_owner="worker-1",
    )

    statements = [call.args[0] for call in session.execute.await_args_list]
    assert "pg_advisory_xact_lock" in str(statements[0])
    assert "sdk_source_state_candidates" in str(statements[1])
    assert "run_jobs" in str(statements[2]) and "FOR UPDATE" in str(statements[2])
    assert "runs" in str(statements[3]) and "run_jobs" not in str(statements[3])
    assert "FOR UPDATE" in str(statements[3])
    assert run.status == RunStatus.COMPLETED.value
    session.commit.assert_awaited_once()
