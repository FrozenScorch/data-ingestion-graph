from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest
from app.models.execution import Run, RunJob, RunJobStatus
from app.models.sdk_source_state import SDKSourceState, SDKSourceStateCandidate
from app.services.sdk_source_state_service import (
    RunCompletionLeaseError,
    StudioSDKSourceStateStore,
    complete_run_with_source_state_promotion,
)


def _scalar_result(value):
    result = MagicMock()
    result.scalar_one_or_none.return_value = value
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
    assert session.execute.await_count == 2


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
    assert session.execute.await_count == 1
