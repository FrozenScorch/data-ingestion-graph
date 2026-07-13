"""Run-state and candidate lifecycle regressions for SDK acknowledgement."""

from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest
from app.models.execution import Run, RunJobType
from app.services.execution_service import (
    cancel_run,
    create_run,
    update_run_status,
)
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
async def test_new_full_run_prunes_only_prior_failed_and_cancelled_graph_candidates():
    graph_id = uuid4()
    db = AsyncMock()
    db.add = MagicMock()
    db.execute.return_value = SimpleNamespace(rowcount=2)

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

    prune_statement = db.execute.await_args.args[0]
    sql = str(prune_statement)
    raw_params = prune_statement.compile().params.values()
    params = {
        item
        for value in raw_params
        for item in (value if isinstance(value, (list, tuple)) else (value,))
    }
    assert "DELETE FROM sdk_source_state_candidates" in sql
    assert "graphs.owner_id" in sql
    assert "run_jobs" in sql
    assert "NOT (EXISTS" in sql
    assert graph_id in params
    assert {"failed", "cancelled"} <= params
    assert {"queued", "leased"} <= params
    assert "paused" not in params
    db.commit.assert_awaited_once()


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
