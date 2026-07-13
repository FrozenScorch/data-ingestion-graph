"""Promotion-point tests for downstream SDK source-state acknowledgement."""

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest
from app.engine import executor as executor_module
from app.engine import run_job_executor
from app.engine.executor import DAGExecutor
from app.models.execution import NodeStatus, Run, RunJob, RunNode


def _scalar_result(value):
    result = MagicMock()
    result.scalar_one_or_none.return_value = value
    return result


@pytest.mark.asyncio
async def test_full_run_promotes_only_after_destination_level_completes(monkeypatch):
    run = Run(id=uuid4(), graph_id=uuid4(), status="pending")
    db = AsyncMock()
    db.execute.return_value = _scalar_result(run)
    executor = DAGExecutor(db)
    monkeypatch.setattr(executor, "_graph_owner", AsyncMock(return_value=uuid4()))
    monkeypatch.setattr(executor, "_resolve_connections", AsyncMock(return_value={}))
    events: list[str] = []

    async def execute_level(_run, _level, level_nodes, *_args):
        events.append(level_nodes[0])
        return None

    async def promote(_db, _run_id, **_kwargs):
        events.append("promote")
        return True

    monkeypatch.setattr(executor, "_execute_level_sequential", AsyncMock(side_effect=execute_level))
    monkeypatch.setattr(executor_module, "complete_run_with_source_state_promotion", promote)

    await executor.execute(
        run,
        {"source": {"type": "sdk_document_source"}, "destination": {"type": "writer"}},
        [{"source": "source", "target": "destination"}],
    )

    assert events == ["source", "destination", "promote"]


@pytest.mark.asyncio
async def test_destination_failure_never_reaches_source_state_promotion(monkeypatch):
    run = Run(id=uuid4(), graph_id=uuid4(), status="pending")
    db = AsyncMock()
    db.execute.return_value = _scalar_result(run)
    executor = DAGExecutor(db)
    monkeypatch.setattr(executor, "_graph_owner", AsyncMock(return_value=uuid4()))
    monkeypatch.setattr(executor, "_resolve_connections", AsyncMock(return_value={}))
    promote = AsyncMock()

    async def execute_level(run_arg, _level, level_nodes, *_args):
        if level_nodes == ["destination"]:
            run_arg.status = "failed"
            return run_arg
        return None

    monkeypatch.setattr(executor, "_execute_level_sequential", AsyncMock(side_effect=execute_level))
    monkeypatch.setattr(executor_module, "complete_run_with_source_state_promotion", promote)

    await executor.execute(
        run,
        {"source": {"type": "sdk_document_source"}, "destination": {"type": "writer"}},
        [{"source": "source", "target": "destination"}],
    )

    promote.assert_not_awaited()


@pytest.mark.asyncio
async def test_executor_locked_reload_does_not_resurrect_cached_cancelled_run(monkeypatch):
    cached_run = Run(id=uuid4(), graph_id=uuid4(), status="running")
    db = AsyncMock()

    async def locked_reload(statement):
        assert "FOR UPDATE" in str(statement)
        assert statement.get_execution_options()["populate_existing"] is True
        # Faithfully model SQLAlchemy refreshing the existing identity-map object.
        cached_run.status = "cancelled"
        return _scalar_result(cached_run)

    db.execute.side_effect = locked_reload
    executor = DAGExecutor(db)
    graph_owner = AsyncMock()
    monkeypatch.setattr(executor, "_graph_owner", graph_owner)

    returned = await executor.execute(cached_run, {}, [])

    assert returned is cached_run
    assert returned.status == "cancelled"
    db.commit.assert_awaited_once()
    graph_owner.assert_not_awaited()


@pytest.mark.asyncio
async def test_failed_node_retry_restores_source_output_and_promotes_without_rerun(monkeypatch):
    graph_id, run_id, version_id = uuid4(), uuid4(), uuid4()
    run = Run(id=run_id, graph_id=graph_id, graph_version_id=version_id, status="failed")
    job = RunJob(id=uuid4(), run_id=run_id, status="leased", lease_owner="worker-1")
    version = SimpleNamespace(
        graph_id=graph_id,
        nodes_data={
            "source": {"type": "sdk_document_source"},
            "destination": {"type": "writer"},
        },
        edges_data=[{"source": "source", "target": "destination"}],
        node_configs={},
    )
    checkpoint = SimpleNamespace(
        checkpoint_type="post_exec",
        node_output={"items": [{"id": "restored"}]},
        node_id="source",
    )
    failed_node = RunNode(
        run_id=run_id,
        node_id="destination",
        node_type="writer",
        status=NodeStatus.FAILED.value,
    )
    completed_node = RunNode(
        run_id=run_id,
        node_id="destination",
        node_type="writer",
        status=NodeStatus.COMPLETED.value,
        output_data={"written": 1},
    )
    node_result = MagicMock()
    node_result.scalars.return_value.all.return_value = [failed_node]
    owner_result = MagicMock()
    owner_result.scalar_one_or_none.return_value = uuid4()
    db = AsyncMock()
    db.get.return_value = version
    db.execute.side_effect = [node_result, owner_result, _scalar_result(run)]

    executor = MagicMock()
    executor._resolve_connections = AsyncMock(return_value={})
    executor._collect_inputs.return_value = ({"items": [{"id": "restored"}]}, [])
    monkeypatch.setattr(run_job_executor, "DAGExecutor", MagicMock(return_value=executor))
    monkeypatch.setattr(run_job_executor, "get_checkpoints", AsyncMock(return_value=[checkpoint]))
    run_node = AsyncMock(return_value=completed_node)
    monkeypatch.setattr(run_job_executor, "run_node_with_retry", run_node)
    monkeypatch.setattr(run_job_executor, "save_checkpoint", AsyncMock())
    promote = AsyncMock(return_value=True)
    monkeypatch.setattr(run_job_executor, "complete_run_with_source_state_promotion", promote)

    await run_job_executor._execute_failed_nodes(db, run, job, None)

    assert run_node.await_count == 1
    assert run_node.await_args.kwargs["node_id"] == "destination"
    assert run_node.await_args.kwargs["input_data"] == {"items": [{"id": "restored"}]}
    promote.assert_awaited_once_with(
        db,
        run_id,
        job_id=job.id,
        lease_owner="worker-1",
    )
