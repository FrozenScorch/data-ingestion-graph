"""Execute durable run jobs from immutable graph-version data."""

from __future__ import annotations

from typing import Any

from app.engine.checkpoint import get_checkpoints, save_checkpoint
from app.engine.executor import DAGExecutor
from app.engine.graph_data import unpack_version_data
from app.engine.runner import run_node_with_retry
from app.engine.scheduler import topological_sort
from app.models.execution import (
    CheckpointType,
    NodeStatus,
    Run,
    RunJob,
    RunJobType,
    RunNode,
    RunStatus,
)
from app.models.graph import Graph, GraphVersion
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession


async def execute_run_job(db: AsyncSession, job: RunJob, ws_manager: Any = None) -> None:
    run = await db.get(Run, job.run_id)
    if run is None:
        raise RuntimeError("Queued run no longer exists")
    if run.status in (RunStatus.CANCELLED.value, RunStatus.COMPLETED.value):
        return
    if job.job_type == RunJobType.RETRY_FAILED.value:
        await _execute_failed_nodes(db, run, ws_manager)
    else:
        await _execute_full_run(db, run, ws_manager)


async def _load_version(
    db: AsyncSession, run: Run
) -> tuple[dict[str, dict[str, Any]], list[dict[str, Any]], dict[str, dict[str, Any]]]:
    if run.graph_version_id is None:
        raise RuntimeError("Run has no immutable graph version")
    version = await db.get(GraphVersion, run.graph_version_id)
    if version is None:
        raise RuntimeError("Run graph version no longer exists")
    if version.graph_id != run.graph_id:
        raise RuntimeError("Run graph version belongs to a different graph")
    nodes, edges = unpack_version_data(version.nodes_data or {}, version.edges_data or [])
    return nodes, edges, version.node_configs or {}


async def _execute_full_run(db: AsyncSession, run: Run, ws_manager: Any) -> None:
    nodes, edges, node_configs = await _load_version(db, run)
    await DAGExecutor(db, ws_manager).execute(run, nodes, edges, node_configs)


async def _execute_failed_nodes(db: AsyncSession, run: Run, ws_manager: Any) -> None:
    nodes, edges, node_configs = await _load_version(db, run)
    checkpoints = await get_checkpoints(db, run.id)
    restored_outputs: dict[str, Any] = {}
    for checkpoint in checkpoints:
        if (
            checkpoint.checkpoint_type == CheckpointType.POST_EXEC.value
            and checkpoint.node_output
            and checkpoint.node_id not in restored_outputs
        ):
            restored_outputs[checkpoint.node_id] = checkpoint.node_output

    node_result = await db.execute(
        select(RunNode).where(RunNode.run_id == run.id).order_by(RunNode.created_at.desc())
    )
    latest_nodes: dict[str, RunNode] = {}
    for run_node in node_result.scalars().all():
        latest_nodes.setdefault(run_node.node_id, run_node)
    failed_node_ids = {
        node_id
        for node_id, run_node in latest_nodes.items()
        if run_node.status == NodeStatus.FAILED.value
    }
    if not failed_node_ids:
        await _execute_full_run(db, run, ws_manager)
        return

    downstream: dict[str, set[str]] = {}
    for edge in edges:
        source = str(edge.get("source", edge.get("source_id", "")))
        target = str(edge.get("target", edge.get("target_id", "")))
        downstream.setdefault(source, set()).add(target)

    nodes_to_reexecute = set(failed_node_ids)
    frontier = list(failed_node_ids)
    while frontier:
        source = frontier.pop()
        for target in downstream.get(source, set()):
            if target and target not in nodes_to_reexecute:
                nodes_to_reexecute.add(target)
                frontier.append(target)

    owner_result = await db.execute(select(Graph.owner_id).where(Graph.id == run.graph_id))
    owner_id = owner_result.scalar_one_or_none()
    if owner_id is None:
        raise RuntimeError("Graph owner not found")
    executor = DAGExecutor(db, ws_manager)
    connections = await executor._resolve_connections(owner_id, node_configs)
    exec_state: dict[str, Any] = {
        "outputs": restored_outputs,
        "connections": connections,
        "owner_id": str(owner_id),
        "graph_id": str(run.graph_id),
    }

    run.status = RunStatus.RUNNING.value
    run.error_message = None
    await db.commit()

    for level_nodes in topological_sort(nodes, edges):
        await db.refresh(run)
        if run.status == RunStatus.CANCELLED.value:
            return
        for node_id in (node for node in level_nodes if node in nodes_to_reexecute):
            node_def = nodes[node_id]
            node_type = node_def.get("type", node_def.get("node_type", "unknown"))
            node_config = node_configs.get(node_id, {})
            inputs, _ = executor._collect_inputs(node_id, edges, exec_state)
            run_node = await run_node_with_retry(
                db=db,
                run_id=run.id,
                node_id=node_id,
                node_type=node_type,
                config=node_config,
                input_data=inputs,
                state=executor._state_for_node(node_id, node_configs, exec_state),
            )
            if run_node.status != NodeStatus.COMPLETED.value:
                run.status = RunStatus.FAILED.value
                run.error_message = f"Retry failed at node {node_id}: {run_node.error_message}"
                await db.commit()
                return
            if run_node.output_data:
                exec_state["outputs"][node_id] = run_node.output_data
            await save_checkpoint(
                db,
                run.id,
                node_id,
                CheckpointType.POST_EXEC.value,
                state_data={"retry": True},
                node_output=run_node.output_data,
            )

    run.status = RunStatus.COMPLETED.value
    await db.commit()
