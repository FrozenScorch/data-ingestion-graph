"""
Execution API routes: run creation, management, and retrieval.
"""
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, status, BackgroundTasks
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.session import get_session
from app.middleware.auth import get_current_user
from app.schemas.execution import (
    RunCreate,
    RunResponse,
    RunListResponse,
    RunDetailResponse,
    RunNodeResponse,
    RunControlRequest,
)
from app.services.execution_service import (
    create_run,
    get_run,
    list_runs,
    cancel_run,
    pause_run,
    resume_run,
)
from app.services.graph_service import get_graph, get_graph_versions
from app.engine.executor import DAGExecutor

router = APIRouter(tags=["executions"])


def _unpack_version_data(nodes_data: dict | None, edges_data: dict | list | None) -> tuple[dict, list]:
    """Unpack version data from frontend storage format into executor format.

    Frontend saves: nodes_data={"nodes": [GraphNode, ...]}, edges_data={"edges": [GraphEdge, ...]}
    Executor expects: nodes_data={node_id: node_def}, edges_data=[edge_dict, ...]
    """
    # Unpack nodes: from {"nodes": [...]} to {node_id: node_def, ...}
    if isinstance(nodes_data, dict) and "nodes" in nodes_data:
        nodes_list = nodes_data["nodes"]
        if isinstance(nodes_list, list):
            nodes_data = {str(n.get("id", n.get("node_id"))): n for n in nodes_list}

    # Unpack edges: from {"edges": [...]} to [edge_dict, ...]
    if isinstance(edges_data, dict) and "edges" in edges_data:
        edges_data = edges_data["edges"]

    return nodes_data or {}, edges_data or []


@router.post("/api/graphs/{graph_id}/run", response_model=RunResponse, status_code=status.HTTP_201_CREATED)
async def start_run(
    graph_id: UUID,
    background_tasks: BackgroundTasks,
    request: RunCreate | None = None,
    db: AsyncSession = Depends(get_session),
    current_user: dict = Depends(get_current_user),
):
    """Start a new execution run for a graph."""
    # Verify graph exists and user has access
    graph = await get_graph(db, graph_id)
    if not graph:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Graph not found")
    if current_user["role"] != "admin" and str(graph.owner_id) != str(current_user["user_id"]):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="No permission")

    # Get latest version if not specified
    graph_version_id = request.graph_version_id if request else None
    if not graph_version_id:
        versions = await get_graph_versions(db, graph_id, limit=1)
        if versions:
            graph_version_id = versions[0].id

    trigger_type = request.trigger_type if request else "manual"

    run = await create_run(
        db,
        graph_id=graph_id,
        triggered_by=current_user["user_id"],
        trigger_type=trigger_type,
        graph_version_id=graph_version_id,
    )

    # Execute in background
    async def execute_background():
        from app.db.session import AsyncSessionLocal
        from app.ws.execution_ws import ws_manager
        from app.models.execution import Run

        run_id = run.id  # capture before reassignment to avoid UnboundLocalError

        async with AsyncSessionLocal() as bg_db:
            try:
                # Reload run in background session to avoid detached instance error
                run = await bg_db.get(Run, run_id)
                if not run:
                    return

                if graph_version_id:
                    from app.models.graph import GraphVersion
                    result = await bg_db.execute(
                        __import__("sqlalchemy").select(GraphVersion).where(GraphVersion.id == graph_version_id)
                    )
                    version = result.scalar_one_or_none()
                    raw_nodes = version.nodes_data or {}
                    raw_edges = version.edges_data or []
                    node_configs = version.node_configs or {}
                else:
                    raw_nodes = {}
                    raw_edges = []
                    node_configs = {}

                nodes_data, edges_data = _unpack_version_data(raw_nodes, raw_edges)

                executor = DAGExecutor(bg_db, ws_manager)
                await executor.execute(run, nodes_data, edges_data, node_configs)
            except Exception as e:
                import logging
                logging.getLogger(__name__).exception(f"Background execution failed: {e}")

    background_tasks.add_task(execute_background)
    return run


@router.get("/api/executions", response_model=RunListResponse)
async def list_executions(
    graph_id: UUID | None = Query(None),
    status_filter: str | None = Query(None, alias="status"),
    offset: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=100),
    db: AsyncSession = Depends(get_session),
    current_user: dict = Depends(get_current_user),
):
    """List all execution runs."""
    runs, total = await list_runs(
        db,
        graph_id=graph_id,
        status=status_filter,
        offset=offset,
        limit=limit,
    )
    return RunListResponse(runs=runs, total=total)


@router.get("/api/executions/{run_id}", response_model=RunDetailResponse)
async def get_run_detail(
    run_id: UUID,
    db: AsyncSession = Depends(get_session),
    current_user: dict = Depends(get_current_user),
):
    """Get detailed run information including node statuses."""
    run = await get_run(db, run_id, load_nodes=True)
    if not run:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Run not found")
    await _check_run_access(run, current_user, db)

    run_nodes = [
        RunNodeResponse.model_validate(node) for node in run.run_nodes
    ]
    return RunDetailResponse(
        run=RunResponse.model_validate(run),
        run_nodes=run_nodes,
    )


@router.post("/api/executions/{run_id}/cancel", response_model=RunResponse)
async def cancel_run_endpoint(
    run_id: UUID,
    db: AsyncSession = Depends(get_session),
    current_user: dict = Depends(get_current_user),
):
    """Cancel a running execution."""
    run = await cancel_run(db, run_id)
    if not run:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Run not found")
    return run


@router.post("/api/executions/{run_id}/pause", response_model=RunResponse)
async def pause_run_endpoint(
    run_id: UUID,
    db: AsyncSession = Depends(get_session),
    current_user: dict = Depends(get_current_user),
):
    """Pause a running execution."""
    run = await pause_run(db, run_id)
    if not run:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Run not found")
    return run


@router.post("/api/executions/{run_id}/resume", response_model=RunResponse)
async def resume_run_endpoint(
    run_id: UUID,
    db: AsyncSession = Depends(get_session),
    current_user: dict = Depends(get_current_user),
):
    """Resume a paused execution."""
    run = await resume_run(db, run_id)
    if not run:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Run not found")
    return run


@router.post("/api/executions/{run_id}/retry", response_model=RunResponse)
async def retry_failed_run(
    run_id: UUID,
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_session),
    current_user: dict = Depends(get_current_user),
):
    """
    Retry only the failed nodes of a run, using checkpointed data from
    successful nodes. The original run_id is reused and status is reset.
    """
    run = await get_run(db, run_id)
    if not run:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Run not found")

    if run.status not in ("failed", "cancelled"):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Cannot retry run in '{run.status}' status. Only 'failed' or 'cancelled' runs can be retried.",
        )

    # Load the graph version data
    if run.graph_version_id:
        from app.models.graph import GraphVersion
        result = await db.execute(
            __import__("sqlalchemy").select(GraphVersion).where(GraphVersion.id == run.graph_version_id)
        )
        version = result.scalar_one_or_none()
        if not version:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Graph version not found")
        raw_nodes = version.nodes_data or {}
        raw_edges = version.edges_data or []
        node_configs = version.node_configs or {}
    else:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Run has no graph version, cannot retry",
        )

    nodes_data, edges_data = _unpack_version_data(raw_nodes, raw_edges)

    # Reset run status
    run.status = "pending"
    run.error_message = None
    await db.commit()

    # Execute in background
    async def retry_background():
        from app.db.session import AsyncSessionLocal
        from app.ws.execution_ws import ws_manager
        from app.engine.checkpoint import get_checkpoints
        from app.engine.scheduler import topological_sort
        from app.engine.runner import run_node_with_retry
        from app.models.execution import RunStatus, CheckpointType

        async with AsyncSessionLocal() as bg_db:
            try:
                # Reload run in background session to avoid detached instance error
                run = await bg_db.get(Run, run.id)
                if not run:
                    return

                # Load checkpoints from the original (failed) run to restore
                # successful node outputs
                checkpoints = await get_checkpoints(bg_db, run.id)
                restored_outputs = {}
                for cp in checkpoints:
                    if cp.checkpoint_type == CheckpointType.POST_EXEC.value and cp.node_output:
                        restored_outputs[cp.node_id] = cp.node_output

                # Determine which nodes failed
                from sqlalchemy import select
                from app.models.execution import RunNode, NodeStatus
                failed_nodes_result = await bg_db.execute(
                    select(RunNode).where(
                        RunNode.run_id == run.id,
                        RunNode.status == NodeStatus.FAILED.value,
                    )
                )
                failed_nodes = list(failed_nodes_result.scalars().all())
                failed_node_ids = set(n.node_id for n in failed_nodes)

                # If no failed nodes, just run everything from scratch
                if not failed_node_ids:
                    executor = DAGExecutor(bg_db, ws_manager)
                    await executor.execute(run, nodes_data, edges_data, node_configs)
                    return

                # Compute topological levels
                levels = topological_sort(nodes_data, edges_data)

                # Build exec_state from restored checkpoints
                exec_state: dict = {"outputs": dict(restored_outputs)}

                # Re-execute only failed nodes (and any downstream nodes that
                # depend on them)
                nodes_to_reexecute = set(failed_node_ids)

                # Find all downstream nodes of failed nodes
                for edge in edges_data:
                    source = edge.get("source", edge.get("source_id"))
                    target = edge.get("target", edge.get("target_id"))
                    if source in nodes_to_reexecute:
                        nodes_to_reexecute.add(target)

                run.status = RunStatus.RUNNING.value
                await bg_db.commit()

                # Execute levels, skipping already-completed nodes
                for level_idx, level_nodes in enumerate(levels):
                    nodes_in_level = [n for n in level_nodes if n in nodes_to_reexecute]
                    if not nodes_in_level:
                        continue

                    for node_id in nodes_in_level:
                        node_def = nodes_data[node_id]
                        node_type = node_def.get("type", node_def.get("node_type", "unknown"))
                        node_config = (node_configs or {}).get(node_id, {})

                        # Collect inputs from restored outputs or re-executed outputs
                        inputs = {}
                        for edge in edges_data:
                            target = edge.get("target", edge.get("target_id"))
                            source = edge.get("source", edge.get("source_id"))
                            source_port = edge.get("source_port", "output")
                            target_port = edge.get("target_port", "input")
                            if target == node_id and source in exec_state.get("outputs", {}):
                                source_output = exec_state["outputs"][source]
                                if isinstance(source_output, dict):
                                    inputs[target_port] = source_output.get(source_port, source_output)
                                else:
                                    inputs[target_port] = source_output

                        run_node = await run_node_with_retry(
                            db=bg_db,
                            run_id=run.id,
                            node_id=node_id,
                            node_type=node_type,
                            config=node_config,
                            input_data=inputs,
                            state=exec_state,
                        )

                        if run_node.output_data and run_node.status == "completed":
                            exec_state["outputs"][node_id] = run_node.output_data
                        elif run_node.status == "failed":
                            run.status = RunStatus.FAILED.value
                            run.error_message = f"Retry failed at node {node_id}: {run_node.error_message}"
                            await bg_db.commit()
                            return

                if run.status == RunStatus.RUNNING.value:
                    run.status = RunStatus.COMPLETED.value
                    await bg_db.commit()

            except Exception as e:
                import logging
                logging.getLogger(__name__).exception(f"Retry execution failed: {e}")
                run.status = RunStatus.FAILED.value
                run.error_message = str(e)
                await bg_db.commit()

    background_tasks.add_task(retry_background)
    return run


@router.post("/api/executions/{run_id}/replay", response_model=RunResponse, status_code=status.HTTP_201_CREATED)
async def replay_run(
    run_id: UUID,
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_session),
    current_user: dict = Depends(get_current_user),
):
    """
    Full replay of a run from the beginning.
    Creates a new run with a new run_id, using the same graph version.
    """
    run = await get_run(db, run_id)
    if not run:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Run not found")

    # Load the graph version data
    if run.graph_version_id:
        from app.models.graph import GraphVersion
        result = await db.execute(
            __import__("sqlalchemy").select(GraphVersion).where(GraphVersion.id == run.graph_version_id)
        )
        version = result.scalar_one_or_none()
        if not version:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Graph version not found")
        raw_nodes = version.nodes_data or {}
        raw_edges = version.edges_data or []
        node_configs = version.node_configs or {}
    else:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Run has no graph version, cannot replay",
        )

    nodes_data, edges_data = _unpack_version_data(raw_nodes, raw_edges)

    # Create a new run
    new_run = await create_run(
        db,
        graph_id=run.graph_id,
        triggered_by=current_user["user_id"],
        trigger_type="manual",
        graph_version_id=run.graph_version_id,
    )

    # Execute in background
    async def replay_background():
        from app.db.session import AsyncSessionLocal
        from app.ws.execution_ws import ws_manager

        async with AsyncSessionLocal() as bg_db:
            try:
                # Reload new_run in background session to avoid detached instance error
                from app.models.execution import Run
                new_run = await bg_db.get(Run, new_run.id)
                if not new_run:
                    return

                executor = DAGExecutor(bg_db, ws_manager)
                await executor.execute(new_run, nodes_data, edges_data, node_configs)
            except Exception as e:
                import logging
                logging.getLogger(__name__).exception(f"Replay execution failed: {e}")

    background_tasks.add_task(replay_background)
    return new_run
