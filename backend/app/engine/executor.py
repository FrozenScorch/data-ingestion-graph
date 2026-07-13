"""
DAG Executor: main orchestrator for graph execution.
Coordinates topological sort, parallel node execution, checkpointing, and error handling.
"""

import asyncio
import logging
import sys
from typing import Any
from uuid import UUID

from app.db.session import AsyncSessionLocal
from app.engine.checkpoint import save_checkpoint
from app.engine.runner import run_node_with_retry
from app.engine.scheduler import topological_sort, validate_dag
from app.engine.state import can_transition
from app.models.execution import CheckpointType, Run, RunStatus
from app.models.graph import Connection, Graph
from app.services.connection_crypto import decrypt_connection_config
from app.services.execution_service import fail_run_if_running
from app.services.sdk_source_state_service import (
    SDKSourceStateLeaseError,
    complete_run_with_source_state_promotion,
    revalidate_source_state_staging_lease,
)
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)

# Minimum number of nodes in a level to trigger parallel execution.
# Small groups execute sequentially to avoid asyncio overhead.
PARALLEL_THRESHOLD = 2


class DAGExecutor:
    """
    Executes a graph DAG by:
    1. Validating the DAG (no cycles)
    2. Computing topological sort with parallel grouping
    3. Executing each level sequentially, nodes within a level in parallel
       (fallback to sequential for levels below PARALLEL_THRESHOLD)
    4. Saving checkpoints after each level
    5. Tracking lineage and costs
    """

    def __init__(
        self,
        db: AsyncSession,
        ws_manager=None,
        *,
        completion_job_id: UUID | None = None,
        completion_lease_owner: str | None = None,
    ):
        self.db = db
        self.ws_manager = ws_manager
        self.completion_job_id = completion_job_id
        self.completion_lease_owner = completion_lease_owner

    async def execute(
        self,
        run: Run,
        nodes_data: dict[str, dict],
        edges_data: list[dict],
        node_configs: dict[str, dict] | None = None,
    ) -> Run:
        """
        Execute a graph run.

        Args:
            run: The Run record
            nodes_data: Dict of node_id -> node definition
            edges_data: List of edge definitions
            node_configs: Dict of node_id -> configuration overrides

        Returns:
            Updated Run record with final status.
        """
        # Force-refresh the identity-mapped run under lock before acting on it.
        # A cancellation, pause, or completion committed by another session wins.
        run_result = await self.db.execute(
            select(Run)
            .where(Run.id == run.id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
        locked_run = run_result.scalar_one_or_none()
        if locked_run is None:
            await self.db.rollback()
            raise RuntimeError("Run no longer exists")
        run = locked_run
        if run.status == RunStatus.PENDING.value:
            run.status = RunStatus.RUNNING.value
        if run.status != RunStatus.RUNNING.value:
            await self.db.commit()
            return run
        await self.db.commit()

        # Validate DAG
        errors = validate_dag(nodes_data, edges_data)
        if errors:
            return await self._fail_run(
                run,
                f"Invalid DAG: {'; '.join(errors)}",
                {"errors": errors},
            )

        await self._emit_event(run.id, "run_started", {})

        # Compute execution levels (topological sort)
        try:
            levels = topological_sort(nodes_data, edges_data)
        except ValueError as e:
            return await self._fail_run(run, str(e), {"error": str(e)})

        # Resolve only graph-owner connections referenced by this immutable graph
        # version. Credentials remain server-side and are never copied into node configs.
        try:
            owner_id = await self._graph_owner(run.graph_id)
            connections = await self._resolve_connections(owner_id, node_configs or {})
        except ValueError:
            error = "Invalid or unauthorized graph resource reference"
            return await self._fail_run(run, error, {"error": error})

        # Shared orchestration state. Credentials are scoped into a per-node copy
        # immediately before execution and are never exposed to unrelated nodes.
        exec_state: dict[str, Any] = {
            "outputs": {},
            "connections": connections,
            "owner_id": str(owner_id),
            "graph_id": str(run.graph_id),
        }

        # Execute each level
        for level_idx, level_nodes in enumerate(levels):
            # Check if run is still running (could have been cancelled)
            await self.db.refresh(run)
            if run.status in (RunStatus.PAUSED.value, RunStatus.CANCELLED.value):
                return run

            logger.info(f"Executing level {level_idx} with {len(level_nodes)} nodes")

            if len(level_nodes) >= PARALLEL_THRESHOLD:
                # Parallel execution using asyncio.gather
                results = await self._execute_level_parallel(
                    run,
                    level_idx,
                    level_nodes,
                    nodes_data,
                    edges_data,
                    node_configs,
                    exec_state,
                )
                await self.db.refresh(run)
                if run.status in (RunStatus.PAUSED.value, RunStatus.CANCELLED.value):
                    return run
                # Check for failures in parallel results
                for node_id, run_node in results.items():
                    if run_node.status == "failed":
                        node_type = nodes_data[node_id].get("type", "unknown")
                        error = (
                            f"Node {node_id} ({node_type}) failed: {run_node.error_message}"
                        )
                        return await self._fail_run(
                            run,
                            error,
                            {"node_id": node_id, "error": run_node.error_message},
                        )
            else:
                # Sequential fallback for small groups
                result = await self._execute_level_sequential(
                    run,
                    level_idx,
                    level_nodes,
                    nodes_data,
                    edges_data,
                    node_configs,
                    exec_state,
                )
                if result is not None:
                    return result  # A failure occurred

        await self.db.refresh(run)
        # Mark run as completed if no failures (with state machine validation)
        if run.status == RunStatus.RUNNING.value and can_transition(
            run.status, RunStatus.COMPLETED.value
        ):
            completed = await complete_run_with_source_state_promotion(
                self.db,
                run.id,
                job_id=self.completion_job_id,
                lease_owner=self.completion_lease_owner,
            )
            if completed:
                await self._emit_event(run.id, "run_completed", {})

        return run

    async def _fail_run(
        self,
        run: Run,
        error_message: str,
        event_data: dict[str, Any],
    ) -> Run:
        current_run, transitioned = await fail_run_if_running(
            self.db,
            run.id,
            error_message,
            job_id=self.completion_job_id,
            lease_owner=self.completion_lease_owner,
        )
        resolved_run = current_run or run
        if transitioned:
            await self._emit_event(resolved_run.id, "run_failed", event_data)
        return resolved_run

    async def _graph_owner(self, graph_id: UUID) -> UUID:
        result = await self.db.execute(select(Graph.owner_id).where(Graph.id == graph_id))
        owner_id = result.scalar_one_or_none()
        if owner_id is None:
            raise ValueError("Graph owner not found")
        return owner_id

    async def _resolve_connections(
        self, owner_id: UUID, node_configs: dict[str, dict]
    ) -> dict[str, dict[str, Any]]:
        raw_ids = {
            str(config["connection_id"])
            for config in node_configs.values()
            if isinstance(config, dict) and config.get("connection_id")
        }
        if not raw_ids:
            return {}
        try:
            connection_ids = {UUID(value) for value in raw_ids}
        except ValueError as exc:
            raise ValueError("Invalid connection reference") from exc

        result = await self.db.execute(
            select(Connection).where(
                Connection.id.in_(connection_ids),
                Connection.user_id == owner_id,
            )
        )
        connections = list(result.scalars().all())
        if {connection.id for connection in connections} != connection_ids:
            raise ValueError("Connection does not belong to graph owner")
        return {
            str(connection.id): decrypt_connection_config(connection.config)
            for connection in connections
        }

    @staticmethod
    def _state_for_node(
        node_id: str,
        node_configs: dict[str, dict] | None,
        exec_state: dict[str, Any],
    ) -> dict[str, Any]:
        """Return execution state containing only this node's saved credential."""
        node_config = (node_configs or {}).get(node_id, {})
        raw_connection_id = node_config.get("connection_id")
        connection_id = str(raw_connection_id) if raw_connection_id else None
        connections = exec_state.get("connections", {})
        scoped_connections = {}
        if connection_id and connection_id in connections:
            scoped_connections[str(connection_id)] = connections[connection_id]
        return {
            "connections": scoped_connections,
            "owner_id": exec_state.get("owner_id"),
            "graph_id": exec_state.get("graph_id"),
        }

    async def _execute_level_parallel(
        self,
        run: Run,
        level_idx: int,
        level_nodes: list[str],
        nodes_data: dict[str, dict],
        edges_data: list[dict],
        node_configs: dict[str, dict] | None,
        exec_state: dict[str, Any],
    ) -> dict[str, Any]:
        """
        Execute all nodes in a level in parallel using asyncio.gather.
        Each coroutine creates its own DB session to avoid sharing AsyncSession
        across concurrent tasks.

        Returns a dict of node_id -> RunNode. Failed nodes will have
        status='failed'.
        """

        async def _run_single(node_id: str) -> tuple[str, Any]:
            """Execute a single node with its own session and return (node_id, RunNode)."""
            async with AsyncSessionLocal() as node_db:
                try:
                    # Reload run in this session to avoid detached instance error
                    from app.models.execution import Run

                    node_run = await node_db.get(Run, run.id)
                    if node_run and node_run.status in (
                        RunStatus.PAUSED.value,
                        RunStatus.CANCELLED.value,
                    ):
                        from app.models.execution import NodeStatus
                        from app.models.execution import RunNode as RN

                        return node_id, RN(
                            run_id=run.id,
                            node_id=node_id,
                            status=NodeStatus.SKIPPED.value,
                        )

                    node_def = nodes_data[node_id]
                    node_type = node_def.get("type", node_def.get("node_type", "unknown"))
                    node_config = (node_configs or {}).get(node_id, {})

                    # Collect input data from predecessor outputs
                    input_data, lineage_edges = self._collect_inputs(
                        node_id, edges_data, exec_state
                    )

                    await self._emit_event(
                        run.id,
                        "node_started",
                        {
                            "node_id": node_id,
                            "node_type": node_type,
                        },
                    )

                    # Run the node with retry support (uses its own session)
                    run_node = await run_node_with_retry(
                        db=node_db,
                        run_id=run.id,
                        node_id=node_id,
                        node_type=node_type,
                        config=node_config,
                        input_data=input_data,
                        state=self._state_for_node(node_id, node_configs, exec_state),
                        defer_completion_commit=True,
                        job_id=self.completion_job_id,
                        lease_owner=self.completion_lease_owner,
                    )
                    if run_node.status == "skipped":
                        return node_id, run_node

                    # Store output in execution state (only on success)
                    if run_node.output_data and run_node.status == "completed":
                        exec_state["outputs"][node_id] = run_node.output_data

                        # Record lineage for each edge that provided data
                        for edge_info in lineage_edges:
                            await self._record_lineage(
                                db=node_db,
                                run_id=run.id,
                                source_node_id=edge_info["source_node_id"],
                                target_node_id=edge_info["target_node_id"],
                                source_port=edge_info["source_port"],
                                target_port=edge_info["target_port"],
                                data=edge_info["data"],
                            )

                    # Save post-execution checkpoint
                    if node_type == "sdk_document_source":
                        await revalidate_source_state_staging_lease(
                            node_db,
                            run.id,
                            job_id=self.completion_job_id,
                            lease_owner=self.completion_lease_owner,
                        )
                    await save_checkpoint(
                        db=node_db,
                        run_id=run.id,
                        node_id=node_id,
                        checkpoint_type=CheckpointType.POST_EXEC.value,
                        state_data={"level": level_idx},
                        node_output=run_node.output_data,
                    )

                    # Emit completion event
                    if run_node.status == "completed":
                        await self._emit_event(
                            run.id,
                            "node_completed",
                            {
                                "node_id": node_id,
                                "node_type": node_type,
                                "items_processed": run_node.items_processed,
                                "duration_ms": run_node.duration_ms,
                                "output_data": run_node.output_data,
                            },
                        )
                    else:
                        # Record in DLQ
                        error_type = "ExecutionError"
                        await self._record_dlq(
                            db=node_db,
                            run_id=run.id,
                            node_id=node_id,
                            node_type=node_type,
                            error_type=error_type,
                            error_message=run_node.error_message,
                            input_data=input_data,
                            retry_count=run_node.attempt_count,
                        )
                        await self._emit_event(
                            run.id,
                            "node_failed",
                            {
                                "node_id": node_id,
                                "node_type": node_type,
                                "error": run_node.error_message,
                            },
                        )

                    return node_id, run_node
                except SDKSourceStateLeaseError:
                    raise
                except Exception:
                    logger.exception(f"Unexpected error in parallel node {node_id}")
                    from app.models.execution import NodeStatus
                    from app.models.execution import RunNode as RN

                    return node_id, RN(
                        run_id=run.id,
                        node_id=node_id,
                        status=NodeStatus.FAILED.value,
                        error_message="Unexpected error during parallel execution",
                    )

        # Run all nodes in parallel
        results_list = await asyncio.gather(
            *[_run_single(node_id) for node_id in level_nodes],
            return_exceptions=False,
        )

        return dict(results_list)

    async def _execute_level_sequential(
        self,
        run: Run,
        level_idx: int,
        level_nodes: list[str],
        nodes_data: dict[str, dict],
        edges_data: list[dict],
        node_configs: dict[str, dict] | None,
        exec_state: dict[str, Any],
    ) -> Run | None:
        """
        Execute nodes in a level sequentially (fallback for small groups).

        Returns None on success, or the Run on failure (so the caller can
        return it immediately).
        """
        for node_id in level_nodes:
            await self.db.refresh(run)
            if run.status in (RunStatus.PAUSED.value, RunStatus.CANCELLED.value):
                return run

            node_def = nodes_data[node_id]
            node_type = node_def.get("type", node_def.get("node_type", "unknown"))
            node_config = (node_configs or {}).get(node_id, {})

            # Collect input data from predecessor outputs
            input_data, lineage_edges = self._collect_inputs(node_id, edges_data, exec_state)

            await self._emit_event(
                run.id,
                "node_started",
                {
                    "node_id": node_id,
                    "node_type": node_type,
                },
            )

            # Run the node with retry support (uses shared session for sequential)
            run_node = await run_node_with_retry(
                db=self.db,
                run_id=run.id,
                node_id=node_id,
                node_type=node_type,
                config=node_config,
                input_data=input_data,
                state=self._state_for_node(node_id, node_configs, exec_state),
                defer_completion_commit=True,
                job_id=self.completion_job_id,
                lease_owner=self.completion_lease_owner,
            )
            if run_node.status == "skipped":
                await self.db.refresh(run)
                return run

            # Store output in execution state
            if run_node.output_data:
                exec_state["outputs"][node_id] = run_node.output_data

            # Record lineage on success
            if run_node.output_data and run_node.status == "completed":
                for edge_info in lineage_edges:
                    await self._record_lineage(
                        run_id=run.id,
                        source_node_id=edge_info["source_node_id"],
                        target_node_id=edge_info["target_node_id"],
                        source_port=edge_info["source_port"],
                        target_port=edge_info["target_port"],
                        data=edge_info["data"],
                    )

            # Save post-execution checkpoint
            if node_type == "sdk_document_source":
                await revalidate_source_state_staging_lease(
                    self.db,
                    run.id,
                    job_id=self.completion_job_id,
                    lease_owner=self.completion_lease_owner,
                )
            await save_checkpoint(
                db=self.db,
                run_id=run.id,
                node_id=node_id,
                checkpoint_type=CheckpointType.POST_EXEC.value,
                state_data={"level": level_idx},
                node_output=run_node.output_data,
            )

            # Emit completion event
            if run_node.status == "completed":
                await self._emit_event(
                    run.id,
                    "node_completed",
                    {
                        "node_id": node_id,
                        "node_type": node_type,
                        "items_processed": run_node.items_processed,
                        "duration_ms": run_node.duration_ms,
                    },
                )
            else:
                # Record in DLQ
                error_type = "ExecutionError"
                await self._record_dlq(
                    run_id=run.id,
                    node_id=node_id,
                    node_type=node_type,
                    error_type=error_type,
                    error_message=run_node.error_message,
                    input_data=input_data,
                    retry_count=run_node.attempt_count,
                )
                await self._emit_event(
                    run.id,
                    "node_failed",
                    {
                        "node_id": node_id,
                        "node_type": node_type,
                        "error": run_node.error_message,
                    },
                )
                # Fail the entire run on node failure (with state machine validation)
                error = f"Node {node_id} ({node_type}) failed: {run_node.error_message}"
                return await self._fail_run(
                    run,
                    error,
                    {"node_id": node_id, "error": run_node.error_message},
                )

        return None

    def _collect_inputs(
        self,
        node_id: str,
        edges: list[dict],
        exec_state: dict[str, Any],
    ) -> tuple[dict[str, Any], list[dict]]:
        """
        Collect input data for a node from its predecessor outputs.

        Returns:
            Tuple of (inputs dict, list of edge info dicts for lineage recording).
            Each edge info dict has: source_node_id, target_node_id, source_port,
            target_port, data (the actual data that was passed).
        """
        inputs: dict[str, Any] = {}
        lineage_edges: list[dict] = []
        for edge in edges:
            target = edge.get("target", edge.get("target_id"))
            source = edge.get("source", edge.get("source_id"))
            source_port = edge.get("source_port", "output")
            target_port = edge.get("target_port", "input")

            if target == node_id and source in exec_state.get("outputs", {}):
                source_output = exec_state["outputs"][source]
                if isinstance(source_output, dict):
                    data = source_output.get(source_port, source_output)
                else:
                    data = source_output
                inputs[target_port] = data
                lineage_edges.append(
                    {
                        "source_node_id": source,
                        "target_node_id": node_id,
                        "source_port": source_port,
                        "target_port": target_port,
                        "data": data,
                    }
                )
        return inputs, lineage_edges

    async def _record_lineage(
        self,
        run_id: UUID,
        source_node_id: str,
        target_node_id: str,
        source_port: str,
        target_port: str,
        data: Any,
        db: AsyncSession | None = None,
    ) -> None:
        """Record a lineage entry for data flowing from source to target node."""
        try:
            import json

            from app.models.lineage import DataLineage

            session = db or self.db

            # Calculate approximate metrics
            items_count = None
            items_sample: Any = None
            bytes_transferred = None

            if isinstance(data, (list, tuple)):
                items_count = len(data)
                items_sample = data[:3] if data else None
                bytes_transferred = len(json.dumps(data[:3], default=str))
            elif isinstance(data, dict):
                items_count = len(data)
                items_sample = dict(list(data.items())[:3])
                bytes_transferred = len(json.dumps(items_sample, default=str))
            elif data is not None:
                items_count = 1
                bytes_transferred = sys.getsizeof(data)

            lineage_entry = DataLineage(
                run_id=run_id,
                source_node_id=source_node_id,
                target_node_id=target_node_id,
                source_port=source_port,
                target_port=target_port,
                items_count=items_count,
                items_sample=items_sample,
                bytes_transferred=bytes_transferred,
            )
            session.add(lineage_entry)
            await session.commit()
        except Exception as e:
            logger.warning(f"Failed to record lineage: {e}")

    async def _record_dlq(
        self,
        run_id: UUID,
        node_id: str,
        node_type: str,
        error_type: str,
        error_message: str,
        input_data: dict | None,
        retry_count: int,
        db: AsyncSession | None = None,
    ) -> None:
        """Record a failed node in the dead letter queue."""
        try:
            from app.engine.dead_letter import add_to_dlq

            session = db or self.db
            await add_to_dlq(
                db=session,
                run_id=run_id,
                node_id=node_id,
                node_type=node_type,
                error_type=error_type,
                error_message=error_message,
                input_data=input_data,
                retry_count=retry_count,
            )
        except Exception as e:
            logger.warning(f"Failed to record DLQ entry: {e}")

    async def _emit_event(self, run_id: UUID, event_type: str, data: dict) -> None:
        """Emit a WebSocket event if manager is available."""
        if self.ws_manager:
            await self.ws_manager.broadcast(run_id, event_type, data)
