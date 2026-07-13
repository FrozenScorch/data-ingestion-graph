"""
Node runner: executes a single node and records results.
Includes retry integration via the retry handler.
"""

import logging
import time
from typing import Any
from uuid import UUID

from app.config import settings
from app.engine.retry import RetryConfig, retry_async
from app.engine.state import can_node_transition
from app.models.execution import (
    ExecutionLog,
    LogLevel,
    NodeStatus,
    Run,
    RunNode,
    RunStatus,
)
from app.nodes.base import NodeContext, NodeResult
from app.nodes.registry import get_node as registry_get_node
from app.services.sdk_source_state_service import SDKSourceStateLeaseError
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)

_CONTROL_STATUS_KEY = "_run_control_status"


async def run_node_with_retry(
    db: AsyncSession,
    run_id: UUID,
    node_id: str,
    node_type: str,
    config: dict[str, Any],
    input_data: dict[str, Any],
    state: dict[str, Any],
    working_dir: str | None = None,
    max_retries: int = 3,
    retry_config: RetryConfig | None = None,
    defer_completion_commit: bool = False,
    job_id: UUID | None = None,
    lease_owner: str | None = None,
) -> RunNode:
    """
    Execute a single node with retry support.

    On first failure, if retries are available, uses the retry handler
    from ``app.engine.retry`` to retry with exponential backoff.

    Returns the updated RunNode record. Executors set ``defer_completion_commit``
    so successful output and adapter state commit with the POST_EXEC checkpoint.
    """
    # Create the initial RunNode record
    run_node = RunNode(
        run_id=run_id,
        node_id=node_id,
        node_type=node_type,
        status=NodeStatus.RUNNING.value,
        attempt_count=1,
        max_retries=max_retries,
        input_data=input_data,
    )
    db.add(run_node)
    if defer_completion_commit:
        await db.flush()
    else:
        await db.commit()
    await db.refresh(run_node)

    # Get node implementation
    node_impl = registry_get_node(node_type)
    if not node_impl:
        new_status = NodeStatus.FAILED.value
        if can_node_transition(run_node.status, new_status):
            run_node.status = new_status
        run_node.error_message = f"Unknown node type: {node_type}"
        await db.commit()
        return run_node

    # Build execution context
    context = NodeContext(
        run_id=str(run_id),
        node_id=node_id,
        config=config,
        input_data=input_data,
        state=state,
        working_dir=working_dir or settings.temp_dir,
        db_session=db,
        job_id=str(job_id) if job_id is not None else None,
        lease_owner=lease_owner,
    )

    # Build retry config from parameters
    if retry_config is None:
        retry_config = RetryConfig(
            max_retries=max_retries,
            base_delay_seconds=2.0,
            max_delay_seconds=60.0,
            jitter=True,
        )

    async def _execute_attempt() -> NodeResult:
        """Execute the node once. Raises on failure so retry_async can catch it."""
        control_status = await _stopped_run_status(db, run_id)
        if control_status is not None:
            return NodeResult(success=True, metadata={_CONTROL_STATUS_KEY: control_status})
        try:
            result = await node_impl.execute(context)
        except Exception:
            control_status = await _stopped_run_status(db, run_id)
            if control_status is not None:
                return NodeResult(success=True, metadata={_CONTROL_STATUS_KEY: control_status})
            raise
        control_status = await _stopped_run_status(db, run_id)
        if control_status is not None:
            return NodeResult(success=True, metadata={_CONTROL_STATUS_KEY: control_status})
        return result

    start_time = time.monotonic()

    try:
        result = await retry_async(
            _execute_attempt,
            retry_config=retry_config,
        )
        elapsed_ms = int((time.monotonic() - start_time) * 1000)

        control_status = result.metadata.get(_CONTROL_STATUS_KEY)
        if control_status is not None:
            new_status = NodeStatus.SKIPPED.value
        else:
            new_status = NodeStatus.COMPLETED.value if result.success else NodeStatus.FAILED.value
        if can_node_transition(run_node.status, new_status):
            run_node.status = new_status
        else:
            logger.warning(
                f"Invalid node transition: {run_node.status} -> {new_status} "
                f"for node {node_id}. Skipping status update."
            )
        run_node.output_data = {} if control_status is not None else result.output_data
        run_node.items_processed = 0 if control_status is not None else result.items_processed
        run_node.duration_ms = elapsed_ms
        run_node.error_message = None if control_status is not None else result.error_message
        run_node.attempt_count = (
            retry_config.max_retries
        )  # we don't track per-attempt, but set to max

        # Log execution
        log = ExecutionLog(
            run_id=run_id,
            run_node_id=run_node.id,
            node_id=node_id,
            level=LogLevel.INFO.value,
            message=(
                f"Node {node_type} stopped because run is {control_status}"
                if control_status is not None
                else f"Node {node_type} completed in {elapsed_ms}ms"
            ),
            structured_data={"items_processed": run_node.items_processed},
        )
        db.add(log)

    except SDKSourceStateLeaseError:
        await db.rollback()
        raise
    except Exception as e:
        elapsed_ms = int((time.monotonic() - start_time) * 1000)
        new_status = NodeStatus.FAILED.value
        if can_node_transition(run_node.status, new_status):
            run_node.status = new_status
        else:
            logger.warning(
                f"Invalid node transition: {run_node.status} -> {new_status} "
                f"for node {node_id}. Skipping status update."
            )
        run_node.duration_ms = elapsed_ms
        run_node.error_message = str(e)
        run_node.attempt_count = retry_config.max_retries

        logger.exception(f"Node {node_type} ({node_id}) failed after retries: {e}")

        log = ExecutionLog(
            run_id=run_id,
            run_node_id=run_node.id,
            node_id=node_id,
            level=LogLevel.ERROR.value,
            message=f"Node {node_type} failed after retries: {e}",
        )
        db.add(log)

    if defer_completion_commit:
        await db.flush()
    else:
        await db.commit()
    await db.refresh(run_node)
    return run_node


async def _stopped_run_status(db: AsyncSession, run_id: UUID) -> str | None:
    result = await db.execute(select(Run.status).where(Run.id == run_id))
    status = result.scalar_one_or_none()
    if status in (RunStatus.PAUSED.value, RunStatus.CANCELLED.value):
        return status
    return None


# Keep backward-compatible alias
run_node = run_node_with_retry
