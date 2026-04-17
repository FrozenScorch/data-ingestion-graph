"""
Node runner: executes a single node and records results.
Includes retry integration via the retry handler.

Nodes return NodeResult(success=False) for expected failures.
The runner checks result.success and retries on failure.
Raised exceptions are also caught and trigger retries.
"""
import time
import logging
from typing import Any
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from app.models.execution import RunNode, NodeStatus, ExecutionLog, LogLevel
from app.nodes.base import BaseNode, NodeContext, NodeResult
from app.nodes.registry import get_node as registry_get_node
from app.engine.retry import RetryConfig, retry_async
from app.engine.state import can_node_transition

logger = logging.getLogger(__name__)


class _NodeFailedError(Exception):
    """Wrapper so retry_async can catch NodeResult(success=False) as a raised exception."""
    def __init__(self, result: NodeResult):
        self.result = result
        super().__init__(result.error_message or "Node returned success=False")


async def run_node_with_retry(
    db: AsyncSession,
    run_id: UUID,
    node_id: str,
    node_type: str,
    config: dict[str, Any],
    input_data: dict[str, Any],
    state: dict[str, Any],
    working_dir: str = "./data/temp",
    max_retries: int = 3,
    retry_config: RetryConfig | None = None,
) -> RunNode:
    """
    Execute a single node with retry support.

    On failure (either raised exception or NodeResult(success=False)),
    retries with exponential backoff up to max_retries times.

    Returns the updated RunNode record.
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
        working_dir=working_dir,
    )

    # Build retry config from parameters
    if retry_config is None:
        retry_config = RetryConfig(
            max_retries=max_retries,
            base_delay_seconds=2.0,
            max_delay_seconds=60.0,
            jitter=True,
        )

    actual_attempts = 0

    async def _execute_attempt() -> NodeResult:
        """
        Execute the node once.
        If the node returns NodeResult(success=False), raise _NodeFailedError
        so retry_async can catch it and retry.
        """
        nonlocal actual_attempts
        actual_attempts += 1
        result = await node_impl.execute(context)
        if not result.success:
            raise _NodeFailedError(result)
        return result

    start_time = time.monotonic()

    try:
        result = await retry_async(
            _execute_attempt,
            retry_config=retry_config,
        )
        elapsed_ms = int((time.monotonic() - start_time) * 1000)

        new_status = NodeStatus.COMPLETED.value if result.success else NodeStatus.FAILED.value
        if can_node_transition(run_node.status, new_status):
            run_node.status = new_status
        else:
            logger.warning(
                f"Invalid node transition: {run_node.status} -> {new_status} "
                f"for node {node_id}. Skipping status update."
            )
        run_node.output_data = result.output_data
        run_node.items_processed = result.items_processed
        run_node.duration_ms = elapsed_ms
        run_node.error_message = result.error_message
        run_node.attempt_count = actual_attempts

        # Log execution
        log = ExecutionLog(
            run_id=run_id,
            run_node_id=run_node.id,
            node_id=node_id,
            level=LogLevel.INFO.value,
            message=f"Node {node_type} completed in {elapsed_ms}ms (attempts: {actual_attempts})",
            structured_data={"items_processed": result.items_processed},
        )
        db.add(log)

    except _NodeFailedError as e:
        # Node returned success=False and exhausted retries
        elapsed_ms = int((time.monotonic() - start_time) * 1000)
        new_status = NodeStatus.FAILED.value
        if can_node_transition(run_node.status, new_status):
            run_node.status = new_status
        run_node.duration_ms = elapsed_ms
        run_node.error_message = e.result.error_message or "Node returned failure"
        run_node.output_data = e.result.output_data
        run_node.items_processed = e.result.items_processed
        run_node.attempt_count = actual_attempts

        logger.warning(f"Node {node_type} ({node_id}) failed after {actual_attempts} attempts: {e}")

        log = ExecutionLog(
            run_id=run_id,
            run_node_id=run_node.id,
            node_id=node_id,
            level=LogLevel.ERROR.value,
            message=f"Node {node_type} failed after {actual_attempts} attempts: {e}",
        )
        db.add(log)

    except Exception as e:
        # Unexpected exception (retries exhausted)
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
        run_node.attempt_count = actual_attempts

        logger.exception(f"Node {node_type} ({node_id}) failed after {actual_attempts} retries: {e}")

        log = ExecutionLog(
            run_id=run_id,
            run_node_id=run_node.id,
            node_id=node_id,
            level=LogLevel.ERROR.value,
            message=f"Node {node_type} failed after {actual_attempts} retries: {e}",
        )
        db.add(log)

    await db.commit()
    await db.refresh(run_node)
    return run_node


# Keep backward-compatible alias
run_node = run_node_with_retry
