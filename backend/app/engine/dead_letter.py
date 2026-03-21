"""
Dead letter queue engine handler.
Routes unresolvable failures to the DLQ.
"""
import logging
from typing import Any
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from app.models.dead_letter import DeadLetterQueue

logger = logging.getLogger(__name__)


async def add_to_dlq(
    db: AsyncSession,
    run_id: UUID | None = None,
    node_id: str | None = None,
    node_type: str | None = None,
    error_type: str | None = None,
    error_message: str | None = None,
    input_data: dict | None = None,
    retry_count: int = 0,
) -> DeadLetterQueue:
    """Add a failed item to the dead letter queue."""
    dlq_entry = DeadLetterQueue(
        run_id=run_id,
        node_id=node_id,
        node_type=node_type,
        error_type=error_type,
        error_message=error_message,
        input_data=input_data,
        retry_count=retry_count,
        resolved=False,
    )
    db.add(dlq_entry)
    await db.commit()
    await db.refresh(dlq_entry)

    logger.warning(
        f"Added to DLQ: run={run_id}, node={node_id}, type={node_type}, "
        f"error={error_type}: {error_message}"
    )
    return dlq_entry
