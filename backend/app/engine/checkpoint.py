"""
Checkpoint manager: save and restore execution state in PostgreSQL.
"""

from typing import Any, Optional
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.execution import Checkpoint, CheckpointType


async def save_checkpoint(
    db: AsyncSession,
    run_id: UUID,
    node_id: str,
    checkpoint_type: str,
    state_data: Optional[dict] = None,
    node_output: Optional[dict] = None,
) -> Checkpoint:
    """Save a checkpoint for a node execution."""
    checkpoint = Checkpoint(
        run_id=run_id,
        node_id=node_id,
        checkpoint_type=checkpoint_type,
        state_data=state_data,
        node_output=node_output,
    )
    db.add(checkpoint)
    try:
        await db.commit()
    except Exception:
        # The same transaction may contain deferred node output and source state.
        # A failed checkpoint must roll that entire acknowledgement unit back.
        await db.rollback()
        raise
    await db.refresh(checkpoint)
    return checkpoint


async def get_checkpoints(
    db: AsyncSession,
    run_id: UUID,
    node_id: Optional[str] = None,
) -> list[Checkpoint]:
    """Get checkpoints for a run, optionally filtered by node_id."""
    query = select(Checkpoint).where(Checkpoint.run_id == run_id)
    if node_id:
        query = query.where(Checkpoint.node_id == node_id)
    query = query.order_by(Checkpoint.created_at.desc())
    result = await db.execute(query)
    return list(result.scalars().all())


async def get_latest_checkpoint(
    db: AsyncSession,
    run_id: UUID,
    node_id: str,
    checkpoint_type: str = CheckpointType.POST_EXEC.value,
) -> Optional[Checkpoint]:
    """Get the most recent checkpoint for a specific node."""
    query = (
        select(Checkpoint)
        .where(Checkpoint.run_id == run_id)
        .where(Checkpoint.node_id == node_id)
        .where(Checkpoint.checkpoint_type == checkpoint_type)
        .order_by(Checkpoint.created_at.desc())
        .limit(1)
    )
    result = await db.execute(query)
    return result.scalar_one_or_none()
