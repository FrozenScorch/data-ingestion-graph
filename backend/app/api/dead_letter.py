"""
Dead Letter Queue API routes: list, retry, resolve, and delete DLQ items.
"""

import uuid
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, Field
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.session import get_session
from app.middleware.auth import get_current_user
from app.models.dead_letter import DeadLetterQueue
from app.services.graph_service import get_graph


async def _check_dlq_item_access(item, current_user, db):
    if not item.run_id:
        return
    from app.models.execution import Run
    from sqlalchemy import select as _s

    result = await db.execute(_s(Run).where(Run.id == item.run_id))
    run = result.scalar_one_or_none()
    if not run:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Parent run not found")
    graph = await get_graph(db, run.graph_id)
    if not graph:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Parent graph not found")
    if current_user["role"] != "admin" and str(graph.owner_id) != str(current_user["user_id"]):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="No permission")


router = APIRouter(prefix="/api/dead-letter", tags=["dead-letter"])


class DLQResolveRequest(BaseModel):
    """Request body for resolving a DLQ item."""

    note: str = Field(..., min_length=1, description="Resolution note")


@router.get("")
async def list_dlq_items(
    resolved: Optional[bool] = None,
    node_type: Optional[str] = None,
    offset: int = 0,
    limit: int = 50,
    db: AsyncSession = Depends(get_session),
    current_user: dict = Depends(get_current_user),
):
    """List DLQ items with optional filtering and pagination."""
    query = select(DeadLetterQueue)
    count_query = select(func.count()).select_from(DeadLetterQueue)

    if resolved is not None:
        query = query.where(DeadLetterQueue.resolved == resolved)
        count_query = count_query.where(DeadLetterQueue.resolved == resolved)
    if node_type:
        query = query.where(DeadLetterQueue.node_type == node_type)
        count_query = count_query.where(DeadLetterQueue.node_type == node_type)

    query = query.order_by(DeadLetterQueue.created_at.desc()).offset(offset).limit(limit)

    total_result = await db.execute(count_query)
    total = total_result.scalar() or 0

    result = await db.execute(query)
    items = list(result.scalars().all())

    return {
        "items": [
            {
                "id": str(item.id),
                "run_id": str(item.run_id) if item.run_id else None,
                "node_id": item.node_id,
                "node_type": item.node_type,
                "error_type": item.error_type,
                "error_message": item.error_message,
                "input_data": item.input_data,
                "retry_count": item.retry_count,
                "resolved": item.resolved,
                "resolution_note": item.resolution_note,
                "created_at": item.created_at.isoformat() if item.created_at else None,
                "updated_at": item.updated_at.isoformat() if item.updated_at else None,
            }
            for item in items
        ],
        "total": total,
        "offset": offset,
        "limit": limit,
    }


@router.post("/{item_id}/retry")
async def retry_dlq_item(
    item_id: uuid.UUID,
    db: AsyncSession = Depends(get_session),
    current_user: dict = Depends(get_current_user),
):
    """
    Retry a failed DLQ item by re-executing the node with its original input data.

    This creates a new attempt using the stored input_data and node_type.
    """
    result = await db.execute(select(DeadLetterQueue).where(DeadLetterQueue.id == item_id))
    item = result.scalar_one_or_none()

    if not item:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="DLQ item not found",
        )

    await _check_dlq_item_access(item, current_user, db)

    if item.resolved:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Cannot retry a resolved DLQ item",
        )

    if not item.input_data:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="No input data available for retry",
        )

    # Attempt to re-execute the node through the retry infrastructure
    try:
        from app.engine.runner import run_node_with_retry
        from app.models.execution import Run
        from app.models.graph import Graph
        from sqlalchemy import select as _s

        # Retrieve parent run for proper retry context
        run_result = await db.execute(_s(Run).where(Run.id == item.run_id))
        run = run_result.scalar_one_or_none()
        if not run:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Parent run not found",
            )

        # Execute through the full retry infrastructure
        run_node = await run_node_with_retry(
            db=db,
            run_id=run.id,
            node_id=item.node_id or "dlq-retry-node",
            node_type=item.node_type,
            config={},
            input_data=item.input_data,
            state={"owner_id": str((await db.execute(
                _s(Graph.owner_id).where(Graph.id == run.graph_id)
            )).scalar_one())},
            max_retries=1,
        )

        node_success = run_node.status == "completed"

        if node_success:
            # Mark as resolved on successful retry
            item.resolved = True
            item.resolution_note = f"Retry succeeded. Items processed: {run_node.items_processed}"
            await db.commit()
            await db.refresh(item)

            return {
                "success": True,
                "message": "Retry succeeded",
                "items_processed": run_node.items_processed,
                "item": {
                    "id": str(item.id),
                    "resolved": item.resolved,
                    "resolution_note": item.resolution_note,
                },
            }
        else:
            # Increment retry count but leave unresolved
            item.retry_count += 1
            item.error_message = run_node.error_message
            await db.commit()
            await db.refresh(item)

            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
                detail=f"Retry failed: {run_node.error_message}",
            )

    except HTTPException:
        raise
    except Exception as e:
        item.retry_count += 1
        await db.commit()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Retry error: {str(e)}",
        )


@router.post("/{item_id}/resolve")
async def resolve_dlq_item(
    item_id: uuid.UUID,
    request: DLQResolveRequest,
    db: AsyncSession = Depends(get_session),
    current_user: dict = Depends(get_current_user),
):
    """Mark a DLQ item as resolved with a note."""
    result = await db.execute(select(DeadLetterQueue).where(DeadLetterQueue.id == item_id))
    item = result.scalar_one_or_none()

    if not item:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="DLQ item not found",
        )

    await _check_dlq_item_access(item, current_user, db)

    if item.resolved:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Item is already resolved",
        )

    item.resolved = True
    item.resolution_note = request.note
    await db.commit()
    await db.refresh(item)

    return {
        "id": str(item.id),
        "resolved": item.resolved,
        "resolution_note": item.resolution_note,
        "updated_at": item.updated_at.isoformat() if item.updated_at else None,
    }


@router.delete("/{item_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_dlq_item(
    item_id: uuid.UUID,
    db: AsyncSession = Depends(get_session),
    current_user: dict = Depends(get_current_user),
):
    """Delete a DLQ item."""
    result = await db.execute(select(DeadLetterQueue).where(DeadLetterQueue.id == item_id))
    item = result.scalar_one_or_none()

    if not item:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="DLQ item not found",
        )

    await _check_dlq_item_access(item, current_user, db)

    await db.delete(item)
    await db.commit()
