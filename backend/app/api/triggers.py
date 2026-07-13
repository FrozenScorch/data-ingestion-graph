"""Owner-scoped trigger management and public signed-webhook ingress."""

from typing import Annotated
from uuid import UUID

from app.db.session import get_session
from app.middleware.auth import get_current_user
from app.schemas.trigger import (
    TriggerCreate,
    TriggerCreateResponse,
    TriggerResponse,
    TriggerUpdate,
    WebhookAcceptedResponse,
    WebhookSecretResponse,
)
from app.services.trigger_service import (
    TriggerConflictError,
    create_trigger,
    delete_trigger,
    get_accessible_graph,
    get_accessible_trigger,
    list_triggers,
    rotate_webhook_secret,
    update_trigger,
)
from app.services.webhook_service import (
    accept_webhook_delivery,
    parse_delivery_id,
    parse_signature,
    parse_timestamp,
    read_limited_body,
)
from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response, status
from sqlalchemy.ext.asyncio import AsyncSession

router = APIRouter(tags=["triggers"])
SessionDependency = Annotated[AsyncSession, Depends(get_session)]
UserDependency = Annotated[dict, Depends(get_current_user)]


def _is_admin(current_user: dict) -> bool:
    return current_user.get("role") == "admin"


async def _graph_or_404(
    db: AsyncSession,
    graph_id: UUID,
    current_user: dict,
):
    graph = await get_accessible_graph(
        db,
        graph_id,
        user_id=current_user["user_id"],
        is_admin=_is_admin(current_user),
    )
    if graph is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Graph not found")
    return graph


async def _trigger_or_404(
    db: AsyncSession,
    trigger_id: UUID,
    current_user: dict,
    *,
    for_update: bool = False,
):
    trigger = await get_accessible_trigger(
        db,
        trigger_id,
        user_id=current_user["user_id"],
        is_admin=_is_admin(current_user),
        for_update=for_update,
    )
    if trigger is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Trigger not found",
        )
    return trigger


@router.get(
    "/api/graphs/{graph_id}/triggers",
    response_model=list[TriggerResponse],
)
async def list_graph_triggers(
    graph_id: UUID,
    db: SessionDependency,
    current_user: UserDependency,
    offset: int = Query(default=0, ge=0),
    limit: int = Query(default=50, ge=1, le=100),
) -> list[TriggerResponse]:
    await _graph_or_404(db, graph_id, current_user)
    triggers, _ = await list_triggers(
        db,
        graph_id=graph_id,
        offset=offset,
        limit=limit,
    )
    return triggers


@router.post(
    "/api/graphs/{graph_id}/triggers",
    response_model=TriggerCreateResponse,
    status_code=status.HTTP_201_CREATED,
)
async def create_graph_trigger(
    graph_id: UUID,
    request: TriggerCreate,
    db: SessionDependency,
    current_user: UserDependency,
) -> TriggerCreateResponse:
    graph = await _graph_or_404(db, graph_id, current_user)
    try:
        trigger, plaintext_secret = await create_trigger(
            db,
            graph=graph,
            created_by=current_user["user_id"],
            request=request,
        )
    except TriggerConflictError as exc:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    return TriggerCreateResponse(trigger=trigger, secret=plaintext_secret)


@router.get("/api/triggers/{trigger_id}", response_model=TriggerResponse)
async def get_trigger(
    trigger_id: UUID,
    db: SessionDependency,
    current_user: UserDependency,
):
    return await _trigger_or_404(db, trigger_id, current_user)


@router.patch("/api/triggers/{trigger_id}", response_model=TriggerResponse)
async def patch_trigger(
    trigger_id: UUID,
    request: TriggerUpdate,
    db: SessionDependency,
    current_user: UserDependency,
):
    trigger = await _trigger_or_404(
        db,
        trigger_id,
        current_user,
        for_update=True,
    )
    try:
        return await update_trigger(db, trigger=trigger, request=request)
    except TriggerConflictError as exc:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc


@router.delete("/api/triggers/{trigger_id}", status_code=status.HTTP_204_NO_CONTENT)
async def remove_trigger(
    trigger_id: UUID,
    db: SessionDependency,
    current_user: UserDependency,
) -> Response:
    trigger = await _trigger_or_404(
        db,
        trigger_id,
        current_user,
        for_update=True,
    )
    await delete_trigger(db, trigger)
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.post(
    "/api/triggers/{trigger_id}/rotate-secret",
    response_model=WebhookSecretResponse,
)
async def rotate_trigger_secret(
    trigger_id: UUID,
    db: SessionDependency,
    current_user: UserDependency,
) -> WebhookSecretResponse:
    trigger = await _trigger_or_404(
        db,
        trigger_id,
        current_user,
        for_update=True,
    )
    try:
        plaintext_secret = await rotate_webhook_secret(db, trigger)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    return WebhookSecretResponse(trigger=trigger, secret=plaintext_secret)


@router.post(
    "/api/webhooks/{trigger_id}",
    response_model=WebhookAcceptedResponse,
    status_code=status.HTTP_202_ACCEPTED,
)
async def receive_webhook(
    trigger_id: UUID,
    request: Request,
    db: SessionDependency,
) -> WebhookAcceptedResponse:
    timestamp, _ = parse_timestamp(request.headers.get("x-ingestion-timestamp"))
    signature_digest = parse_signature(request.headers.get("x-ingestion-signature"))
    delivery_id = parse_delivery_id(request.headers.get("x-ingestion-delivery"))
    body = await read_limited_body(request)
    run = await accept_webhook_delivery(
        db,
        trigger_id=trigger_id,
        timestamp=timestamp,
        signature_digest=signature_digest,
        delivery_id=delivery_id,
        body=body,
    )
    return WebhookAcceptedResponse(run_id=run.id, delivery_id=delivery_id)
