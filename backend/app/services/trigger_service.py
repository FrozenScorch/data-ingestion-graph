"""Owner-scoped management operations for schedule and webhook triggers."""

import secrets
from datetime import UTC, datetime
from uuid import UUID

from app.models.graph import Graph, GraphVersion
from app.models.trigger import GraphTrigger, GraphTriggerType
from app.schemas.trigger import TriggerCreate, TriggerUpdate
from app.services.connection_crypto import (
    decrypt_connection_config,
    encrypt_connection_config,
)
from app.services.trigger_schedule import (
    compute_next_run_at,
    validate_schedule_configuration,
)
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession


class TriggerConflictError(ValueError):
    """A graph already has a trigger with the requested name."""


def utc_now() -> datetime:
    return datetime.now(UTC)


def generate_webhook_secret() -> str:
    """Generate a URL-safe secret with 256 bits of entropy."""
    return secrets.token_urlsafe(32)


def decrypt_webhook_secret(trigger: GraphTrigger) -> str:
    """Decrypt a webhook secret for request verification only."""
    decoded = decrypt_connection_config(trigger.webhook_secret)
    secret = decoded.get("secret")
    if not isinstance(secret, str) or not secret:
        raise ValueError("Webhook secret is unavailable")
    return secret


async def get_accessible_graph(
    db: AsyncSession,
    graph_id: UUID,
    *,
    user_id: UUID,
    is_admin: bool,
) -> Graph | None:
    query = select(Graph).where(Graph.id == graph_id)
    if not is_admin:
        query = query.where(Graph.owner_id == user_id)
    result = await db.execute(query)
    return result.scalar_one_or_none()


async def get_accessible_trigger(
    db: AsyncSession,
    trigger_id: UUID,
    *,
    user_id: UUID,
    is_admin: bool,
    for_update: bool = False,
) -> GraphTrigger | None:
    query = select(GraphTrigger).join(Graph, Graph.id == GraphTrigger.graph_id).where(
        GraphTrigger.id == trigger_id
    )
    if not is_admin:
        query = query.where(Graph.owner_id == user_id)
    if for_update:
        query = query.with_for_update(of=GraphTrigger)
    result = await db.execute(query)
    return result.scalar_one_or_none()


async def resolve_graph_version(
    db: AsyncSession,
    graph_id: UUID,
    requested_version_id: UUID | None,
) -> GraphVersion | None:
    query = select(GraphVersion).where(GraphVersion.graph_id == graph_id)
    if requested_version_id is not None:
        query = query.where(GraphVersion.id == requested_version_id)
    else:
        query = query.order_by(GraphVersion.version_number.desc()).limit(1)
    result = await db.execute(query)
    return result.scalar_one_or_none()


async def create_trigger(
    db: AsyncSession,
    *,
    graph: Graph,
    created_by: UUID,
    request: TriggerCreate,
    now: datetime | None = None,
) -> tuple[GraphTrigger, str | None]:
    version = await resolve_graph_version(db, graph.id, request.graph_version_id)
    if version is None:
        raise ValueError("Graph has no matching saved version")

    created_at = now or utc_now()
    plaintext_secret: str | None = None
    encrypted_secret: dict[str, str] | None = None
    next_run_at: datetime | None = None
    if request.trigger_type == GraphTriggerType.WEBHOOK.value:
        plaintext_secret = generate_webhook_secret()
        encrypted_secret = encrypt_connection_config({"secret": plaintext_secret})
    elif request.enabled:
        next_run_at = compute_next_run_at(
            schedule_kind=str(request.schedule_kind),
            interval_seconds=request.interval_seconds,
            cron_expression=request.cron_expression,
            timezone_name=request.timezone,
            now=created_at,
        )

    trigger = GraphTrigger(
        graph_id=graph.id,
        graph_version_id=version.id,
        created_by=created_by,
        name=request.name,
        trigger_type=request.trigger_type,
        enabled=request.enabled,
        schedule_kind=request.schedule_kind,
        interval_seconds=request.interval_seconds,
        cron_expression=request.cron_expression,
        timezone=request.timezone,
        next_run_at=next_run_at,
        webhook_secret=encrypted_secret,
        rate_limit_per_minute=request.rate_limit_per_minute,
    )
    db.add(trigger)
    try:
        await db.commit()
    except IntegrityError as exc:
        await db.rollback()
        raise TriggerConflictError("A trigger with this name already exists") from exc
    await db.refresh(trigger)
    return trigger, plaintext_secret


async def list_triggers(
    db: AsyncSession,
    *,
    graph_id: UUID,
    offset: int,
    limit: int,
) -> tuple[list[GraphTrigger], int]:
    filters = (GraphTrigger.graph_id == graph_id,)
    count_result = await db.execute(
        select(func.count()).select_from(GraphTrigger).where(*filters)
    )
    result = await db.execute(
        select(GraphTrigger)
        .where(*filters)
        .order_by(GraphTrigger.created_at.desc(), GraphTrigger.id.desc())
        .offset(offset)
        .limit(limit)
    )
    return list(result.scalars().all()), int(count_result.scalar() or 0)


async def update_trigger(
    db: AsyncSession,
    *,
    trigger: GraphTrigger,
    request: TriggerUpdate,
    now: datetime | None = None,
) -> GraphTrigger:
    updates = request.model_dump(exclude_unset=True)
    if "graph_version_id" in updates:
        version = await resolve_graph_version(
            db,
            trigger.graph_id,
            updates.pop("graph_version_id"),
        )
        if version is None:
            raise ValueError("Graph has no matching saved version")
        trigger.graph_version_id = version.id

    schedule_fields = {
        "schedule_kind",
        "interval_seconds",
        "cron_expression",
        "timezone",
    }
    schedule_changed = bool(schedule_fields.intersection(updates))
    if trigger.trigger_type == GraphTriggerType.WEBHOOK.value:
        if schedule_changed:
            raise ValueError("Schedule fields cannot be changed on a webhook trigger")
        trigger.next_run_at = None
    else:
        schedule_kind = updates.get("schedule_kind", trigger.schedule_kind)
        interval_seconds = updates.get("interval_seconds", trigger.interval_seconds)
        cron_expression = updates.get("cron_expression", trigger.cron_expression)
        if updates.get("schedule_kind") == "interval":
            cron_expression = updates.get("cron_expression")
        elif updates.get("schedule_kind") == "cron":
            interval_seconds = updates.get("interval_seconds")
        timezone_name = updates.get("timezone", trigger.timezone)
        (
            schedule_kind,
            interval_seconds,
            cron_expression,
            timezone_name,
        ) = validate_schedule_configuration(
            schedule_kind=schedule_kind,
            interval_seconds=interval_seconds,
            cron_expression=cron_expression,
            timezone_name=timezone_name,
        )
        updates.update(
            {
                "schedule_kind": schedule_kind,
                "interval_seconds": interval_seconds,
                "cron_expression": cron_expression,
                "timezone": timezone_name,
            }
        )

    for field, value in updates.items():
        setattr(trigger, field, value)

    enabled_changed = "enabled" in updates
    if trigger.trigger_type == GraphTriggerType.SCHEDULE.value and (
        schedule_changed or enabled_changed
    ):
        trigger.next_run_at = (
            compute_next_run_at(
                schedule_kind=str(trigger.schedule_kind),
                interval_seconds=trigger.interval_seconds,
                cron_expression=trigger.cron_expression,
                timezone_name=trigger.timezone,
                now=now or utc_now(),
            )
            if trigger.enabled
            else None
        )

    try:
        await db.commit()
    except IntegrityError as exc:
        await db.rollback()
        raise TriggerConflictError("A trigger with this name already exists") from exc
    await db.refresh(trigger)
    return trigger


async def delete_trigger(db: AsyncSession, trigger: GraphTrigger) -> None:
    await db.delete(trigger)
    await db.commit()


async def rotate_webhook_secret(
    db: AsyncSession,
    trigger: GraphTrigger,
) -> str:
    if trigger.trigger_type != GraphTriggerType.WEBHOOK.value:
        raise ValueError("Only webhook triggers have secrets")
    plaintext_secret = generate_webhook_secret()
    trigger.webhook_secret = encrypt_connection_config({"secret": plaintext_secret})
    await db.commit()
    await db.refresh(trigger)
    return plaintext_secret
