"""Signed webhook validation and atomic delivery-to-run conversion."""

import hashlib
import hmac
import json
import re
from datetime import UTC, datetime, timedelta
from typing import Any

from app.config import settings
from app.models.execution import Run, RunJobType, TriggerType
from app.models.trigger import GraphTrigger, GraphTriggerType, WebhookDelivery
from app.services.execution_service import create_run
from app.services.trigger_service import decrypt_webhook_secret
from fastapi import HTTPException, Request, status
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

_SIGNATURE_RE = re.compile(r"^sha256=([0-9a-fA-F]{64})$")
_TIMESTAMP_RE = re.compile(r"^-?[0-9]{1,12}$")
_DELIVERY_RE = re.compile(r"^[\x21-\x7e]{1,255}$")


def utc_now() -> datetime:
    return datetime.now(UTC)


async def read_limited_body(request: Request, max_bytes: int | None = None) -> bytes:
    """Read a request body without ever buffering more than the configured limit."""
    limit = max_bytes or settings.webhook_max_bytes
    content_length = request.headers.get("content-length")
    if content_length is not None:
        try:
            declared_length = int(content_length)
        except ValueError:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Invalid Content-Length header",
            ) from None
        if declared_length < 0:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Invalid Content-Length header",
            )
        if declared_length > limit:
            raise HTTPException(
                status_code=413,
                detail="Webhook payload exceeds the configured limit",
            )

    body = bytearray()
    async for chunk in request.stream():
        if len(body) + len(chunk) > limit:
            raise HTTPException(
                status_code=413,
                detail="Webhook payload exceeds the configured limit",
            )
        body.extend(chunk)
    return bytes(body)


def parse_timestamp(value: str | None, *, now: datetime | None = None) -> tuple[str, datetime]:
    if value is None or not _TIMESTAMP_RE.fullmatch(value):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="X-Ingestion-Timestamp must be Unix seconds",
        )
    try:
        timestamp_seconds = int(value)
        received_time = datetime.fromtimestamp(timestamp_seconds, tz=UTC)
    except (OverflowError, OSError, ValueError):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="X-Ingestion-Timestamp must be Unix seconds",
        ) from None
    current_time = now or utc_now()
    if current_time.tzinfo is None:
        current_time = current_time.replace(tzinfo=UTC)
    if abs((current_time.astimezone(UTC) - received_time).total_seconds()) > (
        settings.webhook_timestamp_skew_seconds
    ):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Webhook timestamp is outside the allowed clock skew",
        )
    return value, received_time


def parse_delivery_id(value: str | None) -> str:
    if value is None or not _DELIVERY_RE.fullmatch(value):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="X-Ingestion-Delivery is invalid",
        )
    return value


def parse_signature(value: str | None) -> bytes:
    match = _SIGNATURE_RE.fullmatch(value or "")
    if match is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="X-Ingestion-Signature is invalid",
        )
    return bytes.fromhex(match.group(1))


def verify_webhook_signature(
    *,
    secret: str,
    timestamp: str,
    delivery_id: str,
    body: bytes,
    provided_digest: bytes,
) -> bool:
    signed_payload = (
        timestamp.encode("ascii")
        + b"."
        + delivery_id.encode("ascii")
        + b"."
        + body
    )
    expected_digest = hmac.new(
        secret.encode("utf-8"),
        signed_payload,
        hashlib.sha256,
    ).digest()
    return hmac.compare_digest(expected_digest, provided_digest)


def parse_json_object(body: bytes) -> dict[str, Any]:
    try:
        payload = json.loads(body)
    except (json.JSONDecodeError, UnicodeDecodeError):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Webhook body must be valid JSON",
        ) from None
    if not isinstance(payload, dict):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Webhook body must be a JSON object",
        )
    return payload


async def accept_webhook_delivery(
    db: AsyncSession,
    *,
    trigger_id: Any,
    timestamp: str,
    signature_digest: bytes,
    delivery_id: str,
    body: bytes,
    now: datetime | None = None,
) -> Run:
    """Verify and accept one delivery while holding a short trigger-row lock."""
    received_at = now or utc_now()
    trigger_result = await db.execute(
        select(GraphTrigger)
        .where(GraphTrigger.id == trigger_id)
        .with_for_update(of=GraphTrigger)
        .execution_options(populate_existing=True)
    )
    trigger = trigger_result.scalar_one_or_none()
    if (
        trigger is None
        or trigger.trigger_type != GraphTriggerType.WEBHOOK.value
        or not trigger.enabled
    ):
        await db.rollback()
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Webhook trigger not found",
        )

    try:
        secret = decrypt_webhook_secret(trigger)
    except ValueError:
        await db.rollback()
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Webhook trigger is unavailable",
        ) from None
    if not verify_webhook_signature(
        secret=secret,
        timestamp=timestamp,
        delivery_id=delivery_id,
        body=body,
        provided_digest=signature_digest,
    ):
        await db.rollback()
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Webhook signature is invalid",
        )

    try:
        payload = parse_json_object(body)
    except HTTPException:
        await db.rollback()
        raise
    replay_result = await db.execute(
        select(WebhookDelivery.id).where(
            WebhookDelivery.trigger_id == trigger.id,
            WebhookDelivery.delivery_id == delivery_id,
        )
    )
    if replay_result.scalar_one_or_none() is not None:
        await db.rollback()
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Webhook delivery has already been accepted",
        )

    window_start = received_at - timedelta(minutes=1)
    rate_result = await db.execute(
        select(func.count())
        .select_from(WebhookDelivery)
        .where(
            WebhookDelivery.trigger_id == trigger.id,
            WebhookDelivery.received_at >= window_start,
        )
    )
    if int(rate_result.scalar() or 0) >= trigger.rate_limit_per_minute:
        await db.rollback()
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail="Webhook rate limit exceeded",
            headers={"Retry-After": "60"},
        )

    delivery = WebhookDelivery(
        trigger_id=trigger.id,
        delivery_id=delivery_id,
        body_sha256=hashlib.sha256(body).hexdigest(),
        received_at=received_at,
    )
    db.add(delivery)
    try:
        await db.flush()
        run = await create_run(
            db,
            graph_id=trigger.graph_id,
            graph_version_id=trigger.graph_version_id,
            triggered_by=trigger.created_by,
            trigger_type=TriggerType.WEBHOOK.value,
            trigger_payload=payload,
            enqueue_job_type=RunJobType.FULL.value,
            commit=False,
        )
        delivery.run_id = run.id
        trigger.last_run_at = received_at
        trigger.last_run_id = run.id
        await db.commit()
    except IntegrityError as exc:
        await db.rollback()
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Webhook delivery has already been accepted",
        ) from exc
    return run
