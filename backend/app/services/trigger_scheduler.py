"""Multi-process-safe schedule dispatch and webhook-ledger maintenance."""

import asyncio
import logging
from contextlib import suppress
from datetime import UTC, datetime, timedelta
from typing import Any

from app.config import settings
from app.db.session import AsyncSessionLocal
from app.models.execution import RunJobType, TriggerType
from app.models.trigger import GraphTrigger, GraphTriggerType, WebhookDelivery
from app.services.execution_service import create_run
from app.services.trigger_schedule import compute_next_run_at
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)

_scheduler_health: dict[str, Any] = {
    "status": "disabled" if not settings.trigger_scheduler_enabled else "stopped",
    "last_poll_at": None,
    "last_dispatch_count": 0,
    "last_error": None,
}


def utc_now() -> datetime:
    return datetime.now(UTC)


def get_trigger_scheduler_health() -> dict[str, Any]:
    return dict(_scheduler_health)


async def dispatch_due_schedules(
    db: AsyncSession,
    *,
    now: datetime | None = None,
    batch_size: int | None = None,
) -> tuple[int, int]:
    """Claim and dispatch a due batch, skipping rows locked by other processes."""
    dispatched_at = now or utc_now()
    result = await db.execute(
        select(GraphTrigger)
        .where(
            GraphTrigger.trigger_type == GraphTriggerType.SCHEDULE.value,
            GraphTrigger.enabled.is_(True),
            GraphTrigger.next_run_at.is_not(None),
            GraphTrigger.next_run_at <= dispatched_at,
        )
        .order_by(GraphTrigger.next_run_at.asc(), GraphTrigger.id.asc())
        .limit(batch_size or settings.trigger_scheduler_batch_size)
        .with_for_update(skip_locked=True, of=GraphTrigger)
    )
    triggers = list(result.scalars().all())
    dispatched = 0
    failures = 0
    for trigger in triggers:
        trigger_id = trigger.id
        try:
            async with db.begin_nested():
                scheduled_for = trigger.next_run_at or dispatched_at
                run = await create_run(
                    db,
                    graph_id=trigger.graph_id,
                    graph_version_id=trigger.graph_version_id,
                    triggered_by=trigger.created_by,
                    trigger_type=TriggerType.SCHEDULE.value,
                    trigger_payload={
                        "trigger_id": str(trigger.id),
                        "scheduled_for": scheduled_for.isoformat(),
                    },
                    enqueue_job_type=RunJobType.FULL.value,
                    commit=False,
                )
                trigger.last_run_at = dispatched_at
                trigger.last_run_id = run.id
                trigger.next_run_at = compute_next_run_at(
                    schedule_kind=str(trigger.schedule_kind),
                    interval_seconds=trigger.interval_seconds,
                    cron_expression=trigger.cron_expression,
                    timezone_name=trigger.timezone,
                    now=dispatched_at,
                    previous_run_at=scheduled_for,
                )
                await db.flush()
            dispatched += 1
        except asyncio.CancelledError:
            raise
        except Exception:
            failures += 1
            logger.exception("Failed to dispatch schedule trigger %s", trigger_id)
    await db.commit()
    return dispatched, failures


async def prune_webhook_deliveries(
    db: AsyncSession,
    *,
    now: datetime | None = None,
    retention_hours: int | None = None,
) -> int:
    cutoff = (now or utc_now()) - timedelta(
        hours=retention_hours or settings.webhook_delivery_retention_hours
    )
    result = await db.execute(
        delete(WebhookDelivery).where(WebhookDelivery.received_at < cutoff)
    )
    await db.commit()
    return int(result.rowcount or 0)


class TriggerScheduler:
    """One polling loop per API process; database locks coordinate all loops."""

    def __init__(self) -> None:
        self._stop = asyncio.Event()
        self._task: asyncio.Task[None] | None = None
        self._last_prune_at: datetime | None = None

    async def start(self) -> None:
        if self._task is not None:
            return
        self._stop.clear()
        _scheduler_health.update(status="ok", last_error=None)
        self._task = asyncio.create_task(self._loop(), name="trigger-scheduler")
        logger.info("Started trigger scheduler")

    async def stop(self) -> None:
        self._stop.set()
        if self._task is not None:
            self._task.cancel()
            with suppress(asyncio.CancelledError):
                await self._task
            self._task = None
        _scheduler_health["status"] = "stopped"
        logger.info("Stopped trigger scheduler")

    async def _loop(self) -> None:
        while not self._stop.is_set():
            try:
                poll_at = utc_now()
                async with AsyncSessionLocal() as db:
                    dispatched, failures = await dispatch_due_schedules(db, now=poll_at)
                if (
                    self._last_prune_at is None
                    or (poll_at - self._last_prune_at).total_seconds()
                    >= settings.webhook_prune_interval_seconds
                ):
                    async with AsyncSessionLocal() as db:
                        await prune_webhook_deliveries(db, now=poll_at)
                    self._last_prune_at = poll_at
                _scheduler_health.update(
                    status="ok" if failures == 0 else "degraded",
                    last_poll_at=poll_at.isoformat(),
                    last_dispatch_count=dispatched,
                    last_error=(
                        None
                        if failures == 0
                        else f"{failures} schedule trigger(s) failed to dispatch"
                    ),
                )
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                _scheduler_health.update(
                    status="error",
                    last_poll_at=utc_now().isoformat(),
                    last_error=f"{type(exc).__name__}: {exc}"[:1000],
                )
                logger.exception("Trigger scheduler poll failed")
            await self._wait_for_poll()

    async def _wait_for_poll(self) -> None:
        with suppress(TimeoutError):
            await asyncio.wait_for(
                self._stop.wait(),
                timeout=settings.trigger_scheduler_poll_seconds,
            )
