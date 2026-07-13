"""Conditional PostgreSQL races for schedule and webhook trigger claims."""

import asyncio
import hashlib
import hmac
import os
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from app.models.base import Base
from app.models.execution import Run, RunJob
from app.models.graph import Graph, GraphVersion
from app.models.trigger import GraphTrigger, WebhookDelivery
from app.models.user import User
from app.services.connection_crypto import encrypt_connection_config
from app.services.trigger_scheduler import dispatch_due_schedules
from app.services.webhook_service import accept_webhook_delivery
from fastapi import HTTPException
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine


@pytest.fixture
async def postgres_sessions():
    database_url = os.getenv("TEST_DATABASE_URL")
    if not database_url:
        pytest.skip("TEST_DATABASE_URL is required for PostgreSQL trigger races")

    schema = f"trigger_race_{uuid4().hex}"
    admin_engine = create_async_engine(database_url)
    async with admin_engine.begin() as connection:
        await connection.execute(text(f'CREATE SCHEMA "{schema}"'))

    test_engine = create_async_engine(
        database_url,
        execution_options={"schema_translate_map": {None: schema}},
    )
    tables = [
        User.__table__,
        Graph.__table__,
        GraphVersion.__table__,
        Run.__table__,
        RunJob.__table__,
        GraphTrigger.__table__,
        WebhookDelivery.__table__,
    ]
    try:
        async with test_engine.begin() as connection:
            await connection.run_sync(
                lambda sync_connection: Base.metadata.create_all(
                    sync_connection,
                    tables=tables,
                )
            )
        yield async_sessionmaker(test_engine, expire_on_commit=False)
    finally:
        await test_engine.dispose()
        async with admin_engine.begin() as connection:
            await connection.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
        await admin_engine.dispose()


async def seed_trigger(session_factory, *, trigger_type: str):
    now = datetime(2026, 7, 13, 12, tzinfo=UTC)
    user_id = uuid4()
    graph_id = uuid4()
    version_id = uuid4()
    trigger_id = uuid4()
    secret = "postgres-webhook-race-secret"
    async with session_factory() as db:
        db.add(
            User(
                id=user_id,
                username=f"user-{user_id}",
                email=f"{user_id}@example.test",
                password_hash="not-used",
                role="editor",
                is_active=True,
            )
        )
        db.add(
            Graph(
                id=graph_id,
                name="race graph",
                owner_id=user_id,
                status="active",
                tags=[],
            )
        )
        db.add(
            GraphVersion(
                id=version_id,
                graph_id=graph_id,
                version_number=1,
                nodes_data={},
                edges_data=[],
                node_configs={},
            )
        )
        if trigger_type == "schedule":
            trigger = GraphTrigger(
                id=trigger_id,
                graph_id=graph_id,
                graph_version_id=version_id,
                created_by=user_id,
                name="schedule",
                trigger_type="schedule",
                enabled=True,
                schedule_kind="interval",
                interval_seconds=60,
                cron_expression=None,
                timezone="UTC",
                next_run_at=now - timedelta(seconds=1),
                webhook_secret=None,
                rate_limit_per_minute=60,
            )
        else:
            trigger = GraphTrigger(
                id=trigger_id,
                graph_id=graph_id,
                graph_version_id=version_id,
                created_by=user_id,
                name="webhook",
                trigger_type="webhook",
                enabled=True,
                schedule_kind=None,
                interval_seconds=None,
                cron_expression=None,
                timezone="UTC",
                next_run_at=None,
                webhook_secret=encrypt_connection_config({"secret": secret}),
                rate_limit_per_minute=60,
            )
        db.add(trigger)
        await db.commit()
    return trigger_id, secret, now


@pytest.mark.asyncio
async def test_two_schedulers_dispatch_one_due_trigger(postgres_sessions):
    trigger_id, _, now = await seed_trigger(postgres_sessions, trigger_type="schedule")

    async def poll_once():
        async with postgres_sessions() as db:
            return await dispatch_due_schedules(db, now=now, batch_size=10)

    results = await asyncio.gather(poll_once(), poll_once())

    assert sum(dispatched for dispatched, _ in results) == 1
    assert sum(failures for _, failures in results) == 0
    async with postgres_sessions() as db:
        run_count = await db.scalar(select(func.count()).select_from(Run))
        job_count = await db.scalar(select(func.count()).select_from(RunJob))
        trigger = await db.get(GraphTrigger, trigger_id)
    assert run_count == 1
    assert job_count == 1
    assert trigger is not None
    assert trigger.last_run_id is not None
    assert trigger.next_run_at is not None and trigger.next_run_at > now


@pytest.mark.asyncio
async def test_concurrent_duplicate_webhook_creates_one_delivery_and_run(postgres_sessions):
    trigger_id, secret, now = await seed_trigger(postgres_sessions, trigger_type="webhook")
    timestamp = str(int(now.timestamp()))
    body = b'{"event":"concurrent"}'
    digest = hmac.new(
        secret.encode(),
        timestamp.encode() + b"." + body,
        hashlib.sha256,
    ).digest()

    async def deliver_once():
        async with postgres_sessions() as db:
            try:
                run = await accept_webhook_delivery(
                    db,
                    trigger_id=trigger_id,
                    timestamp=timestamp,
                    signature_digest=digest,
                    delivery_id="same-delivery",
                    body=body,
                    now=now,
                )
                return run.id
            except HTTPException as exc:
                return exc.status_code

    outcomes = await asyncio.gather(deliver_once(), deliver_once())

    assert sorted(str(outcome) for outcome in outcomes).count("409") == 1
    async with postgres_sessions() as db:
        run_count = await db.scalar(select(func.count()).select_from(Run))
        job_count = await db.scalar(select(func.count()).select_from(RunJob))
        delivery_count = await db.scalar(select(func.count()).select_from(WebhookDelivery))
    assert (run_count, job_count, delivery_count) == (1, 1, 1)
