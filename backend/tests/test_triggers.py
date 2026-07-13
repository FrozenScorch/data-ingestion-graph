"""Schedule and signed-webhook trigger control-plane tests."""

import hashlib
import hmac
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest
from app.config import settings
from app.engine.executor import DAGExecutor
from app.models.execution import Run
from app.models.trigger import GraphTrigger
from app.schemas.trigger import TriggerCreate, TriggerResponse, TriggerUpdate
from app.services.execution_service import create_run
from app.services.trigger_schedule import compute_next_run_at
from app.services.trigger_scheduler import dispatch_due_schedules
from app.services.trigger_service import get_accessible_trigger
from app.services.webhook_service import (
    accept_webhook_delivery,
    parse_json_object,
    parse_timestamp,
    read_limited_body,
    verify_webhook_signature,
)
from fastapi import HTTPException
from pydantic import ValidationError


def scalar_result(value):
    result = MagicMock()
    result.scalar_one_or_none.return_value = value
    return result


def count_result(value: int):
    result = MagicMock()
    result.scalar.return_value = value
    return result


def trigger_model(**overrides) -> GraphTrigger:
    now = datetime(2026, 7, 13, 12, tzinfo=UTC)
    values = {
        "id": uuid4(),
        "graph_id": uuid4(),
        "graph_version_id": uuid4(),
        "created_by": uuid4(),
        "name": "trigger",
        "trigger_type": "webhook",
        "enabled": True,
        "schedule_kind": None,
        "interval_seconds": None,
        "cron_expression": None,
        "timezone": "UTC",
        "next_run_at": None,
        "last_run_at": None,
        "last_run_id": None,
        "webhook_secret": None,
        "rate_limit_per_minute": 60,
        "created_at": now,
        "updated_at": now,
    }
    values.update(overrides)
    return GraphTrigger(**values)


def test_trigger_schema_enforces_conditional_schedule_fields_and_timezone():
    interval = TriggerCreate(
        name="Every minute",
        trigger_type="schedule",
        schedule_kind="interval",
        interval_seconds=60,
    )
    assert interval.interval_seconds == 60

    with pytest.raises(ValidationError, match="cron_expression"):
        TriggerCreate(
            name="Bad interval",
            trigger_type="schedule",
            schedule_kind="interval",
            interval_seconds=60,
            cron_expression="* * * * *",
        )
    with pytest.raises(ValidationError, match="exactly five fields"):
        TriggerCreate(
            name="Six fields",
            trigger_type="schedule",
            schedule_kind="cron",
            cron_expression="0 0 * * * *",
        )
    with pytest.raises(ValidationError, match="Unknown timezone"):
        TriggerCreate(
            name="Bad zone",
            trigger_type="schedule",
            schedule_kind="cron",
            cron_expression="0 9 * * *",
            timezone="Mars/Olympus",
        )
    with pytest.raises(ValidationError, match="not valid for a webhook"):
        TriggerCreate(
            name="Mixed",
            trigger_type="webhook",
            schedule_kind="interval",
            interval_seconds=60,
        )


@pytest.mark.parametrize(
    "field_name", ["name", "enabled", "timezone", "rate_limit_per_minute"]
)
def test_trigger_patch_rejects_explicit_null_for_required_fields(field_name):
    with pytest.raises(ValidationError, match=f"{field_name} must not be null"):
        TriggerUpdate.model_validate({field_name: None})


def test_schedule_next_time_skips_interval_backlog_and_honors_cron_timezone():
    now = datetime(2026, 7, 13, 12, 5, 30, tzinfo=UTC)
    next_interval = compute_next_run_at(
        schedule_kind="interval",
        interval_seconds=60,
        cron_expression=None,
        timezone_name="UTC",
        now=now,
        previous_run_at=datetime(2026, 7, 13, 12, 0, tzinfo=UTC),
    )
    assert next_interval == datetime(2026, 7, 13, 12, 6, tzinfo=UTC)

    next_cron = compute_next_run_at(
        schedule_kind="cron",
        interval_seconds=None,
        cron_expression="0 9 * * *",
        timezone_name="America/New_York",
        now=datetime(2026, 7, 13, 12, 30, tzinfo=UTC),
    )
    assert next_cron == datetime(2026, 7, 13, 13, 0, tzinfo=UTC)


def test_normal_trigger_response_never_contains_encrypted_or_plaintext_secret():
    trigger = trigger_model(webhook_secret={"$encrypted": "ciphertext"})
    payload = TriggerResponse.model_validate(trigger).model_dump(mode="json")
    assert "secret" not in payload
    assert "webhook_secret" not in payload
    assert payload["webhook_path"] == f"/api/webhooks/{trigger.id}"


def test_hmac_contract_uses_timestamp_dot_raw_body_and_constant_digest_shape():
    secret = "a" * 43
    timestamp = "1783944000"
    delivery_id = "delivery-1"
    body = b'{"event":"created"}'
    digest = hmac.new(
        secret.encode(),
        timestamp.encode() + b"." + delivery_id.encode() + b"." + body,
        hashlib.sha256,
    ).digest()
    assert verify_webhook_signature(
        secret=secret,
        timestamp=timestamp,
        delivery_id=delivery_id,
        body=body,
        provided_digest=digest,
    )
    assert not verify_webhook_signature(
        secret=secret,
        timestamp=timestamp,
        delivery_id="delivery-2",
        body=body,
        provided_digest=digest,
    )
    assert not verify_webhook_signature(
        secret=secret,
        timestamp=timestamp,
        delivery_id=delivery_id,
        body=body + b" ",
        provided_digest=digest,
    )


@pytest.mark.parametrize("offset", [-1, 1])
def test_webhook_timestamp_rejects_stale_and_future_values(offset):
    now = datetime(2026, 7, 13, 12, tzinfo=UTC)
    outside = now + timedelta(
        seconds=offset * (settings.webhook_timestamp_skew_seconds + 1)
    )
    with pytest.raises(HTTPException) as exc_info:
        parse_timestamp(str(int(outside.timestamp())), now=now)
    assert exc_info.value.status_code == 401


def test_webhook_json_requires_an_object():
    assert parse_json_object(b'{"ok":true}') == {"ok": True}
    for body in (b"not-json", b"[]", b'"string"'):
        with pytest.raises(HTTPException) as exc_info:
            parse_json_object(body)
        assert exc_info.value.status_code == 400


@pytest.mark.asyncio
async def test_webhook_body_stream_stops_at_configured_limit():
    class ChunkedRequest:
        headers = {}

        async def stream(self):
            yield b"1234"
            yield b"5"

    with pytest.raises(HTTPException) as exc_info:
        await read_limited_body(ChunkedRequest(), max_bytes=4)
    assert exc_info.value.status_code == 413


@pytest.mark.asyncio
async def test_trigger_owner_lookup_uses_404_safe_owner_filter():
    db = AsyncMock()
    db.execute = AsyncMock(return_value=scalar_result(None))
    owner_id = uuid4()

    assert (
        await get_accessible_trigger(
            db,
            uuid4(),
            user_id=owner_id,
            is_admin=False,
        )
        is None
    )
    sql = str(db.execute.await_args.args[0])
    assert "JOIN graphs" in sql
    assert "graphs.owner_id" in sql


@pytest.mark.asyncio
async def test_create_run_commit_false_keeps_run_and_job_in_callers_transaction():
    db = AsyncMock()
    db.add = MagicMock()
    graph_id = uuid4()
    user_id = uuid4()
    version_id = uuid4()

    async def assign_run_id():
        added = db.add.call_args.args[0]
        added.id = uuid4()

    db.flush = AsyncMock(side_effect=assign_run_id)
    with (
        patch(
            "app.services.execution_service._supersede_inactive_failed_runs",
            new=AsyncMock(),
        ),
        patch(
            "app.services.run_queue_service.enqueue_run_job",
            new=AsyncMock(),
        ) as enqueue,
    ):
        run = await create_run(
            db,
            graph_id=graph_id,
            graph_version_id=version_id,
            triggered_by=user_id,
            trigger_type="webhook",
            trigger_payload={"event": "created"},
            enqueue_job_type="full",
            commit=False,
        )

    assert run.trigger_payload == {"event": "created"}
    enqueue.assert_awaited_once()
    assert enqueue.await_args.kwargs["commit"] is False
    db.commit.assert_not_awaited()
    db.refresh.assert_not_awaited()


def test_payload_is_injected_only_into_root_webhook_source():
    payload = {"event": "created"}
    state = {"outputs": {}, "trigger_payload": payload}
    assert DAGExecutor._inject_webhook_payload(
        "root",
        "webhook_source",
        [],
        {},
        state,
    ) == {"webhook_payload": payload}
    assert DAGExecutor._inject_webhook_payload(
        "other",
        "transform",
        [],
        {},
        state,
    ) == {}
    assert DAGExecutor._inject_webhook_payload(
        "child",
        "webhook_source",
        [{"source": "root", "target": "child"}],
        {},
        state,
    ) == {}


@pytest.mark.asyncio
async def test_scheduler_dispatches_run_job_and_advances_past_missed_intervals():
    due = datetime(2026, 7, 13, 12, 0, tzinfo=UTC)
    now = due + timedelta(minutes=5, seconds=30)
    trigger = trigger_model(
        trigger_type="schedule",
        schedule_kind="interval",
        interval_seconds=60,
        next_run_at=due,
        webhook_secret=None,
    )
    result = MagicMock()
    result.scalars.return_value.all.return_value = [trigger]
    db = AsyncMock()
    db.execute = AsyncMock(return_value=result)
    db.begin_nested = MagicMock()
    savepoint = AsyncMock()
    savepoint.__aenter__.return_value = None
    savepoint.__aexit__.return_value = False
    db.begin_nested.return_value = savepoint
    queued_run = Run(id=uuid4(), graph_id=trigger.graph_id, status="pending")

    with patch(
        "app.services.trigger_scheduler.create_run",
        new=AsyncMock(return_value=queued_run),
    ) as create:
        dispatched, failures = await dispatch_due_schedules(db, now=now, batch_size=10)

    assert (dispatched, failures) == (1, 0)
    assert trigger.last_run_id == queued_run.id
    assert trigger.next_run_at == datetime(2026, 7, 13, 12, 6, tzinfo=UTC)
    assert create.await_args.kwargs["commit"] is False
    assert create.await_args.kwargs["enqueue_job_type"] == "full"
    assert "FOR UPDATE" in str(db.execute.await_args.args[0])
    db.commit.assert_awaited_once()


@pytest.mark.asyncio
async def test_one_bad_schedule_does_not_block_the_rest_of_the_claimed_batch():
    now = datetime(2026, 7, 13, 12, tzinfo=UTC)
    bad = trigger_model(
        trigger_type="schedule",
        schedule_kind="interval",
        interval_seconds=60,
        next_run_at=now - timedelta(minutes=1),
    )
    good = trigger_model(
        trigger_type="schedule",
        schedule_kind="interval",
        interval_seconds=60,
        next_run_at=now - timedelta(minutes=1),
    )
    result = MagicMock()
    result.scalars.return_value.all.return_value = [bad, good]
    db = AsyncMock()
    db.execute = AsyncMock(return_value=result)
    db.begin_nested = MagicMock()
    savepoint = AsyncMock()
    savepoint.__aenter__.return_value = None
    savepoint.__aexit__.return_value = False
    db.begin_nested.return_value = savepoint
    queued_run = Run(id=uuid4(), graph_id=good.graph_id, status="pending")

    with patch(
        "app.services.trigger_scheduler.create_run",
        new=AsyncMock(side_effect=[RuntimeError("broken"), queued_run]),
    ):
        dispatched, failures = await dispatch_due_schedules(db, now=now, batch_size=10)

    assert (dispatched, failures) == (1, 1)
    assert good.last_run_id == queued_run.id
    db.commit.assert_awaited_once()


@pytest.mark.asyncio
async def test_rotated_secret_is_encrypted_and_returned_only_once():
    from app.services.connection_crypto import decrypt_connection_config
    from app.services.trigger_service import rotate_webhook_secret

    trigger = trigger_model(webhook_secret={"$encrypted": "old"})
    db = AsyncMock()

    plaintext = await rotate_webhook_secret(db, trigger)

    assert len(plaintext) >= 43
    assert trigger.webhook_secret != {"secret": plaintext}
    assert decrypt_connection_config(trigger.webhook_secret) == {"secret": plaintext}
    db.commit.assert_awaited_once()
    db.refresh.assert_awaited_once_with(trigger)


@pytest.mark.asyncio
async def test_webhook_acceptance_creates_delivery_run_and_job_atomically():
    from app.services.connection_crypto import encrypt_connection_config

    secret = "webhook-secret-with-sufficient-entropy"
    body = b'{"event":"created","id":7}'
    timestamp = "1783944000"
    digest = hmac.new(
        secret.encode(),
        timestamp.encode() + b".delivery-1." + body,
        hashlib.sha256,
    ).digest()
    trigger = trigger_model(
        webhook_secret=encrypt_connection_config({"secret": secret}),
        rate_limit_per_minute=2,
    )
    db = AsyncMock()
    db.add = MagicMock()
    db.execute = AsyncMock(
        side_effect=[scalar_result(trigger), scalar_result(None), count_result(0)]
    )
    run = Run(id=uuid4(), graph_id=trigger.graph_id, status="pending")

    with patch(
        "app.services.webhook_service.create_run",
        new=AsyncMock(return_value=run),
    ) as create:
        accepted = await accept_webhook_delivery(
            db,
            trigger_id=trigger.id,
            timestamp=timestamp,
            signature_digest=digest,
            delivery_id="delivery-1",
            body=body,
            now=datetime(2026, 7, 13, 12, tzinfo=UTC),
        )

    assert accepted is run
    delivery = db.add.call_args.args[0]
    assert delivery.delivery_id == "delivery-1"
    assert delivery.body_sha256 == hashlib.sha256(body).hexdigest()
    assert delivery.run_id == run.id
    assert create.await_args.kwargs["trigger_payload"] == {
        "event": "created",
        "id": 7,
    }
    assert create.await_args.kwargs["commit"] is False
    db.commit.assert_awaited_once()


@pytest.mark.asyncio
async def test_webhook_replay_and_rate_limit_never_create_a_second_run():
    from app.services.connection_crypto import encrypt_connection_config

    secret = "webhook-secret-with-sufficient-entropy"
    body = b'{"event":"created"}'
    timestamp = "1783944000"
    digest = hmac.new(
        secret.encode(),
        timestamp.encode() + b".delivery-1." + body,
        hashlib.sha256,
    ).digest()
    trigger = trigger_model(
        webhook_secret=encrypt_connection_config({"secret": secret}),
        rate_limit_per_minute=1,
    )
    for results, expected_status in (
        ([scalar_result(trigger), scalar_result(uuid4())], 409),
        ([scalar_result(trigger), scalar_result(None), count_result(1)], 429),
    ):
        db = AsyncMock()
        db.execute = AsyncMock(side_effect=results)
        with (
            patch(
                "app.services.webhook_service.create_run",
                new=AsyncMock(),
            ) as create,
            pytest.raises(HTTPException) as exc_info,
        ):
            await accept_webhook_delivery(
                db,
                trigger_id=trigger.id,
                timestamp=timestamp,
                signature_digest=digest,
                delivery_id="delivery-1",
                body=body,
                now=datetime(2026, 7, 13, 12, tzinfo=UTC),
            )
        assert exc_info.value.status_code == expected_status
        create.assert_not_awaited()
        db.rollback.assert_awaited_once()
