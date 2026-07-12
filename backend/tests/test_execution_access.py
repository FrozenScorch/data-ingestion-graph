from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest
from app.api.executions import cancel_run_endpoint, retry_failed_run
from fastapi import HTTPException


def owner_result(owner_id):
    result = MagicMock()
    result.scalar_one_or_none.return_value = owner_id
    return result


@pytest.mark.asyncio
async def test_other_user_cannot_cancel_victim_run():
    victim_owner = uuid4()
    attacker = uuid4()
    run = SimpleNamespace(id=uuid4(), graph_id=uuid4(), status="running")
    db = AsyncMock()
    db.execute = AsyncMock(return_value=owner_result(victim_owner))

    with (
        patch("app.api.executions.get_run", new=AsyncMock(return_value=run)),
        patch("app.api.executions.cancel_run", new=AsyncMock()) as cancel,
        pytest.raises(HTTPException) as exc,
    ):
        await cancel_run_endpoint(
            run_id=run.id,
            db=db,
            current_user={"user_id": attacker, "role": "user"},
        )
    assert exc.value.status_code == 404
    cancel.assert_not_awaited()


@pytest.mark.asyncio
async def test_other_user_cannot_retry_victim_upload_run():
    victim_owner = uuid4()
    attacker = uuid4()
    run = SimpleNamespace(
        id=uuid4(),
        graph_id=uuid4(),
        graph_version_id=uuid4(),
        status="failed",
    )
    db = AsyncMock()
    db.execute = AsyncMock(return_value=owner_result(victim_owner))

    with (
        patch("app.api.executions.get_run", new=AsyncMock(return_value=run)),
        pytest.raises(HTTPException) as exc,
    ):
        await retry_failed_run(
            run_id=run.id,
            background_tasks=MagicMock(),
            db=db,
            current_user={"user_id": attacker, "role": "user"},
        )
    assert exc.value.status_code == 404
    db.commit.assert_not_awaited()
