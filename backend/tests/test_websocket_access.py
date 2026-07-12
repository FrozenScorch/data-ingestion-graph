from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest
from app.ws.execution_ws import authorize_run_subscription


@pytest.mark.asyncio
async def test_websocket_denies_other_users_run():
    owner_id = uuid4()
    attacker_id = uuid4()
    db = AsyncMock()
    result = MagicMock()
    result.scalar_one_or_none.return_value = owner_id
    db.execute = AsyncMock(return_value=result)
    attacker = SimpleNamespace(id=attacker_id, role="viewer", is_active=True)
    with patch("app.ws.execution_ws.get_user_by_id", new=AsyncMock(return_value=attacker)):
        allowed = await authorize_run_subscription(db, run_id=str(uuid4()), user_id=attacker_id)
    assert allowed is False


@pytest.mark.asyncio
async def test_websocket_allows_run_owner():
    owner_id = uuid4()
    db = AsyncMock()
    result = MagicMock()
    result.scalar_one_or_none.return_value = owner_id
    db.execute = AsyncMock(return_value=result)
    owner = SimpleNamespace(id=owner_id, role="viewer", is_active=True)
    with patch("app.ws.execution_ws.get_user_by_id", new=AsyncMock(return_value=owner)):
        allowed = await authorize_run_subscription(db, run_id=str(uuid4()), user_id=owner_id)
    assert allowed is True
