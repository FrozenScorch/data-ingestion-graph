"""Regression tests for tenant isolation and operator-facing correctness."""

from unittest.mock import ANY, AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest
from fastapi import HTTPException, Response
from fastapi.security import HTTPAuthorizationCredentials


@pytest.mark.asyncio
async def test_non_admin_dlq_list_is_scoped_to_graph_owner():
    from app.api.dead_letter import list_dlq_items

    count_result = MagicMock()
    count_result.scalar.return_value = 0
    items_result = MagicMock()
    items_result.scalars.return_value.all.return_value = []
    db = AsyncMock()
    db.execute = AsyncMock(side_effect=[count_result, items_result])

    owner_id = uuid4()
    await list_dlq_items(
        resolved=None,
        node_type=None,
        db=db,
        current_user={"user_id": owner_id, "role": "user"},
    )

    count_query, items_query = [call.args[0] for call in db.execute.await_args_list]
    for query in (count_query, items_query):
        sql = str(query)
        assert "JOIN runs" in sql
        assert "JOIN graphs" in sql
        assert "graphs.owner_id" in sql


@pytest.mark.asyncio
async def test_non_admin_cannot_mutate_runless_dlq_item():
    from app.api.dead_letter import _check_dlq_item_access

    item = MagicMock(run_id=None)
    with pytest.raises(HTTPException) as exc_info:
        await _check_dlq_item_access(
            item,
            {"user_id": uuid4(), "role": "user"},
            AsyncMock(),
        )

    assert exc_info.value.status_code == 404
    assert exc_info.value.detail == "DLQ item not found"


@pytest.mark.asyncio
async def test_admin_can_access_runless_dlq_item():
    from app.api.dead_letter import _check_dlq_item_access

    db = AsyncMock()
    await _check_dlq_item_access(
        MagicMock(run_id=None),
        {"user_id": uuid4(), "role": "admin"},
        db,
    )
    db.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_source_lineage_query_is_scoped_to_graph_owner():
    from app.services.lineage_service import get_lineage_for_source

    provenance_result = MagicMock()
    provenance_result.scalars.return_value.all.return_value = []
    db = AsyncMock()
    db.execute = AsyncMock(return_value=provenance_result)

    await get_lineage_for_source(db, "s3://private/object", owner_id=uuid4())

    query = db.execute.await_args.args[0]
    sql = str(query)
    assert "JOIN runs" in sql
    assert "JOIN graphs" in sql
    assert "graphs.owner_id" in sql


@pytest.mark.asyncio
async def test_source_lineage_endpoint_passes_owner_scope():
    from app.api.lineage import list_lineage_for_source

    owner_id = uuid4()
    with patch(
        "app.api.lineage.get_lineage_for_source", new_callable=AsyncMock, return_value=[]
    ) as get_source:
        result = await list_lineage_for_source(
            source_ref="s3://private/object",
            db=AsyncMock(),
            current_user={"user_id": owner_id, "role": "user"},
        )

    get_source.assert_awaited_once_with(ANY, "s3://private/object", owner_id=owner_id)
    assert result["total"] == 0


@pytest.mark.asyncio
async def test_database_health_failure_returns_unhealthy_503():
    from app.api.health import health_check

    session_factory = MagicMock()
    session_factory.return_value.__aenter__ = AsyncMock(side_effect=RuntimeError("db down"))
    session_factory.return_value.__aexit__ = AsyncMock(return_value=None)
    redis = AsyncMock()

    response = Response()
    with (
        patch("app.db.session.AsyncSessionLocal", session_factory),
        patch("app.db.redis.get_redis", return_value=redis),
    ):
        result = await health_check(response)

    assert response.status_code == 503
    assert result["status"] == "unhealthy"
    assert result["components"]["database"]["status"] == "error"
    assert "detail" not in result["components"]["database"]


@pytest.mark.asyncio
async def test_malformed_access_token_subject_returns_401():
    from app.middleware.auth import get_current_user

    credentials = HTTPAuthorizationCredentials(scheme="Bearer", credentials="token")
    get_user = AsyncMock()
    with (
        patch(
            "app.middleware.auth.decode_token",
            return_value={"type": "access", "sub": "not-a-uuid"},
        ),
        patch("app.middleware.auth.get_user_by_id", get_user),
        pytest.raises(HTTPException) as exc_info,
    ):
        await get_current_user(request=MagicMock(), credentials=credentials, db=AsyncMock())

    assert exc_info.value.status_code == 401
    assert exc_info.value.detail == "Invalid token payload"
    get_user.assert_not_awaited()
