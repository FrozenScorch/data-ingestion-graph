"""Proof that Studio consumes the reusable SDK for ingest-and-query testing."""

import os
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest
from app.api.query import query_run_output
from app.nodes.base import NodeContext
from app.nodes.sdk_query_store import SDKQueryStoreNode
from fastapi import HTTPException
from ingestion_graph.destinations import SQLiteCollection
from ingestion_graph.query import QueryRequest


@pytest.mark.asyncio
async def test_studio_node_materializes_queryable_sdk_collection(tmp_path):
    run_id = str(uuid4())
    context = NodeContext(
        run_id=run_id,
        node_id="query-store",
        config={"collection": "preview"},
        input_data={
            "items": [
                {"id": "one", "text": "deployment checkpoint failed"},
                {"id": "two", "text": "quarterly spreadsheet imported"},
            ]
        },
        working_dir=str(tmp_path),
    )

    result = await SDKQueryStoreNode().execute(context)

    assert result.success is True
    assert result.items_processed == 2
    store = SQLiteCollection(tmp_path / "query" / f"{run_id}.db")
    try:
        hits = await store.query(QueryRequest(text="deployment"))
    finally:
        await store.close()
    assert len(hits) == 1
    assert hits[0].envelope.stream == "preview"


@pytest.mark.asyncio
async def test_authenticated_run_query_returns_sdk_envelopes(tmp_path):
    run_id = uuid4()
    graph_id = uuid4()
    owner_id = uuid4()
    context = NodeContext(
        run_id=str(run_id),
        node_id="query-store",
        input_data={"items": [{"id": "one", "text": "searchable pipeline output"}]},
        working_dir=str(tmp_path),
    )
    assert (await SDKQueryStoreNode().execute(context)).success

    owner_result = MagicMock()
    owner_result.scalar_one_or_none.return_value = owner_id
    db = AsyncMock()
    db.execute.return_value = owner_result
    run = SimpleNamespace(id=run_id, graph_id=graph_id)

    with (
        patch("app.api.query.get_run", new_callable=AsyncMock, return_value=run),
        patch("app.api.query.settings.temp_dir", str(tmp_path)),
    ):
        response = await query_run_output(
            run_id=run_id,
            q="pipeline",
            source=None,
            stream=None,
            limit=20,
            offset=0,
            db=db,
            current_user={"role": "user", "user_id": owner_id},
        )

    assert response["count"] == 1
    assert response["hits"][0]["envelope"]["source"] == "studio"
    assert response["hits"][0]["envelope"]["payload"]["data"]["text"] == (
        "searchable pipeline output"
    )


@pytest.mark.asyncio
async def test_run_query_rejects_punctuation_only_search(tmp_path):
    run_id = uuid4()
    owner_id = uuid4()
    context = NodeContext(
        run_id=str(run_id),
        node_id="query-store",
        input_data={"items": [{"id": "one", "text": "searchable"}]},
        working_dir=str(tmp_path),
    )
    assert (await SDKQueryStoreNode().execute(context)).success
    owner_result = MagicMock()
    owner_result.scalar_one_or_none.return_value = owner_id
    db = AsyncMock()
    db.execute.return_value = owner_result
    run = SimpleNamespace(id=run_id, graph_id=uuid4())

    with (
        patch("app.api.query.get_run", new_callable=AsyncMock, return_value=run),
        patch("app.api.query.settings.temp_dir", str(tmp_path)),
        pytest.raises(HTTPException) as exc_info,
    ):
        await query_run_output(
            run_id=run_id,
            q="!!!",
            source=None,
            stream=None,
            limit=20,
            offset=0,
            db=db,
            current_user={"role": "user", "user_id": owner_id},
        )

    assert exc_info.value.status_code == 422


@pytest.mark.asyncio
async def test_run_query_deletes_and_rejects_expired_artifact(tmp_path):
    run_id = uuid4()
    owner_id = uuid4()
    context = NodeContext(
        run_id=str(run_id),
        node_id="query-store",
        input_data={"items": [{"id": "one", "text": "expired"}]},
        working_dir=str(tmp_path),
    )
    assert (await SDKQueryStoreNode().execute(context)).success
    store_path = tmp_path / "query" / f"{run_id}.db"
    expired = (datetime.now(UTC) - timedelta(hours=2)).timestamp()
    os.utime(store_path, (expired, expired))
    owner_result = MagicMock()
    owner_result.scalar_one_or_none.return_value = owner_id
    db = AsyncMock()
    db.execute.return_value = owner_result
    run = SimpleNamespace(id=run_id, graph_id=uuid4())

    with (
        patch("app.api.query.get_run", new_callable=AsyncMock, return_value=run),
        patch("app.api.query.settings.temp_dir", str(tmp_path)),
        patch("app.api.query.settings.query_artifact_ttl_hours", 1),
        pytest.raises(HTTPException) as exc_info,
    ):
        await query_run_output(
            run_id=run_id,
            q=None,
            source=None,
            stream=None,
            limit=20,
            offset=0,
            db=db,
            current_user={"role": "user", "user_id": owner_id},
        )

    assert exc_info.value.status_code == 410
    assert not store_path.exists()


@pytest.mark.asyncio
async def test_runner_defaults_to_configured_temp_dir(tmp_path):
    from app.engine.retry import RetryConfig
    from app.engine.runner import run_node_with_retry
    from app.nodes.base import NodeResult

    observed = {}

    class CaptureNode:
        async def execute(self, context):
            observed["working_dir"] = context.working_dir
            return NodeResult(success=True)

    db = AsyncMock()
    db.add = MagicMock()
    with (
        patch("app.engine.runner.registry_get_node", return_value=CaptureNode()),
        patch("app.engine.runner.settings.temp_dir", str(tmp_path)),
    ):
        result = await run_node_with_retry(
            db=db,
            run_id=uuid4(),
            node_id="capture",
            node_type="capture",
            config={},
            input_data={},
            state={},
            retry_config=RetryConfig(max_retries=1, jitter=False),
        )

    assert result.status == "completed"
    assert observed["working_dir"] == str(tmp_path)
