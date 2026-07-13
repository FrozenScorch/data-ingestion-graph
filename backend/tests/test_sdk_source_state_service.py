from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest
from app.services.sdk_source_state_service import StudioSDKSourceStateStore


def test_state_pipeline_scope_includes_owner_graph_and_node():
    owner_a, owner_b, graph_id = uuid4(), uuid4(), uuid4()
    first = StudioSDKSourceStateStore(
        AsyncMock(), owner_id=owner_a, graph_id=graph_id, node_id="documents"
    )
    second = StudioSDKSourceStateStore(
        AsyncMock(), owner_id=owner_b, graph_id=graph_id, node_id="documents"
    )
    assert first.pipeline_key != second.pipeline_key
    with pytest.raises(ValueError, match="outside"):
        first._require_pipeline(second.pipeline_key)


@pytest.mark.asyncio
async def test_state_load_query_contains_every_owner_scope_dimension():
    owner_id, graph_id = uuid4(), uuid4()
    result = MagicMock()
    result.scalar_one_or_none.return_value = None
    session = AsyncMock()
    session.execute.return_value = result
    store = StudioSDKSourceStateStore(
        session, owner_id=owner_id, graph_id=graph_id, node_id="documents"
    )

    assert await store.load(store.pipeline_key, "local_documents", "upload-artifact") == {}
    statement = session.execute.await_args.args[0]
    parameter_values = set(statement.compile().params.values())
    assert owner_id in parameter_values
    assert graph_id in parameter_values
    assert "documents" in parameter_values
    assert "local_documents" in parameter_values
    assert "upload-artifact" in parameter_values
