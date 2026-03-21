"""
Phase 5 tests for ingestion-graph backend.

Tests:
- Lineage recording in the executor (mock DB, verify lineage entries created)
- Lineage service queries (mock DB)
- DLQ API endpoints (mock DB)
- DLQ retry (mock node execution)
- DLQ resolve (mock DB update)
- Run retry (re-run failed nodes from checkpoints)
- Run replay (full new run)
"""
import uuid
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.models.lineage import DataLineage, Provenance
from app.models.dead_letter import DeadLetterQueue
from app.models.execution import Run, RunStatus, RunNode, NodeStatus, Checkpoint, CheckpointType


# ---------------------------------------------------------------------------
# Helper fixtures and factories
# ---------------------------------------------------------------------------

def _make_uuid() -> uuid.UUID:
    return uuid.uuid4()


def _make_run(
    run_id: uuid.UUID | None = None,
    graph_id: uuid.UUID | None = None,
    status: str = RunStatus.FAILED.value,
    graph_version_id: uuid.UUID | None = None,
) -> MagicMock:
    """Create a mock Run object."""
    run = MagicMock()
    run.id = run_id or _make_uuid()
    run.graph_id = graph_id or _make_uuid()
    run.graph_version_id = graph_version_id or _make_uuid()
    run.status = status
    run.error_message = "Some error"
    return run


def _make_run_node(
    node_id: str = "node-1",
    node_type: str = "file_parser",
    status: str = NodeStatus.COMPLETED.value,
    output_data: dict | None = None,
    error_message: str | None = None,
) -> MagicMock:
    """Create a mock RunNode object."""
    node = MagicMock()
    node.id = _make_uuid()
    node.run_id = _make_uuid()
    node.node_id = node_id
    node.node_type = node_type
    node.status = status
    node.attempt_count = 3
    node.max_retries = 3
    node.input_data = {"input": "data"}
    node.output_data = output_data or {"result": "output"}
    node.items_processed = 10
    node.duration_ms = 100
    node.error_message = error_message
    return node


def _make_dlq_item(
    item_id: uuid.UUID | None = None,
    run_id: uuid.UUID | None = None,
    node_id: str = "node-1",
    node_type: str = "file_parser",
    error_type: str = "ValueError",
    error_message: str = "Bad value",
    input_data: dict | None = None,
    resolved: bool = False,
) -> MagicMock:
    """Create a mock DLQ item."""
    item = MagicMock()
    item.id = item_id or _make_uuid()
    item.run_id = run_id or _make_uuid()
    item.node_id = node_id
    item.node_type = node_type
    item.error_type = error_type
    item.error_message = error_message
    item.input_data = input_data or {"data": "bad input"}
    item.retry_count = 3
    item.resolved = resolved
    item.resolution_note = None
    item.created_at = datetime(2025, 1, 1, 12, 0, 0)
    item.updated_at = datetime(2025, 1, 1, 12, 0, 0)
    return item


def _make_checkpoint(
    node_id: str = "node-1",
    checkpoint_type: str = CheckpointType.POST_EXEC.value,
    node_output: dict | None = None,
) -> MagicMock:
    """Create a mock Checkpoint object."""
    cp = MagicMock()
    cp.id = _make_uuid()
    cp.run_id = _make_uuid()
    cp.node_id = node_id
    cp.checkpoint_type = checkpoint_type
    cp.state_data = {"level": 0}
    cp.node_output = node_output or {"result": "checkpointed"}
    return cp


def _make_lineage_entry(
    source_node_id: str = "node-a",
    target_node_id: str = "node-b",
) -> MagicMock:
    """Create a mock DataLineage entry."""
    entry = MagicMock()
    entry.id = _make_uuid()
    entry.run_id = _make_uuid()
    entry.source_node_id = source_node_id
    entry.target_node_id = target_node_id
    entry.source_port = "output"
    entry.target_port = "input"
    entry.items_count = 10
    entry.items_sample = ["a", "b", "c"]
    entry.bytes_transferred = 42
    entry.created_at = datetime(2025, 1, 1, 12, 0, 0)
    return entry


def _make_provenance(
    source_ref: str = "s3://bucket/file.csv",
) -> MagicMock:
    """Create a mock Provenance entry."""
    prov = MagicMock()
    prov.id = _make_uuid()
    prov.run_id = _make_uuid()
    prov.source_type = "s3"
    prov.source_ref = source_ref
    prov.output_target = "postgres://db/table"
    prov.records_affected = 100
    prov.metadata_ = {"format": "csv"}
    prov.created_at = datetime(2025, 1, 1, 12, 0, 0)
    return prov


# ===========================================================================
# Lineage Recording Tests
# ===========================================================================


class TestLineageRecording:
    """Tests for lineage recording in the executor."""

    @pytest.mark.asyncio
    async def test_record_lineage_with_list_data(self):
        """Test that lineage is recorded for list data."""
        from app.engine.executor import DAGExecutor

        mock_db = MagicMock()
        mock_db.commit = AsyncMock()
        executor = DAGExecutor(mock_db)
        run_id = _make_uuid()

        await executor._record_lineage(
            run_id=run_id,
            source_node_id="node-a",
            target_node_id="node-b",
            source_port="output",
            target_port="input",
            data=["item1", "item2", "item3", "item4", "item5"],
        )

        # Verify db.add was called
        assert mock_db.add.called
        mock_db.commit.assert_called_once()
        # Verify the added object has correct lineage attributes
        added_obj = mock_db.add.call_args[0][0]
        assert hasattr(added_obj, "run_id")
        assert added_obj.run_id == run_id
        assert added_obj.source_node_id == "node-a"
        assert added_obj.target_node_id == "node-b"
        assert added_obj.items_count == 5
        assert len(added_obj.items_sample) == 3

    @pytest.mark.asyncio
    async def test_record_lineage_with_dict_data(self):
        """Test that lineage is recorded for dict data."""
        from app.engine.executor import DAGExecutor

        mock_db = MagicMock()
        mock_db.commit = AsyncMock()
        executor = DAGExecutor(mock_db)
        run_id = _make_uuid()

        await executor._record_lineage(
            run_id=run_id,
            source_node_id="node-a",
            target_node_id="node-b",
            source_port="output",
            target_port="input",
            data={"key1": "val1", "key2": "val2", "key3": "val3", "key4": "val4"},
        )

        assert mock_db.add.called
        added_obj = mock_db.add.call_args[0][0]
        assert added_obj.items_count == 4
        assert isinstance(added_obj.items_sample, dict)

    @pytest.mark.asyncio
    async def test_record_lineage_with_none_data(self):
        """Test that lineage handles None data gracefully."""
        from app.engine.executor import DAGExecutor

        mock_db = MagicMock()
        mock_db.commit = AsyncMock()
        executor = DAGExecutor(mock_db)
        run_id = _make_uuid()

        await executor._record_lineage(
            run_id=run_id,
            source_node_id="node-a",
            target_node_id="node-b",
            source_port="output",
            target_port="input",
            data=None,
        )

        assert mock_db.add.called
        added_obj = mock_db.add.call_args[0][0]
        assert added_obj.items_count is None

    @pytest.mark.asyncio
    async def test_record_lineage_with_empty_list(self):
        """Test that lineage handles empty lists."""
        from app.engine.executor import DAGExecutor

        mock_db = MagicMock()
        mock_db.commit = AsyncMock()
        executor = DAGExecutor(mock_db)
        run_id = _make_uuid()

        await executor._record_lineage(
            run_id=run_id,
            source_node_id="node-a",
            target_node_id="node-b",
            source_port="output",
            target_port="input",
            data=[],
        )

        assert mock_db.add.called
        added_obj = mock_db.add.call_args[0][0]
        assert added_obj.items_count == 0
        assert added_obj.items_sample is None

    @pytest.mark.asyncio
    async def test_record_lineage_error_does_not_raise(self):
        """Test that lineage recording errors are caught and logged."""
        from app.engine.executor import DAGExecutor

        mock_db = AsyncMock()
        mock_db.add.side_effect = Exception("DB error")
        executor = DAGExecutor(mock_db)
        run_id = _make_uuid()

        # Should not raise
        await executor._record_lineage(
            run_id=run_id,
            source_node_id="node-a",
            target_node_id="node-b",
            source_port="output",
            target_port="input",
            data=["item1"],
        )


# ===========================================================================
# DLQ Recording Tests
# ===========================================================================


class TestDLQRecording:
    """Tests for dead letter queue recording in the executor."""

    @pytest.mark.asyncio
    async def test_record_dlq_calls_add_to_dlq(self):
        """Test that _record_dlq calls the dead_letter handler."""
        from app.engine.executor import DAGExecutor

        mock_db = AsyncMock()
        executor = DAGExecutor(mock_db)
        run_id = _make_uuid()

        with patch("app.engine.dead_letter.add_to_dlq", new_callable=AsyncMock) as mock_add:
            await executor._record_dlq(
                run_id=run_id,
                node_id="node-1",
                node_type="file_parser",
                error_type="ValueError",
                error_message="Bad data",
                input_data={"key": "value"},
                retry_count=3,
            )

            mock_add.assert_called_once_with(
                db=mock_db,
                run_id=run_id,
                node_id="node-1",
                node_type="file_parser",
                error_type="ValueError",
                error_message="Bad data",
                input_data={"key": "value"},
                retry_count=3,
            )

    @pytest.mark.asyncio
    async def test_record_dlq_error_does_not_raise(self):
        """Test that DLQ recording errors are caught and logged."""
        from app.engine.executor import DAGExecutor

        mock_db = AsyncMock()
        executor = DAGExecutor(mock_db)

        with patch(
            "app.engine.dead_letter.add_to_dlq",
            new_callable=AsyncMock,
            side_effect=Exception("DB error"),
        ):
            # Should not raise
            await executor._record_dlq(
                run_id=_make_uuid(),
                node_id="node-1",
                node_type="file_parser",
                error_type="ValueError",
                error_message="Bad data",
                input_data={},
                retry_count=3,
            )


# ===========================================================================
# _collect_inputs Tests
# ===========================================================================


class TestCollectInputs:
    """Tests for the executor's _collect_inputs method."""

    def test_collect_inputs_returns_tuple(self):
        """Test that _collect_inputs returns (inputs, lineage_edges)."""
        from app.engine.executor import DAGExecutor

        mock_db = AsyncMock()
        executor = DAGExecutor(mock_db)

        exec_state = {
            "outputs": {
                "source-node": {"documents": [{"text": "hello"}]},
            }
        }
        edges = [
            {"source": "source-node", "target": "target-node", "source_port": "output", "target_port": "input"}
        ]

        inputs, lineage_edges = executor._collect_inputs("target-node", edges, exec_state)

        assert isinstance(inputs, dict)
        assert isinstance(lineage_edges, list)
        assert "input" in inputs
        assert len(lineage_edges) == 1
        assert lineage_edges[0]["source_node_id"] == "source-node"
        assert lineage_edges[0]["target_node_id"] == "target-node"

    def test_collect_inputs_empty_no_predecessors(self):
        """Test _collect_inputs with no predecessor outputs."""
        from app.engine.executor import DAGExecutor

        mock_db = AsyncMock()
        executor = DAGExecutor(mock_db)

        inputs, lineage_edges = executor._collect_inputs("target-node", [], {"outputs": {}})

        assert inputs == {}
        assert lineage_edges == []

    def test_collect_inputs_multiple_predecessors(self):
        """Test _collect_inputs with multiple predecessor nodes."""
        from app.engine.executor import DAGExecutor

        mock_db = AsyncMock()
        executor = DAGExecutor(mock_db)

        exec_state = {
            "outputs": {
                "source-a": {"documents": [{"text": "from a"}]},
                "source-b": [{"text": "from b"}],
            }
        }
        edges = [
            {"source": "source-a", "target": "merge-node", "source_port": "documents", "target_port": "input_a"},
            {"source": "source-b", "target": "merge-node", "source_port": "output", "target_port": "input_b"},
        ]

        inputs, lineage_edges = executor._collect_inputs("merge-node", edges, exec_state)

        assert "input_a" in inputs
        assert "input_b" in inputs
        assert len(lineage_edges) == 2


# ===========================================================================
# Lineage Service Tests
# ===========================================================================


class TestLineageService:
    """Tests for the lineage service."""

    @pytest.mark.asyncio
    async def test_get_lineage_for_run(self):
        """Test getting lineage entries for a run."""
        from app.services.lineage_service import get_lineage_for_run

        mock_db = AsyncMock()
        run_id = _make_uuid()
        entries = [_make_lineage_entry(), _make_lineage_entry("node-b", "node-c")]

        mock_result = MagicMock()
        mock_scalars = MagicMock()
        mock_scalars.all.return_value = entries
        mock_result.scalars.return_value = mock_scalars
        mock_db.execute = AsyncMock(return_value=mock_result)

        result = await get_lineage_for_run(mock_db, run_id)

        assert len(result) == 2
        mock_db.execute.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_lineage_for_graph(self):
        """Test getting lineage entries across all runs for a graph."""
        from app.services.lineage_service import get_lineage_for_graph

        mock_db = AsyncMock()
        graph_id = _make_uuid()
        entries = [_make_lineage_entry()]

        mock_result = MagicMock()
        mock_scalars = MagicMock()
        mock_scalars.all.return_value = entries
        mock_result.scalars.return_value = mock_scalars
        mock_db.execute = AsyncMock(return_value=mock_result)

        result = await get_lineage_for_graph(mock_db, graph_id, limit=50)

        assert len(result) == 1
        mock_db.execute.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_lineage_for_source_no_provenance(self):
        """Test getting lineage for a source with no provenance records."""
        from app.services.lineage_service import get_lineage_for_source

        mock_db = AsyncMock()

        # No provenance records found
        mock_result = MagicMock()
        mock_scalars = MagicMock()
        mock_scalars.all.return_value = []
        mock_result.scalars.return_value = mock_scalars
        mock_db.execute = AsyncMock(return_value=mock_result)

        result = await get_lineage_for_source(mock_db, "s3://bucket/nonexistent")

        assert result == []

    @pytest.mark.asyncio
    async def test_get_lineage_for_source_with_provenance(self):
        """Test getting lineage for a source that has provenance records."""
        from app.services.lineage_service import get_lineage_for_source

        mock_db = AsyncMock()
        run_id = _make_uuid()
        prov = _make_provenance("s3://bucket/file.csv")
        prov.run_id = run_id

        lineage_entries = [_make_lineage_entry()]
        lineage_entries[0].run_id = run_id

        # First call returns provenance, second call returns lineage
        mock_prov_result = MagicMock()
        mock_prov_scalars = MagicMock()
        mock_prov_scalars.all.return_value = [prov]
        mock_prov_result.scalars.return_value = mock_prov_scalars

        mock_lineage_result = MagicMock()
        mock_lineage_scalars = MagicMock()
        mock_lineage_scalars.all.return_value = lineage_entries
        mock_lineage_result.scalars.return_value = mock_lineage_scalars

        mock_db.execute = AsyncMock(side_effect=[mock_prov_result, mock_lineage_result])

        result = await get_lineage_for_source(mock_db, "s3://bucket/file.csv")

        assert len(result) == 1
        assert result[0]["provenance"] == prov
        assert len(result[0]["lineage"]) == 1

    @pytest.mark.asyncio
    async def test_get_provenance_for_run(self):
        """Test getting provenance records for a run."""
        from app.services.lineage_service import get_provenance_for_run

        mock_db = AsyncMock()
        run_id = _make_uuid()
        prov_entries = [_make_provenance(), _make_provenance("s3://bucket/file2.csv")]

        mock_result = MagicMock()
        mock_scalars = MagicMock()
        mock_scalars.all.return_value = prov_entries
        mock_result.scalars.return_value = mock_scalars
        mock_db.execute = AsyncMock(return_value=mock_result)

        result = await get_provenance_for_run(mock_db, run_id)

        assert len(result) == 2

    @pytest.mark.asyncio
    async def test_record_provenance(self):
        """Test creating a provenance record."""
        from app.services.lineage_service import record_provenance

        mock_db = AsyncMock()
        run_id = _make_uuid()

        with patch("app.services.lineage_service.Provenance") as MockProvenance:
            mock_instance = MagicMock()
            mock_instance.id = _make_uuid()
            MockProvenance.return_value = mock_instance

            result = await record_provenance(
                mock_db,
                run_id=run_id,
                source_type="s3",
                source_ref="s3://bucket/file.csv",
                output_target="postgres://db/table",
                records_affected=100,
            )

        mock_db.add.assert_called_once()
        mock_db.commit.assert_called_once()
        mock_db.refresh.assert_called_once()


# ===========================================================================
# DLQ API Tests
# ===========================================================================


class TestDLQAPI:
    """Tests for the dead letter queue API endpoints."""

    @pytest.mark.asyncio
    async def test_list_dlq_items_all(self):
        """Test listing all DLQ items."""
        from app.api.dead_letter import list_dlq_items

        mock_db = AsyncMock()
        items = [_make_dlq_item(), _make_dlq_item(node_id="node-2")]

        # Mock count query and items query
        mock_count_result = MagicMock()
        mock_count_result.scalar.return_value = 2

        mock_items_result = MagicMock()
        mock_scalars = MagicMock()
        mock_scalars.all.return_value = items
        mock_items_result.scalars.return_value = mock_scalars

        mock_db.execute = AsyncMock(side_effect=[mock_count_result, mock_items_result])

        result = await list_dlq_items(
            resolved=None,
            node_type=None,
            db=mock_db,
            current_user={"user_id": _make_uuid()},
        )

        assert result["total"] == 2
        assert len(result["items"]) == 2

    @pytest.mark.asyncio
    async def test_list_dlq_items_filter_resolved(self):
        """Test listing DLQ items filtered by resolved status."""
        from app.api.dead_letter import list_dlq_items

        mock_db = AsyncMock()
        items = [_make_dlq_item(resolved=True)]

        mock_count_result = MagicMock()
        mock_count_result.scalar.return_value = 1

        mock_items_result = MagicMock()
        mock_scalars = MagicMock()
        mock_scalars.all.return_value = items
        mock_items_result.scalars.return_value = mock_scalars

        mock_db.execute = AsyncMock(side_effect=[mock_count_result, mock_items_result])

        result = await list_dlq_items(
            resolved=True,
            node_type=None,
            db=mock_db,
            current_user={"user_id": _make_uuid()},
        )

        assert result["total"] == 1
        assert result["items"][0]["resolved"] is True

    @pytest.mark.asyncio
    async def test_list_dlq_items_filter_node_type(self):
        """Test listing DLQ items filtered by node type."""
        from app.api.dead_letter import list_dlq_items

        mock_db = AsyncMock()
        items = [_make_dlq_item(node_type="embedder")]

        mock_count_result = MagicMock()
        mock_count_result.scalar.return_value = 1

        mock_items_result = MagicMock()
        mock_scalars = MagicMock()
        mock_scalars.all.return_value = items
        mock_items_result.scalars.return_value = mock_scalars

        mock_db.execute = AsyncMock(side_effect=[mock_count_result, mock_items_result])

        result = await list_dlq_items(
            resolved=None,
            node_type="embedder",
            db=mock_db,
            current_user={"user_id": _make_uuid()},
        )

        assert result["total"] == 1
        assert result["items"][0]["node_type"] == "embedder"

    @pytest.mark.asyncio
    async def test_resolve_dlq_item(self):
        """Test resolving a DLQ item."""
        from app.api.dead_letter import resolve_dlq_item, DLQResolveRequest

        mock_db = AsyncMock()
        item = _make_dlq_item()
        item.resolved = False

        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = item
        mock_db.execute = AsyncMock(return_value=mock_result)

        request = DLQResolveRequest(note="Fixed the issue")

        result = await resolve_dlq_item(
            item_id=item.id,
            request=request,
            db=mock_db,
            current_user={"user_id": _make_uuid()},
        )

        assert result["resolved"] is True
        assert result["resolution_note"] == "Fixed the issue"
        mock_db.commit.assert_called_once()

    @pytest.mark.asyncio
    async def test_resolve_dlq_item_not_found(self):
        """Test resolving a non-existent DLQ item."""
        from fastapi import HTTPException
        from app.api.dead_letter import resolve_dlq_item, DLQResolveRequest

        mock_db = AsyncMock()
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = None
        mock_db.execute = AsyncMock(return_value=mock_result)

        request = DLQResolveRequest(note="Fixed")

        with pytest.raises(HTTPException) as exc_info:
            await resolve_dlq_item(
                item_id=_make_uuid(),
                request=request,
                db=mock_db,
                current_user={"user_id": _make_uuid()},
            )

        assert exc_info.value.status_code == 404

    @pytest.mark.asyncio
    async def test_resolve_dlq_item_already_resolved(self):
        """Test resolving an already-resolved item raises error."""
        from fastapi import HTTPException
        from app.api.dead_letter import resolve_dlq_item, DLQResolveRequest

        mock_db = AsyncMock()
        item = _make_dlq_item(resolved=True)
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = item
        mock_db.execute = AsyncMock(return_value=mock_result)

        request = DLQResolveRequest(note="Already fixed")

        with pytest.raises(HTTPException) as exc_info:
            await resolve_dlq_item(
                item_id=item.id,
                request=request,
                db=mock_db,
                current_user={"user_id": _make_uuid()},
            )

        assert exc_info.value.status_code == 400

    @pytest.mark.asyncio
    async def test_delete_dlq_item(self):
        """Test deleting a DLQ item."""
        from app.api.dead_letter import delete_dlq_item

        mock_db = AsyncMock()
        item = _make_dlq_item()

        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = item
        mock_db.execute = AsyncMock(return_value=mock_result)

        await delete_dlq_item(
            item_id=item.id,
            db=mock_db,
            current_user={"user_id": _make_uuid()},
        )

        mock_db.delete.assert_called_once_with(item)
        mock_db.commit.assert_called_once()

    @pytest.mark.asyncio
    async def test_delete_dlq_item_not_found(self):
        """Test deleting a non-existent DLQ item."""
        from fastapi import HTTPException
        from app.api.dead_letter import delete_dlq_item

        mock_db = AsyncMock()
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = None
        mock_db.execute = AsyncMock(return_value=mock_result)

        with pytest.raises(HTTPException) as exc_info:
            await delete_dlq_item(
                item_id=_make_uuid(),
                db=mock_db,
                current_user={"user_id": _make_uuid()},
            )

        assert exc_info.value.status_code == 404


# ===========================================================================
# DLQ Retry Tests
# ===========================================================================


class TestDLQRetry:
    """Tests for the DLQ retry endpoint."""

    @pytest.mark.asyncio
    async def test_retry_dlq_item_success(self):
        """Test successful retry of a DLQ item."""
        from app.api.dead_letter import retry_dlq_item
        from app.nodes.base import NodeResult

        mock_db = AsyncMock()
        item = _make_dlq_item()

        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = item
        mock_db.execute = AsyncMock(return_value=mock_result)

        mock_node_impl = MagicMock()
        mock_node_impl.execute = AsyncMock(return_value=NodeResult(
            success=True,
            output_data={"result": "retried"},
            items_processed=5,
        ))

        with patch("app.nodes.registry.get_node", return_value=mock_node_impl):
            result = await retry_dlq_item(
                item_id=item.id,
                db=mock_db,
                current_user={"user_id": _make_uuid()},
            )

        assert result["success"] is True
        assert item.resolved is True
        assert "Retry succeeded" in item.resolution_note

    @pytest.mark.asyncio
    async def test_retry_dlq_item_failure(self):
        """Test retry of a DLQ item that still fails."""
        from fastapi import HTTPException
        from app.api.dead_letter import retry_dlq_item
        from app.nodes.base import NodeResult

        mock_db = AsyncMock()
        item = _make_dlq_item()

        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = item
        mock_db.execute = AsyncMock(return_value=mock_result)

        mock_node_impl = MagicMock()
        mock_node_impl.execute = AsyncMock(return_value=NodeResult(
            success=False,
            error_message="Still broken",
        ))

        with patch("app.nodes.registry.get_node", return_value=mock_node_impl):
            with pytest.raises(HTTPException) as exc_info:
                await retry_dlq_item(
                    item_id=item.id,
                    db=mock_db,
                    current_user={"user_id": _make_uuid()},
                )

        assert exc_info.value.status_code == 422
        assert item.retry_count == 4  # Incremented from 3

    @pytest.mark.asyncio
    async def test_retry_dlq_item_not_found(self):
        """Test retry of a non-existent DLQ item."""
        from fastapi import HTTPException
        from app.api.dead_letter import retry_dlq_item

        mock_db = AsyncMock()
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = None
        mock_db.execute = AsyncMock(return_value=mock_result)

        with pytest.raises(HTTPException) as exc_info:
            await retry_dlq_item(
                item_id=_make_uuid(),
                db=mock_db,
                current_user={"user_id": _make_uuid()},
            )

        assert exc_info.value.status_code == 404

    @pytest.mark.asyncio
    async def test_retry_dlq_item_already_resolved(self):
        """Test retry of an already-resolved item raises error."""
        from fastapi import HTTPException
        from app.api.dead_letter import retry_dlq_item

        mock_db = AsyncMock()
        item = _make_dlq_item(resolved=True)

        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = item
        mock_db.execute = AsyncMock(return_value=mock_result)

        with pytest.raises(HTTPException) as exc_info:
            await retry_dlq_item(
                item_id=item.id,
                db=mock_db,
                current_user={"user_id": _make_uuid()},
            )

        assert exc_info.value.status_code == 400

    @pytest.mark.asyncio
    async def test_retry_dlq_item_no_input_data(self):
        """Test retry of a DLQ item with no input data raises error."""
        from fastapi import HTTPException
        from app.api.dead_letter import retry_dlq_item

        mock_db = AsyncMock()
        item = _make_dlq_item()
        item.input_data = None

        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = item
        mock_db.execute = AsyncMock(return_value=mock_result)

        with pytest.raises(HTTPException) as exc_info:
            await retry_dlq_item(
                item_id=item.id,
                db=mock_db,
                current_user={"user_id": _make_uuid()},
            )

        assert exc_info.value.status_code == 400


# ===========================================================================
# Run Retry Tests
# ===========================================================================


class TestRunRetry:
    """Tests for the run retry endpoint."""

    @pytest.mark.asyncio
    async def test_retry_failed_run(self):
        """Test retrying a failed run."""
        from app.api.executions import retry_failed_run

        mock_db = AsyncMock()
        run = _make_run(status=RunStatus.FAILED.value)
        run.run_nodes = []

        # Mock get_run
        mock_run_result = MagicMock()
        mock_run_result.scalar_one_or_none.return_value = run
        mock_db.execute = AsyncMock(return_value=mock_run_result)

        # Mock BackgroundTasks
        mock_bg_tasks = MagicMock()

        with patch("app.api.executions.get_run", new_callable=AsyncMock, return_value=run):
            with patch("app.api.executions.get_graph_versions", new_callable=AsyncMock, return_value=[]):
                # Need to mock the graph version lookup inside the endpoint
                mock_version_result = MagicMock()
                mock_version = MagicMock()
                mock_version.nodes_data = {"node-1": {"id": "node-1", "type": "file_source"}}
                mock_version.edges_data = []
                mock_version.node_configs = {}
                mock_version_result.scalar_one_or_none.return_value = mock_version

                # Override execute for the graph version query
                original_execute = mock_db.execute

                call_count = [0]
                async def execute_side_effect(query):
                    call_count[0] += 1
                    if call_count[0] == 1:
                        return mock_run_result
                    return mock_version_result

                mock_db.execute = AsyncMock(side_effect=execute_side_effect)

                result = await retry_failed_run(
                    run_id=run.id,
                    background_tasks=mock_bg_tasks,
                    db=mock_db,
                    current_user={"user_id": _make_uuid()},
                )

        assert result.status == "pending"
        assert result.error_message is None
        mock_bg_tasks.add_task.assert_called_once()

    @pytest.mark.asyncio
    async def test_retry_run_not_failed(self):
        """Test retrying a run that is not in failed status."""
        from fastapi import HTTPException
        from app.api.executions import retry_failed_run

        mock_db = AsyncMock()
        run = _make_run(status=RunStatus.COMPLETED.value)

        with patch("app.api.executions.get_run", new_callable=AsyncMock, return_value=run):
            with pytest.raises(HTTPException) as exc_info:
                await retry_failed_run(
                    run_id=run.id,
                    background_tasks=MagicMock(),
                    db=mock_db,
                    current_user={"user_id": _make_uuid()},
                )

        assert exc_info.value.status_code == 400

    @pytest.mark.asyncio
    async def test_retry_run_not_found(self):
        """Test retrying a non-existent run."""
        from fastapi import HTTPException
        from app.api.executions import retry_failed_run

        mock_db = AsyncMock()

        with patch("app.api.executions.get_run", new_callable=AsyncMock, return_value=None):
            with pytest.raises(HTTPException) as exc_info:
                await retry_failed_run(
                    run_id=_make_uuid(),
                    background_tasks=MagicMock(),
                    db=mock_db,
                    current_user={"user_id": _make_uuid()},
                )

        assert exc_info.value.status_code == 404


# ===========================================================================
# Run Replay Tests
# ===========================================================================


class TestRunReplay:
    """Tests for the run replay endpoint."""

    @pytest.mark.asyncio
    async def test_replay_run(self):
        """Test replaying a run creates a new run."""
        from app.api.executions import replay_run

        mock_db = AsyncMock()
        run = _make_run(status=RunStatus.COMPLETED.value)

        new_run = _make_run(status=RunStatus.PENDING.value)
        new_run.error_message = None

        # Mock the graph version lookup inside the endpoint
        mock_version = MagicMock()
        mock_version.nodes_data = {"node-1": {"id": "node-1", "type": "file_source"}}
        mock_version.edges_data = []
        mock_version.node_configs = {}

        mock_db_result = MagicMock()
        mock_db_result.scalar_one_or_none.return_value = mock_version
        mock_db.execute = AsyncMock(return_value=mock_db_result)

        with patch("app.api.executions.get_run", new_callable=AsyncMock, return_value=run):
            with patch("app.api.executions.create_run", new_callable=AsyncMock, return_value=new_run):
                mock_bg_tasks = MagicMock()

                result = await replay_run(
                    run_id=run.id,
                    background_tasks=mock_bg_tasks,
                    db=mock_db,
                    current_user={"user_id": _make_uuid()},
                )

        assert result is new_run
        mock_bg_tasks.add_task.assert_called_once()

    @pytest.mark.asyncio
    async def test_replay_run_not_found(self):
        """Test replaying a non-existent run."""
        from fastapi import HTTPException
        from app.api.executions import replay_run

        mock_db = AsyncMock()

        with patch("app.api.executions.get_run", new_callable=AsyncMock, return_value=None):
            with pytest.raises(HTTPException) as exc_info:
                await replay_run(
                    run_id=_make_uuid(),
                    background_tasks=MagicMock(),
                    db=mock_db,
                    current_user={"user_id": _make_uuid()},
                )

        assert exc_info.value.status_code == 404


# ===========================================================================
# Lineage API Tests
# ===========================================================================


class TestLineageAPI:
    """Tests for the lineage API endpoints."""

    @pytest.mark.asyncio
    async def test_list_lineage_for_run(self):
        """Test the lineage/run endpoint."""
        from app.api.lineage import list_lineage_for_run

        mock_db = AsyncMock()
        run_id = _make_uuid()
        entries = [_make_lineage_entry()]

        mock_result = MagicMock()
        mock_scalars = MagicMock()
        mock_scalars.all.return_value = entries
        mock_result.scalars.return_value = mock_scalars
        mock_db.execute = AsyncMock(return_value=mock_result)

        result = await list_lineage_for_run(
            run_id=run_id,
            db=mock_db,
            current_user={"user_id": _make_uuid()},
        )

        assert result["run_id"] == str(run_id)
        assert result["total"] == 1
        assert len(result["lineage"]) == 1
        assert result["lineage"][0]["source_node_id"] == "node-a"

    @pytest.mark.asyncio
    async def test_list_lineage_for_graph(self):
        """Test the lineage/graph endpoint."""
        from app.api.lineage import list_lineage_for_graph

        mock_db = AsyncMock()
        graph_id = _make_uuid()
        entries = [_make_lineage_entry()]

        mock_result = MagicMock()
        mock_scalars = MagicMock()
        mock_scalars.all.return_value = entries
        mock_result.scalars.return_value = mock_scalars
        mock_db.execute = AsyncMock(return_value=mock_result)

        result = await list_lineage_for_graph(
            graph_id=graph_id,
            limit=100,
            db=mock_db,
            current_user={"user_id": _make_uuid()},
        )

        assert result["graph_id"] == str(graph_id)
        assert result["total"] == 1

    @pytest.mark.asyncio
    async def test_list_lineage_for_source(self):
        """Test the lineage/source endpoint."""
        from app.api.lineage import list_lineage_for_source

        mock_db = AsyncMock()

        run_id = _make_uuid()
        prov = _make_provenance("s3://bucket/file.csv")
        prov.run_id = run_id

        lineage_entry = _make_lineage_entry()
        lineage_entry.run_id = run_id

        mock_prov_result = MagicMock()
        mock_prov_scalars = MagicMock()
        mock_prov_scalars.all.return_value = [prov]
        mock_prov_result.scalars.return_value = mock_prov_scalars

        mock_lineage_result = MagicMock()
        mock_lineage_scalars = MagicMock()
        mock_lineage_scalars.all.return_value = [lineage_entry]
        mock_lineage_result.scalars.return_value = mock_lineage_scalars

        mock_db.execute = AsyncMock(side_effect=[mock_prov_result, mock_lineage_result])

        result = await list_lineage_for_source(
            source_ref="s3://bucket/file.csv",
            db=mock_db,
            current_user={"user_id": _make_uuid()},
        )

        assert result["source_ref"] == "s3://bucket/file.csv"
        assert result["total"] == 1

    @pytest.mark.asyncio
    async def test_list_provenance_for_run(self):
        """Test the provenance/run endpoint."""
        from app.api.lineage import list_provenance_for_run

        mock_db = AsyncMock()
        run_id = _make_uuid()
        prov_entries = [_make_provenance()]

        mock_result = MagicMock()
        mock_scalars = MagicMock()
        mock_scalars.all.return_value = prov_entries
        mock_result.scalars.return_value = mock_scalars
        mock_db.execute = AsyncMock(return_value=mock_result)

        result = await list_provenance_for_run(
            run_id=run_id,
            db=mock_db,
            current_user={"user_id": _make_uuid()},
        )

        assert result["run_id"] == str(run_id)
        assert result["total"] == 1
        assert result["provenance"][0]["source_type"] == "s3"


# ===========================================================================
# Dead Letter Handler Tests
# ===========================================================================


class TestDeadLetterHandler:
    """Tests for the dead_letter engine module."""

    @pytest.mark.asyncio
    async def test_add_to_dlq(self):
        """Test adding an item to the dead letter queue."""
        from app.engine.dead_letter import add_to_dlq

        mock_db = AsyncMock()
        run_id = _make_uuid()

        with patch("app.engine.dead_letter.DeadLetterQueue") as MockDLQ:
            mock_instance = MagicMock()
            mock_instance.id = _make_uuid()
            MockDLQ.return_value = mock_instance

            result = await add_to_dlq(
                db=mock_db,
                run_id=run_id,
                node_id="node-1",
                node_type="file_parser",
                error_type="ValueError",
                error_message="Bad value",
                input_data={"key": "value"},
                retry_count=3,
            )

        mock_db.add.assert_called_once()
        mock_db.commit.assert_called_once()
        mock_db.refresh.assert_called_once()
        assert result is not None
