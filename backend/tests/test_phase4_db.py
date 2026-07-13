"""
Phase 4 tests for ingestion-graph backend.

Tests:
- DatabaseSourceNode processes SQL query (mock DB connection)
- DatabaseWriterNode inserts rows (mock DB connection)
- VectorStoreNode stores embeddings and creates index (mock asyncpg)
- Connection service CRUD operations (mock DB session)
- Connection test endpoint (mock DB connection)
"""

import uuid
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from app.engine.executor import DAGExecutor
from app.nodes.base import NodeContext, PortDataType
from app.nodes.database_source import DatabaseSourceNode
from app.nodes.db_writer import DatabaseWriterNode
from app.nodes.vector_store import VectorStoreNode
from ingestion_graph.connectors import CheckResult, StreamDescriptor
from ingestion_graph.messages import RecordMessage, StateMessage
from ingestion_graph.models import Envelope, RecordPayload

# ---------------------------------------------------------------------------
# Helper fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def sample_connection_config():
    return {
        "host": "localhost",
        "port": 5432,
        "database": "testdb",
        "username": "testuser",
        "password": "testpass",
    }


@pytest.fixture
def base_context(sample_connection_config):
    """Create a base NodeContext with common fields."""
    return NodeContext(
        run_id=str(uuid.uuid4()),
        node_id="test-node-1",
        config={
            "connection_id": "conn-123",
        },
        input_data={},
        state={"connections": {"conn-123": sample_connection_config}},
    )


def _mock_postgres_source(rows_data, columns, type_hints=None):
    connector = MagicMock()
    connector.check = AsyncMock(return_value=CheckResult(True))
    connector.discover = AsyncMock(
        return_value=[
            StreamDescriptor(
                "studio_query",
                json_schema={
                    "type": "object",
                    "properties": {column: {} for column in columns},
                },
            )
        ]
    )

    async def read(stream, state):
        del stream, state
        for index, row in enumerate(rows_data):
            yield RecordMessage(
                Envelope(
                    id=str(index),
                    source="postgres",
                    stream="studio_query",
                    payload=RecordPayload(row),
                    metadata={
                        "ingestion_graph.postgres_types": (
                            type_hints[index] if type_hints is not None else {}
                        )
                    },
                )
            )
        yield StateMessage("studio_query", {})

    connector.read = read
    return connector


def _mock_postgres_destination(*, written=0):
    connector = MagicMock()
    connector.write = AsyncMock(return_value=written)
    connector.replace = AsyncMock(return_value=written)
    connector.flush = AsyncMock()
    return connector


# ===========================================================================
# DatabaseSourceNode Tests
# ===========================================================================


class TestDatabaseSourceNode:
    """Tests for the DatabaseSourceNode."""

    def test_node_metadata(self):
        """Test node type, category, and port definitions."""
        node = DatabaseSourceNode()
        assert node.node_type == "database_source"
        assert node.display_name == "Database Source"
        assert node.category == "source"
        assert len(node.inputs) == 0  # Source node has no inputs
        assert len(node.outputs) == 1
        assert node.outputs[0].data_type == PortDataType.TABLE

    def test_config_schema(self):
        """Test that config schema has required fields."""
        node = DatabaseSourceNode()
        schema = node.config_schema
        assert "connection_id" in schema["properties"]
        assert "query" in schema["properties"]
        assert "batch_size" in schema["properties"]
        assert "connection_id" in schema["properties"]
        assert "query" in schema["required"]
        assert "connection_id" in schema["required"]

    @pytest.mark.asyncio
    async def test_inline_credentials_are_not_accepted(self, sample_connection_config):
        node = DatabaseSourceNode()
        context = NodeContext(
            run_id=str(uuid.uuid4()),
            node_id="source",
            config={**sample_connection_config, "query": "SELECT 1"},
            input_data={},
            state={},
        )

        errors = await node.validate_config(context.config)
        assert "connection_id" in str(errors)
        with pytest.raises(ValueError, match="requires connection_id"):
            node._build_connection_url(context)

    def test_rejects_ctes_and_multiple_statements(self):
        node = DatabaseSourceNode()
        with pytest.raises(ValueError, match="Only SELECT"):
            node._validate_query(
                "WITH changed AS (DELETE FROM users RETURNING *) SELECT * FROM changed"
            )
        with pytest.raises(ValueError, match="Multiple SQL"):
            node._validate_query("SELECT 1; DELETE FROM users")

    @pytest.mark.asyncio
    async def test_execute_returns_rows(self, base_context):
        """Test that execute returns rows from a SQL query."""
        node = DatabaseSourceNode()
        base_context.config["query"] = "SELECT id, name FROM users LIMIT 10"
        base_context.config["batch_size"] = 100

        connector = _mock_postgres_source(
            [{"id": 1, "name": "Alice"}],
            ["id", "name"],
        )
        with patch("app.nodes.database_source.PostgresSource", return_value=connector):
            result = await node.execute(base_context)

        assert result.success is True
        assert result.output_data["row_count"] == 1
        assert len(result.output_data["rows"]) == 1
        assert result.output_data["rows"][0] == {"id": 1, "name": "Alice"}
        assert result.output_data["columns"] == ["id", "name"]

    @pytest.mark.asyncio
    async def test_execute_respects_batch_size(self, base_context):
        """Test that execute respects the batch_size limit."""
        node = DatabaseSourceNode()
        base_context.config["query"] = "SELECT id FROM items"
        base_context.config["batch_size"] = 2

        connector = _mock_postgres_source([{"id": 0}, {"id": 1}], ["id"])
        with patch("app.nodes.database_source.PostgresSource", return_value=connector) as sdk:
            result = await node.execute(base_context)

        assert result.success is True
        assert result.output_data["row_count"] == 2
        assert result.items_processed == 2
        assert sdk.call_args.kwargs["max_records"] == 2

    @pytest.mark.asyncio
    async def test_execute_preserves_postgres_type_hint_sidecar(self, base_context):
        node = DatabaseSourceNode()
        base_context.config["query"] = "SELECT occurred_at FROM items"
        connector = _mock_postgres_source(
            [{"occurred_at": "2026-01-02T03:04:05+00:00"}],
            ["occurred_at"],
            [{"occurred_at": "datetime"}],
        )

        with patch("app.nodes.database_source.PostgresSource", return_value=connector):
            result = await node.execute(base_context)

        assert result.output_data["postgres_type_hints"] == [{"occurred_at": "datetime"}]

    @pytest.mark.asyncio
    async def test_execute_non_select_query(self, base_context):
        """Test that non-SELECT queries are rejected for security."""
        node = DatabaseSourceNode()
        base_context.config["query"] = "INSERT INTO logs (msg) VALUES ('test')"

        result = await node.execute(base_context)

        assert result.success is False
        assert "SELECT" in result.error_message

    @pytest.mark.asyncio
    async def test_execute_db_error(self, base_context):
        """Test that database errors are handled gracefully."""
        node = DatabaseSourceNode()
        base_context.config["query"] = "SELECT 1"

        connector = _mock_postgres_source([], [])
        connector.check = AsyncMock(side_effect=Exception("Connection refused with password"))
        with patch("app.nodes.database_source.PostgresSource", return_value=connector):
            result = await node.execute(base_context)

        assert result.success is False
        assert result.error_message == "Database query failed: Exception"
        assert "Connection refused" not in result.error_message

    @pytest.mark.asyncio
    async def test_execute_uses_state_connection(self, base_context):
        """Test that saved connection config from state is used."""
        node = DatabaseSourceNode()
        base_context.config = {"connection_id": "saved-conn-1", "query": "SELECT 1"}
        base_context.state["connections"] = {
            "saved-conn-1": {
                "host": "saved-host",
                "port": 5433,
                "database": "saved_db",
                "username": "saved_user",
                "password": "saved_pass",
            }
        }

        connector = _mock_postgres_source([], ["col1"])
        with patch("app.nodes.database_source.PostgresSource", return_value=connector) as sdk:
            await node.execute(base_context)

        assert sdk.call_args.args[:4] == ("saved-host", 5433, "saved_db", "saved_user")
        assert "saved_pass" not in repr(sdk.call_args)

    @pytest.mark.asyncio
    async def test_validate_config(self):
        """Test config validation."""
        node = DatabaseSourceNode()
        errors = await node.validate_config({})
        assert "query" in str(errors)

        errors = await node.validate_config(
            {
                "connection_id": "conn-1",
                "query": "SELECT 1",
            }
        )
        assert errors == []

        # Inline connection fields are not persisted in graph configs.
        errors = await node.validate_config(
            {
                "host": "localhost",
                "database": "mydb",
                "query": "SELECT 1",
            }
        )
        assert "connection_id" in str(errors)


# ===========================================================================
# DatabaseWriterNode Tests
# ===========================================================================


class TestDatabaseWriterNode:
    """Tests for the DatabaseWriterNode."""

    @pytest.mark.asyncio
    async def test_rejects_untrusted_column_identifiers(self, base_context):
        node = DatabaseWriterNode()
        base_context.config["table_name"] = "output_table"
        base_context.input_data = {"rows": [{'safe") VALUES (1); DROP TABLE users;--': 1}]}

        result = await node.execute(base_context)

        assert result.success is False
        assert "Invalid PostgreSQL column" in result.error_message

    def test_node_metadata(self):
        """Test node type, category, and port definitions."""
        node = DatabaseWriterNode()
        assert node.node_type == "database_writer"
        assert node.display_name == "Database Writer"
        assert node.category == "output"
        assert len(node.inputs) == 1
        assert len(node.outputs) == 1

    def test_config_schema(self):
        """Test that config schema has required fields."""
        node = DatabaseWriterNode()
        schema = node.config_schema
        assert "connection_id" in schema["properties"]
        assert "table_name" in schema["properties"]
        assert "mode" in schema["properties"]
        assert "batch_size" in schema["properties"]
        assert "upsert_key" in schema["properties"]
        assert "connection_id" in schema["required"]
        assert "table_name" in schema["required"]
        assert "insert" in schema["properties"]["mode"]["enum"]
        assert "upsert" in schema["properties"]["mode"]["enum"]
        assert "replace" in schema["properties"]["mode"]["enum"]

    @pytest.mark.asyncio
    async def test_execute_insert_mode(self, base_context):
        """Test inserting rows in insert mode."""
        node = DatabaseWriterNode()
        base_context.config["table_name"] = "output_table"
        base_context.config["mode"] = "insert"
        base_context.config["batch_size"] = 100
        base_context.input_data = {
            "rows": [
                {"id": 1, "name": "Alice", "score": 10},
                {"id": 2, "name": "Bob", "score": 20},
            ]
        }

        connector = _mock_postgres_destination(written=2)
        with patch("app.nodes.db_writer.PostgresDestination", return_value=connector):
            result = await node.execute(base_context)

        assert result.success is True
        assert result.output_data["rows_affected"] == 2
        assert result.output_data["table"] == "output_table"
        assert result.items_processed == 2
        connector.write.assert_awaited_once()
        assert len(connector.write.await_args.args[0]) == 2

    @pytest.mark.asyncio
    async def test_execute_forwards_postgres_type_hint_sidecar(self, base_context):
        node = DatabaseWriterNode()
        base_context.config["table_name"] = "output_table"
        base_context.input_data = {
            "rows": [{"occurred_at": "2026-01-02T03:04:05+00:00"}],
            "postgres_type_hints": [{"occurred_at": "datetime"}],
        }
        connector = _mock_postgres_destination(written=1)

        with patch("app.nodes.db_writer.PostgresDestination", return_value=connector):
            result = await node.execute(base_context)

        assert result.success is True
        envelope = connector.write.await_args.args[0][0]
        assert envelope.metadata["ingestion_graph.postgres_types"] == {"occurred_at": "datetime"}

    @pytest.mark.asyncio
    async def test_executor_table_edge_unwraps_source_bundle_and_sidecar(self, base_context):
        source_bundle = {
            "rows": [{"occurred_at": "2026-01-02T03:04:05+00:00"}],
            "row_count": 1,
            "columns": ["occurred_at"],
            "postgres_type_hints": [{"occurred_at": "datetime"}],
        }
        inputs, _lineage = DAGExecutor(MagicMock())._collect_inputs(
            "writer",
            [
                {
                    "source": "source",
                    "target": "writer",
                    "source_port": "table",
                    "target_port": "table",
                }
            ],
            {"outputs": {"source": source_bundle}},
        )
        base_context.node_id = "writer"
        base_context.config["table_name"] = "output_table"
        base_context.input_data = inputs
        connector = _mock_postgres_destination(written=1)

        with patch("app.nodes.db_writer.PostgresDestination", return_value=connector):
            result = await DatabaseWriterNode().execute(base_context)

        assert result.success is True
        envelope = connector.write.await_args.args[0][0]
        assert envelope.payload.data == source_bundle["rows"][0]
        assert envelope.metadata["ingestion_graph.postgres_types"] == {"occurred_at": "datetime"}

    @pytest.mark.asyncio
    async def test_execute_replace_mode(self, base_context):
        """Test that replace mode truncates before inserting."""
        node = DatabaseWriterNode()
        base_context.config["table_name"] = "output_table"
        base_context.config["mode"] = "replace"
        base_context.input_data = {"rows": [{"id": 1, "name": "Test"}]}

        connector = _mock_postgres_destination(written=1)
        with patch("app.nodes.db_writer.PostgresDestination", return_value=connector):
            result = await node.execute(base_context)

        assert result.success is True
        connector.replace.assert_awaited_once()
        connector.write.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_execute_empty_rows(self, base_context):
        """Test that empty row input returns zero affected."""
        node = DatabaseWriterNode()
        base_context.config["table_name"] = "output_table"
        base_context.input_data = {"rows": []}

        result = await node.execute(base_context)

        assert result.success is True
        assert result.output_data["rows_affected"] == 0

    @pytest.mark.asyncio
    async def test_execute_missing_table_name(self, base_context):
        """Test that missing table_name returns error."""
        node = DatabaseWriterNode()
        base_context.config["table_name"] = ""
        base_context.input_data = {"rows": [{"id": 1}]}

        result = await node.execute(base_context)

        assert result.success is False
        assert "table_name" in result.error_message

    @pytest.mark.asyncio
    async def test_execute_db_error(self, base_context):
        """Test that database errors are handled gracefully."""
        node = DatabaseWriterNode()
        base_context.config["table_name"] = "output_table"
        base_context.input_data = {"rows": [{"id": 1}]}

        connector = _mock_postgres_destination()
        connector.write = AsyncMock(side_effect=Exception("Table does not exist with password"))
        with patch("app.nodes.db_writer.PostgresDestination", return_value=connector):
            result = await node.execute(base_context)

        assert result.success is False
        assert result.error_message == "Database write failed: Exception"
        assert "Table does not exist" not in result.error_message

    @pytest.mark.asyncio
    async def test_validate_config(self):
        """Test config validation."""
        node = DatabaseWriterNode()
        errors = await node.validate_config({})
        assert "connection_id" in str(errors)
        assert "table_name" in str(errors)

        errors = await node.validate_config(
            {
                "connection_id": "conn-1",
                "table_name": "my_table",
            }
        )
        assert errors == []


# ===========================================================================
# VectorStoreNode Tests
# ===========================================================================


class TestVectorStoreNode:
    """Tests for the VectorStoreNode."""

    @pytest.mark.asyncio
    async def test_rejects_non_integer_embedding_dimension(self, base_context):
        node = VectorStoreNode()
        base_context.config["embedding_dim"] = "3); DROP TABLE users;--"
        base_context.input_data = {
            "embeddings": [{"content": "unsafe", "embedding": [0.1, 0.2, 0.3]}]
        }

        result = await node.execute(base_context)

        assert result.success is False
        assert result.error_message == "embedding_dim must be an integer"

    def test_node_metadata(self):
        """Test node type, category, and port definitions."""
        node = VectorStoreNode()
        assert node.node_type == "vector_store"
        assert node.display_name == "Vector Store"
        assert node.category == "output"
        assert len(node.inputs) == 1
        assert node.inputs[0].data_type == PortDataType.EMBEDDINGS
        assert len(node.outputs) == 1

    def test_config_schema(self):
        """Test that config schema has all expected fields."""
        node = VectorStoreNode()
        schema = node.config_schema
        props = schema["properties"]
        assert "connection_id" in props
        assert "table_name" in props
        assert "embedding_dim" in props
        assert "create_index" in props
        assert "id_column" in props
        assert "content_column" in props
        assert "metadata_column" in props
        assert "vector_column" in props
        assert "connection_id" in schema["required"]
        # Check defaults
        assert props["table_name"]["default"] == "documents"
        assert props["embedding_dim"]["default"] == 1536
        assert props["create_index"]["default"] is True
        assert props["id_column"]["default"] == "id"
        assert props["content_column"]["default"] == "content"
        assert props["metadata_column"]["default"] == "metadata"
        assert props["vector_column"]["default"] == "embedding"

    @pytest.mark.asyncio
    async def test_execute_stores_embeddings(self, base_context):
        """Test that embeddings are stored in the database."""
        node = VectorStoreNode()
        base_context.config["table_name"] = "documents"
        base_context.config["embedding_dim"] = 3
        base_context.input_data = {
            "embeddings": [
                {
                    "content": "Hello world",
                    "metadata": {"source": "test"},
                    "embedding": [0.1, 0.2, 0.3],
                },
                {
                    "content": "Second doc",
                    "metadata": {"source": "test2"},
                    "embedding": [0.4, 0.5, 0.6],
                },
            ]
        }

        mock_conn = AsyncMock()
        mock_conn.execute = AsyncMock()
        mock_conn.close = AsyncMock()

        # asyncpg is imported inside the method, so patch at the asyncpg module level
        with patch("asyncpg.connect", new_callable=AsyncMock, return_value=mock_conn):
            result = await node.execute(base_context)

        assert result.success is True
        assert result.output_data["stored_count"] == 2
        assert result.output_data["table"] == "documents"
        assert result.items_processed == 2
        # CREATE EXTENSION, CREATE TABLE, and CREATE INDEX use execute; inserts
        # are sent as one optimized executemany batch.
        assert mock_conn.execute.call_count == 3
        mock_conn.executemany.assert_awaited_once()
        mock_conn.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_execute_creates_table(self, base_context):
        """Test that the table is auto-created with correct schema."""
        node = VectorStoreNode()
        base_context.config["table_name"] = "my_vectors"
        base_context.config["embedding_dim"] = 768
        base_context.input_data = {
            "embeddings": [
                {"content": "test", "embedding": [0.1] * 768, "metadata": {}},
            ]
        }

        mock_conn = AsyncMock()
        mock_conn.execute = AsyncMock()
        mock_conn.close = AsyncMock()

        with patch("asyncpg.connect", new_callable=AsyncMock, return_value=mock_conn):
            await node.execute(base_context)

        # Check that CREATE EXTENSION was called
        calls = [call.args[0] for call in mock_conn.execute.call_args_list]
        assert any("CREATE EXTENSION IF NOT EXISTS vector" in c for c in calls)
        # Check that CREATE TABLE was called
        assert any("CREATE TABLE IF NOT EXISTS" in c and "my_vectors" in c for c in calls)
        # Check that vector dimension is correct
        assert any("vector(768)" in c for c in calls)

    @pytest.mark.asyncio
    async def test_execute_creates_hnsw_index(self, base_context):
        """Test that HNSW index is created when create_index is True."""
        node = VectorStoreNode()
        base_context.config["create_index"] = True
        base_context.config["embedding_dim"] = 3
        base_context.config["table_name"] = "documents"
        base_context.config["vector_column"] = "embedding"
        base_context.input_data = {
            "embeddings": [
                {"content": "test", "embedding": [0.1, 0.2, 0.3], "metadata": {}},
            ]
        }

        mock_conn = AsyncMock()
        mock_conn.execute = AsyncMock()
        mock_conn.close = AsyncMock()

        with patch("asyncpg.connect", new_callable=AsyncMock, return_value=mock_conn):
            result = await node.execute(base_context)

        assert result.output_data["index_created"] is True
        calls = [call.args[0] for call in mock_conn.execute.call_args_list]
        assert any("hnsw" in c and "vector_cosine_ops" in c for c in calls)

    @pytest.mark.asyncio
    async def test_execute_no_index_when_disabled(self, base_context):
        """Test that HNSW index is NOT created when create_index is False."""
        node = VectorStoreNode()
        base_context.config["create_index"] = False
        base_context.config["embedding_dim"] = 3
        base_context.input_data = {
            "embeddings": [
                {"content": "test", "embedding": [0.1, 0.2, 0.3], "metadata": {}},
            ]
        }

        mock_conn = AsyncMock()
        mock_conn.execute = AsyncMock()
        mock_conn.close = AsyncMock()

        with patch("asyncpg.connect", new_callable=AsyncMock, return_value=mock_conn):
            result = await node.execute(base_context)

        assert result.output_data["index_created"] is False
        calls = [call.args[0] for call in mock_conn.execute.call_args_list]
        assert not any("hnsw" in c for c in calls)

    @pytest.mark.asyncio
    async def test_execute_empty_embeddings(self, base_context):
        """Test that empty embeddings input returns zero count."""
        node = VectorStoreNode()
        base_context.input_data = {"embeddings": []}

        result = await node.execute(base_context)

        assert result.success is True
        assert result.output_data["stored_count"] == 0

    @pytest.mark.asyncio
    async def test_execute_skips_items_without_embedding(self, base_context):
        """Test that items without an embedding vector are skipped."""
        node = VectorStoreNode()
        base_context.config["embedding_dim"] = 3
        base_context.input_data = {
            "embeddings": [
                {"content": "has embedding", "embedding": [0.1, 0.2, 0.3]},
                {"content": "no embedding"},  # Should be skipped
                {"embedding": [0.4, 0.5, 0.6]},  # Should be stored (no content but has vector)
            ]
        }

        mock_conn = AsyncMock()
        mock_conn.execute = AsyncMock()
        mock_conn.close = AsyncMock()

        with patch("asyncpg.connect", new_callable=AsyncMock, return_value=mock_conn):
            result = await node.execute(base_context)

        assert result.output_data["stored_count"] == 2

    @pytest.mark.asyncio
    async def test_execute_handles_vector_key(self, base_context):
        """Test that 'vector' key is also accepted as embedding key."""
        node = VectorStoreNode()
        base_context.config["embedding_dim"] = 3
        base_context.input_data = {
            "embeddings": [
                {"text": "doc text", "vector": [0.1, 0.2, 0.3], "metadata": {"k": "v"}},
            ]
        }

        mock_conn = AsyncMock()
        mock_conn.execute = AsyncMock()
        mock_conn.close = AsyncMock()

        with patch("asyncpg.connect", new_callable=AsyncMock, return_value=mock_conn):
            result = await node.execute(base_context)

        assert result.success is True
        assert result.output_data["stored_count"] == 1

    @pytest.mark.asyncio
    async def test_execute_error(self, base_context):
        """Test that errors are handled gracefully."""
        node = VectorStoreNode()
        base_context.input_data = {"embeddings": [{"content": "test", "embedding": [0.1]}]}

        with patch(
            "asyncpg.connect", new_callable=AsyncMock, side_effect=Exception("Connection refused")
        ):
            result = await node.execute(base_context)

        assert result.success is False
        assert "Connection refused" in result.error_message

    @pytest.mark.asyncio
    async def test_execute_closes_connection_on_error(self, base_context):
        """Test that the connection is closed even when an error occurs."""
        node = VectorStoreNode()
        base_context.input_data = {"embeddings": [{"content": "test", "embedding": [0.1]}]}

        mock_conn = AsyncMock()
        mock_conn.execute = AsyncMock(side_effect=Exception("Query error"))
        mock_conn.close = AsyncMock()

        with patch("asyncpg.connect", new_callable=AsyncMock, return_value=mock_conn):
            result = await node.execute(base_context)

        assert result.success is False
        mock_conn.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_validate_config(self):
        """Test config validation."""
        node = VectorStoreNode()
        errors = await node.validate_config({})
        assert "connection_id" in str(errors)

        errors = await node.validate_config({"connection_id": "conn-1"})
        assert errors == []


# ===========================================================================
# Connection Service Tests
# ===========================================================================


class TestConnectionService:
    """Tests for the connection service CRUD operations."""

    def test_connection_configs_are_encrypted_at_rest(self):
        from app.services.connection_crypto import (
            decrypt_connection_config,
            encrypt_connection_config,
        )

        original = {"host": "db.internal", "password": "do-not-store-in-plaintext"}
        encrypted = encrypt_connection_config(original)

        assert encrypted != original
        assert "do-not-store-in-plaintext" not in str(encrypted)
        assert decrypt_connection_config(encrypted) == original

    def test_graph_versions_reject_inline_secrets(self):
        from app.services.graph_service import _assert_no_inline_secrets

        with pytest.raises(ValueError, match="use connection_id"):
            _assert_no_inline_secrets({"database-node": {"host": "db", "password": "plaintext"}})

        _assert_no_inline_secrets({"database-node": {"connection_id": str(uuid.uuid4())}})

        with pytest.raises(ValueError, match="nodes_data"):
            _assert_no_inline_secrets(
                {"nodes": [{"data": {"config": {"bot_token": "plaintext"}}}]},
                "nodes_data",
            )

        # Token-related non-secret settings must remain usable.
        _assert_no_inline_secrets(
            {"llm": {"max_tokens": 500}, "chunker": {"tokenizer": "cl100k_base"}}
        )

    def test_graph_response_redacts_defense_in_depth_nodes_data(self):
        from datetime import UTC, datetime

        from app.schemas.graph import GraphVersionResponse

        response = GraphVersionResponse(
            id=uuid.uuid4(),
            graph_id=uuid.uuid4(),
            version_number=1,
            nodes_data={"nodes": [{"data": {"config": {"bot_token": "secret"}}}]},
            edges_data=None,
            node_configs=None,
            checksum=None,
            created_at=datetime.now(UTC),
        ).model_dump()

        assert response["nodes_data"]["nodes"][0]["data"]["config"]["bot_token"] == "********"

    def test_discord_uses_encrypted_saved_connection_schema(self):
        from app.nodes.discord_source import DiscordSourceNode
        from app.services.connection_service import SUPPORTED_CONNECTION_TYPES

        schema = DiscordSourceNode().config_schema
        assert "discord" in SUPPORTED_CONNECTION_TYPES
        assert "connection_id" in schema["required"]
        assert "bot_token" not in schema["properties"]

    def test_production_rejects_documented_connection_key_placeholder(self):
        from app.config import Settings

        config = Settings(
            app_env="production",
            jwt_secret_key="a-real-jwt-secret-that-is-long-enough",
            connection_encryption_key="change-this-connection-encryption-key",
            _env_file=None,
        )

        with pytest.raises(RuntimeError, match="CONNECTION_ENCRYPTION_KEY"):
            config.validate_security()

    def test_connection_response_redacts_nested_secrets(self):
        from datetime import UTC, datetime

        from app.schemas.graph import ConnectionResponse

        response = ConnectionResponse(
            id=uuid.uuid4(),
            user_id=uuid.uuid4(),
            name="Secret DB",
            type="postgres",
            config={
                "host": "db.internal",
                "password": "do-not-return",
                "headers": {"Authorization": "Bearer do-not-return"},
            },
            is_valid=True,
            created_at=datetime.now(UTC),
        ).model_dump()

        assert response["config"]["host"] == "db.internal"
        assert response["config"]["password"] == "********"
        assert response["config"]["headers"]["Authorization"] == "********"

    @pytest.mark.asyncio
    async def test_create_connection(self):
        """Test creating a connection."""
        from app.services.connection_service import create_connection

        mock_db = AsyncMock()
        mock_db.add = MagicMock()
        user_id = uuid.uuid4()

        with patch("app.services.connection_service.Connection") as MockConnection:
            mock_instance = MagicMock()
            mock_instance.id = uuid.uuid4()
            mock_instance.user_id = user_id
            mock_instance.name = "Test DB"
            mock_instance.type = "postgres"
            mock_instance.config = {"host": "localhost"}
            mock_instance.is_valid = False
            MockConnection.return_value = mock_instance

            result = await create_connection(
                mock_db,
                user_id=user_id,
                name="Test DB",
                type="postgres",
                config={
                    "host": "localhost",
                    "database": "testdb",
                    "username": "testuser",
                    "password": "testpass",
                },
            )

        assert result is not None
        mock_db.add.assert_called_once()
        mock_db.commit.assert_called_once()
        mock_db.refresh.assert_called_once()

    @pytest.mark.asyncio
    async def test_create_connection_unsupported_type(self):
        """Test creating a connection with unsupported type raises ValueError."""
        from app.services.connection_service import create_connection

        mock_db = AsyncMock()
        user_id = uuid.uuid4()

        with pytest.raises(ValueError, match="Unsupported connection type"):
            await create_connection(
                mock_db,
                user_id=user_id,
                name="Bad Type",
                type="mysql",
                config={},
            )

    @pytest.mark.asyncio
    async def test_get_connection(self):
        """Test getting a connection by ID."""
        from app.services.connection_service import get_connection

        mock_db = AsyncMock()
        conn_id = uuid.uuid4()

        # Use MagicMock for the result since scalars() and scalar_one_or_none() are sync
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = MagicMock(id=conn_id)
        mock_db.execute = AsyncMock(return_value=mock_result)

        result = await get_connection(mock_db, conn_id)

        assert result is not None
        mock_db.execute.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_connection_not_found(self):
        """Test getting a non-existent connection returns None."""
        from app.services.connection_service import get_connection

        mock_db = AsyncMock()
        conn_id = uuid.uuid4()

        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = None
        mock_db.execute = AsyncMock(return_value=mock_result)

        result = await get_connection(mock_db, conn_id)

        assert result is None

    @pytest.mark.asyncio
    async def test_get_connections(self):
        """Test listing connections for a user."""
        from app.services.connection_service import get_connections

        mock_db = AsyncMock()
        user_id = uuid.uuid4()

        mock_result = MagicMock()
        mock_scalars = MagicMock()
        mock_scalars.all.return_value = [MagicMock(), MagicMock()]
        mock_result.scalars.return_value = mock_scalars
        mock_db.execute = AsyncMock(return_value=mock_result)

        result = await get_connections(mock_db, user_id=user_id)

        assert len(result) == 2
        mock_db.execute.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_connections_filtered_by_type(self):
        """Test listing connections filtered by type."""
        from app.services.connection_service import get_connections

        mock_db = AsyncMock()
        user_id = uuid.uuid4()

        mock_result = MagicMock()
        mock_scalars = MagicMock()
        mock_scalars.all.return_value = [MagicMock()]
        mock_result.scalars.return_value = mock_scalars
        mock_db.execute = AsyncMock(return_value=mock_result)

        result = await get_connections(mock_db, user_id=user_id, type="postgres")

        assert len(result) == 1

    @pytest.mark.asyncio
    async def test_update_connection(self):
        """Test updating a connection."""
        from app.services.connection_service import update_connection

        mock_db = AsyncMock()
        conn_id = uuid.uuid4()
        user_id = uuid.uuid4()

        mock_connection = MagicMock()
        mock_connection.user_id = user_id
        mock_connection.name = "Old Name"
        mock_connection.type = "postgres"

        with patch(
            "app.services.connection_service.get_connection",
            AsyncMock(return_value=mock_connection),
        ):
            result = await update_connection(
                mock_db,
                connection_id=conn_id,
                user_id=user_id,
                name="New Name",
                config={
                    "host": "new-host",
                    "database": "testdb",
                    "username": "testuser",
                    "password": "testpass",
                },
            )

        assert result is not None
        assert result.name == "New Name"
        from app.services.connection_crypto import decrypt_connection_config

        assert decrypt_connection_config(result.config) == {
            "host": "new-host",
            "database": "testdb",
            "username": "testuser",
            "password": "testpass",
        }
        assert result.is_valid is False  # Reset after config change
        mock_db.commit.assert_called_once()

    @pytest.mark.asyncio
    async def test_update_connection_not_found(self):
        """Test updating a non-existent connection returns None."""
        from app.services.connection_service import update_connection

        mock_db = AsyncMock()
        conn_id = uuid.uuid4()
        user_id = uuid.uuid4()

        with patch(
            "app.services.connection_service.get_connection",
            AsyncMock(return_value=None),
        ):
            result = await update_connection(
                mock_db,
                connection_id=conn_id,
                user_id=user_id,
                name="New Name",
            )

        assert result is None

    @pytest.mark.asyncio
    async def test_update_connection_wrong_owner(self):
        """Test updating another user's connection returns None."""
        from app.services.connection_service import update_connection

        mock_db = AsyncMock()
        conn_id = uuid.uuid4()
        user_id = uuid.uuid4()

        mock_connection = MagicMock()
        mock_connection.user_id = uuid.uuid4()  # Different user

        with patch(
            "app.services.connection_service.get_connection",
            AsyncMock(return_value=mock_connection),
        ):
            result = await update_connection(
                mock_db,
                connection_id=conn_id,
                user_id=user_id,
                name="Hacked",
            )

        assert result is None

    @pytest.mark.asyncio
    async def test_delete_connection(self):
        """Test deleting a connection."""
        from app.services.connection_service import delete_connection

        mock_db = AsyncMock()
        conn_id = uuid.uuid4()
        user_id = uuid.uuid4()

        mock_connection = MagicMock()
        mock_connection.user_id = user_id

        with patch(
            "app.services.connection_service.get_connection",
            AsyncMock(return_value=mock_connection),
        ):
            result = await delete_connection(
                mock_db,
                connection_id=conn_id,
                user_id=user_id,
            )

        assert result is True
        mock_db.delete.assert_called_once()
        mock_db.commit.assert_called_once()

    @pytest.mark.asyncio
    async def test_delete_connection_not_found(self):
        """Test deleting a non-existent connection returns False."""
        from app.services.connection_service import delete_connection

        mock_db = AsyncMock()
        conn_id = uuid.uuid4()
        user_id = uuid.uuid4()

        with patch(
            "app.services.connection_service.get_connection",
            AsyncMock(return_value=None),
        ):
            result = await delete_connection(
                mock_db,
                connection_id=conn_id,
                user_id=user_id,
            )

        assert result is False


# ===========================================================================
# Connection Test (connectivity) Tests
# ===========================================================================


class TestConnectionTest:
    """Tests for the connection test functionality."""

    @pytest.mark.asyncio
    async def test_test_postgres_connection_success(self):
        """Test successful PostgreSQL connection test."""
        from app.services.connection_service import test_connection
        from app.services.egress_policy import EgressPolicy

        mock_conn = AsyncMock()
        mock_conn.execute = AsyncMock(return_value=None)
        mock_conn.close = AsyncMock()

        config = {
            "host": "localhost",
            "port": 5432,
            "database": "testdb",
            "username": "testuser",
            "password": "testpass",
        }

        # psycopg is imported inside the function, patch at module level
        with patch(
            "psycopg.AsyncConnection.connect", new_callable=AsyncMock, return_value=mock_conn
        ):
            result = await test_connection(
                config,
                "postgres",
                egress_policy=EgressPolicy(
                    allowed_hosts=("localhost",),
                    resolver=AsyncMock(return_value=("127.0.0.1",)),
                ),
            )

        assert result["success"] is True
        assert "successful" in result["message"].lower()
        mock_conn.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_test_postgres_connection_failure(self):
        """Test failed PostgreSQL connection test."""
        from app.services.connection_service import test_connection
        from app.services.egress_policy import EgressPolicy

        config = {
            "host": "badhost",
            "port": 5432,
            "database": "missingdb",
            "username": "nouser",
            "password": "wrongpass",
        }

        with patch(
            "psycopg.AsyncConnection.connect",
            new_callable=AsyncMock,
            side_effect=Exception("Connection refused"),
        ):
            result = await test_connection(
                config,
                "postgres",
                egress_policy=EgressPolicy(resolver=AsyncMock(return_value=("93.184.216.34",))),
            )

        assert result["success"] is False
        assert "failed" in result["message"].lower()

    @pytest.mark.asyncio
    async def test_test_unsupported_type(self):
        """Test that unsupported type raises ValueError."""
        from app.services.connection_service import test_connection

        with pytest.raises(ValueError, match="Unsupported connection type"):
            await test_connection({}, "mysql")

    @pytest.mark.asyncio
    async def test_test_postgres_closes_on_error(self):
        """Test that connection is closed even on error."""
        from app.services.connection_service import test_connection
        from app.services.egress_policy import EgressPolicy

        mock_conn = AsyncMock()
        mock_conn.execute = AsyncMock(side_effect=Exception("Auth failed"))
        mock_conn.close = AsyncMock()

        config = {
            "host": "localhost",
            "port": 5432,
            "database": "db",
            "username": "u",
            "password": "p",
        }

        with patch(
            "psycopg.AsyncConnection.connect", new_callable=AsyncMock, return_value=mock_conn
        ):
            result = await test_connection(
                config,
                "postgres",
                egress_policy=EgressPolicy(
                    allowed_hosts=("localhost",),
                    resolver=AsyncMock(return_value=("127.0.0.1",)),
                ),
            )

        assert result["success"] is False
        mock_conn.close.assert_called_once()


# ===========================================================================
# Node to_dict / Serialization Tests
# ===========================================================================


class TestNodeSerialization:
    """Tests for node serialization via to_dict()."""

    def test_database_source_to_dict(self):
        """Test DatabaseSourceNode serialization."""
        node = DatabaseSourceNode()
        d = node.to_dict()
        assert d["type"] == "database_source"
        assert d["display_name"] == "Database Source"
        assert d["category"] == "source"
        assert "config_schema" in d
        assert len(d["inputs"]) == 0
        assert len(d["outputs"]) == 1

    def test_database_writer_to_dict(self):
        """Test DatabaseWriterNode serialization."""
        node = DatabaseWriterNode()
        d = node.to_dict()
        assert d["type"] == "database_writer"
        assert d["category"] == "output"
        assert len(d["inputs"]) == 1
        assert len(d["outputs"]) == 1

    def test_vector_store_to_dict(self):
        """Test VectorStoreNode serialization."""
        node = VectorStoreNode()
        d = node.to_dict()
        assert d["type"] == "vector_store"
        assert d["category"] == "output"
        assert len(d["inputs"]) == 1
        assert len(d["outputs"]) == 1
        # Verify config schema has all pgvector-related fields
        props = d["config_schema"]["properties"]
        assert "table_name" in props
        assert "embedding_dim" in props
        assert "create_index" in props
        assert "id_column" in props
        assert "content_column" in props
        assert "metadata_column" in props
        assert "vector_column" in props
