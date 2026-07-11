"""Typed connection catalog and pre-SDK graph compatibility tests."""

from unittest.mock import AsyncMock, patch

import pytest
from app.connection_catalog import CONNECTION_DEFINITIONS, validate_connection_config
from app.nodes.base import NodeContext
from app.nodes.database_source import DatabaseSourceNode
from app.nodes.db_writer import DatabaseWriterNode
from app.nodes.discord_source import DiscordSourceNode
from app.nodes.vector_store import VectorStoreNode


def _context(config: dict | None = None) -> NodeContext:
    return NodeContext(run_id="legacy-run", node_id="legacy-node", config=config or {})


def test_catalog_matches_executable_connection_types_and_node_hints():
    assert set(CONNECTION_DEFINITIONS) == {"postgres", "discord"}
    assert DatabaseSourceNode().config_schema["properties"]["connection_id"] == {
        "type": "string",
        "format": "connection-ref",
        "connection_type": "postgres",
        "description": "Saved PostgreSQL connection ID",
    }
    assert DiscordSourceNode().config_schema["properties"]["connection_id"][
        "connection_type"
    ] == "discord"


def test_catalog_rejects_incomplete_credentials():
    with pytest.raises(ValueError, match="database, username, password"):
        validate_connection_config("postgres", {"host": "db"})
    with pytest.raises(ValueError, match="bot_token"):
        validate_connection_config("discord", {})


def test_legacy_database_nodes_require_explicit_saved_connections():
    with pytest.raises(ValueError, match="select an encrypted saved connection"):
        DatabaseSourceNode()._build_connection_url(_context({"query": "SELECT 1"}))
    with pytest.raises(ValueError, match="select an encrypted saved connection"):
        DatabaseWriterNode()._build_connection_url(_context({"table_name": "items"}))
    with pytest.raises(ValueError, match="Saved connection not available"):
        DatabaseSourceNode()._build_connection_url(
            _context({"connection_id": "missing", "query": "SELECT 1"})
        )


@pytest.mark.asyncio
async def test_legacy_vector_store_never_falls_back_to_control_plane_database():
    with (
        patch("asyncpg.connect", new_callable=AsyncMock) as connect,
        pytest.raises(ValueError, match="select an encrypted saved connection"),
    ):
        await VectorStoreNode()._get_asyncpg_connection(_context())
    connect.assert_not_awaited()


@pytest.mark.asyncio
async def test_legacy_discord_secret_gets_actionable_migration_error():
    result = await DiscordSourceNode().execute(
        _context({"bot_token": "legacy-secret", "channel_id": "123"})
    )
    assert not result.success
    assert "Create a saved Discord connection" in (result.error_message or "")
