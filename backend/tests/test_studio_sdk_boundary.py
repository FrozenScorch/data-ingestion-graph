"""Enterprise Studio packaging, template, and SDK adapter boundary tests."""

from copy import deepcopy
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest
from app.api.executions import _unpack_version_data
from app.graph_templates import (
    TEMPLATES,
    create_graph_from_template,
    materialize_template,
    validate_templates,
)
from app.graph_validation import (
    sanitize_node_configs,
    strip_embedded_node_configs,
    validate_graph_edges,
)
from app.nodes.discord_source import DiscordSourceNode
from app.nodes.registry import discover_nodes
from app.nodes.sdk_document_source import SDKDocumentSourceNode, SDKOCRDocumentSourceNode
from app.nodes.sdk_manifest import (
    ManifestFieldProjection,
    project_manifest_config_schema,
    serialize_connector_manifest,
)
from app.nodes.sdk_query_store import SDKQueryStoreNode
from ingestion_graph.destinations import SQLiteCollection
from ingestion_graph.sources import DiscordSource, LocalDocumentsSource


def test_ocr_adapter_is_a_thin_sdk_preset():
    node = SDKOCRDocumentSourceNode()
    assert node.node_type == "sdk_ocr_document_source"
    assert node.studio_default_ocr_mode == "auto"
    properties = node.config_schema["properties"]
    assert ".png" in properties["artifact_ids"]["accepted_extensions"]
    assert properties["ocr_mode"]["default"] == "auto"
    assert properties["table_mode"]["enum"] == ["off", "native"]
    assert SDKDocumentSourceNode().config_schema["properties"]["ocr_mode"]["default"] == "off"


def test_sdk_adapter_metadata_is_visible_to_studio():
    discord = DiscordSourceNode().to_dict()
    documents = SDKDocumentSourceNode().to_dict()
    query = SDKQueryStoreNode().to_dict()

    assert discord["implementation"] == "sdk-adapter"
    assert discord["sdk_component"] == "ingestion_graph.sources.DiscordSource"
    assert discord["connector_manifest"] == {
        "name": "discord",
        "version": "1.0.0",
        "capabilities": {
            "incremental": True,
            "resumable_full_refresh": True,
            "deletes": False,
            "schema_discovery": True,
            "rate_limits": True,
        },
    }
    assert documents["implementation"] == "sdk-adapter"
    assert documents["sdk_component"] == "ingestion_graph.sources.LocalDocumentsSource"
    assert documents["connector_manifest"] == serialize_connector_manifest(
        LocalDocumentsSource.manifest()
    )
    assert query["implementation"] == "sdk-adapter"
    assert query["sdk_component"] == "ingestion_graph.destinations.SQLiteCollection"
    assert query["connector_manifest"] == serialize_connector_manifest(SQLiteCollection.manifest())


def test_discord_studio_schema_is_strictly_projected_from_sdk_manifest():
    sdk_schema = DiscordSource.manifest().config_schema
    studio_schema = DiscordSourceNode().config_schema

    assert studio_schema["properties"]["channel_id"] == {
        **sdk_schema["properties"]["channel_ids"]["items"],
        "description": "Discord channel ID to preview",
    }
    assert studio_schema["required"] == ["channel_id", "connection_id"]
    assert "token" not in studio_schema["properties"]


def test_document_studio_schema_is_strictly_projected_from_sdk_manifest():
    sdk_schema = LocalDocumentsSource.manifest().config_schema
    studio_schema = SDKDocumentSourceNode().config_schema

    for field, maximum in (
        ("checkpoint_interval", 1000),
        ("text_chunk_chars", 1_000_000),
        ("table_batch_rows", 5000),
    ):
        assert studio_schema["properties"][field] == {
            **sdk_schema["properties"][field],
            "maximum": maximum,
        }
    assert studio_schema["properties"]["artifact_ids"]["format"] == "artifact-refs"
    assert studio_schema["properties"]["artifact_ids"]["accepted_extensions"]
    assert studio_schema["required"] == []
    assert {
        "paths",
        "recursive",
        "extensions",
        "include_hidden",
        "follow_symlinks",
        "stream_names",
        "max_file_size_bytes",
        "max_archive_uncompressed_bytes",
    }.isdisjoint(studio_schema["properties"])


def test_query_store_schema_omits_sdk_path_and_adds_studio_collection():
    studio_schema = SDKQueryStoreNode().config_schema

    assert "path" not in studio_schema["properties"]
    assert studio_schema["properties"] == {
        "collection": {
            "type": "string",
            "default": "pipeline-output",
            "pattern": "^[a-zA-Z0-9_-]+$",
            "description": "Logical collection name shown in query results",
        }
    }
    assert studio_schema["required"] == []


def test_manifest_projection_fails_when_sdk_fields_are_not_accounted_for():
    manifest = DiscordSource.manifest()
    drifted_schema = dict(manifest.config_schema)
    drifted_schema["properties"] = {
        **manifest.config_schema["properties"],
        "new_sdk_field": {"type": "string"},
    }
    drifted = type(manifest)(
        name=manifest.name,
        version=manifest.version,
        config_schema=drifted_schema,
        capabilities=manifest.capabilities,
    )

    with pytest.raises(ValueError, match="new_sdk_field"):
        project_manifest_config_schema(
            drifted,
            fields=(
                ManifestFieldProjection(
                    source_field="channel_ids",
                    target_field="channel_id",
                    source_path=("items",),
                ),
            ),
            omitted={"token": "saved connection"},
        )


@pytest.mark.parametrize(
    ("connector_type", "node_type"),
    [
        (DiscordSource, "discord_source"),
        (LocalDocumentsSource, "sdk_document_source"),
        (SQLiteCollection, "sdk_query_store"),
    ],
)
def test_node_discovery_fails_startup_when_sdk_manifest_drifts(connector_type, node_type):
    manifest = connector_type.manifest()
    drifted_schema = dict(manifest.config_schema)
    drifted_schema["properties"] = {
        **manifest.config_schema["properties"],
        "unprojected": {"type": "string"},
    }
    drifted = type(manifest)(
        name=manifest.name,
        version=manifest.version,
        config_schema=drifted_schema,
        capabilities=manifest.capabilities,
    )

    with (
        patch.object(connector_type, "manifest", return_value=drifted),
        pytest.raises(ValueError, match=rf"{node_type}.*unprojected"),
    ):
        discover_nodes()


def test_templates_materialize_live_nodes_configs_and_dual_edge_ports():
    discover_nodes()
    validate_templates()
    nodes_data, edges_data, configs = materialize_template(TEMPLATES["discord-search"])

    assert len(nodes_data["nodes"]) == 2
    assert nodes_data["nodes"][0]["data"]["implementation"] == "sdk-adapter"
    assert nodes_data["nodes"][0]["data"]["config"] == {}
    assert configs["discord"] == {"message_limit": 100}
    edge = edges_data["edges"][0]
    assert edge["sourceHandle"] == edge["source_port"] == "json"
    assert edge["targetHandle"] == edge["target_port"] == "items"


def test_ui_edge_handles_are_normalized_for_executor_ports():
    _, edges = _unpack_version_data(
        {"nodes": []},
        {
            "edges": [
                {
                    "id": "edge",
                    "source": "source",
                    "target": "target",
                    "sourceHandle": "documents",
                    "targetHandle": "items",
                }
            ]
        },
    )

    assert edges[0]["source_port"] == "documents"
    assert edges[0]["target_port"] == "items"


def test_saved_graph_edges_enforce_registered_port_types():
    discover_nodes()
    nodes_data, edges_data, _ = materialize_template(TEMPLATES["documents-search"])
    validate_graph_edges(nodes_data, edges_data)

    invalid_edges = deepcopy(edges_data)
    invalid_edges["edges"][0].update({"sourceHandle": "missing", "source_port": "missing"})
    with pytest.raises(ValueError, match="references an unknown port"):
        validate_graph_edges(nodes_data, invalid_edges)


def test_legacy_discord_secret_is_dropped_when_migrating_to_saved_connection():
    discover_nodes()
    nodes_data, _, _ = materialize_template(TEMPLATES["discord-search"])
    configs = sanitize_node_configs(
        nodes_data,
        {
            "discord": {
                "bot_token": "legacy-secret",
                "connection_id": "saved-connection",
                "channel_id": "123",
            }
        },
    )
    assert configs["discord"] == {
        "connection_id": "saved-connection",
        "channel_id": "123",
    }
    legacy_nodes = deepcopy(nodes_data)
    legacy_nodes["nodes"][0]["data"]["config"] = {"bot_token": "legacy-secret"}
    normalized = strip_embedded_node_configs(legacy_nodes)
    assert normalized is not None
    assert normalized["nodes"][0]["data"]["config"] == {}


@pytest.mark.asyncio
async def test_template_creation_commits_graph_and_version_atomically():
    discover_nodes()
    db = AsyncMock()
    db.add = MagicMock()

    async def assign_graph_id():
        graph = db.add.call_args_list[0].args[0]
        graph.id = uuid4()

    db.flush.side_effect = assign_graph_id
    owner_id = uuid4()
    graph = await create_graph_from_template(
        db,
        template_id="postgres-search",
        owner_id=owner_id,
        name="Customer database",
        description=None,
        tags=["enterprise"],
    )

    assert graph.owner_id == owner_id
    assert graph.tags == ["enterprise", "template", "postgres-search"]
    assert db.add.call_count == 2
    version = db.add.call_args_list[1].args[0]
    assert version.graph_id == graph.id
    assert version.version_number == 1
    assert version.node_configs["database"]["batch_size"] == 100
    db.commit.assert_awaited_once()
    db.rollback.assert_not_awaited()
