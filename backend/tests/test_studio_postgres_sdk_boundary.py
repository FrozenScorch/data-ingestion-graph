"""Studio compatibility boundary for reusable PostgreSQL SDK connectors."""

from unittest.mock import patch

import pytest
from app.nodes.database_source import DatabaseSourceNode
from app.nodes.db_writer import DatabaseWriterNode
from app.nodes.registry import discover_nodes
from app.nodes.sdk_manifest import serialize_connector_manifest
from ingestion_graph.destinations import PostgresDestination
from ingestion_graph.sources import PostgresSource


def test_postgres_nodes_preserve_saved_graph_contracts_as_sdk_adapters() -> None:
    source = DatabaseSourceNode().to_dict()
    destination = DatabaseWriterNode().to_dict()

    assert source["type"] == "database_source"
    assert source["implementation"] == "sdk-adapter"
    assert source["sdk_component"] == "ingestion_graph.sources.PostgresSource"
    assert source["connector_manifest"] == serialize_connector_manifest(PostgresSource.manifest())
    assert source["outputs"][0]["name"] == "table"
    assert set(source["config_schema"]["properties"]) == {
        "connection_id",
        "query",
        "batch_size",
    }
    assert source["config_schema"]["required"] == ["query", "connection_id"]

    assert destination["type"] == "database_writer"
    assert destination["implementation"] == "sdk-adapter"
    assert destination["sdk_component"] == ("ingestion_graph.destinations.PostgresDestination")
    assert destination["connector_manifest"] == serialize_connector_manifest(
        PostgresDestination.manifest()
    )
    assert destination["inputs"][0]["name"] == "table"
    assert set(destination["config_schema"]["properties"]) == {
        "connection_id",
        "table_name",
        "mode",
        "batch_size",
        "upsert_key",
    }
    assert destination["config_schema"]["properties"]["mode"]["enum"] == [
        "insert",
        "upsert",
        "replace",
    ]
    assert destination["config_schema"]["required"] == [
        "table_name",
        "connection_id",
    ]


@pytest.mark.parametrize(
    ("connector_type", "node_type"),
    [
        (PostgresSource, "database_source"),
        (PostgresDestination, "database_writer"),
    ],
)
def test_postgres_manifest_drift_fails_studio_startup(connector_type, node_type) -> None:
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
