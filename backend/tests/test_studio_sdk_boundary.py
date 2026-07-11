"""Enterprise Studio packaging, template, and SDK adapter boundary tests."""

from copy import deepcopy
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest
from app.api.executions import _unpack_version_data
from app.graph_templates import (
    TEMPLATES,
    create_graph_from_template,
    materialize_template,
    validate_templates,
)
from app.graph_validation import validate_graph_edges
from app.nodes.discord_source import DiscordSourceNode
from app.nodes.registry import discover_nodes
from app.nodes.sdk_query_store import SDKQueryStoreNode


def test_sdk_adapter_metadata_is_visible_to_studio():
    discord = DiscordSourceNode().to_dict()
    query = SDKQueryStoreNode().to_dict()

    assert discord["implementation"] == "sdk-adapter"
    assert discord["sdk_component"] == "ingestion_graph.sources.DiscordSource"
    assert query["implementation"] == "sdk-adapter"
    assert query["sdk_component"] == "ingestion_graph.destinations.SQLiteCollection"


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
    invalid_edges["edges"][0].update(
        {"target": "chunk", "targetHandle": "documents", "target_port": "documents"}
    )
    with pytest.raises(ValueError, match="cannot connect file_list to document"):
        validate_graph_edges(nodes_data, invalid_edges)


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
