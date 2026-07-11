"""Code-owned predefined Studio graphs built from the live node registry."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from uuid import UUID

from app.models.graph import Graph, GraphStatus, GraphVersion
from app.nodes.registry import get_node
from app.services.graph_service import compute_checksum
from sqlalchemy.ext.asyncio import AsyncSession


@dataclass(frozen=True, slots=True)
class TemplateNode:
    id: str
    type: str
    x: int
    y: int
    config: dict[str, Any]


@dataclass(frozen=True, slots=True)
class TemplateEdge:
    id: str
    source: str
    target: str
    source_port: str
    target_port: str


@dataclass(frozen=True, slots=True)
class GraphTemplate:
    id: str
    name: str
    description: str
    category: str
    setup: tuple[str, ...]
    nodes: tuple[TemplateNode, ...]
    edges: tuple[TemplateEdge, ...]

    def summary(self) -> dict[str, Any]:
        sdk_nodes = sum(
            1
            for item in self.nodes
            if (node := get_node(item.type)) is not None and node.implementation == "sdk-adapter"
        )
        return {
            "id": self.id,
            "name": self.name,
            "description": self.description,
            "category": self.category,
            "setup": list(self.setup),
            "node_count": len(self.nodes),
            "sdk_node_count": sdk_nodes,
        }


TEMPLATES: dict[str, GraphTemplate] = {
    "discord-search": GraphTemplate(
        id="discord-search",
        name="Discord to Search",
        description="Preview a Discord channel through the SDK and query the run output.",
        category="messaging",
        setup=("Select a saved Discord connection", "Enter a channel ID"),
        nodes=(
            TemplateNode("discord", "discord_source", 80, 140, {"message_limit": 100}),
            TemplateNode("query_store", "sdk_query_store", 460, 140, {"collection": "discord"}),
        ),
        edges=(TemplateEdge("discord-query", "discord", "query_store", "json", "items"),),
    ),
    "postgres-search": GraphTemplate(
        id="postgres-search",
        name="PostgreSQL to Search",
        description="Run a safe SELECT and inspect the result through the SDK query store.",
        category="database",
        setup=("Select a saved PostgreSQL connection", "Edit the SELECT query"),
        nodes=(
            TemplateNode(
                "database",
                "database_source",
                80,
                140,
                {"query": "SELECT * FROM your_table LIMIT 100", "batch_size": 100},
            ),
            TemplateNode("query_store", "sdk_query_store", 460, 140, {"collection": "database"}),
        ),
        edges=(TemplateEdge("database-query", "database", "query_store", "table", "items"),),
    ),
    "documents-search": GraphTemplate(
        id="documents-search",
        name="Documents to Search",
        description="Parse and chunk server-side PDF, Word, CSV, or text files for inspection.",
        category="documents",
        setup=("Upload files in Studio, then select them on the File Source node",),
        nodes=(
            TemplateNode(
                "files",
                "file_source",
                40,
                140,
                {"source_type": "upload", "artifact_ids": []},
            ),
            TemplateNode("parse", "file_parser", 310, 140, {"parser": "auto"}),
            TemplateNode(
                "chunk",
                "text_chunker",
                580,
                140,
                {"chunk_size": 512, "chunk_overlap": 50, "tokenizer": "words"},
            ),
            TemplateNode("query_store", "sdk_query_store", 850, 140, {"collection": "documents"}),
        ),
        edges=(
            TemplateEdge("files-parse", "files", "parse", "file_list", "file_list"),
            TemplateEdge("parse-chunk", "parse", "chunk", "documents", "documents"),
            TemplateEdge("chunk-query", "chunk", "query_store", "chunks", "items"),
        ),
    ),
}


def validate_templates() -> None:
    """Fail fast when a code-owned template drifts from registered node contracts."""
    for key, template in TEMPLATES.items():
        if key != template.id:
            raise ValueError(f"Template catalog key {key!r} does not match {template.id!r}")
        node_ids = [node.id for node in template.nodes]
        if len(node_ids) != len(set(node_ids)):
            raise ValueError(f"Template {key!r} contains duplicate node IDs")
        definitions = {}
        for item in template.nodes:
            node = get_node(item.type)
            if node is None:
                raise ValueError(f"Template {key!r} requires missing node {item.type!r}")
            definitions[item.id] = node
        connected_inputs: set[tuple[str, str]] = set()
        edge_ids: set[str] = set()
        for edge in template.edges:
            if edge.id in edge_ids:
                raise ValueError(f"Template {key!r} contains duplicate edge {edge.id!r}")
            edge_ids.add(edge.id)
            if edge.source not in definitions or edge.target not in definitions:
                raise ValueError(f"Template {key!r} edge {edge.id!r} has an unknown endpoint")
            source_port = next(
                (
                    port
                    for port in definitions[edge.source].outputs
                    if port.name == edge.source_port
                ),
                None,
            )
            target_port = next(
                (port for port in definitions[edge.target].inputs if port.name == edge.target_port),
                None,
            )
            if source_port is None or target_port is None:
                raise ValueError(f"Template {key!r} edge {edge.id!r} references an unknown port")
            if (
                target_port.data_type.value != "any"
                and source_port.data_type != target_port.data_type
            ):
                raise ValueError(f"Template {key!r} edge {edge.id!r} has incompatible ports")
            connected_inputs.add((edge.target, edge.target_port))
        for node_id, definition in definitions.items():
            for port in definition.inputs:
                if port.required and (node_id, port.name) not in connected_inputs:
                    raise ValueError(
                        f"Template {key!r} does not connect required input {node_id}.{port.name}"
                    )


def materialize_template(template: GraphTemplate) -> tuple[dict, dict, dict]:
    nodes: list[dict[str, Any]] = []
    node_configs: dict[str, dict[str, Any]] = {}
    for item in template.nodes:
        node = get_node(item.type)
        if node is None:
            raise RuntimeError(f"Template {template.id!r} requires missing node {item.type!r}")
        definition = node.to_dict()
        nodes.append(
            {
                "id": item.id,
                "type": item.type,
                "position": {"x": item.x, "y": item.y},
                "data": {
                    "label": definition["display_name"],
                    "config": {},
                    "category": definition["category"],
                    "inputs": definition["inputs"],
                    "outputs": definition["outputs"],
                    "implementation": definition["implementation"],
                    "sdk_component": definition["sdk_component"],
                },
            }
        )
        node_configs[item.id] = dict(item.config)

    edges = [
        {
            "id": edge.id,
            "source": edge.source,
            "target": edge.target,
            "sourceHandle": edge.source_port,
            "targetHandle": edge.target_port,
            "source_port": edge.source_port,
            "target_port": edge.target_port,
        }
        for edge in template.edges
    ]
    return {"nodes": nodes}, {"edges": edges}, node_configs


async def create_graph_from_template(
    db: AsyncSession,
    *,
    template_id: str,
    owner_id: UUID,
    name: str,
    description: str | None,
    tags: list[str],
) -> Graph:
    template = TEMPLATES.get(template_id)
    if template is None:
        raise KeyError(template_id)
    nodes_data, edges_data, node_configs = materialize_template(template)
    graph = Graph(
        name=name,
        description=description or template.description,
        owner_id=owner_id,
        status=GraphStatus.DRAFT.value,
        tags=[*tags, "template", template.id],
    )
    try:
        db.add(graph)
        await db.flush()
        combined = {"nodes": nodes_data, "edges": edges_data, "configs": node_configs}
        version = GraphVersion(
            graph_id=graph.id,
            version_number=1,
            nodes_data=nodes_data,
            edges_data=edges_data,
            node_configs=node_configs,
            checksum=compute_checksum(combined),
        )
        db.add(version)
        await db.commit()
        await db.refresh(graph)
        return graph
    except Exception:
        await db.rollback()
        raise
