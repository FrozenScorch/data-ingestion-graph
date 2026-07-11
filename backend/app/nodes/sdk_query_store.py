"""Studio adapter that materializes pipeline output with the reusable SDK."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from typing import Any

from app.nodes.base import BaseNode, NodeContext, NodeResult, PortDataType, PortDef
from app.services.query_artifact_service import (
    artifact_expires_at,
    query_artifact_path,
)
from ingestion_graph.destinations import SQLiteCollection
from ingestion_graph.models import Envelope, Operation, RecordPayload, stable_record_id


class SDKQueryStoreNode(BaseNode):
    """Make a run's output immediately searchable for pipeline testing."""

    @property
    def implementation(self) -> str:
        return "sdk-adapter"

    @property
    def sdk_component(self) -> str:
        return "ingestion_graph.destinations.SQLiteCollection"

    @property
    def node_type(self) -> str:
        return "sdk_query_store"

    @property
    def display_name(self) -> str:
        return "Queryable Test Store"

    @property
    def category(self) -> str:
        return "output"

    @property
    def description(self) -> str:
        return "Store pipeline output in the SDK's local searchable current view"

    @property
    def inputs(self) -> list[PortDef]:
        return [PortDef(name="items", data_type=PortDataType.ANY, required=True)]

    @property
    def outputs(self) -> list[PortDef]:
        return [PortDef(name="result", data_type=PortDataType.JSON, label="Query Store")]

    @property
    def config_schema(self) -> dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "collection": {
                    "type": "string",
                    "default": "pipeline-output",
                    "pattern": "^[a-zA-Z0-9_-]+$",
                    "description": "Logical collection name shown in query results",
                }
            },
        }

    async def execute(self, context: NodeContext) -> NodeResult:
        collection = str(context.config.get("collection") or "pipeline-output")
        if re.fullmatch(r"[a-zA-Z0-9_-]+", collection) is None:
            return NodeResult(success=False, error_message="Invalid collection name")

        items = _extract_items(context.input_data)
        envelopes = [
            _to_envelope(item, context=context, collection=collection, index=index)
            for index, item in enumerate(items)
        ]
        store_path = query_artifact_path(context.run_id, base_dir=context.working_dir)
        store = SQLiteCollection(store_path)
        try:
            check = await store.check()
            if not check.ok:
                return NodeResult(success=False, error_message="Query store is unavailable")
            written = await store.write(envelopes)
            await store.flush()
        except Exception as exc:
            return NodeResult(
                success=False,
                error_message=f"Query store write failed: {type(exc).__name__}",
            )
        finally:
            await store.close()

        return NodeResult(
            success=True,
            output_data={
                "result": {
                    "run_id": context.run_id,
                    "collection": collection,
                    "records_received": len(envelopes),
                    "records_written": written,
                    "query_endpoint": f"/api/executions/{context.run_id}/query",
                    "artifact_size_bytes": store_path.stat().st_size,
                    "artifact_expires_at": artifact_expires_at(store_path).isoformat(),
                }
            },
            items_processed=len(envelopes),
        )


def _extract_items(input_data: dict[str, Any]) -> list[Any]:
    """Accept common Studio port shapes without coupling the SDK to Studio."""
    if not input_data:
        return []
    for key in (
        "items",
        "chunks",
        "documents",
        "rows",
        "embeddings",
        "json",
        "merged",
        "result",
        "output",
    ):
        if key in input_data:
            value = input_data[key]
            return list(value) if isinstance(value, list) else [value]
    if len(input_data) == 1:
        value = next(iter(input_data.values()))
        return list(value) if isinstance(value, list) else [value]
    return [input_data]


def _to_envelope(
    item: Any,
    *,
    context: NodeContext,
    collection: str,
    index: int,
) -> Envelope:
    data = dict(item) if isinstance(item, Mapping) else {"value": item}
    encoded = json.dumps(data, sort_keys=True, default=str, separators=(",", ":")).encode()
    native_id = str(
        data.get("id")
        or data.get("document_id")
        or data.get("message_id")
        or hashlib.sha256(encoded).hexdigest()
    )
    return Envelope(
        id=stable_record_id("studio", collection, native_id),
        source="studio",
        stream=collection,
        operation=Operation.UPSERT,
        cursor=str(index),
        checksum=hashlib.sha256(encoded).hexdigest(),
        payload=RecordPayload(data),
        metadata={"run_id": context.run_id, "node_id": context.node_id},
        provenance={"adapter": "backend.sdk_query_store"},
    )


def register() -> None:
    from app.nodes.registry import register_node

    register_node(SDKQueryStoreNode())
