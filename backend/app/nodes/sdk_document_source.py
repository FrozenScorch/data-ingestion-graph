"""Studio adapter for the SDK's owner-scoped local document source."""

from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any, Protocol, cast
from uuid import UUID

from app.nodes.base import BaseNode, NodeContext, NodeResult, PortDataType, PortDef
from app.nodes.sdk_manifest import (
    ManifestFieldProjection,
    project_manifest_config_schema,
    serialize_connector_manifest,
)
from app.services import upload_service
from app.services.sdk_source_state_service import (
    SDKSourceStateLeaseError,
    StudioSDKSourceStateStore,
)
from ingestion_graph.messages import RecordMessage, StateMessage
from ingestion_graph.sources import LocalDocumentsSource
from ingestion_graph.sources.documents import OCR_IMAGE_EXTENSIONS, SUPPORTED_EXTENSIONS

SOURCE_NAME = "local_documents"
STREAM_PREFIX = "upload-"
MAX_OUTPUT_ITEMS = 10_000
MAX_OUTPUT_BYTES = 64 * 1024 * 1024


class _OutputLimitExceeded(RuntimeError):
    pass


class _StudioStateStore(Protocol):
    pipeline_key: str

    async def acquire_lock(self) -> None: ...

    async def revalidate_lease(self) -> None: ...

    async def load(self, pipeline: str, source: str, stream: str) -> Mapping[str, Any]: ...

    async def save(
        self,
        pipeline: str,
        source: str,
        stream: str,
        state: Mapping[str, Any],
    ) -> None: ...

    async def list_streams(self, pipeline: str, source: str) -> list[str]: ...

    async def delete(self, pipeline: str, source: str, stream: str) -> None: ...


class SDKDocumentSourceNode(BaseNode):
    """Emit canonical incremental document envelopes from managed uploads."""

    def __init__(self, state_store: _StudioStateStore | None = None) -> None:
        self._injected_state_store = state_store

    @property
    def studio_accepted_extensions(self) -> tuple[str, ...]:
        return SUPPORTED_EXTENSIONS

    @property
    def studio_default_ocr_mode(self) -> str:
        return "off"

    @property
    def implementation(self) -> str:
        return "sdk-adapter"

    @property
    def sdk_component(self) -> str:
        return "ingestion_graph.sources.LocalDocumentsSource"

    @property
    def connector_manifest(self) -> dict[str, Any]:
        return serialize_connector_manifest(LocalDocumentsSource.manifest())

    @property
    def node_type(self) -> str:
        return "sdk_document_source"

    @property
    def display_name(self) -> str:
        return "Document Source (SDK)"

    @property
    def category(self) -> str:
        return "source"

    @property
    def description(self) -> str:
        return "Read selected managed uploads as resumable SDK document deltas"

    @property
    def inputs(self) -> list[PortDef]:
        return []

    @property
    def outputs(self) -> list[PortDef]:
        return [PortDef(name="items", data_type=PortDataType.ANY, label="Document Deltas")]

    @property
    def config_schema(self) -> dict[str, Any]:
        return project_manifest_config_schema(
            LocalDocumentsSource.manifest(),
            fields=(
                ManifestFieldProjection(
                    source_field="checkpoint_interval",
                    target_field="checkpoint_interval",
                    overrides={"maximum": 1000},
                ),
                ManifestFieldProjection(
                    source_field="text_chunk_chars",
                    target_field="text_chunk_chars",
                    overrides={"maximum": 1_000_000},
                ),
                ManifestFieldProjection(
                    source_field="table_batch_rows",
                    target_field="table_batch_rows",
                    overrides={"maximum": 5000},
                ),
                ManifestFieldProjection(source_field="ocr_mode", target_field="ocr_mode"),
                ManifestFieldProjection(source_field="table_mode", target_field="table_mode"),
                ManifestFieldProjection(source_field="failure_mode", target_field="failure_mode"),
                ManifestFieldProjection(
                    source_field="min_native_text_quality",
                    target_field="min_native_text_quality",
                ),
            ),
            omitted={
                "paths": "Studio resolves owner-scoped upload IDs to managed paths",
                "recursive": "Studio selects explicit managed files rather than directory trees",
                "extensions": "Studio validates managed uploads against supported extensions",
                "include_hidden": "Studio-managed uploads do not traverse hidden directories",
                "follow_symlinks": "Studio-managed uploads never enable symlink traversal",
                "stream_names": "Studio derives stable stream names from upload IDs",
                "max_file_size_bytes": "Studio enforces centrally managed upload limits",
                "max_archive_uncompressed_bytes": (
                    "Studio enforces centrally managed archive expansion limits"
                ),
                "ocr_languages": "Studio uses the deployment's configured OCR language set",
                "render_dpi": "Studio keeps renderer resolution deployment-controlled",
                "page_timeout_seconds": "Studio keeps page timeouts deployment-controlled",
                "max_page_concurrency": "Studio keeps concurrency deployment-controlled",
                "vision_fallback": "Studio has no live vision adapter in this release",
            },
            studio_properties={
                "artifact_ids": {
                    "type": "array",
                    "items": {"type": "string", "format": "uuid"},
                    "format": "artifact-refs",
                    "default": [],
                    "uniqueItems": True,
                    "maxItems": 100,
                    "accepted_extensions": list(self.studio_accepted_extensions),
                    "description": "Explicit Studio-managed files to ingest. Empty selects none.",
                },
                "max_output_items": {
                    "type": "integer",
                    "minimum": 1,
                    "maximum": MAX_OUTPUT_ITEMS,
                    "default": 5000,
                    "description": "Fail safely when a run delta exceeds this item count",
                },
                "max_output_bytes": {
                    "type": "integer",
                    "minimum": 1024,
                    "maximum": MAX_OUTPUT_BYTES,
                    "default": 16 * 1024 * 1024,
                    "description": "Fail safely when serialized run output exceeds this size",
                },
            },
        )

    async def execute(self, context: NodeContext) -> NodeResult:
        forbidden = {"path", "paths", "file_path", "file_pattern", "source_type"}
        if forbidden.intersection(context.config):
            return self._failure("Document Source accepts managed upload artifact IDs only")

        try:
            owner_id = UUID(str(context.state["owner_id"]))
            graph_id = UUID(str(context.state["graph_id"]))
        except (KeyError, TypeError, ValueError):
            return self._failure("Document execution is missing graph-owner context")

        raw_artifact_ids = context.config.get("artifact_ids") or []
        if not isinstance(raw_artifact_ids, list):
            return self._failure("artifact_ids must be an array of managed upload IDs")
        try:
            artifact_ids = [UUID(str(item)) for item in raw_artifact_ids]
        except (TypeError, ValueError):
            return self._failure("Invalid upload reference")
        if len(set(artifact_ids)) != len(artifact_ids):
            return self._failure("Duplicate upload references are not allowed")
        if len(artifact_ids) > 100:
            return self._failure("Document Source accepts at most 100 artifacts per node")

        try:
            checkpoint_interval = int(context.config.get("checkpoint_interval", 50))
            text_chunk_chars = int(context.config.get("text_chunk_chars", 12_000))
            table_batch_rows = int(context.config.get("table_batch_rows", 500))
            ocr_mode = str(context.config.get("ocr_mode", self.studio_default_ocr_mode))
            table_mode = str(context.config.get("table_mode", "off"))
            failure_mode = str(context.config.get("failure_mode", "strict"))
            min_native_text_quality = float(context.config.get("min_native_text_quality", 0.65))
            max_output_items = int(context.config.get("max_output_items", 5_000))
            max_output_bytes = int(context.config.get("max_output_bytes", 16 * 1024 * 1024))
        except (TypeError, ValueError):
            return self._failure("Document parser limits must be integers")
        if not 1 <= max_output_items <= MAX_OUTPUT_ITEMS:
            return self._failure(f"max_output_items must be between 1 and {MAX_OUTPUT_ITEMS}")
        if not 1024 <= max_output_bytes <= MAX_OUTPUT_BYTES:
            return self._failure(f"max_output_bytes must be between 1024 and {MAX_OUTPUT_BYTES}")
        if not 1 <= checkpoint_interval <= 1000:
            return self._failure("checkpoint_interval must be between 1 and 1000")
        if not 256 <= text_chunk_chars <= 1_000_000:
            return self._failure("text_chunk_chars must be between 256 and 1000000")
        if not 1 <= table_batch_rows <= 5000:
            return self._failure("table_batch_rows must be between 1 and 5000")

        store = self._injected_state_store
        production_store = store is None
        if store is None:
            if context.db_session is None:
                return self._failure("Document source state database is unavailable")
            store = cast(
                _StudioStateStore,
                StudioSDKSourceStateStore(
                    context.db_session,
                    run_id=UUID(context.run_id),
                    owner_id=owner_id,
                    graph_id=graph_id,
                    node_id=context.node_id,
                    job_id=UUID(context.job_id) if context.job_id is not None else None,
                    lease_owner=context.lease_owner,
                ),
            )
        pipeline = store.pipeline_key

        connector: LocalDocumentsSource | None = None
        try:
            await store.acquire_lock()
            saved_streams = set(await store.list_streams(pipeline, SOURCE_NAME))
            selected_streams = {_stream_name(item): item for item in artifact_ids}
            configured: list[tuple[UUID, str, Any]] = []

            for stream, artifact_id in selected_streams.items():
                try:
                    path = upload_service.resolve_uploads(owner_id, [str(artifact_id)])[0]
                except ValueError:
                    if stream not in saved_streams:
                        return self._failure("Upload not found or does not belong to graph owner")
                    path = upload_service.upload_reconciliation_path(owner_id, artifact_id)
                if path.exists() and path.suffix.lower() not in self.studio_accepted_extensions:
                    return self._failure(
                        f"Upload type {path.suffix.lower() or '<none>'} is not supported "
                        "by Document Source"
                    )
                configured.append((artifact_id, stream, path))

            for stream in sorted(saved_streams - set(selected_streams)):
                prior_artifact_id = _artifact_id(stream)
                if prior_artifact_id is None:
                    continue
                configured.append(
                    (
                        prior_artifact_id,
                        stream,
                        upload_service.upload_reconciliation_path(owner_id, prior_artifact_id),
                    )
                )

            if not configured:
                return NodeResult(
                    success=True,
                    output_data={"items": []},
                    metadata={"sdk_connector": SOURCE_NAME, "incremental_delta": True},
                )

            connector = LocalDocumentsSource(
                [item[2] for item in configured],
                stream_names=[item[1] for item in configured],
                checkpoint_interval=checkpoint_interval,
                text_chunk_chars=text_chunk_chars,
                table_batch_rows=table_batch_rows,
                extensions=self.studio_accepted_extensions,
                ocr_mode=ocr_mode,
                table_mode=table_mode,
                failure_mode=failure_mode,
                min_native_text_quality=min_native_text_quality,
            )
            check = await connector.check()
            if not check.ok:
                return self._failure("Document SDK source check failed")

            artifact_by_stream = {stream: artifact_id for artifact_id, stream, _ in configured}
            items: list[dict[str, Any]] = []
            output_bytes = 0
            pending_states: dict[str, Mapping[str, Any]] = {}
            streams = await connector.discover()
            for descriptor in streams:
                saved_state = await store.load(pipeline, SOURCE_NAME, descriptor.name)
                final_state: Mapping[str, Any] | None = None
                async for message in connector.read(descriptor, saved_state):
                    if isinstance(message, RecordMessage):
                        if len(items) >= max_output_items:
                            raise _OutputLimitExceeded("max_output_items")
                        item = _sanitized_envelope(
                            message.envelope,
                            artifact_by_stream[descriptor.name],
                        )
                        item_bytes = len(
                            json.dumps(
                                item,
                                ensure_ascii=False,
                                separators=(",", ":"),
                            ).encode("utf-8")
                        )
                        if output_bytes + item_bytes > max_output_bytes:
                            raise _OutputLimitExceeded("max_output_bytes")
                        items.append(item)
                        output_bytes += item_bytes
                    elif isinstance(message, StateMessage):
                        final_state = dict(message.state)
                if final_state is None:
                    raise RuntimeError("Document SDK source ended without a state message")
                pending_states[descriptor.name] = final_state

            # Do not stage any candidate until every configured SDK stream completed.
            # POST_EXEC makes the candidate durable; whole-run completion promotes it.
            if production_store:
                async with context.db_session.begin_nested():
                    await _stage_states(
                        store,
                        pipeline=pipeline,
                        selected_streams=set(selected_streams),
                        pending_states=pending_states,
                    )
                    await store.revalidate_lease()
            else:
                await _stage_states(
                    store,
                    pipeline=pipeline,
                    selected_streams=set(selected_streams),
                    pending_states=pending_states,
                )

            operations: dict[str, int] = {}
            for item in items:
                operation = str(item["operation"])
                operations[operation] = operations.get(operation, 0) + 1
            return NodeResult(
                success=True,
                output_data={"items": items},
                items_processed=len(items),
                metadata={
                    "sdk_connector": SOURCE_NAME,
                    "incremental_delta": True,
                    "artifact_count": len(artifact_ids),
                    "operations": operations,
                },
            )
        except SDKSourceStateLeaseError:
            raise
        except _OutputLimitExceeded as exc:
            return self._failure(
                f"Document delta exceeds {exc}; narrow the selection or raise the limit"
            )
        except Exception as exc:
            return self._failure(f"Document SDK source failed: {type(exc).__name__}")
        finally:
            if connector is not None:
                await connector.close()

    @staticmethod
    def _failure(message: str) -> NodeResult:
        return NodeResult(success=False, output_data={"items": []}, error_message=message)


def _stream_name(artifact_id: UUID) -> str:
    return f"{STREAM_PREFIX}{artifact_id}"


def _artifact_id(stream: str) -> UUID | None:
    if not stream.startswith(STREAM_PREFIX):
        return None
    try:
        return UUID(stream.removeprefix(STREAM_PREFIX))
    except ValueError:
        return None


def _sanitized_envelope(envelope: Any, artifact_id: UUID) -> dict[str, Any]:
    value = envelope.to_dict()
    metadata = dict(value.get("metadata") or {})
    for key in ("path", "root", "source_path", "original_path", "absolute_path"):
        metadata.pop(key, None)
    metadata["artifact_id"] = str(artifact_id)
    value["metadata"] = metadata
    provenance = dict(value.get("provenance") or {})
    provenance.pop("path", None)
    provenance["artifact_id"] = str(artifact_id)
    value["provenance"] = provenance
    return value


async def _stage_states(
    store: _StudioStateStore,
    *,
    pipeline: str,
    selected_streams: set[str],
    pending_states: Mapping[str, Mapping[str, Any]],
) -> None:
    for stream, state in pending_states.items():
        if stream in selected_streams:
            await store.save(pipeline, SOURCE_NAME, stream, state)
        else:
            await store.delete(pipeline, SOURCE_NAME, stream)


class SDKOCRDocumentSourceNode(SDKDocumentSourceNode):
    @property
    def node_type(self) -> str:
        return "sdk_ocr_document_source"

    @property
    def display_name(self) -> str:
        return "OCR Document Source (SDK)"

    @property
    def description(self) -> str:
        return "Read managed uploads with local-first OCR and table recovery"

    @property
    def studio_accepted_extensions(self) -> tuple[str, ...]:
        return SUPPORTED_EXTENSIONS + OCR_IMAGE_EXTENSIONS

    @property
    def studio_default_ocr_mode(self) -> str:
        return "auto"


def register() -> None:
    from app.nodes.registry import register_node

    register_node(SDKDocumentSourceNode())
    register_node(SDKOCRDocumentSourceNode())
