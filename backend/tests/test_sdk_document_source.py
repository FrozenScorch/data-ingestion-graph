"""Studio boundary tests for the SDK local-document adapter."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from io import BytesIO
from typing import Any
from uuid import UUID, uuid4

import pytest
from app.config import settings
from app.nodes import sdk_document_source as sdk_document_source_module
from app.nodes.base import NodeContext
from app.nodes.sdk_document_source import SDKDocumentSourceNode
from app.services import upload_service
from fastapi import UploadFile
from ingestion_graph.connectors import CheckResult, StreamDescriptor
from ingestion_graph.messages import RecordMessage, StateMessage
from ingestion_graph.models import DocumentElement, Envelope, TableBatch


class MemoryStudioStateStore:
    def __init__(self, *, owner_id: UUID, graph_id: UUID, node_id: str) -> None:
        self.pipeline_key = f"studio:{owner_id}:{graph_id}:{node_id}"
        self.states: dict[tuple[str, str], dict[str, Any]] = {}

    async def acquire_lock(self) -> None:
        return None

    async def load(self, pipeline: str, source: str, stream: str) -> dict[str, Any]:
        assert pipeline == self.pipeline_key
        return dict(self.states.get((source, stream), {}))

    async def save(
        self,
        pipeline: str,
        source: str,
        stream: str,
        state: dict[str, Any],
    ) -> None:
        assert pipeline == self.pipeline_key
        self.states[(source, stream)] = dict(state)

    async def list_streams(self, pipeline: str, source: str) -> list[str]:
        assert pipeline == self.pipeline_key
        return [stream for saved_source, stream in self.states if saved_source == source]

    async def delete(self, pipeline: str, source: str, stream: str) -> None:
        assert pipeline == self.pipeline_key
        self.states.pop((source, stream), None)


@pytest.fixture
def upload_root(monkeypatch, tmp_path):
    monkeypatch.setattr(settings, "upload_dir", str(tmp_path))
    return tmp_path


async def _save(owner_id: UUID, name: str, content: bytes) -> dict[str, Any]:
    return await upload_service.save_upload(
        owner_id,
        UploadFile(filename=name, file=BytesIO(content)),
    )


def _context(
    *,
    owner_id: UUID,
    graph_id: UUID,
    config: dict[str, Any],
) -> NodeContext:
    return NodeContext(
        run_id=str(uuid4()),
        node_id="documents",
        config=config,
        state={"owner_id": str(owner_id), "graph_id": str(graph_id)},
    )


def _source_type(envelope_factory):
    class StubSource:
        def __init__(
            self,
            paths,
            *,
            stream_names,
            **_kwargs,
        ):
            self.path = paths[0]
            self.stream_name = stream_names[0]

        async def check(self):
            return CheckResult(True)

        async def discover(self):
            return [StreamDescriptor(self.stream_name)]

        async def read(self, stream, _state):
            yield RecordMessage(envelope_factory(self.path, stream.name))
            yield StateMessage(stream.name, {"cursor": "complete"})

        async def close(self):
            return None

    return StubSource


@pytest.mark.asyncio
async def test_adapter_uses_owner_scoped_artifacts_and_rejects_paths(upload_root):
    owner, other_owner, graph_id = uuid4(), uuid4(), uuid4()
    saved = await _save(owner, "notes.txt", b"private")
    other_store = MemoryStudioStateStore(
        owner_id=other_owner, graph_id=graph_id, node_id="documents"
    )
    other_node = SDKDocumentSourceNode(other_store)  # type: ignore[arg-type]

    isolated = await other_node.execute(
        _context(
            owner_id=other_owner,
            graph_id=graph_id,
            config={"artifact_ids": [saved["id"]]},
        )
    )
    assert isolated.success is False
    assert isolated.output_data == {"items": []}
    assert "does not belong" in (isolated.error_message or "")

    rejected = await other_node.execute(
        _context(
            owner_id=other_owner,
            graph_id=graph_id,
            config={"paths": [str(upload_root / "secret.txt")]},
        )
    )
    assert rejected.success is False
    assert "artifact IDs only" in (rejected.error_message or "")
    assert str(upload_root) not in (rejected.error_message or "")


@pytest.mark.asyncio
async def test_adapter_emits_sanitized_canonical_envelopes(upload_root):
    owner_id, graph_id = uuid4(), uuid4()
    saved = await _save(owner_id, "notes.txt", b"hello from the SDK")
    store = MemoryStudioStateStore(owner_id=owner_id, graph_id=graph_id, node_id="documents")
    result = await SDKDocumentSourceNode(store).execute(  # type: ignore[arg-type]
        _context(
            owner_id=owner_id,
            graph_id=graph_id,
            config={"artifact_ids": [saved["id"]]},
        )
    )

    assert result.success is True
    assert result.items_processed == 1
    item = result.output_data["items"][0]
    assert item["id"]
    assert item["operation"] == "upsert"
    assert item["stream"] == f"upload-{saved['id']}"
    assert item["provenance"]["artifact_id"] == saved["id"]
    assert item["metadata"]["artifact_id"] == saved["id"]
    assert "path" not in item["metadata"]
    assert "path" not in item["provenance"]
    assert str(upload_root) not in json.dumps(item)
    assert str(upload_root) not in json.dumps(result.output_data)


@pytest.mark.asyncio
async def test_adapter_preserves_table_payload_keys_while_sanitizing_context(
    upload_root,
    monkeypatch,
):
    owner_id, graph_id = uuid4(), uuid4()
    saved = await _save(owner_id, "table.txt", b"table")
    store = MemoryStudioStateStore(owner_id=owner_id, graph_id=graph_id, node_id="documents")
    expected_payload = {
        "columns": ["path", "root", "value"],
        "rows": [
            {
                "path": "customer/imports/report.csv",
                "root": "north-america",
                "value": 42,
            }
        ],
        "kind": "table_batch",
    }

    def envelope_factory(path, stream):
        return Envelope(
            id="record",
            source="local_documents",
            stream=stream,
            payload=TableBatch(
                ("path", "root", "value"),
                (
                    {
                        "path": "customer/imports/report.csv",
                        "root": "north-america",
                        "value": 42,
                    },
                ),
            ),
            metadata={
                "relative_path": path.name,
                "nested": {"source_path": str(path.resolve()), "safe": "preserved"},
            },
            provenance={
                "path": str(path.resolve()),
                "nested": {"absolute_path": str(path.resolve()), "safe": "preserved"},
            },
        )

    monkeypatch.setattr(
        sdk_document_source_module,
        "LocalDocumentsSource",
        _source_type(envelope_factory),
    )

    result = await SDKDocumentSourceNode(store).execute(  # type: ignore[arg-type]
        _context(
            owner_id=owner_id,
            graph_id=graph_id,
            config={"artifact_ids": [saved["id"]]},
        )
    )

    assert result.success is True
    item = result.output_data["items"][0]
    assert item["payload"] == expected_payload
    assert item["metadata"]["nested"] == {"safe": "preserved"}
    assert item["provenance"]["nested"] == {"safe": "preserved"}
    assert str(upload_root) not in json.dumps(item)


@pytest.mark.asyncio
async def test_adapter_redacts_obfuscated_file_uris_only_from_context(
    upload_root,
    monkeypatch,
):
    owner_id, graph_id = uuid4(), uuid4()
    saved = await _save(owner_id, "table.txt", b"table")
    store = MemoryStudioStateStore(owner_id=owner_id, graph_id=graph_id, node_id="documents")
    safe_value = " \thttps://example.test/document"

    def envelope_factory(_path, stream):
        return Envelope(
            id="record",
            source="local_documents",
            stream=stream,
            payload=DocumentElement("file:///user-supplied-content"),
            metadata={
                "safe_uri": safe_value,
                "nested": {
                    "uri": " \x00\tFiLe:///server/private.pdf",
                    "link": "\x1ffile:///server/private.png",
                    "unicode_space": "\u00a0file:///server/private.webp",
                },
                "references": ["kept", "\x01 file:///server/private.tiff"],
            },
            provenance={"uri": "\r\nfile:///server/private.pdf"},
        )

    monkeypatch.setattr(
        sdk_document_source_module,
        "LocalDocumentsSource",
        _source_type(envelope_factory),
    )

    result = await SDKDocumentSourceNode(store).execute(  # type: ignore[arg-type]
        _context(
            owner_id=owner_id,
            graph_id=graph_id,
            config={"artifact_ids": [saved["id"]]},
        )
    )

    assert result.success is True
    item = result.output_data["items"][0]
    assert item["payload"]["text"] == "file:///user-supplied-content"
    assert item["metadata"]["safe_uri"] == safe_value
    assert item["metadata"]["nested"] == {}
    assert item["metadata"]["references"] == ["kept"]
    assert "uri" not in item["provenance"]
    context_json = json.dumps({"metadata": item["metadata"], "provenance": item["provenance"]})
    assert "file:" not in context_json.casefold()


@pytest.mark.asyncio
async def test_adapter_rejects_artifact_retention_without_an_artifact_store(
    upload_root,
    monkeypatch,
):
    owner_id, graph_id = uuid4(), uuid4()
    saved = await _save(owner_id, "table.txt", b"table")
    store = MemoryStudioStateStore(owner_id=owner_id, graph_id=graph_id, node_id="documents")

    def unexpected_source(*_args, **_kwargs):
        raise AssertionError("SDK source must not be constructed")

    monkeypatch.setattr(sdk_document_source_module, "LocalDocumentsSource", unexpected_source)

    result = await SDKDocumentSourceNode(store).execute(  # type: ignore[arg-type]
        _context(
            owner_id=owner_id,
            graph_id=graph_id,
            config={
                "artifact_ids": [saved["id"]],
                "retain_extraction_artifacts": True,
            },
        )
    )

    assert result.success is False
    assert "does not support retain_extraction_artifacts" in (result.error_message or "")
    assert result.output_data == {"items": []}
    assert store.states == {}


@pytest.mark.asyncio
async def test_output_byte_limit_counts_json_wrapper_and_accepts_exact_boundary(
    upload_root,
    monkeypatch,
):
    owner_id, graph_id = uuid4(), uuid4()
    saved = await _save(owner_id, "table.txt", b"table")
    artifact_id = UUID(saved["id"])
    stream_name = f"upload-{artifact_id}"
    observed_at = datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC)

    def envelope_with_text(text: str, stream: str) -> Envelope:
        return Envelope(
            id="record",
            source="local_documents",
            stream=stream,
            payload=DocumentElement(text),
            observed_at=observed_at,
        )

    envelope: Envelope | None = None
    output_size = 0
    for text_size in range(2_000):
        candidate = envelope_with_text("x" * text_size, stream_name)
        item = sdk_document_source_module._sanitized_envelope(candidate, artifact_id)
        item_size = len(json.dumps(item, ensure_ascii=False, separators=(",", ":")).encode("utf-8"))
        candidate_output_size = len(
            json.dumps({"items": [item]}, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        )
        if item_size <= 1_024 < candidate_output_size:
            envelope = candidate
            output_size = candidate_output_size
            break
    assert envelope is not None
    selected_envelope = envelope

    def envelope_factory(_path, stream):
        assert stream == stream_name
        return selected_envelope

    monkeypatch.setattr(
        sdk_document_source_module,
        "LocalDocumentsSource",
        _source_type(envelope_factory),
    )

    rejected_store = MemoryStudioStateStore(
        owner_id=owner_id,
        graph_id=graph_id,
        node_id="documents",
    )
    rejected = await SDKDocumentSourceNode(rejected_store).execute(  # type: ignore[arg-type]
        _context(
            owner_id=owner_id,
            graph_id=graph_id,
            config={"artifact_ids": [saved["id"]], "max_output_bytes": 1_024},
        )
    )
    assert rejected.success is False
    assert "max_output_bytes" in (rejected.error_message or "")
    assert rejected_store.states == {}

    accepted_store = MemoryStudioStateStore(
        owner_id=owner_id,
        graph_id=graph_id,
        node_id="documents",
    )
    accepted = await SDKDocumentSourceNode(accepted_store).execute(  # type: ignore[arg-type]
        _context(
            owner_id=owner_id,
            graph_id=graph_id,
            config={"artifact_ids": [saved["id"]], "max_output_bytes": output_size},
        )
    )
    assert accepted.success is True
    serialized = json.dumps(
        accepted.output_data,
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")
    assert len(serialized) == output_size
    assert accepted_store.states


@pytest.mark.asyncio
async def test_repeat_runs_resume_and_deselection_emits_deletes(upload_root):
    owner_id, graph_id = uuid4(), uuid4()
    saved = await _save(owner_id, "long.txt", (b"many words " * 800))
    store = MemoryStudioStateStore(owner_id=owner_id, graph_id=graph_id, node_id="documents")
    node = SDKDocumentSourceNode(store)  # type: ignore[arg-type]
    selected = {"artifact_ids": [saved["id"]], "text_chunk_chars": 256}

    first = await node.execute(_context(owner_id=owner_id, graph_id=graph_id, config=selected))
    second = await node.execute(_context(owner_id=owner_id, graph_id=graph_id, config=selected))
    removed = await node.execute(
        _context(owner_id=owner_id, graph_id=graph_id, config={"artifact_ids": []})
    )
    settled = await node.execute(
        _context(owner_id=owner_id, graph_id=graph_id, config={"artifact_ids": []})
    )

    assert first.success is True and first.items_processed > 1
    assert second.success is True and second.output_data["items"] == []
    assert removed.success is True
    assert len(removed.output_data["items"]) == first.items_processed
    assert {item["operation"] for item in removed.output_data["items"]} == {"delete"}
    assert settled.success is True and settled.output_data["items"] == []
    assert store.states == {}


@pytest.mark.asyncio
async def test_legacy_doc_upload_is_excluded_from_sdk_adapter(upload_root):
    owner_id, graph_id = uuid4(), uuid4()
    saved = await _save(owner_id, "legacy.doc", b"legacy")
    store = MemoryStudioStateStore(owner_id=owner_id, graph_id=graph_id, node_id="documents")
    result = await SDKDocumentSourceNode(store).execute(  # type: ignore[arg-type]
        _context(
            owner_id=owner_id,
            graph_id=graph_id,
            config={"artifact_ids": [saved["id"]]},
        )
    )
    assert result.success is False
    assert ".doc" in (result.error_message or "")


@pytest.mark.asyncio
async def test_output_limit_fails_without_advancing_state(upload_root):
    owner_id, graph_id = uuid4(), uuid4()
    saved = await _save(owner_id, "large.txt", b"many words " * 800)
    store = MemoryStudioStateStore(owner_id=owner_id, graph_id=graph_id, node_id="documents")
    result = await SDKDocumentSourceNode(store).execute(  # type: ignore[arg-type]
        _context(
            owner_id=owner_id,
            graph_id=graph_id,
            config={
                "artifact_ids": [saved["id"]],
                "text_chunk_chars": 256,
                "max_output_items": 2,
            },
        )
    )
    assert result.success is False
    assert "max_output_items" in (result.error_message or "")
    assert result.output_data == {"items": []}
    assert store.states == {}


@pytest.mark.asyncio
async def test_output_byte_limit_fails_without_advancing_state(upload_root):
    owner_id, graph_id = uuid4(), uuid4()
    saved = await _save(owner_id, "large.txt", b"x" * 2_000)
    store = MemoryStudioStateStore(owner_id=owner_id, graph_id=graph_id, node_id="documents")
    result = await SDKDocumentSourceNode(store).execute(  # type: ignore[arg-type]
        _context(
            owner_id=owner_id,
            graph_id=graph_id,
            config={"artifact_ids": [saved["id"]], "max_output_bytes": 1_024},
        )
    )
    assert result.success is False
    assert "max_output_bytes" in (result.error_message or "")
    assert result.output_data == {"items": []}
    assert store.states == {}
