"""Studio boundary tests for the SDK local-document adapter."""

from __future__ import annotations

import json
from io import BytesIO
from typing import Any
from uuid import UUID, uuid4

import pytest
from app.config import settings
from app.nodes.base import NodeContext
from app.nodes.sdk_document_source import SDKDocumentSourceNode
from app.services import upload_service
from fastapi import UploadFile


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
