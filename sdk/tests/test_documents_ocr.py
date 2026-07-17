from __future__ import annotations

import hashlib

import pytest

from ingestion_graph.document_ai import (
    ComponentDescriptor,
    ExtractionWarning,
    MemoryExtractionCache,
    OcrResult,
    SplitChunk,
    SQLiteExtractionCache,
    TableArtifact,
    TableCell,
)
from ingestion_graph.messages import LogMessage, RecordMessage, StateMessage
from ingestion_graph.models import DocumentElement, Operation, TableBatch
from ingestion_graph.sources.documents import LocalDocumentsSource


class FakeOcr:
    descriptor = ComponentDescriptor("fake-ocr", "1", deterministic=True)

    async def check(self):
        from ingestion_graph.connectors.base import CheckResult

        return CheckResult(True)

    async def recognize(self, image: bytes, *, language: str = "eng") -> OcrResult:
        assert image == b"image-bytes"
        return OcrResult("recognized table text", confidence=0.91)

    async def close(self):
        return None


@pytest.mark.asyncio
async def test_explicit_image_ocr_uses_snapshot_and_emits_provenance(tmp_path):
    path = tmp_path / "scan.png"
    path.write_bytes(b"image-bytes")
    source = LocalDocumentsSource(
        path,
        extensions=(".png",),
        ocr_mode="auto",
        ocr_engine=FakeOcr(),
        stream_names=("scan",),
    )
    descriptor = (await source.discover())[0]
    messages = [item async for item in source.read(descriptor)]
    record = messages[0].envelope
    assert isinstance(record.payload, DocumentElement)
    assert record.payload.text == "recognized table text"
    assert record.metadata["extraction_mode"] == "ocr"
    assert record.metadata["ocr_confidence"] == 0.91


def test_images_are_not_accepted_by_legacy_defaults(tmp_path):
    path = tmp_path / "scan.png"
    path.write_bytes(b"image")
    with pytest.raises(Exception, match="Image extensions"):
        LocalDocumentsSource(path, extensions=(".png",))


class FailingOcr(FakeOcr):
    async def recognize(self, image: bytes, *, language: str = "eng") -> OcrResult:
        raise RuntimeError("transient OCR failure")


@pytest.mark.asyncio
async def test_best_effort_failure_preserves_prior_records_and_checkpoint(tmp_path):
    path = tmp_path / "scan.png"
    path.write_bytes(b"image-bytes")
    source = LocalDocumentsSource(
        path,
        extensions=(".png",),
        ocr_mode="always",
        ocr_engine=FailingOcr(),
        failure_mode="best_effort",
        stream_names=("scan",),
    )
    stream = (await source.discover())[0]
    parser = source._parser_fingerprint()
    prior = {
        "parser_fingerprint": parser,
        "files": {
            "scan.png": {
                "sha256": "0" * 64,
                "element_count": 2,
                "parser_fingerprint": parser,
            }
        },
    }

    messages = [item async for item in source.read(stream, prior)]

    assert any(isinstance(item, LogMessage) for item in messages)
    assert not any(isinstance(item, RecordMessage) for item in messages)
    final = [item for item in messages if isinstance(item, StateMessage)][-1].state
    assert final["files"] == prior["files"]


@pytest.mark.asyncio
async def test_later_files_cannot_overwrite_failed_in_progress_state(tmp_path):
    failed = tmp_path / "a.png"
    failed.write_bytes(b"image-bytes")
    (tmp_path / "b.txt").write_text("many words " * 100, encoding="utf-8")
    source = LocalDocumentsSource(
        tmp_path,
        extensions=(".png", ".txt"),
        ocr_mode="always",
        ocr_engine=FailingOcr(),
        failure_mode="best_effort",
        checkpoint_interval=1,
        text_chunk_chars=256,
        stream_names=("documents",),
    )
    stream = (await source.discover())[0]
    parser = source._parser_fingerprint()
    state = {
        "parser_fingerprint": parser,
        "files": {},
        "in_progress": {
            "relative_path": "a.png",
            "sha256": hashlib.sha256(b"image-bytes").hexdigest(),
            "next_index": 2,
            "parser_fingerprint": parser,
        },
    }

    messages = [item async for item in source.read(stream, state)]
    final = [item for item in messages if isinstance(item, StateMessage)][-1].state
    assert final["in_progress"]["relative_path"] == "a.png"

    failed.unlink()
    retried = [item async for item in source.read(stream, final)]
    deletes = [
        item
        for item in retried
        if isinstance(item, RecordMessage) and item.envelope.operation is Operation.DELETE
    ]
    assert len(deletes) == 2


class RecordingOcr(FakeOcr):
    def __init__(self) -> None:
        self.calls = 0

    async def recognize(self, image: bytes, *, language: str = "eng") -> OcrResult:
        self.calls += 1
        return OcrResult(
            "cached text",
            confidence=0.8,
            warnings=(ExtractionWarning("low_contrast", "Low contrast", 1),),
            usage={"engine_ms": 5},
        )


@pytest.mark.asyncio
async def test_ocr_cache_hit_preserves_warnings_and_payload(tmp_path):
    path = tmp_path / "scan.png"
    path.write_bytes(b"image-bytes")
    engine = RecordingOcr()
    cache = MemoryExtractionCache()
    source = LocalDocumentsSource(
        path,
        extensions=(".png",),
        ocr_mode="always",
        ocr_engine=engine,
        extraction_cache=cache,
        stream_names=("scan",),
    )
    stream = (await source.discover())[0]

    first = [item async for item in source.read(stream)]
    second = [item async for item in source.read(stream)]
    first_record = next(item for item in first if isinstance(item, RecordMessage))
    second_record = next(item for item in second if isinstance(item, RecordMessage))

    assert engine.calls == 1
    assert first_record.envelope.payload == second_record.envelope.payload
    assert first_record.envelope.metadata["warnings"] == second_record.envelope.metadata["warnings"]


class FakeSplitter:
    descriptor = ComponentDescriptor("fake-splitter", "1", deterministic=True)

    def __init__(self, prefix: str = "split") -> None:
        self.prefix = prefix
        self.calls = 0

    async def split(self, element: DocumentElement):
        self.calls += 1
        return (SplitChunk(f"{self.prefix}:{element.text}"),)

    async def close(self):
        return None


@pytest.mark.asyncio
async def test_custom_splitter_runs_for_text_but_never_tables(tmp_path):
    (tmp_path / "notes.txt").write_text("hello", encoding="utf-8")
    (tmp_path / "rows.csv").write_text("name\nAda\n", encoding="utf-8")
    splitter = FakeSplitter()
    source = LocalDocumentsSource(
        tmp_path,
        document_splitter=splitter,
        stream_names=("documents",),
    )
    stream = (await source.discover())[0]
    messages = [item async for item in source.read(stream)]
    payloads = [item.envelope.payload for item in messages if isinstance(item, RecordMessage)]

    assert splitter.calls == 1
    assert any(
        isinstance(item, DocumentElement) and item.text == "split:hello" for item in payloads
    )
    assert any(isinstance(item, TableBatch) for item in payloads)


class NondeterministicSplitter(FakeSplitter):
    descriptor = ComponentDescriptor("nondeterministic-splitter", "1", deterministic=False)

    async def split(self, element: DocumentElement):
        self.calls += 1
        return (
            SplitChunk(f"{self.prefix}-a"),
            SplitChunk(f"{self.prefix}-b"),
        )


@pytest.mark.asyncio
async def test_nondeterministic_split_manifest_is_reused_on_resume(tmp_path):
    path = tmp_path / "notes.txt"
    path.write_text("hello", encoding="utf-8")
    cache_path = tmp_path / "manifest.db"
    first_splitter = NondeterministicSplitter("first")
    first_source = LocalDocumentsSource(
        path,
        document_splitter=first_splitter,
        extraction_cache=SQLiteExtractionCache(cache_path),
        checkpoint_interval=1,
        stream_names=("notes",),
    )
    stream = (await first_source.discover())[0]
    first = [item async for item in first_source.read(stream)]
    checkpoint = next(
        item.state
        for item in first
        if isinstance(item, StateMessage)
        and item.state.get("in_progress", {}).get("next_index") == 1
    )
    assert checkpoint["in_progress"]["manifest_cache_key"]

    second_splitter = NondeterministicSplitter("different")
    second_source = LocalDocumentsSource(
        path,
        document_splitter=second_splitter,
        extraction_cache=SQLiteExtractionCache(cache_path),
        checkpoint_interval=1,
        stream_names=("notes",),
    )
    resumed = [item async for item in second_source.read(stream, checkpoint)]
    resumed_records = [item for item in resumed if isinstance(item, RecordMessage)]

    assert second_splitter.calls == 0
    assert [item.envelope.payload.text for item in resumed_records] == ["first-b"]
    assert all(item.envelope.operation is Operation.UPSERT for item in resumed_records)


class FakeTableExtractor:
    descriptor = ComponentDescriptor("fake-table", "1", deterministic=True)

    async def extract(self, image: bytes, *, page_number: int | None = None):
        return (
            TableArtifact(
                "table-1",
                page_number,
                None,
                (
                    TableCell(0, 0, "Name", header_level=0),
                    TableCell(1, 0, "Ada"),
                ),
                2,
                1,
            ),
        )

    async def close(self):
        return None


@pytest.mark.asyncio
@pytest.mark.parametrize("retain", [False, True])
async def test_table_artifact_metadata_requires_explicit_retention(tmp_path, retain):
    path = tmp_path / "table.png"
    path.write_bytes(b"image-bytes")
    source = LocalDocumentsSource(
        path,
        extensions=(".png",),
        ocr_mode="always",
        ocr_engine=FakeOcr(),
        table_mode="local",
        table_extractor=FakeTableExtractor(),
        retain_extraction_artifacts=retain,
        stream_names=("table",),
    )
    stream = (await source.discover())[0]
    table_records = [
        item
        async for item in source.read(stream)
        if isinstance(item, RecordMessage) and isinstance(item.envelope.payload, TableBatch)
    ]

    assert len(table_records) == 1
    assert ("table_artifact" in table_records[0].envelope.metadata) is retain
    assert table_records[0].envelope.metadata["table_id"] == "table-1"


def test_artifact_retention_changes_enhanced_parser_fingerprint(tmp_path):
    path = tmp_path / "table.png"
    path.write_bytes(b"image-bytes")
    common = {
        "paths": path,
        "extensions": (".png",),
        "ocr_mode": "always",
        "ocr_engine": FakeOcr(),
        "table_mode": "local",
        "table_extractor": FakeTableExtractor(),
    }

    without_artifacts = LocalDocumentsSource(**common, retain_extraction_artifacts=False)
    with_artifacts = LocalDocumentsSource(**common, retain_extraction_artifacts=True)

    assert without_artifacts._parser_fingerprint() != with_artifacts._parser_fingerprint()
