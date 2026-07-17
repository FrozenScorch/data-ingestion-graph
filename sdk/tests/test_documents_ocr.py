from __future__ import annotations

import asyncio
import base64
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
from ingestion_graph.errors import ConfigurationError
from ingestion_graph.messages import LogMessage, RecordMessage, StateMessage
from ingestion_graph.models import DocumentElement, Operation, TableBatch
from ingestion_graph.sources.documents import LocalDocumentsSource

VALID_PNG = base64.b64decode(
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNk+A8AAQUBAScY42YAAAAASUVORK5CYII="
)


class FakeOcr:
    descriptor = ComponentDescriptor("fake-ocr", "1", deterministic=True)

    async def check(self):
        from ingestion_graph.connectors.base import CheckResult

        return CheckResult(True)

    async def recognize(self, image: bytes, *, language: str = "eng") -> OcrResult:
        assert image == VALID_PNG
        return OcrResult("recognized table text", confidence=0.91)

    async def close(self):
        return None


@pytest.mark.asyncio
async def test_explicit_image_ocr_uses_snapshot_and_emits_provenance(tmp_path):
    path = tmp_path / "scan.png"
    path.write_bytes(VALID_PNG)
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


def test_manifest_advertises_opt_in_images_but_keeps_legacy_default():
    extensions = LocalDocumentsSource.manifest().config_schema["properties"]["extensions"]

    assert ".png" in extensions["items"]["enum"]
    assert ".png" not in extensions["default"]


@pytest.mark.asyncio
async def test_pdf_page_concurrency_is_bounded_and_output_order_is_stable(tmp_path, monkeypatch):
    from ingestion_graph.sources import documents

    class FakePage:
        def __init__(self, page_number: int) -> None:
            self.page_number = page_number

        def extract_text(self) -> str:
            return f"native-{self.page_number}"

    class FakeReader:
        def __init__(self, _path) -> None:
            self.pages = [FakePage(index) for index in range(1, 5)]

    class TrackingRenderer:
        descriptor = ComponentDescriptor("tracking-renderer", "1", deterministic=True)

        def __init__(self) -> None:
            self.active = 0
            self.max_active = 0

        async def render(self, _pdf: bytes, *, page_number: int, dpi: int) -> bytes:
            self.active += 1
            self.max_active = max(self.max_active, self.active)
            try:
                await asyncio.sleep(0.01 * (5 - page_number))
                return f"image-{page_number}".encode()
            finally:
                self.active -= 1

        async def close(self) -> None:
            return None

    class PageOcr(FakeOcr):
        async def recognize(self, image: bytes, *, language: str = "eng") -> OcrResult:
            return OcrResult(image.decode(), confidence=1.0)

    monkeypatch.setattr(documents, "PdfReader", FakeReader)
    path = tmp_path / "pages.pdf"
    path.write_bytes(b"snapshot-pdf")
    renderer = TrackingRenderer()
    source = LocalDocumentsSource(
        path,
        ocr_mode="always",
        ocr_engine=PageOcr(),
        page_renderer=renderer,
        max_page_concurrency=2,
    )

    elements = await source._parse_file_with_document_ai(path)

    # Shared component instances are serialized because protocols do not require reentrancy;
    # the page window still bounds all queued work to max_page_concurrency.
    assert renderer.max_active == 1
    assert [element.payload.page_number for element in elements] == [1, 2, 3, 4]
    assert [element.payload.text for element in elements] == [
        "image-1",
        "image-2",
        "image-3",
        "image-4",
    ]


@pytest.mark.asyncio
async def test_page_timeout_covers_splitter_work(tmp_path):
    class SlowSplitter:
        descriptor = ComponentDescriptor("slow-splitter", "1", deterministic=True)

        async def split(self, _element: DocumentElement):
            await asyncio.sleep(1)
            return (SplitChunk("late"),)

        async def close(self) -> None:
            return None

    path = tmp_path / "scan.png"
    path.write_bytes(VALID_PNG)
    source = LocalDocumentsSource(
        path,
        extensions=(".png",),
        ocr_mode="always",
        ocr_engine=FakeOcr(),
        document_splitter=SlowSplitter(),
        page_timeout_seconds=0.01,
    )

    with pytest.raises(TimeoutError):
        await source._parse_file_with_document_ai(path)


@pytest.mark.asyncio
async def test_oversized_encoded_image_is_rejected_before_ocr(tmp_path):
    import struct
    import zlib

    def chunk(name: bytes, value: bytes) -> bytes:
        return (
            struct.pack(">I", len(value))
            + name
            + value
            + struct.pack(">I", zlib.crc32(name + value) & 0xFFFFFFFF)
        )

    oversized = (
        b"\x89PNG\r\n\x1a\n"
        + chunk(b"IHDR", struct.pack(">IIBBBBB", 100_000, 100_000, 8, 2, 0, 0, 0))
        + chunk(b"IEND", b"")
    )
    path = tmp_path / "oversized.png"
    path.write_bytes(oversized)
    engine = RecordingOcr()
    source = LocalDocumentsSource(
        path,
        extensions=(".png",),
        ocr_mode="always",
        ocr_engine=engine,
    )

    with pytest.raises(ConfigurationError, match="invalid or exceeds safety limits"):
        await source._parse_file_with_document_ai(path)
    assert engine.calls == 0


@pytest.mark.asyncio
async def test_pdf_vision_receives_one_nonempty_rendered_page(tmp_path, monkeypatch):
    from ingestion_graph.sources import documents

    class FakePage:
        def extract_text(self) -> str:
            return "clean native PDF text"

    class FakeReader:
        def __init__(self, _path) -> None:
            self.pages = [FakePage()]

    class Renderer:
        descriptor = ComponentDescriptor("vision-renderer", "1", deterministic=True)

        async def render(self, _pdf: bytes, *, page_number: int, dpi: int) -> bytes:
            assert page_number == 1
            return b"bounded-rendered-page"

        async def close(self) -> None:
            return None

    class Vision:
        descriptor = ComponentDescriptor("fake-vision", "1", deterministic=True, external=True)

        def __init__(self) -> None:
            self.images: list[bytes] = []

        async def extract(self, image: bytes, *, schema):
            self.images.append(image)
            return {
                "table_artifacts": [
                    {
                        "schema_version": "1",
                        "table_id": "vision-table",
                        "page_number": 1,
                        "row_count": 1,
                        "column_count": 1,
                        "cells": [{"row": 0, "column": 0, "text": "value"}],
                    }
                ]
            }

        async def close(self) -> None:
            return None

    class Allow:
        async def authorize(self, *, purpose: str, page_number: int | None) -> bool:
            return purpose == "table" and page_number == 1

    monkeypatch.setattr(documents, "PdfReader", FakeReader)
    path = tmp_path / "vision.pdf"
    path.write_bytes(b"snapshot-pdf")
    vision = Vision()
    source = LocalDocumentsSource(
        path,
        table_mode="vision",
        page_renderer=Renderer(),
        vision_extractor=vision,
        external_processing_policy=Allow(),
        extraction_cache=SQLiteExtractionCache(tmp_path / "vision.db"),
    )

    await source._parse_file_with_document_ai(path)

    assert vision.images == [b"bounded-rendered-page"]


@pytest.mark.asyncio
async def test_invalid_vision_response_retries_once_before_accepting_table(tmp_path):
    class RetryVision:
        descriptor = ComponentDescriptor("retry-vision", "1", deterministic=False, external=True)

        def __init__(self) -> None:
            self.calls = 0

        async def extract(self, _image: bytes, *, schema):
            self.calls += 1
            if self.calls == 1:
                return {"invalid": True}
            return {
                "table_artifacts": [
                    {
                        "schema_version": "1",
                        "table_id": "recovered",
                        "page_number": 1,
                        "row_count": 2,
                        "column_count": 1,
                        "cells": [
                            {"row": 0, "column": 0, "text": "name", "header_level": 0},
                            {"row": 1, "column": 0, "text": "value"},
                        ],
                    }
                ]
            }

        async def close(self) -> None:
            return None

    class Allow:
        async def authorize(self, *, purpose: str, page_number: int | None) -> bool:
            return True

    path = tmp_path / "vision.png"
    path.write_bytes(VALID_PNG)
    vision = RetryVision()
    source = LocalDocumentsSource(
        path,
        extensions=(".png",),
        ocr_mode="always",
        ocr_engine=FakeOcr(),
        table_mode="vision",
        vision_extractor=vision,
        external_processing_policy=Allow(),
        extraction_cache=SQLiteExtractionCache(tmp_path / "vision-retry.db"),
    )

    elements = await source._parse_file_with_document_ai(path)

    assert vision.calls == 2
    assert any(isinstance(element.payload, TableBatch) for element in elements)


class FailingOcr(FakeOcr):
    async def recognize(self, image: bytes, *, language: str = "eng") -> OcrResult:
        raise RuntimeError("transient OCR failure")


@pytest.mark.asyncio
async def test_best_effort_failure_preserves_prior_records_and_checkpoint(tmp_path):
    path = tmp_path / "scan.png"
    path.write_bytes(VALID_PNG)
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
    failed.write_bytes(VALID_PNG)
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
            "sha256": hashlib.sha256(VALID_PNG).hexdigest(),
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


@pytest.mark.asyncio
async def test_failed_current_file_does_not_block_unrelated_deletes(tmp_path):
    failed = tmp_path / "a.png"
    failed.write_bytes(VALID_PNG)
    source = LocalDocumentsSource(
        tmp_path,
        extensions=(".png",),
        ocr_mode="always",
        ocr_engine=FailingOcr(),
        failure_mode="best_effort",
        checkpoint_interval=1,
        stream_names=("documents",),
    )
    stream = (await source.discover())[0]
    parser = source._parser_fingerprint()
    state = {
        "parser_fingerprint": parser,
        "files": {
            "b.png": {
                "sha256": "b" * 64,
                "element_count": 2,
                "parser_fingerprint": parser,
            }
        },
        "in_progress": {
            "relative_path": "a.png",
            "sha256": hashlib.sha256(VALID_PNG).hexdigest(),
            "next_index": 2,
            "parser_fingerprint": parser,
        },
    }

    first = [item async for item in source.read(stream, state)]
    deletes = [
        item
        for item in first
        if isinstance(item, RecordMessage) and item.envelope.operation is Operation.DELETE
    ]
    final = [item for item in first if isinstance(item, StateMessage)][-1].state

    assert len(deletes) == 2
    assert "b.png" not in final["files"]
    assert final["in_progress"]["relative_path"] == "a.png"
    repeated = [item async for item in source.read(stream, final)]
    assert not any(
        isinstance(item, RecordMessage) and item.envelope.operation is Operation.DELETE
        for item in repeated
    )


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
    path.write_bytes(VALID_PNG)
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
        self.closes = 0

    async def split(self, element: DocumentElement):
        self.calls += 1
        return (SplitChunk(f"{self.prefix}:{element.text}"),)

    async def close(self):
        self.closes += 1


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


@pytest.mark.asyncio
async def test_source_closes_injected_components_once(tmp_path):
    path = tmp_path / "notes.txt"
    path.write_text("hello", encoding="utf-8")
    splitter = FakeSplitter()
    source = LocalDocumentsSource(path, document_splitter=splitter)

    await source.close()
    await source.close()

    assert splitter.closes == 1


@pytest.mark.asyncio
async def test_source_attempts_every_close_after_a_component_failure(tmp_path):
    class FailingClose(FakeOcr):
        def __init__(self) -> None:
            self.closes = 0

        async def close(self) -> None:
            self.closes += 1
            raise RuntimeError("close failed")

    path = tmp_path / "notes.txt"
    path.write_text("hello", encoding="utf-8")
    failing = FailingClose()
    tracking = FakeSplitter()
    source = LocalDocumentsSource(
        path,
        ocr_engine=failing,
        document_splitter=tracking,
    )

    with pytest.raises(RuntimeError, match="close failed"):
        await source.close()
    await source.close()

    assert failing.closes == 1
    assert tracking.closes == 1


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


@pytest.mark.asyncio
async def test_changed_file_does_not_reuse_nondeterministic_in_progress_manifest(tmp_path):
    path = tmp_path / "notes.txt"
    path.write_text("first content", encoding="utf-8")
    cache_path = tmp_path / "changed-manifest.db"
    first_source = LocalDocumentsSource(
        path,
        document_splitter=NondeterministicSplitter("first"),
        extraction_cache=SQLiteExtractionCache(cache_path),
        checkpoint_interval=1,
        stream_names=("notes",),
    )
    stream = (await first_source.discover())[0]
    initial = [item async for item in first_source.read(stream)]
    checkpoint = next(
        item.state
        for item in initial
        if isinstance(item, StateMessage)
        and item.state.get("in_progress", {}).get("next_index") == 1
    )

    path.write_text("changed content", encoding="utf-8")
    changed_splitter = NondeterministicSplitter("changed")
    changed_source = LocalDocumentsSource(
        path,
        document_splitter=changed_splitter,
        extraction_cache=SQLiteExtractionCache(cache_path),
        checkpoint_interval=1,
        stream_names=("notes",),
    )
    resumed = [item async for item in changed_source.read(stream, checkpoint)]
    resumed_records = [item for item in resumed if isinstance(item, RecordMessage)]

    assert changed_splitter.calls == 1
    assert [item.envelope.payload.text for item in resumed_records] == [
        "changed-a",
        "changed-b",
    ]
    final = [item for item in resumed if isinstance(item, StateMessage)][-1].state
    assert final["files"]["notes.txt"]["sha256"] == hashlib.sha256(b"changed content").hexdigest()


@pytest.mark.asyncio
async def test_nondeterministic_restore_fails_when_manifest_is_missing(tmp_path):
    path = tmp_path / "notes.txt"
    path.write_text("hello", encoding="utf-8")
    cache = SQLiteExtractionCache(tmp_path / "restore.db")
    source = LocalDocumentsSource(
        path,
        document_splitter=NondeterministicSplitter("stable"),
        extraction_cache=cache,
        checkpoint_interval=1,
        stream_names=("notes",),
    )
    stream = (await source.discover())[0]
    initial = [item async for item in source.read(stream)]
    final = [item for item in initial if isinstance(item, StateMessage)][-1].state
    manifest_key = final["files"]["notes.txt"]["manifest_cache_key"]

    path.unlink()
    deletion = source.read(stream, final)
    checkpoint = None
    async for message in deletion:
        if isinstance(message, StateMessage) and message.state.get("in_progress", {}).get(
            "tombstone_next_index"
        ):
            checkpoint = message.state
            break
    await deletion.aclose()
    assert checkpoint is not None

    await cache.delete(manifest_key)
    path.write_text("hello", encoding="utf-8")
    with pytest.raises(ConfigurationError, match="manifest is unavailable"):
        [item async for item in source.read(stream, checkpoint)]


@pytest.mark.asyncio
async def test_reappearing_file_after_config_changed_builds_a_new_manifest(tmp_path):
    class ConfiguredSplitter(NondeterministicSplitter):
        def __init__(self, prefix: str) -> None:
            super().__init__(prefix)
            self.descriptor = ComponentDescriptor(
                "configured-splitter",
                "1",
                {"prefix": prefix},
                deterministic=False,
            )

    path = tmp_path / "configured.txt"
    path.write_text("hello", encoding="utf-8")
    cache_path = tmp_path / "configured.db"
    old_source = LocalDocumentsSource(
        path,
        document_splitter=ConfiguredSplitter("old"),
        extraction_cache=SQLiteExtractionCache(cache_path),
        checkpoint_interval=1,
        stream_names=("configured",),
    )
    stream = (await old_source.discover())[0]
    initial = [item async for item in old_source.read(stream)]
    committed = [item for item in initial if isinstance(item, StateMessage)][-1].state

    path.unlink()
    new_splitter = ConfiguredSplitter("new")
    new_source = LocalDocumentsSource(
        path,
        document_splitter=new_splitter,
        extraction_cache=SQLiteExtractionCache(cache_path),
        checkpoint_interval=1,
        stream_names=("configured",),
    )
    deletion = new_source.read(stream, committed)
    checkpoint = None
    async for message in deletion:
        if isinstance(message, StateMessage) and message.state.get("in_progress", {}).get(
            "tombstone_next_index"
        ):
            checkpoint = message.state
            break
    await deletion.aclose()
    assert checkpoint is not None

    path.write_text("hello", encoding="utf-8")
    restored = [item async for item in new_source.read(stream, checkpoint)]
    restored_records = [item for item in restored if isinstance(item, RecordMessage)]

    assert new_splitter.calls == 1
    assert [item.envelope.payload.text for item in restored_records[:2]] == ["new-a", "new-b"]


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
    path.write_bytes(VALID_PNG)
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
    path.write_bytes(VALID_PNG)
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
