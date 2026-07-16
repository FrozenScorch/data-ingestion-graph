from __future__ import annotations

import pytest

from ingestion_graph.document_ai import ComponentDescriptor, OcrResult
from ingestion_graph.models import DocumentElement
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
