from __future__ import annotations

import asyncio
import threading
import time
from collections.abc import Sequence
from types import ModuleType

import pytest

from ingestion_graph.document_ai.docling_adapter import DoclingTableExtractor
from ingestion_graph.document_ai.rendering import PdfiumPageRenderer
from ingestion_graph.document_ai.tesseract import (
    ProcessOutputLimitError,
    ProcessResult,
    TesseractOcrEngine,
)
from ingestion_graph.errors import ConfigurationError


@pytest.mark.asyncio
async def test_pdfium_renderer_loads_optional_dependencies_lazily() -> None:
    loaded: list[str] = []
    closed: list[str] = []

    class FakeImage:
        size = (20, 10)

        def save(self, output: object, *, format: str) -> None:
            assert format == "PNG"
            output.write(b"fake-png")  # type: ignore[attr-defined]

    class FakeBitmap:
        def to_pil(self) -> FakeImage:
            return FakeImage()

        def close(self) -> None:
            closed.append("bitmap")

    class FakePage:
        def render(self, *, scale: float) -> FakeBitmap:
            assert scale == 2.0
            return FakeBitmap()

        def close(self) -> None:
            closed.append("page")

    class FakeDocument:
        def __init__(self, source: bytes) -> None:
            assert source == b"%PDF-fake"

        def __len__(self) -> int:
            return 1

        def __getitem__(self, index: int) -> FakePage:
            assert index == 0
            return FakePage()

        def close(self) -> None:
            closed.append("document")

    pdfium = ModuleType("pypdfium2")
    pdfium.PdfDocument = FakeDocument  # type: ignore[attr-defined]
    pillow = ModuleType("PIL.Image")

    def load(name: str) -> ModuleType:
        loaded.append(name)
        return pdfium if name == "pypdfium2" else pillow

    renderer = PdfiumPageRenderer(module_loader=load)
    assert loaded == []
    assert await renderer.render(b"%PDF-fake", page_number=1, dpi=144) == b"fake-png"
    assert loaded == ["pypdfium2", "PIL.Image"]
    assert closed == ["bitmap", "page", "document"]


@pytest.mark.asyncio
async def test_pdfium_renderer_rejects_oversized_page_before_render() -> None:
    rendered = False

    class FakePage:
        def get_size(self):
            return (1000.0, 1000.0)

        def render(self, *, scale: float):
            nonlocal rendered
            rendered = True
            raise AssertionError("oversized page must not render")

        def close(self):
            return None

    class FakeDocument:
        def __init__(self, source: bytes):
            return None

        def __len__(self):
            return 1

        def __getitem__(self, index: int):
            return FakePage()

        def close(self):
            return None

    pdfium = ModuleType("pypdfium2")
    pdfium.PdfDocument = FakeDocument  # type: ignore[attr-defined]
    renderer = PdfiumPageRenderer(
        module_loader=lambda name: pdfium if name == "pypdfium2" else ModuleType(name),
        max_pixels=100,
    )

    with pytest.raises(ConfigurationError, match="pixel limit"):
        await renderer.render(b"%PDF-fake", page_number=1, dpi=72)
    assert rendered is False


@pytest.mark.asyncio
async def test_pdfium_renderer_bounds_encoded_output() -> None:
    class FakeImage:
        size = (1, 1)

        def save(self, output: object, *, format: str) -> None:
            output.write(b"too-large-output")  # type: ignore[attr-defined]

    class FakeBitmap:
        def to_pil(self):
            return FakeImage()

        def close(self):
            return None

    class FakePage:
        def get_size(self):
            return (1.0, 1.0)

        def render(self, *, scale: float):
            return FakeBitmap()

        def close(self):
            return None

    class FakeDocument:
        def __init__(self, source: bytes):
            return None

        def __len__(self):
            return 1

        def __getitem__(self, index: int):
            return FakePage()

        def close(self):
            return None

    pdfium = ModuleType("pypdfium2")
    pdfium.PdfDocument = FakeDocument  # type: ignore[attr-defined]
    renderer = PdfiumPageRenderer(
        module_loader=lambda name: pdfium if name == "pypdfium2" else ModuleType(name),
        max_output_bytes=4,
    )

    with pytest.raises(ConfigurationError, match="output-size limit"):
        await renderer.render(b"%PDF-fake", page_number=1, dpi=72)


@pytest.mark.asyncio
async def test_pdfium_renderer_cancellation_terminates_worker() -> None:
    started = asyncio.Event()

    class FakeProcess:
        returncode = None
        terminated = False

        async def communicate(self, source: bytes):
            started.set()
            await asyncio.Event().wait()

        def terminate(self):
            self.terminated = True
            self.returncode = -15

        def kill(self):
            self.returncode = -9

        async def wait(self):
            return self.returncode

    process = FakeProcess()

    async def factory(*args, **kwargs):
        return process

    renderer = PdfiumPageRenderer(use_subprocess=True, process_factory=factory)
    task = asyncio.create_task(renderer.render(b"%PDF-fake", page_number=1))
    await started.wait()
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task
    assert process.terminated is True


@pytest.mark.asyncio
async def test_pdfium_renderer_reports_missing_optional_dependencies() -> None:
    def missing(_: str) -> ModuleType:
        raise ModuleNotFoundError("not installed")

    renderer = PdfiumPageRenderer(module_loader=missing)
    with pytest.raises(ConfigurationError, match=r"ingestion-graph\[ocr\]"):
        await renderer.render(b"%PDF-fake", page_number=1)


@pytest.mark.asyncio
async def test_tesseract_engine_uses_injected_runner_and_normalizes_tsv() -> None:
    calls: list[tuple[tuple[str, ...], bytes | None, float, int]] = []
    tsv = (
        b"level\tpage_num\tblock_num\tpar_num\tline_num\tword_num\tleft\ttop\twidth\t"
        b"height\tconf\ttext\n"
        b"1\t1\t0\t0\t0\t0\t0\t0\t100\t50\t-1\t\n"
        b"5\t1\t1\t1\t1\t1\t10\t5\t30\t10\t96\tHello\n"
        b"5\t1\t1\t1\t1\t2\t45\t5\t40\t10\t84\tworld\n"
    )

    async def runner(
        arguments: Sequence[str],
        input_data: bytes | None,
        timeout_seconds: float,
        max_output_bytes: int,
    ) -> ProcessResult:
        calls.append((tuple(arguments), input_data, timeout_seconds, max_output_bytes))
        return ProcessResult(0, tsv)

    engine = TesseractOcrEngine(process_runner=runner, timeout_seconds=4, max_output_bytes=2048)
    result = await engine.recognize(b"image", language="eng")

    assert calls == [
        (
            ("tesseract", "stdin", "stdout", "-l", "eng", "--psm", "3", "tsv"),
            b"image",
            4,
            2048,
        )
    ]
    assert result.text == "Hello world"
    assert [token.text for token in result.tokens] == ["Hello", "world"]
    assert result.confidence == pytest.approx(0.9)
    assert result.tokens[0].coordinates is not None
    assert result.tokens[0].coordinates.left == pytest.approx(0.1)


@pytest.mark.asyncio
async def test_tesseract_engine_fails_closed_on_bounded_runner_error() -> None:
    async def runner(
        arguments: Sequence[str],
        input_data: bytes | None,
        timeout_seconds: float,
        max_output_bytes: int,
    ) -> ProcessResult:
        raise ProcessOutputLimitError("too much output")

    engine = TesseractOcrEngine(process_runner=runner)
    with pytest.raises(ConfigurationError, match="failed safely"):
        await engine.recognize(b"image")


@pytest.mark.asyncio
async def test_docling_adapter_lazily_normalizes_fake_table_output() -> None:
    created = 0

    class FakeConverter:
        def convert_bytes(self, image: bytes) -> object:
            assert image == b"image"
            return {
                "document": {
                    "tables": [
                        {
                            "caption": "Quarterly results",
                            "coordinates": {
                                "left": 0.1,
                                "top": 0.2,
                                "right": 0.9,
                                "bottom": 0.8,
                            },
                            "rows": [["Name", "Value"], ["alpha", 2]],
                        }
                    ]
                }
            }

    def factory() -> object:
        nonlocal created
        created += 1
        return FakeConverter()

    extractor = DoclingTableExtractor(converter_factory=factory)
    assert created == 0
    tables = await extractor.extract(b"image", page_number=2)

    assert created == 1
    assert len(tables) == 1
    table = tables[0]
    assert table.page_number == 2
    assert table.caption == "Quarterly results"
    assert (table.row_count, table.column_count) == (2, 2)
    assert [cell.text for cell in table.cells] == ["Name", "Value", "alpha", "2"]
    assert table.coordinates is not None
    assert table.engine is DoclingTableExtractor.descriptor


@pytest.mark.asyncio
async def test_docling_adapter_never_auto_configures_or_downloads_models() -> None:
    extractor = DoclingTableExtractor()
    with pytest.raises(ConfigurationError, match="automatic model downloads are disabled"):
        await extractor.extract(b"image")


@pytest.mark.asyncio
async def test_docling_adapter_serializes_shared_converter_calls() -> None:
    active = 0
    max_active = 0
    guard = threading.Lock()

    class FakeConverter:
        def convert_bytes(self, _image: bytes) -> object:
            nonlocal active, max_active
            with guard:
                active += 1
                max_active = max(max_active, active)
            try:
                time.sleep(0.02)
                return {"document": {"tables": []}}
            finally:
                with guard:
                    active -= 1

    extractor = DoclingTableExtractor(converter_factory=FakeConverter)
    await asyncio.gather(extractor.extract(b"one"), extractor.extract(b"two"))

    assert max_active == 1
