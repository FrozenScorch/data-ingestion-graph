"""Optional, local PDF page rendering."""

from __future__ import annotations

import asyncio
import importlib
from collections.abc import Callable
from io import BytesIO
from types import ModuleType

from ingestion_graph.document_ai.models import ComponentDescriptor
from ingestion_graph.errors import ConfigurationError

ModuleLoader = Callable[[str], ModuleType]


class PdfiumPageRenderer:
    """Render one-based PDF pages to PNG without importing optional packages eagerly."""

    descriptor = ComponentDescriptor(
        "pypdfium2",
        "1",
        configuration={"format": "png"},
        deterministic=True,
        external=False,
    )

    def __init__(self, *, module_loader: ModuleLoader = importlib.import_module) -> None:
        self._module_loader = module_loader

    async def render(self, source: bytes, *, page_number: int, dpi: int = 300) -> bytes:
        if not source:
            raise ValueError("PDF source must not be empty")
        if page_number < 1:
            raise ValueError("page_number must be one-based and positive")
        if dpi < 36 or dpi > 1200:
            raise ValueError("dpi must be between 36 and 1200")
        return await asyncio.to_thread(self._render_sync, source, page_number, dpi)

    def _render_sync(self, source: bytes, page_number: int, dpi: int) -> bytes:
        try:
            pdfium = self._module_loader("pypdfium2")
            # ``bitmap.to_pil`` imports Pillow internally. Loading it here gives callers a
            # stable SDK error instead of an implementation-specific error from pdfium.
            self._module_loader("PIL.Image")
        except (ImportError, ModuleNotFoundError) as exc:
            raise ConfigurationError(
                "PDF page rendering requires: pip install 'ingestion-graph[ocr]'"
            ) from exc

        document = pdfium.PdfDocument(source)
        try:
            page_count = len(document)
            if page_number > page_count:
                raise ValueError(
                    f"page_number {page_number} exceeds the PDF page count ({page_count})"
                )
            page = document[page_number - 1]
            try:
                bitmap = page.render(scale=dpi / 72.0)
                try:
                    image = bitmap.to_pil()
                    output = BytesIO()
                    image.save(output, format="PNG")
                    return output.getvalue()
                finally:
                    _close_if_supported(bitmap)
            finally:
                _close_if_supported(page)
        finally:
            _close_if_supported(document)

    async def close(self) -> None:
        return None


def _close_if_supported(value: object) -> None:
    close = getattr(value, "close", None)
    if callable(close):
        close()


Pypdfium2PageRenderer = PdfiumPageRenderer

__all__ = ["PdfiumPageRenderer", "Pypdfium2PageRenderer"]
