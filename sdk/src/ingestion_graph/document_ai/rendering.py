"""Optional, local PDF page rendering."""

from __future__ import annotations

import asyncio
import importlib
import math
import sys
import warnings
from collections.abc import Callable
from io import BytesIO
from types import ModuleType
from typing import Any

from ingestion_graph.document_ai.models import ComponentDescriptor
from ingestion_graph.errors import ConfigurationError

ModuleLoader = Callable[[str], ModuleType]
ProcessFactory = Callable[..., Any]
MAX_IMAGE_PIXELS = 50_000_000
MAX_IMAGE_DIMENSION = 32_768
MAX_IMAGE_FRAMES = 256
_IMAGE_FORMATS = {
    ".jpeg": "JPEG",
    ".jpg": "JPEG",
    ".png": "PNG",
    ".tif": "TIFF",
    ".tiff": "TIFF",
    ".webp": "WEBP",
}


def validate_image_payload(
    source: bytes,
    *,
    extension: str,
    max_pixels: int = MAX_IMAGE_PIXELS,
    max_dimension: int = MAX_IMAGE_DIMENSION,
    max_frames: int = MAX_IMAGE_FRAMES,
) -> None:
    """Validate encoded image geometry before an OCR process decodes it."""
    if not source:
        raise ConfigurationError("Image input must not be empty")
    expected_format = _IMAGE_FORMATS.get(extension.lower())
    if expected_format is None:
        raise ConfigurationError("Image input has an unsupported extension")
    if max_pixels < 1 or max_dimension < 1 or max_frames < 1:
        raise ValueError("Image safety limits must be positive")
    try:
        image_module = importlib.import_module("PIL.Image")
    except (ImportError, ModuleNotFoundError) as exc:
        raise ConfigurationError(
            "Image validation requires: pip install 'ingestion-graph[ocr]'"
        ) from exc
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("error", image_module.DecompressionBombWarning)
            with image_module.open(BytesIO(source), formats=(expected_format,)) as image:
                frame_count = int(getattr(image, "n_frames", 1))
                if frame_count < 1 or frame_count > max_frames:
                    raise ConfigurationError("Image input exceeds the frame-count limit")
                for frame_index in range(frame_count):
                    if frame_index:
                        image.seek(frame_index)
                    width, height = image.size
                    if width < 1 or height < 1:
                        raise ConfigurationError("Image input dimensions are invalid")
                    if width > max_dimension or height > max_dimension:
                        raise ConfigurationError("Image input exceeds the dimension limit")
                    if width * height > max_pixels:
                        raise ConfigurationError("Image input exceeds the pixel limit")
    except ConfigurationError:
        raise
    except Exception as exc:
        raise ConfigurationError("Image input is invalid or exceeds safety limits") from exc


class PdfiumPageRenderer:
    """Render one-based PDF pages to PNG without importing optional packages eagerly."""

    def __init__(
        self,
        *,
        module_loader: ModuleLoader = importlib.import_module,
        max_pixels: int = 50_000_000,
        max_output_bytes: int = 64 * 1024 * 1024,
        max_source_bytes: int = 256 * 1024 * 1024,
        use_subprocess: bool | None = None,
        process_factory: ProcessFactory | None = None,
    ) -> None:
        if max_pixels < 1 or max_output_bytes < 1 or max_source_bytes < 1:
            raise ValueError("render limits must be positive")
        self._module_loader = module_loader
        self.max_pixels = max_pixels
        self.max_output_bytes = max_output_bytes
        self.max_source_bytes = max_source_bytes
        self._use_subprocess = (
            module_loader is importlib.import_module if use_subprocess is None else use_subprocess
        )
        self._process_factory = process_factory or asyncio.create_subprocess_exec
        self.descriptor = ComponentDescriptor(
            "pypdfium2",
            "3",
            configuration={
                "format": "png",
                "max_pixels": max_pixels,
                "max_output_bytes": max_output_bytes,
                "max_source_bytes": max_source_bytes,
                "execution": "subprocess" if self._use_subprocess else "in_process",
            },
            deterministic=True,
            external=False,
        )

    async def render(self, source: bytes, *, page_number: int, dpi: int = 300) -> bytes:
        if not source:
            raise ValueError("PDF source must not be empty")
        if len(source) > self.max_source_bytes:
            raise ConfigurationError("PDF source exceeds the renderer input-size limit")
        if page_number < 1:
            raise ValueError("page_number must be one-based and positive")
        if dpi < 36 or dpi > 1200:
            raise ValueError("dpi must be between 36 and 1200")
        if self._use_subprocess:
            return await self._render_subprocess(source, page_number, dpi)
        return await asyncio.to_thread(self._render_sync, source, page_number, dpi)

    async def _render_subprocess(self, source: bytes, page_number: int, dpi: int) -> bytes:
        process = await self._process_factory(
            sys.executable,
            "-m",
            "ingestion_graph.document_ai.rendering_worker",
            str(page_number),
            str(dpi),
            str(self.max_pixels),
            str(self.max_output_bytes),
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            stdout, _stderr = await process.communicate(source)
        except asyncio.CancelledError:
            await _terminate_process(process)
            raise
        if process.returncode != 0:
            raise ConfigurationError("PDF renderer worker failed")
        if len(stdout) > self.max_output_bytes:
            raise ConfigurationError("Rendered PDF page exceeds the output-size limit")
        return bytes(stdout)

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
                get_size = getattr(page, "get_size", None)
                if callable(get_size):
                    width_points, height_points = get_size()
                    if (
                        not math.isfinite(width_points)
                        or not math.isfinite(height_points)
                        or width_points <= 0
                        or height_points <= 0
                    ):
                        raise ConfigurationError("PDF page dimensions are invalid")
                    estimated_pixels = math.ceil(width_points * dpi / 72) * math.ceil(
                        height_points * dpi / 72
                    )
                    if estimated_pixels > self.max_pixels:
                        raise ConfigurationError("Rendered PDF page exceeds the pixel limit")
                bitmap = page.render(scale=dpi / 72.0)
                try:
                    image = bitmap.to_pil()
                    size = getattr(image, "size", None)
                    if size is not None and size[0] * size[1] > self.max_pixels:
                        raise ConfigurationError("Rendered PDF page exceeds the pixel limit")
                    output = _BoundedBytesIO(self.max_output_bytes)
                    image.save(output, format="PNG")
                    payload = output.getvalue()
                    return payload
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


class _BoundedBytesIO(BytesIO):
    def __init__(self, limit: int) -> None:
        super().__init__()
        self._limit = limit

    def write(self, value: Any) -> int:
        if max(len(self.getbuffer()), self.tell() + len(value)) > self._limit:
            raise ConfigurationError("Rendered PDF page exceeds the output-size limit")
        return super().write(value)


async def _terminate_process(process: Any) -> None:
    if process.returncode is not None:
        return
    try:
        process.terminate()
    except ProcessLookupError:
        return
    try:
        await asyncio.wait_for(process.wait(), timeout=1.0)
    except TimeoutError:
        try:
            process.kill()
        except ProcessLookupError:
            return
        await process.wait()


__all__ = ["PdfiumPageRenderer", "Pypdfium2PageRenderer", "validate_image_payload"]
