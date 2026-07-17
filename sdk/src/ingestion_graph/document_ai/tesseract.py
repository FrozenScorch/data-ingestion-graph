"""Bounded local Tesseract CLI OCR adapter."""

from __future__ import annotations

import asyncio
import csv
import io
import math
import re
from collections.abc import Awaitable, Callable, Sequence
from contextlib import suppress
from dataclasses import dataclass

from ingestion_graph.connectors.base import CheckResult
from ingestion_graph.document_ai.models import BoundingBox, ComponentDescriptor, OcrResult, OcrToken
from ingestion_graph.errors import ConfigurationError

_LANGUAGE = re.compile(r"[A-Za-z0-9][A-Za-z0-9_+.-]{0,63}\Z")


@dataclass(frozen=True, slots=True)
class ProcessResult:
    returncode: int
    stdout: bytes = b""
    stderr: bytes = b""


ProcessRunner = Callable[
    [Sequence[str], bytes | None, float, int],
    Awaitable[ProcessResult],
]


class ProcessTimeoutError(RuntimeError):
    """A child process exceeded its configured deadline."""


class ProcessOutputLimitError(RuntimeError):
    """A child process exceeded its bounded output allowance."""


async def run_bounded_process(
    arguments: Sequence[str],
    input_data: bytes | None,
    timeout_seconds: float,
    max_output_bytes: int,
) -> ProcessResult:
    """Run an argv directly, killing it on timeout, cancellation, or excessive output."""

    if not arguments:
        raise ValueError("process arguments must not be empty")
    if timeout_seconds <= 0 or not math.isfinite(timeout_seconds):
        raise ValueError("timeout_seconds must be positive and finite")
    if max_output_bytes < 1:
        raise ValueError("max_output_bytes must be positive")

    process = await asyncio.create_subprocess_exec(
        *arguments,
        stdin=asyncio.subprocess.PIPE if input_data is not None else asyncio.subprocess.DEVNULL,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    if process.stdout is None or process.stderr is None:
        process.kill()
        await process.wait()
        raise RuntimeError("subprocess pipes were not created")

    write_task = asyncio.create_task(_write_input(process, input_data))
    stdout_task = asyncio.create_task(_read_bounded(process.stdout, max_output_bytes))
    stderr_task = asyncio.create_task(_read_bounded(process.stderr, max_output_bytes))
    wait_task = asyncio.create_task(process.wait())
    tasks = (write_task, stdout_task, stderr_task, wait_task)
    try:
        try:
            _, stdout, stderr, returncode = await asyncio.wait_for(
                asyncio.gather(*tasks), timeout=timeout_seconds
            )
        except TimeoutError as exc:
            raise ProcessTimeoutError(
                f"process exceeded its {timeout_seconds:g} second timeout"
            ) from exc
        return ProcessResult(returncode, stdout, stderr)
    finally:
        if process.returncode is None:
            with suppress(ProcessLookupError):
                process.kill()
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        if process.returncode is None:
            await process.wait()


async def _write_input(process: asyncio.subprocess.Process, data: bytes | None) -> None:
    if data is None or process.stdin is None:
        return
    try:
        for offset in range(0, len(data), 64 * 1024):
            process.stdin.write(data[offset : offset + 64 * 1024])
            await process.stdin.drain()
    except (BrokenPipeError, ConnectionResetError):
        pass
    finally:
        process.stdin.close()
        with suppress(BrokenPipeError, ConnectionResetError):
            await process.stdin.wait_closed()


async def _read_bounded(reader: asyncio.StreamReader, limit: int) -> bytes:
    output = bytearray()
    while True:
        chunk = await reader.read(min(64 * 1024, limit - len(output) + 1))
        if not chunk:
            return bytes(output)
        output.extend(chunk)
        if len(output) > limit:
            raise ProcessOutputLimitError(f"process output exceeded {limit} bytes")


class TesseractOcrEngine:
    """CPU-only OCR through an installed Tesseract executable."""

    def __init__(
        self,
        *,
        executable: str = "tesseract",
        timeout_seconds: float = 30.0,
        max_output_bytes: int = 8 * 1024 * 1024,
        max_image_bytes: int = 64 * 1024 * 1024,
        page_segmentation_mode: int = 3,
        process_runner: ProcessRunner = run_bounded_process,
    ) -> None:
        if not executable or "\x00" in executable:
            raise ValueError("executable must be a non-empty path")
        if timeout_seconds <= 0 or not math.isfinite(timeout_seconds):
            raise ValueError("timeout_seconds must be positive and finite")
        if max_output_bytes < 1 or max_image_bytes < 1:
            raise ValueError("process and image byte limits must be positive")
        if page_segmentation_mode < 0 or page_segmentation_mode > 13:
            raise ValueError("page_segmentation_mode must be between 0 and 13")
        self._executable = executable
        self._timeout_seconds = timeout_seconds
        self._max_output_bytes = max_output_bytes
        self._max_image_bytes = max_image_bytes
        self._page_segmentation_mode = page_segmentation_mode
        self._process_runner = process_runner
        self.descriptor = ComponentDescriptor(
            "tesseract-cli",
            "1",
            configuration={
                "page_segmentation_mode": page_segmentation_mode,
                "timeout_seconds": timeout_seconds,
                "max_output_bytes": max_output_bytes,
            },
            deterministic=True,
            external=False,
        )

    async def check(self) -> CheckResult:
        try:
            result = await self._process_runner(
                (self._executable, "--version"),
                None,
                min(self._timeout_seconds, 5.0),
                min(self._max_output_bytes, 64 * 1024),
            )
        except (OSError, ProcessTimeoutError, ProcessOutputLimitError) as exc:
            return CheckResult(False, f"Tesseract is unavailable: {exc}")
        if result.returncode != 0:
            return CheckResult(False, _failure_message(result))
        version = result.stdout.decode("utf-8", errors="replace").splitlines()
        return CheckResult(True, version[0] if version else "Tesseract is available")

    async def recognize(self, image: bytes, *, language: str = "eng") -> OcrResult:
        if not image:
            raise ValueError("OCR image must not be empty")
        if len(image) > self._max_image_bytes:
            raise ValueError(f"OCR image exceeds {self._max_image_bytes} bytes")
        if _LANGUAGE.fullmatch(language) is None:
            raise ValueError("language must be a safe Tesseract language identifier")

        arguments = (
            self._executable,
            "stdin",
            "stdout",
            "-l",
            language,
            "--psm",
            str(self._page_segmentation_mode),
            "tsv",
        )
        try:
            result = await self._process_runner(
                arguments,
                image,
                self._timeout_seconds,
                self._max_output_bytes,
            )
        except FileNotFoundError as exc:
            raise ConfigurationError(
                "Tesseract OCR requires a local 'tesseract' executable"
            ) from exc
        except (OSError, ProcessTimeoutError, ProcessOutputLimitError) as exc:
            raise ConfigurationError(f"Tesseract OCR failed safely: {exc}") from exc
        if result.returncode != 0:
            raise ConfigurationError(_failure_message(result))
        return _parse_tsv(result.stdout, language=language)

    async def close(self) -> None:
        return None


def _failure_message(result: ProcessResult) -> str:
    detail = result.stderr.decode("utf-8", errors="replace").strip()
    if not detail:
        detail = f"exit status {result.returncode}"
    return f"Tesseract OCR failed: {detail}"


def _parse_tsv(payload: bytes, *, language: str) -> OcrResult:
    text = payload.decode("utf-8-sig", errors="replace")
    reader = csv.DictReader(io.StringIO(text), delimiter="\t")
    required = {"level", "left", "top", "width", "height", "conf", "text"}
    if reader.fieldnames is None or not required.issubset(reader.fieldnames):
        raise ConfigurationError("Tesseract returned malformed TSV output")
    rows = list(reader)
    page_width = max(
        (_integer(row.get("left")) + _integer(row.get("width")) for row in rows),
        default=0,
    )
    page_height = max(
        (_integer(row.get("top")) + _integer(row.get("height")) for row in rows),
        default=0,
    )

    tokens: list[OcrToken] = []
    lines: list[list[str]] = []
    line_keys: list[tuple[str, str, str, str]] = []
    for row in rows:
        token_text = (row.get("text") or "").strip()
        if row.get("level") != "5" or not token_text:
            continue
        confidence = _confidence(row.get("conf"))
        coordinates = _coordinates(row, page_width, page_height)
        tokens.append(OcrToken(token_text, confidence, coordinates))
        line_key = (
            row.get("page_num", ""),
            row.get("block_num", ""),
            row.get("par_num", ""),
            row.get("line_num", ""),
        )
        if not line_keys or line_keys[-1] != line_key:
            line_keys.append(line_key)
            lines.append([])
        lines[-1].append(token_text)

    confidences = [token.confidence for token in tokens if token.confidence is not None]
    overall = sum(confidences) / len(confidences) if confidences else None
    return OcrResult(
        "\n".join(" ".join(line) for line in lines),
        tuple(tokens),
        overall,
        usage={"engine": "tesseract-cli", "language": language, "token_count": len(tokens)},
    )


def _integer(value: str | None) -> int:
    try:
        return max(0, int(value or "0"))
    except ValueError:
        return 0


def _confidence(value: str | None) -> float | None:
    try:
        parsed = float(value or "-1")
    except ValueError:
        return None
    if parsed < 0 or not math.isfinite(parsed):
        return None
    return min(1.0, parsed / 100.0)


def _coordinates(row: dict[str, str], page_width: int, page_height: int) -> BoundingBox | None:
    if page_width < 1 or page_height < 1:
        return None
    left = _integer(row.get("left"))
    top = _integer(row.get("top"))
    right = min(page_width, left + _integer(row.get("width")))
    bottom = min(page_height, top + _integer(row.get("height")))
    return BoundingBox(
        left / page_width,
        top / page_height,
        right / page_width,
        bottom / page_height,
    )


TesseractCliOcrEngine = TesseractOcrEngine

__all__ = [
    "ProcessOutputLimitError",
    "ProcessResult",
    "ProcessRunner",
    "ProcessTimeoutError",
    "TesseractCliOcrEngine",
    "TesseractOcrEngine",
    "run_bounded_process",
]
