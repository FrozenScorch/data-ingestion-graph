"""Checkpoint-safe local document and folder source."""

from __future__ import annotations

import csv
import hashlib
import json
import mimetypes
import os
from collections.abc import AsyncIterator, Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from email import policy
from email.parser import BytesParser
from html.parser import HTMLParser
from pathlib import Path
from typing import Any

from ingestion_graph.connectors.base import (
    CheckResult,
    ConnectorCapabilities,
    ConnectorSpec,
    Source,
    StreamDescriptor,
)
from ingestion_graph.errors import ConfigurationError
from ingestion_graph.messages import RecordMessage, SourceMessage, StateMessage
from ingestion_graph.models import (
    DocumentElement,
    Envelope,
    Payload,
    TableBatch,
    stable_record_id,
)

WordDocument: Any
try:
    from docx import Document as _WordDocument

    WordDocument = _WordDocument
except ImportError:  # pragma: no cover - exercised without the optional extra
    WordDocument = None

load_workbook: Any
try:
    from openpyxl import load_workbook as _load_workbook  # type: ignore[import-untyped]

    load_workbook = _load_workbook
except ImportError:  # pragma: no cover - exercised without the optional extra
    load_workbook = None

PdfReader: Any
try:
    from pypdf import PdfReader as _PdfReader

    PdfReader = _PdfReader
except ImportError:  # pragma: no cover - exercised without the optional extra
    PdfReader = None


SUPPORTED_EXTENSIONS = (
    ".csv",
    ".docx",
    ".eml",
    ".htm",
    ".html",
    ".md",
    ".pdf",
    ".txt",
    ".xlsx",
)


@dataclass(frozen=True, slots=True)
class _ParsedElement:
    payload: Payload
    metadata: Mapping[str, Any]


class LocalDocumentsSource(Source):
    """Read supported files or directory trees as canonical SDK envelopes.

    Each file is a discoverable stream. Checkpoints contain the file SHA-256
    and next element index, so a crash resumes within an unchanged document and
    a content change safely restarts that file from its first element.
    """

    def __init__(
        self,
        paths: str | Path | Sequence[str | Path],
        *,
        recursive: bool = True,
        extensions: Sequence[str] = SUPPORTED_EXTENSIONS,
        checkpoint_interval: int = 50,
        text_chunk_chars: int = 12_000,
        table_batch_rows: int = 500,
        include_hidden: bool = False,
        follow_symlinks: bool = False,
    ) -> None:
        raw_paths = (paths,) if isinstance(paths, (str, Path)) else tuple(paths)
        if not raw_paths:
            raise ConfigurationError("Document paths must not be empty")
        normalized_extensions = tuple(_normalize_extension(item) for item in extensions)
        if not normalized_extensions:
            raise ConfigurationError("Document extensions must not be empty")
        unsupported = sorted(set(normalized_extensions) - set(SUPPORTED_EXTENSIONS))
        if unsupported:
            raise ConfigurationError(f"Unsupported document extensions: {', '.join(unsupported)}")
        if checkpoint_interval < 1:
            raise ConfigurationError("Document checkpoint_interval must be positive")
        if text_chunk_chars < 256:
            raise ConfigurationError("Document text_chunk_chars must be at least 256")
        if table_batch_rows < 1:
            raise ConfigurationError("Document table_batch_rows must be positive")

        self.paths = tuple(Path(item).expanduser() for item in raw_paths)
        self.recursive = recursive
        self.extensions = normalized_extensions
        self.checkpoint_interval = checkpoint_interval
        self.text_chunk_chars = text_chunk_chars
        self.table_batch_rows = table_batch_rows
        self.include_hidden = include_hidden
        self.follow_symlinks = follow_symlinks
        self._streams: dict[str, Path] = {}

    def spec(self) -> ConnectorSpec:
        return ConnectorSpec(
            name="local_documents",
            version="1.0.0",
            config_schema={
                "type": "object",
                "properties": {
                    "paths": {
                        "type": "array",
                        "items": {"type": "string"},
                        "minItems": 1,
                        "format": "local-paths",
                    },
                    "recursive": {"type": "boolean", "default": True},
                    "extensions": {
                        "type": "array",
                        "items": {"type": "string", "enum": list(SUPPORTED_EXTENSIONS)},
                        "default": list(SUPPORTED_EXTENSIONS),
                    },
                    "checkpoint_interval": {
                        "type": "integer",
                        "minimum": 1,
                        "default": 50,
                    },
                    "text_chunk_chars": {
                        "type": "integer",
                        "minimum": 256,
                        "default": 12_000,
                    },
                    "table_batch_rows": {
                        "type": "integer",
                        "minimum": 1,
                        "default": 500,
                    },
                    "include_hidden": {"type": "boolean", "default": False},
                    "follow_symlinks": {"type": "boolean", "default": False},
                },
                "required": ["paths"],
            },
            capabilities=ConnectorCapabilities(
                incremental=True,
                resumable_full_refresh=True,
                deletes=False,
                schema_discovery=True,
            ),
        )

    async def check(self) -> CheckResult:
        missing = [str(path) for path in self.paths if not path.exists()]
        if missing:
            return CheckResult(False, f"Document paths do not exist: {', '.join(missing)}")
        try:
            files = self._discover_files()
        except OSError as exc:
            return CheckResult(False, str(exc))
        dependency_error = _missing_dependency(files)
        if dependency_error:
            return CheckResult(False, dependency_error)
        return CheckResult(True, f"Discovered {len(files)} supported document files")

    async def discover(self) -> Sequence[StreamDescriptor]:
        files = self._discover_files()
        self._streams = self._name_streams(files)
        return [
            StreamDescriptor(
                name=name,
                namespace="local.documents",
                primary_key=("element_id",),
                cursor_field=("sha256", "next_index"),
                json_schema={
                    "type": "object",
                    "properties": {
                        "path": {"type": "string"},
                        "filename": {"type": "string"},
                        "extension": {"type": "string"},
                        "sha256": {"type": "string"},
                        "element_index": {"type": "integer"},
                    },
                },
            )
            for name in self._streams
        ]

    async def read(
        self,
        stream: StreamDescriptor,
        state: Mapping[str, Any] | None = None,
    ) -> AsyncIterator[SourceMessage]:
        if stream.name not in self._streams:
            await self.discover()
        path = self._streams.get(stream.name)
        if path is None:
            raise ConfigurationError(f"Document stream {stream.name!r} is not configured")

        fingerprint = _fingerprint(path)
        current = _validate_state(state)
        if current.get("sha256") == fingerprint and current.get("complete") is True:
            yield StateMessage(stream.name, current)
            return
        next_index = (
            int(current.get("next_index", 0)) if current.get("sha256") == fingerprint else 0
        )
        elements = _parse_file(
            path,
            text_chunk_chars=self.text_chunk_chars,
            table_batch_rows=self.table_batch_rows,
        )
        stat = path.stat()
        media_type = mimetypes.guess_type(path.name)[0] or "application/octet-stream"
        event_time = datetime.fromtimestamp(stat.st_mtime, UTC)
        emitted = 0
        parsed_count = 0
        for index, element in enumerate(elements):
            parsed_count = index + 1
            if index < next_index:
                continue
            native_id = str(element.metadata.get("native_id", index))
            checksum = _payload_checksum(element.payload)
            yield RecordMessage(
                Envelope(
                    id=stable_record_id("local_documents", stream.name, native_id),
                    source="local_documents",
                    stream=stream.name,
                    payload=element.payload,
                    cursor=f"{fingerprint}:{index}",
                    checksum=checksum,
                    event_time=event_time,
                    metadata={
                        "element_id": native_id,
                        "element_index": index,
                        "path": str(path),
                        "filename": path.name,
                        "extension": path.suffix.lower(),
                        "media_type": media_type,
                        "size_bytes": stat.st_size,
                        "modified_at": event_time.isoformat(),
                        **element.metadata,
                    },
                    provenance={
                        "connector": "local_documents",
                        "path": str(path.resolve()),
                        "sha256": fingerprint,
                    },
                )
            )
            emitted += 1
            if emitted >= self.checkpoint_interval:
                yield StateMessage(
                    stream.name,
                    {"sha256": fingerprint, "next_index": index + 1, "complete": False},
                )
                emitted = 0

        if next_index > parsed_count:
            raise ConfigurationError(
                f"Document checkpoint next_index exceeds parsed elements for {path}"
            )

        yield StateMessage(
            stream.name,
            {"sha256": fingerprint, "next_index": parsed_count, "complete": True},
        )

    def _discover_files(self) -> list[Path]:
        files: list[Path] = []
        for root in self.paths:
            if root.is_file():
                candidates: Iterable[Path] = (root,)
            elif root.is_dir():
                candidates = self._walk_directory(root)
            else:
                continue
            for candidate in candidates:
                if not candidate.is_file() or candidate.suffix.lower() not in self.extensions:
                    continue
                if not self.follow_symlinks and candidate.is_symlink():
                    continue
                if not self.include_hidden and _is_hidden(candidate, root):
                    continue
                files.append(candidate)
        return sorted(set(files), key=lambda item: str(item).casefold())

    def _walk_directory(self, root: Path) -> Iterable[Path]:
        for current, directory_names, file_names in os.walk(root, followlinks=self.follow_symlinks):
            current_path = Path(current)
            if not self.include_hidden:
                directory_names[:] = [name for name in directory_names if not name.startswith(".")]
            if not self.follow_symlinks:
                directory_names[:] = [
                    name for name in directory_names if not (current_path / name).is_symlink()
                ]
            if not self.recursive:
                directory_names.clear()
            yield from (current_path / name for name in file_names)

    def _name_streams(self, files: Sequence[Path]) -> dict[str, Path]:
        streams: dict[str, Path] = {}
        multiple_roots = len(self.paths) > 1
        for path in files:
            root_index, root = _containing_root(path, self.paths)
            relative = path.name if root.is_file() else path.relative_to(root).as_posix()
            name = f"root-{root_index}/{relative}" if multiple_roots else relative
            if name in streams and streams[name] != path:
                suffix = hashlib.sha256(str(path.resolve()).encode()).hexdigest()[:10]
                name = f"{name}#{suffix}"
            streams[name] = path
        return streams


def _normalize_extension(value: str) -> str:
    normalized = str(value).strip().lower()
    if not normalized:
        raise ConfigurationError("Document extension must not be empty")
    return normalized if normalized.startswith(".") else f".{normalized}"


def _containing_root(path: Path, roots: Sequence[Path]) -> tuple[int, Path]:
    for index, root in enumerate(roots):
        if root.is_file() and path == root:
            return index, root
        if root.is_dir():
            try:
                path.relative_to(root)
                return index, root
            except ValueError:
                continue
    raise ConfigurationError(f"Document path is outside configured roots: {path}")


def _is_hidden(path: Path, root: Path) -> bool:
    boundary = root.parent if root.is_file() else root
    try:
        relative = path.relative_to(boundary)
    except ValueError:
        relative = path
    return any(part.startswith(".") for part in relative.parts)


def _fingerprint(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _validate_state(state: Mapping[str, Any] | None) -> dict[str, Any]:
    current = dict(state or {})
    sha256 = current.get("sha256")
    next_index = current.get("next_index", 0)
    complete = current.get("complete", False)
    if sha256 is not None and not isinstance(sha256, str):
        raise ConfigurationError("Document checkpoint sha256 must be a string")
    if isinstance(next_index, bool) or not isinstance(next_index, int) or next_index < 0:
        raise ConfigurationError("Document checkpoint next_index must be non-negative")
    if not isinstance(complete, bool):
        raise ConfigurationError("Document checkpoint complete must be boolean")
    return current


def _missing_dependency(files: Sequence[Path]) -> str | None:
    extensions = {path.suffix.lower() for path in files}
    if ".pdf" in extensions and PdfReader is None:
        return "PDF support requires: pip install 'ingestion-graph[documents]'"
    if ".docx" in extensions and WordDocument is None:
        return "Word support requires: pip install 'ingestion-graph[documents]'"
    if ".xlsx" in extensions and load_workbook is None:
        return "Excel support requires: pip install 'ingestion-graph[documents]'"
    return None


def _parse_file(
    path: Path,
    *,
    text_chunk_chars: int,
    table_batch_rows: int,
) -> Iterable[_ParsedElement]:
    extension = path.suffix.lower()
    if extension == ".pdf":
        return _parse_pdf(path)
    if extension == ".docx":
        return _parse_docx(path, text_chunk_chars)
    if extension == ".xlsx":
        return _parse_xlsx(path, table_batch_rows)
    if extension == ".csv":
        return _parse_csv(path, table_batch_rows)
    if extension == ".eml":
        return _parse_email(path, text_chunk_chars)
    text = path.read_text(encoding="utf-8", errors="replace")
    if extension in {".htm", ".html"}:
        parser = _TextHTMLParser()
        parser.feed(text)
        text = parser.text
    return _document_chunks(text, text_chunk_chars, element_type="text")


def _parse_pdf(path: Path) -> list[_ParsedElement]:
    if PdfReader is None:
        raise ConfigurationError("PDF support requires: pip install 'ingestion-graph[documents]'")
    reader = PdfReader(str(path))
    return [
        _ParsedElement(
            DocumentElement(
                text=page.extract_text() or "",
                element_type="page",
                page_number=index,
            ),
            {"native_id": f"page-{index}", "page_number": index},
        )
        for index, page in enumerate(reader.pages, start=1)
    ]


def _parse_docx(path: Path, chunk_chars: int) -> list[_ParsedElement]:
    if WordDocument is None:
        raise ConfigurationError("Word support requires: pip install 'ingestion-graph[documents]'")
    document = WordDocument(str(path))
    paragraphs = "\n\n".join(item.text for item in document.paragraphs if item.text.strip())
    elements = _document_chunks(paragraphs, chunk_chars, element_type="paragraphs")
    for table_index, table in enumerate(document.tables, start=1):
        values = [[cell.text for cell in row.cells] for row in table.rows]
        if not values:
            continue
        columns = _column_names(values[0])
        rows = [dict(zip(columns, row, strict=False)) for row in values[1:]]
        elements.append(
            _ParsedElement(
                TableBatch(columns, rows),
                {"native_id": f"table-{table_index}", "table_index": table_index},
            )
        )
    return elements


def _parse_xlsx(path: Path, batch_rows: int) -> Iterable[_ParsedElement]:
    if load_workbook is None:
        raise ConfigurationError("Excel support requires: pip install 'ingestion-graph[documents]'")
    workbook = load_workbook(filename=str(path), read_only=True, data_only=True)
    try:
        for worksheet in workbook.worksheets:
            iterator = worksheet.iter_rows(values_only=True)
            first = next(iterator, None)
            if first is None:
                continue
            columns = _column_names(first)
            rows: list[Mapping[str, Any]] = []
            batch_index = 0
            for values in iterator:
                rows.append(dict(zip(columns, values, strict=False)))
                if len(rows) >= batch_rows:
                    yield _table_element(worksheet.title, batch_index, columns, rows)
                    rows = []
                    batch_index += 1
            if rows:
                yield _table_element(worksheet.title, batch_index, columns, rows)
    finally:
        workbook.close()


def _parse_csv(path: Path, batch_rows: int) -> Iterable[_ParsedElement]:
    with path.open("r", encoding="utf-8-sig", errors="replace", newline="") as handle:
        reader = csv.DictReader(handle)
        columns = _column_names(reader.fieldnames or ())
        rows: list[Mapping[str, Any]] = []
        batch_index = 0
        for row in reader:
            rows.append(
                {
                    column: row.get(original)
                    for column, original in zip(columns, reader.fieldnames or (), strict=False)
                }
            )
            if len(rows) >= batch_rows:
                yield _table_element("csv", batch_index, columns, rows)
                rows = []
                batch_index += 1
        if rows:
            yield _table_element("csv", batch_index, columns, rows)


def _parse_email(path: Path, chunk_chars: int) -> list[_ParsedElement]:
    message = BytesParser(policy=policy.default).parsebytes(path.read_bytes())
    body = message.get_body(preferencelist=("plain", "html"))
    text = body.get_content() if body is not None else ""
    if body is not None and body.get_content_type() == "text/html":
        parser = _TextHTMLParser()
        parser.feed(str(text))
        text = parser.text
    attachments = [
        {
            "filename": part.get_filename(),
            "content_type": part.get_content_type(),
            "size_bytes": len(part.get_payload(decode=True) or b""),
        }
        for part in message.iter_attachments()
    ]
    common = {
        "subject": str(message.get("subject", "")),
        "from": str(message.get("from", "")),
        "to": str(message.get("to", "")),
        "date": str(message.get("date", "")),
        "message_id": str(message.get("message-id", "")),
        "attachments": attachments,
    }
    chunks = _document_chunks(str(text), chunk_chars, element_type="email_body")
    return [_ParsedElement(item.payload, {**common, **item.metadata}) for item in chunks] or [
        _ParsedElement(DocumentElement("", "email_body"), {"native_id": "body-0", **common})
    ]


def _document_chunks(text: str, limit: int, *, element_type: str) -> list[_ParsedElement]:
    normalized = "\n".join(line.rstrip() for line in text.splitlines()).strip()
    if not normalized:
        return []
    chunks: list[str] = []
    remaining = normalized
    while remaining:
        if len(remaining) <= limit:
            chunks.append(remaining)
            break
        split_at = remaining.rfind("\n", 0, limit + 1)
        if split_at < limit // 2:
            split_at = remaining.rfind(" ", 0, limit + 1)
        if split_at < limit // 2:
            split_at = limit
        chunks.append(remaining[:split_at].strip())
        remaining = remaining[split_at:].lstrip()
    return [
        _ParsedElement(
            DocumentElement(chunk, element_type=element_type),
            {"native_id": f"{element_type}-{index}", "chunk_index": index},
        )
        for index, chunk in enumerate(chunks)
    ]


def _column_names(values: Sequence[Any]) -> tuple[str, ...]:
    columns: list[str] = []
    seen: dict[str, int] = {}
    for index, value in enumerate(values, start=1):
        base = str(value).strip() if value is not None else ""
        base = base or f"column_{index}"
        count = seen.get(base, 0) + 1
        seen[base] = count
        columns.append(base if count == 1 else f"{base}_{count}")
    return tuple(columns)


def _table_element(
    sheet: str,
    batch_index: int,
    columns: Sequence[str],
    rows: Sequence[Mapping[str, Any]],
) -> _ParsedElement:
    return _ParsedElement(
        TableBatch(tuple(columns), tuple(dict(row) for row in rows)),
        {
            "native_id": f"{sheet}-batch-{batch_index}",
            "sheet": sheet,
            "batch_index": batch_index,
            "row_count": len(rows),
        },
    )


def _payload_checksum(payload: Payload) -> str:
    if isinstance(payload, DocumentElement):
        value: Any = {
            "text": payload.text,
            "element_type": payload.element_type,
            "page_number": payload.page_number,
        }
    elif isinstance(payload, TableBatch):
        value = {"columns": list(payload.columns), "rows": list(payload.rows)}
    else:
        value = repr(payload)
    encoded = json.dumps(value, sort_keys=True, default=str, ensure_ascii=False).encode()
    return hashlib.sha256(encoded).hexdigest()


class _TextHTMLParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self._parts: list[str] = []
        self._ignored_depth = 0

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.lower() in {"script", "style"}:
            self._ignored_depth += 1

    def handle_endtag(self, tag: str) -> None:
        if tag.lower() in {"script", "style"} and self._ignored_depth:
            self._ignored_depth -= 1

    def handle_data(self, data: str) -> None:
        if not self._ignored_depth and data.strip():
            self._parts.append(data.strip())

    @property
    def text(self) -> str:
        return "\n".join(self._parts)
