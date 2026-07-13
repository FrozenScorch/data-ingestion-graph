"""Checkpoint-safe local document and folder source."""

from __future__ import annotations

import csv
import hashlib
import json
import mimetypes
import os
import tempfile
import zipfile
from collections.abc import AsyncIterator, Iterable, Iterator, Mapping, Sequence
from contextlib import contextmanager, suppress
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
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
    Operation,
    Payload,
    TableBatch,
    Tombstone,
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
PARSER_VERSION = "2"


@dataclass(frozen=True, slots=True)
class _ParsedElement:
    payload: Payload
    metadata: Mapping[str, Any]


@dataclass(frozen=True, slots=True)
class _Snapshot:
    path: Path
    sha256: str
    stat: os.stat_result


class LocalDocumentsSource(Source):
    """Read supported files or directory trees as canonical SDK envelopes.

    Each configured root is a stream. Per-file SHA-256 and element-count state
    supports mid-document resume, changed-file reconciliation, and tombstones
    when files disappear from a directory root.
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
        stream_names: Sequence[str] | None = None,
        max_file_size_bytes: int = 256 * 1024 * 1024,
        max_archive_uncompressed_bytes: int = 512 * 1024 * 1024,
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
        if max_file_size_bytes < 1 or max_archive_uncompressed_bytes < 1:
            raise ConfigurationError("Document size limits must be positive")
        if stream_names is not None:
            normalized_names = tuple(str(item).strip() for item in stream_names)
            if len(normalized_names) != len(raw_paths) or any(
                not item for item in normalized_names
            ):
                raise ConfigurationError("Document stream_names must name every configured path")
            if len(set(normalized_names)) != len(normalized_names):
                raise ConfigurationError("Document stream_names must be unique")
        else:
            normalized_names = ()

        self.paths = tuple(Path(item).expanduser() for item in raw_paths)
        self.recursive = recursive
        self.extensions = normalized_extensions
        self.checkpoint_interval = checkpoint_interval
        self.text_chunk_chars = text_chunk_chars
        self.table_batch_rows = table_batch_rows
        self.include_hidden = include_hidden
        self.follow_symlinks = follow_symlinks
        self.stream_names = normalized_names
        self.max_file_size_bytes = max_file_size_bytes
        self.max_archive_uncompressed_bytes = max_archive_uncompressed_bytes
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
                    "stream_names": {
                        "type": "array",
                        "items": {"type": "string", "minLength": 1},
                        "description": "Optional stable stream name for each configured path",
                    },
                    "max_file_size_bytes": {
                        "type": "integer",
                        "minimum": 1,
                        "default": 268_435_456,
                    },
                    "max_archive_uncompressed_bytes": {
                        "type": "integer",
                        "minimum": 1,
                        "default": 536_870_912,
                    },
                },
                "required": ["paths"],
            },
            capabilities=ConnectorCapabilities(
                incremental=True,
                resumable_full_refresh=True,
                deletes=True,
                schema_discovery=True,
            ),
        )

    async def check(self) -> CheckResult:
        missing = [str(path) for path in self.paths if not path.exists()]
        if missing:
            return CheckResult(False, f"Document paths do not exist: {', '.join(missing)}")
        if not self.follow_symlinks and any(path.is_symlink() for path in self.paths):
            return CheckResult(False, "Configured document roots must not be symlinks")
        try:
            files = self._discover_files()
        except OSError as exc:
            return CheckResult(False, str(exc))
        dependency_error = _missing_dependency(files)
        if dependency_error:
            return CheckResult(False, dependency_error)
        oversized = [str(path) for path in files if path.stat().st_size > self.max_file_size_bytes]
        if oversized:
            return CheckResult(
                False, f"Document files exceed the configured size limit: {oversized[0]}"
            )
        return CheckResult(True, f"Discovered {len(files)} supported document files")

    async def discover(self) -> Sequence[StreamDescriptor]:
        self._streams = self._name_streams()
        return [
            StreamDescriptor(
                name=name,
                namespace="local.documents",
                primary_key=("element_id",),
                cursor_field=("files", "in_progress"),
                json_schema={
                    "type": "object",
                    "properties": {
                        "path": {"type": "string"},
                        "filename": {"type": "string"},
                        "extension": {"type": "string"},
                        "relative_path": {"type": "string"},
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
        root = self._streams.get(stream.name)
        if root is None:
            raise ConfigurationError(f"Document stream {stream.name!r} is not configured")

        current = _validate_state(state)
        files_state = dict(current["files"])
        prior_parser = current.get("parser_fingerprint")
        parser_fingerprint = self._parser_fingerprint()
        in_progress = current.get("in_progress")
        discovered = self._discover_files(root)
        current_paths = {_relative_path(path, root): path for path in discovered}

        removed_paths = set(files_state) - set(current_paths)
        if isinstance(in_progress, Mapping):
            progress_path = in_progress.get("relative_path")
            if isinstance(progress_path, str) and progress_path not in current_paths:
                removed_paths.add(progress_path)
        for relative_path in sorted(removed_paths):
            prior = files_state.get(relative_path)
            prior_count = int(prior["element_count"]) if prior is not None else 0
            progress_count = (
                int(in_progress.get("next_index", 0))
                if isinstance(in_progress, Mapping)
                and in_progress.get("relative_path") == relative_path
                else 0
            )
            async for message in self._emit_tombstones(
                stream.name,
                relative_path,
                max(prior_count, progress_count),
                files_state,
                parser_fingerprint,
            ):
                yield message
            files_state.pop(relative_path, None)
            yield StateMessage(
                stream.name,
                _checkpoint(files_state, parser_fingerprint),
            )

        for relative_path, path in sorted(current_paths.items()):
            if path.stat().st_size > self.max_file_size_bytes:
                raise ConfigurationError(f"Document file exceeds the configured size limit: {path}")
            prior = files_state.get(relative_path)
            quick_fingerprint = _fingerprint(path)
            if (
                prior_parser == parser_fingerprint
                and prior is not None
                and prior["sha256"] == quick_fingerprint
                and not (
                    isinstance(in_progress, Mapping)
                    and in_progress.get("relative_path") == relative_path
                )
            ):
                continue

            with _snapshot_file(path) as snapshot:
                fingerprint = snapshot.sha256
                if snapshot.stat.st_size > self.max_file_size_bytes:
                    raise ConfigurationError(
                        f"Document file exceeds the configured size limit: {path}"
                    )
                if path.suffix.lower() in {".docx", ".xlsx"}:
                    _validate_archive(
                        snapshot.path,
                        max_uncompressed_bytes=self.max_archive_uncompressed_bytes,
                    )
                resume_index = 0
                if (
                    prior_parser == parser_fingerprint
                    and isinstance(in_progress, Mapping)
                    and in_progress.get("relative_path") == relative_path
                    and in_progress.get("sha256") == fingerprint
                ):
                    resume_index = int(in_progress.get("next_index", 0))

                parsed_count = 0
                emitted = 0
                elements = _parse_file(
                    snapshot.path,
                    text_chunk_chars=self.text_chunk_chars,
                    table_batch_rows=self.table_batch_rows,
                )
                for index, element in enumerate(elements):
                    parsed_count = index + 1
                    if index < resume_index:
                        continue
                    yield RecordMessage(
                        self._record_envelope(
                            stream.name,
                            path,
                            relative_path,
                            fingerprint,
                            snapshot.stat,
                            index,
                            element,
                        )
                    )
                    emitted += 1
                    if emitted >= self.checkpoint_interval:
                        yield StateMessage(
                            stream.name,
                            _checkpoint(
                                files_state,
                                parser_fingerprint,
                                {
                                    "relative_path": relative_path,
                                    "sha256": fingerprint,
                                    "next_index": index + 1,
                                },
                            ),
                        )
                        emitted = 0

                if resume_index > parsed_count:
                    raise ConfigurationError(
                        f"Document checkpoint next_index exceeds parsed elements for {path}"
                    )
                prior_count = int(prior["element_count"]) if prior is not None else 0
                if prior_count > parsed_count:
                    async for message in self._emit_tombstones(
                        stream.name,
                        relative_path,
                        prior_count,
                        files_state,
                        parser_fingerprint,
                        start=parsed_count,
                    ):
                        yield message
                files_state[relative_path] = {
                    "sha256": fingerprint,
                    "element_count": parsed_count,
                }
                yield StateMessage(
                    stream.name,
                    _checkpoint(files_state, parser_fingerprint),
                )

        yield StateMessage(stream.name, _checkpoint(files_state, parser_fingerprint))

    def _record_envelope(
        self,
        stream_name: str,
        original_path: Path,
        relative_path: str,
        fingerprint: str,
        stat: os.stat_result,
        index: int,
        element: _ParsedElement,
    ) -> Envelope:
        element_id = f"{relative_path}:{index}"
        event_time = datetime.fromtimestamp(stat.st_mtime, UTC)
        media_type = mimetypes.guess_type(original_path.name)[0] or "application/octet-stream"
        return Envelope(
            id=stable_record_id("local_documents", stream_name, element_id),
            source="local_documents",
            stream=stream_name,
            payload=element.payload,
            cursor=f"{fingerprint}:{index}",
            checksum=_payload_checksum(element.payload),
            event_time=event_time,
            metadata={
                "element_id": element_id,
                "element_index": index,
                "relative_path": relative_path,
                "filename": original_path.name,
                "extension": original_path.suffix.lower(),
                "media_type": media_type,
                "size_bytes": stat.st_size,
                "modified_at": event_time.isoformat(),
                **element.metadata,
            },
            provenance={
                "connector": "local_documents",
                "path": str(original_path.resolve()),
                "sha256": fingerprint,
            },
        )

    async def _emit_tombstones(
        self,
        stream_name: str,
        relative_path: str,
        count: int,
        files_state: Mapping[str, Mapping[str, Any]],
        parser_fingerprint: str,
        *,
        start: int = 0,
    ) -> AsyncIterator[SourceMessage]:
        emitted = 0
        for index in range(start, count):
            element_id = f"{relative_path}:{index}"
            yield RecordMessage(
                Envelope(
                    id=stable_record_id("local_documents", stream_name, element_id),
                    source="local_documents",
                    stream=stream_name,
                    payload=Tombstone("document element removed"),
                    operation=Operation.DELETE,
                    cursor=f"deleted:{relative_path}:{index}",
                    metadata={
                        "element_id": element_id,
                        "element_index": index,
                        "relative_path": relative_path,
                    },
                    provenance={"connector": "local_documents"},
                )
            )
            emitted += 1
            if emitted >= self.checkpoint_interval:
                yield StateMessage(
                    stream_name,
                    _checkpoint(files_state, parser_fingerprint),
                )
                emitted = 0

    def _parser_fingerprint(self) -> str:
        raw = json.dumps(
            {
                "parser_version": PARSER_VERSION,
                "text_chunk_chars": self.text_chunk_chars,
                "table_batch_rows": self.table_batch_rows,
            },
            sort_keys=True,
        )
        return hashlib.sha256(raw.encode()).hexdigest()

    def _discover_files(self, only_root: Path | None = None) -> list[Path]:
        files: list[Path] = []
        for root in (only_root,) if only_root is not None else self.paths:
            if not self.follow_symlinks and root.is_symlink():
                raise ConfigurationError(f"Configured document root must not be a symlink: {root}")
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
                if not self.follow_symlinks:
                    try:
                        candidate.resolve(strict=True).relative_to(root.resolve(strict=True))
                    except (OSError, ValueError):
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

    def _name_streams(self) -> dict[str, Path]:
        streams: dict[str, Path] = {}
        for index, root in enumerate(self.paths):
            if self.stream_names:
                name = self.stream_names[index]
            else:
                base = root.stem if root.is_file() else root.name
                path_hash = hashlib.sha256(str(root.resolve()).encode()).hexdigest()[:10]
                name = f"{base or 'documents'}#{path_hash}"
            streams[name] = root
        return streams


def _normalize_extension(value: str) -> str:
    normalized = str(value).strip().lower()
    if not normalized:
        raise ConfigurationError("Document extension must not be empty")
    return normalized if normalized.startswith(".") else f".{normalized}"


def _relative_path(path: Path, root: Path) -> str:
    return path.name if root.is_file() else path.relative_to(root).as_posix()


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


@contextmanager
def _snapshot_file(path: Path) -> Iterator[_Snapshot]:
    flags = os.O_RDONLY | getattr(os, "O_BINARY", 0) | getattr(os, "O_NOFOLLOW", 0)
    source_descriptor: int | None = None
    temp_descriptor: int | None = None
    temp_name: str | None = None
    digest = hashlib.sha256()
    try:
        source_descriptor = os.open(path, flags)
        temp_descriptor, temp_name = tempfile.mkstemp(
            prefix="ingestion-document-", suffix=path.suffix
        )
        with (
            os.fdopen(source_descriptor, "rb") as source,
            os.fdopen(temp_descriptor, "wb") as target,
        ):
            source_descriptor = None
            temp_descriptor = None
            source_stat = os.fstat(source.fileno())
            while block := source.read(1024 * 1024):
                digest.update(block)
                target.write(block)
            target.flush()
        yield _Snapshot(Path(temp_name), digest.hexdigest(), source_stat)
    finally:
        if source_descriptor is not None:
            with suppress(OSError):
                os.close(source_descriptor)
        if temp_descriptor is not None:
            with suppress(OSError):
                os.close(temp_descriptor)
        if temp_name is not None:
            Path(temp_name).unlink(missing_ok=True)


def _validate_state(state: Mapping[str, Any] | None) -> dict[str, Any]:
    current = dict(state or {})
    raw_files = current.get("files", {})
    if not isinstance(raw_files, Mapping):
        raise ConfigurationError("Document checkpoint files must be an object")
    files: dict[str, dict[str, Any]] = {}
    for relative_path, raw_file in raw_files.items():
        if not isinstance(relative_path, str) or not isinstance(raw_file, Mapping):
            raise ConfigurationError("Document checkpoint file entries are invalid")
        sha256 = raw_file.get("sha256")
        element_count = raw_file.get("element_count")
        if not isinstance(sha256, str):
            raise ConfigurationError("Document checkpoint file sha256 must be a string")
        if (
            isinstance(element_count, bool)
            or not isinstance(element_count, int)
            or element_count < 0
        ):
            raise ConfigurationError("Document checkpoint file element_count must be non-negative")
        files[relative_path] = {"sha256": sha256, "element_count": element_count}

    parser_fingerprint = current.get("parser_fingerprint")
    if parser_fingerprint is not None and not isinstance(parser_fingerprint, str):
        raise ConfigurationError("Document checkpoint parser_fingerprint must be a string")
    in_progress = current.get("in_progress")
    if in_progress is not None:
        if not isinstance(in_progress, Mapping):
            raise ConfigurationError("Document checkpoint in_progress must be an object")
        relative_path = in_progress.get("relative_path")
        sha256 = in_progress.get("sha256")
        next_index = in_progress.get("next_index")
        if not isinstance(relative_path, str) or not isinstance(sha256, str):
            raise ConfigurationError("Document checkpoint in_progress identity is invalid")
        if isinstance(next_index, bool) or not isinstance(next_index, int) or next_index < 0:
            raise ConfigurationError(
                "Document checkpoint in_progress next_index must be non-negative"
            )
        in_progress = {
            "relative_path": relative_path,
            "sha256": sha256,
            "next_index": next_index,
        }
    return {
        "files": files,
        "parser_fingerprint": parser_fingerprint,
        "in_progress": in_progress,
    }


def _checkpoint(
    files: Mapping[str, Mapping[str, Any]],
    parser_fingerprint: str,
    in_progress: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    value: dict[str, Any] = {
        "parser_fingerprint": parser_fingerprint,
        "files": {key: dict(item) for key, item in files.items()},
    }
    if in_progress is not None:
        value["in_progress"] = dict(in_progress)
    return value


def _missing_dependency(files: Sequence[Path]) -> str | None:
    extensions = {path.suffix.lower() for path in files}
    if ".pdf" in extensions and PdfReader is None:
        return "PDF support requires: pip install 'ingestion-graph[documents]'"
    if ".docx" in extensions and WordDocument is None:
        return "Word support requires: pip install 'ingestion-graph[documents]'"
    if ".xlsx" in extensions and load_workbook is None:
        return "Excel support requires: pip install 'ingestion-graph[documents]'"
    return None


def _validate_archive(path: Path, *, max_uncompressed_bytes: int) -> None:
    try:
        with zipfile.ZipFile(path) as archive:
            total = sum(item.file_size for item in archive.infolist())
    except zipfile.BadZipFile as exc:
        raise ConfigurationError(f"Document archive is invalid: {path.name}") from exc
    if total > max_uncompressed_bytes:
        raise ConfigurationError(
            f"Document archive exceeds the uncompressed size limit: {path.name}"
        )


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
        return _parse_docx(path, text_chunk_chars, table_batch_rows)
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


def _parse_pdf(path: Path) -> Iterable[_ParsedElement]:
    if PdfReader is None:
        raise ConfigurationError("PDF support requires: pip install 'ingestion-graph[documents]'")
    reader = PdfReader(str(path))
    for index, page in enumerate(reader.pages, start=1):
        yield _ParsedElement(
            DocumentElement(
                text=page.extract_text() or "",
                element_type="page",
                page_number=index,
            ),
            {"native_id": f"page-{index}", "page_number": index},
        )


def _parse_docx(path: Path, chunk_chars: int, batch_rows: int) -> list[_ParsedElement]:
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
        for batch_index, offset in enumerate(range(0, len(rows), batch_rows)):
            batch = rows[offset : offset + batch_rows]
            elements.append(
                _ParsedElement(
                    TableBatch(columns, batch),
                    {
                        "native_id": f"table-{table_index}-batch-{batch_index}",
                        "table_index": table_index,
                        "batch_index": batch_index,
                        "row_count": len(batch),
                    },
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
            columns = _column_names(tuple(_json_cell(value) for value in first))
            rows: list[Mapping[str, Any]] = []
            batch_index = 0
            for values in iterator:
                rows.append(
                    dict(
                        zip(
                            columns,
                            (_json_cell(value) for value in values),
                            strict=False,
                        )
                    )
                )
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
        reader = csv.reader(handle)
        header = next(reader, ())
        columns = _column_names(header)
        rows: list[Mapping[str, Any]] = []
        batch_index = 0
        for values in reader:
            padded = [*values, *([None] * max(0, len(columns) - len(values)))]
            rows.append(dict(zip(columns, padded, strict=False)))
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
    start = 0
    while start < len(normalized):
        remaining_length = len(normalized) - start
        if remaining_length <= limit:
            chunks.append(normalized[start:])
            break
        end = start + limit
        split_at = normalized.rfind("\n", start, end + 1)
        if split_at < start + limit // 2:
            split_at = normalized.rfind(" ", start, end + 1)
        if split_at < start + limit // 2:
            split_at = end
        chunks.append(normalized[start:split_at].strip())
        start = split_at
        while start < len(normalized) and normalized[start].isspace():
            start += 1
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


def _json_cell(value: Any) -> Any:
    if value is None or isinstance(value, (str, bool, int, float)):
        return value
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, (date, time)):
        return value.isoformat()
    if isinstance(value, timedelta):
        return value.total_seconds()
    return str(value)


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
