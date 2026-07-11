"""Resumable source for ordinary local JSON Lines files."""

from __future__ import annotations

import hashlib
import json
from collections.abc import AsyncIterator, Mapping, Sequence
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
    Envelope,
    Operation,
    Payload,
    RecordPayload,
    Tombstone,
    stable_record_id,
)


class JsonlSource(Source):
    """Read JSON objects with byte-offset checkpoints and prefix validation."""

    def __init__(
        self,
        path: str | Path,
        *,
        stream: str | None = None,
        id_field: str = "id",
        operation_field: str = "_operation",
        batch_size: int = 500,
    ) -> None:
        self.path = Path(path)
        self.stream = stream or self.path.stem
        self.id_field = id_field
        self.operation_field = operation_field
        self.batch_size = batch_size
        if not self.stream:
            raise ConfigurationError("JSONL stream must not be empty")
        if not self.id_field:
            raise ConfigurationError("JSONL id_field must not be empty")
        if not self.operation_field:
            raise ConfigurationError("JSONL operation_field must not be empty")
        if batch_size < 1:
            raise ConfigurationError("JSONL batch_size must be positive")

    def spec(self) -> ConnectorSpec:
        return ConnectorSpec(
            name="jsonl",
            version="1.0.0",
            config_schema={
                "type": "object",
                "properties": {
                    "path": {"type": "string"},
                    "stream": {"type": "string"},
                    "id_field": {"type": "string", "default": "id"},
                    "operation_field": {"type": "string", "default": "_operation"},
                    "batch_size": {"type": "integer", "minimum": 1},
                },
                "required": ["path"],
            },
            capabilities=ConnectorCapabilities(
                incremental=True,
                resumable_full_refresh=True,
                deletes=True,
                schema_discovery=False,
            ),
        )

    async def check(self) -> CheckResult:
        if not self.path.is_file():
            return CheckResult(False, f"JSONL file does not exist: {self.path}")
        try:
            with self.path.open("rb") as handle:
                handle.read(1)
            return CheckResult(True, "JSONL file is readable")
        except OSError as exc:
            return CheckResult(False, str(exc))

    async def discover(self) -> Sequence[StreamDescriptor]:
        return [
            StreamDescriptor(
                name=self.stream,
                namespace="jsonl.file",
                primary_key=(self.id_field,),
                cursor_field=("byte_offset",),
                json_schema={"type": "object"},
            )
        ]

    async def read(
        self,
        stream: StreamDescriptor,
        state: Mapping[str, Any] | None = None,
    ) -> AsyncIterator[SourceMessage]:
        if stream.name != self.stream:
            raise ConfigurationError(f"JSONL stream {stream.name!r} is not configured")
        current = dict(state or {})
        byte_offset = _state_int(current, "byte_offset")
        line_number = _state_int(current, "line_number")
        expected_prefix = current.get("prefix_sha256")
        if expected_prefix is not None and not isinstance(expected_prefix, str):
            raise ConfigurationError("JSONL checkpoint prefix_sha256 must be a string")

        hasher = hashlib.sha256()
        emitted_since_checkpoint = 0
        with self.path.open("rb") as handle:
            prefix = handle.read(byte_offset)
            if len(prefix) != byte_offset:
                raise ConfigurationError("JSONL file is shorter than its saved checkpoint")
            hasher.update(prefix)
            if expected_prefix is not None and hasher.hexdigest() != expected_prefix:
                raise ConfigurationError(
                    "JSONL content before the saved checkpoint changed; use a new pipeline name "
                    "or clear its state to re-ingest the file"
                )

            while raw_line := handle.readline():
                line_number += 1
                byte_offset = handle.tell()
                hasher.update(raw_line)
                if not raw_line.strip():
                    continue
                item = _decode_line(raw_line, line_number)
                native_id = item.get(self.id_field, line_number)
                if native_id is None:
                    native_id = line_number
                operation_value = item.get(self.operation_field, Operation.UPSERT.value)
                try:
                    operation = Operation(str(operation_value).lower())
                except ValueError as exc:
                    raise ConfigurationError(
                        f"JSONL line {line_number} has invalid operation {operation_value!r}"
                    ) from exc
                if operation is Operation.DELETE:
                    payload: Payload = Tombstone("deleted by JSONL input")
                else:
                    payload = RecordPayload(item)
                yield RecordMessage(
                    Envelope(
                        id=stable_record_id("jsonl", self.stream, str(native_id)),
                        source="jsonl",
                        stream=self.stream,
                        payload=payload,
                        operation=operation,
                        cursor=str(byte_offset),
                        metadata={
                            "native_id": str(native_id),
                            "line_number": line_number,
                            "path": str(self.path),
                        },
                        provenance={"connector": "jsonl", "path": str(self.path.resolve())},
                    )
                )
                emitted_since_checkpoint += 1
                if emitted_since_checkpoint >= self.batch_size:
                    yield StateMessage(
                        self.stream,
                        _checkpoint(byte_offset, line_number, hasher.hexdigest()),
                    )
                    emitted_since_checkpoint = 0

        yield StateMessage(
            self.stream,
            _checkpoint(byte_offset, line_number, hasher.hexdigest()),
        )


def _decode_line(raw_line: bytes, line_number: int) -> dict[str, Any]:
    try:
        value = json.loads(raw_line)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ConfigurationError(f"JSONL line {line_number} is not valid UTF-8 JSON") from exc
    if not isinstance(value, dict):
        raise ConfigurationError(f"JSONL line {line_number} must contain a JSON object")
    return {str(key): item for key, item in value.items()}


def _state_int(state: Mapping[str, Any], key: str) -> int:
    value = state.get(key, 0)
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ConfigurationError(f"JSONL checkpoint {key} must be a non-negative integer")
    return value


def _checkpoint(byte_offset: int, line_number: int, prefix_sha256: str) -> dict[str, Any]:
    return {
        "byte_offset": byte_offset,
        "line_number": line_number,
        "prefix_sha256": prefix_sha256,
    }
