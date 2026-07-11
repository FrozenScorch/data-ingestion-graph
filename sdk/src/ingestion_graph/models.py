"""Versioned, JSON-serializable data-plane models."""

from __future__ import annotations

import hashlib
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import Enum, StrEnum
from typing import Any


class Operation(StrEnum):
    SNAPSHOT = "snapshot"
    UPSERT = "upsert"
    DELETE = "delete"


@dataclass(frozen=True, slots=True)
class RecordPayload:
    data: Mapping[str, Any]


@dataclass(frozen=True, slots=True)
class DocumentElement:
    text: str
    element_type: str = "text"
    page_number: int | None = None
    parent_id: str | None = None
    coordinates: Mapping[str, Any] | None = None


@dataclass(frozen=True, slots=True)
class TableBatch:
    columns: Sequence[str]
    rows: Sequence[Mapping[str, Any]]


@dataclass(frozen=True, slots=True)
class BlobRef:
    uri: str
    sha256: str
    size_bytes: int
    media_type: str = "application/octet-stream"


@dataclass(frozen=True, slots=True)
class Tombstone:
    reason: str | None = None


Payload = RecordPayload | DocumentElement | TableBatch | BlobRef | Tombstone


def stable_record_id(source: str, stream: str, native_id: str) -> str:
    """Return a deterministic ID suitable for replay deduplication."""
    raw = f"{source}\x1f{stream}\x1f{native_id}".encode()
    return hashlib.sha256(raw).hexdigest()


@dataclass(frozen=True, slots=True)
class Envelope:
    """Canonical envelope shared by every source and destination."""

    id: str
    source: str
    stream: str
    payload: Payload
    operation: Operation = Operation.UPSERT
    schema_version: str = "1"
    observed_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    event_time: datetime | None = None
    cursor: str | None = None
    checksum: str | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)
    provenance: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        for field_name in ("id", "source", "stream", "schema_version"):
            if not getattr(self, field_name):
                raise ValueError(f"Envelope.{field_name} must not be empty")
        if self.operation is Operation.DELETE and not isinstance(self.payload, Tombstone):
            raise ValueError("DELETE envelopes must carry a Tombstone payload")

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "source": self.source,
            "stream": self.stream,
            "payload": _jsonable(self.payload),
            "operation": self.operation.value,
            "schema_version": self.schema_version,
            "observed_at": _jsonable(self.observed_at),
            "event_time": _jsonable(self.event_time),
            "cursor": self.cursor,
            "checksum": self.checksum,
            "metadata": _jsonable(self.metadata),
            "provenance": _jsonable(self.provenance),
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> Envelope:
        """Rebuild an envelope from its canonical JSON representation."""
        payload_value = value.get("payload")
        if not isinstance(payload_value, Mapping):
            raise ValueError("Envelope.payload must be an object")
        payload = _payload_from_dict(payload_value)
        return cls(
            id=_required_string(value, "id"),
            source=_required_string(value, "source"),
            stream=_required_string(value, "stream"),
            payload=payload,
            operation=Operation(str(value.get("operation", Operation.UPSERT.value))),
            schema_version=str(value.get("schema_version", "1")),
            observed_at=_parse_datetime(value.get("observed_at")) or datetime.now(UTC),
            event_time=_parse_datetime(value.get("event_time")),
            cursor=_optional_string(value.get("cursor")),
            checksum=_optional_string(value.get("checksum")),
            metadata=_mapping(value.get("metadata")),
            provenance=_mapping(value.get("provenance")),
        )


def _jsonable(value: Any) -> Any:
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, datetime):
        return value.astimezone(UTC).isoformat()
    if isinstance(value, RecordPayload):
        return {"data": _jsonable(value.data), "kind": "record"}
    if isinstance(value, DocumentElement):
        return {
            "text": value.text,
            "element_type": value.element_type,
            "page_number": value.page_number,
            "parent_id": value.parent_id,
            "coordinates": _jsonable(value.coordinates),
            "kind": "document_element",
        }
    if isinstance(value, TableBatch):
        return {
            "columns": _jsonable(value.columns),
            "rows": _jsonable(value.rows),
            "kind": "table_batch",
        }
    if isinstance(value, BlobRef):
        return {
            "uri": value.uri,
            "sha256": value.sha256,
            "size_bytes": value.size_bytes,
            "media_type": value.media_type,
            "kind": "blob_ref",
        }
    if isinstance(value, Tombstone):
        return {"reason": value.reason, "kind": "tombstone"}
    if isinstance(value, Mapping):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(item) for item in value]
    return value


def _payload_from_dict(value: Mapping[str, Any]) -> Payload:
    kind = value.get("kind")
    if kind == "record":
        return RecordPayload(_mapping(value.get("data")))
    if kind == "document_element":
        text = value.get("text")
        if not isinstance(text, str):
            raise ValueError("DocumentElement.text must be a string")
        coordinates = value.get("coordinates")
        return DocumentElement(
            text=text,
            element_type=str(value.get("element_type", "text")),
            page_number=_optional_int(value.get("page_number")),
            parent_id=_optional_string(value.get("parent_id")),
            coordinates=None if coordinates is None else _mapping(coordinates),
        )
    if kind == "table_batch":
        columns = value.get("columns")
        rows = value.get("rows")
        if not isinstance(columns, list) or not all(isinstance(item, str) for item in columns):
            raise ValueError("TableBatch.columns must be an array of strings")
        if not isinstance(rows, list):
            raise ValueError("TableBatch.rows must be an array")
        return TableBatch(tuple(columns), tuple(_mapping(row) for row in rows))
    if kind == "blob_ref":
        return BlobRef(
            uri=_required_string(value, "uri"),
            sha256=_required_string(value, "sha256"),
            size_bytes=int(value.get("size_bytes", -1)),
            media_type=str(value.get("media_type", "application/octet-stream")),
        )
    if kind == "tombstone":
        return Tombstone(_optional_string(value.get("reason")))
    raise ValueError(f"Unknown payload kind: {kind!r}")


def _mapping(value: Any) -> Mapping[str, Any]:
    if value is None:
        return {}
    if not isinstance(value, Mapping):
        raise ValueError("Expected an object")
    return {str(key): item for key, item in value.items()}


def _required_string(value: Mapping[str, Any], key: str) -> str:
    item = value.get(key)
    if not isinstance(item, str) or not item:
        raise ValueError(f"Envelope.{key} must be a non-empty string")
    return item


def _optional_string(value: Any) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError("Expected a string or null")
    return value


def _optional_int(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError("Expected an integer or null")
    return value


def _parse_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError("Expected an ISO 8601 datetime string or null")
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("Datetime values must include a timezone")
    return parsed.astimezone(UTC)
