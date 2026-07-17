"""Provider-neutral, JSON-safe document intelligence models."""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, TypeAlias

JSONValue: TypeAlias = None | bool | int | float | str | list["JSONValue"] | dict[str, "JSONValue"]


def _json(value: Any) -> JSONValue:
    if value is None or isinstance(value, (bool, int, float, str)):
        if isinstance(value, float) and not math.isfinite(value):
            raise ValueError("JSON values must be finite")
        return value
    if isinstance(value, Mapping):
        return {
            str(key): _json(item) for key, item in sorted(value.items(), key=lambda x: str(x[0]))
        }
    if isinstance(value, (list, tuple)):
        return [_json(item) for item in value]
    raise TypeError(f"Value {type(value).__name__} is not JSON serializable")


def canonical_json(value: Any) -> bytes:
    return json.dumps(
        _json(value), sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode()


def canonical_fingerprint(value: Any) -> str:
    return hashlib.sha256(canonical_json(value)).hexdigest()


@dataclass(frozen=True, slots=True)
class ComponentDescriptor:
    name: str
    version: str
    configuration: Mapping[str, JSONValue] = field(default_factory=dict)
    deterministic: bool = True
    external: bool = False

    def __post_init__(self) -> None:
        if not self.name.strip() or not self.version.strip():
            raise ValueError("ComponentDescriptor name and version must be non-empty")
        _json(self.configuration)

    def to_dict(self) -> dict[str, JSONValue]:
        return {
            "name": self.name,
            "version": self.version,
            "configuration": _json(self.configuration),
            "deterministic": self.deterministic,
            "external": self.external,
        }


@dataclass(frozen=True, slots=True)
class BoundingBox:
    left: float
    top: float
    right: float
    bottom: float

    def __post_init__(self) -> None:
        values = (self.left, self.top, self.right, self.bottom)
        if any(not math.isfinite(value) for value in values):
            raise ValueError("BoundingBox values must be finite")
        if self.left < 0 or self.top < 0 or self.right > 1 or self.bottom > 1:
            raise ValueError("BoundingBox values must be normalized to [0, 1]")
        if self.right < self.left or self.bottom < self.top:
            raise ValueError("BoundingBox edges are inverted")

    def to_dict(self) -> dict[str, float]:
        return {"left": self.left, "top": self.top, "right": self.right, "bottom": self.bottom}


@dataclass(frozen=True, slots=True)
class ExtractionWarning:
    code: str
    message: str
    page_number: int | None = None

    def to_dict(self) -> dict[str, JSONValue]:
        return {"code": self.code, "message": self.message, "page_number": self.page_number}


@dataclass(frozen=True, slots=True)
class OcrToken:
    text: str
    confidence: float | None = None
    coordinates: BoundingBox | None = None


@dataclass(frozen=True, slots=True)
class OcrResult:
    text: str
    tokens: Sequence[OcrToken] = ()
    confidence: float | None = None
    warnings: Sequence[ExtractionWarning] = ()
    usage: Mapping[str, JSONValue] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class TableCell:
    row: int
    column: int
    text: str
    coordinates: BoundingBox | None = None
    rowspan: int = 1
    colspan: int = 1
    header_level: int | None = None
    confidence: float | None = None
    value: JSONValue = None

    def __post_init__(self) -> None:
        if self.row < 0 or self.column < 0 or self.rowspan < 1 or self.colspan < 1:
            raise ValueError("Table cell indexes and spans must be positive")


@dataclass(frozen=True, slots=True)
class TableArtifact:
    table_id: str
    page_number: int | None
    coordinates: BoundingBox | None
    cells: Sequence[TableCell]
    row_count: int
    column_count: int
    caption: str | None = None
    confidence: float | None = None
    engine: ComponentDescriptor | None = None
    warnings: Sequence[ExtractionWarning] = ()
    metadata: Mapping[str, JSONValue] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.table_id or self.row_count < 0 or self.column_count < 0:
            raise ValueError("TableArtifact identity and dimensions are invalid")
        occupied: set[tuple[int, int]] = set()
        for cell in self.cells:
            if (
                cell.row + cell.rowspan > self.row_count
                or cell.column + cell.colspan > self.column_count
            ):
                raise ValueError("Table cell span exceeds table dimensions")
            for row in range(cell.row, cell.row + cell.rowspan):
                for column in range(cell.column, cell.column + cell.colspan):
                    position = (row, column)
                    if position in occupied:
                        raise ValueError("Table cells overlap")
                    occupied.add(position)
        _json(self.metadata)

    def to_dict(self) -> dict[str, JSONValue]:
        return {
            "schema_version": "1",
            "table_id": self.table_id,
            "page_number": self.page_number,
            "coordinates": _json(None if self.coordinates is None else self.coordinates.to_dict()),
            "row_count": self.row_count,
            "column_count": self.column_count,
            "caption": self.caption,
            "confidence": self.confidence,
            "engine": _json(None if self.engine is None else self.engine.to_dict()),
            "warnings": [warning.to_dict() for warning in self.warnings],
            "metadata": _json(self.metadata),
            "cells": [
                {
                    "row": cell.row,
                    "column": cell.column,
                    "text": cell.text,
                    "coordinates": _json(
                        None if cell.coordinates is None else cell.coordinates.to_dict()
                    ),
                    "rowspan": cell.rowspan,
                    "colspan": cell.colspan,
                    "header_level": cell.header_level,
                    "confidence": cell.confidence,
                    "value": _json(cell.value),
                }
                for cell in self.cells
            ],
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> TableArtifact:
        """Reconstruct a validated artifact from its JSON-safe representation."""
        coordinates = value.get("coordinates")
        box = None if not isinstance(coordinates, Mapping) else BoundingBox(**coordinates)
        cells: list[TableCell] = []
        for raw in value.get("cells", ()):
            if not isinstance(raw, Mapping):
                raise ValueError("TableArtifact cells must be mappings")
            cell_box = raw.get("coordinates")
            cells.append(
                TableCell(
                    row=int(raw["row"]),
                    column=int(raw["column"]),
                    text=str(raw.get("text", "")),
                    coordinates=None
                    if not isinstance(cell_box, Mapping)
                    else BoundingBox(**cell_box),
                    rowspan=int(raw.get("rowspan", 1)),
                    colspan=int(raw.get("colspan", 1)),
                    header_level=raw.get("header_level")
                    if isinstance(raw.get("header_level"), int)
                    else None,
                    confidence=raw.get("confidence")
                    if isinstance(raw.get("confidence"), (int, float))
                    else None,
                    value=raw.get("value"),
                )
            )
        return cls(
            table_id=str(value.get("table_id", "vision-table")),
            page_number=value.get("page_number")
            if isinstance(value.get("page_number"), int)
            else None,
            coordinates=box,
            cells=cells,
            row_count=int(value.get("row_count", 0)),
            column_count=int(value.get("column_count", 0)),
            caption=value.get("caption") if isinstance(value.get("caption"), str) else None,
            confidence=value.get("confidence")
            if isinstance(value.get("confidence"), (int, float))
            else None,
            metadata=value.get("metadata", {})
            if isinstance(value.get("metadata"), Mapping)
            else {},
        )


@dataclass(frozen=True, slots=True)
class SplitChunk:
    text: str
    element_type: str = "text"
    page_number: int | None = None
    parent_id: str | None = None
    coordinates: BoundingBox | None = None
    metadata: Mapping[str, JSONValue] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class EngineUsage:
    duration_ms: int | None = None
    cache_hit: bool = False
    attributes: Mapping[str, JSONValue] = field(default_factory=dict)
