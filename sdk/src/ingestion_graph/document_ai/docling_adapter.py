"""Offline-safe normalization adapter for Docling table output."""

from __future__ import annotations

import asyncio
import inspect
from collections.abc import Callable, Mapping, Sequence
from typing import Any

from ingestion_graph.document_ai.models import (
    BoundingBox,
    ComponentDescriptor,
    TableArtifact,
    TableCell,
)
from ingestion_graph.errors import ConfigurationError

ConverterFactory = Callable[[], object]


class DoclingTableExtractor:
    """Normalize a preconfigured offline Docling converter's table output.

    The SDK intentionally does not construct Docling's default converter because doing so
    may fetch model artifacts. Applications must inject a factory whose converter and model
    files are already configured locally.
    """

    descriptor = ComponentDescriptor(
        "docling",
        "1",
        configuration={"offline_converter_required": True},
        deterministic=True,
        external=False,
    )

    def __init__(self, *, converter_factory: ConverterFactory | None = None) -> None:
        self._converter_factory = converter_factory
        self._converter: object | None = None
        self._converter_lock = asyncio.Lock()

    async def extract(
        self, image: bytes, *, page_number: int | None = None
    ) -> Sequence[TableArtifact]:
        if not image:
            raise ValueError("table image must not be empty")
        if page_number is not None and page_number < 1:
            raise ValueError("page_number must be positive when provided")
        converter = await self._get_converter()
        result = await asyncio.to_thread(_convert, converter, image)
        return tuple(_normalize_result(result, image=image, page_number=page_number))

    async def _get_converter(self) -> object:
        if self._converter is not None:
            return self._converter
        async with self._converter_lock:
            if self._converter is None:
                if self._converter_factory is None:
                    raise ConfigurationError(
                        "Docling table extraction requires an explicitly configured offline "
                        "converter_factory; automatic model downloads are disabled"
                    )
                self._converter = await asyncio.to_thread(self._converter_factory)
            return self._converter

    async def close(self) -> None:
        converter, self._converter = self._converter, None
        if converter is None:
            return
        close = getattr(converter, "close", None)
        if not callable(close):
            return
        result = close()
        if inspect.isawaitable(result):
            await result


def _convert(converter: object, image: bytes) -> object:
    convert = getattr(converter, "convert_bytes", None)
    if not callable(convert):
        convert = getattr(converter, "convert", None)
    if not callable(convert):
        raise ConfigurationError("The offline Docling converter has no bytes conversion method")
    return convert(image)


def _normalize_result(
    result: object, *, image: bytes, page_number: int | None
) -> list[TableArtifact]:
    document = _member(result, "document", result)
    tables = _member(document, "tables", None)
    if tables is None and isinstance(document, Sequence) and not isinstance(document, (str, bytes)):
        tables = document
    if not isinstance(tables, Sequence) or isinstance(tables, (str, bytes)):
        raise ConfigurationError("Docling converter returned no recognizable table collection")

    import hashlib

    digest = hashlib.sha256(image).hexdigest()[:16]
    artifacts: list[TableArtifact] = []
    for index, table in enumerate(tables):
        artifacts.append(
            _normalize_table(
                table,
                table_id=f"docling-{digest}-{index + 1}",
                page_number=page_number,
            )
        )
    return artifacts


def _normalize_table(table: object, *, table_id: str, page_number: int | None) -> TableArtifact:
    matrix = _matrix(table)
    if matrix is not None:
        cells = tuple(
            TableCell(row, column, _text(value), header_level=0 if row == 0 else None)
            for row, values in enumerate(matrix)
            for column, value in enumerate(values)
        )
        row_count = len(matrix)
        column_count = max((len(row) for row in matrix), default=0)
    else:
        data = _member(table, "data", table)
        raw_cells = _member(data, "table_cells", _member(data, "cells", None))
        if not isinstance(raw_cells, Sequence) or isinstance(raw_cells, (str, bytes)):
            raise ConfigurationError("Docling table output has no recognizable cells")
        cells = tuple(_normalize_cell(cell) for cell in raw_cells)
        row_count = _positive_int(_member(data, "num_rows", None)) or max(
            (cell.row + cell.rowspan for cell in cells), default=0
        )
        column_count = _positive_int(_member(data, "num_cols", None)) or max(
            (cell.column + cell.colspan for cell in cells), default=0
        )

    return TableArtifact(
        table_id=table_id,
        page_number=page_number,
        coordinates=_box(_member(table, "bbox", _member(table, "coordinates", None))),
        cells=cells,
        row_count=row_count,
        column_count=column_count,
        caption=_optional_text(_member(table, "caption", None)),
        confidence=_optional_float(_member(table, "confidence", None)),
        engine=DoclingTableExtractor.descriptor,
        metadata={"source": "docling"},
    )


def _matrix(table: object) -> list[list[object]] | None:
    for name in ("rows", "cells"):
        candidate = _member(table, name, None)
        matrix = _as_matrix(candidate)
        if matrix is not None:
            return matrix
    export = getattr(table, "export_to_dataframe", None)
    if not callable(export):
        return None
    frame = export()
    columns = getattr(frame, "columns", ())
    values = getattr(frame, "values", None)
    tolist = getattr(values, "tolist", None)
    if not callable(tolist):
        return None
    return [list(columns), *[list(row) for row in tolist()]]


def _as_matrix(candidate: object) -> list[list[object]] | None:
    if not isinstance(candidate, Sequence) or isinstance(candidate, (str, bytes)):
        return None
    if not candidate:
        return []
    if all(isinstance(row, Mapping) for row in candidate):
        keys = list(candidate[0])
        return [keys, *[[row.get(key) for key in keys] for row in candidate]]
    if all(isinstance(row, Sequence) and not isinstance(row, (str, bytes)) for row in candidate):
        return [list(row) for row in candidate]
    return None


def _normalize_cell(cell: object) -> TableCell:
    row = _nonnegative_int(_member(cell, "start_row_offset_idx", _member(cell, "row", 0)))
    column = _nonnegative_int(_member(cell, "start_col_offset_idx", _member(cell, "column", 0)))
    rowspan = _positive_int(_member(cell, "row_span", _member(cell, "rowspan", 1))) or 1
    colspan = _positive_int(_member(cell, "col_span", _member(cell, "colspan", 1))) or 1
    header = bool(_member(cell, "column_header", False)) or row == 0
    return TableCell(
        row=row,
        column=column,
        text=_text(_member(cell, "text", "")),
        coordinates=_box(_member(cell, "bbox", _member(cell, "coordinates", None))),
        rowspan=rowspan,
        colspan=colspan,
        header_level=0 if header else None,
        confidence=_optional_float(_member(cell, "confidence", None)),
    )


def _member(value: object, name: str, default: Any) -> Any:
    if isinstance(value, Mapping):
        return value.get(name, default)
    return getattr(value, name, default)


def _box(value: object) -> BoundingBox | None:
    if value is None:
        return None
    left = _optional_float(_member(value, "left", _member(value, "l", None)))
    top = _optional_float(_member(value, "top", _member(value, "t", None)))
    right = _optional_float(_member(value, "right", _member(value, "r", None)))
    bottom = _optional_float(_member(value, "bottom", _member(value, "b", None)))
    if None in (left, top, right, bottom):
        return None
    try:
        return BoundingBox(left, top, right, bottom)  # type: ignore[arg-type]
    except ValueError:
        return None


def _text(value: object) -> str:
    return "" if value is None else str(value)


def _optional_text(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _optional_float(value: object) -> float | None:
    try:
        parsed = float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None
    return parsed if math_is_finite(parsed) else None


def math_is_finite(value: float) -> bool:
    return value == value and value not in (float("inf"), float("-inf"))


def _nonnegative_int(value: object) -> int:
    try:
        return max(0, int(str(value)))
    except (TypeError, ValueError):
        return 0


def _positive_int(value: object) -> int | None:
    parsed = _nonnegative_int(value)
    return parsed if parsed > 0 else None


DoclingAdapter = DoclingTableExtractor

__all__ = ["ConverterFactory", "DoclingAdapter", "DoclingTableExtractor"]
