"""Deterministic conversion of internal table artifacts to public batches."""

from __future__ import annotations

from ingestion_graph.document_ai.models import TableArtifact
from ingestion_graph.models import TableBatch


def table_artifact_to_batches(
    artifact: TableArtifact, *, batch_rows: int = 500
) -> list[TableBatch]:
    if batch_rows < 1:
        raise ValueError("batch_rows must be positive")
    anchors = {(cell.row, cell.column): cell for cell in artifact.cells}
    coverage = {
        (row, column): cell
        for cell in artifact.cells
        for row in range(cell.row, cell.row + cell.rowspan)
        for column in range(cell.column, cell.column + cell.colspan)
    }
    header_cells = [cell for cell in artifact.cells if cell.header_level is not None]
    header_rows = max((cell.row + cell.rowspan for cell in header_cells), default=1)
    header_rows = min(header_rows, artifact.row_count)
    headers: list[str] = []
    for column in range(artifact.column_count):
        parts: list[str] = []
        seen_cells: set[tuple[int, int]] = set()
        for row in range(header_rows):
            cell = coverage.get((row, column))
            if cell is None or (cell.row, cell.column) in seen_cells:
                continue
            seen_cells.add((cell.row, cell.column))
            if cell.text.strip():
                parts.append(cell.text.strip())
        headers.append(" / ".join(parts) or f"column_{column + 1}")
    seen: dict[str, int] = {}
    for index, header in enumerate(headers):
        count = seen.get(header, 0) + 1
        seen[header] = count
        headers[index] = header if count == 1 else f"{header}_{count}"
    rows = []
    for row in range(header_rows, artifact.row_count):
        values = {}
        for column, header in enumerate(headers):
            cell = anchors.get((row, column))
            values[header] = (
                None if cell is None else (cell.value if cell.value is not None else cell.text)
            )
        rows.append(values)
    return [
        TableBatch(tuple(headers), tuple(rows[offset : offset + batch_rows]))
        for offset in range(0, len(rows), batch_rows)
    ]
