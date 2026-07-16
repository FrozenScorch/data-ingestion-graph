"""Deterministic conversion of internal table artifacts to public batches."""

from __future__ import annotations

from ingestion_graph.document_ai.models import TableArtifact
from ingestion_graph.models import TableBatch


def table_artifact_to_batches(
    artifact: TableArtifact, *, batch_rows: int = 500
) -> list[TableBatch]:
    if batch_rows < 1:
        raise ValueError("batch_rows must be positive")
    cells = {(cell.row, cell.column): cell for cell in artifact.cells}
    header_rows = (
        max((cell.header_level or 0) for cell in artifact.cells) + 1 if artifact.cells else 1
    )
    headers: list[str] = []
    for column in range(artifact.column_count):
        parts = [
            cells[(row, column)].text.strip()
            for row in range(min(header_rows, artifact.row_count))
            if (row, column) in cells and cells[(row, column)].text.strip()
        ]
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
            cell = cells.get((row, column))
            values[header] = (
                None if cell is None else (cell.value if cell.value is not None else cell.text)
            )
        rows.append(values)
    return [
        TableBatch(tuple(headers), tuple(rows[offset : offset + batch_rows]))
        for offset in range(0, len(rows), batch_rows)
    ]
