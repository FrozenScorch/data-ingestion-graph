from __future__ import annotations

import asyncio

from ingestion_graph.document_ai import (
    BoundingBox,
    ComponentDescriptor,
    MemoryExtractionCache,
    SQLiteExtractionCache,
    TableArtifact,
    TableCell,
    canonical_fingerprint,
    evaluate_text_quality,
    table_artifact_to_batches,
)


def test_quality_and_fingerprint_are_deterministic() -> None:
    assert evaluate_text_quality("A useful document " * 20).score > 0.65
    assert evaluate_text_quality("").reason == "empty"
    assert canonical_fingerprint({"b": 2, "a": 1}) == canonical_fingerprint({"a": 1, "b": 2})
    assert ComponentDescriptor("engine", "1").to_dict()["deterministic"] is True


def test_table_artifact_preserves_hierarchical_headers_and_batches() -> None:
    artifact = TableArtifact(
        "table-1",
        2,
        BoundingBox(0, 0, 1, 1),
        (
            TableCell(0, 0, "Revenue", header_level=0),
            TableCell(1, 0, "Q1", header_level=1),
            TableCell(1, 1, "Q2", header_level=1),
            TableCell(2, 0, "10", value=10),
            TableCell(2, 1, "20", value=20),
        ),
        3,
        2,
    )
    batches = table_artifact_to_batches(artifact, batch_rows=1)
    assert batches[0].columns == ("Revenue / Q1", "Q2")
    assert batches[0].rows == ({"Revenue / Q1": 10, "Q2": 20},)


def test_table_artifact_preserves_merged_headers_and_anchor_cells() -> None:
    artifact = TableArtifact(
        "merged",
        1,
        BoundingBox(0, 0, 1, 1),
        (
            TableCell(0, 0, "Group", colspan=2, header_level=0),
            TableCell(1, 0, "A", header_level=1),
            TableCell(1, 1, "B", header_level=1),
            TableCell(2, 0, "merged value", rowspan=2),
            TableCell(2, 1, "first"),
            TableCell(3, 1, "second"),
        ),
        4,
        2,
    )

    batch = table_artifact_to_batches(artifact)[0]

    assert batch.columns == ("Group / A", "Group / B")
    assert batch.rows == (
        {"Group / A": "merged value", "Group / B": "first"},
        {"Group / A": None, "Group / B": "second"},
    )


def test_table_artifact_rejects_overlapping_spans() -> None:
    import pytest

    with pytest.raises(ValueError, match="overlap"):
        TableArtifact(
            "overlap",
            1,
            None,
            (TableCell(0, 0, "wide", colspan=2), TableCell(0, 1, "collision")),
            1,
            2,
        )


def test_memory_and_sqlite_cache_round_trip(tmp_path) -> None:
    async def run() -> None:
        memory = MemoryExtractionCache()
        await memory.put("key", b"value")
        assert await memory.get("key") == b"value"
        cache = SQLiteExtractionCache(tmp_path / "cache.db")
        await cache.put("key", b"value")
        assert await cache.get("key") == b"value"
        await cache.delete("key")
        assert await cache.get("key") is None

    asyncio.run(run())
