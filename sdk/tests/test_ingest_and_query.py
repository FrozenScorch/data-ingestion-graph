from __future__ import annotations

import json
import sys
from datetime import UTC, datetime

import pytest

from ingestion_graph import (
    DocumentElement,
    Envelope,
    MemoryStateStore,
    Operation,
    Pipeline,
    Query,
    QueryRequest,
    RecordPayload,
    Tombstone,
)
from ingestion_graph.cli import main
from ingestion_graph.destinations import SQLiteCollection
from ingestion_graph.errors import ConfigurationError
from ingestion_graph.sources import JsonlSource


def _record(
    record_id: str,
    text: str,
    *,
    source: str = "example",
    stream: str = "items",
) -> Envelope:
    return Envelope(
        id=record_id,
        source=source,
        stream=stream,
        payload=RecordPayload({"text": text}),
        observed_at=datetime(2026, 1, 1, tzinfo=UTC),
    )


@pytest.mark.asyncio
async def test_sqlite_collection_current_view_upsert_replay_and_delete(tmp_path):
    collection = SQLiteCollection(tmp_path / "query.db")
    original = _record("one", "old searchable words")
    updated = _record("one", "new searchable words")

    assert await collection.write([original]) == 1
    assert await collection.write([original]) == 0
    assert await collection.write([updated]) == 1
    assert (await collection.query(Query("old"))).total == 0
    result = await collection.query(Query("new"))
    assert result.total == 1
    assert len(result) == 1
    assert result[0].envelope.payload == RecordPayload({"text": "new searchable words"})
    assert await collection.get("example", "items", "one") == updated

    deletion = Envelope(
        id="one",
        source="example",
        stream="items",
        payload=Tombstone("gone"),
        operation=Operation.DELETE,
    )
    assert await collection.write([deletion]) == 1
    assert await collection.write([deletion]) == 0
    assert await collection.get("example", "items", "one") is None
    assert (await collection.query(QueryRequest())).total == 0


@pytest.mark.asyncio
async def test_sqlite_collection_key_is_source_stream_and_id(tmp_path):
    collection = SQLiteCollection(tmp_path / "query.db")
    records = [
        _record("same", "shared token alpha", source="one", stream="a"),
        _record("same", "shared token beta", source="one", stream="b"),
        _record("same", "shared token gamma", source="two", stream="a"),
    ]
    assert await collection.write(records) == 3
    assert (await collection.query(QueryRequest("shared"))).total == 3
    filtered = await collection.query(QueryRequest("shared", source="one", stream="b"))
    assert filtered.total == 1
    assert filtered[0].envelope.payload == RecordPayload({"text": "shared token beta"})


@pytest.mark.asyncio
async def test_sqlite_collection_lists_current_records_without_query_text(tmp_path):
    collection = SQLiteCollection(tmp_path / "query.db")
    await collection.write([_record("one", "first"), _record("two", "second")])
    result = await collection.query(QueryRequest(limit=1))
    assert result.total == 2
    assert len(result) == 1
    assert result[0].score == 0.0


@pytest.mark.asyncio
async def test_sqlite_collection_search_treats_fts_operators_as_plain_input(tmp_path):
    collection = SQLiteCollection(tmp_path / "query.db")
    await collection.write([_record("one", 'literal near words with "quotes"')])
    result = await collection.query(QueryRequest('near("words")'))
    assert result.total == 1


@pytest.mark.asyncio
async def test_jsonl_source_resumes_when_file_is_appended(tmp_path):
    input_path = tmp_path / "people.jsonl"
    input_path.write_text(
        '{"id":"1","name":"Ada Lovelace"}\n{"id":"2","name":"Grace Hopper"}\n',
        encoding="utf-8",
    )
    state = MemoryStateStore()
    collection_path = tmp_path / "query.db"
    first = await Pipeline(
        "people",
        JsonlSource(input_path, batch_size=1),
        SQLiteCollection(collection_path),
        state_store=state,
    ).run()
    assert first.records_written == 2

    with input_path.open("a", encoding="utf-8") as handle:
        handle.write('{"id":"3","name":"Katherine Johnson"}\n')
    second = await Pipeline(
        "people",
        JsonlSource(input_path, batch_size=1),
        SQLiteCollection(collection_path),
        state_store=state,
    ).run()
    assert second.records_written == 1
    result = await SQLiteCollection(collection_path).query(Query("Katherine"))
    assert result.total == 1
    assert result[0].envelope.metadata["native_id"] == "3"


@pytest.mark.asyncio
async def test_jsonl_source_detects_changes_before_checkpoint(tmp_path):
    input_path = tmp_path / "items.jsonl"
    input_path.write_text('{"id":"1","value":"before"}\n', encoding="utf-8")
    state = MemoryStateStore()
    await Pipeline(
        "items",
        JsonlSource(input_path),
        SQLiteCollection(tmp_path / "query.db"),
        state_store=state,
    ).run()
    input_path.write_text('{"id":"1","value":"after!"}\n', encoding="utf-8")
    with pytest.raises(ConfigurationError, match="saved checkpoint changed"):
        await Pipeline(
            "items",
            JsonlSource(input_path),
            SQLiteCollection(tmp_path / "query.db"),
            state_store=state,
        ).run()


@pytest.mark.asyncio
async def test_jsonl_delete_removes_current_record(tmp_path):
    input_path = tmp_path / "events.jsonl"
    input_path.write_text(
        '{"id":"1","text":"temporary"}\n{"id":"1","_operation":"delete"}\n',
        encoding="utf-8",
    )
    collection = SQLiteCollection(tmp_path / "query.db")
    result = await Pipeline(
        "events",
        JsonlSource(input_path, batch_size=1),
        collection,
        state_store=MemoryStateStore(),
    ).run()
    assert result.records_written == 2
    assert (await collection.query(QueryRequest())).total == 0


def test_envelope_from_dict_round_trip():
    envelope = Envelope(
        id="document",
        source="files",
        stream="manuals",
        payload=DocumentElement("How to query", page_number=3),
        event_time=datetime(2026, 1, 2, tzinfo=UTC),
        metadata={"filename": "guide.pdf"},
    )
    assert Envelope.from_dict(envelope.to_dict()) == envelope


def test_cli_ingests_queries_and_inspects_jsonl(tmp_path, monkeypatch, capsys):
    input_path = tmp_path / "items.jsonl"
    collection = tmp_path / "query.db"
    state = tmp_path / "state.db"
    input_path.write_text('{"id":"1","title":"portable ingestion"}\n', encoding="utf-8")
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "ingestion-graph",
            "ingest-jsonl",
            str(input_path),
            "--collection",
            str(collection),
            "--state",
            str(state),
        ],
    )
    assert main() == 0
    ingest_output = json.loads(capsys.readouterr().out)
    assert ingest_output["records_written"] == 1

    monkeypatch.setattr(
        sys,
        "argv",
        ["ingestion-graph", "query", "portable", "--collection", str(collection)],
    )
    assert main() == 0
    search_output = json.loads(capsys.readouterr().out)
    assert search_output["total"] == 1
    assert search_output["hits"][0]["envelope"]["payload"]["data"]["title"] == (
        "portable ingestion"
    )

    monkeypatch.setattr(
        sys,
        "argv",
        ["ingestion-graph", "query", "--collection", str(collection)],
    )
    assert main() == 0
    assert json.loads(capsys.readouterr().out)["total"] == 1
