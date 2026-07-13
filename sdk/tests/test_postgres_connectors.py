from __future__ import annotations

from collections.abc import Mapping
from copy import deepcopy
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from uuid import UUID

import pytest

from ingestion_graph.destinations import PostgresDestination
from ingestion_graph.errors import ConfigurationError
from ingestion_graph.messages import RecordMessage, StateMessage
from ingestion_graph.models import Envelope, Operation, RecordPayload, Tombstone
from ingestion_graph.postgres import (
    PostgresConnection,
    decode_scalar,
    encode_scalar,
)
from ingestion_graph.secrets import EnvSecretProvider, SecretRef
from ingestion_graph.sources import PostgresSource


def source(**overrides: Any) -> PostgresSource:
    values = {
        "query": "SELECT id, cursor, value FROM items",
        "stream": "items",
        "primary_key": ("id",),
        "page_size": 2,
    }
    values.update(overrides)
    return PostgresSource(
        "db",
        5432,
        "data",
        "reader",
        SecretRef("PASSWORD"),
        secret_provider=EnvSecretProvider({"PASSWORD": "secret"}),
        **values,
    )


def destination(**overrides: Any) -> PostgresDestination:
    values = {"target": "public.items"}
    values.update(overrides)
    return PostgresDestination(
        "db",
        5432,
        "data",
        "writer",
        SecretRef("PASSWORD"),
        secret_provider=EnvSecretProvider({"PASSWORD": "secret"}),
        **values,
    )


def envelope(
    record_id: str,
    value: str = "A",
    *,
    cursor: str = "1",
    operation: Operation = Operation.UPSERT,
) -> Envelope:
    payload = (
        Tombstone()
        if operation is Operation.DELETE
        else RecordPayload({"id": int(record_id), "value": value})
    )
    return Envelope(
        id=record_id,
        source="source",
        stream="items",
        payload=payload,
        operation=operation,
        cursor=cursor,
        metadata={"key": {"id": int(record_id)}},
    )


class Transaction:
    def __init__(self, connection: FakeConnection) -> None:
        self.connection = connection
        self.snapshot: tuple[Any, ...] | None = None

    async def __aenter__(self) -> None:
        self.snapshot = (
            deepcopy(self.connection.ledger),
            deepcopy(self.connection.rows),
            self.connection.truncates,
            self.connection.mutations,
        )

    async def __aexit__(self, exc_type, exc, traceback) -> bool:
        if exc is not None and self.snapshot is not None:
            (
                self.connection.ledger,
                self.connection.rows,
                self.connection.truncates,
                self.connection.mutations,
            ) = self.snapshot
        return False


class FakeType:
    def __init__(self, name: str) -> None:
        self.name = name


class FakeAttribute:
    def __init__(self, name: str, type_name: str) -> None:
        self.name = name
        self.type = FakeType(type_name)


class FakeStatement:
    def get_attributes(self) -> list[FakeAttribute]:
        return [
            FakeAttribute("id", "int8"),
            FakeAttribute("cursor", "numeric"),
            FakeAttribute("value", "text"),
        ]


class FakeConnection:
    def __init__(self, pages: list[list[Mapping[str, Any]]] | None = None) -> None:
        self.pages = list(pages or [])
        self.fetch_calls: list[tuple[str, tuple[Any, ...]]] = []
        self.ledger: set[tuple[str, str, str, str, str]] = set()
        self.rows: list[tuple[Any, ...]] = []
        self.truncates = 0
        self.mutations = 0
        self.fail_target_insert = False
        self.closed = False

    def transaction(self, **kwargs: Any) -> Transaction:
        del kwargs
        return Transaction(self)

    async def close(self) -> None:
        self.closed = True

    async def prepare(self, query: str) -> FakeStatement:
        assert "LIMIT 0" in query
        return FakeStatement()

    async def fetch(self, query: str, *args: Any) -> list[Any]:
        self.fetch_calls.append((query, args))
        if "FROM pg_index" in query:
            return [{"columns": ["id"]}]
        return list(self.pages.pop(0))

    async def fetchval(self, query: str, *args: Any) -> Any:
        if "to_regclass" in query:
            return "public.items"
        if "event_hash" in query:
            return 1 if tuple(args) in self.ledger else None
        return None

    async def execute(self, query: str, *args: Any) -> str:
        normalized = " ".join(query.split())
        if "pg_advisory_xact_lock" in normalized or normalized.startswith("CREATE TABLE"):
            return "SELECT 1"
        if normalized.startswith("TRUNCATE TABLE"):
            self.rows.clear()
            self.truncates += 1
            return "TRUNCATE TABLE"
        if normalized.startswith("DELETE FROM") and "WHERE target_table=$1" in normalized:
            self.ledger = {item for item in self.ledger if item[0] != args[0]}
            return "DELETE 1"
        if normalized.startswith("SELECT pg_advisory"):
            return "SELECT 1"
        if normalized.startswith("INSERT INTO") and "event_hash" in normalized:
            self.ledger.add(tuple(args))
            return "INSERT 0 1"
        if normalized.startswith("INSERT INTO"):
            if self.fail_target_insert:
                raise RuntimeError("injected insert failure")
            self.rows.append(tuple(args))
            self.mutations += 1
            return "INSERT 0 1"
        if normalized.startswith("DELETE FROM"):
            self.mutations += 1
            return "DELETE 1"
        raise AssertionError(normalized)


@pytest.fixture
def connect(monkeypatch):
    connections: list[FakeConnection] = []

    async def fake_connect(self) -> FakeConnection:
        del self
        if not connections:
            raise AssertionError("test did not provide a fake connection")
        return connections.pop(0)

    monkeypatch.setattr(PostgresConnection, "connect", fake_connect)
    return connections


def test_postgres_config_is_strict_and_manifests_keep_secrets_as_refs() -> None:
    with pytest.raises(ConfigurationError, match="SELECT"):
        source(query="DELETE FROM items", primary_key=("id",))
    with pytest.raises(ConfigurationError, match="primary_key"):
        source(cursor_field="cursor", primary_key=())
    with pytest.raises(ConfigurationError, match="bounded preview"):
        source(primary_key=(), max_records=None)
    with pytest.raises(ConfigurationError, match="target"):
        destination(target='items"; DROP TABLE users;--')
    with pytest.raises(ConfigurationError, match="requires key_fields"):
        destination(mode="upsert")

    assert PostgresSource.manifest().config_schema["properties"]["password"]["format"] == (
        "secret-ref"
    )
    assert PostgresDestination.manifest().capabilities.deletes is True


def test_tagged_checkpoint_scalars_round_trip_exactly() -> None:
    values = [
        True,
        42,
        1.25,
        "value",
        Decimal("10.500"),
        datetime(2026, 1, 2, 3, 4, tzinfo=UTC),
        UUID("12345678-1234-5678-1234-567812345678"),
        b"bytes",
    ]

    assert [decode_scalar(encode_scalar(value)) for value in values] == values


@pytest.mark.asyncio
async def test_source_discovers_schema_and_rejects_missing_key_fields(connect) -> None:
    valid = FakeConnection()
    connect.append(valid)
    descriptor = (await source(cursor_field="cursor").discover())[0]
    assert descriptor.primary_key == ("id",)
    assert descriptor.cursor_field == ("cursor",)
    assert descriptor.json_schema["properties"]["id"]["type"] == ["integer", "null"]

    class MissingConnection(FakeConnection):
        async def prepare(self, query: str) -> FakeStatement:
            del query

            class MissingStatement(FakeStatement):
                def get_attributes(self) -> list[FakeAttribute]:
                    return [FakeAttribute("value", "text")]

            return MissingStatement()

    connect.append(MissingConnection())
    with pytest.raises(ConfigurationError, match="does not return"):
        await source(cursor_field="cursor").discover()


@pytest.mark.asyncio
async def test_snapshot_keyset_pages_checkpoint_and_reset_only_at_clean_eof(connect) -> None:
    connection = FakeConnection(
        [
            [
                {"id": 1, "cursor": 10, "value": "a"},
                {"id": 2, "cursor": 20, "value": "b"},
                {"id": 3, "cursor": 30, "value": "c"},
            ],
            [{"id": 3, "cursor": 30, "value": "c"}],
        ]
    )
    connect.append(connection)
    connector = source()

    messages = [message async for message in connector.read((await _descriptor(connector)), {})]
    records = [message for message in messages if isinstance(message, RecordMessage)]
    states = [message.state for message in messages if isinstance(message, StateMessage)]

    assert [record.envelope.payload.data["id"] for record in records] == [1, 2, 3]
    assert states[-1]["cycle"] == 1
    assert "after" not in states[-1]
    assert 'ORDER BY "id"' in connection.fetch_calls[0][0]
    assert "> ($1)" in connection.fetch_calls[1][0]


@pytest.mark.asyncio
async def test_incremental_keyset_uses_cursor_and_pk_and_resumes_exactly(connect) -> None:
    first = FakeConnection(
        [
            [
                {"id": 1, "cursor": Decimal("10"), "value": "a"},
                {"id": 2, "cursor": Decimal("10"), "value": "b"},
                {"id": 3, "cursor": Decimal("11"), "value": "c"},
            ],
            [{"id": 3, "cursor": Decimal("11"), "value": "c"}],
        ]
    )
    connect.append(first)
    connector = source(cursor_field="cursor")
    messages = [message async for message in connector.read(await _descriptor(connector), {})]
    checkpoint = [message.state for message in messages if isinstance(message, StateMessage)][0]
    assert decode_scalar(checkpoint["after"][0]) == Decimal("10")
    assert decode_scalar(checkpoint["after"][1]) == 2

    resumed = FakeConnection([[{"id": 3, "cursor": Decimal("11"), "value": "c"}]])
    connect.append(resumed)
    replay = [message async for message in connector.read(await _descriptor(connector), checkpoint)]
    assert [
        message.envelope.payload.data["id"]
        for message in replay
        if isinstance(message, RecordMessage)
    ] == [3]
    assert resumed.fetch_calls[0][1][0:2] == (Decimal("10"), 2)


@pytest.mark.asyncio
async def test_preview_is_bounded_one_batch_and_rejects_resume_state(connect) -> None:
    connector = source(primary_key=(), max_records=2)
    connect.append(FakeConnection([[{"id": 1}, {"id": 2}]]))
    messages = [message async for message in connector.read(await _descriptor(connector), {})]
    assert len([message for message in messages if isinstance(message, RecordMessage)]) == 2
    assert isinstance(messages[-1], StateMessage) and messages[-1].state == {}

    with pytest.raises(ConfigurationError, match="cannot resume"):
        _ = [
            message
            async for message in connector.read(await _descriptor(connector), {"unexpected": True})
        ]


@pytest.mark.asyncio
async def test_destination_replay_event_history_and_atomic_replace(connect) -> None:
    shared = FakeConnection()
    connect.extend([shared, shared, shared, shared, shared])
    connector = destination(mode="upsert", key_fields=("id",))
    first = envelope("1", "A", cursor="1")
    second = envelope("1", "B", cursor="2")
    reverted = envelope("1", "A", cursor="3")

    assert await connector.write([first]) == 1
    assert await connector.write([second, reverted]) == 2
    assert await connector.write([first]) == 0
    assert shared.mutations == 3

    deleted = envelope("1", operation=Operation.DELETE, cursor="4")
    assert await connector.write([deleted]) == 1
    assert await connector.write([deleted]) == 0


@pytest.mark.asyncio
async def test_replace_rolls_back_truncate_and_ledger_when_insert_fails(connect) -> None:
    shared = FakeConnection()
    shared.rows = [(99, "existing")]
    shared.ledger.add(("public.items", "s", "x", "old", "hash"))
    shared.fail_target_insert = True
    connect.append(shared)
    connector = destination()

    with pytest.raises(ConfigurationError, match="RuntimeError"):
        await connector.replace([envelope("1")])

    assert shared.rows == [(99, "existing")]
    assert ("public.items", "s", "x", "old", "hash") in shared.ledger
    assert shared.truncates == 0


@pytest.mark.asyncio
async def test_delete_and_rows_fail_closed_before_connect(connect) -> None:
    connector = destination(mode="upsert", key_fields=("id",))
    missing_key = Envelope(
        id="1",
        source="source",
        stream="items",
        payload=Tombstone(),
        operation=Operation.DELETE,
    )
    with pytest.raises(ConfigurationError, match="metadata.key"):
        await connector.write([missing_key])

    inconsistent = [
        envelope("1"),
        Envelope(
            id="2",
            source="source",
            stream="items",
            payload=RecordPayload({"id": 2, "other": "value"}),
        ),
    ]
    with pytest.raises(ConfigurationError, match="identical ordered columns"):
        await connector.write(inconsistent)
    assert connect == []


async def _descriptor(connector: PostgresSource):
    from ingestion_graph.connectors.base import StreamDescriptor

    return StreamDescriptor(
        connector.stream,
        primary_key=connector.primary_key,
        cursor_field=(connector.cursor_field,) if connector.cursor_field else (),
    )
