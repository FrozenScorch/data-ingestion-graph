"""Opt-in real PostgreSQL/pgvector verification for WireGuard CI hosts."""

import os
from datetime import UTC, date, datetime, time, timedelta
from typing import Any
from uuid import uuid4

import asyncpg
import pytest
from app.nodes.base import NodeContext
from app.nodes.database_source import DatabaseSourceNode
from app.nodes.db_writer import DatabaseWriterNode
from app.nodes.vector_store import VectorStoreNode
from ingestion_graph.destinations import PostgresDestination
from ingestion_graph.messages import RecordMessage, StateMessage
from ingestion_graph.models import Envelope, Operation, RecordPayload, Tombstone
from ingestion_graph.secrets import EnvSecretProvider, SecretRef
from ingestion_graph.sources import PostgresSource
from sqlalchemy.engine import make_url

pytestmark = pytest.mark.skipif(
    not (os.getenv("INTEGRATION_POSTGRES_HOST") or os.getenv("TEST_DATABASE_URL")),
    reason="set INTEGRATION_POSTGRES_HOST or TEST_DATABASE_URL to run PostgreSQL tests",
)


def _connection_settings() -> dict[str, Any]:
    if os.getenv("INTEGRATION_POSTGRES_HOST"):
        return {
            "host": os.environ["INTEGRATION_POSTGRES_HOST"],
            "port": int(os.getenv("INTEGRATION_POSTGRES_PORT", "5432")),
            "database": os.getenv("INTEGRATION_POSTGRES_DB", "ingestion_test"),
            "username": os.getenv("INTEGRATION_POSTGRES_USER", "ingestion_test"),
            "password": os.getenv("INTEGRATION_POSTGRES_PASSWORD", "ingestion_test"),
        }
    url = make_url(os.environ["TEST_DATABASE_URL"])
    return {
        "host": url.host or "localhost",
        "port": url.port or 5432,
        "database": url.database or "ingestion_test",
        "username": url.username or "ingestion",
        "password": url.password or "ingestion",
    }


@pytest.mark.asyncio
async def test_database_source_writer_and_vector_store_against_postgres():
    connection = _connection_settings()
    table = f"verify_rows_{uuid4().hex}"
    sdk_target = f"verify_sdk_rows_{uuid4().hex}"
    typed_source = f"verify_typed_source_{uuid4().hex}"
    typed_target = f"verify_typed_target_{uuid4().hex}"
    partial_target = f"verify_partial_target_{uuid4().hex}"
    included_target = f"verify_included_target_{uuid4().hex}"
    deferred_target = f"verify_deferred_target_{uuid4().hex}"
    vectors = f"verify_vectors_{uuid4().hex}"
    admin = await asyncpg.connect(
        host=connection["host"],
        port=connection["port"],
        database=connection["database"],
        user=connection["username"],
        password=connection["password"],
    )
    try:
        await admin.execute(f'CREATE TABLE "{table}" (id INTEGER PRIMARY KEY, name TEXT)')
        await admin.execute(
            f'CREATE TABLE "{sdk_target}" (id INTEGER PRIMARY KEY, name TEXT NOT NULL)'
        )
        typed_columns = (
            "id INTEGER PRIMARY KEY, occurred_at TIMESTAMPTZ NOT NULL, "
            "local_at TIMESTAMP NOT NULL, day DATE NOT NULL, at TIME NOT NULL, "
            "duration INTERVAL NOT NULL, body BYTEA NOT NULL"
        )
        await admin.execute(f'CREATE TABLE "{typed_source}" ({typed_columns})')
        await admin.execute(f'CREATE TABLE "{typed_target}" ({typed_columns})')
        await admin.execute(
            f'CREATE TABLE "{partial_target}" (id INTEGER, active BOOLEAN, name TEXT)'
        )
        await admin.execute(
            f'CREATE UNIQUE INDEX "{partial_target}_key" ON "{partial_target}" (id) WHERE active'
        )
        await admin.execute(f'CREATE TABLE "{included_target}" (id INTEGER, name TEXT)')
        await admin.execute(
            f'CREATE UNIQUE INDEX "{included_target}_key" ON "{included_target}" (id) '
            "INCLUDE (name)"
        )
        await admin.execute(
            f'CREATE TABLE "{deferred_target}" '
            "(id INTEGER, name TEXT, UNIQUE (id) DEFERRABLE INITIALLY IMMEDIATE)"
        )
        state = {"connections": {"integration": connection}}

        writer_result = await DatabaseWriterNode().execute(
            NodeContext(
                run_id="integration",
                node_id="writer",
                config={"connection_id": "integration", "table_name": table},
                input_data={"rows": [{"id": 1, "name": "wirecard"}]},
                state=state,
            )
        )
        assert writer_result.success is True
        assert writer_result.items_processed == 1

        upsert_result = await DatabaseWriterNode().execute(
            NodeContext(
                run_id="integration",
                node_id="writer-upsert",
                config={
                    "connection_id": "integration",
                    "table_name": table,
                    "mode": "upsert",
                    "upsert_key": "id",
                },
                input_data={"rows": [{"id": 1, "name": "wirecard-updated"}]},
                state=state,
            )
        )
        assert upsert_result.success is True
        assert upsert_result.items_processed == 1

        source_result = await DatabaseSourceNode().execute(
            NodeContext(
                run_id="integration",
                node_id="source",
                config={
                    "connection_id": "integration",
                    "query": f'SELECT id, name FROM "{table}"',
                },
                state=state,
            )
        )
        assert source_result.success is True
        assert source_result.output_data["rows"] == [{"id": 1, "name": "wirecard-updated"}]

        secrets = EnvSecretProvider({"POSTGRES_PASSWORD": connection["password"]})
        source = PostgresSource(
            connection["host"],
            connection["port"],
            connection["database"],
            connection["username"],
            SecretRef("POSTGRES_PASSWORD"),
            query=f'SELECT id, name FROM "{table}"',
            stream="wirecard_rows",
            primary_key=("id",),
            cursor_field="id",
            secret_provider=secrets,
        )
        descriptor = (await source.discover())[0]
        messages = [message async for message in source.read(descriptor, {})]
        sdk_rows = [message.envelope for message in messages if isinstance(message, RecordMessage)]
        assert [item.payload.data for item in sdk_rows] == [{"id": 1, "name": "wirecard-updated"}]
        assert any(isinstance(message, StateMessage) for message in messages)

        destination = PostgresDestination(
            connection["host"],
            connection["port"],
            connection["database"],
            connection["username"],
            SecretRef("POSTGRES_PASSWORD"),
            target=sdk_target,
            mode="upsert",
            key_fields=("id",),
            secret_provider=secrets,
        )
        assert (await destination.check()).ok
        assert await destination.write(sdk_rows) == 1
        assert await destination.write(sdk_rows) == 0
        updated = Envelope(
            id=sdk_rows[0].id,
            source=sdk_rows[0].source,
            stream=sdk_rows[0].stream,
            payload=RecordPayload({"id": 1, "name": "sdk-updated"}),
            operation=Operation.UPSERT,
            cursor="2",
            metadata={"key": {"id": 1}},
        )
        assert await destination.write([updated]) == 1
        assert await admin.fetchval(f'SELECT name FROM "{sdk_target}" WHERE id=1') == (
            "sdk-updated"
        )
        deleted = Envelope(
            id=updated.id,
            source=updated.source,
            stream=updated.stream,
            payload=Tombstone(),
            operation=Operation.DELETE,
            cursor="3",
            metadata={"key": {"id": 1}},
        )
        assert await destination.write([deleted]) == 1
        assert await admin.fetchval(f'SELECT count(*) FROM "{sdk_target}"') == 0

        seed = Envelope(
            id="seed",
            source="integration",
            stream="replace",
            payload=RecordPayload({"id": 9, "name": "seed"}),
            cursor="1",
        )
        assert await destination.replace([seed]) == 1
        invalid = Envelope(
            id="invalid",
            source="integration",
            stream="replace",
            payload=RecordPayload({"id": 10, "name": None}),
            cursor="2",
        )
        with pytest.raises(Exception, match="PostgreSQL destination replace failed"):
            await destination.replace([invalid])
        assert await admin.fetchval(f'SELECT name FROM "{sdk_target}" WHERE id=9') == "seed"
        assert await destination.replace([]) == 0
        assert await admin.fetchval(f'SELECT count(*) FROM "{sdk_target}"') == 0

        occurred_at = datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC)
        local_at = datetime(2026, 1, 2, 3, 4, 5)
        day = date(2026, 1, 2)
        at = time(3, 4, 5)
        duration = timedelta(days=2, seconds=3, microseconds=4)
        body = b"wirecard-bytes"
        await admin.execute(
            f'INSERT INTO "{typed_source}" VALUES ($1, $2, $3, $4, $5, $6, $7)',
            1,
            occurred_at,
            local_at,
            day,
            at,
            duration,
            body,
        )
        typed_reader = PostgresSource(
            connection["host"],
            connection["port"],
            connection["database"],
            connection["username"],
            SecretRef("POSTGRES_PASSWORD"),
            query=f'SELECT * FROM "{typed_source}"',
            stream="typed_rows",
            primary_key=("id",),
            secret_provider=secrets,
        )
        typed_messages = [
            message async for message in typed_reader.read((await typed_reader.discover())[0], {})
        ]
        typed_records = [
            message.envelope for message in typed_messages if isinstance(message, RecordMessage)
        ]
        typed_writer = PostgresDestination(
            connection["host"],
            connection["port"],
            connection["database"],
            connection["username"],
            SecretRef("POSTGRES_PASSWORD"),
            target=typed_target,
            mode="upsert",
            key_fields=("id",),
            secret_provider=secrets,
        )
        assert await typed_writer.write(typed_records) == 1
        typed_row = await admin.fetchrow(f'SELECT * FROM "{typed_target}" WHERE id=1')
        assert dict(typed_row) == {
            "id": 1,
            "occurred_at": occurred_at,
            "local_at": local_at,
            "day": day,
            "at": at,
            "duration": duration,
            "body": body,
        }

        snapshot_reader = PostgresSource(
            connection["host"],
            connection["port"],
            connection["database"],
            connection["username"],
            SecretRef("POSTGRES_PASSWORD"),
            query=f'SELECT id, name FROM "{table}"',
            stream="snapshot_reversion",
            primary_key=("id",),
            secret_provider=secrets,
        )
        snapshot_state = {}
        snapshot_records = []
        for name in ("A", "B", "A"):
            await admin.execute(f'UPDATE "{table}" SET name=$1 WHERE id=1', name)
            snapshot_messages = [
                message
                async for message in snapshot_reader.read(
                    (await snapshot_reader.discover())[0], snapshot_state
                )
            ]
            record = next(
                message.envelope
                for message in snapshot_messages
                if isinstance(message, RecordMessage)
            )
            snapshot_records.append(record)
            snapshot_state = next(
                message.state
                for message in reversed(snapshot_messages)
                if isinstance(message, StateMessage)
            )
            assert await destination.write([record]) == 1
        assert await admin.fetchval(f'SELECT name FROM "{sdk_target}" WHERE id=1') == "A"
        qualified_alias = PostgresDestination(
            connection["host"],
            connection["port"],
            connection["database"],
            connection["username"],
            SecretRef("POSTGRES_PASSWORD"),
            target=f"public.{sdk_target}",
            mode="upsert",
            key_fields=("id",),
            secret_provider=secrets,
        )
        assert await qualified_alias.write([snapshot_records[-1]]) == 0

        async def upsert_check(target: str) -> bool:
            connector = PostgresDestination(
                connection["host"],
                connection["port"],
                connection["database"],
                connection["username"],
                SecretRef("POSTGRES_PASSWORD"),
                target=target,
                mode="upsert",
                key_fields=("id",),
                secret_provider=secrets,
            )
            return (await connector.check()).ok

        assert await upsert_check(partial_target) is False
        assert await upsert_check(included_target) is True
        assert await upsert_check(deferred_target) is False

        if os.getenv("INTEGRATION_POSTGRES_HOST"):
            vector_result = await VectorStoreNode().execute(
                NodeContext(
                    run_id="integration",
                    node_id="vectors",
                    config={
                        "connection_id": "integration",
                        "table_name": vectors,
                        "embedding_dim": 3,
                        "create_index": True,
                    },
                    input_data={
                        "embeddings": [
                            {
                                "content": "verified on wirecard",
                                "metadata": {"environment": "integration"},
                                "embedding": [0.1, 0.2, 0.3],
                            }
                        ]
                    },
                    state=state,
                )
            )
            assert vector_result.success is True
            assert vector_result.output_data["stored_count"] == 1
            assert vector_result.output_data["index_created"] is True
    finally:
        await admin.execute(f'DROP TABLE IF EXISTS "{vectors}"')
        await admin.execute(f'DROP TABLE IF EXISTS "{deferred_target}"')
        await admin.execute(f'DROP TABLE IF EXISTS "{included_target}"')
        await admin.execute(f'DROP TABLE IF EXISTS "{partial_target}"')
        await admin.execute(f'DROP TABLE IF EXISTS "{typed_target}"')
        await admin.execute(f'DROP TABLE IF EXISTS "{typed_source}"')
        await admin.execute(f'DROP TABLE IF EXISTS "{sdk_target}"')
        await admin.execute(f'DROP TABLE IF EXISTS "{table}"')
        await admin.close()
