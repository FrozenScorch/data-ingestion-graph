"""Opt-in real PostgreSQL/pgvector verification for WireGuard CI hosts."""

import os
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

pytestmark = pytest.mark.skipif(
    not os.getenv("INTEGRATION_POSTGRES_HOST"),
    reason="set INTEGRATION_POSTGRES_HOST to run PostgreSQL integration tests",
)


@pytest.mark.asyncio
async def test_database_source_writer_and_vector_store_against_postgres():
    connection = {
        "host": os.environ["INTEGRATION_POSTGRES_HOST"],
        "port": int(os.getenv("INTEGRATION_POSTGRES_PORT", "5432")),
        "database": os.getenv("INTEGRATION_POSTGRES_DB", "ingestion_test"),
        "username": os.getenv("INTEGRATION_POSTGRES_USER", "ingestion_test"),
        "password": os.getenv("INTEGRATION_POSTGRES_PASSWORD", "ingestion_test"),
    }
    table = f"verify_rows_{uuid4().hex}"
    sdk_target = f"verify_sdk_rows_{uuid4().hex}"
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
        await admin.execute(f'DROP TABLE IF EXISTS "{sdk_target}"')
        await admin.execute(f'DROP TABLE IF EXISTS "{table}"')
        await admin.close()
