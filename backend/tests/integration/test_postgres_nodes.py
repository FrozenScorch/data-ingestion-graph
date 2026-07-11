"""Opt-in real PostgreSQL/pgvector verification for WireGuard CI hosts."""

import os
from uuid import uuid4

import asyncpg
import pytest

from app.nodes.base import NodeContext
from app.nodes.database_source import DatabaseSourceNode
from app.nodes.db_writer import DatabaseWriterNode
from app.nodes.vector_store import VectorStoreNode


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
        await admin.execute(f'DROP TABLE IF EXISTS "{table}"')
        await admin.close()
