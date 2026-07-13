"""Live PostgreSQL coverage for the Studio SDK state bridge."""

from __future__ import annotations

import os
from uuid import uuid4

import pytest
from app.services.sdk_source_state_service import StudioSDKSourceStateStore
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

TEST_DATABASE_URL = os.getenv("TEST_DATABASE_URL")
pytestmark = pytest.mark.skipif(
    not TEST_DATABASE_URL,
    reason="TEST_DATABASE_URL is required for PostgreSQL SDK-state integration tests",
)


@pytest.mark.asyncio
async def test_postgres_state_bridge_round_trips_and_isolates_owners():
    schema = f"sdk_state_{uuid4().hex}"
    engine = create_async_engine(
        TEST_DATABASE_URL,
        connect_args={"server_settings": {"search_path": schema}},
    )
    admin_engine = create_async_engine(TEST_DATABASE_URL)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    owner_a, owner_b, graph_id = uuid4(), uuid4(), uuid4()

    try:
        async with admin_engine.begin() as connection:
            await connection.execute(text(f'CREATE SCHEMA "{schema}"'))
            await connection.execute(text(f'CREATE TABLE "{schema}".users (id UUID PRIMARY KEY)'))
            await connection.execute(
                text(
                    f"""
                    CREATE TABLE "{schema}".graphs (
                        id UUID PRIMARY KEY,
                        owner_id UUID NOT NULL REFERENCES "{schema}".users(id)
                    )
                    """
                )
            )
            await connection.execute(
                text(
                    f"""
                    CREATE TABLE "{schema}".sdk_source_states (
                        id UUID PRIMARY KEY,
                        owner_id UUID NOT NULL REFERENCES "{schema}".users(id) ON DELETE CASCADE,
                        graph_id UUID NOT NULL REFERENCES "{schema}".graphs(id) ON DELETE CASCADE,
                        node_id VARCHAR(255) NOT NULL,
                        source VARCHAR(255) NOT NULL,
                        stream VARCHAR(255) NOT NULL,
                        state_data JSONB NOT NULL,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                        updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                        CONSTRAINT uq_sdk_source_state_scope UNIQUE
                            (owner_id, graph_id, node_id, source, stream)
                    )
                    """
                )
            )
            await connection.execute(
                text(f'INSERT INTO "{schema}".users (id) VALUES (:owner_a), (:owner_b)'),
                {"owner_a": owner_a, "owner_b": owner_b},
            )
            await connection.execute(
                text(f'INSERT INTO "{schema}".graphs (id, owner_id) VALUES (:graph_id, :owner_a)'),
                {"graph_id": graph_id, "owner_a": owner_a},
            )

        async with sessions() as session:
            first = StudioSDKSourceStateStore(
                session,
                owner_id=owner_a,
                graph_id=graph_id,
                node_id="documents",
            )
            await first.acquire_lock()
            await first.save(first.pipeline_key, "local_documents", "upload-1", {"cursor": 1})
            await session.commit()

        async with sessions() as session:
            first = StudioSDKSourceStateStore(
                session,
                owner_id=owner_a,
                graph_id=graph_id,
                node_id="documents",
            )
            second = StudioSDKSourceStateStore(
                session,
                owner_id=owner_b,
                graph_id=graph_id,
                node_id="documents",
            )
            assert await first.load(first.pipeline_key, "local_documents", "upload-1") == {
                "cursor": 1
            }
            assert await first.list_streams(first.pipeline_key, "local_documents") == ["upload-1"]
            assert await second.load(second.pipeline_key, "local_documents", "upload-1") == {}
            await first.save(first.pipeline_key, "local_documents", "upload-1", {"cursor": 2})
            await session.commit()

        async with sessions() as session:
            first = StudioSDKSourceStateStore(
                session,
                owner_id=owner_a,
                graph_id=graph_id,
                node_id="documents",
            )
            assert await first.load(first.pipeline_key, "local_documents", "upload-1") == {
                "cursor": 2
            }
            await first.delete(first.pipeline_key, "local_documents", "upload-1")
            await session.commit()
            assert await first.load(first.pipeline_key, "local_documents", "upload-1") == {}
    finally:
        await engine.dispose()
        async with admin_engine.begin() as connection:
            await connection.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        await admin_engine.dispose()
