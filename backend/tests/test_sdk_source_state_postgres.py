"""Live PostgreSQL coverage for whole-run Studio SDK state acknowledgement."""

from __future__ import annotations

import os
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import pytest
from app.models.execution import Run
from app.models.sdk_source_state import SDKSourceState, SDKSourceStateCandidate
from app.services.sdk_source_state_service import (
    StaleSDKSourceStateCandidateError,
    StudioSDKSourceStateStore,
    complete_run_with_source_state_promotion,
)
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

TEST_DATABASE_URL = os.getenv("TEST_DATABASE_URL")
pytestmark = pytest.mark.skipif(
    not TEST_DATABASE_URL,
    reason="TEST_DATABASE_URL is required for PostgreSQL SDK-state integration tests",
)


async def _create_schema(admin_engine, schema: str) -> None:
    async with admin_engine.begin() as connection:
        await connection.execute(text(f'CREATE SCHEMA "{schema}"'))
        statements = [
            f'CREATE TABLE "{schema}".users (id UUID PRIMARY KEY)',
            f"""
                CREATE TABLE "{schema}".graphs (
                    id UUID PRIMARY KEY,
                    owner_id UUID NOT NULL REFERENCES "{schema}".users(id)
                )
            """,
            f"""
                CREATE TABLE "{schema}".runs (
                    id UUID PRIMARY KEY,
                    graph_id UUID NOT NULL REFERENCES "{schema}".graphs(id),
                    graph_version_id UUID NULL,
                    trigger_type VARCHAR(50) NOT NULL DEFAULT 'manual',
                    triggered_by UUID NULL REFERENCES "{schema}".users(id),
                    status VARCHAR(50) NOT NULL,
                    error_message TEXT NULL,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
                )
            """,
            f"""
                CREATE TABLE "{schema}".run_jobs (
                    id UUID PRIMARY KEY,
                    run_id UUID NOT NULL UNIQUE REFERENCES "{schema}".runs(id) ON DELETE CASCADE,
                    job_type VARCHAR(50) NOT NULL DEFAULT 'full',
                    status VARCHAR(50) NOT NULL,
                    available_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    lease_owner VARCHAR(255),
                    lease_expires_at TIMESTAMPTZ,
                    heartbeat_at TIMESTAMPTZ,
                    attempt_count INTEGER NOT NULL DEFAULT 1,
                    last_error TEXT,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
                )
            """,
            f"""
                CREATE TABLE "{schema}".sdk_source_states (
                    id UUID PRIMARY KEY,
                    owner_id UUID NOT NULL REFERENCES "{schema}".users(id) ON DELETE CASCADE,
                    graph_id UUID NOT NULL REFERENCES "{schema}".graphs(id) ON DELETE CASCADE,
                    node_id VARCHAR(255) NOT NULL,
                    source VARCHAR(255) NOT NULL,
                    stream VARCHAR(255) NOT NULL,
                    state_data JSONB NOT NULL,
                    revision INTEGER NOT NULL DEFAULT 1,
                    is_deleted BOOLEAN NOT NULL DEFAULT false,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    CONSTRAINT uq_sdk_source_state_scope UNIQUE
                        (owner_id, graph_id, node_id, source, stream)
                )
            """,
            f"""
                CREATE TABLE "{schema}".sdk_source_state_candidates (
                    id UUID PRIMARY KEY,
                    run_id UUID NOT NULL REFERENCES "{schema}".runs(id) ON DELETE CASCADE,
                    owner_id UUID NOT NULL REFERENCES "{schema}".users(id) ON DELETE CASCADE,
                    graph_id UUID NOT NULL REFERENCES "{schema}".graphs(id) ON DELETE CASCADE,
                    node_id VARCHAR(255) NOT NULL,
                    source VARCHAR(255) NOT NULL,
                    stream VARCHAR(255) NOT NULL,
                    operation VARCHAR(20) NOT NULL,
                    state_data JSONB,
                    base_state_data JSONB,
                    base_revision INTEGER NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    CONSTRAINT uq_sdk_source_state_candidate_scope UNIQUE
                        (run_id, owner_id, graph_id, node_id, source, stream)
                )
            """,
            f"""
                CREATE INDEX ix_sdk_source_state_candidates_run_id
                    ON "{schema}".sdk_source_state_candidates (run_id)
            """,
        ]
        for statement in statements:
            await connection.execute(text(statement))


async def _seed_run(
    session: AsyncSession,
    *,
    owner_id: UUID,
    graph_id: UUID,
    run_id: UUID,
    job_id: UUID,
    worker: str,
) -> None:
    values = {
        "owner_id": owner_id,
        "graph_id": graph_id,
        "run_id": run_id,
        "job_id": job_id,
        "worker": worker,
        "lease_expires_at": datetime.now(UTC) + timedelta(minutes=5),
    }
    await session.execute(
        text("INSERT INTO users (id) VALUES (:owner_id) ON CONFLICT DO NOTHING"),
        values,
    )
    await session.execute(
        text(
            """
            INSERT INTO graphs (id, owner_id) VALUES (:graph_id, :owner_id)
                ON CONFLICT DO NOTHING
            """
        ),
        values,
    )
    await session.execute(
        text("INSERT INTO runs (id, graph_id, status) VALUES (:run_id, :graph_id, 'running')"),
        values,
    )
    await session.execute(
        text(
            """
            INSERT INTO run_jobs (
                id, run_id, status, lease_owner, lease_expires_at
            ) VALUES (
                :job_id, :run_id, 'leased', :worker, :lease_expires_at
            )
            """
        ),
        values,
    )
    await session.commit()


def _store(
    session: AsyncSession,
    *,
    run_id: UUID,
    owner_id: UUID,
    graph_id: UUID,
) -> StudioSDKSourceStateStore:
    return StudioSDKSourceStateStore(
        session,
        run_id=run_id,
        owner_id=owner_id,
        graph_id=graph_id,
        node_id="documents",
    )


@pytest.mark.asyncio
async def test_downstream_failure_keeps_state_uncommitted_until_same_run_retry_succeeds():
    schema = f"sdk_ack_{uuid4().hex}"
    engine = create_async_engine(
        TEST_DATABASE_URL,
        connect_args={"server_settings": {"search_path": schema}},
    )
    admin_engine = create_async_engine(TEST_DATABASE_URL)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    owner_id, graph_id, run_id, job_id = uuid4(), uuid4(), uuid4(), uuid4()
    worker = "worker-1"

    try:
        await _create_schema(admin_engine, schema)
        async with sessions() as session:
            await _seed_run(
                session,
                owner_id=owner_id,
                graph_id=graph_id,
                run_id=run_id,
                job_id=job_id,
                worker=worker,
            )
            store = _store(
                session,
                run_id=run_id,
                owner_id=owner_id,
                graph_id=graph_id,
            )
            await store.acquire_lock()
            await store.save(store.pipeline_key, "local_documents", "upload-1", {"cursor": 1})
            await session.commit()  # source POST_EXEC candidate/output boundary

        async with sessions() as session:
            store = _store(
                session,
                run_id=run_id,
                owner_id=owner_id,
                graph_id=graph_id,
            )
            assert await store.load(store.pipeline_key, "local_documents", "upload-1") == {}
            assert await session.scalar(select(func.count(SDKSourceState.id))) == 0
            assert await session.scalar(select(func.count(SDKSourceStateCandidate.id))) == 1
            run = await session.get(Run, run_id)
            assert run is not None
            run.status = "failed"  # a downstream destination failed
            await session.commit()

        async with sessions() as session:
            # Failed-node retry restores source output, so it reuses this durable candidate.
            run = await session.get(Run, run_id)
            assert run is not None
            run.status = "running"
            await session.commit()
            assert await complete_run_with_source_state_promotion(
                session,
                run_id,
                job_id=job_id,
                lease_owner=worker,
            )

        async with sessions() as session:
            store = _store(
                session,
                run_id=run_id,
                owner_id=owner_id,
                graph_id=graph_id,
            )
            assert await store.load(store.pipeline_key, "local_documents", "upload-1") == {
                "cursor": 1
            }
            assert await session.scalar(select(func.count(SDKSourceStateCandidate.id))) == 0
            run = await session.get(Run, run_id)
            assert run is not None and run.status == "completed"
    finally:
        await engine.dispose()
        async with admin_engine.begin() as connection:
            await connection.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        await admin_engine.dispose()


@pytest.mark.asyncio
async def test_concurrent_stale_candidate_cannot_regress_newer_committed_state():
    schema = f"sdk_stale_{uuid4().hex}"
    engine = create_async_engine(
        TEST_DATABASE_URL,
        connect_args={"server_settings": {"search_path": schema}},
    )
    admin_engine = create_async_engine(TEST_DATABASE_URL)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    owner_id, graph_id = uuid4(), uuid4()
    older_run, older_job, newer_run, newer_job = uuid4(), uuid4(), uuid4(), uuid4()

    try:
        await _create_schema(admin_engine, schema)
        async with sessions() as session:
            await _seed_run(
                session,
                owner_id=owner_id,
                graph_id=graph_id,
                run_id=older_run,
                job_id=older_job,
                worker="older-worker",
            )
            await _seed_run(
                session,
                owner_id=owner_id,
                graph_id=graph_id,
                run_id=newer_run,
                job_id=newer_job,
                worker="newer-worker",
            )
            # Omitting the new columns exercises migration defaults for legacy rows.
            await session.execute(
                text(
                    """
                    INSERT INTO sdk_source_states (
                        id, owner_id, graph_id, node_id, source, stream, state_data
                    ) VALUES (
                        :id, :owner_id, :graph_id, 'documents',
                        'local_documents', 'upload-1', '{"cursor": 0}'::jsonb
                    )
                    """
                ),
                {"id": uuid4(), "owner_id": owner_id, "graph_id": graph_id},
            )
            await session.commit()

        for run_id, cursor in ((older_run, 1), (newer_run, 2)):
            async with sessions() as session:
                store = _store(
                    session,
                    run_id=run_id,
                    owner_id=owner_id,
                    graph_id=graph_id,
                )
                await store.acquire_lock()
                assert await store.load(
                    store.pipeline_key, "local_documents", "upload-1"
                ) == {"cursor": 0}
                await store.save(
                    store.pipeline_key,
                    "local_documents",
                    "upload-1",
                    {"cursor": cursor},
                )
                await session.commit()

        async with sessions() as session:
            assert await complete_run_with_source_state_promotion(
                session,
                newer_run,
                job_id=newer_job,
                lease_owner="newer-worker",
            )

        async with sessions() as session:
            with pytest.raises(StaleSDKSourceStateCandidateError, match="changed"):
                await complete_run_with_source_state_promotion(
                    session,
                    older_run,
                    job_id=older_job,
                    lease_owner="older-worker",
                )

        async with sessions() as session:
            state = await session.scalar(select(SDKSourceState))
            assert state is not None
            assert state.state_data == {"cursor": 2}
            assert state.revision == 2
            assert await session.scalar(select(func.count(SDKSourceStateCandidate.id))) == 1
            run = await session.get(Run, older_run)
            assert run is not None and run.status == "running"
    finally:
        await engine.dispose()
        async with admin_engine.begin() as connection:
            await connection.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        await admin_engine.dispose()
