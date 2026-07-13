"""Live PostgreSQL coverage for whole-run Studio SDK state acknowledgement."""

from __future__ import annotations

import asyncio
import os
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import pytest
from app.models.execution import Run
from app.models.sdk_source_state import SDKSourceState, SDKSourceStateCandidate
from app.services.execution_service import (
    RunFailureLeaseError,
    cancel_run,
    fail_run_if_running,
)
from app.services.run_queue_service import heartbeat_run_job
from app.services.sdk_source_state_service import (
    RunCompletionLeaseError,
    SDKSourceStateLeaseError,
    StaleSDKSourceStateCandidateError,
    StudioSDKSourceStateStore,
    _run_lock_id,
    complete_run_with_source_state_promotion,
    revalidate_source_state_staging_lease,
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
                    trigger_payload JSONB NULL,
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
    job_id: UUID | None = None,
    worker: str | None = None,
) -> StudioSDKSourceStateStore:
    return StudioSDKSourceStateStore(
        session,
        run_id=run_id,
        owner_id=owner_id,
        graph_id=graph_id,
        node_id="documents",
        job_id=job_id,
        lease_owner=worker,
    )


@pytest.mark.asyncio
async def test_stale_identity_map_cancellation_cannot_overwrite_completed_run():
    schema = f"sdk_cancel_race_{uuid4().hex}"
    engine = create_async_engine(
        TEST_DATABASE_URL,
        connect_args={"server_settings": {"search_path": schema}},
    )
    admin_engine = create_async_engine(TEST_DATABASE_URL)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    owner_id, graph_id, run_id, job_id = uuid4(), uuid4(), uuid4(), uuid4()

    try:
        await _create_schema(admin_engine, schema)
        async with sessions() as seed_session:
            await _seed_run(
                seed_session,
                owner_id=owner_id,
                graph_id=graph_id,
                run_id=run_id,
                job_id=job_id,
                worker="worker-1",
            )

        async with sessions() as stale_session:
            cached_run = await stale_session.get(Run, run_id)
            assert cached_run is not None and cached_run.status == "running"

            async with sessions() as completion_session:
                assert await complete_run_with_source_state_promotion(
                    completion_session,
                    run_id,
                    job_id=job_id,
                    lease_owner="worker-1",
                )

            with pytest.raises(ValueError, match="completed -> cancelled"):
                await cancel_run(stale_session, run_id)
            assert cached_run.status == "completed"

        async with sessions() as verification_session:
            run = await verification_session.get(Run, run_id)
            assert run is not None and run.status == "completed"
    finally:
        await engine.dispose()
        async with admin_engine.begin() as connection:
            await connection.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        await admin_engine.dispose()


@pytest.mark.asyncio
async def test_stale_failure_cannot_overwrite_cancelled_run():
    schema = f"sdk_failure_race_{uuid4().hex}"
    engine = create_async_engine(
        TEST_DATABASE_URL,
        connect_args={"server_settings": {"search_path": schema}},
    )
    admin_engine = create_async_engine(TEST_DATABASE_URL)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    owner_id, graph_id, run_id, job_id = uuid4(), uuid4(), uuid4(), uuid4()

    try:
        await _create_schema(admin_engine, schema)
        async with sessions() as seed_session:
            await _seed_run(
                seed_session,
                owner_id=owner_id,
                graph_id=graph_id,
                run_id=run_id,
                job_id=job_id,
                worker="worker-1",
            )

        async with sessions() as stale_failure_session:
            cached_run = await stale_failure_session.get(Run, run_id)
            assert cached_run is not None and cached_run.status == "running"

            async with sessions() as cancellation_session:
                cancelled = await cancel_run(cancellation_session, run_id)
                assert cancelled is not None and cancelled.status == "cancelled"

            current_run, transitioned = await fail_run_if_running(
                stale_failure_session,
                run_id,
                "late node failure",
            )
            assert current_run is cached_run
            assert transitioned is False
            assert cached_run.status == "cancelled"
            assert cached_run.error_message is None

        async with sessions() as verification_session:
            run = await verification_session.get(Run, run_id)
            assert run is not None and run.status == "cancelled"
    finally:
        await engine.dispose()
        async with admin_engine.begin() as connection:
            await connection.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        await admin_engine.dispose()


@pytest.mark.asyncio
async def test_replaced_worker_lease_cannot_transition_run_to_failed():
    schema = f"sdk_failure_lease_{uuid4().hex}"
    engine = create_async_engine(
        TEST_DATABASE_URL,
        connect_args={"server_settings": {"search_path": schema}},
    )
    admin_engine = create_async_engine(TEST_DATABASE_URL)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    owner_id, graph_id, run_id, job_id = uuid4(), uuid4(), uuid4(), uuid4()

    try:
        await _create_schema(admin_engine, schema)
        async with sessions() as seed_session:
            await _seed_run(
                seed_session,
                owner_id=owner_id,
                graph_id=graph_id,
                run_id=run_id,
                job_id=job_id,
                worker="stale-worker",
            )

        async with sessions() as replacement_session:
            await replacement_session.execute(
                text(
                    "UPDATE run_jobs SET lease_owner = 'replacement-worker' "
                    "WHERE id = :job_id"
                ),
                {"job_id": job_id},
            )
            await replacement_session.commit()

        async with sessions() as stale_failure_session:
            with pytest.raises(RunFailureLeaseError, match="lease was lost"):
                await fail_run_if_running(
                    stale_failure_session,
                    run_id,
                    "stale worker failure",
                    job_id=job_id,
                    lease_owner="stale-worker",
                )

        async with sessions() as verification_session:
            run = await verification_session.get(Run, run_id)
            assert run is not None and run.status == "running"
            assert run.error_message is None
    finally:
        await engine.dispose()
        async with admin_engine.begin() as connection:
            await connection.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        await admin_engine.dispose()


@pytest.mark.asyncio
async def test_replaced_worker_lease_cannot_stage_source_candidate():
    schema = f"sdk_staging_lease_{uuid4().hex}"
    engine = create_async_engine(
        TEST_DATABASE_URL,
        connect_args={"server_settings": {"search_path": schema}},
    )
    admin_engine = create_async_engine(TEST_DATABASE_URL)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    owner_id, graph_id, run_id, job_id = uuid4(), uuid4(), uuid4(), uuid4()

    try:
        await _create_schema(admin_engine, schema)
        async with sessions() as session:
            await _seed_run(
                session,
                owner_id=owner_id,
                graph_id=graph_id,
                run_id=run_id,
                job_id=job_id,
                worker="stale-worker",
            )
            await session.execute(
                text("UPDATE run_jobs SET lease_owner = 'replacement-worker' WHERE id = :id"),
                {"id": job_id},
            )
            await session.commit()

        async with sessions() as session:
            store = _store(
                session,
                run_id=run_id,
                owner_id=owner_id,
                graph_id=graph_id,
                job_id=job_id,
                worker="stale-worker",
            )
            with pytest.raises(SDKSourceStateLeaseError, match="lease was lost"):
                await store.save(
                    store.pipeline_key,
                    "local_documents",
                    "upload-1",
                    {"cursor": 1},
                )

        async with sessions() as session:
            assert await session.scalar(select(func.count(SDKSourceStateCandidate.id))) == 0
    finally:
        await engine.dispose()
        async with admin_engine.begin() as connection:
            await connection.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        await admin_engine.dispose()


@pytest.mark.asyncio
async def test_completion_waits_for_candidate_commit_then_promotes_it():
    schema = f"sdk_scope_snapshot_{uuid4().hex}"
    engine = create_async_engine(
        TEST_DATABASE_URL,
        connect_args={"server_settings": {"search_path": schema}},
    )
    admin_engine = create_async_engine(TEST_DATABASE_URL)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    owner_id, graph_id, run_id, job_id = uuid4(), uuid4(), uuid4(), uuid4()

    try:
        await _create_schema(admin_engine, schema)
        async with sessions() as seed:
            await _seed_run(
                seed,
                owner_id=owner_id,
                graph_id=graph_id,
                run_id=run_id,
                job_id=job_id,
                worker="worker-1",
            )

        async with sessions() as staging:
            store = _store(
                staging,
                run_id=run_id,
                owner_id=owner_id,
                graph_id=graph_id,
                job_id=job_id,
                worker="worker-1",
            )
            await store.save(
                store.pipeline_key,
                "local_documents",
                "upload-1",
                {"cursor": 1},
            )

            async def complete() -> bool:
                async with sessions() as completion:
                    return await complete_run_with_source_state_promotion(
                        completion,
                        run_id,
                        job_id=job_id,
                        lease_owner="worker-1",
                    )

            completion_task = asyncio.create_task(complete())
            await asyncio.sleep(0.05)
            assert not completion_task.done()
            await staging.commit()
            assert await asyncio.wait_for(completion_task, timeout=3)

        async with sessions() as verification:
            state = await verification.scalar(select(SDKSourceState))
            assert state is not None and state.state_data == {"cursor": 1}
            assert await verification.scalar(
                select(func.count(SDKSourceStateCandidate.id))
            ) == 0
            run = await verification.get(Run, run_id)
            assert run is not None and run.status == "completed"
    finally:
        await engine.dispose()
        async with admin_engine.begin() as connection:
            await connection.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        await admin_engine.dispose()


@pytest.mark.asyncio
async def test_completion_lease_expiry_while_waiting_on_run_fence_fails_closed():
    schema = f"sdk_completion_expiry_{uuid4().hex}"
    engine = create_async_engine(
        TEST_DATABASE_URL,
        connect_args={"server_settings": {"search_path": schema}},
    )
    admin_engine = create_async_engine(TEST_DATABASE_URL)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    owner_id, graph_id, run_id, job_id = uuid4(), uuid4(), uuid4(), uuid4()

    try:
        await _create_schema(admin_engine, schema)
        async with sessions() as seed:
            await _seed_run(
                seed,
                owner_id=owner_id,
                graph_id=graph_id,
                run_id=run_id,
                job_id=job_id,
                worker="worker-1",
            )
            await seed.execute(
                text("UPDATE run_jobs SET lease_expires_at = :expiry WHERE id = :id"),
                {
                    "id": job_id,
                    "expiry": datetime.now(UTC) + timedelta(milliseconds=200),
                },
            )
            await seed.commit()

        async with sessions() as blocker:
            await blocker.execute(select(func.pg_advisory_xact_lock(_run_lock_id(run_id))))

            async def complete() -> bool:
                async with sessions() as completion:
                    return await complete_run_with_source_state_promotion(
                        completion,
                        run_id,
                        job_id=job_id,
                        lease_owner="worker-1",
                    )

            completion_task = asyncio.create_task(complete())
            await asyncio.sleep(0.3)
            assert not completion_task.done()
            await blocker.commit()
            with pytest.raises(RunCompletionLeaseError, match="lease was lost"):
                await asyncio.wait_for(completion_task, timeout=3)

        async with sessions() as verification:
            run = await verification.get(Run, run_id)
            assert run is not None and run.status == "running"
            assert await verification.scalar(select(func.count(SDKSourceState.id))) == 0
    finally:
        await engine.dispose()
        async with admin_engine.begin() as connection:
            await connection.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        await admin_engine.dispose()


@pytest.mark.asyncio
async def test_long_read_fences_allow_heartbeat_then_stage_and_promote():
    schema = f"sdk_read_heartbeat_{uuid4().hex}"
    engine = create_async_engine(
        TEST_DATABASE_URL,
        connect_args={"server_settings": {"search_path": schema}},
    )
    admin_engine = create_async_engine(TEST_DATABASE_URL)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    owner_id, graph_id, run_id, job_id = uuid4(), uuid4(), uuid4(), uuid4()
    worker = "worker-1"
    original_expiry = datetime.now(UTC) + timedelta(seconds=2)

    try:
        await _create_schema(admin_engine, schema)
        async with sessions() as seed:
            await _seed_run(
                seed,
                owner_id=owner_id,
                graph_id=graph_id,
                run_id=run_id,
                job_id=job_id,
                worker=worker,
            )
            await seed.execute(
                text("UPDATE run_jobs SET lease_expires_at = :expiry WHERE id = :id"),
                {"id": job_id, "expiry": original_expiry},
            )
            await seed.commit()

        async with sessions() as long_read:
            store = _store(
                long_read,
                run_id=run_id,
                owner_id=owner_id,
                graph_id=graph_id,
                job_id=job_id,
                worker=worker,
            )
            await store.acquire_lock()

            async with sessions() as heartbeat:
                assert await asyncio.wait_for(
                    heartbeat_run_job(
                        heartbeat,
                        job_id=job_id,
                        worker_id=worker,
                        lease_seconds=10,
                    ),
                    timeout=1,
                )

            await asyncio.sleep(
                max(0.0, (original_expiry - datetime.now(UTC)).total_seconds() + 0.1)
            )
            await store.save(
                store.pipeline_key,
                "local_documents",
                "upload-1",
                {"cursor": 1},
            )
            await revalidate_source_state_staging_lease(
                long_read,
                run_id,
                job_id=job_id,
                lease_owner=worker,
            )
            await long_read.commit()

        async with sessions() as completion:
            assert await complete_run_with_source_state_promotion(
                completion,
                run_id,
                job_id=job_id,
                lease_owner=worker,
            )

        async with sessions() as verification:
            state = await verification.scalar(select(SDKSourceState))
            assert state is not None and state.state_data == {"cursor": 1}
            run = await verification.get(Run, run_id)
            assert run is not None and run.status == "completed"
    finally:
        await engine.dispose()
        async with admin_engine.begin() as connection:
            await connection.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        await admin_engine.dispose()


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
