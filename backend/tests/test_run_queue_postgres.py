"""PostgreSQL integration coverage for durable run-job leases."""

from __future__ import annotations

import asyncio
import os
from datetime import timedelta
from uuid import uuid4

import pytest
from app.models.execution import RunJobStatus
from app.services.run_queue_service import (
    claim_run_job,
    finish_run_job,
    heartbeat_run_job,
    recover_orphaned_runs,
    utc_now,
)
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

TEST_DATABASE_URL = os.getenv("TEST_DATABASE_URL")
pytestmark = pytest.mark.skipif(
    not TEST_DATABASE_URL,
    reason="TEST_DATABASE_URL is required for PostgreSQL lease integration tests",
)


@pytest.mark.asyncio
async def test_postgres_claim_is_exclusive_and_expired_lease_is_reclaimed():
    schema = f"run_queue_{uuid4().hex}"
    engine = create_async_engine(
        TEST_DATABASE_URL,
        connect_args={"server_settings": {"search_path": schema}},
    )
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    run_id = uuid4()
    job_id = uuid4()
    now = utc_now()

    admin_engine = create_async_engine(TEST_DATABASE_URL)
    try:
        async with admin_engine.begin() as conn:
            await conn.execute(text(f'CREATE SCHEMA "{schema}"'))
            await conn.execute(
                text(
                    f"""
                    CREATE TABLE "{schema}".runs (
                        id UUID PRIMARY KEY,
                        graph_id UUID NOT NULL,
                        graph_version_id UUID,
                        trigger_type VARCHAR(50) NOT NULL DEFAULT 'manual',
                        triggered_by UUID,
                        status VARCHAR(50) NOT NULL DEFAULT 'pending',
                        error_message TEXT,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                        updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
                    )
                    """
                )
            )
            await conn.execute(
                text(
                    f"""
                    CREATE TABLE "{schema}".run_jobs (
                        id UUID PRIMARY KEY,
                        run_id UUID NOT NULL UNIQUE REFERENCES "{schema}".runs(id),
                        job_type VARCHAR(50) NOT NULL,
                        status VARCHAR(50) NOT NULL,
                        available_at TIMESTAMPTZ NOT NULL,
                        lease_owner VARCHAR(255),
                        lease_expires_at TIMESTAMPTZ,
                        heartbeat_at TIMESTAMPTZ,
                        attempt_count INTEGER NOT NULL DEFAULT 0,
                        last_error TEXT,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                        updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
                    )
                    """
                )
            )
            await conn.execute(
                text(
                    f"""
                    INSERT INTO "{schema}".runs (id, graph_id, graph_version_id)
                    VALUES (:run_id, :graph_id, :graph_version_id)
                    """
                ),
                {"run_id": run_id, "graph_id": uuid4(), "graph_version_id": uuid4()},
            )
            await conn.execute(
                text(
                    f"""
                    INSERT INTO "{schema}".run_jobs
                        (id, run_id, job_type, status, available_at)
                    VALUES (:job_id, :run_id, 'full', 'queued', :available_at)
                    """
                ),
                {"job_id": job_id, "run_id": run_id, "available_at": now},
            )

        async def claim(worker_id: str):
            async with sessions() as db:
                return await claim_run_job(
                    db,
                    worker_id=worker_id,
                    lease_seconds=60,
                    now=now,
                )

        first, second = await asyncio.gather(claim("worker-a"), claim("worker-b"))
        claimed = [job for job in (first, second) if job is not None]
        assert len(claimed) == 1
        original_owner = claimed[0].lease_owner
        assert original_owner in {"worker-a", "worker-b"}
        assert claimed[0].attempt_count == 1

        async with admin_engine.begin() as conn:
            await conn.execute(
                text(
                    f"""
                    UPDATE "{schema}".run_jobs
                    SET lease_expires_at = :expired_at
                    WHERE id = :job_id
                    """
                ),
                {"expired_at": now - timedelta(seconds=1), "job_id": job_id},
            )

        replacement_owner = "worker-b" if original_owner == "worker-a" else "worker-a"
        async with sessions() as db:
            reclaimed = await claim_run_job(
                db,
                worker_id=replacement_owner,
                lease_seconds=60,
                now=now,
            )
        assert reclaimed is not None
        assert reclaimed.lease_owner == replacement_owner
        assert reclaimed.status == RunJobStatus.LEASED.value
        assert reclaimed.attempt_count == 2

        async with sessions() as db:
            assert not await heartbeat_run_job(
                db,
                job_id=job_id,
                worker_id=original_owner,
                lease_seconds=60,
                now=now,
            )
        async with sessions() as db:
            assert not await finish_run_job(
                db,
                job_id=job_id,
                worker_id=original_owner,
            )
        async with sessions() as db:
            assert await finish_run_job(
                db,
                job_id=job_id,
                worker_id=replacement_owner,
            )

        orphaned_run_id = uuid4()
        async with admin_engine.begin() as conn:
            await conn.execute(
                text(
                    f"""
                    INSERT INTO "{schema}".runs
                        (id, graph_id, graph_version_id, status)
                    VALUES (:run_id, :graph_id, :graph_version_id, 'running')
                    """
                ),
                {
                    "run_id": orphaned_run_id,
                    "graph_id": uuid4(),
                    "graph_version_id": uuid4(),
                },
            )
        async with sessions() as db:
            assert await recover_orphaned_runs(db) == 1
        async with admin_engine.connect() as conn:
            recovered = (
                await conn.execute(
                    text(
                        f"""
                        SELECT r.status, j.status, j.job_type
                        FROM "{schema}".runs r
                        JOIN "{schema}".run_jobs j ON j.run_id = r.id
                        WHERE r.id = :run_id
                        """
                    ),
                    {"run_id": orphaned_run_id},
                )
            ).one()
        assert recovered == ("pending", "queued", "full")
    finally:
        await engine.dispose()
        async with admin_engine.begin() as conn:
            await conn.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        await admin_engine.dispose()
