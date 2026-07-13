"""In-process consumer for the database-backed durable run queue."""

from __future__ import annotations

import asyncio
import logging
import os
import socket
import uuid
from contextlib import suppress
from uuid import UUID

from app.config import settings
from app.db.session import AsyncSessionLocal
from app.engine.run_job_executor import execute_run_job
from app.models.execution import Run, RunJobStatus, RunStatus
from app.services.run_queue_service import (
    claim_run_job,
    finish_run_job,
    heartbeat_run_job,
    mark_run_failed_if_owned,
    recover_orphaned_runs,
    release_run_job,
)

logger = logging.getLogger(__name__)


class DurableRunWorker:
    def __init__(self) -> None:
        self.worker_id = f"{socket.gethostname()}:{os.getpid()}:{uuid.uuid4().hex[:12]}"
        self._stop = asyncio.Event()
        self._tasks: list[asyncio.Task[None]] = []

    async def start(self) -> None:
        if self._tasks:
            return
        async with AsyncSessionLocal() as db:
            recovered = await recover_orphaned_runs(db)
        if recovered:
            logger.warning("Recovered %s orphaned pending/running runs", recovered)
        self._stop.clear()
        self._tasks = [
            asyncio.create_task(self._worker_loop(index), name=f"run-worker-{index}")
            for index in range(settings.run_worker_concurrency)
        ]
        logger.info(
            "Started durable run worker %s with concurrency %s",
            self.worker_id,
            settings.run_worker_concurrency,
        )

    async def stop(self) -> None:
        self._stop.set()
        for task in self._tasks:
            task.cancel()
        for task in self._tasks:
            with suppress(asyncio.CancelledError):
                await task
        self._tasks.clear()
        logger.info("Stopped durable run worker %s", self.worker_id)

    async def _worker_loop(self, index: int) -> None:
        worker_slot = f"{self.worker_id}:{index}"
        while not self._stop.is_set():
            try:
                async with AsyncSessionLocal() as db:
                    job = await claim_run_job(
                        db,
                        worker_id=worker_slot,
                        lease_seconds=settings.run_worker_lease_seconds,
                    )
                if job is None:
                    await self._wait_for_poll()
                    continue
                await self._process_job(job.id, job.run_id, worker_slot)
            except asyncio.CancelledError:
                if self._stop.is_set():
                    raise
                logger.warning("Run worker slot %s cancelled after losing a lease", worker_slot)
            except Exception:
                logger.exception("Durable run worker loop failed")
                await self._wait_for_poll()

    async def _wait_for_poll(self) -> None:
        try:
            await asyncio.wait_for(self._stop.wait(), timeout=settings.run_worker_poll_seconds)
        except TimeoutError:
            pass

    async def _process_job(self, job_id: UUID, run_id: UUID, worker_id: str) -> None:
        heartbeat_stop = asyncio.Event()
        processing_task = asyncio.current_task()
        if processing_task is None:
            raise RuntimeError("Run worker has no current task")
        heartbeat_task = asyncio.create_task(
            self._heartbeat_loop(job_id, worker_id, heartbeat_stop, processing_task),
            name=f"run-heartbeat-{job_id}",
        )
        error: str | None = None
        try:
            async with AsyncSessionLocal() as db:
                from app.models.execution import RunJob
                from app.ws.execution_ws import ws_manager

                job = await db.get(RunJob, job_id)
                if (
                    job is None
                    or job.status != RunJobStatus.LEASED.value
                    or job.lease_owner != worker_id
                ):
                    return
                await execute_run_job(db, job, ws_manager)
                run = await db.get(Run, run_id)
                if run is not None and run.status == RunStatus.FAILED.value:
                    error = run.error_message or "Run execution failed"
        except asyncio.CancelledError:
            async with AsyncSessionLocal() as db:
                await release_run_job(db, job_id=job_id, worker_id=worker_id)
            raise
        except Exception as exc:
            error = f"{type(exc).__name__}: {exc}"[:4000]
            logger.exception("Durable run job %s failed", job_id)
            await self._mark_run_failed_if_owned(job_id, run_id, worker_id, error)
        finally:
            heartbeat_stop.set()
            heartbeat_task.cancel()
            with suppress(asyncio.CancelledError):
                await heartbeat_task

        async with AsyncSessionLocal() as db:
            finished = await finish_run_job(
                db,
                job_id=job_id,
                worker_id=worker_id,
                error=error,
            )
        if not finished:
            logger.warning("Run job %s lost its lease before completion", job_id)

    async def _heartbeat_loop(
        self,
        job_id: UUID,
        worker_id: str,
        stop: asyncio.Event,
        processing_task: asyncio.Task[None],
    ) -> None:
        while not stop.is_set():
            try:
                await asyncio.wait_for(stop.wait(), timeout=settings.run_worker_heartbeat_seconds)
                return
            except TimeoutError:
                try:
                    async with AsyncSessionLocal() as db:
                        renewed = await heartbeat_run_job(
                            db,
                            job_id=job_id,
                            worker_id=worker_id,
                            lease_seconds=settings.run_worker_lease_seconds,
                        )
                except Exception:
                    logger.exception("Run job %s heartbeat failed", job_id)
                    processing_task.cancel()
                    return
                if not renewed:
                    logger.warning("Run job %s heartbeat lost ownership", job_id)
                    processing_task.cancel()
                    return

    @staticmethod
    async def _mark_run_failed_if_owned(
        job_id: UUID,
        run_id: UUID,
        worker_id: str,
        error: str,
    ) -> None:
        async with AsyncSessionLocal() as db:
            await mark_run_failed_if_owned(
                db,
                job_id=job_id,
                run_id=run_id,
                worker_id=worker_id,
                error=error,
            )
