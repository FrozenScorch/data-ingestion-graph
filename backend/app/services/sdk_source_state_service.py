"""PostgreSQL state-store bridge and whole-run source-state promotion."""

from __future__ import annotations

import asyncio
import hashlib
import time
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

from app.models.execution import Run, RunJob, RunJobStatus, RunStatus
from app.models.sdk_source_state import SDKSourceState, SDKSourceStateCandidate
from ingestion_graph.state import StateStore
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

SAVE_OPERATION = "save"
DELETE_OPERATION = "delete"


class StaleSDKSourceStateCandidateError(RuntimeError):
    """A run attempted to promote state derived from an obsolete checkpoint."""


class RunCompletionLeaseError(RuntimeError):
    """The worker no longer owns the lease required to acknowledge a run."""


def _pipeline_key(owner_id: UUID, graph_id: UUID, node_id: str) -> str:
    return f"studio:{owner_id}:{graph_id}:{node_id}"


def _lock_id(pipeline_key: str) -> int:
    digest = hashlib.sha256(pipeline_key.encode()).digest()
    return int.from_bytes(digest[:8], byteorder="big", signed=True)


class StudioSDKSourceStateStore(StateStore):
    """Bind SDK state to committed reads and one run's candidate writes.

    A source node durably stages candidates with its POST_EXEC output. Only the
    graph-completion transaction promotes those candidates to committed state.
    """

    def __init__(
        self,
        session: AsyncSession,
        *,
        run_id: UUID,
        owner_id: UUID,
        graph_id: UUID,
        node_id: str,
        lock_timeout_seconds: float = 30.0,
    ) -> None:
        self.session = session
        self.run_id = UUID(str(run_id))
        self.owner_id = UUID(str(owner_id))
        self.graph_id = UUID(str(graph_id))
        self.node_id = node_id
        self.lock_timeout_seconds = max(0.0, lock_timeout_seconds)
        self.pipeline_key = _pipeline_key(self.owner_id, self.graph_id, node_id)

    async def acquire_lock(self) -> None:
        """Serialize candidate creation with state promotion for this node."""
        deadline = time.monotonic() + self.lock_timeout_seconds
        lock_id = _lock_id(self.pipeline_key)
        while True:
            result = await self.session.execute(select(func.pg_try_advisory_xact_lock(lock_id)))
            if bool(result.scalar_one()):
                return
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError("Timed out waiting for SDK source state lock")
            await asyncio.sleep(min(0.1, remaining))

    async def load(self, pipeline: str, source: str, stream: str) -> Mapping[str, Any]:
        """Read only state acknowledged by a previously completed graph run."""
        self._require_pipeline(pipeline)
        item = await self._committed(source, stream, visible_only=True)
        return dict(item.state_data) if item is not None else {}

    async def save(
        self,
        pipeline: str,
        source: str,
        stream: str,
        state: Mapping[str, Any],
    ) -> None:
        self._require_pipeline(pipeline)
        await self._lock_active_run()
        candidate = await self._candidate(source, stream)
        committed = await self._committed(source, stream)
        if candidate is None:
            candidate = SDKSourceStateCandidate(
                run_id=self.run_id,
                owner_id=self.owner_id,
                graph_id=self.graph_id,
                node_id=self.node_id,
                source=source,
                stream=stream,
                operation=SAVE_OPERATION,
                state_data=dict(state),
                base_state_data=_visible_state(committed),
                base_revision=committed.revision if committed is not None else 0,
            )
            self.session.add(candidate)
            return
        candidate.operation = SAVE_OPERATION
        candidate.state_data = dict(state)
        candidate.base_state_data = _visible_state(committed)
        candidate.base_revision = committed.revision if committed is not None else 0

    async def list_streams(self, pipeline: str, source: str) -> list[str]:
        """List only streams acknowledged by a completed graph run."""
        self._require_pipeline(pipeline)
        result = await self.session.execute(
            select(SDKSourceState.stream).where(
                SDKSourceState.owner_id == self.owner_id,
                SDKSourceState.graph_id == self.graph_id,
                SDKSourceState.node_id == self.node_id,
                SDKSourceState.source == source,
                SDKSourceState.is_deleted.is_(False),
            )
        )
        return list(result.scalars().all())

    async def delete(self, pipeline: str, source: str, stream: str) -> None:
        self._require_pipeline(pipeline)
        await self._lock_active_run()
        candidate = await self._candidate(source, stream)
        committed = await self._committed(source, stream)
        if candidate is not None:
            if committed is None or committed.is_deleted:
                await self.session.delete(candidate)
            else:
                candidate.operation = DELETE_OPERATION
                candidate.state_data = None
                candidate.base_state_data = dict(committed.state_data)
                candidate.base_revision = committed.revision
            return

        if committed is None or committed.is_deleted:
            return
        self.session.add(
            SDKSourceStateCandidate(
                run_id=self.run_id,
                owner_id=self.owner_id,
                graph_id=self.graph_id,
                node_id=self.node_id,
                source=source,
                stream=stream,
                operation=DELETE_OPERATION,
                state_data=None,
                base_state_data=dict(committed.state_data),
                base_revision=committed.revision,
            )
        )

    async def _committed(
        self,
        source: str,
        stream: str,
        *,
        visible_only: bool = False,
    ) -> SDKSourceState | None:
        query = select(SDKSourceState).where(
            SDKSourceState.owner_id == self.owner_id,
            SDKSourceState.graph_id == self.graph_id,
            SDKSourceState.node_id == self.node_id,
            SDKSourceState.source == source,
            SDKSourceState.stream == stream,
        )
        if visible_only:
            query = query.where(SDKSourceState.is_deleted.is_(False))
        result = await self.session.execute(query)
        return result.scalar_one_or_none()

    async def _candidate(self, source: str, stream: str) -> SDKSourceStateCandidate | None:
        result = await self.session.execute(
            select(SDKSourceStateCandidate).where(
                SDKSourceStateCandidate.run_id == self.run_id,
                SDKSourceStateCandidate.owner_id == self.owner_id,
                SDKSourceStateCandidate.graph_id == self.graph_id,
                SDKSourceStateCandidate.node_id == self.node_id,
                SDKSourceStateCandidate.source == source,
                SDKSourceStateCandidate.stream == stream,
            )
        )
        return result.scalar_one_or_none()

    async def _lock_active_run(self) -> None:
        result = await self.session.execute(
            select(Run)
            .where(Run.id == self.run_id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
        run = result.scalar_one_or_none()
        if run is None or run.status != RunStatus.RUNNING.value:
            raise RuntimeError("SDK source state cannot be staged for an inactive run")

    def _require_pipeline(self, pipeline: str) -> None:
        if pipeline != self.pipeline_key:
            raise ValueError("SDK source state requested outside its graph-node scope")


async def _lock_candidate_scopes(db: AsyncSession, run_id: UUID) -> list[str]:
    scope_result = await db.execute(
        select(
            SDKSourceStateCandidate.owner_id,
            SDKSourceStateCandidate.graph_id,
            SDKSourceStateCandidate.node_id,
        ).where(SDKSourceStateCandidate.run_id == run_id)
    )
    pipeline_keys = sorted(
        {
            _pipeline_key(owner_id, graph_id, node_id)
            for owner_id, graph_id, node_id in scope_result.all()
        }
    )
    for pipeline_key in pipeline_keys:
        await db.execute(select(func.pg_advisory_xact_lock(_lock_id(pipeline_key))))
    return pipeline_keys


async def promote_sdk_source_state_candidates(
    db: AsyncSession,
    run_id: UUID,
    *,
    locked_pipeline_keys: list[str] | None = None,
) -> int:
    """Promote one run's candidates without committing the surrounding transaction."""
    pipeline_keys = locked_pipeline_keys
    if pipeline_keys is None:
        pipeline_keys = await _lock_candidate_scopes(db, run_id)

    result = await db.execute(
        select(SDKSourceStateCandidate)
        .where(SDKSourceStateCandidate.run_id == run_id)
        .order_by(
            SDKSourceStateCandidate.owner_id,
            SDKSourceStateCandidate.graph_id,
            SDKSourceStateCandidate.node_id,
            SDKSourceStateCandidate.source,
            SDKSourceStateCandidate.stream,
        )
        .with_for_update()
    )
    candidates = list(result.scalars().all())
    if any(
        _pipeline_key(candidate.owner_id, candidate.graph_id, candidate.node_id)
        not in pipeline_keys
        for candidate in candidates
    ):
        raise StaleSDKSourceStateCandidateError(
            "SDK source candidates changed while graph completion was being fenced"
        )

    for candidate in candidates:
        committed_result = await db.execute(
            select(SDKSourceState)
            .where(
                SDKSourceState.owner_id == candidate.owner_id,
                SDKSourceState.graph_id == candidate.graph_id,
                SDKSourceState.node_id == candidate.node_id,
                SDKSourceState.source == candidate.source,
                SDKSourceState.stream == candidate.stream,
            )
            .with_for_update()
        )
        committed = committed_result.scalar_one_or_none()
        current_revision = committed.revision if committed is not None else 0
        if (
            current_revision != candidate.base_revision
            or _visible_state(committed) != candidate.base_state_data
        ):
            raise StaleSDKSourceStateCandidateError(
                "SDK source state changed after this run staged its candidate"
            )

        if committed is None:
            if candidate.operation != SAVE_OPERATION or candidate.state_data is None:
                raise StaleSDKSourceStateCandidateError(
                    "SDK source delete candidate has no committed base state"
                )
            db.add(
                SDKSourceState(
                    owner_id=candidate.owner_id,
                    graph_id=candidate.graph_id,
                    node_id=candidate.node_id,
                    source=candidate.source,
                    stream=candidate.stream,
                    state_data=dict(candidate.state_data),
                    revision=1,
                    is_deleted=False,
                )
            )
        elif candidate.operation == SAVE_OPERATION and candidate.state_data is not None:
            committed.state_data = dict(candidate.state_data)
            committed.revision += 1
            committed.is_deleted = False
        elif candidate.operation == DELETE_OPERATION:
            committed.state_data = {}
            committed.revision += 1
            committed.is_deleted = True
        else:
            raise StaleSDKSourceStateCandidateError("Invalid SDK source state candidate intent")
        await db.delete(candidate)

    return len(candidates)


async def complete_run_with_source_state_promotion(
    db: AsyncSession,
    run_id: UUID,
    *,
    job_id: UUID | None = None,
    lease_owner: str | None = None,
) -> bool:
    """Atomically promote candidates and mark a still-owned run completed.

    Returns ``False`` when cancellation or pause won the run-row lock. A supplied
    durable-job lease is fenced in the same transaction before any state moves.
    """
    if job_id is not None:
        job_result = await db.execute(
            select(RunJob)
            .where(RunJob.id == job_id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
        job = job_result.scalar_one_or_none()
        now = datetime.now(UTC)
        if (
            job is None
            or lease_owner is None
            or job.run_id != run_id
            or job.status != RunJobStatus.LEASED.value
            or job.lease_owner != lease_owner
            or job.lease_expires_at is None
            or job.lease_expires_at <= now
        ):
            await db.rollback()
            raise RunCompletionLeaseError("Run job lease was lost before graph completion")

    # Source staging takes its scope lock before the run row. Match that order so
    # cancellation, staging, and completion cannot form an advisory/row deadlock.
    pipeline_keys = await _lock_candidate_scopes(db, run_id)

    run_result = await db.execute(
        select(Run)
        .where(Run.id == run_id)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    run = run_result.scalar_one_or_none()
    if run is None:
        await db.rollback()
        raise RuntimeError("Run no longer exists")
    if run.status != RunStatus.RUNNING.value:
        run_status = run.status
        await db.rollback()
        if run_status in (RunStatus.CANCELLED.value, RunStatus.PAUSED.value):
            return False
        raise RuntimeError(f"Cannot complete run in {run_status!r} state")

    try:
        await promote_sdk_source_state_candidates(
            db,
            run_id,
            locked_pipeline_keys=pipeline_keys,
        )
        run.status = RunStatus.COMPLETED.value
        run.error_message = None
        await db.commit()
    except Exception:
        await db.rollback()
        raise
    return True


def _visible_state(item: SDKSourceState | None) -> dict[str, Any] | None:
    if item is None or item.is_deleted:
        return None
    return dict(item.state_data)
