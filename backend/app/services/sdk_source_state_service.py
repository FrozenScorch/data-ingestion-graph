"""PostgreSQL state-store bridge for SDK sources executed by Studio."""

from __future__ import annotations

import asyncio
import hashlib
import time
from collections.abc import Mapping
from typing import Any
from uuid import UUID

from app.models.sdk_source_state import SDKSourceState
from ingestion_graph.state import StateStore
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession


class StudioSDKSourceStateStore(StateStore):
    """Bind the SDK StateStore contract to one owner/graph/node scope.

    Writes are staged on the caller's execution session. The node runner's final
    commit therefore makes the successful RunNode output and source checkpoint
    durable together.
    """

    def __init__(
        self,
        session: AsyncSession,
        *,
        owner_id: UUID,
        graph_id: UUID,
        node_id: str,
        lock_timeout_seconds: float = 30.0,
    ) -> None:
        self.session = session
        self.owner_id = UUID(str(owner_id))
        self.graph_id = UUID(str(graph_id))
        self.node_id = node_id
        self.lock_timeout_seconds = max(0.0, lock_timeout_seconds)
        self.pipeline_key = f"studio:{self.owner_id}:{self.graph_id}:{node_id}"

    async def acquire_lock(self) -> None:
        """Serialize this node's state transition across Studio workers."""
        digest = hashlib.sha256(self.pipeline_key.encode()).digest()
        lock_id = int.from_bytes(digest[:8], byteorder="big", signed=True)
        deadline = time.monotonic() + self.lock_timeout_seconds
        while True:
            result = await self.session.execute(select(func.pg_try_advisory_xact_lock(lock_id)))
            if bool(result.scalar_one()):
                return
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError("Timed out waiting for SDK source state lock")
            await asyncio.sleep(min(0.1, remaining))

    async def load(self, pipeline: str, source: str, stream: str) -> Mapping[str, Any]:
        self._require_pipeline(pipeline)
        result = await self.session.execute(
            select(SDKSourceState).where(
                SDKSourceState.owner_id == self.owner_id,
                SDKSourceState.graph_id == self.graph_id,
                SDKSourceState.node_id == self.node_id,
                SDKSourceState.source == source,
                SDKSourceState.stream == stream,
            )
        )
        item = result.scalar_one_or_none()
        return dict(item.state_data) if item is not None else {}

    async def save(
        self,
        pipeline: str,
        source: str,
        stream: str,
        state: Mapping[str, Any],
    ) -> None:
        self._require_pipeline(pipeline)
        result = await self.session.execute(
            select(SDKSourceState).where(
                SDKSourceState.owner_id == self.owner_id,
                SDKSourceState.graph_id == self.graph_id,
                SDKSourceState.node_id == self.node_id,
                SDKSourceState.source == source,
                SDKSourceState.stream == stream,
            )
        )
        item = result.scalar_one_or_none()
        if item is None:
            self.session.add(
                SDKSourceState(
                    owner_id=self.owner_id,
                    graph_id=self.graph_id,
                    node_id=self.node_id,
                    source=source,
                    stream=stream,
                    state_data=dict(state),
                )
            )
        else:
            item.state_data = dict(state)

    async def list_streams(self, pipeline: str, source: str) -> list[str]:
        self._require_pipeline(pipeline)
        result = await self.session.execute(
            select(SDKSourceState.stream).where(
                SDKSourceState.owner_id == self.owner_id,
                SDKSourceState.graph_id == self.graph_id,
                SDKSourceState.node_id == self.node_id,
                SDKSourceState.source == source,
            )
        )
        return list(result.scalars().all())

    async def delete(self, pipeline: str, source: str, stream: str) -> None:
        self._require_pipeline(pipeline)
        result = await self.session.execute(
            select(SDKSourceState).where(
                SDKSourceState.owner_id == self.owner_id,
                SDKSourceState.graph_id == self.graph_id,
                SDKSourceState.node_id == self.node_id,
                SDKSourceState.source == source,
                SDKSourceState.stream == stream,
            )
        )
        item = result.scalar_one_or_none()
        if item is not None:
            await self.session.delete(item)

    def _require_pipeline(self, pipeline: str) -> None:
        if pipeline != self.pipeline_key:
            raise ValueError("SDK source state requested outside its graph-node scope")
