"""Replaceable durable state stores for connector cursors."""

from __future__ import annotations

import asyncio
import json
import sqlite3
import threading
from abc import ABC, abstractmethod
from collections.abc import Mapping
from pathlib import Path
from typing import Any


class StateStore(ABC):
    @abstractmethod
    async def load(self, pipeline: str, source: str, stream: str) -> Mapping[str, Any]: ...

    @abstractmethod
    async def save(
        self,
        pipeline: str,
        source: str,
        stream: str,
        state: Mapping[str, Any],
    ) -> None: ...


class MemoryStateStore(StateStore):
    def __init__(self) -> None:
        self._states: dict[tuple[str, str, str], dict[str, Any]] = {}
        self._lock = asyncio.Lock()

    async def load(self, pipeline: str, source: str, stream: str) -> Mapping[str, Any]:
        async with self._lock:
            return dict(self._states.get((pipeline, source, stream), {}))

    async def save(self, pipeline: str, source: str, stream: str, state: Mapping[str, Any]) -> None:
        async with self._lock:
            self._states[(pipeline, source, stream)] = dict(state)


class SQLiteStateStore(StateStore):
    """Small durable state store using one atomic UPSERT per checkpoint."""

    def __init__(self, path: str | Path = ".ingestion/state.db") -> None:
        self.path = Path(path)
        self._init_lock = threading.Lock()
        self._initialized = False

    def _connect(self) -> sqlite3.Connection:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(self.path, timeout=30)
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA synchronous=FULL")
        return connection

    def _initialize(self) -> None:
        with self._init_lock:
            if self._initialized:
                return
            with self._connect() as connection:
                connection.execute(
                    """
                    CREATE TABLE IF NOT EXISTS connector_state (
                        pipeline TEXT NOT NULL,
                        source TEXT NOT NULL,
                        stream TEXT NOT NULL,
                        state_json TEXT NOT NULL,
                        updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        PRIMARY KEY (pipeline, source, stream)
                    )
                    """
                )
            self._initialized = True

    async def load(self, pipeline: str, source: str, stream: str) -> Mapping[str, Any]:
        return await asyncio.to_thread(self._load_sync, pipeline, source, stream)

    def _load_sync(self, pipeline: str, source: str, stream: str) -> Mapping[str, Any]:
        self._initialize()
        with self._connect() as connection:
            row = connection.execute(
                "SELECT state_json FROM connector_state WHERE pipeline=? AND source=? AND stream=?",
                (pipeline, source, stream),
            ).fetchone()
        return json.loads(row[0]) if row else {}

    async def save(self, pipeline: str, source: str, stream: str, state: Mapping[str, Any]) -> None:
        await asyncio.to_thread(self._save_sync, pipeline, source, stream, dict(state))

    def _save_sync(self, pipeline: str, source: str, stream: str, state: dict[str, Any]) -> None:
        self._initialize()
        serialized = json.dumps(state, sort_keys=True, separators=(",", ":"))
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            connection.execute(
                """
                INSERT INTO connector_state (pipeline, source, stream, state_json)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(pipeline, source, stream) DO UPDATE SET
                    state_json=excluded.state_json,
                    updated_at=CURRENT_TIMESTAMP
                """,
                (pipeline, source, stream, serialized),
            )
            connection.commit()
