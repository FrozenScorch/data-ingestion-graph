"""Replay-safe extraction caches."""

from __future__ import annotations

import asyncio
import sqlite3
import time
from abc import ABC, abstractmethod
from pathlib import Path


class ExtractionCache(ABC):
    persistent: bool = False

    @abstractmethod
    async def get(self, key: str) -> bytes | None: ...

    @abstractmethod
    async def put(self, key: str, value: bytes, *, ttl_seconds: int | None = None) -> None: ...

    async def delete(self, key: str) -> None:
        return None

    async def close(self) -> None:
        return None


class MemoryExtractionCache(ExtractionCache):
    def __init__(self) -> None:
        self._values: dict[str, tuple[float | None, bytes]] = {}
        self._lock = asyncio.Lock()

    async def get(self, key: str) -> bytes | None:
        async with self._lock:
            item = self._values.get(key)
            if item is None:
                return None
            expires, value = item
            if expires is not None and expires <= time.time():
                self._values.pop(key, None)
                return None
            return bytes(value)

    async def put(self, key: str, value: bytes, *, ttl_seconds: int | None = None) -> None:
        if ttl_seconds is not None and ttl_seconds < 1:
            raise ValueError("ttl_seconds must be positive")
        expires = None if ttl_seconds is None else time.time() + ttl_seconds
        async with self._lock:
            self._values[key] = (expires, bytes(value))

    async def delete(self, key: str) -> None:
        async with self._lock:
            self._values.pop(key, None)


class SQLiteExtractionCache(ExtractionCache):
    persistent = True

    def __init__(self, path: str | Path = ".ingestion/extraction-cache.db") -> None:
        self.path = Path(path)
        self._initialized = False
        self._lock = asyncio.Lock()

    def _init(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(self.path) as db:
            db.execute("PRAGMA journal_mode=WAL")
            db.execute(
                "CREATE TABLE IF NOT EXISTS extraction_cache ("
                "key TEXT PRIMARY KEY, value BLOB NOT NULL, expires REAL)"
            )
        self._initialized = True

    async def get(self, key: str) -> bytes | None:
        async with self._lock:
            return await asyncio.to_thread(self._get_sync, key)

    def _get_sync(self, key: str) -> bytes | None:
        if not self._initialized:
            self._init()
        with sqlite3.connect(self.path) as db:
            row = db.execute(
                "SELECT value, expires FROM extraction_cache WHERE key=?", (key,)
            ).fetchone()
            if row is None:
                return None
            if row[1] is not None and float(row[1]) <= time.time():
                db.execute("DELETE FROM extraction_cache WHERE key=?", (key,))
                return None
            return bytes(row[0])

    async def put(self, key: str, value: bytes, *, ttl_seconds: int | None = None) -> None:
        if ttl_seconds is not None and ttl_seconds < 1:
            raise ValueError("ttl_seconds must be positive")
        async with self._lock:
            await asyncio.to_thread(self._put_sync, key, value, ttl_seconds)

    def _put_sync(self, key: str, value: bytes, ttl_seconds: int | None) -> None:
        if not self._initialized:
            self._init()
        expires = None if ttl_seconds is None else time.time() + ttl_seconds
        with sqlite3.connect(self.path) as db:
            db.execute(
                "INSERT OR REPLACE INTO extraction_cache(key,value,expires) VALUES(?,?,?)",
                (key, value, expires),
            )

    async def delete(self, key: str) -> None:
        async with self._lock:
            await asyncio.to_thread(self._delete_sync, key)

    def _delete_sync(self, key: str) -> None:
        if not self._initialized:
            self._init()
        with sqlite3.connect(self.path) as db:
            db.execute("DELETE FROM extraction_cache WHERE key=?", (key,))
