"""Durable SQLite FTS5 destination and query collection."""

from __future__ import annotations

import asyncio
import hashlib
import json
import re
import sqlite3
import threading
from collections.abc import Mapping, Sequence
from contextlib import closing
from pathlib import Path
from typing import Any

from ingestion_graph.connectors.base import (
    CheckResult,
    ConnectorCapabilities,
    ConnectorSpec,
    Destination,
)
from ingestion_graph.models import Envelope, Operation
from ingestion_graph.query import QueryHit, QueryRequest, QueryResult, QueryStore


class SQLiteCollection(Destination, QueryStore):
    """Materialize envelopes as an atomic, searchable current view.

    The logical key is ``(source, stream, id)``. Replayed versions are ignored,
    UPSERT replaces the current searchable document, and DELETE removes it.
    """

    idempotent = True

    @classmethod
    def manifest(cls) -> ConnectorSpec:
        return ConnectorSpec(
            name="sqlite",
            version="1.0.0",
            config_schema={
                "type": "object",
                "properties": {
                    "path": {
                        "type": "string",
                        "minLength": 1,
                        "default": ".ingestion/query.db",
                        "description": "Path to the durable SQLite query collection.",
                    }
                },
                "additionalProperties": False,
            },
            capabilities=ConnectorCapabilities(
                incremental=True,
                resumable_full_refresh=True,
                deletes=True,
            ),
        )

    def spec(self) -> ConnectorSpec:
        return self.manifest()

    def __init__(self, path: str | Path = ".ingestion/query.db") -> None:
        self.path = Path(path)
        self._initialized = False
        self._init_lock = threading.Lock()
        self._lock = asyncio.Lock()

    def _connect(self) -> sqlite3.Connection:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(self.path, timeout=30)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA synchronous=FULL")
        connection.execute("PRAGMA foreign_keys=ON")
        return connection

    def _initialize(self) -> None:
        with self._init_lock:
            if self._initialized:
                return
            with closing(self._connect()) as connection, connection:
                connection.executescript(
                    """
                    CREATE TABLE IF NOT EXISTS collection_records (
                        rowid INTEGER PRIMARY KEY,
                        source TEXT NOT NULL,
                        stream TEXT NOT NULL,
                        record_id TEXT NOT NULL,
                        version_hash TEXT NOT NULL,
                        envelope_json TEXT NOT NULL,
                        searchable_text TEXT NOT NULL,
                        updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE (source, stream, record_id)
                    );
                    CREATE VIRTUAL TABLE IF NOT EXISTS collection_fts USING fts5(
                        searchable_text,
                        tokenize='unicode61 remove_diacritics 2'
                    );
                    """
                )
            self._initialized = True

    async def check(self) -> CheckResult:
        try:
            await asyncio.to_thread(self._initialize)
            return CheckResult(True, "SQLite FTS5 collection is ready")
        except (OSError, sqlite3.Error) as exc:
            return CheckResult(False, str(exc))

    async def write(self, records: Sequence[Envelope]) -> int:
        async with self._lock:
            return await asyncio.to_thread(self._write_sync, list(records))

    def _write_sync(self, records: list[Envelope]) -> int:
        self._initialize()
        applied = 0
        with closing(self._connect()) as connection, connection:
            connection.execute("BEGIN IMMEDIATE")
            for envelope in records:
                existing = connection.execute(
                    """
                    SELECT rowid, version_hash
                    FROM collection_records
                    WHERE source=? AND stream=? AND record_id=?
                    """,
                    (envelope.source, envelope.stream, envelope.id),
                ).fetchone()
                if envelope.operation is Operation.DELETE:
                    if existing is None:
                        continue
                    connection.execute(
                        "DELETE FROM collection_fts WHERE rowid=?", (existing["rowid"],)
                    )
                    connection.execute(
                        "DELETE FROM collection_records WHERE rowid=?", (existing["rowid"],)
                    )
                    applied += 1
                    continue

                serialized = envelope.to_dict()
                version_hash = _version_hash(serialized)
                if existing is not None and existing["version_hash"] == version_hash:
                    continue
                envelope_json = json.dumps(serialized, sort_keys=True, separators=(",", ":"))
                searchable_text = _searchable_text(serialized)
                if existing is None:
                    cursor = connection.execute(
                        """
                        INSERT INTO collection_records (
                            source, stream, record_id, version_hash, envelope_json, searchable_text
                        ) VALUES (?, ?, ?, ?, ?, ?)
                        """,
                        (
                            envelope.source,
                            envelope.stream,
                            envelope.id,
                            version_hash,
                            envelope_json,
                            searchable_text,
                        ),
                    )
                    if cursor.lastrowid is None:
                        raise sqlite3.DatabaseError("SQLite did not return an inserted row ID")
                    rowid = cursor.lastrowid
                else:
                    rowid = int(existing["rowid"])
                    connection.execute("DELETE FROM collection_fts WHERE rowid=?", (rowid,))
                    connection.execute(
                        """
                        UPDATE collection_records SET
                            version_hash=?, envelope_json=?, searchable_text=?,
                            updated_at=CURRENT_TIMESTAMP
                        WHERE rowid=?
                        """,
                        (version_hash, envelope_json, searchable_text, rowid),
                    )
                connection.execute(
                    "INSERT INTO collection_fts(rowid, searchable_text) VALUES (?, ?)",
                    (rowid, searchable_text),
                )
                applied += 1
            connection.commit()
        return applied

    async def flush(self) -> None:
        # Each write is already a FULL-synchronous transaction.
        return None

    async def get(self, source: str, stream: str, record_id: str) -> Envelope | None:
        return await asyncio.to_thread(self._get_sync, source, stream, record_id)

    def _get_sync(self, source: str, stream: str, record_id: str) -> Envelope | None:
        self._initialize()
        with closing(self._connect()) as connection, connection:
            row = connection.execute(
                """
                SELECT envelope_json FROM collection_records
                WHERE source=? AND stream=? AND record_id=?
                """,
                (source, stream, record_id),
            ).fetchone()
        if row is None:
            return None
        return Envelope.from_dict(json.loads(row["envelope_json"]))

    async def query(self, request: QueryRequest) -> QueryResult:
        return await asyncio.to_thread(self._query_sync, request)

    def _query_sync(self, request: QueryRequest) -> QueryResult:
        self._initialize()
        filters: list[str] = []
        parameters: list[Any] = []
        if request.source is not None:
            filters.append("records.source=?")
            parameters.append(request.source)
        if request.stream is not None:
            filters.append("records.stream=?")
            parameters.append(request.stream)
        filter_sql = " AND ".join(filters)
        with closing(self._connect()) as connection, connection:
            if request.text is not None and request.text.strip():
                match = _fts_query(request.text)
                fts_where = "WHERE collection_fts MATCH ?"
                if filter_sql:
                    fts_where += " AND " + filter_sql
                query_parameters = [match, *parameters]
                total = int(
                    connection.execute(
                        """
                        SELECT count(*) FROM collection_fts
                        JOIN collection_records AS records ON records.rowid=collection_fts.rowid
                        """
                        + fts_where,
                        query_parameters,
                    ).fetchone()[0]
                )
                rows = connection.execute(
                    """
                    SELECT records.envelope_json, -bm25(collection_fts) AS score
                    FROM collection_fts
                    JOIN collection_records AS records ON records.rowid=collection_fts.rowid
                    """
                    + fts_where
                    + " ORDER BY bm25(collection_fts), records.rowid LIMIT ? OFFSET ?",
                    [*query_parameters, request.limit, request.offset],
                ).fetchall()
            else:
                plain_where = "WHERE " + filter_sql if filter_sql else ""
                total = int(
                    connection.execute(
                        "SELECT count(*) FROM collection_records AS records " + plain_where,
                        parameters,
                    ).fetchone()[0]
                )
                rows = connection.execute(
                    """
                    SELECT records.envelope_json, 0.0 AS score
                    FROM collection_records AS records
                    """
                    + plain_where
                    + " ORDER BY records.updated_at DESC, records.rowid DESC LIMIT ? OFFSET ?",
                    [*parameters, request.limit, request.offset],
                ).fetchall()
        hits = tuple(
            QueryHit(
                Envelope.from_dict(json.loads(row["envelope_json"])),
                float(row["score"]),
            )
            for row in rows
        )
        return QueryResult(request=request, hits=hits, total=total)


def _version_hash(value: Mapping[str, Any]) -> str:
    semantic = dict(value)
    semantic.pop("observed_at", None)
    semantic.pop("provenance", None)
    encoded = json.dumps(semantic, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


def _searchable_text(value: Any) -> str:
    parts: list[str] = []

    def collect(item: Any) -> None:
        if isinstance(item, str):
            parts.append(item)
        elif isinstance(item, Mapping):
            for key, child in item.items():
                if key not in {"observed_at", "provenance", "kind"}:
                    collect(child)
        elif isinstance(item, (list, tuple)):
            for child in item:
                collect(child)
        elif item is not None and not isinstance(item, bool):
            parts.append(str(item))

    collect(value)
    return " ".join(parts)


def _fts_query(text: str) -> str:
    # Treat user input as terms, not as raw FTS syntax. This makes punctuation,
    # quotes, and operators safe and predictable for interactive CLI use.
    terms = re.findall(r"\w+", text, flags=re.UNICODE)
    if not terms:
        raise ValueError("Query text must contain at least one searchable character")
    return " AND ".join(f'"{term.replace(chr(34), chr(34) * 2)}"' for term in terms)
