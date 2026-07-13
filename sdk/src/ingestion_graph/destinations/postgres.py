"""Transactional, replay-safe PostgreSQL row destination."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from ingestion_graph.connectors.base import (
    CheckResult,
    ConnectorCapabilities,
    ConnectorSpec,
    Destination,
)
from ingestion_graph.errors import ConfigurationError
from ingestion_graph.models import Envelope, Operation, RecordPayload
from ingestion_graph.postgres import (
    CONNECTION_REQUIRED,
    CONNECTION_SCHEMA,
    PostgresConnection,
    identifier,
    identifier_path,
    quote_identifier,
    quote_path,
    restore_transport_value,
    safe_postgres_error,
)
from ingestion_graph.secrets import SecretProvider, SecretRef


@dataclass(frozen=True, slots=True)
class _TargetIdentity:
    oid: int
    schema: str
    table: str

    @property
    def parts(self) -> tuple[str, str]:
        return self.schema, self.table

    @property
    def key(self) -> str:
        return str(self.oid)


class PostgresDestination(Destination):
    """Apply envelope events to an existing table exactly once per event identity.

    Every write and replace takes the same target-level advisory transaction
    lock. Target mutations and the applied-event ledger commit together.
    """

    idempotent = True

    def __init__(
        self,
        host: str,
        port: int,
        database: str,
        username: str,
        password: SecretRef,
        *,
        target: str,
        mode: str = "insert",
        key_fields: Sequence[str] = (),
        batch_size: int = 500,
        ledger_table: str = "_ingestion_graph_versions",
        secret_provider: SecretProvider | None = None,
    ) -> None:
        self.connection = PostgresConnection.create(
            host,
            port,
            database,
            username,
            password,
            secret_provider=secret_provider,
        )
        self.target_parts = identifier_path(target, label="target")
        self.target = ".".join(self.target_parts)
        if mode not in {"insert", "upsert"}:
            raise ConfigurationError("PostgreSQL destination mode must be insert or upsert")
        self.mode = mode
        self.key_fields = tuple(identifier(item, label="key field") for item in key_fields)
        if len(set(self.key_fields)) != len(self.key_fields):
            raise ConfigurationError("PostgreSQL key_fields must be unique")
        if mode == "upsert" and not self.key_fields:
            raise ConfigurationError("PostgreSQL upsert mode requires key_fields")
        if isinstance(batch_size, bool) or not isinstance(batch_size, int) or batch_size < 1:
            raise ConfigurationError("PostgreSQL batch_size must be positive")
        self.batch_size = batch_size
        self.ledger_table = identifier(ledger_table, label="ledger table")

    @classmethod
    def manifest(cls) -> ConnectorSpec:
        properties = {
            **CONNECTION_SCHEMA,
            "target": {
                "type": "string",
                "pattern": ("^[A-Za-z_][A-Za-z0-9_]*(\\.[A-Za-z_][A-Za-z0-9_]*)?$"),
            },
            "mode": {"type": "string", "enum": ["insert", "upsert"], "default": "insert"},
            "key_fields": {
                "type": "array",
                "items": {"type": "string", "pattern": "^[A-Za-z_][A-Za-z0-9_]*$"},
                "default": [],
                "uniqueItems": True,
            },
            "batch_size": {"type": "integer", "minimum": 1, "default": 500},
            "ledger_table": {
                "type": "string",
                "pattern": "^[A-Za-z_][A-Za-z0-9_]*$",
                "default": "_ingestion_graph_versions",
            },
        }
        return ConnectorSpec(
            name="postgres",
            version="1.0.0",
            config_schema={
                "type": "object",
                "properties": properties,
                "required": [*CONNECTION_REQUIRED, "target"],
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

    async def check(self) -> CheckResult:
        connection: Any = None
        try:
            connection = await self.connection.connect()
            async with connection.transaction():
                target = await self._resolve_target(connection)
                if target is None:
                    return CheckResult(False, "PostgreSQL target table does not exist")
                await self._ensure_ledger(connection, target)
                if self.mode == "upsert" and not await self._has_unique_key(connection, target):
                    return CheckResult(
                        False,
                        "PostgreSQL upsert key_fields must match a unique or primary constraint",
                    )
            return CheckResult(True, "PostgreSQL destination is ready")
        except Exception as exc:
            mapped = (
                exc
                if isinstance(exc, ConfigurationError)
                else safe_postgres_error("PostgreSQL destination check failed", exc)
            )
            return CheckResult(False, str(mapped))
        finally:
            if connection is not None:
                await connection.close()

    async def write(self, records: Sequence[Envelope]) -> int:
        prepared = self._prepare_records(records, allow_deletes=True)
        if not prepared:
            return 0
        connection: Any = None
        try:
            connection = await self.connection.connect()
            async with connection.transaction():
                target = await self._require_target(connection)
                await self._lock_target(connection, target)
                await self._ensure_ledger(connection, target)
                return await self._apply_prepared(connection, target, prepared)
        except ConfigurationError:
            raise
        except Exception as exc:
            raise safe_postgres_error("PostgreSQL destination write failed", exc) from exc
        finally:
            if connection is not None:
                await connection.close()

    async def replace(self, records: Sequence[Envelope]) -> int:
        """Atomically replace all target rows and scoped replay-ledger events."""
        prepared = self._prepare_records(records, allow_deletes=False)
        connection: Any = None
        try:
            connection = await self.connection.connect()
            async with connection.transaction():
                target = await self._require_target(connection)
                await self._lock_target(connection, target)
                await self._ensure_ledger(connection, target)
                ledger_parts = (target.schema, self.ledger_table)
                await connection.execute(f"TRUNCATE TABLE {quote_path(target.parts)}")
                await connection.execute(
                    f"DELETE FROM {quote_path(ledger_parts)} WHERE target_table=$1",
                    target.key,
                )
                return await self._apply_prepared(connection, target, prepared)
        except ConfigurationError:
            raise
        except Exception as exc:
            raise safe_postgres_error("PostgreSQL destination replace failed", exc) from exc
        finally:
            if connection is not None:
                await connection.close()

    async def reset(self) -> None:
        """Destructively clear the target and its replay ledger in one transaction."""
        await self.replace(())

    async def flush(self) -> None:
        return None

    def _prepare_records(
        self,
        records: Sequence[Envelope],
        *,
        allow_deletes: bool,
    ) -> list[tuple[Envelope, Mapping[str, Any] | None, str]]:
        prepared: list[tuple[Envelope, Mapping[str, Any] | None, str]] = []
        expected_columns: tuple[str, ...] | None = None
        for envelope in records:
            if not isinstance(envelope, Envelope):
                raise ConfigurationError("PostgreSQL destination records must be Envelopes")
            event_hash = _event_hash(envelope)
            raw_type_hints = envelope.metadata.get("ingestion_graph.postgres_types", {})
            if not isinstance(raw_type_hints, Mapping):
                raise ConfigurationError("PostgreSQL transport type hints must be an object")
            if envelope.operation is Operation.DELETE:
                if not allow_deletes:
                    raise ConfigurationError("PostgreSQL replace does not accept DELETE envelopes")
                if not self.key_fields:
                    raise ConfigurationError("PostgreSQL DELETE requires configured key_fields")
                raw_key = envelope.metadata.get("key")
                if not isinstance(raw_key, Mapping):
                    raise ConfigurationError("PostgreSQL DELETE requires metadata.key")
                key = {
                    field: restore_transport_value(raw_key.get(field), raw_type_hints.get(field))
                    for field in self.key_fields
                }
                if any(value is None for value in key.values()):
                    raise ConfigurationError(
                        "PostgreSQL DELETE key values must be complete and non-null"
                    )
                prepared.append((envelope, key, event_hash))
                continue
            if not isinstance(envelope.payload, RecordPayload):
                raise ConfigurationError(
                    "PostgreSQL UPSERT/SNAPSHOT envelopes require RecordPayload"
                )
            row = {
                str(column): restore_transport_value(value, raw_type_hints.get(str(column)))
                for column, value in envelope.payload.data.items()
            }
            if not row:
                raise ConfigurationError("PostgreSQL destination rows must not be empty")
            columns = tuple(str(column) for column in row)
            for column in columns:
                identifier(column, label="column")
            if expected_columns is None:
                expected_columns = columns
            elif columns != expected_columns:
                raise ConfigurationError(
                    "PostgreSQL destination rows must have identical ordered columns"
                )
            if any(field not in row or row[field] is None for field in self.key_fields):
                raise ConfigurationError("PostgreSQL key_fields must be present and non-null")
            prepared.append((envelope, row, event_hash))
        return prepared

    async def _apply_prepared(
        self,
        connection: Any,
        target: _TargetIdentity,
        prepared: Sequence[tuple[Envelope, Mapping[str, Any] | None, str]],
    ) -> int:
        applied = 0
        ledger_parts = (target.schema, self.ledger_table)
        for start in range(0, len(prepared), self.batch_size):
            for envelope, value, event_hash in prepared[start : start + self.batch_size]:
                exists = await connection.fetchval(
                    f"SELECT 1 FROM {quote_path(ledger_parts)} "
                    "WHERE target_table=$1 AND source=$2 AND stream=$3 "
                    "AND record_id=$4 AND event_hash=$5",
                    target.key,
                    envelope.source,
                    envelope.stream,
                    envelope.id,
                    event_hash,
                )
                if exists is not None:
                    continue
                changed = await self._apply_event(connection, target, envelope, value)
                await connection.execute(
                    f"INSERT INTO {quote_path(ledger_parts)} "
                    "(target_table, source, stream, record_id, event_hash) "
                    "VALUES ($1, $2, $3, $4, $5)",
                    target.key,
                    envelope.source,
                    envelope.stream,
                    envelope.id,
                    event_hash,
                )
                applied += changed
        return applied

    async def _apply_event(
        self,
        connection: Any,
        target_identity: _TargetIdentity,
        envelope: Envelope,
        value: Mapping[str, Any] | None,
    ) -> int:
        target = quote_path(target_identity.parts)
        if envelope.operation is Operation.DELETE:
            assert value is not None
            predicate = " AND ".join(
                f"{quote_identifier(field)}=${index}"
                for index, field in enumerate(self.key_fields, start=1)
            )
            status = await connection.execute(
                f"DELETE FROM {target} WHERE {predicate}",
                *(value[field] for field in self.key_fields),
            )
            return _affected_rows(status)
        assert value is not None
        columns = tuple(str(column) for column in value)
        quoted_columns = ", ".join(quote_identifier(column) for column in columns)
        placeholders = ", ".join(f"${index}" for index in range(1, len(columns) + 1))
        statement = f"INSERT INTO {target} ({quoted_columns}) VALUES ({placeholders})"
        if self.mode == "upsert":
            conflicts = ", ".join(quote_identifier(field) for field in self.key_fields)
            updates = [column for column in columns if column not in self.key_fields]
            action = "DO NOTHING"
            if updates:
                action = "DO UPDATE SET " + ", ".join(
                    f"{quote_identifier(column)}=EXCLUDED.{quote_identifier(column)}"
                    for column in updates
                )
            statement += f" ON CONFLICT ({conflicts}) {action}"
        status = await connection.execute(statement, *(value[column] for column in columns))
        return _affected_rows(status)

    async def _resolve_target(self, connection: Any) -> _TargetIdentity | None:
        row = await connection.fetchrow(
            """
            SELECT target.oid::bigint AS oid,
                   namespace.nspname AS schema_name,
                   target.relname AS table_name
            FROM pg_class AS target
            JOIN pg_namespace AS namespace ON namespace.oid=target.relnamespace
            WHERE target.oid=to_regclass($1) AND target.relkind IN ('r', 'p')
            """,
            self.target,
        )
        if row is None:
            return None
        return _TargetIdentity(
            oid=int(row["oid"]),
            schema=identifier(str(row["schema_name"]), label="resolved schema"),
            table=identifier(str(row["table_name"]), label="resolved table"),
        )

    async def _require_target(self, connection: Any) -> _TargetIdentity:
        target = await self._resolve_target(connection)
        if target is None:
            raise ConfigurationError("PostgreSQL target table does not exist")
        return target

    async def _ensure_ledger(self, connection: Any, target: _TargetIdentity) -> None:
        ledger_parts = (target.schema, self.ledger_table)
        await connection.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {quote_path(ledger_parts)} (
                target_table TEXT NOT NULL,
                source TEXT NOT NULL,
                stream TEXT NOT NULL,
                record_id TEXT NOT NULL,
                event_hash TEXT NOT NULL,
                applied_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (target_table, source, stream, record_id, event_hash)
            )
            """
        )

    async def _lock_target(self, connection: Any, target: _TargetIdentity) -> None:
        await connection.execute(
            "SELECT pg_advisory_xact_lock($1::bigint)",
            target.oid,
        )

    async def _has_unique_key(self, connection: Any, target: _TargetIdentity) -> bool:
        rows = await connection.fetch(
            """
            SELECT array_agg(attribute.attname ORDER BY key.ordinality) AS columns
            FROM pg_index AS index
            CROSS JOIN LATERAL unnest(index.indkey) WITH ORDINALITY AS key(attnum, ordinality)
            JOIN pg_attribute AS attribute
              ON attribute.attrelid=index.indrelid AND attribute.attnum=key.attnum
            WHERE index.indrelid=$1::oid
              AND index.indisunique
              AND index.indisvalid
              AND index.indimmediate
              AND index.indpred IS NULL
              AND index.indexprs IS NULL
              AND key.ordinality <= index.indnkeyatts
            GROUP BY index.indexrelid
            """,
            target.oid,
        )
        expected = frozenset(self.key_fields)
        return any(
            len(row["columns"]) == len(self.key_fields) and frozenset(row["columns"]) == expected
            for row in rows
        )


def _event_hash(envelope: Envelope) -> str:
    value = envelope.to_dict()
    value.pop("observed_at", None)
    value.pop("provenance", None)
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


def _affected_rows(status: str) -> int:
    try:
        value = int(status.rsplit(" ", 1)[-1])
    except (AttributeError, ValueError) as exc:
        raise ConfigurationError("PostgreSQL returned an invalid mutation status") from exc
    return max(value, 0)
