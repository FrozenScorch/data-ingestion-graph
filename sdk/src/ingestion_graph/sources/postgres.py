"""Read-only PostgreSQL query source with keyset checkpoints."""

from __future__ import annotations

import hashlib
import json
from collections.abc import AsyncIterator, Mapping, Sequence
from typing import Any

from ingestion_graph.connectors.base import (
    CheckResult,
    ConnectorCapabilities,
    ConnectorSpec,
    Source,
    StreamDescriptor,
)
from ingestion_graph.errors import ConfigurationError
from ingestion_graph.messages import RecordMessage, SourceMessage, StateMessage
from ingestion_graph.models import Envelope, Operation, RecordPayload, stable_record_id
from ingestion_graph.postgres import (
    CONNECTION_REQUIRED,
    CONNECTION_SCHEMA,
    PostgresConnection,
    canonical_typed,
    decode_scalar,
    encode_scalar,
    identifier,
    json_value,
    normalize_select_query,
    quote_identifier,
    safe_postgres_error,
)
from ingestion_graph.secrets import SecretProvider, SecretRef


class PostgresSource(Source):
    """Read a SELECT query as a bounded preview or resumable keyset stream.

    A primary key enables recurring full-refresh cycles. Adding a monotonic
    cursor field switches to incremental polling. Without a primary key the
    source is deliberately a bounded, non-resumable preview.
    """

    def __init__(
        self,
        host: str,
        port: int,
        database: str,
        username: str,
        password: SecretRef,
        *,
        query: str,
        stream: str = "query",
        primary_key: Sequence[str] = (),
        cursor_field: str | None = None,
        page_size: int = 1000,
        max_records: int | None = None,
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
        self.query = normalize_select_query(query)
        self.stream = identifier(stream, label="stream")
        self.primary_key = tuple(identifier(item, label="primary key") for item in primary_key)
        if len(set(self.primary_key)) != len(self.primary_key):
            raise ConfigurationError("PostgreSQL primary_key fields must be unique")
        self.cursor_field = (
            identifier(cursor_field, label="cursor field") if cursor_field is not None else None
        )
        if self.cursor_field is not None and not self.primary_key:
            raise ConfigurationError("PostgreSQL incremental mode requires primary_key fields")
        if isinstance(page_size, bool) or not isinstance(page_size, int) or page_size < 1:
            raise ConfigurationError("PostgreSQL page_size must be positive")
        if max_records is not None and (
            isinstance(max_records, bool) or not isinstance(max_records, int) or max_records < 1
        ):
            raise ConfigurationError("PostgreSQL max_records must be positive")
        if not self.primary_key and max_records is None:
            raise ConfigurationError(
                "PostgreSQL queries without primary_key require max_records for bounded preview"
            )
        self.page_size = page_size
        self.max_records = max_records
        self._fingerprint = self._config_fingerprint()

    @classmethod
    def manifest(cls) -> ConnectorSpec:
        properties = {
            **CONNECTION_SCHEMA,
            "query": {"type": "string", "minLength": 1, "format": "textarea"},
            "stream": {"type": "string", "default": "query"},
            "primary_key": {
                "type": "array",
                "items": {"type": "string", "pattern": "^[A-Za-z_][A-Za-z0-9_]*$"},
                "default": [],
                "uniqueItems": True,
            },
            "cursor_field": {
                "type": ["string", "null"],
                "pattern": "^[A-Za-z_][A-Za-z0-9_]*$",
                "default": None,
            },
            "page_size": {"type": "integer", "minimum": 1, "default": 1000},
            "max_records": {"type": ["integer", "null"], "minimum": 1, "default": None},
        }
        return ConnectorSpec(
            name="postgres",
            version="1.0.0",
            config_schema={
                "type": "object",
                "properties": properties,
                "required": [*CONNECTION_REQUIRED, "query"],
                "additionalProperties": False,
            },
            capabilities=ConnectorCapabilities(
                incremental=True,
                resumable_full_refresh=True,
                deletes=False,
                schema_discovery=True,
            ),
        )

    def spec(self) -> ConnectorSpec:
        return self.manifest()

    async def check(self) -> CheckResult:
        connection: Any = None
        try:
            connection = await self.connection.connect()
            async with connection.transaction(readonly=True):
                await connection.prepare(self._introspection_query())
            return CheckResult(True, "PostgreSQL query is readable")
        except Exception as exc:
            mapped = (
                exc
                if isinstance(exc, ConfigurationError)
                else safe_postgres_error("PostgreSQL source check failed", exc)
            )
            return CheckResult(False, str(mapped))
        finally:
            if connection is not None:
                await connection.close()

    async def discover(self) -> Sequence[StreamDescriptor]:
        connection: Any = None
        try:
            connection = await self.connection.connect()
            async with connection.transaction(readonly=True):
                statement = await connection.prepare(self._introspection_query())
                attributes = statement.get_attributes()
            properties = {
                attribute.name: _json_schema_for_type(attribute.type.name)
                for attribute in attributes
            }
            columns = set(properties)
            missing = (
                set(self.primary_key) | ({self.cursor_field} if self.cursor_field else set())
            ) - columns
            if missing:
                raise ConfigurationError(
                    f"PostgreSQL query does not return configured fields: {sorted(missing)}"
                )
            return [
                StreamDescriptor(
                    name=self.stream,
                    namespace="postgres.query",
                    json_schema={"type": "object", "properties": properties},
                    primary_key=self.primary_key,
                    cursor_field=(self.cursor_field,) if self.cursor_field else (),
                )
            ]
        except ConfigurationError:
            raise
        except Exception as exc:
            raise safe_postgres_error("PostgreSQL discovery failed", exc) from exc
        finally:
            if connection is not None:
                await connection.close()

    async def read(
        self,
        stream: StreamDescriptor,
        state: Mapping[str, Any] | None = None,
    ) -> AsyncIterator[SourceMessage]:
        if stream.name != self.stream:
            raise ConfigurationError(f"PostgreSQL stream {stream.name!r} is not configured")
        current = dict(state or {})
        if not self.primary_key:
            if current:
                raise ConfigurationError("Bounded PostgreSQL previews cannot resume saved state")
            async for message in self._read_preview():
                yield message
            return

        mode = "incremental" if self.cursor_field else "snapshot"
        if current and current.get("mode") != mode:
            raise ConfigurationError("PostgreSQL checkpoint mode does not match source config")
        if current and current.get("fingerprint") != self._fingerprint:
            raise ConfigurationError("PostgreSQL query configuration changed since checkpoint")
        encoded_after = current.get("after")
        after = _decode_tuple(encoded_after) if encoded_after is not None else None
        order_fields = (
            (self.cursor_field, *self.primary_key)
            if self.cursor_field is not None
            else self.primary_key
        )
        if after is not None and len(after) != len(order_fields):
            raise ConfigurationError("PostgreSQL checkpoint key length is invalid")
        cycle = _state_cycle(current)
        emitted = 0
        connection: Any = None
        try:
            connection = await self.connection.connect()
            async with connection.transaction(isolation="repeatable_read", readonly=True):
                while True:
                    remaining = None if self.max_records is None else self.max_records - emitted
                    if remaining is not None and remaining <= 0:
                        return
                    page_limit = (
                        min(self.page_size, remaining) if remaining is not None else self.page_size
                    )
                    sql, parameters = self._page_query(order_fields, after, page_limit + 1)
                    rows = list(await connection.fetch(sql, *parameters))
                    page = rows[:page_limit]
                    if not page:
                        if self.cursor_field is None:
                            yield StateMessage(
                                self.stream,
                                {
                                    "mode": "snapshot",
                                    "fingerprint": self._fingerprint,
                                    "cycle": cycle + 1,
                                },
                            )
                        elif not current:
                            yield StateMessage(
                                self.stream,
                                {"mode": mode, "fingerprint": self._fingerprint},
                            )
                        return
                    tuples = [_order_tuple(row, order_fields) for row in page]
                    if len(set(canonical_typed(item) for item in tuples)) != len(tuples):
                        raise ConfigurationError(
                            "PostgreSQL keyset fields do not form a unique order"
                        )
                    if (
                        len(rows) > page_limit
                        and _order_tuple(rows[page_limit], order_fields) == tuples[-1]
                    ):
                        raise ConfigurationError(
                            "PostgreSQL keyset fields are tied across a page boundary"
                        )
                    for row in page:
                        yield RecordMessage(self._envelope(row, order_fields, mode))
                    after = tuples[-1]
                    emitted += len(page)
                    checkpoint: dict[str, Any] = {
                        "mode": mode,
                        "fingerprint": self._fingerprint,
                        "after": [dict(encode_scalar(item)) for item in after],
                    }
                    if mode == "snapshot":
                        checkpoint["cycle"] = cycle
                    yield StateMessage(self.stream, checkpoint)
                    current = checkpoint
                    if len(page) < page_limit:
                        if mode == "snapshot":
                            yield StateMessage(
                                self.stream,
                                {
                                    "mode": "snapshot",
                                    "fingerprint": self._fingerprint,
                                    "cycle": cycle + 1,
                                },
                            )
                        return
        except ConfigurationError:
            raise
        except Exception as exc:
            raise safe_postgres_error("PostgreSQL read failed", exc) from exc
        finally:
            if connection is not None:
                await connection.close()

    async def _read_preview(self) -> AsyncIterator[SourceMessage]:
        connection: Any = None
        try:
            connection = await self.connection.connect()
            async with connection.transaction(isolation="repeatable_read", readonly=True):
                sql = f"SELECT * FROM ({self.query}) AS _ingestion_source LIMIT $1"
                rows = await connection.fetch(sql, self.max_records)
                for row in rows:
                    yield RecordMessage(self._envelope(row, (), "preview"))
                yield StateMessage(self.stream, {})
        except ConfigurationError:
            raise
        except Exception as exc:
            raise safe_postgres_error("PostgreSQL preview failed", exc) from exc
        finally:
            if connection is not None:
                await connection.close()

    def _page_query(
        self,
        order_fields: Sequence[str],
        after: Sequence[Any] | None,
        limit: int,
    ) -> tuple[str, list[Any]]:
        quoted = ", ".join(quote_identifier(field) for field in order_fields)
        parameters: list[Any] = []
        where = ""
        if after is not None:
            parameters.extend(after)
            placeholders = ", ".join(f"${index}" for index in range(1, len(after) + 1))
            where = f"WHERE ({quoted}) > ({placeholders})"
        parameters.append(limit)
        limit_parameter = f"${len(parameters)}"
        return (
            f"SELECT * FROM ({self.query}) AS _ingestion_source "
            f"{where} ORDER BY {quoted} LIMIT {limit_parameter}",
            parameters,
        )

    def _envelope(
        self,
        row: Mapping[str, Any],
        order_fields: Sequence[str],
        mode: str,
    ) -> Envelope:
        data = {str(key): json_value(value) for key, value in row.items()}
        key_values = tuple(row[field] for field in self.primary_key)
        native_id = (
            canonical_typed(key_values)
            if key_values
            else hashlib.sha256(
                json.dumps(data, sort_keys=True, separators=(",", ":")).encode()
            ).hexdigest()
        )
        order_values = tuple(row[field] for field in order_fields)
        cursor = canonical_typed(order_values) if order_values else None
        return Envelope(
            id=stable_record_id("postgres", self.stream, native_id),
            source="postgres",
            stream=self.stream,
            payload=RecordPayload(data),
            operation=Operation.UPSERT if mode == "incremental" else Operation.SNAPSHOT,
            cursor=cursor,
            metadata={
                "key": {field: json_value(row[field]) for field in self.primary_key},
                "mode": mode,
            },
            provenance={"connector": "postgres"},
        )

    def _introspection_query(self) -> str:
        return f"SELECT * FROM ({self.query}) AS _ingestion_source LIMIT 0"

    def _config_fingerprint(self) -> str:
        value = {
            "query": self.query,
            "stream": self.stream,
            "primary_key": self.primary_key,
            "cursor_field": self.cursor_field,
        }
        return hashlib.sha256(
            json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()


def _decode_tuple(value: Any) -> tuple[Any, ...]:
    if not isinstance(value, list):
        raise ConfigurationError("PostgreSQL checkpoint after must be an array")
    return tuple(decode_scalar(item) for item in value)


def _order_tuple(row: Mapping[str, Any], fields: Sequence[str]) -> tuple[Any, ...]:
    try:
        values = tuple(row[field] for field in fields)
    except KeyError as exc:
        raise ConfigurationError(f"PostgreSQL query omitted keyset field {exc.args[0]!r}") from exc
    for value in values:
        encode_scalar(value)
    return values


def _state_cycle(state: Mapping[str, Any]) -> int:
    value = state.get("cycle", 0)
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ConfigurationError("PostgreSQL checkpoint cycle must be non-negative")
    return value


def _json_schema_for_type(type_name: str) -> Mapping[str, Any]:
    lowered = type_name.lower()
    if lowered in {"int2", "int4", "int8", "serial", "bigserial"}:
        return {"type": ["integer", "null"]}
    if lowered in {"float4", "float8", "numeric", "decimal"}:
        return {"type": ["number", "string", "null"]}
    if lowered in {"bool", "boolean"}:
        return {"type": ["boolean", "null"]}
    if lowered in {"json", "jsonb"}:
        return {}
    if "timestamp" in lowered or lowered == "date":
        return {"type": ["string", "null"], "format": "date-time"}
    return {"type": ["string", "null"]}
