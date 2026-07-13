"""Shared, optional PostgreSQL connector support."""

from __future__ import annotations

import base64
import json
import math
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, time
from decimal import Decimal
from typing import Any
from uuid import UUID

from ingestion_graph.errors import (
    AuthenticationError,
    ConfigurationError,
    PermissionDeniedError,
)
from ingestion_graph.secrets import EnvSecretProvider, SecretProvider, SecretRef

try:
    import asyncpg  # type: ignore[import-untyped]
except ImportError:  # pragma: no cover - exercised by optional-dependency tests
    asyncpg = None


_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


@dataclass(frozen=True, slots=True)
class PostgresConnection:
    host: str
    port: int
    database: str
    username: str
    password: SecretRef
    secret_provider: SecretProvider
    connect_timeout: float = 10.0
    command_timeout: float = 60.0

    @classmethod
    def create(
        cls,
        host: str,
        port: int,
        database: str,
        username: str,
        password: SecretRef,
        *,
        secret_provider: SecretProvider | None = None,
        connect_timeout: float = 10.0,
        command_timeout: float = 60.0,
    ) -> PostgresConnection:
        if not isinstance(host, str) or not host.strip():
            raise ConfigurationError("PostgreSQL host must not be empty")
        if isinstance(port, bool) or not isinstance(port, int) or not 1 <= port <= 65535:
            raise ConfigurationError("PostgreSQL port must be between 1 and 65535")
        if not isinstance(database, str) or not database.strip():
            raise ConfigurationError("PostgreSQL database must not be empty")
        if not isinstance(username, str) or not username.strip():
            raise ConfigurationError("PostgreSQL username must not be empty")
        if not isinstance(password, SecretRef) or not password.key:
            raise ConfigurationError("PostgreSQL password must be a SecretRef")
        if connect_timeout <= 0 or command_timeout <= 0:
            raise ConfigurationError("PostgreSQL timeouts must be positive")
        return cls(
            host=host.strip(),
            port=port,
            database=database.strip(),
            username=username.strip(),
            password=password,
            secret_provider=secret_provider or EnvSecretProvider(),
            connect_timeout=float(connect_timeout),
            command_timeout=float(command_timeout),
        )

    async def connect(self) -> Any:
        if asyncpg is None:
            raise ConfigurationError(
                "PostgreSQL support requires: pip install 'ingestion-graph[postgres]'"
            )
        secret = self.secret_provider.resolve(self.password)
        try:
            return await asyncpg.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.username,
                password=str(secret),
                timeout=self.connect_timeout,
                command_timeout=self.command_timeout,
            )
        except Exception as exc:
            raise safe_postgres_error("PostgreSQL connection failed", exc) from exc


def safe_postgres_error(prefix: str, exc: Exception) -> Exception:
    """Map driver failures without returning a DSN, query, or secret."""
    error_name = type(exc).__name__
    if error_name in {
        "InvalidPasswordError",
        "InvalidAuthorizationSpecificationError",
    }:
        return AuthenticationError(f"{prefix}: authentication failed")
    if error_name in {"InsufficientPrivilegeError", "InvalidGrantOperationError"}:
        return PermissionDeniedError(f"{prefix}: permission denied")
    return ConfigurationError(f"{prefix}: {error_name}")


def normalize_select_query(query: str) -> str:
    if not isinstance(query, str) or not query.strip():
        raise ConfigurationError("PostgreSQL query must not be empty")
    normalized = query.strip()
    if normalized.endswith(";"):
        normalized = normalized[:-1].rstrip()
    if ";" in normalized:
        raise ConfigurationError("PostgreSQL source accepts exactly one SQL statement")
    if re.match(r"(?is)^select\b", normalized) is None:
        raise ConfigurationError("PostgreSQL source accepts SELECT statements only")
    return normalized


def identifier(value: str, *, label: str = "identifier") -> str:
    if not isinstance(value, str) or _IDENTIFIER.fullmatch(value) is None:
        raise ConfigurationError(f"Invalid PostgreSQL {label}: {value!r}")
    return value


def identifier_path(value: str, *, label: str = "identifier") -> tuple[str, ...]:
    if not isinstance(value, str):
        raise ConfigurationError(f"Invalid PostgreSQL {label}")
    parts = tuple(value.split("."))
    if len(parts) not in {1, 2}:
        raise ConfigurationError(f"PostgreSQL {label} must be table or schema.table")
    return tuple(identifier(part, label=label) for part in parts)


def quote_identifier(value: str) -> str:
    return f'"{identifier(value)}"'


def quote_path(parts: Sequence[str]) -> str:
    return ".".join(quote_identifier(part) for part in parts)


def encode_scalar(value: Any) -> Mapping[str, Any]:
    if value is None:
        raise ConfigurationError("PostgreSQL checkpoint keys must not be null")
    if isinstance(value, bool):
        return {"type": "bool", "value": value}
    if isinstance(value, int):
        return {"type": "int", "value": str(value)}
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ConfigurationError("PostgreSQL checkpoint floats must be finite")
        return {"type": "float", "value": repr(value)}
    if isinstance(value, str):
        return {"type": "str", "value": value}
    if isinstance(value, Decimal):
        return {"type": "decimal", "value": str(value)}
    if isinstance(value, datetime):
        return {"type": "datetime", "value": value.isoformat()}
    if isinstance(value, date):
        return {"type": "date", "value": value.isoformat()}
    if isinstance(value, time):
        return {"type": "time", "value": value.isoformat()}
    if isinstance(value, UUID):
        return {"type": "uuid", "value": str(value)}
    if isinstance(value, bytes):
        return {"type": "bytes", "value": base64.b64encode(value).decode("ascii")}
    raise ConfigurationError(f"Unsupported PostgreSQL checkpoint type: {type(value).__name__}")


def decode_scalar(value: Any) -> Any:
    if not isinstance(value, Mapping):
        raise ConfigurationError("PostgreSQL checkpoint components must be tagged objects")
    kind = value.get("type")
    raw = value.get("value")
    try:
        if kind == "bool" and isinstance(raw, bool):
            return raw
        if kind == "int" and isinstance(raw, str):
            return int(raw)
        if kind == "float" and isinstance(raw, str):
            result = float(raw)
            if math.isfinite(result):
                return result
        if kind == "str" and isinstance(raw, str):
            return raw
        if kind == "decimal" and isinstance(raw, str):
            return Decimal(raw)
        if kind == "datetime" and isinstance(raw, str):
            return datetime.fromisoformat(raw)
        if kind == "date" and isinstance(raw, str):
            return date.fromisoformat(raw)
        if kind == "time" and isinstance(raw, str):
            return time.fromisoformat(raw)
        if kind == "uuid" and isinstance(raw, str):
            return UUID(raw)
        if kind == "bytes" and isinstance(raw, str):
            return base64.b64decode(raw, validate=True)
    except (ValueError, TypeError) as exc:
        raise ConfigurationError("Invalid PostgreSQL checkpoint scalar") from exc
    raise ConfigurationError("Invalid PostgreSQL checkpoint scalar")


def json_value(value: Any) -> Any:
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, (Decimal, UUID)):
        return str(value)
    if isinstance(value, (datetime, date, time)):
        return value.isoformat()
    if isinstance(value, bytes):
        return base64.b64encode(value).decode("ascii")
    if isinstance(value, Mapping):
        return {str(key): json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [json_value(item) for item in value]
    raise ConfigurationError(f"Unsupported PostgreSQL value type: {type(value).__name__}")


def canonical_typed(values: Sequence[Any]) -> str:
    encoded = [dict(encode_scalar(value)) for value in values]
    return json.dumps(encoded, sort_keys=True, separators=(",", ":"))


CONNECTION_SCHEMA: Mapping[str, Mapping[str, Any]] = {
    "host": {"type": "string", "minLength": 1},
    "port": {"type": "integer", "minimum": 1, "maximum": 65535, "default": 5432},
    "database": {"type": "string", "minLength": 1},
    "username": {"type": "string", "minLength": 1},
    "password": {"type": "string", "format": "secret-ref"},
}


CONNECTION_REQUIRED = ("host", "database", "username", "password")
