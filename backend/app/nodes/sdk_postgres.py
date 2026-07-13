"""Studio-only saved-connection bridge for SDK PostgreSQL connectors."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from ingestion_graph.secrets import SecretRef, SecretValue


class SavedPostgresSecrets:
    name = "saved-connection"

    def __init__(self, values: Mapping[str, Any]) -> None:
        self._values = values

    def resolve(self, reference: SecretRef) -> SecretValue:
        if reference.provider != self.name:
            raise ValueError("PostgreSQL secret reference uses the wrong provider")
        value = self._values.get(reference.key)
        if not isinstance(value, str) or not value:
            raise ValueError("Saved PostgreSQL connection has no password")
        return SecretValue(value)


def saved_postgres_connection(
    config: Mapping[str, Any], state: Mapping[str, Any], *, node_label: str
) -> tuple[Mapping[str, Any], SavedPostgresSecrets, SecretRef]:
    connection_id = config.get("connection_id")
    connections = state.get("connections", {})
    connection = connections.get(connection_id) if isinstance(connections, Mapping) else None
    if not connection_id:
        raise ValueError(
            f"{node_label} requires connection_id; select an encrypted saved connection"
        )
    if not isinstance(connection, Mapping):
        raise ValueError(f"Saved connection not available: {connection_id}")
    required = {
        "host": connection.get("host"),
        "database": connection.get("database"),
        "username": connection.get("username") or connection.get("user"),
        "password": connection.get("password"),
    }
    if any(value in (None, "") for value in required.values()):
        raise ValueError("Saved PostgreSQL connection is incomplete")
    provider = SavedPostgresSecrets(connection)
    return connection, provider, SecretRef("password", provider=provider.name)
