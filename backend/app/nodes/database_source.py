"""Studio adapter for the reusable SDK PostgreSQL query source."""

from __future__ import annotations

import logging
from typing import Any

from app.nodes.base import BaseNode, NodeContext, NodeResult, PortDataType, PortDef
from app.nodes.sdk_manifest import (
    ManifestFieldProjection,
    project_manifest_config_schema,
    serialize_connector_manifest,
)
from app.nodes.sdk_postgres import saved_postgres_connection
from ingestion_graph.errors import ConfigurationError
from ingestion_graph.messages import RecordMessage
from ingestion_graph.models import RecordPayload
from ingestion_graph.sources import PostgresSource

logger = logging.getLogger(__name__)


class DatabaseSourceNode(BaseNode):
    @property
    def implementation(self) -> str:
        return "sdk-adapter"

    @property
    def sdk_component(self) -> str:
        return "ingestion_graph.sources.PostgresSource"

    @property
    def connector_manifest(self) -> dict[str, Any]:
        return serialize_connector_manifest(PostgresSource.manifest())

    @property
    def node_type(self) -> str:
        return "database_source"

    @property
    def display_name(self) -> str:
        return "Database Source"

    @property
    def category(self) -> str:
        return "source"

    @property
    def description(self) -> str:
        return "Read a bounded PostgreSQL SELECT through the reusable SDK"

    @property
    def inputs(self) -> list[PortDef]:
        return []

    @property
    def outputs(self) -> list[PortDef]:
        return [PortDef(name="table", data_type=PortDataType.TABLE, label="Table Data")]

    @property
    def config_schema(self) -> dict[str, Any]:
        return project_manifest_config_schema(
            PostgresSource.manifest(),
            fields=(
                ManifestFieldProjection(
                    source_field="query",
                    target_field="query",
                    overrides={
                        "default": "SELECT * FROM table LIMIT 1000",
                        "description": "SQL query to execute (SELECT only)",
                    },
                ),
                ManifestFieldProjection(
                    source_field="max_records",
                    target_field="batch_size",
                    overrides={
                        "type": "integer",
                        "default": 1000,
                        "description": "Maximum rows returned by this Studio preview",
                    },
                ),
            ),
            omitted={
                "host": "Studio resolves host from an encrypted saved connection",
                "port": "Studio resolves port from an encrypted saved connection",
                "database": "Studio resolves database from an encrypted saved connection",
                "username": "Studio resolves username from an encrypted saved connection",
                "password": "Studio resolves password from an encrypted saved connection",
                "stream": "Studio uses a stable adapter-owned preview stream",
                "primary_key": "This legacy-compatible node is a bounded preview",
                "cursor_field": "This legacy-compatible node is a bounded preview",
                "page_size": "The Studio batch_size is the complete preview bound",
            },
            studio_properties={
                "connection_id": {
                    "type": "string",
                    "format": "connection-ref",
                    "connection_type": "postgres",
                    "description": "Saved PostgreSQL connection ID",
                }
            },
            studio_required=("connection_id",),
        )

    @staticmethod
    def _validate_query(query: str) -> None:
        # Retained for callers of the legacy validation helper; the SDK performs
        # the authoritative validation during connector construction.
        try:
            PostgresSource(
                "validation.invalid",
                5432,
                "validation",
                "validation",
                _validation_secret(),
                query=query,
                primary_key=(),
                max_records=1,
            )
        except ConfigurationError as exc:
            if "SELECT" in str(exc):
                raise ValueError("Only SELECT queries are allowed") from exc
            if "one SQL statement" in str(exc):
                raise ValueError("Multiple SQL statements are not allowed") from exc
            raise ValueError(str(exc)) from exc

    def _build_connection_url(self, context: NodeContext) -> str:
        # Compatibility diagnostic for older callers. Runtime execution never
        # serializes a password-bearing URL.
        connection, _, _ = saved_postgres_connection(
            context.config, context.state, node_label="Database source"
        )
        return (
            f"postgresql://{connection.get('username') or connection.get('user')}@"
            f"{connection['host']}:{int(connection.get('port', 5432))}/{connection['database']}"
        )

    async def execute(self, context: NodeContext) -> NodeResult:
        query = context.config.get("query", "SELECT * FROM table LIMIT 1000")
        try:
            batch_size = int(context.config.get("batch_size", 1000))
            connection, provider, password = saved_postgres_connection(
                context.config, context.state, node_label="Database source"
            )
            connector = PostgresSource(
                str(connection["host"]),
                int(connection.get("port", 5432)),
                str(connection["database"]),
                str(connection.get("username") or connection.get("user")),
                password,
                query=str(query),
                stream="studio_query",
                primary_key=(),
                max_records=batch_size,
                secret_provider=provider,
            )
            check = await connector.check()
            if not check.ok:
                return NodeResult(success=False, error_message="Database query check failed")
            descriptor = (await connector.discover())[0]
            rows: list[dict[str, Any]] = []
            async for message in connector.read(descriptor, {}):
                if isinstance(message, RecordMessage):
                    payload = message.envelope.payload
                    if not isinstance(payload, RecordPayload):
                        raise TypeError("PostgreSQL source emitted a non-row payload")
                    rows.append(dict(payload.data))
            columns = list(descriptor.json_schema.get("properties", {}))
            return NodeResult(
                success=True,
                output_data={"rows": rows, "row_count": len(rows), "columns": columns},
                items_processed=len(rows),
                metadata={"query": str(query), "batch_size": batch_size},
            )
        except (ConfigurationError, ValueError) as exc:
            return NodeResult(success=False, error_message=str(exc))
        except Exception as exc:
            logger.error("DatabaseSourceNode SDK adapter failed: %s", type(exc).__name__)
            return NodeResult(
                success=False,
                error_message=f"Database query failed: {type(exc).__name__}",
            )


def _validation_secret():
    from ingestion_graph.secrets import SecretRef

    return SecretRef("VALIDATION_ONLY")


def register() -> None:
    from app.nodes.registry import register_node

    register_node(DatabaseSourceNode())
