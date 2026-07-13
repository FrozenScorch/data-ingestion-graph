"""Studio adapter for the reusable SDK PostgreSQL destination."""

from __future__ import annotations

import logging
from collections.abc import Mapping
from typing import Any

from app.nodes.base import BaseNode, NodeContext, NodeResult, PortDataType, PortDef
from app.nodes.sdk_manifest import (
    ManifestFieldProjection,
    project_manifest_config_schema,
    serialize_connector_manifest,
)
from app.nodes.sdk_postgres import saved_postgres_connection
from ingestion_graph.destinations import PostgresDestination
from ingestion_graph.errors import ConfigurationError
from ingestion_graph.models import Envelope, Operation, RecordPayload, stable_record_id
from ingestion_graph.postgres import canonical_typed

logger = logging.getLogger(__name__)


class DatabaseWriterNode(BaseNode):
    @property
    def implementation(self) -> str:
        return "sdk-adapter"

    @property
    def sdk_component(self) -> str:
        return "ingestion_graph.destinations.PostgresDestination"

    @property
    def connector_manifest(self) -> dict[str, Any]:
        return serialize_connector_manifest(PostgresDestination.manifest())

    @property
    def node_type(self) -> str:
        return "database_writer"

    @property
    def display_name(self) -> str:
        return "Database Writer"

    @property
    def category(self) -> str:
        return "output"

    @property
    def description(self) -> str:
        return "Write rows transactionally through the reusable PostgreSQL SDK destination"

    @property
    def inputs(self) -> list[PortDef]:
        return [PortDef(name="table", data_type=PortDataType.TABLE, required=True)]

    @property
    def outputs(self) -> list[PortDef]:
        return [PortDef(name="count", data_type=PortDataType.ANY, label="Rows Affected")]

    @property
    def config_schema(self) -> dict[str, Any]:
        return project_manifest_config_schema(
            PostgresDestination.manifest(),
            fields=(
                ManifestFieldProjection(
                    source_field="target",
                    target_field="table_name",
                    overrides={
                        "default": "output_data",
                        "description": "Target table name to write to",
                    },
                ),
                ManifestFieldProjection(
                    source_field="mode",
                    target_field="mode",
                    overrides={
                        "enum": ["insert", "upsert", "replace"],
                        "description": (
                            "Write mode: insert, upsert, or atomic truncate-and-replace"
                        ),
                    },
                ),
                ManifestFieldProjection(
                    source_field="batch_size",
                    target_field="batch_size",
                ),
            ),
            omitted={
                "host": "Studio resolves host from an encrypted saved connection",
                "port": "Studio resolves port from an encrypted saved connection",
                "database": "Studio resolves database from an encrypted saved connection",
                "username": "Studio resolves username from an encrypted saved connection",
                "password": "Studio resolves password from an encrypted saved connection",
                "key_fields": "Studio preserves the legacy comma-separated upsert_key field",
                "ledger_table": "Studio uses the SDK-managed replay ledger name",
            },
            studio_properties={
                "connection_id": {
                    "type": "string",
                    "format": "connection-ref",
                    "connection_type": "postgres",
                    "description": "Saved PostgreSQL connection ID",
                },
                "upsert_key": {
                    "type": "string",
                    "description": "Comma-separated columns for upsert conflict resolution",
                },
            },
            studio_required=("connection_id",),
        )

    @staticmethod
    def _build_connection_url(context: NodeContext) -> str:
        connection, _, _ = saved_postgres_connection(
            context.config, context.state, node_label="Database writer"
        )
        return (
            f"postgresql://{connection.get('username') or connection.get('user')}@"
            f"{connection['host']}:{int(connection.get('port', 5432))}/{connection['database']}"
        )

    async def execute(self, context: NodeContext) -> NodeResult:
        table_name = str(context.config.get("table_name") or "")
        mode = str(context.config.get("mode") or "insert")
        try:
            if not table_name:
                raise ValueError("Missing required config: table_name")
            batch_size = int(context.config.get("batch_size", 500))
            if mode not in {"insert", "upsert", "replace"}:
                raise ValueError("Invalid database write mode")
            table_input = context.input_data.get("table", context.input_data)
            if not isinstance(table_input, Mapping):
                raise ValueError("Database writer table input must be an object")
            raw_rows = table_input.get("rows", [])
            if not isinstance(raw_rows, list) or any(
                not isinstance(row, Mapping) for row in raw_rows
            ):
                raise ValueError("Database writer input rows must be objects")
            rows = [dict(row) for row in raw_rows]
            raw_type_hints = table_input.get("postgres_type_hints")
            if raw_type_hints is None:
                postgres_type_hints: list[dict[str, Any]] = [{} for _ in rows]
            elif (
                not isinstance(raw_type_hints, list)
                or len(raw_type_hints) != len(rows)
                or any(not isinstance(item, Mapping) for item in raw_type_hints)
            ):
                raise ValueError(
                    "Database writer postgres_type_hints must align one-to-one with rows"
                )
            else:
                postgres_type_hints = [dict(item) for item in raw_type_hints]
            upsert_keys = tuple(
                key.strip()
                for key in str(context.config.get("upsert_key") or "").split(",")
                if key.strip()
            )
            if mode == "upsert" and not upsert_keys:
                raise ValueError("upsert mode requires at least one upsert_key")
            connection, provider, password = saved_postgres_connection(
                context.config, context.state, node_label="Database writer"
            )
            destination = PostgresDestination(
                str(connection["host"]),
                int(connection.get("port", 5432)),
                str(connection["database"]),
                str(connection.get("username") or connection.get("user")),
                password,
                target=table_name,
                mode="upsert" if mode == "upsert" else "insert",
                key_fields=upsert_keys,
                batch_size=batch_size,
                secret_provider=provider,
            )
            envelopes = _rows_to_envelopes(
                rows,
                context=context,
                table_name=table_name,
                mode=mode,
                key_fields=upsert_keys,
                postgres_type_hints=postgres_type_hints,
            )
            written = (
                await destination.replace(envelopes)
                if mode == "replace"
                else await destination.write(envelopes)
            )
            await destination.flush()
            return NodeResult(
                success=True,
                output_data={"rows_affected": written, "table": table_name},
                items_processed=written,
                metadata={
                    "mode": mode,
                    "batch_size": batch_size,
                    "batches": (len(rows) + batch_size - 1) // batch_size if rows else 0,
                },
            )
        except (ConfigurationError, ValueError) as exc:
            return NodeResult(success=False, error_message=str(exc))
        except Exception as exc:
            logger.error("DatabaseWriterNode SDK adapter failed: %s", type(exc).__name__)
            return NodeResult(
                success=False,
                error_message=f"Database write failed: {type(exc).__name__}",
            )


def _rows_to_envelopes(
    rows: list[dict[str, Any]],
    *,
    context: NodeContext,
    table_name: str,
    mode: str,
    key_fields: tuple[str, ...],
    postgres_type_hints: list[dict[str, Any]],
) -> list[Envelope]:
    result: list[Envelope] = []
    for index, row in enumerate(rows):
        if mode == "upsert":
            try:
                key_values = tuple(row[field] for field in key_fields)
            except KeyError as exc:
                raise ValueError(f"Missing upsert key: {exc.args[0]}") from exc
            native_id = canonical_typed(key_values)
            key = {field: row[field] for field in key_fields}
        else:
            native_id = f"{context.run_id}:{context.node_id}:{index}"
            key = {}
        result.append(
            Envelope(
                id=stable_record_id("studio", table_name, native_id),
                source="studio",
                stream=table_name,
                operation=Operation.UPSERT,
                cursor=str(index),
                payload=RecordPayload(row),
                metadata={
                    "key": key,
                    "run_id": context.run_id,
                    "node_id": context.node_id,
                    "ingestion_graph.postgres_types": postgres_type_hints[index],
                },
                provenance={"adapter": "backend.database_writer"},
            )
        )
    return result


def register() -> None:
    from app.nodes.registry import register_node

    register_node(DatabaseWriterNode())
