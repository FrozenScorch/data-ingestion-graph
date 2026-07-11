import re as _re_dw

"""
DatabaseWriter node: write data to PostgreSQL.

Writes row data to a PostgreSQL table using SQLAlchemy Core for
efficient bulk operations. Uses the app's own database connection.
Input: rows data from DatabaseSourceNode or TransformNode
Output: {rows_affected: N, table: "..."}
"""
import logging
from typing import Any

from sqlalchemy import URL, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.nodes.base import BaseNode, NodeContext, NodeResult, PortDef, PortDataType

logger = logging.getLogger(__name__)

_SQL_IDENTIFIER_RE = _re_dw.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")


class DatabaseWriterNode(BaseNode):
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
        return "Write data to the app database (PostgreSQL)"

    @property
    def inputs(self) -> list[PortDef]:
        return [PortDef(name="table", data_type=PortDataType.TABLE, required=True)]

    @property
    def outputs(self) -> list[PortDef]:
        return [PortDef(name="count", data_type=PortDataType.ANY, label="Rows Affected")]

    @property
    def config_schema(self) -> dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "connection_id": {
                    "type": "string",
                    "format": "connection-ref",
                    "connection_type": "postgres",
                    "description": "Saved PostgreSQL connection ID",
                },
                "table_name": {
                    "type": "string",
                    "default": "output_data",
                    "description": "Target table name to write to",
                },
                "mode": {
                    "type": "string",
                    "enum": ["insert", "upsert", "replace"],
                    "default": "insert",
                    "description": "Write mode: insert, upsert (update on conflict), or replace (truncate + insert)",
                },
                "batch_size": {
                    "type": "integer",
                    "default": 500,
                    "minimum": 1,
                    "description": "Number of rows per insert batch",
                },
                "upsert_key": {
                    "type": "string",
                    "description": "Comma-separated column names for upsert conflict resolution",
                },
            },
            "required": ["connection_id", "table_name"],
        }

    @staticmethod
    def _build_connection_url(context: NodeContext) -> str:
        config = context.config
        connection_id = config.get("connection_id")
        connection = (
            context.state.get("connections", {}).get(connection_id) if connection_id else None
        )
        if connection and connection.get("host") and connection.get("database"):
            return URL.create(
                "postgresql+asyncpg",
                username=connection.get("username") or connection.get("user"),
                password=connection.get("password"),
                host=connection["host"],
                port=int(connection.get("port", 5432)),
                database=connection["database"],
            ).render_as_string(hide_password=False)
        if not connection_id:
            raise ValueError(
                "Database writer requires connection_id; select an encrypted saved connection"
            )
        raise ValueError(f"Saved connection not available: {connection_id}")

    async def execute(self, context: NodeContext) -> NodeResult:
        """
        Execute the database writer node.

        Writes input rows to the configured PostgreSQL table.
        Supports insert, upsert, and replace modes.
        """
        config = context.config
        table_name = config.get("table_name", "")
        mode = config.get("mode", "insert")
        batch_size = config.get("batch_size", 500)
        upsert_key_str = config.get("upsert_key", "")

        if not table_name:
            return NodeResult(
                success=False,
                error_message="Missing required config: table_name",
            )

        if not _SQL_IDENTIFIER_RE.match(table_name):
            return NodeResult(success=False, error_message=f"Invalid table_name: {table_name}")

        # Extract rows from input data
        input_data = context.input_data
        rows = input_data.get("rows", [])
        if not rows:
            return NodeResult(
                success=True,
                output_data={"rows_affected": 0, "table": table_name},
                items_processed=0,
            )

        columns = list(rows[0].keys())
        if not columns:
            return NodeResult(
                success=True,
                output_data={"rows_affected": 0, "table": table_name},
                items_processed=0,
            )

        invalid_columns = [
            str(column) for column in columns if not _SQL_IDENTIFIER_RE.match(str(column))
        ]
        if invalid_columns:
            return NodeResult(
                success=False,
                error_message=f"Invalid input column names: {', '.join(invalid_columns)}",
            )

        try:
            connection_url = self._build_connection_url(context)
        except Exception as e:
            return NodeResult(
                success=False,
                error_message=f"Failed to get database URL: {e}",
            )

        # Parse upsert keys
        upsert_keys = []
        if upsert_key_str:
            upsert_keys = [k.strip() for k in upsert_key_str.split(",") if k.strip()]
        invalid_upsert_keys = [
            key for key in upsert_keys if not _SQL_IDENTIFIER_RE.match(key) or key not in columns
        ]
        if invalid_upsert_keys:
            return NodeResult(
                success=False,
                error_message=f"Invalid upsert keys: {', '.join(invalid_upsert_keys)}",
            )
        if mode == "upsert" and not upsert_keys:
            return NodeResult(
                success=False,
                error_message="upsert mode requires at least one upsert_key",
            )

        quoted_cols = ", ".join(f'"{column}"' for column in columns)
        param_placeholders = ", ".join(f":{column}__param" for column in columns)

        engine = None
        try:
            engine = create_async_engine(connection_url, pool_pre_ping=True)
            session_factory = async_sessionmaker(
                bind=engine,
                class_=AsyncSession,
                expire_on_commit=False,
            )

            async with session_factory() as session:
                # Handle replace mode: truncate the table first
                if mode == "replace":
                    await session.execute(text(f'TRUNCATE TABLE "{table_name}" CASCADE'))
                    await session.commit()

                # Build insert statements in batches
                total_affected = 0

                for batch_start in range(0, len(rows), batch_size):
                    batch = rows[batch_start : batch_start + batch_size]

                    if mode == "upsert" and upsert_keys:
                        conflict_columns = ", ".join(f'"{key}"' for key in upsert_keys)
                        update_columns = [column for column in columns if column not in upsert_keys]
                        action = "DO NOTHING"
                        if update_columns:
                            assignments = ", ".join(
                                f'"{column}" = EXCLUDED."{column}"' for column in update_columns
                            )
                            action = f"DO UPDATE SET {assignments}"
                        statement = text(
                            f'INSERT INTO "{table_name}" ({quoted_cols}) '
                            f"VALUES ({param_placeholders}) "
                            f"ON CONFLICT ({conflict_columns}) {action}"
                        )
                        for row in batch:
                            await session.execute(
                                statement,
                                {f"{column}__param": row[column] for column in columns},
                            )
                    else:
                        # Simple insert - use per-row execution for simplicity
                        for row in batch:
                            await session.execute(
                                text(
                                    f'INSERT INTO "{table_name}" ({quoted_cols}) '
                                    f"VALUES ({param_placeholders})"
                                ),
                                {f"{c}__param": row[c] for c in columns if c in row},
                            )

                    await session.commit()
                    total_affected += len(batch)

            return NodeResult(
                success=True,
                output_data={
                    "rows_affected": total_affected,
                    "table": table_name,
                },
                items_processed=total_affected,
                metadata={
                    "mode": mode,
                    "batch_size": batch_size,
                    "batches": (len(rows) + batch_size - 1) // batch_size,
                },
            )

        except Exception as e:
            logger.error(f"DatabaseWriterNode error: {e}", exc_info=True)
            # Sanitize error message to avoid leaking connection credentials
            return NodeResult(
                success=False,
                error_message=f"Database write failed: {type(e).__name__}",
            )
        finally:
            if engine is not None:
                await engine.dispose()


def register():
    from app.nodes.registry import register_node

    register_node(DatabaseWriterNode())
