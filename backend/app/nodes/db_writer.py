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

from sqlalchemy import Column, Integer, MetaData, Table, Text, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.nodes.base import BaseNode, NodeContext, NodeResult, PortDef, PortDataType
from app.config import settings

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
            "required": ["table_name"],
        }

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

        try:
            connection_url = settings.database_url
        except Exception as e:
            return NodeResult(
                success=False,
                error_message=f"Failed to get database URL: {e}",
            )

        # Parse upsert keys
        upsert_keys = []
        if upsert_key_str:
            upsert_keys = [k.strip() for k in upsert_key_str.split(",") if k.strip()]

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

                # Get column names from the first row
                columns = list(rows[0].keys()) if rows else []

                if not columns:
                    return NodeResult(
                        success=True,
                        output_data={"rows_affected": 0, "table": table_name},
                        items_processed=0,
                    )

                # Build insert statements in batches
                total_affected = 0

                # Pre-compute SQL fragments to avoid nested f-string issues
                quoted_cols = ", ".join(f'"{c}"' for c in columns)
                param_placeholders = ", ".join(f":{c}__param" for c in columns)

                for batch_start in range(0, len(rows), batch_size):
                    batch = rows[batch_start : batch_start + batch_size]

                    if mode == "upsert" and upsert_keys:
                        # Use PostgreSQL ON CONFLICT ... DO UPDATE
                        stmt = pg_insert(text(f'"{table_name}"')).values(batch)
                        update_dict = {
                            col: stmt.excluded[col]
                            for col in columns
                            if col not in upsert_keys
                        }
                        stmt = stmt.on_conflict_do_update(
                            index_elements=upsert_keys,
                            set_=update_dict,
                        )
                        result = await session.execute(stmt)
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
            return NodeResult(
                success=False,
                error_message=f"Database write failed: {e}",
            )
        finally:
            if engine is not None:
                await engine.dispose()


def register():
    from app.nodes.registry import register_node
    register_node(DatabaseWriterNode())
