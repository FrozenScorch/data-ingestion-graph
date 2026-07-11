"""
DatabaseSource node: PostgreSQL reader node.

Reads data from a PostgreSQL database using a SQL query.
Uses SQLAlchemy async with asyncpg for database access.
Input: none (source node)
Output: {rows: [...], row_count: N, columns: [...]}
"""

import logging
import re
from typing import Any

from sqlalchemy import URL, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.nodes.base import BaseNode, NodeContext, NodeResult, PortDef, PortDataType

logger = logging.getLogger(__name__)

# SQL identifier regex: only simple identifiers like table_name or "schema"."table"
_SQL_IDENTIFIER_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*(\.[a-zA-Z_][a-zA-Z0-9_]*)?$")


class DatabaseSourceNode(BaseNode):
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
        return "Read data from a PostgreSQL database using a SQL query"

    @property
    def inputs(self) -> list[PortDef]:
        return []

    @property
    def outputs(self) -> list[PortDef]:
        return [PortDef(name="table", data_type=PortDataType.TABLE, label="Table Data")]

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
                "query": {
                    "type": "string",
                    "format": "textarea",
                    "default": "SELECT * FROM table LIMIT 1000",
                    "description": "SQL query to execute (SELECT only)",
                },
                "batch_size": {
                    "type": "integer",
                    "default": 1000,
                    "minimum": 1,
                    "description": "Number of rows per batch",
                },
            },
            "required": ["connection_id", "query"],
        }

    @staticmethod
    def _validate_query(query: str) -> None:
        """Validate that the query is SELECT-only to prevent data modification."""
        stripped = query.strip().rstrip(";").strip()
        upper = stripped.upper()
        # CTEs are intentionally rejected: PostgreSQL permits data-modifying
        # INSERT/UPDATE/DELETE statements inside WITH clauses.
        if not upper.startswith("SELECT"):
            raise ValueError(
                "Only SELECT queries are allowed in database_source node. "
                f"Query starts with: {stripped[:20]}"
            )
        if ";" in stripped:
            raise ValueError("Multiple SQL statements are not allowed")

    def _build_connection_url(self, context: NodeContext) -> str:
        """Build a URL from the saved connection authorized for this node."""
        config = context.config
        connection_id = config.get("connection_id")
        saved_connections = context.state.get("connections", {})
        connection = saved_connections.get(connection_id) if connection_id else None

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
                "Database source requires connection_id; select an encrypted saved connection"
            )
        raise ValueError(f"Saved connection not available: {connection_id}")

    async def execute(self, context: NodeContext) -> NodeResult:
        """
        Execute the database source node.

        Reads data from PostgreSQL using the configured SQL query.
        Returns rows, row_count, and column names.
        """
        config = context.config
        query = config.get("query", "SELECT * FROM table LIMIT 1000")
        batch_size = config.get("batch_size", 1000)

        # Validate query is SELECT-only
        try:
            self._validate_query(query)
        except ValueError as e:
            return NodeResult(
                success=False,
                error_message=str(e),
            )

        try:
            connection_url = self._build_connection_url(context)
        except Exception as e:
            return NodeResult(
                success=False,
                error_message=f"Failed to build connection URL: {e}",
            )

        engine = None
        try:
            # Create a temporary engine for this read operation
            engine = create_async_engine(
                connection_url,
                pool_pre_ping=True,
                connect_args={"server_settings": {"default_transaction_read_only": "on"}},
            )
            session_factory = async_sessionmaker(
                bind=engine,
                class_=AsyncSession,
                expire_on_commit=False,
            )

            async with session_factory() as session:
                # Execute the query
                result = await session.execute(text(query))

                # Get column names from result metadata
                if result.returns_rows:
                    columns = list(result.keys())
                    rows = []
                    for row in result:
                        rows.append(dict(row._mapping))
                        # Respect batch_size limit
                        if len(rows) >= batch_size:
                            break
                else:
                    # Non-SELECT query (INSERT, UPDATE, etc.)
                    await session.commit()
                    return NodeResult(
                        success=True,
                        output_data={
                            "rows": [],
                            "row_count": 0,
                            "columns": [],
                        },
                        items_processed=0,
                        metadata={"query_type": "non_select"},
                    )

            return NodeResult(
                success=True,
                output_data={
                    "rows": rows,
                    "row_count": len(rows),
                    "columns": columns,
                },
                items_processed=len(rows),
                metadata={
                    "query": query,
                    "batch_size": batch_size,
                },
            )

        except Exception as e:
            logger.error(f"DatabaseSourceNode error: {e}", exc_info=True)
            # Sanitize error message to avoid leaking connection credentials
            return NodeResult(
                success=False,
                error_message=f"Database query failed: {type(e).__name__}",
            )
        finally:
            if engine is not None:
                await engine.dispose()


def register():
    from app.nodes.registry import register_node

    register_node(DatabaseSourceNode())
