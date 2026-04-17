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

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.config import settings
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
            "required": ["query"],
        }

    @staticmethod
    def _validate_query(query: str) -> None:
        """Validate that the query is SELECT-only to prevent data modification."""
        stripped = query.strip().rstrip(";").strip()
        upper = stripped.upper()
        # Must start with SELECT or WITH (CTE)
        if not (upper.startswith("SELECT") or upper.startswith("WITH")):
            raise ValueError(
                "Only SELECT queries are allowed in database_source node. "
                f"Query starts with: {stripped[:20]}"
            )

    def _build_connection_url(self) -> str:
        """Return the app's own database connection URL."""
        return settings.database_url

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
            connection_url = self._build_connection_url()
        except Exception as e:
            return NodeResult(
                success=False,
                error_message=f"Failed to build connection URL: {e}",
            )

        engine = None
        try:
            # Create a temporary engine for this read operation
            engine = create_async_engine(connection_url, pool_pre_ping=True)
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
