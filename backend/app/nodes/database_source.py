"""
DatabaseSource node: PostgreSQL reader node.

Reads data from a PostgreSQL database using a SQL query.
Uses SQLAlchemy async with asyncpg for database access.
Input: none (source node)
Output: {rows: [...], row_count: N, columns: [...]}
"""
import logging
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.nodes.base import BaseNode, NodeContext, NodeResult, PortDef, PortDataType

logger = logging.getLogger(__name__)


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
                    "description": "Reference to a saved connection ID",
                },
                "query": {
                    "type": "string",
                    "default": "SELECT * FROM table LIMIT 1000",
                    "description": "SQL query to execute",
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

    async def _build_connection_url(self, context: NodeContext) -> str:
        """
        Build a database connection URL from the node context.

        Looks for connection config in context.state['connections'] keyed
        by connection_id, or falls back to inline config in context.config.
        """
        config = context.config
        connection_id = config.get("connection_id", "")

        # Check if a saved connection was loaded into state by the engine
        connections = context.state.get("connections", {})
        if connection_id in connections:
            conn_config = connections[connection_id]
        else:
            # Use inline config if provided
            conn_config = config

        # Build asyncpg URL
        host = conn_config.get("host", "localhost")
        port = conn_config.get("port", 5432)
        database = conn_config.get("database", conn_config.get("dbname", "postgres"))
        username = conn_config.get("username", conn_config.get("user", "postgres"))
        password = conn_config.get("password", "")

        url = f"postgresql+asyncpg://{username}:{password}@{host}:{port}/{database}"
        return url

    async def execute(self, context: NodeContext) -> NodeResult:
        """
        Execute the database source node.

        Reads data from PostgreSQL using the configured SQL query.
        Returns rows, row_count, and column names.
        """
        config = context.config
        query = config.get("query", "SELECT * FROM table LIMIT 1000")
        batch_size = config.get("batch_size", 1000)

        try:
            connection_url = await self._build_connection_url(context)
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
            return NodeResult(
                success=False,
                error_message=f"Database query failed: {e}",
            )
        finally:
            if engine is not None:
                await engine.dispose()


def register():
    from app.nodes.registry import register_node
    register_node(DatabaseSourceNode())
