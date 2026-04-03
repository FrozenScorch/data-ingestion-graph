"""
VectorStore node: store embeddings in pgvector.

Stores embedding vectors in a PostgreSQL table with pgvector extension.
Uses raw SQL via asyncpg for vector operations (pgvector works best with raw SQL).
Input: embeddings list from EmbedderNode
Output: {stored_count: N, table: "...", index_created: bool}
"""
import json
import logging
import re
from typing import Any

_SQL_IDENTIFIER_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")

from app.nodes.base import BaseNode, NodeContext, NodeResult, PortDef, PortDataType

logger = logging.getLogger(__name__)


class VectorStoreNode(BaseNode):
    @property
    def node_type(self) -> str:
        return "vector_store"

    @property
    def display_name(self) -> str:
        return "Vector Store"

    @property
    def category(self) -> str:
        return "output"

    @property
    def description(self) -> str:
        return "Store embeddings in PostgreSQL with pgvector"

    @property
    def inputs(self) -> list[PortDef]:
        return [PortDef(name="embeddings", data_type=PortDataType.EMBEDDINGS, required=True)]

    @property
    def outputs(self) -> list[PortDef]:
        return [PortDef(name="count", data_type=PortDataType.ANY, label="Count")]

    @property
    def config_schema(self) -> dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "connection_id": {
                    "type": "string",
                    "description": "Reference to a saved connection ID",
                },
                "table_name": {
                    "type": "string",
                    "default": "documents",
                    "description": "Target table for vectors",
                },
                "embedding_dim": {
                    "type": "integer",
                    "default": 1536,
                    "description": "Embedding dimensions (1536 for text-embedding-3-small)",
                },
                "create_index": {
                    "type": "boolean",
                    "default": True,
                    "description": "Create HNSW index on the vector column",
                },
                "id_column": {
                    "type": "string",
                    "default": "id",
                    "description": "Column name for document IDs",
                },
                "content_column": {
                    "type": "string",
                    "default": "content",
                    "description": "Column name for text content",
                },
                "metadata_column": {
                    "type": "string",
                    "default": "metadata",
                    "description": "Column name for JSONB metadata",
                },
                "vector_column": {
                    "type": "string",
                    "default": "embedding",
                    "description": "Column name for pgvector column",
                },
            },
            "required": ["connection_id"],
        }

    async def _get_asyncpg_connection(self, context: NodeContext):
        """
        Create an asyncpg connection from the node context.

        Uses asyncpg directly for raw SQL vector operations.
        """
        import asyncpg

        config = context.config
        connection_id = config.get("connection_id", "")

        connections = context.state.get("connections", {})
        if connection_id in connections:
            conn_config = connections[connection_id]
        else:
            conn_config = config

        host = conn_config.get("host", "localhost")
        port = conn_config.get("port", 5432)
        database = conn_config.get("database", conn_config.get("dbname", "postgres"))
        username = conn_config.get("username", conn_config.get("user", "postgres"))
        password = conn_config.get("password", "")

        conn = await asyncpg.connect(
            host=host,
            port=port,
            database=database,
            user=username,
            password=password,
        )
        return conn

    async def execute(self, context: NodeContext) -> NodeResult:
        """
        Execute the vector store node.

        Creates the target table if it doesn't exist, stores embeddings,
        and optionally creates an HNSW index for vector search.
        """
        config = context.config
        table_name = config.get("table_name", "documents")
        embedding_dim = config.get("embedding_dim", 1536)
        create_index = config.get("create_index", True)
        id_column = config.get("id_column", "id")
        content_column = config.get("content_column", "content")
        metadata_column = config.get("metadata_column", "metadata")
        vector_column = config.get("vector_column", "embedding")

        # Validate all SQL identifiers before interpolation to prevent injection
        for field, value in [
            ("table_name", table_name),
            ("id_column", id_column),
            ("content_column", content_column),
            ("metadata_column", metadata_column),
            ("vector_column", vector_column),
        ]:
            if not _SQL_IDENTIFIER_RE.match(str(value)):
                return NodeResult(
                    success=False,
                    error_message=(
                        f"Invalid SQL identifier for {field}: {value!r}. "
                        "Must start with a letter or underscore and contain only "
                        "letters, digits, or underscores."
                    ),
                )

        # Extract embeddings from input data
        input_data = context.input_data
        embeddings_data = input_data.get("embeddings", [])

        if not embeddings_data:
            return NodeResult(
                success=True,
                output_data={"stored_count": 0, "table": table_name, "index_created": False},
                items_processed=0,
            )

        conn = None
        try:
            conn = await self._get_asyncpg_connection(context)

            # Ensure pgvector extension is enabled
            await conn.execute("CREATE EXTENSION IF NOT EXISTS vector")

            # Auto-create table if it doesn't exist
            create_table_sql = f"""
                CREATE TABLE IF NOT EXISTS "{table_name}" (
                    {id_column} UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    {content_column} TEXT NOT NULL,
                    {metadata_column} JSONB DEFAULT '{{}}',
                    {vector_column} vector({embedding_dim}),
                    created_at TIMESTAMPTZ DEFAULT NOW()
                );
            """
            await conn.execute(create_table_sql)

            # Insert embeddings
            stored_count = 0
            for item in embeddings_data:
                content = item.get("content", item.get("text", ""))
                metadata = item.get("metadata", {})
                embedding = item.get("embedding", item.get("vector", []))

                if not embedding:
                    continue

                # Convert embedding list to pgvector string format: [1.0, 2.0, 3.0, ...]
                embedding_str = "[" + ",".join(str(float(v)) for v in embedding) + "]"
                metadata_str = json.dumps(metadata) if isinstance(metadata, dict) else str(metadata)

                insert_sql = f"""
                    INSERT INTO "{table_name}" ({content_column}, {metadata_column}, {vector_column})
                    VALUES ($1, $2, $3::vector)
                """
                await conn.execute(insert_sql, content, metadata_str, embedding_str)
                stored_count += 1

            # Create HNSW index if requested
            index_created = False
            if create_index:
                index_name = f"idx_{table_name}_{vector_column}_hnsw"
                try:
                    index_sql = f"""
                        CREATE INDEX IF NOT EXISTS {index_name}
                        ON "{table_name}" USING hnsw ({vector_column} vector_cosine_ops)
                    """
                    await conn.execute(index_sql)
                    index_created = True
                except Exception as e:
                    logger.warning(f"Failed to create HNSW index: {e}")
                    index_created = False

            return NodeResult(
                success=True,
                output_data={
                    "stored_count": stored_count,
                    "table": table_name,
                    "index_created": index_created,
                },
                items_processed=stored_count,
                metadata={
                    "embedding_dim": embedding_dim,
                    "table_name": table_name,
                    "vector_column": vector_column,
                },
            )

        except Exception as e:
            logger.error(f"VectorStoreNode error: {e}", exc_info=True)
            return NodeResult(
                success=False,
                error_message=f"Vector store failed: {e}",
            )
        finally:
            if conn is not None:
                await conn.close()


def register():
    from app.nodes.registry import register_node
    register_node(VectorStoreNode())
