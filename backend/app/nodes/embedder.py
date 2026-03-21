"""
Embedder node: generates embeddings via OpenRouter.

Takes chunks from TextChunkerNode and generates vector embeddings
for each chunk using the configured embedding model.
"""
import logging
from typing import Any

from app.nodes.base import BaseNode, NodeContext, NodeResult, PortDef, PortDataType
from app.services.openrouter_service import openrouter_service, SUPPORTED_EMBEDDING_MODELS

logger = logging.getLogger(__name__)


class EmbedderNode(BaseNode):
    @property
    def node_type(self) -> str:
        return "embedder"

    @property
    def display_name(self) -> str:
        return "Embedder"

    @property
    def category(self) -> str:
        return "ai"

    @property
    def description(self) -> str:
        return "Generate text embeddings using OpenRouter embedding models"

    @property
    def inputs(self) -> list[PortDef]:
        return [PortDef(name="chunks", data_type=PortDataType.CHUNKS, required=True, label="Chunks")]

    @property
    def outputs(self) -> list[PortDef]:
        return [PortDef(name="embeddings", data_type=PortDataType.EMBEDDINGS, label="Embeddings")]

    @property
    def config_schema(self) -> dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "model": {
                    "type": "string",
                    "enum": SUPPORTED_EMBEDDING_MODELS,
                    "default": SUPPORTED_EMBEDDING_MODELS[0],
                    "description": "Embedding model to use",
                },
                "batch_size": {
                    "type": "integer",
                    "default": 32,
                    "minimum": 1,
                    "maximum": 2048,
                    "description": "Number of texts per embedding batch",
                },
                "input_field": {
                    "type": "string",
                    "default": "text",
                    "description": "Which field in each chunk to embed",
                },
            },
            "required": ["model"],
        }

    async def execute(self, context: NodeContext) -> NodeResult:
        """
        Generate embeddings for input chunks.

        Expects context.input_data["chunks"] to be a list of dicts,
        each with at least the field specified by config["input_field"].

        Returns embeddings list: [{text, embedding, metadata, model, dimensions}, ...]
        """
        config = context.config
        model = config.get("model", "openai/text-embedding-3-small")
        batch_size = config.get("batch_size", 32)
        input_field = config.get("input_field", "text")

        chunks = context.input_data.get("chunks", [])
        if not chunks:
            return NodeResult(
                success=True,
                output_data={"embeddings": []},
                items_processed=0,
                metadata={"model": model},
            )

        # Collect texts from chunks
        texts = []
        for chunk in chunks:
            if isinstance(chunk, dict):
                text = chunk.get(input_field, "")
            else:
                text = str(chunk)
            texts.append(text)

        # Process in batches
        all_embeddings = []
        total_tokens_used = 0

        for i in range(0, len(texts), batch_size):
            batch_texts = texts[i:i + batch_size]
            batch_chunks = chunks[i:i + batch_size]

            try:
                response = await openrouter_service.create_embeddings(model, batch_texts)
                embeddings_data = response.get("data", [])

                # Extract usage info
                usage = response.get("usage", {})
                batch_tokens = usage.get("total_tokens", 0)
                total_tokens_used += batch_tokens

                for idx, emb_entry in enumerate(embeddings_data):
                    embedding = emb_entry.get("embedding", [])
                    chunk_data = batch_chunks[idx] if idx < len(batch_chunks) else {}
                    metadata = chunk_data.get("metadata", {}) if isinstance(chunk_data, dict) else {}

                    all_embeddings.append({
                        "text": batch_texts[idx] if idx < len(batch_texts) else "",
                        "embedding": embedding,
                        "metadata": metadata,
                        "model": model,
                        "dimensions": len(embedding),
                    })

            except Exception as e:
                logger.error(f"Embedding batch failed at offset {i}: {e}")
                return NodeResult(
                    success=False,
                    output_data={"embeddings": all_embeddings},
                    items_processed=len(all_embeddings),
                    error_message=f"Embedding failed at batch offset {i}: {str(e)}",
                    metadata={
                        "model": model,
                        "total_tokens_used": total_tokens_used,
                        "batch_size": batch_size,
                    },
                )

        # Record cost
        cost_info = openrouter_service.calculate_cost(model, total_tokens_used, 0)

        return NodeResult(
            success=True,
            output_data={"embeddings": all_embeddings},
            items_processed=len(all_embeddings),
            metadata={
                "model": model,
                "total_tokens_used": total_tokens_used,
                "input_tokens": total_tokens_used,
                "output_tokens": 0,
                "cost_usd": cost_info["total_cost_usd"],
                "input_cost_usd": cost_info["input_cost_usd"],
                "output_cost_usd": cost_info["output_cost_usd"],
                "batch_size": batch_size,
                "dimensions": len(all_embeddings[0]["embedding"]) if all_embeddings else 0,
            },
        )


def register():
    from app.nodes.registry import register_node
    register_node(EmbedderNode())
