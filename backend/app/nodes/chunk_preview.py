"""
Chunk Preview node: pass-through sink for previewing chunked data.

Captures input items, formats a preview with sample text and metadata,
and passes data through unchanged for downstream nodes.
"""
import logging
from typing import Any

from app.nodes.base import BaseNode, NodeContext, NodeResult, PortDef, PortDataType

logger = logging.getLogger(__name__)


class ChunkPreviewNode(BaseNode):
    @property
    def node_type(self) -> str:
        return "chunk_preview"

    @property
    def display_name(self) -> str:
        return "Chunk Preview"

    @property
    def category(self) -> str:
        return "processing"

    @property
    def description(self) -> str:
        return "Pass-through sink for previewing chunks with sample text and metadata"

    @property
    def inputs(self) -> list[PortDef]:
        return [PortDef(name="chunks", data_type=PortDataType.CHUNKS, required=True)]

    @property
    def outputs(self) -> list[PortDef]:
        return [PortDef(name="output", data_type=PortDataType.CHUNKS, label="Output")]

    @property
    def config_schema(self) -> dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "max_preview_items": {
                    "type": "integer",
                    "description": "Maximum number of items to include in the preview",
                    "default": 10,
                    "minimum": 1,
                },
                "max_chars_per_item": {
                    "type": "integer",
                    "description": "Maximum characters per item text in the preview",
                    "default": 200,
                    "minimum": 1,
                },
            },
        }

    async def execute(self, context: NodeContext) -> NodeResult:
        """Preview chunks by capturing sample text and metadata, pass through unchanged."""
        chunks = context.input_data.get("chunks", [])

        if not isinstance(chunks, list):
            chunks = [chunks] if chunks else []

        max_preview_items = context.config.get("max_preview_items", 10)
        max_chars_per_item = context.config.get("max_chars_per_item", 200)

        # Clamp config values
        max_preview_items = max(1, int(max_preview_items))
        max_chars_per_item = max(1, int(max_chars_per_item))

        truncated = len(chunks) > max_preview_items
        preview_chunks = chunks[:max_preview_items]

        preview_items = []
        for idx, chunk in enumerate(preview_chunks):
            if isinstance(chunk, dict):
                text = str(chunk.get("text", chunk.get("content", "")))
                metadata = {k: v for k, v in chunk.items() if k not in ("text", "content")}
            else:
                text = str(chunk)
                metadata = {}

            # Truncate text if needed
            display_text = text[:max_chars_per_item]
            if len(text) > max_chars_per_item:
                display_text += "..."

            preview_items.append({
                "index": idx,
                "text": display_text,
                "metadata": metadata,
            })

        # Build output_data with preview info, passing chunks through on "output"
        output_data = {
            "output": chunks,
            "chunks": preview_items,
            "total_items": len(chunks),
            "previewed_items": len(preview_items),
            "truncated": truncated,
        }

        return NodeResult(
            success=True,
            output_data=output_data,
            items_processed=len(chunks),
            metadata={
                "total_items": len(chunks),
                "previewed_items": len(preview_items),
                "truncated": truncated,
                "max_preview_items": max_preview_items,
                "max_chars_per_item": max_chars_per_item,
            },
        )


def register():
    from app.nodes.registry import register_node
    register_node(ChunkPreviewNode())
