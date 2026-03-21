"""
Split node: fan-out -- splits items into batches for parallel processing.

Takes a list of items and outputs them as individual batches.
With batch_size=1, each item is output individually (full fan-out).
"""
import logging
from typing import Any

from app.nodes.base import BaseNode, NodeContext, NodeResult, PortDef, PortDataType

logger = logging.getLogger(__name__)


class SplitNode(BaseNode):
    @property
    def node_type(self) -> str:
        return "split"

    @property
    def display_name(self) -> str:
        return "Split"

    @property
    def category(self) -> str:
        return "processing"

    @property
    def description(self) -> str:
        return "Split items into batches for parallel processing (fan-out)"

    @property
    def inputs(self) -> list[PortDef]:
        return [PortDef(name="items", data_type=PortDataType.ITEMS, required=True)]

    @property
    def outputs(self) -> list[PortDef]:
        return [PortDef(name="item", data_type=PortDataType.ITEMS, label="Item (per batch)")]

    @property
    def config_schema(self) -> dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "batch_size": {"type": "integer", "default": 1, "minimum": 1},
            },
        }

    async def execute(self, context: NodeContext) -> NodeResult:
        """Split items list into batches."""
        items = context.input_data.get("items", [])
        batch_size = context.config.get("batch_size", 1)

        if not isinstance(items, list):
            return NodeResult(
                success=False,
                output_data={"item": []},
                items_processed=0,
                error_message=f"Expected list input, got {type(items).__name__}",
            )

        if batch_size < 1:
            batch_size = 1

        # Create batches
        batches = [
            items[i : i + batch_size]
            for i in range(0, len(items), batch_size)
        ]

        return NodeResult(
            success=True,
            output_data={"item": batches},
            items_processed=len(items),
            metadata={
                "total_items": len(items),
                "batch_size": batch_size,
                "batch_count": len(batches),
            },
        )


def register():
    from app.nodes.registry import register_node
    register_node(SplitNode())
