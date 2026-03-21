"""
WebhookSource node: webhook trigger input node.
"""
from typing import Any

from app.nodes.base import BaseNode, NodeContext, NodeResult, PortDef, PortDataType


class WebhookSourceNode(BaseNode):
    @property
    def node_type(self) -> str:
        return "webhook_source"

    @property
    def display_name(self) -> str:
        return "Webhook Source"

    @property
    def category(self) -> str:
        return "source"

    @property
    def description(self) -> str:
        return "Receive data from webhook POST requests"

    @property
    def inputs(self) -> list[PortDef]:
        return []

    @property
    def outputs(self) -> list[PortDef]:
        return [PortDef(name="json", data_type=PortDataType.JSON, label="JSON Data")]

    @property
    def config_schema(self) -> dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "path": {"type": "string", "description": "Auto-generated webhook path"},
                "method": {"type": "string", "enum": ["POST", "GET", "PUT"], "default": "POST"},
            },
        }

    async def execute(self, context: NodeContext) -> NodeResult:
        return NodeResult(success=True, output_data={"json": context.input_data.get("webhook_payload", {})}, items_processed=1)


def register():
    from app.nodes.registry import register_node
    register_node(WebhookSourceNode())
