"""
HttpRequest node: send HTTP requests.
"""
from typing import Any

from app.nodes.base import BaseNode, NodeContext, NodeResult, PortDef, PortDataType


class HttpRequestNode(BaseNode):
    @property
    def node_type(self) -> str:
        return "http_request"

    @property
    def display_name(self) -> str:
        return "HTTP Request"

    @property
    def category(self) -> str:
        return "output"

    @property
    def description(self) -> str:
        return "Send HTTP requests to external APIs"

    @property
    def inputs(self) -> list[PortDef]:
        return [PortDef(name="data", data_type=PortDataType.ANY, required=False)]

    @property
    def outputs(self) -> list[PortDef]:
        return [PortDef(name="json", data_type=PortDataType.JSON, label="Response")]

    @property
    def config_schema(self) -> dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "url": {"type": "string", "format": "uri"},
                "method": {"type": "string", "enum": ["GET", "POST", "PUT", "DELETE", "PATCH"], "default": "POST"},
                "headers": {"type": "object", "default": {}},
                "body": {"type": ["string", "object"], "description": "Request body template"},
            },
            "required": ["url"],
        }

    async def execute(self, context: NodeContext) -> NodeResult:
        return NodeResult(success=True, output_data={"json": {}}, items_processed=0)


def register():
    from app.nodes.registry import register_node
    register_node(HttpRequestNode())
