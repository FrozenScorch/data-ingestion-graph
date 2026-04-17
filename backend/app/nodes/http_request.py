"""
HttpRequest node: send HTTP requests.
"""
import json
import logging
from typing import Any

from app.nodes.base import BaseNode, NodeContext, NodeResult, PortDef, PortDataType

logger = logging.getLogger(__name__)


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
                "headers": {"type": "string", "format": "textarea", "default": "{}", "description": "HTTP headers as JSON object"},
                "body": {"type": "string", "format": "textarea", "description": "Request body (text or JSON)"},
            },
            "required": ["url"],
        }

    @staticmethod
    def _parse_json_field(value: Any) -> Any:
        """Try to parse a string value as JSON. Returns the original value if parsing fails."""
        if isinstance(value, str):
            try:
                return json.loads(value)
            except (json.JSONDecodeError, ValueError):
                return value
        return value

    async def execute(self, context: NodeContext) -> NodeResult:
        config = context.config

        # Parse headers: if string, try JSON parse; fall back to plain string
        headers = self._parse_json_field(config.get("headers", "{}"))
        if isinstance(headers, str):
            headers = {}
            logger.warning("headers could not be parsed as JSON, using empty dict")

        # Parse body: if string, try JSON parse; fall back to plain string
        body = self._parse_json_field(config.get("body"))

        # TODO: implement actual HTTP request logic using headers and body
        return NodeResult(
            success=False,
            output_data={"json": {}},
            items_processed=0,
            error_message="HTTP Request node is not yet implemented",
        )


def register():
    from app.nodes.registry import register_node
    register_node(HttpRequestNode())
