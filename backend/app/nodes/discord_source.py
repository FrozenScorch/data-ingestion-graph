"""
DiscordSource node: Discord channel reader node.
"""
from typing import Any

from app.nodes.base import BaseNode, NodeContext, NodeResult, PortDef, PortDataType


class DiscordSourceNode(BaseNode):
    @property
    def node_type(self) -> str:
        return "discord_source"

    @property
    def display_name(self) -> str:
        return "Discord Source"

    @property
    def category(self) -> str:
        return "source"

    @property
    def description(self) -> str:
        return "Read messages from Discord channels"

    @property
    def inputs(self) -> list[PortDef]:
        return []

    @property
    def outputs(self) -> list[PortDef]:
        return [PortDef(name="json", data_type=PortDataType.JSON, label="Messages")]

    @property
    def config_schema(self) -> dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "bot_token": {"type": "string", "format": "password", "description": "Discord bot token"},
                "connection_id": {"type": "string"},
                "channel_id": {"type": "string"},
                "message_limit": {"type": "integer", "default": 100},
            },
            "required": ["bot_token", "channel_id"],
        }

    async def execute(self, context: NodeContext) -> NodeResult:
        return NodeResult(success=True, output_data={"json": []}, items_processed=0)


def register():
    from app.nodes.registry import register_node
    register_node(DiscordSourceNode())
