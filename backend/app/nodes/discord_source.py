"""Studio adapter for the SDK's Discord source connector."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from app.nodes.base import BaseNode, NodeContext, NodeResult, PortDataType, PortDef
from ingestion_graph.messages import RecordMessage, StateMessage
from ingestion_graph.models import RecordPayload
from ingestion_graph.secrets import SecretRef, SecretValue
from ingestion_graph.sources import DiscordSource


class _SavedConnectionSecrets:
    name = "saved-connection"

    def __init__(self, values: Mapping[str, Any]) -> None:
        self._values = values

    def resolve(self, reference: SecretRef) -> SecretValue:
        value = self._values.get(reference.key)
        if not isinstance(value, str) or not value:
            raise ValueError("Saved Discord connection has no bot token")
        return SecretValue(value)


class DiscordSourceNode(BaseNode):
    @property
    def implementation(self) -> str:
        return "sdk-adapter"

    @property
    def sdk_component(self) -> str:
        return "ingestion_graph.sources.DiscordSource"

    @property
    def node_type(self) -> str:
        return "discord_source"

    @property
    def display_name(self) -> str:
        return "Discord Source (SDK)"

    @property
    def category(self) -> str:
        return "source"

    @property
    def description(self) -> str:
        return "Preview Discord messages through the reusable, rate-limit-aware SDK connector"

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
                "connection_id": {
                    "type": "string",
                    "description": "Encrypted saved Discord connection ID",
                },
                "channel_id": {
                    "type": "string",
                    "pattern": "^[0-9]+$",
                    "description": "Discord channel ID to preview",
                },
                "message_limit": {
                    "type": "integer",
                    "default": 100,
                    "minimum": 1,
                    "maximum": 100,
                    "description": "Maximum messages in this bounded Studio preview",
                },
            },
            "required": ["connection_id", "channel_id"],
        }

    async def execute(self, context: NodeContext) -> NodeResult:
        connection_id = context.config.get("connection_id")
        connection = context.state.get("connections", {}).get(connection_id)
        channel_id = str(context.config.get("channel_id") or "")
        try:
            message_limit = int(context.config.get("message_limit", 100))
        except (TypeError, ValueError):
            return NodeResult(success=False, error_message="message_limit must be an integer")
        if not connection:
            return NodeResult(success=False, error_message="Saved Discord connection not available")
        token = connection.get("bot_token") or connection.get("token")
        if not isinstance(token, str) or not token:
            return NodeResult(
                success=False, error_message="Saved Discord connection has no bot token"
            )

        connector: DiscordSource | None = None
        try:
            connector = DiscordSource(
                [channel_id],
                SecretRef("bot_token", provider="saved-connection"),
                secret_provider=_SavedConnectionSecrets({"bot_token": token}),
                page_size=message_limit,
            )
            stream = (await connector.discover())[0]
            messages: list[dict[str, Any]] = []
            async for message in connector.read(stream):
                if isinstance(message, RecordMessage):
                    payload = message.envelope.payload
                    if isinstance(payload, RecordPayload):
                        messages.append(dict(payload.data))
                elif isinstance(message, StateMessage):
                    break
            return NodeResult(
                success=True,
                output_data={"json": messages},
                items_processed=len(messages),
                metadata={"sdk_connector": "discord", "bounded_preview": True},
            )
        except Exception as exc:
            return NodeResult(
                success=False,
                error_message=f"Discord SDK source failed: {type(exc).__name__}",
            )
        finally:
            if connector is not None:
                await connector.close()


def register() -> None:
    from app.nodes.registry import register_node

    register_node(DiscordSourceNode())
