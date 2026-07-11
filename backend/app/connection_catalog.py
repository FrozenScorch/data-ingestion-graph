"""Authoritative typed connection definitions shared by API and Studio UI."""

from typing import Any

CONNECTION_DEFINITIONS: dict[str, dict[str, Any]] = {
    "postgres": {
        "type": "postgres",
        "label": "PostgreSQL",
        "description": "PostgreSQL or pgvector database",
        "fields": [
            {"name": "host", "label": "Host", "type": "string", "required": True},
            {"name": "port", "label": "Port", "type": "integer", "default": 5432},
            {"name": "database", "label": "Database", "type": "string", "required": True},
            {"name": "username", "label": "Username", "type": "string", "required": True},
            {
                "name": "password",
                "label": "Password",
                "type": "string",
                "format": "password",
                "required": True,
            },
        ],
    },
    "discord": {
        "type": "discord",
        "label": "Discord",
        "description": "Discord bot account used to read configured channels",
        "fields": [
            {
                "name": "bot_token",
                "label": "Bot Token",
                "type": "string",
                "format": "password",
                "required": True,
            }
        ],
    },
}

SUPPORTED_CONNECTION_TYPES = tuple(CONNECTION_DEFINITIONS)


def validate_connection_config(connection_type: str, config: dict[str, Any] | None) -> None:
    definition = CONNECTION_DEFINITIONS.get(connection_type)
    if definition is None:
        raise ValueError(
            f"Unsupported connection type: {connection_type}. "
            f"Supported: {', '.join(SUPPORTED_CONNECTION_TYPES)}"
        )
    values = config or {}
    missing = [
        field["name"]
        for field in definition["fields"]
        if field.get("required") and values.get(field["name"]) in (None, "")
    ]
    if missing:
        raise ValueError(f"Missing required connection fields: {', '.join(missing)}")
