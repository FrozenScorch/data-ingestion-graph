"""
Pydantic schemas for graph operations.
"""

from datetime import datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel, Field, field_serializer


_SECRET_KEY_MARKERS = ("password", "token", "secret", "api_key", "authorization", "cookie")


def _is_secret_key(key: object) -> bool:
    normalized = str(key).lower().replace("-", "_")
    return any(
        normalized == marker or normalized.endswith(f"_{marker}") for marker in _SECRET_KEY_MARKERS
    )


def _redact_sensitive(value: Any) -> Any:
    if isinstance(value, dict):
        if set(value) == {"$encrypted"}:
            return {"configured": True}
        redacted = {}
        for key, item in value.items():
            redacted[key] = "********" if _is_secret_key(key) else _redact_sensitive(item)
        return redacted
    if isinstance(value, list):
        return [_redact_sensitive(item) for item in value]
    return value


class GraphCreate(BaseModel):
    name: str = Field(..., min_length=1, max_length=255)
    description: str | None = Field(default=None, max_length=5000)
    tags: list[str] = Field(default_factory=list)
    template_id: str | None = Field(default=None, pattern="^[a-z0-9-]+$")


class GraphUpdate(BaseModel):
    name: str | None = Field(default=None, min_length=1, max_length=255)
    description: str | None = Field(default=None, max_length=5000)
    status: str | None = Field(default=None, pattern="^(draft|active|archived)$")
    tags: list[str] | None = None


class GraphVersionSave(BaseModel):
    nodes_data: dict | None = None
    edges_data: dict | None = None
    node_configs: dict | None = None


class GraphVersionResponse(BaseModel):
    id: UUID
    graph_id: UUID
    version_number: int
    nodes_data: dict | None
    edges_data: dict | None
    node_configs: dict | None
    checksum: str | None
    created_at: datetime

    model_config = {"from_attributes": True}

    @field_serializer("node_configs")
    def serialize_node_configs(self, value: dict | None) -> dict | None:
        return _redact_sensitive(value)

    @field_serializer("nodes_data")
    def serialize_nodes_data(self, value: dict | None) -> dict | None:
        return _redact_sensitive(value)


class GraphResponse(BaseModel):
    id: UUID
    name: str
    description: str | None
    owner_id: UUID
    status: str
    tags: list[str] | None
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}


class GraphDetailResponse(GraphResponse):
    """Graph response with latest version data included."""

    latest_version: GraphVersionResponse | None = None


class GraphListResponse(BaseModel):
    graphs: list[GraphResponse]
    total: int


class ConnectionCreate(BaseModel):
    name: str = Field(..., min_length=1, max_length=255)
    type: str = Field(..., pattern="^(postgres|discord)$")
    config: dict | None = None


class ConnectionUpdate(BaseModel):
    name: str | None = Field(default=None, min_length=1, max_length=255)
    config: dict | None = None


class ConnectionResponse(BaseModel):
    id: UUID
    user_id: UUID
    name: str
    type: str
    config: dict | None
    is_valid: bool
    created_at: datetime

    model_config = {"from_attributes": True}

    @field_serializer("config")
    def serialize_config(self, value: dict | None) -> dict | None:
        return _redact_sensitive(value)
