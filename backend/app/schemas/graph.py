"""
Pydantic schemas for graph operations.
"""
from datetime import datetime
from typing import Optional, Any
from uuid import UUID

from pydantic import BaseModel, Field


class GraphCreate(BaseModel):
    name: str = Field(..., min_length=1, max_length=255)
    description: str | None = Field(default=None, max_length=5000)
    tags: list[str] = Field(default_factory=list)


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


class GraphListResponse(BaseModel):
    graphs: list[GraphResponse]
    total: int


class ConnectionCreate(BaseModel):
    name: str = Field(..., min_length=1, max_length=255)
    type: str = Field(..., pattern="^(postgres|discord|github|webhook)$")
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
