"""
Pydantic schemas for node registry.
"""

from typing import Any

from pydantic import BaseModel, Field


class PortDefSchema(BaseModel):
    name: str
    data_type: str
    label: str = ""
    required: bool = False
    multi: bool = False


class NodeTypeDefSchema(BaseModel):
    type: str
    display_name: str
    category: str
    description: str
    implementation: str = "studio"
    sdk_component: str | None = None
    connector_manifest: dict[str, Any] | None = None
    inputs: list[PortDefSchema]
    outputs: list[PortDefSchema]
    config_schema: dict[str, Any]


class NodeRegistryResponse(BaseModel):
    nodes: list[NodeTypeDefSchema]
    total: int


class NodeValidateRequest(BaseModel):
    config: dict[str, Any] = Field(default_factory=dict)


class NodeValidateResponse(BaseModel):
    valid: bool
    errors: list[str] = Field(default_factory=list)
