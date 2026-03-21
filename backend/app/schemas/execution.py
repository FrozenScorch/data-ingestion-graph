"""
Pydantic schemas for execution (runs).
"""
from datetime import datetime
from typing import Optional, Any
from uuid import UUID

from pydantic import BaseModel, Field


class RunCreate(BaseModel):
    trigger_type: str = Field(default="manual", pattern="^(manual|webhook|schedule)$")
    graph_version_id: Optional[UUID] = None


class RunResponse(BaseModel):
    id: UUID
    graph_id: UUID
    graph_version_id: Optional[UUID]
    trigger_type: str
    triggered_by: Optional[UUID]
    status: str
    error_message: Optional[str]
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}


class RunListResponse(BaseModel):
    runs: list[RunResponse]
    total: int


class RunNodeResponse(BaseModel):
    id: UUID
    run_id: UUID
    node_id: str
    node_type: str
    status: str
    attempt_count: int
    max_retries: int
    input_data: Optional[dict] = None
    output_data: Optional[dict] = None
    items_processed: Optional[int] = None
    duration_ms: Optional[int] = None
    error_message: Optional[str] = None
    created_at: datetime

    model_config = {"from_attributes": True}


class RunDetailResponse(BaseModel):
    run: RunResponse
    run_nodes: list[RunNodeResponse] = Field(default_factory=list)


class RunControlRequest(BaseModel):
    """Request body for run control operations (cancel, pause, resume, retry)."""
    pass  # No additional parameters needed for Phase 1
