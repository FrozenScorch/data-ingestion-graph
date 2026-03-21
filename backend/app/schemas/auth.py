"""
Pydantic schemas for authentication.
"""
from datetime import datetime
from typing import Optional
from uuid import UUID

from pydantic import BaseModel, EmailStr, Field


class LoginRequest(BaseModel):
    username: str = Field(..., min_length=1, max_length=255)
    password: str = Field(..., min_length=1)


class RegisterRequest(BaseModel):
    username: str = Field(..., min_length=1, max_length=255)
    email: str = Field(..., min_length=1, max_length=255)
    password: str = Field(..., min_length=6, max_length=255)
    role: str = Field(default="viewer", pattern="^(admin|editor|viewer)$")


class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    expires_in: int = Field(description="Seconds until token expires")


class UserResponse(BaseModel):
    id: UUID
    username: str
    email: str
    role: str
    is_active: bool
    created_at: datetime

    model_config = {"from_attributes": True}


class ApiKeyCreate(BaseModel):
    name: str = Field(..., min_length=1, max_length=255)
    scopes: list[str] = Field(default_factory=list)
    expires_days: Optional[int] = Field(default=None, ge=1)


class ApiKeyResponse(BaseModel):
    id: UUID
    name: str
    key_prefix: str
    key: Optional[str] = Field(default=None, description="Full API key (only returned on creation)")
    scopes: list[str] | None
    expires_at: Optional[datetime]
    created_at: datetime

    model_config = {"from_attributes": True}
