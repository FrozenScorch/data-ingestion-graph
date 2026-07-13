"""Strict request and redacted response schemas for graph triggers."""

from datetime import datetime
from typing import Literal
from uuid import UUID

from app.services.trigger_schedule import (
    MAX_INTERVAL_SECONDS,
    MIN_INTERVAL_SECONDS,
    validate_schedule_configuration,
    validate_timezone,
)
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class TriggerCreate(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    name: str = Field(min_length=1, max_length=255)
    trigger_type: Literal["schedule", "webhook"]
    graph_version_id: UUID | None = None
    enabled: bool = True
    schedule_kind: Literal["interval", "cron"] | None = None
    interval_seconds: int | None = Field(
        default=None,
        ge=MIN_INTERVAL_SECONDS,
        le=MAX_INTERVAL_SECONDS,
    )
    cron_expression: str | None = Field(default=None, min_length=1, max_length=255)
    timezone: str = Field(default="UTC", min_length=1, max_length=255)
    rate_limit_per_minute: int = Field(default=60, ge=1, le=10_000)

    @model_validator(mode="after")
    def validate_configuration(self) -> "TriggerCreate":
        if self.trigger_type == "webhook":
            if any(
                value is not None
                for value in (
                    self.schedule_kind,
                    self.interval_seconds,
                    self.cron_expression,
                )
            ):
                raise ValueError("schedule fields are not valid for a webhook trigger")
            self.timezone = validate_timezone(self.timezone)
            return self
        (
            self.schedule_kind,
            self.interval_seconds,
            self.cron_expression,
            self.timezone,
        ) = validate_schedule_configuration(
            schedule_kind=self.schedule_kind,
            interval_seconds=self.interval_seconds,
            cron_expression=self.cron_expression,
            timezone_name=self.timezone,
        )
        return self


class TriggerUpdate(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    name: str | None = Field(default=None, min_length=1, max_length=255)
    graph_version_id: UUID | None = None
    enabled: bool | None = None
    schedule_kind: Literal["interval", "cron"] | None = None
    interval_seconds: int | None = Field(
        default=None,
        ge=MIN_INTERVAL_SECONDS,
        le=MAX_INTERVAL_SECONDS,
    )
    cron_expression: str | None = Field(default=None, min_length=1, max_length=255)
    timezone: str | None = Field(default=None, min_length=1, max_length=255)
    rate_limit_per_minute: int | None = Field(default=None, ge=1, le=10_000)

    @field_validator("timezone")
    @classmethod
    def validate_timezone_field(cls, value: str | None) -> str | None:
        return validate_timezone(value) if value is not None else None

    @model_validator(mode="after")
    def reject_null_for_required_fields(self) -> "TriggerUpdate":
        """Distinguish an omitted PATCH field from an explicit JSON null."""
        for field_name in ("name", "enabled", "timezone", "rate_limit_per_minute"):
            if field_name in self.model_fields_set and getattr(self, field_name) is None:
                raise ValueError(f"{field_name} must not be null")
        return self


class TriggerResponse(BaseModel):
    """Management representation; encrypted webhook material is intentionally absent."""

    model_config = ConfigDict(from_attributes=True)

    id: UUID
    graph_id: UUID
    graph_version_id: UUID
    created_by: UUID
    name: str
    trigger_type: Literal["schedule", "webhook"]
    enabled: bool
    schedule_kind: Literal["interval", "cron"] | None
    interval_seconds: int | None
    cron_expression: str | None
    timezone: str
    next_run_at: datetime | None
    last_run_at: datetime | None
    last_run_id: UUID | None
    rate_limit_per_minute: int
    webhook_path: str | None
    created_at: datetime
    updated_at: datetime


class TriggerListResponse(BaseModel):
    triggers: list[TriggerResponse]
    total: int


class TriggerCreateResponse(BaseModel):
    """One-time create response; secret is absent from every later read."""

    trigger: TriggerResponse
    secret: str | None = None


class WebhookSecretResponse(BaseModel):
    trigger: TriggerResponse
    secret: str


class WebhookAcceptedResponse(BaseModel):
    run_id: UUID
    delivery_id: str
