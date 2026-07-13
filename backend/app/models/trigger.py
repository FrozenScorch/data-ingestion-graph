"""Persistent schedule and signed-webhook trigger models."""

import uuid
from datetime import datetime
from enum import StrEnum

from app.models.base import Base, TimestampMixin, UUIDMixin
from sqlalchemy import (
    Boolean,
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship


class GraphTriggerType(StrEnum):
    SCHEDULE = "schedule"
    WEBHOOK = "webhook"


class ScheduleKind(StrEnum):
    INTERVAL = "interval"
    CRON = "cron"


class GraphTrigger(UUIDMixin, TimestampMixin, Base):
    """An owner-managed trigger pinned to one immutable graph version."""

    __tablename__ = "graph_triggers"
    __table_args__ = (
        UniqueConstraint("graph_id", "name", name="uq_graph_triggers_graph_name"),
        CheckConstraint(
            "trigger_type IN ('schedule', 'webhook')",
            name="ck_graph_triggers_type",
        ),
        CheckConstraint(
            "rate_limit_per_minute >= 1 AND rate_limit_per_minute <= 10000",
            name="ck_graph_triggers_rate_limit",
        ),
        CheckConstraint(
            "(trigger_type = 'webhook' AND webhook_secret IS NOT NULL) OR "
            "(trigger_type = 'schedule' AND webhook_secret IS NULL)",
            name="ck_graph_triggers_secret",
        ),
        CheckConstraint(
            "(trigger_type = 'schedule' AND ("
            "(schedule_kind = 'interval' AND interval_seconds IS NOT NULL "
            "AND interval_seconds >= 60 AND interval_seconds <= 31536000 "
            "AND cron_expression IS NULL) OR "
            "(schedule_kind = 'cron' AND interval_seconds IS NULL "
            "AND cron_expression IS NOT NULL))) OR "
            "(trigger_type = 'webhook' AND schedule_kind IS NULL "
            "AND interval_seconds IS NULL AND cron_expression IS NULL "
            "AND next_run_at IS NULL)",
            name="ck_graph_triggers_configuration",
        ),
        Index(
            "ix_graph_triggers_due",
            "trigger_type",
            "enabled",
            "next_run_at",
        ),
        Index("ix_graph_triggers_graph_id", "graph_id"),
    )

    graph_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("graphs.id", ondelete="CASCADE"),
        nullable=False,
    )
    graph_version_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("graph_versions.id", ondelete="RESTRICT"),
        nullable=False,
    )
    created_by: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("users.id", ondelete="RESTRICT"),
        nullable=False,
    )
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    trigger_type: Mapped[str] = mapped_column(String(20), nullable=False)
    enabled: Mapped[bool] = mapped_column(
        Boolean,
        default=True,
        server_default="true",
        nullable=False,
    )
    schedule_kind: Mapped[str | None] = mapped_column(String(20), nullable=True)
    interval_seconds: Mapped[int | None] = mapped_column(Integer, nullable=True)
    cron_expression: Mapped[str | None] = mapped_column(String(255), nullable=True)
    timezone: Mapped[str] = mapped_column(
        String(255),
        default="UTC",
        server_default="UTC",
        nullable=False,
    )
    next_run_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    last_run_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    last_run_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("runs.id", ondelete="SET NULL"),
        nullable=True,
    )
    webhook_secret: Mapped[dict[str, str] | None] = mapped_column(
        JSONB(none_as_null=True),
        nullable=True,
    )
    rate_limit_per_minute: Mapped[int] = mapped_column(
        Integer,
        default=60,
        server_default="60",
        nullable=False,
    )

    graph = relationship("Graph", lazy="noload")
    graph_version = relationship("GraphVersion", lazy="noload")
    creator = relationship("User", foreign_keys=[created_by], lazy="noload")
    last_run = relationship("Run", foreign_keys=[last_run_id], lazy="noload")
    deliveries = relationship(
        "WebhookDelivery",
        back_populates="trigger",
        cascade="all, delete-orphan",
        passive_deletes=True,
        lazy="noload",
    )

    @property
    def webhook_path(self) -> str | None:
        if self.trigger_type != GraphTriggerType.WEBHOOK.value:
            return None
        return f"/api/webhooks/{self.id}"


class WebhookDelivery(UUIDMixin, Base):
    """Replay/rate-limit ledger for accepted webhook deliveries."""

    __tablename__ = "webhook_deliveries"
    __table_args__ = (
        UniqueConstraint(
            "trigger_id",
            "delivery_id",
            name="uq_webhook_deliveries_trigger_delivery",
        ),
        CheckConstraint(
            "char_length(body_sha256) = 64",
            name="ck_webhook_deliveries_body_sha256",
        ),
        Index(
            "ix_webhook_deliveries_trigger_received",
            "trigger_id",
            "received_at",
        ),
        Index("ix_webhook_deliveries_received_at", "received_at"),
    )

    trigger_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("graph_triggers.id", ondelete="CASCADE"),
        nullable=False,
    )
    delivery_id: Mapped[str] = mapped_column(String(255), nullable=False)
    body_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    received_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )
    run_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("runs.id", ondelete="SET NULL"),
        nullable=True,
    )

    trigger = relationship("GraphTrigger", back_populates="deliveries")
    run = relationship("Run", foreign_keys=[run_id], lazy="noload")
