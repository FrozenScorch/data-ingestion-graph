"""Committed and run-scoped candidate SDK source checkpoints."""

import uuid

from app.models.base import Base, TimestampMixin, UUIDMixin
from sqlalchemy import Boolean, ForeignKey, Index, Integer, String, UniqueConstraint, false
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column


class SDKSourceState(UUIDMixin, TimestampMixin, Base):
    __tablename__ = "sdk_source_states"
    __table_args__ = (
        UniqueConstraint(
            "owner_id",
            "graph_id",
            "node_id",
            "source",
            "stream",
            name="uq_sdk_source_state_scope",
        ),
    )

    owner_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("users.id", ondelete="CASCADE"), nullable=False
    )
    graph_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("graphs.id", ondelete="CASCADE"), nullable=False
    )
    node_id: Mapped[str] = mapped_column(String(255), nullable=False)
    source: Mapped[str] = mapped_column(String(255), nullable=False)
    stream: Mapped[str] = mapped_column(String(255), nullable=False)
    state_data: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    revision: Mapped[int] = mapped_column(
        Integer, nullable=False, default=1, server_default="1"
    )
    is_deleted: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=False, server_default=false()
    )


class SDKSourceStateCandidate(UUIDMixin, TimestampMixin, Base):
    """A source state transition waiting for its whole run to succeed."""

    __tablename__ = "sdk_source_state_candidates"
    __table_args__ = (
        UniqueConstraint(
            "run_id",
            "owner_id",
            "graph_id",
            "node_id",
            "source",
            "stream",
            name="uq_sdk_source_state_candidate_scope",
        ),
        Index("ix_sdk_source_state_candidates_run_id", "run_id"),
    )

    run_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("runs.id", ondelete="CASCADE"), nullable=False
    )
    owner_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("users.id", ondelete="CASCADE"), nullable=False
    )
    graph_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("graphs.id", ondelete="CASCADE"), nullable=False
    )
    node_id: Mapped[str] = mapped_column(String(255), nullable=False)
    source: Mapped[str] = mapped_column(String(255), nullable=False)
    stream: Mapped[str] = mapped_column(String(255), nullable=False)
    operation: Mapped[str] = mapped_column(String(20), nullable=False)
    state_data: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    base_state_data: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    base_revision: Mapped[int] = mapped_column(Integer, nullable=False)
