"""Durable SDK source checkpoints owned by Studio graphs."""

import uuid

from app.models.base import Base, TimestampMixin, UUIDMixin
from sqlalchemy import ForeignKey, String, UniqueConstraint
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
