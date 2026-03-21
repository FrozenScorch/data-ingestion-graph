"""
Dead letter queue model for failed processing items.
"""
import uuid
from datetime import datetime
from sqlalchemy import String, Integer, Boolean, Text, Index
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.orm import Mapped, mapped_column

from app.models.base import Base, UUIDMixin


class DeadLetterQueue(UUIDMixin, Base):
    __tablename__ = "dead_letter_queue"
    __table_args__ = (
        Index("ix_dead_letter_queue_run_id_resolved", "run_id", "resolved"),
    )

    run_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True)
    node_id: Mapped[str | None] = mapped_column(String(255), nullable=True)
    node_type: Mapped[str | None] = mapped_column(String(100), nullable=True)
    error_type: Mapped[str | None] = mapped_column(String(255), nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    input_data: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    retry_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    resolved: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    resolution_note: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at = mapped_column(
        __import__("sqlalchemy").DateTime(timezone=True),
        server_default="now()",
        nullable=False,
    )
    updated_at = mapped_column(
        __import__("sqlalchemy").DateTime(timezone=True),
        server_default="now()",
        onupdate="now()",
        nullable=False,
    )

    def __repr__(self) -> str:
        return f"<DLQ id={self.id} node_type={self.node_type} resolved={self.resolved}>"
