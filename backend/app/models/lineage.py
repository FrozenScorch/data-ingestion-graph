"""
Data lineage and provenance models.
"""
import uuid
from sqlalchemy import String, Integer, BigInteger, Text, ForeignKey
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.models.base import Base, UUIDMixin


class DataLineage(UUIDMixin, Base):
    __tablename__ = "data_lineage"

    run_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("runs.id"), nullable=False,
    )
    source_node_id: Mapped[str] = mapped_column(String(255), nullable=False)
    target_node_id: Mapped[str] = mapped_column(String(255), nullable=False)
    source_port: Mapped[str | None] = mapped_column(String(255), nullable=True)
    target_port: Mapped[str | None] = mapped_column(String(255), nullable=True)
    items_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    items_sample: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    bytes_transferred: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    created_at = mapped_column(
        __import__("sqlalchemy").DateTime(timezone=True),
        server_default="now()",
        nullable=False,
    )

    # Relationship back to Run
    run = relationship("Run", back_populates="data_lineage")

    def __repr__(self) -> str:
        return f"<DataLineage id={self.id} {self.source_node_id}->{self.target_node_id}>"


class Provenance(UUIDMixin, Base):
    __tablename__ = "provenance"

    run_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("runs.id"), nullable=False,
    )
    source_type: Mapped[str] = mapped_column(String(100), nullable=False)
    source_ref: Mapped[str] = mapped_column(Text, nullable=False)
    output_target: Mapped[str | None] = mapped_column(Text, nullable=True)
    records_affected: Mapped[int | None] = mapped_column(Integer, nullable=True)
    metadata_: Mapped[dict | None] = mapped_column("metadata", JSONB, nullable=True)
    created_at = mapped_column(
        __import__("sqlalchemy").DateTime(timezone=True),
        server_default="now()",
        nullable=False,
    )

    run = relationship("Run", back_populates="provenance")

    def __repr__(self) -> str:
        return f"<Provenance id={self.id} source={self.source_type}:{self.source_ref}>"
