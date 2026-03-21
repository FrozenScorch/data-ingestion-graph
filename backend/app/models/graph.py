"""
Graph, graph version, and connection models.
"""
import enum
import uuid
from datetime import datetime
from sqlalchemy import String, Boolean, DateTime, ForeignKey, Text, Integer, ARRAY, Index, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.models.base import Base, TimestampMixin, UUIDMixin


class GraphStatus(str, enum.Enum):
    DRAFT = "draft"
    ACTIVE = "active"
    ARCHIVED = "archived"


class ConnectionType(str, enum.Enum):
    POSTGRES = "postgres"
    DISCORD = "discord"
    GITHUB = "github"
    WEBHOOK = "webhook"


class Graph(UUIDMixin, TimestampMixin, Base):
    __tablename__ = "graphs"
    __table_args__ = (
        Index("ix_graphs_owner_id_status", "owner_id", "status"),
    )

    name: Mapped[str] = mapped_column(String(255), nullable=False)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    owner_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("users.id"), nullable=False)
    status: Mapped[str] = mapped_column(String(50), default=GraphStatus.DRAFT.value, nullable=False)
    tags: Mapped[list[str] | None] = mapped_column(ARRAY(String), nullable=True, default=list)

    # Relationships -- use lazy="noload" to prevent N+1 queries.
    # Use explicit selectinload() in queries that need these.
    owner = relationship("User", back_populates="graphs", lazy="noload")
    versions = relationship("GraphVersion", back_populates="graph", lazy="noload", order_by="GraphVersion.version_number.desc()")
    runs = relationship("Run", back_populates="graph", lazy="noload")

    def __repr__(self) -> str:
        return f"<Graph id={self.id} name={self.name} status={self.status}>"


class GraphVersion(UUIDMixin, TimestampMixin, Base):
    __tablename__ = "graph_versions"
    __table_args__ = (
        UniqueConstraint("graph_id", "version_number", name="uq_graph_version"),
    )

    graph_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("graphs.id"), nullable=False)
    version_number: Mapped[int] = mapped_column(Integer, nullable=False)
    nodes_data: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    edges_data: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    node_configs: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    checksum: Mapped[str | None] = mapped_column(String(64), nullable=True)

    # Relationships
    graph = relationship("Graph", back_populates="versions")
    runs = relationship("Run", back_populates="graph_version")

    def __repr__(self) -> str:
        return f"<GraphVersion id={self.id} graph_id={self.graph_id} version={self.version_number}>"


class Connection(UUIDMixin, TimestampMixin, Base):
    __tablename__ = "connections"

    user_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("users.id"), nullable=False)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    type: Mapped[str] = mapped_column(String(50), nullable=False)
    config: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    is_valid: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)

    def __repr__(self) -> str:
        return f"<Connection id={self.id} name={self.name} type={self.type}>"
