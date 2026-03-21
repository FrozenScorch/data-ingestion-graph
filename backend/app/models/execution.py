"""
Execution models: runs, run_nodes, checkpoints, execution_logs, run_costs.
"""
import enum
import uuid
from datetime import datetime
from sqlalchemy import String, Integer, Float, DateTime, ForeignKey, Text, Index
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.models.base import Base, TimestampMixin, UUIDMixin


class TriggerType(str, enum.Enum):
    MANUAL = "manual"
    WEBHOOK = "webhook"
    SCHEDULE = "schedule"


class RunStatus(str, enum.Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    PAUSED = "paused"


class NodeStatus(str, enum.Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"
    RETRYING = "retrying"


class CheckpointType(str, enum.Enum):
    PRE_EXEC = "pre_exec"
    POST_EXEC = "post_exec"


class LogLevel(str, enum.Enum):
    DEBUG = "debug"
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


class Run(UUIDMixin, TimestampMixin, Base):
    __tablename__ = "runs"
    __table_args__ = (
        Index("ix_runs_graph_id_status", "graph_id", "status"),
    )

    graph_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("graphs.id"), nullable=False)
    graph_version_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), ForeignKey("graph_versions.id"), nullable=True)
    trigger_type: Mapped[str] = mapped_column(String(50), default=TriggerType.MANUAL.value, nullable=False)
    triggered_by: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), ForeignKey("users.id"), nullable=True)
    status: Mapped[str] = mapped_column(String(50), default=RunStatus.PENDING.value, nullable=False)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)

    # Relationships -- all use lazy="noload" to prevent N+1 queries.
    # Use explicit selectinload() in queries that need these.
    graph = relationship("Graph", back_populates="runs", lazy="noload")
    graph_version = relationship("GraphVersion", back_populates="runs", lazy="noload")
    triggered_by_user = relationship("User", back_populates="runs", foreign_keys=[triggered_by], lazy="noload")
    run_nodes = relationship("RunNode", back_populates="run", lazy="noload")
    checkpoints = relationship("Checkpoint", back_populates="run", lazy="noload")
    execution_logs = relationship("ExecutionLog", back_populates="run", lazy="noload")
    run_costs = relationship("RunCost", back_populates="run", lazy="noload")
    data_lineage = relationship("DataLineage", back_populates="run", lazy="noload")
    provenance = relationship("Provenance", back_populates="run", lazy="noload")

    def __repr__(self) -> str:
        return f"<Run id={self.id} status={self.status}>"


class RunNode(UUIDMixin, TimestampMixin, Base):
    __tablename__ = "run_nodes"
    __table_args__ = (
        Index("ix_run_nodes_run_id_status", "run_id", "status"),
    )

    run_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("runs.id"), nullable=False)
    node_id: Mapped[str] = mapped_column(String(255), nullable=False)
    node_type: Mapped[str] = mapped_column(String(100), nullable=False)
    status: Mapped[str] = mapped_column(String(50), default=NodeStatus.PENDING.value, nullable=False)
    attempt_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    max_retries: Mapped[int] = mapped_column(Integer, default=3, nullable=False)
    input_data: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    output_data: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    items_processed: Mapped[int | None] = mapped_column(Integer, nullable=True)
    duration_ms: Mapped[int | None] = mapped_column(Integer, nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)

    # Relationships
    run = relationship("Run", back_populates="run_nodes")

    def __repr__(self) -> str:
        return f"<RunNode id={self.id} node_id={self.node_id} status={self.status}>"


class Checkpoint(UUIDMixin, Base):
    __tablename__ = "checkpoints"

    run_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("runs.id"), nullable=False)
    node_id: Mapped[str] = mapped_column(String(255), nullable=False)
    checkpoint_type: Mapped[str] = mapped_column(String(50), nullable=False)
    state_data: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    node_output: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    created_at = mapped_column(
        DateTime(timezone=True),
        server_default="now()",
        nullable=False,
    )

    # Relationships
    run = relationship("Run", back_populates="checkpoints")

    def __repr__(self) -> str:
        return f"<Checkpoint id={self.id} run_id={self.run_id} node_id={self.node_id}>"


class ExecutionLog(UUIDMixin, Base):
    __tablename__ = "execution_logs"
    __table_args__ = (
        Index("ix_execution_logs_run_id_level", "run_id", "level"),
    )

    run_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("runs.id"), nullable=False)
    run_node_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), ForeignKey("run_nodes.id"), nullable=True)
    node_id: Mapped[str | None] = mapped_column(String(255), nullable=True)
    level: Mapped[str] = mapped_column(String(20), default=LogLevel.INFO.value, nullable=False)
    message: Mapped[str] = mapped_column(Text, nullable=False)
    structured_data: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    created_at = mapped_column(
        DateTime(timezone=True),
        server_default="now()",
        nullable=False,
    )

    # Relationships
    run = relationship("Run", back_populates="execution_logs")

    def __repr__(self) -> str:
        return f"<ExecutionLog id={self.id} level={self.level}>"


class RunCost(UUIDMixin, Base):
    __tablename__ = "run_costs"
    __table_args__ = (
        Index("ix_run_costs_run_id", "run_id"),
    )

    run_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("runs.id"), nullable=False)
    run_node_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), ForeignKey("run_nodes.id"), nullable=True)
    provider: Mapped[str] = mapped_column(String(100), nullable=False)
    model_id: Mapped[str] = mapped_column(String(255), nullable=False)
    input_tokens: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    output_tokens: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    input_cost_usd: Mapped[float] = mapped_column(Float, default=0.0, nullable=False)
    output_cost_usd: Mapped[float] = mapped_column(Float, default=0.0, nullable=False)
    total_cost_usd: Mapped[float] = mapped_column(Float, default=0.0, nullable=False)
    created_at = mapped_column(
        DateTime(timezone=True),
        server_default="now()",
        nullable=False,
    )

    # Relationships
    run = relationship("Run", back_populates="run_costs")

    def __repr__(self) -> str:
        return f"<RunCost id={self.id} model={self.model_id} cost={self.total_cost_usd}>"
