"""Add durable leased run jobs.

Revision ID: 0002_durable_run_jobs
Revises: 0001_db_model_improvements
Create Date: 2026-07-12
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0002_durable_run_jobs"
down_revision: str | None = "0001_db_model_improvements"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    bind = op.get_bind()
    if not sa.inspect(bind).has_table("run_jobs"):
        op.create_table(
            "run_jobs",
            sa.Column("run_id", postgresql.UUID(as_uuid=True), nullable=False),
            sa.Column("job_type", sa.String(length=50), nullable=False),
            sa.Column("status", sa.String(length=50), nullable=False),
            sa.Column("available_at", sa.DateTime(timezone=True), nullable=False),
            sa.Column("lease_owner", sa.String(length=255), nullable=True),
            sa.Column("lease_expires_at", sa.DateTime(timezone=True), nullable=True),
            sa.Column("heartbeat_at", sa.DateTime(timezone=True), nullable=True),
            sa.Column("attempt_count", sa.Integer(), nullable=False),
            sa.Column("last_error", sa.Text(), nullable=True),
            sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
            sa.Column(
                "created_at",
                sa.DateTime(timezone=True),
                server_default=sa.func.now(),
                nullable=False,
            ),
            sa.Column(
                "updated_at",
                sa.DateTime(timezone=True),
                server_default=sa.func.now(),
                nullable=False,
            ),
            sa.ForeignKeyConstraint(["run_id"], ["runs.id"], ondelete="CASCADE"),
            sa.PrimaryKeyConstraint("id"),
            sa.UniqueConstraint("run_id", name="uq_run_jobs_run_id"),
        )
    existing = {index["name"] for index in sa.inspect(bind).get_indexes("run_jobs")}
    if "ix_run_jobs_claim" not in existing:
        op.create_index(
            "ix_run_jobs_claim",
            "run_jobs",
            ["status", "available_at", "lease_expires_at"],
        )


def downgrade() -> None:
    op.drop_index("ix_run_jobs_claim", table_name="run_jobs")
    op.drop_table("run_jobs")
