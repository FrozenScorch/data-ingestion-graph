"""Stage SDK source state until the whole graph run succeeds.

Revision ID: 0004_sdk_state_candidates
Revises: 0003_sdk_source_states
Create Date: 2026-07-12
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0004_sdk_state_candidates"
down_revision: str | None = "0003_sdk_source_states"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    committed_columns = {
        column["name"] for column in inspector.get_columns("sdk_source_states")
    }
    if "revision" not in committed_columns:
        op.add_column(
            "sdk_source_states",
            sa.Column("revision", sa.Integer(), server_default="1", nullable=False),
        )
    if "is_deleted" not in committed_columns:
        op.add_column(
            "sdk_source_states",
            sa.Column("is_deleted", sa.Boolean(), server_default=sa.false(), nullable=False),
        )

    if not inspector.has_table("sdk_source_state_candidates"):
        op.create_table(
            "sdk_source_state_candidates",
            sa.Column("run_id", postgresql.UUID(as_uuid=True), nullable=False),
            sa.Column("owner_id", postgresql.UUID(as_uuid=True), nullable=False),
            sa.Column("graph_id", postgresql.UUID(as_uuid=True), nullable=False),
            sa.Column("node_id", sa.String(length=255), nullable=False),
            sa.Column("source", sa.String(length=255), nullable=False),
            sa.Column("stream", sa.String(length=255), nullable=False),
            sa.Column("operation", sa.String(length=20), nullable=False),
            sa.Column("state_data", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
            sa.Column("base_state_data", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
            sa.Column("base_revision", sa.Integer(), nullable=False),
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
            sa.ForeignKeyConstraint(["graph_id"], ["graphs.id"], ondelete="CASCADE"),
            sa.ForeignKeyConstraint(["owner_id"], ["users.id"], ondelete="CASCADE"),
            sa.ForeignKeyConstraint(["run_id"], ["runs.id"], ondelete="CASCADE"),
            sa.PrimaryKeyConstraint("id"),
            sa.UniqueConstraint(
                "run_id",
                "owner_id",
                "graph_id",
                "node_id",
                "source",
                "stream",
                name="uq_sdk_source_state_candidate_scope",
            ),
        )
    existing_indexes = {
        index["name"]
        for index in sa.inspect(bind).get_indexes("sdk_source_state_candidates")
    }
    if "ix_sdk_source_state_candidates_run_id" not in existing_indexes:
        op.create_index(
            "ix_sdk_source_state_candidates_run_id",
            "sdk_source_state_candidates",
            ["run_id"],
        )


def downgrade() -> None:
    op.drop_index(
        "ix_sdk_source_state_candidates_run_id",
        table_name="sdk_source_state_candidates",
    )
    op.drop_table("sdk_source_state_candidates")
    source_states = sa.table(
        "sdk_source_states",
        sa.column("is_deleted", sa.Boolean()),
    )
    op.execute(source_states.delete().where(source_states.c.is_deleted.is_(True)))
    op.drop_column("sdk_source_states", "is_deleted")
    op.drop_column("sdk_source_states", "revision")
