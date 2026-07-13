"""Add interval/cron schedules and signed webhook triggers.

Revision ID: 0005_graph_triggers
Revises: 0004_sdk_state_candidates
Create Date: 2026-07-13
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0005_graph_triggers"
down_revision: str | None = "0004_sdk_state_candidates"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    run_columns = {column["name"] for column in inspector.get_columns("runs")}
    if "trigger_payload" not in run_columns:
        op.add_column(
            "runs",
            sa.Column(
                "trigger_payload",
                postgresql.JSONB(astext_type=sa.Text()),
                nullable=True,
            ),
        )

    if not inspector.has_table("graph_triggers"):
        op.create_table(
            "graph_triggers",
            sa.Column("graph_id", postgresql.UUID(as_uuid=True), nullable=False),
            sa.Column("graph_version_id", postgresql.UUID(as_uuid=True), nullable=False),
            sa.Column("created_by", postgresql.UUID(as_uuid=True), nullable=False),
            sa.Column("name", sa.String(length=255), nullable=False),
            sa.Column("trigger_type", sa.String(length=20), nullable=False),
            sa.Column(
                "enabled",
                sa.Boolean(),
                server_default=sa.true(),
                nullable=False,
            ),
            sa.Column("schedule_kind", sa.String(length=20), nullable=True),
            sa.Column("interval_seconds", sa.Integer(), nullable=True),
            sa.Column("cron_expression", sa.String(length=255), nullable=True),
            sa.Column(
                "timezone",
                sa.String(length=255),
                server_default="UTC",
                nullable=False,
            ),
            sa.Column("next_run_at", sa.DateTime(timezone=True), nullable=True),
            sa.Column("last_run_at", sa.DateTime(timezone=True), nullable=True),
            sa.Column("last_run_id", postgresql.UUID(as_uuid=True), nullable=True),
            sa.Column(
                "webhook_secret",
                postgresql.JSONB(astext_type=sa.Text()),
                nullable=True,
            ),
            sa.Column(
                "rate_limit_per_minute",
                sa.Integer(),
                server_default="60",
                nullable=False,
            ),
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
            sa.CheckConstraint(
                "trigger_type IN ('schedule', 'webhook')",
                name="ck_graph_triggers_type",
            ),
            sa.CheckConstraint(
                "rate_limit_per_minute >= 1 AND rate_limit_per_minute <= 10000",
                name="ck_graph_triggers_rate_limit",
            ),
            sa.CheckConstraint(
                "(trigger_type = 'webhook' AND webhook_secret IS NOT NULL) OR "
                "(trigger_type = 'schedule' AND webhook_secret IS NULL)",
                name="ck_graph_triggers_secret",
            ),
            sa.CheckConstraint(
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
            sa.ForeignKeyConstraint(
                ["created_by"],
                ["users.id"],
                ondelete="RESTRICT",
            ),
            sa.ForeignKeyConstraint(
                ["graph_id"],
                ["graphs.id"],
                ondelete="CASCADE",
            ),
            sa.ForeignKeyConstraint(
                ["graph_version_id"],
                ["graph_versions.id"],
                ondelete="RESTRICT",
            ),
            sa.ForeignKeyConstraint(
                ["last_run_id"],
                ["runs.id"],
                ondelete="SET NULL",
            ),
            sa.PrimaryKeyConstraint("id"),
            sa.UniqueConstraint(
                "graph_id",
                "name",
                name="uq_graph_triggers_graph_name",
            ),
        )
        op.create_index(
            "ix_graph_triggers_due",
            "graph_triggers",
            ["trigger_type", "enabled", "next_run_at"],
        )
        op.create_index(
            "ix_graph_triggers_graph_id",
            "graph_triggers",
            ["graph_id"],
        )

    inspector = sa.inspect(bind)
    if not inspector.has_table("webhook_deliveries"):
        op.create_table(
            "webhook_deliveries",
            sa.Column("trigger_id", postgresql.UUID(as_uuid=True), nullable=False),
            sa.Column("delivery_id", sa.String(length=255), nullable=False),
            sa.Column("body_sha256", sa.String(length=64), nullable=False),
            sa.Column(
                "received_at",
                sa.DateTime(timezone=True),
                server_default=sa.func.now(),
                nullable=False,
            ),
            sa.Column("run_id", postgresql.UUID(as_uuid=True), nullable=True),
            sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
            sa.CheckConstraint(
                "char_length(body_sha256) = 64",
                name="ck_webhook_deliveries_body_sha256",
            ),
            sa.ForeignKeyConstraint(
                ["run_id"],
                ["runs.id"],
                ondelete="SET NULL",
            ),
            sa.ForeignKeyConstraint(
                ["trigger_id"],
                ["graph_triggers.id"],
                ondelete="CASCADE",
            ),
            sa.PrimaryKeyConstraint("id"),
            sa.UniqueConstraint(
                "trigger_id",
                "delivery_id",
                name="uq_webhook_deliveries_trigger_delivery",
            ),
        )
        op.create_index(
            "ix_webhook_deliveries_trigger_received",
            "webhook_deliveries",
            ["trigger_id", "received_at"],
        )
        op.create_index(
            "ix_webhook_deliveries_received_at",
            "webhook_deliveries",
            ["received_at"],
        )


def downgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    if inspector.has_table("webhook_deliveries"):
        op.drop_table("webhook_deliveries")
    inspector = sa.inspect(bind)
    if inspector.has_table("graph_triggers"):
        op.drop_table("graph_triggers")
    run_columns = {column["name"] for column in sa.inspect(bind).get_columns("runs")}
    if "trigger_payload" in run_columns:
        op.drop_column("runs", "trigger_payload")
