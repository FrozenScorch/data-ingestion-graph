"""Add unique constraint on graph_versions, missing indexes on multiple tables.

Revision ID: 0001_db_model_improvements
Revises: None
Create Date: 2026-03-20
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0001_db_model_improvements"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    unique_names = {
        constraint["name"] for constraint in inspector.get_unique_constraints("graph_versions")
    }
    if "uq_graph_version" not in unique_names:
        op.create_unique_constraint(
            "uq_graph_version",
            "graph_versions",
            ["graph_id", "version_number"],
        )

    indexes = (
        ("ix_runs_graph_id_status", "runs", ["graph_id", "status"]),
        ("ix_run_nodes_run_id_status", "run_nodes", ["run_id", "status"]),
        ("ix_data_lineage_run_id", "data_lineage", ["run_id"]),
        (
            "ix_data_lineage_source_target",
            "data_lineage",
            ["source_node_id", "target_node_id"],
        ),
        (
            "ix_dead_letter_queue_run_id_resolved",
            "dead_letter_queue",
            ["run_id", "resolved"],
        ),
        ("ix_execution_logs_run_id_level", "execution_logs", ["run_id", "level"]),
        ("ix_graphs_owner_id_status", "graphs", ["owner_id", "status"]),
        ("ix_run_costs_run_id", "run_costs", ["run_id"]),
    )
    for name, table, columns in indexes:
        existing = {index["name"] for index in sa.inspect(bind).get_indexes(table)}
        if name not in existing:
            op.create_index(name, table, columns)


def downgrade() -> None:
    op.drop_index("ix_run_costs_run_id", table_name="run_costs")
    op.drop_index("ix_graphs_owner_id_status", table_name="graphs")
    op.drop_index("ix_execution_logs_run_id_level", table_name="execution_logs")
    op.drop_index("ix_dead_letter_queue_run_id_resolved", table_name="dead_letter_queue")
    op.drop_index("ix_data_lineage_source_target", table_name="data_lineage")
    op.drop_index("ix_data_lineage_run_id", table_name="data_lineage")
    op.drop_index("ix_run_nodes_run_id_status", table_name="run_nodes")
    op.drop_index("ix_runs_graph_id_status", table_name="runs")
    op.drop_constraint("uq_graph_version", "graph_versions", type_="unique")
