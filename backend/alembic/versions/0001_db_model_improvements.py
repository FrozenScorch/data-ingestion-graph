"""Add unique constraint on graph_versions, missing indexes on multiple tables.

Revision ID: 0001_db_model_improvements
Revises: None
Create Date: 2026-03-20
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = "0001_db_model_improvements"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_unique_constraint(
        "uq_graph_version",
        "graph_versions",
        ["graph_id", "version_number"],
    )
    op.create_index("ix_runs_graph_id_status", "runs", ["graph_id", "status"])
    op.create_index("ix_run_nodes_run_id_status", "run_nodes", ["run_id", "status"])
    op.create_index("ix_data_lineage_run_id", "data_lineage", ["run_id"])
    op.create_index("ix_data_lineage_source_target", "data_lineage", ["source_node_id", "target_node_id"])
    op.create_index("ix_dead_letter_queue_run_id_resolved", "dead_letter_queue", ["run_id", "resolved"])
    op.create_index("ix_execution_logs_run_id_level", "execution_logs", ["run_id", "level"])
    op.create_index("ix_graphs_owner_id_status", "graphs", ["owner_id", "status"])
    op.create_index("ix_run_costs_run_id", "run_costs", ["run_id"])


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
