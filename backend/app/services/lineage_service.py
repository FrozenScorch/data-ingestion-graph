"""
Lineage service: query data lineage and provenance records.
"""
from typing import Optional
from uuid import UUID

from app.models.lineage import DataLineage, Provenance
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession


async def get_lineage_for_run(
    db: AsyncSession,
    run_id: UUID,
) -> list[DataLineage]:
    """Get all lineage entries for a specific run, ordered by creation time."""
    result = await db.execute(
        select(DataLineage)
        .where(DataLineage.run_id == run_id)
        .order_by(DataLineage.created_at.asc())
    )
    return list(result.scalars().all())


async def get_lineage_for_graph(
    db: AsyncSession,
    graph_id: UUID,
    limit: int = 100,
) -> list[DataLineage]:
    """
    Get lineage entries across all runs for a specific graph.

    Joins through the Run table to filter by graph_id.
    """
    from app.models.execution import Run

    result = await db.execute(
        select(DataLineage)
        .join(Run, DataLineage.run_id == Run.id)
        .where(Run.graph_id == graph_id)
        .order_by(DataLineage.created_at.desc())
        .limit(limit)
    )
    return list(result.scalars().all())


async def get_lineage_for_source(
    db: AsyncSession,
    source_ref: str,
    owner_id: UUID | None = None,
) -> list[dict]:
    """
    Find all runs that consumed a specific source.

    Searches provenance records by source_ref and returns lineage
    entries associated with those runs.
    """
    # First, find runs that have provenance records for this source
    provenance_query = select(Provenance).where(Provenance.source_ref == source_ref)
    if owner_id is not None:
        from app.models.execution import Run
        from app.models.graph import Graph

        provenance_query = (
            provenance_query.join(Run, Provenance.run_id == Run.id)
            .join(Graph, Run.graph_id == Graph.id)
            .where(Graph.owner_id == owner_id)
        )
    provenance_result = await db.execute(
        provenance_query.order_by(Provenance.created_at.desc())
    )
    provenance_records = list(provenance_result.scalars().all())

    run_ids = list(set(p.run_id for p in provenance_records))

    if not run_ids:
        return []

    # Get lineage for those runs
    lineage_result = await db.execute(
        select(DataLineage)
        .where(DataLineage.run_id.in_(run_ids))
        .order_by(DataLineage.created_at.asc())
    )
    lineage_entries = list(lineage_result.scalars().all())

    return [
        {
            "provenance": p,
            "lineage": [l for l in lineage_entries if l.run_id == p.run_id],
        }
        for p in provenance_records
    ]


async def get_provenance_for_run(
    db: AsyncSession,
    run_id: UUID,
) -> list[Provenance]:
    """Get provenance records for a specific run."""
    result = await db.execute(
        select(Provenance)
        .where(Provenance.run_id == run_id)
        .order_by(Provenance.created_at.asc())
    )
    return list(result.scalars().all())


async def record_provenance(
    db: AsyncSession,
    run_id: UUID,
    source_type: str,
    source_ref: str,
    output_target: Optional[str] = None,
    records_affected: Optional[int] = None,
    metadata: Optional[dict] = None,
) -> Provenance:
    """Create a new provenance record."""
    provenance = Provenance(
        run_id=run_id,
        source_type=source_type,
        source_ref=source_ref,
        output_target=output_target,
        records_affected=records_affected,
        metadata_=metadata,
    )
    db.add(provenance)
    await db.commit()
    await db.refresh(provenance)
    return provenance
