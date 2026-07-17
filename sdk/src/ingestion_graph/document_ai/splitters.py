"""Splitter helpers and legacy identity behavior."""

from __future__ import annotations

from collections.abc import Sequence

from ingestion_graph.document_ai.models import ComponentDescriptor, SplitChunk
from ingestion_graph.models import DocumentElement


class IdentitySplitter:
    descriptor = ComponentDescriptor("identity", "1", deterministic=True)

    async def split(self, element: DocumentElement) -> Sequence[SplitChunk]:
        return [
            SplitChunk(
                element.text, element.element_type, element.page_number, element.parent_id, None
            )
        ]

    async def close(self) -> None:
        return None
