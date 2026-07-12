"""Reusable transform contract for checkpoint-bounded envelope batches."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Sequence

from ingestion_graph.models import Envelope


class Transform(ABC):
    """Map, filter, or expand a batch before it reaches a destination.

    A transform receives one source batch ending at a state checkpoint. It may
    return zero or more envelopes, but every returned envelope must remain in
    the source stream being checkpointed. Pipeline state advances only after
    the complete transform chain has run and the destination has flushed.
    """

    @abstractmethod
    async def apply(self, records: Sequence[Envelope]) -> Sequence[Envelope]: ...

    async def close(self) -> None:
        """Release transform resources."""
        return None
