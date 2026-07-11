"""Typed contracts for querying an ingestion collection."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from typing import overload

from ingestion_graph.models import Envelope


@dataclass(frozen=True, slots=True)
class QueryRequest:
    """A portable full-text query over the current ingested view."""

    text: str | None = None
    limit: int = 10
    offset: int = 0
    source: str | None = None
    stream: str | None = None

    def __post_init__(self) -> None:
        if not 1 <= self.limit <= 1000:
            raise ValueError("Query limit must be between 1 and 1000")
        if self.offset < 0:
            raise ValueError("Query offset must not be negative")
        if self.source is not None and not self.source:
            raise ValueError("Query source must not be empty")
        if self.stream is not None and not self.stream:
            raise ValueError("Query stream must not be empty")


# Short spelling retained as the primary ergonomic API.
Query = QueryRequest


@dataclass(frozen=True, slots=True)
class QueryHit:
    envelope: Envelope
    score: float


@dataclass(frozen=True, slots=True)
class QueryResult(Sequence[QueryHit]):
    request: QueryRequest
    hits: tuple[QueryHit, ...]
    total: int

    @overload
    def __getitem__(self, index: int) -> QueryHit: ...

    @overload
    def __getitem__(self, index: slice) -> tuple[QueryHit, ...]: ...

    def __getitem__(self, index: int | slice) -> QueryHit | tuple[QueryHit, ...]:
        return self.hits[index]

    def __len__(self) -> int:
        return len(self.hits)

    def __iter__(self) -> Iterator[QueryHit]:
        return iter(self.hits)


class QueryStore(ABC):
    """Read contract implemented by local and remote searchable collections."""

    @abstractmethod
    async def get(self, source: str, stream: str, record_id: str) -> Envelope | None: ...

    @abstractmethod
    async def query(self, request: QueryRequest) -> QueryResult: ...
