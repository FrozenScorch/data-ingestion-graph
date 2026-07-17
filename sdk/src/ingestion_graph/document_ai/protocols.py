"""Async extension contracts for document extraction."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Protocol

from ingestion_graph.connectors.base import CheckResult
from ingestion_graph.document_ai.models import (
    ComponentDescriptor,
    OcrResult,
    SplitChunk,
    TableArtifact,
)
from ingestion_graph.models import DocumentElement


class OcrEngine(Protocol):
    @property
    def descriptor(self) -> ComponentDescriptor: ...

    async def check(self) -> CheckResult: ...

    async def recognize(self, image: bytes, *, language: str = "eng") -> OcrResult: ...

    async def close(self) -> None: ...


class PageRenderer(Protocol):
    @property
    def descriptor(self) -> ComponentDescriptor: ...

    async def render(self, source: bytes, *, page_number: int, dpi: int = 300) -> bytes: ...

    async def close(self) -> None: ...


class TableExtractor(Protocol):
    @property
    def descriptor(self) -> ComponentDescriptor: ...

    async def extract(
        self, image: bytes, *, page_number: int | None = None
    ) -> Sequence[TableArtifact]: ...

    async def close(self) -> None: ...


class LayoutAnalyzer(Protocol):
    @property
    def descriptor(self) -> ComponentDescriptor: ...

    async def analyze(self, image: bytes, *, page_number: int | None = None) -> object: ...

    async def close(self) -> None: ...


class VisionExtractor(Protocol):
    @property
    def descriptor(self) -> ComponentDescriptor: ...

    async def extract(
        self, image: bytes, *, schema: Mapping[str, object]
    ) -> Mapping[str, object]: ...

    async def close(self) -> None: ...


class ExternalProcessingPolicy(Protocol):
    async def authorize(self, *, purpose: str, page_number: int | None) -> bool: ...


class DocumentSplitter(Protocol):
    @property
    def descriptor(self) -> ComponentDescriptor: ...

    async def split(self, element: DocumentElement) -> Sequence[SplitChunk]: ...

    async def close(self) -> None: ...
