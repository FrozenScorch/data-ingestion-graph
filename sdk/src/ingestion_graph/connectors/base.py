"""Connector contracts. Implementations must remain independent of the server app."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import AsyncIterator, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

from ingestion_graph.messages import SourceMessage
from ingestion_graph.models import Envelope


@dataclass(frozen=True, slots=True)
class ConnectorCapabilities:
    incremental: bool = False
    resumable_full_refresh: bool = False
    deletes: bool = False
    schema_discovery: bool = False
    rate_limits: bool = False


@dataclass(frozen=True, slots=True)
class ConnectorSpec:
    name: str
    version: str
    config_schema: Mapping[str, Any]
    capabilities: ConnectorCapabilities = field(default_factory=ConnectorCapabilities)


@dataclass(frozen=True, slots=True)
class CheckResult:
    ok: bool
    message: str = ""


@dataclass(frozen=True, slots=True)
class StreamDescriptor:
    name: str
    namespace: str | None = None
    json_schema: Mapping[str, Any] = field(default_factory=dict)
    primary_key: Sequence[str] = field(default_factory=tuple)
    cursor_field: Sequence[str] = field(default_factory=tuple)


class Source(ABC):
    @classmethod
    def manifest(cls) -> ConnectorSpec:
        """Return constructor-free connector metadata when the source supports it.

        Existing third-party sources that only implement ``spec()`` remain valid.
        Manifest-aware registries should treat ``NotImplementedError`` as an
        explicit legacy-plugin boundary rather than instantiate the connector.
        """
        raise NotImplementedError(f"{cls.__name__} does not expose a connector manifest")

    @abstractmethod
    def spec(self) -> ConnectorSpec: ...

    @abstractmethod
    async def check(self) -> CheckResult: ...

    @abstractmethod
    async def discover(self) -> Sequence[StreamDescriptor]: ...

    @abstractmethod
    def read(
        self,
        stream: StreamDescriptor,
        state: Mapping[str, Any] | None = None,
    ) -> AsyncIterator[SourceMessage]: ...

    async def close(self) -> None:
        return None


class Destination(ABC):
    idempotent: bool = False

    @abstractmethod
    async def check(self) -> CheckResult: ...

    @abstractmethod
    async def write(self, records: Sequence[Envelope]) -> int:
        """Durably accept records and return the number newly written."""
        ...

    @abstractmethod
    async def flush(self) -> None: ...

    async def close(self) -> None:
        return None
