"""Built-in connector interfaces and implementations."""

from ingestion_graph.connectors.base import (
    CheckResult,
    ConnectorCapabilities,
    ConnectorSpec,
    Destination,
    Source,
    StreamDescriptor,
)

__all__ = [
    "CheckResult",
    "ConnectorCapabilities",
    "ConnectorSpec",
    "Destination",
    "Source",
    "StreamDescriptor",
]
