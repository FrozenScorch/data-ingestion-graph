"""Built-in connector interfaces and implementations."""

from ingestion_graph.connectors.base import (
    CheckResult,
    ConnectorCapabilities,
    ConnectorSpec,
    Destination,
    Source,
    StreamDescriptor,
)
from ingestion_graph.plugins import load_connector_manifest

__all__ = [
    "CheckResult",
    "ConnectorCapabilities",
    "ConnectorSpec",
    "Destination",
    "Source",
    "StreamDescriptor",
    "load_connector_manifest",
]
