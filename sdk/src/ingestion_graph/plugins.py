"""Standard Python entry-point discovery for external connectors."""

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from importlib.metadata import EntryPoint, entry_points
from typing import Any

from ingestion_graph.connectors.base import ConnectorCapabilities, ConnectorSpec
from ingestion_graph.errors import PluginError

PLUGIN_GROUPS = {
    "sources": "ingestion_graph.sources",
    "destinations": "ingestion_graph.destinations",
    "transforms": "ingestion_graph.transforms",
    "state_stores": "ingestion_graph.state_stores",
    "ocr_engines": "ingestion_graph.document_ai.ocr_engines",
    "page_renderers": "ingestion_graph.document_ai.page_renderers",
    "table_extractors": "ingestion_graph.document_ai.table_extractors",
    "document_splitters": "ingestion_graph.document_ai.document_splitters",
    "extraction_caches": "ingestion_graph.document_ai.extraction_caches",
    "vision_extractors": "ingestion_graph.document_ai.vision_extractors",
    "external_processing_policies": ("ingestion_graph.document_ai.external_processing_policies"),
}


@dataclass(frozen=True, slots=True)
class ProviderSpec:
    """Constructor-free metadata for transforms, state stores, and component providers."""

    name: str
    version: str
    config_schema: Mapping[str, Any]


def discover_plugins(kind: str) -> dict[str, EntryPoint]:
    try:
        group = PLUGIN_GROUPS[kind]
    except KeyError as exc:
        raise PluginError(f"Unknown plugin kind: {kind!r}") from exc
    discovered: dict[str, EntryPoint] = {}
    duplicates: set[str] = set()
    for point in entry_points(group=group):
        if point.name in discovered:
            duplicates.add(point.name)
        else:
            discovered[point.name] = point
    if duplicates:
        names = ", ".join(repr(name) for name in sorted(duplicates))
        raise PluginError(f"Duplicate {kind} plugin entry points are installed: {names}")
    return discovered


def load_plugin(kind: str, name: str) -> Any:
    plugins = discover_plugins(kind)
    try:
        return plugins[name].load()
    except KeyError as exc:
        raise PluginError(f"No {kind} plugin named {name!r} is installed") from exc
    except Exception as exc:
        raise PluginError(f"Failed to load {kind} plugin {name!r}: {exc}") from exc


def load_connector_manifest(kind: str, name: str) -> ConnectorSpec:
    """Load constructor-free metadata for a manifest-aware connector plugin."""
    plugin = load_plugin(kind, name)
    manifest = getattr(plugin, "manifest", None)
    if not callable(manifest):
        raise PluginError(f"{kind} plugin {name!r} does not expose manifest()")
    try:
        spec = manifest()
    except NotImplementedError as exc:
        raise PluginError(f"{kind} plugin {name!r} does not expose manifest()") from exc
    except Exception as exc:
        raise PluginError(f"Failed to load {kind} plugin {name!r} manifest: {exc}") from exc
    if not isinstance(spec, ConnectorSpec):
        raise PluginError(f"{kind} plugin {name!r} manifest() did not return ConnectorSpec")
    if spec.name != name:
        raise PluginError(
            f"{kind} plugin entry point {name!r} returned manifest name {spec.name!r}"
        )
    validate_connector_manifest(spec, kind=kind, entry_point_name=name)
    return spec


def validate_connector_manifest(
    spec: ConnectorSpec,
    *,
    kind: str,
    entry_point_name: str,
) -> None:
    prefix = f"{kind} plugin {entry_point_name!r} manifest"
    if not isinstance(spec.name, str) or not spec.name.strip():
        raise PluginError(f"{prefix} has an empty name")
    if not isinstance(spec.version, str) or not spec.version.strip():
        raise PluginError(f"{prefix} has an empty version")
    if not isinstance(spec.capabilities, ConnectorCapabilities):
        raise PluginError(f"{prefix} has invalid capabilities")
    capability_values = (
        spec.capabilities.incremental,
        spec.capabilities.resumable_full_refresh,
        spec.capabilities.deletes,
        spec.capabilities.schema_discovery,
        spec.capabilities.rate_limits,
    )
    if any(not isinstance(value, bool) for value in capability_values):
        raise PluginError(f"{prefix} capability flags must be booleans")

    schema = spec.config_schema
    if not isinstance(schema, Mapping) or schema.get("type") != "object":
        raise PluginError(f"{prefix} config_schema must be an object schema")
    properties = schema.get("properties")
    if not isinstance(properties, Mapping):
        raise PluginError(f"{prefix} config_schema.properties must be an object")
    if any(not isinstance(field_name, str) or not field_name for field_name in properties):
        raise PluginError(f"{prefix} config field names must be nonempty strings")
    if any(not isinstance(field_schema, Mapping) for field_schema in properties.values()):
        raise PluginError(f"{prefix} config fields must be schema objects")

    required = schema.get("required", [])
    if (
        not isinstance(required, Sequence)
        or isinstance(required, (str, bytes))
        or any(not isinstance(field, str) for field in required)
    ):
        raise PluginError(f"{prefix} config_schema.required must be a string array")
    unknown_required = set(required) - set(properties)
    if unknown_required:
        raise PluginError(
            f"{prefix} requires unknown fields: {', '.join(sorted(unknown_required))}"
        )


def validate_provider_manifest(
    spec: ProviderSpec,
    *,
    kind: str,
    entry_point_name: str,
) -> None:
    """Validate the shared metadata subset used by all non-connector providers."""
    prefix = f"{kind} plugin {entry_point_name!r} manifest"
    if not isinstance(spec.name, str) or not spec.name.strip():
        raise PluginError(f"{prefix} has an empty name")
    if not isinstance(spec.version, str) or not spec.version.strip():
        raise PluginError(f"{prefix} has an empty version")
    _validate_config_schema(spec.config_schema, prefix=prefix)


def _validate_config_schema(schema: Mapping[str, Any], *, prefix: str) -> None:
    if not isinstance(schema, Mapping) or schema.get("type") != "object":
        raise PluginError(f"{prefix} config_schema must be an object schema")
    properties = schema.get("properties")
    if not isinstance(properties, Mapping):
        raise PluginError(f"{prefix} config_schema.properties must be an object")
    if any(not isinstance(field_name, str) or not field_name for field_name in properties):
        raise PluginError(f"{prefix} config field names must be nonempty strings")
    if any(not isinstance(field_schema, Mapping) for field_schema in properties.values()):
        raise PluginError(f"{prefix} config fields must be schema objects")

    required = schema.get("required", [])
    if (
        not isinstance(required, Sequence)
        or isinstance(required, (str, bytes))
        or any(not isinstance(field, str) for field in required)
    ):
        raise PluginError(f"{prefix} config_schema.required must be a string array")
    unknown_required = set(required) - set(properties)
    if unknown_required:
        raise PluginError(
            f"{prefix} requires unknown fields: {', '.join(sorted(unknown_required))}"
        )


__all__ = [
    "PLUGIN_GROUPS",
    "ProviderSpec",
    "discover_plugins",
    "load_connector_manifest",
    "load_plugin",
    "validate_connector_manifest",
    "validate_provider_manifest",
]
