"""Standard Python entry-point discovery for external connectors."""

from collections.abc import Mapping, Sequence
from importlib.metadata import EntryPoint, entry_points
from typing import Any

from ingestion_graph.connectors.base import ConnectorCapabilities, ConnectorSpec
from ingestion_graph.errors import PluginError

PLUGIN_GROUPS = {
    "sources": "ingestion_graph.sources",
    "destinations": "ingestion_graph.destinations",
    "transforms": "ingestion_graph.transforms",
}


def discover_plugins(kind: str) -> dict[str, EntryPoint]:
    try:
        group = PLUGIN_GROUPS[kind]
    except KeyError as exc:
        raise PluginError(f"Unknown plugin kind: {kind!r}") from exc
    return {point.name: point for point in entry_points(group=group)}


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
    _validate_connector_manifest(spec, kind=kind, entry_point_name=name)
    return spec


def _validate_connector_manifest(
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
