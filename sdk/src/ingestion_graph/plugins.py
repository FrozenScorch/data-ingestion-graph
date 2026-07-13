"""Standard Python entry-point discovery for external connectors."""

from importlib.metadata import EntryPoint, entry_points
from typing import Any

from ingestion_graph.connectors.base import ConnectorSpec
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
    return spec
