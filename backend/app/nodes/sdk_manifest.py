"""Strict projections from reusable SDK manifests into Studio-safe node schemas."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from copy import deepcopy
from dataclasses import asdict, dataclass, field
from typing import Any

from ingestion_graph.connectors import ConnectorSpec


@dataclass(frozen=True, slots=True)
class ManifestFieldProjection:
    """Map one SDK config field into its Studio-safe representation."""

    source_field: str
    target_field: str
    source_path: tuple[str, ...] = ()
    overrides: Mapping[str, Any] = field(default_factory=dict)


def project_manifest_config_schema(
    manifest: ConnectorSpec,
    *,
    fields: Sequence[ManifestFieldProjection],
    omitted: Mapping[str, str],
    studio_properties: Mapping[str, Mapping[str, Any]] | None = None,
    studio_required: Sequence[str] = (),
) -> dict[str, Any]:
    """Project every SDK field or fail when a connector manifest drifts."""
    schema = deepcopy(dict(manifest.config_schema))
    source_properties = schema.get("properties")
    if schema.get("type") != "object" or not isinstance(source_properties, Mapping):
        raise ValueError(f"SDK connector {manifest.name!r} must expose an object config schema")

    projected_sources = [item.source_field for item in fields]
    if len(set(projected_sources)) != len(projected_sources):
        raise ValueError(f"SDK connector {manifest.name!r} projects a field more than once")
    unexplained = set(source_properties) - set(projected_sources) - set(omitted)
    unknown = (set(projected_sources) | set(omitted)) - set(source_properties)
    if unexplained or unknown:
        raise ValueError(
            f"SDK connector {manifest.name!r} projection drift: "
            f"unexplained={sorted(unexplained)}, unknown={sorted(unknown)}"
        )
    if any(not reason.strip() for reason in omitted.values()):
        raise ValueError("Every omitted SDK field requires an explicit reason")

    properties: dict[str, Any] = {}
    required: list[str] = []
    sdk_required = set(schema.get("required", []))
    for projection in fields:
        value: Any = deepcopy(source_properties[projection.source_field])
        for segment in projection.source_path:
            if not isinstance(value, Mapping) or segment not in value:
                raise ValueError(
                    f"SDK connector {manifest.name!r} field {projection.source_field!r} "
                    f"has no projection path {projection.source_path!r}"
                )
            value = deepcopy(value[segment])
        if not isinstance(value, Mapping):
            raise ValueError("Projected SDK fields must resolve to JSON Schema objects")
        projected = dict(value)
        projected.update(deepcopy(dict(projection.overrides)))
        if projection.target_field in properties:
            raise ValueError(f"Duplicate Studio field {projection.target_field!r}")
        properties[projection.target_field] = projected
        if projection.source_field in sdk_required:
            required.append(projection.target_field)

    for name, value in (studio_properties or {}).items():
        if name in properties:
            raise ValueError(f"Duplicate Studio field {name!r}")
        properties[name] = deepcopy(dict(value))
    for name in studio_required:
        if name not in properties:
            raise ValueError(f"Required Studio field {name!r} is not defined")
        if name not in required:
            required.append(name)

    schema["properties"] = properties
    schema["required"] = required
    return schema


def serialize_connector_manifest(manifest: ConnectorSpec) -> dict[str, Any]:
    """Return stable connector metadata suitable for the node-registry API."""
    return {
        "name": manifest.name,
        "version": manifest.version,
        "capabilities": asdict(manifest.capabilities),
    }
