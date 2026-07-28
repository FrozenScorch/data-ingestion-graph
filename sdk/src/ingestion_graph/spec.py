"""Versioned, portable pipeline specifications.

The specification is deliberately smaller than the Studio DAG format. Version 1
models the SDK's current checkpoint boundary: one source, an ordered transform
chain, one idempotent destination, and one state store. Keeping this contract in
the SDK lets embedded applications, CI, and Studio share the same executable
configuration without importing the control plane.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
import tomllib
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from types import MappingProxyType
from typing import Any, TypeAlias

from ingestion_graph.errors import ConfigurationError

JSONValue: TypeAlias = None | bool | int | float | str | list["JSONValue"] | dict[str, "JSONValue"]
PIPELINE_SPEC_VERSION = "1"
_PLUGIN_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]*$")


def _json_value(value: Any, *, path: str) -> JSONValue:
    if value is None or isinstance(value, (bool, int, str)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ConfigurationError(f"{path} must contain only finite JSON numbers")
        return value
    if isinstance(value, Mapping):
        return {
            str(key): _json_value(item, path=f"{path}.{key}")
            for key, item in sorted(value.items(), key=lambda pair: str(pair[0]))
        }
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [_json_value(item, path=f"{path}[{index}]") for index, item in enumerate(value)]
    raise ConfigurationError(f"{path} contains non-serializable {type(value).__name__}")


def _freeze_json(value: JSONValue) -> JSONValue:
    """Return an immutable JSON tree without retaining caller-owned containers."""
    if isinstance(value, dict):
        return MappingProxyType({key: _freeze_json(item) for key, item in value.items()})  # type: ignore[return-value]
    if isinstance(value, list):
        return tuple(_freeze_json(item) for item in value)  # type: ignore[return-value]
    return value


def _copy_json(value: Any, *, path: str = "value") -> JSONValue:
    """Return an ordinary mutable JSON tree suitable for serialization."""
    return _json_value(value, path=path)


def _immutable_mapping(value: Any, *, path: str) -> Mapping[str, JSONValue]:
    normalized = _json_value(value, path=path)
    if not isinstance(normalized, dict):
        raise ConfigurationError(f"{path} must be an object")
    return MappingProxyType({key: _freeze_json(item) for key, item in normalized.items()})


def _mapping(value: Any, *, path: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ConfigurationError(f"{path} must be an object")
    return {str(key): item for key, item in value.items()}


def _reject_unknown(value: Mapping[str, Any], allowed: set[str], *, path: str) -> None:
    unknown = sorted(set(value) - allowed)
    if unknown:
        raise ConfigurationError(f"{path} contains unknown fields: {', '.join(unknown)}")


def _required_string(value: Mapping[str, Any], key: str, *, path: str) -> str:
    item = value.get(key)
    if not isinstance(item, str) or not item.strip():
        raise ConfigurationError(f"{path}.{key} must be a non-empty string")
    return item.strip()


@dataclass(frozen=True, slots=True)
class ComponentRef:
    """A named plugin plus JSON-safe constructor configuration."""

    plugin: str
    config: Mapping[str, JSONValue] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not isinstance(self.plugin, str) or _PLUGIN_NAME.fullmatch(self.plugin) is None:
            raise ConfigurationError(
                "Component plugin names must start with an alphanumeric character and contain "
                "only letters, digits, dots, underscores, or hyphens"
            )
        normalized = _json_value(self.config, path=f"component[{self.plugin}].config")
        if not isinstance(normalized, dict):
            raise ConfigurationError("Component config must be an object")
        object.__setattr__(
            self,
            "config",
            MappingProxyType({key: _freeze_json(item) for key, item in normalized.items()}),
        )

    def to_dict(self) -> dict[str, JSONValue]:
        config = _copy_json(self.config, path=f"component[{self.plugin}].config")
        if not isinstance(config, dict):  # pragma: no cover - guaranteed by construction
            raise AssertionError("Component config is not an object")
        return {"plugin": self.plugin, "config": config}

    @classmethod
    def from_dict(cls, value: Mapping[str, Any], *, path: str = "component") -> ComponentRef:
        raw = _mapping(value, path=path)
        _reject_unknown(raw, {"plugin", "config"}, path=path)
        plugin = _required_string(raw, "plugin", path=path)
        config = _mapping(raw.get("config", {}), path=f"{path}.config")
        return cls(plugin=plugin, config=config)


@dataclass(frozen=True, slots=True)
class StateStoreRef:
    """Portable state-store selection.

    State stores are kept separate from ordinary destinations because they are
    part of the checkpoint protocol rather than the materialized data plane.
    """

    plugin: str = "sqlite"
    config: Mapping[str, JSONValue] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not isinstance(self.plugin, str) or _PLUGIN_NAME.fullmatch(self.plugin) is None:
            raise ConfigurationError("State-store plugin name is invalid")
        normalized = _json_value(self.config, path=f"state_store[{self.plugin}].config")
        if not isinstance(normalized, dict):
            raise ConfigurationError("State-store config must be an object")
        object.__setattr__(
            self,
            "config",
            MappingProxyType({key: _freeze_json(item) for key, item in normalized.items()}),
        )

    def to_dict(self) -> dict[str, JSONValue]:
        config = _copy_json(self.config, path=f"state_store[{self.plugin}].config")
        if not isinstance(config, dict):  # pragma: no cover - guaranteed by construction
            raise AssertionError("State-store config is not an object")
        return {"plugin": self.plugin, "config": config}

    @classmethod
    def from_dict(cls, value: Mapping[str, Any], *, path: str = "state_store") -> StateStoreRef:
        raw = _mapping(value, path=path)
        _reject_unknown(raw, {"plugin", "config"}, path=path)
        plugin = _required_string(raw, "plugin", path=path)
        config = _mapping(raw.get("config", {}), path=f"{path}.config")
        return cls(plugin=plugin, config=config)


@dataclass(frozen=True, slots=True)
class PipelineSpec:
    """Versioned linear ingestion graph executable by the headless SDK."""

    name: str
    source: ComponentRef
    destination: ComponentRef
    transforms: tuple[ComponentRef, ...] = ()
    state_store: StateStoreRef = field(default_factory=StateStoreRef)
    streams: tuple[str, ...] = ()
    state_namespace: str | None = None
    metadata: Mapping[str, JSONValue] = field(default_factory=dict)
    schema_version: str = PIPELINE_SPEC_VERSION
    origin: Path | None = field(default=None, repr=False, compare=False)

    def __post_init__(self) -> None:
        if not isinstance(self.name, str) or not self.name.strip():
            raise ConfigurationError("PipelineSpec.name must be a non-empty string")
        object.__setattr__(self, "name", self.name.strip())
        if not isinstance(self.source, ComponentRef):
            raise ConfigurationError("PipelineSpec.source must be a ComponentRef")
        if not isinstance(self.destination, ComponentRef):
            raise ConfigurationError("PipelineSpec.destination must be a ComponentRef")
        if not isinstance(self.state_store, StateStoreRef):
            raise ConfigurationError("PipelineSpec.state_store must be a StateStoreRef")
        try:
            transforms = tuple(self.transforms)
        except TypeError as exc:
            raise ConfigurationError(
                "PipelineSpec.transforms must be a component sequence"
            ) from exc
        if any(not isinstance(transform, ComponentRef) for transform in transforms):
            raise ConfigurationError(
                "PipelineSpec.transforms must contain only ComponentRef values"
            )
        object.__setattr__(self, "transforms", transforms)
        if self.schema_version != PIPELINE_SPEC_VERSION:
            raise ConfigurationError(
                f"Unsupported pipeline schema_version {self.schema_version!r}; "
                f"expected {PIPELINE_SPEC_VERSION!r}"
            )
        normalized_streams: list[str] = []
        for index, stream in enumerate(self.streams):
            if not isinstance(stream, str) or not stream.strip():
                raise ConfigurationError(f"PipelineSpec.streams[{index}] must be non-empty")
            normalized_streams.append(stream.strip())
        if len(normalized_streams) != len(set(normalized_streams)):
            raise ConfigurationError("PipelineSpec.streams must not contain duplicates")
        object.__setattr__(self, "streams", tuple(normalized_streams))
        if self.state_namespace is not None:
            if not isinstance(self.state_namespace, str) or not self.state_namespace.strip():
                raise ConfigurationError("PipelineSpec.state_namespace must be non-empty or null")
            object.__setattr__(self, "state_namespace", self.state_namespace.strip())
        object.__setattr__(
            self,
            "metadata",
            _immutable_mapping(self.metadata, path="pipeline.metadata"),
        )
        if self.origin is not None:
            object.__setattr__(self, "origin", Path(self.origin).expanduser().resolve())

    @property
    def base_dir(self) -> Path | None:
        return None if self.origin is None else self.origin.parent

    def to_dict(self) -> dict[str, JSONValue]:
        metadata = _copy_json(self.metadata, path="pipeline.metadata")
        if not isinstance(metadata, dict):  # pragma: no cover - guaranteed by construction
            raise AssertionError("Pipeline metadata is not an object")
        return {
            "schema_version": self.schema_version,
            "name": self.name,
            "source": self.source.to_dict(),
            "transforms": [item.to_dict() for item in self.transforms],
            "destination": self.destination.to_dict(),
            "state_store": self.state_store.to_dict(),
            "streams": list(self.streams),
            "state_namespace": self.state_namespace,
            "metadata": metadata,
        }

    @property
    def fingerprint(self) -> str:
        """Fingerprint the complete executable specification, excluding its file location."""
        return _fingerprint(self.to_dict())

    @property
    def definition_processing_fingerprint(self) -> str:
        """Fingerprint serialized processing fields without resolving runtime plugins.

        This is a definition comparison helper, not a checkpoint identity. Use
        ``plan_ingestion_pipeline(spec).processing_fingerprint`` for replay decisions.
        """
        return _fingerprint(
            {
                "schema_version": self.schema_version,
                "source": self.source.to_dict(),
                "transforms": [item.to_dict() for item in self.transforms],
                "streams": list(self.streams),
            }
        )

    @property
    def definition_materialization_fingerprint(self) -> str:
        """Fingerprint serialized processing and destination fields.

        This deliberately excludes manifest defaults, implementation versions, and
        resolved paths. Use the corresponding ``PipelinePlan`` fingerprint for replay.
        """
        return _fingerprint(
            {
                "schema_version": self.schema_version,
                "source": self.source.to_dict(),
                "transforms": [item.to_dict() for item in self.transforms],
                "destination": self.destination.to_dict(),
                "streams": list(self.streams),
            }
        )

    @classmethod
    def from_dict(
        cls,
        value: Mapping[str, Any],
        *,
        origin: str | Path | None = None,
    ) -> PipelineSpec:
        raw = _mapping(value, path="pipeline")
        _reject_unknown(
            raw,
            {
                "schema_version",
                "name",
                "source",
                "transforms",
                "destination",
                "state_store",
                "streams",
                "state_namespace",
                "metadata",
            },
            path="pipeline",
        )
        source = ComponentRef.from_dict(
            _mapping(raw.get("source"), path="pipeline.source"), path="pipeline.source"
        )
        destination = ComponentRef.from_dict(
            _mapping(raw.get("destination"), path="pipeline.destination"),
            path="pipeline.destination",
        )
        transforms_value = raw.get("transforms", [])
        if not isinstance(transforms_value, Sequence) or isinstance(
            transforms_value, (str, bytes, bytearray)
        ):
            raise ConfigurationError("pipeline.transforms must be an array")
        transforms = tuple(
            ComponentRef.from_dict(
                _mapping(item, path=f"pipeline.transforms[{index}]"),
                path=f"pipeline.transforms[{index}]",
            )
            for index, item in enumerate(transforms_value)
        )
        state_value = raw.get("state_store", {"plugin": "sqlite", "config": {}})
        state_store = StateStoreRef.from_dict(
            _mapping(state_value, path="pipeline.state_store"), path="pipeline.state_store"
        )
        streams_value = raw.get("streams", [])
        if not isinstance(streams_value, Sequence) or isinstance(
            streams_value, (str, bytes, bytearray)
        ):
            raise ConfigurationError("pipeline.streams must be an array")
        state_namespace = raw.get("state_namespace")
        if state_namespace is not None and not isinstance(state_namespace, str):
            raise ConfigurationError("pipeline.state_namespace must be a string or null")
        metadata = _mapping(raw.get("metadata", {}), path="pipeline.metadata")
        return cls(
            schema_version=str(raw.get("schema_version", PIPELINE_SPEC_VERSION)),
            name=_required_string(raw, "name", path="pipeline"),
            source=source,
            transforms=transforms,
            destination=destination,
            state_store=state_store,
            streams=tuple(streams_value),
            state_namespace=state_namespace,
            metadata=metadata,
            origin=None if origin is None else Path(origin),
        )

    @classmethod
    def load(cls, path: str | Path) -> PipelineSpec:
        source_path = Path(path).expanduser().resolve()
        try:
            if source_path.suffix.lower() == ".json":
                raw = json.loads(source_path.read_text(encoding="utf-8"))
            elif source_path.suffix.lower() in {".toml", ".tml"}:
                with source_path.open("rb") as handle:
                    raw = tomllib.load(handle)
            else:
                raise ConfigurationError("Pipeline specs must use .json or .toml")
        except ConfigurationError:
            raise
        except (OSError, json.JSONDecodeError, tomllib.TOMLDecodeError) as exc:
            raise ConfigurationError(f"Could not load pipeline spec {source_path}: {exc}") from exc
        if not isinstance(raw, Mapping):
            raise ConfigurationError("Pipeline spec root must be an object")
        return cls.from_dict(raw, origin=source_path)


def load_pipeline_spec(path: str | Path) -> PipelineSpec:
    return PipelineSpec.load(path)


def _fingerprint(value: Any) -> str:
    normalized = _json_value(value, path="fingerprint")
    encoded = json.dumps(
        normalized,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


__all__ = [
    "ComponentRef",
    "JSONValue",
    "PIPELINE_SPEC_VERSION",
    "PipelineSpec",
    "StateStoreRef",
    "load_pipeline_spec",
]
