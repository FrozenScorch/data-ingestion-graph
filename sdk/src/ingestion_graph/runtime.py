"""Build, plan, and run portable ingestion pipeline specifications."""

from __future__ import annotations

import hashlib
import inspect
import re
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from dataclasses import field as dataclass_field
from importlib import metadata
from pathlib import Path
from typing import Any, cast, get_args, get_origin, get_type_hints

from ingestion_graph.connectors.base import (
    ConnectorCapabilities,
    ConnectorSpec,
    Destination,
    Source,
)
from ingestion_graph.document_ai.models import ComponentDescriptor
from ingestion_graph.errors import ConfigurationError, PluginError
from ingestion_graph.pipeline import Pipeline, PipelineResult
from ingestion_graph.plugins import (
    PLUGIN_GROUPS,
    ProviderSpec,
    load_plugin,
    validate_connector_manifest,
    validate_provider_manifest,
)
from ingestion_graph.secrets import SecretRef
from ingestion_graph.spec import ComponentRef, JSONValue, PipelineSpec, _copy_json, _fingerprint
from ingestion_graph.state import MemoryStateStore, SQLiteStateStore, StateStore
from ingestion_graph.transforms import Transform

Factory = Callable[..., object]

_DOCUMENT_EXTENSION_FIELDS = {
    "ocr_engine": "ocr_engines",
    "page_renderer": "page_renderers",
    "table_extractor": "table_extractors",
    "document_splitter": "document_splitters",
    "extraction_cache": "extraction_caches",
    "vision_extractor": "vision_extractors",
    "external_processing_policy": "external_processing_policies",
}
_PATH_FIELDS: dict[tuple[str, str], tuple[str, ...]] = {
    ("sources", "jsonl"): ("path",),
    ("sources", "local_documents"): ("paths",),
    ("destinations", "jsonl"): ("path",),
    ("destinations", "sqlite"): ("path",),
    ("state_stores", "sqlite"): ("path",),
    ("extraction_caches", "sqlite"): ("path",),
}
_PROVIDER_METHODS: dict[str, tuple[str, ...]] = {
    "ocr_engines": ("check", "recognize", "close"),
    "page_renderers": ("render", "close"),
    "table_extractors": ("extract", "close"),
    "document_splitters": ("split", "close"),
    "extraction_caches": ("get", "put"),
    "vision_extractors": ("extract", "close"),
    "external_processing_policies": ("authorize",),
}


@dataclass(frozen=True, slots=True)
class ExtensionPlan:
    field: str
    kind: str
    plugin: str
    config: Mapping[str, JSONValue]
    built_in: bool
    version: str
    implementation: Mapping[str, JSONValue] = dataclass_field(default_factory=dict)
    descriptor: Mapping[str, JSONValue] | None = None
    implicit: bool = False
    config_schema: Mapping[str, Any] | None = dataclass_field(
        default=None, repr=False, compare=False
    )

    def __post_init__(self) -> None:
        config = ComponentRef(self.plugin, self.config).config
        implementation = _immutable_json_mapping(
            self.implementation, path=f"{self.kind}.{self.plugin}.implementation"
        )
        descriptor = (
            None
            if self.descriptor is None
            else _immutable_json_mapping(
                self.descriptor, path=f"{self.kind}.{self.plugin}.descriptor"
            )
        )
        schema = (
            None
            if self.config_schema is None
            else _immutable_json_mapping(
                self.config_schema, path=f"{self.kind}.{self.plugin}.config_schema"
            )
        )
        object.__setattr__(self, "config", config)
        object.__setattr__(self, "implementation", implementation)
        object.__setattr__(self, "descriptor", descriptor)
        object.__setattr__(self, "config_schema", schema)

    def to_dict(self) -> dict[str, JSONValue]:
        return {
            "field": self.field,
            "kind": self.kind,
            "plugin": self.plugin,
            "config": _json_object(self.config, path=f"{self.kind}.{self.plugin}.config"),
            "built_in": self.built_in,
            "version": self.version,
            "implementation": _json_object(
                self.implementation, path=f"{self.kind}.{self.plugin}.implementation"
            ),
            "descriptor": None
            if self.descriptor is None
            else _json_object(self.descriptor, path=f"{self.kind}.{self.plugin}.descriptor"),
            "implicit": self.implicit,
        }

    def identity_dict(self) -> dict[str, JSONValue]:
        return {
            "kind": self.kind,
            "plugin": self.plugin,
            "version": self.version,
            "implementation": _json_object(
                self.implementation, path=f"{self.kind}.{self.plugin}.implementation"
            ),
            "descriptor": None
            if self.descriptor is None
            else _json_object(self.descriptor, path=f"{self.kind}.{self.plugin}.descriptor"),
            "config": _json_object(self.config, path=f"{self.kind}.{self.plugin}.config"),
        }


@dataclass(frozen=True, slots=True)
class ConnectorPlan:
    kind: str
    plugin: str
    version: str | None
    config: Mapping[str, JSONValue]
    capabilities: ConnectorCapabilities | None
    manifest_aware: bool
    implementation: Mapping[str, JSONValue] = dataclass_field(default_factory=dict)
    config_schema: Mapping[str, Any] | None = dataclass_field(
        default=None, repr=False, compare=False
    )

    def __post_init__(self) -> None:
        object.__setattr__(self, "config", ComponentRef(self.plugin, self.config).config)
        object.__setattr__(
            self,
            "implementation",
            _immutable_json_mapping(
                self.implementation, path=f"{self.kind}.{self.plugin}.implementation"
            ),
        )
        if self.config_schema is not None:
            object.__setattr__(
                self,
                "config_schema",
                _immutable_json_mapping(
                    self.config_schema, path=f"{self.kind}.{self.plugin}.config_schema"
                ),
            )

    def to_dict(self) -> dict[str, JSONValue]:
        capabilities = self.capabilities
        return {
            "kind": self.kind,
            "plugin": self.plugin,
            "version": self.version,
            "config": _json_object(self.config, path=f"{self.kind}.{self.plugin}.config"),
            "manifest_aware": self.manifest_aware,
            "implementation": _json_object(
                self.implementation, path=f"{self.kind}.{self.plugin}.implementation"
            ),
            "capabilities": None
            if capabilities is None
            else {
                "incremental": capabilities.incremental,
                "resumable_full_refresh": capabilities.resumable_full_refresh,
                "deletes": capabilities.deletes,
                "schema_discovery": capabilities.schema_discovery,
                "rate_limits": capabilities.rate_limits,
            },
        }

    def identity_dict(self) -> dict[str, JSONValue]:
        return {
            "kind": self.kind,
            "plugin": self.plugin,
            "version": self.version,
            "implementation": _json_object(
                self.implementation, path=f"{self.kind}.{self.plugin}.implementation"
            ),
            "config": _json_object(self.config, path=f"{self.kind}.{self.plugin}.config"),
        }


@dataclass(frozen=True, slots=True)
class PipelinePlan:
    name: str
    schema_version: str
    spec_fingerprint: str
    processing_fingerprint: str
    materialization_fingerprint: str
    runtime_fingerprint: str
    state_namespace: str
    source: ConnectorPlan
    transforms: tuple[ConnectorPlan, ...]
    destination: ConnectorPlan
    state_store: ComponentRef
    streams: tuple[str, ...]
    extensions: tuple[ExtensionPlan, ...]
    warnings: tuple[str, ...]
    state_store_schema: Mapping[str, Any] | None = dataclass_field(
        default=None, repr=False, compare=False
    )
    spec_origin: str | None = dataclass_field(default=None, repr=False, compare=False)
    resolution_root: str | None = dataclass_field(default=None, repr=False, compare=False)

    def __post_init__(self) -> None:
        if self.state_store_schema is not None:
            object.__setattr__(
                self,
                "state_store_schema",
                _immutable_json_mapping(self.state_store_schema, path="state_stores.config_schema"),
            )

    def to_dict(self) -> dict[str, JSONValue]:
        return {
            "name": self.name,
            "schema_version": self.schema_version,
            "spec_fingerprint": self.spec_fingerprint,
            "processing_fingerprint": self.processing_fingerprint,
            "materialization_fingerprint": self.materialization_fingerprint,
            "runtime_fingerprint": self.runtime_fingerprint,
            "state_namespace": self.state_namespace,
            "source": self.source.to_dict(),
            "transforms": [item.to_dict() for item in self.transforms],
            "destination": self.destination.to_dict(),
            "state_store": self.state_store.to_dict(),
            "streams": list(self.streams),
            "extensions": [item.to_dict() for item in self.extensions],
            "warnings": list(self.warnings),
        }


class PipelineRegistry:
    """Side-effect-free registry used by specs and embedders.

    Built-ins are always available from a source checkout. Unknown names fall
    back to standard Python entry points, so third-party packages remain the
    extension mechanism rather than arbitrary import strings in configuration.
    """

    def __init__(self) -> None:
        self._overrides: dict[str, dict[str, Factory]] = {}

    def register(self, kind: str, name: str, factory: Factory) -> None:
        if not kind or not name or not callable(factory):
            raise ValueError("Registry kind, name, and callable factory are required")
        if kind not in PLUGIN_GROUPS:
            raise ValueError(f"Unknown registry kind: {kind!r}")
        registered = self._overrides.setdefault(kind, {})
        if name in registered:
            raise PluginError(f"{kind} plugin {name!r} is already registered")
        registered[name] = factory

    def is_built_in(self, kind: str, name: str) -> bool:
        return name not in self._overrides.get(kind, {}) and name in _built_in_factories(kind)

    def factory(self, kind: str, name: str) -> Factory:
        override = self._overrides.get(kind, {}).get(name)
        if override is not None:
            return override
        built_in = _built_in_factories(kind).get(name)
        if built_in is not None:
            return built_in
        try:
            loaded = load_plugin(kind, name)
        except PluginError:
            raise
        if not callable(loaded):
            raise PluginError(f"{kind} plugin {name!r} is not callable")
        return cast(Factory, loaded)

    def connector_manifest(self, kind: str, name: str) -> ConnectorSpec | None:
        factory = self.factory(kind, name)
        return _connector_manifest(factory, kind=kind, name=name)

    def provider_manifest(self, kind: str, name: str) -> ProviderSpec | None:
        factory = self.factory(kind, name)
        if self.is_built_in(kind, name):
            built_in = _built_in_provider_manifests(kind).get(name)
            if built_in is not None:
                return built_in
        manifest = getattr(factory, "manifest", None)
        if not callable(manifest):
            return None
        try:
            result = manifest()
        except NotImplementedError as exc:
            del exc
            return None
        except Exception as exc:
            raise PluginError(f"Failed to load {kind} plugin {name!r} manifest: {exc}") from exc
        if isinstance(result, ConnectorSpec):
            validate_connector_manifest(result, kind=kind, entry_point_name=name)
            result = ProviderSpec(result.name, result.version, result.config_schema)
        if not isinstance(result, ProviderSpec):
            raise PluginError(f"{kind} plugin {name!r} manifest() did not return ProviderSpec")
        if result.name != name:
            raise PluginError(f"{kind} plugin {name!r} returned manifest name {result.name!r}")
        validate_provider_manifest(result, kind=kind, entry_point_name=name)
        return result


def _connector_manifest(factory: Factory, *, kind: str, name: str) -> ConnectorSpec | None:
    manifest = getattr(factory, "manifest", None)
    if not callable(manifest):
        if kind in {"sources", "destinations"}:
            raise PluginError(f"{kind} plugin {name!r} does not expose manifest()")
        return None
    try:
        result = manifest()
    except NotImplementedError as exc:
        if kind in {"sources", "destinations"}:
            raise PluginError(f"{kind} plugin {name!r} does not expose manifest()") from exc
        return None
    except Exception as exc:
        raise PluginError(f"Failed to load {kind} plugin {name!r} manifest: {exc}") from exc
    if not isinstance(result, ConnectorSpec):
        raise PluginError(f"{kind} plugin {name!r} manifest() did not return ConnectorSpec")
    if result.name != name:
        raise PluginError(f"{kind} plugin {name!r} returned manifest name {result.name!r}")
    validate_connector_manifest(result, kind=kind, entry_point_name=name)
    return ConnectorSpec(
        name=result.name,
        version=result.version,
        config_schema=_json_object(
            result.config_schema, path=f"{kind}.{name}.manifest.config_schema"
        ),
        capabilities=result.capabilities,
    )


def _built_in_factories(kind: str) -> dict[str, Factory]:
    if kind == "sources":
        from ingestion_graph.sources import (
            DiscordSource,
            JsonlSource,
            LocalDocumentsSource,
            PostgresSource,
            RestSource,
        )

        return {
            "discord": DiscordSource,
            "jsonl": JsonlSource,
            "local_documents": LocalDocumentsSource,
            "postgres": PostgresSource,
            "rest": RestSource,
        }
    if kind == "destinations":
        from ingestion_graph.destinations import (
            JsonlDestination,
            PostgresDestination,
            SQLiteCollection,
        )

        return {
            "jsonl": JsonlDestination,
            "postgres": PostgresDestination,
            "sqlite": SQLiteCollection,
        }
    if kind == "state_stores":
        return {"memory": MemoryStateStore, "sqlite": SQLiteStateStore}
    if kind == "ocr_engines":
        from ingestion_graph.document_ai import TesseractOcrEngine

        return {"tesseract": TesseractOcrEngine}
    if kind == "page_renderers":
        from ingestion_graph.document_ai import PdfiumPageRenderer

        return {"pdfium": PdfiumPageRenderer}
    if kind == "document_splitters":
        from ingestion_graph.document_ai import IdentitySplitter

        return {"identity": IdentitySplitter}
    if kind == "extraction_caches":
        from ingestion_graph.document_ai import MemoryExtractionCache, SQLiteExtractionCache

        return {"memory": MemoryExtractionCache, "sqlite": SQLiteExtractionCache}
    return {}


def _built_in_provider_manifests(kind: str) -> dict[str, ProviderSpec]:
    object_schema: dict[str, Any] = {
        "type": "object",
        "properties": {},
        "additionalProperties": False,
    }
    if kind == "state_stores":
        return {
            "memory": ProviderSpec("memory", "1", object_schema),
            "sqlite": ProviderSpec(
                "sqlite",
                "1",
                {
                    "type": "object",
                    "properties": {
                        "path": {
                            "type": "string",
                            "minLength": 1,
                            "format": "local-path",
                            "default": ".ingestion/state.db",
                        }
                    },
                    "additionalProperties": False,
                },
            ),
        }
    if kind == "ocr_engines":
        return {
            "tesseract": ProviderSpec(
                "tesseract",
                "1",
                {
                    "type": "object",
                    "properties": {
                        "executable": {"type": "string", "minLength": 1, "default": "tesseract"},
                        "timeout_seconds": {
                            "type": "number",
                            "exclusiveMinimum": 0,
                            "default": 30.0,
                        },
                        "max_output_bytes": {
                            "type": "integer",
                            "minimum": 1,
                            "default": 8 * 1024 * 1024,
                        },
                        "max_image_bytes": {
                            "type": "integer",
                            "minimum": 1,
                            "default": 64 * 1024 * 1024,
                        },
                        "page_segmentation_mode": {
                            "type": "integer",
                            "minimum": 0,
                            "maximum": 13,
                            "default": 3,
                        },
                    },
                    "additionalProperties": False,
                },
            )
        }
    if kind == "page_renderers":
        return {
            "pdfium": ProviderSpec(
                "pdfium",
                "3",
                {
                    "type": "object",
                    "properties": {
                        "max_pixels": {
                            "type": "integer",
                            "minimum": 1,
                            "default": 50_000_000,
                        },
                        "max_output_bytes": {
                            "type": "integer",
                            "minimum": 1,
                            "default": 64 * 1024 * 1024,
                        },
                        "max_source_bytes": {
                            "type": "integer",
                            "minimum": 1,
                            "default": 256 * 1024 * 1024,
                        },
                        "use_subprocess": {
                            "type": ["boolean", "null"],
                            "default": None,
                        },
                    },
                    "additionalProperties": False,
                },
            )
        }
    if kind == "document_splitters":
        return {"identity": ProviderSpec("identity", "1", object_schema)}
    if kind == "extraction_caches":
        return {
            "memory": ProviderSpec("memory", "1", object_schema),
            "sqlite": ProviderSpec(
                "sqlite",
                "1",
                {
                    "type": "object",
                    "properties": {
                        "path": {
                            "type": "string",
                            "minLength": 1,
                            "format": "local-path",
                            "default": ".ingestion/extraction-cache.db",
                        }
                    },
                    "additionalProperties": False,
                },
            ),
        }
    return {}


def _as_pipeline_spec(value: PipelineSpec | Mapping[str, Any] | str | Path) -> PipelineSpec:
    if isinstance(value, PipelineSpec):
        return value
    if isinstance(value, (str, Path)):
        return PipelineSpec.load(value)
    if isinstance(value, Mapping):
        return PipelineSpec.from_dict(value)
    raise TypeError("Expected PipelineSpec, mapping, or .json/.toml path")


@dataclass(frozen=True, slots=True)
class _ProviderResolution:
    plugin: str
    version: str
    config: Mapping[str, JSONValue]
    implementation: Mapping[str, JSONValue]
    descriptor: Mapping[str, JSONValue] | None
    built_in: bool
    config_schema: Mapping[str, Any] | None

    def identity_dict(self, *, kind: str) -> dict[str, JSONValue]:
        return {
            "kind": kind,
            "plugin": self.plugin,
            "version": self.version,
            "implementation": _json_object(
                self.implementation, path=f"{kind}.{self.plugin}.implementation"
            ),
            "descriptor": None
            if self.descriptor is None
            else _json_object(self.descriptor, path=f"{kind}.{self.plugin}.descriptor"),
            "config": _json_object(self.config, path=f"{kind}.{self.plugin}.config"),
        }


def _plan_provider(
    registry: PipelineRegistry,
    kind: str,
    reference: ComponentRef,
    base_dir: Path | None,
) -> _ProviderResolution:
    factory = registry.factory(kind, reference.plugin)
    _validate_factory_contract(kind, reference.plugin, factory)
    manifest = registry.provider_manifest(kind, reference.plugin)
    descriptor = _static_descriptor(factory, kind=kind, name=reference.plugin)
    version, implementation = _implementation_identity(
        factory,
        declared_version=None if manifest is None else manifest.version,
        descriptor=descriptor,
    )
    config = _effective_config(
        reference.config,
        schema=None if manifest is None else manifest.config_schema,
        factory=factory,
        kind=kind,
        plugin=reference.plugin,
        base_dir=base_dir,
    )
    return _ProviderResolution(
        plugin=reference.plugin,
        version=version,
        config=ComponentRef(reference.plugin, config).config,
        implementation=_immutable_json_mapping(
            implementation, path=f"{kind}.{reference.plugin}.implementation"
        ),
        descriptor=None
        if descriptor is None
        else _immutable_json_mapping(
            descriptor.to_dict(), path=f"{kind}.{reference.plugin}.descriptor"
        ),
        built_in=registry.is_built_in(kind, reference.plugin),
        config_schema=None if manifest is None else manifest.config_schema,
    )


def plan_ingestion_pipeline(
    value: PipelineSpec | Mapping[str, Any] | str | Path,
    *,
    registry: PipelineRegistry | None = None,
) -> PipelinePlan:
    spec = _as_pipeline_spec(value)
    active_registry = registry or PipelineRegistry()
    base_dir = spec.base_dir or Path.cwd().resolve()
    source = _plan_connector(active_registry, "sources", spec.source, base_dir)
    source, extensions = _plan_extensions(active_registry, source, base_dir)
    if source.plugin == "local_documents":
        _validate_document_configuration(source.config, extensions, active_registry)
    destination = _plan_connector(active_registry, "destinations", spec.destination, base_dir)
    transforms = tuple(
        _plan_connector(active_registry, "transforms", transform, base_dir)
        for transform in spec.transforms
    )
    state = _plan_provider(
        active_registry,
        "state_stores",
        ComponentRef(spec.state_store.plugin, spec.state_store.config),
        base_dir,
    )
    state_ref = ComponentRef(state.plugin, state.config)

    processing_identity: dict[str, JSONValue] = {
        "schema_version": spec.schema_version,
        "source": source.identity_dict(),
        "transforms": [item.identity_dict() for item in transforms],
        "streams": list(spec.streams),
        "document_extensions": [item.identity_dict() for item in extensions],
    }
    processing_fingerprint = _fingerprint(processing_identity)
    materialization_fingerprint = _fingerprint(
        {
            **processing_identity,
            "destination": destination.identity_dict(),
        }
    )
    runtime_fingerprint = _fingerprint(
        {
            **processing_identity,
            "destination": destination.identity_dict(),
            "state_store": state.identity_dict(kind="state_stores"),
        }
    )
    state_namespace = (
        spec.state_namespace
        if spec.state_namespace is not None
        else f"{spec.name}:{materialization_fingerprint}"
    )
    warnings = _plan_warnings(spec, source)
    return PipelinePlan(
        name=spec.name,
        schema_version=spec.schema_version,
        spec_fingerprint=spec.fingerprint,
        processing_fingerprint=processing_fingerprint,
        materialization_fingerprint=materialization_fingerprint,
        runtime_fingerprint=runtime_fingerprint,
        state_namespace=state_namespace,
        source=source,
        transforms=transforms,
        destination=destination,
        state_store=state_ref,
        streams=spec.streams,
        extensions=extensions,
        warnings=warnings,
        state_store_schema=state.config_schema,
        spec_origin=None if spec.origin is None else str(spec.origin),
        resolution_root=str(base_dir),
    )


def create_ingestion_pipeline(
    value: PipelineSpec | Mapping[str, Any] | str | Path,
    *,
    registry: PipelineRegistry | None = None,
    plan: PipelinePlan | None = None,
) -> Pipeline:
    spec = _as_pipeline_spec(value)
    active_registry = registry or PipelineRegistry()
    active_plan = plan or plan_ingestion_pipeline(spec, registry=active_registry)
    _validate_plan_for_spec(active_plan, spec)
    if plan is not None:
        current_plan = plan_ingestion_pipeline(spec, registry=active_registry)
        if current_plan.runtime_fingerprint != active_plan.runtime_fingerprint:
            raise ConfigurationError(
                "PipelinePlan does not match the active PipelineRegistry implementations"
            )
    source = _instantiate_connector(
        active_registry, active_plan.source, extensions=active_plan.extensions
    )
    destination = _instantiate_connector(active_registry, active_plan.destination)
    transform_objects = tuple(
        _instantiate_connector(active_registry, item) for item in active_plan.transforms
    )
    state_store = _instantiate_state_store(
        active_registry, active_plan.state_store, active_plan.state_store_schema
    )
    if not isinstance(source, Source):
        raise PluginError(f"Source plugin {active_plan.source.plugin!r} did not return Source")
    if not isinstance(destination, Destination):
        raise PluginError(
            f"Destination plugin {active_plan.destination.plugin!r} did not return Destination"
        )
    transforms: list[Transform] = []
    for index, transform in enumerate(transform_objects):
        if not isinstance(transform, Transform):
            raise PluginError(
                f"Transform plugin {active_plan.transforms[index].plugin!r} "
                "did not return Transform"
            )
        transforms.append(transform)
    return Pipeline(
        spec.name,
        source,
        destination,
        transforms=transforms,
        state_store=state_store,
        state_namespace=active_plan.state_namespace,
    )


async def run_ingestion_pipeline(
    value: PipelineSpec | Mapping[str, Any] | str | Path,
    *,
    registry: PipelineRegistry | None = None,
    plan: PipelinePlan | None = None,
) -> PipelineResult:
    spec = _as_pipeline_spec(value)
    pipeline = create_ingestion_pipeline(spec, registry=registry, plan=plan)
    return await pipeline.run(stream_names=spec.streams or None)


def _validate_plan_for_spec(plan: PipelinePlan, spec: PipelineSpec) -> None:
    if plan.spec_fingerprint != spec.fingerprint:
        raise ConfigurationError("PipelinePlan does not match the supplied PipelineSpec")
    if (
        plan.name != spec.name
        or plan.schema_version != spec.schema_version
        or plan.streams != spec.streams
    ):
        raise ConfigurationError("PipelinePlan metadata does not match the supplied PipelineSpec")
    expected_origin = None if spec.origin is None else str(spec.origin)
    if plan.spec_origin != expected_origin:
        raise ConfigurationError("PipelinePlan was resolved for a different PipelineSpec origin")
    expected_root = str(spec.base_dir or Path.cwd().resolve())
    if plan.resolution_root != expected_root:
        raise ConfigurationError("PipelinePlan was resolved from a different working directory")


def _plan_connector(
    registry: PipelineRegistry,
    kind: str,
    reference: ComponentRef,
    base_dir: Path | None,
) -> ConnectorPlan:
    factory = registry.factory(kind, reference.plugin)
    _validate_factory_contract(kind, reference.plugin, factory)
    if kind == "transforms":
        provider_manifest = registry.provider_manifest(kind, reference.plugin)
        manifest = None
        schema = None if provider_manifest is None else provider_manifest.config_schema
        manifest_version = None if provider_manifest is None else provider_manifest.version
        capabilities = None
        manifest_aware = provider_manifest is not None
    else:
        manifest = registry.connector_manifest(kind, reference.plugin)
        if manifest is None:  # pragma: no cover - source/destination manifests are required
            raise PluginError(f"{kind} plugin {reference.plugin!r} has no manifest")
        schema = manifest.config_schema
        manifest_version = manifest.version
        capabilities = manifest.capabilities
        manifest_aware = True
    descriptor = _static_descriptor(factory, kind=kind, name=reference.plugin)
    version, implementation = _implementation_identity(
        factory,
        declared_version=manifest_version,
        descriptor=descriptor,
    )
    config = _effective_config(
        reference.config,
        schema=schema,
        factory=factory,
        kind=kind,
        plugin=reference.plugin,
        base_dir=base_dir,
    )
    return ConnectorPlan(
        kind=kind,
        plugin=reference.plugin,
        version=version,
        config=config,
        capabilities=capabilities,
        manifest_aware=manifest_aware,
        implementation=implementation,
        config_schema=schema,
    )


def _plan_extensions(
    registry: PipelineRegistry,
    source: ConnectorPlan,
    base_dir: Path | None,
) -> tuple[ConnectorPlan, tuple[ExtensionPlan, ...]]:
    if source.plugin != "local_documents":
        return source, ()
    source_config = _json_object(source.config, path="sources.local_documents")
    explicit_fields = set(source_config)
    if source_config.get("ocr_mode") in {"auto", "always"}:
        source_config.setdefault(
            "ocr_engine",
            {"plugin": "tesseract", "config": {}},
        )
    if (
        source_config.get("ocr_mode") in {"auto", "always"}
        or source_config.get("table_mode") in {"auto", "local", "vision"}
        or source_config.get("vision_fallback") is True
    ):
        source_config.setdefault(
            "page_renderer",
            {"plugin": "pdfium", "config": {}},
        )
    plans: list[ExtensionPlan] = []
    for field, kind in _DOCUMENT_EXTENSION_FIELDS.items():
        value = source_config.get(field)
        if value is None:
            continue
        if not isinstance(value, Mapping):
            raise ConfigurationError(f"sources.local_documents.{field} must be a component object")
        reference = ComponentRef.from_dict(value, path=f"sources.local_documents.{field}")
        provider = _plan_provider(registry, kind, reference, base_dir)
        source_config[field] = {
            "plugin": provider.plugin,
            "config": _json_object(provider.config, path=f"{kind}.{provider.plugin}.config"),
        }
        plans.append(
            ExtensionPlan(
                field=field,
                kind=kind,
                plugin=provider.plugin,
                config=provider.config,
                built_in=provider.built_in,
                version=provider.version,
                implementation=provider.implementation,
                descriptor=provider.descriptor,
                implicit=field not in explicit_fields,
                config_schema=provider.config_schema,
            )
        )
    return (
        ConnectorPlan(
            kind=source.kind,
            plugin=source.plugin,
            version=source.version,
            config=source_config,
            capabilities=source.capabilities,
            manifest_aware=source.manifest_aware,
            implementation=source.implementation,
            config_schema=source.config_schema,
        ),
        tuple(plans),
    )


def _plan_warnings(spec: PipelineSpec, source: ConnectorPlan) -> tuple[str, ...]:
    warnings: list[str] = []
    if spec.state_namespace is not None:
        warnings.append(
            "An explicit state_namespace disables automatic checkpoint isolation for changes "
            "to the source, document extension providers, transforms, selected streams, "
            "destination, plugin or implementation versions, effective configuration defaults, "
            "and resolved resource paths; reuse it only when processing and materialization "
            "semantics remain compatible."
        )
    if source.plugin == "local_documents":
        config = source.config
        definition_config = spec.source.config
        if config.get("ocr_mode") == "always":
            warnings.append(
                "ocr_mode='always' replaces native PDF text with OCR output; prefer 'auto' "
                "unless the corpus is known to contain unreliable text layers."
            )
        if config.get("failure_mode") == "best_effort":
            warnings.append(
                "failure_mode='best_effort' emits warning events and skips failed files; "
                "capture pipeline events so skipped documents remain visible."
            )
        if config.get("ocr_mode") in {"auto", "always"} and "ocr_engine" not in definition_config:
            warnings.append("OCR will use the built-in local Tesseract adapter.")
        if (
            config.get("ocr_mode") in {"auto", "always"}
            and "page_renderer" not in definition_config
        ):
            warnings.append("PDF pages that need OCR will use the built-in PDFium renderer.")
    return tuple(warnings)


def _instantiate_connector(
    registry: PipelineRegistry,
    plan: ConnectorPlan,
    *,
    extensions: Sequence[ExtensionPlan] = (),
) -> object:
    factory = registry.factory(plan.kind, plan.plugin)
    config = _json_object(plan.config, path=f"{plan.kind}.{plan.plugin}.config")
    if plan.config_schema is not None:
        config = _coerce_config(config, plan.config_schema, path=f"{plan.kind}.{plan.plugin}")
    if plan.kind == "sources" and plan.plugin == "local_documents":
        config = _instantiate_document_extensions(registry, config, extensions)
    try:
        return factory(**config)
    except (ConfigurationError, PluginError):
        raise
    except TypeError as exc:
        raise ConfigurationError(
            f"Could not construct {plan.kind} plugin {plan.plugin!r}: {exc}"
        ) from exc


def _instantiate_document_extensions(
    registry: PipelineRegistry,
    config: dict[str, Any],
    extensions: Sequence[ExtensionPlan],
) -> dict[str, Any]:
    resolved = dict(config)
    by_field = {extension.field: extension for extension in extensions}
    for field, kind in _DOCUMENT_EXTENSION_FIELDS.items():
        extension = by_field.get(field)
        if extension is None:
            continue
        factory = registry.factory(kind, extension.plugin)
        extension_config = _json_object(extension.config, path=f"{kind}.{extension.plugin}.config")
        if extension.config_schema is not None:
            extension_config = _coerce_config(
                extension_config,
                extension.config_schema,
                path=f"{kind}.{extension.plugin}",
            )
        try:
            instance = factory(**extension_config)
        except (ConfigurationError, PluginError):
            raise
        except TypeError as exc:
            raise ConfigurationError(
                f"Could not construct {kind} plugin {extension.plugin!r}: {exc}"
            ) from exc
        _validate_instance_contract(kind, extension.plugin, instance)
        resolved[field] = instance
    return resolved


def _instantiate_state_store(
    registry: PipelineRegistry,
    reference: ComponentRef,
    schema: Mapping[str, Any] | None = None,
) -> StateStore:
    config = _json_object(reference.config, path=f"state_stores.{reference.plugin}.config")
    if schema is not None:
        config = _coerce_config(config, schema, path=f"state_stores.{reference.plugin}")
    factory = registry.factory("state_stores", reference.plugin)
    try:
        state_store = factory(**config)
    except TypeError as exc:
        raise ConfigurationError(
            f"Could not construct state-store plugin {reference.plugin!r}: {exc}"
        ) from exc
    if not isinstance(state_store, StateStore):
        raise PluginError(f"State-store plugin {reference.plugin!r} did not return StateStore")
    return state_store


def _validate_document_configuration(
    config: Mapping[str, JSONValue],
    extensions: Sequence[ExtensionPlan],
    registry: PipelineRegistry,
) -> None:
    table_mode = config.get("table_mode", "off")
    if table_mode in {"auto", "local"} and "table_extractor" not in config:
        raise ConfigurationError(
            "Declarative table_mode='auto' or 'local' requires an explicit table_extractor "
            "component. Wrap your offline Docling/Paddle provider as a table_extractor plugin; "
            "the SDK will not guess model paths or download models."
        )
    if table_mode == "vision" and "vision_extractor" not in config:
        raise ConfigurationError("table_mode='vision' requires a vision_extractor component")
    if config.get("vision_fallback") and "vision_extractor" not in config:
        raise ConfigurationError("vision_fallback requires a vision_extractor component")
    if "vision_extractor" in config and "external_processing_policy" not in config:
        raise ConfigurationError(
            "vision_extractor requires an explicit external_processing_policy component"
        )
    active_fields = {"document_splitter"}
    if config.get("ocr_mode") in {"auto", "always"}:
        active_fields.update(("ocr_engine", "page_renderer"))
    if table_mode in {"auto", "local"}:
        active_fields.update(("table_extractor", "page_renderer"))
    if table_mode == "vision" or config.get("vision_fallback") is True:
        active_fields.update(("vision_extractor", "page_renderer"))
    risky = [
        extension
        for extension in extensions
        if extension.field in active_fields
        and extension.descriptor is not None
        and (
            extension.descriptor.get("external") is True
            or extension.descriptor.get("deterministic") is False
        )
    ]
    if not risky:
        return
    cache = next(
        (extension for extension in extensions if extension.field == "extraction_cache"),
        None,
    )
    if cache is None:
        raise ConfigurationError(
            "External or nondeterministic document components require a persistent extraction cache"
        )
    cache_type = _factory_result_type(registry.factory(cache.kind, cache.plugin))
    if cache_type is None or getattr(cache_type, "persistent", False) is not True:
        raise ConfigurationError(
            "External or nondeterministic document components require a persistent extraction cache"
        )


def _effective_config(
    config: Mapping[str, Any],
    *,
    schema: Mapping[str, Any] | None,
    factory: Factory,
    kind: str,
    plugin: str,
    base_dir: Path | None,
) -> dict[str, JSONValue]:
    path = f"{kind}.{plugin}"
    effective = _json_object(config, path=f"{path}.config")
    if schema is not None:
        effective = cast(dict[str, JSONValue], _apply_defaults(effective, schema))
        effective = _apply_signature_defaults(
            effective,
            factory,
            properties=_schema_properties(schema),
            allow_undeclared=schema.get("additionalProperties") is not False,
            schema=schema,
            path=path,
        )
        effective = cast(dict[str, JSONValue], _apply_defaults(effective, schema))
    else:
        effective = _apply_signature_defaults(
            effective,
            factory,
            properties=None,
            allow_undeclared=True,
            schema=None,
            path=path,
        )
    effective = _resolve_config_paths(
        effective,
        schema=schema,
        kind=kind,
        plugin=plugin,
        base_dir=base_dir,
    )
    if schema is not None:
        _validate_schema(effective, schema, path=path)
    _validate_factory_arguments(factory, effective, kind=kind, plugin=plugin)
    return effective


def _apply_defaults(value: JSONValue, schema: Mapping[str, Any]) -> JSONValue:
    one_of = schema.get("oneOf")
    if isinstance(one_of, Sequence) and not isinstance(one_of, (str, bytes, bytearray)):
        matches: list[JSONValue] = []
        for candidate in one_of:
            if not isinstance(candidate, Mapping):
                continue
            candidate_value = _apply_defaults(value, candidate)
            try:
                _validate_schema(candidate_value, candidate, path="default")
            except ConfigurationError:
                continue
            matches.append(candidate_value)
        if len(matches) == 1:
            return matches[0]

    if isinstance(value, Mapping):
        result = _json_object(value, path="default")
        properties = _schema_properties(schema)
        for key, field_schema in properties.items():
            if key not in result and "default" in field_schema:
                result[key] = _copy_json(field_schema["default"], path=f"default.{key}")
            if key in result:
                result[key] = _apply_defaults(result[key], field_schema)
        return result
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        item_schema = schema.get("items")
        values = list(value)
        if isinstance(item_schema, Mapping):
            return [_apply_defaults(item, item_schema) for item in values]
        return values
    return value


def _apply_signature_defaults(
    config: dict[str, JSONValue],
    factory: Factory,
    *,
    properties: Mapping[str, Mapping[str, Any]] | None,
    allow_undeclared: bool,
    schema: Mapping[str, Any] | None,
    path: str,
) -> dict[str, JSONValue]:
    try:
        signature = inspect.signature(factory)
    except (TypeError, ValueError):
        return config
    result = dict(config)
    for parameter in signature.parameters.values():
        if (
            parameter.name in result
            or parameter.default is inspect.Parameter.empty
            or parameter.kind in {inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD}
            or (
                properties is not None and parameter.name not in properties and not allow_undeclared
            )
        ):
            continue
        try:
            default = _copy_json(
                parameter.default, path=f"{path}.{parameter.name}.constructor_default"
            )
        except ConfigurationError:
            continue
        if properties is not None and parameter.name in properties:
            field_schema = properties[parameter.name]
            try:
                _validate_schema(default, field_schema, path=f"{path}.{parameter.name}")
            except ConfigurationError:
                continue
        result[parameter.name] = default
    if schema is not None:
        _validate_schema(result, schema, path=path)
    return result


def _resolve_config_paths(
    config: dict[str, JSONValue],
    *,
    schema: Mapping[str, Any] | None,
    kind: str,
    plugin: str,
    base_dir: Path | None,
) -> dict[str, JSONValue]:
    if base_dir is None:
        return config
    resolved = cast(
        dict[str, JSONValue],
        _resolve_schema_paths(config, schema or {}, base_dir),
    )
    for field in _PATH_FIELDS.get((kind, plugin), ()):
        value = resolved.get(field)
        if value is None:
            continue
        if field == "paths":
            if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
                continue
            resolved[field] = [_resolve_path(item, base_dir) for item in value]
        elif isinstance(value, str):
            resolved[field] = _resolve_path(value, base_dir)
    return resolved


def _resolve_schema_paths(
    value: JSONValue,
    schema: Mapping[str, Any],
    base_dir: Path,
) -> JSONValue:
    value_format = schema.get("format")
    if value_format == "local-path":
        return _resolve_path(value, base_dir)
    if value_format == "local-paths":
        if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
            raise ConfigurationError("Portable path lists must be arrays of strings")
        return [_resolve_path(item, base_dir) for item in value]
    one_of = schema.get("oneOf")
    if isinstance(one_of, Sequence) and not isinstance(one_of, (str, bytes, bytearray)):
        for candidate in one_of:
            if not isinstance(candidate, Mapping):
                continue
            try:
                _validate_schema(value, candidate, path="path")
            except ConfigurationError:
                continue
            return _resolve_schema_paths(value, candidate, base_dir)
    if isinstance(value, Mapping):
        properties = _schema_properties(schema)
        return {
            str(key): _resolve_schema_paths(
                item,
                properties.get(str(key), {}),
                base_dir,
            )
            for key, item in value.items()
        }
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        item_schema = schema.get("items")
        if isinstance(item_schema, Mapping):
            return [_resolve_schema_paths(item, item_schema, base_dir) for item in value]
        return list(value)
    return value


def _resolve_path(value: Any, base_dir: Path) -> str:
    if not isinstance(value, str):
        raise ConfigurationError("Portable path values must be strings")
    candidate = Path(value).expanduser()
    if not candidate.is_absolute():
        candidate = base_dir / candidate
    return str(candidate.resolve())


def _schema_properties(
    schema: Mapping[str, Any],
) -> Mapping[str, Mapping[str, Any]]:
    properties = schema.get("properties", {})
    if not isinstance(properties, Mapping):
        return {}
    return {
        str(key): cast(Mapping[str, Any], value)
        for key, value in properties.items()
        if isinstance(value, Mapping)
    }


def _validate_factory_arguments(
    factory: Factory,
    config: Mapping[str, Any],
    *,
    kind: str,
    plugin: str,
) -> None:
    try:
        inspect.signature(factory).bind(**config)
    except (TypeError, ValueError) as exc:
        raise ConfigurationError(
            f"Invalid configuration for {kind} plugin {plugin!r}: {exc}"
        ) from exc


def _coerce_config(
    config: Mapping[str, Any],
    schema: Mapping[str, Any],
    *,
    path: str,
) -> dict[str, Any]:
    properties = schema.get("properties", {})
    if not isinstance(properties, Mapping):
        return dict(config)
    coerced = dict(config)
    for key, field_schema in properties.items():
        if key not in coerced or not isinstance(field_schema, Mapping):
            continue
        coerced[key] = _coerce_value(coerced[key], field_schema, path=f"{path}.{key}")
    return coerced


def _coerce_value(value: Any, schema: Mapping[str, Any], *, path: str) -> Any:
    one_of = schema.get("oneOf")
    if isinstance(one_of, Sequence) and not isinstance(one_of, (str, bytes, bytearray)):
        matches: list[Mapping[str, Any]] = []
        for candidate in one_of:
            if not isinstance(candidate, Mapping):
                continue
            try:
                _validate_schema(value, candidate, path=path)
            except ConfigurationError:
                continue
            matches.append(candidate)
        if len(matches) == 1:
            return _coerce_value(value, matches[0], path=path)
    raw_types = schema.get("type")
    allowed_types = [raw_types] if isinstance(raw_types, str) else raw_types
    if (
        value is None
        and isinstance(allowed_types, Sequence)
        and not isinstance(allowed_types, (str, bytes, bytearray))
        and "null" in allowed_types
    ):
        return None
    if schema.get("format") == "secret-ref":
        if not isinstance(value, str):
            raise ConfigurationError(f"{path} must be a secret reference")
        provider, separator, key = value.partition(":")
        if not separator or not provider or not key:
            raise ConfigurationError(f"{path} must use '<provider>:<key>', for example env:API_KEY")
        return SecretRef(key=key, provider=provider)
    if isinstance(value, Mapping) and schema.get("type") == "object":
        return _coerce_config(value, schema, path=path)
    if isinstance(value, list) and isinstance(schema.get("items"), Mapping):
        item_schema = schema["items"]
        return [
            _coerce_value(item, item_schema, path=f"{path}[{index}]")
            for index, item in enumerate(value)
        ]
    return value


def _validate_factory_contract(kind: str, plugin: str, factory: Factory) -> None:
    if inspect.isclass(factory) and inspect.isabstract(factory):
        raise PluginError(f"{kind} plugin {plugin!r} is abstract and cannot be constructed")
    if kind == "sources":
        _require_subclass(factory, Source, kind=kind, plugin=plugin)
        return
    if kind == "destinations":
        _require_subclass(factory, Destination, kind=kind, plugin=plugin)
        target = _factory_result_type(factory)
        if target is None or getattr(target, "idempotent", False) is not True:
            raise PluginError(f"Destination plugin {plugin!r} must declare idempotent=True")
        return
    if kind == "transforms":
        _require_subclass(factory, Transform, kind=kind, plugin=plugin)
        return
    if kind == "state_stores":
        _require_subclass(factory, StateStore, kind=kind, plugin=plugin)
        return
    methods = _PROVIDER_METHODS.get(kind)
    if methods is None:
        raise PluginError(f"Unknown plugin kind: {kind!r}")
    target = _factory_result_type(factory)
    if target is None:
        raise PluginError(
            f"{kind} plugin {plugin!r} must be a class or declare a concrete return type "
            "so its provider contract can be validated without construction"
        )
    missing = [method for method in methods if not callable(getattr(target, method, None))]
    if missing:
        raise PluginError(
            f"{kind} plugin {plugin!r} is missing required methods: {', '.join(missing)}"
        )


def _require_subclass(
    factory: Factory,
    expected: type[object],
    *,
    kind: str,
    plugin: str,
) -> None:
    target = _factory_result_type(factory)
    if target is None or not issubclass(target, expected):
        raise PluginError(f"{kind} plugin {plugin!r} must construct {expected.__name__} instances")


def _factory_result_type(factory: Factory) -> type[Any] | None:
    if inspect.isclass(factory):
        return factory
    try:
        signature = inspect.signature(factory)
    except (TypeError, ValueError):
        return None
    try:
        annotation = get_type_hints(factory).get("return", signature.return_annotation)
    except (NameError, TypeError):
        annotation = signature.return_annotation
    if annotation is inspect.Signature.empty:
        return None
    origin = get_origin(annotation)
    if origin is not None:
        candidates = [
            item
            for item in get_args(annotation)
            if inspect.isclass(item) and item is not type(None)
        ]
        return candidates[0] if len(candidates) == 1 else None
    return annotation if inspect.isclass(annotation) else None


def _validate_instance_contract(kind: str, plugin: str, instance: object) -> None:
    methods = _PROVIDER_METHODS.get(kind, ())
    missing = [method for method in methods if not callable(getattr(instance, method, None))]
    if missing:
        raise PluginError(
            f"{kind} plugin {plugin!r} returned an invalid provider missing: " + ", ".join(missing)
        )


def _static_descriptor(
    factory: Factory,
    *,
    kind: str,
    name: str,
) -> ComponentDescriptor | None:
    target = _factory_result_type(factory)
    if target is None:
        return None
    try:
        descriptor = inspect.getattr_static(target, "descriptor")
    except AttributeError:
        return None
    if isinstance(descriptor, (property, staticmethod, classmethod)):
        return None
    if not isinstance(descriptor, ComponentDescriptor):
        raise PluginError(f"{kind} plugin {name!r} has invalid static descriptor metadata")
    return descriptor


def _implementation_identity(
    factory: Factory,
    *,
    declared_version: str | None,
    descriptor: ComponentDescriptor | None,
) -> tuple[str, dict[str, JSONValue]]:
    module_name = getattr(factory, "__module__", type(factory).__module__)
    qualified_name = getattr(factory, "__qualname__", type(factory).__qualname__)
    version = declared_version
    version_source = "manifest"
    if version is None and descriptor is not None:
        version = descriptor.version
        version_source = "descriptor"
    if version is None:
        candidate = getattr(factory, "__version__", None)
        if isinstance(candidate, str) and candidate.strip():
            version = candidate.strip()
            version_source = "implementation"
    if version is None:
        distribution_version = _distribution_version(module_name)
        if distribution_version is not None:
            version = distribution_version
            version_source = "distribution"
    if version is None:
        try:
            source = inspect.getsource(factory).encode("utf-8")
        except (OSError, TypeError):
            source = f"{module_name}:{qualified_name}".encode()
        version = f"sha256:{hashlib.sha256(source).hexdigest()}"
        version_source = "source"
    identity: dict[str, JSONValue] = {
        "module": str(module_name),
        "qualname": str(qualified_name),
        "version": version,
        "version_source": version_source,
    }
    return version, identity


def _distribution_version(module_name: str) -> str | None:
    top_level = module_name.partition(".")[0]
    try:
        return metadata.version(top_level)
    except metadata.PackageNotFoundError:
        return None


def _json_object(value: Any, *, path: str) -> dict[str, JSONValue]:
    copied = _copy_json(value, path=path)
    if not isinstance(copied, dict):
        raise ConfigurationError(f"{path} must be an object")
    return copied


def _immutable_json_mapping(value: Any, *, path: str) -> Mapping[str, JSONValue]:
    return ComponentRef("immutable", _json_object(value, path=path)).config


def _validate_schema(value: Any, schema: Mapping[str, Any], *, path: str) -> None:
    one_of = schema.get("oneOf")
    if isinstance(one_of, Sequence) and not isinstance(one_of, (str, bytes, bytearray)):
        successes = 0
        messages: list[str] = []
        for candidate in one_of:
            if not isinstance(candidate, Mapping):
                continue
            try:
                _validate_schema(value, candidate, path=path)
            except ConfigurationError as exc:
                messages.append(str(exc))
            else:
                successes += 1
        if successes != 1:
            detail = "; ".join(messages[:2])
            raise ConfigurationError(f"{path} must match exactly one allowed shape: {detail}")
        return

    raw_types = schema.get("type")
    if raw_types is not None:
        types = [raw_types] if isinstance(raw_types, str) else list(raw_types)
        if not any(_matches_type(value, item) for item in types if isinstance(item, str)):
            expected = " or ".join(str(item) for item in types)
            raise ConfigurationError(f"{path} must be {expected}")

    if "enum" in schema and value not in schema["enum"]:
        raise ConfigurationError(f"{path} must be one of {schema['enum']!r}")

    if isinstance(value, str):
        minimum_length = schema.get("minLength")
        if isinstance(minimum_length, int) and len(value) < minimum_length:
            raise ConfigurationError(f"{path} is shorter than {minimum_length} characters")
        maximum_length = schema.get("maxLength")
        if isinstance(maximum_length, int) and len(value) > maximum_length:
            raise ConfigurationError(f"{path} is longer than {maximum_length} characters")
        pattern = schema.get("pattern")
        if isinstance(pattern, str) and re.fullmatch(pattern, value) is None:
            raise ConfigurationError(f"{path} does not match required pattern {pattern!r}")
        if schema.get("format") == "secret-ref":
            provider, separator, key = value.partition(":")
            if not separator or not provider or not key:
                raise ConfigurationError(
                    f"{path} must be a reference such as env:API_KEY, not a secret value"
                )

    if isinstance(value, (int, float)) and not isinstance(value, bool):
        minimum = schema.get("minimum")
        if isinstance(minimum, (int, float)) and value < minimum:
            raise ConfigurationError(f"{path} must be at least {minimum}")
        maximum = schema.get("maximum")
        if isinstance(maximum, (int, float)) and value > maximum:
            raise ConfigurationError(f"{path} must be at most {maximum}")
        exclusive_minimum = schema.get("exclusiveMinimum")
        if isinstance(exclusive_minimum, (int, float)) and value <= exclusive_minimum:
            raise ConfigurationError(f"{path} must be greater than {exclusive_minimum}")

    if isinstance(value, Mapping):
        properties = schema.get("properties", {})
        if not isinstance(properties, Mapping):
            properties = {}
        required = schema.get("required", [])
        if isinstance(required, Sequence) and not isinstance(required, (str, bytes, bytearray)):
            missing = [item for item in required if isinstance(item, str) and item not in value]
            if missing:
                raise ConfigurationError(f"{path} is missing required fields: {', '.join(missing)}")
        if schema.get("additionalProperties") is False:
            unknown = sorted(set(value) - set(properties))
            if unknown:
                raise ConfigurationError(f"{path} contains unknown fields: {', '.join(unknown)}")
        for key, item in value.items():
            child_schema = properties.get(key)
            if isinstance(child_schema, Mapping):
                _validate_schema(item, child_schema, path=f"{path}.{key}")

    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        minimum_items = schema.get("minItems")
        if isinstance(minimum_items, int) and len(value) < minimum_items:
            raise ConfigurationError(f"{path} must contain at least {minimum_items} items")
        maximum_items = schema.get("maxItems")
        if isinstance(maximum_items, int) and len(value) > maximum_items:
            raise ConfigurationError(f"{path} must contain at most {maximum_items} items")
        if schema.get("uniqueItems") is True:
            normalized = [repr(item) for item in value]
            if len(normalized) != len(set(normalized)):
                raise ConfigurationError(f"{path} must contain unique items")
        item_schema = schema.get("items")
        if isinstance(item_schema, Mapping):
            for index, item in enumerate(value):
                _validate_schema(item, item_schema, path=f"{path}[{index}]")


def _matches_type(value: Any, expected: str) -> bool:
    if expected == "null":
        return value is None
    if expected == "boolean":
        return isinstance(value, bool)
    if expected == "integer":
        return isinstance(value, int) and not isinstance(value, bool)
    if expected == "number":
        return isinstance(value, (int, float)) and not isinstance(value, bool)
    if expected == "string":
        return isinstance(value, str)
    if expected == "array":
        return isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray))
    if expected == "object":
        return isinstance(value, Mapping)
    return False


__all__ = [
    "ConnectorPlan",
    "ExtensionPlan",
    "PipelinePlan",
    "PipelineRegistry",
    "create_ingestion_pipeline",
    "plan_ingestion_pipeline",
    "run_ingestion_pipeline",
]
