from __future__ import annotations

import asyncio
import json
from importlib.metadata import EntryPoint
from pathlib import Path
from unittest.mock import patch

import pytest

from ingestion_graph import (
    CheckResult,
    ComponentRef,
    ConnectorCapabilities,
    ConnectorSpec,
    Destination,
    LocalDocumentsSource,
    PipelineSpec,
    ProviderSpec,
    SecretRef,
    Source,
    StateStoreRef,
    StreamDescriptor,
    Transform,
    create_ingestion_pipeline,
    plan_ingestion_pipeline,
    run_ingestion_pipeline,
)
from ingestion_graph.destinations import SQLiteCollection
from ingestion_graph.document_ai import ComponentDescriptor, OcrResult
from ingestion_graph.errors import ConfigurationError, PluginError, ProtocolError
from ingestion_graph.plugins import discover_plugins
from ingestion_graph.query import QueryRequest
from ingestion_graph.runtime import PipelineRegistry
from ingestion_graph.state import StateStore


def _write_toml_spec(root: Path) -> Path:
    path = root / "pipeline.toml"
    path.write_text(
        """
schema_version = "1"
name = "portable-jsonl"

[source]
plugin = "jsonl"
[source.config]
path = "data/items.jsonl"
batch_size = 1

[destination]
plugin = "sqlite"
[destination.config]
path = ".ingestion/items.db"

[state_store]
plugin = "sqlite"
[state_store.config]
path = ".ingestion/state.db"
""".strip(),
        encoding="utf-8",
    )
    return path


class _CallableOcr:
    descriptor = ComponentDescriptor("callable-ocr", "1")

    def __init__(self, *, model: str = "local") -> None:
        self.model = model

    async def check(self):
        return CheckResult(True)

    async def recognize(self, image, *, language="eng"):
        del image, language
        return OcrResult("callable")

    async def close(self) -> None:
        return None


def _make_callable_ocr(*, model: str = "local") -> _CallableOcr:
    return _CallableOcr(model=model)


def test_pipeline_spec_round_trip_and_checkpoint_identity() -> None:
    spec = PipelineSpec.from_dict(
        {
            "schema_version": "1",
            "name": "people",
            "source": {"plugin": "jsonl", "config": {"path": "people.jsonl"}},
            "destination": {"plugin": "sqlite", "config": {"path": "people.db"}},
            "metadata": {"owner": "example"},
        }
    )
    restored = PipelineSpec.from_dict(spec.to_dict())
    different_destination = PipelineSpec.from_dict(
        {
            **spec.to_dict(),
            "destination": {"plugin": "sqlite", "config": {"path": "other.db"}},
        }
    )
    different_source = PipelineSpec.from_dict(
        {
            **spec.to_dict(),
            "source": {
                "plugin": "jsonl",
                "config": {"path": "people.jsonl", "id_field": "person_id"},
            },
        }
    )
    different_transform = PipelineSpec.from_dict(
        {
            **spec.to_dict(),
            "transforms": [{"plugin": "normalize-person", "config": {"version": 2}}],
        }
    )
    different_state_store = PipelineSpec.from_dict(
        {
            **spec.to_dict(),
            "state_store": {"plugin": "sqlite", "config": {"path": "other-state.db"}},
        }
    )

    assert restored == spec
    assert different_destination.fingerprint != spec.fingerprint
    assert (
        different_destination.definition_processing_fingerprint
        == spec.definition_processing_fingerprint
    )
    assert (
        different_destination.definition_materialization_fingerprint
        != spec.definition_materialization_fingerprint
    )
    assert (
        different_source.definition_processing_fingerprint != spec.definition_processing_fingerprint
    )
    assert (
        different_source.definition_materialization_fingerprint
        != spec.definition_materialization_fingerprint
    )
    assert (
        different_transform.definition_processing_fingerprint
        != spec.definition_processing_fingerprint
    )
    assert different_state_store.fingerprint != spec.fingerprint
    assert (
        different_state_store.definition_materialization_fingerprint
        == spec.definition_materialization_fingerprint
    )


@pytest.mark.asyncio
async def test_toml_spec_resolves_paths_relative_to_the_spec_and_runs_incrementally(
    tmp_path,
) -> None:
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    input_path = data_dir / "items.jsonl"
    input_path.write_text(
        json.dumps({"id": "1", "name": "Ada"}) + "\n",
        encoding="utf-8",
    )
    spec_path = _write_toml_spec(tmp_path)

    spec = PipelineSpec.load(spec_path)
    plan = plan_ingestion_pipeline(spec)
    pipeline = create_ingestion_pipeline(spec)

    assert pipeline.source.path == input_path.resolve()  # type: ignore[attr-defined]
    assert pipeline.destination.path == (tmp_path / ".ingestion/items.db").resolve()  # type: ignore[attr-defined]
    assert pipeline.state_store.path == (tmp_path / ".ingestion/state.db").resolve()  # type: ignore[attr-defined]
    assert pipeline.state_namespace == plan.state_namespace

    first = await run_ingestion_pipeline(spec_path)
    second = await run_ingestion_pipeline(spec_path)
    result = await SQLiteCollection(tmp_path / ".ingestion/items.db").query(QueryRequest("Ada"))

    assert first.records_written == 1
    assert second.records_written == 0
    assert result.total == 1
    assert result[0].envelope.metadata["line_number"] == 1


@pytest.mark.asyncio
async def test_destination_change_gets_a_fresh_replay_with_a_shared_state_store(tmp_path) -> None:
    input_path = tmp_path / "items.jsonl"
    input_path.write_text(json.dumps({"id": "1", "name": "Ada"}) + "\n", encoding="utf-8")
    state_path = tmp_path / "state.db"

    first = PipelineSpec(
        name="portable",
        source=ComponentRef("jsonl", {"path": str(input_path)}),
        destination=ComponentRef("sqlite", {"path": str(tmp_path / "first.db")}),
        state_store=StateStoreRef("sqlite", {"path": str(state_path)}),
    )
    second = PipelineSpec.from_dict(
        {
            **first.to_dict(),
            "destination": {"plugin": "sqlite", "config": {"path": str(tmp_path / "second.db")}},
        }
    )

    first_result = await run_ingestion_pipeline(first)
    second_result = await run_ingestion_pipeline(second)
    second_query = await SQLiteCollection(tmp_path / "second.db").query(QueryRequest("Ada"))

    assert first_result.records_written == 1
    assert second_result.records_written == 1
    assert (
        plan_ingestion_pipeline(first).state_namespace
        != plan_ingestion_pipeline(second).state_namespace
    )
    assert second_query.total == 1


def test_plan_validates_secret_references_without_resolving_them() -> None:
    valid = PipelineSpec.from_dict(
        {
            "name": "discord",
            "source": {
                "plugin": "discord",
                "config": {"channel_ids": ["123"], "token": "env:DISCORD_BOT_TOKEN"},
            },
            "destination": {"plugin": "jsonl", "config": {"path": "out.jsonl"}},
        }
    )

    plan = plan_ingestion_pipeline(valid)

    assert plan.source.config["token"] == "env:DISCORD_BOT_TOKEN"
    with pytest.raises(ConfigurationError, match="reference such as env:API_KEY"):
        plan_ingestion_pipeline(
            PipelineSpec.from_dict(
                {
                    **valid.to_dict(),
                    "source": {
                        "plugin": "discord",
                        "config": {"channel_ids": ["123"], "token": "raw-token"},
                    },
                }
            )
        )


def test_declarative_document_components_are_explicit_and_replaceable(tmp_path) -> None:
    class FakeOcr:
        descriptor = ComponentDescriptor("learning-ocr", "1")

        def __init__(self, *, model: str) -> None:
            self.model = model

        async def check(self):
            return CheckResult(True)

        async def recognize(self, image, *, language="eng"):
            del image, language
            return OcrResult("recognized")

        async def close(self) -> None:
            return None

    registry = PipelineRegistry()
    registry.register("ocr_engines", "learning-ocr", FakeOcr)
    spec = PipelineSpec.from_dict(
        {
            "name": "documents",
            "source": {
                "plugin": "local_documents",
                "config": {
                    "paths": [str(tmp_path)],
                    "ocr_mode": "auto",
                    "ocr_engine": {
                        "plugin": "learning-ocr",
                        "config": {"model": "v1"},
                    },
                    "failure_mode": "best_effort",
                },
            },
            "destination": {"plugin": "sqlite", "config": {"path": "documents.db"}},
            "state_store": {"plugin": "memory", "config": {}},
        }
    )

    plan = plan_ingestion_pipeline(spec, registry=registry)
    pipeline = create_ingestion_pipeline(spec, registry=registry)

    assert plan.extensions[0].field == "ocr_engine"
    assert plan.extensions[0].plugin == "learning-ocr"
    assert pipeline.source.ocr_engine.model == "v1"  # type: ignore[attr-defined]
    assert any("best_effort" in warning for warning in plan.warnings)


def test_declarative_local_table_mode_requires_a_provider(tmp_path) -> None:
    spec = PipelineSpec.from_dict(
        {
            "name": "tables",
            "source": {
                "plugin": "local_documents",
                "config": {
                    "paths": [str(tmp_path)],
                    "table_mode": "local",
                },
            },
            "destination": {"plugin": "sqlite", "config": {}},
        }
    )

    with pytest.raises(ConfigurationError, match="explicit table_extractor"):
        plan_ingestion_pipeline(spec)


@pytest.mark.asyncio
async def test_unknown_spec_fields_and_bad_stream_selection_fail_closed(tmp_path) -> None:
    with pytest.raises(ConfigurationError, match="unknown fields"):
        PipelineSpec.from_dict(
            {
                "name": "bad",
                "source": {"plugin": "jsonl", "config": {"path": "input.jsonl"}},
                "destination": {"plugin": "sqlite", "config": {}},
                "surprise": True,
            }
        )

    input_path = tmp_path / "input.jsonl"
    input_path.write_text(json.dumps({"id": 1}) + "\n", encoding="utf-8")
    spec = PipelineSpec(
        name="selected",
        source=ComponentRef("jsonl", {"path": str(input_path)}),
        destination=ComponentRef("sqlite", {"path": str(tmp_path / "query.db")}),
        streams=("missing",),
    )

    with pytest.raises(ProtocolError, match="did not discover requested streams"):
        await run_ingestion_pipeline(spec)


def test_cli_validate_and_plan_are_static_and_machine_readable(
    tmp_path, monkeypatch, capsys
) -> None:
    from ingestion_graph.cli import main

    spec_path = _write_toml_spec(tmp_path)
    monkeypatch.setattr("sys.argv", ["ingestion-graph", "validate", str(spec_path), "--json"])
    assert main() == 0
    validation = json.loads(capsys.readouterr().out)
    assert validation["valid"] is True
    assert validation["name"] == "portable-jsonl"

    monkeypatch.setattr("sys.argv", ["ingestion-graph", "plan", str(spec_path), "--json"])
    assert main() == 0
    plan = json.loads(capsys.readouterr().out)
    assert plan["source"]["plugin"] == "jsonl"
    assert plan["destination"]["plugin"] == "sqlite"


def test_effective_defaults_are_canonical_and_resolved_before_identity(tmp_path) -> None:
    (tmp_path / "items.jsonl").write_text('{"id": 1}\n', encoding="utf-8")
    origin = tmp_path / "pipeline.toml"
    implicit = PipelineSpec.from_dict(
        {
            "name": "defaults",
            "source": {"plugin": "jsonl", "config": {"path": "items.jsonl"}},
            "destination": {"plugin": "sqlite", "config": {}},
            "state_store": {"plugin": "sqlite", "config": {}},
        },
        origin=origin,
    )
    explicit = PipelineSpec.from_dict(
        {
            **implicit.to_dict(),
            "source": {
                "plugin": "jsonl",
                "config": {
                    "path": "items.jsonl",
                    "id_field": "id",
                    "operation_field": "_operation",
                    "batch_size": 500,
                },
            },
            "destination": {
                "plugin": "sqlite",
                "config": {"path": ".ingestion/query.db"},
            },
            "state_store": {
                "plugin": "sqlite",
                "config": {"path": ".ingestion/state.db"},
            },
        },
        origin=origin,
    )

    implicit_plan = plan_ingestion_pipeline(implicit)
    explicit_plan = plan_ingestion_pipeline(explicit)
    pipeline = create_ingestion_pipeline(implicit, plan=implicit_plan)

    assert implicit.fingerprint != explicit.fingerprint
    assert implicit_plan.processing_fingerprint == explicit_plan.processing_fingerprint
    assert implicit_plan.materialization_fingerprint == explicit_plan.materialization_fingerprint
    assert implicit_plan.destination.config["path"] == str(
        (tmp_path / ".ingestion/query.db").resolve()
    )
    assert implicit_plan.state_store.config["path"] == str(
        (tmp_path / ".ingestion/state.db").resolve()
    )
    assert pipeline.destination.path == (tmp_path / ".ingestion/query.db").resolve()  # type: ignore[attr-defined]
    assert pipeline.state_store.path == (tmp_path / ".ingestion/state.db").resolve()  # type: ignore[attr-defined]


def test_undeclared_json_safe_constructor_defaults_are_canonical() -> None:
    base = {
        "name": "discord-defaults",
        "source": {
            "plugin": "discord",
            "config": {"channel_ids": ["123"], "token": "env:DISCORD_TOKEN"},
        },
        "destination": {"plugin": "jsonl", "config": {"path": "messages.jsonl"}},
    }
    implicit = PipelineSpec.from_dict(base)
    implicit_plan = plan_ingestion_pipeline(implicit)
    explicit = PipelineSpec.from_dict(
        {
            **base,
            "source": {
                "plugin": "discord",
                "config": {
                    "channel_ids": ["123"],
                    "token": "env:DISCORD_TOKEN",
                    "api_base": implicit_plan.source.config["api_base"],
                    "page_size": implicit_plan.source.config["page_size"],
                    "max_retries": implicit_plan.source.config["max_retries"],
                },
            },
        }
    )
    explicit_plan = plan_ingestion_pipeline(explicit)
    pipeline = create_ingestion_pipeline(implicit, plan=implicit_plan)

    assert implicit_plan.processing_fingerprint == explicit_plan.processing_fingerprint
    assert implicit_plan.source.config["page_size"] == 100
    assert implicit_plan.source.config["max_retries"] == 5
    assert pipeline.source.page_size == 100  # type: ignore[attr-defined]
    assert pipeline.source.max_retries == 5  # type: ignore[attr-defined]


@pytest.mark.asyncio
async def test_relocated_specs_do_not_share_incompatible_checkpoints(tmp_path) -> None:
    state_path = tmp_path / "shared-state.db"
    specs: list[PipelineSpec] = []
    for folder, item_id in (("first", 1), ("second", 2)):
        root = tmp_path / folder
        root.mkdir()
        (root / "items.jsonl").write_text(
            json.dumps({"id": item_id, "folder": folder}) + "\n",
            encoding="utf-8",
        )
        specs.append(
            PipelineSpec.from_dict(
                {
                    "name": "relocated",
                    "source": {"plugin": "jsonl", "config": {"path": "items.jsonl"}},
                    "destination": {"plugin": "sqlite", "config": {"path": "output.db"}},
                    "state_store": {
                        "plugin": "sqlite",
                        "config": {"path": str(state_path)},
                    },
                },
                origin=root / "pipeline.toml",
            )
        )

    plans = [plan_ingestion_pipeline(spec) for spec in specs]
    results = [
        await run_ingestion_pipeline(spec, plan=plan)
        for spec, plan in zip(specs, plans, strict=True)
    ]

    assert plans[0].processing_fingerprint != plans[1].processing_fingerprint
    assert plans[0].materialization_fingerprint != plans[1].materialization_fingerprint
    assert plans[0].state_namespace != plans[1].state_namespace
    assert [result.records_written for result in results] == [1, 1]
    with pytest.raises(ConfigurationError, match="different PipelineSpec origin"):
        create_ingestion_pipeline(specs[1], plan=plans[0])


def test_programmatic_relative_paths_bind_to_planning_working_directory(
    tmp_path, monkeypatch
) -> None:
    first_root = tmp_path / "first-cwd"
    second_root = tmp_path / "second-cwd"
    first_root.mkdir()
    second_root.mkdir()
    spec = PipelineSpec.from_dict(
        {
            "name": "cwd-bound",
            "source": {"plugin": "jsonl", "config": {"path": "items.jsonl"}},
            "destination": {"plugin": "sqlite", "config": {"path": "output.db"}},
            "state_store": {
                "plugin": "sqlite",
                "config": {"path": str(tmp_path / "shared-state.db")},
            },
        }
    )

    monkeypatch.chdir(first_root)
    first = plan_ingestion_pipeline(spec)
    monkeypatch.chdir(second_root)
    second = plan_ingestion_pipeline(spec)

    assert first.source.config["path"] == str((first_root / "items.jsonl").resolve())
    assert second.source.config["path"] == str((second_root / "items.jsonl").resolve())
    assert first.processing_fingerprint != second.processing_fingerprint
    assert first.materialization_fingerprint != second.materialization_fingerprint
    assert first.state_namespace != second.state_namespace
    with pytest.raises(ConfigurationError, match="different working directory"):
        create_ingestion_pipeline(spec, plan=first)


def test_provider_manifest_version_defaults_and_resource_paths_affect_identity(tmp_path) -> None:
    class VersionedOcr:
        manifest_version = "1"
        descriptor = ComponentDescriptor("versioned-ocr", "engine-2")

        def __init__(self, *, model_path: str = "models/default.bin") -> None:
            self.model_path = model_path

        @classmethod
        def manifest(cls) -> ProviderSpec:
            return ProviderSpec(
                "versioned",
                cls.manifest_version,
                {
                    "type": "object",
                    "properties": {
                        "model_path": {
                            "type": "string",
                            "format": "local-path",
                            "default": "models/default.bin",
                        }
                    },
                    "additionalProperties": False,
                },
            )

        async def check(self):
            return None

        async def recognize(self, image, *, language="eng"):
            del image, language
            return None

        async def close(self) -> None:
            return None

    registry = PipelineRegistry()
    registry.register("ocr_engines", "versioned", VersionedOcr)
    spec = PipelineSpec.from_dict(
        {
            "name": "provider-version",
            "source": {
                "plugin": "local_documents",
                "config": {
                    "paths": ["documents"],
                    "ocr_mode": "auto",
                    "ocr_engine": {"plugin": "versioned", "config": {}},
                },
            },
            "destination": {"plugin": "sqlite", "config": {}},
        },
        origin=tmp_path / "pipeline.toml",
    )

    first = plan_ingestion_pipeline(spec, registry=registry)
    VersionedOcr.manifest_version = "2"
    second = plan_ingestion_pipeline(spec, registry=registry)

    extension = first.extensions[0]
    assert extension.version == "1"
    assert extension.descriptor is not None
    assert extension.descriptor["version"] == "engine-2"
    assert extension.config["model_path"] == str((tmp_path / "models/default.bin").resolve())
    assert first.processing_fingerprint != second.processing_fingerprint


def test_implicit_document_providers_are_canonical_replay_inputs(tmp_path) -> None:
    base = {
        "name": "implicit-providers",
        "source": {
            "plugin": "local_documents",
            "config": {"paths": ["documents"], "ocr_mode": "auto"},
        },
        "destination": {"plugin": "sqlite", "config": {}},
    }
    implicit = PipelineSpec.from_dict(base, origin=tmp_path / "pipeline.toml")
    explicit = PipelineSpec.from_dict(
        {
            **base,
            "source": {
                "plugin": "local_documents",
                "config": {
                    "paths": ["documents"],
                    "ocr_mode": "auto",
                    "ocr_engine": {"plugin": "tesseract", "config": {}},
                    "page_renderer": {"plugin": "pdfium", "config": {}},
                },
            },
        },
        origin=tmp_path / "pipeline.toml",
    )

    implicit_plan = plan_ingestion_pipeline(implicit)
    explicit_plan = plan_ingestion_pipeline(explicit)

    assert [(item.plugin, item.version) for item in implicit_plan.extensions] == [
        ("tesseract", "1"),
        ("pdfium", "3"),
    ]
    assert all(item.implicit for item in implicit_plan.extensions)
    assert not any(item.implicit for item in explicit_plan.extensions)
    assert implicit_plan.processing_fingerprint == explicit_plan.processing_fingerprint
    assert implicit_plan.materialization_fingerprint == explicit_plan.materialization_fingerprint


def test_prepared_plan_rejects_a_different_registry_implementation(tmp_path) -> None:
    class OcrV1(_CallableOcr):
        @classmethod
        def manifest(cls) -> ProviderSpec:
            return ProviderSpec(
                "swappable",
                "1",
                {
                    "type": "object",
                    "properties": {"model": {"type": "string", "default": "local"}},
                    "additionalProperties": False,
                },
            )

    class OcrV2(OcrV1):
        @classmethod
        def manifest(cls) -> ProviderSpec:
            manifest = super().manifest()
            return ProviderSpec(manifest.name, "2", manifest.config_schema)

    first_registry = PipelineRegistry()
    first_registry.register("ocr_engines", "swappable", OcrV1)
    second_registry = PipelineRegistry()
    second_registry.register("ocr_engines", "swappable", OcrV2)
    spec = PipelineSpec.from_dict(
        {
            "name": "registry-bound",
            "source": {
                "plugin": "local_documents",
                "config": {
                    "paths": [str(tmp_path)],
                    "ocr_mode": "auto",
                    "ocr_engine": {"plugin": "swappable", "config": {}},
                },
            },
            "destination": {"plugin": "sqlite", "config": {}},
        }
    )
    plan = plan_ingestion_pipeline(spec, registry=first_registry)

    with pytest.raises(ConfigurationError, match="active PipelineRegistry"):
        create_ingestion_pipeline(spec, registry=second_registry, plan=plan)


def test_future_annotated_callable_provider_is_statically_validated(tmp_path) -> None:
    registry = PipelineRegistry()
    registry.register("ocr_engines", "callable", _make_callable_ocr)
    spec = PipelineSpec.from_dict(
        {
            "name": "callable-provider",
            "source": {
                "plugin": "local_documents",
                "config": {
                    "paths": [str(tmp_path)],
                    "ocr_mode": "auto",
                    "ocr_engine": {"plugin": "callable", "config": {}},
                },
            },
            "destination": {"plugin": "sqlite", "config": {}},
        }
    )

    plan = plan_ingestion_pipeline(spec, registry=registry)
    pipeline = create_ingestion_pipeline(spec, registry=registry, plan=plan)

    assert plan.extensions[0].plugin == "callable"
    assert pipeline.source.ocr_engine.model == "local"  # type: ignore[attr-defined]


def test_nullable_and_one_of_secret_refs_share_plan_and_construction() -> None:
    class OptionalSecretTransform(Transform):
        def __init__(self, *, credential: SecretRef | None = None) -> None:
            self.credential = credential

        @classmethod
        def manifest(cls) -> ProviderSpec:
            return ProviderSpec(
                "optional-secret",
                "1",
                {
                    "type": "object",
                    "properties": {
                        "credential": {
                            "oneOf": [
                                {"type": "null"},
                                {"type": "string", "format": "secret-ref"},
                            ],
                            "default": None,
                        }
                    },
                    "additionalProperties": False,
                },
            )

        async def apply(self, records):
            return records

    registry = PipelineRegistry()
    registry.register("transforms", "optional-secret", OptionalSecretTransform)
    base = {
        "name": "optional-secret",
        "source": {"plugin": "jsonl", "config": {"path": "input.jsonl"}},
        "transforms": [{"plugin": "optional-secret", "config": {}}],
        "destination": {"plugin": "sqlite", "config": {}},
    }
    omitted = PipelineSpec.from_dict(base)
    explicit = PipelineSpec.from_dict(
        {
            **base,
            "transforms": [
                {
                    "plugin": "optional-secret",
                    "config": {"credential": "env:OPTIONAL_TOKEN"},
                }
            ],
        }
    )

    omitted_plan = plan_ingestion_pipeline(omitted, registry=registry)
    omitted_pipeline = create_ingestion_pipeline(omitted, registry=registry, plan=omitted_plan)
    explicit_plan = plan_ingestion_pipeline(explicit, registry=registry)
    explicit_pipeline = create_ingestion_pipeline(explicit, registry=registry, plan=explicit_plan)

    assert omitted_plan.transforms[0].config["credential"] is None
    assert omitted_pipeline.transforms[0].credential is None  # type: ignore[attr-defined]
    credential = explicit_pipeline.transforms[0].credential  # type: ignore[attr-defined]
    assert credential == SecretRef("OPTIONAL_TOKEN")
    assert explicit_plan.transforms[0].config["credential"] == "env:OPTIONAL_TOKEN"


def test_planning_rejects_invalid_provider_before_constructor_side_effects(tmp_path) -> None:
    constructed = 0

    class InvalidOcr:
        def __init__(self, *, model: int) -> None:
            nonlocal constructed
            constructed += 1
            self.model = model

        @classmethod
        def manifest(cls) -> ProviderSpec:
            return ProviderSpec(
                "invalid",
                "1",
                {
                    "type": "object",
                    "properties": {"model": {"type": "integer"}},
                    "required": ["model"],
                    "additionalProperties": False,
                },
            )

        async def check(self):
            return None

        async def close(self) -> None:
            return None

    registry = PipelineRegistry()
    registry.register("ocr_engines", "invalid", InvalidOcr)
    spec = PipelineSpec.from_dict(
        {
            "name": "invalid-provider",
            "source": {
                "plugin": "local_documents",
                "config": {
                    "paths": [str(tmp_path)],
                    "ocr_mode": "auto",
                    "ocr_engine": {"plugin": "invalid", "config": {"model": "large"}},
                },
            },
            "destination": {"plugin": "sqlite", "config": {}},
        }
    )

    with pytest.raises(PluginError, match="missing required methods: recognize"):
        plan_ingestion_pipeline(spec, registry=registry)
    assert constructed == 0


def test_planning_validates_provider_schema_without_constructing(tmp_path) -> None:
    constructed = 0

    class SchemaOcr:
        def __init__(self, *, model: int) -> None:
            nonlocal constructed
            constructed += 1
            self.model = model

        @classmethod
        def manifest(cls) -> ProviderSpec:
            return ProviderSpec(
                "schema-ocr",
                "1",
                {
                    "type": "object",
                    "properties": {"model": {"type": "integer"}},
                    "required": ["model"],
                    "additionalProperties": False,
                },
            )

        async def check(self):
            return None

        async def recognize(self, image, *, language="eng"):
            del image, language
            return None

        async def close(self) -> None:
            return None

    registry = PipelineRegistry()
    registry.register("ocr_engines", "schema-ocr", SchemaOcr)
    spec = PipelineSpec.from_dict(
        {
            "name": "invalid-provider-config",
            "source": {
                "plugin": "local_documents",
                "config": {
                    "paths": [str(tmp_path)],
                    "ocr_mode": "auto",
                    "ocr_engine": {
                        "plugin": "schema-ocr",
                        "config": {"model": "large"},
                    },
                },
            },
            "destination": {"plugin": "sqlite", "config": {}},
        }
    )

    with pytest.raises(ConfigurationError, match="must be integer"):
        plan_ingestion_pipeline(spec, registry=registry)
    assert constructed == 0


def test_registry_rejects_duplicate_programmatic_registration() -> None:
    registry = PipelineRegistry()
    registry.register("ocr_engines", "custom", lambda: None)

    with pytest.raises(PluginError, match="already registered"):
        registry.register("ocr_engines", "custom", lambda: None)


def test_state_store_entry_point_group_is_discoverable() -> None:
    point = EntryPoint(
        "custom",
        "example_package:CustomStateStore",
        "ingestion_graph.state_stores",
    )
    with patch("ingestion_graph.plugins.entry_points", return_value=[point]) as mocked:
        discovered = discover_plugins("state_stores")

    assert discovered == {"custom": point}
    mocked.assert_called_once_with(group="ingestion_graph.state_stores")


def test_pipeline_spec_nested_values_are_mutation_isolated() -> None:
    original = {
        "name": "immutable",
        "source": {
            "plugin": "jsonl",
            "config": {"path": "items.jsonl", "nested": {"values": [1, 2]}},
        },
        "destination": {"plugin": "sqlite", "config": {}},
        "metadata": {"owners": [{"name": "data"}]},
    }
    spec = PipelineSpec.from_dict(original)
    caller_transforms = [ComponentRef("normalize", {"rules": [{"field": "name"}]})]
    direct = PipelineSpec(
        name="direct-immutable",
        source=ComponentRef("jsonl", {"path": "items.jsonl"}),
        destination=ComponentRef("sqlite", {}),
        transforms=caller_transforms,  # type: ignore[arg-type]
    )
    fingerprint = spec.fingerprint
    original["source"]["config"]["nested"]["values"].append(3)  # type: ignore[index,union-attr]
    caller_transforms.clear()

    with pytest.raises(TypeError):
        spec.source.config["nested"]["values"][0] = 99  # type: ignore[index]
    serialized = spec.to_dict()
    serialized["metadata"]["owners"][0]["name"] = "changed"  # type: ignore[index]

    assert spec.fingerprint == fingerprint
    assert spec.source.config["nested"]["values"] == (1, 2)  # type: ignore[index]
    assert spec.metadata["owners"][0]["name"] == "data"  # type: ignore[index]
    assert len(direct.transforms) == 1
    assert direct.transforms[0].config["rules"][0]["field"] == "name"  # type: ignore[index]


def test_explicit_namespace_warning_names_all_compatibility_risks(tmp_path) -> None:
    spec = PipelineSpec.from_dict(
        {
            "name": "manual-state",
            "state_namespace": "shared",
            "source": {"plugin": "jsonl", "config": {"path": str(tmp_path / "in.jsonl")}},
            "destination": {"plugin": "sqlite", "config": {}},
        }
    )

    warning = plan_ingestion_pipeline(spec).warnings[0]

    for risk in (
        "source",
        "document extension providers",
        "transforms",
        "selected streams",
        "destination",
        "implementation versions",
        "configuration defaults",
        "resolved resource paths",
    ):
        assert risk in warning


def test_implicit_namespace_uses_the_full_materialization_digest(tmp_path) -> None:
    spec = PipelineSpec.from_dict(
        {
            "name": "full-width",
            "source": {"plugin": "jsonl", "config": {"path": str(tmp_path / "in.jsonl")}},
            "destination": {"plugin": "sqlite", "config": {}},
        }
    )

    plan = plan_ingestion_pipeline(spec)

    assert len(plan.materialization_fingerprint) == 64
    assert plan.state_namespace == f"full-width:{plan.materialization_fingerprint}"


def test_vision_provider_policy_validation_matches_local_documents(tmp_path) -> None:
    class Vision:
        descriptor = ComponentDescriptor("vision", "1")

        async def extract(self, image, *, schema):
            del image, schema
            return {}

        async def close(self) -> None:
            return None

    registry = PipelineRegistry()
    registry.register("vision_extractors", "vision", Vision)
    spec = PipelineSpec.from_dict(
        {
            "name": "vision-policy",
            "source": {
                "plugin": "local_documents",
                "config": {
                    "paths": [str(tmp_path)],
                    "vision_extractor": {"plugin": "vision", "config": {}},
                },
            },
            "destination": {"plugin": "sqlite", "config": {}},
        }
    )

    with pytest.raises(ConfigurationError, match="vision_extractor requires"):
        plan_ingestion_pipeline(spec, registry=registry)


def test_external_provider_requires_persistent_cache_before_construction(tmp_path) -> None:
    constructed = 0

    class ExternalVision:
        descriptor = ComponentDescriptor(
            "external-vision",
            "1",
            deterministic=False,
            external=True,
        )

        def __init__(self) -> None:
            nonlocal constructed
            constructed += 1

        async def extract(self, image, *, schema):
            del image, schema
            return {}

        async def close(self) -> None:
            return None

    class Policy:
        def __init__(self) -> None:
            nonlocal constructed
            constructed += 1

        async def authorize(self, *, purpose, page_number):
            del purpose, page_number
            return True

    registry = PipelineRegistry()
    registry.register("vision_extractors", "external", ExternalVision)
    registry.register("external_processing_policies", "policy", Policy)
    spec = PipelineSpec.from_dict(
        {
            "name": "external-cache",
            "source": {
                "plugin": "local_documents",
                "config": {
                    "paths": [str(tmp_path)],
                    "table_mode": "vision",
                    "vision_extractor": {"plugin": "external", "config": {}},
                    "external_processing_policy": {
                        "plugin": "policy",
                        "config": {},
                    },
                    "extraction_cache": {"plugin": "memory", "config": {}},
                },
            },
            "destination": {"plugin": "sqlite", "config": {}},
        }
    )

    with pytest.raises(ConfigurationError, match="persistent extraction cache"):
        plan_ingestion_pipeline(spec, registry=registry)
    assert constructed == 0


def test_cli_run_loads_the_path_spec_once(tmp_path, monkeypatch, capsys) -> None:
    from ingestion_graph.cli import main

    data_dir = tmp_path / "data"
    data_dir.mkdir()
    (data_dir / "items.jsonl").write_text('{"id": 1}\n', encoding="utf-8")
    spec_path = _write_toml_spec(tmp_path)
    original_load = PipelineSpec.load
    load_count = 0

    def counted_load(cls, path):
        nonlocal load_count
        load_count += 1
        return original_load(path)

    monkeypatch.setattr(PipelineSpec, "load", classmethod(counted_load))
    monkeypatch.setattr("sys.argv", ["ingestion-graph", "run", str(spec_path)])

    assert main() == 0
    assert load_count == 1
    assert json.loads(capsys.readouterr().out)["records_written"] == 1


@pytest.mark.asyncio
async def test_document_root_symlink_is_preserved_for_connector_policy(tmp_path) -> None:
    real_root = tmp_path / "real-documents"
    real_root.mkdir()
    (real_root / "note.md").write_text("# private\n", encoding="utf-8")
    linked_root = tmp_path / "linked-documents"
    try:
        linked_root.symlink_to(real_root, target_is_directory=True)
    except (NotImplementedError, OSError) as exc:
        pytest.skip(f"directory symlinks are unavailable: {exc}")

    linked_spec = PipelineSpec.from_dict(
        {
            "name": "linked-root",
            "source": {
                "plugin": "local_documents",
                "config": {"paths": ["linked-documents"]},
            },
            "destination": {"plugin": "sqlite", "config": {"path": "output.db"}},
        },
        origin=tmp_path / "pipeline.toml",
    )
    direct_spec = PipelineSpec.from_dict(
        {
            **linked_spec.to_dict(),
            "source": {
                "plugin": "local_documents",
                "config": {"paths": ["real-documents"]},
            },
        },
        origin=tmp_path / "pipeline.toml",
    )

    linked_plan = plan_ingestion_pipeline(linked_spec)
    direct_plan = plan_ingestion_pipeline(direct_spec)
    pipeline = create_ingestion_pipeline(linked_spec, plan=linked_plan)

    assert linked_plan.source.config["paths"] == (str(linked_root.absolute()),)
    assert pipeline.source.paths == (linked_root.absolute(),)  # type: ignore[attr-defined]
    assert linked_plan.processing_fingerprint != direct_plan.processing_fingerprint
    assert not (await pipeline.source.check()).ok
    await pipeline._close_resources()  # type: ignore[attr-defined]


def test_image_extensions_with_ocr_disabled_fail_during_planning(tmp_path) -> None:
    spec = PipelineSpec.from_dict(
        {
            "name": "invalid-image-routing",
            "source": {
                "plugin": "local_documents",
                "config": {
                    "paths": [str(tmp_path)],
                    "extensions": [".png"],
                    "ocr_mode": "off",
                },
            },
            "destination": {"plugin": "sqlite", "config": {}},
        }
    )

    with pytest.raises(ConfigurationError, match="Image extensions require"):
        plan_ingestion_pipeline(spec)


@pytest.mark.asyncio
async def test_construction_failure_rolls_back_async_resources_once(tmp_path, monkeypatch) -> None:
    closed: list[str] = []
    expected_loop = asyncio.get_running_loop()
    cleanup_complete = asyncio.Event()

    class TrackingSource(Source):
        def __init__(self, *, path: str) -> None:
            self.path = path

        @classmethod
        def manifest(cls) -> ConnectorSpec:
            return ConnectorSpec(
                "tracking-source",
                "1",
                {
                    "type": "object",
                    "properties": {"path": {"type": "string"}},
                    "required": ["path"],
                    "additionalProperties": False,
                },
            )

        def spec(self) -> ConnectorSpec:
            return self.manifest()

        async def check(self):
            return CheckResult(True)

        async def discover(self):
            return [StreamDescriptor("records")]

        async def read(self, stream, state=None):
            del stream, state
            if False:  # pragma: no cover - establishes the async-generator contract
                yield None

        async def close(self) -> None:
            assert asyncio.get_running_loop() is expected_loop
            closed.append("source")
            cleanup_complete.set()

    class TrackingDestination(Destination):
        idempotent = True

        @classmethod
        def manifest(cls) -> ConnectorSpec:
            return ConnectorSpec(
                "tracking-destination",
                "1",
                {"type": "object", "properties": {}, "additionalProperties": False},
                ConnectorCapabilities(),
            )

        async def check(self):
            return CheckResult(True)

        async def write(self, records):
            return len(records)

        async def flush(self) -> None:
            return None

        async def close(self) -> None:
            assert asyncio.get_running_loop() is expected_loop
            closed.append("destination")

    class TrackingStateStore(StateStore):
        @classmethod
        def manifest(cls) -> ProviderSpec:
            return ProviderSpec(
                "tracking-state",
                "1",
                {"type": "object", "properties": {}, "additionalProperties": False},
            )

        async def load(self, pipeline, source, stream):
            del pipeline, source, stream
            return {}

        async def save(self, pipeline, source, stream, state) -> None:
            del pipeline, source, stream, state

        async def close(self) -> None:
            assert asyncio.get_running_loop() is expected_loop
            closed.append("state")

    registry = PipelineRegistry()
    registry.register("sources", "tracking-source", TrackingSource)
    registry.register("destinations", "tracking-destination", TrackingDestination)
    registry.register("state_stores", "tracking-state", TrackingStateStore)
    spec = PipelineSpec.from_dict(
        {
            "name": "rollback",
            "source": {
                "plugin": "tracking-source",
                "config": {"path": str(tmp_path / "input")},
            },
            "destination": {"plugin": "tracking-destination", "config": {}},
            "state_store": {"plugin": "tracking-state", "config": {}},
        }
    )

    def fail_pipeline(*args, **kwargs):
        del args, kwargs
        raise RuntimeError("pipeline construction failed")

    monkeypatch.setattr("ingestion_graph.runtime.Pipeline", fail_pipeline)

    with pytest.raises(RuntimeError, match="pipeline construction failed"):
        create_ingestion_pipeline(spec, registry=registry)

    await asyncio.wait_for(cleanup_complete.wait(), timeout=1)
    assert closed == ["state", "destination", "source"]


@pytest.mark.asyncio
async def test_async_run_awaits_source_owned_provider_rollback_on_its_loop(
    tmp_path, monkeypatch
) -> None:
    closes = 0
    expected_loop = asyncio.get_running_loop()

    class TrackingOcr:
        descriptor = ComponentDescriptor("tracking-ocr", "1")

        async def check(self):
            return CheckResult(True)

        async def recognize(self, image, *, language="eng"):
            del image, language
            return OcrResult("text")

        async def close(self) -> None:
            nonlocal closes
            assert asyncio.get_running_loop() is expected_loop
            closes += 1

    registry = PipelineRegistry()
    registry.register("ocr_engines", "tracking-ocr", TrackingOcr)
    spec = PipelineSpec.from_dict(
        {
            "name": "provider-rollback",
            "source": {
                "plugin": "local_documents",
                "config": {
                    "paths": [str(tmp_path)],
                    "ocr_mode": "auto",
                    "ocr_engine": {"plugin": "tracking-ocr", "config": {}},
                },
            },
            "destination": {"plugin": "sqlite", "config": {}},
        }
    )

    def fail_pipeline(*args, **kwargs):
        del args, kwargs
        raise RuntimeError("pipeline construction failed")

    monkeypatch.setattr("ingestion_graph.runtime.Pipeline", fail_pipeline)

    with pytest.raises(RuntimeError, match="pipeline construction failed"):
        await run_ingestion_pipeline(spec, registry=registry)

    assert closes == 1


@pytest.mark.asyncio
async def test_prepared_plan_constructs_the_exact_factory_verified_in_replan(tmp_path) -> None:
    class FirstTransform(Transform):
        @classmethod
        def manifest(cls) -> ProviderSpec:
            return ProviderSpec(
                "mutable-transform",
                "1",
                {"type": "object", "properties": {}, "additionalProperties": False},
            )

        async def apply(self, records):
            return records

    class SecondTransform(FirstTransform):
        pass

    SecondTransform.__module__ = FirstTransform.__module__
    SecondTransform.__qualname__ = FirstTransform.__qualname__

    stable_registry = PipelineRegistry()
    stable_registry.register("transforms", "mutable-transform", FirstTransform)
    spec = PipelineSpec.from_dict(
        {
            "name": "factory-binding",
            "source": {"plugin": "jsonl", "config": {"path": str(tmp_path / "in.jsonl")}},
            "transforms": [{"plugin": "mutable-transform", "config": {}}],
            "destination": {"plugin": "sqlite", "config": {}},
        }
    )
    prepared = plan_ingestion_pipeline(spec, registry=stable_registry)

    class MutableRegistry(PipelineRegistry):
        def __init__(self) -> None:
            super().__init__()
            self.transform_lookups = 0

        def factory(self, kind: str, name: str):
            if (kind, name) == ("transforms", "mutable-transform"):
                self.transform_lookups += 1
                return FirstTransform if self.transform_lookups == 1 else SecondTransform
            return super().factory(kind, name)

    mutable_registry = MutableRegistry()
    pipeline = create_ingestion_pipeline(spec, registry=mutable_registry, plan=prepared)

    assert mutable_registry.transform_lookups == 1
    assert type(pipeline.transforms[0]) is FirstTransform
    await pipeline._close_resources()  # type: ignore[attr-defined]


def test_document_provider_requires_construction_free_descriptor(tmp_path) -> None:
    constructed = 0

    class DescriptorlessVision:
        def __init__(self) -> None:
            nonlocal constructed
            constructed += 1

        async def extract(self, image, *, schema):
            del image, schema
            return {}

        async def close(self) -> None:
            return None

    class Policy:
        async def authorize(self, *, purpose, page_number):
            del purpose, page_number
            return True

    registry = PipelineRegistry()
    registry.register("vision_extractors", "descriptorless", DescriptorlessVision)
    registry.register("external_processing_policies", "policy", Policy)
    spec = PipelineSpec.from_dict(
        {
            "name": "descriptor-required",
            "source": {
                "plugin": "local_documents",
                "config": {
                    "paths": [str(tmp_path)],
                    "table_mode": "vision",
                    "vision_extractor": {"plugin": "descriptorless", "config": {}},
                    "external_processing_policy": {"plugin": "policy", "config": {}},
                    "extraction_cache": {"plugin": "memory", "config": {}},
                },
            },
            "destination": {"plugin": "sqlite", "config": {}},
        }
    )

    with pytest.raises(PluginError, match="class-level ComponentDescriptor"):
        plan_ingestion_pipeline(spec, registry=registry)
    assert constructed == 0


def test_document_provider_protocol_methods_must_be_async(tmp_path) -> None:
    class SyncVision:
        descriptor = ComponentDescriptor("sync-vision", "1")

        def extract(self, image, *, schema):
            del image, schema
            return {}

        async def close(self) -> None:
            return None

    registry = PipelineRegistry()
    registry.register("vision_extractors", "sync-vision", SyncVision)
    spec = PipelineSpec.from_dict(
        {
            "name": "async-contract",
            "source": {
                "plugin": "local_documents",
                "config": {
                    "paths": [str(tmp_path)],
                    "vision_extractor": {"plugin": "sync-vision", "config": {}},
                },
            },
            "destination": {"plugin": "sqlite", "config": {}},
        }
    )

    with pytest.raises(PluginError, match="must implement async methods: extract"):
        plan_ingestion_pipeline(spec, registry=registry)


def test_nested_unsupported_manifest_schema_type_fails_closed(tmp_path) -> None:
    class InvalidSchemaTransform(Transform):
        @classmethod
        def manifest(cls) -> ProviderSpec:
            return ProviderSpec(
                "invalid-schema",
                "1",
                {
                    "type": "object",
                    "properties": {"unused": {"type": "bogus"}},
                    "additionalProperties": False,
                },
            )

        async def apply(self, records):
            return records

    registry = PipelineRegistry()
    registry.register("transforms", "invalid-schema", InvalidSchemaTransform)
    spec = PipelineSpec.from_dict(
        {
            "name": "invalid-schema",
            "source": {"plugin": "jsonl", "config": {"path": str(tmp_path / "in.jsonl")}},
            "transforms": [{"plugin": "invalid-schema", "config": {}}],
            "destination": {"plugin": "sqlite", "config": {}},
        }
    )

    with pytest.raises(PluginError, match="unsupported values: 'bogus'"):
        plan_ingestion_pipeline(spec, registry=registry)


@pytest.mark.parametrize("use_mapping", [False, True], ids=["spec", "mapping"])
@pytest.mark.parametrize(
    ("invalid_config", "message"),
    [
        ({"extensions": []}, "Document extensions must not be empty"),
        ({"ocr_languages": []}, "Document ocr_languages must contain valid language codes"),
        (
            {"ocr_languages": ["eng!"]},
            "Document ocr_languages must contain valid language codes",
        ),
        (
            {"paths": ["one", "two"], "stream_names": ["only-one"]},
            "Document stream_names must name every configured path",
        ),
        (
            {"stream_names": [" "]},
            "Document stream_names must name every configured path",
        ),
        (
            {"paths": ["one", "two"], "stream_names": ["same", " same "]},
            "Document stream_names must be unique",
        ),
    ],
)
def test_document_constructor_invariants_fail_during_planning_without_construction(
    tmp_path,
    use_mapping,
    invalid_config,
    message,
) -> None:
    constructed = 0

    class TrackingDocumentsSource(LocalDocumentsSource):
        def __init__(self, *args, **kwargs) -> None:
            nonlocal constructed
            constructed += 1
            super().__init__(*args, **kwargs)

    registry = PipelineRegistry()
    registry.register("sources", "local_documents", TrackingDocumentsSource)
    definition = {
        "name": "invalid-document-config",
        "source": {
            "plugin": "local_documents",
            "config": {"paths": [str(tmp_path)], **invalid_config},
        },
        "destination": {"plugin": "sqlite", "config": {}},
    }
    value = definition if use_mapping else PipelineSpec.from_dict(definition)

    with pytest.raises(ConfigurationError, match=message):
        plan_ingestion_pipeline(value, registry=registry)
    assert constructed == 0
