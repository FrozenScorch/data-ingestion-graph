"""Constructor-free connector manifest contract tests."""

from collections.abc import AsyncIterator, Mapping, Sequence
from importlib.metadata import EntryPoint, PackageNotFoundError, version
from unittest.mock import patch

import pytest

import ingestion_graph
from ingestion_graph.connectors.base import (
    CheckResult,
    ConnectorCapabilities,
    ConnectorSpec,
    Destination,
    Source,
    StreamDescriptor,
)
from ingestion_graph.destinations import JsonlDestination, SQLiteCollection
from ingestion_graph.errors import PluginError
from ingestion_graph.messages import SourceMessage
from ingestion_graph.plugins import discover_plugins, load_connector_manifest
from ingestion_graph.sources import DiscordSource, JsonlSource, LocalDocumentsSource


@pytest.mark.parametrize(
    ("source_type", "name"),
    [
        (DiscordSource, "discord"),
        (JsonlSource, "jsonl"),
        (LocalDocumentsSource, "local_documents"),
    ],
)
def test_builtin_source_manifests_need_no_runtime_configuration(source_type, name):
    manifest = source_type.manifest()

    assert manifest.name == name
    assert manifest.version
    assert manifest.config_schema["type"] == "object"
    assert manifest.capabilities.incremental is True


@pytest.mark.parametrize(
    ("destination_type", "name"),
    [(JsonlDestination, "jsonl"), (SQLiteCollection, "sqlite")],
)
def test_builtin_destination_manifests_need_no_runtime_configuration(destination_type, name):
    manifest = destination_type.manifest()

    assert manifest.name == name
    assert manifest.version
    assert manifest.config_schema["type"] == "object"
    assert manifest.capabilities.incremental is True
    assert manifest.capabilities.deletes is True


def test_builtin_destination_specs_match_manifests(tmp_path):
    assert JsonlDestination(tmp_path / "items.jsonl").spec() == JsonlDestination.manifest()
    assert SQLiteCollection(tmp_path / "query.db").spec() == SQLiteCollection.manifest()


def test_plugin_manifest_loader_returns_validated_connector_spec():
    with patch("ingestion_graph.plugins.load_plugin", return_value=DiscordSource):
        manifest = load_connector_manifest("sources", "discord")

    assert manifest == DiscordSource.manifest()


class _LegacySource(Source):
    def spec(self):
        return DiscordSource.manifest()

    async def check(self) -> CheckResult:
        return CheckResult(True)

    async def discover(self) -> Sequence[StreamDescriptor]:
        return []

    async def read(
        self,
        stream: StreamDescriptor,
        state: Mapping[str, object] | None = None,
    ) -> AsyncIterator[SourceMessage]:
        del stream, state
        if False:
            yield


def test_legacy_plugin_is_still_a_valid_source_but_not_silently_instantiated():
    legacy = _LegacySource()
    assert legacy.spec().name == "discord"

    with (
        patch("ingestion_graph.plugins.load_plugin", return_value=_LegacySource),
        pytest.raises(PluginError, match="does not expose manifest"),
    ):
        load_connector_manifest("sources", "legacy")


class _LegacyDestination(Destination):
    async def check(self) -> CheckResult:
        return CheckResult(True)

    async def write(self, records) -> int:
        return len(records)

    async def flush(self) -> None:
        return None


def test_legacy_destination_remains_instantiable_but_has_no_manifest():
    legacy = _LegacyDestination()
    assert isinstance(legacy, Destination)

    with (
        patch("ingestion_graph.plugins.load_plugin", return_value=_LegacyDestination),
        pytest.raises(PluginError, match="does not expose manifest"),
    ):
        load_connector_manifest("destinations", "legacy")


@pytest.mark.parametrize(
    ("kind", "expected"),
    [
        ("sources", {"discord", "jsonl", "local_documents"}),
        ("destinations", {"jsonl", "sqlite"}),
    ],
)
def test_installed_entry_points_load_matching_manifests(kind, expected):
    try:
        installed_version = version("ingestion-graph")
    except PackageNotFoundError:
        pytest.skip("installed distribution metadata is unavailable")
    if installed_version != ingestion_graph.__version__:
        pytest.skip(
            f"installed metadata is {installed_version}, source is {ingestion_graph.__version__}"
        )

    plugins = discover_plugins(kind)

    assert expected <= plugins.keys()
    for name in expected:
        assert load_connector_manifest(kind, name).name == name


def test_duplicate_entry_point_names_fail_closed():
    points = [
        EntryPoint("same", "package_one:Plugin", "ingestion_graph.destinations"),
        EntryPoint("same", "package_two:Plugin", "ingestion_graph.destinations"),
    ]
    with (
        patch("ingestion_graph.plugins.entry_points", return_value=points),
        pytest.raises(PluginError, match="Duplicate destinations.*'same'"),
    ):
        discover_plugins("destinations")


def _plugin_with_manifest(manifest: ConnectorSpec):
    class ManifestPlugin:
        @classmethod
        def manifest(cls) -> ConnectorSpec:
            return manifest

    return ManifestPlugin


@pytest.mark.parametrize(
    ("manifest", "message"),
    [
        (ConnectorSpec("example", "", {"type": "object", "properties": {}}), "version"),
        (ConnectorSpec("example", "1", {}), "object schema"),
        (
            ConnectorSpec("example", "1", {"type": "object", "properties": []}),
            "properties",
        ),
        (
            ConnectorSpec(
                "example",
                "1",
                {"type": "object", "properties": {"field": "not-a-schema"}},
            ),
            "schema objects",
        ),
        (
            ConnectorSpec(
                "example",
                "1",
                {"type": "object", "properties": {}, "required": "field"},
            ),
            "string array",
        ),
        (
            ConnectorSpec(
                "example",
                "1",
                {"type": "object", "properties": {}, "required": ["missing"]},
            ),
            "unknown fields",
        ),
        (
            ConnectorSpec(
                "example",
                "1",
                {"type": "object", "properties": {}},
                capabilities="incremental",  # type: ignore[arg-type]
            ),
            "capabilities",
        ),
    ],
)
def test_plugin_manifest_loader_rejects_malformed_specs(manifest, message):
    with (
        patch("ingestion_graph.plugins.load_plugin", return_value=_plugin_with_manifest(manifest)),
        pytest.raises(PluginError, match=message),
    ):
        load_connector_manifest("sources", "example")


def test_plugin_manifest_loader_rejects_empty_manifest_name():
    manifest = ConnectorSpec("", "1", {"type": "object", "properties": {}})
    with (
        patch("ingestion_graph.plugins.load_plugin", return_value=_plugin_with_manifest(manifest)),
        pytest.raises(PluginError, match="empty name"),
    ):
        load_connector_manifest("sources", "")


def test_plugin_manifest_loader_rejects_non_boolean_capability_flags():
    capabilities = ConnectorCapabilities(incremental="yes")  # type: ignore[arg-type]
    manifest = ConnectorSpec(
        "example",
        "1",
        {"type": "object", "properties": {}},
        capabilities,
    )
    with (
        patch("ingestion_graph.plugins.load_plugin", return_value=_plugin_with_manifest(manifest)),
        pytest.raises(PluginError, match="flags must be booleans"),
    ):
        load_connector_manifest("sources", "example")
