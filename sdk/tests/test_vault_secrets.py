from __future__ import annotations

import json
import traceback
from dataclasses import asdict
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from ingestion_graph import (
    SecretRef,
    SecretValue,
    VaultSecretBinding,
    VaultSecretProvider,
    inspect_secret_redaction,
)
from ingestion_graph.errors import ConfigurationError
from ingestion_graph.postgres import PostgresConnection


def kv_response(value: object) -> dict:
    return {"data": {"data": {"password": value}, "metadata": {"version": 2}}}


def provider(reader, **binding_options):
    return VaultSecretProvider(
        reader,
        {
            "database-password": VaultSecretBinding(
                "team/project/database", "password", **binding_options
            )
        },
    )


def test_resolves_only_bound_field_and_keeps_reference_serializable():
    reader = Mock(return_value={"data": {"data": {"password": "private-value", "other": "hidden"}}})
    secrets = provider(reader, mount_point="team/kv", version=3)
    reference = SecretRef("database-password", provider="vault")

    value = secrets.resolve(reference)

    assert isinstance(value, SecretValue)
    assert str(value) == "private-value"
    reader.assert_called_once_with(
        path="team/project/database",
        mount_point="team/kv",
        version=3,
        raise_on_deleted_version=True,
    )
    assert json.loads(json.dumps(asdict(reference))) == {
        "key": "database-password",
        "provider": "vault",
    }
    inspect_secret_redaction(
        {"password": "private-value", "other": "hidden"},
        [repr(value), repr(secrets), json.dumps(asdict(reference))],
    ).raise_for_errors()


@pytest.mark.parametrize(
    "reference",
    [
        SecretRef("database-password"),
        SecretRef("other-project", provider="vault"),
        SecretRef("team/project/database", provider="vault"),
        SecretRef("../other-project/password", provider="vault"),
        SecretRef("", provider="vault"),
    ],
)
def test_unknown_references_fail_before_accessing_vault(reference):
    reader = Mock()
    with pytest.raises(ConfigurationError, match="not configured"):
        provider(reader).resolve(reference)
    reader.assert_not_called()


def test_binding_maps_are_snapshots_and_clients_are_not_shared():
    bindings = {"password": VaultSecretBinding("tenant-a/database", "password")}
    reader_a = Mock(return_value=kv_response("value-a"))
    reader_b = Mock(return_value=kv_response("value-b"))
    secrets_a = VaultSecretProvider(reader_a, bindings)
    bindings["password"] = VaultSecretBinding("tenant-b/database", "password")
    bindings["other"] = VaultSecretBinding("tenant-b/admin", "password")
    secrets_b = VaultSecretProvider(reader_b, bindings)
    reference = SecretRef("password", provider="vault")

    assert str(secrets_a.resolve(reference)) == "value-a"
    assert str(secrets_b.resolve(reference)) == "value-b"
    assert reader_a.call_args.kwargs["path"] == "tenant-a/database"
    assert reader_b.call_args.kwargs["path"] == "tenant-b/database"
    with pytest.raises(ConfigurationError):
        secrets_a.resolve(SecretRef("other", provider="vault"))
    assert reader_a.call_count == 1


def test_latest_value_is_refetched_and_failure_never_returns_cached_secret(caplog):
    reader = Mock(
        side_effect=[
            kv_response("first-value"),
            kv_response("rotated-value"),
            RuntimeError("403: token=private-token; password=rotated-value"),
        ]
    )
    secrets = provider(reader)
    reference = SecretRef("database-password", provider="vault")
    assert str(secrets.resolve(reference)) == "first-value"
    assert str(secrets.resolve(reference)) == "rotated-value"

    with pytest.raises(ConfigurationError) as caught:
        secrets.resolve(reference)

    assert caught.value.__context__ is None
    assert caught.value.__cause__ is None
    assert reader.call_count == 3
    assert all(call.kwargs["version"] is None for call in reader.call_args_list)
    inspect_secret_redaction(
        {"token": "private-token", "password": "rotated-value"},
        [
            str(caught.value),
            repr(caught.value),
            "".join(traceback.format_exception(caught.value)),
            caplog.text,
        ],
    ).raise_for_errors()


@pytest.mark.parametrize(
    "response",
    [
        None,
        [],
        {},
        {"data": None},
        {"data": []},
        {"data": {"data": None}},
        {"data": {"data": {"other": "private-value"}}},
        # KV v1, empty and non-string values are deliberately unsupported.
        {"data": {"password": "private-value"}},
        kv_response(""),
        kv_response(None),
        kv_response(123),
        kv_response(False),
        kv_response({"password": "private-value"}),
        kv_response(["private-value"]),
    ],
)
def test_missing_deleted_or_malformed_values_fail_closed(response):
    with pytest.raises(ConfigurationError, match="could not be resolved") as caught:
        provider(Mock(return_value=response)).resolve(
            SecretRef("database-password", provider="vault")
        )
    assert "private-value" not in str(caught.value)


@pytest.mark.parametrize(
    "path",
    [
        "",
        "/root",
        "root/",
        "root//key",
        ".",
        "a/../b",
        "a/./b",
        "a\\b",
        "a?x=1",
        "a#b",
        "https://vault.example/secret",
        "a/%2e%2e/b",
        "a\nb",
        "a b",
    ],
)
@pytest.mark.parametrize("argument", ["path", "mount_point"])
def test_binding_rejects_url_or_traversal_paths(path, argument):
    options = {"path": "team/database", "field": "password", "mount_point": "secret"}
    options[argument] = path
    with pytest.raises(ConfigurationError):
        VaultSecretBinding(**options)


@pytest.mark.parametrize("version", [0, -1, True, 1.5, "1"])
def test_binding_requires_positive_integer_version(version):
    with pytest.raises(ConfigurationError):
        VaultSecretBinding("database", "password", version=version)


@pytest.mark.parametrize("field", ["", None, 1])
def test_binding_requires_named_field(field):
    with pytest.raises(ConfigurationError):
        VaultSecretBinding("database", field)


@pytest.mark.parametrize(
    "bindings", [{"": VaultSecretBinding("db", "password")}, {"key": "raw-value"}, None]
)
def test_provider_rejects_invalid_binding_maps(bindings):
    with pytest.raises(ConfigurationError):
        VaultSecretProvider(Mock(), bindings)


def test_empty_bindings_grant_no_access():
    reader = Mock()
    secrets = VaultSecretProvider(reader, {})
    with pytest.raises(ConfigurationError):
        secrets.resolve(SecretRef("database-password", provider="vault"))
    reader.assert_not_called()


def test_hvac_reader_compatibility_without_network(monkeypatch):
    hvac = pytest.importorskip("hvac")
    client = hvac.Client(url="https://vault.example.invalid", token="synthetic-test-token")
    get = Mock(return_value=kv_response("database-secret"))
    monkeypatch.setattr(client.adapter, "get", get)
    secrets = provider(client.secrets.kv.v2.read_secret_version, mount_point="kv", version=3)

    assert (
        str(secrets.resolve(SecretRef("database-password", provider="vault"))) == "database-secret"
    )
    get.assert_called_once_with(url="/v1/kv/data/team/project/database", params={"version": 3})


@pytest.mark.asyncio
async def test_existing_postgres_connection_uses_vault_provider(monkeypatch):
    connect = AsyncMock(return_value=object())
    monkeypatch.setattr("ingestion_graph.postgres.asyncpg", SimpleNamespace(connect=connect))
    reader = Mock(return_value=kv_response("database-secret"))
    connection = PostgresConnection.create(
        "db.example",
        5432,
        "project",
        "project_reader",
        SecretRef("database-password", provider="vault"),
        secret_provider=provider(reader),
    )

    assert await connection.connect() is connect.return_value
    assert connect.call_args.kwargs["password"] == "database-secret"
    assert "database-secret" not in repr(connection)


@pytest.mark.asyncio
async def test_vault_failure_prevents_database_connection(monkeypatch):
    connect = AsyncMock()
    monkeypatch.setattr("ingestion_graph.postgres.asyncpg", SimpleNamespace(connect=connect))
    connection = PostgresConnection.create(
        "db.example",
        5432,
        "project",
        "project_reader",
        SecretRef("database-password", provider="vault"),
        secret_provider=provider(Mock(side_effect=RuntimeError("revoked"))),
    )
    with pytest.raises(ConfigurationError):
        await connection.connect()
    connect.assert_not_called()
