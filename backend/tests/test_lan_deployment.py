"""Local/LAN appliance configuration and migration-gate tests."""

from __future__ import annotations

from argparse import ArgumentTypeError
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from app.db import migrate as migration

from scripts.init_lan_env import build_environment, validate_host, write_environment
from scripts.verify_compose import verify

ROOT = Path(__file__).resolve().parents[2]


def test_generated_environment_uses_exact_origin_and_independent_secrets():
    values = build_environment(
        "192.168.1.50",
        tls=True,
        http_port=8040,
        https_port=8443,
    )

    assert values["STUDIO_ORIGIN"] == "https://192.168.1.50:8443"
    assert values["CADDY_CONFIG"] == "Caddyfile.tls"
    secret_values = {
        values["POSTGRES_PASSWORD"],
        values["REDIS_PASSWORD"],
        values["JWT_SECRET_KEY"],
        values["CONNECTION_ENCRYPTION_KEY"],
        values["ADMIN_PASSWORD"],
    }
    assert len(secret_values) == 5
    assert min(map(len, secret_values)) >= 32


def test_environment_writer_refuses_to_replace_existing_secrets(tmp_path):
    output = tmp_path / ".env"
    write_environment(output, {"SECRET": "first"}, force=False)

    with pytest.raises(FileExistsError):
        write_environment(output, {"SECRET": "second"}, force=False)
    assert "SECRET=first" in output.read_text(encoding="utf-8")


@pytest.mark.parametrize("host", ["https://server", "server:8040", "server/path", ""])
def test_generator_rejects_hosts_that_could_corrupt_the_public_origin(host):
    with pytest.raises(ArgumentTypeError):
        validate_host(host)


class _Connection:
    def __init__(self, versioned: bool) -> None:
        self.versioned = versioned

    async def run_sync(self, callback):
        del callback
        return self.versioned


class _ConnectionContext:
    def __init__(self, versioned: bool) -> None:
        self.connection = _Connection(versioned)

    async def __aenter__(self):
        return self.connection

    async def __aexit__(self, exc_type, exc, traceback):
        return False


@pytest.mark.asyncio
async def test_schema_gate_bootstraps_only_unversioned_databases():
    unversioned_engine = MagicMock()
    unversioned_engine.connect.return_value = _ConnectionContext(False)
    with (
        patch.object(migration, "engine", unversioned_engine),
        patch.object(migration, "init_db", new_callable=AsyncMock) as init_db,
    ):
        assert await migration.prepare_schema() is True
        init_db.assert_awaited_once()

    versioned_engine = MagicMock()
    versioned_engine.connect.return_value = _ConnectionContext(True)
    with (
        patch.object(migration, "engine", versioned_engine),
        patch.object(migration, "init_db", new_callable=AsyncMock) as init_db,
    ):
        assert await migration.prepare_schema() is False
        init_db.assert_not_awaited()


def test_schema_gate_stamps_bootstrap_and_upgrades_versioned_databases():
    config = MagicMock()

    def completed_with(value):
        def run(coroutine):
            coroutine.close()
            return value

        return run

    with (
        patch.object(migration.asyncio, "run", side_effect=completed_with(True)),
        patch.object(migration, "alembic_config", return_value=config),
        patch.object(migration.command, "stamp") as stamp,
        patch.object(migration.command, "upgrade") as upgrade,
    ):
        migration.migrate()
    stamp.assert_called_once_with(config, "head")
    upgrade.assert_not_called()

    with (
        patch.object(migration.asyncio, "run", side_effect=completed_with(False)),
        patch.object(migration, "alembic_config", return_value=config),
        patch.object(migration.command, "stamp") as stamp,
        patch.object(migration.command, "upgrade") as upgrade,
    ):
        migration.migrate()
    upgrade.assert_called_once_with(config, "head")
    stamp.assert_not_called()


def test_repository_compose_contract_has_private_data_plane_and_edge_proxy():
    compose = (ROOT / "docker-compose.yml").read_text(encoding="utf-8")
    routes = (ROOT / "deploy/caddy/routes.caddy").read_text(encoding="utf-8")

    assert "network_mode: host" not in compose
    assert "profiles:" not in compose
    assert "service_completed_successfully" in compose
    assert "internal: true" in compose
    assert "caddy:2.11.4-alpine" in compose
    assert "header Origin {$STUDIO_ORIGIN}" in routes
    assert "reverse_proxy ingestion-api:8040" in routes
    assert "reverse_proxy ingestion-frontend:3000" in routes


def test_rendered_compose_verifier_accepts_logical_network_keys():
    services = {
        "ingestion-postgres": {},
        "ingestion-redis": {},
        "ingestion-migrate": {},
        "ingestion-api": {
            "environment": {
                "APP_ENV": "production",
                "APP_DEBUG": "false",
                "DATABASE_URL": "postgresql://user:secret@ingestion-postgres:5432/db",
                "REDIS_URL": "redis://:secret@ingestion-redis:6379/0",
            },
            "depends_on": {
                "ingestion-migrate": {"condition": "service_completed_successfully"}
            },
        },
        "ingestion-frontend": {
            "environment": {"API_HOST": "http://ingestion-api:8040"}
        },
        "ingestion-proxy": {"ports": [{"target": 8080, "published": "8040"}]},
    }
    rendered = {
        "services": services,
        "networks": {
            "data": {"name": "ingestion-graph_data", "internal": True},
            "edge": {"name": "ingestion-graph_edge"},
        },
    }

    assert verify(rendered) == []
