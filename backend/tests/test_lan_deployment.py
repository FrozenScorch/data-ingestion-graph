"""Local/LAN appliance configuration and migration-gate tests."""

from __future__ import annotations

import os
import subprocess
import sys
from argparse import ArgumentTypeError
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest
from app.db import migrate as migration
from app.models.base import Base
from sqlalchemy import inspect as sqlalchemy_inspect
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from scripts.init_lan_env import (
    build_environment,
    read_environment,
    validate_host,
    write_environment,
)
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


def test_reconfiguration_preserves_secrets_and_custom_settings(tmp_path):
    original = build_environment("old.home", tls=False, http_port=8040, https_port=8443)
    original["RUN_WORKER_CONCURRENCY"] = "4"
    output = tmp_path / ".env"
    write_environment(output, original, force=False)

    updated = build_environment(
        "new.home",
        tls=True,
        http_port=8080,
        https_port=9443,
        existing=read_environment(output),
    )

    for key in (
        "POSTGRES_PASSWORD",
        "REDIS_PASSWORD",
        "JWT_SECRET_KEY",
        "CONNECTION_ENCRYPTION_KEY",
        "ADMIN_PASSWORD",
    ):
        assert updated[key] == original[key]
    assert updated["RUN_WORKER_CONCURRENCY"] == "4"
    assert updated["STUDIO_ORIGIN"] == "https://new.home:9443"


@pytest.mark.parametrize("host", ["https://server", "server:8040", "server/path", ""])
def test_generator_rejects_hosts_that_could_corrupt_the_public_origin(host):
    with pytest.raises(ArgumentTypeError):
        validate_host(host)


class _Connection:
    async def run_sync(self, callback):
        return callback(MagicMock())


class _ConnectionContext:
    def __init__(self) -> None:
        self.connection = _Connection()

    async def __aenter__(self):
        return self.connection

    async def __aexit__(self, exc_type, exc, traceback):
        return False


@pytest.mark.asyncio
async def test_schema_gate_distinguishes_fresh_legacy_and_versioned_databases():
    test_engine = MagicMock()
    test_engine.connect.return_value = _ConnectionContext()

    def inspector_for(*tables):
        inspector = MagicMock()
        inspector.has_table.side_effect = lambda table: table in tables
        return inspector

    with (
        patch.object(migration, "engine", test_engine),
        patch.object(migration, "inspect", return_value=inspector_for()),
        patch.object(migration, "init_db", new_callable=AsyncMock) as init_db,
    ):
        assert await migration.prepare_schema() is migration.SchemaState.FRESH
        init_db.assert_awaited_once()

    with (
        patch.object(migration, "engine", test_engine),
        patch.object(migration, "inspect", return_value=inspector_for("graphs")),
        patch.object(migration, "init_db", new_callable=AsyncMock) as init_db,
    ):
        assert await migration.prepare_schema() is migration.SchemaState.LEGACY
        init_db.assert_not_awaited()

    with (
        patch.object(migration, "engine", test_engine),
        patch.object(migration, "inspect", return_value=inspector_for("alembic_version")),
        patch.object(migration, "init_db", new_callable=AsyncMock) as init_db,
    ):
        assert await migration.prepare_schema() is migration.SchemaState.VERSIONED
        init_db.assert_not_awaited()


def test_schema_gate_stamps_bootstrap_and_upgrades_versioned_databases():
    config = MagicMock()

    def completed_with(value):
        def run(coroutine):
            coroutine.close()
            return value

        return run

    with (
        patch.object(
            migration.asyncio,
            "run",
            side_effect=completed_with(migration.SchemaState.FRESH),
        ),
        patch.object(migration, "alembic_config", return_value=config),
        patch.object(migration.command, "stamp") as stamp,
        patch.object(migration.command, "upgrade") as upgrade,
    ):
        migration.migrate()
    stamp.assert_called_once_with(config, "head")
    upgrade.assert_not_called()

    with (
        patch.object(
            migration.asyncio,
            "run",
            side_effect=completed_with(migration.SchemaState.VERSIONED),
        ),
        patch.object(migration, "alembic_config", return_value=config),
        patch.object(migration.command, "stamp") as stamp,
        patch.object(migration.command, "upgrade") as upgrade,
    ):
        migration.migrate()
    upgrade.assert_called_once_with(config, "head")
    stamp.assert_not_called()

    with (
        patch.object(
            migration.asyncio,
            "run",
            side_effect=completed_with(migration.SchemaState.LEGACY),
        ),
        patch.object(migration, "alembic_config", return_value=config),
        patch.object(migration.command, "stamp") as stamp,
        patch.object(migration.command, "upgrade") as upgrade,
    ):
        migration.migrate()
    stamp.assert_called_once_with(config, "base")
    upgrade.assert_called_once_with(config, "head")


@pytest.mark.asyncio
@pytest.mark.skipif(
    not os.getenv("TEST_DATABASE_URL"),
    reason="TEST_DATABASE_URL is required for the legacy schema upgrade test",
)
async def test_unversioned_legacy_postgres_schema_upgrades_to_head():
    test_url = os.environ["TEST_DATABASE_URL"]
    database_name = f"ingestion_legacy_{uuid4().hex}"
    admin_url = test_url.rsplit("/", 1)[0] + "/postgres"
    database_url = test_url.rsplit("/", 1)[0] + f"/{database_name}"
    admin_engine = create_async_engine(admin_url, isolation_level="AUTOCOMMIT")
    legacy_engine = create_async_engine(database_url)

    try:
        async with admin_engine.connect() as connection:
            await connection.execute(text(f'CREATE DATABASE "{database_name}"'))
        import app.models  # noqa: F401

        async with legacy_engine.begin() as connection:
            await connection.run_sync(Base.metadata.create_all)
        await legacy_engine.dispose()

        environment = {**os.environ, "DATABASE_URL": database_url}
        result = subprocess.run(
            [sys.executable, "-m", "app.db.migrate"],
            cwd=ROOT / "backend",
            env=environment,
            capture_output=True,
            text=True,
            check=False,
        )
        assert result.returncode == 0, result.stdout + result.stderr

        verification_engine = create_async_engine(database_url)
        async with verification_engine.connect() as connection:
            revision = await connection.scalar(text("SELECT version_num FROM alembic_version"))
            schema = await connection.run_sync(
                lambda sync_connection: {
                    "indexes": {
                        index["name"]
                        for index in sqlalchemy_inspect(sync_connection).get_indexes("runs")
                    },
                    "constraints": {
                        constraint["name"]
                        for constraint in sqlalchemy_inspect(
                            sync_connection
                        ).get_unique_constraints("graph_versions")
                    },
                    "tables": set(sqlalchemy_inspect(sync_connection).get_table_names()),
                }
            )
        await verification_engine.dispose()

        assert revision == "0003_sdk_source_states"
        assert "ix_runs_graph_id_status" in schema["indexes"]
        assert "uq_graph_version" in schema["constraints"]
        assert {"run_jobs", "sdk_source_states"} <= schema["tables"]
    finally:
        await legacy_engine.dispose()
        async with admin_engine.connect() as connection:
            await connection.execute(
                text(f'DROP DATABASE IF EXISTS "{database_name}" WITH (FORCE)')
            )
        await admin_engine.dispose()


def test_repository_compose_contract_has_private_data_plane_and_edge_proxy():
    compose = (ROOT / "docker-compose.yml").read_text(encoding="utf-8")
    routes = (ROOT / "deploy/caddy/routes.caddy").read_text(encoding="utf-8")
    frontend_image = (ROOT / "frontend/Dockerfile").read_text(encoding="utf-8")

    assert "network_mode: host" not in compose
    assert not compose.startswith("name:")
    assert "profiles:" not in compose
    assert "service_completed_successfully" in compose
    assert "internal: true" in compose
    assert "caddy:2.11.4-alpine" in compose
    assert "./data/uploads:/app/data/uploads" in compose
    assert "header Origin {$STUDIO_ORIGIN}" in routes
    assert "reverse_proxy ingestion-api:8040" in routes
    assert "reverse_proxy ingestion-frontend:3000" in routes
    assert "http://127.0.0.1:3000/" in frontend_image

    alembic_environment = (ROOT / "backend/alembic/env.py").read_text(encoding="utf-8")
    assert 'set_main_option("sqlalchemy.url", settings.database_url)' in alembic_environment


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
            "depends_on": {"ingestion-migrate": {"condition": "service_completed_successfully"}},
        },
        "ingestion-frontend": {"environment": {"API_HOST": "http://ingestion-api:8040"}},
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
