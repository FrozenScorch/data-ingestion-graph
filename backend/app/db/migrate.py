"""Production schema gate for legacy create-all and Alembic-managed databases."""

from __future__ import annotations

import asyncio
import logging
from enum import StrEnum
from pathlib import Path

from alembic import command
from alembic.config import Config
from app.db.session import engine, init_db
from sqlalchemy import inspect

logger = logging.getLogger(__name__)


class SchemaState(StrEnum):
    FRESH = "fresh"
    LEGACY = "legacy"
    VERSIONED = "versioned"


async def prepare_schema() -> SchemaState:
    """Classify the database and materialize only a completely fresh schema."""
    async with engine.connect() as connection:
        versioned, legacy = await connection.run_sync(
            lambda sync_connection: (
                inspect(sync_connection).has_table("alembic_version"),
                inspect(sync_connection).has_table("graphs"),
            )
        )
    if versioned:
        return SchemaState.VERSIONED
    if legacy:
        return SchemaState.LEGACY

    # Alembic has no migration that creates the original application tables. A fresh
    # database is materialized from current metadata and stamped; an existing legacy
    # schema must instead run every idempotent migration from the pre-0001 baseline.
    await init_db()
    return SchemaState.FRESH


def alembic_config() -> Config:
    backend_root = Path(__file__).resolve().parents[2]
    config = Config(str(backend_root / "alembic.ini"))
    config.set_main_option("script_location", str(backend_root / "alembic"))
    return config


def migrate() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    schema_state = asyncio.run(prepare_schema())
    config = alembic_config()
    if schema_state is SchemaState.FRESH:
        command.stamp(config, "head")
        logger.info("Bootstrapped a fresh schema and stamped the Alembic head")
    else:
        if schema_state is SchemaState.LEGACY:
            command.stamp(config, "base")
            logger.info("Based an unversioned legacy schema before migration 0001")
        command.upgrade(config, "head")
        logger.info("Applied pending Alembic migrations")


if __name__ == "__main__":
    migrate()
