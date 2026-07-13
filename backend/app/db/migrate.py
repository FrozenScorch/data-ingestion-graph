"""Production schema gate for legacy create-all and Alembic-managed databases."""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path

from alembic import command
from alembic.config import Config
from app.db.session import engine, init_db
from sqlalchemy import inspect

logger = logging.getLogger(__name__)


async def prepare_schema() -> bool:
    """Return True after bootstrapping a legacy database without Alembic state."""
    async with engine.connect() as connection:
        versioned = await connection.run_sync(
            lambda sync_connection: inspect(sync_connection).has_table("alembic_version")
        )
    if versioned:
        return False

    # Historical Studio releases used metadata.create_all and shipped no base-table
    # migration. Materialize the current model once, then establish the Alembic head;
    # all subsequent releases run ordinary ordered migrations before API startup.
    await init_db()
    return True


def alembic_config() -> Config:
    backend_root = Path(__file__).resolve().parents[2]
    config = Config(str(backend_root / "alembic.ini"))
    config.set_main_option("script_location", str(backend_root / "alembic"))
    return config


def migrate() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    bootstrapped = asyncio.run(prepare_schema())
    config = alembic_config()
    if bootstrapped:
        command.stamp(config, "head")
        logger.info("Bootstrapped current schema and stamped the Alembic head")
    else:
        command.upgrade(config, "head")
        logger.info("Applied pending Alembic migrations")


if __name__ == "__main__":
    migrate()
