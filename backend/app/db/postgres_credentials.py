"""Transition the exact legacy Compose database password before migrations."""

from __future__ import annotations

import asyncio
import logging
import os

import asyncpg

logger = logging.getLogger(__name__)


async def _connect(password: str) -> asyncpg.Connection:
    return await asyncpg.connect(
        host=os.getenv("POSTGRES_HOST", "ingestion-postgres"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        user=os.getenv("POSTGRES_USER", "ingestion"),
        password=password,
        database=os.getenv("POSTGRES_DB", "ingestion_db"),
    )


async def ensure_current_password() -> bool:
    """Return True only when the exact legacy password had to be replaced."""
    current_password = os.environ["POSTGRES_PASSWORD"]
    try:
        connection = await _connect(current_password)
    except asyncpg.InvalidPasswordError:
        legacy_password = os.getenv("LEGACY_POSTGRES_PASSWORD", "ingestion_password")
        connection = await _connect(legacy_password)
        try:
            quoted_password = await connection.fetchval(
                "SELECT quote_literal($1)", current_password
            )
            await connection.execute(f"ALTER ROLE ingestion PASSWORD {quoted_password}")
        finally:
            await connection.close()
        logger.info("Transitioned the legacy Studio PostgreSQL role password")
        return True
    else:
        await connection.close()
        logger.info("Studio PostgreSQL role already uses the generated password")
        return False


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    asyncio.run(ensure_current_password())


if __name__ == "__main__":
    main()
