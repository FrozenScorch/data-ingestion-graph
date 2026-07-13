"""Re-encrypt saved connection secrets when leaving recognized public legacy keys."""

from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import logging
import os
from typing import Any

import asyncpg
from cryptography.fernet import Fernet, InvalidToken

logger = logging.getLogger(__name__)
_ENCRYPTED_KEY = "$encrypted"
_LEGACY_KEYS = (
    "change-this-connection-encryption-key",
    "replace-with-generated-value-at-least-32-characters",
)


def _fernet(material: str) -> Fernet:
    key = base64.urlsafe_b64encode(hashlib.sha256(material.encode("utf-8")).digest())
    return Fernet(key)


def reencrypt_config(config: dict[str, Any], current_key: str) -> tuple[dict[str, Any], bool]:
    token = config.get(_ENCRYPTED_KEY)
    if token is None:
        return config, False
    encoded = str(token).encode("ascii")
    try:
        _fernet(current_key).decrypt(encoded)
        return config, False
    except InvalidToken:
        pass

    for legacy_key in _LEGACY_KEYS:
        try:
            plaintext = _fernet(legacy_key).decrypt(encoded)
        except InvalidToken:
            continue
        return {_ENCRYPTED_KEY: _fernet(current_key).encrypt(plaintext).decode("ascii")}, True
    raise RuntimeError("Saved connection credentials use an unknown encryption key")


async def migrate_connection_keys() -> int:
    connection = await asyncpg.connect(
        host=os.getenv("POSTGRES_HOST", "ingestion-postgres"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        user=os.getenv("POSTGRES_USER", "ingestion"),
        password=os.environ["POSTGRES_PASSWORD"],
        database=os.getenv("POSTGRES_DB", "ingestion_db"),
    )
    migrated = 0
    try:
        async with connection.transaction():
            rows = await connection.fetch("SELECT id, config FROM connections FOR UPDATE")
            for row in rows:
                raw_config = row["config"]
                config = (
                    json.loads(raw_config)
                    if isinstance(raw_config, str)
                    else dict(raw_config or {})
                )
                updated, changed = reencrypt_config(
                    config,
                    os.environ["CONNECTION_ENCRYPTION_KEY"],
                )
                if changed:
                    await connection.execute(
                        "UPDATE connections SET config = $1::jsonb WHERE id = $2",
                        json.dumps(updated),
                        row["id"],
                    )
                    migrated += 1
    finally:
        await connection.close()
    logger.info("Re-encrypted %d saved connection configuration(s)", migrated)
    return migrated


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    asyncio.run(migrate_connection_keys())


if __name__ == "__main__":
    main()
