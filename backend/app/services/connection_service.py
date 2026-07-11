"""
Connection service: CRUD operations for database connections.

Manages saved connections that nodes can reference by connection_id.
Supports testing connections before saving.
"""

import logging
import uuid
from typing import Optional

from sqlalchemy import select, delete
from sqlalchemy.ext.asyncio import AsyncSession

from app.connection_catalog import SUPPORTED_CONNECTION_TYPES, validate_connection_config
from app.models.graph import Connection
from app.services.connection_crypto import (
    decrypt_connection_config,
    encrypt_connection_config,
    is_encrypted_connection_config,
)

logger = logging.getLogger(__name__)

async def migrate_plaintext_connection_configs(db: AsyncSession) -> int:
    """Encrypt legacy plaintext connection rows after the schema is available."""
    result = await db.execute(select(Connection))
    migrated = 0
    for connection in result.scalars().all():
        if not is_encrypted_connection_config(connection.config):
            connection.config = encrypt_connection_config(connection.config)
            migrated += 1
    if migrated:
        await db.commit()
    return migrated


async def create_connection(
    db: AsyncSession,
    user_id: uuid.UUID,
    name: str,
    type: str,
    config: Optional[dict] = None,
) -> Connection:
    """
    Create a new connection for a user.

    Args:
        db: Async database session
        user_id: Owner user ID
        name: Connection name
        type: Connection type (e.g., "postgres")
        config: Connection configuration dict (host, port, database, username, password)

    Returns:
        Created Connection model instance
    """
    validate_connection_config(type, config)

    connection = Connection(
        user_id=user_id,
        name=name,
        type=type,
        config=encrypt_connection_config(config),
        is_valid=False,  # Will be set True after successful test
    )
    db.add(connection)
    await db.commit()
    await db.refresh(connection)
    return connection


async def get_connection(
    db: AsyncSession,
    connection_id: uuid.UUID,
) -> Optional[Connection]:
    """
    Get a connection by ID.

    Args:
        db: Async database session
        connection_id: Connection UUID

    Returns:
        Connection model instance or None
    """
    result = await db.execute(select(Connection).where(Connection.id == connection_id))
    return result.scalar_one_or_none()


async def get_connections(
    db: AsyncSession,
    user_id: uuid.UUID,
    type: Optional[str] = None,
) -> list[Connection]:
    """
    List all connections for a user, optionally filtered by type.

    Args:
        db: Async database session
        user_id: Owner user ID
        type: Optional connection type filter

    Returns:
        List of Connection model instances
    """
    query = select(Connection).where(Connection.user_id == user_id)
    if type is not None:
        query = query.where(Connection.type == type)
    query = query.order_by(Connection.created_at.desc())
    result = await db.execute(query)
    return list(result.scalars().all())


async def update_connection(
    db: AsyncSession,
    connection_id: uuid.UUID,
    user_id: uuid.UUID,
    name: Optional[str] = None,
    config: Optional[dict] = None,
) -> Optional[Connection]:
    """
    Update a connection.

    Args:
        db: Async database session
        connection_id: Connection UUID
        user_id: Owner user ID (for authorization)
        name: New name (optional)
        config: New config dict (optional)

    Returns:
        Updated Connection model instance or None
    """
    connection = await get_connection(db, connection_id)
    if connection is None:
        return None
    if connection.user_id != user_id:
        return None

    if name is not None:
        connection.name = name
    if config is not None:
        validate_connection_config(connection.type, config)
        connection.config = encrypt_connection_config(config)
        # Reset is_valid since config changed; user should re-test
        connection.is_valid = False

    await db.commit()
    await db.refresh(connection)
    return connection


async def delete_connection(
    db: AsyncSession,
    connection_id: uuid.UUID,
    user_id: uuid.UUID,
) -> bool:
    """
    Delete a connection.

    Args:
        db: Async database session
        connection_id: Connection UUID
        user_id: Owner user ID (for authorization)

    Returns:
        True if deleted, False if not found or not authorized
    """
    connection = await get_connection(db, connection_id)
    if connection is None:
        return False
    if connection.user_id != user_id:
        return False

    await db.delete(connection)
    await db.commit()
    return True


async def test_connection(
    config: dict,
    type: str,
) -> dict:
    """
    Test if a connection works.

    Args:
        config: Connection configuration dict
        type: Connection type

    Returns:
        Dict with "success" bool and "message" string

    Raises:
        ValueError: For unsupported connection types
    """
    validate_connection_config(type, config)

    if type == "postgres":
        return await _test_postgres_connection(config)
    if type == "discord":
        return await _test_discord_connection(config)

    return {"success": False, "message": f"No test implemented for type: {type}"}


async def _test_postgres_connection(config: dict) -> dict:
    """
    Test a PostgreSQL connection by connecting and running SELECT 1.

    Args:
        config: Connection config with host, port, database, username, password

    Returns:
        Dict with "success" bool and "message" string
    """
    import asyncpg

    host = config.get("host", "localhost")
    port = config.get("port", 5432)
    database = config.get("database", config.get("dbname", "postgres"))
    username = config.get("username", config.get("user", "postgres"))
    password = config.get("password", "")

    conn = None
    try:
        conn = await asyncpg.connect(
            host=host,
            port=port,
            database=database,
            user=username,
            password=password,
            timeout=10,
        )
        result = await conn.fetchval("SELECT 1")
        return {
            "success": True,
            "message": f"Connection successful to {host}:{port}/{database}",
        }
    except Exception as e:
        return {
            "success": False,
            "message": f"Connection failed: {e}",
        }
    finally:
        if conn is not None:
            await conn.close()


async def _test_discord_connection(config: dict) -> dict:
    """Validate an encrypted Discord bot-token connection."""
    import httpx

    token = config.get("bot_token") or config.get("token")
    if not token:
        return {"success": False, "message": "Discord bot token is required"}
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.get(
                "https://discord.com/api/v10/users/@me",
                headers={"Authorization": f"Bot {token}"},
            )
        if response.status_code == 200:
            return {"success": True, "message": "Discord connection successful"}
        if response.status_code in {401, 403}:
            return {"success": False, "message": "Discord credentials are invalid"}
        return {"success": False, "message": f"Discord returned HTTP {response.status_code}"}
    except httpx.HTTPError as exc:
        return {
            "success": False,
            "message": f"Discord connection failed: {type(exc).__name__}",
        }
