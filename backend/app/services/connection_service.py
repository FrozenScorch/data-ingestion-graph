"""
Connection service: CRUD operations for database connections.

Manages saved connections that nodes can reference by connection_id.
Supports testing connections before saving.
"""

import logging
import uuid
from collections.abc import Callable
from contextlib import suppress
from typing import Any

from app.connection_catalog import SUPPORTED_CONNECTION_TYPES, validate_connection_config
from app.models.graph import Connection
from app.services.connection_crypto import (
    encrypt_connection_config,
    is_encrypted_connection_config,
)
from app.services.egress_policy import (
    EgressPolicy,
    EgressPolicyError,
    ValidatedTarget,
    create_pinned_http_client,
)
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)

__all__ = ["SUPPORTED_CONNECTION_TYPES"]


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
    config: dict | None = None,
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
) -> Connection | None:
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
    type: str | None = None,
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
    name: str | None = None,
    config: dict | None = None,
) -> Connection | None:
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
    *,
    egress_policy: EgressPolicy | None = None,
    http_client_factory: Callable[[ValidatedTarget, float], Any] = create_pinned_http_client,
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
    try:
        policy = egress_policy or EgressPolicy.from_settings()
    except EgressPolicyError as exc:
        return {"success": False, "message": str(exc)}

    if type == "postgres":
        return await _test_postgres_connection(config, policy)
    if type == "discord":
        return await _test_discord_connection(config, policy, http_client_factory)

    return {"success": False, "message": f"No test implemented for type: {type}"}


async def _test_postgres_connection(config: dict, policy: EgressPolicy) -> dict:
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

    try:
        target = await policy.validate_host(host, port)
    except EgressPolicyError as exc:
        return {"success": False, "message": str(exc)}

    conn = None
    try:
        pinned_hosts = tuple(str(address) for address in target.addresses)
        conn = await asyncpg.connect(
            host=pinned_hosts[0] if len(pinned_hosts) == 1 else pinned_hosts,
            port=target.port,
            database=database,
            user=username,
            password=password,
            timeout=10,
        )
        await conn.fetchval("SELECT 1")
        return {
            "success": True,
            "message": "PostgreSQL connection successful",
        }
    except Exception as exc:
        return {
            "success": False,
            "message": f"PostgreSQL connection failed ({type(exc).__name__})",
        }
    finally:
        if conn is not None:
            with suppress(Exception):
                await conn.close()


async def _test_discord_connection(
    config: dict,
    policy: EgressPolicy,
    client_factory: Callable[[ValidatedTarget, float], Any],
) -> dict:
    """Validate an encrypted Discord bot-token connection."""
    token = config.get("bot_token") or config.get("token")
    if not token:
        return {"success": False, "message": "Discord bot token is required"}
    try:
        target = await policy.validate_url("https://discord.com/api/v10/users/@me")
    except EgressPolicyError as exc:
        return {"success": False, "message": str(exc)}
    try:
        async with client_factory(target, 10.0) as client:
            response = await client.get(
                target.url,
                headers={"Authorization": f"Bot {token}"},
                follow_redirects=False,
            )
        if response.status_code == 200:
            return {"success": True, "message": "Discord connection successful"}
        if response.status_code in {401, 403}:
            return {"success": False, "message": "Discord credentials are invalid"}
        return {"success": False, "message": f"Discord returned HTTP {response.status_code}"}
    except Exception as exc:
        return {
            "success": False,
            "message": f"Discord connection failed ({type(exc).__name__})",
        }
