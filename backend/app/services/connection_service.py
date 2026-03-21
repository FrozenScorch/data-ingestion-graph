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

from app.models.graph import Connection

logger = logging.getLogger(__name__)

# Supported connection types
SUPPORTED_CONNECTION_TYPES = ["postgres"]


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
    if type not in SUPPORTED_CONNECTION_TYPES:
        raise ValueError(f"Unsupported connection type: {type}. Supported: {SUPPORTED_CONNECTION_TYPES}")

    connection = Connection(
        user_id=user_id,
        name=name,
        type=type,
        config=config or {},
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
    result = await db.execute(
        select(Connection).where(Connection.id == connection_id)
    )
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
        connection.config = config
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
    if type not in SUPPORTED_CONNECTION_TYPES:
        raise ValueError(f"Unsupported connection type: {type}. Supported: {SUPPORTED_CONNECTION_TYPES}")

    if type == "postgres":
        return await _test_postgres_connection(config)

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
