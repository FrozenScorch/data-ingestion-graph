"""
Redis connection management.
"""
import redis.asyncio as redis

from app.config import settings

redis_client: redis.Redis | None = None


async def init_redis() -> redis.Redis:
    """
    Initialize and return Redis connection.
    Called during app startup lifespan.
    """
    global redis_client
    redis_client = redis.from_url(
        settings.redis_url,
        encoding="utf-8",
        decode_responses=True,
    )
    return redis_client


async def close_redis() -> None:
    """Close Redis connection. Called during app shutdown."""
    global redis_client
    if redis_client:
        await redis_client.close()
        redis_client = None


def get_redis() -> redis.Redis:
    """Get the current Redis client. Raises if not initialized."""
    if redis_client is None:
        raise RuntimeError("Redis not initialized. Call init_redis() first.")
    return redis_client
