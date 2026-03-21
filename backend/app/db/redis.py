"""
Redis connection management.
"""
import redis.asyncio as redis

from app.config import settings

redis_client: redis.Redis | None = None


def _build_redis_url() -> str:
    """
    Build Redis URL, ensuring password is included if configured.
    If the redis_url already contains a password (via ://:password@),
    use it as-is. Otherwise, append the password from redis_password setting.
    """
    url = settings.redis_url
    # If URL already has a password (://:<password>@), use as-is
    if "://:" in url:
        return url
    # If redis_password is set but not in URL, inject it
    if settings.redis_password:
        # redis://host:port/db -> redis://:password@host:port/db
        from urllib.parse import urlparse, urlunparse
        parsed = urlparse(url)
        netloc = f":{settings.redis_password}@{parsed.hostname or 'localhost'}"
        if parsed.port:
            netloc += f":{parsed.port}"
        parsed = parsed._replace(netloc=netloc)
        return urlunparse(parsed)
    return url


async def init_redis() -> redis.Redis:
    """
    Initialize and return Redis connection.
    Called during app startup lifespan.
    """
    global redis_client
    redis_client = redis.from_url(
        _build_redis_url(),
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
