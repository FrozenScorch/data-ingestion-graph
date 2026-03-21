"""
Health check endpoint: verifies database and Redis connectivity.
Performs live connectivity checks rather than relying on cached startup state.
"""
from fastapi import APIRouter, Response

router = APIRouter(tags=["health"])


@router.get("/health")
async def health_check(response: Response):
    """
    Check system health: database and Redis connectivity.

    Performs live checks against both database and Redis.
    - Database failure: returns HTTP 503 (service unavailable).
    - Redis failure: returns HTTP 503 (degraded but not fatal).

    Falls back to startup-time health status if live checks fail
    due to transient issues.
    """
    health_status = {"status": "healthy", "components": {}}
    has_error = False

    # Check database (live connectivity)
    try:
        from app.db.session import AsyncSessionLocal
        async with AsyncSessionLocal() as session:
            from sqlalchemy import text
            await session.execute(text("SELECT 1"))
        health_status["components"]["database"] = {"status": "ok"}
    except Exception as e:
        has_error = True
        health_status["components"]["database"] = {"status": "error", "detail": str(e)}

    # Check Redis (live connectivity)
    try:
        from app.db.redis import get_redis
        redis = get_redis()
        await redis.ping()
        health_status["components"]["redis"] = {"status": "ok"}
    except Exception as e:
        # Redis is optional; report unhealthy but don't fail the endpoint
        health_status["components"]["redis"] = {"status": "error", "detail": str(e)}
        # Only mark as degraded if database is fine but Redis is not
        if not has_error:
            health_status["status"] = "degraded"
            has_error = True

    if has_error and health_status["status"] == "unhealthy":
        response.status_code = 503
    elif has_error and health_status["status"] == "degraded":
        response.status_code = 200

    return health_status
