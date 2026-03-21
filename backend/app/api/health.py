"""
Health check endpoint: verifies database and Redis connectivity.
"""
from fastapi import APIRouter

router = APIRouter(tags=["health"])


@router.get("/health")
async def health_check():
    """Check system health: database and Redis connectivity."""
    health_status = {"status": "healthy", "components": {}}

    # Check database
    try:
        from app.db.session import AsyncSessionLocal
        async with AsyncSessionLocal() as session:
            from sqlalchemy import text
            await session.execute(text("SELECT 1"))
        health_status["components"]["database"] = {"status": "ok"}
    except Exception as e:
        health_status["status"] = "unhealthy"
        health_status["components"]["database"] = {"status": "error", "detail": str(e)}

    # Check Redis
    try:
        from app.db.redis import get_redis
        redis = get_redis()
        await redis.ping()
        health_status["components"]["redis"] = {"status": "ok"}
    except Exception as e:
        health_status["status"] = "unhealthy"
        health_status["components"]["redis"] = {"status": "error", "detail": str(e)}

    return health_status
