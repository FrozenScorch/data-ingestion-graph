"""
FastAPI application with lifespan context manager.
Initializes database, Redis, and node registry on startup.
"""

import logging
import sys
from contextlib import asynccontextmanager
from typing import AsyncGenerator

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.config import settings
from app.api.router import api_router
from app.db.session import init_db
from app.db.redis import init_redis, close_redis
from app.services.auth_service import seed_admin_user

# Configure logging
logging.basicConfig(
    level=getattr(logging, settings.log_level.upper(), logging.INFO),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Track component health for the health endpoint
_component_health: dict[str, str] = {}


def get_component_health() -> dict[str, str]:
    """Return the current component health status."""
    return _component_health.copy()


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """
    Application lifespan: startup and shutdown logic.
    Initializes database (mandatory), Redis (optional), seeds admin user, loads node registry.
    """
    global _component_health
    _component_health = {}
    logger.info("Starting Enterprise Data Ingestion Graph Studio...")

    # Validate security-critical settings (skip in development for convenience)
    if settings.app_env != "development":
        try:
            settings.validate_security()
        except RuntimeError as e:
            logger.critical(str(e))
            raise

    # Initialize database (mandatory - app must not start without it)
    try:
        await init_db()
        _component_health["database"] = "ok"
        logger.info("Database initialized successfully")
    except Exception as e:
        _component_health["database"] = f"error: {e}"
        logger.critical(f"Failed to initialize database (FATAL): {e}")
        logger.critical("Application cannot start without a database connection. Exiting.")
        sys.exit(1)

    # Encrypt any legacy plaintext connector credentials before serving traffic.
    try:
        from app.db.session import AsyncSessionLocal
        from app.services.connection_service import migrate_plaintext_connection_configs

        async with AsyncSessionLocal() as session:
            migrated = await migrate_plaintext_connection_configs(session)
        if migrated:
            logger.info("Encrypted %d legacy connection configurations", migrated)
    except Exception as e:
        logger.critical("Failed to encrypt saved connection credentials: %s", e)
        raise

    # Initialize Redis connection (optional - cache only, warn but continue)
    try:
        await init_redis()
        _component_health["redis"] = "ok"
        logger.info("Redis connected successfully")
    except Exception as e:
        _component_health["redis"] = f"error: {e}"
        logger.warning(f"Failed to connect to Redis (non-fatal, caching disabled): {e}")

    # Seed admin user
    try:
        from app.db.session import AsyncSessionLocal

        async with AsyncSessionLocal() as session:
            await seed_admin_user(session)
        logger.info("Admin user seeded (or already exists)")
    except Exception as e:
        logger.error(f"Failed to seed admin user: {e}")

    from app.services.query_artifact_service import prune_query_artifacts

    try:
        removed_artifacts = prune_query_artifacts()
    except OSError as exc:
        removed_artifacts = 0
        logger.warning("Could not prune query artifacts: %s", exc)
    if removed_artifacts:
        logger.info("Pruned %s expired query artifacts", removed_artifacts)

    # Discover and register all node types
    try:
        from app.nodes.registry import discover_nodes

        discover_nodes()
        from app.nodes.registry import get_all_nodes
        from app.graph_templates import validate_templates

        node_count = len(get_all_nodes())
        validate_templates()
        logger.info(f"Node registry loaded: {node_count} node types registered")
    except Exception as e:
        logger.exception("Failed to load node registry or graph templates")
        raise RuntimeError("Studio node registry failed validation") from e

    logger.info(f"Enterprise Data Ingestion Graph Studio ready on port {settings.app_port}")

    yield  # Application is running

    # Shutdown
    logger.info("Shutting down Enterprise Data Ingestion Graph Studio...")
    await close_redis()
    logger.info("Shutdown complete")


# Create FastAPI application
app = FastAPI(
    title="Enterprise Data Ingestion Graph Studio",
    description=(
        "Visual control plane for building and testing pipelines powered by the "
        "independently installable ingestion_graph SDK"
    ),
    version="0.2.0",
    lifespan=lifespan,
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins_list,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include API router
app.include_router(api_router)

# WebSocket endpoint for execution progress
from fastapi import WebSocket
from app.ws.execution_ws import ws_manager


@app.websocket("/ws/executions/{run_id}")
async def execution_websocket(websocket: WebSocket, run_id: str):
    """WebSocket endpoint for live execution run progress. Requires JWT token via ?token=xxx query param."""
    await ws_manager.handle_connection(websocket, run_id)


@app.get("/")
async def root():
    """Root endpoint - redirect to docs."""
    from ingestion_graph import __version__ as sdk_version

    return {
        "name": settings.app_name,
        "product": "Enterprise Data Ingestion Graph Studio",
        "studio_version": "0.2.0",
        "sdk": {"package": "ingestion-graph", "version": sdk_version},
        "docs": "/docs",
    }
