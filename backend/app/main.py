"""
FastAPI application with lifespan context manager.
Initializes database, Redis, and node registry on startup.
"""
import logging
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


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """
    Application lifespan: startup and shutdown logic.
    Initializes database, Redis, seeds admin user, loads node registry.
    """
    logger.info("Starting ingestion-graph application...")

    # Initialize database (create tables)
    try:
        await init_db()
        logger.info("Database initialized successfully")
    except Exception as e:
        logger.error(f"Failed to initialize database: {e}")

    # Initialize Redis connection
    try:
        await init_redis()
        logger.info("Redis connected successfully")
    except Exception as e:
        logger.error(f"Failed to connect to Redis: {e}")

    # Seed admin user
    try:
        from app.db.session import AsyncSessionLocal
        async with AsyncSessionLocal() as session:
            await seed_admin_user(session)
        logger.info("Admin user seeded (or already exists)")
    except Exception as e:
        logger.error(f"Failed to seed admin user: {e}")

    # Discover and register all node types
    try:
        from app.nodes.registry import discover_nodes
        discover_nodes()
        from app.nodes.registry import get_all_nodes
        node_count = len(get_all_nodes())
        logger.info(f"Node registry loaded: {node_count} node types registered")
    except Exception as e:
        logger.error(f"Failed to load node registry: {e}")

    logger.info(f"ingestion-graph ready on port {settings.app_port}")

    yield  # Application is running

    # Shutdown
    logger.info("Shutting down ingestion-graph...")
    await close_redis()
    logger.info("Shutdown complete")


# Create FastAPI application
app = FastAPI(
    title="Ingestion Graph",
    description="Visual, node-based data ingestion pipeline builder with DAG execution engine",
    version="0.1.0",
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
    """WebSocket endpoint for live execution run progress."""
    await ws_manager.handle_connection(websocket, run_id)


@app.get("/")
async def root():
    """Root endpoint - redirect to docs."""
    return {"name": "ingestion-graph", "version": "0.1.0", "docs": "/docs"}
