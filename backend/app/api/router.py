"""
Main API router that aggregates all sub-routers.
"""
from fastapi import APIRouter

from app.api.auth import router as auth_router
from app.api.graphs import router as graphs_router
from app.api.executions import router as executions_router
from app.api.nodes import router as nodes_router
from app.api.openrouter import router as openrouter_router
from app.api.health import router as health_router
from app.api.connections import router as connections_router
from app.api.lineage import router as lineage_router
from app.api.dead_letter import router as dead_letter_router
from app.api.query import router as query_router
from app.api.graph_templates import router as graph_templates_router
from app.api.files import router as files_router

api_router = APIRouter()

api_router.include_router(auth_router)
api_router.include_router(graphs_router)
api_router.include_router(executions_router)
api_router.include_router(nodes_router)
api_router.include_router(openrouter_router)
api_router.include_router(health_router)
api_router.include_router(connections_router)
api_router.include_router(lineage_router)
api_router.include_router(dead_letter_router)
api_router.include_router(query_router)
api_router.include_router(graph_templates_router)
api_router.include_router(files_router)
