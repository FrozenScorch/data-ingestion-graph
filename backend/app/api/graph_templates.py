"""Predefined Enterprise Studio graph templates."""

from app.graph_templates import TEMPLATES
from app.middleware.auth import get_current_user
from fastapi import APIRouter, Depends

router = APIRouter(prefix="/api/graph-templates", tags=["graph-templates"])


@router.get("")
async def list_graph_templates(
    current_user: dict = Depends(get_current_user),
) -> dict:
    return {"templates": [template.summary() for template in TEMPLATES.values()]}
