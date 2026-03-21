"""
OpenRouter API routes: model listing, cost estimation.
"""
from typing import Optional

from fastapi import APIRouter, Depends, Query, HTTPException, status

from app.middleware.auth import get_current_user
from app.services.openrouter_service import openrouter_service

router = APIRouter(prefix="/api/openrouter", tags=["openrouter"])


@router.get("/models")
async def list_models(
    category: Optional[str] = Query(None, description="Filter: 'chat' or 'embedding'"),
    current_user: dict = Depends(get_current_user),
):
    """List OpenRouter models sorted by cost (cheapest first)."""
    if category == "embedding":
        models = await openrouter_service.list_embedding_models()
    elif category == "chat":
        models = await openrouter_service.list_chat_models()
    else:
        models = await openrouter_service.list_models()

    # Enrich with free badge
    from app.config import settings
    for model in models:
        model["is_free"] = openrouter_service.is_free_model(model.get("id", ""))

    return {"models": models, "total": len(models)}


@router.get("/embedding-models")
async def list_embedding_models(
    current_user: dict = Depends(get_current_user),
):
    """List available embedding models."""
    models = await openrouter_service.list_embedding_models()
    return {"models": models, "total": len(models)}


@router.get("/cost-estimate")
async def estimate_cost(
    model: str = Query(..., description="Model ID"),
    input_tokens: int = Query(1000, ge=0, description="Input tokens"),
    output_tokens: int = Query(500, ge=0, description="Output tokens"),
    current_user: dict = Depends(get_current_user),
):
    """Estimate cost for a model invocation."""
    cost = openrouter_service.calculate_cost(model, input_tokens, output_tokens)
    return {
        "model": model,
        "input_tokens": input_tokens,
        "output_tokens": output_tokens,
        "is_free": openrouter_service.is_free_model(model),
        **cost,
    }
