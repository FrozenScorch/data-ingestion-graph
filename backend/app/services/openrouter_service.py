"""
OpenRouter service: model listing, cost calculation, chat/embedding proxies.

Features:
- Model listing with Redis caching (1hr TTL)
- Chat and embedding proxies via OpenAI SDK
- Cost estimation from model pricing
- Test-mode free-model validation
"""
import json
import logging
from typing import Any, Optional

from openai import AsyncOpenAI

from app.config import settings

logger = logging.getLogger(__name__)

# Embedding models supported by this project
SUPPORTED_EMBEDDING_MODELS = [
    "openai/text-embedding-3-small",
    "qwen/qwen3-embedding-8b",
    "baai/bge-m3",
]

# Cache TTL in seconds (1 hour)
MODELS_CACHE_TTL = 3600
MODELS_CACHE_KEY = "openrouter:models_cache"


class OpenRouterService:
    """
    Service for interacting with OpenRouter API.
    Handles model listing (cached in Redis), cost calculation,
    and proxying chat/embedding requests.
    """

    def __init__(self):
        self.client = AsyncOpenAI(
            base_url="https://openrouter.ai/api/v1",
            api_key=settings.openrouter_api_key,
        )
        self._models_cache: list[dict] | None = None

    async def list_models(self) -> list[dict]:
        """
        Fetch available models from OpenRouter, sorted by prompt cost (cheapest first).

        Results are cached in Redis for 1 hour. Falls back to in-memory cache
        if Redis is unavailable.

        Returns:
            List of model dicts with id, name, pricing, context_length, etc.
        """
        # Try Redis cache first
        redis_client = None
        try:
            import redis as redis_lib
            redis_client = redis_lib.from_url(settings.redis_url, decode_responses=True)
            cached = redis_client.get(MODELS_CACHE_KEY)
            if cached:
                models = json.loads(cached)
                self._models_cache = models
                return models
        except Exception:
            logger.debug("Redis not available for model cache, using in-memory or fetching fresh")

        # Fetch from OpenRouter API
        try:
            response = await self.client.models.list()
            models = []
            for model in response.data:
                model_dict = model.model_dump()
                models.append(model_dict)

            # Sort by prompt cost per 1M tokens ascending
            models.sort(key=lambda m: float(m.get("pricing", {}).get("prompt", "999") or "999"))
            self._models_cache = models

            # Cache in Redis
            if redis_client:
                try:
                    redis_client.setex(MODELS_CACHE_KEY, MODELS_CACHE_TTL, json.dumps(models))
                except Exception:
                    logger.debug("Failed to cache models in Redis")

            return models
        except Exception as e:
            logger.error(f"Failed to fetch models from OpenRouter: {e}")
            return self._models_cache or []

    async def list_chat_models(self) -> list[dict]:
        """Get chat models only, sorted by cost."""
        all_models = await self.list_models()
        return [
            m for m in all_models
            if "embedding" not in m.get("id", "").lower()
            and "image" not in m.get("id", "").lower()
            and "tts" not in m.get("id", "").lower()
            and "whisper" not in m.get("id", "").lower()
        ]

    async def list_embedding_models(self) -> list[dict]:
        """Get embedding models only."""
        all_models = await self.list_models()
        return [
            m for m in all_models
            if "embedding" in m.get("id", "").lower()
        ]

    def is_free_model(self, model_id: str) -> bool:
        """Check if a model is free."""
        return model_id in settings.free_models_list

    def validate_test_model(self, model_id: str) -> None:
        """
        Validate that a model can be used in test/development environments.

        Only free models are allowed for testing.

        Args:
            model_id: The model identifier to validate.

        Raises:
            ValueError: If the model is not a free model.
        """
        if not self.is_free_model(model_id):
            raise ValueError(
                f"Paid model '{model_id}' cannot be used in testing. "
                f"Only free models are allowed: {settings.free_models_list}"
            )

    def calculate_cost(
        self,
        model_id: str,
        input_tokens: int,
        output_tokens: int,
    ) -> dict[str, float]:
        """
        Calculate cost for a model invocation.

        Returns dict with input_cost, output_cost, total_cost in USD.
        """
        model_info = None
        if self._models_cache:
            model_info = next(
                (m for m in self._models_cache if m.get("id") == model_id),
                None,
            )

        if model_info and model_info.get("pricing"):
            pricing = model_info["pricing"]
            prompt_cost_per_token = float(pricing.get("prompt", "0") or "0")
            completion_cost_per_token = float(pricing.get("completion", "0") or "0")
        else:
            prompt_cost_per_token = 0.0
            completion_cost_per_token = 0.0

        input_cost = input_tokens * prompt_cost_per_token
        output_cost = output_tokens * completion_cost_per_token

        return {
            "input_cost_usd": round(input_cost, 10),
            "output_cost_usd": round(output_cost, 10),
            "total_cost_usd": round(input_cost + output_cost, 10),
        }

    def estimate_cost(
        self,
        model_id: str,
        input_tokens: int,
        output_tokens: int,
    ) -> float:
        """
        Estimate total USD cost for a model invocation.

        Convenience method that returns a single float.
        """
        costs = self.calculate_cost(model_id, input_tokens, output_tokens)
        return costs["total_cost_usd"]

    async def chat_completion(
        self,
        model: str,
        messages: list[dict],
        temperature: float = 0.7,
        max_tokens: int = 2000,
        **kwargs,
    ) -> dict[str, Any]:
        """
        Proxy a chat completion request to OpenRouter.

        Enforces the free-model constraint for testing.

        Returns:
            Full response dict from OpenAI SDK, including usage info.
        """
        if settings.app_env in ("development", "testing"):
            self.validate_test_model(model)

        response = await self.client.chat.completions.create(
            model=model,
            messages=messages,
            temperature=temperature,
            max_tokens=max_tokens,
            **kwargs,
        )
        return response.model_dump()

    async def create_embeddings(
        self,
        model: str,
        input_data: list[str],
    ) -> dict[str, Any]:
        """
        Proxy an embedding request to OpenRouter.

        Returns:
            Full response dict from OpenAI SDK, including usage info.
        """
        response = await self.client.embeddings.create(
            model=model,
            input=input_data,
        )
        return response.model_dump()

    def set_models_cache(self, models: list[dict]) -> None:
        """
        Set the models cache directly (useful for testing or pre-loading).

        Args:
            models: List of model dicts to use as cache.
        """
        self._models_cache = models


# Global service instance
openrouter_service = OpenRouterService()
