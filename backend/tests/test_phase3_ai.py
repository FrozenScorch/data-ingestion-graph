"""
Phase 3 tests for AI nodes: Embedder, LLM Extract, LLM Classify, LLM Summarize.

All OpenRouter API calls are MOCKED -- no real API calls are made.
Tests cover:
- Embedder processes chunks with mocked embedding response
- LLM extract returns structured output from mocked LLM response
- LLM classify returns category + confidence
- LLM summarize returns summary
- Cost tracking recorded correctly in node metadata
- Free model validation passes for glm-4.5-air:free
- Free model validation rejects paid models
"""
import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.nodes.base import NodeContext
from app.nodes.embedder import EmbedderNode
from app.nodes.llm_extract import LLMExtractNode
from app.nodes.llm_classify import LLMClassifyNode
from app.nodes.llm_summarize import LLMSummarizeNode
from app.services.openrouter_service import OpenRouterService, SUPPORTED_EMBEDDING_MODELS


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def mock_openrouter_service():
    """Create an OpenRouterService with a fake models cache for cost calculations."""
    service = OpenRouterService()
    # Pre-populate models cache with pricing data
    service._models_cache = [
        {
            "id": "z-ai/glm-4.5-air:free",
            "pricing": {"prompt": "0", "completion": "0"},
        },
        {
            "id": "qwen/qwen3.5-9b",
            "pricing": {"prompt": "0", "completion": "0"},
        },
        {
            "id": "openai/gpt-4",
            "pricing": {"prompt": "0.00003", "completion": "0.00006"},
        },
        {
            "id": "openai/text-embedding-3-small",
            "pricing": {"prompt": "0.00000002", "completion": "0"},
        },
        {
            "id": "qwen/qwen3-embedding-8b",
            "pricing": {"prompt": "0", "completion": "0"},
        },
        {
            "id": "baai/bge-m3",
            "pricing": {"prompt": "0", "completion": "0"},
        },
    ]
    return service


@pytest.fixture
def make_context():
    """Factory fixture for creating NodeContext instances."""
    def _make(config=None, input_data=None):
        return NodeContext(
            run_id="test-run-001",
            node_id="test-node-001",
            config=config or {},
            input_data=input_data or {},
        )
    return _make


# ---------------------------------------------------------------------------
# OpenRouter Service Tests
# ---------------------------------------------------------------------------

class TestOpenRouterService:

    def test_free_model_validation_passes(self, mock_openrouter_service):
        """Free model validation should pass for allowed free models."""
        service = mock_openrouter_service
        # Should not raise
        service.validate_test_model("z-ai/glm-4.5-air:free")
        service.validate_test_model("qwen/qwen3.5-9b")

    def test_free_model_validation_rejects_paid(self, mock_openrouter_service):
        """Free model validation should reject paid models."""
        service = mock_openrouter_service
        with pytest.raises(ValueError, match="Paid model"):
            service.validate_test_model("openai/gpt-4")
        with pytest.raises(ValueError, match="Paid model"):
            service.validate_test_model("anthropic/claude-3-opus")

    def test_is_free_model(self, mock_openrouter_service):
        service = mock_openrouter_service
        assert service.is_free_model("z-ai/glm-4.5-air:free") is True
        assert service.is_free_model("qwen/qwen3.5-9b") is True
        assert service.is_free_model("openai/gpt-4") is False

    def test_calculate_cost_free_model(self, mock_openrouter_service):
        service = mock_openrouter_service
        costs = service.calculate_cost("z-ai/glm-4.5-air:free", 100, 50)
        assert costs["input_cost_usd"] == 0.0
        assert costs["output_cost_usd"] == 0.0
        assert costs["total_cost_usd"] == 0.0

    def test_calculate_cost_paid_model(self, mock_openrouter_service):
        service = mock_openrouter_service
        # gpt-4: prompt=0.00003, completion=0.00006
        costs = service.calculate_cost("openai/gpt-4", 1000, 500)
        assert costs["input_cost_usd"] == pytest.approx(0.03, abs=1e-10)
        assert costs["output_cost_usd"] == pytest.approx(0.03, abs=1e-10)
        assert costs["total_cost_usd"] == pytest.approx(0.06, abs=1e-10)

    def test_estimate_cost(self, mock_openrouter_service):
        service = mock_openrouter_service
        total = service.estimate_cost("openai/gpt-4", 1000, 500)
        assert total == pytest.approx(0.06, abs=1e-10)

    def test_calculate_cost_no_cache(self, mock_openrouter_service):
        """Cost calculation with empty cache should return zeros."""
        service = mock_openrouter_service
        service._models_cache = None
        costs = service.calculate_cost("any-model", 100, 50)
        assert costs["input_cost_usd"] == 0.0
        assert costs["output_cost_usd"] == 0.0
        assert costs["total_cost_usd"] == 0.0

    def test_calculate_cost_unknown_model(self, mock_openrouter_service):
        """Cost calculation for unknown model should return zeros."""
        service = mock_openrouter_service
        costs = service.calculate_cost("unknown/model", 100, 50)
        assert costs["input_cost_usd"] == 0.0
        assert costs["output_cost_usd"] == 0.0
        assert costs["total_cost_usd"] == 0.0

    def test_set_models_cache(self, mock_openrouter_service):
        service = mock_openrouter_service
        fake_models = [{"id": "test/model", "pricing": {"prompt": "1", "completion": "2"}}]
        service.set_models_cache(fake_models)
        assert len(service._models_cache) == 1
        assert service._models_cache[0]["id"] == "test/model"


# ---------------------------------------------------------------------------
# EmbedderNode Tests
# ---------------------------------------------------------------------------

class TestEmbedderNode:

    @pytest.mark.asyncio
    async def test_embedder_processes_chunks(self, make_context):
        """Embedder should process chunks and return embeddings."""
        node = EmbedderNode()

        chunks = [
            {"text": "Hello world", "metadata": {"source": "doc1"}},
            {"text": "Second chunk", "metadata": {"source": "doc1"}},
            {"text": "Third chunk text", "metadata": {"source": "doc2"}},
        ]

        # Use side_effect to return correct number of embeddings per batch call
        batch1_response = {
            "data": [
                {"embedding": [0.1] * 10, "index": 0},
                {"embedding": [0.2] * 10, "index": 1},
            ],
            "usage": {"prompt_tokens": 20, "total_tokens": 20},
        }
        batch2_response = {
            "data": [
                {"embedding": [0.3] * 10, "index": 0},
            ],
            "usage": {"prompt_tokens": 10, "total_tokens": 10},
        }

        with patch("app.nodes.embedder.openrouter_service") as mock_service:
            mock_service.create_embeddings = AsyncMock(
                side_effect=[batch1_response, batch2_response]
            )
            mock_service.calculate_cost = MagicMock(return_value={
                "input_cost_usd": 0.0,
                "output_cost_usd": 0.0,
                "total_cost_usd": 0.0,
            })

            context = make_context(
                config={"model": "openai/text-embedding-3-small", "batch_size": 2},
                input_data={"chunks": chunks},
            )

            result = await node.execute(context)

        assert result.success is True
        assert result.items_processed == 3

        embeddings = result.output_data["embeddings"]
        assert len(embeddings) == 3

        # Check first embedding structure
        assert embeddings[0]["text"] == "Hello world"
        assert embeddings[0]["embedding"] == [0.1] * 10
        assert embeddings[0]["model"] == "openai/text-embedding-3-small"
        assert embeddings[0]["dimensions"] == 10
        assert embeddings[0]["metadata"] == {"source": "doc1"}

        # Check second embedding
        assert embeddings[1]["text"] == "Second chunk"
        assert embeddings[1]["embedding"] == [0.2] * 10

        # Check third embedding
        assert embeddings[2]["text"] == "Third chunk text"
        assert embeddings[2]["embedding"] == [0.3] * 10
        assert embeddings[2]["metadata"] == {"source": "doc2"}

        # Verify cost tracking in metadata
        assert "cost_usd" in result.metadata
        assert "input_tokens" in result.metadata
        assert "output_tokens" in result.metadata
        mock_service.calculate_cost.assert_called_once_with("openai/text-embedding-3-small", 30, 0)

    @pytest.mark.asyncio
    async def test_embedder_empty_chunks(self, make_context):
        """Embedder should handle empty chunks gracefully."""
        node = EmbedderNode()
        context = make_context(
            config={"model": "openai/text-embedding-3-small"},
            input_data={"chunks": []},
        )
        result = await node.execute(context)
        assert result.success is True
        assert result.output_data["embeddings"] == []
        assert result.items_processed == 0

    @pytest.mark.asyncio
    async def test_embedder_custom_input_field(self, make_context):
        """Embedder should use the configured input_field."""
        node = EmbedderNode()

        chunks = [
            {"content": "Custom field text", "metadata": {}},
        ]

        mock_response = {
            "data": [
                {"embedding": [0.5] * 8, "index": 0},
            ],
            "usage": {"prompt_tokens": 10, "total_tokens": 10},
        }

        with patch("app.nodes.embedder.openrouter_service") as mock_service:
            mock_service.create_embeddings = AsyncMock(return_value=mock_response)
            mock_service.calculate_cost = MagicMock(return_value={
                "input_cost_usd": 0.0,
                "output_cost_usd": 0.0,
                "total_cost_usd": 0.0,
            })

            context = make_context(
                config={"model": "baai/bge-m3", "input_field": "content"},
                input_data={"chunks": chunks},
            )

            result = await node.execute(context)

        assert result.success is True
        embeddings = result.output_data["embeddings"]
        assert len(embeddings) == 1
        assert embeddings[0]["text"] == "Custom field text"
        assert embeddings[0]["model"] == "baai/bge-m3"
        assert embeddings[0]["dimensions"] == 8

        # Verify the create_embeddings was called with the correct text
        call_args = mock_service.create_embeddings.call_args
        assert call_args[0][1] == ["Custom field text"]  # texts list

    @pytest.mark.asyncio
    async def test_embedder_batching(self, make_context):
        """Embedder should split into correct number of batches."""
        node = EmbedderNode()
        chunks = [{"text": f"chunk {i}", "metadata": {}} for i in range(5)]

        # Mock response for each batch (batch_size=2 => 3 batches: 2, 2, 1)
        def make_batch_response(texts):
            return {
                "data": [{"embedding": [0.0] * 5, "index": i} for i in range(len(texts))],
                "usage": {"prompt_tokens": len(texts) * 5, "total_tokens": len(texts) * 5},
            }

        call_count = 0

        async def mock_create(model, texts):
            nonlocal call_count
            call_count += 1
            return make_batch_response(texts)

        with patch("app.nodes.embedder.openrouter_service") as mock_service:
            mock_service.create_embeddings = AsyncMock(side_effect=mock_create)
            mock_service.calculate_cost = MagicMock(return_value={
                "input_cost_usd": 0.0,
                "output_cost_usd": 0.0,
                "total_cost_usd": 0.0,
            })

            context = make_context(
                config={"model": "qwen/qwen3-embedding-8b", "batch_size": 2},
                input_data={"chunks": chunks},
            )

            result = await node.execute(context)

        assert result.success is True
        assert result.items_processed == 5
        assert call_count == 3  # 2+2+1 chunks

    @pytest.mark.asyncio
    async def test_embedder_handles_api_error(self, make_context):
        """Embedder should return failure on API error."""
        node = EmbedderNode()
        chunks = [{"text": "Hello", "metadata": {}}]

        with patch("app.nodes.embedder.openrouter_service") as mock_service:
            mock_service.create_embeddings = AsyncMock(
                side_effect=Exception("API rate limit exceeded")
            )

            context = make_context(
                config={"model": "openai/text-embedding-3-small"},
                input_data={"chunks": chunks},
            )

            result = await node.execute(context)

        assert result.success is False
        assert "rate limit" in result.error_message.lower()
        assert result.items_processed == 0


# ---------------------------------------------------------------------------
# LLMExtractNode Tests
# ---------------------------------------------------------------------------

class TestLLMExtractNode:

    @pytest.mark.asyncio
    async def test_extract_returns_structured_output(self, make_context):
        """LLM extract should parse structured JSON from LLM response."""
        node = LLMExtractNode()

        extracted_data = {"name": "John Doe", "age": 30, "city": "New York"}
        mock_response = {
            "choices": [
                {
                    "message": {"role": "assistant", "content": json.dumps(extracted_data)},
                    "finish_reason": "stop",
                }
            ],
            "usage": {"prompt_tokens": 50, "completion_tokens": 20, "total_tokens": 70},
        }

        with patch("app.nodes.llm_extract.openrouter_service") as mock_service:
            mock_service.chat_completion = AsyncMock(return_value=mock_response)
            mock_service.calculate_cost = MagicMock(return_value={
                "input_cost_usd": 0.0,
                "output_cost_usd": 0.0,
                "total_cost_usd": 0.0,
            })

            context = make_context(
                config={
                    "model": "z-ai/glm-4.5-air:free",
                    "prompt": "Extract person info from the text",
                    "temperature": 0.0,
                    "max_tokens": 1024,
                },
                input_data={"text": "John Doe is 30 years old and lives in New York."},
            )

            result = await node.execute(context)

        assert result.success is True
        assert result.items_processed == 1

        extracted = result.output_data["json"]
        assert extracted["name"] == "John Doe"
        assert extracted["age"] == 30
        assert extracted["city"] == "New York"

        # Verify cost tracking
        assert "cost_usd" in result.metadata
        assert result.metadata["model"] == "z-ai/glm-4.5-air:free"
        assert result.metadata["tokens_used"]["total_tokens"] == 70
        mock_service.calculate_cost.assert_called_once_with("z-ai/glm-4.5-air:free", 50, 20)

    @pytest.mark.asyncio
    async def test_extract_handles_markdown_json(self, make_context):
        """LLM extract should extract JSON from markdown code blocks."""
        node = LLMExtractNode()

        content = '```json\n{"name": "Jane", "role": "engineer"}\n```'
        mock_response = {
            "choices": [
                {"message": {"role": "assistant", "content": content}, "finish_reason": "stop"}
            ],
            "usage": {"prompt_tokens": 30, "completion_tokens": 15, "total_tokens": 45},
        }

        with patch("app.nodes.llm_extract.openrouter_service") as mock_service:
            mock_service.chat_completion = AsyncMock(return_value=mock_response)
            mock_service.calculate_cost = MagicMock(return_value={
                "input_cost_usd": 0.0,
                "output_cost_usd": 0.0,
                "total_cost_usd": 0.0,
            })

            context = make_context(
                config={"model": "qwen/qwen3.5-9b", "prompt": "Extract info"},
                input_data={"text": "Jane is an engineer"},
            )

            result = await node.execute(context)

        assert result.success is True
        extracted = result.output_data["json"]
        assert extracted["name"] == "Jane"
        assert extracted["role"] == "engineer"

    @pytest.mark.asyncio
    async def test_extract_with_output_schema(self, make_context):
        """LLM extract should include schema in prompt when provided."""
        node = LLMExtractNode()

        schema = {
            "type": "object",
            "properties": {
                "title": {"type": "string"},
                "sentiment": {"type": "string", "enum": ["positive", "negative", "neutral"]},
            },
            "required": ["title", "sentiment"],
        }

        mock_response = {
            "choices": [
                {
                    "message": {"role": "assistant", "content": '{"title": "Great product", "sentiment": "positive"}'},
                    "finish_reason": "stop",
                }
            ],
            "usage": {"prompt_tokens": 60, "completion_tokens": 10, "total_tokens": 70},
        }

        with patch("app.nodes.llm_extract.openrouter_service") as mock_service:
            mock_service.chat_completion = AsyncMock(return_value=mock_response)
            mock_service.calculate_cost = MagicMock(return_value={
                "input_cost_usd": 0.0,
                "output_cost_usd": 0.0,
                "total_cost_usd": 0.0,
            })

            context = make_context(
                config={
                    "model": "z-ai/glm-4.5-air:free",
                    "prompt": "Analyze this review",
                    "output_schema": schema,
                },
                input_data={"text": "This product is amazing!"},
            )

            result = await node.execute(context)

        assert result.success is True
        # Verify the schema was included in the system prompt
        call_args = mock_service.chat_completion.call_args
        messages = call_args[1]["messages"]
        system_content = messages[0]["content"]
        assert "title" in system_content
        assert "sentiment" in system_content
        assert "JSON" in system_content

    @pytest.mark.asyncio
    async def test_extract_no_input_text(self, make_context):
        """LLM extract should return error when no input text."""
        node = LLMExtractNode()
        context = make_context(
            config={"model": "z-ai/glm-4.5-air:free", "prompt": "Extract info"},
            input_data={"text": ""},
        )
        result = await node.execute(context)
        assert result.success is False
        assert "No input text" in result.error_message

    @pytest.mark.asyncio
    async def test_extract_paid_model_rejected(self, make_context):
        """LLM extract should fail when a paid model is used in testing."""
        node = LLMExtractNode()

        with patch("app.nodes.llm_extract.openrouter_service") as mock_service:
            mock_service.chat_completion = AsyncMock(
                side_effect=ValueError("Paid model 'openai/gpt-4' cannot be used in testing")
            )

            context = make_context(
                config={"model": "openai/gpt-4", "prompt": "Extract info"},
                input_data={"text": "Some text"},
            )

            result = await node.execute(context)

        assert result.success is False
        assert "Paid model" in result.error_message

    @pytest.mark.asyncio
    async def test_extract_handles_invalid_json(self, make_context):
        """LLM extract should handle non-JSON response gracefully."""
        node = LLMExtractNode()

        mock_response = {
            "choices": [
                {
                    "message": {"role": "assistant", "content": "I cannot extract data from this text."},
                    "finish_reason": "stop",
                }
            ],
            "usage": {"prompt_tokens": 30, "completion_tokens": 10, "total_tokens": 40},
        }

        with patch("app.nodes.llm_extract.openrouter_service") as mock_service:
            mock_service.chat_completion = AsyncMock(return_value=mock_response)
            mock_service.calculate_cost = MagicMock(return_value={
                "input_cost_usd": 0.0,
                "output_cost_usd": 0.0,
                "total_cost_usd": 0.0,
            })

            context = make_context(
                config={"model": "z-ai/glm-4.5-air:free", "prompt": "Extract info"},
                input_data={"text": "Some text"},
            )

            result = await node.execute(context)

        # Should still succeed but with raw_response
        assert result.success is True
        extracted = result.output_data["json"]
        assert extracted.get("parse_error") is True
        assert "raw_response" in extracted


# ---------------------------------------------------------------------------
# LLMClassifyNode Tests
# ---------------------------------------------------------------------------

class TestLLMClassifyNode:

    @pytest.mark.asyncio
    async def test_classify_returns_category_and_confidence(self, make_context):
        """LLM classify should return category and confidence scores."""
        node = LLMClassifyNode()

        classify_response = {
            "category": "positive",
            "confidence": 0.92,
            "all_scores": {"positive": 0.92, "negative": 0.05, "neutral": 0.03},
        }
        mock_response = {
            "choices": [
                {
                    "message": {"role": "assistant", "content": json.dumps(classify_response)},
                    "finish_reason": "stop",
                }
            ],
            "usage": {"prompt_tokens": 40, "completion_tokens": 15, "total_tokens": 55},
        }

        with patch("app.nodes.llm_classify.openrouter_service") as mock_service:
            mock_service.chat_completion = AsyncMock(return_value=mock_response)
            mock_service.calculate_cost = MagicMock(return_value={
                "input_cost_usd": 0.0,
                "output_cost_usd": 0.0,
                "total_cost_usd": 0.0,
            })

            context = make_context(
                config={
                    "model": "z-ai/glm-4.5-air:free",
                    "prompt": "Classify the sentiment",
                    "categories": ["positive", "negative", "neutral"],
                    "temperature": 0.0,
                },
                input_data={"text": "This is absolutely wonderful!"},
            )

            result = await node.execute(context)

        assert result.success is True
        assert result.items_processed == 1

        classification = result.output_data["json"]
        assert classification["category"] == "positive"
        assert classification["confidence"] == 0.92
        assert classification["all_scores"]["positive"] == 0.92
        assert classification["all_scores"]["negative"] == 0.05
        assert classification["all_scores"]["neutral"] == 0.03

        # Verify cost tracking
        assert "cost_usd" in result.metadata
        assert result.metadata["model"] == "z-ai/glm-4.5-air:free"
        assert result.metadata["tokens_used"]["total_tokens"] == 55

    @pytest.mark.asyncio
    async def test_classify_handles_markdown_json(self, make_context):
        """LLM classify should parse JSON from markdown code blocks."""
        node = LLMClassifyNode()

        content = '```json\n{"category": "sports", "confidence": 0.88, "all_scores": {"sports": 0.88, "tech": 0.12}}\n```'
        mock_response = {
            "choices": [
                {"message": {"role": "assistant", "content": content}, "finish_reason": "stop"}
            ],
            "usage": {"prompt_tokens": 35, "completion_tokens": 20, "total_tokens": 55},
        }

        with patch("app.nodes.llm_classify.openrouter_service") as mock_service:
            mock_service.chat_completion = AsyncMock(return_value=mock_response)
            mock_service.calculate_cost = MagicMock(return_value={
                "input_cost_usd": 0.0,
                "output_cost_usd": 0.0,
                "total_cost_usd": 0.0,
            })

            context = make_context(
                config={
                    "model": "qwen/qwen3.5-9b",
                    "prompt": "Classify the topic",
                    "categories": ["sports", "tech"],
                },
                input_data={"text": "The basketball game was exciting"},
            )

            result = await node.execute(context)

        assert result.success is True
        classification = result.output_data["json"]
        assert classification["category"] == "sports"
        assert classification["confidence"] == 0.88

    @pytest.mark.asyncio
    async def test_classify_clamps_confidence(self, make_context):
        """LLM classify should clamp confidence to [0.0, 1.0]."""
        node = LLMClassifyNode()

        classify_response = {"category": "tech", "confidence": 1.5, "all_scores": {"tech": 1.5, "sports": -0.5}}
        mock_response = {
            "choices": [
                {
                    "message": {"role": "assistant", "content": json.dumps(classify_response)},
                    "finish_reason": "stop",
                }
            ],
            "usage": {"prompt_tokens": 30, "completion_tokens": 10, "total_tokens": 40},
        }

        with patch("app.nodes.llm_classify.openrouter_service") as mock_service:
            mock_service.chat_completion = AsyncMock(return_value=mock_response)
            mock_service.calculate_cost = MagicMock(return_value={
                "input_cost_usd": 0.0,
                "output_cost_usd": 0.0,
                "total_cost_usd": 0.0,
            })

            context = make_context(
                config={
                    "model": "z-ai/glm-4.5-air:free",
                    "prompt": "Classify",
                    "categories": ["tech", "sports"],
                },
                input_data={"text": "AI is advancing"},
            )

            result = await node.execute(context)

        assert result.success is True
        classification = result.output_data["json"]
        assert classification["confidence"] == 1.0  # clamped from 1.5

    @pytest.mark.asyncio
    async def test_classify_no_categories(self, make_context):
        """LLM classify should fail when no categories provided."""
        node = LLMClassifyNode()
        context = make_context(
            config={
                "model": "z-ai/glm-4.5-air:free",
                "prompt": "Classify this",
                "categories": [],
            },
            input_data={"text": "Some text"},
        )
        result = await node.execute(context)
        assert result.success is False
        assert "No categories" in result.error_message

    @pytest.mark.asyncio
    async def test_classify_no_input_text(self, make_context):
        """LLM classify should fail when no input text."""
        node = LLMClassifyNode()
        context = make_context(
            config={
                "model": "z-ai/glm-4.5-air:free",
                "prompt": "Classify",
                "categories": ["a", "b"],
            },
            input_data={"text": ""},
        )
        result = await node.execute(context)
        assert result.success is False
        assert "No input text" in result.error_message

    @pytest.mark.asyncio
    async def test_classify_includes_categories_in_prompt(self, make_context):
        """LLM classify should include category list in the system prompt."""
        node = LLMClassifyNode()

        mock_response = {
            "choices": [
                {
                    "message": {"role": "assistant", "content": '{"category": "a", "confidence": 1.0, "all_scores": {"a": 1.0}}'},
                    "finish_reason": "stop",
                }
            ],
            "usage": {"prompt_tokens": 20, "completion_tokens": 10, "total_tokens": 30},
        }

        with patch("app.nodes.llm_classify.openrouter_service") as mock_service:
            mock_service.chat_completion = AsyncMock(return_value=mock_response)
            mock_service.calculate_cost = MagicMock(return_value={
                "input_cost_usd": 0.0,
                "output_cost_usd": 0.0,
                "total_cost_usd": 0.0,
            })

            context = make_context(
                config={
                    "model": "z-ai/glm-4.5-air:free",
                    "prompt": "Classify this text",
                    "categories": ["positive", "negative"],
                },
                input_data={"text": "Test text"},
            )

            result = await node.execute(context)

        # Verify the categories were included in the prompt
        call_args = mock_service.chat_completion.call_args
        messages = call_args[1]["messages"]
        system_content = messages[0]["content"]
        assert "positive" in system_content
        assert "negative" in system_content
        assert "JSON" in system_content


# ---------------------------------------------------------------------------
# LLMSummarizeNode Tests
# ---------------------------------------------------------------------------

class TestLLMSummarizeNode:

    @pytest.mark.asyncio
    async def test_summarize_returns_summary(self, make_context):
        """LLM summarize should return the LLM summary text."""
        node = LLMSummarizeNode()

        mock_response = {
            "choices": [
                {
                    "message": {
                        "role": "assistant",
                        "content": "This is a concise summary of the input text.",
                    },
                    "finish_reason": "stop",
                }
            ],
            "usage": {"prompt_tokens": 100, "completion_tokens": 25, "total_tokens": 125},
        }

        with patch("app.nodes.llm_summarize.openrouter_service") as mock_service:
            mock_service.chat_completion = AsyncMock(return_value=mock_response)
            mock_service.calculate_cost = MagicMock(return_value={
                "input_cost_usd": 0.0,
                "output_cost_usd": 0.0,
                "total_cost_usd": 0.0,
            })

            context = make_context(
                config={
                    "model": "z-ai/glm-4.5-air:free",
                    "prompt": "Summarize the following text:",
                    "max_tokens": 512,
                    "temperature": 0.3,
                },
                input_data={"text": "This is a long text that needs to be summarized. " * 20},
            )

            result = await node.execute(context)

        assert result.success is True
        assert result.items_processed == 1
        assert result.output_data["text"] == "This is a concise summary of the input text."

        # Verify metadata
        assert result.metadata["summary"] == "This is a concise summary of the input text."
        assert result.metadata["model"] == "z-ai/glm-4.5-air:free"
        assert result.metadata["tokens_used"]["total_tokens"] == 125
        assert result.metadata["tokens_used"]["input_tokens"] == 100
        assert result.metadata["tokens_used"]["output_tokens"] == 25
        assert "cost_usd" in result.metadata

        # Verify cost calculation
        mock_service.calculate_cost.assert_called_once_with("z-ai/glm-4.5-air:free", 100, 25)

    @pytest.mark.asyncio
    async def test_summarize_default_prompt(self, make_context):
        """LLM summarize should use default prompt when not specified."""
        node = LLMSummarizeNode()

        mock_response = {
            "choices": [
                {
                    "message": {"role": "assistant", "content": "Summary here"},
                    "finish_reason": "stop",
                }
            ],
            "usage": {"prompt_tokens": 50, "completion_tokens": 10, "total_tokens": 60},
        }

        with patch("app.nodes.llm_summarize.openrouter_service") as mock_service:
            mock_service.chat_completion = AsyncMock(return_value=mock_response)
            mock_service.calculate_cost = MagicMock(return_value={
                "input_cost_usd": 0.0,
                "output_cost_usd": 0.0,
                "total_cost_usd": 0.0,
            })

            context = make_context(
                config={"model": "qwen/qwen3.5-9b"},
                input_data={"text": "Some text to summarize"},
            )

            result = await node.execute(context)

        assert result.success is True
        assert result.output_data["text"] == "Summary here"

        # Verify the default prompt was used
        call_args = mock_service.chat_completion.call_args
        messages = call_args[1]["messages"]
        assert "Summarize" in messages[0]["content"]

    @pytest.mark.asyncio
    async def test_summarize_no_input_text(self, make_context):
        """LLM summarize should return error when no input text."""
        node = LLMSummarizeNode()
        context = make_context(
            config={"model": "z-ai/glm-4.5-air:free"},
            input_data={"text": ""},
        )
        result = await node.execute(context)
        assert result.success is False
        assert "No input text" in result.error_message

    @pytest.mark.asyncio
    async def test_summarize_paid_model_rejected(self, make_context):
        """LLM summarize should fail when a paid model is used in testing."""
        node = LLMSummarizeNode()

        with patch("app.nodes.llm_summarize.openrouter_service") as mock_service:
            mock_service.chat_completion = AsyncMock(
                side_effect=ValueError("Paid model 'openai/gpt-4' cannot be used in testing")
            )

            context = make_context(
                config={"model": "openai/gpt-4"},
                input_data={"text": "Some text"},
            )

            result = await node.execute(context)

        assert result.success is False
        assert "Paid model" in result.error_message

    @pytest.mark.asyncio
    async def test_summarize_empty_response(self, make_context):
        """LLM summarize should handle empty LLM response."""
        node = LLMSummarizeNode()

        mock_response = {
            "choices": [
                {
                    "message": {"role": "assistant", "content": ""},
                    "finish_reason": "stop",
                }
            ],
            "usage": {"prompt_tokens": 50, "completion_tokens": 0, "total_tokens": 50},
        }

        with patch("app.nodes.llm_summarize.openrouter_service") as mock_service:
            mock_service.chat_completion = AsyncMock(return_value=mock_response)
            mock_service.calculate_cost = MagicMock(return_value={
                "input_cost_usd": 0.0,
                "output_cost_usd": 0.0,
                "total_cost_usd": 0.0,
            })

            context = make_context(
                config={"model": "z-ai/glm-4.5-air:free"},
                input_data={"text": "Some text"},
            )

            result = await node.execute(context)

        assert result.success is True
        assert result.output_data["text"] == ""

    @pytest.mark.asyncio
    async def test_summarize_no_choices(self, make_context):
        """LLM summarize should handle API returning no choices."""
        node = LLMSummarizeNode()

        mock_response = {
            "choices": [],
            "usage": {"prompt_tokens": 50, "completion_tokens": 0, "total_tokens": 50},
        }

        with patch("app.nodes.llm_summarize.openrouter_service") as mock_service:
            mock_service.chat_completion = AsyncMock(return_value=mock_response)

            context = make_context(
                config={"model": "z-ai/glm-4.5-air:free"},
                input_data={"text": "Some text"},
            )

            result = await node.execute(context)

        assert result.success is False
        assert "no choices" in result.error_message.lower()


# ---------------------------------------------------------------------------
# Cost Tracking Tests
# ---------------------------------------------------------------------------

class TestCostTracking:

    @pytest.mark.asyncio
    async def test_embedder_cost_tracking(self, make_context):
        """Embedder should record cost metadata after embedding."""
        node = EmbedderNode()

        mock_response = {
            "data": [{"embedding": [0.1] * 5, "index": 0}],
            "usage": {"prompt_tokens": 15, "total_tokens": 15},
        }

        with patch("app.nodes.embedder.openrouter_service") as mock_service:
            mock_service.create_embeddings = AsyncMock(return_value=mock_response)
            mock_service.calculate_cost = MagicMock(return_value={
                "input_cost_usd": 0.0000003,
                "output_cost_usd": 0.0,
                "total_cost_usd": 0.0000003,
            })

            context = make_context(
                config={"model": "openai/text-embedding-3-small"},
                input_data={"chunks": [{"text": "test", "metadata": {}}]},
            )

            result = await node.execute(context)

        meta = result.metadata
        assert meta["input_cost_usd"] == 0.0000003
        assert meta["output_cost_usd"] == 0.0
        assert meta["cost_usd"] == 0.0000003
        assert meta["input_tokens"] == 15
        assert meta["output_tokens"] == 0

    @pytest.mark.asyncio
    async def test_extract_cost_tracking(self, make_context):
        """LLM extract should record cost with input/output token breakdown."""
        node = LLMExtractNode()

        mock_response = {
            "choices": [
                {"message": {"role": "assistant", "content": '{"key": "value"}'}, "finish_reason": "stop"}
            ],
            "usage": {"prompt_tokens": 80, "completion_tokens": 10, "total_tokens": 90},
        }

        with patch("app.nodes.llm_extract.openrouter_service") as mock_service:
            mock_service.chat_completion = AsyncMock(return_value=mock_response)
            mock_service.calculate_cost = MagicMock(return_value={
                "input_cost_usd": 0.001,
                "output_cost_usd": 0.002,
                "total_cost_usd": 0.003,
            })

            context = make_context(
                config={"model": "z-ai/glm-4.5-air:free", "prompt": "Extract"},
                input_data={"text": "Input text"},
            )

            result = await node.execute(context)

        meta = result.metadata
        assert meta["input_cost_usd"] == 0.001
        assert meta["output_cost_usd"] == 0.002
        assert meta["cost_usd"] == 0.003
        assert meta["tokens_used"]["input_tokens"] == 80
        assert meta["tokens_used"]["output_tokens"] == 10

    @pytest.mark.asyncio
    async def test_classify_cost_tracking(self, make_context):
        """LLM classify should record cost in metadata."""
        node = LLMClassifyNode()

        mock_response = {
            "choices": [
                {
                    "message": {"role": "assistant", "content": '{"category": "a", "confidence": 0.9, "all_scores": {"a": 0.9}}'},
                    "finish_reason": "stop",
                }
            ],
            "usage": {"prompt_tokens": 40, "completion_tokens": 8, "total_tokens": 48},
        }

        with patch("app.nodes.llm_classify.openrouter_service") as mock_service:
            mock_service.chat_completion = AsyncMock(return_value=mock_response)
            mock_service.calculate_cost = MagicMock(return_value={
                "input_cost_usd": 0.0005,
                "output_cost_usd": 0.0003,
                "total_cost_usd": 0.0008,
            })

            context = make_context(
                config={
                    "model": "z-ai/glm-4.5-air:free",
                    "prompt": "Classify",
                    "categories": ["a", "b"],
                },
                input_data={"text": "Text"},
            )

            result = await node.execute(context)

        meta = result.metadata
        assert meta["cost_usd"] == 0.0008
        assert meta["tokens_used"]["input_tokens"] == 40
        assert meta["tokens_used"]["output_tokens"] == 8

    @pytest.mark.asyncio
    async def test_summarize_cost_tracking(self, make_context):
        """LLM summarize should record cost in metadata."""
        node = LLMSummarizeNode()

        mock_response = {
            "choices": [
                {"message": {"role": "assistant", "content": "Summary text"}, "finish_reason": "stop"}
            ],
            "usage": {"prompt_tokens": 200, "completion_tokens": 50, "total_tokens": 250},
        }

        with patch("app.nodes.llm_summarize.openrouter_service") as mock_service:
            mock_service.chat_completion = AsyncMock(return_value=mock_response)
            mock_service.calculate_cost = MagicMock(return_value={
                "input_cost_usd": 0.01,
                "output_cost_usd": 0.015,
                "total_cost_usd": 0.025,
            })

            context = make_context(
                config={"model": "z-ai/glm-4.5-air:free"},
                input_data={"text": "Long text to summarize"},
            )

            result = await node.execute(context)

        meta = result.metadata
        assert meta["cost_usd"] == 0.025
        assert meta["tokens_used"]["input_tokens"] == 200
        assert meta["tokens_used"]["output_tokens"] == 50


# ---------------------------------------------------------------------------
# Node Interface Tests
# ---------------------------------------------------------------------------

class TestNodeInterfaces:

    def test_embedder_node_properties(self):
        """EmbedderNode should have correct base node properties."""
        node = EmbedderNode()
        assert node.node_type == "embedder"
        assert node.display_name == "Embedder"
        assert node.category == "ai"
        assert len(node.inputs) == 1
        assert node.inputs[0].data_type.value == "chunks"
        assert len(node.outputs) == 1
        assert node.outputs[0].data_type.value == "embeddings"
        assert "model" in node.config_schema["properties"]
        assert "batch_size" in node.config_schema["properties"]
        assert "input_field" in node.config_schema["properties"]

    def test_llm_extract_node_properties(self):
        """LLMExtractNode should have correct base node properties."""
        node = LLMExtractNode()
        assert node.node_type == "llm_extract"
        assert node.display_name == "LLM Extract"
        assert node.category == "ai"
        assert node.inputs[0].data_type.value == "text"
        assert node.outputs[0].data_type.value == "json"
        assert "model" in node.config_schema["required"]
        assert "prompt" in node.config_schema["required"]
        assert "output_schema" in node.config_schema["properties"]
        assert "temperature" in node.config_schema["properties"]

    def test_llm_classify_node_properties(self):
        """LLMClassifyNode should have correct base node properties."""
        node = LLMClassifyNode()
        assert node.node_type == "llm_classify"
        assert node.display_name == "LLM Classify"
        assert node.category == "ai"
        assert "categories" in node.config_schema["required"]
        assert node.config_schema["properties"]["temperature"]["default"] == 0.0

    def test_llm_summarize_node_properties(self):
        """LLMSummarizeNode should have correct base node properties."""
        node = LLMSummarizeNode()
        assert node.node_type == "llm_summarize"
        assert node.display_name == "LLM Summarize"
        assert node.category == "ai"
        assert node.inputs[0].data_type.value == "text"
        assert node.outputs[0].data_type.value == "text"
        assert node.config_schema["properties"]["temperature"]["default"] == 0.3
        assert node.config_schema["properties"]["max_tokens"]["default"] == 512

    def test_embedder_config_schema_enum(self):
        """EmbedderNode config schema should include supported embedding models."""
        node = EmbedderNode()
        model_prop = node.config_schema["properties"]["model"]
        assert model_prop["enum"] == SUPPORTED_EMBEDDING_MODELS

    def test_node_serialization(self):
        """All AI nodes should serialize correctly."""
        for node_class in [EmbedderNode, LLMExtractNode, LLMClassifyNode, LLMSummarizeNode]:
            node = node_class()
            d = node.to_dict()
            assert d["type"] == node.node_type
            assert d["display_name"] == node.display_name
            assert d["category"] == "ai"
            assert "inputs" in d
            assert "outputs" in d
            assert "config_schema" in d
