"""
LLMClassify node: text classification using LLM via OpenRouter.

Takes text input and classifies it into one of the provided categories
using an LLM, returning the predicted category and confidence scores.
"""
import json
import logging
from typing import Any

from app.nodes.base import BaseNode, NodeContext, NodeResult, PortDef, PortDataType
from app.services.openrouter_service import openrouter_service

logger = logging.getLogger(__name__)


class LLMClassifyNode(BaseNode):
    @property
    def node_type(self) -> str:
        return "llm_classify"

    @property
    def display_name(self) -> str:
        return "LLM Classify"

    @property
    def category(self) -> str:
        return "ai"

    @property
    def description(self) -> str:
        return "Classify text into categories using an LLM"

    @property
    def inputs(self) -> list[PortDef]:
        return [PortDef(name="text", data_type=PortDataType.TEXT, required=True, label="Input Text")]

    @property
    def outputs(self) -> list[PortDef]:
        return [PortDef(name="json", data_type=PortDataType.JSON, label="Classification Result")]

    @property
    def config_schema(self) -> dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "model": {
                    "type": "string",
                    "description": "Chat model ID (sorted by cost in UI)",
                },
                "prompt": {
                    "type": "string",
                    "description": "Classification prompt describing the classification task",
                },
                "categories": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Comma-separated or JSON array of category labels",
                },
                "temperature": {
                    "type": "number",
                    "default": 0.0,
                    "minimum": 0.0,
                    "maximum": 2.0,
                    "description": "Temperature (0.0 for deterministic classification)",
                },
                "max_tokens": {
                    "type": "integer",
                    "default": 1024,
                    "minimum": 1,
                    "maximum": 4096,
                    "description": "Maximum tokens in the LLM response",
                },
            },
            "required": ["model", "prompt", "categories"],
        }

    async def execute(self, context: NodeContext) -> NodeResult:
        """
        Classify text into categories using an LLM.

        Expects context.input_data["text"] to be a string.
        Uses config["prompt"] as the system prompt and config["categories"]
        as the list of valid categories.

        Returns: {category: "...", confidence: 0.95, all_scores: {...}}
        """
        config = context.config
        model = config.get("model", "")
        prompt = config.get("prompt", "Classify the following text.")
        categories = config.get("categories", [])
        temperature = config.get("temperature", 0.0)
        max_tokens = config.get("max_tokens", 1024)

        if not categories:
            return NodeResult(
                success=False,
                output_data={"json": {"category": None}},
                items_processed=0,
                error_message="No categories provided in configuration",
            )

        input_text = context.input_data.get("text", "")
        if not input_text:
            return NodeResult(
                success=False,
                output_data={"json": {"category": None}},
                items_processed=0,
                error_message="No input text provided",
            )

        # Build classification prompt
        categories_str = ", ".join(categories)
        system_prompt = (
            f"{prompt}\n\n"
            f"Valid categories: {categories_str}\n\n"
            f"Respond with ONLY a JSON object in this exact format:\n"
            f'{{"category": "<one of the categories>", "confidence": <float 0.0-1.0>, '
            f'"all_scores": {{<each category>: <float 0.0-1.0>}}}}\n\n'
            f"Do not include any other text outside the JSON."
        )

        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": input_text},
        ]

        try:
            response = await openrouter_service.chat_completion(
                model=model,
                messages=messages,
                temperature=temperature,
                max_tokens=max_tokens,
            )

            # Extract response text
            choices = response.get("choices", [])
            if not choices:
                return NodeResult(
                    success=False,
                    output_data={"json": {"category": None}},
                    items_processed=0,
                    error_message="LLM returned no choices",
                    metadata={"model": model},
                )

            content = choices[0].get("message", {}).get("content", "")

            # Parse JSON response
            try:
                result = json.loads(content)
            except json.JSONDecodeError:
                # Try to extract JSON from markdown code blocks
                import re
                json_match = re.search(r"```(?:json)?\s*\n?(.*?)\n?\s*```", content, re.DOTALL)
                if json_match:
                    try:
                        result = json.loads(json_match.group(1))
                    except json.JSONDecodeError:
                        result = {"category": None, "confidence": 0.0, "all_scores": {}, "parse_error": True}
                else:
                    result = {"category": None, "confidence": 0.0, "all_scores": {}, "parse_error": True}

            # Ensure valid structure
            category = result.get("category")
            confidence = result.get("confidence", 0.0)
            all_scores = result.get("all_scores", {})

            # Validate category is in the allowed list
            if category not in categories:
                logger.warning(
                    f"LLM returned unexpected category '{category}'. "
                    f"Allowed: {categories}"
                )

            # Ensure confidence is a float between 0 and 1
            try:
                confidence = float(confidence)
                confidence = max(0.0, min(1.0, confidence))
            except (TypeError, ValueError):
                confidence = 0.0

            classification_result = {
                "category": category,
                "confidence": confidence,
                "all_scores": all_scores,
            }

            # Extract usage
            usage = response.get("usage", {})
            input_tokens = usage.get("prompt_tokens", 0)
            output_tokens = usage.get("completion_tokens", 0)
            total_tokens = usage.get("total_tokens", input_tokens + output_tokens)

            # Record cost
            cost_info = openrouter_service.calculate_cost(model, input_tokens, output_tokens)

            return NodeResult(
                success=True,
                output_data={"json": classification_result},
                items_processed=1,
                metadata={
                    "model": model,
                    "tokens_used": {
                        "input_tokens": input_tokens,
                        "output_tokens": output_tokens,
                        "total_tokens": total_tokens,
                    },
                    "cost_usd": cost_info["total_cost_usd"],
                    "input_cost_usd": cost_info["input_cost_usd"],
                    "output_cost_usd": cost_info["output_cost_usd"],
                },
            )

        except ValueError as e:
            # Free model validation error
            return NodeResult(
                success=False,
                output_data={"json": {"category": None}},
                items_processed=0,
                error_message=str(e),
                metadata={"model": model},
            )
        except Exception as e:
            logger.error(f"LLM classify failed: {e}")
            return NodeResult(
                success=False,
                output_data={"json": {"category": None}},
                items_processed=0,
                error_message=f"LLM classify failed: {str(e)}",
                metadata={"model": model},
            )


def register():
    from app.nodes.registry import register_node
    register_node(LLMClassifyNode())
