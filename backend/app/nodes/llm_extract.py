"""
LLMExtract node: structured data extraction using LLM via OpenRouter.

Takes text input and uses an LLM to extract structured data according
to the provided prompt and optional output schema.
"""
import json
import logging
from typing import Any

from app.nodes.base import BaseNode, NodeContext, NodeResult, PortDef, PortDataType
from app.services.openrouter_service import openrouter_service

logger = logging.getLogger(__name__)


class LLMExtractNode(BaseNode):
    @property
    def node_type(self) -> str:
        return "llm_extract"

    @property
    def display_name(self) -> str:
        return "LLM Extract"

    @property
    def category(self) -> str:
        return "ai"

    @property
    def description(self) -> str:
        return "Extract structured data from text using an LLM"

    @property
    def inputs(self) -> list[PortDef]:
        return [PortDef(name="text", data_type=PortDataType.TEXT, required=True, label="Input Text")]

    @property
    def outputs(self) -> list[PortDef]:
        return [PortDef(name="json", data_type=PortDataType.JSON, label="Extracted Data")]

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
                    "description": "System prompt for extraction instructions",
                },
                "output_schema": {
                    "type": "object",
                    "description": "JSON schema the LLM should follow for structured extraction",
                },
                "temperature": {
                    "type": "number",
                    "default": 0.0,
                    "minimum": 0.0,
                    "maximum": 2.0,
                    "description": "Temperature (0.0 for deterministic extraction)",
                },
                "max_tokens": {
                    "type": "integer",
                    "default": 1024,
                    "minimum": 1,
                    "maximum": 16384,
                    "description": "Maximum tokens in the LLM response",
                },
            },
            "required": ["model", "prompt"],
        }

    async def execute(self, context: NodeContext) -> NodeResult:
        """
        Extract structured data from text using an LLM.

        Expects context.input_data["text"] to be a string.
        Uses config["prompt"] as the system prompt and optionally
        validates output against config["output_schema"].

        Returns: {extracted: {...}, model, tokens_used}
        """
        config = context.config
        model = config.get("model", "")
        prompt = config.get("prompt", "Extract structured data from the following text.")
        output_schema = config.get("output_schema")
        temperature = config.get("temperature", 0.0)
        max_tokens = config.get("max_tokens", 1024)

        input_text = context.input_data.get("text", "")
        if not input_text:
            return NodeResult(
                success=False,
                output_data={"json": {}},
                items_processed=0,
                error_message="No input text provided",
            )

        # Build system prompt with schema instructions
        system_prompt = prompt
        if output_schema:
            schema_str = json.dumps(output_schema, indent=2)
            system_prompt += (
                f"\n\nRespond ONLY with valid JSON matching this schema:\n{schema_str}\n"
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
                    output_data={"json": {}},
                    items_processed=0,
                    error_message="LLM returned no choices",
                    metadata={"model": model},
                )

            content = choices[0].get("message", {}).get("content", "")

            # Parse JSON from response
            try:
                extracted = json.loads(content)
            except json.JSONDecodeError:
                # Try to extract JSON from markdown code blocks
                import re
                json_match = re.search(r"```(?:json)?\s*\n?(.*?)\n?\s*```", content, re.DOTALL)
                if json_match:
                    try:
                        extracted = json.loads(json_match.group(1))
                    except json.JSONDecodeError:
                        extracted = {"raw_response": content, "parse_error": True}
                else:
                    extracted = {"raw_response": content, "parse_error": True}

            # Extract usage
            usage = response.get("usage", {})
            input_tokens = usage.get("prompt_tokens", 0)
            output_tokens = usage.get("completion_tokens", 0)
            total_tokens = usage.get("total_tokens", input_tokens + output_tokens)

            # Record cost
            cost_info = openrouter_service.calculate_cost(model, input_tokens, output_tokens)

            return NodeResult(
                success=True,
                output_data={"json": extracted},
                items_processed=1,
                metadata={
                    "extracted": extracted,
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
                output_data={"json": {}},
                items_processed=0,
                error_message=str(e),
                metadata={"model": model},
            )
        except Exception as e:
            logger.error(f"LLM extract failed: {e}")
            return NodeResult(
                success=False,
                output_data={"json": {}},
                items_processed=0,
                error_message=f"LLM extract failed: {str(e)}",
                metadata={"model": model},
            )


def register():
    from app.nodes.registry import register_node
    register_node(LLMExtractNode())
