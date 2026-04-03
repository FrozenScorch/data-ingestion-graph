"""
HttpRequest node: send HTTP requests.
"""
import json
import logging
from typing import Any

import httpx

from app.nodes.base import BaseNode, NodeContext, NodeResult, PortDef, PortDataType

logger = logging.getLogger(__name__)


class HttpRequestNode(BaseNode):
    @property
    def node_type(self) -> str:
        return "http_request"

    @property
    def display_name(self) -> str:
        return "HTTP Request"

    @property
    def category(self) -> str:
        return "output"

    @property
    def description(self) -> str:
        return "Send HTTP requests to external APIs"

    @property
    def inputs(self) -> list[PortDef]:
        return [PortDef(name="data", data_type=PortDataType.ANY, required=False)]

    @property
    def outputs(self) -> list[PortDef]:
        return [PortDef(name="json", data_type=PortDataType.JSON, label="Response")]

    @property
    def config_schema(self) -> dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "url": {"type": "string", "format": "uri", "description": "Target URL to send the request to"},
                "method": {"type": "string", "enum": ["GET", "POST", "PUT", "DELETE", "PATCH"], "default": "POST", "description": "HTTP method"},
                "headers": {"type": "string", "format": "textarea", "default": "{}", "description": "HTTP headers as JSON object"},
                "body": {"type": "string", "format": "textarea", "description": "Request body (text or JSON)"},
                "timeout": {"type": "integer", "description": "Request timeout in seconds", "default": 30, "minimum": 1, "maximum": 300},
            },
            "required": ["url"],
        }

    @staticmethod
    def _parse_json_field(value: Any) -> Any:
        """Try to parse a string value as JSON. Returns the original value if parsing fails."""
        if isinstance(value, str):
            try:
                return json.loads(value)
            except (json.JSONDecodeError, ValueError):
                return value
        return value

    async def execute(self, context: NodeContext) -> NodeResult:
        config = context.config

        # Parse headers: if string, try JSON parse; fall back to plain string
        headers = self._parse_json_field(config.get("headers", "{}"))
        if isinstance(headers, str):
            headers = {}
            logger.warning("headers could not be parsed as JSON, using empty dict")

        # Parse body: if string, try JSON parse; fall back to plain string
        body = self._parse_json_field(config.get("body"))

        url = config.get("url", "")
        method = (config.get("method") or "POST").upper()
        timeout = float(config.get("timeout", 30))

        if not url:
            return NodeResult(
                success=False,
                output_data={"json": {}},
                items_processed=0,
                error_message="URL is required",
            )

        # Build request keyword arguments based on method
        req_kwargs: dict[str, Any] = {"headers": headers}
        if method in ("POST", "PUT", "PATCH") and body is not None:
            if isinstance(body, (dict, list)):
                req_kwargs["json"] = body
            else:
                req_kwargs["content"] = str(body)

        try:
            async with httpx.AsyncClient(timeout=timeout) as client:
                response = await client.request(method, url, **req_kwargs)
                response.raise_for_status()

            # Try to parse response body as JSON; fall back to raw text
            content_type = response.headers.get("content-type", "")
            if "application/json" in content_type:
                response_data = response.json()
            else:
                try:
                    response_data = response.json()
                except (json.JSONDecodeError, ValueError):
                    response_data = {"raw_text": response.text}

            return NodeResult(
                success=True,
                output_data={"json": response_data},
                items_processed=1,
                metadata={
                    "status_code": response.status_code,
                    "url": str(response.url),
                    "method": method,
                    "content_type": content_type,
                },
            )

        except httpx.HTTPStatusError as e:
            error_msg = f"HTTP {e.response.status_code}: {e.response.text[:500]}"
            logger.error(f"HTTP request failed with status {e.response.status_code}: {url}")
            return NodeResult(
                success=False,
                output_data={"json": {"error": error_msg, "status_code": e.response.status_code}},
                items_processed=0,
                error_message=error_msg,
            )
        except httpx.TimeoutException as e:
            error_msg = f"Request timed out after {timeout}s: {str(e)}"
            logger.error(f"HTTP request timed out: {url}")
            return NodeResult(
                success=False,
                output_data={"json": {"error": error_msg}},
                items_processed=0,
                error_message=error_msg,
            )
        except httpx.RequestError as e:
            error_msg = f"Request failed: {str(e)}"
            logger.error(f"HTTP request error for {url}: {e}")
            return NodeResult(
                success=False,
                output_data={"json": {"error": error_msg}},
                items_processed=0,
                error_message=error_msg,
            )
        except Exception as e:
            error_msg = f"Unexpected error: {str(e)}"
            logger.exception(f"Unexpected error in HttpRequestNode: {e}")
            return NodeResult(
                success=False,
                output_data={"json": {"error": error_msg}},
                items_processed=0,
                error_message=error_msg,
            )


def register():
    from app.nodes.registry import register_node
    register_node(HttpRequestNode())
