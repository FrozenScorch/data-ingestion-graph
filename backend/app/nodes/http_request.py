"""
HttpRequest node: send HTTP requests.
"""

import json
import logging
from collections.abc import Callable, Mapping
from typing import Any
from urllib.parse import urljoin

import httpx
from app.config import settings
from app.nodes.base import BaseNode, NodeContext, NodeResult, PortDataType, PortDef
from app.services.egress_policy import (
    EgressPolicy,
    EgressPolicyError,
    ValidatedTarget,
    create_pinned_http_client,
)

logger = logging.getLogger(__name__)


class HttpRequestNode(BaseNode):
    def __init__(
        self,
        *,
        egress_policy: EgressPolicy | None = None,
        client_factory: Callable[[ValidatedTarget, float], Any] = create_pinned_http_client,
        max_redirects: int | None = None,
    ) -> None:
        self._egress_policy = egress_policy
        self._client_factory = client_factory
        self._max_redirects = max_redirects

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
                "url": {
                    "type": "string",
                    "format": "uri",
                    "description": "Target URL to send the request to",
                },
                "method": {
                    "type": "string",
                    "enum": ["GET", "POST", "PUT", "DELETE", "PATCH"],
                    "default": "POST",
                    "description": "HTTP method",
                },
                "headers": {
                    "type": "string",
                    "format": "textarea",
                    "default": "{}",
                    "description": "HTTP headers as JSON object",
                },
                "body": {
                    "type": "string",
                    "format": "textarea",
                    "description": "Request body (text or JSON)",
                },
                "timeout": {
                    "type": "integer",
                    "description": "Request timeout in seconds",
                    "default": 30,
                    "minimum": 1,
                    "maximum": 300,
                },
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
        if not isinstance(headers, Mapping):
            headers = {}
            logger.warning("headers could not be parsed as JSON, using empty dict")
        elif any(
            not isinstance(key, str)
            or not isinstance(value, str)
            or "\r" in key
            or "\n" in key
            or "\r" in value
            or "\n" in value
            for key, value in headers.items()
        ):
            return self._error("HTTP headers must be a string-to-string JSON object")
        elif any(key.lower() == "host" for key in headers):
            return self._error("The Host header is controlled by outbound policy")
        else:
            headers = dict(headers)

        # Parse body: if string, try JSON parse; fall back to plain string
        body = self._parse_json_field(config.get("body"))

        url = config.get("url", "")
        method = (config.get("method") or "POST").upper()
        try:
            timeout = float(config.get("timeout", 30))
        except (TypeError, ValueError, OverflowError):
            return self._error("Request timeout must be a number")

        if not url:
            return self._error("URL is required")
        if method not in {"GET", "POST", "PUT", "DELETE", "PATCH"}:
            return self._error("HTTP method is not supported")
        if not 1 <= timeout <= 300:
            return self._error("Request timeout must be between 1 and 300 seconds")

        # Build request keyword arguments based on method
        req_kwargs: dict[str, Any] = {"headers": headers}
        if method in ("POST", "PUT", "PATCH") and body is not None:
            if isinstance(body, (dict, list)):
                req_kwargs["json"] = body
            else:
                req_kwargs["content"] = str(body)

        try:
            policy = self._egress_policy or EgressPolicy.from_settings()
            target = await policy.validate_url(url)
            max_redirects = (
                settings.egress_max_redirects
                if self._max_redirects is None
                else self._max_redirects
            )
            redirect_count = 0
            while True:
                async with self._client_factory(target, timeout) as client:
                    response = await client.request(
                        method,
                        target.url,
                        follow_redirects=False,
                        **req_kwargs,
                    )
                if response.status_code not in {301, 302, 303, 307, 308}:
                    break
                if method != "GET":
                    raise EgressPolicyError("Redirects are allowed only for GET requests")
                if redirect_count >= max_redirects:
                    raise EgressPolicyError("Outbound redirect limit was exceeded")
                location = response.headers.get("location")
                if not isinstance(location, str) or not location:
                    raise EgressPolicyError("Outbound redirect is missing a Location header")
                redirected = await policy.validate_url(urljoin(target.url, location))
                if redirected.safe_origin != target.safe_origin:
                    raise EgressPolicyError("Cross-origin outbound redirects are blocked")
                target = redirected
                redirect_count += 1
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
                    "url": target.safe_origin,
                    "method": method,
                    "content_type": content_type,
                    "redirects_followed": redirect_count,
                },
            )

        except EgressPolicyError as exc:
            logger.warning("Outbound HTTP request blocked by egress policy")
            return self._error(str(exc))
        except httpx.HTTPStatusError as exc:
            error_msg = f"HTTP request returned status {exc.response.status_code}"
            logger.error("Outbound HTTP request returned status %s", exc.response.status_code)
            return NodeResult(
                success=False,
                output_data={"json": {"error": error_msg, "status_code": exc.response.status_code}},
                items_processed=0,
                error_message=error_msg,
            )
        except httpx.TimeoutException:
            logger.error("Outbound HTTP request timed out")
            return self._error(f"Request timed out after {timeout:g} seconds")
        except httpx.RequestError as exc:
            logger.error("Outbound HTTP request failed (%s)", type(exc).__name__)
            return self._error(f"HTTP request failed ({type(exc).__name__})")
        except Exception as exc:
            logger.error("Unexpected outbound HTTP failure (%s)", type(exc).__name__)
            return self._error(f"Unexpected HTTP request failure ({type(exc).__name__})")

    @staticmethod
    def _error(message: str) -> NodeResult:
        return NodeResult(
            success=False,
            output_data={"json": {"error": message}},
            items_processed=0,
            error_message=message,
        )


def register():
    from app.nodes.registry import register_node

    register_node(HttpRequestNode())
