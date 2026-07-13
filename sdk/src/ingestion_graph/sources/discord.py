"""Resumable Discord channel history source using the official REST API."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Mapping, Sequence
from datetime import datetime
from typing import Any
from urllib.parse import urlparse

from ingestion_graph.connectors.base import (
    CheckResult,
    ConnectorCapabilities,
    ConnectorSpec,
    Source,
    StreamDescriptor,
)
from ingestion_graph.errors import (
    AuthenticationError,
    ConfigurationError,
    PermissionDeniedError,
    RateLimitError,
)
from ingestion_graph.messages import LogMessage, RecordMessage, SourceMessage, StateMessage
from ingestion_graph.models import Envelope, Operation, RecordPayload, stable_record_id
from ingestion_graph.secrets import EnvSecretProvider, SecretProvider, SecretRef

try:
    import httpx
except ImportError:  # pragma: no cover - exercised when optional extra is absent
    httpx = None  # type: ignore[assignment]


DISCORD_API_BASE = "https://discord.com/api/v10"


class DiscordSource(Source):
    """Read one or more Discord channels with page-level resumability.

    Discord returns channel messages newest-first. To avoid skipping records, the
    connector stores a continuation cursor while walking backwards to the prior
    high-water mark, then atomically promotes the newest observed snowflake.
    """

    def __init__(
        self,
        channel_ids: Sequence[str],
        token: SecretRef,
        *,
        secret_provider: SecretProvider | None = None,
        api_base: str = DISCORD_API_BASE,
        page_size: int = 100,
        max_retries: int = 5,
        client: Any = None,
    ) -> None:
        if not channel_ids or any(not str(item).isdigit() for item in channel_ids):
            raise ConfigurationError("Discord channel_ids must contain numeric snowflake IDs")
        if not 1 <= page_size <= 100:
            raise ConfigurationError("Discord page_size must be between 1 and 100")
        parsed_api = urlparse(api_base)
        if parsed_api.scheme != "https" or parsed_api.hostname not in {
            "discord.com",
            "discordapp.com",
        }:
            raise ConfigurationError("Discord api_base must use HTTPS on an official Discord host")
        if max_retries < 0:
            raise ConfigurationError("Discord max_retries must not be negative")
        self.channel_ids = tuple(str(item) for item in channel_ids)
        self.token_ref = token
        self.secret_provider = secret_provider or EnvSecretProvider()
        self.api_base = api_base.rstrip("/")
        self.page_size = page_size
        self.max_retries = max_retries
        self._client = client
        self._owns_client = client is None

    @classmethod
    def manifest(cls) -> ConnectorSpec:
        return ConnectorSpec(
            name="discord",
            version="1.0.0",
            config_schema={
                "type": "object",
                "properties": {
                    "channel_ids": {
                        "type": "array",
                        "items": {"type": "string", "pattern": "^[0-9]+$"},
                        "minItems": 1,
                    },
                    "token": {"type": "string", "format": "secret-ref"},
                },
                "required": ["channel_ids", "token"],
            },
            capabilities=ConnectorCapabilities(
                incremental=True,
                resumable_full_refresh=True,
                deletes=False,
                schema_discovery=True,
                rate_limits=True,
            ),
        )

    def spec(self) -> ConnectorSpec:
        return self.manifest()

    async def _get_client(self) -> Any:
        if self._client is None:
            if httpx is None:
                raise ConfigurationError(
                    "Discord support requires the optional dependency: "
                    "pip install 'ingestion-graph[discord]'"
                )
            token = self.secret_provider.resolve(self.token_ref)
            self._client = httpx.AsyncClient(
                base_url=self.api_base,
                headers={
                    "Authorization": f"Bot {token}",
                    "User-Agent": "ingestion-graph/0.2 (+https://github.com/FrozenScorch/data-ingestion-graph)",
                },
                timeout=30,
            )
        return self._client

    async def check(self) -> CheckResult:
        try:
            await self._request("GET", "/users/@me")
            return CheckResult(True, "Discord credentials are valid")
        except (AuthenticationError, PermissionDeniedError, ConfigurationError) as exc:
            return CheckResult(False, str(exc))

    async def discover(self) -> Sequence[StreamDescriptor]:
        return [
            StreamDescriptor(
                name=channel_id,
                namespace="discord.channel",
                primary_key=("id",),
                cursor_field=("id",),
                json_schema={
                    "type": "object",
                    "required": ["id", "channel_id", "author", "timestamp"],
                    "properties": {
                        "id": {"type": "string"},
                        "channel_id": {"type": "string"},
                        "content": {"type": "string"},
                        "author": {"type": "object"},
                        "timestamp": {"type": "string", "format": "date-time"},
                        "edited_timestamp": {
                            "type": ["string", "null"],
                            "format": "date-time",
                        },
                        "attachments": {"type": "array"},
                    },
                },
            )
            for channel_id in self.channel_ids
        ]

    async def read(
        self,
        stream: StreamDescriptor,
        state: Mapping[str, Any] | None = None,
    ) -> AsyncIterator[SourceMessage]:
        if stream.name not in self.channel_ids:
            raise ConfigurationError(f"Discord channel {stream.name!r} is not configured")

        current = dict(state or {})
        floor_id = str(current.get("floor_id") or current.get("last_message_id") or "0")
        before_id = current.get("before")
        high_watermark = current.get("high_watermark")
        mode = str(current.get("mode") or ("catchup" if floor_id != "0" else "backfill"))

        while True:
            params: dict[str, Any] = {"limit": self.page_size}
            if before_id:
                params["before"] = str(before_id)
            page = await self._request("GET", f"/channels/{stream.name}/messages", params=params)
            if not isinstance(page, list):
                raise ConfigurationError("Discord returned a non-list channel history response")

            if not page:
                final_cursor = str(high_watermark or floor_id)
                yield StateMessage(stream.name, {"last_message_id": final_cursor})
                return

            page_ids = [str(message["id"]) for message in page]
            if high_watermark is None:
                high_watermark = str(max(int(item) for item in page_ids))

            eligible = [message for message in page if int(str(message["id"])) > int(floor_id)]
            for message in sorted(eligible, key=lambda item: int(str(item["id"]))):
                yield RecordMessage(self._to_envelope(stream.name, message))

            oldest_id = str(min(int(item) for item in page_ids))
            reached_floor = floor_id != "0" and int(oldest_id) <= int(floor_id)
            reached_end = len(page) < self.page_size
            if reached_floor or reached_end:
                yield StateMessage(
                    stream.name, {"last_message_id": str(high_watermark or floor_id)}
                )
                return

            before_id = oldest_id
            yield StateMessage(
                stream.name,
                {
                    "mode": mode,
                    "before": before_id,
                    "floor_id": floor_id,
                    "high_watermark": str(high_watermark),
                },
            )
            yield LogMessage(
                "debug",
                "Discord page checkpoint committed",
                {"channel_id": stream.name, "before": before_id, "mode": mode},
            )

    def _to_envelope(self, channel_id: str, message: Mapping[str, Any]) -> Envelope:
        message_id = str(message["id"])
        event_time = _parse_timestamp(message.get("timestamp"))
        return Envelope(
            id=stable_record_id("discord", channel_id, message_id),
            source="discord",
            stream=channel_id,
            operation=Operation.UPSERT,
            cursor=message_id,
            event_time=event_time,
            payload=RecordPayload(dict(message)),
            metadata={
                "native_id": message_id,
                "channel_id": channel_id,
                "edited": bool(message.get("edited_timestamp")),
                "attachment_count": len(message.get("attachments") or []),
            },
            provenance={
                "connector": "discord",
                "endpoint": f"/channels/{channel_id}/messages",
            },
        )

    async def _request(self, method: str, path: str, **kwargs: Any) -> Any:
        client = await self._get_client()
        for attempt in range(self.max_retries + 1):
            response = await client.request(method, path, **kwargs)
            if response.status_code == 401:
                raise AuthenticationError("Discord token is invalid or expired")
            if response.status_code == 403:
                raise PermissionDeniedError(
                    "Discord bot needs VIEW_CHANNEL and READ_MESSAGE_HISTORY permissions"
                )
            if response.status_code == 429:
                if attempt >= self.max_retries:
                    raise RateLimitError("Discord rate limit retry budget exhausted")
                body = response.json()
                delay = float(response.headers.get("Retry-After") or body.get("retry_after") or 1)
                await asyncio.sleep(max(0.0, delay))
                continue
            if 500 <= response.status_code < 600:
                if attempt >= self.max_retries:
                    response.raise_for_status()
                await asyncio.sleep(min(2**attempt, 30))
                continue
            response.raise_for_status()
            return response.json()
        raise RateLimitError("Discord request retry budget exhausted")

    async def close(self) -> None:
        if self._owns_client and self._client is not None:
            await self._client.aclose()
            self._client = None


def _parse_timestamp(value: Any) -> datetime | None:
    if not value or not isinstance(value, str):
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00"))
