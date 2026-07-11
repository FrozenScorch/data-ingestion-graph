from collections import deque

import pytest

from ingestion_graph.connectors.base import StreamDescriptor
from ingestion_graph.messages import RecordMessage, StateMessage
from ingestion_graph.secrets import EnvSecretProvider, SecretRef
from ingestion_graph.sources import DiscordSource


def message(message_id: int):
    return {
        "id": str(message_id),
        "channel_id": "123",
        "content": f"message {message_id}",
        "author": {"id": "7", "username": "person"},
        "timestamp": "2026-01-01T00:00:00+00:00",
        "edited_timestamp": None,
        "attachments": [],
    }


class FakeResponse:
    def __init__(self, payload, status_code=200, headers=None):
        self.payload = payload
        self.status_code = status_code
        self.headers = headers or {}

    def json(self):
        return self.payload

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


class FakeClient:
    def __init__(self, responses):
        self.responses = deque(responses)
        self.requests = []

    async def request(self, method, path, **kwargs):
        self.requests.append((method, path, kwargs))
        return self.responses.popleft()


@pytest.mark.asyncio
async def test_discord_backfill_is_page_checkpointed_and_promotes_high_watermark():
    client = FakeClient(
        [
            FakeResponse([message(5), message(4)]),
            FakeResponse([message(3)]),
        ]
    )
    source = DiscordSource(
        ["123"],
        SecretRef("DISCORD_TOKEN"),
        secret_provider=EnvSecretProvider({"DISCORD_TOKEN": "secret"}),
        page_size=2,
        client=client,
    )
    emitted = [item async for item in source.read(StreamDescriptor("123"), {})]
    records = [item for item in emitted if isinstance(item, RecordMessage)]
    checkpoints = [item for item in emitted if isinstance(item, StateMessage)]
    assert [record.envelope.cursor for record in records] == ["4", "5", "3"]
    assert checkpoints[0].state == {
        "mode": "backfill",
        "before": "4",
        "floor_id": "0",
        "high_watermark": "5",
    }
    assert checkpoints[-1].state == {"last_message_id": "5"}
    assert client.requests[1][2]["params"]["before"] == "4"


@pytest.mark.asyncio
async def test_discord_resume_continues_backfill_without_restarting_newest_page():
    client = FakeClient([FakeResponse([message(3)])])
    source = DiscordSource(["123"], SecretRef("TOKEN"), page_size=2, client=client)
    state = {
        "mode": "backfill",
        "before": "4",
        "floor_id": "0",
        "high_watermark": "5",
    }
    emitted = [item async for item in source.read(StreamDescriptor("123"), state)]
    assert client.requests[0][2]["params"]["before"] == "4"
    assert [item.state for item in emitted if isinstance(item, StateMessage)] == [
        {"last_message_id": "5"}
    ]


@pytest.mark.asyncio
async def test_discord_incremental_stops_when_prior_cursor_is_reached():
    client = FakeClient([FakeResponse([message(7), message(6), message(5)])])
    source = DiscordSource(["123"], SecretRef("TOKEN"), page_size=3, client=client)
    emitted = [
        item async for item in source.read(StreamDescriptor("123"), {"last_message_id": "5"})
    ]
    assert [item.envelope.cursor for item in emitted if isinstance(item, RecordMessage)] == [
        "6",
        "7",
    ]
    assert [item.state for item in emitted if isinstance(item, StateMessage)] == [
        {"last_message_id": "7"}
    ]


@pytest.mark.asyncio
async def test_discord_honors_429_retry_after_without_losing_page():
    client = FakeClient(
        [
            FakeResponse({"retry_after": 0}, status_code=429),
            FakeResponse([message(1)]),
        ]
    )
    source = DiscordSource(["123"], SecretRef("TOKEN"), page_size=2, client=client)
    emitted = [item async for item in source.read(StreamDescriptor("123"), {})]
    assert len(client.requests) == 2
    assert [item.envelope.cursor for item in emitted if isinstance(item, RecordMessage)] == ["1"]
