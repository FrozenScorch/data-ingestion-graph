from __future__ import annotations

from collections import deque
from collections.abc import Mapping
from typing import Any
from unittest.mock import patch
from urllib.parse import parse_qs, parse_qsl, urlsplit

import httpx
import pytest

from ingestion_graph.conformance import inspect_source_messages
from ingestion_graph.connectors.base import StreamDescriptor
from ingestion_graph.errors import ConfigurationError, ProtocolError
from ingestion_graph.messages import RecordMessage, StateMessage
from ingestion_graph.models import Operation
from ingestion_graph.secrets import EnvSecretProvider, SecretRef
from ingestion_graph.sources import RestSource


class FakeResponse:
    def __init__(
        self,
        payload: Any,
        *,
        status_code: int = 200,
        headers: Mapping[str, str] | None = None,
    ) -> None:
        self.payload = payload
        self.status_code = status_code
        self.headers = dict(headers or {})

    def json(self) -> Any:
        if isinstance(self.payload, Exception):
            raise self.payload
        return self.payload


class FakeClient:
    def __init__(self, responses: list[FakeResponse]) -> None:
        self.responses = deque(responses)
        self.requests: list[tuple[str, str, dict[str, Any]]] = []
        self.closed = False

    async def request(self, method: str, url: str, **kwargs: Any) -> FakeResponse:
        self.requests.append((method, url, kwargs))
        return self.responses.popleft()

    async def aclose(self) -> None:
        self.closed = True


def rest_source(client: Any, **kwargs: Any) -> RestSource:
    options: dict[str, Any] = {
        "stream": "widgets",
        "records_path": "data.items",
        "primary_key": ("id",),
        "client": client,
    }
    options.update(kwargs)
    return RestSource("https://api.example.test", "/v1/widgets", **options)


async def collect(source: RestSource, state: Mapping[str, Any] | None = None) -> list[Any]:
    return [item async for item in source.read(StreamDescriptor("widgets"), state)]


def records(messages: list[Any]) -> list[RecordMessage]:
    return [item for item in messages if isinstance(item, RecordMessage)]


def states(messages: list[Any]) -> list[StateMessage]:
    return [item for item in messages if isinstance(item, StateMessage)]


@pytest.mark.parametrize(
    ("base_url", "allow_http", "message"),
    [
        ("http://api.example.test", True, "loopback"),
        ("http://localhost:8080", False, "allow_http"),
        ("https://user:password@api.example.test", False, "credentials"),
        ("https://api.example.test?api_key=bad", False, "query string"),
    ],
)
def test_rest_rejects_unsafe_base_urls(base_url: str, allow_http: bool, message: str) -> None:
    with pytest.raises(ConfigurationError, match=message):
        RestSource(
            base_url,
            "/items",
            stream="items",
            records_path="items",
            primary_key=("id",),
            allow_http=allow_http,
            client=FakeClient([]),
        )


def test_rest_allows_explicit_loopback_http_but_rejects_url_credentials() -> None:
    source = RestSource(
        "http://127.0.0.1:8080",
        "/items",
        stream="items",
        records_path="items",
        primary_key=("id",),
        allow_http=True,
        client=FakeClient([]),
    )
    assert source.base_url == "http://127.0.0.1:8080"

    with pytest.raises(ConfigurationError, match="SecretRef-backed headers"):
        rest_source(FakeClient([]), query_params={"api_key": "plaintext"})


@pytest.mark.parametrize(
    "name",
    ["token", "refresh_token", "client_secret", "X-Amz-Credential", "accessToken"],
)
def test_rest_rejects_sensitive_query_parameter_names(name: str) -> None:
    with pytest.raises(ConfigurationError, match="SecretRef-backed headers"):
        rest_source(FakeClient([]), query_params={name: "plaintext"})


@pytest.mark.asyncio
async def test_cursor_pagination_emits_checkpoint_after_every_page() -> None:
    client = FakeClient(
        [
            FakeResponse({"data": {"items": [{"id": 1}, {"id": 2}]}, "next": "page-2"}),
            FakeResponse({"data": {"items": [{"id": 3}]}, "next": None}),
        ]
    )
    source = rest_source(
        client,
        pagination="cursor",
        next_cursor_path="next",
        cursor_param="after",
    )

    messages = await collect(source)

    emitted = records(messages)
    checkpoints = states(messages)
    assert [item.envelope.payload.data["id"] for item in emitted] == [1, 2, 3]
    assert all(item.envelope.operation is Operation.SNAPSHOT for item in emitted)
    assert checkpoints[-1].state["cycle"] == 1
    assert checkpoints[-1].state["request_url"] == "https://api.example.test/v1/widgets"
    assert checkpoints[0].state["record_offset"] == 0
    assert parse_qs(urlsplit(client.requests[1][1]).query)["after"] == ["page-2"]
    assert isinstance(messages[2], StateMessage)
    report = inspect_source_messages(source, StreamDescriptor("widgets"), messages)
    assert report.ok, report.issues


@pytest.mark.asyncio
async def test_partial_page_checkpoint_refetches_and_resumes_exact_page() -> None:
    page = {"data": {"items": [{"id": 1}, {"id": 2}, {"id": 3}]}, "next": "done"}
    first = rest_source(
        FakeClient([FakeResponse(page)]),
        pagination="cursor",
        next_cursor_path="next",
        max_records=2,
    )
    first_messages = await collect(first)
    checkpoint = states(first_messages)[-1].state
    assert checkpoint["record_offset"] == 2
    assert "page_fingerprint" in checkpoint

    resumed_client = FakeClient(
        [
            FakeResponse(page),
            FakeResponse({"data": {"items": [{"id": 4}]}, "next": None}),
        ]
    )
    resumed = rest_source(
        resumed_client,
        pagination="cursor",
        next_cursor_path="next",
    )
    resumed_messages = await collect(resumed, checkpoint)

    assert [item.envelope.payload.data["id"] for item in records(resumed_messages)] == [3, 4]
    assert states(resumed_messages)[-1].state["cycle"] == 1
    assert urlsplit(resumed_client.requests[0][1]).query == ""


@pytest.mark.asyncio
async def test_changed_partial_page_fails_closed_before_skipping() -> None:
    original = {"data": {"items": [{"id": 1}, {"id": 2}]}}
    source = rest_source(FakeClient([FakeResponse(original)]), max_records=1)
    checkpoint = states(await collect(source))[-1].state
    changed = rest_source(FakeClient([FakeResponse({"data": {"items": [{"id": 1}, {"id": 999}]}})]))

    with pytest.raises(ProtocolError, match="changed after a partial-page checkpoint"):
        await collect(changed, checkpoint)


@pytest.mark.asyncio
async def test_interrupted_mid_page_replays_same_identity_without_skipping() -> None:
    payload = {"data": {"items": [{"id": 1}, {"id": 2}]}}
    interrupted = rest_source(FakeClient([FakeResponse(payload)]))
    iterator = interrupted.read(StreamDescriptor("widgets"), {})
    first = await anext(iterator)
    assert isinstance(first, RecordMessage)
    await iterator.aclose()

    replayed = await collect(rest_source(FakeClient([FakeResponse(payload)])), {})
    replay_records = records(replayed)
    assert [item.envelope.payload.data["id"] for item in replay_records] == [1, 2]
    assert replay_records[0].envelope.id == first.envelope.id


@pytest.mark.asyncio
async def test_stable_primary_key_produces_exact_identity_replay() -> None:
    payload = {"data": {"items": [{"id": "same", "value": 1}]}}
    first = records(await collect(rest_source(FakeClient([FakeResponse(payload)]))))[0]
    second = records(await collect(rest_source(FakeClient([FakeResponse(payload)]))))[0]

    assert first.envelope.id == second.envelope.id
    assert first.envelope.cursor == second.envelope.cursor
    assert first.envelope.metadata["key"] == {"id": "same"}


@pytest.mark.asyncio
async def test_snapshot_cycle_distinguishes_a_b_a_but_same_cycle_retry_is_stable() -> None:
    state = {}
    emitted = []
    for value in ("A", "B", "A"):
        messages = await collect(
            rest_source(
                FakeClient([FakeResponse({"data": {"items": [{"id": "same", "value": value}]}})])
            ),
            state,
        )
        emitted.append(records(messages)[0].envelope)
        state = states(messages)[-1].state

    assert [item.metadata["snapshot_cycle"] for item in emitted] == [0, 1, 2]
    assert len({item.id for item in emitted}) == 1
    assert emitted[0].to_dict() != emitted[2].to_dict()

    retried = records(
        await collect(
            rest_source(
                FakeClient([FakeResponse({"data": {"items": [{"id": "same", "value": "A"}]}})])
            ),
            {**state, "cycle": 2},
        )
    )[0].envelope
    assert retried.metadata["snapshot_cycle"] == emitted[2].metadata["snapshot_cycle"]
    assert retried.id == emitted[2].id


@pytest.mark.asyncio
async def test_rate_limit_and_server_retries_honor_retry_after() -> None:
    client = FakeClient(
        [
            FakeResponse({}, status_code=429, headers={"retry-after": "2"}),
            FakeResponse({}, status_code=503),
            FakeResponse({"data": {"items": []}}),
        ]
    )
    sleeps: list[float] = []

    async def sleep(delay: float) -> None:
        sleeps.append(delay)

    source = rest_source(client, max_retries=2, sleep=sleep)
    messages = await collect(source)

    assert sleeps == [2.0, 2.0]
    assert states(messages)[-1].state["cycle"] == 1
    assert len(client.requests) == 3


@pytest.mark.asyncio
async def test_bearer_and_api_key_auth_resolve_secret_only_into_headers() -> None:
    provider = EnvSecretProvider({"REST_SECRET": "top-secret"})
    bearer_client = FakeClient([FakeResponse({"data": {"items": []}})])
    bearer = rest_source(
        bearer_client,
        auth_type="bearer",
        secret=SecretRef("REST_SECRET"),
        secret_provider=provider,
    )
    bearer_messages = await collect(bearer)
    assert bearer_client.requests[0][2]["headers"] == {"Authorization": "Bearer top-secret"}
    assert "top-secret" not in repr(bearer)
    assert "top-secret" not in repr(states(bearer_messages))

    key_client = FakeClient([FakeResponse({"data": {"items": []}})])
    key_source = rest_source(
        key_client,
        auth_type="api_key",
        secret=SecretRef("REST_SECRET"),
        api_key_header="X-Service-Key",
        secret_provider=provider,
    )
    await collect(key_source)
    assert key_client.requests[0][2]["headers"] == {"X-Service-Key": "top-secret"}


@pytest.mark.asyncio
async def test_permission_failure_does_not_echo_response_or_secret() -> None:
    source = rest_source(
        FakeClient([FakeResponse({"error": "top-secret"}, status_code=403)]),
        auth_type="bearer",
        secret=SecretRef("REST_SECRET"),
        secret_provider=EnvSecretProvider({"REST_SECRET": "top-secret"}),
    )

    result = await source.check()

    assert result.ok is False
    assert "permission" in result.message
    assert "top-secret" not in result.message


@pytest.mark.asyncio
async def test_link_pagination_accepts_relative_same_origin_next_link() -> None:
    client = FakeClient(
        [
            FakeResponse(
                {"data": {"items": [{"id": 1}]}},
                headers={
                    "link": '</v1/widgets?page=2>; rel="next", </v1/widgets?page=9>; rel="last"'
                },
            ),
            FakeResponse({"data": {"items": [{"id": 2}]}}),
        ]
    )

    messages = await collect(rest_source(client, pagination="link"))

    assert [item.envelope.payload.data["id"] for item in records(messages)] == [1, 2]
    assert client.requests[1][1] == "https://api.example.test/v1/widgets?page=2"


@pytest.mark.asyncio
async def test_cross_origin_next_link_is_rejected_before_request_or_secret_forwarding() -> None:
    client = FakeClient(
        [
            FakeResponse(
                {"data": {"items": [{"id": 1}]}},
                headers={"Link": '<https://attacker.test/items?page=2>; rel="next"'},
            )
        ]
    )

    with pytest.raises(ProtocolError, match="cross-origin"):
        await collect(rest_source(client, pagination="link"))
    assert len(client.requests) == 1


@pytest.mark.parametrize("name", ["client_secret", "refresh_token", "X-Amz-Credential"])
@pytest.mark.asyncio
async def test_sensitive_next_link_query_is_rejected_before_checkpointing(name: str) -> None:
    client = FakeClient(
        [
            FakeResponse(
                {"data": {"items": []}},
                headers={"Link": f'</v1/widgets?{name}=plaintext>; rel="next"'},
            )
        ]
    )

    with pytest.raises(ProtocolError, match="appears to contain credentials"):
        await collect(rest_source(client, pagination="link"))
    assert len(client.requests) == 1


@pytest.mark.asyncio
async def test_injected_redirect_following_client_cannot_forward_auth() -> None:
    requests: list[httpx.Request] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.url.host == "api.example.test":
            return httpx.Response(
                302,
                headers={"Location": "https://attacker.test/collect"},
                request=request,
            )
        return httpx.Response(200, json={"data": {"items": []}}, request=request)

    client = httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
        follow_redirects=True,
    )
    source = rest_source(
        client,
        auth_type="bearer",
        secret=SecretRef("REST_SECRET"),
        secret_provider=EnvSecretProvider({"REST_SECRET": "top-secret"}),
    )

    try:
        with pytest.raises(ConfigurationError, match="HTTP 302"):
            await collect(source)
    finally:
        await client.aclose()

    assert len(requests) == 1
    assert requests[0].url.host == "api.example.test"
    assert requests[0].headers["Authorization"] == "Bearer top-secret"


@pytest.mark.asyncio
async def test_opted_in_cross_origin_link_never_receives_auth_header() -> None:
    client = FakeClient(
        [
            FakeResponse(
                {"data": {"items": [{"id": 1}]}},
                headers={"Link": '<https://cdn.example.test/items?page=2>; rel="next"'},
            ),
            FakeResponse({"data": {"items": [{"id": 2}]}}),
        ]
    )
    source = rest_source(
        client,
        pagination="link",
        allow_cross_origin_next=True,
        auth_type="bearer",
        secret=SecretRef("REST_SECRET"),
        secret_provider=EnvSecretProvider({"REST_SECRET": "top-secret"}),
    )

    await collect(source)

    assert client.requests[0][2]["headers"] == {"Authorization": "Bearer top-secret"}
    assert client.requests[1][2]["headers"] == {}


@pytest.mark.asyncio
async def test_malformed_records_and_primary_key_schema_drift_fail_closed() -> None:
    malformed = rest_source(FakeClient([FakeResponse({"data": {"items": {"id": 1}}})]))
    with pytest.raises(ProtocolError, match="JSON array"):
        await collect(malformed)

    drifting = rest_source(
        FakeClient(
            [
                FakeResponse({"data": {"items": [{"id": 1}]}, "next": "second"}),
                FakeResponse({"data": {"items": [{"id": "1"}]}, "next": None}),
            ]
        ),
        pagination="cursor",
        next_cursor_path="next",
    )
    with pytest.raises(ProtocolError, match="primary key types changed"):
        await collect(drifting)

    field_drift = rest_source(
        FakeClient(
            [
                FakeResponse({"data": {"items": [{"id": 1, "name": "one"}]}, "next": "second"}),
                FakeResponse({"data": {"items": [{"id": 2, "name": 2}]}, "next": None}),
            ]
        ),
        pagination="cursor",
        next_cursor_path="next",
    )
    with pytest.raises(ProtocolError, match=r"field '/name'.*between pages"):
        await collect(field_drift)


@pytest.mark.asyncio
async def test_discover_infers_sample_schema_without_creating_checkpoint() -> None:
    client = FakeClient(
        [
            FakeResponse(
                {
                    "data": {
                        "items": [
                            {"id": 1, "name": "one", "optional": None},
                            {"id": 2, "name": "two"},
                        ]
                    }
                }
            )
        ]
    )
    source = rest_source(client)

    descriptor = (await source.discover())[0]

    assert descriptor.primary_key == ("id",)
    assert descriptor.json_schema["properties"]["id"] == {"type": "integer"}
    assert descriptor.json_schema["properties"]["optional"] == {"type": "null"}
    assert descriptor.json_schema["required"] == ["id", "name"]
    assert len(client.requests) == 1


@pytest.mark.asyncio
async def test_checkpoint_fingerprint_and_missing_optional_dependency_fail_safely() -> None:
    configured = rest_source(FakeClient([]))
    with pytest.raises(ConfigurationError, match="does not match"):
        await collect(
            configured,
            {
                "version": 1,
                "configuration_fingerprint": "0" * 64,
                "request_url": "https://api.example.test/v1/widgets",
                "cycle": 0,
                "record_offset": 0,
            },
        )

    without_client = RestSource(
        "https://api.example.test",
        "/items",
        stream="items",
        records_path="items",
        primary_key=("id",),
    )
    with patch("ingestion_graph.sources.rest.httpx", None):
        result = await without_client.check()
    assert result.ok is False
    assert "ingestion-graph[rest]" in result.message


@pytest.mark.asyncio
async def test_max_pages_stops_on_a_durable_next_request_checkpoint() -> None:
    client = FakeClient([FakeResponse({"data": {"items": [{"id": 1}]}, "next": "second"})])
    source = rest_source(
        client,
        pagination="cursor",
        next_cursor_path="next",
        max_pages=1,
    )

    messages = await collect(source)

    checkpoint = states(messages)[-1].state
    assert parse_qs(urlsplit(checkpoint["request_url"]).query)["cursor"] == ["second"]
    assert len(client.requests) == 1


@pytest.mark.asyncio
async def test_pagination_loop_is_detected_across_max_pages_runs() -> None:
    endpoint = "https://api.example.test/v1/widgets"
    first = rest_source(
        FakeClient(
            [
                FakeResponse(
                    {"data": {"items": []}},
                    headers={"Link": '</v1/widgets?page=2>; rel="next"'},
                )
            ]
        ),
        pagination="link",
        max_pages=1,
    )
    checkpoint = states(await collect(first))[-1].state
    assert checkpoint["completed_request_hashes"]

    second = rest_source(
        FakeClient(
            [
                FakeResponse(
                    {"data": {"items": []}},
                    headers={"Link": f'<{endpoint}>; rel="next"'},
                )
            ]
        ),
        pagination="link",
        max_pages=1,
    )
    with pytest.raises(ProtocolError, match="repeated a request URL"):
        await collect(second, checkpoint)


@pytest.mark.asyncio
async def test_cursor_pagination_preserves_blank_query_parameters() -> None:
    client = FakeClient(
        [
            FakeResponse({"data": {"items": []}, "next": "second"}),
            FakeResponse({"data": {"items": []}, "next": None}),
        ]
    )
    source = rest_source(
        client,
        pagination="cursor",
        next_cursor_path="next",
        query_params={"include": None},
    )

    await collect(source)

    assert ("include", "") in parse_qsl(
        urlsplit(client.requests[1][1]).query,
        keep_blank_values=True,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "invalid_hashes",
    [
        "not-a-list",
        ["not-a-hash"],
        ["0" * 64, "0" * 64],
    ],
)
async def test_checkpoint_rejects_invalid_completed_request_hashes(
    invalid_hashes: Any,
) -> None:
    original = rest_source(FakeClient([FakeResponse({"data": {"items": []}})]))
    checkpoint = states(await collect(original))[-1].state
    checkpoint["completed_request_hashes"] = invalid_hashes

    with pytest.raises(ConfigurationError, match="completed_request_hashes"):
        await collect(rest_source(FakeClient([])), checkpoint)
