from __future__ import annotations

import asyncio
import gzip
import ipaddress
import json
import logging
import socket
import threading
import time
from collections import deque
from collections.abc import Mapping, Sequence
from typing import Any
from unittest.mock import MagicMock, patch

import app.nodes.http_request as http_request_module
import httpcore
import httpx
import pytest
from app.config import Settings, settings
from app.nodes.base import NodeContext
from app.nodes.http_request import HttpRequestNode
from app.services.connection_service import test_connection as check_connection
from app.services.egress_policy import (
    EgressPolicy,
    EgressPolicyError,
    ValidatedTarget,
    _PinnedNetworkBackend,
    _PinnedResponseStream,
)


class Resolver:
    def __init__(self, answers: Mapping[str, Sequence[str]]) -> None:
        self.answers = answers
        self.calls: list[tuple[str, int]] = []

    async def __call__(self, host: str, port: int) -> Sequence[str]:
        self.calls.append((host, port))
        return self.answers.get(host, ())


class FakeResponse:
    def __init__(
        self,
        status_code: int = 200,
        *,
        headers: Mapping[str, str] | None = None,
        payload: Any = None,
        text: str = "",
    ) -> None:
        self.status_code = status_code
        self.headers = dict(headers or {})
        self._payload = {} if payload is None else payload
        self.text = text
        self.content = text.encode() if text else json.dumps(self._payload).encode()

    def json(self) -> Any:
        return self._payload

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            request = httpx.Request("GET", "https://redacted.invalid/")
            response = httpx.Response(self.status_code, request=request, text=self.text)
            raise httpx.HTTPStatusError(
                "sensitive upstream text", request=request, response=response
            )

    async def __aenter__(self) -> FakeResponse:
        return self

    async def __aexit__(self, *_args: Any) -> None:
        return None

    async def aiter_bytes(self):
        yield self.content


class FakeClient:
    def __init__(self, response: FakeResponse, requests: list[dict[str, Any]]) -> None:
        self.response = response
        self.requests = requests

    async def __aenter__(self) -> FakeClient:
        return self

    async def __aexit__(self, *_args: Any) -> None:
        return None

    async def request(self, method: str, url: str, **kwargs: Any) -> FakeResponse:
        self.requests.append({"method": method, "url": url, **kwargs})
        return self.response

    def stream(self, method: str, url: str, **kwargs: Any) -> FakeResponse:
        self.requests.append({"method": method, "url": url, **kwargs})
        return self.response

    async def get(self, url: str, **kwargs: Any) -> FakeResponse:
        self.requests.append({"method": "GET", "url": url, **kwargs})
        return self.response


class ClientFactory:
    def __init__(self, responses: Sequence[FakeResponse]) -> None:
        self.responses = deque(responses)
        self.targets: list[ValidatedTarget] = []
        self.requests: list[dict[str, Any]] = []

    def __call__(self, target: ValidatedTarget, _timeout: float) -> FakeClient:
        self.targets.append(target)
        return FakeClient(self.responses.popleft(), self.requests)


def context(url: str, **config: Any) -> NodeContext:
    return NodeContext(
        run_id="run",
        node_id="http",
        config={"url": url, "method": "GET", **config},
    )


def test_settings_parse_exact_host_and_cidr_allowlists() -> None:
    configured = Settings(
        _env_file=None,
        egress_policy_mode="allowlist-only",
        egress_allowed_hosts="db.lan, nas.lan ",
        egress_allowed_cidrs="10.0.0.0/24, fd00::/64",
        egress_max_redirects=2,
    )

    assert configured.egress_allowed_hosts_list == ["db.lan", "nas.lan"]
    assert configured.egress_allowed_cidrs_list == ["10.0.0.0/24", "fd00::/64"]
    assert configured.egress_max_redirects == 2
    assert configured.egress_max_response_bytes == 10 * 1024 * 1024


@pytest.mark.asyncio
async def test_public_ipv4_and_ipv6_literals_are_normalized_without_dns() -> None:
    resolver = Resolver({})
    policy = EgressPolicy(resolver=resolver)

    ipv4 = await policy.validate_url("HTTPS://93.184.216.34:443/items?q=1")
    ipv6 = await policy.validate_url("https://[2606:4700:4700::1111]/dns")

    assert ipv4.url == "https://93.184.216.34/items?q=1"
    assert ipv4.addresses == (ipaddress.ip_address("93.184.216.34"),)
    assert ipv6.host == "2606:4700:4700::1111"
    assert resolver.calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "address",
    [
        "127.0.0.1",
        "10.0.0.1",
        "169.254.1.1",
        "224.0.0.1",
        "0.0.0.0",
        "240.0.0.1",
        "100.64.0.1",
        "::1",
        "fe80::1",
        "fec0::1",
        "ff02::1",
        "::",
    ],
)
async def test_restricted_address_classes_are_blocked_by_default(address: str) -> None:
    rendered = f"[{address}]" if ":" in address else address
    with pytest.raises(EgressPolicyError, match="address is blocked"):
        await EgressPolicy().validate_url(f"http://{rendered}/")


@pytest.mark.asyncio
async def test_mapped_ipv6_is_checked_as_ipv4_and_metadata_is_always_blocked() -> None:
    with pytest.raises(EgressPolicyError, match="address is blocked"):
        await EgressPolicy().validate_url("http://[::ffff:127.0.0.1]/")
    with pytest.raises(EgressPolicyError, match="metadata"):
        await EgressPolicy(allowed_cidrs=("169.254.0.0/16",)).validate_url(
            "http://169.254.169.254/latest/meta-data"
        )
    with pytest.raises(EgressPolicyError, match="metadata"):
        await EgressPolicy().validate_url("http://metadata.google.internal/")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "address",
    [
        "169.254.170.23",
        "168.63.129.16",
        "fd00:ec2::23",
        "fd20:ce::254",
    ],
)
async def test_additional_cloud_identity_endpoints_cannot_be_allowlisted(address: str) -> None:
    rendered = f"[{address}]" if ":" in address else address
    policy = EgressPolicy(allowed_cidrs=(f"{address}/128" if ":" in address else f"{address}/32",))

    with pytest.raises(EgressPolicyError, match="metadata"):
        await policy.validate_url(f"http://{rendered}/")


@pytest.mark.asyncio
@pytest.mark.parametrize("host", ["metadata", "metadata.goog"])
async def test_cloud_metadata_aliases_are_blocked_before_dns(host: str) -> None:
    resolver = Resolver({host: ("93.184.216.34",)})

    with pytest.raises(EgressPolicyError, match="metadata"):
        await EgressPolicy(allowed_hosts=(host,), resolver=resolver).validate_url(f"http://{host}/")
    assert resolver.calls == []


@pytest.mark.asyncio
async def test_mixed_public_private_dns_is_blocked_even_for_allowlisted_host() -> None:
    resolver = Resolver({"mixed.example": ("93.184.216.34", "10.1.2.3")})
    policy = EgressPolicy(allowed_hosts=("mixed.example",), resolver=resolver)

    with pytest.raises(EgressPolicyError, match="Mixed public and restricted"):
        await policy.validate_url("https://mixed.example/data")


@pytest.mark.asyncio
async def test_mixed_public_and_site_local_ipv6_dns_is_blocked() -> None:
    resolver = Resolver({"mixed.example": ("2606:4700:4700::1111", "fec0::1")})

    with pytest.raises(EgressPolicyError, match="Mixed public and restricted"):
        await EgressPolicy(resolver=resolver).validate_url("https://mixed.example/data")


@pytest.mark.asyncio
async def test_all_public_a_and_aaaa_answers_are_retained_for_pinning() -> None:
    resolver = Resolver({"dual.example": ("93.184.216.34", "2606:4700:4700::1111")})

    target = await EgressPolicy(resolver=resolver).validate_url("https://dual.example/")

    assert target.addresses == (
        ipaddress.ip_address("93.184.216.34"),
        ipaddress.ip_address("2606:4700:4700::1111"),
    )


@pytest.mark.asyncio
async def test_exact_host_and_cidr_allowlists_enable_intentional_lan_targets() -> None:
    resolver = Resolver(
        {
            "db.lan": ("10.20.30.40",),
            "other.lan": ("192.168.50.8",),
        }
    )
    policy = EgressPolicy(
        allowed_hosts=("DB.LAN.",),
        allowed_cidrs=("192.168.50.0/24",),
        resolver=resolver,
    )

    exact = await policy.validate_host("db.lan.", 5432)
    cidr = await policy.validate_url("http://other.lan:8080/health")

    assert str(exact.addresses[0]) == "10.20.30.40"
    assert str(cidr.addresses[0]) == "192.168.50.8"


@pytest.mark.asyncio
async def test_allowlist_only_mode_blocks_unlisted_public_destinations() -> None:
    policy = EgressPolicy(mode="allowlist-only")
    with pytest.raises(EgressPolicyError, match="not allowlisted"):
        await policy.validate_url("https://93.184.216.34/")


@pytest.mark.asyncio
async def test_idna_trailing_dot_and_default_port_share_one_canonical_target() -> None:
    resolver = Resolver({"xn--bcher-kva.example": ("93.184.216.34",)})
    policy = EgressPolicy(resolver=resolver)

    target = await policy.validate_url("https://BÜCHER.example.:443/path")

    assert target.host == "xn--bcher-kva.example"
    assert target.url == "https://xn--bcher-kva.example/path"
    assert resolver.calls == [("xn--bcher-kva.example", 443)]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "url",
    [
        "file:///etc/passwd",
        "https://user:secret@example.com/",
        "https:///missing-host",
        "https://example.com:0/",
        "https://example.com:99999/",
        "https://[not-ipv6]/",
        "https://example.com/path#fragment",
    ],
)
async def test_malformed_and_credential_urls_fail_without_echoing_input(url: str) -> None:
    with pytest.raises(EgressPolicyError) as exc_info:
        await EgressPolicy().validate_url(url)
    assert "secret" not in str(exc_info.value)
    assert url not in str(exc_info.value)


@pytest.mark.asyncio
async def test_pinned_backend_connects_to_validated_ip_not_hostname(monkeypatch) -> None:
    calls: list[tuple[str, int]] = []
    stream = object()

    class Backend:
        async def connect_tcp(self, host: str, port: int, **_kwargs: Any) -> Any:
            calls.append((host, port))
            return stream

    target = await EgressPolicy(
        resolver=Resolver({"public.example": ("93.184.216.34",)})
    ).validate_url("https://public.example/")
    backend = _PinnedNetworkBackend(target)
    monkeypatch.setattr(backend, "_backend", Backend())

    assert await backend.connect_tcp("public.example", 443) is stream
    assert calls == [("93.184.216.34", 443)]


@pytest.mark.asyncio
async def test_pinned_backend_preserves_connect_timeout(monkeypatch) -> None:
    class Backend:
        async def connect_tcp(self, *_args: Any, **_kwargs: Any) -> Any:
            raise httpcore.ConnectTimeout

    target = await EgressPolicy(
        resolver=Resolver({"public.example": ("93.184.216.34",)})
    ).validate_url("https://public.example/")
    backend = _PinnedNetworkBackend(target)
    monkeypatch.setattr(backend, "_backend", Backend())

    with pytest.raises(httpcore.ConnectTimeout):
        await backend.connect_tcp("public.example", 443)


@pytest.mark.asyncio
async def test_resolution_timeout_is_bounded_and_cancels_resolver() -> None:
    cancelled = asyncio.Event()

    async def slow_resolver(_host: str, _port: int) -> Sequence[str]:
        try:
            await asyncio.sleep(60)
        finally:
            cancelled.set()
        return ("93.184.216.34",)

    policy = EgressPolicy(resolver=slow_resolver, resolution_timeout=0.02)
    started = asyncio.get_running_loop().time()
    with pytest.raises(EgressPolicyError, match="resolution timed out"):
        await policy.validate_url("https://slow.example/")

    assert asyncio.get_running_loop().time() - started < 0.2
    assert cancelled.is_set()


@pytest.mark.asyncio
async def test_native_dns_timeouts_are_isolated_and_capacity_limited() -> None:
    release = threading.Event()

    def slow_getaddrinfo(*_args: Any, **_kwargs: Any) -> Any:
        release.wait(timeout=2)
        return [
            (
                socket.AF_INET,
                socket.SOCK_STREAM,
                socket.IPPROTO_TCP,
                "",
                ("93.184.216.34", 443),
            )
        ]

    policy = EgressPolicy(resolution_timeout=0.02)
    try:
        with patch("socket.getaddrinfo", side_effect=slow_getaddrinfo):
            results = await asyncio.gather(
                *(policy.validate_url(f"https://slow-{index}.example/") for index in range(4)),
                return_exceptions=True,
            )
            assert all(
                isinstance(result, EgressPolicyError) and "timed out" in str(result)
                for result in results
            )
            with pytest.raises(EgressPolicyError, match="capacity is exhausted"):
                await policy.validate_url("https://one-too-many.example/")
    finally:
        release.set()
        await asyncio.sleep(0.05)


@pytest.mark.asyncio
async def test_pinned_backend_shares_connect_deadline_across_addresses(monkeypatch) -> None:
    class Backend:
        def __init__(self) -> None:
            self.timeouts: list[float] = []

        async def connect_tcp(self, *_args: Any, **kwargs: Any) -> Any:
            timeout = float(kwargs["timeout"])
            self.timeouts.append(timeout)
            await asyncio.sleep(min(0.03, timeout))
            raise httpcore.ConnectTimeout

    target = await EgressPolicy(
        resolver=Resolver({"public.example": ("93.184.216.34", "2606:4700:4700::1111")})
    ).validate_url("https://public.example/")
    backend = _PinnedNetworkBackend(target)
    fake_backend = Backend()
    monkeypatch.setattr(backend, "_backend", fake_backend)

    started = asyncio.get_running_loop().time()
    with pytest.raises(httpcore.ConnectTimeout):
        await backend.connect_tcp("public.example", 443, timeout=0.05)
    elapsed = asyncio.get_running_loop().time() - started

    assert len(fake_backend.timeouts) == 2
    assert fake_backend.timeouts[1] < fake_backend.timeouts[0]
    assert elapsed < 0.09


@pytest.mark.asyncio
async def test_http_node_timeout_bounds_dns_and_entire_request_lifecycle() -> None:
    async def slow_resolver(_host: str, _port: int) -> Sequence[str]:
        await asyncio.sleep(60)
        return ("93.184.216.34",)

    policy = EgressPolicy(resolver=slow_resolver, resolution_timeout=10)
    factory = ClientFactory([])
    node = HttpRequestNode(egress_policy=policy, client_factory=factory)

    started = asyncio.get_running_loop().time()
    result = await node.execute(context("https://slow.example/", timeout=1))
    elapsed = asyncio.get_running_loop().time() - started

    assert result.success is False
    assert "timed out" in (result.error_message or "")
    assert elapsed < 1.5
    assert factory.targets == []


@pytest.mark.asyncio
async def test_timed_out_response_parsers_are_capacity_limited() -> None:
    release = threading.Event()

    def blocked_decode(_content: bytes, _content_type: str) -> Any:
        release.wait(timeout=2)
        return {"ok": True}

    try:
        with patch("app.nodes.http_request._decode_response_body", side_effect=blocked_decode):
            results = await asyncio.gather(
                *(
                    asyncio.wait_for(
                        http_request_module._parse_response_body(b"{}", "application/json"),
                        timeout=0.02,
                    )
                    for _index in range(2)
                ),
                return_exceptions=True,
            )
            assert all(isinstance(result, TimeoutError) for result in results)
            with pytest.raises(EgressPolicyError, match="parsing capacity is exhausted"):
                await http_request_module._parse_response_body(b"{}", "application/json")
    finally:
        release.set()
        await asyncio.sleep(0.05)


@pytest.mark.asyncio
async def test_http_node_timeout_preempts_bounded_response_parsing() -> None:
    policy = EgressPolicy()
    factory = ClientFactory(
        [FakeResponse(200, payload={"ok": True}, headers={"content-type": "application/json"})]
    )
    node = HttpRequestNode(egress_policy=policy, client_factory=factory)

    def slow_decode(_content: bytes, _content_type: str) -> Any:
        time.sleep(1.2)
        return {"ok": True}

    started = asyncio.get_running_loop().time()
    with patch("app.nodes.http_request._decode_response_body", side_effect=slow_decode):
        result = await node.execute(context("https://93.184.216.34/", timeout=1))
    elapsed = asyncio.get_running_loop().time() - started

    assert result.success is False
    assert "timed out" in (result.error_message or "")
    assert elapsed < 1.15


@pytest.mark.asyncio
async def test_http_node_limits_decoded_gzip_body_before_buffering() -> None:
    compressed = gzip.compress(b"x" * 2_000_000)
    assert len(compressed) < 10_000

    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            content=compressed,
            headers={"content-encoding": "gzip", "content-type": "text/plain"},
            request=request,
        )

    def factory(_target: ValidatedTarget, timeout: float) -> httpx.AsyncClient:
        return httpx.AsyncClient(
            transport=httpx.MockTransport(handler),
            timeout=timeout,
            follow_redirects=False,
        )

    node = HttpRequestNode(egress_policy=EgressPolicy(), client_factory=factory)
    with patch.object(settings, "egress_max_response_bytes", 10_000):
        result = await node.execute(context("https://93.184.216.34/"))

    assert result.success is False
    assert "size limit" in (result.error_message or "")


@pytest.mark.asyncio
async def test_late_dns_and_parser_failures_are_consumed_safely() -> None:
    loop = asyncio.get_running_loop()
    original_handler = loop.get_exception_handler()
    unhandled: list[dict[str, Any]] = []
    loop.set_exception_handler(lambda _loop, context: unhandled.append(context))

    def delayed_dns_failure(*_args: Any, **_kwargs: Any) -> Any:
        time.sleep(0.05)
        raise socket.gaierror("sensitive resolver detail")

    def delayed_parse_failure(_content: bytes, _content_type: str) -> Any:
        time.sleep(0.05)
        raise ValueError("sensitive parser detail")

    try:
        with (
            patch("socket.getaddrinfo", side_effect=delayed_dns_failure),
            pytest.raises(EgressPolicyError, match="timed out"),
        ):
            await EgressPolicy(resolution_timeout=0.01).validate_url(
                "https://late-failure.example/"
            )
        with (
            patch(
                "app.nodes.http_request._decode_response_body",
                side_effect=delayed_parse_failure,
            ),
            pytest.raises(TimeoutError),
        ):
            await asyncio.wait_for(
                http_request_module._parse_response_body(b"{}", "application/json"),
                timeout=0.01,
            )
        await asyncio.sleep(0.1)
    finally:
        loop.set_exception_handler(original_handler)

    assert unhandled == []


@pytest.mark.asyncio
async def test_http_node_blocks_before_instantiating_network_client() -> None:
    policy = EgressPolicy(resolver=Resolver({"internal.example": ("10.0.0.5",)}))
    factory = ClientFactory([])
    node = HttpRequestNode(egress_policy=policy, client_factory=factory)

    result = await node.execute(context("https://internal.example/private"))

    assert result.success is False
    assert factory.targets == []
    assert "https://" not in (result.error_message or "")


@pytest.mark.asyncio
async def test_http_node_revalidates_each_same_origin_redirect_and_disables_auto_follow() -> None:
    policy = EgressPolicy(resolver=Resolver({"public.example": ("93.184.216.34",)}))
    factory = ClientFactory(
        [
            FakeResponse(302, headers={"location": "/two"}),
            FakeResponse(307, headers={"location": "https://public.example/final"}),
            FakeResponse(200, payload={"ok": True}, headers={"content-type": "application/json"}),
        ]
    )
    node = HttpRequestNode(egress_policy=policy, client_factory=factory, max_redirects=3)

    result = await node.execute(context("https://public.example/one"))

    assert result.success is True
    assert result.output_data["json"] == {"ok": True}
    assert result.metadata["redirects_followed"] == 2
    assert len(factory.targets) == 3
    assert all(request["follow_redirects"] is False for request in factory.requests)


@pytest.mark.asyncio
async def test_http_node_enforces_redirect_bound_before_creating_another_client() -> None:
    policy = EgressPolicy(resolver=Resolver({"public.example": ("93.184.216.34",)}))
    factory = ClientFactory(
        [
            FakeResponse(302, headers={"location": "/two"}),
            FakeResponse(302, headers={"location": "/three"}),
        ]
    )
    node = HttpRequestNode(egress_policy=policy, client_factory=factory, max_redirects=1)

    result = await node.execute(context("https://public.example/one"))

    assert result.success is False
    assert "redirect limit" in (result.error_message or "")
    assert len(factory.targets) == 2


@pytest.mark.asyncio
async def test_http_node_rejects_private_and_cross_origin_redirect_before_second_client() -> None:
    resolver = Resolver(
        {
            "public.example": ("93.184.216.34",),
            "private.example": ("10.0.0.8",),
            "other.example": ("1.1.1.1",),
        }
    )
    policy = EgressPolicy(resolver=resolver)

    private_factory = ClientFactory(
        [FakeResponse(302, headers={"location": "https://private.example/secret"})]
    )
    private_result = await HttpRequestNode(
        egress_policy=policy, client_factory=private_factory
    ).execute(context("https://public.example/"))
    assert private_result.success is False
    assert len(private_factory.targets) == 1

    cross_factory = ClientFactory(
        [FakeResponse(302, headers={"location": "https://other.example/next"})]
    )
    cross_result = await HttpRequestNode(
        egress_policy=policy, client_factory=cross_factory
    ).execute(context("https://public.example/"))
    assert cross_result.success is False
    assert "Cross-origin" in (cross_result.error_message or "")
    assert len(cross_factory.targets) == 1


@pytest.mark.asyncio
async def test_http_node_redacts_credentials_and_upstream_error_body(caplog) -> None:
    factory = ClientFactory([])
    blocked = await HttpRequestNode(egress_policy=EgressPolicy(), client_factory=factory).execute(
        context("https://user:top-secret@93.184.216.34/data")
    )
    assert "top-secret" not in repr(blocked)
    assert "top-secret" not in caplog.text
    assert factory.targets == []

    error_factory = ClientFactory([FakeResponse(500, text="top-secret")])
    failed = await HttpRequestNode(
        egress_policy=EgressPolicy(), client_factory=error_factory
    ).execute(context("https://93.184.216.34/data?token=top-secret"))
    assert "top-secret" not in repr(failed)
    assert "top-secret" not in caplog.text


@pytest.mark.asyncio
async def test_real_httpx_info_log_cannot_emit_query_secrets(caplog) -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={"ok": True},
            headers={"content-type": "application/json"},
            request=request,
        )

    def factory(_target: ValidatedTarget, timeout: float) -> httpx.AsyncClient:
        return httpx.AsyncClient(
            transport=httpx.MockTransport(handler),
            timeout=timeout,
            follow_redirects=False,
        )

    caplog.set_level(logging.INFO)
    node = HttpRequestNode(egress_policy=EgressPolicy(), client_factory=factory)

    result = await node.execute(context("https://93.184.216.34/data?token=top-secret"))

    assert result.success is True
    assert "top-secret" not in caplog.text
    assert logging.getLogger("httpx").getEffectiveLevel() >= logging.WARNING


@pytest.mark.asyncio
async def test_pinned_response_stream_maps_httpcore_timeout_to_httpx() -> None:
    class TimeoutStream:
        def __aiter__(self):
            return self

        async def __anext__(self):
            raise httpcore.ReadTimeout

        async def aclose(self) -> None:
            return None

    request = httpx.Request("GET", "https://redacted.invalid/")
    stream = _PinnedResponseStream(TimeoutStream(), request, max_bytes=1024)

    with pytest.raises(httpx.TimeoutException):
        await anext(stream.__aiter__())


@pytest.mark.asyncio
async def test_pinned_response_stream_enforces_size_limit_while_reading() -> None:
    class ChunkStream:
        async def __aiter__(self):
            yield b"1234"
            yield b"5678"

        async def aclose(self) -> None:
            return None

    request = httpx.Request("GET", "https://redacted.invalid/")
    stream = _PinnedResponseStream(ChunkStream(), request, max_bytes=6)

    with pytest.raises(httpx.RequestError, match="size limit"):
        _chunks = [chunk async for chunk in stream]


@pytest.mark.asyncio
async def test_postgres_connection_test_pins_ip_and_redacts_driver_errors() -> None:
    resolver = Resolver({"db.lan": ("10.20.30.40",)})
    policy = EgressPolicy(allowed_hosts=("db.lan",), resolver=resolver)
    config = {
        "host": "db.lan",
        "port": 5432,
        "database": "items",
        "username": "user",
        "password": "top-secret",
    }
    connection = MagicMock()
    connection.execute.return_value = None

    with patch("psycopg.connect", return_value=connection) as connect:
        result = await check_connection(config, "postgres", egress_policy=policy)

    assert result["success"] is True
    assert connect.call_args.kwargs["host"] == "db.lan"
    assert connect.call_args.kwargs["hostaddr"] == "10.20.30.40"
    connection.close.assert_called_once()

    with patch(
        "psycopg.connect",
        side_effect=RuntimeError("top-secret at postgresql://user:top-secret@db.lan"),
    ):
        failed = await check_connection(config, "postgres", egress_policy=policy)
    assert failed["success"] is False
    assert "top-secret" not in failed["message"]
    assert "postgresql://" not in failed["message"]


@pytest.mark.asyncio
async def test_postgres_connection_test_passes_every_validated_ip_to_driver() -> None:
    policy = EgressPolicy(
        resolver=Resolver({"db.example": ("93.184.216.34", "2606:4700:4700::1111")})
    )
    config = {
        "host": "db.example",
        "port": 5432,
        "database": "items",
        "username": "user",
        "password": "top-secret",
    }
    connection = MagicMock()
    connection.execute.return_value = None

    with patch("psycopg.connect", return_value=connection) as connect:
        result = await check_connection(config, "postgres", egress_policy=policy)

    assert result["success"] is True
    assert connect.call_args.kwargs["host"] == "db.example,db.example"
    assert connect.call_args.kwargs["hostaddr"] == "93.184.216.34,2606:4700:4700::1111"


@pytest.mark.asyncio
async def test_discord_connection_test_uses_pinned_client_without_redirects() -> None:
    policy = EgressPolicy(resolver=Resolver({"discord.com": ("93.184.216.34",)}))
    factory = ClientFactory([FakeResponse(200)])

    result = await check_connection(
        {"bot_token": "top-secret"},
        "discord",
        egress_policy=policy,
        http_client_factory=factory,
    )

    assert result == {"success": True, "message": "Discord connection successful"}
    assert tuple(str(address) for address in factory.targets[0].addresses) == ("93.184.216.34",)
    assert factory.requests[0]["follow_redirects"] is False


@pytest.mark.asyncio
async def test_blocked_postgres_and_discord_tests_never_create_clients() -> None:
    postgres_config = {
        "host": "127.0.0.1",
        "port": 5432,
        "database": "items",
        "username": "user",
        "password": "secret",
    }
    with patch("psycopg.connect") as connect:
        result = await check_connection(postgres_config, "postgres", egress_policy=EgressPolicy())
    assert result["success"] is False
    connect.assert_not_called()

    factory = ClientFactory([])
    discord_policy = EgressPolicy(resolver=Resolver({"discord.com": ("127.0.0.1",)}))
    result = await check_connection(
        {"bot_token": "top-secret"},
        "discord",
        egress_policy=discord_policy,
        http_client_factory=factory,
    )
    assert result["success"] is False
    assert factory.targets == []
    assert "top-secret" not in result["message"]
