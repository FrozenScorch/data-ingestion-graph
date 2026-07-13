"""Centralized outbound egress and SSRF policy for Studio."""

from __future__ import annotations

import asyncio
import ipaddress
import re
import socket
import ssl
from collections.abc import Awaitable, Callable, Iterable, Sequence
from dataclasses import dataclass
from typing import Any, Literal, TypeAlias
from urllib.parse import SplitResult, urlsplit, urlunsplit

import httpcore
import httpx
from app.config import Settings, settings

IPAddress: TypeAlias = ipaddress.IPv4Address | ipaddress.IPv6Address
IPNetwork: TypeAlias = ipaddress.IPv4Network | ipaddress.IPv6Network
Resolver: TypeAlias = Callable[[str, int], Awaitable[Sequence[str]]]
PolicyMode: TypeAlias = Literal["public", "allowlist-only"]

_HOST_LABEL = re.compile(r"^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$")
_METADATA_HOSTS = frozenset(
    {
        "metadata.google.internal",
        "metadata.google.com",
        "metadata.azure.internal",
    }
)
_METADATA_ADDRESSES = frozenset(
    {
        ipaddress.ip_address("169.254.169.254"),
        ipaddress.ip_address("169.254.170.2"),
        ipaddress.ip_address("100.100.100.200"),
        ipaddress.ip_address("fd00:ec2::254"),
    }
)


class EgressPolicyError(ValueError):
    """A safe, credential-free outbound-policy failure."""


@dataclass(frozen=True, slots=True)
class ValidatedTarget:
    """Canonical target plus the complete validated address set."""

    url: str
    scheme: str
    host: str
    port: int
    addresses: tuple[IPAddress, ...]
    host_was_name: bool

    @property
    def safe_origin(self) -> str:
        """Credential/query-free origin suitable for result metadata."""
        host = f"[{self.host}]" if ":" in self.host else self.host
        default_port = 443 if self.scheme == "https" else 80
        suffix = "" if self.port == default_port else f":{self.port}"
        return f"{self.scheme}://{host}{suffix}"


class EgressPolicy:
    """Resolve once, validate every answer, and return addresses for pinning."""

    def __init__(
        self,
        *,
        mode: PolicyMode = "public",
        allowed_hosts: Iterable[str] = (),
        allowed_cidrs: Iterable[str] = (),
        resolver: Resolver | None = None,
    ) -> None:
        if mode not in {"public", "allowlist-only"}:
            raise EgressPolicyError("Outbound policy mode is invalid")
        self.mode: PolicyMode = mode
        self.allowed_hosts = frozenset(_normalize_hostname(host) for host in allowed_hosts)
        self.allowed_networks = tuple(_normalize_network(cidr) for cidr in allowed_cidrs)
        self._resolver = resolver or resolve_all_addresses

    @classmethod
    def from_settings(cls, configured: Settings = settings) -> EgressPolicy:
        return cls(
            mode=configured.egress_policy_mode,  # type: ignore[arg-type]
            allowed_hosts=configured.egress_allowed_hosts_list,
            allowed_cidrs=configured.egress_allowed_cidrs_list,
        )

    async def validate_url(self, value: Any) -> ValidatedTarget:
        parsed = _parse_url(value)
        scheme = parsed.scheme.lower()
        port = _url_port(parsed, scheme)
        host = _normalize_host(parsed.hostname or "")
        addresses, was_name = await self._resolve_and_validate(host, port)
        canonical = _canonical_url(parsed, scheme=scheme, host=host, port=port)
        return ValidatedTarget(canonical, scheme, host, port, addresses, was_name)

    async def validate_host(self, host: Any, port: Any) -> ValidatedTarget:
        normalized_port = _validate_port(port)
        normalized_host = _normalize_host_value(host)
        addresses, was_name = await self._resolve_and_validate(normalized_host, normalized_port)
        literal = f"[{normalized_host}]" if ":" in normalized_host else normalized_host
        url = f"tcp://{literal}:{normalized_port}"
        return ValidatedTarget(url, "tcp", normalized_host, normalized_port, addresses, was_name)

    async def _resolve_and_validate(
        self, host: str, port: int
    ) -> tuple[tuple[IPAddress, ...], bool]:
        if host in _METADATA_HOSTS:
            raise EgressPolicyError("Cloud metadata destinations are blocked")
        literal = _parse_ip(host)
        host_was_name = literal is None
        if literal is None:
            try:
                answers = await self._resolver(host, port)
            except Exception:
                raise EgressPolicyError("Outbound hostname resolution failed") from None
            if not answers:
                raise EgressPolicyError("Outbound hostname did not resolve")
            resolved: list[IPAddress] = []
            try:
                for answer in answers:
                    address = _normalize_address(ipaddress.ip_address(answer))
                    if address not in resolved:
                        resolved.append(address)
            except (TypeError, ValueError):
                raise EgressPolicyError("Outbound hostname returned an invalid address") from None
        else:
            resolved = [literal]

        if any(address in _METADATA_ADDRESSES for address in resolved):
            raise EgressPolicyError("Cloud metadata destinations are blocked")

        restricted = [address for address in resolved if _is_restricted(address)]
        public = [address for address in resolved if not _is_restricted(address)]
        if restricted and public:
            raise EgressPolicyError("Mixed public and restricted DNS answers are blocked")

        host_allowed = host in self.allowed_hosts
        for address in resolved:
            address_allowed = any(address in network for network in self.allowed_networks)
            if _is_restricted(address) and not (host_allowed or address_allowed):
                raise EgressPolicyError("Outbound destination address is blocked")
            if self.mode == "allowlist-only" and not (host_allowed or address_allowed):
                raise EgressPolicyError("Outbound destination is not allowlisted")
        return tuple(resolved), host_was_name


async def resolve_all_addresses(host: str, port: int) -> Sequence[str]:
    """Resolve every TCP A/AAAA answer without opening a socket."""
    loop = asyncio.get_running_loop()
    try:
        answers = await loop.getaddrinfo(
            host,
            port,
            family=socket.AF_UNSPEC,
            type=socket.SOCK_STREAM,
            proto=socket.IPPROTO_TCP,
        )
    except OSError:
        raise EgressPolicyError("Outbound hostname resolution failed") from None
    return tuple(str(answer[4][0]) for answer in answers)


def _parse_url(value: Any) -> SplitResult:
    if not isinstance(value, str) or not value or any(ord(char) < 32 for char in value):
        raise EgressPolicyError("Outbound URL is invalid")
    try:
        parsed = urlsplit(value)
    except ValueError:
        raise EgressPolicyError("Outbound URL is invalid") from None
    if parsed.scheme.lower() not in {"http", "https"}:
        raise EgressPolicyError("Outbound URL scheme must be HTTP or HTTPS")
    if not parsed.hostname:
        raise EgressPolicyError("Outbound URL host is required")
    if parsed.username is not None or parsed.password is not None:
        raise EgressPolicyError("Outbound URL credentials are not allowed")
    if parsed.fragment:
        raise EgressPolicyError("Outbound URL fragments are not allowed")
    return parsed


def _url_port(parsed: SplitResult, scheme: str) -> int:
    try:
        port = parsed.port
    except ValueError:
        raise EgressPolicyError("Outbound URL port is invalid") from None
    return _validate_port(port if port is not None else (443 if scheme == "https" else 80))


def _validate_port(value: Any) -> int:
    if isinstance(value, bool):
        raise EgressPolicyError("Outbound port is invalid")
    try:
        port = int(value)
    except (TypeError, ValueError, OverflowError):
        raise EgressPolicyError("Outbound port is invalid") from None
    if isinstance(value, float) and not value.is_integer():
        raise EgressPolicyError("Outbound port is invalid")
    if not 1 <= port <= 65535:
        raise EgressPolicyError("Outbound port is invalid")
    return port


def _normalize_host_value(value: Any) -> str:
    if not isinstance(value, str) or not value or any(ord(char) < 32 for char in value):
        raise EgressPolicyError("Outbound host is invalid")
    candidate = value[1:-1] if value.startswith("[") and value.endswith("]") else value
    if "/" in candidate or "@" in candidate:
        raise EgressPolicyError("Outbound host is invalid")
    return _normalize_host(candidate)


def _normalize_host(value: str) -> str:
    if "%" in value:
        raise EgressPolicyError("Scoped outbound IP addresses are not allowed")
    literal = _parse_ip(value)
    return str(literal) if literal is not None else _normalize_hostname(value)


def _normalize_hostname(value: str) -> str:
    candidate = value.rstrip(".").lower()
    if not candidate:
        raise EgressPolicyError("Outbound host is invalid")
    try:
        ascii_name = candidate.encode("idna").decode("ascii")
    except UnicodeError:
        raise EgressPolicyError("Outbound host is invalid") from None
    labels = ascii_name.split(".")
    if len(ascii_name) > 253 or any(not _HOST_LABEL.fullmatch(label) for label in labels):
        raise EgressPolicyError("Outbound host is invalid")
    return ascii_name


def _parse_ip(value: str) -> IPAddress | None:
    try:
        return _normalize_address(ipaddress.ip_address(value))
    except ValueError:
        return None


def _normalize_address(value: IPAddress) -> IPAddress:
    if isinstance(value, ipaddress.IPv6Address) and value.ipv4_mapped is not None:
        return value.ipv4_mapped
    return value


def _normalize_network(value: str) -> IPNetwork:
    try:
        network = ipaddress.ip_network(value, strict=False)
    except (TypeError, ValueError):
        raise EgressPolicyError("Outbound CIDR allowlist contains an invalid entry") from None
    if isinstance(network, ipaddress.IPv6Network) and network.network_address.ipv4_mapped:
        if network.prefixlen < 96:
            raise EgressPolicyError("IPv4-mapped CIDR prefixes shorter than /96 are invalid")
        return ipaddress.IPv4Network(
            (int(network.network_address.ipv4_mapped), network.prefixlen - 96)
        )
    return network


def _is_restricted(address: IPAddress) -> bool:
    return any(
        (
            not address.is_global,
            address.is_loopback,
            address.is_private,
            address.is_link_local,
            address.is_multicast,
            address.is_unspecified,
            address.is_reserved,
        )
    )


def _canonical_url(parsed: SplitResult, *, scheme: str, host: str, port: int) -> str:
    rendered_host = f"[{host}]" if ":" in host else host
    default_port = 443 if scheme == "https" else 80
    netloc = rendered_host if port == default_port else f"{rendered_host}:{port}"
    return urlunsplit((scheme, netloc, parsed.path or "/", parsed.query, ""))


class _PinnedNetworkBackend(httpcore.AsyncNetworkBackend):
    """Delegate socket creation only to addresses returned by the policy."""

    def __init__(self, target: ValidatedTarget) -> None:
        self._target = target
        self._backend = httpcore.AnyIOBackend()

    async def connect_tcp(
        self,
        host: str,
        port: int,
        timeout: float | None = None,
        local_address: str | None = None,
        socket_options: Iterable[tuple[int, int, Any]] | None = None,
    ) -> httpcore.AsyncNetworkStream:
        try:
            requested_host = _normalize_host(host)
        except EgressPolicyError:
            raise httpcore.ConnectError("Pinned outbound host changed") from None
        if requested_host != self._target.host or port != self._target.port:
            raise httpcore.ConnectError("Pinned outbound destination changed")
        for address in self._target.addresses:
            try:
                return await self._backend.connect_tcp(
                    str(address),
                    port,
                    timeout=timeout,
                    local_address=local_address,
                    socket_options=socket_options,
                )
            except Exception:
                continue
        raise httpcore.ConnectError("Pinned outbound connection failed") from None

    async def connect_unix_socket(
        self,
        path: str,
        timeout: float | None = None,
        socket_options: Iterable[tuple[int, int, Any]] | None = None,
    ) -> httpcore.AsyncNetworkStream:
        del path, timeout, socket_options
        raise httpcore.ConnectError("Unix sockets are disabled for outbound requests")

    async def sleep(self, seconds: float) -> None:
        await self._backend.sleep(seconds)


class _PinnedResponseStream(httpx.AsyncByteStream):
    def __init__(self, stream: Any) -> None:
        self._stream = stream

    async def __aiter__(self):
        async for chunk in self._stream:
            yield chunk

    async def aclose(self) -> None:
        await self._stream.aclose()


class PinnedAsyncHTTPTransport(httpx.AsyncBaseTransport):
    """HTTPX transport that preserves TLS SNI while pinning the TCP address."""

    def __init__(self, target: ValidatedTarget) -> None:
        self._pool = httpcore.AsyncConnectionPool(
            ssl_context=ssl.create_default_context(),
            network_backend=_PinnedNetworkBackend(target),
            max_connections=2,
            max_keepalive_connections=0,
            http1=True,
            http2=False,
        )

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        core_request = httpcore.Request(
            method=request.method,
            url=httpcore.URL(
                scheme=request.url.raw_scheme,
                host=request.url.raw_host,
                port=request.url.port,
                target=request.url.raw_path,
            ),
            headers=request.headers.raw,
            content=request.stream,
            extensions=request.extensions,
        )
        try:
            response = await self._pool.handle_async_request(core_request)
        except Exception:
            raise httpx.RequestError("Pinned outbound request failed", request=request) from None
        return httpx.Response(
            status_code=response.status,
            headers=response.headers,
            stream=_PinnedResponseStream(response.stream),
            extensions=response.extensions,
        )

    async def aclose(self) -> None:
        await self._pool.aclose()


def create_pinned_http_client(target: ValidatedTarget, timeout: float) -> httpx.AsyncClient:
    """Create a no-proxy, no-redirect HTTP client pinned to one policy decision."""
    return httpx.AsyncClient(
        transport=PinnedAsyncHTTPTransport(target),
        timeout=timeout,
        follow_redirects=False,
        trust_env=False,
    )
