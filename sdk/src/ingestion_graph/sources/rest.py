"""Generic, resumable HTTPS JSON REST source."""

from __future__ import annotations

import asyncio
import hashlib
import ipaddress
import json
import math
import re
from collections.abc import AsyncIterator, Awaitable, Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from typing import Any, Literal, TypeAlias
from urllib.parse import parse_qsl, unquote, urlencode, urljoin, urlsplit, urlunsplit

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
    IngestionError,
    PermissionDeniedError,
    ProtocolError,
    RateLimitError,
)
from ingestion_graph.messages import RecordMessage, SourceMessage, StateMessage
from ingestion_graph.models import Envelope, Operation, RecordPayload, stable_record_id
from ingestion_graph.secrets import EnvSecretProvider, SecretProvider, SecretRef

try:
    import httpx
except ImportError:  # pragma: no cover - exercised when the optional extra is absent
    httpx = None  # type: ignore[assignment]


AuthType: TypeAlias = Literal["none", "bearer", "api_key"]
PaginationType: TypeAlias = Literal["none", "cursor", "link"]
JsonScalar: TypeAlias = str | int | float | bool | None
Sleep: TypeAlias = Callable[[float], Awaitable[None]]

_HEADER_NAME = re.compile(r"^[!#$%&'*+.^_`|~0-9A-Za-z-]+$")
_STREAM_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")
_MAX_COMPLETED_REQUEST_HASHES = 100_000
_SENSITIVE_QUERY_COMPONENTS = frozenset(
    {
        "apikey",
        "auth",
        "authorization",
        "credential",
        "key",
        "password",
        "secret",
        "signature",
        "sig",
        "token",
    }
)
_SENSITIVE_QUERY_SUFFIXES = (
    "apikey",
    "credential",
    "password",
    "secret",
    "signature",
    "token",
)


@dataclass(frozen=True, slots=True)
class _ResumePoint:
    request_url: str
    cycle: int = 0
    record_offset: int = 0
    page_fingerprint: str | None = None
    primary_key_types: tuple[str, ...] | None = None
    record_field_types: Mapping[str, tuple[str, ...]] | None = None
    completed_request_hashes: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class _Page:
    records: tuple[Mapping[str, Any], ...]
    next_url: str | None
    fingerprint: str


class RestSource(Source):
    """Read JSON records from a bounded, paginated REST endpoint.

    Checkpoints identify the *next request*. If a run stops part-way through a
    response page, the checkpoint instead identifies that page, its content hash,
    and the first uncommitted record. Resuming therefore refetches and validates
    the exact page before skipping already committed records.
    """

    def __init__(
        self,
        base_url: str,
        path: str,
        *,
        stream: str,
        records_path: str | Sequence[str],
        primary_key: Sequence[str],
        pagination: PaginationType = "none",
        next_cursor_path: str | Sequence[str] = ("next_cursor",),
        cursor_param: str = "cursor",
        query_params: Mapping[str, JsonScalar] | None = None,
        auth_type: AuthType = "none",
        secret: SecretRef | None = None,
        api_key_header: str = "X-API-Key",
        secret_provider: SecretProvider | None = None,
        allow_http: bool = False,
        allow_cross_origin_next: bool = False,
        max_pages: int = 100,
        max_records: int = 100_000,
        max_retries: int = 5,
        request_timeout: float = 30.0,
        client: Any = None,
        sleep: Sleep = asyncio.sleep,
    ) -> None:
        if not isinstance(allow_http, bool):
            raise ConfigurationError("REST allow_http must be a boolean")
        if not isinstance(allow_cross_origin_next, bool):
            raise ConfigurationError("REST allow_cross_origin_next must be a boolean")
        if allow_cross_origin_next and client is not None:
            raise ConfigurationError(
                "REST cross-origin pagination cannot use an injected HTTP client"
            )
        self.base_url = _validate_base_url(base_url, allow_http=allow_http)
        self.path = _validate_path(path)
        if not isinstance(stream, str) or not _STREAM_NAME.fullmatch(stream):
            raise ConfigurationError(
                "REST stream must start with an alphanumeric character and contain only "
                "letters, numbers, dots, underscores, or hyphens"
            )
        self.stream = stream
        self.records_path = _normalize_data_path(records_path, "records_path")
        self.primary_key = _normalize_primary_key(primary_key)
        if pagination not in {"none", "cursor", "link"}:
            raise ConfigurationError("REST pagination must be 'none', 'cursor', or 'link'")
        self.pagination: PaginationType = pagination
        self.next_cursor_path = _normalize_data_path(next_cursor_path, "next_cursor_path")
        if pagination == "cursor" and not self.next_cursor_path:
            raise ConfigurationError("REST cursor pagination requires next_cursor_path")
        self.cursor_param = _validate_parameter_name(cursor_param, "cursor_param")
        if _is_sensitive_query_name(self.cursor_param) and not _is_pagination_token_query_name(
            self.cursor_param
        ):
            raise ConfigurationError(
                "REST cursor_param must be a pagination parameter, not a credential name"
            )
        self.query_params = _validate_query_params(
            {} if query_params is None else query_params, self.cursor_param
        )
        if auth_type not in {"none", "bearer", "api_key"}:
            raise ConfigurationError("REST auth_type must be 'none', 'bearer', or 'api_key'")
        if auth_type == "none" and secret is not None:
            raise ConfigurationError("REST secret requires bearer or api_key authentication")
        if auth_type != "none" and not isinstance(secret, SecretRef):
            raise ConfigurationError("REST bearer and api_key authentication require SecretRef")
        if not isinstance(api_key_header, str) or not _HEADER_NAME.fullmatch(api_key_header):
            raise ConfigurationError("REST api_key_header is not a safe HTTP header name")
        _positive_int(max_pages, "max_pages")
        _positive_int(max_records, "max_records")
        if isinstance(max_retries, bool) or not isinstance(max_retries, int) or max_retries < 0:
            raise ConfigurationError("REST max_retries must be a non-negative integer")
        if isinstance(request_timeout, bool) or not isinstance(request_timeout, (int, float)):
            raise ConfigurationError("REST request_timeout must be a positive finite number")
        if not math.isfinite(float(request_timeout)) or request_timeout <= 0:
            raise ConfigurationError("REST request_timeout must be a positive finite number")

        self.auth_type: AuthType = auth_type
        self.secret_ref = secret
        self.api_key_header = api_key_header
        self.secret_provider = secret_provider or EnvSecretProvider()
        self.allow_http = allow_http
        self.allow_cross_origin_next = allow_cross_origin_next
        self.max_pages = max_pages
        self.max_records = max_records
        self.max_retries = max_retries
        self.request_timeout = float(request_timeout)
        self._client = client
        self._owns_client = client is None
        self._sleep = sleep
        self._endpoint_url = _append_query(
            f"{self.base_url.rstrip('/')}/{self.path.lstrip('/')}", self.query_params
        )
        self._fingerprint = _configuration_fingerprint(self)

    @classmethod
    def manifest(cls) -> ConnectorSpec:
        return ConnectorSpec(
            name="rest",
            version="1.0.0",
            config_schema={
                "type": "object",
                "properties": {
                    "base_url": {"type": "string", "format": "uri"},
                    "path": {"type": "string", "minLength": 1},
                    "stream": {"type": "string", "minLength": 1},
                    "records_path": {
                        "oneOf": [
                            {"type": "string"},
                            {"type": "array", "items": {"type": "string"}},
                        ]
                    },
                    "primary_key": {
                        "type": "array",
                        "items": {"type": "string"},
                        "minItems": 1,
                    },
                    "pagination": {
                        "type": "string",
                        "enum": ["none", "cursor", "link"],
                        "default": "none",
                    },
                    "next_cursor_path": {
                        "oneOf": [
                            {"type": "string"},
                            {"type": "array", "items": {"type": "string"}},
                        ],
                        "default": ["next_cursor"],
                    },
                    "cursor_param": {"type": "string", "default": "cursor"},
                    "query_params": {"type": "object", "default": {}},
                    "auth_type": {
                        "type": "string",
                        "enum": ["none", "bearer", "api_key"],
                        "default": "none",
                    },
                    "secret": {"type": "string", "format": "secret-ref"},
                    "api_key_header": {"type": "string", "default": "X-API-Key"},
                    "allow_http": {"type": "boolean", "default": False},
                    "allow_cross_origin_next": {"type": "boolean", "default": False},
                    "max_pages": {"type": "integer", "minimum": 1, "default": 100},
                    "max_records": {
                        "type": "integer",
                        "minimum": 1,
                        "default": 100_000,
                    },
                    "max_retries": {"type": "integer", "minimum": 0, "default": 5},
                    "request_timeout": {
                        "type": "number",
                        "exclusiveMinimum": 0,
                        "default": 30.0,
                    },
                },
                "required": ["base_url", "path", "stream", "records_path", "primary_key"],
                "additionalProperties": False,
            },
            capabilities=ConnectorCapabilities(
                incremental=False,
                resumable_full_refresh=True,
                deletes=False,
                schema_discovery=True,
                rate_limits=True,
            ),
        )

    def spec(self) -> ConnectorSpec:
        return self.manifest()

    async def check(self) -> CheckResult:
        try:
            page = await self._fetch_page(self._endpoint_url)
            self._validate_page_identities(page.records, None)
            _merge_record_field_types(None, _record_field_types(page.records))
            return CheckResult(True, "REST endpoint and response shape are valid")
        except IngestionError as exc:
            return CheckResult(False, str(exc))

    async def discover(self) -> Sequence[StreamDescriptor]:
        page = await self._fetch_page(self._endpoint_url)
        self._validate_page_identities(page.records, None)
        _merge_record_field_types(None, _record_field_types(page.records))
        return [
            StreamDescriptor(
                name=self.stream,
                namespace="rest.endpoint",
                json_schema=_infer_records_schema(page.records),
                primary_key=self.primary_key,
            )
        ]

    async def read(
        self,
        stream: StreamDescriptor,
        state: Mapping[str, Any] | None = None,
    ) -> AsyncIterator[SourceMessage]:
        if stream.name != self.stream:
            raise ConfigurationError(f"REST stream {stream.name!r} is not configured")

        resume = _decode_state(state, self._fingerprint, self._endpoint_url)
        request_url = resume.request_url
        record_offset = resume.record_offset
        expected_page_fingerprint = resume.page_fingerprint
        cycle = resume.cycle
        primary_key_types = resume.primary_key_types
        record_field_types = resume.record_field_types
        completed_request_hashes = set(resume.completed_request_hashes)
        emitted = 0

        for _page_number in range(self.max_pages):
            if len(completed_request_hashes) >= _MAX_COMPLETED_REQUEST_HASHES:
                raise ProtocolError("REST pagination exceeded the cycle page-history limit")
            request_hash = _request_hash(request_url)
            if request_hash in completed_request_hashes:
                raise ProtocolError("REST pagination repeated a request URL without progress")
            page = await self._fetch_page(request_url)
            identities, page_key_types = self._validate_page_identities(
                page.records, primary_key_types
            )
            record_field_types = _merge_record_field_types(
                record_field_types, _record_field_types(page.records)
            )
            if primary_key_types is None and page_key_types is not None:
                primary_key_types = page_key_types
            if expected_page_fingerprint is not None:
                if page.fingerprint != expected_page_fingerprint:
                    raise ProtocolError(
                        "REST response page changed after a partial-page checkpoint; "
                        "clear state or restore a stable API snapshot"
                    )
                if record_offset >= len(page.records):
                    raise ConfigurationError("REST checkpoint record_offset is outside its page")
            elif record_offset != 0:
                raise ConfigurationError(
                    "REST checkpoint with record_offset requires page_fingerprint"
                )

            remaining = self.max_records - emitted
            page_remaining = len(page.records) - record_offset
            take = min(remaining, page_remaining)
            stop_offset = record_offset + take
            for index in range(record_offset, stop_offset):
                yield RecordMessage(
                    self._to_envelope(page.records[index], identities[index], request_url, cycle)
                )
            emitted += take

            if stop_offset < len(page.records):
                yield StateMessage(
                    self.stream,
                    _encode_state(
                        self._fingerprint,
                        request_url,
                        cycle=cycle,
                        record_offset=stop_offset,
                        page_fingerprint=page.fingerprint,
                        primary_key_types=primary_key_types,
                        record_field_types=record_field_types,
                        completed_request_hashes=completed_request_hashes,
                    ),
                )
                return

            completed_request_hashes.add(request_hash)
            if page.next_url is None:
                yield StateMessage(
                    self.stream,
                    _encode_state(
                        self._fingerprint,
                        self._endpoint_url,
                        cycle=cycle + 1,
                        primary_key_types=primary_key_types,
                        record_field_types=record_field_types,
                        completed_request_hashes=(),
                    ),
                )
                return

            continuation = _encode_state(
                self._fingerprint,
                page.next_url,
                cycle=cycle,
                primary_key_types=primary_key_types,
                record_field_types=record_field_types,
                completed_request_hashes=completed_request_hashes,
            )
            if _request_hash(page.next_url) in completed_request_hashes:
                raise ProtocolError("REST pagination repeated a request URL without progress")
            yield StateMessage(self.stream, continuation)
            if emitted >= self.max_records:
                return
            request_url = page.next_url
            record_offset = 0
            expected_page_fingerprint = None

        # max_pages is an intentional per-run boundary. The last page checkpoint
        # already names the exact next request.
        return

    async def close(self) -> None:
        if self._owns_client and self._client is not None:
            await self._client.aclose()
            self._client = None

    async def _get_client(self) -> Any:
        if self._client is None:
            if httpx is None:
                raise ConfigurationError(
                    "REST support requires the optional dependency: "
                    "pip install 'ingestion-graph[rest]'"
                )
            self._client = httpx.AsyncClient(
                timeout=self.request_timeout,
                follow_redirects=False,
                headers={
                    "Accept": "application/json",
                    "User-Agent": "ingestion-graph/rest-1.0",
                },
            )
        return self._client

    def _auth_headers(self, request_url: str) -> dict[str, str]:
        if self.auth_type == "none" or not _same_origin(request_url, self.base_url):
            return {}
        credential = self._resolve_credential()
        if self.auth_type == "bearer":
            return {"Authorization": f"Bearer {credential}"}
        return {self.api_key_header: credential}

    def _resolve_credential(self) -> str:
        if self.secret_ref is None:  # guarded by constructor; keeps narrowing explicit
            raise ConfigurationError("REST authentication secret is not configured")
        try:
            credential = str(self.secret_provider.resolve(self.secret_ref))
        except Exception:
            raise ConfigurationError("REST authentication secret could not be resolved") from None
        if not credential or "\r" in credential or "\n" in credential:
            raise ConfigurationError("REST authentication secret is empty or invalid")
        return credential

    def _reject_credential_url(self, request_url: str) -> None:
        if self.auth_type == "none":
            return
        credential = self._resolve_credential()
        parsed = urlsplit(request_url)
        decoded_query = (
            component
            for pair in parse_qsl(parsed.query, keep_blank_values=True)
            for component in pair
        )
        if credential in unquote(parsed.path) or any(
            credential in component for component in decoded_query
        ):
            raise ProtocolError("REST request URL must not contain the authentication secret")

    async def _request(self, request_url: str) -> Any:
        _validate_request_url(
            request_url,
            base_url=self.base_url,
            allow_http=self.allow_http,
            allow_cross_origin=self.allow_cross_origin_next,
            allowed_sensitive_query_names=(self.cursor_param,)
            if self.pagination == "cursor"
            else (),
            allow_pagination_tokens=self.pagination == "link",
        )
        self._reject_credential_url(request_url)
        client = await self._get_client()
        for attempt in range(self.max_retries + 1):
            try:
                response = await client.request(
                    "GET",
                    request_url,
                    headers=self._auth_headers(request_url),
                    follow_redirects=False,
                )
            except IngestionError:
                raise
            except Exception:
                raise ConfigurationError(
                    "REST request failed before receiving a response"
                ) from None

            status = int(response.status_code)
            if status == 401:
                raise AuthenticationError("REST credentials were rejected")
            if status == 403:
                raise PermissionDeniedError("REST credentials lack endpoint permission")
            if status == 429 or 500 <= status < 600:
                if attempt >= self.max_retries:
                    if status == 429:
                        raise RateLimitError("REST rate-limit retry budget was exhausted")
                    raise ProtocolError("REST server error retry budget was exhausted")
                delay = _retry_after(_header_value(response.headers, "Retry-After"))
                if delay is None:
                    delay = min(float(2**attempt), 30.0)
                await self._sleep(delay)
                continue
            if status >= 400 or 300 <= status < 400:
                raise ConfigurationError(f"REST endpoint returned HTTP {status}")
            return response
        raise ProtocolError("REST retry loop ended unexpectedly")  # pragma: no cover

    async def _fetch_page(self, request_url: str) -> _Page:
        response = await self._request(request_url)
        try:
            payload = response.json()
        except Exception:
            raise ProtocolError("REST endpoint returned malformed JSON") from None
        extracted = _extract_required(payload, self.records_path, "records_path")
        if not isinstance(extracted, list):
            raise ProtocolError("REST records_path must resolve to a JSON array")
        if any(not isinstance(item, Mapping) for item in extracted):
            raise ProtocolError("REST records array must contain only JSON objects")
        records = tuple({str(key): value for key, value in item.items()} for item in extracted)

        if self.pagination == "cursor":
            cursor = _extract_optional(payload, self.next_cursor_path)
            if cursor is None or cursor == "":
                next_url = None
            elif not _is_json_scalar(cursor):
                raise ProtocolError("REST next cursor must be a JSON scalar")
            else:
                next_url = _replace_query_parameter(self._endpoint_url, self.cursor_param, cursor)
        elif self.pagination == "link":
            next_target = _parse_next_link(_header_value(response.headers, "Link"))
            next_url = None if next_target is None else urljoin(request_url, next_target)
        else:
            next_url = None

        if next_url is not None:
            next_url = _validate_request_url(
                next_url,
                base_url=self.base_url,
                allow_http=self.allow_http,
                allow_cross_origin=self.allow_cross_origin_next,
                allowed_sensitive_query_names=(self.cursor_param,)
                if self.pagination == "cursor"
                else (),
                allow_pagination_tokens=self.pagination == "link",
            )
            self._reject_credential_url(next_url)
        return _Page(records, next_url, _page_fingerprint(records, next_url))

    def _validate_page_identities(
        self,
        records: Sequence[Mapping[str, Any]],
        expected_types: tuple[str, ...] | None,
    ) -> tuple[tuple[tuple[JsonScalar, ...], ...], tuple[str, ...] | None]:
        identities: list[tuple[JsonScalar, ...]] = []
        page_types = expected_types
        seen: set[str] = set()
        for record in records:
            values: list[JsonScalar] = []
            types: list[str] = []
            for field in self.primary_key:
                value = _extract_required(record, tuple(field.split(".")), f"primary key {field!r}")
                if value is None or not _is_json_scalar(value):
                    raise ProtocolError(
                        f"REST primary key field {field!r} must be a non-null JSON scalar"
                    )
                scalar = value
                values.append(scalar)
                types.append(_scalar_type(scalar))
            key_types = tuple(types)
            if page_types is None:
                page_types = key_types
            elif page_types != key_types:
                raise ProtocolError("REST primary key types changed between response records")
            identity = tuple(values)
            encoded = _canonical_identity(identity)
            if encoded in seen:
                raise ProtocolError("REST response page contains duplicate primary keys")
            seen.add(encoded)
            identities.append(identity)
        return tuple(identities), page_types

    def _to_envelope(
        self,
        record: Mapping[str, Any],
        identity: tuple[JsonScalar, ...],
        request_url: str,
        cycle: int,
    ) -> Envelope:
        native_id = _canonical_identity(identity)
        key = {field: value for field, value in zip(self.primary_key, identity, strict=True)}
        parsed_url = urlsplit(request_url)
        return Envelope(
            id=stable_record_id("rest", self.stream, native_id),
            source="rest",
            stream=self.stream,
            operation=Operation.SNAPSHOT,
            cursor=native_id,
            payload=RecordPayload(dict(record)),
            metadata={
                "key": key,
                "query_fingerprint": self._fingerprint,
                "snapshot_cycle": cycle,
            },
            provenance={
                "connector": "rest",
                "endpoint": urlunsplit(
                    (parsed_url.scheme, parsed_url.netloc, parsed_url.path, "", "")
                ),
            },
        )


def _validate_base_url(value: str, *, allow_http: bool) -> str:
    if not isinstance(value, str) or not value.strip() or any(ord(char) < 32 for char in value):
        raise ConfigurationError("REST base_url must be a non-empty URL")
    parsed = urlsplit(value.strip())
    if parsed.scheme not in {"https", "http"} or not parsed.hostname:
        raise ConfigurationError("REST base_url must be an absolute HTTPS URL")
    if parsed.username is not None or parsed.password is not None:
        raise ConfigurationError("REST base_url must not contain credentials")
    if parsed.query or parsed.fragment:
        raise ConfigurationError("REST base_url must not contain a query string or fragment")
    try:
        parsed_port = parsed.port
    except ValueError:
        raise ConfigurationError("REST base_url contains an invalid port") from None
    if parsed.scheme == "http" and (not allow_http or not _is_loopback_host(parsed.hostname)):
        raise ConfigurationError(
            "Plain HTTP is allowed only for loopback hosts with allow_http=True"
        )
    host = parsed.hostname.lower()
    if ":" in host:
        host = f"[{host}]"
    port = "" if parsed_port is None else f":{parsed_port}"
    path = parsed.path.rstrip("/")
    return urlunsplit((parsed.scheme.lower(), f"{host}{port}", path, "", ""))


def _is_loopback_host(host: str) -> bool:
    if host.lower() == "localhost":
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


def _validate_path(value: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ConfigurationError("REST path must be a non-empty relative or absolute path")
    if any(ord(char) < 32 for char in value) or "\\" in value:
        raise ConfigurationError("REST path contains unsafe characters")
    parsed = urlsplit(value.strip())
    if parsed.scheme or parsed.netloc or parsed.query or parsed.fragment:
        raise ConfigurationError("REST path must not contain an origin, query string, or fragment")
    if parsed.path.startswith("//"):
        raise ConfigurationError("REST path must not be a network-path reference")
    decoded_segments = [unquote(segment) for segment in parsed.path.split("/")]
    if any(segment in {".", ".."} for segment in decoded_segments):
        raise ConfigurationError("REST path must not contain traversal segments")
    return "/" + parsed.path.lstrip("/")


def _normalize_data_path(value: str | Sequence[str], field_name: str) -> tuple[str, ...]:
    if isinstance(value, str):
        pieces = () if not value else tuple(value.split("."))
    elif isinstance(value, Sequence):
        pieces = tuple(value)
    else:
        raise ConfigurationError(f"REST {field_name} must be a dotted string or string array")
    if any(
        not isinstance(piece, str)
        or not piece
        or "." in piece
        or any(ord(char) < 32 for char in piece)
        for piece in pieces
    ):
        raise ConfigurationError(f"REST {field_name} contains an invalid path component")
    return pieces


def _normalize_primary_key(value: Sequence[str]) -> tuple[str, ...]:
    if isinstance(value, (str, bytes)) or not isinstance(value, Sequence) or not value:
        raise ConfigurationError("REST primary_key must be a non-empty string array")
    fields: list[str] = []
    for item in value:
        if not isinstance(item, str):
            raise ConfigurationError("REST primary_key must contain only strings")
        path = _normalize_data_path(item, "primary_key")
        if not path:
            raise ConfigurationError("REST primary_key fields must not be empty")
        fields.append(".".join(path))
    if len(fields) != len(set(fields)):
        raise ConfigurationError("REST primary_key must not contain duplicate fields")
    return tuple(fields)


def _validate_parameter_name(value: str, field_name: str) -> str:
    if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_.-]*", value):
        raise ConfigurationError(f"REST {field_name} is not a safe query parameter name")
    return value


def _validate_query_params(
    values: Mapping[str, JsonScalar], cursor_param: str
) -> dict[str, JsonScalar]:
    if not isinstance(values, Mapping):
        raise ConfigurationError("REST query_params must be an object")
    result: dict[str, JsonScalar] = {}
    for key, value in values.items():
        safe_key = _validate_parameter_name(key, "query_params key")
        if _is_sensitive_query_name(safe_key):
            raise ConfigurationError(
                "REST authentication credentials must use SecretRef-backed headers, not URLs"
            )
        if safe_key == cursor_param:
            raise ConfigurationError("REST query_params must not preconfigure cursor_param")
        if not _is_json_scalar(value):
            raise ConfigurationError("REST query_params values must be JSON scalars")
        result[safe_key] = value
    return result


def _positive_int(value: int, field_name: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ConfigurationError(f"REST {field_name} must be a positive integer")


def _append_query(url: str, values: Mapping[str, JsonScalar]) -> str:
    if not values:
        return url
    parsed = urlsplit(url)
    query = urlencode([(key, _query_value(value)) for key, value in values.items()])
    return urlunsplit((parsed.scheme, parsed.netloc, parsed.path, query, ""))


def _replace_query_parameter(url: str, name: str, value: JsonScalar) -> str:
    parsed = urlsplit(url)
    pairs = [
        (key, item) for key, item in parse_qsl(parsed.query, keep_blank_values=True) if key != name
    ]
    pairs.append((name, _query_value(value)))
    return urlunsplit((parsed.scheme, parsed.netloc, parsed.path, urlencode(pairs), ""))


def _query_value(value: JsonScalar) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def _query_name_components(value: str) -> tuple[tuple[str, ...], str]:
    separated = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", value)
    components = tuple(item for item in re.split(r"[^A-Za-z0-9]+", separated.lower()) if item)
    return components, "".join(components)


def _is_pagination_token_query_name(value: str) -> bool:
    components, collapsed = _query_name_components(value)
    allowed_patterns = {
        ("after", "token"),
        ("before", "token"),
        ("continuation", "token"),
        ("cursor", "token"),
        ("next", "page", "token"),
        ("next", "token"),
        ("page", "token"),
        ("pagination", "token"),
    }
    return components in allowed_patterns or collapsed in {
        "aftertoken",
        "beforetoken",
        "continuationtoken",
        "cursortoken",
        "nextpagetoken",
        "nexttoken",
        "pagetoken",
        "paginationtoken",
    }


def _is_sensitive_query_name(value: str) -> bool:
    # Split separators and camelCase before comparing whole semantic components.
    # The suffix check also covers common all-lowercase spellings such as
    # ``refreshtoken`` without treating unrelated names such as ``monkey`` as keys.
    components, collapsed = _query_name_components(value)
    if any(item in _SENSITIVE_QUERY_COMPONENTS for item in components):
        return True
    if collapsed in _SENSITIVE_QUERY_COMPONENTS:
        return True
    return any(
        len(collapsed) > len(suffix) and collapsed.endswith(suffix)
        for suffix in _SENSITIVE_QUERY_SUFFIXES
    )


def _configuration_fingerprint(source: RestSource) -> str:
    semantic = {
        "base_url": source.base_url,
        "path": source.path,
        "stream": source.stream,
        "records_path": source.records_path,
        "primary_key": source.primary_key,
        "pagination": source.pagination,
        "next_cursor_path": source.next_cursor_path,
        "cursor_param": source.cursor_param,
        "query_params": source.query_params,
        "auth_type": source.auth_type,
        "api_key_header": source.api_key_header,
    }
    encoded = json.dumps(semantic, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


def _origin(url: str) -> tuple[str, str, int | None]:
    parsed = urlsplit(url)
    scheme = parsed.scheme.lower()
    try:
        port = parsed.port
    except ValueError:
        raise ProtocolError("REST URL contains an invalid port") from None
    if port is None:
        port = 443 if scheme == "https" else 80 if scheme == "http" else None
    return scheme, (parsed.hostname or "").lower(), port


def _same_origin(left: str, right: str) -> bool:
    return _origin(left) == _origin(right)


def _validate_request_url(
    value: str,
    *,
    base_url: str,
    allow_http: bool,
    allow_cross_origin: bool,
    allowed_sensitive_query_names: Iterable[str] = (),
    allow_pagination_tokens: bool = False,
) -> str:
    if not isinstance(value, str) or any(ord(char) < 32 for char in value):
        raise ProtocolError("REST pagination produced an invalid next URL")
    parsed = urlsplit(value)
    if parsed.scheme not in {"https", "http"} or not parsed.hostname:
        raise ProtocolError("REST pagination next URL must be absolute HTTP(S)")
    if parsed.username is not None or parsed.password is not None:
        raise ProtocolError("REST pagination next URL must not contain credentials")
    if parsed.fragment:
        raise ProtocolError("REST pagination next URL must not contain a fragment")
    if parsed.scheme == "http" and (not allow_http or not _is_loopback_host(parsed.hostname)):
        raise ProtocolError("REST pagination attempted an unsafe plain-HTTP request")
    if not allow_cross_origin and not _same_origin(value, base_url):
        raise ProtocolError("REST pagination attempted a cross-origin next link")
    allowed_names = {name.casefold() for name in allowed_sensitive_query_names}
    for key, _item in parse_qsl(parsed.query, keep_blank_values=True):
        if _is_sensitive_query_name(key) and not (
            key.casefold() in allowed_names
            or (allow_pagination_tokens and _is_pagination_token_query_name(key))
        ):
            raise ProtocolError("REST pagination next URL appears to contain credentials")
    return value


def _header_value(headers: Any, name: str) -> Any:
    if not isinstance(headers, Mapping):
        raise ProtocolError("REST response headers are malformed")
    expected = name.lower()
    for key, value in headers.items():
        if isinstance(key, str) and key.lower() == expected:
            return value
    return None


def _retry_after(value: Any) -> float | None:
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        seconds = float(value)
    except ValueError:
        try:
            parsed = parsedate_to_datetime(value)
        except (TypeError, ValueError, OverflowError):
            return None
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=UTC)
        seconds = (parsed.astimezone(UTC) - datetime.now(UTC)).total_seconds()
    if not math.isfinite(seconds):
        return None
    return min(max(seconds, 0.0), 300.0)


_MISSING = object()


def _extract_required(value: Any, path: Sequence[str], label: str) -> Any:
    extracted = _extract(value, path)
    if extracted is _MISSING:
        dotted = ".".join(path) or "<root>"
        raise ProtocolError(f"REST {label} path {dotted!r} is missing from the response")
    return extracted


def _extract_optional(value: Any, path: Sequence[str]) -> Any:
    extracted = _extract(value, path)
    return None if extracted is _MISSING else extracted


def _extract(value: Any, path: Sequence[str]) -> Any:
    current = value
    for component in path:
        if not isinstance(current, Mapping) or component not in current:
            return _MISSING
        current = current[component]
    return current


def _is_json_scalar(value: Any) -> bool:
    if value is None or isinstance(value, (str, bool, int)):
        return True
    return isinstance(value, float) and math.isfinite(value)


def _scalar_type(value: JsonScalar) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int):
        return "integer"
    if isinstance(value, float):
        return "number"
    return "string"


def _canonical_identity(values: Sequence[JsonScalar]) -> str:
    typed = [{"type": _scalar_type(value), "value": value} for value in values]
    return json.dumps(typed, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _request_hash(url: str) -> str:
    return hashlib.sha256(url.encode()).hexdigest()


def _page_fingerprint(records: Sequence[Mapping[str, Any]], next_url: str | None) -> str:
    try:
        encoded = json.dumps(
            {"records": list(records), "next_url": next_url},
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode()
    except (TypeError, ValueError):
        raise ProtocolError("REST response contains values that are not valid JSON") from None
    return hashlib.sha256(encoded).hexdigest()


def _parse_next_link(value: Any) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise ProtocolError("REST Link header must be a string")
    matches: list[str] = []
    for entry in _split_link_header(value):
        target_match = re.match(r"\s*<([^>]*)>(.*)$", entry)
        if target_match is None:
            raise ProtocolError("REST Link header is malformed")
        target, parameters = target_match.groups()
        relationships: list[str] = []
        for parameter in parameters.split(";"):
            if not parameter.strip():
                continue
            name, separator, raw_value = parameter.partition("=")
            if not separator:
                raise ProtocolError("REST Link header parameter is malformed")
            if name.strip().lower() == "rel":
                relationships.extend(raw_value.strip().strip('"').lower().split())
        if "next" in relationships:
            matches.append(target)
    if len(matches) > 1:
        raise ProtocolError("REST Link header contains multiple rel=next targets")
    return matches[0] if matches else None


def _split_link_header(value: str) -> tuple[str, ...]:
    entries: list[str] = []
    start = 0
    in_angle = False
    in_quote = False
    escaped = False
    for index, character in enumerate(value):
        if escaped:
            escaped = False
        elif character == "\\" and in_quote:
            escaped = True
        elif character == '"':
            in_quote = not in_quote
        elif character == "<" and not in_quote:
            in_angle = True
        elif character == ">" and not in_quote:
            in_angle = False
        elif character == "," and not in_angle and not in_quote:
            entries.append(value[start:index].strip())
            start = index + 1
    entries.append(value[start:].strip())
    if in_angle or in_quote or any(not entry for entry in entries):
        raise ProtocolError("REST Link header is malformed")
    return tuple(entries)


def _encode_state(
    fingerprint: str,
    request_url: str,
    *,
    cycle: int,
    record_offset: int = 0,
    page_fingerprint: str | None = None,
    primary_key_types: tuple[str, ...] | None = None,
    record_field_types: Mapping[str, tuple[str, ...]] | None = None,
    completed_request_hashes: Iterable[str] = (),
) -> dict[str, Any]:
    state: dict[str, Any] = {
        "version": 1,
        "configuration_fingerprint": fingerprint,
        "request_url": request_url,
        "cycle": cycle,
        "record_offset": record_offset,
    }
    if page_fingerprint is not None:
        state["page_fingerprint"] = page_fingerprint
    if primary_key_types is not None:
        state["primary_key_types"] = list(primary_key_types)
    if record_field_types:
        state["record_field_types"] = {
            path: list(types) for path, types in sorted(record_field_types.items())
        }
    completed = sorted(set(completed_request_hashes))
    if len(completed) > _MAX_COMPLETED_REQUEST_HASHES:
        raise ProtocolError("REST pagination exceeded the cycle page-history limit")
    if completed:
        state["completed_request_hashes"] = completed
    return state


def _decode_state(
    state: Mapping[str, Any] | None,
    fingerprint: str,
    endpoint_url: str,
) -> _ResumePoint:
    if not state:
        return _ResumePoint(endpoint_url)
    if not isinstance(state, Mapping):
        raise ConfigurationError("REST checkpoint must be an object")
    if state.get("version") != 1:
        raise ConfigurationError("REST checkpoint version is unsupported")
    if state.get("configuration_fingerprint") != fingerprint:
        raise ConfigurationError(
            "REST checkpoint does not match this source configuration; clear saved state"
        )
    request_url = state.get("request_url")
    if not isinstance(request_url, str) or not request_url:
        raise ConfigurationError("REST checkpoint request_url must be a non-empty string")
    cycle = state.get("cycle")
    if isinstance(cycle, bool) or not isinstance(cycle, int) or cycle < 0:
        raise ConfigurationError("REST checkpoint cycle must be a non-negative integer")
    record_offset = state.get("record_offset", 0)
    if isinstance(record_offset, bool) or not isinstance(record_offset, int) or record_offset < 0:
        raise ConfigurationError("REST checkpoint record_offset must be a non-negative integer")
    page_fingerprint = state.get("page_fingerprint")
    if page_fingerprint is not None and (
        not isinstance(page_fingerprint, str)
        or re.fullmatch(r"[0-9a-f]{64}", page_fingerprint) is None
    ):
        raise ConfigurationError("REST checkpoint page_fingerprint is invalid")
    if record_offset > 0 and page_fingerprint is None:
        raise ConfigurationError("REST checkpoint with record_offset requires page_fingerprint")
    if record_offset == 0 and page_fingerprint is not None:
        raise ConfigurationError(
            "REST checkpoint page_fingerprint requires a positive record_offset"
        )
    raw_types = state.get("primary_key_types")
    primary_key_types: tuple[str, ...] | None = None
    if raw_types is not None:
        if (
            not isinstance(raw_types, list)
            or not raw_types
            or any(item not in {"boolean", "integer", "number", "string"} for item in raw_types)
        ):
            raise ConfigurationError("REST checkpoint primary_key_types is invalid")
        primary_key_types = tuple(raw_types)
    raw_field_types = state.get("record_field_types")
    record_field_types: dict[str, tuple[str, ...]] | None = None
    if raw_field_types is not None:
        if not isinstance(raw_field_types, Mapping):
            raise ConfigurationError("REST checkpoint record_field_types is invalid")
        record_field_types = {}
        for path, types in raw_field_types.items():
            if (
                not isinstance(path, str)
                or not path.startswith("/")
                or not isinstance(types, list)
                or len(types) > 1
                or any(
                    item not in {"array", "boolean", "integer", "number", "object", "string"}
                    for item in types
                )
            ):
                raise ConfigurationError("REST checkpoint record_field_types is invalid")
            record_field_types[path] = tuple(types)
    raw_completed = state.get("completed_request_hashes", [])
    if (
        not isinstance(raw_completed, list)
        or len(raw_completed) > _MAX_COMPLETED_REQUEST_HASHES
        or len(raw_completed) != len(set(raw_completed))
        or any(
            not isinstance(item, str) or re.fullmatch(r"[0-9a-f]{64}", item) is None
            for item in raw_completed
        )
    ):
        raise ConfigurationError("REST checkpoint completed_request_hashes is invalid")
    return _ResumePoint(
        request_url=request_url,
        cycle=cycle,
        record_offset=record_offset,
        page_fingerprint=page_fingerprint,
        primary_key_types=primary_key_types,
        record_field_types=record_field_types,
        completed_request_hashes=tuple(raw_completed),
    )


def _infer_records_schema(records: Sequence[Mapping[str, Any]]) -> Mapping[str, Any]:
    return _infer_object_schema(records)


def _record_field_types(
    records: Sequence[Mapping[str, Any]],
) -> dict[str, tuple[str, ...]]:
    observed: dict[str, set[str]] = {}

    def visit(value: Any, path: str) -> None:
        value_type = _json_type(value)
        if value_type != "null":
            observed.setdefault(path, set()).add(value_type)
        if isinstance(value, Mapping):
            for key, child in value.items():
                escaped = str(key).replace("~", "~0").replace("/", "~1")
                visit(child, f"{path}/{escaped}")
        elif isinstance(value, list):
            for child in value:
                visit(child, f"{path}/*")

    for record in records:
        for key, value in record.items():
            escaped = str(key).replace("~", "~0").replace("/", "~1")
            visit(value, f"/{escaped}")
    return {path: tuple(sorted(types)) for path, types in observed.items()}


def _merge_record_field_types(
    existing: Mapping[str, tuple[str, ...]] | None,
    observed: Mapping[str, tuple[str, ...]],
) -> dict[str, tuple[str, ...]]:
    merged = dict(existing or {})
    for path, observed_types in observed.items():
        if len(observed_types) > 1:
            raise ProtocolError(f"REST response field {path!r} changed JSON type within a page")
        prior_types = merged.get(path, ())
        combined = tuple(sorted(set(prior_types) | set(observed_types)))
        if len(combined) > 1:
            raise ProtocolError(f"REST response field {path!r} changed JSON type between pages")
        merged[path] = combined
    return merged


def _infer_object_schema(values: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    if not values:
        return {"type": "object", "properties": {}, "additionalProperties": True}
    keys = sorted({str(key) for value in values for key in value})
    required = sorted(set.intersection(*(set(value) for value in values)))
    properties = {
        key: _infer_values([value[key] for value in values if key in value]) for key in keys
    }
    schema: dict[str, Any] = {
        "type": "object",
        "properties": properties,
        "additionalProperties": True,
    }
    if required:
        schema["required"] = required
    return schema


def _infer_values(values: Sequence[Any]) -> dict[str, Any]:
    types = sorted({_json_type(value) for value in values})
    if types == ["object"]:
        objects = [value for value in values if isinstance(value, Mapping)]
        return _infer_object_schema(objects)
    if types == ["array"]:
        children = [child for value in values if isinstance(value, list) for child in value]
        return {"type": "array", "items": _infer_values(children) if children else {}}
    if len(types) == 1:
        return {"type": types[0]}
    return {"type": types}


def _json_type(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int):
        return "integer"
    if isinstance(value, float):
        return "number"
    if isinstance(value, str):
        return "string"
    if isinstance(value, Mapping):
        return "object"
    if isinstance(value, list):
        return "array"
    raise ProtocolError("REST response contains a value outside the JSON data model")
