"""Resumable, robots-aware ingestion for public HTML sitemap pages."""

from __future__ import annotations

import asyncio
import hashlib
import json
import math
import re
from collections.abc import AsyncIterator, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from html.parser import HTMLParser
from typing import Any
from urllib.parse import urljoin, urlsplit, urlunsplit
from urllib.robotparser import RobotFileParser
from xml.etree import ElementTree

from ingestion_graph.connectors.base import (
    CheckResult,
    ConnectorCapabilities,
    ConnectorSpec,
    Source,
    StreamDescriptor,
)
from ingestion_graph.errors import ConfigurationError, ProtocolError, RateLimitError
from ingestion_graph.messages import RecordMessage, SourceMessage, StateMessage
from ingestion_graph.models import DocumentElement, Envelope, Operation, Tombstone, stable_record_id

try:
    import httpx
except ImportError:  # pragma: no cover - exercised without the optional extra
    httpx = None  # type: ignore[assignment]


_STREAM_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")
_EXTRACTOR_VERSION = "1"
_MAX_SITEMAP_BYTES = 16 * 1024 * 1024
_MAX_PAGE_BYTES = 8 * 1024 * 1024
_SITEMAP_NAMESPACE = "http://www.sitemaps.org/schemas/sitemap/0.9"


@dataclass(frozen=True, slots=True)
class _SitemapEntry:
    url: str
    lastmod: str | None


@dataclass(frozen=True, slots=True)
class _HtmlPage:
    title: str
    canonical_url: str
    text: str


class _VisibleHtmlParser(HTMLParser):
    _BLOCK_TAGS = frozenset(
        {
            "article",
            "blockquote",
            "br",
            "dd",
            "div",
            "dl",
            "dt",
            "h1",
            "h2",
            "h3",
            "h4",
            "h5",
            "h6",
            "li",
            "p",
            "section",
            "table",
            "td",
            "th",
            "tr",
            "ul",
        }
    )
    _SKIP_TAGS = frozenset({"footer", "nav", "noscript", "script", "style", "svg"})

    def __init__(self, request_url: str) -> None:
        super().__init__(convert_charrefs=True)
        self.request_url = request_url
        self.main_depth = 0
        self.skip_depth = 0
        self.title_depth = 0
        self.title_parts: list[str] = []
        self.parts: list[str] = []
        self.canonical_url: str | None = None

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        lowered = tag.casefold()
        values = {key.casefold(): value for key, value in attrs}
        if lowered == "main":
            self.main_depth += 1
        if lowered in self._SKIP_TAGS and self.main_depth:
            self.skip_depth += 1
        if lowered == "title":
            self.title_depth += 1
        if lowered == "link" and "canonical" in str(values.get("rel") or "").casefold():
            href = str(values.get("href") or "").strip()
            if href:
                self.canonical_url = urljoin(self.request_url, href)
        if lowered in self._BLOCK_TAGS and self.main_depth and not self.skip_depth:
            self.parts.append("\n")

    def handle_startendtag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        self.handle_starttag(tag, attrs)
        self.handle_endtag(tag)

    def handle_endtag(self, tag: str) -> None:
        lowered = tag.casefold()
        if lowered in self._BLOCK_TAGS and self.main_depth and not self.skip_depth:
            self.parts.append("\n")
        if lowered == "title" and self.title_depth:
            self.title_depth -= 1
        if lowered in self._SKIP_TAGS and self.main_depth and self.skip_depth:
            self.skip_depth -= 1
        if lowered == "main" and self.main_depth:
            self.main_depth -= 1

    def handle_data(self, data: str) -> None:
        if self.title_depth:
            self.title_parts.append(data)
        if self.main_depth and not self.skip_depth:
            self.parts.append(data)

    def page(self) -> _HtmlPage:
        title = _collapse_inline(" ".join(self.title_parts))
        lines = [_collapse_inline(line) for line in "".join(self.parts).splitlines()]
        text = "\n".join(line for line in lines if line)
        if not text:
            raise ProtocolError("Sitemap HTML page has no visible <main> content")
        return _HtmlPage(
            title=title,
            canonical_url=self.canonical_url or self.request_url,
            text=text,
        )


class SitemapHtmlSource(Source):
    """Read same-origin public HTML pages enumerated by an XML sitemap.

    The source consults ``robots.txt`` before discovery and every crawl, follows
    only same-origin sitemap children, bounds every response, and checkpoints a
    complete URL/content manifest. Unchanged ``lastmod`` entries are not
    refetched; removed URLs emit tombstones.
    """

    def __init__(
        self,
        sitemap_url: str,
        *,
        stream: str,
        allowed_path_prefixes: Sequence[str],
        user_agent: str = "ingestion-graph-sitemap/1.0",
        max_urls: int = 5_000,
        max_sitemaps: int = 20,
        checkpoint_interval: int = 25,
        request_timeout: float = 30.0,
        request_delay_seconds: float = 0.1,
        max_retries: int = 4,
        client: Any = None,
        sleep: Any = asyncio.sleep,
    ) -> None:
        self.sitemap_url = _validate_root_url(sitemap_url)
        if not isinstance(stream, str) or not _STREAM_NAME.fullmatch(stream):
            raise ConfigurationError("Sitemap stream name is invalid")
        prefixes = tuple(_validate_path_prefix(value) for value in allowed_path_prefixes)
        if not prefixes:
            raise ConfigurationError("Sitemap allowed_path_prefixes must not be empty")
        if not isinstance(user_agent, str) or not user_agent.strip() or "\n" in user_agent:
            raise ConfigurationError("Sitemap user_agent is invalid")
        for value, label in (
            (max_urls, "max_urls"),
            (max_sitemaps, "max_sitemaps"),
            (checkpoint_interval, "checkpoint_interval"),
        ):
            if isinstance(value, bool) or not isinstance(value, int) or value < 1:
                raise ConfigurationError(f"Sitemap {label} must be a positive integer")
        if isinstance(max_retries, bool) or not isinstance(max_retries, int) or max_retries < 0:
            raise ConfigurationError("Sitemap max_retries must be a non-negative integer")
        for numeric_value, label, allow_zero in (
            (request_timeout, "request_timeout", False),
            (request_delay_seconds, "request_delay_seconds", True),
        ):
            if isinstance(numeric_value, bool) or not isinstance(
                numeric_value, (int, float)
            ):
                raise ConfigurationError(f"Sitemap {label} must be numeric")
            if (
                not math.isfinite(float(numeric_value))
                or numeric_value < 0
                or (not allow_zero and numeric_value == 0)
            ):
                raise ConfigurationError(f"Sitemap {label} is outside its allowed range")

        self.stream = stream
        self.allowed_path_prefixes = prefixes
        self.user_agent = user_agent.strip()
        self.max_urls = max_urls
        self.max_sitemaps = max_sitemaps
        self.checkpoint_interval = checkpoint_interval
        self.request_timeout = float(request_timeout)
        self.request_delay_seconds = float(request_delay_seconds)
        self.max_retries = max_retries
        self._client = client
        self._owns_client = client is None
        self._sleep = sleep
        self._origin = _origin(self.sitemap_url)
        self._robots_url = f"{self._origin}/robots.txt"
        self._fingerprint = _configuration_fingerprint(self)

    @classmethod
    def manifest(cls) -> ConnectorSpec:
        return ConnectorSpec(
            name="sitemap_html",
            version="1.0.0",
            config_schema={
                "type": "object",
                "properties": {
                    "sitemap_url": {"type": "string", "format": "uri"},
                    "stream": {"type": "string", "minLength": 1},
                    "allowed_path_prefixes": {
                        "type": "array",
                        "items": {"type": "string", "minLength": 1},
                        "minItems": 1,
                    },
                    "user_agent": {"type": "string", "default": "ingestion-graph-sitemap/1.0"},
                    "max_urls": {"type": "integer", "minimum": 1, "default": 5_000},
                    "max_sitemaps": {"type": "integer", "minimum": 1, "default": 20},
                    "checkpoint_interval": {"type": "integer", "minimum": 1, "default": 25},
                    "request_timeout": {"type": "number", "exclusiveMinimum": 0, "default": 30},
                    "request_delay_seconds": {"type": "number", "minimum": 0, "default": 0.1},
                    "max_retries": {"type": "integer", "minimum": 0, "default": 4},
                },
                "required": ["sitemap_url", "stream", "allowed_path_prefixes"],
                "additionalProperties": False,
            },
            capabilities=ConnectorCapabilities(
                incremental=True,
                resumable_full_refresh=True,
                deletes=True,
                rate_limits=True,
            ),
        )

    def spec(self) -> ConnectorSpec:
        return self.manifest()

    async def check(self) -> CheckResult:
        try:
            robots = await self._robots()
            entries = await self._entries(robots)
            if not entries:
                raise ProtocolError("Sitemap contains no allowed HTML pages")
            await self._page(entries[0].url, robots)
            return CheckResult(True, f"Sitemap exposes {len(entries)} allowed HTML pages")
        except (ConfigurationError, ProtocolError, RateLimitError) as exc:
            return CheckResult(False, str(exc))

    async def discover(self) -> Sequence[StreamDescriptor]:
        robots = await self._robots()
        entries = await self._entries(robots)
        if not entries:
            raise ProtocolError("Sitemap contains no allowed HTML pages")
        return (StreamDescriptor(self.stream, namespace="sitemap.html"),)

    async def read(
        self,
        stream: StreamDescriptor,
        state: Mapping[str, Any] | None = None,
    ) -> AsyncIterator[SourceMessage]:
        if stream.name != self.stream:
            raise ConfigurationError(f"Sitemap stream {stream.name!r} is not configured")
        decoded = _decode_state(state, self._fingerprint)
        previous = decoded["records"]
        cycle = decoded["cycle"]
        robots = await self._robots()
        entries = await self._entries(robots)
        snapshot_hash = _entries_hash(entries)
        progress = decoded.get("in_progress")
        current: dict[str, dict[str, str]]
        if progress:
            if progress.get("snapshot_hash") != snapshot_hash:
                raise ProtocolError(
                    "Sitemap changed during a resumed crawl; clear in-progress state or retry"
                )
            position = int(progress["position"])
            current = dict(progress["current"])
            phase = str(progress.get("phase") or "crawl")
            deletion_position = int(progress.get("deletion_position") or 0)
        else:
            position = 0
            current = {}
            phase = "crawl"
            deletion_position = 0

        if phase == "crawl":
            for index in range(position, len(entries)):
                entry = entries[index]
                old = previous.get(entry.url)
                emitted = False
                if isinstance(old, Mapping) and old.get("lastmod") == entry.lastmod and old.get(
                    "checksum"
                ):
                    current[entry.url] = {str(key): str(value) for key, value in old.items()}
                else:
                    page = await self._page(entry.url, robots)
                    checksum = hashlib.sha256(page.text.encode()).hexdigest()
                    current[entry.url] = {
                        "checksum": checksum,
                        "lastmod": entry.lastmod or "",
                        "title": page.title,
                        "canonical_url": page.canonical_url,
                    }
                    yield RecordMessage(self._envelope(entry, page, checksum, cycle))
                    emitted = True
                next_position = index + 1
                if emitted or next_position % self.checkpoint_interval == 0:
                    yield StateMessage(
                        self.stream,
                        _encode_state(
                            self._fingerprint,
                            cycle,
                            previous,
                            snapshot_hash=snapshot_hash,
                            position=next_position,
                            current=current,
                            phase="crawl",
                        ),
                    )
            phase = "delete"

        deleted_urls = sorted(set(previous) - set(current))
        for index in range(deletion_position, len(deleted_urls)):
            url = deleted_urls[index]
            yield RecordMessage(
                Envelope(
                    id=stable_record_id("sitemap_html", self.stream, url),
                    source="sitemap_html",
                    stream=self.stream,
                    payload=Tombstone("URL removed from sitemap"),
                    operation=Operation.DELETE,
                    cursor=url,
                    provenance={"url": url, "sitemap_url": self.sitemap_url},
                )
            )
            yield StateMessage(
                self.stream,
                _encode_state(
                    self._fingerprint,
                    cycle,
                    previous,
                    snapshot_hash=snapshot_hash,
                    position=len(entries),
                    current=current,
                    phase="delete",
                    deletion_position=index + 1,
                ),
            )
        yield StateMessage(
            self.stream,
            {
                "version": 1,
                "configuration_fingerprint": self._fingerprint,
                "cycle": cycle + 1,
                "records": current,
            },
        )

    async def close(self) -> None:
        if self._owns_client and self._client is not None:
            await self._client.aclose()
            self._client = None

    async def _robots(self) -> RobotFileParser:
        text = await self._request_text(self._robots_url, _MAX_SITEMAP_BYTES)
        parser = RobotFileParser(self._robots_url)
        parser.parse(text.splitlines())
        if not parser.can_fetch(self.user_agent, self.sitemap_url):
            raise ConfigurationError("robots.txt disallows the configured sitemap")
        return parser

    async def _entries(self, robots: RobotFileParser) -> tuple[_SitemapEntry, ...]:
        pending = [self.sitemap_url]
        seen_sitemaps: set[str] = set()
        entries: dict[str, _SitemapEntry] = {}
        while pending:
            sitemap_url = pending.pop(0)
            if sitemap_url in seen_sitemaps:
                continue
            if len(seen_sitemaps) >= self.max_sitemaps:
                raise ProtocolError("Sitemap index exceeded max_sitemaps")
            _validate_same_origin_url(sitemap_url, self._origin)
            if not robots.can_fetch(self.user_agent, sitemap_url):
                raise ConfigurationError("robots.txt disallows a sitemap child")
            seen_sitemaps.add(sitemap_url)
            xml = await self._request_text(sitemap_url, _MAX_SITEMAP_BYTES)
            try:
                root = ElementTree.fromstring(xml)
            except ElementTree.ParseError:
                raise ProtocolError("Sitemap returned malformed XML") from None
            kind = _local_name(root.tag)
            if kind == "sitemapindex":
                for node in root.findall(f"{{{_SITEMAP_NAMESPACE}}}sitemap"):
                    loc = node.findtext(f"{{{_SITEMAP_NAMESPACE}}}loc")
                    if loc:
                        pending.append(loc.strip())
                continue
            if kind != "urlset":
                raise ProtocolError("Sitemap root must be urlset or sitemapindex")
            for node in root.findall(f"{{{_SITEMAP_NAMESPACE}}}url"):
                loc = node.findtext(f"{{{_SITEMAP_NAMESPACE}}}loc")
                if not loc:
                    continue
                url = _validate_same_origin_url(loc.strip(), self._origin)
                if not any(
                    urlsplit(url).path.startswith(prefix)
                    for prefix in self.allowed_path_prefixes
                ):
                    continue
                if not robots.can_fetch(self.user_agent, url):
                    continue
                lastmod = node.findtext(f"{{{_SITEMAP_NAMESPACE}}}lastmod")
                entries[url] = _SitemapEntry(url, lastmod.strip() if lastmod else None)
                if len(entries) > self.max_urls:
                    raise ProtocolError("Sitemap exceeded max_urls")
        return tuple(entries[url] for url in sorted(entries))

    async def _page(self, url: str, robots: RobotFileParser) -> _HtmlPage:
        _validate_same_origin_url(url, self._origin)
        if not robots.can_fetch(self.user_agent, url):
            raise ConfigurationError("robots.txt disallows an HTML page")
        html = await self._request_text(url, _MAX_PAGE_BYTES)
        parser = _VisibleHtmlParser(url)
        try:
            parser.feed(html)
            page = parser.page()
        except (UnicodeError, ValueError):
            raise ProtocolError("Sitemap HTML page could not be parsed") from None
        canonical = _validate_same_origin_url(page.canonical_url, self._origin)
        if not any(
            urlsplit(canonical).path.startswith(prefix)
            for prefix in self.allowed_path_prefixes
        ):
            raise ProtocolError("Sitemap HTML canonical URL is outside allowed paths")
        if self.request_delay_seconds:
            await self._sleep(self.request_delay_seconds)
        return _HtmlPage(page.title, canonical, page.text)

    async def _request_text(self, url: str, maximum_bytes: int) -> str:
        _validate_same_origin_url(url, self._origin)
        client = await self._get_client()
        for attempt in range(self.max_retries + 1):
            try:
                response = await client.request("GET", url, follow_redirects=False)
            except Exception:
                raise ConfigurationError(
                    "Sitemap request failed before receiving a response"
                ) from None
            status = int(response.status_code)
            if status == 429 or 500 <= status < 600:
                if attempt >= self.max_retries:
                    if status == 429:
                        raise RateLimitError("Sitemap rate-limit retry budget was exhausted")
                    raise ProtocolError("Sitemap server retry budget was exhausted")
                await self._sleep(min(float(2**attempt), 30.0))
                continue
            if status >= 300:
                raise ConfigurationError(f"Sitemap endpoint returned HTTP {status}")
            content = bytes(response.content)
            if len(content) > maximum_bytes:
                raise ProtocolError("Sitemap response exceeded its byte limit")
            encoding = getattr(response, "encoding", None) or "utf-8"
            try:
                return content.decode(encoding)
            except (LookupError, UnicodeDecodeError):
                raise ProtocolError("Sitemap response encoding is invalid") from None
        raise ProtocolError("Sitemap retry loop ended unexpectedly")  # pragma: no cover

    async def _get_client(self) -> Any:
        if self._client is None:
            if httpx is None:
                raise ConfigurationError(
                    "Sitemap support requires: pip install 'ingestion-graph[sitemap]'"
                )
            self._client = httpx.AsyncClient(
                timeout=self.request_timeout,
                follow_redirects=False,
                headers={
                    "Accept": "text/html, application/xml;q=0.9, text/xml;q=0.9",
                    "User-Agent": self.user_agent,
                },
            )
        return self._client

    def _envelope(
        self,
        entry: _SitemapEntry,
        page: _HtmlPage,
        checksum: str,
        cycle: int,
    ) -> Envelope:
        return Envelope(
            id=stable_record_id("sitemap_html", self.stream, entry.url),
            source="sitemap_html",
            stream=self.stream,
            payload=DocumentElement(page.text, element_type="html_page"),
            operation=Operation.SNAPSHOT,
            event_time=_parse_lastmod(entry.lastmod),
            cursor=entry.url,
            checksum=checksum,
            metadata={
                "title": page.title,
                "canonical_url": page.canonical_url,
                "sitemap_lastmod": entry.lastmod or "",
                "media_type": "text/html",
            },
            provenance={"url": entry.url, "sitemap_url": self.sitemap_url, "cycle": cycle},
        )


def _validate_root_url(value: str) -> str:
    if not isinstance(value, str) or any(ord(char) < 32 for char in value):
        raise ConfigurationError("Sitemap URL is invalid")
    parsed = urlsplit(value)
    if parsed.scheme != "https" or not parsed.hostname:
        raise ConfigurationError("Sitemap URL must use HTTPS")
    if parsed.username is not None or parsed.password is not None:
        raise ConfigurationError("Sitemap URL must not contain credentials")
    if parsed.query or parsed.fragment:
        raise ConfigurationError("Sitemap URL must not contain a query or fragment")
    return urlunsplit((parsed.scheme, parsed.netloc, parsed.path or "/", "", ""))


def _validate_same_origin_url(value: str, origin: str) -> str:
    normalized = _validate_root_url(value)
    if _origin(normalized) != origin:
        raise ProtocolError("Sitemap attempted a cross-origin URL")
    return normalized


def _validate_path_prefix(value: str) -> str:
    if not isinstance(value, str) or not value.startswith("/") or "?" in value or "#" in value:
        raise ConfigurationError("Sitemap allowed path prefixes must be absolute paths")
    return value


def _origin(value: str) -> str:
    parsed = urlsplit(value)
    return f"{parsed.scheme}://{parsed.netloc}"


def _collapse_inline(value: str) -> str:
    return " ".join(value.split())


def _local_name(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


def _entries_hash(entries: Sequence[_SitemapEntry]) -> str:
    value = [[entry.url, entry.lastmod] for entry in entries]
    return hashlib.sha256(json.dumps(value, separators=(",", ":")).encode()).hexdigest()


def _configuration_fingerprint(source: SitemapHtmlSource) -> str:
    value = {
        "sitemap_url": source.sitemap_url,
        "stream": source.stream,
        "allowed_path_prefixes": source.allowed_path_prefixes,
        "extractor_version": _EXTRACTOR_VERSION,
    }
    return hashlib.sha256(json.dumps(value, sort_keys=True).encode()).hexdigest()


def _parse_lastmod(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _encode_state(
    fingerprint: str,
    cycle: int,
    previous: Mapping[str, Mapping[str, str]],
    *,
    snapshot_hash: str,
    position: int,
    current: Mapping[str, Mapping[str, str]],
    phase: str,
    deletion_position: int = 0,
) -> dict[str, Any]:
    return {
        "version": 1,
        "configuration_fingerprint": fingerprint,
        "cycle": cycle,
        "records": previous,
        "in_progress": {
            "snapshot_hash": snapshot_hash,
            "position": position,
            "current": current,
            "phase": phase,
            "deletion_position": deletion_position,
        },
    }


def _decode_state(state: Mapping[str, Any] | None, fingerprint: str) -> dict[str, Any]:
    if not state:
        return {"cycle": 0, "records": {}}
    if state.get("version") != 1:
        raise ConfigurationError("Sitemap checkpoint version is unsupported")
    if state.get("configuration_fingerprint") != fingerprint:
        raise ConfigurationError(
            "Sitemap checkpoint does not match this source configuration; clear saved state"
        )
    cycle = state.get("cycle")
    records = state.get("records")
    if isinstance(cycle, bool) or not isinstance(cycle, int) or cycle < 0:
        raise ConfigurationError("Sitemap checkpoint cycle is invalid")
    if not isinstance(records, Mapping):
        raise ConfigurationError("Sitemap checkpoint records must be an object")
    normalized_records: dict[str, dict[str, str]] = {}
    for url, record in records.items():
        if not isinstance(url, str) or not isinstance(record, Mapping):
            raise ConfigurationError("Sitemap checkpoint record is malformed")
        normalized_records[url] = {str(key): str(value) for key, value in record.items()}
    decoded: dict[str, Any] = {"cycle": cycle, "records": normalized_records}
    progress = state.get("in_progress")
    if progress is not None:
        if not isinstance(progress, Mapping) or not isinstance(progress.get("current"), Mapping):
            raise ConfigurationError("Sitemap in-progress checkpoint is malformed")
        position = progress.get("position")
        deletion_position = progress.get("deletion_position", 0)
        if any(
            isinstance(value, bool) or not isinstance(value, int) or value < 0
            for value in (position, deletion_position)
        ):
            raise ConfigurationError("Sitemap in-progress offsets are invalid")
        decoded["in_progress"] = {
            "snapshot_hash": str(progress.get("snapshot_hash") or ""),
            "position": position,
            "current": {
                str(url): {str(key): str(value) for key, value in record.items()}
                for url, record in progress["current"].items()
                if isinstance(record, Mapping)
            },
            "phase": str(progress.get("phase") or "crawl"),
            "deletion_position": deletion_position,
        }
    return decoded
