from __future__ import annotations

from collections import defaultdict, deque
from collections.abc import Mapping
from typing import Any

import pytest

from ingestion_graph.conformance import inspect_source_messages
from ingestion_graph.connectors.base import StreamDescriptor
from ingestion_graph.errors import ConfigurationError
from ingestion_graph.messages import RecordMessage, StateMessage
from ingestion_graph.models import DocumentElement, Operation, Tombstone
from ingestion_graph.sources import SitemapHtmlSource

ORIGIN = "https://example.test"
SITEMAP = f"{ORIGIN}/sitemap-units.xml"
ROBOTS = b"User-agent: *\nAllow: /\nDisallow: /_api/\n"


class FakeResponse:
    def __init__(self, content: bytes | str, status_code: int = 200) -> None:
        self.content = content.encode() if isinstance(content, str) else content
        self.status_code = status_code
        self.encoding = "utf-8"


class FakeClient:
    def __init__(self, responses: Mapping[str, list[FakeResponse]]) -> None:
        self.responses = defaultdict(deque)
        for url, values in responses.items():
            self.responses[url].extend(values)
        self.requests: list[str] = []
        self.closed = False

    async def request(self, method: str, url: str, **_kwargs: Any) -> FakeResponse:
        assert method == "GET"
        self.requests.append(url)
        return self.responses[url].popleft()

    async def aclose(self) -> None:
        self.closed = True


def sitemap(*entries: tuple[str, str]) -> str:
    body = "".join(
        f"<url><loc>{url}</loc><lastmod>{lastmod}</lastmod></url>"
        for url, lastmod in entries
    )
    return (
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">'
        f"{body}</urlset>"
    )


def html(url: str, title: str, body: str) -> str:
    return (
        "<html><head>"
        f"<title>{title}</title><link rel='canonical' href='{url}'>"
        "</head><body><nav>Ignore navigation</nav>"
        f"<main><h1>{title}</h1><p>{body}</p><nav>Ignore related links</nav></main>"
        "<footer>Ignore footer</footer></body></html>"
    )


def source(client: FakeClient, **kwargs: Any) -> SitemapHtmlSource:
    options = {
        "stream": "broken-arrow-units",
        "allowed_path_prefixes": ("/en/units/",),
        "client": client,
        "request_delay_seconds": 0,
    }
    options.update(kwargs)
    return SitemapHtmlSource(SITEMAP, **options)


async def collect(
    current: SitemapHtmlSource, state: Mapping[str, Any] | None = None
) -> list[Any]:
    return [
        item
        async for item in current.read(StreamDescriptor("broken-arrow-units"), state)
    ]


def final_state(messages: list[Any]) -> Mapping[str, Any]:
    return [item.state for item in messages if isinstance(item, StateMessage)][-1]


@pytest.mark.parametrize(
    "url",
    [
        "http://example.test/sitemap.xml",
        "https://user:secret@example.test/sitemap.xml",
        "https://example.test/sitemap.xml?token=bad",
    ],
)
def test_sitemap_rejects_unsafe_root_urls(url: str) -> None:
    with pytest.raises(ConfigurationError):
        SitemapHtmlSource(
            url,
            stream="units",
            allowed_path_prefixes=("/units/",),
            client=FakeClient({}),
        )


@pytest.mark.asyncio
async def test_sitemap_emits_canonical_visible_html_documents() -> None:
    first = f"{ORIGIN}/en/units/55/"
    second = f"{ORIGIN}/en/units/115/"
    client = FakeClient(
        {
            f"{ORIGIN}/robots.txt": [FakeResponse(ROBOTS)],
            SITEMAP: [FakeResponse(sitemap((first, "2026-08-10"), (second, "2026-08-06")))],
            first: [FakeResponse(html(first, "Marine Raiders CQC", "Cost 100 HP 70"))],
            second: [FakeResponse(html(second, "Delta Force", "Cost 110 HP 60"))],
        }
    )
    current = source(client)

    messages = await collect(current)

    emitted = [item.envelope for item in messages if isinstance(item, RecordMessage)]
    assert len(emitted) == 2
    assert all(isinstance(item.payload, DocumentElement) for item in emitted)
    by_title = {item.metadata["title"]: item for item in emitted}
    marine = by_title["Marine Raiders CQC"]
    assert "Ignore navigation" not in marine.payload.text
    assert marine.metadata["canonical_url"] == first
    assert marine.metadata["sitemap_lastmod"] == "2026-08-10"
    assert marine.event_time is not None
    report = inspect_source_messages(
        current,
        StreamDescriptor("broken-arrow-units"),
        messages,
    )
    assert report.ok, report.issues


@pytest.mark.asyncio
async def test_unchanged_lastmod_skips_page_refetch() -> None:
    page_url = f"{ORIGIN}/en/units/55/"
    first_client = FakeClient(
        {
            f"{ORIGIN}/robots.txt": [FakeResponse(ROBOTS)],
            SITEMAP: [FakeResponse(sitemap((page_url, "2026-08-10")))],
            page_url: [FakeResponse(html(page_url, "Marine Raiders CQC", "HP 70"))],
        }
    )
    first_messages = await collect(source(first_client))
    second_client = FakeClient(
        {
            f"{ORIGIN}/robots.txt": [FakeResponse(ROBOTS)],
            SITEMAP: [FakeResponse(sitemap((page_url, "2026-08-10")))],
        }
    )

    second_messages = await collect(source(second_client), final_state(first_messages))

    assert not [item for item in second_messages if isinstance(item, RecordMessage)]
    assert page_url not in second_client.requests
    assert final_state(second_messages)["cycle"] == 2


@pytest.mark.asyncio
async def test_changed_lastmod_refetches_and_removed_url_tombstones() -> None:
    kept = f"{ORIGIN}/en/units/55/"
    removed = f"{ORIGIN}/en/units/115/"
    first_client = FakeClient(
        {
            f"{ORIGIN}/robots.txt": [FakeResponse(ROBOTS)],
            SITEMAP: [FakeResponse(sitemap((kept, "2026-08-10"), (removed, "2026-08-06")))],
            kept: [FakeResponse(html(kept, "Marine Raiders CQC", "HP 70"))],
            removed: [FakeResponse(html(removed, "Delta Force", "HP 60"))],
        }
    )
    first_messages = await collect(source(first_client))
    next_client = FakeClient(
        {
            f"{ORIGIN}/robots.txt": [FakeResponse(ROBOTS)],
            SITEMAP: [FakeResponse(sitemap((kept, "2026-08-11")))],
            kept: [FakeResponse(html(kept, "Marine Raiders CQC", "HP 75"))],
        }
    )

    next_messages = await collect(source(next_client), final_state(first_messages))

    emitted = [item.envelope for item in next_messages if isinstance(item, RecordMessage)]
    assert [item.operation for item in emitted] == [Operation.SNAPSHOT, Operation.DELETE]
    assert isinstance(emitted[1].payload, Tombstone)
    assert emitted[1].provenance["url"] == removed
    assert set(final_state(next_messages)["records"]) == {kept}


@pytest.mark.asyncio
async def test_robots_denial_fails_closed() -> None:
    client = FakeClient(
        {
            f"{ORIGIN}/robots.txt": [
                FakeResponse("User-agent: *\nDisallow: /sitemap-units.xml\n")
            ]
        }
    )

    result = await source(client).check()

    assert result.ok is False
    assert "robots.txt" in result.message
