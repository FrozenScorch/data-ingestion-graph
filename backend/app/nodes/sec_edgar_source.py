"""
SEC EDGAR Source node: fetches SEC filings from EDGAR full-text search API.

Downloads filings by company ticker or CIK number, strips HTML, and returns
documents suitable for downstream processing nodes.
"""
import asyncio
import logging
import re
from html.parser import HTMLParser
from typing import Any
from urllib.parse import quote

import httpx

from app.nodes.base import BaseNode, NodeContext, NodeResult, PortDef, PortDataType

logger = logging.getLogger(__name__)

EDGAR_SEARCH_URL = "https://efts.sec.gov/LATEST/search-index"


class _HTMLStripper(HTMLParser):
    """Strip HTML tags and extract text content."""

    def __init__(self) -> None:
        super().__init__()
        self._parts: list[str] = []
        self._skip = False

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag in ("script", "style"):
            self._skip = True

    def handle_endtag(self, tag: str) -> None:
        if tag in ("script", "style"):
            self._skip = False
        elif tag in ("p", "div", "br", "li", "tr", "h1", "h2", "h3", "h4", "h5", "h6"):
            self._parts.append("\n")

    def handle_data(self, data: str) -> None:
        if not self._skip:
            self._parts.append(data)

    def get_text(self) -> str:
        return re.sub(r"\n{3,}", "\n\n", "".join(self._parts)).strip()


def _strip_html(html_text: str) -> str:
    stripper = _HTMLStripper()
    stripper.feed(html_text)
    return stripper.get_text()


class SECEdgarSourceNode(BaseNode):
    @property
    def node_type(self) -> str:
        return "sec_edgar_source"

    @property
    def display_name(self) -> str:
        return "SEC EDGAR Source"

    @property
    def category(self) -> str:
        return "source"

    @property
    def description(self) -> str:
        return "Fetch SEC filings from EDGAR by company ticker or CIK number"

    @property
    def inputs(self) -> list[PortDef]:
        return []

    @property
    def outputs(self) -> list[PortDef]:
        return [PortDef(name="documents", data_type=PortDataType.DOCUMENT, label="Documents")]

    @property
    def config_schema(self) -> dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "identifier": {
                    "type": "string",
                    "description": "Company ticker (e.g. AAPL) or CIK number",
                },
                "filing_type": {
                    "type": "string",
                    "enum": ["10-K", "10-Q", "8-K", "13-F", "DEF 14A", "ALL"],
                    "default": "10-K",
                    "description": "SEC filing type to search for",
                },
                "start_date": {
                    "type": "string",
                    "format": "date",
                    "description": "Start date for filing search",
                },
                "end_date": {
                    "type": "string",
                    "format": "date",
                    "description": "End date for filing search",
                },
                "user_agent_email": {
                    "type": "string",
                    "description": "Your email (required by SEC EDGAR)",
                },
                "max_filings": {
                    "type": "integer",
                    "default": 10,
                    "minimum": 1,
                    "maximum": 100,
                    "description": "Maximum number of filings to retrieve",
                },
            },
            "required": ["identifier", "filing_type", "user_agent_email"],
        }

    async def validate_config(self, config: dict[str, Any]) -> list[str]:
        """Validate that identifier and user_agent_email are non-empty."""
        errors = await super().validate_config(config)
        identifier = config.get("identifier", "")
        if not isinstance(identifier, str) or not identifier.strip():
            errors.append("identifier must be a non-empty string")
        email = config.get("user_agent_email", "")
        if not isinstance(email, str) or not email.strip():
            errors.append("user_agent_email must be a non-empty string")
        return errors

    async def execute(self, context: NodeContext) -> NodeResult:
        """Fetch and download SEC EDGAR filings."""
        identifier = context.config.get("identifier", "")
        filing_type = context.config.get("filing_type", "10-K")
        start_date = context.config.get("start_date", "2020-01-01")
        end_date = context.config.get("end_date", "2025-12-31")
        email = context.config.get("user_agent_email", "")
        max_filings = context.config.get("max_filings", 10)

        user_agent = f"IngestionGraph/1.0 (mailto:{email})"

        headers = {
            "User-Agent": user_agent,
            "Accept": "application/json",
        }

        documents: list[dict[str, Any]] = []

        try:
            async with httpx.AsyncClient(timeout=60.0) as client:
                # Build search query
                forms_param = filing_type if filing_type != "ALL" else ""
                params: dict[str, str] = {
                    "q": identifier,
                    "dateRange": "custom",
                    "startdt": start_date,
                    "enddt": end_date,
                }
                if forms_param:
                    params["forms"] = forms_param

                # Search for filings
                response = await client.get(
                    EDGAR_SEARCH_URL,
                    params=params,
                    headers=headers,
                )
                response.raise_for_status()

                search_data = response.json()

                # Debug: log raw response structure for diagnosis
                logger.info(f"EDGAR search response keys: {list(search_data.keys()) if isinstance(search_data, dict) else type(search_data)}")

                # Extract filing entries from response.
                # EDGAR full-text search returns: {"hits": {"hits": [{"_source": {...}}]}}
                hits: list[dict[str, Any]] = []
                if isinstance(search_data, dict):
                    # Try nested hits.hits
                    outer_hits = search_data.get("hits")
                    if isinstance(outer_hits, dict):
                        inner_hits = outer_hits.get("hits")
                        if isinstance(inner_hits, list):
                            hits = inner_hits
                    # Also try flat structures
                    if not hits:
                        for key in ("hits", "filings", "results", "entries"):
                            val = search_data.get(key)
                            if isinstance(val, list):
                                hits = val
                                break
                            elif isinstance(val, dict):
                                sub = val.get("hits", val.get("filings", val.get("results", [])))
                                if isinstance(sub, list):
                                    hits = sub
                                    break

                if not hits:
                    logger.warning("No filing entries found in EDGAR response: %s", str(search_data)[:500])
                    return NodeResult(
                        success=True,
                        output_data={"documents": []},
                        items_processed=0,
                        metadata={"identifier": identifier, "filing_type": filing_type, "warning": "No filings found", "raw_response_keys": list(search_data.keys()) if isinstance(search_data, dict) else str(type(search_data))},
                    )

                # Extract document URLs from filings
                filing_urls: list[tuple[str, str, str]] = []  # (url, filename, cik)
                for hit in hits[:max_filings]:
                    # EDGAR wraps results in _source: {"_source": {"file_num": "...", ...}}
                    hit_data = hit.get("_source", hit) if isinstance(hit, dict) else hit
                    if not isinstance(hit_data, dict):
                        continue

                    # Look for the primary document URL — EDGAR uses several field names
                    doc_url = (
                        hit_data.get("linkToFilingDetails")
                        or hit_data.get("filingDetailUrl")
                        or hit_data.get("url")
                        or hit_data.get("filing_href")
                        or ""
                    )
                    # Build full URL if relative
                    if doc_url and not doc_url.startswith("http"):
                        doc_url = f"https://www.sec.gov/Archives/edgar/data/{doc_url}"

                    filename = (
                        hit_data.get("fileNum")
                        or hit_data.get("file_num")
                        or hit_data.get("filename")
                        or ""
                    )
                    cik = str(hit_data.get("cik", hit_data.get("entityId", "")))

                    if doc_url:
                        filing_urls.append((doc_url, filename, cik))

                # Download each filing document
                for idx, (url, filename, cik) in enumerate(filing_urls[:max_filings]):
                    try:
                        doc_response = await client.get(url, headers=headers)
                        doc_response.raise_for_status()

                        raw_html = doc_response.text
                        cleaned_text = _strip_html(raw_html)

                        if cleaned_text:
                            documents.append({
                                "text": cleaned_text,
                                "metadata": {
                                    "source": url,
                                    "name": filename,
                                    "filing_type": filing_type,
                                    "cik": str(cik),
                                    "identifier": identifier,
                                },
                            })

                        # SEC rate limiting: sleep between requests
                        if idx < len(filing_urls[:max_filings]) - 1:
                            await asyncio.sleep(0.15)

                    except httpx.HTTPError as e:
                        logger.error(f"Failed to download filing {url}: {e}")
                        continue

        except httpx.HTTPError as e:
            logger.error(f"SEC EDGAR request failed: {e}")
            return NodeResult(
                success=False,
                output_data={"documents": documents},
                items_processed=len(documents),
                error_message=f"SEC EDGAR request failed: {str(e)}",
            )
        except Exception as e:
            logger.exception(f"SECEdgarSourceNode error: {e}")
            return NodeResult(
                success=False,
                output_data={"documents": documents},
                items_processed=len(documents),
                error_message=str(e),
            )

        return NodeResult(
            success=True,
            output_data={"documents": documents},
            items_processed=len(documents),
            metadata={
                "identifier": identifier,
                "filing_type": filing_type,
                "start_date": start_date,
                "end_date": end_date,
                "total_documents": len(documents),
            },
        )


def register():
    from app.nodes.registry import register_node
    register_node(SECEdgarSourceNode())
