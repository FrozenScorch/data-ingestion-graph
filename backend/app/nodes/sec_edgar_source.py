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

# 2024 Fortune 500 top companies (ticker symbols)
FORTUNE_500_TICKERS: list[str] = [
    "WMT", "AMZN", "AAPL", "UNH", "CVX",
    "EXC", "BHGE", "MET", "F", "KO",
    "AIG", "LIN", "PFE", "JPM", "CMCSA",
    "BA", "CVS", "VZ", "T", "HD",
    "XOM", "BAC", "WFC", "C", "GOOGL",
    "BRK.B", "MS", "RTX", "ABT", "COST",
    "MCD", "NKE", "MRK", "DIS", "PG",
    "PM", "TMO", "UPS", "INTC", "CSCO",
    "ADBE", "PEP", "AVGO", "TXN", "QCOM",
]


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
                    "description": "Company ticker (e.g. AAPL, MSFT, GOOG), CIK number, or 'fortune500' for Fortune 50 preset",
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

        # Parse tickers from identifier (comma-separated)
        tickers = [t.strip().upper() for t in identifier.split(",") if t.strip()]

        # Check for fortune500 preset
        if tickers and tickers[0].lower() == "fortune500":
            tickers = FORTUNE_500_TICKERS

        if not tickers:
            return NodeResult(
                success=False,
                output_data={"documents": []},
                items_processed=0,
                error_message="No valid ticker(s) provided in identifier",
            )

        logger.info(
            "Processing SEC EDGAR filings for %d ticker(s): %s",
            len(tickers),
            ", ".join(tickers[:10]) + ("..." if len(tickers) > 10 else ""),
        )

        user_agent = f"IngestionGraph/1.0 (mailto:{email})"

        headers = {
            "User-Agent": user_agent,
            "Accept": "application/json",
        }

        documents: list[dict[str, Any]] = []
        # Track total filings across all tickers to respect max_filings cap
        total_downloaded = 0

        try:
            async with httpx.AsyncClient(timeout=60.0) as client:
                for ticker in tickers:
                    if total_downloaded >= max_filings:
                        logger.info("Reached max_filings limit (%d), stopping ticker loop", max_filings)
                        break

                    remaining = max_filings - total_downloaded
                    logger.info("Searching EDGAR for ticker=%s (remaining quota: %d)", ticker, remaining)

                    # Build search query
                    forms_param = filing_type if filing_type != "ALL" else ""
                    params: dict[str, str] = {
                        "q": ticker,
                        "dateRange": "custom",
                        "startdt": start_date,
                        "enddt": end_date,
                    }
                    if forms_param:
                        params["forms"] = forms_param

                    # Search for filings
                    try:
                        response = await client.get(
                            EDGAR_SEARCH_URL,
                            params=params,
                            headers=headers,
                        )
                        response.raise_for_status()
                    except httpx.HTTPError as e:
                        logger.error("SEC EDGAR search failed for ticker %s: %s", ticker, e)
                        continue

                    search_data = response.json()

                    # Debug: log raw response structure for diagnosis
                    logger.info(
                        "EDGAR search response keys for %s: %s",
                        ticker,
                        list(search_data.keys()) if isinstance(search_data, dict) else type(search_data),
                    )

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
                        logger.warning("No filing entries found for ticker %s", ticker)
                        continue

                    # Build .txt filing URLs from search hits
                    filing_urls: list[tuple[str, str, str, str]] = []  # (doc_url, filename, cik, adsh)
                    for hit in hits[:remaining]:
                        # EDGAR wraps results in _source
                        hit_data = hit.get("_source", hit) if isinstance(hit, dict) else hit
                        if not isinstance(hit_data, dict):
                            continue

                        # Build .txt full submission URL from accession number (adsh) and cik
                        adsh = hit_data.get("adsh", "")
                        ciks = hit_data.get("ciks", [])
                        cik = ciks[0] if isinstance(ciks, list) and ciks else str(ciks)

                        if adsh and cik:
                            clean_cik = str(cik).lstrip("0")
                            clean_adsh = adsh.replace("-", "")
                            index_url = f"https://www.sec.gov/Archives/edgar/data/{clean_cik}/{clean_adsh}/{adsh}-index.htm"
                        else:
                            continue

                        form = hit_data.get("form", filing_type)
                        file_desc = hit_data.get("file_description", "")
                        display_names = hit_data.get("display_names", [])
                        name = display_names[0] if isinstance(display_names, list) and display_names else str(display_names)
                        filename = f"{form} - {name} - {file_desc}"

                        filing_urls.append((index_url, filename, cik, adsh, form))

                    # Download each filing's primary document via the index page
                    for index_url, filename, cik, adsh, form in filing_urls:
                        if total_downloaded >= max_filings:
                            break

                        try:
                            # Step 1: Download the index page and find the primary document link
                            index_response = await client.get(index_url, headers=headers)
                            index_response.raise_for_status()

                            primary_doc_url: str | None = None

                            # Parse the index page table: find first row whose Type column
                            # matches the filing type. The table has columns: Seq, Type, Document, Description, Size.
                            rows = re.findall(r"<tr[^>]*>(.*?)</tr>", index_response.text, re.DOTALL | re.IGNORECASE)
                            for row in rows:
                                cells = re.findall(r"<td[^>]*>(.*?)</td>", row, re.DOTALL | re.IGNORECASE)
                                # cells[0]=Seq, cells[1]=Type, cells[2]=Document link, cells[3]=Description, cells[4]=Size
                                if len(cells) >= 3:
                                    type_text = re.sub(r"<[^>]+>", "", cells[1]).strip().upper()
                                    if type_text == form.upper():
                                        # Found matching row — extract the href from the Document column (cells[2])
                                        link_match = re.search(r'href="([^"]+)"', cells[2])
                                        if link_match:
                                            href = link_match.group(1)
                                            # Resolve /ix?doc= or absolute /Archives/ links
                                            if href.startswith("/"):
                                                primary_doc_url = f"https://www.sec.gov{href}"
                                            else:
                                                primary_doc_url = f"https://www.sec.gov/Archives/edgar/data/{cik.lstrip('0')}/{href}"
                                            break

                            # Fallback: if no table match, try the first .htm link that isn't an exhibit
                            if not primary_doc_url:
                                all_links = re.findall(r'href="(/[^"]+\.htm)"', index_response.text, re.IGNORECASE)
                                for link in all_links:
                                    if "exhibit" not in link.lower() and "-index.htm" not in link.lower():
                                        primary_doc_url = f"https://www.sec.gov{link}"
                                        break

                            if not primary_doc_url:
                                logger.warning("Could not find primary document link in %s", index_url)
                                continue

                            # Step 2: Download the actual primary document
                            doc_response = await client.get(primary_doc_url, headers=headers)
                            doc_response.raise_for_status()

                            raw_html = doc_response.text
                            cleaned_text = _strip_html(raw_html)

                            if cleaned_text:
                                documents.append({
                                    "text": cleaned_text,
                                    "metadata": {
                                        "source": primary_doc_url,
                                        "name": filename,
                                        "filing_type": filing_type,
                                        "cik": str(cik),
                                        "adsh": adsh,
                                        "ticker": ticker,
                                        "identifier": identifier,
                                    },
                                })
                                total_downloaded += 1

                            # SEC rate limiting: sleep between requests
                            if total_downloaded < max_filings:
                                await asyncio.sleep(0.15)

                        except httpx.HTTPError as e:
                            logger.error("Failed to download filing from %s: %s", index_url, e)
                            continue

                    # Rate limit between ticker searches
                    if total_downloaded < max_filings:
                        await asyncio.sleep(0.15)

        except httpx.HTTPError as e:
            logger.error("SEC EDGAR request failed: %s", e)
            return NodeResult(
                success=False,
                output_data={"documents": documents},
                items_processed=len(documents),
                error_message=f"SEC EDGAR request failed: {str(e)}",
            )
        except Exception as e:
            logger.exception("SECEdgarSourceNode error: %s", e)
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
                "tickers_processed": len(tickers),
                "total_documents": len(documents),
            },
        )


def register():
    from app.nodes.registry import register_node
    register_node(SECEdgarSourceNode())
