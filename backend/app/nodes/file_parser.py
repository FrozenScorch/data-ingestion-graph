"""
FileParser node: parses files (PDF, DOCX, CSV, TXT) into documents.

Supports auto-detection from file extension or explicit parser selection.
"""
import csv
import io
import logging
from pathlib import Path
from typing import Any

from app.nodes.base import BaseNode, NodeContext, NodeResult, PortDef, PortDataType

logger = logging.getLogger(__name__)

# Map file extensions to parser types
EXTENSION_MAP: dict[str, str] = {
    ".pdf": "pdf",
    ".docx": "docx",
    ".doc": "docx",
    ".csv": "csv",
    ".txt": "txt",
    ".md": "txt",
    ".json": "txt",
    ".xml": "txt",
    ".html": "txt",
    ".htm": "txt",
}


class FileParserNode(BaseNode):
    @property
    def node_type(self) -> str:
        return "file_parser"

    @property
    def display_name(self) -> str:
        return "File Parser"

    @property
    def category(self) -> str:
        return "processing"

    @property
    def description(self) -> str:
        return "Parse files into text documents (PDF, DOCX, CSV, TXT)"

    @property
    def inputs(self) -> list[PortDef]:
        return [PortDef(name="file_list", data_type=PortDataType.FILE_LIST, required=True)]

    @property
    def outputs(self) -> list[PortDef]:
        return [PortDef(name="documents", data_type=PortDataType.DOCUMENT, label="Documents")]

    @property
    def config_schema(self) -> dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "parser": {
                    "type": "string",
                    "enum": ["auto", "pdf", "docx", "csv", "txt"],
                    "default": "auto",
                },
                "ocr_enabled": {"type": "boolean", "default": False},
            },
        }

    async def execute(self, context: NodeContext) -> NodeResult:
        """Parse file_list into documents."""
        file_list = context.input_data.get("file_list", [])
        if not file_list:
            return NodeResult(
                success=True,
                output_data={"documents": []},
                items_processed=0,
            )

        parser_mode = context.config.get("parser", "auto")
        documents: list[dict[str, Any]] = []
        errors: list[str] = []

        for file_info in file_list:
            try:
                doc = await self._parse_file(file_info, parser_mode, context)
                documents.append(doc)
            except Exception as e:
                logger.warning(f"Failed to parse {file_info.get('path', 'unknown')}: {e}")
                errors.append(f"{file_info.get('path', 'unknown')}: {str(e)}")

        success = len(errors) == 0
        return NodeResult(
            success=success,
            output_data={"documents": documents},
            items_processed=len(documents),
            metadata={
                "total_files": len(file_list),
                "parsed": len(documents),
                "errors": len(errors),
                "error_messages": errors,
            },
            error_message="; ".join(errors) if errors else None,
        )

    async def _parse_file(
        self,
        file_info: dict[str, Any],
        parser_mode: str,
        context: NodeContext,
    ) -> dict[str, Any]:
        """Parse a single file into a document dict."""
        file_path = Path(file_info.get("path", ""))
        extension = file_info.get("extension", file_path.suffix.lower())

        # Determine parser
        if parser_mode == "auto":
            parser = EXTENSION_MAP.get(extension, "txt")
        else:
            parser = parser_mode

        if parser == "pdf":
            text, page_count = self._parse_pdf(file_path)
        elif parser == "docx":
            text, page_count = self._parse_docx(file_path)
        elif parser == "csv":
            text, page_count = self._parse_csv(file_path)
        else:
            # Default: plain text
            text, page_count = self._parse_txt(file_path)

        return {
            "text": text,
            "metadata": {
                "source": str(file_path),
                "name": file_info.get("name", file_path.name),
                "size": file_info.get("size", 0),
                "content_type": file_info.get("content_type", ""),
                "parser": parser,
                "extension": extension,
            },
            "page_count": page_count,
        }

    def _parse_pdf(self, file_path: Path) -> tuple[str, int]:
        """Parse a PDF file using pypdf."""
        try:
            from pypdf import PdfReader
        except ImportError:
            raise ImportError("pypdf is required for PDF parsing. Install with: pip install pypdf")

        reader = PdfReader(str(file_path))
        pages = []
        for page in reader.pages:
            text = page.extract_text()
            if text:
                pages.append(text)

        return "\n\n".join(pages), len(reader.pages)

    def _parse_docx(self, file_path: Path) -> tuple[str, int]:
        """Parse a DOCX file using python-docx."""
        try:
            from docx import Document
        except ImportError:
            raise ImportError(
                "python-docx is required for DOCX parsing. Install with: pip install python-docx"
            )

        doc = Document(str(file_path))
        paragraphs = [para.text for para in doc.paragraphs if para.text.strip()]
        text = "\n\n".join(paragraphs)

        # Estimate page count (rough: ~3000 chars per page)
        page_count = max(1, len(text) // 3000) if text else 1

        return text, page_count

    def _parse_csv(self, file_path: Path) -> tuple[str, int]:
        """Parse a CSV file using the csv module."""
        with open(file_path, "r", encoding="utf-8", errors="replace") as f:
            # Detect dialect
            sample = f.read(8192)
            f.seek(0)

            try:
                dialect = csv.Sniffer().sniff(sample)
            except csv.Error:
                dialect = csv.excel

            reader = csv.reader(f, dialect)
            rows = list(reader)

        if not rows:
            return "", 1

        # Format as text: each row on a line, columns separated by " | "
        lines = [" | ".join(row) for row in rows]
        text = "\n".join(lines)

        # Page count: rows / ~50 rows per page
        page_count = max(1, len(rows) // 50)

        return text, page_count

    def _parse_txt(self, file_path: Path) -> tuple[str, int]:
        """Parse a plain text file."""
        # Try UTF-8 first, fall back to latin-1
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                text = f.read()
        except UnicodeDecodeError:
            with open(file_path, "r", encoding="latin-1") as f:
                text = f.read()

        # Estimate page count
        page_count = max(1, len(text) // 3000) if text else 1

        return text, page_count


def register():
    from app.nodes.registry import register_node
    register_node(FileParserNode())
