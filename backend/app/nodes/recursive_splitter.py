"""
Recursive Character Splitter node: splits documents using a hierarchy of
separator strings, applied from largest to smallest.

Implements the classic recursive text splitter pattern:
1. Try splitting with the first (largest) separator.
2. If a piece still exceeds chunk_size, recurse with remaining separators.
3. If no separators remain, fall back to character-level splitting.
4. Optionally add overlap between consecutive chunks.
"""
import logging
from typing import Any

from app.nodes.base import BaseNode, NodeContext, NodeResult, PortDef, PortDataType

logger = logging.getLogger(__name__)


class RecursiveSplitterNode(BaseNode):
    @property
    def node_type(self) -> str:
        return "recursive_splitter"

    @property
    def display_name(self) -> str:
        return "Recursive Character Splitter"

    @property
    def category(self) -> str:
        return "processing"

    @property
    def description(self) -> str:
        return "Split documents using a hierarchy of separator strings with overlap"

    @property
    def inputs(self) -> list[PortDef]:
        return [PortDef(name="documents", data_type=PortDataType.DOCUMENT, required=True)]

    @property
    def outputs(self) -> list[PortDef]:
        return [PortDef(name="chunks", data_type=PortDataType.CHUNKS, label="Chunks")]

    @property
    def config_schema(self) -> dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "separators": {
                    "type": "string",
                    'default': "\\n\\n,\\n,. , , ",
                    "description": "Comma-separated separator strings, tried largest first",
                },
                "chunk_size": {
                    "type": "integer",
                    "default": 1000,
                    "minimum": 10,
                    "description": "Target chunk size in characters",
                },
                "chunk_overlap": {
                    "type": "integer",
                    "default": 200,
                    "minimum": 0,
                    "description": "Number of overlapping characters between chunks",
                },
            },
        }

    async def execute(self, context: NodeContext) -> NodeResult:
        """Recursively split documents into chunks."""
        documents = context.input_data.get("documents", [])
        if not documents:
            return NodeResult(
                success=True,
                output_data={"chunks": []},
                items_processed=0,
            )

        separators_raw = context.config.get("separators", "\\n\\n,\\n,. , , ")
        chunk_size = context.config.get("chunk_size", 1000)
        chunk_overlap = context.config.get("chunk_overlap", 200)

        if chunk_overlap >= chunk_size:
            return NodeResult(
                success=False,
                output_data={"chunks": []},
                items_processed=0,
                error_message=f"chunk_overlap ({chunk_overlap}) must be less than chunk_size ({chunk_size})",
            )

        # Parse separators: split by comma and unescape
        separators = self._parse_separators(separators_raw)

        all_chunks: list[dict[str, Any]] = []
        global_chunk_index = 0

        for doc_idx, doc in enumerate(documents):
            text = doc.get("text", "")
            if not text.strip():
                continue

            doc_metadata = doc.get("metadata", {})
            source = doc_metadata.get("source", doc_metadata.get("name", ""))

            # Recursive split
            raw_chunks = self._recursive_split(text, separators, chunk_size)

            # Add overlap between consecutive chunks
            chunks_with_overlap = self._add_overlap(raw_chunks, chunk_overlap)

            for chunk_text in chunks_with_overlap:
                if not chunk_text.strip():
                    continue
                all_chunks.append({
                    "text": chunk_text,
                    "metadata": {
                        **doc_metadata,
                        "source": source,
                        "doc_index": doc_idx,
                        "chunk_index": global_chunk_index,
                    },
                })
                global_chunk_index += 1

        return NodeResult(
            success=True,
            output_data={"chunks": all_chunks},
            items_processed=len(all_chunks),
            metadata={
                "total_documents": len(documents),
                "total_chunks": len(all_chunks),
                "chunk_size": chunk_size,
                "chunk_overlap": chunk_overlap,
                "separators": separators,
            },
        )

    @staticmethod
    def _parse_separators(raw: str) -> list[str]:
        """
        Parse a comma-separated separators string with backslash escaping.

        Handles sequences like ``\\n\\n`` (two newlines), ``\\n`` (newline),
        ``. `` (period-space), etc.
        """
        # Split on comma
        parts = raw.split(",")
        separators: list[str] = []
        for part in parts:
            # Unescape common escape sequences
            unescaped = (
                part
                .replace("\\n", "\n")
                .replace("\\t", "\t")
                .replace("\\r", "\r")
                .replace("\\\\", "\\")
            )
            separators.append(unescaped)
        return separators

    def _recursive_split(
        self,
        text: str,
        separators: list[str],
        chunk_size: int,
    ) -> list[str]:
        """
        Recursively split text using the separator hierarchy.

        Tries each separator in order (largest first). If a piece still
        exceeds chunk_size, recurses with the remaining separators.
        """
        if not text:
            return []

        if len(text) <= chunk_size:
            return [text]

        if not separators:
            # No separators left; fall back to character-level splitting
            return self._split_by_char(text, chunk_size)

        separator = separators[0]
        remaining_separators = separators[1:]

        # Split on the current separator
        pieces = text.split(separator)

        chunks: list[str] = []
        current_chunk = ""

        for piece in pieces:
            # If the piece itself is too large, recurse
            if len(piece) > chunk_size:
                # Flush any accumulated text first
                if current_chunk:
                    chunks.append(current_chunk)
                    current_chunk = ""

                sub_chunks = self._recursive_split(piece, remaining_separators, chunk_size)
                chunks.extend(sub_chunks)
            elif len(current_chunk) + len(separator) + len(piece) <= chunk_size if current_chunk else len(piece) <= chunk_size:
                # Piece fits in the current chunk
                if current_chunk:
                    current_chunk += separator + piece
                else:
                    current_chunk = piece
            else:
                # Piece doesn't fit; flush current chunk and start a new one
                if current_chunk:
                    chunks.append(current_chunk)
                current_chunk = piece

        if current_chunk:
            chunks.append(current_chunk)

        return chunks

    @staticmethod
    def _split_by_char(text: str, chunk_size: int) -> list[str]:
        """Fall back to character-level splitting."""
        chunks: list[str] = []
        for i in range(0, len(text), chunk_size):
            chunks.append(text[i : i + chunk_size])
        return chunks

    @staticmethod
    def _add_overlap(chunks: list[str], overlap: int) -> list[str]:
        """
        Add overlap between consecutive chunks.

        Each chunk appends the first ``overlap`` characters of the next chunk.
        """
        if overlap <= 0 or len(chunks) <= 1:
            return list(chunks)

        result: list[str] = []
        for i, chunk in enumerate(chunks):
            if i < len(chunks) - 1:
                overlap_text = chunks[i + 1][:overlap]
                result.append(chunk + overlap_text)
            else:
                result.append(chunk)

        return result


def register():
    from app.nodes.registry import register_node
    register_node(RecursiveSplitterNode())
