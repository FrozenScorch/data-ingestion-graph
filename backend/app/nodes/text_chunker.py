"""
TextChunker node: splits documents into chunks using tiktoken or simple tokenizers.

Supports multiple tokenizer modes:
- "tiktoken_cl100k": OpenAI cl100k_base tokenizer via tiktoken (default)
- "words": simple word-based splitting
- "chars": character-based splitting
"""
import logging
from typing import Any

from app.nodes.base import BaseNode, NodeContext, NodeResult, PortDef, PortDataType

logger = logging.getLogger(__name__)


class TextChunkerNode(BaseNode):
    @property
    def node_type(self) -> str:
        return "text_chunker"

    @property
    def display_name(self) -> str:
        return "Text Chunker"

    @property
    def category(self) -> str:
        return "processing"

    @property
    def description(self) -> str:
        return "Split documents into chunks using configurable tokenizer"

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
                "chunk_size": {"type": "integer", "default": 512, "minimum": 10, "description": "Target chunk size in characters"},
                "chunk_overlap": {"type": "integer", "default": 50, "minimum": 0, "description": "Number of overlapping characters between chunks"},
                "tokenizer": {
                    "type": "string",
                    "enum": ["tiktoken_cl100k", "words", "chars"],
                    "default": "tiktoken_cl100k",
                    "description": "Tokenization method: tiktoken (OpenAI), word-based, or character-based",
                },
            },
        }

    async def execute(self, context: NodeContext) -> NodeResult:
        """Chunk documents into smaller pieces."""
        documents = context.input_data.get("documents", [])
        if not documents:
            return NodeResult(
                success=True,
                output_data={"chunks": []},
                items_processed=0,
            )

        chunk_size = context.config.get("chunk_size", 512)
        chunk_overlap = context.config.get("chunk_overlap", 50)
        tokenizer_mode = context.config.get("tokenizer", "tiktoken_cl100k")

        if chunk_overlap >= chunk_size:
            return NodeResult(
                success=False,
                output_data={"chunks": []},
                items_processed=0,
                error_message=f"chunk_overlap ({chunk_overlap}) must be less than chunk_size ({chunk_size})",
            )

        # Get the tokenizer function
        tokenize_fn = self._get_tokenizer(tokenizer_mode)

        all_chunks: list[dict[str, Any]] = []
        global_chunk_index = 0

        for doc_idx, doc in enumerate(documents):
            text = doc.get("text", "")
            if not text.strip():
                continue

            metadata = doc.get("metadata", {})
            page_count = doc.get("page_count", 0)

            # Tokenize the text
            tokens = tokenize_fn(text)

            # Generate chunks with overlap
            doc_chunks = self._create_chunks(
                tokens=tokens,
                chunk_size=chunk_size,
                chunk_overlap=chunk_overlap,
                doc_index=doc_idx,
                global_start=global_chunk_index,
                metadata=metadata,
                page_count=page_count,
                tokenizer_mode=tokenizer_mode,
            )

            all_chunks.extend(doc_chunks)
            global_chunk_index += len(doc_chunks)

        return NodeResult(
            success=True,
            output_data={"chunks": all_chunks},
            items_processed=len(all_chunks),
            metadata={
                "total_documents": len(documents),
                "total_chunks": len(all_chunks),
                "chunk_size": chunk_size,
                "chunk_overlap": chunk_overlap,
                "tokenizer": tokenizer_mode,
            },
        )

    def _get_tokenizer(self, mode: str):
        """Return a tokenizer function for the given mode."""
        if mode == "tiktoken_cl100k":
            try:
                import tiktoken
                enc = tiktoken.get_encoding("cl100k_base")
                return enc.encode
            except ImportError:
                logger.warning("tiktoken not available, falling back to word tokenizer")
                return self._tokenize_words
        elif mode == "words":
            return self._tokenize_words
        elif mode == "chars":
            return self._tokenize_chars
        else:
            raise ValueError(f"Unknown tokenizer mode: {mode}")

    @staticmethod
    def _tokenize_words(text: str) -> list[str]:
        """Simple word-level tokenizer: splits on whitespace."""
        return text.split()

    @staticmethod
    def _tokenize_chars(text: str) -> list[str]:
        """Character-level tokenizer: each character is a token."""
        return list(text)

    def _create_chunks(
        self,
        tokens: list[str],
        chunk_size: int,
        chunk_overlap: int,
        doc_index: int,
        global_start: int,
        metadata: dict[str, Any],
        page_count: int,
        tokenizer_mode: str,
    ) -> list[dict[str, Any]]:
        """
        Create chunks from a token list with overlap.

        Each chunk overlaps with the previous one by ``chunk_overlap`` tokens.
        """
        chunks: list[dict[str, Any]] = []
        step = chunk_size - chunk_overlap

        if step <= 0:
            step = 1

        i = 0
        while i < len(tokens):
            chunk_tokens = tokens[i : i + chunk_size]
            if not chunk_tokens:
                break

            # Reconstruct text from tokens
            if tokenizer_mode == "chars":
                chunk_text = "".join(chunk_tokens)
            else:
                # For word and tiktoken tokens, join with space
                chunk_text = " ".join(chunk_tokens) if tokenizer_mode == "words" else self._decode_tokens(chunk_tokens)

            chunks.append({
                "text": chunk_text,
                "chunk_index": global_start + len(chunks),
                "metadata": {
                    **metadata,
                    "doc_index": doc_index,
                    "chunk_tokens": len(chunk_tokens),
                    "page_count": page_count,
                },
            })

            i += step

        return chunks

    @staticmethod
    def _decode_tokens(tokens: list[str]) -> str:
        """Decode tiktoken tokens back to text. Falls back to join if not tiktoken tokens."""
        try:
            import tiktoken
            enc = tiktoken.get_encoding("cl100k_base")
            # If tokens are integers (tiktoken output), decode them
            if tokens and isinstance(tokens[0], int):
                return enc.decode(tokens)
            # If tokens are strings (already encoded somehow), just join
            return " ".join(tokens)
        except Exception:
            return " ".join(tokens)


def register():
    from app.nodes.registry import register_node
    register_node(TextChunkerNode())
