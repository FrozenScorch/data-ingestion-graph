"""
Semantic Chunker node: splits documents based on semantic similarity
between consecutive sentences.

Uses embedding cosine similarity to detect topic boundaries, then splits
chunks at those boundaries while respecting min/max chunk size constraints.
"""
import logging
import math
import re
from typing import Any

from app.nodes.base import BaseNode, NodeContext, NodeResult, PortDef, PortDataType
from app.services.openrouter_service import openrouter_service

logger = logging.getLogger(__name__)


def _cosine_similarity(a: list[float], b: list[float]) -> float:
    """Compute cosine similarity between two vectors."""
    if not a or not b or len(a) != len(b):
        return 0.0
    dot = sum(x * y for x, y in zip(a, b))
    norm_a = math.sqrt(sum(x * x for x in a))
    norm_b = math.sqrt(sum(x * x for x in b))
    if norm_a == 0 or norm_b == 0:
        return 0.0
    return dot / (norm_a * norm_b)


def _split_sentences(text: str) -> list[str]:
    """
    Split text into sentences using regex.

    Splits on [.!?] followed by a space or end-of-string.
    """
    # Split on sentence-ending punctuation followed by whitespace
    sentences = re.split(r"(?<=[.!?])\s+", text.strip())
    return [s.strip() for s in sentences if s.strip()]


class SemanticChunkerNode(BaseNode):
    @property
    def node_type(self) -> str:
        return "semantic_chunker"

    @property
    def display_name(self) -> str:
        return "Semantic Chunker"

    @property
    def category(self) -> str:
        return "processing"

    @property
    def description(self) -> str:
        return "Split documents into semantically coherent chunks using embedding similarity"

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
                "embedding_model": {
                    "type": "string",
                    "default": "openai/text-embedding-3-small",
                    "description": "Embedding model",
                },
                "similarity_threshold": {
                    "type": "number",
                    "default": 0.7,
                    "minimum": 0.0,
                    "maximum": 1.0,
                },
                "min_chunk_size": {
                    "type": "integer",
                    "default": 100,
                    "minimum": 10,
                },
                "max_chunk_size": {
                    "type": "integer",
                    "default": 2000,
                    "minimum": 100,
                },
                "batch_size": {
                    "type": "integer",
                    "default": 32,
                    "minimum": 1,
                },
            },
            "required": ["embedding_model"],
        }

    async def execute(self, context: NodeContext) -> NodeResult:
        """Split documents into semantically coherent chunks."""
        documents = context.input_data.get("documents", [])
        if not documents:
            return NodeResult(
                success=True,
                output_data={"chunks": []},
                items_processed=0,
            )

        embedding_model = context.config.get("embedding_model", "openai/text-embedding-3-small")
        similarity_threshold = context.config.get("similarity_threshold", 0.7)
        min_chunk_size = context.config.get("min_chunk_size", 100)
        max_chunk_size = context.config.get("max_chunk_size", 2000)
        batch_size = context.config.get("batch_size", 32)

        all_chunks: list[dict[str, Any]] = []
        global_chunk_index = 0
        total_tokens_used = 0

        for doc_idx, doc in enumerate(documents):
            text = doc.get("text", "")
            if not text.strip():
                continue

            doc_metadata = doc.get("metadata", {})
            source = doc_metadata.get("source", doc_metadata.get("name", ""))

            # Split text into sentences
            sentences = _split_sentences(text)
            if not sentences:
                continue

            # Group sentences into candidate chunks (accumulate until max_chunk_size)
            candidate_groups = self._group_sentences(sentences, max_chunk_size)

            # Flatten all sentences from all groups for embedding
            all_sentences: list[str] = []
            group_boundaries: list[tuple[int, int]] = []  # (start_idx, end_idx) in all_sentences
            idx = 0
            for group in candidate_groups:
                group_sentences = group["sentences"]
                group_boundaries.append((idx, idx + len(group_sentences)))
                all_sentences.extend(group_sentences)
                idx += len(group_sentences)

            # Get embeddings for all sentences in batches
            all_embeddings: list[list[float]] = []
            fallback_emitted = False  # Track whether we already emitted fallback chunks for this doc
            for i in range(0, len(all_sentences), batch_size):
                batch = all_sentences[i : i + batch_size]
                try:
                    response = await openrouter_service.create_embeddings(embedding_model, batch)
                    embeddings_data = response.get("data", [])
                    usage = response.get("usage", {})
                    total_tokens_used += usage.get("total_tokens", 0)

                    for emb_entry in embeddings_data:
                        all_embeddings.append(emb_entry.get("embedding", []))

                except Exception as e:
                    logger.error(f"Embedding failed for doc {doc_idx} at offset {i}: {e}")
                    # Fall back to naive sentence-based chunks (only once per document)
                    if not fallback_emitted:
                        fallback_emitted = True
                        for group in candidate_groups:
                            group_text = " ".join(group["sentences"])
                            if group_text.strip():
                                all_chunks.append({
                                    "text": group_text,
                                    "metadata": {
                                        **doc_metadata,
                                        "source": source,
                                        "doc_index": doc_idx,
                                        "chunk_index": global_chunk_index,
                                        "similarity_score": None,
                                    },
                                })
                                global_chunk_index += 1
                    continue

            # If embeddings are incomplete or fallback was already emitted, skip semantic splitting
            if fallback_emitted or not all_embeddings or len(all_embeddings) != len(all_sentences):
                if not fallback_emitted:
                    # Only emit fallback chunks if we haven't already
                    for group in candidate_groups:
                        group_text = " ".join(group["sentences"])
                        if group_text.strip():
                            all_chunks.append({
                                "text": group_text,
                                "metadata": {
                                    **doc_metadata,
                                    "source": source,
                                    "doc_index": doc_idx,
                                    "chunk_index": global_chunk_index,
                                    "similarity_score": None,
                                },
                            })
                            global_chunk_index += 1
                continue

            # Compute cosine similarity between adjacent sentences
            similarities: list[float] = []
            for i in range(len(all_embeddings) - 1):
                sim = _cosine_similarity(all_embeddings[i], all_embeddings[i + 1])
                similarities.append(sim)

            # Find topic boundaries where similarity < threshold
            boundary_indices: set[int] = set()
            for i, sim in enumerate(similarities):
                if sim < similarity_threshold:
                    boundary_indices.add(i + 1)  # boundary is between sentence i and i+1

            # Split sentences at boundaries, respecting min/max sizes
            doc_chunks = self._split_at_boundaries(
                sentences=all_sentences,
                boundary_indices=boundary_indices,
                similarities=similarities,
                min_chunk_size=min_chunk_size,
                max_chunk_size=max_chunk_size,
                source=source,
                doc_metadata=doc_metadata,
                doc_index=doc_idx,
                global_start=global_chunk_index,
            )

            all_chunks.extend(doc_chunks)
            global_chunk_index += len(doc_chunks)

        # Record cost
        cost_info = openrouter_service.calculate_cost(embedding_model, total_tokens_used, 0)

        return NodeResult(
            success=True,
            output_data={"chunks": all_chunks},
            items_processed=len(all_chunks),
            metadata={
                "total_documents": len(documents),
                "total_chunks": len(all_chunks),
                "embedding_model": embedding_model,
                "similarity_threshold": similarity_threshold,
                "min_chunk_size": min_chunk_size,
                "max_chunk_size": max_chunk_size,
                "total_tokens_used": total_tokens_used,
                "cost_usd": cost_info["total_cost_usd"],
            },
        )

    @staticmethod
    def _group_sentences(
        sentences: list[str],
        max_chunk_size: int,
    ) -> list[dict[str, Any]]:
        """
        Group sentences into candidate chunks that do not exceed max_chunk_size.

        Each group accumulates sentences until adding the next one would
        exceed max_chunk_size.
        """
        groups: list[dict[str, Any]] = []
        current_sentences: list[str] = []
        current_length = 0

        for sentence in sentences:
            sentence_len = len(sentence)
            # Account for the space between sentences
            added_length = sentence_len + (1 if current_sentences else 0)

            if current_length + added_length > max_chunk_size and current_sentences:
                groups.append({
                    "sentences": current_sentences,
                    "length": current_length,
                })
                current_sentences = [sentence]
                current_length = sentence_len
            else:
                current_sentences.append(sentence)
                current_length += added_length

        if current_sentences:
            groups.append({
                "sentences": current_sentences,
                "length": current_length,
            })

        return groups

    @staticmethod
    def _split_at_boundaries(
        sentences: list[str],
        boundary_indices: set[int],
        similarities: list[float],
        min_chunk_size: int,
        max_chunk_size: int,
        source: str,
        doc_metadata: dict[str, Any],
        doc_index: int,
        global_start: int,
    ) -> list[dict[str, Any]]:
        """
        Split the sentence list at detected topic boundaries.

        Enforces min/max chunk size constraints and includes the
        average similarity score for each chunk in its metadata.
        """
        chunks: list[dict[str, Any]] = []
        current_sentences: list[str] = []
        current_similarities: list[float] = []
        current_length = 0

        for i, sentence in enumerate(sentences):
            is_boundary = i in boundary_indices

            # Check if we should split here
            should_split = False
            if is_boundary and current_length >= min_chunk_size:
                should_split = True
            if current_length + len(sentence) + (1 if current_sentences else 0) > max_chunk_size:
                should_split = True

            if should_split and current_sentences:
                # Finalise current chunk
                chunk_text = " ".join(current_sentences)
                avg_similarity = (
                    sum(current_similarities) / len(current_similarities)
                    if current_similarities
                    else 1.0
                )
                chunks.append({
                    "text": chunk_text,
                    "metadata": {
                        **doc_metadata,
                        "source": source,
                        "doc_index": doc_index,
                        "chunk_index": global_start + len(chunks),
                        "similarity_score": round(avg_similarity, 4),
                    },
                })
                current_sentences = []
                current_similarities = []
                current_length = 0

            # Add sentence to current chunk
            added_len = len(sentence) + (1 if current_sentences else 0)
            current_sentences.append(sentence)
            current_length += added_len

            # Track similarity with the previous sentence
            if i > 0 and (i - 1) < len(similarities):
                current_similarities.append(similarities[i - 1])

        # Flush remaining sentences
        if current_sentences:
            chunk_text = " ".join(current_sentences)
            avg_similarity = (
                sum(current_similarities) / len(current_similarities)
                if current_similarities
                else 1.0
            )
            chunks.append({
                "text": chunk_text,
                "metadata": {
                    **doc_metadata,
                    "source": source,
                    "doc_index": doc_index,
                    "chunk_index": global_start + len(chunks),
                    "similarity_score": round(avg_similarity, 4),
                },
            })

        return chunks


def register():
    from app.nodes.registry import register_node
    register_node(SemanticChunkerNode())
