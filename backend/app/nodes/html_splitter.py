"""
HTML Splitter node: splits HTML documents into chunks based on tag priority.

Walks the HTML DOM, creates sections at priority tags (h1, h2, p, etc.),
merges small sections up to max_chunk_size, and splits oversized sections
by character.
"""
import logging
from html.parser import HTMLParser
from typing import Any

from app.nodes.base import BaseNode, NodeContext, NodeResult, PortDef, PortDataType

logger = logging.getLogger(__name__)


class _HTMLSectionParser(HTMLParser):
    """
    Parse HTML into sections delimited by priority tags.

    Each time a priority tag is opened or closed, the current section is
    finalised and a new one begins.
    """

    def __init__(self, priority_tags: set[str]) -> None:
        super().__init__()
        self._priority_tags = priority_tags
        self._sections: list[dict[str, Any]] = []
        self._current_parts: list[str] = []
        self._current_tags: list[str] = []  # tag hierarchy stack
        self._skip = False

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag in ("script", "style"):
            self._skip = True
            return
        if tag in self._priority_tags and self._current_parts:
            self._flush_section()
        self._current_tags.append(tag)

    def handle_endtag(self, tag: str) -> None:
        if tag in ("script", "style"):
            self._skip = False
            return
        if tag in self._current_tags:
            self._current_tags.pop()
        if tag in self._priority_tags:
            self._flush_section()

    def handle_data(self, data: str) -> None:
        if not self._skip:
            self._current_parts.append(data)

    def _flush_section(self) -> None:
        text = "".join(self._current_parts).strip()
        if text:
            self._sections.append({
                "text": text,
                "tag_hierarchy": list(self._current_tags),
            })
        self._current_parts = []

    def get_sections(self) -> list[dict[str, Any]]:
        self._flush_section()  # flush any remaining content
        return self._sections


class HTMLSplitterNode(BaseNode):
    @property
    def node_type(self) -> str:
        return "html_splitter"

    @property
    def display_name(self) -> str:
        return "HTML Splitter"

    @property
    def category(self) -> str:
        return "processing"

    @property
    def description(self) -> str:
        return "Split HTML documents into chunks based on tag priority"

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
                "tag_priority": {
                    "type": "string",
                    "default": "h1,h2,h3,p,table,section",
                    "description": "HTML tags to split on, tried in order",
                },
                "max_chunk_size": {
                    "type": "integer",
                    "default": 2000,
                    "minimum": 100,
                    "description": "Maximum characters per chunk",
                },
                "include_metadata": {
                    "type": "boolean",
                    "default": True,
                    "description": "Include HTML tag metadata in chunk headers",
                },
            },
        }

    async def execute(self, context: NodeContext) -> NodeResult:
        """Split HTML documents into chunks based on tag boundaries."""
        documents = context.input_data.get("documents", [])
        if not documents:
            return NodeResult(
                success=True,
                output_data={"chunks": []},
                items_processed=0,
            )

        tag_priority_str = context.config.get("tag_priority", "h1,h2,h3,p,table,section")
        max_chunk_size = context.config.get("max_chunk_size", 2000)
        include_metadata = context.config.get("include_metadata", True)

        priority_tags = {t.strip().lower() for t in tag_priority_str.split(",") if t.strip()}

        all_chunks: list[dict[str, Any]] = []
        global_chunk_index = 0

        for doc_idx, doc in enumerate(documents):
            text = doc.get("text", "")
            if not text.strip():
                continue

            doc_metadata = doc.get("metadata", {})
            source = doc_metadata.get("source", doc_metadata.get("name", ""))

            # Parse HTML into sections
            parser = _HTMLSectionParser(priority_tags)
            parser.feed(text)
            sections = parser.get_sections()

            if not sections:
                # No sections found (plain text or no priority tags matched);
                # treat the whole document as one section
                sections = [{"text": text, "tag_hierarchy": []}]

            # Merge small adjacent sections until they reach max_chunk_size
            merged = self._merge_sections(sections, max_chunk_size)

            # Split any sections that still exceed max_chunk_size
            for section in merged:
                section_text = section["text"]
                tag_hierarchy = section.get("tag_hierarchy", [])

                if len(section_text) <= max_chunk_size:
                    chunk_entry: dict[str, Any] = {
                        "text": section_text,
                        "metadata": {
                            "source": source,
                            "tag_hierarchy": tag_hierarchy,
                            "chunk_index": global_chunk_index,
                        },
                    }
                    if include_metadata:
                        chunk_entry["metadata"].update({
                            k: v for k, v in doc_metadata.items()
                            if k not in ("source", "name")
                        })
                    all_chunks.append(chunk_entry)
                    global_chunk_index += 1
                else:
                    # Split oversized section by character
                    sub_chunks = self._split_by_char(
                        section_text, max_chunk_size, tag_hierarchy
                    )
                    for sub_chunk_text, sub_hierarchy in sub_chunks:
                        chunk_entry = {
                            "text": sub_chunk_text,
                            "metadata": {
                                "source": source,
                                "tag_hierarchy": sub_hierarchy,
                                "chunk_index": global_chunk_index,
                            },
                        }
                        if include_metadata:
                            chunk_entry["metadata"].update({
                                k: v for k, v in doc_metadata.items()
                                if k not in ("source", "name")
                            })
                        all_chunks.append(chunk_entry)
                        global_chunk_index += 1

        return NodeResult(
            success=True,
            output_data={"chunks": all_chunks},
            items_processed=len(all_chunks),
            metadata={
                "total_documents": len(documents),
                "total_chunks": len(all_chunks),
                "max_chunk_size": max_chunk_size,
                "tag_priority": tag_priority_str,
            },
        )

    @staticmethod
    def _merge_sections(
        sections: list[dict[str, Any]],
        max_chunk_size: int,
    ) -> list[dict[str, Any]]:
        """Merge adjacent sections until they approach max_chunk_size."""
        merged: list[dict[str, Any]] = []
        for section in sections:
            if not merged:
                merged.append({
                    "text": section["text"],
                    "tag_hierarchy": list(section.get("tag_hierarchy", [])),
                })
                continue

            last = merged[-1]
            combined_len = len(last["text"]) + len(section["text"])
            if combined_len <= max_chunk_size:
                last["text"] = last["text"] + "\n\n" + section["text"]
                # Keep the most specific tag hierarchy
                if section.get("tag_hierarchy"):
                    last["tag_hierarchy"] = list(section["tag_hierarchy"])
            else:
                merged.append({
                    "text": section["text"],
                    "tag_hierarchy": list(section.get("tag_hierarchy", [])),
                })

        return merged

    @staticmethod
    def _split_by_char(
        text: str,
        max_chunk_size: int,
        tag_hierarchy: list[str],
    ) -> list[tuple[str, list[str]]]:
        """Split a long text into character-based chunks at max_chunk_size."""
        chunks: list[tuple[str, list[str]]] = []
        for i in range(0, len(text), max_chunk_size):
            chunk_text = text[i : i + max_chunk_size]
            chunks.append((chunk_text, list(tag_hierarchy)))
        return chunks


def register():
    from app.nodes.registry import register_node
    register_node(HTMLSplitterNode())
