"""
Phase 2 integration test: full pipeline execution.

Tests a complete FileSource -> FileParser -> TextChunker pipeline
with a temporary text file, verifying chunks come out correctly.
"""
import os
import tempfile
from pathlib import Path

import pytest

from app.nodes.base import NodeContext
from app.nodes.file_source import FileSourceNode
from app.nodes.file_parser import FileParserNode
from app.nodes.text_chunker import TextChunkerNode


def _make_context(config: dict = None, input_data: dict = None, working_dir: str = None) -> NodeContext:
    """Helper to create a NodeContext."""
    return NodeContext(
        run_id="test-integration-run",
        node_id="test-integration-node",
        config=config or {},
        input_data=input_data or {},
        state={},
        working_dir=working_dir or tempfile.mkdtemp(),
    )


class TestFullPipelineExecution:
    """
    Integration test: FileSource -> FileParser -> TextChunker.

    Creates a temp text file, runs it through all three nodes,
    and verifies that chunks are produced with correct metadata.
    """

    @pytest.fixture
    def temp_file(self):
        """Create a temporary text file with enough content to be chunked."""
        # Create a file with ~200 words to produce multiple chunks
        content = " ".join([f"This is sentence number {i} for testing the ingestion pipeline." for i in range(200)])
        fd, path = tempfile.mkstemp(suffix=".txt", text=True)
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            f.write(content)
        yield path
        os.unlink(path)

    @pytest.mark.asyncio
    async def test_file_source_to_chunks(self, temp_file):
        """
        Full pipeline: FileSource(path) -> FileParser(txt) -> TextChunker(words).

        Steps:
        1. FileSource reads the temp file and produces file metadata.
        2. FileParser parses the file into a document with text.
        3. TextChunker splits the document into chunks.
        """
        # Step 1: FileSource
        file_source = FileSourceNode()
        ctx1 = _make_context(
            config={
                "source_type": "path",
                "file_path": temp_file,
            },
            working_dir=os.path.dirname(temp_file),
        )

        source_result = await file_source.execute(ctx1)
        assert source_result.success is True, f"FileSource failed: {source_result.error_message}"
        assert source_result.items_processed == 1

        file_list = source_result.output_data["file_list"]
        assert len(file_list) == 1
        assert file_list[0]["name"].endswith(".txt")
        assert file_list[0]["size"] > 0

        # Step 2: FileParser
        file_parser = FileParserNode()
        ctx2 = _make_context(
            config={"parser": "auto"},
            input_data={"file_list": file_list},
        )

        parser_result = await file_parser.execute(ctx2)
        assert parser_result.success is True, f"FileParser failed: {parser_result.error_message}"
        assert parser_result.items_processed == 1

        documents = parser_result.output_data["documents"]
        assert len(documents) == 1
        assert documents[0]["text"]
        assert "sentence" in documents[0]["text"]
        assert documents[0]["page_count"] >= 1
        assert documents[0]["metadata"]["source"] == temp_file

        # Step 3: TextChunker
        text_chunker = TextChunkerNode()
        ctx3 = _make_context(
            config={
                "chunk_size": 50,    # 50 words per chunk
                "chunk_overlap": 5,  # 5 word overlap
                "tokenizer": "words",
            },
            input_data={"documents": documents},
        )

        chunker_result = await text_chunker.execute(ctx3)
        assert chunker_result.success is True, f"TextChunker failed: {chunker_result.error_message}"

        chunks = chunker_result.output_data["chunks"]
        assert len(chunks) >= 2, f"Expected at least 2 chunks, got {len(chunks)}"

        # Verify chunk structure
        for i, chunk in enumerate(chunks):
            assert "text" in chunk, f"Chunk {i} missing 'text'"
            assert "chunk_index" in chunk, f"Chunk {i} missing 'chunk_index'"
            assert "metadata" in chunk, f"Chunk {i} missing 'metadata'"
            assert chunk["chunk_index"] == i, f"Chunk {i} has wrong index: {chunk['chunk_index']}"
            # Each chunk should have fewer than chunk_size words (approximately)
            word_count = len(chunk["text"].split())
            assert word_count <= 50, f"Chunk {i} has {word_count} words, expected <= 50"
            # Metadata should carry source info
            assert "source" in chunk["metadata"], f"Chunk {i} metadata missing 'source'"

        # Verify all original content is covered (approximately)
        original_text = documents[0]["text"]
        original_words = set(original_text.split())
        covered_words = set()
        for chunk in chunks:
            covered_words.update(chunk["text"].split())

        # Most words should be covered (allowing for edge effects at boundaries)
        coverage = len(covered_words & original_words) / len(original_words) if original_words else 0
        assert coverage >= 0.9, f"Word coverage is only {coverage:.1%}, expected >= 90%"

        # Verify overlap between consecutive chunks
        if len(chunks) >= 2:
            for i in range(1, len(chunks)):
                prev_words = set(chunks[i - 1]["text"].split())
                curr_words = set(chunks[i]["text"].split())
                overlap = prev_words & curr_words
                # With overlap=5, there should be shared words
                assert len(overlap) >= 1, f"No overlap between chunks {i-1} and {i}"

    @pytest.mark.asyncio
    async def test_pipeline_with_csv_file(self):
        """Test pipeline with a CSV file: FileSource -> FileParser -> TextChunker."""
        # Create a CSV file with enough rows to produce chunks
        rows = [f"name_{i},value_{i},category_{i}" for i in range(100)]
        csv_content = "name,value,category\n" + "\n".join(rows)

        fd, path = tempfile.mkstemp(suffix=".csv", text=True)
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            f.write(csv_content)

        try:
            # Step 1: FileSource
            file_source = FileSourceNode()
            ctx1 = _make_context(
                config={"source_type": "path", "file_path": path},
                working_dir=os.path.dirname(path),
            )
            source_result = await file_source.execute(ctx1)
            assert source_result.success is True

            # Step 2: FileParser
            file_parser = FileParserNode()
            ctx2 = _make_context(
                config={"parser": "csv"},
                input_data={"file_list": source_result.output_data["file_list"]},
            )
            parser_result = await file_parser.execute(ctx2)
            assert parser_result.success is True
            doc_text = parser_result.output_data["documents"][0]["text"]
            assert "name_0" in doc_text
            assert "name_99" in doc_text

            # Step 3: TextChunker
            text_chunker = TextChunkerNode()
            ctx3 = _make_context(
                config={"chunk_size": 10, "chunk_overlap": 2, "tokenizer": "words"},
                input_data={"documents": parser_result.output_data["documents"]},
            )
            chunker_result = await text_chunker.execute(ctx3)
            assert chunker_result.success is True
            chunks = chunker_result.output_data["chunks"]
            assert len(chunks) >= 2
        finally:
            os.unlink(path)

    @pytest.mark.asyncio
    async def test_pipeline_empty_file(self):
        """Test pipeline gracefully handles an empty file."""
        fd, path = tempfile.mkstemp(suffix=".txt", text=True)
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            f.write("")

        try:
            # FileSource
            file_source = FileSourceNode()
            ctx1 = _make_context(
                config={"source_type": "path", "file_path": path},
                working_dir=os.path.dirname(path),
            )
            source_result = await file_source.execute(ctx1)
            assert source_result.success is True

            # FileParser
            file_parser = FileParserNode()
            ctx2 = _make_context(
                config={"parser": "txt"},
                input_data={"file_list": source_result.output_data["file_list"]},
            )
            parser_result = await file_parser.execute(ctx2)
            assert parser_result.success is True

            # TextChunker with empty text
            text_chunker = TextChunkerNode()
            ctx3 = _make_context(
                config={"chunk_size": 50, "chunk_overlap": 5},
                input_data={"documents": parser_result.output_data["documents"]},
            )
            chunker_result = await text_chunker.execute(ctx3)
            assert chunker_result.success is True
            assert chunker_result.output_data["chunks"] == []
        finally:
            os.unlink(path)

    @pytest.mark.asyncio
    async def test_pipeline_multiple_files(self):
        """Test pipeline with multiple files from glob pattern."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create multiple text files
            for i in range(3):
                Path(tmpdir, f"doc{i}.txt").write_text(
                    f"Document {i}: " + " ".join([f"word{j}" for j in range(50)]),
                    encoding="utf-8",
                )

            # FileSource with glob
            file_source = FileSourceNode()
            ctx1 = _make_context(
                config={"source_type": "glob", "file_pattern": "*.txt", "base_dir": tmpdir},
                working_dir=tmpdir,
            )
            source_result = await file_source.execute(ctx1)
            assert source_result.success is True
            assert source_result.items_processed == 3

            # FileParser
            file_parser = FileParserNode()
            ctx2 = _make_context(
                config={"parser": "auto"},
                input_data={"file_list": source_result.output_data["file_list"]},
            )
            parser_result = await file_parser.execute(ctx2)
            assert parser_result.success is True
            assert parser_result.items_processed == 3
            documents = parser_result.output_data["documents"]
            assert len(documents) == 3

            # TextChunker
            text_chunker = TextChunkerNode()
            ctx3 = _make_context(
                config={"chunk_size": 20, "chunk_overlap": 3, "tokenizer": "words"},
                input_data={"documents": documents},
            )
            chunker_result = await text_chunker.execute(ctx3)
            assert chunker_result.success is True
            chunks = chunker_result.output_data["chunks"]
            assert len(chunks) >= 6  # At least 2 chunks per document

            # Verify metadata carries source info for different docs
            sources = set(c["metadata"].get("source", "") for c in chunks)
            assert len(sources) == 3  # 3 different source files
