"""
Phase 2 unit tests for all node implementations.

Tests:
- FileSourceNode: glob pattern, path mode, empty results
- FileParserNode: txt parsing, csv parsing
- TextChunkerNode: chunking with known text, overlap verification
- FilterNode: condition matching
- TransformNode: python expression, jinja2 template
- SplitNode: splitting lists into batches
- MergeNode: concat, zip modes
"""
import os
import tempfile
from pathlib import Path

import pytest

from app.nodes.base import NodeContext
from app.nodes.file_source import FileSourceNode
from app.nodes.file_parser import FileParserNode
from app.nodes.text_chunker import TextChunkerNode
from app.nodes.filter import FilterNode
from app.nodes.transform import TransformNode
from app.nodes.split import SplitNode
from app.nodes.merge import MergeNode


def _make_context(config: dict = None, input_data: dict = None, working_dir: str = None) -> NodeContext:
    """Helper to create a NodeContext with sensible defaults."""
    return NodeContext(
        run_id="test-run",
        node_id="test-node",
        config=config or {},
        input_data=input_data or {},
        state={},
        working_dir=working_dir or tempfile.mkdtemp(),
    )


# ============================================================
# FileSourceNode Tests
# ============================================================

class TestFileSourceNode:
    """Tests for FileSourceNode."""

    @pytest.fixture
    def node(self):
        return FileSourceNode()

    @pytest.mark.asyncio
    async def test_glob_pattern(self, node):
        """Test that glob source_type finds matching files."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create test files
            Path(tmpdir, "file1.txt").write_text("hello", encoding="utf-8")
            Path(tmpdir, "file2.txt").write_text("world", encoding="utf-8")
            Path(tmpdir, "other.csv").write_text("a,b\n1,2\n", encoding="utf-8")

            ctx = _make_context(
                config={
                    "source_type": "glob",
                    "file_pattern": "*.txt",
                    "base_dir": tmpdir,
                },
                working_dir=tmpdir,
            )

            result = await node.execute(ctx)
            assert result.success is True
            assert result.items_processed == 2
            file_list = result.output_data["file_list"]
            names = [f["name"] for f in file_list]
            assert "file1.txt" in names
            assert "file2.txt" in names
            assert "other.csv" not in names

    @pytest.mark.asyncio
    async def test_path_single_file(self, node):
        """Test that path source_type reads a single file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            filepath = Path(tmpdir, "single.txt")
            filepath.write_text("single file content", encoding="utf-8")

            ctx = _make_context(
                config={
                    "source_type": "path",
                    "file_path": str(filepath),
                    "base_dir": tmpdir,
                },
                working_dir=tmpdir,
            )

            result = await node.execute(ctx)
            assert result.success is True
            assert result.items_processed == 1
            file_list = result.output_data["file_list"]
            assert len(file_list) == 1
            assert file_list[0]["name"] == "single.txt"
            assert file_list[0]["path"] == str(filepath)
            assert file_list[0]["size"] > 0
            assert "content_type" in file_list[0]
            assert "extension" in file_list[0]

    @pytest.mark.asyncio
    async def test_empty_directory(self, node):
        """Test that glob on an empty directory returns empty list."""
        with tempfile.TemporaryDirectory() as tmpdir:
            ctx = _make_context(
                config={
                    "source_type": "glob",
                    "file_pattern": "*.txt",
                    "base_dir": tmpdir,
                },
                working_dir=tmpdir,
            )

            result = await node.execute(ctx)
            assert result.success is True
            assert result.items_processed == 0
            assert result.output_data["file_list"] == []

    @pytest.mark.asyncio
    async def test_path_nonexistent_file(self, node):
        """Test that path source_type fails for nonexistent file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            ctx = _make_context(
                config={
                    "source_type": "path",
                    "file_path": "nonexistent.txt",
                    "base_dir": tmpdir,
                },
                working_dir=tmpdir,
            )

            result = await node.execute(ctx)
            assert result.success is False
            assert result.error_message is not None

    @pytest.mark.asyncio
    async def test_recursive_glob(self, node):
        """Test that glob finds files in subdirectories when recursive."""
        with tempfile.TemporaryDirectory() as tmpdir:
            subdir = Path(tmpdir, "subdir")
            subdir.mkdir()
            Path(subdir, "nested.txt").write_text("nested", encoding="utf-8")
            Path(tmpdir, "top.txt").write_text("top", encoding="utf-8")

            ctx = _make_context(
                config={
                    "source_type": "glob",
                    "file_pattern": "**/*.txt",
                    "base_dir": tmpdir,
                    "recursive": True,
                },
                working_dir=tmpdir,
            )

            result = await node.execute(ctx)
            assert result.success is True
            assert result.items_processed == 2

    @pytest.mark.asyncio
    async def test_file_metadata_fields(self, node):
        """Test that file metadata includes all expected fields."""
        with tempfile.TemporaryDirectory() as tmpdir:
            filepath = Path(tmpdir, "test.csv")
            filepath.write_text("col1,col2\nval1,val2\n", encoding="utf-8")

            ctx = _make_context(
                config={
                    "source_type": "path",
                    "file_path": str(filepath),
                    "base_dir": tmpdir,
                },
                working_dir=tmpdir,
            )

            result = await node.execute(ctx)
            meta = result.output_data["file_list"][0]
            assert "path" in meta
            assert "name" in meta
            assert "size" in meta
            assert "content_type" in meta
            assert "extension" in meta
            assert meta["extension"] == ".csv"


# ============================================================
# FileParserNode Tests
# ============================================================

class TestFileParserNode:
    """Tests for FileParserNode."""

    @pytest.fixture
    def node(self):
        return FileParserNode()

    @pytest.mark.asyncio
    async def test_txt_parsing(self, node):
        """Test parsing a plain text file."""
        with tempfile.NamedTemporaryFile(suffix=".txt", mode="w", delete=False, encoding="utf-8") as f:
            f.write("Hello, this is a test document.\nWith multiple lines.\n")
            f.flush()
            filepath = f.name

        try:
            ctx = _make_context(
                config={"parser": "txt"},
                input_data={
                    "file_list": [{
                        "path": filepath,
                        "name": "test.txt",
                        "size": os.path.getsize(filepath),
                        "content_type": "text/plain",
                        "extension": ".txt",
                    }]
                },
            )

            result = await node.execute(ctx)
            assert result.success is True
            assert result.items_processed == 1
            docs = result.output_data["documents"]
            assert len(docs) == 1
            assert "Hello" in docs[0]["text"]
            assert "multiple lines" in docs[0]["text"]
            assert docs[0]["page_count"] >= 1
            assert docs[0]["metadata"]["parser"] == "txt"
        finally:
            os.unlink(filepath)

    @pytest.mark.asyncio
    async def test_csv_parsing(self, node):
        """Test parsing a CSV file."""
        with tempfile.NamedTemporaryFile(suffix=".csv", mode="w", delete=False, encoding="utf-8") as f:
            f.write("name,age,city\nAlice,30,NYC\nBob,25,LA\nCharlie,35,SF\n")
            f.flush()
            filepath = f.name

        try:
            ctx = _make_context(
                config={"parser": "csv"},
                input_data={
                    "file_list": [{
                        "path": filepath,
                        "name": "test.csv",
                        "size": os.path.getsize(filepath),
                        "content_type": "text/csv",
                        "extension": ".csv",
                    }]
                },
            )

            result = await node.execute(ctx)
            assert result.success is True
            assert result.items_processed == 1
            docs = result.output_data["documents"]
            assert len(docs) == 1
            text = docs[0]["text"]
            assert "Alice" in text
            assert "Bob" in text
            assert "Charlie" in text
            assert "name" in text
            assert docs[0]["metadata"]["parser"] == "csv"
        finally:
            os.unlink(filepath)

    @pytest.mark.asyncio
    async def test_auto_detection_txt(self, node):
        """Test auto-detection of txt files by extension."""
        with tempfile.NamedTemporaryFile(suffix=".md", mode="w", delete=False, encoding="utf-8") as f:
            f.write("# Title\n\nSome markdown content here.")
            f.flush()
            filepath = f.name

        try:
            ctx = _make_context(
                config={"parser": "auto"},
                input_data={
                    "file_list": [{
                        "path": filepath,
                        "name": "test.md",
                        "size": os.path.getsize(filepath),
                        "content_type": "text/markdown",
                        "extension": ".md",
                    }]
                },
            )

            result = await node.execute(ctx)
            assert result.success is True
            docs = result.output_data["documents"]
            assert docs[0]["text"] == "# Title\n\nSome markdown content here."
            # .md maps to "txt" parser
            assert docs[0]["metadata"]["parser"] == "txt"
        finally:
            os.unlink(filepath)

    @pytest.mark.asyncio
    async def test_empty_file_list(self, node):
        """Test that empty file_list returns empty documents."""
        ctx = _make_context(
            config={"parser": "auto"},
            input_data={"file_list": []},
        )

        result = await node.execute(ctx)
        assert result.success is True
        assert result.output_data["documents"] == []

    @pytest.mark.asyncio
    async def test_metadata_preserved(self, node):
        """Test that file metadata is preserved in parsed documents."""
        with tempfile.NamedTemporaryFile(suffix=".txt", mode="w", delete=False, encoding="utf-8") as f:
            f.write("content")
            f.flush()
            filepath = f.name

        try:
            ctx = _make_context(
                config={"parser": "txt"},
                input_data={
                    "file_list": [{
                        "path": filepath,
                        "name": "special.txt",
                        "size": 7,
                        "content_type": "text/plain",
                        "extension": ".txt",
                    }]
                },
            )

            result = await node.execute(ctx)
            doc_meta = result.output_data["documents"][0]["metadata"]
            assert doc_meta["name"] == "special.txt"
            assert doc_meta["size"] == 7
            assert doc_meta["source"] == filepath
            assert doc_meta["content_type"] == "text/plain"
        finally:
            os.unlink(filepath)


# ============================================================
# TextChunkerNode Tests
# ============================================================

class TestTextChunkerNode:
    """Tests for TextChunkerNode."""

    @pytest.fixture
    def node(self):
        return TextChunkerNode()

    @pytest.mark.asyncio
    async def test_chunking_with_words(self, node):
        """Test basic word-based chunking."""
        # Create a document with exactly 20 words
        words = [f"word{i}" for i in range(20)]
        text = " ".join(words)

        ctx = _make_context(
            config={"chunk_size": 8, "chunk_overlap": 2, "tokenizer": "words"},
            input_data={
                "documents": [{
                    "text": text,
                    "metadata": {"source": "test.txt"},
                    "page_count": 1,
                }]
            },
        )

        result = await node.execute(ctx)
        assert result.success is True
        chunks = result.output_data["chunks"]
        assert len(chunks) >= 2

        # Verify all original words appear somewhere in chunks
        all_chunk_text = " ".join(c["text"] for c in chunks)
        for w in words:
            assert w in all_chunk_text

    @pytest.mark.asyncio
    async def test_chunking_with_chars(self, node):
        """Test character-based chunking."""
        text = "a" * 100

        ctx = _make_context(
            config={"chunk_size": 30, "chunk_overlap": 5, "tokenizer": "chars"},
            input_data={
                "documents": [{
                    "text": text,
                    "metadata": {},
                    "page_count": 1,
                }]
            },
        )

        result = await node.execute(ctx)
        assert result.success is True
        chunks = result.output_data["chunks"]
        assert len(chunks) >= 2

        # Each chunk should be at most chunk_size characters
        for chunk in chunks:
            assert len(chunk["text"]) <= 30

        # The last chunk should contain the last character of the text
        assert chunks[-1]["text"][-1] == "a"
        # The first chunk should contain the first character
        assert chunks[0]["text"][0] == "a"

    @pytest.mark.asyncio
    async def test_overlap_verification(self, node):
        """Test that chunks overlap correctly."""
        text = " ".join([f"word{i}" for i in range(30)])

        ctx = _make_context(
            config={"chunk_size": 10, "chunk_overlap": 3, "tokenizer": "words"},
            input_data={
                "documents": [{
                    "text": text,
                    "metadata": {},
                    "page_count": 1,
                }]
            },
        )

        result = await node.execute(ctx)
        chunks = result.output_data["chunks"]
        assert len(chunks) >= 2

        # Verify overlap between consecutive chunks
        for i in range(1, len(chunks)):
            prev_words = set(chunks[i - 1]["text"].split())
            curr_words = set(chunks[i]["text"].split())
            overlap = prev_words & curr_words
            assert len(overlap) >= 1, f"No overlap between chunk {i-1} and {i}"

    @pytest.mark.asyncio
    async def test_chunk_index_sequential(self, node):
        """Test that chunk_index values are sequential."""
        text = " ".join([f"word{i}" for i in range(50)])

        ctx = _make_context(
            config={"chunk_size": 10, "chunk_overlap": 2, "tokenizer": "words"},
            input_data={
                "documents": [{
                    "text": text,
                    "metadata": {},
                    "page_count": 1,
                }]
            },
        )

        result = await node.execute(ctx)
        chunks = result.output_data["chunks"]
        indices = [c["chunk_index"] for c in chunks]
        assert indices == list(range(len(chunks)))

    @pytest.mark.asyncio
    async def test_chunk_metadata(self, node):
        """Test that chunks carry document metadata."""
        ctx = _make_context(
            config={"chunk_size": 5, "chunk_overlap": 1, "tokenizer": "words"},
            input_data={
                "documents": [{
                    "text": "hello world this is a test",
                    "metadata": {"source": "file.txt", "custom": "value"},
                    "page_count": 3,
                }]
            },
        )

        result = await node.execute(ctx)
        chunks = result.output_data["chunks"]
        assert len(chunks) >= 1
        for chunk in chunks:
            assert chunk["metadata"]["source"] == "file.txt"
            assert chunk["metadata"]["custom"] == "value"
            assert chunk["metadata"]["page_count"] == 3
            assert "doc_index" in chunk["metadata"]
            assert "chunk_tokens" in chunk["metadata"]

    @pytest.mark.asyncio
    async def test_overlap_ge_chunk_size_fails(self, node):
        """Test that overlap >= chunk_size returns an error."""
        ctx = _make_context(
            config={"chunk_size": 10, "chunk_overlap": 10, "tokenizer": "words"},
            input_data={"documents": [{"text": "some text"}]},
        )

        result = await node.execute(ctx)
        assert result.success is False
        assert "overlap" in result.error_message.lower()

    @pytest.mark.asyncio
    async def test_empty_documents(self, node):
        """Test that empty document list returns empty chunks."""
        ctx = _make_context(
            config={"chunk_size": 512, "chunk_overlap": 50},
            input_data={"documents": []},
        )

        result = await node.execute(ctx)
        assert result.success is True
        assert result.output_data["chunks"] == []

    @pytest.mark.asyncio
    async def test_tiktoken_chunking(self, node):
        """Test chunking with tiktoken tokenizer."""
        text = "This is a test sentence for tiktoken chunking. " * 100

        ctx = _make_context(
            config={"chunk_size": 50, "chunk_overlap": 5, "tokenizer": "tiktoken_cl100k"},
            input_data={
                "documents": [{
                    "text": text,
                    "metadata": {},
                    "page_count": 1,
                }]
            },
        )

        result = await node.execute(ctx)
        assert result.success is True
        chunks = result.output_data["chunks"]
        assert len(chunks) >= 2


# ============================================================
# FilterNode Tests
# ============================================================

class TestFilterNode:
    """Tests for FilterNode."""

    @pytest.fixture
    def node(self):
        return FilterNode()

    @pytest.mark.asyncio
    async def test_condition_matching_size(self, node):
        """Test filtering by size attribute."""
        items = [
            {"name": "small", "size": 10},
            {"name": "big", "size": 200},
            {"name": "medium", "size": 50},
        ]

        ctx = _make_context(
            config={"condition": "item['size'] > 30"},
            input_data={"items": items},
        )

        result = await node.execute(ctx)
        assert result.success is True
        matched = result.output_data["matched"]
        rejected = result.output_data["rejected"]
        assert len(matched) == 2  # big and medium
        assert len(rejected) == 1  # small
        assert matched[0]["name"] == "big"
        assert rejected[0]["name"] == "small"

    @pytest.mark.asyncio
    async def test_condition_matching_string(self, node):
        """Test filtering by string attribute."""
        items = [
            {"type": "pdf", "name": "doc1"},
            {"type": "txt", "name": "doc2"},
            {"type": "pdf", "name": "doc3"},
        ]

        ctx = _make_context(
            config={"condition": "item.get('type') == 'pdf'"},
            input_data={"items": items},
        )

        result = await node.execute(ctx)
        matched = result.output_data["matched"]
        assert len(matched) == 2
        assert all(m["type"] == "pdf" for m in matched)

    @pytest.mark.asyncio
    async def test_all_matched(self, node):
        """Test when all items match."""
        items = [{"val": 1}, {"val": 2}, {"val": 3}]
        ctx = _make_context(
            config={"condition": "item['val'] > 0"},
            input_data={"items": items},
        )

        result = await node.execute(ctx)
        assert len(result.output_data["matched"]) == 3
        assert len(result.output_data["rejected"]) == 0

    @pytest.mark.asyncio
    async def test_none_matched(self, node):
        """Test when no items match."""
        items = [{"val": 1}, {"val": 2}]
        ctx = _make_context(
            config={"condition": "item['val'] > 100"},
            input_data={"items": items},
        )

        result = await node.execute(ctx)
        assert len(result.output_data["matched"]) == 0
        assert len(result.output_data["rejected"]) == 2

    @pytest.mark.asyncio
    async def test_empty_items(self, node):
        """Test filter on empty list."""
        ctx = _make_context(
            config={"condition": "item > 0"},
            input_data={"items": []},
        )

        result = await node.execute(ctx)
        assert result.success is True
        assert result.output_data["matched"] == []
        assert result.output_data["rejected"] == []

    @pytest.mark.asyncio
    async def test_no_condition_returns_error(self, node):
        """Test that missing condition returns an error."""
        ctx = _make_context(
            config={},
            input_data={"items": [{"a": 1}]},
        )

        result = await node.execute(ctx)
        assert result.success is False
        assert "condition" in result.error_message.lower()

    @pytest.mark.asyncio
    async def test_dangerous_expression_blocked(self, node):
        """Test that private attribute access is blocked in conditions."""
        ctx = _make_context(
            config={"condition": "item.__class__"},
            input_data={"items": [{"a": 1}]},
        )

        result = await node.execute(ctx)
        assert result.success is False
        assert "private" in result.error_message.lower()


# ============================================================
# TransformNode Tests
# ============================================================

class TestTransformNode:
    """Tests for TransformNode."""

    @pytest.fixture
    def node(self):
        return TransformNode()

    @pytest.mark.asyncio
    async def test_python_expression_upper(self, node):
        """Test Python expression that uppercases a string."""
        ctx = _make_context(
            config={"mode": "python", "expression": "str(data).upper()"},
            input_data={"data": "hello world"},
        )

        result = await node.execute(ctx)
        assert result.success is True
        assert result.output_data["result"] == "HELLO WORLD"

    @pytest.mark.asyncio
    async def test_python_expression_list_comprehension(self, node):
        """Test Python expression with list comprehension."""
        ctx = _make_context(
            config={"mode": "python", "expression": "[x * 2 for x in data]"},
            input_data={"data": [1, 2, 3, 4]},
        )

        result = await node.execute(ctx)
        assert result.success is True
        assert result.output_data["result"] == [2, 4, 6, 8]

    @pytest.mark.asyncio
    async def test_python_expression_dict_access(self, node):
        """Test Python expression accessing dict keys."""
        ctx = _make_context(
            config={"mode": "python", "expression": "data.get('items', [])[:2]"},
            input_data={"data": {"items": ["a", "b", "c", "d"]}},
        )

        result = await node.execute(ctx)
        assert result.success is True
        assert result.output_data["result"] == ["a", "b"]

    @pytest.mark.asyncio
    async def test_python_expression_len(self, node):
        """Test Python expression with len."""
        ctx = _make_context(
            config={"mode": "python", "expression": "len(data)"},
            input_data={"data": [1, 2, 3, 4, 5]},
        )

        result = await node.execute(ctx)
        assert result.success is True
        assert result.output_data["result"] == 5

    @pytest.mark.asyncio
    async def test_jinja2_template(self, node):
        """Test Jinja2 template rendering with dict data."""
        ctx = _make_context(
            config={"mode": "jinja2", "expression": "Hello {{ name }}, you are {{ age }} years old!"},
            input_data={"data": {"name": "Alice", "age": 30}},
        )

        result = await node.execute(ctx)
        assert result.success is True
        assert result.output_data["result"] == "Hello Alice, you are 30 years old!"

    @pytest.mark.asyncio
    async def test_jinja2_loop(self, node):
        """Test Jinja2 template with loop."""
        ctx = _make_context(
            config={
                "mode": "jinja2",
                "expression": "{% for item in items %}{{ item }} {% endfor %}",
            },
            input_data={"data": {"items": ["a", "b", "c"]}},
        )

        result = await node.execute(ctx)
        assert result.success is True
        assert "a" in result.output_data["result"]
        assert "b" in result.output_data["result"]
        assert "c" in result.output_data["result"]

    @pytest.mark.asyncio
    async def test_no_expression_returns_error(self, node):
        """Test that missing expression returns an error."""
        ctx = _make_context(
            config={"mode": "python"},
            input_data={"data": "test"},
        )

        result = await node.execute(ctx)
        assert result.success is False
        assert "expression" in result.error_message.lower()

    @pytest.mark.asyncio
    async def test_dangerous_python_blocked(self, node):
        """Test that import statements are blocked in Python expressions."""
        ctx = _make_context(
            config={"mode": "python", "expression": "__import__('os')"},
            input_data={"data": None},
        )

        result = await node.execute(ctx)
        assert result.success is False


# ============================================================
# SplitNode Tests
# ============================================================

class TestSplitNode:
    """Tests for SplitNode."""

    @pytest.fixture
    def node(self):
        return SplitNode()

    @pytest.mark.asyncio
    async def test_split_batch_size_1(self, node):
        """Test splitting list into individual items (batch_size=1)."""
        items = ["a", "b", "c", "d", "e"]
        ctx = _make_context(
            config={"batch_size": 1},
            input_data={"items": items},
        )

        result = await node.execute(ctx)
        assert result.success is True
        batches = result.output_data["item"]
        assert len(batches) == 5
        assert batches[0] == ["a"]
        assert batches[4] == ["e"]
        assert result.metadata["batch_count"] == 5

    @pytest.mark.asyncio
    async def test_split_batch_size_2(self, node):
        """Test splitting list into batches of 2."""
        items = [1, 2, 3, 4, 5]
        ctx = _make_context(
            config={"batch_size": 2},
            input_data={"items": items},
        )

        result = await node.execute(ctx)
        batches = result.output_data["item"]
        assert len(batches) == 3  # [1,2], [3,4], [5]
        assert batches[0] == [1, 2]
        assert batches[1] == [3, 4]
        assert batches[2] == [5]

    @pytest.mark.asyncio
    async def test_split_empty_list(self, node):
        """Test splitting empty list."""
        ctx = _make_context(
            config={"batch_size": 1},
            input_data={"items": []},
        )

        result = await node.execute(ctx)
        assert result.success is True
        assert result.output_data["item"] == []
        assert result.items_processed == 0

    @pytest.mark.asyncio
    async def test_split_non_list_input(self, node):
        """Test that non-list input returns an error."""
        ctx = _make_context(
            config={"batch_size": 1},
            input_data={"items": "not a list"},
        )

        result = await node.execute(ctx)
        assert result.success is False
        assert "list" in result.error_message.lower()

    @pytest.mark.asyncio
    async def test_split_batch_larger_than_list(self, node):
        """Test batch_size larger than the list."""
        ctx = _make_context(
            config={"batch_size": 100},
            input_data={"items": [1, 2, 3]},
        )

        result = await node.execute(ctx)
        batches = result.output_data["item"]
        assert len(batches) == 1
        assert batches[0] == [1, 2, 3]


# ============================================================
# MergeNode Tests
# ============================================================

class TestMergeNode:
    """Tests for MergeNode."""

    @pytest.fixture
    def node(self):
        return MergeNode()

    @pytest.mark.asyncio
    async def test_concat_mode(self, node):
        """Test concatenating multiple lists."""
        ctx = _make_context(
            config={"mode": "concat"},
            input_data={
                "source_a": [1, 2, 3],
                "source_b": [4, 5],
                "source_c": [6],
            },
        )

        result = await node.execute(ctx)
        assert result.success is True
        merged = result.output_data["merged"]
        assert merged == [1, 2, 3, 4, 5, 6]

    @pytest.mark.asyncio
    async def test_concat_multiple_keys(self, node):
        """Test concat when inputs come from different keys."""
        ctx = _make_context(
            config={"mode": "concat"},
            input_data={
                "source_a": [1, 2],
                "source_b": [3, 4],
            },
        )

        result = await node.execute(ctx)
        merged = result.output_data["merged"]
        assert 1 in merged
        assert 2 in merged
        assert 3 in merged
        assert 4 in merged
        assert len(merged) == 4

    @pytest.mark.asyncio
    async def test_zip_mode(self, node):
        """Test zipping lists together."""
        ctx = _make_context(
            config={"mode": "zip"},
            input_data={
                "list_a": ["a", "b", "c"],
                "list_b": [1, 2, 3],
                "list_c": ["x", "y", "z"],
            },
        )

        result = await node.execute(ctx)
        merged = result.output_data["merged"]
        assert len(merged) == 3
        assert merged[0] == ["a", 1, "x"]
        assert merged[1] == ["b", 2, "y"]
        assert merged[2] == ["c", 3, "z"]

    @pytest.mark.asyncio
    async def test_zip_uneven_lists(self, node):
        """Test zipping lists of different lengths (zips to shortest)."""
        ctx = _make_context(
            config={"mode": "zip"},
            input_data={
                "short": ["a", "b"],
                "long": [1, 2, 3, 4],
            },
        )

        result = await node.execute(ctx)
        merged = result.output_data["merged"]
        assert len(merged) == 2
        assert merged[0] == ["a", 1]
        assert merged[1] == ["b", 2]

    @pytest.mark.asyncio
    async def test_merge_objects_mode(self, node):
        """Test deep-merging dicts."""
        ctx = _make_context(
            config={"mode": "merge_objects"},
            input_data={
                "inputs": [
                    {"a": 1, "b": 2},
                    {"b": 3, "c": 4},
                ],
            },
        )

        result = await node.execute(ctx)
        merged = result.output_data["merged"]
        assert merged["a"] == 1
        assert merged["b"] == 3  # overridden by second dict
        assert merged["c"] == 4

    @pytest.mark.asyncio
    async def test_merge_objects_deep(self, node):
        """Test deep merging with nested dicts."""
        ctx = _make_context(
            config={"mode": "merge_objects"},
            input_data={
                "inputs": [
                    {"config": {"timeout": 10, "retries": 3}},
                    {"config": {"timeout": 30, "verbose": True}},
                ],
            },
        )

        result = await node.execute(ctx)
        merged = result.output_data["merged"]
        assert merged["config"]["timeout"] == 30
        assert merged["config"]["retries"] == 3
        assert merged["config"]["verbose"] is True

    @pytest.mark.asyncio
    async def test_empty_inputs(self, node):
        """Test merge with no inputs."""
        ctx = _make_context(
            config={"mode": "concat"},
            input_data={},
        )

        result = await node.execute(ctx)
        assert result.success is True
        assert result.output_data["merged"] == []

    @pytest.mark.asyncio
    async def test_single_list_concat(self, node):
        """Test concat with a single list."""
        ctx = _make_context(
            config={"mode": "concat"},
            input_data={"inputs": [1, 2, 3]},
        )

        result = await node.execute(ctx)
        merged = result.output_data["merged"]
        assert merged == [1, 2, 3]
