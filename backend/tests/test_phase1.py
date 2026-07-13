"""
Phase 1 integration tests for ingestion-graph backend.

Tests:
- All module imports
- DAG scheduler (topological sort, parallel grouping, cycle detection)
- Execution state machine (valid/invalid transitions)
- Auth service (password hashing, JWT tokens)
- Node registry (auto-discovery, categories)
- Node config validation
- Retry handler (exponential backoff)
- Graph service (checksum)
"""
import asyncio
import uuid
import pytest

from app.engine.scheduler import topological_sort, validate_dag
from app.engine.state import can_transition, ExecutionState
from app.engine.retry import calculate_delay, RetryConfig, retry_async
from app.services.auth_service import (
    hash_password, verify_password, create_access_token, decode_token,
)
from app.services.graph_service import compute_checksum
from app.nodes.registry import get_all_nodes, get_node, get_nodes_by_category, discover_nodes
from app.nodes.file_source import register as register_file_source
from app.nodes.file_parser import FileParserNode


class TestDAGScheduler:
    """Tests for DAG topological sort and validation."""

    def test_sequential_dag(self):
        nodes = {
            'a': {'id': 'a', 'type': 'file_source'},
            'b': {'id': 'b', 'type': 'file_parser'},
            'c': {'id': 'c', 'type': 'text_chunker'},
            'd': {'id': 'd', 'type': 'embedder'},
            'e': {'id': 'e', 'type': 'vector_store'},
        }
        edges = [
            {'source': 'a', 'target': 'b'},
            {'source': 'b', 'target': 'c'},
            {'source': 'c', 'target': 'd'},
            {'source': 'd', 'target': 'e'},
        ]
        levels = topological_sort(nodes, edges)
        assert levels == [['a'], ['b'], ['c'], ['d'], ['e']]

    def test_parallel_dag(self):
        nodes = {
            'a': {'id': 'a', 'type': 'file_source'},
            'b': {'id': 'b', 'type': 'file_parser'},
            'c': {'id': 'c', 'type': 'file_parser'},
            'd': {'id': 'd', 'type': 'merge'},
        }
        edges = [
            {'source': 'a', 'target': 'b'},
            {'source': 'a', 'target': 'c'},
            {'source': 'b', 'target': 'd'},
            {'source': 'c', 'target': 'd'},
        ]
        levels = topological_sort(nodes, edges)
        assert len(levels) == 3
        assert len(levels[1]) == 2  # b and c in parallel

    def test_cycle_detection(self):
        nodes = {'a': {'id': 'a'}, 'b': {'id': 'b'}, 'c': {'id': 'c'}}
        edges = [
            {'source': 'a', 'target': 'b'},
            {'source': 'b', 'target': 'c'},
            {'source': 'c', 'target': 'a'},
        ]
        with pytest.raises(ValueError, match="cycle"):
            topological_sort(nodes, edges)

    def test_validate_dag_valid(self):
        nodes = {'a': {'id': 'a'}, 'b': {'id': 'b'}}
        edges = [{'source': 'a', 'target': 'b'}]
        errors = validate_dag(nodes, edges)
        assert len(errors) == 0

    def test_validate_dag_invalid_edge(self):
        nodes = {'a': {'id': 'a'}, 'b': {'id': 'b'}}
        edges = [{'source': 'a', 'target': 'nonexistent'}]
        errors = validate_dag(nodes, edges)
        assert len(errors) > 0


class TestStateMachine:
    """Tests for execution state transitions."""

    def test_valid_transitions(self):
        assert can_transition('pending', 'running') is True
        assert can_transition('running', 'paused') is True
        assert can_transition('running', 'completed') is True
        assert can_transition('running', 'failed') is True
        assert can_transition('running', 'cancelled') is True
        assert can_transition('paused', 'running') is True
        assert can_transition('failed', 'running') is True  # retry
        assert can_transition('failed', 'superseded') is True

    def test_invalid_transitions(self):
        assert can_transition('pending', 'completed') is False
        assert can_transition('completed', 'running') is False
        assert can_transition('cancelled', 'running') is False
        assert can_transition('completed', 'failed') is False
        assert can_transition('superseded', 'running') is False


class TestAuthService:
    """Tests for authentication service."""

    def test_password_hashing(self):
        password = 'test_password_123'
        hashed = hash_password(password)
        assert verify_password(password, hashed) is True
        assert verify_password('wrong', hashed) is False

    def test_jwt_token(self):
        user_id = uuid.uuid4()
        token = create_access_token(user_id, 'admin')
        payload = decode_token(token)
        assert str(payload['sub']) == str(user_id)
        assert payload['role'] == 'admin'
        assert payload['type'] == 'access'


class TestNodeRegistry:
    """Tests for node registry."""

    @pytest.fixture(autouse=True)
    def ensure_registered(self):
        """Register file_source if not already present."""
        if get_node('file_source') is None:
            register_file_source()

    def test_register_and_get(self):
        node = get_node('file_source')
        assert node is not None
        assert node.display_name == 'File Source'
        assert node.category == 'source'

    def test_categories(self):
        source_nodes = get_nodes_by_category('source')
        assert any(n.node_type == 'file_source' for n in source_nodes)

    def test_node_serialization(self):
        node = get_node('file_source')
        d = node.to_dict()
        assert d['type'] == 'file_source'
        assert 'inputs' in d
        assert 'outputs' in d
        assert 'config_schema' in d


class TestNodeConfigValidation:
    """Tests for node configuration validation."""

    @pytest.mark.asyncio
    async def test_file_parser_no_required(self):
        node = FileParserNode()
        errors = await node.validate_config({})
        assert errors == []

    @pytest.mark.asyncio
    async def test_file_parser_valid_config(self):
        node = FileParserNode()
        errors = await node.validate_config({'parser': 'auto'})
        assert errors == []


class TestRetryHandler:
    """Tests for exponential backoff retry."""

    def test_exponential_backoff(self):
        config = RetryConfig(base_delay_seconds=2.0, max_delay_seconds=60.0, jitter=False)
        assert calculate_delay(1, config) == 4.0
        assert calculate_delay(2, config) == 8.0
        assert calculate_delay(3, config) == 16.0
        assert calculate_delay(5, config) == 60.0  # capped

    def test_jitter_range(self):
        config = RetryConfig(base_delay_seconds=2.0, jitter=True)
        for _ in range(50):
            delay = calculate_delay(1, config)
            assert 2.0 <= delay <= 6.0

    @pytest.mark.asyncio
    async def test_retry_async_success(self):
        call_count = 0

        async def flaky_func():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise ValueError('Not yet!')
            return 'success'

        result = await retry_async(
            flaky_func,
            retry_config=RetryConfig(
                max_retries=5,
                base_delay_seconds=0.01,
                max_delay_seconds=0.1,
                jitter=False,
            ),
        )
        assert result == 'success'
        assert call_count == 3


class TestGraphService:
    """Tests for graph service utilities."""

    def test_checksum_deterministic(self):
        data1 = {'nodes': {'a': {}}, 'edges': []}
        data2 = {'nodes': {'a': {}}, 'edges': []}
        assert compute_checksum(data1) == compute_checksum(data2)

    def test_checksum_different(self):
        data1 = {'nodes': {'a': {}}, 'edges': []}
        data2 = {'nodes': {'b': {}}, 'edges': []}
        assert compute_checksum(data1) != compute_checksum(data2)
