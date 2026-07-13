"""
Execution state machine for run and node status management.
"""
from enum import Enum


class ExecutionState(str, Enum):
    """States a run can be in."""
    PENDING = "pending"
    RUNNING = "running"
    PAUSED = "paused"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    SUPERSEDED = "superseded"


class NodeExecutionState(str, Enum):
    """States a run node can be in."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"
    RETRYING = "retrying"


# Valid state transitions for runs: current_state -> set of allowed next states
VALID_TRANSITIONS: dict[ExecutionState, set[ExecutionState]] = {
    ExecutionState.PENDING: {ExecutionState.RUNNING, ExecutionState.CANCELLED},
    ExecutionState.RUNNING: {
        ExecutionState.PAUSED,
        ExecutionState.COMPLETED,
        ExecutionState.FAILED,
        ExecutionState.CANCELLED,
    },
    ExecutionState.PAUSED: {ExecutionState.RUNNING, ExecutionState.CANCELLED},
    ExecutionState.COMPLETED: set(),  # Terminal state
    ExecutionState.FAILED: {
        ExecutionState.RUNNING,
        ExecutionState.SUPERSEDED,
    },  # Retry or abandon for a new full run
    ExecutionState.CANCELLED: set(),  # Terminal state
    ExecutionState.SUPERSEDED: set(),  # Terminal state
}

# Valid state transitions for nodes: current_state -> set of allowed next states
VALID_NODE_TRANSITIONS: dict[NodeExecutionState, set[NodeExecutionState]] = {
    NodeExecutionState.PENDING: {NodeExecutionState.RUNNING, NodeExecutionState.SKIPPED},
    NodeExecutionState.RUNNING: {
        NodeExecutionState.COMPLETED,
        NodeExecutionState.FAILED,
        NodeExecutionState.RETRYING,
        NodeExecutionState.SKIPPED,
    },
    NodeExecutionState.RETRYING: {
        NodeExecutionState.RUNNING,
        NodeExecutionState.FAILED,
        NodeExecutionState.SKIPPED,
    },
    NodeExecutionState.COMPLETED: set(),  # Terminal state
    NodeExecutionState.FAILED: {NodeExecutionState.RUNNING},  # Can retry
    NodeExecutionState.SKIPPED: set(),  # Terminal state
}


def can_transition(current: str, target: str) -> bool:
    """Check if a run state transition is valid."""
    try:
        current_state = ExecutionState(current)
        target_state = ExecutionState(target)
    except ValueError:
        return False

    allowed = VALID_TRANSITIONS.get(current_state, set())
    return target_state in allowed


def can_node_transition(current: str, target: str) -> bool:
    """Check if a node state transition is valid."""
    try:
        current_state = NodeExecutionState(current)
        target_state = NodeExecutionState(target)
    except ValueError:
        return False

    allowed = VALID_NODE_TRANSITIONS.get(current_state, set())
    return target_state in allowed


def get_terminal_states() -> list[str]:
    """Get run states that cannot be transitioned out of."""
    return [
        ExecutionState.COMPLETED.value,
        ExecutionState.CANCELLED.value,
        ExecutionState.SUPERSEDED.value,
    ]
