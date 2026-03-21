"""
Execution state machine for run status management.
"""
from enum import Enum
from typing import Optional


class ExecutionState(str, Enum):
    """States a run can be in."""
    PENDING = "pending"
    RUNNING = "running"
    PAUSED = "paused"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


# Valid state transitions: current_state -> set of allowed next states
VALID_TRANSITIONS: dict[ExecutionState, set[ExecutionState]] = {
    ExecutionState.PENDING: {ExecutionState.RUNNING, ExecutionState.CANCELLED},
    ExecutionState.RUNNING: {ExecutionState.PAUSED, ExecutionState.COMPLETED, ExecutionState.FAILED, ExecutionState.CANCELLED},
    ExecutionState.PAUSED: {ExecutionState.RUNNING, ExecutionState.CANCELLED},
    ExecutionState.COMPLETED: set(),  # Terminal state
    ExecutionState.FAILED: {ExecutionState.RUNNING},  # Can retry/replay
    ExecutionState.CANCELLED: set(),  # Terminal state
}


def can_transition(current: str, target: str) -> bool:
    """Check if a state transition is valid."""
    try:
        current_state = ExecutionState(current)
        target_state = ExecutionState(target)
    except ValueError:
        return False

    allowed = VALID_TRANSITIONS.get(current_state, set())
    return target_state in allowed


def get_terminal_states() -> list[str]:
    """Get states that cannot be transitioned out of."""
    return [
        ExecutionState.COMPLETED.value,
        ExecutionState.CANCELLED.value,
    ]
