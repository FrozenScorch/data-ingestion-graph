"""
Exponential backoff retry handler for node execution.
"""
import asyncio
import random
from dataclasses import dataclass


@dataclass
class RetryConfig:
    """Configuration for retry behavior."""
    max_retries: int = 3
    base_delay_seconds: float = 2.0
    max_delay_seconds: float = 60.0
    jitter: bool = True
    retryable_errors: tuple[type[Exception], ...] = (Exception,)


def calculate_delay(attempt: int, config: RetryConfig) -> float:
    """
    Calculate exponential backoff delay with optional jitter.

    Args:
        attempt: Current attempt number (1-indexed)
        config: Retry configuration

    Returns:
        Delay in seconds
    """
    # Exponential backoff: 2^attempt * base_delay
    delay = min(
        (2 ** attempt) * config.base_delay_seconds,
        config.max_delay_seconds,
    )

    if config.jitter:
        # Add random jitter: 50% to 150% of calculated delay
        delay *= random.uniform(0.5, 1.5)

    return delay


async def retry_async(
    func,
    *args,
    retry_config: RetryConfig | None = None,
    **kwargs,
):
    """
    Execute an async function with exponential backoff retry.

    Args:
        func: Async function to execute
        *args: Positional arguments for func
        retry_config: Retry configuration (uses defaults if None)
        **kwargs: Keyword arguments for func

    Returns:
        Result of func

    Raises:
        Last exception if all retries exhausted
    """
    if retry_config is None:
        retry_config = RetryConfig()

    last_exception = None

    for attempt in range(1, retry_config.max_retries + 1):
        try:
            return await func(*args, **kwargs)
        except retry_config.retryable_errors as e:
            last_exception = e
            if attempt < retry_config.max_retries:
                delay = calculate_delay(attempt, retry_config)
                import logging
                logging.getLogger(__name__).warning(
                    f"Attempt {attempt}/{retry_config.max_retries} failed: {e}. "
                    f"Retrying in {delay:.1f}s"
                )
                await asyncio.sleep(delay)

    raise last_exception  # type: ignore
