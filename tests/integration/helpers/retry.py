"""Retry decorator for transient errors."""

import time
from functools import wraps
from typing import Callable, TypeVar

import requests

T = TypeVar("T")


def retry_on_transient_error(
    max_retries: int = 3,
    delay: float = 1.0,
    backoff: float = 2.0,
    retryable_exceptions: tuple = (
        requests.exceptions.ConnectionError,
        requests.exceptions.Timeout,
    ),
    retryable_status_codes: tuple = (502, 503, 504),
) -> Callable:
    """Decorator that retries a function on transient HTTP errors.

    Args:
        max_retries: Maximum number of retry attempts
        delay: Initial delay between retries in seconds
        backoff: Multiplier for delay after each retry
        retryable_exceptions: Exception types to retry on
        retryable_status_codes: HTTP status codes to retry on

    Returns:
        Decorated function with retry logic
    """

    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @wraps(func)
        def wrapper(*args, **kwargs) -> T:
            last_exception = None
            current_delay = delay

            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except retryable_exceptions as e:
                    last_exception = e
                    if attempt < max_retries:
                        time.sleep(current_delay)
                        current_delay *= backoff
                except requests.exceptions.HTTPError as e:
                    if e.response is not None and e.response.status_code in retryable_status_codes:
                        last_exception = e
                        if attempt < max_retries:
                            time.sleep(current_delay)
                            current_delay *= backoff
                    else:
                        raise

            # All retries exhausted
            raise last_exception  # type: ignore

        return wrapper

    return decorator
