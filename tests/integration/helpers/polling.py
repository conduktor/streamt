"""Polling utilities for reliable integration testing."""

import json
import time

# Import KafkaHelper type for type hints (avoid circular import at runtime)
from typing import TYPE_CHECKING, Callable, Optional, TypeVar

from confluent_kafka import Consumer

from .config import INFRA_CONFIG

if TYPE_CHECKING:
    from .kafka import KafkaHelper

T = TypeVar("T")


def poll_until(
    condition_fn: Callable[[], bool],
    timeout: float = 30.0,
    interval: float = 1.0,
    description: str = "condition",
) -> bool:
    """
    Poll until a condition is met or timeout is reached.

    Args:
        condition_fn: Function that returns True when condition is met
        timeout: Maximum time to wait in seconds
        interval: Time between polls in seconds
        description: Description for error messages

    Returns:
        True if condition was met, False on timeout
    """
    start = time.time()
    while time.time() - start < timeout:
        try:
            if condition_fn():
                return True
        except Exception:
            pass
        time.sleep(interval)
    return False


def poll_for_result(
    check_fn: Callable[[], tuple[bool, Optional[T]]],
    timeout: float = 30.0,
    interval: float = 1.0,
) -> Optional[T]:
    """
    Poll until a result is available or timeout is reached.

    This is the Extract Method refactoring for the Long Method smell in FlinkHelper.
    Multiple methods had the same polling pattern - this consolidates them.

    Args:
        check_fn: Function that returns (done, result). If done is True, polling stops.
        timeout: Maximum time to wait in seconds
        interval: Time between polls in seconds

    Returns:
        The result from check_fn, or None on timeout
    """
    start = time.time()
    while time.time() - start < timeout:
        try:
            done, result = check_fn()
            if done:
                return result
        except Exception:
            pass
        time.sleep(interval)
    return None


def poll_until_messages(
    kafka_helper: "KafkaHelper",
    topic: str,
    min_messages: int,
    group_id: str,
    timeout: float = 60.0,
    poll_interval: float = 1.0,
) -> list[dict]:
    """
    Poll until at least min_messages are available in a topic.

    Uses a single consumer instance to avoid consumer group explosion.

    Args:
        kafka_helper: KafkaHelper instance (unused but kept for API compatibility)
        topic: Topic to consume from
        min_messages: Minimum number of messages required
        group_id: Consumer group ID (used as-is, no timestamp suffix)
        timeout: Maximum time to wait
        poll_interval: Time between poll attempts in seconds

    Returns:
        List of consumed messages

    Raises:
        TimeoutError: If min_messages not received within timeout
    """
    # Create a single consumer for the entire polling operation
    consumer = Consumer(
        {
            "bootstrap.servers": INFRA_CONFIG.kafka_bootstrap_servers,
            "group.id": group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([topic])

    all_messages = []
    decode_errors = []
    start = time.time()

    try:
        while time.time() - start < timeout:
            msg = consumer.poll(timeout=poll_interval)

            if msg is None:
                # No message available, check if we have enough
                if len(all_messages) >= min_messages:
                    return all_messages[:min_messages]
                continue

            if msg.error():
                continue

            try:
                value = json.loads(msg.value().decode("utf-8"))
                all_messages.append(value)

                # Check if we have enough messages
                if len(all_messages) >= min_messages:
                    return all_messages[:min_messages]

            except (json.JSONDecodeError, UnicodeDecodeError) as e:
                raw_value = msg.value()[:100] if msg.value() else b"<empty>"
                decode_errors.append(f"Offset {msg.offset()}: {e} (raw: {raw_value})")

    finally:
        consumer.close()

    # Return what we have if any
    if len(all_messages) > 0:
        return all_messages

    # No messages - check if there were decode errors
    if decode_errors:
        raise ValueError(
            f"Failed to decode messages from topic '{topic}'. " f"Errors: {decode_errors[:3]}"
        )

    raise TimeoutError(
        f"Expected at least {min_messages} messages in topic '{topic}', "
        f"but received {len(all_messages)} within {timeout}s"
    )
