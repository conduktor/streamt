"""Kafka helper for integration tests."""

import json
import time
from typing import Optional

from confluent_kafka import Consumer
from confluent_kafka.admin import AdminClient, NewTopic

from .config import INFRA_CONFIG


class KafkaHelper:
    """Helper class for Kafka operations in tests."""

    def __init__(self, admin: AdminClient, producer):
        self.admin = admin
        self.producer = producer

    def create_topic(
        self,
        name: str,
        partitions: int = 1,
        replication_factor: int = 1,
        config: Optional[dict] = None,
    ) -> None:
        """Create a topic and wait for it to be ready."""
        topic = NewTopic(
            name,
            num_partitions=partitions,
            replication_factor=replication_factor,
            config=config or {},
        )
        futures = self.admin.create_topics([topic])
        for topic_name, future in futures.items():
            future.result(timeout=30)

        # Wait for topic to be fully created
        self._wait_for_topic(name)

    def delete_topic(self, name: str) -> None:
        """Delete a topic."""
        futures = self.admin.delete_topics([name])
        for topic_name, future in futures.items():
            try:
                future.result(timeout=30)
            except Exception:
                pass  # Topic might not exist

    def topic_exists(self, name: str) -> bool:
        """Check if a topic exists."""
        metadata = self.admin.list_topics(timeout=10)
        return name in metadata.topics

    def get_topic_partitions(self, name: str) -> int:
        """Get the number of partitions for a topic."""
        metadata = self.admin.list_topics(timeout=10)
        if name not in metadata.topics:
            raise ValueError(f"Topic {name} does not exist")
        return len(metadata.topics[name].partitions)

    def produce_messages(
        self,
        topic: str,
        messages: list[dict],
        key_field: Optional[str] = None,
    ) -> None:
        """Produce messages to a topic."""
        for msg in messages:
            key = msg.get(key_field) if key_field else None
            if key:
                key = str(key).encode("utf-8")
            value = json.dumps(msg).encode("utf-8")
            self.producer.produce(topic, value=value, key=key)
        self.producer.flush(timeout=30)

    def consume_messages(
        self,
        topic: str,
        group_id: str,
        max_messages: int = 100,
        timeout: float = 30.0,
    ) -> list[dict]:
        """Consume messages from a topic.

        Raises:
            ValueError: If a message cannot be decoded as valid JSON
        """
        consumer = Consumer(
            {
                "bootstrap.servers": INFRA_CONFIG.kafka_bootstrap_servers,
                "group.id": group_id,
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
            }
        )
        consumer.subscribe([topic])

        messages = []
        decode_errors = []
        start = time.time()

        try:
            while len(messages) < max_messages and (time.time() - start) < timeout:
                msg = consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                if msg.error():
                    continue
                try:
                    value = json.loads(msg.value().decode("utf-8"))
                    messages.append(value)
                except (json.JSONDecodeError, UnicodeDecodeError) as e:
                    # Track decode errors - don't silently swallow them
                    raw_value = msg.value()[:100] if msg.value() else b"<empty>"
                    decode_errors.append(f"Offset {msg.offset()}: {e} (raw: {raw_value})")
        finally:
            consumer.close()

        # If we got no valid messages but had decode errors, raise to surface the issue
        if len(messages) == 0 and len(decode_errors) > 0:
            raise ValueError(
                f"Failed to decode any messages from topic '{topic}'. "
                f"Decode errors ({len(decode_errors)}): {decode_errors[:3]}"
            )

        return messages

    def _wait_for_topic(self, name: str, timeout: int = 30) -> None:
        """Wait for a topic to be available."""
        start = time.time()
        while time.time() - start < timeout:
            if self.topic_exists(name):
                return
            time.sleep(0.5)
        raise TimeoutError(f"Topic {name} did not become available")
