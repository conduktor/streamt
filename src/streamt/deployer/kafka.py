"""Kafka deployer for topic management."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Optional

from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.admin import (
    AdminClient,
    AlterConfigOpType,
    ConfigEntry,
    ConfigResource,
    ConfigSource,
    NewPartitions,
    NewTopic,
    ResourceType,
)

from streamt.compiler.manifest import TopicArtifact
from streamt.core import errors

logger = logging.getLogger(__name__)

# Default timeouts (in seconds)
DEFAULT_TIMEOUT = 10


@dataclass
class TopicState:
    """Current state of a topic."""

    name: str
    exists: bool
    partitions: Optional[int] = None
    replication_factor: Optional[int] = None
    config: dict = None

    def __post_init__(self) -> None:
        if self.config is None:
            self.config = {}


@dataclass
class PartitionLag:
    """Lag information for a single partition."""

    partition: int
    current_offset: int
    end_offset: int
    lag: int


@dataclass
class ConsumerGroupLag:
    """Consumer group lag information for a topic."""

    group_id: str
    topic: str
    total_lag: int
    partitions: list[PartitionLag] = field(default_factory=list)
    state: Optional[str] = None  # e.g., "Stable", "Empty", "Dead"


@dataclass
class TopicChange:
    """A change to apply to a topic."""

    topic: str
    action: str  # create, update, delete
    current: Optional[TopicState] = None
    desired: Optional[TopicArtifact] = None
    changes: dict = None

    def __post_init__(self) -> None:
        if self.changes is None:
            self.changes = {}


class KafkaDeployer:
    """Deployer for Kafka topics.

    Supports context manager protocol for proper resource cleanup:

        with KafkaDeployer(bootstrap_servers) as deployer:
            deployer.list_topics()
    """

    def __init__(self, bootstrap_servers: str, **kafka_config: dict) -> None:
        """Initialize Kafka deployer."""
        config = {"bootstrap.servers": bootstrap_servers}
        config.update(kafka_config)
        self.admin = AdminClient(config)
        self._closed = False

    def __enter__(self) -> "KafkaDeployer":
        """Enter context manager."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Exit context manager, cleaning up resources."""
        self.close()

    def close(self) -> None:
        """Close the deployer and clean up resources.

        Note: confluent_kafka AdminClient doesn't have an explicit close method,
        but we mark the deployer as closed to prevent further operations.
        """
        self._closed = True
        # AdminClient doesn't have close(), but setting to None helps GC
        self.admin = None

    def get_topic_state(self, topic_name: str) -> TopicState:
        """Get current state of a topic."""
        metadata = self.admin.list_topics(timeout=DEFAULT_TIMEOUT)

        if topic_name not in metadata.topics:
            return TopicState(name=topic_name, exists=False)

        topic_metadata = metadata.topics[topic_name]
        partitions = len(topic_metadata.partitions)

        # Get replication factor from first partition
        rf = None
        if topic_metadata.partitions:
            first_partition = list(topic_metadata.partitions.values())[0]
            rf = len(first_partition.replicas)

        # Get topic config
        config_resource = ConfigResource(ResourceType.TOPIC, topic_name)
        configs = self.admin.describe_configs([config_resource])
        topic_config = {}

        for resource, future in configs.items():
            try:
                config_entries = future.result(timeout=DEFAULT_TIMEOUT)
                topic_config = {
                    entry.name: entry.value
                    for entry in config_entries.values()
                    if not entry.is_default
                }
            except Exception as e:
                logger.warning(f"Failed to get config for topic '{topic_name}': {e}")

        return TopicState(
            name=topic_name,
            exists=True,
            partitions=partitions,
            replication_factor=rf,
            config=topic_config,
        )

    def plan_topic(self, artifact: TopicArtifact) -> TopicChange:
        """Plan changes for a topic."""
        current = self.get_topic_state(artifact.name)

        if not current.exists:
            return TopicChange(
                topic=artifact.name,
                action="create",
                current=current,
                desired=artifact,
            )

        # Check for changes
        changes = {}

        # Partitions can only be increased
        if current.partitions != artifact.partitions:
            if artifact.partitions > current.partitions:
                changes["partitions"] = {
                    "from": current.partitions,
                    "to": artifact.partitions,
                }
            elif artifact.partitions < current.partitions:
                # Cannot reduce partitions
                changes["partitions_error"] = {
                    "message": errors.cannot_reduce_partitions(
                        artifact.name, current.partitions, artifact.partitions
                    ),
                }

        # Check config changes
        for key, value in artifact.config.items():
            current_value = current.config.get(key)
            if str(current_value) != str(value):
                changes[f"config.{key}"] = {
                    "from": current_value,
                    "to": value,
                }

        if changes:
            return TopicChange(
                topic=artifact.name,
                action="update",
                current=current,
                desired=artifact,
                changes=changes,
            )

        return TopicChange(
            topic=artifact.name,
            action="none",
            current=current,
            desired=artifact,
        )

    def create_topic(self, artifact: TopicArtifact) -> None:
        """Create a new topic."""
        new_topic = NewTopic(
            artifact.name,
            num_partitions=artifact.partitions,
            replication_factor=artifact.replication_factor,
            config=artifact.config,
        )

        futures = self.admin.create_topics([new_topic])

        for topic, future in futures.items():
            try:
                future.result(timeout=DEFAULT_TIMEOUT)
            except Exception as e:
                raise RuntimeError(f"Failed to create topic '{topic}': {e}")

    def update_topic(self, artifact: TopicArtifact, changes: dict) -> None:
        """Update an existing topic."""
        # Handle partition increase
        if "partitions" in changes:
            new_partitions = changes["partitions"]["to"]
            futures = self.admin.create_partitions(
                [NewPartitions(artifact.name, new_partitions)]
            )
            for topic, future in futures.items():
                try:
                    future.result(timeout=DEFAULT_TIMEOUT)
                except Exception as e:
                    raise RuntimeError(f"Failed to increase partitions for '{topic}': {e}")

        # Handle config changes using incremental_alter_configs (alter_configs is deprecated)
        config_changes = {
            k.replace("config.", ""): v["to"] for k, v in changes.items() if k.startswith("config.")
        }

        if config_changes:
            incremental_configs = [
                ConfigEntry(
                    name=config_name,
                    value=str(config_value),
                    source=ConfigSource.DYNAMIC_TOPIC_CONFIG,
                    incremental_operation=AlterConfigOpType.SET,
                )
                for config_name, config_value in config_changes.items()
            ]
            config_resource = ConfigResource(
                ResourceType.TOPIC,
                artifact.name,
                incremental_configs=incremental_configs,
            )
            futures = self.admin.incremental_alter_configs([config_resource])
            for resource, future in futures.items():
                try:
                    future.result(timeout=DEFAULT_TIMEOUT)
                except Exception as e:
                    raise RuntimeError(f"Failed to update config for '{artifact.name}': {e}")

    def delete_topic(self, topic_name: str) -> None:
        """Delete a topic."""
        futures = self.admin.delete_topics([topic_name])

        for topic, future in futures.items():
            try:
                future.result(timeout=DEFAULT_TIMEOUT)
            except Exception as e:
                raise RuntimeError(f"Failed to delete topic '{topic}': {e}")

    def apply_topic(self, artifact: TopicArtifact) -> str:
        """Apply a topic artifact. Returns action taken."""
        change = self.plan_topic(artifact)

        if change.action == "create":
            self.create_topic(artifact)
            return "created"
        elif change.action == "update":
            if "partitions_error" in change.changes:
                raise RuntimeError(change.changes["partitions_error"]["message"])
            self.update_topic(artifact, change.changes)
            return "updated"
        else:
            return "unchanged"

    def apply(self, artifact: TopicArtifact) -> str:
        """Alias for apply_topic."""
        return self.apply_topic(artifact)

    def list_topics(self) -> list[str]:
        """List all topics in the cluster."""
        metadata = self.admin.list_topics(timeout=DEFAULT_TIMEOUT)
        return [
            topic
            for topic in metadata.topics.keys()
            if not topic.startswith("_")  # Exclude internal topics
        ]

    def compute_diff(self, artifact: TopicArtifact) -> dict:
        """Compute diff between current and desired state."""
        change = self.plan_topic(artifact)
        return change.changes

    def get_consumer_groups(self) -> list[str]:
        """List all consumer groups in the cluster."""
        future = self.admin.list_consumer_groups()
        result = future.result(timeout=DEFAULT_TIMEOUT)
        return [g.group_id for g in result.valid]

    def get_consumer_group_lag(
        self, group_id: str, topic: str
    ) -> Optional[ConsumerGroupLag]:
        """Get consumer group lag for a specific topic.

        Returns None if the group doesn't exist or has no committed offsets.
        """
        # Get topic partitions
        metadata = self.admin.list_topics(timeout=DEFAULT_TIMEOUT)
        if topic not in metadata.topics:
            return None

        topic_metadata = metadata.topics[topic]
        partition_count = len(topic_metadata.partitions)

        if partition_count == 0:
            return None

        # Create a temporary consumer to get committed offsets
        # Note: This is more reliable than using AdminClient for offset fetching
        consumer_config = {
            "bootstrap.servers": list(self.admin.list_topics().brokers.values())[0][0]
            if self.admin.list_topics().brokers
            else "localhost:9092",
            "group.id": group_id,
            "enable.auto.commit": False,
        }

        try:
            # Use AdminClient to list consumer group offsets (Kafka 2.4+)
            from confluent_kafka.admin import ConsumerGroupTopicPartitions

            topic_partitions = [
                TopicPartition(topic, p) for p in range(partition_count)
            ]
            cgtp = ConsumerGroupTopicPartitions(group_id, topic_partitions)

            # Get committed offsets
            futures = self.admin.list_consumer_group_offsets([cgtp])
            result = list(futures.values())[0].result(timeout=DEFAULT_TIMEOUT)

            # Get end offsets (high watermarks)
            consumer = Consumer(consumer_config)
            try:
                end_offsets = {}
                for tp in topic_partitions:
                    low, high = consumer.get_watermark_offsets(tp, timeout=DEFAULT_TIMEOUT)
                    end_offsets[tp.partition] = high
            finally:
                consumer.close()

            # Calculate lag per partition
            partition_lags = []
            total_lag = 0

            for tp in result.topic_partitions:
                if tp.topic != topic:
                    continue
                current_offset = tp.offset if tp.offset >= 0 else 0
                end_offset = end_offsets.get(tp.partition, 0)
                lag = max(0, end_offset - current_offset)
                total_lag += lag

                partition_lags.append(
                    PartitionLag(
                        partition=tp.partition,
                        current_offset=current_offset,
                        end_offset=end_offset,
                        lag=lag,
                    )
                )

            return ConsumerGroupLag(
                group_id=group_id,
                topic=topic,
                total_lag=total_lag,
                partitions=partition_lags,
            )

        except Exception as e:
            logger.warning(f"Failed to get consumer group lag for {group_id}: {e}")
            return None

    def get_topic_message_count(self, topic: str) -> int:
        """Get approximate message count for a topic (sum of end offsets)."""
        metadata = self.admin.list_topics(timeout=DEFAULT_TIMEOUT)
        if topic not in metadata.topics:
            return 0

        topic_metadata = metadata.topics[topic]
        partition_count = len(topic_metadata.partitions)

        if partition_count == 0:
            return 0

        # Create a temporary consumer to get watermarks
        brokers = list(self.admin.list_topics().brokers.values())
        if not brokers:
            return 0

        broker_host, broker_port = brokers[0]
        consumer_config = {
            "bootstrap.servers": f"{broker_host}:{broker_port}",
            "group.id": "_streamt_internal_count",
            "enable.auto.commit": False,
        }

        try:
            consumer = Consumer(consumer_config)
            total = 0
            for partition in range(partition_count):
                tp = TopicPartition(topic, partition)
                low, high = consumer.get_watermark_offsets(tp, timeout=DEFAULT_TIMEOUT)
                total += high - low
            consumer.close()
            return total
        except Exception as e:
            logger.warning(f"Failed to get message count for {topic}: {e}")
            return 0
