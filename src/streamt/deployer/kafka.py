"""Kafka deployer for topic management."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Optional

from confluent_kafka.admin import AdminClient, ConfigResource, NewPartitions, NewTopic, ResourceType

from streamt.compiler.manifest import TopicArtifact

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
                    "message": f"Cannot reduce partitions from {current.partitions} to {artifact.partitions}",
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

        # Handle config changes
        config_changes = {
            k.replace("config.", ""): v["to"] for k, v in changes.items() if k.startswith("config.")
        }

        if config_changes:
            config_resource = ConfigResource(
                ResourceType.TOPIC,
                artifact.name,
                set_config=config_changes,
            )
            futures = self.admin.alter_configs([config_resource])
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
