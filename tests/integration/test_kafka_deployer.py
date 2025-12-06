"""Integration tests for Kafka deployer."""

import pytest
import time

from streamt.deployer.kafka import KafkaDeployer
from streamt.compiler.manifest import TopicArtifact


# Skip all tests if Kafka is not available
pytestmark = pytest.mark.skipif(
    not pytest.importorskip("confluent_kafka"),
    reason="confluent_kafka not installed",
)


class TestKafkaDeployer:
    """Integration tests for KafkaDeployer."""

    @pytest.fixture
    def deployer(self):
        """Create a Kafka deployer connected to local Kafka."""
        return KafkaDeployer("localhost:9092")

    @pytest.fixture
    def unique_topic_name(self):
        """Generate a unique topic name for testing."""
        return f"test_topic_{int(time.time() * 1000)}"

    def test_connection(self, deployer):
        """TC-APPLY-001: Should connect to Kafka."""
        # If we can create the deployer and list topics, connection works
        topics = deployer.list_topics()
        assert isinstance(topics, list)

    def test_create_topic(self, deployer, unique_topic_name):
        """TC-APPLY-002: Should create a topic."""
        artifact = TopicArtifact(
            name=unique_topic_name,
            partitions=3,
            replication_factor=1,
            config={"retention.ms": "86400000"},
        )

        # Create topic
        deployer.create_topic(artifact)

        # Wait for topic to be created
        time.sleep(2)

        # Verify topic exists
        state = deployer.get_topic_state(unique_topic_name)
        assert state.exists
        assert state.partitions == 3

        # Cleanup
        deployer.delete_topic(unique_topic_name)

    def test_get_topic_state_nonexistent(self, deployer):
        """Should return exists=False for nonexistent topic."""
        state = deployer.get_topic_state("nonexistent_topic_xyz123")
        assert not state.exists

    def test_topic_diff(self, deployer, unique_topic_name):
        """TC-PLAN-001: Should compute diff between current and desired state."""
        # Create a topic first
        artifact = TopicArtifact(
            name=unique_topic_name,
            partitions=3,
            replication_factor=1,
            config={},
        )
        deployer.create_topic(artifact)
        time.sleep(2)

        # Now request more partitions
        new_artifact = TopicArtifact(
            name=unique_topic_name,
            partitions=6,  # Increase partitions
            replication_factor=1,
            config={"retention.ms": "172800000"},
        )

        diff = deployer.compute_diff(new_artifact)

        assert "partitions" in diff
        assert diff["partitions"]["from"] == 3
        assert diff["partitions"]["to"] == 6

        # Cleanup
        deployer.delete_topic(unique_topic_name)

    def test_update_topic_partitions(self, deployer, unique_topic_name):
        """TC-APPLY-003: Should increase topic partitions."""
        # Create topic with 3 partitions
        artifact = TopicArtifact(
            name=unique_topic_name,
            partitions=3,
            replication_factor=1,
            config={},
        )
        deployer.create_topic(artifact)
        time.sleep(2)

        # Update to 6 partitions
        new_artifact = TopicArtifact(
            name=unique_topic_name,
            partitions=6,
            replication_factor=1,
            config={},
        )

        diff = deployer.compute_diff(new_artifact)
        deployer.update_topic(new_artifact, diff)
        time.sleep(2)

        # Verify
        state = deployer.get_topic_state(unique_topic_name)
        assert state.partitions == 6

        # Cleanup
        deployer.delete_topic(unique_topic_name)

    def test_list_topics(self, deployer, unique_topic_name):
        """Should list all topics."""
        # Create a topic
        artifact = TopicArtifact(
            name=unique_topic_name,
            partitions=1,
            replication_factor=1,
            config={},
        )
        deployer.create_topic(artifact)
        time.sleep(2)

        # List topics
        topics = deployer.list_topics()
        assert unique_topic_name in topics

        # Cleanup
        deployer.delete_topic(unique_topic_name)


class TestKafkaDeployerApply:
    """Test full apply workflow."""

    @pytest.fixture
    def deployer(self):
        """Create a Kafka deployer."""
        return KafkaDeployer("localhost:9092")

    def test_apply_creates_new_topic(self, deployer):
        """TC-APPLY-004: Apply should create new topics."""
        topic_name = f"apply_test_{int(time.time() * 1000)}"

        artifact = TopicArtifact(
            name=topic_name,
            partitions=6,
            replication_factor=1,
            config={"retention.ms": "86400000"},
        )

        # Apply
        deployer.apply(artifact)
        time.sleep(2)

        # Verify
        state = deployer.get_topic_state(topic_name)
        assert state.exists
        assert state.partitions == 6

        # Cleanup
        deployer.delete_topic(topic_name)

    def test_apply_idempotent(self, deployer):
        """TC-APPLY-005: Apply should be idempotent."""
        topic_name = f"idempotent_test_{int(time.time() * 1000)}"

        artifact = TopicArtifact(
            name=topic_name,
            partitions=3,
            replication_factor=1,
            config={},
        )

        # Apply twice
        deployer.apply(artifact)
        time.sleep(2)
        deployer.apply(artifact)  # Should not fail
        time.sleep(1)

        # Verify still correct
        state = deployer.get_topic_state(topic_name)
        assert state.exists
        assert state.partitions == 3

        # Cleanup
        deployer.delete_topic(topic_name)
