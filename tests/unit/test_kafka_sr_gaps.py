"""Gap tests for KafkaDeployer and SchemaRegistryDeployer: config change
detection, string comparison issues, compatibility order, and validation.

Groups covered:
  7.  Topic config removed keys detection
  8.  Schema compatibility order
  9.  Config string comparison issues
  14. Empty connection string validation
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from streamt.compiler.manifest import SchemaArtifact, TopicArtifact
from streamt.deployer.kafka import KafkaDeployer, TopicState
from streamt.deployer.schema_registry import (
    SchemaChange,
    SchemaRegistryDeployer,
)

# ===========================================================================
# GROUP 7: Topic config removed keys detection
# ===========================================================================


class TestTopicConfigRemovedKeys:
    """plan_topic() should detect when config keys present on the current
    topic are absent from the desired artifact."""

    @pytest.fixture
    def deployer(self):
        d = KafkaDeployer.__new__(KafkaDeployer)
        d.admin = MagicMock()
        d._config = {}
        d._closed = False
        return d

    def test_removed_config_key_detected(self, deployer):
        """Current has {retention.ms, cleanup.policy}. Desired has only
        {retention.ms}. Should detect cleanup.policy removal."""
        with patch.object(deployer, "get_topic_state") as mock_state:
            mock_state.return_value = TopicState(
                name="events", exists=True, partitions=3, replication_factor=1,
                config={"retention.ms": "86400000", "cleanup.policy": "delete"},
            )
            artifact = TopicArtifact(
                name="events", partitions=3, replication_factor=1,
                config={"retention.ms": "86400000"},
            )
            change = deployer.plan_topic(artifact)
            assert change.action == "update"
            assert any("cleanup.policy" in k for k in change.changes)

    def test_added_config_key_detected(self, deployer):
        """Adding a new key that wasn't there should be detected (works today)."""
        with patch.object(deployer, "get_topic_state") as mock_state:
            mock_state.return_value = TopicState(
                name="events", exists=True, partitions=3, replication_factor=1,
                config={},
            )
            artifact = TopicArtifact(
                name="events", partitions=3, replication_factor=1,
                config={"retention.ms": "86400000"},
            )
            change = deployer.plan_topic(artifact)
            assert change.action == "update"
            assert "config.retention.ms" in change.changes

    def test_no_change_when_configs_match(self, deployer):
        """Same config on both sides -> no change."""
        with patch.object(deployer, "get_topic_state") as mock_state:
            mock_state.return_value = TopicState(
                name="events", exists=True, partitions=3, replication_factor=1,
                config={"retention.ms": "86400000"},
            )
            artifact = TopicArtifact(
                name="events", partitions=3, replication_factor=1,
                config={"retention.ms": "86400000"},
            )
            change = deployer.plan_topic(artifact)
            assert change.action == "none"

    def test_partition_increase_detected(self, deployer):
        """Partition increase should be detected as update."""
        with patch.object(deployer, "get_topic_state") as mock_state:
            mock_state.return_value = TopicState(
                name="events", exists=True, partitions=3, replication_factor=1,
                config={},
            )
            artifact = TopicArtifact(
                name="events", partitions=6, replication_factor=1, config={},
            )
            change = deployer.plan_topic(artifact)
            assert change.action == "update"
            assert "partitions" in change.changes

    def test_partition_decrease_detected_as_error(self, deployer):
        """Partition decrease should produce partitions_error."""
        with patch.object(deployer, "get_topic_state") as mock_state:
            mock_state.return_value = TopicState(
                name="events", exists=True, partitions=6, replication_factor=1,
                config={},
            )
            artifact = TopicArtifact(
                name="events", partitions=3, replication_factor=1, config={},
            )
            change = deployer.plan_topic(artifact)
            assert change.action == "update"
            assert "partitions_error" in change.changes

    def test_new_topic_returns_create(self, deployer):
        """Non-existent topic -> create action."""
        with patch.object(deployer, "get_topic_state") as mock_state:
            mock_state.return_value = TopicState(name="new_topic", exists=False)
            artifact = TopicArtifact(
                name="new_topic", partitions=3, replication_factor=1, config={},
            )
            change = deployer.plan_topic(artifact)
            assert change.action == "create"

    def test_strict_topic_observation_propagates_config_read_failure(self, deployer):
        topic_metadata = MagicMock()
        topic_metadata.partitions = {0: MagicMock(replicas=[1, 2, 3])}
        metadata = MagicMock(topics={"events": topic_metadata})
        deployer.admin.list_topics.return_value = metadata
        future = MagicMock()
        future.result.side_effect = RuntimeError("describe config failed")
        deployer.admin.describe_configs.return_value = {MagicMock(): future}

        with pytest.raises(RuntimeError, match="Failed to get config"):
            deployer.get_topic_state("events", strict_config=True)


# ===========================================================================
# GROUP 8: Schema compatibility order
# ===========================================================================


class TestSchemaCompatibilityOrder:
    """set_compatibility should be called BEFORE register_schema when
    lowering compatibility (e.g. BACKWARD -> NONE)."""

    def test_schema_registered_before_compatibility_change(self):
        """Subject has BACKWARD. Artifact wants NONE + schema update.
        register_schema() must precede set_compatibility() so the schema is
        validated under the current (stricter) rules before they are relaxed."""
        deployer = SchemaRegistryDeployer.__new__(SchemaRegistryDeployer)
        deployer.url = "http://localhost:8081"
        deployer.auth = None
        deployer.headers = {}
        deployer._closed = False
        deployer._http_session = MagicMock()

        call_order = []

        with patch.object(deployer, "plan_schema") as mock_plan, \
             patch.object(deployer, "register_schema") as mock_reg, \
             patch.object(deployer, "set_compatibility") as mock_compat:
            mock_plan.return_value = SchemaChange(
                subject="e-value", action="update",
                changes={
                    "schema": {"from_version": 1, "to_version": 2, "compatible": True},
                    "compatibility": {"from": "BACKWARD", "to": "NONE"},
                },
            )
            mock_reg.side_effect = lambda *a, **k: call_order.append("register")
            mock_compat.side_effect = lambda *a, **k: call_order.append("set_compat")

            artifact = SchemaArtifact(
                subject="e-value",
                schema={"type": "record", "name": "E", "fields": []},
                compatibility="NONE",
            )
            deployer.apply_schema(artifact)

            # Schema registered first, then compatibility relaxed
            assert call_order.index("register") < call_order.index("set_compat")


# ===========================================================================
# GROUP 9: Config string comparison issues
# ===========================================================================


class TestConfigStringComparison:
    """KafkaDeployer uses str() for config comparison, causing edge cases."""

    @pytest.fixture
    def deployer(self):
        d = KafkaDeployer.__new__(KafkaDeployer)
        d.admin = MagicMock()
        d._config = {}
        d._closed = False
        return d

    def test_matching_string_values_no_change(self, deployer):
        """Both sides '1000' -> no change (works today)."""
        with patch.object(deployer, "get_topic_state") as mock_state:
            mock_state.return_value = TopicState(
                name="e", exists=True, partitions=3, replication_factor=1,
                config={"retention.ms": "1000"},
            )
            artifact = TopicArtifact(
                name="e", partitions=3, replication_factor=1,
                config={"retention.ms": "1000"},
            )
            assert deployer.plan_topic(artifact).action == "none"

    def test_none_string_vs_none_value_false_negative(self, deployer):
        """Current absent (None), desired literal 'None'.
        Now correctly detected as a change."""
        with patch.object(deployer, "get_topic_state") as mock_state:
            mock_state.return_value = TopicState(
                name="e", exists=True, partitions=3, replication_factor=1,
                config={},
            )
            artifact = TopicArtifact(
                name="e", partitions=3, replication_factor=1,
                config={"cleanup.policy": "None"},
            )
            change = deployer.plan_topic(artifact)
            assert change.action == "update"

    def test_boolean_case_mismatch_false_positive(self, deployer):
        """Kafka returns 'true', artifact has True -> should match (case-insensitive)."""

        with patch.object(deployer, "get_topic_state") as mock_state:
            mock_state.return_value = TopicState(
                name="e", exists=True, partitions=3, replication_factor=1,
                config={"unclean.leader.election.enable": "true"},
            )
            artifact = TopicArtifact(
                name="e", partitions=3, replication_factor=1,
                config={"unclean.leader.election.enable": True},
            )
            assert deployer.plan_topic(artifact).action == "none"


# ===========================================================================
# GROUP 14: Empty connection string validation
# ===========================================================================


class TestEmptyConnectionStringValidation:
    """Runtime config models should reject empty connection strings."""

    def test_kafka_empty_bootstrap_servers(self):
        from pydantic import ValidationError

        from streamt.core.runtime import KafkaConfig
        with pytest.raises(ValidationError):
            KafkaConfig(bootstrap_servers="")

    def test_kafka_whitespace_bootstrap_servers(self):
        from pydantic import ValidationError

        from streamt.core.runtime import KafkaConfig
        with pytest.raises(ValidationError):
            KafkaConfig(bootstrap_servers="   ")

    def test_schema_registry_empty_url(self):
        from pydantic import ValidationError

        from streamt.core.runtime import SchemaRegistryConfig
        with pytest.raises(ValidationError):
            SchemaRegistryConfig(url="")

    def test_kafka_valid_bootstrap_servers_accepted(self):
        from streamt.core.runtime import KafkaConfig
        cfg = KafkaConfig(bootstrap_servers="localhost:9092")
        assert cfg.bootstrap_servers == "localhost:9092"

    def test_schema_registry_valid_url_accepted(self):
        from streamt.core.runtime import SchemaRegistryConfig
        cfg = SchemaRegistryConfig(url="http://localhost:8081")
        assert cfg.url == "http://localhost:8081"
