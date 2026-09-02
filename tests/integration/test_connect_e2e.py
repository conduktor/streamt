"""End-to-end tests for Kafka Connect connector management with Docker infrastructure.

These tests verify that the ConnectDeployer correctly:
- Creates connectors
- Updates connector configurations
- Deletes connectors
- Monitors connector status
- Handles connector lifecycle
"""

import time
import uuid

import pytest
import requests

from streamt.compiler.manifest import ConnectorArtifact
from streamt.deployer.connect import ConnectDeployer

from .conftest import INFRA_CONFIG, ConnectHelper, KafkaHelper


@pytest.mark.integration
@pytest.mark.connect
class TestConnectDeployerConnection:
    """Test Connect cluster connectivity."""

    def test_check_connection(
        self,
        docker_services,
    ):
        """Test checking Connect cluster connection."""
        deployer = ConnectDeployer(INFRA_CONFIG.connect_url)
        assert deployer.check_connection() is True

    def test_check_connection_invalid(self):
        """Test connection check with invalid URL."""
        deployer = ConnectDeployer("http://localhost:99999")
        assert deployer.check_connection() is False

    def test_list_connectors_empty(
        self,
        docker_services,
    ):
        """Test listing connectors on fresh cluster."""
        deployer = ConnectDeployer(INFRA_CONFIG.connect_url)
        connectors = deployer.list_connectors()
        # May or may not be empty, but should be a list
        assert isinstance(connectors, list)


@pytest.mark.integration
@pytest.mark.connect
class TestConnectorCreation:
    """Test connector creation via ConnectDeployer."""

    def test_create_datagen_source_connector(
        self,
        docker_services,
        kafka_helper: KafkaHelper,
    ):
        """Test creating a Datagen source connector."""
        deployer = ConnectDeployer(INFRA_CONFIG.connect_url)
        topic_name = f"datagen_test_{uuid.uuid4().hex[:8]}"
        connector_name = f"datagen_source_{uuid.uuid4().hex[:8]}"

        try:
            # Create target topic
            kafka_helper.create_topic(topic_name, partitions=1)

            artifact = ConnectorArtifact(
                name=connector_name,
                connector_class="io.confluent.kafka.connect.datagen.DatagenConnector",
                topics=[topic_name],
                config={
                    "kafka.topic": topic_name,
                    "quickstart": "users",
                    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
                    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
                    "value.converter.schemas.enable": "false",
                    "max.interval": "1000",
                    "iterations": "10",
                    "tasks.max": "1",
                },
            )

            # Plan should show create action
            plan = deployer.plan_connector(artifact)
            assert plan.action == "create"
            assert not plan.current.exists

            # Apply connector
            result = deployer.apply_connector(artifact)
            assert result == "created"

            # Wait for connector to be registered
            time.sleep(5)

            # Verify connector exists
            state = deployer.get_connector_state(connector_name)
            assert state.exists
            assert state.config is not None

            # Wait for connector to start
            time.sleep(5)

            # Check status
            state = deployer.get_connector_state(connector_name)
            assert state.status in ["RUNNING", "PAUSED"]

        finally:
            try:
                deployer.delete_connector(connector_name)
            except Exception:
                pass
            kafka_helper.delete_topic(topic_name)

    def test_strict_managed_observation_parses_real_connector_document(
        self,
        docker_services,
        kafka_helper: KafkaHelper,
    ):
        """The identity-bound observer accepts the real Connect resource shape."""
        deployer = ConnectDeployer(
            INFRA_CONFIG.connect_url,
            cluster_alias="integration",
        )
        topic_name = f"strict_observation_{uuid.uuid4().hex[:8]}"
        connector_name = f"strict_observation_{uuid.uuid4().hex[:8]}"
        artifact = ConnectorArtifact(
            name=connector_name,
            connector_class="io.confluent.kafka.connect.datagen.DatagenConnector",
            topics=[topic_name],
            config={
                "kafka.topic": topic_name,
                "quickstart": "users",
                "key.converter": "org.apache.kafka.connect.storage.StringConverter",
                "value.converter": "org.apache.kafka.connect.json.JsonConverter",
                "value.converter.schemas.enable": "false",
                "max.interval": "1000",
                "iterations": "1",
                "tasks.max": "1",
            },
        )

        try:
            kafka_helper.create_topic(topic_name, partitions=1)
            assert deployer.apply_connector(artifact) == "created"
            observation = deployer.observe_managed_connector(connector_name)

            assert observation.exists is True
            assert observation.name == connector_name
            assert observation.binding == deployer.require_cluster_binding()
            assert observation.config_dict()["name"] == connector_name
            assert observation.config_dict()["connector.class"] == (
                artifact.connector_class
            )
        finally:
            try:
                deployer.delete_connector(connector_name)
            except Exception:
                pass
            deployer.close()
            kafka_helper.delete_topic(topic_name)


@pytest.mark.integration
@pytest.mark.connect
class TestConnectorUpdates:
    """Test connector configuration updates."""

    def test_update_connector_config(
        self,
        docker_services,
        kafka_helper: KafkaHelper,
    ):
        """Test updating connector configuration."""
        deployer = ConnectDeployer(INFRA_CONFIG.connect_url)
        topic_name = f"update_test_{uuid.uuid4().hex[:8]}"
        connector_name = f"update_connector_{uuid.uuid4().hex[:8]}"

        try:
            kafka_helper.create_topic(topic_name, partitions=1)

            # Create initial connector (datagen)
            initial_artifact = ConnectorArtifact(
                name=connector_name,
                connector_class="io.confluent.kafka.connect.datagen.DatagenConnector",
                topics=[topic_name],
                config={
                    "kafka.topic": topic_name,
                    "quickstart": "users",
                    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
                    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
                    "value.converter.schemas.enable": "false",
                    "max.interval": "1000",
                    "iterations": "100",
                    "tasks.max": "1",
                },
            )

            deployer.apply_connector(initial_artifact)
            time.sleep(5)

            # Update connector with new config (different interval)
            updated_artifact = ConnectorArtifact(
                name=connector_name,
                connector_class="io.confluent.kafka.connect.datagen.DatagenConnector",
                topics=[topic_name],
                config={
                    "kafka.topic": topic_name,
                    "quickstart": "users",
                    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
                    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
                    "value.converter.schemas.enable": "false",
                    "max.interval": "500",  # Changed
                    "iterations": "100",
                    "tasks.max": "1",
                },
            )

            plan = deployer.plan_connector(updated_artifact)
            assert plan.action == "update"
            assert "max.interval" in plan.changes

            result = deployer.apply_connector(updated_artifact)
            assert result == "updated"

            # Wait for update to propagate
            time.sleep(3)

            # Verify update
            state = deployer.get_connector_state(connector_name)
            assert state.config["max.interval"] == "500"

        finally:
            try:
                deployer.delete_connector(connector_name)
            except Exception:
                pass
            kafka_helper.delete_topic(topic_name)


@pytest.mark.integration
@pytest.mark.connect
class TestConnectorDeletion:
    """Test connector deletion."""

    def test_delete_connector(
        self,
        docker_services,
        kafka_helper: KafkaHelper,
    ):
        """Test deleting an existing connector."""
        deployer = ConnectDeployer(INFRA_CONFIG.connect_url)
        topic_name = f"delete_test_{uuid.uuid4().hex[:8]}"
        connector_name = f"delete_connector_{uuid.uuid4().hex[:8]}"

        try:
            kafka_helper.create_topic(topic_name, partitions=1)

            artifact = ConnectorArtifact(
                name=connector_name,
                connector_class="io.confluent.kafka.connect.datagen.DatagenConnector",
                topics=[topic_name],
                config={
                    "kafka.topic": topic_name,
                    "quickstart": "users",
                    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
                    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
                    "value.converter.schemas.enable": "false",
                    "max.interval": "1000",
                    "iterations": "10",
                    "tasks.max": "1",
                },
            )

            deployer.apply_connector(artifact)
            time.sleep(5)

            # Verify it exists
            state = deployer.get_connector_state(connector_name)
            assert state.exists

            # Delete it
            deployer.delete_connector(connector_name)
            time.sleep(2)

            # Verify it's gone
            state = deployer.get_connector_state(connector_name)
            assert not state.exists

        finally:
            kafka_helper.delete_topic(topic_name)

    def test_delete_nonexistent_connector(
        self,
        docker_services,
    ):
        """Test deleting a non-existent connector (should not raise)."""
        deployer = ConnectDeployer(INFRA_CONFIG.connect_url)
        connector_name = f"nonexistent_{uuid.uuid4().hex[:8]}"

        # Should not raise (404 is handled gracefully by the deployer)
        try:
            deployer.delete_connector(connector_name)
        except requests.HTTPError as e:
            # 404 is expected during cleanup
            if e.response.status_code != 404:
                raise


@pytest.mark.integration
@pytest.mark.connect
class TestConnectorLifecycle:
    """Test connector lifecycle operations."""

    def test_pause_resume_connector(
        self,
        docker_services,
        kafka_helper: KafkaHelper,
    ):
        """Test pausing and resuming a connector."""
        deployer = ConnectDeployer(INFRA_CONFIG.connect_url)
        topic_name = f"pause_test_{uuid.uuid4().hex[:8]}"
        connector_name = f"pause_connector_{uuid.uuid4().hex[:8]}"

        try:
            kafka_helper.create_topic(topic_name, partitions=1)

            artifact = ConnectorArtifact(
                name=connector_name,
                connector_class="io.confluent.kafka.connect.datagen.DatagenConnector",
                topics=[topic_name],
                config={
                    "kafka.topic": topic_name,
                    "quickstart": "users",
                    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
                    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
                    "value.converter.schemas.enable": "false",
                    "max.interval": "1000",
                    "iterations": "1000",
                    "tasks.max": "1",
                },
            )

            deployer.apply_connector(artifact)
            time.sleep(5)

            # Check initial state
            state = deployer.get_connector_state(connector_name)
            assert state.status == "RUNNING"

            # Pause
            deployer.pause_connector(connector_name)
            time.sleep(3)

            state = deployer.get_connector_state(connector_name)
            assert state.status == "PAUSED"

            # Resume
            deployer.resume_connector(connector_name)
            time.sleep(3)

            state = deployer.get_connector_state(connector_name)
            assert state.status == "RUNNING"

        finally:
            try:
                deployer.delete_connector(connector_name)
            except Exception:
                pass
            kafka_helper.delete_topic(topic_name)

    def test_restart_connector(
        self,
        docker_services,
        kafka_helper: KafkaHelper,
    ):
        """Test restarting a connector."""
        deployer = ConnectDeployer(INFRA_CONFIG.connect_url)
        topic_name = f"restart_test_{uuid.uuid4().hex[:8]}"
        connector_name = f"restart_connector_{uuid.uuid4().hex[:8]}"

        try:
            kafka_helper.create_topic(topic_name, partitions=1)

            artifact = ConnectorArtifact(
                name=connector_name,
                connector_class="io.confluent.kafka.connect.datagen.DatagenConnector",
                topics=[topic_name],
                config={
                    "kafka.topic": topic_name,
                    "quickstart": "users",
                    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
                    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
                    "value.converter.schemas.enable": "false",
                    "max.interval": "1000",
                    "iterations": "1000",
                    "tasks.max": "1",
                },
            )

            deployer.apply_connector(artifact)
            time.sleep(5)

            # Restart
            deployer.restart_connector(connector_name)
            time.sleep(5)

            # Should still be running after restart
            state = deployer.get_connector_state(connector_name)
            assert state.status in ["RUNNING", "RESTARTING"]

        finally:
            try:
                deployer.delete_connector(connector_name)
            except Exception:
                pass
            kafka_helper.delete_topic(topic_name)


@pytest.mark.integration
@pytest.mark.connect
class TestConnectorIdempotency:
    """Test idempotent operations."""

    def test_apply_same_connector_twice(
        self,
        docker_services,
        kafka_helper: KafkaHelper,
    ):
        """Test that applying the same connector twice is idempotent."""
        deployer = ConnectDeployer(INFRA_CONFIG.connect_url)
        topic_name = f"idempotent_test_{uuid.uuid4().hex[:8]}"
        connector_name = f"idempotent_connector_{uuid.uuid4().hex[:8]}"

        try:
            kafka_helper.create_topic(topic_name, partitions=1)

            artifact = ConnectorArtifact(
                name=connector_name,
                connector_class="io.confluent.kafka.connect.datagen.DatagenConnector",
                topics=[topic_name],
                config={
                    "kafka.topic": topic_name,
                    "quickstart": "users",
                    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
                    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
                    "value.converter.schemas.enable": "false",
                    "max.interval": "1000",
                    "iterations": "100",
                    "tasks.max": "1",
                },
            )

            # First apply
            result1 = deployer.apply_connector(artifact)
            assert result1 == "created"
            time.sleep(5)

            # Second apply with same config
            result2 = deployer.apply_connector(artifact)
            assert result2 == "unchanged"

        finally:
            try:
                deployer.delete_connector(connector_name)
            except Exception:
                pass
            kafka_helper.delete_topic(topic_name)


@pytest.mark.integration
@pytest.mark.connect
class TestConnectorHelperClass:
    """Test the ConnectHelper fixture."""

    def test_helper_list_connectors(
        self,
        docker_services,
        connect_helper: ConnectHelper,
    ):
        """Test listing connectors via helper."""
        connectors = connect_helper.list_connectors()
        assert isinstance(connectors, list)

    def test_helper_create_and_wait(
        self,
        docker_services,
        connect_helper: ConnectHelper,
        kafka_helper: KafkaHelper,
    ):
        """Test creating connector and waiting for RUNNING state."""
        topic_name = f"helper_test_{uuid.uuid4().hex[:8]}"
        connector_name = f"helper_connector_{uuid.uuid4().hex[:8]}"

        try:
            kafka_helper.create_topic(topic_name, partitions=1)

            config = {
                "connector.class": "io.confluent.kafka.connect.datagen.DatagenConnector",
                "kafka.topic": topic_name,
                "quickstart": "users",
                "key.converter": "org.apache.kafka.connect.storage.StringConverter",
                "value.converter": "org.apache.kafka.connect.json.JsonConverter",
                "value.converter.schemas.enable": "false",
                "max.interval": "1000",
                "iterations": "100",
                "tasks.max": "1",
            }

            connect_helper.create_connector(connector_name, config)

            # Wait for running
            is_running = connect_helper.wait_for_connector_running(
                connector_name,
                timeout=30,
            )
            assert is_running

            # Get status
            status = connect_helper.get_connector_status(connector_name)
            assert status["connector"]["state"] == "RUNNING"

        finally:
            try:
                connect_helper.delete_connector(connector_name)
            except Exception:
                pass
            kafka_helper.delete_topic(topic_name)


@pytest.mark.integration
@pytest.mark.connect
@pytest.mark.slow
class TestConnectorDataFlow:
    """Test actual data flow through connectors."""

    def test_datagen_produces_messages(
        self,
        docker_services,
        kafka_helper: KafkaHelper,
        connect_helper: ConnectHelper,
    ):
        """Test that datagen connector produces messages to Kafka."""
        topic_name = f"datagen_flow_{uuid.uuid4().hex[:8]}"
        connector_name = f"datagen_flow_connector_{uuid.uuid4().hex[:8]}"

        try:
            kafka_helper.create_topic(topic_name, partitions=1)

            config = {
                "connector.class": "io.confluent.kafka.connect.datagen.DatagenConnector",
                "kafka.topic": topic_name,
                "quickstart": "users",
                "key.converter": "org.apache.kafka.connect.storage.StringConverter",
                "value.converter": "org.apache.kafka.connect.json.JsonConverter",
                "value.converter.schemas.enable": "false",
                "max.interval": "100",
                "iterations": "50",
                "tasks.max": "1",
            }

            connect_helper.create_connector(connector_name, config)
            connect_helper.wait_for_connector_running(connector_name, timeout=30)

            # Wait for some data to be produced
            time.sleep(5)

            # Consume messages
            messages = kafka_helper.consume_messages(
                topic_name,
                group_id=f"datagen_consumer_{uuid.uuid4().hex[:8]}",
                max_messages=10,
                timeout=30.0,
            )

            # Should have received some messages
            assert len(messages) > 0, (
                f"Expected messages from datagen connector in topic '{topic_name}', "
                "but received none. Datagen connector may have failed to produce data."
            )

            # Verify message structure matches datagen users quickstart schema
            # The users quickstart generates: userid, regionid, gender, registertime
            expected_fields = {"userid", "regionid", "gender"}
            for msg in messages:
                assert isinstance(msg, dict), f"Expected dict message, got {type(msg).__name__}"
                # Verify at least some expected fields are present
                msg_fields = set(msg.keys())
                matching_fields = msg_fields & expected_fields
                assert len(matching_fields) >= 2, (
                    f"Datagen 'users' quickstart should produce fields like {expected_fields}, "
                    f"but message only has {msg_fields}. Message: {msg}"
                )
                # Verify field types for fields that exist
                if "userid" in msg:
                    assert msg["userid"] is not None, "userid should not be None"
                if "regionid" in msg:
                    assert msg["regionid"] is not None, "regionid should not be None"
                if "gender" in msg:
                    assert msg["gender"] is not None, "gender should not be None"

        finally:
            try:
                connect_helper.delete_connector(connector_name)
            except Exception:
                pass
            time.sleep(1)
            kafka_helper.delete_topic(topic_name)
