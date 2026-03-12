"""Integration tests for auth/SSL across deployers.

Requires Docker services from docker-compose.auth.yml:
    docker compose -f docker-compose.yml -f docker-compose.auth.yml up kafka-sasl schema-registry-auth -d

Skip automatically when services are not available.
"""

from __future__ import annotations

import time

import pytest
import requests

# ---------------------------------------------------------------------------
# Fixtures: detect running auth services
# ---------------------------------------------------------------------------

KAFKA_SASL_BOOTSTRAP = "localhost:9094"
KAFKA_SASL_USER = "testuser"
KAFKA_SASL_PASS = "testpass"

SR_AUTH_URL = "http://localhost:8085"
SR_AUTH_USER = "sruser"
SR_AUTH_PASS = "srpass"

FLINK_AUTH_URL = "http://localhost:8086"
FLINK_AUTH_USER = "flinkuser"
FLINK_AUTH_PASS = "flinkpass"

CONNECT_AUTH_URL = "http://localhost:8087"
CONNECT_AUTH_USER = "connectuser"
CONNECT_AUTH_PASS = "connectpass"


def _kafka_sasl_available() -> bool:
    """Check if SASL Kafka is reachable."""
    try:
        from confluent_kafka.admin import AdminClient

        admin = AdminClient({
            "bootstrap.servers": KAFKA_SASL_BOOTSTRAP,
            "security.protocol": "SASL_PLAINTEXT",
            "sasl.mechanism": "PLAIN",
            "sasl.username": KAFKA_SASL_USER,
            "sasl.password": KAFKA_SASL_PASS,
            "socket.timeout.ms": 5000,
        })
        md = admin.list_topics(timeout=5)
        return md is not None
    except Exception:
        return False


def _sr_auth_available() -> bool:
    """Check if auth-protected Schema Registry is reachable."""
    try:
        resp = requests.get(
            f"{SR_AUTH_URL}/subjects",
            auth=(SR_AUTH_USER, SR_AUTH_PASS),
            timeout=3,
        )
        return resp.status_code == 200
    except Exception:
        return False


def _flink_auth_available() -> bool:
    """Check if auth-protected Flink REST is reachable."""
    try:
        resp = requests.get(
            f"{FLINK_AUTH_URL}/config",
            auth=(FLINK_AUTH_USER, FLINK_AUTH_PASS),
            timeout=3,
        )
        return resp.status_code == 200
    except Exception:
        return False


def _connect_auth_available() -> bool:
    """Check if auth-protected Connect REST is reachable."""
    try:
        resp = requests.get(
            f"{CONNECT_AUTH_URL}/connectors",
            auth=(CONNECT_AUTH_USER, CONNECT_AUTH_PASS),
            timeout=3,
        )
        return resp.status_code == 200
    except Exception:
        return False


kafka_sasl_available = pytest.mark.skipif(
    not _kafka_sasl_available(),
    reason="Kafka SASL not available (run: docker compose -f docker-compose.yml -f docker-compose.auth.yml up kafka -d)",
)

sr_auth_available = pytest.mark.skipif(
    not _sr_auth_available(),
    reason="Schema Registry auth not available (run: docker compose -f docker-compose.yml -f docker-compose.auth.yml up schema-registry-auth -d)",
)

flink_auth_available = pytest.mark.skipif(
    not _flink_auth_available(),
    reason="Flink auth not available (run: docker compose -f docker-compose.yml -f docker-compose.auth.yml up flink-auth -d)",
)

connect_auth_available = pytest.mark.skipif(
    not _connect_auth_available(),
    reason="Connect auth not available (run: docker compose -f docker-compose.yml -f docker-compose.auth.yml up connect-auth -d)",
)


# ---------------------------------------------------------------------------
# Kafka SASL_PLAINTEXT integration tests
# ---------------------------------------------------------------------------

@kafka_sasl_available
class TestKafkaSASLIntegration:
    """Verify KafkaDeployer works with SASL_PLAINTEXT authentication."""

    def _make_deployer(self):
        from streamt.deployer.kafka import KafkaDeployer

        return KafkaDeployer(
            KAFKA_SASL_BOOTSTRAP,
            **{
                "security.protocol": "SASL_PLAINTEXT",
                "sasl.mechanism": "PLAIN",
                "sasl.username": KAFKA_SASL_USER,
                "sasl.password": KAFKA_SASL_PASS,
            },
        )

    def test_list_topics(self):
        """KafkaDeployer can list topics over SASL."""
        deployer = self._make_deployer()
        topics = deployer.list_topics()
        assert isinstance(topics, list)

    def test_create_and_get_topic(self):
        """KafkaDeployer can create and inspect a topic over SASL."""
        from streamt.compiler.manifest import TopicArtifact

        deployer = self._make_deployer()
        topic_name = f"auth_test_{int(time.time())}"

        artifact = TopicArtifact(
            name=topic_name,
            partitions=1,
            replication_factor=1,
            config={},
        )

        try:
            deployer.create_topic(artifact)
            # Give Kafka a moment to propagate
            time.sleep(1)
            state = deployer.get_topic_state(topic_name)
            assert state.exists
            assert state.partitions == 1
        finally:
            try:
                deployer.delete_topic(topic_name)
            except Exception:
                pass

    def test_wrong_password_fails(self):
        """KafkaDeployer with wrong SASL password cannot list topics."""
        from streamt.deployer.kafka import KafkaDeployer

        deployer = KafkaDeployer(
            KAFKA_SASL_BOOTSTRAP,
            **{
                "security.protocol": "SASL_PLAINTEXT",
                "sasl.mechanism": "PLAIN",
                "sasl.username": KAFKA_SASL_USER,
                "sasl.password": "wrong-password",
            },
        )
        # confluent_kafka will raise or return error metadata
        with pytest.raises(Exception):
            deployer.list_topics()

    def test_no_auth_fails(self):
        """KafkaDeployer without auth cannot connect to SASL broker."""
        from streamt.deployer.kafka import KafkaDeployer

        deployer = KafkaDeployer(KAFKA_SASL_BOOTSTRAP)
        with pytest.raises(Exception):
            deployer.list_topics()

    def test_yaml_to_deployer_roundtrip(self):
        """Full path: YAML config → KafkaConfig → to_confluent_config → KafkaDeployer → list_topics."""
        from streamt.core.models import KafkaConfig
        from streamt.deployer.kafka import KafkaDeployer

        cfg = KafkaConfig(
            bootstrap_servers=KAFKA_SASL_BOOTSTRAP,
            security_protocol="SASL_PLAINTEXT",
            sasl_mechanism="PLAIN",
            sasl_username=KAFKA_SASL_USER,
            sasl_password=KAFKA_SASL_PASS,
        )

        confluent = cfg.to_confluent_config()
        bootstrap = confluent.pop("bootstrap.servers")
        deployer = KafkaDeployer(bootstrap, **confluent)

        topics = deployer.list_topics()
        assert isinstance(topics, list)


# ---------------------------------------------------------------------------
# Schema Registry basic auth integration tests
# ---------------------------------------------------------------------------

@sr_auth_available
class TestSchemaRegistryAuthIntegration:
    """Verify SchemaRegistryDeployer works with basic auth."""

    def _make_deployer(self):
        from streamt.deployer.schema_registry import SchemaRegistryDeployer

        return SchemaRegistryDeployer(
            SR_AUTH_URL,
            username=SR_AUTH_USER,
            password=SR_AUTH_PASS,
        )

    def test_check_connection(self):
        """SchemaRegistryDeployer can connect with basic auth."""
        deployer = self._make_deployer()
        assert deployer.check_connection() is True

    def test_list_subjects(self):
        """SchemaRegistryDeployer can list subjects with basic auth."""
        deployer = self._make_deployer()
        subjects = deployer.list_subjects()
        assert isinstance(subjects, list)

    def test_register_and_get_schema(self):
        """SchemaRegistryDeployer can register and retrieve a schema with basic auth."""
        deployer = self._make_deployer()
        subject = f"auth-test-{int(time.time())}-value"
        schema = {
            "type": "record",
            "name": "AuthTest",
            "fields": [{"name": "id", "type": "string"}],
        }

        try:
            schema_id = deployer.register_schema(subject, schema, "AVRO")
            assert isinstance(schema_id, int)

            state = deployer.get_schema_state(subject)
            assert state.exists
            assert state.version == 1
        finally:
            try:
                deployer.delete_subject(subject)
            except Exception:
                pass

    def test_wrong_password_fails(self):
        """SchemaRegistryDeployer with wrong password gets 401."""
        from streamt.deployer.schema_registry import SchemaRegistryDeployer

        deployer = SchemaRegistryDeployer(
            SR_AUTH_URL,
            username=SR_AUTH_USER,
            password="wrong-password",
        )
        assert deployer.check_connection() is False

    def test_no_auth_fails(self):
        """SchemaRegistryDeployer without auth gets 401."""
        from streamt.deployer.schema_registry import SchemaRegistryDeployer

        deployer = SchemaRegistryDeployer(SR_AUTH_URL)
        assert deployer.check_connection() is False

    def test_yaml_to_deployer_roundtrip(self):
        """Full path: YAML config → SchemaRegistryConfig → deployer → list_subjects."""
        from streamt.core.models import SchemaRegistryConfig
        from streamt.deployer.schema_registry import SchemaRegistryDeployer

        cfg = SchemaRegistryConfig(
            url=SR_AUTH_URL,
            username=SR_AUTH_USER,
            password=SR_AUTH_PASS,
        )

        deployer = SchemaRegistryDeployer(
            cfg.url,
            username=cfg.username,
            password=cfg.password,
            ssl_ca_location=cfg.ssl_ca_location,
            ssl_certificate_location=cfg.ssl_certificate_location,
            ssl_key_location=cfg.ssl_key_location,
        )

        subjects = deployer.list_subjects()
        assert isinstance(subjects, list)


# ---------------------------------------------------------------------------
# Flink REST basic auth integration tests
# ---------------------------------------------------------------------------

@flink_auth_available
class TestFlinkAuthIntegration:
    """Verify FlinkDeployer works with basic auth via nginx proxy."""

    def _make_deployer(self):
        from streamt.deployer.flink import FlinkDeployer

        return FlinkDeployer(
            rest_url=FLINK_AUTH_URL,
            username=FLINK_AUTH_USER,
            password=FLINK_AUTH_PASS,
        )

    def test_check_connection(self):
        """FlinkDeployer can connect with basic auth."""
        deployer = self._make_deployer()
        try:
            assert deployer.check_connection() is True
        finally:
            deployer.close()

    def test_list_jobs(self):
        """FlinkDeployer can list jobs with basic auth."""
        deployer = self._make_deployer()
        try:
            jobs = deployer.list_jobs()
            assert isinstance(jobs, list)
        finally:
            deployer.close()

    def test_wrong_password_fails(self):
        """FlinkDeployer with wrong password gets 401."""
        from streamt.deployer.flink import FlinkDeployer

        deployer = FlinkDeployer(
            rest_url=FLINK_AUTH_URL,
            username=FLINK_AUTH_USER,
            password="wrong-password",
        )
        try:
            assert deployer.check_connection() is False
        finally:
            deployer.close()

    def test_no_auth_fails(self):
        """FlinkDeployer without auth gets 401."""
        from streamt.deployer.flink import FlinkDeployer

        deployer = FlinkDeployer(rest_url=FLINK_AUTH_URL)
        try:
            assert deployer.check_connection() is False
        finally:
            deployer.close()

    def test_yaml_to_deployer_roundtrip(self):
        """Full path: YAML config → FlinkClusterConfig → deployer → list_jobs."""
        from streamt.core.models import FlinkClusterConfig
        from streamt.deployer.flink import FlinkDeployer

        cfg = FlinkClusterConfig(
            rest_url=FLINK_AUTH_URL,
            username=FLINK_AUTH_USER,
            password=FLINK_AUTH_PASS,
        )

        deployer = FlinkDeployer(
            rest_url=cfg.rest_url,
            username=cfg.username,
            password=cfg.password.get_secret_value() if cfg.password else None,
        )
        try:
            jobs = deployer.list_jobs()
            assert isinstance(jobs, list)
        finally:
            deployer.close()


# ---------------------------------------------------------------------------
# Connect REST basic auth integration tests
# ---------------------------------------------------------------------------

@connect_auth_available
class TestConnectAuthIntegration:
    """Verify ConnectDeployer works with basic auth via nginx proxy."""

    def _make_deployer(self):
        from streamt.deployer.connect import ConnectDeployer

        return ConnectDeployer(
            CONNECT_AUTH_URL,
            username=CONNECT_AUTH_USER,
            password=CONNECT_AUTH_PASS,
        )

    def test_check_connection(self):
        """ConnectDeployer can connect with basic auth."""
        deployer = self._make_deployer()
        try:
            assert deployer.check_connection() is True
        finally:
            deployer.close()

    def test_list_connectors(self):
        """ConnectDeployer can list connectors with basic auth."""
        deployer = self._make_deployer()
        try:
            connectors = deployer.list_connectors()
            assert isinstance(connectors, list)
        finally:
            deployer.close()

    def test_wrong_password_fails(self):
        """ConnectDeployer with wrong password gets 401."""
        from streamt.deployer.connect import ConnectDeployer

        deployer = ConnectDeployer(
            CONNECT_AUTH_URL,
            username=CONNECT_AUTH_USER,
            password="wrong-password",
        )
        try:
            assert deployer.check_connection() is False
        finally:
            deployer.close()

    def test_no_auth_fails(self):
        """ConnectDeployer without auth gets 401."""
        from streamt.deployer.connect import ConnectDeployer

        deployer = ConnectDeployer(CONNECT_AUTH_URL)
        try:
            assert deployer.check_connection() is False
        finally:
            deployer.close()

    def test_yaml_to_deployer_roundtrip(self):
        """Full path: YAML config → ConnectClusterConfig → deployer → list_connectors."""
        from streamt.core.models import ConnectClusterConfig
        from streamt.deployer.connect import ConnectDeployer

        cfg = ConnectClusterConfig(
            rest_url=CONNECT_AUTH_URL,
            username=CONNECT_AUTH_USER,
            password=CONNECT_AUTH_PASS,
        )

        deployer = ConnectDeployer(
            cfg.rest_url,
            username=cfg.username,
            password=cfg.password.get_secret_value() if cfg.password else None,
        )
        try:
            connectors = deployer.list_connectors()
            assert isinstance(connectors, list)
        finally:
            deployer.close()
