"""Pytest fixtures for integration tests with Docker infrastructure.

This module provides fixtures for:
- Docker Compose lifecycle management
- Kafka client connections
- Schema Registry connections
- Flink SQL Gateway connections
- Connect REST API connections
- Test data producers and consumers

Test subset execution:
    pytest -m kafka          # Run only Kafka tests
    pytest -m flink          # Run only Flink tests
    pytest -m connect        # Run only Connect tests
    pytest -m schema_registry # Run only Schema Registry tests
    pytest -m slow           # Run slow tests (>30s)
    pytest -m "kafka and not slow"  # Combine markers
"""

from typing import Generator

import pytest
from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient

# Import helpers from extracted modules
from tests.integration.helpers import (
    INFRA_CONFIG,
    ConnectHelper,
    DockerComposeManager,
    FlinkHelper,
    GatewayHelper,
    KafkaHelper,
    SchemaRegistryHelper,
    get_docker_manager,
    is_docker_running,
    is_kafka_healthy,
    poll_until,
    poll_until_messages,
)

# Re-export for backward compatibility with tests that import directly from conftest
__all__ = [
    "INFRA_CONFIG",
    "ConnectHelper",
    "DockerComposeManager",
    "FlinkHelper",
    "GatewayHelper",
    "KafkaHelper",
    "SchemaRegistryHelper",
    "poll_until",
    "poll_until_messages",
]


# =============================================================================
# Pytest Fixtures
# =============================================================================


@pytest.fixture(scope="session")
def docker_services() -> Generator[DockerComposeManager, None, None]:
    """
    Session-scoped fixture that starts Docker Compose services.

    This fixture checks if services are already running. If not, it starts them.
    Services are stopped at the end of the test session.
    """
    if not is_docker_running():
        pytest.skip("Docker is not running")

    manager = get_docker_manager()

    # Check if services are already running
    if is_kafka_healthy(INFRA_CONFIG.kafka_bootstrap_servers):
        print("\nDocker services already running, reusing...")
        manager.started = True
    else:
        manager.start()

    yield manager

    # Optionally stop services (comment out to keep running for debugging)
    # manager.stop()


@pytest.fixture(scope="session")
def kafka_admin(docker_services: DockerComposeManager) -> AdminClient:
    """Session-scoped Kafka admin client."""
    return AdminClient({"bootstrap.servers": INFRA_CONFIG.kafka_bootstrap_servers})


@pytest.fixture(scope="session")
def kafka_producer(docker_services: DockerComposeManager) -> Producer:
    """Session-scoped Kafka producer."""
    return Producer(
        {
            "bootstrap.servers": INFRA_CONFIG.kafka_bootstrap_servers,
            "client.id": "streamt-test-producer",
        }
    )


@pytest.fixture(scope="function")
def kafka_consumer_factory(docker_services: DockerComposeManager):
    """Factory fixture for creating Kafka consumers."""
    consumers = []

    def create_consumer(
        group_id: str,
        topics: list[str],
        auto_offset_reset: str = "earliest",
    ) -> Consumer:
        consumer = Consumer(
            {
                "bootstrap.servers": INFRA_CONFIG.kafka_bootstrap_servers,
                "group.id": group_id,
                "auto.offset.reset": auto_offset_reset,
                "enable.auto.commit": False,
            }
        )
        consumer.subscribe(topics)
        consumers.append(consumer)
        return consumer

    yield create_consumer

    # Cleanup consumers
    for consumer in consumers:
        consumer.close()


@pytest.fixture(scope="function")
def kafka_helper(kafka_admin: AdminClient, kafka_producer: Producer) -> KafkaHelper:
    """Fixture providing Kafka helper for tests."""
    return KafkaHelper(kafka_admin, kafka_producer)


@pytest.fixture(scope="function")
def connect_helper(docker_services: DockerComposeManager) -> ConnectHelper:
    """Fixture providing Connect helper for tests."""
    return ConnectHelper(INFRA_CONFIG.connect_url)


@pytest.fixture(scope="function")
def flink_helper(docker_services: DockerComposeManager) -> Generator[FlinkHelper, None, None]:
    """Fixture providing Flink helper for tests.

    Cancels all running jobs before each test to ensure slot availability
    and prevent interference between tests.
    """
    helper = FlinkHelper(INFRA_CONFIG.flink_rest_url, INFRA_CONFIG.flink_sql_gateway_url)
    # Cancel all running jobs before this test to free up slots
    helper.cancel_all_running_jobs()
    yield helper
    # Cleanup session and cancel any jobs started by this test
    helper.close_sql_session()
    helper.cancel_all_running_jobs()


@pytest.fixture(scope="function")
def schema_registry_helper(docker_services: DockerComposeManager) -> SchemaRegistryHelper:
    """Fixture providing Schema Registry helper for tests."""
    return SchemaRegistryHelper(INFRA_CONFIG.schema_registry_url)


@pytest.fixture(scope="function")
def gateway_helper(docker_services: DockerComposeManager) -> Generator[GatewayHelper, None, None]:
    """Fixture providing Conduktor Gateway helper for tests.

    Cleans up test resources after each test.
    """
    gateway_config = INFRA_CONFIG.gateway
    helper = GatewayHelper(
        admin_url=gateway_config.admin_url,
        username=gateway_config.username,
        password=gateway_config.password,
    )
    yield helper
    # Cleanup test resources
    helper.cleanup_test_resources()


# =============================================================================
# Markers for test categorization
# =============================================================================


def pytest_configure(config):
    """Configure pytest markers."""
    config.addinivalue_line(
        "markers",
        "integration: mark test as integration test requiring Docker",
    )
    config.addinivalue_line(
        "markers",
        "slow: mark test as slow (may take > 30 seconds)",
    )
    config.addinivalue_line(
        "markers",
        "kafka: mark test as requiring Kafka",
    )
    config.addinivalue_line(
        "markers",
        "flink: mark test as requiring Flink",
    )
    config.addinivalue_line(
        "markers",
        "connect: mark test as requiring Kafka Connect",
    )
    config.addinivalue_line(
        "markers",
        "schema_registry: mark test as requiring Schema Registry",
    )
    config.addinivalue_line(
        "markers",
        "gateway: mark test as requiring Conduktor Gateway",
    )
