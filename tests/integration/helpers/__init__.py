"""Integration test helpers.

This package provides helper classes and utilities for integration testing
with Kafka, Flink, Connect, Schema Registry, and Conduktor Gateway.
"""

from .config import INFRA_CONFIG, PROJECT_ROOT, FlinkConfig, GatewayConfig, InfrastructureConfig
from .connect import ConnectHelper
from .docker import (
    DockerComposeManager,
    get_docker_manager,
    is_docker_running,
    is_kafka_healthy,
    is_service_healthy,
    wait_for_service,
)
from .factories import FlinkJobFactory, TopicFactory, unique_name
from .flink import FlinkHelper
from .gateway import GatewayHelper
from .kafka import KafkaHelper
from .polling import poll_for_result, poll_until, poll_until_messages
from .retry import retry_on_transient_error
from .schema_registry import SchemaRegistryHelper

__all__ = [
    # Config
    "FlinkConfig",
    "GatewayConfig",
    "INFRA_CONFIG",
    "InfrastructureConfig",
    "PROJECT_ROOT",
    # Docker
    "DockerComposeManager",
    "get_docker_manager",
    "is_docker_running",
    "is_kafka_healthy",
    "is_service_healthy",
    "wait_for_service",
    # Factories
    "FlinkJobFactory",
    "TopicFactory",
    "unique_name",
    # Helpers
    "ConnectHelper",
    "FlinkHelper",
    "GatewayHelper",
    "KafkaHelper",
    "SchemaRegistryHelper",
    # Polling
    "poll_for_result",
    "poll_until",
    "poll_until_messages",
    # Retry
    "retry_on_transient_error",
]
