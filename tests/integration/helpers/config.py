"""Infrastructure configuration for integration tests."""

import os
from dataclasses import dataclass
from pathlib import Path

# Project root directory
PROJECT_ROOT = Path(__file__).parent.parent.parent.parent


@dataclass
class FlinkConfig:
    """Flink connection configuration (Introduce Parameter Object pattern).

    Groups related Flink URLs that are always passed together.
    """
    rest_url: str
    sql_gateway_url: str


@dataclass
class GatewayConfig:
    """Gateway connection configuration."""
    admin_url: str
    proxy_bootstrap: str
    username: str = "admin"
    password: str = "conduktor"


@dataclass
class InfrastructureConfig:
    """Configuration for test infrastructure.

    All settings can be overridden via environment variables:
        KAFKA_BOOTSTRAP_SERVERS - Kafka broker for external/host access (default: localhost:9092)
        KAFKA_INTERNAL_SERVERS - Kafka broker for internal Docker access (default: kafka:29092)
        SCHEMA_REGISTRY_URL - Schema Registry URL (default: http://localhost:8081)
        CONNECT_URL - Kafka Connect REST URL (default: http://localhost:8083)
        FLINK_REST_URL - Flink REST API URL (default: http://localhost:8082)
        FLINK_SQL_GATEWAY_URL - Flink SQL Gateway URL (default: http://localhost:8084)
        GATEWAY_ADMIN_URL - Conduktor Gateway Admin API URL (default: http://localhost:8888)
        GATEWAY_PROXY_BOOTSTRAP - Gateway proxy bootstrap servers (default: localhost:6969)

    Note: KAFKA_INTERNAL_SERVERS is used for Flink SQL tables since Flink runs inside Docker
    and needs to connect to Kafka via the internal Docker network.
    """

    kafka_bootstrap_servers: str = ""
    kafka_internal_servers: str = ""  # For Flink/Docker internal access
    schema_registry_url: str = ""
    connect_url: str = ""
    flink_rest_url: str = ""
    flink_sql_gateway_url: str = ""
    gateway_admin_url: str = ""
    gateway_proxy_bootstrap: str = ""

    def __post_init__(self):
        """Load configuration from environment variables with defaults."""
        self.kafka_bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        self.kafka_internal_servers = os.getenv("KAFKA_INTERNAL_SERVERS", "kafka:29092")
        self.schema_registry_url = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")
        self.connect_url = os.getenv("CONNECT_URL", "http://localhost:8083")
        self.flink_rest_url = os.getenv("FLINK_REST_URL", "http://localhost:8082")
        self.flink_sql_gateway_url = os.getenv("FLINK_SQL_GATEWAY_URL", "http://localhost:8084")
        self.gateway_admin_url = os.getenv("GATEWAY_ADMIN_URL", "http://localhost:8888")
        self.gateway_proxy_bootstrap = os.getenv("GATEWAY_PROXY_BOOTSTRAP", "localhost:6969")

    @property
    def flink(self) -> FlinkConfig:
        """Get Flink configuration as a parameter object."""
        return FlinkConfig(
            rest_url=self.flink_rest_url,
            sql_gateway_url=self.flink_sql_gateway_url,
        )

    @property
    def gateway(self) -> GatewayConfig:
        """Get Gateway configuration as a parameter object."""
        return GatewayConfig(
            admin_url=self.gateway_admin_url,
            proxy_bootstrap=self.gateway_proxy_bootstrap,
        )


# Global config singleton
INFRA_CONFIG = InfrastructureConfig()
