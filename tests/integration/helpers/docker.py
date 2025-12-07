"""Docker Compose management for integration tests."""

import subprocess
import time
from pathlib import Path
from typing import Callable, Optional

import requests
from confluent_kafka.admin import AdminClient

from .config import INFRA_CONFIG, PROJECT_ROOT


def is_docker_running() -> bool:
    """Check if Docker daemon is running."""
    try:
        result = subprocess.run(
            ["docker", "info"],
            capture_output=True,
            timeout=10,
        )
        return result.returncode == 0
    except Exception:
        return False


def is_service_healthy(url: str, timeout: int = 5) -> bool:
    """Check if a service is healthy by making an HTTP request."""
    try:
        response = requests.get(url, timeout=timeout)
        return response.status_code == 200
    except Exception:
        return False


def is_kafka_healthy(bootstrap_servers: str, timeout: int = 10) -> bool:
    """Check if Kafka is healthy by listing topics."""
    try:
        admin = AdminClient({"bootstrap.servers": bootstrap_servers})
        metadata = admin.list_topics(timeout=timeout)
        return metadata is not None
    except Exception:
        return False


def wait_for_service(
    check_fn: Callable[[], bool],
    service_name: str,
    max_wait: int = 120,
    interval: int = 2,
) -> bool:
    """Wait for a service to become healthy."""
    start = time.time()
    while time.time() - start < max_wait:
        if check_fn():
            return True
        print(f"Waiting for {service_name}...")
        time.sleep(interval)
    return False


class DockerComposeManager:
    """Manager for Docker Compose lifecycle."""

    def __init__(self, compose_file: Path):
        self.compose_file = compose_file
        self.started = False

    def start(self) -> None:
        """Start Docker Compose services."""
        if self.started:
            return

        print(f"\nStarting Docker Compose from {self.compose_file}...")
        subprocess.run(
            ["docker-compose", "-f", str(self.compose_file), "up", "-d"],
            check=True,
            cwd=self.compose_file.parent,
        )
        self.started = True

        # Wait for services to be healthy
        self._wait_for_services()

    def stop(self) -> None:
        """Stop Docker Compose services."""
        if not self.started:
            return

        print("\nStopping Docker Compose...")
        subprocess.run(
            ["docker-compose", "-f", str(self.compose_file), "down", "-v"],
            check=True,
            cwd=self.compose_file.parent,
        )
        self.started = False

    def _wait_for_services(self) -> None:
        """Wait for all services to be healthy."""
        print("Waiting for Kafka...")
        if not wait_for_service(
            lambda: is_kafka_healthy(INFRA_CONFIG.kafka_bootstrap_servers),
            "Kafka",
            max_wait=120,
        ):
            raise RuntimeError("Kafka did not become healthy")

        print("Waiting for Schema Registry...")
        if not wait_for_service(
            lambda: is_service_healthy(f"{INFRA_CONFIG.schema_registry_url}/subjects"),
            "Schema Registry",
            max_wait=60,
        ):
            raise RuntimeError("Schema Registry did not become healthy")

        print("Waiting for Kafka Connect...")
        if not wait_for_service(
            lambda: is_service_healthy(f"{INFRA_CONFIG.connect_url}/connectors"),
            "Kafka Connect",
            max_wait=90,
        ):
            raise RuntimeError("Kafka Connect did not become healthy")

        print("Waiting for Flink...")
        if not wait_for_service(
            lambda: is_service_healthy(f"{INFRA_CONFIG.flink_rest_url}/overview"),
            "Flink Job Manager",
            max_wait=90,
        ):
            raise RuntimeError("Flink did not become healthy")

        print("All services are healthy!")


# Global Docker Compose manager (reused across test session)
_docker_manager: Optional[DockerComposeManager] = None


def get_docker_manager() -> DockerComposeManager:
    """Get or create the Docker Compose manager."""
    global _docker_manager
    if _docker_manager is None:
        _docker_manager = DockerComposeManager(PROJECT_ROOT / "docker-compose.yml")
    return _docker_manager
