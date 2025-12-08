"""Gateway helper for integration tests."""

from __future__ import annotations

import json
import logging
import time
from typing import Any, Optional

import requests
from confluent_kafka import Consumer, KafkaError
from requests.auth import HTTPBasicAuth

logger = logging.getLogger(__name__)

# Default timeout for API calls
DEFAULT_TIMEOUT = 10


class GatewayHelper:
    """Helper for Conduktor Gateway operations in tests.

    Provides utilities for:
    - Checking Gateway health
    - Managing alias topics
    - Managing interceptors
    - Cleanup after tests

    Uses Gateway API v2 format (kind, apiVersion, metadata, spec).
    See: https://developers.conduktor.io/openapi/gateway/gateway-3.15.0-v2.yaml
    """

    def __init__(
        self,
        admin_url: str,
        username: str = "admin",
        password: str = "conduktor",
    ) -> None:
        """Initialize Gateway helper.

        Args:
            admin_url: Gateway Admin API URL (e.g., http://localhost:8888)
            username: Admin API username
            password: Admin API password
        """
        self.admin_url = admin_url.rstrip("/")
        self.auth = HTTPBasicAuth(username, password)
        self._session = requests.Session()
        self._session.auth = self.auth

    def check_connection(self) -> bool:
        """Check if Gateway is reachable and healthy."""
        try:
            response = self._session.get(
                f"{self.admin_url}/health",
                timeout=DEFAULT_TIMEOUT,
            )
            return response.status_code == 200
        except requests.RequestException:
            return False

    def _request(
        self,
        method: str,
        endpoint: str,
        json_data: Optional[dict[str, Any]] = None,
        params: Optional[dict[str, str]] = None,
    ) -> requests.Response:
        """Make an authenticated request to the Gateway API."""
        url = f"{self.admin_url}/gateway/v2{endpoint}"
        return self._session.request(
            method=method,
            url=url,
            json=json_data,
            params=params,
            timeout=DEFAULT_TIMEOUT,
        )

    # -------------------------------------------------------------------------
    # Alias Topics (Gateway v2 API)
    # -------------------------------------------------------------------------

    def list_alias_topics(self) -> list[dict[str, Any]]:
        """List all alias topics."""
        response = self._request("GET", "/alias-topic")
        if response.status_code == 200:
            return response.json()
        return []

    def create_alias_topic(
        self,
        name: str,
        physical_topic: str,
        vcluster: Optional[str] = None,
    ) -> bool:
        """Create an alias topic mapping using Gateway v2 API.

        Args:
            name: Virtual topic name
            physical_topic: Physical Kafka topic name
            vcluster: Optional virtual cluster name

        Returns:
            True if created successfully
        """
        metadata: dict[str, Any] = {"name": name}
        if vcluster:
            metadata["vCluster"] = vcluster

        payload: dict[str, Any] = {
            "kind": "AliasTopic",
            "apiVersion": "gateway/v2",
            "metadata": metadata,
            "spec": {
                "physicalName": physical_topic,
            },
        }

        response = self._request("PUT", "/alias-topic", json_data=payload)
        return response.status_code in (200, 201)

    def get_alias_topic(self, name: str) -> Optional[dict[str, Any]]:
        """Get alias topic by name."""
        aliases = self.list_alias_topics()
        for alias in aliases:
            # v2 API returns resources with metadata.name
            alias_name = alias.get("metadata", {}).get("name") or alias.get("name")
            if alias_name == name:
                return alias
        return None

    def delete_alias_topic(self, name: str, vcluster: Optional[str] = None) -> bool:
        """Delete an alias topic using request body (Gateway v2 format)."""
        # Gateway v2 uses DELETE with body, not path parameter
        body: dict[str, Any] = {"name": name}
        if vcluster:
            body["vCluster"] = vcluster

        response = self._request("DELETE", "/alias-topic", json_data=body)
        return response.status_code in (200, 204, 404)

    # -------------------------------------------------------------------------
    # Interceptors (Gateway v2 API)
    # -------------------------------------------------------------------------

    def list_interceptors(self) -> list[dict[str, Any]]:
        """List all interceptors."""
        response = self._request("GET", "/interceptor")
        if response.status_code == 200:
            return response.json()
        return []

    def create_interceptor(
        self,
        name: str,
        plugin_class: str,
        config: dict[str, Any],
        vcluster: Optional[str] = None,
        group: Optional[str] = None,
        username: Optional[str] = None,
        priority: int = 100,
        comment: Optional[str] = None,
    ) -> bool:
        """Create an interceptor using Gateway v2 API.

        Args:
            name: Unique interceptor name
            plugin_class: Full class name of the Gateway plugin
            config: Plugin-specific configuration
            vcluster: Optional virtual cluster scope
            group: Optional group scope
            username: Optional username scope
            priority: Interceptor priority (lower = earlier)
            comment: Optional description

        Returns:
            True if created successfully
        """
        # Build scope - only include if at least one is set
        scope: dict[str, Any] = {}
        if vcluster:
            scope["vCluster"] = vcluster
        if group:
            scope["group"] = group
        if username:
            scope["username"] = username

        metadata: dict[str, Any] = {"name": name}
        if scope:
            metadata["scope"] = scope

        spec: dict[str, Any] = {
            "pluginClass": plugin_class,
            "priority": priority,
            "config": config,
        }
        if comment:
            spec["comment"] = comment

        payload: dict[str, Any] = {
            "kind": "Interceptor",
            "apiVersion": "gateway/v2",
            "metadata": metadata,
            "spec": spec,
        }

        response = self._request("PUT", "/interceptor", json_data=payload)
        return response.status_code in (200, 201)

    def get_interceptor(self, name: str) -> Optional[dict[str, Any]]:
        """Get interceptor by name."""
        interceptors = self.list_interceptors()
        for interceptor in interceptors:
            # v2 API returns resources with metadata.name
            int_name = interceptor.get("metadata", {}).get("name") or interceptor.get("name")
            if int_name == name:
                return interceptor
        return None

    def delete_interceptor(self, name: str) -> bool:
        """Delete an interceptor."""
        response = self._request("DELETE", f"/interceptor/{name}")
        return response.status_code in (200, 204, 404)

    # -------------------------------------------------------------------------
    # Cleanup Utilities
    # -------------------------------------------------------------------------

    def delete_all_test_aliases(self, prefix: str = "test-") -> int:
        """Delete all alias topics with a specific prefix.

        Returns:
            Number of aliases deleted
        """
        aliases = self.list_alias_topics()
        count = 0
        for alias in aliases:
            # Handle both v1 and v2 API response formats
            alias_name = alias.get("metadata", {}).get("name") or alias.get("name", "")
            if alias_name.startswith(prefix):
                self.delete_alias_topic(alias_name)
                count += 1
        return count

    def delete_all_test_interceptors(self, prefix: str = "test-") -> int:
        """Delete all interceptors with a specific prefix.

        Returns:
            Number of interceptors deleted
        """
        interceptors = self.list_interceptors()
        count = 0
        for interceptor in interceptors:
            # Handle both v1 and v2 API response formats
            int_name = interceptor.get("metadata", {}).get("name") or interceptor.get("name", "")
            if int_name.startswith(prefix):
                self.delete_interceptor(int_name)
                count += 1
        return count

    def cleanup_test_resources(self, prefix: str = "test-") -> dict[str, int]:
        """Delete all test resources (aliases and interceptors).

        Returns:
            Dict with counts of deleted resources
        """
        return {
            "aliases": self.delete_all_test_aliases(prefix),
            "interceptors": self.delete_all_test_interceptors(prefix),
        }

    # -------------------------------------------------------------------------
    # Consumer through Gateway Proxy
    # -------------------------------------------------------------------------

    def consume_through_gateway(
        self,
        proxy_bootstrap: str,
        topic: str,
        group_id: str,
        max_messages: int = 100,
        timeout: float = 30.0,
        expected_count: Optional[int] = None,
    ) -> list[dict]:
        """Consume messages through the Gateway proxy.

        This connects to the Gateway proxy (not Kafka directly) to verify
        that interceptors (filters, masks, etc.) are applied correctly.

        Args:
            proxy_bootstrap: Gateway proxy bootstrap servers (e.g., localhost:6969)
            topic: Topic name to consume from (can be virtual topic)
            group_id: Consumer group ID
            max_messages: Maximum messages to consume
            timeout: Timeout in seconds
            expected_count: If set, wait until this many messages are received

        Returns:
            List of decoded message values (as dicts)
        """
        consumer = Consumer(
            {
                "bootstrap.servers": proxy_bootstrap,
                "group.id": group_id,
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
                "session.timeout.ms": 10000,
                "heartbeat.interval.ms": 3000,
            }
        )
        consumer.subscribe([topic])

        messages = []
        start = time.time()

        try:
            while len(messages) < max_messages and (time.time() - start) < timeout:
                msg = consumer.poll(timeout=1.0)
                if msg is None:
                    # If we have expected_count and reached it, stop
                    if expected_count and len(messages) >= expected_count:
                        break
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # End of partition, check if we have enough
                        if expected_count and len(messages) >= expected_count:
                            break
                        continue
                    logger.warning(f"Consumer error: {msg.error()}")
                    continue

                try:
                    value = json.loads(msg.value().decode("utf-8"))
                    messages.append(value)
                except (json.JSONDecodeError, UnicodeDecodeError) as e:
                    logger.warning(f"Failed to decode message: {e}")
                    # Still append raw for debugging
                    messages.append({"_raw": msg.value().decode("utf-8", errors="replace")})

        finally:
            consumer.close()

        return messages

    def wait_for_messages_through_gateway(
        self,
        proxy_bootstrap: str,
        topic: str,
        group_id: str,
        expected_count: int,
        timeout: float = 30.0,
    ) -> list[dict]:
        """Wait until expected number of messages are available through Gateway.

        Useful for tests that need to verify filtering - wait for messages
        to flow through Gateway's interceptor pipeline.

        Args:
            proxy_bootstrap: Gateway proxy bootstrap servers
            topic: Topic name (can be virtual)
            group_id: Consumer group ID
            expected_count: Number of messages to wait for
            timeout: Maximum time to wait

        Returns:
            List of messages

        Raises:
            TimeoutError: If expected messages don't arrive in time
        """
        messages = self.consume_through_gateway(
            proxy_bootstrap=proxy_bootstrap,
            topic=topic,
            group_id=group_id,
            max_messages=expected_count * 2,  # Read more to check for extras
            timeout=timeout,
            expected_count=expected_count,
        )

        if len(messages) < expected_count:
            raise TimeoutError(
                f"Expected {expected_count} messages but only got {len(messages)} "
                f"from topic '{topic}' through Gateway within {timeout}s"
            )

        return messages
