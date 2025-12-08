"""Gateway deployer for Conduktor Gateway interceptors and alias topics."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Optional

import requests
from requests.auth import HTTPBasicAuth

from streamt.compiler.manifest import GatewayRuleArtifact

logger = logging.getLogger(__name__)

# Default timeouts (in seconds)
DEFAULT_TIMEOUT = 10


class GatewayError(Exception):
    """Base exception for Gateway operations."""

    pass


class GatewayConnectionError(GatewayError):
    """Cannot connect to Gateway."""

    pass


class GatewayAuthenticationError(GatewayError):
    """Gateway authentication failed."""

    pass


@dataclass
class InterceptorState:
    """Current state of an interceptor."""

    name: str
    exists: bool
    plugin_class: Optional[str] = None
    config: Optional[dict[str, Any]] = None
    scope: Optional[dict[str, Any]] = None


@dataclass
class AliasTopicState:
    """Current state of an alias topic."""

    name: str
    exists: bool
    physical_topic: Optional[str] = None


@dataclass
class GatewayRuleChange:
    """A change to apply to a gateway rule."""

    name: str
    action: str  # create, update, delete, none
    current_alias: Optional[AliasTopicState] = None
    current_interceptors: Optional[list[InterceptorState]] = None
    desired: Optional[GatewayRuleArtifact] = None
    changes: Optional[dict[str, Any]] = None


# Mapping from streamt interceptor types to Gateway plugin classes
INTERCEPTOR_PLUGINS = {
    "filter": "io.conduktor.gateway.interceptor.VirtualSqlTopicPlugin",
    "mask": "io.conduktor.gateway.interceptor.safeguard.FieldLevelMaskingPlugin",
    "encrypt": "io.conduktor.gateway.interceptor.FieldLevelEncryptionPlugin",
    "readonly": "io.conduktor.gateway.interceptor.safeguard.ReadOnlyTopicPolicyPlugin",
}


class GatewayDeployer:
    """Deployer for Conduktor Gateway interceptors and alias topics.

    Supports context manager protocol for proper resource cleanup:

        with GatewayDeployer(admin_url) as deployer:
            deployer.list_interceptors()

    Example:
        deployer = GatewayDeployer(
            admin_url="http://localhost:8888",
            username="admin",
            password="conduktor"
        )
        deployer.apply(gateway_rule_artifact)
    """

    def __init__(
        self,
        admin_url: str,
        username: str = "admin",
        password: str = "conduktor",
        virtual_cluster: Optional[str] = None,
    ) -> None:
        """Initialize Gateway deployer.

        Args:
            admin_url: Gateway Admin API URL (e.g., http://localhost:8888)
            username: Admin API username
            password: Admin API password
            virtual_cluster: Optional virtual cluster for multi-tenant setups
        """
        self.admin_url = admin_url.rstrip("/")
        self.auth = HTTPBasicAuth(username, password)
        self.virtual_cluster = virtual_cluster
        self._session = requests.Session()
        self._session.auth = self.auth
        self._closed = False

    def __enter__(self) -> "GatewayDeployer":
        """Enter context manager."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Exit context manager, cleaning up resources."""
        self.close()

    def close(self) -> None:
        """Close the deployer and clean up resources."""
        self._closed = True
        self._session.close()

    def _request(
        self,
        method: str,
        endpoint: str,
        json: Optional[dict[str, Any]] = None,
        params: Optional[dict[str, str]] = None,
    ) -> requests.Response:
        """Make an authenticated request to the Gateway API."""
        url = f"{self.admin_url}/gateway/v2{endpoint}"

        try:
            response = self._session.request(
                method=method,
                url=url,
                json=json,
                params=params,
                timeout=DEFAULT_TIMEOUT,
            )
        except requests.ConnectionError as e:
            raise GatewayConnectionError(
                f"Cannot connect to Gateway at {self.admin_url}. Is it running? Error: {e}"
            )

        if response.status_code == 401:
            raise GatewayAuthenticationError(
                "Gateway authentication failed. Check username/password."
            )

        return response

    def health_check(self) -> bool:
        """Check if Gateway is healthy."""
        try:
            response = self._session.get(
                f"{self.admin_url}/health",
                timeout=DEFAULT_TIMEOUT,
            )
            return response.status_code == 200
        except requests.ConnectionError:
            return False

    # -------------------------------------------------------------------------
    # Interceptors
    # -------------------------------------------------------------------------

    def list_interceptors(self) -> list[dict[str, Any]]:
        """List all interceptors."""
        response = self._request("GET", "/interceptor")
        if response.status_code == 200:
            return response.json()
        return []

    def get_interceptor(self, name: str) -> Optional[dict[str, Any]]:
        """Get a specific interceptor by name."""
        interceptors = self.list_interceptors()
        for interceptor in interceptors:
            # v2 API returns resources with metadata.name
            int_name = interceptor.get("metadata", {}).get("name") or interceptor.get("name")
            if int_name == name:
                return interceptor
        return None

    def create_interceptor(
        self,
        name: str,
        plugin_class: str,
        config: dict[str, Any],
        vcluster: Optional[str] = None,
        priority: int = 100,
    ) -> dict[str, Any]:
        """Create or update an interceptor using Gateway v2 API.

        Args:
            name: Unique name for the interceptor
            plugin_class: Full class name of the Gateway plugin
            config: Plugin-specific configuration
            vcluster: Optional virtual cluster scope
            priority: Interceptor priority (lower = earlier)

        Returns:
            The created/updated interceptor configuration
        """
        # Build scope
        scope: dict[str, Any] = {}
        if vcluster:
            scope["vCluster"] = vcluster
        elif self.virtual_cluster:
            scope["vCluster"] = self.virtual_cluster

        # Build metadata
        metadata: dict[str, Any] = {"name": name}
        if scope:
            metadata["scope"] = scope

        # Build v2 API payload
        payload: dict[str, Any] = {
            "kind": "Interceptor",
            "apiVersion": "gateway/v2",
            "metadata": metadata,
            "spec": {
                "pluginClass": plugin_class,
                "priority": priority,
                "config": config,
            },
        }

        response = self._request("PUT", "/interceptor", json=payload)
        response.raise_for_status()

        logger.info(f"Created/updated interceptor '{name}'")
        return payload

    def delete_interceptor(self, name: str) -> bool:
        """Delete an interceptor by name."""
        response = self._request("DELETE", f"/interceptor/{name}")

        if response.status_code == 404:
            return False

        response.raise_for_status()
        logger.info(f"Deleted interceptor '{name}'")
        return True

    # -------------------------------------------------------------------------
    # Alias Topics
    # -------------------------------------------------------------------------

    def list_alias_topics(self) -> list[dict[str, Any]]:
        """List all alias topic mappings."""
        response = self._request("GET", "/alias-topic")
        if response.status_code == 200:
            return response.json()
        return []

    def get_alias_topic(self, name: str) -> Optional[dict[str, Any]]:
        """Get a specific alias topic by name."""
        aliases = self.list_alias_topics()
        for alias in aliases:
            # v2 API returns resources with metadata.name
            alias_name = alias.get("metadata", {}).get("name") or alias.get("name")
            if alias_name == name:
                return alias
        return None

    def create_alias_topic(
        self,
        name: str,
        physical_topic: str,
        vcluster: Optional[str] = None,
    ) -> dict[str, Any]:
        """Create or update an alias topic mapping using Gateway v2 API.

        Args:
            name: Virtual topic name (what consumers use)
            physical_topic: Physical Kafka topic name
            vcluster: Optional virtual cluster scope

        Returns:
            The created/updated alias configuration
        """
        # Build metadata
        metadata: dict[str, Any] = {"name": name}
        if vcluster:
            metadata["vCluster"] = vcluster
        elif self.virtual_cluster:
            metadata["vCluster"] = self.virtual_cluster

        # Build v2 API payload
        payload: dict[str, Any] = {
            "kind": "AliasTopic",
            "apiVersion": "gateway/v2",
            "metadata": metadata,
            "spec": {
                "physicalName": physical_topic,
            },
        }

        response = self._request("PUT", "/alias-topic", json=payload)
        response.raise_for_status()

        logger.info(f"Created/updated alias topic '{name}' -> '{physical_topic}'")
        return payload

    def delete_alias_topic(self, name: str, vcluster: Optional[str] = None) -> bool:
        """Delete an alias topic by name using request body (Gateway v2 format)."""
        # Gateway v2 uses DELETE with body, not path parameter
        body: dict[str, Any] = {"name": name}
        if vcluster:
            body["vCluster"] = vcluster
        elif self.virtual_cluster:
            body["vCluster"] = self.virtual_cluster

        response = self._request("DELETE", "/alias-topic", json=body)

        if response.status_code == 404:
            return False

        # 200 and 204 are both success
        if response.status_code in (200, 204):
            logger.info(f"Deleted alias topic '{name}'")
            return True

        response.raise_for_status()
        return True

    # -------------------------------------------------------------------------
    # Gateway Rules (combined alias + interceptors)
    # -------------------------------------------------------------------------

    def apply(self, artifact: GatewayRuleArtifact) -> str:
        """Deploy a gateway rule (alias topic + interceptors).

        Returns action taken: "created", "updated", or "unchanged"
        """
        # 1. Create alias topic mapping
        alias_existed = self.get_alias_topic(artifact.virtual_topic) is not None
        self.create_alias_topic(
            name=artifact.virtual_topic,
            physical_topic=artifact.physical_topic,
        )

        # 2. Create interceptors for this rule
        for i, interceptor_config in enumerate(artifact.interceptors):
            interceptor_type = interceptor_config.get("type", "filter")
            config = interceptor_config.get("config", {})

            plugin_class = INTERCEPTOR_PLUGINS.get(interceptor_type)
            if not plugin_class:
                logger.warning(
                    f"Unknown interceptor type '{interceptor_type}', skipping"
                )
                continue

            # Build plugin-specific config
            plugin_config = self._build_plugin_config(
                interceptor_type, config, artifact
            )

            interceptor_name = f"{artifact.name}_{interceptor_type}_{i}"
            self.create_interceptor(
                name=interceptor_name,
                plugin_class=plugin_class,
                config=plugin_config,
            )

        return "updated" if alias_existed else "created"

    def _build_plugin_config(
        self,
        interceptor_type: str,
        config: dict[str, Any],
        artifact: GatewayRuleArtifact,
    ) -> dict[str, Any]:
        """Build plugin-specific configuration."""
        if interceptor_type == "filter":
            # VirtualSqlTopicPlugin config (Gateway v2)
            # Topic names with dashes must be double-quoted in SQL
            where_clause = config.get("where", "")
            quoted_topic = f'"{artifact.physical_topic}"'
            return {
                "virtualTopic": artifact.virtual_topic,
                "statement": f"SELECT * FROM {quoted_topic} WHERE {where_clause}",
            }

        elif interceptor_type == "mask":
            # FieldLevelMaskingPlugin config
            return {
                "policies": [
                    {
                        "name": f"mask_{config.get('field', 'unknown')}",
                        "rule": {
                            "type": config.get("method", "MASK_ALL"),
                            "maskingString": "***",
                        },
                        "fields": [config.get("field")],
                    }
                ]
            }

        elif interceptor_type == "encrypt":
            # FieldLevelEncryptionPlugin config
            return {
                "fields": [config.get("field")],
                "algorithm": config.get("algorithm", "AES256_GCM"),
            }

        else:
            return config

    def delete(self, name: str) -> bool:
        """Delete a gateway rule by name (alias + all related interceptors)."""
        deleted = False

        # Delete alias topic
        if self.delete_alias_topic(name):
            deleted = True

        # Delete related interceptors (by prefix)
        interceptors = self.list_interceptors()
        for interceptor in interceptors:
            # v2 API uses metadata.name
            int_name = interceptor.get("metadata", {}).get("name") or interceptor.get("name", "")
            if int_name.startswith(f"{name}_"):
                self.delete_interceptor(int_name)
                deleted = True

        return deleted

    def plan(self, artifact: GatewayRuleArtifact) -> GatewayRuleChange:
        """Plan changes for a gateway rule."""
        alias_state = self.get_alias_topic(artifact.virtual_topic)

        if alias_state is None:
            return GatewayRuleChange(
                name=artifact.name,
                action="create",
                desired=artifact,
            )

        # Check if physical topic changed (v2 API uses spec.physicalName)
        current_physical = (
            alias_state.get("spec", {}).get("physicalName")
            or alias_state.get("physicalName")
        )
        if current_physical != artifact.physical_topic:
            return GatewayRuleChange(
                name=artifact.name,
                action="update",
                current_alias=AliasTopicState(
                    name=artifact.virtual_topic,
                    exists=True,
                    physical_topic=current_physical,
                ),
                desired=artifact,
                changes={"physical_topic": {"from": current_physical, "to": artifact.physical_topic}},
            )

        return GatewayRuleChange(
            name=artifact.name,
            action="none",
            current_alias=AliasTopicState(
                name=artifact.virtual_topic,
                exists=True,
                physical_topic=current_physical,
            ),
            desired=artifact,
        )

    def list_rules(self) -> list[dict[str, Any]]:
        """List all gateway rules (alias topics with their interceptors)."""
        aliases = self.list_alias_topics()
        interceptors = self.list_interceptors()

        rules = []
        for alias in aliases:
            # v2 API uses metadata.name and spec.physicalName
            alias_name = alias.get("metadata", {}).get("name") or alias.get("name", "")
            physical_topic = (
                alias.get("spec", {}).get("physicalName")
                or alias.get("physicalName")
            )

            related_interceptors = [
                i for i in interceptors
                if (i.get("metadata", {}).get("name") or i.get("name", "")).startswith(f"{alias_name}_")
            ]

            rules.append({
                "name": alias_name,
                "physical_topic": physical_topic,
                "interceptors": related_interceptors,
            })

        return rules
