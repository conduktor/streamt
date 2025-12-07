"""Kafka Connect deployer for connector management."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Optional

import requests

from streamt.compiler.manifest import ConnectorArtifact

logger = logging.getLogger(__name__)

# Default timeouts (in seconds)
DEFAULT_TIMEOUT = 30
HEALTH_CHECK_TIMEOUT = 10


@dataclass
class ConnectorState:
    """Current state of a connector."""

    name: str
    exists: bool
    config: Optional[dict] = None
    status: Optional[str] = None
    tasks: list[dict] = None

    def __post_init__(self) -> None:
        if self.tasks is None:
            self.tasks = []


@dataclass
class ConnectorChange:
    """A change to apply to a connector."""

    connector_name: str
    action: str  # create, update, delete, none
    current: Optional[ConnectorState] = None
    desired: Optional[ConnectorArtifact] = None
    changes: dict = None

    def __post_init__(self) -> None:
        if self.changes is None:
            self.changes = {}


class ConnectDeployer:
    """Deployer for Kafka Connect connectors.

    Supports context manager protocol for proper resource cleanup:

        with ConnectDeployer(rest_url) as deployer:
            deployer.list_connectors()
    """

    def __init__(self, rest_url: str) -> None:
        """Initialize Connect deployer."""
        self.rest_url = rest_url.rstrip("/")
        self._http_session = requests.Session()

    def __enter__(self) -> "ConnectDeployer":
        """Enter context manager."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Exit context manager, cleaning up resources."""
        self.close()

    def close(self) -> None:
        """Close the deployer and clean up resources."""
        self._http_session.close()

    def _request(
        self,
        method: str,
        endpoint: str,
        timeout: int = DEFAULT_TIMEOUT,
        **kwargs: Any,
    ) -> dict | list:
        """Make a request to Connect REST API.

        Args:
            method: HTTP method
            endpoint: API endpoint
            timeout: Request timeout in seconds
            **kwargs: Additional arguments passed to requests
        """
        url = f"{self.rest_url}{endpoint}"
        response = self._http_session.request(method, url, timeout=timeout, **kwargs)
        response.raise_for_status()
        return response.json() if response.content else {}

    def check_connection(self) -> bool:
        """Check if Connect cluster is accessible."""
        try:
            self._request("GET", "/", timeout=HEALTH_CHECK_TIMEOUT)
            return True
        except Exception as e:
            logger.debug(f"Connect connection check failed: {e}")
            return False

    def list_connectors(self) -> list[str]:
        """List all connectors."""
        return self._request("GET", "/connectors")

    def get_connector_state(self, connector_name: str) -> ConnectorState:
        """Get current state of a connector."""
        try:
            config = self._request("GET", f"/connectors/{connector_name}/config")
            status = self._request("GET", f"/connectors/{connector_name}/status")

            return ConnectorState(
                name=connector_name,
                exists=True,
                config=config,
                status=status.get("connector", {}).get("state"),
                tasks=status.get("tasks", []),
            )
        except requests.HTTPError as e:
            if e.response.status_code == 404:
                return ConnectorState(name=connector_name, exists=False)
            raise

    def create_connector(self, artifact: ConnectorArtifact) -> dict:
        """Create a new connector."""
        payload = {
            "name": artifact.name,
            "config": artifact.to_dict()["config"],
        }
        return self._request("POST", "/connectors", json=payload)

    def update_connector(self, artifact: ConnectorArtifact) -> dict:
        """Update an existing connector."""
        config = artifact.to_dict()["config"]
        return self._request(
            "PUT",
            f"/connectors/{artifact.name}/config",
            json=config,
        )

    def delete_connector(self, connector_name: str) -> None:
        """Delete a connector."""
        self._request("DELETE", f"/connectors/{connector_name}")

    def restart_connector(self, connector_name: str) -> None:
        """Restart a connector."""
        self._request("POST", f"/connectors/{connector_name}/restart")

    def pause_connector(self, connector_name: str) -> None:
        """Pause a connector."""
        self._request("PUT", f"/connectors/{connector_name}/pause")

    def resume_connector(self, connector_name: str) -> None:
        """Resume a connector."""
        self._request("PUT", f"/connectors/{connector_name}/resume")

    def plan_connector(self, artifact: ConnectorArtifact) -> ConnectorChange:
        """Plan changes for a connector."""
        current = self.get_connector_state(artifact.name)

        if not current.exists:
            return ConnectorChange(
                connector_name=artifact.name,
                action="create",
                current=current,
                desired=artifact,
            )

        # Check for config changes
        desired_config = artifact.to_dict()["config"]
        changes = {}

        # Remove name from comparison
        current_config = dict(current.config or {})
        current_config.pop("name", None)
        desired_config_cmp = dict(desired_config)
        desired_config_cmp.pop("name", None)

        for key, value in desired_config_cmp.items():
            current_value = current_config.get(key)
            if str(current_value) != str(value):
                changes[key] = {
                    "from": current_value,
                    "to": value,
                }

        # Check for removed keys and warn
        removed_keys = set(current_config.keys()) - set(desired_config_cmp.keys())
        if removed_keys:
            logger.warning(
                f"Connector '{artifact.name}' will have config keys removed: {sorted(removed_keys)}"
            )
            for key in removed_keys:
                changes[key] = {
                    "from": current_config[key],
                    "to": None,
                }

        if changes:
            return ConnectorChange(
                connector_name=artifact.name,
                action="update",
                current=current,
                desired=artifact,
                changes=changes,
            )

        return ConnectorChange(
            connector_name=artifact.name,
            action="none",
            current=current,
            desired=artifact,
        )

    def apply_connector(self, artifact: ConnectorArtifact) -> str:
        """Apply a connector artifact. Returns action taken."""
        change = self.plan_connector(artifact)

        if change.action == "create":
            self.create_connector(artifact)
            return "created"
        elif change.action == "update":
            self.update_connector(artifact)
            return "updated"
        else:
            return "unchanged"
