"""Kafka Connect deployer for connector management."""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Optional

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

    def __init__(
        self,
        rest_url: str,
        username: Optional[str] = None,
        password: Optional[str] = None,
        ssl_ca_location: Optional[str] = None,
        ssl_certificate_location: Optional[str] = None,
        ssl_key_location: Optional[str] = None,
        ssl_key_password: Optional[str] = None,
    ) -> None:
        """Initialize Connect deployer."""
        from streamt.deployer.ssl_utils import configure_session_ssl

        if not rest_url or not rest_url.startswith(("http://", "https://")):
            raise ValueError(f"Invalid Connect REST URL: {rest_url!r} — must start with http:// or https://")
        self.rest_url = rest_url.rstrip("/")
        self._closed = False
        self._http_session = requests.Session()
        if username and password:
            self._http_session.auth = (username, password)
        configure_session_ssl(
            self._http_session,
            ssl_ca_location=ssl_ca_location,
            ssl_certificate_location=ssl_certificate_location,
            ssl_key_location=ssl_key_location,
            ssl_key_password=ssl_key_password,
        )

    def __enter__(self) -> ConnectDeployer:
        """Enter context manager."""
        return self

    def __exit__(self, exc_type: object, exc_val: object, exc_tb: object) -> None:
        """Exit context manager, cleaning up resources."""
        self.close()

    def close(self) -> None:
        """Close the deployer and clean up resources."""
        self._closed = True
        self._http_session.close()

    def _request(
        self,
        method: str,
        endpoint: str,
        timeout: int = DEFAULT_TIMEOUT,
        **kwargs: object,
    ) -> dict | list | None:
        """Make a request to Connect REST API. Returns parsed JSON.

        Raises on HTTP errors.
        """
        if self._closed:
            raise RuntimeError("ConnectDeployer is closed")
        url = f"{self.rest_url}{endpoint}"
        last_err: Optional[Exception] = None
        for attempt in range(3):
            try:
                response = self._http_session.request(method, url, timeout=timeout, **kwargs)
                status_code = getattr(response, "status_code", 200)
                if isinstance(status_code, int) and status_code >= 500 and attempt < 2:
                    last_err = requests.HTTPError(response=response)
                    time.sleep(0.5 * (attempt + 1))
                    continue
                break
            except (requests.ConnectionError, requests.Timeout) as e:
                last_err = e
                if attempt < 2:
                    time.sleep(0.5 * (attempt + 1))
        else:
            raise last_err  # type: ignore[misc]
        response.raise_for_status()
        if response.status_code == 204 or not response.content:
            return None
        return response.json()

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
            if current_value is None or str(current_value).lower() != str(value).lower():
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
