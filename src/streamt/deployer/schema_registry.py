"""Schema Registry deployer for schema management."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import Any, Optional

import requests

from streamt.compiler.manifest import SchemaArtifact

logger = logging.getLogger(__name__)

# Default timeouts (in seconds)
DEFAULT_TIMEOUT = 30
HEALTH_CHECK_TIMEOUT = 10


@dataclass
class SchemaState:
    """Current state of a schema subject."""

    subject: str
    exists: bool
    version: Optional[int] = None
    schema_id: Optional[int] = None
    schema: Optional[dict[str, Any]] = None
    schema_type: Optional[str] = None
    compatibility: Optional[str] = None


@dataclass
class SchemaChange:
    """A change to apply to a schema."""

    subject: str
    action: str  # register, update, delete, none
    current: Optional[SchemaState] = None
    desired: Optional[SchemaArtifact] = None
    changes: Optional[dict[str, Any]] = None

    def __post_init__(self) -> None:
        if self.changes is None:
            self.changes = {}


class SchemaRegistryDeployer:
    """Deployer for Schema Registry schemas.

    Supports context manager protocol for proper resource cleanup:

        with SchemaRegistryDeployer(url) as deployer:
            deployer.list_subjects()
    """

    def __init__(
        self,
        url: str,
        username: Optional[str] = None,
        password: Optional[str] = None,
    ) -> None:
        """Initialize Schema Registry deployer."""
        self.url = url.rstrip("/")
        self.auth = (username, password) if username and password else None
        self.headers = {"Content-Type": "application/vnd.schemaregistry.v1+json"}
        self._http_session = requests.Session()
        self._http_session.headers.update(self.headers)
        if self.auth:
            self._http_session.auth = self.auth

    def __enter__(self) -> "SchemaRegistryDeployer":
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
        path: str,
        timeout: int = DEFAULT_TIMEOUT,
        **kwargs: Any,
    ) -> requests.Response:
        """Make a request to Schema Registry.

        Args:
            method: HTTP method
            path: API path
            timeout: Request timeout in seconds
            **kwargs: Additional arguments passed to requests
        """
        url = f"{self.url}{path}"
        return self._http_session.request(method, url, timeout=timeout, **kwargs)

    def check_connection(self) -> bool:
        """Check if Schema Registry is available."""
        try:
            response = self._request("GET", "/subjects", timeout=HEALTH_CHECK_TIMEOUT)
            return response.status_code == 200
        except Exception as e:
            logger.debug(f"Schema Registry connection check failed: {e}")
            return False

    def list_subjects(self) -> list[str]:
        """List all subjects."""
        response = self._request("GET", "/subjects")
        response.raise_for_status()
        return response.json()

    def get_schema_state(self, subject: str) -> SchemaState:
        """Get current state of a schema subject."""
        response = self._request("GET", f"/subjects/{subject}/versions/latest")

        if response.status_code == 404:
            return SchemaState(subject=subject, exists=False)

        response.raise_for_status()
        data = response.json()

        # Parse schema JSON string
        schema = json.loads(data.get("schema", "{}"))

        # Get compatibility level
        compat_response = self._request("GET", f"/config/{subject}")
        compatibility = None
        if compat_response.status_code == 200:
            compatibility = compat_response.json().get("compatibilityLevel")

        return SchemaState(
            subject=subject,
            exists=True,
            version=data.get("version"),
            schema_id=data.get("id"),
            schema=schema,
            schema_type=data.get("schemaType", "AVRO"),
            compatibility=compatibility,
        )

    def register_schema(
        self,
        subject: str,
        schema: dict[str, Any],
        schema_type: str = "AVRO",
    ) -> int:
        """Register a schema and return the schema ID."""
        payload = {
            "schema": json.dumps(schema) if isinstance(schema, dict) else schema,
            "schemaType": schema_type,
        }
        response = self._request(
            "POST",
            f"/subjects/{subject}/versions",
            json=payload,
        )
        response.raise_for_status()
        return response.json()["id"]

    def set_compatibility(self, subject: str, level: str) -> None:
        """Set compatibility level for a subject."""
        response = self._request(
            "PUT",
            f"/config/{subject}",
            json={"compatibility": level},
        )
        response.raise_for_status()

    def check_compatibility(
        self,
        subject: str,
        schema: dict[str, Any],
        schema_type: str = "AVRO",
    ) -> bool:
        """Check if a schema is compatible with existing versions."""
        payload = {
            "schema": json.dumps(schema) if isinstance(schema, dict) else schema,
            "schemaType": schema_type,
        }
        response = self._request(
            "POST",
            f"/compatibility/subjects/{subject}/versions/latest",
            json=payload,
        )
        if response.status_code == 404:
            return True  # No existing schema, anything is compatible
        response.raise_for_status()
        return response.json().get("is_compatible", False)

    def delete_subject(self, subject: str, permanent: bool = False) -> list[int]:
        """Delete a subject."""
        url = f"/subjects/{subject}"
        if permanent:
            url += "?permanent=true"
        response = self._request("DELETE", url)
        if response.status_code == 404:
            return []
        response.raise_for_status()
        return response.json()

    def plan_schema(self, artifact: SchemaArtifact) -> SchemaChange:
        """Plan changes for a schema."""
        current = self.get_schema_state(artifact.subject)

        if not current.exists:
            return SchemaChange(
                subject=artifact.subject,
                action="register",
                current=current,
                desired=artifact,
            )

        # Check for changes
        changes: dict[str, Any] = {}

        # Compare schemas (normalize for comparison)
        current_schema_str = json.dumps(current.schema, sort_keys=True)
        desired_schema_str = json.dumps(artifact.schema, sort_keys=True)

        if current_schema_str != desired_schema_str:
            # Check compatibility before allowing update
            is_compatible = self.check_compatibility(
                artifact.subject,
                artifact.schema,
                artifact.schema_type,
            )
            if is_compatible:
                changes["schema"] = {
                    "from_version": current.version,
                    "to_version": (current.version or 0) + 1,
                    "compatible": True,
                }
            else:
                changes["schema_incompatible"] = {
                    "message": "Schema change is not compatible with existing versions",
                    "current_version": current.version,
                }

        # Check compatibility level change
        if artifact.compatibility and artifact.compatibility != current.compatibility:
            changes["compatibility"] = {
                "from": current.compatibility,
                "to": artifact.compatibility,
            }

        if changes:
            return SchemaChange(
                subject=artifact.subject,
                action="update",
                current=current,
                desired=artifact,
                changes=changes,
            )

        return SchemaChange(
            subject=artifact.subject,
            action="none",
            current=current,
            desired=artifact,
        )

    def apply_schema(self, artifact: SchemaArtifact) -> str:
        """Apply a schema artifact. Returns action taken."""
        change = self.plan_schema(artifact)

        if change.action == "register":
            self.register_schema(
                artifact.subject,
                artifact.schema,
                artifact.schema_type,
            )
            if artifact.compatibility:
                self.set_compatibility(artifact.subject, artifact.compatibility)
            return "registered"

        elif change.action == "update":
            if change.changes and "schema_incompatible" in change.changes:
                raise RuntimeError(change.changes["schema_incompatible"]["message"])

            if change.changes and "schema" in change.changes:
                self.register_schema(
                    artifact.subject,
                    artifact.schema,
                    artifact.schema_type,
                )

            if change.changes and "compatibility" in change.changes:
                self.set_compatibility(artifact.subject, artifact.compatibility)

            return "updated"

        return "unchanged"

    def apply(self, artifact: SchemaArtifact) -> str:
        """Alias for apply_schema."""
        return self.apply_schema(artifact)

    def compute_diff(self, artifact: SchemaArtifact) -> dict[str, Any]:
        """Compute diff between current and desired state."""
        change = self.plan_schema(artifact)
        return change.changes or {}
