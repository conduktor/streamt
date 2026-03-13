"""Schema Registry deployer for schema management."""

from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass
from typing import Optional

import requests

from streamt.compiler.manifest import SchemaArtifact
from streamt.core import errors

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
    schema: Optional[dict[str, object]] = None
    schema_type: Optional[str] = None
    compatibility: Optional[str] = None


@dataclass
class SchemaChange:
    """A change to apply to a schema."""

    subject: str
    action: str  # register, update, delete, none
    current: Optional[SchemaState] = None
    desired: Optional[SchemaArtifact] = None
    changes: Optional[dict[str, object]] = None

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
        ssl_ca_location: Optional[str] = None,
        ssl_certificate_location: Optional[str] = None,
        ssl_key_location: Optional[str] = None,
        ssl_key_password: Optional[str] = None,
    ) -> None:
        """Initialize Schema Registry deployer."""
        from streamt.deployer.ssl_utils import configure_session_ssl

        self.url = url.rstrip("/")
        self.auth = (username, password) if username and password else None
        self.headers = {"Content-Type": "application/vnd.schemaregistry.v1+json"}
        self._http_session = requests.Session()
        self._http_session.headers.update(self.headers)
        if self.auth:
            self._http_session.auth = self.auth
        configure_session_ssl(
            self._http_session,
            ssl_ca_location=ssl_ca_location,
            ssl_certificate_location=ssl_certificate_location,
            ssl_key_location=ssl_key_location,
            ssl_key_password=ssl_key_password,
        )

    def __enter__(self) -> SchemaRegistryDeployer:
        """Enter context manager."""
        return self

    def __exit__(self, exc_type: object, exc_val: object, exc_tb: object) -> None:
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
        not_found_ok: bool = False,
        **kwargs: object,
    ) -> object:
        """Make a request to Schema Registry. Returns parsed JSON.

        Raises on HTTP errors. If not_found_ok=True, returns None on 404.
        """
        url = f"{self.url}{path}"
        last_err: Optional[requests.ConnectionError] = None
        for attempt in range(3):
            try:
                response = self._http_session.request(method, url, timeout=timeout, **kwargs)
                break
            except requests.ConnectionError as e:
                last_err = e
                if attempt < 2:
                    time.sleep(0.5 * (attempt + 1))
        else:
            raise last_err  # type: ignore[misc]
        if not_found_ok and response.status_code == 404:
            return None
        response.raise_for_status()
        if response.status_code == 204:
            return None
        return response.json()

    def check_connection(self) -> bool:
        """Check if Schema Registry is available."""
        try:
            self._request("GET", "/subjects", timeout=HEALTH_CHECK_TIMEOUT)
            return True
        except Exception as e:
            logger.debug(f"Schema Registry connection check failed: {e}")
            return False

    def list_subjects(self) -> list[str]:
        """List all subjects."""
        return self._request("GET", "/subjects")

    def get_schema_state(self, subject: str) -> SchemaState:
        """Get current state of a schema subject."""
        data = self._request("GET", f"/subjects/{subject}/versions/latest", not_found_ok=True)

        if data is None:
            return SchemaState(subject=subject, exists=False)

        # Parse schema JSON string
        schema = json.loads(data.get("schema", "{}"))

        # Get compatibility level
        compat_data = self._request("GET", f"/config/{subject}", not_found_ok=True)
        compatibility = compat_data.get("compatibilityLevel") if compat_data else None

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
        schema: dict[str, object],
        schema_type: str = "AVRO",
    ) -> int:
        """Register a schema and return the schema ID."""
        payload = {
            "schema": json.dumps(schema) if isinstance(schema, dict) else schema,
            "schemaType": schema_type,
        }
        data = self._request("POST", f"/subjects/{subject}/versions", json=payload)
        return data["id"]

    def set_compatibility(self, subject: str, level: str) -> None:
        """Set compatibility level for a subject."""
        self._request("PUT", f"/config/{subject}", json={"compatibility": level})

    def check_compatibility(
        self,
        subject: str,
        schema: dict[str, object],
        schema_type: str = "AVRO",
    ) -> bool:
        """Check if a schema is compatible with existing versions."""
        payload = {
            "schema": json.dumps(schema) if isinstance(schema, dict) else schema,
            "schemaType": schema_type,
        }
        data = self._request(
            "POST",
            f"/compatibility/subjects/{subject}/versions/latest",
            json=payload,
            not_found_ok=True,
        )
        if data is None:
            return True  # No existing schema, anything is compatible
        return data.get("is_compatible", False)

    def delete_subject(self, subject: str, permanent: bool = False) -> list[int]:
        """Delete a subject."""
        url = f"/subjects/{subject}"
        if permanent:
            url += "?permanent=true"
        data = self._request("DELETE", url, not_found_ok=True)
        return data if data is not None else []

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
        changes: dict[str, object] = {}

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
                # Get compatibility mode for better error message
                compat_mode = current.compatibility or artifact.compatibility or "BACKWARD"
                changes["schema_incompatible"] = {
                    "message": errors.schema_incompatible(
                        artifact.subject,
                        compat_mode,
                        breaking_changes=None,  # Would need schema diff analysis
                    ),
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
            # Set compatibility before registration to allow schema evolution
            if artifact.compatibility:
                self.set_compatibility(artifact.subject, artifact.compatibility)
            self.register_schema(
                artifact.subject,
                artifact.schema,
                artifact.schema_type,
            )
            return "registered"

        elif change.action == "update":
            if change.changes and "schema_incompatible" in change.changes:
                raise RuntimeError(change.changes["schema_incompatible"]["message"])

            # Set compatibility before registration to allow breaking changes
            if change.changes and "compatibility" in change.changes:
                self.set_compatibility(artifact.subject, artifact.compatibility)

            if change.changes and "schema" in change.changes:
                self.register_schema(
                    artifact.subject,
                    artifact.schema,
                    artifact.schema_type,
                )

            return "updated"

        return "unchanged"

    def apply(self, artifact: SchemaArtifact) -> str:
        """Alias for apply_schema."""
        return self.apply_schema(artifact)

    def compute_diff(self, artifact: SchemaArtifact) -> dict[str, object]:
        """Compute diff between current and desired state."""
        change = self.plan_schema(artifact)
        return change.changes or {}
