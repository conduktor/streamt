"""Schema Registry deployer for schema management."""

from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass, field
from typing import Literal, Optional
from urllib.parse import quote

import requests

from streamt.compiler.manifest import SchemaArtifact
from streamt.core import errors

logger = logging.getLogger(__name__)

# Default timeouts (in seconds)
DEFAULT_TIMEOUT = 30
HEALTH_CHECK_TIMEOUT = 10
SchemaVersion = int | Literal["latest"]


class SchemaRegistryResolutionError(RuntimeError):
    """A Schema Registry subject or reference graph could not be resolved."""


@dataclass(frozen=True)
class SchemaReference:
    """A version-pinned Schema Registry reference."""

    name: str
    subject: str
    version: int


@dataclass
class SchemaState:
    """Current state of a schema subject."""

    subject: str
    exists: bool
    version: Optional[int] = None
    schema_id: Optional[int] = None
    schema: object | None = None
    schema_type: Optional[str] = None
    compatibility: Optional[str] = None
    references: list[SchemaReference] = field(default_factory=list)
    resolved_references: list[SchemaState] = field(default_factory=list)


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

        if not url or not url.startswith(("http://", "https://")):
            raise ValueError(f"Invalid Schema Registry URL: {url!r} — must start with http:// or https://")
        self.url = url.rstrip("/")
        self.auth = (username, password) if username and password else None
        self.headers = {"Content-Type": "application/vnd.schemaregistry.v1+json"}
        self._closed = False
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
        self._closed = True
        self._http_session.close()

    def _request(
        self,
        method: str,
        path: str,
        timeout: int = DEFAULT_TIMEOUT,
        not_found_ok: bool = False,
        **kwargs: object,
    ) -> dict | list | None:
        """Make a request to Schema Registry. Returns parsed JSON.

        Raises on HTTP errors. If not_found_ok=True, returns None on 404.
        """
        if self._closed:
            raise RuntimeError("SchemaRegistryDeployer is closed")
        url = f"{self.url}{path}"
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

    @staticmethod
    def _subject_path(subject: str) -> str:
        """Encode a subject as one Schema Registry URL path segment."""
        return quote(subject, safe="")

    @staticmethod
    def _validate_version(version: SchemaVersion) -> SchemaVersion:
        if version == "latest" or (
            isinstance(version, int) and not isinstance(version, bool) and version > 0
        ):
            return version
        raise ValueError("Schema version must be a positive integer or 'latest'")

    def get_schema_state(
        self,
        subject: str,
        version: SchemaVersion = "latest",
        *,
        include_compatibility: bool = True,
    ) -> SchemaState:
        """Read one subject version and its reference metadata without mutation.

        Avro and JSON documents are decoded from JSON. Protobuf is retained as
        raw schema text; its version and references remain available for validation.
        """
        requested_version = self._validate_version(version)
        encoded_subject = self._subject_path(subject)
        data = self._request(
            "GET",
            f"/subjects/{encoded_subject}/versions/{requested_version}",
            not_found_ok=True,
        )

        if data is None:
            return SchemaState(subject=subject, exists=False)
        if not isinstance(data, dict):
            raise SchemaRegistryResolutionError(
                f"Schema Registry returned invalid metadata for subject '{subject}' "
                f"version '{requested_version}'"
            )

        schema_type = str(data.get("schemaType", "AVRO")).upper()
        resolved_version = data.get("version")
        schema_id = data.get("id")
        if not isinstance(resolved_version, int) or resolved_version < 1:
            raise SchemaRegistryResolutionError(
                f"Schema Registry returned no positive version for subject '{subject}' "
                f"requested as '{requested_version}'"
            )
        if not isinstance(schema_id, int):
            raise SchemaRegistryResolutionError(
                f"Schema Registry returned no schema id for subject '{subject}' "
                f"version {resolved_version}"
            )
        raw_schema = data.get("schema")
        if not isinstance(raw_schema, str):
            raise SchemaRegistryResolutionError(
                f"Schema Registry response for subject '{subject}' version "
                f"'{requested_version}' has no schema text"
            )

        if schema_type in {"AVRO", "JSON"}:
            try:
                schema: object = json.loads(raw_schema)
            except json.JSONDecodeError as e:
                raise SchemaRegistryResolutionError(
                    f"Subject '{subject}' version '{requested_version}' contains malformed "
                    f"{schema_type} schema JSON: {e.msg}"
                ) from e
        else:
            schema = raw_schema

        references: list[SchemaReference] = []
        raw_references = data.get("references", [])
        if not isinstance(raw_references, list):
            raise SchemaRegistryResolutionError(
                f"Schema Registry returned invalid references for subject '{subject}' "
                f"version '{requested_version}'"
            )
        for index, reference in enumerate(raw_references):
            if not isinstance(reference, dict):
                raise SchemaRegistryResolutionError(
                    f"Reference {index} on subject '{subject}' version "
                    f"'{requested_version}' is not an object"
                )
            name = reference.get("name")
            referenced_subject = reference.get("subject")
            referenced_version = reference.get("version")
            if (
                not isinstance(name, str)
                or not isinstance(referenced_subject, str)
                or not isinstance(referenced_version, int)
                or referenced_version < 1
            ):
                raise SchemaRegistryResolutionError(
                    f"Reference {index} on subject '{subject}' version "
                    f"'{requested_version}' must contain name, subject, and a positive version"
                )
            references.append(
                SchemaReference(
                    name=name,
                    subject=referenced_subject,
                    version=referenced_version,
                )
            )

        compatibility = None
        if include_compatibility:
            compat_data = self._request(
                "GET",
                f"/config/{encoded_subject}?defaultToGlobal=true",
                not_found_ok=True,
            )
            compatibility = (
                compat_data.get("compatibilityLevel") if isinstance(compat_data, dict) else None
            )

        return SchemaState(
            subject=subject,
            exists=True,
            version=resolved_version,
            schema_id=schema_id,
            schema=schema,
            schema_type=schema_type,
            compatibility=compatibility,
            references=references,
        )

    def resolve_schema_state(
        self,
        subject: str,
        version: SchemaVersion = "latest",
        *,
        max_reference_depth: int = 20,
    ) -> SchemaState:
        """Resolve a subject plus all version-pinned references using GET requests only."""
        if max_reference_depth < 1:
            raise ValueError("max_reference_depth must be a positive integer")
        root = self.get_schema_state(subject, version)
        if not root.exists:
            return root

        seen: set[tuple[str, int]] = set()
        if root.version is not None:
            seen.add((root.subject, root.version))

        def resolve_references(state: SchemaState, depth: int) -> None:
            for reference in state.references:
                if depth >= max_reference_depth:
                    raise SchemaRegistryResolutionError(
                        f"Schema reference graph for subject '{subject}' exceeds "
                        f"maximum depth {max_reference_depth}"
                    )
                key = (reference.subject, reference.version)
                if key in seen:
                    continue
                seen.add(key)
                referenced = self.get_schema_state(
                    reference.subject,
                    reference.version,
                    include_compatibility=False,
                )
                if not referenced.exists:
                    raise SchemaRegistryResolutionError(
                        f"Subject '{state.subject}' version {state.version} reference "
                        f"'{reference.name}' points to missing subject "
                        f"'{reference.subject}' version {reference.version}"
                    )
                state.resolved_references.append(referenced)
                resolve_references(referenced, depth + 1)

        resolve_references(root, 0)
        return root

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
        if not isinstance(data, dict) or "id" not in data:
            raise RuntimeError(
                f"Schema Registry did not return a schema ID for '{subject}'. Response: {data}"
            )
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

        schema_type_changed = (
            current.schema_type is not None
            and current.schema_type.upper() != artifact.schema_type.upper()
        )
        if current_schema_str != desired_schema_str or schema_type_changed:
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
                if schema_type_changed:
                    changes["schema_type"] = {
                        "from": current.schema_type,
                        "to": artifact.schema_type.upper(),
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
            # Register schema first (validates content under current rules),
            # then set compatibility (only once we know the schema is valid).
            self.register_schema(
                artifact.subject,
                artifact.schema,
                artifact.schema_type,
            )
            if artifact.compatibility:
                try:
                    self.set_compatibility(artifact.subject, artifact.compatibility)
                except Exception as e:
                    logger.warning(
                        "Schema '%s' registered but compatibility not set: %s",
                        artifact.subject, e,
                    )
            return "registered"

        elif change.action == "update":
            if change.changes and "schema_incompatible" in change.changes:
                err = change.changes["schema_incompatible"]
                raise RuntimeError(err.get("message", str(err)))

            # Register schema first (under current compatibility rules),
            # then update compatibility level (safe order: content before policy).
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

    def compute_diff(self, artifact: SchemaArtifact) -> dict[str, object]:
        """Compute diff between current and desired state."""
        change = self.plan_schema(artifact)
        return change.changes or {}
