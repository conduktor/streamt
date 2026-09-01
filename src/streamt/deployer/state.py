"""Persisted ownership state for safe deployment planning.

This module intentionally does not produce deployment actions.  It records
only resources explicitly managed or adopted by streamt and can report inert
removal candidates for a later, explicit destructive workflow.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import tempfile
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal

CURRENT_STATE_VERSION = 1
_CHECKSUM_PATTERN = re.compile(r"^sha256:[0-9a-f]{64}$")


class StateError(ValueError):
    """Base error for invalid or incompatible persisted state."""


class StateFormatError(StateError):
    """Persisted state is malformed."""


class StateVersionError(StateError):
    """Persisted state uses an unsupported version."""


class StateIdentityError(StateError):
    """Persisted state belongs to a different project or environment."""


def _require_segment(value: object, label: str) -> str:
    """Validate one unescaped component of a stable resource identity."""
    if not isinstance(value, str) or not value:
        raise StateFormatError(f"{label} must be a non-empty string")
    if "/" in value:
        raise StateFormatError(f"{label} must not contain '/'")
    return value


@dataclass(frozen=True, order=True)
class ResourceIdentity:
    """Stable logical identity for one runtime resource."""

    project: str
    environment: str
    kind: str
    logical_name: str

    def __post_init__(self) -> None:
        _require_segment(self.project, "project")
        _require_segment(self.environment, "environment")
        _require_segment(self.kind, "kind")
        _require_segment(self.logical_name, "logical_name")

    @property
    def uri(self) -> str:
        """Return the canonical streamt resource URI."""
        return (
            f"streamt://{self.project}/{self.environment}/"
            f"{self.kind}/{self.logical_name}"
        )

    @classmethod
    def parse(cls, value: object) -> ResourceIdentity:
        """Parse and validate a canonical streamt resource URI."""
        if not isinstance(value, str) or not value.startswith("streamt://"):
            raise StateFormatError("resource identity must start with 'streamt://'")
        parts = value.removeprefix("streamt://").split("/")
        if len(parts) != 4:
            raise StateFormatError(
                "resource identity must be "
                "streamt://<project>/<environment>/<kind>/<logical-name>"
            )
        identity = cls(*parts)
        if identity.uri != value:
            raise StateFormatError(f"resource identity is not canonical: {value!r}")
        return identity


def resource_id(project: str, environment: str, kind: str, logical_name: str) -> str:
    """Build a stable resource URI."""
    return ResourceIdentity(project, environment, kind, logical_name).uri


def artifact_checksum(artifact: Mapping[str, object]) -> str:
    """Hash an artifact using deterministic canonical JSON."""
    try:
        payload = json.dumps(
            artifact,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise StateFormatError(f"artifact is not JSON serializable: {exc}") from exc
    return f"sha256:{hashlib.sha256(payload).hexdigest()}"


OwnershipMode = Literal["managed", "adopted"]


@dataclass(frozen=True)
class ManagedResourceRecord:
    """Last-applied state for one resource streamt owns or has adopted."""

    physical_name: str
    ownership: OwnershipMode
    artifact_checksum: str
    backend: str

    def __post_init__(self) -> None:
        if not isinstance(self.physical_name, str) or not self.physical_name:
            raise StateFormatError("physical_name must be a non-empty string")
        if self.ownership not in ("managed", "adopted"):
            raise StateFormatError("ownership must be 'managed' or 'adopted'")
        if not isinstance(self.artifact_checksum, str) or not _CHECKSUM_PATTERN.fullmatch(
            self.artifact_checksum
        ):
            raise StateFormatError("artifact_checksum must be a sha256:<64 lowercase hex> value")
        if not isinstance(self.backend, str) or not self.backend:
            raise StateFormatError("backend must be a non-empty string")

    def to_dict(self) -> dict[str, str]:
        return {
            "physical_name": self.physical_name,
            "ownership": self.ownership,
            "artifact_checksum": self.artifact_checksum,
            "backend": self.backend,
        }

    @classmethod
    def from_dict(cls, data: object) -> ManagedResourceRecord:
        """Parse one resource record without accepting unknown fields."""
        if not isinstance(data, dict):
            raise StateFormatError("resource record must be an object")
        expected = {"physical_name", "ownership", "artifact_checksum", "backend"}
        unknown = set(data) - expected
        missing = expected - set(data)
        if unknown:
            raise StateFormatError(
                f"resource record has unknown field(s): {', '.join(sorted(unknown))}"
            )
        if missing:
            raise StateFormatError(
                f"resource record is missing field(s): {', '.join(sorted(missing))}"
            )
        ownership = data["ownership"]
        if ownership not in ("managed", "adopted"):
            raise StateFormatError("ownership must be 'managed' or 'adopted'")
        return cls(
            physical_name=data["physical_name"],
            ownership=ownership,
            artifact_checksum=data["artifact_checksum"],
            backend=data["backend"],
        )


@dataclass(frozen=True)
class RemovalCandidate:
    """An inert prior-state resource absent from the desired comparison set."""

    resource_id: str
    record: ManagedResourceRecord


@dataclass
class LocalState:
    """Versioned local snapshot of resources owned by one project environment."""

    project: str
    environment: str
    serial: int = 0
    resources: dict[str, ManagedResourceRecord] = field(default_factory=dict)
    state_version: int = CURRENT_STATE_VERSION

    def __post_init__(self) -> None:
        _require_segment(self.project, "project")
        _require_segment(self.environment, "environment")
        if type(self.state_version) is not int or self.state_version != CURRENT_STATE_VERSION:
            raise StateVersionError(
                f"unsupported state version {self.state_version!r}; "
                f"expected {CURRENT_STATE_VERSION}"
            )
        if type(self.serial) is not int or self.serial < 0:
            raise StateFormatError("serial must be a non-negative integer")
        for identity, record in self.resources.items():
            self._validate_resource(identity, record)

    def _validate_resource(self, resource_uri: object, record: object) -> None:
        identity = ResourceIdentity.parse(resource_uri)
        if identity.project != self.project or identity.environment != self.environment:
            raise StateIdentityError(
                f"resource {identity.uri!r} does not belong to "
                f"{self.project!r}/{self.environment!r}"
            )
        if not isinstance(record, ManagedResourceRecord):
            raise StateFormatError(f"resource {identity.uri!r} has an invalid record")

    def to_dict(self) -> dict[str, object]:
        """Serialize using stable resource ordering."""
        return {
            "state_version": self.state_version,
            "project": self.project,
            "environment": self.environment,
            "serial": self.serial,
            "resources": {
                resource_uri: self.resources[resource_uri].to_dict()
                for resource_uri in sorted(self.resources)
            },
        }

    @classmethod
    def from_dict(
        cls,
        data: object,
        *,
        expected_project: str | None = None,
        expected_environment: str | None = None,
    ) -> LocalState:
        """Parse state and optionally enforce its project/environment identity."""
        if not isinstance(data, dict):
            raise StateFormatError("state must be a JSON object")
        expected = {"state_version", "project", "environment", "serial", "resources"}
        unknown = set(data) - expected
        missing = expected - set(data)
        if unknown:
            raise StateFormatError(f"state has unknown field(s): {', '.join(sorted(unknown))}")
        if missing:
            raise StateFormatError(f"state is missing field(s): {', '.join(sorted(missing))}")

        version = data["state_version"]
        if type(version) is not int or version != CURRENT_STATE_VERSION:
            raise StateVersionError(
                f"unsupported state version {version!r}; expected {CURRENT_STATE_VERSION}"
            )
        project = _require_segment(data["project"], "project")
        environment = _require_segment(data["environment"], "environment")
        if expected_project is not None and project != expected_project:
            raise StateIdentityError(
                f"state belongs to project {project!r}, expected {expected_project!r}"
            )
        if expected_environment is not None and environment != expected_environment:
            raise StateIdentityError(
                f"state belongs to environment {environment!r}, "
                f"expected {expected_environment!r}"
            )
        raw_resources = data["resources"]
        if not isinstance(raw_resources, dict):
            raise StateFormatError("resources must be an object keyed by resource identity")
        resources = {
            resource_uri: ManagedResourceRecord.from_dict(record)
            for resource_uri, record in raw_resources.items()
        }
        return cls(
            state_version=version,
            project=project,
            environment=environment,
            serial=data["serial"],
            resources=resources,
        )

    def save(self, path: Path) -> None:
        """Atomically save state without risking truncation of the prior snapshot."""
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = json.dumps(self.to_dict(), indent=2, sort_keys=True)
        temp_name: str | None = None
        try:
            with tempfile.NamedTemporaryFile(
                mode="w",
                encoding="utf-8",
                dir=path.parent,
                prefix=f".{path.name}.",
                suffix=".tmp",
                delete=False,
            ) as handle:
                temp_name = handle.name
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
            Path(temp_name).replace(path)
        except Exception:
            if temp_name:
                Path(temp_name).unlink(missing_ok=True)
            raise

    @classmethod
    def load(
        cls,
        path: Path,
        *,
        expected_project: str | None = None,
        expected_environment: str | None = None,
    ) -> LocalState:
        """Load and validate a persisted state snapshot."""
        try:
            with Path(path).open(encoding="utf-8") as handle:
                data = json.load(handle)
        except json.JSONDecodeError as exc:
            raise StateFormatError(f"state is not valid JSON: {exc}") from exc
        return cls.from_dict(
            data,
            expected_project=expected_project,
            expected_environment=expected_environment,
        )

    def removal_candidates(
        self,
        desired_resource_ids: Iterable[str],
        *,
        comparison_scope: Iterable[str] | None = None,
    ) -> list[RemovalCandidate]:
        """Report prior owned resources absent from desired state.

        ``comparison_scope`` bounds partial/targeted comparisons.  Omitting it
        means the desired IDs represent a full project comparison.  Candidates
        are informational and are never converted to delete actions here.
        """
        desired = self._validated_identity_set(desired_resource_ids)
        scope = (
            set(self.resources)
            if comparison_scope is None
            else self._validated_identity_set(comparison_scope)
        )
        return [
            RemovalCandidate(resource_id=resource_uri, record=self.resources[resource_uri])
            for resource_uri in sorted(set(self.resources) & scope - desired)
        ]

    def _validated_identity_set(self, values: Iterable[str]) -> set[str]:
        result: set[str] = set()
        for value in values:
            identity = ResourceIdentity.parse(value)
            if identity.project != self.project or identity.environment != self.environment:
                raise StateIdentityError(
                    f"resource {identity.uri!r} does not belong to "
                    f"{self.project!r}/{self.environment!r}"
                )
            result.add(identity.uri)
        return result
