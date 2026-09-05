"""Persisted ownership state for safe deployment planning.

This module intentionally does not produce deployment actions.  It records
only resources explicitly managed or adopted by streamt and can report inert
removal candidates for a later, explicit destructive workflow.
"""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
import re
import tempfile
import unicodedata
from collections.abc import Iterable, Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Literal

from streamt.compiler.connector_artifact import parse_compiled_connector_artifact
from streamt.compiler.gateway_artifact import parse_compiled_gateway_rule_artifact
from streamt.compiler.manifest import ArtifactOwnership, ConnectorArtifact
from streamt.deployer.connect import (
    ConnectClusterBinding,
    ConnectorChange,
    is_connect_backend_identity,
)
from streamt.deployer.gateway import (
    is_gateway_backend_identity,
    is_gateway_resource_name,
)

if TYPE_CHECKING:
    from streamt.deployer.planner import DeploymentPlan

CURRENT_STATE_VERSION = 1
LOCAL_STATE_RELATIVE_DIR = Path(".streamt/state")
LOCAL_STATE_CI_WARNING = (
    "Local ownership state is for single-user development only and is unsuitable "
    "for shared CI. Remote state and distributed locking are not yet supported."
)
_CHECKSUM_PATTERN = re.compile(r"^sha256:[0-9a-f]{64}$")
_ENVIRONMENT_PATTERN = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9-]*$")


class StateError(ValueError):
    """Base error for invalid or incompatible persisted state."""


class StateFormatError(StateError):
    """Persisted state is malformed."""


class StateVersionError(StateError):
    """Persisted state uses an unsupported version."""


class StateIdentityError(StateError):
    """Persisted state belongs to a different project or environment."""


class StateConflictError(StateError):
    """Persisted state changed after a caller loaded its prior snapshot."""


class LocalStateOperationLock:
    """Exclusive same-host lock for one environment's complete mutation.

    Mutating commands hold this boundary from their authoritative state read
    through live observation, runtime mutation, and state persistence.  This
    lock is intentionally local-only; it is neither a distributed lock nor a
    durable recovery marker.
    """

    def __init__(self, state_path: Path) -> None:
        self.state_path = Path(state_path)
        self.lock_path = self.state_path.with_name(f".{self.state_path.name}.lock")
        self._fd: int | None = None

    def __enter__(self) -> LocalStateOperationLock:
        if self._fd is not None:
            raise RuntimeError("local state operation lock is already held")
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        lock_fd = os.open(self.lock_path, os.O_CREAT | os.O_RDWR, 0o600)
        try:
            fcntl.flock(lock_fd, fcntl.LOCK_EX)
        except BaseException:
            os.close(lock_fd)
            raise
        self._fd = lock_fd
        return self

    def __exit__(self, *_exc: object) -> None:
        lock_fd = self._fd
        if lock_fd is None:
            return
        self._fd = None
        try:
            fcntl.flock(lock_fd, fcntl.LOCK_UN)
        finally:
            os.close(lock_fd)

    def save_if_serial(
        self,
        state: LocalState,
        *,
        expected_serial: int,
    ) -> None:
        """CAS-save while retaining this operation-wide lock."""
        if self._fd is None:
            raise RuntimeError("local state operation lock is not held")
        state._save_if_serial_locked(
            self.state_path,
            expected_serial=expected_serial,
        )

    @property
    def is_held(self) -> bool:
        """Return whether this process still owns the local lock handle."""
        return self._fd is not None


@contextmanager
def local_state_operation_lock(
    state_path: Path,
) -> Iterator[LocalStateOperationLock]:
    """Acquire the exclusive local mutation boundary for one state address."""
    with LocalStateOperationLock(state_path) as lock:
        yield lock


def _require_segment(value: object, label: str) -> str:
    """Validate one unescaped component of a stable resource identity."""
    if not isinstance(value, str) or not value:
        raise StateFormatError(f"{label} must be a non-empty string")
    if "/" in value:
        raise StateFormatError(f"{label} must not contain '/'")
    return value


def _require_environment(value: object) -> str:
    """Validate an environment name for identity and safe path construction."""
    environment = _require_segment(value, "environment")
    if not _ENVIRONMENT_PATTERN.fullmatch(environment):
        raise StateFormatError(
            "environment must start with an alphanumeric character and contain "
            "only alphanumeric characters or hyphens"
        )
    return environment


@dataclass(frozen=True, order=True)
class ResourceIdentity:
    """Stable logical identity for one runtime resource."""

    project: str
    environment: str
    kind: str
    logical_name: str

    def __post_init__(self) -> None:
        _require_segment(self.project, "project")
        _require_environment(self.environment)
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
class ManagedGatewayResourceDeletion:
    """One exact secret-neutral Gateway ownership-state deletion claim.

    Runtime mutation is authorized elsewhere.  This value only gives the state
    projection boundary enough exact identity to remove the matching prior
    record after that mutation has been validated as successful.
    """

    resource_id: str
    backend_identity: str
    alias_name: str

    def __post_init__(self) -> None:
        if type(self.resource_id) is not str:
            raise StateFormatError(
                "managed Gateway deletion resource_id must be a canonical string"
            )
        try:
            identity = ResourceIdentity.parse(self.resource_id)
        except StateError:
            raise StateFormatError(
                "managed Gateway deletion resource_id must be canonical"
            ) from None
        if identity.kind != "gateway_rule":
            raise StateFormatError(
                "managed Gateway deletion resource_id must identify a gateway_rule"
            )
        if type(self.backend_identity) is not str or not is_gateway_backend_identity(
            self.backend_identity
        ):
            raise StateFormatError("managed Gateway deletion backend_identity must be canonical")
        if type(self.alias_name) is not str or not is_gateway_resource_name(
            self.alias_name
        ):
            raise StateFormatError(
                "managed Gateway deletion alias_name must be a valid Gateway resource name"
            )


@dataclass(frozen=True)
class ManagedConnectorResourceDeletion:
    """One exact secret-neutral Connector ownership-state deletion claim.

    Runtime mutation and durable completion are authorized and proven
    elsewhere.  This value gives state projection only the exact identities
    needed to remove one matching prior managed record.
    """

    resource_id: str
    backend_identity: str
    connector_name: str
    prior_artifact_checksum: str

    def __post_init__(self) -> None:
        if type(self.resource_id) is not str:
            raise StateFormatError(
                "managed Connector deletion resource_id must be a canonical string"
            )
        try:
            identity = ResourceIdentity.parse(self.resource_id)
        except StateError:
            raise StateFormatError(
                "managed Connector deletion resource_id must be canonical"
            ) from None
        if identity.kind != "connector":
            raise StateFormatError(
                "managed Connector deletion resource_id must identify a connector"
            )
        if (
            type(self.backend_identity) is not str
            or not is_connect_backend_identity(self.backend_identity)
        ):
            raise StateFormatError(
                "managed Connector deletion backend_identity must be canonical"
            )
        if (
            type(self.connector_name) is not str
            or not self.connector_name.strip()
            or len(self.connector_name) > 256
            or any(
                unicodedata.category(character) in {"Cc", "Cs"}
                for character in self.connector_name
            )
        ):
            raise StateFormatError(
                "managed Connector deletion connector_name must be valid"
            )
        if (
            type(self.prior_artifact_checksum) is not str
            or not _CHECKSUM_PATTERN.fullmatch(self.prior_artifact_checksum)
        ):
            raise StateFormatError(
                "managed Connector deletion prior_artifact_checksum must be canonical"
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
        _require_environment(self.environment)
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
        environment = _require_environment(data["environment"])
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

    def save_if_serial(self, path: Path, *, expected_serial: int) -> None:
        """Lock, compare the current serial, and atomically save this snapshot."""
        with local_state_operation_lock(path) as operation_lock:
            operation_lock.save_if_serial(
                self,
                expected_serial=expected_serial,
            )

    def _save_if_serial_locked(self, path: Path, *, expected_serial: int) -> None:
        """Compare-and-save while the caller retains the operation lock."""
        if type(expected_serial) is not int or expected_serial < 0:
            raise StateFormatError("expected_serial must be a non-negative integer")
        if self.serial != expected_serial + 1:
            raise StateFormatError(
                "replacement state serial must be exactly expected_serial + 1"
            )

        path = Path(path)
        if path.exists():
            current_serial = LocalState.load(
                path,
                expected_project=self.project,
                expected_environment=self.environment,
            ).serial
        else:
            current_serial = 0
        if current_serial != expected_serial:
            raise StateConflictError(
                f"state serial changed from {expected_serial} to {current_serial}; "
                "reload state and produce a fresh plan"
            )
        self.save(path)

    @classmethod
    def load(
        cls,
        path: Path,
        *,
        expected_project: str | None = None,
        expected_environment: str | None = None,
    ) -> LocalState:
        """Load and validate a persisted state snapshot."""
        def reject_duplicates(pairs: list[tuple[str, object]]) -> dict[str, object]:
            result: dict[str, object] = {}
            for key, value in pairs:
                if key in result:
                    raise StateFormatError(f"state contains duplicate field {key!r}")
                result[key] = value
            return result

        try:
            with Path(path).open(encoding="utf-8") as handle:
                data = json.load(handle, object_pairs_hook=reject_duplicates)
        except StateFormatError:
            raise
        except json.JSONDecodeError as exc:
            raise StateFormatError(f"state is not valid JSON: {exc}") from exc
        except (OSError, UnicodeError) as exc:
            raise StateFormatError(f"cannot read state file {str(path)!r}: {exc}") from exc
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


def local_state_path(project_path: Path, *, environment: str) -> Path:
    """Return the deterministic environment-namespaced state path."""
    validated_environment = _require_environment(environment)
    return Path(project_path) / LOCAL_STATE_RELATIVE_DIR / f"{validated_environment}.json"


def load_local_state(
    project_path: Path,
    *,
    project: str,
    environment: str,
) -> LocalState:
    """Load strict local state, or return a new in-memory serial-zero snapshot."""
    path = local_state_path(project_path, environment=environment)
    if not path.exists():
        return LocalState(project=project, environment=environment)
    return LocalState.load(
        path,
        expected_project=project,
        expected_environment=environment,
    )


def _add_desired_record(
    resources: dict[str, ManagedResourceRecord],
    *,
    project: str,
    environment: str,
    kind: str,
    physical_name: str,
    backend: str,
    artifact: object,
    blocked_resource_ids: set[str],
) -> None:
    """Add one explicitly owned, successfully plannable desired artifact."""
    if artifact is None or not hasattr(artifact, "to_dict"):
        return
    raw_artifact = artifact.to_dict()
    if not isinstance(raw_artifact, dict):
        raise StateFormatError(f"desired {kind} artifact must serialize to an object")
    ownership = ArtifactOwnership.from_dict(raw_artifact.get("ownership"))
    if (
        ownership is None
        or ownership.project != project
        or ownership.mode not in ("managed", "adopted")
    ):
        return

    identity = resource_id(project, environment, kind, ownership.owner_name)
    if identity in blocked_resource_ids:
        return
    record = ManagedResourceRecord(
        physical_name=physical_name,
        ownership=ownership.mode,  # type: ignore[arg-type]
        artifact_checksum=artifact_checksum(raw_artifact),
        backend=backend,
    )
    existing = resources.get(identity)
    if existing is not None and existing != record:
        raise StateFormatError(
            f"multiple desired resources resolve to stable identity {identity!r}"
        )
    resources[identity] = record


def desired_managed_records(
    plan: DeploymentPlan,
    *,
    project: str,
    environment: str,
) -> dict[str, ManagedResourceRecord]:
    """Build records only for explicitly owned desired resources in a live plan."""
    resources: dict[str, ManagedResourceRecord] = {}
    blocked_resource_ids = {
        requirement.resource_id
        for requirement in getattr(plan, "ownership_requirements", [])
    }

    from streamt.compiler.manifest import parse_compiled_kafka_streams_job_artifact
    from streamt.deployer.kafka_streams import KafkaStreamsJobChange

    for runner_change in getattr(plan, "kafka_streams_changes", []):
        if type(runner_change) is not KafkaStreamsJobChange or runner_change.desired is None:
            raise StateFormatError("Kafka Streams state projection requires an exact desired job change")
        desired = parse_compiled_kafka_streams_job_artifact(runner_change.desired.to_dict())
        ownership = ArtifactOwnership.from_dict(desired.ownership)
        if ownership is None or ownership.project != project:
            raise StateIdentityError("Kafka Streams state projection has foreign or invalid ownership")
        if ownership.mode == "external":
            if runner_change.action != "none" or runner_change.current is not None or runner_change.backend_identity is not None:
                raise StateFormatError("External Kafka Streams state projection must be declaration-only")
            continue
        backend = runner_change.backend_identity
        if type(backend) is not str or re.fullmatch(r"kafka-streams-docker:v1:[0-9a-f]{64}", backend) is None:
            raise StateFormatError("Kafka Streams state projection requires an exact Docker/Kafka backend identity")
        _add_desired_record(
            resources, project=project, environment=environment, kind="kafka_streams_job",
            physical_name=desired.application_id, backend=backend, artifact=desired,
            blocked_resource_ids=blocked_resource_ids,
        )

    for schema_change in getattr(plan, "schema_changes", []):
        _add_desired_record(
            resources,
            project=project,
            environment=environment,
            kind="schema",
            physical_name=schema_change.subject,
            backend="schema-registry",
            artifact=schema_change.desired,
            blocked_resource_ids=blocked_resource_ids,
        )
    for topic_change in getattr(plan, "topic_changes", []):
        _add_desired_record(
            resources,
            project=project,
            environment=environment,
            kind="topic",
            physical_name=topic_change.topic,
            backend="direct-kafka",
            artifact=topic_change.desired,
            blocked_resource_ids=blocked_resource_ids,
        )
    for flink_change in getattr(plan, "flink_changes", []):
        _add_desired_record(
            resources,
            project=project,
            environment=environment,
            kind="flink_job",
            physical_name=flink_change.job_name,
            backend="flink",
            artifact=flink_change.desired,
            blocked_resource_ids=blocked_resource_ids,
        )
    for connector_change in getattr(plan, "connector_changes", []):
        if connector_change.desired is None:
            continue
        backend_identity = getattr(connector_change, "backend_identity", None)
        if not isinstance(backend_identity, str) or not is_connect_backend_identity(
            backend_identity
        ):
            raise StateFormatError(
                "desired connector change requires a canonical Connect backend identity"
            )
        _add_desired_record(
            resources,
            project=project,
            environment=environment,
            kind="connector",
            physical_name=connector_change.connector_name,
            backend=backend_identity,
            artifact=connector_change.desired,
            blocked_resource_ids=blocked_resource_ids,
        )
    for gateway_change in getattr(plan, "gateway_changes", []):
        desired = gateway_change.desired
        if desired is None:
            continue
        backend_identity = getattr(gateway_change, "backend_identity", None)
        if (
            gateway_change.action == "none"
            and gateway_change.current is None
            and backend_identity is None
        ):
            # Declaration-only Gateway entries have no observed managed surface.
            # Validate the artifact before exempting it from backend evidence;
            # foreign, malformed and actionable entries keep the strict path.
            external_desired = parse_compiled_gateway_rule_artifact(desired.to_dict())
            ownership = ArtifactOwnership.from_dict(external_desired.ownership)
            if (
                ownership is not None
                and ownership.project == project
                and ownership.mode == "external"
            ):
                continue
        if not isinstance(backend_identity, str):
            raise StateFormatError(
                "desired Gateway change requires a strict artifact and backend identity"
            )
        if not is_gateway_backend_identity(backend_identity):
            raise StateFormatError(
                "desired Gateway change requires a canonical Gateway backend identity"
            )
        parsed_desired = parse_compiled_gateway_rule_artifact(desired.to_dict())
        _add_desired_record(
            resources,
            project=project,
            environment=environment,
            kind="gateway_rule",
            physical_name=parsed_desired.virtual_topic,
            backend=backend_identity,
            artifact=parsed_desired,
            blocked_resource_ids=blocked_resource_ids,
        )
    return resources


def updated_local_state(
    prior_state: LocalState,
    plan: DeploymentPlan,
    *,
    managed_gateway_deletions: tuple[ManagedGatewayResourceDeletion, ...] = (),
    managed_connector_deletions: tuple[ManagedConnectorResourceDeletion, ...] = (),
) -> LocalState | None:
    """Return serial+1 state when desired owned records changed, else ``None``.

    Prior records absent from the desired plan are intentionally retained. This
    helper never infers deletion or ownership relinquishment from absence.  It
    removes a Gateway record only when the caller supplies an exact explicit
    deletion claim that matches the prior state and does not conflict with any
    desired claim. Connector claims additionally bind the prior artifact
    checksum and managed ownership mode.
    """
    if type(managed_gateway_deletions) is not tuple:
        raise StateFormatError("managed Gateway deletions must be an exact tuple")
    if any(
        type(deletion) is not ManagedGatewayResourceDeletion
        for deletion in managed_gateway_deletions
    ):
        raise StateFormatError("managed Gateway deletions must contain exact deletion values")
    if type(managed_connector_deletions) is not tuple:
        raise StateFormatError("managed Connector deletions must be an exact tuple")
    if any(
        type(deletion) is not ManagedConnectorResourceDeletion
        for deletion in managed_connector_deletions
    ):
        raise StateFormatError(
            "managed Connector deletions must contain exact deletion values"
        )

    desired = desired_managed_records(
        plan,
        project=prior_state.project,
        environment=prior_state.environment,
    )
    resources = dict(prior_state.resources)
    changed = False

    deletion_resource_ids: set[str] = set()
    deletion_provider_ids: set[tuple[str, str]] = set()
    prior_provider_owners: dict[tuple[str, str], list[str]] = {}
    desired_provider_owners: dict[tuple[str, str], list[str]] = {}
    connector_deletion_resource_ids: set[str] = set()
    connector_deletion_provider_ids: set[tuple[str, str]] = set()
    prior_connector_provider_owners: dict[tuple[str, str], list[str]] = {}
    desired_connector_provider_owners: dict[tuple[str, str], list[str]] = {}
    for resource_uri, record in prior_state.resources.items():
        resource_kind = ResourceIdentity.parse(resource_uri).kind
        if resource_kind == "gateway_rule":
            prior_provider_owners.setdefault(
                (record.backend, record.physical_name),
                [],
            ).append(resource_uri)
        elif resource_kind == "connector" and managed_connector_deletions:
            if (
                type(resource_uri) is not str
                or type(record) is not ManagedResourceRecord
                or type(record.physical_name) is not str
                or not record.physical_name.strip()
                or len(record.physical_name) > 256
                or any(
                    unicodedata.category(character) in {"Cc", "Cs"}
                    for character in record.physical_name
                )
                or type(record.ownership) is not str
                or record.ownership not in ("managed", "adopted")
                or type(record.artifact_checksum) is not str
                or not _CHECKSUM_PATTERN.fullmatch(record.artifact_checksum)
                or type(record.backend) is not str
                or not is_connect_backend_identity(record.backend)
            ):
                raise StateIdentityError(
                    "prior Connector state contains invalid exact identity evidence"
                )
            binding = ConnectClusterBinding.from_backend_identity(record.backend)
            prior_connector_provider_owners.setdefault(
                (binding.endpoint_fingerprint, record.physical_name),
                [],
            ).append(resource_uri)
    for resource_uri, record in desired.items():
        resource_kind = ResourceIdentity.parse(resource_uri).kind
        if resource_kind == "gateway_rule":
            desired_provider_owners.setdefault(
                (record.backend, record.physical_name),
                [],
            ).append(resource_uri)

    if managed_connector_deletions:
        connector_changes = getattr(plan, "connector_changes", None)
        if type(connector_changes) is not list:
            raise StateFormatError(
                "Connector deletion projection requires an exact desired change collection"
            )
        for connector_change in connector_changes:
            if type(connector_change) is not ConnectorChange:
                raise StateFormatError(
                    "Connector deletion projection contains an invalid desired change"
                )
            desired_connector = connector_change.desired
            if desired_connector is None:
                continue
            if type(desired_connector) is not ConnectorArtifact:
                raise StateFormatError(
                    "Connector deletion projection contains an invalid desired artifact"
                )
            try:
                parsed_desired = parse_compiled_connector_artifact(
                    desired_connector.to_dict()
                )
                desired_ownership = ArtifactOwnership.from_dict(
                    parsed_desired.ownership
                )
                desired_binding = ConnectClusterBinding.from_backend_identity(
                    connector_change.backend_identity
                )
                logical_owner = (
                    parsed_desired.name
                    if desired_ownership is None
                    else desired_ownership.owner_name
                )
                desired_resource_uri = resource_id(
                    prior_state.project,
                    prior_state.environment,
                    "connector",
                    logical_owner,
                )
            except Exception:
                raise StateFormatError(
                    "Connector deletion projection contains an invalid desired claim"
                ) from None
            if (
                type(connector_change.connector_name) is not str
                or type(parsed_desired.name) is not str
                or connector_change.connector_name != parsed_desired.name
                or type(parsed_desired.cluster) is not str
                or parsed_desired.cluster != desired_binding.cluster_alias
                or (
                    desired_ownership is not None
                    and (
                        type(desired_ownership.project) is not str
                        or type(desired_ownership.owner_type) is not str
                        or type(desired_ownership.owner_name) is not str
                        or type(desired_ownership.mode) is not str
                    )
                )
            ):
                raise StateFormatError(
                    "Connector deletion projection contains an invalid desired claim"
                )
            desired_connector_provider_owners.setdefault(
                (desired_binding.endpoint_fingerprint, parsed_desired.name),
                [],
            ).append(desired_resource_uri)

    for gateway_deletion in managed_gateway_deletions:
        deletion_identity = ResourceIdentity.parse(gateway_deletion.resource_id)
        if (
            deletion_identity.project != prior_state.project
            or deletion_identity.environment != prior_state.environment
        ):
            raise StateIdentityError(
                "managed Gateway deletion does not belong to the current state"
            )
        if gateway_deletion.resource_id in deletion_resource_ids:
            raise StateIdentityError(
                "managed Gateway deletions contain a duplicate resource identity"
            )
        deletion_resource_ids.add(gateway_deletion.resource_id)

        provider_id = (gateway_deletion.backend_identity, gateway_deletion.alias_name)
        if provider_id in deletion_provider_ids:
            raise StateIdentityError(
                "managed Gateway deletions contain a duplicate provider identity"
            )
        deletion_provider_ids.add(provider_id)

        prior_record = prior_state.resources.get(gateway_deletion.resource_id)
        if (
            prior_record is None
            or prior_record.backend != gateway_deletion.backend_identity
            or prior_record.physical_name != gateway_deletion.alias_name
            or prior_provider_owners.get(provider_id) != [gateway_deletion.resource_id]
        ):
            raise StateIdentityError(
                "managed Gateway deletion does not match one exact prior-state record"
            )
        if (
            gateway_deletion.resource_id in desired
            or provider_id in desired_provider_owners
        ):
            raise StateIdentityError(
                "managed Gateway deletion conflicts with a desired resource claim"
            )

    for connector_deletion in managed_connector_deletions:
        deletion_identity = ResourceIdentity.parse(connector_deletion.resource_id)
        if (
            deletion_identity.project != prior_state.project
            or deletion_identity.environment != prior_state.environment
        ):
            raise StateIdentityError(
                "managed Connector deletion does not belong to the current state"
            )
        if connector_deletion.resource_id in connector_deletion_resource_ids:
            raise StateIdentityError(
                "managed Connector deletions contain a duplicate resource identity"
            )
        connector_deletion_resource_ids.add(connector_deletion.resource_id)

        deletion_binding = ConnectClusterBinding.from_backend_identity(
            connector_deletion.backend_identity
        )
        provider_id = (
            deletion_binding.endpoint_fingerprint,
            connector_deletion.connector_name,
        )
        if provider_id in connector_deletion_provider_ids:
            raise StateIdentityError(
                "managed Connector deletions contain a duplicate provider identity"
            )
        connector_deletion_provider_ids.add(provider_id)

        prior_record = prior_state.resources.get(connector_deletion.resource_id)
        if (
            type(prior_record) is not ManagedResourceRecord
            or type(prior_record.ownership) is not str
            or type(prior_record.backend) is not str
            or type(prior_record.physical_name) is not str
            or type(prior_record.artifact_checksum) is not str
            or prior_record.ownership != "managed"
            or prior_record.backend != connector_deletion.backend_identity
            or prior_record.physical_name != connector_deletion.connector_name
            or prior_record.artifact_checksum
            != connector_deletion.prior_artifact_checksum
            or prior_connector_provider_owners.get(provider_id)
            != [connector_deletion.resource_id]
        ):
            raise StateIdentityError(
                "managed Connector deletion does not match one exact prior managed record"
            )
        if (
            connector_deletion.resource_id in desired
            or provider_id in desired_connector_provider_owners
        ):
            raise StateIdentityError(
                "managed Connector deletion conflicts with a desired resource claim"
            )

    for resource_uri, record in desired.items():
        if resources.get(resource_uri) != record:
            resources[resource_uri] = record
            changed = True
    for gateway_deletion in managed_gateway_deletions:
        del resources[gateway_deletion.resource_id]
        changed = True
    for connector_deletion in managed_connector_deletions:
        del resources[connector_deletion.resource_id]
        changed = True
    if not changed:
        return None
    return LocalState(
        project=prior_state.project,
        environment=prior_state.environment,
        serial=prior_state.serial + 1,
        resources=resources,
    )
