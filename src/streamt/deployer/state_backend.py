"""Provider-neutral access to deployment ownership state.

This module keeps provider selection behind one application boundary without
changing the local persistence format or exposing provider handles to CLI
commands.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat
import tempfile
import unicodedata
import uuid
from collections.abc import Iterator
from contextlib import AbstractContextManager, contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Literal, Protocol, cast, overload, runtime_checkable

from streamt.core.deployment_state import (
    DeploymentStateConfig,
    PostgresDeploymentStateConfig,
)
from streamt.deployer.kafka_streams_evidence import (
    KAFKA_STREAMS_CHECKPOINT_PHASES,
    KAFKA_STREAMS_CONTROL_VERSION,
    KafkaStreamsActionEvidence,
    KafkaStreamsCheckpointEvidence,
)
from streamt.deployer.state import (
    LocalState,
    LocalStateOperationLock,
    ResourceIdentity,
    StateConflictError,
    StateError,
    StateFormatError,
    StateIdentityError,
    local_state_operation_lock,
    local_state_path,
)

if TYPE_CHECKING:
    from streamt.deployer.recovery import (
        RecoveryResolutionRecord,
        RecoverySnapshotEvidence,
    )

LOCAL_STATE_NAMESPACE = "local"
ABSENT_STATE_REVISION = "ABSENT"
CURRENT_CONTROL_VERSION = 3
CURRENT_RECOVERY_HISTORY_VERSION = 1
CURRENT_RECOVERY_HISTORY_EVENT_VERSION = 1
MAX_LOCAL_RECOVERY_HISTORY_BYTES = 1024 * 1024
MAX_LOCAL_RECOVERY_HISTORY_EVENTS = 4096
_BACKEND_KIND_PATTERN = re.compile(r"^[a-z][a-z0-9_-]*$")
_CHECKSUM_PATTERN = re.compile(r"^sha256:[0-9a-f]{64}$")
_ACTION_PATTERN = re.compile(r"^[a-z][a-z0-9_-]*$")
_TIMESTAMP_PATTERN = re.compile(
    r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?Z$"
)
_CREDENTIAL_URL = re.compile(r"://([^:@/\s]+):([^@/\s]+)@")
_POSTGRES_URL = re.compile(r"\bpostgres(?:ql)?://", re.IGNORECASE)
_INLINE_SECRET = re.compile(
    r"(?:password|passwd|secret|token|api[_-]?key|authorization|bearer)"
    r"\s*[=:]\s*\S+",
    re.IGNORECASE,
)
# Unrelated operations deliberately keep their existing v3 wire format. Only
# evidenced runner replacements opt into v4; old readers then fail closed.
_CONTROL_VERSIONS = (1, 2, 3, KAFKA_STREAMS_CONTROL_VERSION)


class StateBackendError(StateError):
    """Base class for stable, sanitized state-provider failures."""


class StateBackendUnavailableError(StateBackendError):
    """The selected backend could not be reached or used."""


class StateBackendInvalidStateError(StateBackendError, StateFormatError):
    """The provider returned state that violates the strict state contract."""


class StateBackendConflictError(StateBackendError, StateConflictError):
    """A state observation is no longer current for compare-and-swap."""


class StateBackendLockTimeoutError(StateBackendError):
    """Exclusive state operation ownership was not acquired in time."""


class StateBackendLockLostError(StateBackendError):
    """The caller no longer owns the state operation lock."""

    def __init__(self, message: str, *, operation_id: str | None = None) -> None:
        super().__init__(message)
        self.operation_id = operation_id


class StateBackendRecoveryRequiredError(StateBackendError):
    """An unfinished operation must be reconciled before mutation."""


class StateBackendUnknownCommitError(StateBackendError):
    """The backend cannot prove whether a state transition committed."""

    def __init__(self, message: str, *, operation_id: str | None = None) -> None:
        super().__init__(message)
        self.operation_id = operation_id


class StateBackendReleaseAfterCommitError(StateBackendError):
    """A verified commit succeeded, but operation authority release failed.

    Callers must report the committed outcome and must not suggest replaying the
    mutation. Providers should keep implementation details in the exception
    cause and expose only a sanitized message here.
    """

    committed: Literal[True] = True

    def __init__(self, message: str, *, operation_id: str | None = None) -> None:
        super().__init__(message)
        self.operation_id = operation_id


def _require_address_segment(value: object, label: str) -> str:
    if not isinstance(value, str) or not value:
        raise StateFormatError(f"state address {label} must be a non-empty string")
    if "/" in value:
        raise StateFormatError(f"state address {label} must not contain '/'")
    return value


@dataclass(frozen=True, order=True)
class StateAddress:
    """Canonical namespace, project, and environment state address."""

    namespace: str
    project: str
    environment: str

    def __post_init__(self) -> None:
        _require_address_segment(self.namespace, "namespace")
        # LocalState remains the strict source of project/environment rules.
        LocalState(project=self.project, environment=self.environment)

    @property
    def uri(self) -> str:
        return (
            f"streamt-state://{self.namespace}/{self.project}/"
            f"{self.environment}"
        )

    @classmethod
    def parse(cls, value: object) -> StateAddress:
        if not isinstance(value, str) or not value.startswith("streamt-state://"):
            raise StateFormatError(
                "state address must start with 'streamt-state://'"
            )
        parts = value.removeprefix("streamt-state://").split("/")
        if len(parts) != 3:
            raise StateFormatError(
                "state address must be "
                "streamt-state://<namespace>/<project>/<environment>"
            )
        address = cls(*parts)
        if address.uri != value:
            raise StateFormatError(f"state address is not canonical: {value!r}")
        return address


@dataclass(frozen=True)
class StateStoreIdentity:
    """Safe immutable identity for one state provider instance."""

    backend: str
    store_id: str

    def __post_init__(self) -> None:
        if not isinstance(self.backend, str) or not _BACKEND_KIND_PATTERN.fullmatch(
            self.backend
        ):
            raise StateFormatError(
                "state backend must be a lowercase backend identifier"
            )
        if not isinstance(self.store_id, str):
            raise StateFormatError("state store_id must be a canonical UUID")
        try:
            parsed_store_id = uuid.UUID(self.store_id)
        except (ValueError, AttributeError) as error:
            raise StateFormatError(
                "state store_id must be a canonical UUID"
            ) from error
        if str(parsed_store_id) != self.store_id:
            raise StateFormatError("state store_id must be a canonical UUID")


@dataclass(frozen=True)
class StateRevision:
    """Opaque compare-and-swap revision meaningful only to its backend."""

    value: str

    def __post_init__(self) -> None:
        if not isinstance(self.value, str) or not self.value:
            raise StateFormatError("state revision must be a non-empty string")

    @property
    def is_absent(self) -> bool:
        return self.value == ABSENT_STATE_REVISION

    @classmethod
    def absent(cls) -> StateRevision:
        return cls(ABSENT_STATE_REVISION)


@dataclass(frozen=True)
class StateObservation:
    """One consistent ownership-state observation and its CAS evidence."""

    store: StateStoreIdentity
    address: StateAddress
    state: LocalState
    revision: StateRevision

    def __post_init__(self) -> None:
        if (
            self.state.project != self.address.project
            or self.state.environment != self.address.environment
        ):
            raise StateIdentityError(
                "observed state identity does not match its canonical address"
            )
        if self.revision.is_absent and (
            self.state.serial != 0 or self.state.resources
        ):
            raise StateFormatError(
                "an absent state revision requires an empty serial-zero state"
            )

    @property
    def state_serial(self) -> int:
        return self.state.serial


def _require_uuid(value: object, label: str) -> str:
    if not isinstance(value, str):
        raise StateFormatError(f"{label} must be a canonical UUID")
    try:
        parsed = uuid.UUID(value)
    except (ValueError, AttributeError) as error:
        raise StateFormatError(f"{label} must be a canonical UUID") from error
    if str(parsed) != value:
        raise StateFormatError(f"{label} must be a canonical UUID")
    return value


def _require_checksum(value: object, label: str) -> str:
    if not isinstance(value, str) or not _CHECKSUM_PATTERN.fullmatch(value):
        raise StateFormatError(
            f"{label} must be a sha256:<64 lowercase hex> value"
        )
    return value


def _require_timestamp(value: object, label: str) -> str:
    if not isinstance(value, str) or not _TIMESTAMP_PATTERN.fullmatch(value):
        raise StateFormatError(f"{label} must be a UTC ISO-8601 timestamp")
    try:
        datetime.fromisoformat(value.removesuffix("Z") + "+00:00")
    except ValueError as error:
        raise StateFormatError(f"{label} must be a UTC ISO-8601 timestamp") from error
    return value


def _require_safe_text(value: object, label: str, *, maximum: int = 512) -> str:
    if (
        not isinstance(value, str)
        or not value
        or len(value) > maximum
        or any(ord(character) < 32 for character in value)
    ):
        raise StateFormatError(
            f"{label} must be a non-empty control-character-free string up to "
            f"{maximum} characters"
        )
    return value


@dataclass(frozen=True)
class GatewayActionSurfaceEvidence:
    """One exact, secret-neutral normalized Gateway aggregate surface."""

    exists: bool
    fingerprint: str
    managed_interceptor_count: int

    def __post_init__(self) -> None:
        if type(self.exists) is not bool:
            raise StateFormatError("Gateway action surface exists must be a boolean")
        _require_checksum(
            self.fingerprint,
            "Gateway action surface fingerprint",
        )
        if (
            type(self.managed_interceptor_count) is not int
            or self.managed_interceptor_count < 0
        ):
            raise StateFormatError(
                "Gateway action surface managed_interceptor_count must be a "
                "non-negative integer"
            )
        if not self.exists and self.managed_interceptor_count != 0:
            raise StateFormatError(
                "An absent Gateway action surface must have zero managed interceptors"
            )

    def to_dict(self) -> dict[str, object]:
        return {
            "exists": self.exists,
            "fingerprint": self.fingerprint,
            "managed_interceptor_count": self.managed_interceptor_count,
        }

    @classmethod
    def from_dict(cls, value: object) -> GatewayActionSurfaceEvidence:
        data = _strict_object(
            value,
            label="Gateway action surface evidence",
            expected={"exists", "fingerprint", "managed_interceptor_count"},
        )
        return cls(
            exists=cast(bool, data["exists"]),
            fingerprint=cast(str, data["fingerprint"]),
            managed_interceptor_count=cast(
                int,
                data["managed_interceptor_count"],
            ),
        )


@dataclass(frozen=True)
class GatewayActionEvidence:
    """Exact Gateway preimage and candidate evidence for one durable action."""

    version: int
    backend_identity: str
    rule_name: str
    alias_name: str
    current: GatewayActionSurfaceEvidence
    desired: GatewayActionSurfaceEvidence

    def __post_init__(self) -> None:
        # Keep the provider-specific import out of module initialization. This
        # value remains secret-neutral JSON while using Gateway's canonical
        # backend-identity contract.
        from streamt.deployer.gateway import (
            is_gateway_backend_identity,
            is_gateway_resource_name,
            managed_gateway_absence_fingerprint,
        )

        if type(self.version) is not int or self.version != 1:
            raise StateFormatError("Unsupported Gateway action evidence version")
        if not is_gateway_backend_identity(self.backend_identity):
            raise StateFormatError(
                "Gateway action evidence backend_identity must be canonical"
            )
        for value, label in (
            (self.rule_name, "rule_name"),
            (self.alias_name, "alias_name"),
        ):
            if not is_gateway_resource_name(value):
                raise StateFormatError(f"Gateway action evidence {label} is invalid")
        if type(self.current) is not GatewayActionSurfaceEvidence or type(
            self.desired
        ) is not GatewayActionSurfaceEvidence:
            raise StateFormatError(
                "Gateway action evidence requires exact current and desired surfaces"
            )
        absence_fingerprint = managed_gateway_absence_fingerprint(
            self.backend_identity,
            self.rule_name,
            self.alias_name,
        )
        if any(
            (not surface.exists and surface.fingerprint != absence_fingerprint)
            or (surface.exists and surface.fingerprint == absence_fingerprint)
            for surface in (self.current, self.desired)
        ):
            raise StateFormatError(
                "Gateway action evidence contains an incoherent absence fingerprint"
            )

    def to_dict(self) -> dict[str, object]:
        return {
            "version": self.version,
            "backend_identity": self.backend_identity,
            "rule_name": self.rule_name,
            "alias_name": self.alias_name,
            "current": self.current.to_dict(),
            "desired": self.desired.to_dict(),
        }

    @classmethod
    def from_dict(cls, value: object) -> GatewayActionEvidence:
        data = _strict_object(
            value,
            label="Gateway action evidence",
            expected={
                "version",
                "backend_identity",
                "rule_name",
                "alias_name",
                "current",
                "desired",
            },
        )
        return cls(
            version=cast(int, data["version"]),
            backend_identity=cast(str, data["backend_identity"]),
            rule_name=cast(str, data["rule_name"]),
            alias_name=cast(str, data["alias_name"]),
            current=GatewayActionSurfaceEvidence.from_dict(data["current"]),
            desired=GatewayActionSurfaceEvidence.from_dict(data["desired"]),
        )


@dataclass(frozen=True)
class ConnectorActionSurfaceEvidence:
    """One exact, secret-neutral Kafka Connect presence surface."""

    exists: bool
    fingerprint: str

    def __post_init__(self) -> None:
        if type(self.exists) is not bool:
            raise StateFormatError("Connector action surface exists must be a boolean")
        _require_checksum(
            self.fingerprint,
            "Connector action surface fingerprint",
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "exists": self.exists,
            "fingerprint": self.fingerprint,
        }

    @classmethod
    def from_dict(cls, value: object) -> ConnectorActionSurfaceEvidence:
        data = _strict_object(
            value,
            label="Connector action surface evidence",
            expected={"exists", "fingerprint"},
        )
        return cls(
            exists=cast(bool, data["exists"]),
            fingerprint=cast(str, data["fingerprint"]),
        )


@dataclass(frozen=True)
class ConnectorActionEvidence:
    """Exact secret-neutral evidence for one managed Connector deletion."""

    version: int
    backend_identity: str
    connector_name: str
    prior_artifact_checksum: str
    current: ConnectorActionSurfaceEvidence
    desired: ConnectorActionSurfaceEvidence

    def __post_init__(self) -> None:
        from streamt.deployer.connect import (
            ConnectClusterBindingError,
            ConnectManagedObservationError,
            is_connect_backend_identity,
            managed_connector_absence_fingerprint,
        )

        if type(self.version) is not int or self.version != 1:
            raise StateFormatError("Unsupported Connector action evidence version")
        if not is_connect_backend_identity(self.backend_identity):
            raise StateFormatError("Connector action evidence backend_identity must be canonical")
        if (
            type(self.connector_name) is not str
            or not self.connector_name.strip()
            or len(self.connector_name) > 256
            or any(
                unicodedata.category(character) in {"Cc", "Cs"} for character in self.connector_name
            )
            or _CREDENTIAL_URL.search(self.connector_name)
            or _POSTGRES_URL.search(self.connector_name)
            or _INLINE_SECRET.search(self.connector_name)
        ):
            raise StateFormatError("Connector action evidence connector_name is invalid")
        _require_checksum(
            self.prior_artifact_checksum,
            "Connector action evidence prior_artifact_checksum",
        )
        if (
            type(self.current) is not ConnectorActionSurfaceEvidence
            or type(self.desired) is not ConnectorActionSurfaceEvidence
        ):
            raise StateFormatError(
                "Connector action evidence requires exact current and desired surfaces"
            )
        try:
            absence_fingerprint = managed_connector_absence_fingerprint(
                self.backend_identity,
                self.connector_name,
            )
        except (ConnectClusterBindingError, ConnectManagedObservationError):
            raise StateFormatError(
                "Connector action evidence contains an invalid canonical identity"
            ) from None
        if (
            not self.current.exists
            or self.current.fingerprint == absence_fingerprint
            or self.desired.exists
            or self.desired.fingerprint != absence_fingerprint
            or self.current.fingerprint == self.desired.fingerprint
        ):
            raise StateFormatError(
                "Connector action evidence must describe exact present-to-absent surfaces"
            )

    def to_dict(self) -> dict[str, object]:
        return {
            "version": self.version,
            "backend_identity": self.backend_identity,
            "connector_name": self.connector_name,
            "prior_artifact_checksum": self.prior_artifact_checksum,
            "current": self.current.to_dict(),
            "desired": self.desired.to_dict(),
        }

    @classmethod
    def from_dict(cls, value: object) -> ConnectorActionEvidence:
        data = _strict_object(
            value,
            label="Connector action evidence",
            expected={
                "version",
                "backend_identity",
                "connector_name",
                "prior_artifact_checksum",
                "current",
                "desired",
            },
        )
        return cls(
            version=cast(int, data["version"]),
            backend_identity=cast(str, data["backend_identity"]),
            connector_name=cast(str, data["connector_name"]),
            prior_artifact_checksum=cast(str, data["prior_artifact_checksum"]),
            current=ConnectorActionSurfaceEvidence.from_dict(data["current"]),
            desired=ConnectorActionSurfaceEvidence.from_dict(data["desired"]),
        )


@dataclass(frozen=True)
class OperationAction:
    """One ordered runtime or state action covered by an operation intent."""

    index: int
    resource_id: str
    action: str
    gateway_evidence: GatewayActionEvidence | None = None
    connector_evidence: ConnectorActionEvidence | None = None
    kafka_streams_evidence: KafkaStreamsActionEvidence | None = None
    _wire_version: int = field(
        default=CURRENT_CONTROL_VERSION,
        init=False,
        repr=False,
        compare=False,
    )

    def __post_init__(self) -> None:
        if type(self.index) is not int or self.index < 0:
            raise StateFormatError("operation action index must be a non-negative integer")
        _require_safe_text(self.resource_id, "operation action resource_id")
        if not isinstance(self.action, str) or not _ACTION_PATTERN.fullmatch(self.action):
            raise StateFormatError("operation action must be a lowercase action identifier")

        gateway_evidence = self.gateway_evidence
        connector_evidence = self.connector_evidence
        kafka_streams_evidence = self.kafka_streams_evidence
        if sum(item is not None for item in (gateway_evidence, connector_evidence, kafka_streams_evidence)) > 1:
            raise StateFormatError(
                "operation action Gateway, Connector and Kafka Streams evidence are mutually exclusive"
            )
        if gateway_evidence is not None and type(gateway_evidence) is not GatewayActionEvidence:
            raise StateFormatError("operation action Gateway evidence is invalid")
        if (
            connector_evidence is not None
            and type(connector_evidence) is not ConnectorActionEvidence
        ):
            raise StateFormatError("operation action Connector evidence is invalid")
        if kafka_streams_evidence is not None and type(kafka_streams_evidence) is not KafkaStreamsActionEvidence:
            raise StateFormatError("operation action Kafka Streams evidence is invalid")
        if gateway_evidence is None and connector_evidence is None and kafka_streams_evidence is None:
            try:
                identity = ResourceIdentity.parse(self.resource_id)
            except StateFormatError:
                return
            if identity.kind == "connector" and self.action == "delete":
                raise StateFormatError("Connector deletion requires exact action evidence")
            if identity.kind == "kafka_streams_job" and self.action == "update":
                raise StateFormatError("Kafka Streams replacement requires exact action evidence")
            return
        try:
            identity = ResourceIdentity.parse(self.resource_id)
        except StateFormatError as error:
            raise StateFormatError(
                "evidenced operation action requires a canonical resource identity"
            ) from error
        if gateway_evidence is not None and identity.kind != "gateway_rule":
            raise StateFormatError(
                "Gateway action evidence is allowed only for gateway_rule resources"
            )
        if gateway_evidence is not None:
            current = gateway_evidence.current
            desired = gateway_evidence.desired
            valid_transition = (
                self.action == "adopt"
                and current.exists
                and desired.exists
                and current.managed_interceptor_count == 0
                and desired.managed_interceptor_count == 0
            ) or (
                current.fingerprint != desired.fingerprint
                and (
                    (self.action == "create" and not current.exists and desired.exists)
                    or (self.action == "update" and current.exists and desired.exists)
                    or (self.action == "delete" and current.exists and not desired.exists)
                )
            )
            if not valid_transition:
                raise StateFormatError(
                    "Gateway action evidence does not match the action transition"
                )
        if connector_evidence is not None and (
            identity.kind != "connector" or self.action != "delete"
        ):
            raise StateFormatError(
                "Connector action evidence is allowed only for connector delete actions"
            )
        if kafka_streams_evidence is not None:
            from streamt.compiler.kafka_streams import application_id

            artifact = kafka_streams_evidence.prior_artifact.artifact
            ownership = cast(dict[str, str], artifact.to_dict()["ownership"])
            if (
                identity.kind != "kafka_streams_job" or self.action != "update"
                or identity.logical_name != artifact.name or identity.project != ownership["project"]
                or artifact.application_id != application_id(identity.project, identity.environment, identity.logical_name)
            ):
                raise StateFormatError("Kafka Streams action evidence requires the exact replacement resource identity")
            object.__setattr__(self, "_wire_version", KAFKA_STREAMS_CONTROL_VERSION)

    def _is_connector_delete(self) -> bool:
        try:
            identity = ResourceIdentity.parse(self.resource_id)
        except StateFormatError:
            return False
        return identity.kind == "connector" and self.action == "delete"

    def to_dict(self, *, control_version: int | None = None) -> dict[str, object]:
        version = self._wire_version if control_version is None else control_version
        if type(version) is not int or version not in _CONTROL_VERSIONS:
            raise StateFormatError("operation action control version is unsupported")
        if version < KAFKA_STREAMS_CONTROL_VERSION and self.kafka_streams_evidence is not None:
            raise StateFormatError("Kafka Streams replacement evidence requires control version 4")
        if version == 1:
            if self.gateway_evidence is not None or self.connector_evidence is not None:
                raise StateFormatError("control version 1 cannot contain action evidence")
            if self._is_connector_delete():
                raise StateFormatError("control version 1 cannot authorize Connector deletion")
            return {
                "index": self.index,
                "resource_id": self.resource_id,
                "action": self.action,
            }
        if version == 2:
            if self.connector_evidence is not None or self._is_connector_delete():
                raise StateFormatError("control version 2 cannot authorize Connector deletion")
            return {
                "index": self.index,
                "resource_id": self.resource_id,
                "action": self.action,
                "gateway_evidence": (
                    self.gateway_evidence.to_dict() if self.gateway_evidence is not None else None
                ),
            }
        if self._is_connector_delete() and self.connector_evidence is None:
            raise StateFormatError("control version 3 Connector deletion requires action evidence")
        result: dict[str, object] = {
            "index": self.index,
            "resource_id": self.resource_id,
            "action": self.action,
            "gateway_evidence": (
                self.gateway_evidence.to_dict() if self.gateway_evidence is not None else None
            ),
            "connector_evidence": (
                self.connector_evidence.to_dict() if self.connector_evidence is not None else None
            ),
        }
        if version >= KAFKA_STREAMS_CONTROL_VERSION:
            result["kafka_streams_evidence"] = (
                self.kafka_streams_evidence.to_dict() if self.kafka_streams_evidence is not None else None
            )
        return result

    @classmethod
    def from_dict(
        cls,
        value: object,
        *,
        control_version: int | None = None,
    ) -> OperationAction:
        if not isinstance(value, dict):
            raise StateFormatError("operation action must be an object")
        version = control_version
        if version is None:
            version = (
                4 if "kafka_streams_evidence" in value else
                3 if "connector_evidence" in value else 2 if "gateway_evidence" in value else 1
            )
        if type(version) is not int or version not in _CONTROL_VERSIONS:
            raise StateFormatError("operation action control version is unsupported")
        expected = {"index", "resource_id", "action"}
        if version >= 2:
            expected.add("gateway_evidence")
        if version >= 3:
            expected.add("connector_evidence")
        if version >= 4:
            expected.add("kafka_streams_evidence")
        data = _strict_object(
            value,
            label="operation action",
            expected=expected,
        )
        raw_evidence = data.get("gateway_evidence")
        raw_connector_evidence = data.get("connector_evidence")
        raw_kafka_streams_evidence = data.get("kafka_streams_evidence")
        action = cls(
            index=cast(int, data["index"]),
            resource_id=cast(str, data["resource_id"]),
            action=cast(str, data["action"]),
            gateway_evidence=(
                None if raw_evidence is None else GatewayActionEvidence.from_dict(raw_evidence)
            ),
            connector_evidence=(
                None
                if raw_connector_evidence is None
                else ConnectorActionEvidence.from_dict(raw_connector_evidence)
            ),
            kafka_streams_evidence=(
                None if raw_kafka_streams_evidence is None
                else KafkaStreamsActionEvidence.from_dict(raw_kafka_streams_evidence)
            ),
        )
        action.to_dict(control_version=version)
        object.__setattr__(action, "_wire_version", version)
        return action


OperationKind = Literal["apply", "adopt"]


@dataclass(frozen=True)
class OperationIntent:
    """Durable evidence written before an operation can mutate anything."""

    operation_id: str
    kind: OperationKind
    started_at: str
    actor: str
    prior_state_serial: int
    prior_state_checksum: str
    reviewed_plan_checksum: str | None
    actions: tuple[OperationAction, ...]
    _wire_version: int = field(
        default=CURRENT_CONTROL_VERSION,
        init=False,
        repr=False,
        compare=False,
    )

    def __post_init__(self) -> None:
        _require_uuid(self.operation_id, "operation_id")
        if self.kind not in ("apply", "adopt"):
            raise StateFormatError("operation kind must be 'apply' or 'adopt'")
        _require_timestamp(self.started_at, "operation started_at")
        _require_safe_text(self.actor, "operation actor", maximum=128)
        if type(self.prior_state_serial) is not int or self.prior_state_serial < 0:
            raise StateFormatError(
                "operation prior_state_serial must be a non-negative integer"
            )
        _require_checksum(self.prior_state_checksum, "operation prior_state_checksum")
        if self.reviewed_plan_checksum is not None:
            _require_checksum(
                self.reviewed_plan_checksum,
                "operation reviewed_plan_checksum",
            )
        if not isinstance(self.actions, tuple):
            raise StateFormatError("operation actions must be an ordered tuple")
        if any(type(action) is not OperationAction for action in self.actions):
            raise StateFormatError("operation actions must contain exact actions")
        if [action.index for action in self.actions] != list(range(len(self.actions))):
            raise StateFormatError("operation action indexes must be contiguous from zero")
        if any(action.kafka_streams_evidence is not None for action in self.actions):
            if self.kind != "apply":
                raise StateFormatError("Kafka Streams replacement requires an apply operation")
            object.__setattr__(self, "_wire_version", KAFKA_STREAMS_CONTROL_VERSION)

    def to_dict(self, *, control_version: int | None = None) -> dict[str, object]:
        version = self._wire_version if control_version is None else control_version
        return {
            "operation_id": self.operation_id,
            "kind": self.kind,
            "started_at": self.started_at,
            "actor": self.actor,
            "prior_state_serial": self.prior_state_serial,
            "prior_state_checksum": self.prior_state_checksum,
            "reviewed_plan_checksum": self.reviewed_plan_checksum,
            "actions": [
                action.to_dict(control_version=version) for action in self.actions
            ],
        }

    def validate_kafka_streams_prior_state(self, state: LocalState) -> None:
        """A retained preimage must match protected ownership, never just labels."""
        self._validate_kafka_streams_records(state, desired=False)

    def validate_kafka_streams_result_state(self, state: LocalState) -> None:
        """Never clear completed replacement evidence while retaining old ownership."""
        self._validate_kafka_streams_records(state, desired=True)

    def _validate_kafka_streams_records(self, state: LocalState, *, desired: bool) -> None:
        for action in self.actions:
            evidence = action.kafka_streams_evidence
            if evidence is None:
                continue
            artifact = evidence.desired_artifact if desired else evidence.prior_artifact
            record = state.resources.get(action.resource_id)
            ownership = cast(dict[str, str], artifact.to_dict()["ownership"])
            if record is None or (
                record.physical_name != evidence.application_id
                or record.backend != evidence.backend_identity
                or record.artifact_checksum != artifact.checksum
                or record.ownership != ownership["mode"]
            ):
                surface = "result" if desired else "preimage"
                raise StateBackendConflictError(f"Kafka Streams replacement {surface} does not match protected ownership state")

    @classmethod
    def from_dict(
        cls,
        value: object,
        *,
        control_version: int = CURRENT_CONTROL_VERSION,
    ) -> OperationIntent:
        if type(control_version) is not int or control_version not in _CONTROL_VERSIONS:
            raise StateFormatError("operation intent control version is unsupported")
        data = _strict_object(
            value,
            label="operation intent",
            expected={
                "operation_id",
                "kind",
                "started_at",
                "actor",
                "prior_state_serial",
                "prior_state_checksum",
                "reviewed_plan_checksum",
                "actions",
            },
        )
        raw_actions = data["actions"]
        if not isinstance(raw_actions, list):
            raise StateFormatError("operation intent actions must be an array")
        kind = data["kind"]
        if kind not in ("apply", "adopt"):
            raise StateFormatError("operation kind must be 'apply' or 'adopt'")
        intent = cls(
            operation_id=cast(str, data["operation_id"]),
            kind=cast(OperationKind, kind),
            started_at=cast(str, data["started_at"]),
            actor=cast(str, data["actor"]),
            prior_state_serial=cast(int, data["prior_state_serial"]),
            prior_state_checksum=cast(str, data["prior_state_checksum"]),
            reviewed_plan_checksum=cast(str | None, data["reviewed_plan_checksum"]),
            actions=tuple(
                OperationAction.from_dict(
                    action,
                    control_version=control_version,
                )
                for action in raw_actions
            ),
        )
        object.__setattr__(intent, "_wire_version", control_version)
        return intent


ProgressStatus = Literal["started", "checkpoint", "completed"]


@dataclass(frozen=True)
class OperationProgress:
    """One durably recorded action boundary for an active operation."""

    operation_id: str
    action_index: int
    resource_id: str
    action: str
    status: ProgressStatus
    succeeded: bool | None
    recorded_at: str
    kafka_streams_checkpoint: KafkaStreamsCheckpointEvidence | None = None

    def __post_init__(self) -> None:
        _require_uuid(self.operation_id, "progress operation_id")
        if type(self.action_index) is not int or self.action_index < 0:
            raise StateFormatError("progress action_index must be a non-negative integer")
        _require_safe_text(self.resource_id, "progress resource_id")
        if not isinstance(self.action, str) or not _ACTION_PATTERN.fullmatch(self.action):
            raise StateFormatError("progress action must be a lowercase action identifier")
        if self.status not in ("started", "checkpoint", "completed"):
            raise StateFormatError("progress status must be 'started', 'checkpoint' or 'completed'")
        if self.status in ("started", "checkpoint") and self.succeeded is not None:
            raise StateFormatError("started/checkpoint progress cannot have an outcome")
        if self.status == "completed" and type(self.succeeded) is not bool:
            raise StateFormatError("completed progress requires a boolean outcome")
        checkpoint = self.kafka_streams_checkpoint
        if self.status == "checkpoint":
            if type(checkpoint) is not KafkaStreamsCheckpointEvidence:
                raise StateFormatError("checkpoint progress requires exact Kafka Streams evidence")
            if checkpoint.operation_id != self.operation_id or checkpoint.action_index != self.action_index:
                raise StateFormatError("checkpoint generation does not match progress operation/action")
        elif checkpoint is not None:
            raise StateFormatError("Kafka Streams checkpoint evidence requires checkpoint progress")
        _require_timestamp(self.recorded_at, "progress recorded_at")

    def to_dict(self, *, control_version: int | None = None) -> dict[str, object]:
        version = control_version if control_version is not None else (4 if self.status == "checkpoint" else 3)
        if type(version) is not int or version not in _CONTROL_VERSIONS:
            raise StateFormatError("operation progress control version is unsupported")
        if self.status == "checkpoint" and version < 4:
            raise StateFormatError("Kafka Streams checkpoint requires control version 4")
        result: dict[str, object] = {
            "operation_id": self.operation_id,
            "action_index": self.action_index,
            "resource_id": self.resource_id,
            "action": self.action,
            "status": self.status,
            "succeeded": self.succeeded,
            "recorded_at": self.recorded_at,
        }
        # Non-checkpoint boundaries retain their exact v1-3 representation.
        if self.kafka_streams_checkpoint is not None:
            result["kafka_streams_checkpoint"] = self.kafka_streams_checkpoint.to_dict()
        return result

    @classmethod
    def from_dict(cls, value: object, *, control_version: int | None = None) -> OperationProgress:
        expected = {"operation_id", "action_index", "resource_id", "action", "status", "succeeded", "recorded_at"}
        if isinstance(value, dict) and value.get("status") == "checkpoint":
            expected.add("kafka_streams_checkpoint")
        data = _strict_object(
            value,
            label="operation progress",
            expected=expected,
        )
        status = data["status"]
        if status not in ("started", "checkpoint", "completed"):
            raise StateFormatError("progress status must be 'started', 'checkpoint' or 'completed'")
        result = cls(
            operation_id=cast(str, data["operation_id"]),
            action_index=cast(int, data["action_index"]),
            resource_id=cast(str, data["resource_id"]),
            action=cast(str, data["action"]),
            status=cast(ProgressStatus, status),
            succeeded=cast(bool | None, data["succeeded"]),
            recorded_at=cast(str, data["recorded_at"]),
            kafka_streams_checkpoint=(
                KafkaStreamsCheckpointEvidence.from_dict(data["kafka_streams_checkpoint"])
                if status == "checkpoint" else None
            ),
        )
        result.to_dict(control_version=control_version)
        return result


@dataclass(frozen=True)
class RecoveryRecord:
    """Sanitized durable evidence that explicit recovery is required."""

    operation_id: str
    failure_code: str
    failed_at: str
    last_completed_action_index: int | None
    mutation_may_have_succeeded: bool = True

    def __post_init__(self) -> None:
        _require_uuid(self.operation_id, "recovery operation_id")
        if not isinstance(self.failure_code, str) or not _ACTION_PATTERN.fullmatch(
            self.failure_code
        ):
            raise StateFormatError(
                "recovery failure_code must be a lowercase stable identifier"
            )
        _require_timestamp(self.failed_at, "recovery failed_at")
        if self.last_completed_action_index is not None and (
            type(self.last_completed_action_index) is not int
            or self.last_completed_action_index < 0
        ):
            raise StateFormatError(
                "recovery last_completed_action_index must be null or non-negative"
            )
        if self.mutation_may_have_succeeded is not True:
            raise StateFormatError(
                "recovery records must conservatively mark possible mutation"
            )

    def to_dict(self) -> dict[str, object]:
        return {
            "operation_id": self.operation_id,
            "failure_code": self.failure_code,
            "failed_at": self.failed_at,
            "last_completed_action_index": self.last_completed_action_index,
            "mutation_may_have_succeeded": self.mutation_may_have_succeeded,
        }

    @classmethod
    def from_dict(cls, value: object) -> RecoveryRecord:
        data = _strict_object(
            value,
            label="recovery record",
            expected={
                "operation_id",
                "failure_code",
                "failed_at",
                "last_completed_action_index",
                "mutation_may_have_succeeded",
            },
        )
        return cls(
            operation_id=cast(str, data["operation_id"]),
            failure_code=cast(str, data["failure_code"]),
            failed_at=cast(str, data["failed_at"]),
            last_completed_action_index=cast(
                int | None,
                data["last_completed_action_index"],
            ),
            mutation_may_have_succeeded=cast(
                bool,
                data["mutation_may_have_succeeded"],
            ),
        )


ControlStatus = Literal["clear", "in_progress", "recovery_required"]


@dataclass(frozen=True)
class OperationControlState:
    """Strict provider-neutral durable operation-control payload."""

    address: StateAddress
    status: ControlStatus = "clear"
    intent: OperationIntent | None = None
    progress: tuple[OperationProgress, ...] = ()
    recovery: RecoveryRecord | None = None
    control_version: int = CURRENT_CONTROL_VERSION

    def __post_init__(self) -> None:
        # Provider implementations reconstruct controls around the immutable
        # intent. Preserve a loaded legacy intent's version and canonical checksum.
        if (
            self.control_version == CURRENT_CONTROL_VERSION
            and self.intent is not None
            and self.intent._wire_version in (1, 2, KAFKA_STREAMS_CONTROL_VERSION)
        ):
            object.__setattr__(self, "control_version", self.intent._wire_version)
        if type(self.control_version) is not int or self.control_version not in _CONTROL_VERSIONS:
            raise StateFormatError(
                f"unsupported control version {self.control_version!r}; "
                "expected 1, 2, 3, or 4"
            )
        if self.status not in ("clear", "in_progress", "recovery_required"):
            raise StateFormatError("control status is invalid")
        if self.status == "clear":
            if self.intent is not None or self.progress or self.recovery is not None:
                raise StateFormatError("clear control state cannot contain operation data")
            return
        if self.intent is None:
            raise StateFormatError("active control state requires an operation intent")
        if self.control_version == 1 and any(
            action.gateway_evidence is not None or action.connector_evidence is not None
            for action in self.intent.actions
        ):
            raise StateFormatError("control version 1 cannot contain action evidence")
        if self.control_version == 2 and any(
            action.connector_evidence is not None for action in self.intent.actions
        ):
            raise StateFormatError("control version 2 cannot contain Connector action evidence")
        for action in self.intent.actions:
            action.to_dict(control_version=self.control_version)
        if self.control_version >= 2:
            for action in self.intent.actions:
                try:
                    identity = ResourceIdentity.parse(action.resource_id)
                except StateFormatError:
                    continue
                if action.gateway_evidence is not None and (
                    identity.project != self.address.project
                    or identity.environment != self.address.environment
                ):
                    raise StateFormatError(
                        "Gateway action evidence belongs to another state address"
                    )
                if action.connector_evidence is not None and (
                    identity.project != self.address.project
                    or identity.environment != self.address.environment
                ):
                    raise StateFormatError(
                        "Connector action evidence belongs to another state address"
                    )
                if action.kafka_streams_evidence is not None and (
                    identity.project != self.address.project
                    or identity.environment != self.address.environment
                ):
                    raise StateFormatError("Kafka Streams action evidence belongs to another state address")
                if (
                    identity.kind == "gateway_rule"
                    and action.action in ("create", "update", "delete", "adopt")
                    and action.gateway_evidence is None
                ):
                    raise StateFormatError(
                        f"control version {self.control_version} Gateway actions require "
                        "action evidence"
                    )
                if (
                    self.control_version >= 3
                    and identity.kind == "connector"
                    and action.action == "delete"
                    and action.connector_evidence is None
                ):
                    raise StateFormatError(
                        "control version 3 Connector deletion requires action evidence"
                    )
        if self.status == "in_progress" and self.recovery is not None:
            raise StateFormatError("in_progress control state cannot contain recovery data")
        if self.status == "recovery_required":
            if self.recovery is None:
                raise StateFormatError("recovery_required control state needs a record")
            if self.recovery.operation_id != self.intent.operation_id:
                raise StateFormatError("recovery record operation_id does not match intent")
        self._validate_progress()
        if self.recovery is not None:
            safely_completed = [
                item.action_index
                for item in self.progress
                if item.status == "completed" and item.succeeded is True
            ]
            expected_last = max(safely_completed) if safely_completed else None
            if self.recovery.last_completed_action_index != expected_last:
                raise StateFormatError(
                    "recovery last_completed_action_index does not match safe progress"
                )

    def _validate_progress(self) -> None:
        if self.intent is None:
            return
        if type(self.progress) is not tuple:
            raise StateFormatError("operation progress must be an immutable ordered tuple")
        seen: set[tuple[int, str]] = set()
        successfully_completed: set[int] = set()
        previous_index = -1
        checkpoint_counts: dict[int, int] = {}
        for item in self.progress:
            if type(item) is not OperationProgress:
                raise StateFormatError("operation progress requires exact typed boundaries")
            item.to_dict(control_version=self.control_version)
            if item.operation_id != self.intent.operation_id:
                raise StateFormatError("progress operation_id does not match intent")
            if item.action_index >= len(self.intent.actions):
                raise StateFormatError("progress action_index is outside the intent")
            action = self.intent.actions[item.action_index]
            if item.resource_id != action.resource_id or item.action != action.action:
                raise StateFormatError("progress action does not match the intent")
            if item.action_index < previous_index:
                raise StateFormatError("operation progress must be ordered")
            if item.action_index > previous_index + 1:
                raise StateFormatError(
                    "operation progress action indexes must be contiguous from zero"
                )
            if item.action_index > previous_index and previous_index >= 0 and (
                previous_index not in successfully_completed
            ):
                raise StateFormatError(
                    "operation progress cannot advance unless the prior action succeeded"
                )
            if (item.action_index, "completed") in seen:
                raise StateFormatError("operation progress cannot follow a completed boundary")
            checkpoint = item.kafka_streams_checkpoint
            key = (item.action_index, checkpoint.phase if checkpoint is not None else item.status)
            if key in seen:
                raise StateFormatError("operation progress contains a duplicate boundary")
            if item.status == "completed" and (item.action_index, "started") not in seen:
                raise StateFormatError("completed progress requires a started boundary")
            if item.action_index > previous_index and item.status != "started":
                raise StateFormatError("a new progress action must start before completion")
            if checkpoint is not None:
                evidence = action.kafka_streams_evidence
                if evidence is None or action.action != "update":
                    raise StateFormatError("checkpoint progress requires an evidenced Kafka Streams replacement")
                checkpoint.validate_action(evidence)
                phase_index = checkpoint_counts.get(item.action_index, 0)
                if checkpoint.phase != KAFKA_STREAMS_CHECKPOINT_PHASES[phase_index]:
                    raise StateFormatError("Kafka Streams checkpoints must follow old_closed, old_removed, replacement_created")
                checkpoint_counts[item.action_index] = phase_index + 1
            if (
                item.status == "completed" and item.succeeded is True
                and action.kafka_streams_evidence is not None
                and checkpoint_counts.get(item.action_index, 0) != len(KAFKA_STREAMS_CHECKPOINT_PHASES)
            ):
                raise StateFormatError("Kafka Streams replacement cannot succeed before all durable checkpoints")
            seen.add(key)
            if item.status == "completed" and item.succeeded is True:
                successfully_completed.add(item.action_index)
            previous_index = item.action_index

    @property
    def actions_completed(self) -> bool:
        """Phase validation above makes successful boundaries authoritative."""
        return self.intent is not None and {
            item.action_index for item in self.progress
            if item.status == "completed" and item.succeeded is True
        } == set(range(len(self.intent.actions)))

    @classmethod
    def clear(cls, address: StateAddress) -> OperationControlState:
        return cls(address=address)

    def to_dict(self) -> dict[str, object]:
        return {
            "control_version": self.control_version,
            "address": self.address.uri,
            "status": self.status,
            "intent": (
                self.intent.to_dict(control_version=self.control_version)
                if self.intent is not None
                else None
            ),
            "progress": [item.to_dict(control_version=self.control_version) for item in self.progress],
            "recovery": self.recovery.to_dict() if self.recovery is not None else None,
        }

    @classmethod
    def from_dict(
        cls,
        value: object,
        *,
        expected_address: StateAddress,
    ) -> OperationControlState:
        data = _strict_object(
            value,
            label="operation control state",
            expected={
                "control_version",
                "address",
                "status",
                "intent",
                "progress",
                "recovery",
            },
        )
        address = StateAddress.parse(data["address"])
        if address != expected_address:
            raise StateIdentityError("operation control state belongs to another address")
        raw_progress = data["progress"]
        if not isinstance(raw_progress, list):
            raise StateFormatError("operation control progress must be an array")
        status = data["status"]
        if status not in ("clear", "in_progress", "recovery_required"):
            raise StateFormatError("control status is invalid")
        control_version = data["control_version"]
        if type(control_version) is not int or control_version not in _CONTROL_VERSIONS:
            raise StateFormatError(
                f"unsupported control version {control_version!r}; "
                "expected 1, 2, 3, or 4"
            )
        return cls(
            control_version=control_version,
            address=address,
            status=cast(ControlStatus, status),
            intent=(
                None
                if data["intent"] is None
                else OperationIntent.from_dict(
                    data["intent"],
                    control_version=control_version,
                )
            ),
            progress=tuple(OperationProgress.from_dict(item, control_version=control_version) for item in raw_progress),
            recovery=(
                None
                if data["recovery"] is None
                else RecoveryRecord.from_dict(data["recovery"])
            ),
        )


@dataclass(frozen=True)
class ControlObservation:
    """One strict control payload and its local CAS revision."""

    control: OperationControlState
    revision: StateRevision

    def safe_status(self) -> dict[str, object]:
        intent = self.control.intent
        recovery = self.control.recovery
        completed = [
            item.action_index
            for item in self.control.progress
            if item.status == "completed" and item.succeeded is True
        ]
        return {
            "status": self.control.status,
            "operation_id": intent.operation_id if intent is not None else None,
            "kind": intent.kind if intent is not None else None,
            "failure_code": recovery.failure_code if recovery is not None else None,
            "last_completed_action_index": max(completed) if completed else None,
        }


@dataclass(frozen=True)
class OperationSnapshot:
    """State and operation control observed at one locked workflow boundary.

    Remote providers can produce this pair from one database snapshot.  The
    local provider binds two adjacent files while retaining its existing
    operation-wide file lock; it does not claim an atomic cross-file read.
    """

    state: StateObservation
    control: ControlObservation

    def __post_init__(self) -> None:
        if self.state.address != self.control.control.address:
            raise StateIdentityError(
                "operation state and control observations have different addresses"
            )

    @property
    def address(self) -> StateAddress:
        return self.state.address


def _strict_object(
    value: object,
    *,
    label: str,
    expected: set[str],
) -> dict[str, object]:
    if not isinstance(value, dict):
        raise StateFormatError(f"{label} must be an object")
    unknown = set(value) - expected
    missing = expected - set(value)
    if unknown:
        raise StateFormatError(
            f"{label} has unknown field(s): {', '.join(sorted(unknown))}"
        )
    if missing:
        raise StateFormatError(
            f"{label} is missing field(s): {', '.join(sorted(missing))}"
        )
    return value


@runtime_checkable
class DeploymentStateOperation(Protocol):
    """Exclusive state operation used by mutating application workflows."""

    def read(self) -> StateObservation: ...

    def read_control(self) -> ControlObservation: ...

    def observe(self) -> OperationSnapshot: ...

    def ensure_ready(
        self,
        observation: OperationSnapshot | ControlObservation,
    ) -> None: ...

    def check_lock(self) -> None: ...

    @overload
    def begin_operation(
        self,
        observation: OperationSnapshot,
        intent: OperationIntent,
    ) -> OperationSnapshot: ...

    @overload
    def begin_operation(
        self,
        observation: ControlObservation,
        intent: OperationIntent,
    ) -> ControlObservation: ...

    @overload
    def record_progress(
        self,
        observation: OperationSnapshot,
        progress: OperationProgress,
    ) -> OperationSnapshot: ...

    @overload
    def record_progress(
        self,
        observation: ControlObservation,
        progress: OperationProgress,
    ) -> ControlObservation: ...

    @overload
    def mark_recovery_required(
        self,
        observation: OperationSnapshot,
        recovery: RecoveryRecord,
    ) -> OperationSnapshot: ...

    @overload
    def mark_recovery_required(
        self,
        observation: ControlObservation,
        recovery: RecoveryRecord,
    ) -> ControlObservation: ...

    @overload
    def clear_operation(
        self,
        observation: OperationSnapshot,
    ) -> OperationSnapshot: ...

    @overload
    def clear_operation(
        self,
        observation: ControlObservation,
    ) -> ControlObservation: ...

    def commit_operation(
        self,
        observation: OperationSnapshot,
        replacement: LocalState | None,
    ) -> OperationSnapshot: ...

    def clear_before_mutation(
        self,
        observation: OperationSnapshot,
    ) -> OperationSnapshot: ...

    def finalize_recovery(
        self,
        observation: OperationSnapshot,
        evidence: RecoverySnapshotEvidence,
        resolution: RecoveryResolutionRecord,
        replacement: LocalState | None,
    ) -> OperationSnapshot: ...

    def compare_and_swap(
        self,
        observation: StateObservation,
        replacement: LocalState,
    ) -> StateObservation: ...


@runtime_checkable
class DeploymentStateBackend(Protocol):
    """Provider contract available to the state application service."""

    def describe(self) -> StateStoreIdentity: ...

    def read(self, address: StateAddress) -> StateObservation: ...

    def read_control(self, address: StateAddress) -> ControlObservation: ...

    def operation(
        self,
        address: StateAddress,
    ) -> AbstractContextManager[DeploymentStateOperation]: ...


def state_checksum(state: LocalState) -> str:
    """Hash the complete strict ownership payload using canonical JSON."""
    payload = json.dumps(
        state.to_dict(),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")
    return f"sha256:{hashlib.sha256(payload).hexdigest()}"


def _state_revision(state: LocalState) -> StateRevision:
    return StateRevision(state_checksum(state))


def _control_revision(control: OperationControlState) -> StateRevision:
    payload = json.dumps(
        control.to_dict(),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")
    return StateRevision(f"sha256:{hashlib.sha256(payload).hexdigest()}")


def local_control_path(project_path: Path, *, environment: str) -> Path:
    """Return the control sidecar adjacent to an environment's v1 state."""
    state_path = local_state_path(project_path, environment=environment)
    return state_path.with_name(f"{environment}.control.json")


def local_recovery_history_path(project_path: Path, *, environment: str) -> Path:
    """Return the append-only recovery audit sidecar for an environment."""
    state_path = local_state_path(project_path, environment=environment)
    return state_path.with_name(f"{environment}.recovery-history.json")


def operation_timestamp() -> str:
    """Return a canonical UTC timestamp suitable for durable control records."""
    return datetime.now(timezone.utc).isoformat(timespec="microseconds").replace(
        "+00:00", "Z"
    )


RecoveryHistoryEventKind = Literal["recovery_intent", "recovery_resolution"]


def _same_recovery_resolution_identity(
    left: RecoveryResolutionRecord,
    right: RecoveryResolutionRecord,
) -> bool:
    """Compare retry identity while preserving first-attempt audit time."""
    left_data = left.to_dict()
    right_data = right.to_dict()
    del left_data["resolved_at"]
    del right_data["resolved_at"]
    return left_data == right_data


def _validate_recovery_transition_inputs(
    observation: OperationSnapshot,
    evidence: RecoverySnapshotEvidence,
    resolution: RecoveryResolutionRecord,
    replacement: LocalState | None,
) -> bool:
    """Validate shared invariants and report an exact prior-state match.

    ``False`` means the state is already the exact declared result. Providers
    may accept that only when their durable audit proves this recovery wrote it.
    """
    from streamt.deployer.recovery import control_checksum

    control = observation.control.control
    intent = control.intent
    if control.status not in ("in_progress", "recovery_required") or intent is None:
        raise StateBackendRecoveryRequiredError(
            "clear deployment state control is not recoverable"
        )
    if evidence.store != observation.state.store:
        raise StateIdentityError("recovery evidence belongs to another state store")
    if evidence.address != observation.address or resolution.address != observation.address:
        raise StateIdentityError("recovery evidence belongs to another state address")
    if evidence.control != control or (
        evidence.control_checksum != control_checksum(control)
    ):
        raise StateBackendConflictError(
            "operation control changed after recovery evidence was reviewed"
        )
    if resolution.blocked_operation_id != intent.operation_id or (
        evidence.blocked_operation_id != intent.operation_id
    ):
        raise StateIdentityError("recovery evidence belongs to another blocked operation")
    if (
        resolution.prior_state_serial != evidence.state.serial
        or resolution.prior_state_checksum != evidence.state_checksum
        or state_checksum(evidence.state) != evidence.state_checksum
    ):
        raise StateBackendConflictError(
            "recovery resolution does not match its reviewed prior state"
        )
    if (
        intent.prior_state_serial != evidence.state.serial
        or intent.prior_state_checksum != evidence.state_checksum
    ):
        raise StateBackendConflictError(
            "blocked operation intent does not match recovery prior state"
        )
    if resolution.resolution == "abandoned_before_mutation" and control.progress:
        raise StateBackendRecoveryRequiredError(
            "abandoned-before-mutation recovery is forbidden after an action started"
        )

    if replacement is None:
        if resolution.state_changed:
            raise StateFormatError("changed observed recovery requires exact replacement state")
        if (
            resolution.result_state_serial != evidence.state.serial
            or resolution.result_state_checksum != evidence.state_checksum
        ):
            raise StateFormatError("unchanged recovery must preserve the reviewed prior state")
        if observation.state.state != evidence.state:
            raise StateBackendConflictError(
                "state changed after recovery evidence was reviewed"
            )
        return True

    if resolution.resolution != "observed" or not resolution.state_changed:
        raise StateFormatError(
            "replacement state is allowed only for changed observed recovery"
        )
    if (
        replacement.project != observation.address.project
        or replacement.environment != observation.address.environment
    ):
        raise StateIdentityError("recovery replacement state belongs to another address")
    if replacement.resources == evidence.state.resources:
        raise StateFormatError(
            "recovery must not increment state serial when ownership is unchanged"
        )
    if (
        replacement.serial != evidence.state.serial + 1
        or resolution.result_state_serial != replacement.serial
        or resolution.result_state_checksum != state_checksum(replacement)
    ):
        raise StateFormatError("recovery replacement does not match its declared result state")
    if observation.state.state == evidence.state:
        return True
    if observation.state.state == replacement:
        return False
    raise StateBackendConflictError(
        "state does not match the reviewed prior or declared recovery result"
    )


def _recovery_history_event_checksum(
    *,
    sequence: int,
    kind: RecoveryHistoryEventKind,
    previous_checksum: str | None,
    record: RecoveryResolutionRecord,
) -> str:
    payload = json.dumps(
        {
            "event_version": CURRENT_RECOVERY_HISTORY_EVENT_VERSION,
            "sequence": sequence,
            "kind": kind,
            "previous_checksum": previous_checksum,
            "record": record.to_dict(),
        },
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")
    return f"sha256:{hashlib.sha256(payload).hexdigest()}"


@dataclass(frozen=True)
class _LocalRecoveryHistoryEvent:
    """One checksum-chained local recovery intent or resolution event."""

    sequence: int
    kind: RecoveryHistoryEventKind
    previous_checksum: str | None
    record: RecoveryResolutionRecord
    checksum: str
    event_version: int = CURRENT_RECOVERY_HISTORY_EVENT_VERSION

    def __post_init__(self) -> None:
        if type(self.event_version) is not int or (
            self.event_version != CURRENT_RECOVERY_HISTORY_EVENT_VERSION
        ):
            raise StateFormatError(
                f"unsupported recovery history event version {self.event_version!r}; "
                f"expected {CURRENT_RECOVERY_HISTORY_EVENT_VERSION}"
            )
        if type(self.sequence) is not int or self.sequence < 0:
            raise StateFormatError("recovery history sequence must be a non-negative integer")
        if self.kind not in ("recovery_intent", "recovery_resolution"):
            raise StateFormatError("recovery history event kind is invalid")
        if self.previous_checksum is not None:
            _require_checksum(
                self.previous_checksum,
                "recovery history previous_checksum",
            )
        _require_checksum(self.checksum, "recovery history event checksum")
        expected = _recovery_history_event_checksum(
            sequence=self.sequence,
            kind=self.kind,
            previous_checksum=self.previous_checksum,
            record=self.record,
        )
        if self.checksum != expected:
            raise StateFormatError("recovery history event checksum does not match")

    @classmethod
    def create(
        cls,
        *,
        sequence: int,
        kind: RecoveryHistoryEventKind,
        previous_checksum: str | None,
        record: RecoveryResolutionRecord,
    ) -> _LocalRecoveryHistoryEvent:
        return cls(
            sequence=sequence,
            kind=kind,
            previous_checksum=previous_checksum,
            record=record,
            checksum=_recovery_history_event_checksum(
                sequence=sequence,
                kind=kind,
                previous_checksum=previous_checksum,
                record=record,
            ),
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "event_version": self.event_version,
            "sequence": self.sequence,
            "kind": self.kind,
            "previous_checksum": self.previous_checksum,
            "record": self.record.to_dict(),
            "checksum": self.checksum,
        }

    @classmethod
    def from_dict(cls, value: object) -> _LocalRecoveryHistoryEvent:
        from streamt.deployer.recovery import RecoveryResolutionRecord

        data = _strict_object(
            value,
            label="recovery history event",
            expected={
                "event_version",
                "sequence",
                "kind",
                "previous_checksum",
                "record",
                "checksum",
            },
        )
        kind = data["kind"]
        if kind not in ("recovery_intent", "recovery_resolution"):
            raise StateFormatError("recovery history event kind is invalid")
        return cls(
            event_version=cast(int, data["event_version"]),
            sequence=cast(int, data["sequence"]),
            kind=cast(RecoveryHistoryEventKind, kind),
            previous_checksum=cast(str | None, data["previous_checksum"]),
            record=RecoveryResolutionRecord.from_dict(data["record"]),
            checksum=cast(str, data["checksum"]),
        )


@dataclass(frozen=True)
class _LocalRecoveryHistory:
    """Strict bounded append-only local recovery audit payload."""

    address: StateAddress
    events: tuple[_LocalRecoveryHistoryEvent, ...] = ()
    history_version: int = CURRENT_RECOVERY_HISTORY_VERSION

    def __post_init__(self) -> None:
        if type(self.history_version) is not int or (
            self.history_version != CURRENT_RECOVERY_HISTORY_VERSION
        ):
            raise StateFormatError(
                f"unsupported recovery history version {self.history_version!r}; "
                f"expected {CURRENT_RECOVERY_HISTORY_VERSION}"
            )
        if not isinstance(self.events, tuple):
            raise StateFormatError("recovery history events must be an ordered tuple")
        if len(self.events) > MAX_LOCAL_RECOVERY_HISTORY_EVENTS:
            raise StateFormatError("local recovery history contains too many events")

        expected_previous: str | None = None
        pending: RecoveryResolutionRecord | None = None
        completed_operation_ids: set[str] = set()
        recovered_blocked_operation_ids: set[str] = set()
        for sequence, event in enumerate(self.events):
            if event.sequence != sequence:
                raise StateFormatError(
                    "recovery history event sequences must be contiguous from zero"
                )
            if event.previous_checksum != expected_previous:
                raise StateFormatError("recovery history checksum chain is broken")
            if event.record.address != self.address:
                raise StateIdentityError("recovery history event belongs to another address")
            operation_id = event.record.recovery_operation_id
            if event.kind == "recovery_intent":
                if pending is not None:
                    raise StateFormatError("recovery history contains an unresolved prior intent")
                if operation_id in completed_operation_ids:
                    raise StateFormatError(
                        "recovery history contains a duplicate recovery operation"
                    )
                if event.record.blocked_operation_id in recovered_blocked_operation_ids:
                    raise StateFormatError(
                        "recovery history resolves one blocked operation more than once"
                    )
                pending = event.record
            else:
                if pending is None or event.record != pending:
                    raise StateFormatError(
                        "recovery resolution does not match its preceding intent"
                    )
                completed_operation_ids.add(operation_id)
                recovered_blocked_operation_ids.add(event.record.blocked_operation_id)
                pending = None
            expected_previous = event.checksum

    def events_for(
        self,
        recovery_operation_id: str,
    ) -> tuple[_LocalRecoveryHistoryEvent, ...]:
        return tuple(
            event
            for event in self.events
            if event.record.recovery_operation_id == recovery_operation_id
        )

    def append(
        self,
        kind: RecoveryHistoryEventKind,
        record: RecoveryResolutionRecord,
    ) -> _LocalRecoveryHistory:
        previous_checksum = self.events[-1].checksum if self.events else None
        event = _LocalRecoveryHistoryEvent.create(
            sequence=len(self.events),
            kind=kind,
            previous_checksum=previous_checksum,
            record=record,
        )
        return _LocalRecoveryHistory(
            address=self.address,
            events=(*self.events, event),
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "history_version": self.history_version,
            "address": self.address.uri,
            "events": [event.to_dict() for event in self.events],
        }

    @classmethod
    def from_dict(
        cls,
        value: object,
        *,
        expected_address: StateAddress,
    ) -> _LocalRecoveryHistory:
        data = _strict_object(
            value,
            label="local recovery history",
            expected={"history_version", "address", "events"},
        )
        address = StateAddress.parse(data["address"])
        if address != expected_address:
            raise StateIdentityError("local recovery history belongs to another address")
        raw_events = data["events"]
        if not isinstance(raw_events, list):
            raise StateFormatError("local recovery history events must be an array")
        if len(raw_events) > MAX_LOCAL_RECOVERY_HISTORY_EVENTS:
            raise StateFormatError("local recovery history contains too many events")
        return cls(
            history_version=cast(int, data["history_version"]),
            address=address,
            events=tuple(_LocalRecoveryHistoryEvent.from_dict(item) for item in raw_events),
        )


class _LocalDeploymentStateOperation:
    def __init__(
        self,
        backend: LocalDeploymentStateBackend,
        address: StateAddress,
        lock: LocalStateOperationLock,
    ) -> None:
        self._backend = backend
        self._address = address
        self._lock = lock
        self._active_operation_id: str | None = None

    def read(self) -> StateObservation:
        return self._backend._read(self._address)

    def read_control(self) -> ControlObservation:
        return self._backend._read_control(self._address)

    def observe(self) -> OperationSnapshot:
        """Bind state and control reads while retaining the local file lock."""
        self.check_lock()
        return OperationSnapshot(
            state=self.read(),
            control=self.read_control(),
        )

    def check_lock(self) -> None:
        if not self._lock.is_held:
            raise StateBackendLockLostError(
                "deployment state operation lock was lost",
                operation_id=self._active_operation_id,
            )

    def _validate_snapshot(self, snapshot: OperationSnapshot) -> None:
        self._backend._validate_observation(self._address, snapshot.state)
        self._backend._validate_control_observation(
            self._address,
            snapshot.control,
        )
        self.check_lock()
        current = self._backend._read(self._address)
        if current.revision != snapshot.state.revision:
            raise StateBackendConflictError(
                "state revision changed after the operation snapshot was observed"
            )

    def _snapshot_for_control(
        self,
        observation: OperationSnapshot | ControlObservation,
    ) -> tuple[OperationSnapshot, bool]:
        if isinstance(observation, OperationSnapshot):
            self._validate_snapshot(observation)
            return observation, True
        self._backend._validate_control_observation(self._address, observation)
        self.check_lock()
        return OperationSnapshot(state=self.read(), control=observation), False

    @staticmethod
    def _validate_intent_state(
        snapshot: OperationSnapshot,
        intent: OperationIntent,
    ) -> None:
        if (
            intent.prior_state_serial != snapshot.state.state_serial
            or intent.prior_state_checksum != state_checksum(snapshot.state.state)
        ):
            raise StateBackendConflictError(
                "operation intent does not match its prior state snapshot"
            )
        intent.validate_kafka_streams_prior_state(snapshot.state.state)

    def ensure_ready(
        self,
        observation: OperationSnapshot | ControlObservation,
    ) -> None:
        if isinstance(observation, OperationSnapshot):
            self._validate_snapshot(observation)
            control = observation.control
        else:
            self._backend._validate_control_observation(
                self._address,
                observation,
            )
            self.check_lock()
            control = observation
        if control.control.status != "clear":
            raise StateBackendRecoveryRequiredError(
                "deployment state has an unfinished operation; explicit recovery "
                "is required before apply or adopt"
            )

    @overload
    def begin_operation(
        self,
        observation: OperationSnapshot,
        intent: OperationIntent,
    ) -> OperationSnapshot: ...

    @overload
    def begin_operation(
        self,
        observation: ControlObservation,
        intent: OperationIntent,
    ) -> ControlObservation: ...

    def begin_operation(
        self,
        observation: OperationSnapshot | ControlObservation,
        intent: OperationIntent,
    ) -> OperationSnapshot | ControlObservation:
        snapshot, return_snapshot = self._snapshot_for_control(observation)
        self.ensure_ready(snapshot)
        if return_snapshot or any(action.kafka_streams_evidence is not None for action in intent.actions):
            self._validate_intent_state(snapshot, intent)
        # The legacy control-only delegate cannot bind the caller's prior
        # state revision. Its existing CAS path remains responsible for the
        # conflict until apply/adopt migrate to OperationSnapshot.
        replacement = OperationControlState(
            address=self._address,
            status="in_progress",
            intent=intent,
        )
        active = self._backend._save_control_locked(
            self._address,
            snapshot.control,
            replacement,
            self._lock,
        )
        self._active_operation_id = intent.operation_id
        if return_snapshot:
            return OperationSnapshot(state=snapshot.state, control=active)
        # Compatibility delegate for the current CLI. New workflows retain
        # the state/control pair by passing an OperationSnapshot.
        return active

    @overload
    def record_progress(
        self,
        observation: OperationSnapshot,
        progress: OperationProgress,
    ) -> OperationSnapshot: ...

    @overload
    def record_progress(
        self,
        observation: ControlObservation,
        progress: OperationProgress,
    ) -> ControlObservation: ...

    def record_progress(
        self,
        observation: OperationSnapshot | ControlObservation,
        progress: OperationProgress,
    ) -> OperationSnapshot | ControlObservation:
        snapshot, return_snapshot = self._snapshot_for_control(observation)
        self._backend._validate_active_control(self._address, snapshot.control)
        intent = cast(OperationIntent, snapshot.control.control.intent)
        if progress.operation_id != intent.operation_id:
            raise StateIdentityError(
                "progress belongs to another deployment operation"
            )
        replacement = OperationControlState(
            address=self._address,
            status="in_progress",
            intent=intent,
            progress=(*snapshot.control.control.progress, progress),
        )
        active = self._backend._save_control_locked(
            self._address,
            snapshot.control,
            replacement,
            self._lock,
        )
        if return_snapshot:
            return OperationSnapshot(state=snapshot.state, control=active)
        return active

    @overload
    def mark_recovery_required(
        self,
        observation: OperationSnapshot,
        recovery: RecoveryRecord,
    ) -> OperationSnapshot: ...

    @overload
    def mark_recovery_required(
        self,
        observation: ControlObservation,
        recovery: RecoveryRecord,
    ) -> ControlObservation: ...

    def mark_recovery_required(
        self,
        observation: OperationSnapshot | ControlObservation,
        recovery: RecoveryRecord,
    ) -> OperationSnapshot | ControlObservation:
        snapshot, return_snapshot = self._snapshot_for_control(observation)
        self._backend._validate_active_control(self._address, snapshot.control)
        intent = cast(OperationIntent, snapshot.control.control.intent)
        if recovery.operation_id != intent.operation_id:
            raise StateIdentityError(
                "recovery record belongs to another deployment operation"
            )
        replacement = OperationControlState(
            address=self._address,
            status="recovery_required",
            intent=intent,
            progress=snapshot.control.control.progress,
            recovery=recovery,
        )
        recovery_observation = self._backend._save_control_locked(
            self._address,
            snapshot.control,
            replacement,
            self._lock,
        )
        self._active_operation_id = None
        if return_snapshot:
            return OperationSnapshot(
                state=snapshot.state,
                control=recovery_observation,
            )
        return recovery_observation

    @overload
    def clear_operation(
        self,
        observation: OperationSnapshot,
    ) -> OperationSnapshot: ...

    @overload
    def clear_operation(
        self,
        observation: ControlObservation,
    ) -> ControlObservation: ...

    def clear_operation(
        self,
        observation: OperationSnapshot | ControlObservation,
    ) -> OperationSnapshot | ControlObservation:
        snapshot, return_snapshot = self._snapshot_for_control(observation)
        self._backend._validate_active_control(self._address, snapshot.control)
        intent = cast(OperationIntent, snapshot.control.control.intent)
        if any(action.kafka_streams_evidence is not None for action in intent.actions):
            raise StateBackendRecoveryRequiredError(
                "Kafka Streams replacement requires typed commit or explicit recovery, not legacy clear"
            )
        cleared = self._backend._save_control_locked(
            self._address,
            snapshot.control,
            OperationControlState.clear(self._address),
            self._lock,
        )
        self._active_operation_id = None
        if return_snapshot:
            return OperationSnapshot(state=snapshot.state, control=cleared)
        # Compatibility delegate. It intentionally retains the existing broad
        # clear behavior until apply/adopt migrate to the typed finalizers.
        return cleared

    @staticmethod
    def _require_completed_operation(snapshot: OperationSnapshot) -> None:
        if not snapshot.control.control.actions_completed:
            raise StateBackendRecoveryRequiredError(
                "deployment operation is incomplete; explicit recovery is required"
            )

    def commit_operation(
        self,
        observation: OperationSnapshot,
        replacement: LocalState | None,
    ) -> OperationSnapshot:
        """Commit ownership before clearing local operation control.

        The two local files cannot be committed atomically.  Ordering the state
        write first ensures an uncertain clear normally leaves the prewritten
        marker blocking a successor instead of clearing before ownership is
        authoritative.
        """
        self._validate_snapshot(observation)
        self._backend._validate_active_control(
            self._address,
            observation.control,
        )
        self._require_completed_operation(observation)
        intent = cast(OperationIntent, observation.control.control.intent)
        intent.validate_kafka_streams_result_state(replacement if replacement is not None else observation.state.state)
        current_control = self._backend._read_control(self._address)
        if current_control.revision != observation.control.revision:
            raise StateBackendConflictError(
                "operation control state changed after it was observed"
            )

        committed_state = observation.state
        if replacement is not None:
            committed_state = self.compare_and_swap(observation.state, replacement)
        cleared = self._backend._save_control_locked(
            self._address,
            observation.control,
            OperationControlState.clear(self._address),
            self._lock,
        )
        self._active_operation_id = None
        return OperationSnapshot(state=committed_state, control=cleared)

    def clear_before_mutation(
        self,
        observation: OperationSnapshot,
    ) -> OperationSnapshot:
        """Clear an intent only when no durable action-start boundary exists."""
        self._validate_snapshot(observation)
        self._backend._validate_active_control(
            self._address,
            observation.control,
        )
        if observation.control.control.progress:
            raise StateBackendRecoveryRequiredError(
                "deployment operation may have started mutation; explicit recovery "
                "is required"
            )
        cleared = self._backend._save_control_locked(
            self._address, observation.control,
            OperationControlState.clear(self._address), self._lock,
        )
        self._active_operation_id = None
        return OperationSnapshot(state=observation.state, control=cleared)

    def finalize_recovery(
        self,
        observation: OperationSnapshot,
        evidence: RecoverySnapshotEvidence,
        resolution: RecoveryResolutionRecord,
        replacement: LocalState | None,
    ) -> OperationSnapshot:
        """Durably reconcile one blocked local operation and clear its marker.

        Local state and sidecars cannot share one atomic commit.  A declared,
        checksum-chained intent therefore precedes an optional ownership write;
        a matching resolution follows it before operation control is cleared.
        The exact intent also makes retries safe across either durable boundary.
        """
        self._active_operation_id = resolution.recovery_operation_id
        self._validate_snapshot(observation)
        current_control = self._backend._read_control(self._address)
        if current_control.revision != observation.control.revision:
            raise StateBackendConflictError(
                "operation control changed after the recovery snapshot was observed"
            )

        if observation.control.control.status == "clear":
            history = self._backend._read_recovery_history(self._address)
            matching_events = history.events_for(resolution.recovery_operation_id)
            if not matching_events:
                raise StateBackendRecoveryRequiredError(
                    "clear deployment state control is not recoverable"
                )
            if len(matching_events) != 2 or any(
                not _same_recovery_resolution_identity(event.record, resolution)
                for event in matching_events
            ):
                raise StateBackendConflictError("a conflicting recovery attempt already exists")
            # The immutable reviewed evidence necessarily contains the former
            # active control. Reconstruct only that validation view; the actual
            # fresh observation remains clear and is never mutated here.
            evidence_snapshot = OperationSnapshot(
                state=observation.state,
                control=ControlObservation(
                    control=evidence.control,
                    revision=observation.control.revision,
                ),
            )
            _validate_recovery_transition_inputs(
                evidence_snapshot,
                evidence,
                resolution,
                replacement,
            )
            expected_state = replacement if replacement is not None else evidence.state
            if observation.state.state != expected_state:
                raise StateBackendConflictError(
                    "completed recovery state does not match its audited result"
                )
            self._active_operation_id = None
            return observation

        prior_matches = _validate_recovery_transition_inputs(
            observation,
            evidence,
            resolution,
            replacement,
        )

        history = self._backend._read_recovery_history(self._address)
        matching_events = history.events_for(resolution.recovery_operation_id)
        if len(matching_events) > 2 or any(
            not _same_recovery_resolution_identity(event.record, resolution)
            for event in matching_events
        ):
            raise StateBackendConflictError("a conflicting recovery attempt already exists")
        if matching_events and matching_events[0].kind != "recovery_intent":
            raise StateBackendInvalidStateError("local recovery history is invalid")
        if len(matching_events) == 2 and (matching_events[1].kind != "recovery_resolution"):
            raise StateBackendInvalidStateError("local recovery history is invalid")
        if (
            not matching_events
            and history.events
            and (history.events[-1].kind == "recovery_intent")
        ):
            raise StateBackendConflictError("a different recovery attempt is already in progress")

        result_matches = not prior_matches
        if not matching_events:
            if not prior_matches:
                raise StateBackendConflictError(
                    "state changed after recovery evidence was reviewed"
                )
            history = self._backend._append_recovery_history_locked(
                self._address,
                history,
                "recovery_intent",
                resolution,
                self._lock,
            )
            matching_events = history.events_for(resolution.recovery_operation_id)

        committed_state = observation.state
        has_resolution = len(matching_events) == 2
        effective_resolution = matching_events[0].record
        if has_resolution and not (result_matches or (replacement is None and prior_matches)):
            raise StateBackendInvalidStateError(
                "local recovery resolution precedes its declared state result"
            )
        if replacement is not None and prior_matches:
            if has_resolution:
                raise StateBackendInvalidStateError(
                    "local recovery resolution precedes its ownership update"
                )
            try:
                committed_state = self.compare_and_swap(
                    observation.state,
                    replacement,
                )
            except StateBackendError:
                raise
            except StateConflictError as error:
                raise StateBackendConflictError(
                    "state changed while finalizing explicit recovery"
                ) from error
            except BaseException as error:
                raise StateBackendUnknownCommitError(
                    "local recovery ownership commit could not be confirmed",
                    operation_id=resolution.recovery_operation_id,
                ) from error
        elif replacement is not None:
            # A matching durable intent makes this the only permitted state
            # mismatch from the immutable reviewed evidence on retry.
            committed_state = observation.state

        if not has_resolution:
            history = self._backend._append_recovery_history_locked(
                self._address,
                history,
                "recovery_resolution",
                effective_resolution,
                self._lock,
            )
            matching_events = history.events_for(resolution.recovery_operation_id)
            if len(matching_events) != 2:
                raise StateBackendUnknownCommitError(
                    "local recovery audit commit could not be confirmed",
                    operation_id=resolution.recovery_operation_id,
                )

        final_state = self._backend._read(self._address)
        expected_state = replacement if replacement is not None else evidence.state
        if final_state.state != expected_state:
            raise StateBackendConflictError(
                "state changed before recovery control could be cleared"
            )
        committed_state = final_state
        try:
            cleared = self._backend._save_control_locked(
                self._address,
                observation.control,
                OperationControlState.clear(self._address),
                self._lock,
            )
        except StateBackendUnknownCommitError:
            # A clear may have reached durable storage before a later fsync
            # failed. Verify the exact terminal state instead of suggesting a
            # replay of a recovery that has already completed.
            current_control = self._backend._read_control(self._address)
            if current_control.control.status != "clear":
                raise
            cleared = current_control
        self._active_operation_id = None
        return OperationSnapshot(state=committed_state, control=cleared)

    def compare_and_swap(
        self,
        observation: StateObservation,
        replacement: LocalState,
    ) -> StateObservation:
        self._backend._validate_observation(self._address, observation)
        if (
            replacement.project != self._address.project
            or replacement.environment != self._address.environment
        ):
            raise StateIdentityError(
                "replacement state identity does not match its canonical address"
            )

        current = self._backend._read(self._address)
        if current.revision != observation.revision:
            raise StateBackendConflictError(
                "state revision changed after it was observed; reload state and "
                "produce a fresh plan"
            )
        self._lock.save_if_serial(
            replacement,
            expected_serial=observation.state_serial,
        )
        return self._backend._read(self._address)


class LocalDeploymentStateBackend:
    """Local backend preserving the existing strict version 1 JSON format."""

    def __init__(self, project_path: Path) -> None:
        self.project_path = Path(project_path)
        resolved_root = self.project_path.resolve()
        self._identity = StateStoreIdentity(
            backend="local",
            store_id=str(uuid.uuid5(uuid.NAMESPACE_URL, resolved_root.as_uri())),
        )

    def describe(self) -> StateStoreIdentity:
        return self._identity

    def _path(self, address: StateAddress) -> Path:
        if address.namespace != LOCAL_STATE_NAMESPACE:
            raise StateIdentityError(
                f"local state backend cannot serve namespace {address.namespace!r}"
            )
        return local_state_path(
            self.project_path,
            environment=address.environment,
        )

    def _control_path(self, address: StateAddress) -> Path:
        # Reuse the local namespace and environment validation of the v1 path.
        self._path(address)
        return local_control_path(
            self.project_path,
            environment=address.environment,
        )

    def _recovery_history_path(self, address: StateAddress) -> Path:
        # Reuse the local namespace and environment validation of the v1 path.
        self._path(address)
        return local_recovery_history_path(
            self.project_path,
            environment=address.environment,
        )

    @staticmethod
    def _load_recovery_history_payload(path: Path) -> object:
        def reject_duplicates(
            pairs: list[tuple[str, object]],
        ) -> dict[str, object]:
            result: dict[str, object] = {}
            for key, value in pairs:
                if key in result:
                    raise StateFormatError(
                        f"local recovery history contains duplicate field {key!r}"
                    )
                result[key] = value
            return result

        descriptor: int | None = None
        try:
            descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
            file_status = os.fstat(descriptor)
            if not stat.S_ISREG(file_status.st_mode):
                raise StateFormatError("local recovery history must be a regular file")
            if stat.S_IMODE(file_status.st_mode) != 0o600:
                raise StateFormatError("local recovery history must have mode 0600")
            chunks: list[bytes] = []
            remaining = MAX_LOCAL_RECOVERY_HISTORY_BYTES + 1
            while remaining:
                chunk = os.read(descriptor, min(64 * 1024, remaining))
                if not chunk:
                    break
                chunks.append(chunk)
                remaining -= len(chunk)
            payload = b"".join(chunks)
            if len(payload) > MAX_LOCAL_RECOVERY_HISTORY_BYTES:
                raise StateFormatError("local recovery history exceeds the size limit")
            return json.loads(
                payload.decode("utf-8"),
                object_pairs_hook=reject_duplicates,
            )
        except StateFormatError:
            raise
        except (json.JSONDecodeError, OSError, UnicodeError) as error:
            raise StateBackendInvalidStateError("local recovery history is unreadable") from error
        finally:
            if descriptor is not None:
                os.close(descriptor)

    def _read_recovery_history(
        self,
        address: StateAddress,
    ) -> _LocalRecoveryHistory:
        path = self._recovery_history_path(address)
        try:
            payload = self._load_recovery_history_payload(path)
        except StateBackendInvalidStateError as error:
            cause = error.__cause__
            if isinstance(cause, FileNotFoundError):
                return _LocalRecoveryHistory(address=address)
            raise
        except StateFormatError as error:
            raise StateBackendInvalidStateError("local recovery history is invalid") from error
        try:
            return _LocalRecoveryHistory.from_dict(
                payload,
                expected_address=address,
            )
        except StateIdentityError:
            raise
        except StateFormatError as error:
            raise StateBackendInvalidStateError("local recovery history is invalid") from error

    @staticmethod
    def _write_recovery_history(
        path: Path,
        history: _LocalRecoveryHistory,
        *,
        operation_id: str,
    ) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = json.dumps(history.to_dict(), indent=2, sort_keys=True)
        if len(payload.encode("utf-8")) > MAX_LOCAL_RECOVERY_HISTORY_BYTES:
            raise StateBackendInvalidStateError("local recovery history exceeds the size limit")
        temp_name: str | None = None
        file_descriptor: int | None = None
        try:
            file_descriptor, temp_name = tempfile.mkstemp(
                dir=path.parent,
                prefix=f".{path.name}.",
                suffix=".tmp",
            )
            os.fchmod(file_descriptor, 0o600)
            handle = os.fdopen(file_descriptor, "w", encoding="utf-8")
            file_descriptor = None
            with handle:
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temp_name, path)
            temp_name = None
            directory_fd = os.open(path.parent, os.O_RDONLY)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
        except StateBackendInvalidStateError:
            raise
        except BaseException as error:
            if file_descriptor is not None:
                try:
                    os.close(file_descriptor)
                except OSError:
                    pass
            if temp_name is not None:
                try:
                    Path(temp_name).unlink(missing_ok=True)
                except OSError:
                    pass
            raise StateBackendUnknownCommitError(
                "local recovery audit commit could not be confirmed",
                operation_id=operation_id,
            ) from error

    def _append_recovery_history_locked(
        self,
        address: StateAddress,
        observed: _LocalRecoveryHistory,
        kind: RecoveryHistoryEventKind,
        record: RecoveryResolutionRecord,
        lock: LocalStateOperationLock,
    ) -> _LocalRecoveryHistory:
        operation_id = record.recovery_operation_id
        if not lock.is_held:
            raise StateBackendLockLostError(
                "deployment state operation lock was lost",
                operation_id=operation_id,
            )
        current = self._read_recovery_history(address)
        if current != observed:
            raise StateBackendConflictError("local recovery history changed after it was observed")
        try:
            replacement = observed.append(kind, record)
        except StateFormatError as error:
            raise StateBackendInvalidStateError(
                "local recovery history cannot accept another event"
            ) from error
        self._write_recovery_history(
            self._recovery_history_path(address),
            replacement,
            operation_id=operation_id,
        )
        committed = self._read_recovery_history(address)
        if committed != replacement:
            raise StateBackendUnknownCommitError(
                "local recovery audit commit could not be confirmed",
                operation_id=operation_id,
            )
        return committed

    @staticmethod
    def _load_control_payload(path: Path) -> object:
        def reject_duplicates(
            pairs: list[tuple[str, object]],
        ) -> dict[str, object]:
            result: dict[str, object] = {}
            for key, value in pairs:
                if key in result:
                    raise StateFormatError(
                        f"operation control contains duplicate field {key!r}"
                    )
                result[key] = value
            return result

        try:
            with path.open(encoding="utf-8") as handle:
                return json.load(handle, object_pairs_hook=reject_duplicates)
        except StateFormatError:
            raise
        except (json.JSONDecodeError, OSError, UnicodeError) as error:
            raise StateBackendInvalidStateError(
                "local operation control state is unreadable"
            ) from error

    def _read_control(self, address: StateAddress) -> ControlObservation:
        path = self._control_path(address)
        if not path.exists():
            return ControlObservation(
                control=OperationControlState.clear(address),
                revision=StateRevision.absent(),
            )
        try:
            control = OperationControlState.from_dict(
                self._load_control_payload(path),
                expected_address=address,
            )
        except StateIdentityError:
            raise
        except StateFormatError as error:
            raise StateBackendInvalidStateError(
                "local operation control state is invalid"
            ) from error
        return ControlObservation(
            control=control,
            revision=_control_revision(control),
        )

    def read_control(self, address: StateAddress) -> ControlObservation:
        return self._read_control(address)

    @staticmethod
    def _write_control(
        path: Path,
        control: OperationControlState,
        *,
        operation_id: str | None,
    ) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = json.dumps(control.to_dict(), indent=2, sort_keys=True)
        temp_name: str | None = None
        file_descriptor: int | None = None
        try:
            file_descriptor, temp_name = tempfile.mkstemp(
                dir=path.parent,
                prefix=f".{path.name}.",
                suffix=".tmp",
            )
            os.fchmod(file_descriptor, 0o600)
            handle = os.fdopen(file_descriptor, "w", encoding="utf-8")
            file_descriptor = None
            with handle:
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temp_name, path)
            temp_name = None
            directory_fd = os.open(path.parent, os.O_RDONLY)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
        except BaseException as error:
            if file_descriptor is not None:
                try:
                    os.close(file_descriptor)
                except OSError:
                    pass
            if temp_name is not None:
                try:
                    Path(temp_name).unlink(missing_ok=True)
                except OSError:
                    pass
            raise StateBackendUnknownCommitError(
                "local operation control state commit could not be confirmed",
                operation_id=operation_id,
            ) from error

    def _validate_control_observation(
        self,
        address: StateAddress,
        observation: ControlObservation,
    ) -> None:
        if observation.control.address != address:
            raise StateIdentityError(
                "control observation belongs to another address"
            )

    def _validate_active_control(
        self,
        address: StateAddress,
        observation: ControlObservation,
    ) -> None:
        self._validate_control_observation(address, observation)
        if observation.control.status != "in_progress":
            raise StateBackendRecoveryRequiredError(
                "deployment operation is not in a writable in_progress state"
            )

    def _save_control_locked(
        self,
        address: StateAddress,
        observation: ControlObservation,
        replacement: OperationControlState,
        lock: LocalStateOperationLock,
    ) -> ControlObservation:
        self._validate_control_observation(address, observation)
        if replacement.address != address:
            raise StateIdentityError(
                "replacement control state belongs to another address"
            )
        intent = observation.control.intent or replacement.intent
        operation_id = intent.operation_id if intent is not None else None
        if not lock.is_held:
            raise StateBackendLockLostError(
                "deployment state operation lock was lost",
                operation_id=operation_id,
            )
        current = self._read_control(address)
        if current.revision != observation.revision:
            raise StateBackendConflictError(
                "operation control state changed after it was observed"
            )
        self._write_control(
            self._control_path(address),
            replacement,
            operation_id=operation_id,
        )
        return self._read_control(address)

    def _read(self, address: StateAddress) -> StateObservation:
        path = self._path(address)
        if path.exists():
            state = LocalState.load(
                path,
                expected_project=address.project,
                expected_environment=address.environment,
            )
            revision = _state_revision(state)
        else:
            state = LocalState(
                project=address.project,
                environment=address.environment,
            )
            revision = StateRevision.absent()
        return StateObservation(
            store=self._identity,
            address=address,
            state=state,
            revision=revision,
        )

    def read(self, address: StateAddress) -> StateObservation:
        return self._read(address)

    def _validate_observation(
        self,
        address: StateAddress,
        observation: StateObservation,
    ) -> None:
        if observation.store != self._identity:
            raise StateIdentityError("state observation belongs to another backend store")
        if observation.address != address:
            raise StateIdentityError("state observation belongs to another address")

    @contextmanager
    def operation(
        self,
        address: StateAddress,
    ) -> Iterator[DeploymentStateOperation]:
        path = self._path(address)
        with local_state_operation_lock(path) as lock:
            yield _LocalDeploymentStateOperation(self, address, lock)


@dataclass(frozen=True)
class DeploymentStateService:
    """Small provider-neutral application service for one state address."""

    backend: DeploymentStateBackend
    address: StateAddress

    @property
    def store(self) -> StateStoreIdentity:
        return self.backend.describe()

    def read(self) -> StateObservation:
        return self.backend.read(self.address)

    def read_control(self) -> ControlObservation:
        return self.backend.read_control(self.address)

    def operation(self) -> AbstractContextManager[DeploymentStateOperation]:
        return self.backend.operation(self.address)


def make_deployment_state_service(
    project_path: Path,
    *,
    project: str,
    environment: str,
    config: DeploymentStateConfig,
) -> DeploymentStateService:
    """Construct the configured provider without fallback between authorities."""
    if config.backend == "postgres":
        return _make_postgres_state_service(
            project=project,
            environment=environment,
            config=config,
            credential_scope="deployment",
        )

    address = StateAddress(
        namespace=LOCAL_STATE_NAMESPACE,
        project=project,
        environment=environment,
    )
    return DeploymentStateService(
        backend=LocalDeploymentStateBackend(project_path),
        address=address,
    )


def _make_postgres_state_service(
    *,
    project: str,
    environment: str,
    config: PostgresDeploymentStateConfig,
    credential_scope: Literal["deployment", "recovery"],
) -> DeploymentStateService:
    """Construct PostgreSQL state access from the dedicated writer binding."""
    writer_dsn_env = config.postgres.writer_dsn_env
    if writer_dsn_env is None:
        raise StateBackendUnavailableError(
            f"PostgreSQL {credential_scope} state credentials are not configured"
        )
    writer_dsn = os.environ.get(writer_dsn_env)
    if writer_dsn is None or not writer_dsn.strip():
        raise StateBackendUnavailableError(
            f"PostgreSQL {credential_scope} state credentials are unavailable"
        )

    # Keep the optional PostgreSQL dependency outside the local state path.
    from streamt.deployer.postgres_state_backend import (
        PrivatePostgresStateReadBackend,
    )

    address = StateAddress(
        namespace=config.namespace,
        project=project,
        environment=environment,
    )
    return DeploymentStateService(
        backend=cast(
            DeploymentStateBackend,
            PrivatePostgresStateReadBackend(
                dsn=writer_dsn,
                schema=config.postgres.schema_name,
                lock_timeout_seconds=config.lock_timeout_seconds,
                require_v2_writer=True,
            ),
        ),
        address=address,
    )


def make_recovery_state_service(
    project_path: Path,
    *,
    project: str,
    environment: str,
    config: DeploymentStateConfig,
) -> DeploymentStateService:
    """Construct the explicitly recovery-scoped deployment-state authority."""
    if config.backend == "local":
        return make_deployment_state_service(
            project_path,
            project=project,
            environment=environment,
            config=config,
        )

    return _make_postgres_state_service(
        project=project,
        environment=environment,
        config=config,
        credential_scope="recovery",
    )
