"""Provider-neutral access to deployment ownership state.

Only the local version 1 JSON provider is selectable today.  This module
defines the application boundary needed by future remote providers without
changing the local persistence format or exposing provider handles to CLI
commands.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import tempfile
import uuid
from collections.abc import Iterator
from contextlib import AbstractContextManager, contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Literal, Protocol, cast, runtime_checkable

from streamt.deployer.state import (
    LocalState,
    LocalStateOperationLock,
    StateConflictError,
    StateError,
    StateFormatError,
    StateIdentityError,
    local_state_operation_lock,
    local_state_path,
)

LOCAL_STATE_NAMESPACE = "local"
ABSENT_STATE_REVISION = "ABSENT"
CURRENT_CONTROL_VERSION = 1
_BACKEND_KIND_PATTERN = re.compile(r"^[a-z][a-z0-9_-]*$")
_CHECKSUM_PATTERN = re.compile(r"^sha256:[0-9a-f]{64}$")
_ACTION_PATTERN = re.compile(r"^[a-z][a-z0-9_-]*$")
_TIMESTAMP_PATTERN = re.compile(
    r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?Z$"
)


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


class StateBackendRecoveryRequiredError(StateBackendError):
    """An unfinished operation must be reconciled before mutation."""


class StateBackendUnknownCommitError(StateBackendError):
    """The backend cannot prove whether a state transition committed."""


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
class OperationAction:
    """One ordered runtime or state action covered by an operation intent."""

    index: int
    resource_id: str
    action: str

    def __post_init__(self) -> None:
        if type(self.index) is not int or self.index < 0:
            raise StateFormatError("operation action index must be a non-negative integer")
        _require_safe_text(self.resource_id, "operation action resource_id")
        if not isinstance(self.action, str) or not _ACTION_PATTERN.fullmatch(self.action):
            raise StateFormatError(
                "operation action must be a lowercase action identifier"
            )

    def to_dict(self) -> dict[str, object]:
        return {
            "index": self.index,
            "resource_id": self.resource_id,
            "action": self.action,
        }

    @classmethod
    def from_dict(cls, value: object) -> OperationAction:
        data = _strict_object(
            value,
            label="operation action",
            expected={"index", "resource_id", "action"},
        )
        return cls(
            index=cast(int, data["index"]),
            resource_id=cast(str, data["resource_id"]),
            action=cast(str, data["action"]),
        )


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
        if [action.index for action in self.actions] != list(range(len(self.actions))):
            raise StateFormatError("operation action indexes must be contiguous from zero")

    def to_dict(self) -> dict[str, object]:
        return {
            "operation_id": self.operation_id,
            "kind": self.kind,
            "started_at": self.started_at,
            "actor": self.actor,
            "prior_state_serial": self.prior_state_serial,
            "prior_state_checksum": self.prior_state_checksum,
            "reviewed_plan_checksum": self.reviewed_plan_checksum,
            "actions": [action.to_dict() for action in self.actions],
        }

    @classmethod
    def from_dict(cls, value: object) -> OperationIntent:
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
        return cls(
            operation_id=cast(str, data["operation_id"]),
            kind=cast(OperationKind, kind),
            started_at=cast(str, data["started_at"]),
            actor=cast(str, data["actor"]),
            prior_state_serial=cast(int, data["prior_state_serial"]),
            prior_state_checksum=cast(str, data["prior_state_checksum"]),
            reviewed_plan_checksum=cast(str | None, data["reviewed_plan_checksum"]),
            actions=tuple(OperationAction.from_dict(action) for action in raw_actions),
        )


ProgressStatus = Literal["started", "completed"]


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

    def __post_init__(self) -> None:
        _require_uuid(self.operation_id, "progress operation_id")
        if type(self.action_index) is not int or self.action_index < 0:
            raise StateFormatError("progress action_index must be a non-negative integer")
        _require_safe_text(self.resource_id, "progress resource_id")
        if not isinstance(self.action, str) or not _ACTION_PATTERN.fullmatch(self.action):
            raise StateFormatError("progress action must be a lowercase action identifier")
        if self.status not in ("started", "completed"):
            raise StateFormatError("progress status must be 'started' or 'completed'")
        if self.status == "started" and self.succeeded is not None:
            raise StateFormatError("started progress cannot have an outcome")
        if self.status == "completed" and type(self.succeeded) is not bool:
            raise StateFormatError("completed progress requires a boolean outcome")
        _require_timestamp(self.recorded_at, "progress recorded_at")

    def to_dict(self) -> dict[str, object]:
        return {
            "operation_id": self.operation_id,
            "action_index": self.action_index,
            "resource_id": self.resource_id,
            "action": self.action,
            "status": self.status,
            "succeeded": self.succeeded,
            "recorded_at": self.recorded_at,
        }

    @classmethod
    def from_dict(cls, value: object) -> OperationProgress:
        data = _strict_object(
            value,
            label="operation progress",
            expected={
                "operation_id",
                "action_index",
                "resource_id",
                "action",
                "status",
                "succeeded",
                "recorded_at",
            },
        )
        status = data["status"]
        if status not in ("started", "completed"):
            raise StateFormatError("progress status must be 'started' or 'completed'")
        return cls(
            operation_id=cast(str, data["operation_id"]),
            action_index=cast(int, data["action_index"]),
            resource_id=cast(str, data["resource_id"]),
            action=cast(str, data["action"]),
            status=cast(ProgressStatus, status),
            succeeded=cast(bool | None, data["succeeded"]),
            recorded_at=cast(str, data["recorded_at"]),
        )


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
        if type(self.control_version) is not int or self.control_version != CURRENT_CONTROL_VERSION:
            raise StateFormatError(
                f"unsupported control version {self.control_version!r}; "
                f"expected {CURRENT_CONTROL_VERSION}"
            )
        if self.status not in ("clear", "in_progress", "recovery_required"):
            raise StateFormatError("control status is invalid")
        if self.status == "clear":
            if self.intent is not None or self.progress or self.recovery is not None:
                raise StateFormatError("clear control state cannot contain operation data")
            return
        if self.intent is None:
            raise StateFormatError("active control state requires an operation intent")
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
        seen: set[tuple[int, str]] = set()
        successfully_completed: set[int] = set()
        previous_index = -1
        for item in self.progress:
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
            key = (item.action_index, item.status)
            if key in seen:
                raise StateFormatError("operation progress contains a duplicate boundary")
            if item.status == "completed" and (item.action_index, "started") not in seen:
                raise StateFormatError("completed progress requires a started boundary")
            if item.action_index > previous_index and item.status != "started":
                raise StateFormatError("a new progress action must start before completion")
            seen.add(key)
            if item.status == "completed" and item.succeeded is True:
                successfully_completed.add(item.action_index)
            previous_index = item.action_index

    @classmethod
    def clear(cls, address: StateAddress) -> OperationControlState:
        return cls(address=address)

    def to_dict(self) -> dict[str, object]:
        return {
            "control_version": self.control_version,
            "address": self.address.uri,
            "status": self.status,
            "intent": self.intent.to_dict() if self.intent is not None else None,
            "progress": [item.to_dict() for item in self.progress],
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
        return cls(
            control_version=cast(int, data["control_version"]),
            address=address,
            status=cast(ControlStatus, status),
            intent=(
                None
                if data["intent"] is None
                else OperationIntent.from_dict(data["intent"])
            ),
            progress=tuple(OperationProgress.from_dict(item) for item in raw_progress),
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

    def ensure_ready(self, observation: ControlObservation) -> None: ...

    def check_lock(self) -> None: ...

    def begin_operation(
        self,
        observation: ControlObservation,
        intent: OperationIntent,
    ) -> ControlObservation: ...

    def record_progress(
        self,
        observation: ControlObservation,
        progress: OperationProgress,
    ) -> ControlObservation: ...

    def mark_recovery_required(
        self,
        observation: ControlObservation,
        recovery: RecoveryRecord,
    ) -> ControlObservation: ...

    def clear_operation(
        self,
        observation: ControlObservation,
    ) -> ControlObservation: ...

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


def operation_timestamp() -> str:
    """Return a canonical UTC timestamp suitable for durable control records."""
    return datetime.now(timezone.utc).isoformat(timespec="microseconds").replace(
        "+00:00", "Z"
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

    def read(self) -> StateObservation:
        return self._backend._read(self._address)

    def read_control(self) -> ControlObservation:
        return self._backend._read_control(self._address)

    def check_lock(self) -> None:
        if not self._lock.is_held:
            raise StateBackendLockLostError(
                "deployment state operation lock was lost"
            )

    def ensure_ready(self, observation: ControlObservation) -> None:
        self._backend._validate_control_observation(self._address, observation)
        self.check_lock()
        if observation.control.status != "clear":
            raise StateBackendRecoveryRequiredError(
                "deployment state has an unfinished operation; explicit recovery "
                "is required before apply or adopt"
            )

    def begin_operation(
        self,
        observation: ControlObservation,
        intent: OperationIntent,
    ) -> ControlObservation:
        self.ensure_ready(observation)
        replacement = OperationControlState(
            address=self._address,
            status="in_progress",
            intent=intent,
        )
        return self._backend._save_control_locked(
            self._address,
            observation,
            replacement,
            self._lock,
        )

    def record_progress(
        self,
        observation: ControlObservation,
        progress: OperationProgress,
    ) -> ControlObservation:
        self._backend._validate_active_control(self._address, observation)
        self.check_lock()
        intent = cast(OperationIntent, observation.control.intent)
        if progress.operation_id != intent.operation_id:
            raise StateIdentityError(
                "progress belongs to another deployment operation"
            )
        replacement = OperationControlState(
            address=self._address,
            status="in_progress",
            intent=intent,
            progress=(*observation.control.progress, progress),
        )
        return self._backend._save_control_locked(
            self._address,
            observation,
            replacement,
            self._lock,
        )

    def mark_recovery_required(
        self,
        observation: ControlObservation,
        recovery: RecoveryRecord,
    ) -> ControlObservation:
        self._backend._validate_active_control(self._address, observation)
        self.check_lock()
        intent = cast(OperationIntent, observation.control.intent)
        if recovery.operation_id != intent.operation_id:
            raise StateIdentityError(
                "recovery record belongs to another deployment operation"
            )
        replacement = OperationControlState(
            address=self._address,
            status="recovery_required",
            intent=intent,
            progress=observation.control.progress,
            recovery=recovery,
        )
        return self._backend._save_control_locked(
            self._address,
            observation,
            replacement,
            self._lock,
        )

    def clear_operation(
        self,
        observation: ControlObservation,
    ) -> ControlObservation:
        self._backend._validate_active_control(self._address, observation)
        self.check_lock()
        return self._backend._save_control_locked(
            self._address,
            observation,
            OperationControlState.clear(self._address),
            self._lock,
        )

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
    def _write_control(path: Path, control: OperationControlState) -> None:
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
                "local operation control state commit could not be confirmed"
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
        if not lock.is_held:
            raise StateBackendLockLostError(
                "deployment state operation lock was lost"
            )
        current = self._read_control(address)
        if current.revision != observation.revision:
            raise StateBackendConflictError(
                "operation control state changed after it was observed"
            )
        self._write_control(self._control_path(address), replacement)
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
) -> DeploymentStateService:
    """Construct the only currently selectable backend: strict local JSON."""
    address = StateAddress(
        namespace=LOCAL_STATE_NAMESPACE,
        project=project,
        environment=environment,
    )
    return DeploymentStateService(
        backend=LocalDeploymentStateBackend(project_path),
        address=address,
    )
