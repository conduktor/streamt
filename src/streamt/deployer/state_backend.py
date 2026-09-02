"""Provider-neutral access to deployment ownership state.

Only the local version 1 JSON provider is selectable today.  This module
defines the application boundary needed by future remote providers without
changing the local persistence format or exposing provider handles to CLI
commands.
"""

from __future__ import annotations

import hashlib
import json
import uuid
from collections.abc import Iterator
from contextlib import AbstractContextManager, contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol, runtime_checkable

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
        _require_address_segment(self.backend, "backend")
        _require_address_segment(self.store_id, "store_id")


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


@runtime_checkable
class DeploymentStateOperation(Protocol):
    """Exclusive state operation used by mutating application workflows."""

    def read(self) -> StateObservation: ...

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

    def operation(
        self,
        address: StateAddress,
    ) -> AbstractContextManager[DeploymentStateOperation]: ...


def _state_revision(state: LocalState) -> StateRevision:
    payload = json.dumps(
        state.to_dict(),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")
    return StateRevision(f"sha256:{hashlib.sha256(payload).hexdigest()}")


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
