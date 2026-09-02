"""Strict provider-neutral evidence for explicit deployment-state recovery."""

from __future__ import annotations

import hashlib
import json
import re
import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Literal, cast

from streamt.deployer.state import LocalState, ResourceIdentity, StateFormatError
from streamt.deployer.state_backend import (
    OperationAction,
    OperationControlState,
    OperationSnapshot,
    StateAddress,
    StateStoreIdentity,
    state_checksum,
)

RecoveryResolution = Literal[
    "observed",
    "rolled_back",
    "abandoned_before_mutation",
]
RecoveryTargetPresence = Literal["present", "absent"]
RecoveryAcceptedState = Literal["prior", "candidate"]

_CHECKSUM_PATTERN = re.compile(r"^sha256:[0-9a-f]{64}$")
_TIMESTAMP_PATTERN = re.compile(
    r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?Z$"
)
_CREDENTIAL_URL = re.compile(r"://([^:@/\s]+):([^@/\s]+)@")
_POSTGRES_URL = re.compile(r"\bpostgres(?:ql)?://", re.IGNORECASE)
_INLINE_SECRET = re.compile(
    r"(?:password|passwd|secret|token|api[_-]?key|authorization|bearer)\s*[=:]\s*\S+",
    re.IGNORECASE,
)


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


def _canonical_checksum(value: object) -> str:
    payload = json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return f"sha256:{hashlib.sha256(payload).hexdigest()}"


def _reject_unsafe_text(value: object, *, label: str) -> None:
    """Reject credentials in otherwise safe persisted state/control strings."""
    if isinstance(value, str):
        if any(ord(character) < 32 for character in value):
            raise StateFormatError(f"{label} contains unsafe control characters")
        if (
            _CREDENTIAL_URL.search(value)
            or _POSTGRES_URL.search(value)
            or _INLINE_SECRET.search(value)
        ):
            raise StateFormatError(f"{label} contains credential-like text")
        return
    if isinstance(value, list):
        for item in value:
            _reject_unsafe_text(item, label=label)
        return
    if isinstance(value, dict):
        for key, item in value.items():
            _reject_unsafe_text(key, label=label)
            _reject_unsafe_text(item, label=label)


def control_checksum(control: OperationControlState) -> str:
    """Hash strict control content without a provider-specific CAS revision."""
    return _canonical_checksum(control.to_dict())


@dataclass(frozen=True)
class RecoveryTargetEvidence:
    """One normalized, secret-free live observation bound to an intent action."""

    action: OperationAction
    presence: RecoveryTargetPresence
    accepted_as: RecoveryAcceptedState
    fingerprint: str

    def __post_init__(self) -> None:
        try:
            ResourceIdentity.parse(self.action.resource_id)
        except StateFormatError as error:
            raise StateFormatError(
                "recovery target action must use a canonical resource identity"
            ) from error
        if self.presence not in ("present", "absent"):
            raise StateFormatError("recovery target presence must be 'present' or 'absent'")
        if self.accepted_as not in ("prior", "candidate"):
            raise StateFormatError(
                "recovery target accepted_as must be 'prior' or 'candidate'"
            )
        _require_checksum(self.fingerprint, "recovery target fingerprint")

    def to_dict(self) -> dict[str, object]:
        return {
            "action": self.action.to_dict(),
            "presence": self.presence,
            "accepted_as": self.accepted_as,
            "fingerprint": self.fingerprint,
        }

    @classmethod
    def from_dict(cls, value: object) -> RecoveryTargetEvidence:
        data = _strict_object(
            value,
            label="recovery target evidence",
            expected={"action", "presence", "accepted_as", "fingerprint"},
        )
        presence = data["presence"]
        accepted_as = data["accepted_as"]
        if presence not in ("present", "absent"):
            raise StateFormatError("recovery target presence must be 'present' or 'absent'")
        if accepted_as not in ("prior", "candidate"):
            raise StateFormatError(
                "recovery target accepted_as must be 'prior' or 'candidate'"
            )
        return cls(
            action=OperationAction.from_dict(data["action"]),
            presence=cast(RecoveryTargetPresence, presence),
            accepted_as=cast(RecoveryAcceptedState, accepted_as),
            fingerprint=cast(str, data["fingerprint"]),
        )


@dataclass(frozen=True)
class RecoverySnapshotEvidence:
    """Exact portable state/control preimage without provider revision tokens."""

    store: StateStoreIdentity
    address: StateAddress
    state: LocalState
    state_checksum: str
    control: OperationControlState
    control_checksum: str

    def __post_init__(self) -> None:
        if (
            self.state.project != self.address.project
            or self.state.environment != self.address.environment
        ):
            raise StateFormatError("recovery state does not match its canonical address")
        if self.control.address != self.address:
            raise StateFormatError("recovery control does not match its canonical address")
        if self.control.status == "clear" or self.control.intent is None:
            raise StateFormatError("recovery evidence requires an active operation control")
        if state_checksum(self.state) != self.state_checksum:
            raise StateFormatError("recovery state checksum does not match state content")
        if control_checksum(self.control) != self.control_checksum:
            raise StateFormatError("recovery control checksum does not match control content")
        _reject_unsafe_text(self.state.to_dict(), label="recovery state")
        _reject_unsafe_text(self.control.to_dict(), label="recovery control")
        _reject_unsafe_text(
            {
                "backend": self.store.backend,
                "store_id": self.store.store_id,
                "address": self.address.uri,
            },
            label="recovery identity",
        )

    @property
    def blocked_operation_id(self) -> str:
        intent = self.control.intent
        if intent is None:  # pragma: no cover - guarded by __post_init__
            raise StateFormatError("recovery evidence requires an operation intent")
        return intent.operation_id

    @classmethod
    def from_operation_snapshot(
        cls,
        snapshot: OperationSnapshot,
    ) -> RecoverySnapshotEvidence:
        """Build portable evidence while deliberately dropping both CAS revisions."""
        return cls(
            store=snapshot.state.store,
            address=snapshot.address,
            state=snapshot.state.state,
            state_checksum=state_checksum(snapshot.state.state),
            control=snapshot.control.control,
            control_checksum=control_checksum(snapshot.control.control),
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "store": {
                "backend": self.store.backend,
                "store_id": self.store.store_id,
            },
            "address": self.address.uri,
            "state": self.state.to_dict(),
            "state_checksum": self.state_checksum,
            "control": self.control.to_dict(),
            "control_checksum": self.control_checksum,
        }

    @classmethod
    def from_dict(cls, value: object) -> RecoverySnapshotEvidence:
        data = _strict_object(
            value,
            label="recovery snapshot",
            expected={
                "store",
                "address",
                "state",
                "state_checksum",
                "control",
                "control_checksum",
            },
        )
        store_data = _strict_object(
            data["store"],
            label="recovery state store",
            expected={"backend", "store_id"},
        )
        try:
            address = StateAddress.parse(data["address"])
            return cls(
                store=StateStoreIdentity(
                    backend=cast(str, store_data["backend"]),
                    store_id=cast(str, store_data["store_id"]),
                ),
                address=address,
                state=LocalState.from_dict(
                    data["state"],
                    expected_project=address.project,
                    expected_environment=address.environment,
                ),
                state_checksum=cast(str, data["state_checksum"]),
                control=OperationControlState.from_dict(
                    data["control"],
                    expected_address=address,
                ),
                control_checksum=cast(str, data["control_checksum"]),
            )
        except (TypeError, AttributeError) as error:
            raise StateFormatError("recovery snapshot is invalid") from error


@dataclass(frozen=True)
class RecoveryResolutionRecord:
    """Sanitized append-only audit record for one completed explicit recovery."""

    address: StateAddress
    recovery_operation_id: str
    blocked_operation_id: str
    resolution: RecoveryResolution
    resolved_at: str
    evidence_checksum: str
    prior_state_serial: int
    prior_state_checksum: str
    result_state_serial: int
    result_state_checksum: str
    state_changed: bool

    def __post_init__(self) -> None:
        _require_uuid(self.recovery_operation_id, "recovery operation_id")
        _require_uuid(self.blocked_operation_id, "blocked operation_id")
        if self.recovery_operation_id == self.blocked_operation_id:
            raise StateFormatError(
                "recovery operation_id must differ from the blocked operation_id"
            )
        if self.resolution not in (
            "observed",
            "rolled_back",
            "abandoned_before_mutation",
        ):
            raise StateFormatError("recovery resolution is invalid")
        _require_timestamp(self.resolved_at, "recovery resolved_at")
        _require_checksum(self.evidence_checksum, "recovery evidence_checksum")
        if type(self.prior_state_serial) is not int or self.prior_state_serial < 0:
            raise StateFormatError(
                "recovery prior_state_serial must be a non-negative integer"
            )
        if type(self.result_state_serial) is not int or self.result_state_serial < 0:
            raise StateFormatError(
                "recovery result_state_serial must be a non-negative integer"
            )
        _require_checksum(self.prior_state_checksum, "recovery prior_state_checksum")
        _require_checksum(self.result_state_checksum, "recovery result_state_checksum")
        if type(self.state_changed) is not bool:
            raise StateFormatError("recovery state_changed must be boolean")
        if self.state_changed:
            if self.resolution != "observed":
                raise StateFormatError("only observed recovery may change ownership state")
            if self.result_state_serial != self.prior_state_serial + 1:
                raise StateFormatError(
                    "changed recovery state serial must increase by exactly one"
                )
            if self.result_state_checksum == self.prior_state_checksum:
                raise StateFormatError("changed recovery state must have a new checksum")
        elif (
            self.result_state_serial != self.prior_state_serial
            or self.result_state_checksum != self.prior_state_checksum
        ):
            raise StateFormatError(
                "unchanged recovery state must retain its serial and checksum"
            )
        _reject_unsafe_text(self.address.uri, label="recovery resolution address")

    def to_dict(self) -> dict[str, object]:
        return {
            "address": self.address.uri,
            "recovery_operation_id": self.recovery_operation_id,
            "blocked_operation_id": self.blocked_operation_id,
            "resolution": self.resolution,
            "resolved_at": self.resolved_at,
            "evidence_checksum": self.evidence_checksum,
            "prior_state_serial": self.prior_state_serial,
            "prior_state_checksum": self.prior_state_checksum,
            "result_state_serial": self.result_state_serial,
            "result_state_checksum": self.result_state_checksum,
            "state_changed": self.state_changed,
        }

    @classmethod
    def from_dict(cls, value: object) -> RecoveryResolutionRecord:
        data = _strict_object(
            value,
            label="recovery resolution record",
            expected={
                "address",
                "recovery_operation_id",
                "blocked_operation_id",
                "resolution",
                "resolved_at",
                "evidence_checksum",
                "prior_state_serial",
                "prior_state_checksum",
                "result_state_serial",
                "result_state_checksum",
                "state_changed",
            },
        )
        resolution = data["resolution"]
        if resolution not in (
            "observed",
            "rolled_back",
            "abandoned_before_mutation",
        ):
            raise StateFormatError("recovery resolution is invalid")
        return cls(
            address=StateAddress.parse(data["address"]),
            recovery_operation_id=cast(str, data["recovery_operation_id"]),
            blocked_operation_id=cast(str, data["blocked_operation_id"]),
            resolution=cast(RecoveryResolution, resolution),
            resolved_at=cast(str, data["resolved_at"]),
            evidence_checksum=cast(str, data["evidence_checksum"]),
            prior_state_serial=cast(int, data["prior_state_serial"]),
            prior_state_checksum=cast(str, data["prior_state_checksum"]),
            result_state_serial=cast(int, data["result_state_serial"]),
            result_state_checksum=cast(str, data["result_state_checksum"]),
            state_changed=cast(bool, data["state_changed"]),
        )
