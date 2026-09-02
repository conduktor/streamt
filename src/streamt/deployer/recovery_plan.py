"""Deterministic, integrity-checked reviewed evidence for explicit recovery."""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import stat
import tempfile
import uuid
from dataclasses import dataclass, replace
from pathlib import Path
from typing import cast

from streamt import __version__
from streamt.deployer.recovery import (
    RecoveryResolution,
    RecoveryResolutionRecord,
    RecoverySnapshotEvidence,
    RecoveryTargetEvidence,
    _reject_unsafe_text,
)
from streamt.deployer.state import LocalState, StateError
from streamt.deployer.state_backend import state_checksum

RECOVERY_PLAN_FILE_KIND = "streamt.recovery-plan"
RECOVERY_PLAN_FILE_VERSION = 1
MAX_RECOVERY_PLAN_FILE_BYTES = 10 * 1024 * 1024

_CHECKSUM_PREFIX = "sha256:"
_CHECKSUM_LENGTH = len(_CHECKSUM_PREFIX) + 64


class RecoveryPlanError(ValueError):
    """A recovery plan is malformed, modified, unsafe, or internally inconsistent."""


def _canonical_json(value: object) -> str:
    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        )
    except (TypeError, ValueError) as error:
        raise RecoveryPlanError("Recovery plan data is not canonical JSON") from error


def _checksum(value: object) -> str:
    digest = hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()
    return f"sha256:{digest}"


def _is_checksum(value: object) -> bool:
    return (
        isinstance(value, str)
        and len(value) == _CHECKSUM_LENGTH
        and value.startswith(_CHECKSUM_PREFIX)
        and all(character in "0123456789abcdef" for character in value[7:])
    )


def _require_checksum(value: object, label: str) -> str:
    if not _is_checksum(value):
        raise RecoveryPlanError(
            f"Recovery plan {label} must be a sha256:<64 lowercase hex> value"
        )
    return cast(str, value)


def _require_uuid(value: object, label: str) -> str:
    if not isinstance(value, str):
        raise RecoveryPlanError(f"Recovery plan {label} must be a canonical UUID")
    try:
        parsed = uuid.UUID(value)
    except (ValueError, AttributeError) as error:
        raise RecoveryPlanError(
            f"Recovery plan {label} must be a canonical UUID"
        ) from error
    if str(parsed) != value:
        raise RecoveryPlanError(f"Recovery plan {label} must be a canonical UUID")
    return value


def _strict_object(
    value: object,
    *,
    label: str,
    expected: set[str],
) -> dict[str, object]:
    if not isinstance(value, dict):
        raise RecoveryPlanError(f"Recovery plan {label} must be an object")
    unknown = set(value) - expected
    missing = expected - set(value)
    if unknown:
        raise RecoveryPlanError(
            f"Recovery plan {label} has unknown field(s): {', '.join(sorted(unknown))}"
        )
    if missing:
        raise RecoveryPlanError(
            f"Recovery plan {label} is missing field(s): {', '.join(sorted(missing))}"
        )
    return value


def _parse_resolution(value: object) -> RecoveryResolution:
    if value not in ("observed", "rolled_back", "abandoned_before_mutation"):
        raise RecoveryPlanError("Recovery plan resolution is invalid")
    return cast(RecoveryResolution, value)


@dataclass(frozen=True)
class RecoveryPlanFile:
    """Versioned reviewed recovery evidence with an exact deterministic checksum."""

    resolution: RecoveryResolution
    blocked_operation_id: str
    recovery_operation_id: str
    snapshot: RecoverySnapshotEvidence
    targets: tuple[RecoveryTargetEvidence, ...]
    candidate_state: LocalState | None
    environment_fingerprint: str | None
    manifest_checksum: str | None
    streamt_version: str = __version__
    evidence_checksum: str = ""

    def __post_init__(self) -> None:
        if self.resolution not in (
            "observed",
            "rolled_back",
            "abandoned_before_mutation",
        ):
            raise RecoveryPlanError("Recovery plan resolution is invalid")
        _require_uuid(self.blocked_operation_id, "blocked_operation_id")
        _require_uuid(self.recovery_operation_id, "recovery_operation_id")
        if self.blocked_operation_id == self.recovery_operation_id:
            raise RecoveryPlanError(
                "Recovery plan operation IDs must identify different operations"
            )
        if self.blocked_operation_id != self.snapshot.blocked_operation_id:
            raise RecoveryPlanError(
                "Recovery plan blocked operation does not match the control intent"
            )
        if not isinstance(self.targets, tuple):
            raise RecoveryPlanError("Recovery plan targets must be an ordered tuple")
        if not isinstance(self.streamt_version, str) or not self.streamt_version:
            raise RecoveryPlanError("Recovery plan streamt_version must be non-empty")
        if any(ord(character) < 32 for character in self.streamt_version):
            raise RecoveryPlanError("Recovery plan streamt_version contains unsafe text")
        try:
            _reject_unsafe_text(
                self.streamt_version,
                label="recovery plan streamt_version",
            )
        except StateError as error:
            raise RecoveryPlanError("Recovery plan streamt_version is unsafe") from error
        if self.evidence_checksum:
            _require_checksum(self.evidence_checksum, "evidence_checksum")
            if not hmac.compare_digest(
                self.evidence_checksum,
                _checksum(self._unsigned_dict()),
            ):
                raise RecoveryPlanError(
                    "Recovery plan checksum does not match its evidence"
                )

        intent = self.snapshot.control.intent
        if intent is None:  # pragma: no cover - RecoverySnapshotEvidence rejects this
            raise RecoveryPlanError("Recovery plan requires an active operation intent")
        if (
            self.resolution != "abandoned_before_mutation"
            and tuple(target.action for target in self.targets) != intent.actions
        ):
            raise RecoveryPlanError(
                "Recovery plan requires exactly one evidence target per intent action"
            )
        for target in self.targets:
            identity = target.action.resource_id.removeprefix("streamt://").split("/")
            if (
                len(identity) != 4
                or identity[0] != self.snapshot.address.project
                or identity[1] != self.snapshot.address.environment
            ):
                raise RecoveryPlanError(
                    "Recovery plan target belongs to another project or environment"
                )

        if self.resolution == "abandoned_before_mutation":
            self._validate_abandoned(intent.prior_state_serial, intent.prior_state_checksum)
        elif self.resolution == "rolled_back":
            self._validate_project_fingerprints()
            self._validate_prior_state(intent.prior_state_serial, intent.prior_state_checksum)
            if self.candidate_state is not None:
                raise RecoveryPlanError("Rolled-back recovery cannot contain candidate state")
            for target in self.targets:
                if target.accepted_as != "prior":
                    raise RecoveryPlanError(
                        "Rolled-back recovery targets must all be accepted as prior state"
                    )
                expected_presence = (
                    "present"
                    if target.action.resource_id in self.snapshot.state.resources
                    else "absent"
                )
                if target.presence != expected_presence:
                    raise RecoveryPlanError(
                        "Rolled-back recovery target presence does not match prior state"
                    )
        else:
            self._validate_project_fingerprints()
            self._validate_observed_candidate()

    def _validate_project_fingerprints(self) -> None:
        _require_checksum(self.environment_fingerprint, "environment_fingerprint")
        _require_checksum(self.manifest_checksum, "manifest_checksum")

    def _validate_prior_state(self, serial: int, checksum: str) -> None:
        if self.snapshot.state.serial != serial or self.snapshot.state_checksum != checksum:
            raise RecoveryPlanError(
                "Recovery plan state does not match the blocked operation prior state"
            )

    def _validate_abandoned(self, serial: int, checksum: str) -> None:
        self._validate_prior_state(serial, checksum)
        if self.snapshot.control.progress:
            raise RecoveryPlanError(
                "Abandoned-before-mutation recovery requires empty durable progress"
            )
        if self.targets:
            raise RecoveryPlanError(
                "Abandoned-before-mutation recovery cannot contain live target evidence"
            )
        if self.candidate_state is not None:
            raise RecoveryPlanError(
                "Abandoned-before-mutation recovery cannot contain candidate state"
            )
        if self.environment_fingerprint is not None or self.manifest_checksum is not None:
            raise RecoveryPlanError(
                "Abandoned-before-mutation recovery cannot contain project fingerprints"
            )

    def _validate_observed_candidate(self) -> None:
        candidate = self.candidate_state
        if candidate is None:
            raise RecoveryPlanError("Observed recovery requires exact candidate state")
        if (
            candidate.project != self.snapshot.address.project
            or candidate.environment != self.snapshot.address.environment
        ):
            raise RecoveryPlanError(
                "Recovery candidate state belongs to another project or environment"
            )
        try:
            _reject_unsafe_text(candidate.to_dict(), label="recovery candidate state")
        except StateError as error:
            raise RecoveryPlanError("Recovery candidate state contains unsafe text") from error
        prior = self.snapshot.state
        resources_changed = candidate.resources != prior.resources
        expected_serial = prior.serial + 1 if resources_changed else prior.serial
        if candidate.serial != expected_serial:
            raise RecoveryPlanError(
                "Recovery candidate serial must increase exactly when ownership changes"
            )
        target_ids = {target.action.resource_id for target in self.targets}
        prior_unrelated = {
            resource_id: record
            for resource_id, record in prior.resources.items()
            if resource_id not in target_ids
        }
        candidate_unrelated = {
            resource_id: record
            for resource_id, record in candidate.resources.items()
            if resource_id not in target_ids
        }
        if prior_unrelated != candidate_unrelated:
            raise RecoveryPlanError(
                "Observed recovery candidate changes resources outside the blocked intent"
            )
        for target in self.targets:
            resource_id = target.action.resource_id
            if target.accepted_as == "prior":
                prior_record = prior.resources.get(resource_id)
                candidate_record = candidate.resources.get(resource_id)
                if candidate_record != prior_record:
                    raise RecoveryPlanError(
                        "Observed recovery target accepted as prior must retain its "
                        "prior ownership record"
                    )
                expected_presence = "present" if prior_record is not None else "absent"
            else:
                expected_presence = (
                    "present" if resource_id in candidate.resources else "absent"
                )
            if target.presence != expected_presence:
                raise RecoveryPlanError(
                    "Observed recovery target presence does not match its accepted state"
                )

    @classmethod
    def create(
        cls,
        *,
        resolution: RecoveryResolution,
        recovery_operation_id: str,
        snapshot: RecoverySnapshotEvidence,
        targets: tuple[RecoveryTargetEvidence, ...],
        candidate_state: LocalState | None = None,
        environment_fingerprint: str | None = None,
        manifest_checksum: str | None = None,
    ) -> RecoveryPlanFile:
        """Create and checksum reviewed evidence for one exact recovery attempt."""
        plan = cls(
            resolution=resolution,
            blocked_operation_id=snapshot.blocked_operation_id,
            recovery_operation_id=recovery_operation_id,
            snapshot=snapshot,
            targets=targets,
            candidate_state=candidate_state,
            environment_fingerprint=environment_fingerprint,
            manifest_checksum=manifest_checksum,
        )
        return replace(plan, evidence_checksum=_checksum(plan._unsigned_dict()))

    def _unsigned_dict(self) -> dict[str, object]:
        return {
            "kind": RECOVERY_PLAN_FILE_KIND,
            "format_version": RECOVERY_PLAN_FILE_VERSION,
            "streamt_version": self.streamt_version,
            "resolution": self.resolution,
            "blocked_operation_id": self.blocked_operation_id,
            "recovery_operation_id": self.recovery_operation_id,
            "snapshot": self.snapshot.to_dict(),
            "targets": [target.to_dict() for target in self.targets],
            "candidate_state": (
                self.candidate_state.to_dict() if self.candidate_state is not None else None
            ),
            "environment_fingerprint": self.environment_fingerprint,
            "manifest_checksum": self.manifest_checksum,
        }

    def to_dict(self) -> dict[str, object]:
        """Return the complete recovery-plan envelope including integrity evidence."""
        return {**self._unsigned_dict(), "evidence_checksum": self.evidence_checksum}

    def save(self, path: Path) -> None:
        """Create a restrictive complete file atomically without following or overwriting."""
        if not self.evidence_checksum or not hmac.compare_digest(
            self.evidence_checksum,
            _checksum(self._unsigned_dict()),
        ):
            raise RecoveryPlanError("Recovery plan must have valid integrity evidence")
        content = json.dumps(
            self.to_dict(),
            ensure_ascii=False,
            allow_nan=False,
            indent=2,
            sort_keys=True,
        ) + "\n"
        encoded = content.encode("utf-8")
        if len(encoded) > MAX_RECOVERY_PLAN_FILE_BYTES:
            raise RecoveryPlanError("Recovery plan exceeds the 10 MiB size limit")

        requested = Path(path)
        try:
            requested.parent.mkdir(parents=True, exist_ok=True)
            parent = requested.parent.resolve(strict=True)
        except OSError as error:
            raise RecoveryPlanError("Cannot prepare recovery plan destination") from error
        target = parent / requested.name
        try:
            os.lstat(target)
        except FileNotFoundError:
            pass
        except OSError as error:
            raise RecoveryPlanError("Cannot inspect recovery plan destination") from error
        else:
            raise RecoveryPlanError("Recovery plan destination already exists")

        temp_path: Path | None = None
        installed = False
        try:
            with tempfile.NamedTemporaryFile(
                mode="wb",
                dir=parent,
                prefix=f".{target.name}.",
                suffix=".tmp",
                delete=False,
            ) as handle:
                temp_path = Path(handle.name)
                os.chmod(temp_path, 0o600)
                handle.write(encoded)
                handle.flush()
                os.fsync(handle.fileno())
            try:
                os.link(temp_path, target, follow_symlinks=False)
            except FileExistsError as error:
                raise RecoveryPlanError(
                    "Recovery plan destination already exists"
                ) from error
            installed = True
            directory_fd = os.open(parent, os.O_RDONLY)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
        except RecoveryPlanError:
            raise
        except OSError as error:
            message = (
                "Recovery plan was created but directory durability is unverified"
                if installed
                else "Cannot write recovery plan file"
            )
            raise RecoveryPlanError(message) from error
        finally:
            if temp_path is not None:
                try:
                    temp_path.unlink()
                except FileNotFoundError:
                    pass

    @classmethod
    def load(cls, path: Path) -> RecoveryPlanFile:
        """Load a regular file without following symlinks and validate every field."""
        flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
        try:
            file_descriptor = os.open(Path(path), flags)
        except OSError as error:
            raise RecoveryPlanError("Cannot read recovery plan file") from error
        try:
            file_stat = os.fstat(file_descriptor)
            if not stat.S_ISREG(file_stat.st_mode):
                raise RecoveryPlanError("Recovery plan path must be a regular file")
            if file_stat.st_size > MAX_RECOVERY_PLAN_FILE_BYTES:
                raise RecoveryPlanError("Recovery plan exceeds the 10 MiB size limit")
            with os.fdopen(file_descriptor, mode="r", encoding="utf-8") as handle:
                file_descriptor = -1
                raw = handle.read(MAX_RECOVERY_PLAN_FILE_BYTES + 1)
        except RecoveryPlanError:
            raise
        except (OSError, UnicodeError) as error:
            raise RecoveryPlanError("Cannot read recovery plan file") from error
        finally:
            if file_descriptor >= 0:
                os.close(file_descriptor)
        if len(raw.encode("utf-8")) > MAX_RECOVERY_PLAN_FILE_BYTES:
            raise RecoveryPlanError("Recovery plan exceeds the 10 MiB size limit")

        def reject_duplicates(pairs: list[tuple[str, object]]) -> dict[str, object]:
            result: dict[str, object] = {}
            for key, value in pairs:
                if key in result:
                    raise RecoveryPlanError(
                        f"Recovery plan contains duplicate field {key!r}"
                    )
                result[key] = value
            return result

        def reject_constant(value: str) -> object:
            raise RecoveryPlanError(
                f"Recovery plan contains non-finite number {value!r}"
            )

        try:
            value = json.loads(
                raw,
                object_pairs_hook=reject_duplicates,
                parse_constant=reject_constant,
            )
        except RecoveryPlanError:
            raise
        except json.JSONDecodeError as error:
            raise RecoveryPlanError("Recovery plan is not valid UTF-8 JSON") from error
        data = _strict_object(
            value,
            label="file root",
            expected={
                "kind",
                "format_version",
                "streamt_version",
                "resolution",
                "blocked_operation_id",
                "recovery_operation_id",
                "snapshot",
                "targets",
                "candidate_state",
                "environment_fingerprint",
                "manifest_checksum",
                "evidence_checksum",
            },
        )
        if data["kind"] != RECOVERY_PLAN_FILE_KIND:
            raise RecoveryPlanError("Unsupported recovery plan kind")
        if (
            type(data["format_version"]) is not int
            or data["format_version"] != RECOVERY_PLAN_FILE_VERSION
        ):
            raise RecoveryPlanError(
                f"Unsupported recovery plan format version {data['format_version']!r}; "
                f"expected {RECOVERY_PLAN_FILE_VERSION}"
            )
        evidence_checksum = _require_checksum(
            data["evidence_checksum"], "evidence_checksum"
        )
        unsigned = {key: item for key, item in data.items() if key != "evidence_checksum"}
        if not hmac.compare_digest(evidence_checksum, _checksum(unsigned)):
            raise RecoveryPlanError(
                "Recovery plan checksum mismatch; the file was modified or is incomplete"
            )
        if not isinstance(data["streamt_version"], str) or not data["streamt_version"]:
            raise RecoveryPlanError("Recovery plan streamt_version must be non-empty")
        raw_targets = data["targets"]
        if not isinstance(raw_targets, list):
            raise RecoveryPlanError("Recovery plan targets must be an array")

        try:
            snapshot = RecoverySnapshotEvidence.from_dict(data["snapshot"])
            raw_candidate = data["candidate_state"]
            candidate = (
                None
                if raw_candidate is None
                else LocalState.from_dict(
                    raw_candidate,
                    expected_project=snapshot.address.project,
                    expected_environment=snapshot.address.environment,
                )
            )
            return cls(
                resolution=_parse_resolution(data["resolution"]),
                blocked_operation_id=cast(str, data["blocked_operation_id"]),
                recovery_operation_id=cast(str, data["recovery_operation_id"]),
                snapshot=snapshot,
                targets=tuple(
                    RecoveryTargetEvidence.from_dict(target) for target in raw_targets
                ),
                candidate_state=candidate,
                environment_fingerprint=cast(
                    str | None, data["environment_fingerprint"]
                ),
                manifest_checksum=cast(str | None, data["manifest_checksum"]),
                streamt_version=data["streamt_version"],
                evidence_checksum=evidence_checksum,
            )
        except RecoveryPlanError:
            raise
        except StateError as error:
            raise RecoveryPlanError("Recovery plan evidence is invalid") from error

    def make_resolution_record(
        self,
        *,
        resolved_at: str,
    ) -> RecoveryResolutionRecord:
        """Build the exact audit record implied by this already validated plan."""
        prior = self.snapshot.state
        result = self.candidate_state if self.candidate_state is not None else prior
        return RecoveryResolutionRecord(
            address=self.snapshot.address,
            recovery_operation_id=self.recovery_operation_id,
            blocked_operation_id=self.blocked_operation_id,
            resolution=self.resolution,
            resolved_at=resolved_at,
            evidence_checksum=self.evidence_checksum,
            prior_state_serial=prior.serial,
            prior_state_checksum=self.snapshot.state_checksum,
            result_state_serial=result.serial,
            result_state_checksum=state_checksum(result),
            state_changed=result.resources != prior.resources,
        )
