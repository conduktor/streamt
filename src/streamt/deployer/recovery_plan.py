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
from streamt.deployer.connect import ConnectClusterBinding, ConnectClusterBindingError
from streamt.deployer.recovery import (
    RecoveryResolution,
    RecoveryResolutionRecord,
    RecoverySnapshotEvidence,
    RecoveryTargetEvidence,
    _reject_unsafe_text,
    contains_connector_recovery_action,
)
from streamt.deployer.state import (
    LocalState,
    ManagedConnectorResourceDeletion,
    ManagedResourceRecord,
    ResourceIdentity,
    StateError,
)
from streamt.deployer.state_backend import (
    ConnectorActionEvidence,
    ConnectorActionSurfaceEvidence,
    OperationAction,
    state_checksum,
)

RECOVERY_PLAN_FILE_KIND = "streamt.recovery-plan"
RECOVERY_PLAN_FILE_VERSION = 3
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
    format_version: int = RECOVERY_PLAN_FILE_VERSION

    def __post_init__(self) -> None:
        if type(self.format_version) is not int or self.format_version not in (1, 2, 3):
            raise RecoveryPlanError(
                f"Unsupported recovery plan format version {self.format_version!r}; "
                "expected 1, 2, or 3"
            )
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
        snapshot_control_version = self.snapshot.control.control_version
        if self.format_version == 1 and snapshot_control_version != 1:
            raise RecoveryPlanError("Recovery plan format version 1 requires control version 1")
        if self.format_version == 2 and snapshot_control_version not in (1, 2):
            raise RecoveryPlanError(
                "Recovery plan format version 2 requires control version 1 or 2"
            )
        if self.format_version == 1:
            intent = self.snapshot.control.intent
            if (
                intent is not None
                and any(
                    action.gateway_evidence is not None or action.connector_evidence is not None
                    for action in intent.actions
                )
            ) or any(
                target.action.gateway_evidence is not None
                or target.action.connector_evidence is not None
                for target in self.targets
            ):
                raise RecoveryPlanError(
                    "Recovery plan format version 1 cannot contain action evidence"
                )
        if self.format_version in (1, 2):
            legacy_actions = (
                () if self.snapshot.control.intent is None else self.snapshot.control.intent.actions
            ) + tuple(target.action for target in self.targets)
            if any(
                action.connector_evidence is not None
                or contains_connector_recovery_action((action,))
                for action in legacy_actions
            ):
                raise RecoveryPlanError(
                    f"Recovery plan format version {self.format_version} cannot authorize "
                    "Connector deletion"
                )
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
        connector_recovery = contains_connector_recovery_action(intent.actions) or any(
            contains_connector_recovery_action((target.action,)) for target in self.targets
        )
        if connector_recovery and self.resolution != "abandoned_before_mutation":
            if self.format_version != 3 or snapshot_control_version != 3:
                raise RecoveryPlanError(
                    "Connector recovery requires recovery plan and control version 3"
                )
            if self.snapshot.store.backend != "postgres":
                raise RecoveryPlanError(
                    "Connector recovery requires PostgreSQL state authority"
                )
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
                if target.action.gateway_evidence is not None:
                    # Gateway provider presence is an exact aggregate surface and
                    # may differ from ownership-record presence during recreate.
                    # RecoveryTargetEvidence already binds it to the accepted
                    # durable current surface and fingerprint.
                    self._validate_gateway_prior_ownership(
                        target,
                        self.snapshot.state.resources.get(target.action.resource_id),
                    )
                    continue
                if target.action.connector_evidence is not None:
                    self._validate_connector_prior_ownership(
                        target,
                        self.snapshot.state.resources.get(target.action.resource_id),
                    )
                    continue
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

    @staticmethod
    def _validate_gateway_prior_ownership(
        target: RecoveryTargetEvidence,
        prior_record: object | None,
    ) -> None:
        """Bind a Gateway mutation to the exact reviewed prior ownership locator."""
        gateway_evidence = target.action.gateway_evidence
        if gateway_evidence is None:  # pragma: no cover - caller narrows this
            return
        exact_prior = isinstance(prior_record, ManagedResourceRecord) and (
            prior_record.backend == gateway_evidence.backend_identity
            and prior_record.physical_name == gateway_evidence.alias_name
        )
        if target.action.action == "adopt":
            if prior_record is not None:
                raise RecoveryPlanError(
                    "Gateway recovery adoption requires absent prior ownership evidence"
                )
        elif target.action.action == "create":
            if prior_record is not None and not exact_prior:
                raise RecoveryPlanError(
                    "Gateway recovery create has mismatched prior ownership evidence"
                )
        elif not exact_prior:
            raise RecoveryPlanError(
                "Gateway recovery mutation requires exact prior ownership evidence"
            )

    @staticmethod
    def _validate_gateway_candidate_ownership(
        target: RecoveryTargetEvidence,
        candidate_record: object | None,
    ) -> None:
        """Bind an accepted desired Gateway surface to candidate ownership."""
        gateway_evidence = target.action.gateway_evidence
        if gateway_evidence is None:  # pragma: no cover - caller narrows this
            return
        if not gateway_evidence.desired.exists:
            if candidate_record is not None:
                raise RecoveryPlanError(
                    "Gateway recovery deletion candidate must remove its ownership record"
                )
            return
        if not isinstance(candidate_record, ManagedResourceRecord) or (
            candidate_record.backend != gateway_evidence.backend_identity
            or candidate_record.physical_name != gateway_evidence.alias_name
        ):
            raise RecoveryPlanError(
                "Gateway recovery candidate requires exact desired ownership evidence"
            )
        if (
            target.action.action == "adopt"
            and candidate_record.ownership != "adopted"
        ):
            raise RecoveryPlanError(
                "Gateway recovery adoption candidate requires adopted ownership evidence"
            )

    def _validate_connector_prior_ownership(
        self,
        target: RecoveryTargetEvidence,
        prior_record: object | None,
    ) -> ManagedConnectorResourceDeletion:
        """Bind a Connector delete to one exact prior managed ownership record."""
        connector_evidence = target.action.connector_evidence
        if (
            type(target.action) is not OperationAction
            or type(target.action.index) is not int
            or type(target.action.resource_id) is not str
            or type(target.action.action) is not str
            or type(connector_evidence) is not ConnectorActionEvidence
            or type(connector_evidence.version) is not int
            or type(connector_evidence.backend_identity) is not str
            or type(connector_evidence.connector_name) is not str
            or type(connector_evidence.prior_artifact_checksum) is not str
            or type(connector_evidence.current) is not ConnectorActionSurfaceEvidence
            or type(connector_evidence.current.exists) is not bool
            or type(connector_evidence.current.fingerprint) is not str
            or type(connector_evidence.desired) is not ConnectorActionSurfaceEvidence
            or type(connector_evidence.desired.exists) is not bool
            or type(connector_evidence.desired.fingerprint) is not str
        ):
            raise RecoveryPlanError(
                "Connector recovery deletion requires exact action evidence"
            )
        if (
            type(prior_record) is not ManagedResourceRecord
            or type(prior_record.physical_name) is not str
            or type(prior_record.ownership) is not str
            or type(prior_record.artifact_checksum) is not str
            or type(prior_record.backend) is not str
            or prior_record.ownership != "managed"
            or prior_record.backend != connector_evidence.backend_identity
            or prior_record.physical_name != connector_evidence.connector_name
            or prior_record.artifact_checksum
            != connector_evidence.prior_artifact_checksum
        ):
            raise RecoveryPlanError(
                "Connector recovery deletion requires exact prior managed ownership evidence"
            )
        try:
            deletion_binding = ConnectClusterBinding.from_backend_identity(
                connector_evidence.backend_identity
            )
        except (ConnectClusterBindingError, StateError):
            raise RecoveryPlanError(
                "Connector recovery deletion has invalid prior provider identity"
            ) from None
        matching_provider_records = []
        for resource_id, record in self.snapshot.state.resources.items():
            try:
                identity = ResourceIdentity.parse(resource_id)
            except StateError:
                raise RecoveryPlanError(
                    "Connector recovery deletion has invalid prior provider identity"
                ) from None
            if identity.kind != "connector":
                continue
            if type(record) is not ManagedResourceRecord:
                raise RecoveryPlanError(
                    "Connector recovery deletion has invalid prior provider identity"
                )
            try:
                record_binding = ConnectClusterBinding.from_backend_identity(
                    record.backend
                )
            except ConnectClusterBindingError:
                raise RecoveryPlanError(
                    "Connector recovery deletion has invalid prior provider identity"
                ) from None
            if (
                record_binding.endpoint_fingerprint
                == deletion_binding.endpoint_fingerprint
                and record.physical_name == connector_evidence.connector_name
            ):
                matching_provider_records.append(resource_id)
        if matching_provider_records != [target.action.resource_id]:
            raise RecoveryPlanError(
                "Connector recovery deletion requires one exact prior provider identity"
            )
        return ManagedConnectorResourceDeletion(
            resource_id=target.action.resource_id,
            backend_identity=connector_evidence.backend_identity,
            connector_name=connector_evidence.connector_name,
            prior_artifact_checksum=connector_evidence.prior_artifact_checksum,
        )

    def managed_connector_deletions(
        self,
    ) -> tuple[ManagedConnectorResourceDeletion, ...]:
        """Return exact deletion claims proved by observed absent Connector targets."""
        if self.resolution != "observed":
            return ()
        deletions: list[ManagedConnectorResourceDeletion] = []
        provider_ids: set[tuple[str, str]] = set()
        for target in self.targets:
            connector_evidence = target.action.connector_evidence
            if connector_evidence is None or target.accepted_as != "candidate":
                continue
            if target.presence != "absent":
                raise RecoveryPlanError(
                    "Connector recovery deletion candidate requires exact absence"
                )
            if not any(
                progress.action_index == target.action.index
                and progress.status == "started"
                for progress in self.snapshot.control.progress
            ):
                raise RecoveryPlanError(
                    "Connector recovery cannot remove ownership for an action that never started"
                )
            deletion = self._validate_connector_prior_ownership(
                target,
                self.snapshot.state.resources.get(target.action.resource_id),
            )
            binding = ConnectClusterBinding.from_backend_identity(
                deletion.backend_identity
            )
            provider_id = (binding.endpoint_fingerprint, deletion.connector_name)
            if provider_id in provider_ids:
                raise RecoveryPlanError(
                    "Connector recovery contains a duplicate provider identity"
                )
            provider_ids.add(provider_id)
            deletions.append(deletion)
        return tuple(deletions)

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
            prior_record = prior.resources.get(resource_id)
            candidate_record = candidate.resources.get(resource_id)
            if target.accepted_as == "prior":
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
            if target.action.gateway_evidence is not None:
                # Ownership membership remains authoritative for candidate-state
                # validation, but not for the separately attested provider surface.
                self._validate_gateway_prior_ownership(target, prior_record)
                if target.accepted_as == "candidate":
                    self._validate_gateway_candidate_ownership(
                        target,
                        candidate_record,
                    )
                continue
            if target.action.connector_evidence is not None:
                self._validate_connector_prior_ownership(target, prior_record)
                if target.accepted_as == "candidate":
                    if candidate_record is not None:
                        raise RecoveryPlanError(
                            "Connector recovery deletion candidate must remove its "
                            "ownership record"
                        )
                elif candidate_record != prior_record:
                    raise RecoveryPlanError(
                        "Connector recovery target accepted as prior must retain its "
                        "prior ownership record"
                    )
                continue
            if target.presence != expected_presence:
                raise RecoveryPlanError(
                    "Observed recovery target presence does not match its accepted state"
                )
        # Constructing the claims is part of validation: only exact absent,
        # started Connector actions may authorize ownership removal.
        self.managed_connector_deletions()

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
            "format_version": self.format_version,
            "streamt_version": self.streamt_version,
            "resolution": self.resolution,
            "blocked_operation_id": self.blocked_operation_id,
            "recovery_operation_id": self.recovery_operation_id,
            "snapshot": self.snapshot.to_dict(),
            "targets": [self._target_dict(target) for target in self.targets],
            "candidate_state": (
                self.candidate_state.to_dict() if self.candidate_state is not None else None
            ),
            "environment_fingerprint": self.environment_fingerprint,
            "manifest_checksum": self.manifest_checksum,
        }

    def _target_dict(self, target: RecoveryTargetEvidence) -> dict[str, object]:
        serialized = target.to_dict()
        serialized["action"] = target.action.to_dict(
            control_version=self.format_version,
        )
        return serialized

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
    def from_dict(cls, value: object) -> RecoveryPlanFile:
        """Parse, checksum, validate, and detach one in-memory plan envelope."""
        try:
            canonical = json.dumps(
                value,
                ensure_ascii=False,
                allow_nan=False,
                separators=(",", ":"),
                sort_keys=True,
            )
            if len(canonical.encode("utf-8")) > MAX_RECOVERY_PLAN_FILE_BYTES:
                raise RecoveryPlanError("Recovery plan exceeds the 10 MiB size limit")
            value = json.loads(canonical)
        except RecoveryPlanError:
            raise
        except (TypeError, ValueError, UnicodeError):
            raise RecoveryPlanError("Recovery plan evidence is invalid") from None
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
        if type(data["format_version"]) is not int or data["format_version"] not in (
            1,
            2,
            RECOVERY_PLAN_FILE_VERSION,
        ):
            raise RecoveryPlanError(
                f"Unsupported recovery plan format version {data['format_version']!r}; "
                f"expected 1, 2, or {RECOVERY_PLAN_FILE_VERSION}"
            )
        format_version = data["format_version"]
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
            targets: list[RecoveryTargetEvidence] = []
            for raw_target in raw_targets:
                target_data = _strict_object(
                    raw_target,
                    label="target evidence",
                    expected={"action", "presence", "accepted_as", "fingerprint"},
                )
                action = OperationAction.from_dict(
                    target_data["action"],
                    control_version=format_version,
                )
                parsed_target = RecoveryTargetEvidence.from_dict(raw_target)
                targets.append(replace(parsed_target, action=action))
            return cls(
                resolution=_parse_resolution(data["resolution"]),
                blocked_operation_id=cast(str, data["blocked_operation_id"]),
                recovery_operation_id=cast(str, data["recovery_operation_id"]),
                snapshot=snapshot,
                targets=tuple(targets),
                candidate_state=candidate,
                environment_fingerprint=cast(
                    str | None, data["environment_fingerprint"]
                ),
                manifest_checksum=cast(str | None, data["manifest_checksum"]),
                streamt_version=data["streamt_version"],
                evidence_checksum=evidence_checksum,
                format_version=format_version,
            )
        except RecoveryPlanError:
            raise
        except StateError as error:
            raise RecoveryPlanError("Recovery plan evidence is invalid") from error

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
        return cls.from_dict(value)

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
