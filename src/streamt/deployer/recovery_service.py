"""Provider-neutral orchestration for reviewed deployment-state recovery."""

from __future__ import annotations

import hmac
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Protocol

from streamt.deployer.recovery import (
    RecoveryResolution,
    RecoverySnapshotEvidence,
    RecoveryTargetEvidence,
)
from streamt.deployer.recovery_plan import RecoveryPlanError, RecoveryPlanFile
from streamt.deployer.state import LocalState
from streamt.deployer.state_backend import (
    DeploymentStateService,
    OperationControlState,
    OperationSnapshot,
)


class RecoveryServiceError(ValueError):
    """Reviewed recovery evidence is missing, stale, or inconsistent."""


@dataclass(frozen=True)
class RecoveryProjectContext:
    """Portable fingerprints for the exact project inputs used during recovery."""

    environment_fingerprint: str
    manifest_checksum: str


@dataclass(frozen=True)
class RecoveryLiveObservation:
    """Normalized, secret-free result of observing every blocked intent target."""

    targets: tuple[RecoveryTargetEvidence, ...]
    candidate_state: LocalState | None


class RecoveryContextReader(Protocol):
    """Supplies current project fingerprints without exposing runtime credentials."""

    def read_recovery_context(self) -> RecoveryProjectContext: ...


class RecoveryTargetObserver(Protocol):
    """Converts live provider state into portable evidence for one blocked intent."""

    def observe_recovery_targets(
        self,
        *,
        resolution: RecoveryResolution,
        snapshot: RecoverySnapshotEvidence,
    ) -> RecoveryLiveObservation: ...


def _new_operation_id() -> str:
    return str(uuid.uuid4())


def _resolved_at() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


@dataclass(frozen=True)
class RecoveryService:
    """Plan and execute explicit recovery while state-operation authority is held."""

    state: DeploymentStateService
    operation_id_factory: Callable[[], str] = _new_operation_id
    resolved_at_factory: Callable[[], str] = _resolved_at

    def create_plan(
        self,
        *,
        resolution: RecoveryResolution,
        destination: Path,
        observer: RecoveryTargetObserver | None = None,
        context_reader: RecoveryContextReader | None = None,
    ) -> RecoveryPlanFile:
        """Observe and durably save one no-overwrite recovery plan under the lock."""
        with self.state.operation() as operation:
            snapshot = operation.observe()
            self._require_snapshot_identity(snapshot)
            if snapshot.control.control.status == "clear":
                raise RecoveryServiceError("Recovery planning requires an active blocked operation")
            evidence = RecoverySnapshotEvidence.from_operation_snapshot(snapshot)

            if resolution == "abandoned_before_mutation":
                live = RecoveryLiveObservation(targets=(), candidate_state=None)
                context = None
            else:
                context = self._read_context(context_reader)
                live = self._observe_targets(observer, resolution, evidence)
                if self._read_context(context_reader) != context:
                    raise RecoveryServiceError(
                        "Project inputs changed while recovery evidence was collected"
                    )

            reread = operation.observe()
            self._require_snapshot_identity(reread)
            if reread != snapshot:
                raise RecoveryServiceError(
                    "State or operation control changed while recovery evidence was collected"
                )
            operation.check_lock()

            plan = RecoveryPlanFile.create(
                resolution=resolution,
                recovery_operation_id=self.operation_id_factory(),
                snapshot=evidence,
                targets=live.targets,
                candidate_state=live.candidate_state,
                environment_fingerprint=(
                    context.environment_fingerprint if context is not None else None
                ),
                manifest_checksum=context.manifest_checksum if context is not None else None,
            )
            plan.save(destination)
            operation.check_lock()
            return plan

    def execute_plan(
        self,
        plan_or_path: RecoveryPlanFile | Path,
        *,
        confirm_operation_id: str,
        confirm_resolution: str,
        confirm_evidence_checksum: str,
        observer: RecoveryTargetObserver | None = None,
        context_reader: RecoveryContextReader | None = None,
    ) -> OperationSnapshot:
        """Revalidate live evidence and atomically finalize one reviewed recovery."""
        plan = self._validated_plan(plan_or_path)
        self._verify_confirmations(
            plan,
            operation_id=confirm_operation_id,
            resolution=confirm_resolution,
            evidence_checksum=confirm_evidence_checksum,
        )
        self._require_plan_identity(plan)

        with self.state.operation() as operation:
            current = operation.observe()
            self._require_snapshot_identity(current)
            expected_state = plan.candidate_state or plan.snapshot.state
            partial_candidate = (
                plan.resolution == "observed"
                and plan.candidate_state is not None
                and plan.candidate_state.resources != plan.snapshot.state.resources
                and current.state.state == plan.candidate_state
            )
            blocked_retry = (
                current.control.control == plan.snapshot.control
                and (
                    current.state.state == plan.snapshot.state
                    or partial_candidate
                )
            )
            already_completed = (
                current.state.state == expected_state
                and current.control.control
                == OperationControlState.clear(self.state.address)
            )
            if not blocked_retry and not already_completed:
                raise RecoveryServiceError(
                    "State or operation control changed after recovery evidence was reviewed"
                )

            if not already_completed:
                if plan.resolution == "abandoned_before_mutation":
                    live = RecoveryLiveObservation(targets=(), candidate_state=None)
                else:
                    context = self._read_context(context_reader)
                    self._require_context_matches_plan(context, plan)
                    live = self._observe_targets(observer, plan.resolution, plan.snapshot)
                    if self._read_context(context_reader) != context:
                        raise RecoveryServiceError(
                            "Project inputs changed while recovery targets were observed"
                        )
                if live.targets != plan.targets or live.candidate_state != plan.candidate_state:
                    raise RecoveryServiceError(
                        "Live targets changed after recovery evidence was reviewed"
                    )

            operation.check_lock()
            record = plan.make_resolution_record(resolved_at=self.resolved_at_factory())
            replacement = plan.candidate_state if record.state_changed else None
            result = operation.finalize_recovery(
                current,
                plan.snapshot,
                record,
                replacement,
            )
            self._verify_result(plan, result)
            operation.check_lock()
            return result

    def _require_snapshot_identity(self, snapshot: OperationSnapshot) -> None:
        if snapshot.address != self.state.address or snapshot.state.store != self.state.store:
            raise RecoveryServiceError(
                "Recovery state does not match the configured store and address"
            )

    def _require_plan_identity(self, plan: RecoveryPlanFile) -> None:
        if plan.snapshot.address != self.state.address or plan.snapshot.store != self.state.store:
            raise RecoveryServiceError(
                "Recovery plan does not match the configured store and address"
            )

    @staticmethod
    def _require_context_matches_plan(
        context: RecoveryProjectContext,
        plan: RecoveryPlanFile,
    ) -> None:
        if (
            context.environment_fingerprint != plan.environment_fingerprint
            or context.manifest_checksum != plan.manifest_checksum
        ):
            raise RecoveryServiceError(
                "Project inputs changed after recovery evidence was reviewed"
            )

    @staticmethod
    def _validated_plan(plan_or_path: RecoveryPlanFile | Path) -> RecoveryPlanFile:
        plan = (
            plan_or_path
            if isinstance(plan_or_path, RecoveryPlanFile)
            else RecoveryPlanFile.load(plan_or_path)
        )
        if not plan.evidence_checksum:
            raise RecoveryPlanError("Recovery plan requires integrity evidence")
        return plan

    @staticmethod
    def _verify_confirmations(
        plan: RecoveryPlanFile,
        *,
        operation_id: str,
        resolution: str,
        evidence_checksum: str,
    ) -> None:
        if not hmac.compare_digest(operation_id, plan.blocked_operation_id):
            raise RecoveryServiceError(
                "Blocked operation confirmation does not match the recovery plan"
            )
        if not hmac.compare_digest(resolution, plan.resolution):
            raise RecoveryServiceError("Resolution confirmation does not match the recovery plan")
        if not hmac.compare_digest(evidence_checksum, plan.evidence_checksum):
            raise RecoveryServiceError(
                "Evidence checksum confirmation does not match the recovery plan"
            )

    @staticmethod
    def _read_context(
        context_reader: RecoveryContextReader | None,
    ) -> RecoveryProjectContext:
        if context_reader is None:
            raise RecoveryServiceError("Recovery requires current project fingerprint evidence")
        try:
            context = context_reader.read_recovery_context()
        except Exception:
            raise RecoveryServiceError(
                "Current project fingerprint evidence could not be read"
            ) from None
        if not isinstance(context, RecoveryProjectContext):
            raise RecoveryServiceError("Current project fingerprint evidence is ambiguous")
        return context

    @staticmethod
    def _observe_targets(
        observer: RecoveryTargetObserver | None,
        resolution: RecoveryResolution,
        snapshot: RecoverySnapshotEvidence,
    ) -> RecoveryLiveObservation:
        if observer is None:
            raise RecoveryServiceError("Recovery requires fresh live target evidence")
        try:
            observation = observer.observe_recovery_targets(
                resolution=resolution,
                snapshot=snapshot,
            )
        except Exception:
            raise RecoveryServiceError("Fresh live target evidence could not be observed") from None
        if not isinstance(observation, RecoveryLiveObservation):
            raise RecoveryServiceError("Fresh live target evidence is ambiguous")
        return observation

    def _verify_result(
        self,
        plan: RecoveryPlanFile,
        result: OperationSnapshot,
    ) -> None:
        self._require_snapshot_identity(result)
        expected_state = plan.candidate_state or plan.snapshot.state
        if result.state.state != expected_state:
            raise RecoveryServiceError(
                "Recovery backend did not return the reviewed ownership state"
            )
        if result.control.control != OperationControlState.clear(self.state.address):
            raise RecoveryServiceError("Recovery backend did not clear the blocked operation")
