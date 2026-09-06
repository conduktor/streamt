"""Internal reviewed-plan coordinator; public replacement commands remain blocked.

The caller acquires/releases storage authority and supplies a fresh full-project
reader. This coordinator binds the original reviewed tuple, keeps only verified
acknowledgements, and rechecks project context before every driver transition.
It never creates a fresh plan from a partially replaced runtime.
"""

from __future__ import annotations

import hmac
import uuid
from collections.abc import Callable
from dataclasses import replace
from typing import Literal

from streamt.compiler.compiler import Compiler
from streamt.compiler.manifest import parse_compiled_kafka_streams_job_artifact
from streamt.core.models import StreamtProject
from streamt.core.validator import ProjectValidator
from streamt.deployer.kafka_streams_replacement import decide_replacement
from streamt.deployer.kafka_streams_replacement_executor import (
    KafkaStreamsReplacementExecutionError,
    KafkaStreamsReplacementExecutor,
    ReplacementExecutionState,
    _same_snapshot,
)
from streamt.deployer.kafka_streams_replacement_observer import KafkaStreamsReplacementObserver
from streamt.deployer.plan_file import (
    KAFKA_STREAMS_PLAN_FILE_VERSION,
    PlanFileError,
    ReviewedPlanFile,
    _reviewed_checksum,
)
from streamt.deployer.planner import DeploymentPlan
from streamt.deployer.state import LocalState
from streamt.deployer.state_backend import (
    DeploymentStateOperation,
    OperationAction,
    OperationControlState,
    OperationIntent,
    OperationResumeRecord,
    OperationSnapshot,
    RecoveryRecord,
    StateBackendConflictError,
    StateBackendRecoveryRequiredError,
    StateBackendUnknownCommitError,
    completed_runner_state_pair,
    operation_timestamp,
    state_checksum,
)


class KafkaStreamsReplacementCoordinator:
    """Coordinate one reviewed replacement under a caller-owned operation lock."""

    def __init__(
        self, observer: KafkaStreamsReplacementObserver, project_reader: Callable[[], StreamtProject],
    ) -> None:
        if type(observer) is not KafkaStreamsReplacementObserver or not callable(project_reader):
            raise KafkaStreamsReplacementExecutionError("Replacement requires a bound observer and full-project reader")
        self.observer = observer
        self.project_reader = project_reader

    @staticmethod
    def _inputs(state: ReplacementExecutionState, operation_id: str, timeout_seconds: float) -> None:
        if (
            type(state) is not ReplacementExecutionState or type(state.snapshot) is not OperationSnapshot
            or type(timeout_seconds) not in (int, float) or not 0 < timeout_seconds <= 600
        ):
            raise KafkaStreamsReplacementExecutionError("Replacement requires an exact snapshot holder and bounded timeout")
        try:
            if type(operation_id) is not str or str(uuid.UUID(operation_id)) != operation_id or uuid.UUID(operation_id).int == 0:
                raise ValueError("invalid identity")
        except (ValueError, AttributeError):
            raise KafkaStreamsReplacementExecutionError("Replacement requires a canonical original operation UUID") from None

    @staticmethod
    def _reviewed(plan: ReviewedPlanFile) -> None:
        if type(plan) is not ReviewedPlanFile or plan.format_version != KAFKA_STREAMS_PLAN_FILE_VERSION:
            raise PlanFileError("Replacement requires an original reviewed format-6 plan")
        # Revalidate nested mutable payloads as well as the checksummed raw v4 action.
        validated = replace(plan)
        expected = _reviewed_checksum(
            validated._unsigned_dict(), format_version=validated.format_version, actions=validated.actions,
        )
        if type(plan.checksum) is not str or not hmac.compare_digest(plan.checksum, expected):
            raise PlanFileError("Reviewed replacement checksum no longer matches its content")

    def _context(
        self, plan: ReviewedPlanFile, snapshot: OperationSnapshot, *, prior: LocalState | None = None,
    ) -> None:
        self._reviewed(plan)
        try:
            project = self.project_reader()
            if type(project) is not StreamtProject or not ProjectValidator(project).validate().is_valid:
                raise ValueError("invalid project")
            manifest = Compiler(project).compile(dry_run=True)
        except Exception:
            raise KafkaStreamsReplacementExecutionError("Current replacement project cannot be validated and compiled") from None
        plan.verify_context(
            manifest, project=project.project.name, environment=project.environment_name,
            runtime=project.runtime,
            state_observation=replace(snapshot.state, state=prior) if prior is not None else snapshot.state,
        )
        evidence = plan.actions[0].kafka_streams_evidence
        assert evidence is not None  # Reviewed format 6 is strictly validated above.
        artifacts = tuple(parse_compiled_kafka_streams_job_artifact(raw) for raw in manifest.artifacts.get("kafka_streams_jobs", []))
        matching = tuple(artifact for artifact in artifacts if artifact.name == evidence.desired_artifact.artifact.name)
        if len(matching) != 1 or matching[0].to_dict() != evidence.desired_artifact.to_dict():
            raise PlanFileError("Reviewed replacement action does not match the full current compiled project")

    @staticmethod
    def _current(operation: DeploymentStateOperation, snapshot: OperationSnapshot) -> None:
        operation.check_lock()
        current = operation.observe()
        if type(current) is not OperationSnapshot or not _same_snapshot(current, snapshot):
            raise StateBackendConflictError("Replacement state or control changed after observation")
        operation.check_lock()

    @staticmethod
    def _bound(plan: ReviewedPlanFile, snapshot: OperationSnapshot, operation_id: str) -> None:
        intent = snapshot.control.control.intent
        if (
            intent is None or intent.operation_id != operation_id or intent.kind != "apply"
            or intent.reviewed_plan_checksum != plan.checksum or intent.actions != plan.actions
        ):
            raise PlanFileError("Pending replacement does not match the original operation and reviewed action tuple")

    def _execution_context(
        self, plan: ReviewedPlanFile, snapshot: OperationSnapshot, operation_id: str,
    ) -> None:
        self._bound(plan, snapshot, operation_id)
        self._context(plan, snapshot)

    @staticmethod
    def _interrupt_incomplete(operation: DeploymentStateOperation, state: ReplacementExecutionState) -> None:
        control = state.snapshot.control.control
        if control.status != "in_progress" or control.intent is None or any(
            progress.status == "completed" for progress in control.progress
        ):
            return
        # Never overwrite an unacknowledged journal write with an invented
        # frontier, nor change a terminal control after a finalizer attempt.
        try:
            KafkaStreamsReplacementCoordinator._current(operation, state.snapshot)
            record = RecoveryRecord(control.intent.operation_id, "runner_operation_interrupted", operation_timestamp(), None)
            blocked = operation.mark_recovery_required(state.snapshot, record)
            expected = replace(control, status="recovery_required", recovery=record)
            if type(blocked) is OperationSnapshot and blocked.state == state.snapshot.state and blocked.control.control == expected:
                state.snapshot = blocked
        except BaseException:
            # The durable marker remains blocking; preserve the original error
            # and the caller's last acknowledged snapshot if authority is lost.
            pass

    def execute(
        self, operation: DeploymentStateOperation, state: ReplacementExecutionState, *,
        plan: ReviewedPlanFile, current_plan: DeploymentPlan, current_actions: tuple[OperationAction, ...],
        operation_id: str, actor: str, timeout_seconds: float = 60,
    ) -> OperationSnapshot:
        """Begin a reviewed operation, execute its exact tuple and finalize it."""
        self._inputs(state, operation_id, timeout_seconds)
        self._context(plan, state.snapshot)
        self._current(operation, state.snapshot)
        operation.ensure_ready(state.snapshot)
        actions = plan.bind_current_actions(current_plan, actions=current_actions, state_observation=state.snapshot.state)
        intent = OperationIntent(
            operation_id, "apply", operation_timestamp(), actor,
            state.snapshot.state.state_serial, state_checksum(state.snapshot.state.state), plan.checksum, actions,
        )
        self._context(plan, state.snapshot)
        self._current(operation, state.snapshot)
        try:
            active = operation.begin_operation(state.snapshot, intent)
            expected = OperationControlState(state.snapshot.address, "in_progress", intent)
            if type(active) is not OperationSnapshot or active.state != state.snapshot.state or active.control.control != expected:
                raise StateBackendUnknownCommitError("Replacement intent acknowledgement is invalid", operation_id=operation_id)
            state.snapshot = active
            return self._drive(operation, state, plan, operation_id, "execute", timeout_seconds)
        except BaseException:
            self._interrupt_incomplete(operation, state)
            raise

    def resume(
        self, operation: DeploymentStateOperation, state: ReplacementExecutionState, *,
        plan: ReviewedPlanFile, operation_id: str, actor: str, timeout_seconds: float = 60,
    ) -> OperationSnapshot:
        """Resume the original intent or finalize its already-completed result."""
        self._inputs(state, operation_id, timeout_seconds)
        self._reviewed(plan)
        self._bound(plan, state.snapshot, operation_id)
        control = state.snapshot.control.control
        if any(progress.status == "completed" for progress in control.progress):
            return self._finish(operation, state, plan, operation_id)
        self._execution_context(plan, state.snapshot, operation_id)
        self._current(operation, state.snapshot)
        try:
            if control.status == "in_progress":
                record = RecoveryRecord(operation_id, "runner_interruption_acknowledged", operation_timestamp(), None)
                blocked = operation.mark_recovery_required(state.snapshot, record)
                expected = replace(control, status="recovery_required", recovery=record)
                if type(blocked) is not OperationSnapshot or blocked.state != state.snapshot.state or blocked.control.control != expected:
                    raise StateBackendUnknownCommitError("Replacement interruption acknowledgement is invalid", operation_id=operation_id)
                state.snapshot = blocked
            elif control.status != "recovery_required":
                raise StateBackendRecoveryRequiredError("Replacement has no pending operation to resume")
            pending = operation.pending_resume_authorization(state.snapshot)
            authorization = pending or OperationResumeRecord.create(
                state.snapshot, resume_id=str(uuid.uuid4()), actor=actor, resumed_at=operation_timestamp(),
            )
            self._execution_context(plan, state.snapshot, operation_id)
            self._current(operation, state.snapshot)
            resumed = operation.resume_operation(state.snapshot, authorization)
            expected = replace(
                state.snapshot.control.control, status="in_progress", recovery=None, control_version=5,
                resume_history=(*state.snapshot.control.control.resume_history, authorization),
            )
            if type(resumed) is not OperationSnapshot or resumed.state != state.snapshot.state or resumed.control.control != expected:
                raise StateBackendUnknownCommitError("Replacement resume acknowledgement is invalid", operation_id=operation_id)
            state.snapshot = resumed
            return self._drive(operation, state, plan, operation_id, "resume", timeout_seconds)
        except BaseException:
            self._interrupt_incomplete(operation, state)
            raise

    def _drive(
        self, operation: DeploymentStateOperation, state: ReplacementExecutionState,
        plan: ReviewedPlanFile, operation_id: str, mode: Literal["execute", "resume"], timeout_seconds: float,
    ) -> OperationSnapshot:
        driver = KafkaStreamsReplacementExecutor(
            self.observer, context_check=lambda: self._execution_context(plan, state.snapshot, operation_id),
        )
        driver.run(operation, state, operation_id=operation_id, mode=mode, timeout_seconds=timeout_seconds)
        return self._finish(operation, state, plan, operation_id)

    def _finish(
        self, operation: DeploymentStateOperation, state: ReplacementExecutionState,
        plan: ReviewedPlanFile, operation_id: str,
    ) -> OperationSnapshot:
        self._reviewed(plan)
        self._bound(plan, state.snapshot, operation_id)
        prior, result = completed_runner_state_pair(
            state.snapshot, allow_written_result=state.snapshot.state.store.backend == "local",
        )
        self._context(plan, state.snapshot, prior=prior)
        self._current(operation, state.snapshot)
        action = plan.actions[0]
        evidence = action.kafka_streams_evidence
        assert evidence is not None
        observed = self.observer.observe(evidence, prior.resources[action.resource_id])
        decision = decide_replacement(state.snapshot.control.control, 0, observed, mode="recover")
        if decision.step != "candidate_verified":
            raise KafkaStreamsReplacementExecutionError("Completed replacement candidate is not verified ready")
        self._context(plan, state.snapshot, prior=prior)
        self._current(operation, state.snapshot)
        finished = operation.finalize_completed_runner(state.snapshot)
        if (
            type(finished) is not OperationSnapshot or finished.address != state.snapshot.address
            or finished.state.store != state.snapshot.state.store or finished.state.state != result
            or finished.control.control.status != "clear"
        ):
            raise StateBackendUnknownCommitError("Replacement finalization acknowledgement is invalid", operation_id=operation_id)
        state.snapshot = finished
        return finished
