"""Reviewed-plan replacement coordination and read-only frontier reporting.

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
        self, observer: KafkaStreamsReplacementObserver | None, project_reader: Callable[[], StreamtProject],
        *, observer_factory: Callable[[], KafkaStreamsReplacementObserver] | None = None,
    ) -> None:
        valid_observer = type(observer) is KafkaStreamsReplacementObserver and observer_factory is None
        valid_factory = observer is None and callable(observer_factory)
        if not callable(project_reader) or not (valid_observer or valid_factory):
            raise KafkaStreamsReplacementExecutionError("Replacement requires a bound observer and full-project reader")
        self._observer = observer
        self._observer_factory = observer_factory
        self.project_reader = project_reader

    @property
    def observer(self) -> KafkaStreamsReplacementObserver:
        """Construct only the exact runner observer, after static/storage checks."""
        if self._observer is None:
            assert self._observer_factory is not None
            observer = self._observer_factory()
            if type(observer) is not KafkaStreamsReplacementObserver:
                raise KafkaStreamsReplacementExecutionError("Replacement observer binding is invalid")
            self._observer = observer
        return self._observer

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
        KafkaStreamsReplacementCoordinator._bound_control(plan, snapshot.control.control, operation_id)

    @staticmethod
    def _bound_control(plan: ReviewedPlanFile, control: OperationControlState, operation_id: str) -> None:
        intent = control.intent
        if (
            intent is None or intent.operation_id != operation_id or intent.kind != "apply"
            or intent.reviewed_plan_checksum != plan.checksum or intent.actions != plan.actions
        ):
            raise PlanFileError("Pending replacement does not match the original operation and reviewed action tuple")

    def inspect(
        self, operation: DeploymentStateOperation, state: ReplacementExecutionState, *,
        plan: ReviewedPlanFile, operation_id: str,
    ) -> dict[str, object]:
        """Report a verified frontier without writing authority or runtime state.

        ``resumable`` and ``next_step`` are observations, never authorization.
        A later resume must reacquire the lock and repeat every check.
        """
        self._inputs(state, operation_id, 60)
        self._reviewed(plan)
        self._current(operation, state.snapshot)
        snapshot = state.snapshot
        control = snapshot.control.control
        receipt = None
        completed = control.status == "clear"
        if completed:
            receipt = operation.completed_runner_receipt(snapshot, operation_id)
            if receipt is None:
                raise StateBackendRecoveryRequiredError("No exact completed runner receipt exists for this operation")
            control = receipt.control
            prior = receipt.verify_result_state(snapshot.state.state)
        else:
            self._bound(plan, snapshot, operation_id)
            terminal = next((item for item in control.progress if item.status == "completed"), None)
            if terminal is not None and terminal.succeeded is True:
                operation.validate_completed_runner_snapshot(snapshot)
                prior, _result = completed_runner_state_pair(snapshot, allow_written_result=snapshot.state.store.backend == "local")
            else:
                operation.pending_resume_authorization(snapshot)
                prior = snapshot.state.state
                assert control.intent is not None
                control.intent.validate_kafka_streams_prior_state(prior)
        self._bound_control(plan, control, operation_id)
        self._context(plan, snapshot, prior=prior)
        self._current(operation, snapshot)
        action = plan.actions[0]
        evidence = action.kafka_streams_evidence
        assert evidence is not None
        observed = self.observer.observe(evidence, prior.resources[action.resource_id])
        decision = decide_replacement(control, 0, observed, mode="resume")
        self._context(plan, snapshot, prior=prior)
        self._current(operation, snapshot)
        if completed:
            if operation.completed_runner_receipt(snapshot, operation_id) != receipt:
                raise StateBackendConflictError("Completed runner receipt changed during observation")
            if decision.step != "candidate_verified":
                raise KafkaStreamsReplacementExecutionError("Completed replacement candidate is not verified ready")
        elif any(item.status == "completed" and item.succeeded is True for item in control.progress):
            operation.validate_completed_runner_snapshot(snapshot)
        else:
            operation.pending_resume_authorization(snapshot)
        self._current(operation, snapshot)
        status = "completed" if completed else "blocked" if decision.step == "blocked" else "ready_to_finalize" if decision.step == "candidate_verified" else "pending"
        boundary: str = control.progress[-1].status if control.progress else "intent"
        if control.progress and control.progress[-1].kafka_streams_checkpoint is not None:
            boundary = control.progress[-1].kafka_streams_checkpoint.phase
        return {
            "operation_id": operation_id, "plan_checksum": plan.checksum,
            "state_serial": snapshot.state.state.serial,
            "committed": True if completed else False if snapshot.state.state == prior else None,
            "status": status, "control_status": snapshot.control.control.status,
            "lifecycle_phase": boundary, "resumable": not completed and decision.step != "blocked",
            "next_step": decision.step, "reason": decision.reason,
            "next_action": "none" if completed else "investigate" if decision.step == "blocked" else "resume_same_operation",
            "read_only": True,
        }

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
        on_started: Callable[[], None] | None = None,
    ) -> OperationSnapshot:
        """Begin a reviewed operation, execute its exact tuple and finalize it."""
        self._inputs(state, operation_id, timeout_seconds)
        if on_started is not None and not callable(on_started):
            raise KafkaStreamsReplacementExecutionError("Replacement start callback must be callable")
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
            if on_started is not None:
                on_started()
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
        if state.snapshot.control.control.status == "clear":
            report = self.inspect(operation, state, plan=plan, operation_id=operation_id)
            if report["status"] != "completed":
                raise StateBackendRecoveryRequiredError("Replacement completion is not verified")
            return state.snapshot
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
        operation.validate_completed_runner_snapshot(state.snapshot)
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
        operation.validate_completed_runner_snapshot(state.snapshot)
        finished = operation.finalize_completed_runner(state.snapshot)
        if (
            type(finished) is not OperationSnapshot or finished.address != state.snapshot.address
            or finished.state.store != state.snapshot.state.store or finished.state.state != result
            or finished.control.control.status != "clear"
        ):
            raise StateBackendUnknownCommitError("Replacement finalization acknowledgement is invalid", operation_id=operation_id)
        state.snapshot = finished
        return finished
