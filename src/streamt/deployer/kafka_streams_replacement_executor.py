"""Inactive, journal-aware executor for one predicate-only runner replacement.

The caller owns the operation lock, initial intent and final state commit. This
driver never clears pending work, authorizes recovery, initializes offsets or
creates a state volume. An acknowledged journal snapshot survives every error.
"""

from __future__ import annotations

import re
import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from streamt.deployer.kafka_streams_evidence import KafkaStreamsActionEvidence
from streamt.deployer.kafka_streams_replacement import ReplacementDecision, decide_replacement
from streamt.deployer.kafka_streams_replacement_observer import KafkaStreamsReplacementObserver
from streamt.deployer.state_backend import (
    ControlObservation,
    DeploymentStateOperation,
    OperationAction,
    OperationProgress,
    OperationSnapshot,
    StateBackendConflictError,
    StateBackendError,
    StateBackendRecoveryRequiredError,
    StateBackendUnknownCommitError,
    StateObservation,
    operation_timestamp,
    state_checksum,
)


class KafkaStreamsReplacementExecutionError(ValueError):
    """A neutral failure; the caller retains its latest acknowledged snapshot."""


@dataclass
class ReplacementExecutionState:
    """Only successful record_progress acknowledgements advance this holder."""

    snapshot: OperationSnapshot

    def __post_init__(self) -> None:
        if type(self.snapshot) is not OperationSnapshot:
            raise KafkaStreamsReplacementExecutionError("An exact operation snapshot is required")


def _validate_snapshot(snapshot: OperationSnapshot, operation_id: str) -> OperationAction:
    if (
        type(snapshot) is not OperationSnapshot or type(snapshot.state) is not StateObservation
        or type(snapshot.control) is not ControlObservation
    ):
        raise KafkaStreamsReplacementExecutionError("An exact full operation snapshot is required")
    control = snapshot.control.control
    if control.status != "in_progress":
        raise StateBackendRecoveryRequiredError("Replacement requires separately authorized in_progress state")
    intent = control.intent
    if (
        control.control_version not in (4, 5) or intent is None or intent.kind != "apply"
        or type(operation_id) is not str or intent.operation_id != operation_id
        or len(intent.actions) != 1
    ):
        raise KafkaStreamsReplacementExecutionError("One exact same-operation replacement intent is required")
    action = intent.actions[0]
    if (
        type(action) is not OperationAction or action.index != 0 or action.action != "update"
        or type(action.kafka_streams_evidence) is not KafkaStreamsActionEvidence
    ):
        raise KafkaStreamsReplacementExecutionError("One typed runner update action is required")
    if (
        intent.prior_state_serial != snapshot.state.state_serial
        or intent.prior_state_checksum != state_checksum(snapshot.state.state)
    ):
        raise StateBackendConflictError("Replacement snapshot no longer matches the original protected state")
    intent.validate_kafka_streams_prior_state(snapshot.state.state)
    if any(record.store != snapshot.state.store for record in control.resume_history):
        raise StateBackendConflictError("Replacement resume history belongs to another state store")
    return action


def _same_snapshot(left: OperationSnapshot, right: OperationSnapshot) -> bool:
    return (
        left.state.store == right.state.store and left.address == right.address
        and left.state.revision == right.state.revision
        and state_checksum(left.state.state) == state_checksum(right.state.state)
        and left.control.revision == right.control.revision
        and left.control.control == right.control.control
    )


class KafkaStreamsReplacementExecutor:
    def __init__(
        self, observer: KafkaStreamsReplacementObserver, *, context_check: Callable[[], None] | None = None,
    ) -> None:
        if type(observer) is not KafkaStreamsReplacementObserver:
            raise KafkaStreamsReplacementExecutionError("An exact bound replacement observer is required")
        if context_check is not None and not callable(context_check):
            raise KafkaStreamsReplacementExecutionError("Replacement context check must be callable")
        self.observer = observer
        self.context_check = context_check

    def _check_current(
        self, operation: DeploymentStateOperation, state: ReplacementExecutionState, operation_id: str,
    ) -> OperationAction:
        action = _validate_snapshot(state.snapshot, operation_id)
        if self.context_check is not None:
            self.context_check()
        operation.check_lock()
        current = operation.observe()
        _validate_snapshot(current, operation_id)
        if not _same_snapshot(current, state.snapshot):
            raise StateBackendConflictError("Replacement operation snapshot changed before transition")
        return action

    @staticmethod
    def _record(
        operation: DeploymentStateOperation, state: ReplacementExecutionState,
        action: OperationAction, decision: ReplacementDecision, operation_id: str,
    ) -> None:
        before = state.snapshot
        progress = OperationProgress(
            operation_id, action.index, action.resource_id, action.action,
            "checkpoint" if decision.checkpoint is not None else
            "completed" if decision.step == "record_completed" else "started",
            True if decision.step == "record_completed" else None,
            operation_timestamp(), decision.checkpoint,
        )
        # Preserve backend lock/conflict/unknown-commit exceptions unchanged.
        operation.check_lock()
        acknowledged = operation.record_progress(before, progress)
        if type(acknowledged) is not OperationSnapshot:
            raise StateBackendUnknownCommitError("Replacement checkpoint acknowledgement is invalid", operation_id=operation_id)
        _validate_snapshot(acknowledged, operation_id)
        if (
            acknowledged.state != before.state
            or acknowledged.control.control.control_version != before.control.control.control_version
            or acknowledged.control.control.resume_history != before.control.control.resume_history
            or acknowledged.control.control.intent != before.control.control.intent
            or acknowledged.control.control.progress != (*before.control.control.progress, progress)
        ):
            raise StateBackendUnknownCommitError("Replacement checkpoint acknowledgement changed its boundary", operation_id=operation_id)
        state.snapshot = acknowledged

    def run(
        self, operation: DeploymentStateOperation, state: ReplacementExecutionState, *,
        operation_id: str, mode: Literal["execute", "resume"],
        timeout_seconds: float = 60, poll_seconds: float = 0.25,
    ) -> OperationSnapshot:
        """Reach verified completion, leaving the intent pending for the caller.

        ``resume`` permits only an already authorized ``in_progress`` operation;
        it cannot rewrite ``recovery_required``. The deadline bounds polling and
        starting additional work, not an in-flight provider's own timeout.
        """
        if (
            type(state) is not ReplacementExecutionState or type(mode) is not str
            or mode not in {"execute", "resume"}
            or type(timeout_seconds) not in {int, float} or not 0 < timeout_seconds <= 600
            or type(poll_seconds) not in {int, float} or not 0 < poll_seconds <= min(10, timeout_seconds)
        ):
            raise KafkaStreamsReplacementExecutionError("Replacement execution mode or polling bounds are invalid")
        _validate_snapshot(state.snapshot, operation_id)
        deadline = time.monotonic() + timeout_seconds
        inputs: tuple[Path, Path] | None = None
        term_sent = False
        started_id: str | None = None
        created_id: str | None = None
        # A previous invocation may have started the exact candidate and lost
        # its response. Explicit resume can wait for its status using reads
        # only; the durable frontier still decides every subsequent write.
        waiting_after_write = mode == "resume" and any(
            item.status == "started" for item in state.snapshot.control.control.progress
        )

        def require_time() -> None:
            if time.monotonic() >= deadline:
                raise KafkaStreamsReplacementExecutionError("Replacement observation deadline exceeded")

        def pause() -> None:
            require_time()
            time.sleep(min(poll_seconds, max(0, deadline - time.monotonic())))

        while True:
            require_time()
            action = self._check_current(operation, state, operation_id)
            evidence = action.kafka_streams_evidence
            assert evidence is not None
            record = state.snapshot.state.state.resources[action.resource_id]
            try:
                observed = self.observer.observe(evidence, record)
            except StateBackendError:
                raise
            except ValueError:
                # Retrying a read never retries a write or invents an absent /
                # healthy runtime. In particular, missing startup status is not
                # permission to record readiness, clear state or start again.
                if not waiting_after_write:
                    raise KafkaStreamsReplacementExecutionError("Cannot observe replacement runtime") from None
                pause()
                continue
            except Exception:
                raise KafkaStreamsReplacementExecutionError("Cannot observe replacement runtime") from None
            require_time()
            if created_id is not None and (
                observed.candidate_container is None or observed.candidate_container.container_id != created_id
            ):
                raise KafkaStreamsReplacementExecutionError("Acknowledged candidate identity changed")
            decision = decide_replacement(state.snapshot.control.control, 0, observed, mode=mode)
            if decision.step == "blocked":
                raise KafkaStreamsReplacementExecutionError("Replacement transition is not proved")
            if decision.requires_resume_authority:
                raise StateBackendRecoveryRequiredError("Replacement requires separate durable resume authority")
            if decision.step == "candidate_verified":
                self._check_current(operation, state, operation_id)
                return state.snapshot
            if decision.step.startswith("record_"):
                self._check_current(operation, state, operation_id)
                require_time()
                self._record(operation, state, action, decision, operation_id)
                continue
            if decision.step in {"wait_old_closed", "wait_candidate_ready"}:
                pause()
                continue
            if decision.step == "term_old" and term_sent:
                pause()
                continue
            if decision.step == "start_candidate" and started_id is not None:
                if observed.candidate_container is not None and observed.candidate_container.process_state == "created":
                    pause()
                    continue
                raise KafkaStreamsReplacementExecutionError("Started candidate did not remain running")
            runtime = self.observer.deployer
            if decision.step == "create_candidate" and inputs is None:
                self._check_current(operation, state, operation_id)
                require_time()
                operation.check_lock()
                try:
                    inputs = runtime._private_inputs(evidence.desired_artifact.artifact)
                except StateBackendError:
                    raise
                except Exception:
                    raise KafkaStreamsReplacementExecutionError("Cannot prepare private replacement inputs") from None
                # Re-observe every provider identity/offset before the create.
                continue
            self._check_current(operation, state, operation_id)
            require_time()
            operation.check_lock()
            try:
                if decision.step == "create_candidate":
                    assert inputs is not None
                    assert decision.generation is not None
                    created_id = runtime.docker.create(
                        application_id=evidence.application_id, image_id=evidence.image_id,
                        network=evidence.network_id, plan_file=inputs[0], properties_file=inputs[1],
                        state_volume=evidence.volume.name, artifact_hash=evidence.desired_artifact.checksum,
                        plan_hash=evidence.desired_artifact.plan_hash, backend=evidence.backend_identity,
                        input_topic_id=evidence.progress.input_topic_id, output_topic_id=evidence.progress.output_topic_id,
                        cluster_id=evidence.progress.cluster_id, generation=decision.generation,
                        expected_volume=evidence.volume,
                    )
                    if type(created_id) is not str or re.fullmatch(r"[0-9a-f]{64}", created_id) is None:
                        raise KafkaStreamsReplacementExecutionError("Created candidate acknowledgement is invalid")
                elif decision.step in {"term_old", "remove_old", "start_candidate"}:
                    assert decision.container_id is not None
                    command = {"term_old": "term", "remove_old": "remove", "start_candidate": "start"}[decision.step]
                    runtime.docker.owned_command(command, evidence.application_id, evidence.backend_identity, expected_id=decision.container_id)
                    if decision.step == "term_old":
                        term_sent = True
                    elif decision.step == "start_candidate":
                        started_id = decision.container_id
                else:
                    raise KafkaStreamsReplacementExecutionError("Unsupported replacement transition")
            except StateBackendError:
                raise
            except Exception:
                # A missing acknowledgement can follow a successful write.
                # Never retry, infer its checkpoint, terminate or remove here.
                raise KafkaStreamsReplacementExecutionError("Replacement runtime write outcome is uncertain") from None
            waiting_after_write = True
