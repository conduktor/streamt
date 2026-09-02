"""Crash-safety and strictness tests for explicit local-state recovery."""

from __future__ import annotations

import json
import os
import stat
import uuid
from pathlib import Path
from typing import cast

import pytest

from streamt.deployer.recovery import (
    RecoveryResolution,
    RecoveryResolutionRecord,
    RecoverySnapshotEvidence,
)
from streamt.deployer.state import (
    LocalState,
    ManagedResourceRecord,
    StateFormatError,
    StateIdentityError,
    artifact_checksum,
    resource_id,
)
from streamt.deployer.state_backend import (
    LOCAL_STATE_NAMESPACE,
    DeploymentStateOperation,
    LocalDeploymentStateBackend,
    OperationAction,
    OperationControlState,
    OperationIntent,
    OperationProgress,
    OperationSnapshot,
    RecoveryRecord,
    StateAddress,
    StateBackendConflictError,
    StateBackendInvalidStateError,
    StateBackendRecoveryRequiredError,
    StateBackendUnknownCommitError,
    StateStoreIdentity,
    local_recovery_history_path,
    state_checksum,
)

_STARTED_AT = "2026-09-02T12:00:00.000000Z"
_FAILED_AT = "2026-09-02T12:01:00.000000Z"
_RESOLVED_AT = "2026-09-02T12:02:00.000000Z"
_RETRIED_AT = "2026-09-02T12:03:00.000000Z"
_PLAN_CHECKSUM = f"sha256:{'a' * 64}"


def _address() -> StateAddress:
    return StateAddress(
        namespace=LOCAL_STATE_NAMESPACE,
        project="payments",
        environment="prod",
    )


def _action() -> OperationAction:
    return OperationAction(
        index=0,
        resource_id=resource_id(
            "payments",
            "prod",
            "topic",
            "payments_clean",
        ),
        action="create",
    )


def _replacement(prior: LocalState) -> LocalState:
    identity = _action().resource_id
    artifact = {"name": "payments.clean.v1", "partitions": 3}
    return LocalState(
        project=prior.project,
        environment=prior.environment,
        serial=prior.serial + 1,
        resources={
            identity: ManagedResourceRecord(
                physical_name="payments.clean.v1",
                ownership="managed",
                artifact_checksum=artifact_checksum(artifact),
                backend="direct-kafka",
            )
        },
    )


def _begin(
    operation: DeploymentStateOperation,
    *,
    status: str = "in_progress",
    started_action: bool = False,
) -> OperationSnapshot:
    snapshot = operation.observe()
    intent = OperationIntent(
        operation_id=str(uuid.uuid4()),
        kind="apply",
        started_at=_STARTED_AT,
        actor="test",
        prior_state_serial=snapshot.state.state.serial,
        prior_state_checksum=state_checksum(snapshot.state.state),
        reviewed_plan_checksum=None,
        actions=(_action(),),
    )
    active = operation.begin_operation(snapshot, intent)
    if started_action:
        active = operation.record_progress(
            active,
            OperationProgress(
                operation_id=intent.operation_id,
                action_index=0,
                resource_id=_action().resource_id,
                action="create",
                status="started",
                succeeded=None,
                recorded_at=_STARTED_AT,
            ),
        )
    if status == "recovery_required":
        active = operation.mark_recovery_required(
            active,
            RecoveryRecord(
                operation_id=intent.operation_id,
                failure_code="runtime_interrupted",
                failed_at=_FAILED_AT,
                last_completed_action_index=None,
            ),
        )
    return active


def _resolution(
    evidence: RecoverySnapshotEvidence,
    outcome: RecoveryResolution,
    *,
    replacement: LocalState | None = None,
    recovery_operation_id: str | None = None,
    evidence_checksum: str = _PLAN_CHECKSUM,
) -> RecoveryResolutionRecord:
    result = replacement if replacement is not None else evidence.state
    return RecoveryResolutionRecord(
        address=evidence.address,
        recovery_operation_id=recovery_operation_id or str(uuid.uuid4()),
        blocked_operation_id=evidence.blocked_operation_id,
        resolution=outcome,
        resolved_at=_RESOLVED_AT,
        evidence_checksum=evidence_checksum,
        prior_state_serial=evidence.state.serial,
        prior_state_checksum=evidence.state_checksum,
        result_state_serial=result.serial,
        result_state_checksum=state_checksum(result),
        state_changed=replacement is not None,
    )


def _history_payload(tmp_path: Path) -> dict[str, object]:
    path = local_recovery_history_path(tmp_path, environment="prod")
    return cast(dict[str, object], json.loads(path.read_text()))


@pytest.mark.parametrize(
    ("outcome", "status", "started_action"),
    [
        ("abandoned_before_mutation", "in_progress", False),
        ("rolled_back", "recovery_required", True),
        ("observed", "recovery_required", True),
    ],
)
def test_unchanged_recovery_outcomes_preserve_state_and_append_audit(
    tmp_path: Path,
    outcome: RecoveryResolution,
    status: str,
    started_action: bool,
) -> None:
    backend = LocalDeploymentStateBackend(tmp_path)
    with backend.operation(_address()) as operation:
        active = _begin(operation, status=status, started_action=started_action)
        evidence = RecoverySnapshotEvidence.from_operation_snapshot(active)
        record = _resolution(evidence, outcome)

        recovered = operation.finalize_recovery(active, evidence, record, None)

    assert recovered.state.state == evidence.state
    assert recovered.control.control == OperationControlState.clear(_address())
    history = _history_payload(tmp_path)
    events = cast(list[dict[str, object]], history["events"])
    assert [event["kind"] for event in events] == [
        "recovery_intent",
        "recovery_resolution",
    ]
    assert events[0]["record"] == events[1]["record"] == record.to_dict()
    assert events[0]["previous_checksum"] is None
    assert events[1]["previous_checksum"] == events[0]["checksum"]
    history_path = local_recovery_history_path(tmp_path, environment="prod")
    assert stat.S_IMODE(history_path.stat().st_mode) == 0o600


def test_observed_recovery_changes_ownership_once_and_increments_serial_once(
    tmp_path: Path,
) -> None:
    backend = LocalDeploymentStateBackend(tmp_path)
    with backend.operation(_address()) as operation:
        active = _begin(operation, status="recovery_required", started_action=True)
        evidence = RecoverySnapshotEvidence.from_operation_snapshot(active)
        replacement = _replacement(evidence.state)
        record = _resolution(evidence, "observed", replacement=replacement)

        recovered = operation.finalize_recovery(
            active,
            evidence,
            record,
            replacement,
        )

    assert recovered.state.state == replacement
    assert recovered.state.state.serial == evidence.state.serial + 1
    assert backend.read_control(_address()).control.status == "clear"


def test_abandoned_before_mutation_is_permanently_forbidden_after_action_start(
    tmp_path: Path,
) -> None:
    backend = LocalDeploymentStateBackend(tmp_path)
    with backend.operation(_address()) as operation:
        active = _begin(operation, started_action=True)
        evidence = RecoverySnapshotEvidence.from_operation_snapshot(active)
        record = _resolution(evidence, "abandoned_before_mutation")

        with pytest.raises(
            StateBackendRecoveryRequiredError,
            match="forbidden after an action started",
        ):
            operation.finalize_recovery(active, evidence, record, None)

    assert not local_recovery_history_path(tmp_path, environment="prod").exists()
    assert backend.read_control(_address()).control.status == "in_progress"


def test_clear_control_is_not_recoverable(tmp_path: Path) -> None:
    backend = LocalDeploymentStateBackend(tmp_path)
    with backend.operation(_address()) as operation:
        active = _begin(operation)
        evidence = RecoverySnapshotEvidence.from_operation_snapshot(active)
        record = _resolution(evidence, "rolled_back")
        cleared = operation.clear_before_mutation(active)
        assert cleared.control.control.status == "clear"

        with pytest.raises(
            StateBackendRecoveryRequiredError,
            match="clear deployment state control is not recoverable",
        ):
            operation.finalize_recovery(cleared, evidence, record, None)


def test_recovery_rejects_stale_control_evidence_and_wrong_blocked_operation(
    tmp_path: Path,
) -> None:
    backend = LocalDeploymentStateBackend(tmp_path)
    with backend.operation(_address()) as operation:
        active = _begin(operation)
        stale_evidence = RecoverySnapshotEvidence.from_operation_snapshot(active)
        intent = cast(OperationIntent, active.control.control.intent)
        progressed = operation.record_progress(
            active,
            OperationProgress(
                operation_id=intent.operation_id,
                action_index=0,
                resource_id=_action().resource_id,
                action="create",
                status="started",
                succeeded=None,
                recorded_at=_STARTED_AT,
            ),
        )
        stale_record = _resolution(stale_evidence, "rolled_back")
        with pytest.raises(StateBackendConflictError, match="control changed"):
            operation.finalize_recovery(
                progressed,
                stale_evidence,
                stale_record,
                None,
            )

        evidence = RecoverySnapshotEvidence.from_operation_snapshot(progressed)
        wrong_record = RecoveryResolutionRecord(
            **{
                **_resolution(evidence, "rolled_back").__dict__,
                "blocked_operation_id": str(uuid.uuid4()),
            }
        )
        with pytest.raises(StateIdentityError, match="another blocked operation"):
            operation.finalize_recovery(progressed, evidence, wrong_record, None)


@pytest.mark.parametrize("bad_case", ["outcome", "replacement", "same_ownership"])
def test_recovery_rejects_outcome_and_state_inconsistency(
    tmp_path: Path,
    bad_case: str,
) -> None:
    backend = LocalDeploymentStateBackend(tmp_path)
    with backend.operation(_address()) as operation:
        active = _begin(operation)
        evidence = RecoverySnapshotEvidence.from_operation_snapshot(active)
        replacement = _replacement(evidence.state)
        if bad_case == "outcome":
            record = _resolution(evidence, "rolled_back")
            supplied = replacement
        elif bad_case == "replacement":
            record = _resolution(evidence, "observed", replacement=replacement)
            supplied = None
        else:
            supplied = LocalState(
                project=evidence.state.project,
                environment=evidence.state.environment,
                serial=evidence.state.serial + 1,
                resources=evidence.state.resources,
            )
            record = _resolution(evidence, "observed", replacement=supplied)

        with pytest.raises(StateFormatError):
            operation.finalize_recovery(active, evidence, record, supplied)


def test_recovery_rejects_evidence_from_another_store(tmp_path: Path) -> None:
    backend = LocalDeploymentStateBackend(tmp_path)
    with backend.operation(_address()) as operation:
        active = _begin(operation)
        evidence = RecoverySnapshotEvidence.from_operation_snapshot(active)
        wrong_evidence = RecoverySnapshotEvidence(
            store=StateStoreIdentity(
                backend="local",
                store_id=str(uuid.uuid4()),
            ),
            address=evidence.address,
            state=evidence.state,
            state_checksum=evidence.state_checksum,
            control=evidence.control,
            control_checksum=evidence.control_checksum,
        )
        record = _resolution(wrong_evidence, "rolled_back")

        with pytest.raises(StateIdentityError, match="another state store"):
            operation.finalize_recovery(active, wrong_evidence, record, None)


def test_retry_resumes_after_intent_audit_without_duplicate_event(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    backend = LocalDeploymentStateBackend(tmp_path)
    original = backend._write_recovery_history
    failed = False

    def fail_after_intent(
        path: Path,
        history: object,
        *,
        operation_id: str,
    ) -> None:
        nonlocal failed
        original(path, history, operation_id=operation_id)  # type: ignore[arg-type]
        if not failed and len(history.events) == 1:  # type: ignore[attr-defined]
            failed = True
            raise StateBackendUnknownCommitError(
                "injected intent uncertainty",
                operation_id=operation_id,
            )

    monkeypatch.setattr(backend, "_write_recovery_history", fail_after_intent)
    with backend.operation(_address()) as operation:
        active = _begin(operation)
        evidence = RecoverySnapshotEvidence.from_operation_snapshot(active)
        record = _resolution(evidence, "rolled_back")
        with pytest.raises(StateBackendUnknownCommitError):
            operation.finalize_recovery(active, evidence, record, None)

    monkeypatch.setattr(backend, "_write_recovery_history", original)
    with backend.operation(_address()) as operation:
        recovered = operation.finalize_recovery(
            operation.observe(),
            evidence,
            record,
            None,
        )

    assert recovered.control.control.status == "clear"
    assert len(cast(list[object], _history_payload(tmp_path)["events"])) == 2


def test_retry_resumes_after_ownership_write_without_incrementing_twice(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    backend = LocalDeploymentStateBackend(tmp_path)
    original_append = backend._append_recovery_history_locked
    failed = False

    def fail_before_resolution(*args: object, **kwargs: object) -> object:
        nonlocal failed
        kind = args[2]
        if kind == "recovery_resolution" and not failed:
            failed = True
            record = args[3]
            raise StateBackendUnknownCommitError(
                "injected resolution uncertainty",
                operation_id=record.recovery_operation_id,  # type: ignore[attr-defined]
            )
        return original_append(*args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(
        backend,
        "_append_recovery_history_locked",
        fail_before_resolution,
    )
    with backend.operation(_address()) as operation:
        active = _begin(operation)
        evidence = RecoverySnapshotEvidence.from_operation_snapshot(active)
        replacement = _replacement(evidence.state)
        record = _resolution(evidence, "observed", replacement=replacement)
        with pytest.raises(StateBackendUnknownCommitError):
            operation.finalize_recovery(active, evidence, record, replacement)

    assert backend.read(_address()).state == replacement
    monkeypatch.setattr(backend, "_append_recovery_history_locked", original_append)
    retried_record = RecoveryResolutionRecord(**{**record.__dict__, "resolved_at": _RETRIED_AT})
    with backend.operation(_address()) as operation:
        recovered = operation.finalize_recovery(
            operation.observe(),
            evidence,
            retried_record,
            replacement,
        )

    assert recovered.state.state == replacement
    assert recovered.state.state.serial == 1
    events = cast(list[dict[str, object]], _history_payload(tmp_path)["events"])
    assert len(events) == 2
    assert events[0]["record"] == events[1]["record"] == record.to_dict()


def test_unknown_ownership_commit_is_sanitized_and_exact_retry_resumes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    backend = LocalDeploymentStateBackend(tmp_path)
    with backend.operation(_address()) as operation:
        active = _begin(operation)
        evidence = RecoverySnapshotEvidence.from_operation_snapshot(active)
        replacement = _replacement(evidence.state)
        record = _resolution(evidence, "observed", replacement=replacement)
        original_compare_and_swap = operation.compare_and_swap

        def fail_after_ownership_write(*args: object, **kwargs: object) -> object:
            original_compare_and_swap(*args, **kwargs)  # type: ignore[arg-type]
            raise OSError("password=do-not-expose")

        monkeypatch.setattr(
            operation,
            "compare_and_swap",
            fail_after_ownership_write,
        )
        with pytest.raises(StateBackendUnknownCommitError) as raised:
            operation.finalize_recovery(active, evidence, record, replacement)

    assert "do-not-expose" not in str(raised.value)
    assert backend.read(_address()).state == replacement
    with backend.operation(_address()) as operation:
        recovered = operation.finalize_recovery(
            operation.observe(),
            evidence,
            record,
            replacement,
        )
    assert recovered.state.state.serial == 1


def test_retry_rejects_state_drift_not_matching_declared_result(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    backend = LocalDeploymentStateBackend(tmp_path)
    original_append = backend._append_recovery_history_locked

    def fail_before_resolution(*args: object, **kwargs: object) -> object:
        if args[2] == "recovery_resolution":
            record = args[3]
            raise StateBackendUnknownCommitError(
                "injected resolution uncertainty",
                operation_id=record.recovery_operation_id,  # type: ignore[attr-defined]
            )
        return original_append(*args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(
        backend,
        "_append_recovery_history_locked",
        fail_before_resolution,
    )
    with backend.operation(_address()) as operation:
        active = _begin(operation)
        evidence = RecoverySnapshotEvidence.from_operation_snapshot(active)
        record = _resolution(evidence, "rolled_back")
        with pytest.raises(StateBackendUnknownCommitError):
            operation.finalize_recovery(active, evidence, record, None)

    drift = _replacement(evidence.state)
    drift.save(backend._path(_address()))
    monkeypatch.setattr(backend, "_append_recovery_history_locked", original_append)
    with (
        backend.operation(_address()) as operation,
        pytest.raises(
            StateBackendConflictError,
            match="state changed after recovery evidence was reviewed",
        ),
    ):
        operation.finalize_recovery(
            operation.observe(),
            evidence,
            record,
            None,
        )


def test_retry_resumes_after_resolution_audit_without_duplicate_event(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    backend = LocalDeploymentStateBackend(tmp_path)
    original = backend._write_recovery_history
    failed = False

    def fail_after_resolution(
        path: Path,
        history: object,
        *,
        operation_id: str,
    ) -> None:
        nonlocal failed
        original(path, history, operation_id=operation_id)  # type: ignore[arg-type]
        if not failed and len(history.events) == 2:  # type: ignore[attr-defined]
            failed = True
            raise StateBackendUnknownCommitError(
                "injected resolution uncertainty",
                operation_id=operation_id,
            )

    monkeypatch.setattr(backend, "_write_recovery_history", fail_after_resolution)
    with backend.operation(_address()) as operation:
        active = _begin(operation)
        evidence = RecoverySnapshotEvidence.from_operation_snapshot(active)
        record = _resolution(evidence, "rolled_back")
        with pytest.raises(StateBackendUnknownCommitError):
            operation.finalize_recovery(active, evidence, record, None)

    monkeypatch.setattr(backend, "_write_recovery_history", original)
    with backend.operation(_address()) as operation:
        recovered = operation.finalize_recovery(
            operation.observe(),
            evidence,
            record,
            None,
        )

    assert recovered.control.control.status == "clear"
    assert len(cast(list[object], _history_payload(tmp_path)["events"])) == 2


def test_verified_clear_uncertainty_returns_completed_recovery(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    backend = LocalDeploymentStateBackend(tmp_path)
    original = backend._write_control

    def fail_after_clear(
        path: Path,
        control: OperationControlState,
        *,
        operation_id: str | None,
    ) -> None:
        original(path, control, operation_id=operation_id)
        if control.status == "clear":
            raise StateBackendUnknownCommitError(
                "injected clear uncertainty",
                operation_id=operation_id,
            )

    monkeypatch.setattr(backend, "_write_control", fail_after_clear)
    with backend.operation(_address()) as operation:
        active = _begin(operation)
        evidence = RecoverySnapshotEvidence.from_operation_snapshot(active)
        record = _resolution(evidence, "rolled_back")

        recovered = operation.finalize_recovery(active, evidence, record, None)

    assert recovered.control.control.status == "clear"
    assert len(cast(list[object], _history_payload(tmp_path)["events"])) == 2


def test_exact_retry_verifies_completed_recovery_after_unknown_clear_outcome(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    backend = LocalDeploymentStateBackend(tmp_path)
    original_write = backend._write_control
    original_read = backend._read_control
    fail_next_verification = False

    def fail_after_clear(
        path: Path,
        control: OperationControlState,
        *,
        operation_id: str | None,
    ) -> None:
        nonlocal fail_next_verification
        original_write(path, control, operation_id=operation_id)
        if control.status == "clear":
            fail_next_verification = True
            raise StateBackendUnknownCommitError(
                "injected clear uncertainty",
                operation_id=operation_id,
            )

    def fail_verification_once(address: StateAddress) -> object:
        nonlocal fail_next_verification
        if fail_next_verification:
            fail_next_verification = False
            raise StateBackendInvalidStateError("injected post-clear verification failure")
        return original_read(address)

    monkeypatch.setattr(backend, "_write_control", fail_after_clear)
    monkeypatch.setattr(backend, "_read_control", fail_verification_once)
    with backend.operation(_address()) as operation:
        active = _begin(operation)
        evidence = RecoverySnapshotEvidence.from_operation_snapshot(active)
        record = _resolution(evidence, "rolled_back")
        with pytest.raises(StateBackendInvalidStateError):
            operation.finalize_recovery(active, evidence, record, None)

    assert backend.read_control(_address()).control.status == "clear"
    retried_record = RecoveryResolutionRecord(**{**record.__dict__, "resolved_at": _RETRIED_AT})
    with backend.operation(_address()) as operation:
        clear = operation.observe()
        recovered = operation.finalize_recovery(
            clear,
            evidence,
            retried_record,
            None,
        )

    assert recovered == clear
    assert len(cast(list[object], _history_payload(tmp_path)["events"])) == 2


def test_pending_intent_rejects_conflicting_retry_and_then_allows_exact_retry(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    backend = LocalDeploymentStateBackend(tmp_path)
    original_append = backend._append_recovery_history_locked

    def fail_before_resolution(*args: object, **kwargs: object) -> object:
        if args[2] == "recovery_resolution":
            record = args[3]
            raise StateBackendUnknownCommitError(
                "injected resolution uncertainty",
                operation_id=record.recovery_operation_id,  # type: ignore[attr-defined]
            )
        return original_append(*args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(
        backend,
        "_append_recovery_history_locked",
        fail_before_resolution,
    )
    with backend.operation(_address()) as operation:
        active = _begin(operation)
        evidence = RecoverySnapshotEvidence.from_operation_snapshot(active)
        record = _resolution(evidence, "rolled_back")
        with pytest.raises(StateBackendUnknownCommitError):
            operation.finalize_recovery(active, evidence, record, None)

    with backend.operation(_address()) as operation:
        current = operation.observe()
        conflicting_same_id = _resolution(
            evidence,
            "rolled_back",
            recovery_operation_id=record.recovery_operation_id,
            evidence_checksum=f"sha256:{'b' * 64}",
        )
        with pytest.raises(StateBackendConflictError, match="conflicting"):
            operation.finalize_recovery(
                current,
                evidence,
                conflicting_same_id,
                None,
            )
        conflicting_id = _resolution(evidence, "rolled_back")
        with pytest.raises(StateBackendConflictError, match="different recovery"):
            operation.finalize_recovery(current, evidence, conflicting_id, None)

    monkeypatch.setattr(backend, "_append_recovery_history_locked", original_append)
    with backend.operation(_address()) as operation:
        recovered = operation.finalize_recovery(
            operation.observe(),
            evidence,
            record,
            None,
        )
    assert recovered.control.control.status == "clear"


@pytest.mark.parametrize(
    "mutation",
    ["unknown", "duplicate", "tampered_checksum", "wrong_mode", "oversized", "symlink"],
)
def test_local_recovery_history_rejects_malformed_or_unsafe_payloads(
    tmp_path: Path,
    mutation: str,
) -> None:
    backend = LocalDeploymentStateBackend(tmp_path)
    with backend.operation(_address()) as operation:
        active = _begin(operation)
        evidence = RecoverySnapshotEvidence.from_operation_snapshot(active)
        record = _resolution(evidence, "rolled_back")
        operation.finalize_recovery(active, evidence, record, None)

    path = local_recovery_history_path(tmp_path, environment="prod")
    if mutation == "unknown":
        payload = _history_payload(tmp_path)
        payload["secret"] = "forbidden"
        path.write_text(json.dumps(payload))
        path.chmod(0o600)
    elif mutation == "duplicate":
        original = path.read_text()
        path.write_text(original.replace("{", '{"history_version":1,', 1))
        path.chmod(0o600)
    elif mutation == "tampered_checksum":
        payload = _history_payload(tmp_path)
        events = cast(list[dict[str, object]], payload["events"])
        events[0]["checksum"] = f"sha256:{'0' * 64}"
        path.write_text(json.dumps(payload))
        path.chmod(0o600)
    elif mutation == "wrong_mode":
        path.chmod(0o644)
    elif mutation == "oversized":
        path.write_bytes(b" " * (1024 * 1024 + 1))
        path.chmod(0o600)
    else:
        target = tmp_path / "redirected-history.json"
        target.write_text(path.read_text())
        target.chmod(0o600)
        path.unlink()
        path.symlink_to(target)

    with pytest.raises((StateBackendInvalidStateError, StateFormatError)):
        backend._read_recovery_history(_address())


def test_history_identity_mismatch_is_rejected(tmp_path: Path) -> None:
    backend = LocalDeploymentStateBackend(tmp_path)
    path = local_recovery_history_path(tmp_path, environment="prod")
    path.parent.mkdir(parents=True)
    path.write_text(
        json.dumps(
            {
                "history_version": 1,
                "address": "streamt-state://local/other/prod",
                "events": [],
            }
        )
    )
    os.chmod(path, 0o600)

    with pytest.raises(StateIdentityError, match="another address"):
        backend._read_recovery_history(_address())
