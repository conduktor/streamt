"""Public read-only retrieval of exact local prewritten resume authority."""

from __future__ import annotations

import uuid
from dataclasses import replace

import pytest

from streamt.deployer.recovery import RecoverySnapshotEvidence
from streamt.deployer.state import (
    LocalState,
    StateFormatError,
    StateIdentityError,
    local_state_path,
)
from streamt.deployer.state_backend import (
    LocalDeploymentStateBackend,
    OperationSnapshot,
    RecoveryRecord,
    StateAddress,
    StateBackendConflictError,
    StateBackendInvalidStateError,
    StateBackendLockLostError,
    StateBackendUnknownCommitError,
    StateRevision,
    StateStoreIdentity,
    _control_revision,
    _LocalRecoveryHistory,
)
from tests.unit.test_kafka_streams_operation_evidence import (
    _boundaries,
    _desired_state,
    _intent,
    _state,
)
from tests.unit.test_kafka_streams_resume_local import (
    ADDRESS,
    FAILED,
    OPERATION,
    RESOURCE,
    _backend,
    _blocked,
    _history,
    _record,
)
from tests.unit.test_local_recovery import _resolution


def _leave_pending(backend, operation, monkeypatch, *, blocked=None, prefix=1):
    blocked = blocked or _blocked(operation, prefix)
    record = _record(blocked)
    with monkeypatch.context() as context:
        def fail(*_args, **_kwargs):
            raise StateBackendUnknownCommitError("control response unavailable")
        context.setattr(backend, "_write_control", fail)
        with pytest.raises(StateBackendUnknownCommitError):
            operation.resume_operation(blocked, record)
    return blocked, record


def _forbid_writes(monkeypatch, backend):
    def forbidden(*_args, **_kwargs):
        pytest.fail("pending authorization lookup must perform no writes")
    monkeypatch.setattr(backend, "_write_control", forbidden)
    monkeypatch.setattr(backend, "_write_recovery_history", forbidden)
    monkeypatch.setattr(backend, "_append_recovery_history_locked", forbidden)
    monkeypatch.setattr(LocalState, "save", forbidden)


@pytest.mark.parametrize("prefix", range(5))
def test_new_backend_and_lock_return_exact_deserialized_pending_record_without_writes(tmp_path, monkeypatch, prefix):
    backend = _backend(tmp_path)
    with backend.operation(ADDRESS) as operation:
        blocked, record = _leave_pending(backend, operation, monkeypatch, prefix=prefix)
    reopened = LocalDeploymentStateBackend(tmp_path)
    paths = [local_state_path(tmp_path, environment="prod"), reopened._control_path(ADDRESS), reopened._recovery_history_path(ADDRESS)]
    contents = [path.read_bytes() for path in paths]
    with reopened.operation(ADDRESS) as operation:
        snapshot = operation.observe()
        assert snapshot == blocked
        _forbid_writes(monkeypatch, reopened)
        recovered = operation.pending_resume_authorization(snapshot)
        assert recovered == record
        assert recovered is not record
        assert operation.pending_resume_authorization(snapshot) == recovered
        assert operation.observe() == snapshot
    assert [path.read_bytes() for path in paths] == contents


@pytest.mark.parametrize("status", ["clear", "active_v4", "blocked_v4", "active_v5", "blocked_v5", "committed"])
def test_exact_matching_audit_returns_none_without_claiming_resume_eligibility(tmp_path, monkeypatch, status):
    backend = _backend(tmp_path)
    with backend.operation(ADDRESS) as operation:
        if status == "clear":
            snapshot = operation.observe()
        elif status == "active_v4":
            before = operation.observe()
            snapshot = operation.begin_operation(before, _intent(before.state.state))
        else:
            snapshot = _blocked(operation)
            if status != "blocked_v4":
                snapshot = operation.resume_operation(snapshot, _record(snapshot))
            if status == "blocked_v5":
                snapshot = operation.mark_recovery_required(snapshot, RecoveryRecord(OPERATION, "later", FAILED, None))
            if status == "committed":
                for boundary in _boundaries(exit_code=143)[1:]:
                    snapshot = operation.record_progress(snapshot, boundary)
                snapshot = operation.commit_operation(snapshot, _desired_state())
        _forbid_writes(monkeypatch, backend)
        assert operation.pending_resume_authorization(snapshot) is None
        assert operation.observe() == snapshot


def test_pending_second_resume_retains_exact_prior_history(tmp_path, monkeypatch):
    backend = _backend(tmp_path)
    with backend.operation(ADDRESS) as operation:
        blocked = _blocked(operation, 2)
        first = _record(blocked)
        active = operation.resume_operation(blocked, first)
        blocked = operation.mark_recovery_required(active, RecoveryRecord(OPERATION, "second", FAILED, None))
        blocked, second = _leave_pending(backend, operation, monkeypatch, blocked=blocked)
    with LocalDeploymentStateBackend(tmp_path).operation(ADDRESS) as operation:
        current = operation.observe()
        recovered = operation.pending_resume_authorization(current)
        assert recovered == second
        assert current.control.control.resume_history == (first,)
        active = operation.resume_operation(current, recovered)
        assert active.control.control.resume_history == (first, second)


@pytest.mark.parametrize("change", ["store", "address", "state_content", "state_revision", "control_revision", "control_content", "incident"])
def test_lookup_rejects_snapshot_not_equal_to_current_full_authority(tmp_path, monkeypatch, change):
    backend = _backend(tmp_path)
    with backend.operation(ADDRESS) as operation:
        snapshot, _record_value = _leave_pending(backend, operation, monkeypatch)
        if change == "store":
            snapshot = replace(snapshot, state=replace(snapshot.state, store=StateStoreIdentity("local", str(uuid.uuid4()))))
        elif change == "address":
            address = StateAddress("other", "payments", "prod")
            control = replace(snapshot.control.control, address=address)
            snapshot = OperationSnapshot(replace(snapshot.state, address=address), replace(snapshot.control, control=control, revision=_control_revision(control)))
        elif change == "state_content":
            state = _state()
            state.resources[RESOURCE] = replace(state.resources[RESOURCE], artifact_checksum="sha256:" + "f" * 64)
            snapshot = replace(snapshot, state=replace(snapshot.state, state=state))
        elif change == "state_revision":
            snapshot = replace(snapshot, state=replace(snapshot.state, revision=StateRevision("stale-state")))
        elif change == "control_revision":
            snapshot = replace(snapshot, control=replace(snapshot.control, revision=StateRevision("stale-control")))
        else:
            control = snapshot.control.control
            control = replace(control, recovery=replace(control.recovery, failure_code="different")) if change == "incident" else replace(control, intent=replace(control.intent, actor="different"))
            snapshot = replace(snapshot, control=replace(snapshot.control, control=control))
        _forbid_writes(monkeypatch, backend)
        with pytest.raises((StateBackendConflictError, StateIdentityError)):
            operation.pending_resume_authorization(snapshot)


@pytest.mark.parametrize("value", [None, {}, "snapshot", "control_only"])
def test_lookup_requires_full_typed_snapshot(tmp_path, monkeypatch, value):
    backend = _backend(tmp_path)
    with backend.operation(ADDRESS) as operation:
        snapshot = operation.observe()
        supplied = snapshot.control if value == "control_only" else value
        _forbid_writes(monkeypatch, backend)
        with pytest.raises(StateFormatError, match="exact full"):
            operation.pending_resume_authorization(supplied)


@pytest.mark.parametrize("change", ["state", "control", "lock", "archive"])
def test_lookup_rechecks_lock_snapshot_and_archive_after_initial_archive_read(tmp_path, monkeypatch, change):
    backend = _backend(tmp_path)
    with backend.operation(ADDRESS) as operation:
        snapshot, _record_value = _leave_pending(backend, operation, monkeypatch)
        original_history = backend._read_recovery_history
        original_state = operation.read
        original_control = operation.read_control
        original_lock = operation.check_lock
        archive_read = [False]

        def history(address):
            if archive_read[0] and change == "archive":
                return _LocalRecoveryHistory(ADDRESS)
            result = original_history(address)
            archive_read[0] = True
            return result

        def state():
            result = original_state()
            return replace(result, revision=StateRevision("changed")) if archive_read[0] and change == "state" else result

        def control():
            result = original_control()
            return replace(result, revision=StateRevision("changed")) if archive_read[0] and change == "control" else result

        def lock():
            if archive_read[0] and change == "lock":
                raise StateBackendLockLostError("lost")
            original_lock()

        monkeypatch.setattr(backend, "_read_recovery_history", history)
        monkeypatch.setattr(operation, "read", state)
        monkeypatch.setattr(operation, "read_control", control)
        monkeypatch.setattr(operation, "check_lock", lock)
        _forbid_writes(monkeypatch, backend)
        with pytest.raises((StateBackendConflictError, StateBackendLockLostError)):
            operation.pending_resume_authorization(snapshot)


@pytest.mark.parametrize("damage", ["missing_expected", "truncated_expected", "multiple_extra", "changed_prefix", "wrong_incident", "wrong_checksum", "active_extra", "foreign_store", "malformed"])
def test_lookup_rejects_missing_or_unmatched_archive_instead_of_returning_none(tmp_path, monkeypatch, damage):
    backend = _backend(tmp_path)
    with backend.operation(ADDRESS) as operation:
        blocked = _blocked(operation, 2)
        first = _record(blocked)
        active = operation.resume_operation(blocked, first)
        snapshot = operation.mark_recovery_required(active, RecoveryRecord(OPERATION, "second", FAILED, None))
        second = _record(snapshot)
        expected = _history(backend)
        if damage == "missing_expected":
            history = _LocalRecoveryHistory(ADDRESS)
        elif damage == "truncated_expected":
            active = operation.resume_operation(snapshot, second)
            snapshot = operation.mark_recovery_required(active, RecoveryRecord(OPERATION, "third", FAILED, None))
            history = expected
        elif damage == "multiple_extra":
            extra = replace(second, resume_id=str(uuid.uuid4()), source_control_checksum="sha256:" + "e" * 64)
            history = expected.append("operation_resumed", second).append("operation_resumed", extra)
        elif damage == "changed_prefix":
            changed = replace(first, actor="changed")
            history = _LocalRecoveryHistory(ADDRESS).append("operation_resumed", changed)
        elif damage == "wrong_incident":
            changed = replace(second, recovery=replace(second.recovery, failure_code="changed"))
            history = expected.append("operation_resumed", changed)
        elif damage == "wrong_checksum":
            history = expected.append("operation_resumed", replace(second, source_control_checksum="sha256:" + "f" * 64))
        elif damage == "active_extra":
            snapshot = active
            history = expected.append("operation_resumed", second)
            backend._write_control(backend._control_path(ADDRESS), active.control.control, operation_id=OPERATION)
        elif damage == "foreign_store":
            foreign = replace(second, store=StateStoreIdentity("local", str(uuid.uuid4())))
            history = expected.append("operation_resumed", foreign)
        else:
            history = expected
        backend._write_recovery_history(backend._recovery_history_path(ADDRESS), history, operation_id=OPERATION)
        if damage == "malformed":
            backend._recovery_history_path(ADDRESS).write_text("{broken")
        _forbid_writes(monkeypatch, backend)
        with pytest.raises((StateBackendConflictError, StateBackendInvalidStateError, StateFormatError, StateIdentityError)):
            operation.pending_resume_authorization(snapshot)


@pytest.mark.parametrize("resolved", [False, True])
def test_lookup_rejects_pending_or_completed_resolution_for_current_operation(tmp_path, monkeypatch, resolved):
    backend = _backend(tmp_path)
    with backend.operation(ADDRESS) as operation:
        snapshot = _blocked(operation)
        resolution = _resolution(RecoverySnapshotEvidence.from_operation_snapshot(snapshot), "rolled_back")
        history = _history(backend).append("recovery_intent", resolution)
        if resolved:
            history = history.append("recovery_resolution", resolution)
        backend._write_recovery_history(backend._recovery_history_path(ADDRESS), history, operation_id=OPERATION)
        _forbid_writes(monkeypatch, backend)
        with pytest.raises(StateBackendConflictError, match="recovery resolution"):
            operation.pending_resume_authorization(snapshot)


def test_lookup_after_lock_release_is_not_authority(tmp_path, monkeypatch):
    backend = _backend(tmp_path)
    with backend.operation(ADDRESS) as operation:
        snapshot, _record_value = _leave_pending(backend, operation, monkeypatch)
    _forbid_writes(monkeypatch, backend)
    with pytest.raises(StateBackendLockLostError):
        operation.pending_resume_authorization(snapshot)
