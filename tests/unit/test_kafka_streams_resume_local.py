"""Local durable resume authority and its independent crash-safe audit chain."""

from __future__ import annotations

import hashlib
import json
import os
import uuid
from dataclasses import replace
from pathlib import Path

import pytest

from streamt.deployer.recovery import RecoverySnapshotEvidence
from streamt.deployer.state import StateFormatError, StateIdentityError, local_state_path
from streamt.deployer.state_backend import (
    LocalDeploymentStateBackend,
    OperationControlState,
    OperationResumeRecord,
    RecoveryRecord,
    StateBackendConflictError,
    StateBackendInvalidStateError,
    StateBackendLockLostError,
    StateBackendRecoveryRequiredError,
    StateBackendUnknownCommitError,
    StateStoreIdentity,
    _LocalRecoveryHistory,
    _LocalRecoveryHistoryEvent,
    local_recovery_history_path,
)
from tests.unit.test_kafka_streams_operation_evidence import (
    ADDRESS,
    OPERATION,
    RESOURCE,
    _boundaries,
    _desired_state,
    _intent,
    _state,
)
from tests.unit.test_local_recovery import _resolution

PLAN = "sha256:" + "a" * 64
FAILED = "2026-09-05T12:00:01Z"
RESUMED = "2026-09-05T12:00:02Z"


def _backend(directory: Path):
    _state().save(local_state_path(directory, environment="prod"))
    return LocalDeploymentStateBackend(directory)


def _blocked(operation, prefix=1):
    before = operation.observe()
    intent = replace(_intent(before.state.state), reviewed_plan_checksum=PLAN)
    active = operation.begin_operation(before, intent)
    for progress in _boundaries(exit_code=143)[:prefix]:
        active = operation.record_progress(active, progress)
    return operation.mark_recovery_required(active, RecoveryRecord(OPERATION, "runner_interrupted", FAILED, None))


def _record(snapshot, **changes):
    record = OperationResumeRecord.create(snapshot, resume_id=str(uuid.uuid4()), actor="local-test", resumed_at=RESUMED)
    return replace(record, **changes)


def _history(backend):
    return backend._read_recovery_history(ADDRESS)


def test_control_only_delegate_cannot_drop_incident_authority_via_v4_downgrade(tmp_path):
    backend = _backend(tmp_path)
    with backend.operation(ADDRESS) as operation:
        blocked = _blocked(operation, prefix=1)
        active = operation.resume_operation(blocked, _record(blocked))
        downgraded = replace(active.control.control, control_version=4, resume_history=())
        backend._write_control(backend._control_path(ADDRESS), downgraded, operation_id=OPERATION)
        before = operation.read_control()
        with pytest.raises(StateBackendConflictError, match="resume audit"):
            operation.record_progress(before, _boundaries(exit_code=143)[1])
        assert operation.read_control() == before
        assert len(_history(backend).resumes_for(OPERATION)) == 1


@pytest.mark.parametrize("prefix", range(5))
def test_resume_archives_incident_before_control_and_preserves_original_v4_intent(tmp_path, prefix, monkeypatch):
    backend = _backend(tmp_path)
    with backend.operation(ADDRESS) as operation:
        blocked = _blocked(operation, prefix)
        record = _record(blocked)
        original = backend._write_control

        def control_write(path, control, *, operation_id):
            assert _history(backend).resumes_for(OPERATION) == (record,)
            assert backend.read_control(ADDRESS).control == blocked.control.control
            original(path, control, operation_id=operation_id)

        monkeypatch.setattr(backend, "_write_control", control_write)
        resumed = operation.resume_operation(blocked, record)
        assert resumed.control.control.status == "in_progress"
        assert resumed.control.control.control_version == 5
        assert resumed.control.control.intent == blocked.control.control.intent
        assert resumed.control.control.intent.to_dict(control_version=4) == blocked.control.control.intent.to_dict(control_version=4)
        assert resumed.control.control.progress == blocked.control.control.progress
        assert resumed.control.control.recovery is None
        assert resumed.control.control.resume_history == (record,)
        assert resumed.state == blocked.state
    reopened = LocalDeploymentStateBackend(tmp_path)
    assert reopened.read_control(ADDRESS) == resumed.control
    history = _history(reopened)
    assert history.history_version == 2
    assert history.events[0].event_version == 2
    assert history.events[0].kind == "operation_resumed"
    assert history.events[0].record.recovery == blocked.control.control.recovery
    assert local_recovery_history_path(tmp_path, environment="prod").stat().st_mode & 0o777 == 0o600


@pytest.mark.parametrize("phase", ["audit_before", "audit_after", "control_before", "control_after"])
def test_lost_audit_or_control_ack_keeps_durable_prefix_and_requires_exact_retry(tmp_path, phase, monkeypatch):
    backend = _backend(tmp_path)
    with backend.operation(ADDRESS) as operation:
        blocked = _blocked(operation)
        record = _record(blocked)
        method = "_write_recovery_history" if phase.startswith("audit") else "_write_control"
        original = getattr(backend, method)
        error = StateBackendUnknownCommitError("test write acknowledgement lost", operation_id=OPERATION)

        def fail(path, value, *, operation_id):
            if phase.endswith("after"):
                original(path, value, operation_id=operation_id)
            raise error

        monkeypatch.setattr(backend, method, fail)
        with pytest.raises(StateBackendUnknownCommitError) as caught:
            operation.resume_operation(blocked, record)
        assert caught.value is error
        assert backend.read(ADDRESS) == blocked.state
        assert len(_history(backend).events) == (0 if phase == "audit_before" else 1)
        current = operation.observe()
        assert current.control.control.status == ("in_progress" if phase == "control_after" else "recovery_required")
    # A new backend and actual reacquired file lock, not a process-local cache.
    backend = LocalDeploymentStateBackend(tmp_path)
    with backend.operation(ADDRESS) as operation:
        current = operation.observe()
        if phase == "control_after":
            with pytest.raises(StateBackendRecoveryRequiredError):
                operation.resume_operation(current, record)
            with pytest.raises(StateBackendConflictError):
                operation.resume_operation(blocked, record)
            assert _history(backend).resumes_for(OPERATION) == (record,)
        else:
            retry_record = record
            if phase != "audit_before":
                # The caller can recover the first authorization itself from
                # the durable sidecar after losing all process-local state.
                retry_record = _history(backend).resumes_for(OPERATION)[-1]
                assert retry_record == record
                assert retry_record is not record
                for changed in (
                    replace(record, resume_id=str(uuid.uuid4())),
                    replace(record, actor="different-actor"),
                    replace(record, resumed_at="2026-09-05T12:00:03Z"),
                ):
                    with pytest.raises(StateBackendConflictError):
                        operation.resume_operation(current, changed)
                    assert _history(backend).resumes_for(OPERATION) == (record,)
            resumed = operation.resume_operation(current, retry_record)
            assert resumed.control.control.resume_history == (record,)
            assert _history(backend).resumes_for(OPERATION) == (record,)


def test_lock_loss_after_audit_retains_authorization_without_reopening_control(tmp_path, monkeypatch):
    backend = _backend(tmp_path)
    with backend.operation(ADDRESS) as operation:
        blocked = _blocked(operation)
        record = _record(blocked)
        original = operation.check_lock
        error = StateBackendLockLostError("lost", operation_id=OPERATION)

        def check():
            if _history(backend).events:
                raise error
            original()

        monkeypatch.setattr(operation, "check_lock", check)
        with pytest.raises(StateBackendLockLostError) as caught:
            operation.resume_operation(blocked, record)
        assert caught.value is error
        assert backend.read_control(ADDRESS) == blocked.control
        assert _history(backend).resumes_for(OPERATION) == (record,)


def test_stale_control_or_state_fails_before_audit_write(tmp_path):
    backend = _backend(tmp_path)
    with backend.operation(ADDRESS) as operation:
        blocked = _blocked(operation)
        record = _record(blocked)
        forged_control = replace(blocked.control.control, recovery=replace(blocked.control.control.recovery, failure_code="different_incident"))
        forged = replace(blocked, control=replace(blocked.control, control=forged_control))
        forged_record = _record(forged)
        with pytest.raises(StateBackendConflictError):
            operation.resume_operation(forged, forged_record)
        assert not _history(backend).events
        changed = _state()
        changed.serial += 1
        changed.save(local_state_path(tmp_path, environment="prod"))
        with pytest.raises(StateBackendConflictError):
            operation.resume_operation(blocked, record)
        assert not _history(backend).events


def test_multiple_resume_incidents_survive_progress_marking_and_final_commit(tmp_path):
    backend = _backend(tmp_path)
    with backend.operation(ADDRESS) as operation:
        blocked = _blocked(operation)
        first = _record(blocked)
        active = operation.resume_operation(blocked, first)
        active = operation.record_progress(active, _boundaries(exit_code=143)[1])
        assert active.control.control.resume_history == (first,)
        second_blocked = operation.mark_recovery_required(active, RecoveryRecord(OPERATION, "another_interruption", "2026-09-05T12:00:03Z", None))
        assert second_blocked.control.control.resume_history == (first,)
        second = _record(second_blocked, resumed_at="2026-09-05T12:00:04Z")
        active = operation.resume_operation(second_blocked, second)
        for progress in _boundaries(exit_code=143)[2:]:
            active = operation.record_progress(active, progress)
            assert active.control.control.resume_history == (first, second)
        completed = operation.commit_operation(active, _desired_state())
        assert completed.control.control.status == "clear"
        assert completed.state.state == _desired_state()
    history = _history(LocalDeploymentStateBackend(tmp_path))
    assert history.resumes_for(OPERATION) == (first, second)
    assert [event.kind for event in history.events] == ["operation_resumed", "operation_resumed"]
    assert history.events[1].previous_checksum == history.events[0].checksum


def test_zero_prefix_resumed_incident_cannot_use_before_mutation_clear(tmp_path):
    backend = _backend(tmp_path)
    with backend.operation(ADDRESS) as operation:
        blocked = _blocked(operation, prefix=0)
        record = _record(blocked)
        active = operation.resume_operation(blocked, record)
        assert not active.control.control.progress
        with pytest.raises(StateBackendRecoveryRequiredError, match="explicit recovery"):
            operation.clear_before_mutation(active)
        assert operation.observe() == active
        assert _history(backend).resumes_for(OPERATION) == (record,)


@pytest.mark.parametrize("transition", ["progress", "mark", "commit", "finalize"])
def test_missing_resume_archive_blocks_later_transitions(tmp_path, transition):
    backend = _backend(tmp_path)
    with backend.operation(ADDRESS) as operation:
        active = operation.resume_operation(blocked := _blocked(operation), _record(blocked))
        if transition == "commit":
            for progress in _boundaries(exit_code=143)[1:]:
                active = operation.record_progress(active, progress)
        evidence = RecoverySnapshotEvidence.from_operation_snapshot(active)
        resolution = _resolution(evidence, "rolled_back")
        backend._write_recovery_history(backend._recovery_history_path(ADDRESS), _LocalRecoveryHistory(ADDRESS), operation_id=OPERATION)
        before_state, before_control = backend.read(ADDRESS), backend.read_control(ADDRESS)
        transitions = {
            "progress": lambda: operation.record_progress(active, _boundaries(exit_code=143)[1]),
            "mark": lambda: operation.mark_recovery_required(active, RecoveryRecord(OPERATION, "again", FAILED, None)),
            "commit": lambda: operation.commit_operation(active, _desired_state()),
            "finalize": lambda: operation.finalize_recovery(active, evidence, resolution, None),
        }
        with pytest.raises(StateBackendConflictError, match="audit"):
            transitions[transition]()
        assert backend.read(ADDRESS) == before_state
        assert backend.read_control(ADDRESS) == before_control


def test_pending_resume_audit_blocks_competing_recovery_resolution(tmp_path, monkeypatch):
    backend = _backend(tmp_path)
    with backend.operation(ADDRESS) as operation:
        blocked = _blocked(operation)
        record = _record(blocked)
        with monkeypatch.context() as context:
            context.setattr(backend, "_write_control", lambda *_args, **_kwargs: (_ for _ in ()).throw(StateBackendUnknownCommitError("test failure")))
            with pytest.raises(StateBackendUnknownCommitError):
                operation.resume_operation(blocked, record)
        evidence = RecoverySnapshotEvidence.from_operation_snapshot(blocked)
        resolution = _resolution(evidence, "rolled_back")
        with pytest.raises(StateBackendConflictError, match="audit"):
            operation.finalize_recovery(blocked, evidence, resolution, None)
        assert [event.kind for event in _history(backend).events] == ["operation_resumed"]
        assert backend.read_control(ADDRESS) == blocked.control


@pytest.mark.parametrize("resolved", [False, True])
def test_pending_or_completed_recovery_resolution_blocks_resume(tmp_path, resolved):
    backend = _backend(tmp_path)
    with backend.operation(ADDRESS) as operation:
        blocked = _blocked(operation)
        record = _record(blocked)
        evidence = RecoverySnapshotEvidence.from_operation_snapshot(blocked)
        resolution = _resolution(evidence, "rolled_back")
        history = _history(backend).append("recovery_intent", resolution)
        if resolved:
            history = history.append("recovery_resolution", resolution)
        backend._write_recovery_history(backend._recovery_history_path(ADDRESS), history, operation_id=OPERATION)
        with pytest.raises(StateBackendConflictError):
            operation.resume_operation(blocked, record)
        assert _history(backend) == history
        assert backend.read_control(ADDRESS) == blocked.control


def test_recovery_after_successful_resume_keeps_resume_archive_after_clear(tmp_path):
    backend = _backend(tmp_path)
    with backend.operation(ADDRESS) as operation:
        blocked = _blocked(operation)
        record = _record(blocked)
        active = operation.resume_operation(blocked, record)
        evidence = RecoverySnapshotEvidence.from_operation_snapshot(active)
        resolution = _resolution(evidence, "rolled_back")
        final = operation.finalize_recovery(active, evidence, resolution, None)
        assert final.control.control.status == "clear"
        assert operation.finalize_recovery(final, evidence, resolution, None) == final
    history = _history(backend)
    assert [event.kind for event in history.events] == ["operation_resumed", "recovery_intent", "recovery_resolution"]
    assert [event.event_version for event in history.events] == [2, 1, 1]
    assert history.resumes_for(OPERATION) == (record,)
    assert len(history.events_for(resolution.recovery_operation_id)) == 2


def test_legacy_v1_history_bytes_checksums_survive_mixed_v2_append(tmp_path):
    backend = _backend(tmp_path)
    with backend.operation(ADDRESS) as operation:
        blocked = _blocked(operation)
        record = _record(blocked)
        resolution = replace(_resolution(RecoverySnapshotEvidence.from_operation_snapshot(blocked), "rolled_back"), blocked_operation_id=str(uuid.uuid4()))
        legacy = _LocalRecoveryHistory(ADDRESS).append("recovery_intent", resolution).append("recovery_resolution", resolution)
        wire = legacy.to_dict()
        for event in wire["events"]:
            payload = {key: value for key, value in event.items() if key != "checksum"}
            assert event["checksum"] == "sha256:" + hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()).hexdigest()
        assert _LocalRecoveryHistory.from_dict(wire, expected_address=ADDRESS).to_dict() == wire
        mixed = legacy.append("operation_resumed", record)
        assert mixed.history_version == 2
        assert mixed.to_dict()["events"][:2] == wire["events"]
        assert mixed.events[-1].previous_checksum == legacy.events[-1].checksum
        assert _LocalRecoveryHistory.from_dict(mixed.to_dict(), expected_address=ADDRESS) == mixed


@pytest.mark.parametrize("field", ["history_version", "event_version", "checksum", "unknown_record_field", "duplicate_resume", "duplicate_incident", "mixed_kind", "wrong_store"])
def test_resume_history_rejects_tampering_malformed_versions_and_cross_store(tmp_path, field):
    backend = _backend(tmp_path)
    with backend.operation(ADDRESS) as operation:
        blocked = _blocked(operation)
        record = _record(blocked)
        operation.resume_operation(blocked, record)
    history = _history(backend)
    payload = history.to_dict()
    if field == "history_version":
        payload["history_version"] = 1
    elif field == "event_version":
        payload["events"][0]["event_version"] = 1
    elif field == "checksum":
        payload["events"][0]["checksum"] = "sha256:" + "f" * 64
    elif field == "unknown_record_field":
        payload["events"][0]["record"]["credentials"] = "not-allowed"
    elif field in {"duplicate_resume", "duplicate_incident"}:
        repeated = record if field == "duplicate_resume" else replace(record, resume_id=str(uuid.uuid4()))
        duplicate = _LocalRecoveryHistoryEvent.create(sequence=1, kind="operation_resumed", previous_checksum=history.events[0].checksum, record=repeated)
        payload["events"].append(duplicate.to_dict())
    elif field == "mixed_kind":
        payload["events"][0]["kind"] = "recovery_resolution"
    else:
        other = replace(record, store=StateStoreIdentity("local", str(uuid.uuid4())))
        foreign = _LocalRecoveryHistory(ADDRESS).append("operation_resumed", other)
        payload = foreign.to_dict()
    path = backend._recovery_history_path(ADDRESS)
    path.write_text(json.dumps(payload))
    with pytest.raises((StateBackendInvalidStateError, StateIdentityError)):
        _history(backend)


def test_loaded_v5_control_from_another_store_is_rejected(tmp_path):
    backend = _backend(tmp_path)
    with backend.operation(ADDRESS) as operation:
        blocked = _blocked(operation)
        active = operation.resume_operation(blocked, _record(blocked))
    payload = active.control.control.to_dict()
    payload["resume_history"][0]["store"]["store_id"] = str(uuid.uuid4())
    # The generic wire schema is valid; only the authoritative local backend
    # can prove that the embedded store belongs to this concrete directory.
    OperationControlState.from_dict(payload, expected_address=ADDRESS)
    backend._control_path(ADDRESS).write_text(json.dumps(payload))
    with pytest.raises(StateIdentityError, match="another local state store"):
        backend.read_control(ADDRESS)


def test_resume_audit_symlink_is_never_followed(tmp_path):
    backend = _backend(tmp_path)
    with backend.operation(ADDRESS) as operation:
        blocked = _blocked(operation)
        record = _record(blocked)
        target = tmp_path / "other.json"
        target.write_text("{}")
        os.chmod(target, 0o600)
        backend._recovery_history_path(ADDRESS).symlink_to(target)
        with pytest.raises(StateBackendInvalidStateError):
            operation.resume_operation(blocked, record)
        assert target.read_text() == "{}"
        assert backend.read_control(ADDRESS) == blocked.control


@pytest.mark.parametrize("field", ["actor", "source_control_checksum", "prior_state_serial", "progress_count", "operation_id"])
def test_bad_resume_authority_cannot_write_archive_or_control(tmp_path, field):
    backend = _backend(tmp_path)
    with backend.operation(ADDRESS) as operation:
        blocked = _blocked(operation)
        record = _record(blocked)
        values = {"actor": "password=forbidden", "source_control_checksum": "sha256:" + "f" * 64,
                  "prior_state_serial": 99, "progress_count": 0, "operation_id": str(uuid.uuid4())}
        with pytest.raises((StateFormatError, StateIdentityError, StateBackendConflictError)):
            operation.resume_operation(blocked, replace(record, **{field: values[field]}))
        assert not _history(backend).events
        assert backend.read_control(ADDRESS) == blocked.control


def test_changed_protected_ownership_cannot_resume(tmp_path):
    backend = _backend(tmp_path)
    with backend.operation(ADDRESS) as operation:
        blocked = _blocked(operation)
        record = _record(blocked)
        mutated = _state()
        mutated.resources[RESOURCE] = replace(mutated.resources[RESOURCE], artifact_checksum="sha256:" + "f" * 64)
        mutated.save(local_state_path(tmp_path, environment="prod"))
        with pytest.raises(StateBackendConflictError):
            operation.resume_operation(blocked, record)
        assert not _history(backend).events
