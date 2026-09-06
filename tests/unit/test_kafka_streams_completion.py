"""Storage-only finalization preserves exact runner authority at every write boundary."""

from __future__ import annotations

import copy
import json
from dataclasses import replace

import pytest

from streamt.deployer.recovery import RecoverySnapshotEvidence
from streamt.deployer.state import (
    ManagedResourceRecord,
    StateFormatError,
    StateIdentityError,
    local_state_path,
)
from streamt.deployer.state_backend import (
    LocalDeploymentStateBackend,
    RecoveryRecord,
    RunnerCompletionRecord,
    StateBackendConflictError,
    StateBackendError,
    StateBackendInvalidStateError,
    StateBackendLockLostError,
    StateBackendRecoveryRequiredError,
    StateBackendUnknownCommitError,
    StateStoreIdentity,
    _LocalRecoveryHistory,
    _LocalRecoveryHistoryEvent,
    completed_runner_state_pair,
    local_recovery_history_path,
    state_checksum,
)
from tests.unit.test_kafka_streams_operation_evidence import (
    ADDRESS,
    OPERATION,
    RESOURCE,
    _boundaries,
    _desired_state,
    _intent,
    _postgres,
    _state,
)
from tests.unit.test_kafka_streams_resume_local import FAILED, PLAN, _backend, _history, _record
from tests.unit.test_local_recovery import _resolution
from tests.unit.test_postgres_kafka_streams_resume import _restore_validator


def _complete(operation, *, resumed=False, blocked=False, prefix=5, success=True, reviewed=True):
    before = operation.observe()
    intent = replace(_intent(before.state.state), reviewed_plan_checksum=PLAN if reviewed else None)
    current = operation.begin_operation(before, intent)
    boundaries = _boundaries(exit_code=143)
    for index, boundary in enumerate(boundaries[:prefix]):
        if boundary.status == "completed":
            boundary = replace(boundary, succeeded=success)
        current = operation.record_progress(current, boundary)
        if resumed and index == 0:
            interrupted = operation.mark_recovery_required(current, RecoveryRecord(OPERATION, "runner_interrupted", FAILED, None))
            current = operation.resume_operation(interrupted, _record(interrupted))
    if blocked:
        current = operation.mark_recovery_required(current, RecoveryRecord(OPERATION, "runner_interrupted", FAILED, 0 if prefix == 5 and success else None))
    return current


def _receipt(snapshot):
    prior, result = completed_runner_state_pair(snapshot, allow_written_result=True)
    return RunnerCompletionRecord(OPERATION, snapshot.address, snapshot.state.store, snapshot.control.control,
                                  prior.serial, state_checksum(prior), result.serial, state_checksum(result), FAILED)


def _forbid(*args, **kwargs):
    raise AssertionError("unexpected durable write")


@pytest.mark.parametrize("resumed", [False, True])
@pytest.mark.parametrize("blocked", [False, True])
def test_local_completion_archives_full_terminal_control_before_only_checksum_and_serial_change(tmp_path, monkeypatch, resumed, blocked):
    state = _state()
    other = "streamt://payments/prod/topic/unrelated"
    state.resources[other] = ManagedResourceRecord("unrelated", "managed", "sha256:" + "f" * 64, "kafka:cluster-a")
    state.save(local_state_path(tmp_path, environment="prod"))
    backend = LocalDeploymentStateBackend(tmp_path)
    with backend.operation(ADDRESS) as operation:
        completed = _complete(operation, resumed=resumed, blocked=blocked)
        prior, result = completed_runner_state_pair(completed)
        history_before = _history(backend)
        original = operation.compare_and_swap

        def commit(observation, desired):
            archive = _history(backend)
            receipt = archive.completion_for(OPERATION)
            assert receipt is not None
            assert receipt.control == completed.control.control
            assert archive.events[:-1] == history_before.events
            assert desired == result
            assert observation.state == prior
            return original(observation, desired)

        monkeypatch.setattr(operation, "compare_and_swap", commit)
        final = operation.finalize_completed_runner(completed)
        assert final.control.control.status == "clear"
        assert final.state.state == result
        assert result.resources[other] == state.resources[other]
        assert result.serial == state.serial + 1
        assert prior == state
        assert prior is not completed.state.state
        assert result is not prior
        assert result.resources is not prior.resources
    archive = _history(LocalDeploymentStateBackend(tmp_path))
    assert archive.history_version == 3
    assert archive.events[-1].event_version == 3
    assert archive.events[-1].kind == "runner_completed"
    assert archive.completion_for(OPERATION).control == completed.control.control
    assert archive.completion_for(OPERATION).control.recovery == completed.control.control.recovery
    assert archive.completion_for(OPERATION).control.resume_history == completed.control.control.resume_history
    assert _LocalRecoveryHistory.from_dict(archive.to_dict(), expected_address=ADDRESS) == archive
    assert local_recovery_history_path(tmp_path, environment="prod").stat().st_mode & 0o777 == 0o600


@pytest.mark.parametrize("resumed", [False, True])
@pytest.mark.parametrize("blocked", [False, True])
@pytest.mark.parametrize("phase", ["audit_before", "audit_after", "state_before", "state_after", "clear_before", "clear_after"])
def test_local_completion_unknown_ack_stops_and_fresh_lock_reuses_exact_receipt(tmp_path, monkeypatch, resumed, blocked, phase):
    backend = _backend(tmp_path)
    with backend.operation(ADDRESS) as operation:
        completed = _complete(operation, resumed=resumed, blocked=blocked)
        before_count = len(_history(backend).events)
        writes = []
        for label, owner, method in (("audit", backend, "_write_recovery_history"), ("state", operation, "compare_and_swap"), ("clear", backend, "_write_control")):
            original = getattr(owner, method)

            def write(*args, _label=label, _original=original, **kwargs):
                writes.append(_label)
                if phase == _label + "_before":
                    raise StateBackendUnknownCommitError("lost acknowledgement", operation_id=OPERATION)
                result = _original(*args, **kwargs)
                if phase == _label + "_after":
                    raise StateBackendUnknownCommitError("lost acknowledgement", operation_id=OPERATION)
                return result

            monkeypatch.setattr(owner, method, write)
        with pytest.raises(StateBackendUnknownCommitError):
            operation.finalize_completed_runner(completed)
        count = {"audit": 1, "state": 2, "clear": 3}[phase.split("_")[0]]
        assert writes == ["audit", "state", "clear"][:count]
        assert backend.read(ADDRESS).state.serial == (2 if phase in ("state_after", "clear_before", "clear_after") else 1)
        assert backend.read_control(ADDRESS).control.status == ("clear" if phase == "clear_after" else completed.control.control.status)
        original_receipt = _history(backend).completion_for(OPERATION)
        assert (original_receipt is None) == (phase == "audit_before")
    reopened = LocalDeploymentStateBackend(tmp_path)
    with reopened.operation(ADDRESS) as operation:
        current = operation.observe()
        if phase == "clear_after":
            with pytest.raises(StateBackendRecoveryRequiredError):
                operation.finalize_completed_runner(current)
        else:
            if phase in ("state_after", "clear_before"):
                monkeypatch.setattr(operation, "compare_and_swap", _forbid)
            operation.finalize_completed_runner(current)
        assert operation.observe().state.state == _desired_state()
        assert operation.observe().control.control.status == "clear"
    archive = _history(reopened)
    assert len(archive.events) == before_count + 1
    if original_receipt is not None:
        assert archive.completion_for(OPERATION) == original_receipt


@pytest.mark.parametrize("blocked", [False, True])
def test_local_recognizes_generic_ownership_only_commit_without_increment_or_rewrite(tmp_path, monkeypatch, blocked):
    backend = _backend(tmp_path)
    with backend.operation(ADDRESS) as operation:
        completed = _complete(operation, blocked=blocked)
        _desired_state().save(local_state_path(tmp_path, environment="prod"))
        current = operation.observe()
        with pytest.raises(StateBackendConflictError):
            completed_runner_state_pair(current)
        assert completed_runner_state_pair(current, allow_written_result=True) == (_state(), _desired_state())
        monkeypatch.setattr(operation, "compare_and_swap", _forbid)
        final = operation.finalize_completed_runner(current)
        assert final.state.state == _desired_state()
        assert _history(backend).completion_for(OPERATION).control == completed.control.control


@pytest.mark.parametrize(("prefix", "success", "reviewed"), [(p, True, True) for p in range(5)] + [(5, False, True), (5, True, False)])
@pytest.mark.parametrize("blocked", [False, True])
def test_local_incomplete_failed_unreviewed_never_writes(tmp_path, monkeypatch, prefix, success, reviewed, blocked):
    backend = _backend(tmp_path)
    with backend.operation(ADDRESS) as operation:
        snapshot = _complete(operation, prefix=prefix, success=success, reviewed=reviewed, blocked=blocked)
        monkeypatch.setattr(backend, "_write_recovery_history", _forbid)
        monkeypatch.setattr(backend, "_write_control", _forbid)
        monkeypatch.setattr(operation, "compare_and_swap", _forbid)
        with pytest.raises(StateBackendRecoveryRequiredError):
            operation.finalize_completed_runner(snapshot)


@pytest.mark.parametrize("damage", ["store", "state", "unrelated_result", "serial", "runner_record", "control", "lock"])
def test_local_stale_or_foreign_completion_rejects_before_first_write(tmp_path, monkeypatch, damage):
    backend = _backend(tmp_path)
    with backend.operation(ADDRESS) as operation:
        snapshot = _complete(operation, resumed=True, blocked=True)
        if damage == "store":
            snapshot = replace(snapshot, state=replace(snapshot.state, store=StateStoreIdentity("local", "00000000-0000-4000-8000-000000000099")))
        elif damage in ("state", "unrelated_result", "serial", "runner_record"):
            state = _desired_state() if damage == "unrelated_result" else _state()
            if damage in ("state", "unrelated_result"):
                state.resources["streamt://payments/prod/topic/unrelated"] = ManagedResourceRecord("x", "managed", "sha256:" + "e" * 64, "kafka:cluster-a")
            elif damage == "serial":
                state.serial = 3
            else:
                state.resources[RESOURCE] = replace(state.resources[RESOURCE], physical_name="foreign")
            state.save(local_state_path(tmp_path, environment="prod"))
            snapshot = operation.observe()
        elif damage == "control":
            changed = replace(snapshot.control.control, recovery=replace(snapshot.control.control.recovery, failure_code="different_incident"))
            backend._write_control(backend._control_path(ADDRESS), changed, operation_id=OPERATION)
        else:
            monkeypatch.setattr(operation, "check_lock", lambda: (_ for _ in ()).throw(StateBackendLockLostError("lost")))
        monkeypatch.setattr(backend, "_write_recovery_history", _forbid)
        monkeypatch.setattr(backend, "_write_control", _forbid)
        monkeypatch.setattr(operation, "compare_and_swap", _forbid)
        with pytest.raises((StateBackendError, StateFormatError, StateIdentityError)):
            operation.finalize_completed_runner(snapshot)


@pytest.mark.parametrize("damage", ["missing", "truncated", "foreign", "receipt_result", "receipt_control"])
def test_local_completion_requires_exact_independent_archive(tmp_path, monkeypatch, damage):
    backend = _backend(tmp_path)
    with backend.operation(ADDRESS) as operation:
        snapshot = _complete(operation, resumed=True, blocked=True)
        history = _history(backend)
        if damage in ("missing", "truncated"):
            changed = _LocalRecoveryHistory(ADDRESS)
        elif damage == "foreign":
            resume = replace(history.resumes_for(OPERATION)[0], store=StateStoreIdentity("local", "00000000-0000-4000-8000-000000000099"))
            changed = _LocalRecoveryHistory(ADDRESS).append("operation_resumed", resume)
        else:
            record = _receipt(snapshot)
            if damage == "receipt_result":
                record = replace(record, result_state_checksum="sha256:" + "b" * 64)
            else:
                record = replace(record, control=replace(record.control, recovery=replace(record.control.recovery, failure_code="different_incident")))
            changed = history.append("runner_completed", record)
        backend._write_recovery_history(local_recovery_history_path(tmp_path, environment="prod"), changed, operation_id=OPERATION)
        monkeypatch.setattr(backend, "_write_recovery_history", _forbid)
        monkeypatch.setattr(backend, "_write_control", _forbid)
        monkeypatch.setattr(operation, "compare_and_swap", _forbid)
        with pytest.raises((StateBackendError, StateIdentityError)):
            operation.finalize_completed_runner(snapshot)


def test_completion_audit_freezes_terminal_incident_against_generic_mutators(tmp_path, monkeypatch):
    backend = _backend(tmp_path)
    with backend.operation(ADDRESS) as operation:
        snapshot = _complete(operation, resumed=True)
        with monkeypatch.context() as patch:
            patch.setattr(operation, "compare_and_swap", lambda *a, **kw: (_ for _ in ()).throw(StateBackendUnknownCommitError("before state", operation_id=OPERATION)))
            with pytest.raises(StateBackendUnknownCommitError):
                operation.finalize_completed_runner(snapshot)
        assert operation.observe() == snapshot
        monkeypatch.setattr(backend, "_write_recovery_history", _forbid)
        monkeypatch.setattr(backend, "_write_control", _forbid)
        monkeypatch.setattr(operation, "compare_and_swap", _forbid)
        for invoke in (
            lambda: operation.mark_recovery_required(snapshot, RecoveryRecord(OPERATION, "new_incident", FAILED, 0)),
            lambda: operation.commit_operation(snapshot, _desired_state()),
            lambda: operation.pending_resume_authorization(snapshot),
        ):
            with pytest.raises(StateBackendConflictError, match="completion"):
                invoke()


@pytest.mark.parametrize("damage", ["extra", "missing", "serial_bool", "result_equal", "failed", "incomplete", "wrong_op", "unknown_event_version", "wrong_envelope"])
def test_completion_record_and_version_are_strict(tmp_path, damage):
    backend = _backend(tmp_path)
    with backend.operation(ADDRESS) as operation:
        snapshot = _complete(operation)
        record = _receipt(snapshot)
    assert RunnerCompletionRecord.from_dict(record.to_dict()) == record
    payload = record.to_dict()
    if damage == "extra":
        payload["extra"] = True
    elif damage == "missing":
        payload.pop("store")
    elif damage == "serial_bool":
        payload["prior_state_serial"] = True
    elif damage == "result_equal":
        payload["result_state_checksum"] = payload["prior_state_checksum"]
    elif damage == "wrong_op":
        payload["operation_id"] = "00000000-0000-4000-8000-000000000099"
    elif damage == "failed":
        payload["control"]["progress"][-1]["succeeded"] = False
    elif damage == "incomplete":
        payload["control"]["progress"].pop()
    elif damage == "unknown_event_version":
        event = _LocalRecoveryHistoryEvent.create(sequence=0, kind="runner_completed", record=record, previous_checksum=None).to_dict()
        event["event_version"] = 2
        with pytest.raises(StateFormatError):
            _LocalRecoveryHistoryEvent.from_dict(event)
        return
    else:
        archive = _LocalRecoveryHistory(ADDRESS).append("runner_completed", record).to_dict()
        archive["history_version"] = 2
        with pytest.raises(StateFormatError):
            _LocalRecoveryHistory.from_dict(archive, expected_address=ADDRESS)
        return
    with pytest.raises((StateFormatError, StateIdentityError, StateBackendRecoveryRequiredError)):
        RunnerCompletionRecord.from_dict(payload)


def test_completion_archive_rejects_duplicate_and_lost_resume_chain(tmp_path):
    backend = _backend(tmp_path)
    with backend.operation(ADDRESS) as operation:
        snapshot = _complete(operation, resumed=True, blocked=True)
        record = _receipt(snapshot)
    history = _history(backend)
    with pytest.raises(StateFormatError):
        _LocalRecoveryHistory(ADDRESS).append("runner_completed", record)
    completed = history.append("runner_completed", record)
    with pytest.raises(StateFormatError):
        completed.append("runner_completed", record)
    with pytest.raises(StateFormatError):
        completed.append("operation_resumed", history.resumes_for(OPERATION)[0])
    assert [event.to_dict() for event in completed.events[:-1]] == [event.to_dict() for event in history.events]


@pytest.mark.parametrize("resolved", [False, True])
def test_local_competing_recovery_resolution_blocks_completed_runner_finalization(tmp_path, monkeypatch, resolved):
    backend = _backend(tmp_path)
    with backend.operation(ADDRESS) as operation:
        snapshot = _complete(operation, resumed=True, blocked=True)
        resolution = _resolution(RecoverySnapshotEvidence.from_operation_snapshot(snapshot), "observed")
        archive = _history(backend).append("recovery_intent", resolution)
        if resolved:
            archive = archive.append("recovery_resolution", resolution)
        backend._write_recovery_history(local_recovery_history_path(tmp_path, environment="prod"), archive, operation_id=OPERATION)
        monkeypatch.setattr(backend, "_write_recovery_history", _forbid)
        monkeypatch.setattr(backend, "_write_control", _forbid)
        monkeypatch.setattr(operation, "compare_and_swap", _forbid)
        with pytest.raises(StateBackendConflictError, match="recovery resolution"):
            operation.finalize_completed_runner(snapshot)


def test_local_completion_audit_size_failure_cannot_write_state_or_clear(tmp_path, monkeypatch):
    backend = _backend(tmp_path)
    with backend.operation(ADDRESS) as operation:
        snapshot = _complete(operation)
        monkeypatch.setattr("streamt.deployer.state_backend.MAX_LOCAL_RECOVERY_HISTORY_BYTES", 128)
        monkeypatch.setattr(backend, "_write_control", _forbid)
        monkeypatch.setattr(operation, "compare_and_swap", _forbid)
        with pytest.raises(StateBackendInvalidStateError, match="size limit"):
            operation.finalize_completed_runner(snapshot)
        assert operation.observe() == snapshot


@pytest.mark.parametrize("boundary", ["audit", "state"])
def test_local_lock_loss_after_ack_stops_before_next_durable_write(tmp_path, monkeypatch, boundary):
    backend = _backend(tmp_path)
    with backend.operation(ADDRESS) as operation:
        snapshot = _complete(operation, resumed=True, blocked=True)
        owner, method = (backend, "_write_recovery_history") if boundary == "audit" else (operation, "compare_and_swap")
        original = getattr(owner, method)

        def lose_after(*args, **kwargs):
            result = original(*args, **kwargs)
            monkeypatch.setattr(operation, "check_lock", lambda: (_ for _ in ()).throw(StateBackendLockLostError("lost")))
            return result

        monkeypatch.setattr(owner, method, lose_after)
        monkeypatch.setattr(backend, "_write_control", _forbid)
        if boundary == "audit":
            monkeypatch.setattr(operation, "compare_and_swap", _forbid)
        with pytest.raises(StateBackendLockLostError):
            operation.finalize_completed_runner(snapshot)
        assert backend.read(ADDRESS).state.serial == (1 if boundary == "audit" else 2)
        assert backend.read_control(ADDRESS) == snapshot.control
        assert _history(backend).completion_for(OPERATION).control == snapshot.control.control


@pytest.mark.parametrize("resumed", [False, True])
@pytest.mark.parametrize("damage", ["unreadable", "changed", "missing", "extra", "lock_after_clear", "lock_during_history"])
def test_local_finalization_requires_exact_archive_and_held_lock_after_clear(tmp_path, monkeypatch, resumed, damage):
    backend = _backend(tmp_path)
    with backend.operation(ADDRESS) as operation:
        snapshot = _complete(operation, resumed=resumed, blocked=True)
        original_clear = backend._save_control_locked
        original_history = backend._read_recovery_history
        post_clear_reads = []

        def lost_lock():
            raise StateBackendLockLostError("lock lost after clear", operation_id=OPERATION)

        def clear_then_damage(*args, **kwargs):
            result = original_clear(*args, **kwargs)
            archive = original_history(ADDRESS)
            completion = archive.completion_for(OPERATION)
            assert completion is not None
            before_completion = _LocalRecoveryHistory(
                ADDRESS, archive.events[:-1], history_version=2 if resumed else 1,
            )

            def damaged_history(address):
                post_clear_reads.append(address)
                if damage == "unreadable":
                    raise StateBackendInvalidStateError("local recovery history is unreadable")
                if damage == "changed":
                    return before_completion.append("runner_completed", replace(completion, completed_at="2026-09-05T12:00:09Z"))
                if damage == "missing":
                    return before_completion
                if damage == "extra":
                    resolution = replace(
                        _resolution(RecoverySnapshotEvidence.from_operation_snapshot(snapshot), "observed"),
                        blocked_operation_id="00000000-0000-4000-8000-000000000099",
                    )
                    return archive.append("recovery_intent", resolution).append("recovery_resolution", resolution)
                if damage == "lock_during_history":
                    monkeypatch.setattr(operation, "check_lock", lost_lock)
                return archive

            monkeypatch.setattr(backend, "_read_recovery_history", damaged_history)
            if damage == "lock_after_clear":
                monkeypatch.setattr(operation, "check_lock", lost_lock)
            # After the acknowledged clear all remaining work must be reads.
            monkeypatch.setattr(backend, "_write_control", _forbid)
            monkeypatch.setattr(backend, "_write_recovery_history", _forbid)
            monkeypatch.setattr(operation, "compare_and_swap", _forbid)
            return result

        monkeypatch.setattr(backend, "_save_control_locked", clear_then_damage)
        with pytest.raises(StateBackendUnknownCommitError, match="postimage") as caught:
            operation.finalize_completed_runner(snapshot)
        assert caught.value.operation_id == OPERATION
        assert post_clear_reads == ([] if damage == "lock_after_clear" else [ADDRESS])
        assert backend.read(ADDRESS).state == _desired_state()
        assert backend.read_control(ADDRESS).control.status == "clear"
        assert original_history(ADDRESS).completion_for(OPERATION).control == snapshot.control.control


@pytest.mark.parametrize("resumed", [False, True])
@pytest.mark.parametrize("blocked", [False, True])
@pytest.mark.parametrize("schema_version", [1, 2])
def test_postgres_completion_preserves_full_incident_history_and_restores(monkeypatch, resumed, blocked, schema_version):
    operation, database, owner, _driver = _postgres(monkeypatch)
    snapshot = _complete(operation, resumed=resumed, blocked=blocked)
    before = copy.deepcopy(database.operation_history)
    writes = len(owner.dml_attempts)
    final = operation.finalize_completed_runner(snapshot)
    assert final.state.state == _desired_state()
    assert final.control.control.status == "clear"
    assert database.operation_history[:-1] == before
    assert database.operation_history[-1][1] == "succeeded"
    assert owner.dml_attempts[writes:] == ["update_current_state", "insert_state_history", "update_operation_control", "insert_operation_history"]
    _records, validate = _restore_validator(database, schema_version=schema_version)
    validate()


@pytest.mark.parametrize(("prefix", "success", "reviewed"), [(p, True, True) for p in range(5)] + [(5, False, True), (5, True, False)])
@pytest.mark.parametrize("blocked", [False, True])
def test_postgres_ineligible_finalization_never_attempts_dml(monkeypatch, prefix, success, reviewed, blocked):
    operation, _database, owner, _driver = _postgres(monkeypatch)
    snapshot = _complete(operation, prefix=prefix, success=success, reviewed=reviewed, blocked=blocked)
    writes = list(owner.dml_attempts)
    with pytest.raises(StateBackendRecoveryRequiredError):
        operation.finalize_completed_runner(snapshot)
    assert owner.dml_attempts == writes


@pytest.mark.parametrize("damage", ["postimage", "history", "state", "store", "control", "lock"])
def test_postgres_completion_exact_cas_and_history_before_dml(monkeypatch, damage):
    operation, database, owner, _driver = _postgres(monkeypatch)
    snapshot = _complete(operation, resumed=True, blocked=True)
    if damage == "postimage":
        database.state = _desired_state()
        database.state_revision += 1
        snapshot = operation.observe()
    elif damage == "history":
        database.operation_history.pop(2)
    elif damage == "state":
        database.state.serial += 1
    elif damage == "store":
        snapshot = replace(snapshot, state=replace(snapshot.state, store=StateStoreIdentity("postgres", "00000000-0000-4000-8000-000000000099")))
    elif damage == "control":
        database.control = replace(database.control, recovery=replace(database.control.recovery, failure_code="different_incident"))
    else:
        owner.cursor_value.lock_owned = False
    writes = list(owner.dml_attempts)
    with pytest.raises((StateBackendError, StateFormatError, StateIdentityError)):
        operation.finalize_completed_runner(snapshot)
    assert owner.dml_attempts == writes


@pytest.mark.parametrize("mode", ["reject", "apply_then_raise", "corrupt_history"])
def test_postgres_completion_commit_ack_is_never_replayed(monkeypatch, mode):
    operation, database, owner, _driver = _postgres(monkeypatch)
    snapshot = _complete(operation, resumed=True, blocked=True)
    owner.commit_mode = mode
    before = copy.deepcopy(database.operation_history)
    writes = len(owner.dml_attempts)
    if mode == "apply_then_raise":
        assert operation.finalize_completed_runner(snapshot).state.state == _desired_state()
    else:
        with pytest.raises(StateBackendError):
            operation.finalize_completed_runner(snapshot)
    assert owner.dml_attempts[writes:] == ["update_current_state", "insert_state_history", "update_operation_control", "insert_operation_history"]
    if mode == "reject":
        assert database.state == _state()
        assert database.control == snapshot.control.control
        assert database.operation_history == before
    else:
        assert database.state == _desired_state()
    attempts = list(owner.dml_attempts)
    with pytest.raises(StateBackendError):
        operation.finalize_completed_runner(snapshot)
    assert owner.dml_attempts == attempts


def test_postgres_restore_does_not_generalize_success_after_interruption(monkeypatch):
    operation, database, _owner, _driver = _postgres(monkeypatch)
    snapshot = _complete(operation, blocked=True)
    operation.finalize_completed_runner(snapshot)
    records, validate = _restore_validator(database)
    index = next(i for i, row in enumerate(records) if row[5] == "recovery_required")
    row = list(records[index])
    control = json.loads(row[6])
    control["intent"]["reviewed_plan_checksum"] = None
    # Editing all prior preimages coherently must still not permit an
    # unreviewed runner to cross the new terminal-after-incident exception.
    for i, entry in enumerate(records[:-1]):
        changed = list(entry)
        payload = json.loads(changed[6])
        payload["intent"]["reviewed_plan_checksum"] = None
        changed[6] = json.dumps(payload, sort_keys=True, separators=(",", ":"))
        changed[7] = len(changed[6].encode())
        records[i] = tuple(changed)
    with pytest.raises((StateBackendInvalidStateError, StateBackendRecoveryRequiredError)):
        validate()
