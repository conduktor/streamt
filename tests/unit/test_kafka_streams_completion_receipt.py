"""Read-only receipt and pending-terminal gates never manufacture resume authority."""

from __future__ import annotations

import copy
from dataclasses import replace
from datetime import datetime, timedelta, timezone

import pytest

from streamt.deployer.state import ManagedResourceRecord, StateError, local_state_path
from streamt.deployer.state_backend import (
    LocalDeploymentStateBackend,
    StateBackendConflictError,
    StateBackendInvalidStateError,
    StateBackendLockLostError,
    StateBackendRecoveryRequiredError,
    StateBackendUnknownCommitError,
    StateStoreIdentity,
    _LocalRecoveryHistory,
    local_recovery_history_path,
    state_checksum,
)
from tests.unit.test_kafka_streams_completion import _complete, _forbid
from tests.unit.test_kafka_streams_operation_evidence import (
    ADDRESS,
    OPERATION,
    RESOURCE,
    _desired_state,
    _postgres,
    _state,
)
from tests.unit.test_kafka_streams_resume_local import _backend, _history

UNKNOWN = "00000000-0000-4000-8000-000000000099"
RECORDED = datetime(2026, 9, 5, 12, 0, 7, 123456, tzinfo=timezone(timedelta(hours=-4)))


def _local_finish(path, *, resumed=False, blocked=False):
    backend = _backend(path)
    with backend.operation(ADDRESS) as operation:
        completed = _complete(operation, resumed=resumed, blocked=blocked)
        operation.finalize_completed_runner(completed)
    return backend, completed


def _readonly_local(operation, backend, patch):
    patch.setattr(backend, "_write_control", _forbid)
    patch.setattr(backend, "_write_recovery_history", _forbid)
    patch.setattr(operation, "compare_and_swap", _forbid)
    patch.setattr(operation, "begin_operation", _forbid)
    patch.setattr(operation, "resume_operation", _forbid)
    patch.setattr(operation, "finalize_completed_runner", _forbid)


@pytest.mark.parametrize("resumed", [False, True])
@pytest.mark.parametrize("blocked", [False, True])
def test_local_receipt_fresh_backend_lock_preserves_exact_control_and_reconstructs_prior(tmp_path, monkeypatch, resumed, blocked):
    original, completed = _local_finish(tmp_path, resumed=resumed, blocked=blocked)
    archived = _history(original)
    backend = LocalDeploymentStateBackend(tmp_path)
    with backend.operation(ADDRESS) as operation:
        snapshot = operation.observe()
        _readonly_local(operation, backend, monkeypatch)
        receipt = operation.completed_runner_receipt(snapshot, OPERATION)
        assert receipt == archived.completion_for(OPERATION)
        assert receipt is not archived.completion_for(OPERATION)
        assert receipt.control == completed.control.control
        prior = receipt.verify_result_state(snapshot.state.state)
        assert prior == _state()
        assert prior is not snapshot.state.state
        prior.resources.clear()
        assert snapshot.state.state == _desired_state()
        assert operation.observe() == snapshot
        assert _history(backend) == archived
        assert operation.completed_runner_receipt(snapshot, UNKNOWN) is None


def test_local_absence_is_verified_without_writing_or_asserting_success(tmp_path, monkeypatch):
    backend = _backend(tmp_path)
    with backend.operation(ADDRESS) as operation:
        snapshot = operation.observe()
        _readonly_local(operation, backend, monkeypatch)
        assert operation.completed_runner_receipt(snapshot, OPERATION) is None
        assert operation.observe() == snapshot
        assert not local_recovery_history_path(tmp_path, environment="prod").exists()


def test_receipt_result_helper_cannot_rebase_an_unrelated_change_into_its_checksum(tmp_path):
    backend, _completed = _local_finish(tmp_path)
    receipt = _history(backend).completion_for(OPERATION)
    current = _desired_state()
    current.resources["streamt://payments/prod/topic/unrelated"] = ManagedResourceRecord(
        "unrelated", "managed", "sha256:" + "b" * 64, "kafka:cluster-a",
    )
    with pytest.raises(StateBackendConflictError, match="exact runner completion result"):
        receipt.verify_result_state(current)
    # PostgreSQL derives the result checksum from its current row. Even a
    # coherently changed result hash cannot replace the original intent hash.
    rebased = replace(receipt, result_state_checksum=state_checksum(current))
    with pytest.raises(StateBackendConflictError, match="exact original state"):
        rebased.verify_result_state(current)


@pytest.mark.parametrize("damage", ["prior", "serial", "runner", "store", "stale_control", "stale_state", "missing_resume", "unreadable", "lock"])
def test_local_receipt_rejects_wrong_result_history_or_authority_without_writes(tmp_path, monkeypatch, damage):
    backend, _completed = _local_finish(tmp_path, resumed=True, blocked=True)
    with backend.operation(ADDRESS) as operation:
        snapshot = operation.observe()
        if damage in ("prior", "serial", "runner", "stale_state"):
            state = _state() if damage == "prior" else _desired_state()
            if damage == "serial":
                state.serial += 1
            elif damage == "runner":
                state.resources[RESOURCE] = replace(state.resources[RESOURCE], physical_name="foreign")
            elif damage == "stale_state":
                state.serial += 2
            state.save(local_state_path(tmp_path, environment="prod"))
            if damage != "stale_state":
                snapshot = operation.observe()
        elif damage == "store":
            snapshot = replace(snapshot, state=replace(snapshot.state, store=StateStoreIdentity("local", UNKNOWN)))
        elif damage == "stale_control":
            backend._write_control(backend._control_path(ADDRESS), _completed.control.control, operation_id=OPERATION)
        elif damage == "missing_resume":
            history = _history(backend)
            payload = history.to_dict()
            payload["events"].pop(0)
            monkeypatch.setattr(backend, "_load_recovery_history_payload", lambda _path: payload)
        elif damage == "unreadable":
            monkeypatch.setattr(backend, "_read_recovery_history", lambda *_: (_ for _ in ()).throw(StateBackendInvalidStateError("unreadable")))
        else:
            monkeypatch.setattr(operation, "check_lock", lambda: (_ for _ in ()).throw(StateBackendLockLostError("lost")))
        _readonly_local(operation, backend, monkeypatch)
        with pytest.raises(StateError):
            operation.completed_runner_receipt(snapshot, OPERATION)


@pytest.mark.parametrize("phase", ["history_first", "history_second"])
@pytest.mark.parametrize("damage", ["history", "state", "control", "lock"])
def test_local_receipt_rechecks_full_snapshot_and_archive_after_reads(tmp_path, monkeypatch, phase, damage):
    backend, completed = _local_finish(tmp_path, resumed=True)
    with backend.operation(ADDRESS) as operation:
        snapshot = operation.observe()
        original = backend._read_recovery_history
        calls = []

        def interfere(address):
            calls.append(address)
            archive = original(address)
            if len(calls) == (1 if phase == "history_first" else 2):
                if damage == "history":
                    return _LocalRecoveryHistory(ADDRESS)
                if damage == "state":
                    changed = _desired_state()
                    changed.serial += 1
                    changed.save(local_state_path(tmp_path, environment="prod"))
                elif damage == "control":
                    # Simulate an out-of-band state file edit, not API DML.
                    monkeypatch.setattr(operation, "read_control", lambda: completed.control)
                else:
                    monkeypatch.setattr(operation, "check_lock", lambda: (_ for _ in ()).throw(StateBackendLockLostError("lost")))
            return archive

        monkeypatch.setattr(backend, "_read_recovery_history", interfere)
        _readonly_local(operation, backend, monkeypatch)
        with pytest.raises(StateError):
            operation.completed_runner_receipt(snapshot, OPERATION)


@pytest.mark.parametrize("resumed", [False, True])
@pytest.mark.parametrize("blocked", [False, True])
@pytest.mark.parametrize("frontier", ["no_receipt", "receipt", "written_result"])
def test_local_pending_success_gate_reads_exact_preimage_or_postimage_without_mutation(tmp_path, monkeypatch, resumed, blocked, frontier):
    backend = _backend(tmp_path)
    with backend.operation(ADDRESS) as operation:
        snapshot = _complete(operation, resumed=resumed, blocked=blocked)
        if frontier != "no_receipt":
            with monkeypatch.context() as patch:
                if frontier == "receipt":
                    patch.setattr(operation, "compare_and_swap", lambda *_: (_ for _ in ()).throw(StateBackendUnknownCommitError("before state")))
                else:
                    patch.setattr(backend, "_save_control_locked", lambda *_: (_ for _ in ()).throw(StateBackendUnknownCommitError("before clear")))
                with pytest.raises(StateBackendUnknownCommitError):
                    operation.finalize_completed_runner(snapshot)
            snapshot = operation.observe()
        history = _history(backend)
        _readonly_local(operation, backend, monkeypatch)
        assert operation.validate_completed_runner_snapshot(snapshot) is None
        with pytest.raises(StateBackendRecoveryRequiredError):
            operation.completed_runner_receipt(snapshot, OPERATION)
        assert operation.observe() == snapshot
        assert _history(backend) == history


@pytest.mark.parametrize("damage", ["incomplete", "failed", "clear", "missing_resume", "wrong_receipt_result"])
def test_local_pending_gate_blocks_invalid_or_incoherent_authority(tmp_path, monkeypatch, damage):
    backend = _backend(tmp_path)
    with backend.operation(ADDRESS) as operation:
        snapshot = _complete(operation, resumed=True, blocked=True, prefix=4 if damage == "incomplete" else 5, success=damage != "failed")
        if damage == "clear":
            operation.finalize_completed_runner(snapshot)
            snapshot = operation.observe()
        elif damage == "missing_resume":
            monkeypatch.setattr(backend, "_read_recovery_history", lambda *_: _LocalRecoveryHistory(ADDRESS))
        elif damage == "wrong_receipt_result":
            from tests.unit.test_kafka_streams_completion import _receipt
            archive = _history(backend).append("runner_completed", replace(_receipt(snapshot), result_state_checksum="sha256:" + "c" * 64))
            backend._write_recovery_history(local_recovery_history_path(tmp_path, environment="prod"), archive, operation_id=OPERATION)
        _readonly_local(operation, backend, monkeypatch)
        with pytest.raises(StateError):
            operation.validate_completed_runner_snapshot(snapshot)


class _ReceiptCursor:
    """Add the existing durable recorded_at column to the legacy fake cursor."""

    def __init__(self, delegate):
        self.delegate = delegate
        self.recorded = [(RECORDED,)]
        self.current = None
        self.after_timestamp = None
        self.queries = []

    def execute(self, query, params=None):
        rendered = str(query)
        self.queries.append(rendered)
        self.current = None
        if rendered.startswith("SELECT recorded_at"):
            self.current = self.recorded
            if self.after_timestamp:
                self.after_timestamp()
        elif rendered.startswith("SELECT event_index") and params[3] == UNKNOWN:
            self.current = []
        else:
            return self.delegate.execute(query, params)

    def fetchall(self):
        return self.delegate.fetchall() if self.current is None else self.current

    def close(self):
        return self.delegate.close()


def _pg_finish(monkeypatch, *, resumed=False, blocked=False):
    operation, database, owner, driver = _postgres(monkeypatch)
    completed = _complete(operation, resumed=resumed, blocked=blocked)
    final = operation.finalize_completed_runner(completed)
    cursor = _ReceiptCursor(operation._cursor)
    operation._cursor = cursor
    return operation, database, owner, driver, completed, final, cursor


@pytest.mark.parametrize("resumed", [False, True])
@pytest.mark.parametrize("blocked", [False, True])
def test_postgres_receipt_uses_existing_durable_timestamp_full_journal_and_result(monkeypatch, resumed, blocked):
    operation, database, owner, _driver, completed, final, cursor = _pg_finish(monkeypatch, resumed=resumed, blocked=blocked)
    history = copy.deepcopy(database.operation_history)
    writes = list(owner.dml_attempts)
    receipt = operation.completed_runner_receipt(final, OPERATION)
    assert receipt.control == completed.control.control
    assert receipt.completed_at == "2026-09-05T16:00:07.123456Z"
    assert receipt.verify_result_state(final.state.state) == _state()
    assert operation.completed_runner_receipt(final, OPERATION) == receipt
    assert operation.completed_runner_receipt(final, UNKNOWN) is None
    assert database.operation_history == history
    assert owner.dml_attempts == writes
    assert any("REPEATABLE READ READ ONLY" in query for query in cursor.queries)
    assert not any(query.startswith(("INSERT", "UPDATE", "DELETE", "MERGE")) for query in cursor.queries)


@pytest.mark.parametrize("damage", ["missing_history", "truncated", "drop_incident", "state_history", "prior_state", "changed_state", "unrelated_state", "store", "stale_revision", "not_clear", "timestamp_absent", "timestamp_naive", "timestamp_text", "lock_after_timestamp"])
def test_postgres_receipt_never_accepts_incomplete_foreign_stale_or_corrupt_proof(monkeypatch, damage):
    operation, database, owner, _driver, completed, final, cursor = _pg_finish(monkeypatch, resumed=True, blocked=True)
    if damage == "missing_history":
        database.operation_history.clear()
    elif damage == "truncated":
        database.operation_history.pop()
    elif damage == "drop_incident":
        database.operation_history.pop(2)
    elif damage == "state_history":
        database.state_history.pop()
    elif damage in ("prior_state", "changed_state", "unrelated_state"):
        database.state = _state() if damage == "prior_state" else _desired_state()
        if damage == "changed_state":
            database.state.serial += 1
        elif damage == "unrelated_state":
            database.state.resources["streamt://payments/prod/topic/unrelated"] = ManagedResourceRecord(
                "unrelated", "managed", "sha256:" + "b" * 64, "kafka:cluster-a",
            )
        final = operation.observe()
    elif damage == "store":
        final = replace(final, state=replace(final.state, store=StateStoreIdentity("postgres", UNKNOWN)))
    elif damage == "stale_revision":
        database.state_revision += 1
    elif damage == "not_clear":
        final = replace(final, control=completed.control)
    elif damage == "timestamp_absent":
        cursor.recorded = []
    elif damage == "timestamp_naive":
        cursor.recorded = [(RECORDED.replace(tzinfo=None),)]
    elif damage == "timestamp_text":
        cursor.recorded = [(RECORDED.isoformat(),)]
    else:
        cursor.after_timestamp = lambda: setattr(owner.cursor_value, "lock_owned", False)
    writes = list(owner.dml_attempts)
    with pytest.raises(StateError):
        operation.completed_runner_receipt(final, OPERATION)
    assert owner.dml_attempts == writes


@pytest.mark.parametrize("resumed", [False, True])
@pytest.mark.parametrize("blocked", [False, True])
@pytest.mark.parametrize("damage", [None, "missing_history", "postimage", "incomplete", "failed"])
def test_postgres_pending_gate_validates_exact_full_history_without_dml(monkeypatch, resumed, blocked, damage):
    operation, database, owner, _driver = _postgres(monkeypatch)
    snapshot = _complete(operation, resumed=resumed, blocked=blocked, prefix=4 if damage == "incomplete" else 5, success=damage != "failed")
    if damage == "missing_history":
        database.operation_history.pop(0)
    elif damage == "postimage":
        database.state = _desired_state()
        database.state_revision += 1
        snapshot = operation.observe()
    writes = list(owner.dml_attempts)
    if damage is None:
        assert operation.validate_completed_runner_snapshot(snapshot) is None
    else:
        with pytest.raises(StateError):
            operation.validate_completed_runner_snapshot(snapshot)
    assert owner.dml_attempts == writes
