"""Same-operation resume is atomic, incident-preserving, and replayable on PG."""

from __future__ import annotations

import copy
import json
from dataclasses import replace

import pytest

import streamt.deployer.postgres_state_backend as postgres_backend
from streamt.deployer.kafka_streams_replacement_executor import (
    KafkaStreamsReplacementExecutor,
    ReplacementExecutionState,
)
from streamt.deployer.postgres_state import (
    PrivatePostgresStateV2Migrator,
    _operation_history_states,
    _validate_operation_history_states,
)
from streamt.deployer.state import StateError
from streamt.deployer.state_backend import (
    OperationControlState,
    OperationResumeRecord,
    RecoveryRecord,
    StateBackendConflictError,
    StateBackendInvalidStateError,
    StateBackendLockLostError,
    StateBackendRecoveryRequiredError,
    StateBackendUnavailableError,
    StateBackendUnknownCommitError,
    StateStoreIdentity,
    state_checksum,
)
from tests.unit.test_kafka_streams_operation_evidence import (
    OPERATION,
    RESOURCE,
    STAMP,
    _boundaries,
    _desired_state,
    _intent,
    _postgres,
    _state,
)
from tests.unit.test_kafka_streams_replacement_executor import World
from tests.unit.test_postgres_state_mutation import _json
from tests.unit.test_postgres_state_v2 import _Cursor, _FakeSql

RESUME = "00000000-0000-4000-8000-000000000010"
OTHER_RESUME = "00000000-0000-4000-8000-000000000011"
REVIEWED = "sha256:" + "5" * 64


def _start(monkeypatch, *, prefix=1, exit_code=0):
    operation, database, owner, driver = _postgres(monkeypatch)
    initial = operation.observe()
    intent = replace(_intent(initial.state.state), reviewed_plan_checksum=REVIEWED)
    active = operation.begin_operation(initial, intent)
    for boundary in _boundaries(exit_code=exit_code)[:prefix]:
        active = operation.record_progress(active, boundary)
    return operation, database, owner, driver, active


def _interrupt(operation, active, *, failure_code="interrupted"):
    return operation.mark_recovery_required(
        active, RecoveryRecord(OPERATION, failure_code, STAMP, None),
    )


def _authorization(snapshot, *, resume_id=RESUME):
    return OperationResumeRecord.create(
        snapshot, resume_id=resume_id, actor="explicit-operator", resumed_at=STAMP,
    )


def _events(database):
    return [
        (
            index, kind,
            OperationControlState.from_dict(json.loads(raw), expected_address=database.address),
        )
        for index, kind, raw in database.operation_history
    ]


@pytest.mark.parametrize("prefix", range(5))
@pytest.mark.parametrize("exit_code", [0, 143])
def test_resume_at_every_unfinished_boundary_preserves_exact_preimage(
    monkeypatch, prefix, exit_code,
):
    operation, database, owner, _driver, active = _start(
        monkeypatch, prefix=prefix, exit_code=exit_code,
    )
    blocked = _interrupt(operation, active)
    record = _authorization(blocked)
    old_history = copy.deepcopy(database.operation_history)
    old_state_history = copy.deepcopy(database.state_history)
    writes = len(owner.dml_attempts)

    resumed = operation.resume_operation(blocked, record)

    control = resumed.control.control
    assert control.control_version == 5
    assert control.intent == blocked.control.control.intent
    assert control.intent._wire_version == 4
    assert control.progress == blocked.control.control.progress
    assert control.status == "in_progress"
    assert control.recovery is None
    assert control.resume_history == (record,)
    assert record.recovery == blocked.control.control.recovery
    assert resumed.state == blocked.state
    assert database.state_revision == 1
    assert database.state_history == old_state_history
    assert database.operation_history[:-1] == old_history
    assert database.operation_history[-1][:2] == (prefix + 2, "operation_resumed")
    assert owner.dml_attempts[writes:] == ["update_operation_control", "insert_operation_history"]
    assert operation.last_operation_id == OPERATION
    assert not operation.finalized
    assert control.to_dict()["intent"] == blocked.control.control.to_dict()["intent"]
    assert control.to_dict()["progress"] == blocked.control.control.to_dict()["progress"]
    for boundary in _boundaries(exit_code=exit_code)[prefix:]:
        resumed = operation.record_progress(resumed, boundary)
        assert resumed.control.control.resume_history == (record,)
    operation.commit_operation(resumed, _desired_state())
    assert database.control.status == "clear"
    assert database.operation_history[-1][:2] == (8, "succeeded")
    assert database.state_history[-1][-1] == OPERATION
    _validate_operation_history_states(_events(database), address=database.address, operation_id=OPERATION)


def test_repeated_interruptions_at_same_and_later_prefix_preserve_every_incident(monkeypatch):
    operation, database, _owner, _driver, active = _start(monkeypatch)
    authorizations = []
    for index, prefix in enumerate((1, 1, 2, 3, 4)):
        while len(active.control.control.progress) < prefix:
            active = operation.record_progress(active, _boundaries()[len(active.control.control.progress)])
        blocked = _interrupt(operation, active, failure_code=f"interrupted_{index}")
        record = _authorization(blocked, resume_id=f"00000000-0000-4000-8000-{index + 32:012d}")
        authorizations.append(record)
        active = operation.resume_operation(blocked, record)
        assert active.control.control.resume_history == tuple(authorizations)
        assert [(kind, control) for _index, kind, control in _events(database)] == _operation_history_states(active.control.control)
    active = operation.record_progress(active, _boundaries()[-1])
    operation.commit_operation(active, _desired_state())
    events = _events(database)
    assert [index for index, _kind, _control in events] == list(range(17))
    assert [control.recovery.failure_code for _index, kind, control in events if kind == "recovery_required"] == [f"interrupted_{i}" for i in range(5)]
    assert _validate_operation_history_states(events, address=database.address, operation_id=OPERATION).resume_history == tuple(authorizations)


@pytest.mark.parametrize("failure", ["state", "state_revision", "store", "control_revision", "history", "lock"])
def test_resume_requires_exact_locked_store_state_control_and_history(monkeypatch, failure):
    operation, database, owner, _driver, active = _start(monkeypatch)
    blocked = _interrupt(operation, active)
    record = _authorization(blocked)
    if failure == "state":
        database.state.resources[RESOURCE] = replace(database.state.resources[RESOURCE], artifact_checksum="sha256:" + "9" * 64)
    elif failure == "state_revision":
        database.state_revision += 1
    elif failure == "store":
        database.store_id = "00000000-0000-4000-8000-000000000090"
    elif failure == "control_revision":
        database.control_revision += 1
    elif failure == "history":
        database.operation_history.pop()
    else:
        owner.cursor_value.lock_owned = False
    writes = list(owner.dml_attempts)
    with pytest.raises((StateBackendConflictError, StateBackendLockLostError)):
        operation.resume_operation(blocked, record)
    assert owner.dml_attempts == writes
    assert database.control.status == "recovery_required"


@pytest.mark.parametrize("table", ["operation_control", "operation_history"])
def test_resume_dml_failure_rolls_back_control_and_incident_together(monkeypatch, table):
    operation, database, owner, _driver, active = _start(monkeypatch, prefix=3)
    blocked = _interrupt(operation, active)
    history = copy.deepcopy(database.operation_history)
    revision = database.control_revision
    owner.fail_dml_pattern = f'"streamt"."{table}"'
    # SELECTs use the same table; constrain the injection to the mutation statement.
    owner.fail_dml_pattern = ("UPDATE " if table == "operation_control" else "INSERT INTO ") + owner.fail_dml_pattern
    with pytest.raises(StateBackendUnavailableError) as error:
        operation.resume_operation(blocked, _authorization(blocked))
    assert "secret" not in str(error.value)
    assert database.control == blocked.control.control
    assert database.control_revision == revision
    assert database.operation_history == history
    assert database.state == _state()
    assert len(database.state_history) == 1
    owner.fail_dml_pattern = None
    resumed = operation.resume_operation(blocked, _authorization(blocked))
    assert resumed.control.control.status == "in_progress"


@pytest.mark.parametrize("mode", ["reject", "apply_then_raise", "corrupt_history"])
def test_lost_resume_ack_never_retries_or_returns_runtime_authority(monkeypatch, mode):
    operation, database, owner, _driver, active = _start(monkeypatch, prefix=4)
    blocked = _interrupt(operation, active)
    record = _authorization(blocked)
    writes = len(owner.dml_attempts)
    history_length = len(database.operation_history)
    owner.commit_mode = mode
    expected = StateBackendUnknownCommitError if mode == "corrupt_history" else StateBackendLockLostError
    with pytest.raises(expected):
        operation.resume_operation(blocked, record)
    assert owner.dml_attempts[writes:] == ["update_operation_control", "insert_operation_history"]
    assert database.control.status == ("recovery_required" if mode == "reject" else "in_progress")
    assert len(database.operation_history) == history_length + (mode == "apply_then_raise")
    writes = list(owner.dml_attempts)
    with pytest.raises(StateBackendLockLostError):
        operation.resume_operation(blocked, record)
    assert owner.dml_attempts == writes
    assert database.state == _state()
    assert not operation.finalized


def test_resume_ack_preimage_not_trusted_while_writer_can_still_commit(monkeypatch):
    operation, database, owner, _driver, active = _start(monkeypatch)
    operation._lock_timeout_seconds = 0
    blocked = _interrupt(operation, active)
    owner.commit_mode = "reject"
    owner.close_releases_writer = False
    writes = len(owner.dml_attempts)
    with pytest.raises(StateBackendUnknownCommitError):
        operation.resume_operation(blocked, _authorization(blocked))
    assert owner.dml_attempts[writes:] == ["update_operation_control", "insert_operation_history"]
    assert database.control == blocked.control.control


def test_resume_reproves_v2_writer_before_transition_and_postimage(monkeypatch):
    operation, _database, _owner, _driver, active = _start(monkeypatch)
    blocked = _interrupt(operation, active)
    operation._require_v2_writer = True
    checks = []
    monkeypatch.setattr(postgres_backend, "_prove_private_postgres_v2_writer", lambda *_args, **kwargs: checks.append(kwargs["address"]))
    monkeypatch.setattr(postgres_backend, "_prove_mutation_authority", lambda *_args, **_kwargs: pytest.fail("v1 authority cannot authorize production resume"))
    operation.resume_operation(blocked, _authorization(blocked))
    assert checks == [blocked.address, blocked.address]


@pytest.mark.parametrize("backend", ["postgres", "local"])
def test_foreign_resume_authority_is_rejected_on_read_before_direct_sdk_progress(monkeypatch, backend):
    operation, database, owner, _driver, active = _start(monkeypatch)
    blocked = _interrupt(operation, active)
    operation.resume_operation(blocked, _authorization(blocked))
    foreign = replace(
        database.control.resume_history[0],
        store=StateStoreIdentity(backend, OTHER_RESUME),
    )
    database.control = replace(database.control, resume_history=(foreign,))
    index, kind, _raw = database.operation_history[-1]
    database.operation_history[-1] = (index, kind, _json(database.control.to_dict()))
    writes = list(owner.dml_attempts)
    # Even consistently transplanted control + history is not local authority.
    with pytest.raises(StateBackendInvalidStateError, match="deployment state is invalid"):
        operation.observe()
    assert owner.dml_attempts == writes


@pytest.mark.parametrize("prefix", [1, 2, 4])
@pytest.mark.parametrize("damage", ["missing", "truncated", "downgraded_control"])
def test_executor_rechecks_active_resume_history_before_any_provider_write(monkeypatch, prefix, damage):
    operation, database, owner, _driver, active = _start(monkeypatch, prefix=prefix, exit_code=143)
    blocked = _interrupt(operation, active)
    active = operation.resume_operation(blocked, _authorization(blocked))
    if damage == "missing":
        database.operation_history.clear()
    elif damage == "truncated":
        database.operation_history.pop()
    else:
        database.control = replace(database.control, control_version=4, resume_history=())
    events = []
    original_check_lock = postgres_backend._PostgresStateReadOperation.check_lock

    def check_lock(self):
        events.append("lock")
        original_check_lock(self)

    monkeypatch.setattr(postgres_backend._PostgresStateReadOperation, "check_lock", check_lock)
    world = World(events, prefix=prefix)
    holder = ReplacementExecutionState(active)
    writes = list(owner.dml_attempts)
    with pytest.raises(StateBackendInvalidStateError):
        KafkaStreamsReplacementExecutor(world.observer).run(
            operation, holder, operation_id=OPERATION, mode="resume",
        )
    assert world.commands == []
    assert world.creates == []
    assert world.read_count == 0
    assert owner.dml_attempts == writes
    assert holder.snapshot == active


@pytest.mark.parametrize("prefix", [0, 1, 2, 4])
def test_active_v4_runner_also_requires_complete_history_on_read(monkeypatch, prefix):
    operation, database, owner, _driver, active = _start(monkeypatch, prefix=prefix)
    assert active.control.control.control_version == 4
    database.operation_history.pop()
    writes = list(owner.dml_attempts)
    with pytest.raises(StateBackendInvalidStateError):
        operation.observe()
    assert owner.dml_attempts == writes


def test_resume_cannot_duplicate_authority_or_clear_before_mutation(monkeypatch):
    operation, database, owner, _driver, active = _start(monkeypatch, prefix=0)
    blocked = _interrupt(operation, active)
    record = _authorization(blocked)
    resumed = operation.resume_operation(blocked, record)
    writes = list(owner.dml_attempts)
    with pytest.raises(StateBackendConflictError):
        operation.resume_operation(blocked, record)
    with pytest.raises(StateBackendRecoveryRequiredError):
        operation.resume_operation(resumed, record)
    with pytest.raises(StateBackendRecoveryRequiredError):
        operation.clear_before_mutation(resumed)
    assert owner.dml_attempts == writes
    assert database.control.resume_history == (record,)


def test_prior_authorization_cannot_authorize_a_later_incident(monkeypatch):
    operation, database, owner, _driver, active = _start(monkeypatch)
    blocked = _interrupt(operation, active)
    record = _authorization(blocked)
    resumed = operation.resume_operation(blocked, record)
    later = _interrupt(operation, resumed, failure_code="interrupted_again")
    writes = list(owner.dml_attempts)
    with pytest.raises(StateBackendConflictError):
        operation.resume_operation(later, record)
    assert owner.dml_attempts == writes
    assert database.control.recovery.failure_code == "interrupted_again"
    assert database.control.resume_history == (record,)


def _completed_archive(monkeypatch, *, prefix=3, exit_code=0):
    operation, database, _owner, _driver, active = _start(monkeypatch, prefix=prefix, exit_code=exit_code)
    blocked = _interrupt(operation, active)
    active = operation.resume_operation(blocked, _authorization(blocked))
    for boundary in _boundaries(exit_code=exit_code)[prefix:]:
        active = operation.record_progress(active, boundary)
    operation.commit_operation(active, _desired_state())
    return database


def _restore_validator(database, *, schema_version=2):
    address = database.address
    address_fields = (address.namespace, address.project, address.environment)
    records = [(*address_fields, OPERATION, index, kind, raw, len(raw.encode())) for index, kind, raw in database.operation_history]

    def responder(query, _params):
        if '"operation_history"' in query:
            return records
        if '"state_history"' in query:
            return [(*address_fields, revision, serial, checksum, raw, operation_id, len(raw.encode())) for revision, serial, checksum, raw, operation_id in database.state_history]
        if '"current_state"' in query:
            raw = _json(database.state.to_dict())
            return [(*address_fields, database.state_revision, database.state.serial, state_checksum(database.state), raw, len(raw.encode()))]
        raise AssertionError(query)

    migrator = PrivatePostgresStateV2Migrator(dsn="dbname=unit", schema="streamt", lock_timeout_seconds=3, writer_role="writer")

    def validate():
        migrator._validate_all_durable_rows(_Cursor(responder), _FakeSql(), expected_store_id=database.store_id, source_schema_version=schema_version)

    return records, validate


@pytest.mark.parametrize("schema_version", [1, 2])
@pytest.mark.parametrize("prefix", range(5))
@pytest.mark.parametrize("exit_code", [0, 143])
def test_restore_accepts_v5_resume_without_sql_catalog_migration(monkeypatch, schema_version, prefix, exit_code):
    database = _completed_archive(monkeypatch, prefix=prefix, exit_code=exit_code)
    _records, validate = _restore_validator(database, schema_version=schema_version)
    validate()


@pytest.mark.parametrize("damage", ["drop_incident", "drop_resume", "duplicate_resume", "swap_resume_incident", "wrong_kind", "change_incident", "lose_audit", "foreign_store", "foreign_backend", "unknown_field", "terminal_before_resume", "not_completed", "wrong_operation"])
def test_restore_rejects_missing_reordered_or_forged_resume_history(monkeypatch, damage):
    database = _completed_archive(monkeypatch)
    records, validate = _restore_validator(database)
    validate()
    incident = next(i for i, row in enumerate(records) if row[5] == "recovery_required")
    resumed = incident + 1
    if damage == "drop_incident":
        records.pop(incident)
    elif damage == "drop_resume":
        records.pop(resumed)
    elif damage == "duplicate_resume":
        records.insert(resumed, records[resumed])
    elif damage == "swap_resume_incident":
        records[incident], records[resumed] = records[resumed], records[incident]
    elif damage == "wrong_kind":
        records[resumed] = (*records[resumed][:5], "progress_checkpoint", *records[resumed][6:])
    elif damage == "terminal_before_resume":
        records[incident] = (*records[incident][:5], "succeeded", *records[-1][6:])
    elif damage == "not_completed":
        records.pop(-2)
    elif damage == "wrong_operation":
        records[:] = [(*row[:3], OTHER_RESUME, *row[4:]) for row in records]
    else:
        for index, row in enumerate(records):
            raw = json.loads(row[-2])
            if damage == "change_incident" and index == incident:
                raw["recovery"]["failure_code"] = "different_incident"
            elif raw["control_version"] == 5:
                if damage == "lose_audit":
                    raw["resume_history"] = []
                elif damage == "foreign_store":
                    raw["resume_history"][0]["store"]["store_id"] = OTHER_RESUME
                elif damage == "foreign_backend":
                    raw["resume_history"][0]["store"]["backend"] = "local"
                elif damage == "unknown_field":
                    raw["resume_history"][0]["cleared"] = True
            changed = _json(raw)
            records[index] = (*row[:-2], changed, len(changed.encode()))
    # Reindex so structural attacks cannot be rejected only as an index gap.
    records[:] = [(*row[:4], index, *row[5:]) for index, row in enumerate(records)]
    with pytest.raises(StateBackendInvalidStateError):
        validate()


def test_failed_completion_after_resume_cannot_be_archived_as_success(monkeypatch):
    operation, database, _owner, _driver, active = _start(monkeypatch)
    blocked = _interrupt(operation, active)
    active = operation.resume_operation(blocked, _authorization(blocked))
    failed = replace(_boundaries()[-1], succeeded=False)
    active = operation.record_progress(active, failed)
    assert active.control.control.resume_history
    blocked = operation.mark_recovery_required(active, RecoveryRecord(OPERATION, "failed", STAMP, None))
    with pytest.raises(StateError):
        _authorization(blocked, resume_id=OTHER_RESUME)
    assert database.control.status == "recovery_required"


def test_missing_reviewed_plan_is_not_resumable_even_with_valid_runner_evidence(monkeypatch):
    operation, database, owner, _driver = _postgres(monkeypatch)
    initial = operation.observe()
    active = operation.begin_operation(initial, _intent(initial.state.state))
    blocked = _interrupt(operation, active)
    writes = list(owner.dml_attempts)
    with pytest.raises(StateError):
        _authorization(blocked)
    assert owner.dml_attempts == writes
    assert database.control.status == "recovery_required"
