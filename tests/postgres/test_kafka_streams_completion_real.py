"""Real PostgreSQL storage finalization of a typed completed runner journal.

Kafka/Docker completion observations are typed fixture evidence, not live
provider observations. These tests prove PostgreSQL atomicity and retained
history; they do not authorize runtime work or a public replacement command.
"""

from __future__ import annotations

import copy
import json
import uuid
from dataclasses import replace

import pytest

import streamt.deployer.postgres_state_backend as postgres_backend_module
from streamt.deployer.postgres_state import _PsycopgBundle
from streamt.deployer.postgres_state_backend import PrivatePostgresStateReadBackend
from streamt.deployer.state import LocalState, ManagedResourceRecord, StateError
from streamt.deployer.state_backend import (
    OperationAction,
    OperationIntent,
    OperationProgress,
    OperationSnapshot,
    RecoveryRecord,
    StateBackendConflictError,
    StateBackendLockLostError,
    StateBackendReleaseAfterCommitError,
    StateStoreIdentity,
    operation_timestamp,
    state_checksum,
)
from tests.postgres.conftest import PostgresCase, WriterIdentity
from tests.postgres.test_kafka_streams_resume_real import (
    ADDRESS,
    RESOURCE,
    _backend,
    _boundaries,
    _durable_rows,
    _FirstConnectionCommitAckLossDriver,
    _history,
    _initialize,
    _intent,
    _interrupt,
    _resume_record,
    _revalidate,
)

pytestmark = [pytest.mark.integration, pytest.mark.postgres]

UNRELATED = "streamt://payments/prod/topic/unrelated"


class _DmlSpyCursor:
    """Observe real SQL writes, including attempts that would roll back."""

    def __init__(self, cursor: object, writes: list[str]) -> None:
        self.cursor = cursor
        self.writes = writes

    def execute(self, query: object, params: object = None) -> object:
        rendered = query.as_string() if hasattr(query, "as_string") else str(query)
        statement = rendered.lstrip().split(maxsplit=1)[0].upper()
        if statement in {"INSERT", "UPDATE", "DELETE", "MERGE"}:
            self.writes.append(statement)
        return self.cursor.execute(query, params)

    def fetchall(self) -> object:
        return self.cursor.fetchall()

    def close(self) -> None:
        self.cursor.close()


def _assert_rejected_without_dml(
    backend: PrivatePostgresStateReadBackend, snapshot: OperationSnapshot,
    *, error: type[StateError] = StateError,
) -> None:
    writes: list[str] = []
    with backend.operation(ADDRESS) as operation:
        operation._cursor = _DmlSpyCursor(operation._cursor, writes)
        with pytest.raises(error):
            operation.finalize_completed_runner(snapshot)
    assert writes == []


def _initialize_with_unrelated_owner(
    case: PostgresCase, writer: WriterIdentity,
) -> tuple[PrivatePostgresStateReadBackend, str]:
    backend, store_id = _initialize(case, writer)
    with backend.operation(ADDRESS) as operation:
        snapshot = operation.observe()
        intent = OperationIntent(
            str(uuid.uuid4()), "adopt", operation_timestamp(), "postgres-completion-fixture",
            snapshot.state.state_serial, state_checksum(snapshot.state.state), None, (),
        )
        replacement = copy.deepcopy(snapshot.state.state)
        replacement.serial += 1
        replacement.resources[UNRELATED] = ManagedResourceRecord(
            "unrelated.topic", "managed", "sha256:" + "7" * 64, "direct-kafka",
        )
        operation.commit_operation(operation.begin_operation(snapshot, intent), replacement)
    return backend, store_id


def _completed(
    backend: PrivatePostgresStateReadBackend, *, interrupted: bool = False,
    resumed: bool = False, mode: str = "success",
) -> OperationSnapshot:
    with backend.operation(ADDRESS) as operation:
        observed = operation.observe()
        intent = _intent(observed)
        if mode == "mixed":
            intent = replace(intent, actions=(*intent.actions, OperationAction(1, UNRELATED, "create")))
        current = operation.begin_operation(observed, intent)
        boundaries = _boundaries(intent, succeeded=mode != "failed")
        for index, boundary in enumerate(boundaries[:4] if mode == "incomplete" else boundaries):
            current = operation.record_progress(current, boundary)
            if resumed and index == 1:
                current = _interrupt(operation, current)
                current = operation.resume_operation(current, _resume_record(current))
        if mode == "mixed":
            for phase, succeeded in (("started", None), ("completed", True)):
                current = operation.record_progress(current, OperationProgress(
                    intent.operation_id, 1, UNRELATED, "create", phase, succeeded, operation_timestamp(),
                ))
        if interrupted:
            completed = [
                item.action_index for item in current.control.control.progress
                if item.status == "completed" and item.succeeded is True
            ]
            current = operation.mark_recovery_required(current, RecoveryRecord(
                intent.operation_id, "runtime_outcome_unknown", operation_timestamp(),
                max(completed) if completed else None,
            ))
        return current


def _expected_state(snapshot: OperationSnapshot) -> LocalState:
    intent = snapshot.control.control.intent
    assert intent is not None
    evidence = intent.actions[0].kafka_streams_evidence
    assert evidence is not None
    result = copy.deepcopy(snapshot.state.state)
    result.serial += 1
    result.resources[RESOURCE] = replace(
        result.resources[RESOURCE], artifact_checksum=evidence.desired_artifact.checksum,
    )
    return result


def _assert_atomic_result(
    case: PostgresCase, before: dict[str, list[tuple[object, ...]]],
    original: OperationSnapshot, result: OperationSnapshot,
) -> None:
    intent = original.control.control.intent
    assert intent is not None
    assert result.state.state == _expected_state(original)
    assert result.state.state.resources[UNRELATED] == original.state.state.resources[UNRELATED]
    assert result.control.control.status == "clear"
    after = _durable_rows(case)
    assert after["current_state"][0][0] == before["current_state"][0][0] + 1
    assert after["current_state"][0][1] == before["current_state"][0][1] + 1
    assert after["operation_control"][0][0] == before["operation_control"][0][0] + 1
    assert after["state_history"][:-1] == before["state_history"]
    assert len(after["state_history"]) == len(before["state_history"]) + 1
    assert after["state_history"][-1][-1] == intent.operation_id
    before_history = [row[1:] for row in before["operation_history"] if row[0] == intent.operation_id]
    after_history = _history(case, intent)
    assert after_history[:-1] == before_history
    assert after_history[-1][:2] == (len(before_history), "succeeded")
    assert json.loads(after_history[-2][2]) == original.control.control.to_dict()
    assert [row for row in after["operation_history"] if row[0] != intent.operation_id] == [
        row for row in before["operation_history"] if row[0] != intent.operation_id
    ]


@pytest.mark.parametrize("interrupted", [False, True])
@pytest.mark.parametrize("resumed", [False, True])
def test_real_postgres_finalizes_completed_runner_on_fresh_connection_with_incident_intact(
    postgres_case: PostgresCase, postgres_writer: WriterIdentity, interrupted: bool, resumed: bool,
) -> None:
    backend, store_id = _initialize_with_unrelated_owner(postgres_case, postgres_writer)
    original = _completed(backend, interrupted=interrupted, resumed=resumed)
    before = _durable_rows(postgres_case)
    with _backend(postgres_case, postgres_writer).operation(ADDRESS) as operation:
        observed = operation.observe()
        assert observed == original
        result = operation.finalize_completed_runner(observed)
    _assert_atomic_result(postgres_case, before, original, result)
    assert original.control.control.progress[1].kafka_streams_checkpoint.exit_code == 143
    assert original.control.control.control_version == (5 if resumed else 4)
    _revalidate(postgres_case, postgres_writer, store_id)
    final_rows = _durable_rows(postgres_case)
    with backend.operation(ADDRESS) as operation:
        observed = operation.observe()
        operation.ensure_ready(observed)
        with pytest.raises(StateError):
            operation.finalize_completed_runner(observed)
    assert _durable_rows(postgres_case) == final_rows


@pytest.mark.parametrize("interrupted", [False, True])
@pytest.mark.parametrize("commit_on_server", [False, True])
def test_real_postgres_finalizer_commit_ack_loss_is_atomic_and_never_replayed(
    postgres_case: PostgresCase, postgres_writer: WriterIdentity,
    monkeypatch: pytest.MonkeyPatch, interrupted: bool, commit_on_server: bool,
) -> None:
    backend, store_id = _initialize_with_unrelated_owner(postgres_case, postgres_writer)
    original = _completed(backend, interrupted=interrupted, resumed=True)
    before = _durable_rows(postgres_case)
    driver = _FirstConnectionCommitAckLossDriver(postgres_case.psycopg, commit_on_server=commit_on_server)
    expected_error = StateBackendReleaseAfterCommitError if commit_on_server else StateBackendLockLostError
    with monkeypatch.context() as patch:
        patch.setattr(postgres_backend_module, "_load_psycopg", lambda: _PsycopgBundle(driver=driver, sql=postgres_case.sql))
        with pytest.raises(expected_error) as raised, backend.operation(ADDRESS) as operation:
            operation.finalize_completed_runner(operation.observe())
    assert raised.value.operation_id == original.control.control.intent.operation_id
    assert driver.first is not None
    assert driver.first.commits == 1
    assert driver.connections >= 2
    with _backend(postgres_case, postgres_writer).operation(ADDRESS) as operation:
        observed = operation.observe()
        if commit_on_server:
            result = observed
            assert observed.control.control.status == "clear"
            after_lost_ack = _durable_rows(postgres_case)
            with pytest.raises(StateError):
                operation.finalize_completed_runner(observed)
            assert _durable_rows(postgres_case) == after_lost_ack
        else:
            assert observed == original
            assert _durable_rows(postgres_case) == before
            result = operation.finalize_completed_runner(observed)
    _assert_atomic_result(postgres_case, before, original, result)
    _revalidate(postgres_case, postgres_writer, store_id)


@pytest.mark.parametrize("mode", ["failed", "incomplete", "mixed"])
@pytest.mark.parametrize("interrupted", [False, True])
def test_real_postgres_finalizer_rejects_nonsole_or_unsuccessful_completion_without_mutation(
    postgres_case: PostgresCase, postgres_writer: WriterIdentity, mode: str, interrupted: bool,
) -> None:
    backend, _store_id = _initialize_with_unrelated_owner(postgres_case, postgres_writer)
    original = _completed(backend, interrupted=interrupted, mode=mode)
    before = _durable_rows(postgres_case)
    _assert_rejected_without_dml(backend, original)
    assert _durable_rows(postgres_case) == before


@pytest.mark.parametrize("interrupted", [False, True])
def test_real_postgres_finalizer_rejects_corrupt_current_journal_without_mutation(
    postgres_case: PostgresCase, postgres_writer: WriterIdentity, interrupted: bool,
) -> None:
    backend, _store_id = _initialize_with_unrelated_owner(postgres_case, postgres_writer)
    original = _completed(backend, interrupted=interrupted, resumed=True)
    intent = original.control.control.intent
    assert intent is not None
    with postgres_case.psycopg.connect(postgres_case.owner_dsn) as connection:
        connection.execute(postgres_case.sql.SQL(
            "DELETE FROM {}.{} WHERE operation_id = %s AND event_kind = 'progress_completed'",
        ).format(
            postgres_case.sql.Identifier(postgres_case.schema),
            postgres_case.sql.Identifier("operation_history"),
        ), (intent.operation_id,))
    corrupted = _durable_rows(postgres_case)
    _assert_rejected_without_dml(backend, original)
    assert _durable_rows(postgres_case) == corrupted


@pytest.mark.parametrize("damage", ["written_result", "unrelated_ownership", "wrong_store"])
def test_real_postgres_finalizer_requires_exact_atomic_preimage(
    postgres_case: PostgresCase, postgres_writer: WriterIdentity, damage: str,
) -> None:
    backend, _store_id = _initialize_with_unrelated_owner(postgres_case, postgres_writer)
    original = _completed(backend)
    if damage == "wrong_store":
        supplied = replace(original, state=replace(
            original.state, store=StateStoreIdentity("postgres", str(uuid.uuid4())),
        ))
    else:
        changed = _expected_state(original) if damage == "written_result" else copy.deepcopy(original.state.state)
        if damage == "unrelated_ownership":
            changed.resources[UNRELATED] = replace(changed.resources[UNRELATED], physical_name="other.topic")
            assert changed.resources[RESOURCE] == original.state.state.resources[RESOURCE]
        with postgres_case.psycopg.connect(postgres_case.owner_dsn) as connection:
            connection.execute(postgres_case.sql.SQL(
                "UPDATE {}.{} SET revision = revision + %s, state_serial = %s, state_checksum = %s, state_json = %s",
            ).format(
                postgres_case.sql.Identifier(postgres_case.schema),
                postgres_case.sql.Identifier("current_state"),
            ), (int(damage == "written_result"), changed.serial, state_checksum(changed), json.dumps(changed.to_dict(), sort_keys=True, separators=(",", ":"))))
        with backend.operation(ADDRESS) as operation:
            supplied = operation.observe()
            assert supplied.state.state == changed
    before = _durable_rows(postgres_case)
    _assert_rejected_without_dml(backend, supplied, error=StateBackendConflictError)
    assert _durable_rows(postgres_case) == before


def test_real_postgres_finalizer_cannot_write_after_loss_of_exact_lock_session(
    postgres_case: PostgresCase, postgres_writer: WriterIdentity,
) -> None:
    backend, _store_id = _initialize_with_unrelated_owner(postgres_case, postgres_writer)
    original = _completed(backend, interrupted=True)
    before = _durable_rows(postgres_case)
    writes: list[str] = []

    def finalize_after_loss() -> None:
        with backend.operation(ADDRESS) as operation:
            operation._cursor = _DmlSpyCursor(operation._cursor, writes)
            observed = operation.observe()
            assert observed == original
            with postgres_case.psycopg.connect(postgres_case.admin_dsn, autocommit=True) as connection:
                assert connection.execute(
                    "SELECT pg_catalog.pg_terminate_backend(%s)", (operation.backend_pid,),
                ).fetchone() == (True,)
            operation.finalize_completed_runner(observed)

    with pytest.raises(StateBackendLockLostError):
        finalize_after_loss()
    assert writes == []
    assert _durable_rows(postgres_case) == before
