"""Real read-only receipt lookup and pending-completion gates under writer locks."""

from __future__ import annotations

import uuid
from contextlib import nullcontext
from dataclasses import replace
from datetime import timezone

import pytest

from streamt.deployer.state import StateError
from streamt.deployer.state_backend import (
    OperationIntent,
    StateBackendLockLostError,
    StateStoreIdentity,
    operation_timestamp,
    state_checksum,
)
from tests.postgres.conftest import PostgresCase, WriterIdentity
from tests.postgres.test_kafka_streams_completion_real import (
    _completed,
    _DmlSpyCursor,
    _expected_state,
    _initialize_with_unrelated_owner,
)
from tests.postgres.test_kafka_streams_resume_real import ADDRESS, _backend, _durable_rows

pytestmark = [pytest.mark.integration, pytest.mark.postgres]


def _finish(case, writer, *, resumed=False, interrupted=False):
    backend, store_id = _initialize_with_unrelated_owner(case, writer)
    pending = _completed(backend, resumed=resumed, interrupted=interrupted)
    with backend.operation(ADDRESS) as operation:
        completed = operation.finalize_completed_runner(pending)
    return backend, store_id, pending, completed


@pytest.mark.parametrize("resumed", [False, True])
@pytest.mark.parametrize("interrupted", [False, True])
def test_real_receipt_new_connection_retains_exact_journal_timestamp_and_current_result(postgres_case: PostgresCase, postgres_writer: WriterIdentity, resumed, interrupted):
    case = postgres_case
    _original, _store_id, pending, _completed_snapshot = _finish(case, postgres_writer, resumed=resumed, interrupted=interrupted)
    intent = pending.control.control.intent
    before = _durable_rows(case)
    with case.psycopg.connect(case.admin_dsn, autocommit=True) as connection:
        recorded = connection.execute(case.sql.SQL(
            "SELECT recorded_at FROM {}.operation_history WHERE operation_id = %s AND event_kind = 'succeeded'"
        ).format(case.sql.Identifier(case.schema)), (intent.operation_id,)).fetchone()[0]
    backend = _backend(case, postgres_writer)
    writes = []
    with backend.operation(ADDRESS) as operation:
        snapshot = operation.observe()
        operation._cursor = _DmlSpyCursor(operation._cursor, writes)
        receipt = operation.completed_runner_receipt(snapshot, intent.operation_id)
        assert receipt.control == pending.control.control
        assert receipt.completed_at == recorded.astimezone(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")
        assert receipt.verify_result_state(snapshot.state.state) == pending.state.state
        assert snapshot.state.state == _expected_state(pending)
        assert operation.completed_runner_receipt(snapshot, intent.operation_id) == receipt
        assert operation.completed_runner_receipt(snapshot, str(uuid.uuid4())) is None
    assert writes == []
    assert _durable_rows(case) == before


@pytest.mark.parametrize("resumed", [False, True])
@pytest.mark.parametrize("interrupted", [False, True])
def test_real_pending_success_gate_has_no_dml_and_is_distinct_from_cleared_receipt(postgres_case: PostgresCase, postgres_writer: WriterIdentity, resumed, interrupted):
    backend, _store = _initialize_with_unrelated_owner(postgres_case, postgres_writer)
    pending = _completed(backend, resumed=resumed, interrupted=interrupted)
    before = _durable_rows(postgres_case)
    writes = []
    with backend.operation(ADDRESS) as operation:
        operation._cursor = _DmlSpyCursor(operation._cursor, writes)
        assert operation.validate_completed_runner_snapshot(pending) is None
        with pytest.raises(StateError):
            operation.completed_runner_receipt(pending, pending.control.control.intent.operation_id)
    assert writes == []
    assert _durable_rows(postgres_case) == before


@pytest.mark.parametrize("damage", ["missing_history", "missing_terminal", "missing_incident", "missing_state_history", "foreign_store", "later_state", "stale_snapshot", "killed_session"])
def test_real_receipt_rejects_invalid_or_historical_proof_without_dml(postgres_case: PostgresCase, postgres_writer: WriterIdentity, damage):
    case = postgres_case
    backend, _store, pending, completed = _finish(case, postgres_writer, resumed=True, interrupted=True)
    intent = pending.control.control.intent
    if damage in ("missing_history", "missing_terminal", "missing_incident", "missing_state_history"):
        with case.psycopg.connect(case.admin_dsn, autocommit=True) as connection:
            table = "state_history" if damage == "missing_state_history" else "operation_history"
            predicate = "operation_id = %s"
            if damage == "missing_terminal":
                predicate += " AND event_kind = 'succeeded'"
            elif damage == "missing_incident":
                predicate += " AND event_kind = 'recovery_required'"
            connection.execute(case.sql.SQL("DELETE FROM {}.{} WHERE " + predicate).format(
                case.sql.Identifier(case.schema), case.sql.Identifier(table),
            ), (intent.operation_id,))
    elif damage in ("later_state", "stale_snapshot"):
        with backend.operation(ADDRESS) as operation:
            initial = operation.observe()
            mutation = OperationIntent(str(uuid.uuid4()), "adopt", operation_timestamp(), "receipt-other-operation", initial.state.state_serial, state_checksum(initial.state.state), None, ())
            desired = replace(initial.state.state, serial=initial.state.state.serial + 1)
            operation.commit_operation(operation.begin_operation(initial, mutation), desired)
    before = _durable_rows(case)
    writes = []
    release = pytest.raises(StateBackendLockLostError) if damage == "killed_session" else nullcontext()
    with release, backend.operation(ADDRESS) as operation:
        snapshot = completed if damage == "stale_snapshot" else operation.observe()
        if damage == "foreign_store":
            snapshot = replace(snapshot, state=replace(snapshot.state, store=StateStoreIdentity("postgres", str(uuid.uuid4()))))
        operation._cursor = _DmlSpyCursor(operation._cursor, writes)
        if damage == "killed_session":
            with case.psycopg.connect(case.admin_dsn, autocommit=True) as connection:
                assert connection.execute("SELECT pg_terminate_backend(%s)", (operation.backend_pid,)).fetchone()[0] is True
        with pytest.raises(StateError):
            operation.completed_runner_receipt(snapshot, intent.operation_id)
    assert writes == []
    assert _durable_rows(case) == before


@pytest.mark.parametrize("mode", ["incomplete", "failed", "mixed"])
def test_real_pending_gate_rejects_non_successful_sole_runner_without_dml(postgres_case: PostgresCase, postgres_writer: WriterIdentity, mode):
    backend, _store = _initialize_with_unrelated_owner(postgres_case, postgres_writer)
    pending = _completed(backend, interrupted=True, mode=mode)
    before = _durable_rows(postgres_case)
    writes = []
    with backend.operation(ADDRESS) as operation:
        operation._cursor = _DmlSpyCursor(operation._cursor, writes)
        with pytest.raises(StateError):
            operation.validate_completed_runner_snapshot(pending)
    assert writes == []
    assert _durable_rows(postgres_case) == before
