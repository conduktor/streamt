"""Real-server conformance tests for PostgreSQL state mutation."""

from __future__ import annotations

import json
import multiprocessing
import os
import threading
import time
import uuid
from pathlib import Path

import pytest

from streamt.core.deployment_state import validate_deployment_state_config
from streamt.deployer.postgres_state import PostgresStateInitializer
from streamt.deployer.postgres_state_backend import PrivatePostgresStateReadBackend
from streamt.deployer.state import LocalState
from streamt.deployer.state_backend import (
    OperationAction,
    OperationControlState,
    OperationIntent,
    OperationProgress,
    OperationSnapshot,
    RecoveryRecord,
    StateAddress,
    StateBackendConflictError,
    StateBackendInvalidStateError,
    StateBackendLockLostError,
    StateBackendRecoveryRequiredError,
    StateBackendUnavailableError,
    make_deployment_state_service,
    operation_timestamp,
    state_checksum,
)

pytestmark = [pytest.mark.integration, pytest.mark.postgres]


def _address(*, project: str = "payments") -> StateAddress:
    return StateAddress(namespace="platform", project=project, environment="prod")


def _initializer(case: object) -> PostgresStateInitializer:
    return PostgresStateInitializer(
        dsn=case.owner_dsn,
        schema=case.schema,
        lock_timeout_seconds=10,
    )


def _backend(
    case: object,
    *,
    dsn: str | None = None,
    timeout: int = 5,
) -> PrivatePostgresStateReadBackend:
    return PrivatePostgresStateReadBackend(
        dsn=dsn or case.owner_dsn,
        schema=case.schema,
        lock_timeout_seconds=timeout,
    )


def _json(value: dict[str, object]) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _intent(
    snapshot: OperationSnapshot,
    *,
    operation_id: str | None = None,
    actions: bool = True,
) -> OperationIntent:
    return OperationIntent(
        operation_id=operation_id or str(uuid.uuid4()),
        kind="apply",
        started_at=operation_timestamp(),
        actor="postgres-real-conformance",
        prior_state_serial=snapshot.state.state_serial,
        prior_state_checksum=state_checksum(snapshot.state.state),
        reviewed_plan_checksum=None,
        actions=(
            (
                OperationAction(
                    index=0,
                    resource_id=("streamt://platform/payments/prod/kafka_topic/orders"),
                    action="create",
                ),
            )
            if actions
            else ()
        ),
    )


def _progress(intent: OperationIntent, *, status: str) -> OperationProgress:
    action = intent.actions[0]
    return OperationProgress(
        operation_id=intent.operation_id,
        action_index=action.index,
        resource_id=action.resource_id,
        action=action.action,
        status=status,
        succeeded=True if status == "completed" else None,
        recorded_at=operation_timestamp(),
    )


def _begin_and_complete(
    operation: object,
    snapshot: OperationSnapshot,
    *,
    operation_id: str | None = None,
) -> tuple[
    OperationSnapshot,
    OperationIntent,
]:
    intent = _intent(snapshot, operation_id=operation_id)
    current = operation.begin_operation(snapshot, intent)
    current = operation.record_progress(current, _progress(intent, status="started"))
    current = operation.record_progress(current, _progress(intent, status="completed"))
    return current, intent


def _table_rows(case: object, table: str, columns: str) -> list[tuple[object, ...]]:
    with case.psycopg.connect(case.owner_dsn) as connection:
        return list(
            connection.execute(
                case.sql.SQL("SELECT " + columns + " FROM {}.{} ORDER BY 1, 2, 3").format(
                    case.sql.Identifier(case.schema),
                    case.sql.Identifier(table),
                )
            ).fetchall()
        )


def _durable_rows(case: object) -> dict[str, list[tuple[object, ...]]]:
    return {
        "current_state": _table_rows(
            case,
            "current_state",
            "revision, state_serial, state_checksum, state_json",
        ),
        "operation_control": _table_rows(
            case,
            "operation_control",
            "revision, status, control_json",
        ),
        "state_history": _table_rows(
            case,
            "state_history",
            "revision, state_serial, state_checksum, state_json, operation_id::text",
        ),
        "operation_history": _table_rows(
            case,
            "operation_history",
            "operation_id::text, event_index, event_kind, control_json",
        ),
    }


def _lock_key(case: object, address: StateAddress) -> int:
    with case.psycopg.connect(case.owner_dsn) as connection:
        row = connection.execute(
            case.sql.SQL(
                "SELECT advisory_lock_key FROM {}.{} WHERE namespace = %s "
                "AND project = %s AND environment = %s"
            ).format(
                case.sql.Identifier(case.schema),
                case.sql.Identifier("state_addresses"),
            ),
            (address.namespace, address.project, address.environment),
        ).fetchone()
    assert row is not None
    assert type(row[0]) is int
    return row[0]


def _assert_lock_available(case: object, address: StateAddress) -> None:
    with case.psycopg.connect(case.owner_dsn, autocommit=True) as connection:
        acquired = connection.execute(
            "SELECT pg_catalog.pg_try_advisory_lock(%s)",
            (_lock_key(case, address),),
        ).fetchone()
        assert acquired == (True,)
        released = connection.execute(
            "SELECT pg_catalog.pg_advisory_unlock(%s)",
            (_lock_key(case, address),),
        ).fetchone()
        assert released == (True,)


def _assert_successor_blocked(case: object, address: StateAddress) -> None:
    backend = _backend(case)
    with backend.operation(address) as successor:
        observed = successor.observe()
        with pytest.raises(StateBackendRecoveryRequiredError):
            successor.ensure_ready(observed)


def _grant_reader(case: object) -> None:
    with case.psycopg.connect(case.admin_dsn, autocommit=True) as connection:
        connection.execute(
            case.sql.SQL("GRANT USAGE ON SCHEMA {} TO {}").format(
                case.sql.Identifier(case.schema),
                case.sql.Identifier(case.reader_role),
            )
        )
        connection.execute(
            case.sql.SQL("GRANT SELECT ON ALL TABLES IN SCHEMA {} TO {}").format(
                case.sql.Identifier(case.schema),
                case.sql.Identifier(case.reader_role),
            )
        )


def _begin_then_exit(dsn: str, schema: str, sender: object) -> None:
    address = _address()
    try:
        backend = PrivatePostgresStateReadBackend(
            dsn=dsn,
            schema=schema,
            lock_timeout_seconds=10,
        )
        with backend.operation(address) as operation:
            snapshot = operation.observe()
            intent = _intent(snapshot)
            operation.begin_operation(snapshot, intent)
            sender.send(("begun", operation.backend_pid, intent.operation_id))
            sender.close()
            os._exit(0)
    except BaseException as error:
        sender.send(("error", type(error).__name__, str(error)))
        sender.close()


def _terminate_backend(case: object, backend_pid: int) -> None:
    with case.psycopg.connect(case.admin_dsn, autocommit=True) as connection:
        terminated = connection.execute(
            "SELECT pg_catalog.pg_terminate_backend(%s)",
            (backend_pid,),
        ).fetchone()
        assert terminated == (True,)
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            active = connection.execute(
                "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_stat_activity WHERE pid = %s)",
                (backend_pid,),
            ).fetchone()
            if active == (False,):
                return
            time.sleep(0.05)
    pytest.fail("terminated PostgreSQL state session remained active")


def _terminate_after_started_progress(case: object, operation_id: str) -> None:
    address = _address()
    with _backend(case).operation(address) as operation:
        snapshot = operation.observe()
        intent = _intent(snapshot, operation_id=operation_id)
        active = operation.begin_operation(snapshot, intent)
        operation.record_progress(active, _progress(intent, status="started"))
        _terminate_backend(case, operation.backend_pid)
        operation.check_lock()


def _kill_writer_at_terminal_insert(
    case: object,
    *,
    address: StateAddress,
    operation_id: str,
    replacement: LocalState,
) -> None:
    failure: list[BaseException] = []
    with _backend(case).operation(address) as operation:
        completed, intent = _begin_and_complete(
            operation,
            operation.observe(),
            operation_id=operation_id,
        )
        clear_json = _json(OperationControlState.clear(address).to_dict())

        with case.psycopg.connect(case.owner_dsn) as blocker:
            blocker.execute(
                case.sql.SQL(
                    "INSERT INTO {}.{} (namespace, project, environment, "
                    "operation_id, event_index, event_kind, control_json, recorded_at) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s, "
                    "pg_catalog.clock_timestamp())"
                ).format(
                    case.sql.Identifier(case.schema),
                    case.sql.Identifier("operation_history"),
                ),
                (
                    address.namespace,
                    address.project,
                    address.environment,
                    intent.operation_id,
                    3,
                    "succeeded",
                    clear_json,
                ),
            )

            def finalize() -> None:
                try:
                    operation.commit_operation(completed, replacement)
                except BaseException as error:
                    failure.append(error)

            writer = threading.Thread(target=finalize, daemon=True)
            writer.start()
            with case.psycopg.connect(case.admin_dsn, autocommit=True) as observer:
                deadline = time.monotonic() + 15
                while time.monotonic() < deadline:
                    activity = observer.execute(
                        "SELECT wait_event_type, query FROM pg_catalog.pg_stat_activity "
                        "WHERE pid = %s",
                        (operation.backend_pid,),
                    ).fetchone()
                    if (
                        activity is not None
                        and activity[0] == "Lock"
                        and "operation_history" in activity[1]
                    ):
                        break
                    time.sleep(0.05)
                else:
                    pytest.fail("finalization did not reach the blocked terminal insert")
            _terminate_backend(case, operation.backend_pid)
            writer.join(timeout=15)
            assert writer.is_alive() is False
            assert len(failure) == 1
            assert isinstance(failure[0], StateBackendLockLostError)
            blocker.rollback()

        raise failure[0]


def test_changed_then_unchanged_lifecycles_cover_all_durable_tables(
    postgres_case: object,
) -> None:
    address = _address()
    _initializer(postgres_case).initialize(address)
    backend = _backend(postgres_case)
    replacement = LocalState(project="payments", environment="prod", serial=1)

    with backend.operation(address) as operation:
        completed, first_intent = _begin_and_complete(operation, operation.observe())
        committed = operation.commit_operation(completed, replacement)
        assert committed.state.state == replacement
        assert committed.control.control.status == "clear"

    with backend.operation(address) as operation:
        observed = operation.observe()
        second_intent = _intent(observed, actions=False)
        active = operation.begin_operation(observed, second_intent)
        committed = operation.commit_operation(active, None)
        assert committed.state.state == replacement
        assert committed.control.control.status == "clear"

    rows = _durable_rows(postgres_case)
    expected_state_json = _json(replacement.to_dict())
    assert rows["current_state"] == [(1, 1, state_checksum(replacement), expected_state_json)]
    assert rows["operation_control"] == [
        (6, "clear", _json(OperationControlState.clear(address).to_dict()))
    ]
    assert rows["state_history"] == [
        (
            1,
            1,
            state_checksum(replacement),
            expected_state_json,
            first_intent.operation_id,
        )
    ]
    history = rows["operation_history"]
    history_events: dict[str, list[tuple[int, str]]] = {}
    for operation_id, event_index, event_kind, _payload in history:
        history_events.setdefault(operation_id, []).append((event_index, event_kind))
    assert history_events == {
        first_intent.operation_id: [
            (0, "intent"),
            (1, "progress_started"),
            (2, "progress_completed"),
            (3, "succeeded"),
        ],
        second_intent.operation_id: [(0, "intent"), (1, "succeeded")],
    }
    for _operation_id, _event_index, _event_kind, payload in history:
        assert _json(json.loads(payload)) == payload
    _assert_lock_available(postgres_case, address)


def test_process_exit_after_first_begin_preserves_blocker_and_releases_lock(
    postgres_case: object,
) -> None:
    address = _address()
    _initializer(postgres_case).initialize(address)
    context = multiprocessing.get_context("spawn")
    receiver, sender = context.Pipe(duplex=False)
    process = context.Process(
        target=_begin_then_exit,
        args=(postgres_case.owner_dsn, postgres_case.schema, sender),
    )
    try:
        process.start()
        sender.close()
        assert receiver.poll(30), "mutation child did not report its durable begin"
        outcome = receiver.recv()
        process.join(timeout=15)
        assert outcome[0] == "begun", outcome
        assert process.exitcode == 0
    finally:
        if process.is_alive():
            process.terminate()
        process.join(timeout=5)
        receiver.close()
        sender.close()

    operation_id = outcome[2]
    snapshot = _backend(postgres_case).read_snapshot(address)
    rows = _durable_rows(postgres_case)
    assert snapshot.state.revision.is_absent
    assert snapshot.control.control.status == "in_progress"
    assert snapshot.control.control.intent is not None
    assert snapshot.control.control.intent.operation_id == operation_id
    assert rows["current_state"] == []
    assert rows["state_history"] == []
    assert [(row[0], row[1], row[2]) for row in rows["operation_history"]] == [
        (operation_id, 0, "intent")
    ]
    _assert_lock_available(postgres_case, address)
    _assert_successor_blocked(postgres_case, address)


def test_terminated_session_after_started_progress_preserves_recovery_evidence(
    postgres_case: object,
) -> None:
    address = _address()
    _initializer(postgres_case).initialize(address)
    operation_id = str(uuid.uuid4())

    with pytest.raises(StateBackendLockLostError):
        _terminate_after_started_progress(postgres_case, operation_id)

    snapshot = _backend(postgres_case).read_snapshot(address)
    rows = _durable_rows(postgres_case)
    assert snapshot.state.revision.is_absent
    assert snapshot.control.control.status == "in_progress"
    assert [(item.status, item.succeeded) for item in snapshot.control.control.progress] == [
        ("started", None)
    ]
    assert [(row[0], row[1], row[2]) for row in rows["operation_history"]] == [
        (operation_id, 0, "intent"),
        (operation_id, 1, "progress_started"),
    ]
    _assert_lock_available(postgres_case, address)
    _assert_successor_blocked(postgres_case, address)


def test_backend_termination_at_terminal_insert_rolls_back_entire_finalization(
    postgres_case: object,
) -> None:
    address = _address()
    _initializer(postgres_case).initialize(address)
    replacement = LocalState(project="payments", environment="prod", serial=1)
    operation_id = str(uuid.uuid4())

    with pytest.raises(StateBackendLockLostError):
        _kill_writer_at_terminal_insert(
            postgres_case,
            address=address,
            operation_id=operation_id,
            replacement=replacement,
        )

    snapshot = _backend(postgres_case).read_snapshot(address)
    rows = _durable_rows(postgres_case)
    assert snapshot.state.revision.is_absent
    assert snapshot.control.control.status == "in_progress"
    assert rows["current_state"] == []
    assert rows["state_history"] == []
    assert [(row[0], row[1], row[2]) for row in rows["operation_history"]] == [
        (operation_id, 0, "intent"),
        (operation_id, 1, "progress_started"),
        (operation_id, 2, "progress_completed"),
    ]
    assert rows["operation_control"][0][0:2] == (3, "in_progress")
    _assert_lock_available(postgres_case, address)
    _assert_successor_blocked(postgres_case, address)


def test_stale_state_and_control_snapshots_fail_before_history_dml(
    postgres_case: object,
) -> None:
    initializer = _initializer(postgres_case)
    control_address = _address(project="control-stale")
    state_address = _address(project="state-stale")
    initializer.initialize(control_address)
    initializer.initialize(state_address)

    with _backend(postgres_case).operation(control_address) as operation:
        stale = operation.observe()
        with postgres_case.psycopg.connect(postgres_case.owner_dsn) as connection:
            connection.execute(
                postgres_case.sql.SQL(
                    "UPDATE {}.{} SET revision = revision + 1, "
                    "updated_at = pg_catalog.clock_timestamp() WHERE namespace = %s "
                    "AND project = %s AND environment = %s"
                ).format(
                    postgres_case.sql.Identifier(postgres_case.schema),
                    postgres_case.sql.Identifier("operation_control"),
                ),
                (
                    control_address.namespace,
                    control_address.project,
                    control_address.environment,
                ),
            )
        with pytest.raises(StateBackendConflictError):
            operation.begin_operation(stale, _intent(stale, actions=False))

    inserted = LocalState(project=state_address.project, environment="prod", serial=1)
    with _backend(postgres_case).operation(state_address) as operation:
        stale = operation.observe()
        with postgres_case.psycopg.connect(postgres_case.owner_dsn) as connection:
            connection.execute(
                postgres_case.sql.SQL(
                    "INSERT INTO {}.{} (namespace, project, environment, revision, "
                    "state_serial, state_checksum, state_json, updated_at) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s, "
                    "pg_catalog.clock_timestamp())"
                ).format(
                    postgres_case.sql.Identifier(postgres_case.schema),
                    postgres_case.sql.Identifier("current_state"),
                ),
                (
                    state_address.namespace,
                    state_address.project,
                    state_address.environment,
                    1,
                    1,
                    state_checksum(inserted),
                    _json(inserted.to_dict()),
                ),
            )
        with pytest.raises(StateBackendConflictError):
            operation.begin_operation(stale, _intent(stale, actions=False))

    rows = _table_rows(
        postgres_case,
        "operation_history",
        "operation_id::text, event_index, event_kind",
    )
    assert rows == []


def test_clear_and_recovery_transitions_update_control_and_history_atomically(
    postgres_case: object,
) -> None:
    initializer = _initializer(postgres_case)
    clear_address = _address(project="clear-atomic")
    recovery_address = _address(project="recovery-atomic")
    initializer.initialize(clear_address)
    initializer.initialize(recovery_address)

    with _backend(postgres_case).operation(clear_address) as operation:
        observed = operation.observe()
        clear_intent = _intent(observed, actions=False)
        active = operation.begin_operation(observed, clear_intent)
        cleared = operation.clear_before_mutation(active)
        assert cleared.control.control.status == "clear"

    with _backend(postgres_case).operation(recovery_address) as operation:
        observed = operation.observe()
        recovery_intent = _intent(observed)
        active = operation.begin_operation(observed, recovery_intent)
        started = operation.record_progress(
            active,
            _progress(recovery_intent, status="started"),
        )
        recovery = RecoveryRecord(
            operation_id=recovery_intent.operation_id,
            failure_code="runtime_outcome_unknown",
            failed_at=operation_timestamp(),
            last_completed_action_index=None,
        )
        marked = operation.mark_recovery_required(started, recovery)
        assert marked.control.control.status == "recovery_required"

    rows = _durable_rows(postgres_case)
    controls = {json.loads(row[2])["address"]: row for row in rows["operation_control"]}
    assert controls[clear_address.uri][0:2] == (2, "clear")
    assert controls[recovery_address.uri][0:2] == (3, "recovery_required")
    histories: dict[str, list[tuple[int, str]]] = {}
    for operation_id, event_index, event_kind, _payload in rows["operation_history"]:
        histories.setdefault(operation_id, []).append((event_index, event_kind))
    assert histories == {
        clear_intent.operation_id: [(0, "intent"), (1, "cleared_before_mutation")],
        recovery_intent.operation_id: [
            (0, "intent"),
            (1, "progress_started"),
            (2, "recovery_required"),
        ],
    }
    assert rows["current_state"] == []
    assert rows["state_history"] == []


def test_non_owner_mutation_is_denied_and_factory_rejects_v1_owner(
    postgres_case: object,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    address = _address()
    _initializer(postgres_case).initialize(address)
    _grant_reader(postgres_case)
    non_read_only_dsn = postgres_case.conninfo.make_conninfo(
        postgres_case.reader_dsn,
        options="-c default_transaction_read_only=off",
    )
    backend = _backend(postgres_case, dsn=non_read_only_dsn)

    with backend.operation(address) as operation:
        observed = operation.observe()
        with pytest.raises(StateBackendUnavailableError):
            operation.begin_operation(observed, _intent(observed, actions=False))

    rows = _durable_rows(postgres_case)
    assert rows["current_state"] == []
    assert rows["state_history"] == []
    assert rows["operation_history"] == []
    assert rows["operation_control"][0][0:2] == (0, "clear")

    monkeypatch.setenv("PRIVATE_POSTGRES_STATE_ADMIN_DSN", postgres_case.owner_dsn)
    monkeypatch.setenv("PRIVATE_POSTGRES_STATE_WRITER_DSN", postgres_case.owner_dsn)
    config = validate_deployment_state_config(
        {
            "backend": "postgres",
            "namespace": "platform",
            "postgres": {
                "dsn_env": "PRIVATE_POSTGRES_STATE_ADMIN_DSN",
                "writer_dsn_env": "PRIVATE_POSTGRES_STATE_WRITER_DSN",
                "schema": postgres_case.schema,
            },
        }
    )
    service = make_deployment_state_service(
        tmp_path,
        project="payments",
        environment="prod",
        config=config,
    )
    with pytest.raises(StateBackendInvalidStateError):
        service.read()


def test_waiter_observes_fresh_post_release_snapshot(postgres_case: object) -> None:
    address = _address()
    _initializer(postgres_case).initialize(address)
    replacement = LocalState(project="payments", environment="prod", serial=1)
    started = threading.Event()
    acquired = threading.Event()
    results: list[OperationSnapshot | BaseException] = []

    def wait_for_lock() -> None:
        started.set()
        try:
            with _backend(postgres_case, timeout=10).operation(address) as operation:
                results.append(operation.observe())
                acquired.set()
        except BaseException as error:
            results.append(error)

    with _backend(postgres_case).operation(address) as first:
        waiter = threading.Thread(target=wait_for_lock, daemon=True)
        waiter.start()
        assert started.wait(timeout=5)
        time.sleep(0.2)
        assert acquired.is_set() is False
        observed = first.observe()
        intent = _intent(observed, actions=False)
        active = first.begin_operation(observed, intent)
        first.commit_operation(active, replacement)

    waiter.join(timeout=15)
    assert waiter.is_alive() is False
    assert len(results) == 1
    assert isinstance(results[0], OperationSnapshot)
    assert results[0].state.state == replacement
    assert results[0].state.revision.value == "postgres-v1:1"
    assert results[0].control.control.status == "clear"
    assert results[0].control.revision.value == "postgres-v1:2"
