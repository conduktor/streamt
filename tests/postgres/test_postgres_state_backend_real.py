"""Real PostgreSQL tests for the private v1 read/session-lock scaffold."""

from __future__ import annotations

import json
import time
import uuid

import pytest

from streamt.deployer.postgres_state import PostgresStateInitializer
from streamt.deployer.postgres_state_backend import PrivatePostgresStateReadBackend
from streamt.deployer.state import LocalState
from streamt.deployer.state_backend import (
    OperationControlState,
    OperationIntent,
    StateAddress,
    StateBackendLockLostError,
    StateBackendLockTimeoutError,
    state_checksum,
)

pytestmark = [pytest.mark.integration, pytest.mark.postgres]


def _address() -> StateAddress:
    return StateAddress(namespace="platform", project="payments", environment="prod")


def _initializer(case: object) -> PostgresStateInitializer:
    return PostgresStateInitializer(
        dsn=case.owner_dsn,
        schema=case.schema,
        lock_timeout_seconds=10,
    )


def _backend(case: object, *, timeout: int = 2) -> PrivatePostgresStateReadBackend:
    return PrivatePostgresStateReadBackend(
        dsn=case.owner_dsn,
        schema=case.schema,
        lock_timeout_seconds=timeout,
    )


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
    return row[0]


def _current_row_count(case: object) -> int:
    with case.psycopg.connect(case.owner_dsn) as connection:
        row = connection.execute(
            case.sql.SQL("SELECT count(*) FROM {}.{}").format(
                case.sql.Identifier(case.schema),
                case.sql.Identifier("current_state"),
            )
        ).fetchone()
    assert row is not None
    return row[0]


def _insert_state(case: object, address: StateAddress, state: LocalState) -> None:
    raw = json.dumps(
        state.to_dict(),
        sort_keys=True,
        ensure_ascii=False,
        separators=(",", ":"),
    )
    with case.psycopg.connect(case.owner_dsn) as connection:
        connection.execute(
            case.sql.SQL(
                "INSERT INTO {}.{} (namespace, project, environment, revision, "
                "state_serial, state_checksum, state_json, updated_at) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)"
            ).format(
                case.sql.Identifier(case.schema),
                case.sql.Identifier("current_state"),
            ),
            (
                address.namespace,
                address.project,
                address.environment,
                1,
                state.serial,
                state_checksum(state),
                raw,
            ),
        )


def _record_first_operation_intent(case: object, address: StateAddress) -> str:
    operation_id = str(uuid.uuid4())
    empty = LocalState(project=address.project, environment=address.environment)
    intent = OperationIntent(
        operation_id=operation_id,
        kind="apply",
        started_at="2026-09-02T00:00:00Z",
        actor="postgres-conformance",
        prior_state_serial=0,
        prior_state_checksum=state_checksum(empty),
        reviewed_plan_checksum=None,
        actions=(),
    )
    control = OperationControlState(
        address=address,
        status="in_progress",
        intent=intent,
    )
    raw = json.dumps(
        control.to_dict(),
        sort_keys=True,
        ensure_ascii=False,
        separators=(",", ":"),
    )
    with case.psycopg.connect(case.owner_dsn) as connection:
        connection.execute(
            case.sql.SQL(
                "UPDATE {}.{} SET revision = %s, status = %s, control_json = %s, "
                "updated_at = CURRENT_TIMESTAMP WHERE namespace = %s AND project = %s "
                "AND environment = %s"
            ).format(
                case.sql.Identifier(case.schema),
                case.sql.Identifier("operation_control"),
            ),
            (
                1,
                "in_progress",
                raw,
                address.namespace,
                address.project,
                address.environment,
            ),
        )
    return operation_id


def _assert_lock_is_available(case: object, lock_key: int) -> None:
    with case.psycopg.connect(case.owner_dsn, autocommit=True) as connection:
        acquired = connection.execute(
            "SELECT pg_catalog.pg_try_advisory_lock(%s)",
            (lock_key,),
        ).fetchone()
        assert acquired == (True,)
        released = connection.execute(
            "SELECT pg_catalog.pg_advisory_unlock(%s)",
            (lock_key,),
        ).fetchone()
        assert released == (True,)


def _terminate_locked_operation(case: object, address: StateAddress) -> None:
    with _backend(case).operation(address) as operation:
        with case.psycopg.connect(
            case.admin_dsn,
            autocommit=True,
        ) as connection:
            terminated = connection.execute(
                "SELECT pg_catalog.pg_terminate_backend(%s)",
                (operation.backend_pid,),
            ).fetchone()
            assert terminated == (True,)
            deadline = time.monotonic() + 10
            while time.monotonic() < deadline:
                active = connection.execute(
                    "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_stat_activity WHERE pid = %s)",
                    (operation.backend_pid,),
                ).fetchone()
                if active == (False,):
                    break
                time.sleep(0.05)
            else:
                pytest.fail("terminated PostgreSQL state session remained active")
        operation.check_lock()


def test_real_snapshot_reads_absent_and_present_without_dml(postgres_case: object) -> None:
    address = _address()
    initialized = _initializer(postgres_case).initialize(address)
    backend = _backend(postgres_case)

    absent = backend.read_snapshot(address)

    assert absent.state.store.store_id == initialized.store_id
    assert absent.state.revision.is_absent
    assert absent.state.state.serial == 0
    assert absent.control.control.status == "clear"
    assert absent.control.revision.value == "postgres-v1:0"
    assert _current_row_count(postgres_case) == 0

    expected = LocalState(project="payments", environment="prod", serial=1)
    _insert_state(postgres_case, address, expected)
    present = backend.read_snapshot(address)

    assert present.state.state == expected
    assert present.state.revision.value == "postgres-v1:1"
    assert present.control.revision.value == "postgres-v1:0"
    assert _current_row_count(postgres_case) == 1


def test_real_absent_state_with_active_control_remains_observable(
    postgres_case: object,
) -> None:
    address = _address()
    _initializer(postgres_case).initialize(address)
    operation_id = _record_first_operation_intent(postgres_case, address)

    snapshot = _backend(postgres_case).read_snapshot(address)

    assert snapshot.state.revision.is_absent
    assert snapshot.state.state.serial == 0
    assert snapshot.control.revision.value == "postgres-v1:1"
    assert snapshot.control.control.status == "in_progress"
    assert snapshot.control.control.intent is not None
    assert snapshot.control.control.intent.operation_id == operation_id
    assert _current_row_count(postgres_case) == 0


def test_real_operation_contends_times_out_and_releases_cleanly(
    postgres_case: object,
) -> None:
    address = _address()
    _initializer(postgres_case).initialize(address)
    lock_key = _lock_key(postgres_case, address)

    with _backend(postgres_case).operation(address) as first:
        first.check_lock()
        with (
            pytest.raises(StateBackendLockTimeoutError),
            _backend(postgres_case, timeout=0).operation(address),
        ):
            pytest.fail("a contended session lock must not be yielded")

    _assert_lock_is_available(postgres_case, lock_key)
    assert _current_row_count(postgres_case) == 0


def test_real_terminated_operation_session_fails_closed_and_server_releases_lock(
    postgres_case: object,
) -> None:
    address = _address()
    _initializer(postgres_case).initialize(address)
    lock_key = _lock_key(postgres_case, address)

    with pytest.raises(
        StateBackendLockLostError,
        match=r"^PostgreSQL deployment state operation lock was lost$",
    ):
        _terminate_locked_operation(postgres_case, address)

    _assert_lock_is_available(postgres_case, lock_key)
    assert _current_row_count(postgres_case) == 0
