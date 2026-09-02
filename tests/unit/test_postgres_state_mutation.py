"""Private owner-v1 PostgreSQL mutation contracts."""

from __future__ import annotations

import copy
import json
import uuid
from dataclasses import dataclass, field, replace
from typing import cast

import pytest

import streamt.deployer.postgres_state_backend as postgres_backend
from streamt.deployer.postgres_state import (
    POSTGRES_STATE_MAX_BYTES,
    PostgresStateAdministration,
    PostgresStateStatus,
    _advisory_lock_key,
    _DriverModule,
    _PsycopgBundle,
    _SqlModule,
)
from streamt.deployer.postgres_state import (
    _Connection as _ConnectionProtocol,
)
from streamt.deployer.postgres_state import (
    _Cursor as _CursorProtocol,
)
from streamt.deployer.state import LocalState, ManagedResourceRecord
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
    StateBackendReleaseAfterCommitError,
    StateBackendUnavailableError,
    StateBackendUnknownCommitError,
    operation_timestamp,
    state_checksum,
)


class _Composable:
    def __init__(self, value: str) -> None:
        self.value = value

    def format(self, *args: object) -> _Composable:
        rendered = self.value
        for argument in args:
            rendered = rendered.replace("{}", str(argument), 1)
        return _Composable(rendered)

    def __str__(self) -> str:
        return self.value


class _Sql:
    def SQL(self, value: str) -> _Composable:  # noqa: N802
        return _Composable(value)

    def Identifier(self, *values: str) -> _Composable:  # noqa: N802
        return _Composable(".".join(f'"{value}"' for value in values))


@dataclass
class _Database:
    address: StateAddress
    store_id: str = "00000000-0000-4000-8000-000000000001"
    state: LocalState | None = None
    state_revision: int | None = None
    control: OperationControlState = field(init=False)
    control_revision: int = 0
    operation_history: list[tuple[int, str, str]] = field(default_factory=list)
    state_history: list[tuple[int, int, str, str, str]] = field(default_factory=list)
    writer_active: bool = True
    writer_lock_owned: bool = True

    def __post_init__(self) -> None:
        self.control = OperationControlState.clear(self.address)


class _Cursor:
    def __init__(self, connection: _Connection, *, pid: int) -> None:
        self.connection = connection
        self.pid = pid
        self.current: object = ()
        self.calls: list[tuple[str, tuple[object, ...] | None]] = []
        self.closed = False
        self.lock_owned = True
        self.release_result = True

    @property
    def database(self) -> _Database:
        return self.connection.transaction or self.connection.database

    def execute(
        self,
        query: object,
        params: tuple[object, ...] | None = None,
    ) -> object:
        rendered = str(query)
        self.calls.append((rendered, params))
        failure = self.connection.fail_dml_pattern
        if failure is not None and failure in rendered:
            raise RuntimeError("postgresql://owner:secret@db.internal/state")
        if rendered.startswith("BEGIN"):
            self.connection.transaction = copy.deepcopy(self.connection.database)
            self.current = ()
        elif "set_config(" in rendered:
            self.current = ()
        elif "pg_advisory_unlock" in rendered:
            self.current = ((self.pid, False, self.release_result),)
            if self.release_result:
                self.lock_owned = False
        elif "pg_is_in_recovery" in rendered and "pg_locks" not in rendered:
            self.current = ((False, self.pid),)
        elif "pg_stat_activity" in rendered:
            self.current = (
                (
                    not self.database.writer_active,
                    not self.database.writer_lock_owned,
                ),
            )
        elif "pg_locks" in rendered:
            self.current = ((self.pid, False, self.lock_owned),)
        elif rendered.startswith("SELECT CURRENT_USER"):
            self.current = (("streamt_owner", self.connection.owner_ok, 7, True),)
        elif 'FROM "streamt"."operation_control"' in rendered:
            payload = _json(self.database.control.to_dict())
            self.current = (
                (
                    self.database.control_revision,
                    self.database.control.status,
                    payload,
                    len(payload.encode()),
                ),
            )
        elif 'FROM "streamt"."current_state"' in rendered:
            state = self.database.state
            if state is None:
                self.current = ()
            else:
                payload = _json(state.to_dict())
                self.current = (
                    (
                        self.database.state_revision,
                        state.serial,
                        state_checksum(state),
                        payload,
                        len(payload.encode()),
                    ),
                )
        elif rendered.startswith("SELECT event_index"):
            self.current = tuple(
                (index, kind, payload, len(payload.encode()))
                for index, kind, payload in self.database.operation_history
            )
        elif rendered.startswith("SELECT revision, state_serial"):
            assert params is not None
            operation_id = cast(str, params[-1])
            self.current = tuple(
                (revision, serial, checksum, payload, len(payload.encode()))
                for revision, serial, checksum, payload, stored_operation_id in self.database.state_history
                if stored_operation_id == operation_id
            )
        elif rendered.startswith("SELECT revision FROM"):
            assert params is not None
            operation_id = cast(str, params[-1])
            self.current = tuple(
                (revision,)
                for revision, _serial, _checksum, _payload, stored_operation_id in self.database.state_history
                if stored_operation_id == operation_id
            )
        elif rendered.startswith('INSERT INTO "streamt"."current_state"'):
            self.connection.dml_attempts.append("insert_current_state")
            assert params is not None
            if self.database.state is not None:
                self.current = ()
            else:
                self.database.state_revision = cast(int, params[3])
                self.database.state = LocalState.from_dict(json.loads(cast(str, params[6])))
                self.current = ((self.database.state_revision,),)
        elif rendered.startswith('UPDATE "streamt"."current_state"'):
            self.connection.dml_attempts.append("update_current_state")
            assert params is not None
            expected_revision = params[6]
            expected_serial = params[7]
            expected_checksum = params[8]
            expected_json = params[9]
            current = self.database.state
            if (
                current is None
                or self.database.state_revision != expected_revision
                or current.serial != expected_serial
                or state_checksum(current) != expected_checksum
                or _json(current.to_dict()) != expected_json
            ):
                self.current = ()
            else:
                self.database.state = LocalState.from_dict(json.loads(cast(str, params[2])))
                self.database.state_revision = cast(int, expected_revision) + 1
                self.current = ((self.database.state_revision,),)
        elif rendered.startswith('INSERT INTO "streamt"."state_history"'):
            self.connection.dml_attempts.append("insert_state_history")
            assert params is not None
            revision = cast(int, params[3])
            if any(row[0] == revision for row in self.database.state_history):
                self.current = ()
            else:
                self.database.state_history.append(
                    (
                        revision,
                        cast(int, params[4]),
                        cast(str, params[5]),
                        cast(str, params[6]),
                        cast(str, params[7]),
                    )
                )
                self.current = ((revision,),)
        elif rendered.startswith('UPDATE "streamt"."operation_control"'):
            self.connection.dml_attempts.append("update_operation_control")
            assert params is not None
            if (
                self.database.control_revision != params[5]
                or self.database.control.status != params[6]
                or _json(self.database.control.to_dict()) != params[7]
            ):
                self.current = ()
            else:
                self.database.control_revision += 1
                self.database.control = OperationControlState.from_dict(
                    json.loads(cast(str, params[1])),
                    expected_address=self.database.address,
                )
                self.current = ((self.database.control_revision,),)
        elif rendered.startswith('INSERT INTO "streamt"."operation_history"'):
            self.connection.dml_attempts.append("insert_operation_history")
            assert params is not None
            index = cast(int, params[4])
            if any(row[0] == index for row in self.database.operation_history):
                self.current = ()
            else:
                self.database.operation_history.append(
                    (index, cast(str, params[5]), cast(str, params[6]))
                )
                self.current = ((index,),)
        else:
            raise AssertionError(f"unexpected SQL: {rendered}")
        return self

    def fetchall(self) -> object:
        return self.current

    def close(self) -> None:
        self.closed = True


class _Connection:
    def __init__(self, database: _Database, *, pid: int) -> None:
        self.database = database
        self.transaction: _Database | None = None
        self.cursor_value = _Cursor(self, pid=pid)
        self.fail_dml_pattern: str | None = None
        self.owner_ok = True
        self.commit_mode = "normal"
        self.close_releases_writer = True
        self.dml_attempts: list[str] = []
        self.closed = False
        self.rollbacks = 0

    def cursor(self) -> _Cursor:
        return self.cursor_value

    def commit(self) -> None:
        if self.transaction is None:
            raise AssertionError("commit without transaction")
        transaction = self.transaction
        self.transaction = None
        if self.commit_mode == "reject":
            raise RuntimeError("commit acknowledgement unavailable")
        self.database.__dict__.update(copy.deepcopy(transaction.__dict__))
        if self.commit_mode == "apply_then_raise":
            raise RuntimeError("commit acknowledgement unavailable")
        if self.commit_mode == "corrupt_history":
            self.database.operation_history.pop()

    def rollback(self) -> None:
        self.rollbacks += 1
        self.transaction = None

    def close(self) -> None:
        self.closed = True
        if self.cursor_value.pid == 701 and self.close_releases_writer:
            self.database.writer_active = False
            self.database.writer_lock_owned = False


class _Driver:
    def __init__(self, database: _Database) -> None:
        self.database = database
        self.connections: list[_Connection] = []

    def connect(self, _dsn: str, **_kwargs: object) -> _Connection:
        connection = _Connection(self.database, pid=800 + len(self.connections))
        self.connections.append(connection)
        return connection


def _json(value: dict[str, object]) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _address() -> StateAddress:
    return StateAddress(namespace="platform", project="payments", environment="prod")


def _status(address: StateAddress, database: _Database) -> PostgresStateStatus:
    return PostgresStateStatus(
        store_status="ready",
        store_id=database.store_id,
        schema_version=1,
        address=address,
        address_status="registered",
        state_status="present" if database.state is not None else "absent",
        state_serial=database.state.serial if database.state is not None else 0,
        state_checksum=state_checksum(database.state) if database.state is not None else None,
        operation_status=None,
    )


def _operation(
    monkeypatch: pytest.MonkeyPatch,
    *,
    timeout: int = 3,
) -> tuple[
    postgres_backend._PostgresStateReadOperation,
    _Database,
    _Connection,
    _Driver,
]:
    database = _Database(_address())
    owner = _Connection(database, pid=701)
    driver = _Driver(database)
    bundle = _PsycopgBundle(
        driver=cast(_DriverModule, driver),
        sql=cast(_SqlModule, _Sql()),
    )
    monkeypatch.setattr(postgres_backend, "_load_psycopg", lambda: bundle)
    monkeypatch.setattr(
        PostgresStateAdministration,
        "_read_status",
        lambda _self, _cursor, _sql, address: _status(address, database),
    )
    operation = postgres_backend._PostgresStateReadOperation(
        connection=cast(_ConnectionProtocol, owner),
        cursor=cast(_CursorProtocol, owner.cursor_value),
        bundle=bundle,
        dsn="host=/var/run/postgresql dbname=state",
        schema="streamt",
        lock_timeout_seconds=timeout,
        address=_address(),
        lock_key=_advisory_lock_key(_address()),
        backend_pid=701,
    )
    return operation, database, owner, driver


def _intent(snapshot: OperationSnapshot, *, actions: bool = True) -> OperationIntent:
    return OperationIntent(
        operation_id=str(uuid.uuid4()),
        kind="apply",
        started_at=operation_timestamp(),
        actor="unit-test",
        prior_state_serial=snapshot.state.state_serial,
        prior_state_checksum=state_checksum(snapshot.state.state),
        reviewed_plan_checksum=None,
        actions=(
            OperationAction(
                index=0,
                resource_id="streamt://payments/prod/topic/orders",
                action="create",
            ),
        )
        if actions
        else (),
    )


def _completed(
    operation: postgres_backend._PostgresStateReadOperation,
    snapshot: OperationSnapshot,
    intent: OperationIntent,
) -> OperationSnapshot:
    snapshot = operation.record_progress(
        snapshot,
        OperationProgress(
            operation_id=intent.operation_id,
            action_index=0,
            resource_id="streamt://payments/prod/topic/orders",
            action="create",
            status="started",
            succeeded=None,
            recorded_at=operation_timestamp(),
        ),
    )
    return operation.record_progress(
        snapshot,
        OperationProgress(
            operation_id=intent.operation_id,
            action_index=0,
            resource_id="streamt://payments/prod/topic/orders",
            action="create",
            status="completed",
            succeeded=True,
            recorded_at=operation_timestamp(),
        ),
    )


def test_changed_commit_is_one_atomic_transaction_with_exact_histories(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    operation, database, owner, driver = _operation(monkeypatch)
    initial = operation.observe()
    intent = _intent(initial)
    active = operation.begin_operation(initial, intent)
    active = _completed(operation, active, intent)
    replacement = LocalState(project="payments", environment="prod", serial=1)

    committed = operation.commit_operation(active, replacement)

    assert committed.state.state == replacement
    assert committed.control.control.status == "clear"
    assert database.state == replacement
    assert database.state_revision == 1
    assert len(database.state_history) == 1
    assert [row[:2] for row in database.operation_history] == [
        (0, "intent"),
        (1, "progress_started"),
        (2, "progress_completed"),
        (3, "succeeded"),
    ]
    assert owner.dml_attempts.count("insert_current_state") == 1
    assert owner.dml_attempts.count("insert_state_history") == 1
    assert owner.dml_attempts.count("update_operation_control") == 4
    assert len(driver.connections) == 4


def test_unchanged_commit_never_writes_current_or_state_history(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    operation, database, owner, _driver = _operation(monkeypatch)
    initial = operation.observe()
    intent = _intent(initial, actions=False)
    active = operation.begin_operation(initial, intent)

    committed = operation.commit_operation(active, None)

    assert committed.state == initial.state
    assert database.state is None
    assert database.state_history == []
    assert not any("current_state" in attempt for attempt in owner.dml_attempts)
    assert [row[1] for row in database.operation_history] == ["intent", "succeeded"]


def test_present_state_commit_uses_exact_update_and_next_revision(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    operation, database, owner, _driver = _operation(monkeypatch)
    database.state = LocalState(project="payments", environment="prod", serial=4)
    database.state_revision = 9
    initial = operation.observe()
    intent = _intent(initial, actions=False)
    active = operation.begin_operation(initial, intent)
    replacement = LocalState(project="payments", environment="prod", serial=5)

    committed = operation.commit_operation(active, replacement)

    assert committed.state.revision.value == "postgres-v1:10"
    assert database.state == replacement
    assert owner.dml_attempts.count("update_current_state") == 1
    assert owner.dml_attempts.count("insert_current_state") == 0
    assert database.state_history[0][:2] == (10, 5)


def test_clear_before_mutation_appends_terminal_clear_event(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    operation, database, _owner, _driver = _operation(monkeypatch)
    initial = operation.observe()
    intent = _intent(initial, actions=False)
    active = operation.begin_operation(initial, intent)

    cleared = operation.clear_before_mutation(active)

    assert cleared.control.control.status == "clear"
    assert [row[1] for row in database.operation_history] == [
        "intent",
        "cleared_before_mutation",
    ]


def test_stale_control_revision_is_rejected_before_any_dml(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    operation, database, owner, _driver = _operation(monkeypatch)
    initial = operation.observe()
    database.control_revision += 1

    with pytest.raises(StateBackendConflictError):
        operation.begin_operation(initial, _intent(initial, actions=False))

    assert owner.dml_attempts == []


def test_private_mutation_requires_exact_v1_schema_and_table_owner(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    operation, _database, owner, _driver = _operation(monkeypatch)
    initial = operation.observe()
    owner.owner_ok = False

    with pytest.raises(StateBackendUnavailableError, match="version-one owner"):
        operation.begin_operation(initial, _intent(initial, actions=False))

    assert owner.dml_attempts == []


def test_finalization_dml_failure_rolls_back_and_recovery_can_be_persisted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    operation, database, owner, _driver = _operation(monkeypatch)
    initial = operation.observe()
    intent = _intent(initial)
    active = _completed(operation, operation.begin_operation(initial, intent), intent)
    before_control_revision = database.control_revision
    before_history = copy.deepcopy(database.operation_history)
    owner.fail_dml_pattern = 'UPDATE "streamt"."operation_control"'

    with pytest.raises(StateBackendUnavailableError) as raised:
        operation.commit_operation(
            active,
            LocalState(project="payments", environment="prod", serial=1),
        )

    assert "secret" not in str(raised.value)
    assert database.state is None
    assert database.state_history == []
    assert database.operation_history == before_history
    assert database.control_revision == before_control_revision
    owner.fail_dml_pattern = None
    recovery = operation.mark_recovery_required(
        active,
        RecoveryRecord(
            operation_id=intent.operation_id,
            failure_code="state_commit_failed",
            failed_at=operation_timestamp(),
            last_completed_action_index=0,
        ),
    )
    assert recovery.control.control.status == "recovery_required"


def test_exact_preimage_after_ack_loss_proves_not_committed_without_retry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    operation, _database, owner, _driver = _operation(monkeypatch)
    initial = operation.observe()
    intent = _intent(initial, actions=False)
    active = operation.begin_operation(initial, intent)
    owner.commit_mode = "reject"

    with pytest.raises(StateBackendLockLostError) as raised:
        operation.commit_operation(active, None)

    assert raised.value.operation_id == intent.operation_id
    assert owner.dml_attempts.count("update_operation_control") == 2
    assert owner.dml_attempts.count("insert_operation_history") == 2


def test_preimage_is_not_accepted_while_original_writer_may_still_commit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    operation, _database, owner, _driver = _operation(monkeypatch, timeout=0)
    initial = operation.observe()
    intent = _intent(initial, actions=False)
    active = operation.begin_operation(initial, intent)
    owner.commit_mode = "reject"
    owner.close_releases_writer = False

    with pytest.raises(StateBackendUnknownCommitError) as raised:
        operation.commit_operation(active, None)

    assert raised.value.operation_id == intent.operation_id
    assert owner.dml_attempts.count("update_operation_control") == 2
    assert owner.dml_attempts.count("insert_operation_history") == 2


def test_mixed_postimage_after_commit_is_unknown_and_never_retried(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    operation, _database, owner, _driver = _operation(monkeypatch)
    initial = operation.observe()
    intent = _intent(initial, actions=False)
    active = operation.begin_operation(initial, intent)
    owner.commit_mode = "corrupt_history"

    with pytest.raises(StateBackendUnknownCommitError) as raised:
        operation.commit_operation(active, None)

    assert "secret" not in str(raised.value)
    assert raised.value.operation_id == intent.operation_id
    assert owner.dml_attempts.count("update_operation_control") == 2
    assert owner.dml_attempts.count("insert_operation_history") == 2
    attempts = list(owner.dml_attempts)
    with pytest.raises(StateBackendLockLostError) as lost:
        operation.mark_recovery_required(
            active,
            RecoveryRecord(
                operation_id=intent.operation_id,
                failure_code="state_commit_unknown",
                failed_at=operation_timestamp(),
                last_completed_action_index=None,
            ),
        )
    assert lost.value.operation_id == intent.operation_id
    assert owner.dml_attempts == attempts


def test_applied_but_unacknowledged_final_commit_is_independently_proved_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    operation, database, owner, driver = _operation(monkeypatch)
    initial = operation.observe()
    intent = _intent(initial, actions=False)
    active = operation.begin_operation(initial, intent)
    owner.commit_mode = "apply_then_raise"
    connections_before = len(driver.connections)

    committed = operation.commit_operation(active, None)

    assert committed.control.control.status == "clear"
    assert operation.finalized is True
    assert database.control.status == "clear"
    assert owner.dml_attempts.count("update_operation_control") == 2
    assert len(driver.connections) == connections_before + 1


def test_stale_control_and_corrupt_history_fail_before_dml(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    operation, database, owner, _driver = _operation(monkeypatch)
    initial = operation.observe()
    intent = _intent(initial)
    active = operation.begin_operation(initial, intent)
    database.operation_history[0] = (
        0,
        "intent",
        _json(OperationControlState.clear(_address()).to_dict()),
    )
    attempts = len(owner.dml_attempts)

    with pytest.raises(StateBackendConflictError):
        operation.record_progress(
            active,
            OperationProgress(
                operation_id=intent.operation_id,
                action_index=0,
                resource_id="streamt://payments/prod/topic/orders",
                action="create",
                status="started",
                succeeded=None,
                recorded_at=operation_timestamp(),
            ),
        )

    assert len(owner.dml_attempts) == attempts


def test_lost_lock_and_oversized_state_fail_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    operation, database, owner, _driver = _operation(monkeypatch)
    initial = operation.observe()
    intent = _intent(initial, actions=False)
    active = operation.begin_operation(initial, intent)
    oversized = LocalState(project="payments", environment="prod", serial=1)
    oversized.resources = {
        "streamt://payments/prod/topic/orders": ManagedResourceRecord(
            physical_name="x" * (POSTGRES_STATE_MAX_BYTES + 1),
            ownership="managed",
            artifact_checksum="sha256:" + "0" * 64,
            backend="kafka",
        )
    }

    with pytest.raises(StateBackendInvalidStateError, match="size limit"):
        operation.commit_operation(active, oversized)
    assert database.control.status == "in_progress"
    owner.cursor_value.lock_owned = False
    with pytest.raises(StateBackendLockLostError):
        operation.ensure_ready(active)


def test_verified_commit_release_failure_reports_committed_outcome(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    operation, _database, owner, _driver = _operation(monkeypatch)
    initial = operation.observe()
    intent = _intent(initial, actions=False)
    active = operation.begin_operation(initial, intent)
    operation.commit_operation(active, None)
    owner.cursor_value.release_result = False
    context = postgres_backend._PostgresStateOperationContext(
        dsn="host=/var/run/postgresql dbname=state",
        schema="streamt",
        lock_timeout_seconds=3,
        address=_address(),
    )
    context._connection = cast(_ConnectionProtocol, owner)
    context._cursor = cast(_CursorProtocol, owner.cursor_value)
    context._operation = operation
    context._lock_key = _advisory_lock_key(_address())
    context._lock_acquired = True

    with pytest.raises(StateBackendReleaseAfterCommitError) as raised:
        context.__exit__(None, None, None)

    assert raised.value.committed is True


def test_failed_begin_retains_operation_identity_for_scope_release(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    operation, _database, owner, _driver = _operation(monkeypatch)
    initial = operation.observe()
    intent = replace(_intent(initial, actions=False), prior_state_serial=1)

    with pytest.raises(StateBackendConflictError):
        operation.begin_operation(initial, intent)

    owner.cursor_value.release_result = False
    context = postgres_backend._PostgresStateOperationContext(
        dsn="host=/var/run/postgresql dbname=state",
        schema="streamt",
        lock_timeout_seconds=3,
        address=_address(),
    )
    context._connection = cast(_ConnectionProtocol, owner)
    context._cursor = cast(_CursorProtocol, owner.cursor_value)
    context._operation = operation
    context._lock_key = _advisory_lock_key(_address())
    context._lock_acquired = True

    with pytest.raises(StateBackendLockLostError) as raised:
        context.__exit__(None, None, None)

    assert raised.value.operation_id == intent.operation_id
