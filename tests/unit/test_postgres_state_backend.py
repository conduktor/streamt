"""Private PostgreSQL read/sessional-lock scaffold contracts."""

from __future__ import annotations

import inspect
import json
import uuid
from collections.abc import Iterator
from pathlib import Path
from typing import cast

import pytest

import streamt.deployer.postgres_state as postgres_state
import streamt.deployer.postgres_state_backend as postgres_backend
from streamt.cli.commands import adopt, apply, plan, state_cmd
from streamt.core.deployment_state import validate_deployment_state_config
from streamt.deployer.postgres_state import (
    PostgresStateAdministration,
    PostgresStateStatus,
)
from streamt.deployer.postgres_state_backend import PrivatePostgresStateReadBackend
from streamt.deployer.state import LocalState
from streamt.deployer.state_backend import (
    ControlObservation,
    DeploymentStateBackend,
    OperationControlState,
    OperationIntent,
    StateAddress,
    StateBackendInvalidStateError,
    StateBackendLockLostError,
    StateBackendLockTimeoutError,
    StateBackendUnavailableError,
    StateRevision,
    StateStoreIdentity,
    make_deployment_state_service,
    state_checksum,
)


class _FakeComposable:
    def __init__(self, value: str) -> None:
        self.value = value

    def format(self, *args: object) -> _FakeComposable:
        rendered = self.value
        for argument in args:
            rendered = rendered.replace("{}", str(argument), 1)
        return _FakeComposable(rendered)

    def __str__(self) -> str:
        return self.value


class _FakeSql:
    def SQL(self, value: str) -> _FakeComposable:  # noqa: N802
        return _FakeComposable(value)

    def Identifier(self, *values: str) -> _FakeComposable:  # noqa: N802
        return _FakeComposable(".".join(f'"{value}"' for value in values))


class _FakeCursor:
    def __init__(
        self,
        *,
        address: StateAddress,
        store_id: str,
        state: LocalState | None = None,
        primary: bool = True,
        try_lock: Iterator[bool] | None = None,
    ) -> None:
        self.address = address
        self.store_id = store_id
        self.state = state
        self.primary = primary
        self.try_lock = try_lock or iter((True,))
        self.current: object = ()
        self.calls: list[tuple[str, tuple[object, ...] | None]] = []
        self.closed = False
        self.lock_owned = False
        self.fail_health = False
        self.acquisition_pid = 701
        self.release_pid = 701
        self.release_result = True

    def execute(
        self,
        query: object,
        params: tuple[object, ...] | None = None,
    ) -> object:
        rendered = str(query)
        self.calls.append((rendered, params))
        if self.fail_health and "pg_locks" in rendered:
            raise RuntimeError("postgresql://alice:secret@db.internal/state")
        if "pg_try_advisory_lock" in rendered:
            acquired = next(self.try_lock, False)
            gated = acquired and self.acquisition_pid == 701 and self.primary
            self.lock_owned = gated
            self.current = ((self.acquisition_pid, not self.primary, gated),)
        elif "pg_advisory_unlock" in rendered:
            gated = self.release_result and self.release_pid == 701 and self.primary
            self.current = ((self.release_pid, not self.primary, gated),)
            if gated:
                self.lock_owned = False
        elif "pg_is_in_recovery" in rendered and "pg_locks" not in rendered:
            self.current = ((not self.primary, 701),)
        elif "pg_locks" in rendered:
            self.current = ((701, not self.primary, self.lock_owned),)
        elif '"streamt"."state_addresses"' in rendered:
            self.current = (
                (
                    self.address.uri,
                    postgres_state._advisory_lock_key(self.address),
                ),
            )
        elif '"streamt"."store_metadata"' in rendered:
            self.current = ((self.store_id, 1),)
        elif '"streamt"."operation_control"' in rendered:
            raw_control = json.dumps(
                OperationControlState.clear(self.address).to_dict(),
                sort_keys=True,
                separators=(",", ":"),
            )
            self.current = ((4, "clear", raw_control, len(raw_control.encode())),)
        elif '"streamt"."current_state"' in rendered:
            if self.state is None:
                self.current = ()
            else:
                raw_state = json.dumps(
                    self.state.to_dict(),
                    sort_keys=True,
                    separators=(",", ":"),
                )
                self.current = (
                    (
                        9,
                        self.state.serial,
                        state_checksum(self.state),
                        raw_state,
                        len(raw_state.encode()),
                    ),
                )
        elif rendered.startswith("BEGIN") or "set_config(" in rendered:
            self.current = ()
        else:
            raise AssertionError(f"unexpected SQL boundary: {rendered}")
        return self

    def fetchall(self) -> object:
        return self.current

    def close(self) -> None:
        self.closed = True


class _FakeConnection:
    def __init__(self, cursor: _FakeCursor) -> None:
        self._cursor = cursor
        self.closed = False
        self.rollbacks = 0
        self.rollback_failure: Exception | None = None

    def cursor(self) -> _FakeCursor:
        return self._cursor

    def rollback(self) -> None:
        self.rollbacks += 1
        if self.rollback_failure is not None:
            raise self.rollback_failure

    def close(self) -> None:
        self.closed = True


class _FakeDriver:
    def __init__(self, connection: _FakeConnection) -> None:
        self.connection = connection
        self.calls: list[tuple[str, dict[str, object]]] = []

    def connect(self, conninfo: str, **kwargs: object) -> _FakeConnection:
        self.calls.append((conninfo, kwargs))
        return self.connection


def _address() -> StateAddress:
    return StateAddress(namespace="platform", project="payments", environment="prod")


def _status(address: StateAddress, store_id: str) -> PostgresStateStatus:
    return PostgresStateStatus(
        store_status="ready",
        store_id=store_id,
        schema_version=1,
        address=address,
        address_status="registered",
        state_status="absent",
        state_serial=0,
        state_checksum=None,
        operation_status=None,
    )


def _install(
    monkeypatch: pytest.MonkeyPatch,
    *,
    state: LocalState | None = None,
    primary: bool = True,
    try_lock: Iterator[bool] | None = None,
) -> tuple[_FakeDriver, _FakeConnection, _FakeCursor]:
    address = _address()
    store_id = "00000000-0000-4000-8000-000000000001"
    cursor = _FakeCursor(
        address=address,
        store_id=store_id,
        state=state,
        primary=primary,
        try_lock=try_lock,
    )
    connection = _FakeConnection(cursor)
    driver = _FakeDriver(connection)
    bundle = postgres_state._PsycopgBundle(
        driver=cast(postgres_state._DriverModule, driver),
        sql=cast(postgres_state._SqlModule, _FakeSql()),
    )
    monkeypatch.setattr(postgres_backend, "_load_psycopg", lambda: bundle)
    monkeypatch.setattr(
        PostgresStateAdministration,
        "_read_status",
        lambda _self, _cursor, _sql, requested: _status(requested, store_id),
    )
    return driver, connection, cursor


def _backend(*, timeout: int = 1) -> PrivatePostgresStateReadBackend:
    return PrivatePostgresStateReadBackend(
        dsn="host=/var/run/postgresql dbname=state",
        schema="streamt",
        lock_timeout_seconds=timeout,
    )


@pytest.mark.parametrize("present", [False, True])
def test_read_snapshot_parses_exact_state_and_control_from_one_transaction(
    monkeypatch: pytest.MonkeyPatch,
    present: bool,
) -> None:
    state = LocalState(project="payments", environment="prod", serial=3) if present else None
    driver, connection, cursor = _install(monkeypatch, state=state)

    snapshot = _backend().read_snapshot(_address())

    assert snapshot.state.store.store_id == "00000000-0000-4000-8000-000000000001"
    assert snapshot.state.address == _address()
    assert snapshot.state.state == (state or LocalState(project="payments", environment="prod"))
    assert snapshot.state.revision.value == ("postgres-v1:9" if present else "ABSENT")
    assert snapshot.control.revision.value == "postgres-v1:4"
    assert snapshot.control.control.status == "clear"
    assert driver.calls == [
        (
            "host=/var/run/postgresql dbname=state",
            {"connect_timeout": 10, "sslmode": "prefer"},
        )
    ]
    assert next(query for query, _ in cursor.calls if query.startswith("BEGIN")).endswith(
        "REPEATABLE READ READ ONLY"
    )
    assert connection.rollbacks == 1
    assert connection.closed is True
    assert cursor.closed is True


def test_absent_state_with_active_control_preserves_recovery_blocker() -> None:
    address = _address()
    empty = LocalState(project=address.project, environment=address.environment)
    intent = OperationIntent(
        operation_id=str(uuid.uuid4()),
        kind="apply",
        started_at="2026-09-02T00:00:00Z",
        actor="test",
        prior_state_serial=0,
        prior_state_checksum=state_checksum(empty),
        reviewed_plan_checksum=None,
        actions=(),
    )
    control = ControlObservation(
        control=OperationControlState(
            address=address,
            status="in_progress",
            intent=intent,
        ),
        revision=StateRevision("postgres-v1:1"),
    )

    state = postgres_backend._parse_state_row(
        None,
        address=address,
        store=StateStoreIdentity(
            backend="postgres",
            store_id="00000000-0000-4000-8000-000000000001",
        ),
        control=control,
    )

    assert state.revision.is_absent
    assert state.state == empty
    assert control.control.status == "in_progress"


@pytest.mark.parametrize(
    "row",
    [
        None,
        (0, "clear", "{}", 2),
        (0, "clear", "not-json", 8),
    ],
)
def test_absent_state_still_requires_a_present_strict_control_row(
    row: tuple[object, ...] | None,
) -> None:
    with pytest.raises(StateBackendInvalidStateError):
        postgres_backend._parse_control_row(row, _address())


def test_operation_uses_autocommit_physical_session_checks_lock_and_cleans_up(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    driver, connection, cursor = _install(monkeypatch)

    with _backend().operation(_address()) as operation:
        assert operation.backend_pid == 701
        operation.check_lock()
        snapshot = operation.observe()
        assert snapshot.state.revision.is_absent
        assert cursor.lock_owned is True

    assert driver.calls[0][1]["autocommit"] is True
    assert sum("pg_locks" in query for query, _params in cursor.calls) >= 3
    assert any("pg_advisory_unlock" in query for query, _params in cursor.calls)
    assert not any(
        query.lstrip().upper().startswith(("INSERT ", "UPDATE ", "DELETE ", "MERGE "))
        for query, _params in cursor.calls
    )
    assert cursor.lock_owned is False
    assert cursor.closed is True
    assert connection.closed is True


def test_lock_contention_times_out_and_closes_session(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _driver, connection, cursor = _install(monkeypatch, try_lock=iter((False,)))

    with (
        pytest.raises(
            StateBackendLockTimeoutError,
            match=r"^PostgreSQL deployment state operation lock timed out$",
        ),
        _backend(timeout=0).operation(_address()),
    ):
        pytest.fail("busy lock must not yield an operation")

    assert cursor.closed is True
    assert connection.closed is True
    assert not any("pg_advisory_unlock" in query for query, _params in cursor.calls)


def test_pooler_backend_switch_is_gated_before_lock_acquisition(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _driver, connection, cursor = _install(monkeypatch)
    cursor.acquisition_pid = 702

    def acquire() -> None:
        with _backend().operation(_address()):
            pytest.fail("a switched backend must not yield operation authority")

    with pytest.raises(
        StateBackendUnavailableError,
        match=r"^PostgreSQL deployment state operation lock is unavailable$",
    ):
        acquire()

    acquire_call = next(call for call in cursor.calls if "pg_try_advisory_lock" in call[0])
    assert "CASE WHEN" in acquire_call[0]
    assert acquire_call[1] == (701, postgres_state._advisory_lock_key(_address()))
    assert cursor.lock_owned is False
    assert connection.closed is True


def test_standby_is_rejected_before_lock_acquisition(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _driver, connection, cursor = _install(monkeypatch, primary=False)

    with (
        pytest.raises(
            StateBackendUnavailableError,
            match=r"^PostgreSQL deployment state operation lock is unavailable$",
        ) as raised,
        _backend().operation(_address()),
    ):
        pytest.fail("standby must not yield an operation")

    assert raised.value.__cause__ is None
    assert not any("pg_try_advisory_lock" in query for query, _params in cursor.calls)
    assert cursor.closed is True
    assert connection.closed is True


def test_session_failure_is_secret_neutral_lock_loss_and_close_releases_server_lock(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _driver, connection, cursor = _install(monkeypatch)

    def lose_lock() -> None:
        with _backend().operation(_address()) as operation:
            cursor.fail_health = True
            operation.check_lock()

    with pytest.raises(StateBackendLockLostError) as raised:
        lose_lock()

    assert str(raised.value) == "PostgreSQL deployment state operation lock was lost"
    assert "alice" not in str(raised.value)
    assert "secret" not in str(raised.value)

    assert cursor.closed is True
    assert connection.closed is True


def test_unverified_unlock_fails_closed_after_clean_body(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _driver, connection, cursor = _install(monkeypatch)

    with (
        pytest.raises(
            StateBackendLockLostError,
            match=r"operation lock release was not verified$",
        ),
        _backend().operation(_address()),
    ):
        cursor.release_result = False

    assert cursor.closed is True
    assert connection.closed is True


def test_pooler_backend_switch_is_gated_before_unlock(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _driver, connection, cursor = _install(monkeypatch)

    def release() -> None:
        with _backend().operation(_address()):
            cursor.release_pid = 702

    with pytest.raises(StateBackendLockLostError, match="release was not verified"):
        release()

    release_call = next(call for call in cursor.calls if "pg_advisory_unlock" in call[0])
    assert "CASE WHEN" in release_call[0]
    assert release_call[1] == (701, postgres_state._advisory_lock_key(_address()))
    assert cursor.lock_owned is True
    assert connection.closed is True


def test_catalog_invalid_precedes_rollback_lock_loss_and_is_sanitized(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _driver, connection, _cursor = _install(monkeypatch)

    def read_invalid_catalog() -> None:
        with _backend().operation(_address()) as operation:
            monkeypatch.setattr(
                postgres_backend,
                "_read_snapshot_transaction",
                lambda **_kwargs: (_ for _ in ()).throw(
                    StateBackendInvalidStateError("provider-secret schema=private")
                ),
            )
            connection.rollback_failure = RuntimeError("postgresql://alice:secret@db.internal")
            operation.observe()

    with pytest.raises(StateBackendInvalidStateError) as raised:
        read_invalid_catalog()

    assert str(raised.value) == "PostgreSQL deployment state is invalid"
    assert "secret" not in str(raised.value)

    assert connection.closed is True


def test_private_scaffold_exposes_no_mutation_methods() -> None:
    backend = _backend()

    assert not hasattr(backend, "compare_and_swap")
    assert not hasattr(backend, "begin_operation")
    assert not hasattr(backend, "commit_operation")
    assert not hasattr(backend, "clear_before_mutation")


def test_private_backend_structurally_conforms_without_factory_selection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _driver, _connection, _cursor = _install(monkeypatch)
    backend = _backend()

    assert isinstance(backend, DeploymentStateBackend)
    assert backend.describe() == StateStoreIdentity(
        backend="postgres",
        store_id="00000000-0000-4000-8000-000000000001",
    )
    assert backend.read(_address()).revision.is_absent
    assert backend.read_control(_address()).control.status == "clear"


def test_private_scaffold_is_not_factory_selectable_or_cli_authority(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("PRIVATE_POSTGRES_STATE_DSN", "host=/var/run/postgresql dbname=state")
    config = validate_deployment_state_config(
        {
            "backend": "postgres",
            "namespace": "platform",
            "postgres": {"dsn_env": "PRIVATE_POSTGRES_STATE_DSN", "schema": "streamt"},
        }
    )

    with pytest.raises(
        StateBackendUnavailableError,
        match=r"^PostgreSQL deployment state is unavailable in this release$",
    ):
        make_deployment_state_service(
            tmp_path,
            project="payments",
            environment="prod",
            config=config,
        )

    for command_module in (plan, apply, adopt, state_cmd):
        assert "postgres_state_backend" not in inspect.getsource(command_module)
        assert not hasattr(command_module, "PrivatePostgresStateReadBackend")


def test_construction_does_not_load_optional_psycopg(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = 0

    def fail_if_loaded() -> object:
        nonlocal calls
        calls += 1
        raise AssertionError("construction must remain driver-lazy")

    monkeypatch.setattr(postgres_backend, "_load_psycopg", fail_if_loaded)

    backend = _backend()

    assert isinstance(backend, PrivatePostgresStateReadBackend)
    assert calls == 0
