"""Transient PostgreSQL deployment-state lock-probe contracts."""

from __future__ import annotations

from typing import cast

import pytest

import streamt.deployer.postgres_state as postgres_state
from streamt.core.deployment_state import (
    PostgresDeploymentStateConfig,
    local_deployment_state_config,
    validate_deployment_state_config,
)
from streamt.deployer.postgres_state import (
    PostgresStateAdministration,
    PostgresStateLockProbe,
    PostgresStateStatus,
    make_postgres_state_lock_probe,
)
from streamt.deployer.state_backend import (
    StateAddress,
    StateBackendInvalidStateError,
    StateBackendUnavailableError,
)


@pytest.fixture(autouse=True)
def _clear_libpq_endpoint_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in postgres_state._LIBPQ_ENDPOINT_ENVIRONMENT_VARIABLES:
        monkeypatch.setenv(name, "streamt-test-sentinel")
        monkeypatch.delenv(name)


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
        quoted = ".".join(f'"{value}"' for value in values)
        return _FakeComposable(quoted)


class _FakeCursor:
    def __init__(
        self,
        *,
        recovery_rows: object = ((False,),),
        address_rows: object = (),
        probe_rows: object = ((True,),),
        fail_pattern: str | None = None,
        failure: BaseException | None = None,
        close_failure: Exception | None = None,
    ) -> None:
        self.recovery_rows = recovery_rows
        self.address_rows = address_rows
        self.probe_rows = probe_rows
        self.fail_pattern = fail_pattern
        self.failure = failure
        self.close_failure = close_failure
        self.current: object = ()
        self.calls: list[tuple[str, tuple[object, ...] | None]] = []
        self.closed = False

    def execute(
        self,
        query: object,
        params: tuple[object, ...] | None = None,
    ) -> object:
        rendered = str(query)
        self.calls.append((rendered, params))
        if self.fail_pattern is not None and self.fail_pattern in rendered:
            if self.failure is None:
                raise RuntimeError("provider-secret host=db.internal")
            raise self.failure
        if "pg_is_in_recovery" in rendered:
            self.current = self.recovery_rows
        elif '"streamt"."state_addresses"' in rendered:
            self.current = self.address_rows
        elif "pg_try_advisory_xact_lock" in rendered:
            self.current = self.probe_rows
        elif rendered.startswith("BEGIN") or "set_config(" in rendered:
            self.current = ()
        else:
            raise AssertionError(f"unexpected SQL boundary: {rendered}")
        return self

    def fetchall(self) -> object:
        return self.current

    def close(self) -> None:
        self.closed = True
        if self.close_failure is not None:
            raise self.close_failure


class _FakeConnection:
    def __init__(
        self,
        cursor: _FakeCursor,
        *,
        rollback_failure: Exception | None = None,
        close_failure: Exception | None = None,
    ) -> None:
        self._cursor = cursor
        self.rollback_failure = rollback_failure
        self.close_failure = close_failure
        self.rolled_back = False
        self.closed = False

    def cursor(self) -> _FakeCursor:
        return self._cursor

    def rollback(self) -> None:
        self.rolled_back = True
        if self.rollback_failure is not None:
            raise self.rollback_failure

    def close(self) -> None:
        self.closed = True
        if self.close_failure is not None:
            raise self.close_failure


class _FakeDriver:
    def __init__(self, connection: _FakeConnection) -> None:
        self.connection = connection
        self.calls: list[tuple[str, dict[str, object]]] = []

    def connect(self, conninfo: str, **kwargs: object) -> _FakeConnection:
        self.calls.append((conninfo, kwargs))
        return self.connection


def _address() -> StateAddress:
    return StateAddress(namespace="platform", project="payments", environment="prod")


def _status(*, registered: bool, store_id: str | None = None) -> PostgresStateStatus:
    if store_id is None and registered:
        store_id = "00000000-0000-4000-8000-000000000001"
    return PostgresStateStatus(
        store_status="ready" if store_id is not None else "uninitialized",
        store_id=store_id,
        schema_version=1 if store_id is not None else None,
        address=_address(),
        address_status="registered" if registered else "unregistered",
        state_status="absent" if registered else "unregistered",
        state_serial=0 if registered else None,
        state_checksum=None,
        operation_status=None,
    )


def _probe(
    *,
    dsn: str = "host=/var/run/postgresql dbname=state",
) -> PostgresStateLockProbe:
    return PostgresStateLockProbe(
        dsn=dsn,
        schema="streamt",
        lock_timeout_seconds=17,
    )


def _install_fake(
    monkeypatch: pytest.MonkeyPatch,
    *,
    status: PostgresStateStatus,
    cursor: _FakeCursor | None = None,
    connection: _FakeConnection | None = None,
) -> tuple[_FakeDriver, _FakeConnection, _FakeCursor, list[tuple[object, ...]]]:
    installed_cursor = cursor or _FakeCursor()
    installed_connection = connection or _FakeConnection(installed_cursor)
    driver = _FakeDriver(installed_connection)
    bundle = postgres_state._PsycopgBundle(
        driver=cast(postgres_state._DriverModule, driver),
        sql=cast(postgres_state._SqlModule, _FakeSql()),
    )
    monkeypatch.setattr(postgres_state, "_load_psycopg", lambda: bundle)
    status_calls: list[tuple[object, ...]] = []

    def read_status(
        _administration: PostgresStateAdministration,
        status_cursor: object,
        sql_module: object,
        address: StateAddress,
    ) -> PostgresStateStatus:
        status_calls.append((status_cursor, sql_module, address))
        return status

    monkeypatch.setattr(
        postgres_state.PostgresStateAdministration,
        "_read_status",
        read_status,
    )
    return driver, installed_connection, installed_cursor, status_calls


@pytest.mark.parametrize(
    ("probe_rows", "expected"),
    [
        (((True,),), "available"),
        (((False,),), "busy"),
    ],
)
def test_registered_probe_uses_one_parameterized_xact_try_and_rollback_release(
    monkeypatch: pytest.MonkeyPatch,
    probe_rows: object,
    expected: str,
) -> None:
    address = _address()
    lock_key = postgres_state._advisory_lock_key(address)
    cursor = _FakeCursor(
        address_rows=((address.uri, lock_key),),
        probe_rows=probe_rows,
    )
    driver, connection, cursor, status_calls = _install_fake(
        monkeypatch,
        status=_status(registered=True),
        cursor=cursor,
    )

    result = _probe(dsn="postgresql://db.internal/state").probe(address)

    assert result.lock_status == expected
    assert result.to_dict() == {
        "backend": "postgres",
        "store_id": "00000000-0000-4000-8000-000000000001",
        "address": address.uri,
        "lock_status": expected,
        "reservation": "none",
        "ordinary_state_authority": "disabled",
    }
    assert driver.calls == [
        (
            "postgresql://db.internal/state",
            {"connect_timeout": 10, "sslmode": "require"},
        )
    ]
    assert cursor.calls[:4] == [
        ("BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY", None),
        (
            "SELECT pg_catalog.set_config('search_path', 'pg_catalog', true)",
            None,
        ),
        (
            "SELECT pg_catalog.set_config('statement_timeout', %s, true)",
            ("30000ms",),
        ),
        ("SELECT pg_catalog.set_config('lock_timeout', %s, true)", ("17000ms",)),
    ]
    assert len(status_calls) == 1
    assert status_calls[0][0] is cursor
    assert status_calls[0][2] == address
    probe_call = next(
        call for call in cursor.calls if "pg_try_advisory_xact_lock" in call[0]
    )
    assert probe_call[0] == "SELECT pg_catalog.pg_try_advisory_xact_lock(%s)"
    assert probe_call[1] == (lock_key,)
    assert str(lock_key) not in probe_call[0]
    assert connection.rolled_back is True
    assert connection.closed is True
    assert cursor.closed is True


@pytest.mark.parametrize(
    ("status", "expected_store_id"),
    [
        (_status(registered=False), None),
        (
            _status(
                registered=False,
                store_id="00000000-0000-4000-8000-000000000001",
            ),
            "00000000-0000-4000-8000-000000000001",
        ),
    ],
)
def test_unregistered_probe_never_calls_an_advisory_function(
    monkeypatch: pytest.MonkeyPatch,
    status: PostgresStateStatus,
    expected_store_id: str | None,
) -> None:
    _driver, _connection, cursor, _status_calls = _install_fake(
        monkeypatch,
        status=status,
    )

    result = _probe().probe(_address())

    assert result.lock_status == "unregistered"
    assert result.store_id == expected_store_id
    assert not any("pg_try_advisory" in query for query, _params in cursor.calls)
    assert not any("state_addresses" in query for query, _params in cursor.calls)


def test_standby_is_unavailable_before_any_advisory_call(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cursor = _FakeCursor(recovery_rows=((True,),))
    _install_fake(
        monkeypatch,
        status=_status(registered=True),
        cursor=cursor,
    )

    with pytest.raises(
        StateBackendUnavailableError,
        match=r"^PostgreSQL deployment state lock probe is unavailable$",
    ) as raised:
        _probe().probe(_address())

    assert raised.value.__cause__ is None
    assert not any("pg_try_advisory" in query for query, _params in cursor.calls)


def test_invalid_registered_mapping_is_state_invalid(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cursor = _FakeCursor(address_rows=((_address().uri, 123),))
    _install_fake(
        monkeypatch,
        status=_status(registered=True),
        cursor=cursor,
    )

    with pytest.raises(
        StateBackendInvalidStateError,
        match=r"^PostgreSQL deployment state is invalid$",
    ) as raised:
        _probe().probe(_address())

    assert raised.value.__cause__ is None
    assert not any("pg_try_advisory" in query for query, _params in cursor.calls)


@pytest.mark.parametrize(
    "probe_rows",
    [
        ((True, False),),
        ((True, True),),
        (),
        "provider-secret host=db.internal",
    ],
)
def test_malformed_or_unverified_release_is_sanitized_unavailable(
    monkeypatch: pytest.MonkeyPatch,
    probe_rows: object,
) -> None:
    address = _address()
    cursor = _FakeCursor(
        address_rows=((address.uri, postgres_state._advisory_lock_key(address)),),
        probe_rows=probe_rows,
    )
    _install_fake(
        monkeypatch,
        status=_status(registered=True),
        cursor=cursor,
    )

    with pytest.raises(StateBackendUnavailableError) as raised:
        _probe().probe(address)

    assert str(raised.value) == "PostgreSQL deployment state lock probe is unavailable"
    assert "provider-secret" not in str(raised.value)
    assert "db.internal" not in str(raised.value)
    assert raised.value.__cause__ is None


def test_provider_failure_is_sanitized_and_connection_is_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cursor = _FakeCursor(
        fail_pattern="pg_is_in_recovery",
        failure=RuntimeError("postgresql://alice:secret@db.internal/state"),
    )
    _driver, connection, cursor, _status_calls = _install_fake(
        monkeypatch,
        status=_status(registered=True),
        cursor=cursor,
    )

    with pytest.raises(StateBackendUnavailableError) as raised:
        _probe().probe(_address())

    message = str(raised.value)
    assert "alice" not in message
    assert "secret" not in message
    assert "db.internal" not in message
    assert raised.value.__cause__ is None
    assert connection.closed is True
    assert cursor.closed is True


def test_cleanup_failure_prevents_reporting_a_released_result(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    address = _address()
    cursor = _FakeCursor(
        address_rows=((address.uri, postgres_state._advisory_lock_key(address)),),
        probe_rows=((True,),),
        close_failure=RuntimeError("cursor cleanup failed"),
    )
    connection = _FakeConnection(
        cursor,
        rollback_failure=RuntimeError("rollback cleanup failed"),
        close_failure=RuntimeError("connection cleanup failed"),
    )
    _install_fake(
        monkeypatch,
        status=_status(registered=True),
        cursor=cursor,
        connection=connection,
    )

    with pytest.raises(StateBackendUnavailableError):
        _probe().probe(address)

    assert cursor.closed is True
    assert connection.rolled_back is True
    assert connection.closed is True


def test_cancellation_still_closes_the_fresh_session(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    address = _address()
    cursor = _FakeCursor(
        address_rows=((address.uri, postgres_state._advisory_lock_key(address)),),
        fail_pattern="pg_try_advisory_xact_lock",
        failure=KeyboardInterrupt(),
    )
    _driver, connection, cursor, _status_calls = _install_fake(
        monkeypatch,
        status=_status(registered=True),
        cursor=cursor,
    )

    with pytest.raises(KeyboardInterrupt):
        _probe().probe(address)

    assert cursor.closed is True
    assert connection.rolled_back is True
    assert connection.closed is True


def test_full_status_validation_failure_stays_state_invalid(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cursor = _FakeCursor()
    connection = _FakeConnection(cursor)
    driver = _FakeDriver(connection)
    bundle = postgres_state._PsycopgBundle(
        driver=cast(postgres_state._DriverModule, driver),
        sql=cast(postgres_state._SqlModule, _FakeSql()),
    )
    monkeypatch.setattr(postgres_state, "_load_psycopg", lambda: bundle)
    monkeypatch.setattr(
        postgres_state.PostgresStateAdministration,
        "_read_status",
        lambda *_args: (_ for _ in ()).throw(
            StateBackendInvalidStateError("schema=private provider-secret")
        ),
    )

    with pytest.raises(StateBackendInvalidStateError) as raised:
        _probe().probe(_address())

    assert str(raised.value) == "PostgreSQL deployment state is invalid"
    assert "private" not in str(raised.value)
    assert "provider-secret" not in str(raised.value)
    assert raised.value.__cause__ is None


def test_factory_is_separate_secret_free_and_never_falls_back(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = validate_deployment_state_config(
        {
            "backend": "postgres",
            "namespace": "platform",
            "lock_timeout_seconds": 19,
            "postgres": {
                "dsn_env": "PRIVATE_LOCK_PROBE_DSN",
                "schema": "streamt_private",
            },
        }
    )
    assert isinstance(config, PostgresDeploymentStateConfig)
    monkeypatch.setenv(
        "PRIVATE_LOCK_PROBE_DSN",
        "postgresql://alice:secret@db.internal/state",
    )

    probe = make_postgres_state_lock_probe(config)

    assert isinstance(probe, PostgresStateLockProbe)
    representation = repr(probe)
    assert "alice" not in representation
    assert "secret" not in representation
    assert "db.internal" not in representation
    assert not hasattr(probe, "read")
    assert not hasattr(probe, "acquire")
    assert not hasattr(probe, "initialize")

    with pytest.raises(StateBackendUnavailableError):
        make_postgres_state_lock_probe(local_deployment_state_config())

    monkeypatch.delenv("PRIVATE_LOCK_PROBE_DSN")
    with pytest.raises(StateBackendUnavailableError) as raised:
        make_postgres_state_lock_probe(config)
    assert "PRIVATE_LOCK_PROBE_DSN" not in str(raised.value)
