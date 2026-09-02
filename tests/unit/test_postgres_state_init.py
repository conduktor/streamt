"""Explicit PostgreSQL deployment-state initialization contracts."""

from __future__ import annotations

import os
import re
import uuid
from collections.abc import Iterator
from typing import cast

import pytest

import streamt.deployer.postgres_state as postgres_state
from streamt.core.deployment_state import (
    local_deployment_state_config,
    validate_deployment_state_config,
)
from streamt.deployer.postgres_state import (
    POSTGRES_SCHEMA_V2_VERSION,
    POSTGRES_SCHEMA_VERSION,
    PostgresStateAdministration,
    PostgresStateInitialization,
    PostgresStateInitializer,
    PostgresStateStatus,
    SafeOperationStatus,
    make_postgres_state_initializer,
)
from streamt.deployer.state_backend import (
    StateAddress,
    StateBackendInvalidStateError,
    StateBackendUnavailableError,
    StateBackendUnknownCommitError,
)


@pytest.fixture(autouse=True)
def _clear_libpq_endpoint_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in postgres_state._LIBPQ_ENDPOINT_ENVIRONMENT_VARIABLES:
        monkeypatch.delenv(name, raising=False)


class _FakeComposable:
    def __init__(self, value: str) -> None:
        self.value = value

    def format(self, *args: object) -> _FakeComposable:
        return _FakeComposable(self.value.format(*(str(argument) for argument in args)))

    def __str__(self) -> str:
        return self.value


class _FakeSql:
    def __init__(self) -> None:
        self.identifiers: list[tuple[str, ...]] = []

    def SQL(self, value: str) -> _FakeComposable:  # noqa: N802
        return _FakeComposable(value)

    def Identifier(self, *values: str) -> _FakeComposable:  # noqa: N802
        self.identifiers.append(values)
        quoted = ".".join(f'"{value.replace(chr(34), chr(34) * 2)}"' for value in values)
        return _FakeComposable(quoted)


class _FakeCursor:
    def __init__(
        self,
        *,
        schema_rows: list[tuple[object, ...]] | None = None,
        collision_rows: list[tuple[object, ...]] | None = None,
        fail_pattern: str | None = None,
        close_error: Exception | None = None,
    ) -> None:
        self.schema_rows = schema_rows or []
        self.collision_rows = collision_rows or []
        self.fail_pattern = fail_pattern
        self.close_error = close_error
        self.current: list[tuple[object, ...]] = []
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
            raise RuntimeError("provider-secret SQL failure")
        if "FROM pg_catalog.pg_namespace" in rendered:
            self.current = list(self.schema_rows)
        elif "WHERE advisory_lock_key = %s" in rendered:
            self.current = list(self.collision_rows)
        elif "pg_advisory_unlock" in rendered:
            self.current = [(True,)]
        else:
            self.current = []
        return self

    def fetchall(self) -> object:
        return list(self.current)

    def close(self) -> None:
        self.closed = True
        if self.close_error is not None:
            raise self.close_error


class _FakeConnection:
    def __init__(
        self,
        cursor: _FakeCursor,
        *,
        commit_error: Exception | None = None,
        close_error: Exception | None = None,
    ) -> None:
        self._cursor = cursor
        self.commit_error = commit_error
        self.close_error = close_error
        self.commits = 0
        self.rollbacks = 0
        self.closed = False

    def cursor(self) -> _FakeCursor:
        return self._cursor

    def commit(self) -> None:
        self.commits += 1
        if self.commit_error is not None:
            raise self.commit_error

    def rollback(self) -> None:
        self.rollbacks += 1

    def close(self) -> None:
        self.closed = True
        if self.close_error is not None:
            raise self.close_error


class _FakeDriver:
    def __init__(self, connections: list[_FakeConnection]) -> None:
        self.connections = list(connections)
        self.calls: list[tuple[str, dict[str, object]]] = []

    def connect(self, conninfo: str, **kwargs: object) -> _FakeConnection:
        self.calls.append((conninfo, kwargs))
        if not self.connections:
            raise AssertionError("initializer opened an unexpected connection")
        return self.connections.pop(0)


def _address() -> StateAddress:
    return StateAddress(namespace="platform", project="payments", environment="prod")


def _clear_operation() -> SafeOperationStatus:
    return SafeOperationStatus(
        status="clear",
        operation_id=None,
        kind=None,
        failure_code=None,
        last_completed_action_index=None,
    )


def _uninitialized(address: StateAddress) -> PostgresStateStatus:
    return PostgresStateStatus(
        store_status="uninitialized",
        store_id=None,
        schema_version=None,
        address=address,
        address_status="unregistered",
        state_status="unregistered",
        state_serial=None,
        state_checksum=None,
        operation_status=None,
    )


def _ready(
    address: StateAddress,
    *,
    store_id: str = "00000000-0000-4000-8000-000000000001",
    registered: bool = True,
    state_status: str = "absent",
    operation: SafeOperationStatus | None = None,
) -> PostgresStateStatus:
    if not registered:
        return PostgresStateStatus(
            store_status="ready",
            store_id=store_id,
            schema_version=POSTGRES_SCHEMA_VERSION,
            address=address,
            address_status="unregistered",
            state_status="unregistered",
            state_serial=None,
            state_checksum=None,
            operation_status=None,
        )
    return PostgresStateStatus(
        store_status="ready",
        store_id=store_id,
        schema_version=POSTGRES_SCHEMA_VERSION,
        address=address,
        address_status="registered",
        state_status=cast(postgres_state.OwnershipStatus, state_status),
        state_serial=0 if state_status == "absent" else 1,
        state_checksum="sha256:" + "1" * 64,
        operation_status=operation or _clear_operation(),
    )


def _initializer() -> PostgresStateInitializer:
    return PostgresStateInitializer(
        dsn="host=/var/run/postgresql dbname=state",
        schema="streamt",
        lock_timeout_seconds=17,
    )


def _install_fake(
    monkeypatch: pytest.MonkeyPatch,
    *,
    initial_status: PostgresStateStatus,
    verified_status: PostgresStateStatus,
    precommit_status: PostgresStateStatus | None = None,
    schema_rows: list[tuple[object, ...]] | None = None,
    collision_rows: list[tuple[object, ...]] | None = None,
    primary_cursor: _FakeCursor | None = None,
    primary_connection: _FakeConnection | None = None,
) -> tuple[_FakeDriver, _FakeSql, _FakeConnection, _FakeCursor, _FakeConnection]:
    cursor = primary_cursor or _FakeCursor(
        schema_rows=schema_rows,
        collision_rows=collision_rows,
    )
    primary = primary_connection or _FakeConnection(cursor)
    verification_cursor = _FakeCursor()
    verification = _FakeConnection(verification_cursor)
    driver = _FakeDriver([primary, verification])
    sql_module = _FakeSql()
    bundle = postgres_state._PsycopgBundle(
        driver=cast(postgres_state._DriverModule, driver),
        sql=cast(postgres_state._SqlModule, sql_module),
    )
    monkeypatch.setattr(postgres_state, "_load_psycopg", lambda: bundle)
    statuses: Iterator[PostgresStateStatus] = iter(
        [initial_status, precommit_status or verified_status, verified_status]
    )
    monkeypatch.setattr(
        PostgresStateAdministration,
        "_read_status",
        lambda _self, _cursor, _sql, _address: next(statuses),
    )
    return driver, sql_module, primary, cursor, verification


def _rendered(cursor: _FakeCursor) -> list[str]:
    return [query for query, _params in cursor.calls]


def test_schema_v1_ddl_matches_the_frozen_columns_constraints_and_indexes() -> None:
    assert tuple(table for table, _template, _reference in postgres_state._SCHEMA_V1_DDL) == (
        "store_metadata",
        "schema_migrations",
        "state_addresses",
        "current_state",
        "operation_control",
        "state_history",
        "operation_history",
    )
    templates = {
        table: template.replace("{{", "{").replace("}}", "}")
        for table, template, _reference in postgres_state._SCHEMA_V1_DDL
    }
    constraint_names = {
        name
        for template in templates.values()
        for name in re.findall(r"\bCONSTRAINT\s+([a-z0-9_]+)", template)
    }
    assert constraint_names == {
        constraint[1] for constraint in postgres_state._EXPECTED_CONSTRAINTS
    }
    index_names = {index[1] for index in postgres_state._EXPECTED_INDEXES}
    assert index_names <= constraint_names
    for table, column, data_type, _udt, nullable, default in (
        postgres_state._EXPECTED_COLUMNS
    ):
        assert default is None
        suffix = " NOT NULL" if nullable == "NO" else ""
        assert re.search(
            rf"^\s*{re.escape(column)}\s+{re.escape(data_type)}{suffix}(?:,|$)",
            templates[table],
            re.MULTILINE,
        )
    rendered = "\n".join(templates.values())
    assert "DEFAULT" not in rendered
    assert "IF NOT EXISTS" not in rendered
    assert "GRANT" not in rendered
    assert "[0-9a-f]{64}" in rendered


def test_absent_schema_is_created_atomically_and_verified_on_fresh_connection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    address = _address()
    store_uuid = uuid.UUID("00000000-0000-4000-8000-000000000042")
    monkeypatch.setattr(postgres_state.uuid, "uuid4", lambda: store_uuid)
    driver, sql_module, primary, cursor, verification = _install_fake(
        monkeypatch,
        initial_status=_uninitialized(address),
        verified_status=_ready(address, store_id=str(store_uuid)),
    )

    result = _initializer().initialize(address)

    assert result == PostgresStateInitialization(
        store_id=str(store_uuid),
        address=address,
        created_store=True,
        registered_address=True,
    )
    assert result.outcome == "initialized"
    assert result.to_dict()["ordinary_state_authority"] == "disabled"
    rendered = _rendered(cursor)
    assert rendered[:2] == [
        "SELECT pg_catalog.set_config('statement_timeout', %s, false)",
        "SELECT pg_catalog.set_config('lock_timeout', %s, false)",
    ]
    advisory_index = next(i for i, query in enumerate(rendered) if "pg_advisory_lock" in query)
    transaction_index = rendered.index(
        "BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE READ WRITE"
    )
    schema_read_index = next(i for i, query in enumerate(rendered) if "FROM pg_catalog" in query)
    assert advisory_index < transaction_index < schema_read_index
    assert any(
        "pg_catalog.set_config('search_path', 'pg_catalog', true)" in query
        for query in rendered
    )
    assert "CREATE SCHEMA \"streamt\"" in rendered
    create_tables = [query for query in rendered if query.lstrip().startswith("CREATE TABLE")]
    assert len(create_tables) == 7
    assert all('"streamt".' in query for query in create_tables)
    assert "REVOKE ALL ON SCHEMA \"streamt\" FROM PUBLIC" in rendered
    assert sum(query.startswith("REVOKE ALL ON TABLE") for query in rendered) == 7
    assert not any("IF NOT EXISTS" in query or " GRANT " in query for query in rendered)
    inserts = [query for query in rendered if query.startswith("INSERT INTO")]
    assert len(inserts) == 4
    assert not any('"current_state"' in query or '"history"' in query for query in inserts)
    metadata_call = next(
        call
        for call in cursor.calls
        if call[0].startswith("INSERT INTO") and '"streamt"."store_metadata"' in call[0]
    )
    assert metadata_call[1] is not None
    assert metadata_call[1][:3] == (True, store_uuid, 1)
    control_call = next(
        call
        for call in cursor.calls
        if call[0].startswith("INSERT INTO") and '"operation_control"' in call[0]
    )
    assert control_call[1] is not None
    assert control_call[1][3:6] == (
        0,
        "clear",
        postgres_state._canonical_json(
            postgres_state.OperationControlState.clear(address).to_dict()
        ),
    )
    assert primary.commits == 1
    assert primary.rollbacks == 0
    assert primary.closed is True
    assert verification.commits == 0
    assert verification.rollbacks == 1
    assert verification.closed is True
    assert driver.calls == [
        (
            "host=/var/run/postgresql dbname=state",
            {"autocommit": True, "connect_timeout": 10, "sslmode": "prefer"},
        ),
        (
            "host=/var/run/postgresql dbname=state",
            {"connect_timeout": 10, "sslmode": "prefer"},
        ),
    ]
    assert ("streamt",) in sql_module.identifiers
    assert ("store_metadata",) in sql_module.identifiers


def test_existing_empty_schema_is_initialized_without_recreating_schema(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    address = _address()
    store_uuid = uuid.UUID("00000000-0000-4000-8000-000000000043")
    monkeypatch.setattr(postgres_state.uuid, "uuid4", lambda: store_uuid)
    _driver, _sql, primary, cursor, _verification = _install_fake(
        monkeypatch,
        initial_status=_uninitialized(address),
        verified_status=_ready(address, store_id=str(store_uuid)),
        schema_rows=[("streamt", True)],
    )

    result = _initializer().initialize(address)

    assert result.created_store is True
    assert not any(query.startswith("CREATE SCHEMA") for query in _rendered(cursor))
    assert sum(query.lstrip().startswith("CREATE TABLE") for query in _rendered(cursor)) == 7
    assert primary.commits == 1


def test_compatible_store_registers_only_the_new_address(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    address = _address()
    existing = _ready(address, registered=False)
    driver, _sql, primary, cursor, _verification = _install_fake(
        monkeypatch,
        initial_status=existing,
        verified_status=_ready(address),
        schema_rows=[("streamt", True)],
    )
    monkeypatch.setattr(
        postgres_state.uuid,
        "uuid4",
        lambda: (_ for _ in ()).throw(AssertionError("replaced immutable store ID")),
    )

    result = _initializer().initialize(address)

    assert result.store_id == existing.store_id
    assert result.created_store is False
    assert result.registered_address is True
    assert result.outcome == "address_registered"
    rendered = _rendered(cursor)
    assert not any(query.startswith("CREATE") for query in rendered)
    inserts = [query for query in rendered if query.startswith("INSERT INTO")]
    assert len(inserts) == 2
    assert all(
        '"state_addresses"' in query or '"operation_control"' in query for query in inserts
    )
    assert primary.commits == 1
    assert len(driver.calls) == 2


def test_same_compatible_store_and_address_is_an_exact_idempotent_noop(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    address = _address()
    ready = _ready(address)
    collision = [
        (address.namespace, address.project, address.environment, address.uri)
    ]
    _driver, _sql, primary, cursor, _verification = _install_fake(
        monkeypatch,
        initial_status=ready,
        verified_status=ready,
        schema_rows=[("streamt", True)],
        collision_rows=collision,
    )

    first = _initializer().initialize(address)

    assert first.store_id == ready.store_id
    assert first.created_store is False
    assert first.registered_address is False
    assert first.outcome == "already_initialized"
    assert not any(
        query.startswith("CREATE") or query.startswith("INSERT")
        for query in _rendered(cursor)
    )
    assert primary.commits == 1


@pytest.mark.parametrize("case", ["partial", "owned", "active"])
def test_partial_or_nonempty_compatible_target_fails_without_commit(
    case: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    address = _address()
    if case == "owned":
        initial = _ready(address, state_status="present")
    elif case == "active":
        initial = _ready(
            address,
            operation=SafeOperationStatus(
                status="in_progress",
                operation_id="00000000-0000-4000-8000-000000000099",
                kind="apply",
                failure_code=None,
                last_completed_action_index=None,
            ),
        )
    else:
        initial = _uninitialized(address)
    collision = [
        (address.namespace, address.project, address.environment, address.uri)
    ]
    cursor = _FakeCursor(schema_rows=[("streamt", True)], collision_rows=collision)
    primary = _FakeConnection(cursor)
    verification = _FakeConnection(_FakeCursor())
    driver = _FakeDriver([primary, verification])
    sql_module = _FakeSql()
    monkeypatch.setattr(
        postgres_state,
        "_load_psycopg",
        lambda: postgres_state._PsycopgBundle(
            driver=cast(postgres_state._DriverModule, driver),
            sql=cast(postgres_state._SqlModule, sql_module),
        ),
    )
    if case == "partial":
        monkeypatch.setattr(
            PostgresStateAdministration,
            "_read_status",
            lambda *_args: (_ for _ in ()).throw(
                StateBackendInvalidStateError("provider-secret partial schema")
            ),
        )
    else:
        monkeypatch.setattr(
            PostgresStateAdministration,
            "_read_status",
            lambda *_args: initial,
        )

    with pytest.raises(StateBackendInvalidStateError) as raised:
        _initializer().initialize(address)

    assert "provider-secret" not in str(raised.value)
    assert raised.value.__cause__ is None
    assert primary.commits == 0
    assert primary.rollbacks == 1
    assert primary.closed is True
    assert len(driver.calls) == 1


def test_advisory_lock_collision_fails_without_address_or_control_insert(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    address = _address()
    collision = [("other", "project", "prod", "streamt-state://other/project/prod")]
    _driver, _sql, primary, cursor, _verification = _install_fake(
        monkeypatch,
        initial_status=_ready(address, registered=False),
        verified_status=_ready(address),
        schema_rows=[("streamt", True)],
        collision_rows=collision,
    )

    with pytest.raises(StateBackendInvalidStateError):
        _initializer().initialize(address)

    assert not any(query.startswith("INSERT") for query in _rendered(cursor))
    assert primary.commits == 0
    assert primary.rollbacks == 1


def test_ambiguous_commit_is_not_retried_or_verified_and_is_secret_neutral(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    address = _address()
    cursor = _FakeCursor(schema_rows=[("streamt", True)])
    primary = _FakeConnection(
        cursor,
        commit_error=RuntimeError("provider-secret commit response"),
    )
    driver, _sql, _primary, _cursor, _verification = _install_fake(
        monkeypatch,
        initial_status=_ready(address, registered=False),
        verified_status=_ready(address),
        primary_cursor=cursor,
        primary_connection=primary,
    )

    with pytest.raises(StateBackendUnknownCommitError) as raised:
        _initializer().initialize(address)

    assert "provider-secret" not in str(raised.value)
    assert "state status" in str(raised.value)
    assert raised.value.__cause__ is None
    assert primary.commits == 1
    assert primary.rollbacks == 0
    assert primary.closed is True
    assert len(driver.calls) == 1


def test_precommit_provider_failure_rolls_back_and_is_not_reported_as_unknown(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    address = _address()
    cursor = _FakeCursor(schema_rows=[("streamt", True)], fail_pattern="INSERT INTO")
    primary = _FakeConnection(cursor)
    driver, _sql, _primary, _cursor, _verification = _install_fake(
        monkeypatch,
        initial_status=_ready(address, registered=False),
        verified_status=_ready(address),
        primary_cursor=cursor,
        primary_connection=primary,
    )

    with pytest.raises(StateBackendUnavailableError) as raised:
        _initializer().initialize(address)

    assert "provider-secret" not in str(raised.value)
    assert raised.value.__cause__ is None
    assert primary.commits == 0
    assert primary.rollbacks == 1
    assert len(driver.calls) == 1


def test_postcommit_verification_mismatch_fails_closed_without_second_commit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    address = _address()
    wrong_store = _ready(
        address,
        store_id="00000000-0000-4000-8000-000000000002",
    )
    driver, _sql, primary, _cursor, verification = _install_fake(
        monkeypatch,
        initial_status=_ready(address, registered=False),
        precommit_status=_ready(address),
        verified_status=wrong_store,
        schema_rows=[("streamt", True)],
    )

    with pytest.raises(StateBackendInvalidStateError):
        _initializer().initialize(address)

    assert primary.commits == 1
    assert verification.commits == 0
    assert verification.rollbacks == 1
    assert len(driver.calls) == 2


def test_precommit_verification_mismatch_rolls_back_without_committing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    address = _address()
    wrong_store = _ready(
        address,
        store_id="00000000-0000-4000-8000-000000000002",
    )
    driver, _sql, primary, _cursor, _verification = _install_fake(
        monkeypatch,
        initial_status=_ready(address, registered=False),
        precommit_status=wrong_store,
        verified_status=_ready(address),
        schema_rows=[("streamt", True)],
    )

    with pytest.raises(StateBackendInvalidStateError):
        _initializer().initialize(address)

    assert primary.commits == 0
    assert primary.rollbacks == 1
    assert len(driver.calls) == 1


@pytest.mark.parametrize("failure_surface", ["cursor", "connection"])
def test_cleanup_failure_after_known_commit_does_not_hide_durable_success(
    failure_surface: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    address = _address()
    cursor = _FakeCursor(
        schema_rows=[("streamt", True)],
        close_error=RuntimeError("provider-secret cursor close")
        if failure_surface == "cursor"
        else None,
    )
    primary = _FakeConnection(
        cursor,
        close_error=RuntimeError("provider-secret connection close")
        if failure_surface == "connection"
        else None,
    )
    driver, _sql, _primary, _cursor, _verification = _install_fake(
        monkeypatch,
        initial_status=_ready(address, registered=False),
        verified_status=_ready(address),
        primary_cursor=cursor,
        primary_connection=primary,
    )

    result = _initializer().initialize(address)

    assert result.outcome == "address_registered"
    assert primary.commits == 1
    assert len(driver.calls) == 2


def test_factory_and_initializer_remain_lazy_without_optional_driver(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = validate_deployment_state_config(
        {
            "backend": "postgres",
            "namespace": "platform",
            "postgres": {"dsn_env": "STREAMT_INIT_DSN"},
        }
    )
    monkeypatch.setenv("STREAMT_INIT_DSN", "host=/var/run/postgresql dbname=state")
    imports: list[str] = []

    def missing(name: str) -> object:
        imports.append(name)
        raise ModuleNotFoundError("provider-secret module")

    monkeypatch.setattr(postgres_state.importlib, "import_module", missing)

    initializer = make_postgres_state_initializer(config)
    assert imports == []
    with pytest.raises(StateBackendUnavailableError) as raised:
        initializer.initialize(_address())
    assert imports == ["psycopg"]
    assert "provider-secret" not in str(raised.value)
    assert raised.value.__cause__ is None


def test_initializer_factory_rejects_local_or_missing_credentials_without_driver(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with pytest.raises(StateBackendUnavailableError):
        make_postgres_state_initializer(local_deployment_state_config())

    config = validate_deployment_state_config(
        {
            "backend": "postgres",
            "namespace": "platform",
            "postgres": {"dsn_env": "MISSING_INIT_DSN"},
        }
    )
    monkeypatch.delenv("MISSING_INIT_DSN", raising=False)
    monkeypatch.setattr(
        postgres_state,
        "_load_psycopg",
        lambda: (_ for _ in ()).throw(AssertionError("driver loaded")),
    )

    with pytest.raises(StateBackendUnavailableError) as raised:
        make_postgres_state_initializer(config)

    assert "MISSING_INIT_DSN" not in str(raised.value)
    assert raised.value.__cause__ is None


@pytest.mark.parametrize("name", postgres_state._LIBPQ_ENDPOINT_ENVIRONMENT_VARIABLES)
def test_initializer_rejects_endpoint_environment_before_driver_loading(
    name: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(name, "provider-secret")
    monkeypatch.setattr(
        postgres_state,
        "_load_psycopg",
        lambda: (_ for _ in ()).throw(AssertionError("driver loaded first")),
    )

    with pytest.raises(StateBackendUnavailableError) as raised:
        _initializer().initialize(_address())

    assert "provider-secret" not in str(raised.value)


def test_initialization_lock_key_is_stable_per_schema_and_domain_separated() -> None:
    key = postgres_state._initialization_lock_key("streamt")
    assert key == postgres_state._initialization_lock_key("streamt")
    assert len(key) == 2
    assert all(-(2**31) <= word <= 2**31 - 1 for word in key)
    assert isinstance(postgres_state._advisory_lock_key(_address()), int)


def test_result_contains_only_safe_identity_and_disabled_authority() -> None:
    result = PostgresStateInitialization(
        store_id="00000000-0000-4000-8000-000000000001",
        address=_address(),
        created_store=False,
        registered_address=False,
    )
    serialized = str(result.to_dict())
    for secret in ("password", "host=", "dbname=", "advisory_lock_key"):
        assert secret not in serialized
    assert result.to_dict()["ordinary_state_authority"] == "disabled"
    assert "PGHOST" not in os.environ


def test_v2_address_registration_reports_released_catalog_capability() -> None:
    result = PostgresStateInitialization(
        store_id="00000000-0000-4000-8000-000000000001",
        address=_address(),
        created_store=False,
        registered_address=True,
        schema_version=POSTGRES_SCHEMA_V2_VERSION,
    )

    assert (
        result.to_dict()["ordinary_state_authority"]
        == "supported_for_v2_writer"
    )
