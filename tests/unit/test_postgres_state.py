"""Read-only PostgreSQL deployment-state administration contracts."""

from __future__ import annotations

import json
from pathlib import Path
from typing import cast

import pytest

import streamt.deployer.postgres_state as postgres_state
from streamt.core.deployment_state import (
    PostgresDeploymentStateConfig,
    local_deployment_state_config,
    validate_deployment_state_config,
)
from streamt.deployer.postgres_state import (
    POSTGRES_SCHEMA_V1_CHECKSUM,
    PostgresStateAdministration,
    make_postgres_state_administration,
)
from streamt.deployer.state import LocalState
from streamt.deployer.state_backend import (
    OperationControlState,
    StateAddress,
    StateBackendInvalidStateError,
    StateBackendUnavailableError,
    make_deployment_state_service,
    state_checksum,
)


@pytest.fixture(autouse=True)
def _clear_libpq_endpoint_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in postgres_state._LIBPQ_ENDPOINT_ENVIRONMENT_VARIABLES:
        monkeypatch.delenv(name, raising=False)


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
    def __init__(self) -> None:
        self.identifiers: list[tuple[str, ...]] = []

    def SQL(self, value: str) -> _FakeComposable:  # noqa: N802
        return _FakeComposable(value)

    def Identifier(self, *values: str) -> _FakeComposable:  # noqa: N802
        self.identifiers.append(values)
        quoted = ".".join(f'"{value.replace(chr(34), chr(34) * 2)}"' for value in values)
        return _FakeComposable(quoted)


def _constraint_rows() -> list[tuple[object, ...]]:
    result: list[tuple[object, ...]] = []
    for constraint in postgres_state._EXPECTED_CONSTRAINTS:
        kind = constraint[2]
        actions: tuple[object, object, object] = (
            ("a", "r", "s") if kind == "f" else (None, None, None)
        )
        result.append((*constraint, False, False, True, *actions, kind != "c", True))
    return result


def _index_rows() -> list[tuple[object, ...]]:
    return [
        (*index, None, None, True, True, True, False, True, False, "btree")
        for index in postgres_state._EXPECTED_INDEXES
    ]


def _canonical(value: dict[str, object]) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _base_responses(
    address: StateAddress,
    *,
    registered: bool = True,
    state: LocalState | None = None,
) -> dict[str, list[tuple[object, ...]]]:
    control_json = _canonical(OperationControlState.clear(address).to_dict())
    address_rows = [(address.uri, postgres_state._advisory_lock_key(address))] if registered else []
    state_rows: list[tuple[object, ...]] = []
    if state is not None:
        state_json = _canonical(state.to_dict())
        state_rows = [
            (1, state.serial, state_checksum(state), state_json, len(state_json.encode()))
        ]
    return {
        "schema": [("streamt", True)],
        "relations": [
            (table, "r", "p", False, False, False, True, True)
            for table in postgres_state._EXPECTED_TABLES
        ],
        "functions": [],
        "types": [],
        "schema_objects": [],
        "columns": list(postgres_state._EXPECTED_COLUMNS),
        "constraints": _constraint_rows(),
        "indexes": _index_rows(),
        "triggers": [],
        "policies": [],
        "metadata": [(True, "00000000-0000-4000-8000-000000000001", 1)],
        "migrations": [(1, "schema-v1", POSTGRES_SCHEMA_V1_CHECKSUM)],
        "address": address_rows,
        "control": [(0, "clear", control_json, len(control_json.encode()))],
        "state": state_rows,
    }


class _FakeCursor:
    def __init__(self, responses: dict[str, list[tuple[object, ...]]]) -> None:
        self.responses = responses
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
        key: str | None = None
        if "FROM pg_catalog.pg_constraint" in rendered:
            key = "constraints"
        elif "FROM pg_catalog.pg_index" in rendered:
            key = "indexes"
        elif "FROM pg_catalog.pg_trigger" in rendered:
            key = "triggers"
        elif "FROM pg_catalog.pg_policy" in rendered:
            key = "policies"
        elif "FROM pg_catalog.pg_collation" in rendered:
            key = "schema_objects"
        elif "FROM pg_catalog.pg_proc" in rendered:
            key = "functions"
        elif "FROM pg_catalog.pg_type" in rendered:
            key = "types"
        elif "FROM pg_catalog.pg_class AS c" in rendered:
            key = "relations"
        elif "FROM pg_catalog.pg_namespace" in rendered:
            key = "schema"
        elif "FROM information_schema.columns" in rendered:
            key = "columns"
        elif '"streamt"."store_metadata"' in rendered:
            key = "metadata"
        elif '"streamt"."schema_migrations"' in rendered:
            key = "migrations"
        elif '"streamt"."state_addresses"' in rendered:
            key = "address"
        elif '"streamt"."operation_control"' in rendered:
            key = "control"
        elif '"streamt"."current_state"' in rendered:
            key = "state"
        elif rendered.startswith("BEGIN") or "set_config(" in rendered:
            self.current = []
            return self
        else:
            raise AssertionError(f"unexpected SQL boundary: {rendered}")
        self.current = list(self.responses[key])
        return self

    def fetchall(self) -> object:
        return list(self.current)

    def close(self) -> None:
        self.closed = True


class _FakeConnection:
    def __init__(self, cursor: _FakeCursor) -> None:
        self._cursor = cursor
        self.rolled_back = False
        self.closed = False

    def cursor(self) -> _FakeCursor:
        return self._cursor

    def rollback(self) -> None:
        self.rolled_back = True

    def close(self) -> None:
        self.closed = True


class _FakeDriver:
    def __init__(self, connection: _FakeConnection) -> None:
        self.connection = connection
        self.calls: list[tuple[str, dict[str, object]]] = []

    def connect(self, conninfo: str, **kwargs: object) -> _FakeConnection:
        self.calls.append((conninfo, kwargs))
        return self.connection


def _install_fake(
    monkeypatch: pytest.MonkeyPatch,
    responses: dict[str, list[tuple[object, ...]]],
) -> tuple[_FakeDriver, _FakeSql, _FakeConnection, _FakeCursor]:
    cursor = _FakeCursor(responses)
    connection = _FakeConnection(cursor)
    driver = _FakeDriver(connection)
    sql_module = _FakeSql()
    bundle = postgres_state._PsycopgBundle(
        driver=cast(postgres_state._DriverModule, driver),
        sql=cast(postgres_state._SqlModule, sql_module),
    )
    monkeypatch.setattr(postgres_state, "_load_psycopg", lambda: bundle)
    return driver, sql_module, connection, cursor


def _address() -> StateAddress:
    return StateAddress(namespace="platform", project="payments", environment="prod")


def _administration(
    *,
    dsn: str = "host=/var/run/postgresql dbname=state",
) -> PostgresStateAdministration:
    return PostgresStateAdministration(
        dsn=dsn,
        schema="streamt",
        lock_timeout_seconds=17,
    )


def test_base_import_and_local_factory_do_not_load_optional_driver(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        postgres_state.importlib,
        "import_module",
        lambda _name: (_ for _ in ()).throw(AssertionError("loaded psycopg")),
    )

    service = make_deployment_state_service(
        tmp_path,
        project="payments",
        environment="prod",
        config=local_deployment_state_config(),
    )

    assert service.store.backend == "local"


def test_missing_optional_driver_is_a_safe_unchained_unavailable_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        postgres_state.importlib,
        "import_module",
        lambda _name: (_ for _ in ()).throw(ModuleNotFoundError("provider-secret")),
    )

    with pytest.raises(StateBackendUnavailableError) as raised:
        _administration().status(_address())

    assert "provider-secret" not in str(raised.value)
    assert raised.value.__cause__ is None


@pytest.mark.parametrize(
    ("dsn", "expected"),
    [
        (
            "postgresql://db.internal/state",
            {"sslmode": "require"},
        ),
        (
            "host=db.internal dbname=state",
            {"sslmode": "require"},
        ),
        (
            "postgresql://localhost/state?sslmode=disable",
            {"sslmode": "disable"},
        ),
        (
            "host=/var/run/postgresql dbname=state sslmode=disable",
            {"sslmode": "disable"},
        ),
        (
            "host=/var/run/postgresql dbname=state",
            {"sslmode": "prefer"},
        ),
        (
            "postgresql://db.internal/state?sslmode=verify-full",
            {"sslmode": "verify-full"},
        ),
        (
            "postgresql:///state?host=127.0.0.1&sslmode=disable",
            {"sslmode": "disable"},
        ),
        (
            "host=db.example hostaddr=127.0.0.1 dbname=state sslmode=disable",
            {"sslmode": "disable"},
        ),
        ("service=remote", {"sslmode": "require"}),
    ],
)
def test_tls_options_are_fail_closed(dsn: str, expected: dict[str, object]) -> None:
    assert postgres_state._dsn_tls_options(dsn) == expected


@pytest.mark.parametrize("sslmode", ["disable", "allow", "prefer"])
def test_weak_remote_tls_mode_is_rejected_without_dsn_detail(sslmode: str) -> None:
    dsn = f"postgresql://alice:secret@db.internal/state?sslmode={sslmode}"

    with pytest.raises(StateBackendUnavailableError) as raised:
        postgres_state._dsn_tls_options(dsn)

    assert "alice" not in str(raised.value)
    assert "secret" not in str(raised.value)
    assert "db.internal" not in str(raised.value)


@pytest.mark.parametrize(
    "dsn",
    [
        "postgresql:///state?host=db.internal&sslmode=disable",
        "postgresql:///state?hostaddr=10.0.0.8&sslmode=disable",
        "postgresql:///state?service=remote&sslmode=disable",
        "service=remote sslmode=prefer",
        "host=/var/run/postgresql service=remote sslmode=disable",
        "postgresql://localhost/state?service=remote&sslmode=disable",
        "postgresql://localhost:5432,db.internal:5432/state?sslmode=disable",
        "postgresql://db.internal/state?sslmode=unknown",
        "postgresql:///state",
        "dbname=state",
    ],
)
def test_dsn_override_and_multihost_tls_bypasses_fail_closed(dsn: str) -> None:
    with pytest.raises(StateBackendUnavailableError):
        postgres_state._dsn_tls_options(dsn)


def test_invalid_tls_configuration_is_rejected_before_driver_loading(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        postgres_state,
        "_load_psycopg",
        lambda: (_ for _ in ()).throw(AssertionError("driver loaded first")),
    )

    with pytest.raises(StateBackendUnavailableError):
        _administration(
            dsn="postgresql:///state?host=db.internal&sslmode=disable"
        ).status(_address())


def test_absent_schema_is_uninitialized_and_transaction_is_read_only(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    responses = {"schema": []}
    driver, _sql, connection, cursor = _install_fake(monkeypatch, responses)

    status = _administration(dsn="postgresql://db.internal/state").status(_address())

    assert status.to_dict() == {
        "backend": "postgres",
        "store_status": "uninitialized",
        "store_id": None,
        "schema_version": None,
        "address": _address().uri,
        "address_status": "unregistered",
        "state_status": "unregistered",
        "state_serial": None,
        "state_checksum": None,
        "operation_status": None,
        "mutation_status": "disabled",
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
    assert connection.rolled_back is True
    assert connection.closed is True
    assert cursor.closed is True


def test_nonempty_schema_object_without_tables_is_invalid(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    responses = {
        "schema": [("streamt", True)],
        "relations": [],
        "functions": [("provider_secret_function",)],
        "types": [],
        "schema_objects": [],
    }
    _install_fake(monkeypatch, responses)

    with pytest.raises(StateBackendInvalidStateError) as raised:
        _administration().status(_address())

    assert "provider_secret_function" not in str(raised.value)
    assert raised.value.__cause__ is None


def test_standalone_composite_relation_makes_empty_schema_invalid(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    responses = {
        "schema": [("streamt", True)],
        "relations": [
            ("unexpected_composite", "c", "p", False, False, False, True, True)
        ],
        "functions": [],
        "types": [],
        "schema_objects": [],
    }
    _install_fake(monkeypatch, responses)

    with pytest.raises(StateBackendInvalidStateError):
        _administration().status(_address())


def test_ready_unregistered_store_uses_composed_identifiers_and_bound_values(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    responses = _base_responses(_address(), registered=False)
    _driver, sql_module, _connection, cursor = _install_fake(monkeypatch, responses)

    status = _administration().status(_address())

    assert status.store_status == "ready"
    assert status.address_status == "unregistered"
    assert status.state_status == "unregistered"
    assert status.operation_status is None
    assert sql_module.identifiers
    assert set(sql_module.identifiers) >= {
        ("streamt",),
        ("store_metadata",),
        ("schema_migrations",),
        ("state_addresses",),
    }
    address_call = next(call for call in cursor.calls if '"streamt"."state_addresses"' in call[0])
    assert address_call[1] == ("platform", "payments", "prod")
    assert "platform" not in address_call[0]
    index_call = next(call for call in cursor.calls if "pg_catalog.pg_index" in call[0])
    assert "to_jsonb(i)->>'indnullsnotdistinct'" in index_call[0]
    constraint_call = next(
        call for call in cursor.calls if "pg_catalog.pg_constraint" in call[0]
    )
    assert "c.contype <> 'n'" in constraint_call[0]
    assert "to_jsonb(c)->>'conenforced'" in constraint_call[0]
    relation_call = next(
        call for call in cursor.calls if "FROM pg_catalog.pg_class AS c" in call[0]
    )
    assert "acl.grantee = 0" in relation_call[0]
    assert "acl.is_grantable" in relation_call[0]


@pytest.mark.parametrize("surface", ["schema_acl", "table_owner", "table_acl"])
def test_unsafe_acl_or_mixed_owner_catalog_is_not_ready(
    surface: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    responses = _base_responses(_address())
    if surface == "schema_acl":
        responses["schema"] = [("streamt", False)]
    else:
        relation = list(responses["relations"][0])
        relation[6 if surface == "table_owner" else 7] = False
        responses["relations"][0] = tuple(relation)
    _install_fake(monkeypatch, responses)

    with pytest.raises(StateBackendInvalidStateError):
        _administration().status(_address())


def test_registered_absent_state_reports_strict_empty_checksum(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    responses = _base_responses(_address())
    _install_fake(monkeypatch, responses)

    status = _administration().status(_address())

    assert status.state_status == "absent"
    assert status.state_serial == 0
    assert status.state_checksum == state_checksum(
        LocalState(project="payments", environment="prod")
    )
    assert status.operation_status is not None
    assert status.operation_status.to_dict() == {
        "status": "clear",
        "operation_id": None,
        "kind": None,
        "failure_code": None,
        "last_completed_action_index": None,
    }


def test_present_state_is_strictly_parsed_without_returning_resources(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state = LocalState(project="payments", environment="prod", serial=4)
    responses = _base_responses(_address(), state=state)
    _install_fake(monkeypatch, responses)

    status = _administration().status(_address())

    assert status.state_status == "present"
    assert status.state_serial == 4
    assert status.state_checksum == state_checksum(state)
    assert "resources" not in status.to_dict()


@pytest.mark.parametrize("broken_surface", ["constraints", "indexes", "triggers"])
def test_incomplete_or_extended_catalog_is_not_ready(
    monkeypatch: pytest.MonkeyPatch,
    broken_surface: str,
) -> None:
    responses = _base_responses(_address(), registered=False)
    if broken_surface == "constraints":
        responses[broken_surface] = responses[broken_surface][1:]
    elif broken_surface == "indexes":
        responses[broken_surface] = responses[broken_surface][:-1]
    else:
        responses[broken_surface] = [("state_addresses", "unsafe_trigger")]
    _install_fake(monkeypatch, responses)

    with pytest.raises(StateBackendInvalidStateError) as raised:
        _administration().status(_address())

    assert "unsafe_trigger" not in str(raised.value)


@pytest.mark.parametrize(
    ("surface", "replacement"),
    [
        ("metadata", [(False, "00000000-0000-4000-8000-000000000001", 1)]),
        ("address", [(_address().uri, True)]),
    ],
)
def test_singleton_and_advisory_identity_are_strict(
    monkeypatch: pytest.MonkeyPatch,
    surface: str,
    replacement: list[tuple[object, ...]],
) -> None:
    responses = _base_responses(_address())
    responses[surface] = replacement
    _install_fake(monkeypatch, responses)

    with pytest.raises(StateBackendInvalidStateError):
        _administration().status(_address())


def test_bool_byte_length_and_serial_zero_present_row_fail_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    responses = _base_responses(_address())
    raw = _canonical(LocalState(project="payments", environment="prod").to_dict())
    responses["state"] = [(1, 0, "sha256:" + "0" * 64, raw, True)]
    _install_fake(monkeypatch, responses)

    with pytest.raises(StateBackendInvalidStateError):
        _administration().status(_address())


def test_provider_failure_is_sanitized_unchained_and_never_returned(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _FailingDriver:
        def connect(self, _conninfo: str, **_kwargs: object) -> object:
            raise RuntimeError("postgresql://alice:secret@db.internal/state schema=private")

    bundle = postgres_state._PsycopgBundle(
        driver=cast(postgres_state._DriverModule, _FailingDriver()),
        sql=cast(postgres_state._SqlModule, _FakeSql()),
    )
    monkeypatch.setattr(postgres_state, "_load_psycopg", lambda: bundle)

    with pytest.raises(StateBackendUnavailableError) as raised:
        _administration(dsn="postgresql://db.internal/state").status(_address())

    message = str(raised.value)
    assert "alice" not in message
    assert "secret" not in message
    assert "db.internal" not in message
    assert "private" not in message
    assert raised.value.__cause__ is None


@pytest.mark.parametrize(
    ("name", "value"),
    [
        ("PGHOST", "hostile.internal"),
        ("PGHOSTADDR", "10.0.0.8"),
        ("PGPORT", "6543"),
        ("PGSERVICE", "hostile-service"),
        ("PGSERVICEFILE", "/private/provider-secret.conf"),
    ],
)
def test_endpoint_environment_is_rejected_before_driver_loading(
    name: str,
    value: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(name, value)
    monkeypatch.setattr(
        postgres_state,
        "_load_psycopg",
        lambda: (_ for _ in ()).throw(AssertionError("driver loaded first")),
    )

    with pytest.raises(StateBackendUnavailableError) as raised:
        _administration().status(_address())

    assert value not in str(raised.value)
    assert raised.value.__cause__ is None


def test_hostless_connection_is_rejected_before_driver_loading(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        postgres_state,
        "_load_psycopg",
        lambda: (_ for _ in ()).throw(AssertionError("driver loaded first")),
    )

    with pytest.raises(StateBackendUnavailableError) as raised:
        _administration(dsn="postgresql:///state").status(_address())

    assert raised.value.__cause__ is None


def test_tls_override_wins_over_libpq_tls_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("PGSSLMODE", "disable")
    driver, _sql, _connection, _cursor = _install_fake(monkeypatch, {"schema": []})

    _administration(dsn="postgresql://db.internal/state").status(_address())

    assert driver.calls == [
        (
            "postgresql://db.internal/state",
            {"connect_timeout": 10, "sslmode": "require"},
        )
    ]


def test_deep_json_is_invalid_instead_of_provider_unavailable() -> None:
    deep_json = "[" * 2_000 + "0" + "]" * 2_000

    with pytest.raises(StateBackendInvalidStateError):
        postgres_state._strict_json(deep_json, label="ownership state")


def test_factory_resolves_only_named_dsn_and_has_secret_free_repr(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = validate_deployment_state_config(
        {
            "backend": "postgres",
            "namespace": "platform",
            "postgres": {"dsn_env": "PRIVATE_STATE_DSN"},
        }
    )
    assert isinstance(config, PostgresDeploymentStateConfig)
    monkeypatch.setenv(
        "PRIVATE_STATE_DSN",
        "postgresql://alice:secret@db.internal/state",
    )

    administration = make_postgres_state_administration(config)

    representation = repr(administration)
    assert "alice" not in representation
    assert "secret" not in representation
    assert "db.internal" not in representation
    assert not hasattr(administration, "initialize")
    assert not hasattr(administration, "acquire")


def test_factory_rejects_local_and_missing_credentials_without_driver_load(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with pytest.raises(StateBackendUnavailableError):
        make_postgres_state_administration(local_deployment_state_config())

    config = validate_deployment_state_config(
        {
            "backend": "postgres",
            "namespace": "platform",
            "postgres": {"dsn_env": "MISSING_STATE_DSN"},
        }
    )
    monkeypatch.delenv("MISSING_STATE_DSN", raising=False)
    with pytest.raises(StateBackendUnavailableError) as raised:
        make_postgres_state_administration(config)

    assert "MISSING_STATE_DSN" not in str(raised.value)
