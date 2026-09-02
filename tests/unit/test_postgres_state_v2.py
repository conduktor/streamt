"""Private PostgreSQL schema-v2 migration and writer-role contracts."""

from __future__ import annotations

import time
from collections.abc import Callable
from typing import cast

import pytest

import streamt.deployer.postgres_state as postgres_state
from streamt.deployer.postgres_state import (
    POSTGRES_SCHEMA_V2_CHECKSUM,
    POSTGRES_SCHEMA_V2_VERSION,
    PostgresStateStatus,
    PostgresStateV2Migration,
    PrivatePostgresStateV2Migrator,
)
from streamt.deployer.state_backend import (
    StateAddress,
    StateBackendInvalidStateError,
    StateBackendReleaseAfterCommitError,
    StateBackendUnavailableError,
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


class _Cursor:
    def __init__(
        self,
        responder: Callable[[str, tuple[object, ...] | None], list[tuple[object, ...]]]
        | None = None,
    ) -> None:
        self.responder = responder or (lambda _query, _params: [])
        self.current: list[tuple[object, ...]] = []
        self.calls: list[tuple[str, tuple[object, ...] | None]] = []

    def execute(
        self,
        query: object,
        params: tuple[object, ...] | None = None,
    ) -> object:
        rendered = str(query)
        self.calls.append((rendered, params))
        self.current = self.responder(rendered, params)
        return self

    def fetchall(self) -> object:
        return list(self.current)

    def close(self) -> None:
        pass


class _Connection:
    def __init__(self, cursor: _Cursor) -> None:
        self._cursor = cursor

    def cursor(self) -> _Cursor:
        return self._cursor

    def commit(self) -> None:
        pass

    def rollback(self) -> None:
        pass

    def close(self) -> None:
        pass


class _Driver:
    def __init__(self, connection: _Connection) -> None:
        self.connection = connection

    def connect(self, _conninfo: str, **_kwargs: object) -> _Connection:
        return self.connection


def _address() -> StateAddress:
    return StateAddress(namespace="platform", project="payments", environment="prod")


def _status(*, version: int, store_id: str) -> PostgresStateStatus:
    return PostgresStateStatus(
        store_status="ready",
        store_id=store_id,
        schema_version=version,
        address=_address(),
        address_status="unregistered",
        state_status="unregistered",
        state_serial=None,
        state_checksum=None,
        operation_status=None,
    )


def _migrator(*, timeout: int = 10) -> PrivatePostgresStateV2Migrator:
    return PrivatePostgresStateV2Migrator(
        dsn="host=/var/run/postgresql dbname=state",
        schema="streamt",
        lock_timeout_seconds=timeout,
        writer_role="streamt_writer",
    )


def test_v2_contract_is_portable_complete_and_deterministic() -> None:
    metadata_columns = [
        column for column in postgres_state._EXPECTED_COLUMNS_V2 if column[0] == "store_metadata"
    ]
    assert [column[1] for column in metadata_columns] == [
        "singleton",
        "store_id",
        "schema_version",
        "initialized_at",
        "writer_role_name",
    ]
    assert all("writer_role_oid" not in column for column in metadata_columns)
    assert {
        constraint[1] for constraint in postgres_state._EXPECTED_CONSTRAINTS_V2
    } >= {
        "store_metadata_schema_version_check",
        "store_metadata_writer_role_name_check",
    }
    assert postgres_state._EXPECTED_MIGRATIONS_V2 == (
        postgres_state._EXPECTED_MIGRATION,
        (
            POSTGRES_SCHEMA_V2_VERSION,
            "schema-v2-writer-role",
            POSTGRES_SCHEMA_V2_CHECKSUM,
        ),
    )
    assert POSTGRES_SCHEMA_V2_CHECKSUM.startswith("sha256:")
    assert len(POSTGRES_SCHEMA_V2_CHECKSUM) == 71
    assert b"NOINHERIT" in postgres_state._SCHEMA_V2_CONTRACT_BYTES
    assert b"schema_owner" in postgres_state._SCHEMA_V2_CONTRACT_BYTES
    assert b"default_acl" in postgres_state._SCHEMA_V2_CONTRACT_BYTES


def test_writer_column_contract_is_exact() -> None:
    privileges = {
        (table, privilege, column)
        for table, privilege, columns in postgres_state._SCHEMA_V2_WRITER_COLUMN_PRIVILEGES
        for column in columns
    }
    assert ("current_state", "INSERT", "namespace") in privileges
    assert ("current_state", "UPDATE", "state_json") in privileges
    assert ("current_state", "UPDATE", "namespace") not in privileges
    assert ("operation_control", "UPDATE", "control_json") in privileges
    assert not any(
        table in {"store_metadata", "schema_migrations", "state_addresses"}
        for table, _privilege, _column in privileges
    )
    assert not any(privilege == "DELETE" for _table, privilege, _column in privileges)


def test_migration_result_is_secret_neutral() -> None:
    result = PostgresStateV2Migration(
        store_id="00000000-0000-4000-8000-000000000001",
        migrated=True,
    )

    assert result.outcome == "migrated"
    assert result.to_dict() == {
        "backend": "postgres",
        "outcome": "migrated",
        "store_id": "00000000-0000-4000-8000-000000000001",
        "schema_version": 2,
        "ordinary_state_authority": "supported_for_v2_writer",
    }


@pytest.mark.parametrize(
    ("confirmed_role", "confirmed_store"),
    [
        ("wrong", "00000000-0000-4000-8000-000000000001"),
        ("streamt_writer", "not-a-store-id"),
    ],
)
def test_confirmation_is_rejected_before_driver_loading(
    monkeypatch: pytest.MonkeyPatch,
    confirmed_role: str,
    confirmed_store: str,
) -> None:
    monkeypatch.setattr(
        postgres_state,
        "_load_psycopg",
        lambda: (_ for _ in ()).throw(AssertionError("driver loaded")),
    )

    with pytest.raises(StateBackendInvalidStateError) as raised:
        _migrator().migrate(
            confirmed_writer_role=confirmed_role,
            confirmed_store_id=confirmed_store,
        )

    assert "streamt_writer" not in str(raised.value)
    assert confirmed_store not in str(raised.value)


def test_schema_lock_call_is_pid_and_primary_gated(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cursor = _Cursor(lambda _query, _params: [(701, True, True)])
    monkeypatch.setattr(time, "monotonic", lambda: 1.0)

    _migrator()._acquire_schema_lock(
        cast(postgres_state._Cursor, cursor),
        backend_pid=701,
        lock_key=(11, 12),
        deadline=2.0,
    )

    query, params = cursor.calls[0]
    assert "CASE WHEN pg_catalog.pg_backend_pid() = %s" in query
    assert "THEN pg_catalog.pg_try_advisory_lock(%s, %s)" in query
    assert params == (701, 11, 12)


def test_address_locks_are_acquired_sequentially_with_one_deadline(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    attempts: dict[int, int] = {101: 0, 102: 0}

    def respond(
        query: str,
        params: tuple[object, ...] | None,
    ) -> list[tuple[object, ...]]:
        assert "CASE WHEN pg_catalog.pg_backend_pid() = %s" in query
        assert params is not None
        lock_key = cast(int, params[1])
        attempts[lock_key] += 1
        acquired = lock_key == 102 or attempts[lock_key] > 1
        return [(701, True, acquired)]

    cursor = _Cursor(respond)
    moments = iter((1.0, 1.1, 1.2, 1.3, 1.4))
    monkeypatch.setattr(time, "monotonic", lambda: next(moments))
    monkeypatch.setattr(time, "sleep", lambda _seconds: None)
    acquired: list[int] = []

    _migrator()._acquire_address_locks(
        cast(postgres_state._Cursor, cursor),
        [101, 102],
        backend_pid=701,
        deadline=2.0,
        acquired=acquired,
    )

    assert acquired == [101, 102]
    assert [params for _query, params in cursor.calls] == [
        (701, 101),
        (701, 101),
        (701, 102),
    ]


def test_transaction_authority_requires_exact_pid_primary_and_lock_identities() -> None:
    migrator = _migrator()
    schema_key = postgres_state._initialization_lock_key("streamt")
    address_key = -7
    address_identity = migrator._advisory_catalog_identity(address_key)
    expected_rows: list[tuple[object, ...]] = sorted(
        [
            (
                701,
                True,
                schema_key[0] & 0xFFFFFFFF,
                schema_key[1] & 0xFFFFFFFF,
                2,
            ),
            (701, True, *address_identity),
        ]
    )
    cursor = _Cursor(lambda _query, _params: expected_rows)

    migrator._prove_migration_authority(
        cast(postgres_state._Cursor, cursor),
        backend_pid=701,
        schema_lock_key=schema_key,
        address_lock_keys=(address_key,),
    )

    assert "FROM pg_catalog.pg_locks" in cursor.calls[0][0]
    drifted = _Cursor(lambda _query, _params: [*expected_rows, (701, True, 1, 2, 1)])
    with pytest.raises(StateBackendUnavailableError):
        migrator._prove_migration_authority(
            cast(postgres_state._Cursor, drifted),
            backend_pid=701,
            schema_lock_key=schema_key,
            address_lock_keys=(address_key,),
        )


@pytest.mark.parametrize(
    ("version", "writer", "departed", "expected"),
    [
        (2, "streamt_writer", True, "committed"),
        (2, "streamt_writer", False, "committed_release_unknown"),
        (1, None, True, "not_committed"),
        (1, None, False, "unknown"),
    ],
)
def test_ambiguous_commit_classification_requires_backend_departure(
    monkeypatch: pytest.MonkeyPatch,
    version: int,
    writer: str | None,
    departed: bool,
    expected: str,
) -> None:
    store_id = "00000000-0000-4000-8000-000000000001"
    observed = (_status(version=version, store_id=store_id), writer, departed)
    monkeypatch.setattr(
        PrivatePostgresStateV2Migrator,
        "_fresh_catalog_state",
        lambda _self, _bundle, **_kwargs: observed,
    )
    result = PostgresStateV2Migration(store_id=store_id, migrated=True)

    assert _migrator()._classify_commit(
        cast(postgres_state._PsycopgBundle, object()),
        result,
        701,
    ) == expected


def test_interrupt_after_acknowledged_commit_cannot_mask_unverified_release(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store_id = "00000000-0000-4000-8000-000000000001"

    def respond(
        query: str,
        _params: tuple[object, ...] | None,
    ) -> list[tuple[object, ...]]:
        if "NOT EXISTS" in query and "pg_backend_pid" in query:
            return [(701, True, True)]
        if "pg_advisory_unlock" in query:
            return [(701, True, False)]
        return []

    cursor = _Cursor(respond)
    bundle = postgres_state._PsycopgBundle(
        driver=cast(postgres_state._DriverModule, _Driver(_Connection(cursor))),
        sql=cast(postgres_state._SqlModule, _FakeSql()),
    )
    monkeypatch.setattr(postgres_state, "_load_psycopg", lambda: bundle)
    monkeypatch.setattr(
        PrivatePostgresStateV2Migrator,
        "_acquire_schema_lock",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        PrivatePostgresStateV2Migrator,
        "_prove_migration_authority",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        PrivatePostgresStateV2Migrator,
        "_configure_session",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        PrivatePostgresStateV2Migrator,
        "_configure_transaction",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        PrivatePostgresStateV2Migrator,
        "_read_migration_source",
        lambda *_args, **_kwargs: (_status(version=1, store_id=store_id), []),
    )
    monkeypatch.setattr(
        PrivatePostgresStateV2Migrator,
        "_acquire_address_locks",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        PrivatePostgresStateV2Migrator,
        "_migrate_transaction",
        lambda *_args, **_kwargs: PostgresStateV2Migration(
            store_id=store_id,
            migrated=True,
        ),
    )
    monkeypatch.setattr(
        PrivatePostgresStateV2Migrator,
        "_fresh_catalog_state",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(KeyboardInterrupt()),
    )

    with pytest.raises(StateBackendReleaseAfterCommitError):
        _migrator().migrate(
            confirmed_writer_role="streamt_writer",
            confirmed_store_id=store_id,
        )


def test_writer_acl_statements_reset_every_table_and_column_before_grants() -> None:
    cursor = _Cursor()

    _migrator()._replace_writer_acl(
        cast(postgres_state._Cursor, cursor),
        cast(postgres_state._SqlModule, _FakeSql()),
    )

    rendered = [query for query, _params in cursor.calls]
    assert rendered[0] == 'REVOKE ALL PRIVILEGES ON SCHEMA "streamt" FROM "streamt_writer"'
    assert rendered[1] == 'GRANT USAGE ON SCHEMA "streamt" TO "streamt_writer"'
    for table in postgres_state._EXPECTED_TABLES:
        relation = f'"streamt"."{table}"'
        revoke_table = f'REVOKE ALL PRIVILEGES ON TABLE {relation} FROM "streamt_writer"'
        grant_select = f'GRANT SELECT ON TABLE {relation} TO "streamt_writer"'
        assert rendered.index(revoke_table) < rendered.index(grant_select)
        assert any(
            statement.startswith("REVOKE UPDATE (") and relation in statement
            for statement in rendered
        )
    assert not any("CREATE ROLE" in statement for statement in rendered)
    assert not any("DEFAULT PRIVILEGES" in statement for statement in rendered)
