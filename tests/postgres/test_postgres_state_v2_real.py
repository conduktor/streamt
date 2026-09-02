"""Real-server schema-v2 and least-privilege writer conformance.

The migration surface is intentionally private.  These tests prove the
administrative migration and the SQL authority it installs, but do not make
the ordinary PostgreSQL backend reachable through the production factory.
"""

from __future__ import annotations

import json
import os
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

import pytest

from streamt.core.deployment_state import validate_deployment_state_config
from streamt.deployer import postgres_state as postgres_state_module
from streamt.deployer.postgres_state import (
    POSTGRES_SCHEMA_V2_VERSION,
    PostgresStateAdministration,
    PostgresStateInitializer,
    PostgresStateLockProbe,
    PrivatePostgresStateV2Migrator,
)
from streamt.deployer.postgres_state_backend import PrivatePostgresStateReadBackend
from streamt.deployer.state import LocalState
from streamt.deployer.state_backend import (
    OperationAction,
    OperationIntent,
    OperationProgress,
    OperationSnapshot,
    RecoveryRecord,
    StateAddress,
    StateBackendInvalidStateError,
    StateBackendLockTimeoutError,
    StateBackendUnavailableError,
    make_deployment_state_service,
    operation_timestamp,
    state_checksum,
)
from tests.postgres.conftest import WriterIdentity

pytestmark = [pytest.mark.integration, pytest.mark.postgres]

_TABLES = (
    "current_state",
    "operation_control",
    "operation_history",
    "schema_migrations",
    "state_addresses",
    "state_history",
    "store_metadata",
)

_EXPECTED_COLUMN_ACL = {
    "current_state": {
        ("environment", "INSERT"),
        ("namespace", "INSERT"),
        ("project", "INSERT"),
        ("revision", "INSERT"),
        ("revision", "UPDATE"),
        ("state_checksum", "INSERT"),
        ("state_checksum", "UPDATE"),
        ("state_json", "INSERT"),
        ("state_json", "UPDATE"),
        ("state_serial", "INSERT"),
        ("state_serial", "UPDATE"),
        ("updated_at", "INSERT"),
        ("updated_at", "UPDATE"),
    },
    "operation_control": {
        ("control_json", "UPDATE"),
        ("revision", "UPDATE"),
        ("status", "UPDATE"),
        ("updated_at", "UPDATE"),
    },
    "operation_history": {
        ("control_json", "INSERT"),
        ("environment", "INSERT"),
        ("event_index", "INSERT"),
        ("event_kind", "INSERT"),
        ("namespace", "INSERT"),
        ("operation_id", "INSERT"),
        ("project", "INSERT"),
        ("recorded_at", "INSERT"),
    },
    "state_history": {
        ("environment", "INSERT"),
        ("namespace", "INSERT"),
        ("operation_id", "INSERT"),
        ("project", "INSERT"),
        ("recorded_at", "INSERT"),
        ("revision", "INSERT"),
        ("state_checksum", "INSERT"),
        ("state_json", "INSERT"),
        ("state_serial", "INSERT"),
    },
}


class CommitAckLossConnection:
    """Delegate a real connection but lose the one commit acknowledgement."""

    def __init__(
        self,
        connection: object,
        error_type: type[BaseException],
        *,
        commit_on_server: bool,
    ) -> None:
        self._connection = connection
        self._error_type = error_type
        self._commit_on_server = commit_on_server

    def cursor(self) -> object:
        return self._connection.cursor()

    def commit(self) -> None:
        if self._commit_on_server:
            self._connection.commit()
        raise self._error_type("injected commit acknowledgement loss")

    def rollback(self) -> None:
        self._connection.rollback()

    def close(self) -> None:
        self._connection.close()


class CommitAckLossDriver:
    """Wrap only the authoritative migration session; verification stays fresh."""

    def __init__(self, driver: object, *, commit_on_server: bool = True) -> None:
        self._driver = driver
        self._commit_on_server = commit_on_server
        self.connections = 0

    def connect(self, conninfo: str, **kwargs: object) -> object:
        connection = self._driver.connect(conninfo, **kwargs)
        self.connections += 1
        if self.connections == 1:
            return CommitAckLossConnection(
                connection,
                self._driver.OperationalError,
                commit_on_server=self._commit_on_server,
            )
        return connection


def _address(*, project: str = "payments") -> StateAddress:
    return StateAddress(namespace="platform", project=project, environment="prod")


def _initializer(case: object) -> PostgresStateInitializer:
    return PostgresStateInitializer(
        dsn=case.owner_dsn,
        schema=case.schema,
        lock_timeout_seconds=10,
    )


def _migrator(
    case: object,
    writer: WriterIdentity,
    *,
    dsn: str | None = None,
    writer_role: str | None = None,
) -> PrivatePostgresStateV2Migrator:
    return PrivatePostgresStateV2Migrator(
        dsn=dsn or case.owner_dsn,
        schema=case.schema,
        lock_timeout_seconds=10,
        writer_role=writer_role or writer.role,
    )


def _store_id(case: object) -> str:
    with case.psycopg.connect(case.owner_dsn) as connection:
        row = connection.execute(
            case.sql.SQL("SELECT store_id::text FROM {}.{}").format(
                case.sql.Identifier(case.schema),
                case.sql.Identifier("store_metadata"),
            )
        ).fetchone()
    assert row is not None
    assert isinstance(row[0], str)
    return row[0]


def _migrate(case: object, writer: WriterIdentity) -> object:
    return _migrator(case, writer).migrate(
        confirmed_writer_role=writer.role,
        confirmed_store_id=_store_id(case),
    )


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


def _writer_acl(
    case: object,
    writer: WriterIdentity,
) -> tuple[
    set[tuple[str, bool, str]],
    set[tuple[str, str, bool, str]],
    set[tuple[str, str, str, bool, str]],
]:
    """Return direct schema, table, and column ACL rows for one role."""
    with case.psycopg.connect(case.admin_dsn) as connection:
        schema_acl = set(
            connection.execute(
                "SELECT acl.privilege_type, acl.is_grantable, grantor.rolname "
                "FROM pg_catalog.pg_namespace AS n "
                "CROSS JOIN LATERAL pg_catalog.aclexplode(n.nspacl) AS acl "
                "JOIN pg_catalog.pg_roles AS r ON r.oid = acl.grantee "
                "JOIN pg_catalog.pg_roles AS grantor ON grantor.oid = acl.grantor "
                "WHERE n.nspname = %s AND r.rolname = %s",
                (case.schema, writer.role),
            ).fetchall()
        )
        table_acl = set(
            connection.execute(
                "SELECT c.relname, acl.privilege_type, acl.is_grantable, "
                "grantor.rolname "
                "FROM pg_catalog.pg_class AS c "
                "JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace "
                "CROSS JOIN LATERAL pg_catalog.aclexplode(c.relacl) AS acl "
                "JOIN pg_catalog.pg_roles AS r ON r.oid = acl.grantee "
                "JOIN pg_catalog.pg_roles AS grantor ON grantor.oid = acl.grantor "
                "WHERE n.nspname = %s AND c.relkind = 'r' AND r.rolname = %s",
                (case.schema, writer.role),
            ).fetchall()
        )
        column_acl = set(
            connection.execute(
                "SELECT c.relname, a.attname, acl.privilege_type, acl.is_grantable, "
                "grantor.rolname "
                "FROM pg_catalog.pg_attribute AS a "
                "JOIN pg_catalog.pg_class AS c ON c.oid = a.attrelid "
                "JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace "
                "CROSS JOIN LATERAL pg_catalog.aclexplode(a.attacl) AS acl "
                "JOIN pg_catalog.pg_roles AS r ON r.oid = acl.grantee "
                "JOIN pg_catalog.pg_roles AS grantor ON grantor.oid = acl.grantor "
                "WHERE n.nspname = %s AND c.relkind = 'r' AND a.attnum > 0 "
                "AND NOT a.attisdropped AND r.rolname = %s",
                (case.schema, writer.role),
            ).fetchall()
        )
    return schema_acl, table_acl, column_acl


def _public_acl(case: object) -> list[tuple[object, ...]]:
    with case.psycopg.connect(case.admin_dsn) as connection:
        return list(
            connection.execute(
                "SELECT object_kind, object_name, column_name, privilege_type, "
                "is_grantable FROM ("
                "SELECT 'schema'::text AS object_kind, n.nspname AS object_name, "
                "NULL::text AS column_name, acl.privilege_type, acl.is_grantable "
                "FROM pg_catalog.pg_namespace AS n "
                "CROSS JOIN LATERAL pg_catalog.aclexplode(n.nspacl) AS acl "
                "WHERE n.nspname = %s AND acl.grantee = 0 UNION ALL "
                "SELECT 'table', c.relname, NULL::text, acl.privilege_type, "
                "acl.is_grantable FROM pg_catalog.pg_class AS c "
                "JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace "
                "CROSS JOIN LATERAL pg_catalog.aclexplode(c.relacl) AS acl "
                "WHERE n.nspname = %s AND c.relkind = 'r' AND acl.grantee = 0 "
                "UNION ALL SELECT 'column', c.relname, a.attname, "
                "acl.privilege_type, acl.is_grantable "
                "FROM pg_catalog.pg_attribute AS a "
                "JOIN pg_catalog.pg_class AS c ON c.oid = a.attrelid "
                "JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace "
                "CROSS JOIN LATERAL pg_catalog.aclexplode(a.attacl) AS acl "
                "WHERE n.nspname = %s AND c.relkind = 'r' AND acl.grantee = 0"
                ") AS public_acl ORDER BY 1, 2, 3, 4",
                (case.schema, case.schema, case.schema),
            ).fetchall()
        )


def _assert_exact_writer_acl(case: object, writer: WriterIdentity) -> None:
    schema_acl, table_acl, column_acl = _writer_acl(case, writer)
    assert schema_acl == {("USAGE", False, case.owner_role)}
    assert table_acl == {
        (table, "SELECT", False, case.owner_role) for table in _TABLES
    }
    assert column_acl == {
        (table, column, privilege, False, case.owner_role)
        for table, privileges in _EXPECTED_COLUMN_ACL.items()
        for column, privilege in privileges
    }
    assert _public_acl(case) == []
    with case.psycopg.connect(case.admin_dsn) as connection:
        role = connection.execute(
            "SELECT rolcanlogin, rolinherit, rolsuper, rolcreatedb, rolcreaterole, "
            "rolreplication, rolbypassrls FROM pg_catalog.pg_roles WHERE rolname = %s",
            (writer.role,),
        ).fetchone()
        memberships = connection.execute(
            "SELECT count(*) FROM pg_catalog.pg_auth_members AS m "
            "JOIN pg_catalog.pg_roles AS r ON r.oid IN (m.roleid, m.member) "
            "WHERE r.rolname = %s",
            (writer.role,),
        ).fetchone()
        owners = connection.execute(
            "SELECT count(*) FROM ("
            "SELECT n.oid FROM pg_catalog.pg_namespace AS n "
            "JOIN pg_catalog.pg_roles AS r ON r.oid = n.nspowner "
            "WHERE n.nspname = %s AND r.rolname = %s UNION ALL "
            "SELECT c.oid FROM pg_catalog.pg_class AS c "
            "JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace "
            "JOIN pg_catalog.pg_roles AS r ON r.oid = c.relowner "
            "WHERE n.nspname = %s AND r.rolname = %s) AS owned",
            (case.schema, writer.role, case.schema, writer.role),
        ).fetchone()
    assert role == (True, False, False, False, False, False, False)
    assert memberships == (0,)
    assert owners == (0,)


def _metadata_and_migrations(case: object) -> tuple[tuple[object, ...], list[tuple[object, ...]]]:
    with case.psycopg.connect(case.owner_dsn) as connection:
        metadata = connection.execute(
            case.sql.SQL(
                "SELECT store_id::text, schema_version, writer_role_name FROM {}.{}"
            ).format(
                case.sql.Identifier(case.schema),
                case.sql.Identifier("store_metadata"),
            )
        ).fetchone()
        migrations = list(
            connection.execute(
                case.sql.SQL(
                    "SELECT schema_version, migration_name, migration_checksum "
                    "FROM {}.{} ORDER BY schema_version"
                ).format(
                    case.sql.Identifier(case.schema),
                    case.sql.Identifier("schema_migrations"),
                )
            ).fetchall()
        )
    assert metadata is not None
    return metadata, migrations


def _assert_v1_unchanged(case: object, writer: WriterIdentity) -> None:
    with case.psycopg.connect(case.owner_dsn) as connection:
        metadata_columns = {
            row[0]
            for row in connection.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema = %s AND table_name = 'store_metadata'",
                (case.schema,),
            ).fetchall()
        }
        version = connection.execute(
            case.sql.SQL("SELECT schema_version FROM {}.{}").format(
                case.sql.Identifier(case.schema),
                case.sql.Identifier("store_metadata"),
            )
        ).fetchone()
        migrations = connection.execute(
            case.sql.SQL("SELECT schema_version FROM {}.{} ORDER BY schema_version").format(
                case.sql.Identifier(case.schema),
                case.sql.Identifier("schema_migrations"),
            )
        ).fetchall()
    assert "writer_role_oid" not in metadata_columns
    assert "writer_role_name" not in metadata_columns
    assert version == (1,)
    assert migrations == [(1,)]
    assert _writer_acl(case, writer) == (set(), set(), set())


def _intent(
    snapshot: OperationSnapshot,
    address: StateAddress,
    *,
    actions: bool = True,
) -> OperationIntent:
    return OperationIntent(
        operation_id=str(uuid.uuid4()),
        kind="apply",
        started_at=operation_timestamp(),
        actor="postgres-v2-real-conformance",
        prior_state_serial=snapshot.state.state_serial,
        prior_state_checksum=state_checksum(snapshot.state.state),
        reviewed_plan_checksum=None,
        actions=(
            (
                OperationAction(
                    index=0,
                    resource_id=(
                        f"streamt://platform/{address.project}/prod/kafka_topic/orders"
                    ),
                    action="create",
                ),
            )
            if actions
            else ()
        ),
    )


def _record_action(operation: object, snapshot: OperationSnapshot, intent: OperationIntent) -> object:
    action = intent.actions[0]
    current = snapshot
    for status in ("started", "completed"):
        current = operation.record_progress(
            current,
            OperationProgress(
                operation_id=intent.operation_id,
                action_index=action.index,
                resource_id=action.resource_id,
                action=action.action,
                status=status,
                succeeded=True if status == "completed" else None,
                recorded_at=operation_timestamp(),
            ),
        )
    return current


def _seed_populated_state(case: object, address: StateAddress) -> dict[str, int]:
    replacement = LocalState(project=address.project, environment=address.environment, serial=1)
    backend = PrivatePostgresStateReadBackend(
        dsn=case.owner_dsn,
        schema=case.schema,
        lock_timeout_seconds=10,
    )
    with backend.operation(address) as operation:
        observed = operation.observe()
        intent = _intent(observed, address)
        active = operation.begin_operation(observed, intent)
        active = _record_action(operation, active, intent)
        operation.commit_operation(active, replacement)
    with case.psycopg.connect(case.owner_dsn) as connection:
        return {
            table: connection.execute(
                case.sql.SQL("SELECT count(*) FROM {}.{}").format(
                    case.sql.Identifier(case.schema),
                    case.sql.Identifier(table),
                )
            ).fetchone()[0]
            for table in _TABLES
        }


def _durable_row_bytes(case: object) -> dict[str, tuple[bytes, ...]]:
    """Capture exact durable rows not intentionally changed by schema v2."""
    tables = (
        "current_state",
        "operation_control",
        "operation_history",
        "state_addresses",
        "state_history",
    )
    with case.psycopg.connect(case.owner_dsn) as connection:
        return {
            table: tuple(
                row[0].encode("utf-8")
                for row in connection.execute(
                    case.sql.SQL(
                        "SELECT pg_catalog.row_to_json(row_value)::text FROM {}.{} "
                        "AS row_value ORDER BY pg_catalog.row_to_json(row_value)::text"
                    ).format(
                        case.sql.Identifier(case.schema),
                        case.sql.Identifier(table),
                    )
                ).fetchall()
            )
            for table in tables
        }


def test_clean_v1_to_v2_migration_is_exact_and_idempotent(
    postgres_case: object,
    postgres_writer: WriterIdentity,
) -> None:
    address = _address()
    initialized = _initializer(postgres_case).initialize(address)
    _grant_reader(postgres_case)

    first = _migrate(postgres_case, postgres_writer)
    second = _migrate(postgres_case, postgres_writer)

    assert first.store_id == initialized.store_id
    assert first.migrated is True
    assert first.outcome == "migrated"
    assert second.store_id == initialized.store_id
    assert second.migrated is False
    assert second.outcome == "already_migrated"
    assert first.to_dict()["schema_version"] == POSTGRES_SCHEMA_V2_VERSION
    assert "writer" not in json.dumps(first.to_dict()).lower()

    metadata, migrations = _metadata_and_migrations(postgres_case)
    assert metadata == (
        initialized.store_id,
        POSTGRES_SCHEMA_V2_VERSION,
        postgres_writer.role,
    )
    assert [row[0] for row in migrations] == [1, POSTGRES_SCHEMA_V2_VERSION]
    assert all(str(row[2]).startswith("sha256:") and len(str(row[2])) == 71 for row in migrations)
    _assert_exact_writer_acl(postgres_case, postgres_writer)

    status = PostgresStateAdministration(
        dsn=postgres_case.reader_dsn,
        schema=postgres_case.schema,
        lock_timeout_seconds=10,
    ).status(address)
    assert status.schema_version == POSTGRES_SCHEMA_V2_VERSION
    assert status.store_id == initialized.store_id
    assert status.to_dict()["mutation_status"] == "catalog_ready"
    assert postgres_writer.role not in json.dumps(status.to_dict())


def test_v2_status_lock_probe_and_owner_registration_preserve_admin_boundaries(
    postgres_case: object,
    postgres_writer: WriterIdentity,
) -> None:
    address = _address()
    second_address = _address(project="fulfillment")
    initializer = _initializer(postgres_case)
    initialized = initializer.initialize(address)
    _grant_reader(postgres_case)
    _migrate(postgres_case, postgres_writer)

    for dsn in (
        postgres_case.owner_dsn,
        postgres_case.reader_dsn,
        postgres_writer.dsn,
    ):
        status = PostgresStateAdministration(
            dsn=dsn,
            schema=postgres_case.schema,
            lock_timeout_seconds=10,
        ).status(address)
        assert status.schema_version == POSTGRES_SCHEMA_V2_VERSION
        assert status.store_id == initialized.store_id
        probe = PostgresStateLockProbe(
            dsn=dsn,
            schema=postgres_case.schema,
            lock_timeout_seconds=10,
        ).probe(address)
        assert probe.lock_status == "available"

    registered = initializer.initialize(second_address)
    assert registered.created_store is False
    assert registered.registered_address is True
    assert registered.schema_version == POSTGRES_SCHEMA_V2_VERSION
    second_status = PostgresStateAdministration(
        dsn=postgres_writer.dsn,
        schema=postgres_case.schema,
        lock_timeout_seconds=10,
    ).status(second_address)
    assert second_status.address_status == "registered"
    assert second_status.operation_status is not None
    assert second_status.operation_status.status == "clear"
    _assert_exact_writer_acl(postgres_case, postgres_writer)

    with pytest.raises(StateBackendInvalidStateError):
        PostgresStateInitializer(
            dsn=postgres_writer.dsn,
            schema=postgres_case.schema,
            lock_timeout_seconds=10,
        ).initialize(_address(project="forbidden-registration"))


def test_populated_clear_store_migrates_without_changing_durable_rows(
    postgres_case: object,
    postgres_writer: WriterIdentity,
) -> None:
    address = _address()
    _initializer(postgres_case).initialize(address)
    before_counts = _seed_populated_state(postgres_case, address)
    before_rows = _durable_row_bytes(postgres_case)

    result = _migrate(postgres_case, postgres_writer)

    assert result.migrated is True
    assert _durable_row_bytes(postgres_case) == before_rows
    with postgres_case.psycopg.connect(postgres_case.owner_dsn) as connection:
        after = {
            table: connection.execute(
                postgres_case.sql.SQL("SELECT count(*) FROM {}.{}").format(
                    postgres_case.sql.Identifier(postgres_case.schema),
                    postgres_case.sql.Identifier(table),
                )
            ).fetchone()[0]
            for table in _TABLES
        }
    assert after == {
        **before_counts,
        "schema_migrations": before_counts["schema_migrations"] + 1,
    }
    _assert_exact_writer_acl(postgres_case, postgres_writer)


@dataclass(frozen=True)
class SemanticCorruption:
    name: str
    table: str
    assignment: str
    value: str
    predicate: str = "TRUE"

    def __str__(self) -> str:
        return self.name


_SEMANTIC_CORRUPTIONS = (
    SemanticCorruption(
        "current_state_checksum",
        "current_state",
        "state_checksum = %s",
        "sha256:" + "0" * 64,
    ),
    SemanticCorruption("current_state_json", "current_state", "state_json = %s", "{}"),
    SemanticCorruption(
        "state_history_checksum",
        "state_history",
        "state_checksum = %s",
        "sha256:" + "0" * 64,
    ),
    SemanticCorruption("state_history_json", "state_history", "state_json = %s", "{}"),
    SemanticCorruption(
        "operation_history_control_json",
        "operation_history",
        "control_json = %s",
        "{}",
        "event_index = 0",
    ),
    SemanticCorruption(
        "operation_history_event_kind",
        "operation_history",
        "event_kind = %s",
        "succeeded",
        "event_index = 0",
    ),
)


@pytest.mark.parametrize("corruption", _SEMANTIC_CORRUPTIONS, ids=str)
def test_semantically_corrupt_populated_v1_is_rejected_without_partial_v2(
    postgres_case: object,
    postgres_writer: WriterIdentity,
    corruption: SemanticCorruption,
) -> None:
    address = _address()
    _initializer(postgres_case).initialize(address)
    _seed_populated_state(postgres_case, address)
    with postgres_case.psycopg.connect(postgres_case.owner_dsn) as connection:
        connection.execute(
            postgres_case.sql.SQL(
                f"UPDATE {{}}.{{}} SET {corruption.assignment} "
                f"WHERE {corruption.predicate}"
            ).format(
                postgres_case.sql.Identifier(postgres_case.schema),
                postgres_case.sql.Identifier(corruption.table),
            ),
            (corruption.value,),
        )

    with pytest.raises(
        StateBackendInvalidStateError,
        match=r"^PostgreSQL deployment state migration is incompatible$",
    ):
        _migrate(postgres_case, postgres_writer)

    _assert_v1_unchanged(postgres_case, postgres_writer)


@pytest.mark.parametrize(
    "corruption",
    [
        "swapped_intent",
        "rewritten_progress",
        "incomplete_success",
        "state_history_serial_mismatch",
        "duplicate_state_history_operation",
    ],
)
def test_individually_valid_but_impossible_history_sequence_is_rejected(
    postgres_case: object,
    postgres_writer: WriterIdentity,
    corruption: str,
) -> None:
    address = _address()
    _initializer(postgres_case).initialize(address)
    _seed_populated_state(postgres_case, address)

    if corruption in (
        "state_history_serial_mismatch",
        "duplicate_state_history_operation",
    ):
        backend = PrivatePostgresStateReadBackend(
            dsn=postgres_case.owner_dsn,
            schema=postgres_case.schema,
            lock_timeout_seconds=10,
        )
        with backend.operation(address) as operation:
            observed = operation.observe()
            intent = _intent(observed, address, actions=False)
            active = operation.begin_operation(observed, intent)
            operation.commit_operation(
                active,
                LocalState(project=address.project, environment=address.environment, serial=2),
            )

    with postgres_case.psycopg.connect(postgres_case.owner_dsn) as connection:
        operation_history = postgres_case.sql.SQL("{}.{}").format(
            postgres_case.sql.Identifier(postgres_case.schema),
            postgres_case.sql.Identifier("operation_history"),
        )
        state_history = postgres_case.sql.SQL("{}.{}").format(
            postgres_case.sql.Identifier(postgres_case.schema),
            postgres_case.sql.Identifier("state_history"),
        )
        if corruption in ("swapped_intent", "rewritten_progress"):
            event_index = 1 if corruption == "swapped_intent" else 2
            row = connection.execute(
                postgres_case.sql.SQL(
                    "SELECT control_json FROM {} WHERE event_index = %s"
                ).format(operation_history),
                (event_index,),
            ).fetchone()
            assert row is not None
            payload = json.loads(row[0])
            if corruption == "swapped_intent":
                # Keep the operation ID and progress internally valid while
                # swapping a different immutable intent field in a later event.
                payload["intent"]["actor"] = "history-attacker"
            else:
                payload["progress"][0]["recorded_at"] = operation_timestamp()
            connection.execute(
                postgres_case.sql.SQL(
                    "UPDATE {} SET control_json = %s WHERE event_index = %s"
                ).format(operation_history),
                (
                    json.dumps(
                        payload,
                        sort_keys=True,
                        separators=(",", ":"),
                        ensure_ascii=False,
                    ),
                    event_index,
                ),
            )
        elif corruption == "incomplete_success":
            connection.execute(
                postgres_case.sql.SQL("DELETE FROM {} WHERE event_index = 2").format(
                    operation_history
                )
            )
            connection.execute(
                postgres_case.sql.SQL(
                    "UPDATE {} SET event_index = 2 WHERE event_index = 3"
                ).format(operation_history)
            )
        elif corruption == "state_history_serial_mismatch":
            mismatched = LocalState(
                project=address.project,
                environment=address.environment,
                serial=3,
            )
            canonical_json = json.dumps(
                mismatched.to_dict(),
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=False,
            )
            updated_history = connection.execute(
                postgres_case.sql.SQL(
                    "UPDATE {} SET state_serial = %s, state_checksum = %s, "
                    "state_json = %s WHERE revision = 2 RETURNING revision"
                ).format(state_history),
                (
                    mismatched.serial,
                    state_checksum(mismatched),
                    canonical_json,
                ),
            ).fetchall()
            updated_current = connection.execute(
                postgres_case.sql.SQL(
                    "UPDATE {}.{} SET state_serial = %s, state_checksum = %s, "
                    "state_json = %s WHERE revision = 2 RETURNING revision"
                ).format(
                    postgres_case.sql.Identifier(postgres_case.schema),
                    postgres_case.sql.Identifier("current_state"),
                ),
                (mismatched.serial, state_checksum(mismatched), canonical_json),
            ).fetchall()
            assert updated_history == [(2,)]
            assert updated_current == [(2,)]
        else:
            connection.execute(
                postgres_case.sql.SQL(
                    "UPDATE {} AS later SET operation_id = earlier.operation_id "
                    "FROM {} AS earlier WHERE later.revision = 2 "
                    "AND earlier.revision = 1"
                ).format(state_history, state_history)
            )

    with pytest.raises(
        StateBackendInvalidStateError,
        match=r"^PostgreSQL deployment state migration is incompatible$",
    ):
        _migrate(postgres_case, postgres_writer)
    _assert_v1_unchanged(postgres_case, postgres_writer)


@pytest.mark.parametrize("terminal", ["in_progress", "recovery_required"])
def test_active_or_recovery_target_is_rejected_without_partial_migration(
    postgres_case: object,
    postgres_writer: WriterIdentity,
    terminal: str,
) -> None:
    address = _address()
    _initializer(postgres_case).initialize(address)
    backend = PrivatePostgresStateReadBackend(
        dsn=postgres_case.owner_dsn,
        schema=postgres_case.schema,
        lock_timeout_seconds=10,
    )
    with backend.operation(address) as operation:
        observed = operation.observe()
        intent = OperationIntent(
            operation_id=str(uuid.uuid4()),
            kind="apply",
            started_at=operation_timestamp(),
            actor="postgres-v2-real-conformance",
            prior_state_serial=observed.state.state_serial,
            prior_state_checksum=state_checksum(observed.state.state),
            reviewed_plan_checksum=None,
            actions=(),
        )
        active = operation.begin_operation(observed, intent)
        if terminal == "recovery_required":
            operation.mark_recovery_required(
                active,
                RecoveryRecord(
                    operation_id=intent.operation_id,
                    failure_code="runtime_outcome_unknown",
                    failed_at=operation_timestamp(),
                    last_completed_action_index=None,
                ),
            )

    with pytest.raises(
        StateBackendInvalidStateError,
        match=r"^PostgreSQL deployment state migration is incompatible$",
    ):
        _migrate(postgres_case, postgres_writer)

    _assert_v1_unchanged(postgres_case, postgres_writer)


def test_held_address_lock_times_out_and_leaves_v1_untouched(
    postgres_case: object,
    postgres_writer: WriterIdentity,
) -> None:
    address = _address()
    _initializer(postgres_case).initialize(address)
    backend = PrivatePostgresStateReadBackend(
        dsn=postgres_case.owner_dsn,
        schema=postgres_case.schema,
        lock_timeout_seconds=10,
    )
    with (
        backend.operation(address),
        pytest.raises(
            StateBackendLockTimeoutError,
            match=r"^PostgreSQL deployment state migration lock timed out$",
        ),
    ):
        PrivatePostgresStateV2Migrator(
            dsn=postgres_case.owner_dsn,
            schema=postgres_case.schema,
            lock_timeout_seconds=1,
            writer_role=postgres_writer.role,
        ).migrate(
            confirmed_writer_role=postgres_writer.role,
            confirmed_store_id=_store_id(postgres_case),
        )

    _assert_v1_unchanged(postgres_case, postgres_writer)


def test_precommit_failure_rolls_back_schema_ledger_metadata_and_acl(
    postgres_case: object,
    postgres_writer: WriterIdentity,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    address = _address()
    _initializer(postgres_case).initialize(address)
    original: Callable[..., object] = PrivatePostgresStateV2Migrator._migrate_transaction
    transaction_body_completed = False

    def fail_after_transaction_body(self: object, *args: object, **kwargs: object) -> object:
        nonlocal transaction_body_completed
        original(self, *args, **kwargs)
        transaction_body_completed = True
        raise RuntimeError("injected before commit")

    monkeypatch.setattr(
        PrivatePostgresStateV2Migrator,
        "_migrate_transaction",
        fail_after_transaction_body,
    )
    with pytest.raises(
        StateBackendUnavailableError,
        match=r"^PostgreSQL deployment state migration is unavailable$",
    ):
        _migrate(postgres_case, postgres_writer)

    assert transaction_body_completed is True
    _assert_v1_unchanged(postgres_case, postgres_writer)

    monkeypatch.setattr(
        PrivatePostgresStateV2Migrator,
        "_migrate_transaction",
        original,
    )
    assert _migrate(postgres_case, postgres_writer).migrated is True


def test_lost_commit_ack_is_classified_by_fresh_postimage_without_replay(
    postgres_case: object,
    postgres_writer: WriterIdentity,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    address = _address()
    initialized = _initializer(postgres_case).initialize(address)
    driver = CommitAckLossDriver(postgres_case.psycopg)
    bundle = postgres_state_module._PsycopgBundle(
        driver=driver,
        sql=postgres_case.sql,
    )
    monkeypatch.setattr(postgres_state_module, "_load_psycopg", lambda: bundle)

    result = _migrator(postgres_case, postgres_writer).migrate(
        confirmed_writer_role=postgres_writer.role,
        confirmed_store_id=initialized.store_id,
    )

    assert result.migrated is True
    assert result.store_id == initialized.store_id
    assert driver.connections >= 2
    metadata, migrations = _metadata_and_migrations(postgres_case)
    assert metadata == (
        initialized.store_id,
        POSTGRES_SCHEMA_V2_VERSION,
        postgres_writer.role,
    )
    assert [row[0] for row in migrations] == [1, POSTGRES_SCHEMA_V2_VERSION]
    _assert_exact_writer_acl(postgres_case, postgres_writer)


def test_failed_commit_before_server_is_classified_v1_and_safe_retry_succeeds(
    postgres_case: object,
    postgres_writer: WriterIdentity,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    address = _address()
    initialized = _initializer(postgres_case).initialize(address)
    original_loader = postgres_state_module._load_psycopg
    driver = CommitAckLossDriver(postgres_case.psycopg, commit_on_server=False)
    bundle = postgres_state_module._PsycopgBundle(
        driver=driver,
        sql=postgres_case.sql,
    )
    monkeypatch.setattr(postgres_state_module, "_load_psycopg", lambda: bundle)

    with pytest.raises(
        StateBackendUnavailableError,
        match=r"^PostgreSQL deployment state migration is unavailable$",
    ):
        _migrator(postgres_case, postgres_writer).migrate(
            confirmed_writer_role=postgres_writer.role,
            confirmed_store_id=initialized.store_id,
        )

    assert driver.connections >= 2
    _assert_v1_unchanged(postgres_case, postgres_writer)
    monkeypatch.setattr(postgres_state_module, "_load_psycopg", original_loader)
    retried = _migrate(postgres_case, postgres_writer)
    assert retried.migrated is True
    assert retried.store_id == initialized.store_id
    _assert_exact_writer_acl(postgres_case, postgres_writer)


def test_exact_writer_can_read_catalog_but_not_admin_or_unrelated_objects(
    postgres_case: object,
    postgres_writer: WriterIdentity,
) -> None:
    address = _address()
    _initializer(postgres_case).initialize(address)
    _migrate(postgres_case, postgres_writer)

    with postgres_case.psycopg.connect(postgres_writer.dsn) as connection:
        assert connection.execute("SELECT current_user").fetchone() == (postgres_writer.role,)
        for table in _TABLES:
            connection.execute(
                postgres_case.sql.SQL("SELECT * FROM {}.{} LIMIT 0").format(
                    postgres_case.sql.Identifier(postgres_case.schema),
                    postgres_case.sql.Identifier(table),
                )
            )

    unrelated_schema = f"unrelated_{uuid.uuid4().hex[:12]}"
    try:
        with postgres_case.psycopg.connect(
            postgres_case.admin_dsn,
            autocommit=True,
        ) as connection:
            connection.execute(
                postgres_case.sql.SQL("CREATE SCHEMA {}").format(
                    postgres_case.sql.Identifier(unrelated_schema)
                )
            )
            connection.execute(
                postgres_case.sql.SQL("CREATE TABLE {}.secret (value text)").format(
                    postgres_case.sql.Identifier(unrelated_schema)
                )
            )

        forbidden = (
            postgres_case.sql.SQL("CREATE TABLE {}.forbidden (value text)").format(
                postgres_case.sql.Identifier(postgres_case.schema)
            ),
            postgres_case.sql.SQL("CREATE ROLE forbidden_writer_child"),
            postgres_case.sql.SQL("SELECT * FROM {}.secret").format(
                postgres_case.sql.Identifier(unrelated_schema)
            ),
            postgres_case.sql.SQL("DELETE FROM {}.{}").format(
                postgres_case.sql.Identifier(postgres_case.schema),
                postgres_case.sql.Identifier("current_state"),
            ),
            postgres_case.sql.SQL("INSERT INTO {}.{} DEFAULT VALUES").format(
                postgres_case.sql.Identifier(postgres_case.schema),
                postgres_case.sql.Identifier("operation_control"),
            ),
            postgres_case.sql.SQL("UPDATE {}.{} SET singleton = singleton").format(
                postgres_case.sql.Identifier(postgres_case.schema),
                postgres_case.sql.Identifier("store_metadata"),
            ),
        )
        for statement in forbidden:
            with (
                postgres_case.psycopg.connect(postgres_writer.dsn) as connection,
                pytest.raises(postgres_case.psycopg.Error),
            ):
                connection.execute(statement)

        # PostgreSQL treats an ungrantable GRANT as a warning and a no-op,
        # rather than an exception.  Prove the privilege was not propagated.
        with postgres_case.psycopg.connect(postgres_writer.dsn) as connection:
            connection.execute(
                postgres_case.sql.SQL("GRANT SELECT ON {}.{} TO PUBLIC").format(
                    postgres_case.sql.Identifier(postgres_case.schema),
                    postgres_case.sql.Identifier("current_state"),
                )
            )
        assert _public_acl(postgres_case) == []
    finally:
        with postgres_case.psycopg.connect(
            postgres_case.admin_dsn,
            autocommit=True,
        ) as connection:
            connection.execute(
                postgres_case.sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(
                    postgres_case.sql.Identifier(unrelated_schema)
                )
            )


def test_v2_writer_backend_changed_and_unchanged_lifecycle_excludes_owner_and_reader(
    postgres_case: object,
    postgres_writer: WriterIdentity,
) -> None:
    address = _address()
    _initializer(postgres_case).initialize(address)
    _grant_reader(postgres_case)
    _migrate(postgres_case, postgres_writer)
    replacement = LocalState(project=address.project, environment=address.environment, serial=1)
    writer_backend = PrivatePostgresStateReadBackend(
        dsn=postgres_writer.dsn,
        schema=postgres_case.schema,
        lock_timeout_seconds=10,
    )

    with writer_backend.operation(address) as operation:
        observed = operation.observe()
        changed_intent = _intent(observed, address)
        active = operation.begin_operation(observed, changed_intent)
        active = _record_action(operation, active, changed_intent)
        committed = operation.commit_operation(active, replacement)
        assert committed.state.state == replacement
        assert committed.control.control.status == "clear"

    with writer_backend.operation(address) as operation:
        observed = operation.observe()
        unchanged_intent = _intent(observed, address, actions=False)
        active = operation.begin_operation(observed, unchanged_intent)
        committed = operation.commit_operation(active, None)
        assert committed.state.state == replacement
        assert committed.control.control.status == "clear"

    with postgres_case.psycopg.connect(postgres_case.owner_dsn) as connection:
        durable = {
            "current": connection.execute(
                postgres_case.sql.SQL("SELECT count(*), max(revision) FROM {}.{}").format(
                    postgres_case.sql.Identifier(postgres_case.schema),
                    postgres_case.sql.Identifier("current_state"),
                )
            ).fetchone(),
            "state_history": connection.execute(
                postgres_case.sql.SQL("SELECT count(*) FROM {}.{}").format(
                    postgres_case.sql.Identifier(postgres_case.schema),
                    postgres_case.sql.Identifier("state_history"),
                )
            ).fetchone(),
            "control": connection.execute(
                postgres_case.sql.SQL("SELECT revision, status FROM {}.{}").format(
                    postgres_case.sql.Identifier(postgres_case.schema),
                    postgres_case.sql.Identifier("operation_control"),
                )
            ).fetchone(),
            "events": connection.execute(
                postgres_case.sql.SQL(
                    "SELECT operation_id::text, event_index, event_kind FROM {}.{} "
                    "ORDER BY recorded_at, event_index"
                ).format(
                    postgres_case.sql.Identifier(postgres_case.schema),
                    postgres_case.sql.Identifier("operation_history"),
                )
            ).fetchall(),
        }
    assert durable["current"] == (1, 1)
    assert durable["state_history"] == (1,)
    assert durable["control"] == (6, "clear")
    events = durable["events"]
    assert [(row[1], row[2]) for row in events if row[0] == changed_intent.operation_id] == [
        (0, "intent"),
        (1, "progress_started"),
        (2, "progress_completed"),
        (3, "succeeded"),
    ]
    assert [(row[1], row[2]) for row in events if row[0] == unchanged_intent.operation_id] == [
        (0, "intent"),
        (1, "succeeded"),
    ]

    writable_reader_dsn = postgres_case.conninfo.make_conninfo(
        postgres_case.reader_dsn,
        options="-c default_transaction_read_only=off",
    )
    for rejected_dsn in (postgres_case.owner_dsn, writable_reader_dsn):
        rejected_backend = PrivatePostgresStateReadBackend(
            dsn=rejected_dsn,
            schema=postgres_case.schema,
            lock_timeout_seconds=10,
        )
        with rejected_backend.operation(address) as operation:
            observed = operation.observe()
            rejected_intent = _intent(observed, address, actions=False)
            with pytest.raises(StateBackendInvalidStateError):
                operation.begin_operation(observed, rejected_intent)

    with postgres_case.psycopg.connect(postgres_case.owner_dsn) as connection:
        after_denials = connection.execute(
            postgres_case.sql.SQL(
                "SELECT o.revision, o.status, (SELECT count(*) FROM {}.{}) "
                "FROM {}.{} AS o"
            ).format(
                postgres_case.sql.Identifier(postgres_case.schema),
                postgres_case.sql.Identifier("operation_history"),
                postgres_case.sql.Identifier(postgres_case.schema),
                postgres_case.sql.Identifier("operation_control"),
            )
        ).fetchone()
    assert after_denials == (6, "clear", 6)


def test_reader_remains_read_only_after_migration(
    postgres_case: object,
    postgres_writer: WriterIdentity,
) -> None:
    address = _address()
    _initializer(postgres_case).initialize(address)
    _grant_reader(postgres_case)
    _migrate(postgres_case, postgres_writer)

    with postgres_case.psycopg.connect(postgres_case.reader_dsn) as connection:
        for table in _TABLES:
            connection.execute(
                postgres_case.sql.SQL("SELECT * FROM {}.{} LIMIT 0").format(
                    postgres_case.sql.Identifier(postgres_case.schema),
                    postgres_case.sql.Identifier(table),
                )
            )
    writable_reader_dsn = postgres_case.conninfo.make_conninfo(
        postgres_case.reader_dsn,
        options="-c default_transaction_read_only=off",
    )
    with (
        postgres_case.psycopg.connect(writable_reader_dsn) as connection,
        pytest.raises(postgres_case.psycopg.Error),
    ):
        connection.execute(
            postgres_case.sql.SQL("UPDATE {}.{} SET revision = revision").format(
                postgres_case.sql.Identifier(postgres_case.schema),
                postgres_case.sql.Identifier("operation_control"),
            )
        )


def test_wrong_confirmation_executor_and_rebinding_are_rejected_without_role_leakage(
    postgres_case: object,
    postgres_writer: WriterIdentity,
) -> None:
    address = _address()
    initialized = _initializer(postgres_case).initialize(address)
    migrator = _migrator(postgres_case, postgres_writer)

    for confirmed in (postgres_case.owner_role, postgres_case.reader_role, "missing-role"):
        with pytest.raises(StateBackendInvalidStateError) as raised:
            migrator.migrate(
                confirmed_writer_role=confirmed,
                confirmed_store_id=initialized.store_id,
            )
        rendered = str(raised.value)
        assert postgres_writer.role not in rendered
        assert confirmed not in rendered
        assert postgres_case.owner_dsn not in rendered
    wrong_store_id = str(uuid.uuid4())
    with pytest.raises(StateBackendInvalidStateError) as raised:
        migrator.migrate(
            confirmed_writer_role=postgres_writer.role,
            confirmed_store_id=wrong_store_id,
        )
    assert wrong_store_id not in str(raised.value)
    _assert_v1_unchanged(postgres_case, postgres_writer)

    assert _migrate(postgres_case, postgres_writer).store_id == initialized.store_id
    with pytest.raises(StateBackendInvalidStateError):
        _migrator(
            postgres_case,
            postgres_writer,
            writer_role=postgres_case.reader_role,
        ).migrate(
            confirmed_writer_role=postgres_case.reader_role,
            confirmed_store_id=initialized.store_id,
        )
    with pytest.raises(StateBackendInvalidStateError):
        _migrator(
            postgres_case,
            postgres_writer,
            dsn=postgres_writer.dsn,
        ).migrate(
            confirmed_writer_role=postgres_writer.role,
            confirmed_store_id=initialized.store_id,
        )


@dataclass(frozen=True)
class AclDrift:
    name: str
    statement: str
    identifiers: tuple[str, ...]
    minimum_major: int = 14

    def __str__(self) -> str:
        return self.name


_ACL_DRIFTS = (
    AclDrift("missing_schema_usage", "REVOKE USAGE ON SCHEMA {} FROM {}", ("schema", "role")),
    AclDrift(
        "missing_table_select",
        "REVOKE SELECT ON {}.{} FROM {}",
        ("schema", "store_metadata", "role"),
    ),
    AclDrift(
        "missing_current_insert_column",
        "REVOKE INSERT ({}) ON {}.{} FROM {}",
        ("state_json", "schema", "current_state", "role"),
    ),
    AclDrift(
        "missing_current_update_column",
        "REVOKE UPDATE ({}) ON {}.{} FROM {}",
        ("state_json", "schema", "current_state", "role"),
    ),
    AclDrift(
        "missing_control_update_column",
        "REVOKE UPDATE ({}) ON {}.{} FROM {}",
        ("control_json", "schema", "operation_control", "role"),
    ),
    AclDrift(
        "missing_state_history_insert_column",
        "REVOKE INSERT ({}) ON {}.{} FROM {}",
        ("state_json", "schema", "state_history", "role"),
    ),
    AclDrift(
        "missing_operation_history_insert_column",
        "REVOKE INSERT ({}) ON {}.{} FROM {}",
        ("control_json", "schema", "operation_history", "role"),
    ),
    AclDrift("extra_schema_create", "GRANT CREATE ON SCHEMA {} TO {}", ("schema", "role")),
    AclDrift(
        "extra_metadata_insert",
        "GRANT INSERT ON {}.{} TO {}",
        ("schema", "store_metadata", "role"),
    ),
    AclDrift(
        "extra_current_delete",
        "GRANT DELETE ON {}.{} TO {}",
        ("schema", "current_state", "role"),
    ),
    AclDrift(
        "extra_current_table_insert",
        "GRANT INSERT ON {}.{} TO {}",
        ("schema", "current_state", "role"),
    ),
    AclDrift(
        "extra_current_table_update",
        "GRANT UPDATE ON {}.{} TO {}",
        ("schema", "current_state", "role"),
    ),
    AclDrift(
        "extra_current_key_update",
        "GRANT UPDATE ({}) ON {}.{} TO {}",
        ("namespace", "schema", "current_state", "role"),
    ),
    AclDrift(
        "extra_control_insert",
        "GRANT INSERT ON {}.{} TO {}",
        ("schema", "operation_control", "role"),
    ),
    AclDrift(
        "extra_control_table_update",
        "GRANT UPDATE ON {}.{} TO {}",
        ("schema", "operation_control", "role"),
    ),
    AclDrift(
        "extra_state_history_table_insert",
        "GRANT INSERT ON {}.{} TO {}",
        ("schema", "state_history", "role"),
    ),
    AclDrift(
        "extra_state_history_update",
        "GRANT UPDATE ON {}.{} TO {}",
        ("schema", "state_history", "role"),
    ),
    AclDrift(
        "extra_operation_history_delete",
        "GRANT DELETE ON {}.{} TO {}",
        ("schema", "operation_history", "role"),
    ),
    AclDrift(
        "extra_truncate",
        "GRANT TRUNCATE ON {}.{} TO {}",
        ("schema", "current_state", "role"),
    ),
    AclDrift(
        "extra_references",
        "GRANT REFERENCES ON {}.{} TO {}",
        ("schema", "current_state", "role"),
    ),
    AclDrift(
        "extra_trigger",
        "GRANT TRIGGER ON {}.{} TO {}",
        ("schema", "current_state", "role"),
    ),
    AclDrift(
        "extra_maintain_pg18",
        "GRANT MAINTAIN ON {}.{} TO {}",
        ("schema", "current_state", "role"),
        minimum_major=18,
    ),
    AclDrift(
        "grantable_schema_usage",
        "GRANT USAGE ON SCHEMA {} TO {} WITH GRANT OPTION",
        ("schema", "role"),
    ),
    AclDrift(
        "grantable_table_select",
        "GRANT SELECT ON {}.{} TO {} WITH GRANT OPTION",
        ("schema", "current_state", "role"),
    ),
    AclDrift(
        "grantable_column_insert",
        "GRANT INSERT ({}) ON {}.{} TO {} WITH GRANT OPTION",
        ("state_json", "schema", "current_state", "role"),
    ),
    AclDrift("public_schema_usage", "GRANT USAGE ON SCHEMA {} TO PUBLIC", ("schema",)),
    AclDrift(
        "public_table_select",
        "GRANT SELECT ON {}.{} TO PUBLIC",
        ("schema", "current_state"),
    ),
    AclDrift(
        "public_column_insert",
        "GRANT INSERT ({}) ON {}.{} TO PUBLIC",
        ("state_json", "schema", "current_state"),
    ),
)


def _drift_identifiers(case: object, writer: WriterIdentity, drift: AclDrift) -> list[object]:
    values = {
        "schema": case.schema,
        "role": writer.role,
        **{table: table for table in _TABLES},
        "namespace": "namespace",
        "state_json": "state_json",
        "control_json": "control_json",
    }
    return [case.sql.Identifier(values[name]) for name in drift.identifiers]


@pytest.mark.parametrize("drift", _ACL_DRIFTS, ids=str)
def test_every_missing_extra_grantable_and_public_acl_fails_closed(
    postgres_case: object,
    postgres_writer: WriterIdentity,
    drift: AclDrift,
) -> None:
    address = _address()
    _initializer(postgres_case).initialize(address)
    _migrate(postgres_case, postgres_writer)
    with postgres_case.psycopg.connect(postgres_case.admin_dsn) as connection:
        postgres_major = connection.info.server_version // 10_000
    if postgres_major < drift.minimum_major:
        pytest.skip(f"{drift.name} requires PostgreSQL {drift.minimum_major}+")

    with postgres_case.psycopg.connect(
        postgres_case.owner_dsn,
        autocommit=True,
    ) as connection:
        connection.execute(
            postgres_case.sql.SQL(drift.statement).format(
                *_drift_identifiers(postgres_case, postgres_writer, drift)
            )
        )

    with pytest.raises(
        StateBackendInvalidStateError,
        match=r"^PostgreSQL deployment state migration is incompatible$",
    ):
        _migrate(postgres_case, postgres_writer)


def test_empty_owner_default_acl_catalog_row_fails_closed(
    postgres_case: object,
    postgres_writer: WriterIdentity,
) -> None:
    _initializer(postgres_case).initialize(_address())
    catalog_oid: int | None = None
    try:
        # PostgreSQL normally removes an empty default-ACL row. Inject the
        # otherwise-valid catalog state directly to cover the row-presence
        # validator that aclexplode(empty-array) would hide. The CI bootstrap
        # identity is an isolated superuser, and the row is removed below.
        with postgres_case.psycopg.connect(
            postgres_case.admin_dsn,
            autocommit=True,
        ) as connection:
            connection.execute("SET allow_system_table_mods = on")
            row = connection.execute(
                "INSERT INTO pg_catalog.pg_default_acl "
                "(oid, defaclrole, defaclnamespace, defaclobjtype, defaclacl) "
                "SELECT (COALESCE((SELECT max(oid)::bigint "
                "FROM pg_catalog.pg_default_acl), 50000) + 1)::oid, r.oid, n.oid, "
                "'r', '{}'::aclitem[] FROM pg_catalog.pg_roles AS r, "
                "pg_catalog.pg_namespace AS n WHERE r.rolname = %s "
                "AND n.nspname = %s RETURNING oid::bigint",
                (postgres_case.owner_role, postgres_case.schema),
            ).fetchone()
            assert row is not None
            catalog_oid = row[0]
            assert type(catalog_oid) is int

        with pytest.raises(
            StateBackendInvalidStateError,
            match=r"^PostgreSQL deployment state migration is incompatible$",
        ):
            _migrate(postgres_case, postgres_writer)
        with postgres_case.psycopg.connect(postgres_case.admin_dsn) as connection:
            still_present = connection.execute(
                "SELECT defaclobjtype, defaclacl::text "
                "FROM pg_catalog.pg_default_acl WHERE oid = %s",
                (catalog_oid,),
            ).fetchone()
        assert still_present == ("r", "{}")
    finally:
        if catalog_oid is not None:
            with postgres_case.psycopg.connect(
                postgres_case.admin_dsn,
                autocommit=True,
            ) as connection:
                connection.execute("SET allow_system_table_mods = on")
                connection.execute(
                    "DELETE FROM pg_catalog.pg_default_acl WHERE oid = %s",
                    (catalog_oid,),
                )
    _assert_v1_unchanged(postgres_case, postgres_writer)


def test_owner_role_every_elevated_attribute_and_membership_direction_are_rejected(
    postgres_case: object,
    postgres_writer: WriterIdentity,
) -> None:
    address = _address()
    _initializer(postgres_case).initialize(address)

    with pytest.raises(StateBackendInvalidStateError):
        PrivatePostgresStateV2Migrator(
            dsn=postgres_case.owner_dsn,
            schema=postgres_case.schema,
            lock_timeout_seconds=10,
            writer_role=postgres_case.owner_role,
        ).migrate(
            confirmed_writer_role=postgres_case.owner_role,
            confirmed_store_id=_store_id(postgres_case),
        )

    attribute_drifts = (
        ("SUPERUSER", "NOSUPERUSER"),
        ("CREATEDB", "NOCREATEDB"),
        ("CREATEROLE", "NOCREATEROLE"),
        ("INHERIT", "NOINHERIT"),
        ("REPLICATION", "NOREPLICATION"),
        ("BYPASSRLS", "NOBYPASSRLS"),
        ("NOLOGIN", "LOGIN"),
    )
    for unsafe, restore in attribute_drifts:
        with postgres_case.psycopg.connect(
            postgres_case.admin_dsn,
            autocommit=True,
        ) as connection:
            connection.execute(
                postgres_case.sql.SQL(f"ALTER ROLE {{}} {unsafe}").format(
                    postgres_case.sql.Identifier(postgres_writer.role)
                )
            )
        with pytest.raises(StateBackendInvalidStateError):
            _migrate(postgres_case, postgres_writer)
        with postgres_case.psycopg.connect(
            postgres_case.admin_dsn,
            autocommit=True,
        ) as connection:
            connection.execute(
                postgres_case.sql.SQL(f"ALTER ROLE {{}} {restore}").format(
                    postgres_case.sql.Identifier(postgres_writer.role)
                )
            )

    memberships = (
        (postgres_case.reader_role, postgres_writer.role),
        (postgres_writer.role, postgres_case.reader_role),
    )
    for granted_role, member_role in memberships:
        with postgres_case.psycopg.connect(
            postgres_case.admin_dsn,
            autocommit=True,
        ) as connection:
            connection.execute(
                postgres_case.sql.SQL("GRANT {} TO {}").format(
                    postgres_case.sql.Identifier(granted_role),
                    postgres_case.sql.Identifier(member_role),
                )
            )
        with pytest.raises(StateBackendInvalidStateError):
            _migrate(postgres_case, postgres_writer)
        with postgres_case.psycopg.connect(
            postgres_case.admin_dsn,
            autocommit=True,
        ) as connection:
            connection.execute(
                postgres_case.sql.SQL("REVOKE {} FROM {}").format(
                    postgres_case.sql.Identifier(granted_role),
                    postgres_case.sql.Identifier(member_role),
                )
            )


def test_factory_remains_disabled_after_exact_v2_migration(
    postgres_case: object,
    postgres_writer: WriterIdentity,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _initializer(postgres_case).initialize(_address())
    _migrate(postgres_case, postgres_writer)
    env_name = "PRIVATE_POSTGRES_V2_WRITER_DSN"
    monkeypatch.setenv(env_name, postgres_writer.dsn)
    config = validate_deployment_state_config(
        {
            "backend": "postgres",
            "namespace": "platform",
            "postgres": {
                "dsn_env": env_name,
                "schema": postgres_case.schema,
            },
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
    assert os.environ[env_name] not in repr(config)
