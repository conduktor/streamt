"""Real-server PostgreSQL catalog, initialization, and rollback contracts."""

from __future__ import annotations

from collections.abc import Callable

import pytest

from streamt.deployer.postgres_state import (
    PostgresStateAdministration,
    PostgresStateInitializer,
)
from streamt.deployer.state_backend import (
    StateAddress,
    StateBackendInvalidStateError,
    StateBackendUnavailableError,
)

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


def _address() -> StateAddress:
    return StateAddress(namespace="platform", project="payments", environment="prod")


def _administration(dsn: str, schema: str) -> PostgresStateAdministration:
    return PostgresStateAdministration(
        dsn=dsn,
        schema=schema,
        lock_timeout_seconds=10,
    )


def _initializer(dsn: str, schema: str) -> PostgresStateInitializer:
    return PostgresStateInitializer(
        dsn=dsn,
        schema=schema,
        lock_timeout_seconds=10,
    )


def _schema_exists(case: object) -> bool:
    with case.psycopg.connect(case.admin_dsn) as connection:
        row = connection.execute(
            "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_namespace WHERE nspname = %s)",
            (case.schema,),
        ).fetchone()
    return row == (True,)


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


def _catalog_and_row_counts(case: object) -> tuple[tuple[str, ...], dict[str, int]]:
    with case.psycopg.connect(case.owner_dsn) as connection:
        relations = tuple(
            row[0]
            for row in connection.execute(
                "SELECT c.relname FROM pg_catalog.pg_class AS c "
                "JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace "
                "WHERE n.nspname = %s AND c.relkind = 'r' ORDER BY c.relname",
                (case.schema,),
            ).fetchall()
        )
        counts: dict[str, int] = {}
        for table in _TABLES:
            row = connection.execute(
                case.sql.SQL("SELECT count(*) FROM {}.{}").format(
                    case.sql.Identifier(case.schema),
                    case.sql.Identifier(table),
                )
            ).fetchone()
            counts[table] = row[0]
    return relations, counts


def _assert_status_rejects_catalog(
    case: object,
    address: StateAddress,
    *,
    dsn: str | None = None,
) -> None:
    with pytest.raises(
        StateBackendInvalidStateError,
        match=r"^PostgreSQL deployment state is invalid$",
    ):
        _administration(dsn or case.owner_dsn, case.schema).status(address)


def test_missing_schema_status_is_uninitialized_and_read_only(postgres_case: object) -> None:
    status = _administration(postgres_case.owner_dsn, postgres_case.schema).status(_address())

    assert status.store_status == "uninitialized"
    assert status.address_status == "unregistered"
    assert status.state_status == "unregistered"
    assert _schema_exists(postgres_case) is False


def test_real_ddl_status_and_repeated_initialization(postgres_case: object) -> None:
    address = _address()
    initializer = _initializer(postgres_case.owner_dsn, postgres_case.schema)

    first = initializer.initialize(address)
    second = initializer.initialize(address)

    assert first.created_store is True
    assert first.registered_address is True
    assert second.created_store is False
    assert second.registered_address is False
    assert second.store_id == first.store_id
    relations, counts = _catalog_and_row_counts(postgres_case)
    assert relations == _TABLES
    assert counts == {
        "current_state": 0,
        "operation_control": 1,
        "operation_history": 0,
        "schema_migrations": 1,
        "state_addresses": 1,
        "state_history": 0,
        "store_metadata": 1,
    }

    _grant_reader(postgres_case)
    status = _administration(postgres_case.reader_dsn, postgres_case.schema).status(address)
    assert status.store_status == "ready"
    assert status.store_id == first.store_id
    assert status.address_status == "registered"
    assert status.state_status == "absent"
    assert status.state_serial == 0
    assert status.operation_status is not None
    assert status.operation_status.to_dict() == {
        "status": "clear",
        "operation_id": None,
        "kind": None,
        "failure_code": None,
        "last_completed_action_index": None,
    }


def test_real_catalog_drift_fails_closed(postgres_case: object) -> None:
    address = _address()
    _initializer(postgres_case.owner_dsn, postgres_case.schema).initialize(address)
    with postgres_case.psycopg.connect(postgres_case.owner_dsn) as connection:
        connection.execute(
            postgres_case.sql.SQL("ALTER TABLE {}.{} ADD COLUMN unexpected text").format(
                postgres_case.sql.Identifier(postgres_case.schema),
                postgres_case.sql.Identifier("current_state"),
            )
        )

    _assert_status_rejects_catalog(postgres_case, address)


def test_poisoned_search_path_cannot_shadow_schema_functions(postgres_case: object) -> None:
    address = _address()
    poison_schema = f"poison_{postgres_case.schema.removeprefix('streamt_ci_')}"
    with postgres_case.psycopg.connect(
        postgres_case.owner_dsn,
        autocommit=True,
    ) as connection:
        connection.execute(
            postgres_case.sql.SQL("CREATE SCHEMA {} AUTHORIZATION CURRENT_USER").format(
                postgres_case.sql.Identifier(poison_schema)
            )
        )
        connection.execute(
            postgres_case.sql.SQL(
                "CREATE FUNCTION {}.strpos(text, text) RETURNS integer "
                "LANGUAGE sql IMMUTABLE STRICT AS 'SELECT 0'"
            ).format(postgres_case.sql.Identifier(poison_schema))
        )
        connection.execute(
            postgres_case.sql.SQL(
                "CREATE FUNCTION {}.octet_length(text) RETURNS integer "
                "LANGUAGE sql IMMUTABLE STRICT AS 'SELECT 0'"
            ).format(postgres_case.sql.Identifier(poison_schema))
        )

    poisoned_dsn = postgres_case.conninfo.make_conninfo(
        postgres_case.owner_dsn,
        options=f"-c search_path={poison_schema},pg_catalog",
    )
    try:
        initialized = _initializer(poisoned_dsn, postgres_case.schema).initialize(address)
        poisoned_status = _administration(poisoned_dsn, postgres_case.schema).status(address)
        canonical_status = _administration(
            postgres_case.owner_dsn,
            postgres_case.schema,
        ).status(address)
    finally:
        with postgres_case.psycopg.connect(
            postgres_case.admin_dsn,
            autocommit=True,
        ) as connection:
            connection.execute(
                postgres_case.sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(
                    postgres_case.sql.Identifier(poison_schema)
                )
            )

    assert initialized.created_store is True
    assert poisoned_status.store_status == "ready"
    assert canonical_status.store_status == "ready"
    assert canonical_status.store_id == initialized.store_id
    assert canonical_status.address_status == "registered"
    assert canonical_status.state_status == "absent"


def test_default_privileges_are_neutralized_and_acl_or_owner_drift_is_rejected(
    postgres_case: object,
) -> None:
    address = _address()
    with postgres_case.psycopg.connect(
        postgres_case.admin_dsn,
        autocommit=True,
    ) as connection:
        connection.execute(
            postgres_case.sql.SQL(
                "ALTER DEFAULT PRIVILEGES FOR ROLE {} "
                "GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO PUBLIC"
            ).format(postgres_case.sql.Identifier(postgres_case.owner_role))
        )

    try:
        initialized = _initializer(
            postgres_case.owner_dsn,
            postgres_case.schema,
        ).initialize(address)
    finally:
        with postgres_case.psycopg.connect(
            postgres_case.admin_dsn,
            autocommit=True,
        ) as connection:
            connection.execute(
                postgres_case.sql.SQL(
                    "ALTER DEFAULT PRIVILEGES FOR ROLE {} "
                    "REVOKE ALL PRIVILEGES ON TABLES FROM PUBLIC"
                ).format(postgres_case.sql.Identifier(postgres_case.owner_role))
            )

    with postgres_case.psycopg.connect(postgres_case.admin_dsn) as connection:
        owner_row = connection.execute(
            "SELECT r.rolname FROM pg_catalog.pg_namespace AS n "
            "JOIN pg_catalog.pg_roles AS r ON r.oid = n.nspowner "
            "WHERE n.nspname = %s",
            (postgres_case.schema,),
        ).fetchone()
        table_owners = connection.execute(
            "SELECT DISTINCT r.rolname FROM pg_catalog.pg_class AS c "
            "JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace "
            "JOIN pg_catalog.pg_roles AS r ON r.oid = c.relowner "
            "WHERE n.nspname = %s AND c.relkind = 'r'",
            (postgres_case.schema,),
        ).fetchall()
        non_owner_schema_acl = connection.execute(
            "SELECT acl.grantee, acl.privilege_type "
            "FROM pg_catalog.pg_namespace AS n "
            "CROSS JOIN LATERAL pg_catalog.aclexplode("
            "COALESCE(n.nspacl, pg_catalog.acldefault('n', n.nspowner))) AS acl "
            "WHERE n.nspname = %s AND acl.grantee <> n.nspowner",
            (postgres_case.schema,),
        ).fetchall()
        non_owner_table_acl = connection.execute(
            "SELECT c.relname, acl.grantee, acl.privilege_type "
            "FROM pg_catalog.pg_class AS c "
            "JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace "
            "CROSS JOIN LATERAL pg_catalog.aclexplode("
            "COALESCE(c.relacl, pg_catalog.acldefault('r', c.relowner))) AS acl "
            "WHERE n.nspname = %s AND c.relkind = 'r' "
            "AND acl.grantee <> c.relowner",
            (postgres_case.schema,),
        ).fetchall()

    assert initialized.created_store is True
    assert owner_row == (postgres_case.owner_role,)
    assert table_owners == [(postgres_case.owner_role,)]
    assert non_owner_schema_acl == []
    assert non_owner_table_acl == []
    _grant_reader(postgres_case)
    assert _administration(postgres_case.reader_dsn, postgres_case.schema).status(
        address
    ).store_status == "ready"

    with postgres_case.psycopg.connect(
        postgres_case.admin_dsn,
        autocommit=True,
    ) as connection:
        connection.execute(
            postgres_case.sql.SQL("GRANT CREATE ON SCHEMA {} TO PUBLIC").format(
                postgres_case.sql.Identifier(postgres_case.schema)
            )
        )
    _assert_status_rejects_catalog(
        postgres_case,
        address,
        dsn=postgres_case.reader_dsn,
    )
    with postgres_case.psycopg.connect(
        postgres_case.admin_dsn,
        autocommit=True,
    ) as connection:
        connection.execute(
            postgres_case.sql.SQL("REVOKE CREATE ON SCHEMA {} FROM PUBLIC").format(
                postgres_case.sql.Identifier(postgres_case.schema)
            )
        )
        connection.execute(
            postgres_case.sql.SQL("GRANT INSERT ON {}.{} TO PUBLIC").format(
                postgres_case.sql.Identifier(postgres_case.schema),
                postgres_case.sql.Identifier("current_state"),
            )
        )
    _assert_status_rejects_catalog(
        postgres_case,
        address,
        dsn=postgres_case.reader_dsn,
    )
    with postgres_case.psycopg.connect(
        postgres_case.admin_dsn,
        autocommit=True,
    ) as connection:
        connection.execute(
            postgres_case.sql.SQL("REVOKE INSERT ON {}.{} FROM PUBLIC").format(
                postgres_case.sql.Identifier(postgres_case.schema),
                postgres_case.sql.Identifier("current_state"),
            )
        )
        postgres_major = connection.info.server_version // 10_000
        if postgres_major >= 18:
            connection.execute(
                postgres_case.sql.SQL("GRANT MAINTAIN ON {}.{} TO {}").format(
                    postgres_case.sql.Identifier(postgres_case.schema),
                    postgres_case.sql.Identifier("current_state"),
                    postgres_case.sql.Identifier(postgres_case.reader_role),
                )
            )
    if postgres_major >= 18:
        _assert_status_rejects_catalog(
            postgres_case,
            address,
            dsn=postgres_case.reader_dsn,
        )
        with postgres_case.psycopg.connect(
            postgres_case.admin_dsn,
            autocommit=True,
        ) as connection:
            connection.execute(
                postgres_case.sql.SQL("REVOKE MAINTAIN ON {}.{} FROM {}").format(
                    postgres_case.sql.Identifier(postgres_case.schema),
                    postgres_case.sql.Identifier("current_state"),
                    postgres_case.sql.Identifier(postgres_case.reader_role),
                )
            )
    with postgres_case.psycopg.connect(
        postgres_case.admin_dsn,
        autocommit=True,
    ) as connection:
        connection.execute(
            postgres_case.sql.SQL("ALTER TABLE {}.{} OWNER TO {}").format(
                postgres_case.sql.Identifier(postgres_case.schema),
                postgres_case.sql.Identifier("current_state"),
                postgres_case.sql.Identifier(postgres_case.reader_role),
            )
        )
    _assert_status_rejects_catalog(
        postgres_case,
        address,
        dsn=postgres_case.reader_dsn,
    )
    with postgres_case.psycopg.connect(
        postgres_case.admin_dsn,
        autocommit=True,
    ) as connection:
        connection.execute(
            postgres_case.sql.SQL("ALTER TABLE {}.{} OWNER TO {}").format(
                postgres_case.sql.Identifier(postgres_case.schema),
                postgres_case.sql.Identifier("current_state"),
                postgres_case.sql.Identifier(postgres_case.owner_role),
            )
        )
        connection.execute(
            postgres_case.sql.SQL("ALTER SCHEMA {} OWNER TO {}").format(
                postgres_case.sql.Identifier(postgres_case.schema),
                postgres_case.sql.Identifier(postgres_case.reader_role),
            )
        )
    _assert_status_rejects_catalog(
        postgres_case,
        address,
        dsn=postgres_case.reader_dsn,
    )
    with postgres_case.psycopg.connect(
        postgres_case.admin_dsn,
        autocommit=True,
    ) as connection:
        connection.execute(
            postgres_case.sql.SQL("ALTER SCHEMA {} OWNER TO {}").format(
                postgres_case.sql.Identifier(postgres_case.schema),
                postgres_case.sql.Identifier(postgres_case.owner_role),
            )
        )


def test_precommit_failure_rolls_back_real_ddl(
    postgres_case: object,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original: Callable[..., object] = PostgresStateInitializer._initialize_transaction
    transaction_body_completed = False

    def fail_after_transaction_body(
        self: PostgresStateInitializer,
        cursor: object,
        sql_module: object,
        address: StateAddress,
    ) -> object:
        nonlocal transaction_body_completed
        original(self, cursor, sql_module, address)
        transaction_body_completed = True
        raise RuntimeError("injected before commit")

    monkeypatch.setattr(
        PostgresStateInitializer,
        "_initialize_transaction",
        fail_after_transaction_body,
    )

    with pytest.raises(
        StateBackendUnavailableError,
        match=r"^PostgreSQL deployment state initialization is unavailable$",
    ):
        _initializer(postgres_case.owner_dsn, postgres_case.schema).initialize(_address())

    assert transaction_body_completed is True
    assert _schema_exists(postgres_case) is False
    status = _administration(postgres_case.owner_dsn, postgres_case.schema).status(_address())
    assert status.store_status == "uninitialized"
