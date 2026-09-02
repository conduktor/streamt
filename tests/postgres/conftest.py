"""Isolated credentials and schemas for real PostgreSQL conformance tests."""

from __future__ import annotations

import importlib
import os
import uuid
from collections.abc import Generator
from dataclasses import dataclass
from types import ModuleType

import pytest

_ADMIN_DSN_ENV = "STREAMT_TEST_POSTGRES_ADMIN_DSN"
_ENDPOINT_ENVIRONMENT_VARIABLES = (
    "PGHOST",
    "PGHOSTADDR",
    "PGPORT",
    "PGSERVICE",
    "PGSERVICEFILE",
)


@dataclass(frozen=True)
class PostgresCase:
    """One isolated schema with non-superuser owner and read-only identities."""

    admin_dsn: str
    owner_dsn: str
    reader_dsn: str
    owner_role: str
    reader_role: str
    schema: str
    psycopg: ModuleType
    sql: ModuleType
    conninfo: ModuleType


@pytest.fixture(autouse=True)
def clear_libpq_endpoint_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Keep endpoint selection entirely inside each explicit test DSN."""
    for name in _ENDPOINT_ENVIRONMENT_VARIABLES:
        monkeypatch.delenv(name, raising=False)


@pytest.fixture(scope="session")
def postgres_admin_dsn() -> str:
    dsn = os.environ.get(_ADMIN_DSN_ENV)
    if dsn is None or not dsn.strip():
        pytest.skip(f"{_ADMIN_DSN_ENV} is not set")
    return dsn


@pytest.fixture(scope="session")
def psycopg_modules(postgres_admin_dsn: str) -> tuple[ModuleType, ModuleType, ModuleType]:
    del postgres_admin_dsn
    try:
        driver = importlib.import_module("psycopg")
        sql_module = importlib.import_module("psycopg.sql")
        conninfo_module = importlib.import_module("psycopg.conninfo")
    except (ImportError, ModuleNotFoundError) as error:
        pytest.fail(f"PostgreSQL extra is missing: {type(error).__name__}")
    return driver, sql_module, conninfo_module


@pytest.fixture
def postgres_case(
    clear_libpq_endpoint_environment: None,
    postgres_admin_dsn: str,
    psycopg_modules: tuple[ModuleType, ModuleType, ModuleType],
) -> Generator[PostgresCase, None, None]:
    del clear_libpq_endpoint_environment
    driver, sql_module, conninfo_module = psycopg_modules
    suffix = uuid.uuid4().hex[:16]
    schema = f"streamt_ci_{suffix}"
    owner_role = f"streamt_owner_{suffix}"
    reader_role = f"streamt_reader_{suffix}"
    owner_password = f"owner-ci-{suffix}"
    reader_password = f"reader-ci-{suffix}"

    with driver.connect(postgres_admin_dsn, autocommit=True) as connection:
        database = connection.execute("SELECT current_database()").fetchone()[0]
        connection.execute(
            sql_module.SQL(
                "CREATE ROLE {} LOGIN PASSWORD {} NOSUPERUSER NOCREATEDB "
                "NOCREATEROLE NOINHERIT NOREPLICATION"
            ).format(
                sql_module.Identifier(owner_role),
                sql_module.Literal(owner_password),
            )
        )
        connection.execute(
            sql_module.SQL(
                "CREATE ROLE {} LOGIN PASSWORD {} NOSUPERUSER NOCREATEDB "
                "NOCREATEROLE NOINHERIT NOREPLICATION"
            ).format(
                sql_module.Identifier(reader_role),
                sql_module.Literal(reader_password),
            )
        )
        connection.execute(
            sql_module.SQL("GRANT CONNECT, CREATE ON DATABASE {} TO {}").format(
                sql_module.Identifier(database),
                sql_module.Identifier(owner_role),
            )
        )
        connection.execute(
            sql_module.SQL("GRANT CONNECT ON DATABASE {} TO {}").format(
                sql_module.Identifier(database),
                sql_module.Identifier(reader_role),
            )
        )
        connection.execute(
            sql_module.SQL("ALTER ROLE {} SET default_transaction_read_only = on").format(
                sql_module.Identifier(reader_role)
            )
        )

    make_conninfo = conninfo_module.make_conninfo
    owner_dsn = make_conninfo(
        postgres_admin_dsn,
        user=owner_role,
        password=owner_password,
    )
    reader_dsn = make_conninfo(
        postgres_admin_dsn,
        user=reader_role,
        password=reader_password,
    )
    case = PostgresCase(
        admin_dsn=postgres_admin_dsn,
        owner_dsn=owner_dsn,
        reader_dsn=reader_dsn,
        owner_role=owner_role,
        reader_role=reader_role,
        schema=schema,
        psycopg=driver,
        sql=sql_module,
        conninfo=conninfo_module,
    )
    try:
        yield case
    finally:
        with driver.connect(postgres_admin_dsn, autocommit=True) as connection:
            connection.execute(
                sql_module.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(
                    sql_module.Identifier(schema)
                )
            )
            connection.execute(
                sql_module.SQL("REVOKE ALL PRIVILEGES ON DATABASE {} FROM {}, {}").format(
                    sql_module.Identifier(database),
                    sql_module.Identifier(reader_role),
                    sql_module.Identifier(owner_role),
                )
            )
            connection.execute(
                sql_module.SQL("DROP ROLE IF EXISTS {}").format(
                    sql_module.Identifier(reader_role)
                )
            )
            connection.execute(
                sql_module.SQL("DROP ROLE IF EXISTS {}").format(
                    sql_module.Identifier(owner_role)
                )
            )
