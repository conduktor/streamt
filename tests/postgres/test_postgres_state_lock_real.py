"""Real-server PostgreSQL advisory-lock probe contracts."""

from __future__ import annotations

import importlib
import multiprocessing
import time
from queue import Empty

import pytest

from streamt.deployer.postgres_state import (
    PostgresStateInitializer,
    PostgresStateLockProbe,
)
from streamt.deployer.state_backend import StateAddress

pytestmark = [pytest.mark.integration, pytest.mark.postgres]


def _address(*, project: str = "payments") -> StateAddress:
    return StateAddress(namespace="platform", project=project, environment="prod")


def _initializer(dsn: str, schema: str) -> PostgresStateInitializer:
    return PostgresStateInitializer(
        dsn=dsn,
        schema=schema,
        lock_timeout_seconds=10,
    )


def _probe(dsn: str, schema: str) -> PostgresStateLockProbe:
    return PostgresStateLockProbe(
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


def _address_lock_key(case: object, address: StateAddress) -> int:
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
    assert type(row[0]) is int
    return row[0]


def _administrative_row_counts(case: object) -> tuple[int, int]:
    with case.psycopg.connect(case.owner_dsn) as connection:
        address_count = connection.execute(
            case.sql.SQL("SELECT count(*) FROM {}.{}").format(
                case.sql.Identifier(case.schema),
                case.sql.Identifier("state_addresses"),
            )
        ).fetchone()[0]
        control_count = connection.execute(
            case.sql.SQL("SELECT count(*) FROM {}.{}").format(
                case.sql.Identifier(case.schema),
                case.sql.Identifier("operation_control"),
            )
        ).fetchone()[0]
    return address_count, control_count


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


def _try_lock_and_release(case: object, lock_key: int) -> None:
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


def _hold_lock_child(
    dsn: str,
    lock_key: int,
    ready: object,
    release: object,
) -> None:
    announced = False
    try:
        driver = importlib.import_module("psycopg")
        with driver.connect(dsn, autocommit=True) as connection:
            pid = connection.execute("SELECT pg_catalog.pg_backend_pid()").fetchone()[0]
            acquired = connection.execute(
                "SELECT pg_catalog.pg_try_advisory_lock(%s)",
                (lock_key,),
            ).fetchone()
            ready.put(("locked", pid, acquired))
            announced = True
            release.wait(timeout=30)
            connection.execute(
                "SELECT pg_catalog.pg_advisory_unlock(%s)",
                (lock_key,),
            )
    except BaseException as error:
        if not announced:
            ready.put(("error", type(error).__name__, str(error)))


def _start_lock_holder(case: object, lock_key: int) -> tuple[object, object, object]:
    context = multiprocessing.get_context("spawn")
    ready = context.Queue()
    release = context.Event()
    process = context.Process(
        target=_hold_lock_child,
        args=(case.owner_dsn, lock_key, ready, release),
    )
    process.start()
    try:
        outcome = ready.get(timeout=15)
    except Empty as error:
        process.terminate()
        process.join(timeout=5)
        ready.close()
        ready.join_thread()
        raise AssertionError("lock-holder child did not report readiness") from error
    if outcome[0] != "locked" or outcome[2] != (True,):
        release.set()
        process.join(timeout=5)
        if process.is_alive():
            process.terminate()
            process.join(timeout=5)
    ready.close()
    ready.join_thread()
    assert outcome[0] == "locked", outcome
    assert outcome[2] == (True,), outcome
    return process, release, outcome[1]


def _stop_lock_holder(process: object, release: object) -> None:
    release.set()
    process.join(timeout=10)
    if process.is_alive():
        process.terminate()
        process.join(timeout=5)
    assert process.is_alive() is False


def test_probe_uninitialized_store_is_unregistered_without_mutation(
    postgres_case: object,
) -> None:
    address = _address()

    result = _probe(postgres_case.owner_dsn, postgres_case.schema).probe(address)

    assert result.lock_status == "unregistered"
    assert _schema_exists(postgres_case) is False


def test_probe_ready_unregistered_address_does_not_register_it(
    postgres_case: object,
) -> None:
    registered = _address()
    unregistered = _address(project="settlements")
    _initializer(postgres_case.owner_dsn, postgres_case.schema).initialize(registered)
    before = _administrative_row_counts(postgres_case)

    result = _probe(postgres_case.owner_dsn, postgres_case.schema).probe(unregistered)

    assert result.lock_status == "unregistered"
    assert before == (1, 1)
    assert _administrative_row_counts(postgres_case) == before


def test_probe_available_releases_transaction_lock(postgres_case: object) -> None:
    address = _address()
    _initializer(postgres_case.owner_dsn, postgres_case.schema).initialize(address)
    lock_key = _address_lock_key(postgres_case, address)

    result = _probe(postgres_case.owner_dsn, postgres_case.schema).probe(address)

    assert result.lock_status == "available"
    _try_lock_and_release(postgres_case, lock_key)


def test_probe_reports_busy_for_separate_session_holder(postgres_case: object) -> None:
    address = _address()
    _initializer(postgres_case.owner_dsn, postgres_case.schema).initialize(address)
    lock_key = _address_lock_key(postgres_case, address)
    process, release, _pid = _start_lock_holder(postgres_case, lock_key)
    try:
        result = _probe(postgres_case.owner_dsn, postgres_case.schema).probe(address)
        assert result.lock_status == "busy"
    finally:
        _stop_lock_holder(process, release)

    assert process.exitcode == 0
    _try_lock_and_release(postgres_case, lock_key)


def test_terminated_holder_becomes_available(postgres_case: object) -> None:
    address = _address()
    _initializer(postgres_case.owner_dsn, postgres_case.schema).initialize(address)
    lock_key = _address_lock_key(postgres_case, address)
    process, release, pid = _start_lock_holder(postgres_case, lock_key)
    try:
        with postgres_case.psycopg.connect(
            postgres_case.admin_dsn,
            autocommit=True,
        ) as connection:
            terminated = connection.execute(
                "SELECT pg_catalog.pg_terminate_backend(%s)",
                (pid,),
            ).fetchone()
            assert terminated == (True,)
            deadline = time.monotonic() + 10
            while time.monotonic() < deadline:
                active = connection.execute(
                    "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_stat_activity WHERE pid = %s)",
                    (pid,),
                ).fetchone()
                if active == (False,):
                    break
                time.sleep(0.05)
            else:
                pytest.fail("terminated lock-holder session remained active")

        result = _probe(postgres_case.owner_dsn, postgres_case.schema).probe(address)
        assert result.lock_status == "available"
    finally:
        _stop_lock_holder(process, release)

    _try_lock_and_release(postgres_case, lock_key)


def test_probe_succeeds_for_default_read_only_reader(postgres_case: object) -> None:
    address = _address()
    _initializer(postgres_case.owner_dsn, postgres_case.schema).initialize(address)
    lock_key = _address_lock_key(postgres_case, address)
    _grant_reader(postgres_case)
    with postgres_case.psycopg.connect(postgres_case.reader_dsn) as connection:
        read_only = connection.execute("SHOW default_transaction_read_only").fetchone()

    result = _probe(postgres_case.reader_dsn, postgres_case.schema).probe(address)

    assert read_only == ("on",)
    assert result.lock_status == "available"
    _try_lock_and_release(postgres_case, lock_key)
