"""Separate-process PostgreSQL initialization convergence contract."""

from __future__ import annotations

import multiprocessing
from queue import Empty

import pytest

from streamt.deployer.postgres_state import (
    PostgresStateAdministration,
    PostgresStateInitializer,
)
from streamt.deployer.state_backend import StateAddress

pytestmark = [pytest.mark.integration, pytest.mark.postgres]


def _initialize_child(
    dsn: str,
    schema: str,
    barrier: object,
    results: object,
) -> None:
    address = StateAddress(namespace="platform", project="payments", environment="prod")
    try:
        barrier.wait(timeout=30)
        initialized = PostgresStateInitializer(
            dsn=dsn,
            schema=schema,
            lock_timeout_seconds=20,
        ).initialize(address)
        results.put(("ok", initialized.store_id, initialized.outcome))
    except BaseException as error:
        results.put(("error", type(error).__name__, str(error)))


def test_two_processes_converge_on_one_store(postgres_case: object) -> None:
    context = multiprocessing.get_context("spawn")
    barrier = context.Barrier(2)
    results = context.Queue()
    processes = [
        context.Process(
            target=_initialize_child,
            args=(postgres_case.owner_dsn, postgres_case.schema, barrier, results),
        )
        for _ in range(2)
    ]
    try:
        for process in processes:
            process.start()
        for process in processes:
            process.join(timeout=45)
        assert all(not process.is_alive() for process in processes)
        assert [process.exitcode for process in processes] == [0, 0]
        try:
            outcomes = [results.get(timeout=5) for _ in processes]
        except Empty as error:
            raise AssertionError("initializer child did not report an outcome") from error
    finally:
        for process in processes:
            if process.is_alive():
                process.terminate()
            process.join(timeout=5)
        results.close()
        results.join_thread()

    assert all(outcome[0] == "ok" for outcome in outcomes), outcomes
    assert len({outcome[1] for outcome in outcomes}) == 1
    assert sorted(outcome[2] for outcome in outcomes) == [
        "already_initialized",
        "initialized",
    ]

    address = StateAddress(namespace="platform", project="payments", environment="prod")
    status = PostgresStateAdministration(
        dsn=postgres_case.owner_dsn,
        schema=postgres_case.schema,
        lock_timeout_seconds=10,
    ).status(address)
    assert status.store_status == "ready"
    assert status.store_id == outcomes[0][1]
    assert status.address_status == "registered"
    assert status.state_status == "absent"
