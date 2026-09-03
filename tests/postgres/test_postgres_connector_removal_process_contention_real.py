"""Two-process PostgreSQL-v2 contention gate for Connector removal apply."""

from __future__ import annotations

import json
import multiprocessing
import os
import queue
from pathlib import Path
from typing import Any, Protocol, cast
from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

from streamt.cli import main
from streamt.deployer.connect import (
    ConnectClusterBinding,
    ManagedConnectorObservation,
)
from streamt.deployer.kafka import KafkaDeployer
from streamt.deployer.state import LocalState
from tests.postgres.conftest import PostgresCase, WriterIdentity
from tests.postgres.test_postgres_connector_removal_concurrency_real import (
    _CONNECTOR,
    _OWNER_DSN_ENV,
    _WRITER_DSN_ENV,
    _binding,
    _initial_state,
    _lock_key,
    _prepare_reviewed_plan,
    _present_observation,
    _provider,
    _provision_v2,
    _seed_state,
    _service,
    _write_project,
)

pytestmark = [pytest.mark.integration, pytest.mark.postgres]


class _ProcessEvent(Protocol):
    def is_set(self) -> bool: ...

    def set(self) -> None: ...

    def wait(self, timeout: float | None = None) -> bool: ...


class _ProcessQueue(Protocol):
    def get(self, block: bool = True, timeout: float | None = None) -> object: ...

    def put(self, value: object) -> None: ...

    def close(self) -> None: ...

    def join_thread(self) -> None: ...


class _NoopKafka:
    """Configured Kafka dependency that cannot perform runtime work."""

    def close(self) -> None:
        return None


class _PausingConnect:
    """Exact deterministic Connector surface with a process-visible delete pause."""

    def __init__(
        self,
        *,
        observed: _ProcessEvent,
        delete_entered: _ProcessEvent,
        release_delete: _ProcessEvent,
    ) -> None:
        self.cluster_binding = _binding()
        self._observed = observed
        self._delete_entered = delete_entered
        self._release_delete = release_delete
        self.observe_calls = 0
        self.delete_calls = 0

    def require_cluster_binding(self) -> ConnectClusterBinding:
        return self.cluster_binding

    def resolve_connector_artifact(self, artifact: object) -> object:
        raise AssertionError("shared recovery planning must resolve Connector artifacts purely")

    def observe_managed_connector(self, connector_name: str) -> ManagedConnectorObservation:
        if connector_name != _CONNECTOR:
            raise AssertionError("apply observed an unexpected Connector target")
        self.observe_calls += 1
        self._observed.set()
        return _present_observation()

    def delete_managed_connector(
        self,
        current: ManagedConnectorObservation,
    ) -> str:
        if current != _present_observation():
            raise AssertionError("apply mutated a Connector outside its reviewed preimage")
        self.delete_calls += 1
        self._delete_entered.set()
        if not self._release_delete.wait(timeout=45):
            raise AssertionError("Connector delete pause timed out")
        return "deleted"

    def delete_connector(self, _connector_name: str) -> None:
        raise AssertionError("managed Connector deletion used the legacy delete route")

    def apply_connector(self, _artifact: object) -> str:
        raise AssertionError("Connector removal attempted a create or update")

    def close(self) -> None:
        return None


def _apply_worker(
    worker: str,
    project_dir: str,
    reviewed_plan: str,
    constructed: _ProcessEvent,
    observed: _ProcessEvent,
    delete_entered: _ProcessEvent,
    release_delete: _ProcessEvent,
    results: _ProcessQueue,
) -> None:
    """Invoke the public apply command in one independently spawned process."""
    connect: _PausingConnect | None = None
    factory_calls = 0

    def make_connect(*_args: object, **_kwargs: object) -> _PausingConnect:
        nonlocal connect, factory_calls
        factory_calls += 1
        constructed.set()
        connect = _PausingConnect(
            observed=observed,
            delete_entered=delete_entered,
            release_delete=release_delete,
        )
        return connect

    try:
        with (
            patch(
                "streamt.cli.commands.apply.make_kafka_deployer",
                return_value=_NoopKafka(),
            ),
            patch(
                "streamt.cli.commands.apply.make_connect_deployer",
                side_effect=make_connect,
            ),
        ):
            result = CliRunner().invoke(
                main,
                [
                    "-o",
                    "json",
                    "apply",
                    "-p",
                    project_dir,
                    "--plan",
                    reviewed_plan,
                    "--force",
                ],
            )
        results.put(
            {
                "worker": worker,
                "pid": os.getpid(),
                "exit_code": result.exit_code,
                "stdout": result.stdout,
                "factory_calls": factory_calls,
                "observe_calls": connect.observe_calls if connect is not None else 0,
                "delete_calls": connect.delete_calls if connect is not None else 0,
                "exception": None,
            }
        )
    except BaseException as error:
        # Provider and DSN exception text must never cross this process boundary.
        results.put(
            {
                "worker": worker,
                "pid": os.getpid(),
                "exit_code": -1,
                "stdout": "",
                "factory_calls": factory_calls,
                "observe_calls": connect.observe_calls if connect is not None else 0,
                "delete_calls": connect.delete_calls if connect is not None else 0,
                "exception": type(error).__name__,
            }
        )


def _result(values: _ProcessQueue, *, timeout: float = 60) -> dict[str, Any]:
    try:
        value = values.get(timeout=timeout)
    except queue.Empty:
        pytest.fail("timed out waiting for an apply process result")
    assert isinstance(value, dict)
    return cast(dict[str, Any], value)


def _json_payload(result: dict[str, Any]) -> dict[str, Any]:
    value = json.loads(result["stdout"])
    assert isinstance(value, dict)
    return cast(dict[str, Any], value)


def _assert_address_lock_is_held(postgres_case: PostgresCase) -> None:
    lock_key = _lock_key(postgres_case)
    with postgres_case.psycopg.connect(
        postgres_case.owner_dsn,
        autocommit=True,
    ) as contender:
        assert contender.execute(
            "SELECT pg_catalog.pg_try_advisory_lock(%s)",
            (lock_key,),
        ).fetchone() == (False,)


def test_two_apply_processes_serialize_before_connector_construction(
    tmp_path: Path,
    postgres_case: PostgresCase,
    postgres_writer: WriterIdentity,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A losing apply times out before Connect while the winner mutates once."""
    _write_project(tmp_path, postgres_case, lock_timeout=1)
    _provision_v2(postgres_case, postgres_writer)
    monkeypatch.delenv(_OWNER_DSN_ENV, raising=False)
    monkeypatch.setenv(_WRITER_DSN_ENV, postgres_writer.dsn)
    state_service = _service(postgres_case, postgres_writer)
    initial_state, unrelated_resource_id, unrelated_record = _initial_state()
    _seed_state(state_service, initial_state)

    planning_connect = _provider({"value": True})
    plan_path, reviewed = _prepare_reviewed_plan(
        tmp_path,
        connect=planning_connect,
        kafka=MagicMock(spec=KafkaDeployer),
    )
    assert len(reviewed.actions) == 1
    assert reviewed.actions[0].connector_evidence is not None

    context = multiprocessing.get_context("spawn")
    first_constructed = context.Event()
    first_observed = context.Event()
    first_delete_entered = context.Event()
    second_constructed = context.Event()
    second_observed = context.Event()
    second_delete_entered = context.Event()
    release_first = context.Event()
    never_release_second = context.Event()
    results = context.Queue()
    first = context.Process(
        target=_apply_worker,
        args=(
            "first",
            str(tmp_path),
            str(plan_path),
            first_constructed,
            first_observed,
            first_delete_entered,
            release_first,
            results,
        ),
    )
    second = context.Process(
        target=_apply_worker,
        args=(
            "second",
            str(tmp_path),
            str(plan_path),
            second_constructed,
            second_observed,
            second_delete_entered,
            never_release_second,
            results,
        ),
    )
    outcomes: dict[str, dict[str, Any]] = {}
    try:
        first.start()
        assert first_delete_entered.wait(timeout=45), (
            "first apply did not reach its paused Connector mutation"
        )
        assert first.is_alive()
        assert first_constructed.is_set()
        assert first_observed.is_set()
        _assert_address_lock_is_held(postgres_case)

        second.start()
        losing = _result(results, timeout=30)
        outcomes[cast(str, losing["worker"])] = losing
        assert losing["worker"] == "second"
        second.join(timeout=10)
        assert not second.is_alive()
        assert second.exitcode == 0

        losing_payload = _json_payload(losing)
        assert losing["exit_code"] == 1
        assert losing["exception"] is None
        assert losing["factory_calls"] == 0
        assert losing["observe_calls"] == 0
        assert losing["delete_calls"] == 0
        assert not second_constructed.is_set()
        assert not second_observed.is_set()
        assert not second_delete_entered.is_set()
        assert losing_payload["status"] == "error"
        assert losing_payload["errors"][0]["code"] == "E422_STATE_LOCK_TIMEOUT"

        # The winning process remains inside the provider callback and still
        # owns the address lock until the parent explicitly releases it.
        assert first.is_alive()
        _assert_address_lock_is_held(postgres_case)
        release_first.set()
        winning = _result(results, timeout=45)
        outcomes[cast(str, winning["worker"])] = winning
        assert winning["worker"] == "first"
        first.join(timeout=15)
        assert not first.is_alive()
        assert first.exitcode == 0

        winning_payload = _json_payload(winning)
        assert winning["exit_code"] == 0
        assert winning["exception"] is None
        assert winning["factory_calls"] == 1
        assert winning["observe_calls"] == 1
        assert winning["delete_calls"] == 1
        assert winning_payload["status"] == "ok"
        assert winning_payload["errors"] == []
        assert winning_payload["data"]["deleted"] == [f"connector:{_CONNECTOR}"]
        assert winning_payload["data"]["committed"] is True
        assert winning_payload["data"]["state_serial"] == initial_state.serial + 1
        assert winning_payload["data"]["plan_checksum"] == reviewed.checksum
    finally:
        release_first.set()
        never_release_second.set()
        for process in (first, second):
            if process.is_alive():
                process.terminate()
            process.join(timeout=5)
        results.close()
        results.join_thread()

    assert set(outcomes) == {"first", "second"}
    assert outcomes["first"]["pid"] != outcomes["second"]["pid"]
    assert outcomes["first"]["pid"] != os.getpid()
    assert outcomes["second"]["pid"] != os.getpid()

    expected_state = LocalState(
        project=initial_state.project,
        environment=initial_state.environment,
        serial=initial_state.serial + 1,
        resources={unrelated_resource_id: unrelated_record},
    )
    assert state_service.read().state == expected_state
    assert state_service.read_control().control.status == "clear"
