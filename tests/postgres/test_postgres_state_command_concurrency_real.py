"""Separate-process command contention against the private PostgreSQL writer."""

from __future__ import annotations

import json
import multiprocessing
import queue
import time
from pathlib import Path
from typing import Any, Protocol, cast
from unittest.mock import patch

import pytest
import yaml
from click.testing import CliRunner

from streamt.cli import main
from streamt.compiler.manifest import ArtifactOwnership, Manifest, TopicArtifact
from streamt.deployer.kafka import TopicState
from streamt.deployer.postgres_state import (
    PostgresStateInitializer,
    PrivatePostgresStateV2Migrator,
)
from streamt.deployer.postgres_state_backend import PrivatePostgresStateReadBackend
from streamt.deployer.state import local_state_path, resource_id
from streamt.deployer.state_backend import DeploymentStateService, StateAddress, local_control_path
from tests.postgres.conftest import PostgresCase, WriterIdentity

pytestmark = [pytest.mark.integration, pytest.mark.postgres]

_CONTENDERS = 20
_NAMESPACE = "platform"
_PROJECT = "command-concurrency"
_ENVIRONMENT = "default"
_LOGICAL_NAME = "orders"
_PHYSICAL_NAME = "orders.v1"


class _ProcessEvent(Protocol):
    def set(self) -> None: ...

    def wait(self, timeout: float | None = None) -> bool: ...


class _ProcessQueue(Protocol):
    def get(
        self,
        block: bool = True,
        timeout: float | None = None,
    ) -> object: ...

    def put(self, value: object) -> None: ...

    def close(self) -> None: ...

    def join_thread(self) -> None: ...


class _ReadOnlyKafka:
    """Deterministic live observation that cannot mutate Kafka."""

    def get_topic_state(
        self,
        name: str,
        *,
        strict_config: bool = False,
    ) -> TopicState:
        if name != _PHYSICAL_NAME or strict_config is not True:
            raise AssertionError("adoption used an unexpected Kafka observation")
        # Keep each command inside its PostgreSQL operation long enough for
        # independently spawned contenders to queue on the same session lock.
        time.sleep(0.05)
        return TopicState(
            name=_PHYSICAL_NAME,
            exists=True,
            partitions=3,
            replication_factor=1,
            config={"cleanup.policy": "delete"},
        )

    def apply_topic(self, *_args: object, **_kwargs: object) -> str:
        raise AssertionError("adoption must not mutate Kafka")

    def create_topic(self, *_args: object, **_kwargs: object) -> str:
        raise AssertionError("adoption must not mutate Kafka")

    def update_topic(self, *_args: object, **_kwargs: object) -> str:
        raise AssertionError("adoption must not mutate Kafka")

    def delete_topic(self, *_args: object, **_kwargs: object) -> str:
        raise AssertionError("adoption must not mutate Kafka")

    def close(self) -> None:
        return None


def _address() -> StateAddress:
    return StateAddress(
        namespace=_NAMESPACE,
        project=_PROJECT,
        environment=_ENVIRONMENT,
    )


def _topic() -> TopicArtifact:
    return TopicArtifact(
        name=_PHYSICAL_NAME,
        partitions=3,
        replication_factor=1,
        config={"cleanup.policy": "compact"},
        ownership=ArtifactOwnership(
            project=_PROJECT,
            owner_type="model",
            owner_name=_LOGICAL_NAME,
            mode="adopted",
        ),
    )


def _manifest() -> Manifest:
    return Manifest(
        version="1.0",
        project_name=_PROJECT,
        artifacts={"topics": [_topic().to_dict()]},
    )


def _write_project(path: Path) -> None:
    (path / "stream_project.yml").write_text(
        yaml.safe_dump(
            {
                "apiVersion": "streamt.dev/v1alpha1",
                "project": {"name": _PROJECT},
                "runtime": {"kafka": {"bootstrap_servers": "unreachable.invalid:9092"}},
                "deployment_state": {
                    "backend": "postgres",
                    "namespace": _NAMESPACE,
                    "lock_timeout_seconds": 60,
                    "postgres": {
                        "dsn_env": "COMMAND_CONCURRENCY_DSN",
                        "writer_role_env": "COMMAND_CONCURRENCY_WRITER",
                        "schema": "configured_by_test_injection",
                    },
                },
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )


def _adopt_worker(
    worker_index: int,
    project_dir: str,
    writer_dsn: str,
    schema: str,
    start_event: _ProcessEvent,
    ready_queue: _ProcessQueue,
    result_queue: _ProcessQueue,
) -> None:
    """Run one fully isolated CLI process with a private writer injection."""
    try:
        backend = PrivatePostgresStateReadBackend(
            dsn=writer_dsn,
            schema=schema,
            lock_timeout_seconds=60,
        )
        service = DeploymentStateService(backend=backend, address=_address())
        kafka = _ReadOnlyKafka()
        ready_queue.put(("ready", worker_index))
        if not start_event.wait(timeout=60):
            result_queue.put((worker_index, -1, "", "start_timeout"))
            return
        resource_uri = resource_id(
            _PROJECT,
            _ENVIRONMENT,
            "topic",
            _LOGICAL_NAME,
        )
        args = [
            "-o",
            "json",
            "adopt",
            "-p",
            project_dir,
            "-e",
            _ENVIRONMENT,
            "--kind",
            "topic",
            "--name",
            _LOGICAL_NAME,
            "--confirm-resource",
            resource_uri,
            "--confirm-env",
            _ENVIRONMENT,
        ]
        with (
            patch("streamt.compiler.Compiler.compile", return_value=_manifest()),
            patch(
                "streamt.cli.commands.adopt.make_deployment_state_service",
                return_value=service,
            ),
            patch(
                "streamt.cli.commands.adopt.make_kafka_deployer",
                return_value=kafka,
            ),
        ):
            result = CliRunner().invoke(main, args)
        result_queue.put((worker_index, result.exit_code, result.stdout, None))
    except BaseException as error:
        # Only the exception type crosses the process boundary. Provider text
        # can contain endpoint or credential material and is never serialized.
        result_queue.put((worker_index, -1, "", type(error).__name__))


def _payload(output: str) -> dict[str, Any]:
    return cast(dict[str, Any], json.loads(output))


def _collect_queue(
    values: _ProcessQueue,
    *,
    count: int,
    timeout_seconds: float,
) -> list[object]:
    deadline = time.monotonic() + timeout_seconds
    collected: list[object] = []
    while len(collected) < count:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            pytest.fail(f"received {len(collected)} of {count} process messages before timeout")
        try:
            collected.append(values.get(timeout=remaining))
        except queue.Empty:
            pytest.fail(f"received {len(collected)} of {count} process messages before timeout")
    return collected


def _assert_lock_released(postgres_case: PostgresCase) -> None:
    address = _address()
    with postgres_case.psycopg.connect(
        postgres_case.admin_dsn,
        autocommit=True,
    ) as connection:
        lock_key_row = connection.execute(
            postgres_case.sql.SQL(
                "SELECT advisory_lock_key FROM {}.{} WHERE namespace = %s "
                "AND project = %s AND environment = %s"
            ).format(
                postgres_case.sql.Identifier(postgres_case.schema),
                postgres_case.sql.Identifier("state_addresses"),
            ),
            (address.namespace, address.project, address.environment),
        ).fetchone()
        assert lock_key_row is not None
        lock_key = lock_key_row[0]
        assert type(lock_key) is int
        assert connection.execute(
            "SELECT pg_catalog.pg_try_advisory_lock(%s)",
            (lock_key,),
        ).fetchone() == (True,)
        assert connection.execute(
            "SELECT pg_catalog.pg_advisory_unlock(%s)",
            (lock_key,),
        ).fetchone() == (True,)


def test_twenty_adoption_processes_converge_on_one_remote_state_commit(
    postgres_case: PostgresCase,
    postgres_writer: WriterIdentity,
    tmp_path: Path,
) -> None:
    _write_project(tmp_path)
    initialized = PostgresStateInitializer(
        dsn=postgres_case.owner_dsn,
        schema=postgres_case.schema,
        lock_timeout_seconds=60,
    ).initialize(_address())
    PrivatePostgresStateV2Migrator(
        dsn=postgres_case.owner_dsn,
        schema=postgres_case.schema,
        lock_timeout_seconds=60,
        writer_role=postgres_writer.role,
    ).migrate(
        confirmed_writer_role=postgres_writer.role,
        confirmed_store_id=initialized.store_id,
    )

    context = multiprocessing.get_context("spawn")
    start_event = context.Event()
    ready_queue = context.Queue()
    result_queue = context.Queue()
    processes = [
        context.Process(
            target=_adopt_worker,
            args=(
                index,
                str(tmp_path),
                postgres_writer.dsn,
                postgres_case.schema,
                start_event,
                ready_queue,
                result_queue,
            ),
        )
        for index in range(_CONTENDERS)
    ]
    outcomes: list[object] = []
    try:
        for process in processes:
            process.start()
        ready = _collect_queue(
            ready_queue,
            count=_CONTENDERS,
            timeout_seconds=90,
        )
        assert {cast(tuple[str, int], item) for item in ready} == {
            ("ready", index) for index in range(_CONTENDERS)
        }
        start_event.set()
        outcomes = _collect_queue(
            result_queue,
            count=_CONTENDERS,
            timeout_seconds=180,
        )
        for process in processes:
            process.join(timeout=30)
        assert all(not process.is_alive() for process in processes)
        assert [process.exitcode for process in processes] == [0] * _CONTENDERS
    finally:
        start_event.set()
        for process in processes:
            if process.is_alive():
                process.terminate()
            process.join(timeout=5)
        ready_queue.close()
        ready_queue.join_thread()
        result_queue.close()
        result_queue.join_thread()

    typed_outcomes = sorted(
        (cast(tuple[int, int, str, str | None], item) for item in outcomes),
        key=lambda item: item[0],
    )
    assert [item[0] for item in typed_outcomes] == list(range(_CONTENDERS))
    assert [item[1] for item in typed_outcomes] == [0] * _CONTENDERS
    assert [item[3] for item in typed_outcomes] == [None] * _CONTENDERS

    payloads = [_payload(item[2]) for item in typed_outcomes]
    assert sum(payload["data"]["adopted"] is True for payload in payloads) == 1
    assert sum(payload["data"]["already_owned"] is True for payload in payloads) == 19
    assert {payload["data"]["state_serial"] for payload in payloads} == {1}
    for payload in payloads:
        assert payload["status"] == "ok"
        assert payload["errors"] == []
        assert "state_file" not in payload["data"]
        assert all(
            warning.get("code") != "W106_LOCAL_STATE_ONLY" for warning in payload["warnings"]
        )

    combined_output = "".join(item[2] for item in typed_outcomes)
    writer_details = postgres_case.conninfo.conninfo_to_dict(postgres_writer.dsn)
    for secret in (
        postgres_writer.dsn,
        postgres_writer.role,
        postgres_case.owner_dsn,
        cast(str, writer_details["password"]),
    ):
        assert secret not in combined_output

    backend = PrivatePostgresStateReadBackend(
        dsn=postgres_writer.dsn,
        schema=postgres_case.schema,
        lock_timeout_seconds=60,
    )
    snapshot = backend.read_snapshot(_address())
    expected_resource = resource_id(
        _PROJECT,
        _ENVIRONMENT,
        "topic",
        _LOGICAL_NAME,
    )
    assert snapshot.state.state.serial == 1
    assert set(snapshot.state.state.resources) == {expected_resource}
    assert snapshot.control.control.status == "clear"

    with postgres_case.psycopg.connect(postgres_case.owner_dsn) as connection:
        state_rows = connection.execute(
            postgres_case.sql.SQL(
                "SELECT revision, state_serial FROM {}.{} WHERE namespace = %s "
                "AND project = %s AND environment = %s"
            ).format(
                postgres_case.sql.Identifier(postgres_case.schema),
                postgres_case.sql.Identifier("current_state"),
            ),
            (_NAMESPACE, _PROJECT, _ENVIRONMENT),
        ).fetchall()
        control_rows = connection.execute(
            postgres_case.sql.SQL(
                "SELECT revision, status FROM {}.{} WHERE namespace = %s "
                "AND project = %s AND environment = %s"
            ).format(
                postgres_case.sql.Identifier(postgres_case.schema),
                postgres_case.sql.Identifier("operation_control"),
            ),
            (_NAMESPACE, _PROJECT, _ENVIRONMENT),
        ).fetchall()
        state_history = connection.execute(
            postgres_case.sql.SQL(
                "SELECT operation_id::text, revision, state_serial FROM {}.{} "
                "WHERE namespace = %s AND project = %s AND environment = %s"
            ).format(
                postgres_case.sql.Identifier(postgres_case.schema),
                postgres_case.sql.Identifier("state_history"),
            ),
            (_NAMESPACE, _PROJECT, _ENVIRONMENT),
        ).fetchall()
        operation_history = connection.execute(
            postgres_case.sql.SQL(
                "SELECT operation_id::text, event_index, event_kind FROM {}.{} "
                "WHERE namespace = %s AND project = %s AND environment = %s "
                "ORDER BY event_index"
            ).format(
                postgres_case.sql.Identifier(postgres_case.schema),
                postgres_case.sql.Identifier("operation_history"),
            ),
            (_NAMESPACE, _PROJECT, _ENVIRONMENT),
        ).fetchall()

    assert state_rows == [(1, 1)]
    assert control_rows == [(4, "clear")]
    assert len(state_history) == 1
    operation_id = state_history[0][0]
    assert state_history == [(operation_id, 1, 1)]
    assert operation_history == [
        (operation_id, 0, "intent"),
        (operation_id, 1, "progress_started"),
        (operation_id, 2, "progress_completed"),
        (operation_id, 3, "succeeded"),
    ]
    _assert_lock_released(postgres_case)
    assert not local_state_path(tmp_path, environment=_ENVIRONMENT).exists()
    assert not local_control_path(tmp_path, environment=_ENVIRONMENT).exists()
