"""Real PostgreSQL composition failures through the apply command boundary.

The ordinary provider factory remains disabled.  These tests inject the private
least-privilege writer backend at the command-local factory import and exercise
only failure composition that unit/backend tests cannot prove independently.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Literal
from unittest.mock import MagicMock, patch

import pytest
import yaml
from click.testing import CliRunner, Result

from streamt.cli import main
from streamt.compiler.manifest import ArtifactOwnership, Manifest, TopicArtifact
from streamt.deployer import postgres_state as postgres_state_module
from streamt.deployer import postgres_state_backend as postgres_backend_module
from streamt.deployer.kafka import TopicChange, TopicState
from streamt.deployer.postgres_state import (
    PostgresStateInitializer,
    PrivatePostgresStateV2Migrator,
)
from streamt.deployer.postgres_state_backend import PrivatePostgresStateReadBackend
from streamt.deployer.state_backend import (
    DeploymentStateService,
    OperationControlState,
    OperationSnapshot,
    StateAddress,
)
from tests.postgres.conftest import PostgresCase, WriterIdentity

pytestmark = [pytest.mark.integration, pytest.mark.postgres]

_PROJECT = "postgres-command-failures"
_ENVIRONMENT = "default"


def _address() -> StateAddress:
    return StateAddress(
        namespace="platform",
        project=_PROJECT,
        environment=_ENVIRONMENT,
    )


def _write_project(path: Path, case: PostgresCase) -> None:
    (path / "stream_project.yml").write_text(
        yaml.safe_dump(
            {
                "apiVersion": "streamt.dev/v1alpha1",
                "project": {"name": _PROJECT},
                "runtime": {"kafka": {"bootstrap_servers": "unused.invalid:9092"}},
                "deployment_state": {
                    "backend": "postgres",
                    "namespace": "platform",
                    "postgres": {
                        "dsn_env": "STREAMT_UNUSED_OWNER_DSN",
                        "schema": case.schema,
                        "writer_role_env": "STREAMT_UNUSED_WRITER_ROLE",
                    },
                },
            }
        ),
        encoding="utf-8",
    )


def _manifest() -> Manifest:
    topic = TopicArtifact(
        name="payments.clean.v1",
        partitions=3,
        replication_factor=1,
        ownership=ArtifactOwnership(
            project=_PROJECT,
            owner_type="model",
            owner_name="payments_clean",
            mode="managed",
        ),
    )
    return Manifest(
        version="1.0",
        project_name=_PROJECT,
        artifacts={"topics": [topic.to_dict()]},
    )


def _kafka() -> MagicMock:
    deployer = MagicMock()

    def plan_topic(artifact: TopicArtifact) -> TopicChange:
        return TopicChange(
            topic=artifact.name,
            action="create",
            current=TopicState(name=artifact.name, exists=False),
            desired=artifact,
        )

    deployer.plan_topic.side_effect = plan_topic
    deployer.apply_topic.return_value = "created"
    deployer.get_consumer_groups.return_value = []
    return deployer


def _provision_v2(case: PostgresCase, writer: WriterIdentity) -> None:
    initialized = PostgresStateInitializer(
        dsn=case.owner_dsn,
        schema=case.schema,
        lock_timeout_seconds=10,
    ).initialize(_address())
    PrivatePostgresStateV2Migrator(
        dsn=case.owner_dsn,
        schema=case.schema,
        lock_timeout_seconds=10,
        writer_role=writer.role,
    ).migrate(
        confirmed_store_id=initialized.store_id,
        confirmed_writer_role=writer.role,
    )


def _service(
    case: PostgresCase,
    writer: WriterIdentity,
    *,
    timeout: int = 5,
) -> DeploymentStateService:
    return DeploymentStateService(
        backend=PrivatePostgresStateReadBackend(
            dsn=writer.dsn,
            schema=case.schema,
            lock_timeout_seconds=timeout,
        ),
        address=_address(),
    )


def _snapshot(case: PostgresCase, writer: WriterIdentity) -> OperationSnapshot:
    return PrivatePostgresStateReadBackend(
        dsn=writer.dsn,
        schema=case.schema,
        lock_timeout_seconds=5,
    ).read_snapshot(_address())


def _payload(result: Result) -> dict[str, object]:
    value = json.loads(result.stdout)
    assert isinstance(value, dict)
    return value


def _invoke_apply(
    tmp_path: Path,
    *,
    state_service: DeploymentStateService,
    kafka: MagicMock | None = None,
) -> tuple[Result, MagicMock]:
    runtime = kafka or _kafka()
    with (
        patch("streamt.compiler.Compiler.compile", return_value=_manifest()),
        patch(
            "streamt.cli.commands.apply.make_deployment_state_service",
            return_value=state_service,
        ),
        patch(
            "streamt.cli.commands.apply.make_kafka_deployer",
            return_value=runtime,
        ) as runtime_factory,
    ):
        result = CliRunner().invoke(
            main,
            ["-o", "json", "apply", "-p", str(tmp_path)],
        )
    return result, runtime_factory


def _lock_key(case: PostgresCase) -> int:
    address = _address()
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


def _operation_history(case: PostgresCase) -> list[tuple[object, ...]]:
    address = _address()
    with case.psycopg.connect(case.owner_dsn) as connection:
        return list(
            connection.execute(
                case.sql.SQL(
                    "SELECT operation_id::text, event_index, event_kind "
                    "FROM {}.{} WHERE namespace = %s AND project = %s "
                    "AND environment = %s ORDER BY operation_id, event_index"
                ).format(
                    case.sql.Identifier(case.schema),
                    case.sql.Identifier("operation_history"),
                ),
                (address.namespace, address.project, address.environment),
            ).fetchall()
        )


def _assert_secret_neutral(
    result: Result,
    *,
    case: PostgresCase,
    writer: WriterIdentity,
    injected_secret: str | None = None,
) -> None:
    rendered = result.output
    assert writer.role not in rendered
    assert case.schema not in rendered
    assert "writer-ci-" not in rendered
    assert "unused.invalid" not in rendered
    assert "commit-boundary-secret" not in rendered
    assert "verification-boundary-secret" not in rendered
    assert "release-boundary-secret" not in rendered
    if injected_secret is not None:
        assert injected_secret not in rendered


class _FaultCursor:
    """Delegate all SQL except the final session unlock."""

    def __init__(self, cursor: object, driver: _FaultDriver) -> None:
        self._cursor = cursor
        self._driver = driver

    def execute(
        self,
        statement: object,
        params: object = None,
    ) -> _FaultCursor:
        rendered = str(statement)
        if (
            "operation_history" in rendered
            and isinstance(params, tuple)
            and "succeeded" in params
        ):
            self._driver.terminal_history_insert_attempts += 1
        if self._driver.mode == "release_failure" and "pg_advisory_unlock" in rendered:
            raise RuntimeError("password=release-boundary-secret")
        self._cursor.execute(statement, params)
        return self

    def fetchall(self) -> object:
        return self._cursor.fetchall()

    def close(self) -> None:
        self._cursor.close()


class _FaultConnection:
    """Inject only the terminal commit acknowledgement or unlock failure."""

    def __init__(self, connection: object, driver: _FaultDriver) -> None:
        self._connection = connection
        self._driver = driver

    def cursor(self) -> _FaultCursor:
        return _FaultCursor(self._connection.cursor(), self._driver)

    def commit(self) -> None:
        self._driver.main_commit_count += 1
        self._connection.commit()
        if self._driver.main_commit_count == 4 and self._driver.mode.startswith(
            "ack_loss"
        ):
            if self._driver.mode == "ack_loss_verification_unavailable":
                self._driver.fail_next_connect = True
            raise RuntimeError("password=commit-boundary-secret")

    def rollback(self) -> None:
        self._connection.rollback()

    def close(self) -> None:
        self._connection.close()


_FaultMode = Literal[
    "clean",
    "ack_loss_verified",
    "ack_loss_verification_unavailable",
    "release_failure",
]


class _FaultDriver:
    """Wrap the one authoritative operation connection; fresh reads stay real."""

    def __init__(self, driver: object, *, mode: _FaultMode) -> None:
        self._driver = driver
        self.mode = mode
        self.connections = 0
        self.main_commit_count = 0
        self.terminal_history_insert_attempts = 0
        self.fail_next_connect = False

    def connect(self, conninfo: str, **kwargs: object) -> object:
        if self.fail_next_connect:
            self.fail_next_connect = False
            raise RuntimeError("password=verification-boundary-secret")
        connection = self._driver.connect(conninfo, **kwargs)
        self.connections += 1
        if self.connections == 1:
            return _FaultConnection(connection, self)
        return connection


def test_held_address_lock_is_e422_before_runtime_construction(
    postgres_case: PostgresCase,
    postgres_writer: WriterIdentity,
    tmp_path: Path,
) -> None:
    _write_project(tmp_path, postgres_case)
    _provision_v2(postgres_case, postgres_writer)
    runtime_factory = MagicMock()
    lock_key = _lock_key(postgres_case)

    with postgres_case.psycopg.connect(
        postgres_case.owner_dsn,
        autocommit=True,
    ) as holder:
        acquired = holder.execute(
            "SELECT pg_catalog.pg_advisory_lock(%s)",
            (lock_key,),
        ).fetchone()
        assert acquired is not None
        with (
            patch("streamt.compiler.Compiler.compile", return_value=_manifest()),
            patch(
                "streamt.cli.commands.apply.make_deployment_state_service",
                return_value=_service(postgres_case, postgres_writer, timeout=0),
            ),
            patch(
                "streamt.cli.commands.apply.make_kafka_deployer",
                runtime_factory,
            ),
        ):
            result = CliRunner().invoke(
                main,
                ["-o", "json", "apply", "-p", str(tmp_path)],
            )

    assert result.exit_code == 1, result.output
    assert _payload(result)["errors"][0]["code"] == "E422_STATE_LOCK_TIMEOUT"
    runtime_factory.assert_not_called()
    assert _operation_history(postgres_case) == []
    _assert_secret_neutral(result, case=postgres_case, writer=postgres_writer)


def test_runtime_failure_persists_recovery_and_blocks_successor_before_runtime(
    postgres_case: PostgresCase,
    postgres_writer: WriterIdentity,
    tmp_path: Path,
) -> None:
    _write_project(tmp_path, postgres_case)
    _provision_v2(postgres_case, postgres_writer)
    kafka = _kafka()
    kafka.apply_topic.side_effect = RuntimeError(
        "password=runtime-boundary-secret"
    )

    failed, _factory = _invoke_apply(
        tmp_path,
        state_service=_service(postgres_case, postgres_writer),
        kafka=kafka,
    )

    assert failed.exit_code == 1, failed.output
    assert _payload(failed)["errors"][0]["code"] == "E407_DEPLOY_ERROR"
    snapshot = _snapshot(postgres_case, postgres_writer)
    control = snapshot.control.control
    assert control.status == "recovery_required"
    assert control.intent is not None
    assert [(item.status, item.succeeded) for item in control.progress] == [
        ("started", None),
        ("completed", False),
    ]
    assert control.recovery is not None
    assert control.recovery.operation_id == control.intent.operation_id
    assert control.recovery.failure_code == "runtime_action_failed"
    assert snapshot.state.revision.is_absent
    _assert_secret_neutral(
        failed,
        case=postgres_case,
        writer=postgres_writer,
        injected_secret="runtime-boundary-secret",
    )

    successor_runtime_factory = MagicMock()
    with (
        patch("streamt.compiler.Compiler.compile", return_value=_manifest()),
        patch(
            "streamt.cli.commands.apply.make_deployment_state_service",
            return_value=_service(postgres_case, postgres_writer),
        ),
        patch(
            "streamt.cli.commands.apply.make_kafka_deployer",
            successor_runtime_factory,
        ),
    ):
        successor = CliRunner().invoke(
            main,
            ["-o", "json", "apply", "-p", str(tmp_path)],
        )

    assert successor.exit_code == 1, successor.output
    assert _payload(successor)["errors"][0]["code"] == (
        "E419_STATE_RECOVERY_REQUIRED"
    )
    successor_runtime_factory.assert_not_called()


def test_writer_session_termination_during_runtime_returns_e423_with_evidence(
    postgres_case: PostgresCase,
    postgres_writer: WriterIdentity,
    tmp_path: Path,
) -> None:
    _write_project(tmp_path, postgres_case)
    _provision_v2(postgres_case, postgres_writer)
    kafka = _kafka()
    lock_key = _lock_key(postgres_case)

    def terminate_writer(_artifact: TopicArtifact) -> str:
        with postgres_case.psycopg.connect(
            postgres_case.admin_dsn,
            autocommit=True,
        ) as connection:
            row = connection.execute(
                "SELECT a.pid FROM pg_catalog.pg_stat_activity AS a "
                "JOIN pg_catalog.pg_locks AS l ON l.pid = a.pid "
                "WHERE a.usename = %s AND l.locktype = 'advisory' AND l.granted "
                "AND l.classid = (((%s::bigint >> 32) & 4294967295)::oid) "
                "AND l.objid = ((%s::bigint & 4294967295)::oid) "
                "AND l.objsubid = 1",
                (postgres_writer.role, lock_key, lock_key),
            ).fetchone()
            assert row is not None
            assert type(row[0]) is int
            assert connection.execute(
                "SELECT pg_catalog.pg_terminate_backend(%s)",
                (row[0],),
            ).fetchone() == (True,)
        return "created"

    kafka.apply_topic.side_effect = terminate_writer
    result, _factory = _invoke_apply(
        tmp_path,
        state_service=_service(postgres_case, postgres_writer),
        kafka=kafka,
    )

    assert result.exit_code == 1, result.output
    payload = _payload(result)
    error = payload["errors"][0]
    assert error["code"] == "E423_STATE_LOCK_LOST"
    assert isinstance(error["operation_id"], str)
    snapshot = _snapshot(postgres_case, postgres_writer)
    control = snapshot.control.control
    assert control.status == "in_progress"
    assert control.intent is not None
    assert control.intent.operation_id == error["operation_id"]
    assert [(item.status, item.succeeded) for item in control.progress] == [
        ("started", None)
    ]
    assert snapshot.state.revision.is_absent
    assert [row[2] for row in _operation_history(postgres_case)] == [
        "intent",
        "progress_started",
    ]
    _assert_secret_neutral(result, case=postgres_case, writer=postgres_writer)

    successor_runtime_factory = MagicMock()
    with (
        patch("streamt.compiler.Compiler.compile", return_value=_manifest()),
        patch(
            "streamt.cli.commands.apply.make_deployment_state_service",
            return_value=_service(postgres_case, postgres_writer),
        ),
        patch(
            "streamt.cli.commands.apply.make_kafka_deployer",
            successor_runtime_factory,
        ),
    ):
        successor = CliRunner().invoke(
            main,
            ["-o", "json", "apply", "-p", str(tmp_path)],
        )

    assert successor.exit_code == 1, successor.output
    assert _payload(successor)["errors"][0]["code"] == (
        "E419_STATE_RECOVERY_REQUIRED"
    )
    successor_runtime_factory.assert_not_called()


@pytest.mark.parametrize(
    ("mode", "expected_exit", "expected_code"),
    [
        ("clean", 0, None),
        (
            "ack_loss_verified",
            1,
            "E426_STATE_RELEASE_FAILED_AFTER_COMMIT",
        ),
        (
            "ack_loss_verification_unavailable",
            1,
            "E425_STATE_UNKNOWN_OUTCOME",
        ),
        (
            "release_failure",
            1,
            "E426_STATE_RELEASE_FAILED_AFTER_COMMIT",
        ),
    ],
)
def test_final_commit_and_release_failures_cross_cli_without_replay(
    mode: _FaultMode,
    expected_exit: int,
    expected_code: str | None,
    postgres_case: PostgresCase,
    postgres_writer: WriterIdentity,
    tmp_path: Path,
) -> None:
    _write_project(tmp_path, postgres_case)
    _provision_v2(postgres_case, postgres_writer)
    driver = _FaultDriver(postgres_case.psycopg, mode=mode)
    bundle = postgres_state_module._PsycopgBundle(
        driver=driver,
        sql=postgres_case.sql,
    )

    with patch.object(
        postgres_backend_module,
        "_load_psycopg",
        return_value=bundle,
    ):
        result, _factory = _invoke_apply(
            tmp_path,
            state_service=_service(postgres_case, postgres_writer),
        )

    assert result.exit_code == expected_exit, result.output
    payload = _payload(result)
    if expected_code is None:
        assert payload["status"] == "ok"
        assert payload["errors"] == []
        assert payload["data"]["committed"] is True
    else:
        assert payload["status"] == "error"
        assert payload["errors"][0]["code"] == expected_code
        assert isinstance(payload["errors"][0]["operation_id"], str)
        assert "Apply complete" not in result.output
        if expected_code == "E426_STATE_RELEASE_FAILED_AFTER_COMMIT":
            assert payload["data"]["committed"] is True
            assert payload["data"]["state_serial"] == 1
            assert payload["data"]["created"] == ["topic:payments.clean.v1"]
        else:
            assert payload["data"] == {}

    snapshot = _snapshot(postgres_case, postgres_writer)
    assert snapshot.state.state.serial == 1
    assert snapshot.control.control == OperationControlState.clear(_address())
    history = _operation_history(postgres_case)
    assert [row[2] for row in history] == [
        "intent",
        "progress_started",
        "progress_completed",
        "succeeded",
    ]
    assert len({row[0] for row in history}) == 1
    assert driver.main_commit_count == 4
    assert driver.terminal_history_insert_attempts == 1
    _assert_secret_neutral(
        result,
        case=postgres_case,
        writer=postgres_writer,
        injected_secret=(
            "verification-boundary-secret"
            if mode == "ack_loss_verification_unavailable"
            else "commit-boundary-secret"
            if mode == "ack_loss_verified"
            else "release-boundary-secret"
            if mode == "release_failure"
            else None
        ),
    )
