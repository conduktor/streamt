"""Real PostgreSQL lock and session-loss gates for Connector removal."""

from __future__ import annotations

import json
import uuid
from collections.abc import Iterator
from contextlib import ExitStack, contextmanager
from pathlib import Path
from typing import cast
from unittest.mock import MagicMock, patch

import pytest
import yaml
from click.testing import CliRunner, Result

from streamt.cli import main
from streamt.compiler.manifest import ArtifactOwnership, ConnectorArtifact
from streamt.deployer.connect import (
    ConnectClusterBinding,
    ConnectDeployer,
    ConnectorConfigScalar,
    ManagedConnectorObservation,
)
from streamt.deployer.kafka import KafkaDeployer
from streamt.deployer.plan_file import ReviewedPlanFile
from streamt.deployer.postgres_state import (
    PostgresStateInitializer,
    PrivatePostgresStateV2Migrator,
)
from streamt.deployer.postgres_state_backend import PrivatePostgresStateReadBackend
from streamt.deployer.recovery_plan import RecoveryPlanFile
from streamt.deployer.state import (
    LocalState,
    ManagedResourceRecord,
    artifact_checksum,
    resource_id,
)
from streamt.deployer.state_backend import (
    DeploymentStateOperation,
    DeploymentStateService,
    OperationIntent,
    OperationSnapshot,
    StateAddress,
    operation_timestamp,
    state_checksum,
)
from tests.postgres.conftest import PostgresCase, WriterIdentity

pytestmark = [pytest.mark.integration, pytest.mark.postgres]

_PROJECT = "connector-removal-concurrency"
_ENVIRONMENT = "default"
_NAMESPACE = "platform"
_OWNER = "archive_orders"
_CONNECTOR = "archive-orders-sink"
_CONNECT_ALIAS = "primary"
_CONNECT_URL = "https://connect.concurrency.example.test/api/"
_OWNER_DSN_ENV = "STREAMT_CONNECTOR_CONCURRENCY_OWNER_DSN"
_WRITER_DSN_ENV = "STREAMT_CONNECTOR_CONCURRENCY_WRITER_DSN"


def _address() -> StateAddress:
    return StateAddress(
        namespace=_NAMESPACE,
        project=_PROJECT,
        environment=_ENVIRONMENT,
    )


def _binding() -> ConnectClusterBinding:
    return ConnectClusterBinding.from_endpoint(_CONNECT_ALIAS, _CONNECT_URL)


def _prior_artifact() -> ConnectorArtifact:
    return ConnectorArtifact(
        name=_CONNECTOR,
        connector_class="com.example.ArchiveSink",
        topics=["orders.events.v1"],
        cluster=_CONNECT_ALIAS,
        config={"tasks.max": 2},
        ownership=ArtifactOwnership(
            project=_PROJECT,
            owner_type="model",
            owner_name=_OWNER,
            mode="managed",
        ),
    )


def _present_observation() -> ManagedConnectorObservation:
    raw_config = _prior_artifact().to_dict()["config"]
    assert isinstance(raw_config, dict)
    config = cast(
        tuple[tuple[str, ConnectorConfigScalar], ...],
        tuple(sorted(raw_config.items())),
    )
    return ManagedConnectorObservation(
        binding=_binding(),
        name=_CONNECTOR,
        exists=True,
        config=config,
    )


def _absent_observation() -> ManagedConnectorObservation:
    return ManagedConnectorObservation(
        binding=_binding(),
        name=_CONNECTOR,
        exists=False,
    )


def _write_project(path: Path, case: PostgresCase, *, lock_timeout: int = 10) -> None:
    (path / "stream_project.yml").write_text(
        yaml.safe_dump(
            {
                "apiVersion": "streamt.dev/v1alpha1",
                "project": {"name": _PROJECT},
                "runtime": {
                    "kafka": {"bootstrap_servers": "broker.invalid:9092"},
                    "connect": {
                        "default": _CONNECT_ALIAS,
                        "clusters": {
                            _CONNECT_ALIAS: {
                                "rest_url": _CONNECT_URL,
                            }
                        },
                    },
                },
                "lifecycle": {
                    "connector_removals": [
                        {
                            "logical_owner": _OWNER,
                            "name": _CONNECTOR,
                            "cluster": _CONNECT_ALIAS,
                        }
                    ]
                },
                "deployment_state": {
                    "backend": "postgres",
                    "namespace": _NAMESPACE,
                    "lock_timeout_seconds": lock_timeout,
                    "postgres": {
                        "dsn_env": _OWNER_DSN_ENV,
                        "writer_dsn_env": _WRITER_DSN_ENV,
                        "schema": case.schema,
                    },
                },
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )


def _provision_v2(case: PostgresCase, writer: WriterIdentity) -> None:
    initialized = PostgresStateInitializer(
        dsn=case.owner_dsn,
        schema=case.schema,
        lock_timeout_seconds=10,
    ).initialize(_address())
    migrated = PrivatePostgresStateV2Migrator(
        dsn=case.owner_dsn,
        schema=case.schema,
        lock_timeout_seconds=10,
        writer_role=writer.role,
    ).migrate(
        confirmed_store_id=initialized.store_id,
        confirmed_writer_role=writer.role,
    )
    assert migrated.migrated is True


def _service(
    case: PostgresCase,
    writer: WriterIdentity,
    *,
    timeout: int = 10,
) -> DeploymentStateService:
    return DeploymentStateService(
        backend=PrivatePostgresStateReadBackend(
            dsn=writer.dsn,
            schema=case.schema,
            lock_timeout_seconds=timeout,
            require_v2_writer=True,
        ),
        address=_address(),
    )


def _snapshot(service: DeploymentStateService) -> OperationSnapshot:
    backend = cast(PrivatePostgresStateReadBackend, service.backend)
    return backend.read_snapshot(_address())


def _initial_state() -> tuple[LocalState, str, ManagedResourceRecord]:
    connector_id = resource_id(_PROJECT, _ENVIRONMENT, "connector", _OWNER)
    unrelated_id = resource_id(_PROJECT, _ENVIRONMENT, "topic", "audit_log")
    unrelated = ManagedResourceRecord(
        physical_name="audit.events.v1",
        ownership="managed",
        artifact_checksum=artifact_checksum({"name": "audit.events.v1"}),
        backend="direct-kafka",
    )
    return (
        LocalState(
            project=_PROJECT,
            environment=_ENVIRONMENT,
            serial=1,
            resources={
                connector_id: ManagedResourceRecord(
                    physical_name=_CONNECTOR,
                    ownership="managed",
                    artifact_checksum=artifact_checksum(_prior_artifact().to_dict()),
                    backend=_binding().backend_identity,
                ),
                unrelated_id: unrelated,
            },
        ),
        unrelated_id,
        unrelated,
    )


def _seed_state(service: DeploymentStateService, state: LocalState) -> None:
    with service.operation() as operation:
        observed = operation.observe()
        active = operation.begin_operation(
            observed,
            OperationIntent(
                operation_id=str(uuid.uuid4()),
                kind="adopt",
                started_at=operation_timestamp(),
                actor="connector-concurrency-test",
                prior_state_serial=observed.state.state_serial,
                prior_state_checksum=state_checksum(observed.state.state),
                reviewed_plan_checksum=None,
                actions=(),
            ),
        )
        operation.commit_operation(active, state)


def _provider(present: dict[str, bool]) -> MagicMock:
    connect = MagicMock(spec=ConnectDeployer)
    connect.cluster_binding = _binding()
    connect.require_cluster_binding.return_value = _binding()

    def observe(name: str) -> ManagedConnectorObservation:
        assert name == _CONNECTOR
        return _present_observation() if present["value"] else _absent_observation()

    connect.observe_managed_connector.side_effect = observe
    return connect


def _payload(result: Result) -> dict[str, object]:
    payload = json.loads(result.stdout)
    assert isinstance(payload, dict)
    return payload


def _prepare_reviewed_plan(
    path: Path,
    *,
    connect: MagicMock,
    kafka: MagicMock,
) -> tuple[Path, ReviewedPlanFile]:
    plan_path = path / "connector-removal.plan.json"
    with (
        patch("streamt.cli.commands.plan.make_kafka_deployer", return_value=kafka),
        patch("streamt.cli.commands.plan.make_connect_deployer", return_value=connect),
    ):
        result = CliRunner().invoke(
            main,
            ["-o", "json", "plan", "-p", str(path), "--out", str(plan_path)],
        )
    assert result.exit_code == 0, result.output
    reviewed = ReviewedPlanFile.load(plan_path)
    assert len(reviewed.actions) == 1
    assert reviewed.actions[0].connector_evidence is not None
    connect.reset_mock()
    kafka.reset_mock()
    return plan_path, reviewed


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


def _terminate_address_writer(case: PostgresCase, writer: WriterIdentity) -> None:
    lock_key = _lock_key(case)
    with case.psycopg.connect(case.admin_dsn, autocommit=True) as connection:
        row = connection.execute(
            "SELECT a.pid FROM pg_catalog.pg_stat_activity AS a "
            "JOIN pg_catalog.pg_locks AS l ON l.pid = a.pid "
            "WHERE a.usename = %s AND l.locktype = 'advisory' AND l.granted "
            "AND l.classid = (((%s::bigint >> 32) & 4294967295)::oid) "
            "AND l.objid = ((%s::bigint & 4294967295)::oid) "
            "AND l.objsubid = 1",
            (writer.role, lock_key, lock_key),
        ).fetchone()
        assert row is not None
        assert type(row[0]) is int
        assert connection.execute(
            "SELECT pg_catalog.pg_terminate_backend(%s)",
            (row[0],),
        ).fetchone() == (True,)


class _TerminateAfterBeginOperation:
    """Lose the real writer session immediately after durable intent."""

    def __init__(
        self,
        operation: DeploymentStateOperation,
        case: PostgresCase,
        writer: WriterIdentity,
    ) -> None:
        self._operation = operation
        self._case = case
        self._writer = writer

    def __getattr__(self, name: str) -> object:
        return getattr(self._operation, name)

    def begin_operation(
        self,
        observation: OperationSnapshot,
        intent: OperationIntent,
    ) -> OperationSnapshot:
        active = self._operation.begin_operation(observation, intent)
        _terminate_address_writer(self._case, self._writer)
        return active


class _TerminateAfterBeginService:
    def __init__(
        self,
        service: DeploymentStateService,
        case: PostgresCase,
        writer: WriterIdentity,
    ) -> None:
        self._service = service
        self._case = case
        self._writer = writer

    @contextmanager
    def operation(self) -> Iterator[_TerminateAfterBeginOperation]:
        with self._service.operation() as operation:
            yield _TerminateAfterBeginOperation(operation, self._case, self._writer)


def _operation_event_kinds(case: PostgresCase, operation_id: str) -> list[str]:
    address = _address()
    with case.psycopg.connect(case.owner_dsn) as connection:
        return [
            row[0]
            for row in connection.execute(
                case.sql.SQL(
                    "SELECT event_kind FROM {}.{} WHERE namespace = %s "
                    "AND project = %s AND environment = %s AND operation_id = %s "
                    "ORDER BY event_index"
                ).format(
                    case.sql.Identifier(case.schema),
                    case.sql.Identifier("operation_history"),
                ),
                (
                    address.namespace,
                    address.project,
                    address.environment,
                    operation_id,
                ),
            ).fetchall()
        ]


def test_same_address_contender_cannot_reach_connect_while_lock_is_held(
    tmp_path: Path,
    postgres_case: PostgresCase,
    postgres_writer: WriterIdentity,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_project(tmp_path, postgres_case)
    _provision_v2(postgres_case, postgres_writer)
    monkeypatch.setenv(_WRITER_DSN_ENV, postgres_writer.dsn)
    service = _service(postgres_case, postgres_writer)
    initial_state, _unrelated_id, _unrelated = _initial_state()
    _seed_state(service, initial_state)
    present = {"value": True}
    connect = _provider(present)
    kafka = MagicMock(spec=KafkaDeployer)
    plan_path, _reviewed = _prepare_reviewed_plan(
        tmp_path,
        connect=connect,
        kafka=kafka,
    )
    before_state = service.read().state
    before_control = service.read_control().control
    connect_factory = MagicMock()
    lock_key = _lock_key(postgres_case)

    with postgres_case.psycopg.connect(
        postgres_case.owner_dsn,
        autocommit=True,
    ) as holder:
        assert (
            holder.execute(
                "SELECT pg_catalog.pg_advisory_lock(%s)",
                (lock_key,),
            ).fetchone()
            is not None
        )
        with (
            patch(
                "streamt.cli.commands.apply.make_deployment_state_service",
                return_value=_service(postgres_case, postgres_writer, timeout=0),
            ),
            patch(
                "streamt.cli.commands.apply.make_connect_deployer",
                connect_factory,
            ),
        ):
            result = CliRunner().invoke(
                main,
                [
                    "-o",
                    "json",
                    "apply",
                    "-p",
                    str(tmp_path),
                    "--plan",
                    str(plan_path),
                    "--force",
                ],
            )

    assert result.exit_code == 1, result.output
    errors = _payload(result)["errors"]
    assert isinstance(errors, list)
    assert isinstance(errors[0], dict)
    assert errors[0]["code"] == "E422_STATE_LOCK_TIMEOUT"
    connect_factory.assert_not_called()
    connect.observe_managed_connector.assert_not_called()
    connect.delete_managed_connector.assert_not_called()
    assert service.read().state == before_state == initial_state
    assert service.read_control().control == before_control


def test_writer_session_loss_after_intent_stops_before_connector_provider(
    tmp_path: Path,
    postgres_case: PostgresCase,
    postgres_writer: WriterIdentity,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_project(tmp_path, postgres_case)
    _provision_v2(postgres_case, postgres_writer)
    monkeypatch.setenv(_WRITER_DSN_ENV, postgres_writer.dsn)
    service = _service(postgres_case, postgres_writer)
    initial_state, _unrelated_id, _unrelated = _initial_state()
    _seed_state(service, initial_state)
    present = {"value": True}
    connect = _provider(present)
    kafka = MagicMock(spec=KafkaDeployer)
    plan_path, reviewed = _prepare_reviewed_plan(
        tmp_path,
        connect=connect,
        kafka=kafka,
    )
    terminating_service = _TerminateAfterBeginService(
        service,
        postgres_case,
        postgres_writer,
    )

    with (
        patch(
            "streamt.cli.commands.apply.make_deployment_state_service",
            return_value=terminating_service,
        ),
        patch("streamt.cli.commands.apply.make_kafka_deployer", return_value=kafka),
        patch("streamt.cli.commands.apply.make_connect_deployer", return_value=connect),
    ):
        result = CliRunner().invoke(
            main,
            [
                "-o",
                "json",
                "apply",
                "-p",
                str(tmp_path),
                "--plan",
                str(plan_path),
                "--force",
            ],
        )

    assert result.exit_code == 1, result.output
    errors = _payload(result)["errors"]
    assert isinstance(errors, list)
    assert isinstance(errors[0], dict)
    assert errors[0]["code"] == "E423_STATE_LOCK_LOST"
    operation_id = errors[0].get("operation_id")
    assert isinstance(operation_id, str)
    connect.delete_managed_connector.assert_not_called()
    assert present["value"] is True

    blocked = _snapshot(service)
    assert blocked.state.state == initial_state
    assert blocked.control.control.status == "in_progress"
    assert blocked.control.control.progress == ()
    assert blocked.control.control.intent is not None
    assert blocked.control.control.intent.operation_id == operation_id
    assert blocked.control.control.intent.actions == reviewed.actions
    assert _operation_event_kinds(postgres_case, operation_id) == ["intent"]

    successor_connect = MagicMock()
    with (
        patch(
            "streamt.cli.commands.apply.make_deployment_state_service",
            return_value=service,
        ),
        patch(
            "streamt.cli.commands.apply.make_connect_deployer",
            successor_connect,
        ),
    ):
        successor = CliRunner().invoke(
            main,
            [
                "-o",
                "json",
                "apply",
                "-p",
                str(tmp_path),
                "--plan",
                str(plan_path),
                "--force",
            ],
        )
    successor_errors = _payload(successor)["errors"]
    assert isinstance(successor_errors, list)
    assert isinstance(successor_errors[0], dict)
    assert successor_errors[0]["code"] == "E419_STATE_RECOVERY_REQUIRED"
    successor_connect.assert_not_called()


def test_session_loss_after_connector_delete_recovers_exact_candidate_state(
    tmp_path: Path,
    postgres_case: PostgresCase,
    postgres_writer: WriterIdentity,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_project(tmp_path, postgres_case)
    _provision_v2(postgres_case, postgres_writer)
    monkeypatch.setenv(_WRITER_DSN_ENV, postgres_writer.dsn)
    service = _service(postgres_case, postgres_writer)
    initial_state, unrelated_id, unrelated = _initial_state()
    _seed_state(service, initial_state)
    present = {"value": True}
    connect = _provider(present)
    kafka = MagicMock(spec=KafkaDeployer)
    plan_path, reviewed = _prepare_reviewed_plan(
        tmp_path,
        connect=connect,
        kafka=kafka,
    )

    def delete_then_lose_session(current: ManagedConnectorObservation) -> str:
        assert current == _present_observation()
        snapshot = _snapshot(service)
        assert snapshot.state.state == initial_state
        assert snapshot.control.control.status == "in_progress"
        assert [
            (progress.status, progress.succeeded) for progress in snapshot.control.control.progress
        ] == [("started", None)]
        present["value"] = False
        _terminate_address_writer(postgres_case, postgres_writer)
        return "deleted"

    connect.delete_managed_connector.side_effect = delete_then_lose_session
    with (
        patch(
            "streamt.cli.commands.apply.make_deployment_state_service",
            return_value=service,
        ),
        patch("streamt.cli.commands.apply.make_kafka_deployer", return_value=kafka),
        patch("streamt.cli.commands.apply.make_connect_deployer", return_value=connect),
    ):
        result = CliRunner().invoke(
            main,
            [
                "-o",
                "json",
                "apply",
                "-p",
                str(tmp_path),
                "--plan",
                str(plan_path),
                "--force",
            ],
        )

    assert result.exit_code == 1, result.output
    errors = _payload(result)["errors"]
    assert isinstance(errors, list)
    assert isinstance(errors[0], dict)
    assert errors[0]["code"] == "E423_STATE_LOCK_LOST"
    operation_id = errors[0].get("operation_id")
    assert isinstance(operation_id, str)
    connect.delete_managed_connector.assert_called_once()
    assert present["value"] is False

    blocked = _snapshot(service)
    assert blocked.state.state == initial_state
    assert blocked.control.control.status == "in_progress"
    assert blocked.control.control.intent is not None
    assert blocked.control.control.intent.operation_id == operation_id
    assert blocked.control.control.intent.actions == reviewed.actions
    assert [
        (progress.status, progress.succeeded) for progress in blocked.control.control.progress
    ] == [("started", None)]
    assert _operation_event_kinds(postgres_case, operation_id) == [
        "intent",
        "progress_started",
    ]

    recovery_path = tmp_path / "connector-removal.recovery.json"
    runner = CliRunner()
    with ExitStack() as stack:
        stack.enter_context(patch("streamt.cli.helpers.make_kafka_deployer", return_value=kafka))
        stack.enter_context(
            patch("streamt.cli.helpers.make_connect_deployer", return_value=connect)
        )
        planned = runner.invoke(
            main,
            [
                "-o",
                "json",
                "state",
                "recovery-plan",
                "-p",
                str(tmp_path),
                "--resolution",
                "observed",
                "--out",
                str(recovery_path),
            ],
        )
        assert planned.exit_code == 0, planned.output
        recovery = RecoveryPlanFile.load(recovery_path)
        assert recovery.blocked_operation_id == operation_id
        assert len(recovery.targets) == 1
        assert recovery.targets[0].presence == "absent"
        assert recovery.targets[0].accepted_as == "candidate"

        recovered = runner.invoke(
            main,
            [
                "-o",
                "json",
                "state",
                "recover",
                "-p",
                str(tmp_path),
                "--plan",
                str(recovery_path),
                "--confirm-operation-id",
                operation_id,
                "--confirm-resolution",
                "observed",
                "--confirm-evidence-checksum",
                recovery.evidence_checksum,
            ],
        )

    assert recovered.exit_code == 0, recovered.output
    expected = LocalState(
        project=_PROJECT,
        environment=_ENVIRONMENT,
        serial=initial_state.serial + 1,
        resources={unrelated_id: unrelated},
    )
    final = _snapshot(service)
    assert final.state.state == expected
    assert final.control.control.status == "clear"
    assert connect.delete_managed_connector.call_count == 1
