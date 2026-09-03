"""Real PostgreSQL CLI lifecycle gate for reviewed Connector removals."""

from __future__ import annotations

import json
import os
import uuid
from contextlib import ExitStack
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
    managed_connector_absence_fingerprint,
)
from streamt.deployer.kafka import KafkaDeployer
from streamt.deployer.plan_file import PLAN_FILE_VERSION, ReviewedPlanFile
from streamt.deployer.postgres_state import (
    PostgresStateInitializer,
    PrivatePostgresStateV2Migrator,
)
from streamt.deployer.postgres_state_backend import PrivatePostgresStateReadBackend
from streamt.deployer.state import (
    LocalState,
    ManagedResourceRecord,
    artifact_checksum,
    local_state_path,
    resource_id,
)
from streamt.deployer.state_backend import (
    DeploymentStateService,
    OperationControlState,
    OperationIntent,
    StateAddress,
    operation_timestamp,
    state_checksum,
)
from tests.postgres.conftest import PostgresCase, WriterIdentity

pytestmark = [pytest.mark.integration, pytest.mark.postgres]

_PROJECT = "connector-removal-command"
_ENVIRONMENT = "default"
_NAMESPACE = "platform"
_OWNER = "archive_orders"
_CONNECTOR = "archive-orders-sink"
_CONNECT_ALIAS = "primary"
_CONNECT_URL = "https://connect.example.test:8443/api/"
_CONNECT_PASSWORD = "connector-postgres-runtime-secret"
_LIVE_CONFIG_SECRET = "connector-postgres-live-secret"
_OWNER_DSN_ENV = "STREAMT_CONNECTOR_REMOVAL_OWNER_DSN"
_WRITER_DSN_ENV = "STREAMT_CONNECTOR_REMOVAL_WRITER_DSN"


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
        topics=["orders.events.v1", "orders.events.v2"],
        cluster=_CONNECT_ALIAS,
        config={
            "password": _LIVE_CONFIG_SECRET,
            "tasks.max": 2,
        },
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


def _write_project(path: Path, case: PostgresCase) -> None:
    project = {
        "apiVersion": "streamt.dev/v1alpha1",
        "project": {"name": _PROJECT},
        "runtime": {
            "kafka": {"bootstrap_servers": "broker.invalid:9092"},
            "connect": {
                "default": _CONNECT_ALIAS,
                "clusters": {
                    _CONNECT_ALIAS: {
                        "rest_url": _CONNECT_URL,
                        "username": "connector-postgres-runtime-user",
                        "password": _CONNECT_PASSWORD,
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
            "lock_timeout_seconds": 10,
            "postgres": {
                "dsn_env": _OWNER_DSN_ENV,
                "writer_dsn_env": _WRITER_DSN_ENV,
                "schema": case.schema,
            },
        },
    }
    (path / "stream_project.yml").write_text(
        yaml.safe_dump(project, sort_keys=False),
        encoding="utf-8",
    )


def _initialize_v2(case: PostgresCase, writer: WriterIdentity) -> str:
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
    return initialized.store_id


def _verification_service(
    case: PostgresCase,
    writer: WriterIdentity,
) -> DeploymentStateService:
    return DeploymentStateService(
        backend=PrivatePostgresStateReadBackend(
            dsn=writer.dsn,
            schema=case.schema,
            lock_timeout_seconds=10,
        ),
        address=_address(),
    )


def _operation_rows(
    case: PostgresCase,
) -> tuple[tuple[object, ...], list[tuple[object, ...]], int]:
    address = _address()
    with case.psycopg.connect(case.owner_dsn) as connection:
        control = connection.execute(
            case.sql.SQL(
                "SELECT revision, status, control_json FROM {}.{} "
                "WHERE namespace = %s AND project = %s AND environment = %s"
            ).format(
                case.sql.Identifier(case.schema),
                case.sql.Identifier("operation_control"),
            ),
            (address.namespace, address.project, address.environment),
        ).fetchone()
        events = list(
            connection.execute(
                case.sql.SQL(
                    "SELECT operation_id::text, event_index, event_kind, control_json "
                    "FROM {}.{} WHERE namespace = %s AND project = %s "
                    "AND environment = %s ORDER BY recorded_at, event_index"
                ).format(
                    case.sql.Identifier(case.schema),
                    case.sql.Identifier("operation_history"),
                ),
                (address.namespace, address.project, address.environment),
            ).fetchall()
        )
        current_count = connection.execute(
            case.sql.SQL(
                "SELECT count(*) FROM {}.{} WHERE namespace = %s "
                "AND project = %s AND environment = %s"
            ).format(
                case.sql.Identifier(case.schema),
                case.sql.Identifier("current_state"),
            ),
            (address.namespace, address.project, address.environment),
        ).fetchone()[0]
    assert control is not None
    return control, events, current_count


def _durable_rows(case: PostgresCase) -> dict[str, tuple[str, ...]]:
    tables = (
        "current_state",
        "operation_control",
        "operation_history",
        "state_history",
    )
    with case.psycopg.connect(case.owner_dsn) as connection:
        return {
            table: tuple(
                row[0]
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


def _payload(result: Result) -> dict[str, object]:
    value = json.loads(result.stdout)
    assert isinstance(value, dict)
    return value


def _data(result: Result) -> dict[str, object]:
    value = _payload(result).get("data")
    assert isinstance(value, dict)
    return value


def test_postgres_reviewed_connector_removal_is_exact_and_durable(
    tmp_path: Path,
    postgres_case: PostgresCase,
    postgres_writer: WriterIdentity,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_project(tmp_path, postgres_case)
    store_id = _initialize_v2(postgres_case, postgres_writer)
    monkeypatch.delenv(_OWNER_DSN_ENV, raising=False)
    monkeypatch.setenv(_WRITER_DSN_ENV, postgres_writer.dsn)
    assert _OWNER_DSN_ENV not in os.environ

    binding = _binding()
    prior_artifact = _prior_artifact()
    prior_checksum = artifact_checksum(prior_artifact.to_dict())
    removed_resource_id = resource_id(
        _PROJECT,
        _ENVIRONMENT,
        "connector",
        _OWNER,
    )
    unrelated_resource_id = resource_id(
        _PROJECT,
        _ENVIRONMENT,
        "topic",
        "audit_log",
    )
    unrelated_record = ManagedResourceRecord(
        physical_name="audit.events.v1",
        ownership="managed",
        artifact_checksum=artifact_checksum({"name": "audit.events.v1"}),
        backend="direct-kafka",
    )
    initial_state = LocalState(
        project=_PROJECT,
        environment=_ENVIRONMENT,
        serial=1,
        resources={
            removed_resource_id: ManagedResourceRecord(
                physical_name=_CONNECTOR,
                ownership="managed",
                artifact_checksum=prior_checksum,
                backend=binding.backend_identity,
            ),
            unrelated_resource_id: unrelated_record,
        },
    )
    state_service = _verification_service(postgres_case, postgres_writer)
    with state_service.operation() as operation:
        observed = operation.observe()
        active = operation.begin_operation(
            observed,
            OperationIntent(
                operation_id=str(uuid.uuid4()),
                kind="adopt",
                started_at=operation_timestamp(),
                actor="postgres-connector-removal-test",
                prior_state_serial=observed.state.state_serial,
                prior_state_checksum=state_checksum(observed.state.state),
                reviewed_plan_checksum=None,
                actions=(),
            ),
        )
        operation.commit_operation(active, initial_state)

    provider_state = {"present": True}
    observations: list[ManagedConnectorObservation] = []
    durable_progress_controls: list[OperationControlState] = []
    connect = MagicMock(spec=ConnectDeployer)
    connect.cluster_binding = binding
    connect.require_cluster_binding.return_value = binding
    kafka = MagicMock(spec=KafkaDeployer)

    def observe_connector(name: str) -> ManagedConnectorObservation:
        assert name == _CONNECTOR
        current = (
            _present_observation()
            if provider_state["present"]
            else ManagedConnectorObservation(
                binding=binding,
                name=_CONNECTOR,
                exists=False,
            )
        )
        observations.append(current)
        return current

    connect.observe_managed_connector.side_effect = observe_connector

    def delete_connector(current: ManagedConnectorObservation) -> str:
        assert current == observations[-1]
        assert current == _present_observation()
        control_row, history_rows, current_count = _operation_rows(postgres_case)
        durable_control = OperationControlState.from_dict(
            json.loads(control_row[2]),
            expected_address=_address(),
        )
        durable_progress_controls.append(durable_control)
        assert control_row[1] == "in_progress"
        assert current_count == 1
        assert durable_control.intent is not None
        assert durable_control.progress[-1].status == "started"
        assert durable_control.progress[-1].resource_id == removed_resource_id
        operation_id = durable_control.intent.operation_id
        assert [(row[1], row[2]) for row in history_rows if row[0] == operation_id] == [
            (0, "intent"),
            (1, "progress_started"),
        ]
        provider_state["present"] = False
        return "deleted"

    connect.delete_managed_connector.side_effect = delete_connector
    reviewed_path = tmp_path / "connector-removal.plan.json"
    runner = CliRunner()

    with ExitStack() as stack:
        for command in ("plan", "apply"):
            stack.enter_context(
                patch(
                    f"streamt.cli.commands.{command}.make_kafka_deployer",
                    return_value=kafka,
                )
            )
            stack.enter_context(
                patch(
                    f"streamt.cli.commands.{command}.make_connect_deployer",
                    return_value=connect,
                )
            )

        planned = runner.invoke(
            main,
            ["-o", "json", "plan", "-p", str(tmp_path), "--out", str(reviewed_path)],
        )
        assert planned.exit_code == 0, planned.output
        assert _data(planned)["deletes"] == 1
        assert _data(planned)["connector_removal_assessments"] == []

        reviewed = ReviewedPlanFile.load(reviewed_path)
        assert reviewed.to_dict()["format_version"] == PLAN_FILE_VERSION
        assert reviewed.state is not None
        assert reviewed.state.backend == "postgres"
        assert reviewed.state.store_id == store_id
        assert reviewed.state.address == _address().uri
        assert reviewed.state.serial == initial_state.serial
        assert reviewed.state.checksum == state_checksum(initial_state)
        assert reviewed.plan["resources"] == [
            {
                "kind": "connector",
                "name": _CONNECTOR,
                "action": "delete",
                "changes": {},
            }
        ]
        assert len(reviewed.actions) == 1
        reviewed_action = reviewed.actions[0]
        assert reviewed_action.index == 0
        assert reviewed_action.resource_id == removed_resource_id
        assert reviewed_action.action == "delete"
        assert reviewed_action.gateway_evidence is None
        assert reviewed_action.connector_evidence is not None
        evidence = reviewed_action.connector_evidence
        assert evidence.backend_identity == binding.backend_identity
        assert evidence.connector_name == _CONNECTOR
        assert evidence.prior_artifact_checksum == prior_checksum
        assert evidence.current.exists is True
        assert evidence.current.fingerprint == observations[-1].fingerprint
        assert evidence.desired.exists is False
        assert evidence.desired.fingerprint == managed_connector_absence_fingerprint(
            binding.backend_identity,
            _CONNECTOR,
        )
        reviewed_wire = reviewed_path.read_text(encoding="utf-8")
        for forbidden in (
            _CONNECT_URL,
            _CONNECT_PASSWORD,
            _LIVE_CONFIG_SECRET,
            "connector-postgres-runtime-user",
            postgres_case.schema,
            postgres_case.owner_role,
            postgres_writer.role,
            postgres_case.owner_dsn,
            postgres_writer.dsn,
            _OWNER_DSN_ENV,
            _WRITER_DSN_ENV,
        ):
            assert forbidden not in reviewed_wire

        before_blocked = _durable_rows(postgres_case)
        blocked = runner.invoke(
            main,
            ["-o", "json", "apply", "-p", str(tmp_path), "--plan", str(reviewed_path)],
        )
        assert blocked.exit_code == 1
        blocked_payload = _payload(blocked)
        assert blocked_payload["status"] == "error"
        errors = blocked_payload["errors"]
        assert isinstance(errors, list)
        assert isinstance(errors[0], dict)
        assert errors[0]["code"] == "E503_ENVIRONMENT_ERROR"
        assert "Destructive ops blocked" in errors[0]["message"]
        connect.delete_managed_connector.assert_not_called()
        assert _durable_rows(postgres_case) == before_blocked

        applied = runner.invoke(
            main,
            [
                "-o",
                "json",
                "apply",
                "-p",
                str(tmp_path),
                "--plan",
                str(reviewed_path),
                "--force",
            ],
        )
        assert applied.exit_code == 0, (
            f"{applied.output}\n"
            f"provider_present={provider_state['present']!r}; "
            f"provider_calls={connect.delete_managed_connector.call_count}; "
            f"durable_snapshots={len(durable_progress_controls)}"
        )

    applied_data = _data(applied)
    assert applied_data["deleted"] == [f"connector:{_CONNECTOR}"]
    assert applied_data["committed"] is True
    assert applied_data["state_serial"] == initial_state.serial + 1
    assert applied_data["plan_checksum"] == reviewed.checksum
    connect.delete_managed_connector.assert_called_once()
    connect.delete_connector.assert_not_called()
    connect.apply_connector.assert_not_called()
    assert len(durable_progress_controls) == 1
    durable_control = durable_progress_controls[0]
    assert durable_control.intent is not None
    assert durable_control.intent.reviewed_plan_checksum == reviewed.checksum
    assert durable_control.intent.actions == reviewed.actions
    assert [
        (entry.action_index, entry.action, entry.status, entry.succeeded)
        for entry in durable_control.progress
    ] == [(0, "delete", "started", None)]

    expected_state = LocalState(
        project=_PROJECT,
        environment=_ENVIRONMENT,
        serial=initial_state.serial + 1,
        resources={unrelated_resource_id: unrelated_record},
    )
    committed = state_service.read()
    assert committed.state == expected_state
    assert state_service.read_control().control == OperationControlState.clear(_address())

    control_row, history_rows, current_count = _operation_rows(postgres_case)
    assert control_row[0:2] == (6, "clear")
    assert json.loads(control_row[2]) == OperationControlState.clear(_address()).to_dict()
    assert current_count == 1
    delete_operation_id = durable_control.intent.operation_id
    delete_history = [row for row in history_rows if row[0] == delete_operation_id]
    assert [(row[1], row[2]) for row in delete_history] == [
        (0, "intent"),
        (1, "progress_started"),
        (2, "progress_completed"),
        (3, "succeeded"),
    ]
    assert json.loads(delete_history[0][3])["intent"]["actions"] == [reviewed_action.to_dict()]
    assert json.loads(delete_history[-1][3]) == OperationControlState.clear(_address()).to_dict()
    history_wire = "\n".join(str(row[3]) for row in delete_history)
    assert _LIVE_CONFIG_SECRET not in history_wire
    assert _CONNECT_PASSWORD not in history_wire
    assert _CONNECT_URL not in history_wire

    with postgres_case.psycopg.connect(postgres_case.owner_dsn) as connection:
        state_history = list(
            connection.execute(
                postgres_case.sql.SQL(
                    "SELECT revision, state_serial, state_checksum, state_json, "
                    "operation_id::text FROM {}.{} WHERE namespace = %s "
                    "AND project = %s AND environment = %s ORDER BY revision"
                ).format(
                    postgres_case.sql.Identifier(postgres_case.schema),
                    postgres_case.sql.Identifier("state_history"),
                ),
                (_NAMESPACE, _PROJECT, _ENVIRONMENT),
            ).fetchall()
        )
    assert [(row[0], row[1]) for row in state_history] == [(1, 1), (2, 2)]
    assert state_history[-1][2] == state_checksum(expected_state)
    assert json.loads(state_history[-1][3]) == expected_state.to_dict()
    assert state_history[-1][4] == delete_operation_id
    assert not local_state_path(tmp_path, environment=_ENVIRONMENT).exists()
    assert not (tmp_path / ".streamt").exists()
    assert [observation.exists for observation in observations] == [True, True, True]
