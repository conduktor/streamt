"""Real PostgreSQL recovery gate for an uncertain Connector deletion."""

from __future__ import annotations

import json
import os
import uuid
from contextlib import ExitStack
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import yaml
from click.testing import CliRunner

from streamt.cli import main
from streamt.deployer.connect import (
    ConnectDeployer,
    ManagedConnectorObservation,
    managed_connector_absence_fingerprint,
)
from streamt.deployer.kafka import KafkaDeployer
from streamt.deployer.plan_file import ReviewedPlanFile
from streamt.deployer.recovery_plan import RecoveryPlanFile
from streamt.deployer.state import (
    LocalState,
    ManagedResourceRecord,
    artifact_checksum,
    local_state_path,
    resource_id,
)
from streamt.deployer.state_backend import (
    OperationControlState,
    OperationIntent,
    operation_timestamp,
    state_checksum,
)
from tests.postgres.conftest import PostgresCase, WriterIdentity
from tests.postgres.test_postgres_connector_removal_commands_real import (
    _CONNECT_PASSWORD,
    _CONNECT_URL,
    _CONNECTOR,
    _ENVIRONMENT,
    _LIVE_CONFIG_SECRET,
    _NAMESPACE,
    _OWNER,
    _OWNER_DSN_ENV,
    _PROJECT,
    _WRITER_DSN_ENV,
    _address,
    _binding,
    _data,
    _durable_rows,
    _initialize_v2,
    _operation_rows,
    _payload,
    _present_observation,
    _prior_artifact,
    _verification_service,
    _write_project,
)

pytestmark = [pytest.mark.integration, pytest.mark.postgres]

_PROVIDER_FAILURE_SECRET = "connector-delete-response-loss-secret"


def _remove_connector_tombstone(project_path: Path) -> None:
    config_path = project_path / "stream_project.yml"
    raw = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    assert isinstance(raw, dict)
    lifecycle = raw.pop("lifecycle")
    assert lifecycle == {
        "connector_removals": [
            {
                "logical_owner": _OWNER,
                "name": _CONNECTOR,
                "cluster": "primary",
            }
        ]
    }
    config_path.write_text(
        yaml.safe_dump(raw, sort_keys=False),
        encoding="utf-8",
    )


def _state_history_rows(case: PostgresCase) -> list[tuple[object, ...]]:
    with case.psycopg.connect(case.owner_dsn) as connection:
        return list(
            connection.execute(
                case.sql.SQL(
                    "SELECT revision, state_serial, state_checksum, state_json, "
                    "operation_id::text FROM {}.{} WHERE namespace = %s "
                    "AND project = %s AND environment = %s ORDER BY revision"
                ).format(
                    case.sql.Identifier(case.schema),
                    case.sql.Identifier("state_history"),
                ),
                (_NAMESPACE, _PROJECT, _ENVIRONMENT),
            ).fetchall()
        )


@pytest.mark.parametrize(
    ("provider_present_after_failure", "resolution"),
    [(False, "observed"), (True, "rolled_back")],
    ids=("absent-candidate", "present-prior"),
)
def test_uncertain_connector_delete_recovers_without_tombstone(
    tmp_path: Path,
    postgres_case: PostgresCase,
    postgres_writer: WriterIdentity,
    monkeypatch: pytest.MonkeyPatch,
    provider_present_after_failure: bool,
    resolution: str,
) -> None:
    """Response loss stays blocked until exact live state is reviewed and recovered."""
    _write_project(tmp_path, postgres_case)
    store_id = _initialize_v2(postgres_case, postgres_writer)
    monkeypatch.delenv(_OWNER_DSN_ENV, raising=False)
    monkeypatch.setenv(_WRITER_DSN_ENV, postgres_writer.dsn)
    assert _OWNER_DSN_ENV not in os.environ

    binding = _binding()
    prior_checksum = artifact_checksum(_prior_artifact().to_dict())
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
    seed_operation_id = str(uuid.uuid4())
    with state_service.operation() as operation:
        observed = operation.observe()
        active = operation.begin_operation(
            observed,
            OperationIntent(
                operation_id=seed_operation_id,
                kind="adopt",
                started_at=operation_timestamp(),
                actor="postgres-connector-recovery-test",
                prior_state_serial=observed.state.state_serial,
                prior_state_checksum=state_checksum(observed.state.state),
                reviewed_plan_checksum=None,
                actions=(),
            ),
        )
        operation.commit_operation(active, initial_state)

    provider_state = {"present": True}
    observations: list[ManagedConnectorObservation] = []
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

    def delete_then_lose_response(current: ManagedConnectorObservation) -> str:
        assert current == _present_observation()
        provider_state["present"] = provider_present_after_failure
        raise RuntimeError(
            f"Connect accepted DELETE but response was lost token={_PROVIDER_FAILURE_SECRET}"
        )

    connect.delete_managed_connector.side_effect = delete_then_lose_response
    reviewed_path = tmp_path / "connector-removal-uncertain.plan.json"
    recovery_path = tmp_path / "connector-removal.recovery.json"
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
        reviewed = ReviewedPlanFile.load(reviewed_path)
        assert len(reviewed.actions) == 1
        assert reviewed.state is not None
        assert reviewed.state.backend == "postgres"
        assert reviewed.state.store_id == store_id
        assert reviewed.state.address == _address().uri
        reviewed_action = reviewed.actions[0]
        assert reviewed_action.resource_id == removed_resource_id
        assert reviewed_action.action == "delete"
        assert reviewed_action.connector_evidence is not None

        failed = runner.invoke(
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

    assert failed.exit_code == 1
    failed_payload = _payload(failed)
    assert failed_payload["status"] == "error"
    errors = failed_payload["errors"]
    assert isinstance(errors, list)
    assert isinstance(errors[0], dict)
    assert errors[0]["code"] == "E428_CONNECTOR_REMOVAL_DRIFT"
    assert _PROVIDER_FAILURE_SECRET not in failed.output
    assert provider_state["present"] is provider_present_after_failure
    connect.delete_managed_connector.assert_called_once()
    connect.delete_connector.assert_not_called()

    blocked_control = state_service.read_control().control
    assert blocked_control.status == "recovery_required"
    assert blocked_control.intent is not None
    assert blocked_control.intent.reviewed_plan_checksum == reviewed.checksum
    assert blocked_control.intent.actions == reviewed.actions
    assert [
        (entry.action_index, entry.action, entry.status, entry.succeeded)
        for entry in blocked_control.progress
    ] == [(0, "delete", "started", None), (0, "delete", "completed", False)]
    assert blocked_control.recovery is not None
    assert blocked_control.recovery.failure_code == "connector_removal_drift"
    assert blocked_control.recovery.last_completed_action_index is None
    assert state_service.read().state == initial_state

    blocked_operation_id = blocked_control.intent.operation_id
    _control_row, blocked_history_rows, _current_count = _operation_rows(postgres_case)
    blocked_history = [row for row in blocked_history_rows if row[0] == blocked_operation_id]
    assert [(row[1], row[2]) for row in blocked_history] == [
        (0, "intent"),
        (1, "progress_started"),
        (2, "progress_completed"),
        (3, "recovery_required"),
    ]
    assert json.loads(blocked_history[-1][3])["intent"]["actions"] == [reviewed_action.to_dict()]

    # Recovery must be driven only by durable v3 action evidence and the current
    # runtime binding. The lifecycle declaration that created the delete is gone.
    _remove_connector_tombstone(tmp_path)
    current_project = yaml.safe_load((tmp_path / "stream_project.yml").read_text(encoding="utf-8"))
    assert isinstance(current_project, dict)
    assert "lifecycle" not in current_project
    before_recovery_plan = _durable_rows(postgres_case)

    with (
        patch("streamt.cli.helpers.make_kafka_deployer", return_value=kafka),
        patch("streamt.cli.helpers.make_connect_deployer", return_value=connect),
    ):
        recovery_planned = runner.invoke(
            main,
            [
                "-o",
                "json",
                "state",
                "recovery-plan",
                "-p",
                str(tmp_path),
                "--resolution",
                resolution,
                "--out",
                str(recovery_path),
            ],
        )
        assert recovery_planned.exit_code == 0, recovery_planned.output
        assert _durable_rows(postgres_case) == before_recovery_plan

        recovery_plan = RecoveryPlanFile.load(recovery_path)
        assert recovery_plan.blocked_operation_id == blocked_operation_id
        assert recovery_plan.resolution == resolution
        assert recovery_plan.snapshot.store.backend == "postgres"
        assert recovery_plan.snapshot.store.store_id == store_id
        assert recovery_plan.snapshot.address == _address()
        assert len(recovery_plan.targets) == 1
        target = recovery_plan.targets[0]
        assert target.action == reviewed_action
        assert target.action.connector_evidence == reviewed_action.connector_evidence
        if resolution == "observed":
            assert target.presence == "absent"
            assert target.accepted_as == "candidate"
            assert target.fingerprint == managed_connector_absence_fingerprint(
                binding.backend_identity,
                _CONNECTOR,
            )
            expected_state = LocalState(
                project=_PROJECT,
                environment=_ENVIRONMENT,
                serial=initial_state.serial + 1,
                resources={unrelated_resource_id: unrelated_record},
            )
            assert recovery_plan.candidate_state == expected_state
        else:
            assert target.presence == "present"
            assert target.accepted_as == "prior"
            assert reviewed_action.connector_evidence is not None
            assert target.fingerprint == reviewed_action.connector_evidence.current.fingerprint
            expected_state = initial_state
            assert recovery_plan.candidate_state is None
        recovery_wire = recovery_path.read_text(encoding="utf-8")
        for forbidden in (
            _CONNECT_URL,
            _CONNECT_PASSWORD,
            _LIVE_CONFIG_SECRET,
            _PROVIDER_FAILURE_SECRET,
            postgres_case.schema,
            postgres_case.owner_dsn,
            postgres_writer.dsn,
        ):
            assert forbidden not in recovery_wire

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
                blocked_operation_id,
                "--confirm-resolution",
                resolution,
                "--confirm-evidence-checksum",
                recovery_plan.evidence_checksum,
            ],
        )

    assert recovered.exit_code == 0, recovered.output
    recovered_data = _data(recovered)
    assert recovered_data["store"] == {"backend": "postgres", "store_id": store_id}
    assert recovered_data["address"] == _address().uri
    assert recovered_data["state_changed"] is (resolution == "observed")
    assert recovered_data["state_serial"] == expected_state.serial
    assert recovered_data["state_checksum"] == state_checksum(expected_state)
    assert recovered_data["control_status"] == "clear"
    assert state_service.read().state == expected_state
    assert state_service.read_control().control == OperationControlState.clear(_address())
    assert connect.observe_managed_connector.call_count == 4
    assert [observation.exists for observation in observations] == [
        True,
        True,
        provider_present_after_failure,
        provider_present_after_failure,
    ]

    control_row, history_rows, current_count = _operation_rows(postgres_case)
    assert control_row[0:2] == (7, "clear")
    assert json.loads(control_row[2]) == OperationControlState.clear(_address()).to_dict()
    assert current_count == 1
    recovery_history = [
        row for row in history_rows if row[0] == recovery_plan.recovery_operation_id
    ]
    assert [(row[1], row[2]) for row in recovery_history] == [
        (0, "recovery_intent"),
        (1, f"recovered_{resolution}"),
    ]
    persisted_recovery = json.loads(recovery_history[0][3])
    assert persisted_recovery["control"]["intent"]["actions"] == [reviewed_action.to_dict()]
    history_wire = "\n".join(str(row[3]) for row in blocked_history + recovery_history)
    for forbidden in (
        _CONNECT_URL,
        _CONNECT_PASSWORD,
        _LIVE_CONFIG_SECRET,
        _PROVIDER_FAILURE_SECRET,
        postgres_case.owner_dsn,
        postgres_writer.dsn,
    ):
        assert forbidden not in history_wire

    state_history = _state_history_rows(postgres_case)
    assert [row[0] for row in state_history] == ([1, 2] if resolution == "observed" else [1])
    expected_state_history = [(initial_state.serial, state_checksum(initial_state))]
    if resolution == "observed":
        expected_state_history.append((expected_state.serial, state_checksum(expected_state)))
    assert [(row[1], row[2]) for row in state_history] == expected_state_history
    assert json.loads(state_history[0][3]) == initial_state.to_dict()
    assert state_history[0][4] == seed_operation_id
    if resolution == "observed":
        assert json.loads(state_history[1][3]) == expected_state.to_dict()
        assert state_history[1][4] == recovery_plan.recovery_operation_id
    assert not local_state_path(tmp_path, environment=_ENVIRONMENT).exists()
    assert not (tmp_path / ".streamt").exists()
