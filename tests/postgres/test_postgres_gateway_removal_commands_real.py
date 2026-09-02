"""Real PostgreSQL CLI lifecycle gate for reviewed Gateway removals."""

from __future__ import annotations

import json
import os
import uuid
from contextlib import ExitStack
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import yaml
from click.testing import CliRunner, Result

from streamt.cli import main
from streamt.compiler import Compiler
from streamt.core.parser import ProjectParser
from streamt.deployer.gateway import (
    GatewayBackendBinding,
    GatewayDeployer,
    ManagedGatewayRuleObservation,
    managed_gateway_absence_fingerprint,
)
from streamt.deployer.kafka import KafkaDeployer
from streamt.deployer.plan_file import PLAN_FILE_VERSION, ReviewedPlanFile
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

_PROJECT = "gateway-removal-command"
_ENVIRONMENT = "default"
_NAMESPACE = "platform"
_OWNER = "orders_view"
_RULE = "orders_access_rule"
_ALIAS = "orders.public"
_PHYSICAL = "orders.raw"
_ADMIN_URL = "https://gateway.example.test/admin"
_VIRTUAL_CLUSTER = "payments"
_OWNER_DSN_ENV = "STREAMT_GATEWAY_REMOVAL_OWNER_DSN"
_WRITER_DSN_ENV = "STREAMT_GATEWAY_REMOVAL_WRITER_DSN"


def _address() -> StateAddress:
    return StateAddress(
        namespace=_NAMESPACE,
        project=_PROJECT,
        environment=_ENVIRONMENT,
    )


def _write_project(path: Path, case: PostgresCase) -> None:
    project = {
        "apiVersion": "streamt.dev/v1alpha1",
        "project": {"name": _PROJECT},
        "runtime": {
            "kafka": {"bootstrap_servers": "broker.invalid:9092"},
            "conduktor": {
                "gateway": {
                    "admin_url": _ADMIN_URL,
                    "password": "gateway-postgres-plan-secret",
                    "virtual_cluster": _VIRTUAL_CLUSTER,
                }
            },
        },
        "lifecycle": {
            "gateway_rule_removals": [
                {
                    "logical_owner": _OWNER,
                    "prior_artifact": {
                        "name": _RULE,
                        "virtualTopic": _ALIAS,
                        "physicalTopic": _PHYSICAL,
                        "interceptors": [],
                    },
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


def test_postgres_reviewed_gateway_removal_is_exact_and_durable(
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

    project = ProjectParser(tmp_path).parse()
    manifest = Compiler(project).compile(dry_run=True)
    raw_removals = manifest.artifacts["gateway_rule_removals"]
    assert len(raw_removals) == 1
    raw_removal = raw_removals[0]
    prior_artifact = raw_removal["priorArtifact"]
    assert raw_removal["logicalOwner"] == _OWNER
    assert isinstance(prior_artifact, dict)

    binding = GatewayBackendBinding.from_endpoint(
        _ADMIN_URL,
        virtual_cluster=_VIRTUAL_CLUSTER,
    )
    removed_resource_id = resource_id(
        _PROJECT,
        _ENVIRONMENT,
        "gateway_rule",
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
                physical_name=_ALIAS,
                ownership="managed",
                artifact_checksum=artifact_checksum(prior_artifact),
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
                actor="postgres-gateway-removal-test",
                prior_state_serial=observed.state.state_serial,
                prior_state_checksum=state_checksum(observed.state.state),
                reviewed_plan_checksum=None,
                actions=(),
            ),
        )
        operation.commit_operation(active, initial_state)

    provider_state = {"present": True}
    snapshot_observations: list[ManagedGatewayRuleObservation] = []
    durable_progress_controls: list[OperationControlState] = []
    gateway = MagicMock(spec=GatewayDeployer)
    gateway.cluster_binding = binding
    kafka = MagicMock(spec=KafkaDeployer)

    def observe_snapshot() -> MagicMock:
        current = ManagedGatewayRuleObservation(
            binding=binding,
            logical_name=_RULE,
            alias_name=_ALIAS,
            exists=provider_state["present"],
            physical_name=_PHYSICAL if provider_state["present"] else None,
            physical_cluster="main" if provider_state["present"] else None,
        )
        snapshot_observations.append(current)
        snapshot = MagicMock()
        snapshot.binding = binding

        def rule(rule_name: str, alias_name: str) -> ManagedGatewayRuleObservation:
            assert (rule_name, alias_name) == (_RULE, _ALIAS)
            return current

        snapshot.rule.side_effect = rule
        return snapshot

    gateway.observe_managed_gateway_snapshot.side_effect = observe_snapshot

    def delete_rule(current: ManagedGatewayRuleObservation) -> str:
        assert current == snapshot_observations[-1]
        assert current.logical_name == _RULE
        assert current.alias_name == _ALIAS
        assert current.exists is True
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

    gateway.delete_managed_gateway_rule.side_effect = delete_rule
    reviewed_path = tmp_path / "gateway-removal.plan.json"
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
                    f"streamt.cli.commands.{command}.make_gateway_deployer",
                    return_value=gateway,
                )
            )

        planned = runner.invoke(
            main,
            ["-o", "json", "plan", "-p", str(tmp_path), "--out", str(reviewed_path)],
        )
        assert planned.exit_code == 0, planned.output
        assert _data(planned)["deletes"] == 1
        assert _data(planned)["gateway_removal_assessments"] == []

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
                "kind": "gateway_rule",
                "name": _RULE,
                "action": "delete",
                "changes": {
                    "categories": ["presence"],
                    "current": {
                        "exists": True,
                        "fingerprint": snapshot_observations[-1].fingerprint,
                        "managed_interceptor_count": 0,
                    },
                    "desired": {
                        "exists": False,
                        "fingerprint": managed_gateway_absence_fingerprint(
                            binding.backend_identity,
                            _RULE,
                            _ALIAS,
                        ),
                        "managed_interceptor_count": 0,
                    },
                },
            }
        ]
        assert len(reviewed.actions) == 1
        reviewed_action = reviewed.actions[0]
        assert reviewed_action.index == 0
        assert reviewed_action.resource_id == removed_resource_id
        assert reviewed_action.action == "delete"
        assert reviewed_action.gateway_evidence is not None
        assert reviewed_action.gateway_evidence.backend_identity == binding.backend_identity
        assert reviewed_action.gateway_evidence.rule_name == _RULE
        assert reviewed_action.gateway_evidence.alias_name == _ALIAS
        assert reviewed_action.gateway_evidence.current.exists is True
        assert reviewed_action.gateway_evidence.desired.exists is False
        reviewed_wire = reviewed_path.read_text(encoding="utf-8")
        for forbidden in (
            _ADMIN_URL,
            "gateway-postgres-plan-secret",
            _PHYSICAL,
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
        assert "Destructive ops blocked" in errors[0]["message"]
        gateway.delete_managed_gateway_rule.assert_not_called()
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
        assert applied.exit_code == 0, applied.output

    applied_data = _data(applied)
    assert applied_data["deleted"] == [f"gateway_rule:{_RULE}"]
    assert applied_data["committed"] is True
    assert applied_data["state_serial"] == initial_state.serial + 1
    assert applied_data["plan_checksum"] == reviewed.checksum
    gateway.delete_managed_gateway_rule.assert_called_once()
    gateway.apply_managed_gateway_rule.assert_not_called()
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
    assert durable_control.intent is not None
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
    assert [observation.exists for observation in snapshot_observations] == [True, True, True]


def test_postgres_uncertain_gateway_delete_recovers_completed_candidate(
    tmp_path: Path,
    postgres_case: PostgresCase,
    postgres_writer: WriterIdentity,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_project(tmp_path, postgres_case)
    _initialize_v2(postgres_case, postgres_writer)
    monkeypatch.delenv(_OWNER_DSN_ENV, raising=False)
    monkeypatch.setenv(_WRITER_DSN_ENV, postgres_writer.dsn)

    project = ProjectParser(tmp_path).parse()
    manifest = Compiler(project).compile(dry_run=True)
    raw_removal = manifest.artifacts["gateway_rule_removals"][0]
    prior_artifact = raw_removal["priorArtifact"]
    assert isinstance(prior_artifact, dict)
    binding = GatewayBackendBinding.from_endpoint(
        _ADMIN_URL,
        virtual_cluster=_VIRTUAL_CLUSTER,
    )
    removed_resource_id = resource_id(
        _PROJECT,
        _ENVIRONMENT,
        "gateway_rule",
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
                physical_name=_ALIAS,
                ownership="managed",
                artifact_checksum=artifact_checksum(prior_artifact),
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
                actor="postgres-gateway-removal-test",
                prior_state_serial=observed.state.state_serial,
                prior_state_checksum=state_checksum(observed.state.state),
                reviewed_plan_checksum=None,
                actions=(),
            ),
        )
        operation.commit_operation(active, initial_state)

    provider_state = {"present": True}
    snapshot_observations: list[ManagedGatewayRuleObservation] = []
    gateway = MagicMock(spec=GatewayDeployer)
    gateway.cluster_binding = binding
    kafka = MagicMock(spec=KafkaDeployer)

    def observe_snapshot() -> MagicMock:
        current = ManagedGatewayRuleObservation(
            binding=binding,
            logical_name=_RULE,
            alias_name=_ALIAS,
            exists=provider_state["present"],
            physical_name=_PHYSICAL if provider_state["present"] else None,
            physical_cluster="main" if provider_state["present"] else None,
        )
        snapshot_observations.append(current)
        snapshot = MagicMock()
        snapshot.binding = binding

        def rule(rule_name: str, alias_name: str) -> ManagedGatewayRuleObservation:
            assert (rule_name, alias_name) == (_RULE, _ALIAS)
            return current

        snapshot.rule.side_effect = rule
        return snapshot

    gateway.observe_managed_gateway_snapshot.side_effect = observe_snapshot

    def delete_then_lose_response(current: ManagedGatewayRuleObservation) -> str:
        assert current == snapshot_observations[-1]
        assert current.exists is True
        provider_state["present"] = False
        raise RuntimeError("Gateway response was lost token=provider-runtime-secret")

    gateway.delete_managed_gateway_rule.side_effect = delete_then_lose_response
    reviewed_path = tmp_path / "gateway-removal-uncertain.plan.json"
    recovery_path = tmp_path / "gateway-removal.recovery.json"
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
                    f"streamt.cli.commands.{command}.make_gateway_deployer",
                    return_value=gateway,
                )
            )

        planned = runner.invoke(
            main,
            ["-o", "json", "plan", "-p", str(tmp_path), "--out", str(reviewed_path)],
        )
        assert planned.exit_code == 0, planned.output
        reviewed = ReviewedPlanFile.load(reviewed_path)
        assert len(reviewed.actions) == 1
        reviewed_action = reviewed.actions[0]
        assert reviewed_action.resource_id == removed_resource_id
        assert reviewed_action.action == "delete"
        assert reviewed_action.gateway_evidence is not None

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
    assert provider_state["present"] is False
    gateway.delete_managed_gateway_rule.assert_called_once()
    assert "provider-runtime-secret" not in failed.output
    blocked_control = state_service.read_control().control
    assert blocked_control.status == "recovery_required"
    assert blocked_control.intent is not None
    assert blocked_control.intent.reviewed_plan_checksum == reviewed.checksum
    assert blocked_control.intent.actions == reviewed.actions
    assert [
        (entry.action_index, entry.action, entry.status, entry.succeeded)
        for entry in blocked_control.progress
    ] == [
        (0, "delete", "started", None),
        (0, "delete", "completed", False),
    ]
    assert blocked_control.recovery is not None
    assert blocked_control.recovery.failure_code == "runtime_action_failed"
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
    persisted_blocked = json.loads(blocked_history[-1][3])
    assert persisted_blocked["intent"]["actions"] == [reviewed_action.to_dict()]
    assert persisted_blocked["recovery"]["failure_code"] == "runtime_action_failed"

    with (
        patch("streamt.cli.helpers.make_kafka_deployer", return_value=kafka),
        patch(
            "streamt.cli.helpers.make_gateway_deployer",
            return_value=gateway,
        ),
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
                "observed",
                "--out",
                str(recovery_path),
            ],
        )
        assert recovery_planned.exit_code == 0, recovery_planned.output
        recovery_plan = RecoveryPlanFile.load(recovery_path)
        assert recovery_plan.blocked_operation_id == blocked_operation_id
        assert recovery_plan.resolution == "observed"
        assert len(recovery_plan.targets) == 1
        target = recovery_plan.targets[0]
        assert target.action == reviewed_action
        assert target.action.gateway_evidence == reviewed_action.gateway_evidence
        assert target.presence == "absent"
        assert target.accepted_as == "candidate"
        assert target.fingerprint == reviewed_action.gateway_evidence.desired.fingerprint
        assert recovery_plan.candidate_state is not None
        expected_state = LocalState(
            project=_PROJECT,
            environment=_ENVIRONMENT,
            serial=initial_state.serial + 1,
            resources={unrelated_resource_id: unrelated_record},
        )
        assert recovery_plan.candidate_state == expected_state
        recovery_wire = recovery_path.read_text(encoding="utf-8")
        for forbidden in (
            _ADMIN_URL,
            "gateway-postgres-plan-secret",
            "provider-runtime-secret",
            _PHYSICAL,
            postgres_case.schema,
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
                "observed",
                "--confirm-evidence-checksum",
                recovery_plan.evidence_checksum,
            ],
        )

    assert recovered.exit_code == 0, recovered.output
    recovered_data = _data(recovered)
    assert recovered_data["state_changed"] is True
    assert recovered_data["state_serial"] == expected_state.serial
    assert recovered_data["state_checksum"] == state_checksum(expected_state)
    assert recovered_data["control_status"] == "clear"
    assert state_service.read().state == expected_state
    assert state_service.read_control().control == OperationControlState.clear(_address())

    control_row, history_rows, current_count = _operation_rows(postgres_case)
    assert control_row[0:2] == (7, "clear")
    assert current_count == 1
    recovery_history = [
        row for row in history_rows if row[0] == recovery_plan.recovery_operation_id
    ]
    assert [(row[1], row[2]) for row in recovery_history] == [
        (0, "recovery_intent"),
        (1, "recovered_observed"),
    ]
    persisted_recovery = json.loads(recovery_history[0][3])
    assert persisted_recovery["control"]["intent"]["actions"] == [reviewed_action.to_dict()]
    assert gateway.observe_managed_gateway_snapshot.call_count == 4
    assert [observation.exists for observation in snapshot_observations] == [
        True,
        True,
        False,
        False,
    ]
