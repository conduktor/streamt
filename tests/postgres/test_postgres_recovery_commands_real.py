"""Real PostgreSQL command coverage for reviewed recovery."""

from __future__ import annotations

import json
import uuid
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import yaml
from click.testing import CliRunner

from streamt.cli import main
from streamt.compiler.manifest import ArtifactOwnership, GatewayRuleArtifact
from streamt.deployer.gateway import (
    GatewayBackendBinding,
    GatewayDeployer,
    ManagedGatewayRuleObservation,
    ManagedGatewaySnapshot,
    build_desired_gateway_rule,
)
from streamt.deployer.kafka import KafkaDeployer
from streamt.deployer.postgres_state import (
    PostgresStateInitializer,
    PrivatePostgresStateV2Migrator,
)
from streamt.deployer.postgres_state_backend import PrivatePostgresStateReadBackend
from streamt.deployer.recovery_plan import RecoveryPlanFile
from streamt.deployer.state import ManagedResourceRecord, artifact_checksum, resource_id
from streamt.deployer.state_backend import (
    DeploymentStateService,
    GatewayActionEvidence,
    GatewayActionSurfaceEvidence,
    OperationAction,
    OperationControlState,
    OperationIntent,
    OperationProgress,
    RecoveryRecord,
    StateAddress,
    operation_timestamp,
    state_checksum,
)
from tests.postgres.conftest import PostgresCase, WriterIdentity

pytestmark = [pytest.mark.integration, pytest.mark.postgres]

_WRITER_DSN_ENV = "STREAMT_RECOVERY_COMMAND_WRITER_DSN"
_GATEWAY_ENDPOINT = "https://gateway.example.test/admin"
_GATEWAY_VCLUSTER = "recovery-vcluster"
_GATEWAY_SECRET = "gateway-command-secret-9281"


def _address() -> StateAddress:
    return StateAddress(
        namespace="platform",
        project="recovery-command",
        environment="default",
    )


def _write_project(path: Path, case: PostgresCase) -> None:
    (path / "stream_project.yml").write_text(
        yaml.safe_dump(
            {
                "apiVersion": "streamt.dev/v1alpha1",
                "project": {"name": "recovery-command"},
                "runtime": {
                    "kafka": {"bootstrap_servers": "unreachable.invalid:9092"}
                },
                "deployment_state": {
                    "backend": "postgres",
                    "namespace": "platform",
                    "lock_timeout_seconds": 10,
                    "postgres": {
                        "dsn_env": "STREAMT_RECOVERY_COMMAND_OWNER_DSN",
                        "writer_dsn_env": _WRITER_DSN_ENV,
                        "schema": case.schema,
                    },
                },
            }
        ),
        encoding="utf-8",
    )


def _write_gateway_project(
    path: Path,
    case: PostgresCase,
    *,
    ownership_mode: str = "managed",
) -> None:
    (path / "stream_project.yml").write_text(
        yaml.safe_dump(
            {
                "apiVersion": "streamt.dev/v1alpha1",
                "project": {"name": "recovery-command"},
                "runtime": {
                    "kafka": {"bootstrap_servers": "unreachable.invalid:9092"},
                    "conduktor": {
                        "gateway": {
                            "admin_url": _GATEWAY_ENDPOINT,
                            "password": _GATEWAY_SECRET,
                            "virtual_cluster": _GATEWAY_VCLUSTER,
                        }
                    },
                },
                "sources": [{"name": "orders_source", "topic": "orders.v1"}],
                "models": [
                    {
                        "name": "orders_rule",
                        "materialized": "virtual_topic",
                        "gateway": {"virtual_topic": {"name": "orders.public"}},
                        "ownership": {"mode": ownership_mode},
                        "sql": 'SELECT * FROM {{ source("orders_source") }}',
                    }
                ],
                "deployment_state": {
                    "backend": "postgres",
                    "namespace": "platform",
                    "lock_timeout_seconds": 10,
                    "postgres": {
                        "dsn_env": "STREAMT_RECOVERY_COMMAND_OWNER_DSN",
                        "writer_dsn_env": _WRITER_DSN_ENV,
                        "schema": case.schema,
                    },
                },
            }
        ),
        encoding="utf-8",
    )


def _service(
    case: PostgresCase,
    writer: WriterIdentity,
) -> DeploymentStateService:
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
    return DeploymentStateService(
        backend=PrivatePostgresStateReadBackend(
            dsn=writer.dsn,
            schema=case.schema,
            lock_timeout_seconds=10,
        ),
        address=_address(),
    )


def _operation_history(case: PostgresCase) -> list[tuple[object, ...]]:
    with case.psycopg.connect(case.owner_dsn) as connection:
        return list(
            connection.execute(
                case.sql.SQL(
                    "SELECT operation_id::text, event_index, event_kind, control_json "
                    "FROM {}.{} ORDER BY operation_id, event_index"
                ).format(
                    case.sql.Identifier(case.schema),
                    case.sql.Identifier("operation_history"),
                )
            ).fetchall()
        )


def test_postgres_abandoned_recovery_commands_use_writer_authority(
    tmp_path: Path,
    postgres_case: PostgresCase,
    postgres_writer: WriterIdentity,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_project(tmp_path, postgres_case)
    monkeypatch.setenv(_WRITER_DSN_ENV, postgres_writer.dsn)
    service = _service(postgres_case, postgres_writer)
    blocked_operation_id = str(uuid.uuid4())
    with service.operation() as operation:
        snapshot = operation.observe()
        operation.begin_operation(
            snapshot,
            OperationIntent(
                operation_id=blocked_operation_id,
                kind="apply",
                started_at=operation_timestamp(),
                actor="postgres-command-test",
                prior_state_serial=snapshot.state.state_serial,
                prior_state_checksum=state_checksum(snapshot.state.state),
                reviewed_plan_checksum=None,
                actions=(),
            ),
        )

    plan_path = tmp_path / "recovery.json"
    runner = CliRunner()
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
            "abandoned_before_mutation",
            "--out",
            str(plan_path),
        ],
    )

    assert planned.exit_code == 0, planned.output
    plan_data = json.loads(planned.stdout)["data"]
    assert plan_data["blocked_operation_id"] == blocked_operation_id

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
            str(plan_path),
            "--confirm-operation-id",
            blocked_operation_id,
            "--confirm-resolution",
            "abandoned_before_mutation",
            "--confirm-evidence-checksum",
            plan_data["evidence_checksum"],
        ],
    )

    assert recovered.exit_code == 0, recovered.output
    recovered_data = json.loads(recovered.stdout)["data"]
    assert recovered_data["store"]["backend"] == "postgres"
    assert recovered_data["address"] == _address().uri
    assert recovered_data["state_serial"] == 0
    assert recovered_data["state_changed"] is False
    assert recovered_data["control_status"] == "clear"
    assert service.read_control().control == OperationControlState.clear(_address())


def test_postgres_gateway_recovery_commands_round_trip_exact_evidence(
    tmp_path: Path,
    postgres_case: PostgresCase,
    postgres_writer: WriterIdentity,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_gateway_project(tmp_path, postgres_case)
    monkeypatch.setenv(_WRITER_DSN_ENV, postgres_writer.dsn)
    service = _service(postgres_case, postgres_writer)
    binding = GatewayBackendBinding.from_endpoint(
        _GATEWAY_ENDPOINT,
        virtual_cluster=_GATEWAY_VCLUSTER,
    )
    artifact = GatewayRuleArtifact(
        name="orders_rule",
        virtual_topic="orders.public",
        physical_topic="orders.v1",
        ownership=ArtifactOwnership(
            project="recovery-command",
            owner_type="model",
            owner_name="orders_rule",
            mode="managed",
        ),
    )
    desired = build_desired_gateway_rule(artifact, binding)
    current = ManagedGatewayRuleObservation(
        binding=binding,
        logical_name=artifact.name,
        alias_name=artifact.virtual_topic,
        exists=False,
    )
    target_id = resource_id(
        "recovery-command",
        "default",
        "gateway_rule",
        "orders_rule",
    )
    action = OperationAction(
        index=0,
        resource_id=target_id,
        action="create",
        gateway_evidence=GatewayActionEvidence(
            version=1,
            backend_identity=binding.backend_identity,
            rule_name=artifact.name,
            alias_name=artifact.virtual_topic,
            current=GatewayActionSurfaceEvidence(
                exists=False,
                fingerprint=current.fingerprint,
                managed_interceptor_count=0,
            ),
            desired=GatewayActionSurfaceEvidence(
                exists=True,
                fingerprint=desired.fingerprint,
                managed_interceptor_count=len(desired.interceptors),
            ),
        ),
    )
    blocked_operation_id = str(uuid.uuid4())
    with service.operation() as operation:
        snapshot = operation.observe()
        intent = OperationIntent(
            operation_id=blocked_operation_id,
            kind="apply",
            started_at=operation_timestamp(),
            actor="postgres-gateway-command-test",
            prior_state_serial=snapshot.state.state_serial,
            prior_state_checksum=state_checksum(snapshot.state.state),
            reviewed_plan_checksum=None,
            actions=(action,),
        )
        active = operation.begin_operation(snapshot, intent)
        started = operation.record_progress(
            active,
            OperationProgress(
                operation_id=blocked_operation_id,
                action_index=0,
                resource_id=target_id,
                action="create",
                status="started",
                succeeded=None,
                recorded_at=operation_timestamp(),
            ),
        )
        blocked = operation.mark_recovery_required(
            started,
            RecoveryRecord(
                operation_id=blocked_operation_id,
                failure_code="runtime_outcome_unknown",
                failed_at=operation_timestamp(),
                last_completed_action_index=None,
            ),
        )
    assert blocked.control.control.status == "recovery_required"

    live_snapshot = ManagedGatewaySnapshot(
        binding=binding,
        aliases=(
            GatewayDeployer._parse_alias_topic(
                {
                    "kind": "AliasTopic",
                    "apiVersion": "gateway/v2",
                    "metadata": {
                        "name": artifact.virtual_topic,
                        "vCluster": _GATEWAY_VCLUSTER,
                    },
                    "spec": {
                        "physicalName": artifact.physical_topic,
                        "physicalCluster": "main",
                    },
                }
            ),
        ),
        interceptors=(),
    )
    assert live_snapshot.rule(artifact.name, artifact.virtual_topic) == desired
    gateway = MagicMock(spec=GatewayDeployer)
    gateway.cluster_binding = binding
    gateway.observe_managed_gateway_snapshot.return_value = live_snapshot
    kafka = MagicMock(spec=KafkaDeployer)
    plan_path = tmp_path / "gateway-observed.recovery.json"
    runner = CliRunner()

    with (
        patch("streamt.cli.helpers.make_kafka_deployer", return_value=kafka) as make_kafka,
        patch(
            "streamt.cli.helpers.make_gateway_deployer",
            return_value=gateway,
        ) as make_gateway,
    ):
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
                str(plan_path),
            ],
        )

        assert planned.exit_code == 0, planned.output
        plan_data = json.loads(planned.stdout)["data"]
        reviewed = RecoveryPlanFile.load(plan_path)
        assert reviewed.blocked_operation_id == blocked_operation_id
        assert reviewed.evidence_checksum == plan_data["evidence_checksum"]
        assert reviewed.resolution == "observed"
        assert len(reviewed.targets) == 1
        target = reviewed.targets[0]
        assert target.action == action
        assert target.action.gateway_evidence == action.gateway_evidence
        assert target.presence == "present"
        assert target.accepted_as == "candidate"
        assert target.fingerprint == desired.fingerprint
        assert reviewed.candidate_state is not None
        assert reviewed.candidate_state.serial == reviewed.snapshot.state.serial + 1
        assert reviewed.candidate_state.resources == {
            target_id: ManagedResourceRecord(
                physical_name=artifact.virtual_topic,
                ownership="managed",
                artifact_checksum=artifact_checksum(artifact.to_dict()),
                backend=binding.backend_identity,
            )
        }
        assert _GATEWAY_SECRET not in plan_path.read_text(encoding="utf-8")

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
                str(plan_path),
                "--confirm-operation-id",
                blocked_operation_id,
                "--confirm-resolution",
                "observed",
                "--confirm-evidence-checksum",
                reviewed.evidence_checksum,
            ],
        )

    assert recovered.exit_code == 0, recovered.output
    recovered_data = json.loads(recovered.stdout)["data"]
    assert recovered_data["store"]["backend"] == "postgres"
    assert recovered_data["address"] == _address().uri
    assert recovered_data["state_serial"] == reviewed.candidate_state.serial
    assert recovered_data["state_checksum"] == state_checksum(reviewed.candidate_state)
    assert recovered_data["state_changed"] is True
    assert recovered_data["control_status"] == "clear"
    assert make_gateway.call_count == 2
    assert make_kafka.call_count == 2
    assert gateway.observe_managed_gateway_snapshot.call_count == 2
    assert service.read().state == reviewed.candidate_state
    assert service.read_control().control == OperationControlState.clear(_address())

    recovery_history = [
        row
        for row in _operation_history(postgres_case)
        if row[0] == reviewed.recovery_operation_id
    ]
    assert [(row[1], row[2]) for row in recovery_history] == [
        (0, "recovery_intent"),
        (1, "recovered_observed"),
    ]
    persisted_snapshot = json.loads(recovery_history[0][3])
    assert (
        persisted_snapshot["control"]["intent"]["actions"][0]["gateway_evidence"]
        == action.gateway_evidence.to_dict()
    )
    history_before_retry = _operation_history(postgres_case)

    with (
        patch(
            "streamt.cli.helpers.make_kafka_deployer",
            side_effect=AssertionError("completed retry read Kafka"),
        ) as retry_kafka,
        patch(
            "streamt.cli.helpers.make_gateway_deployer",
            side_effect=AssertionError("completed retry read Gateway"),
        ) as retry_gateway,
    ):
        retried = runner.invoke(
            main,
            [
                "-o",
                "json",
                "state",
                "recover",
                "-p",
                str(tmp_path),
                "--plan",
                str(plan_path),
                "--confirm-operation-id",
                blocked_operation_id,
                "--confirm-resolution",
                "observed",
                "--confirm-evidence-checksum",
                reviewed.evidence_checksum,
            ],
        )

    assert retried.exit_code == 0, retried.output
    retry_gateway.assert_not_called()
    retry_kafka.assert_not_called()
    assert _operation_history(postgres_case) == history_before_retry
    assert service.read().state == reviewed.candidate_state
    assert service.read_control().control == OperationControlState.clear(_address())


def test_postgres_gateway_adoption_unknown_outcome_recovers_and_retries_idempotently(
    tmp_path: Path,
    postgres_case: PostgresCase,
    postgres_writer: WriterIdentity,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_gateway_project(
        tmp_path,
        postgres_case,
        ownership_mode="adopted",
    )
    monkeypatch.setenv(_WRITER_DSN_ENV, postgres_writer.dsn)
    service = _service(postgres_case, postgres_writer)
    binding = GatewayBackendBinding.from_endpoint(
        _GATEWAY_ENDPOINT,
        virtual_cluster=_GATEWAY_VCLUSTER,
    )
    artifact = GatewayRuleArtifact(
        name="orders_rule",
        virtual_topic="orders.public",
        physical_topic="orders.v1",
        interceptors=[],
        ownership=ArtifactOwnership(
            project="recovery-command",
            owner_type="model",
            owner_name="orders_rule",
            mode="adopted",
        ),
    )
    desired = build_desired_gateway_rule(artifact, binding)
    current = ManagedGatewayRuleObservation(
        binding=binding,
        logical_name=artifact.name,
        alias_name=artifact.virtual_topic,
        exists=True,
        physical_name="orders.legacy.private",
        physical_cluster="main",
        interceptors=(),
    )
    target_id = resource_id(
        "recovery-command",
        "default",
        "gateway_rule",
        "orders_rule",
    )
    action = OperationAction(
        index=0,
        resource_id=target_id,
        action="adopt",
        gateway_evidence=GatewayActionEvidence(
            version=1,
            backend_identity=binding.backend_identity,
            rule_name=artifact.name,
            alias_name=artifact.virtual_topic,
            current=GatewayActionSurfaceEvidence(
                exists=True,
                fingerprint=current.fingerprint,
                managed_interceptor_count=0,
            ),
            desired=GatewayActionSurfaceEvidence(
                exists=True,
                fingerprint=desired.fingerprint,
                managed_interceptor_count=0,
            ),
        ),
    )
    blocked_operation_id = str(uuid.uuid4())
    with service.operation() as operation:
        snapshot = operation.observe()
        active = operation.begin_operation(
            snapshot,
            OperationIntent(
                operation_id=blocked_operation_id,
                kind="adopt",
                started_at=operation_timestamp(),
                actor="postgres-gateway-adoption-test",
                prior_state_serial=snapshot.state.state_serial,
                prior_state_checksum=state_checksum(snapshot.state.state),
                reviewed_plan_checksum=None,
                actions=(action,),
            ),
        )
        started = operation.record_progress(
            active,
            OperationProgress(
                operation_id=blocked_operation_id,
                action_index=0,
                resource_id=target_id,
                action="adopt",
                status="started",
                succeeded=None,
                recorded_at=operation_timestamp(),
            ),
        )
        completed = operation.record_progress(
            started,
            OperationProgress(
                operation_id=blocked_operation_id,
                action_index=0,
                resource_id=target_id,
                action="adopt",
                status="completed",
                succeeded=True,
                recorded_at=operation_timestamp(),
            ),
        )
        blocked = operation.mark_recovery_required(
            completed,
            RecoveryRecord(
                operation_id=blocked_operation_id,
                failure_code="adoption_state_commit_uncertain",
                failed_at=operation_timestamp(),
                last_completed_action_index=0,
            ),
        )
    assert blocked.state.state.resources == {}
    assert blocked.control.control.status == "recovery_required"
    assert blocked.control.control.recovery is not None
    assert blocked.control.control.recovery.mutation_may_have_succeeded is True

    provider_snapshot = MagicMock(spec=ManagedGatewaySnapshot)
    provider_snapshot.binding = binding
    provider_snapshot.rule.return_value = current
    gateway = MagicMock(spec=GatewayDeployer)
    gateway.cluster_binding = binding
    gateway.observe_managed_gateway_snapshot.return_value = provider_snapshot
    kafka = MagicMock(spec=KafkaDeployer)
    plan_path = tmp_path / "gateway-adopt-observed.recovery.json"
    runner = CliRunner()

    with (
        patch("streamt.cli.helpers.make_kafka_deployer", return_value=kafka) as make_kafka,
        patch(
            "streamt.cli.helpers.make_gateway_deployer",
            return_value=gateway,
        ) as make_gateway,
    ):
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
                str(plan_path),
            ],
        )

        assert planned.exit_code == 0, planned.output
        reviewed = RecoveryPlanFile.load(plan_path)
        plan_data = json.loads(planned.stdout)["data"]
        assert reviewed.blocked_operation_id == blocked_operation_id
        assert reviewed.evidence_checksum == plan_data["evidence_checksum"]
        assert reviewed.resolution == "observed"
        assert len(reviewed.targets) == 1
        target = reviewed.targets[0]
        assert target.action == action
        assert target.presence == "present"
        assert target.accepted_as == "candidate"
        assert target.fingerprint == current.fingerprint
        assert target.fingerprint != desired.fingerprint
        assert reviewed.candidate_state is not None
        assert reviewed.candidate_state.serial == reviewed.snapshot.state.serial + 1
        assert reviewed.candidate_state.resources == {
            target_id: ManagedResourceRecord(
                physical_name=artifact.virtual_topic,
                ownership="adopted",
                artifact_checksum=artifact_checksum(artifact.to_dict()),
                backend=binding.backend_identity,
            )
        }
        assert _GATEWAY_SECRET not in plan_path.read_text(encoding="utf-8")
        assert "orders.legacy.private" not in plan_path.read_text(encoding="utf-8")

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
                str(plan_path),
                "--confirm-operation-id",
                blocked_operation_id,
                "--confirm-resolution",
                "observed",
                "--confirm-evidence-checksum",
                reviewed.evidence_checksum,
            ],
        )

    assert recovered.exit_code == 0, recovered.output
    recovered_data = json.loads(recovered.stdout)["data"]
    assert recovered_data["store"]["backend"] == "postgres"
    assert recovered_data["address"] == _address().uri
    assert recovered_data["state_serial"] == reviewed.candidate_state.serial
    assert recovered_data["state_checksum"] == state_checksum(reviewed.candidate_state)
    assert recovered_data["state_changed"] is True
    assert recovered_data["control_status"] == "clear"
    assert make_gateway.call_count == 2
    assert make_kafka.call_count == 2
    assert gateway.observe_managed_gateway_snapshot.call_count == 2
    assert provider_snapshot.rule.call_args_list == [
        ((artifact.name, artifact.virtual_topic), {}),
        ((artifact.name, artifact.virtual_topic), {}),
    ]
    for method in (
        "apply_managed_gateway_rule",
        "delete_managed_gateway_rule",
        "create_alias_topic",
        "delete_alias_topic",
        "create_interceptor",
        "delete_interceptor",
        "apply",
        "delete",
    ):
        getattr(gateway, method).assert_not_called()
    assert service.read().state == reviewed.candidate_state
    assert service.read_control().control == OperationControlState.clear(_address())

    blocked_history = [
        row for row in _operation_history(postgres_case) if row[0] == blocked_operation_id
    ]
    assert [(row[1], row[2]) for row in blocked_history] == [
        (0, "intent"),
        (1, "progress_started"),
        (2, "progress_completed"),
        (3, "recovery_required"),
    ]
    persisted_intent = json.loads(blocked_history[0][3])["intent"]
    assert persisted_intent["kind"] == "adopt"
    assert persisted_intent["actions"] == [action.to_dict()]
    recovery_history = [
        row
        for row in _operation_history(postgres_case)
        if row[0] == reviewed.recovery_operation_id
    ]
    assert [(row[1], row[2]) for row in recovery_history] == [
        (0, "recovery_intent"),
        (1, "recovered_observed"),
    ]
    history_before_retry = _operation_history(postgres_case)

    with (
        patch(
            "streamt.cli.helpers.make_kafka_deployer",
            side_effect=AssertionError("completed adoption retry read Kafka"),
        ) as retry_kafka,
        patch(
            "streamt.cli.helpers.make_gateway_deployer",
            side_effect=AssertionError("completed adoption retry read Gateway"),
        ) as retry_gateway,
    ):
        retried = runner.invoke(
            main,
            [
                "-o",
                "json",
                "state",
                "recover",
                "-p",
                str(tmp_path),
                "--plan",
                str(plan_path),
                "--confirm-operation-id",
                blocked_operation_id,
                "--confirm-resolution",
                "observed",
                "--confirm-evidence-checksum",
                reviewed.evidence_checksum,
            ],
        )

    assert retried.exit_code == 0, retried.output
    retry_gateway.assert_not_called()
    retry_kafka.assert_not_called()
    assert _operation_history(postgres_case) == history_before_retry
    assert service.read().state == reviewed.candidate_state
    assert service.read_control().control == OperationControlState.clear(_address())
