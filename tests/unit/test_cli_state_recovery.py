"""CLI coverage for explicit reviewed deployment-state recovery."""

from __future__ import annotations

import json
import stat
from pathlib import Path
from typing import cast
from unittest.mock import MagicMock, call, patch

import pytest
import yaml
from click.testing import CliRunner
from pydantic import SecretStr

from streamt.cli import main
from streamt.cli.commands.state_cmd import (
    _recovery_service_and_runtime,
    _RecoveryRuntime,
    _StrictRecoveryKafkaDeployer,
)
from streamt.compiler import Compiler
from streamt.compiler.gateway_artifact import parse_compiled_gateway_rule_artifact
from streamt.compiler.manifest import (
    ArtifactOwnership,
    GatewayRuleArtifact,
    Manifest,
    TopicArtifact,
)
from streamt.core.deployment_state import local_deployment_state_config
from streamt.core.models import ProjectInfo, StreamtProject
from streamt.core.parser import ProjectParser
from streamt.core.runtime import (
    ConduktorConfig,
    ConnectClusterConfig,
    ConnectConfig,
    GatewayConfig,
    KafkaConfig,
    RuntimeConfig,
)
from streamt.deployer.connect import (
    ConnectClusterBinding,
    ConnectDeployer,
    ManagedConnectorObservation,
    managed_connector_absence_fingerprint,
)
from streamt.deployer.gateway import (
    GatewayBackendBinding,
    GatewayDeployer,
    ManagedGatewayRuleObservation,
    ManagedGatewaySnapshot,
    build_desired_gateway_rule,
)
from streamt.deployer.kafka import KafkaDeployer, TopicState
from streamt.deployer.recovery import (
    RecoveryResolution,
    RecoverySnapshotEvidence,
    RecoveryTargetEvidence,
)
from streamt.deployer.recovery_plan import RecoveryPlanFile
from streamt.deployer.recovery_service import (
    RecoveryLiveObservation,
    RecoveryProjectContext,
    RecoveryServiceError,
)
from streamt.deployer.state import (
    LocalState,
    ManagedResourceRecord,
    artifact_checksum,
    resource_id,
)
from streamt.deployer.state_backend import (
    ConnectorActionEvidence,
    ConnectorActionSurfaceEvidence,
    ControlObservation,
    DeploymentStateService,
    GatewayActionEvidence,
    GatewayActionSurfaceEvidence,
    OperationAction,
    OperationControlState,
    OperationIntent,
    OperationKind,
    OperationProgress,
    OperationSnapshot,
    StateAddress,
    StateBackendConflictError,
    StateBackendInvalidStateError,
    StateBackendLockLostError,
    StateBackendLockTimeoutError,
    StateBackendRecoveryRequiredError,
    StateBackendReleaseAfterCommitError,
    StateBackendUnavailableError,
    StateBackendUnknownCommitError,
    StateObservation,
    StateRevision,
    StateStoreIdentity,
    local_recovery_history_path,
    make_deployment_state_service,
    operation_timestamp,
    state_checksum,
)
from streamt.output import OutputFormatter

BLOCKED_OPERATION_ID = "00000000-0000-4000-8000-000000000201"
TARGET_FINGERPRINT = "sha256:" + "c" * 64
ENVIRONMENT_FINGERPRINT = "sha256:" + "a" * 64
MANIFEST_CHECKSUM = "sha256:" + "b" * 64
GATEWAY_ENDPOINT = "https://gateway.example.test/admin"
GATEWAY_VCLUSTER = "recovery-vcluster"
GATEWAY_BINDING = GatewayBackendBinding.from_endpoint(
    GATEWAY_ENDPOINT,
    virtual_cluster=GATEWAY_VCLUSTER,
)
GATEWAY_CONFIG_SECRET = "gateway-config-secret-9281"
CONNECT_ENDPOINT = "https://connect.example.test"
CONNECT_BINDING = ConnectClusterBinding.from_endpoint("primary", CONNECT_ENDPOINT)
CONNECTOR_RESOURCE = resource_id(
    "recovery-test",
    "default",
    "connector",
    "orders_connector",
)
CONNECTOR_NAME = "orders-sink"
CONNECTOR_PRIOR_CHECKSUM = "sha256:" + "d" * 64
CONNECTOR_CURRENT_FINGERPRINT = "sha256:" + "e" * 64


def _write_project(path: Path) -> None:
    (path / "stream_project.yml").write_text(
        yaml.safe_dump(
            {
                "apiVersion": "streamt.dev/v1alpha1",
                "project": {"name": "recovery-test"},
                "runtime": {"kafka": {"bootstrap_servers": "unreachable.invalid:9092"}},
            }
        ),
        encoding="utf-8",
    )


def _write_gateway_recovery_project(path: Path, *, include_rule: bool) -> None:
    config: dict[str, object] = {
        "apiVersion": "streamt.dev/v1alpha1",
        "project": {"name": "recovery-test"},
        "runtime": {
            "kafka": {"bootstrap_servers": "broker.invalid:9092"},
            "conduktor": {
                "gateway": {
                    "admin_url": GATEWAY_ENDPOINT,
                    "password": GATEWAY_CONFIG_SECRET,
                    "virtual_cluster": GATEWAY_VCLUSTER,
                }
            },
        },
        "sources": [{"name": "orders_source", "topic": "orders.v1"}],
    }
    if include_rule:
        config["models"] = [
            {
                "name": "orders_rule",
                "materialized": "virtual_topic",
                "gateway": {"virtual_topic": {"name": "orders.public"}},
                "sql": 'SELECT * FROM {{ source("orders_source") }}',
            }
        ]
    (path / "stream_project.yml").write_text(
        yaml.safe_dump(config),
        encoding="utf-8",
    )


def _json(result: object) -> dict[str, object]:
    return json.loads(result.stdout)


def _target_id() -> str:
    return resource_id("recovery-test", "default", "topic", "orders")


def _gateway_project(
    *,
    endpoint: str = GATEWAY_ENDPOINT,
) -> StreamtProject:
    return StreamtProject(
        project=ProjectInfo(name="recovery-test"),
        runtime=RuntimeConfig(
            kafka=KafkaConfig(bootstrap_servers="broker.invalid:9092"),
            conduktor=ConduktorConfig(
                gateway=GatewayConfig(
                    admin_url=endpoint,
                    password=SecretStr(GATEWAY_CONFIG_SECRET),
                    virtual_cluster=GATEWAY_VCLUSTER,
                )
            ),
        ),
    )


def _connector_project(*, endpoint: str = CONNECT_ENDPOINT) -> StreamtProject:
    return StreamtProject(
        project=ProjectInfo(name="recovery-test"),
        runtime=RuntimeConfig(
            kafka=KafkaConfig(bootstrap_servers="broker.invalid:9092"),
            connect=ConnectConfig(
                default="primary",
                clusters={
                    "primary": ConnectClusterConfig(rest_url=endpoint),
                },
            ),
        ),
    )


def _connector_recovery_snapshot(
    *,
    binding: ConnectClusterBinding = CONNECT_BINDING,
) -> RecoverySnapshotEvidence:
    state = LocalState(
        project="recovery-test",
        environment="default",
        serial=4,
        resources={
            CONNECTOR_RESOURCE: ManagedResourceRecord(
                physical_name=CONNECTOR_NAME,
                ownership="managed",
                artifact_checksum=CONNECTOR_PRIOR_CHECKSUM,
                backend=binding.backend_identity,
            ),
        },
    )
    action = OperationAction(
        index=0,
        resource_id=CONNECTOR_RESOURCE,
        action="delete",
        connector_evidence=ConnectorActionEvidence(
            version=1,
            backend_identity=binding.backend_identity,
            connector_name=CONNECTOR_NAME,
            prior_artifact_checksum=CONNECTOR_PRIOR_CHECKSUM,
            current=ConnectorActionSurfaceEvidence(
                exists=True,
                fingerprint=CONNECTOR_CURRENT_FINGERPRINT,
            ),
            desired=ConnectorActionSurfaceEvidence(
                exists=False,
                fingerprint=managed_connector_absence_fingerprint(
                    binding.backend_identity,
                    CONNECTOR_NAME,
                ),
            ),
        ),
    )
    address = StateAddress(
        namespace="default",
        project="recovery-test",
        environment="default",
    )
    intent = OperationIntent(
        operation_id=BLOCKED_OPERATION_ID,
        kind="apply",
        started_at="2026-09-02T12:00:00Z",
        actor="prior-runner",
        prior_state_serial=state.serial,
        prior_state_checksum=state_checksum(state),
        reviewed_plan_checksum=MANIFEST_CHECKSUM,
        actions=(action,),
    )
    control = OperationControlState(
        address=address,
        status="in_progress",
        intent=intent,
        progress=(
            OperationProgress(
                operation_id=BLOCKED_OPERATION_ID,
                action_index=0,
                resource_id=action.resource_id,
                action=action.action,
                status="started",
                succeeded=None,
                recorded_at="2026-09-02T12:01:00Z",
            ),
        ),
        control_version=3,
    )
    return RecoverySnapshotEvidence.from_operation_snapshot(
        OperationSnapshot(
            state=StateObservation(
                store=StateStoreIdentity(
                    backend="postgres",
                    store_id="00000000-0000-4000-8000-000000000203",
                ),
                address=address,
                state=state,
                revision=StateRevision("state-revision"),
            ),
            control=ControlObservation(
                control=control,
                revision=StateRevision("control-revision"),
            ),
        )
    )


def _gateway_rule(
    *,
    rule_name: str = "orders_rule",
    alias_name: str = "orders.public",
    physical_name: str = "orders.v1",
    owner_name: str = "orders_owner",
    where: str | None = None,
    ownership_mode: str = "managed",
) -> GatewayRuleArtifact:
    interceptors: list[dict[str, object]] = []
    if where is not None:
        interceptors.append({"type": "filter", "config": {"where": where}})
    return GatewayRuleArtifact(
        name=rule_name,
        virtual_topic=alias_name,
        physical_topic=physical_name,
        interceptors=interceptors,
        ownership=ArtifactOwnership(
            project="recovery-test",
            owner_type="model",
            owner_name=owner_name,
            mode=ownership_mode,
        ),
    )


def _absent_gateway(
    present: ManagedGatewayRuleObservation,
) -> ManagedGatewayRuleObservation:
    return ManagedGatewayRuleObservation(
        binding=present.binding,
        logical_name=present.logical_name,
        alias_name=present.alias_name,
        exists=False,
    )


def _gateway_operation_action(
    *,
    index: int,
    owner_name: str,
    action: str,
    current: ManagedGatewayRuleObservation,
    desired: ManagedGatewayRuleObservation,
) -> OperationAction:
    return OperationAction(
        index=index,
        resource_id=resource_id(
            "recovery-test",
            "default",
            "gateway_rule",
            owner_name,
        ),
        action=action,
        gateway_evidence=GatewayActionEvidence(
            version=1,
            backend_identity=current.binding.backend_identity,
            rule_name=current.logical_name,
            alias_name=current.alias_name,
            current=GatewayActionSurfaceEvidence(
                exists=current.exists,
                fingerprint=current.fingerprint,
                managed_interceptor_count=len(current.interceptors),
            ),
            desired=GatewayActionSurfaceEvidence(
                exists=desired.exists,
                fingerprint=desired.fingerprint,
                managed_interceptor_count=len(desired.interceptors),
            ),
        ),
    )


def _gateway_recovery_snapshot(
    state: LocalState,
    actions: tuple[OperationAction, ...],
    *,
    address: StateAddress | None = None,
    control_version: int = 2,
    intent_kind: OperationKind = "apply",
) -> RecoverySnapshotEvidence:
    effective_address = address or StateAddress(
        namespace="default",
        project="recovery-test",
        environment="default",
    )
    intent = OperationIntent(
        operation_id=BLOCKED_OPERATION_ID,
        kind=intent_kind,
        started_at="2026-09-02T12:00:00Z",
        actor="prior-runner",
        prior_state_serial=state.serial,
        prior_state_checksum=state_checksum(state),
        reviewed_plan_checksum=None,
        actions=actions,
    )
    control = OperationControlState(
        address=effective_address,
        status="in_progress",
        intent=intent,
        control_version=control_version,
    )
    return RecoverySnapshotEvidence.from_operation_snapshot(
        OperationSnapshot(
            state=StateObservation(
                store=StateStoreIdentity(
                    backend="local",
                    store_id="00000000-0000-4000-8000-000000000202",
                ),
                address=effective_address,
                state=state,
                revision=StateRevision("state-revision"),
            ),
            control=ControlObservation(
                control=control,
                revision=StateRevision("control-revision"),
            ),
        )
    )


def _gateway_prior_record(
    artifact: GatewayRuleArtifact,
    *,
    binding: GatewayBackendBinding = GATEWAY_BINDING,
) -> ManagedResourceRecord:
    return ManagedResourceRecord(
        physical_name=artifact.virtual_topic,
        ownership="managed",
        artifact_checksum=artifact_checksum(artifact.to_dict()),
        backend=binding.backend_identity,
    )


def _observe_gateway_runtime(
    *,
    manifest: Manifest,
    snapshot: RecoverySnapshotEvidence,
    observations: dict[tuple[str, str], ManagedGatewayRuleObservation],
    project: StreamtProject | None = None,
    resolution: RecoveryResolution = "observed",
) -> tuple[RecoveryLiveObservation, MagicMock, MagicMock]:
    effective_project = project or _gateway_project()
    conduktor = effective_project.runtime.conduktor
    assert conduktor is not None
    gateway_config = conduktor.gateway
    assert gateway_config is not None
    assert gateway_config.admin_url is not None
    gateway = MagicMock(spec=GatewayDeployer)
    gateway.cluster_binding = GatewayBackendBinding.from_endpoint(
        gateway_config.admin_url,
        virtual_cluster=GATEWAY_VCLUSTER,
    )
    provider_snapshot = MagicMock(spec=ManagedGatewaySnapshot)
    provider_snapshot.binding = gateway.cluster_binding
    provider_snapshot.rule.side_effect = lambda rule_name, alias_name: observations[
        (rule_name, alias_name)
    ]
    gateway.observe_managed_gateway_snapshot.return_value = provider_snapshot
    kafka = MagicMock(spec=KafkaDeployer)
    runtime = _RecoveryRuntime(Path("/not-read"), None)

    with (
        patch.object(
            _RecoveryRuntime,
            "_compile",
            return_value=(effective_project, manifest, "default"),
        ),
        patch("streamt.cli.helpers.make_sr_deployer", return_value=None),
        patch("streamt.cli.helpers.make_kafka_deployer", return_value=kafka),
        patch("streamt.cli.helpers.make_flink_deployer", return_value=None),
        patch("streamt.cli.helpers.make_connect_deployer", return_value=None),
        patch("streamt.cli.helpers.make_gateway_deployer", return_value=gateway),
    ):
        result = runtime.observe_recovery_targets(
            resolution=resolution,
            snapshot=snapshot,
        )
    return result, gateway, provider_snapshot


def _block_local_operation(path: Path, *, with_progress: bool) -> OperationAction:
    service = make_deployment_state_service(
        path,
        project="recovery-test",
        environment="default",
        config=local_deployment_state_config(),
    )
    action = OperationAction(0, _target_id(), "create")
    with service.operation() as operation:
        snapshot = operation.observe()
        intent = OperationIntent(
            operation_id=BLOCKED_OPERATION_ID,
            kind="apply",
            started_at=operation_timestamp(),
            actor="prior-runner",
            prior_state_serial=snapshot.state.state.serial,
            prior_state_checksum=state_checksum(snapshot.state.state),
            reviewed_plan_checksum=None,
            actions=(action,),
        )
        active = operation.begin_operation(snapshot, intent)
        if with_progress:
            operation.record_progress(
                active,
                OperationProgress(
                    operation_id=BLOCKED_OPERATION_ID,
                    action_index=0,
                    resource_id=action.resource_id,
                    action=action.action,
                    status="started",
                    succeeded=None,
                    recorded_at=operation_timestamp(),
                ),
            )
    return action


def _fake_context(self: _RecoveryRuntime) -> RecoveryProjectContext:
    return RecoveryProjectContext(
        environment_fingerprint=ENVIRONMENT_FINGERPRINT,
        manifest_checksum=MANIFEST_CHECKSUM,
    )


def _fake_observation(
    self: _RecoveryRuntime,
    *,
    resolution: RecoveryResolution,
    snapshot: RecoverySnapshotEvidence,
) -> RecoveryLiveObservation:
    action = snapshot.control.intent.actions[0]
    if resolution == "rolled_back":
        return RecoveryLiveObservation(
            targets=(
                RecoveryTargetEvidence(
                    action=action,
                    presence="absent",
                    accepted_as="prior",
                    fingerprint=TARGET_FINGERPRINT,
                ),
            ),
            candidate_state=None,
        )
    record = ManagedResourceRecord(
        physical_name="orders.v1",
        ownership="managed",
        artifact_checksum=artifact_checksum(
            {
                "name": "orders.v1",
                "partitions": 3,
                "replication_factor": 1,
                "config": {},
            }
        ),
        backend="direct-kafka",
    )
    candidate = LocalState(
        project=snapshot.state.project,
        environment=snapshot.state.environment,
        serial=snapshot.state.serial + 1,
        resources={action.resource_id: record},
    )
    return RecoveryLiveObservation(
        targets=(
            RecoveryTargetEvidence(
                action=action,
                presence="present",
                accepted_as="candidate",
                fingerprint=TARGET_FINGERPRINT,
            ),
        ),
        candidate_state=candidate,
    )


@pytest.mark.parametrize(
    "resolution",
    ["observed", "rolled_back", "abandoned_before_mutation"],
)
def test_local_commands_plan_and_execute_all_recovery_outcomes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    resolution: RecoveryResolution,
) -> None:
    _write_project(tmp_path)
    _block_local_operation(
        tmp_path,
        with_progress=resolution != "abandoned_before_mutation",
    )
    monkeypatch.setattr(_RecoveryRuntime, "read_recovery_context", _fake_context)
    monkeypatch.setattr(_RecoveryRuntime, "observe_recovery_targets", _fake_observation)
    plan_path = tmp_path / f"{resolution}.recovery.json"

    planned = CliRunner().invoke(
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
            str(plan_path),
        ],
    )

    assert planned.exit_code == 0, planned.output
    planned_data = _json(planned)["data"]
    assert planned_data["plan_file"] == str(plan_path.resolve())
    assert planned_data["blocked_operation_id"] == BLOCKED_OPERATION_ID
    assert planned_data["resolution"] == resolution
    assert planned_data["evidence_checksum"].startswith("sha256:")
    assert stat.S_IMODE(plan_path.stat().st_mode) == 0o600

    executed = CliRunner().invoke(
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
            BLOCKED_OPERATION_ID,
            "--confirm-resolution",
            resolution,
            "--confirm-evidence-checksum",
            cast(str, planned_data["evidence_checksum"]),
        ],
    )

    assert executed.exit_code == 0, executed.output
    executed_data = _json(executed)["data"]
    assert executed_data["store"]["backend"] == "local"
    assert executed_data["address"] == "streamt-state://local/recovery-test/default"
    assert executed_data["control_status"] == "clear"
    assert executed_data["state_changed"] is (resolution == "observed")
    assert executed_data["state_serial"] == (1 if resolution == "observed" else 0)
    assert executed_data["state_checksum"].startswith("sha256:")


def test_recovery_plan_refuses_overwrite_and_symlink_destinations(
    tmp_path: Path,
) -> None:
    _write_project(tmp_path)
    _block_local_operation(tmp_path, with_progress=False)
    plan_path = tmp_path / "reviewed.json"
    command = [
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
    ]

    first = CliRunner().invoke(main, command)
    second = CliRunner().invoke(main, command)

    assert first.exit_code == 0, first.output
    assert second.exit_code == 1
    assert _json(second)["errors"][0]["code"] == "E408_PLAN_FILE_INVALID"

    symlink_path = tmp_path / "linked.json"
    symlink_path.symlink_to(plan_path)
    linked = CliRunner().invoke(main, [*command[:-1], str(symlink_path)])
    assert linked.exit_code == 1
    assert _json(linked)["errors"][0]["code"] == "E408_PLAN_FILE_INVALID"


def test_recovery_plan_requires_an_unfinished_operation(tmp_path: Path) -> None:
    _write_project(tmp_path)

    result = CliRunner().invoke(
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
            str(tmp_path / "unused.json"),
        ],
    )

    assert result.exit_code == 1
    payload = _json(result)
    assert payload["errors"][0]["code"] == "E419_STATE_RECOVERY_REQUIRED"
    assert "Traceback" not in result.output


def test_recover_rejects_malformed_confirmations_before_reading_plan(
    tmp_path: Path,
) -> None:
    _write_project(tmp_path)

    result = CliRunner().invoke(
        main,
        [
            "-o",
            "json",
            "state",
            "recover",
            "-p",
            str(tmp_path),
            "--plan",
            str(tmp_path / "missing-secret-plan.json"),
            "--confirm-operation-id",
            "not-a-uuid",
            "--confirm-resolution",
            "observed",
            "--confirm-evidence-checksum",
            "sha256:not-a-checksum",
        ],
    )

    assert result.exit_code == 1
    assert _json(result)["errors"][0]["code"] == "E408_PLAN_FILE_INVALID"
    assert "missing-secret-plan" not in result.output
    assert "Traceback" not in result.output


def test_tampered_recovery_plan_is_rejected_without_echoing_content(
    tmp_path: Path,
) -> None:
    _write_project(tmp_path)
    _block_local_operation(tmp_path, with_progress=False)
    plan_path = tmp_path / "reviewed.json"
    planned = CliRunner().invoke(
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
    data = _json(planned)["data"]
    plan_path.write_text(
        plan_path.read_text(encoding="utf-8").replace(
            '"streamt_version":',
            '"password": "do-not-print", "streamt_version":',
        ),
        encoding="utf-8",
    )

    result = CliRunner().invoke(
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
            BLOCKED_OPERATION_ID,
            "--confirm-resolution",
            "abandoned_before_mutation",
            "--confirm-evidence-checksum",
            cast(str, data["evidence_checksum"]),
        ],
    )

    assert result.exit_code == 1
    assert _json(result)["errors"][0]["code"] == "E408_PLAN_FILE_INVALID"
    assert "do-not-print" not in result.output


def test_recovery_backend_failures_are_stably_mapped_and_redacted(
    tmp_path: Path,
) -> None:
    _write_project(tmp_path)
    _block_local_operation(tmp_path, with_progress=False)
    secret = "postgresql://writer:writer-secret@db.internal/state"

    with patch(
        "streamt.deployer.recovery_service.RecoveryService.create_plan",
        side_effect=StateBackendUnavailableError(
            f"provider unavailable password=do-not-print dsn={secret}"
        ),
    ):
        result = CliRunner().invoke(
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
                str(tmp_path / "unused.json"),
            ],
        )

    assert result.exit_code == 1
    assert _json(result)["errors"][0]["code"] == "E420_STATE_BACKEND_UNAVAILABLE"
    assert "do-not-print" not in result.output
    assert "writer-secret" not in result.output
    assert secret not in result.output


@pytest.mark.parametrize(
    ("error", "code"),
    [
        (RecoveryServiceError("live evidence changed"), "E409_PLAN_STALE"),
        (
            RecoveryServiceError("operation confirmation changed"),
            "E408_PLAN_FILE_INVALID",
        ),
        (
            StateBackendRecoveryRequiredError("recovery required"),
            "E419_STATE_RECOVERY_REQUIRED",
        ),
        (StateBackendLockTimeoutError("timeout"), "E422_STATE_LOCK_TIMEOUT"),
        (StateBackendLockLostError("lost"), "E423_STATE_LOCK_LOST"),
        (StateBackendConflictError("conflict"), "E424_STATE_CONFLICT"),
        (StateBackendUnknownCommitError("unknown"), "E425_STATE_UNKNOWN_OUTCOME"),
        (
            StateBackendReleaseAfterCommitError("release"),
            "E426_STATE_RELEASE_FAILED_AFTER_COMMIT",
        ),
        (StateBackendInvalidStateError("invalid"), "E411_STATE_INVALID"),
    ],
)
def test_recovery_command_maps_stable_failure_codes(
    tmp_path: Path,
    error: Exception,
    code: str,
) -> None:
    _write_project(tmp_path)
    _block_local_operation(tmp_path, with_progress=False)

    with patch(
        "streamt.deployer.recovery_service.RecoveryService.create_plan",
        side_effect=error,
    ):
        result = CliRunner().invoke(
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
                str(tmp_path / "unused.json"),
            ],
        )

    assert result.exit_code == 1
    assert _json(result)["errors"][0]["code"] == code
    assert "Traceback" not in result.output


def test_abandoned_planning_never_constructs_live_deployers(
    tmp_path: Path,
) -> None:
    _write_project(tmp_path)
    _block_local_operation(tmp_path, with_progress=False)

    with (
        patch(
            "streamt.cli.helpers.make_kafka_deployer",
            side_effect=AssertionError("Kafka deployer constructed"),
        ),
        patch(
            "streamt.cli.helpers.make_sr_deployer",
            side_effect=AssertionError("Schema Registry deployer constructed"),
        ),
        patch(
            "streamt.cli.helpers.make_gateway_deployer",
            side_effect=AssertionError("Gateway deployer constructed"),
        ),
    ):
        result = CliRunner().invoke(
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
                str(tmp_path / "abandoned.json"),
            ],
        )

    assert result.exit_code == 0, result.output


@pytest.mark.parametrize("gateway_action", ["create", "update", "delete"])
def test_local_gateway_recovery_cli_round_trip_and_completed_retry(
    tmp_path: Path,
    gateway_action: str,
) -> None:
    _write_gateway_recovery_project(
        tmp_path,
        include_rule=gateway_action != "delete",
    )
    project = ProjectParser(tmp_path).parse()
    manifest = Compiler(project).compile(dry_run=True)
    compiled_rules = manifest.artifacts.get("gateway_rules", [])
    expected_new = _gateway_rule(
        rule_name="orders_rule",
        alias_name="orders.public",
        physical_name="orders.v1",
        owner_name="orders_rule",
    )
    if gateway_action == "delete":
        assert compiled_rules == []
    else:
        assert len(compiled_rules) == 1
        assert parse_compiled_gateway_rule_artifact(compiled_rules[0]) == expected_new

    old = _gateway_rule(
        rule_name="orders_rule",
        alias_name="orders.public",
        physical_name="orders.v0",
        owner_name="orders_rule",
    )
    old_live = build_desired_gateway_rule(old, GATEWAY_BINDING)
    new_live = build_desired_gateway_rule(expected_new, GATEWAY_BINDING)
    if gateway_action == "create":
        current = _absent_gateway(new_live)
        desired = new_live
        provider_observation = new_live
    elif gateway_action == "update":
        current = old_live
        desired = new_live
        provider_observation = new_live
    else:
        current = old_live
        desired = _absent_gateway(old_live)
        provider_observation = desired

    action = _gateway_operation_action(
        index=0,
        owner_name="orders_rule",
        action=gateway_action,
        current=current,
        desired=desired,
    )
    target_id = action.resource_id
    prior_resources = (
        {}
        if gateway_action == "create"
        else {target_id: _gateway_prior_record(old)}
    )
    service = make_deployment_state_service(
        tmp_path,
        project="recovery-test",
        environment="default",
        config=local_deployment_state_config(),
    )
    with service.operation() as operation:
        initial = operation.observe()
        if prior_resources:
            operation.compare_and_swap(
                initial.state,
                LocalState(
                    project="recovery-test",
                    environment="default",
                    serial=1,
                    resources=prior_resources,
                ),
            )
        snapshot = operation.observe()
        prior_state = snapshot.state.state
        intent = OperationIntent(
            operation_id=BLOCKED_OPERATION_ID,
            kind="apply",
            started_at=operation_timestamp(),
            actor="prior-runner",
            prior_state_serial=prior_state.serial,
            prior_state_checksum=state_checksum(prior_state),
            reviewed_plan_checksum=None,
            actions=(action,),
        )
        active = operation.begin_operation(snapshot, intent)
        operation.record_progress(
            active,
            OperationProgress(
                operation_id=BLOCKED_OPERATION_ID,
                action_index=0,
                resource_id=target_id,
                action=gateway_action,
                status="started",
                succeeded=None,
                recorded_at=operation_timestamp(),
            ),
        )

    gateway = MagicMock(spec=GatewayDeployer)
    gateway.cluster_binding = GATEWAY_BINDING
    provider_snapshot = MagicMock(spec=ManagedGatewaySnapshot)
    provider_snapshot.binding = GATEWAY_BINDING

    def observe_rule(
        rule_name: str,
        alias_name: str,
    ) -> ManagedGatewayRuleObservation:
        assert (rule_name, alias_name) == ("orders_rule", "orders.public")
        return provider_observation

    provider_snapshot.rule.side_effect = observe_rule
    gateway.observe_managed_gateway_snapshot.return_value = provider_snapshot
    kafka = MagicMock(spec=KafkaDeployer)
    plan_path = tmp_path / f"gateway-{gateway_action}.recovery.json"
    runner = CliRunner()

    with (
        patch(
            "streamt.cli.helpers.make_kafka_deployer",
            return_value=kafka,
        ) as make_kafka,
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
        planned_data = cast(dict[str, object], _json(planned)["data"])
        reviewed = RecoveryPlanFile.load(plan_path)
        assert stat.S_IMODE(plan_path.stat().st_mode) == 0o600
        assert reviewed.evidence_checksum == planned_data["evidence_checksum"]
        assert reviewed.snapshot.state == prior_state
        assert reviewed.resolution == "observed"
        assert len(reviewed.targets) == 1
        assert reviewed.targets[0].action == action
        assert reviewed.targets[0].presence == (
            "absent" if gateway_action == "delete" else "present"
        )
        assert reviewed.targets[0].accepted_as == "candidate"
        assert reviewed.targets[0].fingerprint == provider_observation.fingerprint
        assert reviewed.candidate_state is not None
        assert reviewed.candidate_state.serial == prior_state.serial + 1
        if gateway_action == "delete":
            assert target_id not in reviewed.candidate_state.resources
        else:
            candidate_record = reviewed.candidate_state.resources[target_id]
            assert candidate_record == ManagedResourceRecord(
                physical_name="orders.public",
                ownership="managed",
                artifact_checksum=artifact_checksum(expected_new.to_dict()),
                backend=GATEWAY_BINDING.backend_identity,
            )
        assert GATEWAY_CONFIG_SECRET not in plan_path.read_text(encoding="utf-8")

        executed = runner.invoke(
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
                BLOCKED_OPERATION_ID,
                "--confirm-resolution",
                "observed",
                "--confirm-evidence-checksum",
                reviewed.evidence_checksum,
            ],
        )

    assert executed.exit_code == 0, executed.output
    executed_data = cast(dict[str, object], _json(executed)["data"])
    assert executed_data["state_changed"] is True
    assert executed_data["control_status"] == "clear"
    assert executed_data["state_checksum"] == state_checksum(reviewed.candidate_state)
    assert make_gateway.call_count == 2
    assert make_kafka.call_count == 2
    assert gateway.observe_managed_gateway_snapshot.call_count == 2
    assert provider_snapshot.rule.call_args_list == [
        call("orders_rule", "orders.public"),
        call("orders_rule", "orders.public"),
    ]

    assert service.read().state == reviewed.candidate_state
    assert service.read_control().control == OperationControlState.clear(service.address)
    history_path = local_recovery_history_path(tmp_path, environment="default")
    history_before_retry = cast(
        dict[str, object],
        json.loads(history_path.read_text(encoding="utf-8")),
    )
    events = cast(list[dict[str, object]], history_before_retry["events"])
    assert history_before_retry["address"] == service.address.uri
    assert [event["kind"] for event in events] == [
        "recovery_intent",
        "recovery_resolution",
    ]
    assert events[0]["record"] == events[1]["record"]
    resolution_record = cast(dict[str, object], events[0]["record"])
    assert resolution_record["blocked_operation_id"] == BLOCKED_OPERATION_ID
    assert resolution_record["recovery_operation_id"] == reviewed.recovery_operation_id
    assert resolution_record["resolution"] == "observed"
    assert resolution_record["evidence_checksum"] == reviewed.evidence_checksum
    assert resolution_record["state_changed"] is True
    assert resolution_record["result_state_checksum"] == state_checksum(
        reviewed.candidate_state
    )
    assert events[0]["previous_checksum"] is None
    assert events[1]["previous_checksum"] == events[0]["checksum"]

    with (
        patch(
            "streamt.cli.helpers.make_kafka_deployer",
            side_effect=AssertionError("Kafka provider was read during completed retry"),
        ) as retry_kafka,
        patch(
            "streamt.cli.helpers.make_gateway_deployer",
            side_effect=AssertionError("Gateway provider was read during completed retry"),
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
                BLOCKED_OPERATION_ID,
                "--confirm-resolution",
                "observed",
                "--confirm-evidence-checksum",
                reviewed.evidence_checksum,
            ],
        )

    assert retried.exit_code == 0, retried.output
    retry_gateway.assert_not_called()
    retry_kafka.assert_not_called()
    assert json.loads(history_path.read_text(encoding="utf-8")) == history_before_retry
    assert service.read().state == reviewed.candidate_state
    assert service.read_control().control == OperationControlState.clear(service.address)


def test_recovery_runtime_observes_removed_gateway_delete_without_manifest_rule() -> None:
    removed = _gateway_rule()
    present = build_desired_gateway_rule(removed, GATEWAY_BINDING)
    absent = _absent_gateway(present)
    target_id = resource_id(
        "recovery-test",
        "default",
        "gateway_rule",
        "orders_owner",
    )
    prior = LocalState(
        project="recovery-test",
        environment="default",
        serial=7,
        resources={target_id: _gateway_prior_record(removed)},
    )
    action = _gateway_operation_action(
        index=0,
        owner_name="orders_owner",
        action="delete",
        current=present,
        desired=absent,
    )

    observed, gateway, provider_snapshot = _observe_gateway_runtime(
        manifest=Manifest(version="1", project_name="recovery-test"),
        snapshot=_gateway_recovery_snapshot(prior, (action,)),
        observations={("orders_rule", "orders.public"): absent},
    )

    gateway.observe_managed_gateway_snapshot.assert_called_once_with()
    provider_snapshot.rule.assert_called_once_with("orders_rule", "orders.public")
    assert observed.targets[0].presence == "absent"
    assert observed.targets[0].accepted_as == "candidate"
    assert observed.candidate_state is not None
    assert target_id not in observed.candidate_state.resources


def test_recovery_runtime_uses_one_gateway_snapshot_for_desired_and_removed_rules() -> None:
    old_orders = _gateway_rule(physical_name="orders.v0")
    new_orders = _gateway_rule(
        physical_name="orders.v1",
        where=f"tenant_token = '{GATEWAY_CONFIG_SECRET}'",
    )
    removed_payments = _gateway_rule(
        rule_name="payments_rule",
        alias_name="payments.public",
        physical_name="payments.v1",
        owner_name="payments_owner",
    )
    old_orders_live = build_desired_gateway_rule(old_orders, GATEWAY_BINDING)
    new_orders_live = build_desired_gateway_rule(new_orders, GATEWAY_BINDING)
    payments_live = build_desired_gateway_rule(removed_payments, GATEWAY_BINDING)
    payments_absent = _absent_gateway(payments_live)
    orders_id = resource_id(
        "recovery-test",
        "default",
        "gateway_rule",
        "orders_owner",
    )
    payments_id = resource_id(
        "recovery-test",
        "default",
        "gateway_rule",
        "payments_owner",
    )
    prior = LocalState(
        project="recovery-test",
        environment="default",
        serial=11,
        resources={
            orders_id: _gateway_prior_record(old_orders),
            payments_id: _gateway_prior_record(removed_payments),
        },
    )
    actions = (
        _gateway_operation_action(
            index=0,
            owner_name="orders_owner",
            action="update",
            current=old_orders_live,
            desired=new_orders_live,
        ),
        _gateway_operation_action(
            index=1,
            owner_name="payments_owner",
            action="delete",
            current=payments_live,
            desired=payments_absent,
        ),
    )

    observed, gateway, provider_snapshot = _observe_gateway_runtime(
        manifest=Manifest(
            version="1",
            project_name="recovery-test",
            artifacts={"gateway_rules": [new_orders.to_dict()]},
        ),
        snapshot=_gateway_recovery_snapshot(prior, actions),
        observations={
            ("orders_rule", "orders.public"): new_orders_live,
            ("payments_rule", "payments.public"): payments_absent,
        },
    )

    gateway.observe_managed_gateway_snapshot.assert_called_once_with()
    provider_snapshot.rule.assert_has_calls(
        [
            call("orders_rule", "orders.public"),
            call("payments_rule", "payments.public"),
        ]
    )
    assert [target.accepted_as for target in observed.targets] == [
        "candidate",
        "candidate",
    ]
    assert observed.candidate_state is not None
    assert set(observed.candidate_state.resources) == {orders_id}
    assert GATEWAY_CONFIG_SECRET not in repr(observed)


def test_recovery_runtime_routes_gateway_adopt_through_one_shared_snapshot() -> None:
    artifact = _gateway_rule(
        rule_name="provider_orders_rule",
        alias_name="orders.public",
        physical_name="orders.v2",
        owner_name="orders_model_owner",
        ownership_mode="adopted",
    )
    desired = build_desired_gateway_rule(artifact, GATEWAY_BINDING)
    current = ManagedGatewayRuleObservation(
        binding=GATEWAY_BINDING,
        logical_name=artifact.name,
        alias_name=artifact.virtual_topic,
        exists=True,
        physical_name="orders.v1",
        physical_cluster="main",
    )
    action = _gateway_operation_action(
        index=0,
        owner_name="orders_model_owner",
        action="adopt",
        current=current,
        desired=desired,
    )
    state = LocalState(project="recovery-test", environment="default")

    observed, gateway, provider_snapshot = _observe_gateway_runtime(
        manifest=Manifest(
            version="1",
            project_name="recovery-test",
            artifacts={"gateway_rules": [artifact.to_dict()]},
        ),
        snapshot=_gateway_recovery_snapshot(
            state,
            (action,),
            intent_kind="adopt",
        ),
        observations={(artifact.name, artifact.virtual_topic): current},
    )

    gateway.observe_managed_gateway_snapshot.assert_called_once_with()
    provider_snapshot.rule.assert_called_once_with(
        artifact.name,
        artifact.virtual_topic,
    )
    assert observed.targets[0].accepted_as == "candidate"
    assert observed.targets[0].fingerprint == current.fingerprint
    assert observed.candidate_state is not None
    assert observed.candidate_state.resources[action.resource_id] == ManagedResourceRecord(
        physical_name=artifact.virtual_topic,
        ownership="adopted",
        artifact_checksum=artifact_checksum(artifact.to_dict()),
        backend=GATEWAY_BINDING.backend_identity,
    )


@pytest.mark.parametrize("resolution", ["observed", "rolled_back"])
def test_recovery_runtime_rejects_legacy_gateway_before_deployer_construction(
    resolution: RecoveryResolution,
) -> None:
    state = LocalState(project="recovery-test", environment="default")
    legacy_action = OperationAction(
        index=0,
        resource_id=resource_id(
            "recovery-test",
            "default",
            "gateway_rule",
            "orders_owner",
        ),
        action="delete",
    )
    snapshot = _gateway_recovery_snapshot(
        state,
        (legacy_action,),
        control_version=1,
    )

    with (
        patch.object(
            _RecoveryRuntime,
            "_compile",
            return_value=(
                _gateway_project(),
                Manifest(version="1", project_name="recovery-test"),
                "default",
            ),
        ),
        patch(
            "streamt.cli.helpers.make_gateway_deployer",
            side_effect=AssertionError("Gateway deployer constructed"),
        ) as make_gateway,
        pytest.raises(ValueError, match="Gateway"),
    ):
        _RecoveryRuntime(Path("/not-read"), None).observe_recovery_targets(
            resolution=resolution,
            snapshot=snapshot,
        )

    make_gateway.assert_not_called()


@pytest.mark.parametrize("resolution", ["observed", "rolled_back"])
def test_recovery_runtime_rejects_changed_connector_binding_before_any_deployer(
    resolution: RecoveryResolution,
) -> None:
    changed_endpoint = "https://changed-connect.example.test"
    snapshot = _connector_recovery_snapshot()

    with (
        patch.object(
            _RecoveryRuntime,
            "_compile",
            return_value=(
                _connector_project(endpoint=changed_endpoint),
                Manifest(version="1", project_name="recovery-test"),
                "default",
            ),
        ),
        patch(
            "streamt.cli.helpers.make_sr_deployer",
            side_effect=AssertionError("provider deployer constructed"),
        ) as make_schema_registry,
        patch(
            "streamt.cli.helpers.make_connect_deployer",
            side_effect=AssertionError("Connect deployer constructed"),
        ) as make_connect,
        pytest.raises(ValueError) as failure,
    ):
        _RecoveryRuntime(Path("/not-read"), None).observe_recovery_targets(
            resolution=resolution,
            snapshot=snapshot,
        )

    make_schema_registry.assert_not_called()
    make_connect.assert_not_called()
    assert CONNECT_ENDPOINT not in str(failure.value)
    assert changed_endpoint not in str(failure.value)


def test_recovery_runtime_observes_connector_delete_without_manifest_tombstone() -> None:
    snapshot = _connector_recovery_snapshot()
    connect = MagicMock(spec=ConnectDeployer)
    connect.require_cluster_binding.return_value = CONNECT_BINDING
    connect.observe_managed_connector.return_value = ManagedConnectorObservation(
        binding=CONNECT_BINDING,
        name=CONNECTOR_NAME,
        exists=False,
    )
    kafka = MagicMock(spec=KafkaDeployer)

    with (
        patch.object(
            _RecoveryRuntime,
            "_compile",
            return_value=(
                _connector_project(),
                Manifest(version="1", project_name="recovery-test"),
                "default",
            ),
        ),
        patch("streamt.cli.helpers.make_sr_deployer", return_value=None),
        patch("streamt.cli.helpers.make_kafka_deployer", return_value=kafka),
        patch("streamt.cli.helpers.make_flink_deployer", return_value=None),
        patch("streamt.cli.helpers.make_connect_deployer", return_value=connect),
        patch("streamt.cli.helpers.make_gateway_deployer", return_value=None),
    ):
        observed = _RecoveryRuntime(
            Path("/not-read"),
            None,
        ).observe_recovery_targets(
            resolution="observed",
            snapshot=snapshot,
        )

    connect.observe_managed_connector.assert_called_once_with(CONNECTOR_NAME)
    assert observed.targets[0].accepted_as == "candidate"
    assert observed.targets[0].presence == "absent"
    assert observed.candidate_state == LocalState(
        project="recovery-test",
        environment="default",
        serial=5,
    )


def test_recovery_runtime_rejects_gateway_backend_evidence_before_live_reads() -> None:
    artifact = _gateway_rule()
    present = build_desired_gateway_rule(artifact, GATEWAY_BINDING)
    other_binding = GatewayBackendBinding.from_endpoint(
        "https://other-gateway.example.test/admin",
        virtual_cluster=GATEWAY_VCLUSTER,
    )
    other_present = build_desired_gateway_rule(artifact, other_binding)
    action = _gateway_operation_action(
        index=0,
        owner_name="orders_owner",
        action="delete",
        current=other_present,
        desired=_absent_gateway(other_present),
    )
    target_id = action.resource_id
    prior = LocalState(
        project="recovery-test",
        environment="default",
        resources={target_id: _gateway_prior_record(artifact)},
    )
    gateway = MagicMock(spec=GatewayDeployer)
    gateway.cluster_binding = GATEWAY_BINDING

    with (
        patch.object(
            _RecoveryRuntime,
            "_compile",
            return_value=(
                _gateway_project(),
                Manifest(version="1", project_name="recovery-test"),
                "default",
            ),
        ),
        patch(
            "streamt.cli.helpers.make_gateway_deployer",
            return_value=gateway,
        ) as make_gateway,
        pytest.raises(ValueError) as failure,
    ):
        _RecoveryRuntime(Path("/not-read"), None).observe_recovery_targets(
            resolution="observed",
            snapshot=_gateway_recovery_snapshot(prior, (action,)),
        )

    make_gateway.assert_called_once()
    gateway.observe_managed_gateway_snapshot.assert_not_called()
    error = str(failure.value)
    assert GATEWAY_ENDPOINT not in error
    assert GATEWAY_CONFIG_SECRET not in error
    assert present.physical_name is not None
    assert present.physical_name not in error


def test_recovery_runtime_rejects_gateway_alias_evidence_without_leaking_config() -> None:
    prior_artifact = _gateway_rule()
    evidence_artifact = _gateway_rule(
        alias_name="other.public",
        where=f"token = '{GATEWAY_CONFIG_SECRET}'",
    )
    evidence_present = build_desired_gateway_rule(evidence_artifact, GATEWAY_BINDING)
    action = _gateway_operation_action(
        index=0,
        owner_name="orders_owner",
        action="delete",
        current=evidence_present,
        desired=_absent_gateway(evidence_present),
    )
    prior = LocalState(
        project="recovery-test",
        environment="default",
        resources={action.resource_id: _gateway_prior_record(prior_artifact)},
    )
    gateway = MagicMock(spec=GatewayDeployer)
    gateway.cluster_binding = GATEWAY_BINDING

    with (
        patch.object(
            _RecoveryRuntime,
            "_compile",
            return_value=(
                _gateway_project(),
                Manifest(version="1", project_name="recovery-test"),
                "default",
            ),
        ),
        patch(
            "streamt.cli.helpers.make_gateway_deployer",
            return_value=gateway,
        ) as make_gateway,
        pytest.raises(ValueError) as failure,
    ):
        _RecoveryRuntime(Path("/not-read"), None).observe_recovery_targets(
            resolution="observed",
            snapshot=_gateway_recovery_snapshot(prior, (action,)),
        )

    make_gateway.assert_called_once()
    gateway.observe_managed_gateway_snapshot.assert_not_called()
    error = str(failure.value)
    assert GATEWAY_ENDPOINT not in error
    assert GATEWAY_CONFIG_SECRET not in error
    assert evidence_present.physical_name is not None
    assert evidence_present.physical_name not in error


def test_recovery_runtime_rejects_changed_state_address_before_live_reads() -> None:
    other_state = LocalState(project="other-project", environment="default")
    other_address = StateAddress(
        namespace="default",
        project="other-project",
        environment="default",
    )
    snapshot = _gateway_recovery_snapshot(
        other_state,
        (),
        address=other_address,
    )

    with (
        patch.object(
            _RecoveryRuntime,
            "_compile",
            return_value=(
                _gateway_project(),
                Manifest(version="1", project_name="recovery-test"),
                "default",
            ),
        ),
        patch(
            "streamt.cli.helpers.make_gateway_deployer",
            side_effect=AssertionError("Gateway deployer constructed"),
        ) as make_gateway,
        pytest.raises(ValueError, match="identity changed"),
    ):
        _RecoveryRuntime(Path("/not-read"), None).observe_recovery_targets(
            resolution="observed",
            snapshot=snapshot,
        )

    make_gateway.assert_not_called()


def test_recovery_runtime_forces_strict_topic_config_and_replans_action() -> None:
    artifact = TopicArtifact(
        name="orders.v1",
        partitions=3,
        replication_factor=1,
        config={"cleanup.policy": "compact"},
        ownership=ArtifactOwnership(
            project="recovery-test",
            owner_type="model",
            owner_name="orders",
            mode="managed",
        ),
    )
    delegate = cast(KafkaDeployer, MagicMock())
    delegate.get_topic_state.return_value = TopicState(
        name="orders.v1",
        exists=True,
        partitions=3,
        replication_factor=1,
        config={"cleanup.policy": "compact"},
    )
    strict = _StrictRecoveryKafkaDeployer(delegate)

    change = strict.plan_topic(artifact)

    delegate.get_topic_state.assert_called_once_with(
        "orders.v1",
        strict_config=True,
    )
    assert change.action == "none"
    assert change.current is delegate.get_topic_state.return_value


def test_recovery_runtime_fails_closed_on_partial_present_topic() -> None:
    artifact = TopicArtifact(
        name="orders.v1",
        partitions=3,
        replication_factor=1,
        config={},
    )
    delegate = cast(KafkaDeployer, MagicMock())
    delegate.get_topic_state.return_value = TopicState(
        name="orders.v1",
        exists=True,
        partitions=3,
        replication_factor=None,
        config={},
    )

    with pytest.raises(ValueError, match="observation is incomplete"):
        _StrictRecoveryKafkaDeployer(delegate).plan_topic(artifact)


def test_recovery_command_selects_recovery_only_postgres_factory(
    tmp_path: Path,
) -> None:
    (tmp_path / "stream_project.yml").write_text(
        yaml.safe_dump(
            {
                "apiVersion": "streamt.dev/v1alpha1",
                "project": {"name": "recovery-test"},
                "runtime": {"kafka": {"bootstrap_servers": "unreachable.invalid:9092"}},
                "deployment_state": {
                    "backend": "postgres",
                    "namespace": "platform",
                    "postgres": {
                        "dsn_env": "STREAMT_ADMIN_DSN",
                        "writer_dsn_env": "STREAMT_RECOVERY_WRITER_DSN",
                        "schema": "state_catalog",
                    },
                },
            }
        ),
        encoding="utf-8",
    )
    sentinel = cast(DeploymentStateService, object())
    fmt = OutputFormatter("text", quiet=True)

    with (
        patch(
            "streamt.cli.commands.state_cmd.make_recovery_state_service",
            return_value=sentinel,
        ) as recovery_factory,
        patch(
            "streamt.cli.commands.state_cmd.make_deployment_state_service",
            side_effect=AssertionError("ordinary PostgreSQL factory selected"),
        ),
    ):
        service, runtime = _recovery_service_and_runtime(tmp_path, None, fmt)

    assert service is sentinel
    assert runtime.project_path == tmp_path
    config = recovery_factory.call_args.kwargs["config"]
    assert config.backend == "postgres"
    assert config.postgres.writer_dsn_env == "STREAMT_RECOVERY_WRITER_DSN"
    assert recovery_factory.call_args.kwargs["project"] == "recovery-test"
    assert recovery_factory.call_args.kwargs["environment"] == "default"


def test_recovery_commands_expose_required_review_workflow_help() -> None:
    runner = CliRunner()

    planned = runner.invoke(main, ["state", "recovery-plan", "--help"])
    recovered = runner.invoke(main, ["state", "recover", "--help"])

    assert planned.exit_code == 0
    assert "--resolution [observed|rolled_back|abandoned_before_mutation]" in planned.output
    assert "--out FILE" in planned.output
    assert recovered.exit_code == 0
    assert "--confirm-operation-id" in recovered.output
    assert "--confirm-resolution" in recovered.output
    assert "--confirm-evidence-checksum" in recovered.output
