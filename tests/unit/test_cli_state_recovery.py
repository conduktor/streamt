"""CLI coverage for explicit reviewed deployment-state recovery."""

from __future__ import annotations

import json
import stat
from pathlib import Path
from typing import cast
from unittest.mock import MagicMock, patch

import pytest
import yaml
from click.testing import CliRunner

from streamt.cli import main
from streamt.cli.commands.state_cmd import (
    _recovery_service_and_runtime,
    _RecoveryRuntime,
    _StrictRecoveryKafkaDeployer,
)
from streamt.compiler.manifest import ArtifactOwnership, TopicArtifact
from streamt.core.deployment_state import local_deployment_state_config
from streamt.deployer.kafka import KafkaDeployer, TopicState
from streamt.deployer.recovery import (
    RecoveryResolution,
    RecoverySnapshotEvidence,
    RecoveryTargetEvidence,
)
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
    DeploymentStateService,
    OperationAction,
    OperationIntent,
    OperationProgress,
    StateBackendConflictError,
    StateBackendInvalidStateError,
    StateBackendLockLostError,
    StateBackendLockTimeoutError,
    StateBackendRecoveryRequiredError,
    StateBackendReleaseAfterCommitError,
    StateBackendUnavailableError,
    StateBackendUnknownCommitError,
    make_deployment_state_service,
    operation_timestamp,
    state_checksum,
)
from streamt.output import OutputFormatter

BLOCKED_OPERATION_ID = "00000000-0000-4000-8000-000000000201"
TARGET_FINGERPRINT = "sha256:" + "c" * 64
ENVIRONMENT_FINGERPRINT = "sha256:" + "a" * 64
MANIFEST_CHECKSUM = "sha256:" + "b" * 64


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


def _json(result: object) -> dict[str, object]:
    return json.loads(result.stdout)


def _target_id() -> str:
    return resource_id("recovery-test", "default", "topic", "orders")


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
