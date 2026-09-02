"""CLI tests for fail-closed topic ownership adoption."""

from __future__ import annotations

import json
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import yaml
from click.testing import CliRunner

from streamt.cli import main
from streamt.compiler.manifest import ArtifactOwnership, Manifest, TopicArtifact
from streamt.deployer.kafka import TopicState
from streamt.deployer.state import (
    LocalState,
    ManagedResourceRecord,
    artifact_checksum,
    local_state_path,
    resource_id,
)
from streamt.deployer.state_backend import (
    OperationAction,
    OperationIntent,
    RecoveryRecord,
    StateObservation,
    make_deployment_state_service,
    operation_timestamp,
    state_checksum,
)


def _write_project(path: Path) -> None:
    (path / "stream_project.yml").write_text(
        yaml.safe_dump(
            {
                "apiVersion": "streamt.dev/v1alpha1",
                "project": {"name": "adoption-test"},
                "runtime": {"kafka": {"bootstrap_servers": "localhost:9092"}},
            }
        )
    )


def _write_multi_environment_project(path: Path) -> None:
    (path / "stream_project.yml").write_text(
        yaml.safe_dump(
            {
                "apiVersion": "streamt.dev/v1alpha1",
                "project": {"name": "adoption-test"},
            }
        )
    )
    environments = path / "environments"
    environments.mkdir()
    for environment in ("dev", "prod"):
        (environments / f"{environment}.yml").write_text(
            yaml.safe_dump(
                {
                    "environment": {"name": environment},
                    "runtime": {
                        "kafka": {
                            "bootstrap_servers": f"{environment}-broker:9092"
                        }
                    },
                }
            )
        )


def _topic(
    *,
    logical_name: str = "orders",
    physical_name: str = "orders.v1",
    mode: str = "adopted",
) -> TopicArtifact:
    return TopicArtifact(
        name=physical_name,
        partitions=6,
        replication_factor=3,
        config={
            "cleanup.policy": "compact",
            "basic.auth.user.info": "desired-user:desired-password",
        },
        ownership=ArtifactOwnership(
            project="adoption-test",
            owner_type="model",
            owner_name=logical_name,
            mode=mode,
        ),
    )


def _manifest(*artifacts: TopicArtifact) -> Manifest:
    return Manifest(
        version="1.0",
        project_name="adoption-test",
        artifacts={"topics": [artifact.to_dict() for artifact in artifacts]},
    )


def _kafka(*, exists: bool = True) -> MagicMock:
    deployer = MagicMock()
    deployer.get_topic_state.return_value = TopicState(
        name="orders.v1",
        exists=exists,
        partitions=3 if exists else None,
        replication_factor=2 if exists else None,
        config={
            "cleanup.policy": "delete",
            "sasl.jaas.config": "username=live-user password=live-password",
            "endpoint": "https://alice:url-password@example.test",
        }
        if exists
        else {},
    )
    return deployer


def _invoke(
    path: Path,
    *,
    environment: str = "default",
    extra: list[str] | None = None,
    output: str = "json",
    input_text: str | None = None,
):
    resource_uri = resource_id("adoption-test", environment, "topic", "orders")
    args = [
        "-o",
        output,
        "adopt",
        "-p",
        str(path),
        "-e",
        environment,
        "--kind",
        "topic",
        "--name",
        "orders",
    ]
    if extra is None:
        args.extend(
            [
                "--confirm-resource",
                resource_uri,
                "--confirm-env",
                environment,
            ]
        )
    else:
        args.extend(extra)
    return CliRunner().invoke(main, args, input=input_text)


def _payload(result) -> dict[str, object]:
    return json.loads(result.stdout)


def _patch_adoption(manifest: Manifest, kafka: MagicMock):
    return (
        patch("streamt.compiler.Compiler.compile", return_value=manifest),
        patch("streamt.cli.commands.adopt.make_kafka_deployer", return_value=kafka),
    )


def test_success_observes_redacts_and_writes_only_adopted_state(tmp_path: Path) -> None:
    _write_project(tmp_path)
    artifact = _topic()
    kafka = _kafka()

    compiler_patch, kafka_patch = _patch_adoption(_manifest(artifact), kafka)
    with compiler_patch, kafka_patch:
        result = _invoke(tmp_path)

    assert result.exit_code == 0, result.output
    payload = _payload(result)
    data = payload["data"]
    assert data["adopted"] is True
    assert data["resource_id"] == resource_id(
        "adoption-test", "default", "topic", "orders"
    )
    assert data["physical_name"] == "orders.v1"
    assert data["observed"]["partitions"] == 3
    assert data["observed"]["replication_factor"] == 2
    assert data["pending_diffs"]["partitions"] == {"from": 3, "to": 6}
    assert data["next_command"] == [
        "streamt",
        "plan",
        "--project-dir",
        str(tmp_path),
        "--out",
        str(tmp_path / ".streamt" / "plans" / "default-reviewed-plan.json"),
    ]
    serialized = json.dumps(payload)
    for secret in (
        "live-user",
        "live-password",
        "desired-user",
        "desired-password",
        "url-password",
    ):
        assert secret not in serialized

    state = LocalState.load(local_state_path(tmp_path, environment="default"))
    record = state.resources[data["resource_id"]]
    assert state.serial == 1
    assert record == ManagedResourceRecord(
        physical_name="orders.v1",
        ownership="adopted",
        artifact_checksum=artifact_checksum(artifact.to_dict()),
        backend="direct-kafka",
    )
    kafka.get_topic_state.assert_called_once_with("orders.v1", strict_config=True)
    for method in (
        "apply_topic",
        "create_topic",
        "update_topic",
        "delete_topic",
        "apply",
    ):
        getattr(kafka, method).assert_not_called()
    kafka.close.assert_called_once_with()
    assert (
        make_deployment_state_service(
            tmp_path,
            project="adoption-test",
            environment="default",
        ).read_control().control.status
        == "clear"
    )


def test_existing_recovery_marker_blocks_adoption_before_runtime_setup(
    tmp_path: Path,
) -> None:
    _write_project(tmp_path)
    service = make_deployment_state_service(
        tmp_path,
        project="adoption-test",
        environment="default",
    )
    with service.operation() as operation:
        state = operation.read().state
        intent = OperationIntent(
            operation_id="00000000-0000-4000-8000-000000000021",
            kind="apply",
            started_at=operation_timestamp(),
            actor="prior-runner",
            prior_state_serial=state.serial,
            prior_state_checksum=state_checksum(state),
            reviewed_plan_checksum=None,
            actions=(OperationAction(0, "topic:orders", "create"),),
        )
        active = operation.begin_operation(operation.read_control(), intent)
        operation.mark_recovery_required(
            active,
            RecoveryRecord(
                operation_id=intent.operation_id,
                failure_code="operation_interrupted",
                failed_at=operation_timestamp(),
                last_completed_action_index=None,
            ),
        )

    deployer_factory = MagicMock()
    with (
        patch(
            "streamt.compiler.Compiler.compile",
            return_value=_manifest(_topic()),
        ),
        patch(
            "streamt.cli.commands.adopt.make_kafka_deployer",
            deployer_factory,
        ),
    ):
        result = _invoke(tmp_path)

    assert result.exit_code == 1
    assert _payload(result)["errors"][0]["code"] == "E419_STATE_RECOVERY_REQUIRED"
    deployer_factory.assert_not_called()


def test_adoption_holds_operation_lock_during_observation_and_confirmation(
    tmp_path: Path,
) -> None:
    _write_project(tmp_path)
    artifact = _topic()
    kafka = _kafka()
    current = kafka.get_topic_state.return_value
    events: list[str] = []

    def observe(*_args: object, **_kwargs: object) -> TopicState:
        events.append("live-observation")
        return current

    def confirm(**_kwargs: object) -> None:
        events.append("confirmation")

    @contextmanager
    def operation() -> Iterator[MagicMock]:
        events.append("lock-enter")
        service = make_deployment_state_service(
            tmp_path,
            project="adoption-test",
            environment="default",
        )
        with service.operation() as delegate:
            operation = MagicMock()
            operation.read.side_effect = delegate.read

            def save(
                observation: StateObservation,
                state: LocalState,
            ) -> StateObservation:
                assert state.serial == 1
                assert observation == delegate.read()
                events.append("state-save")
                return delegate.compare_and_swap(observation, state)

            operation.compare_and_swap.side_effect = save
            try:
                yield operation
            finally:
                events.append("lock-exit")

    state_service = MagicMock()
    state_service.operation.side_effect = operation

    kafka.get_topic_state.side_effect = observe
    compiler_patch, kafka_patch = _patch_adoption(_manifest(artifact), kafka)
    with (
        compiler_patch,
        kafka_patch,
        patch(
            "streamt.cli.commands.adopt.make_deployment_state_service",
            return_value=state_service,
        ),
        patch(
            "streamt.cli.commands.adopt._require_confirmation",
            side_effect=confirm,
        ),
    ):
        result = _invoke(tmp_path)

    assert result.exit_code == 0, result.output
    assert events == [
        "lock-enter",
        "live-observation",
        "confirmation",
        "state-save",
        "lock-exit",
    ]


@pytest.mark.parametrize("mode", ["external", "managed"])
def test_rejects_declaration_without_explicit_adopted_mode(
    tmp_path: Path,
    mode: str,
) -> None:
    _write_project(tmp_path)
    kafka = _kafka()
    compiler_patch, kafka_patch = _patch_adoption(_manifest(_topic(mode=mode)), kafka)

    with compiler_patch, kafka_patch:
        result = _invoke(tmp_path)

    assert result.exit_code == 1
    assert _payload(result)["errors"][0]["code"] == "E412_ADOPTION_TARGET_INVALID"
    assert not local_state_path(tmp_path, environment="default").exists()
    kafka.get_topic_state.assert_not_called()


def test_missing_live_topic_never_writes_or_mutates(tmp_path: Path) -> None:
    _write_project(tmp_path)
    kafka = _kafka(exists=False)
    compiler_patch, kafka_patch = _patch_adoption(_manifest(_topic()), kafka)

    with compiler_patch, kafka_patch:
        result = _invoke(tmp_path)

    assert result.exit_code == 1
    assert _payload(result)["errors"][0]["code"] == "E413_ADOPTION_LIVE_NOT_FOUND"
    assert not local_state_path(tmp_path, environment="default").exists()
    kafka.apply_topic.assert_not_called()


def test_config_observation_failure_fails_before_confirmation_or_save(
    tmp_path: Path,
) -> None:
    _write_project(tmp_path)
    kafka = _kafka()
    kafka.get_topic_state.side_effect = RuntimeError(
        "config read failed password=live-secret"
    )
    compiler_patch, kafka_patch = _patch_adoption(_manifest(_topic()), kafka)

    with compiler_patch, kafka_patch:
        result = _invoke(tmp_path)

    assert result.exit_code == 1
    payload = _payload(result)
    assert payload["errors"][0]["code"] == "E416_ADOPTION_FAILED"
    assert "live-secret" not in result.output
    assert not local_state_path(tmp_path, environment="default").exists()
    kafka.get_topic_state.assert_called_once_with("orders.v1", strict_config=True)


@pytest.mark.parametrize(
    "confirmation",
    [
        [],
        ["--confirm-env", "default"],
        ["--confirm-resource", "streamt://adoption-test/default/topic/wrong"],
        [
            "--confirm-resource",
            "streamt://adoption-test/default/topic/orders",
            "--confirm-env",
            "wrong",
        ],
    ],
)
def test_noninteractive_confirmation_requires_both_exact_values(
    tmp_path: Path,
    confirmation: list[str],
) -> None:
    _write_project(tmp_path)
    kafka = _kafka()
    compiler_patch, kafka_patch = _patch_adoption(_manifest(_topic()), kafka)

    with compiler_patch, kafka_patch:
        result = _invoke(tmp_path, extra=confirmation)

    assert result.exit_code == 1
    assert _payload(result)["errors"][0]["code"] == (
        "E414_ADOPTION_CONFIRMATION_REQUIRED"
    )
    assert not local_state_path(tmp_path, environment="default").exists()


def test_interactive_confirmation_accepts_exact_token(tmp_path: Path) -> None:
    _write_project(tmp_path)
    kafka = _kafka()
    resource_uri = resource_id("adoption-test", "default", "topic", "orders")
    token = f"adopt {resource_uri} in default\n"
    compiler_patch, kafka_patch = _patch_adoption(_manifest(_topic()), kafka)

    with (
        compiler_patch,
        kafka_patch,
        patch("streamt.cli.commands.adopt._stdin_is_interactive", return_value=True),
    ):
        result = _invoke(tmp_path, extra=[], output="text", input_text=token)

    assert result.exit_code == 0, result.output
    assert "Kafka was not modified" in result.output
    assert LocalState.load(local_state_path(tmp_path, environment="default")).serial == 1


def test_interactive_abort_never_writes_state(tmp_path: Path) -> None:
    _write_project(tmp_path)
    kafka = _kafka()
    compiler_patch, kafka_patch = _patch_adoption(_manifest(_topic()), kafka)

    with (
        compiler_patch,
        kafka_patch,
        patch("streamt.cli.commands.adopt._stdin_is_interactive", return_value=True),
    ):
        result = _invoke(
            tmp_path,
            extra=[],
            output="text",
            input_text="not-the-token\n",
        )

    assert result.exit_code == 1
    assert "did not match" in result.output
    assert not local_state_path(tmp_path, environment="default").exists()


def test_different_prior_record_fails_closed_before_observation(tmp_path: Path) -> None:
    _write_project(tmp_path)
    resource_uri = resource_id("adoption-test", "default", "topic", "orders")
    LocalState(
        project="adoption-test",
        environment="default",
        serial=4,
        resources={
            resource_uri: ManagedResourceRecord(
                physical_name="orders.old",
                ownership="adopted",
                artifact_checksum=artifact_checksum({"name": "orders.old"}),
                backend="direct-kafka",
            )
        },
    ).save(local_state_path(tmp_path, environment="default"))
    kafka = _kafka()
    compiler_patch, kafka_patch = _patch_adoption(_manifest(_topic()), kafka)

    with compiler_patch, kafka_patch:
        result = _invoke(tmp_path)

    assert result.exit_code == 1
    assert _payload(result)["errors"][0]["code"] == "E415_ADOPTION_STATE_CONFLICT"
    assert LocalState.load(local_state_path(tmp_path, environment="default")).serial == 4
    kafka.get_topic_state.assert_not_called()


@pytest.mark.parametrize("prior_ownership", ["managed", "adopted"])
def test_identical_prior_record_is_idempotent_without_confirmation_or_serial_change(
    tmp_path: Path,
    prior_ownership: str,
) -> None:
    _write_project(tmp_path)
    artifact = _topic()
    resource_uri = resource_id("adoption-test", "default", "topic", "orders")
    LocalState(
        project="adoption-test",
        environment="default",
        serial=7,
        resources={
            resource_uri: ManagedResourceRecord(
                physical_name=artifact.name,
                ownership=prior_ownership,  # type: ignore[arg-type]
                artifact_checksum=artifact_checksum(artifact.to_dict()),
                backend="direct-kafka",
            )
        },
    ).save(local_state_path(tmp_path, environment="default"))
    kafka = _kafka()
    compiler_patch, kafka_patch = _patch_adoption(_manifest(artifact), kafka)

    with compiler_patch, kafka_patch:
        result = _invoke(tmp_path, extra=[])

    assert result.exit_code == 0, result.output
    assert _payload(result)["data"]["already_owned"] is True
    assert LocalState.load(local_state_path(tmp_path, environment="default")).serial == 7
    kafka.get_topic_state.assert_called_once_with("orders.v1", strict_config=True)


def test_adoption_retains_unrelated_state_records(tmp_path: Path) -> None:
    _write_project(tmp_path)
    other_uri = resource_id("adoption-test", "default", "topic", "payments")
    other_record = ManagedResourceRecord(
        physical_name="payments.v1",
        ownership="managed",
        artifact_checksum=artifact_checksum({"name": "payments.v1"}),
        backend="direct-kafka",
    )
    LocalState(
        project="adoption-test",
        environment="default",
        serial=2,
        resources={other_uri: other_record},
    ).save(local_state_path(tmp_path, environment="default"))
    kafka = _kafka()
    compiler_patch, kafka_patch = _patch_adoption(_manifest(_topic()), kafka)

    with compiler_patch, kafka_patch:
        result = _invoke(tmp_path)

    assert result.exit_code == 0, result.output
    state = LocalState.load(local_state_path(tmp_path, environment="default"))
    assert state.serial == 3
    assert state.resources[other_uri] == other_record
    assert len(state.resources) == 2


def test_concurrent_state_change_fails_conflict_and_preserves_newer_snapshot(
    tmp_path: Path,
) -> None:
    _write_project(tmp_path)
    state_path = local_state_path(tmp_path, environment="default")
    other_uri = resource_id("adoption-test", "default", "topic", "payments")
    newer = LocalState(
        project="adoption-test",
        environment="default",
        serial=1,
        resources={
            other_uri: ManagedResourceRecord(
                physical_name="payments.v1",
                ownership="managed",
                artifact_checksum=artifact_checksum({"name": "payments.v1"}),
                backend="direct-kafka",
            )
        },
    )
    kafka = _kafka()
    compiler_patch, kafka_patch = _patch_adoption(_manifest(_topic()), kafka)

    def write_concurrent_state(**_kwargs: object) -> None:
        newer.save(state_path)

    with (
        compiler_patch,
        kafka_patch,
        patch(
            "streamt.cli.commands.adopt._require_confirmation",
            side_effect=write_concurrent_state,
        ),
    ):
        result = _invoke(tmp_path)

    assert result.exit_code == 1
    assert _payload(result)["errors"][0]["code"] == "E415_ADOPTION_STATE_CONFLICT"
    assert LocalState.load(state_path) == newer


def test_environment_states_are_isolated(tmp_path: Path) -> None:
    _write_multi_environment_project(tmp_path)
    artifact = _topic()
    for environment in ("dev", "prod"):
        kafka = _kafka()
        compiler_patch, kafka_patch = _patch_adoption(_manifest(artifact), kafka)
        with compiler_patch, kafka_patch:
            result = _invoke(tmp_path, environment=environment)
        assert result.exit_code == 0, result.output
        next_command = _payload(result)["data"]["next_command"]
        assert next_command[next_command.index("--env") + 1] == environment
        assert next_command[-1].endswith(
            f"/.streamt/plans/{environment}-reviewed-plan.json"
        )

    dev = LocalState.load(local_state_path(tmp_path, environment="dev"))
    prod = LocalState.load(local_state_path(tmp_path, environment="prod"))
    assert set(dev.resources) == {
        resource_id("adoption-test", "dev", "topic", "orders")
    }
    assert set(prod.resources) == {
        resource_id("adoption-test", "prod", "topic", "orders")
    }


def test_atomic_save_failure_reports_error_and_leaves_no_state(tmp_path: Path) -> None:
    _write_project(tmp_path)
    kafka = _kafka()
    compiler_patch, kafka_patch = _patch_adoption(_manifest(_topic()), kafka)

    with (
        compiler_patch,
        kafka_patch,
        patch(
            "streamt.cli.commands.adopt.LocalState.save",
            side_effect=OSError("swap failed password=state-secret"),
        ),
    ):
        result = _invoke(tmp_path)

    assert result.exit_code == 1
    payload = _payload(result)
    assert payload["errors"][0]["code"] == "E416_ADOPTION_FAILED"
    assert "state-secret" not in json.dumps(payload)
    assert not local_state_path(tmp_path, environment="default").exists()
    control = make_deployment_state_service(
        tmp_path,
        project="adoption-test",
        environment="default",
    ).read_control().control
    assert control.status == "recovery_required"
    assert control.recovery is not None
    assert control.recovery.failure_code == "adoption_state_commit_uncertain"


def test_unexpected_compiler_failure_is_structured_and_redacted(tmp_path: Path) -> None:
    _write_project(tmp_path)
    with patch(
        "streamt.compiler.Compiler.compile",
        side_effect=RuntimeError(
            "compile exploded password=compile-secret at "
            "https://alice:url-secret@example.test"
        ),
    ):
        result = _invoke(tmp_path)

    assert result.exit_code == 1
    payload = _payload(result)
    assert payload["errors"][0]["code"] == "E416_ADOPTION_FAILED"
    assert "compile-secret" not in result.output
    assert "url-secret" not in result.output
    assert not local_state_path(tmp_path, environment="default").exists()


def test_next_command_uses_real_safe_path_and_is_executable(tmp_path: Path) -> None:
    _write_project(tmp_path)
    compiler_patch, kafka_patch = _patch_adoption(_manifest(_topic()), _kafka())
    with compiler_patch, kafka_patch:
        adopted = _invoke(tmp_path)

    next_command = _payload(adopted)["data"]["next_command"]
    plan_path = tmp_path / ".streamt" / "plans" / "default-reviewed-plan.json"
    assert next_command[-1] == str(plan_path)
    assert "--env" not in next_command

    with patch(
        "streamt.cli.commands.plan.make_kafka_deployer",
        return_value=MagicMock(),
    ):
        planned = CliRunner().invoke(main, next_command[1:])

    assert planned.exit_code == 0, planned.output
    assert plan_path.exists()
