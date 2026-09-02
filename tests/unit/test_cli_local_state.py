"""CLI integration tests for local deployment ownership state."""

from __future__ import annotations

import json
from collections.abc import Iterator
from contextlib import AbstractContextManager, contextmanager
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import yaml
from click.testing import CliRunner

from streamt.cli import main
from streamt.compiler.manifest import ArtifactOwnership, Manifest, TopicArtifact
from streamt.core.deployment_state import local_deployment_state_config
from streamt.deployer.kafka import TopicChange, TopicState
from streamt.deployer.plan_file import ReviewedPlanFile, StateReference
from streamt.deployer.state import (
    LocalState,
    ManagedResourceRecord,
    artifact_checksum,
    local_state_path,
    resource_id,
)
from streamt.deployer.state_backend import (
    ControlObservation,
    DeploymentStateOperation,
    DeploymentStateService,
    OperationAction,
    OperationControlState,
    OperationIntent,
    OperationProgress,
    OperationSnapshot,
    RecoveryRecord,
    StateAddress,
    StateBackendConflictError,
    StateBackendLockLostError,
    StateBackendLockTimeoutError,
    StateBackendReleaseAfterCommitError,
    StateBackendUnknownCommitError,
    StateObservation,
    StateRevision,
    StateStoreIdentity,
    local_control_path,
    make_deployment_state_service,
    operation_timestamp,
    state_checksum,
)
from streamt.output import OutputFormatter


class _RecordingStateOperation:
    def __init__(
        self,
        delegate: DeploymentStateOperation,
        events: list[str],
    ) -> None:
        self._delegate = delegate
        self._events = events

    def read(self) -> StateObservation:
        self._events.append("state-read")
        return self._delegate.read()

    def read_control(self) -> ControlObservation:
        self._events.append("control-read")
        return self._delegate.read_control()

    def observe(self) -> OperationSnapshot:
        self._events.append("snapshot-observe")
        return self._delegate.observe()

    def ensure_ready(self, observation: OperationSnapshot) -> None:
        self._events.append("control-ready")
        self._delegate.ensure_ready(observation)

    def check_lock(self) -> None:
        self._events.append("lock-check")
        self._delegate.check_lock()

    def begin_operation(
        self,
        observation: OperationSnapshot,
        intent: OperationIntent,
    ) -> OperationSnapshot:
        self._events.append("control-begin")
        return self._delegate.begin_operation(observation, intent)

    def record_progress(
        self,
        observation: OperationSnapshot,
        progress: OperationProgress,
    ) -> OperationSnapshot:
        self._events.append(f"progress-{progress.status}")
        return self._delegate.record_progress(observation, progress)

    def mark_recovery_required(
        self,
        observation: OperationSnapshot,
        recovery: RecoveryRecord,
    ) -> OperationSnapshot:
        self._events.append("control-recovery")
        return self._delegate.mark_recovery_required(observation, recovery)

    def clear_operation(
        self,
        observation: OperationSnapshot,
    ) -> OperationSnapshot:
        self._events.append("control-clear")
        return self._delegate.clear_operation(observation)

    def commit_operation(
        self,
        observation: OperationSnapshot,
        state: LocalState | None,
    ) -> OperationSnapshot:
        self._events.append("operation-commit")
        return self._delegate.commit_operation(observation, state)

    def clear_before_mutation(
        self,
        observation: OperationSnapshot,
    ) -> OperationSnapshot:
        self._events.append("control-clear-before-mutation")
        return self._delegate.clear_before_mutation(observation)

    def compare_and_swap(
        self,
        observation: StateObservation,
        state: LocalState,
    ) -> StateObservation:
        self._events.append("state-save")
        return self._delegate.compare_and_swap(observation, state)


class _FakeReadBackend:
    """Minimal provider fake proving commands depend on the typed boundary."""

    def __init__(self, observation: StateObservation) -> None:
        self.observation = observation

    def describe(self) -> StateStoreIdentity:
        return self.observation.store

    def read(self, address: StateAddress) -> StateObservation:
        assert address == self.observation.address
        return self.observation

    def read_control(self, address: StateAddress) -> ControlObservation:
        assert address == self.observation.address
        return ControlObservation(
            control=OperationControlState.clear(address),
            revision=StateRevision.absent(),
        )

    def operation(
        self,
        address: StateAddress,
    ) -> AbstractContextManager[DeploymentStateOperation]:
        raise AssertionError(f"read-only plan must not acquire {address.uri}")


def _write_project(path: Path) -> None:
    config = {
        "apiVersion": "streamt.dev/v1alpha1",
        "project": {"name": "plan-test"},
        "runtime": {"kafka": {"bootstrap_servers": "localhost:9092"}},
    }
    (path / "stream_project.yml").write_text(yaml.safe_dump(config))


def _write_multi_environment_project(path: Path) -> None:
    config = {
        "apiVersion": "streamt.dev/v1alpha1",
        "project": {"name": "plan-test"},
    }
    (path / "stream_project.yml").write_text(yaml.safe_dump(config))
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
    name: str,
    *,
    owner: str,
    mode: str = "managed",
    partitions: int = 3,
) -> dict[str, object]:
    return TopicArtifact(
        name=name,
        partitions=partitions,
        replication_factor=1,
        ownership=ArtifactOwnership(
            project="plan-test",
            owner_type="model",
            owner_name=owner,
            mode=mode,
        ),
    ).to_dict()


def _manifest(*topics: dict[str, object]) -> Manifest:
    return Manifest(
        version="1.0",
        project_name="plan-test",
        artifacts={"topics": list(topics)},
    )


def _kafka(*, exists: bool, action: str | None = None) -> MagicMock:
    deployer = MagicMock()

    def plan_topic(artifact: TopicArtifact) -> TopicChange:
        observed_action = action or ("update" if exists else "create")
        return TopicChange(
            topic=artifact.name,
            action=observed_action,
            current=TopicState(name=artifact.name, exists=exists),
            desired=artifact,
        )

    deployer.plan_topic.side_effect = plan_topic
    deployer.apply_topic.return_value = "updated" if exists else "created"
    deployer.get_consumer_groups.return_value = []
    return deployer


def _json(result: object) -> dict[str, object]:
    return json.loads(result.stdout)


def test_first_apply_persists_state_and_repeat_plan_has_update_authority(
    tmp_path: Path,
) -> None:
    _write_project(tmp_path)
    topic = _topic("payments.clean.v1", owner="payments_clean")
    manifest = _manifest(topic)
    first_kafka = _kafka(exists=False)

    with (
        patch("streamt.compiler.Compiler.compile", return_value=manifest),
        patch(
            "streamt.cli.commands.apply.make_kafka_deployer",
            return_value=first_kafka,
        ),
    ):
        applied = CliRunner().invoke(
            main,
            ["-o", "json", "apply", "-p", str(tmp_path)],
        )

    assert applied.exit_code == 0, applied.output
    state_path = local_state_path(tmp_path, environment="default")
    state = LocalState.load(
        state_path,
        expected_project="plan-test",
        expected_environment="default",
    )
    topic_id = resource_id("plan-test", "default", "topic", "payments_clean")
    assert state.serial == 1
    assert state.resources[topic_id].physical_name == "payments.clean.v1"
    assert state.resources[topic_id].artifact_checksum == artifact_checksum(topic)
    assert state.resources[topic_id].backend == "direct-kafka"
    assert _json(applied)["data"]["state_serial"] == 1

    repeat_kafka = _kafka(exists=True)
    with (
        patch("streamt.compiler.Compiler.compile", return_value=manifest),
        patch(
            "streamt.cli.commands.plan.make_kafka_deployer",
            return_value=repeat_kafka,
        ),
    ):
        repeated = CliRunner().invoke(
            main,
            ["-o", "json", "plan", "-p", str(tmp_path)],
        )

    assert repeated.exit_code == 0, repeated.output
    payload = _json(repeated)
    assert payload["data"]["updates"] == 1
    assert payload["data"]["ownership_requirements"] == []
    assert payload["warnings"][0]["code"] == "W106_LOCAL_STATE_ONLY"
    assert "not yet supported" in payload["warnings"][0]["message"]


@pytest.mark.parametrize(
    ("error", "expected_code"),
    [
        (
            StateBackendLockTimeoutError("lock timeout password=lock-secret"),
            "E422_STATE_LOCK_TIMEOUT",
        ),
        (
            StateBackendLockLostError("lock lost token=lock-secret"),
            "E423_STATE_LOCK_LOST",
        ),
        (
            StateBackendConflictError("state conflict password=state-secret"),
            "E424_STATE_CONFLICT",
        ),
        (
            StateBackendUnknownCommitError("unknown token=commit-secret"),
            "E425_STATE_UNKNOWN_OUTCOME",
        ),
    ],
)
def test_apply_reports_distinct_redacted_state_backend_failures(
    tmp_path: Path,
    error: Exception,
    expected_code: str,
) -> None:
    _write_project(tmp_path)

    @contextmanager
    def failed_operation() -> Iterator[DeploymentStateOperation]:
        raise error
        yield  # pragma: no cover

    state_service = MagicMock()
    state_service.operation.side_effect = failed_operation
    with (
        patch("streamt.compiler.Compiler.compile", return_value=_manifest()),
        patch(
            "streamt.cli.commands.apply.make_deployment_state_service",
            return_value=state_service,
        ),
    ):
        result = CliRunner().invoke(
            main,
            ["-o", "json", "apply", "-p", str(tmp_path)],
        )

    assert result.exit_code == 1
    payload = _json(result)
    assert payload["errors"][0]["code"] == expected_code
    assert "secret" not in json.dumps(payload)


def test_apply_release_failure_after_commit_reports_committed_without_success(
    tmp_path: Path,
) -> None:
    _write_project(tmp_path)
    manifest = _manifest(_topic("payments.clean.v1", owner="payments_clean"))
    kafka = _kafka(exists=False)
    delegate_service = make_deployment_state_service(
        tmp_path,
        project="plan-test",
        environment="default",
        config=local_deployment_state_config(),
    )

    @contextmanager
    def operation() -> Iterator[DeploymentStateOperation]:
        with delegate_service.operation() as delegate:
            yield delegate
        raise StateBackendReleaseAfterCommitError(
            "operation release failed password=release-secret"
        )

    state_service = MagicMock()
    state_service.operation.side_effect = operation
    with (
        patch("streamt.compiler.Compiler.compile", return_value=manifest),
        patch("streamt.cli.commands.apply.make_kafka_deployer", return_value=kafka),
        patch(
            "streamt.cli.commands.apply.make_deployment_state_service",
            return_value=state_service,
        ),
    ):
        result = CliRunner().invoke(
            main,
            ["-o", "json", "apply", "-p", str(tmp_path)],
        )

    assert result.exit_code == 1
    payload = _json(result)
    assert payload["errors"][0]["code"] == "E426_STATE_RELEASE_FAILED_AFTER_COMMIT"
    assert payload["data"]["committed"] is True
    assert payload["data"]["state_serial"] == 1
    assert payload["data"]["created"] == ["topic:payments.clean.v1"]
    assert "suggestion" not in payload["errors"][0]
    assert "release-secret" not in json.dumps(payload)
    assert "Apply complete" not in result.output
    assert LocalState.load(local_state_path(tmp_path, environment="default")).serial == 1


def test_apply_holds_operation_lock_from_final_state_read_through_mutation_and_save(
    tmp_path: Path,
) -> None:
    _write_project(tmp_path)
    manifest = _manifest(_topic("payments.clean.v1", owner="payments_clean"))
    kafka = _kafka(exists=False)
    events: list[str] = []
    original_plan_topic = kafka.plan_topic.side_effect

    def plan_topic(artifact: TopicArtifact) -> TopicChange:
        events.append("live-plan")
        return original_plan_topic(artifact)

    def apply_topic(_artifact: TopicArtifact) -> str:
        events.append("runtime-mutation")
        return "created"

    @contextmanager
    def operation() -> Iterator[_RecordingStateOperation]:
        events.append("lock-enter")
        service = make_deployment_state_service(
            tmp_path,
            project="plan-test",
            environment="default",
            config=local_deployment_state_config(),
        )
        with service.operation() as delegate:
            try:
                yield _RecordingStateOperation(delegate, events)
            finally:
                events.append("lock-exit")

    state_service = MagicMock()
    state_service.operation.side_effect = operation

    kafka.plan_topic.side_effect = plan_topic
    kafka.apply_topic.side_effect = apply_topic
    original_flush = OutputFormatter.flush

    def flush(formatter: OutputFormatter) -> None:
        events.append("flush")
        original_flush(formatter)

    with (
        patch("streamt.compiler.Compiler.compile", return_value=manifest),
        patch(
            "streamt.cli.commands.apply.make_kafka_deployer",
            return_value=kafka,
        ),
        patch(
            "streamt.cli.commands.apply.make_deployment_state_service",
            return_value=state_service,
        ),
        patch("streamt.output.OutputFormatter.flush", new=flush),
    ):
        result = CliRunner().invoke(
            main,
            ["-o", "json", "apply", "-p", str(tmp_path)],
        )

    assert result.exit_code == 0, result.output
    assert events == [
        "lock-enter",
        "snapshot-observe",
        "control-ready",
        "live-plan",
        "snapshot-observe",
        "control-ready",
        "control-begin",
        "lock-check",
        "progress-started",
        "runtime-mutation",
        "lock-check",
        "progress-completed",
        "lock-check",
        "operation-commit",
        "lock-exit",
        "flush",
    ]
    assert "state-save" not in events
    assert "control-clear" not in events


def test_runtime_base_exception_leaves_recovery_marker_and_blocks_successor(
    tmp_path: Path,
) -> None:
    _write_project(tmp_path)
    manifest = _manifest(_topic("payments.clean.v1", owner="payments_clean"))
    kafka = _kafka(exists=False)
    kafka.apply_topic.side_effect = KeyboardInterrupt()

    with (
        patch("streamt.compiler.Compiler.compile", return_value=manifest),
        patch(
            "streamt.cli.commands.apply.make_kafka_deployer",
            return_value=kafka,
        ),
    ):
        interrupted = CliRunner().invoke(
            main,
            ["-o", "json", "apply", "-p", str(tmp_path)],
        )

    assert interrupted.exit_code == 130
    control = make_deployment_state_service(
        tmp_path,
        project="plan-test",
        environment="default",
        config=local_deployment_state_config(),
    ).read_control()
    assert control.control.status == "recovery_required"
    assert control.control.recovery is not None
    assert control.control.recovery.failure_code == "operation_interrupted"
    assert control.control.progress[-1].status == "started"
    expected_resource_id = resource_id(
        "plan-test",
        "default",
        "topic",
        "payments_clean",
    )
    assert control.control.intent is not None
    assert control.control.intent.actions[0].resource_id == expected_resource_id
    assert control.control.progress[-1].resource_id == expected_resource_id

    deployer_factory = MagicMock()
    with (
        patch("streamt.compiler.Compiler.compile", return_value=manifest),
        patch(
            "streamt.cli.commands.apply.make_kafka_deployer",
            deployer_factory,
        ),
    ):
        successor = CliRunner().invoke(
            main,
            ["-o", "json", "apply", "-p", str(tmp_path)],
        )

    assert successor.exit_code == 1
    assert _json(successor)["errors"][0]["code"] == "E419_STATE_RECOVERY_REQUIRED"
    deployer_factory.assert_not_called()


def test_controlled_apply_stops_after_first_failed_runtime_action(
    tmp_path: Path,
) -> None:
    _write_project(tmp_path)
    manifest = _manifest(
        _topic("orders.v1", owner="orders"),
        _topic("payments.v1", owner="payments"),
    )
    kafka = _kafka(exists=False)
    called: list[str] = []

    def fail_first(artifact: TopicArtifact) -> str:
        called.append(artifact.name)
        raise RuntimeError("unknown runtime result")

    kafka.apply_topic.side_effect = fail_first
    with (
        patch("streamt.compiler.Compiler.compile", return_value=manifest),
        patch(
            "streamt.cli.commands.apply.make_kafka_deployer",
            return_value=kafka,
        ),
    ):
        result = CliRunner().invoke(
            main,
            ["-o", "json", "apply", "-p", str(tmp_path)],
        )

    assert result.exit_code == 1
    assert called == ["orders.v1"]
    control = make_deployment_state_service(
        tmp_path,
        project="plan-test",
        environment="default",
        config=local_deployment_state_config(),
    ).read_control().control
    assert control.status == "recovery_required"
    assert [
        (item.action_index, item.status, item.succeeded)
        for item in control.progress
    ] == [(0, "started", None), (0, "completed", False)]
    assert control.recovery is not None
    assert control.recovery.last_completed_action_index is None


def test_ownership_save_failure_after_runtime_success_remains_recovery_required(
    tmp_path: Path,
) -> None:
    _write_project(tmp_path)
    manifest = _manifest(_topic("payments.clean.v1", owner="payments_clean"))
    kafka = _kafka(exists=False)

    with (
        patch("streamt.compiler.Compiler.compile", return_value=manifest),
        patch(
            "streamt.cli.commands.apply.make_kafka_deployer",
            return_value=kafka,
        ),
        patch(
            "streamt.deployer.state.LocalStateOperationLock.save_if_serial",
            side_effect=OSError("token=must-not-leak"),
        ),
    ):
        result = CliRunner().invoke(
            main,
            ["-o", "json", "apply", "-p", str(tmp_path)],
        )

    assert result.exit_code == 1
    assert "must-not-leak" not in result.output
    control = make_deployment_state_service(
        tmp_path,
        project="plan-test",
        environment="default",
        config=local_deployment_state_config(),
    ).read_control()
    assert control.control.status == "recovery_required"
    assert control.control.recovery is not None
    assert control.control.recovery.failure_code == "state_commit_uncertain"
    assert not local_state_path(tmp_path, environment="default").exists()


def test_uncertain_zero_action_finalizer_is_not_cleared_as_pre_mutation_failure(
    tmp_path: Path,
) -> None:
    _write_project(tmp_path)
    manifest = _manifest()

    with (
        patch("streamt.compiler.Compiler.compile", return_value=manifest),
        patch(
            "streamt.cli.commands.apply.make_kafka_deployer",
            return_value=_kafka(exists=False),
        ),
        patch(
            "streamt.deployer.state_backend._LocalDeploymentStateOperation.commit_operation",
            side_effect=OSError("uncertain final clear"),
        ),
    ):
        result = CliRunner().invoke(
            main,
            ["-o", "json", "apply", "-p", str(tmp_path)],
        )

    assert result.exit_code == 1
    control = make_deployment_state_service(
        tmp_path,
        project="plan-test",
        environment="default",
        config=local_deployment_state_config(),
    ).read_control().control
    assert control.status == "recovery_required"
    assert control.intent is not None
    assert control.intent.actions == ()
    assert control.recovery is not None
    assert control.recovery.failure_code == "state_commit_uncertain"


def test_failure_before_first_runtime_action_clears_intent(tmp_path: Path) -> None:
    _write_project(tmp_path)
    manifest = _manifest(_topic("payments.clean.v1", owner="payments_clean"))
    kafka = _kafka(exists=False)

    with (
        patch("streamt.compiler.Compiler.compile", return_value=manifest),
        patch(
            "streamt.cli.commands.apply.make_kafka_deployer",
            return_value=kafka,
        ),
        patch(
            "streamt.deployer.planner.DeploymentPlanner.apply",
            side_effect=RuntimeError("pre-action failure"),
        ),
    ):
        result = CliRunner().invoke(
            main,
            ["-o", "json", "apply", "-p", str(tmp_path)],
        )

    assert result.exit_code == 1
    kafka.apply_topic.assert_not_called()
    control = make_deployment_state_service(
        tmp_path,
        project="plan-test",
        environment="default",
        config=local_deployment_state_config(),
    ).read_control()
    assert control.control.status == "clear"
    assert local_control_path(tmp_path, environment="default").exists()
    assert not local_state_path(tmp_path, environment="default").exists()


def test_saved_online_plan_rejects_changed_state_serial(tmp_path: Path) -> None:
    _write_project(tmp_path)
    manifest = _manifest(_topic("payments.clean.v1", owner="payments_clean"))
    reviewed_path = tmp_path / "reviewed.plan.json"

    with (
        patch("streamt.compiler.Compiler.compile", return_value=manifest),
        patch(
            "streamt.cli.commands.plan.make_kafka_deployer",
            return_value=_kafka(exists=False),
        ),
    ):
        planned = CliRunner().invoke(
            main,
            [
                "plan",
                "-p",
                str(tmp_path),
                "--out",
                str(reviewed_path),
            ],
        )

    assert planned.exit_code == 0, planned.output
    assert ReviewedPlanFile.load(reviewed_path).state_serial == 0
    LocalState(project="plan-test", environment="default", serial=1).save(
        local_state_path(tmp_path, environment="default")
    )

    with patch("streamt.compiler.Compiler.compile", return_value=manifest):
        applied = CliRunner().invoke(
            main,
            [
                "-o",
                "json",
                "apply",
                "-p",
                str(tmp_path),
                "--plan",
                str(reviewed_path),
            ],
        )

    assert applied.exit_code == 1
    payload = _json(applied)
    assert payload["errors"][0]["code"] == "E409_PLAN_STALE"
    assert "state serial" in payload["errors"][0]["message"]


def test_external_resources_are_excluded_from_persisted_state(tmp_path: Path) -> None:
    _write_project(tmp_path)
    managed = _topic("payments.clean.v1", owner="payments_clean")
    external = _topic("upstream.raw.v1", owner="raw_events", mode="external")
    manifest = _manifest(managed, external)

    with (
        patch("streamt.compiler.Compiler.compile", return_value=manifest),
        patch(
            "streamt.cli.commands.apply.make_kafka_deployer",
            return_value=_kafka(exists=False),
        ),
    ):
        result = CliRunner().invoke(main, ["apply", "-p", str(tmp_path)])

    assert result.exit_code == 0, result.output
    state = LocalState.load(local_state_path(tmp_path, environment="default"))
    assert set(state.resources) == {
        resource_id("plan-test", "default", "topic", "payments_clean")
    }


@pytest.mark.parametrize(
    "state_payload",
    [
        "{not-json",
        json.dumps(
            {
                "state_version": 1,
                "project": "some-other-project",
                "environment": "default",
                "serial": 0,
                "resources": {},
            }
        ),
    ],
)
def test_malformed_or_mismatched_state_fails_closed(
    tmp_path: Path,
    state_payload: str,
) -> None:
    _write_project(tmp_path)
    state_path = local_state_path(tmp_path, environment="default")
    state_path.parent.mkdir(parents=True)
    state_path.write_text(state_payload)

    result = CliRunner().invoke(
        main,
        ["-o", "json", "plan", "-p", str(tmp_path)],
    )

    assert result.exit_code == 1
    assert _json(result)["errors"][0]["code"] == "E411_STATE_INVALID"


def test_apply_failure_and_rollback_never_save_state(tmp_path: Path) -> None:
    _write_project(tmp_path)
    manifest = _manifest(
        _topic("first.v1", owner="first"),
        _topic("second.v1", owner="second"),
    )
    kafka = _kafka(exists=False)
    kafka.apply_topic.side_effect = ["created", RuntimeError("second create failed")]

    with (
        patch("streamt.compiler.Compiler.compile", return_value=manifest),
        patch(
            "streamt.cli.commands.apply.make_kafka_deployer",
            return_value=kafka,
        ),
    ):
        result = CliRunner().invoke(
            main,
            ["-o", "json", "apply", "-p", str(tmp_path)],
        )

    assert result.exit_code == 1
    assert _json(result)["errors"][0]["code"] == "E407_DEPLOY_ERROR"
    assert not local_state_path(tmp_path, environment="default").exists()
    kafka.delete_topic.assert_called_once_with("first.v1")


def test_apply_cas_rejects_concurrent_state_and_preserves_newer_snapshot(
    tmp_path: Path,
) -> None:
    _write_project(tmp_path)
    manifest = _manifest(_topic("payments.clean.v1", owner="payments_clean"))
    state_path = local_state_path(tmp_path, environment="default")
    other_uri = resource_id("plan-test", "default", "topic", "other")
    concurrent = LocalState(
        project="plan-test",
        environment="default",
        serial=1,
        resources={
            other_uri: ManagedResourceRecord(
                physical_name="other.v1",
                ownership="managed",
                artifact_checksum=artifact_checksum({"name": "other.v1"}),
                backend="direct-kafka",
            )
        },
    )
    kafka = _kafka(exists=False)

    def apply_after_concurrent_write(_artifact: object) -> str:
        concurrent.save(state_path)
        return "created"

    kafka.apply_topic.side_effect = apply_after_concurrent_write
    with (
        patch("streamt.compiler.Compiler.compile", return_value=manifest),
        patch(
            "streamt.cli.commands.apply.make_kafka_deployer",
            return_value=kafka,
        ),
    ):
        result = CliRunner().invoke(
            main,
            ["-o", "json", "apply", "-p", str(tmp_path)],
        )

    assert result.exit_code == 1
    assert _json(result)["errors"][0]["code"] == "E424_STATE_CONFLICT"
    assert LocalState.load(state_path) == concurrent


def test_direct_apply_rejects_state_drift_on_final_pre_intent_observation(
    tmp_path: Path,
) -> None:
    _write_project(tmp_path)
    manifest = _manifest(_topic("payments.clean.v1", owner="payments_clean"))
    state_path = local_state_path(tmp_path, environment="default")
    other_uri = resource_id("plan-test", "default", "topic", "other")
    concurrent = LocalState(
        project="plan-test",
        environment="default",
        serial=1,
        resources={
            other_uri: ManagedResourceRecord(
                physical_name="other.v1",
                ownership="managed",
                artifact_checksum=artifact_checksum({"name": "other.v1"}),
                backend="direct-kafka",
            )
        },
    )
    kafka = _kafka(exists=False)
    original_plan_topic = kafka.plan_topic.side_effect

    def plan_after_concurrent_write(artifact: TopicArtifact) -> TopicChange:
        change = original_plan_topic(artifact)
        concurrent.save(state_path)
        return change

    kafka.plan_topic.side_effect = plan_after_concurrent_write
    with (
        patch("streamt.compiler.Compiler.compile", return_value=manifest),
        patch(
            "streamt.cli.commands.apply.make_kafka_deployer",
            return_value=kafka,
        ),
    ):
        result = CliRunner().invoke(
            main,
            ["-o", "json", "apply", "-p", str(tmp_path)],
        )

    assert result.exit_code == 1
    assert _json(result)["errors"][0]["code"] == "E424_STATE_CONFLICT"
    assert "changed during live planning" in _json(result)["errors"][0]["message"]
    kafka.apply_topic.assert_not_called()
    assert LocalState.load(state_path) == concurrent
    control = make_deployment_state_service(
        tmp_path,
        project="plan-test",
        environment="default",
        config=local_deployment_state_config(),
    ).read_control()
    assert control.control.status == "clear"


def test_direct_apply_rejects_clear_control_revision_drift_before_intent(
    tmp_path: Path,
) -> None:
    _write_project(tmp_path)
    manifest = _manifest(_topic("payments.clean.v1", owner="payments_clean"))
    kafka = _kafka(exists=False)
    original_plan_topic = kafka.plan_topic.side_effect
    service = make_deployment_state_service(
        tmp_path,
        project="plan-test",
        environment="default",
        config=local_deployment_state_config(),
    )
    control_path = local_control_path(tmp_path, environment="default")

    def plan_after_control_churn(artifact: TopicArtifact) -> TopicChange:
        change = original_plan_topic(artifact)
        control_path.parent.mkdir(parents=True, exist_ok=True)
        control_path.write_text(
            json.dumps(OperationControlState.clear(service.address).to_dict())
        )
        return change

    kafka.plan_topic.side_effect = plan_after_control_churn
    with (
        patch("streamt.compiler.Compiler.compile", return_value=manifest),
        patch(
            "streamt.cli.commands.apply.make_kafka_deployer",
            return_value=kafka,
        ),
    ):
        result = CliRunner().invoke(
            main,
            ["-o", "json", "apply", "-p", str(tmp_path)],
        )

    assert result.exit_code == 1
    payload = _json(result)
    assert payload["errors"][0]["code"] == "E424_STATE_CONFLICT"
    assert "operation control changed" in payload["errors"][0]["message"]
    kafka.apply_topic.assert_not_called()
    assert service.read_control().control.status == "clear"


def test_offline_plan_does_not_read_or_create_local_state(tmp_path: Path) -> None:
    _write_project(tmp_path)
    state_path = local_state_path(tmp_path, environment="default")
    state_path.parent.mkdir(parents=True)
    state_path.write_text("{malformed-but-irrelevant")
    before = state_path.read_bytes()

    with patch(
        "streamt.cli.commands.plan.make_deployment_state_service",
        side_effect=AssertionError("offline plan constructed state backend"),
    ) as state_factory:
        result = CliRunner().invoke(
            main,
            ["-o", "json", "plan", "-p", str(tmp_path), "--offline"],
        )

    assert result.exit_code == 0, result.output
    state_factory.assert_not_called()
    assert state_path.read_bytes() == before
    assert _json(result)["warnings"] == []
    assert _json(result)["data"]["operation_status"] == {
        "status": "unavailable",
        "operation_id": None,
        "kind": None,
        "failure_code": None,
        "last_completed_action_index": None,
    }


def test_online_plan_reads_injected_backend_without_touching_local_state(
    tmp_path: Path,
) -> None:
    _write_project(tmp_path)
    state_path = local_state_path(tmp_path, environment="default")
    state_path.parent.mkdir(parents=True)
    state_path.write_text("{malformed-but-not-selected")
    address = StateAddress(
        namespace="test",
        project="plan-test",
        environment="default",
    )
    observation = StateObservation(
        store=StateStoreIdentity(
            backend="fake",
            store_id="00000000-0000-4000-8000-000000000007",
        ),
        address=address,
        state=LocalState(project="plan-test", environment="default", serial=7),
        revision=StateRevision("fake:7"),
    )
    service = DeploymentStateService(
        backend=_FakeReadBackend(observation),
        address=address,
    )
    reviewed_path = tmp_path / "reviewed.plan.json"
    manifest = _manifest(_topic("payments.clean.v1", owner="payments_clean"))

    with (
        patch("streamt.compiler.Compiler.compile", return_value=manifest),
        patch(
            "streamt.cli.commands.plan.make_kafka_deployer",
            return_value=_kafka(exists=False),
        ),
        patch(
            "streamt.cli.commands.plan.make_deployment_state_service",
            return_value=service,
        ),
    ):
        result = CliRunner().invoke(
            main,
            ["plan", "-p", str(tmp_path), "--out", str(reviewed_path)],
        )

    assert result.exit_code == 0, result.output
    loaded = ReviewedPlanFile.load(reviewed_path)
    assert loaded.state_serial == 7
    assert loaded.state == StateReference.from_observation(observation)
    assert "fake:7" not in reviewed_path.read_text()
    assert state_path.read_text() == "{malformed-but-not-selected"


def test_online_plan_exposes_safe_recovery_status_without_mutating_it(
    tmp_path: Path,
) -> None:
    _write_project(tmp_path)
    service = make_deployment_state_service(
        tmp_path,
        project="plan-test",
        environment="default",
        config=local_deployment_state_config(),
    )
    with service.operation() as operation:
        state = operation.read().state
        intent = OperationIntent(
            operation_id="00000000-0000-4000-8000-000000000031",
            kind="apply",
            started_at=operation_timestamp(),
            actor="prior-runner",
            prior_state_serial=state.serial,
            prior_state_checksum=state_checksum(state),
            reviewed_plan_checksum=None,
            actions=(OperationAction(0, "topic:payments.clean.v1", "create"),),
        )
        active = operation.begin_operation(operation.read_control(), intent)
        operation.mark_recovery_required(
            active,
            RecoveryRecord(
                operation_id=intent.operation_id,
                failure_code="runtime_action_failed",
                failed_at=operation_timestamp(),
                last_completed_action_index=None,
            ),
        )
    control_path = local_control_path(tmp_path, environment="default")
    before = control_path.read_bytes()
    manifest = _manifest(_topic("payments.clean.v1", owner="payments_clean"))

    with (
        patch("streamt.compiler.Compiler.compile", return_value=manifest),
        patch(
            "streamt.cli.commands.plan.make_kafka_deployer",
            return_value=_kafka(exists=False),
        ),
    ):
        result = CliRunner().invoke(
            main,
            ["-o", "json", "plan", "-p", str(tmp_path)],
        )

    assert result.exit_code == 0, result.output
    assert _json(result)["data"]["operation_status"] == {
        "status": "recovery_required",
        "operation_id": intent.operation_id,
        "kind": "apply",
        "failure_code": "runtime_action_failed",
        "last_completed_action_index": None,
    }
    assert control_path.read_bytes() == before


def test_dev_and_prod_states_coexist_without_mismatch_or_overwrite(
    tmp_path: Path,
) -> None:
    _write_multi_environment_project(tmp_path)
    manifest = _manifest(_topic("payments.clean.v1", owner="payments_clean"))

    for environment in ("dev", "prod"):
        with (
            patch("streamt.compiler.Compiler.compile", return_value=manifest),
            patch(
                "streamt.cli.commands.apply.make_kafka_deployer",
                return_value=_kafka(exists=False),
            ),
        ):
            result = CliRunner().invoke(
                main,
                ["apply", "-p", str(tmp_path), "--env", environment],
            )
        assert result.exit_code == 0, result.output

    dev_path = local_state_path(tmp_path, environment="dev")
    prod_path = local_state_path(tmp_path, environment="prod")
    assert dev_path == tmp_path / ".streamt" / "state" / "dev.json"
    assert prod_path == tmp_path / ".streamt" / "state" / "prod.json"
    assert dev_path.exists()
    assert prod_path.exists()
    assert LocalState.load(dev_path).environment == "dev"
    assert LocalState.load(prod_path).environment == "prod"
    prod_before = prod_path.read_bytes()

    with (
        patch("streamt.compiler.Compiler.compile", return_value=manifest),
        patch(
            "streamt.cli.commands.plan.make_kafka_deployer",
            return_value=_kafka(exists=True),
        ),
    ):
        switched = CliRunner().invoke(
            main,
            ["-o", "json", "plan", "-p", str(tmp_path), "--env", "dev"],
        )

    assert switched.exit_code == 0, switched.output
    assert _json(switched)["data"]["ownership_requirements"] == []
    assert prod_path.read_bytes() == prod_before
