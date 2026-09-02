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
    DeploymentStateOperation,
    DeploymentStateService,
    StateAddress,
    StateObservation,
    StateRevision,
    StateStoreIdentity,
    make_deployment_state_service,
)


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
    ):
        result = CliRunner().invoke(
            main,
            ["-o", "json", "apply", "-p", str(tmp_path)],
        )

    assert result.exit_code == 0, result.output
    assert events == [
        "lock-enter",
        "state-read",
        "live-plan",
        "runtime-mutation",
        "state-save",
        "lock-exit",
    ]


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
    assert _json(result)["errors"][0]["code"] == "E411_STATE_INVALID"
    assert LocalState.load(state_path) == concurrent


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
