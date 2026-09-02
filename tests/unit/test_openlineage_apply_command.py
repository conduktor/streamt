"""Acceptance tests for durable OpenLineage events from ``streamt apply``."""

from __future__ import annotations

import importlib
import json
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import MagicMock, patch
from uuid import UUID

import pytest
import yaml
from click.testing import CliRunner, Result

from streamt.cli import main
from streamt.compiler.manifest import ArtifactOwnership, Manifest, TopicArtifact
from streamt.core.deployment_state import local_deployment_state_config
from streamt.deployer.kafka import TopicChange, TopicState
from streamt.deployer.state import LocalState, local_state_path
from streamt.deployer.state_backend import (
    DeploymentStateOperation,
    OperationIntent,
    OperationProgress,
    OperationSnapshot,
    RecoveryRecord,
    StateBackendLockLostError,
    StateBackendReleaseAfterCommitError,
    StateBackendUnknownCommitError,
    local_control_path,
    make_deployment_state_service,
    operation_timestamp,
    state_checksum,
)
from streamt.integrations.openlineage import (
    OpenLineageConstructionError,
    OpenLineageTransportConfigurationError,
    validate_event_sequence,
)
from streamt.output import OutputFormatter

apply_command = importlib.import_module("streamt.cli.commands.apply")

_OPENLINEAGE_ENVIRONMENT = (
    "OPENLINEAGE_NAMESPACE",
    "STREAMT_OPENLINEAGE_KAFKA_NAMESPACE",
    "STREAMT_OPENLINEAGE_GATEWAY_NAMESPACE",
    "OPENLINEAGE_CONFIG",
    "OPENLINEAGE_DISABLED",
    "OPENLINEAGE_URL",
    "OPENLINEAGE_API_KEY",
    "OPENLINEAGE__TRANSPORT__TYPE",
    "OPENLINEAGE__TRANSPORT__LOG_FILE_PATH",
    "OPENLINEAGE__TRANSPORT__URL",
    "OPENLINEAGE__TRANSPORT__ENDPOINT",
    "OPENLINEAGE__TRANSPORT__TIMEOUT",
    "OPENLINEAGE__TRANSPORT__VERIFY",
    "OPENLINEAGE__TRANSPORT__RETRY__TOTAL",
    "OPENLINEAGE__TRANSPORT__AUTH__TYPE",
    "OPENLINEAGE__TRANSPORT__AUTH__APIKEY",
)
_JOB_NAMESPACE = "https://lineage.example/namespaces/local"
_TOPIC_NAME = "private.orders.telemetry-do-not-emit"
_RUNTIME_SECRET = "runtime-bootstrap-secret.invalid:9092"
_FAILURE_SECRET = "runtime-password=apply-failure-secret"
_TRANSPORT_SECRET = "transport-token=apply-transport-secret"


@pytest.fixture(autouse=True)
def _clear_openlineage_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in _OPENLINEAGE_ENVIRONMENT:
        monkeypatch.setenv(name, "streamt-apply-test-unset-sentinel")
        monkeypatch.delenv(name, raising=False)


class _FakeTransport:
    """Record lifecycle attempts and optionally fail at delivery boundaries."""

    def __init__(
        self,
        events: list[str],
        *,
        fail_attempts: set[int] | None = None,
        fail_close: bool = False,
    ) -> None:
        self.events = events
        self.attempts: list[dict[str, object]] = []
        self.fail_attempts = fail_attempts or set()
        self.fail_close = fail_close
        self.close_calls = 0

    def emit(self, event: dict[str, object]) -> None:
        self.attempts.append(event)
        event_type = event.get("eventType")
        self.events.append(f"openlineage-{event_type}")
        if len(self.attempts) in self.fail_attempts:
            raise RuntimeError(_TRANSPORT_SECRET)

    def close(self) -> None:
        self.close_calls += 1
        self.events.append("openlineage-close")
        if self.fail_close:
            raise RuntimeError(_TRANSPORT_SECRET)


class _RecordingOperation:
    """Record only durable transition completions around the real local service."""

    def __init__(
        self,
        delegate: DeploymentStateOperation,
        events: list[str],
        intents: list[OperationIntent],
        *,
        fail_commit: bool = False,
        lose_lock_after_begin: bool = False,
    ) -> None:
        self.delegate = delegate
        self.events = events
        self.intents = intents
        self.fail_commit = fail_commit
        self.lose_lock_after_begin = lose_lock_after_begin
        self.begun = False

    def observe(self) -> OperationSnapshot:
        return self.delegate.observe()

    def ensure_ready(self, observation: OperationSnapshot) -> None:
        self.delegate.ensure_ready(observation)

    def check_lock(self) -> None:
        if self.begun and self.lose_lock_after_begin:
            raise StateBackendLockLostError(
                "deployment state operation lock was lost token=lock-secret",
                operation_id=self.intents[-1].operation_id,
            )
        self.delegate.check_lock()

    def begin_operation(
        self,
        observation: OperationSnapshot,
        intent: OperationIntent,
    ) -> OperationSnapshot:
        active = self.delegate.begin_operation(observation, intent)
        self.intents.append(intent)
        self.begun = True
        self.events.append("operation-begin-durable")
        return active

    def record_progress(
        self,
        observation: OperationSnapshot,
        progress: OperationProgress,
    ) -> OperationSnapshot:
        active = self.delegate.record_progress(observation, progress)
        self.events.append(f"progress-{progress.status}-durable")
        return active

    def mark_recovery_required(
        self,
        observation: OperationSnapshot,
        recovery: RecoveryRecord,
    ) -> OperationSnapshot:
        active = self.delegate.mark_recovery_required(observation, recovery)
        self.events.append("recovery-durable")
        return active

    def clear_before_mutation(
        self,
        observation: OperationSnapshot,
    ) -> OperationSnapshot:
        if self.lose_lock_after_begin:
            raise StateBackendLockLostError(
                "deployment state operation lock was lost token=lock-secret",
                operation_id=self.intents[-1].operation_id,
            )
        active = self.delegate.clear_before_mutation(observation)
        self.events.append("pre-mutation-clear-durable")
        return active

    def commit_operation(
        self,
        observation: OperationSnapshot,
        state: LocalState | None,
    ) -> OperationSnapshot:
        if self.fail_commit:
            self.events.append("operation-commit-unknown")
            raise StateBackendUnknownCommitError(
                "state commit could not be confirmed password=commit-secret",
                operation_id=self.intents[-1].operation_id,
            )
        committed = self.delegate.commit_operation(observation, state)
        assert committed.control.control.status == "clear"
        self.events.append("operation-commit-and-clear-durable")
        return committed


def _write_project(path: Path, *, bootstrap: str = _RUNTIME_SECRET) -> None:
    path.joinpath("stream_project.yml").write_text(
        yaml.safe_dump(
            {
                "apiVersion": "streamt.dev/v1alpha1",
                "project": {"name": "telemetry project"},
                "runtime": {"kafka": {"bootstrap_servers": bootstrap}},
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )


def _topic_manifest() -> Manifest:
    topic = TopicArtifact(
        name=_TOPIC_NAME,
        partitions=3,
        replication_factor=1,
        ownership=ArtifactOwnership(
            project="telemetry project",
            owner_type="model",
            owner_name="private_orders_model",
            mode="managed",
        ),
    )
    return Manifest(
        version="1.0",
        project_name="telemetry project",
        artifacts={"topics": [topic.to_dict()]},
    )


def _empty_manifest() -> Manifest:
    return Manifest(
        version="1.0",
        project_name="telemetry project",
        artifacts={},
    )


def _kafka(*, failure: BaseException | None = None) -> MagicMock:
    deployer = MagicMock()

    def plan_topic(artifact: TopicArtifact) -> TopicChange:
        return TopicChange(
            topic=artifact.name,
            action="create",
            current=TopicState(name=artifact.name, exists=False),
            desired=artifact,
        )

    deployer.plan_topic.side_effect = plan_topic
    deployer.apply_topic.side_effect = failure or (lambda _artifact: "created")
    deployer.get_consumer_groups.return_value = []
    return deployer


def _recording_state_service(
    project_path: Path,
    events: list[str],
    intents: list[OperationIntent],
    *,
    fail_commit: bool = False,
    lose_lock_after_begin: bool = False,
    fail_release: bool = False,
) -> MagicMock:
    delegate_service = make_deployment_state_service(
        project_path,
        project="telemetry project",
        environment="default",
        config=local_deployment_state_config(),
    )

    @contextmanager
    def operation() -> Iterator[_RecordingOperation]:
        with delegate_service.operation() as delegate:
            yield _RecordingOperation(
                delegate,
                events,
                intents,
                fail_commit=fail_commit,
                lose_lock_after_begin=lose_lock_after_begin,
            )
        events.append("state-authority-released")
        if fail_release:
            operation_id = intents[-1].operation_id if intents else None
            raise StateBackendReleaseAfterCommitError(
                "authority release failed password=release-secret",
                operation_id=operation_id,
            )

    service = MagicMock()
    service.operation.side_effect = operation
    return service


def _install_transports(
    monkeypatch: pytest.MonkeyPatch,
    *transports: _FakeTransport,
) -> None:
    queue = list(transports)
    sentinel_config = object()

    def load_config(_environment: object, *, emission_requested: bool) -> object:
        assert emission_requested is True
        return sentinel_config

    def create_transport(config: object) -> _FakeTransport:
        assert config is sentinel_config
        return queue.pop(0)

    monkeypatch.setattr(apply_command, "load_openlineage_transport_config", load_config)
    monkeypatch.setattr(apply_command, "create_openlineage_transport", create_transport)


def _invoke(
    project: Path,
    *arguments: str,
    output: str = "json",
) -> Result:
    prefix = ["-o", output] if output != "text" else []
    return CliRunner().invoke(
        main,
        [*prefix, "apply", "-p", str(project), *arguments],
    )


def _emission_arguments(*extra: str) -> tuple[str, ...]:
    return (
        "--emit-openlineage",
        "--openlineage-job-namespace",
        _JOB_NAMESPACE,
        *extra,
    )


def _payload(result: Result) -> dict[str, object]:
    payload = json.loads(result.stdout)
    assert isinstance(payload, dict)
    return payload


def _event_types(transport: _FakeTransport) -> list[object]:
    return [event.get("eventType") for event in transport.attempts]


def _assert_apply_lifecycle(
    transport: _FakeTransport,
    intent: OperationIntent,
    terminal: str,
) -> None:
    assert _event_types(transport) == ["START", terminal]
    validate_event_sequence(transport.attempts)
    start, terminal_event = transport.attempts
    assert start["eventTime"] == intent.started_at
    assert start["run"] == {"runId": intent.operation_id}
    assert terminal_event["run"]["runId"] == intent.operation_id  # type: ignore[index]
    assert start["job"] == terminal_event["job"]
    job = start["job"]
    assert job["namespace"] == _JOB_NAMESPACE  # type: ignore[index]
    assert job["name"] == "streamt/telemetry%20project/commands/apply"  # type: ignore[index]
    job_type = job["facets"]["jobType"]  # type: ignore[index]
    assert {
        "processingType": job_type["processingType"],
        "integration": job_type["integration"],
        "jobType": job_type["jobType"],
    } == {
        "processingType": "BATCH",
        "integration": "STREAMT",
        "jobType": "COMMAND",
    }
    assert UUID(intent.operation_id).version == 4
    for event in transport.attempts:
        assert "inputs" not in event
        assert "outputs" not in event
    if terminal == "FAIL":
        failure = terminal_event["run"]["facets"]["errorMessage"]  # type: ignore[index]
        assert failure["message"] == "streamt apply command did not complete successfully"
        assert failure["programmingLanguage"] == "PYTHON"
        assert "stackTrace" not in failure
    else:
        assert "facets" not in terminal_event["run"]  # type: ignore[operator]


def _create_reviewed_plan(
    project: Path,
    manifest: Manifest,
) -> Path:
    plan_path = project / "reviewed.plan.json"
    with (
        patch("streamt.compiler.Compiler.compile", return_value=manifest),
        patch("streamt.cli.commands.plan.make_kafka_deployer", return_value=_kafka()),
    ):
        result = CliRunner().invoke(
            main,
            [
                "-o",
                "json",
                "plan",
                "-p",
                str(project),
                "--out",
                str(plan_path),
            ],
        )
    assert result.exit_code == 0, result.output
    return plan_path


def test_help_exposes_only_the_explicit_apply_runtime_options() -> None:
    result = CliRunner().invoke(main, ["apply", "--help"])

    assert result.exit_code == 0
    for option in (
        "--emit-openlineage",
        "--openlineage-job-namespace",
        "--openlineage-kafka-namespace",
        "--openlineage-gateway-namespace",
    ):
        assert option in result.stdout


def test_no_opt_in_never_reads_openlineage_environment_or_transport(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_project(tmp_path)
    monkeypatch.setenv("OPENLINEAGE_DISABLED", "not-a-boolean")
    monkeypatch.setenv("OPENLINEAGE_CONFIG", "/private/missing/openlineage.yml")
    monkeypatch.setenv("OPENLINEAGE_URL", "https://user:secret@example.invalid")
    monkeypatch.setattr(
        apply_command,
        "load_openlineage_transport_config",
        lambda *_args, **_kwargs: pytest.fail("apply loaded OpenLineage without opt-in"),
    )
    kafka = _kafka()

    with (
        patch("streamt.compiler.Compiler.compile", return_value=_topic_manifest()),
        patch("streamt.cli.commands.apply.make_kafka_deployer", return_value=kafka),
    ):
        result = _invoke(tmp_path)

    assert result.exit_code == 0, result.output
    assert _payload(result)["data"]["committed"] is True  # type: ignore[index]
    assert LocalState.load(local_state_path(tmp_path, environment="default")).serial == 1
    kafka.apply_topic.assert_called_once()


def test_direct_apply_uses_exact_durable_identity_and_order(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_project(tmp_path)
    events: list[str] = []
    intents: list[OperationIntent] = []
    transport = _FakeTransport(events)
    _install_transports(monkeypatch, transport)
    kafka = _kafka()
    kafka.apply_topic.side_effect = (
        lambda _artifact: events.append("provider-mutation") or "created"
    )
    service = _recording_state_service(tmp_path, events, intents)
    original_flush = OutputFormatter.flush

    def record_flush(formatter: OutputFormatter) -> None:
        events.append("formatter-flush")
        original_flush(formatter)

    with (
        patch("streamt.compiler.Compiler.compile", return_value=_topic_manifest()),
        patch("streamt.cli.commands.apply.make_kafka_deployer", return_value=kafka),
        patch("streamt.cli.commands.apply.make_deployment_state_service", return_value=service),
        patch("streamt.output.OutputFormatter.flush", new=record_flush),
    ):
        result = _invoke(tmp_path, *_emission_arguments())

    assert result.exit_code == 0, result.output
    assert len(intents) == 1
    _assert_apply_lifecycle(transport, intents[0], "COMPLETE")
    assert events.index("operation-begin-durable") < events.index("openlineage-START")
    assert events.index("openlineage-START") < events.index("progress-started-durable")
    assert events.index("progress-started-durable") < events.index("provider-mutation")
    assert events.index("provider-mutation") < events.index("progress-completed-durable")
    assert events.index("progress-completed-durable") < events.index(
        "operation-commit-and-clear-durable"
    )
    assert events.index("operation-commit-and-clear-durable") < events.index("openlineage-COMPLETE")
    assert events.index("openlineage-COMPLETE") < events.index("state-authority-released")
    assert events.index("state-authority-released") < events.index("openlineage-close")
    assert events.index("openlineage-close") < events.index("formatter-flush")
    assert transport.close_calls == 1


def test_zero_action_and_repeated_apply_emit_fresh_durable_pairs(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_project(tmp_path)
    events: list[str] = []
    intents: list[OperationIntent] = []
    first = _FakeTransport(events)
    second = _FakeTransport(events)
    _install_transports(monkeypatch, first, second)
    service = _recording_state_service(tmp_path, events, intents)

    with (
        patch("streamt.compiler.Compiler.compile", return_value=_empty_manifest()),
        patch("streamt.cli.commands.apply.make_kafka_deployer", return_value=_kafka()),
        patch("streamt.cli.commands.apply.make_deployment_state_service", return_value=service),
    ):
        first_result = _invoke(tmp_path, *_emission_arguments())
        second_result = _invoke(tmp_path, *_emission_arguments())

    assert first_result.exit_code == second_result.exit_code == 0
    assert len(intents) == 2
    assert intents[0].actions == intents[1].actions == ()
    assert intents[0].operation_id != intents[1].operation_id
    _assert_apply_lifecycle(first, intents[0], "COMPLETE")
    _assert_apply_lifecycle(second, intents[1], "COMPLETE")


def test_reviewed_apply_emits_the_same_exact_durable_pair(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_project(tmp_path)
    manifest = _topic_manifest()
    plan_path = _create_reviewed_plan(tmp_path, manifest)
    events: list[str] = []
    intents: list[OperationIntent] = []
    transport = _FakeTransport(events)
    _install_transports(monkeypatch, transport)
    service = _recording_state_service(tmp_path, events, intents)

    with (
        patch("streamt.compiler.Compiler.compile", return_value=manifest),
        patch("streamt.cli.commands.apply.make_kafka_deployer", return_value=_kafka()),
        patch("streamt.cli.commands.apply.make_deployment_state_service", return_value=service),
    ):
        result = _invoke(tmp_path, *_emission_arguments("--plan", str(plan_path)))

    assert result.exit_code == 0, result.output
    assert intents[0].reviewed_plan_checksum is not None
    _assert_apply_lifecycle(transport, intents[0], "COMPLETE")
    serialized = json.dumps(transport.attempts, sort_keys=True)
    assert str(plan_path) not in serialized
    assert intents[0].reviewed_plan_checksum not in serialized


@pytest.mark.parametrize("reviewed", [False, True], ids=["direct", "reviewed"])
def test_runtime_failure_emits_fail_only_after_recovery_is_durable(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    reviewed: bool,
) -> None:
    _write_project(tmp_path)
    manifest = _topic_manifest()
    plan_path = _create_reviewed_plan(tmp_path, manifest) if reviewed else None
    events: list[str] = []
    intents: list[OperationIntent] = []
    transport = _FakeTransport(events)
    _install_transports(monkeypatch, transport)
    kafka = _kafka(failure=RuntimeError(_FAILURE_SECRET))
    service = _recording_state_service(tmp_path, events, intents)
    extra = ("--plan", str(plan_path)) if plan_path is not None else ()

    with (
        patch("streamt.compiler.Compiler.compile", return_value=manifest),
        patch("streamt.cli.commands.apply.make_kafka_deployer", return_value=kafka),
        patch("streamt.cli.commands.apply.make_deployment_state_service", return_value=service),
    ):
        result = _invoke(tmp_path, *_emission_arguments(*extra))

    assert result.exit_code == 1
    assert _payload(result)["errors"][0]["code"] == "E407_DEPLOY_ERROR"  # type: ignore[index]
    _assert_apply_lifecycle(transport, intents[0], "FAIL")
    assert events.index("recovery-durable") < events.index("openlineage-FAIL")
    control = (
        make_deployment_state_service(
            tmp_path,
            project="telemetry project",
            environment="default",
            config=local_deployment_state_config(),
        )
        .read_control()
        .control
    )
    assert control.status == "recovery_required"
    assert control.intent is not None
    assert control.intent.operation_id == intents[0].operation_id
    assert control.recovery is not None
    assert control.recovery.failure_code == "runtime_action_failed"
    assert _FAILURE_SECRET not in json.dumps(transport.attempts)


def test_keyboard_interrupt_emits_abort_and_preserves_recovery_and_130(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_project(tmp_path)
    events: list[str] = []
    intents: list[OperationIntent] = []
    transport = _FakeTransport(events)
    _install_transports(monkeypatch, transport)
    service = _recording_state_service(tmp_path, events, intents)

    with (
        patch("streamt.compiler.Compiler.compile", return_value=_topic_manifest()),
        patch(
            "streamt.cli.commands.apply.make_kafka_deployer",
            return_value=_kafka(failure=KeyboardInterrupt()),
        ),
        patch("streamt.cli.commands.apply.make_deployment_state_service", return_value=service),
    ):
        result = _invoke(tmp_path, *_emission_arguments())

    assert result.exit_code == 130
    _assert_apply_lifecycle(transport, intents[0], "ABORT")
    assert events.index("recovery-durable") < events.index("openlineage-ABORT")
    control = (
        make_deployment_state_service(
            tmp_path,
            project="telemetry project",
            environment="default",
            config=local_deployment_state_config(),
        )
        .read_control()
        .control
    )
    assert control.status == "recovery_required"
    assert control.recovery is not None
    assert control.recovery.failure_code == "operation_interrupted"


@pytest.mark.parametrize("exit_kind", ["dry-run", "invalid-reviewed-plan", "recovery-blocker"])
def test_representative_pre_intent_exits_never_prepare_or_emit_openlineage(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    exit_kind: str,
) -> None:
    _write_project(tmp_path)
    manifest = _topic_manifest()
    arguments: tuple[str, ...] = _emission_arguments()
    if exit_kind == "dry-run":
        arguments = (*arguments, "--dry-run")
    elif exit_kind == "invalid-reviewed-plan":
        invalid_plan = tmp_path / "invalid.plan.json"
        invalid_plan.write_text("{}", encoding="utf-8")
        arguments = (*arguments, "--plan", str(invalid_plan))
    else:
        state_service = make_deployment_state_service(
            tmp_path,
            project="telemetry project",
            environment="default",
            config=local_deployment_state_config(),
        )
        with state_service.operation() as operation:
            snapshot = operation.observe()
            intent = OperationIntent(
                operation_id="00000000-0000-4000-8000-000000000111",
                kind="apply",
                started_at=operation_timestamp(),
                actor="test",
                prior_state_serial=snapshot.state.state.serial,
                prior_state_checksum=state_checksum(snapshot.state.state),
                reviewed_plan_checksum=None,
                actions=(),
            )
            active = operation.begin_operation(snapshot, intent)
            operation.mark_recovery_required(
                active,
                RecoveryRecord(
                    operation_id=intent.operation_id,
                    failure_code="test_blocker",
                    failed_at=operation_timestamp(),
                    last_completed_action_index=None,
                ),
            )

    monkeypatch.setattr(
        apply_command,
        "load_openlineage_transport_config",
        lambda *_args, **_kwargs: pytest.fail("pre-intent exit prepared OpenLineage"),
    )
    kafka = _kafka()
    with (
        patch("streamt.compiler.Compiler.compile", return_value=manifest),
        patch("streamt.cli.commands.apply.make_kafka_deployer", return_value=kafka),
    ):
        result = _invoke(tmp_path, *arguments)

    if exit_kind == "dry-run":
        assert result.exit_code == 0, result.output
    else:
        assert result.exit_code == 1
    kafka.apply_topic.assert_not_called()
    if exit_kind != "recovery-blocker":
        assert not local_control_path(tmp_path, environment="default").exists()


@pytest.mark.parametrize(
    "failure_kind",
    ["namespace", "event", "transport-config", "transport-create"],
)
def test_e506_preflight_failure_precedes_durable_begin_and_provider_mutation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    failure_kind: str,
) -> None:
    _write_project(tmp_path)
    events: list[str] = []
    intents: list[OperationIntent] = []
    service = _recording_state_service(tmp_path, events, intents)
    arguments = _emission_arguments()
    if failure_kind == "namespace":
        arguments = ("--emit-openlineage",)
    elif failure_kind == "event":
        monkeypatch.setattr(
            apply_command,
            "build_run_event",
            lambda **_kwargs: (_ for _ in ()).throw(
                OpenLineageConstructionError("event password=event-secret")
            ),
        )
    elif failure_kind == "transport-config":
        monkeypatch.setattr(
            apply_command,
            "load_openlineage_transport_config",
            lambda *_args, **_kwargs: (_ for _ in ()).throw(
                OpenLineageTransportConfigurationError(
                    "transport configuration is invalid",
                    location="openlineage.transport",
                )
            ),
        )
    else:
        monkeypatch.setattr(
            apply_command,
            "load_openlineage_transport_config",
            lambda *_args, **_kwargs: object(),
        )
        monkeypatch.setattr(
            apply_command,
            "create_openlineage_transport",
            lambda _config: (_ for _ in ()).throw(RuntimeError(_TRANSPORT_SECRET)),
        )

    kafka = _kafka()
    with (
        patch("streamt.compiler.Compiler.compile", return_value=_topic_manifest()),
        patch("streamt.cli.commands.apply.make_kafka_deployer", return_value=kafka),
        patch("streamt.cli.commands.apply.make_deployment_state_service", return_value=service),
    ):
        result = _invoke(tmp_path, *arguments)

    assert result.exit_code == 1
    assert _payload(result)["errors"][0]["code"] == "E506_OPENLINEAGE_INVALID"  # type: ignore[index]
    assert intents == []
    assert "operation-begin-durable" not in events
    kafka.apply_topic.assert_not_called()
    assert not local_control_path(tmp_path, environment="default").exists()
    assert "event-secret" not in result.output
    assert "apply-transport-secret" not in result.output


@pytest.mark.parametrize(
    ("fail_attempts", "fail_close", "warning_locations"),
    [
        ({1}, False, {"openlineage.start"}),
        ({2}, False, {"openlineage.terminal"}),
        (set(), True, {"openlineage.transport"}),
    ],
)
def test_delivery_failures_add_w112_without_changing_success_or_state(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    fail_attempts: set[int],
    fail_close: bool,
    warning_locations: set[str],
) -> None:
    _write_project(tmp_path)
    events: list[str] = []
    intents: list[OperationIntent] = []
    transport = _FakeTransport(
        events,
        fail_attempts=fail_attempts,
        fail_close=fail_close,
    )
    _install_transports(monkeypatch, transport)
    service = _recording_state_service(tmp_path, events, intents)
    kafka = _kafka()

    with (
        patch("streamt.compiler.Compiler.compile", return_value=_topic_manifest()),
        patch("streamt.cli.commands.apply.make_kafka_deployer", return_value=kafka),
        patch("streamt.cli.commands.apply.make_deployment_state_service", return_value=service),
    ):
        result = _invoke(tmp_path, *_emission_arguments())

    assert result.exit_code == 0, result.output
    payload = _payload(result)
    assert payload["status"] == "ok"
    assert payload["data"]["committed"] is True  # type: ignore[index]
    warnings = [
        warning
        for warning in payload["warnings"]  # type: ignore[union-attr]
        if warning["code"] == "W112_OPENLINEAGE_EMIT_FAILED"
    ]
    assert {warning["location"] for warning in warnings} == warning_locations
    assert _TRANSPORT_SECRET not in json.dumps(payload)
    _assert_apply_lifecycle(transport, intents[0], "COMPLETE")
    assert LocalState.load(local_state_path(tmp_path, environment="default")).serial == 1
    kafka.apply_topic.assert_called_once()


def test_terminal_and_close_failure_never_replace_runtime_failure_or_recovery(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_project(tmp_path)
    events: list[str] = []
    intents: list[OperationIntent] = []
    transport = _FakeTransport(events, fail_attempts={2}, fail_close=True)
    _install_transports(monkeypatch, transport)
    service = _recording_state_service(tmp_path, events, intents)

    with (
        patch("streamt.compiler.Compiler.compile", return_value=_topic_manifest()),
        patch(
            "streamt.cli.commands.apply.make_kafka_deployer",
            return_value=_kafka(failure=RuntimeError(_FAILURE_SECRET)),
        ),
        patch("streamt.cli.commands.apply.make_deployment_state_service", return_value=service),
    ):
        result = _invoke(tmp_path, *_emission_arguments())

    assert result.exit_code == 1
    payload = _payload(result)
    assert payload["errors"][0]["code"] == "E407_DEPLOY_ERROR"  # type: ignore[index]
    assert [
        warning["location"]
        for warning in payload["warnings"]  # type: ignore[union-attr]
        if warning["code"] == "W112_OPENLINEAGE_EMIT_FAILED"
    ] == ["openlineage.terminal", "openlineage.transport"]
    _assert_apply_lifecycle(transport, intents[0], "FAIL")
    control = (
        make_deployment_state_service(
            tmp_path,
            project="telemetry project",
            environment="default",
            config=local_deployment_state_config(),
        )
        .read_control()
        .control
    )
    assert control.status == "recovery_required"


def test_unknown_commit_emits_fail_after_conservative_recovery(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_project(tmp_path)
    events: list[str] = []
    intents: list[OperationIntent] = []
    transport = _FakeTransport(events)
    _install_transports(monkeypatch, transport)
    service = _recording_state_service(tmp_path, events, intents, fail_commit=True)

    with (
        patch("streamt.compiler.Compiler.compile", return_value=_topic_manifest()),
        patch("streamt.cli.commands.apply.make_kafka_deployer", return_value=_kafka()),
        patch("streamt.cli.commands.apply.make_deployment_state_service", return_value=service),
    ):
        result = _invoke(tmp_path, *_emission_arguments())

    assert result.exit_code == 1
    assert _payload(result)["errors"][0]["code"] == "E425_STATE_UNKNOWN_OUTCOME"  # type: ignore[index]
    _assert_apply_lifecycle(transport, intents[0], "FAIL")
    assert events.index("recovery-durable") < events.index("openlineage-FAIL")
    control = (
        make_deployment_state_service(
            tmp_path,
            project="telemetry project",
            environment="default",
            config=local_deployment_state_config(),
        )
        .read_control()
        .control
    )
    assert control.status == "recovery_required"
    assert control.recovery is not None
    assert control.recovery.failure_code == "state_commit_uncertain"


def test_lock_loss_after_start_emits_fail_and_keeps_blocking_intent(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_project(tmp_path)
    events: list[str] = []
    intents: list[OperationIntent] = []
    transport = _FakeTransport(events)
    _install_transports(monkeypatch, transport)
    service = _recording_state_service(
        tmp_path,
        events,
        intents,
        lose_lock_after_begin=True,
    )
    kafka = _kafka()

    with (
        patch("streamt.compiler.Compiler.compile", return_value=_topic_manifest()),
        patch("streamt.cli.commands.apply.make_kafka_deployer", return_value=kafka),
        patch("streamt.cli.commands.apply.make_deployment_state_service", return_value=service),
    ):
        result = _invoke(tmp_path, *_emission_arguments())

    assert result.exit_code == 1
    assert _payload(result)["errors"][0]["code"] == "E423_STATE_LOCK_LOST"  # type: ignore[index]
    _assert_apply_lifecycle(transport, intents[0], "FAIL")
    kafka.apply_topic.assert_not_called()
    control = (
        make_deployment_state_service(
            tmp_path,
            project="telemetry project",
            environment="default",
            config=local_deployment_state_config(),
        )
        .read_control()
        .control
    )
    assert control.status == "in_progress"


def test_verified_release_failure_keeps_complete_and_e426_after_commit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_project(tmp_path)
    events: list[str] = []
    intents: list[OperationIntent] = []
    transport = _FakeTransport(events)
    _install_transports(monkeypatch, transport)
    service = _recording_state_service(tmp_path, events, intents, fail_release=True)

    with (
        patch("streamt.compiler.Compiler.compile", return_value=_topic_manifest()),
        patch("streamt.cli.commands.apply.make_kafka_deployer", return_value=_kafka()),
        patch("streamt.cli.commands.apply.make_deployment_state_service", return_value=service),
    ):
        result = _invoke(tmp_path, *_emission_arguments())

    assert result.exit_code == 1
    payload = _payload(result)
    assert payload["errors"][0]["code"] == "E426_STATE_RELEASE_FAILED_AFTER_COMMIT"  # type: ignore[index]
    assert payload["data"]["committed"] is True  # type: ignore[index]
    _assert_apply_lifecycle(transport, intents[0], "COMPLETE")
    assert events.index("operation-commit-and-clear-durable") < events.index("openlineage-COMPLETE")
    assert events.index("openlineage-COMPLETE") < events.index("state-authority-released")
    assert LocalState.load(local_state_path(tmp_path, environment="default")).serial == 1


def test_events_and_structured_output_omit_apply_secrets_and_infrastructure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_project(tmp_path)
    events: list[str] = []
    intents: list[OperationIntent] = []
    transport = _FakeTransport(events)
    _install_transports(monkeypatch, transport)
    service = _recording_state_service(tmp_path, events, intents)

    with (
        patch("streamt.compiler.Compiler.compile", return_value=_topic_manifest()),
        patch("streamt.cli.commands.apply.make_kafka_deployer", return_value=_kafka()),
        patch("streamt.cli.commands.apply.make_deployment_state_service", return_value=service),
    ):
        result = _invoke(
            tmp_path,
            *_emission_arguments(
                "--openlineage-kafka-namespace",
                "kafka://catalog-kafka.example:9092",
                "--openlineage-gateway-namespace",
                "kafka://catalog-gateway.example:6969",
            ),
        )

    assert result.exit_code == 0, result.output
    rendered_events = json.dumps(transport.attempts, sort_keys=True)
    rendered_output = json.dumps(_payload(result), sort_keys=True)
    intent = intents[0]
    for forbidden in (
        _RUNTIME_SECRET,
        _TOPIC_NAME,
        "private_orders_model",
        "catalog-kafka.example",
        "catalog-gateway.example",
        str(tmp_path),
        str(local_state_path(tmp_path, environment="default")),
        intent.prior_state_checksum,
        "direct-kafka",
        "streamt://",
    ):
        assert forbidden not in rendered_events
    assert "inputs" not in rendered_events
    assert "outputs" not in rendered_events
    assert _RUNTIME_SECRET not in rendered_output
    assert _TRANSPORT_SECRET not in rendered_output
