"""Public CLI, real parser/compiler/plan files/local journal, fake exact runtime."""

from __future__ import annotations

import json
import uuid
from dataclasses import replace
from types import SimpleNamespace

import pytest
import yaml
from click.testing import CliRunner

from streamt.cli import main
from streamt.cli.commands import runner_state
from streamt.cli.commands.state_cmd import state as state_group
from streamt.compiler import Compiler
from streamt.core.parser import ProjectParser
from streamt.deployer.kafka_streams_replacement_coordinator import (
    KafkaStreamsReplacementCoordinator,
)
from streamt.deployer.kafka_streams_replacement_executor import ReplacementExecutionState
from streamt.deployer.plan_file import ReviewedPlanFile, StateReference
from streamt.deployer.state_backend import (
    LocalDeploymentStateBackend,
    OperationResumeRecord,
    RecoveryRecord,
    StateBackendUnknownCommitError,
    _LocalDeploymentStateOperation,
    operation_timestamp,
)
from tests.unit.test_kafka_streams_operation_evidence import (
    ADDRESS,
    OPERATION,
    _boundaries,
    _intent,
)
from tests.unit.test_kafka_streams_replacement_coordinator import project
from tests.unit.test_kafka_streams_replacement_executor import World
from tests.unit.test_kafka_streams_resume_local import _backend
from tests.unit.test_kafka_streams_reviewed_plan import _actions, _plan


def _forbid(*args, **kwargs):
    pytest.fail("Unrelated provider, new planning, or forbidden write")


@pytest.fixture
def fixture(tmp_path, monkeypatch):
    current = project()
    data = current.model_dump(mode="json", exclude_none=True)
    declaration = {key: data[key] for key in ("project", "sources", "models")}
    path = tmp_path / "stream_project.yml"
    path.write_text(yaml.safe_dump(declaration))
    environments = tmp_path / "environments"
    environments.mkdir()
    env_path = environments / "prod.yml"
    env_path.write_text(yaml.safe_dump({
        "environment": {"name": "prod"}, "runtime": data["runtime"],
        "safety": {"confirm_apply": False},
    }))
    parsed = ProjectParser(tmp_path, environment="prod").parse()
    backend = _backend(tmp_path)
    with backend.operation(ADDRESS) as operation:
        snapshot = operation.observe()
    reviewed = ReviewedPlanFile.create(
        _plan(), Compiler(parsed).compile(dry_run=True), project="payments", environment="prod",
        runtime=parsed.runtime, state=StateReference.from_observation(snapshot.state), actions=_actions(),
    )
    plan_path = tmp_path / "original.plan.json"
    reviewed.save(plan_path)
    events = []
    original_check = _LocalDeploymentStateOperation.check_lock
    def check_lock(operation):
        original_check(operation)
        events.append("lock")
    monkeypatch.setattr(_LocalDeploymentStateOperation, "check_lock", check_lock)
    world = World(events)
    factories = []

    def deployer(*args, **kwargs):
        factories.append("runner")
        return world.runtime

    monkeypatch.setattr(runner_state, "KafkaStreamsDeployer", deployer)
    monkeypatch.setattr(runner_state, "KafkaStreamsReplacementObserver", lambda runtime: world.observer)
    monkeypatch.setattr(world.runtime, "close", lambda: None)
    for name in ("make_kafka_deployer", "make_sr_deployer", "make_flink_deployer", "make_connect_deployer", "make_gateway_deployer"):
        monkeypatch.setattr("streamt.cli.helpers." + name, _forbid)
    monkeypatch.setattr("streamt.deployer.planner.DeploymentPlanner.__init__", _forbid)
    runner_state.register_runner_state_commands(state_group)
    return SimpleNamespace(path=tmp_path, declaration=path, env_path=env_path, backend=backend,
                           plan=reviewed, plan_path=plan_path, world=world, events=events, factories=factories)


def begin(fixture, prefix=0, *, blocked=True, failed=False):
    with fixture.backend.operation(ADDRESS) as operation:
        before = operation.observe()
        intent = replace(_intent(before.state.state), reviewed_plan_checksum=fixture.plan.checksum)
        snapshot = operation.begin_operation(before, intent)
        for boundary in _boundaries(exit_code=143)[:prefix]:
            if failed and boundary.status == "completed":
                boundary = replace(boundary, succeeded=False)
            snapshot = operation.record_progress(snapshot, boundary)
        if blocked:
            snapshot = operation.mark_recovery_required(snapshot, RecoveryRecord(OPERATION, "test_interrupted", operation_timestamp(), 0 if prefix == 5 and not failed else None))
    fixture.world.old = "running" if prefix < 2 else "exited" if prefix == 2 else None
    fixture.world.candidate = "created" if prefix == 4 else "running" if prefix == 5 else None
    return snapshot


def files(fixture):
    return {str(path.relative_to(fixture.path)): path.read_bytes() for path in fixture.path.rglob("*") if path.is_file()}


def invoke(fixture, command="runner-status", *extra, operation_id=OPERATION):
    return CliRunner().invoke(main, ["--output", "json", "state", command, "--plan", str(fixture.plan_path),
                                  "--operation-id", operation_id, "-p", str(fixture.path), "--env", "prod", *extra])


@pytest.mark.parametrize("prefix", range(6))
@pytest.mark.parametrize("blocked", [False, True])
def test_status_reads_each_real_durable_frontier_without_changing_any_file_or_provider(fixture, prefix, blocked):
    begin(fixture, prefix, blocked=blocked)
    before = files(fixture)
    result = invoke(fixture)
    assert result.exit_code == 0, result.output
    report = json.loads(result.output)["data"]
    assert report["status"] == ("ready_to_finalize" if prefix == 5 else "pending")
    assert report["operation_id"] == OPERATION
    assert report["plan_checksum"] == fixture.plan.checksum
    assert report["committed"] is False
    assert report["read_only"] is True
    assert report["resumable"] is True
    if prefix == 3:
        assert report["next_step"] == "create_candidate"
    assert files(fixture) == before
    assert not fixture.world.commands
    assert not fixture.world.creates
    assert fixture.factories == ["runner"]


@pytest.mark.parametrize("prefix", range(6))
def test_public_resume_finalizes_same_operation_then_repeat_and_status_are_read_only(fixture, prefix):
    begin(fixture, prefix)
    result = invoke(fixture, "resume")
    assert result.exit_code == 0, result.output
    report = json.loads(result.output)["data"]
    assert report["status"] == "completed"
    assert report["committed"] is True
    assert report["operation_id"] == OPERATION
    assert report["state_serial"] == 2
    with LocalDeploymentStateBackend(fixture.path).operation(ADDRESS) as operation:
        snapshot = operation.observe()
        receipt = operation.completed_runner_receipt(snapshot, OPERATION)
        assert receipt.control.intent.reviewed_plan_checksum == fixture.plan.checksum
        assert receipt.control.progress[1].kafka_streams_checkpoint.exit_code == 143
    before = files(fixture)
    writes = (list(fixture.world.commands), list(fixture.world.creates))
    for command in ("runner-status", "resume"):
        result = invoke(fixture, command)
        assert result.exit_code == 0, result.output
        report = json.loads(result.output)["data"]
        assert report["status"] == "completed"
        assert report["read_only"] is True
        assert report["resumable"] is False
    assert files(fixture) == before
    assert (fixture.world.commands, fixture.world.creates) == writes


@pytest.mark.parametrize("command", ["runner-status", "resume"])
@pytest.mark.parametrize("damage", ["operation", "uuid", "missing_plan", "modified_plan", "sql", "state", "clear_without_receipt", "failed"])
def test_static_or_storage_invalid_inputs_fail_before_runner_factory_and_all_writes(fixture, command, damage):
    if damage != "clear_without_receipt":
        begin(fixture, 5 if damage == "failed" else 3, failed=damage == "failed")
    identity = OPERATION
    if damage == "operation":
        identity = str(uuid.uuid4())
    elif damage == "uuid":
        identity = "not-a-canonical-uuid"
    elif damage == "missing_plan":
        fixture.plan_path = fixture.path / "missing-private-secret.plan.json"
    elif damage == "modified_plan":
        payload = json.loads(fixture.plan_path.read_text())
        payload["checksum"] = "sha256:" + "f" * 64
        fixture.plan_path.write_text(json.dumps(payload))
    elif damage == "sql":
        data = yaml.safe_load(fixture.declaration.read_text())
        data["models"][0]["sql"] += " AND amount < 90"
        fixture.declaration.write_text(yaml.safe_dump(data))
    elif damage == "state":
        changed = fixture.backend.read(ADDRESS).state
        changed.serial += 1
        changed.save(fixture.backend._path(ADDRESS))
    before = files(fixture)
    result = invoke(fixture, command, operation_id=identity)
    if damage == "failed" and command == "runner-status":
        assert result.exit_code == 0, result.output
        assert json.loads(result.output)["data"]["status"] == "blocked"
    else:
        assert result.exit_code == 1, result.output
    assert files(fixture) == before
    assert not fixture.world.commands
    assert not fixture.world.creates
    if damage != "failed":
        assert not fixture.factories
    assert "missing-private-secret" not in result.output


@pytest.mark.parametrize("policy", ["protected", "confirm_apply", "require_remote_state"])
def test_resume_honors_effective_environment_policy_before_runner_or_state_writes(fixture, policy):
    begin(fixture, 3)
    data = yaml.safe_load(fixture.env_path.read_text())
    if policy == "protected":
        data["environment"]["protected"] = True
    else:
        data["safety"][policy] = True
    fixture.env_path.write_text(yaml.safe_dump(data))
    before = files(fixture)
    result = invoke(fixture, "resume")
    assert result.exit_code == 1, result.output
    assert files(fixture) == before
    assert not fixture.factories
    assert not fixture.world.commands
    assert not fixture.world.creates
    if policy != "require_remote_state":
        wrong = invoke(fixture, "resume", "--confirm-env", "dev")
        assert wrong.exit_code == 1
        accepted = invoke(fixture, "resume", "--confirm-env", "prod")
        assert accepted.exit_code == 0, accepted.output


def test_read_only_runtime_failure_redacts_secrets_and_changes_no_journal(fixture):
    begin(fixture, 3)
    before = files(fixture)
    fixture.world.observer.observe.side_effect = ValueError("private-password provider://host/path?token=secret")
    result = invoke(fixture)
    assert result.exit_code == 1
    assert "private-password" not in result.output
    assert "provider://" not in result.output
    assert json.loads(result.stdout)["data"]["committed"] is None
    assert files(fixture) == before


@pytest.mark.parametrize("timeout", ["0", "-1", "601", "nan", "inf"])
def test_invalid_timeout_never_constructs_a_provider(fixture, timeout):
    begin(fixture, 3)
    before = files(fixture)
    result = invoke(fixture, "resume", "--timeout", timeout)
    assert result.exit_code != 0
    assert not fixture.factories
    assert files(fixture) == before


@pytest.mark.parametrize("fault", ["before_begin", "lost_begin_ack", "callback"])
def test_execute_callback_runs_only_after_valid_intent_ack_without_runtime_on_failure(fixture, monkeypatch, fault):
    called = []
    coordinator = KafkaStreamsReplacementCoordinator(fixture.world.observer, lambda: ProjectParser(fixture.path, environment="prod").parse())
    with fixture.backend.operation(ADDRESS) as operation:
        holder = ReplacementExecutionState(operation.observe())
        if fault == "before_begin":
            callback = "not callable"
        else:
            def callback():
                called.append(holder.snapshot.control.control.status)
                raise ValueError("delivery failed")
        if fault == "lost_begin_ack":
            original = operation.begin_operation
            def lost(*args, **kwargs):
                original(*args, **kwargs)
                raise StateBackendUnknownCommitError("lost intent acknowledgement")
            monkeypatch.setattr(operation, "begin_operation", lost)
        with pytest.raises((ValueError, StateBackendUnknownCommitError)):
            coordinator.execute(operation, holder, plan=fixture.plan, current_plan=_plan(), current_actions=_actions(),
                                operation_id=OPERATION, actor="cli-test", on_started=callback)
    assert called == (["in_progress"] if fault == "callback" else [])
    assert not fixture.world.commands
    assert not fixture.world.creates
    assert not fixture.world.read_count


def test_prearchived_resume_is_reported_without_writes_then_exactly_reused(fixture, monkeypatch):
    snapshot = begin(fixture, 3)
    authorization = OperationResumeRecord.create(snapshot, resume_id=str(uuid.uuid4()), actor="original-operator", resumed_at=operation_timestamp())
    with fixture.backend.operation(ADDRESS) as operation, monkeypatch.context() as patch:
        patch.setattr(fixture.backend, "_write_control", lambda *_a, **_k: (_ for _ in ()).throw(StateBackendUnknownCommitError("before active control")))
        with pytest.raises(StateBackendUnknownCommitError):
            operation.resume_operation(operation.observe(), authorization)
    before = files(fixture)
    result = invoke(fixture)
    assert result.exit_code == 0, result.output
    assert files(fixture) == before
    result = invoke(fixture, "resume")
    assert result.exit_code == 0, result.output
    receipt = fixture.backend._read_recovery_history(ADDRESS).completion_for(OPERATION)
    assert receipt.control.resume_history == (authorization,)


@pytest.mark.parametrize("after_clear", [False, True])
def test_lost_clear_ack_reports_unknown_then_fresh_cli_proves_exact_completion(fixture, monkeypatch, after_clear):
    begin(fixture, 5)
    original = LocalDeploymentStateBackend._write_control
    def lose_clear(path, control, *, operation_id):
        if control.status == "clear":
            if after_clear:
                original(path, control, operation_id=operation_id)
            raise StateBackendUnknownCommitError("lost clear acknowledgement", operation_id=operation_id)
        return original(path, control, operation_id=operation_id)
    with monkeypatch.context() as patch:
        patch.setattr(LocalDeploymentStateBackend, "_write_control", staticmethod(lose_clear))
        result = invoke(fixture, "resume")
    assert result.exit_code == 1
    report = json.loads(result.stdout)["data"]
    assert report["committed"] is None
    assert report["lifecycle_phase"] == "completed"
    assert report["operation_id"] == OPERATION
    state_before = fixture.backend.read(ADDRESS)
    assert state_before.state.serial == 2
    audit_before = fixture.backend._read_recovery_history(ADDRESS)
    fixture.world.commands.clear()
    fixture.world.creates.clear()
    before = files(fixture)
    result = invoke(fixture)
    assert result.exit_code == 0, result.output
    diagnostic = json.loads(result.stdout)["data"]
    assert diagnostic["status"] == ("completed" if after_clear else "ready_to_finalize")
    assert diagnostic["committed"] is (True if after_clear else None)
    assert files(fixture) == before
    with monkeypatch.context() as patch:
        patch.setattr(_LocalDeploymentStateOperation, "compare_and_swap", _forbid)
        patch.setattr(_LocalDeploymentStateOperation, "resume_operation", _forbid)
        patch.setattr(LocalDeploymentStateBackend, "_write_recovery_history", _forbid)
        result = invoke(fixture, "resume")
    assert result.exit_code == 0, result.output
    assert fixture.backend.read(ADDRESS) == state_before
    assert fixture.backend._read_recovery_history(ADDRESS) == audit_before
    assert not fixture.world.commands
    assert not fixture.world.creates


@pytest.mark.parametrize("command", ["runner-status", "resume"])
@pytest.mark.parametrize("completed", [False, True])
def test_corrupt_durable_archive_is_rejected_before_any_runner_observation(fixture, command, completed):
    begin(fixture, 5 if completed else 3)
    if completed:
        result = invoke(fixture, "resume")
        assert result.exit_code == 0, result.output
    fixture.backend._recovery_history_path(ADDRESS).write_text('{"private-secret": "corrupt archive"}')
    fixture.factories.clear()
    before = files(fixture)
    result = invoke(fixture, command)
    assert result.exit_code == 1
    assert "private-secret" not in result.output
    assert files(fixture) == before
    assert not fixture.factories


def test_context_change_during_read_only_observation_prevents_resume_authorization(fixture):
    begin(fixture, 3)
    before = {key: value for key, value in files(fixture).items() if key.startswith(".streamt/")}
    def change_project():
        data = yaml.safe_load(fixture.declaration.read_text())
        data["models"][0]["sql"] += " AND amount < 90"
        fixture.declaration.write_text(yaml.safe_dump(data))
    fixture.world.after_observe = change_project
    result = invoke(fixture, "resume")
    assert result.exit_code == 1
    after = {key: value for key, value in files(fixture).items() if key.startswith(".streamt/")}
    assert after == before
    assert not fixture.world.commands
    assert not fixture.world.creates


@pytest.mark.parametrize("command", ["runner-status", "resume"])
def test_single_environment_omits_explicit_default_and_reaches_exact_operation_gate(fixture, command):
    data = yaml.safe_load(fixture.declaration.read_text())
    data["runtime"] = yaml.safe_load(fixture.env_path.read_text())["runtime"]
    fixture.declaration.write_text(yaml.safe_dump(data))
    fixture.env_path.parent.rename(fixture.path / "unused-environment-fixture")
    before = files(fixture)
    result = CliRunner().invoke(main, ["--output", "json", "state", command, "--plan", str(fixture.plan_path),
                                     "--operation-id", OPERATION, "-p", str(fixture.path)])
    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["errors"][0]["code"] == "E419_STATE_RECOVERY_REQUIRED"
    assert not fixture.factories
    # Acquiring local storage authority can create its lock, never state/control.
    added = set(files(fixture)) - set(before)
    assert all(name.endswith(".lock") for name in added)
    assert all(files(fixture)[name] == value for name, value in before.items())


@pytest.mark.parametrize("invalid_validated_read", [False, True])
def test_lazy_runner_factory_consumes_exact_last_validated_context_without_an_extra_parse(fixture, monkeypatch, invalid_validated_read):
    begin(fixture, 3)
    original = ProjectParser.parse
    parsed = []
    constructed_at = []
    original_factory = runner_state.KafkaStreamsDeployer
    def parse(parser):
        current = original(parser)
        parsed.append(current)
        if invalid_validated_read and len(parsed) == 3:
            current.models[0].sql += " AND amount < 90"
        return current
    def create(config, kafka, **kwargs):
        constructed_at.append(len(parsed))
        assert config is parsed[-1].runtime.kafka_streams
        assert kafka is parsed[-1].runtime.kafka
        return original_factory(config, kafka, **kwargs)
    monkeypatch.setattr(ProjectParser, "parse", parse)
    monkeypatch.setattr(runner_state, "KafkaStreamsDeployer", create)
    before = files(fixture)
    result = invoke(fixture)
    assert result.exit_code == (1 if invalid_validated_read else 0), result.output
    assert constructed_at == ([] if invalid_validated_read else [3])
    assert len(parsed) == (3 if invalid_validated_read else 4)
    assert files(fixture) == before
    assert fixture.world.read_count == (0 if invalid_validated_read else 1)


def test_error_classification_distinguishes_stale_plan_parse_and_runtime():
    from streamt.core.errors import ErrorCode
    from streamt.core.parser import EnvVarError, ParseError
    from streamt.deployer.plan_file import PlanFileError, StalePlanError
    assert runner_state._error_code(StalePlanError("stale")) == ErrorCode.PLAN_STALE
    assert runner_state._error_code(PlanFileError("invalid")) == ErrorCode.PLAN_FILE_INVALID
    assert runner_state._error_code(ParseError("parse")) == ErrorCode.PARSE_ERROR
    assert runner_state._error_code(EnvVarError("env")) == ErrorCode.PARSE_ERROR
    assert runner_state._error_code(ValueError("runtime")) == ErrorCode.DEPLOY_ERROR
