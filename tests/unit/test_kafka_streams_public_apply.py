"""Public reviewed runner apply: real parser/compiler/planner/local state, guarded runtime."""

from __future__ import annotations

import copy
import importlib
import json
import shlex
import uuid
from contextlib import contextmanager
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
import yaml
from click.testing import CliRunner

from streamt.cli import main
from streamt.compiler import Compiler
from streamt.core.parser import ProjectParser
from streamt.deployer.kafka import KafkaDeployer, TopicChange, TopicState
from streamt.deployer.kafka_streams_replacement_observer import KafkaStreamsReplacementObserver
from streamt.deployer.plan_file import ReviewedPlanFile
from streamt.deployer.planner import DeploymentPlanner
from streamt.deployer.state import (
    ManagedResourceRecord,
    artifact_checksum,
    local_state_path,
    resource_id,
)
from streamt.deployer.state_backend import (
    LocalDeploymentStateBackend,
    StateBackendReleaseAfterCommitError,
    StateBackendUnknownCommitError,
)
from tests.unit.test_kafka_streams_operation_evidence import (
    ADDRESS,
    BACKEND,
    OPERATION,
    RESOURCE,
    _evidence,
    _state,
)
from tests.unit.test_kafka_streams_replacement_coordinator import project
from tests.unit.test_kafka_streams_replacement_executor import SECRET, World
from tests.unit.test_kafka_streams_reviewed_plan import _plan
from tests.unit.test_openlineage_apply_command import _FakeTransport, _install_transports

apply_command = importlib.import_module("streamt.cli.commands.apply")
plan_command = importlib.import_module("streamt.cli.commands.plan")


@pytest.fixture
def fixture(tmp_path, monkeypatch):
    config = project().model_dump(mode="json", by_alias=True, exclude_none=True)
    config.pop("environment_name", None)
    runtime_config = config.pop("runtime")
    (tmp_path / "stream_project.yml").write_text(yaml.safe_dump(config))
    (tmp_path / "environments").mkdir()
    (tmp_path / "environments/prod.yml").write_text(yaml.safe_dump({
        "environment": {"name": "prod"}, "runtime": runtime_config,
        "safety": {"confirm_apply": False},
    }))
    current = ProjectParser(tmp_path, environment="prod").parse()
    manifest = Compiler(current).compile(dry_run=True)
    prior = _state()
    for artifact in manifest.artifacts["topics"]:
        prior.resources[resource_id("payments", "prod", "topic", artifact["ownership"]["name"])] = ManagedResourceRecord(
            artifact["name"], "managed", artifact_checksum(artifact), "direct-kafka",
        )
    prior.save(local_state_path(tmp_path, environment="prod"))
    backend = LocalDeploymentStateBackend(tmp_path)
    events = []
    world = World(events)
    runtime = world.runtime
    runtime.config, runtime.kafka, runtime.backend_identity = current.runtime.kafka_streams, current.runtime.kafka, BACKEND
    change = _plan().kafka_streams_changes[0]
    change.blocker = "kafka_streams_replacement_not_verified"
    runtime.plan_job = Mock(side_effect=lambda *_a, **_k: copy.deepcopy(change))
    runtime.close = Mock()
    kafka = Mock(spec=KafkaDeployer)
    kafka.plan_topic.side_effect = lambda desired: TopicChange(
        desired.name, "none", TopicState(desired.name, True, desired.partitions, desired.replication_factor, desired.config), desired,
    )
    kafka.get_consumer_groups.return_value = []
    prepared = Mock(return_value=_evidence())
    monkeypatch.setattr(KafkaStreamsReplacementObserver, "prepare", lambda _observer, *args: prepared(*args))
    monkeypatch.setattr(KafkaStreamsReplacementObserver, "observe", lambda _observer, *args: world.observe(*args))
    forbidden = Mock(side_effect=AssertionError("Unexpected generic mutation or provider"))
    operations = []

    @contextmanager
    def operation():
        with backend.operation(ADDRESS) as owned:
            check = owned.check_lock

            def checked():
                check()
                events.append("lock")

            monkeypatch.setattr(owned, "check_lock", checked)
            monkeypatch.setattr(owned, "commit_operation", forbidden)
            begin, finalize = owned.begin_operation, owned.finalize_completed_runner

            def begun(*args):
                acknowledged = begin(*args)
                events.append("intent-ack")
                return acknowledged

            def finalized(*args):
                acknowledged = finalize(*args)
                events.append("finalize-ack")
                return acknowledged

            monkeypatch.setattr(owned, "begin_operation", begun)
            monkeypatch.setattr(owned, "finalize_completed_runner", finalized)
            operations.append(owned)
            yield owned
        if service.release_error is not None:
            raise service.release_error

    service = SimpleNamespace(operation=operation, release_error=None)
    for command in (apply_command, plan_command):
        monkeypatch.setattr(command, "make_deployment_state_service", lambda *_a, **_k: service)
        monkeypatch.setattr(command, "make_kafka_deployer", lambda *_a, **_k: kafka)
        monkeypatch.setattr(command, "make_kafka_streams_deployer", lambda *_a, **_k: runtime)
        for factory in ("make_sr_deployer", "make_flink_deployer", "make_connect_deployer", "make_gateway_deployer"):
            monkeypatch.setattr(command, factory, forbidden)
    monkeypatch.setattr(DeploymentPlanner, "apply", forbidden)
    monkeypatch.setattr(DeploymentPlanner, "rollback", forbidden)
    monkeypatch.setattr(apply_command, "uuid", SimpleNamespace(uuid4=lambda: uuid.UUID(OPERATION)))
    plan_path = tmp_path / "approved.json"
    reviewed_result = CliRunner().invoke(main, ["-o", "json", "plan", "-p", str(tmp_path), "-e", "prod", "--out", str(plan_path)])
    assert reviewed_result.exit_code == 0, reviewed_result.output
    reviewed = ReviewedPlanFile.load(plan_path)
    assert reviewed.format_version == 6
    return SimpleNamespace(directory=tmp_path, backend=backend, prior=prior, plan_path=plan_path,
                           reviewed=reviewed, world=world, runtime=runtime, kafka=kafka, events=events,
                           prepared=prepared, forbidden=forbidden, operations=operations, change=change, service=service)


def invoke(fixture, *arguments, reviewed=True):
    return CliRunner().invoke(main, [
        "-o", "json", "apply", "-p", str(fixture.directory), "-e", "prod",
        *(["--plan", str(fixture.plan_path)] if reviewed else []), *arguments,
    ])


def snapshot(fixture):
    with fixture.backend.operation(ADDRESS) as operation:
        return operation.observe()


def test_public_reviewed_apply_uses_coordinator_and_exact_original_tuple(fixture):
    result = invoke(fixture)
    assert result.exit_code == 0, result.output
    data = json.loads(result.stdout)["data"]
    assert data["workflow"] == "kafka_streams_replacement"
    assert data["operation_id"] == OPERATION
    assert data["plan_checksum"] == fixture.reviewed.checksum
    assert data["committed"] is True
    assert data["state_serial"] == fixture.prior.serial + 1
    assert data["updated"] == ["kafka_streams_job:filtered"]
    final = snapshot(fixture)
    assert final.control.control.status == "clear"
    assert final.state.state.resources[RESOURCE].artifact_checksum == _evidence().desired_artifact.checksum
    receipt = fixture.backend._read_recovery_history(ADDRESS).completion_for(OPERATION)
    assert receipt.control.intent.actions == fixture.reviewed.actions
    assert receipt.control.intent.reviewed_plan_checksum == fixture.reviewed.checksum
    assert fixture.world.commands == ["term", "remove", "start"]
    assert len(fixture.world.creates) == 1
    fixture.forbidden.assert_not_called()
    fixture.runtime.progress.initialize.assert_not_called()
    fixture.runtime.docker.ensure_state_volume.assert_not_called()
    fixture.kafka.apply_topic.assert_not_called()


def test_public_reviewed_dry_run_has_no_intent_or_provider_mutation(fixture):
    before = snapshot(fixture)
    result = invoke(fixture, "--dry-run")
    assert result.exit_code == 0, result.output
    assert json.loads(result.stdout)["data"]["dry_run"] is True
    assert snapshot(fixture) == before
    assert not fixture.world.commands
    assert not fixture.world.creates
    fixture.forbidden.assert_not_called()


@pytest.mark.parametrize("arguments", [(), ("--force",), ("--dry-run",), ("--target", "filtered"), ("--select", "tag:no-match")])
def test_direct_or_selected_apply_cannot_bypass_reviewed_replacement(fixture, arguments):
    before = snapshot(fixture)
    result = invoke(fixture, *arguments, reviewed=False)
    assert result.exit_code == 1, result.output
    assert snapshot(fixture) == before
    assert not fixture.world.commands
    assert not fixture.world.creates
    fixture.forbidden.assert_not_called()


@pytest.mark.parametrize("damage", ["sql", "state", "checksum", "mixed", "unsupported", "selection", "timeout"])
def test_public_reviewed_apply_rejects_invalid_context_before_any_intent(fixture, damage):
    arguments = []
    if damage == "sql":
        path = fixture.directory / "stream_project.yml"
        declaration = yaml.safe_load(path.read_text())
        declaration["models"][0]["sql"] += " AND amount < 100"
        path.write_text(yaml.safe_dump(declaration))
    elif damage == "state":
        changed = copy.deepcopy(fixture.prior)
        changed.serial += 1
        changed.save(local_state_path(fixture.directory, environment="prod"))
    elif damage == "checksum":
        document = json.loads(fixture.plan_path.read_text())
        document["checksum"] = "sha256:" + "0" * 64
        fixture.plan_path.write_text(json.dumps(document))
    elif damage == "mixed":
        original = fixture.kafka.plan_topic.side_effect
        fixture.kafka.plan_topic.side_effect = lambda desired: replace(original(desired), action="update")
    elif damage == "unsupported":
        fixture.change.blocker = "kafka_streams_unsupported_change"
    elif damage == "selection":
        arguments = ["--target", "filtered"]
    else:
        arguments = ["--runner-timeout", "nan"]
    before = snapshot(fixture)
    result = invoke(fixture, *arguments)
    assert result.exit_code != 0, result.output
    assert snapshot(fixture) == before
    assert not fixture.world.commands
    assert not fixture.world.creates
    fixture.forbidden.assert_not_called()


@pytest.mark.parametrize("boundary", ["term", "remove", "create", "start"])
def test_lost_provider_ack_reports_exact_operation_and_never_generic_rollback(fixture, boundary):
    fixture.world.failure = boundary
    result = invoke(fixture)
    assert result.exit_code == 1, result.output
    assert SECRET not in result.output
    data = json.loads(result.stdout)["data"]
    assert data["committed"] is None
    assert data["operation_id"] == OPERATION
    assert data["plan_checksum"] == fixture.reviewed.checksum
    assert shlex.split(data["next_steps"][0])[:3] == ["streamt", "state", "runner-status"]
    assert shlex.split(data["next_steps"][1])[:3] == ["streamt", "state", "resume"]
    assert "--operation-id" in data["next_steps"][1]
    pending = snapshot(fixture)
    assert pending.control.control.status == "recovery_required"
    assert pending.control.control.intent.actions == fixture.reviewed.actions
    assert pending.state.state == fixture.prior
    assert not any(item.succeeded is False for item in pending.control.control.progress)
    fixture.forbidden.assert_not_called()


@pytest.mark.parametrize("after_clear", [False, True])
def test_lost_finalizer_ack_reports_unknown_not_success_or_second_incident(fixture, monkeypatch, after_clear):
    original = fixture.backend._write_control

    def uncertain(path, control, *, operation_id):
        if control.status == "clear":
            if after_clear:
                original(path, control, operation_id=operation_id)
            raise StateBackendUnknownCommitError("completion unknown", operation_id=OPERATION)
        original(path, control, operation_id=operation_id)

    monkeypatch.setattr(fixture.backend, "_write_control", uncertain)
    result = invoke(fixture)
    assert result.exit_code == 1, result.output
    data = json.loads(result.stdout)["data"]
    assert data["committed"] is None
    assert data["last_acknowledged_boundary"] == "completed"
    final = snapshot(fixture)
    assert final.control.control.status == ("clear" if after_clear else "in_progress")
    receipt = fixture.backend._read_recovery_history(ADDRESS).completion_for(OPERATION)
    assert receipt.control.status == "in_progress"
    assert receipt.control.recovery is None
    assert final.state.state.serial == fixture.prior.serial + 1
    fixture.forbidden.assert_not_called()


def test_runner_openlineage_starts_after_intent_and_completes_after_finalization(fixture, monkeypatch):
    transport = _FakeTransport(fixture.events)
    _install_transports(monkeypatch, transport)
    result = invoke(fixture, "--emit-openlineage", "--openlineage-job-namespace", "https://lineage.example/ns")
    assert result.exit_code == 0, result.output
    assert [event["eventType"] for event in transport.attempts] == ["START", "COMPLETE"]
    assert all(event["run"]["runId"] == OPERATION for event in transport.attempts)
    assert fixture.events.index("intent-ack") < fixture.events.index("openlineage-START") < fixture.events.index("term")
    assert fixture.events.index("finalize-ack") < fixture.events.index("openlineage-COMPLETE")
    assert transport.close_calls == 1


@pytest.mark.parametrize("position", range(6))
@pytest.mark.parametrize("after_write", [False, True])
def test_every_uncertain_journal_boundary_retains_last_ack_without_generic_cleanup(fixture, monkeypatch, position, after_write):
    original = fixture.backend._write_control

    def uncertain(path, control, *, operation_id):
        if control.status == "in_progress" and len(control.progress) == position:
            if after_write:
                original(path, control, operation_id=operation_id)
            raise StateBackendUnknownCommitError("journal outcome unknown", operation_id=OPERATION)
        original(path, control, operation_id=operation_id)

    monkeypatch.setattr(fixture.backend, "_write_control", uncertain)
    result = invoke(fixture)
    assert result.exit_code == 1, result.output
    data = json.loads(result.stdout)["data"]
    assert data["committed"] is None
    assert data["operation_id"] == OPERATION
    last = ["intent_unconfirmed", "intent", "started", "old_closed", "old_removed", "replacement_created"][position]
    assert data["last_acknowledged_boundary"] == last
    control = snapshot(fixture).control.control
    assert len(control.progress) == (position if after_write else max(0, position - 1))
    assert not any(item.succeeded is False for item in control.progress)
    assert snapshot(fixture).state.state == fixture.prior
    fixture.forbidden.assert_not_called()


@pytest.mark.parametrize("phase", ["planning", "before_term", "after_term"])
@pytest.mark.parametrize("damage", ["sql", "environment_policy", "state_authority"])
def test_fresh_full_project_and_authority_checks_bound_every_runtime_transition(fixture, phase, damage):
    changed = False

    def change_project():
        nonlocal changed
        if changed:
            return
        changed = True
        path = fixture.directory / ("environments/prod.yml" if damage == "environment_policy" else "stream_project.yml")
        declaration = yaml.safe_load(path.read_text())
        if damage == "sql":
            declaration["models"][0]["sql"] += " AND amount < 100"
        elif damage == "environment_policy":
            declaration["environment"]["protected"] = True
        else:
            declaration["deployment_state"] = {
                "backend": "postgres", "namespace": "different",
                "postgres": {"dsn_env": "UNUSED_TEST_ADMIN", "writer_dsn_env": "UNUSED_TEST_WRITER"},
            }
        path.write_text(yaml.safe_dump(declaration))

    if phase == "planning":
        fixture.prepared.side_effect = lambda *_args: (change_project(), _evidence())[1]
    else:
        fixture.world.after_observe = lambda: change_project() if phase == "before_term" or "term" in fixture.world.commands else None
    result = invoke(fixture)
    assert result.exit_code == 1, result.output
    assert changed
    assert fixture.world.commands == (["term"] if phase == "after_term" else [])
    assert not fixture.world.creates
    assert snapshot(fixture).state.state == fixture.prior
    fixture.forbidden.assert_not_called()


@pytest.mark.parametrize("after_write", [False, True])
def test_openlineage_is_not_emitted_without_intent_ack(fixture, monkeypatch, after_write):
    transport = _FakeTransport(fixture.events)
    _install_transports(monkeypatch, transport)
    original = fixture.backend._write_control

    def uncertain(path, control, *, operation_id):
        if after_write:
            original(path, control, operation_id=operation_id)
        raise StateBackendUnknownCommitError("unconfirmed intent", operation_id=OPERATION)

    monkeypatch.setattr(fixture.backend, "_write_control", uncertain)
    result = invoke(fixture, "--emit-openlineage", "--openlineage-job-namespace", "https://lineage.example/ns")
    assert result.exit_code == 1, result.output
    assert transport.attempts == []
    assert transport.close_calls == 1
    assert not fixture.world.commands
    fixture.forbidden.assert_not_called()


def test_openlineage_failure_does_not_change_a_successful_runner_commit(fixture, monkeypatch):
    transport = _FakeTransport(fixture.events, fail_attempts={1, 2}, fail_close=True)
    _install_transports(monkeypatch, transport)
    result = invoke(fixture, "--emit-openlineage", "--openlineage-job-namespace", "https://lineage.example/ns")
    assert result.exit_code == 0, result.output
    assert json.loads(result.stdout)["data"]["committed"] is True
    assert [event["eventType"] for event in transport.attempts] == ["START", "COMPLETE"]
    assert transport.close_calls == 1


def test_release_failure_after_verified_completion_retains_successful_state(fixture):
    fixture.service.release_error = StateBackendReleaseAfterCommitError("Release failed after commit", operation_id=OPERATION)
    result = invoke(fixture)
    assert result.exit_code == 1, result.output
    data = json.loads(result.stdout)["data"]
    assert data["committed"] is True
    assert data["operation_id"] == OPERATION
    assert data["state_serial"] == fixture.prior.serial + 1
    assert snapshot(fixture).control.control.status == "clear"
    assert len(fixture.world.creates) == 1
    fixture.forbidden.assert_not_called()


@pytest.mark.parametrize("environment", [None, "default", "prod"])
def test_continuation_commands_quote_exact_paths_and_only_explicit_environments(tmp_path, environment):
    path = tmp_path / "project with spaces;literal"
    plan = path / "plan.json"
    steps = apply_command._runner_continuation_commands(path, environment, plan, OPERATION)
    for step in steps:
        words = shlex.split(step)
        assert words[words.index("--project-dir") + 1] == str(path)
        assert words[words.index("--plan") + 1] == str(plan.resolve())
        assert words[words.index("--operation-id") + 1] == OPERATION
        assert ("--env" in words) is (environment is not None)
        if environment is not None:
            assert words[words.index("--env") + 1] == environment
