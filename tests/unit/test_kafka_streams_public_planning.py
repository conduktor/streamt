"""Public reviewed planning preserves ordinary plans and exact observation seals."""

from __future__ import annotations

import copy
import json
import shlex
from dataclasses import replace
from unittest.mock import Mock

import pytest
import yaml
from click.testing import CliRunner

from streamt.cli import main
from streamt.deployer.plan_file import ReviewedPlanFile
from streamt.deployer.planner import DeploymentPlan
from streamt.deployer.state import StateIdentityError, local_state_path
from tests.unit.test_kafka_streams_replacement_planning import _case, _review
from tests.unit.test_planner_kafka_streams import _planner


def test_explicit_review_hook_prepares_ordinary_observed_update(monkeypatch, tmp_path):
    case = _case(monkeypatch, enabled=False)
    plan = case.planner.plan()
    assert plan.is_apply_blocked
    assert case.planner.prepare_reviewed_kafka_streams_replacement(plan) is True
    assert case.planner._allow_kafka_streams_replacement is False
    assert not plan.is_apply_blocked
    assert plan.kafka_streams_changes[0].kafka_streams_evidence is case.evidence
    reviewed, _observation, actions = _review(case, plan)
    assert reviewed.format_version == 6
    assert actions[0].kafka_streams_evidence is case.evidence
    reviewed.save(tmp_path / "reviewed.json")
    assert ReviewedPlanFile.load(tmp_path / "reviewed.json") == reviewed
    for method in (case.planner.apply, case.planner.operation_actions):
        with pytest.raises(StateIdentityError):
            method(plan)
    case.kafka.apply_topic.assert_not_called()
    case.runtime.apply_job.assert_not_called()


@pytest.mark.parametrize("kind", ["create", "noop", "external"])
def test_ordinary_nonreplacement_review_hook_is_false_and_preserves_generic_behavior(kind):
    planner, kafka, runtime, _change = _planner(
        mode="external" if kind == "external" else "managed",
        exists=kind == "noop", owned=kind == "noop",
    )
    plan = planner.plan()
    before = copy.deepcopy(plan)
    assert planner.prepare_reviewed_kafka_streams_replacement(plan) is False
    assert plan == before
    assert planner._allow_kafka_streams_replacement is False
    assert planner._prepared_kafka_streams_plan is None
    assert len(planner.planned_actions(plan)) == (2 if kind == "create" else 0)
    if kind != "create":
        assert planner.apply(plan)["errors"] == []
        kafka.apply_topic.assert_not_called()
        runtime.apply_job.assert_not_called()


@pytest.mark.parametrize("damage", [
    "copied", "foreign", "newer_plan", "offline", "missing_topic", "added_topic",
    "noop_current", "noop_changes", "job_current", "job_backend", "job_changes",
    "stripped_update", "stripped_blocker", "injected_evidence", "prior_state", "state_serial",
])
def test_observed_review_seal_rejects_forged_or_stale_surface_before_prepare(monkeypatch, damage):
    case = _case(monkeypatch, enabled=False)
    plan = case.planner.plan()
    if damage == "copied":
        plan = copy.deepcopy(plan)
    elif damage == "foreign":
        plan = DeploymentPlan()
    elif damage == "newer_plan":
        case.planner.plan()
    elif damage == "offline":
        case.planner.offline_plan()
    elif damage == "missing_topic":
        plan.topic_changes.clear()
    elif damage == "added_topic":
        plan.topic_changes.append(copy.deepcopy(plan.topic_changes[0]))
    elif damage == "noop_current":
        plan.topic_changes[0].current.partitions += 1
    elif damage == "noop_changes":
        plan.topic_changes[0].changes["forged"] = "value"
    elif damage == "job_current":
        plan.kafka_streams_changes[0].current.container_id = "f" * 64
    elif damage == "job_backend":
        plan.kafka_streams_changes[0].backend_identity = "foreign"
    elif damage == "job_changes":
        plan.kafka_streams_changes[0].changes["network_id"] = "f" * 64
    elif damage == "stripped_update":
        plan.kafka_streams_changes[0].action = "none"
    elif damage == "stripped_blocker":
        plan.kafka_streams_changes[0].blocker = None
    elif damage == "injected_evidence":
        plan.kafka_streams_changes[0].kafka_streams_evidence = case.evidence
    elif damage == "prior_state":
        case.planner.prior_state.resources.clear()
    else:
        case.planner.prior_state.serial += 1
    with pytest.raises(StateIdentityError):
        case.planner.prepare_reviewed_kafka_streams_replacement(plan)
    case.prepared.assert_not_called()


def test_preparation_is_one_shot_and_new_plan_invalidates_old_evidence(monkeypatch):
    case = _case(monkeypatch, enabled=False)
    old = case.planner.plan()
    assert case.planner.prepare_reviewed_kafka_streams_replacement(old)
    with pytest.raises(StateIdentityError):
        case.planner.prepare_reviewed_kafka_streams_replacement(old)
    case.planner.plan()
    with pytest.raises(StateIdentityError):
        case.planner.planned_actions(old)
    case.prepared.assert_called_once()


@pytest.mark.parametrize("damage", ["mixed", "selected", "unsupported", "unowned"])
def test_public_preparation_reuses_full_scope_and_ownership_gates(monkeypatch, damage):
    case = _case(monkeypatch, enabled=False)
    if damage == "mixed":
        original = case.kafka.plan_topic.side_effect
        case.kafka.plan_topic.side_effect = lambda artifact: replace(original(artifact), action="update")
    elif damage == "selected":
        case.manifest.artifacts["topics"].clear()
    elif damage == "unsupported":
        case.change.blocker = "kafka_streams_unsupported_change"
    else:
        case.planner.prior_state.resources.clear()
    plan = case.planner.plan()
    if damage == "unowned":
        # Existing ownership policy suppresses the mutation and retains its
        # adoption blocker; False is not permission to apply that plan.
        assert plan.is_apply_blocked
        assert case.planner.prepare_reviewed_kafka_streams_replacement(plan) is False
    else:
        with pytest.raises(StateIdentityError):
            case.planner.prepare_reviewed_kafka_streams_replacement(plan)
    case.prepared.assert_not_called()


def _cli_fixture(monkeypatch, tmp_path, *, kind="replacement", external_sibling=False):
    if kind == "replacement":
        case = _case(monkeypatch, enabled=False, external_sibling=external_sibling)
        project, prior, kafka, runtime = case.project, case.planner.prior_state, case.kafka, case.runtime
    else:
        planner, kafka, runtime, _change = _planner(
            mode="external" if kind == "external" else "managed",
            exists=kind == "noop", owned=kind == "noop",
        )
        project, prior, case = planner.project, planner.prior_state, None
    (tmp_path / "stream_project.yml").write_text(yaml.safe_dump(
        project.model_dump(mode="json", by_alias=True, exclude_none=True), sort_keys=False,
    ))
    prior.save(local_state_path(tmp_path, environment="default"))
    from streamt.cli.commands import plan as command

    monkeypatch.setattr(command, "make_kafka_deployer", lambda *_a, **_k: kafka)
    monkeypatch.setattr(command, "make_kafka_streams_deployer", lambda *_a, **_k: runtime)
    monkeypatch.setattr(command, "close_deployers", lambda *_a, **_k: None)
    for factory in ("make_sr_deployer", "make_flink_deployer", "make_connect_deployer", "make_gateway_deployer"):
        monkeypatch.setattr(command, factory, Mock(side_effect=AssertionError("Unexpected provider")))
    return case, kafka, runtime


@pytest.mark.parametrize("save", [True, False])
def test_public_online_plan_prepares_and_reports_real_reviewed_v6(monkeypatch, tmp_path, save):
    case, kafka, runtime = _cli_fixture(monkeypatch, tmp_path, external_sibling=True)
    state_path = local_state_path(tmp_path, environment="default")
    prior_bytes = state_path.read_bytes()
    target = tmp_path / "reviewed.json"
    result = CliRunner().invoke(main, ["-o", "json", "plan", "-p", str(tmp_path),
                                     *(["--out", str(target)] if save else [])])
    assert result.exit_code == 0, result.output
    payload = json.loads(result.stdout)
    assert payload["status"] == "ok"
    data = payload["data"]
    assert data["updates"] == 1
    assert data["is_apply_blocked"] is False
    replacement = data["kafka_streams_replacement"]
    assert replacement["requires_plan_file"] is True
    assert replacement["plan_file_saved"] is save
    assert replacement["scope"] == "sole_predicate_update"
    assert len(replacement["next_steps"]) == (1 if save else 2)
    assert all("--env" not in step for step in replacement["next_steps"])
    assert "apply" in replacement["next_steps"][-1]
    assert "--plan" in replacement["next_steps"][-1]
    if save:
        reviewed = ReviewedPlanFile.load(target)
        assert data["plan_format_version"] == reviewed.format_version == 6
        assert data["plan_checksum"] == reviewed.checksum
        assert reviewed.actions[0].kafka_streams_evidence == case.evidence
        assert reviewed.actions[0]._wire_version == 4
    else:
        assert not target.exists()
        assert "plan_format_version" not in data
    assert state_path.read_bytes() == prior_bytes
    case.prepared.assert_called_once()
    assert runtime.plan_job.call_count == 1
    assert all(call.args[0].name != "orders.raw.v1" for call in kafka.plan_topic.call_args_list)
    runtime.apply_job.assert_not_called()
    kafka.apply_topic.assert_not_called()


@pytest.mark.parametrize("kind", ["create", "noop", "external"])
def test_public_ordinary_plans_keep_format5_and_do_not_enable_replacement(monkeypatch, tmp_path, kind):
    _case_unused, kafka, runtime = _cli_fixture(monkeypatch, tmp_path, kind=kind)
    target = tmp_path / "ordinary.json"
    result = CliRunner().invoke(main, ["-o", "json", "plan", "-p", str(tmp_path), "--out", str(target)])
    assert result.exit_code == 0, result.output
    data = json.loads(result.stdout)["data"]
    assert "kafka_streams_replacement" not in data
    assert data["plan_format_version"] == ReviewedPlanFile.load(target).format_version == 5
    assert data["creates"] == (2 if kind == "create" else 0)
    assert data["updates"] == 0
    kafka.apply_topic.assert_not_called()
    runtime.apply_job.assert_not_called()


def test_public_offline_plan_never_prepares_replacement(monkeypatch, tmp_path):
    case, kafka, runtime = _cli_fixture(monkeypatch, tmp_path)
    target = tmp_path / "offline.json"
    result = CliRunner().invoke(main, ["-o", "json", "plan", "-p", str(tmp_path), "--offline", "--out", str(target)])
    assert result.exit_code == 0, result.output
    reviewed = ReviewedPlanFile.load(target)
    assert reviewed.offline
    assert reviewed.format_version == 5
    assert reviewed.actions == ()
    case.prepared.assert_not_called()
    kafka.plan_topic.assert_not_called()
    runtime.plan_job.assert_not_called()


def test_public_next_steps_preserve_explicit_environment_and_quote_paths(monkeypatch, tmp_path):
    directory = tmp_path / "project with spaces;literal"
    directory.mkdir()
    _cli_fixture(monkeypatch, directory)
    project_file = directory / "stream_project.yml"
    declaration = yaml.safe_load(project_file.read_text())
    environment = {"environment": {"name": "default"}, "runtime": declaration.pop("runtime")}
    project_file.write_text(yaml.safe_dump(declaration, sort_keys=False))
    (directory / "environments").mkdir()
    (directory / "environments/default.yml").write_text(yaml.safe_dump(environment, sort_keys=False))
    result = CliRunner().invoke(main, ["-o", "json", "plan", "-p", str(directory), "--env", "default"])
    assert result.exit_code == 0, result.output
    steps = json.loads(result.stdout)["data"]["kafka_streams_replacement"]["next_steps"]
    assert [shlex.split(step) for step in steps] == [
        ["streamt", "plan", "--project-dir", str(directory), "--env", "default",
         "--out", str(directory / ".streamt/reviewed-plan.json")],
        ["streamt", "apply", "--project-dir", str(directory), "--env", "default",
         "--plan", str(directory / ".streamt/reviewed-plan.json")],
    ]


@pytest.mark.parametrize("damage", ["mixed", "unsupported", "observer_secret", "state_changed"])
def test_public_failed_preparation_never_saves_or_mutates_runtime(monkeypatch, tmp_path, damage):
    case, kafka, runtime = _cli_fixture(monkeypatch, tmp_path)
    if damage == "mixed":
        original = kafka.plan_topic.side_effect
        kafka.plan_topic.side_effect = lambda artifact: replace(original(artifact), action="update")
    elif damage == "unsupported":
        case.change.blocker = "kafka_streams_unsupported_change"
    elif damage == "observer_secret":
        case.prepared.side_effect = RuntimeError("sasl.password=never-print-this-secret")
    else:
        def changed(*_args):
            # Simulate a foreign state write despite the caller-held lock.
            changed_state = copy.deepcopy(case.planner.prior_state)
            changed_state.serial += 1
            changed_state.save(local_state_path(tmp_path, environment="default"))
            return case.evidence
        case.prepared.side_effect = changed
    target = tmp_path / "rejected.json"
    result = CliRunner().invoke(main, ["-o", "json", "plan", "-p", str(tmp_path), "--out", str(target)])
    assert result.exit_code == 1, result.output
    assert json.loads(result.stdout)["status"] == "error"
    assert "never-print-this-secret" not in result.output
    assert not target.exists()
    kafka.apply_topic.assert_not_called()
    runtime.apply_job.assert_not_called()


def test_public_plan_has_no_selected_replacement_path(monkeypatch, tmp_path):
    case, kafka, runtime = _cli_fixture(monkeypatch, tmp_path)
    result = CliRunner().invoke(main, ["plan", "-p", str(tmp_path), "--select", "valuable_orders"])
    assert result.exit_code == 2
    assert "No such option" in result.output
    case.prepared.assert_not_called()
    kafka.plan_topic.assert_not_called()
    runtime.plan_job.assert_not_called()
