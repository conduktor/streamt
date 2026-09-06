"""Internal full-project replacement preparation; no generic execution opt-in."""

from __future__ import annotations

import copy
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from streamt.compiler import Compiler
from streamt.compiler.manifest import parse_compiled_kafka_streams_job_artifact
from streamt.core.models import OwnershipMode
from streamt.deployer.kafka import TopicChange
from streamt.deployer.kafka_streams import (
    KafkaStreamsDeployer,
    KafkaStreamsJobChange,
    KafkaStreamsJobState,
)
from streamt.deployer.kafka_streams_evidence import (
    KafkaStreamsActionEvidence,
    KafkaStreamsArtifactSnapshot,
    KafkaStreamsVolumeEvidence,
)
from streamt.deployer.kafka_streams_replacement_observer import KafkaStreamsReplacementObserver
from streamt.deployer.operation_actions import operation_actions_from_planned
from streamt.deployer.plan_file import ReviewedPlanFile, StalePlanError, StateReference
from streamt.deployer.planner import DeploymentPlanner, OwnershipRequirement, SafetyBlocker
from streamt.deployer.state import StateIdentityError, resource_id
from tests.unit.test_kafka_streams_operation_evidence import (
    NETWORK,
    OLD_ID,
    STAMP,
    TOKEN,
    _progress,
)
from tests.unit.test_plan_file import _state_observation
from tests.unit.test_planner_kafka_streams import BACKEND, _planner


def _case(monkeypatch, *, enabled=True, external_sibling=False):
    previous, kafka, _unused_runner, initial = _planner(exists=True, owned=True)
    project = previous.project.model_copy(deep=True)
    project.models[0].sql = project.models[0].sql.replace("amount >= 50", "amount >= 75")
    if external_sibling:
        sibling = project.models[0].model_copy(deep=True)
        sibling.name = "external_runner"
        sibling.ownership = sibling.ownership.model_copy(update={"mode": OwnershipMode.EXTERNAL})
        project.models.append(sibling)
    manifest = Compiler(project).compile(dry_run=True)
    desired = parse_compiled_kafka_streams_job_artifact(next(
        artifact for artifact in manifest.artifacts["kafka_streams_jobs"]
        if artifact["name"] == initial.desired.name
    ))
    prior = KafkaStreamsArtifactSnapshot.from_artifact(initial.desired)
    evidence = KafkaStreamsActionEvidence(
        1, BACKEND, OLD_ID, prior, KafkaStreamsArtifactSnapshot.from_artifact(desired), desired.image, NETWORK,
        KafkaStreamsVolumeEvidence(desired.application_id + "-state", "local", STAMP, desired.application_id, BACKEND, TOKEN),
        _progress(),
    )
    current = KafkaStreamsJobState(
        desired.name, True, OLD_ID, "running", prior.checksum, prior.plan_hash, desired.image,
        evidence.progress.input_topic_id, evidence.progress.output_topic_id, NETWORK,
    )
    change = KafkaStreamsJobChange(desired.name, "update", current, desired, {
        "application_id": desired.application_id, "image_id": desired.image,
        "topic_bindings": {desired.plan["input_topic"]: evidence.progress.input_topic_id,
                           desired.plan["output_topic"]: evidence.progress.output_topic_id},
        "initial_offset": desired.initial_offset, "network_id": NETWORK,
        "desired_artifact_hash": evidence.desired_artifact.checksum,
    }, BACKEND, "kafka_streams_replacement_not_verified")
    # Exact runtime type without client construction. Its provider observations
    # are fixtures; the real observer entry point is called and captured below.
    runtime = object.__new__(KafkaStreamsDeployer)
    runtime.config, runtime.kafka = project.runtime.kafka_streams, project.runtime.kafka
    runtime.backend_identity = BACKEND
    runtime.plan_job = Mock(side_effect=lambda artifact, **_kwargs: copy.deepcopy(change))
    runtime.preflight, runtime.apply_job = Mock(), Mock()
    prepared = Mock(return_value=evidence)

    def prepare(observer, artifact, ownership):
        assert type(observer) is KafkaStreamsReplacementObserver
        assert observer.deployer is runtime
        return prepared(artifact, ownership)

    monkeypatch.setattr(KafkaStreamsReplacementObserver, "prepare", prepare)
    planner = DeploymentPlanner(
        manifest, kafka_deployer=kafka, kafka_streams_deployer=runtime,
        project=project, prior_state=previous.prior_state,
        allow_kafka_streams_replacement=enabled,
    )
    return SimpleNamespace(planner=planner, runtime=runtime, kafka=kafka, change=change,
                           prepared=prepared, evidence=evidence, project=project, manifest=manifest)


def _review(case, plan):
    planner = case.planner
    observation = _state_observation(
        project=planner.project_name, environment=planner.environment,
        serial=planner.prior_state.serial, resources=planner.prior_state.resources,
    )
    actions = operation_actions_from_planned(planner.planned_actions(plan))
    reviewed = ReviewedPlanFile.create(
        plan, planner.manifest, project=planner.project_name, environment=planner.environment,
        runtime=planner.project.runtime, state=StateReference.from_observation(observation), actions=actions,
    )
    return reviewed, observation, actions


def test_internal_opt_in_prepares_one_exact_reviewed_v6_action(monkeypatch, tmp_path):
    case = _case(monkeypatch)
    plan = case.planner.plan()
    assert not plan.is_apply_blocked
    assert (plan.creates, plan.updates, plan.deletes) == (0, 1, 0)
    change = plan.kafka_streams_changes[0]
    assert change.blocker is None
    assert change.kafka_streams_evidence is case.evidence
    prior = case.planner.prior_state.resources[resource_id(case.planner.project_name, "default", "kafka_streams_job", change.job_name)]
    case.prepared.assert_called_once_with(change.desired, prior)
    reviewed, observation, actions = _review(case, plan)
    assert reviewed.format_version == 6
    assert actions[0].kafka_streams_evidence is case.evidence
    assert actions[0]._wire_version == 4
    reviewed.verify_current_plan(plan, actions=actions, state_observation=observation)
    path = tmp_path / "reviewed.json"
    reviewed.save(path)
    assert ReviewedPlanFile.load(path) == reviewed
    assert case.planner.manifest.artifacts == Compiler(case.project).compile(dry_run=True).artifacts
    assert not case.runtime.preflight.called
    assert not case.runtime.apply_job.called
    assert not case.kafka.apply_topic.called


def test_default_planning_keeps_replacement_blocked_and_does_not_prepare(monkeypatch):
    case = _case(monkeypatch, enabled=False)
    plan = case.planner.plan()
    assert plan.is_apply_blocked
    assert plan.kafka_streams_changes[0].kafka_streams_evidence is None
    with pytest.raises(StateIdentityError, match="lifecycle"):
        case.planner.planned_actions(plan)
    case.prepared.assert_not_called()


@pytest.mark.parametrize("value", [None, 0, 1, "true", [], object()])
def test_opt_in_is_a_strict_bool(monkeypatch, value):
    with pytest.raises(StateIdentityError, match="boolean"):
        _case(monkeypatch, enabled=value)


@pytest.mark.parametrize("method", ["offline_plan", "apply", "operation_actions"])
def test_opt_in_never_authorizes_offline_or_generic_execution(monkeypatch, method):
    case = _case(monkeypatch)
    plan = case.planner.plan()
    with pytest.raises(StateIdentityError):
        getattr(case.planner, method)(*(() if method == "offline_plan" else (plan,)))
    case.runtime.apply_job.assert_not_called()
    case.kafka.apply_topic.assert_not_called()


@pytest.mark.parametrize("damage", ["project", "environment", "runtime", "selected_manifest", "selected_models", "removals", "missing_state"])
def test_incomplete_context_fails_before_provider_observation(monkeypatch, damage):
    case = _case(monkeypatch)
    if damage == "project":
        case.planner.project = None
    elif damage == "environment":
        case.planner.environment = "other"
    elif damage == "runtime":
        case.runtime.kafka = case.runtime.kafka.model_copy(update={"bootstrap_servers": "other:9092"})
    elif damage == "selected_manifest":
        case.manifest.artifacts["topics"] = []
    elif damage == "selected_models":
        case.manifest.models = []
    elif damage == "removals":
        case.manifest.artifacts["connector_removals"] = [{"name": "foreign"}]
    else:
        case.planner.prior_state = None
    with pytest.raises(StateIdentityError):
        case.planner.plan()
    case.runtime.plan_job.assert_not_called()
    case.kafka.plan_topic.assert_not_called()
    case.prepared.assert_not_called()


@pytest.mark.parametrize("damage", ["extra_mutation", "missing_topic", "missing_noop_current", "unowned", "artifact_drift", "stopped", "wrong_current_uuid", "wrong_changes", "adopted", "foreign_preparation", "provider_injected_proof"])
def test_unsupported_or_incomplete_transition_cannot_be_prepared(monkeypatch, damage):
    case = _case(monkeypatch)
    if damage == "extra_mutation":
        original = case.kafka.plan_topic.side_effect
        case.kafka.plan_topic.side_effect = lambda artifact: replace(original(artifact), action="update")
    elif damage == "missing_topic":
        case.planner.kafka_deployer = None
    elif damage == "missing_noop_current":
        original = case.kafka.plan_topic.side_effect
        case.kafka.plan_topic.side_effect = lambda artifact: replace(original(artifact), current=None)
    elif damage in {"unowned", "adopted"}:
        key = resource_id(case.planner.project_name, "default", "kafka_streams_job", case.change.job_name)
        if damage == "unowned":
            del case.planner.prior_state.resources[key]
        else:
            case.planner.prior_state.resources[key] = replace(case.planner.prior_state.resources[key], ownership="adopted")
    elif damage == "artifact_drift":
        case.change.current.artifact_hash = "sha256:" + "f" * 64
    elif damage == "stopped":
        case.change.current.status = "stopped"
    elif damage == "wrong_current_uuid":
        case.change.current.output_topic_id = "AAAAAAAAAAAAAAAAAAAAAw"
    elif damage == "wrong_changes":
        case.change.changes["initial_offset"] = "latest"
    elif damage == "foreign_preparation":
        case.prepared.return_value = replace(case.evidence, prior_container_id="f" * 64)
    else:
        case.change.kafka_streams_evidence = case.evidence
    with pytest.raises(StateIdentityError):
        case.planner.plan()
    case.runtime.apply_job.assert_not_called()
    case.kafka.apply_topic.assert_not_called()


def test_observer_failure_is_secret_neutral_and_never_downgraded_to_create(monkeypatch):
    case = _case(monkeypatch)
    case.prepared.side_effect = RuntimeError("sasl.password=fixture-secret")
    with pytest.raises(StateIdentityError, match="Cannot prepare exact") as raised:
        case.planner.plan()
    assert "fixture-secret" not in str(raised.value)
    assert case.planner._prepared_kafka_streams_plan is None


@pytest.mark.parametrize("damage", ["stripped", "copied", "added_topic", "removed_topic", "changed_action", "changed_current", "changed_backend", "changed_changes", "changed_prior", "state_serial", "unrelated_state", "noop_current", "noop_changes", "ownership", "safety", "replacement_none", "foreign_plan"])
def test_authoritative_planned_actions_reject_tampered_prepared_plan(monkeypatch, damage):
    case = _case(monkeypatch)
    plan = case.planner.plan()
    change = plan.kafka_streams_changes[0]
    if damage == "stripped":
        change.kafka_streams_evidence = None
    elif damage == "copied":
        change.kafka_streams_evidence = copy.deepcopy(change.kafka_streams_evidence)
    elif damage == "added_topic":
        plan.topic_changes.append(TopicChange("foreign", "create"))
    elif damage == "removed_topic":
        plan.topic_changes.clear()
    elif damage == "changed_action":
        plan.topic_changes[0].action = "update"
    elif damage == "changed_current":
        change.current.container_id = "f" * 64
    elif damage == "changed_backend":
        change.backend_identity = "kafka-streams-docker:v1:" + "f" * 64
    elif damage == "changed_changes":
        change.changes["topic_bindings"][str(change.desired.plan["input_topic"])] = "foreign"
    elif damage == "changed_prior":
        key = resource_id(case.planner.project_name, "default", "kafka_streams_job", change.job_name)
        case.planner.prior_state.resources[key] = replace(case.planner.prior_state.resources[key], artifact_checksum="sha256:" + "f" * 64)
    elif damage == "state_serial":
        case.planner.prior_state.serial += 1
    elif damage == "unrelated_state":
        key = resource_id(case.planner.project_name, "default", "topic", change.job_name)
        case.planner.prior_state.resources[key] = replace(case.planner.prior_state.resources[key], artifact_checksum="sha256:" + "f" * 64)
    elif damage == "noop_current":
        plan.topic_changes[0].current = None
    elif damage == "noop_changes":
        plan.topic_changes[0].changes = {"partitions": {"from": 1, "to": 2}}
    elif damage == "ownership":
        plan.ownership_requirements.append(OwnershipRequirement("id", "topic", "topic", "topic", "requires_adoption", "update", "managed", "blocked"))
    elif damage == "safety":
        plan.safety_blockers.append(SafetyBlocker("blocked", "topic", "topic", "update", "blocked"))
    elif damage == "replacement_none":
        change.action, change.kafka_streams_evidence = "none", None
    else:
        plan = copy.deepcopy(plan)
    with pytest.raises(StateIdentityError):
        case.planner.planned_actions(plan)
    case.runtime.apply_job.assert_not_called()
    case.kafka.apply_topic.assert_not_called()


def test_external_sibling_is_complete_declaration_only_and_never_observed(monkeypatch):
    case = _case(monkeypatch, external_sibling=True)
    plan = case.planner.plan()
    external = next(change for change in plan.kafka_streams_changes if change.job_name == "external_runner")
    assert external.action == "none"
    assert external.current is None
    assert external.kafka_streams_evidence is None
    case.runtime.plan_job.assert_called_once()
    case.prepared.assert_called_once()
    assert len(case.planner.planned_actions(plan)) == 1
    assert _review(case, plan)[0].format_version == 6


def test_reviewed_binding_allows_only_forward_progress_without_rebasing_reviewed_evidence(monkeypatch):
    case = _case(monkeypatch)
    first = case.planner.plan()
    reviewed, observation, _actions = _review(case, first)
    advanced = replace(case.evidence, progress=_progress(committed=15))
    case.prepared.return_value = advanced
    current = case.planner.plan()
    actions = operation_actions_from_planned(case.planner.planned_actions(current))
    reviewed.verify_current_plan(current, actions=actions, state_observation=observation)
    assert reviewed.actions[0].kafka_streams_evidence is case.evidence
    assert actions[0].kafka_streams_evidence is advanced
    case.prepared.return_value = replace(case.evidence, progress=_progress(committed=9))
    regressed = case.planner.plan()
    with pytest.raises(StalePlanError):
        reviewed.verify_current_plan(regressed, actions=operation_actions_from_planned(case.planner.planned_actions(regressed)), state_observation=observation)
