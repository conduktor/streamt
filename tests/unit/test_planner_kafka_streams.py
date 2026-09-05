"""Fail-closed planner/state integration for the bounded Kafka Streams runner."""

from __future__ import annotations

import copy
from unittest.mock import MagicMock

import pytest

from streamt.compiler import Compiler
from streamt.compiler.kafka_streams import application_id
from streamt.compiler.manifest import parse_compiled_kafka_streams_job_artifact
from streamt.core.models import StreamtProject
from streamt.deployer.kafka import KafkaDeployer, TopicChange, TopicState
from streamt.deployer.kafka_streams import (
    KafkaStreamsDeployer,
    KafkaStreamsJobChange,
    KafkaStreamsJobState,
)
from streamt.deployer.operation_actions import operation_actions_from_planned
from streamt.deployer.plan_file import (
    ReviewedPlanFile,
    StalePlanError,
    StateReference,
    deployment_plan_payload,
)
from streamt.deployer.planner import DeploymentPlanner
from streamt.deployer.recovery_observer import RecoveryObservationError, preflight_recovery_intent
from streamt.deployer.state import (
    LocalState,
    ManagedResourceRecord,
    StateIdentityError,
    artifact_checksum,
    desired_managed_records,
    resource_id,
    updated_local_state,
)
from tests.unit.test_kafka_streams_compiler import _config
from tests.unit.test_plan_file import _state_observation

BACKEND = "kafka-streams-docker:v1:" + "b" * 64
CONTAINER = "c" * 64


def _planner(*, mode: str = "managed", exists: bool = False, owned: bool = False):
    config = _config()
    config["models"][0]["ownership"] = {"mode": mode}
    project = StreamtProject.model_validate(config)
    manifest = Compiler(project).compile(dry_run=True)
    artifact = parse_compiled_kafka_streams_job_artifact(manifest.artifacts["kafka_streams_jobs"][0])
    kafka = MagicMock(spec=KafkaDeployer)
    kafka.plan_topic.side_effect = lambda desired: TopicChange(
        topic=desired.name, action="none" if exists else "create", desired=desired,
        current=TopicState(desired.name, exists, partitions=desired.partitions),
    )
    kafka.get_consumer_groups.return_value = []
    kafka.apply_topic.return_value = "created"
    runner = MagicMock(spec=KafkaStreamsDeployer)
    runner.backend_identity = BACKEND
    current = KafkaStreamsJobState(
        artifact.name, exists, CONTAINER if exists else None,
        "running" if exists else None, artifact_checksum(artifact.to_dict()) if exists else None,
        "sha256:" + "d" * 64 if exists else None,
        artifact.image if exists else None,
    )
    change = KafkaStreamsJobChange(
        artifact.name, "none" if exists else "create", current, artifact,
        {
            "application_id": artifact.application_id,
            "image_id": artifact.image,
            "desired_artifact_hash": artifact_checksum(artifact.to_dict()),
            "topic_bindings": {artifact.plan["input_topic"]: "input-id", artifact.plan["output_topic"]: None},
            "initial_offset": artifact.initial_offset,
        },
        BACKEND,
    )
    runner.plan_job.side_effect = lambda _artifact, **_kwargs: copy.deepcopy(change)
    runner.apply_job.return_value = "created"
    prior = LocalState(project=manifest.project_name, environment="default")
    if owned:
        prior.resources[resource_id(manifest.project_name, "default", "kafka_streams_job", artifact.name)] = ManagedResourceRecord(
            artifact.application_id, "managed", artifact_checksum(artifact.to_dict()), BACKEND,
        )
        topic = manifest.artifacts["topics"][0]
        prior.resources[resource_id(manifest.project_name, "default", "topic", artifact.name)] = ManagedResourceRecord(
            str(topic["name"]), "managed", artifact_checksum(topic), "direct-kafka",
        )
    planner = DeploymentPlanner(
        manifest, kafka_deployer=kafka, kafka_streams_deployer=runner,
        project=project, prior_state=prior,
    )
    return planner, kafka, runner, change


@pytest.mark.parametrize("offline", [True, False])
def test_external_runner_is_declaration_only_even_without_any_provider(offline: bool) -> None:
    planner, kafka, runner, _ = _planner(mode="external")
    planner.kafka_deployer = None
    planner.kafka_streams_deployer = None
    plan = planner.offline_plan() if offline else planner.plan()
    change = plan.kafka_streams_changes[0]
    assert (change.action, change.current, change.backend_identity) == ("none", None, None)
    assert not plan.has_changes
    assert not plan.is_apply_blocked
    assert plan.ownership_requirements[-1].observed_action == "none"
    assert "declaration-only" in plan.ownership_requirements[-1].message
    assert planner.planned_actions(plan) == []
    assert desired_managed_records(plan, project=planner.project_name, environment="default") == {}
    assert planner.apply(plan)["errors"] == []
    assert not kafka.mock_calls
    assert not runner.mock_calls


def test_create_has_exact_journal_order_and_preflight_precedes_topic_mutation() -> None:
    planner, kafka, runner, _ = _planner()
    plan = planner.plan()
    assert plan.has_changes
    assert plan.creates == 2
    assert plan.updates == plan.deletes == 0
    assert [risk.kind for risk in plan.ordered_change_risks] == ["topic", "kafka_streams_job"]
    actions = planner.planned_actions(plan)
    assert [action.runtime_label for action in actions] == ["topic:valuable_orders", "kafka_streams_job:valuable_orders"]
    assert planner.operation_actions(plan) == [(action.runtime_label, action.action) for action in actions]
    durable = operation_actions_from_planned(actions)
    assert durable[-1].resource_id == resource_id(planner.project_name, "default", "kafka_streams_job", "valuable_orders")
    events = []
    runner.preflight.side_effect = lambda _change: events.append("runner preflight")
    kafka.apply_topic.side_effect = lambda _artifact: events.append("topic mutation") or "created"
    runner.apply_job.side_effect = lambda _change: events.append("runner mutation") or "created"
    result = planner.apply(
        plan,
        before_action=lambda label, index: events.append(("started", label, index)),
        after_action=lambda label, index, success: events.append(("completed", label, index, success)),
    )
    assert events == [
        "runner preflight", ("started", "topic:valuable_orders", 0), "topic mutation",
        ("completed", "topic:valuable_orders", 0, True),
        ("started", "kafka_streams_job:valuable_orders", 1), "runner mutation",
        ("completed", "kafka_streams_job:valuable_orders", 1, True),
    ]
    assert result["created"] == [action.runtime_label for action in actions]
    records = desired_managed_records(plan, project=planner.project_name, environment="default")
    assert records[durable[-1].resource_id].backend == BACKEND
    assert records[durable[-1].resource_id].physical_name == plan.kafka_streams_changes[0].desired.application_id
    assert all("source" not in resource for resource in records)


def test_repeat_owned_apply_is_noop_without_adoption_or_new_state_serial() -> None:
    planner, kafka, runner, _ = _planner(exists=True, owned=True)
    plan = planner.plan()
    assert not plan.has_changes
    assert not plan.is_apply_blocked
    assert planner.planned_actions(plan) == []
    assert planner.apply(plan)["errors"] == []
    assert updated_local_state(planner.prior_state, plan) is None
    kafka.apply_topic.assert_not_called()
    runner.apply_job.assert_not_called()
    runner.preflight.assert_called_once()


@pytest.mark.parametrize("method", ["plan", "planned_actions", "operation_actions", "apply"])
def test_missing_deployer_cannot_silently_drop_managed_jobs(method: str) -> None:
    planner, kafka, _, _ = _planner()
    plan = planner.plan()
    planner.kafka_streams_deployer = None
    with pytest.raises(StateIdentityError, match="bound Docker"):
        getattr(planner, method)(*(() if method == "plan" else (plan,)))
    kafka.apply_topic.assert_not_called()


def test_update_is_blocked_before_any_topic_mutation_and_names_custom_downstream_app() -> None:
    planner, kafka, runner, change = _planner(exists=True, owned=True)
    change.action = "update"
    change.blocker = "kafka_streams_replacement_not_verified"
    plan = planner.plan()
    assert plan.updates == 1
    assert plan.is_apply_blocked
    assert plan.ordered_safety_blockers[0].code == "kafka_streams_replacement_not_verified"
    assert plan.ordered_change_risks[0].assessment == "state_migration_required"
    assert plan.impact_radius[0].change_type == "kafka_streams_job_update"
    assert plan.impact_radius[0].exposures[0]["name"] == "fraud_application"
    assert plan.impact_radius[0].consumer_evidence["status"] == "verified"
    # Even a caller that clears the presentation-level blocker cannot bypass
    # direct apply's authoritative runner transition check.
    plan.safety_blockers.clear()
    with pytest.raises(StateIdentityError, match="lifecycle"):
        planner.apply(plan)
    kafka.apply_topic.assert_not_called()
    runner.apply_job.assert_not_called()


@pytest.mark.parametrize("failure", ["missing", "artifact", "backend", "physical"])
def test_persisted_runner_identity_drift_cannot_recreate_or_initialize_offsets(failure: str) -> None:
    planner, kafka, runner, change = _planner(exists=True, owned=True)
    if failure == "missing":
        change.current.exists = False
        change.action = "create"
    elif failure == "artifact":
        change.current.artifact_hash = "sha256:" + "e" * 64
    else:
        resource = resource_id(planner.project_name, "default", "kafka_streams_job", change.job_name)
        prior = planner.prior_state.resources[resource]
        planner.prior_state.resources[resource] = ManagedResourceRecord(
            "other-physical" if failure == "physical" else prior.physical_name,
            prior.ownership, prior.artifact_checksum,
            "kafka-streams-docker:v1:" + "e" * 64 if failure == "backend" else prior.backend,
        )
    plan = planner.plan()
    assert plan.is_apply_blocked
    with pytest.raises(StateIdentityError):
        planner.apply(plan)
    kafka.apply_topic.assert_not_called()
    runner.apply_job.assert_not_called()


def test_unowned_live_runner_requires_explicit_adoption_before_any_mutation() -> None:
    planner, kafka, runner, _ = _planner(exists=True)
    plan = planner.plan()
    requirement = next(item for item in plan.ownership_requirements if item.kind == "kafka_streams_job")
    assert requirement.reason == "requires_adoption"
    with pytest.raises(StateIdentityError, match="persisted ownership"):
        planner.apply(plan)
    kafka.apply_topic.assert_not_called()
    runner.apply_job.assert_not_called()


@pytest.mark.parametrize("failure", ["duplicate", "environment", "malformed", "foreign"])
def test_job_claims_fail_closed_before_provider_observation(failure: str) -> None:
    planner, kafka, runner, _ = _planner(mode="external")
    jobs = planner.manifest.artifacts["kafka_streams_jobs"]
    if failure == "duplicate":
        jobs.append(copy.deepcopy(jobs[0]))
    elif failure == "environment":
        jobs[0]["application_id"] = application_id(planner.project_name, "another-env", "valuable_orders")
    elif failure == "malformed":
        jobs[0]["ownership"] = None
    else:
        jobs[0]["ownership"]["project"] = "foreign-project"
        jobs[0]["application_id"] = application_id("foreign-project", "default", "valuable_orders")
    if failure == "foreign":
        plan = planner.plan()
        assert plan.is_apply_blocked
        assert any(item.reason == "ownership_mismatch" for item in plan.ownership_requirements)
    else:
        with pytest.raises(ValueError):
            planner.plan()
    kafka.plan_topic.assert_not_called()
    runner.plan_job.assert_not_called()


@pytest.mark.parametrize("failure", ["omitted", "unknown_verb", "retargeted", "none_without_ownership"])
def test_hand_constructed_plan_cannot_bypass_exact_job_preflight(failure: str) -> None:
    planner, kafka, runner, _ = _planner()
    plan = planner.plan()
    if failure == "omitted":
        plan.kafka_streams_changes.clear()
    elif failure == "unknown_verb":
        plan.kafka_streams_changes[0].action = "restart"
    elif failure == "retargeted":
        plan.kafka_streams_changes[0].desired.plan["output_topic"] = "another-topic"
    else:
        plan.kafka_streams_changes[0].action = "none"
    with pytest.raises(StateIdentityError):
        planner.apply(plan)
    kafka.apply_topic.assert_not_called()
    runner.apply_job.assert_not_called()


def test_failed_runner_preflight_prevents_topic_create_and_journal_started_callback() -> None:
    planner, kafka, runner, _ = _planner()
    plan = planner.plan()
    runner.preflight.side_effect = ValueError("topic UUID changed")
    started = MagicMock()
    with pytest.raises(ValueError, match="UUID"):
        planner.apply(plan, before_action=started)
    started.assert_not_called()
    kafka.apply_topic.assert_not_called()
    runner.apply_job.assert_not_called()


def test_ambiguous_runner_failure_does_not_delete_topics_or_claim_rollback() -> None:
    planner, kafka, runner, _ = _planner()
    plan = planner.plan()
    runner.apply_job.side_effect = OSError("create outcome unknown")
    result = planner.apply(plan)
    assert result["errors"]
    assert result["rollback_candidates"] == []
    kafka.delete_topic.assert_not_called()
    rolled_back, errors = planner.rollback(["kafka_streams_job:valuable_orders"])
    assert rolled_back == []
    assert len(errors) == 1


def test_reviewed_plan_covers_runner_container_topic_and_backend_evidence() -> None:
    planner, _, _, _ = _planner()
    plan = planner.plan()
    observation = _state_observation(project=planner.project_name, environment="default")
    actions = operation_actions_from_planned(planner.planned_actions(plan))
    reviewed = ReviewedPlanFile.create(
        plan, planner.manifest, project=planner.project_name, environment="default",
        runtime=planner.project.runtime, state=StateReference.from_observation(observation), actions=actions,
    )
    resources = deployment_plan_payload(plan)["resources"]
    entry = next(item for item in resources if item["kind"] == "kafka_streams_job")
    assert entry["changes"]["backend_identity"] == BACKEND
    for field, value in (
        ("container_id", "different-container"), ("artifact_hash", "sha256:" + "e" * 64),
        ("input_topic_id", "recreated-input"), ("output_topic_id", "recreated-output"),
        ("network_id", "recreated-network"),
    ):
        changed = copy.deepcopy(plan)
        setattr(changed.kafka_streams_changes[0].current, field, value)
        with pytest.raises(StalePlanError):
            reviewed.verify_current_plan(changed, actions=actions, state_observation=observation)
    changed = copy.deepcopy(plan)
    changed.kafka_streams_changes[0].changes["topic_bindings"]["orders.raw.v1"] = "replacement-topic-id"
    with pytest.raises(StalePlanError):
        reviewed.verify_current_plan(changed, actions=actions, state_observation=observation)


def test_pending_runner_recovery_is_explicitly_blocked_without_observation() -> None:
    from tests.unit.test_recovery_observer import _action, _snapshot, _state

    action = _action("kafka_streams_job", "valuable_orders", "create")
    snapshot = _snapshot(_state(), (action,))
    with pytest.raises(RecoveryObservationError, match="pending operation must remain intact"):
        preflight_recovery_intent(snapshot)
