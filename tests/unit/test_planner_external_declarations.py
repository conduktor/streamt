"""External declarations preserve identities without implicit provider observation."""

from __future__ import annotations

from copy import deepcopy
from unittest.mock import MagicMock

import pytest

from streamt.cli.commands.apply import filter_manifest_for_selection
from streamt.compiler.manifest import ArtifactOwnership, Manifest
from streamt.deployer.planner import DeploymentPlan, DeploymentPlanner
from streamt.deployer.state import (
    StateFormatError,
    StateIdentityError,
    desired_managed_records,
    updated_local_state,
)
from tests.unit import test_connector_removal_planning as connector_removal
from tests.unit import test_gateway_removal_planning as gateway_removal
from tests.unit import test_planner_connector_recovery as connector_recovery
from tests.unit import test_planner_gateway_artifacts as gateway_artifacts
from tests.unit.test_planner_ownership import _deployers, _manifest, _prior_state, _project

_KINDS = ("schemas", "topics", "flink_jobs", "connectors", "gateway_rules")


def _changes(plan: DeploymentPlan) -> list[object]:
    return [
        *plan.schema_changes,
        *plan.topic_changes,
        *plan.flink_changes,
        *plan.connector_changes,
        *plan.gateway_changes,
    ]


def _no_observation_deployers() -> dict[str, MagicMock]:
    deployers = _deployers(exists=False)
    for deployer in deployers.values():
        for method in (
            "plan_schema",
            "plan_topic",
            "plan_job",
            "plan_connector",
            "resolve_connector_artifact",
            "require_cluster_binding",
            "observe_managed_connector",
            "observe_managed_gateway_snapshot",
            "get_consumer_groups",
        ):
            getattr(deployer, method).side_effect = AssertionError(
                "External declaration attempted provider observation"
            )
    return deployers


@pytest.mark.parametrize("offline", [False, True])
@pytest.mark.parametrize("providers_configured", [False, True])
def test_all_external_artifacts_are_retained_without_observation(
    offline: bool, providers_configured: bool
) -> None:
    deployers = _no_observation_deployers() if providers_configured else {}
    planner = DeploymentPlanner(
        _manifest("external"), project=_project(), **deployers
    )
    plan = planner.offline_plan() if offline else planner.plan()

    changes = _changes(plan)
    assert len(changes) == 5
    assert all(change.action == "none" for change in changes)
    assert all(change.current is None for change in changes)
    assert all(change.desired is not None for change in changes)
    assert len(plan.ownership_requirements) == 5
    assert {requirement.reason for requirement in plan.ownership_requirements} == {"external"}
    assert {requirement.observed_action for requirement in plan.ownership_requirements} == {"none"}
    assert all(
        "declaration-only" in requirement.message
        and "not observed" in requirement.message
        for requirement in plan.ownership_requirements
    )
    assert not plan.has_changes
    assert not plan.is_apply_blocked
    assert not plan.impact_radius
    assert planner.planned_actions(plan) == []
    assert all(deployer.mock_calls == [] for deployer in deployers.values())


@pytest.mark.parametrize("kind", _KINDS)
@pytest.mark.parametrize("offline", [False, True])
def test_foreign_external_ownership_remains_a_blocker(kind: str, offline: bool) -> None:
    manifest = _manifest("external")
    manifest.artifacts[kind][0]["ownership"]["project"] = "another-project"
    deployers = _no_observation_deployers()
    planner = DeploymentPlanner(manifest, project=_project(), **deployers)
    plan = planner.offline_plan() if offline else planner.plan()
    assert plan.is_apply_blocked
    assert {requirement.reason for requirement in plan.blocking_ownership_requirements} == {
        "ownership_mismatch"
    }
    assert all(change.current is None for change in _changes(plan))
    assert all(deployer.mock_calls == [] for deployer in deployers.values())


@pytest.mark.parametrize("kind", _KINDS)
@pytest.mark.parametrize(
    "ownership",
    [
        None,
        {},
        {"mode": "external"},
        "external",
        {"project": "payments", "type": "model", "name": "owner", "mode": "unknown"},
        {"project": "payments", "type": "model", "name": " ", "mode": "external"},
    ],
)
def test_malformed_ownership_cannot_become_legacy_managed_authority(
    kind: str, ownership: object
) -> None:
    manifest = _manifest("external")
    manifest.artifacts[kind][0]["ownership"] = ownership
    deployers = _no_observation_deployers()
    planner = DeploymentPlanner(manifest, project=_project(), **deployers)
    for action in (planner.plan, planner.offline_plan):
        with pytest.raises(StateIdentityError, match="malformed ownership"):
            action()
    assert all(deployer.mock_calls == [] for deployer in deployers.values())


def _mixed_manifest() -> Manifest:
    manifest = _manifest("managed")
    external = _manifest("external")
    for kind in _KINDS:
        artifact = external.artifacts[kind][0]
        artifact["ownership"]["name"] += "_external"
        key = "subject" if kind == "schemas" else "name"
        artifact[key] += ".external"
        if kind == "connectors":
            artifact["config"]["name"] = artifact["name"]
        if kind == "gateway_rules":
            artifact["virtualTopic"] += ".external"
        manifest.artifacts[kind].insert(0, artifact)
    return manifest


@pytest.mark.parametrize("external_first", [False, True])
def test_mixed_managed_planning_keeps_live_reads_and_single_gateway_snapshot(
    external_first: bool,
) -> None:
    deployers = _deployers(exists=False)
    deployers["kafka_deployer"].get_consumer_groups.return_value = []
    manifest = _mixed_manifest()
    if not external_first:
        for artifacts in manifest.artifacts.values():
            artifacts.reverse()
    plan = DeploymentPlanner(
        manifest, project=_project(), **deployers
    ).plan()
    assert len(_changes(plan)) == 10
    assert plan.creates == 5
    assert len(plan.ownership_requirements) == 5
    for changes in (
        plan.schema_changes,
        plan.topic_changes,
        plan.flink_changes,
        plan.connector_changes,
        plan.gateway_changes,
    ):
        external, managed = changes if external_first else reversed(changes)
        assert external.action == "none"
        assert external.current is None
        assert managed.action != "none"
        assert managed.current is not None
    deployers["schema_registry_deployer"].plan_schema.assert_called_once()
    deployers["kafka_deployer"].plan_topic.assert_called_once()
    deployers["kafka_deployer"].get_consumer_groups.assert_called_once()
    deployers["flink_deployer"].plan_job.assert_called_once()
    deployers["connect_deployer"].plan_connector.assert_called_once()
    gateway = deployers["gateway_deployer"]
    gateway.observe_managed_gateway_snapshot.assert_called_once()
    gateway.observe_managed_gateway_snapshot.return_value.rule.assert_called_once_with(
        "alias_rule", "virtual-events"
    )


@pytest.mark.parametrize("kind", ["connectors", "gateway_rules"])
def test_external_targets_still_participate_in_provider_identity_collision_checks(
    kind: str,
) -> None:
    manifest = _manifest("external")
    duplicate = deepcopy(manifest.artifacts[kind][0])
    duplicate["ownership"]["mode"] = "managed"
    duplicate["ownership"]["name"] += "_another_owner"
    manifest.artifacts[kind].append(duplicate)
    deployers = _no_observation_deployers()
    with pytest.raises(StateIdentityError, match="duplicate"):
        DeploymentPlanner(manifest, project=_project(), **deployers).plan()
    assert all(deployer.mock_calls == [] for deployer in deployers.values())


def test_external_connector_cannot_hide_a_removal_collision() -> None:
    external = connector_removal._artifact("owner", "sink")
    external.ownership = ArtifactOwnership("payments", "model", "owner", "external")
    manifest = connector_removal._manifest(connector_removal._removal("owner", "sink"))
    manifest.artifacts["connectors"] = [external.to_dict()]
    with pytest.raises(ValueError, match="collide"):
        DeploymentPlanner(
            manifest,
            project=connector_removal._project(),
            prior_state=connector_removal._state(),
        ).plan()


def test_external_gateway_cannot_hide_a_removal_collision() -> None:
    removal = gateway_removal._removal(owner="owner", rule="rule", alias="alias")
    external = deepcopy(removal["priorArtifact"])
    external["ownership"]["mode"] = "external"
    with pytest.raises(StateIdentityError, match="collid"):
        DeploymentPlanner(
            gateway_removal._manifest(desired=[external], removals=[removal]),
            project=gateway_removal._project(),
            prior_state=gateway_removal._state(),
        ).plan()


def test_external_connector_does_not_suppress_unrelated_removal_observation() -> None:
    external = connector_removal._artifact("external_owner", "external_sink")
    external.ownership = ArtifactOwnership(
        "payments", "model", "external_owner", "external"
    )
    manifest = connector_removal._manifest(connector_removal._removal("removed", "removed_sink"))
    manifest.artifacts["connectors"] = [external.to_dict()]
    planner, deployer = connector_removal._planner(
        manifest,
        {"removed_sink": connector_removal._absent("removed_sink")},
        state=connector_removal._state(),
    )
    plan = planner.plan()
    assert plan.connector_changes[0].current is None
    deployer.observe_managed_connector.assert_called_once_with("removed_sink")
    assert plan.connector_removal_assessments[0].status == "already_absent"


def test_external_gateway_does_not_suppress_unrelated_removal_snapshot() -> None:
    removal = gateway_removal._removal(owner="removed", rule="removed_rule", alias="removed_alias")
    external = gateway_removal._rule(owner="external", rule="external_rule", alias="external_alias")
    external["ownership"]["mode"] = "external"
    gateway, snapshot = gateway_removal._gateway({
        ("removed_rule", "removed_alias"): gateway_removal._removal_observation(removal, exists=False),
    })
    plan = DeploymentPlanner(
        gateway_removal._manifest(desired=[external], removals=[removal]),
        project=gateway_removal._project(),
        prior_state=gateway_removal._state(),
        gateway_deployer=gateway,
    ).plan()
    assert plan.gateway_changes[0].current is None
    snapshot.rule.assert_called_once_with("removed_rule", "removed_alias")
    gateway.observe_managed_gateway_snapshot.assert_called_once()
    assert plan.gateway_removal_assessments[0].status == "already_absent"


def test_external_connector_cannot_hide_a_recovery_delete_collision() -> None:
    artifact = connector_recovery._artifact("owner", "sink")
    action = connector_recovery._action(artifact)
    state = connector_recovery._state(artifact)
    external = artifact.to_dict()
    external["ownership"]["mode"] = "external"
    deployer = connector_recovery._deployer({})
    planner = connector_recovery._planner(
        Manifest(version="1.0", project_name="payments", artifacts={"connectors": [external]}),
        deployer,
        state,
    )
    with pytest.raises(StateIdentityError, match="collides with a desired Connector"):
        planner.plan(connector_recovery_actions=(action,))
    deployer.observe_managed_connector.assert_not_called()


@pytest.mark.parametrize("action_name", ["create", "update", "adopt", "delete"])
def test_external_gateway_cannot_become_a_recovery_mutation_target(action_name: str) -> None:
    rule = gateway_artifacts._rule()
    action = gateway_artifacts._recovery_action(rule, action_name)
    state = gateway_artifacts._prior_state() if action_name == "adopt" else gateway_artifacts._prior_state(rule)
    rule["ownership"]["mode"] = "external"
    planner = DeploymentPlanner(
        gateway_artifacts._manifest(rule),
        project=gateway_artifacts._project(),
        prior_state=state,
    )
    with pytest.raises(StateIdentityError, match=r"external|still present"):
        planner.plan(gateway_recovery_actions=(action,))


def test_external_noops_never_add_state_or_relinquish_existing_state() -> None:
    prior = _prior_state()
    plan = DeploymentPlanner(
        _manifest("external"), project=_project(), prior_state=prior
    ).plan()
    assert desired_managed_records(plan, project="payments", environment="prod") == {}
    assert updated_local_state(prior, plan) is None
    assert prior == _prior_state()


@pytest.mark.parametrize("invalid", ["foreign", "managed", "actionable", "malformed"])
def test_external_state_projection_exemption_requires_exact_safe_ownership(invalid: str) -> None:
    plan = DeploymentPlanner(_manifest("external"), project=_project()).plan()
    change = plan.gateway_changes[0]
    assert change.desired is not None
    if invalid == "actionable":
        change.action = "create"
    elif invalid == "malformed":
        change.desired.ownership = {"mode": "external"}
    else:
        change.desired.ownership = ArtifactOwnership(
            "another-project" if invalid == "foreign" else "payments",
            "model",
            "alias_rule",
            "managed" if invalid == "managed" else "external",
        )
    with pytest.raises(StateFormatError, match="backend identity"):
        desired_managed_records(plan, project="payments", environment="default")


@pytest.mark.parametrize("kind", ["topics", "schemas"])
@pytest.mark.parametrize("mode", ["managed", "adopted", None])
@pytest.mark.parametrize("offline", [False, True])
@pytest.mark.parametrize("external_first", [False, True])
def test_external_topic_or_schema_cannot_overlap_a_managed_compiled_identity(
    kind: str, mode: str | None, offline: bool, external_first: bool,
) -> None:
    external = _manifest("external").artifacts[kind][0]
    managed = deepcopy(external)
    if mode is None:
        managed.pop("ownership")
    else:
        managed["ownership"]["mode"] = mode
        managed["ownership"]["name"] = "managed_owner"
    manifest = Manifest(
        version="1.0", project_name="payments",
        artifacts={kind: [external, managed] if external_first else [managed, external]},
    )
    deployers = _no_observation_deployers()
    planner = DeploymentPlanner(manifest, project=_project(), **deployers)
    with pytest.raises(StateIdentityError, match="declared both external and managed"):
        planner.offline_plan() if offline else planner.plan()
    assert all(deployer.mock_calls == [] for deployer in deployers.values())


@pytest.mark.parametrize("kind", ["topics", "schemas"])
@pytest.mark.parametrize("offline", [False, True])
def test_multiple_external_aliases_remain_read_only(kind: str, offline: bool) -> None:
    external = _manifest("external").artifacts[kind][0]
    alias = deepcopy(external)
    alias["ownership"]["name"] = "another_external_owner"
    manifest = Manifest(
        version="1.0", project_name="payments", artifacts={kind: [external, alias]},
    )
    deployers = _no_observation_deployers()
    planner = DeploymentPlanner(manifest, project=_project(), **deployers)
    plan = planner.offline_plan() if offline else planner.plan()
    assert len(_changes(plan)) == 2
    assert all(change.action == "none" and change.current is None for change in _changes(plan))
    assert not plan.is_apply_blocked
    assert all(deployer.mock_calls == [] for deployer in deployers.values())


@pytest.mark.parametrize("kind", ["topics", "schemas"])
def test_selective_apply_keeps_external_topic_and_schema_reservations(kind: str) -> None:
    external = _manifest("external").artifacts[kind][0]
    managed = deepcopy(external)
    managed["ownership"]["mode"] = "managed"
    managed["ownership"]["name"] = "selected_owner"
    manifest = Manifest(
        version="1.0", project_name="payments", artifacts={kind: [external, managed]},
    )
    filter_manifest_for_selection(manifest, {"selected_owner"}, {"selected_owner"})
    assert len(manifest.artifacts[kind]) == 2
    deployers = _no_observation_deployers()
    with pytest.raises(StateIdentityError, match="declared both external and managed"):
        DeploymentPlanner(manifest, project=_project(), **deployers).plan()
    assert all(deployer.mock_calls == [] for deployer in deployers.values())
