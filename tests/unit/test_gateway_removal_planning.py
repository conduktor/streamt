"""One-snapshot planning outcomes for explicit Gateway removals."""

from __future__ import annotations

import json
from dataclasses import FrozenInstanceError
from unittest.mock import MagicMock, call

import pytest

from streamt.compiler.gateway_artifact import parse_compiled_gateway_rule_artifact
from streamt.compiler.manifest import ArtifactOwnership, Manifest
from streamt.core.models import ProjectInfo, StreamtProject
from streamt.core.runtime import (
    ConduktorConfig,
    GatewayConfig,
    KafkaConfig,
    RuntimeConfig,
)
from streamt.deployer.gateway import (
    GatewayBackendBinding,
    GatewayDeployer,
    ManagedGatewayRuleObservation,
    build_desired_gateway_rule,
)
from streamt.deployer.planner import (
    DeploymentPlan,
    DeploymentPlanner,
    GatewayRemovalAssessment,
)
from streamt.deployer.state import (
    LocalState,
    ManagedResourceRecord,
    StateIdentityError,
    artifact_checksum,
    resource_id,
)
from streamt.deployer.state_backend import (
    GatewayActionEvidence,
    GatewayActionSurfaceEvidence,
    OperationAction,
)

_ENDPOINT = "https://gateway.example.test/admin"
_VCLUSTER = "payments-prod"


def _binding() -> GatewayBackendBinding:
    return GatewayBackendBinding.from_endpoint(
        _ENDPOINT,
        virtual_cluster=_VCLUSTER,
    )


def _project() -> StreamtProject:
    return StreamtProject(
        project=ProjectInfo(name="payments"),
        runtime=RuntimeConfig(
            kafka=KafkaConfig(bootstrap_servers="broker.invalid:9092"),
            conduktor=ConduktorConfig(
                gateway=GatewayConfig(
                    admin_url=_ENDPOINT,
                    virtual_cluster=_VCLUSTER,
                )
            ),
        ),
    )


def _ownership(owner: str) -> dict[str, str]:
    return ArtifactOwnership(
        project="payments",
        owner_type="model",
        owner_name=owner,
        mode="managed",
    ).to_dict()


def _rule(
    *,
    owner: str,
    rule: str,
    alias: str,
    physical: str = "orders.raw",
    where: str | None = None,
) -> dict[str, object]:
    interceptors: list[dict[str, object]] = []
    if where is not None:
        interceptors.append({"type": "filter", "config": {"where": where}})
    return {
        "name": rule,
        "virtualTopic": alias,
        "physicalTopic": physical,
        "interceptors": interceptors,
        "ownership": _ownership(owner),
    }


def _removal(
    *,
    owner: str,
    rule: str,
    alias: str,
    physical: str = "orders.raw",
    where: str | None = None,
) -> dict[str, object]:
    return {
        "logicalOwner": owner,
        "priorArtifact": _rule(
            owner=owner,
            rule=rule,
            alias=alias,
            physical=physical,
            where=where,
        ),
    }


def _manifest(
    *,
    desired: list[dict[str, object]] | None = None,
    removals: list[dict[str, object]] | None = None,
) -> Manifest:
    artifacts: dict[str, list[dict[str, object]]] = {
        "gateway_rules": list(desired or []),
    }
    if removals:
        artifacts["gateway_rule_removals"] = list(removals)
    return Manifest(
        version="1.0.0",
        project_name="payments",
        artifacts=artifacts,
    )


def _state(*removals: dict[str, object]) -> LocalState:
    resources: dict[str, ManagedResourceRecord] = {}
    for removal in removals:
        owner = removal["logicalOwner"]
        prior_artifact = removal["priorArtifact"]
        assert isinstance(owner, str)
        assert isinstance(prior_artifact, dict)
        alias = prior_artifact["virtualTopic"]
        assert isinstance(alias, str)
        resources[resource_id("payments", "prod", "gateway_rule", owner)] = ManagedResourceRecord(
            physical_name=alias,
            ownership="managed",
            artifact_checksum=artifact_checksum(prior_artifact),
            backend=_binding().backend_identity,
        )
    return LocalState(
        project="payments",
        environment="prod",
        resources=resources,
    )


def _removal_observation(
    removal: dict[str, object],
    *,
    exists: bool,
) -> ManagedGatewayRuleObservation:
    prior_artifact = removal["priorArtifact"]
    assert isinstance(prior_artifact, dict)
    rule = prior_artifact["name"]
    alias = prior_artifact["virtualTopic"]
    physical = prior_artifact["physicalTopic"]
    assert isinstance(rule, str)
    assert isinstance(alias, str)
    assert isinstance(physical, str)
    return ManagedGatewayRuleObservation(
        binding=_binding(),
        logical_name=rule,
        alias_name=alias,
        exists=exists,
        physical_name=physical if exists else None,
        physical_cluster="main" if exists else None,
    )


def _gateway(
    observations: dict[tuple[str, str], ManagedGatewayRuleObservation],
) -> tuple[GatewayDeployer, MagicMock]:
    gateway = MagicMock(spec=GatewayDeployer)
    gateway.cluster_binding = _binding()
    snapshot = MagicMock()
    snapshot.binding = _binding()
    snapshot.rule.side_effect = lambda rule, alias: observations[(rule, alias)]
    gateway.observe_managed_gateway_snapshot.return_value = snapshot
    return gateway, snapshot


def _assert_no_gateway_mutation(gateway: GatewayDeployer) -> None:
    gateway.apply_managed_gateway_rule.assert_not_called()  # type: ignore[attr-defined]
    gateway.delete_managed_gateway_rule.assert_not_called()  # type: ignore[attr-defined]
    gateway.create_interceptor.assert_not_called()  # type: ignore[attr-defined]
    gateway.delete_interceptor.assert_not_called()  # type: ignore[attr-defined]
    gateway.create_alias_topic.assert_not_called()  # type: ignore[attr-defined]
    gateway.delete_alias_topic.assert_not_called()  # type: ignore[attr-defined]
    gateway.apply.assert_not_called()  # type: ignore[attr-defined]
    gateway.delete.assert_not_called()  # type: ignore[attr-defined]


@pytest.mark.parametrize(
    (
        "has_prior",
        "exists",
        "expected_status",
        "expected_blocker",
        "expected_requirement",
        "expects_delete",
    ),
    [
        (True, True, None, None, None, True),
        (
            True,
            False,
            "state_provider_drift",
            "gateway_removal_state_provider_drift",
            None,
            False,
        ),
        (False, False, "already_absent", None, None, False),
        (
            False,
            True,
            "ownership_required",
            None,
            "requires_adoption",
            False,
        ),
    ],
    ids=["owned-present", "owned-absent", "unowned-absent", "unowned-present"],
)
def test_live_removal_classification_is_exact_and_never_mutates(
    has_prior: bool,
    exists: bool,
    expected_status: str | None,
    expected_blocker: str | None,
    expected_requirement: str | None,
    expects_delete: bool,
) -> None:
    removal = _removal(
        owner="orders_view",
        rule="orders_access_rule",
        alias="orders.public",
    )
    current = _removal_observation(removal, exists=exists)
    gateway, _snapshot = _gateway({(current.logical_name, current.alias_name): current})
    prior_state = _state(removal) if has_prior else _state()
    prior_state_before = prior_state.to_dict()
    planner = DeploymentPlanner(
        _manifest(removals=[removal]),
        project=_project(),
        prior_state=prior_state,
        environment="prod",
        gateway_deployer=gateway,
    )

    plan = planner.plan()

    assert [change.action for change in plan.gateway_changes] == (
        ["delete"] if expects_delete else []
    )
    assert [assessment.status for assessment in plan.gateway_removal_assessments] == (
        [expected_status] if expected_status is not None else []
    )
    assert [blocker.code for blocker in plan.safety_blockers] == (
        [expected_blocker] if expected_blocker is not None else []
    )
    assert [requirement.reason for requirement in plan.ownership_requirements] == (
        [expected_requirement] if expected_requirement is not None else []
    )
    assert plan.is_apply_blocked is bool(expected_blocker or expected_requirement)
    gateway.observe_managed_gateway_snapshot.assert_called_once_with()  # type: ignore[attr-defined]
    _assert_no_gateway_mutation(gateway)
    assert prior_state.to_dict() == prior_state_before

    actions = planner.planned_actions(plan)
    if expects_delete:
        assert len(actions) == 1
        action = actions[0]
        assert action.resource_id == resource_id("payments", "prod", "gateway_rule", "orders_view")
        assert action.runtime_label == "gateway_rule:orders_access_rule"
        assert action.action == "delete"
        assert action.gateway_evidence is not None
        assert action.gateway_evidence.rule_name == "orders_access_rule"
        assert action.gateway_evidence.alias_name == "orders.public"
        assert action.gateway_evidence.current.exists is True
        assert action.gateway_evidence.desired.exists is False
    else:
        assert actions == []


def test_removal_rejects_mismatched_snapshot_rule_without_mutation() -> None:
    removal = _removal(
        owner="orders_view",
        rule="orders_rule",
        alias="orders.public",
    )
    wrong_current = ManagedGatewayRuleObservation(
        binding=_binding(),
        logical_name="another_rule",
        alias_name="orders.public",
        exists=True,
        physical_name="orders.raw",
        physical_cluster="main",
    )
    gateway, snapshot = _gateway({("orders_rule", "orders.public"): wrong_current})

    with pytest.raises(StateIdentityError, match="does not match its exact target"):
        DeploymentPlanner(
            _manifest(removals=[removal]),
            project=_project(),
            prior_state=_state(removal),
            environment="prod",
            gateway_deployer=gateway,
        ).plan()

    gateway.observe_managed_gateway_snapshot.assert_called_once_with()  # type: ignore[attr-defined]
    snapshot.rule.assert_called_once_with("orders_rule", "orders.public")
    _assert_no_gateway_mutation(gateway)


def test_desired_removals_and_recovery_share_one_snapshot() -> None:
    desired = _rule(
        owner="desired_owner",
        rule="desired_rule",
        alias="orders.desired",
        physical="orders.desired.raw",
    )
    owned_removal = _removal(
        owner="removed_owner",
        rule="removed_rule",
        alias="orders.removed",
        physical="orders.removed.raw",
    )
    absent_removal = _removal(
        owner="absent_owner",
        rule="absent_rule",
        alias="orders.absent",
        physical="orders.absent.raw",
    )
    recovery_target = _removal(
        owner="recovery_owner",
        rule="recovery_rule",
        alias="orders.recovery",
        physical="orders.recovery.raw",
    )
    desired_current = build_desired_gateway_rule(
        parse_compiled_gateway_rule_artifact(desired),
        _binding(),
    )
    removal_current = _removal_observation(owned_removal, exists=True)
    absent_current = _removal_observation(absent_removal, exists=False)
    recovery_current = _removal_observation(recovery_target, exists=True)
    recovery_absent = _removal_observation(recovery_target, exists=False)
    gateway, snapshot = _gateway(
        {
            (desired_current.logical_name, desired_current.alias_name): desired_current,
            (removal_current.logical_name, removal_current.alias_name): removal_current,
            (absent_current.logical_name, absent_current.alias_name): absent_current,
            (recovery_current.logical_name, recovery_current.alias_name): recovery_current,
        }
    )
    recovery_action = OperationAction(
        index=0,
        resource_id=resource_id("payments", "prod", "gateway_rule", "recovery_owner"),
        action="delete",
        gateway_evidence=GatewayActionEvidence(
            version=1,
            backend_identity=_binding().backend_identity,
            rule_name="recovery_rule",
            alias_name="orders.recovery",
            current=GatewayActionSurfaceEvidence(
                exists=True,
                fingerprint=recovery_current.fingerprint,
                managed_interceptor_count=0,
            ),
            desired=GatewayActionSurfaceEvidence(
                exists=False,
                fingerprint=recovery_absent.fingerprint,
                managed_interceptor_count=0,
            ),
        ),
    )
    planner = DeploymentPlanner(
        _manifest(desired=[desired], removals=[owned_removal, absent_removal]),
        project=_project(),
        prior_state=_state(owned_removal, recovery_target),
        environment="prod",
        gateway_deployer=gateway,
    )

    plan = planner.plan(gateway_recovery_actions=(recovery_action,))

    gateway.observe_managed_gateway_snapshot.assert_called_once_with()  # type: ignore[attr-defined]
    assert snapshot.rule.call_args_list == [
        call("desired_rule", "orders.desired"),
        call("removed_rule", "orders.removed"),
        call("absent_rule", "orders.absent"),
        call("recovery_rule", "orders.recovery"),
    ]
    assert [change.action for change in plan.gateway_changes] == ["none", "delete"]
    assert [assessment.status for assessment in plan.gateway_removal_assessments] == [
        "already_absent"
    ]
    assert len(plan.gateway_recovery_observations) == 1
    assert plan.gateway_recovery_observations[0].observation == recovery_current
    _assert_no_gateway_mutation(gateway)


def test_offline_removals_are_ordered_blocking_and_secret_neutral() -> None:
    first = _removal(
        owner="z_owner",
        rule="z_rule",
        alias="orders.z",
        physical="secret.physical.topic",
        where="tenant_secret = 'raw-value'",
    )
    second = _removal(
        owner="a_owner",
        rule="a_rule",
        alias="orders.a",
        physical="orders.a.raw",
    )
    gateway, _snapshot = _gateway({})
    plan = DeploymentPlanner(
        _manifest(removals=[first, second]),
        project=_project(),
        environment="prod",
        gateway_deployer=gateway,
    ).offline_plan()

    assert [assessment.logical_owner for assessment in plan.gateway_removal_assessments] == [
        "z_owner",
        "a_owner",
    ]
    assert [assessment.status for assessment in plan.gateway_removal_assessments] == [
        "offline_unverified",
        "offline_unverified",
    ]
    assert [blocker.resource for blocker in plan.ordered_safety_blockers] == [
        resource_id("payments", "prod", "gateway_rule", "a_owner"),
        resource_id("payments", "prod", "gateway_rule", "z_owner"),
    ]
    assert plan.is_apply_blocked is True
    assert plan.has_changes is False
    gateway.observe_managed_gateway_snapshot.assert_not_called()  # type: ignore[attr-defined]
    _assert_no_gateway_mutation(gateway)

    reviewed = json.dumps(
        [assessment.to_dict() for assessment in plan.gateway_removal_assessments],
        sort_keys=True,
    )
    rendered = repr(plan)
    for sensitive in (
        _ENDPOINT,
        "secret.physical.topic",
        "tenant_secret",
        "raw-value",
    ):
        assert sensitive not in reviewed
        assert sensitive not in rendered

    plan.safety_blockers.clear()
    plan.refresh_safety_blockers()
    assert len(plan.safety_blockers) == 2
    plan.gateway_removal_assessments = ()
    plan.refresh_safety_blockers()
    assert plan.safety_blockers == []


def test_removal_assessment_is_strict_immutable_and_serializable() -> None:
    assessment = GatewayRemovalAssessment(
        resource_id=resource_id("payments", "prod", "gateway_rule", "orders_view"),
        logical_owner="orders_view",
        rule_name="orders_rule",
        alias_name="orders.public",
        backend_identity=_binding().backend_identity,
        status="already_absent",
    )

    assert assessment.to_dict() == {
        "resource_id": resource_id("payments", "prod", "gateway_rule", "orders_view"),
        "logical_owner": "orders_view",
        "rule_name": "orders_rule",
        "alias_name": "orders.public",
        "backend_identity": _binding().backend_identity,
        "status": "already_absent",
    }
    with pytest.raises(FrozenInstanceError):
        assessment.status = "offline_unverified"  # type: ignore[misc]
    with pytest.raises(StateIdentityError, match="mismatched identity evidence"):
        GatewayRemovalAssessment(
            resource_id=assessment.resource_id,
            logical_owner="other_owner",
            rule_name=assessment.rule_name,
            alias_name=assessment.alias_name,
            backend_identity=assessment.backend_identity,
            status=assessment.status,
        )
    with pytest.raises(StateIdentityError, match="mismatched identity evidence"):
        GatewayRemovalAssessment(
            resource_id=assessment.resource_id,
            logical_owner=assessment.logical_owner,
            rule_name=assessment.rule_name,
            alias_name=assessment.alias_name,
            backend_identity=assessment.backend_identity,
            status="unknown",  # type: ignore[arg-type]
        )
    with pytest.raises(StateIdentityError, match="assessments are invalid"):
        DeploymentPlan(
            gateway_removal_assessments=[assessment],  # type: ignore[arg-type]
        )
