"""Exact normalized Gateway planner mutation contracts."""

from __future__ import annotations

import json
from unittest.mock import MagicMock

import pytest

from streamt.compiler.manifest import (
    ArtifactOwnership,
    GatewayRuleArtifact,
    Manifest,
    TopicArtifact,
)
from streamt.deployer.gateway import (
    GatewayBackendBinding,
    GatewayRuleChange,
    ManagedGatewayRuleObservation,
    build_desired_gateway_rule,
    plan_managed_gateway_rule,
    plan_managed_gateway_rule_deletion,
)
from streamt.deployer.kafka import TopicChange
from streamt.deployer.planner import DeploymentPlan, DeploymentPlanner
from streamt.deployer.state import (
    LocalState,
    ManagedResourceRecord,
    StateIdentityError,
    artifact_checksum,
    resource_id,
)

_BINDING = GatewayBackendBinding.from_endpoint(
    "https://gateway.example.test",
    virtual_cluster="production",
)


def _artifact(
    *,
    name: str = "orders_rule",
    alias: str = "orders.public",
    physical: str = "orders.v1",
) -> GatewayRuleArtifact:
    return GatewayRuleArtifact(
        name=name,
        virtual_topic=alias,
        physical_topic=physical,
        ownership=ArtifactOwnership(
            project="payments",
            owner_type="model",
            owner_name=name,
        ),
    )


def _absent(desired: ManagedGatewayRuleObservation) -> ManagedGatewayRuleObservation:
    return ManagedGatewayRuleObservation(
        binding=desired.binding,
        logical_name=desired.logical_name,
        alias_name=desired.alias_name,
        exists=False,
    )


def _create_change(
    *,
    name: str = "orders_rule",
    alias: str = "orders.public",
) -> GatewayRuleChange:
    artifact = _artifact(name=name, alias=alias)
    desired = build_desired_gateway_rule(artifact, _BINDING)
    return plan_managed_gateway_rule(artifact, desired, _absent(desired))


def _update_change() -> GatewayRuleChange:
    artifact = _artifact()
    desired = build_desired_gateway_rule(artifact, _BINDING)
    current = ManagedGatewayRuleObservation(
        binding=_BINDING,
        logical_name=desired.logical_name,
        alias_name=desired.alias_name,
        exists=True,
        physical_name="orders.previous",
        physical_cluster="main",
    )
    return plan_managed_gateway_rule(artifact, desired, current)


def _delete_change() -> GatewayRuleChange:
    current = build_desired_gateway_rule(_artifact(), _BINDING)
    return plan_managed_gateway_rule_deletion(current)


def _planner(
    deployer: MagicMock,
    *,
    prior_state: LocalState | None = None,
) -> DeploymentPlanner:
    return DeploymentPlanner(
        Manifest(version="1.0", project_name="payments"),
        gateway_deployer=deployer,
        prior_state=prior_state,
        environment="prod",
    )


def test_apply_routes_create_and_update_through_exact_managed_surfaces() -> None:
    deployer = MagicMock()
    deployer.apply_managed_gateway_rule.side_effect = ["created", "updated"]
    create = _create_change()
    update = _update_change()

    results = _planner(deployer).apply(
        DeploymentPlan(gateway_changes=[create, update])
    )

    assert results["created"] == ["gateway_rule:orders_rule"]
    assert results["updated"] == ["gateway_rule:orders_rule"]
    assert deployer.apply_managed_gateway_rule.call_args_list == [
        ((create.current, create.desired_managed),),
        ((update.current, update.desired_managed),),
    ]
    deployer.apply.assert_not_called()
    deployer.delete.assert_not_called()


@pytest.mark.parametrize(
    ("action", "invalid_result"),
    [("create", "unchanged"), ("update", "created")],
)
def test_apply_requires_the_exact_managed_result_verb(
    action: str,
    invalid_result: str,
) -> None:
    deployer = MagicMock()
    deployer.apply_managed_gateway_rule.return_value = invalid_result
    change = _create_change() if action == "create" else _update_change()

    results = _planner(deployer).apply(
        DeploymentPlan(gateway_changes=[change])
    )

    assert results["created"] == []
    assert results["updated"] == []
    assert results["unchanged"] == []
    assert len(results["errors"]) == 1
    assert "invalid result" in results["errors"][0]
    deployer.apply.assert_not_called()


def test_apply_routes_delete_through_the_exact_current_aggregate() -> None:
    deployer = MagicMock()
    deployer.delete_managed_gateway_rule.return_value = "deleted"
    change = _delete_change()

    results = _planner(deployer).apply(DeploymentPlan(gateway_changes=[change]))

    assert results["deleted"] == ["gateway_rule:orders_rule"]
    deployer.delete_managed_gateway_rule.assert_called_once_with(change.current)
    deployer.apply.assert_not_called()
    deployer.delete.assert_not_called()


def test_delete_requires_the_exact_managed_success_result() -> None:
    deployer = MagicMock()
    deployer.delete_managed_gateway_rule.return_value = "unchanged"

    results = _planner(deployer).apply(
        DeploymentPlan(gateway_changes=[_delete_change()])
    )

    assert results["deleted"] == []
    assert len(results["errors"]) == 1
    assert "invalid result" in results["errors"][0]
    deployer.delete.assert_not_called()


def test_actionable_legacy_gateway_change_fails_before_any_provider_mutation() -> None:
    deployer = MagicMock()
    legacy = GatewayRuleChange(
        name="orders_rule",
        action="create",
        desired=_artifact(),
    )

    with pytest.raises(
        StateIdentityError,
        match="complete normalized aggregate evidence",
    ):
        _planner(deployer).apply(DeploymentPlan(gateway_changes=[legacy]))

    deployer.apply_managed_gateway_rule.assert_not_called()
    deployer.delete_managed_gateway_rule.assert_not_called()
    deployer.apply.assert_not_called()
    deployer.delete.assert_not_called()


@pytest.mark.parametrize("action", ["create", "delete"])
def test_post_plan_evidence_tampering_fails_before_provider_mutation(
    action: str,
) -> None:
    deployer = MagicMock()
    change = _create_change() if action == "create" else _delete_change()
    change.changes = {}

    with pytest.raises(StateIdentityError, match="canonical normalized evidence"):
        _planner(deployer).apply(DeploymentPlan(gateway_changes=[change]))

    deployer.apply_managed_gateway_rule.assert_not_called()
    deployer.delete_managed_gateway_rule.assert_not_called()


@pytest.mark.parametrize(
    ("legacy_field", "legacy_value"),
    [("current_alias", MagicMock()), ("current_interceptors", [])],
)
def test_post_plan_legacy_field_tampering_fails_before_provider_mutation(
    legacy_field: str,
    legacy_value: object,
) -> None:
    deployer = MagicMock()
    change = _create_change()
    setattr(change, legacy_field, legacy_value)

    with pytest.raises(StateIdentityError, match="canonical normalized evidence"):
        _planner(deployer).apply(DeploymentPlan(gateway_changes=[change]))

    deployer.apply_managed_gateway_rule.assert_not_called()
    deployer.delete_managed_gateway_rule.assert_not_called()


def test_actionable_gateway_without_deployer_fails_before_earlier_provider_mutation() -> None:
    kafka = MagicMock()
    plan = DeploymentPlan(
        topic_changes=[
            TopicChange(
                topic="orders",
                action="create",
                desired=TopicArtifact(
                    name="orders",
                    partitions=1,
                    replication_factor=1,
                ),
            )
        ],
        gateway_changes=[_create_change()],
    )
    planner = DeploymentPlanner(
        Manifest(version="1.0", project_name="payments"),
        kafka_deployer=kafka,
        environment="prod",
    )

    with pytest.raises(StateIdentityError, match="configured Gateway deployer"):
        planner.apply(plan)
    with pytest.raises(StateIdentityError, match="configured Gateway deployer"):
        planner.operation_actions(plan)
    with pytest.raises(StateIdentityError, match="configured Gateway deployer"):
        planner.planned_actions(plan)

    kafka.apply_topic.assert_not_called()


def test_planned_gateway_actions_use_exact_alias_locators() -> None:
    deployer = MagicMock()
    create = _create_change(alias="orders.exact")
    delete = _delete_change()
    prior = LocalState(
        project="payments",
        environment="prod",
        resources={
            resource_id("payments", "prod", "gateway_rule", "orders_rule"):
                ManagedResourceRecord(
                    physical_name="orders.public",
                    ownership="managed",
                    artifact_checksum=artifact_checksum({"alias": "orders.public"}),
                    backend=_BINDING.backend_identity,
                )
        },
    )

    planner = _planner(deployer, prior_state=prior)
    create_action = planner.planned_actions(
        DeploymentPlan(gateway_changes=[create])
    )[0]
    delete_action = planner.planned_actions(
        DeploymentPlan(gateway_changes=[delete])
    )[0]

    assert create_action.runtime_label == "gateway_rule:orders_rule"
    assert create_action.resource_id.endswith("/gateway_rule/orders_rule")
    assert delete_action.resource_id.endswith("/gateway_rule/orders_rule")
    assert planner.operation_actions(DeploymentPlan(gateway_changes=[delete])) == [
        ("gateway_rule:orders_rule", "delete")
    ]


@pytest.mark.parametrize(
    ("action", "expected_exists", "expected_counts"),
    [
        ("create", (False, True), (0, 1)),
        ("update", (True, True), (0, 1)),
        ("delete", (True, False), (1, 0)),
    ],
)
def test_planned_gateway_actions_freeze_exact_secret_neutral_transition_evidence(
    action: str,
    expected_exists: tuple[bool, bool],
    expected_counts: tuple[int, int],
) -> None:
    deployer = MagicMock()
    artifact = GatewayRuleArtifact(
        name="provider_rule",
        virtual_topic="orders.public",
        physical_topic="orders.v1",
        interceptors=[
            {
                "type": "filter",
                "config": {"where": "customer_token = 'raw-secret-value'"},
            }
        ],
        ownership=ArtifactOwnership(
            project="payments",
            owner_type="model",
            owner_name="state_owner",
        ),
    )
    desired = build_desired_gateway_rule(artifact, _BINDING)
    if action == "create":
        change = plan_managed_gateway_rule(artifact, desired, _absent(desired))
    elif action == "update":
        change = plan_managed_gateway_rule(
            artifact,
            desired,
            ManagedGatewayRuleObservation(
                binding=_BINDING,
                logical_name=artifact.name,
                alias_name=artifact.virtual_topic,
                exists=True,
                physical_name="orders.previous",
                physical_cluster="main",
            ),
        )
    else:
        change = plan_managed_gateway_rule_deletion(desired)

    prior = LocalState(
        project="payments",
        environment="prod",
        resources={
            resource_id("payments", "prod", "gateway_rule", "state_owner"):
                ManagedResourceRecord(
                    physical_name=artifact.virtual_topic,
                    ownership="managed",
                    artifact_checksum=artifact_checksum(
                        {"alias": artifact.virtual_topic}
                    ),
                    backend=_BINDING.backend_identity,
                )
        },
    )
    planned = _planner(deployer, prior_state=prior).planned_actions(
        DeploymentPlan(gateway_changes=[change])
    )[0]

    assert planned.resource_id.endswith("/gateway_rule/state_owner")
    assert planned.gateway_evidence is not None
    evidence = planned.gateway_evidence
    assert evidence.rule_name == "provider_rule"
    assert evidence.rule_name != "state_owner"
    assert evidence.alias_name == "orders.public"
    assert evidence.backend_identity == _BINDING.backend_identity
    assert (evidence.current.exists, evidence.desired.exists) == expected_exists
    assert (
        evidence.current.managed_interceptor_count,
        evidence.desired.managed_interceptor_count,
    ) == expected_counts
    assert evidence.current.fingerprint != evidence.desired.fingerprint

    wire = json.dumps(evidence.to_dict(), sort_keys=True)
    assert "raw-secret-value" not in wire
    assert "gateway.example.test" not in wire
    assert "customer_token" not in wire
    assert "orders.v1" not in wire


def test_delete_ownership_lookup_is_qualified_by_canonical_backend() -> None:
    deployer = MagicMock()
    wrong_binding = GatewayBackendBinding.from_endpoint(
        "https://other-gateway.example.test",
        virtual_cluster="production",
    )
    prior = LocalState(
        project="payments",
        environment="prod",
        resources={
            resource_id("payments", "prod", "gateway_rule", "orders_rule"):
                ManagedResourceRecord(
                    physical_name="orders.public",
                    ownership="managed",
                    artifact_checksum=artifact_checksum({"alias": "orders.public"}),
                    backend=wrong_binding.backend_identity,
                )
        },
    )

    with pytest.raises(StateIdentityError, match="no canonical ownership identity"):
        _planner(deployer, prior_state=prior).planned_actions(
            DeploymentPlan(gateway_changes=[_delete_change()])
        )


def test_later_gateway_failure_rolls_back_created_exact_desired_aggregate() -> None:
    deployer = MagicMock()
    first = _create_change(name="orders_rule", alias="orders.public")
    second = _create_change(name="archive_rule", alias="archive.public")
    deployer.apply_managed_gateway_rule.side_effect = ["created", RuntimeError("failed")]
    deployer.delete_managed_gateway_rule.return_value = "deleted"
    planner = _planner(deployer)
    plan = DeploymentPlan(gateway_changes=[first, second])
    callbacks: list[tuple[str, str, int, bool | None]] = []

    results = planner.apply(
        plan,
        before_action=lambda label, index: callbacks.append(
            ("before", label, index, None)
        ),
        after_action=lambda label, index, succeeded: callbacks.append(
            ("after", label, index, succeeded)
        ),
        stop_on_error=True,
    )
    rolled_back, errors = planner.rollback(
        results["rollback_candidates"],  # type: ignore[arg-type]
        plan=plan,
    )

    assert results["rollback_candidates"] == ["gateway_rule:orders_rule"]
    assert rolled_back == ["gateway_rule:orders_rule"]
    assert errors == []
    deployer.delete_managed_gateway_rule.assert_called_once_with(first.desired_managed)
    deployer.delete.assert_not_called()
    assert callbacks == [
        ("before", "gateway_rule:orders_rule", 0, None),
        ("after", "gateway_rule:orders_rule", 0, True),
        ("before", "gateway_rule:archive_rule", 1, None),
        ("after", "gateway_rule:archive_rule", 1, False),
    ]


@pytest.mark.parametrize("plan", [None, DeploymentPlan()])
def test_gateway_rollback_missing_exact_plan_evidence_fails_without_mutation(
    plan: DeploymentPlan | None,
) -> None:
    deployer = MagicMock()
    planner = _planner(deployer)

    rolled_back, errors = planner.rollback(
        ["gateway_rule:orders_rule"],
        plan=plan,
    )

    assert rolled_back == []
    assert len(errors) == 1
    assert "exact" in errors[0]
    deployer.delete_managed_gateway_rule.assert_not_called()
    deployer.delete.assert_not_called()


def test_gateway_rollback_ambiguous_plan_evidence_fails_without_mutation() -> None:
    deployer = MagicMock()
    first = _create_change(alias="orders.first")
    second = _create_change(alias="orders.second")
    planner = _planner(deployer)

    rolled_back, errors = planner.rollback(
        ["gateway_rule:orders_rule"],
        plan=DeploymentPlan(gateway_changes=[first, second]),
    )

    assert rolled_back == []
    assert len(errors) == 1
    assert "one exact normalized create change" in errors[0]
    deployer.delete_managed_gateway_rule.assert_not_called()
    deployer.delete.assert_not_called()


def test_gateway_rollback_requires_exact_delete_result() -> None:
    deployer = MagicMock()
    deployer.delete_managed_gateway_rule.return_value = "unchanged"
    change = _create_change()

    rolled_back, errors = _planner(deployer).rollback(
        ["gateway_rule:orders_rule"],
        plan=DeploymentPlan(gateway_changes=[change]),
    )

    assert rolled_back == []
    assert len(errors) == 1
    assert "invalid result" in errors[0]
    deployer.delete.assert_not_called()


def test_duplicate_gateway_rollback_labels_fail_preflight_without_mutation() -> None:
    deployer = MagicMock()
    change = _create_change()

    rolled_back, errors = _planner(deployer).rollback(
        ["gateway_rule:orders_rule", "gateway_rule:orders_rule"],
        plan=DeploymentPlan(gateway_changes=[change]),
    )

    assert rolled_back == []
    assert len(errors) == 1
    assert "unique exact creates" in errors[0]
    deployer.delete_managed_gateway_rule.assert_not_called()
    deployer.delete.assert_not_called()
