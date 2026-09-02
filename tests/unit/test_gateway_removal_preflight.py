"""Pure target and ownership-state preflight for Gateway removals."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import FrozenInstanceError
from unittest.mock import MagicMock

import pytest

from streamt.compiler.manifest import ArtifactOwnership, Manifest
from streamt.core.models import ProjectInfo, StreamtProject
from streamt.core.runtime import (
    ConduktorConfig,
    GatewayConfig,
    KafkaConfig,
    RuntimeConfig,
)
from streamt.deployer.gateway import GatewayBackendBinding, GatewayDeployer
from streamt.deployer.planner import (
    DeploymentPlanner,
    GatewayPlanningTargets,
    ResolvedGatewayRuleRemoval,
    resolve_gateway_planning_targets,
)
from streamt.deployer.state import (
    LocalState,
    ManagedResourceRecord,
    StateIdentityError,
    artifact_checksum,
    resource_id,
)

_ENDPOINT = "https://gateway.example.test/admin"
_VCLUSTER = "payments-prod"


def _binding(endpoint: str = _ENDPOINT) -> GatewayBackendBinding:
    return GatewayBackendBinding.from_endpoint(
        endpoint,
        virtual_cluster=_VCLUSTER,
    )


def _project(*, name: str = "payments") -> StreamtProject:
    return StreamtProject(
        project=ProjectInfo(name=name),
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
    where: str | None = None,
) -> dict[str, object]:
    interceptors: list[dict[str, object]] = []
    if where is not None:
        interceptors.append(
            {"type": "filter", "config": {"where": where}}
        )
    return {
        "name": rule,
        "virtualTopic": alias,
        "physicalTopic": "orders.raw",
        "interceptors": interceptors,
        "ownership": _ownership(owner),
    }


def _removal(
    *,
    owner: str,
    rule: str,
    alias: str,
    where: str | None = None,
) -> dict[str, object]:
    return {
        "logicalOwner": owner,
        "priorArtifact": _rule(
            owner=owner,
            rule=rule,
            alias=alias,
            where=where,
        ),
    }


def _manifest(
    *,
    desired: list[object] | None = None,
    removals: object = (),
) -> Manifest:
    artifacts: dict[str, list[dict[str, object]]] = {
        "gateway_rules": list(desired or []),  # type: ignore[list-item]
    }
    if removals != ():
        artifacts["gateway_rule_removals"] = removals  # type: ignore[assignment]
    return Manifest(
        version="1.0.0",
        project_name="payments",
        artifacts=artifacts,
    )


def _state(
    *removals: dict[str, object],
    overrides: dict[str, ManagedResourceRecord] | None = None,
) -> LocalState:
    resources: dict[str, ManagedResourceRecord] = {}
    for removal in removals:
        owner = removal["logicalOwner"]
        prior = removal["priorArtifact"]
        assert isinstance(owner, str)
        assert isinstance(prior, dict)
        alias = prior["virtualTopic"]
        assert isinstance(alias, str)
        resources[
            resource_id("payments", "prod", "gateway_rule", owner)
        ] = ManagedResourceRecord(
            physical_name=alias,
            ownership="managed",
            artifact_checksum=artifact_checksum(prior),
            backend=_binding().backend_identity,
        )
    resources.update(overrides or {})
    return LocalState(
        project="payments",
        environment="prod",
        resources=resources,
    )


def _assert_preflight_fails_without_gateway_read(
    manifest: Manifest,
    state: LocalState | None,
    message: str,
    *,
    project: StreamtProject | None = None,
    environment: str = "prod",
) -> None:
    gateway = MagicMock(spec=GatewayDeployer)
    gateway.cluster_binding = _binding()
    schema_registry = MagicMock()
    manifest.artifacts["schemas"] = [
        {"subject": "later-value", "schema": '{"type":"string"}'}
    ]

    with pytest.raises(StateIdentityError, match=message):
        DeploymentPlanner(
            manifest,
            schema_registry_deployer=schema_registry,
            project=project or _project(),
            prior_state=state,
            environment=environment,
            gateway_deployer=gateway,
        ).plan()

    schema_registry.plan_schema.assert_not_called()
    gateway.observe_managed_gateway_snapshot.assert_not_called()


def test_resolves_owner_rule_alias_checksum_and_declaration_order() -> None:
    first = _removal(
        owner="orders_view",
        rule="orders_access_rule",
        alias="orders.public",
        where="region = 'us'",
    )
    second = _removal(
        owner="customers_view",
        rule="customers_access_rule",
        alias="customers.public",
    )

    targets = resolve_gateway_planning_targets(
        _manifest(removals=[first, second]),
        _project(),
        environment="prod",
        prior_state=_state(first, second),
        require_authoritative_state=True,
    )

    assert isinstance(targets, GatewayPlanningTargets)
    assert targets.binding == _binding()
    assert targets.desired_rules == ()
    assert [removal.logical_owner for removal in targets.removals] == [
        "orders_view",
        "customers_view",
    ]
    resolved = targets.removals[0]
    assert isinstance(resolved, ResolvedGatewayRuleRemoval)
    assert resolved.resource_id == resource_id(
        "payments", "prod", "gateway_rule", "orders_view"
    )
    assert resolved.logical_owner == "orders_view"
    assert resolved.rule_name == "orders_access_rule"
    assert resolved.alias_name == "orders.public"
    assert resolved.prior_artifact_checksum == artifact_checksum(
        first["priorArtifact"]
    )
    assert len({resolved.logical_owner, resolved.rule_name, resolved.alias_name}) == 3
    with pytest.raises(FrozenInstanceError):
        resolved.alias_name = "changed"  # type: ignore[misc]
    detached = resolved.prior_artifact
    detached.name = "changed"
    detached.interceptors[0]["config"] = {"where": "changed = true"}
    assert resolved.prior_artifact.name == "orders_access_rule"
    assert resolved.prior_artifact.interceptors[0]["config"] == {
        "where": "region = 'us'"
    }


def test_resolved_removal_rejects_ownership_from_another_project() -> None:
    removal = _removal(
        owner="orders_view",
        rule="orders_rule",
        alias="orders.public",
    )
    targets = resolve_gateway_planning_targets(
        _manifest(removals=[removal]),
        _project(),
        environment="prod",
        prior_state=_state(removal),
        require_authoritative_state=True,
    )
    resolved = targets.removals[0]
    detached = resolved.prior_artifact
    detached.ownership = ArtifactOwnership(
        project="other",
        owner_type="model",
        owner_name="orders_view",
        mode="managed",
    )

    with pytest.raises(StateIdentityError, match="mismatched compiled identity"):
        ResolvedGatewayRuleRemoval(
            resource_id=resolved.resource_id,
            logical_owner=resolved.logical_owner,
            prior_artifact=detached,
            prior_artifact_checksum=artifact_checksum(detached.to_dict()),
            binding=resolved.binding,
            rule_name=resolved.rule_name,
            alias_name=resolved.alias_name,
        )


def test_online_removal_requires_authoritative_state_without_gateway_read() -> None:
    removal = _removal(
        owner="orders_view",
        rule="orders_rule",
        alias="orders.public",
    )

    _assert_preflight_fails_without_gateway_read(
        _manifest(removals=[removal]),
        None,
        "authoritative ownership state",
    )


def test_no_prior_record_is_a_valid_preflight_target() -> None:
    removal = _removal(
        owner="orders_view",
        rule="orders_rule",
        alias="orders.public",
    )

    targets = resolve_gateway_planning_targets(
        _manifest(removals=[removal]),
        _project(),
        environment="prod",
        prior_state=_state(),
        require_authoritative_state=True,
    )

    assert targets.removals[0].logical_owner == "orders_view"


def test_only_removal_live_plan_stays_preflight_only() -> None:
    removal = _removal(
        owner="orders_view",
        rule="orders_rule",
        alias="orders.public",
    )
    gateway = MagicMock(spec=GatewayDeployer)
    gateway.cluster_binding = _binding()

    plan = DeploymentPlanner(
        _manifest(removals=[removal]),
        project=_project(),
        prior_state=_state(removal),
        environment="prod",
        gateway_deployer=gateway,
    ).plan()

    assert plan.gateway_changes == []
    gateway.observe_managed_gateway_snapshot.assert_not_called()


def test_offline_removal_preflight_does_not_require_state_or_read_gateway() -> None:
    removal = _removal(
        owner="orders_view",
        rule="orders_rule",
        alias="orders.public",
    )
    gateway = MagicMock(spec=GatewayDeployer)
    gateway.cluster_binding = _binding()

    plan = DeploymentPlanner(
        _manifest(removals=[removal]),
        project=_project(),
        environment="prod",
        gateway_deployer=gateway,
    ).offline_plan()

    assert plan.gateway_changes == []
    gateway.observe_managed_gateway_snapshot.assert_not_called()


@pytest.mark.parametrize(
    "raw_collection",
    [
        None,
        {},
        "removal",
        ({"invalid": True},),
        type("RemovalList", (list,), {})(),
    ],
)
def test_malformed_removal_collection_fails_without_gateway_read(
    raw_collection: object,
) -> None:
    _assert_preflight_fails_without_gateway_read(
        _manifest(removals=raw_collection),
        _state(),
        "manifest collection",
    )


@pytest.mark.parametrize("mismatch", ["project", "environment"])
def test_state_address_mismatch_is_rejected_by_public_preflight(
    mismatch: str,
) -> None:
    removal = _removal(
        owner="orders_view",
        rule="orders_rule",
        alias="orders.public",
    )
    state = LocalState(
        project="other" if mismatch == "project" else "payments",
        environment="staging" if mismatch == "environment" else "prod",
    )

    with pytest.raises(StateIdentityError, match="another project environment"):
        resolve_gateway_planning_targets(
            _manifest(removals=[removal]),
            _project(),
            environment="prod",
            prior_state=state,
            require_authoritative_state=True,
        )

    _assert_preflight_fails_without_gateway_read(
        _manifest(removals=[removal]),
        state,
        "prior state belongs",
    )


def test_project_runtime_mismatch_fails_without_provider_planning() -> None:
    removal = _removal(
        owner="orders_view",
        rule="orders_rule",
        alias="orders.public",
    )

    _assert_preflight_fails_without_gateway_read(
        _manifest(removals=[removal]),
        _state(removal),
        "runtime does not match",
        project=_project(name="other"),
    )


def test_invalid_environment_fails_without_provider_planning() -> None:
    removal = _removal(
        owner="orders_view",
        rule="orders_rule",
        alias="orders.public",
    )

    _assert_preflight_fails_without_gateway_read(
        _manifest(removals=[removal]),
        None,
        "invalid project environment",
        environment="not/valid",
    )


@pytest.mark.parametrize(
    ("record_factory", "message"),
    [
        (
            lambda prior: ManagedResourceRecord(
                physical_name="orders.public",
                ownership="adopted",
                artifact_checksum=artifact_checksum(prior),
                backend=_binding().backend_identity,
            ),
            "managed prior ownership",
        ),
        (
            lambda prior: ManagedResourceRecord(
                physical_name="orders.public",
                ownership="managed",
                artifact_checksum=artifact_checksum(prior),
                backend="conduktor-gateway",
            ),
            "legacy unbound",
        ),
        (
            lambda prior: ManagedResourceRecord(
                physical_name="orders.public",
                ownership="managed",
                artifact_checksum=artifact_checksum(prior),
                backend=_binding("https://other.example.test").backend_identity,
            ),
            "different provider binding",
        ),
        (
            lambda prior: ManagedResourceRecord(
                physical_name="orders.old",
                ownership="managed",
                artifact_checksum=artifact_checksum(prior),
                backend=_binding().backend_identity,
            ),
            "different alias",
        ),
        (
            lambda _prior: ManagedResourceRecord(
                physical_name="orders.public",
                ownership="managed",
                artifact_checksum=artifact_checksum({"different": True}),
                backend=_binding().backend_identity,
            ),
            "different artifact checksum",
        ),
    ],
    ids=["adopted", "legacy", "backend", "alias", "checksum"],
)
def test_prior_state_must_match_exact_removal_without_gateway_read(
    record_factory: Callable[[dict[str, object]], ManagedResourceRecord],
    message: str,
) -> None:
    removal = _removal(
        owner="orders_view",
        rule="orders_rule",
        alias="orders.public",
    )
    prior = removal["priorArtifact"]
    assert isinstance(prior, dict)
    target = resource_id(
        "payments", "prod", "gateway_rule", "orders_view"
    )

    _assert_preflight_fails_without_gateway_read(
        _manifest(removals=[removal]),
        _state(overrides={target: record_factory(prior)}),
        message,
    )


@pytest.mark.parametrize(
    "claim_backend",
    [_binding().backend_identity, "conduktor-gateway"],
    ids=["canonical", "legacy"],
)
def test_another_prior_alias_claim_is_rejected_without_gateway_read(
    claim_backend: str,
) -> None:
    removal = _removal(
        owner="orders_view",
        rule="orders_rule",
        alias="orders.public",
    )
    other = resource_id(
        "payments", "prod", "gateway_rule", "other_owner"
    )

    _assert_preflight_fails_without_gateway_read(
        _manifest(removals=[removal]),
        _state(
            overrides={
                other: ManagedResourceRecord(
                    physical_name="orders.public",
                    ownership="managed",
                    artifact_checksum=artifact_checksum({"other": True}),
                    backend=claim_backend,
                )
            }
        ),
        "claimed by another|duplicate canonical provider claims",
    )


def test_exact_canonical_target_and_legacy_duplicate_alias_claim_are_rejected() -> None:
    removal = _removal(
        owner="orders_view",
        rule="orders_rule",
        alias="orders.public",
    )
    other = resource_id(
        "payments", "prod", "gateway_rule", "legacy_owner"
    )

    _assert_preflight_fails_without_gateway_read(
        _manifest(removals=[removal]),
        _state(
            removal,
            overrides={
                other: ManagedResourceRecord(
                    physical_name="orders.public",
                    ownership="managed",
                    artifact_checksum=artifact_checksum({"legacy": True}),
                    backend="conduktor-gateway",
                )
            },
        ),
        "claimed by another",
    )


@pytest.mark.parametrize("collision", ["owner", "rule", "alias", "interceptor"])
def test_desired_and_removal_collisions_fail_without_gateway_read(
    collision: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    desired = _rule(
        owner="desired_owner",
        rule="desired_rule",
        alias="orders.desired",
        where="region = 'us'",
    )
    removal = _removal(
        owner="removed_owner",
        rule="removed_rule",
        alias="orders.removed",
        where="region = 'us'",
    )
    if collision == "owner":
        removal["logicalOwner"] = "desired_owner"
        prior = removal["priorArtifact"]
        assert isinstance(prior, dict)
        prior["ownership"] = _ownership("desired_owner")
    elif collision == "rule":
        prior = removal["priorArtifact"]
        assert isinstance(prior, dict)
        prior["name"] = "desired_rule"
    elif collision == "alias":
        prior = removal["priorArtifact"]
        assert isinstance(prior, dict)
        prior["virtualTopic"] = "orders.desired"
    else:
        monkeypatch.setattr(
            "streamt.deployer.planner.generate_gateway_interceptor_name",
            lambda _rule, _kind, _ordinal: "desired_rule_filter_0",
        )

    _assert_preflight_fails_without_gateway_read(
        _manifest(desired=[desired], removals=[removal]),
        _state(),
        "Gateway planning targets collide",
    )


@pytest.mark.parametrize("collision", ["owner", "rule", "alias", "interceptor"])
def test_removal_to_removal_collisions_fail_without_gateway_read(
    collision: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    first = _removal(
        owner="first_owner",
        rule="first_rule",
        alias="orders.first",
        where="region = 'us'",
    )
    second = _removal(
        owner="second_owner",
        rule="second_rule",
        alias="orders.second",
        where="region = 'eu'",
    )
    if collision == "owner":
        second["logicalOwner"] = "first_owner"
        prior = second["priorArtifact"]
        assert isinstance(prior, dict)
        prior["ownership"] = _ownership("first_owner")
    elif collision == "rule":
        prior = second["priorArtifact"]
        assert isinstance(prior, dict)
        prior["name"] = "first_rule"
    elif collision == "alias":
        prior = second["priorArtifact"]
        assert isinstance(prior, dict)
        prior["virtualTopic"] = "orders.first"
    else:
        monkeypatch.setattr(
            "streamt.deployer.planner.generate_gateway_interceptor_name",
            lambda _rule, _kind, _ordinal: "shared_filter_0",
        )

    _assert_preflight_fails_without_gateway_read(
        _manifest(removals=[first, second]),
        _state(),
        "Gateway planning targets collide",
    )


@pytest.mark.parametrize(
    "mutate",
    [
        lambda removal: removal.update({"future": True}),
        lambda removal: removal.pop("logicalOwner"),
        lambda removal: removal["priorArtifact"].pop("ownership"),
        lambda removal: removal["priorArtifact"].update(
            {"ownership": _ownership("other_owner")}
        ),
    ],
    ids=["unknown-field", "missing-field", "missing-ownership", "wrong-owner"],
)
def test_malformed_removal_manifest_fails_without_gateway_read(
    mutate: Callable[[dict[str, object]], object],
) -> None:
    removal = _removal(
        owner="orders_view",
        rule="orders_rule",
        alias="orders.public",
    )
    mutate(removal)

    _assert_preflight_fails_without_gateway_read(
        _manifest(removals=[removal]),
        _state(),
        "Gateway removal manifest",
    )
