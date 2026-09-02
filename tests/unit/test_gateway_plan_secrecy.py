"""Pure normalized Gateway planning and secret-neutral evidence."""

from __future__ import annotations

import json
from copy import deepcopy

import pytest

from streamt.compiler.manifest import GatewayRuleArtifact
from streamt.deployer.gateway import (
    AliasTopicState,
    GatewayBackendBinding,
    GatewayChangeEvidenceError,
    GatewayDesiredAggregateError,
    GatewayManagedObservationError,
    GatewayRuleChange,
    InterceptorState,
    ManagedGatewayInterceptor,
    ManagedGatewayRuleObservation,
    build_desired_gateway_rule,
    plan_managed_gateway_rule,
    secret_neutral_gateway_changes,
)

_ENDPOINT = "https://gateway.example.test:8443"
_SECRET = "desired-filter-secret-917c"
_OBSERVED_SECRET = "observed-filter-secret-42af"
_FILTER_PLUGIN = "io.conduktor.gateway.interceptor.VirtualSqlTopicPlugin"


def _binding(
    *,
    endpoint: str = _ENDPOINT,
    virtual_cluster: str | None = "production",
) -> GatewayBackendBinding:
    return GatewayBackendBinding.from_endpoint(
        endpoint,
        virtual_cluster=virtual_cluster,
    )


def _artifact(
    *,
    name: str = "orders_rule",
    alias: str = "orders.public",
    physical: str = "orders.v1",
    where: str | None = f"customer_token = '{_SECRET}'",
) -> GatewayRuleArtifact:
    interceptors = [] if where is None else [{"type": "filter", "config": {"where": where}}]
    return GatewayRuleArtifact(
        name=name,
        virtual_topic=alias,
        physical_topic=physical,
        interceptors=interceptors,
    )


def _desired(
    artifact: GatewayRuleArtifact | None = None,
    binding: GatewayBackendBinding | None = None,
) -> ManagedGatewayRuleObservation:
    return build_desired_gateway_rule(artifact or _artifact(), binding or _binding())


def _absent(desired: ManagedGatewayRuleObservation) -> ManagedGatewayRuleObservation:
    return ManagedGatewayRuleObservation(
        binding=desired.binding,
        logical_name=desired.logical_name,
        alias_name=desired.alias_name,
        exists=False,
    )


def _interceptor(
    desired: ManagedGatewayRuleObservation,
    *,
    name: str | None = None,
    plugin_class: str | None = None,
    priority: int | None = None,
    config_json: str | None = None,
) -> ManagedGatewayInterceptor:
    original = desired.interceptors[0]
    return ManagedGatewayInterceptor(
        name=name or original.name,
        scope=original.scope,
        plugin_class=plugin_class or original.plugin_class,
        priority=original.priority if priority is None else priority,
        config_json=config_json or original.config_json,
    )


def _current(
    desired: ManagedGatewayRuleObservation,
    *,
    logical_name: str | None = None,
    alias_name: str | None = None,
    physical_name: str | None = None,
    interceptor: ManagedGatewayInterceptor | None = None,
    binding: GatewayBackendBinding | None = None,
) -> ManagedGatewayRuleObservation:
    return ManagedGatewayRuleObservation(
        binding=binding or desired.binding,
        logical_name=logical_name or desired.logical_name,
        alias_name=alias_name or desired.alias_name,
        exists=True,
        physical_name=physical_name or desired.physical_name,
        physical_cluster=desired.physical_cluster,
        interceptors=(interceptor,) if interceptor is not None else desired.interceptors,
    )


def _plan_update(
    *,
    current: ManagedGatewayRuleObservation,
    artifact: GatewayRuleArtifact | None = None,
    desired: ManagedGatewayRuleObservation | None = None,
) -> GatewayRuleChange:
    selected_artifact = artifact or _artifact()
    selected_desired = desired or _desired(selected_artifact, current.binding)
    return plan_managed_gateway_rule(selected_artifact, selected_desired, current)


def test_absent_current_plans_create_with_complete_secret_neutral_evidence() -> None:
    artifact = _artifact()
    desired = _desired(artifact)

    change = plan_managed_gateway_rule(artifact, desired, _absent(desired))

    assert change.action == "create"
    assert change.current is not None
    assert change.current.exists is False
    assert change.desired_managed == desired
    assert change.backend_identity == desired.binding.backend_identity
    assert change.changes == {
        "categories": ["presence"],
        "current": {
            "exists": False,
            "fingerprint": change.current.fingerprint,
            "managed_interceptor_count": 0,
        },
        "desired": {
            "exists": True,
            "fingerprint": desired.fingerprint,
            "managed_interceptor_count": 1,
        },
    }


def test_exact_current_plans_noop_without_change_evidence() -> None:
    artifact = _artifact()
    desired = _desired(artifact)

    change = plan_managed_gateway_rule(artifact, desired, desired)

    assert change.action == "none"
    assert change.changes == {}
    assert change.current == desired
    assert change.desired is not artifact


@pytest.mark.parametrize(
    ("expected_category", "make_current"),
    [
        (
            "alias_mapping",
            lambda desired: _current(desired, physical_name="orders.previous"),
        ),
        (
            "interceptor_identities",
            lambda desired: _current(
                desired,
                interceptor=_interceptor(desired, name="orders_rule_filter_1"),
            ),
        ),
        (
            "plugin_classes",
            lambda desired: _current(
                desired,
                interceptor=_interceptor(desired, plugin_class="example.OtherPlugin"),
            ),
        ),
        (
            "priorities",
            lambda desired: _current(
                desired,
                interceptor=_interceptor(desired, priority=99),
            ),
        ),
        (
            "configuration",
            lambda desired: _current(
                desired,
                interceptor=_interceptor(
                    desired,
                    config_json=json.dumps(
                        {
                            "statement": (
                                'SELECT * FROM "orders.v1" WHERE '
                                f"customer_token = '{_OBSERVED_SECRET}'"
                            ),
                            "virtualTopic": "orders.public",
                        },
                        separators=(",", ":"),
                        sort_keys=True,
                    ),
                ),
            ),
        ),
    ],
    ids=[
        "alias-mapping",
        "interceptor-identities",
        "plugin-classes",
        "priorities",
        "configuration",
    ],
)
def test_each_supported_surface_drift_has_one_distinct_category(
    expected_category: str,
    make_current: object,
) -> None:
    artifact = _artifact()
    desired = _desired(artifact)
    assert callable(make_current)

    change = plan_managed_gateway_rule(artifact, desired, make_current(desired))

    assert change.action == "update"
    assert change.changes is not None
    assert change.changes["categories"] == [expected_category]


def test_non_main_physical_cluster_fails_at_strict_observation_boundary() -> None:
    desired = _desired()

    with pytest.raises(GatewayManagedObservationError):
        ManagedGatewayRuleObservation(
            binding=desired.binding,
            logical_name=desired.logical_name,
            alias_name=desired.alias_name,
            exists=True,
            physical_name=desired.physical_name,
            physical_cluster="secondary",
            interceptors=desired.interceptors,
        )


def test_combined_categories_are_deterministically_sorted() -> None:
    artifact = _artifact()
    desired = _desired(artifact)
    current = _current(
        desired,
        physical_name="orders.previous",
        interceptor=_interceptor(
            desired,
            name="orders_rule_filter_1",
            plugin_class="example.OtherPlugin",
            priority=99,
            config_json='{"statement":"different","virtualTopic":"orders.public"}',
        ),
    )

    change = plan_managed_gateway_rule(artifact, desired, current)

    assert change.changes is not None
    categories = change.changes["categories"]
    assert categories == sorted(categories)
    assert categories == [
        "alias_mapping",
        "interceptor_identities",
    ]


def test_config_only_drift_changes_fingerprint_without_exposing_sql() -> None:
    artifact = _artifact()
    desired = _desired(artifact)
    current = _current(
        desired,
        interceptor=_interceptor(
            desired,
            config_json=json.dumps(
                {
                    "statement": (
                        f"SELECT * FROM \"orders.v1\" WHERE customer_token = '{_OBSERVED_SECRET}'"
                    ),
                    "virtualTopic": "orders.public",
                },
                separators=(",", ":"),
                sort_keys=True,
            ),
        ),
    )

    change = plan_managed_gateway_rule(artifact, desired, current)

    assert change.changes is not None
    assert change.changes["categories"] == ["configuration"]
    assert change.changes["current"]["fingerprint"] != (change.changes["desired"]["fingerprint"])
    rendered = json.dumps(change.changes, sort_keys=True)
    assert _SECRET not in rendered
    assert _OBSERVED_SECRET not in rendered
    assert "SELECT *" not in rendered


def test_omitted_and_explicit_passthrough_bindings_plan_identically() -> None:
    artifact = _artifact(where=None)
    omitted_binding = _binding(virtual_cluster=None)
    explicit_binding = _binding(virtual_cluster="passthrough")
    omitted_desired = _desired(artifact, omitted_binding)
    explicit_desired = _desired(artifact, explicit_binding)

    omitted = plan_managed_gateway_rule(
        artifact,
        omitted_desired,
        _absent(omitted_desired),
    )
    explicit = plan_managed_gateway_rule(
        artifact,
        explicit_desired,
        _absent(explicit_desired),
    )

    assert omitted.backend_identity == explicit.backend_identity
    assert omitted.changes == explicit.changes


@pytest.mark.parametrize(
    "mismatch",
    ["binding", "logical", "alias", "artifact"],
)
def test_managed_planner_rejects_mismatched_complete_inputs(mismatch: str) -> None:
    artifact = _artifact()
    desired = _desired(artifact)
    current = _current(desired)
    selected_artifact = artifact
    if mismatch == "binding":
        current = _current(
            desired,
            binding=_binding(endpoint="https://other-gateway.example.test"),
        )
    elif mismatch == "logical":
        current = _current(desired, logical_name="other_rule")
    elif mismatch == "alias":
        current = _current(desired, alias_name="orders.other")
    else:
        selected_artifact = _artifact(physical="orders.v2")

    with pytest.raises(GatewayDesiredAggregateError):
        plan_managed_gateway_rule(selected_artifact, desired, current)


def test_normalized_direct_construction_rejects_wrong_backend() -> None:
    artifact = _artifact()
    desired = _desired(artifact)
    current = _absent(desired)

    with pytest.raises(GatewayDesiredAggregateError):
        GatewayRuleChange(
            name=artifact.name,
            action="create",
            desired=artifact,
            changes={
                "categories": ["presence"],
                "current": {
                    "exists": False,
                    "fingerprint": current.fingerprint,
                    "managed_interceptor_count": 0,
                },
                "desired": {
                    "exists": True,
                    "fingerprint": desired.fingerprint,
                    "managed_interceptor_count": 1,
                },
            },
            current=current,
            desired_managed=desired,
            backend_identity=_binding(
                endpoint="https://other-gateway.example.test"
            ).backend_identity,
        )


def test_plan_repr_and_evidence_exclude_artifact_and_observed_secrets() -> None:
    artifact = _artifact()
    desired = _desired(artifact)
    current = _current(
        desired,
        interceptor=_interceptor(
            desired,
            config_json=json.dumps(
                {
                    "statement": (
                        f"SELECT * FROM \"orders.v1\" WHERE customer_token = '{_OBSERVED_SECRET}'"
                    ),
                    "virtualTopic": "orders.public",
                },
                separators=(",", ":"),
                sort_keys=True,
            ),
        ),
    )

    change = plan_managed_gateway_rule(artifact, desired, current)
    rendered = repr(change) + json.dumps(change.changes, sort_keys=True)

    assert _SECRET not in rendered
    assert _OBSERVED_SECRET not in rendered
    assert "SELECT *" not in rendered
    assert "config_json" not in rendered


def _valid_evidence() -> dict[str, object]:
    artifact = _artifact()
    desired = _desired(artifact)
    change = plan_managed_gateway_rule(artifact, desired, _absent(desired))
    assert change.changes is not None
    return deepcopy(change.changes)


@pytest.mark.parametrize(
    "mutate",
    [
        lambda value: value.update({"raw": "SELECT secret"}),
        lambda value: value["current"].update({"exists": 0}),
        lambda value: value["current"].update({"managed_interceptor_count": True}),
        lambda value: value["desired"].update({"fingerprint": "sha256:nope"}),
        lambda value: value.update({"categories": ["raw_sql"]}),
        lambda value: value.update({"categories": ["presence", "presence"]}),
    ],
    ids=[
        "unknown-key",
        "bool",
        "bool-count",
        "checksum",
        "unknown-category",
        "duplicate-category",
    ],
)
def test_secret_neutral_boundary_rejects_malformed_or_raw_evidence(
    mutate: object,
) -> None:
    evidence = _valid_evidence()
    assert callable(mutate)
    mutate(evidence)

    with pytest.raises(GatewayChangeEvidenceError) as exc_info:
        secret_neutral_gateway_changes(evidence)

    assert "SELECT secret" not in str(exc_info.value)


def test_secret_neutral_boundary_returns_an_independent_exact_copy() -> None:
    evidence = _valid_evidence()

    normalized = secret_neutral_gateway_changes(evidence)
    evidence["categories"].append("configuration")
    evidence["current"]["fingerprint"] = "sha256:" + "0" * 64

    assert normalized["categories"] == ["presence"]
    assert normalized["current"]["fingerprint"] != "sha256:" + "0" * 64


def test_caller_mutation_cannot_change_carried_artifact_current_or_evidence() -> None:
    artifact = _artifact()
    desired = _desired(artifact)
    current = _current(desired, physical_name="orders.previous")
    change = plan_managed_gateway_rule(artifact, desired, current)
    original_evidence = deepcopy(change.changes)

    artifact.physical_topic = "orders.mutated"
    artifact.interceptors[0]["config"]["where"] = "customer_token = 'mutated'"
    object.__setattr__(current, "physical_name", "orders.mutated-current")

    assert change.desired is not None
    assert change.desired.physical_topic == "orders.v1"
    assert change.desired.interceptors[0]["config"]["where"] == (f"customer_token = '{_SECRET}'")
    assert change.current is not None
    assert change.current.physical_name == "orders.previous"
    assert change.changes == original_evidence


def test_legacy_direct_construction_remains_compatible_and_repr_safe() -> None:
    artifact = _artifact()
    legacy = GatewayRuleChange(
        name=artifact.name,
        action="update",
        current_alias=AliasTopicState(
            name=artifact.virtual_topic,
            exists=True,
            physical_topic="legacy-physical-secret",
        ),
        current_interceptors=[
            InterceptorState(
                name="orders_rule_filter_0",
                exists=True,
                config={"statement": f"SELECT '{_OBSERVED_SECRET}'"},
            )
        ],
        desired=artifact,
        changes={"legacy": {"from": _OBSERVED_SECRET, "to": _SECRET}},
    )

    assert legacy.current is None
    assert legacy.desired_managed is None
    assert legacy.backend_identity is None
    rendered = repr(legacy)
    assert _SECRET not in rendered
    assert _OBSERVED_SECRET not in rendered
    assert "SELECT" not in rendered
    assert "legacy-physical-secret" not in rendered
