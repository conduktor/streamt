"""Pure alias-only Gateway adoption target and review evidence tests."""

from __future__ import annotations

import json
from copy import deepcopy
from dataclasses import FrozenInstanceError
from typing import Any, Literal

import pytest

from streamt.compiler.manifest import Manifest
from streamt.core.models import StreamtProject
from streamt.deployer.gateway import (
    GatewayBackendBinding,
    ManagedGatewayInterceptor,
    ManagedGatewayRuleObservation,
)
from streamt.deployer.gateway_adoption import (
    GatewayAdoptionDriftError,
    GatewayAdoptionError,
    GatewayAdoptionLiveNotFoundError,
    GatewayAdoptionTarget,
    build_gateway_adoption_action_evidence,
    build_gateway_adoption_review,
    gateway_alias_mapping_checksum,
    require_unchanged_gateway_adoption_observation,
    resolve_gateway_adoption_target,
    validate_gateway_adoption_observation,
)
from streamt.deployer.state import (
    LocalState,
    ManagedResourceRecord,
    artifact_checksum,
    resource_id,
)
from streamt.deployer.state_backend import OperationAction

_PROJECT = "payments"
_ENVIRONMENT = "prod"
_OWNER = "orders_view"
_RULE = "orders_access_rule"
_ALIAS = "orders.public"
_PHYSICAL = "orders.raw"
_ENDPOINT = "https://gateway.example.test/private-admin-token"
_VCLUSTER = "payments-prod"


def _ownership(
    owner: str = _OWNER,
    *,
    project: str = _PROJECT,
    owner_type: str = "model",
    mode: str = "adopted",
) -> dict[str, str]:
    return {
        "project": project,
        "type": owner_type,
        "name": owner,
        "mode": mode,
    }


def _rule(
    *,
    owner: str = _OWNER,
    name: str = _RULE,
    alias: str = _ALIAS,
    physical: str = _PHYSICAL,
    interceptors: list[dict[str, object]] | None = None,
    ownership: dict[str, str] | None = None,
) -> dict[str, object]:
    return {
        "name": name,
        "virtualTopic": alias,
        "physicalTopic": physical,
        "interceptors": list(interceptors or []),
        "ownership": ownership or _ownership(owner),
    }


def _removal(
    *,
    owner: str = "removed_owner",
    name: str = "removed_rule",
    alias: str = "removed.public",
) -> dict[str, object]:
    return {
        "logicalOwner": owner,
        "priorArtifact": _rule(
            owner=owner,
            name=name,
            alias=alias,
            ownership=_ownership(owner, mode="managed"),
        ),
    }


def _manifest(
    *rules: object,
    removals: object = (),
) -> Manifest:
    artifacts: dict[str, Any] = {"gateway_rules": list(rules)}
    if removals != ():
        artifacts["gateway_rule_removals"] = removals
    return Manifest(
        version="1.0.0",
        project_name=_PROJECT,
        artifacts=artifacts,
    )


def _project(
    *,
    endpoint: str = _ENDPOINT,
    virtual_cluster: str | None = _VCLUSTER,
) -> StreamtProject:
    return StreamtProject.model_validate(
        {
            "apiVersion": "streamt.dev/v1alpha1",
            "project": {"name": _PROJECT},
            "runtime": {
                "kafka": {"bootstrap_servers": "broker.invalid:9092"},
                "conduktor": {
                    "gateway": {
                        "admin_url": endpoint,
                        "virtual_cluster": virtual_cluster,
                    }
                },
            },
        }
    )


def _binding(
    *,
    endpoint: str = _ENDPOINT,
    virtual_cluster: str | None = _VCLUSTER,
) -> GatewayBackendBinding:
    return GatewayBackendBinding.from_endpoint(
        endpoint,
        virtual_cluster=virtual_cluster,
    )


def _state(
    resources: dict[str, ManagedResourceRecord] | None = None,
    *,
    project: str = _PROJECT,
    environment: str = _ENVIRONMENT,
) -> LocalState:
    return LocalState(
        project=project,
        environment=environment,
        serial=3,
        resources=resources or {},
    )


def _resolve(
    manifest: Manifest | None = None,
    *,
    state: LocalState | None = None,
) -> GatewayAdoptionTarget:
    return resolve_gateway_adoption_target(
        manifest or _manifest(_rule()),
        _project(),
        environment=_ENVIRONMENT,
        logical_name=_OWNER,
        prior_state=state or _state(),
    )


def _observation(
    *,
    physical: str = _PHYSICAL,
    exists: bool = True,
    binding: GatewayBackendBinding | None = None,
    rule: str = _RULE,
    alias: str = _ALIAS,
    interceptors: tuple[ManagedGatewayInterceptor, ...] = (),
) -> ManagedGatewayRuleObservation:
    return ManagedGatewayRuleObservation(
        binding=binding or _binding(),
        logical_name=rule,
        alias_name=alias,
        exists=exists,
        physical_name=physical if exists else None,
        physical_cluster="main" if exists else None,
        interceptors=interceptors,
    )


def _owned_interceptor() -> ManagedGatewayInterceptor:
    return ManagedGatewayInterceptor(
        name=f"{_RULE}_filter_0",
        scope=(("group", None), ("username", None), ("vCluster", _VCLUSTER)),
        plugin_class="io.conduktor.gateway.interceptor.VirtualSqlTopicPlugin",
        priority=100,
        config_json=json.dumps(
            {
                "statement": f'SELECT * FROM "{_PHYSICAL}" WHERE amount > 0',
                "virtualTopic": _ALIAS,
            },
            separators=(",", ":"),
            sort_keys=True,
        ),
    )


def test_resolver_returns_one_exact_immutable_alias_only_target() -> None:
    raw_rule = _rule()
    target = _resolve(_manifest(raw_rule))

    assert target.resource_id == resource_id(
        _PROJECT,
        _ENVIRONMENT,
        "gateway_rule",
        _OWNER,
    )
    assert target.logical_owner == _OWNER
    assert target.rule_name == _RULE
    assert target.alias_name == _ALIAS
    assert target.binding == _binding()
    assert target.desired.exists is True
    assert target.desired.physical_name == _PHYSICAL
    assert target.desired.physical_cluster == "main"
    assert target.desired.interceptors == ()
    assert target.desired_artifact_checksum == artifact_checksum(raw_rule)
    assert target.desired_record == ManagedResourceRecord(
        physical_name=_ALIAS,
        ownership="adopted",
        artifact_checksum=artifact_checksum(raw_rule),
        backend=_binding().backend_identity,
    )
    assert target.existing_record is None

    raw_rule["physicalTopic"] = "mutated.raw"
    detached = target.artifact
    detached.physical_topic = "also-mutated.raw"
    assert target.artifact.physical_topic == _PHYSICAL
    with pytest.raises(FrozenInstanceError):
        target.rule_name = "mutated"  # type: ignore[misc]


def test_resolver_canonicalizes_omitted_vcluster_to_passthrough() -> None:
    target = resolve_gateway_adoption_target(
        _manifest(_rule()),
        _project(virtual_cluster=None),
        environment=_ENVIRONMENT,
        logical_name=_OWNER,
        prior_state=_state(),
    )

    assert target.binding.virtual_cluster == "passthrough"
    review = build_gateway_adoption_review(
        target,
        _observation(
            binding=_binding(virtual_cluster=None),
        ),
    )
    assert review.effective_vcluster == "passthrough"


@pytest.mark.parametrize("ownership", ["managed", "adopted"])
def test_resolver_accepts_only_the_exact_idempotent_state_claim(
    ownership: Literal["managed", "adopted"],
) -> None:
    rule = _rule()
    resource_uri = resource_id(
        _PROJECT,
        _ENVIRONMENT,
        "gateway_rule",
        _OWNER,
    )
    record = ManagedResourceRecord(
        physical_name=_ALIAS,
        ownership=ownership,
        artifact_checksum=artifact_checksum(rule),
        backend=_binding().backend_identity,
    )

    target = _resolve(_manifest(rule), state=_state({resource_uri: record}))

    assert target.existing_record == record
    assert target.desired_record == ManagedResourceRecord(
        physical_name=record.physical_name,
        ownership="adopted",
        artifact_checksum=record.artifact_checksum,
        backend=record.backend,
    )


@pytest.mark.parametrize(
    ("ownership", "message"),
    [
        (_ownership(mode="managed"), "adopted model ownership"),
        (_ownership(owner_type="source"), "adopted model ownership"),
        (_ownership(project="other"), "adopted model ownership"),
        (_ownership(owner="another_owner"), "exactly one compiled rule"),
    ],
    ids=["managed", "source", "other-project", "other-owner"],
)
def test_resolver_requires_exact_adopted_project_model_ownership(
    ownership: dict[str, str],
    message: str,
) -> None:
    with pytest.raises(GatewayAdoptionError, match=message):
        _resolve(_manifest(_rule(ownership=ownership)))


def test_resolver_requires_exact_authoritative_state() -> None:
    with pytest.raises(GatewayAdoptionError, match="exact authoritative state"):
        resolve_gateway_adoption_target(
            _manifest(_rule()),
            _project(),
            environment=_ENVIRONMENT,
            logical_name=_OWNER,
            prior_state=None,  # type: ignore[arg-type]
        )


@pytest.mark.parametrize(
    "manifest",
    [
        _manifest({**_rule(), "future": True}),
        _manifest(
            _rule(
                interceptors=[
                    {
                        "type": "filter",
                        "config": {"where": "amount > 0", "future": True},
                    }
                ]
            )
        ),
    ],
    ids=["unknown-rule-field", "unknown-interceptor-config"],
)
def test_resolver_rejects_malformed_artifacts_before_observation(
    manifest: Manifest,
) -> None:
    with pytest.raises(GatewayAdoptionError, match="preflight rejected"):
        _resolve(manifest)


def test_resolver_rejects_nonempty_desired_interceptors() -> None:
    rule = _rule(interceptors=[{"type": "filter", "config": {"where": "amount > 0"}}])

    with pytest.raises(GatewayAdoptionError, match="no desired interceptors"):
        _resolve(_manifest(rule))


@pytest.mark.parametrize("collision", ["owner", "rule", "alias"])
def test_resolver_rejects_global_desired_identity_collisions(
    collision: str,
) -> None:
    first = _rule()
    second = _rule(
        owner="audit_view",
        name="audit_rule",
        alias="audit.public",
        physical="audit.raw",
    )
    if collision == "owner":
        second["ownership"] = _ownership()
    elif collision == "rule":
        second["name"] = _RULE
    else:
        second["virtualTopic"] = _ALIAS

    with pytest.raises(GatewayAdoptionError, match="preflight rejected"):
        _resolve(_manifest(first, second))


def test_resolver_rejects_global_generated_namespace_collision(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    other = _rule(
        owner="audit_view",
        name="audit_rule",
        alias="audit.public",
        physical="audit.raw",
        interceptors=[{"type": "filter", "config": {"where": "severity > 1"}}],
    )
    monkeypatch.setattr(
        "streamt.deployer.gateway.classify_gateway_interceptor_name",
        lambda _logical_name, _candidate: object(),
    )

    with pytest.raises(GatewayAdoptionError, match="preflight rejected"):
        _resolve(_manifest(_rule(), other))


def test_resolver_rejects_desired_removal_collisions() -> None:
    with pytest.raises(GatewayAdoptionError, match="preflight rejected"):
        _resolve(
            _manifest(
                _rule(),
                removals=[_removal(owner=_OWNER, name=_RULE, alias=_ALIAS)],
            )
        )


@pytest.mark.parametrize(
    ("resource_owner", "backend"),
    [
        (_OWNER, "conduktor-gateway"),
        (_OWNER, _binding(endpoint="https://other.example.test").backend_identity),
        ("another_owner", "conduktor-gateway"),
        ("another_owner", _binding().backend_identity),
    ],
    ids=[
        "target-legacy",
        "target-other-binding",
        "other-legacy-alias",
        "other-canonical-alias",
    ],
)
def test_resolver_rejects_legacy_or_conflicting_alias_state_claims(
    resource_owner: str,
    backend: str,
) -> None:
    record = ManagedResourceRecord(
        physical_name=_ALIAS,
        ownership="adopted",
        artifact_checksum=artifact_checksum(_rule()),
        backend=backend,
    )
    resource_uri = resource_id(
        _PROJECT,
        _ENVIRONMENT,
        "gateway_rule",
        resource_owner,
    )

    with pytest.raises(GatewayAdoptionError):
        _resolve(state=_state({resource_uri: record}))


def test_resolver_scopes_alias_claims_to_the_canonical_backend() -> None:
    other_uri = resource_id(
        _PROJECT,
        _ENVIRONMENT,
        "gateway_rule",
        "other_owner",
    )
    other_record = ManagedResourceRecord(
        physical_name=_ALIAS,
        ownership="adopted",
        artifact_checksum=artifact_checksum(_rule()),
        backend=_binding(endpoint="https://other.example.test").backend_identity,
    )

    target = _resolve(state=_state({other_uri: other_record}))

    assert target.existing_record is None


def test_resolver_rejects_bad_gateway_runtime_without_exposing_endpoint() -> None:
    project = _project()
    assert project.runtime.conduktor is not None
    assert project.runtime.conduktor.gateway is not None
    object.__setattr__(
        project.runtime.conduktor.gateway,
        "admin_url",
        "https://operator:gateway-secret@example.test/private",
    )

    with pytest.raises(GatewayAdoptionError) as caught:
        resolve_gateway_adoption_target(
            _manifest(_rule()),
            project,
            environment=_ENVIRONMENT,
            logical_name=_OWNER,
            prior_state=_state(),
        )

    assert "gateway-secret" not in str(caught.value)
    assert "example.test" not in str(caught.value)


def test_review_evidence_is_exact_and_secret_neutral_with_pending_mapping() -> None:
    target = _resolve()
    current = _observation(physical="orders.legacy")

    review = build_gateway_adoption_review(target, current)

    assert review.to_dict() == {
        "resource_id": target.resource_id,
        "effective_vcluster": _VCLUSTER,
        "endpoint_fingerprint": _binding().endpoint_fingerprint,
        "alias_name": _ALIAS,
        "physical_cluster": "main",
        "observed_mapping_checksum": gateway_alias_mapping_checksum(
            "orders.legacy",
            "main",
        ),
        "desired_mapping_checksum": gateway_alias_mapping_checksum(
            _PHYSICAL,
            "main",
        ),
        "desired_artifact_checksum": target.desired_artifact_checksum,
        "pending_change_categories": ["alias_mapping"],
        "observed_aggregate_fingerprint": current.fingerprint,
        "desired_aggregate_fingerprint": target.desired.fingerprint,
    }
    serialized = json.dumps(review.to_dict(), sort_keys=True)
    for secret_or_raw_config in (
        _ENDPOINT,
        "private-admin-token",
        "orders.legacy",
        _PHYSICAL,
        "SELECT *",
    ):
        assert secret_or_raw_config not in serialized
    with pytest.raises(FrozenInstanceError):
        review.alias_name = "mutated"  # type: ignore[misc]


def test_review_has_no_pending_category_for_exact_mapping() -> None:
    target = _resolve()

    review = build_gateway_adoption_review(target, _observation())

    assert review.pending_change_categories == ()
    assert review.observed_mapping_checksum == review.desired_mapping_checksum
    assert review.observed_aggregate_fingerprint == review.desired_aggregate_fingerprint


def test_action_evidence_binds_exact_confirmed_and_desired_alias_surfaces() -> None:
    target = _resolve()
    current = _observation(physical="orders.legacy")

    evidence = build_gateway_adoption_action_evidence(target, current)

    assert evidence.version == 1
    assert evidence.backend_identity == target.binding.backend_identity
    assert evidence.rule_name == _RULE
    assert evidence.alias_name == _ALIAS
    assert evidence.current.exists is True
    assert evidence.current.fingerprint == current.fingerprint
    assert evidence.current.managed_interceptor_count == 0
    assert evidence.desired.exists is True
    assert evidence.desired.fingerprint == target.desired.fingerprint
    assert evidence.desired.managed_interceptor_count == 0
    assert _ENDPOINT not in json.dumps(evidence.to_dict(), sort_keys=True)
    assert (
        OperationAction(
            index=0,
            resource_id=target.resource_id,
            action="adopt",
            gateway_evidence=evidence,
        ).gateway_evidence
        == evidence
    )


@pytest.mark.parametrize(
    "observation",
    [
        object(),
        _observation(exists=False),
        _observation(binding=_binding(endpoint="https://other.example.test")),
        _observation(rule="other_rule"),
        _observation(alias="other.public"),
        _observation(interceptors=(_owned_interceptor(),)),
    ],
    ids=[
        "wrong-type",
        "absent",
        "wrong-binding",
        "wrong-rule",
        "wrong-alias",
        "owned-interceptor",
    ],
)
def test_observation_validation_rejects_nonexact_or_nonempty_surfaces(
    observation: object,
) -> None:
    error = (
        GatewayAdoptionLiveNotFoundError
        if isinstance(observation, ManagedGatewayRuleObservation) and not observation.exists
        else GatewayAdoptionError
    )
    with pytest.raises(error):
        validate_gateway_adoption_observation(
            _resolve(),
            observation,  # type: ignore[arg-type]
        )


def test_observation_validation_rejects_noncanonical_physical_cluster() -> None:
    current = _observation()
    object.__setattr__(current, "physical_cluster", "archive")

    with pytest.raises(GatewayAdoptionError, match="canonical physical cluster"):
        validate_gateway_adoption_observation(_resolve(), current)


def test_confirmation_requires_an_unchanged_aggregate_fingerprint() -> None:
    target = _resolve()
    first = _observation(physical="orders.legacy")
    same = _observation(physical="orders.legacy")
    changed = _observation(physical="orders.replaced")

    assert (
        require_unchanged_gateway_adoption_observation(
            target,
            first,
            same,
        )
        == same
    )
    with pytest.raises(GatewayAdoptionDriftError, match="changed after confirmation"):
        require_unchanged_gateway_adoption_observation(target, first, changed)


def test_mapping_checksum_rejects_incomplete_or_nonmain_mapping() -> None:
    with pytest.raises(GatewayAdoptionError, match="canonical main"):
        gateway_alias_mapping_checksum("", "main")
    with pytest.raises(GatewayAdoptionError, match="canonical main"):
        gateway_alias_mapping_checksum(_PHYSICAL, "archive")


def test_resolver_does_not_mutate_manifest_state_or_project() -> None:
    manifest = _manifest(_rule())
    state = _state()
    project = _project()
    manifest_before = deepcopy(manifest.to_dict())
    state_before = deepcopy(state.to_dict())
    project_before = project.model_dump(mode="json", by_alias=True)

    resolve_gateway_adoption_target(
        manifest,
        project,
        environment=_ENVIRONMENT,
        logical_name=_OWNER,
        prior_state=state,
    )

    assert manifest.to_dict() == manifest_before
    assert state.to_dict() == state_before
    assert project.model_dump(mode="json", by_alias=True) == project_before
