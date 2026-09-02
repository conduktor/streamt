"""Strict Gateway aggregate handling at planner and state boundaries."""

from __future__ import annotations

import json
import re
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from copy import deepcopy
from unittest.mock import MagicMock

import pytest

from streamt.compiler.gateway_artifact import (
    GatewayArtifactFormatError,
    parse_compiled_gateway_rule_artifact,
)
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
    GatewayBindingError,
    GatewayDeployer,
    GatewayDesiredAggregateError,
    GatewayRuleChange,
)
from streamt.deployer.plan_file import deployment_plan_payload
from streamt.deployer.planner import DeploymentPlan, DeploymentPlanner
from streamt.deployer.state import (
    LocalState,
    ManagedResourceRecord,
    StateFormatError,
    StateIdentityError,
    artifact_checksum,
    desired_managed_records,
    resource_id,
)

_ENDPOINT = "https://gateway.example.test:8443/admin"
_VCLUSTER = "payments-prod"
_FILTER_PLUGIN = "io.conduktor.gateway.interceptor.VirtualSqlTopicPlugin"
_CHECKSUM = re.compile(r"sha256:[0-9a-f]{64}")


def _ownership(owner_name: str) -> dict[str, str]:
    return ArtifactOwnership(
        project="payments",
        owner_type="model",
        owner_name=owner_name,
        mode="managed",
    ).to_dict()


def _rule(
    *,
    name: str = "orders_rule",
    alias: str = "orders.public",
    physical: str = "orders.v1",
    owner_name: str = "orders_model",
    where: str | None = None,
) -> dict[str, object]:
    interceptors: list[dict[str, object]] = []
    if where is not None:
        interceptors.append({"type": "filter", "config": {"where": where}})
    return {
        "name": name,
        "virtualTopic": alias,
        "physicalTopic": physical,
        "interceptors": interceptors,
        "ownership": _ownership(owner_name),
    }


def _manifest(*rules: object) -> Manifest:
    return Manifest(
        version="1.0",
        project_name="payments",
        artifacts={"gateway_rules": list(rules)},  # type: ignore[list-item]
    )


def _project(
    *,
    endpoint: str = _ENDPOINT,
    virtual_cluster: str | None = _VCLUSTER,
) -> StreamtProject:
    return StreamtProject(
        project=ProjectInfo(name="payments"),
        runtime=RuntimeConfig(
            kafka=KafkaConfig(bootstrap_servers="broker:9092"),
            conduktor=ConduktorConfig(
                gateway=GatewayConfig(
                    admin_url=endpoint,
                    virtual_cluster=virtual_cluster,
                )
            ),
        ),
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


def _response(payload: list[dict[str, object]]) -> MagicMock:
    response = MagicMock()
    response.status_code = 200
    response.headers = {}
    response.iter_content.return_value = [
        json.dumps(payload, separators=(",", ":")).encode("utf-8")
    ]
    return response


@contextmanager
def _mocked_gateway(
    *,
    endpoint: str = _ENDPOINT,
    virtual_cluster: str | None = _VCLUSTER,
    aliases: list[dict[str, object]] | None = None,
    interceptors: list[dict[str, object]] | None = None,
) -> Iterator[tuple[GatewayDeployer, MagicMock]]:
    deployer = GatewayDeployer(
        endpoint,
        virtual_cluster=virtual_cluster,
    )
    alias_payload = aliases or []
    interceptor_payload = interceptors or []

    def dispatch(method: str, url: str, **_kwargs: object) -> MagicMock:
        if method != "GET":
            return _response([])
        if url.endswith("/gateway/v2/alias-topic"):
            return _response(alias_payload)
        if url.endswith("/gateway/v2/interceptor"):
            return _response(interceptor_payload)
        return _response([])

    request = MagicMock(side_effect=dispatch)
    deployer._session.request = request  # type: ignore[method-assign]
    try:
        yield deployer, request
    finally:
        deployer.close()


def _alias_resource(
    rule: dict[str, object],
    *,
    physical: str | None = None,
    virtual_cluster: str = _VCLUSTER,
) -> dict[str, object]:
    return {
        "kind": "AliasTopic",
        "apiVersion": "gateway/v2",
        "metadata": {
            "name": rule["virtualTopic"],
            "vCluster": virtual_cluster,
        },
        "spec": {
            "physicalName": physical or rule["physicalTopic"],
            "physicalCluster": "main",
        },
    }


def _interceptor_resource(
    rule: dict[str, object],
    *,
    statement: str | None = None,
    virtual_cluster: str = _VCLUSTER,
) -> dict[str, object]:
    declarations = rule["interceptors"]
    assert isinstance(declarations, list)
    assert len(declarations) == 1
    declaration = declarations[0]
    assert isinstance(declaration, dict)
    config = declaration["config"]
    assert isinstance(config, dict)
    where = config["where"]
    assert isinstance(where, str)
    physical_topic = rule["physicalTopic"]
    assert isinstance(physical_topic, str)
    virtual_topic = rule["virtualTopic"]
    assert isinstance(virtual_topic, str)
    logical_name = rule["name"]
    assert isinstance(logical_name, str)
    return {
        "kind": "Interceptor",
        "apiVersion": "gateway/v2",
        "metadata": {
            "name": f"{logical_name}_filter_0",
            "scope": {"vCluster": virtual_cluster},
        },
        "spec": {
            "pluginClass": _FILTER_PLUGIN,
            "priority": 100,
            "config": {
                "virtualTopic": virtual_topic,
                "statement": statement
                or f'SELECT * FROM "{physical_topic}" WHERE {where}',
            },
        },
    }


def _prior_state(
    *rules: dict[str, object],
    backend: str | None = None,
) -> LocalState:
    selected_backend = backend or _binding().backend_identity
    resources: dict[str, ManagedResourceRecord] = {}
    for rule in rules:
        ownership = ArtifactOwnership.from_dict(rule["ownership"])
        assert ownership is not None
        alias_name = rule["virtualTopic"]
        assert isinstance(alias_name, str)
        resources[
            resource_id(
                "payments",
                "prod",
                "gateway_rule",
                ownership.owner_name,
            )
        ] = ManagedResourceRecord(
            physical_name=alias_name,
            ownership="managed",
            artifact_checksum=artifact_checksum(rule),
            backend=selected_backend,
        )
    return LocalState(
        project="payments",
        environment="prod",
        resources=resources,
    )


def _missing_field(value: dict[str, object]) -> object:
    value.pop("physicalTopic")
    return value


def _unknown_field(value: dict[str, object]) -> object:
    value["future"] = True
    return value


@pytest.mark.parametrize(
    "mutate",
    [_missing_field, _unknown_field],
    ids=["missing-field", "unknown-field"],
)
@pytest.mark.parametrize("mode", ["offline", "live"])
def test_planner_propagates_strict_gateway_artifact_errors_before_provider_access(
    mutate: Callable[[dict[str, object]], object],
    mode: str,
) -> None:
    malformed = mutate(deepcopy(_rule()))
    with _mocked_gateway() as (deployer, request):
        planner = DeploymentPlanner(
            _manifest(malformed),
            project=_project(),
            gateway_deployer=deployer,
        )

        with pytest.raises(GatewayArtifactFormatError):
            planner.offline_plan() if mode == "offline" else planner.plan()

        request.assert_not_called()


@pytest.mark.parametrize(
    "declarations",
    [
        [{"type": "mask", "config": {"field": "ssn", "method": "hash"}}],
        [
            {"type": "filter", "config": {"where": "region = 'us'"}},
            {"type": "filter", "config": {"where": "active = true"}},
        ],
    ],
    ids=["mask", "multiple-filters"],
)
@pytest.mark.parametrize("mode", ["offline", "live"])
def test_planner_rejects_unsupported_desired_transform_before_provider_access(
    declarations: list[dict[str, object]],
    mode: str,
) -> None:
    rule = _rule()
    rule["interceptors"] = declarations
    with _mocked_gateway() as (deployer, request):
        planner = DeploymentPlanner(
            _manifest(rule),
            project=_project(),
            gateway_deployer=deployer,
        )

        with pytest.raises(GatewayDesiredAggregateError):
            planner.offline_plan() if mode == "offline" else planner.plan()

        request.assert_not_called()


def test_offline_plan_uses_project_gateway_binding_without_provider_access() -> None:
    rule = _rule(where="region = 'us-east'")
    with _mocked_gateway() as (deployer, request):
        plan = DeploymentPlanner(
            _manifest(rule),
            project=_project(),
            gateway_deployer=deployer,
        ).offline_plan()

        assert len(plan.gateway_changes) == 1
        change = plan.gateway_changes[0]
        assert change.action == "create"
        assert change.desired is not None
        assert change.desired.virtual_topic == "orders.public"
        assert change.desired_managed is not None
        assert change.desired_managed.binding == _binding()
        assert change.backend_identity == _binding().backend_identity
        request.assert_not_called()


@pytest.mark.parametrize(
    ("endpoint", "virtual_cluster"),
    [
        ("https://old-gateway.example.test", _VCLUSTER),
        (_ENDPOINT, "payments-staging"),
    ],
    ids=["endpoint-drift", "vcluster-drift"],
)
def test_live_gateway_binding_must_match_project_before_provider_access(
    endpoint: str,
    virtual_cluster: str,
) -> None:
    with _mocked_gateway(
        endpoint=endpoint,
        virtual_cluster=virtual_cluster,
    ) as (deployer, request):
        with pytest.raises(GatewayBindingError, match="project runtime"):
            DeploymentPlanner(
                _manifest(_rule()),
                project=_project(),
                gateway_deployer=deployer,
            ).plan()

        request.assert_not_called()


@pytest.mark.parametrize(
    ("collision", "message"),
    [
        ("rule_name", "Gateway manifest contains a duplicate rule name"),
        ("owner", "Gateway manifest maps one logical owner to multiple rules"),
        ("alias", "Gateway manifest contains a duplicate canonical alias locator"),
        ("interceptor", "Gateway manifest contains a duplicate interceptor locator"),
        (
            "generated_namespace",
            "Gateway generated interceptor identity maps to multiple rules",
        ),
    ],
)
def test_gateway_identity_collisions_fail_before_provider_access(
    collision: str,
    message: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    first = _rule(
        name="first_rule",
        alias="orders.first",
        owner_name="first_owner",
        where=(
            "region = 'us'"
            if collision in {"interceptor", "generated_namespace"}
            else None
        ),
    )
    second = _rule(
        name="second_rule",
        alias="orders.second",
        owner_name="second_owner",
        where=(
            "region = 'eu'"
            if collision in {"interceptor", "generated_namespace"}
            else None
        ),
    )
    if collision == "rule_name":
        second["name"] = first["name"]
    elif collision == "owner":
        second["ownership"] = _ownership("first_owner")
    elif collision == "alias":
        second["virtualTopic"] = first["virtualTopic"]
    elif collision == "interceptor":
        monkeypatch.setattr(
            "streamt.deployer.gateway.generate_gateway_interceptor_name",
            lambda _logical_name, _declaration_type, _ordinal: "shared_filter_0",
        )
    else:
        monkeypatch.setattr(
            "streamt.deployer.gateway.classify_gateway_interceptor_name",
            lambda _logical_name, _candidate: object(),
        )

    with _mocked_gateway() as (deployer, request):
        with pytest.raises(StateIdentityError, match=message):
            DeploymentPlanner(
                _manifest(first, second),
                project=_project(),
                gateway_deployer=deployer,
            ).plan()

        request.assert_not_called()


def _assert_exact_two_list_gets(request: MagicMock) -> None:
    assert request.call_count == 2
    methods_and_urls: list[tuple[object, object]] = []
    for call in request.call_args_list:
        method = call.args[0] if call.args else call.kwargs.get("method")
        url = call.args[1] if len(call.args) > 1 else call.kwargs.get("url")
        methods_and_urls.append((method, url))
    assert methods_and_urls == [
        ("GET", f"{_ENDPOINT}/gateway/v2/alias-topic"),
        ("GET", f"{_ENDPOINT}/gateway/v2/interceptor"),
    ]


def test_two_rules_share_one_snapshot_and_plan_exact_absent_creates() -> None:
    first = _rule(name="first_rule", alias="orders.first", owner_name="first_owner")
    second = _rule(name="second_rule", alias="orders.second", owner_name="second_owner")
    with _mocked_gateway() as (deployer, request):
        plan = DeploymentPlanner(
            _manifest(first, second),
            project=_project(),
            gateway_deployer=deployer,
        ).plan()

        assert [change.action for change in plan.gateway_changes] == ["create", "create"]
        _assert_exact_two_list_gets(request)


def test_two_rules_share_one_snapshot_and_exact_desired_live_is_noop() -> None:
    first = _rule(name="first_rule", alias="orders.first", owner_name="first_owner")
    second = _rule(
        name="second_rule",
        alias="orders.second",
        owner_name="second_owner",
        where="region = 'us-east'",
    )
    with _mocked_gateway(
        aliases=[_alias_resource(first), _alias_resource(second)],
        interceptors=[_interceptor_resource(second)],
    ) as (deployer, request):
        plan = DeploymentPlanner(
            _manifest(first, second),
            project=_project(),
            gateway_deployer=deployer,
            prior_state=_prior_state(first, second),
        ).plan()

        assert [change.action for change in plan.gateway_changes] == ["none", "none"]
        assert plan.ownership_requirements == []
        _assert_exact_two_list_gets(request)


def test_two_rules_share_one_snapshot_and_alias_or_config_drift_updates() -> None:
    alias_drift = _rule(
        name="alias_rule",
        alias="orders.alias",
        physical="orders.v1",
        owner_name="alias_owner",
    )
    config_drift = _rule(
        name="filter_rule",
        alias="orders.filtered",
        owner_name="filter_owner",
        where="region = 'us-east'",
    )
    with _mocked_gateway(
        aliases=[
            _alias_resource(alias_drift, physical="orders.old"),
            _alias_resource(config_drift),
        ],
        interceptors=[
            _interceptor_resource(
                config_drift,
                statement='SELECT * FROM "orders.v1" WHERE region = \'eu-west\'',
            )
        ],
    ) as (deployer, request):
        plan = DeploymentPlanner(
            _manifest(alias_drift, config_drift),
            project=_project(),
            gateway_deployer=deployer,
            prior_state=_prior_state(alias_drift, config_drift),
        ).plan()

        assert [change.action for change in plan.gateway_changes] == ["update", "update"]
        assert plan.ownership_requirements == []
        _assert_exact_two_list_gets(request)


def test_legacy_gateway_backend_never_authorizes_exact_logical_prior() -> None:
    rule = _rule(owner_name="orders_model")
    with _mocked_gateway() as (deployer, request):
        plan = DeploymentPlanner(
            _manifest(rule),
            project=_project(),
            gateway_deployer=deployer,
            prior_state=_prior_state(rule, backend="conduktor-gateway"),
        ).plan()

        assert plan.gateway_changes[0].action == "none"
        assert len(plan.ownership_requirements) == 1
        assert plan.ownership_requirements[0].reason == "state_mismatch"
        assert desired_managed_records(
            plan,
            project="payments",
            environment="prod",
        ) == {}
        assert all(call.args[0] == "GET" for call in request.call_args_list)
        _assert_exact_two_list_gets(request)


def test_prior_other_logical_record_claiming_desired_alias_fails_preflight() -> None:
    desired = _rule(owner_name="orders_model")
    other = _rule(owner_name="other_model")
    prior_state = _prior_state(other)

    with _mocked_gateway() as (deployer, request):
        with pytest.raises(StateIdentityError, match="Gateway"):
            DeploymentPlanner(
                _manifest(desired),
                project=_project(),
                gateway_deployer=deployer,
                prior_state=prior_state,
            ).plan()

        request.assert_not_called()


def test_duplicate_prior_canonical_gateway_claims_fail_before_provider_access() -> None:
    first = _rule(alias="orders.shared", owner_name="first_owner")
    second = _rule(alias="orders.shared", owner_name="second_owner")
    desired = _rule(alias="orders.new", owner_name="new_owner")

    with _mocked_gateway() as (deployer, request):
        with pytest.raises(StateIdentityError, match="Gateway"):
            DeploymentPlanner(
                _manifest(desired),
                project=_project(),
                gateway_deployer=deployer,
                prior_state=_prior_state(first, second),
            ).plan()

        request.assert_not_called()


def test_overlapping_rule_names_remain_exact_independent_identities() -> None:
    orders = _rule(
        name="orders",
        alias="orders.public",
        owner_name="orders_owner",
        where="region = 'us-east'",
    )
    archive = _rule(
        name="orders_archive",
        alias="orders.archive",
        owner_name="archive_owner",
        where="region = 'us-west'",
    )
    with _mocked_gateway(
        aliases=[_alias_resource(orders), _alias_resource(archive)],
        interceptors=[
            _interceptor_resource(orders),
            _interceptor_resource(archive),
        ],
    ) as (deployer, request):
        plan = DeploymentPlanner(
            _manifest(orders, archive),
            project=_project(),
            gateway_deployer=deployer,
            prior_state=_prior_state(orders, archive),
        ).plan()

        assert [change.action for change in plan.gateway_changes] == ["none", "none"]
        assert [
            tuple(interceptor.name for interceptor in change.current.interceptors)
            for change in plan.gateway_changes
            if change.current is not None
        ] == [("orders_filter_0",), ("orders_archive_filter_0",)]
        _assert_exact_two_list_gets(request)


def test_logical_owner_rule_name_and_alias_remain_distinct() -> None:
    rule = _rule(
        name="orders_access_rule",
        alias="orders.public",
        owner_name="orders_model",
    )
    state = _prior_state(rule)
    assert resource_id(
        "payments", "prod", "gateway_rule", "orders_model"
    ) in state.resources
    assert resource_id(
        "payments", "prod", "gateway_rule", "orders_access_rule"
    ) not in state.resources
    assert resource_id(
        "payments", "prod", "gateway_rule", "orders.public"
    ) not in state.resources

    with _mocked_gateway(aliases=[_alias_resource(rule)]) as (deployer, request):
        plan = DeploymentPlanner(
            _manifest(rule),
            project=_project(),
            gateway_deployer=deployer,
            prior_state=state,
        ).plan()

        assert plan.gateway_changes[0].name == "orders_access_rule"
        assert plan.gateway_changes[0].action == "none"
        assert plan.ownership_requirements == []
        _assert_exact_two_list_gets(request)


def test_gateway_plan_rendering_and_change_evidence_never_expose_raw_config() -> None:
    desired_secret = "raw-filter-secret-9a7c"
    observed_secret = "observed-filter-secret-4d21"
    rule = _rule(
        name="secret_rule",
        alias="orders.secret",
        owner_name="secret_owner",
        where=f"customer_token = '{desired_secret}'",
    )
    with _mocked_gateway(
        aliases=[_alias_resource(rule)],
        interceptors=[
            _interceptor_resource(
                rule,
                statement=(
                    'SELECT * FROM "orders.v1" WHERE '
                    f"customer_token = '{observed_secret}'"
                ),
            )
        ],
    ) as (deployer, _request):
        plan = DeploymentPlanner(
            _manifest(rule),
            project=_project(),
            gateway_deployer=deployer,
            prior_state=_prior_state(rule),
        ).plan()

    change = plan.gateway_changes[0]
    assert change.action == "update"
    assert change.changes is not None
    normalized_evidence = json.dumps(change.changes, sort_keys=True, default=str)
    rendered = "\n".join(
        (
            repr(plan),
            plan.details(color=False),
            normalized_evidence,
            json.dumps(deployment_plan_payload(plan), sort_keys=True),
        )
    )
    assert desired_secret not in rendered
    assert observed_secret not in rendered
    assert "SELECT *" not in rendered
    assert "interceptor" in normalized_evidence.lower()
    assert len(set(_CHECKSUM.findall(normalized_evidence))) >= 2


def test_gateway_state_projection_uses_alias_bound_backend_and_strict_checksum() -> None:
    rule = _rule(where="region = 'us-east'")
    plan = DeploymentPlanner(
        _manifest(rule),
        project=_project(),
        project_name="payments",
        environment="prod",
    ).offline_plan()

    records = desired_managed_records(
        plan,
        project="payments",
        environment="prod",
    )
    record = records[
        resource_id("payments", "prod", "gateway_rule", "orders_model")
    ]
    parsed = parse_compiled_gateway_rule_artifact(rule)
    assert record.physical_name == "orders.public"
    assert record.backend == _binding().backend_identity
    assert record.artifact_checksum == artifact_checksum(parsed.to_dict())


@pytest.mark.parametrize("backend", [None, "conduktor-gateway"])
def test_gateway_state_projection_rejects_missing_or_legacy_backend(
    backend: str | None,
) -> None:
    plan = DeploymentPlanner(
        _manifest(_rule()),
        project=_project(),
        project_name="payments",
        environment="prod",
    ).offline_plan()
    plan.gateway_changes[0].backend_identity = backend

    with pytest.raises(StateFormatError, match="Gateway"):
        desired_managed_records(
            plan,
            project="payments",
            environment="prod",
        )


def test_gateway_state_projection_ignores_a_delete_without_desired_state() -> None:
    plan = DeploymentPlan(
        gateway_changes=[GatewayRuleChange(name="orders_rule", action="delete")]
    )

    assert desired_managed_records(
        plan,
        project="payments",
        environment="prod",
    ) == {}
