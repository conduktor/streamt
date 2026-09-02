"""Exact managed Conduktor Gateway mutation and rollback tests."""

from __future__ import annotations

import json
from copy import deepcopy
from dataclasses import FrozenInstanceError
from typing import Any
from unittest.mock import MagicMock

import pytest
import requests

from streamt.compiler.manifest import GatewayRuleArtifact
from streamt.deployer.gateway import (
    GatewayAliasLocator,
    GatewayDeployer,
    GatewayInterceptorLocator,
    GatewayManagedMutationError,
    GatewayRuleChange,
    GatewayRuleLocator,
    GatewayRuleMutation,
    ManagedGatewayInterceptor,
    ManagedGatewayRuleObservation,
    build_desired_gateway_rule,
    plan_managed_gateway_rule_deletion,
)


def _deployer(*, virtual_cluster: str | None = "production") -> GatewayDeployer:
    return GatewayDeployer(
        "https://gateway.example.test/admin",
        username="operator",
        password="credential-secret",
        virtual_cluster=virtual_cluster,
    )


def _artifact(*, physical: str = "orders.v1", with_filter: bool = False) -> GatewayRuleArtifact:
    interceptors: list[dict[str, object]] = []
    if with_filter:
        interceptors = [{"type": "filter", "config": {"where": "amount > 100"}}]
    return GatewayRuleArtifact(
        name="orders",
        virtual_topic="orders.public",
        physical_topic=physical,
        interceptors=interceptors,
    )


def _desired(
    deployer: GatewayDeployer,
    *,
    physical: str = "orders.v1",
    with_filter: bool = False,
) -> ManagedGatewayRuleObservation:
    return build_desired_gateway_rule(
        _artifact(physical=physical, with_filter=with_filter),
        deployer.cluster_binding,
    )


def _absent(deployer: GatewayDeployer) -> ManagedGatewayRuleObservation:
    return ManagedGatewayRuleObservation(
        binding=deployer.cluster_binding,
        logical_name="orders",
        alias_name="orders.public",
        exists=False,
    )


def _present(
    deployer: GatewayDeployer,
    *,
    physical: str = "orders.v1",
    interceptors: tuple[ManagedGatewayInterceptor, ...] = (),
) -> ManagedGatewayRuleObservation:
    return ManagedGatewayRuleObservation(
        binding=deployer.cluster_binding,
        logical_name="orders",
        alias_name="orders.public",
        exists=True,
        physical_name=physical,
        physical_cluster="main",
        interceptors=interceptors,
    )


def _raw_response(
    status_code: int,
    body: bytes = b"",
    *,
    headers: dict[str, str] | None = None,
) -> MagicMock:
    response = MagicMock()
    response.status_code = status_code
    response.headers = headers or {}
    response.iter_content.return_value = [body]
    return response


def _json_response(status_code: int, value: object) -> MagicMock:
    return _raw_response(
        status_code,
        json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode(),
    )


def _alias_resource(
    observation: ManagedGatewayRuleObservation,
    *,
    omit_passthrough: bool = True,
) -> dict[str, object]:
    metadata: dict[str, object] = {
        "name": observation.alias_name,
        "vCluster": observation.binding.virtual_cluster,
    }
    if omit_passthrough and observation.binding.virtual_cluster == "passthrough":
        metadata.pop("vCluster")
    return {
        "kind": "AliasTopic",
        "apiVersion": "gateway/v2",
        "metadata": metadata,
        "spec": {
            "physicalName": observation.physical_name,
            "physicalCluster": "main",
        },
    }


def _interceptor_resource(
    interceptor: ManagedGatewayInterceptor,
    *,
    comment: str | None = "",
    scope: dict[str, object] | None = None,
) -> dict[str, object]:
    spec: dict[str, object] = {
        "pluginClass": interceptor.plugin_class,
        "priority": interceptor.priority,
        "config": json.loads(interceptor.config_json),
    }
    if comment is not None:
        spec["comment"] = comment
    return {
        "kind": "Interceptor",
        "apiVersion": "gateway/v2",
        "metadata": {
            "name": interceptor.name,
            "scope": scope if scope is not None else dict(interceptor.scope),
        },
        "spec": spec,
    }


def _put_response(resource: dict[str, object], result: str) -> MagicMock:
    return _json_response(200, {"resource": resource, "upsertResult": result})


def _request_kwargs(deployer: GatewayDeployer) -> list[dict[str, Any]]:
    return [call.kwargs for call in deployer._session.request.call_args_list]  # type: ignore[attr-defined]


def test_alias_only_create_has_one_exact_passthrough_put() -> None:
    deployer = _deployer(virtual_cluster=None)
    desired = _desired(deployer)
    response = _put_response(_alias_resource(desired), "Created")
    deployer._session.request = MagicMock(return_value=response)  # type: ignore[method-assign]

    assert deployer.apply_managed_gateway_rule(_absent(deployer), desired) == "created"

    [request] = _request_kwargs(deployer)
    assert request == {
        "method": "PUT",
        "url": "https://gateway.example.test/admin/gateway/v2/alias-topic",
        "json": {
            "kind": "AliasTopic",
            "apiVersion": "gateway/v2",
            "metadata": {"name": "orders.public", "vCluster": "passthrough"},
            "spec": {"physicalName": "orders.v1", "physicalCluster": "main"},
        },
        "timeout": 10,
        "allow_redirects": False,
        "stream": True,
    }
    response.close.assert_called_once_with()


def test_filter_create_orders_alias_before_exact_interceptor() -> None:
    deployer = _deployer()
    desired = _desired(deployer, with_filter=True)
    interceptor = desired.interceptors[0]
    alias_response = _put_response(_alias_resource(desired), "Created")
    interceptor_response = _put_response(_interceptor_resource(interceptor), "Created")
    deployer._session.request = MagicMock(  # type: ignore[method-assign]
        side_effect=[alias_response, interceptor_response]
    )

    assert deployer.apply_managed_gateway_rule(_absent(deployer), desired) == "created"

    alias_request, interceptor_request = _request_kwargs(deployer)
    assert alias_request["url"].endswith("/alias-topic")
    assert interceptor_request["url"].endswith("/interceptor")
    assert interceptor_request["json"] == {
        "kind": "Interceptor",
        "apiVersion": "gateway/v2",
        "metadata": {
            "name": "orders_filter_0",
            "scope": {"group": None, "username": None, "vCluster": "production"},
        },
        "spec": {
            "pluginClass": "io.conduktor.gateway.interceptor.VirtualSqlTopicPlugin",
            "priority": 100,
            "config": {
                "statement": 'SELECT * FROM "orders.v1" WHERE amount > 100',
                "virtualTopic": "orders.public",
            },
        },
    }
    alias_response.close.assert_called_once_with()
    interceptor_response.close.assert_called_once_with()


def test_exact_no_op_returns_unchanged_without_any_request() -> None:
    deployer = _deployer()
    desired = _desired(deployer, with_filter=True)
    deployer._session.request = MagicMock()  # type: ignore[method-assign]

    assert deployer.apply_managed_gateway_rule(desired, desired) == "unchanged"
    deployer._session.request.assert_not_called()  # type: ignore[attr-defined]


def test_update_writes_only_changed_interceptor_and_not_equal_alias() -> None:
    deployer = _deployer()
    desired = _desired(deployer, with_filter=True)
    changed = ManagedGatewayInterceptor(
        name=desired.interceptors[0].name,
        scope=desired.interceptors[0].scope,
        plugin_class=desired.interceptors[0].plugin_class,
        priority=desired.interceptors[0].priority,
        config_json='{"statement":"SELECT * FROM old","virtualTopic":"orders.public"}',
    )
    current = _present(deployer, interceptors=(changed,))
    response = _put_response(_interceptor_resource(desired.interceptors[0]), "Updated")
    deployer._session.request = MagicMock(return_value=response)  # type: ignore[method-assign]

    assert deployer.apply_managed_gateway_rule(current, desired) == "updated"

    [request] = _request_kwargs(deployer)
    assert request["url"].endswith("/interceptor")
    assert request["method"] == "PUT"


def test_apply_deletes_only_exact_stale_identity_after_desired_writes() -> None:
    deployer = _deployer()
    desired = _desired(deployer)
    stale = ManagedGatewayInterceptor(
        name="orders_readonly_0",
        scope=(("group", None), ("username", None), ("vCluster", "production")),
        plugin_class="example.LegacyPlugin",
        priority=100,
        config_json='{"enabled":true}',
    )
    current = _present(deployer, interceptors=(stale,))
    delete_response = _raw_response(204)
    deployer._session.request = MagicMock(return_value=delete_response)  # type: ignore[method-assign]

    assert deployer.apply_managed_gateway_rule(current, desired) == "updated"

    [request] = _request_kwargs(deployer)
    assert request["method"] == "DELETE"
    assert request["url"].endswith("/interceptor/orders_readonly_0")
    assert request["json"] == {
        "group": None,
        "username": None,
        "vCluster": "production",
    }


def test_delete_filter_uses_exact_interceptor_then_alias_collection_body() -> None:
    deployer = _deployer(virtual_cluster=None)
    current = _desired(deployer, with_filter=True)
    interceptor_delete = _raw_response(204)
    alias_delete = _raw_response(204)
    deployer._session.request = MagicMock(  # type: ignore[method-assign]
        side_effect=[interceptor_delete, alias_delete]
    )

    assert deployer.delete_managed_gateway_rule(current) == "deleted"

    interceptor_request, alias_request = _request_kwargs(deployer)
    assert interceptor_request["url"].endswith("/interceptor/orders_filter_0")
    assert interceptor_request["json"] == {
        "group": None,
        "username": None,
        "vCluster": "passthrough",
    }
    assert alias_request["url"].endswith("/alias-topic")
    assert alias_request["json"] == {
        "name": "orders.public",
        "vCluster": "passthrough",
    }


def test_interceptor_delete_operation_percent_encodes_the_exact_name() -> None:
    deployer = _deployer()
    interceptor = ManagedGatewayInterceptor(
        name="orders_filter_0?copy=other/name",
        scope=(("group", None), ("username", None), ("vCluster", "production")),
        plugin_class="example.Plugin",
        priority=100,
        config_json="{}",
    )

    operation = deployer._managed_interceptor_delete_operation(
        deployer.cluster_binding,
        interceptor,
    )

    assert operation.endpoint == "/interceptor/orders_filter_0%3Fcopy%3Dother%2Fname"


@pytest.mark.parametrize("comment", [None, ""])
def test_interceptor_response_accepts_only_requested_or_empty_comment(
    comment: str | None,
) -> None:
    deployer = _deployer()
    desired = _desired(deployer, with_filter=True)
    alias_response = _put_response(_alias_resource(desired), "Created")
    interceptor_response = _put_response(
        _interceptor_resource(desired.interceptors[0], comment=comment),
        "Created",
    )
    deployer._session.request = MagicMock(  # type: ignore[method-assign]
        side_effect=[alias_response, interceptor_response]
    )

    assert deployer.apply_managed_gateway_rule(_absent(deployer), desired) == "created"


@pytest.mark.parametrize(
    ("comment", "scope"),
    [
        ("provider changed it", None),
        ("", {"vCluster": "production"}),
    ],
)
def test_interceptor_response_rejects_unproven_normalization(
    comment: str,
    scope: dict[str, object] | None,
) -> None:
    deployer = _deployer()
    desired = _desired(deployer, with_filter=True)
    alias_response = _put_response(_alias_resource(desired), "Created")
    interceptor_response = _put_response(
        _interceptor_resource(
            desired.interceptors[0],
            comment=comment,
            scope=scope,
        ),
        "Created",
    )
    rollback = _raw_response(204)
    deployer._session.request = MagicMock(  # type: ignore[method-assign]
        side_effect=[alias_response, interceptor_response, rollback]
    )

    with pytest.raises(GatewayManagedMutationError, match="orders_filter_0"):
        deployer.apply_managed_gateway_rule(_absent(deployer), desired)

    assert len(_request_kwargs(deployer)) == 3


@pytest.mark.parametrize("status", [201, 204, 301, 401, 403, 500])
def test_upsert_accepts_only_200_without_retry(status: int) -> None:
    deployer = _deployer()
    response = _raw_response(status, b'{"credential-secret":"must-not-leak"}')
    deployer._session.request = MagicMock(return_value=response)  # type: ignore[method-assign]

    with pytest.raises(GatewayManagedMutationError) as caught:
        deployer.apply_managed_gateway_rule(_absent(deployer), _desired(deployer))

    assert deployer._session.request.call_count == 1  # type: ignore[attr-defined]
    assert "credential-secret" not in str(caught.value)
    assert "gateway.example.test" not in str(caught.value)
    response.close.assert_called_once_with()


@pytest.mark.parametrize("status", [200, 201, 301, 401, 403, 404, 500])
def test_delete_accepts_only_204_and_404_is_an_exact_conflict(status: int) -> None:
    deployer = _deployer()
    response = _raw_response(status)
    deployer._session.request = MagicMock(return_value=response)  # type: ignore[method-assign]

    with pytest.raises(GatewayManagedMutationError) as caught:
        deployer.delete_managed_gateway_rule(_desired(deployer))

    assert deployer._session.request.call_count == 1  # type: ignore[attr-defined]
    assert "orders.public" in str(caught.value)
    response.close.assert_called_once_with()


def _malformed_upsert_cases(desired: ManagedGatewayRuleObservation) -> list[MagicMock]:
    valid_resource = _alias_resource(desired)
    oversized = b"x" * (1024 * 1024 + 1)
    return [
        _raw_response(200, b"\xff"),
        _raw_response(200, b'{"resource":{},"resource":{},"upsertResult":"Created"}'),
        _raw_response(200, b'{"resource":{},"upsertResult":NaN}'),
        _raw_response(200, oversized),
        _json_response(200, {"resource": valid_resource}),
        _json_response(
            200,
            {"resource": valid_resource, "upsertResult": "Created", "extra": True},
        ),
        _json_response(200, {"resource": valid_resource, "upsertResult": "created"}),
        _json_response(
            200,
            {
                "resource": {
                    **valid_resource,
                    "spec": {"physicalName": "another.topic", "physicalCluster": "main"},
                },
                "upsertResult": "Created",
            },
        ),
    ]


def test_malformed_duplicate_nonfinite_and_oversized_upserts_fail_closed() -> None:
    deployer = _deployer()
    desired = _desired(deployer)

    for response in _malformed_upsert_cases(desired):
        deployer._session.request = MagicMock(return_value=response)  # type: ignore[method-assign]
        with pytest.raises(GatewayManagedMutationError) as caught:
            deployer.apply_managed_gateway_rule(_absent(deployer), desired)
        assert "orders.public" in str(caught.value)
        assert deployer._session.request.call_count == 1  # type: ignore[attr-defined]
        response.close.assert_called_once_with()


@pytest.mark.parametrize("result", ["NotChanged", "Updated"])
def test_create_result_mismatch_is_an_unresolved_ownership_conflict(result: str) -> None:
    deployer = _deployer()
    desired = _desired(deployer)
    response = _put_response(_alias_resource(desired), result)
    deployer._session.request = MagicMock(return_value=response)  # type: ignore[method-assign]

    with pytest.raises(GatewayManagedMutationError) as caught:
        deployer.apply_managed_gateway_rule(_absent(deployer), desired)

    assert "orders.public" in str(caught.value)
    assert deployer._session.request.call_count == 1  # type: ignore[attr-defined]


def test_request_exception_is_not_retried_or_echoed() -> None:
    deployer = _deployer()
    deployer._session.request = MagicMock(  # type: ignore[method-assign]
        side_effect=requests.Timeout("credential-secret https://gateway.example.test")
    )

    with pytest.raises(GatewayManagedMutationError) as caught:
        deployer.apply_managed_gateway_rule(_absent(deployer), _desired(deployer))

    assert deployer._session.request.call_count == 1  # type: ignore[attr-defined]
    assert "orders.public" in str(caught.value)
    assert "credential-secret" not in str(caught.value)
    assert "gateway.example.test" not in str(caught.value)


def test_mid_create_failure_rolls_back_only_confirmed_alias() -> None:
    deployer = _deployer()
    desired = _desired(deployer, with_filter=True)
    alias_created = _put_response(_alias_resource(desired), "Created")
    ambiguous_interceptor = _put_response(
        _interceptor_resource(desired.interceptors[0]),
        "NotChanged",
    )
    alias_rollback = _raw_response(204)
    deployer._session.request = MagicMock(  # type: ignore[method-assign]
        side_effect=[alias_created, ambiguous_interceptor, alias_rollback]
    )

    with pytest.raises(GatewayManagedMutationError) as caught:
        deployer.apply_managed_gateway_rule(_absent(deployer), desired)

    requests_made = _request_kwargs(deployer)
    assert [request["method"] for request in requests_made] == ["PUT", "PUT", "DELETE"]
    assert requests_made[-1]["url"].endswith("/alias-topic")
    assert requests_made[-1]["json"] == {
        "name": "orders.public",
        "vCluster": "production",
    }
    assert "orders_filter_0" in str(caught.value)
    assert "credential-secret" not in str(caught.value)


def test_mid_delete_failure_restores_confirmed_interceptor() -> None:
    deployer = _deployer()
    current = _desired(deployer, with_filter=True)
    interceptor = current.interceptors[0]
    interceptor_deleted = _raw_response(204)
    alias_conflict = _raw_response(404)
    interceptor_restored = _put_response(_interceptor_resource(interceptor), "Created")
    deployer._session.request = MagicMock(  # type: ignore[method-assign]
        side_effect=[interceptor_deleted, alias_conflict, interceptor_restored]
    )

    with pytest.raises(GatewayManagedMutationError) as caught:
        deployer.delete_managed_gateway_rule(current)

    requests_made = _request_kwargs(deployer)
    assert [request["method"] for request in requests_made] == ["DELETE", "DELETE", "PUT"]
    assert requests_made[-1]["json"]["metadata"]["name"] == "orders_filter_0"
    assert "orders.public" in str(caught.value)


def test_updated_alias_rolls_back_with_exact_prior_content() -> None:
    deployer = _deployer()
    current_filter = _desired(deployer, physical="orders.old", with_filter=True)
    desired = _desired(deployer, physical="orders.v2", with_filter=True)
    alias_updated = _put_response(_alias_resource(desired), "Updated")
    interceptor_conflict = _put_response(
        _interceptor_resource(desired.interceptors[0]),
        "NotChanged",
    )
    alias_restored = _put_response(_alias_resource(current_filter), "Updated")
    deployer._session.request = MagicMock(  # type: ignore[method-assign]
        side_effect=[alias_updated, interceptor_conflict, alias_restored]
    )

    with pytest.raises(GatewayManagedMutationError):
        deployer.apply_managed_gateway_rule(current_filter, desired)

    rollback_request = _request_kwargs(deployer)[-1]
    assert rollback_request["method"] == "PUT"
    assert rollback_request["json"]["spec"] == {
        "physicalName": "orders.old",
        "physicalCluster": "main",
    }


def test_rollback_failures_are_aggregated_with_only_exact_safe_identities() -> None:
    deployer = _deployer()
    desired = _desired(deployer, with_filter=True)
    alias_created = _put_response(_alias_resource(desired), "Created")
    interceptor_failed = _raw_response(500, b"credential-secret")
    rollback_failed = _raw_response(500, b"credential-secret")
    deployer._session.request = MagicMock(  # type: ignore[method-assign]
        side_effect=[alias_created, interceptor_failed, rollback_failed]
    )

    with pytest.raises(GatewayManagedMutationError) as caught:
        deployer.apply_managed_gateway_rule(_absent(deployer), desired)

    rendered = str(caught.value)
    assert "Interceptor scope='production' name='orders_filter_0'" in rendered
    assert "AliasTopic scope='production' name='orders.public'" in rendered
    assert "credential-secret" not in rendered
    assert "gateway.example.test" not in rendered
    assert "SELECT" not in rendered
    assert deployer._session.request.call_count == 3  # type: ignore[attr-defined]


def test_all_payloads_are_validated_before_the_first_request() -> None:
    deployer = _deployer()
    desired = _desired(deployer, with_filter=True)
    object.__setattr__(desired.interceptors[0], "config_json", "credential-secret")
    deployer._session.request = MagicMock()  # type: ignore[method-assign]

    with pytest.raises(GatewayManagedMutationError):
        deployer.apply_managed_gateway_rule(_absent(deployer), desired)

    deployer._session.request.assert_not_called()  # type: ignore[attr-defined]


def test_locators_are_immutable_sorted_unique_scoped_and_repr_safe() -> None:
    deployer = _deployer()
    observation = _desired(deployer, with_filter=True)
    locator = GatewayRuleLocator.from_observation(observation)

    assert locator.alias == GatewayAliasLocator(deployer.cluster_binding, "orders.public")
    assert locator.interceptors == (
        GatewayInterceptorLocator(
            deployer.cluster_binding,
            "orders_filter_0",
            (("group", None), ("username", None), ("vCluster", "production")),
        ),
    )
    assert "credential-secret" not in repr(locator)
    assert "SELECT" not in repr(locator)
    assert "gateway.example.test" not in repr(locator)
    with pytest.raises(FrozenInstanceError):
        locator.alias_name = "changed"  # type: ignore[misc]

    duplicate = locator.interceptors + locator.interceptors
    with pytest.raises(GatewayManagedMutationError):
        GatewayRuleLocator(
            locator.binding,
            locator.logical_name,
            locator.alias_name,
            duplicate,
        )


def test_locator_rejects_absent_unsorted_wrong_scope_and_overlapping_names() -> None:
    deployer = _deployer()
    with pytest.raises(GatewayManagedMutationError):
        GatewayRuleLocator.from_observation(_absent(deployer))

    right = GatewayInterceptorLocator(
        deployer.cluster_binding,
        "orders_filter_0",
        (("group", None), ("username", None), ("vCluster", "production")),
    )
    overlapping = GatewayInterceptorLocator(
        deployer.cluster_binding,
        "orders_archive_filter_0",
        (("group", None), ("username", None), ("vCluster", "production")),
    )
    with pytest.raises(GatewayManagedMutationError, match="unrelated"):
        GatewayRuleLocator(
            deployer.cluster_binding,
            "orders",
            "orders.public",
            (overlapping,),
        )
    with pytest.raises(GatewayManagedMutationError):
        GatewayInterceptorLocator(
            deployer.cluster_binding,
            right.name,
            (("group", None), ("username", None), ("vCluster", "other")),
        )


def test_mutation_validates_all_four_transitions_and_hides_surfaces_from_repr() -> None:
    deployer = _deployer()
    absent = _absent(deployer)
    first = _desired(deployer)
    changed = _desired(deployer, physical="orders.v2")

    assert GatewayRuleMutation("create", absent, first).expected_action == "create"
    assert GatewayRuleMutation("update", first, changed).expected_action == "update"
    assert GatewayRuleMutation("delete", first, None).expected_action == "delete"
    no_op = GatewayRuleMutation("no-op", first, first)
    assert no_op.expected_action == "no-op"
    assert "orders.v1" not in repr(no_op)
    assert "current=" not in repr(no_op)
    assert "desired=" not in repr(no_op)

    for action, current, desired in [
        ("update", absent, first),
        ("create", first, changed),
        ("no-op", first, changed),
        ("delete", absent, None),
    ]:
        with pytest.raises(GatewayManagedMutationError):
            GatewayRuleMutation(action, current, desired)


def test_managed_transport_does_not_use_or_change_legacy_request_path() -> None:
    deployer = _deployer()
    desired = _desired(deployer)
    response = _put_response(_alias_resource(desired), "Created")
    deployer._session.request = MagicMock(return_value=response)  # type: ignore[method-assign]
    deployer._request = MagicMock(side_effect=AssertionError("legacy path used"))  # type: ignore[method-assign]

    assert deployer.apply_managed_gateway_rule(_absent(deployer), desired) == "created"
    deployer._request.assert_not_called()  # type: ignore[attr-defined]


def test_normalized_deletion_helper_builds_exact_secret_neutral_evidence() -> None:
    deployer = _deployer()
    current = _desired(deployer, with_filter=True)

    change = plan_managed_gateway_rule_deletion(current)

    assert isinstance(change, GatewayRuleChange)
    assert change.name == "orders"
    assert change.action == "delete"
    assert change.current == current
    assert change.current is not current
    assert change.desired is None
    assert change.desired_managed is None
    assert change.current_alias is None
    assert change.current_interceptors is None
    assert change.backend_identity == current.binding.backend_identity
    assert change.changes is not None
    assert change.changes["categories"] == ["presence"]
    assert change.changes["current"] == {
        "exists": True,
        "fingerprint": current.fingerprint,
        "managed_interceptor_count": 1,
    }
    assert change.changes["desired"]["exists"] is False
    assert change.changes["desired"]["managed_interceptor_count"] == 0
    rendered = repr(change)
    assert "SELECT" not in rendered
    assert "credential-secret" not in rendered
    assert "gateway.example.test" not in rendered
    assert "current=" not in rendered
    assert "changes=" not in rendered


def test_direct_normalized_deletion_computes_and_detaches_evidence() -> None:
    deployer = _deployer()
    current = _desired(deployer)

    change = GatewayRuleChange(
        name="orders",
        action="delete",
        current=current,
        backend_identity=current.binding.backend_identity,
    )
    object.__setattr__(current, "physical_name", "mutated.after.plan")

    assert change.current is not None
    assert change.current.physical_name == "orders.v1"
    assert change.changes is not None
    assert change.changes["categories"] == ["presence"]


def test_normalized_deletion_accepts_only_exact_independent_evidence() -> None:
    deployer = _deployer()
    current = _desired(deployer)
    planned = plan_managed_gateway_rule_deletion(current)
    evidence = deepcopy(planned.changes)
    assert isinstance(evidence, dict)

    change = GatewayRuleChange(
        name="orders",
        action="delete",
        current=current,
        backend_identity=current.binding.backend_identity,
        changes=evidence,
    )
    evidence["categories"].append("configuration")

    assert change.changes == planned.changes


@pytest.mark.parametrize(
    "overrides",
    [
        {"name": "another"},
        {"backend_identity": None},
        {"backend_identity": ("conduktor-gateway:v1:p:sha256:" + "0" * 64)},
        {"desired": _artifact()},
        {"desired_managed": "CURRENT"},
        {"current_alias": MagicMock()},
        {"current_interceptors": []},
        {"changes": {}},
        {"changes": {"raw": "credential-secret"}},
    ],
    ids=[
        "name",
        "missing-backend",
        "wrong-backend",
        "desired-artifact",
        "desired-aggregate",
        "legacy-alias",
        "legacy-interceptors",
        "empty-evidence",
        "raw-evidence",
    ],
)
def test_normalized_deletion_rejects_mismatched_or_legacy_fields(
    overrides: dict[str, object],
) -> None:
    deployer = _deployer()
    current = _desired(deployer)
    values: dict[str, object] = {
        "name": current.logical_name,
        "action": "delete",
        "current": current,
        "backend_identity": current.binding.backend_identity,
    }
    normalized_overrides = dict(overrides)
    if normalized_overrides.get("desired_managed") == "CURRENT":
        normalized_overrides["desired_managed"] = current
    values.update(normalized_overrides)

    with pytest.raises((GatewayManagedMutationError, ValueError)) as caught:
        GatewayRuleChange(**values)  # type: ignore[arg-type]

    assert "credential-secret" not in str(caught.value)
    assert "gateway.example.test" not in str(caught.value)


def test_normalized_deletion_rejects_absent_or_unowned_current_surface() -> None:
    deployer = _deployer()
    absent = _absent(deployer)
    with pytest.raises(ValueError):
        plan_managed_gateway_rule_deletion(absent)

    unrelated = ManagedGatewayInterceptor(
        name="orders_archive_filter_0",
        scope=(("group", None), ("username", None), ("vCluster", "production")),
        plugin_class="example.Plugin",
        priority=100,
        config_json="{}",
    )
    current = _present(deployer, interceptors=(unrelated,))
    with pytest.raises(ValueError):
        plan_managed_gateway_rule_deletion(current)
