"""Strict Conduktor Gateway runtime binding and observation foundations."""

from __future__ import annotations

import json
from typing import cast
from unittest.mock import MagicMock, call

import pytest

from streamt.deployer.gateway import (
    GatewayBackendBinding,
    GatewayBindingError,
    GatewayDeployer,
    GatewayManagedObservationError,
    ManagedGatewayInterceptor,
    ManagedGatewayRuleObservation,
    is_gateway_backend_identity,
)


def _raw_response(
    status_code: int,
    body: bytes,
    *,
    headers: dict[str, str] | None = None,
) -> MagicMock:
    response = MagicMock()
    response.status_code = status_code
    response.headers = headers or {}
    response.iter_content.return_value = [body]
    return response


def _response(status_code: int, payload: object) -> MagicMock:
    return _raw_response(
        status_code,
        json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8"),
    )


def _alias(
    name: str = "orders.public",
    *,
    physical_name: str = "orders.v1",
    virtual_cluster: str | None = None,
    include_physical_cluster: bool = True,
) -> dict[str, object]:
    metadata: dict[str, object] = {"name": name}
    if virtual_cluster is not None:
        metadata["vCluster"] = virtual_cluster
    spec: dict[str, object] = {"physicalName": physical_name}
    if include_physical_cluster:
        spec["physicalCluster"] = "main"
    return {
        "kind": "AliasTopic",
        "apiVersion": "gateway/v2",
        "metadata": metadata,
        "spec": spec,
    }


def _interceptor(
    name: str,
    *,
    virtual_cluster: str | None = None,
    group: str | None = None,
    username: str | None = None,
    include_scope: bool | None = None,
    provider_filled_scope: bool = False,
    config: dict[str, object] | None = None,
) -> dict[str, object]:
    metadata: dict[str, object] = {"name": name}
    scope: dict[str, object] = (
        {"group": None, "username": None, "vCluster": None} if provider_filled_scope else {}
    )
    if virtual_cluster is not None:
        scope["vCluster"] = virtual_cluster
    if group is not None:
        scope["group"] = group
    if username is not None:
        scope["username"] = username
    if include_scope is True or (include_scope is not False and scope):
        metadata["scope"] = scope
    return {
        "kind": "Interceptor",
        "apiVersion": "gateway/v2",
        "metadata": metadata,
        "spec": {
            "pluginClass": "example.FilterPlugin",
            "priority": 100,
            "config": config or {"enabled": True},
            "comment": "provider comment",
        },
    }


def _deployer(
    *,
    virtual_cluster: str | None = "production",
) -> GatewayDeployer:
    return GatewayDeployer(
        "HTTPS://Gateway.Example.Test:443/admin/",
        virtual_cluster=virtual_cluster,
    )


def _set_collections(
    deployer: GatewayDeployer,
    aliases: object,
    interceptors: object,
) -> tuple[MagicMock, MagicMock]:
    alias_response = _response(200, aliases)
    interceptor_response = _response(200, interceptors)
    deployer._session.request = MagicMock(  # type: ignore[method-assign]
        side_effect=[alias_response, interceptor_response]
    )
    return alias_response, interceptor_response


def test_gateway_binding_is_normalized_scoped_endpoint_free_and_versioned() -> None:
    first = GatewayBackendBinding.from_endpoint(
        "HTTPS://Gateway.Example.Test:443/admin/",
        virtual_cluster="production",
    )
    same = GatewayBackendBinding.from_endpoint(
        "https://gateway.example.test/admin",
        virtual_cluster="production",
    )
    drifted_scope = GatewayBackendBinding.from_endpoint(
        "https://gateway.example.test/admin",
        virtual_cluster="disaster-recovery",
    )
    passthrough = GatewayBackendBinding.from_endpoint(
        "https://gateway.example.test/admin",
    )
    explicit_passthrough = GatewayBackendBinding.from_endpoint(
        "https://gateway.example.test/admin",
        virtual_cluster="passthrough",
    )

    assert first == same
    assert first != drifted_scope
    assert first != passthrough
    assert passthrough == explicit_passthrough
    assert passthrough.backend_identity == explicit_passthrough.backend_identity
    assert passthrough.virtual_cluster == "passthrough"
    assert first.scope_name == "production"
    assert passthrough.scope_name == "passthrough"
    assert is_gateway_backend_identity(first.backend_identity)
    assert "gateway.example.test" not in first.backend_identity
    assert "production" not in first.backend_identity


@pytest.mark.parametrize(
    "endpoint",
    [
        "gateway.example.test",
        "https://alice:secret@gateway.example.test",
        "https://gateway.example.test?token=secret",
        "https://gateway.example.test#secret",
        " https://gateway.example.test",
        "https://gateway.example.test/\x00hidden",
        "https://gateway.example.test/\x01hidden",
        "https://gateway.example.test/\x7fhidden",
        "https://gateway.example.test/\x80hidden",
        "https://gateway.example.test/\x9fhidden",
    ],
)
def test_gateway_binding_rejects_unsafe_endpoints_without_echo(endpoint: str) -> None:
    with pytest.raises(GatewayBindingError) as caught:
        GatewayBackendBinding.from_endpoint(endpoint)

    rendered = str(caught.value)
    assert endpoint not in rendered
    assert "secret" not in rendered


@pytest.mark.parametrize(
    "scope",
    ["", " production", "production ", "bad\nname", "bad/name", "bad name"],
)
def test_gateway_binding_rejects_invalid_scope(scope: str) -> None:
    with pytest.raises(GatewayBindingError, match="virtual cluster"):
        GatewayBackendBinding.from_endpoint(
            "https://gateway.example.test",
            virtual_cluster=scope,
        )


def test_gateway_binding_rejects_non_v2_api() -> None:
    with pytest.raises(GatewayBindingError, match="API version"):
        GatewayBackendBinding.from_endpoint(
            "https://gateway.example.test",
            api_version="v1",
        )


@pytest.mark.parametrize(
    "scope_token",
    [
        "v-_w",  # invalid UTF-8
        "v-Z",  # invalid base64 length
        "v-cGFzc3Rocm91Z2g",  # explicit encoding of canonical `p`
        "v-YmFkCm5hbWU",  # decoded control character
    ],
)
def test_gateway_backend_identity_rejects_noncanonical_scope_tokens(
    scope_token: str,
) -> None:
    binding = GatewayBackendBinding.from_endpoint("https://gateway.example.test")
    candidate = f"conduktor-gateway:v1:{scope_token}:{binding.endpoint_fingerprint}"

    assert is_gateway_backend_identity(candidate) is False


def test_observer_uses_exact_two_fixed_gets_and_normalizes_provider_order() -> None:
    deployer = _deployer()
    aliases = [
        _alias("orders.public", virtual_cluster="other"),
        _alias("orders.public", virtual_cluster="production"),
    ]
    interceptors = [
        _interceptor("orders_mask_1", virtual_cluster="production", config={"z": 1}),
        _interceptor("orders_filter_0", virtual_cluster="other", config={"ignored": True}),
        _interceptor("orders_filter_0", virtual_cluster="production", config={"a": 2}),
        _interceptor(
            "orders_filter_0",
            virtual_cluster="production",
            group="unrelated",
            config={"ignored": "group"},
        ),
        _interceptor("orders_filter_0_extra", virtual_cluster="production"),
        _interceptor("unrelated_filter_0", virtual_cluster="production"),
    ]
    alias_response, interceptor_response = _set_collections(deployer, aliases, interceptors)

    try:
        observation = deployer.observe_managed_gateway_rule("orders", "orders.public")
    finally:
        deployer.close()

    assert isinstance(observation, ManagedGatewayRuleObservation)
    assert observation.exists is True
    assert observation.physical_name == "orders.v1"
    assert observation.physical_cluster == "main"
    assert [item.name for item in observation.interceptors] == [
        "orders_filter_0",
        "orders_mask_1",
    ]
    assert observation.interceptors[0].scope == (
        ("group", None),
        ("username", None),
        ("vCluster", "production"),
    )
    assert observation.fingerprint.startswith("sha256:")
    assert "ignored" not in repr(observation)
    assert "group" not in repr(observation)
    assert '{"a":2}' not in repr(observation.interceptors[0])
    assert deployer._session.request.call_args_list == [  # type: ignore[attr-defined]
        call(
            "GET",
            "https://gateway.example.test/admin/gateway/v2/alias-topic",
            timeout=10,
            allow_redirects=False,
            stream=True,
        ),
        call(
            "GET",
            "https://gateway.example.test/admin/gateway/v2/interceptor",
            timeout=10,
            allow_redirects=False,
            stream=True,
        ),
    ]
    alias_response.close.assert_called_once_with()
    interceptor_response.close.assert_called_once_with()


def test_observer_fingerprint_is_independent_of_provider_list_order() -> None:
    aliases = [_alias(virtual_cluster="production")]
    interceptors = [
        _interceptor("orders_filter_0", virtual_cluster="production", config={"a": 1}),
        _interceptor("orders_mask_1", virtual_cluster="production", config={"b": 2}),
    ]
    first = _deployer()
    second = _deployer()
    _set_collections(first, aliases, interceptors)
    _set_collections(second, list(reversed(aliases)), list(reversed(interceptors)))

    try:
        first_observation = first.observe_managed_gateway_rule("orders", "orders.public")
        second_observation = second.observe_managed_gateway_rule("orders", "orders.public")
    finally:
        first.close()
        second.close()

    assert first_observation == second_observation
    assert first_observation.fingerprint == second_observation.fingerprint


def test_generated_name_classification_is_anchored_to_the_exact_owner() -> None:
    deployer = _deployer()
    _set_collections(
        deployer,
        [_alias(virtual_cluster="production")],
        [
            _interceptor("orders_filter_0", virtual_cluster="production"),
            _interceptor("orders_archive_filter_0", virtual_cluster="production"),
            _interceptor("provider interceptor name", virtual_cluster="production"),
        ],
    )

    try:
        observation = deployer.observe_managed_gateway_rule("orders", "orders.public")
    finally:
        deployer.close()

    assert [item.name for item in observation.interceptors] == ["orders_filter_0"]


@pytest.mark.parametrize("name", ["orders_unknown_0", "orders_filter_00"])
def test_exact_owner_with_unknown_or_noncanonical_generated_suffix_is_ambiguous(
    name: str,
) -> None:
    deployer = _deployer()
    _set_collections(
        deployer,
        [_alias(virtual_cluster="production")],
        [_interceptor(name, virtual_cluster="production")],
    )

    try:
        with pytest.raises(GatewayManagedObservationError, match="ownership is ambiguous"):
            deployer.observe_managed_gateway_rule("orders", "orders.public")
    finally:
        deployer.close()


def test_omitted_and_explicit_passthrough_observers_are_canonical() -> None:
    omitted = _deployer(virtual_cluster=None)
    explicit = _deployer(virtual_cluster="passthrough")
    _set_collections(
        omitted,
        [_alias(include_physical_cluster=False)],
        [
            _interceptor(
                "orders_filter_0",
                virtual_cluster="passthrough",
                provider_filled_scope=True,
            )
        ],
    )
    _set_collections(
        explicit,
        [_alias(virtual_cluster="passthrough", include_physical_cluster=False)],
        [_interceptor("orders_filter_0", virtual_cluster="passthrough")],
    )

    try:
        omitted_observation = omitted.observe_managed_gateway_rule("orders", "orders.public")
        explicit_observation = explicit.observe_managed_gateway_rule("orders", "orders.public")
    finally:
        omitted.close()
        explicit.close()

    assert omitted_observation == explicit_observation
    assert omitted_observation.binding == explicit_observation.binding
    assert (
        omitted_observation.binding.backend_identity
        == explicit_observation.binding.backend_identity
    )
    assert omitted_observation.binding.scope_name == "passthrough"
    assert omitted_observation.physical_cluster == "main"
    assert omitted_observation.interceptors[0].scope == (
        ("group", None),
        ("username", None),
        ("vCluster", "passthrough"),
    )


def test_same_names_in_other_scopes_are_distinct_and_can_leave_target_absent() -> None:
    deployer = _deployer()
    _set_collections(
        deployer,
        [_alias(virtual_cluster="other")],
        [_interceptor("orders_filter_0", virtual_cluster="other")],
    )

    try:
        observation = deployer.observe_managed_gateway_rule("orders", "orders.public")
    finally:
        deployer.close()

    assert observation.exists is False
    assert observation.interceptors == ()


def test_absent_and_present_all_null_global_scopes_are_distinct() -> None:
    deployer = _deployer()
    _set_collections(
        deployer,
        [_alias(virtual_cluster="production")],
        [
            _interceptor("orders_filter_0", include_scope=False),
            _interceptor(
                "orders_filter_0",
                include_scope=True,
                provider_filled_scope=True,
            ),
        ],
    )

    try:
        observation = deployer.observe_managed_gateway_rule("orders", "orders.public")
    finally:
        deployer.close()

    assert observation.exists is True
    assert observation.interceptors == ()


def test_real_provider_username_scope_shape_is_valid_and_not_target_owned() -> None:
    deployer = _deployer(virtual_cluster=None)
    _set_collections(
        deployer,
        [_alias()],
        [
            {
                **_interceptor("orders_filter_0"),
                "metadata": {
                    "name": "orders_filter_0",
                    "scope": {
                        "vCluster": "passthrough",
                        "group": None,
                        "username": "streamt-probe-user",
                    },
                },
            }
        ],
    )

    try:
        observation = deployer.observe_managed_gateway_rule("orders", "orders.public")
    finally:
        deployer.close()

    assert observation.exists is True
    assert observation.interceptors == ()


def test_default_principal_scope_duplicates_provider_null_filled_shape() -> None:
    deployer = _deployer(virtual_cluster=None)
    abbreviated = _interceptor("provider-name", username="streamt-probe-user")
    provider_filled = {
        **_interceptor("provider-name"),
        "metadata": {
            "name": "provider-name",
            "scope": {
                "vCluster": "passthrough",
                "group": None,
                "username": "streamt-probe-user",
            },
        },
    }
    _set_collections(deployer, [_alias()], [abbreviated, provider_filled])

    try:
        with pytest.raises(GatewayManagedObservationError, match="duplicate scoped identity"):
            deployer.observe_managed_gateway_rule("orders", "orders.public")
    finally:
        deployer.close()


@pytest.mark.parametrize(
    "scope",
    [
        {"vCluster": None, "username": "streamt-probe-user"},
        {"vCluster": "production", "group": "group-a", "username": "user-a"},
        {"group": None},
    ],
)
def test_undocumented_interceptor_scope_combinations_fail_closed(
    scope: dict[str, object],
) -> None:
    deployer = _deployer()
    interceptor = _interceptor("unrelated-name")
    interceptor["metadata"] = {"name": "unrelated-name", "scope": scope}
    _set_collections(
        deployer,
        [_alias(virtual_cluster="production")],
        [interceptor],
    )

    try:
        with pytest.raises(GatewayManagedObservationError, match="invalid scope"):
            deployer.observe_managed_gateway_rule("orders", "orders.public")
    finally:
        deployer.close()


@pytest.mark.parametrize("kind", ["alias", "interceptor"])
def test_duplicate_exact_scoped_identity_fails_closed(kind: str) -> None:
    deployer = _deployer()
    aliases = [_alias(virtual_cluster="production")]
    interceptors: list[dict[str, object]] = []
    if kind == "alias":
        aliases.append(_alias(virtual_cluster="production"))
    else:
        interceptors = [
            _interceptor("orders_filter_0", virtual_cluster="production"),
            _interceptor(
                "orders_filter_0",
                virtual_cluster="production",
                provider_filled_scope=True,
            ),
        ]
    _set_collections(deployer, aliases, interceptors)

    try:
        with pytest.raises(GatewayManagedObservationError, match="duplicate scoped identity"):
            deployer.observe_managed_gateway_rule("orders", "orders.public")
    finally:
        deployer.close()


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("name", "bad/name"),
        ("physicalName", "bad physical"),
        ("vCluster", "bad/scope"),
    ],
)
def test_invalid_alias_identity_in_any_scope_fails_closed(
    field: str,
    value: str,
) -> None:
    deployer = _deployer()
    alias = _alias(name="unrelated", physical_name="unrelated", virtual_cluster="other")
    if field == "physicalName":
        cast(dict[str, object], alias["spec"])[field] = value
    else:
        cast(dict[str, object], alias["metadata"])[field] = value
    _set_collections(deployer, [alias], [])

    try:
        with pytest.raises(GatewayManagedObservationError):
            deployer.observe_managed_gateway_rule("orders", "orders.public")
    finally:
        deployer.close()


def test_interceptor_name_rejects_controls_but_not_unrelated_provider_syntax() -> None:
    accepted = _deployer()
    rejected = _deployer()
    _set_collections(
        accepted,
        [_alias(virtual_cluster="production")],
        [_interceptor("provider name / unconstrained", virtual_cluster="production")],
    )
    _set_collections(
        rejected,
        [_alias(virtual_cluster="production")],
        [_interceptor("provider\x80name", virtual_cluster="other")],
    )

    try:
        observation = accepted.observe_managed_gateway_rule("orders", "orders.public")
        with pytest.raises(GatewayManagedObservationError, match="malformed"):
            rejected.observe_managed_gateway_rule("orders", "orders.public")
    finally:
        accepted.close()
        rejected.close()

    assert observation.interceptors == ()


def test_managed_observation_constructor_rejects_non_interceptor_items_stably() -> None:
    binding = GatewayBackendBinding.from_endpoint(
        "https://gateway.example.test",
        virtual_cluster="production",
    )

    with pytest.raises(
        GatewayManagedObservationError,
        match="invalid interceptor surface",
    ):
        ManagedGatewayRuleObservation(
            binding=binding,
            logical_name="orders",
            alias_name="orders.public",
            exists=False,
            interceptors=cast(tuple[ManagedGatewayInterceptor, ...], (object(),)),
        )


def test_managed_observation_constructor_rejects_mismatched_interceptor_scope() -> None:
    binding = GatewayBackendBinding.from_endpoint(
        "https://gateway.example.test",
        virtual_cluster="production",
    )
    interceptor = ManagedGatewayInterceptor(
        name="orders_filter_0",
        scope=(("group", None), ("username", None), ("vCluster", "other")),
        plugin_class="example.FilterPlugin",
        priority=100,
        config_json="{}",
    )

    with pytest.raises(GatewayManagedObservationError, match="mismatched interceptor scope"):
        ManagedGatewayRuleObservation(
            binding=binding,
            logical_name="orders",
            alias_name="orders.public",
            exists=True,
            physical_name="orders.v1",
            physical_cluster="main",
            interceptors=(interceptor,),
        )


def test_absent_alias_with_exact_scope_owned_interceptors_is_partial() -> None:
    deployer = _deployer()
    _set_collections(
        deployer,
        [],
        [_interceptor("orders_filter_0", virtual_cluster="production")],
    )

    try:
        with pytest.raises(GatewayManagedObservationError, match="partial"):
            deployer.observe_managed_gateway_rule("orders", "orders.public")
    finally:
        deployer.close()


@pytest.mark.parametrize(
    ("aliases", "interceptors"),
    [
        ({}, []),
        ([{"kind": "AliasTopic"}], []),
        ([{**_alias(virtual_cluster="production"), "unexpected": True}], []),
        (
            [
                {
                    **_alias(virtual_cluster="production"),
                    "metadata": {"name": "orders.public", "vCluster": None},
                }
            ],
            [],
        ),
        (
            [
                {
                    **_alias(virtual_cluster="production"),
                    "spec": {
                        "physicalName": "orders.v1",
                        "physicalCluster": "secondary",
                    },
                }
            ],
            [],
        ),
        (
            [_alias(virtual_cluster="production")],
            [
                {
                    **_interceptor("orders_filter_0", virtual_cluster="production"),
                    "spec": {
                        "pluginClass": "example.FilterPlugin",
                        "priority": True,
                        "config": {},
                    },
                }
            ],
        ),
        (
            [_alias(virtual_cluster="production")],
            [
                {
                    **_interceptor("orders_filter_0", virtual_cluster="production"),
                    "metadata": {
                        "name": "orders_filter_0",
                        "scope": {},
                    },
                }
            ],
        ),
    ],
)
def test_partial_or_malformed_collection_content_fails_closed(
    aliases: object,
    interceptors: object,
) -> None:
    deployer = _deployer()
    _set_collections(deployer, aliases, interceptors)

    try:
        with pytest.raises(GatewayManagedObservationError):
            deployer.observe_managed_gateway_rule("orders", "orders.public")
    finally:
        deployer.close()


def test_redirect_is_not_followed_or_read_and_only_one_request_occurs() -> None:
    deployer = _deployer()
    response = _raw_response(
        302,
        b'{"password":"redirect-body-secret"}',
        headers={"Location": "https://alice:redirect-secret@other.example.test"},
    )
    deployer._session.request = MagicMock(return_value=response)  # type: ignore[method-assign]

    try:
        with pytest.raises(GatewayManagedObservationError) as caught:
            deployer.observe_managed_gateway_rule("orders", "orders.public")
    finally:
        deployer.close()

    rendered = str(caught.value)
    assert "redirect-secret" not in rendered
    assert "redirect-body-secret" not in rendered
    deployer._session.request.assert_called_once()
    response.iter_content.assert_not_called()
    response.close.assert_called_once_with()


def test_oversized_body_is_rejected_before_decode_without_values() -> None:
    deployer = _deployer()
    response = _raw_response(
        200,
        b"oversized-secret",
        headers={"Content-Length": str(1024 * 1024 + 1)},
    )
    deployer._session.request = MagicMock(return_value=response)  # type: ignore[method-assign]

    try:
        with pytest.raises(GatewayManagedObservationError, match="oversized") as caught:
            deployer.observe_managed_gateway_rule("orders", "orders.public")
    finally:
        deployer.close()

    assert "oversized-secret" not in str(caught.value)
    response.iter_content.assert_not_called()


@pytest.mark.parametrize(
    "body",
    [
        b'[{"kind":"AliasTopic","kind":"password=duplicate-secret"}]',
        b"[NaN]",
        b"\xff",
    ],
)
def test_noncanonical_json_is_rejected_without_response_values(body: bytes) -> None:
    deployer = _deployer()
    response = _raw_response(200, body)
    deployer._session.request = MagicMock(return_value=response)  # type: ignore[method-assign]

    try:
        with pytest.raises(GatewayManagedObservationError, match="canonical JSON") as caught:
            deployer.observe_managed_gateway_rule("orders", "orders.public")
    finally:
        deployer.close()

    assert "duplicate-secret" not in str(caught.value)
    deployer._session.request.assert_called_once()


@pytest.mark.parametrize("status_code", [401, 403, 404, 500])
def test_invalid_status_is_secret_neutral_and_not_retried(status_code: int) -> None:
    deployer = _deployer()
    response = _raw_response(status_code, b'{"password":"response-secret"}')
    deployer._session.request = MagicMock(return_value=response)  # type: ignore[method-assign]

    try:
        with pytest.raises(GatewayManagedObservationError) as caught:
            deployer.observe_managed_gateway_rule("orders", "orders.public")
    finally:
        deployer.close()

    assert "response-secret" not in str(caught.value)
    deployer._session.request.assert_called_once()
    response.iter_content.assert_not_called()


def test_request_failure_is_secret_neutral_and_not_retried() -> None:
    deployer = _deployer()
    deployer._session.request = MagicMock(  # type: ignore[method-assign]
        side_effect=RuntimeError(
            "password=request-secret https://alice:url-secret@gateway.example.test"
        )
    )

    try:
        with pytest.raises(GatewayManagedObservationError, match="request failed") as caught:
            deployer.observe_managed_gateway_rule("orders", "orders.public")
    finally:
        deployer.close()

    rendered = str(caught.value)
    assert "request-secret" not in rendered
    assert "url-secret" not in rendered
    deployer._session.request.assert_called_once()


def test_invalid_target_and_closed_deployer_make_no_requests() -> None:
    deployer = _deployer()
    request = MagicMock()
    deployer._session.request = request  # type: ignore[method-assign]

    with pytest.raises(GatewayManagedObservationError, match="valid rule identities"):
        deployer.observe_managed_gateway_rule("bad/name", "orders.public")
    deployer.close()
    with pytest.raises(GatewayManagedObservationError, match="closed"):
        deployer.observe_managed_gateway_rule("orders", "orders.public")

    request.assert_not_called()
