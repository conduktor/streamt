"""Strict Kafka Connect runtime binding and observation foundations."""

from __future__ import annotations

import math
from unittest.mock import MagicMock, call, patch

import pytest
from pydantic import SecretStr

from streamt.cli.helpers import make_connect_deployer
from streamt.compiler.manifest import ConnectorArtifact
from streamt.core.runtime import ConnectClusterConfig, ConnectConfig
from streamt.deployer.connect import (
    ConnectClusterBinding,
    ConnectClusterBindingError,
    ConnectDeployer,
    ConnectManagedObservationError,
    ManagedConnectorObservation,
)


def _response(status_code: int, payload: object) -> MagicMock:
    response = MagicMock()
    response.status_code = status_code
    response.json.return_value = payload
    return response


def _bound_deployer(
    endpoint: str = "https://connect.example.test:8443/api/",
    *,
    alias: str = "production",
) -> ConnectDeployer:
    return ConnectDeployer(endpoint, cluster_alias=alias)


def _exact_payload(name: str) -> dict[str, object]:
    return {
        "name": name,
        "config": {
            "name": name,
            "connector.class": "example.SinkConnector",
            "enabled": True,
            "tasks.max": 3,
            "ratio": 0.5,
        },
        "tasks": [{"connector": name, "task": 0}],
        "type": "sink",
    }


def test_cluster_binding_is_stable_normalized_and_endpoint_free() -> None:
    first = ConnectClusterBinding.from_endpoint(
        "production",
        "HTTPS://Connect.Example.Test:443/api/",
    )
    same = ConnectClusterBinding.from_endpoint(
        "production",
        "https://connect.example.test/api",
    )
    drifted_endpoint = ConnectClusterBinding.from_endpoint(
        "production",
        "https://connect.example.test/other",
    )
    drifted_alias = ConnectClusterBinding.from_endpoint(
        "disaster-recovery",
        "https://connect.example.test/api",
    )

    assert first == same
    assert first != drifted_endpoint
    assert first != drifted_alias
    assert first.version == 1
    assert first.endpoint_fingerprint.startswith("sha256:")
    assert first.backend_identity == same.backend_identity
    assert "production" in first.backend_identity
    for rendered in (repr(first), first.backend_identity):
        assert "connect.example.test" not in rendered
        assert "/api" not in rendered


def test_bound_deployer_exposes_only_canonical_backend_identity() -> None:
    deployer = _bound_deployer()

    try:
        identity = deployer.backend_identity
        assert deployer.cluster_binding is not None
        assert identity == deployer.cluster_binding.backend_identity
    finally:
        deployer.close()

    assert identity.startswith("kafka-connect:v1:production:sha256:")
    assert "connect.example.test" not in identity
    assert "/api" not in identity


@pytest.mark.parametrize(
    "endpoint",
    [
        "https://alice:binding-secret@connect.example.test",
        "https://connect.example.test?token=query-secret",
        "https://connect.example.test/#fragment-secret",
        "connect.example.test:8083",
        " https://connect.example.test",
        "https://connect.example.test:invalid-port",
    ],
)
def test_cluster_binding_rejects_unsafe_or_malformed_endpoints_without_echo(
    endpoint: str,
) -> None:
    with pytest.raises(ConnectClusterBindingError) as caught:
        ConnectClusterBinding.from_endpoint("production", endpoint)

    rendered = str(caught.value)
    assert endpoint not in rendered
    for secret in ("binding-secret", "query-secret", "fragment-secret"):
        assert secret not in rendered


@pytest.mark.parametrize("alias", ["", "  ", "bad/alias", "bad alias", "@secret"])
def test_cluster_binding_rejects_malformed_alias_without_echo(alias: str) -> None:
    with pytest.raises(ConnectClusterBindingError) as caught:
        ConnectClusterBinding.from_endpoint(alias, "https://connect.example.test")

    if alias:
        assert alias not in str(caught.value)


def test_helper_supplies_effective_default_connect_alias() -> None:
    project = MagicMock()
    project.runtime.connect = ConnectConfig(
        default="production",
        clusters={
            "production": ConnectClusterConfig(
                rest_url="https://connect.example.test:8443/api",
                username="operator",
                password=SecretStr("runtime-secret"),
            )
        },
    )
    fmt = MagicMock()

    with patch("streamt.deployer.connect.ConnectDeployer") as constructor:
        deployer = make_connect_deployer(project, fmt)

    assert deployer is constructor.return_value
    constructor.assert_called_once_with(
        "https://connect.example.test:8443/api",
        cluster_alias="production",
        username="operator",
        password="runtime-secret",
        ssl_ca_location=None,
        ssl_certificate_location=None,
        ssl_key_location=None,
        ssl_key_password=None,
    )


def test_strict_observation_uses_one_encoded_get_and_exact_config() -> None:
    connector_name = "team/orders sink?#"
    deployer = _bound_deployer()
    response = _response(200, _exact_payload(connector_name))
    deployer._http_session.request = MagicMock(  # type: ignore[method-assign]
        return_value=response
    )

    try:
        observation = deployer.observe_managed_connector(connector_name)
    finally:
        deployer.close()

    deployer._http_session.request.assert_called_once_with(
        "GET",
        "https://connect.example.test:8443/api/connectors/team%2Forders%20sink%3F%23",
        timeout=30,
    )
    assert observation.exists is True
    assert observation.name == connector_name
    assert observation.binding.cluster_alias == "production"
    assert observation.config_dict() == _exact_payload(connector_name)["config"]
    assert observation.fingerprint.startswith("sha256:")


def test_strict_observation_excludes_volatile_fields_from_equality_and_fingerprint() -> None:
    connector_name = "orders-sink"
    first_payload = _exact_payload(connector_name)
    second_payload = {
        **_exact_payload(connector_name),
        "status": {"connector": {"state": "FAILED", "trace": "password=volatile"}},
        "tasks": [{"connector": connector_name, "task": 99, "state": "FAILED"}],
        "type": "source",
    }
    deployer = _bound_deployer()
    deployer._http_session.request = MagicMock(  # type: ignore[method-assign]
        side_effect=[_response(200, first_payload), _response(200, second_payload)]
    )

    try:
        first = deployer.observe_managed_connector(connector_name)
        second = deployer.observe_managed_connector(connector_name)
    finally:
        deployer.close()

    assert first == second
    assert hash(first) == hash(second)
    assert first.fingerprint == second.fingerprint
    assert "volatile" not in repr(first)
    assert "volatile" not in first.fingerprint
    assert deployer._http_session.request.call_count == 2


def test_strict_observation_binding_drift_changes_equality_and_fingerprint() -> None:
    connector_name = "orders-sink"
    primary = _bound_deployer(alias="production")
    secondary = _bound_deployer(alias="disaster-recovery")
    primary._http_session.request = MagicMock(  # type: ignore[method-assign]
        return_value=_response(200, _exact_payload(connector_name))
    )
    secondary._http_session.request = MagicMock(  # type: ignore[method-assign]
        return_value=_response(200, _exact_payload(connector_name))
    )

    try:
        first = primary.observe_managed_connector(connector_name)
        second = secondary.observe_managed_connector(connector_name)
    finally:
        primary.close()
        secondary.close()

    assert first != second
    assert first.fingerprint != second.fingerprint


def test_observation_preserves_exact_floats_in_equality_and_fingerprint() -> None:
    connector_name = "orders-sink"
    deployer = _bound_deployer()
    negative_zero = _exact_payload(connector_name)
    negative_zero["config"] = {
        **negative_zero["config"],  # type: ignore[dict-item]
        "ratio": -0.0,
    }
    positive_zero = _exact_payload(connector_name)
    positive_zero["config"] = {
        **positive_zero["config"],  # type: ignore[dict-item]
        "ratio": 0.0,
    }
    deployer._http_session.request = MagicMock(  # type: ignore[method-assign]
        side_effect=[_response(200, negative_zero), _response(200, positive_zero)]
    )

    try:
        first = deployer.observe_managed_connector(connector_name)
        second = deployer.observe_managed_connector(connector_name)
    finally:
        deployer.close()

    assert first.config_dict()["ratio"] == -0.0
    assert math.copysign(1.0, first.config_dict()["ratio"]) == -1.0  # type: ignore[arg-type]
    assert first != second
    assert first.fingerprint != second.fingerprint


@pytest.mark.parametrize(
    "observation",
    [
        {"exists": True, "config": ()},
        {"exists": True, "config": (("name", "wrong-name"),)},
        {"exists": False, "config": (("name", "orders-sink"),)},
        {
            "exists": True,
            "config": (("name", "orders-sink"), ("password", math.inf)),
        },
    ],
)
def test_direct_observation_construction_enforces_secret_neutral_invariants(
    observation: dict[str, object],
) -> None:
    binding = ConnectClusterBinding.from_endpoint(
        "production",
        "https://connect.example.test",
    )

    with pytest.raises(ConnectManagedObservationError) as caught:
        ManagedConnectorObservation(
            binding=binding,
            name="orders-sink",
            exists=observation["exists"],  # type: ignore[arg-type]
            config=observation["config"],  # type: ignore[arg-type]
        )

    rendered = str(caught.value)
    assert "wrong-name" not in rendered
    assert "orders-sink" not in rendered
    assert "password" not in rendered


def test_strict_observation_404_is_exact_absence_with_one_request() -> None:
    deployer = _bound_deployer()
    response = _response(404, {"message": "password=not-evidence"})
    deployer._http_session.request = MagicMock(  # type: ignore[method-assign]
        return_value=response
    )

    try:
        observation = deployer.observe_managed_connector("missing/connector")
    finally:
        deployer.close()

    assert observation.exists is False
    assert observation.config == ()
    deployer._http_session.request.assert_called_once()
    response.json.assert_not_called()


@pytest.mark.parametrize("status_code", [401, 403])
def test_strict_observation_auth_failure_is_secret_neutral(status_code: int) -> None:
    deployer = ConnectDeployer(
        "https://connect.example.test",
        username="alice",
        password="auth-secret",
        cluster_alias="production",
    )
    response = _response(status_code, {"message": "password=response-secret"})
    deployer._http_session.request = MagicMock(  # type: ignore[method-assign]
        return_value=response
    )

    try:
        with pytest.raises(ConnectManagedObservationError) as caught:
            deployer.observe_managed_connector("orders-sink")
    finally:
        deployer.close()

    rendered = str(caught.value)
    assert "authorization failed" in rendered
    assert "auth-secret" not in rendered
    assert "response-secret" not in rendered
    response.json.assert_not_called()
    deployer._http_session.request.assert_called_once()


def test_strict_observation_request_failure_is_secret_neutral_and_not_retried() -> None:
    deployer = _bound_deployer()
    deployer._http_session.request = MagicMock(  # type: ignore[method-assign]
        side_effect=RuntimeError(
            "password=request-secret https://alice:url-secret@connect.example.test"
        )
    )

    try:
        with pytest.raises(ConnectManagedObservationError) as caught:
            deployer.observe_managed_connector("orders-sink")
    finally:
        deployer.close()

    rendered = str(caught.value)
    assert "request failed" in rendered
    assert "request-secret" not in rendered
    assert "url-secret" not in rendered
    deployer._http_session.request.assert_called_once()


@pytest.mark.parametrize(
    "payload",
    [
        None,
        [],
        {},
        {"name": "wrong-name", "config": {"name": "orders-sink"}},
        {"name": "orders-sink"},
        {"name": "orders-sink", "config": []},
        {"name": "orders-sink", "config": {}},
        {"name": "orders-sink", "config": {"name": "wrong-name"}},
        {"name": "orders-sink", "config": {"name": "orders-sink", " ": "bad"}},
        {"name": "orders-sink", "config": {"name": "orders-sink", "bad": None}},
        {"name": "orders-sink", "config": {"name": "orders-sink", "bad": []}},
        {"name": "orders-sink", "config": {"name": "orders-sink", "bad": {}}},
        {"name": "orders-sink", "config": {"name": "orders-sink", "bad": math.nan}},
        {"name": "orders-sink", "config": {"name": "orders-sink", "bad": math.inf}},
    ],
)
def test_strict_observation_rejects_partial_or_malformed_payloads_without_values(
    payload: object,
) -> None:
    deployer = _bound_deployer()
    deployer._http_session.request = MagicMock(  # type: ignore[method-assign]
        return_value=_response(200, payload)
    )

    try:
        with pytest.raises(ConnectManagedObservationError) as caught:
            deployer.observe_managed_connector("orders-sink")
    finally:
        deployer.close()

    rendered = str(caught.value)
    assert "wrong-name" not in rendered
    assert "orders-sink" not in rendered
    deployer._http_session.request.assert_called_once()


def test_strict_observation_rejects_invalid_json_without_response_text() -> None:
    deployer = _bound_deployer()
    response = _response(200, {})
    response.json.side_effect = ValueError("password=json-secret")
    deployer._http_session.request = MagicMock(  # type: ignore[method-assign]
        return_value=response
    )

    try:
        with pytest.raises(ConnectManagedObservationError) as caught:
            deployer.observe_managed_connector("orders-sink")
    finally:
        deployer.close()

    assert "json-secret" not in str(caught.value)


def test_legacy_unbound_crud_works_but_strict_observation_requires_binding() -> None:
    deployer = ConnectDeployer("http://connect.example.test")
    deployer._http_session.request = MagicMock()  # type: ignore[method-assign]

    try:
        with pytest.raises(ConnectClusterBindingError, match=r"requires.*binding"):
            _ = deployer.backend_identity
        with pytest.raises(ConnectClusterBindingError, match=r"requires.*binding"):
            deployer.observe_managed_connector("orders-sink")
        deployer.delete_connector("team/orders sink")
    finally:
        deployer.close()

    deployer._http_session.request.assert_called_once_with(
        "DELETE",
        "http://connect.example.test/connectors/team%2Forders%20sink",
        timeout=30,
    )


def test_all_legacy_resource_paths_percent_encode_connector_name() -> None:
    connector_name = "team/orders sink?#"
    encoded = "team%2Forders%20sink%3F%23"
    artifact = ConnectorArtifact(
        name=connector_name,
        connector_class="example.SinkConnector",
        topics=["orders"],
    )
    deployer = ConnectDeployer("http://connect.example.test")
    request = MagicMock(
        side_effect=[
            {"name": connector_name},
            {"connector": {"state": "RUNNING"}, "tasks": []},
            {},
            None,
            None,
            None,
            None,
        ]
    )
    deployer._request = request  # type: ignore[method-assign]

    try:
        deployer.get_connector_state(connector_name)
        deployer.update_connector(artifact)
        deployer.delete_connector(connector_name)
        deployer.restart_connector(connector_name)
        deployer.pause_connector(connector_name)
        deployer.resume_connector(connector_name)
    finally:
        deployer.close()

    assert request.call_args_list == [
        call("GET", f"/connectors/{encoded}/config"),
        call("GET", f"/connectors/{encoded}/status"),
        call("PUT", f"/connectors/{encoded}/config", json=artifact.to_dict()["config"]),
        call("DELETE", f"/connectors/{encoded}"),
        call("POST", f"/connectors/{encoded}/restart"),
        call("PUT", f"/connectors/{encoded}/pause"),
        call("PUT", f"/connectors/{encoded}/resume"),
    ]
