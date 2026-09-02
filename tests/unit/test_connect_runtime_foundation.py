"""Strict Kafka Connect runtime binding and observation foundations."""

from __future__ import annotations

import json
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
    ConnectorChange,
    ConnectorState,
    ManagedConnectorObservation,
    bind_connector_artifact,
    is_connect_backend_identity,
    resolve_connector_artifact,
    secret_neutral_connector_changes,
    secret_neutral_connector_config_diff,
)


def _response(status_code: int, payload: object) -> MagicMock:
    return _raw_response(
        status_code,
        json.dumps(payload, separators=(",", ":")).encode("utf-8"),
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
            "topics": "orders",
            "enabled": True,
            "tasks.max": 3,
            "ratio": 0.5,
        },
        "tasks": [{"connector": name, "task": 0}],
        "type": "sink",
    }


def _artifact(
    *,
    cluster: str | None = None,
    config: dict[str, object] | None = None,
) -> ConnectorArtifact:
    return ConnectorArtifact(
        name="orders-sink",
        connector_class="example.SinkConnector",
        topics=["orders"],
        cluster=cluster,
        config=config
        or {
            "enabled": True,
            "tasks.max": 3,
            "ratio": 0.5,
        },
    )


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
    assert is_connect_backend_identity(identity)


@pytest.mark.parametrize(
    "value",
    [
        None,
        "",
        "kafka-connect:v2:production:sha256:" + "a" * 64,
        "kafka-connect:v1:bad/alias:sha256:" + "a" * 64,
        "kafka-connect:v1:production:sha256:secret",
    ],
)
def test_backend_identity_validator_is_exact(value: object) -> None:
    assert not is_connect_backend_identity(value)


def test_connector_change_rejects_noncanonical_backend_identity_without_echo() -> None:
    invalid = "https://alice:identity-secret@connect.example.test"

    with pytest.raises(ConnectClusterBindingError) as caught:
        ConnectorChange(
            connector_name="orders-sink",
            action="none",
            backend_identity=invalid,
        )

    assert invalid not in str(caught.value)
    assert "identity-secret" not in str(caught.value)


def test_pure_and_bound_artifact_resolvers_fill_or_preserve_exact_alias() -> None:
    binding = ConnectClusterBinding.from_endpoint(
        "production",
        "https://connect.example.test",
    )
    unresolved = _artifact()
    explicit = _artifact(cluster="production")
    deployer = _bound_deployer()

    try:
        resolved_default = bind_connector_artifact(unresolved, binding)
        resolved_alias = resolve_connector_artifact(explicit, binding)
        resolved_by_deployer = deployer.resolve_connector_artifact(unresolved)
    finally:
        deployer.close()

    for resolved in (resolved_default, resolved_alias, resolved_by_deployer):
        assert resolved.cluster == "production"
        assert resolved is not unresolved
    assert unresolved.cluster is None
    assert resolved_default.topics is not unresolved.topics
    assert resolved_default.config is not unresolved.config


def test_artifact_resolver_rejects_unbound_or_nonmatching_alias_without_secrets() -> None:
    explicit_cluster = "https://alice:cluster-secret@connect.example.test"
    artifact = _artifact(
        cluster=explicit_cluster,
        config={"password": "config-secret"},
    )
    bound = _bound_deployer()
    unbound = ConnectDeployer("https://connect.example.test")

    try:
        with pytest.raises(ConnectClusterBindingError) as mismatch:
            bound.resolve_connector_artifact(artifact)
        with pytest.raises(ConnectClusterBindingError) as missing:
            unbound.resolve_connector_artifact(artifact)
    finally:
        bound.close()
        unbound.close()

    for rendered in (str(mismatch.value), str(missing.value)):
        assert explicit_cluster not in rendered
        assert "cluster-secret" not in rendered
        assert "config-secret" not in rendered


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
        allow_redirects=False,
        stream=True,
    )
    response.iter_content.assert_called_once_with(chunk_size=64 * 1024)
    response.close.assert_called_once_with()
    assert observation.exists is True
    assert observation.name == connector_name
    assert observation.binding.cluster_alias == "production"
    assert observation.config_dict() == _exact_payload(connector_name)["config"]
    assert observation.fingerprint.startswith("sha256:")


def test_bound_plan_uses_one_strict_get_and_returns_exact_noop_evidence() -> None:
    artifact = _artifact()
    deployer = _bound_deployer()
    request = MagicMock(return_value=_response(200, _exact_payload(artifact.name)))
    deployer._http_session.request = request  # type: ignore[method-assign]

    try:
        change = deployer.plan_connector(artifact)
    finally:
        deployer.close()

    assert change.action == "none"
    assert isinstance(change.current, ManagedConnectorObservation)
    assert change.current.config_dict() == artifact.to_dict()["config"]
    assert change.desired is not None
    assert change.desired.cluster == "production"
    assert change.backend_identity == change.current.binding.backend_identity
    assert is_connect_backend_identity(change.backend_identity)
    request.assert_called_once()


def test_bound_plan_absence_returns_create_with_resolved_authority() -> None:
    artifact = _artifact()
    deployer = _bound_deployer()
    response = _response(404, {"password": "absence-response-secret"})
    request = MagicMock(return_value=response)
    deployer._http_session.request = request  # type: ignore[method-assign]

    try:
        change = deployer.plan_connector(artifact)
    finally:
        deployer.close()

    assert change.action == "create"
    assert isinstance(change.current, ManagedConnectorObservation)
    assert change.current.exists is False
    assert change.desired is not None
    assert change.desired.cluster == "production"
    assert change.backend_identity == change.current.binding.backend_identity
    request.assert_called_once()
    response.json.assert_not_called()


def test_bound_plan_exactly_detects_type_and_case_changes_without_raw_values() -> None:
    artifact = _artifact(config={"mode": "copy", "tasks.max": True})
    desired_config = artifact.to_dict()["config"]
    assert isinstance(desired_config, dict)
    current_config = dict(desired_config)
    current_config["mode"] = "COPY"
    current_config["tasks.max"] = 1
    deployer = _bound_deployer()
    request = MagicMock(
        return_value=_response(
            200,
            {
                "name": artifact.name,
                "config": current_config,
                "status": {"state": "RUNNING"},
            },
        )
    )
    deployer._http_session.request = request  # type: ignore[method-assign]

    try:
        change = deployer.plan_connector(artifact)
    finally:
        deployer.close()

    assert change.action == "update"
    assert isinstance(change.current, ManagedConnectorObservation)
    assert change.desired is not None
    assert change.desired.cluster == "production"
    assert change.backend_identity == change.current.binding.backend_identity
    assert set(change.changes) == {"mode", "tasks.max"}
    assert change.changes["mode"] == {
        "change": "changed",
        "from_present": True,
        "to_present": True,
    }
    assert change.changes["tasks.max"] == {
        "change": "changed",
        "from_present": True,
        "to_present": True,
    }
    rendered = json.dumps(change.changes, sort_keys=True)
    assert "COPY" not in rendered
    assert "copy" not in rendered
    request.assert_called_once()


def test_bound_plan_malformed_config_error_contains_no_raw_config() -> None:
    artifact = _artifact(config={"password": "desired-secret"})
    deployer = _bound_deployer()
    request = MagicMock(
        return_value=_response(
            200,
            {
                "name": artifact.name,
                "config": {
                    "name": artifact.name,
                    "password": {"nested": "current-secret"},
                },
            },
        )
    )
    deployer._http_session.request = request  # type: ignore[method-assign]

    try:
        with pytest.raises(ConnectManagedObservationError) as caught:
            deployer.plan_connector(artifact)
    finally:
        deployer.close()

    rendered = str(caught.value)
    assert "desired-secret" not in rendered
    assert "current-secret" not in rendered
    request.assert_called_once()


def test_unbound_plan_retains_legacy_state_and_no_backend_identity() -> None:
    artifact = _artifact()
    deployer = ConnectDeployer("https://connect.example.test")
    deployer.get_connector_state = MagicMock(  # type: ignore[method-assign]
        return_value=ConnectorState(name=artifact.name, exists=False)
    )

    try:
        change = deployer.plan_connector(artifact)
    finally:
        deployer.close()

    assert change.action == "create"
    assert isinstance(change.current, ConnectorState)
    assert change.desired is artifact
    assert change.backend_identity is None


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
    response = _raw_response(
        200,
        b'{"name":"orders-sink","password":"json-secret"',
    )
    deployer._http_session.request = MagicMock(  # type: ignore[method-assign]
        return_value=response
    )

    try:
        with pytest.raises(ConnectManagedObservationError) as caught:
            deployer.observe_managed_connector("orders-sink")
    finally:
        deployer.close()

    assert "json-secret" not in str(caught.value)
    response.json.assert_not_called()


def test_strict_observation_rejects_redirect_without_following_or_reading_body() -> None:
    deployer = _bound_deployer()
    response = _raw_response(
        302,
        b'{"password":"redirect-body-secret"}',
        headers={
            "Location": "https://alice:redirect-secret@other.example.test/connectors/orders-sink"
        },
    )
    request = MagicMock(return_value=response)
    deployer._http_session.request = request  # type: ignore[method-assign]

    try:
        with pytest.raises(ConnectManagedObservationError) as caught:
            deployer.observe_managed_connector("orders-sink")
    finally:
        deployer.close()

    rendered = str(caught.value)
    assert "redirect-secret" not in rendered
    assert "redirect-body-secret" not in rendered
    request.assert_called_once_with(
        "GET",
        "https://connect.example.test:8443/api/connectors/orders-sink",
        timeout=30,
        allow_redirects=False,
        stream=True,
    )
    response.iter_content.assert_not_called()
    response.json.assert_not_called()
    response.close.assert_called_once_with()


def test_strict_observation_rejects_duplicate_json_keys_without_values() -> None:
    deployer = _bound_deployer()
    response = _raw_response(
        200,
        (
            b'{"name":"orders-sink","config":{"name":"orders-sink",'
            b'"password":"first-secret","password":"duplicate-secret"}}'
        ),
    )
    deployer._http_session.request = MagicMock(  # type: ignore[method-assign]
        return_value=response
    )

    try:
        with pytest.raises(ConnectManagedObservationError) as caught:
            deployer.observe_managed_connector("orders-sink")
    finally:
        deployer.close()

    rendered = str(caught.value)
    assert "first-secret" not in rendered
    assert "duplicate-secret" not in rendered
    assert "canonical JSON" in rendered
    deployer._http_session.request.assert_called_once()
    response.iter_content.assert_called_once_with(chunk_size=64 * 1024)
    response.json.assert_not_called()


def test_strict_observation_rejects_oversized_body_before_json_decode() -> None:
    deployer = _bound_deployer()
    response = _raw_response(
        200,
        b"oversized-secret=" + b"x" * (1024 * 1024),
    )
    deployer._http_session.request = MagicMock(  # type: ignore[method-assign]
        return_value=response
    )

    try:
        with pytest.raises(ConnectManagedObservationError) as caught:
            deployer.observe_managed_connector("orders-sink")
    finally:
        deployer.close()

    rendered = str(caught.value)
    assert "oversized" in rendered
    assert "oversized-secret" not in rendered
    deployer._http_session.request.assert_called_once()
    response.iter_content.assert_called_once_with(chunk_size=64 * 1024)
    response.json.assert_not_called()


def test_connector_change_evidence_omits_values_and_sanitizes_unsafe_keys() -> None:
    current = {
        "safe.key": "current-secret",
        "https://user:password@host/path": "url-key-secret",
        "assignment=secret": True,
        "control\nsecret": 1,
        "x" * 129: "long-key-secret",
    }
    desired = {
        "safe.key": "desired-secret",
        "https://user:password@host/path": "changed-url-key-secret",
        "assignment=secret": False,
        "control\nsecret": 2,
        "x" * 129: "changed-long-key-secret",
    }

    changes = secret_neutral_connector_config_diff(current, desired)

    assert changes == {
        "safe.key": {
            "change": "changed",
            "from_present": True,
            "to_present": True,
        },
        "<unsafe-config-key-1>": {
            "change": "changed",
            "from_present": True,
            "to_present": True,
        },
        "<unsafe-config-key-2>": {
            "change": "changed",
            "from_present": True,
            "to_present": True,
        },
        "<unsafe-config-key-3>": {
            "change": "changed",
            "from_present": True,
            "to_present": True,
        },
        "<unsafe-config-key-4>": {
            "change": "changed",
            "from_present": True,
            "to_present": True,
        },
    }
    rendered = json.dumps(changes, sort_keys=True)
    for secret in (
        "current-secret",
        "desired-secret",
        "url-key-secret",
        "password@host",
        "assignment=secret",
        "control\\nsecret",
        "long-key-secret",
    ):
        assert secret not in rendered

    legacy = secret_neutral_connector_changes(
        {
            "safe.key": {
                "change": "changed",
                "from_present": True,
                "to_present": True,
                "from_fingerprint": "sha256:" + "a" * 64,
                "to_fingerprint": "sha256:" + "b" * 64,
            }
        }
    )
    assert legacy == {"safe.key": changes["safe.key"]}


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
