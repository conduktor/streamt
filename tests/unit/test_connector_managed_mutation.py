"""Strict managed Kafka Connect mutation contract tests."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from unittest.mock import MagicMock, call, patch

import pytest

from streamt.deployer.connect import (
    DEFAULT_TIMEOUT,
    ConnectClusterBinding,
    ConnectDeployer,
    ConnectManagedMutationError,
    ManagedConnectorObservation,
)

_CONNECT_URL = "https://connect.example.test:8443/api/"
_CONNECTOR_NAME = "team/orders sink?#"
_FAILURE = "Kafka Connect managed deletion could not prove exact absence"
_SECRET_SENTINELS = (
    "auth-secret",
    "binding-secret",
    "current-secret",
    "changed-secret",
    "header-secret",
    "response-secret",
    "status-secret",
    "team/orders sink?#",
    "connect.example.test",
)


@contextmanager
def _deployer(*, alias: str = "production") -> Iterator[ConnectDeployer]:
    deployer = ConnectDeployer(
        _CONNECT_URL,
        username="operator",
        password="auth-secret",
        cluster_alias=alias,
    )
    try:
        yield deployer
    finally:
        deployer.close()


def _present(
    deployer: ConnectDeployer,
    *,
    password: str = "current-secret",
) -> ManagedConnectorObservation:
    return ManagedConnectorObservation(
        binding=deployer.require_cluster_binding(),
        name=_CONNECTOR_NAME,
        exists=True,
        config=(
            ("connector.class", "example.SinkConnector"),
            ("name", _CONNECTOR_NAME),
            ("password", password),
            ("topics", "orders"),
        ),
    )


def _absent(deployer: ConnectDeployer) -> ManagedConnectorObservation:
    return ManagedConnectorObservation(
        binding=deployer.require_cluster_binding(),
        name=_CONNECTOR_NAME,
        exists=False,
    )


def _response(
    status_code: object = 204,
    *,
    body: bytes = b"",
    headers: dict[str, object] | None = None,
) -> MagicMock:
    response = MagicMock()
    response.status_code = status_code
    response.headers = headers if headers is not None else {}
    response.iter_content.return_value = [body]
    return response


def _assert_sanitized_failure(error: pytest.ExceptionInfo[ConnectManagedMutationError]) -> None:
    message = str(error.value)
    assert message == _FAILURE
    for sentinel in _SECRET_SENTINELS:
        assert sentinel not in message


def test_delete_managed_connector_uses_one_encoded_delete_then_proves_absence() -> None:
    with _deployer() as deployer:
        current = _present(deployer)
        response = _response()
        events: list[str] = []

        def observe(
            _name: str,
            *,
            timeout: float = DEFAULT_TIMEOUT,
        ) -> ManagedConnectorObservation:
            events.append("observe")
            return current if len(events) == 1 else _absent(deployer)

        def request(*_args: object, **_kwargs: object) -> MagicMock:
            events.append("delete")
            return response

        with (
            patch.object(deployer, "observe_managed_connector", side_effect=observe) as observer,
            patch.object(deployer._http_session, "request", side_effect=request) as requester,
            patch("streamt.deployer.connect.time.monotonic", side_effect=[100.0, 100.0]),
            patch("streamt.deployer.connect.time.sleep") as sleeper,
        ):
            assert deployer.delete_managed_connector(current) == "deleted"

        assert events == ["observe", "delete", "observe"]
        assert observer.call_args_list == [
            call(_CONNECTOR_NAME),
            call(_CONNECTOR_NAME, timeout=10.0),
        ]
        assert requester.call_args_list == [
            call(
                "DELETE",
                "https://connect.example.test:8443/api/connectors/team%2Forders%20sink%3F%23",
                timeout=DEFAULT_TIMEOUT,
                allow_redirects=False,
                stream=True,
            )
        ]
        response.iter_content.assert_called_once_with(chunk_size=64 * 1024)
        response.close.assert_called_once_with()
        sleeper.assert_not_called()


def test_delete_managed_connector_polls_with_bounded_attempts_until_absent() -> None:
    with _deployer() as deployer:
        current = _present(deployer)
        observations = [current, current, current, _absent(deployer)]

        with (
            patch.object(
                deployer,
                "observe_managed_connector",
                side_effect=observations,
            ) as observer,
            patch.object(
                deployer._http_session,
                "request",
                return_value=_response(),
            ) as requester,
            patch("streamt.deployer.connect.time.monotonic", return_value=0.0),
            patch("streamt.deployer.connect.time.sleep") as sleeper,
        ):
            assert deployer.delete_managed_connector(current) == "deleted"

        assert observer.call_count == 4
        requester.assert_called_once()
        assert sleeper.call_args_list == [call(0.1), call(0.1)]


class _DerivedObservation(ManagedConnectorObservation):
    pass


class _HostileObservation(ManagedConnectorObservation):
    def __eq__(self, _other: object) -> bool:
        return True


class _DerivedBinding(ConnectClusterBinding):
    pass


def test_delete_managed_connector_requires_exact_present_bound_observation() -> None:
    with _deployer() as deployer:
        binding = deployer.require_cluster_binding()
        invalid_inputs: list[object] = [
            _CONNECTOR_NAME,
            {"name": _CONNECTOR_NAME},
            _absent(deployer),
            _DerivedObservation(
                binding=binding,
                name=_CONNECTOR_NAME,
                exists=True,
                config=_present(deployer).config,
            ),
            ManagedConnectorObservation(
                binding=ConnectClusterBinding.from_endpoint(
                    "other",
                    "https://other-secret.example.test",
                ),
                name=_CONNECTOR_NAME,
                exists=True,
                config=_present(deployer).config,
            ),
        ]

        with (
            patch.object(deployer, "observe_managed_connector") as observer,
            patch.object(deployer._http_session, "request") as requester,
        ):
            for invalid in invalid_inputs:
                with pytest.raises(ConnectManagedMutationError) as error:
                    deployer.delete_managed_connector(invalid)  # type: ignore[arg-type]
                _assert_sanitized_failure(error)

        observer.assert_not_called()
        requester.assert_not_called()


@pytest.mark.parametrize(
    "config",
    [
        (
            ("connector.class", "example.SinkConnector"),
            ("password", "current-secret"),
            ("topics", "orders"),
        ),
        (
            ("name", _CONNECTOR_NAME),
            ("password", "current-secret"),
            ("topics", "orders"),
        ),
        (
            ("connector.class", "example.SinkConnector"),
            ("name", _CONNECTOR_NAME),
            ("password", "current-secret"),
        ),
        (
            ("connector.class", "example.SinkConnector"),
            ("name", 7),
            ("password", "current-secret"),
            ("topics", "orders"),
        ),
        (
            ("connector.class", 7),
            ("name", _CONNECTOR_NAME),
            ("password", "current-secret"),
            ("topics", "orders"),
        ),
        (
            ("connector.class", "example.SinkConnector"),
            ("name", _CONNECTOR_NAME),
            ("password", "current-secret"),
            ("topics", False),
        ),
        (
            ("connector.class", ""),
            ("name", _CONNECTOR_NAME),
            ("password", "current-secret"),
            ("topics", "orders"),
        ),
        (
            ("connector.class", "example.SinkConnector"),
            ("name", _CONNECTOR_NAME),
            ("password", "current-secret"),
            ("topics", " "),
        ),
    ],
)
def test_delete_managed_connector_revalidates_complete_reserved_config(
    config: tuple[tuple[str, object], ...],
) -> None:
    with _deployer() as deployer:
        current = _present(deployer)
        object.__setattr__(current, "config", config)

        with (
            patch.object(deployer, "observe_managed_connector") as observer,
            patch.object(deployer._http_session, "request") as requester,
            pytest.raises(ConnectManagedMutationError) as error,
        ):
            deployer.delete_managed_connector(current)

        _assert_sanitized_failure(error)
        observer.assert_not_called()
        requester.assert_not_called()


def test_delete_managed_connector_rejects_nested_binding_subclass() -> None:
    with _deployer() as deployer:
        canonical = deployer.require_cluster_binding()
        current = ManagedConnectorObservation(
            binding=_DerivedBinding(
                cluster_alias=canonical.cluster_alias,
                endpoint_fingerprint=canonical.endpoint_fingerprint,
            ),
            name=_CONNECTOR_NAME,
            exists=True,
            config=_present(deployer).config,
        )

        with (
            patch.object(deployer, "observe_managed_connector") as observer,
            patch.object(deployer._http_session, "request") as requester,
            pytest.raises(ConnectManagedMutationError) as error,
        ):
            deployer.delete_managed_connector(current)

        _assert_sanitized_failure(error)
        observer.assert_not_called()
        requester.assert_not_called()


@pytest.mark.parametrize("hostile_kind", ["observation", "binding"])
def test_delete_managed_connector_rejects_hostile_fresh_snapshot_before_delete(
    hostile_kind: str,
) -> None:
    with _deployer() as deployer:
        current = _present(deployer)
        binding = current.binding
        hostile_binding: ConnectClusterBinding = binding
        if hostile_kind == "binding":
            hostile_binding = _DerivedBinding(
                cluster_alias=binding.cluster_alias,
                endpoint_fingerprint=binding.endpoint_fingerprint,
            )
        observation_type = (
            _HostileObservation if hostile_kind == "observation" else ManagedConnectorObservation
        )
        fresh = observation_type(
            binding=hostile_binding,
            name=current.name,
            exists=True,
            config=current.config,
        )

        with (
            patch.object(deployer, "observe_managed_connector", return_value=fresh) as observer,
            patch.object(deployer._http_session, "request") as requester,
            pytest.raises(ConnectManagedMutationError) as error,
        ):
            deployer.delete_managed_connector(current)

        _assert_sanitized_failure(error)
        observer.assert_called_once_with(_CONNECTOR_NAME)
        requester.assert_not_called()


@pytest.mark.parametrize("hostile_kind", ["observation", "binding"])
def test_delete_managed_connector_rejects_hostile_post_delete_snapshot(
    hostile_kind: str,
) -> None:
    with _deployer() as deployer:
        current = _present(deployer)
        binding = current.binding
        hostile_binding: ConnectClusterBinding = binding
        if hostile_kind == "binding":
            hostile_binding = _DerivedBinding(
                cluster_alias=binding.cluster_alias,
                endpoint_fingerprint=binding.endpoint_fingerprint,
            )
        observation_type = (
            _HostileObservation if hostile_kind == "observation" else ManagedConnectorObservation
        )
        hostile_absence = observation_type(
            binding=hostile_binding,
            name=current.name,
            exists=False,
        )

        with (
            patch.object(
                deployer,
                "observe_managed_connector",
                side_effect=[current, hostile_absence],
            ) as observer,
            patch.object(deployer._http_session, "request", return_value=_response()) as requester,
            patch("streamt.deployer.connect.time.monotonic", return_value=0.0),
            pytest.raises(ConnectManagedMutationError) as error,
        ):
            deployer.delete_managed_connector(current)

        _assert_sanitized_failure(error)
        assert observer.call_count == 2
        requester.assert_called_once()


@pytest.mark.parametrize("fresh_kind", ["absent", "changed"])
def test_delete_managed_connector_rejects_preimage_drift_before_delete(
    fresh_kind: str,
) -> None:
    with _deployer() as deployer:
        current = _present(deployer)
        fresh = (
            _absent(deployer)
            if fresh_kind == "absent"
            else _present(
                deployer,
                password="changed-secret",
            )
        )
        with (
            patch.object(deployer, "observe_managed_connector", return_value=fresh) as observer,
            patch.object(deployer._http_session, "request") as requester,
            pytest.raises(ConnectManagedMutationError) as error,
        ):
            deployer.delete_managed_connector(current)

        _assert_sanitized_failure(error)
        observer.assert_called_once_with(_CONNECTOR_NAME)
        requester.assert_not_called()


def test_delete_managed_connector_sanitizes_pre_observation_failure() -> None:
    with _deployer() as deployer:
        current = _present(deployer)
        with (
            patch.object(
                deployer,
                "observe_managed_connector",
                side_effect=RuntimeError("auth-secret at connect.example.test"),
            ),
            patch.object(deployer._http_session, "request") as requester,
            pytest.raises(ConnectManagedMutationError) as error,
        ):
            deployer.delete_managed_connector(current)

        _assert_sanitized_failure(error)
        assert error.value.__cause__ is None
        requester.assert_not_called()


@pytest.mark.parametrize("status_code", [None, True, 200, 302, 401, 403, 404, 409, 500])
def test_delete_managed_connector_rejects_every_non_exact_204_without_retry(
    status_code: object,
) -> None:
    with _deployer() as deployer:
        current = _present(deployer)
        response = _response(status_code, body=b"response-secret")
        with (
            patch.object(deployer, "observe_managed_connector", return_value=current) as observer,
            patch.object(
                deployer._http_session,
                "request",
                return_value=response,
            ) as requester,
            pytest.raises(ConnectManagedMutationError) as error,
        ):
            deployer.delete_managed_connector(current)

        _assert_sanitized_failure(error)
        observer.assert_called_once_with(_CONNECTOR_NAME)
        requester.assert_called_once()
        response.iter_content.assert_not_called()
        response.close.assert_called_once_with()


def test_delete_managed_connector_sanitizes_transport_uncertainty_without_retry() -> None:
    with _deployer() as deployer:
        current = _present(deployer)
        with (
            patch.object(deployer, "observe_managed_connector", return_value=current) as observer,
            patch.object(
                deployer._http_session,
                "request",
                side_effect=RuntimeError("response-secret from connect.example.test"),
            ) as requester,
            pytest.raises(ConnectManagedMutationError) as error,
        ):
            deployer.delete_managed_connector(current)

        _assert_sanitized_failure(error)
        assert error.value.__cause__ is None
        observer.assert_called_once_with(_CONNECTOR_NAME)
        requester.assert_called_once()


@pytest.mark.parametrize(
    ("body", "headers"),
    [
        (b"response-secret", None),
        (b"", {"Content-Length": "1"}),
        (b"", {"Content-Length": "response-secret"}),
    ],
)
def test_delete_managed_connector_rejects_nonempty_or_invalid_204_body(
    body: bytes,
    headers: dict[str, object] | None,
) -> None:
    with _deployer() as deployer:
        current = _present(deployer)
        response = _response(body=body, headers=headers)
        with (
            patch.object(deployer, "observe_managed_connector", return_value=current) as observer,
            patch.object(deployer._http_session, "request", return_value=response) as requester,
            pytest.raises(ConnectManagedMutationError) as error,
        ):
            deployer.delete_managed_connector(current)

        _assert_sanitized_failure(error)
        observer.assert_called_once_with(_CONNECTOR_NAME)
        requester.assert_called_once()
        response.close.assert_called_once_with()


def test_delete_managed_connector_rejects_unreadable_204_body() -> None:
    with _deployer() as deployer:
        current = _present(deployer)
        response = _response()
        response.iter_content.side_effect = RuntimeError("response-secret")
        with (
            patch.object(deployer, "observe_managed_connector", return_value=current),
            patch.object(deployer._http_session, "request", return_value=response) as requester,
            pytest.raises(ConnectManagedMutationError) as error,
        ):
            deployer.delete_managed_connector(current)

        _assert_sanitized_failure(error)
        requester.assert_called_once()
        response.close.assert_called_once_with()


class _HostileStatusResponse:
    def __init__(self) -> None:
        self.closed = False

    @property
    def status_code(self) -> int:
        raise ConnectManagedMutationError("status-secret")

    def close(self) -> None:
        self.closed = True


class _HostileHeaderPropertyResponse:
    status_code = 204

    def __init__(self) -> None:
        self.closed = False

    @property
    def headers(self) -> dict[str, str]:
        raise RuntimeError("header-secret")

    def close(self) -> None:
        self.closed = True


class _HostileHeaders(dict[str, str]):
    def get(self, _key: str, _default: object = None) -> object:
        raise RuntimeError("header-secret")


@pytest.mark.parametrize(
    "response",
    [_HostileStatusResponse(), _HostileHeaderPropertyResponse()],
)
def test_delete_managed_connector_sanitizes_response_property_failures(
    response: _HostileStatusResponse | _HostileHeaderPropertyResponse,
) -> None:
    with _deployer() as deployer:
        current = _present(deployer)
        with (
            patch.object(deployer, "observe_managed_connector", return_value=current),
            patch.object(deployer._http_session, "request", return_value=response) as requester,
            pytest.raises(ConnectManagedMutationError) as error,
        ):
            deployer.delete_managed_connector(current)

        _assert_sanitized_failure(error)
        requester.assert_called_once()
        assert response.closed is True


def test_delete_managed_connector_sanitizes_mapping_get_failure() -> None:
    with _deployer() as deployer:
        current = _present(deployer)
        response = _response(headers=_HostileHeaders())
        with (
            patch.object(deployer, "observe_managed_connector", return_value=current),
            patch.object(deployer._http_session, "request", return_value=response) as requester,
            pytest.raises(ConnectManagedMutationError) as error,
        ):
            deployer.delete_managed_connector(current)

        _assert_sanitized_failure(error)
        requester.assert_called_once()
        response.close.assert_called_once_with()


def test_delete_managed_connector_sanitizes_unexpected_body_reader_failure() -> None:
    with _deployer() as deployer:
        current = _present(deployer)
        response = _response()
        with (
            patch.object(deployer, "observe_managed_connector", return_value=current),
            patch.object(deployer._http_session, "request", return_value=response) as requester,
            patch.object(
                deployer,
                "_read_managed_observation_body",
                side_effect=RuntimeError("response-secret"),
            ),
            pytest.raises(ConnectManagedMutationError) as error,
        ):
            deployer.delete_managed_connector(current)

        _assert_sanitized_failure(error)
        requester.assert_called_once()
        response.close.assert_called_once_with()


def test_delete_managed_connector_rejects_changed_post_delete_preimage() -> None:
    with _deployer() as deployer:
        current = _present(deployer)
        changed = _present(deployer, password="changed-secret")
        with (
            patch.object(
                deployer,
                "observe_managed_connector",
                side_effect=[current, changed],
            ) as observer,
            patch.object(deployer._http_session, "request", return_value=_response()) as requester,
            patch("streamt.deployer.connect.time.monotonic", return_value=0.0),
            patch("streamt.deployer.connect.time.sleep") as sleeper,
            pytest.raises(ConnectManagedMutationError) as error,
        ):
            deployer.delete_managed_connector(current)

        _assert_sanitized_failure(error)
        assert observer.call_count == 2
        requester.assert_called_once()
        sleeper.assert_not_called()


def test_delete_managed_connector_sanitizes_post_delete_observation_failure() -> None:
    with _deployer() as deployer:
        current = _present(deployer)
        with (
            patch.object(
                deployer,
                "observe_managed_connector",
                side_effect=[current, RuntimeError("changed-secret at connect.example.test")],
            ) as observer,
            patch.object(deployer._http_session, "request", return_value=_response()) as requester,
            patch("streamt.deployer.connect.time.monotonic", return_value=0.0),
            pytest.raises(ConnectManagedMutationError) as error,
        ):
            deployer.delete_managed_connector(current)

        _assert_sanitized_failure(error)
        assert error.value.__cause__ is None
        assert observer.call_count == 2
        requester.assert_called_once()


def test_delete_managed_connector_fails_after_bounded_confirmation_attempts() -> None:
    with _deployer() as deployer:
        current = _present(deployer)
        with (
            patch.object(deployer, "observe_managed_connector", return_value=current) as observer,
            patch.object(deployer._http_session, "request", return_value=_response()) as requester,
            patch("streamt.deployer.connect.time.monotonic", return_value=0.0),
            patch("streamt.deployer.connect.time.sleep") as sleeper,
            pytest.raises(ConnectManagedMutationError) as error,
        ):
            deployer.delete_managed_connector(current)

        _assert_sanitized_failure(error)
        assert observer.call_count == 6  # One preimage GET plus five confirmation GETs.
        requester.assert_called_once()
        assert sleeper.call_count == 4


def test_delete_managed_connector_fails_at_confirmation_deadline() -> None:
    with _deployer() as deployer:
        current = _present(deployer)
        with (
            patch.object(deployer, "observe_managed_connector", return_value=current) as observer,
            patch.object(deployer._http_session, "request", return_value=_response()) as requester,
            patch("streamt.deployer.connect.time.monotonic", side_effect=[0.0, 11.0]),
            patch("streamt.deployer.connect.time.sleep") as sleeper,
            pytest.raises(ConnectManagedMutationError) as error,
        ):
            deployer.delete_managed_connector(current)

        _assert_sanitized_failure(error)
        observer.assert_called_once_with(_CONNECTOR_NAME)
        requester.assert_called_once()
        sleeper.assert_not_called()


def test_bare_connector_delete_remains_a_compatibility_primitive() -> None:
    with _deployer() as deployer:
        with patch.object(deployer, "_request", return_value=None) as request:
            assert deployer.delete_connector(_CONNECTOR_NAME) is None

        request.assert_called_once_with(
            "DELETE",
            "/connectors/team%2Forders%20sink%3F%23",
        )
        assert "not lifecycle deletion authority" in (
            ConnectDeployer.delete_connector.__doc__ or ""
        )
