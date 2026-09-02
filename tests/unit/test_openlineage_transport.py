"""Strict, secret-safe OpenLineage transport configuration tests."""

from __future__ import annotations

from dataclasses import FrozenInstanceError
from pathlib import Path

import pytest

from streamt.integrations.openlineage import (
    FileTransportConfig,
    HttpTransportConfig,
    OpenLineageTransportConfigurationError,
    load_openlineage_transport_config,
    parse_openlineage_transport_config,
)


def _http_transport(**updates: object) -> dict[str, object]:
    transport: dict[str, object] = {
        "type": "http",
        "url": "https://lineage.example/base",
        "endpoint": "api/v1/lineage",
    }
    transport.update(updates)
    return {"transport": transport}


def _parse(
    document: dict[str, object] | None,
    environment: dict[str, str] | None = None,
    *,
    emission_requested: bool = True,
) -> FileTransportConfig | HttpTransportConfig:
    return parse_openlineage_transport_config(
        document,
        environment or {},
        emission_requested=emission_requested,
    )


def test_file_config_is_frozen_local_and_secret_neutral_in_repr() -> None:
    config = _parse({"transport": {"type": "file", "log_file_path": "events.jsonl"}})

    assert isinstance(config, FileTransportConfig)
    assert config.log_file_path == Path("events.jsonl")
    assert "events.jsonl" not in repr(config)
    with pytest.raises(FrozenInstanceError):
        config.log_file_path = Path("changed")  # type: ignore[misc]


@pytest.mark.parametrize(
    "path",
    [
        "",
        " events.jsonl",
        "events.jsonl ",
        "file:///tmp/events",
        "s3://bucket/key",
        "//host/file",
        "\\\\host\\file",
        "bad\x00path",
        "line\nbreak",
        "delete\x7fcharacter",
        "control\x85character",
    ],
)
def test_file_config_rejects_nonlocal_or_malformed_paths(path: str) -> None:
    with pytest.raises(OpenLineageTransportConfigurationError) as captured:
        _parse({"transport": {"type": "file", "log_file_path": path}})

    assert captured.value.location == "openlineage.transport.log_file_path"
    if path:
        assert path not in str(captured.value)


def test_file_config_has_an_exact_allowlist() -> None:
    for transport in (
        {"type": "file"},
        {"type": "file", "log_file_path": "events", "append": True},
        {"type": "file", "log_file_path": 123},
    ):
        with pytest.raises(OpenLineageTransportConfigurationError):
            _parse({"transport": transport})


def test_http_defaults_are_conservative_and_repr_hides_configuration() -> None:
    config = _parse(_http_transport())

    assert isinstance(config, HttpTransportConfig)
    assert config.url == "https://lineage.example/base"
    assert config.endpoint == "api/v1/lineage"
    assert config.timeout_seconds == 5.0
    assert config.verify is True
    assert config.retry_total == 0
    assert config.api_key is None
    rendered = repr(config)
    assert "lineage.example" not in rendered
    assert "api/v1/lineage" not in rendered


@pytest.mark.parametrize(
    "url",
    [
        "http://localhost:5000",
        "http://127.0.0.2:5000/base",
        "http://[::1]:5000",
        "https://lineage.example",
        "https://[2001:db8::1]:8443/base",
    ],
)
def test_http_url_accepts_https_and_loopback_plaintext(url: str) -> None:
    config = _parse(_http_transport(url=url))

    assert isinstance(config, HttpTransportConfig)
    assert config.url == url


@pytest.mark.parametrize(
    "url",
    [
        "http://lineage.example",
        "ftp://lineage.example",
        "https://user:secret@lineage.example",
        "https://lineage.example?token=secret",
        "https://lineage.example#fragment",
        "https://[2001:db8::1",
        "https://[fe80::1%25eth0]:8443",
        "https://lineage.example:99999",
        "https://lineage.example:0",
        "https://lineage.example\\path",
        " https://lineage.example",
    ],
)
def test_http_url_rejection_never_echoes_the_value(url: str) -> None:
    with pytest.raises(OpenLineageTransportConfigurationError) as captured:
        _parse(_http_transport(url=url))

    assert captured.value.location == "openlineage.transport.url"
    assert url not in str(captured.value)
    assert "secret" not in str(captured.value)


@pytest.mark.parametrize(
    "endpoint",
    [
        "",
        "/api/v1/lineage",
        "https://other/path",
        "../lineage",
        "api/%2e%2e/lineage",
        "api/%252e%252e/lineage",
        "api%2f..%2flineage",
        "api/%5C..%5clineage",
        "api\\lineage",
        "api?token=x",
        "api#part",
    ],
)
def test_http_endpoint_must_be_an_explicit_relative_nontraversing_path(
    endpoint: str,
) -> None:
    with pytest.raises(OpenLineageTransportConfigurationError) as captured:
        _parse(_http_transport(endpoint=endpoint))

    assert captured.value.location == "openlineage.transport.endpoint"
    if endpoint:
        assert endpoint not in str(captured.value)


@pytest.mark.parametrize("timeout", [True, False, 0, -1, 5.01, float("inf"), float("nan"), "5"])
def test_http_timeout_is_finite_nonboolean_and_bounded(timeout: object) -> None:
    with pytest.raises(OpenLineageTransportConfigurationError) as captured:
        _parse(_http_transport(timeout=timeout))

    assert captured.value.location == "openlineage.transport.timeout"


def test_http_timeout_accepts_positive_values_through_five() -> None:
    for timeout in (0.001, 1, 5):
        config = _parse(_http_transport(timeout=timeout))
        assert isinstance(config, HttpTransportConfig)
        assert config.timeout_seconds == float(timeout)


@pytest.mark.parametrize("verify", [False, 0, 1, "true", "false", None])
def test_http_tls_verification_cannot_be_disabled_or_coerced(verify: object) -> None:
    with pytest.raises(OpenLineageTransportConfigurationError) as captured:
        _parse(_http_transport(verify=verify))

    assert captured.value.location == "openlineage.transport.verify"


def test_http_retry_accepts_only_total_zero_or_one() -> None:
    for total in (0, 1):
        config = _parse(_http_transport(retry={"total": total}))
        assert isinstance(config, HttpTransportConfig)
        assert config.retry_total == total

    for retry in (
        {},
        {"total": 2},
        {"total": -1},
        {"total": True},
        {"total": "1"},
        {"total": 1, "connect": 1},
        1,
    ):
        with pytest.raises(OpenLineageTransportConfigurationError):
            _parse(_http_transport(retry=retry))


def test_http_api_key_is_https_only_and_secret_safe() -> None:
    secret = "authorization-token-value"
    config = _parse(
        _http_transport(auth={"type": "api_key", "apiKey": secret})
    )

    assert isinstance(config, HttpTransportConfig)
    assert config.api_key is not None
    assert config.api_key.get_secret_value() == secret
    assert secret not in repr(config)

    with pytest.raises(OpenLineageTransportConfigurationError) as captured:
        _parse(
            _http_transport(
                url="http://localhost:5000",
                auth={"type": "api_key", "apiKey": secret},
            )
        )
    assert secret not in str(captured.value)


@pytest.mark.parametrize(
    "auth",
    [
        {},
        {"type": "bearer", "apiKey": "secret"},
        {"type": "api_key"},
        {"type": "api_key", "apiKey": ""},
        {"type": "api_key", "apiKey": " secret"},
        {"type": "api_key", "apiKey": "secret", "header": "X-Key"},
        "secret",
    ],
)
def test_http_auth_has_an_exact_api_key_shape(auth: object) -> None:
    with pytest.raises(OpenLineageTransportConfigurationError) as captured:
        _parse(_http_transport(auth=auth))

    assert "secret" not in str(captured.value)


def test_http_transport_and_nested_sections_have_exact_allowlists() -> None:
    for document in (
        _http_transport(headers={"Authorization": "secret"}),
        _http_transport(compression="gzip"),
        _http_transport(session="custom"),
        {"transport": {"type": "console"}},
        {"transport": {"type": "kafka"}},
        {"transport": {"type": "composite"}},
        {"transport": {"type": "async"}},
        {"transport": {"type": "custom"}},
    ):
        with pytest.raises(OpenLineageTransportConfigurationError) as captured:
            _parse(document)
        assert "secret" not in str(captured.value)


def test_only_transport_is_allowed_at_the_configuration_root() -> None:
    for field in ("facets", "tags", "filters", "dataset_normalization", "disabled"):
        document = _http_transport()
        document[field] = {"secret": "value"}
        with pytest.raises(OpenLineageTransportConfigurationError) as captured:
            _parse(document)
        assert captured.value.location == "openlineage.config"
        assert "value" not in str(captured.value)


def test_missing_configuration_never_discovers_an_implicit_default() -> None:
    with pytest.raises(OpenLineageTransportConfigurationError) as captured:
        _parse(None)

    assert captured.value.location == "openlineage.transport"


def test_disabled_value_is_strict_and_conflicts_only_with_requested_emission() -> None:
    with pytest.raises(OpenLineageTransportConfigurationError) as captured:
        _parse(_http_transport(), {"OPENLINEAGE_DISABLED": "true"})
    assert captured.value.location == "openlineage.disabled"

    config = _parse(
        _http_transport(),
        {"OPENLINEAGE_DISABLED": "true"},
        emission_requested=False,
    )
    assert isinstance(config, HttpTransportConfig)
    assert isinstance(
        _parse(_http_transport(), {"OPENLINEAGE_DISABLED": "false"}),
        HttpTransportConfig,
    )

    for value in ("", "TRUE", "False", "1", " true"):
        with pytest.raises(OpenLineageTransportConfigurationError):
            _parse(_http_transport(), {"OPENLINEAGE_DISABLED": value})


def test_legacy_and_unknown_environment_fields_are_rejected_by_presence() -> None:
    for environment in (
        {"OPENLINEAGE_URL": ""},
        {"OPENLINEAGE_API_KEY": "secret"},
        {"OPENLINEAGE__FACETS__DISABLED": "true"},
        {"OPENLINEAGE__TRANSPORT__HEADERS__AUTHORIZATION": "secret"},
        {"OPENLINEAGE__TRANSPORT__AUTH__API_KEY": "secret"},
    ):
        with pytest.raises(OpenLineageTransportConfigurationError) as captured:
            _parse(_http_transport(), environment)
        assert "secret" not in str(captured.value)


def test_recognized_environment_fields_overlay_by_presence() -> None:
    environment = {
        "OPENLINEAGE__TRANSPORT__URL": "https://override.example",
        "OPENLINEAGE__TRANSPORT__ENDPOINT": "custom/lineage",
        "OPENLINEAGE__TRANSPORT__TIMEOUT": "2.5",
        "OPENLINEAGE__TRANSPORT__VERIFY": "true",
        "OPENLINEAGE__TRANSPORT__RETRY__TOTAL": "1",
        "OPENLINEAGE__TRANSPORT__AUTH__TYPE": "api_key",
        "OPENLINEAGE__TRANSPORT__AUTH__APIKEY": "top-secret",
    }

    config = _parse(_http_transport(), environment)

    assert isinstance(config, HttpTransportConfig)
    assert config.url == "https://override.example"
    assert config.endpoint == "custom/lineage"
    assert config.timeout_seconds == 2.5
    assert config.retry_total == 1
    assert config.api_key is not None
    assert config.api_key.get_secret_value() == "top-secret"
    assert "top-secret" not in repr(config)

    environment["OPENLINEAGE__TRANSPORT__URL"] = ""
    with pytest.raises(OpenLineageTransportConfigurationError) as captured:
        _parse(_http_transport(), environment)
    assert "lineage.example" not in str(captured.value)


@pytest.mark.parametrize(
    ("name", "value", "location"),
    [
        ("OPENLINEAGE__TRANSPORT__TIMEOUT", "five", "openlineage.transport.timeout"),
        ("OPENLINEAGE__TRANSPORT__VERIFY", "1", "openlineage.transport.verify"),
        (
            "OPENLINEAGE__TRANSPORT__RETRY__TOTAL",
            "1.0",
            "openlineage.transport.retry.total",
        ),
    ],
)
def test_environment_scalar_coercion_is_narrow(
    name: str,
    value: str,
    location: str,
) -> None:
    with pytest.raises(OpenLineageTransportConfigurationError) as captured:
        _parse(_http_transport(), {name: value})

    assert captured.value.location == location


def test_environment_can_supply_the_entire_transport_without_a_file() -> None:
    config = _parse(
        None,
        {
            "OPENLINEAGE__TRANSPORT__TYPE": "file",
            "OPENLINEAGE__TRANSPORT__LOG_FILE_PATH": "events.jsonl",
        },
    )

    assert config == FileTransportConfig(log_file_path=Path("events.jsonl"))


def test_loader_reads_only_the_explicit_file_then_applies_environment(
    tmp_path: Path,
) -> None:
    config_path = tmp_path / "openlineage.yml"
    config_path.write_text(
        "transport:\n  type: http\n  url: https://file.example\n"
        "  endpoint: api/v1/lineage\n",
        encoding="utf-8",
    )

    config = load_openlineage_transport_config(
        {
            "OPENLINEAGE_CONFIG": str(config_path),
            "OPENLINEAGE__TRANSPORT__URL": "https://environment.example",
        }
    )

    assert isinstance(config, HttpTransportConfig)
    assert config.url == "https://environment.example"


@pytest.mark.parametrize(
    "content",
    [
        "transport:\n  type: file\n  type: http\n",
        "transport:\n  type: http\n  retry:\n    total: 0\n    total: 1\n",
        "- transport\n- file\n",
        "null\n",
        "transport: [file]\n",
    ],
)
def test_loader_rejects_duplicate_or_nonmapping_yaml_safely(
    tmp_path: Path,
    content: str,
) -> None:
    config_path = tmp_path / "contains-secret.yml"
    config_path.write_text(content, encoding="utf-8")

    with pytest.raises(OpenLineageTransportConfigurationError) as captured:
        load_openlineage_transport_config({"OPENLINEAGE_CONFIG": str(config_path)})

    rendered = str(captured.value)
    assert str(config_path) not in rendered
    assert content not in rendered


@pytest.mark.parametrize(
    "content",
    [
        "transport:\n  type: file\n  log_file_path: &path events.jsonl\n",
        "transport: &transport\n  type: file\n  log_file_path: events.jsonl\ncopy: *transport\n",
        "transport:\n"
        + "".join(
            f"{'  ' * level}level_{level}:\n" for level in range(1, 34)
        )
        + f"{'  ' * 34}value: leaf\n",
        "transport:\n  type: [" + ",".join("file" for _ in range(2_100)) + "]\n",
    ],
)
def test_loader_rejects_aliases_and_bounded_yaml_complexity(
    tmp_path: Path,
    content: str,
) -> None:
    config_path = tmp_path / "complex.yml"
    config_path.write_text(content, encoding="utf-8")

    with pytest.raises(OpenLineageTransportConfigurationError) as captured:
        load_openlineage_transport_config({"OPENLINEAGE_CONFIG": str(config_path)})

    assert captured.value.location == "openlineage.config"
    assert str(config_path) not in str(captured.value)


def test_loader_enforces_a_bounded_utf8_regular_file(
    tmp_path: Path,
) -> None:
    oversized = tmp_path / "oversized.yml"
    oversized.write_bytes(b"x" * 65_537)
    invalid_utf8 = tmp_path / "invalid.yml"
    invalid_utf8.write_bytes(b"\xff")

    for path in (oversized, invalid_utf8, tmp_path, tmp_path / "missing.yml"):
        with pytest.raises(OpenLineageTransportConfigurationError) as captured:
            load_openlineage_transport_config({"OPENLINEAGE_CONFIG": str(path)})
        assert str(path) not in str(captured.value)


@pytest.mark.parametrize("path", ["", " config.yml", "bad\x00path"])
def test_loader_rejects_malformed_explicit_config_paths_without_echo(path: str) -> None:
    with pytest.raises(OpenLineageTransportConfigurationError) as captured:
        load_openlineage_transport_config({"OPENLINEAGE_CONFIG": path})

    assert captured.value.location == "openlineage.config"
    if path:
        assert path not in str(captured.value)
