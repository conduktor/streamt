"""Strict, secret-safe OpenLineage transport configuration tests."""

from __future__ import annotations

import json
import os
import stat
from dataclasses import FrozenInstanceError
from pathlib import Path

import pytest
import requests
from requests.adapters import HTTPAdapter

from streamt.core.errors import ErrorCode
from streamt.integrations.openlineage import (
    DatasetIdentity,
    FileTransportConfig,
    HttpTransportConfig,
    OpenLineageDeliveryError,
    OpenLineageTransportConfigurationError,
    OpenLineageValidationError,
    build_dataset_event,
    create_openlineage_transport,
    load_openlineage_transport_config,
    parse_openlineage_transport_config,
)
from streamt.integrations.openlineage import transport as transport_module


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


def _event(name: str = "orders", description: str | None = None) -> dict[str, object]:
    facets = None
    if description is not None:
        facets = {
            "documentation": {
                "_producer": "https://github.com/conduktor/streamt",
                "_schemaURL": (
                    "https://openlineage.io/spec/facets/1-1-0/"
                    "DocumentationDatasetFacet.json#/$defs/DocumentationDatasetFacet"
                ),
                "description": description,
            }
        }
    return build_dataset_event(
        event_time="2026-09-01T12:34:56Z",
        dataset=DatasetIdentity("kafka://broker.example:9092", name),
        facets=facets,
    )


def _canonical_line(event: dict[str, object]) -> bytes:
    return (
        json.dumps(
            event,
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        + "\n"
    ).encode()


def test_file_transport_appends_ordered_canonical_durable_lines_and_preserves_mode(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = tmp_path / "events.jsonl"
    target.write_bytes(b"existing\n")
    target.chmod(0o640)
    sync_calls: list[int] = []
    sync_name = "fdatasync" if hasattr(os, "fdatasync") else "fsync"
    real_sync = getattr(os, sync_name)

    def record_sync(descriptor: int) -> None:
        sync_calls.append(descriptor)
        real_sync(descriptor)

    monkeypatch.setattr(transport_module.os, sync_name, record_sync)
    first = _event("café")
    second = _event("payments")
    transport = create_openlineage_transport(FileTransportConfig(target))

    transport.emit(first)
    transport.emit(second)
    transport.close()
    transport.close()

    assert target.read_bytes() == b"existing\n" + _canonical_line(first) + _canonical_line(second)
    assert stat.S_IMODE(target.stat().st_mode) == 0o640
    assert len(sync_calls) == 2


def test_file_transport_creates_private_append_only_file(tmp_path: Path) -> None:
    target = tmp_path / "new-events.jsonl"
    event = _event()
    transport = create_openlineage_transport(FileTransportConfig(target))

    transport.emit(event)
    transport.close()

    assert target.read_bytes() == _canonical_line(event)
    assert stat.S_IMODE(target.stat().st_mode) == 0o600


def test_file_transport_retries_partial_os_writes_until_one_line_is_complete(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = tmp_path / "partial.jsonl"
    event = _event()
    real_write = os.write
    writes: list[int] = []

    def partial_write(descriptor: int, payload: bytes) -> int:
        chunk = payload[:7]
        writes.append(len(chunk))
        return real_write(descriptor, chunk)

    transport = create_openlineage_transport(FileTransportConfig(target))
    monkeypatch.setattr(transport_module.os, "write", partial_write)

    transport.emit(event)
    transport.close()

    assert target.read_bytes() == _canonical_line(event)
    assert len(writes) > 1


def test_file_transport_validates_before_event_write(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = tmp_path / "invalid.jsonl"
    transport = create_openlineage_transport(FileTransportConfig(target))
    write_called = False

    def unexpected_write(_descriptor: int, _payload: bytes) -> int:
        nonlocal write_called
        write_called = True
        return 0

    monkeypatch.setattr(transport_module.os, "write", unexpected_write)

    with pytest.raises(OpenLineageValidationError):
        transport.emit({"secret": "event-secret"})
    transport.close()

    assert write_called is False
    assert not target.exists()


def test_file_transport_failures_are_fixed_safe_and_close_is_idempotent(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    secret = "file-event-secret"
    target = tmp_path / "secret-path-token.jsonl"
    transport = create_openlineage_transport(FileTransportConfig(target))

    def failed_write(_descriptor: int, _payload: bytes) -> int:
        raise OSError(f"failed at {target} with {secret}")

    monkeypatch.setattr(transport_module.os, "write", failed_write)
    with pytest.raises(OpenLineageDeliveryError) as captured:
        transport.emit(_event(description=secret))

    rendered = repr(captured.value)
    assert str(target) not in rendered
    assert secret not in rendered
    assert captured.value.__cause__ is None
    assert captured.value.__context__ is None
    transport.close()
    transport.close()


def test_file_transport_sync_failure_and_emit_after_close_are_safe(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = tmp_path / "sync-secret.jsonl"
    transport = create_openlineage_transport(FileTransportConfig(target))

    def failed_sync(_descriptor: int) -> None:
        raise OSError(f"sync failed for {target}")

    monkeypatch.setattr(transport_module, "_sync_descriptor", failed_sync)
    with pytest.raises(OpenLineageDeliveryError) as sync_error:
        transport.emit(_event())
    assert str(target) not in repr(sync_error.value)
    assert sync_error.value.__context__ is None

    transport.close()
    with pytest.raises(OpenLineageDeliveryError) as closed_error:
        transport.emit(_event())
    assert str(target) not in repr(closed_error.value)


def test_file_transport_rejects_nonregular_and_symlink_targets_safely(
    tmp_path: Path,
) -> None:
    targets = [tmp_path]
    if hasattr(os, "O_NOFOLLOW"):
        actual = tmp_path / "actual.jsonl"
        actual.touch()
        link = tmp_path / "link.jsonl"
        link.symlink_to(actual)
        targets.append(link)

    for target in targets:
        transport = create_openlineage_transport(FileTransportConfig(target))
        with pytest.raises(OpenLineageDeliveryError) as captured:
            transport.emit(_event())
        transport.close()
        assert str(target) not in str(captured.value)
        assert captured.value.__context__ is None


def test_file_transport_close_failure_is_safe_and_not_retried(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = tmp_path / "close-secret.jsonl"
    transport = create_openlineage_transport(FileTransportConfig(target))
    transport.emit(_event())
    real_close = os.close
    calls = 0

    def failed_after_close(descriptor: int) -> None:
        nonlocal calls
        calls += 1
        real_close(descriptor)
        raise OSError(f"close failed for {target}")

    monkeypatch.setattr(transport_module.os, "close", failed_after_close)
    with pytest.raises(OpenLineageDeliveryError) as captured:
        transport.close()
    transport.close()

    assert calls == 1
    assert str(target) not in str(captured.value)
    assert captured.value.__context__ is None


class _FakeResponse:
    def __init__(self, status_code: int, *, close_error: str | None = None) -> None:
        self.status_code = status_code
        self.close_error = close_error
        self.close_calls = 0

    @property
    def text(self) -> str:
        raise AssertionError("response bodies must not be read")

    @property
    def content(self) -> bytes:
        raise AssertionError("response bodies must not be read")

    def close(self) -> None:
        self.close_calls += 1
        if self.close_error is not None:
            raise RuntimeError(self.close_error)


class _FakeSession:
    def __init__(self, effects: list[object] | None = None) -> None:
        self.effects = list(effects or [_FakeResponse(200)])
        self.trust_env = True
        self.verify = False
        self.mounts: list[tuple[str, object]] = []
        self.calls: list[tuple[str, dict[str, object]]] = []
        self.close_calls = 0
        self.close_error: str | None = None

    def mount(self, prefix: str, adapter: object) -> None:
        self.mounts.append((prefix, adapter))

    def post(self, url: str, **kwargs: object) -> _FakeResponse:
        self.calls.append((url, kwargs))
        effect = self.effects.pop(0)
        if isinstance(effect, BaseException):
            raise effect
        assert isinstance(effect, _FakeResponse)
        return effect

    def close(self) -> None:
        self.close_calls += 1
        if self.close_error is not None:
            raise RuntimeError(self.close_error)


def _http_delivery_config(
    *,
    retry_total: int = 0,
    api_key: str | None = None,
) -> HttpTransportConfig:
    document = _http_transport(retry={"total": retry_total})
    if api_key is not None:
        transport = document["transport"]
        assert isinstance(transport, dict)
        transport["auth"] = {"type": "api_key", "apiKey": api_key}
    config = _parse(document)
    assert isinstance(config, HttpTransportConfig)
    return config


def _create_fake_http_transport(
    monkeypatch: pytest.MonkeyPatch,
    session: _FakeSession,
    *,
    retry_total: int = 0,
    api_key: str | None = None,
):
    monkeypatch.setattr(transport_module.requests, "Session", lambda: session)
    return create_openlineage_transport(
        _http_delivery_config(retry_total=retry_total, api_key=api_key)
    )


def test_factory_revalidates_public_config_records_before_creating_a_session(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session_created = False

    def unexpected_session() -> _FakeSession:
        nonlocal session_created
        session_created = True
        return _FakeSession()

    monkeypatch.setattr(transport_module.requests, "Session", unexpected_session)
    config = HttpTransportConfig(
        url="https://user:factory-secret@lineage.example",
        endpoint="api/v1/lineage",
    )

    with pytest.raises(OpenLineageTransportConfigurationError) as captured:
        create_openlineage_transport(config)

    assert session_created is False
    assert "factory-secret" not in str(captured.value)


def test_http_transport_posts_one_canonical_line_with_explicit_safe_options(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    response = _FakeResponse(204)
    session = _FakeSession([response])
    api_key = "http-api-key-secret"
    transport = _create_fake_http_transport(monkeypatch, session, api_key=api_key)
    event = _event("café")

    transport.emit(event)
    transport.close()
    transport.close()

    assert session.trust_env is False
    assert session.verify is True
    assert [prefix for prefix, _adapter in session.mounts] == ["http://", "https://"]
    for _prefix, adapter in session.mounts:
        assert isinstance(adapter, HTTPAdapter)
        assert adapter.max_retries.total == 0
    assert session.calls == [
        (
            "https://lineage.example/base/api/v1/lineage",
            {
                "data": _canonical_line(event),
                "headers": {
                    "Content-Type": "application/json",
                    "Authorization": f"Bearer {api_key}",
                },
                "timeout": 5.0,
                "verify": True,
                "allow_redirects": False,
                "stream": True,
            },
        )
    ]
    assert response.close_calls == 1
    assert session.close_calls == 1


def test_http_transport_validates_before_request(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session = _FakeSession()
    transport = _create_fake_http_transport(monkeypatch, session)

    with pytest.raises(OpenLineageValidationError):
        transport.emit({"secret": "event-secret"})
    transport.close()

    assert session.calls == []


def test_http_transport_emit_after_close_is_safe_and_does_not_request(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session = _FakeSession()
    transport = _create_fake_http_transport(monkeypatch, session)
    transport.close()

    with pytest.raises(OpenLineageDeliveryError) as captured:
        transport.emit(_event())

    assert session.calls == []
    assert "lineage.example" not in repr(captured.value)


@pytest.mark.parametrize("status", [408, 429, 500, 502, 503, 504])
def test_http_transport_retries_each_supported_transient_status_once(
    status: int,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    first = _FakeResponse(status)
    second = _FakeResponse(200)
    session = _FakeSession([first, second])
    transport = _create_fake_http_transport(monkeypatch, session, retry_total=1)

    transport.emit(_event())
    transport.close()

    assert len(session.calls) == 2
    assert first.close_calls == 1
    assert second.close_calls == 1


@pytest.mark.parametrize(
    "failure",
    [requests.Timeout("timeout-secret"), requests.ConnectionError("connection-secret")],
)
def test_http_transport_retries_only_timeout_and_connection_failures(
    failure: requests.RequestException,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    response = _FakeResponse(200)
    session = _FakeSession([failure, response])
    transport = _create_fake_http_transport(monkeypatch, session, retry_total=1)

    transport.emit(_event())
    transport.close()

    assert len(session.calls) == 2
    assert response.close_calls == 1


@pytest.mark.parametrize("status", [300, 301, 400, 401, 404, 409])
def test_http_transport_never_retries_redirects_or_nontransient_statuses(
    status: int,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    response = _FakeResponse(status)
    session = _FakeSession([response, _FakeResponse(200)])
    transport = _create_fake_http_transport(monkeypatch, session, retry_total=1)

    with pytest.raises(OpenLineageDeliveryError) as captured:
        transport.emit(_event())
    transport.close()

    assert len(session.calls) == 1
    assert response.close_calls == 1
    assert str(status) in str(captured.value)


def test_http_retry_bound_is_zero_when_not_enabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    response = _FakeResponse(503)
    session = _FakeSession([response, _FakeResponse(200)])
    transport = _create_fake_http_transport(monkeypatch, session)

    with pytest.raises(OpenLineageDeliveryError):
        transport.emit(_event())
    transport.close()

    assert len(session.calls) == 1
    assert response.close_calls == 1


def test_http_generic_request_failure_is_not_retried_and_never_leaks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    secret = "request-exception-secret"
    session = _FakeSession([requests.RequestException(secret), _FakeResponse(200)])
    transport = _create_fake_http_transport(
        monkeypatch,
        session,
        retry_total=1,
        api_key="api-key-secret",
    )

    with pytest.raises(OpenLineageDeliveryError) as captured:
        transport.emit(_event(description="event-description-secret"))
    transport.close()

    rendered = repr(captured.value)
    assert len(session.calls) == 1
    assert secret not in rendered
    assert "api-key-secret" not in rendered
    assert "event-description-secret" not in rendered
    assert "lineage.example" not in rendered
    assert captured.value.__cause__ is None
    assert captured.value.__context__ is None


def test_http_response_close_failure_is_safe_and_not_retried(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    response = _FakeResponse(503, close_error="response-close-secret")
    session = _FakeSession([response, _FakeResponse(200)])
    transport = _create_fake_http_transport(monkeypatch, session, retry_total=1)

    with pytest.raises(OpenLineageDeliveryError) as captured:
        transport.emit(_event())
    transport.close()

    assert len(session.calls) == 1
    assert response.close_calls == 1
    assert "response-close-secret" not in repr(captured.value)
    assert captured.value.__context__ is None


def test_http_transport_setup_and_close_failures_are_safe_and_idempotent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    setup_secret = "session-setup-secret"

    def fail_setup() -> _FakeSession:
        raise RuntimeError(setup_secret)

    monkeypatch.setattr(transport_module.requests, "Session", fail_setup)
    with pytest.raises(OpenLineageDeliveryError) as setup_error:
        create_openlineage_transport(_http_delivery_config())
    assert setup_secret not in repr(setup_error.value)
    assert setup_error.value.__context__ is None

    session = _FakeSession()
    session.close_error = "session-close-secret"
    transport = _create_fake_http_transport(monkeypatch, session)
    with pytest.raises(OpenLineageDeliveryError) as close_error:
        transport.close()
    transport.close()

    assert session.close_calls == 1
    assert "session-close-secret" not in repr(close_error.value)
    assert close_error.value.__context__ is None


def test_w112_identifier_is_stable_but_not_yet_emitted_by_a_command() -> None:
    assert ErrorCode.OPENLINEAGE_EMIT_FAILED == "W112_OPENLINEAGE_EMIT_FAILED"
