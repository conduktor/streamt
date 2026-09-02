"""Strict configuration and bounded synchronous OpenLineage transports."""

from __future__ import annotations

import json
import math
import os
import re
import stat
import unicodedata
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from ipaddress import ip_address
from pathlib import Path
from typing import Literal, Protocol
from urllib.parse import urlsplit

import requests
import yaml
from pydantic import SecretStr
from requests.adapters import HTTPAdapter
from yaml.nodes import MappingNode, Node
from yaml.tokens import (
    AliasToken,
    AnchorToken,
    BlockEndToken,
    BlockMappingStartToken,
    BlockSequenceStartToken,
    FlowMappingEndToken,
    FlowMappingStartToken,
    FlowSequenceEndToken,
    FlowSequenceStartToken,
)

_MAX_CONFIG_BYTES = 65_536
_MAX_YAML_TOKENS = 4_096
_MAX_YAML_NESTING = 32
_URI_PREFIX = re.compile(r"^[A-Za-z][A-Za-z0-9+.-]*://")
_INTEGER = re.compile(r"-?[0-9]+")
_PERCENT_ESCAPE = re.compile(r"%([0-9A-Fa-f]{2})")
_RETRYABLE_HTTP_STATUS = frozenset({408, 429, 500, 502, 503, 504})

_CONFIG_ENV = "OPENLINEAGE_CONFIG"
_DISABLED_ENV = "OPENLINEAGE_DISABLED"
_LEGACY_ENV = frozenset({"OPENLINEAGE_URL", "OPENLINEAGE_API_KEY"})
_TRANSPORT_ENV_PREFIX = "OPENLINEAGE__TRANSPORT__"
_ENV_FIELDS = {
    f"{_TRANSPORT_ENV_PREFIX}TYPE": ("type",),
    f"{_TRANSPORT_ENV_PREFIX}LOG_FILE_PATH": ("log_file_path",),
    f"{_TRANSPORT_ENV_PREFIX}URL": ("url",),
    f"{_TRANSPORT_ENV_PREFIX}ENDPOINT": ("endpoint",),
    f"{_TRANSPORT_ENV_PREFIX}TIMEOUT": ("timeout",),
    f"{_TRANSPORT_ENV_PREFIX}VERIFY": ("verify",),
    f"{_TRANSPORT_ENV_PREFIX}RETRY__TOTAL": ("retry", "total"),
    f"{_TRANSPORT_ENV_PREFIX}AUTH__TYPE": ("auth", "type"),
    f"{_TRANSPORT_ENV_PREFIX}AUTH__APIKEY": ("auth", "apiKey"),
}


class OpenLineageTransportConfigurationError(ValueError):
    """A secret-neutral transport configuration failure with a safe location."""

    def __init__(self, message: str, *, location: str) -> None:
        super().__init__(message)
        self.location = location


class OpenLineageDeliveryError(RuntimeError):
    """A fixed secret-neutral File or HTTP delivery failure."""


@dataclass(frozen=True)
class FileTransportConfig:
    """Validated local append-only File transport configuration."""

    log_file_path: Path = field(repr=False)


@dataclass(frozen=True)
class HttpTransportConfig:
    """Validated synchronous HTTP transport configuration."""

    url: str = field(repr=False)
    endpoint: str = field(repr=False)
    timeout_seconds: float = 5.0
    verify: Literal[True] = True
    retry_total: Literal[0, 1] = 0
    api_key: SecretStr | None = field(default=None, repr=False)


OpenLineageTransportConfig = FileTransportConfig | HttpTransportConfig


class OpenLineageTransport(Protocol):
    """Small synchronous delivery interface for later opt-in command hooks."""

    def emit(self, event: Mapping[str, object]) -> None:
        """Validate and synchronously deliver exactly one event."""

    def close(self) -> None:
        """Release local resources without draining background work."""


class _UniqueKeySafeLoader(yaml.SafeLoader):
    """Safe YAML loader that rejects duplicate and non-string mapping keys."""


def _construct_unique_mapping(
    loader: _UniqueKeySafeLoader,
    node: MappingNode,
    deep: bool = False,
) -> dict[str, object]:
    loader.flatten_mapping(node)
    construct_object: Callable[[Node, bool], object] = loader.construct_object
    result: dict[str, object] = {}
    for key_node, value_node in node.value:
        key = construct_object(key_node, deep)
        if not isinstance(key, str):
            raise yaml.constructor.ConstructorError(
                "while constructing an OpenLineage configuration",
                node.start_mark,
                "mapping keys must be strings",
                key_node.start_mark,
            )
        if key in result:
            raise yaml.constructor.ConstructorError(
                "while constructing an OpenLineage configuration",
                node.start_mark,
                "duplicate mapping key",
                key_node.start_mark,
            )
        result[key] = construct_object(value_node, deep)
    return result


_UniqueKeySafeLoader.add_constructor(
    yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG,
    _construct_unique_mapping,
)


def load_openlineage_transport_config(
    environment: Mapping[str, str],
    *,
    emission_requested: bool = True,
) -> OpenLineageTransportConfig:
    """Load one explicit bounded config file, then apply modern environment fields."""
    config_path = environment.get(_CONFIG_ENV)
    document: Mapping[str, object] | None = None
    if config_path is not None:
        document = _load_config_document(config_path)
    return parse_openlineage_transport_config(
        document,
        environment,
        emission_requested=emission_requested,
    )


def parse_openlineage_transport_config(
    document: Mapping[str, object] | None,
    environment: Mapping[str, str],
    *,
    emission_requested: bool = True,
) -> OpenLineageTransportConfig:
    """Resolve a strict transport-only mapping without opening a transport."""
    _validate_disabled(environment, emission_requested=emission_requested)
    if any(name in environment for name in _LEGACY_ENV):
        raise _configuration_error(
            "Legacy OpenLineage transport environment aliases are not supported",
            "openlineage.environment",
        )

    unknown_environment = sorted(
        name
        for name in environment
        if name.startswith("OPENLINEAGE__") and name not in _ENV_FIELDS
    )
    if unknown_environment:
        raise _configuration_error(
            "OpenLineage environment contains an unsupported configuration field",
            "openlineage.environment",
        )

    if document is None:
        root: dict[str, object] = {}
    else:
        root = _copy_string_mapping(document, location="openlineage.config")
    if set(root) - {"transport"}:
        raise _configuration_error(
            "OpenLineage configuration may contain only the transport section",
            "openlineage.config",
        )

    raw_transport = root.get("transport")
    if raw_transport is None:
        transport: dict[str, object] = {}
    else:
        transport = _copy_string_mapping(
            raw_transport,
            location="openlineage.transport",
        )
    _overlay_environment(transport, environment)
    if not transport:
        raise _configuration_error(
            "Explicit OpenLineage transport configuration is required",
            "openlineage.transport",
        )

    transport_type = transport.get("type")
    if transport_type == "file":
        return _parse_file_config(transport)
    if transport_type == "http":
        return _parse_http_config(transport)
    raise _configuration_error(
        "OpenLineage transport type must be file or http",
        "openlineage.transport.type",
    )


def _load_config_document(config_path: str) -> Mapping[str, object]:
    if not isinstance(config_path, str) or not config_path or config_path != config_path.strip():
        raise _configuration_error(
            "OPENLINEAGE_CONFIG must contain an explicit configuration file path",
            "openlineage.config",
        )
    if "\x00" in config_path:
        raise _configuration_error(
            "OPENLINEAGE_CONFIG contains an invalid configuration file path",
            "openlineage.config",
        )
    path = Path(config_path)
    try:
        if not stat.S_ISREG(path.stat().st_mode):
            raise OSError("not a regular file")
        with path.open("rb") as config_file:
            encoded = config_file.read(_MAX_CONFIG_BYTES + 1)
    except OSError:
        raise _configuration_error(
            "Could not read the explicit OpenLineage configuration file",
            "openlineage.config",
        ) from None
    if len(encoded) > _MAX_CONFIG_BYTES:
        raise _configuration_error(
            "OpenLineage configuration file exceeds the supported size limit",
            "openlineage.config",
        )
    try:
        decoded = encoded.decode("utf-8")
        _validate_yaml_complexity(decoded)
        # The custom loader subclasses SafeLoader and only tightens mapping behavior.
        candidate: object = yaml.load(
            decoded,
            Loader=_UniqueKeySafeLoader,  # noqa: S506 -- stricter SafeLoader subclass
        )
    except (RecursionError, UnicodeDecodeError, yaml.YAMLError):
        raise _configuration_error(
            "OpenLineage configuration file is not valid duplicate-free UTF-8 YAML",
            "openlineage.config",
        ) from None
    if not isinstance(candidate, Mapping):
        raise _configuration_error(
            "OpenLineage configuration file must contain a mapping",
            "openlineage.config",
        )
    return _copy_string_mapping(candidate, location="openlineage.config")


def _validate_yaml_complexity(document: str) -> None:
    """Reject aliases and bound parser work before constructing YAML objects."""
    depth = 0
    starts = (
        BlockMappingStartToken,
        BlockSequenceStartToken,
        FlowMappingStartToken,
        FlowSequenceStartToken,
    )
    ends = (BlockEndToken, FlowMappingEndToken, FlowSequenceEndToken)
    for token_count, token in enumerate(yaml.scan(document), start=1):
        if token_count > _MAX_YAML_TOKENS:
            raise yaml.YAMLError("OpenLineage configuration is too complex")
        if isinstance(token, (AliasToken, AnchorToken)):
            raise yaml.YAMLError("OpenLineage configuration aliases are not supported")
        if isinstance(token, starts):
            depth += 1
            if depth > _MAX_YAML_NESTING:
                raise yaml.YAMLError("OpenLineage configuration is nested too deeply")
        elif isinstance(token, ends):
            depth = max(0, depth - 1)


def _validate_disabled(
    environment: Mapping[str, str],
    *,
    emission_requested: bool,
) -> None:
    if _DISABLED_ENV not in environment:
        return
    disabled = environment[_DISABLED_ENV]
    if disabled not in {"true", "false"}:
        raise _configuration_error(
            "OPENLINEAGE_DISABLED must be exactly true or false",
            "openlineage.disabled",
        )
    if disabled == "true" and emission_requested:
        raise _configuration_error(
            "OPENLINEAGE_DISABLED=true conflicts with explicit OpenLineage emission",
            "openlineage.disabled",
        )


def _copy_string_mapping(value: object, *, location: str) -> dict[str, object]:
    if not isinstance(value, Mapping):
        raise _configuration_error(
            "OpenLineage configuration section must be a mapping",
            location,
        )
    result: dict[str, object] = {}
    for key, item in value.items():
        if not isinstance(key, str):
            raise _configuration_error(
                "OpenLineage configuration keys must be strings",
                location,
            )
        result[key] = item
    return result


def _overlay_environment(
    transport: dict[str, object],
    environment: Mapping[str, str],
) -> None:
    for environment_name, path in _ENV_FIELDS.items():
        if environment_name not in environment:
            continue
        raw_value = environment[environment_name]
        value: object = _coerce_environment_value(path, raw_value)
        if len(path) == 1:
            transport[path[0]] = value
            continue
        section_name, field_name = path
        existing = transport.get(section_name)
        if existing is None:
            section: dict[str, object] = {}
        else:
            section = _copy_string_mapping(
                existing,
                location=f"openlineage.transport.{section_name}",
            )
        section[field_name] = value
        transport[section_name] = section


def _coerce_environment_value(path: tuple[str, ...], value: str) -> object:
    location = f"openlineage.transport.{'.'.join(path)}"
    if path == ("timeout",):
        try:
            return float(value)
        except ValueError:
            raise _configuration_error(
                "OpenLineage HTTP timeout must be numeric",
                location,
            ) from None
    if path == ("verify",):
        if value == "true":
            return True
        if value == "false":
            return False
        raise _configuration_error(
            "OpenLineage HTTP verify must be exactly true or false",
            location,
        )
    if path == ("retry", "total"):
        if _INTEGER.fullmatch(value) is None:
            raise _configuration_error(
                "OpenLineage HTTP retry total must be an integer",
                location,
            )
        return int(value)
    return value


def _parse_file_config(transport: Mapping[str, object]) -> FileTransportConfig:
    if set(transport) != {"type", "log_file_path"}:
        raise _configuration_error(
            "OpenLineage File transport accepts only type and log_file_path",
            "openlineage.transport",
        )
    value = transport.get("log_file_path")
    if (
        not isinstance(value, str)
        or not value
        or value != value.strip()
        or any(unicodedata.category(character) == "Cc" for character in value)
        or _URI_PREFIX.match(value) is not None
        or value.startswith("file:")
        or value.startswith("//")
        or value.startswith("\\\\")
    ):
        raise _configuration_error(
            "OpenLineage File transport requires one explicit local path",
            "openlineage.transport.log_file_path",
        )
    return FileTransportConfig(log_file_path=Path(value))


def _parse_http_config(transport: Mapping[str, object]) -> HttpTransportConfig:
    allowed = {"type", "url", "endpoint", "timeout", "verify", "retry", "auth"}
    if set(transport) - allowed:
        raise _configuration_error(
            "OpenLineage HTTP transport contains an unsupported field",
            "openlineage.transport",
        )
    url = _validate_http_url(transport.get("url"))
    endpoint = _validate_http_endpoint(transport.get("endpoint"))
    timeout = _validate_timeout(transport.get("timeout", 5.0))
    verify = transport.get("verify", True)
    if verify is not True or not isinstance(verify, bool):
        raise _configuration_error(
            "OpenLineage HTTP transport requires TLS verification",
            "openlineage.transport.verify",
        )
    retry_total = _validate_retry(transport.get("retry"))
    api_key = _validate_auth(transport.get("auth"), url=url)
    return HttpTransportConfig(
        url=url,
        endpoint=endpoint,
        timeout_seconds=timeout,
        verify=True,
        retry_total=retry_total,
        api_key=api_key,
    )


def _validate_http_url(value: object) -> str:
    if (
        not isinstance(value, str)
        or not value
        or value != value.strip()
        or any(ord(character) < 0x21 for character in value)
        or "?" in value
        or "#" in value
    ):
        raise _configuration_error(
            "OpenLineage HTTP URL must be an absolute credential-free HTTP(S) URL",
            "openlineage.transport.url",
        )
    try:
        parsed = urlsplit(value)
        port = parsed.port
    except ValueError:
        raise _configuration_error(
            "OpenLineage HTTP URL must contain a valid authority",
            "openlineage.transport.url",
        ) from None
    if (
        parsed.scheme not in {"http", "https"}
        or not parsed.netloc
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
        or "\\" in value
        or port == 0
    ):
        raise _configuration_error(
            "OpenLineage HTTP URL must be an absolute credential-free HTTP(S) URL",
            "openlineage.transport.url",
        )
    host = parsed.hostname
    if host is None:  # Defensive narrowing after the authority validation above.
        raise _configuration_error(
            "OpenLineage HTTP URL must contain a host",
            "openlineage.transport.url",
        )
    if ":" in host:
        try:
            parsed_host = ip_address(host)
        except ValueError:
            raise _configuration_error(
                "OpenLineage HTTP URL contains an invalid IPv6 authority",
                "openlineage.transport.url",
            ) from None
        if parsed_host.version != 6 or "%" in host:
            raise _configuration_error(
                "OpenLineage HTTP URL contains an invalid IPv6 authority",
                "openlineage.transport.url",
            )
    if parsed.scheme == "http" and not _is_loopback_host(host):
        raise _configuration_error(
            "Plain HTTP OpenLineage transport is limited to a loopback host",
            "openlineage.transport.url",
        )
    return value


def _is_loopback_host(host: str) -> bool:
    if host.lower() == "localhost":
        return True
    try:
        return ip_address(host).is_loopback
    except ValueError:
        return False


def _validate_http_endpoint(value: object) -> str:
    if (
        not isinstance(value, str)
        or not value
        or value != value.strip()
        or value.startswith(("/", "\\"))
        or "?" in value
        or "#" in value
        or any(ord(character) < 0x21 for character in value)
    ):
        raise _configuration_error(
            "OpenLineage HTTP endpoint must be an explicit relative path",
            "openlineage.transport.endpoint",
        )
    parsed = urlsplit(value)
    direct_segments = value.split("/")
    unsafe_escape = any(
        chr(int(match.group(1), 16)) in {".", "/", "\\", "%"}
        for match in _PERCENT_ESCAPE.finditer(value)
    )
    if (
        parsed.scheme
        or parsed.netloc
        or parsed.query
        or parsed.fragment
        or unsafe_escape
        or any(segment in {".", ".."} or "\\" in segment for segment in direct_segments)
    ):
        raise _configuration_error(
            "OpenLineage HTTP endpoint must be an explicit relative path without traversal",
            "openlineage.transport.endpoint",
        )
    return value


def _validate_timeout(value: object) -> float:
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(value)
        or value <= 0
        or value > 5
    ):
        raise _configuration_error(
            "OpenLineage HTTP timeout must be finite, positive, and at most five seconds",
            "openlineage.transport.timeout",
        )
    return float(value)


def _validate_retry(value: object) -> Literal[0, 1]:
    if value is None:
        return 0
    retry = _copy_string_mapping(value, location="openlineage.transport.retry")
    if set(retry) != {"total"}:
        raise _configuration_error(
            "OpenLineage HTTP retry accepts only total",
            "openlineage.transport.retry",
        )
    total = retry["total"]
    if isinstance(total, bool) or not isinstance(total, int) or total not in {0, 1}:
        raise _configuration_error(
            "OpenLineage HTTP retry total must be zero or one",
            "openlineage.transport.retry.total",
        )
    return 0 if total == 0 else 1


def _validate_auth(value: object, *, url: str) -> SecretStr | None:
    if value is None:
        return None
    auth = _copy_string_mapping(value, location="openlineage.transport.auth")
    if set(auth) != {"type", "apiKey"} or auth.get("type") != "api_key":
        raise _configuration_error(
            "OpenLineage HTTP auth supports only api_key with apiKey",
            "openlineage.transport.auth",
        )
    api_key = auth.get("apiKey")
    if (
        not isinstance(api_key, str)
        or not api_key
        or api_key != api_key.strip()
        or any(ord(character) < 0x21 for character in api_key)
    ):
        raise _configuration_error(
            "OpenLineage HTTP apiKey must be a nonblank single-line value",
            "openlineage.transport.auth.apiKey",
        )
    if urlsplit(url).scheme != "https":
        raise _configuration_error(
            "OpenLineage HTTP api_key authentication requires HTTPS",
            "openlineage.transport.auth",
        )
    return SecretStr(api_key)


def _configuration_error(
    message: str,
    location: str,
) -> OpenLineageTransportConfigurationError:
    return OpenLineageTransportConfigurationError(message, location=location)


def create_openlineage_transport(
    config: OpenLineageTransportConfig,
) -> OpenLineageTransport:
    """Create one explicit synchronous transport without an implicit fallback."""
    if isinstance(config, FileTransportConfig):
        if not isinstance(config.log_file_path, Path):
            raise _configuration_error(
                "OpenLineage File transport requires one explicit local path",
                "openlineage.transport.log_file_path",
            )
        validated_file = _parse_file_config({
            "type": "file",
            "log_file_path": str(config.log_file_path),
        })
        return _FileOpenLineageTransport(validated_file)
    if isinstance(config, HttpTransportConfig):
        auth: dict[str, object] | None = None
        if config.api_key is not None:
            if not isinstance(config.api_key, SecretStr):
                raise _configuration_error(
                    "OpenLineage HTTP apiKey must use the validated secret type",
                    "openlineage.transport.auth.apiKey",
                )
            auth = {
                "type": "api_key",
                "apiKey": config.api_key.get_secret_value(),
            }
        validated_http = _parse_http_config({
            "type": "http",
            "url": config.url,
            "endpoint": config.endpoint,
            "timeout": config.timeout_seconds,
            "verify": config.verify,
            "retry": {"total": config.retry_total},
            "auth": auth,
        })
        return _HttpOpenLineageTransport(validated_http)
    raise _configuration_error(
        "OpenLineage transport configuration type is not supported",
        "openlineage.transport.type",
    )


def _serialize_event_line(event: Mapping[str, object]) -> bytes:
    """Validate before I/O and return one canonical UTF-8 JSON line."""
    from streamt.integrations.openlineage.validation import (
        OpenLineageValidationError,
        validate_event,
    )

    validate_event(event)
    try:
        rendered = json.dumps(
            event,
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        )
    except (TypeError, ValueError):
        raise OpenLineageValidationError(
            "Validated OpenLineage event could not be serialized canonically"
        ) from None
    return (rendered + "\n").encode("utf-8")


class _FileOpenLineageTransport:
    """Unbuffered append-only local File transport with per-event durability."""

    def __init__(self, config: FileTransportConfig) -> None:
        self._path = config.log_file_path
        self._descriptor: int | None = None
        self._closed = False

    def emit(self, event: Mapping[str, object]) -> None:
        payload = _serialize_event_line(event)
        if self._closed:
            raise OpenLineageDeliveryError("OpenLineage File transport is closed")
        descriptor = self._descriptor
        if descriptor is None:
            descriptor = _open_file_descriptor(self._path)
            self._descriptor = descriptor

        position = 0
        failed = False
        try:
            while position < len(payload):
                written = os.write(descriptor, payload[position:])
                if written <= 0:
                    failed = True
                    break
                position += written
            if not failed:
                _sync_descriptor(descriptor)
        except OSError:
            failed = True
        if failed:
            raise OpenLineageDeliveryError("OpenLineage File event delivery failed")

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        descriptor = self._descriptor
        self._descriptor = None
        if descriptor is None:
            return
        failed = False
        try:
            os.close(descriptor)
        except OSError:
            failed = True
        if failed:
            raise OpenLineageDeliveryError("OpenLineage File transport close failed")


def _open_file_descriptor(path: Path) -> int:
    flags = os.O_APPEND | os.O_CREAT | os.O_WRONLY
    flags |= getattr(os, "O_CLOEXEC", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0)
    descriptor: int | None = None
    failed = False
    try:
        descriptor = os.open(path, flags, 0o600)
        if not stat.S_ISREG(os.fstat(descriptor).st_mode):
            failed = True
    except OSError:
        failed = True
    if failed:
        if descriptor is not None:
            _close_descriptor_quietly(descriptor)
        raise OpenLineageDeliveryError(
            "OpenLineage File transport could not be initialized"
        )
    if descriptor is None:  # Defensive narrowing for alternate os implementations.
        raise OpenLineageDeliveryError(
            "OpenLineage File transport could not be initialized"
        )
    return descriptor


def _sync_descriptor(descriptor: int) -> None:
    sync = getattr(os, "fdatasync", os.fsync)
    sync(descriptor)


def _close_descriptor_quietly(descriptor: int) -> None:
    try:
        os.close(descriptor)
    except OSError:
        return


class _HttpOpenLineageTransport:
    """Dedicated synchronous HTTP transport with explicit retry behavior."""

    def __init__(self, config: HttpTransportConfig) -> None:
        session: requests.Session | None = None
        failed = False
        try:
            session = requests.Session()
            session.trust_env = False
            session.verify = True
            session.mount("http://", HTTPAdapter(max_retries=0))
            session.mount("https://", HTTPAdapter(max_retries=0))
        except Exception:
            failed = True
        if failed:
            if session is not None:
                _close_session_quietly(session)
            raise OpenLineageDeliveryError(
                "OpenLineage HTTP transport could not be initialized"
            )
        if session is None:  # Defensive narrowing for alternate Session factories.
            raise OpenLineageDeliveryError(
                "OpenLineage HTTP transport could not be initialized"
            )
        self._session: requests.Session | None = session
        self._url = f"{config.url.rstrip('/')}/{config.endpoint}"
        self._timeout = config.timeout_seconds
        self._retry_total = config.retry_total
        self._api_key = config.api_key

    def emit(self, event: Mapping[str, object]) -> None:
        payload = _serialize_event_line(event)
        session = self._session
        if session is None:
            raise OpenLineageDeliveryError("OpenLineage HTTP transport is closed")

        headers = {"Content-Type": "application/json"}
        if self._api_key is not None:
            headers["Authorization"] = f"Bearer {self._api_key.get_secret_value()}"

        for attempt in range(self._retry_total + 1):
            response: requests.Response | None = None
            failure: Literal["timeout", "connection", "request"] | None = None
            status_code: int | None = None
            try:
                response = session.post(
                    self._url,
                    data=payload,
                    headers=headers,
                    timeout=self._timeout,
                    verify=True,
                    allow_redirects=False,
                    stream=True,
                )
                candidate_status = response.status_code
                if (
                    isinstance(candidate_status, int)
                    and not isinstance(candidate_status, bool)
                    and 100 <= candidate_status <= 599
                ):
                    status_code = candidate_status
                else:
                    failure = "request"
            except requests.Timeout:
                failure = "timeout"
            except requests.ConnectionError:
                failure = "connection"
            except requests.RequestException:
                failure = "request"
            except Exception:
                failure = "request"

            close_failed = _close_response(response)
            if close_failed:
                raise OpenLineageDeliveryError(
                    "OpenLineage HTTP response close failed"
                )

            retry_available = attempt < self._retry_total
            if failure is not None:
                if retry_available and failure in {"timeout", "connection"}:
                    continue
                raise OpenLineageDeliveryError(_http_failure_message(failure))
            if status_code is None:
                raise OpenLineageDeliveryError("OpenLineage HTTP delivery failed")
            if 200 <= status_code < 300:
                return
            if retry_available and status_code in _RETRYABLE_HTTP_STATUS:
                continue
            raise OpenLineageDeliveryError(
                f"OpenLineage HTTP delivery returned status {status_code}"
            )

        raise OpenLineageDeliveryError("OpenLineage HTTP delivery failed")

    def close(self) -> None:
        session = self._session
        if session is None:
            return
        self._session = None
        failed = False
        try:
            session.close()
        except Exception:
            failed = True
        if failed:
            raise OpenLineageDeliveryError("OpenLineage HTTP transport close failed")


def _close_response(response: requests.Response | None) -> bool:
    if response is None:
        return False
    try:
        response.close()
    except Exception:
        return True
    return False


def _close_session_quietly(session: requests.Session) -> None:
    try:
        session.close()
    except Exception:
        return


def _http_failure_message(
    failure: Literal["timeout", "connection", "request"],
) -> str:
    if failure == "timeout":
        return "OpenLineage HTTP delivery timed out"
    if failure == "connection":
        return "OpenLineage HTTP delivery could not connect"
    return "OpenLineage HTTP request failed"
