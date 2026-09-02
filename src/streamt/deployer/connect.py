"""Kafka Connect deployer for connector management."""

from __future__ import annotations

import hashlib
import json
import logging
import math
import re
import time
from collections.abc import Mapping
from dataclasses import dataclass, field, replace
from typing import Optional
from urllib.parse import quote, urlsplit

import requests

from streamt.compiler.manifest import ConnectorArtifact

logger = logging.getLogger(__name__)

# Default timeouts (in seconds)
DEFAULT_TIMEOUT = 30
HEALTH_CHECK_TIMEOUT = 10
_MAX_MANAGED_CONNECTOR_RESPONSE_BYTES = 1024 * 1024
_MANAGED_CONNECTOR_CHUNK_BYTES = 64 * 1024
_FINGERPRINT_PREFIX = "sha256:"
_CONNECT_BINDING_VERSION = 1
_CLUSTER_ALIAS = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
_CONNECT_BACKEND_IDENTITY = re.compile(
    r"^kafka-connect:v1:[A-Za-z0-9][A-Za-z0-9._-]{0,127}:sha256:[0-9a-f]{64}$"
)
_PRESENTABLE_CONFIG_KEY = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")


class ConnectClusterBindingError(ValueError):
    """A Kafka Connect runtime cannot be bound to a canonical cluster identity."""


class ConnectManagedObservationError(RuntimeError):
    """A strict managed-connector observation could not be proven complete."""


class _InvalidManagedConnectorJSONError(ValueError):
    """Internal marker for non-canonical managed-observation JSON."""


def _sha256(value: str) -> str:
    return f"{_FINGERPRINT_PREFIX}{hashlib.sha256(value.encode('utf-8')).hexdigest()}"


def _is_fingerprint(value: object) -> bool:
    if not isinstance(value, str) or not value.startswith(_FINGERPRINT_PREFIX):
        return False
    digest = value.removeprefix(_FINGERPRINT_PREFIX)
    return len(digest) == 64 and all(
        character in "0123456789abcdef" for character in digest
    )


def _reject_duplicate_json_keys(
    pairs: list[tuple[str, object]],
) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise _InvalidManagedConnectorJSONError
        result[key] = value
    return result


def _reject_nonfinite_json_constant(_value: str) -> object:
    raise _InvalidManagedConnectorJSONError


def _normalize_rest_endpoint(rest_url: str) -> str:
    """Normalize a REST endpoint without ever echoing it in validation errors."""
    if not isinstance(rest_url, str) or not rest_url or rest_url != rest_url.strip():
        raise ConnectClusterBindingError("Invalid Connect REST URL: endpoint is malformed")
    if any(character.isspace() for character in rest_url):
        raise ConnectClusterBindingError("Invalid Connect REST URL: endpoint is malformed")
    if "?" in rest_url or "#" in rest_url:
        raise ConnectClusterBindingError(
            "Invalid Connect REST URL: query strings and fragments are not allowed"
        )
    try:
        parsed = urlsplit(rest_url)
        port = parsed.port
    except ValueError:
        raise ConnectClusterBindingError(
            "Invalid Connect REST URL: endpoint is malformed"
        ) from None
    scheme = parsed.scheme.lower()
    if scheme not in {"http", "https"} or not parsed.hostname:
        raise ConnectClusterBindingError(
            "Invalid Connect REST URL: endpoint must use http or https"
        )
    if "@" in parsed.netloc or parsed.username is not None or parsed.password is not None:
        raise ConnectClusterBindingError(
            "Invalid Connect REST URL: endpoint user information is not allowed"
        )
    try:
        hostname = parsed.hostname.encode("idna").decode("ascii").lower()
    except UnicodeError:
        raise ConnectClusterBindingError(
            "Invalid Connect REST URL: endpoint hostname is malformed"
        ) from None
    if ":" in hostname:
        hostname = f"[{hostname}]"
    default_port = 80 if scheme == "http" else 443
    authority = hostname if port in (None, default_port) else f"{hostname}:{port}"
    path = parsed.path.rstrip("/")
    return f"{scheme}://{authority}{path}"


@dataclass(frozen=True)
class ConnectClusterBinding:
    """Versioned non-secret identity for one effective Kafka Connect cluster."""

    cluster_alias: str
    endpoint_fingerprint: str
    version: int = _CONNECT_BINDING_VERSION

    def __post_init__(self) -> None:
        if type(self.version) is not int or self.version != _CONNECT_BINDING_VERSION:
            raise ConnectClusterBindingError("Unsupported Connect cluster binding version")
        if not isinstance(self.cluster_alias, str) or not _CLUSTER_ALIAS.fullmatch(
            self.cluster_alias
        ):
            raise ConnectClusterBindingError("Invalid Connect cluster alias")
        if not _is_fingerprint(self.endpoint_fingerprint):
            raise ConnectClusterBindingError("Invalid Connect endpoint fingerprint")

    @classmethod
    def from_endpoint(cls, cluster_alias: str, rest_url: str) -> ConnectClusterBinding:
        """Bind an alias to the canonical fingerprint of a normalized endpoint."""
        normalized_endpoint = _normalize_rest_endpoint(rest_url)
        return cls(
            cluster_alias=cluster_alias,
            endpoint_fingerprint=_sha256(normalized_endpoint),
        )

    @property
    def backend_identity(self) -> str:
        """Return the canonical, endpoint-free backend identity string."""
        return (
            f"kafka-connect:v{self.version}:{self.cluster_alias}:"
            f"{self.endpoint_fingerprint}"
        )


def is_connect_backend_identity(value: object) -> bool:
    """Whether a value is one exact canonical Kafka Connect backend identity."""
    return isinstance(value, str) and _CONNECT_BACKEND_IDENTITY.fullmatch(value) is not None


def bind_connector_artifact(
    artifact: ConnectorArtifact,
    binding: ConnectClusterBinding,
) -> ConnectorArtifact:
    """Purely resolve one artifact to an exact Kafka Connect cluster binding."""
    if not isinstance(binding, ConnectClusterBinding):
        raise ConnectClusterBindingError("Connector artifact resolution requires a valid binding")
    if artifact.cluster is not None and artifact.cluster != binding.cluster_alias:
        raise ConnectClusterBindingError(
            "Connector artifact cluster does not match the effective Connect cluster "
            "(bound Kafka Connect cluster)"
        )
    ownership = (
        dict(artifact.ownership) if isinstance(artifact.ownership, dict) else artifact.ownership
    )
    return replace(
        artifact,
        topics=list(artifact.topics),
        config=dict(artifact.config),
        cluster=binding.cluster_alias,
        ownership=ownership,
    )


def resolve_connector_artifact(
    artifact: ConnectorArtifact,
    binding: ConnectClusterBinding,
) -> ConnectorArtifact:
    """Compatibility name for pure offline and bound artifact resolution."""
    return bind_connector_artifact(artifact, binding)


ConnectorConfigScalar = str | bool | int | float


@dataclass(frozen=True, eq=False)
class ManagedConnectorObservation:
    """Immutable strict observation used as future managed-resource evidence."""

    binding: ConnectClusterBinding
    name: str
    exists: bool
    config: tuple[tuple[str, ConnectorConfigScalar], ...] = field(
        default_factory=tuple,
        repr=False,
    )

    def __post_init__(self) -> None:
        if not isinstance(self.binding, ConnectClusterBinding):
            raise ConnectManagedObservationError(
                "Kafka Connect managed observation has an invalid cluster binding"
            )
        if not isinstance(self.name, str) or not self.name.strip():
            raise ConnectManagedObservationError(
                "Kafka Connect managed observation has an invalid connector identity"
            )
        if not isinstance(self.exists, bool) or not isinstance(self.config, tuple):
            raise ConnectManagedObservationError(
                "Kafka Connect managed observation has an invalid shape"
            )
        if not self.exists:
            if self.config:
                raise ConnectManagedObservationError(
                    "Absent Kafka Connect managed observation cannot contain config"
                )
            return
        if not self.config:
            raise ConnectManagedObservationError(
                "Present Kafka Connect managed observation requires complete config"
            )

        prior_key: str | None = None
        config_name: object = None
        for entry in self.config:
            if not isinstance(entry, tuple) or len(entry) != 2:
                raise ConnectManagedObservationError(
                    "Kafka Connect managed observation config has an invalid shape"
                )
            key, value = entry
            if not isinstance(key, str) or not key.strip():
                raise ConnectManagedObservationError(
                    "Kafka Connect managed observation config has an invalid key"
                )
            if prior_key is not None and key <= prior_key:
                raise ConnectManagedObservationError(
                    "Kafka Connect managed observation config is not canonical"
                )
            prior_key = key
            if type(value) not in (str, bool, int) and not (
                isinstance(value, float) and math.isfinite(value)
            ):
                raise ConnectManagedObservationError(
                    "Kafka Connect managed observation config has a non-finite or non-scalar value"
                )
            if key == "name":
                config_name = value
        if not isinstance(config_name, str) or config_name != self.name:
            raise ConnectManagedObservationError(
                "Kafka Connect managed observation config identity is mismatched"
            )

    def _canonical_json(self) -> str:
        return json.dumps(
            {
                "binding": self.binding.backend_identity,
                "config": self.config,
                "exists": self.exists,
                "name": self.name,
            },
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        )

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, ManagedConnectorObservation):
            return NotImplemented
        return self._canonical_json() == other._canonical_json()

    def __hash__(self) -> int:
        return hash(self._canonical_json())

    @property
    def fingerprint(self) -> str:
        """Fingerprint stable identity, presence, and exact config only."""
        return _sha256(self._canonical_json())

    def config_dict(self) -> dict[str, ConnectorConfigScalar]:
        """Return a mutable copy of the observed immutable configuration."""
        return dict(self.config)


def _config_change_evidence(
    *,
    from_present: bool,
    to_present: bool,
) -> dict[str, object]:
    if not from_present and to_present:
        direction = "added"
    elif from_present and not to_present:
        direction = "removed"
    else:
        direction = "changed"

    return {
        "change": direction,
        "from_present": from_present,
        "to_present": to_present,
    }


def _secret_neutral_change(value: object) -> dict[str, object] | None:
    if not isinstance(value, Mapping):
        return None
    change = value.get("change")
    from_present = value.get("from_present")
    to_present = value.get("to_present")
    if (
        change not in {"added", "changed", "removed"}
        or type(from_present) is not bool
        or type(to_present) is not bool
    ):
        return None
    return {
        "change": change,
        "from_present": from_present,
        "to_present": to_present,
    }


def _presented_config_keys(keys: set[str]) -> list[tuple[str, str]]:
    presented: list[tuple[str, str]] = []
    unsafe_ordinal = 0
    for key in sorted(keys):
        if _PRESENTABLE_CONFIG_KEY.fullmatch(key):
            presented.append((key, key))
            continue
        unsafe_ordinal += 1
        presented.append((key, f"<unsafe-config-key-{unsafe_ordinal}>"))
    return presented


def secret_neutral_connector_changes(changes: object) -> dict[str, dict[str, object]]:
    """Normalize connector diffs to changed keys and non-revealing evidence only."""
    if not isinstance(changes, Mapping):
        return {}

    keys = {key for key in changes if isinstance(key, str)}
    neutral: dict[str, dict[str, object]] = {}
    for key, presented_key in _presented_config_keys(keys):
        delta = changes[key]
        normalized = _secret_neutral_change(delta)
        if normalized is not None:
            neutral[presented_key] = normalized
            continue
        if isinstance(delta, Mapping) and "from" in delta and "to" in delta:
            from_value = delta["from"]
            to_value = delta["to"]
            from_present = from_value is not None
            to_present = to_value is not None
            if from_value is None and to_value is None:
                from_present = to_present = True
            neutral[presented_key] = _config_change_evidence(
                from_present=from_present,
                to_present=to_present,
            )
            continue

        # Unknown legacy evidence is never passed through. Its key still tells
        # the reviewer what changed without serializing an arbitrary value.
        neutral[presented_key] = {
            "change": "changed",
            "from_present": False,
            "to_present": False,
        }
    return neutral


def secret_neutral_connector_config_diff(
    current_config: Mapping[str, object],
    desired_config: Mapping[str, object],
) -> dict[str, dict[str, object]]:
    """Return an exact typed config diff containing no raw values."""
    keys = set(current_config) | set(desired_config)
    if any(not isinstance(key, str) for key in keys):
        raise ConnectManagedObservationError(
            "Kafka Connect managed config evidence has an invalid key"
        )
    changes: dict[str, dict[str, object]] = {}
    for key, presented_key in _presented_config_keys(keys):
        current_present = key in current_config
        desired_present = key in desired_config
        current_value = current_config.get(key)
        desired_value = desired_config.get(key)
        if (
            current_present
            and desired_present
            and type(current_value) is type(desired_value)
            and current_value == desired_value
        ):
            continue
        changes[presented_key] = _config_change_evidence(
            from_present=current_present,
            to_present=desired_present,
        )
    return changes


@dataclass
class ConnectorState:
    """Current state of a connector."""

    name: str
    exists: bool
    config: Optional[dict] = None
    status: Optional[str] = None
    tasks: list[dict] = None

    def __post_init__(self) -> None:
        if self.tasks is None:
            self.tasks = []


@dataclass
class ConnectorChange:
    """A change to apply to a connector."""

    connector_name: str
    action: str  # create, update, delete, none
    current: ConnectorState | ManagedConnectorObservation | None = field(
        default=None,
        repr=False,
    )
    desired: Optional[ConnectorArtifact] = field(default=None, repr=False)
    changes: dict = None
    backend_identity: str | None = None

    def __post_init__(self) -> None:
        self.changes = secret_neutral_connector_changes(self.changes or {})
        if self.backend_identity is not None and not is_connect_backend_identity(
            self.backend_identity
        ):
            raise ConnectClusterBindingError("Connector change has an invalid backend identity")


class ConnectDeployer:
    """Deployer for Kafka Connect connectors.

    Supports context manager protocol for proper resource cleanup:

        with ConnectDeployer(rest_url) as deployer:
            deployer.list_connectors()
    """

    def __init__(
        self,
        rest_url: str,
        username: Optional[str] = None,
        password: Optional[str] = None,
        ssl_ca_location: Optional[str] = None,
        ssl_certificate_location: Optional[str] = None,
        ssl_key_location: Optional[str] = None,
        ssl_key_password: Optional[str] = None,
        cluster_alias: Optional[str] = None,
    ) -> None:
        """Initialize Connect deployer."""
        from streamt.deployer.ssl_utils import configure_session_ssl

        self.rest_url = _normalize_rest_endpoint(rest_url)
        self.cluster_binding = (
            ConnectClusterBinding.from_endpoint(cluster_alias, self.rest_url)
            if cluster_alias is not None
            else None
        )
        self._closed = False
        self._http_session = requests.Session()
        if username and password:
            self._http_session.auth = (username, password)
        configure_session_ssl(
            self._http_session,
            ssl_ca_location=ssl_ca_location,
            ssl_certificate_location=ssl_certificate_location,
            ssl_key_location=ssl_key_location,
            ssl_key_password=ssl_key_password,
        )

    @property
    def backend_identity(self) -> str:
        """Return the canonical identity of the configured bound cluster."""
        return self.require_cluster_binding().backend_identity

    def require_cluster_binding(self) -> ConnectClusterBinding:
        """Return the exact binding or fail without exposing runtime configuration."""
        if self.cluster_binding is None:
            raise ConnectClusterBindingError(
                "Kafka Connect backend identity requires an effective cluster binding"
            )
        return self.cluster_binding

    def resolve_connector_artifact(self, artifact: ConnectorArtifact) -> ConnectorArtifact:
        """Purely resolve one artifact to this deployer's exact cluster binding."""
        return resolve_connector_artifact(artifact, self.require_cluster_binding())

    def __enter__(self) -> ConnectDeployer:
        """Enter context manager."""
        return self

    def __exit__(self, exc_type: object, exc_val: object, exc_tb: object) -> None:
        """Exit context manager, cleaning up resources."""
        self.close()

    def close(self) -> None:
        """Close the deployer and clean up resources."""
        self._closed = True
        self._http_session.close()

    def _request(
        self,
        method: str,
        endpoint: str,
        timeout: int = DEFAULT_TIMEOUT,
        **kwargs: object,
    ) -> dict | list | None:
        """Make a request to Connect REST API. Returns parsed JSON.

        Raises on HTTP errors.
        """
        if self._closed:
            raise RuntimeError("ConnectDeployer is closed")
        url = f"{self.rest_url}{endpoint}"
        last_err: Optional[Exception] = None
        for attempt in range(3):
            try:
                response = self._http_session.request(method, url, timeout=timeout, **kwargs)
                status_code = getattr(response, "status_code", 200)
                if isinstance(status_code, int) and status_code >= 500 and attempt < 2:
                    last_err = requests.HTTPError(response=response)
                    time.sleep(0.5 * (attempt + 1))
                    continue
                break
            except (requests.ConnectionError, requests.Timeout) as e:
                last_err = e
                if attempt < 2:
                    time.sleep(0.5 * (attempt + 1))
        else:
            raise last_err  # type: ignore[misc]
        response.raise_for_status()
        if response.status_code == 204 or not response.content:
            return None
        return response.json()

    def check_connection(self) -> bool:
        """Check if Connect cluster is accessible."""
        try:
            self._request("GET", "/", timeout=HEALTH_CHECK_TIMEOUT)
            return True
        except Exception as e:
            logger.debug(f"Connect connection check failed: {e}")
            return False

    def list_connectors(self) -> list[str]:
        """List all connectors."""
        return self._request("GET", "/connectors")

    @staticmethod
    def _connector_path(connector_name: str) -> str:
        """Encode an exact connector name as one URL path segment."""
        return quote(connector_name, safe="")

    @staticmethod
    def _strict_config(
        config: object,
        *,
        connector_name: str,
    ) -> tuple[tuple[str, ConnectorConfigScalar], ...]:
        if not isinstance(config, dict):
            raise ConnectManagedObservationError(
                "Kafka Connect managed observation response has no complete config object"
            )
        if config.get("name") != connector_name or not isinstance(config.get("name"), str):
            raise ConnectManagedObservationError(
                "Kafka Connect managed observation response has mismatched config identity"
            )

        normalized: list[tuple[str, ConnectorConfigScalar]] = []
        for key, value in config.items():
            if not isinstance(key, str) or not key.strip():
                raise ConnectManagedObservationError(
                    "Kafka Connect managed observation config has an invalid key"
                )
            if type(value) in (str, bool, int):
                normalized.append((key, value))
                continue
            if isinstance(value, float) and math.isfinite(value):
                normalized.append((key, value))
                continue
            raise ConnectManagedObservationError(
                "Kafka Connect managed observation config has a non-finite or non-scalar value"
            )
        return tuple(sorted(normalized))

    @staticmethod
    def _read_managed_observation_body(response: object) -> bytes:
        """Read one response body with a hard decoded-byte ceiling."""
        headers = getattr(response, "headers", None)
        declared_length: object = None
        if isinstance(headers, Mapping):
            declared_length = headers.get("Content-Length")
        if declared_length is not None:
            try:
                parsed_length = int(declared_length)
            except (TypeError, ValueError):
                raise ConnectManagedObservationError(
                    "Kafka Connect managed observation response has invalid size metadata"
                ) from None
            if parsed_length < 0 or parsed_length > _MAX_MANAGED_CONNECTOR_RESPONSE_BYTES:
                raise ConnectManagedObservationError(
                    "Kafka Connect managed observation response is oversized"
                )

        iter_content = getattr(response, "iter_content", None)
        if not callable(iter_content):
            raise ConnectManagedObservationError(
                "Kafka Connect managed observation response body is unavailable"
            )
        body = bytearray()
        try:
            chunks = iter_content(chunk_size=_MANAGED_CONNECTOR_CHUNK_BYTES)
            for chunk in chunks:
                if not isinstance(chunk, bytes):
                    raise ConnectManagedObservationError(
                        "Kafka Connect managed observation response body is malformed"
                    )
                if len(body) + len(chunk) > _MAX_MANAGED_CONNECTOR_RESPONSE_BYTES:
                    raise ConnectManagedObservationError(
                        "Kafka Connect managed observation response is oversized"
                    )
                body.extend(chunk)
        except ConnectManagedObservationError:
            raise
        except Exception:
            raise ConnectManagedObservationError(
                "Kafka Connect managed observation response body could not be read"
            ) from None
        return bytes(body)

    @staticmethod
    def _decode_managed_observation(body: bytes) -> object:
        try:
            return json.loads(
                body.decode("utf-8"),
                object_pairs_hook=_reject_duplicate_json_keys,
                parse_constant=_reject_nonfinite_json_constant,
            )
        except (
            UnicodeDecodeError,
            json.JSONDecodeError,
            _InvalidManagedConnectorJSONError,
        ):
            raise ConnectManagedObservationError(
                "Kafka Connect managed observation response is not canonical JSON"
            ) from None

    def observe_managed_connector(self, connector_name: str) -> ManagedConnectorObservation:
        """Strictly observe one connector with one immutable, identity-bound GET."""
        binding = self.cluster_binding
        if binding is None:
            raise ConnectClusterBindingError(
                "Managed Kafka Connect observation requires an effective cluster binding"
            )
        if not isinstance(connector_name, str) or not connector_name.strip():
            raise ConnectManagedObservationError(
                "Managed Kafka Connect observation requires a non-empty connector name"
            )
        if self._closed:
            raise ConnectManagedObservationError("Kafka Connect managed observation is closed")

        endpoint = f"/connectors/{self._connector_path(connector_name)}"
        try:
            response = self._http_session.request(
                "GET",
                f"{self.rest_url}{endpoint}",
                timeout=DEFAULT_TIMEOUT,
                allow_redirects=False,
                stream=True,
            )
        except Exception:
            raise ConnectManagedObservationError(
                "Kafka Connect managed observation request failed"
            ) from None
        try:
            status_code = getattr(response, "status_code", None)
            if status_code == 404:
                return ManagedConnectorObservation(
                    binding=binding,
                    name=connector_name,
                    exists=False,
                )
            if status_code in {401, 403}:
                raise ConnectManagedObservationError(
                    "Kafka Connect managed observation authorization failed"
                )
            if not isinstance(status_code, int) or not 200 <= status_code < 300:
                raise ConnectManagedObservationError(
                    "Kafka Connect managed observation request returned an invalid status"
                )
            data = self._decode_managed_observation(self._read_managed_observation_body(response))
        finally:
            try:
                response.close()
            except Exception:
                pass
        if not isinstance(data, dict):
            raise ConnectManagedObservationError(
                "Kafka Connect managed observation response is not an object"
            )
        if data.get("name") != connector_name or not isinstance(data.get("name"), str):
            raise ConnectManagedObservationError(
                "Kafka Connect managed observation response has mismatched connector identity"
            )
        config = self._strict_config(data.get("config"), connector_name=connector_name)
        return ManagedConnectorObservation(
            binding=binding,
            name=connector_name,
            exists=True,
            config=config,
        )

    def get_connector_state(self, connector_name: str) -> ConnectorState:
        """Get current state of a connector."""
        encoded_name = self._connector_path(connector_name)
        try:
            config = self._request("GET", f"/connectors/{encoded_name}/config")
            status = self._request("GET", f"/connectors/{encoded_name}/status")

            return ConnectorState(
                name=connector_name,
                exists=True,
                config=config,
                status=status.get("connector", {}).get("state"),
                tasks=status.get("tasks", []),
            )
        except requests.HTTPError as e:
            if e.response.status_code == 404:
                return ConnectorState(name=connector_name, exists=False)
            raise

    def create_connector(self, artifact: ConnectorArtifact) -> dict:
        """Create a new connector."""
        payload = {
            "name": artifact.name,
            "config": artifact.to_dict()["config"],
        }
        return self._request("POST", "/connectors", json=payload)

    def update_connector(self, artifact: ConnectorArtifact) -> dict:
        """Update an existing connector."""
        config = artifact.to_dict()["config"]
        return self._request(
            "PUT",
            f"/connectors/{self._connector_path(artifact.name)}/config",
            json=config,
        )

    def delete_connector(self, connector_name: str) -> None:
        """Delete a connector."""
        self._request("DELETE", f"/connectors/{self._connector_path(connector_name)}")

    def restart_connector(self, connector_name: str) -> None:
        """Restart a connector."""
        self._request("POST", f"/connectors/{self._connector_path(connector_name)}/restart")

    def pause_connector(self, connector_name: str) -> None:
        """Pause a connector."""
        self._request("PUT", f"/connectors/{self._connector_path(connector_name)}/pause")

    def resume_connector(self, connector_name: str) -> None:
        """Resume a connector."""
        self._request("PUT", f"/connectors/{self._connector_path(connector_name)}/resume")

    def plan_connector(self, artifact: ConnectorArtifact) -> ConnectorChange:
        """Plan changes for a connector."""
        if self.cluster_binding is not None:
            return self._plan_bound_connector(artifact)

        current = self.get_connector_state(artifact.name)

        if not current.exists:
            return ConnectorChange(
                connector_name=artifact.name,
                action="create",
                current=current,
                desired=artifact,
            )

        # Check for config changes
        desired_config = artifact.to_dict()["config"]
        # Remove name from comparison
        current_config = dict(current.config or {})
        current_config.pop("name", None)
        desired_config_cmp = dict(desired_config)
        desired_config_cmp.pop("name", None)
        changes = secret_neutral_connector_config_diff(
            current_config,
            desired_config_cmp,
        )

        # Check for removed keys and warn
        removed_keys = [
            key for key, evidence in changes.items() if evidence["change"] == "removed"
        ]
        if removed_keys:
            logger.warning(
                f"Connector '{artifact.name}' will have config keys removed: {removed_keys}"
            )

        if changes:
            return ConnectorChange(
                connector_name=artifact.name,
                action="update",
                current=current,
                desired=artifact,
                changes=changes,
            )

        return ConnectorChange(
            connector_name=artifact.name,
            action="none",
            current=current,
            desired=artifact,
        )

    def _plan_bound_connector(self, artifact: ConnectorArtifact) -> ConnectorChange:
        """Plan from one strict observation against one resolved bound artifact."""
        binding = self.require_cluster_binding()
        desired = resolve_connector_artifact(artifact, binding)
        current = self.observe_managed_connector(desired.name)
        if current.binding != binding:
            raise ConnectClusterBindingError(
                "Kafka Connect cluster binding changed during managed planning"
            )
        backend_identity = binding.backend_identity
        if not current.exists:
            return ConnectorChange(
                connector_name=desired.name,
                action="create",
                current=current,
                desired=desired,
                backend_identity=backend_identity,
            )

        desired_config = desired.to_dict().get("config")
        if not isinstance(desired_config, dict):
            raise ConnectManagedObservationError(
                "Resolved connector artifact has an invalid config object"
            )
        changes = secret_neutral_connector_config_diff(
            current.config_dict(),
            desired_config,
        )
        action = "update" if changes else "none"
        return ConnectorChange(
            connector_name=desired.name,
            action=action,
            current=current,
            desired=desired,
            changes=changes,
            backend_identity=backend_identity,
        )

    def apply_connector(self, artifact: ConnectorArtifact) -> str:
        """Apply a connector artifact. Returns action taken."""
        change = self.plan_connector(artifact)
        desired = change.desired or artifact

        if change.action == "create":
            self.create_connector(desired)
            return "created"
        elif change.action == "update":
            self.update_connector(desired)
            return "updated"
        else:
            return "unchanged"
