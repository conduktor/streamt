"""Gateway deployer for Conduktor Gateway interceptors and alias topics."""

from __future__ import annotations

import base64
import hashlib
import json
import logging
import re
import time
import unicodedata
from collections.abc import Mapping
from copy import deepcopy
from dataclasses import dataclass
from dataclasses import field as dataclass_field
from typing import Literal, Optional
from urllib.parse import quote, urlsplit

import requests
import sqlglot
from requests.auth import HTTPBasicAuth
from sqlglot import exp
from sqlglot.errors import SqlglotError

from streamt.compiler.manifest import GatewayRuleArtifact
from streamt.deployer.ssl_utils import configure_session_ssl

logger = logging.getLogger(__name__)

# Default timeouts (in seconds)
DEFAULT_TIMEOUT = 10
_MAX_MANAGED_GATEWAY_RESPONSE_BYTES = 1024 * 1024
_MANAGED_GATEWAY_CHUNK_BYTES = 64 * 1024
_GATEWAY_BINDING_VERSION = 1
_GATEWAY_API_VERSION = "v2"
_FINGERPRINT_PREFIX = "sha256:"
_GATEWAY_BACKEND_IDENTITY = re.compile(
    r"^conduktor-gateway:v1:(?P<scope>p|v-[A-Za-z0-9_-]+):"
    r"(?P<endpoint>sha256:[0-9a-f]{64})$"
)
_GATEWAY_RESOURCE_NAME = re.compile(r"^[A-Za-z0-9._-]+$")
_GENERATED_INTERCEPTOR_INDEX = r"(?:0|[1-9][0-9]*)"
_GENERATED_INTERCEPTOR_TYPES = frozenset({"filter", "mask", "encrypt", "readonly"})
_KNOWN_INTERCEPTOR_SCOPE_KEYS = frozenset({"vCluster", "group", "username"})
_FILTER_PLUGIN_CLASS = "io.conduktor.gateway.interceptor.VirtualSqlTopicPlugin"
_MANAGED_INTERCEPTOR_PRIORITY = 100


class GatewayError(Exception):
    """Base exception for Gateway operations."""

    pass


class GatewayConnectionError(GatewayError):
    """Cannot connect to Gateway."""

    pass


class GatewayAuthenticationError(GatewayError):
    """Gateway authentication failed."""

    pass


class GatewayBindingError(ValueError):
    """A Gateway runtime cannot be bound to one canonical provider identity."""


class GatewayManagedObservationError(GatewayError):
    """A strict Gateway rule observation could not be proven complete."""


class GatewayDesiredAggregateError(ValueError):
    """A compiled Gateway rule cannot become exact supported provider state."""


class GatewayManagedMutationError(GatewayError):
    """An exact managed Gateway mutation could not be proven successful."""


class GatewayChangeEvidenceError(ValueError):
    """Gateway plan evidence is not one exact secret-neutral change shape."""


class _GeneratedGatewayInterceptorNameError(ValueError):
    """Internal marker for an ambiguous generated interceptor namespace."""


class _InvalidManagedGatewayJSONError(ValueError):
    """Internal marker for non-canonical managed-observation JSON."""


def _sha256(value: str) -> str:
    return f"{_FINGERPRINT_PREFIX}{hashlib.sha256(value.encode('utf-8')).hexdigest()}"


def _is_fingerprint(value: object) -> bool:
    if not isinstance(value, str) or not value.startswith(_FINGERPRINT_PREFIX):
        return False
    digest = value.removeprefix(_FINGERPRINT_PREFIX)
    return len(digest) == 64 and all(character in "0123456789abcdef" for character in digest)


def _reject_duplicate_json_keys(
    pairs: list[tuple[str, object]],
) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise _InvalidManagedGatewayJSONError
        result[key] = value
    return result


def _reject_nonfinite_json_constant(_value: str) -> object:
    raise _InvalidManagedGatewayJSONError


@dataclass(frozen=True)
class GeneratedGatewayInterceptorName:
    """One exact deterministic interceptor identity owned by a logical rule."""

    logical_name: str
    declaration_type: str
    ordinal: int


def generate_gateway_interceptor_name(
    logical_name: str,
    declaration_type: str,
    ordinal: int,
) -> str:
    """Generate one canonical managed interceptor name."""
    if (
        not isinstance(logical_name, str)
        or _GATEWAY_RESOURCE_NAME.fullmatch(logical_name) is None
        or declaration_type not in _GENERATED_INTERCEPTOR_TYPES
        or type(ordinal) is not int
        or ordinal < 0
    ):
        raise _GeneratedGatewayInterceptorNameError(
            "Gateway generated interceptor identity is invalid"
        )
    return f"{logical_name}_{declaration_type}_{ordinal}"


def classify_gateway_interceptor_name(
    logical_name: str,
    candidate: str,
) -> GeneratedGatewayInterceptorName | None:
    """Classify one exact anchored generated name, or return unrelated.

    A candidate with exactly the target rule prefix and two generated-name
    components is target namespace evidence. Unsupported types and
    non-canonical indexes are ambiguous rather than safely unrelated.
    """
    if (
        not isinstance(logical_name, str)
        or _GATEWAY_RESOURCE_NAME.fullmatch(logical_name) is None
        or not isinstance(candidate, str)
    ):
        raise _GeneratedGatewayInterceptorNameError(
            "Gateway generated interceptor identity is invalid"
        )
    match = re.fullmatch(
        rf"{re.escape(logical_name)}_(?P<type>[^_]+)_(?P<ordinal>[^_]+)",
        candidate,
    )
    if match is None:
        return None
    declaration_type = match.group("type")
    raw_ordinal = match.group("ordinal")
    if (
        declaration_type not in _GENERATED_INTERCEPTOR_TYPES
        or re.fullmatch(_GENERATED_INTERCEPTOR_INDEX, raw_ordinal) is None
    ):
        raise _GeneratedGatewayInterceptorNameError(
            "Gateway generated interceptor namespace is ambiguous"
        )
    ordinal = int(raw_ordinal)
    regenerated = generate_gateway_interceptor_name(
        logical_name,
        declaration_type,
        ordinal,
    )
    if regenerated != candidate:
        raise _GeneratedGatewayInterceptorNameError(
            "Gateway generated interceptor namespace is ambiguous"
        )
    return GeneratedGatewayInterceptorName(
        logical_name=logical_name,
        declaration_type=declaration_type,
        ordinal=ordinal,
    )


def _has_control_character(value: str) -> bool:
    return any(unicodedata.category(character) == "Cc" for character in value)


def _normalize_gateway_admin_url(admin_url: str) -> str:
    """Normalize the configured admin endpoint without echoing it in errors."""
    if not isinstance(admin_url, str) or not admin_url or admin_url != admin_url.strip():
        raise GatewayBindingError("Invalid Gateway admin URL: endpoint is malformed")
    if any(character.isspace() for character in admin_url) or _has_control_character(admin_url):
        raise GatewayBindingError("Invalid Gateway admin URL: endpoint is malformed")
    if "?" in admin_url or "#" in admin_url:
        raise GatewayBindingError(
            "Invalid Gateway admin URL: query strings and fragments are not allowed"
        )
    try:
        parsed = urlsplit(admin_url)
        port = parsed.port
    except ValueError:
        raise GatewayBindingError("Invalid Gateway admin URL: endpoint is malformed") from None
    scheme = parsed.scheme.lower()
    if scheme not in {"http", "https"} or not parsed.hostname:
        raise GatewayBindingError(
            "Invalid Gateway admin URL: endpoint must start with http:// or https://"
        )
    if "@" in parsed.netloc or parsed.username is not None or parsed.password is not None:
        raise GatewayBindingError(
            "Invalid Gateway admin URL: endpoint user information is not allowed"
        )
    try:
        hostname = parsed.hostname.encode("idna").decode("ascii").lower()
    except UnicodeError:
        raise GatewayBindingError("Invalid Gateway admin URL: hostname is malformed") from None
    if ":" in hostname:
        hostname = f"[{hostname}]"
    default_port = 80 if scheme == "http" else 443
    authority = hostname if port in (None, default_port) else f"{hostname}:{port}"
    path = parsed.path.rstrip("/")
    return f"{scheme}://{authority}{path}"


def _validate_virtual_cluster(value: object) -> str:
    if value is None:
        return "passthrough"
    if (
        not isinstance(value, str)
        or not value
        or value != value.strip()
        or len(value.encode("utf-8")) > 256
        or _has_control_character(value)
        or _GATEWAY_RESOURCE_NAME.fullmatch(value) is None
    ):
        raise GatewayBindingError("Invalid Gateway virtual cluster scope")
    return value


def _scope_token(virtual_cluster: str) -> str:
    if virtual_cluster == "passthrough":
        return "p"
    encoded = base64.urlsafe_b64encode(virtual_cluster.encode("utf-8")).decode("ascii")
    return f"v-{encoded.rstrip('=')}"


@dataclass(frozen=True)
class GatewayBackendBinding:
    """Versioned, endpoint-free identity for one Gateway vCluster scope."""

    virtual_cluster: str
    endpoint_fingerprint: str
    api_version: str = _GATEWAY_API_VERSION
    version: int = _GATEWAY_BINDING_VERSION

    def __post_init__(self) -> None:
        if type(self.version) is not int or self.version != _GATEWAY_BINDING_VERSION:
            raise GatewayBindingError("Unsupported Gateway binding version")
        if self.api_version != _GATEWAY_API_VERSION:
            raise GatewayBindingError("Unsupported Gateway API version")
        canonical_scope = _validate_virtual_cluster(self.virtual_cluster)
        object.__setattr__(self, "virtual_cluster", canonical_scope)
        if not _is_fingerprint(self.endpoint_fingerprint):
            raise GatewayBindingError("Invalid Gateway endpoint fingerprint")

    @classmethod
    def from_endpoint(
        cls,
        admin_url: str,
        *,
        virtual_cluster: str | None = None,
        api_version: str = _GATEWAY_API_VERSION,
    ) -> GatewayBackendBinding:
        """Bind an exact scope to the normalized v2 collection API endpoint."""
        if api_version != _GATEWAY_API_VERSION:
            raise GatewayBindingError("Unsupported Gateway API version")
        normalized = _normalize_gateway_admin_url(admin_url)
        scope = _validate_virtual_cluster(virtual_cluster)
        return cls(
            virtual_cluster=scope,
            endpoint_fingerprint=_sha256(f"{normalized}/gateway/{api_version}"),
            api_version=api_version,
        )

    @property
    def scope_name(self) -> str:
        """Return the exact provider scope, including its explicit default."""
        return self.virtual_cluster

    @property
    def backend_identity(self) -> str:
        """Return the canonical identity without an endpoint or credentials."""
        return (
            f"conduktor-gateway:v{self.version}:{_scope_token(self.virtual_cluster)}:"
            f"{self.endpoint_fingerprint}"
        )


def is_gateway_backend_identity(value: object) -> bool:
    """Whether a value is one exact canonical Gateway backend identity."""
    if not isinstance(value, str):
        return False
    match = _GATEWAY_BACKEND_IDENTITY.fullmatch(value)
    if match is None:
        return False
    token = match.group("scope")
    if token == "p":
        return True
    payload = token.removeprefix("v-")
    padded = payload + "=" * (-len(payload) % 4)
    try:
        decoded = base64.b64decode(
            padded.encode("ascii"),
            altchars=b"-_",
            validate=True,
        ).decode("utf-8")
        canonical = _validate_virtual_cluster(decoded)
    except (UnicodeDecodeError, ValueError):
        return False
    return canonical != "passthrough" and _scope_token(canonical) == token


GatewayScope = tuple[tuple[str, str | None], ...]
_CANONICAL_INTERCEPTOR_SCOPE_KEYS = tuple(sorted(_KNOWN_INTERCEPTOR_SCOPE_KEYS))


def _canonical_vcluster_scope(virtual_cluster: str) -> GatewayScope:
    return tuple(
        (
            key,
            virtual_cluster if key == "vCluster" else None,
        )
        for key in _CANONICAL_INTERCEPTOR_SCOPE_KEYS
    )


@dataclass(frozen=True)
class ManagedGatewayInterceptor:
    """One immutable interceptor managed surface within a Gateway rule."""

    name: str
    scope: GatewayScope
    plugin_class: str
    priority: int
    config_json: str = dataclass_field(repr=False)

    def __post_init__(self) -> None:
        if not isinstance(self.name, str) or not self.name or _has_control_character(self.name):
            raise GatewayManagedObservationError(
                "Gateway managed interceptor has an invalid identity"
            )
        if (
            not isinstance(self.scope, tuple)
            or tuple(sorted(self.scope)) != self.scope
            or len({key for key, _value in self.scope}) != len(self.scope)
            or (
                self.scope
                and tuple(key for key, _value in self.scope) != _CANONICAL_INTERCEPTOR_SCOPE_KEYS
            )
        ):
            raise GatewayManagedObservationError("Gateway managed interceptor has an invalid scope")
        for key, value in self.scope:
            if key not in _KNOWN_INTERCEPTOR_SCOPE_KEYS or (
                value is not None
                and (
                    not isinstance(value, str)
                    or not value
                    or value != value.strip()
                    or _has_control_character(value)
                )
            ):
                raise GatewayManagedObservationError(
                    "Gateway managed interceptor has an invalid scope"
                )
        scope = dict(self.scope)
        if scope.get("group") is not None and scope.get("username") is not None:
            raise GatewayManagedObservationError(
                "Gateway managed interceptor has an invalid scope combination"
            )
        virtual_cluster = scope.get("vCluster")
        if virtual_cluster is None and any(
            scope.get(key) is not None for key in ("group", "username")
        ):
            raise GatewayManagedObservationError(
                "Gateway managed interceptor has a noncanonical scope"
            )
        if virtual_cluster is not None:
            try:
                _validate_virtual_cluster(virtual_cluster)
            except GatewayBindingError:
                raise GatewayManagedObservationError(
                    "Gateway managed interceptor has an invalid virtual cluster scope"
                ) from None
        if not isinstance(self.plugin_class, str) or not self.plugin_class:
            raise GatewayManagedObservationError(
                "Gateway managed interceptor has an invalid plugin class"
            )
        if type(self.priority) is not int:
            raise GatewayManagedObservationError(
                "Gateway managed interceptor has an invalid priority"
            )
        try:
            config = json.loads(
                self.config_json,
                object_pairs_hook=_reject_duplicate_json_keys,
                parse_constant=_reject_nonfinite_json_constant,
            )
        except (TypeError, json.JSONDecodeError, _InvalidManagedGatewayJSONError):
            raise GatewayManagedObservationError(
                "Gateway managed interceptor has invalid canonical config"
            ) from None
        if (
            not isinstance(config, dict)
            or json.dumps(
                config,
                ensure_ascii=False,
                allow_nan=False,
                separators=(",", ":"),
                sort_keys=True,
            )
            != self.config_json
        ):
            raise GatewayManagedObservationError(
                "Gateway managed interceptor has invalid canonical config"
            )


@dataclass(frozen=True, eq=False)
class ManagedGatewayRuleObservation:
    """Complete immutable alias and rule-owned interceptor observation."""

    binding: GatewayBackendBinding
    logical_name: str
    alias_name: str
    exists: bool
    physical_name: str | None = None
    physical_cluster: str | None = None
    interceptors: tuple[ManagedGatewayInterceptor, ...] = dataclass_field(
        default_factory=tuple,
        repr=False,
    )

    def __post_init__(self) -> None:
        if not isinstance(self.binding, GatewayBackendBinding):
            raise GatewayManagedObservationError(
                "Gateway managed observation has an invalid backend binding"
            )
        if (
            not isinstance(self.logical_name, str)
            or not GatewayDeployer._VALID_RESOURCE_NAME.fullmatch(self.logical_name)
            or not isinstance(self.alias_name, str)
            or not GatewayDeployer._VALID_RESOURCE_NAME.fullmatch(self.alias_name)
            or type(self.exists) is not bool
        ):
            raise GatewayManagedObservationError(
                "Gateway managed observation has an invalid rule identity"
            )
        if not isinstance(self.interceptors, tuple) or not all(
            isinstance(item, ManagedGatewayInterceptor) for item in self.interceptors
        ):
            raise GatewayManagedObservationError(
                "Gateway managed observation has an invalid interceptor surface"
            )
        target_scope = _canonical_vcluster_scope(self.binding.virtual_cluster)
        if any(item.scope != target_scope for item in self.interceptors):
            raise GatewayManagedObservationError(
                "Gateway managed observation has a mismatched interceptor scope"
            )
        if not self.exists:
            if (
                self.physical_name is not None
                or self.physical_cluster is not None
                or self.interceptors
            ):
                raise GatewayManagedObservationError(
                    "Absent Gateway managed observation contains provider state"
                )
            return
        if (
            not isinstance(self.physical_name, str)
            or not self.physical_name
            or _GATEWAY_RESOURCE_NAME.fullmatch(self.physical_name) is None
            or self.physical_cluster != "main"
            or tuple(sorted(self.interceptors, key=lambda item: item.name)) != self.interceptors
            or len({item.name for item in self.interceptors}) != len(self.interceptors)
        ):
            raise GatewayManagedObservationError(
                "Gateway managed observation has an invalid managed surface"
            )

    def _canonical_json(self) -> str:
        return json.dumps(
            {
                "alias_name": self.alias_name,
                "backend_identity": self.binding.backend_identity,
                "exists": self.exists,
                "interceptors": [
                    {
                        "config": interceptor.config_json,
                        "name": interceptor.name,
                        "plugin_class": interceptor.plugin_class,
                        "priority": interceptor.priority,
                        "scope": interceptor.scope,
                    }
                    for interceptor in self.interceptors
                ],
                "logical_name": self.logical_name,
                "physical_cluster": self.physical_cluster,
                "physical_name": self.physical_name,
            },
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        )

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, ManagedGatewayRuleObservation):
            return NotImplemented
        return self._canonical_json() == other._canonical_json()

    def __hash__(self) -> int:
        return hash(self._canonical_json())

    @property
    def fingerprint(self) -> str:
        """Fingerprint the binding and exact complete provider managed surface."""
        return _sha256(self._canonical_json())


_FILTER_COMPARISON_TYPES = (
    exp.EQ,
    exp.NEQ,
    exp.GT,
    exp.GTE,
    exp.LT,
    exp.LTE,
)


def _is_supported_gateway_filter_expression(expression: exp.Expression) -> bool:
    """Whether one parsed WHERE expression is in the proven Gateway subset."""
    if isinstance(expression, exp.Paren):
        nested = expression.this
        return isinstance(nested, exp.Expression) and _is_supported_gateway_filter_expression(
            nested
        )
    if isinstance(expression, exp.And):
        left = expression.this
        right = expression.expression
        return (
            isinstance(left, exp.Expression)
            and isinstance(right, exp.Expression)
            and _is_supported_gateway_filter_expression(left)
            and _is_supported_gateway_filter_expression(right)
        )
    if isinstance(expression, _FILTER_COMPARISON_TYPES):
        return isinstance(expression.this, exp.Column) and isinstance(
            expression.expression, exp.Literal
        )
    if isinstance(expression, exp.RegexpLike):
        return (
            isinstance(expression.this, exp.Column)
            and isinstance(expression.expression, exp.Literal)
            and expression.expression.is_string
        )
    return False


def _validate_gateway_filter_where(value: object) -> str:
    """Validate and preserve one exact compiler-emitted filter expression."""
    if (
        not isinstance(value, str)
        or not value
        or value != value.strip()
        or any(marker in value for marker in (";", "--", "/*", "*/"))
    ):
        raise GatewayDesiredAggregateError(
            "Gateway filter declaration is outside the supported expression subset"
        )
    try:
        parsed = sqlglot.parse(value)
    except (RecursionError, SqlglotError):
        raise GatewayDesiredAggregateError(
            "Gateway filter declaration is outside the supported expression subset"
        ) from None
    if (
        len(parsed) != 1
        or parsed[0] is None
        or not _is_supported_gateway_filter_expression(parsed[0])
    ):
        raise GatewayDesiredAggregateError(
            "Gateway filter declaration is outside the supported expression subset"
        )
    return value


def build_desired_gateway_rule(
    artifact: GatewayRuleArtifact,
    binding: GatewayBackendBinding,
) -> ManagedGatewayRuleObservation:
    """Build one complete immutable desired Gateway managed surface."""
    if not isinstance(artifact, GatewayRuleArtifact) or not isinstance(
        binding, GatewayBackendBinding
    ):
        raise GatewayDesiredAggregateError(
            "Gateway desired aggregate requires a strict artifact and binding"
        )
    if any(
        not isinstance(name, str) or _GATEWAY_RESOURCE_NAME.fullmatch(name) is None
        for name in (
            artifact.name,
            artifact.virtual_topic,
            artifact.physical_topic,
        )
    ):
        raise GatewayDesiredAggregateError(
            "Gateway desired aggregate has an invalid resource identity"
        )
    if not isinstance(artifact.interceptors, list):
        raise GatewayDesiredAggregateError(
            "Gateway desired aggregate has unsupported interceptor declarations"
        )

    desired_interceptors: tuple[ManagedGatewayInterceptor, ...] = ()
    if artifact.interceptors:
        if len(artifact.interceptors) != 1:
            raise GatewayDesiredAggregateError(
                "Gateway desired aggregate has unsupported interceptor declarations"
            )
        declaration = artifact.interceptors[0]
        if (
            not isinstance(declaration, dict)
            or set(declaration) != {"type", "config"}
            or declaration.get("type") != "filter"
        ):
            raise GatewayDesiredAggregateError(
                "Gateway desired aggregate has unsupported interceptor declarations"
            )
        config = declaration.get("config")
        if not isinstance(config, dict) or set(config) != {"where"}:
            raise GatewayDesiredAggregateError(
                "Gateway desired aggregate has unsupported interceptor declarations"
            )
        where_clause = _validate_gateway_filter_where(config.get("where"))
        provider_config = {
            "virtualTopic": artifact.virtual_topic,
            "statement": (f'SELECT * FROM "{artifact.physical_topic}" WHERE {where_clause}'),
        }
        config_json = json.dumps(
            provider_config,
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        desired_interceptors = (
            ManagedGatewayInterceptor(
                name=generate_gateway_interceptor_name(
                    artifact.name,
                    "filter",
                    0,
                ),
                scope=_canonical_vcluster_scope(binding.virtual_cluster),
                plugin_class=_FILTER_PLUGIN_CLASS,
                priority=_MANAGED_INTERCEPTOR_PRIORITY,
                config_json=config_json,
            ),
        )

    return ManagedGatewayRuleObservation(
        binding=binding,
        logical_name=artifact.name,
        alias_name=artifact.virtual_topic,
        exists=True,
        physical_name=artifact.physical_topic,
        physical_cluster="main",
        interceptors=desired_interceptors,
    )


@dataclass(frozen=True)
class _ParsedAliasTopic:
    scope: str
    name: str
    physical_name: str
    physical_cluster: str

    def __post_init__(self) -> None:
        try:
            canonical_scope = _validate_virtual_cluster(self.scope)
        except GatewayBindingError:
            raise GatewayManagedObservationError(
                "Gateway managed snapshot contains an invalid alias"
            ) from None
        if (
            canonical_scope != self.scope
            or not isinstance(self.name, str)
            or not self.name
            or _GATEWAY_RESOURCE_NAME.fullmatch(self.name) is None
            or not isinstance(self.physical_name, str)
            or not self.physical_name
            or _GATEWAY_RESOURCE_NAME.fullmatch(self.physical_name) is None
            or self.physical_cluster != "main"
        ):
            raise GatewayManagedObservationError(
                "Gateway managed snapshot contains an invalid alias"
            )


@dataclass(frozen=True)
class _ParsedInterceptor:
    scope: GatewayScope
    name: str
    plugin_class: str
    priority: int
    config_json: str = dataclass_field(repr=False)

    def __post_init__(self) -> None:
        try:
            ManagedGatewayInterceptor(
                name=self.name,
                scope=self.scope,
                plugin_class=self.plugin_class,
                priority=self.priority,
                config_json=self.config_json,
            )
        except GatewayManagedObservationError:
            raise GatewayManagedObservationError(
                "Gateway managed snapshot contains an invalid interceptor"
            ) from None


def _gateway_scope_sort_key(
    scope: GatewayScope,
) -> tuple[tuple[str, bool, str], ...]:
    """Return a total ordering for canonical scopes containing nullable values."""
    return tuple(
        (key, value is not None, value if value is not None else "")
        for key, value in scope
    )


def _validate_managed_gateway_rule_identity(
    logical_name: object,
    alias_name: object,
) -> tuple[str, str]:
    if (
        not isinstance(logical_name, str)
        or _GATEWAY_RESOURCE_NAME.fullmatch(logical_name) is None
        or not isinstance(alias_name, str)
        or _GATEWAY_RESOURCE_NAME.fullmatch(alias_name) is None
    ):
        raise GatewayManagedObservationError(
            "Gateway managed observation requires valid rule identities"
        )
    return logical_name, alias_name


@dataclass(frozen=True)
class ManagedGatewaySnapshot:
    """One complete immutable two-list Gateway provider snapshot."""

    binding: GatewayBackendBinding
    aliases: tuple[_ParsedAliasTopic, ...]
    interceptors: tuple[_ParsedInterceptor, ...] = dataclass_field(repr=False)

    def __post_init__(self) -> None:
        if type(self.binding) is not GatewayBackendBinding:
            raise GatewayManagedObservationError(
                "Gateway managed snapshot has an invalid backend binding"
            )
        if not isinstance(self.aliases, tuple) or any(
            type(alias) is not _ParsedAliasTopic for alias in self.aliases
        ):
            raise GatewayManagedObservationError(
                "Gateway managed snapshot has an invalid alias collection"
            )
        if not isinstance(self.interceptors, tuple) or any(
            type(interceptor) is not _ParsedInterceptor
            for interceptor in self.interceptors
        ):
            raise GatewayManagedObservationError(
                "Gateway managed snapshot has an invalid interceptor collection"
            )

        sorted_aliases = tuple(
            sorted(self.aliases, key=lambda alias: (alias.scope, alias.name))
        )
        if sorted_aliases != self.aliases or len(
            {(alias.scope, alias.name) for alias in self.aliases}
        ) != len(self.aliases):
            raise GatewayManagedObservationError(
                "Gateway managed snapshot has a noncanonical alias collection"
            )

        sorted_interceptors = tuple(
            sorted(
                self.interceptors,
                key=lambda interceptor: (
                    _gateway_scope_sort_key(interceptor.scope),
                    interceptor.name,
                ),
            )
        )
        if sorted_interceptors != self.interceptors or len(
            {(interceptor.scope, interceptor.name) for interceptor in self.interceptors}
        ) != len(self.interceptors):
            raise GatewayManagedObservationError(
                "Gateway managed snapshot has a noncanonical interceptor collection"
            )

    def rule(
        self,
        logical_name: str,
        alias_name: str,
    ) -> ManagedGatewayRuleObservation:
        """Derive one exact managed rule without further provider access."""
        logical_name, alias_name = _validate_managed_gateway_rule_identity(
            logical_name,
            alias_name,
        )
        target_scope = _canonical_vcluster_scope(self.binding.virtual_cluster)
        owned: list[_ParsedInterceptor] = []
        for interceptor in self.interceptors:
            if interceptor.scope != target_scope:
                continue
            try:
                generated_name = classify_gateway_interceptor_name(
                    logical_name,
                    interceptor.name,
                )
            except _GeneratedGatewayInterceptorNameError:
                raise GatewayManagedObservationError(
                    "Gateway managed interceptor ownership is ambiguous"
                ) from None
            if generated_name is not None:
                owned.append(interceptor)

        alias = next(
            (
                candidate
                for candidate in self.aliases
                if candidate.scope == self.binding.virtual_cluster
                and candidate.name == alias_name
            ),
            None,
        )
        if alias is None:
            if owned:
                raise GatewayManagedObservationError(
                    "Gateway managed observation is partial: alias is absent with interceptors"
                )
            return ManagedGatewayRuleObservation(
                binding=self.binding,
                logical_name=logical_name,
                alias_name=alias_name,
                exists=False,
            )

        normalized_interceptors = tuple(
            ManagedGatewayInterceptor(
                name=interceptor.name,
                scope=interceptor.scope,
                plugin_class=interceptor.plugin_class,
                priority=interceptor.priority,
                config_json=interceptor.config_json,
            )
            for interceptor in sorted(owned, key=lambda interceptor: interceptor.name)
        )
        return ManagedGatewayRuleObservation(
            binding=self.binding,
            logical_name=logical_name,
            alias_name=alias_name,
            exists=True,
            physical_name=alias.physical_name,
            physical_cluster=alias.physical_cluster,
            interceptors=normalized_interceptors,
        )


@dataclass
class InterceptorState:
    """Current state of an interceptor."""

    name: str
    exists: bool
    plugin_class: Optional[str] = None
    config: Optional[dict[str, object]] = None
    scope: Optional[dict[str, object]] = None


@dataclass
class AliasTopicState:
    """Current state of an alias topic."""

    name: str
    exists: bool
    physical_topic: Optional[str] = None


def _copy_strict_managed_gateway_observation(
    observation: ManagedGatewayRuleObservation,
) -> ManagedGatewayRuleObservation:
    """Revalidate and detach one caller-owned managed observation value."""
    binding = GatewayBackendBinding(
        virtual_cluster=observation.binding.virtual_cluster,
        endpoint_fingerprint=observation.binding.endpoint_fingerprint,
        api_version=observation.binding.api_version,
        version=observation.binding.version,
    )
    interceptors = tuple(
        ManagedGatewayInterceptor(
            name=interceptor.name,
            scope=tuple(interceptor.scope),
            plugin_class=interceptor.plugin_class,
            priority=interceptor.priority,
            config_json=interceptor.config_json,
        )
        for interceptor in observation.interceptors
    )
    return ManagedGatewayRuleObservation(
        binding=binding,
        logical_name=observation.logical_name,
        alias_name=observation.alias_name,
        exists=observation.exists,
        physical_name=observation.physical_name,
        physical_cluster=observation.physical_cluster,
        interceptors=interceptors,
    )


@dataclass(frozen=True)
class GatewayAliasLocator:
    """Secret-free exact identity of one managed AliasTopic."""

    binding: GatewayBackendBinding
    name: str

    def __post_init__(self) -> None:
        if (
            type(self.binding) is not GatewayBackendBinding
            or not isinstance(self.name, str)
            or _GATEWAY_RESOURCE_NAME.fullmatch(self.name) is None
        ):
            raise GatewayManagedMutationError("Gateway alias locator is invalid")


@dataclass(frozen=True)
class GatewayInterceptorLocator:
    """Secret-free exact scoped identity of one managed Interceptor."""

    binding: GatewayBackendBinding
    name: str
    scope: GatewayScope

    def __post_init__(self) -> None:
        if (
            type(self.binding) is not GatewayBackendBinding
            or not isinstance(self.name, str)
            or not self.name
            or _has_control_character(self.name)
            or not isinstance(self.scope, tuple)
            or self.scope != _canonical_vcluster_scope(self.binding.virtual_cluster)
        ):
            raise GatewayManagedMutationError("Gateway interceptor locator is invalid")


def _gateway_interceptor_locator_sort_key(
    locator: GatewayInterceptorLocator,
) -> tuple[tuple[tuple[str, bool, str], ...], str]:
    return (_gateway_scope_sort_key(locator.scope), locator.name)


@dataclass(frozen=True)
class GatewayRuleLocator:
    """Secret-free exact managed provider identities for one complete rule."""

    binding: GatewayBackendBinding
    logical_name: str
    alias_name: str
    interceptors: tuple[GatewayInterceptorLocator, ...] = dataclass_field(
        default_factory=tuple,
        repr=False,
    )

    def __post_init__(self) -> None:
        try:
            logical_name, alias_name = _validate_managed_gateway_rule_identity(
                self.logical_name,
                self.alias_name,
            )
        except GatewayManagedObservationError:
            raise GatewayManagedMutationError("Gateway rule locator is invalid") from None
        if (
            type(self.binding) is not GatewayBackendBinding
            or logical_name != self.logical_name
            or alias_name != self.alias_name
            or not isinstance(self.interceptors, tuple)
            or any(
                type(locator) is not GatewayInterceptorLocator or locator.binding != self.binding
                for locator in self.interceptors
            )
            or tuple(sorted(self.interceptors, key=_gateway_interceptor_locator_sort_key))
            != self.interceptors
            or len({(locator.scope, locator.name) for locator in self.interceptors})
            != len(self.interceptors)
        ):
            raise GatewayManagedMutationError("Gateway rule locator is invalid")

        for locator in self.interceptors:
            try:
                generated = classify_gateway_interceptor_name(
                    self.logical_name,
                    locator.name,
                )
            except _GeneratedGatewayInterceptorNameError:
                raise GatewayManagedMutationError(
                    "Gateway rule locator has ambiguous interceptor ownership"
                ) from None
            if generated is None:
                raise GatewayManagedMutationError(
                    "Gateway rule locator contains an unrelated interceptor"
                )

    @classmethod
    def from_observation(
        cls,
        observation: ManagedGatewayRuleObservation,
    ) -> GatewayRuleLocator:
        """Build a locator only from one complete present observation."""
        if not isinstance(observation, ManagedGatewayRuleObservation) or not observation.exists:
            raise GatewayManagedMutationError(
                "Gateway rule locator requires a complete present observation"
            )
        try:
            detached = _copy_strict_managed_gateway_observation(observation)
        except (AttributeError, GatewayManagedObservationError, TypeError, ValueError):
            raise GatewayManagedMutationError(
                "Gateway rule locator requires a complete present observation"
            ) from None
        return cls(
            binding=detached.binding,
            logical_name=detached.logical_name,
            alias_name=detached.alias_name,
            interceptors=tuple(
                GatewayInterceptorLocator(
                    binding=detached.binding,
                    name=interceptor.name,
                    scope=interceptor.scope,
                )
                for interceptor in detached.interceptors
            ),
        )

    @property
    def alias(self) -> GatewayAliasLocator:
        """Return the exact AliasTopic identity in this rule."""
        return GatewayAliasLocator(binding=self.binding, name=self.alias_name)


@dataclass(frozen=True)
class GatewayRuleMutation:
    """One validated exact create, update, delete, or no-op transition."""

    expected_action: Literal["create", "update", "delete", "no-op"]
    current: ManagedGatewayRuleObservation = dataclass_field(repr=False)
    desired: ManagedGatewayRuleObservation | None = dataclass_field(repr=False)

    def __post_init__(self) -> None:
        if not isinstance(self.current, ManagedGatewayRuleObservation) or (
            self.desired is not None and not isinstance(self.desired, ManagedGatewayRuleObservation)
        ):
            raise GatewayManagedMutationError("Gateway rule mutation is invalid")
        try:
            current = _copy_strict_managed_gateway_observation(self.current)
            desired = (
                _copy_strict_managed_gateway_observation(self.desired)
                if self.desired is not None
                else None
            )
        except (AttributeError, GatewayManagedObservationError, TypeError, ValueError):
            raise GatewayManagedMutationError("Gateway rule mutation is invalid") from None

        if desired is None:
            coherent_identity = current.exists
            actual_action = "delete"
        else:
            coherent_identity = (
                current.binding == desired.binding
                and current.logical_name == desired.logical_name
                and current.alias_name == desired.alias_name
                and desired.exists
            )
            if not current.exists:
                actual_action = "create"
            elif current == desired:
                actual_action = "no-op"
            else:
                actual_action = "update"

        if not coherent_identity or self.expected_action != actual_action:
            raise GatewayManagedMutationError(
                "Gateway rule mutation does not describe one coherent transition"
            )
        if current.exists:
            GatewayRuleLocator.from_observation(current)
        if desired is not None:
            GatewayRuleLocator.from_observation(desired)
        object.__setattr__(self, "current", current)
        object.__setattr__(self, "desired", desired)


@dataclass(frozen=True)
class _ManagedGatewayOperation:
    method: str
    endpoint: str = dataclass_field(repr=False)
    payload: dict[str, object] = dataclass_field(repr=False)
    identity: str
    expected_resource: _ParsedAliasTopic | _ParsedInterceptor | None = dataclass_field(
        default=None,
        repr=False,
    )
    expected_upsert_result: str | None = None


@dataclass(frozen=True)
class _ManagedGatewayJournalEntry:
    inverse: _ManagedGatewayOperation = dataclass_field(repr=False)
    identity: str


@dataclass
class GatewayRuleChange:
    """A change to apply to a gateway rule."""

    name: str
    action: str  # create, update, delete, none
    current_alias: Optional[AliasTopicState] = dataclass_field(default=None, repr=False)
    current_interceptors: Optional[list[InterceptorState]] = dataclass_field(
        default=None,
        repr=False,
    )
    desired: Optional[GatewayRuleArtifact] = dataclass_field(default=None, repr=False)
    changes: Optional[dict[str, object]] = dataclass_field(default=None, repr=False)
    current: ManagedGatewayRuleObservation | None = dataclass_field(
        default=None,
        repr=False,
    )
    desired_managed: ManagedGatewayRuleObservation | None = dataclass_field(
        default=None,
        repr=False,
    )
    backend_identity: str | None = None

    def __post_init__(self) -> None:
        normalized = any(
            value is not None
            for value in (
                self.current,
                self.desired_managed,
                self.backend_identity,
            )
        )
        if not normalized:
            # Temporary compatibility for legacy planner and direct construction.
            # Secret-bearing legacy state and diffs stay out of repr.
            return
        if (
            not isinstance(self.desired, GatewayRuleArtifact)
            or not isinstance(self.current, ManagedGatewayRuleObservation)
            or not isinstance(self.desired_managed, ManagedGatewayRuleObservation)
            or not isinstance(self.backend_identity, str)
            or not is_gateway_backend_identity(self.backend_identity)
        ):
            raise GatewayDesiredAggregateError(
                "Normalized Gateway change requires a complete managed surface"
            )

        # These inputs are value objects in the normalized model. Copy them so
        # mutation of caller-owned artifact or observation objects cannot alter
        # the plan after its evidence has been computed.
        desired = deepcopy(self.desired)
        current = _copy_strict_managed_gateway_observation(self.current)
        desired_managed = _copy_strict_managed_gateway_observation(self.desired_managed)
        binding = desired_managed.binding
        if (
            not desired_managed.exists
            or current.binding != binding
            or self.backend_identity != binding.backend_identity
            or self.name != desired.name
            or self.name != desired_managed.logical_name
            or current.logical_name != desired_managed.logical_name
            or desired.virtual_topic != desired_managed.alias_name
            or current.alias_name != desired_managed.alias_name
        ):
            raise GatewayDesiredAggregateError(
                "Normalized Gateway change has mismatched managed identity"
            )
        reconstructed = build_desired_gateway_rule(desired, binding)
        if reconstructed != desired_managed:
            raise GatewayDesiredAggregateError(
                "Normalized Gateway change does not match its desired artifact"
            )

        expected_action = (
            "create" if not current.exists else "none" if current == desired_managed else "update"
        )
        if self.action != expected_action:
            raise GatewayDesiredAggregateError("Normalized Gateway change has an incoherent action")
        expected_changes = _managed_gateway_change_evidence(current, desired_managed)
        if self.action == "none":
            if self.changes not in (None, {}):
                raise GatewayChangeEvidenceError(
                    "No-op Gateway change must not contain drift evidence"
                )
            normalized_changes: dict[str, object] = {}
        else:
            normalized_changes = secret_neutral_gateway_changes(self.changes)
            if normalized_changes != expected_changes:
                raise GatewayChangeEvidenceError(
                    "Gateway change evidence does not match its managed surfaces"
                )

        self.desired = desired
        self.current = current
        self.desired_managed = desired_managed
        self.changes = normalized_changes


_GATEWAY_DRIFT_CATEGORIES = frozenset(
    {
        "alias_mapping",
        "configuration",
        "interceptor_identities",
        "physical_cluster",
        "plugin_classes",
        "presence",
        "priorities",
    }
)
_GATEWAY_CHANGE_EVIDENCE_KEYS = frozenset({"categories", "current", "desired"})
_GATEWAY_SURFACE_EVIDENCE_KEYS = frozenset({"exists", "fingerprint", "managed_interceptor_count"})


def _managed_gateway_drift_categories(
    current: ManagedGatewayRuleObservation,
    desired: ManagedGatewayRuleObservation,
) -> list[str]:
    categories: set[str] = set()
    if current.exists != desired.exists:
        categories.add("presence")
    if not current.exists or not desired.exists:
        return sorted(categories)
    if (
        current.alias_name,
        current.physical_name,
    ) != (
        desired.alias_name,
        desired.physical_name,
    ):
        categories.add("alias_mapping")
    if current.physical_cluster != desired.physical_cluster:
        categories.add("physical_cluster")
    current_by_identity = {
        (interceptor.scope, interceptor.name): interceptor for interceptor in current.interceptors
    }
    desired_by_identity = {
        (interceptor.scope, interceptor.name): interceptor for interceptor in desired.interceptors
    }
    if current_by_identity.keys() != desired_by_identity.keys():
        categories.add("interceptor_identities")
    common_identities = current_by_identity.keys() & desired_by_identity.keys()
    if any(
        current_by_identity[identity].plugin_class != desired_by_identity[identity].plugin_class
        for identity in common_identities
    ):
        categories.add("plugin_classes")
    if any(
        current_by_identity[identity].priority != desired_by_identity[identity].priority
        for identity in common_identities
    ):
        categories.add("priorities")
    if any(
        current_by_identity[identity].config_json != desired_by_identity[identity].config_json
        for identity in common_identities
    ):
        categories.add("configuration")
    return sorted(categories)


def _managed_gateway_surface_evidence(
    observation: ManagedGatewayRuleObservation,
) -> dict[str, object]:
    return {
        "exists": observation.exists,
        "fingerprint": observation.fingerprint,
        "managed_interceptor_count": len(observation.interceptors),
    }


def _managed_gateway_change_evidence(
    current: ManagedGatewayRuleObservation,
    desired: ManagedGatewayRuleObservation,
) -> dict[str, object]:
    return {
        "categories": _managed_gateway_drift_categories(current, desired),
        "current": _managed_gateway_surface_evidence(current),
        "desired": _managed_gateway_surface_evidence(desired),
    }


def _normalize_gateway_surface_evidence(value: object) -> dict[str, object]:
    if not isinstance(value, Mapping) or set(value) != _GATEWAY_SURFACE_EVIDENCE_KEYS:
        raise GatewayChangeEvidenceError(
            "Gateway change contains malformed managed-surface evidence"
        )
    exists = value.get("exists")
    fingerprint = value.get("fingerprint")
    count = value.get("managed_interceptor_count")
    if (
        type(exists) is not bool
        or not _is_fingerprint(fingerprint)
        or type(count) is not int
        or count < 0
        or (not exists and count != 0)
    ):
        raise GatewayChangeEvidenceError(
            "Gateway change contains malformed managed-surface evidence"
        )
    return {
        "exists": exists,
        "fingerprint": fingerprint,
        "managed_interceptor_count": count,
    }


def secret_neutral_gateway_changes(changes: object) -> dict[str, object]:
    """Validate and copy one exact secret-neutral normalized Gateway diff."""
    if changes is None or changes == {}:
        return {}
    if not isinstance(changes, Mapping) or set(changes) != _GATEWAY_CHANGE_EVIDENCE_KEYS:
        raise GatewayChangeEvidenceError(
            "Gateway change evidence is not the normalized exact shape"
        )
    categories = changes.get("categories")
    if (
        not isinstance(categories, list)
        or not categories
        or any(not isinstance(category, str) for category in categories)
        or categories != sorted(categories)
        or len(categories) != len(set(categories))
        or not set(categories).issubset(_GATEWAY_DRIFT_CATEGORIES)
    ):
        raise GatewayChangeEvidenceError("Gateway change contains invalid drift categories")
    current = _normalize_gateway_surface_evidence(changes.get("current"))
    desired = _normalize_gateway_surface_evidence(changes.get("desired"))
    if (
        desired["exists"] is not True
        or ("presence" in categories) != (current["exists"] != desired["exists"])
        or (
            current["managed_interceptor_count"] != desired["managed_interceptor_count"]
            and "interceptor_identities" not in categories
            and "presence" not in categories
        )
        or current["fingerprint"] == desired["fingerprint"]
    ):
        raise GatewayChangeEvidenceError(
            "Gateway change evidence is inconsistent with its managed surfaces"
        )
    return {
        "categories": list(categories),
        "current": current,
        "desired": desired,
    }


def plan_managed_gateway_rule(
    artifact: GatewayRuleArtifact,
    desired_managed: ManagedGatewayRuleObservation,
    current: ManagedGatewayRuleObservation,
) -> GatewayRuleChange:
    """Purely plan one strict compiled rule against one complete observation."""
    if (
        not isinstance(artifact, GatewayRuleArtifact)
        or not isinstance(desired_managed, ManagedGatewayRuleObservation)
        or not isinstance(current, ManagedGatewayRuleObservation)
    ):
        raise GatewayDesiredAggregateError(
            "Gateway managed planning requires complete strict inputs"
        )
    if (
        not desired_managed.exists
        or current.binding != desired_managed.binding
        or artifact.name != desired_managed.logical_name
        or current.logical_name != desired_managed.logical_name
        or artifact.virtual_topic != desired_managed.alias_name
        or current.alias_name != desired_managed.alias_name
        or build_desired_gateway_rule(artifact, desired_managed.binding) != desired_managed
    ):
        raise GatewayDesiredAggregateError(
            "Gateway managed planning inputs do not share one exact identity"
        )
    action = "create" if not current.exists else "none" if current == desired_managed else "update"
    changes = _managed_gateway_change_evidence(current, desired_managed) if action != "none" else {}
    return GatewayRuleChange(
        name=artifact.name,
        action=action,
        desired=artifact,
        changes=changes,
        current=current,
        desired_managed=desired_managed,
        backend_identity=desired_managed.binding.backend_identity,
    )


# Mapping from streamt interceptor types to Gateway plugin classes
INTERCEPTOR_PLUGINS = {
    "filter": _FILTER_PLUGIN_CLASS,
    "mask": "io.conduktor.gateway.interceptor.safeguard.FieldLevelMaskingPlugin",
    "encrypt": "io.conduktor.gateway.interceptor.FieldLevelEncryptionPlugin",
    "readonly": "io.conduktor.gateway.interceptor.safeguard.ReadOnlyTopicPolicyPlugin",
}


class GatewayDeployer:
    """Deployer for Conduktor Gateway interceptors and alias topics.

    Supports context manager protocol for proper resource cleanup:

        with GatewayDeployer(admin_url) as deployer:
            deployer.list_interceptors()

    Example:
        deployer = GatewayDeployer(
            admin_url="http://localhost:8888",
            username="admin",
            password="***"
        )
        deployer.apply(gateway_rule_artifact)
    """

    def __init__(
        self,
        admin_url: str,
        username: Optional[str] = None,
        password: Optional[str] = None,
        virtual_cluster: Optional[str] = None,
        ssl_ca_location: Optional[str] = None,
        ssl_certificate_location: Optional[str] = None,
        ssl_key_location: Optional[str] = None,
        ssl_key_password: Optional[str] = None,
        api_version: str = "v2",
    ) -> None:
        """Initialize Gateway deployer."""
        self.admin_url = _normalize_gateway_admin_url(admin_url)
        self.cluster_binding = GatewayBackendBinding.from_endpoint(
            self.admin_url,
            virtual_cluster=virtual_cluster,
            api_version=api_version,
        )
        self._api_base = f"/gateway/{_GATEWAY_API_VERSION}"
        self.auth = HTTPBasicAuth(username, password) if username and password else None
        # Preserve legacy CRUD payload behavior: an omitted scope stays omitted on
        # writes even though strict observation binds it to provider `passthrough`.
        self.virtual_cluster = virtual_cluster
        self._session = requests.Session()
        self._session.auth = self.auth
        configure_session_ssl(
            self._session,
            ssl_ca_location=ssl_ca_location,
            ssl_certificate_location=ssl_certificate_location,
            ssl_key_location=ssl_key_location,
            ssl_key_password=ssl_key_password,
        )
        self._closed = False

    def __enter__(self) -> GatewayDeployer:
        """Enter context manager."""
        return self

    def __exit__(self, exc_type: object, exc_val: object, exc_tb: object) -> None:
        """Exit context manager, cleaning up resources."""
        self.close()

    def close(self) -> None:
        """Close the deployer and clean up resources."""
        self._closed = True
        self._session.close()

    @property
    def backend_identity(self) -> str:
        """Return the canonical identity of the configured Gateway scope."""
        return self.cluster_binding.backend_identity

    @staticmethod
    def _read_managed_observation_body(response: object) -> bytes:
        """Read one strict observation response under a decoded-byte ceiling."""
        headers = getattr(response, "headers", None)
        declared_length: object = None
        if isinstance(headers, Mapping):
            declared_length = headers.get("Content-Length")
        if declared_length is not None:
            try:
                parsed_length = int(declared_length)
            except (TypeError, ValueError):
                raise GatewayManagedObservationError(
                    "Gateway managed observation response has invalid size metadata"
                ) from None
            if parsed_length < 0 or parsed_length > _MAX_MANAGED_GATEWAY_RESPONSE_BYTES:
                raise GatewayManagedObservationError(
                    "Gateway managed observation response is oversized"
                )

        iter_content = getattr(response, "iter_content", None)
        if not callable(iter_content):
            raise GatewayManagedObservationError(
                "Gateway managed observation response body is unavailable"
            )
        body = bytearray()
        try:
            for chunk in iter_content(chunk_size=_MANAGED_GATEWAY_CHUNK_BYTES):
                if not isinstance(chunk, bytes):
                    raise GatewayManagedObservationError(
                        "Gateway managed observation response body is malformed"
                    )
                if len(body) + len(chunk) > _MAX_MANAGED_GATEWAY_RESPONSE_BYTES:
                    raise GatewayManagedObservationError(
                        "Gateway managed observation response is oversized"
                    )
                body.extend(chunk)
        except GatewayManagedObservationError:
            raise
        except Exception:
            raise GatewayManagedObservationError(
                "Gateway managed observation response body could not be read"
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
            RecursionError,
            _InvalidManagedGatewayJSONError,
        ):
            raise GatewayManagedObservationError(
                "Gateway managed observation response is not canonical JSON"
            ) from None

    def _observe_managed_collection(self, endpoint: str) -> list[object]:
        """Perform one non-retried, non-redirected strict collection GET."""
        try:
            response = self._session.request(
                "GET",
                f"{self.admin_url}{self._api_base}{endpoint}",
                timeout=DEFAULT_TIMEOUT,
                allow_redirects=False,
                stream=True,
            )
        except Exception:
            raise GatewayManagedObservationError(
                "Gateway managed observation request failed"
            ) from None
        try:
            status_code = getattr(response, "status_code", None)
            if status_code in {401, 403}:
                raise GatewayManagedObservationError(
                    "Gateway managed observation authorization failed"
                )
            if type(status_code) is not int or status_code != 200:
                raise GatewayManagedObservationError(
                    "Gateway managed observation request returned an invalid status"
                )
            data = self._decode_managed_observation(self._read_managed_observation_body(response))
        finally:
            try:
                response.close()
            except Exception:
                pass
        if not isinstance(data, list):
            raise GatewayManagedObservationError(
                "Gateway managed observation response is not an array"
            )
        return data

    @staticmethod
    def _require_exact_object(
        value: object,
        *,
        required: frozenset[str],
        optional: frozenset[str] = frozenset(),
    ) -> dict[str, object]:
        if not isinstance(value, dict):
            raise GatewayManagedObservationError(
                "Gateway managed observation resource is malformed"
            )
        keys = set(value)
        if not required.issubset(keys) or not keys.issubset(required | optional):
            raise GatewayManagedObservationError(
                "Gateway managed observation resource is malformed"
            )
        return value

    @staticmethod
    def _parse_alias_topic(value: object) -> _ParsedAliasTopic:
        resource = GatewayDeployer._require_exact_object(
            value,
            required=frozenset({"kind", "apiVersion", "metadata", "spec"}),
        )
        if resource["kind"] != "AliasTopic" or resource["apiVersion"] != "gateway/v2":
            raise GatewayManagedObservationError(
                "Gateway managed alias observation resource is malformed"
            )
        metadata = GatewayDeployer._require_exact_object(
            resource["metadata"],
            required=frozenset({"name"}),
            optional=frozenset({"vCluster"}),
        )
        spec = GatewayDeployer._require_exact_object(
            resource["spec"],
            required=frozenset({"physicalName"}),
            optional=frozenset({"physicalCluster"}),
        )
        name = metadata["name"]
        physical_name = spec["physicalName"]
        if (
            not isinstance(name, str)
            or not name
            or _has_control_character(name)
            or _GATEWAY_RESOURCE_NAME.fullmatch(name) is None
            or not isinstance(physical_name, str)
            or not physical_name
            or _GATEWAY_RESOURCE_NAME.fullmatch(physical_name) is None
        ):
            raise GatewayManagedObservationError(
                "Gateway managed alias observation resource is malformed"
            )
        scope_value = metadata.get("vCluster")
        if "vCluster" in metadata and scope_value is None:
            raise GatewayManagedObservationError(
                "Gateway managed alias observation resource has invalid scope"
            )
        try:
            scope = _validate_virtual_cluster(scope_value)
        except GatewayBindingError:
            raise GatewayManagedObservationError(
                "Gateway managed alias observation resource has invalid scope"
            ) from None
        physical_cluster = spec.get("physicalCluster", "main")
        if physical_cluster != "main":
            raise GatewayManagedObservationError(
                "Gateway managed alias observation uses an unsupported physical cluster"
            )
        return _ParsedAliasTopic(
            scope=scope,
            name=name,
            physical_name=physical_name,
            physical_cluster="main",
        )

    @staticmethod
    def _parse_interceptor_scope(metadata: dict[str, object]) -> GatewayScope:
        if "scope" not in metadata:
            return ()
        raw_scope = GatewayDeployer._require_exact_object(
            metadata["scope"],
            required=frozenset(),
            optional=_KNOWN_INTERCEPTOR_SCOPE_KEYS,
        )
        if not raw_scope:
            raise GatewayManagedObservationError(
                "Gateway managed interceptor observation resource has invalid scope"
            )

        normalized: dict[str, str | None] = {}
        for key in ("group", "username"):
            value = raw_scope.get(key)
            if value is not None and (
                not isinstance(value, str)
                or not value
                or value != value.strip()
                or _has_control_character(value)
            ):
                raise GatewayManagedObservationError(
                    "Gateway managed interceptor observation resource has invalid scope"
                )
            normalized[key] = value

        if normalized["group"] is not None and normalized["username"] is not None:
            raise GatewayManagedObservationError(
                "Gateway managed interceptor observation resource has invalid scope combination"
            )

        if "vCluster" not in raw_scope:
            if normalized["group"] is None and normalized["username"] is None:
                raise GatewayManagedObservationError(
                    "Gateway managed interceptor observation resource has invalid scope"
                )
            normalized["vCluster"] = "passthrough"
        else:
            virtual_cluster = raw_scope["vCluster"]
            if virtual_cluster is None:
                if (
                    set(raw_scope) != _KNOWN_INTERCEPTOR_SCOPE_KEYS
                    or normalized["group"] is not None
                    or normalized["username"] is not None
                ):
                    raise GatewayManagedObservationError(
                        "Gateway managed interceptor observation resource has invalid scope"
                    )
                normalized["vCluster"] = None
            else:
                try:
                    normalized["vCluster"] = _validate_virtual_cluster(virtual_cluster)
                except GatewayBindingError:
                    raise GatewayManagedObservationError(
                        "Gateway managed interceptor observation resource has invalid scope"
                    ) from None
        return tuple((key, normalized[key]) for key in _CANONICAL_INTERCEPTOR_SCOPE_KEYS)

    @staticmethod
    def _parse_interceptor(value: object) -> _ParsedInterceptor:
        resource = GatewayDeployer._require_exact_object(
            value,
            required=frozenset({"kind", "apiVersion", "metadata", "spec"}),
        )
        if resource["kind"] != "Interceptor" or resource["apiVersion"] != "gateway/v2":
            raise GatewayManagedObservationError(
                "Gateway managed interceptor observation resource is malformed"
            )
        metadata = GatewayDeployer._require_exact_object(
            resource["metadata"],
            required=frozenset({"name"}),
            optional=frozenset({"scope"}),
        )
        spec = GatewayDeployer._require_exact_object(
            resource["spec"],
            required=frozenset({"pluginClass", "priority", "config"}),
            optional=frozenset({"comment"}),
        )
        name = metadata["name"]
        plugin_class = spec["pluginClass"]
        priority = spec["priority"]
        config = spec["config"]
        comment = spec.get("comment")
        if (
            not isinstance(name, str)
            or not name
            or _has_control_character(name)
            or not isinstance(plugin_class, str)
            or not plugin_class
            or type(priority) is not int
            or not isinstance(config, dict)
            or (comment is not None and not isinstance(comment, str))
        ):
            raise GatewayManagedObservationError(
                "Gateway managed interceptor observation resource is malformed"
            )
        try:
            config_json = json.dumps(
                config,
                ensure_ascii=False,
                allow_nan=False,
                separators=(",", ":"),
                sort_keys=True,
            )
        except (TypeError, ValueError):
            raise GatewayManagedObservationError(
                "Gateway managed interceptor observation config is not canonical JSON"
            ) from None
        return _ParsedInterceptor(
            scope=GatewayDeployer._parse_interceptor_scope(metadata),
            name=name,
            plugin_class=plugin_class,
            priority=priority,
            config_json=config_json,
        )

    def observe_managed_gateway_snapshot(self) -> ManagedGatewaySnapshot:
        """Fetch and parse one complete immutable two-list provider snapshot."""
        if self._closed:
            raise GatewayManagedObservationError("Gateway managed observation is closed")

        raw_aliases = self._observe_managed_collection("/alias-topic")
        raw_interceptors = self._observe_managed_collection("/interceptor")

        aliases: dict[tuple[str, str], _ParsedAliasTopic] = {}
        for raw_alias in raw_aliases:
            alias = self._parse_alias_topic(raw_alias)
            identity = (alias.scope, alias.name)
            if identity in aliases:
                raise GatewayManagedObservationError(
                    "Gateway managed alias observation contains a duplicate scoped identity"
                )
            aliases[identity] = alias

        interceptors: dict[tuple[GatewayScope, str], _ParsedInterceptor] = {}
        for raw_interceptor in raw_interceptors:
            interceptor = self._parse_interceptor(raw_interceptor)
            identity = (interceptor.scope, interceptor.name)
            if identity in interceptors:
                raise GatewayManagedObservationError(
                    "Gateway managed interceptor observation contains a duplicate scoped identity"
                )
            interceptors[identity] = interceptor

        return ManagedGatewaySnapshot(
            binding=self.cluster_binding,
            aliases=tuple(
                sorted(aliases.values(), key=lambda alias: (alias.scope, alias.name))
            ),
            interceptors=tuple(
                sorted(
                    interceptors.values(),
                    key=lambda interceptor: (
                        _gateway_scope_sort_key(interceptor.scope),
                        interceptor.name,
                    ),
                )
            ),
        )

    def observe_managed_gateway_rule(
        self,
        logical_name: str,
        alias_name: str,
    ) -> ManagedGatewayRuleObservation:
        """Observe one complete scoped rule with exactly two collection GETs."""
        logical_name, alias_name = _validate_managed_gateway_rule_identity(
            logical_name,
            alias_name,
        )
        return self.observe_managed_gateway_snapshot().rule(logical_name, alias_name)

    @staticmethod
    def _managed_scope_payload(scope: GatewayScope) -> dict[str, object]:
        if (
            not isinstance(scope, tuple)
            or tuple(key for key, _value in scope) != _CANONICAL_INTERCEPTOR_SCOPE_KEYS
        ):
            raise GatewayManagedMutationError(
                "Gateway managed mutation contains an invalid interceptor scope"
            )
        payload = dict(scope)
        if set(payload) != _KNOWN_INTERCEPTOR_SCOPE_KEYS:
            raise GatewayManagedMutationError(
                "Gateway managed mutation contains an invalid interceptor scope"
            )
        return payload

    @staticmethod
    def _managed_alias_identity(
        binding: GatewayBackendBinding,
        name: str,
    ) -> str:
        return f"AliasTopic scope={binding.virtual_cluster!r} name={name!r}"

    @staticmethod
    def _managed_interceptor_identity(
        binding: GatewayBackendBinding,
        name: str,
    ) -> str:
        return f"Interceptor scope={binding.virtual_cluster!r} name={name!r}"

    @staticmethod
    def _managed_alias_payload(
        observation: ManagedGatewayRuleObservation,
    ) -> tuple[dict[str, object], _ParsedAliasTopic]:
        if (
            not observation.exists
            or observation.physical_name is None
            or observation.physical_cluster != "main"
        ):
            raise GatewayManagedMutationError(
                "Gateway managed mutation requires complete alias content"
            )
        payload: dict[str, object] = {
            "kind": "AliasTopic",
            "apiVersion": "gateway/v2",
            "metadata": {
                "name": observation.alias_name,
                "vCluster": observation.binding.virtual_cluster,
            },
            "spec": {
                "physicalName": observation.physical_name,
                "physicalCluster": "main",
            },
        }
        try:
            expected = GatewayDeployer._parse_alias_topic(payload)
        except GatewayManagedObservationError:
            raise GatewayManagedMutationError(
                "Gateway managed mutation contains invalid alias content"
            ) from None
        return payload, expected

    @staticmethod
    def _managed_interceptor_payload(
        binding: GatewayBackendBinding,
        interceptor: ManagedGatewayInterceptor,
    ) -> tuple[dict[str, object], _ParsedInterceptor]:
        if interceptor.scope != _canonical_vcluster_scope(binding.virtual_cluster):
            raise GatewayManagedMutationError(
                "Gateway managed mutation contains a mismatched interceptor scope"
            )
        try:
            config = json.loads(
                interceptor.config_json,
                object_pairs_hook=_reject_duplicate_json_keys,
                parse_constant=_reject_nonfinite_json_constant,
            )
        except (
            TypeError,
            json.JSONDecodeError,
            RecursionError,
            _InvalidManagedGatewayJSONError,
        ):
            raise GatewayManagedMutationError(
                "Gateway managed mutation contains invalid interceptor content"
            ) from None
        if not isinstance(config, dict):
            raise GatewayManagedMutationError(
                "Gateway managed mutation contains invalid interceptor content"
            )
        payload: dict[str, object] = {
            "kind": "Interceptor",
            "apiVersion": "gateway/v2",
            "metadata": {
                "name": interceptor.name,
                "scope": GatewayDeployer._managed_scope_payload(interceptor.scope),
            },
            "spec": {
                "pluginClass": interceptor.plugin_class,
                "priority": interceptor.priority,
                "config": config,
            },
        }
        try:
            expected = GatewayDeployer._parse_interceptor(payload)
        except GatewayManagedObservationError:
            raise GatewayManagedMutationError(
                "Gateway managed mutation contains invalid interceptor content"
            ) from None
        return payload, expected

    @staticmethod
    def _managed_alias_put_operation(
        observation: ManagedGatewayRuleObservation,
        *,
        expected_result: str,
    ) -> _ManagedGatewayOperation:
        payload, expected = GatewayDeployer._managed_alias_payload(observation)
        return _ManagedGatewayOperation(
            method="PUT",
            endpoint="/alias-topic",
            payload=payload,
            identity=GatewayDeployer._managed_alias_identity(
                observation.binding,
                observation.alias_name,
            ),
            expected_resource=expected,
            expected_upsert_result=expected_result,
        )

    @staticmethod
    def _managed_alias_delete_operation(
        observation: ManagedGatewayRuleObservation,
    ) -> _ManagedGatewayOperation:
        # Constructing the full resource here validates rollback content before
        # any request even though DELETE itself carries only the exact locator.
        GatewayDeployer._managed_alias_payload(observation)
        return _ManagedGatewayOperation(
            method="DELETE",
            endpoint="/alias-topic",
            payload={
                "name": observation.alias_name,
                "vCluster": observation.binding.virtual_cluster,
            },
            identity=GatewayDeployer._managed_alias_identity(
                observation.binding,
                observation.alias_name,
            ),
        )

    @staticmethod
    def _managed_interceptor_put_operation(
        binding: GatewayBackendBinding,
        interceptor: ManagedGatewayInterceptor,
        *,
        expected_result: str,
    ) -> _ManagedGatewayOperation:
        payload, expected = GatewayDeployer._managed_interceptor_payload(
            binding,
            interceptor,
        )
        return _ManagedGatewayOperation(
            method="PUT",
            endpoint="/interceptor",
            payload=payload,
            identity=GatewayDeployer._managed_interceptor_identity(
                binding,
                interceptor.name,
            ),
            expected_resource=expected,
            expected_upsert_result=expected_result,
        )

    @staticmethod
    def _managed_interceptor_delete_operation(
        binding: GatewayBackendBinding,
        interceptor: ManagedGatewayInterceptor,
    ) -> _ManagedGatewayOperation:
        # Validate complete prior content now so a rollback PUT can never be
        # discovered to be malformed after a forward DELETE has succeeded.
        GatewayDeployer._managed_interceptor_payload(binding, interceptor)
        return _ManagedGatewayOperation(
            method="DELETE",
            endpoint=f"/interceptor/{quote(interceptor.name, safe='')}",
            payload=GatewayDeployer._managed_scope_payload(interceptor.scope),
            identity=GatewayDeployer._managed_interceptor_identity(
                binding,
                interceptor.name,
            ),
        )

    def _perform_managed_gateway_operation(
        self,
        operation: _ManagedGatewayOperation,
    ) -> None:
        """Perform one exact non-retried, non-redirected managed write."""
        if self._closed:
            raise GatewayManagedMutationError("Gateway managed mutation is closed")
        try:
            response = self._session.request(
                method=operation.method,
                url=f"{self.admin_url}{self._api_base}{operation.endpoint}",
                json=deepcopy(operation.payload),
                timeout=DEFAULT_TIMEOUT,
                allow_redirects=False,
                stream=True,
            )
        except Exception:
            raise GatewayManagedMutationError("Gateway managed mutation request failed") from None

        try:
            status_code = getattr(response, "status_code", None)
            if operation.method == "DELETE":
                if status_code in {401, 403}:
                    raise GatewayManagedMutationError(
                        "Gateway managed mutation authorization failed"
                    )
                if status_code == 404:
                    raise GatewayManagedMutationError(
                        "Gateway managed mutation conflicted with concurrent absence"
                    )
                if type(status_code) is not int or status_code != 204:
                    raise GatewayManagedMutationError(
                        "Gateway managed mutation returned an invalid delete status"
                    )
                return

            if operation.method != "PUT" or operation.expected_resource is None:
                raise GatewayManagedMutationError("Gateway managed mutation operation is invalid")
            if status_code in {401, 403}:
                raise GatewayManagedMutationError("Gateway managed mutation authorization failed")
            if type(status_code) is not int or status_code != 200:
                raise GatewayManagedMutationError(
                    "Gateway managed mutation returned an invalid upsert status"
                )
            try:
                body = self._read_managed_observation_body(response)
                decoded = self._decode_managed_observation(body)
            except GatewayManagedObservationError:
                raise GatewayManagedMutationError(
                    "Gateway managed mutation returned an invalid bounded response"
                ) from None
            if not isinstance(decoded, dict) or set(decoded) != {
                "resource",
                "upsertResult",
            }:
                raise GatewayManagedMutationError(
                    "Gateway managed mutation returned a malformed upsert result"
                )
            upsert_result = decoded["upsertResult"]
            if upsert_result not in {"Created", "Updated", "NotChanged"}:
                raise GatewayManagedMutationError(
                    "Gateway managed mutation returned a malformed upsert result"
                )
            try:
                if isinstance(operation.expected_resource, _ParsedAliasTopic):
                    observed_resource: _ParsedAliasTopic | _ParsedInterceptor = (
                        self._parse_alias_topic(decoded["resource"])
                    )
                else:
                    raw_resource = decoded["resource"]
                    if not isinstance(raw_resource, dict):
                        raise GatewayManagedObservationError
                    raw_metadata = raw_resource.get("metadata")
                    raw_spec = raw_resource.get("spec")
                    expected_payload_scope = dict(operation.expected_resource.scope)
                    if (
                        not isinstance(raw_metadata, dict)
                        or raw_metadata.get("scope") != expected_payload_scope
                        or not isinstance(raw_spec, dict)
                        or (
                            "comment" in raw_spec
                            and raw_spec.get("comment") != ""
                        )
                    ):
                        raise GatewayManagedObservationError
                    observed_resource = self._parse_interceptor(decoded["resource"])
            except GatewayManagedObservationError:
                raise GatewayManagedMutationError(
                    "Gateway managed mutation returned a malformed managed resource"
                ) from None
            if observed_resource != operation.expected_resource:
                raise GatewayManagedMutationError(
                    "Gateway managed mutation response does not match the requested resource"
                )
            if upsert_result != operation.expected_upsert_result:
                raise GatewayManagedMutationError(
                    "Gateway managed mutation conflicted with provider ownership"
                )
        finally:
            try:
                response.close()
            except Exception:
                pass

    def _execute_managed_gateway_operations(
        self,
        operations: list[tuple[_ManagedGatewayOperation, _ManagedGatewayOperation]],
    ) -> None:
        journal: list[_ManagedGatewayJournalEntry] = []
        unresolved_forward: str | None = None
        try:
            for forward, inverse in operations:
                try:
                    self._perform_managed_gateway_operation(forward)
                except GatewayManagedMutationError:
                    unresolved_forward = forward.identity
                    raise
                except Exception:
                    unresolved_forward = forward.identity
                    raise GatewayManagedMutationError(
                        "Gateway managed mutation response handling failed"
                    ) from None
                journal.append(
                    _ManagedGatewayJournalEntry(
                        inverse=inverse,
                        identity=forward.identity,
                    )
                )
        except GatewayManagedMutationError:
            unresolved: list[str] = (
                [unresolved_forward] if unresolved_forward is not None else []
            )
            for entry in reversed(journal):
                try:
                    self._perform_managed_gateway_operation(entry.inverse)
                except GatewayManagedMutationError:
                    unresolved.append(entry.identity)
            if unresolved:
                raise GatewayManagedMutationError(
                    "Gateway managed mutation failed with unresolved mutation identities: "
                    + "; ".join(unresolved)
                ) from None
            raise

    def apply_managed_gateway_rule(
        self,
        current: ManagedGatewayRuleObservation,
        desired: ManagedGatewayRuleObservation,
    ) -> str:
        """Apply one exact complete managed Gateway transition."""
        if not isinstance(current, ManagedGatewayRuleObservation) or not isinstance(
            desired,
            ManagedGatewayRuleObservation,
        ):
            raise GatewayManagedMutationError(
                "Gateway managed apply requires complete observations"
            )
        action = "create" if not current.exists else "no-op" if current == desired else "update"
        mutation = GatewayRuleMutation(
            expected_action=action,
            current=current,
            desired=desired,
        )
        current = mutation.current
        desired = mutation.desired
        if desired is None or desired.binding != self.cluster_binding:
            raise GatewayManagedMutationError(
                "Gateway managed apply binding does not match the deployer"
            )
        if action == "no-op":
            return "unchanged"

        operations: list[tuple[_ManagedGatewayOperation, _ManagedGatewayOperation]] = []
        alias_changed = (
            not current.exists
            or current.physical_name != desired.physical_name
            or current.physical_cluster != desired.physical_cluster
        )
        if alias_changed:
            forward_alias = self._managed_alias_put_operation(
                desired,
                expected_result="Created" if not current.exists else "Updated",
            )
            inverse_alias = (
                self._managed_alias_delete_operation(desired)
                if not current.exists
                else self._managed_alias_put_operation(current, expected_result="Updated")
            )
            operations.append((forward_alias, inverse_alias))

        current_interceptors = {
            (interceptor.scope, interceptor.name): interceptor
            for interceptor in current.interceptors
        }
        desired_interceptors = {
            (interceptor.scope, interceptor.name): interceptor
            for interceptor in desired.interceptors
        }
        for identity in sorted(
            desired_interceptors,
            key=lambda value: (_gateway_scope_sort_key(value[0]), value[1]),
        ):
            desired_interceptor = desired_interceptors[identity]
            current_interceptor = current_interceptors.get(identity)
            if current_interceptor == desired_interceptor:
                continue
            forward_interceptor = self._managed_interceptor_put_operation(
                desired.binding,
                desired_interceptor,
                expected_result="Created" if current_interceptor is None else "Updated",
            )
            inverse_interceptor = (
                self._managed_interceptor_delete_operation(
                    desired.binding,
                    desired_interceptor,
                )
                if current_interceptor is None
                else self._managed_interceptor_put_operation(
                    current.binding,
                    current_interceptor,
                    expected_result="Updated",
                )
            )
            operations.append((forward_interceptor, inverse_interceptor))

        stale_identities = current_interceptors.keys() - desired_interceptors.keys()
        for identity in sorted(
            stale_identities,
            key=lambda value: (_gateway_scope_sort_key(value[0]), value[1]),
        ):
            stale = current_interceptors[identity]
            operations.append(
                (
                    self._managed_interceptor_delete_operation(current.binding, stale),
                    self._managed_interceptor_put_operation(
                        current.binding,
                        stale,
                        expected_result="Created",
                    ),
                )
            )

        self._execute_managed_gateway_operations(operations)
        return "created" if action == "create" else "updated"

    def delete_managed_gateway_rule(
        self,
        current: ManagedGatewayRuleObservation,
    ) -> str:
        """Delete exactly one complete present managed Gateway aggregate."""
        mutation = GatewayRuleMutation(
            expected_action="delete",
            current=current,
            desired=None,
        )
        current = mutation.current
        if current.binding != self.cluster_binding:
            raise GatewayManagedMutationError(
                "Gateway managed delete binding does not match the deployer"
            )

        operations: list[tuple[_ManagedGatewayOperation, _ManagedGatewayOperation]] = []
        for interceptor in sorted(
            current.interceptors,
            key=lambda item: (_gateway_scope_sort_key(item.scope), item.name),
        ):
            operations.append(
                (
                    self._managed_interceptor_delete_operation(
                        current.binding,
                        interceptor,
                    ),
                    self._managed_interceptor_put_operation(
                        current.binding,
                        interceptor,
                        expected_result="Created",
                    ),
                )
            )
        operations.append(
            (
                self._managed_alias_delete_operation(current),
                self._managed_alias_put_operation(current, expected_result="Created"),
            )
        )
        self._execute_managed_gateway_operations(operations)
        return "deleted"

    def _request(
        self,
        method: str,
        endpoint: str,
        json: Optional[dict[str, object]] = None,
        params: Optional[dict[str, str]] = None,
        not_found_ok: bool = False,
    ) -> dict | list | None:
        """Make an authenticated request to the Gateway API. Returns parsed JSON.

        Raises on HTTP errors. If not_found_ok=True, returns None on 404.
        """
        if self._closed:
            raise RuntimeError("GatewayDeployer is closed")
        url = f"{self.admin_url}{self._api_base}{endpoint}"

        last_err: Optional[Exception] = None
        for attempt in range(3):
            try:
                response = self._session.request(
                    method=method,
                    url=url,
                    json=json,
                    params=params,
                    timeout=DEFAULT_TIMEOUT,
                )
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
            raise GatewayConnectionError(
                f"Cannot connect to Gateway at {self.admin_url}. Is it running? Error: {last_err}"
            ) from last_err

        if response.status_code == 401:
            raise GatewayAuthenticationError(
                "Gateway authentication failed. Check username/password."
            )

        if not_found_ok and response.status_code == 404:
            return None

        if response.status_code == 204:
            return None

        response.raise_for_status()
        return response.json()

    def health_check(self) -> bool:
        """Check if Gateway is healthy."""
        try:
            response = self._session.get(
                f"{self.admin_url}/health",
                timeout=DEFAULT_TIMEOUT,
            )
            return response.status_code == 200
        except requests.ConnectionError:
            return False

    # -------------------------------------------------------------------------
    # Interceptors
    # -------------------------------------------------------------------------

    def list_interceptors(self) -> list[dict[str, object]]:
        """List all interceptors."""
        data = self._request("GET", "/interceptor")
        return data if isinstance(data, list) else []

    def get_interceptor(self, name: str) -> Optional[dict[str, object]]:
        """Get a specific interceptor by name."""
        interceptors = self.list_interceptors()
        for interceptor in interceptors:
            # v2 API returns resources with metadata.name
            int_name = interceptor.get("metadata", {}).get("name") or interceptor.get("name")
            if int_name == name:
                return interceptor
        return None

    def create_interceptor(
        self,
        name: str,
        plugin_class: str,
        config: dict[str, object],
        vcluster: Optional[str] = None,
        priority: int = 100,
    ) -> dict[str, object]:
        """Create or update an interceptor using Gateway v2 API.

        Args:
            name: Unique name for the interceptor
            plugin_class: Full class name of the Gateway plugin
            config: Plugin-specific configuration
            vcluster: Optional virtual cluster scope
            priority: Interceptor priority (lower = earlier)

        Returns:
            The created/updated interceptor configuration
        """
        # Build scope
        scope: dict[str, object] = {}
        if vcluster:
            scope["vCluster"] = vcluster
        elif self.virtual_cluster:
            scope["vCluster"] = self.virtual_cluster

        # Build metadata
        metadata: dict[str, object] = {"name": name}
        if scope:
            metadata["scope"] = scope

        # Build v2 API payload
        payload: dict[str, object] = {
            "kind": "Interceptor",
            "apiVersion": "gateway/v2",
            "metadata": metadata,
            "spec": {
                "pluginClass": plugin_class,
                "priority": priority,
                "config": config,
            },
        }

        self._request("PUT", "/interceptor", json=payload)
        logger.info(f"Created/updated interceptor '{name}'")
        return payload

    def delete_interceptor(self, name: str) -> bool:
        """Delete an interceptor by name."""
        data = self._request("DELETE", f"/interceptor/{name}", not_found_ok=True)
        if data is None:
            return False
        logger.info(f"Deleted interceptor '{name}'")
        return True

    # -------------------------------------------------------------------------
    # Alias Topics
    # -------------------------------------------------------------------------

    def list_alias_topics(self) -> list[dict[str, object]]:
        """List all alias topic mappings."""
        data = self._request("GET", "/alias-topic")
        return data if isinstance(data, list) else []

    def get_alias_topic(self, name: str) -> Optional[dict[str, object]]:
        """Get a specific alias topic by name."""
        aliases = self.list_alias_topics()
        for alias in aliases:
            # v2 API returns resources with metadata.name
            alias_name = alias.get("metadata", {}).get("name") or alias.get("name")
            if alias_name == name:
                return alias
        return None

    def create_alias_topic(
        self,
        name: str,
        physical_topic: str,
        vcluster: Optional[str] = None,
    ) -> dict[str, object]:
        """Create or update an alias topic mapping using Gateway v2 API.

        Args:
            name: Virtual topic name (what consumers use)
            physical_topic: Physical Kafka topic name
            vcluster: Optional virtual cluster scope

        Returns:
            The created/updated alias configuration
        """
        # Build metadata
        metadata: dict[str, object] = {"name": name}
        if vcluster:
            metadata["vCluster"] = vcluster
        elif self.virtual_cluster:
            metadata["vCluster"] = self.virtual_cluster

        # Build v2 API payload
        payload: dict[str, object] = {
            "kind": "AliasTopic",
            "apiVersion": "gateway/v2",
            "metadata": metadata,
            "spec": {
                "physicalName": physical_topic,
            },
        }

        self._request("PUT", "/alias-topic", json=payload)
        logger.info(f"Created/updated alias topic '{name}' -> '{physical_topic}'")
        return payload

    def delete_alias_topic(self, name: str, vcluster: Optional[str] = None) -> bool:
        """Delete an alias topic by name using request body (Gateway v2 format)."""
        # Gateway v2 uses DELETE with body, not path parameter
        body: dict[str, object] = {"name": name}
        if vcluster:
            body["vCluster"] = vcluster
        elif self.virtual_cluster:
            body["vCluster"] = self.virtual_cluster

        data = self._request("DELETE", "/alias-topic", json=body, not_found_ok=True)
        if data is None:
            return False
        logger.info(f"Deleted alias topic '{name}'")
        return True

    # -------------------------------------------------------------------------
    # Gateway Rules (combined alias + interceptors)
    # -------------------------------------------------------------------------

    _VALID_RESOURCE_NAME = _GATEWAY_RESOURCE_NAME

    def apply(self, artifact: GatewayRuleArtifact) -> str:
        """Deploy a gateway rule (alias topic + interceptors).

        Returns action taken: "created", "updated", or "unchanged"
        """
        if not self._VALID_RESOURCE_NAME.match(artifact.name):
            raise ValueError(
                f"Invalid gateway rule name '{artifact.name}'. "
                "Names must contain only alphanumeric characters, underscores, hyphens, and dots."
            )

        # 1. Create alias topic mapping
        alias_existed = self.get_alias_topic(artifact.virtual_topic) is not None
        self.create_alias_topic(
            name=artifact.virtual_topic,
            physical_topic=artifact.physical_topic,
        )

        # 2. Delete orphaned interceptors (existing ones not in desired list)
        existing_interceptors = self.list_interceptors()
        rule_interceptors = [
            i for i in existing_interceptors
            if (i.get("metadata", {}).get("name") or i.get("name", "")).startswith(f"{artifact.name}_")
        ]
        desired_names: set[str] = set()
        created_names: list[str] = []

        # 3. Create interceptors for this rule
        try:
            for i, interceptor_config in enumerate(artifact.interceptors):
                interceptor_type = interceptor_config.get("type", "filter")
                config = interceptor_config.get("config", {})

                plugin_class = INTERCEPTOR_PLUGINS.get(interceptor_type)
                if not plugin_class:
                    logger.warning(
                        f"Unknown interceptor type '{interceptor_type}', skipping"
                    )
                    continue

                plugin_config = self._build_plugin_config(
                    interceptor_type, config, artifact
                )

                interceptor_name = f"{artifact.name}_{interceptor_type}_{i}"
                desired_names.add(interceptor_name)
                self.create_interceptor(
                    name=interceptor_name,
                    plugin_class=plugin_class,
                    config=plugin_config,
                )
                created_names.append(interceptor_name)
        except Exception:
            # Roll back: delete interceptors created in this batch
            rollback_failures: list[str] = []
            for name in created_names:
                try:
                    self.delete_interceptor(name)
                except Exception as cleanup_err:
                    rollback_failures.append(f"interceptor {name}: {cleanup_err}")
                    logger.debug("Failed to roll back interceptor %s: %s", name, cleanup_err)
            # Roll back alias if we just created it
            if not alias_existed:
                try:
                    self.delete_alias_topic(artifact.virtual_topic)
                except Exception as cleanup_err:
                    rollback_failures.append(f"alias {artifact.virtual_topic}: {cleanup_err}")
                    logger.debug("Failed to roll back alias %s: %s", artifact.virtual_topic, cleanup_err)
            if rollback_failures:
                logger.warning(
                    "Partial rollback failure for rule '%s'. Orphaned resources may remain: %s",
                    artifact.name, "; ".join(rollback_failures),
                )
            raise

        # 4. Remove orphaned interceptors
        for existing in rule_interceptors:
            name = existing.get("metadata", {}).get("name") or existing.get("name", "")
            if name not in desired_names:
                self.delete_interceptor(name)

        return "updated" if alias_existed else "created"

    def _build_plugin_config(
        self,
        interceptor_type: str,
        config: dict[str, object],
        artifact: GatewayRuleArtifact,
    ) -> dict[str, object]:
        """Build plugin-specific configuration."""
        if interceptor_type == "filter":
            # VirtualSqlTopicPlugin config (Gateway v2)
            # Topic names with dashes must be double-quoted in SQL
            where_clause = config.get("where", "")
            quoted_topic = f'"{artifact.physical_topic}"'
            return {
                "virtualTopic": artifact.virtual_topic,
                "statement": f"SELECT * FROM {quoted_topic} WHERE {where_clause}",
            }

        elif interceptor_type == "mask":
            # FieldLevelMaskingPlugin config
            return {
                "policies": [
                    {
                        "name": f"mask_{config.get('field', 'unknown')}",
                        "rule": {
                            "type": config.get("method", "MASK_ALL"),
                            "maskingString": "***",
                        },
                        "fields": [config.get("field")],
                    }
                ]
            }

        elif interceptor_type == "encrypt":
            # FieldLevelEncryptionPlugin config
            return {
                "fields": [config.get("field")],
                "algorithm": config.get("algorithm", "AES256_GCM"),
            }

        else:
            return config

    def delete(self, name: str) -> bool:
        """Delete a gateway rule by name (alias + all related interceptors)."""
        deleted = False

        # Delete alias topic
        if self.delete_alias_topic(name):
            deleted = True

        # Delete related interceptors (by prefix)
        interceptors = self.list_interceptors()
        for interceptor in interceptors:
            # v2 API uses metadata.name
            int_name = interceptor.get("metadata", {}).get("name") or interceptor.get("name", "")
            if int_name.startswith(f"{name}_"):
                self.delete_interceptor(int_name)
                deleted = True

        return deleted

    def plan(self, artifact: GatewayRuleArtifact) -> GatewayRuleChange:
        """Plan changes for a gateway rule."""
        alias_state = self.get_alias_topic(artifact.virtual_topic)

        if alias_state is None:
            return GatewayRuleChange(
                name=artifact.name,
                action="create",
                desired=artifact,
            )

        # Check if physical topic changed (v2 API uses spec.physicalName)
        current_physical = (
            alias_state.get("spec", {}).get("physicalName")
            or alias_state.get("physicalName")
        )
        changes: dict[str, object] = {}
        if current_physical != artifact.physical_topic:
            changes["physical_topic"] = {"from": current_physical, "to": artifact.physical_topic}

        # Check interceptor changes (count and config)
        current_interceptors = self.list_interceptors()
        rule_interceptors = [
            i for i in current_interceptors
            if (i.get("metadata", {}).get("name") or i.get("name", "")).startswith(f"{artifact.name}_")
        ]
        desired_count = len(artifact.interceptors) if artifact.interceptors else 0
        if len(rule_interceptors) != desired_count:
            changes["interceptors"] = {"from": len(rule_interceptors), "to": desired_count}
        elif desired_count > 0:
            # Compare interceptor configs
            for idx, desired_int in enumerate(artifact.interceptors):
                if idx < len(rule_interceptors):
                    current_spec = rule_interceptors[idx].get("spec", {}).get("config", {})
                    desired_config = desired_int.get("config", {})
                    if current_spec != desired_config:
                        changes["interceptors"] = {"from": "current_config", "to": "desired_config"}
                        break

        if changes:
            return GatewayRuleChange(
                name=artifact.name,
                action="update",
                current_alias=AliasTopicState(
                    name=artifact.virtual_topic,
                    exists=True,
                    physical_topic=current_physical,
                ),
                desired=artifact,
                changes=changes,
            )

        return GatewayRuleChange(
            name=artifact.name,
            action="none",
            current_alias=AliasTopicState(
                name=artifact.virtual_topic,
                exists=True,
                physical_topic=current_physical,
            ),
            desired=artifact,
        )

    def list_rules(self) -> list[dict[str, object]]:
        """List all gateway rules (alias topics with their interceptors)."""
        aliases = self.list_alias_topics()
        interceptors = self.list_interceptors()

        rules = []
        for alias in aliases:
            # v2 API uses metadata.name and spec.physicalName
            alias_name = alias.get("metadata", {}).get("name") or alias.get("name", "")
            physical_topic = (
                alias.get("spec", {}).get("physicalName")
                or alias.get("physicalName")
            )

            related_interceptors = [
                i for i in interceptors
                if (i.get("metadata", {}).get("name") or i.get("name", "")).startswith(f"{alias_name}_")
            ]

            rules.append({
                "name": alias_name,
                "physical_topic": physical_topic,
                "interceptors": related_interceptors,
            })

        return rules
