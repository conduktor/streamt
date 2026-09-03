"""Strict, secret-neutral input parsing for catalog adapters."""

from __future__ import annotations

import json
import re
import unicodedata
from collections.abc import Collection, Mapping
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType

_CATALOG_ID_RE = re.compile(
    r"^[a-z0-9](?:[a-z0-9._-]{0,126}[a-z0-9])?$"
)
_NAMESPACE_RE = re.compile(r"^[a-z0-9](?:-?[a-z0-9])*$")
_NAME_RE = re.compile(r"^[a-z0-9](?:[-_.]?[a-z0-9])*$")
_KIND_RE = re.compile(r"^[a-z][a-z0-9]*(?:[-_.][a-z0-9]+)*$")
_MAX_OWNER_MAP_BYTES = 1_048_576
_MAX_JSON_DEPTH = 4
_MAX_OWNERS = 10_000
_MAX_LABEL_CODEPOINTS = 256
_MAX_REF_CODEPOINTS = 256


class CatalogInputError(ValueError):
    """A safe catalog-input failure carrying one structural location."""

    def __init__(self, message: str, *, location: str) -> None:
        super().__init__(message)
        self.location = location


@dataclass(frozen=True)
class ParsedEntityRef:
    """One validated, complete Backstage entity reference."""

    kind: str
    namespace: str
    name: str
    canonical: str

    def __post_init__(self) -> None:
        if (
            _KIND_RE.fullmatch(self.kind) is None
            or len(self.namespace) > 63
            or _NAMESPACE_RE.fullmatch(self.namespace) is None
            or len(self.name) > 63
            or _NAME_RE.fullmatch(self.name) is None
            or self.canonical != f"{self.kind}:{self.namespace}/{self.name}"
        ):
            raise CatalogInputError(
                "Parsed entity reference is inconsistent",
                location="entity_ref",
            )


def require_catalog_id(value: object) -> str:
    """Validate and preserve one stable catalog identity."""
    if not isinstance(value, str) or _CATALOG_ID_RE.fullmatch(value) is None:
        raise CatalogInputError(
            "Catalog ID must be 1 to 128 lowercase identifier characters",
            location="catalog_id",
        )
    return value


def require_catalog_namespace(value: object) -> str:
    """Validate and preserve one explicit lowercase Backstage namespace."""
    if (
        not isinstance(value, str)
        or len(value) > 63
        or _NAMESPACE_RE.fullmatch(value) is None
    ):
        raise CatalogInputError(
            "Catalog namespace must be a valid lowercase namespace",
            location="catalog_namespace",
        )
    return value


def require_lifecycle(value: object) -> str:
    """Validate and preserve an explicit Backstage lifecycle label."""
    if (
        not isinstance(value, str)
        or not value.strip()
        or len(value) > _MAX_LABEL_CODEPOINTS
        or _has_control_or_surrogate(value)
    ):
        raise CatalogInputError(
            "Lifecycle must be a non-blank string of at most 256 code points",
            location="lifecycle",
        )
    return value


def require_entity_ref(
    value: object,
    *,
    allowed_kinds: Collection[str],
    location: str,
) -> ParsedEntityRef:
    """Parse a complete lowercase ``kind:namespace/name`` entity reference."""
    if (
        not isinstance(allowed_kinds, Collection)
        or isinstance(allowed_kinds, (str, bytes))
        or not allowed_kinds
        or any(
            not isinstance(kind, str)
            or _KIND_RE.fullmatch(kind) is None
            or kind != kind.lower()
            for kind in allowed_kinds
        )
    ):
        raise CatalogInputError(
            "Allowed entity reference kinds are invalid",
            location=location,
        )
    if (
        not isinstance(value, str)
        or not value
        or len(value) > _MAX_REF_CODEPOINTS
        or _has_control_or_surrogate(value)
    ):
        raise CatalogInputError(
            "Entity reference must be a complete lowercase reference",
            location=location,
        )

    match = re.fullmatch(r"([^:]+):([^/]+)/([^/]+)", value)
    if match is None:
        raise CatalogInputError(
            "Entity reference must use kind:namespace/name",
            location=location,
        )
    kind, namespace, name = match.groups()
    if (
        kind not in allowed_kinds
        or _KIND_RE.fullmatch(kind) is None
        or len(namespace) > 63
        or _NAMESPACE_RE.fullmatch(namespace) is None
        or len(name) > 63
        or _NAME_RE.fullmatch(name) is None
    ):
        raise CatalogInputError(
            "Entity reference kind or components are not allowed",
            location=location,
        )
    return ParsedEntityRef(
        kind=kind,
        namespace=namespace,
        name=name,
        canonical=value,
    )


def validate_owner_map(
    mapping: object,
) -> Mapping[str, ParsedEntityRef]:
    """Validate an already decoded version-1 owner map and freeze a copy."""
    if not isinstance(mapping, Mapping):
        raise CatalogInputError(
            "Owner map root must be a JSON object",
            location="owner_map",
        )
    if set(mapping) != {"version", "owners"}:
        raise CatalogInputError(
            "Owner map must contain exactly version and owners",
            location="owner_map",
        )
    if type(mapping["version"]) is not int or mapping["version"] != 1:
        raise CatalogInputError(
            "Owner map version must be integer 1",
            location="owner_map/version",
        )
    owners = mapping["owners"]
    if not isinstance(owners, Mapping):
        raise CatalogInputError(
            "Owner map owners must be a JSON object",
            location="owner_map/owners",
        )
    if len(owners) > _MAX_OWNERS:
        raise CatalogInputError(
            "Owner map may contain at most 10000 owners",
            location="owner_map/owners",
        )
    if any(not isinstance(label, str) for label in owners):
        raise CatalogInputError(
            "Owner map labels must be strings",
            location="owner_map/owners",
        )

    result: dict[str, ParsedEntityRef] = {}
    for index, (label, raw_ref) in enumerate(
        sorted(owners.items(), key=lambda item: item[0])
    ):
        label_location = f"owner_map/owners/{index}/label"
        if (
            not label.strip()
            or len(label) > _MAX_LABEL_CODEPOINTS
            or _has_control_or_surrogate(label)
        ):
            raise CatalogInputError(
                "Owner label must be non-blank and at most 256 code points",
                location=label_location,
            )
        ref_location = f"owner_map/owners/{index}/ref"
        if not isinstance(raw_ref, str) or len(raw_ref) > _MAX_REF_CODEPOINTS:
            raise CatalogInputError(
                "Owner reference must be a string of at most 256 code points",
                location=ref_location,
            )
        result[label] = require_entity_ref(
            raw_ref,
            allowed_kinds=frozenset({"group", "user"}),
            location=ref_location,
        )
    return MappingProxyType(result)


def load_owner_map(path: Path) -> Mapping[str, ParsedEntityRef]:
    """Read one bounded strict UTF-8 JSON owner map without interpolation."""
    if not isinstance(path, Path):
        raise CatalogInputError(
            "Owner map path must be a filesystem path",
            location="owner_map",
        )
    try:
        with path.open("rb") as source:
            payload = source.read(_MAX_OWNER_MAP_BYTES + 1)
    except OSError:
        raise CatalogInputError(
            "Owner map could not be read",
            location="owner_map",
        ) from None
    if len(payload) > _MAX_OWNER_MAP_BYTES:
        raise CatalogInputError(
            "Owner map exceeds the 1048576-byte limit",
            location="owner_map",
        )
    if payload.startswith(b"\xef\xbb\xbf"):
        raise CatalogInputError(
            "Owner map must not contain a UTF-8 byte-order mark",
            location="owner_map",
        )
    try:
        text = payload.decode("utf-8", errors="strict")
    except UnicodeDecodeError:
        raise CatalogInputError(
            "Owner map must be strict UTF-8",
            location="owner_map",
        ) from None

    try:
        decoded = json.loads(
            text,
            object_pairs_hook=_unique_json_object,
            parse_constant=_reject_json_constant,
        )
    except (
        _DuplicateJsonKeyError,
        _InvalidJsonConstantError,
        json.JSONDecodeError,
        RecursionError,
    ):
        raise CatalogInputError(
            "Owner map must be strict JSON without duplicate keys",
            location="owner_map",
        ) from None
    try:
        depth = _json_depth(decoded)
        has_surrogate = _contains_surrogate(decoded)
    except RecursionError:
        raise CatalogInputError(
            "Owner map exceeds the maximum JSON depth",
            location="owner_map",
        ) from None
    if depth > _MAX_JSON_DEPTH:
        raise CatalogInputError(
            "Owner map exceeds the maximum JSON depth",
            location="owner_map",
        )
    if has_surrogate:
        raise CatalogInputError(
            "Owner map contains invalid Unicode",
            location="owner_map",
        )
    return validate_owner_map(decoded)


class _DuplicateJsonKeyError(ValueError):
    """Private decoder sentinel that never retains the duplicate key."""


class _InvalidJsonConstantError(ValueError):
    """Private decoder sentinel for non-standard JSON numeric constants."""


def _unique_json_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise _DuplicateJsonKeyError
        result[key] = value
    return result


def _reject_json_constant(_value: str) -> object:
    raise _InvalidJsonConstantError


def _json_depth(value: object) -> int:
    if isinstance(value, Mapping):
        return 1 + max((_json_depth(item) for item in value.values()), default=0)
    if isinstance(value, list):
        return 1 + max((_json_depth(item) for item in value), default=0)
    return 0


def _contains_surrogate(value: object) -> bool:
    if isinstance(value, str):
        return any(unicodedata.category(character) == "Cs" for character in value)
    if isinstance(value, Mapping):
        return any(
            _contains_surrogate(key) or _contains_surrogate(item)
            for key, item in value.items()
        )
    if isinstance(value, list):
        return any(_contains_surrogate(item) for item in value)
    return False


def _has_control_or_surrogate(value: str) -> bool:
    return any(
        unicodedata.category(character) in {"Cc", "Cs"}
        for character in value
    )
