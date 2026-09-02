"""Strict parsing for compiled Conduktor Gateway rule artifacts."""

from __future__ import annotations

import math

from streamt.compiler.manifest import ArtifactOwnership, GatewayRuleArtifact

_REQUIRED_FIELDS = frozenset(
    {"name", "virtualTopic", "physicalTopic", "interceptors"}
)
_OPTIONAL_FIELDS = frozenset({"ownership"})
_OWNERSHIP_FIELDS = frozenset({"mode", "project", "type", "name"})
_OWNERSHIP_MODES = frozenset({"managed", "adopted", "external"})
_INTERCEPTOR_FIELDS = frozenset({"type", "config"})
_SUPPORTED_INTERCEPTOR_TYPES = frozenset({"filter", "mask"})


class GatewayArtifactFormatError(ValueError):
    """A compiled Gateway rule artifact is malformed or ambiguous."""


def _require_nonempty_string(value: object, *, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise GatewayArtifactFormatError(
            f"compiled Gateway field {field!r} must be a non-empty string"
        )
    return value


def _require_exact_fields(
    value: dict[object, object],
    *,
    required: frozenset[str],
    optional: frozenset[str] = frozenset(),
    label: str,
) -> None:
    fields: set[str] = set()
    for key in value:
        if not isinstance(key, str):
            raise GatewayArtifactFormatError(f"{label} keys must be strings")
        fields.add(key)
    missing = sorted(required - fields)
    if missing:
        raise GatewayArtifactFormatError(
            f"{label} is missing field {missing[0]!r}"
        )
    unsupported = sorted(fields - required - optional)
    if unsupported:
        raise GatewayArtifactFormatError(
            f"{label} has unsupported field {unsupported[0]!r}"
        )


def _copy_json_value(
    value: object,
    *,
    field: str,
    active_containers: set[int],
) -> object:
    """Copy one exact finite JSON value while rejecting cycles and Python objects."""
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        if math.isfinite(value):
            return value
        raise GatewayArtifactFormatError(
            f"compiled Gateway field {field!r} must contain only finite JSON values"
        )
    if isinstance(value, list):
        identity = id(value)
        if identity in active_containers:
            raise GatewayArtifactFormatError(
                f"compiled Gateway field {field!r} must contain only finite JSON values"
            )
        active_containers.add(identity)
        try:
            return [
                _copy_json_value(
                    item,
                    field=field,
                    active_containers=active_containers,
                )
                for item in value
            ]
        finally:
            active_containers.remove(identity)
    if isinstance(value, dict):
        identity = id(value)
        if identity in active_containers:
            raise GatewayArtifactFormatError(
                f"compiled Gateway field {field!r} must contain only finite JSON values"
            )
        active_containers.add(identity)
        try:
            copied: dict[str, object] = {}
            for key, item in value.items():
                if not isinstance(key, str):
                    raise GatewayArtifactFormatError(
                        f"compiled Gateway field {field!r} object keys must be strings"
                    )
                copied[key] = _copy_json_value(
                    item,
                    field=field,
                    active_containers=active_containers,
                )
            return copied
        finally:
            active_containers.remove(identity)
    raise GatewayArtifactFormatError(
        f"compiled Gateway field {field!r} must contain only finite JSON values"
    )


def _parse_ownership(value: object) -> ArtifactOwnership:
    if not isinstance(value, dict):
        raise GatewayArtifactFormatError(
            "compiled Gateway field 'ownership' must be an exact ownership object"
        )
    _require_exact_fields(
        value,
        required=_OWNERSHIP_FIELDS,
        label="compiled Gateway ownership",
    )
    mode = _require_nonempty_string(value["mode"], field="ownership.mode")
    if mode not in _OWNERSHIP_MODES:
        raise GatewayArtifactFormatError(
            "compiled Gateway field 'ownership.mode' is unsupported"
        )
    return ArtifactOwnership(
        project=_require_nonempty_string(
            value["project"], field="ownership.project"
        ),
        owner_type=_require_nonempty_string(
            value["type"], field="ownership.type"
        ),
        owner_name=_require_nonempty_string(
            value["name"], field="ownership.name"
        ),
        mode=mode,
    )


def _parse_roles(value: object, *, index: int) -> list[str]:
    if not isinstance(value, list):
        raise GatewayArtifactFormatError(
            "compiled Gateway mask config field 'forRoles' must be a list"
        )
    roles: list[str] = []
    seen: set[str] = set()
    for role in value:
        parsed = _require_nonempty_string(
            role,
            field=f"interceptors[{index}].config.forRoles[]",
        )
        if parsed in seen:
            raise GatewayArtifactFormatError(
                "compiled Gateway mask config field 'forRoles' must contain unique roles"
            )
        seen.add(parsed)
        roles.append(parsed)
    return roles


def _parse_interceptor_config(
    interceptor_type: str,
    value: object,
    *,
    index: int,
) -> dict[str, object]:
    if not isinstance(value, dict):
        raise GatewayArtifactFormatError(
            f"compiled Gateway interceptor {index} field 'config' must be an object"
        )
    copied_value = _copy_json_value(
        value,
        field=f"interceptors[{index}].config",
        active_containers=set(),
    )
    if not isinstance(copied_value, dict):  # pragma: no cover - narrowed above
        raise GatewayArtifactFormatError(
            f"compiled Gateway interceptor {index} field 'config' must be an object"
        )
    config = copied_value

    if interceptor_type == "filter":
        _require_exact_fields(
            config,
            required=frozenset({"where"}),
            label="compiled Gateway filter config",
        )
        return {
            "where": _require_nonempty_string(
                config["where"],
                field=f"interceptors[{index}].config.where",
            )
        }

    if interceptor_type == "mask":
        _require_exact_fields(
            config,
            required=frozenset({"field", "method"}),
            optional=frozenset({"forRoles"}),
            label="compiled Gateway mask config",
        )
        parsed: dict[str, object] = {
            "field": _require_nonempty_string(
                config["field"],
                field=f"interceptors[{index}].config.field",
            ),
            "method": _require_nonempty_string(
                config["method"],
                field=f"interceptors[{index}].config.method",
            ),
        }
        if "forRoles" in config:
            parsed["forRoles"] = _parse_roles(config["forRoles"], index=index)
        return parsed

    raise GatewayArtifactFormatError(
        f"compiled Gateway interceptor {index} type is unsupported"
    )


def _parse_interceptors(value: object) -> list[dict[str, object]]:
    if not isinstance(value, list):
        raise GatewayArtifactFormatError(
            "compiled Gateway field 'interceptors' must be a list"
        )
    interceptors: list[dict[str, object]] = []
    for index, item in enumerate(value):
        if not isinstance(item, dict):
            raise GatewayArtifactFormatError(
                f"compiled Gateway interceptor {index} must be an object"
            )
        _require_exact_fields(
            item,
            required=_INTERCEPTOR_FIELDS,
            label=f"compiled Gateway interceptor {index}",
        )
        interceptor_type = _require_nonempty_string(
            item["type"],
            field=f"interceptors[{index}].type",
        )
        if interceptor_type not in _SUPPORTED_INTERCEPTOR_TYPES:
            raise GatewayArtifactFormatError(
                f"compiled Gateway interceptor {index} type is unsupported"
            )
        interceptor: dict[str, object] = {
            "type": interceptor_type,
            "config": _parse_interceptor_config(
                interceptor_type,
                item["config"],
                index=index,
            ),
        }
        interceptors.append(interceptor)
    return interceptors


def parse_compiled_gateway_rule_artifact(value: object) -> GatewayRuleArtifact:
    """Parse one exact ``GatewayRuleArtifact.to_dict()`` representation."""
    if not isinstance(value, dict):
        raise GatewayArtifactFormatError(
            "compiled Gateway rule artifact must be an object"
        )
    _require_exact_fields(
        value,
        required=_REQUIRED_FIELDS,
        optional=_OPTIONAL_FIELDS,
        label="compiled Gateway rule artifact",
    )

    return GatewayRuleArtifact(
        name=_require_nonempty_string(value["name"], field="name"),
        virtual_topic=_require_nonempty_string(
            value["virtualTopic"], field="virtualTopic"
        ),
        physical_topic=_require_nonempty_string(
            value["physicalTopic"], field="physicalTopic"
        ),
        interceptors=_parse_interceptors(value["interceptors"]),
        ownership=(
            _parse_ownership(value["ownership"])
            if "ownership" in value
            else None
        ),
    )


__all__ = [
    "GatewayArtifactFormatError",
    "parse_compiled_gateway_rule_artifact",
]
