"""Strict parsing for compiled Kafka Connect connector artifacts."""

from __future__ import annotations

import math

from streamt.compiler.manifest import (
    ArtifactOwnership,
    ConnectorArtifact,
    ConnectorArtifactFormatError,
    ConnectorRemovalArtifact,
)

_REQUIRED_FIELDS = frozenset({"name", "connector_class", "topics", "cluster", "config"})
_OPTIONAL_FIELDS = frozenset({"ownership"})
_RESERVED_CONFIG_FIELDS = frozenset({"name", "connector.class", "topics"})
_REMOVAL_FIELDS = frozenset({"logicalOwner", "name", "cluster"})
# Slice 2 fail-closed boundary: remove only after Slice 4 makes every resolved
# target yield an exact removal assessment and action.
CONNECTOR_REMOVAL_PLANNING_UNAVAILABLE_MESSAGE = (
    "Connector removal planning is not available in this build"
)


class ConnectorRemovalArtifactFormatError(ValueError):
    """A compiled Connector removal is not one exact secret-neutral artifact."""


class ConnectorRemovalPreflightError(ValueError):
    """A Connector removal cannot be resolved without provider access."""


class ConnectorRemovalClusterReferenceError(ConnectorRemovalPreflightError):
    """A Connector removal does not name the effective default cluster."""


class ConnectorRemovalRuntimeRequiredError(ConnectorRemovalPreflightError):
    """A Connector removal has no usable Kafka Connect runtime."""


class ConnectorRemovalStateAuthorityError(ValueError):
    """The initial PostgreSQL-v2 state authority proof failed."""


def _require_nonempty_string(value: object, *, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ConnectorArtifactFormatError(
            f"compiled connector field {field!r} must be a non-empty string"
        )
    return value


def _parse_topics(value: object) -> list[str]:
    if not isinstance(value, list) or not value:
        raise ConnectorArtifactFormatError(
            "compiled connector field 'topics' must be a non-empty list"
        )
    topics: list[str] = []
    for topic in value:
        topics.append(_require_nonempty_string(topic, field="topics[]"))
    return topics


def _parse_cluster(value: object) -> str | None:
    if value is None:
        return None
    return _require_nonempty_string(value, field="cluster")


def _validate_config_value(key: str, value: object) -> None:
    if isinstance(value, (str, bool, int)):
        return
    if isinstance(value, float) and math.isfinite(value):
        return
    raise ConnectorArtifactFormatError(
        f"compiled connector config field {key!r} must be a finite JSON scalar"
    )


def _parse_config(value: object) -> dict[str, object]:
    if not isinstance(value, dict):
        raise ConnectorArtifactFormatError("compiled connector field 'config' must be an object")
    config: dict[str, object] = {}
    for key, item in value.items():
        if not isinstance(key, str) or not key.strip():
            raise ConnectorArtifactFormatError(
                "compiled connector config keys must be non-empty strings"
            )
        _validate_config_value(key, item)
        config[key] = item
    return config


def _parse_ownership(value: object) -> ArtifactOwnership:
    if not isinstance(value, dict):
        raise ConnectorArtifactFormatError(
            "compiled connector field 'ownership' must be a valid ownership object"
        )
    ownership = ArtifactOwnership.from_dict(value)
    if ownership is None:
        raise ConnectorArtifactFormatError(
            "compiled connector field 'ownership' must be a valid ownership object"
        )
    return ownership


def parse_compiled_connector_artifact(value: object) -> ConnectorArtifact:
    """Parse one exact ``ConnectorArtifact.to_dict()`` representation.

    The parser performs no normalization. In particular, the three config fields
    duplicated by the manifest format must match their canonical top-level fields
    byte-for-byte so callers never have to choose which representation to trust.
    """
    if not isinstance(value, dict):
        raise ConnectorArtifactFormatError("compiled connector artifact must be an object")
    if any(not isinstance(key, str) for key in value):
        raise ConnectorArtifactFormatError("compiled connector artifact keys must be strings")

    fields = set(value)
    missing = sorted(_REQUIRED_FIELDS - fields)
    if missing:
        raise ConnectorArtifactFormatError(
            f"compiled connector artifact is missing field {missing[0]!r}"
        )
    unsupported = sorted(fields - _REQUIRED_FIELDS - _OPTIONAL_FIELDS)
    if unsupported:
        raise ConnectorArtifactFormatError(
            f"compiled connector artifact has unsupported field {unsupported[0]!r}"
        )

    name = _require_nonempty_string(value["name"], field="name")
    connector_class = _require_nonempty_string(
        value["connector_class"],
        field="connector_class",
    )
    topics = _parse_topics(value["topics"])
    cluster = _parse_cluster(value["cluster"])
    serialized_config = _parse_config(value["config"])

    expected_reserved = {
        "name": name,
        "connector.class": connector_class,
        "topics": ",".join(topics),
    }
    for key, expected in expected_reserved.items():
        if serialized_config.get(key) != expected:
            raise ConnectorArtifactFormatError(
                f"compiled connector config field {key!r} must exactly match its canonical field"
            )

    ownership = _parse_ownership(value["ownership"]) if "ownership" in value else None
    config = {
        key: item for key, item in serialized_config.items() if key not in _RESERVED_CONFIG_FIELDS
    }
    return ConnectorArtifact(
        name=name,
        connector_class=connector_class,
        topics=topics,
        config=config,
        cluster=cluster,
        ownership=ownership,
    )


def parse_compiled_connector_removal_artifact(
    value: object,
) -> ConnectorRemovalArtifact:
    """Parse one exact ``ConnectorRemovalArtifact.to_dict()`` representation."""
    if not isinstance(value, dict) or any(not isinstance(key, str) for key in value):
        raise ConnectorRemovalArtifactFormatError(
            "compiled Connector removal artifact must be an object"
        )
    if set(value) != _REMOVAL_FIELDS:
        raise ConnectorRemovalArtifactFormatError(
            "compiled Connector removal artifact must have exact fields"
        )
    try:
        return ConnectorRemovalArtifact(
            logical_owner=value["logicalOwner"],
            connector_name=value["name"],
            cluster_alias=value["cluster"],
        )
    except (TypeError, ValueError):
        raise ConnectorRemovalArtifactFormatError(
            "compiled Connector removal artifact contains an invalid identity"
        ) from None


__all__ = [
    "CONNECTOR_REMOVAL_PLANNING_UNAVAILABLE_MESSAGE",
    "ConnectorArtifactFormatError",
    "ConnectorRemovalArtifactFormatError",
    "ConnectorRemovalClusterReferenceError",
    "ConnectorRemovalPreflightError",
    "ConnectorRemovalRuntimeRequiredError",
    "ConnectorRemovalStateAuthorityError",
    "parse_compiled_connector_artifact",
    "parse_compiled_connector_removal_artifact",
]
