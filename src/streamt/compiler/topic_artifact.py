"""Strict, offline parsing for compiled Kafka topic artifacts.

The types in this module are deliberately independent of the mutable manifest
dataclasses.  They form the closed input boundary used by deterministic GitOps
exporters and never render project, owner, topic, or configuration values in
errors or representations.
"""

from __future__ import annotations

import hashlib
import re
import unicodedata
from collections.abc import Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Literal

_TOPIC_FIELDS = frozenset(
    {"name", "partitions", "replication_factor", "config", "ownership"}
)
_OWNERSHIP_FIELDS = frozenset({"mode", "project", "type", "name"})
_OWNER_TYPES = frozenset({"model", "source"})
_SUPPORTED_OWNERSHIP_MODES = frozenset({"managed", "external"})
_DECIMAL_CHUNK_BASE = 1_000_000_000
_DECIMAL_CHUNK_WIDTH = 9
_KAFKA_TOPIC_NAME = re.compile(r"[A-Za-z0-9._-]{1,249}\Z", re.ASCII)
_DNS1123_LABEL = re.compile(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\Z", re.ASCII)
_SENSITIVE_CONFIG_KEY = re.compile(
    r"(^|[._-])(?:password|passwd|secret|token|api[_-]?key|authorization|"
    r"credentials?|basic[._-]auth[._-]user[._-]info|"
    r"sasl[._-]jaas[._-]config)($|[._-])",
    re.IGNORECASE | re.ASCII,
)


class TopicArtifactFormatError(ValueError):
    """A compiled topic artifact cannot be exported unambiguously."""


@dataclass(frozen=True, slots=True, repr=False)
class ParsedTopicOwnership:
    """Validated lifecycle identity with a secret-neutral representation."""

    project: str
    owner_type: Literal["model", "source"]
    owner_name: str
    mode: Literal["managed", "external"]

    def __repr__(self) -> str:
        return "ParsedTopicOwnership(<validated>)"


@dataclass(frozen=True, slots=True, repr=False)
class ParsedTopicArtifact:
    """Immutable defensive copy of one validated compiled topic artifact."""

    name: str
    partitions: int
    replication_factor: int
    ownership: ParsedTopicOwnership
    _config_items: tuple[tuple[str, str], ...] = field(compare=True, hash=True)

    @property
    def config(self) -> Mapping[str, str]:
        """Return a read-only copy of normalized configuration values."""
        return MappingProxyType(dict(self._config_items))

    @property
    def config_items(self) -> tuple[tuple[str, str], ...]:
        """Return normalized configuration in canonical key order."""
        return self._config_items

    @property
    def metadata_name(self) -> str:
        """Return the deterministic Kubernetes identity for this Kafka topic."""
        return kafka_topic_metadata_name(self.name)

    def __repr__(self) -> str:
        return "ParsedTopicArtifact(<validated>)"


def _is_exact_dict(value: object) -> bool:
    return type(value) is dict


def _is_safe_annotation_text(value: object) -> bool:
    if type(value) is not str or not value:
        return False
    return all(unicodedata.category(char) not in {"Cc", "Cs"} for char in value)


def _require_exact_fields(
    value: dict[object, object],
    *,
    expected: frozenset[str],
    location: str,
) -> None:
    if any(type(key) is not str for key in value):
        raise TopicArtifactFormatError(f"{location} keys must be strings")
    if set(value) != expected:
        raise TopicArtifactFormatError(f"{location} must have exact fields")


def validate_kafka_topic_name(value: object) -> str:
    """Validate and return a Kafka 4.3.1 physical topic name."""
    if type(value) is not str or _KAFKA_TOPIC_NAME.fullmatch(value) is None:
        raise TopicArtifactFormatError("compiled topic field 'name' is invalid")
    if value in {".", ".."}:
        raise TopicArtifactFormatError("compiled topic field 'name' is invalid")
    return value


def is_dns1123_label(value: object) -> bool:
    """Return whether *value* is an exact Kubernetes DNS-1123 label."""
    return type(value) is str and _DNS1123_LABEL.fullmatch(value) is not None


def validate_dns1123_label(value: object) -> str:
    """Validate and return an exact Kubernetes DNS-1123 label."""
    if not is_dns1123_label(value):
        raise TopicArtifactFormatError("Kubernetes DNS-1123 label is invalid")
    assert isinstance(value, str)  # narrowed by is_dns1123_label
    return value


def kafka_topic_metadata_name(topic_name: object) -> str:
    """Map a valid physical topic name to its deterministic Kubernetes name."""
    parsed_name = validate_kafka_topic_name(topic_name)
    if is_dns1123_label(parsed_name):
        return parsed_name
    digest = hashlib.sha256(parsed_name.encode("utf-8")).hexdigest()
    return f"streamt-topic-{digest}"


def _parse_bounded_integer(
    value: object,
    *,
    field_name: str,
    maximum: int,
) -> int:
    if type(value) is not int or value < 1 or value > maximum:
        raise TopicArtifactFormatError(
            f"compiled topic field {field_name!r} is outside its supported integer range"
        )
    return value


def _parse_ownership(
    value: object,
    *,
    expected_project: str,
) -> ParsedTopicOwnership:
    if not _is_exact_dict(value):
        raise TopicArtifactFormatError(
            "compiled topic field 'ownership' must be an exact ownership object"
        )
    assert isinstance(value, dict)
    _require_exact_fields(
        value,
        expected=_OWNERSHIP_FIELDS,
        location="compiled topic ownership",
    )

    project = value["project"]
    owner_type = value["type"]
    owner_name = value["name"]
    mode = value["mode"]
    if not _is_safe_annotation_text(project) or project != expected_project:
        raise TopicArtifactFormatError("compiled topic ownership project is invalid")
    if type(owner_type) is not str or owner_type not in _OWNER_TYPES:
        raise TopicArtifactFormatError("compiled topic ownership type is unsupported")
    if not _is_safe_annotation_text(owner_name):
        raise TopicArtifactFormatError("compiled topic ownership name is invalid")
    if type(mode) is not str or mode not in _SUPPORTED_OWNERSHIP_MODES:
        raise TopicArtifactFormatError("compiled topic ownership mode is unsupported")

    owner_type_literal: Literal["model", "source"] = (
        "model" if owner_type == "model" else "source"
    )
    mode_literal: Literal["managed", "external"] = (
        "managed" if mode == "managed" else "external"
    )
    return ParsedTopicOwnership(
        project=project,
        owner_type=owner_type_literal,
        owner_name=owner_name,
        mode=mode_literal,
    )


def _is_valid_config_key(value: object) -> bool:
    if type(value) is not str or not value or not value.isascii():
        return False
    if any(unicodedata.category(char) == "Cc" for char in value):
        return False
    return _SENSITIVE_CONFIG_KEY.search(value) is None


def _normalize_config_scalar(value: object) -> str:
    if type(value) is str:
        if any(unicodedata.category(char) in {"Cc", "Cs"} for char in value):
            raise TopicArtifactFormatError(
                "compiled topic config contains an invalid string value"
            )
        return value
    if type(value) is bool:
        return "true" if value else "false"
    if type(value) is int:
        return _integer_to_decimal(value)
    raise TopicArtifactFormatError(
        "compiled topic config contains an unsupported value type"
    )


def _integer_to_decimal(value: int) -> str:
    """Render an arbitrary integer without changing the process digit limit."""
    if value == 0:
        return "0"

    negative = value < 0
    magnitude = -value if negative else value
    chunks: list[int] = []
    while magnitude:
        magnitude, remainder = divmod(magnitude, _DECIMAL_CHUNK_BASE)
        chunks.append(remainder)

    rendered = [str(chunks.pop())]
    while chunks:
        rendered.append(f"{chunks.pop():0{_DECIMAL_CHUNK_WIDTH}d}")
    result = "".join(rendered)
    return f"-{result}" if negative else result


def _parse_config(value: object) -> tuple[tuple[str, str], ...]:
    if not _is_exact_dict(value):
        raise TopicArtifactFormatError("compiled topic field 'config' must be an object")
    assert isinstance(value, dict)
    normalized: list[tuple[str, str]] = []
    for key, item in value.items():
        if not _is_valid_config_key(key):
            raise TopicArtifactFormatError("compiled topic config key is invalid")
        assert isinstance(key, str)
        normalized.append((key, _normalize_config_scalar(item)))
    normalized.sort(key=lambda pair: pair[0])
    return tuple(normalized)


def parse_compiled_topic_artifact(
    value: object,
    *,
    expected_project: str,
) -> ParsedTopicArtifact:
    """Parse one exact ``TopicArtifact.to_dict()`` representation."""
    if not _is_safe_annotation_text(expected_project):
        raise TopicArtifactFormatError("expected project identity is invalid")
    if not _is_exact_dict(value):
        raise TopicArtifactFormatError("compiled topic artifact must be an object")
    assert isinstance(value, dict)
    _require_exact_fields(
        value,
        expected=_TOPIC_FIELDS,
        location="compiled topic artifact",
    )

    return ParsedTopicArtifact(
        name=validate_kafka_topic_name(value["name"]),
        partitions=_parse_bounded_integer(
            value["partitions"],
            field_name="partitions",
            maximum=2_147_483_647,
        ),
        replication_factor=_parse_bounded_integer(
            value["replication_factor"],
            field_name="replication_factor",
            maximum=32_767,
        ),
        ownership=_parse_ownership(
            value["ownership"],
            expected_project=expected_project,
        ),
        _config_items=_parse_config(value["config"]),
    )


def parse_compiled_topic_artifacts(
    values: object,
    *,
    expected_project: str,
) -> tuple[ParsedTopicArtifact, ...]:
    """Parse a topic collection and reject duplicate physical or K8s identity."""
    if type(values) is not list:
        raise TopicArtifactFormatError("compiled topic collection must be a list")

    parsed: list[ParsedTopicArtifact] = []
    kafka_names: set[str] = set()
    kubernetes_names: set[str] = set()
    assert isinstance(values, list)
    for index, value in enumerate(values):
        try:
            artifact = parse_compiled_topic_artifact(
                value,
                expected_project=expected_project,
            )
        except TopicArtifactFormatError as exc:
            raise TopicArtifactFormatError(
                f"compiled topic collection item {index} is invalid: {exc}"
            ) from None

        if artifact.name in kafka_names:
            raise TopicArtifactFormatError(
                f"compiled topic collection item {index} duplicates a Kafka identity"
            )
        metadata_name = artifact.metadata_name
        if metadata_name in kubernetes_names:
            raise TopicArtifactFormatError(
                f"compiled topic collection item {index} duplicates a Kubernetes identity"
            )
        kafka_names.add(artifact.name)
        kubernetes_names.add(metadata_name)
        parsed.append(artifact)
    return tuple(parsed)


__all__ = [
    "ParsedTopicArtifact",
    "ParsedTopicOwnership",
    "TopicArtifactFormatError",
    "is_dns1123_label",
    "kafka_topic_metadata_name",
    "parse_compiled_topic_artifact",
    "parse_compiled_topic_artifacts",
    "validate_dns1123_label",
    "validate_kafka_topic_name",
]
