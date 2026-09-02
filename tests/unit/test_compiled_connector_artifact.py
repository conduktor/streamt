"""Strict parsing tests for compiled Kafka Connect artifacts."""

from __future__ import annotations

import math

import pytest

from streamt.compiler.connector_artifact import (
    parse_compiled_connector_artifact,
)
from streamt.compiler.manifest import (
    ArtifactOwnership,
    ConnectorArtifact,
    ConnectorArtifactFormatError,
)


def _artifact() -> ConnectorArtifact:
    return ConnectorArtifact(
        name="orders-sink",
        connector_class="com.example.OrdersSink",
        topics=["orders.v1", "orders.v2"],
        cluster="production",
        config={
            "enabled": True,
            "tasks.max": 4,
            "ratio": 0.5,
            "connection.url": "https://warehouse.example.test",
        },
        ownership=ArtifactOwnership(
            project="payments",
            owner_type="model",
            owner_name="orders_sink",
            mode="adopted",
        ),
    )


def test_parser_accepts_exact_to_dict_shape_and_preserves_cluster_and_ownership() -> None:
    expected = _artifact()

    parsed = parse_compiled_connector_artifact(expected.to_dict())

    assert parsed == expected
    assert parsed.to_dict() == expected.to_dict()


def test_parser_accepts_absent_ownership_and_none_cluster() -> None:
    expected = ConnectorArtifact(
        name="orders-sink",
        connector_class="com.example.OrdersSink",
        topics=["orders.v1"],
    )

    parsed = parse_compiled_connector_artifact(expected.to_dict())

    assert parsed == expected


@pytest.mark.parametrize(
    ("reserved", "conflicting"),
    [
        ("name", "other-sink"),
        ("connector.class", "com.example.OtherSink"),
        ("topics", "other.v1"),
    ],
)
def test_to_dict_rejects_config_that_overrides_reserved_fields(
    reserved: str,
    conflicting: str,
) -> None:
    artifact = _artifact()
    artifact.config[reserved] = conflicting

    with pytest.raises(ConnectorArtifactFormatError) as caught:
        artifact.to_dict()

    assert str(caught.value) == (
        f"connector config field {reserved!r} conflicts with its canonical field"
    )
    assert conflicting not in str(caught.value)


def test_to_dict_allows_redundant_reserved_fields_only_when_they_match() -> None:
    artifact = _artifact()
    artifact.config.update(
        {
            "name": artifact.name,
            "connector.class": artifact.connector_class,
            "topics": ",".join(artifact.topics),
        }
    )

    serialized = artifact.to_dict()

    assert serialized["config"]["name"] == artifact.name  # type: ignore[index]


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("name", ""),
        ("name", "   "),
        ("name", 1),
        ("connector_class", ""),
        ("connector_class", None),
        ("topics", []),
        ("topics", "orders.v1"),
        ("topics", [""]),
        ("topics", [1]),
        ("cluster", ""),
        ("cluster", "  "),
        ("cluster", 1),
    ],
)
def test_parser_rejects_malformed_canonical_fields(field: str, value: object) -> None:
    serialized = _artifact().to_dict()
    serialized[field] = value

    with pytest.raises(ConnectorArtifactFormatError):
        parse_compiled_connector_artifact(serialized)


@pytest.mark.parametrize(
    "value",
    [None, ["nested"], {"nested": True}, math.nan, math.inf, -math.inf, object()],
)
def test_parser_rejects_non_scalar_nonfinite_or_null_config_values(value: object) -> None:
    serialized = _artifact().to_dict()
    serialized["config"]["invalid"] = value  # type: ignore[index]

    with pytest.raises(
        ConnectorArtifactFormatError,
        match="must be a finite JSON scalar",
    ):
        parse_compiled_connector_artifact(serialized)


@pytest.mark.parametrize("key", ["", "   ", 1])
def test_parser_rejects_invalid_config_keys(key: object) -> None:
    serialized = _artifact().to_dict()
    serialized["config"][key] = "value"  # type: ignore[index]

    with pytest.raises(
        ConnectorArtifactFormatError,
        match="config keys must be non-empty strings",
    ):
        parse_compiled_connector_artifact(serialized)


@pytest.mark.parametrize(
    ("reserved", "value"),
    [
        ("name", "wrong"),
        ("connector.class", "wrong"),
        ("topics", "wrong"),
        ("name", None),
    ],
)
def test_parser_rejects_missing_or_mismatched_reserved_config(
    reserved: str,
    value: object,
) -> None:
    serialized = _artifact().to_dict()
    config = serialized["config"]
    assert isinstance(config, dict)
    if value is None:
        config.pop(reserved)
    else:
        config[reserved] = value

    with pytest.raises(ConnectorArtifactFormatError, match="must exactly match"):
        parse_compiled_connector_artifact(serialized)


@pytest.mark.parametrize(
    "ownership",
    [None, "payments/orders", {}, {"project": "payments", "type": "model"}],
)
def test_parser_rejects_malformed_present_ownership(ownership: object) -> None:
    serialized = _artifact().to_dict()
    serialized["ownership"] = ownership

    with pytest.raises(ConnectorArtifactFormatError, match="valid ownership object"):
        parse_compiled_connector_artifact(serialized)


def test_parser_rejects_missing_and_unknown_top_level_fields_deterministically() -> None:
    missing = _artifact().to_dict()
    missing.pop("cluster")
    with pytest.raises(ConnectorArtifactFormatError) as missing_error:
        parse_compiled_connector_artifact(missing)
    assert str(missing_error.value) == "compiled connector artifact is missing field 'cluster'"

    unknown = _artifact().to_dict()
    unknown["future"] = "value"
    with pytest.raises(ConnectorArtifactFormatError) as unknown_error:
        parse_compiled_connector_artifact(unknown)
    assert str(unknown_error.value) == (
        "compiled connector artifact has unsupported field 'future'"
    )


@pytest.mark.parametrize("value", [None, [], "connector", {1: "bad-key"}])
def test_parser_rejects_non_object_or_non_string_top_level_keys(value: object) -> None:
    with pytest.raises(ConnectorArtifactFormatError):
        parse_compiled_connector_artifact(value)
