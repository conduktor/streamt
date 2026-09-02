"""Strict parsing tests for compiled Conduktor Gateway rule artifacts."""

from __future__ import annotations

import math

import pytest

from streamt.compiler.gateway_artifact import (
    GatewayArtifactFormatError,
    parse_compiled_gateway_rule_artifact,
)
from streamt.compiler.manifest import ArtifactOwnership, GatewayRuleArtifact


def _artifact() -> GatewayRuleArtifact:
    return GatewayRuleArtifact(
        name="orders_public",
        virtual_topic="orders.public",
        physical_topic="orders.v1",
        interceptors=[
            {"type": "filter", "config": {"where": "region = 'EU'"}},
            {
                "type": "mask",
                "config": {
                    "field": "customer.email",
                    "method": "MASK_ALL",
                    "forRoles": ["analyst", "support"],
                },
            },
        ],
        ownership=ArtifactOwnership(
            project="payments",
            owner_type="model",
            owner_name="orders_public",
            mode="adopted",
        ),
    )


def test_parser_roundtrips_compiler_supported_declarations_exactly() -> None:
    expected = _artifact()

    parsed = parse_compiled_gateway_rule_artifact(expected.to_dict())

    assert parsed == expected
    assert parsed.to_dict() == expected.to_dict()


def test_parser_accepts_empty_interceptors_and_absent_ownership() -> None:
    expected = GatewayRuleArtifact(
        name="orders_public",
        virtual_topic="orders.public",
        physical_topic="orders.v1",
    )

    parsed = parse_compiled_gateway_rule_artifact(expected.to_dict())

    assert parsed == expected


def test_parser_accepts_empty_roles_and_distinct_mask_declarations() -> None:
    serialized = _artifact().to_dict()
    serialized["interceptors"] = [
        {
            "type": "mask",
            "config": {
                "field": "customer.email",
                "method": "MASK_ALL",
                "forRoles": [],
            },
        },
        {
            "type": "mask",
            "config": {
                "field": "customer.phone",
                "method": "MASK_ALL",
                "forRoles": ["support"],
            },
        },
    ]

    parsed = parse_compiled_gateway_rule_artifact(serialized)

    assert parsed.interceptors == serialized["interceptors"]


def test_parser_returns_deep_copies_of_interceptors_and_roles() -> None:
    serialized = _artifact().to_dict()

    parsed = parse_compiled_gateway_rule_artifact(serialized)
    serialized_interceptors = serialized["interceptors"]
    assert isinstance(serialized_interceptors, list)
    serialized_mask = serialized_interceptors[1]
    assert isinstance(serialized_mask, dict)
    serialized_mask_config = serialized_mask["config"]
    assert isinstance(serialized_mask_config, dict)
    serialized_mask_config["field"] = "changed"
    roles = serialized_mask_config["forRoles"]
    assert isinstance(roles, list)
    roles.append("changed")

    assert parsed.interceptors[1]["config"] == {
        "field": "customer.email",
        "method": "MASK_ALL",
        "forRoles": ["analyst", "support"],
    }


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("name", ""),
        ("name", "   "),
        ("name", 1),
        ("virtualTopic", ""),
        ("virtualTopic", None),
        ("physicalTopic", "  "),
        ("physicalTopic", []),
    ],
)
def test_parser_rejects_blank_or_non_string_identities(
    field: str,
    value: object,
) -> None:
    serialized = _artifact().to_dict()
    serialized[field] = value

    with pytest.raises(GatewayArtifactFormatError, match="non-empty string"):
        parse_compiled_gateway_rule_artifact(serialized)


def test_parser_rejects_missing_and_unknown_top_level_fields_deterministically() -> None:
    missing = _artifact().to_dict()
    missing.pop("physicalTopic")
    with pytest.raises(GatewayArtifactFormatError) as missing_error:
        parse_compiled_gateway_rule_artifact(missing)
    assert str(missing_error.value) == (
        "compiled Gateway rule artifact is missing field 'physicalTopic'"
    )

    unknown = _artifact().to_dict()
    unknown["future"] = True
    with pytest.raises(GatewayArtifactFormatError) as unknown_error:
        parse_compiled_gateway_rule_artifact(unknown)
    assert str(unknown_error.value) == (
        "compiled Gateway rule artifact has unsupported field 'future'"
    )


@pytest.mark.parametrize("value", [None, [], "gateway", {1: "bad-key"}])
def test_parser_rejects_non_object_or_non_string_top_level_keys(value: object) -> None:
    with pytest.raises(GatewayArtifactFormatError):
        parse_compiled_gateway_rule_artifact(value)


@pytest.mark.parametrize("value", [None, {}, "filter", 1])
def test_parser_rejects_non_list_interceptors(value: object) -> None:
    serialized = _artifact().to_dict()
    serialized["interceptors"] = value

    with pytest.raises(GatewayArtifactFormatError, match="must be a list"):
        parse_compiled_gateway_rule_artifact(serialized)


@pytest.mark.parametrize("value", [None, "filter", [], 1])
def test_parser_rejects_non_object_interceptor_entries(value: object) -> None:
    serialized = _artifact().to_dict()
    serialized["interceptors"] = [value]

    with pytest.raises(GatewayArtifactFormatError, match="must be an object"):
        parse_compiled_gateway_rule_artifact(serialized)


@pytest.mark.parametrize(
    ("mutation", "expected"),
    [
        ({"config": {}}, "missing field 'type'"),
        ({"type": "filter"}, "missing field 'config'"),
        ({"type": "filter", "config": {}, "priority": 1}, "unsupported field 'priority'"),
        ({1: "bad", "type": "filter", "config": {}}, "keys must be strings"),
    ],
)
def test_parser_rejects_non_exact_interceptor_fields(
    mutation: dict[object, object],
    expected: str,
) -> None:
    serialized = _artifact().to_dict()
    serialized["interceptors"] = [mutation]

    with pytest.raises(GatewayArtifactFormatError, match=expected):
        parse_compiled_gateway_rule_artifact(serialized)


@pytest.mark.parametrize(
    "interceptor_type",
    ["", "   ", "FILTER", "custom", "encrypt", "readonly", 1, None],
)
def test_parser_rejects_blank_unknown_or_non_string_interceptor_types(
    interceptor_type: object,
) -> None:
    serialized = _artifact().to_dict()
    serialized["interceptors"] = [{"type": interceptor_type, "config": {}}]

    with pytest.raises(GatewayArtifactFormatError):
        parse_compiled_gateway_rule_artifact(serialized)


@pytest.mark.parametrize("config", [None, [], "where", 1])
def test_parser_rejects_non_object_interceptor_config(config: object) -> None:
    serialized = _artifact().to_dict()
    serialized["interceptors"] = [{"type": "filter", "config": config}]

    with pytest.raises(GatewayArtifactFormatError, match="must be an object"):
        parse_compiled_gateway_rule_artifact(serialized)


@pytest.mark.parametrize("key", [1, None])
def test_parser_rejects_non_string_config_keys(key: object) -> None:
    serialized = _artifact().to_dict()
    serialized["interceptors"] = [
        {"type": "filter", "config": {"where": "x = 1", key: "value"}}
    ]

    with pytest.raises(GatewayArtifactFormatError, match="object keys must be strings"):
        parse_compiled_gateway_rule_artifact(serialized)


@pytest.mark.parametrize("value", [math.nan, math.inf, -math.inf, object(), ("tuple",)])
def test_parser_rejects_nonfinite_or_non_json_nested_config(value: object) -> None:
    serialized = _artifact().to_dict()
    serialized["interceptors"] = [
        {"type": "filter", "config": {"where": "x = 1", "future": [value]}}
    ]

    with pytest.raises(GatewayArtifactFormatError, match="finite JSON values"):
        parse_compiled_gateway_rule_artifact(serialized)


def test_parser_rejects_cyclic_nested_config() -> None:
    cyclic: list[object] = []
    cyclic.append(cyclic)
    serialized = _artifact().to_dict()
    serialized["interceptors"] = [
        {"type": "filter", "config": {"where": "x = 1", "future": cyclic}}
    ]

    with pytest.raises(GatewayArtifactFormatError, match="finite JSON values"):
        parse_compiled_gateway_rule_artifact(serialized)


@pytest.mark.parametrize(
    "config",
    [
        {},
        {"where": ""},
        {"where": "   "},
        {"where": 1},
        {"where": "x = 1", "future": True},
    ],
)
def test_parser_rejects_malformed_filter_config(config: dict[str, object]) -> None:
    serialized = _artifact().to_dict()
    serialized["interceptors"] = [{"type": "filter", "config": config}]

    with pytest.raises(GatewayArtifactFormatError):
        parse_compiled_gateway_rule_artifact(serialized)


@pytest.mark.parametrize(
    "config",
    [
        {},
        {"field": "email"},
        {"method": "MASK_ALL"},
        {"field": "", "method": "MASK_ALL"},
        {"field": "email", "method": ""},
        {"field": "email", "method": "MASK_ALL", "future": True},
        {"field": "email", "method": "MASK_ALL", "forRoles": "analyst"},
        {"field": "email", "method": "MASK_ALL", "forRoles": [""]},
        {
            "field": "email",
            "method": "MASK_ALL",
            "forRoles": ["analyst", "analyst"],
        },
    ],
)
def test_parser_rejects_malformed_mask_config(config: dict[str, object]) -> None:
    serialized = _artifact().to_dict()
    serialized["interceptors"] = [{"type": "mask", "config": config}]

    with pytest.raises(GatewayArtifactFormatError):
        parse_compiled_gateway_rule_artifact(serialized)


def test_parser_preserves_duplicate_declarations_at_distinct_indices() -> None:
    interceptor = {
        "type": "mask",
        "config": {
            "field": "email",
            "method": "MASK_ALL",
            "forRoles": ["support"],
        },
    }
    serialized = _artifact().to_dict()
    serialized["interceptors"] = [interceptor, dict(interceptor)]

    parsed = parse_compiled_gateway_rule_artifact(serialized)

    assert parsed.interceptors == [interceptor, interceptor]
    assert parsed.interceptors[0] is not parsed.interceptors[1]
    assert parsed.interceptors[0]["config"] is not parsed.interceptors[1]["config"]


@pytest.mark.parametrize(
    "ownership",
    [
        None,
        "payments/orders_public",
        {},
        {"project": "payments", "type": "model", "name": "orders_public"},
        {
            "mode": "managed",
            "project": "payments",
            "type": "model",
            "name": "orders_public",
            "future": True,
        },
        {"mode": "managed", "project": "", "type": "model", "name": "orders_public"},
        {"mode": "claimed", "project": "payments", "type": "model", "name": "orders_public"},
    ],
)
def test_parser_rejects_malformed_or_non_exact_ownership(ownership: object) -> None:
    serialized = _artifact().to_dict()
    serialized["ownership"] = ownership

    with pytest.raises(GatewayArtifactFormatError):
        parse_compiled_gateway_rule_artifact(serialized)
