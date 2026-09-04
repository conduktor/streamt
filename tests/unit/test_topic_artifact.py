"""Closed parsing tests for Strimzi-bound compiled topic artifacts."""

from __future__ import annotations

import os
import subprocess
import sys
from copy import deepcopy
from dataclasses import FrozenInstanceError
from pathlib import Path
from types import MappingProxyType

import pytest

import streamt.compiler.topic_artifact as topic_artifact_module
from streamt.compiler.topic_artifact import (
    ParsedTopicArtifact,
    ParsedTopicOwnership,
    TopicArtifactFormatError,
    is_dns1123_label,
    kafka_topic_metadata_name,
    parse_compiled_topic_artifact,
    parse_compiled_topic_artifacts,
    validate_dns1123_label,
    validate_kafka_topic_name,
)

PROJECT = "public-project"


class _StringSubclass(str):
    pass


class _IntegerSubclass(int):
    pass


class _HostileTruthValue:
    def __bool__(self) -> bool:
        raise RuntimeError("private-hostile-truth-sentinel")

    def __repr__(self) -> str:
        raise RuntimeError("private-hostile-repr-sentinel")


def test_importing_topic_boundary_does_not_import_runtime_or_deployment_layers(
    tmp_path: Path,
) -> None:
    script = """
import sys
import streamt.compiler.topic_artifact
import streamt.integrations.gitops.strimzi_validation

forbidden = (
    "streamt.core.deployment_state",
    "streamt.core.runtime",
    "streamt.deployer",
    "streamt.planner",
    "streamt.provider",
    "streamt.providers",
    "streamt.state",
)
loaded = sorted(
    name
    for name in sys.modules
    if any(name == prefix or name.startswith(prefix + ".") for prefix in forbidden)
)
if loaded:
    raise SystemExit("forbidden runtime layer imported")

import streamt.compiler as compiler
from streamt.compiler.compiler import Compiler
from streamt.compiler.manifest import Manifest

if compiler.Compiler is not Compiler or compiler.Manifest is not Manifest:
    raise SystemExit("legacy compiler export changed")
"""
    environment = dict(os.environ)
    environment.pop("PYTHONPATH", None)
    result = subprocess.run(
        [sys.executable, "-I", "-c", script],
        cwd=tmp_path,
        env=environment,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert result.stdout == ""
    assert result.stderr == ""


def _topic(
    *,
    name: object = "orders-v1",
    partitions: object = 3,
    replication_factor: object = 2,
    config: object | None = None,
    project: object = PROJECT,
    owner_type: object = "model",
    owner_name: object = "orders",
    mode: object = "managed",
) -> dict[object, object]:
    return {
        "name": name,
        "partitions": partitions,
        "replication_factor": replication_factor,
        "config": {} if config is None else config,
        "ownership": {
            "mode": mode,
            "project": project,
            "type": owner_type,
            "name": owner_name,
        },
    }


def test_parses_exact_artifact_into_immutable_defensive_value() -> None:
    raw = _topic(
        config={
            "z.string": "preserved \N{SNOWMAN}",
            "a.bool": True,
            "b.false": False,
            "c.negative": -123456789012345678901234567890,
            "d.positive": 987654321098765432109876543210,
            "e.empty": "",
        }
    )

    parsed = parse_compiled_topic_artifact(raw, expected_project=PROJECT)
    assert parsed == ParsedTopicArtifact(
        name="orders-v1",
        partitions=3,
        replication_factor=2,
        ownership=ParsedTopicOwnership(
            project=PROJECT,
            owner_type="model",
            owner_name="orders",
            mode="managed",
        ),
        _config_items=(
            ("a.bool", "true"),
            ("b.false", "false"),
            ("c.negative", "-123456789012345678901234567890"),
            ("d.positive", "987654321098765432109876543210"),
            ("e.empty", ""),
            ("z.string", "preserved \N{SNOWMAN}"),
        ),
    )
    assert isinstance(parsed.config, MappingProxyType)
    assert parsed.config == dict(parsed.config_items)

    raw["name"] = "changed"
    assert isinstance(raw["config"], dict)
    raw["config"]["a.bool"] = False
    assert parsed.name == "orders-v1"
    assert parsed.config["a.bool"] == "true"
    with pytest.raises(TypeError):
        parsed.config["new"] = "value"  # type: ignore[index]
    with pytest.raises(FrozenInstanceError):
        parsed.name = "changed"  # type: ignore[misc]


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (0, "0"),
        (10**5000, "1" + "0" * 5000),
        (-(10**5000), "-" + "1" + "0" * 5000),
    ],
    ids=["zero", "huge-positive", "huge-negative"],
)
def test_normalizes_arbitrarily_large_integers_without_global_digit_limit(
    value: int,
    expected: str,
) -> None:
    parsed = parse_compiled_topic_artifact(
        _topic(config={"large.integer": value}),
        expected_project=PROJECT,
    )
    assert parsed.config["large.integer"] == expected


@pytest.mark.parametrize("owner_type", ["model", "source"])
@pytest.mark.parametrize("mode", ["managed", "external"])
def test_accepts_only_supported_owner_types_and_modes(
    owner_type: str,
    mode: str,
) -> None:
    parsed = parse_compiled_topic_artifact(
        _topic(owner_type=owner_type, mode=mode),
        expected_project=PROJECT,
    )
    assert parsed.ownership.owner_type == owner_type
    assert parsed.ownership.mode == mode


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("partitions", 1),
        ("partitions", 2_147_483_647),
        ("replication_factor", 1),
        ("replication_factor", 32_767),
    ],
)
def test_accepts_exact_integer_bounds(field: str, value: int) -> None:
    raw = _topic()
    raw[field] = value
    parsed = parse_compiled_topic_artifact(raw, expected_project=PROJECT)
    assert getattr(parsed, field) == value


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("partitions", 0),
        ("partitions", 2_147_483_648),
        ("partitions", True),
        ("partitions", 1.0),
        ("replication_factor", 0),
        ("replication_factor", 32_768),
        ("replication_factor", False),
        ("replication_factor", "1"),
        ("replication_factor", _IntegerSubclass(1)),
    ],
)
def test_rejects_out_of_range_or_non_exact_integers(field: str, value: object) -> None:
    raw = _topic()
    raw[field] = value
    with pytest.raises(TopicArtifactFormatError) as raised:
        parse_compiled_topic_artifact(raw, expected_project=PROJECT)
    assert str(value) not in str(raised.value)


@pytest.mark.parametrize(
    "name",
    ["a", "A._-9", "a" * 249, "Orders_READY_v1", "orders.ready_v1"],
)
def test_accepts_exact_kafka_431_topic_names(name: str) -> None:
    assert validate_kafka_topic_name(name) == name


@pytest.mark.parametrize(
    "name",
    [
        "",
        ".",
        "..",
        "a" * 250,
        "caf\N{LATIN SMALL LETTER E WITH ACUTE}",
        "line\nfeed",
        "surrogate\ud800",
        "slash/name",
        "percent%name",
        b"bytes",
        7,
        _StringSubclass("orders"),
    ],
)
def test_rejects_names_outside_exact_kafka_431_boundary(name: object) -> None:
    with pytest.raises(TopicArtifactFormatError) as raised:
        validate_kafka_topic_name(name)
    assert "caf" not in str(raised.value)
    assert "surrogate" not in str(raised.value)
    assert "slash" not in str(raised.value)
    assert "percent" not in str(raised.value)


@pytest.mark.parametrize(
    "name",
    ["a", "0", "orders-v1", "a" * 63, "a-0"],
)
def test_dns1123_label_direct_boundary(name: str) -> None:
    assert is_dns1123_label(name)
    assert validate_dns1123_label(name) == name
    assert kafka_topic_metadata_name(name) == name


@pytest.mark.parametrize(
    "name",
    ["", "A", "a_b", ".topic", "-topic", "topic-", "a" * 64, "caf\N{LATIN SMALL LETTER E WITH ACUTE}"],
)
def test_rejects_invalid_dns1123_labels(name: str) -> None:
    assert not is_dns1123_label(name)
    with pytest.raises(TopicArtifactFormatError):
        validate_dns1123_label(name)


def test_full_sha256_fallback_matches_frozen_fixture() -> None:
    assert kafka_topic_metadata_name("Orders_READY_v1") == (
        "streamt-topic-"
        "a48756b0f1cc6f4a99afdf5092fd5dd5877abf2f0bb001dcc5d3348d2fb10214"
    )
    assert len(kafka_topic_metadata_name("Orders_READY_v1")) == 78


@pytest.mark.parametrize(
    "value",
    [
        None,
        1.0,
        float("inf"),
        float("nan"),
        [],
        {},
        b"bytes",
        object(),
        _StringSubclass("string"),
        _IntegerSubclass(1),
    ],
)
def test_rejects_unsupported_config_values_without_rendering_them(value: object) -> None:
    secret = "private-rejected-config-value"
    raw = _topic(config={"cleanup.policy": value, "ordinary": secret})
    with pytest.raises(TopicArtifactFormatError) as raised:
        parse_compiled_topic_artifact(raw, expected_project=PROJECT)
    surface = f"{raised.value!s} {raised.value!r}"
    assert secret not in surface
    assert repr(value) not in surface


@pytest.mark.parametrize(
    "key",
    [
        "",
        "line\nfeed",
        "delete\x7fretention",
        "caf\N{LATIN SMALL LETTER E WITH ACUTE}",
        1,
        _StringSubclass("cleanup.policy"),
    ],
)
def test_rejects_non_ascii_empty_or_control_config_keys(key: object) -> None:
    with pytest.raises(TopicArtifactFormatError):
        parse_compiled_topic_artifact(_topic(config={key: "x"}), expected_project=PROJECT)


@pytest.mark.parametrize(
    "key",
    [
        "password",
        "db.passwd",
        "client-secret",
        "access_token",
        "apikey",
        "api_key",
        "api-key",
        "Authorization",
        "user.credential",
        "user.credentials",
        "basic.auth.user.info",
        "sasl_jaas_config",
        "prefix.sasl-jaas-config.suffix",
    ],
)
def test_rejects_exact_sensitive_config_key_expression(key: str) -> None:
    with pytest.raises(TopicArtifactFormatError) as raised:
        parse_compiled_topic_artifact(
            _topic(config={key: "private-config-sentinel"}),
            expected_project=PROJECT,
        )
    surface = f"{raised.value!s} {raised.value!r}"
    assert key not in surface
    assert "private-config-sentinel" not in surface


@pytest.mark.parametrize(
    "key",
    ["passwordless", "mytokenizer", "api.keys", "credentialed", "sasl.jaas.option"],
)
def test_sensitive_key_expression_does_not_overmatch(key: str) -> None:
    parsed = parse_compiled_topic_artifact(
        _topic(config={key: "safe"}),
        expected_project=PROJECT,
    )
    assert parsed.config[key] == "safe"


@pytest.mark.parametrize(
    "value",
    [
        "line\nfeed",
        "nul\0value",
        "surrogate\udfff",
        "escaped\ufeffbom",
        "noncharacter\ufffe",
        "noncharacter\uffff",
        "last-code-point\U0010ffff",
    ],
)
def test_rejects_text_that_cannot_be_emitted_unescaped(value: str) -> None:
    with pytest.raises(TopicArtifactFormatError) as raised:
        parse_compiled_topic_artifact(
            _topic(config={"ordinary": value}),
            expected_project=PROJECT,
        )
    assert "line" not in str(raised.value)
    assert "nul" not in str(raised.value)
    assert "surrogate" not in str(raised.value)
    assert "escaped" not in str(raised.value)
    assert "noncharacter" not in str(raised.value)
    assert "last-code-point" not in str(raised.value)


@pytest.mark.parametrize("value", ["\ufeff", "\ufffe", "\uffff", "\U0010ffff"])
def test_rejects_owner_text_that_canonical_yaml_would_escape(value: str) -> None:
    with pytest.raises(TopicArtifactFormatError):
        parse_compiled_topic_artifact(
            _topic(owner_name=f"owner{value}"),
            expected_project=PROJECT,
        )


@pytest.mark.parametrize("missing", ["name", "partitions", "replication_factor", "config", "ownership"])
def test_requires_exact_topic_fields(missing: str) -> None:
    raw = _topic()
    del raw[missing]
    with pytest.raises(TopicArtifactFormatError):
        parse_compiled_topic_artifact(raw, expected_project=PROJECT)


def test_rejects_extra_or_non_string_topic_fields() -> None:
    for extra_key in ("extra", 7):
        raw = _topic()
        raw[extra_key] = "private-extra-value"
        with pytest.raises(TopicArtifactFormatError) as raised:
            parse_compiled_topic_artifact(raw, expected_project=PROJECT)
        assert "private-extra-value" not in str(raised.value)


@pytest.mark.parametrize(
    "ownership",
    [
        None,
        {},
        {"mode": "managed", "project": PROJECT, "type": "model"},
        {
            "mode": "managed",
            "project": PROJECT,
            "type": "model",
            "name": "orders",
            "extra": "private-extra-value",
        },
    ],
)
def test_rejects_missing_or_non_exact_ownership(ownership: object) -> None:
    raw = _topic()
    raw["ownership"] = ownership
    with pytest.raises(TopicArtifactFormatError) as raised:
        parse_compiled_topic_artifact(raw, expected_project=PROJECT)
    assert "private-extra-value" not in str(raised.value)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("project", "other-project"),
        ("project", "bad\nproject"),
        ("type", "connector"),
        ("type", ["model"]),
        ("name", ""),
        ("name", "bad\0owner"),
        ("mode", "adopted"),
        ("mode", "unknown"),
        ("mode", ["managed"]),
    ],
)
def test_rejects_invalid_or_inconsistent_ownership(field: str, value: object) -> None:
    raw = _topic()
    assert isinstance(raw["ownership"], dict)
    raw["ownership"][field] = value
    with pytest.raises(TopicArtifactFormatError) as raised:
        parse_compiled_topic_artifact(raw, expected_project=PROJECT)
    surface = f"{raised.value!s} {raised.value!r}"
    assert "other-project" not in surface
    assert "connector" not in surface
    assert "bad" not in surface


def test_rejects_invalid_expected_project_without_echo() -> None:
    secret = "private\nproject"
    with pytest.raises(TopicArtifactFormatError) as raised:
        parse_compiled_topic_artifact(_topic(), expected_project=secret)
    assert "private" not in str(raised.value)


@pytest.mark.parametrize("field", ["expected_project", "owner_project", "owner_name"])
def test_hostile_truthiness_never_escapes_the_closed_ownership_boundary(field: str) -> None:
    hostile = _HostileTruthValue()
    raw = _topic()
    expected_project: object = PROJECT
    ownership = raw["ownership"]
    assert isinstance(ownership, dict)
    if field == "expected_project":
        expected_project = hostile
    elif field == "owner_project":
        ownership["project"] = hostile
    else:
        ownership["name"] = hostile

    with pytest.raises(TopicArtifactFormatError) as raised:
        parse_compiled_topic_artifact(
            raw,
            expected_project=expected_project,  # type: ignore[arg-type]
        )
    assert "private-hostile" not in f"{raised.value!s} {raised.value!r}"


def test_collection_is_tuple_and_rejects_non_list_and_duplicate_kafka_identity() -> None:
    parsed = parse_compiled_topic_artifacts([_topic()], expected_project=PROJECT)
    assert isinstance(parsed, tuple)
    with pytest.raises(TopicArtifactFormatError):
        parse_compiled_topic_artifacts((_topic(),), expected_project=PROJECT)
    with pytest.raises(TopicArtifactFormatError) as raised:
        parse_compiled_topic_artifacts([_topic(), _topic()], expected_project=PROJECT)
    assert "orders-v1" not in str(raised.value)


def test_collection_rejects_generated_kubernetes_identity_collision(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _Digest:
        def hexdigest(self) -> str:
            return "a" * 64

    monkeypatch.setattr(topic_artifact_module.hashlib, "sha256", lambda _value: _Digest())
    first = _topic(name="Upper_A")
    second = _topic(name="Upper_B")
    with pytest.raises(TopicArtifactFormatError) as raised:
        parse_compiled_topic_artifacts([first, second], expected_project=PROJECT)
    assert "Upper_A" not in str(raised.value)
    assert "Upper_B" not in str(raised.value)


def test_errors_and_representations_are_secret_neutral() -> None:
    topic_secret = "PUBLIC-TOPIC-NAME"
    project_secret = "PUBLIC-PROJECT-NAME"
    owner_secret = "PUBLIC-OWNER-NAME"
    config_secret = "private-config-value"
    parsed = parse_compiled_topic_artifact(
        _topic(
            name=topic_secret,
            project=project_secret,
            owner_name=owner_secret,
            config={"ordinary": config_secret},
        ),
        expected_project=project_secret,
    )
    surfaces = (repr(parsed), repr(parsed.ownership))
    for surface in surfaces:
        assert topic_secret not in surface
        assert project_secret not in surface
        assert owner_secret not in surface
        assert config_secret not in surface

    invalid = deepcopy(_topic())
    invalid["config"] = {"ordinary": object()}
    with pytest.raises(TopicArtifactFormatError) as raised:
        parse_compiled_topic_artifact(invalid, expected_project=PROJECT)
    assert "object at" not in str(raised.value)
