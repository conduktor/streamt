"""Tests for strict, secret-neutral Backstage adapter inputs."""

from __future__ import annotations

from dataclasses import FrozenInstanceError
from pathlib import Path
from typing import Any, cast

import pytest

from streamt.integrations.catalog.inputs import (
    CatalogInputError,
    ParsedEntityRef,
    load_owner_map,
    require_catalog_id,
    require_catalog_namespace,
    require_entity_ref,
    require_lifecycle,
    validate_owner_map,
)


@pytest.mark.parametrize(
    "value",
    [
        "a",
        "catalog-1",
        "catalog.one_two",
        "a" + "x" * 126 + "z",
    ],
)
def test_catalog_id_accepts_exact_valid_boundaries(value: str) -> None:
    assert require_catalog_id(value) == value


@pytest.mark.parametrize(
    "value",
    [
        "",
        "A",
        "-catalog",
        "catalog-",
        "catalog id",
        "a" * 129,
        "catalog\n",
        1,
        None,
    ],
)
def test_catalog_id_rejects_invalid_values_without_echo(value: object) -> None:
    with pytest.raises(CatalogInputError) as raised:
        require_catalog_id(value)

    assert raised.value.location == "catalog_id"
    if isinstance(value, str) and value:
        assert value not in str(raised.value)


@pytest.mark.parametrize("value", ["a", "team-catalog", "a" * 63])
def test_catalog_namespace_accepts_exact_valid_boundaries(value: str) -> None:
    assert require_catalog_namespace(value) == value


@pytest.mark.parametrize(
    "value",
    [
        "",
        "Default",
        "team.catalog",
        "team_catalog",
        "team--catalog",
        "-team",
        "team-",
        "a" * 64,
        "team\x00catalog",
        [],
    ],
)
def test_catalog_namespace_rejects_noncanonical_values(value: object) -> None:
    with pytest.raises(CatalogInputError) as raised:
        require_catalog_namespace(value)
    assert raised.value.location == "catalog_namespace"


def test_lifecycle_uses_codepoint_boundaries_and_preserves_exact_value() -> None:
    assert require_lifecycle("production") == "production"
    assert require_lifecycle("é" * 256) == "é" * 256

    for invalid in ("", " \t", "é" * 257, "prod\nsecret", "bad\ud800", 7):
        with pytest.raises(CatalogInputError) as raised:
            require_lifecycle(invalid)
        assert raised.value.location == "lifecycle"
        assert "prod\nsecret" not in str(raised.value)


def test_complete_entity_refs_are_parsed_exactly_and_frozen() -> None:
    parsed = require_entity_ref(
        "group:team-space/payments_team.v1",
        allowed_kinds={"group", "user"},
        location="default_owner_ref",
    )

    assert parsed == ParsedEntityRef(
        kind="group",
        namespace="team-space",
        name="payments_team.v1",
        canonical="group:team-space/payments_team.v1",
    )
    with pytest.raises(FrozenInstanceError):
        parsed.name = "replacement"  # type: ignore[misc]


def test_entity_ref_component_boundaries_are_inclusive() -> None:
    namespace = "n" * 63
    name = "x" * 63
    value = f"resource:{namespace}/{name}"

    parsed = require_entity_ref(
        value,
        allowed_kinds={"resource"},
        location="kafka_cluster_ref",
    )

    assert parsed.namespace == namespace
    assert parsed.name == name
    assert parsed.canonical == value


@pytest.mark.parametrize(
    "value",
    [
        "team/name",
        "group:name",
        "group:/name",
        "group:team/",
        "group:team/name/extra",
        "Group:team/name",
        "group:Team/name",
        "group:team/Name",
        "group:team.name/name",
        "group:team--space/name",
        "group:team/name__part",
        "group:team/name.-part",
        "group:-team/name",
        "group:team/-name",
        f"group:{'n' * 64}/name",
        f"group:team/{'n' * 64}",
        " group:team/name",
        "group:team/name ",
        "group:team/na\x00me",
        "group:team/na\ud800me",
        "x" * 257,
        1,
    ],
)
def test_entity_ref_rejects_implicit_or_noncanonical_forms(value: object) -> None:
    with pytest.raises(CatalogInputError) as raised:
        require_entity_ref(
            value,
            allowed_kinds={"group", "user"},
            location="owner_ref",
        )

    assert raised.value.location == "owner_ref"
    if isinstance(value, str):
        assert value not in str(raised.value)


def test_entity_ref_enforces_caller_kind_and_allowed_kind_configuration() -> None:
    with pytest.raises(CatalogInputError):
        require_entity_ref(
            "resource:default/kafka",
            allowed_kinds={"group", "user"},
            location="owner_ref",
        )
    for invalid_kinds in (set(), {"Group"}, cast(Any, "group")):
        with pytest.raises(CatalogInputError) as raised:
            require_entity_ref(
                "group:default/team",
                allowed_kinds=invalid_kinds,
                location="owner_ref",
            )
        assert raised.value.location == "owner_ref"


def test_parsed_entity_ref_rejects_inconsistent_direct_construction() -> None:
    with pytest.raises(CatalogInputError) as raised:
        ParsedEntityRef(
            kind="group",
            namespace="default",
            name="team",
            canonical="user:default/team",
        )
    assert raised.value.location == "entity_ref"


def test_validate_owner_map_returns_sorted_defensive_immutable_mapping() -> None:
    owners: dict[str, object] = {
        "z-team": "user:people/z-user",
        "a-team": "group:teams/a-team",
    }
    source: dict[str, object] = {"version": 1, "owners": owners}

    validated = validate_owner_map(source)
    owners["a-team"] = "user:people/mutated"
    source["version"] = 2

    assert tuple(validated) == ("a-team", "z-team")
    assert validated["a-team"].canonical == "group:teams/a-team"
    with pytest.raises(TypeError):
        validated["new"] = ParsedEntityRef(  # type: ignore[index]
            "group", "teams", "new", "group:teams/new"
        )


@pytest.mark.parametrize(
    ("mapping", "location"),
    [
        ([], "owner_map"),
        ({}, "owner_map"),
        ({"version": 1}, "owner_map"),
        ({"owners": {}}, "owner_map"),
        ({"version": 1, "owners": {}, "extra": True}, "owner_map"),
        ({"version": True, "owners": {}}, "owner_map/version"),
        ({"version": 2, "owners": {}}, "owner_map/version"),
        ({"version": "1", "owners": {}}, "owner_map/version"),
        ({"version": 1, "owners": []}, "owner_map/owners"),
        ({"version": 1, "owners": {1: "group:default/team"}}, "owner_map/owners"),
        ({"version": 1, "owners": {"team": 1}}, "owner_map/owners/0/ref"),
        (
            {"version": 1, "owners": {"team": "resource:default/team"}},
            "owner_map/owners/0/ref",
        ),
    ],
)
def test_validate_owner_map_rejects_wrong_shape_and_types(
    mapping: object,
    location: str,
) -> None:
    with pytest.raises(CatalogInputError) as raised:
        validate_owner_map(mapping)
    assert raised.value.location == location


def test_owner_count_boundary_is_inclusive() -> None:
    owners = {
        f"owner-{index}": "group:default/team"
        for index in range(10_000)
    }
    validated = validate_owner_map({"version": 1, "owners": owners})
    assert len(validated) == 10_000

    owners["one-too-many"] = "group:default/team"
    with pytest.raises(CatalogInputError) as raised:
        validate_owner_map({"version": 1, "owners": owners})
    assert raised.value.location == "owner_map/owners"


def test_owner_label_codepoint_boundaries_and_controls() -> None:
    valid_label = "é" * 256
    validated = validate_owner_map(
        {"version": 1, "owners": {valid_label: "group:default/team"}}
    )
    assert tuple(validated) == (valid_label,)

    for invalid in ("é" * 257, " ", "team\nsecret", "team\ud800"):
        with pytest.raises(CatalogInputError) as raised:
            validate_owner_map(
                {"version": 1, "owners": {invalid: "group:default/team"}}
            )
        assert raised.value.location == "owner_map/owners/0/label"
        assert "secret" not in str(raised.value)


def test_owner_reference_length_limit_is_enforced_before_parsing() -> None:
    longest_grammatical = f"group:{'n' * 63}/{'x' * 63}"
    assert validate_owner_map(
        {"version": 1, "owners": {"team": longest_grammatical}}
    )["team"].canonical == longest_grammatical

    with pytest.raises(CatalogInputError) as raised:
        validate_owner_map(
            {"version": 1, "owners": {"team": "g" * 257}}
        )
    assert raised.value.location == "owner_map/owners/0/ref"


def _write(path: Path, payload: str | bytes) -> None:
    if isinstance(payload, bytes):
        path.write_bytes(payload)
    else:
        path.write_text(payload, encoding="utf-8")


def test_load_owner_map_reads_strict_utf8_json(tmp_path: Path) -> None:
    path = tmp_path / "owners.json"
    _write(
        path,
        '{"version":1,"owners":{"équipe":"group:teams/platform"}}',
    )

    result = load_owner_map(path)

    assert result["équipe"].canonical == "group:teams/platform"


@pytest.mark.parametrize(
    "payload",
    [
        b"\xef\xbb\xbf{\"version\":1,\"owners\":{}}",
        b"\xff",
        b"version: 1\nowners: {}\n",
        b"[1,2,3]",
        b'{"version":NaN,"owners":{}}',
        b'{"version":1,"version":1,"owners":{}}',
        b'{"version":1,"owners":{"team":"group:default/a","team":"user:default/b"}}',
        b'{"version":1,"owners":{},"extra":{"key":1,"key":2}}',
        b'{"version":1,"owners":{"\\ud800":"group:default/team"}}',
    ],
)
def test_load_owner_map_rejects_non_strict_json_forms(
    tmp_path: Path,
    payload: bytes,
) -> None:
    path = tmp_path / "secret-owner-map-name.json"
    _write(path, payload)

    with pytest.raises(CatalogInputError) as raised:
        load_owner_map(path)

    assert raised.value.location == "owner_map"
    assert "secret-owner-map-name" not in str(raised.value)
    assert "team" not in str(raised.value)


def test_owner_map_byte_limit_is_inclusive(tmp_path: Path) -> None:
    base = b'{"version":1,"owners":{}}'
    exact = tmp_path / "exact.json"
    exact.write_bytes(base + b" " * (1_048_576 - len(base)))

    assert load_owner_map(exact) == {}

    oversized = tmp_path / "oversized.json"
    oversized.write_bytes(base + b" " * (1_048_577 - len(base)))
    with pytest.raises(CatalogInputError) as raised:
        load_owner_map(oversized)
    assert raised.value.location == "owner_map"


def test_owner_map_depth_boundary_is_checked_before_shape(tmp_path: Path) -> None:
    depth_four = tmp_path / "depth-four.json"
    _write(
        depth_four,
        '{"version":1,"owners":{"team":{"a":{"b":"value"}}}}',
    )
    with pytest.raises(CatalogInputError) as raised:
        load_owner_map(depth_four)
    assert raised.value.location == "owner_map/owners/0/ref"

    depth_five = tmp_path / "depth-five.json"
    _write(
        depth_five,
        '{"version":1,"owners":{"team":{"a":{"b":{"c":"value"}}}}}',
    )
    with pytest.raises(CatalogInputError) as raised:
        load_owner_map(depth_five)
    assert raised.value.location == "owner_map"
    assert "depth" in str(raised.value).lower()


def test_owner_map_never_expands_environment_variables(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("OWNER_REF_SECRET", "group:default/expanded")
    path = tmp_path / "owners.json"
    _write(
        path,
        '{"version":1,"owners":{"team":"${OWNER_REF_SECRET}"}}',
    )

    with pytest.raises(CatalogInputError) as raised:
        load_owner_map(path)

    assert raised.value.location == "owner_map/owners/0/ref"
    assert "OWNER_REF_SECRET" not in str(raised.value)
    assert "expanded" not in str(raised.value)


def test_owner_map_read_failures_do_not_expose_paths(tmp_path: Path) -> None:
    path = tmp_path / "credential-secret-owner-map.json"

    with pytest.raises(CatalogInputError) as raised:
        load_owner_map(path)

    assert raised.value.location == "owner_map"
    assert "credential-secret" not in str(raised.value)
    assert raised.value.__cause__ is None
