"""Offline integrity and schema tests for Backstage core entities."""

from __future__ import annotations

import base64
import gzip
import hashlib
import importlib.util
import os
import socket
import subprocess
import sys
import tarfile
import zipfile
from collections.abc import Callable, Iterator
from importlib.resources import files
from pathlib import Path

import pytest
from jsonschema import Draft7Validator
from referencing.exceptions import NoSuchResource

from streamt.integrations.catalog import (
    BACKSTAGE_CATALOG_MODEL_VERSION,
    BACKSTAGE_RELEASE,
    BackstageResourceError,
    BackstageValidationError,
    backstage_validation,
    validate_backstage_entity,
)

RESOURCE_EXPECTATIONS = {
    "backstage-1.54.2-entity.json.gz.b64": (
        1_804,
        "b4e741f821e6006b179f0112ae98a8856286f95593e7dc16fb6fdd8aaaba51fc",
    ),
    "backstage-1.54.2-entity-envelope.json.gz.b64": (
        1_775,
        "855ec6c05e3f4d328752349c9bd4888436744db716e8713ff3f58399419b7539",
    ),
    "backstage-1.54.2-entity-meta.json.gz.b64": (
        4_642,
        "d19d6329386fcf5016687b7cb07f71bc9e51d7ad1b54f616c29aefa5ac978616",
    ),
    "backstage-1.54.2-common.json.gz.b64": (
        3_644,
        "7b99ac97e6ae64795836ef4291f859701291bc1880578e6ee7502ec19cc15665",
    ),
    "backstage-1.54.2-system-v1alpha1.json.gz.b64": (
        2_148,
        "9ad7bf11e4db9d1b9b6d4c976ffa5393cd6c02d36cb1b23f1efc3a9c4b072afe",
    ),
    "backstage-1.54.2-resource-v1alpha1.json.gz.b64": (
        2_114,
        "1cea27c74540638d8b44cad0b222c93a887e5a4609f28d6e9bb882ab43d7859e",
    ),
    "backstage-1.54.2-component-v1alpha1.json.gz.b64": (
        3_333,
        "9ff892085ddaf78243ead1cdcf24f03eb5f9af16dffbed189fae4ab0806a8d56",
    ),
}
LEGAL_RESOURCE_EXPECTATIONS = {
    "backstage-1.54.2-LICENSE.txt": (
        "e3620220d6f8a43cb5c968720f86d4e7c6e97847ee61a9c7694039896efb869b"
    ),
    "backstage-1.54.2-NOTICE.txt": (
        "36395569b3867d0769a40ffb908709c0b5fbfd79bf4b827856dd34feeed644ba"
    ),
}
_DISTRIBUTION_RESOURCE_NAMES = frozenset((*RESOURCE_EXPECTATIONS, *LEGAL_RESOURCE_EXPECTATIONS))


@pytest.fixture(autouse=True)
def clear_backstage_schema_caches() -> Iterator[None]:
    backstage_validation._clear_schema_caches()
    yield
    backstage_validation._clear_schema_caches()


def _minimal_entities() -> tuple[dict[str, object], ...]:
    metadata = {"name": "orders", "namespace": "payments"}
    return (
        {
            "apiVersion": "backstage.io/v1alpha1",
            "kind": "System",
            "metadata": metadata,
            "spec": {"owner": "group:platform/payments"},
        },
        {
            "apiVersion": "backstage.io/v1alpha1",
            "kind": "Resource",
            "metadata": metadata,
            "spec": {
                "type": "kafka-topic",
                "owner": "group:platform/payments",
            },
        },
        {
            "apiVersion": "backstage.io/v1alpha1",
            "kind": "Component",
            "metadata": metadata,
            "spec": {
                "type": "data-pipeline",
                "lifecycle": "production",
                "owner": "group:platform/payments",
            },
        },
    )


def _copy_schema_resources(destination: Path) -> None:
    package = files("streamt.docs.schemas")
    for resource_name in RESOURCE_EXPECTATIONS:
        destination.joinpath(resource_name).write_bytes(
            package.joinpath(resource_name).read_bytes()
        )


def _assert_backstage_distribution_resources(
    *,
    member_names: set[str],
    read_member: Callable[[str], bytes],
) -> None:
    members_by_basename: dict[str, list[str]] = {}
    for member in member_names:
        basename = member.rsplit("/", maxsplit=1)[-1]
        if "streamt/docs/schemas/" in member and basename.startswith("backstage-1.54.2-"):
            members_by_basename.setdefault(basename, []).append(member)

    assert set(members_by_basename) == _DISTRIBUTION_RESOURCE_NAMES
    assert all(len(matches) == 1 for matches in members_by_basename.values())

    for resource_name, (decoded_size, decoded_checksum) in RESOURCE_EXPECTATIONS.items():
        member = members_by_basename[resource_name][0]
        encoded = read_member(member)
        decoded = gzip.decompress(base64.b64decode(b"".join(encoded.split()), validate=True))
        assert len(decoded) == decoded_size
        assert hashlib.sha256(decoded).hexdigest() == decoded_checksum

    for resource_name, expected_checksum in LEGAL_RESOURCE_EXPECTATIONS.items():
        member = members_by_basename[resource_name][0]
        assert hashlib.sha256(read_member(member)).hexdigest() == expected_checksum


def test_release_and_catalog_model_pins_are_distinct_and_exact() -> None:
    assert BACKSTAGE_RELEASE == "1.54.2"
    assert BACKSTAGE_CATALOG_MODEL_VERSION == "1.10.0"


def test_all_pinned_resources_have_exact_bytes_digests_and_valid_schemas() -> None:
    package = files("streamt.docs.schemas")
    total_size = 0
    for resource_name, (expected_size, expected_sha256) in RESOURCE_EXPECTATIONS.items():
        encoded = b"".join(package.joinpath(resource_name).read_bytes().split())
        compressed = base64.b64decode(encoded, validate=True)
        assert compressed[:4] == b"\x1f\x8b\x08\x00"
        assert compressed[4:8] == b"\x00\x00\x00\x00"
        decoded = gzip.decompress(compressed)
        total_size += len(decoded)
        assert len(decoded) == expected_size
        assert hashlib.sha256(decoded).hexdigest() == expected_sha256

    assert total_size == 19_460
    schemas = backstage_validation._official_schemas()
    assert set(schemas) == {
        "Entity",
        "EntityEnvelope",
        "EntityMeta",
        "common",
        "SystemV1alpha1",
        "ResourceV1alpha1",
        "ComponentV1alpha1",
    }
    for schema in schemas.values():
        assert schema["$schema"] == "http://json-schema.org/draft-07/schema"
        Draft7Validator.check_schema(schema)

    def refs(value: object) -> set[str]:
        if isinstance(value, dict):
            found = {value["$ref"]} if isinstance(value.get("$ref"), str) else set()
            return found.union(*(refs(child) for child in value.values()))
        if isinstance(value, list):
            return set().union(*(refs(child) for child in value))
        return set()

    assert {schema_id: refs(schema) for schema_id, schema in schemas.items()} == {
        "Entity": {"EntityMeta", "common#relation", "common#status"},
        "EntityEnvelope": set(),
        "EntityMeta": set(),
        "common": {"#error", "#reference", "#statusItem", "#statusLevel"},
        "SystemV1alpha1": {"Entity"},
        "ResourceV1alpha1": {"Entity"},
        "ComponentV1alpha1": {"Entity"},
    }


@pytest.mark.parametrize("entity", _minimal_entities())
def test_minimal_supported_entities_pass_official_schemas(
    entity: dict[str, object],
) -> None:
    validate_backstage_entity(entity)


def test_entity_envelope_is_validated_before_the_selected_kind_schema() -> None:
    entity = {
        "apiVersion": "backstage.io/v1alpha1",
        "kind": "System",
        "metadata": {},
        "spec": {"owner": "group:platform/payments"},
    }

    with pytest.raises(BackstageValidationError) as captured:
        validate_backstage_entity(entity)

    assert str(captured.value) == "Backstage entity envelope validation failed at /metadata"


@pytest.mark.parametrize(
    ("entity", "location"),
    [
        (
            {
                "apiVersion": "backstage.io/v1alpha1",
                "kind": "System",
                "metadata": {"name": "orders"},
                "spec": {},
            },
            "/spec",
        ),
        (
            {
                "apiVersion": "backstage.io/v1alpha1",
                "kind": "Resource",
                "metadata": {"name": "orders"},
                "spec": {"owner": "group:platform/payments"},
            },
            "/spec",
        ),
        (
            {
                "apiVersion": "backstage.io/v1alpha1",
                "kind": "Component",
                "metadata": {"name": "orders"},
                "spec": {
                    "type": "data-pipeline",
                    "owner": "group:platform/payments",
                },
            },
            "/spec",
        ),
        (
            {
                "apiVersion": "backstage.io/v2",
                "kind": "System",
                "metadata": {"name": "orders"},
                "spec": {"owner": "group:platform/payments"},
            },
            "/apiVersion",
        ),
        (
            {
                "apiVersion": "backstage.io/v1alpha1",
                "kind": "Resource",
                "metadata": {"name": ""},
                "spec": {
                    "type": "kafka-topic",
                    "owner": "group:platform/payments",
                },
            },
            "/metadata/name",
        ),
    ],
)
def test_invalid_entities_fail_at_a_safe_location(
    entity: dict[str, object],
    location: str,
) -> None:
    with pytest.raises(BackstageValidationError) as captured:
        validate_backstage_entity(entity)

    assert str(captured.value).endswith(f"at {location}")


@pytest.mark.parametrize("kind", ["API", "system", "", None, ["System"]])
def test_unsupported_or_malformed_kind_fails_before_schema_selection(
    kind: object,
) -> None:
    entity = {
        "apiVersion": "backstage.io/v1alpha1",
        "kind": kind,
        "metadata": {"name": "orders"},
        "spec": {},
    }
    with pytest.raises(
        BackstageValidationError,
        match=r"^Backstage entity kind is unsupported$",
    ):
        validate_backstage_entity(entity)


def test_non_object_entity_fails_cleanly() -> None:
    with pytest.raises(
        BackstageValidationError,
        match=r"^Backstage entity must be an object$",
    ):
        validate_backstage_entity([])  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "corrupt_payload",
    [b"not-valid-base64%%%", base64.b64encode(b"not a gzip stream")],
)
def test_invalid_resource_encoding_fails_closed_with_safe_error(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    corrupt_payload: bytes,
) -> None:
    _copy_schema_resources(tmp_path)
    target = tmp_path / "backstage-1.54.2-entity.json.gz.b64"
    target.write_bytes(corrupt_payload)
    monkeypatch.setattr(backstage_validation, "files", lambda _package: tmp_path)
    backstage_validation._clear_schema_caches()

    with pytest.raises(BackstageResourceError) as captured:
        backstage_validation._official_schemas()

    assert str(captured.value) == "A bundled Backstage schema cannot be decoded"
    assert str(tmp_path) not in str(captured.value)


@pytest.mark.parametrize(
    ("tamper", "expected_message"),
    [
        ("wrong-size", "A bundled Backstage schema does not match its pinned size"),
        (
            "wrong-checksum",
            "A bundled Backstage schema does not match its pinned checksum",
        ),
    ],
)
def test_valid_gzip_tampering_fails_size_or_checksum_pin(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    tamper: str,
    expected_message: str,
) -> None:
    _copy_schema_resources(tmp_path)
    target = tmp_path / "backstage-1.54.2-entity.json.gz.b64"
    if tamper == "wrong-size":
        raw = b"{}"
    else:
        encoded = b"".join(target.read_bytes().split())
        raw = gzip.decompress(base64.b64decode(encoded, validate=True))
        assert raw.endswith(b"\n")
        raw = raw[:-1] + b" "
    target.write_bytes(base64.b64encode(gzip.compress(raw, mtime=0)))
    monkeypatch.setattr(backstage_validation, "files", lambda _package: tmp_path)
    backstage_validation._clear_schema_caches()

    with pytest.raises(BackstageResourceError) as captured:
        backstage_validation._official_schemas()

    assert str(captured.value) == expected_message


def test_registry_resolves_bare_internal_refs_without_network_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail_network(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("unexpected network access")

    monkeypatch.setattr(socket, "create_connection", fail_network)
    monkeypatch.setattr(socket, "getaddrinfo", fail_network)

    for entity in _minimal_entities():
        validate_backstage_entity(entity)
    with pytest.raises(NoSuchResource):
        backstage_validation._schema_registry().get_or_retrieve(
            "https://example.invalid/unbundled-schema.json"
        )


def test_schema_error_does_not_echo_untrusted_entity_material() -> None:
    secret = "secret-token-should-never-appear"
    entity = {
        "apiVersion": "backstage.io/v1alpha1",
        "kind": "System",
        "metadata": {"name": "orders"},
        "spec": {"owner": "group:platform/payments"},
        secret: secret,
    }

    with pytest.raises(BackstageValidationError) as captured:
        validate_backstage_entity(entity)

    assert str(captured.value) == "Backstage System schema validation failed at /"
    assert secret not in str(captured.value)


def test_built_wheel_and_source_distribution_contain_backstage_resources(
    tmp_path: Path,
) -> None:
    configured_dist = os.environ.get("STREAMT_TEST_DISTRIBUTIONS_DIR")
    if configured_dist is None:
        if importlib.util.find_spec("build") is None:
            pytest.skip("the build frontend is required for distribution inspection")
        distribution_dir = tmp_path / "dist"
        repository = Path(__file__).parents[2]
        subprocess.run(
            [
                sys.executable,
                "-m",
                "build",
                "--wheel",
                "--sdist",
                "--outdir",
                str(distribution_dir),
            ],
            cwd=repository,
            check=True,
            capture_output=True,
            text=True,
        )
    else:
        distribution_dir = Path(configured_dist)

    wheels = list(distribution_dir.glob("streamt-*.whl"))
    source_distributions = list(distribution_dir.glob("streamt-*.tar.gz"))
    assert len(wheels) == 1, wheels
    assert len(source_distributions) == 1, source_distributions

    with zipfile.ZipFile(wheels[0]) as wheel:
        _assert_backstage_distribution_resources(
            member_names=set(wheel.namelist()),
            read_member=wheel.read,
        )

    with tarfile.open(source_distributions[0], "r:gz") as source_distribution:

        def read_source_member(name: str) -> bytes:
            extracted = source_distribution.extractfile(name)
            assert extracted is not None
            return extracted.read()

        _assert_backstage_distribution_resources(
            member_names=set(source_distribution.getnames()),
            read_member=read_source_member,
        )
