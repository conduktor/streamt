"""Closed and pinned-schema validation tests for Strimzi KafkaTopic output."""

from __future__ import annotations

import base64
import copy
import gzip
import hashlib
import json
from collections.abc import Iterator
from importlib.resources import files
from pathlib import Path

import pytest
import yaml
from jsonschema import Draft7Validator

from streamt.core.errors import ErrorCode
from streamt.integrations.gitops import strimzi_validation
from streamt.integrations.gitops.strimzi_validation import (
    STRIMZI_API_VERSION,
    STRIMZI_KIND,
    STRIMZI_RELEASE,
    StrimziResourceError,
    StrimziValidationError,
    validate_kafkatopic_document,
    validate_kafkatopic_documents,
    validate_kubernetes_label_value,
    validate_kubernetes_namespace,
)

FIXTURE_DIR = Path(__file__).parents[1] / "fixtures" / "strimzi" / "1.2.0"
SCHEMA_PACKAGE = "streamt.integrations.gitops.schemas"
SCHEMA_RESOURCE = "strimzi-1.2.0-kafkatopic-crd.yaml.gz.b64"


@pytest.fixture(autouse=True)
def clear_strimzi_schema_caches() -> Iterator[None]:
    strimzi_validation._clear_schema_caches()
    yield
    strimzi_validation._clear_schema_caches()


def _documents() -> list[dict[str, object]]:
    value = json.loads((FIXTURE_DIR / "expected-documents.json").read_text())
    assert isinstance(value, list)
    assert all(isinstance(item, dict) for item in value)
    return value


def _document() -> dict[str, object]:
    return copy.deepcopy(_documents()[1])


def _decoded_crd() -> bytes:
    encoded = files(SCHEMA_PACKAGE).joinpath(SCHEMA_RESOURCE).read_bytes()
    return gzip.decompress(base64.b64decode(b"".join(encoded.split()), validate=True))


class _FakeResource:
    def __init__(self, payload: bytes) -> None:
        self._payload = payload

    def joinpath(self, _resource: str) -> _FakeResource:
        return self

    def read_bytes(self) -> bytes:
        return self._payload


def _replace_resource(monkeypatch: pytest.MonkeyPatch, payload: bytes) -> None:
    resource = _FakeResource(payload)
    monkeypatch.setattr(strimzi_validation, "files", lambda _package: resource)


def _encoded_crd(value: object) -> tuple[bytes, bytes]:
    raw = yaml.safe_dump(value, sort_keys=False).encode("utf-8")
    return base64.b64encode(gzip.compress(raw, mtime=0)), raw


def _parsed_crd() -> dict[str, object]:
    value = yaml.safe_load(_decoded_crd())
    assert isinstance(value, dict)
    return value


class _HostileString(str):
    __hash__ = str.__hash__

    def __eq__(self, _other: object) -> bool:
        raise AssertionError("private-hostile-equality-sentinel")

    def __ne__(self, _other: object) -> bool:
        raise AssertionError("private-hostile-inequality-sentinel")


class _HostileValue:
    def __bool__(self) -> bool:
        raise AssertionError("private-hostile-truth-sentinel")

    def __eq__(self, _other: object) -> bool:
        raise AssertionError("private-hostile-equality-sentinel")

    def __hash__(self) -> int:
        raise AssertionError("private-hostile-hash-sentinel")

    def __str__(self) -> str:
        raise AssertionError("private-hostile-string-sentinel")

    def __repr__(self) -> str:
        raise AssertionError("private-hostile-repr-sentinel")


def _set_path(root: object, path: tuple[str | int, ...], value: object) -> None:
    current = root
    for part in path[:-1]:
        if isinstance(part, int):
            assert isinstance(current, list)
            current = current[part]
        else:
            assert isinstance(current, dict)
            current = current[part]
    final = path[-1]
    if isinstance(final, int):
        assert isinstance(current, list)
        current[final] = value
    else:
        assert isinstance(current, dict)
        current[final] = value


def test_target_and_diagnostic_codes_are_exact() -> None:
    assert STRIMZI_RELEASE == "1.2.0"
    assert STRIMZI_API_VERSION == "kafka.strimzi.io/v1"
    assert STRIMZI_KIND == "KafkaTopic"
    assert ErrorCode.STRIMZI_INVALID == "E509_STRIMZI_INVALID"
    assert ErrorCode.STRIMZI_EXTERNAL_TOPIC_OMITTED == "W120_STRIMZI_EXTERNAL_TOPIC_OMITTED"
    assert ErrorCode.STRIMZI_ARTIFACTS_OMITTED == "W121_STRIMZI_ARTIFACTS_OMITTED"


def test_exact_pinned_resource_loads_and_has_one_explicit_extension() -> None:
    raw = _decoded_crd()
    assert len(raw) == 6_329
    assert hashlib.sha256(raw).hexdigest() == (
        "36390f0731c699448076d4ee739e8b7f331d083e91a7fb71500aaa830ab1127e"
    )
    schema = strimzi_validation._kafkatopic_openapi_schema()
    Draft7Validator.check_schema(schema)

    def extension_paths(value: object, path: tuple[str, ...] = ()) -> list[tuple[str, ...]]:
        if isinstance(value, dict):
            result: list[tuple[str, ...]] = []
            for key, item in value.items():
                assert isinstance(key, str)
                item_path = (*path, key)
                if key.startswith("x-kubernetes-"):
                    result.append(item_path)
                result.extend(extension_paths(item, item_path))
            return result
        if isinstance(value, list):
            return [
                item_path
                for index, item in enumerate(value)
                for item_path in extension_paths(item, (*path, str(index)))
            ]
        return []

    raw_schema = strimzi_validation._extract_kafkatopic_openapi_schema(_parsed_crd())
    assert extension_paths(raw_schema) == [
        (
            "properties",
            "spec",
            "properties",
            "config",
            "x-kubernetes-preserve-unknown-fields",
        )
    ]
    assert extension_paths(schema) == []


@pytest.mark.parametrize(
    "payload",
    [
        b"not base64!",
        base64.b64encode(b"not gzip"),
    ],
)
def test_resource_loader_rejects_invalid_encoding_without_cause(
    monkeypatch: pytest.MonkeyPatch,
    payload: bytes,
) -> None:
    _replace_resource(monkeypatch, payload)
    with pytest.raises(StrimziResourceError) as captured:
        strimzi_validation._pinned_kafkatopic_crd()
    assert str(captured.value) == "The bundled Strimzi KafkaTopic schema is invalid"
    assert captured.value.__cause__ is None


def test_resource_loader_rejects_changed_decoded_digest(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    changed = _decoded_crd() + b"\n"
    _replace_resource(
        monkeypatch,
        base64.b64encode(gzip.compress(changed, mtime=0)),
    )
    with pytest.raises(StrimziResourceError):
        strimzi_validation._pinned_kafkatopic_crd()


def test_resource_loader_translates_missing_package_without_leaking_module_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sentinel = "private-missing-package-sentinel"

    def missing(_package: str) -> object:
        raise ModuleNotFoundError(sentinel)

    monkeypatch.setattr(strimzi_validation, "files", missing)
    with pytest.raises(StrimziResourceError) as captured:
        strimzi_validation._pinned_kafkatopic_crd()
    assert str(captured.value) == "The bundled Strimzi KafkaTopic schema is invalid"
    assert sentinel not in repr(captured.value)
    assert captured.value.__cause__ is None


@pytest.mark.parametrize(
    "mutation",
    [
        "extra-root",
        "wrong-api",
        "wrong-name",
        "wrong-group",
        "wrong-kind",
        "extra-version",
        "wrong-version",
        "not-served",
        "not-storage",
        "extra-version-field",
        "missing-openapi",
    ],
)
def test_crd_extraction_fails_closed_on_unexpected_shape_or_version(
    mutation: str,
) -> None:
    crd = copy.deepcopy(_parsed_crd())
    metadata = crd["metadata"]
    spec = crd["spec"]
    assert isinstance(metadata, dict)
    assert isinstance(spec, dict)
    names = spec["names"]
    versions = spec["versions"]
    assert isinstance(names, dict)
    assert isinstance(versions, list)
    version = versions[0]
    assert isinstance(version, dict)

    if mutation == "extra-root":
        crd["unexpected"] = True
    elif mutation == "wrong-api":
        crd["apiVersion"] = "apiextensions.k8s.io/v1beta1"
    elif mutation == "wrong-name":
        metadata["name"] = "other.kafka.strimzi.io"
    elif mutation == "wrong-group":
        spec["group"] = "other.strimzi.io"
    elif mutation == "wrong-kind":
        names["kind"] = "Kafka"
    elif mutation == "extra-version":
        versions.append(copy.deepcopy(version))
    elif mutation == "wrong-version":
        version["name"] = "v1beta2"
    elif mutation == "not-served":
        version["served"] = False
    elif mutation == "not-storage":
        version["storage"] = False
    elif mutation == "extra-version-field":
        version["deprecated"] = False
    elif mutation == "missing-openapi":
        version["schema"] = {}
    else:
        raise AssertionError(mutation)

    with pytest.raises(StrimziResourceError):
        strimzi_validation._extract_kafkatopic_openapi_schema(crd)


@pytest.mark.parametrize(
    ("path", "value"),
    [
        (("properties", "spec", "x-kubernetes-map-type"), "atomic"),
        (
            (
                "properties",
                "spec",
                "properties",
                "config",
                "x-kubernetes-preserve-unknown-fields",
            ),
            False,
        ),
    ],
)
def test_unexpected_or_changed_kubernetes_extensions_fail_closed(
    path: tuple[str, ...],
    value: object,
) -> None:
    schema: object = copy.deepcopy(
        strimzi_validation._extract_kafkatopic_openapi_schema(_parsed_crd())
    )
    current = schema
    for key in path[:-1]:
        assert isinstance(current, dict)
        current = current[key]
    assert isinstance(current, dict)
    current[path[-1]] = value
    with pytest.raises(StrimziResourceError):
        strimzi_validation._strip_kubernetes_extensions(schema)


@pytest.mark.parametrize(
    "namespace",
    ["a", "0", "payments-prod", "a" * 63, "a-0"],
)
def test_accepts_exact_kubernetes_namespace_boundary(namespace: str) -> None:
    assert validate_kubernetes_namespace(namespace) == namespace


@pytest.mark.parametrize(
    "namespace",
    ["", "A", "a_b", ".name", "name.", "-name", "name-", "a" * 64, 1],
)
def test_rejects_invalid_kubernetes_namespaces(namespace: object) -> None:
    with pytest.raises(StrimziValidationError) as captured:
        validate_kubernetes_namespace(namespace)
    assert str(captured.value) == ("Strimzi KafkaTopic validation failed at /metadata/namespace")


@pytest.mark.parametrize(
    "label",
    ["", "A", "a_b", "a.b", "A-0_x.y", "a" * 63],
)
def test_accepts_exact_kubernetes_label_value_boundary(label: str) -> None:
    assert validate_kubernetes_label_value(label) == label


@pytest.mark.parametrize(
    "label",
    ["-a", "a-", ".a", "a.", "_a", "a_", "a" * 64, "caf\N{LATIN SMALL LETTER E WITH ACUTE}", 1],
)
def test_rejects_invalid_kubernetes_label_values(label: object) -> None:
    with pytest.raises(StrimziValidationError):
        validate_kubernetes_label_value(label)


def test_reviewed_documents_pass_closed_then_pinned_validation_without_mutation() -> None:
    documents = _documents()
    before = copy.deepcopy(documents)
    for document in documents:
        validate_kafkatopic_document(document)
    validate_kafkatopic_documents(tuple(documents))
    assert documents == before


def test_mutating_private_crd_schema_or_validator_cannot_poison_public_validation() -> None:
    document = _document()
    validate_kafkatopic_document(document)

    poisoned_crd = strimzi_validation._pinned_kafkatopic_crd()
    poisoned_crd.clear()
    assert strimzi_validation._pinned_kafkatopic_crd()

    poisoned_schema = strimzi_validation._kafkatopic_openapi_schema()
    poisoned_schema.clear()
    assert strimzi_validation._kafkatopic_openapi_schema()

    poisoned_validator = strimzi_validation._kafkatopic_validator()
    assert isinstance(poisoned_validator.schema, dict)
    poisoned_validator.schema.clear()

    validate_kafkatopic_document(document)
    validate_kafkatopic_documents(tuple(_documents()))


def test_collection_prepares_schema_and_constructs_validator_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    counts = {"prepare": 0, "validator": 0}
    original_prepare = strimzi_validation._prepare_kafkatopic_openapi_schema_bytes
    original_validator = strimzi_validation._kafkatopic_validator

    def prepare() -> bytes:
        counts["prepare"] += 1
        return original_prepare()

    def validator() -> Draft7Validator:
        counts["validator"] += 1
        return original_validator()

    monkeypatch.setattr(
        strimzi_validation,
        "_prepare_kafkatopic_openapi_schema_bytes",
        prepare,
    )
    monkeypatch.setattr(strimzi_validation, "_kafkatopic_validator", validator)
    validate_kafkatopic_documents(tuple(_documents()))
    assert counts == {"prepare": 1, "validator": 1}


@pytest.mark.parametrize(
    "path",
    [
        ("apiVersion",),
        ("kind",),
        ("metadata", "name"),
        ("metadata", "namespace"),
        ("metadata", "labels", "strimzi.io/cluster"),
        ("metadata", "labels", "app.kubernetes.io/managed-by"),
        ("metadata", "annotations", "streamt.dev/manifest-checksum"),
        ("metadata", "annotations", "streamt.dev/owner-name"),
        ("metadata", "annotations", "streamt.dev/owner-type"),
        ("metadata", "annotations", "streamt.dev/ownership-mode"),
        ("metadata", "annotations", "streamt.dev/project"),
        ("metadata", "annotations", "streamt.dev/strimzi-release"),
        ("spec", "topicName"),
        ("spec", "partitions"),
        ("spec", "replicas"),
    ],
)
@pytest.mark.parametrize("hostile", [_HostileString("hostile"), _HostileValue()])
def test_hostile_document_values_are_translated_to_fixed_validation_errors(
    path: tuple[str, ...],
    hostile: object,
) -> None:
    document = _document()
    _set_path(document, path, hostile)
    with pytest.raises(StrimziValidationError) as captured:
        validate_kafkatopic_document(document)
    assert str(captured.value).startswith("Strimzi KafkaTopic validation failed at /")
    assert "private-hostile" not in repr(captured.value)
    assert captured.value.__cause__ is None


def test_hostile_config_keys_and_values_are_translated_without_comparison() -> None:
    for config in (
        {_HostileString("ordinary"): "value"},
        {"ordinary": _HostileString("value")},
        {"ordinary": _HostileValue()},
    ):
        document = _document()
        spec = document["spec"]
        assert isinstance(spec, dict)
        spec["config"] = config
        with pytest.raises(StrimziValidationError) as captured:
            validate_kafkatopic_document(document)
        assert str(captured.value).endswith("/spec/config")


@pytest.mark.parametrize(
    "path",
    [
        ("apiVersion",),
        ("kind",),
        ("metadata", "name"),
        ("spec", "group"),
        ("spec", "scope"),
        ("spec", "names", "kind"),
        ("spec", "names", "listKind"),
        ("spec", "names", "singular"),
        ("spec", "names", "plural"),
        ("spec", "names", "shortNames", 0),
        ("spec", "names", "categories", 0),
        ("spec", "conversion", "strategy"),
        ("spec", "versions", 0, "name"),
        (
            "spec",
            "versions",
            0,
            "schema",
            "openAPIV3Schema",
            "type",
        ),
        (
            "spec",
            "versions",
            0,
            "schema",
            "openAPIV3Schema",
            "required",
            0,
        ),
    ],
)
@pytest.mark.parametrize("hostile", [_HostileString("hostile"), _HostileValue()])
def test_hostile_crd_values_are_translated_to_fixed_resource_errors(
    path: tuple[str | int, ...],
    hostile: object,
) -> None:
    crd = _parsed_crd()
    _set_path(crd, path, hostile)
    with pytest.raises(StrimziResourceError) as captured:
        strimzi_validation._extract_kafkatopic_openapi_schema(crd)
    assert str(captured.value) == "The bundled Strimzi KafkaTopic schema is invalid"
    assert "private-hostile" not in repr(captured.value)
    assert captured.value.__cause__ is None


def test_document_runs_closed_validation_before_pinned_schema(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []

    def closed(_document: object) -> None:
        calls.append("closed")

    def pinned(_document: dict[str, object]) -> None:
        calls.append("pinned")

    monkeypatch.setattr(
        strimzi_validation,
        "_validate_closed_kafkatopic_document",
        closed,
    )
    monkeypatch.setattr(
        strimzi_validation,
        "_validate_pinned_kafkatopic_document",
        pinned,
    )
    validate_kafkatopic_document(_document())
    assert calls == ["closed", "pinned"]


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("partitions", 1),
        ("partitions", 2_147_483_647),
        ("replicas", 1),
        ("replicas", 32_767),
    ],
)
def test_closed_validator_accepts_exact_integer_bounds(field: str, value: int) -> None:
    document = _document()
    spec = document["spec"]
    assert isinstance(spec, dict)
    spec[field] = value
    validate_kafkatopic_document(document)


@pytest.mark.parametrize(
    ("field", "value", "location"),
    [
        ("partitions", 0, "/spec/partitions"),
        ("partitions", 2_147_483_648, "/spec/partitions"),
        ("partitions", True, "/spec/partitions"),
        ("replicas", 0, "/spec/replicas"),
        ("replicas", 32_768, "/spec/replicas"),
        ("replicas", False, "/spec/replicas"),
    ],
)
def test_closed_validator_rejects_invalid_integer_bounds_and_bools(
    field: str,
    value: object,
    location: str,
) -> None:
    document = _document()
    spec = document["spec"]
    assert isinstance(spec, dict)
    spec[field] = value
    with pytest.raises(StrimziValidationError) as captured:
        validate_kafkatopic_document(document)
    assert str(captured.value).endswith(location)


@pytest.mark.parametrize(
    ("part", "value"),
    [
        ("apiVersion", "kafka.strimzi.io/v1beta2"),
        ("kind", "Kafka"),
    ],
)
def test_closed_validator_rejects_wrong_resource_identity(part: str, value: str) -> None:
    document = _document()
    document[part] = value
    with pytest.raises(StrimziValidationError):
        validate_kafkatopic_document(document)


@pytest.mark.parametrize(
    "mapping_path",
    ["root", "metadata", "labels", "annotations", "spec"],
)
def test_closed_validator_rejects_extra_fields_at_every_document_level(
    mapping_path: str,
) -> None:
    document = _document()
    current: dict[str, object] = document
    if mapping_path == "metadata":
        current = document["metadata"]  # type: ignore[assignment]
    elif mapping_path in {"labels", "annotations"}:
        metadata = document["metadata"]
        assert isinstance(metadata, dict)
        current = metadata[mapping_path]  # type: ignore[assignment]
    elif mapping_path == "spec":
        current = document["spec"]  # type: ignore[assignment]
    assert isinstance(current, dict)
    current["unexpected"] = "private-structural-sentinel"
    with pytest.raises(StrimziValidationError) as captured:
        validate_kafkatopic_document(document)
    assert "private-structural-sentinel" not in str(captured.value)


def test_closed_validator_rejects_key_reordering() -> None:
    document = _document()
    reordered = {
        "kind": document["kind"],
        "apiVersion": document["apiVersion"],
        "metadata": document["metadata"],
        "spec": document["spec"],
    }
    with pytest.raises(StrimziValidationError):
        validate_kafkatopic_document(reordered)


def test_closed_validator_requires_exact_metadata_name_for_physical_topic() -> None:
    document = _document()
    metadata = document["metadata"]
    assert isinstance(metadata, dict)
    metadata["name"] = "other-name"
    with pytest.raises(StrimziValidationError) as captured:
        validate_kafkatopic_document(document)
    assert str(captured.value).endswith("/metadata/name")


@pytest.mark.parametrize(
    ("annotation", "value"),
    [
        ("streamt.dev/manifest-checksum", "sha256:" + "A" * 64),
        ("streamt.dev/owner-name", "bad\0owner"),
        ("streamt.dev/owner-name", "escaped\ufeffowner"),
        ("streamt.dev/owner-name", "noncharacter\ufffeowner"),
        ("streamt.dev/owner-name", "noncharacter\uffffowner"),
        ("streamt.dev/owner-name", "last\U0010ffffowner"),
        ("streamt.dev/owner-type", "connector"),
        ("streamt.dev/ownership-mode", "external"),
        ("streamt.dev/project", "bad\nproject"),
        ("streamt.dev/strimzi-release", "latest"),
    ],
)
def test_closed_validator_rejects_invalid_annotation_values(
    annotation: str,
    value: object,
) -> None:
    document = _document()
    metadata = document["metadata"]
    assert isinstance(metadata, dict)
    annotations = metadata["annotations"]
    assert isinstance(annotations, dict)
    annotations[annotation] = value
    with pytest.raises(StrimziValidationError) as captured:
        validate_kafkatopic_document(document)
    surface = f"{captured.value!s} {captured.value!r}"
    assert "connector" not in surface
    assert "latest" not in surface
    assert "bad" not in surface


def test_closed_validator_accepts_exact_annotation_limit_and_rejects_one_more_byte() -> None:
    document = _document()
    metadata = document["metadata"]
    assert isinstance(metadata, dict)
    annotations = metadata["annotations"]
    assert isinstance(annotations, dict)
    owner_key = "streamt.dev/owner-name"
    fixed_size = sum(
        len(str(key).encode("utf-8")) + len(str(value).encode("utf-8"))
        for key, value in annotations.items()
        if key != owner_key
    ) + len(owner_key.encode("utf-8"))
    annotations[owner_key] = "x" * ((256 * 1024) - fixed_size)
    validate_kafkatopic_document(document)

    annotations[owner_key] += "x"
    with pytest.raises(StrimziValidationError) as captured:
        validate_kafkatopic_document(document)
    assert str(captured.value).endswith("/metadata/annotations")


@pytest.mark.parametrize(
    "config",
    [
        [],
        {"z": "value", "a": "value"},
        {"api.token": "private-config-sentinel"},
        {"ordinary": True},
        {"ordinary": 1},
        {"ordinary": "bad\nvalue"},
        {"ordinary": "escaped\ufeffvalue"},
        {"ordinary": "noncharacter\ufffevalue"},
        {"ordinary": "noncharacter\uffffvalue"},
        {"ordinary": "last\U0010ffffvalue"},
        {"": "value"},
        {"caf\N{LATIN SMALL LETTER E WITH ACUTE}": "value"},
    ],
)
def test_closed_validator_rejects_malformed_or_non_normalized_config(
    config: object,
) -> None:
    document = _document()
    spec = document["spec"]
    assert isinstance(spec, dict)
    spec["config"] = config
    with pytest.raises(StrimziValidationError) as captured:
        validate_kafkatopic_document(document)
    surface = f"{captured.value!s} {captured.value!r}"
    assert "private-config-sentinel" not in surface
    assert "bad" not in surface


def test_structural_crd_validation_accepts_unknown_string_config_and_rejects_schema_type() -> None:
    document = _document()
    spec = document["spec"]
    assert isinstance(spec, dict)
    config = spec["config"]
    assert isinstance(config, dict)
    config["arbitrary.custom.setting"] = "text"
    # The explicitly handled preserve-unknown extension permits arbitrary
    # config properties during the separate structural evidence pass.
    strimzi_validation._validate_pinned_kafkatopic_document(document)

    spec["replicas"] = "three"
    with pytest.raises(StrimziValidationError) as captured:
        strimzi_validation._validate_pinned_kafkatopic_document(document)
    assert str(captured.value).endswith("/spec/replicas")


def test_document_collection_requires_tuple_sorting_and_unique_identities() -> None:
    documents = _documents()
    with pytest.raises(StrimziValidationError):
        validate_kafkatopic_documents(documents)
    with pytest.raises(StrimziValidationError):
        validate_kafkatopic_documents(tuple(reversed(documents)))
    with pytest.raises(StrimziValidationError):
        validate_kafkatopic_documents((documents[0], copy.deepcopy(documents[0])))


def test_failures_and_representations_never_render_rejected_values() -> None:
    sentinel = "private-invalid-value-sentinel"
    document = _document()
    spec = document["spec"]
    assert isinstance(spec, dict)
    spec["config"] = {"secret": sentinel}
    with pytest.raises(StrimziValidationError) as captured:
        validate_kafkatopic_document(document)
    surfaces = (str(captured.value), repr(captured.value))
    assert all(sentinel not in surface for surface in surfaces)
    assert captured.value.__cause__ is None


def test_loader_rejects_valid_yaml_with_changed_shape_even_when_digest_is_rebound(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    crd = _parsed_crd()
    spec = crd["spec"]
    assert isinstance(spec, dict)
    versions = spec["versions"]
    assert isinstance(versions, list)
    version = versions[0]
    assert isinstance(version, dict)
    version["name"] = "v1beta2"
    encoded, raw = _encoded_crd(crd)
    _replace_resource(monkeypatch, encoded)
    monkeypatch.setattr(strimzi_validation, "_CRD_SIZE", len(raw))
    monkeypatch.setattr(
        strimzi_validation,
        "_CRD_SHA256",
        hashlib.sha256(raw).hexdigest(),
    )
    with pytest.raises(StrimziResourceError):
        strimzi_validation._kafkatopic_openapi_schema()
