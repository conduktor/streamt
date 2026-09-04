"""Closed and integrity-pinned validation for Strimzi KafkaTopic exports.

This module is deliberately offline.  The bundled CRD provides structural
evidence only; the stricter streamt document contract is validated first and
does not depend on Kubernetes client behavior or API-server admission.
"""

from __future__ import annotations

import base64
import gzip
import hashlib
import json
import re
import unicodedata
from collections.abc import Iterable
from functools import lru_cache
from importlib.resources import files

import yaml
from jsonschema import Draft7Validator
from jsonschema.exceptions import SchemaError, ValidationError

from streamt.compiler.topic_artifact import (
    TopicArtifactFormatError,
    kafka_topic_metadata_name,
    validate_dns1123_label,
    validate_kafka_topic_name,
)

STRIMZI_RELEASE = "1.2.0"
STRIMZI_API_VERSION = "kafka.strimzi.io/v1"
STRIMZI_KIND = "KafkaTopic"

_SCHEMA_PACKAGE = "streamt.integrations.gitops.schemas"
_CRD_RESOURCE = "strimzi-1.2.0-kafkatopic-crd.yaml.gz.b64"
_CRD_SIZE = 6_329
_CRD_SHA256 = "36390f0731c699448076d4ee739e8b7f331d083e91a7fb71500aaa830ab1127e"
_ANNOTATION_SIZE_LIMIT = 256 * 1024
_CHECKSUM = re.compile(r"sha256:[0-9a-f]{64}\Z", re.ASCII)
_LABEL_VALUE = re.compile(
    r"(?:[A-Za-z0-9](?:[A-Za-z0-9._-]{0,61}[A-Za-z0-9])?)?\Z",
    re.ASCII,
)
_SENSITIVE_CONFIG_KEY = re.compile(
    r"(^|[._-])(?:password|passwd|secret|token|api[_-]?key|authorization|"
    r"credentials?|basic[._-]auth[._-]user[._-]info|"
    r"sasl[._-]jaas[._-]config)($|[._-])",
    re.IGNORECASE | re.ASCII,
)

_ROOT_KEYS = ("apiVersion", "kind", "metadata", "spec")
_METADATA_KEYS = ("name", "namespace", "labels", "annotations")
_LABEL_KEYS = ("strimzi.io/cluster", "app.kubernetes.io/managed-by")
_ANNOTATION_KEYS = (
    "streamt.dev/manifest-checksum",
    "streamt.dev/owner-name",
    "streamt.dev/owner-type",
    "streamt.dev/ownership-mode",
    "streamt.dev/project",
    "streamt.dev/strimzi-release",
)
_SPEC_KEYS = ("topicName", "partitions", "replicas", "config")
_PRESERVE_UNKNOWN_EXTENSION_PATH = (
    "properties",
    "spec",
    "properties",
    "config",
    "x-kubernetes-preserve-unknown-fields",
)


class StrimziResourceError(ValueError):
    """The integrity-pinned Strimzi resource cannot be used safely."""


class StrimziValidationError(ValueError):
    """A generated KafkaTopic document violates the frozen export contract."""


def _resource_failure() -> StrimziResourceError:
    return StrimziResourceError("The bundled Strimzi KafkaTopic schema is invalid")


def _validation_failure(location: str) -> StrimziValidationError:
    return StrimziValidationError(f"Strimzi KafkaTopic validation failed at {location}")


def _plain_dict(value: object, location: str) -> dict[str, object]:
    if type(value) is not dict:
        raise _validation_failure(location)
    assert isinstance(value, dict)
    if any(type(key) is not str for key in value):
        raise _validation_failure(location)
    return value


def _exact_mapping(
    value: object,
    *,
    keys: tuple[str, ...],
    location: str,
) -> dict[str, object]:
    parsed = _plain_dict(value, location)
    if tuple(parsed) != keys:
        raise _validation_failure(location)
    return parsed


def _exact_string(value: object, expected: str, location: str) -> str:
    if type(value) is not str or value != expected:
        raise _validation_failure(location)
    assert isinstance(value, str)
    return value


def _safe_annotation_value(value: object) -> bool:
    return (
        type(value) is str
        and bool(value)
        and all(unicodedata.category(char) not in {"Cc", "Cs"} for char in value)
    )


def _valid_config_key(value: object) -> bool:
    return (
        type(value) is str
        and bool(value)
        and value.isascii()
        and all(unicodedata.category(char) != "Cc" for char in value)
        and _SENSITIVE_CONFIG_KEY.search(value) is None
    )


def validate_kubernetes_namespace(value: object) -> str:
    """Validate the export contract's exact Kubernetes namespace boundary."""
    try:
        return validate_dns1123_label(value)
    except TopicArtifactFormatError:
        raise _validation_failure("/metadata/namespace") from None


def validate_kubernetes_label_value(value: object) -> str:
    """Validate the Kubernetes label-value syntax used by emitted documents."""
    if type(value) is not str or _LABEL_VALUE.fullmatch(value) is None:
        raise _validation_failure("/metadata/labels")
    assert isinstance(value, str)
    return value


def _bounded_integer(value: object, *, maximum: int, location: str) -> int:
    if type(value) is not int or value < 1 or value > maximum:
        raise _validation_failure(location)
    return value


def _validate_config(value: object) -> None:
    config = _plain_dict(value, "/spec/config")
    if tuple(config) != tuple(sorted(config)):
        raise _validation_failure("/spec/config")
    for key, item in config.items():
        if not _valid_config_key(key):
            raise _validation_failure("/spec/config")
        if type(item) is not str or any(
            unicodedata.category(char) in {"Cc", "Cs"} for char in item
        ):
            raise _validation_failure("/spec/config")


def _validate_closed_kafkatopic_document(document: object) -> None:
    root = _exact_mapping(document, keys=_ROOT_KEYS, location="/")
    _exact_string(root["apiVersion"], STRIMZI_API_VERSION, "/apiVersion")
    _exact_string(root["kind"], STRIMZI_KIND, "/kind")

    metadata = _exact_mapping(
        root["metadata"],
        keys=_METADATA_KEYS,
        location="/metadata",
    )
    namespace = validate_kubernetes_namespace(metadata["namespace"])
    if metadata["namespace"] != namespace:
        raise _validation_failure("/metadata/namespace")

    labels = _exact_mapping(
        metadata["labels"],
        keys=_LABEL_KEYS,
        location="/metadata/labels",
    )
    cluster_name = validate_kubernetes_label_value(labels["strimzi.io/cluster"])
    try:
        validate_dns1123_label(cluster_name)
    except TopicArtifactFormatError:
        raise _validation_failure("/metadata/labels/strimzi.io~1cluster") from None
    _exact_string(
        labels["app.kubernetes.io/managed-by"],
        "streamt",
        "/metadata/labels/app.kubernetes.io~1managed-by",
    )

    annotations = _exact_mapping(
        metadata["annotations"],
        keys=_ANNOTATION_KEYS,
        location="/metadata/annotations",
    )
    if not all(type(value) is str for value in annotations.values()):
        raise _validation_failure("/metadata/annotations")
    checksum = annotations["streamt.dev/manifest-checksum"]
    if type(checksum) is not str or _CHECKSUM.fullmatch(checksum) is None:
        raise _validation_failure("/metadata/annotations/streamt.dev~1manifest-checksum")
    if not _safe_annotation_value(annotations["streamt.dev/owner-name"]):
        raise _validation_failure("/metadata/annotations/streamt.dev~1owner-name")
    if annotations["streamt.dev/owner-type"] not in {"model", "source"}:
        raise _validation_failure("/metadata/annotations/streamt.dev~1owner-type")
    if annotations["streamt.dev/ownership-mode"] != "managed":
        raise _validation_failure("/metadata/annotations/streamt.dev~1ownership-mode")
    if not _safe_annotation_value(annotations["streamt.dev/project"]):
        raise _validation_failure("/metadata/annotations/streamt.dev~1project")
    if annotations["streamt.dev/strimzi-release"] != STRIMZI_RELEASE:
        raise _validation_failure("/metadata/annotations/streamt.dev~1strimzi-release")
    annotation_size = sum(
        len(key.encode("utf-8")) + len(value.encode("utf-8"))
        for key, value in annotations.items()
        if isinstance(value, str)
    )
    if annotation_size > _ANNOTATION_SIZE_LIMIT:
        raise _validation_failure("/metadata/annotations")

    spec = _exact_mapping(root["spec"], keys=_SPEC_KEYS, location="/spec")
    try:
        topic_name = validate_kafka_topic_name(spec["topicName"])
        expected_metadata_name = kafka_topic_metadata_name(topic_name)
    except TopicArtifactFormatError:
        raise _validation_failure("/spec/topicName") from None
    if type(metadata["name"]) is not str or metadata["name"] != expected_metadata_name:
        raise _validation_failure("/metadata/name")
    _bounded_integer(
        spec["partitions"],
        maximum=2_147_483_647,
        location="/spec/partitions",
    )
    _bounded_integer(spec["replicas"], maximum=32_767, location="/spec/replicas")
    _validate_config(spec["config"])


def _resource_dict(value: object) -> dict[str, object]:
    if type(value) is not dict:
        raise _resource_failure()
    assert isinstance(value, dict)
    if any(type(key) is not str for key in value):
        raise _resource_failure()
    return value


def _resource_list(value: object) -> list[object]:
    if type(value) is not list:
        raise _resource_failure()
    assert isinstance(value, list)
    return value


def _resource_string(value: object, expected: str) -> str:
    if type(value) is not str or value != expected:
        raise _resource_failure()
    assert isinstance(value, str)
    return value


def _resource_string_list(value: object, expected: tuple[str, ...]) -> list[object]:
    parsed = _resource_list(value)
    if len(parsed) != len(expected):
        raise _resource_failure()
    for item, expected_item in zip(parsed, expected, strict=True):
        _resource_string(item, expected_item)
    return parsed


def _require_resource_keys(
    value: object,
    expected: frozenset[str],
) -> dict[str, object]:
    parsed = _resource_dict(value)
    if set(parsed) != expected:
        raise _resource_failure()
    return parsed


def _extract_kafkatopic_openapi_schema(crd: object) -> dict[str, object]:
    """Select the sole served/storage v1 schema from the exact CRD shape."""
    root = _require_resource_keys(
        crd,
        frozenset({"apiVersion", "kind", "metadata", "spec"}),
    )
    _resource_string(root["apiVersion"], "apiextensions.k8s.io/v1")
    _resource_string(root["kind"], "CustomResourceDefinition")
    metadata = _require_resource_keys(root["metadata"], frozenset({"name", "labels"}))
    _resource_string(metadata["name"], "kafkatopics.kafka.strimzi.io")
    _resource_dict(metadata["labels"])

    spec = _require_resource_keys(
        root["spec"],
        frozenset({"group", "names", "scope", "conversion", "versions"}),
    )
    _resource_string(spec["group"], "kafka.strimzi.io")
    _resource_string(spec["scope"], "Namespaced")
    names = _require_resource_keys(
        spec["names"],
        frozenset({"kind", "listKind", "singular", "plural", "shortNames", "categories"}),
    )
    _resource_string(names["kind"], STRIMZI_KIND)
    _resource_string(names["listKind"], "KafkaTopicList")
    _resource_string(names["singular"], "kafkatopic")
    _resource_string(names["plural"], "kafkatopics")
    _resource_string_list(names["shortNames"], ("kt",))
    _resource_string_list(names["categories"], ("strimzi",))
    conversion = _require_resource_keys(
        spec["conversion"],
        frozenset({"strategy"}),
    )
    _resource_string(conversion["strategy"], "None")

    versions = _resource_list(spec["versions"])
    if len(versions) != 1:
        raise _resource_failure()
    version = _require_resource_keys(
        versions[0],
        frozenset(
            {
                "name",
                "served",
                "storage",
                "subresources",
                "additionalPrinterColumns",
                "schema",
            }
        ),
    )
    _resource_string(version["name"], "v1")
    if version["served"] is not True or version["storage"] is not True:
        raise _resource_failure()
    subresources = _require_resource_keys(
        version["subresources"],
        frozenset({"status"}),
    )
    if _resource_dict(subresources["status"]):
        raise _resource_failure()
    _resource_list(version["additionalPrinterColumns"])
    schema_wrapper = _require_resource_keys(
        version["schema"],
        frozenset({"openAPIV3Schema"}),
    )
    schema = _resource_dict(schema_wrapper["openAPIV3Schema"])
    if set(schema) != {"type", "properties", "required"}:
        raise _resource_failure()
    _resource_string(schema["type"], "object")
    _resource_string_list(schema["required"], ("spec",))
    properties = _resource_dict(schema["properties"])
    if set(properties) != {"apiVersion", "kind", "metadata", "spec", "status"}:
        raise _resource_failure()
    return schema


def _strip_kubernetes_extensions(
    value: object,
    *,
    path: tuple[str, ...] = (),
) -> tuple[object, tuple[tuple[str, ...], ...]]:
    """Remove only the one pinned extension understood by this validator.

    Draft 7 ignores unknown keywords.  Removing the extension explicitly keeps
    that behavior reviewable: preserve-unknown on ``spec.config`` maps to Draft
    7's default allowance of object properties not otherwise enumerated.
    """
    if type(value) is dict:
        assert isinstance(value, dict)
        result: dict[str, object] = {}
        found: list[tuple[str, ...]] = []
        for key, item in value.items():
            if type(key) is not str:
                raise _resource_failure()
            item_path = (*path, key)
            if key.startswith("x-kubernetes-"):
                if item_path != _PRESERVE_UNKNOWN_EXTENSION_PATH or item is not True:
                    raise _resource_failure()
                found.append(item_path)
                continue
            normalized, child_found = _strip_kubernetes_extensions(
                item,
                path=item_path,
            )
            result[key] = normalized
            found.extend(child_found)
        return result, tuple(found)
    if type(value) is list:
        assert isinstance(value, list)
        result_items: list[object] = []
        list_extensions: list[tuple[str, ...]] = []
        for index, item in enumerate(value):
            normalized, child_found = _strip_kubernetes_extensions(
                item,
                path=(*path, str(index)),
            )
            result_items.append(normalized)
            list_extensions.extend(child_found)
        return result_items, tuple(list_extensions)
    return value, ()


@lru_cache(maxsize=1)
def _pinned_kafkatopic_crd_bytes() -> bytes:
    try:
        encoded = b"".join(files(_SCHEMA_PACKAGE).joinpath(_CRD_RESOURCE).read_bytes().split())
        compressed = base64.b64decode(encoded, validate=True)
        raw = gzip.decompress(compressed)
    except (EOFError, ModuleNotFoundError, OSError, ValueError):
        raise _resource_failure() from None
    if len(raw) != _CRD_SIZE or hashlib.sha256(raw).hexdigest() != _CRD_SHA256:
        raise _resource_failure()
    return raw


def _pinned_kafkatopic_crd() -> dict[str, object]:
    raw = _pinned_kafkatopic_crd_bytes()
    try:
        candidate: object = yaml.safe_load(raw)
    except yaml.YAMLError:
        raise _resource_failure() from None
    return _resource_dict(candidate)


def _prepare_kafkatopic_openapi_schema_bytes() -> bytes:
    """Build one checked schema and freeze it as immutable canonical bytes."""
    schema = _extract_kafkatopic_openapi_schema(_pinned_kafkatopic_crd())
    normalized, extensions = _strip_kubernetes_extensions(schema)
    if extensions != (_PRESERVE_UNKNOWN_EXTENSION_PATH,):
        raise _resource_failure()
    normalized_schema = _resource_dict(normalized)
    try:
        Draft7Validator.check_schema(normalized_schema)
    except SchemaError:
        raise _resource_failure() from None
    try:
        return json.dumps(
            normalized_schema,
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    except (TypeError, ValueError):
        raise _resource_failure() from None


@lru_cache(maxsize=1)
def _cached_kafkatopic_openapi_schema_bytes() -> bytes:
    return _prepare_kafkatopic_openapi_schema_bytes()


def _kafkatopic_openapi_schema() -> dict[str, object]:
    """Return a fresh schema copy backed by immutable checked cache bytes."""
    try:
        candidate: object = json.loads(_cached_kafkatopic_openapi_schema_bytes())
    except (json.JSONDecodeError, UnicodeDecodeError):
        raise _resource_failure() from None
    return _resource_dict(candidate)


def _kafkatopic_validator() -> Draft7Validator:
    return Draft7Validator(_kafkatopic_openapi_schema())


def _safe_pointer(path: Iterable[object]) -> str:
    parts = [str(part).replace("~", "~0").replace("/", "~1") for part in path]
    return "/" + "/".join(parts) if parts else "/"


def _validate_pinned_kafkatopic_document(
    document: dict[str, object],
    *,
    validator: Draft7Validator | None = None,
) -> None:
    local_validator = validator if validator is not None else _kafkatopic_validator()
    try:
        errors = sorted(
            local_validator.iter_errors(document),
            key=lambda error: (
                tuple(str(part) for part in error.absolute_path),
                str(error.validator),
            ),
        )
    except Exception:
        raise _resource_failure() from None
    if errors:
        error: ValidationError = errors[0]
        raise _validation_failure(_safe_pointer(error.absolute_path)) from None


def validate_kafkatopic_document(document: object) -> None:
    """Validate one document first against closed invariants, then the CRD."""
    _validate_closed_kafkatopic_document(document)
    assert isinstance(document, dict)
    _validate_pinned_kafkatopic_document(document)


def validate_kafkatopic_documents(documents: object) -> None:
    """Validate an immutable, sorted document tuple without mutating it."""
    if type(documents) is not tuple:
        raise _validation_failure("/documents")
    assert isinstance(documents, tuple)
    identities: list[tuple[str, str]] = []
    kafka_names: set[str] = set()
    kubernetes_names: set[str] = set()
    validator: Draft7Validator | None = None
    for index, document in enumerate(documents):
        try:
            _validate_closed_kafkatopic_document(document)
            assert isinstance(document, dict)
            if validator is None:
                validator = _kafkatopic_validator()
            _validate_pinned_kafkatopic_document(document, validator=validator)
        except StrimziValidationError:
            raise _validation_failure(f"/documents/{index}") from None
        assert isinstance(document, dict)
        metadata = document["metadata"]
        spec = document["spec"]
        assert isinstance(metadata, dict)
        assert isinstance(spec, dict)
        topic_name = spec["topicName"]
        metadata_name = metadata["name"]
        assert isinstance(topic_name, str)
        assert isinstance(metadata_name, str)
        if topic_name in kafka_names or metadata_name in kubernetes_names:
            raise _validation_failure(f"/documents/{index}")
        kafka_names.add(topic_name)
        kubernetes_names.add(metadata_name)
        identities.append((topic_name, metadata_name))
    if identities != sorted(identities):
        raise _validation_failure("/documents")


def _clear_schema_caches() -> None:
    """Clear resource caches for isolated integrity tests."""
    _cached_kafkatopic_openapi_schema_bytes.cache_clear()
    _pinned_kafkatopic_crd_bytes.cache_clear()


__all__ = [
    "STRIMZI_API_VERSION",
    "STRIMZI_KIND",
    "STRIMZI_RELEASE",
    "StrimziResourceError",
    "StrimziValidationError",
    "validate_kafkatopic_document",
    "validate_kafkatopic_documents",
    "validate_kubernetes_label_value",
    "validate_kubernetes_namespace",
]
