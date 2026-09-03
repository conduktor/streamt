"""Integrity-pinned, offline Backstage core-entity validation."""

from __future__ import annotations

import base64
import gzip
import hashlib
import json
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from functools import lru_cache
from importlib.resources import files
from typing import Literal, cast

from jsonschema import Draft7Validator
from jsonschema.exceptions import SchemaError, ValidationError
from referencing import Registry, Resource
from referencing.jsonschema import DRAFT7

BACKSTAGE_RELEASE = "1.54.2"
BACKSTAGE_CATALOG_MODEL_VERSION = "1.10.0"

BackstageKind = Literal["System", "Resource", "Component"]


class BackstageResourceError(ValueError):
    """Raised when an integrity-pinned Backstage schema cannot be loaded."""


class BackstageValidationError(ValueError):
    """Raised when an entity fails the supported Backstage schema contract."""


@dataclass(frozen=True)
class _SchemaArtifact:
    resource: str
    schema_id: str
    size: int
    sha256: str


_SCHEMA_ARTIFACTS = (
    _SchemaArtifact(
        resource="backstage-1.54.2-entity.json.gz.b64",
        schema_id="Entity",
        size=1_804,
        sha256="b4e741f821e6006b179f0112ae98a8856286f95593e7dc16fb6fdd8aaaba51fc",
    ),
    _SchemaArtifact(
        resource="backstage-1.54.2-entity-envelope.json.gz.b64",
        schema_id="EntityEnvelope",
        size=1_775,
        sha256="855ec6c05e3f4d328752349c9bd4888436744db716e8713ff3f58399419b7539",
    ),
    _SchemaArtifact(
        resource="backstage-1.54.2-entity-meta.json.gz.b64",
        schema_id="EntityMeta",
        size=4_642,
        sha256="d19d6329386fcf5016687b7cb07f71bc9e51d7ad1b54f616c29aefa5ac978616",
    ),
    _SchemaArtifact(
        resource="backstage-1.54.2-common.json.gz.b64",
        schema_id="common",
        size=3_644,
        sha256="7b99ac97e6ae64795836ef4291f859701291bc1880578e6ee7502ec19cc15665",
    ),
    _SchemaArtifact(
        resource="backstage-1.54.2-system-v1alpha1.json.gz.b64",
        schema_id="SystemV1alpha1",
        size=2_148,
        sha256="9ad7bf11e4db9d1b9b6d4c976ffa5393cd6c02d36cb1b23f1efc3a9c4b072afe",
    ),
    _SchemaArtifact(
        resource="backstage-1.54.2-resource-v1alpha1.json.gz.b64",
        schema_id="ResourceV1alpha1",
        size=2_114,
        sha256="1cea27c74540638d8b44cad0b222c93a887e5a4609f28d6e9bb882ab43d7859e",
    ),
    _SchemaArtifact(
        resource="backstage-1.54.2-component-v1alpha1.json.gz.b64",
        schema_id="ComponentV1alpha1",
        size=3_333,
        sha256="9ff892085ddaf78243ead1cdcf24f03eb5f9af16dffbed189fae4ab0806a8d56",
    ),
)

_KIND_SCHEMA_IDS: dict[BackstageKind, str] = {
    "System": "SystemV1alpha1",
    "Resource": "ResourceV1alpha1",
    "Component": "ComponentV1alpha1",
}
_DRAFT7_ID = "http://json-schema.org/draft-07/schema"


@lru_cache(maxsize=1)
def _official_schemas() -> dict[str, dict[str, object]]:
    """Load all official schemas after checking their exact upstream bytes."""
    loaded: dict[str, dict[str, object]] = {}
    for artifact in _SCHEMA_ARTIFACTS:
        try:
            encoded = b"".join(
                files("streamt.docs.schemas")
                .joinpath(artifact.resource)
                .read_bytes()
                .split()
            )
            raw_schema = gzip.decompress(base64.b64decode(encoded, validate=True))
        except (EOFError, OSError, ValueError) as exc:
            raise BackstageResourceError(
                "A bundled Backstage schema cannot be decoded"
            ) from exc

        if len(raw_schema) != artifact.size:
            raise BackstageResourceError(
                "A bundled Backstage schema does not match its pinned size"
            )
        if hashlib.sha256(raw_schema).hexdigest() != artifact.sha256:
            raise BackstageResourceError(
                "A bundled Backstage schema does not match its pinned checksum"
            )

        try:
            candidate: object = json.loads(raw_schema)
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            raise BackstageResourceError(
                "A bundled Backstage schema is not valid JSON"
            ) from exc
        if (
            not isinstance(candidate, dict)
            or candidate.get("$id") != artifact.schema_id
            or candidate.get("$schema") != _DRAFT7_ID
        ):
            raise BackstageResourceError(
                "A bundled Backstage schema has an unexpected identity"
            )
        try:
            Draft7Validator.check_schema(candidate)
        except SchemaError as exc:
            raise BackstageResourceError(
                "A bundled Backstage schema is not a valid Draft 7 schema"
            ) from exc
        loaded[artifact.schema_id] = candidate
    return loaded


@lru_cache(maxsize=1)
def _schema_registry() -> Registry:
    """Build a closed registry keyed by Backstage's exact bare schema IDs."""
    resources = (
        (
            schema_id,
            Resource.from_contents(schema, default_specification=DRAFT7),
        )
        for schema_id, schema in _official_schemas().items()
    )
    return Registry().with_resources(resources)


@lru_cache(maxsize=3)
def _kind_validator(kind: BackstageKind) -> Draft7Validator:
    """Return the pinned official validator for one supported core kind."""
    schema = _official_schemas()[_KIND_SCHEMA_IDS[kind]]
    return Draft7Validator(schema, registry=_schema_registry())


@lru_cache(maxsize=1)
def _envelope_validator() -> Draft7Validator:
    """Return the permissive official envelope validator used before kind validation."""
    return Draft7Validator(
        _official_schemas()["EntityEnvelope"],
        registry=_schema_registry(),
    )


def _sorted_schema_errors(errors: Iterable[ValidationError]) -> list[ValidationError]:
    return sorted(
        errors,
        key=lambda error: (
            tuple(str(part) for part in error.absolute_path),
            str(error.validator),
        ),
    )


def _safe_json_pointer(path: Iterable[object]) -> str:
    parts = [str(part).replace("~", "~0").replace("/", "~1") for part in path]
    return "/" + "/".join(parts) if parts else "/"


def validate_backstage_entity(entity: Mapping[str, object]) -> None:
    """Validate one System, Resource, or Component using only bundled schemas."""
    if not isinstance(entity, Mapping):
        raise BackstageValidationError("Backstage entity must be an object")

    kind_value = entity.get("kind")
    if not isinstance(kind_value, str) or kind_value not in _KIND_SCHEMA_IDS:
        raise BackstageValidationError("Backstage entity kind is unsupported")
    kind = cast(BackstageKind, kind_value)

    envelope_errors = _sorted_schema_errors(_envelope_validator().iter_errors(entity))
    if envelope_errors:
        location = _safe_json_pointer(envelope_errors[0].absolute_path)
        raise BackstageValidationError(
            f"Backstage entity envelope validation failed at {location}"
        )

    errors = _sorted_schema_errors(_kind_validator(kind).iter_errors(entity))
    if errors:
        location = _safe_json_pointer(errors[0].absolute_path)
        raise BackstageValidationError(
            f"Backstage {kind} schema validation failed at {location}"
        )


def _clear_schema_caches() -> None:
    """Clear loader and validator caches for isolated integrity tests."""
    _kind_validator.cache_clear()
    _envelope_validator.cache_clear()
    _schema_registry.cache_clear()
    _official_schemas.cache_clear()
