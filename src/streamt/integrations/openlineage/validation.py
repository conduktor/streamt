"""Offline OpenLineage schema and streamt semantic validation."""

from __future__ import annotations

import base64
import gzip
import hashlib
import json
import re
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from functools import cache, lru_cache
from importlib.resources import files
from typing import Literal
from urllib.parse import urlsplit
from uuid import UUID

from jsonschema import Draft202012Validator, FormatChecker
from jsonschema.exceptions import ValidationError
from referencing import Registry, Resource
from referencing.jsonschema import DRAFT202012

from streamt.integrations.openlineage.events import (
    EVENT_SCHEMA_URLS,
    FACET_SCHEMA_URLS,
    OPENLINEAGE_CORE_SCHEMA_ID,
    OPENLINEAGE_PRODUCER,
    FacetScope,
    OpenLineageConstructionError,
    RunIdentity,
    validate_kafka_namespace,
)


class OpenLineageResourceError(ValueError):
    """Raised when an integrity-pinned bundled schema cannot be loaded."""


class OpenLineageValidationError(ValueError):
    """Raised when an event or event sequence is not valid OpenLineage output."""


@dataclass(frozen=True)
class _SchemaArtifact:
    resource: str
    schema_id: str
    size: int
    sha256: str


_SCHEMA_ARTIFACTS = (
    _SchemaArtifact(
        resource="openlineage-1.53.0-core.json.gz.b64",
        schema_id=OPENLINEAGE_CORE_SCHEMA_ID,
        size=9_155,
        sha256="69f68bee00b9beac88a87059c0102410e7bb05f3f43c46d02a0409831eceb0d2",
    ),
    _SchemaArtifact(
        resource="openlineage-1.53.0-job-type-job-facet.json.gz.b64",
        schema_id="https://openlineage.io/spec/facets/2-0-4/JobTypeJobFacet.json",
        size=3_072,
        sha256="11c12cab95a411ca31066c80d2bb4aefd37bbcadbda5e4d343d2069853b907d5",
    ),
    _SchemaArtifact(
        resource="openlineage-1.53.0-schema-dataset-facet.json.gz.b64",
        schema_id="https://openlineage.io/spec/facets/1-2-0/SchemaDatasetFacet.json",
        size=1_687,
        sha256="50236a779aa64baa0bad0055391838bd22fcb36ce667c41d60ada80915e899b6",
    ),
    _SchemaArtifact(
        resource="openlineage-1.53.0-documentation-dataset-facet.json.gz.b64",
        schema_id="https://openlineage.io/spec/facets/1-1-0/DocumentationDatasetFacet.json",
        size=1_031,
        sha256="bad5c041d679e73b2faea43506860428f48d80710ad1354a2d2393475143285f",
    ),
    _SchemaArtifact(
        resource="openlineage-1.53.0-documentation-job-facet.json.gz.b64",
        schema_id="https://openlineage.io/spec/facets/1-1-0/DocumentationJobFacet.json",
        size=944,
        sha256="b5823685c20c712d9ee1e3b310ad6ca426be7b46db60acd9f23248811af3c8d4",
    ),
    _SchemaArtifact(
        resource="openlineage-1.53.0-dataset-type-dataset-facet.json.gz.b64",
        schema_id="https://openlineage.io/spec/facets/1-0-1/DatasetTypeDatasetFacet.json",
        size=1_008,
        sha256="1a7d5106877151d52c4d3967f77b92788ea42ebf9843c6573d776a63c9a7d157",
    ),
    _SchemaArtifact(
        resource="openlineage-1.53.0-ownership-dataset-facet.json.gz.b64",
        schema_id="https://openlineage.io/spec/facets/1-0-1/OwnershipDatasetFacet.json",
        size=1_368,
        sha256="9a18a508746627eff18fa84ca1694e1a9f2d556ac0052dbdb6970d8a35f75231",
    ),
    _SchemaArtifact(
        resource="openlineage-1.53.0-ownership-job-facet.json.gz.b64",
        schema_id="https://openlineage.io/spec/facets/1-0-1/OwnershipJobFacet.json",
        size=1_344,
        sha256="c54ae771b1183efbe007a778eb5bf05e9e3f39f3d48f22a4beceea00950abdad",
    ),
    _SchemaArtifact(
        resource="openlineage-1.53.0-error-message-run-facet.json.gz.b64",
        schema_id="https://openlineage.io/spec/facets/1-0-1/ErrorMessageRunFacet.json",
        size=1_501,
        sha256="b11b4ee8b0f99f6846264f87cad48390255c7ef142ce623216660c7097be0ea6",
    ),
)

_COMMON_FIELDS = frozenset({"eventTime", "producer", "schemaURL"})
_ALLOWED_FIELDS = {
    "dataset": _COMMON_FIELDS | {"dataset"},
    "job": _COMMON_FIELDS | {"job", "inputs", "outputs"},
    "run": _COMMON_FIELDS | {"eventType", "run", "job", "inputs", "outputs"},
}
_TERMINAL_EVENT_TYPES = frozenset({"COMPLETE", "FAIL", "ABORT"})
_RFC3339_RE = re.compile(
    r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})$"
)


@lru_cache(maxsize=1)
def _official_schemas() -> dict[str, dict[str, object]]:
    """Load every vendored schema only after exact size and digest checks."""
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
            raise OpenLineageResourceError(
                "A bundled OpenLineage schema cannot be decoded"
            ) from exc
        if len(raw_schema) != artifact.size:
            raise OpenLineageResourceError(
                "A bundled OpenLineage schema does not match its pinned size"
            )
        if hashlib.sha256(raw_schema).hexdigest() != artifact.sha256:
            raise OpenLineageResourceError(
                "A bundled OpenLineage schema does not match its pinned checksum"
            )
        try:
            candidate: object = json.loads(raw_schema)
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            raise OpenLineageResourceError(
                "A bundled OpenLineage schema is not valid JSON"
            ) from exc
        if not isinstance(candidate, dict) or candidate.get("$id") != artifact.schema_id:
            raise OpenLineageResourceError(
                "A bundled OpenLineage schema has an unexpected identity"
            )
        try:
            Draft202012Validator.check_schema(candidate)
        except Exception as exc:
            raise OpenLineageResourceError(
                "A bundled OpenLineage schema is not a valid Draft 2020-12 schema"
            ) from exc
        loaded[artifact.schema_id] = candidate
    return loaded


@lru_cache(maxsize=1)
def _schema_registry() -> Registry:
    """Build a closed registry; the referencing default rejects missing resources."""
    resources = (
        (
            schema_id,
            Resource.from_contents(schema, default_specification=DRAFT202012),
        )
        for schema_id, schema in _official_schemas().items()
    )
    return Registry().with_resources(resources)


@lru_cache(maxsize=1)
def _format_checker() -> FormatChecker:
    """Return a checker with required formats enforced without optional extras."""
    checker = FormatChecker()

    @checker.checks("date-time", raises=(OverflowError, ValueError))
    def is_datetime(value: object) -> bool:
        if not isinstance(value, str):
            return True
        if _RFC3339_RE.fullmatch(value) is None:
            return False
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return parsed.tzinfo is not None and parsed.utcoffset() is not None

    @checker.checks("uri", raises=ValueError)
    def is_uri(value: object) -> bool:
        if not isinstance(value, str):
            return True
        parsed = urlsplit(value)
        return bool(parsed.scheme and parsed.netloc)

    @checker.checks("uuid", raises=(AttributeError, TypeError, ValueError))
    def is_uuid(value: object) -> bool:
        if not isinstance(value, str):
            return True
        UUID(value)
        return True

    return checker


@lru_cache(maxsize=1)
def _event_validator() -> Draft202012Validator:
    """Return the official root validator used for schema-contract tests."""
    return Draft202012Validator(
        _official_schemas()[OPENLINEAGE_CORE_SCHEMA_ID],
        registry=_schema_registry(),
        format_checker=_format_checker(),
    )


@cache
def _event_kind_validator(
    kind: Literal["dataset", "job", "run"],
) -> Draft202012Validator:
    """Resolve one event kind directly so errors retain useful safe paths."""
    schema: dict[str, object] = {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$ref": EVENT_SCHEMA_URLS[kind],
    }
    return Draft202012Validator(
        schema,
        registry=_schema_registry(),
        format_checker=_format_checker(),
    )


@cache
def _facet_validator(scope: FacetScope, key: str) -> Draft202012Validator:
    schema: dict[str, object] = {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$ref": FACET_SCHEMA_URLS[scope][key],
    }
    return Draft202012Validator(
        schema,
        registry=_schema_registry(),
        format_checker=_format_checker(),
    )


def validate_standard_facet(
    scope: FacetScope,
    key: str,
    facet: Mapping[str, object],
) -> None:
    """Validate one supported facet against its official and semantic contracts."""
    facet_urls = FACET_SCHEMA_URLS.get(scope)
    expected_url = facet_urls.get(key) if facet_urls is not None else None
    if expected_url is None:
        raise OpenLineageValidationError(
            "Facet key is not supported for the requested OpenLineage entity"
        )
    if facet.get("_producer") != OPENLINEAGE_PRODUCER:
        raise OpenLineageValidationError("Facet producer does not match the streamt producer")
    if facet.get("_schemaURL") != expected_url:
        raise OpenLineageValidationError(
            "Facet schema URL does not match its key and entity"
        )
    _validate_facet_allowed_fields(key, facet)
    try:
        errors = _sorted_schema_errors(
            _facet_validator(scope, key).iter_errors(dict(facet))
        )
    except Exception as exc:
        raise OpenLineageValidationError(
            "Official OpenLineage facet validation could not be completed offline"
        ) from exc
    if errors:
        error = errors[0]
        raise OpenLineageValidationError(
            "Official OpenLineage facet schema validation failed at "
            f"{_safe_json_pointer(error.absolute_path)} ({error.validator})"
        ) from error
    _validate_facet_semantics(scope, key, facet)


def validate_event(document: Mapping[str, object]) -> None:
    """Validate one generated event against official and local semantic rules."""
    kind = event_kind(document)
    try:
        errors = _sorted_schema_errors(
            _event_kind_validator(kind).iter_errors(document)
        )
    except Exception as exc:
        raise OpenLineageValidationError(
            "Official OpenLineage event validation could not be completed offline"
        ) from exc
    if errors:
        error = errors[0]
        raise OpenLineageValidationError(
            "Official OpenLineage event schema validation failed at "
            f"{_safe_json_pointer(error.absolute_path)} ({error.validator})"
        ) from error

    if set(document) != _ALLOWED_FIELDS[kind]:
        optional = {"inputs", "outputs"} if kind in {"job", "run"} else set()
        if set(document) - optional != _ALLOWED_FIELDS[kind] - optional:
            raise OpenLineageValidationError(
                "OpenLineage event fields do not match its event kind"
            )
    if document.get("schemaURL") != EVENT_SCHEMA_URLS[kind]:
        raise OpenLineageValidationError(
            "OpenLineage schema URL does not match its event kind"
        )
    if document.get("producer") != OPENLINEAGE_PRODUCER:
        raise OpenLineageValidationError(
            "OpenLineage event producer does not match the streamt producer"
        )
    _validate_event_time(document.get("eventTime"))

    if kind == "dataset":
        if "run" in document or "eventType" in document:
            raise OpenLineageValidationError(
                "Dataset events cannot contain run lifecycle fields"
            )
        _validate_dataset_object(document.get("dataset"), allow_facets=True)
        return

    if kind == "job":
        if "run" in document or "eventType" in document:
            raise OpenLineageValidationError("Job events cannot contain run lifecycle fields")
    elif "eventType" not in document:
        raise OpenLineageValidationError("Run events require an explicit event type")
    else:
        _validate_run_object(document.get("run"))

    _validate_job_object(document.get("job"))
    input_identities = _validate_dataset_collection(document.get("inputs", []))
    output_identities = _validate_dataset_collection(document.get("outputs", []))
    if input_identities & output_identities:
        raise OpenLineageValidationError(
            "One dataset cannot be both an input and output in the same event"
        )


def event_kind(document: Mapping[str, object]) -> Literal["dataset", "job", "run"]:
    """Classify an event from its required entity fields, failing on ambiguity."""
    if "run" in document:
        kind: Literal["dataset", "job", "run"] = "run"
    elif "dataset" in document:
        kind = "dataset"
    elif "job" in document:
        kind = "job"
    else:
        raise OpenLineageValidationError(
            "OpenLineage event must contain exactly one supported entity shape"
        )
    required_markers = {
        "dataset": {"dataset"},
        "job": {"job"},
        "run": {"run", "job"},
    }
    if not required_markers[kind].issubset(document):
        raise OpenLineageValidationError(
            "OpenLineage event is missing fields required by its event kind"
        )
    return kind


def validate_event_sequence(events: Sequence[Mapping[str, object]]) -> None:
    """Validate event objects plus complete finite BATCH run pairing."""
    processing_types: dict[tuple[str, str], str] = {}
    run_events: dict[str, list[tuple[int, str, tuple[str, str]]]] = {}
    static_datasets: set[tuple[str, str]] = set()
    static_jobs: set[tuple[str, str]] = set()

    for index, event in enumerate(events):
        validate_event(event)
        kind = event_kind(event)
        if kind == "dataset":
            dataset_identity = _validate_dataset_object(
                event.get("dataset"), allow_facets=True
            )
            if dataset_identity in static_datasets:
                raise OpenLineageValidationError(
                    "Static DatasetEvent identities must be unique"
                )
            static_datasets.add(dataset_identity)
            continue
        if kind not in {"job", "run"}:
            continue
        job = _mapping(event.get("job"), "job")
        job_identity = _job_identity(job)
        if kind == "job":
            if job_identity in static_jobs:
                raise OpenLineageValidationError("Static JobEvent identities must be unique")
            static_jobs.add(job_identity)
        processing_type = _job_processing_type(job)
        if processing_type is not None:
            previous = processing_types.get(job_identity)
            if previous is not None and previous != processing_type:
                raise OpenLineageValidationError(
                    "One job identity cannot declare conflicting processing types"
                )
            processing_types[job_identity] = processing_type
        if kind == "run":
            run = _mapping(event.get("run"), "run")
            run_id = run.get("runId")
            if not isinstance(run_id, str):
                raise OpenLineageValidationError("Run identity must be a UUID string")
            event_type = event.get("eventType")
            if not isinstance(event_type, str):
                raise OpenLineageValidationError("Run event type must be a string")
            run_events.setdefault(run_id, []).append((index, event_type, job_identity))

    for records in run_events.values():
        jobs = {record[2] for record in records}
        if len(jobs) != 1:
            raise OpenLineageValidationError(
                "One run identity cannot be reused across different jobs"
            )
        job_identity = next(iter(jobs))
        if processing_types.get(job_identity) != "BATCH":
            continue
        starts = [record for record in records if record[1] == "START"]
        terminals = [record for record in records if record[1] in _TERMINAL_EVENT_TYPES]
        if len(starts) != 1 or len(terminals) != 1:
            raise OpenLineageValidationError(
                "Every finite BATCH run requires exactly one START and one terminal event"
            )
        if starts[0][0] >= terminals[0][0]:
            raise OpenLineageValidationError(
                "A finite BATCH run START must precede its terminal event"
            )


def _validate_event_time(value: object) -> None:
    if not isinstance(value, str) or not value.strip():
        raise OpenLineageValidationError("OpenLineage event time must be nonblank RFC 3339")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise OpenLineageValidationError(
            "OpenLineage event time must be timezone-aware RFC 3339"
        ) from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise OpenLineageValidationError(
            "OpenLineage event time must be timezone-aware RFC 3339"
        )


def _validate_job_object(value: object) -> None:
    job = _mapping(value, "job")
    if set(job) - {"namespace", "name", "facets"}:
        raise OpenLineageValidationError("OpenLineage job contains unsupported fields")
    _job_identity(job)
    _validate_facets("job", job.get("facets", {}))


def _validate_run_object(value: object) -> None:
    run = _mapping(value, "run")
    if set(run) - {"runId", "facets"}:
        raise OpenLineageValidationError("OpenLineage run contains unsupported fields")
    run_id = run.get("runId")
    if not isinstance(run_id, str):
        raise OpenLineageValidationError("Run identity must be a UUID string")
    try:
        RunIdentity(run_id)
    except OpenLineageConstructionError as exc:
        raise OpenLineageValidationError("Run identity must be a canonical UUID") from exc
    _validate_facets("run", run.get("facets", {}))


def _validate_dataset_object(
    value: object,
    *,
    allow_facets: bool,
) -> tuple[str, str]:
    dataset = _mapping(value, "dataset")
    allowed = {"namespace", "name", "facets"} if allow_facets else {"namespace", "name"}
    if set(dataset) - allowed:
        raise OpenLineageValidationError("OpenLineage dataset contains unsupported fields")
    namespace = _nonblank(dataset.get("namespace"), "dataset namespace")
    name = _nonblank(dataset.get("name"), "dataset name")
    try:
        validate_kafka_namespace(namespace)
    except OpenLineageConstructionError as exc:
        raise OpenLineageValidationError(
            "Dataset namespace must be an exact kafka://host:port URI"
        ) from exc
    if allow_facets:
        _validate_facets("dataset", dataset.get("facets", {}))
    return namespace, name


def _validate_dataset_collection(value: object) -> set[tuple[str, str]]:
    if not isinstance(value, list):
        raise OpenLineageValidationError("Dataset collection must be an array")
    identities = {
        _validate_dataset_object(item, allow_facets=False)
        for item in value
    }
    if len(identities) != len(value):
        raise OpenLineageValidationError(
            "Dataset identities must be unique within an event direction"
        )
    return identities


def _validate_facets(scope: FacetScope, value: object) -> None:
    facets = _mapping(value, "facets")
    if not all(isinstance(key, str) for key in facets):
        raise OpenLineageValidationError("Facet keys must be strings")
    for key, facet in facets.items():
        if not isinstance(key, str) or not isinstance(facet, Mapping):
            raise OpenLineageValidationError("Facets must contain keyed objects")
        validate_standard_facet(scope, key, facet)


def _validate_facet_semantics(
    scope: FacetScope,
    key: str,
    facet: Mapping[str, object],
) -> None:
    if key == "schema":
        fields = facet.get("fields", [])
        if not isinstance(fields, list):
            raise OpenLineageValidationError("Schema facet fields must be an array")
        _validate_schema_fields(fields)
    elif key == "ownership":
        owners = facet.get("owners", [])
        if not isinstance(owners, list):
            raise OpenLineageValidationError("Ownership facet owners must be an array")
        names = [
            _nonblank(_mapping(owner, "owner").get("name"), "owner name")
            for owner in owners
        ]
        if len(names) != len(set(names)):
            raise OpenLineageValidationError("Owner identities must be unique within a facet")
        for owner in owners:
            owner_type = _mapping(owner, "owner").get("type")
            if owner_type is not None:
                _nonblank(owner_type, "owner type")
    elif key == "documentation":
        _nonblank(facet.get("description"), "documentation description")
        if facet.get("contentType") is not None:
            _nonblank(facet.get("contentType"), "documentation content type")
    elif key == "datasetType":
        _nonblank(facet.get("datasetType"), "dataset type")
        if facet.get("subType") is not None:
            _nonblank(facet.get("subType"), "dataset subtype")
    elif key == "jobType":
        processing_type = _nonblank(facet.get("processingType"), "processing type")
        if processing_type not in {"BATCH", "STREAMING", "SERVICE"}:
            raise OpenLineageValidationError("Job processing type is not supported")
        _nonblank(facet.get("integration"), "job integration")
        if facet.get("jobType") is not None:
            _nonblank(facet.get("jobType"), "job type")
        emission_pattern = facet.get("emissionPattern")
        if emission_pattern is not None:
            pattern = _mapping(emission_pattern, "job emission pattern")
            if set(pattern) - {"eventTrigger", "eventContentMode", "windowDuration"}:
                raise OpenLineageValidationError(
                    "Job emission pattern contains unsupported fields"
                )
            _nonblank(pattern.get("eventTrigger"), "job event trigger")
            _nonblank(pattern.get("eventContentMode"), "job event content mode")
    elif scope == "run" and key == "errorMessage":
        _nonblank(facet.get("message"), "error message")
        _nonblank(facet.get("programmingLanguage"), "programming language")
        if facet.get("stackTrace") is not None:
            _nonblank(facet.get("stackTrace"), "stack trace")


def _validate_schema_fields(fields: Sequence[object]) -> None:
    names: list[str] = []
    for field in fields:
        field_mapping = _mapping(field, "schema field")
        if set(field_mapping) - {
            "name",
            "type",
            "description",
            "ordinal_position",
            "fields",
        }:
            raise OpenLineageValidationError("Schema field contains unsupported fields")
        name = _nonblank(field_mapping.get("name"), "schema field name")
        names.append(name)
        for optional in ("type", "description"):
            if field_mapping.get(optional) is not None:
                _nonblank(field_mapping.get(optional), f"schema field {optional}")
        nested = field_mapping.get("fields", [])
        if not isinstance(nested, list):
            raise OpenLineageValidationError("Nested schema fields must be an array")
        _validate_schema_fields(nested)
    if len(names) != len(set(names)):
        raise OpenLineageValidationError("Schema field names must be unique per container")


def _job_identity(job: Mapping[object, object]) -> tuple[str, str]:
    return (
        _nonblank(job.get("namespace"), "job namespace"),
        _nonblank(job.get("name"), "job name"),
    )


def _job_processing_type(job: Mapping[object, object]) -> str | None:
    facets = job.get("facets")
    if not isinstance(facets, Mapping):
        return None
    job_type = facets.get("jobType")
    if not isinstance(job_type, Mapping):
        return None
    processing_type = job_type.get("processingType")
    return processing_type if isinstance(processing_type, str) else None


def _mapping(value: object, label: str) -> Mapping[object, object]:
    if not isinstance(value, Mapping):
        raise OpenLineageValidationError(f"OpenLineage {label} must be an object")
    return value


def _nonblank(value: object, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise OpenLineageValidationError(f"OpenLineage {label} must be nonblank")
    return value


def _validate_facet_allowed_fields(key: str, facet: Mapping[str, object]) -> None:
    metadata_fields = {"_producer", "_schemaURL"}
    facet_fields = {
        "datasetType": {"datasetType", "subType"},
        "documentation": {"description", "contentType"},
        "errorMessage": {"message", "programmingLanguage", "stackTrace"},
        "jobType": {"processingType", "integration", "jobType", "emissionPattern"},
        "ownership": {"owners"},
        "schema": {"fields"},
    }
    if set(facet) - metadata_fields - facet_fields[key]:
        raise OpenLineageValidationError(
            "Standard OpenLineage facet contains unsupported fields"
        )
    if key == "ownership":
        owners = facet.get("owners", [])
        if isinstance(owners, list):
            for owner in owners:
                owner_mapping = _mapping(owner, "owner")
                if set(owner_mapping) - {"name", "type"}:
                    raise OpenLineageValidationError(
                        "OpenLineage owner contains unsupported fields"
                    )


def _sorted_schema_errors(
    errors: Iterable[ValidationError],
) -> list[ValidationError]:
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


def _clear_schema_caches() -> None:
    """Clear resource caches for isolated integrity tests."""
    _facet_validator.cache_clear()
    _event_kind_validator.cache_clear()
    _event_validator.cache_clear()
    _schema_registry.cache_clear()
    _official_schemas.cache_clear()
