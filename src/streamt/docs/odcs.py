"""Deterministic Open Data Contract Standard 3.1 export and validation."""

from __future__ import annotations

import base64
import gzip
import hashlib
import json
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from functools import lru_cache
from importlib.resources import files
from uuid import NAMESPACE_URL, uuid5

from jsonschema import Draft201909Validator, FormatChecker

from streamt.core.models import MaterializedType, Model, Source, StreamtProject

ODCS_VERSION = "3.1.0"
ODCS_API_VERSION = f"v{ODCS_VERSION}"
ODCS_SCHEMA_RESOURCE = "odcs-3.1.0.json.gz.b64"
ODCS_OFFICIAL_SCHEMA_SIZE = 86_441
ODCS_OFFICIAL_SCHEMA_SHA256 = (
    "2cb7dd6fe43344d2233e0406438622681dc3ebadcf8f0d606a15b40c8f6752c0"
)
ODCS_INCOMPLETE_SCHEMA_WARNING = "W109_ODCS_SCHEMA_INCOMPLETE"

_SIMPLE_LOGICAL_TYPES = {
    "STRING": "string",
    "TINYINT": "integer",
    "SMALLINT": "integer",
    "INT": "integer",
    "INTEGER": "integer",
    "BIGINT": "integer",
    "REAL": "number",
    "FLOAT": "number",
    "DOUBLE": "number",
    "BOOLEAN": "boolean",
    "BOOL": "boolean",
    "DATE": "date",
}
_CHARACTER_TYPE_RE = re.compile(r"(?:CHAR|VARCHAR)\s*\(\s*(\d+)\s*\)", re.IGNORECASE)
_DECIMAL_TYPE_RE = re.compile(
    r"(?:DECIMAL|NUMERIC)(?:\s*\(\s*(\d+)\s*,\s*(\d+)\s*\))?",
    re.IGNORECASE,
)
_TEMPORAL_TYPE_RE = re.compile(
    r"(TIME|TIMESTAMP|TIMESTAMP_LTZ)(?:\s*\(\s*(\d+)\s*\))?",
    re.IGNORECASE,
)
_MAX_CHARACTER_LENGTH = 2_147_483_647
_MAX_DECIMAL_PRECISION = 38
_MAX_TEMPORAL_PRECISION = 9


class ODCSGenerationError(ValueError):
    """Raised when a project cannot be exported without inventing metadata."""


class ODCSValidationError(ValueError):
    """Raised when a document or bundled schema fails ODCS validation."""


@dataclass(frozen=True)
class ODCSExportWarning:
    """A safe warning produced while mapping an incomplete declared schema."""

    code: str
    message: str
    location: str


@dataclass(frozen=True)
class ODCSExport:
    """A validated ODCS document and its deterministic mapping warnings."""

    document: dict[str, object]
    warnings: tuple[ODCSExportWarning, ...]


@dataclass(frozen=True)
class _DeclaredColumn:
    name: str
    physical_type: str | None
    required: bool | None
    description: str | None
    classification: str | None


@lru_cache(maxsize=1)
def _official_schema() -> dict[str, object]:
    """Load and integrity-check the immutable official ODCS 3.1 schema."""
    try:
        encoded = b"".join(
            files("streamt.docs.schemas").joinpath(ODCS_SCHEMA_RESOURCE).read_bytes().split()
        )
        raw_schema = gzip.decompress(base64.b64decode(encoded, validate=True))
    except (OSError, ValueError) as exc:
        raise ODCSValidationError("Bundled ODCS schema cannot be decoded") from exc

    if len(raw_schema) != ODCS_OFFICIAL_SCHEMA_SIZE:
        raise ODCSValidationError(
            "Bundled ODCS schema size does not match its pinned upstream artifact"
        )
    if hashlib.sha256(raw_schema).hexdigest() != ODCS_OFFICIAL_SCHEMA_SHA256:
        raise ODCSValidationError(
            "Bundled ODCS schema checksum does not match its pinned upstream artifact"
        )

    try:
        loaded: object = json.loads(raw_schema)
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise ODCSValidationError("Bundled ODCS schema is not valid JSON") from exc
    if not isinstance(loaded, dict):
        raise ODCSValidationError("Bundled ODCS schema root must be an object")
    return loaded


@lru_cache(maxsize=1)
def _official_validator() -> Draft201909Validator:
    """Return the offline validator for the pinned official schema."""
    schema = _official_schema()
    Draft201909Validator.check_schema(schema)
    return Draft201909Validator(schema, format_checker=FormatChecker())


def validate_odcs_document(document: Mapping[str, object]) -> None:
    """Validate a streamt ODCS document against official and local semantics."""
    errors = sorted(
        _official_validator().iter_errors(document),
        key=lambda error: (
            tuple(str(part) for part in error.absolute_path),
            error.message,
        ),
    )
    if errors:
        error = errors[0]
        raise ODCSValidationError(
            f"ODCS {ODCS_VERSION} schema validation failed at "
            f"{_json_pointer(error.absolute_path)}: {error.message}"
        )
    _validate_streamt_semantics(document)


def generate_odcs_document(
    project: StreamtProject,
    *,
    contract_id: str,
    status: str,
    contract_version: str | None = None,
) -> ODCSExport:
    """Generate one validated ODCS document for one parsed streamt project."""
    _require_nonblank(contract_id, "contract ID")
    _require_nonblank(status, "contract status")
    version = contract_version if contract_version is not None else project.project.version
    if version is None:
        raise ODCSGenerationError(
            "ODCS contract version is required; set project.version or pass one explicitly"
        )
    _require_nonblank(version, "contract version")

    _assert_project_collisions(project)
    generated_ids: dict[str, str] = {}
    warnings: list[ODCSExportWarning] = []
    schema_objects: list[dict[str, object]] = []

    for source in sorted(project.sources, key=lambda item: item.name):
        schema_objects.append(
            _source_object(
                source,
                contract_id=contract_id,
                generated_ids=generated_ids,
                warnings=warnings,
            )
        )
    for model in sorted(project.models, key=lambda item: item.name):
        schema_objects.append(
            _model_object(
                model,
                contract_id=contract_id,
                generated_ids=generated_ids,
                warnings=warnings,
            )
        )

    if not schema_objects:
        raise ODCSGenerationError(
            "ODCS export requires at least one declared source or model schema object"
        )

    document: dict[str, object] = {
        "apiVersion": ODCS_API_VERSION,
        "kind": "DataContract",
        "id": contract_id,
        "name": project.project.name,
        "version": version,
        "status": status,
        "schema": schema_objects,
    }
    validate_odcs_document(document)
    return ODCSExport(document=document, warnings=tuple(warnings))


def flink_type_to_odcs_logical_type(flink_type: str) -> str | None:
    """Map an unambiguous declared Flink scalar family to an ODCS logical type."""
    normalized = flink_type.strip()
    simple_type = _SIMPLE_LOGICAL_TYPES.get(normalized.upper())
    if simple_type is not None:
        return simple_type

    character_match = _CHARACTER_TYPE_RE.fullmatch(normalized)
    if character_match is not None:
        length = int(character_match.group(1))
        return "string" if 1 <= length <= _MAX_CHARACTER_LENGTH else None

    decimal_match = _DECIMAL_TYPE_RE.fullmatch(normalized)
    if decimal_match is not None:
        precision_text, scale_text = decimal_match.groups()
        if precision_text is None:
            return "number"
        precision = int(precision_text)
        scale = int(scale_text)
        if 1 <= precision <= _MAX_DECIMAL_PRECISION and 0 <= scale <= precision:
            return "number"
        return None

    temporal_match = _TEMPORAL_TYPE_RE.fullmatch(normalized)
    if temporal_match is not None:
        family, precision_text = temporal_match.groups()
        if precision_text is not None:
            precision = int(precision_text)
            if not 0 <= precision <= _MAX_TEMPORAL_PRECISION:
                return None
        return "time" if family.upper() == "TIME" else "timestamp"

    return None


def _source_object(
    source: Source,
    *,
    contract_id: str,
    generated_ids: dict[str, str],
    warnings: list[ODCSExportWarning],
) -> dict[str, object]:
    resource = f"source '{source.name}'"
    schema_object: dict[str, object] = {
        "id": _generated_id(
            contract_id,
            "source",
            source.name,
            generated_ids=generated_ids,
            location=resource,
        ),
        "name": source.name,
        "logicalType": "object",
        "physicalName": source.topic,
        "physicalType": "topic",
    }
    if source.description is not None:
        schema_object["description"] = source.description
    if source.tags:
        schema_object["tags"] = list(source.tags)

    columns = [
        _DeclaredColumn(
            name=column.name,
            physical_type=column.type,
            required=column.required,
            description=column.description,
            classification=(column.classification.value if column.classification else None),
        )
        for column in source.columns
    ]
    properties = _property_objects(
        columns,
        contract_id=contract_id,
        resource_kind="source",
        resource_name=source.name,
        generated_ids=generated_ids,
        primary_key_positions={},
    )
    if properties:
        schema_object["properties"] = properties
    else:
        warnings.append(_incomplete_warning("source", source.name))
    schema_object["customProperties"] = [
        {"property": "streamtResourceType", "value": "source"}
    ]
    return schema_object


def _model_object(
    model: Model,
    *,
    contract_id: str,
    generated_ids: dict[str, str],
    warnings: list[ODCSExportWarning],
) -> dict[str, object]:
    resource = f"model '{model.name}'"
    schema_object: dict[str, object] = {
        "id": _generated_id(
            contract_id,
            "model",
            model.name,
            generated_ids=generated_ids,
            location=resource,
        ),
        "name": model.name,
        "logicalType": "object",
    }
    if not _is_sink_model(model):
        topic_config = model.get_topic_config()
        schema_object["physicalName"] = (
            topic_config.name if topic_config and topic_config.name else model.name
        )
        schema_object["physicalType"] = "topic"
    if model.description is not None:
        schema_object["description"] = model.description
    if model.tags:
        schema_object["tags"] = list(model.tags)

    columns = _model_columns(model)
    primary_key_positions = _primary_key_positions(model, columns)
    properties = _property_objects(
        columns,
        contract_id=contract_id,
        resource_kind="model",
        resource_name=model.name,
        generated_ids=generated_ids,
        primary_key_positions=primary_key_positions,
    )
    if properties:
        schema_object["properties"] = properties
    else:
        warnings.append(_incomplete_warning("model", model.name))

    custom_properties: list[dict[str, object]] = [
        {"property": "streamtResourceType", "value": "model"}
    ]
    if model.contract is not None:
        custom_properties.append(
            {
                "property": "streamtContractEnforced",
                "value": model.contract.enforced,
            }
        )
    schema_object["customProperties"] = custom_properties
    return schema_object


def _model_columns(model: Model) -> list[_DeclaredColumn]:
    security_classifications = (
        model.security.classification if model.security is not None else {}
    )
    if model.contract is not None:
        return [
            _DeclaredColumn(
                name=column.name,
                physical_type=column.type,
                required=(None if column.nullable is None else not column.nullable),
                description=column.description,
                classification=(
                    security_classifications[column.name].value
                    if column.name in security_classifications
                    else None
                ),
            )
            for column in model.contract.columns
        ]
    if model.columns is None:
        return []

    columns: list[_DeclaredColumn] = []
    for column in model.columns:
        declared = column.classification.value if column.classification else None
        security = security_classifications.get(column.name)
        security_value = security.value if security is not None else None
        if declared is not None and security_value is not None and declared != security_value:
            raise ODCSGenerationError(
                f"Conflicting classifications for model '{model.name}' property "
                f"'{column.name}': {declared!r} and {security_value!r}"
            )
        columns.append(
            _DeclaredColumn(
                name=column.name,
                physical_type=column.type,
                required=column.required,
                description=column.description,
                classification=declared or security_value,
            )
        )
    return columns


def _property_objects(
    columns: Sequence[_DeclaredColumn],
    *,
    contract_id: str,
    resource_kind: str,
    resource_name: str,
    generated_ids: dict[str, str],
    primary_key_positions: Mapping[str, int],
) -> list[dict[str, object]]:
    seen_names: set[str] = set()
    properties: list[dict[str, object]] = []
    for column in columns:
        if column.name in seen_names:
            raise ODCSGenerationError(
                f"Duplicate property {column.name!r} in {resource_kind} {resource_name!r}"
            )
        seen_names.add(column.name)

        property_object: dict[str, object] = {
            "id": _generated_id(
                contract_id,
                resource_kind,
                resource_name,
                column.name,
                generated_ids=generated_ids,
                location=f"{resource_kind} '{resource_name}' property '{column.name}'",
            ),
            "name": column.name,
        }
        if column.physical_type is not None:
            property_object["physicalType"] = column.physical_type
            logical_type = flink_type_to_odcs_logical_type(column.physical_type)
            if logical_type is not None:
                property_object["logicalType"] = logical_type
        if column.description is not None:
            property_object["description"] = column.description
        if column.classification is not None:
            property_object["classification"] = column.classification
        if column.required is not None:
            property_object["required"] = column.required
        primary_key_position = primary_key_positions.get(column.name)
        if primary_key_position is not None:
            property_object["primaryKey"] = True
            property_object["primaryKeyPosition"] = primary_key_position
        properties.append(property_object)
    return properties


def _primary_key_positions(
    model: Model,
    columns: Sequence[_DeclaredColumn],
) -> dict[str, int]:
    positions: dict[str, int] = {}
    for position, name in enumerate(model.primary_key or [], start=1):
        if name in positions:
            raise ODCSGenerationError(
                f"Duplicate primary-key property {name!r} in model {model.name!r}"
            )
        positions[name] = position
    available = {column.name for column in columns}
    missing = [name for name in positions if name not in available]
    if missing:
        raise ODCSGenerationError(
            f"Model {model.name!r} primary key references properties not present in its "
            f"exported schema: {missing!r}"
        )
    return positions


def _is_sink_model(model: Model) -> bool:
    """Detect only declared or structural sinks without classifying SQL content."""
    if model.materialized is not None:
        return model.materialized == MaterializedType.SINK
    return bool(model.from_) and not model.sql


def _generated_id(
    contract_id: str,
    resource_kind: str,
    resource_name: str,
    column_name: str | None = None,
    *,
    generated_ids: dict[str, str],
    location: str,
) -> str:
    seed_parts = ["streamt-odcs-v1", contract_id, resource_kind, resource_name]
    if column_name is not None:
        seed_parts.append(column_name)
    seed = json.dumps(seed_parts, ensure_ascii=False, separators=(",", ":"))
    identifier = str(uuid5(NAMESPACE_URL, seed))
    previous = generated_ids.get(identifier)
    if previous is not None:
        raise ODCSGenerationError(
            f"Generated ODCS ID collision between {previous} and {location}: {identifier}"
        )
    generated_ids[identifier] = location
    return identifier


def _assert_project_collisions(project: StreamtProject) -> None:
    seen_names: dict[str, str] = {}
    physical_topics: dict[str, str] = {}
    for source in project.sources:
        _register_collision(seen_names, source.name, f"source '{source.name}'", "logical name")
        if source.topic:
            _register_collision(
                physical_topics,
                source.topic,
                f"source '{source.name}'",
                "physical topic",
            )
    for model in project.models:
        _register_collision(seen_names, model.name, f"model '{model.name}'", "logical name")
        if _is_sink_model(model):
            continue
        topic_config = model.get_topic_config()
        topic = topic_config.name if topic_config and topic_config.name else model.name
        if topic:
            _register_collision(
                physical_topics,
                topic,
                f"model '{model.name}'",
                "physical topic",
            )


def _register_collision(
    seen: dict[str, str],
    identity: str,
    location: str,
    identity_kind: str,
) -> None:
    previous = seen.get(identity)
    if previous is not None:
        raise ODCSGenerationError(
            f"ODCS {identity_kind} collision for {identity!r}: {previous} and {location}"
        )
    seen[identity] = location


def _incomplete_warning(resource_kind: str, name: str) -> ODCSExportWarning:
    return ODCSExportWarning(
        code=ODCS_INCOMPLETE_SCHEMA_WARNING,
        message=(
            f"{resource_kind.capitalize()} '{name}' has no declared export columns; "
            "the ODCS schema object omits properties"
        ),
        location=f"{resource_kind}.{name}",
    )


def _require_nonblank(value: str, label: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise ODCSGenerationError(f"ODCS {label} must contain a non-whitespace character")


def _validate_streamt_semantics(document: Mapping[str, object]) -> None:
    if document.get("apiVersion") != ODCS_API_VERSION:
        raise ODCSValidationError(
            f"ODCS apiVersion must be {ODCS_API_VERSION!r} for this exporter"
        )
    if document.get("kind") != "DataContract":
        raise ODCSValidationError("ODCS kind must be 'DataContract'")
    for field in ("id", "version", "status"):
        value = document.get(field)
        if not isinstance(value, str) or not value.strip():
            raise ODCSValidationError(f"ODCS root {field!r} must be a non-blank string")

    schema = document.get("schema")
    if not isinstance(schema, list) or not schema:
        raise ODCSValidationError("ODCS root 'schema' must be a non-empty array")

    names: dict[str, str] = {}
    physical_topics: dict[str, str] = {}
    identifiers: dict[str, str] = {}
    for index, item in enumerate(schema):
        if not isinstance(item, Mapping):
            continue
        location = f"schema/{index}"
        name = item.get("name")
        if isinstance(name, str):
            _register_validation_identity(names, name, location, "schema name")
        identifier = item.get("id")
        if not isinstance(identifier, str) or not identifier:
            raise ODCSValidationError(f"ODCS {location} requires a generated ID")
        _register_validation_identity(identifiers, identifier, location, "generated ID")
        if item.get("physicalType") == "topic":
            physical_name = item.get("physicalName")
            if isinstance(physical_name, str) and physical_name:
                _register_validation_identity(
                    physical_topics,
                    physical_name,
                    location,
                    "physical topic",
                )

        properties = item.get("properties")
        if not isinstance(properties, list):
            continue
        property_names: dict[str, str] = {}
        for property_index, property_item in enumerate(properties):
            if not isinstance(property_item, Mapping):
                continue
            property_location = f"{location}/properties/{property_index}"
            property_name = property_item.get("name")
            if isinstance(property_name, str):
                _register_validation_identity(
                    property_names,
                    property_name,
                    property_location,
                    "property name",
                )
            property_id = property_item.get("id")
            if not isinstance(property_id, str) or not property_id:
                raise ODCSValidationError(
                    f"ODCS {property_location} requires a generated ID"
                )
            _register_validation_identity(
                identifiers,
                property_id,
                property_location,
                "generated ID",
            )


def _register_validation_identity(
    seen: dict[str, str],
    identity: str,
    location: str,
    identity_kind: str,
) -> None:
    previous = seen.get(identity)
    if previous is not None:
        raise ODCSValidationError(
            f"Duplicate ODCS {identity_kind} {identity!r} at {previous} and {location}"
        )
    seen[identity] = location


def _json_pointer(path: Sequence[object]) -> str:
    parts = [str(part).replace("~", "~0").replace("/", "~1") for part in path]
    return "/" + "/".join(parts) if parts else "<root>"


def _schema_cache_clear() -> None:
    """Clear schema caches for deterministic integrity and offline tests."""
    _official_validator.cache_clear()
    _official_schema.cache_clear()
