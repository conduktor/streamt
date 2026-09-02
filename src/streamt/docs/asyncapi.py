"""Deterministic AsyncAPI 3.1 document generation and offline validation."""

from __future__ import annotations

import base64
import gzip
import hashlib
import json
import re
from collections.abc import Mapping, Sequence
from functools import lru_cache
from importlib.resources import files

from jsonschema import Draft7Validator, FormatChecker

from streamt.core.models import MaterializedType, Model, StreamtProject, TopicConfig

ASYNCAPI_VERSION = "3.1.0"
KAFKA_BINDING_VERSION = "0.5.0"

_OFFICIAL_SCHEMA_RESOURCE = "asyncapi-3.1.0-without-id.json.gz.b64"
_OFFICIAL_SCHEMA_SHA256 = "44c97489a276ad11f6f9edcbdbde47af8b66f35f97f2af9f3a289e9e24c584c5"
_COMPONENT_ID_RE = re.compile(r"^[A-Za-z0-9._-]+$")
_UNSAFE_ID_CHARS_RE = re.compile(r"[^A-Za-z0-9._-]+")
_INTEGER_TOPIC_CONFIG = {
    "retention.ms": -1,
    "retention.bytes": -1,
    "delete.retention.ms": 0,
    "max.message.bytes": 0,
}
_BOOLEAN_TOPIC_CONFIG = {
    "confluent.key.schema.validation",
    "confluent.value.schema.validation",
}
_STRING_TOPIC_CONFIG = {
    "confluent.key.subject.name.strategy",
    "confluent.value.subject.name.strategy",
}


class AsyncAPIGenerationError(ValueError):
    """Raised when a project cannot be represented without inventing metadata."""


class AsyncAPIValidationError(ValueError):
    """Raised when a document fails official-schema or semantic validation."""


@lru_cache(maxsize=1)
def _official_schema() -> Mapping[str, object]:
    """Load and integrity-check the bundled official AsyncAPI 3.1 schema."""
    encoded = b"".join(
        files("streamt.docs.schemas").joinpath(_OFFICIAL_SCHEMA_RESOURCE).read_bytes()
        .split()
    )
    try:
        raw_schema = gzip.decompress(base64.b64decode(encoded, validate=True))
    except (ValueError, OSError) as exc:
        raise AsyncAPIValidationError("Bundled AsyncAPI schema cannot be decoded") from exc

    digest = hashlib.sha256(raw_schema).hexdigest()
    if digest != _OFFICIAL_SCHEMA_SHA256:
        raise AsyncAPIValidationError(
            "Bundled AsyncAPI schema checksum does not match its pinned upstream artifact"
        )

    loaded = json.loads(raw_schema)
    if not isinstance(loaded, dict):
        raise AsyncAPIValidationError("Bundled AsyncAPI schema root must be an object")
    return loaded


@lru_cache(maxsize=1)
def _official_validator() -> Draft7Validator:
    """Return an offline validator for the pinned official schema."""
    schema = _official_schema()
    Draft7Validator.check_schema(schema)
    return Draft7Validator(schema, format_checker=FormatChecker())


def validate_asyncapi_document(document: Mapping[str, object]) -> None:
    """Validate against the official 3.1 schema and cross-reference semantics.

    The official JSON Schema enforces the document/object shapes and Kafka
    binding vocabulary. The additional semantic pass enforces the v3 rules the
    JSON Schema cannot prove: root operation channel references, operation
    message subsets, local component references, and identifier uniqueness.
    """
    errors = sorted(
        _official_validator().iter_errors(document),
        key=lambda error: tuple(str(part) for part in error.absolute_path),
    )
    if errors:
        error = errors[0]
        location = "/".join(str(part) for part in error.absolute_path) or "<root>"
        raise AsyncAPIValidationError(
            f"AsyncAPI {ASYNCAPI_VERSION} schema validation failed at {location}: "
            f"{error.message}"
        )
    _validate_reference_semantics(document)


def generate_asyncapi_document(project: StreamtProject) -> dict[str, object]:
    """Generate a deterministic AsyncAPI 3.1 document for Kafka channels."""
    _assert_unique_generated_identifiers(project)
    channels: dict[str, object] = {}
    operations: dict[str, object] = {}
    messages: dict[str, object] = {}
    schemas: dict[str, object] = {}

    for source in sorted(project.sources, key=lambda item: (item.name, item.topic)):
        base_id = _resource_id("source", source.name)
        channel_id = base_id
        message_id = f"{base_id}.message"
        operation_id = f"receive.{base_id}"
        schema_id = f"{base_id}.payload"

        message: dict[str, object] = {
            "name": message_id,
            "title": f"{source.name} source message",
            "description": source.description or f"Message received from source `{source.name}`.",
        }
        if source.columns:
            payload = _payload_schema(
                resource=f"source '{source.name}'",
                columns=[
                    (column.name, column.type, column.required, column.description)
                    for column in source.columns
                ],
                closed=False,
            )
            _register(schemas, schema_id, payload, "schema")
            message["payload"] = {"$ref": f"#/components/schemas/{schema_id}"}

        _register(messages, message_id, message, "message")
        channel: dict[str, object] = {
            "address": source.topic,
            "title": f"{source.name} source",
            "description": source.description or f"Kafka source declared as `{source.name}`.",
            "messages": {
                message_id: {"$ref": f"#/components/messages/{message_id}"},
            },
        }
        _register(channels, channel_id, channel, "channel")
        _register(
            operations,
            operation_id,
            {
                "action": "receive",
                "title": f"Receive {source.name}",
                "description": f"The streamt project receives messages from `{source.topic}`.",
                "channel": {"$ref": f"#/channels/{channel_id}"},
                "messages": [
                    {"$ref": f"#/channels/{channel_id}/messages/{message_id}"},
                ],
            },
            "operation",
        )

    for model in sorted(project.models, key=lambda item: item.name):
        materialized = _effective_materialization(project, model)
        if materialized == MaterializedType.SINK:
            continue

        topic = (
            model.get_virtual_topic_name()
            if materialized == MaterializedType.VIRTUAL_TOPIC
            else _model_topic_name(model)
        )
        base_id = _resource_id("model", model.name)
        channel_id = base_id
        message_id = f"{base_id}.message"
        operation_id = f"send.{base_id}"
        schema_id = f"{base_id}.payload"

        message = {
            "name": message_id,
            "title": f"{model.name} model message",
            "description": model.description or f"Message produced by model `{model.name}`.",
        }
        declared_columns: list[tuple[str, str | None, bool, str | None]] | None = None
        closed_schema = False
        if model.contract is not None:
            declared_columns = [
                (column.name, column.type, column.nullable is False, column.description)
                for column in model.contract.columns
            ]
            closed_schema = model.contract.enforced
        elif model.columns is not None:
            declared_columns = [
                (column.name, column.type, column.required, column.description)
                for column in model.columns
            ]

        if declared_columns is not None:
            payload = _payload_schema(
                resource=f"model '{model.name}'",
                columns=declared_columns,
                closed=closed_schema,
            )
            _register(schemas, schema_id, payload, "schema")
            message["payload"] = {"$ref": f"#/components/schemas/{schema_id}"}

        _register(messages, message_id, message, "message")
        channel = {
            "address": topic,
            "title": f"{model.name} model output",
            "description": model.description or f"Kafka output declared by model `{model.name}`.",
            "messages": {
                message_id: {"$ref": f"#/components/messages/{message_id}"},
            },
        }
        binding = _kafka_channel_binding(model, materialized)
        if binding is not None:
            channel["bindings"] = {"kafka": binding}
        _register(channels, channel_id, channel, "channel")
        _register(
            operations,
            operation_id,
            {
                "action": "send",
                "title": f"Send {model.name}",
                "description": f"The streamt project sends messages to `{topic}`.",
                "channel": {"$ref": f"#/channels/{channel_id}"},
                "messages": [
                    {"$ref": f"#/channels/{channel_id}/messages/{message_id}"},
                ],
            },
            "operation",
        )

    info: dict[str, object] = {
        "title": project.project.name,
        "version": project.project.version or "0.0.0",
    }
    if project.project.description:
        info["description"] = project.project.description

    document: dict[str, object] = {
        "asyncapi": ASYNCAPI_VERSION,
        "info": info,
        "channels": channels,
        "operations": operations,
        "components": {
            "messages": messages,
            "schemas": schemas,
        },
    }
    validate_asyncapi_document(document)
    return document


def _assert_unique_generated_identifiers(project: StreamtProject) -> None:
    """Reject normalized identifier collisions before any document map is built."""
    seen: dict[str, set[str]] = {
        "channel": set(),
        "message": set(),
        "operation": set(),
        "schema": set(),
    }
    resources: list[tuple[str, str, str]] = [
        ("source", source.name, "receive") for source in project.sources
    ]
    resources.extend(
        ("model", model.name, "send")
        for model in project.models
        if _effective_materialization(project, model) != MaterializedType.SINK
    )
    for kind, name, action in resources:
        base_id = _resource_id(kind, name)
        identifiers = {
            "channel": base_id,
            "message": f"{base_id}.message",
            "operation": f"{action}.{base_id}",
            "schema": f"{base_id}.payload",
        }
        collisions = [
            identifier_kind
            for identifier_kind, identifier in identifiers.items()
            if identifier in seen[identifier_kind]
        ]
        if collisions:
            rendered = ", ".join(
                f"{identifier_kind} {identifiers[identifier_kind]!r}"
                for identifier_kind in collisions
            )
            raise AsyncAPIGenerationError(
                f"Normalized AsyncAPI identifiers for {kind} {name!r} collide: {rendered}"
            )
        for identifier_kind, identifier in identifiers.items():
            seen[identifier_kind].add(identifier)


def _effective_materialization(project: StreamtProject, model: Model) -> MaterializedType:
    """Mirror the compiler's Gateway fallback for truthful topic bindings."""
    materialized = model.get_materialized()
    if materialized != MaterializedType.VIRTUAL_TOPIC:
        return materialized
    has_gateway = bool(project.runtime.conduktor and project.runtime.conduktor.gateway)
    explicit_virtual_topic = bool(model.gateway and model.gateway.virtual_topic)
    if not has_gateway and not explicit_virtual_topic:
        return MaterializedType.FLINK
    return materialized


def _model_topic_name(model: Model) -> str:
    topic_config = model.get_topic_config()
    return topic_config.name if topic_config and topic_config.name else model.name


def _resource_id(kind: str, name: str) -> str:
    normalized = _UNSAFE_ID_CHARS_RE.sub("-", name.strip()).strip("-")
    if not normalized or not _COMPONENT_ID_RE.fullmatch(normalized):
        raise AsyncAPIGenerationError(
            f"Cannot derive a valid AsyncAPI identifier from {kind} name {name!r}"
        )
    return f"{kind}.{normalized}"


def _register(target: dict[str, object], identifier: str, value: object, kind: str) -> None:
    if identifier in target:
        raise AsyncAPIGenerationError(f"Duplicate AsyncAPI {kind} identifier {identifier!r}")
    target[identifier] = value


def _payload_schema(
    *,
    resource: str,
    columns: Sequence[tuple[str, str | None, bool, str | None]],
    closed: bool,
) -> dict[str, object]:
    properties: dict[str, object] = {}
    required: list[str] = []
    for name, flink_type, is_required, description in sorted(columns, key=lambda item: item[0]):
        if name in properties:
            raise AsyncAPIGenerationError(f"Duplicate column {name!r} in {resource}")
        # A declared column without a type still carries useful name,
        # requiredness, and description metadata.  An empty Schema Object is
        # the truthful JSON Schema representation: it accepts any value rather
        # than inventing STRING as the wire type.
        schema = (
            flink_type_to_asyncapi_schema(flink_type)
            if flink_type is not None
            else {}
        )
        if description:
            schema["description"] = description
        properties[name] = schema
        if is_required:
            required.append(name)

    payload: dict[str, object] = {
        "type": "object",
        "properties": properties,
    }
    if required:
        payload["required"] = required
    if closed:
        payload["additionalProperties"] = False
    return payload


def flink_type_to_asyncapi_schema(flink_type: str) -> dict[str, object]:
    """Convert one declared Flink SQL data type without lossy fallbacks."""
    normalized = flink_type.strip()
    if not normalized:
        raise AsyncAPIGenerationError("Flink column type must not be empty")

    wrapped = _wrapped_type(normalized, "ARRAY")
    if wrapped is not None:
        return {"type": "array", "items": flink_type_to_asyncapi_schema(wrapped)}
    wrapped = _wrapped_type(normalized, "MAP")
    if wrapped is not None:
        parts = _split_top_level(wrapped)
        if len(parts) != 2:
            raise AsyncAPIGenerationError(f"Malformed Flink MAP type {flink_type!r}")
        key_schema = flink_type_to_asyncapi_schema(parts[0])
        if key_schema.get("type") != "string":
            raise AsyncAPIGenerationError(
                f"Flink MAP key type must map to a JSON object string key: {flink_type!r}"
            )
        return {
            "type": "object",
            "additionalProperties": flink_type_to_asyncapi_schema(parts[1]),
        }
    wrapped = _wrapped_type(normalized, "ROW")
    if wrapped is not None:
        properties: dict[str, object] = {}
        for field in _split_top_level(wrapped):
            name, field_type = _row_field(field, flink_type)
            if name in properties:
                raise AsyncAPIGenerationError(
                    f"Duplicate ROW field {name!r} in Flink type {flink_type!r}"
                )
            properties[name] = flink_type_to_asyncapi_schema(field_type)
        return {"type": "object", "properties": properties}

    return _scalar_type_schema(normalized)


def _wrapped_type(value: str, wrapper: str) -> str | None:
    match = re.fullmatch(rf"{wrapper}\s*<(.*)>", value, flags=re.IGNORECASE | re.DOTALL)
    if match is None:
        return None
    inner = match.group(1).strip()
    if not inner:
        raise AsyncAPIGenerationError(f"Malformed Flink {wrapper} type {value!r}")
    _assert_balanced_angles(inner, value)
    return inner


def _assert_balanced_angles(value: str, original: str) -> None:
    depth = 0
    for char in value:
        if char == "<":
            depth += 1
        elif char == ">":
            depth -= 1
            if depth < 0:
                break
    if depth != 0:
        raise AsyncAPIGenerationError(f"Malformed nested Flink type {original!r}")


def _split_top_level(value: str) -> list[str]:
    parts: list[str] = []
    start = 0
    angle_depth = 0
    paren_depth = 0
    in_backticks = False
    for index, char in enumerate(value):
        if char == "`":
            in_backticks = not in_backticks
        elif not in_backticks:
            if char == "<":
                angle_depth += 1
            elif char == ">":
                angle_depth -= 1
            elif char == "(":
                paren_depth += 1
            elif char == ")":
                paren_depth -= 1
            elif char == "," and angle_depth == 0 and paren_depth == 0:
                part = value[start:index].strip()
                if not part:
                    raise AsyncAPIGenerationError(f"Malformed Flink type list {value!r}")
                parts.append(part)
                start = index + 1
        if angle_depth < 0 or paren_depth < 0:
            raise AsyncAPIGenerationError(f"Malformed Flink type list {value!r}")
    if in_backticks or angle_depth != 0 or paren_depth != 0:
        raise AsyncAPIGenerationError(f"Malformed Flink type list {value!r}")
    final = value[start:].strip()
    if not final:
        raise AsyncAPIGenerationError(f"Malformed Flink type list {value!r}")
    parts.append(final)
    return parts


def _row_field(value: str, original: str) -> tuple[str, str]:
    if value.startswith("`"):
        closing = value.find("`", 1)
        if closing < 0:
            raise AsyncAPIGenerationError(f"Malformed ROW field in Flink type {original!r}")
        name = value[1:closing]
        field_type = value[closing + 1 :].strip()
    else:
        match = re.match(r"([A-Za-z_][A-Za-z0-9_$]*)\s+(.+)$", value, flags=re.DOTALL)
        if match is None:
            raise AsyncAPIGenerationError(f"Malformed ROW field in Flink type {original!r}")
        name, field_type = match.group(1), match.group(2).strip()
    if not name or not field_type:
        raise AsyncAPIGenerationError(f"Malformed ROW field in Flink type {original!r}")
    return name, field_type


def _scalar_type_schema(value: str) -> dict[str, object]:
    upper = value.upper()
    if upper in {"STRING"}:
        return {"type": "string"}
    if re.fullmatch(r"(?:VARCHAR|CHAR)\s*(?:\(\s*[1-9]\d*\s*\))?", upper):
        return {"type": "string"}
    if upper in {"BOOLEAN", "BOOL"}:
        return {"type": "boolean"}
    if upper in {"TINYINT", "SMALLINT", "INT", "INTEGER"}:
        return {"type": "integer", "format": "int32"}
    if upper in {"BIGINT", "LONG"}:
        return {"type": "integer", "format": "int64"}
    if upper in {"FLOAT", "REAL"}:
        return {"type": "number", "format": "float"}
    if upper == "DOUBLE":
        return {"type": "number", "format": "double"}
    decimal_match = re.fullmatch(
        r"(?:DECIMAL|NUMERIC)(?:\s*\(\s*(\d+)\s*,\s*(\d+)\s*\))?",
        upper,
    )
    if decimal_match:
        if decimal_match.group(1) is not None:
            precision = int(decimal_match.group(1))
            scale = int(decimal_match.group(2))
            if precision < 1 or precision > 38 or scale > precision:
                raise AsyncAPIGenerationError(f"Invalid Flink decimal type {value!r}")
        return {"type": "number"}
    if upper == "DATE":
        return {"type": "string", "format": "date"}
    if re.fullmatch(r"TIME\s*(?:\(\s*[0-9]\s*\))?", upper):
        return {"type": "string", "format": "time"}
    if re.fullmatch(r"TIMESTAMP(?:_LTZ)?\s*(?:\(\s*[0-9]\s*\))?", upper):
        return {"type": "string", "format": "date-time"}
    if upper == "BYTES" or re.fullmatch(
        r"(?:BINARY|VARBINARY)\s*(?:\(\s*[1-9]\d*\s*\))?", upper
    ):
        return {"type": "string", "format": "byte"}
    raise AsyncAPIGenerationError(f"Unsupported or malformed Flink column type {value!r}")


def _kafka_channel_binding(
    model: Model,
    materialized: MaterializedType,
) -> dict[str, object] | None:
    topic_config = model.get_topic_config()
    if topic_config is None or materialized == MaterializedType.VIRTUAL_TOPIC:
        return None

    binding: dict[str, object] = {}
    if topic_config.partitions is not None:
        binding["partitions"] = _positive_int(
            topic_config.partitions, f"model '{model.name}' topic.partitions"
        )
    if topic_config.replication_factor is not None:
        binding["replicas"] = _positive_int(
            topic_config.replication_factor,
            f"model '{model.name}' topic.replication_factor",
        )
    topic_configuration = _kafka_topic_configuration(model.name, topic_config)
    if topic_configuration:
        binding["topicConfiguration"] = topic_configuration
    if not binding:
        return None
    binding["bindingVersion"] = KAFKA_BINDING_VERSION
    return binding


def _kafka_topic_configuration(model_name: str, topic: TopicConfig) -> dict[str, object]:
    result: dict[str, object] = {}
    for key in sorted(topic.config):
        value = topic.config[key]
        location = f"model '{model_name}' topic.config.{key}"
        if key == "cleanup.policy":
            result[key] = _cleanup_policy(value, location)
        elif key in _INTEGER_TOPIC_CONFIG:
            minimum = _INTEGER_TOPIC_CONFIG[key]
            result[key] = _bounded_int(value, minimum, location)
        elif key in _BOOLEAN_TOPIC_CONFIG:
            if not isinstance(value, bool):
                raise AsyncAPIGenerationError(f"{location} must be a boolean")
            result[key] = value
        elif key in _STRING_TOPIC_CONFIG:
            if not isinstance(value, str) or not value.strip():
                raise AsyncAPIGenerationError(f"{location} must be a non-empty string")
            result[key] = value
    return result


def _cleanup_policy(value: object, location: str) -> list[str]:
    if isinstance(value, str):
        policies = [part.strip() for part in value.split(",")]
    elif isinstance(value, list) and all(isinstance(part, str) for part in value):
        policies = [part.strip() for part in value]
    else:
        raise AsyncAPIGenerationError(f"{location} must be a string or list of strings")
    if not policies or any(policy not in {"compact", "delete"} for policy in policies):
        raise AsyncAPIGenerationError(
            f"{location} must contain only 'compact' and/or 'delete'"
        )
    return sorted(set(policies))


def _positive_int(value: object, location: str) -> int:
    return _bounded_int(value, 1, location)


def _bounded_int(value: object, minimum: int, location: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise AsyncAPIGenerationError(f"{location} must be an integer >= {minimum}")
    return value


def _validate_reference_semantics(document: Mapping[str, object]) -> None:
    channels = _mapping(document.get("channels"), "channels")
    operations = _mapping(document.get("operations"), "operations")
    components = _mapping(document.get("components"), "components")
    messages = _mapping(components.get("messages"), "components/messages")
    schemas = _mapping(components.get("schemas"), "components/schemas")

    _validate_ids(channels, "channel")
    _validate_ids(operations, "operation")
    _validate_ids(messages, "message")
    _validate_ids(schemas, "schema")

    for channel_id, channel_value in channels.items():
        channel = _mapping(channel_value, f"channels/{channel_id}")
        channel_messages = _mapping(
            channel.get("messages"), f"channels/{channel_id}/messages"
        )
        for message_id, reference_value in channel_messages.items():
            expected = f"#/components/messages/{message_id}"
            reference = _reference(reference_value, f"channels/{channel_id}/messages/{message_id}")
            if reference != expected or message_id not in messages:
                raise AsyncAPIValidationError(
                    f"Channel message {channel_id!r}/{message_id!r} must reference "
                    f"the matching local component message"
                )

    for message_id, message_value in messages.items():
        message = _mapping(message_value, f"components/messages/{message_id}")
        if "payload" not in message:
            continue
        payload_ref = _reference(message["payload"], f"components/messages/{message_id}/payload")
        prefix = "#/components/schemas/"
        schema_id = payload_ref.removeprefix(prefix)
        if not payload_ref.startswith(prefix) or not schema_id or schema_id not in schemas:
            raise AsyncAPIValidationError(
                f"Message {message_id!r} payload must reference an existing local component schema"
            )

    for operation_id, operation_value in operations.items():
        operation = _mapping(operation_value, f"operations/{operation_id}")
        channel_ref = _reference(operation.get("channel"), f"operations/{operation_id}/channel")
        channel_prefix = "#/channels/"
        channel_id = channel_ref.removeprefix(channel_prefix)
        if not channel_ref.startswith(channel_prefix) or channel_id not in channels:
            raise AsyncAPIValidationError(
                f"Operation {operation_id!r} must reference an existing root channel"
            )
        channel = _mapping(channels[channel_id], f"channels/{channel_id}")
        channel_messages = _mapping(
            channel.get("messages"), f"channels/{channel_id}/messages"
        )
        operation_messages = operation.get("messages")
        if not isinstance(operation_messages, list):
            raise AsyncAPIValidationError(
                f"Operation {operation_id!r} messages must be a list of references"
            )
        seen_refs: set[str] = set()
        for index, reference_value in enumerate(operation_messages):
            reference = _reference(
                reference_value, f"operations/{operation_id}/messages/{index}"
            )
            expected_prefix = f"#/channels/{channel_id}/messages/"
            message_id = reference.removeprefix(expected_prefix)
            if (
                not reference.startswith(expected_prefix)
                or message_id not in channel_messages
                or reference in seen_refs
            ):
                raise AsyncAPIValidationError(
                    f"Operation {operation_id!r} messages must be a unique subset of its channel messages"
                )
            seen_refs.add(reference)


def _mapping(value: object, location: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping) or not all(isinstance(key, str) for key in value):
        raise AsyncAPIValidationError(f"{location} must be an object with string keys")
    return value


def _reference(value: object, location: str) -> str:
    reference = _mapping(value, location)
    if set(reference) != {"$ref"} or not isinstance(reference["$ref"], str):
        raise AsyncAPIValidationError(f"{location} must be a single $ref object")
    return reference["$ref"]


def _validate_ids(values: Mapping[str, object], kind: str) -> None:
    seen: set[str] = set()
    for identifier in values:
        if not _COMPONENT_ID_RE.fullmatch(identifier):
            raise AsyncAPIValidationError(f"Invalid AsyncAPI {kind} identifier {identifier!r}")
        if identifier in seen:
            raise AsyncAPIValidationError(f"Duplicate AsyncAPI {kind} identifier {identifier!r}")
        seen.add(identifier)
