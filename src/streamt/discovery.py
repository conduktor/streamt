"""Read-only discovery of existing streaming resources.

The discovery boundary intentionally exposes metadata reads only.  Callers can
reuse it to describe existing infrastructure without receiving access to any
create, update, or delete operation.
"""

from __future__ import annotations

import fnmatch
import logging
import re
from dataclasses import dataclass
from typing import Optional, Protocol

from streamt.deployer.kafka import TopicState
from streamt.deployer.schema_registry import SchemaState

logger = logging.getLogger(__name__)

INTERNAL_TOPIC_PREFIXES = ("__", "_schemas", "_confluent", "_streamt-connect-")


class KafkaDiscoveryReader(Protocol):
    """The Kafka metadata reads required by discovery."""

    def list_topic_names(self) -> list[str]:
        """Return topic names visible to the configured principal."""

    def get_topic_state(self, topic_name: str) -> TopicState:
        """Return metadata and configuration for one topic."""


class SchemaDiscoveryReader(Protocol):
    """The Schema Registry metadata read required by discovery."""

    def get_schema_state(self, subject: str) -> SchemaState:
        """Return the latest state for one subject."""


@dataclass(frozen=True)
class DiscoveredTopic:
    """A source declaration and the Kafka metadata that produced it."""

    source: dict[str, object]
    topic: str
    partitions: Optional[int]
    replication_factor: Optional[int]

    @property
    def column_count(self) -> int:
        """Return the number of inferred columns in the source declaration."""
        columns = self.source.get("columns")
        return len(columns) if isinstance(columns, list) else 0


def sanitize_source_name(topic: str) -> str:
    """Convert a topic name to the source-name form used by discovery."""
    return re.sub(r"[^a-zA-Z0-9_]", "_", topic)


def is_internal_topic(topic: str) -> bool:
    """Return whether a topic is reserved for Kafka or platform internals."""
    return any(topic.startswith(prefix) for prefix in INTERNAL_TOPIC_PREFIXES)


def select_topic_names(
    topic_names: list[str],
    *,
    include: Optional[str] = None,
    exclude: Optional[str] = None,
) -> list[str]:
    """Filter and deterministically order topic names for discovery."""
    selected = (topic for topic in topic_names if not is_internal_topic(topic))
    if include:
        selected = (topic for topic in selected if fnmatch.fnmatch(topic, include))
    if exclude:
        selected = (topic for topic in selected if not fnmatch.fnmatch(topic, exclude))
    return sorted(selected)


def avro_type_to_flink(avro_type: object) -> str:
    """Convert an Avro type to a Flink SQL type string."""
    if isinstance(avro_type, dict):
        logical = avro_type.get("logicalType")
        if logical == "timestamp-millis":
            return "TIMESTAMP(3)"
        if logical == "timestamp-micros":
            return "TIMESTAMP(6)"
        if logical == "date":
            return "DATE"
        if logical == "decimal":
            precision = avro_type.get("precision", 10)
            scale = avro_type.get("scale", 2)
            return f"DECIMAL({precision},{scale})"
        base = avro_type.get("type", "string")
        return avro_type_to_flink(base)
    if isinstance(avro_type, list):
        non_null = [item for item in avro_type if item != "null"]
        if non_null:
            return avro_type_to_flink(non_null[0])
        return "STRING"
    mapping = {
        "string": "STRING",
        "int": "INT",
        "long": "BIGINT",
        "float": "FLOAT",
        "double": "DOUBLE",
        "boolean": "BOOLEAN",
        "bytes": "BYTES",
    }
    return mapping.get(str(avro_type), "STRING")


def json_schema_type_to_flink(prop: dict[str, object]) -> str:
    """Convert a JSON Schema property to a Flink SQL type string."""
    fmt = prop.get("format")
    if fmt == "date-time":
        return "TIMESTAMP(3)"
    if fmt == "date":
        return "DATE"
    schema_type = prop.get("type", "string")
    if isinstance(schema_type, list):
        non_null = [item for item in schema_type if item != "null"]
        schema_type = non_null[0] if non_null else "string"
    mapping = {
        "string": "STRING",
        "integer": "INT",
        "number": "DOUBLE",
        "boolean": "BOOLEAN",
    }
    if not isinstance(schema_type, str):
        return "STRING"
    return mapping.get(schema_type, "STRING")


def extract_columns_from_json_schema(schema: dict[str, object]) -> list[dict[str, object]]:
    """Extract source columns from the top-level JSON Schema properties."""
    columns: list[dict[str, object]] = []
    properties = schema.get("properties", {})
    if not isinstance(properties, dict):
        return columns
    required = schema.get("required", [])
    required_fields = (
        {item for item in required if isinstance(item, str)}
        if isinstance(required, list)
        else set()
    )
    for name, prop in properties.items():
        if not isinstance(name, str) or not isinstance(prop, dict):
            continue
        column: dict[str, object] = {
            "name": name,
            "type": json_schema_type_to_flink(prop),
        }
        description = prop.get("description")
        if description:
            column["description"] = description
        if name in required_fields:
            column["required"] = True
        columns.append(column)
    return columns


def extract_columns_from_avro(schema: dict[str, object]) -> list[dict[str, object]]:
    """Extract source columns from the top-level fields of an Avro record."""
    columns: list[dict[str, object]] = []
    fields = schema.get("fields", [])
    if not isinstance(fields, list):
        return columns
    for field in fields:
        if not isinstance(field, dict):
            continue
        name = field.get("name")
        if not isinstance(name, str) or not name:
            continue
        column: dict[str, object] = {
            "name": name,
            "type": avro_type_to_flink(field.get("type", "string")),
        }
        description = field.get("doc")
        if description:
            column["description"] = description
        columns.append(column)
    return columns


def _schema_columns(schema_state: SchemaState, topic: str) -> list[dict[str, object]]:
    schema = schema_state.schema
    if not schema_state.exists or schema is None:
        return []
    if schema_state.schema_type == "PROTOBUF":
        logger.debug("Protobuf schema for '%s' — skipping column extraction", topic)
        return []
    if not isinstance(schema, dict):
        return []
    if schema_state.schema_type == "AVRO" and "fields" in schema:
        return extract_columns_from_avro(schema)
    if schema_state.schema_type == "JSON" and "properties" in schema:
        return extract_columns_from_json_schema(schema)
    return []


def discover_topics(
    kafka: KafkaDiscoveryReader,
    schema_registry: Optional[SchemaDiscoveryReader] = None,
    *,
    include: Optional[str] = None,
    exclude: Optional[str] = None,
) -> list[DiscoveredTopic]:
    """Discover Kafka topics and optional value-schema columns without mutation."""
    topics = select_topic_names(kafka.list_topic_names(), include=include, exclude=exclude)
    discovered: list[DiscoveredTopic] = []

    for topic in topics:
        state = kafka.get_topic_state(topic)
        source: dict[str, object] = {
            "name": sanitize_source_name(topic),
            "topic": topic,
            "description": f"Discovered from Kafka ({state.partitions} partitions)",
        }

        if schema_registry is not None:
            try:
                columns = _schema_columns(
                    schema_registry.get_schema_state(f"{topic}-value"), topic
                )
                if columns:
                    source["columns"] = columns
            except Exception as exc:
                logger.debug("Schema discovery failed for topic '%s': %s", topic, exc)

        discovered.append(
            DiscoveredTopic(
                source=source,
                topic=topic,
                partitions=state.partitions,
                replication_factor=state.replication_factor,
            )
        )

    return discovered
