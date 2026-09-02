"""AsyncAPI 3.1 generation, validation, and CLI compatibility tests."""

from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest
import yaml
from click.testing import CliRunner

from streamt.cli import main
from streamt.core.models import StreamtProject
from streamt.docs.asyncapi import (
    ASYNCAPI_VERSION,
    AsyncAPIGenerationError,
    AsyncAPIValidationError,
    flink_type_to_asyncapi_schema,
    generate_asyncapi_document,
    validate_asyncapi_document,
)


def _project(**updates: object) -> StreamtProject:
    data: dict[str, object] = {
        "apiVersion": "streamt.dev/v1alpha1",
        "project": {
            "name": "payments-streams",
            "version": "2.3.0",
            "description": "Payment event contracts.",
        },
        "runtime": {"kafka": {"bootstrap_servers": "broker.example:9092"}},
        "sources": [
            {
                "name": "payments_raw",
                "topic": "payments.raw.v1",
                "description": "Raw payment events.",
                "columns": [
                    {"name": "payment_id", "type": "STRING", "required": True},
                    {"name": "amount", "type": "DECIMAL(12, 2)", "description": "Amount."},
                    {"name": "event_time", "type": "TIMESTAMP_LTZ(3)"},
                ],
            }
        ],
        "models": [
            {
                "name": "payments_clean",
                "description": "Validated payments.",
                "sql": 'SELECT payment_id, amount FROM {{ source("payments_raw") }}',
                "topic": {
                    "name": "payments.clean.v1",
                    "partitions": 12,
                    "replication_factor": 3,
                    "config": {
                        "cleanup.policy": "compact,delete",
                        "retention.ms": 604800000,
                        "compression.type": "zstd",
                    },
                },
                "contract": {
                    "enforced": True,
                    "columns": [
                        {
                            "name": "payment_id",
                            "type": "STRING",
                            "nullable": False,
                            "description": "Stable payment identifier.",
                        },
                        {"name": "amount", "type": "DECIMAL(12, 2)", "nullable": True},
                    ],
                },
            },
            {
                "name": "warehouse_sink",
                "from": "payments_clean",
                "sink": {"connector": "jdbc", "config": {}},
            },
        ],
    }
    data.update(updates)
    return StreamtProject.model_validate(data)


def _write_project(path: Path, project: StreamtProject) -> None:
    serialized = project.model_dump(
        mode="json",
        by_alias=True,
        exclude_none=True,
        exclude={"project_path"},
    )
    (path / "stream_project.yml").write_text(yaml.safe_dump(serialized, sort_keys=False))


def test_generates_officially_valid_v3_document_with_v3_reference_shape() -> None:
    document = generate_asyncapi_document(_project())

    assert document["asyncapi"] == ASYNCAPI_VERSION
    assert "servers" not in document
    channels = document["channels"]
    operations = document["operations"]
    assert isinstance(channels, dict)
    assert isinstance(operations, dict)
    assert channels["source.payments_raw"]["address"] == "payments.raw.v1"
    assert channels["model.payments_clean"]["address"] == "payments.clean.v1"
    assert "model.warehouse_sink" not in channels
    assert operations["receive.source.payments_raw"] == {
        "action": "receive",
        "title": "Receive payments_raw",
        "description": "The streamt project receives messages from `payments.raw.v1`.",
        "channel": {"$ref": "#/channels/source.payments_raw"},
        "messages": [
            {
                "$ref": (
                    "#/channels/source.payments_raw/messages/source.payments_raw.message"
                )
            }
        ],
    }
    assert operations["send.model.payments_clean"]["action"] == "send"


def test_virtual_topic_channel_uses_documented_gateway_alias() -> None:
    project = _project(
        runtime={
            "kafka": {"bootstrap_servers": "broker.example:9092"},
            "conduktor": {
                "gateway": {"admin_url": "https://gateway.example.test"}
            },
        },
        models=[
            {
                "name": "payments_public",
                "materialized": "virtual_topic",
                "gateway": {"virtual_topic": {"name": "payments.public"}},
                "sql": 'SELECT * FROM {{ source("payments_raw") }}',
            }
        ],
    )

    document = generate_asyncapi_document(project)

    assert document["channels"]["model.payments_public"]["address"] == "payments.public"
    validate_asyncapi_document(document)


def test_payload_contracts_descriptions_required_fields_and_kafka_binding() -> None:
    document = generate_asyncapi_document(_project())
    components = document["components"]
    assert isinstance(components, dict)
    schemas = components["schemas"]
    assert isinstance(schemas, dict)

    source_schema = schemas["source.payments_raw.payload"]
    assert source_schema["required"] == ["payment_id"]
    assert source_schema["properties"]["amount"] == {
        "type": "number",
        "description": "Amount.",
    }
    assert source_schema["properties"]["event_time"]["format"] == "date-time"

    model_schema = schemas["model.payments_clean.payload"]
    assert model_schema["required"] == ["payment_id"]
    assert model_schema["additionalProperties"] is False
    assert model_schema["properties"]["payment_id"]["description"] == (
        "Stable payment identifier."
    )

    channel = document["channels"]["model.payments_clean"]
    assert channel["bindings"] == {
        "kafka": {
            "partitions": 12,
            "replicas": 3,
            "topicConfiguration": {
                "cleanup.policy": ["compact", "delete"],
                "retention.ms": 604800000,
            },
            "bindingVersion": "0.5.0",
        }
    }
    assert "bindings" not in document["channels"]["source.payments_raw"]


def test_declared_column_without_type_does_not_invent_string_schema() -> None:
    project = _project(
        sources=[
            {
                "name": "raw",
                "topic": "raw",
                "columns": [
                    {
                        "name": "unknown_value",
                        "required": True,
                        "description": "Declared without a wire type.",
                    }
                ],
            }
        ],
        models=[],
    )

    document = generate_asyncapi_document(project)
    schema = document["components"]["schemas"]["source.raw.payload"]

    assert schema["properties"]["unknown_value"] == {
        "description": "Declared without a wire type."
    }
    assert schema["required"] == ["unknown_value"]


@pytest.mark.parametrize(
    ("flink_type", "expected"),
    [
        ("ARRAY<STRING>", {"type": "array", "items": {"type": "string"}}),
        (
            "MAP<STRING, ARRAY<BIGINT>>",
            {
                "type": "object",
                "additionalProperties": {
                    "type": "array",
                    "items": {"type": "integer", "format": "int64"},
                },
            },
        ),
        (
            "ROW<id STRING, metrics MAP<STRING, DOUBLE>>",
            {
                "type": "object",
                "properties": {
                    "id": {"type": "string"},
                    "metrics": {
                        "type": "object",
                        "additionalProperties": {"type": "number", "format": "double"},
                    },
                },
            },
        ),
    ],
)
def test_nested_flink_types_are_converted_without_string_fallbacks(
    flink_type: str, expected: dict[str, object]
) -> None:
    assert flink_type_to_asyncapi_schema(flink_type) == expected


@pytest.mark.parametrize(
    "flink_type",
    [
        "GEOGRAPHY",
        "MULTISET<STRING>",
        "DECIMAL(2, 4)",
        "ARRAY<>",
        "MAP<INT, STRING>",
        "ROW<id>",
        "ROW<id STRING, id BIGINT>",
    ],
)
def test_unsupported_or_malformed_types_fail_closed(flink_type: str) -> None:
    with pytest.raises(AsyncAPIGenerationError):
        flink_type_to_asyncapi_schema(flink_type)


def test_generated_identifier_collisions_fail_before_overwrite() -> None:
    project = _project(
        sources=[
            {"name": "raw/events", "topic": "one"},
            {"name": "raw events", "topic": "two"},
        ],
        models=[],
    )

    with pytest.raises(
        AsyncAPIGenerationError,
        match=(
            r"collide: channel 'source\.raw-events', message 'source\.raw-events\.message', "
            r"operation 'receive\.source\.raw-events', schema 'source\.raw-events\.payload'"
        ),
    ):
        generate_asyncapi_document(project)


def test_duplicate_columns_fail_before_property_overwrite() -> None:
    project = _project(
        sources=[
            {
                "name": "raw",
                "topic": "raw",
                "columns": [
                    {"name": "id", "type": "STRING"},
                    {"name": "id", "type": "BIGINT"},
                ],
            }
        ],
        models=[],
    )
    with pytest.raises(AsyncAPIGenerationError, match="Duplicate column 'id'"):
        generate_asyncapi_document(project)


def test_official_schema_validation_rejects_invalid_operation_action() -> None:
    document = generate_asyncapi_document(_project())
    invalid = copy.deepcopy(document)
    invalid["operations"]["send.model.payments_clean"]["action"] = "publish"

    with pytest.raises(AsyncAPIValidationError, match="schema validation failed"):
        validate_asyncapi_document(invalid)


def test_semantic_validation_rejects_operation_message_outside_channel() -> None:
    document = generate_asyncapi_document(_project())
    invalid = copy.deepcopy(document)
    invalid["operations"]["send.model.payments_clean"]["messages"] = [
        {"$ref": "#/channels/model.payments_clean/messages/source.payments_raw.message"}
    ]

    with pytest.raises(AsyncAPIValidationError, match="unique subset"):
        validate_asyncapi_document(invalid)


def test_generation_and_text_serialization_are_deterministic(tmp_path: Path) -> None:
    project = _project()
    _write_project(tmp_path, project)
    runner = CliRunner()

    first = runner.invoke(main, ["docs", "asyncapi", "-p", str(tmp_path)])
    second = runner.invoke(main, ["docs", "asyncapi", "-p", str(tmp_path)])

    assert first.exit_code == 0, first.output
    assert second.exit_code == 0, second.output
    assert first.output == second.output
    validate_asyncapi_document(json.loads(first.output))


def test_legacy_project_warning_does_not_corrupt_document_stdout(tmp_path: Path) -> None:
    project = _project()
    serialized = project.model_dump(
        mode="json",
        by_alias=True,
        exclude_none=True,
        exclude={"project_path", "api_version"},
    )
    serialized.pop("apiVersion", None)
    (tmp_path / "stream_project.yml").write_text(
        yaml.safe_dump(serialized, sort_keys=False)
    )

    result = CliRunner().invoke(main, ["docs", "asyncapi", "-p", str(tmp_path)])

    assert result.exit_code == 0, result.output
    assert json.loads(result.stdout)["asyncapi"] == ASYNCAPI_VERSION
    assert "apiVersion" in result.stderr


def test_structured_output_contains_document_and_counts(tmp_path: Path) -> None:
    _write_project(tmp_path, _project())
    result = CliRunner().invoke(
        main,
        ["--output", "json", "docs", "asyncapi", "-p", str(tmp_path)],
    )

    assert result.exit_code == 0, result.output
    envelope = json.loads(result.output)
    assert envelope["command"] == "docs asyncapi"
    assert envelope["data"]["asyncapi"] == ASYNCAPI_VERSION
    assert envelope["data"]["channels"] == 2
    assert envelope["data"]["operations"] == 2
    validate_asyncapi_document(envelope["data"]["document"])


def test_openapi_is_an_exact_compatibility_alias_with_truthful_help(tmp_path: Path) -> None:
    _write_project(tmp_path, _project())
    runner = CliRunner()

    canonical = runner.invoke(main, ["docs", "asyncapi", "-p", str(tmp_path)])
    alias = runner.invoke(main, ["docs", "openapi", "-p", str(tmp_path)])
    help_result = runner.invoke(main, ["docs", "openapi", "--help"])

    assert canonical.exit_code == alias.exit_code == 0
    assert canonical.output == alias.output
    assert json.loads(alias.output)["asyncapi"] == ASYNCAPI_VERSION
    assert "Deprecated alias" in help_result.output
    assert "does not emit OpenAPI" in help_result.output


def test_structured_generation_failure_uses_stable_error_code(tmp_path: Path) -> None:
    project = _project(
        sources=[
            {
                "name": "raw",
                "topic": "raw",
                "columns": [{"name": "location", "type": "GEOGRAPHY"}],
            }
        ],
        models=[],
    )
    _write_project(tmp_path, project)

    result = CliRunner().invoke(
        main,
        ["--output", "json", "docs", "asyncapi", "-p", str(tmp_path)],
    )

    assert result.exit_code == 1
    envelope = json.loads(result.stdout)
    assert envelope["status"] == "error"
    assert envelope["errors"][0]["code"] == "E504_ASYNCAPI_INVALID"
    assert "Unsupported or malformed" in envelope["errors"][0]["message"]
