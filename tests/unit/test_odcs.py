"""ODCS 3.1 pure generation and offline validation tests."""

from __future__ import annotations

import base64
import copy
import gzip
import hashlib
import json
import socket
from collections.abc import Iterator, Mapping, Sequence
from importlib.resources import files
from uuid import UUID

import pytest
import requests
from jsonschema import Draft201909Validator

import streamt.docs.odcs as odcs
from streamt.core.models import Model, StreamtProject
from streamt.docs.odcs import (
    ODCS_API_VERSION,
    ODCS_INCOMPLETE_SCHEMA_WARNING,
    ODCS_OFFICIAL_SCHEMA_SHA256,
    ODCS_OFFICIAL_SCHEMA_SIZE,
    ODCS_SCHEMA_RESOURCE,
    ODCSGenerationError,
    ODCSValidationError,
    flink_type_to_odcs_logical_type,
    generate_odcs_document,
    validate_odcs_document,
)

CONTRACT_ID = "urn:acme:data-contract:payments-streams"


def _project(**updates: object) -> StreamtProject:
    data: dict[str, object] = {
        "apiVersion": "streamt.dev/v1alpha1",
        "project": {
            "name": "payments-streams",
            "version": "2.3.0",
            "description": "ROOT_DESCRIPTION_OMITTED",
        },
        "runtime": {
            "kafka": {
                "bootstrap_servers": "BROKER_ENDPOINT_OMITTED:9092",
            }
        },
        "connections": {
            "warehouse": {
                "type": "jdbc",
                "config": {
                    "url": "JDBC_ENDPOINT_OMITTED",
                    "password": "CONNECTION_SECRET_OMITTED",
                },
            }
        },
        "sources": [
            {
                "name": "zeta_raw",
                "topic": "payments.raw.v1",
                "description": "Raw payment events.",
                "owner": "SOURCE_OWNER_OMITTED",
                "tags": ["payments", "raw"],
                "columns": [
                    {
                        "name": "payment_id",
                        "type": "VARCHAR(64)",
                        "classification": "confidential",
                        "description": "Payment identifier.",
                        "required": True,
                    },
                    {
                        "name": "payload",
                        "type": "ROW<id STRING>",
                    },
                ],
            },
            {
                "name": "alpha_reference",
                "topic": "payments.reference.v1",
                "schema": {
                    "fields": [
                        {
                            "name": "reference_id",
                            "type": "BIGINT",
                            "required": False,
                        }
                    ]
                },
            },
        ],
        "models": [
            {
                "name": "payments_clean",
                "description": "Validated payment events.",
                "owner": "MODEL_OWNER_OMITTED",
                "tags": ["clean", "payments"],
                "materialized": "topic",
                "sql": "SELECT 'SQL_OMITTED_MARKER' FROM ignored",
                "topic": {"name": "payments.clean.v1"},
                "primary_key": ["tenant_id", "payment_id"],
                "columns": [
                    {
                        "name": "ignored_model_column",
                        "type": "STRING",
                    }
                ],
                "security": {
                    "classification": {
                        "payment_id": "highly_sensitive",
                    }
                },
                "contract": {
                    "enforced": False,
                    "columns": [
                        {
                            "name": "payment_id",
                            "type": "STRING",
                            "nullable": False,
                            "description": "Stable payment identifier.",
                        },
                        {
                            "name": "tenant_id",
                            "type": "BIGINT",
                            "nullable": True,
                        },
                        {
                            "name": "opaque_payload",
                            "type": "BYTES",
                        },
                    ],
                },
            },
            {
                "name": "archive_sink",
                "description": "Warehouse archive.",
                "materialized": "sink",
                "from": "payments_clean",
                "sink": {
                    "connector": "jdbc",
                    "connection": "warehouse",
                    "config": {"table": "SINK_DESTINATION_OMITTED"},
                },
                "columns": [
                    {
                        "name": "payment_id",
                        "type": "STRING",
                        "required": False,
                    }
                ],
            },
            {
                "name": "declared_columns",
                "materialized": "topic",
                "columns": [
                    {
                        "name": "event_date",
                        "type": "DATE",
                        "classification": "internal",
                        "required": True,
                    }
                ],
            },
        ],
        "tests": [
            {
                "name": "TEST_NAME_OMITTED",
                "model": "payments_clean",
                "type": "schema",
                "assertions": [],
            }
        ],
        "exposures": [
            {
                "name": "EXPOSURE_NAME_OMITTED",
                "type": "application",
                "owner": "EXPOSURE_OWNER_OMITTED",
                "consumes": [{"ref": "payments_clean"}],
            }
        ],
    }
    data.update(updates)
    return StreamtProject.model_validate(data)


def _schema_by_name(document: Mapping[str, object], name: str) -> Mapping[str, object]:
    schema = document["schema"]
    assert isinstance(schema, list)
    for item in schema:
        assert isinstance(item, Mapping)
        if item.get("name") == name:
            return item
    raise AssertionError(f"Missing ODCS schema object {name!r}")


def _properties_by_name(schema: Mapping[str, object]) -> dict[str, Mapping[str, object]]:
    properties = schema.get("properties", [])
    assert isinstance(properties, list)
    result: dict[str, Mapping[str, object]] = {}
    for item in properties:
        assert isinstance(item, Mapping)
        name = item.get("name")
        assert isinstance(name, str)
        result[name] = item
    return result


def _references(value: object) -> Iterator[str]:
    if isinstance(value, Mapping):
        for key, child in value.items():
            if key == "$ref" and isinstance(child, str):
                yield child
            yield from _references(child)
    elif isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        for child in value:
            yield from _references(child)


def test_pinned_official_schema_resource_has_exact_bytes_and_only_local_refs() -> None:
    encoded = b"".join(
        files("streamt.docs.schemas").joinpath(ODCS_SCHEMA_RESOURCE).read_bytes().split()
    )
    raw_schema = gzip.decompress(base64.b64decode(encoded, validate=True))

    assert len(raw_schema) == ODCS_OFFICIAL_SCHEMA_SIZE == 86_441
    assert hashlib.sha256(raw_schema).hexdigest() == ODCS_OFFICIAL_SCHEMA_SHA256
    loaded = json.loads(raw_schema)
    Draft201909Validator.check_schema(loaded)
    assert loaded["$schema"] == "https://json-schema.org/draft/2019-09/schema"
    assert all(reference.startswith("#/") for reference in _references(loaded))


def test_generates_one_officially_valid_project_document_in_canonical_order() -> None:
    result = generate_odcs_document(
        _project(),
        contract_id=CONTRACT_ID,
        status="active",
    )
    document = result.document

    assert list(document) == [
        "apiVersion",
        "kind",
        "id",
        "name",
        "version",
        "status",
        "schema",
    ]
    assert document["apiVersion"] == ODCS_API_VERSION
    assert document["kind"] == "DataContract"
    assert document["id"] == CONTRACT_ID
    assert document["name"] == "payments-streams"
    assert document["version"] == "2.3.0"
    assert document["status"] == "active"
    assert [item["name"] for item in document["schema"]] == [
        "alpha_reference",
        "zeta_raw",
        "archive_sink",
        "declared_columns",
        "payments_clean",
    ]
    validate_odcs_document(document)


def test_source_mapping_and_schema_fields_promotion_are_exact() -> None:
    document = generate_odcs_document(
        _project(), contract_id=CONTRACT_ID, status="active"
    ).document
    source = _schema_by_name(document, "zeta_raw")

    assert source["physicalName"] == "payments.raw.v1"
    assert source["physicalType"] == "topic"
    assert source["logicalType"] == "object"
    assert source["description"] == "Raw payment events."
    assert source["tags"] == ["payments", "raw"]
    assert source["customProperties"] == [
        {"property": "streamtResourceType", "value": "source"}
    ]
    properties = _properties_by_name(source)
    assert properties["payment_id"] == {
        "id": properties["payment_id"]["id"],
        "name": "payment_id",
        "physicalType": "VARCHAR(64)",
        "logicalType": "string",
        "description": "Payment identifier.",
        "classification": "confidential",
        "required": True,
    }
    assert properties["payload"] == {
        "id": properties["payload"]["id"],
        "name": "payload",
        "physicalType": "ROW<id STRING>",
        "required": False,
    }

    promoted = _schema_by_name(document, "alpha_reference")
    promoted_property = _properties_by_name(promoted)["reference_id"]
    assert promoted_property["physicalType"] == "BIGINT"
    assert promoted_property["logicalType"] == "integer"
    assert promoted_property["required"] is False


def test_model_contract_precedes_columns_and_preserves_contract_semantics() -> None:
    document = generate_odcs_document(
        _project(), contract_id=CONTRACT_ID, status="active"
    ).document
    model = _schema_by_name(document, "payments_clean")

    assert model["physicalName"] == "payments.clean.v1"
    assert model["physicalType"] == "topic"
    assert model["tags"] == ["clean", "payments"]
    assert model["customProperties"] == [
        {"property": "streamtResourceType", "value": "model"},
        {"property": "streamtContractEnforced", "value": False},
    ]
    properties = _properties_by_name(model)
    assert list(properties) == ["payment_id", "tenant_id", "opaque_payload"]
    assert "ignored_model_column" not in properties
    assert properties["payment_id"]["required"] is True
    assert properties["payment_id"]["classification"] == "highly_sensitive"
    assert properties["payment_id"]["primaryKey"] is True
    assert properties["payment_id"]["primaryKeyPosition"] == 2
    assert properties["tenant_id"]["required"] is False
    assert properties["tenant_id"]["primaryKeyPosition"] == 1
    assert "required" not in properties["opaque_payload"]
    assert properties["opaque_payload"]["physicalType"] == "BYTES"
    assert "logicalType" not in properties["opaque_payload"]


def test_explicit_model_columns_are_used_and_sink_physical_destination_is_omitted() -> None:
    document = generate_odcs_document(
        _project(), contract_id=CONTRACT_ID, status="active"
    ).document
    declared = _schema_by_name(document, "declared_columns")
    event_date = _properties_by_name(declared)["event_date"]
    assert event_date["logicalType"] == "date"
    assert event_date["classification"] == "internal"
    assert event_date["required"] is True

    sink = _schema_by_name(document, "archive_sink")
    assert "physicalName" not in sink
    assert "physicalType" not in sink
    assert _properties_by_name(sink)["payment_id"]["required"] is False


def test_sink_detection_never_invokes_sql_materialization_inference(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail_materialization_inference(_model: Model) -> None:
        raise AssertionError("ODCS export inspected SQL to infer materialization")

    monkeypatch.setattr(Model, "get_materialized", fail_materialization_inference)
    project = _project(
        sources=[],
        models=[
            {
                "name": "implicit_output",
                "sql": "SELECT COUNT(*) FROM events GROUP BY account_id",
                "columns": [{"name": "count", "type": "BIGINT"}],
            },
            {
                "name": "structural_sink",
                "from": "implicit_output",
                "columns": [{"name": "count", "type": "BIGINT"}],
            },
        ],
        tests=[],
        exposures=[],
    )

    document = generate_odcs_document(
        project, contract_id=CONTRACT_ID, status="active"
    ).document

    output = _schema_by_name(document, "implicit_output")
    assert output["physicalType"] == "topic"
    assert output["physicalName"] == "implicit_output"
    sink = _schema_by_name(document, "structural_sink")
    assert "physicalType" not in sink
    assert "physicalName" not in sink


@pytest.mark.parametrize(
    ("flink_type", "expected"),
    [
        ("STRING", "string"),
        ("CHAR(4)", "string"),
        ("varchar(255)", "string"),
        ("VARCHAR(2147483647)", "string"),
        ("TINYINT", "integer"),
        ("SMALLINT", "integer"),
        ("INT", "integer"),
        ("INTEGER", "integer"),
        ("BIGINT", "integer"),
        ("DECIMAL(12, 2)", "number"),
        ("DECIMAL(38, 38)", "number"),
        ("NUMERIC", "number"),
        ("REAL", "number"),
        ("FLOAT", "number"),
        ("DOUBLE", "number"),
        ("BOOLEAN", "boolean"),
        ("BOOL", "boolean"),
        ("DATE", "date"),
        ("TIME(0)", "time"),
        ("TIME(3)", "time"),
        ("TIMESTAMP(3)", "timestamp"),
        ("TIMESTAMP_LTZ(9)", "timestamp"),
        ("BYTES", None),
        ("BINARY(16)", None),
        ("LONG", None),
        ("ARRAY<STRING>", None),
        ("MAP<STRING, BIGINT>", None),
        ("ROW<id STRING>", None),
        ("GEOGRAPHY", None),
        ("CHAR", None),
        ("VARCHAR(0)", None),
        ("VARCHAR(2147483648)", None),
        ("VARCHAR(foo)", None),
        ("DECIMAL(foo)", None),
        ("DECIMAL(2, 4)", None),
        ("DECIMAL(2, 4, 6)", None),
        ("DECIMAL(0, 0)", None),
        ("DECIMAL(39, 0)", None),
        ("TIME(foo)", None),
        ("TIME(999)", None),
        ("TIMESTAMP(10)", None),
        ("", None),
    ],
)
def test_flink_type_mapping_is_conservative(
    flink_type: str,
    expected: str | None,
) -> None:
    assert flink_type_to_odcs_logical_type(flink_type) == expected


def test_unknown_physical_type_is_preserved_without_logical_fallback() -> None:
    project = _project(
        sources=[
            {
                "name": "geo",
                "topic": "geo.v1",
                "columns": [{"name": "point", "type": "GEOGRAPHY"}],
            }
        ],
        models=[],
    )
    document = generate_odcs_document(
        project, contract_id=CONTRACT_ID, status="proposed"
    ).document
    point = _properties_by_name(_schema_by_name(document, "geo"))["point"]
    assert point["physicalType"] == "GEOGRAPHY"
    assert "logicalType" not in point


def test_root_metadata_is_explicit_and_version_override_is_exact() -> None:
    document = generate_odcs_document(
        _project(),
        contract_id="  exact contract ID  ",
        status="custom-status",
        contract_version="release-candidate-7",
    ).document

    assert document["id"] == "  exact contract ID  "
    assert document["status"] == "custom-status"
    assert document["version"] == "release-candidate-7"


@pytest.mark.parametrize(
    ("contract_id", "status", "version"),
    [
        ("", "active", "1.0.0"),
        ("   ", "active", "1.0.0"),
        (CONTRACT_ID, "", "1.0.0"),
        (CONTRACT_ID, "\t", "1.0.0"),
        (CONTRACT_ID, "active", ""),
        (CONTRACT_ID, "active", "  "),
    ],
)
def test_blank_required_contract_metadata_fails(
    contract_id: str,
    status: str,
    version: str,
) -> None:
    with pytest.raises(ODCSGenerationError, match="non-whitespace"):
        generate_odcs_document(
            _project(),
            contract_id=contract_id,
            status=status,
            contract_version=version,
        )


def test_missing_project_and_explicit_version_fails() -> None:
    project = _project(project={"name": "payments-streams"})
    with pytest.raises(ODCSGenerationError, match="contract version is required"):
        generate_odcs_document(project, contract_id=CONTRACT_ID, status="active")


def test_empty_project_fails_instead_of_emitting_an_empty_schema() -> None:
    project = _project(sources=[], models=[], tests=[], exposures=[])
    with pytest.raises(ODCSGenerationError, match="at least one declared source or model"):
        generate_odcs_document(project, contract_id=CONTRACT_ID, status="active")


def test_incomplete_schemas_return_typed_deterministic_warnings() -> None:
    project = _project(
        sources=[{"name": "raw", "topic": "raw.v1"}],
        models=[{"name": "output", "materialized": "topic"}],
        tests=[],
        exposures=[],
    )
    result = generate_odcs_document(project, contract_id=CONTRACT_ID, status="draft")

    assert result.warnings == (
        odcs.ODCSExportWarning(
            code=ODCS_INCOMPLETE_SCHEMA_WARNING,
            message=(
                "Source 'raw' has no declared export columns; "
                "the ODCS schema object omits properties"
            ),
            location="source.raw",
        ),
        odcs.ODCSExportWarning(
            code=ODCS_INCOMPLETE_SCHEMA_WARNING,
            message=(
                "Model 'output' has no declared export columns; "
                "the ODCS schema object omits properties"
            ),
            location="model.output",
        ),
    )
    assert "properties" not in _schema_by_name(result.document, "raw")
    assert "properties" not in _schema_by_name(result.document, "output")


def test_ids_and_document_are_stable_under_source_and_model_reordering() -> None:
    project = _project()
    reordered = _project(
        sources=list(reversed(project.sources)),
        models=list(reversed(project.models)),
    )

    first = generate_odcs_document(
        project, contract_id=CONTRACT_ID, status="active"
    ).document
    second = generate_odcs_document(
        reordered, contract_id=CONTRACT_ID, status="active"
    ).document
    assert first == second

    source = _schema_by_name(first, "alpha_reference")
    reference_id = _properties_by_name(source)["reference_id"]
    assert source["id"] == "8765c6e7-94d8-5103-a67c-c4a9783fec0c"
    assert reference_id["id"] == "cfc1e46e-bbd7-5a0a-9388-9a223688e224"

    changed = generate_odcs_document(
        project,
        contract_id="urn:acme:data-contract:other",
        status="active",
    ).document
    assert _schema_by_name(changed, "alpha_reference")["id"] != source["id"]


@pytest.mark.parametrize(
    ("updates", "message"),
    [
        (
            {
                "sources": [
                    {"name": "duplicate", "topic": "one"},
                    {"name": "duplicate", "topic": "two"},
                ],
                "models": [],
            },
            "logical name collision",
        ),
        (
            {
                "sources": [{"name": "same", "topic": "one"}],
                "models": [{"name": "same", "materialized": "sink"}],
            },
            "logical name collision",
        ),
        (
            {
                "sources": [{"name": "source", "topic": "shared"}],
                "models": [
                    {
                        "name": "model",
                        "materialized": "topic",
                        "topic": {"name": "shared"},
                    }
                ],
            },
            "physical topic collision",
        ),
        (
            {
                "sources": [
                    {
                        "name": "duplicate_columns",
                        "topic": "columns",
                        "columns": [
                            {"name": "id", "type": "STRING"},
                            {"name": "id", "type": "BIGINT"},
                        ],
                    }
                ],
                "models": [],
            },
            "Duplicate property",
        ),
        (
            {
                "sources": [],
                "models": [
                    {
                        "name": "missing_pk",
                        "materialized": "topic",
                        "primary_key": ["missing"],
                        "columns": [{"name": "id", "type": "STRING"}],
                    }
                ],
            },
            "primary key references properties not present",
        ),
        (
            {
                "sources": [],
                "models": [
                    {
                        "name": "duplicate_pk",
                        "materialized": "topic",
                        "primary_key": ["id", "id"],
                        "columns": [{"name": "id", "type": "STRING"}],
                    }
                ],
            },
            "Duplicate primary-key property",
        ),
    ],
)
def test_collisions_and_invalid_primary_keys_fail_closed(
    updates: dict[str, object],
    message: str,
) -> None:
    project = _project(tests=[], exposures=[], **updates)
    with pytest.raises(ODCSGenerationError, match=message):
        generate_odcs_document(project, contract_id=CONTRACT_ID, status="active")


def test_conflicting_explicit_classifications_fail_closed() -> None:
    project = _project(
        sources=[],
        models=[
            {
                "name": "classified",
                "materialized": "topic",
                "columns": [
                    {
                        "name": "email",
                        "type": "STRING",
                        "classification": "confidential",
                    }
                ],
                "security": {"classification": {"email": "highly_sensitive"}},
            }
        ],
        tests=[],
        exposures=[],
    )
    with pytest.raises(ODCSGenerationError, match="Conflicting classifications"):
        generate_odcs_document(project, contract_id=CONTRACT_ID, status="active")


def test_generated_uuid_collision_fails_closed(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(odcs, "uuid5", lambda _namespace, _name: UUID(int=0))
    project = _project(
        sources=[
            {
                "name": "events",
                "topic": "events.v1",
                "columns": [{"name": "id", "type": "STRING"}],
            }
        ],
        models=[],
    )

    with pytest.raises(ODCSGenerationError, match="Generated ODCS ID collision"):
        generate_odcs_document(project, contract_id=CONTRACT_ID, status="active")


def test_official_and_local_semantic_validation_fail_closed() -> None:
    document = generate_odcs_document(
        _project(), contract_id=CONTRACT_ID, status="active"
    ).document

    invalid_status = copy.deepcopy(document)
    invalid_status["status"] = 3
    with pytest.raises(ODCSValidationError, match="schema validation failed at /status"):
        validate_odcs_document(invalid_status)

    old_version = copy.deepcopy(document)
    old_version["apiVersion"] = "v3.0.2"
    with pytest.raises(ODCSValidationError, match=r"apiVersion must be 'v3\.1\.0'"):
        validate_odcs_document(old_version)

    empty_schema = copy.deepcopy(document)
    empty_schema["schema"] = []
    with pytest.raises(ODCSValidationError, match="non-empty array"):
        validate_odcs_document(empty_schema)


def test_generation_is_offline_and_never_exposes_unmapped_or_secret_values(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    odcs._schema_cache_clear()

    def fail_network(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("ODCS generation attempted network access")

    monkeypatch.setattr(socket, "getaddrinfo", fail_network)
    monkeypatch.setattr(socket, "create_connection", fail_network)
    monkeypatch.setattr(requests.sessions.Session, "request", fail_network)

    result = generate_odcs_document(
        _project(), contract_id=CONTRACT_ID, status="active"
    )
    rendered = json.dumps(
        {
            "document": result.document,
            "warnings": [warning.__dict__ for warning in result.warnings],
        },
        sort_keys=True,
    )
    for forbidden in (
        "ROOT_DESCRIPTION_OMITTED",
        "BROKER_ENDPOINT_OMITTED",
        "JDBC_ENDPOINT_OMITTED",
        "CONNECTION_SECRET_OMITTED",
        "SOURCE_OWNER_OMITTED",
        "MODEL_OWNER_OMITTED",
        "SQL_OMITTED_MARKER",
        "SINK_DESTINATION_OMITTED",
        "TEST_NAME_OMITTED",
        "EXPOSURE_NAME_OMITTED",
        "EXPOSURE_OWNER_OMITTED",
    ):
        assert forbidden not in rendered


def test_empty_contract_is_authoritative_and_does_not_fall_back_to_model_columns() -> None:
    project = _project(
        sources=[],
        models=[
            {
                "name": "empty_contract",
                "materialized": "topic",
                "columns": [{"name": "must_not_leak", "type": "STRING"}],
                "contract": {"enforced": True, "columns": []},
            }
        ],
        tests=[],
        exposures=[],
    )
    result = generate_odcs_document(project, contract_id=CONTRACT_ID, status="draft")
    schema = _schema_by_name(result.document, "empty_contract")

    assert "properties" not in schema
    assert schema["customProperties"] == [
        {"property": "streamtResourceType", "value": "model"},
        {"property": "streamtContractEnforced", "value": True},
    ]
    assert result.warnings[0].location == "model.empty_contract"
