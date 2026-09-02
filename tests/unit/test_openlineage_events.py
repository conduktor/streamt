"""Focused tests for pure OpenLineage identity and event construction."""

from __future__ import annotations

from dataclasses import FrozenInstanceError
from typing import cast

import pytest

from streamt.integrations.openlineage import (
    EVENT_SCHEMA_URLS,
    FACET_SCHEMA_URLS,
    OPENLINEAGE_PRODUCER,
    DatasetIdentity,
    JobIdentity,
    OpenLineageConstructionError,
    RunIdentity,
    build_dataset_event,
    build_job_event,
    build_run_event,
    command_job_name,
    encode_job_segment,
    kafka_namespace_from_bootstrap,
    model_job_name,
    standard_facet,
    validate_kafka_namespace,
)

EVENT_TIME = "2026-09-01T12:34:56Z"
RUN_ID = "12345678-1234-5678-9234-567812345678"


def test_identity_records_and_url_tables_are_immutable() -> None:
    job = JobIdentity("acme.lineage", "streamt/acme/models/orders")

    with pytest.raises(FrozenInstanceError):
        job.name = "changed"  # type: ignore[misc]
    with pytest.raises(TypeError):
        EVENT_SCHEMA_URLS["job"] = "changed"  # type: ignore[index]
    with pytest.raises(TypeError):
        FACET_SCHEMA_URLS["job"]["jobType"] = "changed"  # type: ignore[index]


@pytest.mark.parametrize(
    ("value", "encoded"),
    [
        ("orders", "orders"),
        ("Orders.v2_~", "Orders.v2_~"),
        ("a/b%c", "a%2Fb%25c"),
        ("café 東京", "caf%C3%A9%20%E6%9D%B1%E4%BA%AC"),
    ],
)
def test_job_segment_percent_encoding_is_canonical(value: str, encoded: str) -> None:
    assert encode_job_segment(value) == encoded


def test_stable_job_names_have_fixed_separators() -> None:
    assert model_job_name("my/project", "model%one") == (
        "streamt/my%2Fproject/models/model%25one"
    )
    assert command_job_name("My Project", "apply") == (
        "streamt/My%20Project/commands/apply"
    )
    assert command_job_name("My Project", "test") == (
        "streamt/My%20Project/commands/test"
    )


@pytest.mark.parametrize(
    "namespace",
    [
        "kafka://broker.internal:9092",
        "kafka://BROKER.internal:19092",
        "kafka://[2001:db8::1]:9092",
    ],
)
def test_explicit_kafka_namespace_is_preserved(namespace: str) -> None:
    assert validate_kafka_namespace(namespace) == namespace
    assert DatasetIdentity(namespace, "topic-a").namespace == namespace


@pytest.mark.parametrize(
    "namespace",
    [
        "https://broker:9092",
        "kafka://broker",
        "kafka://user:password@broker:9092",
        "kafka://broker:9092/path",
        "kafka://broker:9092?cluster=one",
        "kafka://broker:9092?",
        "kafka://broker:9092#fragment",
        "kafka://broker:9092#",
        "kafka://one:9092,two:9092",
        "kafka://broker:0",
    ],
)
def test_invalid_kafka_namespaces_fail_without_echoing_values(namespace: str) -> None:
    with pytest.raises(OpenLineageConstructionError) as captured:
        validate_kafka_namespace(namespace)

    assert namespace not in str(captured.value)
    assert "password" not in str(captured.value)


def test_bootstrap_derivation_is_narrow_and_strips_boundary_whitespace() -> None:
    assert kafka_namespace_from_bootstrap("  broker.internal:9092  ") == (
        "kafka://broker.internal:9092"
    )
    for ambiguous in (
        "one:9092,two:9092",
        "kafka://broker:9092",
        "broker internal:9092",
    ):
        with pytest.raises(OpenLineageConstructionError):
            kafka_namespace_from_bootstrap(ambiguous)


def test_command_job_name_rejects_unimplemented_commands_at_runtime() -> None:
    with pytest.raises(OpenLineageConstructionError, match="not supported"):
        command_job_name("acme", "plan")  # type: ignore[arg-type]


def test_standard_facet_controls_metadata_and_orders_values() -> None:
    facet = standard_facet(
        "job",
        "jobType",
        {
            "jobType": "MODEL",
            "processingType": "STREAMING",
            "integration": "STREAMT",
        },
    )

    assert list(facet) == [
        "_producer",
        "_schemaURL",
        "integration",
        "jobType",
        "processingType",
    ]
    assert facet["_producer"] == OPENLINEAGE_PRODUCER
    assert facet["_schemaURL"] == FACET_SCHEMA_URLS["job"]["jobType"]
    with pytest.raises(OpenLineageConstructionError):
        standard_facet("job", "schema", {})
    with pytest.raises(OpenLineageConstructionError):
        standard_facet("job", "jobType", {"_producer": "other"})


def test_builders_emit_exact_event_shapes_and_sorted_dataset_references() -> None:
    namespace = "kafka://broker:9092"
    first = DatasetIdentity(namespace, "a-topic")
    second = DatasetIdentity(namespace, "z-topic")
    job = JobIdentity("lineage.acme", model_job_name("acme", "orders"))
    dataset_type = standard_facet("dataset", "datasetType", {"datasetType": "TOPIC"})
    job_type = standard_facet(
        "job",
        "jobType",
        {"processingType": "STREAMING", "integration": "STREAMT", "jobType": "MODEL"},
    )

    dataset_event = build_dataset_event(
        event_time=EVENT_TIME,
        dataset=first,
        facets={"datasetType": dataset_type},
    )
    job_event = build_job_event(
        event_time=EVENT_TIME,
        job=job,
        facets={"jobType": job_type},
        inputs=[second, first],
    )

    assert dataset_event == {
        "eventTime": EVENT_TIME,
        "producer": OPENLINEAGE_PRODUCER,
        "schemaURL": EVENT_SCHEMA_URLS["dataset"],
        "dataset": {
            "namespace": namespace,
            "name": "a-topic",
            "facets": {"datasetType": dataset_type},
        },
    }
    assert job_event["schemaURL"] == EVENT_SCHEMA_URLS["job"]
    inputs = cast(list[dict[str, object]], job_event["inputs"])
    assert [item["name"] for item in inputs] == [
        "a-topic",
        "z-topic",
    ]
    assert all("facets" not in item for item in inputs)
    assert "run" not in job_event
    assert "eventType" not in job_event


def test_run_builder_requires_explicit_identity_and_lifecycle_type() -> None:
    job = JobIdentity("lineage.acme", command_job_name("acme", "apply"))
    error_facet = standard_facet(
        "run",
        "errorMessage",
        {"message": "safe failure", "programmingLanguage": "PYTHON"},
    )

    event = build_run_event(
        event_time=EVENT_TIME,
        event_type="FAIL",
        run=RunIdentity(RUN_ID),
        job=job,
        run_facets={"errorMessage": error_facet},
    )

    assert event["schemaURL"] == EVENT_SCHEMA_URLS["run"]
    assert event["eventType"] == "FAIL"
    assert event["run"] == {
        "runId": RUN_ID,
        "facets": {"errorMessage": error_facet},
    }


def test_duplicate_or_overlapping_dataset_identities_fail_closed() -> None:
    dataset = DatasetIdentity("kafka://broker:9092", "orders")
    job = JobIdentity("lineage.acme", model_job_name("acme", "orders"))

    with pytest.raises(OpenLineageConstructionError, match="unique"):
        build_job_event(event_time=EVENT_TIME, job=job, inputs=[dataset, dataset])
    with pytest.raises(OpenLineageConstructionError, match="both"):
        build_job_event(
            event_time=EVENT_TIME,
            job=job,
            inputs=[dataset],
            outputs=[dataset],
        )


@pytest.mark.parametrize("value", ["", " ", "not-a-uuid", "12345678123456789234567812345678"])
def test_invalid_run_identity_is_rejected(value: str) -> None:
    with pytest.raises(OpenLineageConstructionError):
        RunIdentity(value)
