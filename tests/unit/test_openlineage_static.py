"""Pure deterministic OpenLineage static export mapping tests."""

from __future__ import annotations

import json
from dataclasses import replace
from typing import cast

import pytest
from pydantic import SecretStr

from streamt.compiler.compiled_models import CompiledModelView
from streamt.compiler.manifest import Manifest
from streamt.compiler.model_resolution import ModelDependency, ResolvedModel
from streamt.core.models import (
    ColumnDefinition,
    ConnectionConfig,
    ContractColumn,
    FromRef,
    KafkaConfig,
    MaterializedType,
    Model,
    ModelContract,
    ProjectInfo,
    RuntimeConfig,
    SinkConfig,
    Source,
    StreamtProject,
    TopicConfig,
)
from streamt.integrations.openlineage.events import (
    DatasetIdentity,
    JobIdentity,
    RunIdentity,
    build_dataset_event,
    build_run_event,
)
from streamt.integrations.openlineage.namespaces import (
    OpenLineageNamespaceError,
    resolve_openlineage_namespaces,
)
from streamt.integrations.openlineage.static import (
    OpenLineageStaticError,
    StaticOpenLineageExport,
    build_static_export,
    serialize_static_jsonl,
    static_namespace_requirements,
)

EVENT_TIME = "2026-09-01T12:34:56Z"
KAFKA_NAMESPACE = "kafka://broker.example:9092"
GATEWAY_NAMESPACE = "kafka://gateway.example:6969"


def _project(*, sources: list[Source], models: list[Model]) -> StreamtProject:
    return StreamtProject(
        project=ProjectInfo(name="acme/project"),
        runtime=RuntimeConfig(
            kafka=KafkaConfig(
                bootstrap_servers="runtime-secret-broker:19092",
                sasl_password=SecretStr("runtime-password-secret"),
            )
        ),
        connections={
            "warehouse": ConnectionConfig(
                type="snowflake",
                config={"password": "connection-password-secret"},
            )
        },
        sources=sources,
        models=models,
    )


def _manifest() -> Manifest:
    return Manifest(
        version="1.0.0",
        project_name="acme/project",
        compiled_at=EVENT_TIME,
        artifacts={
            "flink_jobs": [{"sql": "SELECT 'manifest-sql-secret'"}],
            "connectors": [{"config": {"password": "manifest-password-secret"}}],
        },
    )


def _resolved(
    model: Model,
    materialized: MaterializedType,
    *dependencies: ModelDependency,
) -> ResolvedModel:
    return ResolvedModel(
        model=model.model_copy(deep=True),
        materialized=materialized,
        dependencies=dependencies,
    )


def _representative_compile() -> tuple[
    StreamtProject,
    Manifest,
    dict[str, ResolvedModel],
    dict[str, CompiledModelView],
]:
    source = Source(
        name="raw",
        topic="orders.raw.v1",
        description="Raw orders",
        owner="source-team",
        columns=[ColumnDefinition(name="id", type="BIGINT", description="Order ID")],
    )
    provision = Model(
        name="provision",
        materialized=MaterializedType.TOPIC,
        topic=TopicConfig(name="provisioned.v1"),
    )
    transform = Model(
        name="transform",
        materialized=MaterializedType.FLINK,
        sql="SELECT 'model-sql-secret' AS hidden FROM raw",
        description="Clean orders",
        owner="model-team",
        columns=[ColumnDefinition(name="fallback", type="STRING")],
        contract=ModelContract(
            columns=[
                ContractColumn(name="id", type="BIGINT", description="Stable ID"),
                ContractColumn(name="amount", type="DECIMAL(10, 2)"),
            ]
        ),
        topic=TopicConfig(name="orders.clean.v2"),
    )
    view = Model(
        name="view",
        materialized=MaterializedType.VIRTUAL_TOPIC,
        description="Filtered view",
        columns=[ColumnDefinition(name="id", type="BIGINT")],
        topic=TopicConfig(name="orders.clean.public"),
    )
    sink = Model(
        name="warehouse",
        materialized=MaterializedType.SINK,
        sink=SinkConfig(
            connector="snowflake-sink",
            connection="warehouse",
            config={"password": "model-password-secret"},
        ),
    )
    sink.from_ = [FromRef(ref="view")]
    project = _project(
        sources=[source],
        models=[provision, transform, view, sink],
    )
    resolved = {
        "provision": _resolved(provision, MaterializedType.TOPIC),
        "transform": _resolved(
            transform,
            MaterializedType.FLINK,
            ModelDependency(name="raw", kind="source"),
        ),
        "view": _resolved(
            view,
            MaterializedType.VIRTUAL_TOPIC,
            ModelDependency(name="transform", kind="model"),
        ),
        "warehouse": _resolved(
            sink,
            MaterializedType.SINK,
            ModelDependency(name="view", kind="model"),
        ),
    }
    compiled = {
        "provision": CompiledModelView(
            model_name="provision",
            materialized=MaterializedType.TOPIC,
            process_kind=None,
            output_kind="kafka",
            output_name="provisioned.v1",
            gateway_physical_input=None,
            connector_inputs=(),
        ),
        "transform": CompiledModelView(
            model_name="transform",
            materialized=MaterializedType.FLINK,
            process_kind="flink",
            output_kind="kafka",
            output_name="orders.clean.v2",
            gateway_physical_input=None,
            connector_inputs=(),
        ),
        "view": CompiledModelView(
            model_name="view",
            materialized=MaterializedType.VIRTUAL_TOPIC,
            process_kind="gateway",
            output_kind="gateway",
            output_name="orders.clean.public",
            gateway_physical_input="orders.clean.v2",
            connector_inputs=(),
        ),
        "warehouse": CompiledModelView(
            model_name="warehouse",
            materialized=MaterializedType.SINK,
            process_kind="connect",
            output_kind=None,
            output_name=None,
            gateway_physical_input=None,
            connector_inputs=("orders.clean.public",),
        ),
    }
    return project, _manifest(), resolved, compiled


def _build_representative() -> StaticOpenLineageExport:
    project, manifest, resolved, compiled = _representative_compile()
    return build_static_export(
        project,
        manifest,
        resolved,
        compiled,
        job_namespace="lineage.prod",
        kafka_namespace=KAFKA_NAMESPACE,
        gateway_namespace=GATEWAY_NAMESPACE,
    )


def _entity_events(
    export: StaticOpenLineageExport, key: str
) -> list[dict[str, object]]:
    return [event for event in export.events if key in event]


def _named_event(
    events: list[dict[str, object]], entity: str, name: str
) -> dict[str, object]:
    for event in events:
        value = cast(dict[str, object], event[entity])
        if value["name"] == name:
            return event
    raise AssertionError(f"No {entity} event named {name!r}")


def test_static_export_maps_inventory_facets_jobs_and_direct_dependencies() -> None:
    export = _build_representative()
    dataset_events = _entity_events(export, "dataset")
    job_events = _entity_events(export, "job")

    assert export.dataset_count == 4
    assert export.job_count == 3
    assert [event["eventTime"] for event in export.events] == [EVENT_TIME] * 7
    assert all("run" not in event and "eventType" not in event for event in export.events)

    dataset_names = {
        (
            cast(dict[str, object], event["dataset"])["namespace"],
            cast(dict[str, object], event["dataset"])["name"],
        )
        for event in dataset_events
    }
    assert dataset_names == {
        (KAFKA_NAMESPACE, "orders.raw.v1"),
        (KAFKA_NAMESPACE, "provisioned.v1"),
        (KAFKA_NAMESPACE, "orders.clean.v2"),
        (GATEWAY_NAMESPACE, "orders.clean.public"),
    }

    source_event = _named_event(dataset_events, "dataset", "orders.raw.v1")
    source_facets = cast(
        dict[str, dict[str, object]],
        cast(dict[str, object], source_event["dataset"])["facets"],
    )
    assert source_facets["documentation"]["description"] == "Raw orders"
    assert source_facets["ownership"]["owners"] == [{"name": "source-team"}]
    assert source_facets["datasetType"]["datasetType"] == "TOPIC"

    output_event = _named_event(dataset_events, "dataset", "orders.clean.v2")
    output_facets = cast(
        dict[str, dict[str, object]],
        cast(dict[str, object], output_event["dataset"])["facets"],
    )
    assert output_facets["schema"]["fields"] == [
        {
            "name": "id",
            "type": "BIGINT",
            "description": "Stable ID",
            "ordinal_position": 1,
        },
        {
            "name": "amount",
            "type": "DECIMAL(10, 2)",
            "ordinal_position": 2,
        },
    ]
    assert output_facets["ownership"]["owners"] == [{"name": "model-team"}]

    assert len(job_events) == 3
    assert not any(
        str(cast(dict[str, object], event["job"])["name"]).endswith("/provision")
        for event in job_events
    )
    transform_job = next(
        event
        for event in job_events
        if str(cast(dict[str, object], event["job"])["name"]).endswith("/transform")
    )
    assert cast(list[dict[str, object]], transform_job["inputs"]) == [
        {"namespace": KAFKA_NAMESPACE, "name": "orders.raw.v1"}
    ]
    assert cast(list[dict[str, object]], transform_job["outputs"]) == [
        {"namespace": KAFKA_NAMESPACE, "name": "orders.clean.v2"}
    ]
    view_job = next(
        event
        for event in job_events
        if str(cast(dict[str, object], event["job"])["name"]).endswith("/view")
    )
    assert cast(list[dict[str, object]], view_job["inputs"]) == [
        {"namespace": KAFKA_NAMESPACE, "name": "orders.clean.v2"}
    ]
    assert cast(list[dict[str, object]], view_job["outputs"]) == [
        {"namespace": GATEWAY_NAMESPACE, "name": "orders.clean.public"}
    ]
    sink_job = next(
        event
        for event in job_events
        if str(cast(dict[str, object], event["job"])["name"]).endswith("/warehouse")
    )
    assert cast(list[dict[str, object]], sink_job["inputs"]) == [
        {"namespace": GATEWAY_NAMESPACE, "name": "orders.clean.public"}
    ]
    assert "outputs" not in sink_job

    assert [(warning.code, warning.location) for warning in export.warnings] == [
        ("W110_OPENLINEAGE_SCHEMA_INCOMPLETE", "/models/0/columns"),
        ("W111_OPENLINEAGE_SINK_OUTPUT_OMITTED", "/models/3/sink"),
    ]


def test_static_export_serialization_is_canonical_and_excludes_secret_bearing_data() -> None:
    export = _build_representative()
    rendered = serialize_static_jsonl(export.events)
    lines = rendered.splitlines()

    assert len(lines) == len(export.events)
    assert rendered.endswith("\n")
    assert not rendered.endswith("\n\n")
    assert all(
        line
        == json.dumps(
            json.loads(line),
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        for line in lines
    )
    for secret in (
        "runtime-password-secret",
        "runtime-secret-broker",
        "connection-password-secret",
        "model-password-secret",
        "model-sql-secret",
        "manifest-sql-secret",
        "manifest-password-secret",
    ):
        assert secret not in rendered

    dataset_identities = [
        (
            cast(dict[str, object], event["dataset"])["namespace"],
            cast(dict[str, object], event["dataset"])["name"],
        )
        for event in _entity_events(export, "dataset")
    ]
    assert dataset_identities == sorted(dataset_identities)
    job_identities = [
        (
            cast(dict[str, object], event["job"])["namespace"],
            cast(dict[str, object], event["job"])["name"],
        )
        for event in _entity_events(export, "job")
    ]
    assert job_identities == sorted(job_identities)


def test_static_serialization_canonicalizes_input_event_order() -> None:
    export = _build_representative()

    assert serialize_static_jsonl(tuple(reversed(export.events))) == (
        serialize_static_jsonl(export.events)
    )


def test_static_serialization_reports_invalid_event_index_before_sequence_validation() -> None:
    event = build_dataset_event(
        event_time=EVENT_TIME,
        dataset=DatasetIdentity(KAFKA_NAMESPACE, "valid"),
    )
    invalid = dict(event)
    invalid["eventTime"] = "not-a-time"

    with pytest.raises(OpenLineageStaticError) as raised:
        serialize_static_jsonl([event, invalid])

    assert raised.value.location == "/events/1"


def test_static_serialization_rejects_run_events() -> None:
    run_event = build_run_event(
        event_time=EVENT_TIME,
        event_type="OTHER",
        run=RunIdentity("12345678-1234-5678-9234-567812345678"),
        job=JobIdentity("jobs", "streamt/project/commands/test"),
    )

    with pytest.raises(OpenLineageStaticError, match="RunEvent") as raised:
        serialize_static_jsonl([run_event])

    assert raised.value.location == "/events/0"


@pytest.mark.parametrize(
    "compiled_at",
    ["2026-09-01T12:34:56Z", "2026-09-01T12:34:56.123456+00:00"],
)
def test_static_export_preserves_utc_manifest_compile_time(compiled_at: str) -> None:
    project, manifest, resolved, compiled = _representative_compile()
    manifest.compiled_at = compiled_at

    export = build_static_export(
        project,
        manifest,
        resolved,
        compiled,
        job_namespace="jobs",
        kafka_namespace=KAFKA_NAMESPACE,
        gateway_namespace=GATEWAY_NAMESPACE,
    )

    assert {event["eventTime"] for event in export.events} == {compiled_at}


@pytest.mark.parametrize(
    "compiled_at",
    ["2026-09-01T12:34:56", "2026-09-01T12:34:56+05:00"],
)
def test_static_export_rejects_non_utc_manifest_compile_time(compiled_at: str) -> None:
    project, manifest, resolved, compiled = _representative_compile()
    manifest.compiled_at = compiled_at

    with pytest.raises(OpenLineageStaticError) as raised:
        build_static_export(
            project,
            manifest,
            resolved,
            compiled,
            job_namespace="jobs",
            kafka_namespace=KAFKA_NAMESPACE,
            gateway_namespace=GATEWAY_NAMESPACE,
        )

    assert raised.value.location == "manifest.compiled_at"


def test_empty_contract_is_authoritative_and_blank_type_is_omitted() -> None:
    model = Model(
        name="empty_contract",
        materialized=MaterializedType.TOPIC,
        columns=[ColumnDefinition(name="fallback", type=" ")],
        contract=ModelContract(columns=[]),
    )
    project = _project(sources=[], models=[model])
    resolved = {"empty_contract": _resolved(model, MaterializedType.TOPIC)}
    compiled = {
        "empty_contract": CompiledModelView(
            model_name="empty_contract",
            materialized=MaterializedType.TOPIC,
            process_kind=None,
            output_kind="kafka",
            output_name="empty_contract",
            gateway_physical_input=None,
            connector_inputs=(),
        )
    }

    export = build_static_export(
        project,
        _manifest(),
        resolved,
        compiled,
        job_namespace="jobs",
        kafka_namespace=KAFKA_NAMESPACE,
        gateway_namespace=None,
    )
    dataset = cast(dict[str, object], export.events[0]["dataset"])
    facets = cast(dict[str, object], dataset["facets"])
    assert "schema" not in facets
    assert export.warnings[0].location == "/models/0/contract/columns"

    model.contract = None
    export_with_columns = build_static_export(
        project,
        _manifest(),
        {"empty_contract": _resolved(model, MaterializedType.TOPIC)},
        compiled,
        job_namespace="jobs",
        kafka_namespace=KAFKA_NAMESPACE,
        gateway_namespace=None,
    )
    dataset = cast(dict[str, object], export_with_columns.events[0]["dataset"])
    fields = cast(
        list[dict[str, object]],
        cast(dict[str, dict[str, object]], dataset["facets"])["schema"]["fields"],
    )
    assert fields == [{"name": "fallback", "ordinal_position": 1}]


@pytest.mark.parametrize("description", ["", "   "])
def test_blank_column_description_fails_at_declaration_location(description: str) -> None:
    source = Source(
        name="raw",
        topic="raw",
        columns=[ColumnDefinition(name="id", type="BIGINT", description=description)],
    )
    project = _project(sources=[source], models=[])

    with pytest.raises(OpenLineageStaticError) as raised:
        build_static_export(
            project,
            _manifest(),
            {},
            {},
            job_namespace="jobs",
            kafka_namespace=KAFKA_NAMESPACE,
            gateway_namespace=None,
        )
    assert raised.value.location == "/sources/0/columns/0/description"


def test_dataset_collision_and_connector_reconciliation_fail_closed() -> None:
    project, manifest, resolved, compiled = _representative_compile()
    project.models[0].topic.name = "orders.raw.v1"  # type: ignore[union-attr]
    compiled["provision"] = replace(
        compiled["provision"], output_name="orders.raw.v1"
    )
    with pytest.raises(OpenLineageStaticError, match="same OpenLineage dataset"):
        build_static_export(
            project,
            manifest,
            resolved,
            compiled,
            job_namespace="jobs",
            kafka_namespace=KAFKA_NAMESPACE,
            gateway_namespace=GATEWAY_NAMESPACE,
        )

    project, manifest, resolved, compiled = _representative_compile()
    compiled["warehouse"] = replace(
        compiled["warehouse"], connector_inputs=("wrong-topic",)
    )
    with pytest.raises(OpenLineageStaticError) as raised:
        build_static_export(
            project,
            manifest,
            resolved,
            compiled,
            job_namespace="jobs",
            kafka_namespace=KAFKA_NAMESPACE,
            gateway_namespace=GATEWAY_NAMESPACE,
        )
    assert raised.value.location == "/models/3/connector_inputs"


def test_compile_view_coverage_and_materialization_mismatch_fail_closed() -> None:
    project, manifest, resolved, compiled = _representative_compile()
    del compiled["transform"]
    with pytest.raises(OpenLineageStaticError) as raised:
        build_static_export(
            project,
            manifest,
            resolved,
            compiled,
            job_namespace="jobs",
            kafka_namespace=KAFKA_NAMESPACE,
            gateway_namespace=GATEWAY_NAMESPACE,
        )
    assert raised.value.location == "compiled_models"

    project, manifest, resolved, compiled = _representative_compile()
    resolved["transform"] = _resolved(
        project.models[1], MaterializedType.TOPIC, ModelDependency("raw", "source")
    )
    with pytest.raises(OpenLineageStaticError) as raised:
        build_static_export(
            project,
            manifest,
            resolved,
            compiled,
            job_namespace="jobs",
            kafka_namespace=KAFKA_NAMESPACE,
            gateway_namespace=GATEWAY_NAMESPACE,
        )
    assert raised.value.location == "/models/1/materialized"


def test_gateway_dependency_cardinality_and_chaining_fail_closed() -> None:
    project, manifest, resolved, compiled = _representative_compile()
    resolved["view"] = _resolved(
        project.models[2],
        MaterializedType.VIRTUAL_TOPIC,
        ModelDependency("raw", "source"),
        ModelDependency("transform", "model"),
    )
    with pytest.raises(OpenLineageStaticError) as raised:
        build_static_export(
            project,
            manifest,
            resolved,
            compiled,
            job_namespace="jobs",
            kafka_namespace=KAFKA_NAMESPACE,
            gateway_namespace=GATEWAY_NAMESPACE,
        )
    assert raised.value.location == "/models/2/dependencies"

    project, manifest, resolved, compiled = _representative_compile()
    second = Model(
        name="second_view",
        materialized=MaterializedType.VIRTUAL_TOPIC,
        columns=[ColumnDefinition(name="id", type="BIGINT")],
    )
    project.models.append(second)
    resolved["second_view"] = _resolved(
        second,
        MaterializedType.VIRTUAL_TOPIC,
        ModelDependency("view", "model"),
    )
    compiled["second_view"] = CompiledModelView(
        model_name="second_view",
        materialized=MaterializedType.VIRTUAL_TOPIC,
        process_kind="gateway",
        output_kind="gateway",
        output_name="second.public",
        gateway_physical_input="orders.clean.public",
        connector_inputs=(),
    )
    with pytest.raises(OpenLineageStaticError, match="Chained Gateway") as raised:
        build_static_export(
            project,
            manifest,
            resolved,
            compiled,
            job_namespace="jobs",
            kafka_namespace=KAFKA_NAMESPACE,
            gateway_namespace=GATEWAY_NAMESPACE,
        )
    assert raised.value.location == "/models/4/dependencies"


def test_static_event_bytes_are_stable_under_declaration_reordering() -> None:
    project, manifest, resolved, compiled = _representative_compile()
    first = build_static_export(
        project,
        manifest,
        resolved,
        compiled,
        job_namespace="jobs",
        kafka_namespace=KAFKA_NAMESPACE,
        gateway_namespace=GATEWAY_NAMESPACE,
    )
    project.sources.reverse()
    project.models.reverse()
    second = build_static_export(
        project,
        manifest,
        resolved,
        compiled,
        job_namespace="jobs",
        kafka_namespace=KAFKA_NAMESPACE,
        gateway_namespace=GATEWAY_NAMESPACE,
    )
    assert serialize_static_jsonl(first.events) == serialize_static_jsonl(second.events)


def test_sequence_validation_rejects_duplicate_static_identities() -> None:
    event = build_dataset_event(
        event_time=EVENT_TIME,
        dataset=DatasetIdentity(KAFKA_NAMESPACE, "duplicate"),
    )
    with pytest.raises(OpenLineageStaticError) as raised:
        serialize_static_jsonl([event, event])
    assert raised.value.location == "/events"


def test_namespace_requirements_and_resolution_are_strict_and_lazy() -> None:
    project, _manifest_value, _resolved_value, compiled = _representative_compile()
    requirements = static_namespace_requirements(project, compiled)
    assert requirements.kafka is True
    assert requirements.gateway is True

    namespaces = resolve_openlineage_namespaces(
        job_namespace=" jobs ",
        kafka_namespace=None,
        gateway_namespace=None,
        kafka_bootstrap=" broker:9092 ",
        gateway_bootstrap="gateway:6969",
        require_kafka=True,
        require_gateway=True,
    )
    assert namespaces.job == " jobs "
    assert namespaces.kafka == "kafka://broker:9092"
    assert namespaces.gateway == "kafka://gateway:6969"

    unused = resolve_openlineage_namespaces(
        job_namespace="jobs",
        kafka_namespace=None,
        gateway_namespace=None,
        kafka_bootstrap="one:9092,two:9092",
        gateway_bootstrap=None,
        require_kafka=False,
        require_gateway=False,
    )
    assert unused.kafka is None
    assert unused.gateway is None

    with pytest.raises(OpenLineageNamespaceError) as raised:
        resolve_openlineage_namespaces(
            job_namespace="jobs",
            kafka_namespace="not-kafka",
            gateway_namespace=None,
            kafka_bootstrap="valid:9092",
            gateway_bootstrap=None,
            require_kafka=False,
            require_gateway=False,
        )
    assert raised.value.location == "kafka_namespace"

    with pytest.raises(OpenLineageNamespaceError) as raised:
        resolve_openlineage_namespaces(
            job_namespace="jobs",
            kafka_namespace=None,
            gateway_namespace=None,
            kafka_bootstrap="one:9092,two:9092",
            gateway_bootstrap=None,
            require_kafka=True,
            require_gateway=False,
        )
    assert raised.value.location == "runtime.kafka.bootstrap_servers"
