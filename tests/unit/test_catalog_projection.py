"""Tests for the private, immutable neutral catalog projection."""

from __future__ import annotations

import json
from dataclasses import FrozenInstanceError, asdict, replace
from pathlib import Path
from typing import Any, cast

import pytest
import yaml

from streamt.compiler import Compiler
from streamt.compiler.compiled_models import CompiledModelView
from streamt.compiler.model_resolution import ModelDependency, ResolvedModel
from streamt.core.models import MaterializedType, StreamtProject
from streamt.core.parser import ProjectParser
from streamt.integrations.catalog.model import (
    CatalogContractSummary,
    CatalogDataset,
    CatalogDependency,
    CatalogOwnerLabel,
    CatalogProcess,
    CatalogProjectionError,
    CatalogSnapshot,
    build_catalog_snapshot,
)


def _compile_project(
    tmp_path: Path,
) -> tuple[
    StreamtProject,
    dict[str, ResolvedModel],
    dict[str, CompiledModelView],
]:
    config: dict[str, object] = {
        "project": {
            "name": "payments-catalog",
            "version": "1.0.0",
            "description": "Payment streams",
        },
        "runtime": {
            "kafka": {
                "bootstrap_servers": "secret-broker.example:9092",
                "security_protocol": "SASL_SSL",
                "sasl_mechanism": "PLAIN",
                "sasl_username": "runtime-user-secret",
                "sasl_password": "runtime-password-secret",
            },
            "conduktor": {
                "gateway": {"proxy_bootstrap": "secret-gateway.example:6969"}
            },
        },
        "connections": {
            "warehouse": {
                "type": "snowflake",
                "config": {"password": "connection-password-secret"},
            }
        },
        "sources": [
            {
                "name": "raw_orders",
                "description": "Raw orders",
                "topic": "orders.raw.v1",
                "owner": "source-team",
                "tags": ["raw", "payments"],
                "columns": [
                    {
                        "name": "id",
                        "type": "BIGINT",
                        "description": "schema-description-secret",
                    }
                ],
            }
        ],
        "models": [
            {
                "name": "plain_topic",
                "description": "Provisioned topic",
                "topic": {"name": "orders.plain.v1"},
            },
            {
                "name": "filtered_orders",
                "description": "Filtered orders",
                "sql": (
                    "SELECT id, 'compiled-sql-secret' AS marker "
                    'FROM {{ source("raw_orders") }} WHERE id > 0'
                ),
                "gateway": {"virtual_topic": {"name": "orders.filtered.virtual"}},
                "owner": "gateway-team",
                "tags": ["payments", "filtered"],
                "contract": {
                    "enforced": True,
                    "columns": [{"name": "contract-column-secret"}],
                },
            },
            {
                "name": "orders_total",
                "description": "Order totals",
                "sql": (
                    "SELECT id, COUNT(*) AS total "
                    'FROM {{ source("raw_orders") }} GROUP BY id'
                ),
                "topic": {"name": "orders.total.v1"},
                "owner": "flink-team",
                "tags": ["aggregate"],
                "contract": {
                    "enforced": False,
                    "columns": [{"name": "another-contract-secret"}],
                },
            },
            {
                "name": "warehouse_sink",
                "description": "Warehouse delivery",
                "from": [{"ref": "orders_total"}],
                "sink": {
                    "connector": "snowflake-sink",
                    "connection": "warehouse",
                    "config": {"api.token": "connector-token-secret"},
                },
                "owner": "sink-team",
            },
        ],
        "exposures": [
            {
                "name": "checkout_dashboard",
                "type": "dashboard",
                "description": "exposure-description-secret",
                "url": "https://secret-dashboard.example",
                "consumes": [{"ref": "orders_total"}],
            }
        ],
    }
    (tmp_path / "stream_project.yml").write_text(
        yaml.safe_dump(config),
        encoding="utf-8",
    )
    project = ProjectParser(tmp_path).parse()
    compiler = Compiler(project)
    compiler.compile(dry_run=True)
    return project, dict(compiler.resolved_models), dict(compiler.compiled_models)


def _snapshot(tmp_path: Path) -> tuple[
    StreamtProject,
    dict[str, ResolvedModel],
    dict[str, CompiledModelView],
    CatalogSnapshot,
]:
    project, resolved, compiled = _compile_project(tmp_path)
    snapshot = build_catalog_snapshot(project, resolved, compiled, "production")
    return project, resolved, compiled, snapshot


def test_representative_real_compile_projects_exact_allowlisted_facts(
    tmp_path: Path,
) -> None:
    project, resolved, compiled, snapshot = _snapshot(tmp_path)

    assert snapshot == CatalogSnapshot(
        project_name="payments-catalog",
        project_description="Payment streams",
        effective_environment="production",
        datasets=(
            CatalogDataset(
                logical_kind="source",
                logical_name="raw_orders",
                transport="kafka",
                physical_name="orders.raw.v1",
                description="Raw orders",
                tags=("raw", "payments"),
                owner=CatalogOwnerLabel("source-team"),
                contract=None,
            ),
            CatalogDataset(
                logical_kind="model",
                logical_name="filtered_orders",
                transport="gateway",
                physical_name="orders.filtered.virtual",
                description="Filtered orders",
                tags=("payments", "filtered"),
                owner=CatalogOwnerLabel("gateway-team"),
                contract=CatalogContractSummary("enforced"),
            ),
            CatalogDataset(
                logical_kind="model",
                logical_name="orders_total",
                transport="kafka",
                physical_name="orders.total.v1",
                description="Order totals",
                tags=("aggregate",),
                owner=CatalogOwnerLabel("flink-team"),
                contract=CatalogContractSummary("declared"),
            ),
            CatalogDataset(
                logical_kind="model",
                logical_name="plain_topic",
                transport="kafka",
                physical_name="orders.plain.v1",
                description="Provisioned topic",
                tags=(),
                owner=None,
                contract=None,
            ),
        ),
        processes=(
            CatalogProcess(
                logical_name="filtered_orders",
                process_kind="gateway",
                description="Filtered orders",
                tags=("payments", "filtered"),
                owner=CatalogOwnerLabel("gateway-team"),
                dependencies=(CatalogDependency("source", "raw_orders"),),
            ),
            CatalogProcess(
                logical_name="orders_total",
                process_kind="flink",
                description="Order totals",
                tags=("aggregate",),
                owner=CatalogOwnerLabel("flink-team"),
                dependencies=(CatalogDependency("source", "raw_orders"),),
            ),
            CatalogProcess(
                logical_name="warehouse_sink",
                process_kind="connect",
                description="Warehouse delivery",
                tags=(),
                owner=CatalogOwnerLabel("sink-team"),
                dependencies=(CatalogDependency("model", "orders_total"),),
            ),
        ),
        omitted_exposure_names=("checkout_dashboard",),
    )
    assert snapshot.version == 1
    assert project.models[0].name == "plain_topic"
    assert resolved["filtered_orders"].dependencies == (
        ModelDependency("raw_orders", "source"),
    )
    assert compiled["filtered_orders"].process_kind == "gateway"


def test_projection_is_secret_neutral(tmp_path: Path) -> None:
    _project, _resolved, _compiled, snapshot = _snapshot(tmp_path)

    rendered = json.dumps(asdict(snapshot), ensure_ascii=False, sort_keys=True)
    for forbidden in (
        "secret-broker.example",
        "secret-gateway.example",
        "runtime-user-secret",
        "runtime-password-secret",
        "connection-password-secret",
        "compiled-sql-secret",
        "connector-token-secret",
        "schema-description-secret",
        "contract-column-secret",
        "another-contract-secret",
        "exposure-description-secret",
        "secret-dashboard.example",
        "bootstrap_servers",
        "api.token",
        "snowflake-sink",
    ):
        assert forbidden not in rendered


def test_projection_defensively_copies_and_freezes_nested_metadata(
    tmp_path: Path,
) -> None:
    project, resolved, _compiled, snapshot = _snapshot(tmp_path)
    original = asdict(snapshot)

    project.project.name = "mutated-project"
    project.sources[0].topic = "mutated.topic"
    project.sources[0].tags.append("mutated-tag")
    project.sources[0].owner = "mutated-owner"
    project.models[1].description = "mutated description"
    resolved["filtered_orders"].model.tags.append("mutated-resolved-tag")

    assert asdict(snapshot) == original
    assert isinstance(snapshot.datasets, tuple)
    assert isinstance(snapshot.processes, tuple)
    assert isinstance(snapshot.datasets[0].tags, tuple)
    assert isinstance(snapshot.processes[0].dependencies, tuple)
    with pytest.raises(FrozenInstanceError):
        snapshot.project_name = "replacement"  # type: ignore[misc]
    with pytest.raises(FrozenInstanceError):
        snapshot.datasets[0].physical_name = "replacement"  # type: ignore[misc]


@pytest.mark.parametrize("mapping_name", ["resolved", "compiled"])
@pytest.mark.parametrize("change", ["missing", "extra"])
def test_model_mapping_coverage_is_exact(
    tmp_path: Path,
    mapping_name: str,
    change: str,
) -> None:
    project, resolved, compiled = _compile_project(tmp_path)
    if mapping_name == "resolved":
        if change == "missing":
            del resolved["filtered_orders"]
        else:
            resolved["extra"] = next(iter(resolved.values()))
    else:
        if change == "missing":
            del compiled["filtered_orders"]
        else:
            compiled["extra"] = next(iter(compiled.values()))

    with pytest.raises(CatalogProjectionError) as raised:
        build_catalog_snapshot(project, resolved, compiled, "production")

    assert raised.value.location == f"{mapping_name}_models"


def test_inner_view_identities_and_materialization_must_match(tmp_path: Path) -> None:
    project, resolved, compiled = _compile_project(tmp_path)
    compiled["filtered_orders"] = replace(
        compiled["filtered_orders"],
        model_name="another_model",
    )
    with pytest.raises(CatalogProjectionError) as raised:
        build_catalog_snapshot(project, resolved, compiled, "production")
    assert raised.value.location == "/models/1/name"

    project, resolved, compiled = _compile_project(tmp_path)
    resolved["filtered_orders"] = replace(
        resolved["filtered_orders"],
        materialized=MaterializedType.FLINK,
    )
    with pytest.raises(CatalogProjectionError) as raised:
        build_catalog_snapshot(project, resolved, compiled, "production")
    assert raised.value.location == "/models/1/materialized"


def test_mixed_project_and_resolution_metadata_fails_closed(tmp_path: Path) -> None:
    project, resolved, compiled = _compile_project(tmp_path)
    resolved["filtered_orders"].model.owner = "different-owner"

    with pytest.raises(CatalogProjectionError) as raised:
        build_catalog_snapshot(project, resolved, compiled, "production")

    assert raised.value.location == "/models/1/resolved"
    assert "different-owner" not in str(raised.value)


def test_physical_dataset_collisions_fail_without_merging(tmp_path: Path) -> None:
    project, resolved, compiled = _compile_project(tmp_path)
    compiled["orders_total"] = replace(
        compiled["orders_total"],
        output_name="orders.raw.v1",
    )

    with pytest.raises(CatalogProjectionError) as raised:
        build_catalog_snapshot(project, resolved, compiled, "production")

    assert raised.value.location == "/models/2/output"
    assert "orders.raw.v1" not in str(raised.value)


def test_same_physical_name_on_distinct_transports_remains_distinct(tmp_path: Path) -> None:
    project, resolved, compiled = _compile_project(tmp_path)
    compiled["filtered_orders"] = replace(
        compiled["filtered_orders"],
        output_name="orders.raw.v1",
    )

    snapshot = build_catalog_snapshot(project, resolved, compiled, "production")

    matching = [
        dataset
        for dataset in snapshot.datasets
        if dataset.physical_name == "orders.raw.v1"
    ]
    assert [(item.transport, item.logical_kind) for item in matching] == [
        ("kafka", "source"),
        ("gateway", "model"),
    ]


def test_gateway_requires_one_exact_resolved_physical_input(tmp_path: Path) -> None:
    project, resolved, compiled = _compile_project(tmp_path)
    compiled["filtered_orders"] = replace(
        compiled["filtered_orders"],
        gateway_physical_input="other.topic",
    )
    with pytest.raises(CatalogProjectionError) as raised:
        build_catalog_snapshot(project, resolved, compiled, "production")
    assert raised.value.location == "/models/1/gateway/physical_input"

    project, resolved, compiled = _compile_project(tmp_path)
    resolved["filtered_orders"] = replace(
        resolved["filtered_orders"],
        dependencies=(
            ModelDependency("raw_orders", "source"),
            ModelDependency("plain_topic", "model"),
        ),
    )
    with pytest.raises(CatalogProjectionError) as raised:
        build_catalog_snapshot(project, resolved, compiled, "production")
    assert raised.value.location == "/models/1/dependencies"


def test_connect_inputs_must_exactly_match_direct_dependency_order(tmp_path: Path) -> None:
    project, resolved, compiled = _compile_project(tmp_path)
    compiled["warehouse_sink"] = replace(
        compiled["warehouse_sink"],
        connector_inputs=("orders.raw.v1",),
    )

    with pytest.raises(CatalogProjectionError) as raised:
        build_catalog_snapshot(project, resolved, compiled, "production")

    assert raised.value.location == "/models/3/connector_inputs"


@pytest.mark.parametrize(
    ("dependencies", "expected_location"),
    [
        (
            (ModelDependency("missing", "source"),),
            "/models/2/dependencies/0",
        ),
        (
            (
                ModelDependency("raw_orders", "source"),
                ModelDependency("raw_orders", "source"),
            ),
            "/models/2/dependencies/1",
        ),
        (
            (ModelDependency("orders_total", "model"),),
            "/models/2/dependencies/0",
        ),
    ],
)
def test_dependency_closure_uniqueness_and_self_reference_fail_closed(
    tmp_path: Path,
    dependencies: tuple[ModelDependency, ...],
    expected_location: str,
) -> None:
    project, resolved, compiled = _compile_project(tmp_path)
    resolved["orders_total"] = replace(
        resolved["orders_total"],
        dependencies=dependencies,
    )

    with pytest.raises(CatalogProjectionError) as raised:
        build_catalog_snapshot(project, resolved, compiled, "production")

    assert raised.value.location == expected_location


def test_model_dependency_cycles_fail_closed(tmp_path: Path) -> None:
    project, resolved, compiled = _compile_project(tmp_path)
    resolved["plain_topic"] = replace(
        resolved["plain_topic"],
        dependencies=(ModelDependency("orders_total", "model"),),
    )
    resolved["orders_total"] = replace(
        resolved["orders_total"],
        dependencies=(ModelDependency("plain_topic", "model"),),
    )

    with pytest.raises(CatalogProjectionError) as raised:
        build_catalog_snapshot(project, resolved, compiled, "production")

    assert raised.value.location.endswith("/dependencies")
    assert "cycle" in str(raised.value)


@pytest.mark.parametrize(
    ("mutation", "expected_location"),
    [
        ("blank_environment", "effective_environment"),
        ("blank_topic", "/sources/0/topic"),
        ("blank_owner", "/sources/0/owner"),
        ("duplicate_tags", "/models/1/tags/2"),
        ("unsafe_description", "/models/1/description"),
    ],
)
def test_blank_duplicate_and_unsafe_metadata_is_rejected(
    tmp_path: Path,
    mutation: str,
    expected_location: str,
) -> None:
    project, resolved, compiled = _compile_project(tmp_path)
    environment = "production"
    if mutation == "blank_environment":
        environment = " \t"
    elif mutation == "blank_topic":
        project.sources[0].topic = " "
    elif mutation == "blank_owner":
        project.sources[0].owner = "\t"
    elif mutation == "duplicate_tags":
        project.models[1].tags.append("payments")
    else:
        project.models[1].description = "description-secret\x00sentinel"

    with pytest.raises(CatalogProjectionError) as raised:
        build_catalog_snapshot(project, resolved, compiled, environment)

    assert raised.value.location == expected_location
    assert "description-secret" not in str(raised.value)


def test_invalid_dependency_kind_is_a_safe_projection_error(tmp_path: Path) -> None:
    project, resolved, compiled = _compile_project(tmp_path)
    invalid = ModelDependency("sensitive-dependency-name", cast(Any, "invalid"))
    resolved["orders_total"] = replace(
        resolved["orders_total"],
        dependencies=(invalid,),
    )

    with pytest.raises(CatalogProjectionError) as raised:
        build_catalog_snapshot(project, resolved, compiled, "production")

    assert raised.value.location == "/models/2/dependencies/0/kind"
    assert "sensitive-dependency-name" not in str(raised.value)


def test_record_constructors_enforce_immutable_collection_boundaries() -> None:
    with pytest.raises(CatalogProjectionError) as raised:
        CatalogProcess(
            logical_name="process",
            process_kind="flink",
            description=None,
            tags=cast(tuple[str, ...], ["tag"]),
            owner=None,
            dependencies=(),
        )
    assert raised.value.location == "process.tags"

    with pytest.raises(CatalogProjectionError) as raised:
        CatalogSnapshot(
            project_name="project",
            project_description=None,
            effective_environment="default",
            datasets=cast(tuple[CatalogDataset, ...], []),
            processes=(),
            omitted_exposure_names=(),
        )
    assert raised.value.location == "snapshot.datasets"


def test_compiled_shape_is_revalidated_defensively(tmp_path: Path) -> None:
    project, resolved, compiled = _compile_project(tmp_path)
    original = compiled["filtered_orders"]
    malformed = object.__new__(CompiledModelView)
    for field, value in vars(original).items():
        object.__setattr__(malformed, field, value)
    object.__setattr__(malformed, "process_kind", "connect")
    compiled["filtered_orders"] = malformed

    with pytest.raises(CatalogProjectionError) as raised:
        build_catalog_snapshot(project, resolved, compiled, "production")

    assert raised.value.location == "/models/1/materialized"


def test_blank_descriptions_are_omitted_without_changing_nonblank_text(
    tmp_path: Path,
) -> None:
    project, resolved, compiled = _compile_project(tmp_path)
    project.project.description = "  \n"
    project.sources[0].description = "\t"
    project.models[0].description = "  "
    resolved["plain_topic"].model.description = "  "

    snapshot = build_catalog_snapshot(project, resolved, compiled, "production")

    assert snapshot.project_description is None
    assert snapshot.datasets[0].description is None
    plain_topic = next(
        dataset for dataset in snapshot.datasets if dataset.logical_name == "plain_topic"
    )
    assert plain_topic.description is None


def test_duplicate_exposure_declarations_remain_separate_omissions(
    tmp_path: Path,
) -> None:
    project, resolved, compiled = _compile_project(tmp_path)
    project.exposures.append(project.exposures[0].model_copy(deep=True))

    snapshot = build_catalog_snapshot(project, resolved, compiled, "production")

    assert snapshot.omitted_exposure_names == (
        "checkout_dashboard",
        "checkout_dashboard",
    )
