"""Pure Backstage mapping, semantic validation, and canonical YAML tests."""

from __future__ import annotations

import socket
from dataclasses import replace
from typing import Any

import pytest
import yaml

from streamt.integrations.catalog import backstage
from streamt.integrations.catalog.backstage import (
    BackstageCatalogExport,
    BackstageExportError,
    BackstageExportWarning,
    generate_backstage_catalog,
)
from streamt.integrations.catalog.inputs import validate_owner_map
from streamt.integrations.catalog.model import (
    CatalogContractSummary,
    CatalogDataset,
    CatalogDependency,
    CatalogOwnerLabel,
    CatalogProcess,
    CatalogSnapshot,
)

OWNER_MAP = {
    "version": 1,
    "owners": {
        "source-team": "group:teams/source",
        "data-team": "group:teams/data",
        "gateway-team": "group:teams/gateway",
        "sink-team": "group:teams/sink",
    },
}

GOLDEN_YAML = """---
apiVersion: backstage.io/v1alpha1
kind: System
metadata:
  name: system-payments-960d9210584eaba6
  namespace: platform
  title: Payments Platform
  description: Streaming orders
  annotations:
    streamt.dev/catalog-id: payments
    streamt.dev/environment: prod
    streamt.dev/logical-kind: project
    streamt.dev/logical-name: Payments Platform
    streamt.dev/project: Payments Platform
spec:
  owner: group:default/platform
  domain: domain:default/commerce
---
apiVersion: backstage.io/v1alpha1
kind: Resource
metadata:
  name: topic-raw-orders-b19264b6db75fff8
  namespace: platform
  title: raw_orders
  description: Raw orders
  annotations:
    streamt.dev/catalog-id: payments
    streamt.dev/environment: prod
    streamt.dev/logical-kind: source
    streamt.dev/logical-name: raw_orders
    streamt.dev/physical-name: raw.orders
    streamt.dev/project: Payments Platform
  tags:
  - raw
spec:
  type: kafka-topic
  owner: group:teams/source
  dependsOn:
  - resource:infra/kafka-main
"""


def _source_dataset(
    *,
    logical_name: str = "raw_orders",
    physical_name: str = "raw.orders",
) -> CatalogDataset:
    return CatalogDataset(
        logical_kind="source",
        logical_name=logical_name,
        transport="kafka",
        physical_name=physical_name,
        description="Raw orders",
        tags=("raw",),
        owner=CatalogOwnerLabel("source-team"),
        contract=None,
    )


def _full_snapshot() -> CatalogSnapshot:
    return CatalogSnapshot(
        project_name="Payments Platform",
        project_description="Streaming orders",
        effective_environment="prod",
        datasets=(
            _source_dataset(),
            CatalogDataset(
                logical_kind="model",
                logical_name="orders_total",
                transport="kafka",
                physical_name="analytics.orders",
                description="Order totals",
                tags=("aggregate", "payments"),
                owner=CatalogOwnerLabel("data-team"),
                contract=CatalogContractSummary("enforced"),
            ),
            CatalogDataset(
                logical_kind="model",
                logical_name="filtered_orders",
                transport="gateway",
                physical_name="orders.filtered",
                description="Filtered orders",
                tags=("filtered",),
                owner=CatalogOwnerLabel("gateway-team"),
                contract=CatalogContractSummary("declared"),
            ),
            CatalogDataset(
                logical_kind="model",
                logical_name="plain_topic",
                transport="kafka",
                physical_name="plain.topic",
                description="Plain topic",
                tags=(),
                owner=None,
                contract=None,
            ),
        ),
        processes=(
            CatalogProcess(
                logical_name="orders_total",
                process_kind="flink",
                description="Order totals",
                tags=("aggregate", "payments"),
                owner=CatalogOwnerLabel("data-team"),
                dependencies=(CatalogDependency("source", "raw_orders"),),
            ),
            CatalogProcess(
                logical_name="filtered_orders",
                process_kind="gateway",
                description="Filtered orders",
                tags=("filtered",),
                owner=CatalogOwnerLabel("gateway-team"),
                dependencies=(CatalogDependency("model", "orders_total"),),
            ),
            CatalogProcess(
                logical_name="warehouse_sink",
                process_kind="connect",
                description="Warehouse sink",
                tags=("sink",),
                owner=CatalogOwnerLabel("sink-team"),
                dependencies=(CatalogDependency("model", "orders_total"),),
            ),
        ),
        omitted_exposure_names=("dashboard", "dashboard"),
    )


def _generate(
    snapshot: CatalogSnapshot | None = None,
    **overrides: object,
) -> BackstageCatalogExport:
    values: dict[str, object] = {
        "catalog_id": "payments",
        "catalog_namespace": "platform",
        "default_owner_ref": "group:default/platform",
        "lifecycle": "production",
        "owner_map": OWNER_MAP,
        "kafka_cluster_ref": "resource:infra/kafka-main",
        "gateway_cluster_ref": "resource:infra/gateway-main",
        "domain_ref": "domain:default/commerce",
    }
    values.update(overrides)
    return generate_backstage_catalog(snapshot or _full_snapshot(), **values)


def _by_title(result: BackstageCatalogExport, title: str) -> dict[str, object]:
    return next(
        entity
        for entity in result.entities
        if entity["metadata"]["title"] == title  # type: ignore[index]
    )


def _names(result: BackstageCatalogExport) -> dict[tuple[str, str], str]:
    return {
        (str(entity["kind"]), str(entity["metadata"]["title"])): str(  # type: ignore[index]
            entity["metadata"]["name"]  # type: ignore[index]
        )
        for entity in result.entities
    }


def _namespaces(result: BackstageCatalogExport) -> dict[tuple[str, str], str]:
    return {
        (str(entity["kind"]), str(entity["metadata"]["title"])): str(  # type: ignore[index]
            entity["metadata"]["namespace"]  # type: ignore[index]
        )
        for entity in result.entities
    }


def test_source_only_golden_entities_and_canonical_yaml() -> None:
    snapshot = CatalogSnapshot(
        project_name="Payments Platform",
        project_description="Streaming orders",
        effective_environment="prod",
        datasets=(_source_dataset(),),
        processes=(),
        omitted_exposure_names=(),
    )

    result = _generate(snapshot, gateway_cluster_ref=None)

    assert result.yaml == GOLDEN_YAML
    assert result.yaml_text == GOLDEN_YAML
    assert result.yaml_bytes == GOLDEN_YAML.encode()
    assert result.warnings == ()
    assert result.entities == tuple(yaml.safe_load_all(GOLDEN_YAML))
    assert result.yaml.count("---\n") == 2
    assert "\n...\n" not in result.yaml
    assert "&id" not in result.yaml
    assert "*id" not in result.yaml
    assert result.yaml.endswith("\n")


def test_all_compiled_shapes_map_to_exact_core_entities_and_direct_edges() -> None:
    result = _generate()

    assert [entity["kind"] for entity in result.entities] == [
        "System",
        "Resource",
        "Resource",
        "Resource",
        "Resource",
        "Component",
        "Component",
        "Component",
    ]
    system = _by_title(result, "Payments Platform")
    assert system["spec"] == {
        "owner": "group:default/platform",
        "domain": "domain:default/commerce",
    }

    source = _by_title(result, "raw_orders")
    assert source["spec"] == {
        "type": "kafka-topic",
        "owner": "group:teams/source",
        "dependsOn": ["resource:infra/kafka-main"],
    }
    assert "system" not in source["spec"]  # type: ignore[operator]

    plain = _by_title(result, "plain_topic")
    assert plain["spec"]["owner"] == "group:default/platform"  # type: ignore[index]
    assert plain["spec"]["dependsOn"] == [  # type: ignore[index]
        "resource:infra/kafka-main"
    ]

    flink = _by_title(result, "orders_total")
    flink_component = next(
        entity
        for entity in result.entities
        if entity["kind"] == "Component" and entity["metadata"]["title"] == "orders_total"  # type: ignore[index]
    )
    assert flink["metadata"]["annotations"]["streamt.dev/contract"] == "enforced"  # type: ignore[index]
    assert flink["spec"]["dependsOn"] == [  # type: ignore[index]
        f"component:platform/{flink_component['metadata']['name']}",  # type: ignore[index]
        "resource:infra/kafka-main",
    ]
    assert flink_component["spec"]["dependsOn"] == [  # type: ignore[index]
        f"resource:platform/{source['metadata']['name']}"  # type: ignore[index]
    ]

    gateway = _by_title(result, "filtered_orders")
    gateway_component = next(
        entity
        for entity in result.entities
        if entity["kind"] == "Component" and entity["metadata"]["title"] == "filtered_orders"  # type: ignore[index]
    )
    assert gateway["spec"]["type"] == "kafka-virtual-topic"  # type: ignore[index]
    assert gateway["metadata"]["annotations"]["streamt.dev/contract"] == "declared"  # type: ignore[index]
    assert gateway["spec"]["dependsOn"] == [  # type: ignore[index]
        f"component:platform/{gateway_component['metadata']['name']}",  # type: ignore[index]
        "resource:infra/gateway-main",
    ]
    assert gateway_component["spec"]["dependsOn"] == [  # type: ignore[index]
        f"resource:platform/{flink['metadata']['name']}"  # type: ignore[index]
    ]

    sink = _by_title(result, "warehouse_sink")
    assert sink["kind"] == "Component"
    assert sink["spec"]["dependsOn"] == [  # type: ignore[index]
        f"resource:platform/{flink['metadata']['name']}"  # type: ignore[index]
    ]
    assert not any(
        entity["kind"] == "Resource" and entity["metadata"]["title"] == "warehouse_sink"  # type: ignore[index]
        for entity in result.entities
    )


def test_warning_records_preserve_duplicate_exposure_omissions() -> None:
    result = _generate()

    assert result.warnings == (
        BackstageExportWarning(
            "W114_BACKSTAGE_EXPOSURE_OMITTED",
            "Exposure metadata is omitted from Backstage export",
            "exposures/dashboard",
        ),
        BackstageExportWarning(
            "W114_BACKSTAGE_EXPOSURE_OMITTED",
            "Exposure metadata is omitted from Backstage export",
            "exposures/dashboard",
        ),
        BackstageExportWarning(
            "W113_BACKSTAGE_SINK_OUTPUT_OMITTED",
            "Connector destination metadata is omitted from Backstage export",
            "models/warehouse_sink",
        ),
    )


def test_entities_are_defensive_copies_and_export_repr_is_bounded() -> None:
    result = _generate()
    first = result.entities
    first[0]["metadata"]["title"] = "mutated"  # type: ignore[index]

    assert result.entities[0]["metadata"]["title"] == "Payments Platform"  # type: ignore[index]
    assert repr(result) == "BackstageCatalogExport(entity_count=8, warning_count=3)"
    assert "Payments Platform" not in repr(result)


def test_export_constructor_rejects_mutable_or_inconsistent_result_state() -> None:
    result = _generate()

    with pytest.raises(BackstageExportError) as mutable_entities:
        BackstageCatalogExport(
            _entity_payloads=list(result._entity_payloads),  # type: ignore[arg-type]
            warnings=result.warnings,
            yaml_text=result.yaml,
        )
    assert mutable_entities.value.location == "export.entities"

    with pytest.raises(BackstageExportError) as mutable_warnings:
        BackstageCatalogExport(
            _entity_payloads=result._entity_payloads,
            warnings=list(result.warnings),  # type: ignore[arg-type]
            yaml_text=result.yaml,
        )
    assert mutable_warnings.value.location == "export.warnings"

    with pytest.raises(BackstageExportError) as inconsistent_yaml:
        BackstageCatalogExport(
            _entity_payloads=result._entity_payloads,
            warnings=result.warnings,
            yaml_text="---\n{}\n",
        )
    assert inconsistent_yaml.value.location == "export.yaml"


def test_reordered_snapshot_collections_and_metadata_are_byte_identical() -> None:
    snapshot = _full_snapshot()
    datasets = tuple(
        replace(dataset, tags=tuple(reversed(dataset.tags)))
        for dataset in reversed(snapshot.datasets)
    )
    processes = tuple(
        replace(
            process,
            tags=tuple(reversed(process.tags)),
            dependencies=tuple(reversed(process.dependencies)),
        )
        for process in reversed(snapshot.processes)
    )
    reordered = replace(
        snapshot,
        datasets=datasets,
        processes=processes,
        omitted_exposure_names=tuple(reversed(snapshot.omitted_exposure_names)),
    )

    assert _generate(reordered).yaml_bytes == _generate(snapshot).yaml_bytes


def test_identity_change_matrix_uses_only_frozen_seed_fields() -> None:
    snapshot = _full_snapshot()
    baseline = _names(_generate(snapshot))

    renamed_result = _generate(replace(snapshot, project_name="Renamed Display Project"))
    renamed_project = _names(renamed_result)
    assert (
        _by_title(renamed_result, "Renamed Display Project")["metadata"]["name"]  # type: ignore[index]
        == baseline[("System", "Payments Platform")]
    )
    assert {key: value for key, value in renamed_project.items() if key[0] != "System"} == {
        key: value for key, value in baseline.items() if key[0] != "System"
    }

    changed_environment = _names(_generate(replace(snapshot, effective_environment="staging")))
    assert all(changed_environment[key] != value for key, value in baseline.items())

    changed_cluster = _names(
        _generate(snapshot, kafka_cluster_ref="resource:infra/kafka-secondary")
    )
    for key, value in baseline.items():
        if key[0] == "Resource" and key[1] != "filtered_orders":
            assert changed_cluster[key] != value
        else:
            assert changed_cluster[key] == value

    changed_physical = replace(
        snapshot,
        datasets=tuple(
            replace(dataset, physical_name="raw.orders.v2")
            if dataset.logical_name == "raw_orders"
            else dataset
            for dataset in snapshot.datasets
        ),
    )
    physical_names = _names(_generate(changed_physical))
    assert physical_names[("Resource", "raw_orders")] != baseline[("Resource", "raw_orders")]
    assert physical_names[("Component", "orders_total")] == baseline[("Component", "orders_total")]


def test_identity_matrix_covers_catalog_namespace_logical_name_and_resource_type() -> None:
    snapshot = _full_snapshot()
    baseline_result = _generate(snapshot)
    baseline = _names(baseline_result)

    changed_catalog = _names(_generate(snapshot, catalog_id="payments-v2"))
    assert all(changed_catalog[key] != value for key, value in baseline.items())

    changed_namespace_result = _generate(snapshot, catalog_namespace="analytics")
    assert _names(changed_namespace_result) == baseline
    assert set(_namespaces(changed_namespace_result).values()) == {"analytics"}
    assert set(_namespaces(baseline_result).values()) == {"platform"}

    renamed_datasets = tuple(
        replace(dataset, logical_name="orders_renamed")
        if dataset.logical_name == "orders_total"
        else dataset
        for dataset in snapshot.datasets
    )
    renamed_processes = tuple(
        replace(
            process,
            logical_name=(
                "orders_renamed" if process.logical_name == "orders_total" else process.logical_name
            ),
            dependencies=tuple(
                replace(dependency, logical_name="orders_renamed")
                if dependency.logical_identity == ("model", "orders_total")
                else dependency
                for dependency in process.dependencies
            ),
        )
        for process in snapshot.processes
    )
    renamed = _names(
        _generate(
            replace(
                snapshot,
                datasets=renamed_datasets,
                processes=renamed_processes,
            )
        )
    )
    assert renamed[("Component", "orders_renamed")] != baseline[("Component", "orders_total")]
    assert renamed[("Resource", "orders_renamed")] == baseline[("Resource", "orders_total")]
    assert renamed[("Component", "filtered_orders")] == baseline[("Component", "filtered_orders")]

    changed_type = replace(
        snapshot,
        datasets=tuple(
            replace(dataset, transport="gateway")
            if dataset.logical_name == "orders_total"
            else dataset
            for dataset in snapshot.datasets
        ),
        processes=tuple(
            replace(process, process_kind="gateway")
            if process.logical_name == "orders_total"
            else process
            for process in snapshot.processes
        ),
    )
    shared_cluster = "resource:infra/shared"
    shared_baseline = _names(
        _generate(
            snapshot,
            kafka_cluster_ref=shared_cluster,
            gateway_cluster_ref=shared_cluster,
        )
    )
    changed_type_names = _names(
        _generate(
            changed_type,
            kafka_cluster_ref=shared_cluster,
            gateway_cluster_ref=shared_cluster,
        )
    )
    assert (
        changed_type_names[("Resource", "orders_total")]
        != shared_baseline[("Resource", "orders_total")]
    )
    assert (
        changed_type_names[("Component", "orders_total")]
        == shared_baseline[("Component", "orders_total")]
    )


def test_unicode_stem_fallback_and_length_are_canonical() -> None:
    snapshot = CatalogSnapshot(
        project_name="Display",
        project_description=None,
        effective_environment="prod",
        datasets=(_source_dataset(logical_name="unicode", physical_name="東京"),),
        processes=(),
        omitted_exposure_names=(),
    )
    result = _generate(
        snapshot,
        catalog_id="a" * 128,
        gateway_cluster_ref=None,
        domain_ref=None,
    )
    names = _names(result)

    assert names[("Resource", "unicode")].startswith("topic-item-")
    assert len(names[("System", "Display")]) == 63


def test_injected_digest_collision_fails_instead_of_merging(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    snapshot = CatalogSnapshot(
        project_name="Display",
        project_description=None,
        effective_environment="prod",
        datasets=(
            _source_dataset(logical_name="first", physical_name="a@b"),
            _source_dataset(logical_name="second", physical_name="a b"),
        ),
        processes=(),
        omitted_exposure_names=(),
    )
    monkeypatch.setattr(backstage, "_digest16", lambda _seed: "0" * 16)

    with pytest.raises(BackstageExportError) as captured:
        _generate(snapshot, gateway_cluster_ref=None, domain_ref=None)

    assert captured.value.location.endswith("metadata/name")
    assert str(captured.value) == "Generated Backstage entity identities collide"


@pytest.mark.parametrize(
    ("overrides", "location"),
    [
        ({"catalog_id": "UPPER"}, "catalog_id"),
        ({"catalog_namespace": "Platform"}, "catalog_namespace"),
        ({"default_owner_ref": "platform/payments"}, "default_owner_ref"),
        ({"lifecycle": ""}, "lifecycle"),
        ({"kafka_cluster_ref": "Resource:infra/kafka"}, "kafka_cluster_ref"),
        ({"gateway_cluster_ref": "resource:Infra/gateway"}, "gateway_cluster_ref"),
        ({"domain_ref": "system:default/commerce"}, "domain_ref"),
    ],
)
def test_invalid_adapter_inputs_retain_safe_locations(
    overrides: dict[str, object],
    location: str,
) -> None:
    with pytest.raises(BackstageExportError) as captured:
        _generate(**overrides)

    assert captured.value.location == location


def test_declared_owner_never_falls_back_when_mapping_is_absent() -> None:
    with pytest.raises(BackstageExportError) as captured:
        _generate(owner_map={"version": 1, "owners": {}})

    assert captured.value.location == "resources.owner"
    assert str(captured.value) == "Declared catalog owner has no explicit mapping"


def test_parsed_owner_map_labels_named_like_raw_envelope_remain_unambiguous() -> None:
    parsed = validate_owner_map(
        {
            "version": 1,
            "owners": {
                "owners": "group:teams/owners",
                "version": "group:teams/version",
            },
        }
    )
    snapshot = CatalogSnapshot(
        project_name="Owner labels",
        project_description=None,
        effective_environment="default",
        datasets=(
            replace(
                _source_dataset(logical_name="one", physical_name="one"),
                owner=CatalogOwnerLabel("owners"),
            ),
            replace(
                _source_dataset(logical_name="two", physical_name="two"),
                owner=CatalogOwnerLabel("version"),
            ),
        ),
        processes=(),
        omitted_exposure_names=(),
    )

    result = _generate(snapshot, owner_map=parsed, gateway_cluster_ref=None, domain_ref=None)

    assert _by_title(result, "one")["spec"]["owner"] == "group:teams/owners"  # type: ignore[index]
    assert _by_title(result, "two")["spec"]["owner"] == "group:teams/version"  # type: ignore[index]


def test_only_a_validated_empty_owner_map_can_use_the_parsed_empty_shape() -> None:
    snapshot = replace(
        _full_snapshot(),
        datasets=(replace(_source_dataset(), owner=None),),
        processes=(),
        omitted_exposure_names=(),
    )
    parsed_empty = validate_owner_map({"version": 1, "owners": {}})

    assert _generate(
        snapshot,
        owner_map=parsed_empty,
        gateway_cluster_ref=None,
        domain_ref=None,
    ).entities
    with pytest.raises(BackstageExportError) as captured:
        _generate(snapshot, owner_map={}, gateway_cluster_ref=None, domain_ref=None)
    assert captured.value.location == "owner_map"


@pytest.mark.parametrize(
    ("overrides", "location"),
    [
        ({"kafka_cluster_ref": None}, "kafka_cluster_ref"),
        ({"gateway_cluster_ref": None}, "gateway_cluster_ref"),
    ],
)
def test_conditionally_required_cluster_references_fail_before_mapping(
    overrides: dict[str, object],
    location: str,
) -> None:
    with pytest.raises(BackstageExportError) as captured:
        _generate(**overrides)

    assert captured.value.location == location


def test_empty_snapshot_needs_no_clusters_and_ignores_valid_superfluous_refs() -> None:
    snapshot = CatalogSnapshot(
        project_name="Empty",
        project_description=None,
        effective_environment="default",
        datasets=(),
        processes=(),
        omitted_exposure_names=(),
    )

    without_clusters = _generate(
        snapshot,
        kafka_cluster_ref=None,
        gateway_cluster_ref=None,
        domain_ref=None,
    )
    with_clusters = _generate(
        snapshot,
        kafka_cluster_ref="resource:infra/kafka-unused",
        gateway_cluster_ref="resource:infra/gateway-unused",
        domain_ref=None,
    )

    assert len(with_clusters.entities) == 1
    assert with_clusters.entities[0]["kind"] == "System"
    assert with_clusters.yaml_bytes == without_clusters.yaml_bytes
    assert "kafka-unused" not in with_clusters.yaml
    assert "gateway-unused" not in with_clusters.yaml


@pytest.mark.parametrize(
    ("field", "location"),
    [
        ("dataset_kind", "snapshot.datasets/0/logical_kind"),
        ("dataset_transport", "snapshot.datasets/0/transport"),
        ("contract_status", "snapshot.datasets/1/contract"),
        ("process_kind", "snapshot.processes/0/process_kind"),
        ("dependency_kind", "snapshot.processes/0/dependencies/0/logical_kind"),
    ],
)
def test_constructed_unhashable_enum_values_fail_with_safe_locations(
    field: str,
    location: str,
) -> None:
    snapshot = _full_snapshot()
    if field == "dataset_kind":
        object.__setattr__(snapshot.datasets[0], "logical_kind", [])
    elif field == "dataset_transport":
        object.__setattr__(snapshot.datasets[0], "transport", {})
    elif field == "contract_status":
        assert snapshot.datasets[1].contract is not None
        object.__setattr__(snapshot.datasets[1].contract, "status", [])
    elif field == "process_kind":
        object.__setattr__(snapshot.processes[0], "process_kind", {})
    else:
        object.__setattr__(snapshot.processes[0].dependencies[0], "logical_kind", [])

    with pytest.raises(BackstageExportError) as captured:
        _generate(snapshot)

    assert captured.value.location == location


def test_exact_topology_rejects_a_closed_but_wrong_internal_dependency(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    baseline = _generate()
    wrong_resource = _by_title(baseline, "plain_topic")
    wrong_ref = f"resource:platform/{wrong_resource['metadata']['name']}"  # type: ignore[index]
    original = backstage._component_entity

    def substitute_closed_target(*args: Any, **kwargs: Any) -> dict[str, object]:
        entity = original(*args, **kwargs)
        metadata = entity["metadata"]
        if isinstance(metadata, dict) and metadata.get("title") == "orders_total":
            spec = entity["spec"]
            assert isinstance(spec, dict)
            spec["dependsOn"] = [wrong_ref]
        return entity

    monkeypatch.setattr(backstage, "_component_entity", substitute_closed_target)

    with pytest.raises(BackstageExportError) as captured:
        _generate()

    assert captured.value.location.endswith("/spec/dependsOn")
    assert str(captured.value) == (
        "Generated Backstage reference does not match its exact relationship"
    )


@pytest.mark.parametrize("failure", ["version", "physical", "dangling", "metadata"])
def test_constructed_invalid_snapshots_fail_closed(failure: str) -> None:
    snapshot = _full_snapshot()
    if failure == "version":
        object.__setattr__(snapshot, "version", 2)
    elif failure == "physical":
        duplicate = replace(
            snapshot.datasets[1],
            logical_name="duplicate_output",
            physical_name=snapshot.datasets[0].physical_name,
        )
        snapshot = replace(snapshot, datasets=(*snapshot.datasets, duplicate))
    elif failure == "dangling":
        process = replace(
            snapshot.processes[0],
            dependencies=(CatalogDependency("source", "absent"),),
        )
        snapshot = replace(snapshot, processes=(process, *snapshot.processes[1:]))
    else:
        process = replace(snapshot.processes[0], description="Contradiction")
        snapshot = replace(snapshot, processes=(process, *snapshot.processes[1:]))

    with pytest.raises(BackstageExportError) as captured:
        _generate(snapshot)

    assert captured.value.location.startswith("snapshot")


def test_invalid_backstage_tag_policy_is_enforced_at_mapper_boundary() -> None:
    dataset = replace(_full_snapshot().datasets[0], tags=("Uppercase",))
    snapshot = replace(_full_snapshot(), datasets=(dataset, *_full_snapshot().datasets[1:]))

    with pytest.raises(BackstageExportError) as captured:
        _generate(snapshot)

    assert captured.value.location == "snapshot.datasets/0/tags/0"


def test_schema_validation_is_offline_and_runs_for_every_entity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []
    original = backstage.validate_backstage_entity

    def fail_network(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("unexpected network access")

    def record(entity: dict[str, object]) -> None:
        calls.append(str(entity["kind"]))
        original(entity)

    monkeypatch.setattr(socket, "create_connection", fail_network)
    monkeypatch.setattr(socket, "getaddrinfo", fail_network)
    monkeypatch.setattr(backstage, "validate_backstage_entity", record)

    result = _generate()

    assert calls == [entity["kind"] for entity in result.entities]


def test_forbidden_extra_snapshot_state_and_secret_error_values_never_leak() -> None:
    secret = "secret-token-at-private-endpoint.example"
    snapshot = _full_snapshot()
    object.__setattr__(snapshot, "runtime_config", {"token": secret})

    result = _generate(snapshot)

    assert secret not in result.yaml
    assert secret not in repr(result)
    object.__setattr__(snapshot, "project_name", secret + "\x00")
    with pytest.raises(BackstageExportError) as captured:
        _generate(snapshot)
    assert secret not in str(captured.value)
    assert secret not in captured.value.location
