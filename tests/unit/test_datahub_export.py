from __future__ import annotations

import json
from dataclasses import FrozenInstanceError, replace
from typing import Any

import pytest

from streamt.integrations.catalog import datahub_export
from streamt.integrations.catalog.datahub import DataHubJobTopology
from streamt.integrations.catalog.datahub_export import (
    DataHubCatalogExport,
    DataHubExportError,
    generate_datahub_catalog,
)
from streamt.integrations.catalog.model import (
    CatalogContractSummary,
    CatalogDataset,
    CatalogDependency,
    CatalogOwnerLabel,
    CatalogProcess,
    CatalogSnapshot,
)


def _dataset(
    kind: str,
    name: str,
    transport: str,
    physical: str,
    *,
    description: str | None = None,
    tags: tuple[str, ...] = (),
    owner: str | None = None,
    contract: str | None = None,
) -> CatalogDataset:
    return CatalogDataset(
        logical_kind=kind,  # type: ignore[arg-type]
        logical_name=name,
        transport=transport,  # type: ignore[arg-type]
        physical_name=physical,
        description=description,
        tags=tags,
        owner=CatalogOwnerLabel(owner) if owner is not None else None,
        contract=(
            CatalogContractSummary(contract)  # type: ignore[arg-type]
            if contract is not None
            else None
        ),
    )


def _process(
    name: str,
    kind: str,
    dependencies: tuple[tuple[str, str], ...],
    *,
    description: str | None = None,
    tags: tuple[str, ...] = (),
    owner: str | None = None,
) -> CatalogProcess:
    return CatalogProcess(
        logical_name=name,
        process_kind=kind,  # type: ignore[arg-type]
        description=description,
        tags=tags,
        owner=CatalogOwnerLabel(owner) if owner is not None else None,
        dependencies=tuple(
            CatalogDependency(dep_kind, dep_name)  # type: ignore[arg-type]
            for dep_kind, dep_name in dependencies
        ),
    )


def _full_snapshot() -> CatalogSnapshot:
    return CatalogSnapshot(
        project_name="Commerce",
        project_description="Commerce streaming catalog\nOwned by Platform",
        effective_environment="production",
        datasets=(
            _dataset(
                "source",
                "orders",
                "kafka",
                "Orders.Raw",
                tags=("source-secret-tag",),
                owner="source-secret-owner",
            ),
            _dataset(
                "model",
                "plain_topic",
                "kafka",
                "orders.plain",
                tags=("topic-secret-tag",),
                contract="declared",
            ),
            _dataset(
                "model",
                "enrich_orders",
                "kafka",
                "orders.enriched",
                description="Enriched orders",
                tags=("flink-secret-tag",),
                owner="flink-secret-owner",
                contract="enforced",
            ),
            _dataset(
                "model",
                "public_orders",
                "gateway",
                "orders(public)",
                description="Public alias",
                tags=("gateway-secret-tag",),
                owner="gateway-secret-owner",
            ),
        ),
        processes=(
            _process(
                "enrich_orders",
                "flink",
                (("source", "orders"),),
                description="Enriched orders",
                tags=("flink-secret-tag",),
                owner="flink-secret-owner",
            ),
            _process(
                "public_orders",
                "gateway",
                (("model", "enrich_orders"),),
                description="Public alias",
                tags=("gateway-secret-tag",),
                owner="gateway-secret-owner",
            ),
            _process(
                "archive_orders",
                "connect",
                (("model", "public_orders"),),
                tags=("sink-secret-tag",),
                owner="sink-secret-owner",
            ),
        ),
        omitted_exposure_names=("dashboard", "dashboard"),
    )


def _generate(
    snapshot: CatalogSnapshot | None = None,
    **overrides: object,
) -> DataHubCatalogExport:
    inputs: dict[str, object] = {
        "catalog_id": "commerce",
        "fabric": "PROD",
        "kafka_platform_instance": "main",
        "gateway_platform_id": "conduktor-gateway",
        "gateway_platform_instance": "edge,west",
    }
    inputs.update(overrides)
    return generate_datahub_catalog(snapshot or _full_snapshot(), **inputs)


def _by_pair(result: DataHubCatalogExport) -> dict[tuple[str, str], dict[str, object]]:
    return {
        (proposal.entity_urn, proposal.aspect_name): proposal.aspect
        for proposal in result.proposals
    }


def test_full_export_maps_exact_entities_aspects_and_direct_lineage() -> None:
    result = _generate()
    proposals = result.proposals
    assert len(proposals) == 15
    assert proposals[0].entity_type == "dataFlow"
    assert proposals[0].aspect_name == "dataFlowInfo"
    assert proposals[0].aspect == {
        "name": "Commerce",
        "description": "Commerce streaming catalog\nOwned by Platform",
        "env": "PROD",
        "customProperties": {},
    }

    dataset_proposals = [item for item in proposals if item.entity_type == "dataset"]
    assert [item.entity_urn for item in dataset_proposals] == sorted(
        [item.entity_urn for item in dataset_proposals], key=lambda urn: urn.encode("utf-8")
    )
    properties = [
        item.aspect for item in dataset_proposals if item.aspect_name == "datasetProperties"
    ]
    assert {item["name"] for item in properties} == {
        "orders",
        "plain_topic",
        "enrich_orders",
        "public_orders",
    }
    assert next(item for item in properties if item["name"] == "plain_topic") == {
        "name": "plain_topic",
        "customProperties": {"streamt.contract.status": "declared"},
        "tags": [],
    }
    assert next(item for item in properties if item["name"] == "enrich_orders")[
        "customProperties"
    ] == {"streamt.contract.status": "enforced"}

    pairs = _by_pair(result)
    job_infos = {
        aspect["name"]: (urn, aspect)
        for (urn, name), aspect in pairs.items()
        if name == "dataJobInfo"
    }
    assert set(job_infos) == {"enrich_orders", "public_orders", "archive_orders"}
    assert job_infos["enrich_orders"][1]["type"] == {"string": "flink"}
    assert job_infos["archive_orders"][1]["type"] == {"string": "connect"}

    enrich_io = pairs[(job_infos["enrich_orders"][0], "dataJobInputOutput")]
    gateway_io = pairs[(job_infos["public_orders"][0], "dataJobInputOutput")]
    sink_io = pairs[(job_infos["archive_orders"][0], "dataJobInputOutput")]
    for aspect in (enrich_io, gateway_io, sink_io):
        assert aspect["inputDatasets"] == []
        assert aspect["outputDatasets"] == []
    assert len(enrich_io["inputDatasetEdges"]) == 1  # type: ignore[arg-type]
    assert len(enrich_io["outputDatasetEdges"]) == 1  # type: ignore[arg-type]
    assert len(gateway_io["inputDatasetEdges"]) == 1  # type: ignore[arg-type]
    assert len(gateway_io["outputDatasetEdges"]) == 1  # type: ignore[arg-type]
    assert sink_io["outputDatasetEdges"] == []


def test_instances_and_process_free_topic_have_exact_shape() -> None:
    result = _generate()
    proposals = result.proposals
    datasets = [item for item in proposals if item.entity_type == "dataset"]
    assert sum(item.aspect_name == "datasetProperties" for item in datasets) == 4
    assert sum(item.aspect_name == "dataPlatformInstance" for item in datasets) == 4
    assert not any(
        item.entity_type == "dataJob" and "plain_topic" in item.entity_urn for item in proposals
    )
    gateway_instance = next(
        item.aspect
        for item in datasets
        if item.aspect_name == "dataPlatformInstance" and "conduktor-gateway" in item.entity_urn
    )
    assert gateway_instance == {
        "platform": "urn:li:dataPlatform:conduktor-gateway",
        "instance": (
            "urn:li:dataPlatformInstance:(urn:li:dataPlatform:conduktor-gateway,edge%2Cwest)"
        ),
    }


def test_kafka_no_instance_omits_only_its_platform_instance_aspects() -> None:
    result = _generate(kafka_platform_instance=None)
    datasets = [item for item in result.proposals if item.entity_type == "dataset"]
    assert sum(item.aspect_name == "datasetProperties" for item in datasets) == 4
    assert sum(item.aspect_name == "dataPlatformInstance" for item in datasets) == 1
    assert any(
        item.entity_urn == "urn:li:dataset:(urn:li:dataPlatform:kafka,Orders.Raw,PROD)"
        for item in datasets
    )


def test_empty_catalog_emits_only_its_data_flow() -> None:
    snapshot = CatalogSnapshot(
        project_name="Empty",
        project_description=None,
        effective_environment="prod",
        datasets=(),
        processes=(),
        omitted_exposure_names=(),
    )
    result = _generate(
        snapshot,
        kafka_platform_instance=None,
        gateway_platform_id=None,
        gateway_platform_instance=None,
    )
    assert [(item.entity_type, item.aspect_name) for item in result.proposals] == [
        ("dataFlow", "dataFlowInfo")
    ]
    assert result.warnings == ()


def test_warning_cardinality_is_per_declaration_and_occurrence() -> None:
    result = _generate()
    counts: dict[str, int] = {}
    for warning in result.warnings:
        counts[warning.code] = counts.get(warning.code, 0) + 1
    assert counts == {
        "W115_DATAHUB_SINK_OUTPUT_OMITTED": 1,
        "W116_DATAHUB_EXPOSURE_OMITTED": 2,
        "W117_DATAHUB_TAGS_OMITTED": 5,
        "W118_DATAHUB_OWNER_OMITTED": 4,
    }
    assert [warning.location for warning in result.warnings if warning.code.startswith("W116")] == [
        "exposures/0",
        "exposures/1",
    ]
    warning_text = repr(result.warnings)
    for forbidden in (
        "source-secret-owner",
        "source-secret-tag",
        "sink-secret-owner",
        "sink-secret-tag",
        "dashboard",
    ):
        assert forbidden not in warning_text


def test_canonical_json_and_result_are_defensive_and_repeatable() -> None:
    first = _generate()
    second = _generate()
    assert first.json_bytes == second.json_bytes
    assert first.json_text.endswith("\n")
    assert not first.json_text.endswith("\n\n")
    assert json.loads(first.json_text) == [item.to_dict() for item in first.proposals]
    assert repr(first) == "DataHubCatalogExport(proposal_count=15, warning_count=12)"

    returned = first.proposals
    object.__setattr__(returned[0], "aspect_name", "changed")
    assert first.proposals[0].aspect_name == "dataFlowInfo"
    returned_warnings = first.warnings
    object.__setattr__(returned_warnings[0], "message", "changed")
    assert first.warnings[0].message != "changed"
    assert "changed" not in repr(first)
    with pytest.raises(FrozenInstanceError):
        first.warnings = ()  # type: ignore[misc]
    with pytest.raises(TypeError):
        DataHubCatalogExport()
    with pytest.raises(TypeError):
        DataHubCatalogExport(  # type: ignore[call-arg]
            _proposal_payload=first.json_text,
            _warning_payloads=(),
        )


def test_snapshot_and_input_order_do_not_change_output() -> None:
    snapshot = _full_snapshot()
    reordered = replace(
        snapshot,
        datasets=tuple(reversed(snapshot.datasets)),
        processes=tuple(reversed(snapshot.processes)),
        omitted_exposure_names=tuple(reversed(snapshot.omitted_exposure_names)),
    )
    assert _generate(snapshot).json_bytes == _generate(reordered).json_bytes
    assert _generate(snapshot).warnings == _generate(reordered).warnings


@pytest.mark.parametrize(
    ("overrides", "location"),
    [
        ({"catalog_id": None}, "catalog_id"),
        ({"catalog_id": "bad\nsecret"}, "data_flow/catalog_id"),
        ({"fabric": "prod"}, "fabric"),
        ({"gateway_platform_id": "gateway", "gateway_platform_instance": None}, "gateway_platform"),
    ],
)
def test_inputs_fail_with_safe_locations(overrides: dict[str, object], location: str) -> None:
    with pytest.raises(DataHubExportError) as captured:
        _generate(**overrides)  # type: ignore[arg-type]
    assert captured.value.location == location
    assert "secret" not in str(captured.value)


def test_gateway_options_are_conditionally_required() -> None:
    with pytest.raises(DataHubExportError, match="explicit platform") as captured:
        _generate(gateway_platform_id=None, gateway_platform_instance=None)
    assert captured.value.location == "gateway_platform"

    source_only = CatalogSnapshot(
        project_name="Sources",
        project_description=None,
        effective_environment="prod",
        datasets=(_dataset("source", "orders", "kafka", "orders"),),
        processes=(),
        omitted_exposure_names=(),
    )
    result = _generate(
        source_only,
        kafka_platform_instance=None,
        gateway_platform_id=None,
        gateway_platform_instance=None,
    )
    assert len(result.proposals) == 2


@pytest.mark.parametrize(
    ("failure", "location"),
    [
        ("version", "snapshot.version"),
        ("dataset_kind", "snapshot.datasets/0/logical_kind"),
        ("dataset_transport", "snapshot.datasets/0/transport"),
        ("contract", "snapshot.datasets/1/contract"),
        ("process_kind", "snapshot.processes/0/process_kind"),
        ("dependency_kind", "snapshot.processes/0/dependencies/0/logical_kind"),
    ],
)
def test_constructor_bypassed_snapshots_fail_closed(failure: str, location: str) -> None:
    snapshot = _full_snapshot()
    if failure == "version":
        object.__setattr__(snapshot, "version", 2)
    elif failure == "dataset_kind":
        object.__setattr__(snapshot.datasets[0], "logical_kind", [])
    elif failure == "dataset_transport":
        object.__setattr__(snapshot.datasets[0], "transport", {})
    elif failure == "contract":
        assert snapshot.datasets[1].contract is not None
        object.__setattr__(snapshot.datasets[1].contract, "status", [])
    elif failure == "process_kind":
        object.__setattr__(snapshot.processes[0], "process_kind", {})
    else:
        object.__setattr__(snapshot.processes[0].dependencies[0], "logical_kind", [])
    with pytest.raises(DataHubExportError) as captured:
        _generate(snapshot)
    assert captured.value.location == location


@pytest.mark.parametrize("failure", ["physical", "dangling", "metadata", "cycle"])
def test_global_snapshot_contradictions_fail(failure: str) -> None:
    snapshot = _full_snapshot()
    if failure == "physical":
        duplicate = replace(
            snapshot.datasets[1],
            logical_name="duplicate",
            physical_name=snapshot.datasets[0].physical_name,
        )
        snapshot = replace(snapshot, datasets=(*snapshot.datasets, duplicate))
    elif failure == "dangling":
        process = replace(
            snapshot.processes[0],
            dependencies=(CatalogDependency("source", "missing"),),
        )
        snapshot = replace(snapshot, processes=(process, *snapshot.processes[1:]))
    elif failure == "metadata":
        process = replace(snapshot.processes[0], description="Contradiction")
        snapshot = replace(snapshot, processes=(process, *snapshot.processes[1:]))
    else:
        first = replace(
            snapshot.processes[0],
            dependencies=(CatalogDependency("model", "public_orders"),),
        )
        snapshot = replace(snapshot, processes=(first, *snapshot.processes[1:]))
    with pytest.raises(DataHubExportError):
        _generate(snapshot)


def test_final_validation_rejects_closed_but_wrong_direct_topology(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original = datahub_export._topology_for_process

    def wrong_topology(*args: Any, **kwargs: Any) -> DataHubJobTopology:
        result = original(*args, **kwargs)
        process = args[0]
        if process.logical_name != "enrich_orders":
            return result
        extra = next(
            identity.urn
            for logical, identity in kwargs["datasets"].items()
            if logical == ("model", "plain_topic")
        )
        return replace(
            result,
            input_dataset_urns=tuple(
                sorted((*result.input_dataset_urns, extra), key=lambda urn: urn.encode("utf-8"))
            ),
        )

    monkeypatch.setattr(datahub_export, "_topology_for_process", wrong_topology)
    with pytest.raises(DataHubExportError, match="exact direct"):
        _generate()


def test_final_validation_rejects_wrong_but_structurally_valid_flow_name(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original = datahub_export._proposal

    def wrong_flow(**kwargs: Any) -> Any:
        if kwargs["aspect_name"] == "dataFlowInfo":
            kwargs["aspect"] = {**kwargs["aspect"], "name": "Wrong project"}
        return original(**kwargs)

    monkeypatch.setattr(datahub_export, "_proposal", wrong_flow)
    with pytest.raises(DataHubExportError, match="catalog snapshot"):
        _generate()


def test_platform_instance_percent_alias_collision_fails() -> None:
    with pytest.raises(DataHubExportError, match="typed URN"):
        _generate(
            kafka_platform_instance="edge,west",
            gateway_platform_id="kafka",
            gateway_platform_instance="edge%2Cwest",
        )


def test_unallowlisted_attached_secret_is_never_observed() -> None:
    sentinel = "do-not-leak-runtime-token"
    snapshot = _full_snapshot()
    object.__setattr__(snapshot, "runtime_config", {"token": sentinel, "sql": sentinel})
    result = _generate(snapshot)
    assert sentinel not in result.json_text
    assert sentinel not in repr(result)
    assert sentinel not in repr(result.warnings)


def test_malformed_uninitialized_snapshot_is_redacted_at_public_boundary() -> None:
    malformed = object.__new__(CatalogSnapshot)
    with pytest.raises(DataHubExportError) as captured:
        _generate(malformed)
    assert captured.value.location == "export"
    assert str(captured.value) == "DataHub export failed safely"


def test_unexpected_helper_error_is_redacted_at_public_boundary(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sentinel = "do-not-leak-unexpected-helper-secret"

    def fail(_snapshot: object) -> Any:
        raise RuntimeError(sentinel)

    monkeypatch.setattr(datahub_export, "_validate_snapshot", fail)
    with pytest.raises(DataHubExportError) as captured:
        _generate()
    assert captured.value.location == "export"
    assert str(captured.value) == "DataHub export failed safely"
    assert sentinel not in str(captured.value)
