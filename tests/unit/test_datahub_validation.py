from __future__ import annotations

import json
from dataclasses import FrozenInstanceError, replace
from pathlib import Path

import pytest

from streamt.integrations.catalog.datahub import (
    DATAHUB_FABRICS,
    DataHubDatasetIdentity,
    DataHubFlowIdentity,
    DataHubIdentityConfig,
    DataHubJobIdentity,
    DataHubJobTopology,
    DataHubProposal,
    DataHubValidationError,
    encode_datahub_urn_component,
    make_datahub_dataset_urn,
    make_datahub_flow_urn,
    make_datahub_job_urn,
    make_datahub_platform_instance_urn,
    make_datahub_platform_urn,
    require_datahub_fabric,
    validate_datahub_identity_claims,
    validate_datahub_proposals,
)


def _config(*, kafka_instance: str | None = "main") -> DataHubIdentityConfig:
    return DataHubIdentityConfig(
        catalog_id="payments",
        fabric="PROD",
        kafka_platform_instance=kafka_instance,
        gateway_platform_id="kafka",
        gateway_platform_instance="gateway",
    )


def _bypass(record: object, **changes: object) -> object:
    clone = object.__new__(type(record))
    clone.__dict__.update(record.__dict__)
    for field, value in changes.items():
        object.__setattr__(clone, field, value)
    return clone


def _identities() -> tuple[
    DataHubIdentityConfig,
    DataHubFlowIdentity,
    tuple[DataHubDatasetIdentity, ...],
    tuple[DataHubJobIdentity, ...],
    tuple[DataHubJobTopology, ...],
]:
    config = _config()
    flow = DataHubFlowIdentity.from_config(config)
    source = DataHubDatasetIdentity.from_config(
        config,
        logical_kind="source",
        logical_name="orders",
        transport="kafka",
        physical_name="orders.raw",
    )
    model = DataHubDatasetIdentity.from_config(
        config,
        logical_kind="model",
        logical_name="clean_orders",
        transport="kafka",
        physical_name="orders.clean",
        contract_status="enforced",
    )
    gateway = DataHubDatasetIdentity.from_config(
        config,
        logical_kind="model",
        logical_name="public_orders",
        transport="gateway",
        physical_name="orders.public",
    )
    flink_job = DataHubJobIdentity.from_config(
        config,
        process_logical_name="clean_orders",
        process_kind="flink",
    )
    gateway_job = DataHubJobIdentity.from_config(
        config,
        process_logical_name="public_orders",
        process_kind="gateway",
    )
    sink_job = DataHubJobIdentity.from_config(
        config,
        process_logical_name="archive_orders",
        process_kind="connect",
    )
    topology = (
        DataHubJobTopology(
            job_urn=flink_job.urn,
            input_dataset_urns=(source.urn,),
            output_dataset_urns=(model.urn,),
        ),
        DataHubJobTopology(
            job_urn=gateway_job.urn,
            input_dataset_urns=(model.urn,),
            output_dataset_urns=(gateway.urn,),
        ),
        DataHubJobTopology(
            job_urn=sink_job.urn,
            input_dataset_urns=(gateway.urn,),
            output_dataset_urns=(),
        ),
    )
    return config, flow, (source, model, gateway), (flink_job, gateway_job, sink_job), topology


def _valid_proposals() -> tuple[
    tuple[DataHubProposal, ...],
    DataHubIdentityConfig,
    DataHubFlowIdentity,
    tuple[DataHubDatasetIdentity, ...],
    tuple[DataHubJobIdentity, ...],
    tuple[DataHubJobTopology, ...],
]:
    config, flow, datasets, jobs, topology = _identities()
    proposal_by_key: dict[tuple[str, str], DataHubProposal] = {}
    proposal_by_key[(flow.urn, "dataFlowInfo")] = DataHubProposal.create(
        entity_type="dataFlow",
        entity_urn=flow.urn,
        aspect_name="dataFlowInfo",
        aspect={
            "name": "Payments",
            "env": "PROD",
            "customProperties": {},
        },
    )
    for dataset in datasets:
        custom_properties = (
            {"streamt.contract.status": dataset.contract_status}
            if dataset.contract_status is not None
            else {}
        )
        proposal_by_key[(dataset.urn, "datasetProperties")] = DataHubProposal.create(
            entity_type="dataset",
            entity_urn=dataset.urn,
            aspect_name="datasetProperties",
            aspect={
                "name": dataset.logical_name,
                "customProperties": custom_properties,
                "tags": [],
            },
        )
        if dataset.platform_instance_urn is not None:
            proposal_by_key[(dataset.urn, "dataPlatformInstance")] = DataHubProposal.create(
                entity_type="dataset",
                entity_urn=dataset.urn,
                aspect_name="dataPlatformInstance",
                aspect={
                    "platform": dataset.platform_urn,
                    "instance": dataset.platform_instance_urn,
                },
            )
    topology_by_job = {item.job_urn: item for item in topology}
    for job in jobs:
        proposal_by_key[(job.urn, "dataJobInfo")] = DataHubProposal.create(
            entity_type="dataJob",
            entity_urn=job.urn,
            aspect_name="dataJobInfo",
            aspect={
                "name": job.process_logical_name,
                "type": {"string": job.process_kind},
                "flowUrn": flow.urn,
                "env": flow.fabric,
                "customProperties": {},
            },
        )
        edges = topology_by_job[job.urn]
        proposal_by_key[(job.urn, "dataJobInputOutput")] = DataHubProposal.create(
            entity_type="dataJob",
            entity_urn=job.urn,
            aspect_name="dataJobInputOutput",
            aspect={
                "inputDatasets": [],
                "outputDatasets": [],
                "inputDatasetEdges": [{"destinationUrn": urn} for urn in edges.input_dataset_urns],
                "outputDatasetEdges": [
                    {"destinationUrn": urn} for urn in edges.output_dataset_urns
                ],
            },
        )
    order: list[tuple[str, str]] = [(flow.urn, "dataFlowInfo")]
    for dataset in sorted(datasets, key=lambda item: item.urn.encode("utf-8")):
        order.append((dataset.urn, "datasetProperties"))
        if dataset.platform_instance is not None:
            order.append((dataset.urn, "dataPlatformInstance"))
    for job in sorted(jobs, key=lambda item: item.urn.encode("utf-8")):
        order.extend(((job.urn, "dataJobInfo"), (job.urn, "dataJobInputOutput")))
    proposals = tuple(proposal_by_key[key] for key in order)
    return proposals, config, flow, datasets, jobs, topology


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("a,b", "a%2Cb"),
        ("a(b)", "a%28b%29"),
        ("a␟b", "a%E2%90%9Fb"),
        ("a/b c:+&$", "a/b c:+&$"),
        ("a%2Cb", "a%2Cb"),
    ],
)
def test_exact_v170_component_encoding(raw: str, expected: str) -> None:
    assert encode_datahub_urn_component(raw) == expected


def test_literal_percent_alias_is_preserved_and_detectable() -> None:
    assert make_datahub_flow_urn("a,b", "PROD") == make_datahub_flow_urn("a%2Cb", "PROD")


def test_public_encoder_rejects_non_strings_safely() -> None:
    with pytest.raises(DataHubValidationError, match="must be strings"):
        encode_datahub_urn_component(b"not-text")


def test_builders_match_the_pinned_v170_identity_fixture() -> None:
    fixture_path = (
        Path(__file__).parents[1] / "fixtures" / "datahub" / "v1.7.0" / "identity-vectors.json"
    )
    fixture = json.loads(fixture_path.read_text(encoding="utf-8"))
    for item in fixture["data_platforms"]:
        assert make_datahub_platform_urn(item["platform"]) == item["urn"]
    for item in fixture["data_platform_instances"]:
        assert make_datahub_platform_instance_urn(item["platform"], item["instance"]) == item["urn"]
    for item in fixture["datasets"]:
        assert (
            make_datahub_dataset_urn(
                item["platform"],
                item["name"],
                item["fabric"],
                platform_instance=item["instance"],
            )
            == item["urn"]
        )
    for item in fixture["data_flows"]:
        assert make_datahub_flow_urn(item["flow_id"], item["fabric"]) == item["urn"]
    for item in fixture["data_jobs"]:
        flow_urn = make_datahub_flow_urn(item["flow_id"], item["fabric"])
        assert make_datahub_job_urn(flow_urn, item["job_id"]) == item["urn"]


@pytest.mark.parametrize("fabric", sorted(DATAHUB_FABRICS))
def test_every_exact_fabric_is_accepted(fabric: str) -> None:
    assert require_datahub_fabric(fabric) == fabric


@pytest.mark.parametrize("fabric", ["prod", " PROD", "PROD ", "UNKNOWN", 1, None])
def test_non_exact_fabric_is_rejected(fabric: object) -> None:
    with pytest.raises(DataHubValidationError, match="exact uppercase"):
        require_datahub_fabric(fabric)


def test_exact_urn_vectors_with_and_without_instance() -> None:
    platform = make_datahub_platform_urn("kafka")
    instance = make_datahub_platform_instance_urn("kafka", "main")
    with_instance = make_datahub_dataset_urn("kafka", "orders", "PROD", platform_instance="main")
    without_instance = make_datahub_dataset_urn("kafka", "orders", "PROD")
    flow = make_datahub_flow_urn("payments", "PROD")
    job = make_datahub_job_urn(flow, "clean_orders")

    assert platform == "urn:li:dataPlatform:kafka"
    assert instance == ("urn:li:dataPlatformInstance:(urn:li:dataPlatform:kafka,main)")
    assert with_instance == ("urn:li:dataset:(urn:li:dataPlatform:kafka,main.orders,PROD)")
    assert without_instance == ("urn:li:dataset:(urn:li:dataPlatform:kafka,orders,PROD)")
    assert flow == "urn:li:dataFlow:(streamt,payments,PROD)"
    assert job == ("urn:li:dataJob:(urn:li:dataFlow:(streamt,payments,PROD),clean_orders)")


def test_dataset_concatenates_before_reserved_encoding() -> None:
    assert make_datahub_dataset_urn(
        "custom(platform)",
        "orders,raw",
        "QA",
        platform_instance="east(region)",
    ) == (
        "urn:li:dataset:(urn:li:dataPlatform:custom%28platform%29,east%28region%29.orders%2Craw,QA)"
    )


@pytest.mark.parametrize("value", ["", "   ", "bad\nvalue", "bad\ud800value"])
def test_identifiers_reject_blank_controls_and_surrogates(value: str) -> None:
    with pytest.raises(DataHubValidationError):
        make_datahub_flow_urn(value, "PROD")


@pytest.mark.parametrize("value", ["urn:li:dataPlatform:kafka", "urn:anything"])
def test_platform_and_instance_inputs_must_be_bare(value: str) -> None:
    with pytest.raises(DataHubValidationError, match="bare identifier"):
        make_datahub_platform_urn(value)
    with pytest.raises(DataHubValidationError, match="bare identifier"):
        make_datahub_platform_instance_urn("kafka", value)


def test_encoded_utf16_and_complete_urn_boundaries() -> None:
    assert make_datahub_flow_urn("a" * 200, "PROD")
    assert make_datahub_job_urn(
        make_datahub_flow_urn("flow", "PROD"),
        "🙂" * 100,
    )
    assert make_datahub_platform_urn("p" * 25)
    assert make_datahub_platform_instance_urn("kafka", "i" * 44)
    assert make_datahub_dataset_urn("kafka", "t" * 210, "PROD")

    for call in (
        lambda: make_datahub_flow_urn("a" * 201, "PROD"),
        lambda: make_datahub_flow_urn("," * 67, "PROD"),
        lambda: make_datahub_job_urn(
            make_datahub_flow_urn("flow", "PROD"),
            "🙂" * 101,
        ),
        lambda: make_datahub_platform_urn("p" * 26),
        lambda: make_datahub_platform_instance_urn("kafka", "i" * 45),
        lambda: make_datahub_dataset_urn("kafka", "t" * 211, "PROD"),
    ):
        with pytest.raises(DataHubValidationError, match=r"length limit|complete-URN"):
            call()


def test_config_enforces_gateway_pair_and_allows_optional_kafka_instance() -> None:
    assert _config(kafka_instance=None).kafka_platform_instance is None
    with pytest.raises(DataHubValidationError, match="supplied together"):
        DataHubIdentityConfig(
            catalog_id="payments",
            fabric="PROD",
            gateway_platform_id="kafka",
        )
    with pytest.raises(DataHubValidationError, match="explicit platform"):
        DataHubDatasetIdentity.from_config(
            DataHubIdentityConfig(catalog_id="payments", fabric="PROD"),
            logical_kind="model",
            logical_name="public_orders",
            transport="gateway",
            physical_name="public.orders",
        )


def test_identity_records_are_frozen_consistent_and_redacted() -> None:
    config, flow, datasets, jobs, topology = _identities()
    assert "payments" not in repr(config)
    assert "orders.raw" not in repr(datasets[0])
    assert "clean_orders" not in repr(jobs[0])
    assert datasets[0].logical_identity == ("source", "orders")
    with pytest.raises(FrozenInstanceError):
        flow.urn = "changed"  # type: ignore[misc]
    with pytest.raises(DataHubValidationError, match="does not match"):
        replace(datasets[0], urn="urn:li:dataset:bad")
    with pytest.raises(DataHubValidationError, match="contract"):
        replace(datasets[0], contract_status="declared")
    validate_datahub_identity_claims(
        datasets,
        jobs,
        topology,
        config=config,
        flow=flow,
    )


def test_direct_dataset_construction_enforces_transport_identity() -> None:
    _, _, datasets, _, _ = _identities()
    source = datasets[0]
    custom_platform = make_datahub_platform_urn("custom")
    custom_instance = make_datahub_platform_instance_urn("custom", "main")
    custom_urn = make_datahub_dataset_urn(
        "custom",
        source.physical_name,
        source.fabric,
        platform_instance="main",
    )
    with pytest.raises(DataHubValidationError, match="fixed kafka"):
        replace(
            source,
            platform_id="custom",
            platform_urn=custom_platform,
            platform_instance_urn=custom_instance,
            urn=custom_urn,
        )

    model = datasets[1]
    no_instance_urn = make_datahub_dataset_urn(
        "kafka",
        model.physical_name,
        model.fabric,
    )
    with pytest.raises(DataHubValidationError, match="explicit platform instance"):
        replace(
            model,
            transport="gateway",
            platform_instance=None,
            platform_instance_urn=None,
            urn=no_instance_urn,
        )


def test_no_instance_and_prefixed_dataset_collision_is_rejected() -> None:
    config = DataHubIdentityConfig(
        catalog_id="payments",
        fabric="PROD",
        gateway_platform_id="kafka",
        gateway_platform_instance="main",
    )
    kafka = DataHubDatasetIdentity.from_config(
        config,
        logical_kind="source",
        logical_name="qualified_orders",
        transport="kafka",
        physical_name="main.orders",
    )
    gateway = DataHubDatasetIdentity.from_config(
        config,
        logical_kind="model",
        logical_name="public_orders",
        transport="gateway",
        physical_name="orders",
    )
    assert kafka.urn == gateway.urn
    with pytest.raises(DataHubValidationError, match="one DataHub URN"):
        validate_datahub_identity_claims(
            (kafka, gateway),
            (),
            (),
            config=config,
            flow=DataHubFlowIdentity.from_config(config),
        )


def test_percent_alias_job_collision_is_rejected() -> None:
    config = _config()
    first = DataHubJobIdentity.from_config(
        config,
        process_logical_name="clean,orders",
        process_kind="flink",
    )
    second = DataHubJobIdentity.from_config(
        config,
        process_logical_name="clean%2Corders",
        process_kind="flink",
    )
    assert first.urn == second.urn
    with pytest.raises(DataHubValidationError, match="one DataJob URN"):
        validate_datahub_identity_claims(
            (),
            (first, second),
            (),
            config=config,
            flow=DataHubFlowIdentity.from_config(config),
        )


def test_platform_instance_percent_alias_collision_is_rejected() -> None:
    config = DataHubIdentityConfig(
        catalog_id="payments",
        fabric="PROD",
        kafka_platform_instance="edge,west",
        gateway_platform_id="kafka",
        gateway_platform_instance="edge%2Cwest",
    )
    kafka = DataHubDatasetIdentity.from_config(
        config,
        logical_kind="source",
        logical_name="orders",
        transport="kafka",
        physical_name="orders.raw",
    )
    gateway = DataHubDatasetIdentity.from_config(
        config,
        logical_kind="model",
        logical_name="public_orders",
        transport="gateway",
        physical_name="orders.public",
    )
    assert kafka.urn != gateway.urn
    assert kafka.platform_instance_urn == gateway.platform_instance_urn
    with pytest.raises(DataHubValidationError, match="raw platform-instance"):
        validate_datahub_identity_claims(
            (kafka, gateway),
            (),
            (),
            config=config,
            flow=DataHubFlowIdentity.from_config(config),
        )


def test_topology_rejects_dangling_wrong_and_shared_outputs() -> None:
    config, flow, datasets, jobs, topology = _identities()
    dangling = replace(
        topology[0],
        input_dataset_urns=(
            make_datahub_dataset_urn("kafka", "missing", "PROD", platform_instance="main"),
        ),
    )
    with pytest.raises(DataHubValidationError, match="not emitted"):
        validate_datahub_identity_claims(
            datasets,
            jobs,
            (dangling, *topology[1:]),
            config=config,
            flow=flow,
        )

    wrong = replace(topology[0], output_dataset_urns=(datasets[2].urn,))
    with pytest.raises(DataHubValidationError, match="same-name"):
        validate_datahub_identity_claims(
            datasets,
            jobs,
            (wrong, *topology[1:]),
            config=config,
            flow=flow,
        )

    second_flink = replace(
        jobs[1],
        process_logical_name=jobs[0].process_logical_name,
        process_kind="flink",
        urn=jobs[0].urn,
    )
    with pytest.raises(DataHubValidationError):
        validate_datahub_identity_claims(
            datasets,
            (jobs[0], second_flink),
            topology[:2],
            config=config,
            flow=flow,
        )


def test_topology_record_requires_sorted_unique_direct_edges() -> None:
    _, _, datasets, jobs, _ = _identities()
    ordered = tuple(sorted((datasets[0].urn, datasets[1].urn), key=lambda urn: urn.encode("utf-8")))
    with pytest.raises(DataHubValidationError, match="unique and sorted"):
        DataHubJobTopology(
            job_urn=jobs[2].urn,
            input_dataset_urns=tuple(reversed(ordered)),
            output_dataset_urns=(),
        )


def test_topology_must_have_exactly_one_record_per_job() -> None:
    config, flow, datasets, jobs, topology = _identities()
    with pytest.raises(DataHubValidationError, match="exactly one topology"):
        validate_datahub_identity_claims(
            datasets,
            jobs,
            topology[:-1],
            config=config,
            flow=flow,
        )
    with pytest.raises(DataHubValidationError, match="not emitted"):
        validate_datahub_identity_claims(
            datasets,
            jobs[:-1],
            topology,
            config=config,
            flow=flow,
        )


def test_final_validation_contains_constructor_bypass_failures() -> None:
    proposals, config, flow, datasets, jobs, topology = _valid_proposals()
    invalid_identity_calls = (
        lambda: validate_datahub_identity_claims(
            datasets,
            jobs,
            topology,
            config=_bypass(config, catalog_id=[]),  # type: ignore[arg-type]
            flow=flow,
        ),
        lambda: validate_datahub_identity_claims(
            datasets,
            jobs,
            topology,
            config=config,
            flow=_bypass(flow, urn=b"\xff"),  # type: ignore[arg-type]
        ),
        lambda: validate_datahub_identity_claims(
            (_bypass(datasets[0], logical_kind=[]), *datasets[1:]),  # type: ignore[arg-type]
            jobs,
            topology,
            config=config,
            flow=flow,
        ),
        lambda: validate_datahub_identity_claims(
            datasets,
            (_bypass(jobs[0], process_logical_name=b"\xff"), *jobs[1:]),  # type: ignore[arg-type]
            topology,
            config=config,
            flow=flow,
        ),
        lambda: validate_datahub_identity_claims(
            datasets,
            jobs,
            (_bypass(topology[0], input_dataset_urns=[]), *topology[1:]),  # type: ignore[arg-type]
            config=config,
            flow=flow,
        ),
    )
    for call in invalid_identity_calls:
        with pytest.raises(DataHubValidationError):
            call()

    invalid_proposal = _bypass(proposals[0], _aspect_payload=b"\xff")
    with pytest.raises(DataHubValidationError, match="canonical JSON text"):
        validate_datahub_proposals(
            (invalid_proposal, *proposals[1:]),  # type: ignore[arg-type]
            config=config,
            flow=flow,
            datasets=datasets,
            jobs=jobs,
            topology=topology,
        )
    with pytest.raises(DataHubValidationError, match="unique and sorted"):
        DataHubJobTopology(
            job_urn=jobs[2].urn,
            input_dataset_urns=(datasets[0].urn, datasets[0].urn),
            output_dataset_urns=(),
        )


def test_proposal_record_is_strict_defensive_and_redacted() -> None:
    nested: dict[str, object] = {"name": "Payments", "items": ["a"]}
    proposal = DataHubProposal.create(
        entity_type="dataFlow",
        entity_urn=make_datahub_flow_urn("secret-flow", "PROD"),
        aspect_name="dataFlowInfo",
        aspect=nested,
    )
    cast_items = nested["items"]
    assert isinstance(cast_items, list)
    cast_items.append("b")
    returned = proposal.aspect
    returned_items = returned["items"]
    assert isinstance(returned_items, list)
    returned_items.append("c")
    assert proposal.aspect["items"] == ["a"]
    assert "secret-flow" not in repr(proposal)
    with pytest.raises(DataHubValidationError, match="exactly"):
        DataHubProposal.from_dict({**proposal.to_dict(), "extra": True})
    with pytest.raises(DataHubValidationError, match="UPSERT"):
        DataHubProposal.from_dict({**proposal.to_dict(), "changeType": "PATCH"})


def test_complete_closed_proposal_set_is_valid() -> None:
    proposals, config, flow, datasets, jobs, topology = _valid_proposals()
    validate_datahub_proposals(
        proposals,
        config=config,
        flow=flow,
        datasets=datasets,
        jobs=jobs,
        topology=topology,
    )


def test_multiline_description_from_neutral_projection_is_valid() -> None:
    proposals, config, flow, datasets, jobs, topology = _valid_proposals()
    described_flow = DataHubProposal.create(
        entity_type="dataFlow",
        entity_urn=flow.urn,
        aspect_name="dataFlowInfo",
        aspect={**proposals[0].aspect, "description": "First line\nSecond line"},
    )
    validate_datahub_proposals(
        (described_flow, *proposals[1:]),
        config=config,
        flow=flow,
        datasets=datasets,
        jobs=jobs,
        topology=topology,
    )


def test_closed_proposal_set_rejects_order_fields_and_identity_mismatch() -> None:
    proposals, config, flow, datasets, jobs, topology = _valid_proposals()
    with pytest.raises(DataHubValidationError, match="canonical order"):
        validate_datahub_proposals(
            (proposals[1], proposals[0], *proposals[2:]),
            config=config,
            flow=flow,
            datasets=datasets,
            jobs=jobs,
            topology=topology,
        )

    changed = DataHubProposal.create(
        entity_type="dataFlow",
        entity_urn=flow.urn,
        aspect_name="dataFlowInfo",
        aspect={
            **proposals[0].aspect,
            "unexpected": "forbidden",
        },
    )
    with pytest.raises(DataHubValidationError, match="closed"):
        validate_datahub_proposals(
            (changed, *proposals[1:]),
            config=config,
            flow=flow,
            datasets=datasets,
            jobs=jobs,
            topology=topology,
        )

    mismatched = DataHubProposal.create(
        entity_type="dataset",
        entity_urn=flow.urn,
        aspect_name="dataFlowInfo",
        aspect=proposals[0].aspect,
    )
    with pytest.raises(DataHubValidationError, match="does not match"):
        validate_datahub_proposals(
            (mismatched, *proposals[1:]),
            config=config,
            flow=flow,
            datasets=datasets,
            jobs=jobs,
            topology=topology,
        )


def test_no_instance_dataset_has_no_platform_instance_proposal() -> None:
    config = DataHubIdentityConfig(catalog_id="payments", fabric="PROD")
    flow = DataHubFlowIdentity.from_config(config)
    dataset = DataHubDatasetIdentity.from_config(
        config,
        logical_kind="source",
        logical_name="orders",
        transport="kafka",
        physical_name="orders",
    )
    proposal_set = (
        DataHubProposal.create(
            entity_type="dataFlow",
            entity_urn=flow.urn,
            aspect_name="dataFlowInfo",
            aspect={"name": "Payments", "env": "PROD", "customProperties": {}},
        ),
        DataHubProposal.create(
            entity_type="dataset",
            entity_urn=dataset.urn,
            aspect_name="datasetProperties",
            aspect={"name": "orders", "customProperties": {}, "tags": []},
        ),
    )
    validate_datahub_proposals(
        proposal_set,
        config=config,
        flow=flow,
        datasets=(dataset,),
        jobs=(),
        topology=(),
    )


def test_failures_do_not_echo_raw_identity_values() -> None:
    sentinel = "do-not-leak-secret"
    with pytest.raises(DataHubValidationError) as exc_info:
        make_datahub_flow_urn(f"{sentinel}\n", "PROD")
    assert sentinel not in str(exc_info.value)
