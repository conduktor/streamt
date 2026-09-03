"""Pure neutral-catalog to DataHub MCP mapping and canonical serialization."""

from __future__ import annotations

import json
import unicodedata
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from types import MappingProxyType
from typing import TypeVar

from streamt.integrations.catalog.datahub import (
    DataHubDatasetIdentity,
    DataHubEntityType,
    DataHubFlowIdentity,
    DataHubIdentityConfig,
    DataHubJobIdentity,
    DataHubJobTopology,
    DataHubProposal,
    DataHubValidationError,
    require_datahub_fabric,
    validate_datahub_identity_claims,
    validate_datahub_proposals,
)
from streamt.integrations.catalog.model import (
    CatalogContractSummary,
    CatalogDataset,
    CatalogDependency,
    CatalogOwnerLabel,
    CatalogProcess,
    CatalogSnapshot,
    LogicalDataset,
)

_WARNING_MESSAGES = {
    "W115_DATAHUB_SINK_OUTPUT_OMITTED": (
        "Connector destination metadata is omitted from DataHub export"
    ),
    "W116_DATAHUB_EXPOSURE_OMITTED": ("Exposure metadata is omitted from DataHub export"),
    "W117_DATAHUB_TAGS_OMITTED": "Declared tags are omitted from DataHub export",
    "W118_DATAHUB_OWNER_OMITTED": "Declared owner is omitted from DataHub export",
}
_T = TypeVar("_T")


class DataHubExportError(ValueError):
    """A bounded, secret-neutral DataHub mapping failure."""

    def __init__(self, message: str, *, location: str) -> None:
        super().__init__(message)
        self.location = location


@dataclass(frozen=True, order=True)
class DataHubExportWarning:
    """One deterministic warning for metadata deliberately omitted in v1."""

    code: str
    message: str
    location: str


@dataclass(frozen=True, repr=False, init=False)
class DataHubCatalogExport:
    """Immutable validated proposals, warnings, and canonical JSON bytes."""

    _proposal_payload: str
    _warning_payloads: tuple[str, ...]

    def __init__(self) -> None:
        raise TypeError("DataHubCatalogExport is created by generate_datahub_catalog")

    @classmethod
    def _from_validated(
        cls,
        proposals: tuple[DataHubProposal, ...],
        warnings: tuple[DataHubExportWarning, ...],
    ) -> DataHubCatalogExport:
        """Construct only from values validated by the complete mapper."""
        if not isinstance(proposals, tuple) or not all(
            isinstance(proposal, DataHubProposal) for proposal in proposals
        ):
            raise DataHubExportError(
                "DataHub export proposals must be immutable records",
                location="export.proposals",
            )
        if not proposals:
            raise DataHubExportError(
                "DataHub export must contain a non-empty proposal array",
                location="export.proposals",
            )
        if not isinstance(warnings, tuple) or not all(
            isinstance(warning, DataHubExportWarning) for warning in warnings
        ):
            raise DataHubExportError(
                "DataHub export warnings must be immutable records",
                location="export.warnings",
            )
        if warnings != tuple(
            sorted(warnings, key=lambda warning: (warning.location, warning.code))
        ):
            raise DataHubExportError(
                "DataHub export warnings must be canonically ordered",
                location="export.warnings",
            )
        proposal_payload = _serialize_proposals(proposals)
        warning_payloads = tuple(
            _serialize_warning(warning, position=index) for index, warning in enumerate(warnings)
        )
        result = object.__new__(cls)
        object.__setattr__(result, "_proposal_payload", proposal_payload)
        object.__setattr__(result, "_warning_payloads", warning_payloads)
        return result

    @property
    def proposals(self) -> tuple[DataHubProposal, ...]:
        """Return fresh immutable proposal records."""
        decoded = json.loads(self._proposal_payload)
        return tuple(DataHubProposal.from_dict(item) for item in decoded)

    @property
    def warnings(self) -> tuple[DataHubExportWarning, ...]:
        """Return fresh immutable warning records."""
        return tuple(_parse_warning(payload) for payload in self._warning_payloads)

    @property
    def json_text(self) -> str:
        """Return canonical UTF-8 JSON text with one final newline."""
        return self._proposal_payload

    @property
    def json_bytes(self) -> bytes:
        """Return canonical UTF-8 JSON bytes."""
        return self._proposal_payload.encode("utf-8")

    def __repr__(self) -> str:
        return (
            "DataHubCatalogExport("
            f"proposal_count={len(self.proposals)}, "
            f"warning_count={len(self.warnings)})"
        )


@dataclass(frozen=True)
class _SnapshotIndex:
    datasets: Mapping[LogicalDataset, CatalogDataset]
    processes: Mapping[str, CatalogProcess]


def generate_datahub_catalog(
    snapshot: CatalogSnapshot,
    catalog_id: object,
    fabric: object,
    kafka_platform_instance: object | None = None,
    gateway_platform_id: object | None = None,
    gateway_platform_instance: object | None = None,
) -> DataHubCatalogExport:
    """Map one neutral snapshot to validated canonical DataHub MCP JSON."""
    try:
        return _generate_datahub_catalog(
            snapshot,
            catalog_id=catalog_id,
            fabric=fabric,
            kafka_platform_instance=kafka_platform_instance,
            gateway_platform_id=gateway_platform_id,
            gateway_platform_instance=gateway_platform_instance,
        )
    except DataHubExportError:
        raise
    except Exception:
        raise DataHubExportError(
            "DataHub export failed safely",
            location="export",
        ) from None


def _generate_datahub_catalog(
    snapshot: CatalogSnapshot,
    *,
    catalog_id: object,
    fabric: object,
    kafka_platform_instance: object | None,
    gateway_platform_id: object | None,
    gateway_platform_instance: object | None,
) -> DataHubCatalogExport:
    """Internal implementation protected by the public failure boundary."""
    config = _validate_inputs(
        catalog_id=catalog_id,
        fabric=fabric,
        kafka_platform_instance=kafka_platform_instance,
        gateway_platform_id=gateway_platform_id,
        gateway_platform_instance=gateway_platform_instance,
    )
    index = _validate_snapshot(snapshot)
    if any(dataset.transport == "gateway" for dataset in index.datasets.values()) and (
        config.gateway_platform_id is None or config.gateway_platform_instance is None
    ):
        raise DataHubExportError(
            "Gateway Datasets require an explicit platform and instance",
            location="gateway_platform",
        )

    flow = _foundation(
        lambda: DataHubFlowIdentity.from_config(config),
    )
    dataset_identities = tuple(
        _dataset_identity(config, dataset) for dataset in index.datasets.values()
    )
    job_identities = tuple(_job_identity(config, process) for process in index.processes.values())
    datasets_by_logical = {identity.logical_identity: identity for identity in dataset_identities}
    jobs_by_name = {identity.process_logical_name: identity for identity in job_identities}
    topology = tuple(
        _topology_for_process(
            process,
            job=jobs_by_name[process.logical_name],
            datasets=datasets_by_logical,
        )
        for process in index.processes.values()
    )

    _foundation(
        lambda: validate_datahub_identity_claims(
            dataset_identities,
            job_identities,
            topology,
            config=config,
            flow=flow,
        )
    )
    proposals = _build_proposals(
        snapshot,
        index=index,
        flow=flow,
        datasets=dataset_identities,
        jobs=job_identities,
        topology=topology,
    )
    _foundation(
        lambda: validate_datahub_proposals(
            proposals,
            config=config,
            flow=flow,
            datasets=dataset_identities,
            jobs=job_identities,
            topology=topology,
        )
    )
    _validate_exact_semantics(
        snapshot,
        index=index,
        config=config,
        flow=flow,
        datasets=dataset_identities,
        jobs=job_identities,
        topology=topology,
        proposals=proposals,
    )
    return DataHubCatalogExport._from_validated(
        proposals,
        _warnings(snapshot, index),
    )


def _validate_inputs(
    *,
    catalog_id: object,
    fabric: object,
    kafka_platform_instance: object | None,
    gateway_platform_id: object | None,
    gateway_platform_instance: object | None,
) -> DataHubIdentityConfig:
    if not isinstance(catalog_id, str):
        raise DataHubExportError(
            "Catalog ID must be a string",
            location="catalog_id",
        )
    exact_fabric = _foundation(lambda: require_datahub_fabric(fabric))
    kafka_instance = _optional_input_text(
        kafka_platform_instance,
        location="kafka_platform_instance",
    )
    gateway_platform = _optional_input_text(
        gateway_platform_id,
        location="gateway_platform_id",
    )
    gateway_instance = _optional_input_text(
        gateway_platform_instance,
        location="gateway_platform_instance",
    )
    return _foundation(
        lambda: DataHubIdentityConfig(
            catalog_id=catalog_id,
            fabric=exact_fabric,
            kafka_platform_instance=kafka_instance,
            gateway_platform_id=gateway_platform,
            gateway_platform_instance=gateway_instance,
        )
    )


def _optional_input_text(value: object | None, *, location: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise DataHubExportError(
            "DataHub identity option must be a string",
            location=location,
        )
    return value


def _dataset_identity(
    config: DataHubIdentityConfig,
    dataset: CatalogDataset,
) -> DataHubDatasetIdentity:
    return _foundation(
        lambda: DataHubDatasetIdentity.from_config(
            config,
            logical_kind=dataset.logical_kind,
            logical_name=dataset.logical_name,
            transport=dataset.transport,
            physical_name=dataset.physical_name,
            contract_status=(dataset.contract.status if dataset.contract is not None else None),
        )
    )


def _job_identity(
    config: DataHubIdentityConfig,
    process: CatalogProcess,
) -> DataHubJobIdentity:
    return _foundation(
        lambda: DataHubJobIdentity.from_config(
            config,
            process_logical_name=process.logical_name,
            process_kind=process.process_kind,
        )
    )


def _foundation(operation: Callable[[], _T]) -> _T:
    """Translate foundation failures without exposing raw inputs or exceptions."""
    try:
        return operation()
    except DataHubValidationError as error:
        raise DataHubExportError(str(error), location=error.location) from None


def _validate_snapshot(snapshot: CatalogSnapshot) -> _SnapshotIndex:
    if not isinstance(snapshot, CatalogSnapshot):
        raise DataHubExportError(
            "DataHub export requires a catalog snapshot",
            location="snapshot",
        )
    if type(snapshot.version) is not int or snapshot.version != 1:
        raise DataHubExportError(
            "Catalog snapshot version is unsupported",
            location="snapshot.version",
        )
    _snapshot_text(snapshot.project_name, location="snapshot.project_name")
    _snapshot_description(
        snapshot.project_description,
        location="snapshot.project_description",
    )
    _snapshot_text(
        snapshot.effective_environment,
        location="snapshot.effective_environment",
    )
    if not isinstance(snapshot.datasets, tuple):
        raise DataHubExportError(
            "Catalog datasets must be an immutable tuple",
            location="snapshot.datasets",
        )
    if not isinstance(snapshot.processes, tuple):
        raise DataHubExportError(
            "Catalog processes must be an immutable tuple",
            location="snapshot.processes",
        )
    if not isinstance(snapshot.omitted_exposure_names, tuple):
        raise DataHubExportError(
            "Omitted exposures must be an immutable tuple",
            location="snapshot.omitted_exposure_names",
        )

    datasets: dict[LogicalDataset, CatalogDataset] = {}
    physical: set[tuple[str, str]] = set()
    logical_names: dict[str, str] = {}
    for position, dataset in enumerate(snapshot.datasets):
        location = f"snapshot.datasets/{position}"
        _validate_dataset(dataset, location=location)
        logical = dataset.logical_identity
        if logical in datasets:
            raise DataHubExportError(
                "Catalog dataset logical identities must be unique",
                location=location,
            )
        prior_kind = logical_names.get(dataset.logical_name)
        if prior_kind is not None and prior_kind != dataset.logical_kind:
            raise DataHubExportError(
                "Source and model logical names must not collide",
                location=f"{location}/logical_name",
            )
        if dataset.physical_identity in physical:
            raise DataHubExportError(
                "Catalog dataset physical identities must be unique",
                location=f"{location}/physical_name",
            )
        datasets[logical] = dataset
        physical.add(dataset.physical_identity)
        logical_names[dataset.logical_name] = dataset.logical_kind

    processes: dict[str, CatalogProcess] = {}
    for position, process in enumerate(snapshot.processes):
        location = f"snapshot.processes/{position}"
        _validate_process(process, datasets=datasets, location=location)
        if process.logical_name in processes:
            raise DataHubExportError(
                "Catalog process logical identities must be unique",
                location=f"{location}/logical_name",
            )
        if ("source", process.logical_name) in datasets:
            raise DataHubExportError(
                "A catalog process cannot use a source logical identity",
                location=f"{location}/logical_name",
            )
        output = datasets.get(("model", process.logical_name))
        if process.process_kind == "connect":
            if output is not None:
                raise DataHubExportError(
                    "Connect processes cannot expose catalog output datasets",
                    location=location,
                )
        else:
            expected_transport = "gateway" if process.process_kind == "gateway" else "kafka"
            if output is None or output.transport != expected_transport:
                raise DataHubExportError(
                    "Catalog process output shape is inconsistent",
                    location=location,
                )
            if (
                output.description != process.description
                or output.tags != process.tags
                or output.owner != process.owner
            ):
                raise DataHubExportError(
                    "Catalog process and output metadata are inconsistent",
                    location=location,
                )
        processes[process.logical_name] = process

    for position, dataset in enumerate(snapshot.datasets):
        if (
            dataset.logical_kind == "model"
            and dataset.transport == "gateway"
            and dataset.logical_name not in processes
        ):
            raise DataHubExportError(
                "Gateway output dataset requires its compiled process",
                location=f"snapshot.datasets/{position}",
            )
    _validate_process_cycles(processes)
    for position, exposure in enumerate(snapshot.omitted_exposure_names):
        _snapshot_text(
            exposure,
            location=f"snapshot.omitted_exposure_names/{position}",
        )

    return _SnapshotIndex(
        datasets=MappingProxyType(
            dict(
                sorted(
                    datasets.items(),
                    key=lambda item: (item[0][0], item[0][1].encode("utf-8")),
                )
            )
        ),
        processes=MappingProxyType(
            dict(
                sorted(
                    processes.items(),
                    key=lambda item: item[0].encode("utf-8"),
                )
            )
        ),
    )


def _validate_dataset(dataset: object, *, location: str) -> None:
    if not isinstance(dataset, CatalogDataset):
        raise DataHubExportError("Catalog dataset has an invalid type", location=location)
    if not isinstance(dataset.logical_kind, str) or dataset.logical_kind not in {
        "source",
        "model",
    }:
        raise DataHubExportError(
            "Catalog dataset logical kind is unsupported",
            location=f"{location}/logical_kind",
        )
    if not isinstance(dataset.transport, str) or dataset.transport not in {
        "kafka",
        "gateway",
    }:
        raise DataHubExportError(
            "Catalog dataset transport is unsupported",
            location=f"{location}/transport",
        )
    if dataset.logical_kind == "source" and dataset.transport != "kafka":
        raise DataHubExportError(
            "Source datasets must use Kafka transport",
            location=f"{location}/transport",
        )
    _snapshot_text(dataset.logical_name, location=f"{location}/logical_name")
    _snapshot_text(dataset.physical_name, location=f"{location}/physical_name")
    _snapshot_description(dataset.description, location=f"{location}/description")
    _snapshot_tags(dataset.tags, location=f"{location}/tags")
    _snapshot_owner(dataset.owner, location=f"{location}/owner")
    if dataset.contract is not None:
        if (
            not isinstance(dataset.contract, CatalogContractSummary)
            or not isinstance(dataset.contract.status, str)
            or dataset.contract.status not in {"declared", "enforced"}
        ):
            raise DataHubExportError(
                "Catalog contract summary is invalid",
                location=f"{location}/contract",
            )
        if dataset.logical_kind != "model":
            raise DataHubExportError(
                "Only model outputs may contain a contract summary",
                location=f"{location}/contract",
            )


def _validate_process(
    process: object,
    *,
    datasets: Mapping[LogicalDataset, CatalogDataset],
    location: str,
) -> None:
    if not isinstance(process, CatalogProcess):
        raise DataHubExportError("Catalog process has an invalid type", location=location)
    if not isinstance(process.process_kind, str) or process.process_kind not in {
        "flink",
        "gateway",
        "connect",
    }:
        raise DataHubExportError(
            "Catalog process kind is unsupported",
            location=f"{location}/process_kind",
        )
    _snapshot_text(process.logical_name, location=f"{location}/logical_name")
    _snapshot_description(process.description, location=f"{location}/description")
    _snapshot_tags(process.tags, location=f"{location}/tags")
    _snapshot_owner(process.owner, location=f"{location}/owner")
    if not isinstance(process.dependencies, tuple):
        raise DataHubExportError(
            "Catalog dependencies must be an immutable tuple",
            location=f"{location}/dependencies",
        )
    seen: set[LogicalDataset] = set()
    for position, dependency in enumerate(process.dependencies):
        dependency_location = f"{location}/dependencies/{position}"
        if not isinstance(dependency, CatalogDependency):
            raise DataHubExportError(
                "Catalog dependency has an invalid type",
                location=dependency_location,
            )
        if not isinstance(dependency.logical_kind, str) or dependency.logical_kind not in {
            "source",
            "model",
        }:
            raise DataHubExportError(
                "Catalog dependency kind is unsupported",
                location=f"{dependency_location}/logical_kind",
            )
        _snapshot_text(
            dependency.logical_name,
            location=f"{dependency_location}/logical_name",
        )
        if dependency.logical_identity in seen:
            raise DataHubExportError(
                "Catalog direct dependencies must be unique",
                location=dependency_location,
            )
        if dependency.logical_identity == ("model", process.logical_name):
            raise DataHubExportError(
                "A catalog process cannot depend on its own output",
                location=dependency_location,
            )
        if dependency.logical_identity not in datasets:
            raise DataHubExportError(
                "Catalog dependency target is absent",
                location=dependency_location,
            )
        seen.add(dependency.logical_identity)
    if process.process_kind in {"gateway", "connect"} and not process.dependencies:
        raise DataHubExportError(
            "Gateway and Connect processes require direct input metadata",
            location=f"{location}/dependencies",
        )
    if process.process_kind == "gateway" and len(process.dependencies) != 1:
        raise DataHubExportError(
            "Gateway processes require exactly one direct input",
            location=f"{location}/dependencies",
        )


def _validate_process_cycles(processes: Mapping[str, CatalogProcess]) -> None:
    states: dict[str, int] = {}

    def visit(name: str) -> None:
        state = states.get(name, 0)
        if state == 2:
            return
        if state == 1:
            raise DataHubExportError(
                "Catalog process dependencies contain a cycle",
                location="snapshot.processes",
            )
        states[name] = 1
        for dependency in processes[name].dependencies:
            if dependency.logical_kind == "model" and dependency.logical_name in processes:
                visit(dependency.logical_name)
        states[name] = 2

    for name in processes:
        visit(name)


def _snapshot_text(value: object, *, location: str) -> str:
    if (
        not isinstance(value, str)
        or not value.strip()
        or _has_unsafe_text(value, allow_layout=False)
    ):
        raise DataHubExportError("Catalog snapshot text is invalid", location=location)
    return value


def _snapshot_description(value: object, *, location: str) -> str | None:
    if value is None:
        return None
    if (
        not isinstance(value, str)
        or not value.strip()
        or _has_unsafe_text(value, allow_layout=True)
    ):
        raise DataHubExportError(
            "Catalog snapshot description is invalid",
            location=location,
        )
    return value


def _snapshot_tags(value: object, *, location: str) -> tuple[str, ...]:
    if not isinstance(value, tuple):
        raise DataHubExportError(
            "Catalog tags must be an immutable tuple",
            location=location,
        )
    seen: set[str] = set()
    for position, tag in enumerate(value):
        _snapshot_text(tag, location=f"{location}/{position}")
        if tag in seen:
            raise DataHubExportError(
                "Catalog tags must be unique",
                location=f"{location}/{position}",
            )
        seen.add(tag)
    return value


def _snapshot_owner(value: object, *, location: str) -> CatalogOwnerLabel | None:
    if value is None:
        return None
    if not isinstance(value, CatalogOwnerLabel):
        raise DataHubExportError("Catalog owner must be an owner label", location=location)
    _snapshot_text(value.label, location=location)
    return value


def _has_unsafe_text(value: str, *, allow_layout: bool) -> bool:
    allowed = {"\t", "\n", "\r"} if allow_layout else set()
    return any(
        character not in allowed and unicodedata.category(character) in {"Cc", "Cs"}
        for character in value
    )


def _topology_for_process(
    process: CatalogProcess,
    *,
    job: DataHubJobIdentity,
    datasets: Mapping[LogicalDataset, DataHubDatasetIdentity],
) -> DataHubJobTopology:
    input_urns = tuple(
        sorted(
            (datasets[dependency.logical_identity].urn for dependency in process.dependencies),
            key=lambda urn: urn.encode("utf-8"),
        )
    )
    output_urns = (
        ()
        if process.process_kind == "connect"
        else (datasets[("model", process.logical_name)].urn,)
    )
    return _foundation(
        lambda: DataHubJobTopology(
            job_urn=job.urn,
            input_dataset_urns=input_urns,
            output_dataset_urns=output_urns,
        )
    )


def _build_proposals(
    snapshot: CatalogSnapshot,
    *,
    index: _SnapshotIndex,
    flow: DataHubFlowIdentity,
    datasets: tuple[DataHubDatasetIdentity, ...],
    jobs: tuple[DataHubJobIdentity, ...],
    topology: tuple[DataHubJobTopology, ...],
) -> tuple[DataHubProposal, ...]:
    proposals = [
        _proposal(
            entity_type="dataFlow",
            entity_urn=flow.urn,
            aspect_name="dataFlowInfo",
            aspect=_flow_aspect(snapshot, flow),
        )
    ]
    snapshot_datasets = index.datasets
    for dataset_identity in sorted(datasets, key=lambda item: item.urn.encode("utf-8")):
        dataset = snapshot_datasets[dataset_identity.logical_identity]
        proposals.append(
            _proposal(
                entity_type="dataset",
                entity_urn=dataset_identity.urn,
                aspect_name="datasetProperties",
                aspect=_dataset_aspect(dataset),
            )
        )
        if dataset_identity.platform_instance_urn is not None:
            proposals.append(
                _proposal(
                    entity_type="dataset",
                    entity_urn=dataset_identity.urn,
                    aspect_name="dataPlatformInstance",
                    aspect={
                        "platform": dataset_identity.platform_urn,
                        "instance": dataset_identity.platform_instance_urn,
                    },
                )
            )

    topology_by_job = {item.job_urn: item for item in topology}
    for job_identity in sorted(jobs, key=lambda item: item.urn.encode("utf-8")):
        process = index.processes[job_identity.process_logical_name]
        proposals.append(
            _proposal(
                entity_type="dataJob",
                entity_urn=job_identity.urn,
                aspect_name="dataJobInfo",
                aspect=_job_aspect(process, flow),
            )
        )
        edges = topology_by_job[job_identity.urn]
        proposals.append(
            _proposal(
                entity_type="dataJob",
                entity_urn=job_identity.urn,
                aspect_name="dataJobInputOutput",
                aspect=_job_io_aspect(edges),
            )
        )
    return tuple(proposals)


def _proposal(
    *,
    entity_type: DataHubEntityType,
    entity_urn: str,
    aspect_name: str,
    aspect: Mapping[str, object],
) -> DataHubProposal:
    return _foundation(
        lambda: DataHubProposal.create(
            entity_type=entity_type,
            entity_urn=entity_urn,
            aspect_name=aspect_name,
            aspect=aspect,
        )
    )


def _flow_aspect(
    snapshot: CatalogSnapshot,
    flow: DataHubFlowIdentity,
) -> dict[str, object]:
    aspect: dict[str, object] = {
        "name": snapshot.project_name,
        "env": flow.fabric,
        "customProperties": {},
    }
    if snapshot.project_description is not None:
        aspect["description"] = snapshot.project_description
    return aspect


def _dataset_aspect(dataset: CatalogDataset) -> dict[str, object]:
    properties = (
        {"streamt.contract.status": dataset.contract.status} if dataset.contract is not None else {}
    )
    aspect: dict[str, object] = {
        "name": dataset.logical_name,
        "customProperties": properties,
        "tags": [],
    }
    if dataset.description is not None:
        aspect["description"] = dataset.description
    return aspect


def _job_aspect(
    process: CatalogProcess,
    flow: DataHubFlowIdentity,
) -> dict[str, object]:
    aspect: dict[str, object] = {
        "name": process.logical_name,
        "type": {"string": process.process_kind},
        "flowUrn": flow.urn,
        "env": flow.fabric,
        "customProperties": {},
    }
    if process.description is not None:
        aspect["description"] = process.description
    return aspect


def _job_io_aspect(topology: DataHubJobTopology) -> dict[str, object]:
    return {
        "inputDatasets": [],
        "outputDatasets": [],
        "inputDatasetEdges": [{"destinationUrn": urn} for urn in topology.input_dataset_urns],
        "outputDatasetEdges": [{"destinationUrn": urn} for urn in topology.output_dataset_urns],
    }


def _validate_exact_semantics(
    snapshot: CatalogSnapshot,
    *,
    index: _SnapshotIndex,
    config: DataHubIdentityConfig,
    flow: DataHubFlowIdentity,
    datasets: tuple[DataHubDatasetIdentity, ...],
    jobs: tuple[DataHubJobIdentity, ...],
    topology: tuple[DataHubJobTopology, ...],
    proposals: tuple[DataHubProposal, ...],
) -> None:
    if flow.catalog_id != config.catalog_id or flow.fabric != config.fabric:
        raise DataHubExportError(
            "Generated DataFlow does not match the exact export inputs",
            location="flow",
        )
    datasets_by_logical = {item.logical_identity: item for item in datasets}
    jobs_by_name = {item.process_logical_name: item for item in jobs}
    if set(datasets_by_logical) != set(index.datasets):
        raise DataHubExportError(
            "Generated Datasets do not match the catalog snapshot",
            location="datasets",
        )
    if set(jobs_by_name) != set(index.processes):
        raise DataHubExportError(
            "Generated DataJobs do not match the catalog snapshot",
            location="jobs",
        )
    for logical, dataset in index.datasets.items():
        dataset_identity = datasets_by_logical[logical]
        expected_contract = dataset.contract.status if dataset.contract is not None else None
        if (
            dataset_identity.logical_kind != dataset.logical_kind
            or dataset_identity.logical_name != dataset.logical_name
            or dataset_identity.transport != dataset.transport
            or dataset_identity.physical_name != dataset.physical_name
            or dataset_identity.contract_status != expected_contract
        ):
            raise DataHubExportError(
                "Generated Dataset identity does not match the catalog snapshot",
                location="datasets",
            )
    for name, process in index.processes.items():
        job_identity = jobs_by_name[name]
        if (
            job_identity.process_logical_name != process.logical_name
            or job_identity.process_kind != process.process_kind
        ):
            raise DataHubExportError(
                "Generated DataJob identity does not match the catalog snapshot",
                location="jobs",
            )

    topology_by_job = {item.job_urn: item for item in topology}
    for name, process in index.processes.items():
        job = jobs_by_name[name]
        expected_inputs = tuple(
            sorted(
                (
                    datasets_by_logical[dependency.logical_identity].urn
                    for dependency in process.dependencies
                ),
                key=lambda urn: urn.encode("utf-8"),
            )
        )
        expected_outputs = (
            () if process.process_kind == "connect" else (datasets_by_logical[("model", name)].urn,)
        )
        actual = topology_by_job.get(job.urn)
        if actual is None or (
            actual.input_dataset_urns != expected_inputs
            or actual.output_dataset_urns != expected_outputs
        ):
            raise DataHubExportError(
                "Generated lineage does not match exact direct dependencies",
                location=f"jobs/{name}/dataJobInputOutput",
            )

    by_pair = {(proposal.entity_urn, proposal.aspect_name): proposal for proposal in proposals}
    flow_proposal = by_pair[(flow.urn, "dataFlowInfo")]
    expected_flow = {
        "name": snapshot.project_name,
        "env": flow.fabric,
        "customProperties": {},
    }
    if snapshot.project_description is not None:
        expected_flow["description"] = snapshot.project_description
    if flow_proposal.aspect != expected_flow:
        raise DataHubExportError(
            "Generated DataFlow metadata does not match the catalog snapshot",
            location="flow/dataFlowInfo",
        )
    for logical, dataset in index.datasets.items():
        dataset_identity = datasets_by_logical[logical]
        if by_pair[(dataset_identity.urn, "datasetProperties")].aspect != _dataset_aspect(dataset):
            raise DataHubExportError(
                "Generated Dataset metadata does not match the catalog snapshot",
                location="datasets/datasetProperties",
            )
    for name, process in index.processes.items():
        job_identity = jobs_by_name[name]
        if by_pair[(job_identity.urn, "dataJobInfo")].aspect != _job_aspect(process, flow):
            raise DataHubExportError(
                "Generated DataJob metadata does not match the catalog snapshot",
                location="jobs/dataJobInfo",
            )


def _warnings(
    snapshot: CatalogSnapshot,
    index: _SnapshotIndex,
) -> tuple[DataHubExportWarning, ...]:
    warnings: list[DataHubExportWarning] = []
    declarations: dict[tuple[str, str], tuple[tuple[str, ...], CatalogOwnerLabel | None]] = {}
    for logical, dataset in index.datasets.items():
        declarations[logical] = (dataset.tags, dataset.owner)
    for name, process in index.processes.items():
        declarations.setdefault(("model", name), (process.tags, process.owner))
        if process.process_kind == "connect":
            warnings.append(
                _warning(
                    "W115_DATAHUB_SINK_OUTPUT_OMITTED",
                    location=f"models/{name}",
                )
            )
    for (kind, name), (tags, owner) in declarations.items():
        prefix = "sources" if kind == "source" else "models"
        if tags:
            warnings.append(
                _warning(
                    "W117_DATAHUB_TAGS_OMITTED",
                    location=f"{prefix}/{name}/tags",
                )
            )
        if owner is not None:
            warnings.append(
                _warning(
                    "W118_DATAHUB_OWNER_OMITTED",
                    location=f"{prefix}/{name}/owner",
                )
            )
    for ordinal, _name in enumerate(
        sorted(snapshot.omitted_exposure_names, key=lambda item: item.encode("utf-8"))
    ):
        warnings.append(
            _warning(
                "W116_DATAHUB_EXPOSURE_OMITTED",
                location=f"exposures/{ordinal}",
            )
        )
    return tuple(sorted(warnings, key=lambda warning: (warning.location, warning.code)))


def _warning(code: str, *, location: str) -> DataHubExportWarning:
    return DataHubExportWarning(
        code=code,
        message=_WARNING_MESSAGES[code],
        location=location,
    )


def _serialize_warning(warning: DataHubExportWarning, *, position: int) -> str:
    _require_warning(warning, location=f"export.warnings/{position}")
    try:
        return json.dumps(
            {
                "code": warning.code,
                "message": warning.message,
                "location": warning.location,
            },
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        )
    except (TypeError, ValueError, UnicodeError, RecursionError):
        raise DataHubExportError(
            "DataHub export warning cannot be serialized",
            location=f"export.warnings/{position}",
        ) from None


def _parse_warning(payload: str) -> DataHubExportWarning:
    try:
        value = json.loads(payload)
    except (json.JSONDecodeError, TypeError, UnicodeError, RecursionError):
        raise DataHubExportError(
            "DataHub export warning payload is invalid",
            location="export.warnings",
        ) from None
    if not isinstance(value, dict) or set(value) != {"code", "message", "location"}:
        raise DataHubExportError(
            "DataHub export warning payload is invalid",
            location="export.warnings",
        )
    warning = DataHubExportWarning(
        code=value["code"],
        message=value["message"],
        location=value["location"],
    )
    _require_warning(warning, location="export.warnings")
    return warning


def _require_warning(warning: object, *, location: str) -> None:
    if type(warning) is not DataHubExportWarning or (
        not isinstance(warning.code, str)
        or not isinstance(warning.message, str)
        or not isinstance(warning.location, str)
        or _WARNING_MESSAGES.get(warning.code) != warning.message
        or not warning.location.strip()
        or _has_unsafe_text(warning.location, allow_layout=False)
    ):
        raise DataHubExportError(
            "DataHub export warning is invalid",
            location=location,
        )


def _serialize_proposals(proposals: Sequence[DataHubProposal]) -> str:
    try:
        return (
            json.dumps(
                [proposal.to_dict() for proposal in proposals],
                ensure_ascii=False,
                allow_nan=False,
                indent=2,
                sort_keys=True,
            )
            + "\n"
        )
    except (TypeError, ValueError, UnicodeError, RecursionError):
        raise DataHubExportError(
            "DataHub proposals cannot be serialized",
            location="export.proposals",
        ) from None
