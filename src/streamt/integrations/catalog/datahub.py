"""Dependency-free DataHub v1.7 identity and closed proposal validation."""

from __future__ import annotations

import json
import unicodedata
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Literal, cast

DataHubFabric = Literal[
    "DEV",
    "TEST",
    "QA",
    "UAT",
    "EI",
    "PRE",
    "STG",
    "NON_PROD",
    "PROD",
    "CORP",
    "RVW",
    "PRD",
    "TST",
    "SIT",
    "SBX",
    "SANDBOX",
    "CERT",
]
DataHubEntityType = Literal["dataFlow", "dataset", "dataJob"]
DataHubDatasetTransport = Literal["kafka", "gateway"]
DataHubLogicalKind = Literal["source", "model"]
DataHubProcessKind = Literal["flink", "gateway", "connect"]
DataHubContractStatus = Literal["declared", "enforced"]

DATAHUB_FABRICS = frozenset(
    {
        "DEV",
        "TEST",
        "QA",
        "UAT",
        "EI",
        "PRE",
        "STG",
        "NON_PROD",
        "PROD",
        "CORP",
        "RVW",
        "PRD",
        "TST",
        "SIT",
        "SBX",
        "SANDBOX",
        "CERT",
    }
)

_ENCODED_CHARACTERS = {
    ",": "%2C",
    "(": "%28",
    ")": "%29",
    "␟": "%E2%90%9F",
}
_RAW_RESERVED = frozenset(_ENCODED_CHARACTERS)
_DATA_PLATFORM_PREFIX = "urn:li:dataPlatform:"
_DATA_PLATFORM_INSTANCE_PREFIX = "urn:li:dataPlatformInstance:"
_DATASET_PREFIX = "urn:li:dataset:"
_DATA_FLOW_PREFIX = "urn:li:dataFlow:"
_DATA_JOB_PREFIX = "urn:li:dataJob:"


class DataHubValidationError(ValueError):
    """A bounded DataHub validation failure with one safe location."""

    def __init__(self, message: str, *, location: str) -> None:
        super().__init__(message)
        self.location = location


def encode_datahub_urn_component(value: object) -> str:
    """Apply v1.7 ``UrnEncoder`` behavior to one string, with bounded failures."""
    if not isinstance(value, str):
        raise DataHubValidationError(
            "DataHub URN components must be strings",
            location="urn_component",
        )
    return "".join(_ENCODED_CHARACTERS.get(character, character) for character in value)


def _utf16_units(value: str) -> int:
    return len(value.encode("utf-16-le")) // 2


def _has_control_or_surrogate(value: str) -> bool:
    return any(unicodedata.category(character) in {"Cc", "Cs"} for character in value)


def _require_raw_identifier(
    value: object,
    *,
    location: str,
    label: str,
    encoded_limit: int | None = None,
    bare: bool = False,
) -> str:
    if (
        not isinstance(value, str)
        or not value
        or not value.strip()
        or _has_control_or_surrogate(value)
    ):
        raise DataHubValidationError(
            f"{label} must be a non-blank control-free string",
            location=location,
        )
    if bare and value.startswith("urn:"):
        raise DataHubValidationError(
            f"{label} must be a bare identifier, not a URN",
            location=location,
        )
    if (
        encoded_limit is not None
        and _utf16_units(encode_datahub_urn_component(value)) > encoded_limit
    ):
        raise DataHubValidationError(
            f"{label} exceeds the DataHub v1.7 length limit",
            location=location,
        )
    return value


def _require_encoded_component(
    value: str,
    *,
    location: str,
    label: str,
    limit: int,
) -> None:
    if (
        not value
        or _has_control_or_surrogate(value)
        or any(character in _RAW_RESERVED for character in value)
    ):
        raise DataHubValidationError(
            f"{label} is not a valid encoded DataHub URN component",
            location=location,
        )
    if _utf16_units(value) > limit:
        raise DataHubValidationError(
            f"{label} exceeds the DataHub v1.7 length limit",
            location=location,
        )


def _require_full_length(value: str, *, maximum: int, location: str, label: str) -> None:
    if _utf16_units(value) > maximum:
        raise DataHubValidationError(
            f"{label} exceeds the DataHub v1.7 complete-URN limit",
            location=location,
        )


def require_datahub_fabric(value: object, *, location: str = "fabric") -> DataHubFabric:
    """Require one exact uppercase v1.7 ``FabricType`` member."""
    if not isinstance(value, str) or value not in DATAHUB_FABRICS:
        raise DataHubValidationError(
            "Fabric must be an exact uppercase DataHub v1.7 FabricType",
            location=location,
        )
    return cast(DataHubFabric, value)


def make_datahub_platform_urn(
    platform_id: object,
    *,
    location: str = "platform_id",
) -> str:
    """Construct one exact typed DataPlatform URN from a bare identifier."""
    raw = _require_raw_identifier(
        platform_id,
        location=location,
        label="Platform ID",
        encoded_limit=25,
        bare=True,
    )
    urn = f"{_DATA_PLATFORM_PREFIX}{encode_datahub_urn_component(raw)}"
    _require_full_length(urn, maximum=45, location=location, label="DataPlatform URN")
    return urn


def make_datahub_platform_instance_urn(
    platform_id: object,
    instance_id: object,
    *,
    location: str = "platform_instance",
) -> str:
    """Construct one exact typed DataPlatformInstance URN."""
    platform_urn = make_datahub_platform_urn(platform_id, location=f"{location}/platform")
    raw_instance = _require_raw_identifier(
        instance_id,
        location=f"{location}/instance",
        label="Platform instance",
        bare=True,
    )
    urn = (
        f"{_DATA_PLATFORM_INSTANCE_PREFIX}"
        f"({platform_urn},{encode_datahub_urn_component(raw_instance)})"
    )
    _require_full_length(
        urn,
        maximum=100,
        location=location,
        label="DataPlatformInstance URN",
    )
    return urn


def make_datahub_dataset_urn(
    platform_id: object,
    physical_name: object,
    fabric: object,
    *,
    platform_instance: object | None = None,
    location: str = "dataset",
) -> str:
    """Mirror v1.7 ``make_dataset_urn_with_platform_instance`` exactly."""
    platform_urn = make_datahub_platform_urn(
        platform_id,
        location=f"{location}/platform",
    )
    raw_physical = _require_raw_identifier(
        physical_name,
        location=f"{location}/physical_name",
        label="Dataset physical name",
    )
    raw_name = raw_physical
    if platform_instance is not None:
        raw_instance = _require_raw_identifier(
            platform_instance,
            location=f"{location}/platform_instance",
            label="Platform instance",
            bare=True,
        )
        make_datahub_platform_instance_urn(
            platform_id,
            raw_instance,
            location=f"{location}/platform_instance",
        )
        raw_name = f"{raw_instance}.{raw_physical}"
    encoded_name = encode_datahub_urn_component(raw_name)
    _require_encoded_component(
        encoded_name,
        location=f"{location}/physical_name",
        label="Composed Dataset name",
        limit=210,
    )
    exact_fabric = require_datahub_fabric(fabric, location=f"{location}/fabric")
    urn = f"{_DATASET_PREFIX}({platform_urn},{encoded_name},{exact_fabric})"
    _require_full_length(urn, maximum=284, location=location, label="Dataset URN")
    return urn


def make_datahub_flow_urn(
    catalog_id: object,
    fabric: object,
    *,
    location: str = "data_flow",
) -> str:
    """Construct the single streamt DataFlow identity."""
    raw_catalog_id = _require_raw_identifier(
        catalog_id,
        location=f"{location}/catalog_id",
        label="Catalog ID",
        encoded_limit=200,
    )
    exact_fabric = require_datahub_fabric(fabric, location=f"{location}/fabric")
    urn = (
        f"{_DATA_FLOW_PREFIX}"
        f"(streamt,{encode_datahub_urn_component(raw_catalog_id)},{exact_fabric})"
    )
    _require_full_length(urn, maximum=373, location=location, label="DataFlow URN")
    return urn


def make_datahub_job_urn(
    flow_urn: object,
    process_logical_name: object,
    *,
    location: str = "data_job",
) -> str:
    """Construct one DataJob nested beneath an exact streamt DataFlow."""
    if not isinstance(flow_urn, str):
        raise DataHubValidationError(
            "DataJob flow must be a DataFlow URN",
            location=f"{location}/flow",
        )
    _parse_flow_urn(flow_urn, location=f"{location}/flow")
    raw_job_id = _require_raw_identifier(
        process_logical_name,
        location=f"{location}/process_logical_name",
        label="Process logical name",
        encoded_limit=200,
    )
    urn = f"{_DATA_JOB_PREFIX}({flow_urn},{encode_datahub_urn_component(raw_job_id)})"
    _require_full_length(urn, maximum=594, location=location, label="DataJob URN")
    return urn


@dataclass(frozen=True, repr=False)
class DataHubIdentityConfig:
    """Exact caller-owned inputs used by the identity renderer."""

    catalog_id: str
    fabric: DataHubFabric
    kafka_platform_instance: str | None = None
    gateway_platform_id: str | None = None
    gateway_platform_instance: str | None = None

    def __post_init__(self) -> None:
        make_datahub_flow_urn(self.catalog_id, self.fabric)
        if self.kafka_platform_instance is not None:
            make_datahub_platform_instance_urn(
                "kafka",
                self.kafka_platform_instance,
                location="kafka_platform_instance",
            )
        gateway_pair = (self.gateway_platform_id, self.gateway_platform_instance)
        if (gateway_pair[0] is None) != (gateway_pair[1] is None):
            raise DataHubValidationError(
                "Gateway platform ID and instance must be supplied together",
                location="gateway_platform",
            )
        if gateway_pair[0] is not None and gateway_pair[1] is not None:
            make_datahub_platform_instance_urn(
                gateway_pair[0],
                gateway_pair[1],
                location="gateway_platform",
            )

    @property
    def flow_urn(self) -> str:
        """Return the configured streamt DataFlow URN."""
        return make_datahub_flow_urn(self.catalog_id, self.fabric)

    def __repr__(self) -> str:
        return "DataHubIdentityConfig(<redacted>)"


@dataclass(frozen=True, repr=False)
class DataHubFlowIdentity:
    """One immutable DataFlow identity claim."""

    catalog_id: str
    fabric: DataHubFabric
    urn: str

    def __post_init__(self) -> None:
        expected = make_datahub_flow_urn(self.catalog_id, self.fabric)
        if self.urn != expected:
            raise DataHubValidationError(
                "DataFlow identity does not match its exact inputs",
                location="flow.urn",
            )

    @classmethod
    def from_config(cls, config: DataHubIdentityConfig) -> DataHubFlowIdentity:
        """Build the sole flow claim from validated configuration."""
        if not isinstance(config, DataHubIdentityConfig):
            raise DataHubValidationError(
                "DataHub identity configuration is invalid",
                location="config",
            )
        return cls(catalog_id=config.catalog_id, fabric=config.fabric, urn=config.flow_urn)

    def __repr__(self) -> str:
        return "DataHubFlowIdentity(<redacted>)"


@dataclass(frozen=True, repr=False)
class DataHubDatasetIdentity:
    """One typed logical Dataset claim and its exact native DataHub identity."""

    logical_kind: DataHubLogicalKind
    logical_name: str
    transport: DataHubDatasetTransport
    physical_name: str
    platform_id: str
    platform_instance: str | None
    fabric: DataHubFabric
    contract_status: DataHubContractStatus | None
    urn: str
    platform_urn: str
    platform_instance_urn: str | None

    def __post_init__(self) -> None:
        if not isinstance(self.logical_kind, str) or self.logical_kind not in {
            "source",
            "model",
        }:
            raise DataHubValidationError(
                "Dataset logical kind is unsupported",
                location="dataset.logical_kind",
            )
        _require_raw_identifier(
            self.logical_name,
            location="dataset.logical_name",
            label="Dataset logical name",
        )
        if not isinstance(self.transport, str) or self.transport not in {
            "kafka",
            "gateway",
        }:
            raise DataHubValidationError(
                "Dataset transport is unsupported",
                location="dataset.transport",
            )
        if self.logical_kind == "source" and self.transport != "kafka":
            raise DataHubValidationError(
                "Source Dataset transport must be kafka",
                location="dataset.transport",
            )
        if self.transport == "kafka" and self.platform_id != "kafka":
            raise DataHubValidationError(
                "Kafka Datasets must use the fixed kafka platform",
                location="dataset.platform_id",
            )
        if self.transport == "gateway" and self.platform_instance is None:
            raise DataHubValidationError(
                "Gateway Datasets require an explicit platform instance",
                location="dataset.platform_instance",
            )
        if self.contract_status is not None and (
            not isinstance(self.contract_status, str)
            or self.contract_status not in {"declared", "enforced"}
        ):
            raise DataHubValidationError(
                "Dataset contract status is unsupported",
                location="dataset.contract_status",
            )
        if self.logical_kind == "source" and self.contract_status is not None:
            raise DataHubValidationError(
                "Source Datasets cannot carry a contract status",
                location="dataset.contract_status",
            )
        expected_platform = make_datahub_platform_urn(self.platform_id)
        expected_instance = (
            make_datahub_platform_instance_urn(self.platform_id, self.platform_instance)
            if self.platform_instance is not None
            else None
        )
        expected_urn = make_datahub_dataset_urn(
            self.platform_id,
            self.physical_name,
            self.fabric,
            platform_instance=self.platform_instance,
        )
        if (
            self.platform_urn != expected_platform
            or self.platform_instance_urn != expected_instance
            or self.urn != expected_urn
        ):
            raise DataHubValidationError(
                "Dataset identity does not match its exact inputs",
                location="dataset.urn",
            )

    @classmethod
    def from_config(
        cls,
        config: DataHubIdentityConfig,
        *,
        logical_kind: DataHubLogicalKind,
        logical_name: str,
        transport: DataHubDatasetTransport,
        physical_name: str,
        contract_status: DataHubContractStatus | None = None,
    ) -> DataHubDatasetIdentity:
        """Resolve one Dataset identity without consulting a provider."""
        if not isinstance(config, DataHubIdentityConfig):
            raise DataHubValidationError(
                "DataHub identity configuration is invalid",
                location="config",
            )
        if transport == "kafka":
            platform_id = "kafka"
            instance = config.kafka_platform_instance
        elif transport == "gateway":
            if config.gateway_platform_id is None or config.gateway_platform_instance is None:
                raise DataHubValidationError(
                    "Gateway Datasets require an explicit platform and instance",
                    location="gateway_platform",
                )
            platform_id = config.gateway_platform_id
            instance = config.gateway_platform_instance
        else:
            raise DataHubValidationError(
                "Dataset transport is unsupported",
                location="dataset.transport",
            )
        platform_urn = make_datahub_platform_urn(platform_id)
        instance_urn = (
            make_datahub_platform_instance_urn(platform_id, instance)
            if instance is not None
            else None
        )
        urn = make_datahub_dataset_urn(
            platform_id,
            physical_name,
            config.fabric,
            platform_instance=instance,
        )
        return cls(
            logical_kind=logical_kind,
            logical_name=logical_name,
            transport=transport,
            physical_name=physical_name,
            platform_id=platform_id,
            platform_instance=instance,
            fabric=config.fabric,
            contract_status=contract_status,
            urn=urn,
            platform_urn=platform_urn,
            platform_instance_urn=instance_urn,
        )

    @property
    def logical_identity(self) -> tuple[DataHubLogicalKind, str]:
        """Return the typed streamt logical identity."""
        return (self.logical_kind, self.logical_name)

    def __repr__(self) -> str:
        return "DataHubDatasetIdentity(<redacted>)"


@dataclass(frozen=True, repr=False)
class DataHubJobIdentity:
    """One compiled process and its exact DataJob identity."""

    process_logical_name: str
    process_kind: DataHubProcessKind
    fabric: DataHubFabric
    flow_urn: str
    urn: str

    def __post_init__(self) -> None:
        if not isinstance(self.process_kind, str) or self.process_kind not in {
            "flink",
            "gateway",
            "connect",
        }:
            raise DataHubValidationError(
                "DataJob process kind is unsupported",
                location="job.process_kind",
            )
        parsed_flow = _parse_flow_urn(self.flow_urn, location="job.flow_urn")
        if parsed_flow[2] != self.fabric:
            raise DataHubValidationError(
                "DataJob fabric does not match its DataFlow",
                location="job.fabric",
            )
        expected = make_datahub_job_urn(self.flow_urn, self.process_logical_name)
        if self.urn != expected:
            raise DataHubValidationError(
                "DataJob identity does not match its exact inputs",
                location="job.urn",
            )

    @classmethod
    def from_config(
        cls,
        config: DataHubIdentityConfig,
        *,
        process_logical_name: str,
        process_kind: DataHubProcessKind,
    ) -> DataHubJobIdentity:
        """Resolve one process identity beneath the configured flow."""
        if not isinstance(config, DataHubIdentityConfig):
            raise DataHubValidationError(
                "DataHub identity configuration is invalid",
                location="config",
            )
        flow_urn = config.flow_urn
        return cls(
            process_logical_name=process_logical_name,
            process_kind=process_kind,
            fabric=config.fabric,
            flow_urn=flow_urn,
            urn=make_datahub_job_urn(flow_urn, process_logical_name),
        )

    def __repr__(self) -> str:
        return "DataHubJobIdentity(<redacted>)"


@dataclass(frozen=True, repr=False)
class DataHubJobTopology:
    """The exact direct Dataset edges expected for one DataJob."""

    job_urn: str
    input_dataset_urns: tuple[str, ...]
    output_dataset_urns: tuple[str, ...]

    def __post_init__(self) -> None:
        _parse_job_urn(self.job_urn, location="topology.job_urn")
        _require_urn_tuple(
            self.input_dataset_urns,
            location="topology.input_dataset_urns",
        )
        _require_urn_tuple(
            self.output_dataset_urns,
            location="topology.output_dataset_urns",
        )
        if len(self.output_dataset_urns) > 1:
            raise DataHubValidationError(
                "A DataJob may have at most one output Dataset",
                location="topology.output_dataset_urns",
            )
        if set(self.input_dataset_urns) & set(self.output_dataset_urns):
            raise DataHubValidationError(
                "A DataJob output cannot also be a direct input",
                location="topology.input_dataset_urns",
            )

    def __repr__(self) -> str:
        return "DataHubJobTopology(<redacted>)"


def _require_urn_tuple(value: object, *, location: str) -> None:
    if not isinstance(value, tuple) or not all(isinstance(item, str) for item in value):
        raise DataHubValidationError(
            "Dataset URNs must be an immutable tuple of strings",
            location=location,
        )
    for index, urn in enumerate(value):
        _parse_dataset_urn(urn, location=f"{location}/{index}")
    if len(value) != len(set(value)) or value != tuple(
        sorted(value, key=lambda item: item.encode("utf-8"))
    ):
        raise DataHubValidationError(
            "Dataset URNs must be unique and sorted by UTF-8 bytes",
            location=location,
        )


@dataclass(frozen=True, repr=False)
class DataHubProposal:
    """One immutable simplified Metadata Change Proposal."""

    entity_type: DataHubEntityType
    entity_urn: str
    aspect_name: str
    _aspect_payload: str

    def __post_init__(self) -> None:
        if not isinstance(self.entity_type, str) or self.entity_type not in {
            "dataFlow",
            "dataset",
            "dataJob",
        }:
            raise DataHubValidationError(
                "Proposal entity type is unsupported",
                location="proposal.entityType",
            )
        if not isinstance(self.entity_urn, str) or not isinstance(self.aspect_name, str):
            raise DataHubValidationError(
                "Proposal identity fields must be strings",
                location="proposal",
            )
        if not isinstance(self._aspect_payload, str):
            raise DataHubValidationError(
                "Proposal aspect payload must be canonical JSON text",
                location="proposal.aspect.json",
            )
        try:
            aspect = json.loads(self._aspect_payload)
        except (json.JSONDecodeError, TypeError, RecursionError):
            raise DataHubValidationError(
                "Proposal aspect payload is invalid",
                location="proposal.aspect.json",
            ) from None
        if not isinstance(aspect, dict) or _canonical_json(aspect) != self._aspect_payload:
            raise DataHubValidationError(
                "Proposal aspect payload is not canonical",
                location="proposal.aspect.json",
            )

    @classmethod
    def create(
        cls,
        *,
        entity_type: DataHubEntityType,
        entity_urn: str,
        aspect_name: str,
        aspect: Mapping[str, object],
    ) -> DataHubProposal:
        """Create a proposal while defensively freezing its aspect object."""
        if not isinstance(aspect, Mapping):
            raise DataHubValidationError(
                "Proposal aspect must be an object",
                location="proposal.aspect.json",
            )
        return cls(
            entity_type=entity_type,
            entity_urn=entity_urn,
            aspect_name=aspect_name,
            _aspect_payload=_canonical_json(dict(aspect)),
        )

    @classmethod
    def from_dict(cls, value: object) -> DataHubProposal:
        """Strictly parse one simplified MCP object."""
        if not isinstance(value, Mapping) or set(value) != {
            "entityType",
            "entityUrn",
            "changeType",
            "aspectName",
            "aspect",
        }:
            raise DataHubValidationError(
                "Proposal must contain exactly the simplified MCP fields",
                location="proposal",
            )
        if value["changeType"] != "UPSERT":
            raise DataHubValidationError(
                "Proposal change type must be UPSERT",
                location="proposal.changeType",
            )
        aspect_wrapper = value["aspect"]
        if not isinstance(aspect_wrapper, Mapping) or set(aspect_wrapper) != {"json"}:
            raise DataHubValidationError(
                "Proposal aspect must contain exactly json",
                location="proposal.aspect",
            )
        entity_type = value["entityType"]
        entity_urn = value["entityUrn"]
        aspect_name = value["aspectName"]
        if (
            not isinstance(entity_type, str)
            or entity_type not in {"dataFlow", "dataset", "dataJob"}
            or not isinstance(entity_urn, str)
            or not isinstance(aspect_name, str)
        ):
            raise DataHubValidationError(
                "Proposal identity fields are invalid",
                location="proposal",
            )
        aspect = aspect_wrapper["json"]
        if not isinstance(aspect, Mapping):
            raise DataHubValidationError(
                "Proposal aspect json must be an object",
                location="proposal.aspect.json",
            )
        return cls.create(
            entity_type=cast(DataHubEntityType, entity_type),
            entity_urn=entity_urn,
            aspect_name=aspect_name,
            aspect=cast(Mapping[str, object], aspect),
        )

    @property
    def aspect(self) -> dict[str, object]:
        """Return a defensive aspect copy."""
        return cast(dict[str, object], json.loads(self._aspect_payload))

    def to_dict(self) -> dict[str, object]:
        """Return the exact simplified MCP object as a defensive copy."""
        return {
            "entityType": self.entity_type,
            "entityUrn": self.entity_urn,
            "changeType": "UPSERT",
            "aspectName": self.aspect_name,
            "aspect": {"json": self.aspect},
        }

    def __repr__(self) -> str:
        return "DataHubProposal(<redacted>)"


def _canonical_json(value: object) -> str:
    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        )
    except (TypeError, ValueError, RecursionError, UnicodeEncodeError):
        raise DataHubValidationError(
            "Proposal aspect contains unsupported values",
            location="proposal.aspect.json",
        ) from None


def _revalidation_error(location: str) -> DataHubValidationError:
    return DataHubValidationError(
        "DataHub validation input is malformed",
        location=location,
    )


def _revalidate_config(config: object) -> DataHubIdentityConfig:
    if type(config) is not DataHubIdentityConfig:
        raise _revalidation_error("config")
    try:
        return DataHubIdentityConfig(
            catalog_id=config.catalog_id,
            fabric=config.fabric,
            kafka_platform_instance=config.kafka_platform_instance,
            gateway_platform_id=config.gateway_platform_id,
            gateway_platform_instance=config.gateway_platform_instance,
        )
    except DataHubValidationError:
        raise
    except (AttributeError, TypeError, ValueError, RecursionError):
        raise _revalidation_error("config") from None


def _revalidate_flow(
    flow: object,
    config: DataHubIdentityConfig,
) -> DataHubFlowIdentity:
    if type(flow) is not DataHubFlowIdentity:
        raise _revalidation_error("flow")
    try:
        canonical = DataHubFlowIdentity(
            catalog_id=flow.catalog_id,
            fabric=flow.fabric,
            urn=flow.urn,
        )
        if canonical != DataHubFlowIdentity.from_config(config):
            raise DataHubValidationError(
                "DataFlow identity does not match the export configuration",
                location="flow",
            )
        return canonical
    except DataHubValidationError:
        raise
    except (AttributeError, TypeError, ValueError, RecursionError):
        raise _revalidation_error("flow") from None


def _revalidate_dataset(
    dataset: object,
    config: DataHubIdentityConfig,
    *,
    location: str,
) -> DataHubDatasetIdentity:
    if type(dataset) is not DataHubDatasetIdentity:
        raise _revalidation_error(location)
    try:
        canonical = DataHubDatasetIdentity(
            logical_kind=dataset.logical_kind,
            logical_name=dataset.logical_name,
            transport=dataset.transport,
            physical_name=dataset.physical_name,
            platform_id=dataset.platform_id,
            platform_instance=dataset.platform_instance,
            fabric=dataset.fabric,
            contract_status=dataset.contract_status,
            urn=dataset.urn,
            platform_urn=dataset.platform_urn,
            platform_instance_urn=dataset.platform_instance_urn,
        )
        expected = DataHubDatasetIdentity.from_config(
            config,
            logical_kind=canonical.logical_kind,
            logical_name=canonical.logical_name,
            transport=canonical.transport,
            physical_name=canonical.physical_name,
            contract_status=canonical.contract_status,
        )
        if canonical != expected:
            raise DataHubValidationError(
                "Dataset identity does not match its transport configuration",
                location=location,
            )
        return canonical
    except DataHubValidationError:
        raise
    except (AttributeError, TypeError, ValueError, RecursionError):
        raise _revalidation_error(location) from None


def _revalidate_job(
    job: object,
    config: DataHubIdentityConfig,
    *,
    location: str,
) -> DataHubJobIdentity:
    if type(job) is not DataHubJobIdentity:
        raise _revalidation_error(location)
    try:
        canonical = DataHubJobIdentity(
            process_logical_name=job.process_logical_name,
            process_kind=job.process_kind,
            fabric=job.fabric,
            flow_urn=job.flow_urn,
            urn=job.urn,
        )
        expected = DataHubJobIdentity.from_config(
            config,
            process_logical_name=canonical.process_logical_name,
            process_kind=canonical.process_kind,
        )
        if canonical != expected:
            raise DataHubValidationError(
                "DataJob identity does not match the export configuration",
                location=location,
            )
        return canonical
    except DataHubValidationError:
        raise
    except (AttributeError, TypeError, ValueError, RecursionError):
        raise _revalidation_error(location) from None


def _revalidate_topology(
    topology: object,
    *,
    location: str,
) -> DataHubJobTopology:
    if type(topology) is not DataHubJobTopology:
        raise _revalidation_error(location)
    try:
        return DataHubJobTopology(
            job_urn=topology.job_urn,
            input_dataset_urns=topology.input_dataset_urns,
            output_dataset_urns=topology.output_dataset_urns,
        )
    except DataHubValidationError:
        raise
    except (AttributeError, TypeError, ValueError, UnicodeError, RecursionError):
        raise _revalidation_error(location) from None


def _revalidate_proposal(
    proposal: object,
    *,
    location: str,
) -> DataHubProposal:
    if type(proposal) is not DataHubProposal:
        raise _revalidation_error(location)
    try:
        return DataHubProposal(
            entity_type=proposal.entity_type,
            entity_urn=proposal.entity_urn,
            aspect_name=proposal.aspect_name,
            _aspect_payload=proposal._aspect_payload,
        )
    except DataHubValidationError:
        raise
    except (AttributeError, TypeError, ValueError, UnicodeError, RecursionError):
        raise _revalidation_error(location) from None


def validate_datahub_identity_claims(
    datasets: tuple[DataHubDatasetIdentity, ...],
    jobs: tuple[DataHubJobIdentity, ...],
    topology: tuple[DataHubJobTopology, ...],
    *,
    config: DataHubIdentityConfig,
    flow: DataHubFlowIdentity,
) -> None:
    """Validate already-mapped identity claims and topology, not a snapshot."""
    _validated_identity_claims(
        config=config,
        flow=flow,
        datasets=datasets,
        jobs=jobs,
        topology=topology,
    )


def _validated_identity_claims(
    *,
    config: DataHubIdentityConfig,
    flow: DataHubFlowIdentity,
    datasets: tuple[DataHubDatasetIdentity, ...],
    jobs: tuple[DataHubJobIdentity, ...],
    topology: tuple[DataHubJobTopology, ...],
) -> tuple[
    DataHubIdentityConfig,
    DataHubFlowIdentity,
    tuple[DataHubDatasetIdentity, ...],
    tuple[DataHubJobIdentity, ...],
    tuple[DataHubJobTopology, ...],
]:
    """Reconstruct all records before validating mapped identities."""
    canonical_config = _revalidate_config(config)
    canonical_flow = _revalidate_flow(flow, canonical_config)
    _require_typed_tuple(datasets, DataHubDatasetIdentity, location="datasets")
    _require_typed_tuple(jobs, DataHubJobIdentity, location="jobs")
    _require_typed_tuple(topology, DataHubJobTopology, location="topology")
    canonical_datasets = tuple(
        _revalidate_dataset(dataset, canonical_config, location=f"datasets/{index}")
        for index, dataset in enumerate(datasets)
    )
    canonical_jobs = tuple(
        _revalidate_job(job, canonical_config, location=f"jobs/{index}")
        for index, job in enumerate(jobs)
    )
    canonical_topology = tuple(
        _revalidate_topology(edges, location=f"topology/{index}")
        for index, edges in enumerate(topology)
    )

    dataset_by_logical: dict[tuple[DataHubLogicalKind, str], DataHubDatasetIdentity] = {}
    dataset_by_urn: dict[str, DataHubDatasetIdentity] = {}
    platform_claims: dict[str, str] = {}
    platform_instance_claims: dict[str, tuple[str, str]] = {}
    transport_claims: dict[str, tuple[str, str | None]] = {}
    for index, dataset in enumerate(canonical_datasets):
        if dataset.logical_identity in dataset_by_logical:
            raise DataHubValidationError(
                "Two Datasets claim one typed logical identity",
                location=f"datasets/{index}",
            )
        if dataset.urn in dataset_by_urn:
            raise DataHubValidationError(
                "Two Dataset declarations resolve to one DataHub URN",
                location=f"datasets/{index}",
            )
        transport_claim = (dataset.platform_id, dataset.platform_instance)
        if (
            dataset.transport in transport_claims
            and transport_claims[dataset.transport] != transport_claim
        ):
            raise DataHubValidationError(
                "One transport has conflicting platform configuration",
                location=f"datasets/{index}",
            )
        transport_claims[dataset.transport] = transport_claim
        if (
            dataset.platform_urn in platform_claims
            and platform_claims[dataset.platform_urn] != dataset.platform_id
        ):
            raise DataHubValidationError(
                "Two raw platform IDs resolve to one DataPlatform URN",
                location=f"datasets/{index}",
            )
        platform_claims[dataset.platform_urn] = dataset.platform_id
        if dataset.platform_instance_urn is not None and dataset.platform_instance is not None:
            raw_instance_claim = (dataset.platform_id, dataset.platform_instance)
            if (
                dataset.platform_instance_urn in platform_instance_claims
                and platform_instance_claims[dataset.platform_instance_urn] != raw_instance_claim
            ):
                raise DataHubValidationError(
                    "Two raw platform-instance pairs resolve to one typed URN",
                    location=f"datasets/{index}",
                )
            platform_instance_claims[dataset.platform_instance_urn] = raw_instance_claim
        dataset_by_logical[dataset.logical_identity] = dataset
        dataset_by_urn[dataset.urn] = dataset

    job_by_name: dict[str, DataHubJobIdentity] = {}
    job_by_urn: dict[str, DataHubJobIdentity] = {}
    for index, job in enumerate(canonical_jobs):
        if job.process_logical_name in job_by_name:
            raise DataHubValidationError(
                "Two DataJobs claim one process logical identity",
                location=f"jobs/{index}",
            )
        if job.urn in job_by_urn:
            raise DataHubValidationError(
                "Two process declarations resolve to one DataJob URN",
                location=f"jobs/{index}",
            )
        job_by_name[job.process_logical_name] = job
        job_by_urn[job.urn] = job

    topology_by_job: dict[str, DataHubJobTopology] = {}
    output_owners: dict[str, str] = {}
    for index, edges in enumerate(canonical_topology):
        if edges.job_urn in topology_by_job:
            raise DataHubValidationError(
                "A DataJob has more than one topology record",
                location=f"topology/{index}",
            )
        current_job = job_by_urn.get(edges.job_urn)
        if current_job is None:
            raise DataHubValidationError(
                "Topology targets a DataJob that is not emitted",
                location=f"topology/{index}/job_urn",
            )
        for edge_kind, urns in (
            ("input", edges.input_dataset_urns),
            ("output", edges.output_dataset_urns),
        ):
            if any(urn not in dataset_by_urn for urn in urns):
                raise DataHubValidationError(
                    f"DataJob {edge_kind} targets a Dataset that is not emitted",
                    location=f"topology/{index}/{edge_kind}",
                )
        expected_output_count = 0 if current_job.process_kind == "connect" else 1
        if len(edges.output_dataset_urns) != expected_output_count:
            raise DataHubValidationError(
                "DataJob output cardinality does not match its process kind",
                location=f"topology/{index}/output",
            )
        if edges.output_dataset_urns:
            output_urn = edges.output_dataset_urns[0]
            output = dataset_by_urn[output_urn]
            expected_transport = "gateway" if current_job.process_kind == "gateway" else "kafka"
            if (
                output.logical_kind != "model"
                or output.logical_name != current_job.process_logical_name
                or output.transport != expected_transport
            ):
                raise DataHubValidationError(
                    "DataJob output is not its same-name model Dataset",
                    location=f"topology/{index}/output",
                )
            if output_urn in output_owners:
                raise DataHubValidationError(
                    "A Dataset is produced by more than one DataJob",
                    location=f"topology/{index}/output",
                )
            output_owners[output_urn] = current_job.urn
        topology_by_job[edges.job_urn] = edges
    if set(topology_by_job) != set(job_by_urn):
        raise DataHubValidationError(
            "Every DataJob must have exactly one topology record",
            location="topology",
        )
    return (
        canonical_config,
        canonical_flow,
        canonical_datasets,
        canonical_jobs,
        canonical_topology,
    )


def _require_typed_tuple(value: object, item_type: type[object], *, location: str) -> None:
    if not isinstance(value, tuple) or not all(type(item) is item_type for item in value):
        raise DataHubValidationError(
            "DataHub validation inputs must be immutable typed tuples",
            location=location,
        )


def validate_datahub_proposals(
    proposals: tuple[DataHubProposal, ...],
    *,
    config: DataHubIdentityConfig,
    flow: DataHubFlowIdentity,
    datasets: tuple[DataHubDatasetIdentity, ...],
    jobs: tuple[DataHubJobIdentity, ...],
    topology: tuple[DataHubJobTopology, ...],
) -> None:
    """Validate mapped proposals; snapshot-to-mapping exactness belongs upstream."""
    _require_typed_tuple(proposals, DataHubProposal, location="proposals")
    canonical_proposals = tuple(
        _revalidate_proposal(proposal, location=f"proposals/{index}")
        for index, proposal in enumerate(proposals)
    )
    (
        canonical_config,
        canonical_flow,
        canonical_datasets,
        canonical_jobs,
        canonical_topology,
    ) = _validated_identity_claims(
        config=config,
        flow=flow,
        datasets=datasets,
        jobs=jobs,
        topology=topology,
    )

    expected: list[tuple[str, str]] = [(canonical_flow.urn, "dataFlowInfo")]
    for dataset in sorted(
        canonical_datasets,
        key=lambda item: item.urn.encode("utf-8"),
    ):
        expected.append((dataset.urn, "datasetProperties"))
        if dataset.platform_instance is not None:
            expected.append((dataset.urn, "dataPlatformInstance"))
    for job in sorted(canonical_jobs, key=lambda item: item.urn.encode("utf-8")):
        expected.extend(((job.urn, "dataJobInfo"), (job.urn, "dataJobInputOutput")))
    actual = [(proposal.entity_urn, proposal.aspect_name) for proposal in canonical_proposals]
    if actual != expected or len(actual) != len(set(actual)):
        raise DataHubValidationError(
            "Proposal set is missing, extra, duplicated, or out of canonical order",
            location="proposals",
        )

    datasets_by_urn = {dataset.urn: dataset for dataset in canonical_datasets}
    jobs_by_urn = {job.urn: job for job in canonical_jobs}
    topology_by_job = {edges.job_urn: edges for edges in canonical_topology}
    for index, proposal in enumerate(canonical_proposals):
        location = f"proposals/{index}"
        parsed_type = _parse_entity_urn(proposal.entity_urn, location=f"{location}/entityUrn")
        if proposal.entity_type != parsed_type:
            raise DataHubValidationError(
                "Proposal entity type does not match its URN",
                location=f"{location}/entityType",
            )
        aspect = proposal.aspect
        if proposal.aspect_name == "dataFlowInfo":
            _validate_data_flow_info(aspect, canonical_flow, location=location)
        elif proposal.aspect_name == "datasetProperties":
            _validate_dataset_properties(
                aspect,
                datasets_by_urn[proposal.entity_urn],
                location=location,
            )
        elif proposal.aspect_name == "dataPlatformInstance":
            _validate_platform_instance(
                aspect,
                datasets_by_urn[proposal.entity_urn],
                location=location,
            )
        elif proposal.aspect_name == "dataJobInfo":
            _validate_data_job_info(
                aspect,
                jobs_by_urn[proposal.entity_urn],
                canonical_flow,
                location=location,
            )
        elif proposal.aspect_name == "dataJobInputOutput":
            _validate_data_job_io(
                aspect,
                topology_by_job[proposal.entity_urn],
                location=location,
            )
        else:
            raise DataHubValidationError(
                "Proposal aspect is not allowed for this entity",
                location=f"{location}/aspectName",
            )
    if canonical_config.flow_urn != canonical_flow.urn:
        raise DataHubValidationError(
            "Proposal set does not match its identity configuration",
            location="config",
        )


def _validate_data_flow_info(
    aspect: dict[str, object],
    flow: DataHubFlowIdentity,
    *,
    location: str,
) -> None:
    _require_exact_keys(
        aspect,
        required={"name", "env", "customProperties"},
        optional={"description"},
        location=location,
    )
    _require_text(aspect["name"], location=f"{location}/aspect/name")
    _require_optional_description(aspect, location=location)
    if aspect["env"] != flow.fabric or aspect["customProperties"] != {}:
        raise DataHubValidationError(
            "DataFlowInfo fabric or defaults are inconsistent",
            location=f"{location}/aspect",
        )


def _validate_dataset_properties(
    aspect: dict[str, object],
    dataset: DataHubDatasetIdentity,
    *,
    location: str,
) -> None:
    _require_exact_keys(
        aspect,
        required={"name", "customProperties", "tags"},
        optional={"description"},
        location=location,
    )
    _require_optional_description(aspect, location=location)
    properties = aspect["customProperties"]
    expected_properties = (
        {"streamt.contract.status": dataset.contract_status}
        if dataset.contract_status is not None
        else {}
    )
    if properties != expected_properties:
        raise DataHubValidationError(
            "DatasetProperties custom properties are not allowlisted",
            location=f"{location}/aspect/customProperties",
        )
    if aspect["name"] != dataset.logical_name or aspect["tags"] != []:
        raise DataHubValidationError(
            "DatasetProperties identity or defaults are inconsistent",
            location=f"{location}/aspect",
        )


def _validate_platform_instance(
    aspect: dict[str, object],
    dataset: DataHubDatasetIdentity,
    *,
    location: str,
) -> None:
    _require_exact_keys(
        aspect,
        required={"platform", "instance"},
        optional=set(),
        location=location,
    )
    if (
        dataset.platform_instance is None
        or dataset.platform_instance_urn is None
        or aspect["platform"] != dataset.platform_urn
        or aspect["instance"] != dataset.platform_instance_urn
    ):
        raise DataHubValidationError(
            "DataPlatformInstance does not match its Dataset identity",
            location=f"{location}/aspect",
        )


def _validate_data_job_info(
    aspect: dict[str, object],
    job: DataHubJobIdentity,
    flow: DataHubFlowIdentity,
    *,
    location: str,
) -> None:
    _require_exact_keys(
        aspect,
        required={"name", "type", "flowUrn", "env", "customProperties"},
        optional={"description"},
        location=location,
    )
    _require_optional_description(aspect, location=location)
    if (
        aspect["name"] != job.process_logical_name
        or aspect["type"] != {"string": job.process_kind}
        or aspect["flowUrn"] != flow.urn
        or aspect["env"] != flow.fabric
        or aspect["customProperties"] != {}
    ):
        raise DataHubValidationError(
            "DataJobInfo identity, type, or defaults are inconsistent",
            location=f"{location}/aspect",
        )


def _validate_data_job_io(
    aspect: dict[str, object],
    topology: DataHubJobTopology,
    *,
    location: str,
) -> None:
    _require_exact_keys(
        aspect,
        required={
            "inputDatasets",
            "outputDatasets",
            "inputDatasetEdges",
            "outputDatasetEdges",
        },
        optional=set(),
        location=location,
    )
    expected_inputs = [{"destinationUrn": urn} for urn in topology.input_dataset_urns]
    expected_outputs = [{"destinationUrn": urn} for urn in topology.output_dataset_urns]
    if (
        aspect["inputDatasets"] != []
        or aspect["outputDatasets"] != []
        or aspect["inputDatasetEdges"] != expected_inputs
        or aspect["outputDatasetEdges"] != expected_outputs
    ):
        raise DataHubValidationError(
            "DataJobInputOutput does not match the exact direct topology",
            location=f"{location}/aspect",
        )


def _require_exact_keys(
    value: Mapping[str, object],
    *,
    required: set[str],
    optional: set[str],
    location: str,
) -> None:
    if not required <= set(value) or not set(value) <= required | optional:
        raise DataHubValidationError(
            "Aspect fields do not match the closed DataHub contract",
            location=f"{location}/aspect",
        )


def _require_text(value: object, *, location: str) -> None:
    if not isinstance(value, str) or not value.strip() or _has_control_or_surrogate(value):
        raise DataHubValidationError(
            "Aspect text must be a non-blank control-free string",
            location=location,
        )


def _require_optional_description(aspect: Mapping[str, object], *, location: str) -> None:
    if "description" in aspect:
        value = aspect["description"]
        if (
            not isinstance(value, str)
            or not value.strip()
            or any(
                character not in {"\t", "\n", "\r"}
                and unicodedata.category(character) in {"Cc", "Cs"}
                for character in value
            )
        ):
            raise DataHubValidationError(
                "Aspect description must be non-blank text",
                location=f"{location}/aspect/description",
            )


def _parse_entity_urn(urn: str, *, location: str) -> DataHubEntityType:
    if urn.startswith(_DATA_FLOW_PREFIX):
        _parse_flow_urn(urn, location=location)
        return "dataFlow"
    if urn.startswith(_DATASET_PREFIX):
        _parse_dataset_urn(urn, location=location)
        return "dataset"
    if urn.startswith(_DATA_JOB_PREFIX):
        _parse_job_urn(urn, location=location)
        return "dataJob"
    raise DataHubValidationError(
        "Entity URN type is unsupported",
        location=location,
    )


def _parse_platform_urn(urn: str, *, location: str) -> str:
    if not urn.startswith(_DATA_PLATFORM_PREFIX):
        raise DataHubValidationError(
            "DataPlatform URN is malformed",
            location=location,
        )
    name = urn[len(_DATA_PLATFORM_PREFIX) :]
    _require_encoded_component(
        name,
        location=location,
        label="DataPlatform name",
        limit=25,
    )
    _require_full_length(urn, maximum=45, location=location, label="DataPlatform URN")
    return name


def _parse_platform_instance_urn(urn: str, *, location: str) -> tuple[str, str]:
    parts = _parse_tuple_urn(
        urn,
        prefix=_DATA_PLATFORM_INSTANCE_PREFIX,
        part_count=2,
        location=location,
    )
    _parse_platform_urn(parts[0], location=f"{location}/platform")
    _require_encoded_component(
        parts[1],
        location=f"{location}/instance",
        label="Platform instance",
        limit=100,
    )
    _require_full_length(
        urn,
        maximum=100,
        location=location,
        label="DataPlatformInstance URN",
    )
    return (parts[0], parts[1])


def _parse_dataset_urn(urn: str, *, location: str) -> tuple[str, str, DataHubFabric]:
    parts = _parse_tuple_urn(
        urn,
        prefix=_DATASET_PREFIX,
        part_count=3,
        location=location,
    )
    _parse_platform_urn(parts[0], location=f"{location}/platform")
    _require_encoded_component(
        parts[1],
        location=f"{location}/name",
        label="Dataset name",
        limit=210,
    )
    fabric = require_datahub_fabric(parts[2], location=f"{location}/fabric")
    _require_full_length(urn, maximum=284, location=location, label="Dataset URN")
    return (parts[0], parts[1], fabric)


def _parse_flow_urn(urn: str, *, location: str) -> tuple[str, str, DataHubFabric]:
    parts = _parse_tuple_urn(
        urn,
        prefix=_DATA_FLOW_PREFIX,
        part_count=3,
        location=location,
    )
    _require_encoded_component(
        parts[0],
        location=f"{location}/orchestrator",
        label="DataFlow orchestrator",
        limit=50,
    )
    if parts[0] != "streamt":
        raise DataHubValidationError(
            "DataFlow orchestrator must be streamt",
            location=f"{location}/orchestrator",
        )
    _require_encoded_component(
        parts[1],
        location=f"{location}/flow_id",
        label="DataFlow ID",
        limit=200,
    )
    fabric = require_datahub_fabric(parts[2], location=f"{location}/cluster")
    _require_full_length(urn, maximum=373, location=location, label="DataFlow URN")
    return (parts[0], parts[1], fabric)


def _parse_job_urn(urn: str, *, location: str) -> tuple[str, str]:
    parts = _parse_tuple_urn(
        urn,
        prefix=_DATA_JOB_PREFIX,
        part_count=2,
        location=location,
    )
    _parse_flow_urn(parts[0], location=f"{location}/flow")
    _require_encoded_component(
        parts[1],
        location=f"{location}/job_id",
        label="DataJob ID",
        limit=200,
    )
    _require_full_length(urn, maximum=594, location=location, label="DataJob URN")
    return (parts[0], parts[1])


def _parse_tuple_urn(
    urn: object,
    *,
    prefix: str,
    part_count: int,
    location: str,
) -> tuple[str, ...]:
    if not isinstance(urn, str) or not urn.startswith(f"{prefix}(") or not urn.endswith(")"):
        raise DataHubValidationError(
            "Typed DataHub URN is malformed",
            location=location,
        )
    body = urn[len(prefix) + 1 : -1]
    parts: list[str] = []
    start = 0
    depth = 0
    for index, character in enumerate(body):
        if character == "(":
            depth += 1
        elif character == ")":
            depth -= 1
            if depth < 0:
                raise DataHubValidationError(
                    "Typed DataHub URN has mismatched parentheses",
                    location=location,
                )
        elif character == "," and depth == 0:
            parts.append(body[start:index])
            start = index + 1
    if depth != 0:
        raise DataHubValidationError(
            "Typed DataHub URN has mismatched parentheses",
            location=location,
        )
    parts.append(body[start:])
    if len(parts) != part_count or any(not part for part in parts):
        raise DataHubValidationError(
            "Typed DataHub URN has the wrong key shape",
            location=location,
        )
    return tuple(parts)
