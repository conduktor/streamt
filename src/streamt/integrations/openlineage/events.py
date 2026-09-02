"""Pure OpenLineage event and identity construction helpers."""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from ipaddress import IPv4Address, IPv6Address
from types import MappingProxyType
from typing import Literal
from urllib.parse import quote, urlsplit
from uuid import UUID

OPENLINEAGE_RELEASE = "1.53.0"
OPENLINEAGE_PRODUCER = "https://github.com/conduktor/streamt"
OPENLINEAGE_CORE_SCHEMA_ID = "https://openlineage.io/spec/2-0-2/OpenLineage.json"

EventKind = Literal["dataset", "job", "run"]
EventType = Literal["START", "RUNNING", "COMPLETE", "ABORT", "FAIL", "OTHER"]
FacetScope = Literal["dataset", "job", "run"]

EVENT_SCHEMA_URLS: Mapping[EventKind, str] = MappingProxyType({
    "dataset": f"{OPENLINEAGE_CORE_SCHEMA_ID}#/$defs/DatasetEvent",
    "job": f"{OPENLINEAGE_CORE_SCHEMA_ID}#/$defs/JobEvent",
    "run": f"{OPENLINEAGE_CORE_SCHEMA_ID}#/$defs/RunEvent",
})

FACET_SCHEMA_URLS: Mapping[FacetScope, Mapping[str, str]] = MappingProxyType({
    "dataset": MappingProxyType({
        "datasetType": (
            "https://openlineage.io/spec/facets/1-0-1/"
            "DatasetTypeDatasetFacet.json#/$defs/DatasetTypeDatasetFacet"
        ),
        "documentation": (
            "https://openlineage.io/spec/facets/1-1-0/"
            "DocumentationDatasetFacet.json#/$defs/DocumentationDatasetFacet"
        ),
        "ownership": (
            "https://openlineage.io/spec/facets/1-0-1/"
            "OwnershipDatasetFacet.json#/$defs/OwnershipDatasetFacet"
        ),
        "schema": (
            "https://openlineage.io/spec/facets/1-2-0/"
            "SchemaDatasetFacet.json#/$defs/SchemaDatasetFacet"
        ),
    }),
    "job": MappingProxyType({
        "documentation": (
            "https://openlineage.io/spec/facets/1-1-0/"
            "DocumentationJobFacet.json#/$defs/DocumentationJobFacet"
        ),
        "jobType": (
            "https://openlineage.io/spec/facets/2-0-4/"
            "JobTypeJobFacet.json#/$defs/JobTypeJobFacet"
        ),
        "ownership": (
            "https://openlineage.io/spec/facets/1-0-1/"
            "OwnershipJobFacet.json#/$defs/OwnershipJobFacet"
        ),
    }),
    "run": MappingProxyType({
        "errorMessage": (
            "https://openlineage.io/spec/facets/1-0-1/"
            "ErrorMessageRunFacet.json#/$defs/ErrorMessageRunFacet"
        ),
    }),
})

_EVENT_TYPES = frozenset({"START", "RUNNING", "COMPLETE", "ABORT", "FAIL", "OTHER"})
_DNS_LABEL_RE = re.compile(r"[A-Za-z0-9](?:[A-Za-z0-9-]*[A-Za-z0-9])?")


class OpenLineageConstructionError(ValueError):
    """Raised when an event cannot be constructed without an invalid identity."""


@dataclass(frozen=True, order=True)
class JobIdentity:
    """A stable OpenLineage job identity."""

    namespace: str
    name: str

    def __post_init__(self) -> None:
        _require_nonblank(self.namespace, "job namespace")
        _require_nonblank(self.name, "job name")


@dataclass(frozen=True, order=True)
class DatasetIdentity:
    """A stable Kafka-backed OpenLineage dataset identity."""

    namespace: str
    name: str

    def __post_init__(self) -> None:
        validate_kafka_namespace(self.namespace)
        _require_nonblank(self.name, "dataset name")


@dataclass(frozen=True, order=True)
class RunIdentity:
    """A canonical UUID OpenLineage run identity."""

    run_id: str

    def __post_init__(self) -> None:
        try:
            parsed = UUID(self.run_id)
        except (AttributeError, TypeError, ValueError) as exc:
            raise OpenLineageConstructionError("run ID must be a UUID") from exc
        canonical = str(parsed)
        if self.run_id.lower() != canonical:
            raise OpenLineageConstructionError("run ID must use canonical UUID syntax")
        object.__setattr__(self, "run_id", canonical)


def encode_job_segment(value: str) -> str:
    """Encode one stable job-name segment using RFC 3986 unreserved characters."""
    _require_nonblank(value, "job-name segment")
    return quote(value, safe="-._~", encoding="utf-8", errors="strict")


def model_job_name(project_name: str, model_name: str) -> str:
    """Return the stable streamt model job name."""
    return (
        f"streamt/{encode_job_segment(project_name)}/models/"
        f"{encode_job_segment(model_name)}"
    )


def command_job_name(project_name: str, command: Literal["apply", "test"]) -> str:
    """Return the stable streamt command job name."""
    if command not in {"apply", "test"}:
        raise OpenLineageConstructionError("OpenLineage command job type is not supported")
    return f"streamt/{encode_job_segment(project_name)}/commands/{command}"


def validate_kafka_namespace(value: str) -> str:
    """Validate and preserve an exact ``kafka://host:port`` dataset namespace."""
    _require_nonblank(value, "dataset namespace")
    if (
        value != value.strip()
        or any(ord(character) < 0x21 or ord(character) > 0x7E for character in value)
        or not value.startswith("kafka://")
        or "?" in value
        or "#" in value
    ):
        raise OpenLineageConstructionError(
            "dataset namespace must be an exact kafka:// URI"
        )
    try:
        parsed = urlsplit(value)
        port = parsed.port
    except ValueError as exc:
        raise OpenLineageConstructionError(
            "dataset namespace must contain one valid host and explicit port"
        ) from exc
    authority = parsed.netloc
    if (
        parsed.scheme != "kafka"
        or not parsed.hostname
        or port is None
        or port < 1
        or parsed.username is not None
        or parsed.password is not None
        or parsed.path
        or parsed.query
        or parsed.fragment
        or "," in parsed.netloc
    ):
        raise OpenLineageConstructionError(
            "dataset namespace must contain one host and explicit port only"
        )
    _validate_kafka_authority(authority)
    return value


def _validate_kafka_authority(authority: str) -> None:
    """Require an ASCII DNS name, IPv4 address, or bracketed IPv6 address."""
    if authority.startswith("["):
        closing_bracket = authority.find("]")
        if closing_bracket < 0:
            raise OpenLineageConstructionError(
                "dataset namespace must contain a valid bracketed IPv6 host"
            )
        host = authority[1:closing_bracket]
        port_text = authority[closing_bracket + 1 :]
        if not port_text.startswith(":") or not port_text[1:].isdigit() or "%" in host:
            raise OpenLineageConstructionError(
                "dataset namespace must contain a valid bracketed IPv6 host"
            )
        try:
            IPv6Address(host)
        except ValueError as exc:
            raise OpenLineageConstructionError(
                "dataset namespace must contain a valid bracketed IPv6 host"
            ) from exc
        return

    if authority.count(":") != 1:
        raise OpenLineageConstructionError(
            "dataset namespace IPv6 hosts must use bracketed URI syntax"
        )
    host, port_text = authority.rsplit(":", 1)
    if not port_text.isdigit():
        raise OpenLineageConstructionError(
            "dataset namespace port must contain ASCII digits only"
        )

    if host.count(".") == 3 and all(part.isdigit() for part in host.split(".")):
        try:
            IPv4Address(host)
        except ValueError as exc:
            raise OpenLineageConstructionError(
                "dataset namespace must contain a valid IPv4 host"
            ) from exc
        return

    dns_name = host[:-1] if host.endswith(".") else host
    labels = dns_name.split(".")
    if (
        not dns_name
        or len(dns_name) > 253
        or any(
            not label
            or len(label) > 63
            or _DNS_LABEL_RE.fullmatch(label) is None
            for label in labels
        )
    ):
        raise OpenLineageConstructionError(
            "dataset namespace must contain a valid ASCII DNS host"
        )


def kafka_namespace_from_bootstrap(bootstrap_servers: str) -> str:
    """Derive a namespace only from one unambiguous ``host:port`` bootstrap value."""
    _require_nonblank(bootstrap_servers, "Kafka bootstrap server")
    endpoint = bootstrap_servers.strip()
    if (
        "," in endpoint
        or "://" in endpoint
        or any(character.isspace() for character in endpoint)
    ):
        raise OpenLineageConstructionError(
            "Kafka namespace derivation requires exactly one host:port value"
        )
    namespace = f"kafka://{endpoint}"
    validate_kafka_namespace(namespace)
    return namespace


def standard_facet(
    scope: FacetScope,
    key: str,
    values: Mapping[str, object],
) -> dict[str, object]:
    """Construct and validate one supported standard facet deterministically."""
    try:
        schema_url = FACET_SCHEMA_URLS[scope][key]
    except KeyError as exc:
        raise OpenLineageConstructionError(
            "facet key is not supported for the requested entity"
        ) from exc
    if "_producer" in values or "_schemaURL" in values:
        raise OpenLineageConstructionError("facet metadata is controlled by streamt")
    facet: dict[str, object] = {
        "_producer": OPENLINEAGE_PRODUCER,
        "_schemaURL": schema_url,
    }
    facet.update((name, values[name]) for name in sorted(values))

    from streamt.integrations.openlineage.validation import validate_standard_facet

    validate_standard_facet(scope, key, facet)
    return facet


def build_dataset_event(
    *,
    event_time: str,
    dataset: DatasetIdentity,
    facets: Mapping[str, Mapping[str, object]] | None = None,
) -> dict[str, object]:
    """Build one deterministic static DatasetEvent."""
    event: dict[str, object] = _base_event("dataset", event_time)
    event["dataset"] = _dataset_object(dataset, facets)
    _validate_built_event(event)
    return event


def build_job_event(
    *,
    event_time: str,
    job: JobIdentity,
    facets: Mapping[str, Mapping[str, object]] | None = None,
    inputs: Sequence[DatasetIdentity] = (),
    outputs: Sequence[DatasetIdentity] = (),
) -> dict[str, object]:
    """Build one deterministic static JobEvent."""
    event: dict[str, object] = _base_event("job", event_time)
    event["job"] = _job_object(job, facets)
    event.update(_dataset_collections(inputs, outputs))
    _validate_built_event(event)
    return event


def build_run_event(
    *,
    event_time: str,
    event_type: EventType,
    run: RunIdentity,
    job: JobIdentity,
    run_facets: Mapping[str, Mapping[str, object]] | None = None,
    job_facets: Mapping[str, Mapping[str, object]] | None = None,
    inputs: Sequence[DatasetIdentity] = (),
    outputs: Sequence[DatasetIdentity] = (),
) -> dict[str, object]:
    """Build one deterministic RunEvent without inventing time or run identity."""
    if event_type not in _EVENT_TYPES:
        raise OpenLineageConstructionError("run event type is not supported")
    event: dict[str, object] = _base_event("run", event_time)
    event["eventType"] = event_type
    run_object: dict[str, object] = {"runId": run.run_id}
    copied_run_facets = _copy_facets(run_facets)
    if copied_run_facets:
        run_object["facets"] = copied_run_facets
    event["run"] = run_object
    event["job"] = _job_object(job, job_facets)
    event.update(_dataset_collections(inputs, outputs))
    _validate_built_event(event)
    return event


def _base_event(kind: EventKind, event_time: str) -> dict[str, object]:
    return {
        "eventTime": event_time,
        "producer": OPENLINEAGE_PRODUCER,
        "schemaURL": EVENT_SCHEMA_URLS[kind],
    }


def _job_object(
    identity: JobIdentity,
    facets: Mapping[str, Mapping[str, object]] | None,
) -> dict[str, object]:
    result: dict[str, object] = {
        "namespace": identity.namespace,
        "name": identity.name,
    }
    copied_facets = _copy_facets(facets)
    if copied_facets:
        result["facets"] = copied_facets
    return result


def _dataset_object(
    identity: DatasetIdentity,
    facets: Mapping[str, Mapping[str, object]] | None = None,
) -> dict[str, object]:
    result: dict[str, object] = {
        "namespace": identity.namespace,
        "name": identity.name,
    }
    copied_facets = _copy_facets(facets)
    if copied_facets:
        result["facets"] = copied_facets
    return result


def _copy_facets(
    facets: Mapping[str, Mapping[str, object]] | None,
) -> dict[str, object]:
    if facets is None:
        return {}
    return {
        key: {name: facet[name] for name in sorted(facet)}
        for key, facet in sorted(facets.items())
    }


def _dataset_collections(
    inputs: Sequence[DatasetIdentity],
    outputs: Sequence[DatasetIdentity],
) -> dict[str, object]:
    input_set = set(inputs)
    output_set = set(outputs)
    if len(input_set) != len(inputs) or len(output_set) != len(outputs):
        raise OpenLineageConstructionError("dataset identities must be unique per direction")
    if input_set & output_set:
        raise OpenLineageConstructionError(
            "one dataset cannot be both an input and output in the same event"
        )
    result: dict[str, object] = {}
    if input_set:
        result["inputs"] = [_dataset_object(item) for item in sorted(input_set)]
    if output_set:
        result["outputs"] = [_dataset_object(item) for item in sorted(output_set)]
    return result


def _validate_built_event(event: Mapping[str, object]) -> None:
    from streamt.integrations.openlineage.validation import validate_event

    validate_event(event)


def _require_nonblank(value: object, label: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise OpenLineageConstructionError(f"{label} must be a nonblank string")
