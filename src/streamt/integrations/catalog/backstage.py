"""Pure Backstage core-entity mapping and canonical YAML serialization."""

from __future__ import annotations

import hashlib
import json
import re
import unicodedata
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Literal, cast

import yaml

from streamt.integrations.catalog.backstage_validation import (
    BackstageResourceError,
    BackstageValidationError,
    validate_backstage_entity,
)
from streamt.integrations.catalog.inputs import (
    CatalogInputError,
    ParsedEntityRef,
    require_catalog_id,
    require_catalog_namespace,
    require_entity_ref,
    require_lifecycle,
    validate_owner_map,
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

BackstageKind = Literal["System", "Resource", "Component"]

_API_VERSION = "backstage.io/v1alpha1"
_ENTITY_NAME_RE = re.compile(r"^[A-Za-z0-9](?:[-_.]?[A-Za-z0-9])*$")
_TAG_RE = re.compile(r"^[a-z0-9:+#]+(?:-[a-z0-9:+#]+)*$")
_NON_STEM_RE = re.compile(r"[^a-z0-9]+")
_BASE_ANNOTATIONS = frozenset(
    {
        "streamt.dev/catalog-id",
        "streamt.dev/environment",
        "streamt.dev/logical-kind",
        "streamt.dev/logical-name",
        "streamt.dev/project",
    }
)
_ANNOTATIONS_BY_KIND = {
    "System": _BASE_ANNOTATIONS,
    "Resource": _BASE_ANNOTATIONS | {"streamt.dev/contract", "streamt.dev/physical-name"},
    "Component": _BASE_ANNOTATIONS | {"streamt.dev/process-kind"},
}


class BackstageExportError(ValueError):
    """A bounded mapping failure with one safe structural location."""

    def __init__(self, message: str, *, location: str) -> None:
        super().__init__(message)
        self.location = location


@dataclass(frozen=True, order=True)
class BackstageExportWarning:
    """One deterministic warning for metadata intentionally not projected."""

    code: str
    message: str
    location: str


@dataclass(frozen=True, repr=False)
class BackstageCatalogExport:
    """Immutable validated entities, warnings, and their canonical YAML."""

    _entity_payloads: tuple[str, ...]
    warnings: tuple[BackstageExportWarning, ...]
    yaml_text: str

    @property
    def entities(self) -> tuple[dict[str, object], ...]:
        """Return defensive entity copies for structured output."""
        return tuple(
            cast(dict[str, object], json.loads(payload)) for payload in self._entity_payloads
        )

    @property
    def yaml_bytes(self) -> bytes:
        """Return the canonical UTF-8 bytes."""
        return self.yaml_text.encode("utf-8")

    @property
    def yaml(self) -> str:
        """Return the canonical YAML text."""
        return self.yaml_text

    def __repr__(self) -> str:
        return (
            "BackstageCatalogExport("
            f"entity_count={len(self._entity_payloads)}, "
            f"warning_count={len(self.warnings)})"
        )


@dataclass(frozen=True)
class _Inputs:
    catalog_id: str
    namespace: str
    default_owner: ParsedEntityRef
    lifecycle: str
    owner_map: Mapping[str, ParsedEntityRef]
    kafka_cluster: ParsedEntityRef | None
    gateway_cluster: ParsedEntityRef | None
    domain: ParsedEntityRef | None


@dataclass(frozen=True)
class _SnapshotIndex:
    datasets: Mapping[LogicalDataset, CatalogDataset]
    processes: Mapping[str, CatalogProcess]


class _CanonicalDumper(yaml.SafeDumper):
    """Safe deterministic dumper that never emits YAML aliases."""

    def ignore_aliases(self, _data: object) -> bool:
        return True


def generate_backstage_catalog(
    snapshot: CatalogSnapshot,
    catalog_id: object,
    catalog_namespace: object,
    default_owner_ref: object,
    lifecycle: object,
    owner_map: Mapping[str, object] | None,
    kafka_cluster_ref: object | None,
    gateway_cluster_ref: object | None,
    domain_ref: object | None,
) -> BackstageCatalogExport:
    """Map one neutral snapshot to validated canonical Backstage entity YAML."""
    inputs = _validate_inputs(
        catalog_id=catalog_id,
        catalog_namespace=catalog_namespace,
        default_owner_ref=default_owner_ref,
        lifecycle=lifecycle,
        owner_map=owner_map,
        kafka_cluster_ref=kafka_cluster_ref,
        gateway_cluster_ref=gateway_cluster_ref,
        domain_ref=domain_ref,
    )
    index = _validate_snapshot(snapshot)
    _require_cluster_refs(snapshot, inputs)

    system = _system_entity(snapshot, inputs)
    system_ref = _generated_ref("system", inputs.namespace, _entity_name(system))
    process_refs = {
        name: _generated_ref(
            "component",
            inputs.namespace,
            _component_name(inputs.catalog_id, snapshot.effective_environment, name),
        )
        for name in index.processes
    }
    dataset_refs = {
        logical: _generated_ref(
            "resource",
            inputs.namespace,
            _resource_name(dataset, snapshot.effective_environment, inputs),
        )
        for logical, dataset in index.datasets.items()
    }

    resources = [
        _resource_entity(
            snapshot,
            dataset,
            inputs,
            system_ref=system_ref,
            resource_ref=dataset_refs[logical],
            producer_ref=process_refs.get(dataset.logical_name),
        )
        for logical, dataset in index.datasets.items()
    ]
    components = [
        _component_entity(
            snapshot,
            process,
            inputs,
            system_ref=system_ref,
            dependency_refs=tuple(
                dataset_refs[dependency.logical_identity] for dependency in process.dependencies
            ),
        )
        for process in index.processes.values()
    ]

    resources.sort(key=_entity_sort_key)
    components.sort(key=_entity_sort_key)
    entities = (system, *resources, *components)
    external_refs = _permitted_external_refs(inputs)
    _validate_entities(entities, inputs, external_refs)

    warnings = _warnings(snapshot, index)
    yaml_text = _serialize_entities(entities)
    payloads = tuple(
        json.dumps(
            entity,
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
        )
        for entity in entities
    )
    return BackstageCatalogExport(
        _entity_payloads=payloads,
        warnings=warnings,
        yaml_text=yaml_text,
    )


def _validate_inputs(
    *,
    catalog_id: object,
    catalog_namespace: object,
    default_owner_ref: object,
    lifecycle: object,
    owner_map: Mapping[str, object] | None,
    kafka_cluster_ref: object | None,
    gateway_cluster_ref: object | None,
    domain_ref: object | None,
) -> _Inputs:
    try:
        validated_catalog_id = require_catalog_id(catalog_id)
        namespace = require_catalog_namespace(catalog_namespace)
        default_owner = require_entity_ref(
            default_owner_ref,
            allowed_kinds=frozenset({"group", "user"}),
            location="default_owner_ref",
        )
        validated_lifecycle = require_lifecycle(lifecycle)
        owners = _validate_mapper_owner_map(owner_map)
        kafka_cluster = _optional_ref(
            kafka_cluster_ref,
            allowed_kinds=frozenset({"resource"}),
            location="kafka_cluster_ref",
        )
        gateway_cluster = _optional_ref(
            gateway_cluster_ref,
            allowed_kinds=frozenset({"resource"}),
            location="gateway_cluster_ref",
        )
        domain = _optional_ref(
            domain_ref,
            allowed_kinds=frozenset({"domain"}),
            location="domain_ref",
        )
    except CatalogInputError as error:
        raise BackstageExportError(str(error), location=error.location) from error
    return _Inputs(
        catalog_id=validated_catalog_id,
        namespace=namespace,
        default_owner=default_owner,
        lifecycle=validated_lifecycle,
        owner_map=owners,
        kafka_cluster=kafka_cluster,
        gateway_cluster=gateway_cluster,
        domain=domain,
    )


def _validate_mapper_owner_map(
    owner_map: Mapping[str, object] | None,
) -> Mapping[str, ParsedEntityRef]:
    if owner_map is None or not owner_map:
        return validate_owner_map({"version": 1, "owners": {}})
    if set(owner_map) == {"version", "owners"}:
        return validate_owner_map(owner_map)
    if all(isinstance(value, ParsedEntityRef) for value in owner_map.values()):
        raw = {label: cast(ParsedEntityRef, value).canonical for label, value in owner_map.items()}
        return validate_owner_map({"version": 1, "owners": raw})
    return validate_owner_map(owner_map)


def _optional_ref(
    value: object | None,
    *,
    allowed_kinds: frozenset[str],
    location: str,
) -> ParsedEntityRef | None:
    if value is None:
        return None
    return require_entity_ref(value, allowed_kinds=allowed_kinds, location=location)


def _validate_snapshot(snapshot: CatalogSnapshot) -> _SnapshotIndex:
    if not isinstance(snapshot, CatalogSnapshot):
        raise BackstageExportError(
            "Backstage export requires a catalog snapshot",
            location="snapshot",
        )
    if type(snapshot.version) is not int or snapshot.version != 1:
        raise BackstageExportError(
            "Catalog snapshot version is unsupported",
            location="snapshot.version",
        )
    _snapshot_text(snapshot.project_name, location="snapshot.project_name")
    _snapshot_text(
        snapshot.effective_environment,
        location="snapshot.effective_environment",
    )
    _snapshot_description(
        snapshot.project_description,
        location="snapshot.project_description",
    )
    if not isinstance(snapshot.datasets, tuple):
        raise BackstageExportError(
            "Catalog datasets must be an immutable tuple",
            location="snapshot.datasets",
        )
    if not isinstance(snapshot.processes, tuple):
        raise BackstageExportError(
            "Catalog processes must be an immutable tuple",
            location="snapshot.processes",
        )
    if not isinstance(snapshot.omitted_exposure_names, tuple):
        raise BackstageExportError(
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
            raise BackstageExportError(
                "Catalog dataset logical identities must be unique",
                location=location,
            )
        prior_kind = logical_names.get(dataset.logical_name)
        if prior_kind is not None and prior_kind != dataset.logical_kind:
            raise BackstageExportError(
                "Source and model logical names must not collide",
                location=f"{location}/logical_name",
            )
        if dataset.physical_identity in physical:
            raise BackstageExportError(
                "Catalog dataset physical identities must be unique",
                location=f"{location}/physical_name",
            )
        datasets[logical] = dataset
        logical_names[dataset.logical_name] = dataset.logical_kind
        physical.add(dataset.physical_identity)

    processes: dict[str, CatalogProcess] = {}
    for position, process in enumerate(snapshot.processes):
        location = f"snapshot.processes/{position}"
        _validate_process(process, datasets=datasets, location=location)
        if process.logical_name in processes:
            raise BackstageExportError(
                "Catalog process logical identities must be unique",
                location=f"{location}/logical_name",
            )
        if ("source", process.logical_name) in datasets:
            raise BackstageExportError(
                "A catalog process cannot use a source logical identity",
                location=f"{location}/logical_name",
            )
        output = datasets.get(("model", process.logical_name))
        if process.process_kind == "connect":
            if output is not None:
                raise BackstageExportError(
                    "Connect processes cannot expose catalog output datasets",
                    location=location,
                )
        else:
            expected_transport = "gateway" if process.process_kind == "gateway" else "kafka"
            if output is None or output.transport != expected_transport:
                raise BackstageExportError(
                    "Catalog process output shape is inconsistent",
                    location=location,
                )
            if (
                output.description != process.description
                or output.tags != process.tags
                or output.owner != process.owner
            ):
                raise BackstageExportError(
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
            raise BackstageExportError(
                "Gateway output dataset requires its compiled process",
                location=f"snapshot.datasets/{position}",
            )

    _validate_process_cycles(processes)
    for position, exposure_name in enumerate(snapshot.omitted_exposure_names):
        _snapshot_text(
            exposure_name,
            location=f"snapshot.omitted_exposure_names/{position}",
        )
    return _SnapshotIndex(datasets=datasets, processes=processes)


def _validate_dataset(dataset: object, *, location: str) -> None:
    if not isinstance(dataset, CatalogDataset):
        raise BackstageExportError(
            "Catalog dataset has an invalid type",
            location=location,
        )
    if dataset.logical_kind not in {"source", "model"}:
        raise BackstageExportError(
            "Catalog dataset logical kind is unsupported",
            location=f"{location}/logical_kind",
        )
    if dataset.transport not in {"kafka", "gateway"}:
        raise BackstageExportError(
            "Catalog dataset transport is unsupported",
            location=f"{location}/transport",
        )
    if dataset.logical_kind == "source" and dataset.transport != "kafka":
        raise BackstageExportError(
            "Source datasets must be Kafka resources",
            location=f"{location}/transport",
        )
    _snapshot_text(dataset.logical_name, location=f"{location}/logical_name")
    _snapshot_text(dataset.physical_name, location=f"{location}/physical_name")
    _snapshot_description(dataset.description, location=f"{location}/description")
    _snapshot_tags(dataset.tags, location=f"{location}/tags")
    _snapshot_owner(dataset.owner, location=f"{location}/owner")
    if dataset.contract is not None:
        if not isinstance(
            dataset.contract, CatalogContractSummary
        ) or dataset.contract.status not in {"declared", "enforced"}:
            raise BackstageExportError(
                "Catalog contract summary is invalid",
                location=f"{location}/contract",
            )
        if dataset.logical_kind != "model":
            raise BackstageExportError(
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
        raise BackstageExportError(
            "Catalog process has an invalid type",
            location=location,
        )
    if process.process_kind not in {"flink", "gateway", "connect"}:
        raise BackstageExportError(
            "Catalog process kind is unsupported",
            location=f"{location}/process_kind",
        )
    _snapshot_text(process.logical_name, location=f"{location}/logical_name")
    _snapshot_description(process.description, location=f"{location}/description")
    _snapshot_tags(process.tags, location=f"{location}/tags")
    _snapshot_owner(process.owner, location=f"{location}/owner")
    if not isinstance(process.dependencies, tuple):
        raise BackstageExportError(
            "Catalog dependencies must be an immutable tuple",
            location=f"{location}/dependencies",
        )
    seen: set[LogicalDataset] = set()
    for position, dependency in enumerate(process.dependencies):
        dependency_location = f"{location}/dependencies/{position}"
        if not isinstance(dependency, CatalogDependency):
            raise BackstageExportError(
                "Catalog dependency has an invalid type",
                location=dependency_location,
            )
        if dependency.logical_kind not in {"source", "model"}:
            raise BackstageExportError(
                "Catalog dependency kind is unsupported",
                location=f"{dependency_location}/logical_kind",
            )
        _snapshot_text(
            dependency.logical_name,
            location=f"{dependency_location}/logical_name",
        )
        if dependency.logical_identity in seen:
            raise BackstageExportError(
                "Catalog direct dependencies must be unique",
                location=dependency_location,
            )
        if dependency.logical_identity == ("model", process.logical_name):
            raise BackstageExportError(
                "A catalog process cannot depend on its own output",
                location=dependency_location,
            )
        if dependency.logical_identity not in datasets:
            raise BackstageExportError(
                "Catalog dependency target is absent",
                location=dependency_location,
            )
        seen.add(dependency.logical_identity)
    if process.process_kind in {"gateway", "connect"} and not process.dependencies:
        raise BackstageExportError(
            "Gateway and Connect processes require direct input metadata",
            location=f"{location}/dependencies",
        )
    if process.process_kind == "gateway" and len(process.dependencies) != 1:
        raise BackstageExportError(
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
            raise BackstageExportError(
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
    if not isinstance(value, str) or not value.strip() or _has_unsafe_text(value, False):
        raise BackstageExportError(
            "Catalog snapshot text is invalid",
            location=location,
        )
    return value


def _snapshot_description(value: object, *, location: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or not value.strip() or _has_unsafe_text(value, True):
        raise BackstageExportError(
            "Catalog snapshot description is invalid",
            location=location,
        )
    return value


def _snapshot_tags(value: object, *, location: str) -> tuple[str, ...]:
    if not isinstance(value, tuple):
        raise BackstageExportError(
            "Catalog tags must be an immutable tuple",
            location=location,
        )
    seen: set[str] = set()
    for position, tag in enumerate(value):
        if not isinstance(tag, str) or len(tag) > 63 or _TAG_RE.fullmatch(tag) is None:
            raise BackstageExportError(
                "Catalog tag does not match the pinned Backstage policy",
                location=f"{location}/{position}",
            )
        if tag in seen:
            raise BackstageExportError(
                "Catalog tags must be unique",
                location=f"{location}/{position}",
            )
        seen.add(tag)
    return value


def _snapshot_owner(value: object, *, location: str) -> CatalogOwnerLabel | None:
    if value is None:
        return None
    if not isinstance(value, CatalogOwnerLabel):
        raise BackstageExportError(
            "Catalog owner must be an owner label",
            location=location,
        )
    _snapshot_text(value.label, location=location)
    return value


def _has_unsafe_text(value: str, allow_layout: bool) -> bool:
    allowed = {"\t", "\n", "\r"} if allow_layout else set()
    return any(
        character not in allowed and unicodedata.category(character) in {"Cc", "Cs"}
        for character in value
    )


def _require_cluster_refs(snapshot: CatalogSnapshot, inputs: _Inputs) -> None:
    if any(dataset.transport == "kafka" for dataset in snapshot.datasets):
        if inputs.kafka_cluster is None:
            raise BackstageExportError(
                "Kafka catalog resources require an explicit cluster reference",
                location="kafka_cluster_ref",
            )
    if any(dataset.transport == "gateway" for dataset in snapshot.datasets):
        if inputs.gateway_cluster is None:
            raise BackstageExportError(
                "Gateway catalog resources require an explicit cluster reference",
                location="gateway_cluster_ref",
            )


def _system_entity(snapshot: CatalogSnapshot, inputs: _Inputs) -> dict[str, object]:
    name = _system_name(inputs.catalog_id, snapshot.effective_environment)
    metadata = _metadata(
        name=name,
        namespace=inputs.namespace,
        title=snapshot.project_name,
        description=snapshot.project_description,
        annotations=_annotations(
            snapshot,
            inputs.catalog_id,
            logical_kind="project",
            logical_name=snapshot.project_name,
        ),
        tags=(),
    )
    spec: dict[str, object] = {"owner": inputs.default_owner.canonical}
    if inputs.domain is not None:
        spec["domain"] = inputs.domain.canonical
    return {
        "apiVersion": _API_VERSION,
        "kind": "System",
        "metadata": metadata,
        "spec": spec,
    }


def _resource_entity(
    snapshot: CatalogSnapshot,
    dataset: CatalogDataset,
    inputs: _Inputs,
    *,
    system_ref: str,
    resource_ref: str,
    producer_ref: str | None,
) -> dict[str, object]:
    del resource_ref
    name = _resource_name(dataset, snapshot.effective_environment, inputs)
    extra_annotations = {"streamt.dev/physical-name": dataset.physical_name}
    if dataset.contract is not None:
        extra_annotations["streamt.dev/contract"] = dataset.contract.status
    metadata = _metadata(
        name=name,
        namespace=inputs.namespace,
        title=dataset.logical_name,
        description=dataset.description,
        annotations=_annotations(
            snapshot,
            inputs.catalog_id,
            logical_kind=dataset.logical_kind,
            logical_name=dataset.logical_name,
            extra=extra_annotations,
        ),
        tags=dataset.tags,
    )
    resource_type = _resource_type(dataset)
    spec: dict[str, object] = {
        "type": resource_type,
        "owner": _resolved_owner(dataset.owner, inputs, location="resources.owner"),
    }
    if dataset.logical_kind == "model":
        spec["system"] = system_ref
    cluster = inputs.gateway_cluster if dataset.transport == "gateway" else inputs.kafka_cluster
    if cluster is None:
        raise BackstageExportError(
            "Catalog resource cluster reference is absent",
            location="resources.dependsOn",
        )
    dependencies = [cluster.canonical]
    if producer_ref is not None and dataset.logical_kind == "model":
        dependencies.append(producer_ref)
    spec["dependsOn"] = sorted(set(dependencies), key=_reference_sort_key)
    return {
        "apiVersion": _API_VERSION,
        "kind": "Resource",
        "metadata": metadata,
        "spec": spec,
    }


def _component_entity(
    snapshot: CatalogSnapshot,
    process: CatalogProcess,
    inputs: _Inputs,
    *,
    system_ref: str,
    dependency_refs: tuple[str, ...],
) -> dict[str, object]:
    metadata = _metadata(
        name=_component_name(
            inputs.catalog_id,
            snapshot.effective_environment,
            process.logical_name,
        ),
        namespace=inputs.namespace,
        title=process.logical_name,
        description=process.description,
        annotations=_annotations(
            snapshot,
            inputs.catalog_id,
            logical_kind="model",
            logical_name=process.logical_name,
            extra={"streamt.dev/process-kind": process.process_kind},
        ),
        tags=process.tags,
    )
    spec: dict[str, object] = {
        "type": "data-pipeline",
        "lifecycle": inputs.lifecycle,
        "owner": _resolved_owner(process.owner, inputs, location="components.owner"),
        "system": system_ref,
    }
    if dependency_refs:
        spec["dependsOn"] = sorted(set(dependency_refs), key=_reference_sort_key)
    return {
        "apiVersion": _API_VERSION,
        "kind": "Component",
        "metadata": metadata,
        "spec": spec,
    }


def _metadata(
    *,
    name: str,
    namespace: str,
    title: str,
    description: str | None,
    annotations: Mapping[str, str],
    tags: Sequence[str],
) -> dict[str, object]:
    metadata: dict[str, object] = {
        "name": name,
        "namespace": namespace,
        "title": title,
    }
    if description is not None:
        metadata["description"] = description
    metadata["annotations"] = dict(sorted(annotations.items()))
    if tags:
        metadata["tags"] = sorted(tags)
    return metadata


def _annotations(
    snapshot: CatalogSnapshot,
    catalog_id: str,
    *,
    logical_kind: str,
    logical_name: str,
    extra: Mapping[str, str] | None = None,
) -> dict[str, str]:
    values = {
        "streamt.dev/catalog-id": catalog_id,
        "streamt.dev/project": snapshot.project_name,
        "streamt.dev/environment": snapshot.effective_environment,
        "streamt.dev/logical-kind": logical_kind,
        "streamt.dev/logical-name": logical_name,
    }
    if extra is not None:
        values.update(extra)
    return dict(sorted(values.items()))


def _resolved_owner(
    owner: CatalogOwnerLabel | None,
    inputs: _Inputs,
    *,
    location: str,
) -> str:
    if owner is None:
        return inputs.default_owner.canonical
    resolved = inputs.owner_map.get(owner.label)
    if resolved is None:
        raise BackstageExportError(
            "Declared catalog owner has no explicit mapping",
            location=location,
        )
    return resolved.canonical


def _resource_type(dataset: CatalogDataset) -> str:
    return "kafka-virtual-topic" if dataset.transport == "gateway" else "kafka-topic"


def _system_name(catalog_id: str, environment: str) -> str:
    return _generated_name(
        "system",
        catalog_id,
        ("streamt-backstage-v1", "system", catalog_id, environment),
    )


def _component_name(catalog_id: str, environment: str, model_name: str) -> str:
    return _generated_name(
        "model",
        model_name,
        (
            "streamt-backstage-v1",
            "component",
            catalog_id,
            environment,
            "model",
            model_name,
        ),
    )


def _resource_name(
    dataset: CatalogDataset,
    environment: str,
    inputs: _Inputs,
) -> str:
    resource_type = _resource_type(dataset)
    cluster = inputs.gateway_cluster if dataset.transport == "gateway" else inputs.kafka_cluster
    if cluster is None:
        raise BackstageExportError(
            "Catalog resource cluster reference is absent",
            location="resources",
        )
    prefix = "virtual-topic" if dataset.transport == "gateway" else "topic"
    return _generated_name(
        prefix,
        dataset.physical_name,
        (
            "streamt-backstage-v1",
            "resource",
            inputs.catalog_id,
            environment,
            resource_type,
            cluster.canonical,
            dataset.physical_name,
        ),
    )


def _generated_name(prefix: str, display_value: str, seed: Sequence[str]) -> str:
    digest = _digest16(seed)
    normalized = unicodedata.normalize("NFKD", display_value)
    ascii_value = normalized.encode("ascii", "ignore").decode("ascii").lower()
    stem = _NON_STEM_RE.sub("-", ascii_value).strip("-") or "item"
    available = 63 - len(prefix) - len(digest) - 2
    stem = stem[:available].rstrip("-") or "item"
    name = f"{prefix}-{stem}-{digest}"
    if len(name) > 63 or _ENTITY_NAME_RE.fullmatch(name) is None:
        raise BackstageExportError(
            "Generated Backstage entity name is invalid",
            location="entities.name",
        )
    return name


def _digest16(seed: Sequence[str]) -> str:
    payload = json.dumps(
        list(seed),
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()[:16]


def _generated_ref(kind: str, namespace: str, name: str) -> str:
    return f"{kind}:{namespace}/{name}"


def _entity_name(entity: Mapping[str, object]) -> str:
    metadata = cast(Mapping[str, object], entity["metadata"])
    return cast(str, metadata["name"])


def _entity_sort_key(entity: Mapping[str, object]) -> tuple[str, str, str]:
    metadata = cast(Mapping[str, object], entity["metadata"])
    namespace = cast(str, metadata["namespace"])
    name = cast(str, metadata["name"])
    return (namespace.casefold(), name.casefold(), name)


def _reference_sort_key(reference: str) -> tuple[str, str, str, str]:
    kind, remainder = reference.split(":", 1)
    namespace, name = remainder.split("/", 1)
    return (kind.casefold(), namespace.casefold(), name.casefold(), reference)


def _permitted_external_refs(inputs: _Inputs) -> frozenset[str]:
    values = {inputs.default_owner.canonical}
    values.update(reference.canonical for reference in inputs.owner_map.values())
    for reference in (inputs.kafka_cluster, inputs.gateway_cluster, inputs.domain):
        if reference is not None:
            values.add(reference.canonical)
    return frozenset(values)


def _validate_entities(
    entities: Sequence[Mapping[str, object]],
    inputs: _Inputs,
    external_refs: frozenset[str],
) -> None:
    generated_refs: set[str] = set()
    identities: set[tuple[str, str, str]] = set()
    kinds: list[str] = []
    for position, entity in enumerate(entities):
        location = f"entities/{position}"
        kind, namespace, name = _validate_entity_shape(entity, inputs, location)
        identity = (kind.casefold(), namespace.casefold(), name.casefold())
        if identity in identities:
            raise BackstageExportError(
                "Generated Backstage entity identities collide",
                location=f"{location}/metadata/name",
            )
        identities.add(identity)
        generated_refs.add(_generated_ref(kind.lower(), namespace, name))
        kinds.append(kind)

    expected_kind_order = sorted(
        kinds,
        key=lambda kind: {"System": 0, "Resource": 1, "Component": 2}[kind],
    )
    if kinds != expected_kind_order or kinds.count("System") != 1:
        raise BackstageExportError(
            "Generated Backstage entities are not in canonical kind order",
            location="entities",
        )
    for kind in ("Resource", "Component"):
        group = [entity for entity in entities if entity["kind"] == kind]
        if group != sorted(group, key=_entity_sort_key):
            raise BackstageExportError(
                "Generated Backstage entities are not canonically sorted",
                location="entities",
            )

    for position, entity in enumerate(entities):
        _validate_entity_references(
            entity,
            generated_refs=generated_refs,
            external_refs=external_refs,
            location=f"entities/{position}",
        )
        try:
            validate_backstage_entity(entity)
        except (BackstageResourceError, BackstageValidationError) as error:
            raise BackstageExportError(
                "Generated entity failed pinned Backstage schema validation",
                location=f"entities/{position}",
            ) from error


def _validate_entity_shape(
    entity: Mapping[str, object],
    inputs: _Inputs,
    location: str,
) -> tuple[BackstageKind, str, str]:
    if tuple(entity) != ("apiVersion", "kind", "metadata", "spec"):
        raise BackstageExportError(
            "Generated Backstage entity envelope is invalid",
            location=location,
        )
    if entity["apiVersion"] != _API_VERSION or entity["kind"] not in {
        "System",
        "Resource",
        "Component",
    }:
        raise BackstageExportError(
            "Generated Backstage entity identity is unsupported",
            location=location,
        )
    kind = cast(BackstageKind, entity["kind"])
    metadata = entity["metadata"]
    spec = entity["spec"]
    if not isinstance(metadata, dict) or not isinstance(spec, dict):
        raise BackstageExportError(
            "Generated Backstage entity containers are invalid",
            location=location,
        )
    _reject_null_or_empty(entity, location=location)
    expected_metadata_keys = ["name", "namespace", "title"]
    if "description" in metadata:
        expected_metadata_keys.append("description")
    expected_metadata_keys.append("annotations")
    if "tags" in metadata:
        expected_metadata_keys.append("tags")
    if list(metadata) != expected_metadata_keys:
        raise BackstageExportError(
            "Generated Backstage metadata fields are invalid or unordered",
            location=f"{location}/metadata",
        )
    name = metadata["name"]
    namespace = metadata["namespace"]
    if (
        not isinstance(name, str)
        or len(name) > 63
        or _ENTITY_NAME_RE.fullmatch(name) is None
        or namespace != inputs.namespace
    ):
        raise BackstageExportError(
            "Generated Backstage metadata identity is invalid",
            location=f"{location}/metadata",
        )
    _snapshot_text(metadata["title"], location=f"{location}/metadata/title")
    if "description" in metadata:
        _snapshot_description(
            metadata["description"],
            location=f"{location}/metadata/description",
        )
    _validate_entity_annotations(metadata["annotations"], kind, location)
    if "tags" in metadata:
        tags = metadata["tags"]
        if not isinstance(tags, list) or tags != sorted(tags) or len(tags) != len(set(tags)):
            raise BackstageExportError(
                "Generated Backstage tags are invalid or unordered",
                location=f"{location}/metadata/tags",
            )
        _snapshot_tags(tuple(tags), location=f"{location}/metadata/tags")
    _validate_spec_keys(kind, spec, location)
    return kind, cast(str, namespace), name


def _validate_entity_annotations(value: object, kind: BackstageKind, location: str) -> None:
    if not isinstance(value, dict) or list(value) != sorted(value):
        raise BackstageExportError(
            "Generated Backstage annotations are invalid or unordered",
            location=f"{location}/metadata/annotations",
        )
    if not _BASE_ANNOTATIONS.issubset(value) or not set(value).issubset(_ANNOTATIONS_BY_KIND[kind]):
        raise BackstageExportError(
            "Generated Backstage annotations are outside the allowlist",
            location=f"{location}/metadata/annotations",
        )
    if kind == "Resource" and "streamt.dev/physical-name" not in value:
        raise BackstageExportError(
            "Generated Resource is missing physical identity annotation",
            location=f"{location}/metadata/annotations",
        )
    if kind == "Component" and "streamt.dev/process-kind" not in value:
        raise BackstageExportError(
            "Generated Component is missing process kind annotation",
            location=f"{location}/metadata/annotations",
        )
    for annotation_value in value.values():
        if not isinstance(annotation_value, str) or _has_unsafe_text(annotation_value, False):
            raise BackstageExportError(
                "Generated Backstage annotation value is invalid",
                location=f"{location}/metadata/annotations",
            )


def _validate_spec_keys(
    kind: BackstageKind,
    spec: Mapping[str, object],
    location: str,
) -> None:
    keys = list(spec)
    if kind == "System":
        expected = ["owner"] + (["domain"] if "domain" in spec else [])
    elif kind == "Resource":
        expected = ["type", "owner"]
        if "system" in spec:
            expected.append("system")
        expected.append("dependsOn")
    else:
        expected = ["type", "lifecycle", "owner", "system"]
        if "dependsOn" in spec:
            expected.append("dependsOn")
    if keys != expected:
        raise BackstageExportError(
            "Generated Backstage spec fields are invalid or unordered",
            location=f"{location}/spec",
        )
    dependencies = spec.get("dependsOn")
    if dependencies is not None:
        if (
            not isinstance(dependencies, list)
            or dependencies != sorted(dependencies, key=_reference_sort_key)
            or len(dependencies) != len(set(dependencies))
        ):
            raise BackstageExportError(
                "Generated Backstage dependencies are invalid or unordered",
                location=f"{location}/spec/dependsOn",
            )


def _reject_null_or_empty(value: object, *, location: str) -> None:
    if value is None:
        raise BackstageExportError(
            "Generated Backstage entities cannot contain null values",
            location=location,
        )
    if isinstance(value, Mapping):
        if not value:
            raise BackstageExportError(
                "Generated Backstage entities cannot contain empty objects",
                location=location,
            )
        for key, child in value.items():
            _reject_null_or_empty(child, location=f"{location}/{key}")
    elif isinstance(value, list):
        if not value:
            raise BackstageExportError(
                "Generated Backstage entities cannot contain empty arrays",
                location=location,
            )
        for position, child in enumerate(value):
            _reject_null_or_empty(child, location=f"{location}/{position}")


def _validate_entity_references(
    entity: Mapping[str, object],
    *,
    generated_refs: set[str],
    external_refs: frozenset[str],
    location: str,
) -> None:
    spec = cast(Mapping[str, object], entity["spec"])
    references: list[tuple[str, object]] = [("owner", spec["owner"])]
    for field in ("domain", "system"):
        if field in spec:
            references.append((field, spec[field]))
    for reference in cast(Sequence[object], spec.get("dependsOn", ())):
        references.append(("dependsOn", reference))
    for field, value in references:
        allowed_kinds = {
            "owner": frozenset({"group", "user"}),
            "domain": frozenset({"domain"}),
            "system": frozenset({"system"}),
            "dependsOn": frozenset({"resource", "component"}),
        }[field]
        try:
            parsed = require_entity_ref(
                value,
                allowed_kinds=allowed_kinds,
                location=f"{location}/spec/{field}",
            )
        except CatalogInputError as error:
            raise BackstageExportError(str(error), location=error.location) from error
        if parsed.canonical not in generated_refs and parsed.canonical not in external_refs:
            raise BackstageExportError(
                "Generated Backstage reference is neither internal nor permitted external metadata",
                location=f"{location}/spec/{field}",
            )


def _warnings(
    snapshot: CatalogSnapshot,
    index: _SnapshotIndex,
) -> tuple[BackstageExportWarning, ...]:
    warnings = [
        BackstageExportWarning(
            code="W113_BACKSTAGE_SINK_OUTPUT_OMITTED",
            message="Connector destination metadata is omitted from Backstage export",
            location=f"models/{process.logical_name}",
        )
        for process in index.processes.values()
        if process.process_kind == "connect"
    ]
    warnings.extend(
        BackstageExportWarning(
            code="W114_BACKSTAGE_EXPOSURE_OMITTED",
            message="Exposure metadata is omitted from Backstage export",
            location=f"exposures/{name}",
        )
        for name in snapshot.omitted_exposure_names
    )
    return tuple(sorted(warnings, key=lambda warning: (warning.location, warning.code)))


def _serialize_entities(entities: Sequence[Mapping[str, object]]) -> str:
    try:
        rendered = yaml.dump_all(
            list(entities),
            Dumper=_CanonicalDumper,
            allow_unicode=True,
            default_flow_style=False,
            explicit_start=True,
            explicit_end=False,
            sort_keys=False,
            width=4_096,
        )
    except yaml.YAMLError as error:
        raise BackstageExportError(
            "Validated Backstage entities could not be serialized",
            location="entities",
        ) from error
    rendered = rendered.replace("\r\n", "\n").replace("\r", "\n")
    if not rendered.endswith("\n"):
        rendered += "\n"
    if "\n...\n" in rendered or not all(
        line == "---" for line in rendered.splitlines() if line.startswith("---")
    ):
        raise BackstageExportError(
            "Backstage YAML document boundaries are not canonical",
            location="entities",
        )
    return rendered
