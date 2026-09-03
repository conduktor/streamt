"""Private, immutable catalog projection from one successful compile."""

from __future__ import annotations

import unicodedata
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Literal

from streamt.compiler.compiled_models import CompiledModelView
from streamt.compiler.model_resolution import ModelDependency, ResolvedModel
from streamt.core.models import (
    Exposure,
    MaterializedType,
    Model,
    ProjectInfo,
    Source,
    StreamtProject,
)

DatasetLogicalKind = Literal["source", "model"]
DatasetTransport = Literal["kafka", "gateway"]
CatalogProcessKind = Literal["flink", "gateway", "connect"]
ContractStatus = Literal["declared", "enforced"]
LogicalDataset = tuple[DatasetLogicalKind, str]


class CatalogProjectionError(ValueError):
    """A secret-neutral projection failure with one safe structural location."""

    def __init__(self, message: str, *, location: str) -> None:
        super().__init__(message)
        self.location = location


@dataclass(frozen=True)
class CatalogOwnerLabel:
    """One exact human owner label awaiting adapter-specific resolution."""

    label: str

    def __post_init__(self) -> None:
        _require_single_line_text(
            self.label,
            label="Owner label",
            location="owner.label",
        )


@dataclass(frozen=True)
class CatalogContractSummary:
    """The only contract fact portable into the first catalog projection."""

    status: ContractStatus

    def __post_init__(self) -> None:
        if self.status not in {"declared", "enforced"}:
            raise CatalogProjectionError(
                "Contract summary status is unsupported",
                location="contract.status",
            )


@dataclass(frozen=True, order=True)
class CatalogDependency:
    """One exact direct logical dataset dependency."""

    logical_kind: DatasetLogicalKind
    logical_name: str

    def __post_init__(self) -> None:
        if self.logical_kind not in {"source", "model"}:
            raise CatalogProjectionError(
                "Catalog dependency kind is unsupported",
                location="dependency.logical_kind",
            )
        _require_single_line_text(
            self.logical_name,
            label="Dependency logical name",
            location="dependency.logical_name",
        )

    @property
    def logical_identity(self) -> LogicalDataset:
        """Return the dependency's typed logical identity."""
        return (self.logical_kind, self.logical_name)


@dataclass(frozen=True)
class CatalogDataset:
    """One source or compiled model-output dataset."""

    logical_kind: DatasetLogicalKind
    logical_name: str
    transport: DatasetTransport
    physical_name: str
    description: str | None
    tags: tuple[str, ...]
    owner: CatalogOwnerLabel | None
    contract: CatalogContractSummary | None

    def __post_init__(self) -> None:
        if self.logical_kind not in {"source", "model"}:
            raise CatalogProjectionError(
                "Catalog dataset kind is unsupported",
                location="dataset.logical_kind",
            )
        if self.transport not in {"kafka", "gateway"}:
            raise CatalogProjectionError(
                "Catalog dataset transport is unsupported",
                location="dataset.transport",
            )
        _require_single_line_text(
            self.logical_name,
            label="Dataset logical name",
            location="dataset.logical_name",
        )
        _require_single_line_text(
            self.physical_name,
            label="Dataset physical name",
            location="dataset.physical_name",
        )
        _require_optional_description(self.description, location="dataset.description")
        _require_frozen_tags(self.tags, location="dataset.tags")
        if self.owner is not None and not isinstance(self.owner, CatalogOwnerLabel):
            raise CatalogProjectionError(
                "Catalog dataset owner must be an owner label",
                location="dataset.owner",
            )
        if self.contract is not None and not isinstance(
            self.contract, CatalogContractSummary
        ):
            raise CatalogProjectionError(
                "Catalog dataset contract must be a contract summary",
                location="dataset.contract",
            )
        if self.logical_kind == "source" and self.contract is not None:
            raise CatalogProjectionError(
                "Source datasets cannot carry model contract summaries",
                location="dataset.contract",
            )

    @property
    def logical_identity(self) -> LogicalDataset:
        """Return the dataset's typed logical identity."""
        return (self.logical_kind, self.logical_name)

    @property
    def physical_identity(self) -> tuple[DatasetTransport, str]:
        """Return the exact provider-independent physical identity."""
        return (self.transport, self.physical_name)


@dataclass(frozen=True)
class CatalogProcess:
    """One process shape that the compiler actually produced for a model."""

    logical_name: str
    process_kind: CatalogProcessKind
    description: str | None
    tags: tuple[str, ...]
    owner: CatalogOwnerLabel | None
    dependencies: tuple[CatalogDependency, ...]

    def __post_init__(self) -> None:
        _require_single_line_text(
            self.logical_name,
            label="Process logical name",
            location="process.logical_name",
        )
        if self.process_kind not in {"flink", "gateway", "connect"}:
            raise CatalogProjectionError(
                "Catalog process kind is unsupported",
                location="process.process_kind",
            )
        _require_optional_description(self.description, location="process.description")
        _require_frozen_tags(self.tags, location="process.tags")
        if self.owner is not None and not isinstance(self.owner, CatalogOwnerLabel):
            raise CatalogProjectionError(
                "Catalog process owner must be an owner label",
                location="process.owner",
            )
        if not isinstance(self.dependencies, tuple) or not all(
            isinstance(dependency, CatalogDependency)
            for dependency in self.dependencies
        ):
            raise CatalogProjectionError(
                "Catalog process dependencies must be an immutable tuple",
                location="process.dependencies",
            )
        if len(set(self.dependencies)) != len(self.dependencies):
            raise CatalogProjectionError(
                "Catalog process dependencies must be unique",
                location="process.dependencies",
            )


@dataclass(frozen=True)
class CatalogSnapshot:
    """Versioned, Backstage-free catalog facts from one compile."""

    project_name: str
    project_description: str | None
    effective_environment: str
    datasets: tuple[CatalogDataset, ...]
    processes: tuple[CatalogProcess, ...]
    omitted_exposure_names: tuple[str, ...]
    version: Literal[1] = 1

    def __post_init__(self) -> None:
        if self.version != 1:
            raise CatalogProjectionError(
                "Catalog snapshot version is unsupported",
                location="snapshot.version",
            )
        _require_single_line_text(
            self.project_name,
            label="Project name",
            location="snapshot.project_name",
        )
        _require_optional_description(
            self.project_description,
            location="snapshot.project_description",
        )
        _require_single_line_text(
            self.effective_environment,
            label="Effective environment",
            location="snapshot.effective_environment",
        )
        if not isinstance(self.datasets, tuple) or not all(
            isinstance(dataset, CatalogDataset) for dataset in self.datasets
        ):
            raise CatalogProjectionError(
                "Catalog datasets must be an immutable tuple",
                location="snapshot.datasets",
            )
        if not isinstance(self.processes, tuple) or not all(
            isinstance(process, CatalogProcess) for process in self.processes
        ):
            raise CatalogProjectionError(
                "Catalog processes must be an immutable tuple",
                location="snapshot.processes",
            )
        if not isinstance(self.omitted_exposure_names, tuple):
            raise CatalogProjectionError(
                "Omitted exposure names must be an immutable tuple",
                location="snapshot.omitted_exposure_names",
            )
        for index, name in enumerate(self.omitted_exposure_names):
            _require_single_line_text(
                name,
                label="Exposure name",
                location=f"snapshot.omitted_exposure_names/{index}",
            )


@dataclass(frozen=True)
class _Declaration:
    """Validated allowlisted declaration metadata used during construction."""

    logical_name: str
    description: str | None
    tags: tuple[str, ...]
    owner: CatalogOwnerLabel | None
    contract: CatalogContractSummary | None
    location: str


def build_catalog_snapshot(
    project: StreamtProject,
    resolved_models: Mapping[str, ResolvedModel],
    compiled_models: Mapping[str, CompiledModelView],
    effective_environment: str,
) -> CatalogSnapshot:
    """Build a defensive neutral snapshot from one successful compiler result."""
    if not isinstance(project, StreamtProject):
        raise CatalogProjectionError(
            "Catalog projection requires a parsed streamt project",
            location="project",
        )
    if not isinstance(resolved_models, Mapping):
        raise CatalogProjectionError(
            "Resolved models must be a mapping",
            location="resolved_models",
        )
    if not isinstance(compiled_models, Mapping):
        raise CatalogProjectionError(
            "Compiled models must be a mapping",
            location="compiled_models",
        )
    if not isinstance(getattr(project, "project", None), ProjectInfo):
        raise CatalogProjectionError(
            "Project metadata has an invalid type",
            location="/project",
        )
    for field_name in ("sources", "models", "exposures"):
        if not isinstance(getattr(project, field_name, None), list):
            raise CatalogProjectionError(
                "Project declarations must be lists",
                location=f"/{field_name}",
            )

    project_name = _require_single_line_text(
        project.project.name,
        label="Project name",
        location="/project/name",
    )
    project_description = _require_optional_description(
        project.project.description,
        location="/project/description",
    )
    environment = _require_single_line_text(
        effective_environment,
        label="Effective environment",
        location="effective_environment",
    )

    sources = _source_declarations(project)
    models = _model_declarations(project)
    source_names = set(sources)
    model_names = set(models)
    if source_names & model_names:
        raise CatalogProjectionError(
            "A logical name is shared by a source and a model",
            location="/models",
        )

    _require_exact_model_keys(model_names, resolved_models, location="resolved_models")
    _require_exact_model_keys(model_names, compiled_models, location="compiled_models")
    _validate_model_views(models, resolved_models, compiled_models)

    datasets: dict[LogicalDataset, CatalogDataset] = {}
    physical_identities: dict[tuple[DatasetTransport, str], LogicalDataset] = {}
    for index, source in enumerate(project.sources):
        declaration = sources[source.name]
        physical_name = _require_single_line_text(
            source.topic,
            label="Source physical topic",
            location=f"/sources/{index}/topic",
        )
        dataset = CatalogDataset(
            logical_kind="source",
            logical_name=declaration.logical_name,
            transport="kafka",
            physical_name=physical_name,
            description=declaration.description,
            tags=declaration.tags,
            owner=declaration.owner,
            contract=None,
        )
        _register_dataset(datasets, physical_identities, dataset, declaration.location)

    for model_name, declaration in models.items():
        compiled = compiled_models[model_name]
        if compiled.output_kind is None:
            continue
        output_name = _require_single_line_text(
            compiled.output_name,
            label="Compiled model output name",
            location=f"{declaration.location}/output",
        )
        dataset = CatalogDataset(
            logical_kind="model",
            logical_name=model_name,
            transport=compiled.output_kind,
            physical_name=output_name,
            description=declaration.description,
            tags=declaration.tags,
            owner=declaration.owner,
            contract=declaration.contract,
        )
        _register_dataset(datasets, physical_identities, dataset, declaration.location)

    dependency_graph = _validate_dependencies(models, resolved_models, datasets)
    processes: list[CatalogProcess] = []
    for model_name, declaration in models.items():
        compiled = compiled_models[model_name]
        dependencies = dependency_graph[model_name]
        _reconcile_compiled_inputs(
            compiled,
            dependencies,
            datasets,
            location=declaration.location,
        )
        if compiled.process_kind is None:
            continue
        processes.append(
            CatalogProcess(
                logical_name=model_name,
                process_kind=compiled.process_kind,
                description=declaration.description,
                tags=declaration.tags,
                owner=declaration.owner,
                dependencies=dependencies,
            )
        )

    exposure_names = _omitted_exposure_names(project)
    return CatalogSnapshot(
        project_name=project_name,
        project_description=project_description,
        effective_environment=environment,
        datasets=tuple(
            sorted(
                datasets.values(),
                key=lambda item: (
                    0 if item.logical_kind == "source" else 1,
                    item.logical_name,
                ),
            )
        ),
        processes=tuple(sorted(processes, key=lambda item: item.logical_name)),
        omitted_exposure_names=exposure_names,
    )


def _source_declarations(project: StreamtProject) -> dict[str, _Declaration]:
    declarations: dict[str, _Declaration] = {}
    for index, source in enumerate(project.sources):
        location = f"/sources/{index}"
        if not isinstance(source, Source):
            raise CatalogProjectionError(
                "Source declaration has an invalid type",
                location=location,
            )
        name = _require_single_line_text(
            source.name,
            label="Source name",
            location=f"{location}/name",
        )
        if name in declarations:
            raise CatalogProjectionError(
                "Source logical names must be unique",
                location=f"{location}/name",
            )
        declarations[name] = _declaration(
            name=name,
            description=source.description,
            tags=source.tags,
            owner=source.owner,
            contract=None,
            location=location,
        )
    return declarations


def _model_declarations(project: StreamtProject) -> dict[str, _Declaration]:
    declarations: dict[str, _Declaration] = {}
    for index, model in enumerate(project.models):
        location = f"/models/{index}"
        if not isinstance(model, Model):
            raise CatalogProjectionError(
                "Model declaration has an invalid type",
                location=location,
            )
        name = _require_single_line_text(
            model.name,
            label="Model name",
            location=f"{location}/name",
        )
        if name in declarations:
            raise CatalogProjectionError(
                "Model logical names must be unique",
                location=f"{location}/name",
            )
        contract = None
        if model.contract is not None:
            enforced = model.contract.enforced
            if not isinstance(enforced, bool):
                raise CatalogProjectionError(
                    "Model contract enforcement must be boolean",
                    location=f"{location}/contract/enforced",
                )
            contract = CatalogContractSummary(
                status="enforced" if enforced else "declared"
            )
        declarations[name] = _declaration(
            name=name,
            description=model.description,
            tags=model.tags,
            owner=model.owner,
            contract=contract,
            location=location,
        )
    return declarations


def _declaration(
    *,
    name: str,
    description: object,
    tags: object,
    owner: object,
    contract: CatalogContractSummary | None,
    location: str,
) -> _Declaration:
    return _Declaration(
        logical_name=name,
        description=_require_optional_description(
            description,
            location=f"{location}/description",
        ),
        tags=_copy_tags(tags, location=f"{location}/tags"),
        owner=(
            CatalogOwnerLabel(
                _require_single_line_text(
                    owner,
                    label="Declared owner",
                    location=f"{location}/owner",
                )
            )
            if owner is not None
            else None
        ),
        contract=contract,
        location=location,
    )


def _validate_model_views(
    declarations: Mapping[str, _Declaration],
    resolved_models: Mapping[str, ResolvedModel],
    compiled_models: Mapping[str, CompiledModelView],
) -> None:
    for model_name, declaration in declarations.items():
        resolved = resolved_models[model_name]
        compiled = compiled_models[model_name]
        location = declaration.location
        if not isinstance(resolved, ResolvedModel):
            raise CatalogProjectionError(
                "Resolved model has an invalid type",
                location=f"{location}/resolved",
            )
        if not isinstance(resolved.model, Model) or resolved.model.name != model_name:
            raise CatalogProjectionError(
                "Resolved model identity does not match its mapping key",
                location=f"{location}/name",
            )
        if not isinstance(compiled, CompiledModelView) or compiled.model_name != model_name:
            raise CatalogProjectionError(
                "Compiled model identity does not match its mapping key",
                location=f"{location}/name",
            )
        if not isinstance(resolved.materialized, MaterializedType):
            raise CatalogProjectionError(
                "Resolved model materialization is unsupported",
                location=f"{location}/materialized",
            )
        if compiled.materialized != resolved.materialized:
            raise CatalogProjectionError(
                "Resolved and compiled model materializations do not match",
                location=f"{location}/materialized",
            )
        _validate_resolved_metadata(declaration, resolved.model)
        _validate_compiled_shape(compiled, location)


def _validate_resolved_metadata(declaration: _Declaration, model: Model) -> None:
    """Reject mixed project/resolution snapshots without observing private fields."""
    resolved_description = _require_optional_description(
        model.description,
        location=f"{declaration.location}/resolved/description",
    )
    resolved_tags = _copy_tags(
        model.tags,
        location=f"{declaration.location}/resolved/tags",
    )
    resolved_owner = (
        _require_single_line_text(
            model.owner,
            label="Resolved owner",
            location=f"{declaration.location}/resolved/owner",
        )
        if model.owner is not None
        else None
    )
    resolved_contract: ContractStatus | None = None
    if model.contract is not None:
        if not isinstance(model.contract.enforced, bool):
            raise CatalogProjectionError(
                "Resolved contract enforcement must be boolean",
                location=f"{declaration.location}/contract/enforced",
            )
        resolved_contract = "enforced" if model.contract.enforced else "declared"
    expected_contract = (
        declaration.contract.status if declaration.contract is not None else None
    )
    if (
        resolved_description != declaration.description
        or resolved_tags != declaration.tags
        or resolved_owner
        != (declaration.owner.label if declaration.owner is not None else None)
        or resolved_contract != expected_contract
    ):
        raise CatalogProjectionError(
            "Resolved model metadata does not match the project declaration",
            location=f"{declaration.location}/resolved",
        )


def _validate_compiled_shape(compiled: CompiledModelView, location: str) -> None:
    materialized = compiled.materialized
    shape = (compiled.process_kind, compiled.output_kind)
    valid_shapes: dict[MaterializedType, set[tuple[object, object]]] = {
        MaterializedType.TOPIC: {(None, "kafka"), ("flink", "kafka")},
        MaterializedType.FLINK: {("flink", "kafka")},
        MaterializedType.VIRTUAL_TOPIC: {("gateway", "gateway")},
        MaterializedType.SINK: {("connect", None)},
    }
    if materialized not in valid_shapes or shape not in valid_shapes[materialized]:
        raise CatalogProjectionError(
            "Compiled process/output shape does not match its materialization",
            location=f"{location}/materialized",
        )

    if compiled.output_kind is None:
        if compiled.output_name is not None:
            raise CatalogProjectionError(
                "Compiled model without output kind has an output name",
                location=f"{location}/output",
            )
    else:
        _require_single_line_text(
            compiled.output_name,
            label="Compiled model output name",
            location=f"{location}/output",
        )

    if compiled.process_kind == "gateway":
        _require_single_line_text(
            compiled.gateway_physical_input,
            label="Compiled Gateway physical input",
            location=f"{location}/gateway/physical_input",
        )
    elif compiled.gateway_physical_input is not None:
        raise CatalogProjectionError(
            "Non-Gateway model has a Gateway physical input",
            location=f"{location}/gateway/physical_input",
        )

    connector_inputs = compiled.connector_inputs
    if not isinstance(connector_inputs, tuple):
        raise CatalogProjectionError(
            "Compiled connector inputs must be an immutable tuple",
            location=f"{location}/connector_inputs",
        )
    for index, name in enumerate(connector_inputs):
        _require_single_line_text(
            name,
            label="Compiled connector input",
            location=f"{location}/connector_inputs/{index}",
        )
    if compiled.process_kind == "connect":
        if not connector_inputs:
            raise CatalogProjectionError(
                "Compiled Connect process must have an input",
                location=f"{location}/connector_inputs",
            )
    elif connector_inputs:
        raise CatalogProjectionError(
            "Non-Connect model has connector inputs",
            location=f"{location}/connector_inputs",
        )


def _validate_dependencies(
    declarations: Mapping[str, _Declaration],
    resolved_models: Mapping[str, ResolvedModel],
    datasets: Mapping[LogicalDataset, CatalogDataset],
) -> dict[str, tuple[CatalogDependency, ...]]:
    graph: dict[str, tuple[CatalogDependency, ...]] = {}
    for model_name, declaration in declarations.items():
        raw = resolved_models[model_name].dependencies
        if not isinstance(raw, tuple):
            raise CatalogProjectionError(
                "Resolved dependencies must be an immutable tuple",
                location=f"{declaration.location}/dependencies",
            )
        dependencies: list[CatalogDependency] = []
        seen: set[LogicalDataset] = set()
        for index, dependency in enumerate(raw):
            dep_location = f"{declaration.location}/dependencies/{index}"
            if not isinstance(dependency, ModelDependency):
                raise CatalogProjectionError(
                    "Resolved dependency has an invalid type",
                    location=dep_location,
                )
            if dependency.kind not in {"source", "model"}:
                raise CatalogProjectionError(
                    "Resolved dependency kind is unsupported",
                    location=f"{dep_location}/kind",
                )
            dep_name = _require_single_line_text(
                dependency.name,
                label="Resolved dependency name",
                location=f"{dep_location}/name",
            )
            logical: LogicalDataset = (dependency.kind, dep_name)
            if logical in seen:
                raise CatalogProjectionError(
                    "Resolved direct dependencies must be unique",
                    location=dep_location,
                )
            seen.add(logical)
            if logical == ("model", model_name):
                raise CatalogProjectionError(
                    "A model cannot depend on itself",
                    location=dep_location,
                )
            if logical not in datasets:
                raise CatalogProjectionError(
                    "A direct dependency has no catalog dataset",
                    location=dep_location,
                )
            dependencies.append(
                CatalogDependency(logical_kind=dependency.kind, logical_name=dep_name)
            )
        graph[model_name] = tuple(dependencies)
    _validate_dependency_cycles(graph, declarations)
    return graph


def _validate_dependency_cycles(
    graph: Mapping[str, tuple[CatalogDependency, ...]],
    declarations: Mapping[str, _Declaration],
) -> None:
    state: dict[str, int] = {}

    def visit(model_name: str) -> None:
        marker = state.get(model_name, 0)
        if marker == 2:
            return
        if marker == 1:
            raise CatalogProjectionError(
                "Catalog model dependencies contain a cycle",
                location=f"{declarations[model_name].location}/dependencies",
            )
        state[model_name] = 1
        for dependency in graph[model_name]:
            if dependency.logical_kind == "model":
                visit(dependency.logical_name)
        state[model_name] = 2

    for name in declarations:
        visit(name)


def _reconcile_compiled_inputs(
    compiled: CompiledModelView,
    dependencies: tuple[CatalogDependency, ...],
    datasets: Mapping[LogicalDataset, CatalogDataset],
    *,
    location: str,
) -> None:
    physical_names = tuple(
        datasets[dependency.logical_identity].physical_name
        for dependency in dependencies
    )
    if compiled.process_kind == "gateway":
        if len(physical_names) != 1:
            raise CatalogProjectionError(
                "Compiled Gateway process must have exactly one direct dependency",
                location=f"{location}/dependencies",
            )
        if compiled.gateway_physical_input != physical_names[0]:
            raise CatalogProjectionError(
                "Compiled Gateway input does not match its direct dependency",
                location=f"{location}/gateway/physical_input",
            )
    if compiled.process_kind == "connect" and compiled.connector_inputs != physical_names:
        raise CatalogProjectionError(
            "Compiled Connect inputs do not match direct dependencies",
            location=f"{location}/connector_inputs",
        )


def _register_dataset(
    datasets: dict[LogicalDataset, CatalogDataset],
    physical_identities: dict[tuple[DatasetTransport, str], LogicalDataset],
    dataset: CatalogDataset,
    location: str,
) -> None:
    logical = dataset.logical_identity
    if logical in datasets:
        raise CatalogProjectionError(
            "Catalog dataset logical identity is duplicated",
            location=location,
        )
    physical = dataset.physical_identity
    if physical in physical_identities:
        raise CatalogProjectionError(
            "Two logical declarations resolve to one physical dataset",
            location=f"{location}/output",
        )
    datasets[logical] = dataset
    physical_identities[physical] = logical


def _require_exact_model_keys(
    expected: set[str],
    actual_mapping: Mapping[str, object],
    *,
    location: str,
) -> None:
    keys = tuple(actual_mapping.keys())
    if any(not isinstance(key, str) for key in keys):
        raise CatalogProjectionError(
            "Compiled model mappings must use string keys",
            location=location,
        )
    actual = set(keys)
    if actual != expected:
        raise CatalogProjectionError(
            "Compiled model mapping does not exactly cover declarations",
            location=location,
        )


def _omitted_exposure_names(project: StreamtProject) -> tuple[str, ...]:
    names: list[str] = []
    for index, exposure in enumerate(project.exposures):
        location = f"/exposures/{index}/name"
        if not isinstance(exposure, Exposure):
            raise CatalogProjectionError(
                "Exposure declaration has an invalid type",
                location=f"/exposures/{index}",
            )
        name = _require_single_line_text(
            exposure.name,
            label="Exposure name",
            location=location,
        )
        names.append(name)
    return tuple(sorted(names))


def _copy_tags(value: object, *, location: str) -> tuple[str, ...]:
    if isinstance(value, (str, bytes)) or not isinstance(value, Sequence):
        raise CatalogProjectionError(
            "Declared tags must be a sequence of strings",
            location=location,
        )
    tags: list[str] = []
    seen: set[str] = set()
    for index, raw_tag in enumerate(value):
        tag = _require_single_line_text(
            raw_tag,
            label="Declared tag",
            location=f"{location}/{index}",
        )
        if tag in seen:
            raise CatalogProjectionError(
                "Declared tags must be unique",
                location=f"{location}/{index}",
            )
        seen.add(tag)
        tags.append(tag)
    return tuple(tags)


def _require_frozen_tags(value: object, *, location: str) -> tuple[str, ...]:
    if not isinstance(value, tuple):
        raise CatalogProjectionError(
            "Catalog tags must be an immutable tuple",
            location=location,
        )
    return _copy_tags(value, location=location)


def _require_optional_description(value: object, *, location: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise CatalogProjectionError(
            "Declared description must be a string when present",
            location=location,
        )
    if _has_unsafe_text(value, allow_layout=True):
        raise CatalogProjectionError(
            "Declared description contains unsupported control characters",
            location=location,
        )
    return value if value.strip() else None


def _require_single_line_text(value: object, *, label: str, location: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise CatalogProjectionError(
            f"{label} must be a non-blank string",
            location=location,
        )
    if _has_unsafe_text(value, allow_layout=False):
        raise CatalogProjectionError(
            f"{label} contains unsupported control characters",
            location=location,
        )
    return value


def _has_unsafe_text(value: str, *, allow_layout: bool) -> bool:
    allowed_controls = {"\t", "\n", "\r"} if allow_layout else set()
    return any(
        character not in allowed_controls
        and unicodedata.category(character) in {"Cc", "Cs"}
        for character in value
    )
