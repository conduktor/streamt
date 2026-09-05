"""Deterministic static OpenLineage mapping from one successful dry-run compile."""

from __future__ import annotations

import json
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Literal, Protocol

from streamt.compiler.compiled_models import CompiledModelView
from streamt.compiler.manifest import Manifest
from streamt.compiler.model_resolution import ModelDependency, ResolvedModel
from streamt.core.errors import ErrorCode
from streamt.core.models import MaterializedType, Model, Source, StreamtProject
from streamt.integrations.openlineage.events import (
    DatasetIdentity,
    JobIdentity,
    OpenLineageConstructionError,
    build_dataset_event,
    build_job_event,
    model_job_name,
    standard_facet,
    validate_kafka_namespace,
)
from streamt.integrations.openlineage.validation import (
    OpenLineageValidationError,
    event_kind,
    validate_event,
    validate_event_sequence,
)

LogicalDataset = tuple[Literal["source", "model"], str]
_STATIC_UTC_EVENT_TIME_RE = re.compile(
    r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|\+00:00)$"
)


class _DeclaredColumn(Protocol):
    """Column attributes common to source/model and contract declarations."""

    name: str
    type: str | None
    description: str | None


class OpenLineageStaticError(ValueError):
    """A safe static mapping failure with a stable project or output location."""

    def __init__(self, message: str, *, location: str) -> None:
        super().__init__(message)
        self.location = location


@dataclass(frozen=True, order=True)
class OpenLineageExportWarning:
    """One deterministic, safe warning from incomplete static metadata."""

    code: str
    message: str
    location: str


@dataclass(frozen=True)
class StaticNamespaceRequirements:
    """Dataset namespace kinds required by one compiled project."""

    kafka: bool
    gateway: bool


@dataclass(frozen=True)
class StaticOpenLineageExport:
    """Canonical validated static events and their mapping warnings."""

    events: tuple[dict[str, object], ...]
    warnings: tuple[OpenLineageExportWarning, ...]
    dataset_count: int
    job_count: int


@dataclass(frozen=True)
class _DatasetDeclaration:
    logical: LogicalDataset
    identity: DatasetIdentity
    declaration: Source | Model
    location: str


@dataclass(frozen=True)
class _StaticContext:
    project: StreamtProject
    manifest: Manifest
    resolved_models: Mapping[str, ResolvedModel]
    compiled_models: Mapping[str, CompiledModelView]
    job_namespace: str
    kafka_namespace: str | None
    gateway_namespace: str | None


def static_namespace_requirements(
    project: StreamtProject,
    compiled_models: Mapping[str, CompiledModelView],
) -> StaticNamespaceRequirements:
    """Return namespace needs without reading runtime configuration."""
    kafka = bool(project.sources)
    gateway = False
    for compiled in compiled_models.values():
        if compiled.output_kind == "kafka":
            kafka = True
        elif compiled.output_kind == "gateway":
            gateway = True
            # Every Gateway job has a physical backing input on ordinary Kafka.
            kafka = True
    return StaticNamespaceRequirements(kafka=kafka, gateway=gateway)


def build_static_export(
    project: StreamtProject,
    manifest: Manifest,
    resolved_models: Mapping[str, ResolvedModel],
    compiled_models: Mapping[str, CompiledModelView],
    *,
    job_namespace: str,
    kafka_namespace: str | None,
    gateway_namespace: str | None,
) -> StaticOpenLineageExport:
    """Map one successful compile to canonical static DatasetEvent and JobEvent records."""
    context = _StaticContext(
        project=project,
        manifest=manifest,
        resolved_models=resolved_models,
        compiled_models=compiled_models,
        job_namespace=job_namespace,
        kafka_namespace=kafka_namespace,
        gateway_namespace=gateway_namespace,
    )
    try:
        return _build_static_export(context)
    except OpenLineageStaticError:
        raise
    except (OpenLineageConstructionError, OpenLineageValidationError) as error:
        raise OpenLineageStaticError(
            f"Generated OpenLineage metadata is invalid: {error}",
            location="events",
        ) from error


def serialize_static_jsonl(events: Sequence[Mapping[str, object]]) -> str:
    """Validate and serialize canonical OpenLineage events as compact UTF-8 JSONL."""
    rendered: list[tuple[tuple[int, str, str], str]] = []
    for index, event in enumerate(events):
        try:
            validate_event(event)
            kind = event_kind(event)
        except OpenLineageValidationError as error:
            raise OpenLineageStaticError(
                f"Could not serialize invalid OpenLineage event {index}",
                location=f"/events/{index}",
            ) from error
        if kind == "run":
            raise OpenLineageStaticError(
                "Static OpenLineage JSONL cannot contain RunEvent records",
                location=f"/events/{index}",
            )

        entity_key = "dataset" if kind == "dataset" else "job"
        entity = _event_entity(event, entity_key)
        sort_key = (
            0 if kind == "dataset" else 1,
            str(entity["namespace"]),
            str(entity["name"]),
        )
        try:
            encoded = json.dumps(
                event,
                ensure_ascii=False,
                allow_nan=False,
                sort_keys=True,
                separators=(",", ":"),
            )
        except (TypeError, ValueError) as error:
            raise OpenLineageStaticError(
                f"Could not serialize validated OpenLineage event {index}",
                location=f"/events/{index}",
            ) from error
        rendered.append((sort_key, encoded))

    try:
        validate_event_sequence(events)
    except OpenLineageValidationError as error:
        raise OpenLineageStaticError(
            "Could not serialize an invalid OpenLineage event sequence",
            location="/events",
        ) from error

    rendered.sort(key=lambda item: item[0])
    lines = [encoded for _sort_key, encoded in rendered]
    return "\n".join(lines) + ("\n" if lines else "")


def _build_static_export(context: _StaticContext) -> StaticOpenLineageExport:
    _validate_export_identity(context)
    event_time = _require_static_event_time(context.manifest.compiled_at)
    declarations, model_locations = _validate_compile_views(context)
    datasets = _build_dataset_inventory(context, declarations)
    dataset_events: list[dict[str, object]] = []
    warnings: list[OpenLineageExportWarning] = []

    for dataset_record in sorted(
        datasets.values(), key=lambda candidate: candidate.identity
    ):
        facets, schema_warning = _dataset_facets(dataset_record)
        if schema_warning is not None:
            warnings.append(schema_warning)
        dataset_events.append(
            build_dataset_event(
                event_time=event_time,
                dataset=dataset_record.identity,
                facets=facets,
            )
        )

    job_events: list[dict[str, object]] = []
    job_identities: dict[JobIdentity, str] = {}
    for model_name in sorted(context.compiled_models):
        compiled = context.compiled_models[model_name]
        if compiled.process_kind is None:
            continue
        model_declaration = declarations[model_name]
        location = model_locations[model_name]
        job = JobIdentity(
            context.job_namespace,
            model_job_name(context.project.project.name, model_name),
        )
        previous = job_identities.get(job)
        if previous is not None and previous != model_name:
            raise OpenLineageStaticError(
                "Two model declarations resolve to the same OpenLineage job identity",
                location=location,
            )
        job_identities[job] = model_name

        inputs = _job_inputs(context, model_name, compiled, datasets, location)
        output = datasets.get(("model", model_name))
        outputs: tuple[DatasetIdentity, ...] = ()
        if compiled.process_kind in {"flink", "gateway", "kafka_streams"}:
            if output is None:
                raise OpenLineageStaticError(
                    "Compiled processing model has no output dataset",
                    location=f"{location}/output",
                )
            outputs = (output.identity,)
        else:
            warnings.append(
                OpenLineageExportWarning(
                    code=ErrorCode.OPENLINEAGE_SINK_OUTPUT_OMITTED,
                    message=(
                        f"Sink model '{model_name}' has no normalized OpenLineage "
                        "output dataset"
                    ),
                    location=f"{location}/sink",
                )
            )

        if set(inputs) & set(outputs):
            raise OpenLineageStaticError(
                "One dataset cannot be both an input and output for a model job",
                location=f"{location}/dependencies",
            )

        job_events.append(
            build_job_event(
                event_time=event_time,
                job=job,
                facets=_job_facets(model_declaration, location),
                inputs=inputs,
                outputs=outputs,
            )
        )

    job_events.sort(
        key=lambda event: (
            str(_event_entity(event, "job")["namespace"]),
            str(_event_entity(event, "job")["name"]),
        )
    )
    events = (*dataset_events, *job_events)
    try:
        validate_event_sequence(events)
    except OpenLineageValidationError as error:
        raise OpenLineageStaticError(
            f"Generated OpenLineage event sequence is invalid: {error}",
            location="/events",
        ) from error

    return StaticOpenLineageExport(
        events=events,
        warnings=tuple(sorted(warnings)),
        dataset_count=len(dataset_events),
        job_count=len(job_events),
    )


def _require_static_event_time(value: object) -> str:
    """Require and preserve a timezone-aware RFC 3339 UTC manifest timestamp."""
    if not isinstance(value, str) or _STATIC_UTC_EVENT_TIME_RE.fullmatch(value) is None:
        raise OpenLineageStaticError(
            "Manifest compile time must be a timezone-aware RFC 3339 UTC value",
            location="manifest.compiled_at",
        )
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise OpenLineageStaticError(
            "Manifest compile time must be a timezone-aware RFC 3339 UTC value",
            location="manifest.compiled_at",
        ) from error
    if parsed.tzinfo is None or parsed.utcoffset() != timedelta(0):
        raise OpenLineageStaticError(
            "Manifest compile time must be a timezone-aware RFC 3339 UTC value",
            location="manifest.compiled_at",
        )
    return value


def _validate_export_identity(context: _StaticContext) -> None:
    if not context.project.project.name.strip():
        raise OpenLineageStaticError(
            "Project name must be non-blank for OpenLineage export",
            location="/project/name",
        )
    if context.manifest.project_name != context.project.project.name:
        raise OpenLineageStaticError(
            "Dry-run manifest project does not match the parsed project",
            location="manifest.project",
        )
    if not context.job_namespace.strip():
        raise OpenLineageStaticError(
            "OpenLineage job namespace must be non-blank",
            location="job_namespace",
        )


def _validate_compile_views(
    context: _StaticContext,
) -> tuple[dict[str, Model], dict[str, str]]:
    declarations: dict[str, Model] = {}
    locations: dict[str, str] = {}
    for index, model in enumerate(context.project.models):
        if not model.name.strip():
            raise OpenLineageStaticError(
                "Model name must be non-blank for OpenLineage export",
                location=f"/models/{index}/name",
            )
        if model.name in declarations:
            raise OpenLineageStaticError(
                f"Duplicate model declaration '{model.name}'",
                location=f"/models/{index}/name",
            )
        declarations[model.name] = model
        locations[model.name] = f"/models/{index}"

    expected = set(declarations)
    _require_exact_keys(expected, set(context.resolved_models), "resolved_models")
    _require_exact_keys(expected, set(context.compiled_models), "compiled_models")
    for name in sorted(expected):
        resolved = context.resolved_models[name]
        compiled = context.compiled_models[name]
        location = locations[name]
        if compiled.model_name != name:
            raise OpenLineageStaticError(
                "Compiled model projection identity does not match its mapping key",
                location=f"{location}/name",
            )
        if compiled.materialized != resolved.materialized:
            raise OpenLineageStaticError(
                "Resolved and compiled model materializations do not match",
                location=f"{location}/materialized",
            )
        _validate_compiled_shape(compiled, location)
    return declarations, locations


def _require_exact_keys(expected: set[str], actual: set[str], location: str) -> None:
    if expected == actual:
        return
    if missing := sorted(expected - actual):
        message = f"Compiled model view is missing model '{missing[0]}'"
    else:
        extra = sorted(actual - expected)
        message = f"Compiled model view contains unknown model '{extra[0]}'"
    raise OpenLineageStaticError(message, location=location)


def _validate_compiled_shape(compiled: CompiledModelView, location: str) -> None:
    materialized = compiled.materialized
    if compiled.output_kind is None:
        if compiled.output_name is not None:
            raise OpenLineageStaticError(
                "Compiled model without an output kind has an output name",
                location=f"{location}/output",
            )
    elif compiled.output_name is None or not compiled.output_name.strip():
        raise OpenLineageStaticError(
            "Compiled model output name must be non-blank",
            location=f"{location}/output",
        )

    if materialized == MaterializedType.TOPIC:
        valid = compiled.output_kind == "kafka" and compiled.process_kind in {
            None,
            "flink",
            "kafka_streams",
        }
    elif materialized == MaterializedType.FLINK:
        valid = compiled.output_kind == "kafka" and compiled.process_kind == "flink"
    elif materialized == MaterializedType.VIRTUAL_TOPIC:
        valid = compiled.output_kind == "gateway" and compiled.process_kind == "gateway"
    elif materialized == MaterializedType.SINK:
        valid = compiled.output_kind is None and compiled.process_kind == "connect"
    else:
        valid = False
    if not valid:
        raise OpenLineageStaticError(
            "Compiled model process/output shape does not match its materialization",
            location=f"{location}/materialized",
        )

    if materialized == MaterializedType.VIRTUAL_TOPIC:
        if (
            compiled.gateway_physical_input is None
            or not compiled.gateway_physical_input.strip()
        ):
            raise OpenLineageStaticError(
                "Compiled Gateway model has no physical input",
                location=f"{location}/gateway/physical_input",
            )
    elif compiled.gateway_physical_input is not None:
        raise OpenLineageStaticError(
            "Non-Gateway model unexpectedly has a Gateway physical input",
            location=f"{location}/gateway/physical_input",
        )

    connector_inputs = _connector_inputs(compiled, location)
    if materialized != MaterializedType.SINK and connector_inputs:
        raise OpenLineageStaticError(
            "Non-sink model unexpectedly has connector inputs",
            location=f"{location}/connector_inputs",
        )


def _connector_inputs(
    compiled: CompiledModelView,
    location: str,
) -> tuple[str, ...]:
    raw = compiled.connector_inputs
    if isinstance(raw, (str, bytes)):
        raise OpenLineageStaticError(
            "Compiled connector inputs must be a sequence of topic names",
            location=f"{location}/connector_inputs",
        )
    values = tuple(raw)
    if any(not isinstance(value, str) or not value.strip() for value in values):
        raise OpenLineageStaticError(
            "Compiled connector input names must be non-blank strings",
            location=f"{location}/connector_inputs",
        )
    return values


def _build_dataset_inventory(
    context: _StaticContext,
    declarations: Mapping[str, Model],
) -> dict[LogicalDataset, _DatasetDeclaration]:
    inventory: dict[LogicalDataset, _DatasetDeclaration] = {}
    identities: dict[DatasetIdentity, LogicalDataset] = {}
    source_names: set[str] = set()
    for index, source in enumerate(context.project.sources):
        location = f"/sources/{index}"
        if not source.name.strip():
            raise OpenLineageStaticError(
                "Source name must be non-blank for OpenLineage export",
                location=f"{location}/name",
            )
        if not source.topic.strip():
            raise OpenLineageStaticError(
                "Source physical topic must be non-blank for OpenLineage export",
                location=f"{location}/topic",
            )
        if source.name in source_names:
            raise OpenLineageStaticError(
                f"Duplicate source declaration '{source.name}'",
                location=f"{location}/name",
            )
        source_names.add(source.name)
        _register_dataset(
            inventory,
            identities,
            logical=("source", source.name),
            identity=DatasetIdentity(
                _require_namespace(context.kafka_namespace, "kafka_namespace"),
                source.topic,
            ),
            declaration=source,
            location=location,
        )

    for model_name, declaration in declarations.items():
        compiled = context.compiled_models[model_name]
        if compiled.output_kind is None:
            continue
        namespace = (
            _require_namespace(context.kafka_namespace, "kafka_namespace")
            if compiled.output_kind == "kafka"
            else _require_namespace(context.gateway_namespace, "gateway_namespace")
        )
        output_name = compiled.output_name
        if output_name is None:  # Defensive; shape validation already rejected this.
            raise OpenLineageStaticError(
                "Compiled model output name is missing",
                location=f"{_model_location(context.project, model_name)}/output",
            )
        _register_dataset(
            inventory,
            identities,
            logical=("model", model_name),
            identity=DatasetIdentity(namespace, output_name),
            declaration=declaration,
            location=_model_location(context.project, model_name),
        )
    return inventory


def _register_dataset(
    inventory: dict[LogicalDataset, _DatasetDeclaration],
    identities: dict[DatasetIdentity, LogicalDataset],
    *,
    logical: LogicalDataset,
    identity: DatasetIdentity,
    declaration: Source | Model,
    location: str,
) -> None:
    previous = identities.get(identity)
    if previous is not None and previous != logical:
        raise OpenLineageStaticError(
            "Two declarations resolve to the same OpenLineage dataset identity",
            location=location,
        )
    identities[identity] = logical
    inventory[logical] = _DatasetDeclaration(
        logical=logical,
        identity=identity,
        declaration=declaration,
        location=location,
    )


def _dataset_facets(
    dataset: _DatasetDeclaration,
) -> tuple[dict[str, dict[str, object]], OpenLineageExportWarning | None]:
    declaration = dataset.declaration
    facets: dict[str, dict[str, object]] = {
        "datasetType": standard_facet(
            "dataset", "datasetType", {"datasetType": "TOPIC"}
        )
    }
    description = _optional_declared_text(
        declaration.description,
        label="description",
        location=f"{dataset.location}/description",
    )
    if description is not None:
        facets["documentation"] = standard_facet(
            "dataset", "documentation", {"description": description}
        )
    owner = _optional_declared_text(
        declaration.owner,
        label="owner",
        location=f"{dataset.location}/owner",
    )
    if owner is not None:
        facets["ownership"] = standard_facet(
            "dataset", "ownership", {"owners": [{"name": owner}]}
        )

    columns, columns_location = _declared_columns(declaration, dataset.location)
    warning: OpenLineageExportWarning | None = None
    if columns:
        fields: list[dict[str, object]] = []
        field_names: set[str] = set()
        for index, column in enumerate(columns):
            name = column.name
            if not isinstance(name, str) or not name.strip():
                raise OpenLineageStaticError(
                    "OpenLineage schema field name must be non-blank",
                    location=f"{columns_location}/{index}/name",
                )
            if name in field_names:
                raise OpenLineageStaticError(
                    f"Duplicate OpenLineage schema field '{name}'",
                    location=f"{columns_location}/{index}/name",
                )
            field_names.add(name)
            field: dict[str, object] = {
                "name": name,
                "ordinal_position": index + 1,
            }
            column_type = column.type
            if column_type is not None and column_type.strip():
                field["type"] = column_type
            if column.description is not None:
                if not column.description.strip():
                    raise OpenLineageStaticError(
                        "OpenLineage schema field description must be non-blank",
                        location=f"{columns_location}/{index}/description",
                    )
                field["description"] = column.description
            fields.append(field)
        facets["schema"] = standard_facet("dataset", "schema", {"fields": fields})
    else:
        kind, name = dataset.logical
        warning = OpenLineageExportWarning(
            code=ErrorCode.OPENLINEAGE_SCHEMA_INCOMPLETE,
            message=(
                f"{kind.capitalize()} dataset '{name}' has no declared columns; "
                "the OpenLineage schema facet is omitted"
            ),
            location=columns_location,
        )
    return facets, warning


def _declared_columns(
    declaration: Source | Model,
    location: str,
) -> tuple[Sequence[_DeclaredColumn], str]:
    if isinstance(declaration, Source):
        return declaration.columns, f"{location}/columns"
    if declaration.contract is not None:
        return declaration.contract.columns, f"{location}/contract/columns"
    if declaration.columns is not None:
        return declaration.columns, f"{location}/columns"
    return (), f"{location}/columns"


def _job_facets(model: Model, location: str) -> dict[str, dict[str, object]]:
    facets: dict[str, dict[str, object]] = {
        "jobType": standard_facet(
            "job",
            "jobType",
            {
                "integration": "STREAMT",
                "jobType": "MODEL",
                "processingType": "STREAMING",
            },
        )
    }
    description = _optional_declared_text(
        model.description,
        label="description",
        location=f"{location}/description",
    )
    if description is not None:
        facets["documentation"] = standard_facet(
            "job", "documentation", {"description": description}
        )
    owner = _optional_declared_text(
        model.owner,
        label="owner",
        location=f"{location}/owner",
    )
    if owner is not None:
        facets["ownership"] = standard_facet(
            "job", "ownership", {"owners": [{"name": owner}]}
        )
    return facets


def _job_inputs(
    context: _StaticContext,
    model_name: str,
    compiled: CompiledModelView,
    datasets: Mapping[LogicalDataset, _DatasetDeclaration],
    location: str,
) -> tuple[DatasetIdentity, ...]:
    dependencies = _unique_dependencies(
        context.resolved_models[model_name].dependencies
    )
    if compiled.process_kind == "gateway":
        if len(dependencies) != 1:
            raise OpenLineageStaticError(
                "Gateway model must have exactly one direct physical dependency",
                location=f"{location}/dependencies",
            )
        dependency_dataset = _resolve_dependency_dataset(
            dependencies[0], datasets, location
        )
        kafka_namespace = _require_namespace(
            context.kafka_namespace, "kafka_namespace"
        )
        if dependency_dataset.namespace != kafka_namespace:
            raise OpenLineageStaticError(
                "Chained Gateway virtual-topic dependencies are not supported",
                location=f"{location}/dependencies",
            )
        if compiled.gateway_physical_input != dependency_dataset.name:
            raise OpenLineageStaticError(
                "Compiled Gateway physical input does not match its direct dependency",
                location=f"{location}/gateway/physical_input",
            )
        return (dependency_dataset,)

    identities: list[DatasetIdentity] = []
    identity_owners: dict[DatasetIdentity, ModelDependency] = {}
    for dependency in dependencies:
        identity = _resolve_dependency_dataset(dependency, datasets, location)
        previous = identity_owners.get(identity)
        if previous is not None and previous != dependency:
            raise OpenLineageStaticError(
                "Distinct logical dependencies resolve to one OpenLineage dataset",
                location=f"{location}/dependencies",
            )
        identity_owners[identity] = dependency
        identities.append(identity)

    if compiled.process_kind == "connect":
        expected = tuple(identity.name for identity in identities)
        actual = _connector_inputs(compiled, location)
        if actual != expected:
            raise OpenLineageStaticError(
                "Compiled connector inputs do not match resolved direct dependencies",
                location=f"{location}/connector_inputs",
            )
    return tuple(sorted(identities))


def _unique_dependencies(
    dependencies: Sequence[ModelDependency],
) -> tuple[ModelDependency, ...]:
    result: list[ModelDependency] = []
    seen: set[ModelDependency] = set()
    for dependency in dependencies:
        if dependency not in seen:
            result.append(dependency)
            seen.add(dependency)
    return tuple(result)


def _resolve_dependency_dataset(
    dependency: ModelDependency,
    datasets: Mapping[LogicalDataset, _DatasetDeclaration],
    location: str,
) -> DatasetIdentity:
    if dependency.kind not in {"source", "model"}:
        raise OpenLineageStaticError(
            "Resolved dependency kind is not supported",
            location=f"{location}/dependencies",
        )
    logical: LogicalDataset = (dependency.kind, dependency.name)
    dataset = datasets.get(logical)
    if dataset is None:
        raise OpenLineageStaticError(
            f"Direct dependency '{dependency.name}' has no exported dataset",
            location=f"{location}/dependencies",
        )
    return dataset.identity


def _optional_declared_text(
    value: str | None,
    *,
    label: str,
    location: str,
) -> str | None:
    if value is None or value == "":
        return None
    if not value.strip():
        raise OpenLineageStaticError(
            f"Declared {label} must not contain only whitespace",
            location=location,
        )
    return value


def _require_namespace(value: str | None, location: str) -> str:
    if value is None:
        raise OpenLineageStaticError(
            "Required OpenLineage dataset namespace was not resolved",
            location=location,
        )
    try:
        return validate_kafka_namespace(value)
    except OpenLineageConstructionError as error:
        raise OpenLineageStaticError(
            f"Invalid OpenLineage dataset namespace: {error}",
            location=location,
        ) from error


def _model_location(project: StreamtProject, name: str) -> str:
    for index, model in enumerate(project.models):
        if model.name == name:
            return f"/models/{index}"
    return "/models"


def _event_entity(event: Mapping[str, object], key: str) -> Mapping[str, object]:
    value = event.get(key)
    if not isinstance(value, Mapping):
        raise OpenLineageStaticError(
            f"Generated OpenLineage event has no {key} object",
            location="events",
        )
    return value
