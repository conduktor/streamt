"""Shared, structurally read-only model resolution for compiler consumers."""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import Literal

from jinja2 import Environment, FileSystemLoader, StrictUndefined, TemplateNotFound, UndefinedError

from streamt.core.models import MaterializedType, Model, StreamtProject

DependencyKind = Literal["source", "model"]

_SOURCE_REF_RE = re.compile(
    r'\{\{\s*source\s*\(\s*["\']([^"\']*)["\']\s*\)\s*\}\}'
)
_MODEL_REF_RE = re.compile(
    r'\{\{\s*ref\s*\(\s*["\']([^"\']*)["\']\s*\)\s*\}\}'
)
_DEPENDENCY_CALL_RE = re.compile(r"\{\{\s*(?:source|ref)\s*\(")


class CompileError(Exception):
    """Error while resolving or compiling a project."""


@dataclass(frozen=True)
class ModelDependency:
    """One direct logical dependency of a resolved model."""

    name: str
    kind: DependencyKind


@dataclass(frozen=True)
class ResolvedModel:
    """One model's frozen metadata and private compiler-owned model copy.

    The outer record and published mapping are immutable. ``model`` remains a
    mutable Pydantic object for existing compiler internals, but it is a deep
    copy and must be treated as read-only by snapshot consumers.
    """

    model: Model
    materialized: MaterializedType
    dependencies: tuple[ModelDependency, ...]


ResolvedModels = Mapping[str, ResolvedModel]


def empty_resolved_models() -> ResolvedModels:
    """Return an immutable empty model snapshot."""
    return MappingProxyType({})


def resolve_project_models(project: StreamtProject) -> ResolvedModels:
    """Render, classify, and validate every model exactly once for one compile."""
    _validate_declaration_names(project)

    resolved: dict[str, ResolvedModel] = {}
    for declaration in sorted(project.models, key=lambda candidate: candidate.name):
        model = declaration.model_copy(deep=True)
        # Public Pydantic copies may bypass validation; compilation still must
        # reject conflicting explicit execution contracts before any artifacts.
        try:
            model.check_executor_configuration()
        except ValueError as error:
            raise CompileError(f"Model '{model.name}': {error}") from error
        if model.macro:
            rendered_sql = _render_macro_sql(project, model)
            model = model.model_copy(
                update={"sql": rendered_sql, "macro": None, "params": {}}
            )

        dependencies = _resolve_dependencies(project, model)
        resolved[model.name] = ResolvedModel(
            model=model,
            materialized=_effective_materialization(project, model),
            dependencies=dependencies,
        )

    _validate_model_cycles(resolved)
    return MappingProxyType(resolved)


def direct_model_dependencies(model: Model) -> tuple[ModelDependency, ...]:
    """Extract and stable-deduplicate one model's direct logical dependencies."""
    candidates: list[ModelDependency] = []
    if model.sql:
        candidates.extend(
            ModelDependency(match.group(1), "source")
            for match in _SOURCE_REF_RE.finditer(model.sql)
        )
        candidates.extend(
            ModelDependency(match.group(1), "model")
            for match in _MODEL_REF_RE.finditer(model.sql)
        )
        unmatched_sql = _MODEL_REF_RE.sub("", _SOURCE_REF_RE.sub("", model.sql))
        if _DEPENDENCY_CALL_RE.search(unmatched_sql):
            raise CompileError(
                f"Model '{model.name}': source and ref calls must use one quoted "
                "non-blank literal name"
            )
    elif model.from_:
        for index, from_ref in enumerate(model.from_):
            targets: list[ModelDependency] = []
            if from_ref.source is not None:
                targets.append(ModelDependency(from_ref.source, "source"))
            if from_ref.ref is not None:
                targets.append(ModelDependency(from_ref.ref, "model"))
            if len(targets) != 1 or not targets[0].name.strip():
                raise CompileError(
                    f"Model '{model.name}': from entry {index} must declare exactly one "
                    "non-blank source or ref"
                )
            candidates.append(targets[0])

    dependencies: list[ModelDependency] = []
    seen: set[ModelDependency] = set()
    for dependency in candidates:
        if not dependency.name.strip():
            raise CompileError(
                f"Model '{model.name}': source and ref calls must use one quoted "
                "non-blank literal name"
            )
        if dependency not in seen:
            dependencies.append(dependency)
            seen.add(dependency)
    return tuple(dependencies)


def _render_macro_sql(project: StreamtProject, model: Model) -> str:
    """Render one macro while preserving the compiler's established behavior."""
    macro_name = model.macro
    project_path = project.project_path

    if project_path is None:
        raise CompileError(
            f"Model '{model.name}': cannot load macro '{macro_name}' without a project path"
        )

    macros_dir = project_path / "macros"
    if not macros_dir.exists():
        raise CompileError(
            f"Model '{model.name}': macro '{macro_name}' referenced but no macros/ directory found"
        )

    env = Environment(
        loader=FileSystemLoader(str(macros_dir)),
        undefined=StrictUndefined,
    )

    def source_fn(name: str) -> str:
        return f'{{{{ source("{name}") }}}}'

    def ref_fn(name: str) -> str:
        return f'{{{{ ref("{name}") }}}}'

    try:
        template = env.get_template(f"{macro_name}.sql.j2")
    except TemplateNotFound:
        raise CompileError(
            f"Model '{model.name}': macro file '{macro_name}.sql.j2' not found in {macros_dir}"
        ) from None

    try:
        return template.render(source=source_fn, ref=ref_fn, **model.params)
    except UndefinedError as error:
        raise CompileError(f"Model '{model.name}': macro template error: {error}") from error


def _resolve_dependencies(
    project: StreamtProject,
    model: Model,
) -> tuple[ModelDependency, ...]:
    """Resolve direct dependencies from SQL, or from ``from`` when SQL is absent."""
    source_names = {source.name for source in project.sources}
    model_names = {candidate.name for candidate in project.models}
    dependencies = direct_model_dependencies(model)
    for dependency in dependencies:
        if dependency.kind == "source" and dependency.name not in source_names:
            raise CompileError(
                f"Model '{model.name}': source '{dependency.name}' was not found"
            )
        if dependency.kind == "model" and dependency.name not in model_names:
            raise CompileError(
                f"Model '{model.name}': model '{dependency.name}' was not found"
            )
        if dependency.kind == "model" and dependency.name == model.name:
            raise CompileError(f"Model '{model.name}' cannot depend on itself")
    return dependencies


def _effective_materialization(
    project: StreamtProject,
    model: Model,
) -> MaterializedType:
    """Resolve the materialization that the compiler will actually generate."""
    materialized = model.get_materialized()
    if materialized != MaterializedType.VIRTUAL_TOPIC:
        return materialized

    has_gateway = bool(
        project.runtime.conduktor and project.runtime.conduktor.gateway
    )
    is_explicit_virtual_topic = bool(model.gateway and model.gateway.virtual_topic)
    if not has_gateway and not is_explicit_virtual_topic:
        return MaterializedType.FLINK
    return materialized


def _validate_declaration_names(project: StreamtProject) -> None:
    """Reject declaration identities that cannot form an unambiguous graph."""
    source_names: set[str] = set()
    for source in project.sources:
        if source.name in source_names:
            raise CompileError(f"Duplicate source name '{source.name}'")
        source_names.add(source.name)

    model_names: set[str] = set()
    for model in project.models:
        if model.name in model_names:
            raise CompileError(f"Duplicate model name '{model.name}'")
        model_names.add(model.name)

    collisions = sorted(source_names & model_names)
    if collisions:
        raise CompileError(
            f"Name '{collisions[0]}' is used as both a source and a model"
        )


def _validate_model_cycles(resolved: dict[str, ResolvedModel]) -> None:
    """Reject cycles with a deterministic dependency path."""
    state: dict[str, int] = {}
    stack: list[str] = []

    def visit(name: str) -> None:
        current_state = state.get(name, 0)
        if current_state == 2:
            return
        if current_state == 1:
            cycle_start = stack.index(name)
            cycle = [*stack[cycle_start:], name]
            raise CompileError(f"Model dependency cycle detected: {' -> '.join(cycle)}")

        state[name] = 1
        stack.append(name)
        dependencies = sorted(
            dependency.name
            for dependency in resolved[name].dependencies
            if dependency.kind == "model"
        )
        for dependency in dependencies:
            visit(dependency)
        stack.pop()
        state[name] = 2

    for name in sorted(resolved):
        visit(name)
