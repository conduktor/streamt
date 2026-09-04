"""Core models and utilities for streamt."""

from __future__ import annotations

from importlib import import_module
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from streamt.core.dag import DAGBuilder
    from streamt.core.models import (
        DataTest,
        Exposure,
        Model,
        Project,
        Source,
        StreamtProject,
    )
    from streamt.core.parser import ProjectParser
    from streamt.core.validator import ProjectValidator

_PUBLIC_MODULES = {
    "DAGBuilder": "streamt.core.dag",
    "DataTest": "streamt.core.models",
    "Exposure": "streamt.core.models",
    "Model": "streamt.core.models",
    "Project": "streamt.core.models",
    "ProjectParser": "streamt.core.parser",
    "ProjectValidator": "streamt.core.validator",
    "Source": "streamt.core.models",
    "StreamtProject": "streamt.core.models",
}


def __getattr__(name: str) -> object:
    """Load legacy package-level exports only when they are requested."""
    module_name = _PUBLIC_MODULES.get(name)
    if module_name is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    value = getattr(import_module(module_name), name)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    """Include lazy public exports in module discovery."""
    return sorted({*globals(), *_PUBLIC_MODULES})

__all__ = [
    "Project",
    "Source",
    "Model",
    "DataTest",
    "Exposure",
    "StreamtProject",
    "ProjectParser",
    "ProjectValidator",
    "DAGBuilder",
]
