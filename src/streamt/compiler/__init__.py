"""Compiler for streamt projects."""

from __future__ import annotations

from importlib import import_module
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from streamt.compiler.compiler import Compiler
    from streamt.compiler.manifest import Manifest

_PUBLIC_MODULES = {
    "Compiler": "streamt.compiler.compiler",
    "Manifest": "streamt.compiler.manifest",
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

__all__ = ["Compiler", "Manifest"]
