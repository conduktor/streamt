"""Structured output for streamt CLI.

Provides machine-readable JSON output for LLM agents and CI/CD pipelines,
while preserving Rich human-readable output for interactive use.
"""

from __future__ import annotations

import json
import sys
from dataclasses import dataclass, field
from typing import Any, Optional

from rich.console import Console
from rich.table import Table


@dataclass
class StructuredError:
    """A machine-readable error."""

    code: str
    message: str
    location: Optional[str] = None
    suggestion: Optional[str] = None
    docs_url: Optional[str] = None

    def to_dict(self) -> dict[str, Any]:
        d: dict[str, Any] = {"code": self.code, "message": self.message}
        if self.location:
            d["location"] = self.location
        if self.suggestion:
            d["suggestion"] = self.suggestion
        if self.docs_url:
            d["docs_url"] = self.docs_url
        return d


@dataclass
class StructuredWarning:
    """A machine-readable warning."""

    code: str
    message: str
    location: Optional[str] = None

    def to_dict(self) -> dict[str, Any]:
        d: dict[str, Any] = {"code": self.code, "message": self.message}
        if self.location:
            d["location"] = self.location
        return d


@dataclass
class CommandResult:
    """Result of a CLI command, used for structured output."""

    status: str = "ok"  # "ok" or "error"
    command: str = ""
    data: dict[str, Any] = field(default_factory=dict)
    errors: list[StructuredError] = field(default_factory=list)
    warnings: list[StructuredWarning] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "command": self.command,
            "data": self.data,
            "errors": [e.to_dict() for e in self.errors],
            "warnings": [w.to_dict() for w in self.warnings],
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), indent=2)


class OutputFormatter:
    """Handles output formatting for CLI commands.

    In text mode: uses Rich for human-readable output (current behavior).
    In json mode: emits structured JSON to stdout, progress/debug to stderr.
    """

    def __init__(self, output_format: str = "text") -> None:
        self.format = output_format
        self.console = Console()
        self.stderr = Console(stderr=True)
        self._result = CommandResult()

    def set_command(self, command: str) -> None:
        """Set the command name for the result envelope."""
        self._result.command = command

    def set_data(self, data: dict[str, Any]) -> None:
        """Set the data payload."""
        self._result.data = data

    def set_status(self, status: str) -> None:
        """Set the result status."""
        self._result.status = status

    def add_error(self, error: StructuredError) -> None:
        """Add a structured error."""
        self._result.errors.append(error)
        self._result.status = "error"

    def add_warning(self, warning: StructuredWarning) -> None:
        """Add a structured warning."""
        self._result.warnings.append(warning)

    # -- Text-mode helpers (no-op in json mode) --

    def print(self, message: str) -> None:
        """Print a message (text mode only)."""
        if self.format == "text":
            self.console.print(message)

    def print_error(self, message: str) -> None:
        """Print an error message to stderr (both modes)."""
        self.stderr.print(f"[red]ERROR[/red]: {message}")

    def print_warning(self, message: str) -> None:
        """Print a warning to stderr in json mode, stdout in text mode."""
        if self.format == "json":
            self.stderr.print(f"[yellow]WARNING[/yellow]: {message}")
        else:
            self.console.print(f"[yellow]WARNING[/yellow]: {message}")

    def print_table(self, title: str, columns: list[tuple[str, str]], rows: list[list[str]]) -> None:
        """Print a Rich table (text mode only).

        Args:
            title: Table title
            columns: List of (name, style) tuples
            rows: List of row value lists
        """
        if self.format == "text":
            table = Table(title=title)
            for name, style in columns:
                table.add_column(name, style=style)
            for row in rows:
                table.add_row(*row)
            self.console.print(table)

    def progress(self, message: str) -> None:
        """Print progress info (always to stderr in json mode, stdout in text mode)."""
        if self.format == "json":
            self.stderr.print(message)
        else:
            self.console.print(message)

    # -- Finalize --

    def flush(self) -> None:
        """Flush the result. In json mode, prints the JSON envelope to stdout.

        Resets internal state after writing to prevent stale data on repeat calls.
        """
        if self.format == "json":
            sys.stdout.write(self._result.to_json() + "\n")
            sys.stdout.flush()
        # Reset to prevent re-emitting stale data on subsequent flush() calls
        command = self._result.command
        self._result = CommandResult(command=command)

    def get_result(self) -> CommandResult:
        """Get the accumulated result."""
        return self._result


def get_output_format_from_context(ctx: Any) -> str:
    """Extract output format from Click context.

    Reads from ctx.obj["output"] (set by the main group callback).
    Falls back to root.params if ctx.obj is not populated.
    """
    # Prefer ctx.obj (canonical, set by main group callback)
    root = ctx
    while root.parent is not None:
        root = root.parent
    if root.obj and isinstance(root.obj, dict) and "output" in root.obj:
        return root.obj["output"]
    return root.params.get("output", "text") or "text"
