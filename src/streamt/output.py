"""Structured output for streamt CLI.

Provides machine-readable JSON output for LLM agents and CI/CD pipelines,
while preserving Rich human-readable output for interactive use.
"""

from __future__ import annotations

import json
import sys
from dataclasses import dataclass, field
from typing import Optional, TypeVar

import click
from rich.console import Console
from rich.table import Table

_ItemT = TypeVar("_ItemT")


@dataclass
class StructuredError:
    """A machine-readable error."""

    code: str
    message: str
    location: Optional[str] = None
    suggestion: Optional[str] = None
    docs_url: Optional[str] = None

    def to_dict(self) -> dict[str, object]:
        d: dict[str, object] = {"code": self.code, "message": self.message}
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

    def to_dict(self) -> dict[str, object]:
        d: dict[str, object] = {"code": self.code, "message": self.message}
        if self.location:
            d["location"] = self.location
        return d


@dataclass
class CommandResult:
    """Result of a CLI command, used for structured output."""

    status: str = "ok"  # "ok" or "error"
    command: str = ""
    data: dict[str, object] = field(default_factory=dict)
    errors: list[StructuredError] = field(default_factory=list)
    warnings: list[StructuredWarning] = field(default_factory=list)

    def to_dict(self) -> dict[str, object]:
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

    def __init__(self, output_format: str = "text", quiet: bool = False) -> None:
        self.format = output_format
        self.quiet = quiet
        self.console = Console()
        self.stderr = Console(stderr=True)
        self._result = CommandResult()

    def set_command(self, command: str) -> None:
        """Set the command name for the result envelope."""
        self._result.command = command

    def set_data(self, data: dict[str, object]) -> None:
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
        """Print a message (text mode only, suppressed in quiet mode)."""
        if self.format == "text" and not self.quiet:
            self.console.print(message)

    def print_error(self, message: str) -> None:
        """Print an error message to stderr (both modes)."""
        self.stderr.print(f"[red]ERROR[/red]: {message}")

    def print_warning(self, message: str, code: str = "W000_WARNING") -> None:
        """Print a warning and auto-capture it in the JSON warnings array."""
        # Always capture for JSON envelope consistency
        existing = {w.message for w in self._result.warnings}
        if message not in existing:
            self._result.warnings.append(StructuredWarning(code=code, message=message))
        if self.quiet:
            return
        if self.format == "json":
            self.stderr.print(f"[yellow]WARNING[/yellow]: {message}")
        else:
            self.console.print(f"[yellow]WARNING[/yellow]: {message}")

    def print_table(
        self, title: str, columns: list[tuple[str, str]], rows: list[list[str]]
    ) -> None:
        """Print a Rich table (text mode only, suppressed in quiet mode)."""
        if self.format == "text" and not self.quiet:
            table = Table(title=title)
            for name, style in columns:
                table.add_column(name, style=style)
            for row in rows:
                table.add_row(*row)
            self.console.print(table)

    def progress(self, message: str) -> None:
        """Print progress info. Suppressed in quiet mode."""
        if self.quiet:
            return
        if self.format == "json":
            self.stderr.print(message)
        else:
            self.console.print(message)

    def progress_bar(self, items: list[_ItemT], label: str = "Processing") -> list[_ItemT]:
        """Wrap iterable with a Rich progress bar. Returns list of results.

        Falls back to plain iteration in JSON/quiet mode.
        """
        if self.quiet or self.format == "json":
            return items
        from rich.progress import Progress

        results: list[_ItemT] = []
        with Progress(console=self.stderr, transient=True) as progress:
            task = progress.add_task(label, total=len(items))
            for item in items:
                results.append(item)
                progress.update(task, advance=1)
        return results

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


def get_output_format_from_context(ctx: click.Context) -> str:
    """Extract output format from Click context.

    Reads from ctx.obj["output"] (set by the main group callback).
    Falls back to root.params if ctx.obj is not populated.
    """
    # Prefer ctx.obj (canonical, set by main group callback)
    root = ctx
    while root.parent is not None:
        root = root.parent
    if root.obj and isinstance(root.obj, dict):
        configured = root.obj.get("output")
        if isinstance(configured, str) and configured:
            return configured
    fallback = root.params.get("output", "text")
    return fallback if isinstance(fallback, str) and fallback else "text"
