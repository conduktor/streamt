"""Shared CLI helpers."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Optional

import click

from streamt.output import OutputFormatter, StructuredError, get_output_format_from_context


def get_project_path(project_dir: Optional[str]) -> Path:
    """Get the project path."""
    if project_dir:
        return Path(project_dir).resolve()
    return Path.cwd()


def make_formatter(ctx: click.Context, command: str) -> OutputFormatter:
    """Create an OutputFormatter from Click context."""
    fmt = OutputFormatter(get_output_format_from_context(ctx))
    fmt.set_command(command)
    return fmt


def handle_parse_error(fmt: OutputFormatter, e: Exception, code: str) -> None:
    """Handle a parse/env/environment error uniformly."""
    fmt.add_error(StructuredError(code=code, message=str(e)))
    fmt.print_error(str(e))
    fmt.flush()
    sys.exit(1)
