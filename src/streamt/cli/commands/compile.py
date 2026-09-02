"""streamt compile command."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Optional

import click

from streamt.cli.helpers import get_project_path, handle_parse_error, make_formatter
from streamt.core.errors import ErrorCode
from streamt.output import StructuredError


@click.command()
@click.option("--project-dir", "-p", type=click.Path(exists=True), help="Path to project directory")
@click.option("--env", "-e", "environment", help="Target environment (reads from STREAMT_ENV if not set)")
@click.option("--output-dir", type=click.Path(), help="Output directory for generated artifacts")
@click.option("--dry-run", is_flag=True, help="Show what would be generated without writing files")
@click.pass_context
def compile(
    ctx: click.Context,
    project_dir: Optional[str],
    environment: Optional[str],
    output_dir: Optional[str],
    dry_run: bool,
) -> None:
    """Compile project to artifacts."""
    from streamt.compiler import Compiler
    from streamt.core.environment import EnvironmentError
    from streamt.core.parser import EnvVarError, ParseError, ProjectParser
    from streamt.core.validator import ProjectValidator

    fmt = make_formatter(ctx, "compile")
    project_path = get_project_path(project_dir)

    try:
        parser = ProjectParser(
            project_path, environment=environment,
            warn_callback=lambda msg: fmt.print(msg),
        )
        project = parser.parse()

        validator = ProjectValidator(project)
        result = validator.validate()
        if not result.is_valid:
            for error in result.errors:
                fmt.add_error(StructuredError(code=ErrorCode.PARSE_ERROR, message=error.message))
                fmt.print_error(error.message)
            fmt.flush()
            sys.exit(1)

        out_path = Path(output_dir) if output_dir else None
        compiler = Compiler(project, out_path)
        manifest = compiler.compile(dry_run=dry_run)

        artifacts = manifest.artifacts
        artifact_kinds = ["topics", "flink_jobs", "connectors", "gateway_rules", "schemas"]
        counts = {kind: len(artifacts.get(kind, [])) for kind in artifact_kinds}
        data: dict[str, object] = {
            "dry_run": dry_run,
            "artifacts": {
                "topics": [t["name"] for t in artifacts.get("topics", [])],
                "flink_jobs": [j["name"] for j in artifacts.get("flink_jobs", [])],
                "connectors": [c["name"] for c in artifacts.get("connectors", [])],
                "gateway_rules": [r["name"] for r in artifacts.get("gateway_rules", [])],
                "schemas": [
                    {"subject": s["subject"], "schema_type": s.get("schema_type")}
                    for s in artifacts.get("schemas", [])
                ],
            },
            "counts": counts,
        }
        if not dry_run:
            data["output_dir"] = str(compiler.output_dir)

        fmt.set_data(data)

        if dry_run:
            fmt.print("[yellow]Dry run - no files written[/yellow]")
            fmt.print("\nArtifacts that would be generated:")
            for kind in artifact_kinds:
                items = artifacts.get(kind, [])
                if items:
                    label = kind.replace("_", " ").title()
                    fmt.print(f"\n[cyan]{label} ({len(items)}):[/cyan]")
                    for item in items:
                        fmt.print(f"  - {item.get('name') or item.get('subject', '?')}")
        else:
            fmt.print(f"[green]Compiled to {compiler.output_dir}[/green]")
            fmt.print_table(
                "Generated Artifacts",
                [("Type", "cyan"), ("Count", "green")],
                [[kind.replace("_", " ").title(), str(count)] for kind, count in counts.items()],
            )

        fmt.flush()

    except (EnvVarError, ParseError, EnvironmentError) as e:
        handle_parse_error(fmt, e, ErrorCode.PARSE_ERROR)
    except Exception as e:
        fmt.add_error(StructuredError(code=ErrorCode.PARSE_ERROR, message=str(e)))
        fmt.print_error(str(e))
        fmt.flush()
        raise
