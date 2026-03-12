"""streamt docs commands."""

from __future__ import annotations

from typing import Optional

import click

from streamt.cli.helpers import get_project_path, handle_parse_error, make_formatter
from streamt.core.errors import ErrorCode


@click.group()
def docs() -> None:
    """Documentation commands."""
    pass


@docs.command("generate")
@click.option("--project-dir", "-p", type=click.Path(exists=True), help="Path to project directory")
@click.option("--env", "-e", "environment", help="Target environment (reads from STREAMT_ENV if not set)")
@click.option("--output-dir", "-O", type=click.Path(), default="docs", help="Output directory")
@click.pass_context
def docs_generate(
    ctx: click.Context,
    project_dir: Optional[str],
    environment: Optional[str],
    output_dir: str,
) -> None:
    """Generate HTML documentation."""
    from streamt.core.dag import DAGBuilder
    from streamt.core.environment import EnvironmentError
    from streamt.core.parser import EnvVarError, ParseError, ProjectParser
    from streamt.docs import generate_docs

    fmt = make_formatter(ctx, "docs generate")
    project_path = get_project_path(project_dir)

    try:
        parser = ProjectParser(
            project_path, environment=environment,
            warn_callback=lambda msg: fmt.print(msg),
        )
        project = parser.parse()
        dag_builder = DAGBuilder(project)
        dag = dag_builder.build()

        out_path = project_path / output_dir
        generate_docs(project, dag, out_path)

        fmt.set_data({"output_dir": str(out_path)})
        fmt.print(f"[green]Documentation generated at {out_path}[/green]")
        fmt.flush()

    except (EnvVarError, ParseError, EnvironmentError) as e:
        handle_parse_error(fmt, e, ErrorCode.PARSE_ERROR)
