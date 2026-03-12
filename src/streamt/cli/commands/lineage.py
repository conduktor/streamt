"""streamt lineage command."""

from __future__ import annotations

import json
from typing import Optional

import click

from streamt.cli.helpers import get_project_path, handle_parse_error, make_formatter
from streamt.core.errors import ErrorCode
from streamt.output import get_output_format_from_context


@click.command()
@click.option("--project-dir", "-p", type=click.Path(exists=True), help="Path to project directory")
@click.option("--env", "-e", "environment", help="Target environment (reads from STREAMT_ENV if not set)")
@click.option("--model", "-m", help="Focus on this model")
@click.option("--upstream", is_flag=True, help="Show only upstream dependencies")
@click.option("--downstream", is_flag=True, help="Show only downstream dependents")
@click.option("--format", "output_format", type=click.Choice(["ascii", "json"]), default=None, help="Output format (overrides global --output)")
@click.pass_context
def lineage(
    ctx: click.Context,
    project_dir: Optional[str],
    environment: Optional[str],
    model: Optional[str],
    upstream: bool,
    downstream: bool,
    output_format: Optional[str],
) -> None:
    """Show the DAG lineage."""
    from streamt.core.dag import DAGBuilder
    from streamt.core.parser import EnvVarError, ParseError, ProjectParser
    from streamt.core.environment import EnvironmentError

    fmt = make_formatter(ctx, "lineage")
    project_path = get_project_path(project_dir)
    effective_format = output_format or get_output_format_from_context(ctx)

    try:
        parser = ProjectParser(
            project_path, environment=environment,
            warn_callback=lambda msg: fmt.print(msg),
        )
        project = parser.parse()

        dag_builder = DAGBuilder(project)
        dag = dag_builder.build()
        dag_data = dag.to_dict()
        fmt.set_data(dag_data)

        if effective_format == "json" and fmt.format != "json":
            # Legacy --format json (not global --output json)
            fmt.print(json.dumps(dag_data, indent=2))
        elif fmt.format != "json":
            fmt.print(dag.render_ascii(focus=model))

        fmt.flush()

    except (EnvVarError, ParseError, EnvironmentError) as e:
        handle_parse_error(fmt, e, ErrorCode.PARSE_ERROR)
