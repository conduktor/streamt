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
@click.option(
    "--env", "-e", "environment", help="Target environment (reads from STREAMT_ENV if not set)"
)
@click.option("--model", "-m", help="Focus on this model")
@click.option("--upstream", is_flag=True, help="Show only upstream dependencies")
@click.option("--downstream", is_flag=True, help="Show only downstream dependents")
@click.option("--columns", is_flag=True, help="Show column-level lineage (requires --model)")
@click.option(
    "--format",
    "output_format",
    type=click.Choice(["ascii", "json", "mermaid", "dot"]),
    default=None,
    help="Output format (overrides global --output)",
)
@click.pass_context
def lineage(
    ctx: click.Context,
    project_dir: Optional[str],
    environment: Optional[str],
    model: Optional[str],
    upstream: bool,
    downstream: bool,
    columns: bool,
    output_format: Optional[str],
) -> None:
    """Show the DAG lineage."""
    from streamt.core.dag import DAGBuilder
    from streamt.core.environment import EnvironmentError
    from streamt.core.parser import EnvVarError, ParseError, ProjectParser

    fmt = make_formatter(ctx, "lineage")
    project_path = get_project_path(project_dir)

    if (upstream or downstream or columns) and not model:
        fmt.print_error("--upstream and --downstream require --model/-m")
        fmt.flush()
        import sys

        sys.exit(1)

    effective_format = output_format or get_output_format_from_context(ctx)

    try:
        parser = ProjectParser(
            project_path,
            environment=environment,
            warn_callback=lambda msg: fmt.print(msg),
        )
        project = parser.parse()

        dag_builder = DAGBuilder(project)
        dag = dag_builder.build()

        # Column-level lineage (LINEAGE-3)
        if columns and model:
            from streamt.core.dag import ColumnLineageBuilder

            col_builder = ColumnLineageBuilder(project)
            col_lineage = col_builder.build(model)
            col_data = [
                {
                    "column": cl.column,
                    "upstream": [{"source": u[0], "column": u[1]} for u in cl.upstream],
                }
                for cl in col_lineage
            ]
            fmt.set_data({"model": model, "column_lineage": col_data})
            if fmt.format != "json":
                fmt.print(f"[cyan]Column lineage for '{model}':[/cyan]\n")
                for cl in col_lineage:
                    origins = ", ".join(f"{u[0]}.{u[1]}" for u in cl.upstream)
                    fmt.print(f"  {cl.column} ← {origins}")
            fmt.flush()
            return

        # Filter nodes for --upstream / --downstream
        nodes_to_show: set[str] | None = None
        if model:
            if model not in dag.nodes:
                fmt.print_error(f"Model '{model}' not found in DAG")
                fmt.flush()
                import sys

                sys.exit(1)

            if upstream:
                nodes_to_show = dag.get_upstream(model) | {model}
            elif downstream:
                nodes_to_show = dag.get_downstream(model) | {model}

        dag_data = dag.to_dict()

        # Filter dag_data if we have a subset
        if nodes_to_show is not None:
            dag_data["nodes"] = [
                n for n in dag_data.get("nodes", []) if n.get("name") in nodes_to_show
            ]
            dag_data["edges"] = [
                e
                for e in dag_data.get("edges", [])
                if e.get("from") in nodes_to_show and e.get("to") in nodes_to_show
            ]

        fmt.set_data(dag_data)

        if effective_format == "mermaid":
            if nodes_to_show is not None:
                from streamt.core.dag import DAG, DAGNode

                sub = DAG()
                for n in nodes_to_show:
                    orig = dag.get_node(n)
                    if orig:
                        sub.add_node(
                            DAGNode(name=orig.name, type=orig.type, materialized=orig.materialized)
                        )
                for n in nodes_to_show:
                    orig = dag.get_node(n)
                    if orig:
                        for d in orig.downstream:
                            if d in nodes_to_show:
                                sub.add_edge(n, d)
                fmt.print(sub.render_mermaid(focus=model))
            else:
                fmt.print(dag.render_mermaid(focus=model))
        elif effective_format == "dot":
            if nodes_to_show is not None:
                from streamt.core.dag import DAG, DAGNode

                sub = DAG()
                for n in nodes_to_show:
                    orig = dag.get_node(n)
                    if orig:
                        sub.add_node(
                            DAGNode(name=orig.name, type=orig.type, materialized=orig.materialized)
                        )
                for n in nodes_to_show:
                    orig = dag.get_node(n)
                    if orig:
                        for d in orig.downstream:
                            if d in nodes_to_show:
                                sub.add_edge(n, d)
                click.echo(sub.render_dot(focus=model))
            else:
                click.echo(dag.render_dot(focus=model))
        elif effective_format == "json" and fmt.format != "json":
            fmt.print(json.dumps(dag_data, indent=2))
        elif fmt.format != "json":
            if nodes_to_show is not None:
                # Build a filtered sub-DAG for ASCII rendering
                from streamt.core.dag import DAG, DAGNode

                sub = DAG()
                for name in nodes_to_show:
                    orig = dag.get_node(name)
                    if orig:
                        sub.add_node(
                            DAGNode(name=orig.name, type=orig.type, materialized=orig.materialized)
                        )
                for name in nodes_to_show:
                    orig = dag.get_node(name)
                    if orig:
                        for d in orig.downstream:
                            if d in nodes_to_show:
                                sub.add_edge(name, d)
                direction = "upstream" if upstream else "downstream"
                fmt.print(f"[cyan]{direction.capitalize()} of '{model}':[/cyan]\n")
                fmt.print(sub.render_ascii(focus=model))
            else:
                fmt.print(dag.render_ascii(focus=model))

        fmt.flush()

    except (EnvVarError, ParseError, EnvironmentError) as e:
        handle_parse_error(fmt, e, ErrorCode.PARSE_ERROR)
