"""streamt list command."""

from __future__ import annotations

import re
from typing import Any, Optional

import click

from streamt.cli.helpers import get_project_path, handle_parse_error, make_formatter
from streamt.core.errors import ErrorCode


@click.command("list")
@click.argument("resource_type", type=click.Choice(["sources", "models", "tests", "exposures"]))
@click.option("--project-dir", "-p", type=click.Path(exists=True), help="Path to project directory")
@click.option("--env", "-e", "environment", help="Target environment (reads from STREAMT_ENV if not set)")
@click.pass_context
def list_resources(
    ctx: click.Context,
    resource_type: str,
    project_dir: Optional[str],
    environment: Optional[str],
) -> None:
    """List project resources (sources, models, tests, exposures)."""
    from streamt.core.parser import EnvVarError, ParseError, ProjectParser
    from streamt.core.environment import EnvironmentError

    fmt = make_formatter(ctx, "list")
    project_path = get_project_path(project_dir)

    try:
        parser = ProjectParser(
            project_path, environment=environment,
            warn_callback=lambda msg: fmt.print(msg),
        )
        project = parser.parse()
        items: list[dict[str, Any]] = []

        if resource_type == "sources":
            for s in project.sources:
                items.append({
                    "name": s.name, "topic": s.topic, "description": s.description,
                    "has_schema": s.schema_ is not None, "columns": len(s.columns), "tags": s.tags,
                })
            fmt.print_table(
                "Sources",
                [("Name", "cyan"), ("Topic", "green"), ("Columns", "yellow"), ("Schema", "dim")],
                [[i["name"], i["topic"], str(i["columns"]), "yes" if i["has_schema"] else "-"] for i in items],
            )

        elif resource_type == "models":
            for m in project.models:
                upstream: list[str] = []
                if m.sql:
                    upstream += re.findall(r'{{\s*source\(\s*["\'](\w+)["\']\s*\)', m.sql)
                    upstream += re.findall(r'{{\s*ref\(\s*["\'](\w+)["\']\s*\)', m.sql)
                if m.from_:
                    for f in m.from_:
                        if f.source:
                            upstream.append(f.source)
                        if f.ref:
                            upstream.append(f.ref)
                items.append({
                    "name": m.name, "materialized": m.get_materialized().value,
                    "description": m.description, "upstream": upstream,
                    "tags": m.tags, "has_sql": m.sql is not None,
                })
            fmt.print_table(
                "Models",
                [("Name", "cyan"), ("Materialized", "green"), ("Upstream", "yellow")],
                [[i["name"], i["materialized"], ", ".join(i["upstream"]) or "-"] for i in items],
            )

        elif resource_type == "tests":
            for t in project.tests:
                items.append({
                    "name": t.name, "model": t.model,
                    "type": t.type.value, "assertions": len(t.assertions),
                })
            fmt.print_table(
                "Tests",
                [("Name", "cyan"), ("Model", "green"), ("Type", "yellow"), ("Assertions", "dim")],
                [[i["name"], i["model"], i["type"], str(i["assertions"])] for i in items],
            )

        elif resource_type == "exposures":
            for e in project.exposures:
                items.append({
                    "name": e.name, "type": e.type.value,
                    "description": e.description, "owner": e.owner,
                })
            fmt.print_table(
                "Exposures",
                [("Name", "cyan"), ("Type", "green"), ("Owner", "yellow")],
                [[i["name"], i["type"], i.get("owner") or "-"] for i in items],
            )

        fmt.set_data({"resource_type": resource_type, "count": len(items), "items": items})
        fmt.flush()

    except (EnvVarError, ParseError, EnvironmentError) as e:
        handle_parse_error(fmt, e, ErrorCode.PARSE_ERROR)
