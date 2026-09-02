"""streamt docs commands."""

from __future__ import annotations

import csv
import io
import json
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
@click.option(
    "--env", "-e", "environment", help="Target environment (reads from STREAMT_ENV if not set)"
)
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
            project_path,
            environment=environment,
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


@docs.command("openapi")
@click.option("--project-dir", "-p", type=click.Path(exists=True), help="Path to project directory")
@click.option("--env", "-e", "environment", help="Target environment")
@click.pass_context
def docs_openapi(
    ctx: click.Context,
    project_dir: Optional[str],
    environment: Optional[str],
) -> None:
    """Generate AsyncAPI/OpenAPI spec for exposed topics."""
    from streamt.core.environment import EnvironmentError
    from streamt.core.parser import EnvVarError, ParseError, ProjectParser

    fmt = make_formatter(ctx, "docs openapi")
    project_path = get_project_path(project_dir)

    try:
        parser = ProjectParser(
            project_path,
            environment=environment,
            warn_callback=lambda msg: fmt.print(msg),
        )
        project = parser.parse()

        channels: dict[str, object] = {}
        schemas: dict[str, object] = {}

        for src in project.sources:
            props = {}
            for col in src.columns:
                props[col.name] = {"type": _flink_to_json_type(col.type or "STRING")}
                if col.description:
                    props[col.name]["description"] = col.description
            schema_name = f"{src.name}_value"
            schemas[schema_name] = {"type": "object", "properties": props}
            channels[src.topic] = {
                "description": src.description or f"Source: {src.name}",
                "subscribe": {"message": {"$ref": f"#/components/schemas/{schema_name}"}},
            }

        for mdl in project.models:
            cols: list[tuple[str, Optional[str]]] = [
                (col.name, col.type) for col in (mdl.columns or [])
            ]
            if mdl.contract and mdl.contract.columns:
                cols = [(col.name, col.type) for col in mdl.contract.columns]
            props = {}
            for col_name, declared_type in cols:
                col_type = declared_type or "STRING"
                props[col_name] = {"type": _flink_to_json_type(col_type)}
            if props:
                schema_name = f"{mdl.name}_value"
                schemas[schema_name] = {"type": "object", "properties": props}
                tc = mdl.get_topic_config()
                topic = tc.name if tc and tc.name else mdl.name
                channels[topic] = {
                    "description": mdl.description or f"Model: {mdl.name}",
                    "subscribe": {"message": {"$ref": f"#/components/schemas/{schema_name}"}},
                }

        spec = {
            "asyncapi": "2.6.0",
            "info": {"title": project.project.name, "version": project.project.version or "1.0.0"},
            "channels": channels,
            "components": {"schemas": schemas},
        }

        fmt.print(json.dumps(spec, indent=2))
        fmt.set_data({"channels": len(channels), "schemas": len(schemas)})
        fmt.flush()

    except (EnvVarError, ParseError, EnvironmentError) as e:
        handle_parse_error(fmt, e, ErrorCode.PARSE_ERROR)


def _flink_to_json_type(flink_type: str) -> str:
    """Map Flink SQL type to JSON Schema type."""
    base = flink_type.upper().split("(")[0].strip()
    mapping = {
        "STRING": "string",
        "VARCHAR": "string",
        "CHAR": "string",
        "INT": "integer",
        "INTEGER": "integer",
        "SMALLINT": "integer",
        "TINYINT": "integer",
        "BIGINT": "integer",
        "LONG": "integer",
        "FLOAT": "number",
        "DOUBLE": "number",
        "DECIMAL": "number",
        "NUMERIC": "number",
        "BOOLEAN": "boolean",
        "BOOL": "boolean",
        "TIMESTAMP": "string",
        "DATE": "string",
        "TIME": "string",
        "BYTES": "string",
    }
    return mapping.get(base, "string")


@docs.command("schema")
@click.option(
    "--output-file", "-o", type=click.Path(), default=None, help="Write to file instead of stdout"
)
@click.pass_context
def docs_schema(ctx: click.Context, output_file: Optional[str]) -> None:
    """Export JSON Schema for stream_project.yml (derived from Pydantic models)."""
    from streamt.core.models import StreamtProject

    schema = StreamtProject.model_json_schema(
        mode="serialization",
        ref_template="#/$defs/{model}",
    )
    schema["$schema"] = "https://json-schema.org/draft/2020-12/schema"
    schema["title"] = "streamt project configuration"

    text = json.dumps(schema, indent=2) + "\n"
    if output_file:
        from pathlib import Path

        Path(output_file).parent.mkdir(parents=True, exist_ok=True)
        Path(output_file).write_text(text)
        fmt = make_formatter(ctx, "docs schema")
        fmt.print(f"Schema written to {output_file}")
        fmt.flush()
    else:
        click.echo(text, nl=False)


@docs.command("dictionary")
@click.option("--project-dir", "-p", type=click.Path(exists=True), help="Path to project directory")
@click.option("--env", "-e", "environment", help="Target environment")
@click.option(
    "--format",
    "output_fmt",
    type=click.Choice(["csv", "json"]),
    default="csv",
    help="Output format (csv or json)",
)
@click.pass_context
def docs_dictionary(
    ctx: click.Context,
    project_dir: Optional[str],
    environment: Optional[str],
    output_fmt: str,
) -> None:
    """Export data dictionary (all sources and models with columns)."""
    from streamt.core.environment import EnvironmentError
    from streamt.core.parser import EnvVarError, ParseError, ProjectParser

    fmt = make_formatter(ctx, "docs dictionary")
    project_path = get_project_path(project_dir)

    try:
        parser = ProjectParser(
            project_path,
            environment=environment,
            warn_callback=lambda msg: fmt.print(msg),
        )
        project = parser.parse()

        entries: list[dict[str, str]] = []
        for src in project.sources:
            for col in src.columns:
                entries.append(
                    {
                        "resource_type": "source",
                        "resource": src.name,
                        "column": col.name,
                        "type": col.type or "",
                        "classification": col.classification.value if col.classification else "",
                        "description": col.description or "",
                    }
                )
            if not src.columns:
                entries.append(
                    {
                        "resource_type": "source",
                        "resource": src.name,
                        "column": "",
                        "type": "",
                        "classification": "",
                        "description": src.description or "",
                    }
                )

        for mdl in project.models:
            if mdl.contract and mdl.contract.columns:
                for contract_col in mdl.contract.columns:
                    entries.append(
                        {
                            "resource_type": "model",
                            "resource": mdl.name,
                            "column": contract_col.name,
                            "type": contract_col.type or "",
                            "classification": "",
                            "description": contract_col.description or "",
                        }
                    )
            elif mdl.columns:
                for model_col in mdl.columns:
                    entries.append(
                        {
                            "resource_type": "model",
                            "resource": mdl.name,
                            "column": model_col.name,
                            "type": model_col.type or "",
                            "classification": (
                                model_col.classification.value if model_col.classification else ""
                            ),
                            "description": model_col.description or "",
                        }
                    )
            else:
                entries.append(
                    {
                        "resource_type": "model",
                        "resource": mdl.name,
                        "column": "",
                        "type": "",
                        "classification": "",
                        "description": mdl.description or "",
                    }
                )

        if output_fmt == "json":
            fmt.print(json.dumps(entries, indent=2))
        else:
            buf = io.StringIO()
            writer = csv.DictWriter(
                buf,
                fieldnames=[
                    "resource_type",
                    "resource",
                    "column",
                    "type",
                    "classification",
                    "description",
                ],
            )
            writer.writeheader()
            writer.writerows(entries)
            fmt.print(buf.getvalue().rstrip())

        fmt.set_data({"entries": len(entries), "format": output_fmt})
        fmt.flush()

    except (EnvVarError, ParseError, EnvironmentError) as e:
        handle_parse_error(fmt, e, ErrorCode.PARSE_ERROR)
