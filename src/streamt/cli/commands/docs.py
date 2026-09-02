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


@docs.command("asyncapi")
@click.option("--project-dir", "-p", type=click.Path(exists=True), help="Path to project directory")
@click.option("--env", "-e", "environment", help="Target environment")
@click.pass_context
def docs_asyncapi(
    ctx: click.Context,
    project_dir: Optional[str],
    environment: Optional[str],
) -> None:
    """Generate and validate an AsyncAPI 3.1 document for Kafka channels."""
    _emit_asyncapi(ctx, project_dir, environment, "docs asyncapi")


@docs.command("openapi")
@click.option("--project-dir", "-p", type=click.Path(exists=True), help="Path to project directory")
@click.option("--env", "-e", "environment", help="Target environment")
@click.pass_context
def docs_openapi(
    ctx: click.Context,
    project_dir: Optional[str],
    environment: Optional[str],
) -> None:
    """Deprecated alias for `docs asyncapi`; this does not emit OpenAPI."""
    _emit_asyncapi(ctx, project_dir, environment, "docs openapi")


def _emit_asyncapi(
    ctx: click.Context,
    project_dir: Optional[str],
    environment: Optional[str],
    command_name: str,
) -> None:
    """Generate the same AsyncAPI document for the canonical and alias commands."""
    from streamt.core.environment import EnvironmentError
    from streamt.core.parser import EnvVarError, ParseError, ProjectParser
    from streamt.docs.asyncapi import (
        AsyncAPIGenerationError,
        AsyncAPIValidationError,
        generate_asyncapi_document,
    )

    fmt = make_formatter(ctx, command_name)
    project_path = get_project_path(project_dir)

    def parser_warning(message: str) -> None:
        """Keep warnings off the raw document stream while preserving JSON metadata."""
        if fmt.format == "json":
            fmt.print_warning(message)
        elif not fmt.quiet:
            fmt.stderr.print(f"[yellow]WARNING[/yellow]: {message}")

    try:
        parser = ProjectParser(
            project_path,
            environment=environment,
            warn_callback=parser_warning,
        )
        project = parser.parse()
        document = generate_asyncapi_document(project)
        components = document["components"]
        if not isinstance(components, dict):
            raise AsyncAPIValidationError("Generated AsyncAPI components must be an object")
        schemas = components.get("schemas")
        if not isinstance(schemas, dict):
            raise AsyncAPIValidationError("Generated AsyncAPI schemas must be an object")
        channels = document["channels"]
        operations = document["operations"]
        if not isinstance(channels, dict) or not isinstance(operations, dict):
            raise AsyncAPIValidationError("Generated AsyncAPI channels/operations must be objects")

        if fmt.format == "text" and not fmt.quiet:
            click.echo(json.dumps(document, indent=2, sort_keys=True))
        fmt.set_data(
            {
                "asyncapi": document["asyncapi"],
                "channels": len(channels),
                "operations": len(operations),
                "schemas": len(schemas),
                "document": document,
            }
        )
        fmt.flush()

    except (EnvVarError, ParseError, EnvironmentError) as e:
        handle_parse_error(fmt, e, ErrorCode.PARSE_ERROR)
    except (AsyncAPIGenerationError, AsyncAPIValidationError) as e:
        handle_parse_error(fmt, e, ErrorCode.ASYNCAPI_INVALID)


def _flink_to_json_type(flink_type: str) -> str:
    """Compatibility helper for callers that only need the JSON root type."""
    from streamt.docs.asyncapi import flink_type_to_asyncapi_schema

    json_type = flink_type_to_asyncapi_schema(flink_type).get("type")
    if not isinstance(json_type, str):
        raise ValueError(f"Flink type {flink_type!r} did not produce a JSON Schema type")
    return json_type


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
