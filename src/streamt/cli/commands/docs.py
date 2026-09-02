"""streamt docs commands."""

from __future__ import annotations

import csv
import io
import json
import os
import re
import tempfile
from pathlib import Path
from typing import NoReturn, Optional

import click
import yaml

from streamt.cli.helpers import (
    get_project_path,
    handle_parse_error,
    make_formatter,
    redact_sensitive_text,
)
from streamt.core.errors import ErrorCode
from streamt.output import OutputFormatter, StructuredError, StructuredWarning

_ODCS_VALIDATION_LOCATION_RE = re.compile(r"validation failed at ([^:]+):")
_OPENLINEAGE_CORE_SCHEMA_VERSION = "2-0-2"
_PARSER_WARNING_PREFIX = "[yellow]WARNING[/yellow]: "


class _ODCSCommandError(ValueError):
    """A safe command-boundary ODCS failure with an optional stable location."""

    def __init__(self, message: str, *, location: str | None = None) -> None:
        super().__init__(message)
        self.location = location


class _OpenLineageCommandError(ValueError):
    """A secret-neutral OpenLineage command failure with a stable location."""

    def __init__(self, message: str, *, location: str) -> None:
        super().__init__(message)
        self.location = location


class _ODCSSafeDumper(yaml.SafeDumper):
    """Safe YAML dumper that never emits aliases into standalone documents."""

    def ignore_aliases(self, _data: object) -> bool:
        return True


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


@docs.command("odcs")
@click.option("--contract-id", help="Explicit ODCS contract identity")
@click.option("--status", help="Explicit ODCS lifecycle status")
@click.option(
    "--contract-version",
    help="ODCS contract version (defaults to the exact project version)",
)
@click.option(
    "--format",
    "serialization",
    type=click.Choice(["yaml", "json"]),
    default="yaml",
    show_default=True,
    help="Raw document serialization",
)
@click.option(
    "--output-file",
    type=click.Path(),
    help="Atomically write the validated document to this file",
)
@click.option("--project-dir", "-p", type=click.Path(exists=True), help="Path to project directory")
@click.option("--env", "-e", "environment", help="Target environment")
@click.pass_context
def docs_odcs(
    ctx: click.Context,
    contract_id: str | None,
    status: str | None,
    contract_version: str | None,
    serialization: str,
    output_file: str | None,
    project_dir: str | None,
    environment: str | None,
) -> None:
    """Generate and validate one project-wide ODCS 3.1 document."""
    from streamt.core.environment import EnvironmentError
    from streamt.core.parser import EnvVarError, ParseError, ProjectParser
    from streamt.docs.odcs import (
        ODCS_VERSION,
        ODCSGenerationError,
        ODCSValidationError,
        generate_odcs_document,
    )

    fmt = make_formatter(ctx, "docs odcs")
    project_path = get_project_path(project_dir)

    def emit_warning(message: str) -> None:
        _emit_odcs_warning(fmt, "W000_WARNING", message)

    try:
        exact_contract_id = _require_odcs_option(
            contract_id,
            label="contract ID",
            location="contract_id",
        )
        exact_status = _require_odcs_option(
            status,
            label="contract status",
            location="status",
        )

        parser = ProjectParser(
            project_path,
            environment=environment,
            warn_callback=emit_warning,
        )
        project = parser.parse()
        if contract_version is None and project.project.version is None:
            raise _ODCSCommandError(
                "ODCS contract version is required; set project.version or pass "
                "--contract-version",
                location="project.version",
            )
        version_location = (
            "contract_version" if contract_version is not None else "project.version"
        )
        exact_version = _require_odcs_option(
            contract_version if contract_version is not None else project.project.version,
            label="contract version",
            location=version_location,
        )

        export = generate_odcs_document(
            project,
            contract_id=exact_contract_id,
            status=exact_status,
            contract_version=exact_version,
        )
        for warning in export.warnings:
            _emit_odcs_warning(
                fmt,
                warning.code,
                warning.message,
                location=warning.location,
            )

        try:
            rendered = _serialize_odcs_document(export.document, serialization)
        except Exception as error:
            raise _ODCSCommandError(
                "Could not serialize ODCS document",
                location="document",
            ) from error

        rendered_output_file: str | None = None
        if output_file is not None:
            target = Path(output_file)
            try:
                _atomic_write_odcs(target, rendered)
            except Exception as error:
                raise _ODCSCommandError(
                    "Could not write ODCS output file atomically",
                    location="output_file",
                ) from error
            rendered_output_file = str(target)
        elif fmt.format == "text" and not fmt.quiet:
            try:
                click.echo(rendered, nl=False)
            except OSError as error:
                raise _ODCSCommandError(
                    "Could not write ODCS document to stdout",
                    location="stdout",
                ) from error

        fmt.set_data(
            {
                "standard": "odcs",
                "standard_version": ODCS_VERSION,
                "document": export.document,
                "serialization": serialization,
                "output_file": rendered_output_file,
            }
        )
        if output_file is not None and fmt.format == "text" and not fmt.quiet:
            # Paths are machine-significant confirmation data. Rich may insert
            # hard line breaks at the terminal width, even in captured output.
            click.echo(f"ODCS document written to {output_file}")
        fmt.flush()

    except (EnvVarError, ParseError, EnvironmentError) as error:
        _fail_odcs_command(fmt, error, ErrorCode.PARSE_ERROR)
    except _ODCSCommandError as error:
        _fail_odcs_command(
            fmt,
            error,
            ErrorCode.ODCS_INVALID,
            location=error.location,
        )
    except (ODCSGenerationError, ODCSValidationError) as error:
        _fail_odcs_command(
            fmt,
            error,
            ErrorCode.ODCS_INVALID,
            location=_odcs_validation_location(error),
        )


@docs.command("openlineage")
@click.option("--job-namespace", help="OpenLineage job namespace")
@click.option("--kafka-namespace", help="Kafka dataset namespace (kafka://host:port)")
@click.option(
    "--gateway-namespace",
    help="Gateway dataset namespace (kafka://host:port)",
)
@click.option(
    "--output-file",
    type=click.Path(),
    help="Atomically write validated OpenLineage JSONL to this file",
)
@click.option("--project-dir", "-p", type=click.Path(exists=True), help="Path to project directory")
@click.option("--env", "-e", "environment", help="Target environment")
@click.pass_context
def docs_openlineage(
    ctx: click.Context,
    job_namespace: str | None,
    kafka_namespace: str | None,
    gateway_namespace: str | None,
    output_file: str | None,
    project_dir: str | None,
    environment: str | None,
) -> None:
    """Export deterministic OpenLineage 1.53.0 design metadata as JSONL."""
    from streamt.compiler import Compiler
    from streamt.core.environment import EnvironmentError
    from streamt.core.parser import EnvVarError, ParseError, ProjectParser
    from streamt.integrations.openlineage import (
        OPENLINEAGE_RELEASE,
        OpenLineageNamespaceError,
        OpenLineageStaticError,
        build_static_export,
        resolve_openlineage_namespaces,
        serialize_static_jsonl,
        static_namespace_requirements,
    )

    fmt = make_formatter(ctx, "docs openlineage")
    project_path = get_project_path(project_dir)

    def parser_warning(message: str) -> None:
        _emit_openlineage_warning(
            fmt,
            "W000_WARNING",
            _normalize_parser_warning(message),
        )

    try:
        parser = ProjectParser(
            project_path,
            environment=environment,
            warn_callback=parser_warning,
        )
        project = parser.parse()

        # Namespace environment values are intentionally read only after parse:
        # ProjectParser is the owner of project and environment-specific .env loading.
        selected_job_namespace = _option_or_environment(
            job_namespace,
            "OPENLINEAGE_NAMESPACE",
        )
        selected_kafka_namespace = _option_or_environment(
            kafka_namespace,
            "STREAMT_OPENLINEAGE_KAFKA_NAMESPACE",
        )
        selected_gateway_namespace = _option_or_environment(
            gateway_namespace,
            "STREAMT_OPENLINEAGE_GATEWAY_NAMESPACE",
        )

        # Fail on missing job identity or any explicitly selected dataset
        # identity before doing compiler work. Dataset derivation remains lazy
        # until the successful compile tells us which namespace kinds are used.
        selected_namespaces = resolve_openlineage_namespaces(
            job_namespace=selected_job_namespace,
            kafka_namespace=selected_kafka_namespace,
            gateway_namespace=selected_gateway_namespace,
            kafka_bootstrap=None,
            gateway_bootstrap=None,
            require_kafka=False,
            require_gateway=False,
        )

        compiler = Compiler(project)
        try:
            manifest = compiler.compile(dry_run=True)
        except Exception as error:
            raise _OpenLineageCommandError(
                "Could not compile project for OpenLineage export",
                location="models",
            ) from error

        requirements = static_namespace_requirements(project, compiler.compiled_models)
        gateway = (
            project.runtime.conduktor.gateway
            if project.runtime.conduktor is not None
            else None
        )
        namespaces = resolve_openlineage_namespaces(
            job_namespace=selected_namespaces.job,
            kafka_namespace=selected_namespaces.kafka,
            gateway_namespace=selected_namespaces.gateway,
            kafka_bootstrap=project.runtime.kafka.bootstrap_servers,
            gateway_bootstrap=gateway.proxy_bootstrap if gateway is not None else None,
            require_kafka=requirements.kafka,
            require_gateway=requirements.gateway,
        )
        export = build_static_export(
            project,
            manifest,
            compiler.resolved_models,
            compiler.compiled_models,
            job_namespace=namespaces.job,
            kafka_namespace=namespaces.kafka,
            gateway_namespace=namespaces.gateway,
        )
        for warning in export.warnings:
            _emit_openlineage_warning(
                fmt,
                warning.code,
                warning.message,
                location=warning.location,
            )

        # Validate and render the complete sequence before any externally visible write.
        rendered = serialize_static_jsonl(export.events)

        rendered_output_file: str | None = None
        if output_file is not None:
            target = Path(output_file)
            try:
                _atomic_write_openlineage(target, rendered)
            except Exception as error:
                raise _OpenLineageCommandError(
                    "Could not write OpenLineage output file atomically",
                    location="output_file",
                ) from error
            rendered_output_file = str(target)
        elif fmt.format == "text" and not fmt.quiet:
            try:
                click.echo(rendered, nl=False)
            except OSError as error:
                raise _OpenLineageCommandError(
                    "Could not write OpenLineage events to stdout",
                    location="stdout",
                ) from error

        data: dict[str, object] = {
            "standard": "OpenLineage",
            "release": OPENLINEAGE_RELEASE,
            "core_schema": _OPENLINEAGE_CORE_SCHEMA_VERSION,
            "events": list(export.events),
            "counts": {
                "total": len(export.events),
                "datasets": export.dataset_count,
                "jobs": export.job_count,
            },
        }
        if rendered_output_file is not None:
            data["output_file"] = rendered_output_file
        fmt.set_data(data)

        if output_file is not None and fmt.format == "text" and not fmt.quiet:
            click.echo(f"OpenLineage events written to {output_file}")
        fmt.flush()

    except (EnvVarError, ParseError, EnvironmentError) as error:
        _fail_openlineage_command(fmt, error, ErrorCode.PARSE_ERROR)
    except (OpenLineageNamespaceError, OpenLineageStaticError) as error:
        _fail_openlineage_command(
            fmt,
            error,
            ErrorCode.OPENLINEAGE_INVALID,
            location=error.location,
        )
    except _OpenLineageCommandError as error:
        _fail_openlineage_command(
            fmt,
            error,
            ErrorCode.OPENLINEAGE_INVALID,
            location=error.location,
        )
    except Exception:
        generic = _OpenLineageCommandError(
            "Could not generate validated OpenLineage export",
            location="events",
        )
        _fail_openlineage_command(
            fmt,
            generic,
            ErrorCode.OPENLINEAGE_INVALID,
            location=generic.location,
        )


def _require_odcs_option(
    value: str | None,
    *,
    label: str,
    location: str,
) -> str:
    """Require a non-blank semantic option without delegating to Click usage errors."""
    if value is None or not value.strip():
        raise _ODCSCommandError(
            f"ODCS {label} must contain a non-whitespace character",
            location=location,
        )
    return value


def _serialize_odcs_document(document: dict[str, object], serialization: str) -> str:
    """Serialize an already validated ODCS document with deterministic bytes."""
    if serialization == "json":
        return (
            json.dumps(
                document,
                ensure_ascii=False,
                allow_nan=False,
                indent=2,
            )
            + "\n"
        )
    if serialization != "yaml":
        raise ValueError(f"Unsupported ODCS serialization {serialization!r}")
    rendered = yaml.dump(
        document,
        Dumper=_ODCSSafeDumper,
        allow_unicode=True,
        default_flow_style=False,
        sort_keys=False,
    )
    return rendered if rendered.endswith("\n") else rendered + "\n"


def _atomic_write_odcs(path: Path, content: str) -> None:
    """Atomically replace an explicit ODCS output path and clean every staging file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=path.parent,
            prefix=f".{path.name}.",
            suffix=".tmp",
            delete=False,
        ) as temp_file:
            temp_path = Path(temp_file.name)
            temp_file.write(content)
            temp_file.flush()
            os.fsync(temp_file.fileno())
        os.replace(temp_path, path)
        temp_path = None
    finally:
        if temp_path is not None:
            try:
                temp_path.unlink(missing_ok=True)
            except OSError:
                pass


def _atomic_write_openlineage(path: Path, content: str) -> None:
    """Atomically replace an OpenLineage JSONL path and clean staging files."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=path.parent,
            prefix=f".{path.name}.",
            suffix=".tmp",
            delete=False,
        ) as temp_file:
            temp_path = Path(temp_file.name)
            temp_file.write(content)
            temp_file.flush()
            os.fsync(temp_file.fileno())
        os.replace(temp_path, path)
        temp_path = None
    finally:
        if temp_path is not None:
            try:
                temp_path.unlink(missing_ok=True)
            except OSError:
                pass


def _option_or_environment(option: str | None, environment_name: str) -> str | None:
    """Apply exact option-over-environment precedence without truthiness fallback."""
    return option if option is not None else os.environ.get(environment_name)


def _normalize_parser_warning(message: str) -> str:
    """Remove the parser's display-only prefix before structured warning capture."""
    if message.startswith(_PARSER_WARNING_PREFIX):
        return message[len(_PARSER_WARNING_PREFIX) :]
    return message


def _emit_openlineage_warning(
    fmt: OutputFormatter,
    code: str,
    message: str,
    *,
    location: str | None = None,
) -> None:
    """Capture warnings once; only raw text mode mirrors them to stderr."""
    safe_message = redact_sensitive_text(message)
    fmt.add_warning(
        StructuredWarning(
            code=code,
            message=safe_message,
            location=location,
        )
    )
    if fmt.format == "text" and not fmt.quiet:
        fmt.stderr.print(f"[yellow]WARNING[/yellow]: {safe_message}")


def _fail_openlineage_command(
    fmt: OutputFormatter,
    error: Exception,
    code: str,
    *,
    location: str | None = None,
) -> NoReturn:
    """Emit one credential-redacted standard CLI error and exit non-zero."""
    safe_message = redact_sensitive_text(error)
    fmt.add_error(
        StructuredError(
            code=code,
            message=safe_message,
            location=location,
        )
    )
    fmt.print_error(safe_message)
    fmt.flush()
    raise click.exceptions.Exit(1)


def _emit_odcs_warning(
    fmt: OutputFormatter,
    code: str,
    message: str,
    *,
    location: str | None = None,
) -> None:
    """Keep warning text off raw stdout while retaining structured metadata."""
    safe_message = redact_sensitive_text(message)
    fmt.add_warning(
        StructuredWarning(
            code=code,
            message=safe_message,
            location=location,
        )
    )
    if not fmt.quiet:
        fmt.stderr.print(f"[yellow]WARNING[/yellow]: {safe_message}")


def _odcs_validation_location(error: Exception) -> str | None:
    """Extract the JSON Pointer already normalized by the pure validator."""
    match = _ODCS_VALIDATION_LOCATION_RE.search(str(error))
    return match.group(1) if match else None


def _fail_odcs_command(
    fmt: OutputFormatter,
    error: Exception,
    code: str,
    *,
    location: str | None = None,
) -> NoReturn:
    """Emit one credential-redacted standard CLI error and exit non-zero."""
    safe_message = redact_sensitive_text(error)
    fmt.add_error(
        StructuredError(
            code=code,
            message=safe_message,
            location=location,
        )
    )
    fmt.print_error(safe_message)
    fmt.flush()
    raise click.exceptions.Exit(1)


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
