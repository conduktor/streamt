"""Additive, read-only import of existing Kafka topics as external sources."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import click
import yaml
from pydantic import ValidationError

from streamt.cli.helpers import (
    close_deployers,
    get_project_path,
    make_formatter,
    make_kafka_deployer,
    make_sr_deployer,
)
from streamt.core.errors import ErrorCode
from streamt.core.models import Source
from streamt.discovery import DiscoveredTopic, discover_topics
from streamt.output import StructuredError

DEFAULT_IMPORT_FILE = Path("sources/imported.kafka.yml")


@dataclass(frozen=True)
class ImportCommandError(ValueError):
    """An import precondition failed before any declaration was written."""

    code: str
    message: str
    suggestion: Optional[str] = None

    def __str__(self) -> str:
        return self.message


def _plain_parser_warning(message: str) -> str:
    prefix = "[yellow]WARNING[/yellow]: "
    return message.removeprefix(prefix)


def _resolve_output_path(project_path: Path, requested: Path) -> tuple[Path, str]:
    """Resolve a declaration target below the project's sources directory."""
    target = requested if requested.is_absolute() else project_path / requested
    target = target.resolve()
    try:
        relative = target.relative_to(project_path)
    except ValueError as exc:
        raise ImportCommandError(
            ErrorCode.IMPORT_PATH_INVALID,
            f"Import output must stay inside the project: {requested}",
            "Choose a path such as sources/imported.kafka.yml.",
        ) from exc

    if len(relative.parts) != 2 or relative.parts[0] != "sources":
        raise ImportCommandError(
            ErrorCode.IMPORT_PATH_INVALID,
            f"Import output must be a direct declaration in the sources directory: {relative}",
            "Choose a path such as sources/imported.kafka.yml.",
        )
    if target.suffix not in {".yml", ".yaml"}:
        raise ImportCommandError(
            ErrorCode.IMPORT_PATH_INVALID,
            f"Import output must use a .yml or .yaml extension: {relative}",
        )
    if target.exists() and target.is_dir():
        raise ImportCommandError(
            ErrorCode.IMPORT_PATH_INVALID,
            f"Import output is a directory, not a declaration file: {relative}",
        )
    return target, relative.as_posix()


def _source_definition(resource: DiscoveredTopic) -> dict[str, object]:
    """Build the concise, explicit external declaration for one topic."""
    definition: dict[str, object] = {
        "name": resource.source["name"],
        "topic": resource.topic,
        "ownership": {"mode": "external"},
    }
    if resource.schema is not None:
        definition["schema"] = {
            "registry": "confluent",
            "subject": resource.schema.subject,
            "version": resource.schema.version,
            "format": resource.schema.format,
        }
    columns = resource.source.get("columns")
    if isinstance(columns, list) and columns:
        definition["columns"] = columns
    return definition


def _serialize_strict_sources(definitions: list[dict[str, object]]) -> str:
    """Strictly validate generated declarations before and after YAML encoding."""
    try:
        for definition in definitions:
            Source.model_validate(definition)
    except ValidationError as exc:
        raise ImportCommandError(
            ErrorCode.IMPORT_VALIDATION_FAILED,
            f"Generated source declaration is invalid: {exc}",
        ) from exc

    document: dict[str, object] = {"sources": definitions}
    serialized = yaml.safe_dump(
        document,
        default_flow_style=False,
        sort_keys=False,
        allow_unicode=True,
    )
    try:
        decoded = yaml.safe_load(serialized)
        if not isinstance(decoded, dict) or set(decoded) != {"sources"}:
            raise ValueError("generated YAML must contain only a sources section")
        decoded_sources = decoded.get("sources")
        if not isinstance(decoded_sources, list):
            raise ValueError("generated YAML sources section must be a list")
        for definition in decoded_sources:
            Source.model_validate(definition)
    except (ValueError, ValidationError, yaml.YAMLError) as exc:
        raise ImportCommandError(
            ErrorCode.IMPORT_VALIDATION_FAILED,
            f"Generated source YAML failed strict round-trip validation: {exc}",
        ) from exc
    return serialized


def _name_collisions(
    resources: list[DiscoveredTopic],
    existing_sources: list[Source],
    existing_model_names: set[str],
) -> list[str]:
    """Describe generated names that would make the combined project ambiguous."""
    existing_by_name: dict[str, list[str]] = {}
    for source in existing_sources:
        existing_by_name.setdefault(source.name, []).append(source.topic)

    generated_by_name: dict[str, list[str]] = {}
    for resource in resources:
        source_name = resource.source.get("name")
        if isinstance(source_name, str):
            generated_by_name.setdefault(source_name, []).append(resource.topic)

    collisions: list[str] = []
    for source_name in sorted(generated_by_name):
        topics = sorted(generated_by_name[source_name])
        if len(topics) > 1:
            collisions.append(f"{source_name!r} is generated by {', '.join(topics)}")
        if source_name in existing_by_name:
            existing_topics = ", ".join(sorted(existing_by_name[source_name]))
            collisions.append(
                f"{source_name!r} already declares {existing_topics} and would also name "
                f"{', '.join(topics)}"
            )
        if source_name in existing_model_names:
            collisions.append(
                f"{source_name!r} is already used by a model and would also name "
                f"{', '.join(topics)}"
            )
    return collisions


def _resource_data(
    resource: DiscoveredTopic,
    *,
    source_name: str,
    disposition: str,
) -> dict[str, object]:
    """Return one stable machine-readable observation."""
    return {
        "topic": resource.topic,
        "source_name": source_name,
        "partitions": resource.partitions,
        "replication_factor": resource.replication_factor,
        "schema": resource.schema.to_dict() if resource.schema is not None else None,
        "disposition": disposition,
    }


def _result_data(
    *,
    environment: str,
    dry_run: bool,
    output_file: str,
    target_exists: bool,
    written: bool,
    definitions: list[dict[str, object]],
    resources: list[dict[str, object]],
) -> dict[str, object]:
    imported_count = sum(item["disposition"] == "imported" for item in resources)
    skipped_count = len(resources) - imported_count
    return {
        "backend": "kafka",
        "environment": environment,
        "dry_run": dry_run,
        "output_file": output_file,
        "target_exists": target_exists,
        "written": written,
        "discovered_count": len(resources),
        "imported_count": imported_count,
        "skipped_count": skipped_count,
        "sources": definitions,
        "resources": resources,
        "created_files": [output_file] if written else [],
    }


def _write_exclusive(target: Path, content: str) -> None:
    """Create a fully rendered declaration without ever replacing a path."""
    try:
        target.parent.mkdir(parents=True, exist_ok=True)
        with target.open("x", encoding="utf-8") as output:
            output.write(content)
    except FileExistsError as exc:
        raise ImportCommandError(
            ErrorCode.IMPORT_TARGET_EXISTS,
            f"Import target already exists and was not changed: {target}",
            "Choose another --output-file or merge the preview manually.",
        ) from exc
    except OSError as exc:
        raise ImportCommandError(
            ErrorCode.IMPORT_WRITE_FAILED,
            f"Could not create import declaration {target}: {exc}",
        ) from exc


@click.command("import")
@click.option(
    "--project-dir",
    "-p",
    type=click.Path(file_okay=False, path_type=Path),
    default=Path("."),
    help="Existing streamt project directory",
)
@click.option(
    "--env",
    "-e",
    "environment",
    help="Target environment (reads from STREAMT_ENV if not set)",
)
@click.option(
    "--include",
    "include_patterns",
    multiple=True,
    help="Include topics matching this glob (repeatable)",
)
@click.option(
    "--exclude",
    "exclude_patterns",
    multiple=True,
    help="Exclude topics matching this glob (repeatable)",
)
@click.option(
    "--output-file",
    type=click.Path(dir_okay=False, path_type=Path),
    default=DEFAULT_IMPORT_FILE,
    show_default=True,
    help="New source declaration file below sources/",
)
@click.option(
    "--schemas/--no-schemas",
    default=True,
    help="Enrich from the configured Schema Registry when available",
)
@click.option("--dry-run", is_flag=True, help="Preview declarations without writing")
@click.pass_context
def import_resources(
    ctx: click.Context,
    project_dir: Path,
    environment: Optional[str],
    include_patterns: tuple[str, ...],
    exclude_patterns: tuple[str, ...],
    output_file: Path,
    schemas: bool,
    dry_run: bool,
) -> None:
    """Import existing Kafka topics as external source declarations."""
    from streamt.core.environment import EnvironmentError
    from streamt.core.parser import EnvVarError, ParseError, ProjectParser

    fmt = make_formatter(ctx, "import")
    project_path = get_project_path(str(project_dir))
    kafka = None
    schema_registry = None
    data: dict[str, object] = {}

    try:
        parser = ProjectParser(
            project_path,
            environment=environment,
            warn_callback=lambda message: fmt.print_warning(_plain_parser_warning(message)),
        )
        project = parser.parse()
        parsed_environment = parser.env_config.environment.name if parser.env_config else None
        effective_environment = (
            parsed_environment
            if isinstance(parsed_environment, str) and parsed_environment
            else "default"
        )
        target, relative_target = _resolve_output_path(project_path, output_file)
        target_exists = target.exists()

        kafka = make_kafka_deployer(project, fmt)
        if kafka is None:
            raise ImportCommandError(
                ErrorCode.IMPORT_DISCOVERY_FAILED,
                "Kafka is not configured or reachable; import requires read-only topic metadata.",
                "Check runtime.kafka for the selected environment.",
            )

        if schemas and project.runtime.schema_registry is not None:
            schema_registry = make_sr_deployer(project, fmt, required=False)
            if schema_registry is not None:
                try:
                    schema_registry.list_subjects()
                except Exception as exc:
                    fmt.print_warning(
                        "Schema Registry enrichment is unavailable "
                        f"({type(exc).__name__}); importing Kafka metadata only.",
                        code=ErrorCode.SCHEMA_ENRICHMENT_SKIPPED,
                    )
                    close_deployers(schema_registry)
                    schema_registry = None

        try:
            discovered = discover_topics(
                kafka,
                schema_registry,
                include=include_patterns or None,
                exclude=exclude_patterns or None,
            )
        except Exception as exc:
            raise ImportCommandError(
                ErrorCode.IMPORT_DISCOVERY_FAILED,
                "Kafka topic discovery failed "
                f"({type(exc).__name__}); no declaration was written.",
                "Check runtime.kafka connectivity, authentication, and topic permissions.",
            ) from exc

        existing_by_topic = {source.topic: source for source in project.sources}
        new_resources: list[DiscoveredTopic] = []
        observations: list[dict[str, object]] = []
        for resource in discovered:
            if resource.schema_error:
                fmt.print_warning(
                    f"Schema enrichment skipped for {resource.topic!r}: "
                    f"{resource.schema_error}.",
                    code=ErrorCode.SCHEMA_ENRICHMENT_SKIPPED,
                )
            existing = existing_by_topic.get(resource.topic)
            if existing is not None:
                observations.append(
                    _resource_data(
                        resource,
                        source_name=existing.name,
                        disposition="already_declared",
                    )
                )
                continue
            new_resources.append(resource)
            source_name = resource.source.get("name")
            observations.append(
                _resource_data(
                    resource,
                    source_name=source_name if isinstance(source_name, str) else "",
                    disposition="imported",
                )
            )

        collisions = _name_collisions(
            new_resources,
            project.sources,
            {model.name for model in project.models},
        )
        if collisions:
            raise ImportCommandError(
                ErrorCode.IMPORT_NAME_COLLISION,
                "Import source-name collision: " + "; ".join(collisions),
                "Choose narrower --include/--exclude filters and rename explicitly after preview.",
            )

        definitions = [_source_definition(resource) for resource in new_resources]
        serialized = _serialize_strict_sources(definitions) if definitions else ""
        data = _result_data(
            environment=effective_environment,
            dry_run=dry_run,
            output_file=relative_target,
            target_exists=target_exists,
            written=False,
            definitions=definitions,
            resources=observations,
        )
        fmt.set_data(data)

        if not definitions:
            fmt.print("[green]No new Kafka topics to import.[/green]")
            fmt.flush()
            return

        if target_exists:
            if dry_run:
                fmt.print_warning(
                    f"Import target {relative_target} exists; a real import would refuse to "
                    "overwrite it.",
                    code=ErrorCode.IMPORT_TARGET_EXISTS_WARNING,
                )
            else:
                raise ImportCommandError(
                    ErrorCode.IMPORT_TARGET_EXISTS,
                    f"Import target already exists and was not changed: {relative_target}",
                    "Choose another --output-file or merge a --dry-run preview manually.",
                )

        if not dry_run:
            _write_exclusive(target, serialized)
            data["written"] = True
            data["created_files"] = [relative_target]
            fmt.set_data(data)
            fmt.print(
                f"[green]Imported {len(definitions)} external source(s) into "
                f"{relative_target}.[/green]"
            )
        else:
            fmt.print(f"Would import {len(definitions)} external source(s) into {relative_target}.")
        fmt.flush()

    except EnvVarError as exc:
        fmt.add_error(StructuredError(code=ErrorCode.ENV_VAR_ERROR, message=str(exc)))
        fmt.print_error(str(exc))
        fmt.flush()
        raise click.exceptions.Exit(1) from exc
    except ParseError as exc:
        fmt.add_error(StructuredError(code=ErrorCode.PARSE_ERROR, message=str(exc)))
        fmt.print_error(str(exc))
        fmt.flush()
        raise click.exceptions.Exit(1) from exc
    except EnvironmentError as exc:
        fmt.add_error(StructuredError(code=ErrorCode.ENVIRONMENT_ERROR, message=str(exc)))
        fmt.print_error(str(exc))
        fmt.flush()
        raise click.exceptions.Exit(1) from exc
    except ImportCommandError as exc:
        if data:
            fmt.set_data(data)
        fmt.add_error(
            StructuredError(
                code=exc.code,
                message=exc.message,
                suggestion=exc.suggestion,
            )
        )
        fmt.print_error(exc.message)
        fmt.flush()
        raise click.exceptions.Exit(1) from exc
    except KeyboardInterrupt as exc:
        fmt.print_error("Interrupted.")
        fmt.flush()
        raise click.exceptions.Exit(130) from exc
    finally:
        close_deployers(schema_registry, kafka)
