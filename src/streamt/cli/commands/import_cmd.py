"""Additive, read-only import of existing Kafka topics as external sources."""

from __future__ import annotations

import errno
import os
import secrets
import stat
from collections import Counter
from collections.abc import Iterator
from contextlib import contextmanager
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
    redact_sensitive_text,
)
from streamt.core.errors import ErrorCode
from streamt.core.models import Source, StreamtProject
from streamt.core.validator import ProjectValidator, ValidationMessage
from streamt.discovery import DiscoveredTopic, discover_topics
from streamt.output import StructuredError

DEFAULT_IMPORT_FILE = Path("sources/imported.kafka.yml")
_DIRECTORY_OPEN_FLAGS = (
    os.O_RDONLY
    | getattr(os, "O_DIRECTORY", 0)
    | getattr(os, "O_NOFOLLOW", 0)
    | getattr(os, "O_CLOEXEC", 0)
)
_FILE_CREATE_FLAGS = (
    os.O_WRONLY
    | os.O_CREAT
    | os.O_EXCL
    | getattr(os, "O_NOFOLLOW", 0)
    | getattr(os, "O_CLOEXEC", 0)
)


@dataclass
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
    """Lexically resolve one direct declaration below the project sources directory."""
    target = requested if requested.is_absolute() else project_path / requested
    target = Path(os.path.abspath(os.fspath(target)))
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


def _validation_key(message: ValidationMessage) -> tuple[str, Optional[str]]:
    """Use stable validator identity; diagnostic context may change with candidates."""
    return message.code, message.location


def _new_project_validation_errors(
    project: StreamtProject,
    definitions: list[dict[str, object]],
) -> list[ValidationMessage]:
    """Return only validation errors introduced by the proposed sources."""
    generated_sources = [Source.model_validate(definition) for definition in definitions]
    baseline = ProjectValidator(project).validate().errors
    baseline_counts = Counter(_validation_key(message) for message in baseline)

    candidate = project.model_copy(
        deep=True,
        update={"sources": [*project.sources, *generated_sources]},
    )
    introduced: list[ValidationMessage] = []
    for message in ProjectValidator(candidate).validate().errors:
        key = _validation_key(message)
        if baseline_counts[key] > 0:
            baseline_counts[key] -= 1
        else:
            introduced.append(message)
    return introduced


def _validate_prospective_project(
    project: StreamtProject,
    definitions: list[dict[str, object]],
) -> None:
    """Fail before creation when proposed sources introduce project errors."""
    introduced = _new_project_validation_errors(project, definitions)
    if not introduced:
        return
    details = "; ".join(f"{message.code}: {message.message}" for message in introduced)
    raise ImportCommandError(
        ErrorCode.IMPORT_VALIDATION_FAILED,
        f"Imported sources would make the project invalid: {details}",
        "Adjust import filters or enrich the declarations in a dry-run preview.",
    )


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


def _close_fd_quietly(fd: int) -> None:
    try:
        os.close(fd)
    except OSError:
        pass


def _verify_sources_binding(project_fd: int, sources_fd: int) -> None:
    """Ensure the opened directory is still the project's direct sources child."""
    try:
        current = os.stat("sources", dir_fd=project_fd, follow_symlinks=False)
        opened = os.fstat(sources_fd)
    except OSError as exc:
        raise ImportCommandError(
            ErrorCode.IMPORT_PATH_INVALID,
            "The project sources directory changed while preparing the import.",
            "Retry after ensuring sources/ is a stable, non-symlink directory.",
        ) from exc

    if not stat.S_ISDIR(current.st_mode) or (current.st_dev, current.st_ino) != (
        opened.st_dev,
        opened.st_ino,
    ):
        raise ImportCommandError(
            ErrorCode.IMPORT_PATH_INVALID,
            "The project sources directory changed while preparing the import.",
            "Retry after ensuring sources/ is a stable, non-symlink directory.",
        )


@contextmanager
def _verified_sources_directory(
    project_path: Path,
    *,
    create: bool,
) -> Iterator[Optional[tuple[int, int]]]:
    """Open project/sources without following its final path components."""
    project_fd = -1
    sources_fd = -1
    missing = False
    try:
        project_fd = os.open(project_path, _DIRECTORY_OPEN_FLAGS)
        try:
            sources_fd = os.open("sources", _DIRECTORY_OPEN_FLAGS, dir_fd=project_fd)
        except FileNotFoundError:
            if not create:
                missing = True
            else:
                try:
                    os.mkdir("sources", mode=0o755, dir_fd=project_fd)
                except FileExistsError:
                    pass
                sources_fd = os.open("sources", _DIRECTORY_OPEN_FLAGS, dir_fd=project_fd)
        if not missing:
            _verify_sources_binding(project_fd, sources_fd)
    except ImportCommandError:
        if sources_fd >= 0:
            _close_fd_quietly(sources_fd)
        if project_fd >= 0:
            _close_fd_quietly(project_fd)
        raise
    except OSError as exc:
        if sources_fd >= 0:
            _close_fd_quietly(sources_fd)
        if project_fd >= 0:
            _close_fd_quietly(project_fd)
        path_error = exc.errno in {errno.ELOOP, errno.ENOTDIR}
        code = (
            ErrorCode.IMPORT_PATH_INVALID
            if path_error or not create
            else ErrorCode.IMPORT_WRITE_FAILED
        )
        raise ImportCommandError(
            code,
            f"Could not open a verified project sources directory: {exc}",
            "Ensure sources/ is a real directory inside the project and retry.",
        ) from exc

    try:
        yield None if missing else (project_fd, sources_fd)
    finally:
        if sources_fd >= 0:
            _close_fd_quietly(sources_fd)
        if project_fd >= 0:
            _close_fd_quietly(project_fd)


def _target_exists(project_path: Path, filename: str) -> bool:
    """Inspect a direct target without following sources/ or the target itself."""
    with _verified_sources_directory(project_path, create=False) as handles:
        if handles is None:
            return False
        _project_fd, sources_fd = handles
        try:
            target_state = os.stat(filename, dir_fd=sources_fd, follow_symlinks=False)
        except FileNotFoundError:
            return False
        except OSError as exc:
            raise ImportCommandError(
                ErrorCode.IMPORT_PATH_INVALID,
                f"Could not inspect import target sources/{filename}: {exc}",
            ) from exc
        if stat.S_ISDIR(target_state.st_mode):
            raise ImportCommandError(
                ErrorCode.IMPORT_PATH_INVALID,
                f"Import output is a directory, not a declaration file: sources/{filename}",
            )
        return True


def _write_all(fd: int, content: bytes) -> None:
    """Write all bytes or raise without treating a short write as success."""
    offset = 0
    while offset < len(content):
        written = os.write(fd, content[offset:])
        if written <= 0:
            raise OSError(errno.EIO, "short write while staging import declaration")
        offset += written


def _close_staged_file(fd: int) -> None:
    """Close a staged file; kept separate so close failures are testable."""
    os.close(fd)


def _unlink_matching(
    directory_fd: int,
    name: str,
    identity: Optional[tuple[int, int]],
) -> None:
    """Remove only the staging inode created by this process."""
    if identity is None:
        return
    try:
        current = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
    except FileNotFoundError:
        return
    except OSError:
        return
    if (current.st_dev, current.st_ino) != identity:
        return
    try:
        os.unlink(name, dir_fd=directory_fd)
    except OSError:
        pass


def _install_no_replace(directory_fd: int, stage_name: str, filename: str) -> None:
    """Atomically link the staged inode into its final name without replacement."""
    os.link(
        stage_name,
        filename,
        src_dir_fd=directory_fd,
        dst_dir_fd=directory_fd,
        follow_symlinks=False,
    )


def _write_exclusive(target: Path, content: str) -> None:
    """Atomically create a durable declaration without clobber or path traversal."""
    project_path = target.parent.parent
    filename = target.name
    with _verified_sources_directory(project_path, create=True) as handles:
        assert handles is not None
        project_fd, sources_fd = handles
        stage_name = ""
        stage_fd = -1
        stage_identity: Optional[tuple[int, int]] = None
        stage_exists = False
        try:
            for _attempt in range(10):
                stage_name = f".streamt-import-{secrets.token_hex(12)}.tmp"
                try:
                    stage_fd = os.open(
                        stage_name,
                        _FILE_CREATE_FLAGS,
                        0o666,
                        dir_fd=sources_fd,
                    )
                    break
                except FileExistsError:
                    continue
            else:
                raise OSError(errno.EEXIST, "could not allocate a unique staging file")

            staged = os.fstat(stage_fd)
            stage_identity = (staged.st_dev, staged.st_ino)
            stage_exists = True
            _write_all(stage_fd, content.encode("utf-8"))
            os.fsync(stage_fd)
            _close_staged_file(stage_fd)
            stage_fd = -1

            _verify_sources_binding(project_fd, sources_fd)
            try:
                _install_no_replace(sources_fd, stage_name, filename)
            except FileExistsError as exc:
                raise ImportCommandError(
                    ErrorCode.IMPORT_TARGET_EXISTS,
                    f"Import target already exists and was not changed: {target}",
                    "Choose another --output-file or merge the preview manually.",
                ) from exc
            os.unlink(stage_name, dir_fd=sources_fd)
            stage_exists = False
            _verify_sources_binding(project_fd, sources_fd)
            os.fsync(sources_fd)
        except BaseException as exc:
            if stage_fd >= 0:
                _close_fd_quietly(stage_fd)
            _unlink_matching(sources_fd, filename, stage_identity)
            if stage_exists and stage_name:
                _unlink_matching(sources_fd, stage_name, stage_identity)
            try:
                os.fsync(sources_fd)
            except OSError:
                pass

            if isinstance(exc, KeyboardInterrupt):
                raise
            if isinstance(exc, ImportCommandError):
                raise
            if isinstance(exc, OSError):
                raise ImportCommandError(
                    ErrorCode.IMPORT_WRITE_FAILED,
                    f"Could not create import declaration {target}: {exc}",
                ) from exc
            raise


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
        target_exists = _target_exists(project_path, target.name)

        kafka = make_kafka_deployer(project, fmt)
        if kafka is None:
            raise ImportCommandError(
                ErrorCode.IMPORT_DISCOVERY_FAILED,
                "Kafka is not configured or reachable; import requires read-only topic metadata.",
                "Check runtime.kafka for the selected environment.",
            )

        if schemas and project.runtime.schema_registry is not None:
            schema_registry = make_sr_deployer(project, fmt, required=False)

        try:
            discovered = discover_topics(
                kafka,
                schema_registry,
                include=include_patterns or None,
                exclude=exclude_patterns or None,
                strict_topic_metadata=True,
                include_schema_compatibility=False,
                stop_schema_enrichment_on_outage=True,
            )
        except Exception as exc:
            raise ImportCommandError(
                ErrorCode.IMPORT_DISCOVERY_FAILED,
                f"Kafka topic discovery failed ({type(exc).__name__}); no declaration was written.",
                "Check runtime.kafka connectivity, authentication, and topic permissions.",
            ) from exc

        existing_by_topic = {source.topic: source for source in project.sources}
        new_resources: list[DiscoveredTopic] = []
        observations: list[dict[str, object]] = []
        for resource in discovered:
            if resource.schema_error:
                fmt.print_warning(
                    f"Schema enrichment skipped for {resource.topic!r}: {resource.schema_error}.",
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
        if definitions:
            _validate_prospective_project(project, definitions)
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
        message = redact_sensitive_text(exc)
        fmt.add_error(StructuredError(code=ErrorCode.ENV_VAR_ERROR, message=message))
        fmt.print_error(message)
        fmt.flush()
        raise click.exceptions.Exit(1) from exc
    except ParseError as exc:
        message = redact_sensitive_text(exc)
        fmt.add_error(StructuredError(code=ErrorCode.PARSE_ERROR, message=message))
        fmt.print_error(message)
        fmt.flush()
        raise click.exceptions.Exit(1) from exc
    except EnvironmentError as exc:
        message = redact_sensitive_text(exc)
        fmt.add_error(StructuredError(code=ErrorCode.ENVIRONMENT_ERROR, message=message))
        fmt.print_error(message)
        fmt.flush()
        raise click.exceptions.Exit(1) from exc
    except ImportCommandError as exc:
        message = redact_sensitive_text(exc.message)
        suggestion = redact_sensitive_text(exc.suggestion) if exc.suggestion is not None else None
        if data:
            fmt.set_data(data)
        fmt.add_error(
            StructuredError(
                code=exc.code,
                message=message,
                suggestion=suggestion,
            )
        )
        fmt.print_error(message)
        fmt.flush()
        raise click.exceptions.Exit(1) from exc
    except KeyboardInterrupt as exc:
        fmt.print_error("Interrupted.")
        fmt.flush()
        raise click.exceptions.Exit(130) from exc
    except Exception as exc:
        detail = redact_sensitive_text(exc)
        message = f"Import failed safely ({type(exc).__name__}): {detail}"
        fmt.add_error(
            StructuredError(
                code=ErrorCode.IMPORT_DISCOVERY_FAILED,
                message=message,
                suggestion="Fix the reported project or backend error and retry.",
            )
        )
        fmt.print_error(message)
        fmt.flush()
        raise click.exceptions.Exit(1) from exc
    finally:
        close_deployers(schema_registry, kafka)
