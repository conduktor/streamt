"""streamt init command."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Optional

import click
import yaml

from streamt.cli.helpers import close_deployers, make_formatter, redact_sensitive_text
from streamt.core.errors import ErrorCode
from streamt.discovery import (
    INTERNAL_TOPIC_PREFIXES as _INTERNAL_TOPIC_PREFIXES,
)
from streamt.discovery import (
    avro_type_to_flink,
    discover_topics,
    extract_columns_from_avro,
    extract_columns_from_json_schema,
    is_internal_topic,
    json_schema_type_to_flink,
    sanitize_source_name,
)
from streamt.output import OutputFormatter, StructuredError

# Preserve the original helper imports while keeping their implementation in
# the reusable discovery module.
_sanitize_name = sanitize_source_name
_is_internal_topic = is_internal_topic
_avro_type_to_flink = avro_type_to_flink
_json_schema_type_to_flink = json_schema_type_to_flink
_extract_columns_from_json_schema = extract_columns_from_json_schema
_extract_columns_from_avro = extract_columns_from_avro
INTERNAL_TOPIC_PREFIXES = _INTERNAL_TOPIC_PREFIXES

DEFAULT_PROJECT_TEMPLATE = {
    "project": {
        "name": "",
        "version": "1.0.0",
    },
    "runtime": {
        "kafka": {
            "bootstrap_servers": "localhost:9092",
        },
    },
}

SCAFFOLD_DIRS = ["sources", "models", "tests"]


@click.command()
@click.option("--project-dir", "-p", type=click.Path(), default=".", help="Directory to initialize")
@click.option(
    "--project-name", type=str, default=None, help="Project name (default: directory name)"
)
@click.option("--force", is_flag=True, help="Overwrite existing project files")
@click.option(
    "--discover", is_flag=True, help="Discover sources from existing Kafka infrastructure"
)
@click.option(
    "--kafka", type=str, default=None, help="Kafka bootstrap servers (required with --discover)"
)
@click.option("--schema-registry", type=str, default=None, help="Schema Registry URL")
@click.option(
    "--security-protocol",
    type=str,
    default=None,
    help="Kafka security protocol (PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL)",
)
@click.option(
    "--sasl-mechanism",
    type=str,
    default=None,
    help="SASL mechanism (PLAIN, SCRAM-SHA-256, SCRAM-SHA-512)",
)
@click.option("--sasl-username", type=str, default=None, help="SASL username / API key")
@click.option("--sasl-password", type=str, default=None, help="SASL password / API secret")
@click.option("--sr-username", type=str, default=None, help="Schema Registry username / API key")
@click.option("--sr-password", type=str, default=None, help="Schema Registry password / API secret")
@click.option("--include", type=str, default=None, help="Include topics matching glob pattern")
@click.option("--exclude", type=str, default=None, help="Exclude topics matching glob pattern")
@click.option("--dry-run", is_flag=True, help="Show what would be created without writing")
@click.pass_context
def init(
    ctx: click.Context,
    project_dir: str,
    project_name: Optional[str],
    force: bool,
    discover: bool,
    kafka: Optional[str],
    schema_registry: Optional[str],
    security_protocol: Optional[str],
    sasl_mechanism: Optional[str],
    sasl_username: Optional[str],
    sasl_password: Optional[str],
    sr_username: Optional[str],
    sr_password: Optional[str],
    include: Optional[str],
    exclude: Optional[str],
    dry_run: bool,
) -> None:
    """Initialize a new streamt project."""
    fmt = make_formatter(ctx, "init")
    project_path = Path(project_dir).resolve()

    project_file = project_path / "stream_project.yml"
    if project_file.exists() and not force and not dry_run:
        fmt.add_error(
            StructuredError(
                code=ErrorCode.ENVIRONMENT_ERROR,
                message=f"stream_project.yml already exists in {project_path}. Use --force to overwrite.",
            )
        )
        fmt.print_error("stream_project.yml already exists. Use --force to overwrite.")
        fmt.flush()
        sys.exit(1)

    name = project_name or project_path.name

    if discover:
        _init_discover(
            fmt,
            project_path,
            name,
            kafka,
            schema_registry,
            include,
            exclude,
            dry_run,
            force,
            security_protocol=security_protocol,
            sasl_mechanism=sasl_mechanism,
            sasl_username=sasl_username,
            sasl_password=sasl_password,
            sr_username=sr_username,
            sr_password=sr_password,
        )
    else:
        _init_scaffold(fmt, project_path, name, dry_run)


def _init_scaffold(fmt: OutputFormatter, project_path: Path, name: str, dry_run: bool) -> None:
    """Create a project scaffold with example source, model, and test."""
    created_files: list[str] = []

    config = {
        "apiVersion": "streamt.dev/v1alpha1",
        "project": {"name": name, "version": "1.0.0"},
        "runtime": {
            "kafka": {"bootstrap_servers": "${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}"},
        },
        "sources": [
            {
                "name": "raw_events",
                "topic": f"{name}.raw.events.v1",
                "description": "Raw event stream — replace with your actual source topic",
                "columns": [
                    {"name": "id", "type": "STRING"},
                    {"name": "event_type", "type": "STRING"},
                    {"name": "payload", "type": "STRING"},
                    {"name": "created_at", "type": "TIMESTAMP(3)"},
                ],
            }
        ],
        "models": [
            {
                "name": "events_clean",
                "description": "Events forwarded to a clean topic — add WHERE or transforms as needed",
                "sql": 'SELECT * FROM {{ source("raw_events") }}',
            }
        ],
        "tests": [
            {
                "name": "events_clean_not_null",
                "model": "events_clean",
                "type": "schema",
                "assertions": [{"not_null": {"columns": ["id", "event_type"]}}],
            }
        ],
    }

    if not dry_run:
        project_path.mkdir(parents=True, exist_ok=True)

        with open(project_path / "stream_project.yml", "w") as f:
            yaml.dump(config, f, default_flow_style=False, sort_keys=False)
        created_files.append("stream_project.yml")

        for d in SCAFFOLD_DIRS:
            dir_path = project_path / d
            dir_path.mkdir(exist_ok=True)
            (dir_path / ".gitkeep").touch()
            created_files.append(f"{d}/")

        fmt.print(f"[green]Initialized project '{name}'[/green]")
        fmt.print("  stream_project.yml")
        fmt.print("  1 source (raw_events), 1 model (events_clean), 1 test")
        for d in SCAFFOLD_DIRS:
            fmt.print(f"  {d}/")
        fmt.print("\nNext steps:")
        fmt.print("  streamt validate        # Check project is valid")
        fmt.print("  streamt compile         # Generate deployment artifacts")
        fmt.print("  streamt plan            # Preview infrastructure changes")
    else:
        fmt.print(f"Would create project '{name}' in {project_path}")
        created_files = ["stream_project.yml"] + [f"{d}/" for d in SCAFFOLD_DIRS]

    fmt.set_data({"project_name": name, "created_files": created_files})
    fmt.flush()


def _init_discover(
    fmt: OutputFormatter,
    project_path: Path,
    name: str,
    kafka: Optional[str],
    schema_registry: Optional[str],
    include: Optional[str],
    exclude: Optional[str],
    dry_run: bool,
    force: bool,
    *,
    security_protocol: Optional[str] = None,
    sasl_mechanism: Optional[str] = None,
    sasl_username: Optional[str] = None,
    sasl_password: Optional[str] = None,
    sr_username: Optional[str] = None,
    sr_password: Optional[str] = None,
) -> None:
    """Discover existing infrastructure and generate project."""
    project_file = project_path / "stream_project.yml"
    if project_file.exists() and not force:
        fmt.add_error(
            StructuredError(
                code=ErrorCode.ENVIRONMENT_ERROR,
                message=f"Project already exists at {project_path}. Use --force to overwrite.",
            )
        )
        fmt.print_error(f"Project already exists at {project_path}. Use --force to overwrite.")
        fmt.flush()
        sys.exit(1)

    if not kafka:
        fmt.add_error(
            StructuredError(
                code=ErrorCode.MISSING_CONFIG,
                message="--kafka is required with --discover",
            )
        )
        fmt.print_error("--kafka is required with --discover")
        fmt.flush()
        sys.exit(1)

    from streamt.deployer.kafka import KafkaDeployer
    from streamt.deployer.schema_registry import SchemaRegistryDeployer

    # Build Kafka auth config
    kafka_config: dict[str, str] = {}
    if security_protocol:
        kafka_config["security.protocol"] = security_protocol
    if sasl_mechanism:
        kafka_config["sasl.mechanisms"] = sasl_mechanism
    if sasl_username:
        kafka_config["sasl.username"] = sasl_username
    if sasl_password:
        kafka_config["sasl.password"] = sasl_password

    # Connect to Kafka
    try:
        kafka_deployer = KafkaDeployer(kafka, **kafka_config)
    except Exception as e:
        safe_kafka = redact_sensitive_text(kafka)
        safe_error = redact_sensitive_text(e)
        fmt.add_error(
            StructuredError(
                code=ErrorCode.ENVIRONMENT_ERROR,
                message=f"Cannot connect to Kafka at {safe_kafka}: {safe_error}",
            )
        )
        fmt.print_error(f"Cannot connect to Kafka at {safe_kafka}: {safe_error}")
        fmt.flush()
        sys.exit(1)

    # Connect to Schema Registry (optional)
    sr_deployer = None
    if schema_registry:
        try:
            sr_deployer = SchemaRegistryDeployer(
                schema_registry, username=sr_username, password=sr_password
            )
            sr_deployer.list_subjects()  # Test connection
        except Exception as e:
            fmt.print_warning(
                f"Cannot connect to Schema Registry: {redact_sensitive_text(e)}"
            )
            close_deployers(sr_deployer)
            sr_deployer = None

    discovered = discover_topics(
        kafka_deployer,
        sr_deployer,
        include=include,
        exclude=exclude,
    )

    fmt.print(f"Discovered {len(discovered)} topic(s) from {kafka}")
    for d in discovered:
        cols = d.column_count
        schema_info = f" ({cols} columns from schema)" if cols else ""
        fmt.print(f"  {d.topic} ({d.partitions} partitions){schema_info}")

    created_files = []

    if not dry_run:
        project_path.mkdir(parents=True, exist_ok=True)

        # Write stream_project.yml
        runtime: dict[str, object] = {"kafka": {"bootstrap_servers": kafka}}
        config: dict[str, object] = {
            "apiVersion": "streamt.dev/v1alpha1",
            "project": {"name": name, "version": "1.0.0"},
            "runtime": runtime,
        }
        if schema_registry:
            runtime["schema_registry"] = {"url": schema_registry}

        with open(project_path / "stream_project.yml", "w") as f:
            yaml.dump(config, f, default_flow_style=False, sort_keys=False)
        created_files.append("stream_project.yml")

        # Create directories
        for directory in SCAFFOLD_DIRS:
            (project_path / directory).mkdir(exist_ok=True)

        # Write sources
        if discovered:
            sources_data = {"sources": [d.source for d in discovered]}
            with open(project_path / "sources" / "discovered.yml", "w") as f:
                yaml.dump(sources_data, f, default_flow_style=False, sort_keys=False)
            created_files.append("sources/discovered.yml")

        for directory in SCAFFOLD_DIRS:
            (project_path / directory / ".gitkeep").touch()
            created_files.append(f"{directory}/")

        fmt.print(f"\n[green]Initialized project '{name}' with {len(discovered)} source(s)[/green]")

    kafka_deployer.close()
    if sr_deployer is not None:
        sr_deployer.close()

    fmt.set_data(
        {
            "project_name": name,
            "discovered_topics": [
                {
                    "topic": d.topic,
                    "partitions": d.partitions,
                    "columns": d.column_count,
                }
                for d in discovered
            ],
            "created_files": created_files,
        }
    )
    fmt.flush()
