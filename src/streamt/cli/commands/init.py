"""streamt init command."""

from __future__ import annotations

import fnmatch
import logging
import re
import sys
from pathlib import Path
from typing import Optional

import click
import yaml

from streamt.cli.helpers import make_formatter
from streamt.core.errors import ErrorCode
from streamt.output import OutputFormatter, StructuredError

logger = logging.getLogger(__name__)

INTERNAL_TOPIC_PREFIXES = ("__", "_schemas", "_confluent", "_streamt-connect-")

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


def _sanitize_name(topic: str) -> str:
    """Convert topic name to a valid source name."""
    return re.sub(r"[^a-zA-Z0-9_]", "_", topic)


def _is_internal_topic(topic: str) -> bool:
    """Check if a topic is internal."""
    return any(topic.startswith(p) for p in INTERNAL_TOPIC_PREFIXES)


def _avro_type_to_flink(avro_type: object) -> str:
    """Convert Avro type to Flink SQL type string."""
    if isinstance(avro_type, dict):
        logical = avro_type.get("logicalType")
        if logical == "timestamp-millis":
            return "TIMESTAMP(3)"
        if logical == "timestamp-micros":
            return "TIMESTAMP(6)"
        if logical == "date":
            return "DATE"
        if logical == "decimal":
            precision = avro_type.get("precision", 10)
            scale = avro_type.get("scale", 2)
            return f"DECIMAL({precision},{scale})"
        base = avro_type.get("type", "string")
        return _avro_type_to_flink(base)
    if isinstance(avro_type, list):
        # Union type — pick first non-null
        non_null = [t for t in avro_type if t != "null"]
        if non_null:
            return _avro_type_to_flink(non_null[0])
        return "STRING"
    mapping = {
        "string": "STRING",
        "int": "INT",
        "long": "BIGINT",
        "float": "FLOAT",
        "double": "DOUBLE",
        "boolean": "BOOLEAN",
        "bytes": "BYTES",
    }
    return mapping.get(str(avro_type), "STRING")


def _extract_columns_from_avro(schema: dict) -> list[dict]:
    """Extract columns from an Avro schema."""
    columns = []
    for field in schema.get("fields", []):
        name = field.get("name")
        if not name:
            continue
        col = {"name": name, "type": _avro_type_to_flink(field.get("type", "string"))}
        if field.get("doc"):
            col["description"] = field["doc"]
        columns.append(col)
    return columns


@click.command()
@click.option("--project-dir", "-p", type=click.Path(), default=".", help="Directory to initialize")
@click.option("--project-name", type=str, default=None, help="Project name (default: directory name)")
@click.option("--force", is_flag=True, help="Overwrite existing project files")
@click.option("--discover", is_flag=True, help="Discover sources from existing Kafka infrastructure")
@click.option("--kafka", type=str, default=None, help="Kafka bootstrap servers (required with --discover)")
@click.option("--schema-registry", type=str, default=None, help="Schema Registry URL")
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
    include: Optional[str],
    exclude: Optional[str],
    dry_run: bool,
) -> None:
    """Initialize a new streamt project."""
    fmt = make_formatter(ctx, "init")
    project_path = Path(project_dir).resolve()

    project_file = project_path / "stream_project.yml"
    if project_file.exists() and not force and not dry_run:
        fmt.add_error(StructuredError(
            code=ErrorCode.ENVIRONMENT_ERROR,
            message=f"stream_project.yml already exists in {project_path}. Use --force to overwrite.",
        ))
        fmt.print_error("stream_project.yml already exists. Use --force to overwrite.")
        fmt.flush()
        sys.exit(1)

    name = project_name or project_path.name

    if discover:
        _init_discover(fmt, project_path, name, kafka, schema_registry, include, exclude, dry_run, force)
    else:
        _init_scaffold(fmt, project_path, name, dry_run)


def _init_scaffold(fmt: OutputFormatter, project_path: Path, name: str, dry_run: bool) -> None:
    """Create an empty project scaffold."""
    created_files = []

    config = {
        "project": {"name": name, "version": "1.0.0"},
        "runtime": {"kafka": {"bootstrap_servers": "localhost:9092"}},
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
        for d in SCAFFOLD_DIRS:
            fmt.print(f"  {d}/")
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
) -> None:
    """Discover existing infrastructure and generate project."""
    project_file = project_path / "stream_project.yml"
    if project_file.exists() and not force:
        fmt.add_error(StructuredError(
            code=ErrorCode.ENVIRONMENT_ERROR,
            message=f"Project already exists at {project_path}. Use --force to overwrite.",
        ))
        fmt.print_error(f"Project already exists at {project_path}. Use --force to overwrite.")
        fmt.flush()
        sys.exit(1)

    if not kafka:
        fmt.add_error(StructuredError(
            code=ErrorCode.MISSING_CONFIG,
            message="--kafka is required with --discover",
        ))
        fmt.print_error("--kafka is required with --discover")
        fmt.flush()
        sys.exit(1)

    from streamt.deployer.kafka import KafkaDeployer
    from streamt.deployer.schema_registry import SchemaRegistryDeployer

    # Connect to Kafka
    try:
        kafka_deployer = KafkaDeployer(kafka)
    except Exception as e:
        fmt.add_error(StructuredError(
            code=ErrorCode.ENVIRONMENT_ERROR,
            message=f"Cannot connect to Kafka at {kafka}: {e}",
        ))
        fmt.print_error(f"Cannot connect to Kafka at {kafka}: {e}")
        fmt.flush()
        sys.exit(1)

    # Get all topics (including internal, then filter)
    metadata = kafka_deployer.admin.list_topics(timeout=10)
    all_topics = sorted(metadata.topics.keys())

    # Filter topics
    topics = [t for t in all_topics if not _is_internal_topic(t)]
    if include:
        topics = [t for t in topics if fnmatch.fnmatch(t, include)]
    if exclude:
        topics = [t for t in topics if not fnmatch.fnmatch(t, exclude)]

    # Connect to Schema Registry (optional)
    sr_deployer = None
    if schema_registry:
        try:
            sr_deployer = SchemaRegistryDeployer(schema_registry)
            sr_deployer.list_subjects()  # Test connection
        except Exception as e:
            fmt.print_warning(f"Cannot connect to Schema Registry: {e}")
            sr_deployer = None

    # Discover topic details and schemas
    discovered = []
    for topic in topics:
        state = kafka_deployer.get_topic_state(topic)
        source_name = _sanitize_name(topic)

        source_def: dict[str, object] = {
            "name": source_name,
            "topic": topic,
            "description": f"Discovered from Kafka ({state.partitions} partitions)",
        }

        # Try to get schema
        if sr_deployer:
            try:
                schema_state = sr_deployer.get_schema_state(f"{topic}-value")
                if schema_state.exists and schema_state.schema:
                    if schema_state.schema_type == "AVRO" and "fields" in schema_state.schema:
                        columns = _extract_columns_from_avro(schema_state.schema)
                        if columns:
                            source_def["columns"] = columns
            except Exception as e:
                logger.debug("Schema discovery failed for topic '%s': %s", topic, e)

        discovered.append({
            "source": source_def,
            "topic": topic,
            "partitions": state.partitions,
            "replication_factor": state.replication_factor,
        })

    fmt.print(f"Discovered {len(discovered)} topic(s) from {kafka}")
    for d in discovered:
        cols = len(d["source"].get("columns", []))
        schema_info = f" ({cols} columns from schema)" if cols else ""
        fmt.print(f"  {d['topic']} ({d['partitions']} partitions){schema_info}")

    created_files = []

    if not dry_run:
        project_path.mkdir(parents=True, exist_ok=True)

        # Write stream_project.yml
        config: dict[str, object] = {
            "project": {"name": name, "version": "1.0.0"},
            "runtime": {"kafka": {"bootstrap_servers": kafka}},
        }
        if schema_registry:
            config["runtime"]["schema_registry"] = {"url": schema_registry}

        with open(project_path / "stream_project.yml", "w") as f:
            yaml.dump(config, f, default_flow_style=False, sort_keys=False)
        created_files.append("stream_project.yml")

        # Create directories
        for d in SCAFFOLD_DIRS:
            (project_path / d).mkdir(exist_ok=True)

        # Write sources
        if discovered:
            sources_data = {"sources": [d["source"] for d in discovered]}
            with open(project_path / "sources" / "discovered.yml", "w") as f:
                yaml.dump(sources_data, f, default_flow_style=False, sort_keys=False)
            created_files.append("sources/discovered.yml")

        for d in SCAFFOLD_DIRS:
            (project_path / d / ".gitkeep").touch()
            created_files.append(f"{d}/")

        fmt.print(f"\n[green]Initialized project '{name}' with {len(discovered)} source(s)[/green]")

    kafka_deployer.close()
    if sr_deployer is not None:
        sr_deployer.close()

    fmt.set_data({
        "project_name": name,
        "discovered_topics": [
            {"topic": d["topic"], "partitions": d["partitions"], "columns": len(d["source"].get("columns", []))}
            for d in discovered
        ],
        "created_files": created_files,
    })
    fmt.flush()
