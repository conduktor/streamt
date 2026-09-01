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


def _json_schema_type_to_flink(prop: dict) -> str:
    """Convert JSON Schema property to Flink SQL type string."""
    fmt = prop.get("format")
    if fmt == "date-time":
        return "TIMESTAMP(3)"
    if fmt == "date":
        return "DATE"
    js_type = prop.get("type", "string")
    if isinstance(js_type, list):
        non_null = [t for t in js_type if t != "null"]
        js_type = non_null[0] if non_null else "string"
    mapping = {
        "string": "STRING",
        "integer": "INT",
        "number": "DOUBLE",
        "boolean": "BOOLEAN",
    }
    return mapping.get(js_type, "STRING")


def _extract_columns_from_json_schema(schema: dict) -> list[dict]:
    """Extract columns from a JSON Schema."""
    columns = []
    properties = schema.get("properties", {})
    required_fields = set(schema.get("required", []))
    for name, prop in properties.items():
        if not isinstance(prop, dict):
            continue
        col: dict[str, object] = {
            "name": name,
            "type": _json_schema_type_to_flink(prop),
        }
        desc = prop.get("description")
        if desc:
            col["description"] = desc
        if name in required_fields:
            col["required"] = True
        columns.append(col)
    return columns


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
        fmt.add_error(
            StructuredError(
                code=ErrorCode.ENVIRONMENT_ERROR,
                message=f"Cannot connect to Kafka at {kafka}: {e}",
            )
        )
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
            sr_deployer = SchemaRegistryDeployer(
                schema_registry, username=sr_username, password=sr_password
            )
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
                    columns: list[dict] = []
                    if schema_state.schema_type == "AVRO" and "fields" in schema_state.schema:
                        columns = _extract_columns_from_avro(schema_state.schema)
                    elif schema_state.schema_type == "JSON" and "properties" in schema_state.schema:
                        columns = _extract_columns_from_json_schema(schema_state.schema)
                    elif schema_state.schema_type == "PROTOBUF":
                        logger.debug("Protobuf schema for '%s' — skipping column extraction", topic)
                    if columns:
                        source_def["columns"] = columns
            except Exception as e:
                logger.debug("Schema discovery failed for topic '%s': %s", topic, e)

        discovered.append(
            {
                "source": source_def,
                "topic": topic,
                "partitions": state.partitions,
                "replication_factor": state.replication_factor,
            }
        )

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
            "apiVersion": "streamt.dev/v1alpha1",
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

    fmt.set_data(
        {
            "project_name": name,
            "discovered_topics": [
                {
                    "topic": d["topic"],
                    "partitions": d["partitions"],
                    "columns": len(d["source"].get("columns", [])),
                }
                for d in discovered
            ],
            "created_files": created_files,
        }
    )
    fmt.flush()
