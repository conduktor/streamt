"""streamt status command."""

from __future__ import annotations

import fnmatch
import json
from collections.abc import Generator
from contextlib import contextmanager
from typing import Literal, Optional, TypedDict

import click
from typing_extensions import NotRequired

from streamt.cli.helpers import (
    close_deployers,
    get_project_path,
    handle_parse_error,
    make_connect_deployer,
    make_flink_deployer,
    make_formatter,
    make_gateway_deployer,
    make_kafka_deployer,
    make_kafka_streams_deployer,
    make_sr_deployer,
    redact_sensitive_text,
)
from streamt.core.errors import ErrorCode
from streamt.deployer.gateway import (
    plan_managed_gateway_rule,
    resolve_managed_gateway_rules,
    secret_neutral_gateway_changes,
)
from streamt.output import OutputFormatter, StructuredError, get_output_format_from_context


class SchemaStatus(TypedDict):
    subject: str
    exists: bool
    version: int | None
    schema_type: str | None


class SourceTopicStatus(TypedDict):
    name: str
    topic: str
    exists: bool | None
    partitions: int | None
    observation: Literal["not_requested", "verified"]


class DriftStatus(TypedDict):
    field: str
    actual: int | None
    desired: int


class TopicStatus(TypedDict):
    name: str
    exists: bool
    partitions: int | None
    replication_factor: int | None
    status: Literal["OK", "MISSING", "DRIFT"]
    message_count: NotRequired[int]
    drifts: NotRequired[list[DriftStatus]]


class PartitionLagStatus(TypedDict):
    partition: int
    current_offset: int
    end_offset: int
    lag: int


class ConsumerTopicLagStatus(TypedDict):
    topic: str
    total_lag: int
    partitions: list[PartitionLagStatus]


class ConsumerGroupStatus(TypedDict):
    group_id: str
    topics: list[ConsumerTopicLagStatus]


class FlinkJobStatus(TypedDict):
    name: str
    exists: bool
    job_id: str | None
    status: str | None


class ConnectorStatus(TypedDict):
    name: str
    exists: bool
    status: str | None


class GatewayRuleStatus(TypedDict):
    name: str
    exists: bool
    virtual_topic: str
    physical_topic: str
    status: Literal["OK", "MISSING", "DRIFT"]
    interceptors_desired: int
    interceptors_found: int
    scope: str
    backend_fingerprint: str
    current_fingerprint: str
    desired_fingerprint: str
    observed_physical_topic: str | None
    observed_physical_cluster: str | None
    drift_categories: NotRequired[list[str]]


def _artifact_str(artifact: dict[str, object], key: str) -> str:
    """Return a required compiler artifact string without leaking its value on error."""
    value = artifact.get(key)
    if not isinstance(value, str) or not value:
        raise TypeError(f"Compiled artifact field {key!r} must be a non-empty string")
    return value


def _artifact_optional_int(artifact: dict[str, object], key: str) -> int | None:
    """Return an optional compiler artifact integer with a path-safe error."""
    value = artifact.get(key)
    if value is None:
        return None
    if not isinstance(value, int) or isinstance(value, bool):
        raise TypeError(f"Compiled artifact field {key!r} must be an integer")
    return value


@contextmanager
def _deployer_section(
    fmt: OutputFormatter, is_text: bool, service: str
) -> Generator[None, None, None]:
    """Catch and report deployer errors uniformly across status sections."""
    try:
        yield
    except Exception as e:
        safe_error = redact_sensitive_text(e)
        fmt.add_error(
            StructuredError(
                code=ErrorCode.CONNECTION_REFUSED,
                message=f"{service}: {safe_error}",
            )
        )
        if is_text:
            fmt.print(f"  [yellow]Cannot connect to {service}: {safe_error}[/yellow]")


@click.command()
@click.option("--project-dir", "-p", type=click.Path(exists=True), help="Path to project directory")
@click.option(
    "--env", "-e", "environment", help="Target environment (reads from STREAMT_ENV if not set)"
)
@click.option("--lag", is_flag=True, help="Show consumer lag for topics")
@click.option("--consumer-groups", is_flag=True, help="Show per-consumer-group lag")
@click.option(
    "--include-external", is_flag=True,
    help="Explicitly inspect external resources as well as managed resources",
)
@click.option(
    "--health", is_flag=True, help="Health check mode: exit 1 if any resource is MISSING or DRIFT"
)
@click.option(
    "--format",
    "output_format",
    type=click.Choice(["text", "json"]),
    default=None,
    help="Output format (overrides global --output)",
)
@click.option(
    "--filter", "filter_pattern", type=str, help="Filter resources by name pattern (glob-style)"
)
@click.pass_context
def status(
    ctx: click.Context,
    project_dir: Optional[str],
    environment: Optional[str],
    lag: bool,
    consumer_groups: bool,
    health: bool,
    output_format: Optional[str],
    filter_pattern: Optional[str],
    include_external: bool = False,
) -> None:
    """Show status of deployed resources."""
    from streamt.compiler import Compiler
    from streamt.compiler.manifest import ArtifactOwnership
    from streamt.core.environment import EnvironmentError
    from streamt.core.parser import EnvVarError, ParseError, ProjectParser

    fmt = make_formatter(ctx, "status")
    project_path = get_project_path(project_dir)
    effective_format = output_format or get_output_format_from_context(ctx)
    is_text = effective_format == "text"

    def matches(name: str) -> bool:
        return not filter_pattern or fnmatch.fnmatch(name, filter_pattern)

    deployers_to_close: list[object] = []
    try:
        parser = ProjectParser(
            project_path,
            environment=environment,
            warn_callback=lambda msg: fmt.print(msg),
        )
        project = parser.parse()
        compiler = Compiler(project)
        manifest = compiler.compile(dry_run=True)

        # External declarations describe dependencies, not observed runtime truth.
        # Filter before constructing clients so a declaration-only project does
        # not connect to configured providers just to report its existence.
        status_artifacts: dict[str, list[dict[str, object]]] = {}
        external_resources: list[dict[str, str]] = []
        try:
            for kind, artifacts in manifest.artifacts.items():
                status_artifacts[kind] = []
                for compiled_artifact in artifacts:
                    artifact_name = None
                    if kind in {"schemas", "topics", "flink_jobs", "kafka_streams_jobs", "connectors", "gateway_rules"}:
                        artifact_name = _artifact_str(
                            compiled_artifact, "subject" if kind == "schemas" else "name"
                        )
                    if artifact_name is not None and not matches(artifact_name):
                        continue
                    ownership = ArtifactOwnership.from_dict(compiled_artifact.get("ownership"))
                    if (
                        not include_external
                        and ownership is not None
                        and ownership.mode == "external"
                        and ownership.project == project.project.name
                    ):
                        if artifact_name is not None:
                            external_resources.append({
                                "kind": kind,
                                "name": artifact_name,
                                "observation": "not_requested",
                            })
                        continue
                    status_artifacts[kind].append(compiled_artifact)
        except (AttributeError, TypeError, ValueError) as exc:
            # Prefiltering runs before provider sections. Preserve the structured
            # error contract without exposing malformed artifact values or opening clients.
            message = "Compiled status artifact metadata is invalid; recompile the project."
            fmt.add_error(StructuredError(code=ErrorCode.PARSE_ERROR, message=message))
            if is_text:
                fmt.print_error(message)
            fmt.flush()
            raise click.exceptions.Exit(1) from exc

        schemas: list[SchemaStatus] = []
        source_topics: list[SourceTopicStatus] = []
        topics: list[TopicStatus] = []
        flink_jobs: list[FlinkJobStatus] = []
        kafka_streams_jobs: list[dict[str, object]] = []
        connectors: list[ConnectorStatus] = []
        gateway_rules: list[GatewayRuleStatus] = []
        group_statuses: list[ConsumerGroupStatus] = []
        observation_incomplete = False
        data: dict[str, object] = {
            "project": project.project.name,
            "schemas": schemas,
            "topics": topics,
            "flink_jobs": flink_jobs,
            "connectors": connectors,
            "gateway_rules": gateway_rules,
            "observation_scope": "managed_and_external" if include_external else "managed",
            "external_resources": external_resources,
        }
        if is_text and external_resources:
            fmt.print(
                "External artifacts are declared, not inspected. "
                "Use --include-external for live observations."
            )

        # Schemas
        if status_artifacts.get("schemas"):
            if is_text:
                fmt.print("\n[cyan]Schemas:[/cyan]")
            sd = make_sr_deployer(project, fmt)
            if sd is not None:
                deployers_to_close.append(sd)
                with _deployer_section(fmt, is_text, "Schema Registry"):
                    for schema_artifact in status_artifacts["schemas"]:
                        subject = _artifact_str(schema_artifact, "subject")
                        if not matches(subject):
                            continue
                        schema_state = sd.get_schema_state(subject)
                        schema_entry: SchemaStatus = {
                            "subject": subject,
                            "exists": schema_state.exists,
                            "version": schema_state.version if schema_state.exists else None,
                            "schema_type": (
                                schema_state.schema_type if schema_state.exists else None
                            ),
                        }
                        schemas.append(schema_entry)
                        if is_text:
                            if schema_state.exists:
                                fmt.print(
                                    f"  [green]OK[/green] {subject} "
                                    f"(v{schema_state.version}, {schema_state.schema_type})"
                                )
                            else:
                                fmt.print(f"  [red]MISSING[/red] {subject}")
            elif is_text:
                fmt.print("  [yellow]No Schema Registry configured[/yellow]")
            if sd is None:
                observation_incomplete = True

        # Source Topics (STATUS-2)
        if project.sources:
            data["source_topics"] = source_topics
            if is_text:
                fmt.print("\n[cyan]Source Topics:[/cyan]")
            observed_sources = []
            for src in project.sources:
                if not matches(src.name) and not matches(src.topic):
                    continue
                if not include_external and src.ownership.mode.value == "external":
                    source_topics.append({
                        "name": src.name, "topic": src.topic,
                        "exists": None, "partitions": None,
                        "observation": "not_requested",
                    })
                    if is_text:
                        fmt.print(
                            f"  DECLARED {src.name} -> {src.topic} "
                            "(external; use --include-external to inspect)"
                        )
                else:
                    observed_sources.append(src)
            skd = make_kafka_deployer(project, fmt) if observed_sources else None
            if skd is not None:
                deployers_to_close.append(skd)
                with _deployer_section(fmt, is_text, "Kafka (sources)"):
                    for src in observed_sources:
                        if not matches(src.name) and not matches(src.topic):
                            continue
                        source_state = skd.get_topic_state(src.topic)
                        source_entry: SourceTopicStatus = {
                            "name": src.name,
                            "topic": src.topic,
                            "exists": source_state.exists,
                            "partitions": (
                                source_state.partitions if source_state.exists else None
                            ),
                            "observation": "verified",
                        }
                        source_topics.append(source_entry)
                        if is_text:
                            if source_state.exists:
                                fmt.print(
                                    f"  [green]OK[/green] {src.name} → {src.topic} "
                                    f"(partitions: {source_state.partitions})"
                                )
                            else:
                                fmt.print(f"  [red]MISSING[/red] {src.name} → {src.topic}")
            elif is_text and observed_sources:
                fmt.print("  [yellow]No Kafka configured[/yellow]")
            if skd is None and observed_sources:
                observation_incomplete = True

        # Topics
        if is_text:
            fmt.print("\n[cyan]Topics:[/cyan]")
        kd = make_kafka_deployer(project, fmt) if status_artifacts.get("topics") else None
        if kd is not None:
            deployers_to_close.append(kd)
            with _deployer_section(fmt, is_text, "Kafka"):
                for topic_artifact in status_artifacts.get("topics", []):
                    topic_name = _artifact_str(topic_artifact, "name")
                    if not matches(topic_name):
                        continue
                    topic_state = kd.get_topic_state(topic_name)
                    topic_entry: TopicStatus = {
                        "name": topic_name,
                        "exists": topic_state.exists,
                        "partitions": topic_state.partitions if topic_state.exists else None,
                        "replication_factor": (
                            topic_state.replication_factor if topic_state.exists else None
                        ),
                        "status": "MISSING",
                    }
                    if lag and topic_state.exists:
                        topic_entry["message_count"] = kd.get_topic_message_count(topic_name)
                    drifts: list[DriftStatus] = []
                    if topic_state.exists:
                        desired_p = _artifact_optional_int(topic_artifact, "partitions")
                        desired_rf = _artifact_optional_int(
                            topic_artifact, "replication_factor"
                        )
                        if desired_p is not None and topic_state.partitions != desired_p:
                            drifts.append(
                                {
                                    "field": "partitions",
                                    "actual": topic_state.partitions,
                                    "desired": desired_p,
                                }
                            )
                        if (
                            desired_rf is not None
                            and topic_state.replication_factor != desired_rf
                        ):
                            drifts.append(
                                {
                                    "field": "replication_factor",
                                    "actual": topic_state.replication_factor,
                                    "desired": desired_rf,
                                }
                            )
                    if drifts:
                        topic_entry["status"] = "DRIFT"
                        topic_entry["drifts"] = drifts
                    elif topic_state.exists:
                        topic_entry["status"] = "OK"
                    topics.append(topic_entry)
                    if is_text:
                        if topic_state.exists and drifts:
                            drift_parts = [
                                f"{d['field']}: {d['actual']} → {d['desired']}" for d in drifts
                            ]
                            fmt.print(
                                f"  [yellow]DRIFT[/yellow] {topic_name}  "
                                f"{', '.join(drift_parts)}"
                            )
                        elif topic_state.exists:
                            line = (
                                f"  [green]OK[/green] {topic_name} "
                                f"(partitions: {topic_state.partitions}, "
                                f"rf: {topic_state.replication_factor})"
                            )
                            if "message_count" in topic_entry:
                                line += (
                                    f" [dim]~{topic_entry['message_count']} msgs[/dim]"
                                )
                            fmt.print(line)
                        else:
                            fmt.print(f"  [red]MISSING[/red] {topic_name}")
        elif status_artifacts.get("topics"):
            observation_incomplete = True

        # Consumer group lag (STATUS-3)
        if consumer_groups and kd is not None:
            data["consumer_groups"] = group_statuses
            if is_text:
                fmt.print("\n[cyan]Consumer Groups:[/cyan]")
            with _deployer_section(fmt, is_text, "Kafka (consumer groups)"):
                groups = kd.get_consumer_groups()
                topic_names = [
                    _artifact_str(topic_artifact, "name")
                    for topic_artifact in status_artifacts.get("topics", [])
                ]
                for group_id in sorted(groups):
                    topic_lags: list[ConsumerTopicLagStatus] = []
                    group_entry: ConsumerGroupStatus = {
                        "group_id": group_id,
                        "topics": topic_lags,
                    }
                    for topic_name in topic_names:
                        if not matches(topic_name):
                            continue
                        lag_info = kd.get_consumer_group_lag(group_id, topic_name)
                        if lag_info is not None:
                            topic_lag: ConsumerTopicLagStatus = {
                                "topic": topic_name,
                                "total_lag": lag_info.total_lag,
                                "partitions": [
                                    {
                                        "partition": partition.partition,
                                        "current_offset": partition.current_offset,
                                        "end_offset": partition.end_offset,
                                        "lag": partition.lag,
                                    }
                                    for partition in lag_info.partitions
                                ],
                            }
                            topic_lags.append(topic_lag)
                    if topic_lags:
                        group_statuses.append(group_entry)
                        if is_text:
                            total = sum(topic_lag["total_lag"] for topic_lag in topic_lags)
                            color = "green" if total == 0 else "yellow" if total < 1000 else "red"
                            fmt.print(f"  [{color}]{group_id}[/{color}] total_lag={total}")
                            for topic_lag in topic_lags:
                                fmt.print(
                                    f"    {topic_lag['topic']}: "
                                    f"lag={topic_lag['total_lag']}"
                                )

        # Flink jobs
        if status_artifacts.get("flink_jobs"):
            if is_text:
                fmt.print("\n[cyan]Flink Jobs:[/cyan]")
            fd = make_flink_deployer(project, fmt, state_dir=project_path / ".streamt")
            if fd is not None:
                deployers_to_close.append(fd)
                with _deployer_section(fmt, is_text, "Flink"):
                    for job_artifact in status_artifacts["flink_jobs"]:
                        job_name = _artifact_str(job_artifact, "name")
                        if not matches(job_name):
                            continue
                        job_state = fd.get_job_state(job_name)
                        job_entry: FlinkJobStatus = {
                            "name": job_name,
                            "exists": job_state.exists,
                            "job_id": job_state.job_id if job_state.exists else None,
                            "status": job_state.status if job_state.exists else None,
                        }
                        flink_jobs.append(job_entry)
                        if is_text:
                            if job_state.exists:
                                color = "green" if job_state.status == "RUNNING" else "yellow"
                                fmt.print(
                                    f"  [{color}]{job_state.status}[/{color}] {job_name}"
                                )
                            else:
                                fmt.print(f"  [red]NOT FOUND[/red] {job_name}")
            elif is_text:
                fmt.print("  [yellow]No Flink configured[/yellow]")
            if fd is None:
                observation_incomplete = True

        # Explicitly observed managed runners (external artifacts were filtered
        # before any factory unless --include-external was requested).
        if manifest.artifacts.get("kafka_streams_jobs"):
            data["kafka_streams_jobs"] = kafka_streams_jobs
        if status_artifacts.get("kafka_streams_jobs"):
            from streamt.compiler.manifest import parse_compiled_kafka_streams_job_artifact

            streams = make_kafka_streams_deployer(project, fmt, state_dir=project_path / ".streamt")
            if streams is None:
                observation_incomplete = True
            else:
                deployers_to_close.append(streams)
                with _deployer_section(fmt, is_text, "Kafka Streams"):
                    for raw_job in status_artifacts["kafka_streams_jobs"]:
                        streams_artifact = parse_compiled_kafka_streams_job_artifact(raw_job)
                        if not matches(streams_artifact.name):
                            continue
                        state = streams.get_job_state(streams_artifact)
                        kafka_streams_jobs.append({
                            "name": streams_artifact.name, "application_id": streams_artifact.application_id,
                            "exists": state.exists, "container_id": state.container_id,
                            "status": state.status,
                        })
                        if is_text:
                            fmt.print(f"  Kafka Streams: {streams_artifact.name} ({state.status or 'missing'})")

        # Connectors
        if status_artifacts.get("connectors"):
            if is_text:
                fmt.print("\n[cyan]Connectors:[/cyan]")
            cd = make_connect_deployer(project, fmt)
            if cd is not None:
                deployers_to_close.append(cd)
                with _deployer_section(fmt, is_text, "Connect"):
                    for connector_artifact in status_artifacts["connectors"]:
                        connector_name = _artifact_str(connector_artifact, "name")
                        if not matches(connector_name):
                            continue
                        connector_state = cd.get_connector_state(connector_name)
                        connector_entry: ConnectorStatus = {
                            "name": connector_name,
                            "exists": connector_state.exists,
                            "status": (
                                connector_state.status if connector_state.exists else None
                            ),
                        }
                        connectors.append(connector_entry)
                        if is_text:
                            if connector_state.exists:
                                color = (
                                    "green" if connector_state.status == "RUNNING" else "yellow"
                                )
                                fmt.print(
                                    f"  [{color}]{connector_state.status}[/{color}] "
                                    f"{connector_name}"
                                )
                            else:
                                fmt.print(f"  [red]NOT FOUND[/red] {connector_name}")
            elif is_text:
                fmt.print("  [yellow]No Connect configured[/yellow]")
            if cd is None:
                observation_incomplete = True

        # Gateway rules
        if status_artifacts.get("gateway_rules"):
            if is_text:
                fmt.print("\n[cyan]Gateway Rules:[/cyan]")
            gd = make_gateway_deployer(project, fmt)
            if gd is not None:
                deployers_to_close.append(gd)
                with _deployer_section(fmt, is_text, "Gateway"):
                    resolved_rules = resolve_managed_gateway_rules(
                        status_artifacts["gateway_rules"],
                        gd.cluster_binding,
                    )

                    selected_rules = [
                        rule for rule in resolved_rules if matches(rule.artifact.name)
                    ]
                    snapshot = gd.observe_managed_gateway_snapshot() if selected_rules else None
                    for rule in selected_rules:
                        artifact = rule.artifact
                        desired = rule.desired
                        if snapshot is None:  # pragma: no cover - narrowed above
                            raise RuntimeError("Gateway status snapshot is unavailable")
                        current = snapshot.rule(
                            artifact.name,
                            artifact.virtual_topic,
                        )
                        change = plan_managed_gateway_rule(
                            artifact,
                            desired,
                            current,
                        )
                        evidence = secret_neutral_gateway_changes(change.changes)
                        raw_categories = evidence.get("categories", [])
                        if not isinstance(raw_categories, list) or any(
                            not isinstance(category, str) for category in raw_categories
                        ):
                            raise TypeError("Gateway status received invalid drift evidence")
                        drift_categories = list(raw_categories)
                        rule_status: Literal["OK", "MISSING", "DRIFT"] = (
                            "OK"
                            if change.action == "none"
                            else "MISSING"
                            if not current.exists
                            else "DRIFT"
                        )
                        rule_entry: GatewayRuleStatus = {
                            "name": artifact.name,
                            "exists": current.exists,
                            "virtual_topic": artifact.virtual_topic,
                            "physical_topic": artifact.physical_topic,
                            "status": rule_status,
                            "interceptors_desired": len(desired.interceptors),
                            "interceptors_found": len(current.interceptors),
                            "scope": current.binding.scope_name,
                            "backend_fingerprint": (current.binding.endpoint_fingerprint),
                            "current_fingerprint": current.fingerprint,
                            "desired_fingerprint": desired.fingerprint,
                            "observed_physical_topic": current.physical_name,
                            "observed_physical_cluster": current.physical_cluster,
                        }
                        if drift_categories:
                            rule_entry["drift_categories"] = drift_categories
                        gateway_rules.append(rule_entry)
                        if is_text:
                            if rule_status == "OK":
                                fmt.print(
                                    f"  [green]OK[/green] {artifact.name} "
                                    f"({artifact.virtual_topic} -> "
                                    f"{artifact.physical_topic}, interceptors: "
                                    f"{len(current.interceptors)}/"
                                    f"{len(desired.interceptors)})"
                                )
                            elif rule_status == "DRIFT":
                                fmt.print(
                                    f"  [yellow]DRIFT[/yellow] {artifact.name} "
                                    f"({', '.join(drift_categories)})"
                                )
                            else:
                                fmt.print(f"  [red]MISSING[/red] {artifact.name}")
            elif is_text:
                fmt.print("  [yellow]No Gateway configured[/yellow]")
            if gd is None:
                observation_incomplete = True

        fmt.set_data(data)

        if is_text:
            fmt.print("")
            healthy_topics = sum(1 for topic in topics if topic["status"] == "OK")
            missing_topics = sum(1 for topic in topics if topic["status"] == "MISSING")
            drifted_topics = sum(1 for topic in topics if topic["status"] == "DRIFT")
            running = sum(1 for job in flink_jobs if job["status"] == "RUNNING")
            other = len(flink_jobs) - running
            parts: list[str] = []
            if topics:
                topic_summary = f"Topics: {healthy_topics} OK, {missing_topics} missing"
                if drifted_topics:
                    topic_summary += f", {drifted_topics} drift"
                parts.append(topic_summary)
            if flink_jobs:
                parts.append(f"Jobs: {running} running, {other} other")
            if parts:
                fmt.print(f"[dim]Summary: {' | '.join(parts)}[/dim]")
        elif fmt.format != "json":
            # Legacy --format json
            fmt.print(json.dumps(data, indent=2))

        unhealthy = health and (
            observation_incomplete
            or bool(fmt.get_result().errors)
            or any(not schema["exists"] for schema in schemas)
            or any(
                source["observation"] == "verified" and not source["exists"]
                for source in source_topics
            )
            or any(topic["status"] != "OK" for topic in topics)
            or any(not job["exists"] or job["status"] != "RUNNING" for job in flink_jobs)
            or any(not job["exists"] or job["status"] != "running" for job in kafka_streams_jobs)
            or any(
                not connector["exists"] or connector["status"] != "RUNNING"
                for connector in connectors
            )
            or any(rule["status"] != "OK" for rule in gateway_rules)
        )

        fmt.flush()
        if unhealthy:
            raise SystemExit(1)

    except (EnvVarError, ParseError, EnvironmentError) as e:
        handle_parse_error(fmt, e, ErrorCode.PARSE_ERROR)
    finally:
        close_deployers(*deployers_to_close)
