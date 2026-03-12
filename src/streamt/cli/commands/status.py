"""streamt status command."""

from __future__ import annotations

import fnmatch
import json
from typing import Any, Optional

import click

from streamt.cli.helpers import get_project_path, handle_parse_error, make_formatter
from streamt.core.errors import ErrorCode
from streamt.output import get_output_format_from_context


@click.command()
@click.option("--project-dir", "-p", type=click.Path(exists=True), help="Path to project directory")
@click.option("--env", "-e", "environment", help="Target environment (reads from STREAMT_ENV if not set)")
@click.option("--lag", is_flag=True, help="Show consumer lag for topics")
@click.option("--format", "output_format", type=click.Choice(["text", "json"]), default=None, help="Output format (overrides global --output)")
@click.option("--filter", "filter_pattern", type=str, help="Filter resources by name pattern (glob-style)")
@click.pass_context
def status(
    ctx: click.Context,
    project_dir: Optional[str],
    environment: Optional[str],
    lag: bool,
    output_format: Optional[str],
    filter_pattern: Optional[str],
) -> None:
    """Show status of deployed resources."""
    from streamt.compiler import Compiler
    from streamt.core.parser import EnvVarError, ParseError, ProjectParser
    from streamt.core.environment import EnvironmentError
    from streamt.deployer.connect import ConnectDeployer
    from streamt.deployer.flink import FlinkDeployer
    from streamt.deployer.kafka import KafkaDeployer
    from streamt.deployer.schema_registry import SchemaRegistryDeployer

    fmt = make_formatter(ctx, "status")
    project_path = get_project_path(project_dir)
    effective_format = output_format or get_output_format_from_context(ctx)
    is_text = effective_format == "text"

    def matches(name: str) -> bool:
        return not filter_pattern or fnmatch.fnmatch(name, filter_pattern)

    try:
        parser = ProjectParser(
            project_path, environment=environment,
            warn_callback=lambda msg: fmt.print(msg),
        )
        project = parser.parse()
        compiler = Compiler(project)
        manifest = compiler.compile(dry_run=True)

        data: dict[str, Any] = {
            "project": project.project.name,
            "schemas": [], "topics": [], "flink_jobs": [], "connectors": [],
        }

        # Schemas
        if manifest.artifacts.get("schemas"):
            if is_text:
                fmt.print("\n[cyan]Schemas:[/cyan]")
            if project.runtime.schema_registry:
                try:
                    sd = SchemaRegistryDeployer(
                        project.runtime.schema_registry.url,
                        username=project.runtime.schema_registry.username,
                        password=project.runtime.schema_registry.password,
                    )
                    for s in manifest.artifacts["schemas"]:
                        if not matches(s["subject"]):
                            continue
                        state = sd.get_schema_state(s["subject"])
                        entry = {"subject": s["subject"], "exists": state.exists,
                                 "version": state.version if state.exists else None,
                                 "schema_type": state.schema_type if state.exists else None}
                        data["schemas"].append(entry)
                        if is_text:
                            if state.exists:
                                fmt.print(f"  [green]OK[/green] {s['subject']} (v{state.version}, {state.schema_type})")
                            else:
                                fmt.print(f"  [red]MISSING[/red] {s['subject']}")
                except Exception as e:
                    if is_text:
                        fmt.print(f"  [yellow]Cannot connect to Schema Registry: {e}[/yellow]")
            elif is_text:
                fmt.print("  [yellow]No Schema Registry configured[/yellow]")

        # Topics
        if is_text:
            fmt.print("\n[cyan]Topics:[/cyan]")
        try:
            kd = KafkaDeployer(project.runtime.kafka.bootstrap_servers)
            for t in manifest.artifacts.get("topics", []):
                if not matches(t["name"]):
                    continue
                state = kd.get_topic_state(t["name"])
                entry: dict[str, Any] = {"name": t["name"], "exists": state.exists,
                         "partitions": state.partitions if state.exists else None,
                         "replication_factor": state.replication_factor if state.exists else None}
                if lag and state.exists:
                    entry["message_count"] = kd.get_topic_message_count(t["name"])
                data["topics"].append(entry)
                if is_text:
                    if state.exists:
                        line = f"  [green]OK[/green] {t['name']} (partitions: {state.partitions}, rf: {state.replication_factor})"
                        if "message_count" in entry:
                            line += f" [dim]~{entry['message_count']} msgs[/dim]"
                        fmt.print(line)
                    else:
                        fmt.print(f"  [red]MISSING[/red] {t['name']}")
        except Exception as e:
            if is_text:
                fmt.print(f"  [yellow]Cannot connect to Kafka: {e}[/yellow]")

        # Flink jobs
        if manifest.artifacts.get("flink_jobs"):
            if is_text:
                fmt.print("\n[cyan]Flink Jobs:[/cyan]")
            if project.runtime.flink and project.runtime.flink.clusters:
                try:
                    default = project.runtime.flink.default
                    if default and default in project.runtime.flink.clusters:
                        cfg = project.runtime.flink.clusters[default]
                        if cfg.rest_url:
                            fd = FlinkDeployer(cfg.rest_url)
                            for j in manifest.artifacts["flink_jobs"]:
                                if not matches(j["name"]):
                                    continue
                                state = fd.get_job_state(j["name"])
                                entry = {"name": j["name"], "exists": state.exists,
                                         "job_id": state.job_id if state.exists else None,
                                         "status": state.status if state.exists else None}
                                data["flink_jobs"].append(entry)
                                if is_text:
                                    if state.exists:
                                        color = "green" if state.status == "RUNNING" else "yellow"
                                        fmt.print(f"  [{color}]{state.status}[/{color}] {j['name']}")
                                    else:
                                        fmt.print(f"  [red]NOT FOUND[/red] {j['name']}")
                except Exception as e:
                    if is_text:
                        fmt.print(f"  [yellow]Cannot connect to Flink: {e}[/yellow]")
            elif is_text:
                fmt.print("  [yellow]No Flink configured[/yellow]")

        # Connectors
        if manifest.artifacts.get("connectors"):
            if is_text:
                fmt.print("\n[cyan]Connectors:[/cyan]")
            if project.runtime.connect and project.runtime.connect.clusters:
                try:
                    default = project.runtime.connect.default
                    if default and default in project.runtime.connect.clusters:
                        cfg = project.runtime.connect.clusters[default]
                        cd = ConnectDeployer(cfg.rest_url)
                        for c in manifest.artifacts["connectors"]:
                            if not matches(c["name"]):
                                continue
                            state = cd.get_connector_state(c["name"])
                            entry = {"name": c["name"], "exists": state.exists,
                                     "status": state.status if state.exists else None}
                            data["connectors"].append(entry)
                            if is_text:
                                if state.exists:
                                    color = "green" if state.status == "RUNNING" else "yellow"
                                    fmt.print(f"  [{color}]{state.status}[/{color}] {c['name']}")
                                else:
                                    fmt.print(f"  [red]NOT FOUND[/red] {c['name']}")
                except Exception as e:
                    if is_text:
                        fmt.print(f"  [yellow]Cannot connect to Connect: {e}[/yellow]")
            elif is_text:
                fmt.print("  [yellow]No Connect configured[/yellow]")

        fmt.set_data(data)

        if is_text:
            fmt.print("")
            healthy = sum(1 for t in data["topics"] if t["exists"])
            missing_t = sum(1 for t in data["topics"] if not t["exists"])
            running = sum(1 for j in data["flink_jobs"] if j.get("status") == "RUNNING")
            other = sum(1 for j in data["flink_jobs"] if j.get("status") and j["status"] != "RUNNING")
            parts = []
            if data["topics"]:
                parts.append(f"Topics: {healthy} OK, {missing_t} missing")
            if data["flink_jobs"]:
                parts.append(f"Jobs: {running} running, {other} other")
            if parts:
                fmt.print(f"[dim]Summary: {' | '.join(parts)}[/dim]")
        elif fmt.format != "json":
            # Legacy --format json
            fmt.print(json.dumps(data, indent=2))

        fmt.flush()

    except (EnvVarError, ParseError, EnvironmentError) as e:
        handle_parse_error(fmt, e, ErrorCode.PARSE_ERROR)
