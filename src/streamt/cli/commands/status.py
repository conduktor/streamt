"""streamt status command."""

from __future__ import annotations

import fnmatch
import json
from collections.abc import Generator
from contextlib import contextmanager
from typing import Optional

import click

from streamt.cli.helpers import (
    close_deployers,
    get_project_path,
    handle_parse_error,
    make_connect_deployer,
    make_flink_deployer,
    make_formatter,
    make_gateway_deployer,
    make_kafka_deployer,
    make_sr_deployer,
)
from streamt.core.errors import ErrorCode
from streamt.output import OutputFormatter, StructuredError, get_output_format_from_context


@contextmanager
def _deployer_section(
    fmt: OutputFormatter, is_text: bool, service: str
) -> Generator[None, None, None]:
    """Catch and report deployer errors uniformly across status sections."""
    try:
        yield
    except Exception as e:
        fmt.add_error(StructuredError(code=ErrorCode.CONNECTION_REFUSED, message=f"{service}: {e}"))
        if is_text:
            fmt.print(f"  [yellow]Cannot connect to {service}: {e}[/yellow]")


@click.command()
@click.option("--project-dir", "-p", type=click.Path(exists=True), help="Path to project directory")
@click.option(
    "--env", "-e", "environment", help="Target environment (reads from STREAMT_ENV if not set)"
)
@click.option("--lag", is_flag=True, help="Show consumer lag for topics")
@click.option("--consumer-groups", is_flag=True, help="Show per-consumer-group lag")
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
    output_format: Optional[str],
    filter_pattern: Optional[str],
) -> None:
    """Show status of deployed resources."""
    from streamt.compiler import Compiler
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

        data: dict[str, object] = {
            "project": project.project.name,
            "schemas": [],
            "topics": [],
            "flink_jobs": [],
            "connectors": [],
            "gateway_rules": [],
        }

        # Schemas
        if manifest.artifacts.get("schemas"):
            if is_text:
                fmt.print("\n[cyan]Schemas:[/cyan]")
            sd = make_sr_deployer(project, fmt)
            if sd:
                deployers_to_close.append(sd)
                with _deployer_section(fmt, is_text, "Schema Registry"):
                    for s in manifest.artifacts["schemas"]:
                        if not matches(s["subject"]):
                            continue
                        state = sd.get_schema_state(s["subject"])
                        entry = {
                            "subject": s["subject"],
                            "exists": state.exists,
                            "version": state.version if state.exists else None,
                            "schema_type": state.schema_type if state.exists else None,
                        }
                        data["schemas"].append(entry)
                        if is_text:
                            if state.exists:
                                fmt.print(
                                    f"  [green]OK[/green] {s['subject']} (v{state.version}, {state.schema_type})"
                                )
                            else:
                                fmt.print(f"  [red]MISSING[/red] {s['subject']}")
            elif is_text:
                fmt.print("  [yellow]No Schema Registry configured[/yellow]")

        # Source Topics (STATUS-2)
        if project.sources:
            data["source_topics"] = []
            if is_text:
                fmt.print("\n[cyan]Source Topics:[/cyan]")
            skd = make_kafka_deployer(project, fmt)
            if skd:
                deployers_to_close.append(skd)
                with _deployer_section(fmt, is_text, "Kafka (sources)"):
                    for src in project.sources:
                        if not matches(src.name) and not matches(src.topic):
                            continue
                        state = skd.get_topic_state(src.topic)
                        entry = {
                            "name": src.name,
                            "topic": src.topic,
                            "exists": state.exists,
                            "partitions": state.partitions if state.exists else None,
                        }
                        data["source_topics"].append(entry)
                        if is_text:
                            if state.exists:
                                fmt.print(
                                    f"  [green]OK[/green] {src.name} → {src.topic} (partitions: {state.partitions})"
                                )
                            else:
                                fmt.print(f"  [red]MISSING[/red] {src.name} → {src.topic}")
            elif is_text:
                fmt.print("  [yellow]No Kafka configured[/yellow]")

        # Topics
        if is_text:
            fmt.print("\n[cyan]Topics:[/cyan]")
        kd = make_kafka_deployer(project, fmt)
        if kd:
            deployers_to_close.append(kd)
            with _deployer_section(fmt, is_text, "Kafka"):
                for t in manifest.artifacts.get("topics", []):
                    if not matches(t["name"]):
                        continue
                    state = kd.get_topic_state(t["name"])
                    entry: dict[str, object] = {
                        "name": t["name"],
                        "exists": state.exists,
                        "partitions": state.partitions if state.exists else None,
                        "replication_factor": state.replication_factor if state.exists else None,
                    }
                    if lag and state.exists:
                        entry["message_count"] = kd.get_topic_message_count(t["name"])
                    drifts: list[dict[str, object]] = []
                    if state.exists:
                        desired_p = t.get("partitions")
                        desired_rf = t.get("replication_factor")
                        if desired_p is not None and state.partitions != desired_p:
                            drifts.append(
                                {
                                    "field": "partitions",
                                    "actual": state.partitions,
                                    "desired": desired_p,
                                }
                            )
                        if desired_rf is not None and state.replication_factor != desired_rf:
                            drifts.append(
                                {
                                    "field": "replication_factor",
                                    "actual": state.replication_factor,
                                    "desired": desired_rf,
                                }
                            )
                    if drifts:
                        entry["status"] = "DRIFT"
                        entry["drifts"] = drifts
                    elif state.exists:
                        entry["status"] = "OK"
                    else:
                        entry["status"] = "MISSING"
                    data["topics"].append(entry)
                    if is_text:
                        if state.exists and drifts:
                            drift_parts = [
                                f"{d['field']}: {d['actual']} → {d['desired']}" for d in drifts
                            ]
                            fmt.print(
                                f"  [yellow]DRIFT[/yellow] {t['name']}  {', '.join(drift_parts)}"
                            )
                        elif state.exists:
                            line = f"  [green]OK[/green] {t['name']} (partitions: {state.partitions}, rf: {state.replication_factor})"
                            if "message_count" in entry:
                                line += f" [dim]~{entry['message_count']} msgs[/dim]"
                            fmt.print(line)
                        else:
                            fmt.print(f"  [red]MISSING[/red] {t['name']}")

        # Consumer group lag (STATUS-3)
        if consumer_groups and kd:
            data["consumer_groups"] = []
            if is_text:
                fmt.print("\n[cyan]Consumer Groups:[/cyan]")
            with _deployer_section(fmt, is_text, "Kafka (consumer groups)"):
                groups = kd.get_consumer_groups()
                topic_names = [t["name"] for t in manifest.artifacts.get("topics", [])]
                for group_id in sorted(groups):
                    group_data: dict[str, object] = {"group_id": group_id, "topics": []}
                    for topic_name in topic_names:
                        if not matches(topic_name):
                            continue
                        lag_info = kd.get_consumer_group_lag(group_id, topic_name)
                        if lag_info is not None:
                            tlag = {
                                "topic": topic_name,
                                "total_lag": lag_info.total_lag,
                                "partitions": lag_info.partitions,
                            }
                            group_data["topics"].append(tlag)
                    if group_data["topics"]:
                        data["consumer_groups"].append(group_data)
                        if is_text:
                            total = sum(t["total_lag"] for t in group_data["topics"])
                            color = "green" if total == 0 else "yellow" if total < 1000 else "red"
                            fmt.print(f"  [{color}]{group_id}[/{color}] total_lag={total}")
                            for tlag in group_data["topics"]:
                                fmt.print(f"    {tlag['topic']}: lag={tlag['total_lag']}")

        # Flink jobs
        if manifest.artifacts.get("flink_jobs"):
            if is_text:
                fmt.print("\n[cyan]Flink Jobs:[/cyan]")
            fd = make_flink_deployer(project, fmt, state_dir=project_path / ".streamt")
            if fd:
                deployers_to_close.append(fd)
                with _deployer_section(fmt, is_text, "Flink"):
                    for j in manifest.artifacts["flink_jobs"]:
                        if not matches(j["name"]):
                            continue
                        state = fd.get_job_state(j["name"])
                        entry = {
                            "name": j["name"],
                            "exists": state.exists,
                            "job_id": state.job_id if state.exists else None,
                            "status": state.status if state.exists else None,
                        }
                        data["flink_jobs"].append(entry)
                        if is_text:
                            if state.exists:
                                color = "green" if state.status == "RUNNING" else "yellow"
                                fmt.print(f"  [{color}]{state.status}[/{color}] {j['name']}")
                            else:
                                fmt.print(f"  [red]NOT FOUND[/red] {j['name']}")
            elif is_text:
                fmt.print("  [yellow]No Flink configured[/yellow]")

        # Connectors
        if manifest.artifacts.get("connectors"):
            if is_text:
                fmt.print("\n[cyan]Connectors:[/cyan]")
            cd = make_connect_deployer(project, fmt)
            if cd:
                deployers_to_close.append(cd)
                with _deployer_section(fmt, is_text, "Connect"):
                    for c in manifest.artifacts["connectors"]:
                        if not matches(c["name"]):
                            continue
                        state = cd.get_connector_state(c["name"])
                        entry = {
                            "name": c["name"],
                            "exists": state.exists,
                            "status": state.status if state.exists else None,
                        }
                        data["connectors"].append(entry)
                        if is_text:
                            if state.exists:
                                color = "green" if state.status == "RUNNING" else "yellow"
                                fmt.print(f"  [{color}]{state.status}[/{color}] {c['name']}")
                            else:
                                fmt.print(f"  [red]NOT FOUND[/red] {c['name']}")
            elif is_text:
                fmt.print("  [yellow]No Connect configured[/yellow]")

        # Gateway rules
        if manifest.artifacts.get("gateway_rules"):
            if is_text:
                fmt.print("\n[cyan]Gateway Rules:[/cyan]")
            gd = make_gateway_deployer(project, fmt)
            if gd:
                deployers_to_close.append(gd)
                with _deployer_section(fmt, is_text, "Gateway"):
                    for r in manifest.artifacts["gateway_rules"]:
                        if not matches(r["name"]):
                            continue
                        alias = gd.get_alias_topic(r["virtualTopic"])
                        exists = alias is not None
                        entry: dict[str, object] = {
                            "name": r["name"],
                            "exists": exists,
                            "virtual_topic": r["virtualTopic"],
                            "physical_topic": r["physicalTopic"],
                        }
                        desired_interceptors = r.get("interceptors", [])
                        if exists and desired_interceptors:
                            found = sum(
                                1
                                for ic in desired_interceptors
                                if ic.get("name") and gd.get_interceptor(ic["name"])
                            )
                            entry["interceptors_desired"] = len(desired_interceptors)
                            entry["interceptors_found"] = found
                        data["gateway_rules"].append(entry)
                        if is_text:
                            if exists:
                                ic_info = ""
                                if "interceptors_found" in entry:
                                    ic_info = f", interceptors: {entry['interceptors_found']}/{entry['interceptors_desired']}"
                                fmt.print(
                                    f"  [green]OK[/green] {r['name']} ({r['virtualTopic']} -> {r['physicalTopic']}{ic_info})"
                                )
                            else:
                                fmt.print(f"  [red]MISSING[/red] {r['name']}")
            elif is_text:
                fmt.print("  [yellow]No Gateway configured[/yellow]")

        fmt.set_data(data)

        if is_text:
            fmt.print("")
            healthy = sum(1 for t in data["topics"] if t["exists"])
            missing_t = sum(1 for t in data["topics"] if not t["exists"])
            running = sum(1 for j in data["flink_jobs"] if j.get("status") == "RUNNING")
            other = sum(
                1 for j in data["flink_jobs"] if j.get("status") and j["status"] != "RUNNING"
            )
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
    finally:
        close_deployers(*deployers_to_close)
