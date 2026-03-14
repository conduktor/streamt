"""streamt diff command — compare declared vs deployed state."""

from __future__ import annotations

from typing import Optional

import click

from streamt.cli.helpers import (
    close_deployers,
    get_project_path,
    handle_parse_error,
    make_formatter,
    make_kafka_deployer,
)
from streamt.output import StructuredError


@click.command("diff")
@click.option("--project-dir", "-p", type=click.Path(exists=True), help="Path to project directory")
@click.option(
    "--env", "-e", "environment", help="Target environment (reads from STREAMT_ENV if not set)"
)
@click.pass_context
def diff_resources(
    ctx: click.Context, project_dir: Optional[str], environment: Optional[str]
) -> None:
    """Compare declared resources against deployed state."""
    from streamt.core.parser import EnvVarError, ParseError, ProjectParser

    fmt = make_formatter(ctx, "diff")

    try:
        project_path = get_project_path(project_dir)
        parser = ProjectParser(project_path, environment=environment)
        project = parser.parse()
    except (EnvVarError, ParseError) as e:
        handle_parse_error(fmt, e, "PARSE_ERROR")
        return

    kd = make_kafka_deployer(project, fmt)
    if not kd:
        fmt.print_warning("Kafka not reachable — skipping diff")
        fmt.set_data({"resources": [], "warning": "kafka_unavailable"})
        fmt.flush()
        return

    resources: list[dict[str, object]] = []
    try:
        for model in project.models:
            tc = model.get_topic_config()
            topic_name = tc.name if tc and tc.name else model.name
            entry: dict[str, object] = {"type": "model", "name": model.name, "topic": topic_name}
            state = kd.get_topic_state(topic_name)
            if not state.exists:
                entry["status"] = "not_deployed"
                fmt.print(f"[yellow]{model.name}: not deployed[/yellow]")
            else:
                diffs: list[dict[str, object]] = []
                desired_p = tc.partitions if tc and tc.partitions else None
                if desired_p and state.partitions != desired_p:
                    diffs.append(
                        {"field": "partitions", "declared": desired_p, "actual": state.partitions}
                    )
                desired_rf = tc.replication_factor if tc and tc.replication_factor else None
                if desired_rf and state.replication_factor != desired_rf:
                    diffs.append(
                        {
                            "field": "replication_factor",
                            "declared": desired_rf,
                            "actual": state.replication_factor,
                        }
                    )
                entry["status"] = "drift" if diffs else "in_sync"
                entry["diffs"] = diffs
                if diffs:
                    fmt.print(f"[yellow]{model.name}: drift[/yellow]")
                    for d in diffs:
                        fmt.print(f"  {d['field']}: declared={d['declared']} actual={d['actual']}")
                else:
                    fmt.print(f"[green]{model.name}: in sync[/green]")
            resources.append(entry)

        for source in project.sources:
            entry = {"type": "source", "name": source.name, "topic": source.topic}
            state = kd.get_topic_state(source.topic)
            entry["exists"] = state.exists
            if state.exists:
                entry["partitions"] = state.partitions
                fmt.print(f"[green]{source.name}: exists (partitions: {state.partitions})[/green]")
            else:
                fmt.print(f"[red]{source.name}: topic not found[/red]")
            resources.append(entry)
    except Exception as e:
        fmt.add_error(StructuredError(code="DIFF_ERROR", message=str(e)))
        fmt.print_error(str(e))
    finally:
        close_deployers(kd)

    fmt.set_data({"resources": resources})
    fmt.flush()
