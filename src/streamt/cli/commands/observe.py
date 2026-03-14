"""streamt observe command — live runtime health of deployed models."""

from __future__ import annotations

import sys
from typing import Optional

import click

from streamt.cli.helpers import (
    close_deployers,
    get_project_path,
    handle_parse_error,
    make_flink_deployer,
    make_formatter,
    make_kafka_deployer,
)
from streamt.compiler.compiler import Compiler
from streamt.core.errors import ErrorCode
from streamt.output import OutputFormatter, StructuredError, get_output_format_from_context


def _health_color(health: str) -> str:
    return {"ok": "green", "warning": "yellow", "degraded": "red"}.get(health, "dim")


def _lag_str(lag: int) -> str:
    if lag == 0:
        return "[green]0[/green]"
    if lag < 10_000:
        return f"[yellow]{lag:,}[/yellow]"
    return f"[red]{lag:,}[/red]"


@click.command()
@click.option("--project-dir", "-p", type=click.Path(exists=True), help="Path to project directory")
@click.option("--env", "-e", "environment", help="Target environment")
@click.option(
    "--model", "-m", "model_filter", type=str, default=None, help="Observe a single model by name"
)
@click.pass_context
def observe(
    ctx: click.Context,
    project_dir: Optional[str],
    environment: Optional[str],
    model_filter: Optional[str],
) -> None:
    """Show live runtime health: consumer lag, Flink job status, backpressure.

    Connects to Kafka (for consumer group lag) and Flink (for job metrics).
    Does not modify any infrastructure.

    \b
    Examples:
      streamt observe
      streamt observe --model payments_clean
      streamt -o json observe
    """
    from streamt.deployer.observer import Observer

    fmt = make_formatter(ctx)
    is_text = get_output_format_from_context(ctx) == "text"

    project_path = get_project_path(project_dir)
    project, parse_err = handle_parse_error(project_path, environment, fmt)
    if parse_err:
        fmt.flush()
        sys.exit(1)

    kafka_deployer = make_kafka_deployer(project, fmt)
    flink_deployer = make_flink_deployer(project, fmt)

    try:
        # Compile to get the manifest (tells us what topics/jobs exist)
        compiler = Compiler(project)
        manifest = compiler.compile(dry_run=True)

        observer = Observer(manifest, kafka_deployer, flink_deployer)
        observations = observer.observe()

        if model_filter:
            observations = [o for o in observations if o.model_name == model_filter]
            if not observations:
                fmt.add_error(
                    StructuredError(
                        code=ErrorCode.NOT_FOUND,
                        message=f"Model '{model_filter}' not found in manifest",
                    )
                )
                fmt.print_error(f"Model '{model_filter}' not found in manifest")
                fmt.flush()
                sys.exit(1)

        if is_text:
            _render_text(fmt, observations)
        else:
            fmt.set_data(
                {
                    "models": [
                        {
                            "name": o.model_name,
                            "topic": o.topic,
                            "health": o.health,
                            "total_lag": o.total_lag,
                            "consumers": [
                                {"group_id": c.group_id, "lag": c.lag, "state": c.state}
                                for c in o.consumers
                            ],
                            "flink": (
                                {
                                    "job_id": o.flink.job_id,
                                    "state": o.flink.state,
                                    "records_in_per_second": o.flink.records_in_per_second,
                                    "is_backpressured": o.flink.is_backpressured,
                                }
                                if o.flink
                                else None
                            ),
                        }
                        for o in observations
                    ]
                }
            )

    finally:
        close_deployers(kafka_deployer=kafka_deployer, flink_deployer=flink_deployer)

    fmt.flush()


def _render_text(fmt: OutputFormatter, observations: list) -> None:
    from streamt.deployer.observer import ModelObservation

    if not observations:
        fmt.print("[dim]No deployed models found in manifest.[/dim]")
        return

    fmt.print(f"\n[bold]Runtime Health[/bold] ({len(observations)} models)\n")

    for obs in observations:
        obs: ModelObservation
        color = _health_color(obs.health)
        bullet = {"ok": "●", "warning": "◐", "degraded": "✗"}.get(obs.health, "○")

        fmt.print(f"  [{color}]{bullet}[/{color}] [bold]{obs.model_name}[/bold]", end="")

        if obs.topic:
            fmt.print(f"  [dim]topic:[/dim] {obs.topic}", end="")
        fmt.print("")

        # Flink job
        if obs.flink:
            state_color = "green" if obs.flink.state == "RUNNING" else "red"
            line = f"      [dim]flink:[/dim] [{state_color}]{obs.flink.state}[/{state_color}]"
            if obs.flink.records_in_per_second is not None:
                line += f"  [dim]{obs.flink.records_in_per_second:.1f} rec/s[/dim]"
            if obs.flink.is_backpressured:
                line += "  [yellow]⚠ backpressured[/yellow]"
            fmt.print(line)

        # Consumer groups
        if obs.consumers:
            total = obs.total_lag
            fmt.print(
                f"      [dim]consumers:[/dim] {len(obs.consumers)} group(s)  lag: {_lag_str(total)}"
            )
            for c in sorted(obs.consumers, key=lambda x: x.lag, reverse=True):
                fmt.print(f"        [dim]·[/dim] {c.group_id}  {_lag_str(c.lag)}")
        elif obs.topic:
            fmt.print("      [dim]consumers:[/dim] [dim]none[/dim]")

        fmt.print("")
