"""CLI for streamt — main group and entry point."""

from __future__ import annotations

import click

from streamt import __version__

from .commands import (
    apply,
    compile,
    diff,
    docs,
    envs,
    init,
    lineage,
    list_cmd,
    observe,
    plan,
    show,
    status,
    test,
    validate,
)


@click.group()
@click.version_option(version=__version__)
@click.option(
    "--output",
    "-o",
    type=click.Choice(["text", "json"]),
    default="text",
    help="Output format (text=human-readable, json=machine-readable)",
)
@click.option("--quiet", "-q", is_flag=True, help="Suppress all output except errors")
@click.option("--verbose", "-v", is_flag=True, help="Enable debug-level output")
@click.pass_context
def main(ctx: click.Context, output: str, quiet: bool, verbose: bool) -> None:
    """streamt - dbt for streaming.

    Declarative streaming pipelines for Kafka, Flink, and Connect.

    Use --output json for machine-readable output (LLM agents, CI/CD).
    """
    ctx.ensure_object(dict)
    ctx.obj["output"] = output
    ctx.obj["quiet"] = quiet
    ctx.obj["verbose"] = verbose
    if verbose:
        import logging

        logging.basicConfig(level=logging.DEBUG, format="%(name)s %(levelname)s: %(message)s")


# Register all commands
main.add_command(validate.validate)
main.add_command(compile.compile)
main.add_command(plan.plan)
main.add_command(apply.apply)
main.add_command(test.test)
main.add_command(lineage.lineage)
main.add_command(observe.observe)
main.add_command(status.status)
main.add_command(list_cmd.list_resources, name="list")
main.add_command(show.show_resource, name="show")
main.add_command(docs.docs)
main.add_command(envs.envs)
main.add_command(diff.diff_resources, name="diff")
main.add_command(init.init)
