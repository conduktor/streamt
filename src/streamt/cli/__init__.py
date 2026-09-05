"""CLI for streamt — main group and entry point."""

from __future__ import annotations

from importlib import import_module

import click

from streamt import __version__


class _LazyCommandRef(click.Command):
    """Import location for a top-level command, resolved only when requested."""

    def __init__(self, name: str, module: str, attribute: str) -> None:
        super().__init__(name=name)
        self.module = module
        self.attribute = attribute


class _LazyGroup(click.Group):
    """Click group that replaces lazy references with their real commands."""

    def get_command(self, ctx: click.Context, cmd_name: str) -> click.Command | None:
        command = super().get_command(ctx, cmd_name)
        if not isinstance(command, _LazyCommandRef):
            return command
        resolved = getattr(import_module(command.module), command.attribute)
        if not isinstance(resolved, click.Command):
            raise TypeError("Lazy CLI target is not a Click command")
        self.commands[cmd_name] = resolved
        return resolved


@click.group(cls=_LazyGroup)
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


# Preserve the previous registration order and explicit aliases while keeping
# deployment and state modules outside the fresh CLI import boundary.
_COMMANDS = (
    ("adopt", "streamt.cli.commands.adopt", "adopt"),
    ("validate", "streamt.cli.commands.validate", "validate"),
    ("compile", "streamt.cli.commands.compile", "compile"),
    ("plan", "streamt.cli.commands.plan", "plan"),
    ("apply", "streamt.cli.commands.apply", "apply"),
    ("test", "streamt.cli.commands.test", "test"),
    ("lineage", "streamt.cli.commands.lineage", "lineage"),
    ("observe", "streamt.cli.commands.observe", "observe"),
    ("status", "streamt.cli.commands.status", "status"),
    ("state", "streamt.cli.commands.state_cmd", "state"),
    ("list", "streamt.cli.commands.list_cmd", "list_resources"),
    ("show", "streamt.cli.commands.show", "show_resource"),
    ("docs", "streamt.cli.commands.docs", "docs"),
    ("envs", "streamt.cli.commands.envs", "envs"),
    ("diff", "streamt.cli.commands.diff", "diff_resources"),
    ("build", "streamt.cli.commands.build", "build"),
    ("runtime", "streamt.cli.commands.runtime", "runtime"),
    ("init", "streamt.cli.commands.init", "init"),
    ("import", "streamt.cli.commands.import_cmd", "import_resources"),
    ("export", "streamt.cli.commands.export", "export"),
)

for _name, _module, _attribute in _COMMANDS:
    main.add_command(_LazyCommandRef(_name, _module, _attribute), name=_name)
