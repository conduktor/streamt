"""streamt apply command."""

from __future__ import annotations

import sys
from typing import Optional

import click

from streamt.cli.helpers import get_project_path, handle_parse_error, make_formatter
from streamt.core.errors import ErrorCode
from streamt.output import StructuredError


@click.command()
@click.option("--project-dir", "-p", type=click.Path(exists=True), help="Path to project directory")
@click.option("--env", "-e", "environment", help="Target environment (reads from STREAMT_ENV if not set)")
@click.option("--target", "-t", help="Deploy only this model and its dependencies")
@click.option("--select", "-s", help="Select models by tag (e.g., 'tag:payments')")
@click.option("--confirm", is_flag=True, help="Skip confirmation prompt for protected environments")
@click.option("--confirm-env", type=str, default=None, help="Non-interactive confirm: pass env name (for agents/CI)")
@click.option("--force", is_flag=True, help="Override safety checks (allow destructive operations)")
@click.pass_context
def apply(
    ctx: click.Context,
    project_dir: Optional[str],
    environment: Optional[str],
    target: Optional[str],
    select: Optional[str],
    confirm: bool,
    confirm_env: Optional[str],
    force: bool,
) -> None:
    """Deploy the project."""
    from streamt.cli.commands.plan import (
        _make_connect_deployer,
        _make_flink_deployer,
        _make_kafka_deployer,
        _make_sr_deployer,
    )
    from streamt.compiler import Compiler
    from streamt.core.parser import EnvVarError, ParseError, ProjectParser
    from streamt.core.environment import EnvironmentError
    from streamt.core.validator import ProjectValidator
    from streamt.deployer.planner import DeploymentPlanner

    fmt = make_formatter(ctx, "apply")
    project_path = get_project_path(project_dir)

    try:
        parser = ProjectParser(
            project_path, environment=environment,
            warn_callback=lambda msg: fmt.print(msg),
        )
        project = parser.parse()

        # Protected environment confirmation
        if parser.env_config and parser.env_config.environment.protected:
            env_name = parser.env_config.environment.name
            fmt.print_warning(f"Deploying to protected environment '{env_name}'")

            if confirm_env:
                if confirm_env != env_name:
                    fmt.add_error(StructuredError(
                        code=ErrorCode.ENVIRONMENT_ERROR,
                        message=f"--confirm-env '{confirm_env}' does not match '{env_name}'",
                    ))
                    fmt.print_error(f"--confirm-env '{confirm_env}' does not match '{env_name}'")
                    fmt.flush()
                    sys.exit(1)
            elif not confirm:
                if sys.stdin.isatty():
                    fmt.print_warning(f"'{env_name}' is a protected environment.")
                    user_input = click.prompt(f"Type '{env_name}' to confirm", default="", show_default=False)
                    if user_input != env_name:
                        fmt.print_error("Aborted")
                        fmt.set_status("error")
                        fmt.flush()
                        sys.exit(1)
                else:
                    fmt.add_error(StructuredError(
                        code=ErrorCode.ENVIRONMENT_ERROR,
                        message=f"Protected env '{env_name}'. Use --confirm or --confirm-env {env_name}.",
                    ))
                    fmt.print_error(f"'{env_name}' is protected. Use --confirm or --confirm-env in CI.")
                    fmt.flush()
                    sys.exit(1)

        # Destructive safety
        if parser.env_config and not parser.env_config.safety.allow_destructive:
            env_name = parser.env_config.environment.name
            if not force:
                fmt.add_error(StructuredError(
                    code=ErrorCode.ENVIRONMENT_ERROR,
                    message=f"Destructive ops blocked for '{env_name}'. Use --force.",
                ))
                fmt.print_error(f"Destructive ops blocked for '{env_name}'. Use --force to override.")
                fmt.flush()
                sys.exit(1)
            fmt.print_warning(f"--force used, allowing destructive ops on '{env_name}'")

        validator = ProjectValidator(project)
        result = validator.validate()
        if not result.is_valid:
            for error in result.errors:
                fmt.add_error(StructuredError(code=ErrorCode.PARSE_ERROR, message=error.message))
                fmt.print_error(error.message)
            fmt.flush()
            sys.exit(1)

        compiler = Compiler(project)
        manifest = compiler.compile()

        # Create deployers — reuse resilient helpers from plan module
        sr = _make_sr_deployer(project, fmt)
        kafka = _make_kafka_deployer(project, fmt)
        flink = _make_flink_deployer(project, fmt)
        connect = _make_connect_deployer(project, fmt)

        planner = DeploymentPlanner(
            manifest, schema_registry_deployer=sr, kafka_deployer=kafka,
            flink_deployer=flink, connect_deployer=connect,
        )
        results = planner.apply()
        fmt.set_data(results)

        if results["created"]:
            fmt.print("\n[green]Created:[/green]")
            for item in results["created"]:
                fmt.print(f"  + {item}")
        if results["updated"]:
            fmt.print("\n[yellow]Updated:[/yellow]")
            for item in results["updated"]:
                fmt.print(f"  ~ {item}")
        if results["unchanged"]:
            fmt.print("\n[dim]Unchanged:[/dim]")
            for item in results["unchanged"]:
                fmt.print(f"  = {item}")
        if results["errors"]:
            fmt.set_status("error")
            fmt.print("\n[red]Errors:[/red]")
            for item in results["errors"]:
                fmt.add_error(StructuredError(code=ErrorCode.PARSE_ERROR, message=item))
                fmt.print_error(item)
            fmt.flush()
            sys.exit(1)

        fmt.print("\n[green]Apply complete[/green]")
        fmt.flush()

    except (EnvVarError, ParseError, EnvironmentError) as e:
        handle_parse_error(fmt, e, ErrorCode.PARSE_ERROR)
    except Exception as e:
        fmt.add_error(StructuredError(code=ErrorCode.PARSE_ERROR, message=f"Cannot connect: {e}"))
        fmt.print_error(f"Cannot connect: {e}")
        fmt.flush()
        sys.exit(1)
