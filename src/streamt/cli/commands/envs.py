"""streamt envs commands."""

from __future__ import annotations

import sys
from typing import Any, Optional

import click

from streamt.cli.helpers import get_project_path, handle_parse_error, make_formatter
from streamt.core.environment import EnvironmentError, EnvironmentManager, mask_secrets
from streamt.core.errors import ErrorCode
from streamt.output import StructuredError


@click.group()
def envs() -> None:
    """Environment management commands."""
    pass


@envs.command("list")
@click.option("--project-dir", "-p", type=click.Path(exists=True), help="Path to project directory")
@click.pass_context
def envs_list(ctx: click.Context, project_dir: Optional[str]) -> None:
    """List available environments."""
    fmt = make_formatter(ctx, "envs list")
    project_path = get_project_path(project_dir)
    env_manager = EnvironmentManager(project_path)

    if env_manager.mode == "single":
        fmt.set_data({"mode": "single", "environments": []})
        fmt.print("No environments configured (single-env mode)")
        fmt.flush()
        return

    environments = env_manager.discover_environments()
    if not environments:
        fmt.set_data({"mode": "multi", "environments": []})
        fmt.print("No environment files found in environments/ directory")
        fmt.flush()
        return

    env_list: list[dict[str, Any]] = []
    for env_name in environments:
        try:
            env_config = env_manager.load_environment(env_name)
            entry: dict[str, Any] = {
                "name": env_name,
                "description": env_config.environment.description,
                "protected": env_config.environment.protected,
            }
            env_list.append(entry)
            desc = env_config.environment.description or ""
            prot = " \\[protected]" if env_config.environment.protected else ""
            fmt.print(f"{env_name:12} {desc}{prot}" if desc else f"{env_name}{prot}")
        except EnvironmentError as e:
            env_list.append({"name": env_name, "error": str(e)})
            fmt.print(f"{env_name:12} [red]Error: {e}[/red]")

    fmt.set_data({"mode": "multi", "environments": env_list})
    fmt.flush()


@envs.command("show")
@click.option("--project-dir", "-p", type=click.Path(exists=True), help="Path to project directory")
@click.argument("name")
@click.pass_context
def envs_show(ctx: click.Context, project_dir: Optional[str], name: str) -> None:
    """Show resolved configuration for an environment."""
    import yaml

    fmt = make_formatter(ctx, "envs show")
    project_path = get_project_path(project_dir)
    env_manager = EnvironmentManager(project_path)

    if env_manager.mode == "single":
        fmt.add_error(StructuredError(
            code=ErrorCode.ENVIRONMENT_ERROR,
            message="No environments configured (single-env mode)",
        ))
        fmt.print_error("No environments configured (single-env mode)")
        fmt.flush()
        sys.exit(1)

    try:
        env_config = env_manager.load_environment(name)
        masked_runtime = mask_secrets(env_config.runtime)

        data: dict[str, Any] = {
            "name": env_config.environment.name,
            "description": env_config.environment.description,
            "protected": env_config.environment.protected,
            "runtime": masked_runtime,
            "safety": {
                "confirm_apply": env_config.safety.confirm_apply,
                "allow_destructive": env_config.safety.allow_destructive,
            },
        }
        fmt.set_data(data)

        fmt.print(f"[cyan]Environment:[/cyan] {env_config.environment.name}")
        if env_config.environment.description:
            fmt.print(f"[cyan]Description:[/cyan] {env_config.environment.description}")
        if env_config.environment.protected:
            fmt.print("[cyan]Protected:[/cyan] [yellow]yes[/yellow]")
        fmt.print("\n[cyan]Runtime:[/cyan]")
        fmt.print(yaml.dump(masked_runtime, default_flow_style=False, sort_keys=False))
        fmt.print("[cyan]Safety:[/cyan]")
        fmt.print(f"  confirm_apply: {env_config.safety.confirm_apply}")
        fmt.print(f"  allow_destructive: {env_config.safety.allow_destructive}")
        fmt.flush()

    except EnvironmentError as e:
        handle_parse_error(fmt, e, ErrorCode.ENVIRONMENT_ERROR)
