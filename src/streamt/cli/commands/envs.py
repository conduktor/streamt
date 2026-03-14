"""streamt envs commands."""

from __future__ import annotations

import sys
from typing import Optional

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

    env_list: list[dict[str, object]] = []
    for env_name in environments:
        try:
            env_config = env_manager.load_environment(env_name)
            entry: dict[str, object] = {
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
        fmt.add_error(
            StructuredError(
                code=ErrorCode.ENVIRONMENT_ERROR,
                message="No environments configured (single-env mode)",
            )
        )
        fmt.print_error("No environments configured (single-env mode)")
        fmt.flush()
        sys.exit(1)

    try:
        env_config = env_manager.load_environment(name)
        masked_runtime = mask_secrets(env_config.runtime)

        data: dict[str, object] = {
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


def _flatten_dict(d: dict, prefix: str = "") -> dict[str, object]:
    """Flatten a nested dict to dot-separated keys."""
    items: dict[str, object] = {}
    for k, v in d.items():
        key = f"{prefix}.{k}" if prefix else k
        if isinstance(v, dict):
            items.update(_flatten_dict(v, key))
        else:
            items[key] = v
    return items


@envs.command("diff")
@click.option("--project-dir", "-p", type=click.Path(exists=True), help="Path to project directory")
@click.argument("env_a")
@click.argument("env_b")
@click.pass_context
def envs_diff(ctx: click.Context, project_dir: Optional[str], env_a: str, env_b: str) -> None:
    """Compare two environment configurations."""
    fmt = make_formatter(ctx, "envs diff")
    project_path = get_project_path(project_dir)
    env_manager = EnvironmentManager(project_path)

    if env_manager.mode == "single":
        fmt.add_error(
            StructuredError(
                code=ErrorCode.ENVIRONMENT_ERROR,
                message="No environments configured (single-env mode)",
            )
        )
        fmt.print_error("No environments configured (single-env mode)")
        fmt.flush()
        sys.exit(1)

    try:
        cfg_a = env_manager.load_environment(env_a)
        cfg_b = env_manager.load_environment(env_b)

        flat_a = _flatten_dict(cfg_a.runtime)
        flat_b = _flatten_dict(cfg_b.runtime)
        all_keys = sorted(set(flat_a) | set(flat_b))

        diffs: list[dict[str, object]] = []
        for key in all_keys:
            val_a = flat_a.get(key)
            val_b = flat_b.get(key)
            if val_a != val_b:
                diffs.append({"key": key, env_a: val_a, env_b: val_b})

        # Compare safety settings
        safety_fields = [
            ("confirm_apply", "safety.confirm_apply"),
            ("allow_destructive", "safety.allow_destructive"),
        ]
        for attr, label in safety_fields:
            va = getattr(cfg_a.safety, attr)
            vb = getattr(cfg_b.safety, attr)
            if va != vb:
                diffs.append({"key": label, env_a: va, env_b: vb})

        if cfg_a.environment.protected != cfg_b.environment.protected:
            diffs.append(
                {
                    "key": "protected",
                    env_a: cfg_a.environment.protected,
                    env_b: cfg_b.environment.protected,
                }
            )

        if not diffs:
            fmt.print(f"No differences between '{env_a}' and '{env_b}'")
        else:
            fmt.print(f"[cyan]Differences: {env_a} vs {env_b}[/cyan]\n")
            rows = []
            for d in diffs:
                va = mask_secrets({d["key"]: d[env_a]}).get(d["key"], d[env_a])
                vb = mask_secrets({d["key"]: d[env_b]}).get(d["key"], d[env_b])
                rows.append(
                    [
                        str(d["key"]),
                        str(va) if va is not None else "-",
                        str(vb) if vb is not None else "-",
                    ]
                )
            fmt.print_table(
                "Diff",
                [("Key", "cyan"), (env_a, "yellow"), (env_b, "green")],
                rows,
            )

        fmt.set_data({"env_a": env_a, "env_b": env_b, "diffs": diffs})
        fmt.flush()

    except EnvironmentError as e:
        handle_parse_error(fmt, e, ErrorCode.ENVIRONMENT_ERROR)
