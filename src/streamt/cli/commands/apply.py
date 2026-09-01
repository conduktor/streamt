"""streamt apply command."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Optional, cast

import click

from streamt.cli.helpers import (
    check_required_deployers,
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
from streamt.compiler.manifest import ArtifactOwnership, Manifest
from streamt.core.environment import EnvironmentConfig
from streamt.core.errors import ErrorCode
from streamt.deployer.plan_file import PlanFileError, ReviewedPlanFile, StalePlanError
from streamt.output import StructuredError

_SELECTABLE_ARTIFACT_KINDS = (
    "schemas",
    "topics",
    "flink_jobs",
    "test_jobs",
    "connectors",
    "gateway_rules",
    "gateway_vclusters",
)


def _artifact_is_selected(
    artifact: dict[str, object],
    selected_models: set[str],
    selected_sources: set[str],
) -> bool:
    """Return whether explicit artifact ownership is inside the selection."""
    ownership = ArtifactOwnership.from_dict(artifact.get("ownership"))
    if not ownership or ownership.mode != "managed":
        return False
    if ownership.owner_type == "model":
        return ownership.owner_name in selected_models
    if ownership.owner_type == "source":
        return ownership.owner_name in selected_sources
    return False


def filter_manifest_for_selection(
    manifest: Manifest,
    selected_models: set[str],
    selected_sources: set[str],
) -> None:
    """Restrict a compiled manifest to the explicitly selected ownership closure."""
    manifest.models = [m for m in manifest.models if m.get("name") in selected_models]
    manifest.sources = [s for s in manifest.sources if s.get("name") in selected_sources]
    manifest.tests = [
        test
        for test in manifest.tests
        if test.get("model") in selected_models or test.get("model") in selected_sources
    ]
    for kind in _SELECTABLE_ARTIFACT_KINDS:
        if kind in manifest.artifacts:
            manifest.artifacts[kind] = [
                artifact
                for artifact in manifest.artifacts[kind]
                if _artifact_is_selected(artifact, selected_models, selected_sources)
            ]


def destructive_operations_allowed(
    env_config: EnvironmentConfig | None,
    force: bool,
) -> bool:
    """Destructive behavior is opt-in, including in single-environment mode."""
    return force or bool(env_config and env_config.safety.allow_destructive)


@click.command()
@click.option("--project-dir", "-p", type=click.Path(exists=True), help="Path to project directory")
@click.option(
    "--env", "-e", "environment", help="Target environment (reads from STREAMT_ENV if not set)"
)
@click.option("--target", "-t", help="Deploy only this model and its dependencies")
@click.option("--select", "-s", help="Select models by tag (e.g., 'tag:payments')")
@click.option("--confirm", is_flag=True, help="Skip confirmation prompt for protected environments")
@click.option(
    "--confirm-env",
    type=str,
    default=None,
    help="Non-interactive confirm: pass env name (for agents/CI)",
)
@click.option("--force", is_flag=True, help="Override safety checks (allow destructive operations)")
@click.option("--dry-run", is_flag=True, help="Show what would change without applying")
@click.option(
    "--plan",
    "reviewed_plan_path",
    type=click.Path(dir_okay=False, path_type=Path),
    help="Apply an integrity-checked reviewed plan file",
)
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
    dry_run: bool,
    reviewed_plan_path: Optional[Path],
) -> None:
    """Deploy the project."""
    from streamt.compiler import Compiler
    from streamt.core.dag import DAGBuilder
    from streamt.core.environment import EnvironmentError
    from streamt.core.parser import EnvVarError, ParseError, ProjectParser
    from streamt.core.validator import ProjectValidator
    from streamt.deployer.planner import DeploymentPlanner

    fmt = make_formatter(ctx, "apply")
    project_path = get_project_path(project_dir)

    if reviewed_plan_path and (target or select):
        message = "--plan cannot be combined with --target or --select"
        fmt.add_error(StructuredError(code=ErrorCode.PLAN_FILE_INVALID, message=message))
        fmt.print_error(message)
        fmt.flush()
        sys.exit(1)

    try:
        parser = ProjectParser(
            project_path,
            environment=environment,
            warn_callback=lambda msg: fmt.print(msg),
        )
        project = parser.parse()

        # Protected environment confirmation
        if parser.env_config and parser.env_config.environment.protected:
            env_name = parser.env_config.environment.name
            fmt.print_warning(f"Deploying to protected environment '{env_name}'")

            if confirm_env:
                if confirm_env != env_name:
                    fmt.add_error(
                        StructuredError(
                            code=ErrorCode.ENVIRONMENT_ERROR,
                            message=f"--confirm-env '{confirm_env}' does not match '{env_name}'",
                        )
                    )
                    fmt.print_error(f"--confirm-env '{confirm_env}' does not match '{env_name}'")
                    fmt.flush()
                    sys.exit(1)
            elif not confirm:
                if sys.stdin.isatty():
                    fmt.print_warning(f"'{env_name}' is a protected environment.")
                    user_input = click.prompt(
                        f"Type '{env_name}' to confirm", default="", show_default=False
                    )
                    if user_input != env_name:
                        fmt.print_error("Aborted")
                        fmt.set_status("error")
                        fmt.flush()
                        sys.exit(1)
                else:
                    fmt.add_error(
                        StructuredError(
                            code=ErrorCode.ENVIRONMENT_ERROR,
                            message=f"Protected env '{env_name}'. Use --confirm or --confirm-env {env_name}.",
                        )
                    )
                    fmt.print_error(
                        f"'{env_name}' is protected. Use --confirm or --confirm-env in CI."
                    )
                    fmt.flush()
                    sys.exit(1)

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
        effective_environment = (
            parser.env_config.environment.name if parser.env_config else "default"
        )
        reviewed_plan = None
        if reviewed_plan_path:
            reviewed_plan = ReviewedPlanFile.load(reviewed_plan_path)
            reviewed_plan.verify_context(
                manifest,
                project=project.project.name,
                environment=effective_environment,
                runtime=project.runtime,
            )

        # --target / --select filtering
        if target or select:
            dag = DAGBuilder(project).build()
            all_model_names = {m.name for m in project.models}
            selected_models: set[str] = set()

            if target:
                if target not in all_model_names:
                    fmt.add_error(
                        StructuredError(
                            code=ErrorCode.PARSE_ERROR,
                            message=f"Target model '{target}' not found. Available: {', '.join(sorted(all_model_names))}",
                        )
                    )
                    fmt.print_error(f"Target model '{target}' not found")
                    fmt.flush()
                    sys.exit(1)
                selected_models = dag.get_upstream(target) & all_model_names | {target}

            if select:
                # Parse tag:X syntax
                if select.startswith("tag:"):
                    tag_value = select[4:]
                    tagged = {m.name for m in project.models if tag_value in m.tags}
                    if not tagged:
                        fmt.add_error(
                            StructuredError(
                                code=ErrorCode.PARSE_ERROR,
                                message=f"No models found with tag '{tag_value}'",
                            )
                        )
                        fmt.print_error(f"No models found with tag '{tag_value}'")
                        fmt.flush()
                        sys.exit(1)
                    # Include upstream deps of tagged models
                    for name in list(tagged):
                        tagged |= dag.get_upstream(name) & all_model_names
                    if selected_models:
                        selected_models &= tagged  # intersection if --target also set
                    else:
                        selected_models = tagged
                else:
                    fmt.add_error(
                        StructuredError(
                            code=ErrorCode.PARSE_ERROR,
                            message=f"Unknown select syntax '{select}'. Expected: tag:<value>",
                        )
                    )
                    fmt.print_error(f"Unknown select syntax '{select}'. Expected: tag:<value>")
                    fmt.flush()
                    sys.exit(1)

            if not selected_models:
                fmt.add_error(
                    StructuredError(
                        code=ErrorCode.PARSE_ERROR,
                        message="The combined --target/--select expression matched no models",
                    )
                )
                fmt.print_error("The combined --target/--select expression matched no models")
                fmt.flush()
                sys.exit(1)

            # Get source names used by selected models
            source_names = {s.name for s in project.sources}
            selected_sources: set[str] = set()
            for model in project.models:
                if model.name in selected_models and model.sql:
                    srcs, _ = parser.extract_refs_from_sql(model.sql)
                    selected_sources.update(s for s in srcs if s in source_names)

            filter_manifest_for_selection(manifest, selected_models, selected_sources)

            fmt.print(
                f"[cyan]Deploying {len(selected_models)} model(s): "
                f"{', '.join(sorted(selected_models))}[/cyan]"
            )

        # Create deployers
        sr = make_sr_deployer(project, fmt)
        kafka = make_kafka_deployer(project, fmt)
        flink = make_flink_deployer(project, fmt, state_dir=project_path / ".streamt")
        connect = make_connect_deployer(project, fmt)
        gateway = make_gateway_deployer(project, fmt)

        # Pre-flight: abort if required deployers are unavailable
        if not check_required_deployers(project, kafka, sr, flink, connect, gateway, fmt):
            close_deployers(sr, kafka, flink, connect, gateway)
            fmt.flush()
            sys.exit(1)

        try:
            planner = DeploymentPlanner(
                manifest,
                schema_registry_deployer=sr,
                kafka_deployer=kafka,
                flink_deployer=flink,
                connect_deployer=connect,
                gateway_deployer=gateway,
                project=project,
                project_name=project.project.name,
                environment=effective_environment,
            )
            deployment_plan = planner.plan()
            if reviewed_plan:
                reviewed_plan.verify_current_plan(deployment_plan)

            if deployment_plan.is_apply_blocked is True:
                all_requirements = [
                    requirement.to_dict()
                    for requirement in deployment_plan.ownership_requirements
                ]
                blocking_requirements = [
                    requirement.to_dict()
                    for requirement in deployment_plan.blocking_ownership_requirements
                ]
                message = (
                    f"Apply blocked: {len(blocking_requirements)} resource(s) require an explicit "
                    "ownership decision or adoption"
                )
                fmt.set_data(
                    {
                        "summary": deployment_plan.summary(),
                        "ownership_requirements": all_requirements,
                        "blocking_ownership_requirements": blocking_requirements,
                        "plan_checksum": reviewed_plan.checksum if reviewed_plan else None,
                    }
                )
                fmt.add_error(
                    StructuredError(code=ErrorCode.OWNERSHIP_REQUIRED, message=message)
                )
                fmt.print(deployment_plan.details())
                fmt.print_error(message)
                fmt.flush()
                sys.exit(1)

            # Destructive safety — only block if plan actually has deletes
            if deployment_plan.deletes > 0:
                env_name = (
                    parser.env_config.environment.name if parser.env_config else "default"
                )
                if not destructive_operations_allowed(parser.env_config, force):
                    fmt.add_error(
                        StructuredError(
                            code=ErrorCode.ENVIRONMENT_ERROR,
                            message=(
                                f"Destructive ops blocked for '{env_name}'. Plan has "
                                f"{deployment_plan.deletes} delete(s). Use --force."
                            ),
                        )
                    )
                    fmt.print_error(
                        f"Destructive ops blocked for '{env_name}'. Use --force to override."
                    )
                    fmt.flush()
                    sys.exit(1)
                if force:
                    fmt.print_warning(f"--force used, allowing destructive ops on '{env_name}'")

            if dry_run:
                fmt.print("[yellow]Dry run — no changes applied[/yellow]")
                fmt.print(deployment_plan.details())
                fmt.set_data(
                    {
                        "dry_run": True,
                        "summary": deployment_plan.summary(),
                        "creates": deployment_plan.creates,
                        "updates": deployment_plan.updates,
                        "deletes": deployment_plan.deletes,
                        "has_changes": deployment_plan.has_changes,
                        "plan_checksum": reviewed_plan.checksum if reviewed_plan else None,
                    }
                )
                fmt.flush()
                close_deployers(sr, kafka, flink, connect, gateway)
                return

            results = planner.apply(deployment_plan)
            if reviewed_plan and reviewed_plan_path:
                results["plan_checksum"] = reviewed_plan.checksum
                results["plan_file"] = str(reviewed_plan_path.resolve())
            fmt.set_data(results)

            created = cast(list[str], results["created"])
            updated = cast(list[str], results["updated"])
            unchanged = cast(list[str], results["unchanged"])
            errors = cast(list[str], results["errors"])
            rollback_candidates = cast(list[str], results.get("rollback_candidates", []))

            if created:
                fmt.print("\n[green]Created:[/green]")
                for item in created:
                    fmt.print(f"  + {item}")
            if updated:
                fmt.print("\n[yellow]Updated:[/yellow]")
                for item in updated:
                    fmt.print(f"  ~ {item}")
            if unchanged:
                fmt.print("\n[dim]Unchanged:[/dim]")
                for item in unchanged:
                    fmt.print(f"  = {item}")
            if errors and rollback_candidates:
                fmt.print("\n[yellow]Rolling back newly created resources...[/yellow]")
                rolled_back, rb_errors = planner.rollback(rollback_candidates)
                if rolled_back:
                    results["rolled_back"] = rolled_back
                    fmt.print(f"  Rolled back {len(rolled_back)} resource(s)")
                    for item in rolled_back:
                        fmt.print(f"  ↩ {item}")
                if rb_errors:
                    results["rollback_errors"] = rb_errors
                    fmt.print("\n[red]Rollback failures (manual cleanup needed):[/red]")
                    for item in rb_errors:
                        fmt.print_error(item)
            if errors:
                fmt.set_status("error")
                fmt.print("\n[red]Errors:[/red]")
                for item in errors:
                    fmt.add_error(StructuredError(code=ErrorCode.DEPLOY_ERROR, message=item))
                    fmt.print_error(item)
                fmt.flush()
                sys.exit(1)

            fmt.print("\n[green]Apply complete[/green]")
            fmt.flush()
        finally:
            close_deployers(sr, kafka, flink, connect, gateway)

    except (EnvVarError, ParseError, EnvironmentError) as e:
        handle_parse_error(fmt, e, ErrorCode.PARSE_ERROR)
    except StalePlanError as e:
        fmt.add_error(StructuredError(code=ErrorCode.PLAN_STALE, message=str(e)))
        fmt.print_error(str(e))
        fmt.flush()
        sys.exit(1)
    except PlanFileError as e:
        fmt.add_error(StructuredError(code=ErrorCode.PLAN_FILE_INVALID, message=str(e)))
        fmt.print_error(str(e))
        fmt.flush()
        sys.exit(1)
    except KeyboardInterrupt:
        fmt.print_error("Interrupted.")
        fmt.flush()
        sys.exit(130)
    except Exception as e:
        fmt.add_error(
            StructuredError(code=ErrorCode.CONNECTION_REFUSED, message=f"Cannot connect: {e}")
        )
        fmt.print_error(f"Cannot connect: {e}")
        fmt.flush()
        sys.exit(1)
