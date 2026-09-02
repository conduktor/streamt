"""streamt apply command."""

from __future__ import annotations

import shlex
import sys
import uuid
from contextlib import ExitStack
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
    redact_sensitive_text,
)
from streamt.compiler.manifest import ArtifactOwnership, Manifest
from streamt.core.deployment_state import (
    RemoteStateRequiredError,
    enforce_remote_state_policy,
)
from streamt.core.environment import EnvironmentConfig
from streamt.core.errors import ErrorCode
from streamt.deployer.plan_file import PlanFileError, ReviewedPlanFile, StalePlanError
from streamt.deployer.state import (
    LOCAL_STATE_CI_WARNING,
    StateError,
    StateFormatError,
    local_state_path,
    updated_local_state,
)
from streamt.deployer.state_backend import (
    OperationAction,
    OperationIntent,
    OperationProgress,
    OperationSnapshot,
    RecoveryRecord,
    StateBackendConflictError,
    StateBackendLockLostError,
    StateBackendLockTimeoutError,
    StateBackendRecoveryRequiredError,
    StateBackendReleaseAfterCommitError,
    StateBackendUnavailableError,
    StateBackendUnknownCommitError,
    make_deployment_state_service,
    operation_timestamp,
    state_checksum,
)
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
    if not ownership or ownership.mode not in ("managed", "adopted"):
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


def _reviewed_plan_commands(
    environment: str,
    project_path: Path,
) -> tuple[str, str]:
    """Return executable commands for the required review/apply workflow."""
    project_arg = shlex.quote(str(project_path))
    environment_arg = shlex.quote(environment)
    plan_file = project_path / ".streamt" / "reviewed-plan.json"
    plan_arg = shlex.quote(str(plan_file))
    return (
        f"streamt plan --project-dir {project_arg} --env {environment_arg} --out {plan_arg}",
        f"streamt apply --project-dir {project_arg} --env {environment_arg} --plan {plan_arg}",
    )


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

    operation_stack = ExitStack()
    verified_commit_data: dict[str, object] | None = None
    try:
        parser = ProjectParser(
            project_path,
            environment=environment,
            warn_callback=lambda msg: fmt.print(msg),
        )
        project = parser.parse()

        env_config = parser.env_config
        if env_config and env_config.requires_reviewed_plan and not reviewed_plan_path:
            env_name = env_config.environment.name
            plan_command, apply_command = _reviewed_plan_commands(env_name, project_path)
            policy = (
                "environment.protected"
                if env_config.environment.protected
                else "safety.require_reviewed_plan"
            )
            message = (
                f"Direct apply is disabled for environment '{env_name}'; "
                "a reviewed plan file is required"
            )
            fmt.set_data(
                {
                    "environment": env_name,
                    "policy": policy,
                    "required_workflow": "reviewed_plan",
                    "next_steps": [plan_command, apply_command],
                }
            )
            fmt.add_error(
                StructuredError(
                    code=ErrorCode.REVIEWED_PLAN_REQUIRED,
                    message=message,
                    suggestion=(
                        f"Run '{plan_command}', review the saved file, then run "
                        f"'{apply_command}'."
                    ),
                    docs_url="https://streamt.dev/docs/reference/cli#apply",
                )
            )
            fmt.print_error(f"{message}. Run: {plan_command}")
            fmt.flush()
            sys.exit(1)

        reviewed_plan = (
            ReviewedPlanFile.load(reviewed_plan_path) if reviewed_plan_path else None
        )
        if reviewed_plan is not None and reviewed_plan.offline:
            raise PlanFileError(
                "Offline reviewed plans are preview-only and cannot authorize apply; "
                "create a fresh online plan with live infrastructure evidence"
            )

        enforce_remote_state_policy(
            project.deployment_state,
            required=bool(env_config and env_config.requires_remote_state),
        )

        # Explicit environment confirmation
        if env_config and env_config.requires_apply_confirmation:
            env_name = env_config.environment.name
            if env_config.environment.protected:
                fmt.print_warning(f"Deploying to protected environment '{env_name}'")
            else:
                fmt.print_warning(
                    f"Environment '{env_name}' requires explicit apply confirmation"
                )

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
                    if env_config.environment.protected:
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
                    reason = (
                        "because it is protected"
                        if env_config.environment.protected
                        else "because safety.confirm_apply is enabled"
                    )
                    fmt.add_error(
                        StructuredError(
                            code=ErrorCode.ENVIRONMENT_ERROR,
                            message=(
                                f"Environment '{env_name}' requires confirmation {reason}. "
                                "Use --confirm or "
                                f"--confirm-env {env_name}."
                            ),
                        )
                    )
                    fmt.print_error(
                        f"'{env_name}' requires confirmation. Use --confirm or "
                        "--confirm-env in CI."
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
        parsed_environment = (
            parser.env_config.environment.name if parser.env_config else None
        )
        effective_environment = (
            parsed_environment
            if isinstance(parsed_environment, str) and parsed_environment
            else "default"
        )
        state_path = local_state_path(
            project_path,
            environment=effective_environment,
        )
        state_service = make_deployment_state_service(
            project_path,
            project=project.project.name,
            environment=effective_environment,
            config=project.deployment_state,
        )
        state_operation = operation_stack.enter_context(
            state_service.operation()
        )
        # This is the planning state/control pair.  The operation lock remains
        # held through live re-planning, runtime mutation, and state commit.
        planning_snapshot = state_operation.observe()
        state_operation.ensure_ready(planning_snapshot)
        prior_observation = planning_snapshot.state
        prior_state = prior_observation.state
        fmt.print_warning(
            f"{LOCAL_STATE_CI_WARNING} State file: {state_path}",
            code=ErrorCode.LOCAL_STATE_ONLY,
        )
        if reviewed_plan is not None:
            reviewed_plan.verify_context(
                manifest,
                project=project.project.name,
                environment=effective_environment,
                runtime=project.runtime,
                state_observation=prior_observation,
            )

        # --target / --select filtering
        if target or select:
            dag = compiler.dag
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
                prior_state=prior_state,
                project_name=project.project.name,
                environment=effective_environment,
            )
            deployment_plan = planner.plan()
            if reviewed_plan is not None:
                reviewed_snapshot = state_operation.observe()
                state_operation.ensure_ready(reviewed_snapshot)
                reviewed_plan.verify_current_plan(
                    deployment_plan,
                    state_observation=reviewed_snapshot.state,
                )

            if deployment_plan.is_apply_blocked is True:
                all_requirements = [
                    requirement.to_dict()
                    for requirement in deployment_plan.ownership_requirements
                ]
                blocking_requirements = [
                    requirement.to_dict()
                    for requirement in deployment_plan.blocking_ownership_requirements
                ]
                safety_blockers = [
                    blocker.to_dict()
                    for blocker in deployment_plan.ordered_safety_blockers
                ]
                if safety_blockers:
                    error_code = ErrorCode.SAFETY_BLOCKED
                    message = f"Apply blocked: {len(safety_blockers)} unsafe change(s)"
                    if blocking_requirements:
                        message += (
                            f" and {len(blocking_requirements)} resource(s) with unresolved "
                            "ownership"
                        )
                    message += " require resolution before apply"
                else:
                    error_code = ErrorCode.OWNERSHIP_REQUIRED
                    message = (
                        f"Apply blocked: {len(blocking_requirements)} resource(s) require an "
                        "explicit ownership decision or adoption"
                    )
                fmt.set_data(
                    {
                        "summary": deployment_plan.summary(),
                        "ownership_requirements": all_requirements,
                        "blocking_ownership_requirements": blocking_requirements,
                        "safety_blockers": safety_blockers,
                        "plan_checksum": reviewed_plan.checksum if reviewed_plan else None,
                    }
                )
                fmt.add_error(StructuredError(code=error_code, message=message))
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
                operation_stack.close()
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

            next_state = updated_local_state(prior_state, deployment_plan)
            ordered_actions = planner.planned_actions(deployment_plan)

            # Bind the durable intent to a fresh state/control pair immediately
            # before it is written.  Direct applies get the same final drift
            # check as reviewed plans rather than relying on a later state CAS.
            intent_snapshot = state_operation.observe()
            state_operation.ensure_ready(intent_snapshot)
            if reviewed_plan is not None:
                reviewed_plan.verify_current_plan(
                    deployment_plan,
                    state_observation=intent_snapshot.state,
                )
            if (
                intent_snapshot.state.store != prior_observation.store
                or intent_snapshot.state.revision != prior_observation.revision
                or intent_snapshot.state.state != prior_state
                or intent_snapshot.control.revision
                != planning_snapshot.control.revision
                or intent_snapshot.control.control
                != planning_snapshot.control.control
            ):
                raise StateBackendConflictError(
                    "deployment state or operation control changed during live "
                    "planning; reload state and produce a fresh plan"
                )

            operation_id = str(uuid.uuid4())
            intent = OperationIntent(
                operation_id=operation_id,
                kind="apply",
                started_at=operation_timestamp(),
                actor="local-cli",
                prior_state_serial=intent_snapshot.state.state.serial,
                prior_state_checksum=state_checksum(intent_snapshot.state.state),
                reviewed_plan_checksum=(
                    reviewed_plan.checksum if reviewed_plan is not None else None
                ),
                actions=tuple(
                    OperationAction(
                        index=index,
                        resource_id=planned_action.resource_id,
                        action=planned_action.action,
                    )
                    for index, planned_action in enumerate(ordered_actions)
                ),
            )
            active_snapshot: list[OperationSnapshot] = [
                state_operation.begin_operation(intent_snapshot, intent)
            ]
            mutation_started = False
            state_commit_attempted = False
            operation_finalized = False

            def before_action(label: str, index: int) -> None:
                nonlocal mutation_started
                state_operation.check_lock()
                action = intent.actions[index]
                planned_action = ordered_actions[index]
                if (
                    planned_action.runtime_label != label
                    or action.resource_id != planned_action.resource_id
                    or action.action != planned_action.action
                ):
                    raise StateFormatError(
                        "runtime action order does not match the durable operation intent"
                    )
                active_snapshot[0] = state_operation.record_progress(
                    active_snapshot[0],
                    OperationProgress(
                        operation_id=operation_id,
                        action_index=index,
                        resource_id=action.resource_id,
                        action=action.action,
                        status="started",
                        succeeded=None,
                        recorded_at=operation_timestamp(),
                    ),
                )
                # No runtime call can begin until the started boundary is durable.
                mutation_started = True

            def after_action(label: str, index: int, succeeded: bool) -> None:
                state_operation.check_lock()
                action = intent.actions[index]
                planned_action = ordered_actions[index]
                if (
                    planned_action.runtime_label != label
                    or action.resource_id != planned_action.resource_id
                    or action.action != planned_action.action
                ):
                    raise StateFormatError(
                        "runtime action order does not match the durable operation intent"
                    )
                active_snapshot[0] = state_operation.record_progress(
                    active_snapshot[0],
                    OperationProgress(
                        operation_id=operation_id,
                        action_index=index,
                        resource_id=action.resource_id,
                        action=action.action,
                        status="completed",
                        succeeded=succeeded,
                        recorded_at=operation_timestamp(),
                    ),
                )

            def mark_recovery(failure_code: str) -> None:
                nonlocal operation_finalized
                completed = [
                    progress.action_index
                    for progress in active_snapshot[0].control.control.progress
                    if progress.status == "completed"
                    and progress.succeeded is True
                ]
                active_snapshot[0] = state_operation.mark_recovery_required(
                    active_snapshot[0],
                    RecoveryRecord(
                        operation_id=operation_id,
                        failure_code=failure_code,
                        failed_at=operation_timestamp(),
                        last_completed_action_index=(
                            max(completed) if completed else None
                        ),
                    ),
                )
                operation_finalized = True

            def clear_before_mutation() -> None:
                nonlocal operation_finalized
                active_snapshot[0] = state_operation.clear_before_mutation(
                    active_snapshot[0]
                )
                operation_finalized = True

            try:
                results = planner.apply(
                    deployment_plan,
                    before_action=before_action,
                    after_action=after_action,
                    stop_on_error=True,
                )
                if reviewed_plan and reviewed_plan_path:
                    results["plan_checksum"] = reviewed_plan.checksum
                    results["plan_file"] = str(reviewed_plan_path.resolve())
                created = cast(list[str], results["created"])
                updated = cast(list[str], results["updated"])
                unchanged = cast(list[str], results["unchanged"])
                errors = cast(list[str], results["errors"])
                rollback_candidates = cast(
                    list[str],
                    results.get("rollback_candidates", []),
                )

                rollback_failed = False
                if errors and rollback_candidates:
                    fmt.print("\n[yellow]Rolling back newly created resources...[/yellow]")
                    rolled_back, rb_errors = planner.rollback(
                        rollback_candidates,
                        before_action=lambda _label, _index: (
                            state_operation.check_lock()
                        ),
                        after_action=lambda _label, _index, _succeeded: (
                            state_operation.check_lock()
                        ),
                        stop_on_error=True,
                    )
                    if rolled_back:
                        results["rolled_back"] = rolled_back
                        fmt.print(f"  Rolled back {len(rolled_back)} resource(s)")
                        for item in rolled_back:
                            fmt.print(f"  ↩ {item}")
                    if rb_errors:
                        rollback_failed = True
                        results["rollback_errors"] = rb_errors
                        fmt.print("\n[red]Rollback failures (manual cleanup needed):[/red]")
                        for item in rb_errors:
                            fmt.print_error(item)
                if errors:
                    if mutation_started:
                        mark_recovery(
                            "rollback_incomplete"
                            if rollback_failed
                            else "runtime_action_failed"
                        )
                    else:
                        clear_before_mutation()
                    fmt.set_data(results)
                    fmt.set_status("error")
                    fmt.print("\n[red]Errors:[/red]")
                    for item in errors:
                        fmt.add_error(
                            StructuredError(code=ErrorCode.DEPLOY_ERROR, message=item)
                        )
                        fmt.print_error(item)
                    fmt.flush()
                    sys.exit(1)

                state_operation.check_lock()
                # Any finalizer attempt can have an ambiguous outcome, even
                # when it only clears a zero-action/no-state-change intent.
                state_commit_attempted = True
                try:
                    active_snapshot[0] = state_operation.commit_operation(
                        active_snapshot[0],
                        next_state,
                    )
                except OSError as error:
                    raise StateBackendUnknownCommitError(
                        "deployment succeeded but ownership state commit "
                        "could not be confirmed"
                    ) from error
                operation_finalized = True
                if next_state is not None:
                    results["state_serial"] = next_state.serial
                    results["state_file"] = str(state_path)
                else:
                    results["state_serial"] = prior_state.serial
                results["committed"] = True
                verified_commit_data = results
                # Preserve the verified outcome if releasing provider authority
                # fails. Output remains buffered until after release succeeds.
                fmt.set_data(results)

                # Do not emit a success result while operation ownership is held.
                operation_stack.close()
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
                fmt.print("\n[green]Apply complete[/green]")
                fmt.flush()
            except BaseException:
                if not operation_finalized:
                    if mutation_started or state_commit_attempted:
                        try:
                            mark_recovery(
                                "state_commit_uncertain"
                                if state_commit_attempted
                                else "operation_interrupted"
                            )
                        except BaseException:
                            # The existing in_progress sidecar remains blocking.
                            pass
                    else:
                        try:
                            clear_before_mutation()
                        except BaseException:
                            # A failed clear preserves the conservative marker.
                            pass
                raise
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
    except RemoteStateRequiredError as e:
        safe_message = redact_sensitive_text(e)
        fmt.add_error(
            StructuredError(
                code=ErrorCode.REMOTE_STATE_REQUIRED,
                message=safe_message,
            )
        )
        fmt.print_error(safe_message)
        fmt.flush()
        sys.exit(1)
    except StateBackendReleaseAfterCommitError as e:
        safe_message = redact_sensitive_text(e)
        release_data = dict(verified_commit_data or {})
        release_data["committed"] = e.committed
        fmt.set_data(release_data)
        fmt.add_error(
            StructuredError(
                code=ErrorCode.STATE_RELEASE_FAILED_AFTER_COMMIT,
                message=safe_message,
            )
        )
        fmt.print_error(safe_message)
        fmt.flush()
        sys.exit(1)
    except StateBackendLockTimeoutError as e:
        safe_message = redact_sensitive_text(e)
        fmt.add_error(
            StructuredError(code=ErrorCode.STATE_LOCK_TIMEOUT, message=safe_message)
        )
        fmt.print_error(safe_message)
        fmt.flush()
        sys.exit(1)
    except StateBackendLockLostError as e:
        safe_message = redact_sensitive_text(e)
        fmt.add_error(
            StructuredError(code=ErrorCode.STATE_LOCK_LOST, message=safe_message)
        )
        fmt.print_error(safe_message)
        fmt.flush()
        sys.exit(1)
    except StateBackendConflictError as e:
        safe_message = redact_sensitive_text(e)
        fmt.add_error(
            StructuredError(code=ErrorCode.STATE_CONFLICT, message=safe_message)
        )
        fmt.print_error(safe_message)
        fmt.flush()
        sys.exit(1)
    except StateBackendUnknownCommitError as e:
        safe_message = redact_sensitive_text(e)
        fmt.add_error(
            StructuredError(
                code=ErrorCode.STATE_UNKNOWN_OUTCOME,
                message=safe_message,
            )
        )
        fmt.print_error(safe_message)
        fmt.flush()
        sys.exit(1)
    except StateBackendUnavailableError as e:
        safe_message = redact_sensitive_text(e)
        fmt.add_error(
            StructuredError(
                code=ErrorCode.STATE_BACKEND_UNAVAILABLE,
                message=safe_message,
            )
        )
        fmt.print_error(safe_message)
        fmt.flush()
        sys.exit(1)
    except StateBackendRecoveryRequiredError as e:
        safe_message = redact_sensitive_text(e)
        fmt.add_error(
            StructuredError(
                code=ErrorCode.STATE_RECOVERY_REQUIRED,
                message=safe_message,
            )
        )
        fmt.print_error(safe_message)
        fmt.flush()
        sys.exit(1)
    except StateError as e:
        safe_message = redact_sensitive_text(e)
        fmt.add_error(
            StructuredError(code=ErrorCode.STATE_INVALID, message=safe_message)
        )
        fmt.print_error(safe_message)
        fmt.flush()
        sys.exit(1)
    except KeyboardInterrupt:
        fmt.print_error("Interrupted.")
        fmt.flush()
        sys.exit(130)
    except Exception as e:
        safe_message = redact_sensitive_text(e)
        fmt.add_error(
            StructuredError(
                code=ErrorCode.CONNECTION_REFUSED,
                message=f"Cannot connect: {safe_message}",
            )
        )
        fmt.print_error(f"Cannot connect: {safe_message}")
        fmt.flush()
        sys.exit(1)
    finally:
        operation_stack.close()
