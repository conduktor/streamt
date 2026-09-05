"""streamt apply command."""

from __future__ import annotations

import os
import shlex
import sys
import uuid
from contextlib import ExitStack
from dataclasses import dataclass
from pathlib import Path
from typing import Literal, Optional, cast

import click

from streamt.cli.connector_removal_guard import (
    CONNECTOR_REMOVAL_DRIFT_MESSAGE,
    connector_removal_delete_count,
    emit_connector_removal_destructive_warning,
    enforce_connector_removal_apply_authorization,
)
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
    make_kafka_streams_deployer,
    make_sr_deployer,
    redact_sensitive_text,
    required_deployer_services,
    state_operation_error_details,
)
from streamt.compiler.connector_artifact import (
    ConnectorRemovalClusterReferenceError,
    ConnectorRemovalPreflightError,
    ConnectorRemovalRuntimeRequiredError,
    ConnectorRemovalStateAuthorityError,
)
from streamt.compiler.manifest import ArtifactOwnership, Manifest
from streamt.core.deployment_state import (
    RemoteStateRequiredError,
    enforce_remote_state_policy,
)
from streamt.core.environment import EnvironmentConfig
from streamt.core.errors import ErrorCode
from streamt.core.models import StreamtProject
from streamt.deployer.operation_actions import operation_actions_from_planned
from streamt.deployer.plan_file import PlanFileError, ReviewedPlanFile, StalePlanError
from streamt.deployer.state import (
    LOCAL_STATE_CI_WARNING,
    ManagedConnectorResourceDeletion,
    ManagedGatewayResourceDeletion,
    StateError,
    StateFormatError,
    StateIdentityError,
    local_state_path,
    updated_local_state,
)
from streamt.deployer.state_backend import (
    OperationIntent,
    OperationProgress,
    OperationSnapshot,
    RecoveryRecord,
    StateBackendConflictError,
    StateBackendInvalidStateError,
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
from streamt.integrations.openlineage import (
    JobIdentity,
    OpenLineageConstructionError,
    OpenLineageNamespaceError,
    OpenLineageTransport,
    OpenLineageTransportConfigurationError,
    OpenLineageValidationError,
    RunIdentity,
    build_run_event,
    command_job_name,
    create_openlineage_transport,
    load_openlineage_transport_config,
    resolve_openlineage_namespaces,
    standard_facet,
    validate_event,
    validate_event_sequence,
)
from streamt.output import OutputFormatter, StructuredError, StructuredWarning

_SELECTABLE_ARTIFACT_KINDS = (
    "schemas",
    "topics",
    "flink_jobs",
    "kafka_streams_jobs",
    "test_jobs",
    "connectors",
    "gateway_rules",
    "gateway_vclusters",
)

_ApplyTerminalEventType = Literal["COMPLETE", "FAIL", "ABORT"]
_APPLY_FAILURE_MESSAGE = "streamt apply command did not complete successfully"


class _OpenLineageApplyPreflightError(ValueError):
    """A fixed, secret-neutral OpenLineage apply preflight failure."""

    def __init__(self, message: str, *, location: str) -> None:
        super().__init__(message)
        self.location = location


@dataclass
class _ApplyOpenLineageLifecycle:
    """Best-effort delivery state for one already validated durable apply run."""

    transport: OpenLineageTransport
    formatter: OutputFormatter
    start_event: dict[str, object]
    run: RunIdentity
    job: JobIdentity
    job_facets: dict[str, dict[str, object]]
    started: bool = False
    terminal_attempted: bool = False
    closed: bool = False

    def start(self) -> None:
        """Attempt START exactly once without changing deployment truth."""
        self.started = True
        try:
            self.transport.emit(self.start_event)
        except Exception:
            _emit_openlineage_delivery_warning(
                self.formatter,
                "OpenLineage START event delivery failed",
                location="openlineage.start",
            )

    def terminal(self, event_type: _ApplyTerminalEventType) -> None:
        """Attempt one terminal event only after START was attempted."""
        if not self.started or self.terminal_attempted:
            return
        self.terminal_attempted = True
        try:
            run_facets = (
                {
                    "errorMessage": standard_facet(
                        "run",
                        "errorMessage",
                        {
                            "message": _APPLY_FAILURE_MESSAGE,
                            "programmingLanguage": "PYTHON",
                        },
                    )
                }
                if event_type == "FAIL"
                else None
            )
            event = build_run_event(
                event_time=operation_timestamp(),
                event_type=event_type,
                run=self.run,
                job=self.job,
                run_facets=run_facets,
                job_facets=self.job_facets,
            )
            validate_event_sequence((self.start_event, event))
            self.transport.emit(event)
        except Exception:
            _emit_openlineage_delivery_warning(
                self.formatter,
                "OpenLineage terminal event delivery failed",
                location="openlineage.terminal",
            )

    def close(self) -> None:
        """Close once without changing the command's durable outcome."""
        if self.closed:
            return
        self.closed = True
        try:
            self.transport.close()
        except Exception:
            _emit_openlineage_delivery_warning(
                self.formatter,
                "OpenLineage transport close failed",
                location="openlineage.transport",
            )


def _artifact_is_selected(
    artifact: dict[str, object],
    selected_models: set[str],
    selected_sources: set[str],
    *,
    project_name: str,
) -> bool:
    """Keep selected managed artifacts and all external identity claims."""
    ownership = ArtifactOwnership.from_dict(artifact.get("ownership"))
    if "ownership" in artifact and (
        ownership is None
        or ownership.mode not in ("managed", "adopted", "external")
        or any(
            not isinstance(value, str) or not value.strip()
            for value in (ownership.project, ownership.owner_type, ownership.owner_name)
        )
    ):
        raise StateFormatError("Selected manifest contains malformed ownership metadata")
    if ownership is None:
        return False
    if ownership.project != project_name:
        raise StateIdentityError("Selected manifest ownership belongs to another project")
    if ownership.mode == "external":
        return True
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
    """Select managed work while retaining external claims for identity checks."""
    selected_artifacts: dict[str, list[dict[str, object]]] = {}
    for kind in _SELECTABLE_ARTIFACT_KINDS:
        if kind not in manifest.artifacts:
            continue
        artifacts = manifest.artifacts[kind]
        if not isinstance(artifacts, list) or any(
            not isinstance(artifact, dict) for artifact in artifacts
        ):
            raise StateFormatError(f"Selected manifest {kind} must contain artifact objects")
        selected_artifacts[kind] = [
            artifact for artifact in artifacts
            if _artifact_is_selected(
                artifact, selected_models, selected_sources, project_name=manifest.project_name,
            )
        ]

    manifest.models = [m for m in manifest.models if m.get("name") in selected_models]
    manifest.sources = [s for s in manifest.sources if s.get("name") in selected_sources]
    manifest.tests = [
        test
        for test in manifest.tests
        if test.get("model") in selected_models or test.get("model") in selected_sources
    ]
    manifest.artifacts.update(selected_artifacts)


def destructive_operations_allowed(
    env_config: EnvironmentConfig | None,
    force: bool,
) -> bool:
    """Destructive behavior is opt-in, including in single-environment mode."""
    return force or bool(env_config and env_config.safety.allow_destructive)


def _option_or_environment(option: str | None, environment_name: str) -> str | None:
    """Apply exact option-over-environment precedence after project parsing."""
    return option if option is not None else os.environ.get(environment_name)


def _prepare_apply_openlineage(
    *,
    project_name: str,
    kafka_bootstrap: str,
    gateway_bootstrap: str | None,
    operation_id: str,
    started_at: str,
    job_namespace: str | None,
    kafka_namespace: str | None,
    gateway_namespace: str | None,
    formatter: OutputFormatter,
) -> _ApplyOpenLineageLifecycle:
    """Build, validate, and open telemetry before a durable operation begins."""
    namespaces = resolve_openlineage_namespaces(
        job_namespace=_option_or_environment(job_namespace, "OPENLINEAGE_NAMESPACE"),
        kafka_namespace=_option_or_environment(
            kafka_namespace,
            "STREAMT_OPENLINEAGE_KAFKA_NAMESPACE",
        ),
        gateway_namespace=_option_or_environment(
            gateway_namespace,
            "STREAMT_OPENLINEAGE_GATEWAY_NAMESPACE",
        ),
        kafka_bootstrap=kafka_bootstrap,
        gateway_bootstrap=gateway_bootstrap,
        require_kafka=False,
        require_gateway=False,
    )
    job = JobIdentity(namespaces.job, command_job_name(project_name, "apply"))
    run = RunIdentity(operation_id)
    job_facets = {
        "jobType": standard_facet(
            "job",
            "jobType",
            {
                "processingType": "BATCH",
                "integration": "STREAMT",
                "jobType": "COMMAND",
            },
        )
    }
    start_event = build_run_event(
        event_time=started_at,
        event_type="START",
        run=run,
        job=job,
        job_facets=job_facets,
    )
    validate_event(start_event)

    failure_facet = {
        "errorMessage": standard_facet(
            "run",
            "errorMessage",
            {
                "message": _APPLY_FAILURE_MESSAGE,
                "programmingLanguage": "PYTHON",
            },
        )
    }
    for terminal_type in ("COMPLETE", "FAIL", "ABORT"):
        candidate = build_run_event(
            event_time=started_at,
            event_type=terminal_type,
            run=run,
            job=job,
            run_facets=failure_facet if terminal_type == "FAIL" else None,
            job_facets=job_facets,
        )
        validate_event_sequence((start_event, candidate))

    config = load_openlineage_transport_config(os.environ, emission_requested=True)
    transport = create_openlineage_transport(config)
    return _ApplyOpenLineageLifecycle(
        transport=transport,
        formatter=formatter,
        start_event=start_event,
        run=run,
        job=job,
        job_facets=job_facets,
    )


def _emit_openlineage_delivery_warning(
    formatter: OutputFormatter,
    message: str,
    *,
    location: str,
) -> None:
    """Record one fixed warning without exposing transport details."""
    safe_message = redact_sensitive_text(message)[:512]
    formatter.add_warning(
        StructuredWarning(
            code=ErrorCode.OPENLINEAGE_EMIT_FAILED,
            message=safe_message,
            location=location,
        )
    )
    if formatter.format == "text" and not formatter.quiet:
        formatter.stderr.print(f"[yellow]WARNING[/yellow]: {safe_message}")


def _fail_openlineage_apply_preflight(
    formatter: OutputFormatter,
    error: Exception,
    *,
    location: str,
) -> None:
    """Fail safely before an apply operation can persist its intent."""
    safe_message = redact_sensitive_text(error)[:1024].strip()
    if not safe_message:
        safe_message = "Could not prepare OpenLineage apply emission"
    formatter.add_error(
        StructuredError(
            code=ErrorCode.OPENLINEAGE_INVALID,
            message=safe_message,
            location=location,
        )
    )
    formatter.print_error(safe_message)
    formatter.flush()
    sys.exit(1)


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


def _enforce_gateway_removal_apply_authorization(
    *,
    manifest: Manifest,
    target: str | None,
    select: str | None,
    reviewed_plan_path: Path | None,
    environment: str,
    project_path: Path,
    fmt: OutputFormatter,
) -> None:
    """Require one complete online reviewed workflow for explicit removals."""
    removals = manifest.artifacts.get("gateway_rule_removals", [])
    has_removal_declaration = type(removals) is not list or bool(removals)
    if not has_removal_declaration or (reviewed_plan_path is not None and not (target or select)):
        return

    plan_command, apply_command = _reviewed_plan_commands(environment, project_path)
    if target or select:
        message = (
            "Gateway rule removals cannot be combined with --target or --select; "
            "a complete online reviewed plan is required"
        )
    else:
        message = (
            "Gateway rule removals cannot be applied directly; an online reviewed plan is required"
        )
    fmt.set_data(
        {
            "environment": environment,
            "policy": "gateway_rule_removal",
            "gateway_rule_removals": (len(removals) if isinstance(removals, list) else None),
            "required_workflow": "reviewed_plan",
            "next_steps": [plan_command, apply_command],
        }
    )
    fmt.add_error(
        StructuredError(
            code=ErrorCode.REVIEWED_PLAN_REQUIRED,
            message=message,
            suggestion=(
                f"Run '{plan_command}', review the complete saved plan, then run '{apply_command}'."
            ),
            docs_url="https://streamt.dev/docs/reference/cli#apply",
        )
    )
    fmt.print_error(f"{message}. Run: {plan_command}")
    fmt.flush()
    sys.exit(1)


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
    "--emit-openlineage",
    is_flag=True,
    help="Emit finite OpenLineage events for this apply run",
)
@click.option("--openlineage-job-namespace", help="OpenLineage job namespace")
@click.option(
    "--openlineage-kafka-namespace",
    help="Kafka dataset namespace (kafka://host:port)",
)
@click.option(
    "--openlineage-gateway-namespace",
    help="Gateway dataset namespace (kafka://host:port)",
)
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
    emit_openlineage: bool,
    openlineage_job_namespace: str | None,
    openlineage_kafka_namespace: str | None,
    openlineage_gateway_namespace: str | None,
    reviewed_plan_path: Optional[Path],
) -> None:
    """Deploy the project."""
    from streamt.compiler import Compiler
    from streamt.core.environment import EnvironmentError
    from streamt.core.parser import EnvVarError, ParseError, ProjectParser
    from streamt.core.validator import ProjectValidator
    from streamt.deployer.planner import (
        DeploymentPlanner,
        require_connector_removal_postgres_state,
        resolve_connector_planning_targets,
        resolve_gateway_planning_targets,
    )

    fmt = make_formatter(ctx, "apply")
    project_path = get_project_path(project_dir)

    operation_stack = ExitStack()
    verified_commit_data: dict[str, object] | None = None
    lineage: _ApplyOpenLineageLifecycle | None = None
    connector_removal_workflow = False
    try:
        parser = ProjectParser(
            project_path,
            environment=environment,
            warn_callback=lambda msg: fmt.print(msg),
        )
        project = parser.parse()
        project_connector_removals = (
            project.lifecycle.connector_removals if isinstance(project, StreamtProject) else []
        )
        connector_removal_workflow = bool(project_connector_removals)
        enforce_connector_removal_apply_authorization(
            project_connector_removals,
            fmt,
            reviewed_plan_path=reviewed_plan_path,
            dry_run=dry_run,
            target=target,
            select=select,
        )

        if reviewed_plan_path and (target or select):
            message = "--plan cannot be combined with --target or --select"
            fmt.add_error(StructuredError(code=ErrorCode.PLAN_FILE_INVALID, message=message))
            fmt.print_error(message)
            fmt.flush()
            sys.exit(1)

        require_connector_removal_postgres_state(
            project_connector_removals,
            project.deployment_state,
        )

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
                        f"Run '{plan_command}', review the saved file, then run '{apply_command}'."
                    ),
                    docs_url="https://streamt.dev/docs/reference/cli#apply",
                )
            )
            fmt.print_error(f"{message}. Run: {plan_command}")
            fmt.flush()
            sys.exit(1)

        reviewed_plan = ReviewedPlanFile.load(reviewed_plan_path) if reviewed_plan_path else None
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
                fmt.print_warning(f"Environment '{env_name}' requires explicit apply confirmation")

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
                        f"'{env_name}' requires confirmation. Use --confirm or --confirm-env in CI."
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

        parsed_environment = parser.env_config.environment.name if parser.env_config else None
        effective_environment = (
            parsed_environment
            if isinstance(parsed_environment, str) and parsed_environment
            else "default"
        )
        compiler = Compiler(project)
        manifest = compiler.compile()
        raw_connector_removals = manifest.artifacts.get("connector_removals", [])
        connector_removal_workflow = connector_removal_workflow or (
            type(raw_connector_removals) is not list or bool(raw_connector_removals)
        )
        require_connector_removal_postgres_state(
            raw_connector_removals,
            project.deployment_state,
        )
        _enforce_gateway_removal_apply_authorization(
            manifest=manifest,
            target=target,
            select=select,
            reviewed_plan_path=reviewed_plan_path,
            environment=effective_environment,
            project_path=project_path,
            fmt=fmt,
        )
        state_path = local_state_path(
            project_path,
            environment=effective_environment,
        )
        try:
            state_service = make_deployment_state_service(
                project_path,
                project=project.project.name,
                environment=effective_environment,
                config=project.deployment_state,
            )
        except StateBackendInvalidStateError:
            if connector_removal_workflow:
                raise ConnectorRemovalStateAuthorityError(
                    "PostgreSQL-v2 Connector removal authority is invalid"
                ) from None
            raise
        try:
            state_operation = operation_stack.enter_context(state_service.operation())
            # This is the planning state/control pair. The operation lock remains
            # held through live re-planning, runtime mutation, and state commit.
            planning_snapshot = state_operation.observe()
            state_operation.ensure_ready(planning_snapshot)
        except StateBackendInvalidStateError:
            if connector_removal_workflow:
                raise ConnectorRemovalStateAuthorityError(
                    "PostgreSQL-v2 Connector removal authority is invalid"
                ) from None
            raise
        prior_observation = planning_snapshot.state
        prior_state = prior_observation.state

        if connector_removal_workflow:
            resolve_connector_planning_targets(
                manifest,
                project,
                environment=effective_environment,
                prior_state=prior_state,
                require_authoritative_state=True,
            )

        if project.deployment_state.backend == "local":
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

        raw_gateway_removals = manifest.artifacts.get(
            "gateway_rule_removals",
            [],
        )
        if type(raw_gateway_removals) is not list or raw_gateway_removals:
            resolve_gateway_planning_targets(
                manifest,
                project,
                environment=effective_environment,
                prior_state=prior_state,
                require_authoritative_state=True,
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
        required_services = required_deployer_services(manifest)
        sr = make_sr_deployer(project, fmt) if "Schema Registry" in required_services else None
        kafka = make_kafka_deployer(project, fmt) if "Kafka" in required_services else None
        flink = (
            make_flink_deployer(project, fmt, state_dir=project_path / ".streamt")
            if "Flink" in required_services else None
        )
        connect = make_connect_deployer(project, fmt) if "Kafka Connect" in required_services else None
        gateway = (
            make_gateway_deployer(project, fmt) if "Conduktor Gateway" in required_services else None
        )
        kafka_streams = (
            make_kafka_streams_deployer(project, fmt, state_dir=project_path / ".streamt")
            if "Kafka Streams" in required_services else None
        )

        # Pre-flight: abort if required deployers are unavailable
        if not check_required_deployers(
            project, kafka, sr, flink, connect, gateway, fmt, required_services=required_services,
            kafka_streams_deployer=kafka_streams,
        ):
            close_deployers(sr, kafka, flink, connect, gateway, kafka_streams)
            fmt.flush()
            sys.exit(1)

        try:
            planner = DeploymentPlanner(
                manifest,
                schema_registry_deployer=sr,
                kafka_deployer=kafka,
                flink_deployer=flink,
                kafka_streams_deployer=kafka_streams,
                connect_deployer=connect,
                gateway_deployer=gateway,
                project=project,
                prior_state=prior_state,
                project_name=project.project.name,
                environment=effective_environment,
            )
            deployment_plan = planner.plan()
            ordered_actions = (
                ()
                if deployment_plan.is_apply_blocked
                else tuple(planner.planned_actions(deployment_plan))
            )
            operation_actions = (
                ()
                if deployment_plan.is_apply_blocked
                else operation_actions_from_planned(ordered_actions)
            )
            connector_delete_count = connector_removal_delete_count(operation_actions)
            gateway_removal_assessments = [
                assessment.to_dict() for assessment in deployment_plan.gateway_removal_assessments
            ]
            connector_removal_assessments = [
                assessment.to_dict()
                for assessment in deployment_plan.connector_removal_assessments
            ]
            if reviewed_plan is not None:
                reviewed_snapshot = state_operation.observe()
                state_operation.ensure_ready(reviewed_snapshot)
                reviewed_plan.verify_current_plan(
                    deployment_plan,
                    actions=operation_actions,
                    state_observation=reviewed_snapshot.state,
                )

            if deployment_plan.is_apply_blocked is True:
                all_requirements = [
                    requirement.to_dict() for requirement in deployment_plan.ownership_requirements
                ]
                blocking_requirements = [
                    requirement.to_dict()
                    for requirement in deployment_plan.blocking_ownership_requirements
                ]
                safety_blockers = [
                    blocker.to_dict() for blocker in deployment_plan.ordered_safety_blockers
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
                        "gateway_removal_assessments": gateway_removal_assessments,
                        "connector_removal_assessments": connector_removal_assessments,
                        "plan_checksum": reviewed_plan.checksum if reviewed_plan else None,
                    }
                )
                fmt.add_error(StructuredError(code=error_code, message=message))
                fmt.print(deployment_plan.details())
                fmt.print_error(message)
                fmt.flush()
                sys.exit(1)

            # Destructive safety — only block if plan actually has deletes
            if deployment_plan.deletes > 0 or connector_delete_count > 0:
                env_name = parser.env_config.environment.name if parser.env_config else "default"
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
                        "gateway_removal_assessments": gateway_removal_assessments,
                        "connector_removal_assessments": connector_removal_assessments,
                        "plan_checksum": reviewed_plan.checksum if reviewed_plan else None,
                    }
                )
                fmt.flush()
                close_deployers(sr, kafka, flink, connect, gateway, kafka_streams)
                return

            # Bind the durable intent to a fresh state/control pair immediately
            # before it is written.  Direct applies get the same final drift
            # check as reviewed plans rather than relying on a later state CAS.
            intent_snapshot = state_operation.observe()
            state_operation.ensure_ready(intent_snapshot)
            if reviewed_plan is not None:
                operation_actions = reviewed_plan.bind_current_actions(
                    deployment_plan,
                    actions=operation_actions,
                    state_observation=intent_snapshot.state,
                )
            if (
                intent_snapshot.state.store != prior_observation.store
                or intent_snapshot.state.revision != prior_observation.revision
                or intent_snapshot.state.state != prior_state
                or intent_snapshot.control.revision != planning_snapshot.control.revision
                or intent_snapshot.control.control != planning_snapshot.control.control
            ):
                raise StateBackendConflictError(
                    "deployment state or operation control changed during live "
                    "planning; reload state and produce a fresh plan"
                )

            managed_gateway_deletions = tuple(
                ManagedGatewayResourceDeletion(
                    resource_id=action.resource_id,
                    backend_identity=action.gateway_evidence.backend_identity,
                    alias_name=action.gateway_evidence.alias_name,
                )
                for action in operation_actions
                if action.action == "delete" and action.gateway_evidence is not None
            )
            # Validate every ordinary and Gateway ownership projection before a
            # durable intent or provider mutation. Connector deletion claims do
            # not exist until their runtime action has durably completed.
            prevalidated_next_state = updated_local_state(
                prior_state,
                deployment_plan,
                managed_gateway_deletions=managed_gateway_deletions,
            )
            emit_connector_removal_destructive_warning(fmt, operation_actions)

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
                actions=operation_actions,
            )
            if emit_openlineage:
                try:
                    lineage = _prepare_apply_openlineage(
                        project_name=project.project.name,
                        kafka_bootstrap=project.runtime.kafka.bootstrap_servers,
                        gateway_bootstrap=(
                            project.runtime.conduktor.gateway.proxy_bootstrap
                            if project.runtime.conduktor is not None
                            and project.runtime.conduktor.gateway is not None
                            else None
                        ),
                        operation_id=intent.operation_id,
                        started_at=intent.started_at,
                        job_namespace=openlineage_job_namespace,
                        kafka_namespace=openlineage_kafka_namespace,
                        gateway_namespace=openlineage_gateway_namespace,
                        formatter=fmt,
                    )
                except OpenLineageNamespaceError as error:
                    _fail_openlineage_apply_preflight(
                        fmt,
                        error,
                        location=error.location,
                    )
                except OpenLineageTransportConfigurationError as error:
                    _fail_openlineage_apply_preflight(
                        fmt,
                        error,
                        location=error.location,
                    )
                except _OpenLineageApplyPreflightError as error:
                    _fail_openlineage_apply_preflight(
                        fmt,
                        error,
                        location=error.location,
                    )
                except (OpenLineageConstructionError, OpenLineageValidationError):
                    _fail_openlineage_apply_preflight(
                        fmt,
                        _OpenLineageApplyPreflightError(
                            "Could not construct validated OpenLineage apply events",
                            location="openlineage.events",
                        ),
                        location="openlineage.events",
                    )
                except Exception:
                    _fail_openlineage_apply_preflight(
                        fmt,
                        _OpenLineageApplyPreflightError(
                            "Could not prepare OpenLineage apply emission",
                            location="openlineage",
                        ),
                        location="openlineage",
                    )
            try:
                active_snapshot: list[OperationSnapshot] = [
                    state_operation.begin_operation(intent_snapshot, intent)
                ]
            except BaseException:
                if lineage is not None:
                    lineage.close()
                raise
            mutation_started = False
            connector_removal_failed = False
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
                    or action.gateway_evidence != planned_action.gateway_evidence
                    or action.connector_evidence != planned_action.connector_evidence
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
                nonlocal connector_removal_failed
                state_operation.check_lock()
                action = intent.actions[index]
                planned_action = ordered_actions[index]
                if (
                    planned_action.runtime_label != label
                    or action.resource_id != planned_action.resource_id
                    or action.action != planned_action.action
                    or action.gateway_evidence != planned_action.gateway_evidence
                    or action.connector_evidence != planned_action.connector_evidence
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
                if (
                    succeeded is False
                    and action.action == "delete"
                    and action.connector_evidence is not None
                ):
                    connector_removal_failed = True

            def mark_recovery(failure_code: str) -> None:
                nonlocal operation_finalized
                completed = [
                    progress.action_index
                    for progress in active_snapshot[0].control.control.progress
                    if progress.status == "completed" and progress.succeeded is True
                ]
                active_snapshot[0] = state_operation.mark_recovery_required(
                    active_snapshot[0],
                    RecoveryRecord(
                        operation_id=operation_id,
                        failure_code=failure_code,
                        failed_at=operation_timestamp(),
                        last_completed_action_index=(max(completed) if completed else None),
                    ),
                )
                operation_finalized = True

            def clear_before_mutation() -> None:
                nonlocal operation_finalized
                active_snapshot[0] = state_operation.clear_before_mutation(active_snapshot[0])
                operation_finalized = True

            try:
                if lineage is not None:
                    lineage.start()
                results = planner.apply(
                    deployment_plan,
                    before_action=before_action,
                    after_action=after_action,
                    stop_on_error=True,
                )
                results["gateway_removal_assessments"] = gateway_removal_assessments
                results["connector_removal_assessments"] = connector_removal_assessments
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
                started_action_indexes = {
                    progress.action_index
                    for progress in active_snapshot[0].control.control.progress
                    if progress.status == "started"
                }
                successfully_completed_indexes = {
                    progress.action_index
                    for progress in active_snapshot[0].control.control.progress
                    if progress.status == "completed" and progress.succeeded is True
                }
                connector_removal_failed = connector_removal_failed or any(
                    action.connector_evidence is not None
                    and action.index in started_action_indexes
                    and action.index not in successfully_completed_indexes
                    for action in intent.actions
                )

                rollback_failed = False
                if errors and rollback_candidates and not connector_removal_failed:
                    fmt.print("\n[yellow]Rolling back newly created resources...[/yellow]")
                    rolled_back, rb_errors = planner.rollback(
                        rollback_candidates,
                        plan=deployment_plan,
                        before_action=lambda _label, _index: (state_operation.check_lock()),
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
                            "connector_removal_drift"
                            if connector_removal_failed
                            else (
                                "rollback_incomplete"
                                if rollback_failed
                                else "runtime_action_failed"
                            )
                        )
                    else:
                        clear_before_mutation()
                    if connector_removal_failed:
                        results["errors"] = [CONNECTOR_REMOVAL_DRIFT_MESSAGE]
                    fmt.set_data(results)
                    fmt.set_status("error")
                    fmt.print("\n[red]Errors:[/red]")
                    if connector_removal_failed:
                        fmt.add_error(
                            StructuredError(
                                code=ErrorCode.CONNECTOR_REMOVAL_DRIFT,
                                message=CONNECTOR_REMOVAL_DRIFT_MESSAGE,
                                operation_id=operation_id,
                            )
                        )
                        fmt.print_error(CONNECTOR_REMOVAL_DRIFT_MESSAGE)
                    else:
                        for item in errors:
                            fmt.add_error(
                                StructuredError(code=ErrorCode.DEPLOY_ERROR, message=item)
                            )
                            fmt.print_error(item)
                    if lineage is not None:
                        lineage.terminal("FAIL")
                        lineage.close()
                    fmt.flush()
                    sys.exit(1)

                completed_action_indexes = successfully_completed_indexes
                connector_actions = tuple(
                    action
                    for action in intent.actions
                    if action.action == "delete" and action.connector_evidence is not None
                )
                if any(action.index not in completed_action_indexes for action in connector_actions):
                    raise StateFormatError(
                        "Connector ownership deletion requires durable completed progress"
                    )
                connector_deletions: list[ManagedConnectorResourceDeletion] = []
                for action in connector_actions:
                    evidence = action.connector_evidence
                    if evidence is None:  # pragma: no cover - filtered above
                        raise StateFormatError(
                            "Connector ownership deletion requires exact action evidence"
                        )
                    connector_deletions.append(
                        ManagedConnectorResourceDeletion(
                            resource_id=action.resource_id,
                            backend_identity=evidence.backend_identity,
                            connector_name=evidence.connector_name,
                            prior_artifact_checksum=evidence.prior_artifact_checksum,
                        )
                    )
                managed_connector_deletions = tuple(connector_deletions)
                next_state = (
                    updated_local_state(
                        prior_state,
                        deployment_plan,
                        managed_gateway_deletions=managed_gateway_deletions,
                        managed_connector_deletions=managed_connector_deletions,
                    )
                    if managed_connector_deletions
                    else prevalidated_next_state
                )
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
                        "deployment succeeded but ownership state commit could not be confirmed",
                        operation_id=operation_id,
                    ) from error
                operation_finalized = True
                if lineage is not None:
                    # A returned finalizer has durably committed ownership and
                    # cleared the operation marker. A later verified authority-
                    # release error does not undo this COMPLETE boundary.
                    lineage.terminal("COMPLETE")
                if next_state is not None:
                    results["state_serial"] = next_state.serial
                    if project.deployment_state.backend == "local":
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
                if lineage is not None:
                    lineage.close()
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
            except BaseException as error:
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
                if lineage is not None:
                    lineage.terminal("ABORT" if isinstance(error, KeyboardInterrupt) else "FAIL")
                    lineage.close()
                raise
        finally:
            close_deployers(sr, kafka, flink, connect, gateway, kafka_streams)

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
    except ConnectorRemovalRuntimeRequiredError as e:
        safe_message = redact_sensitive_text(e)
        fmt.add_error(StructuredError(code=ErrorCode.CONNECT_REQUIRED, message=safe_message))
        fmt.print_error(safe_message)
        fmt.flush()
        sys.exit(1)
    except ConnectorRemovalClusterReferenceError as e:
        safe_message = redact_sensitive_text(e)
        fmt.add_error(StructuredError(code=ErrorCode.INVALID_CLUSTER_REF, message=safe_message))
        fmt.print_error(safe_message)
        fmt.flush()
        sys.exit(1)
    except ConnectorRemovalPreflightError as e:
        safe_message = redact_sensitive_text(e)
        fmt.add_error(
            StructuredError(code=ErrorCode.CONNECTOR_REMOVAL_INVALID, message=safe_message)
        )
        fmt.print_error(safe_message)
        fmt.flush()
        sys.exit(1)
    except ConnectorRemovalStateAuthorityError as e:
        safe_message = redact_sensitive_text(e)
        fmt.add_error(
            StructuredError(code=ErrorCode.STATE_BACKEND_UNAVAILABLE, message=safe_message)
        )
        fmt.print_error(safe_message)
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
        safe_message, error_operation_id = state_operation_error_details(e)
        release_data = dict(verified_commit_data or {})
        release_data["committed"] = e.committed
        fmt.set_data(release_data)
        fmt.add_error(
            StructuredError(
                code=ErrorCode.STATE_RELEASE_FAILED_AFTER_COMMIT,
                message=safe_message,
                operation_id=error_operation_id,
            )
        )
        fmt.print_error(safe_message)
        fmt.flush()
        sys.exit(1)
    except StateBackendLockTimeoutError as e:
        safe_message = redact_sensitive_text(e)
        fmt.add_error(StructuredError(code=ErrorCode.STATE_LOCK_TIMEOUT, message=safe_message))
        fmt.print_error(safe_message)
        fmt.flush()
        sys.exit(1)
    except StateBackendLockLostError as e:
        safe_message, error_operation_id = state_operation_error_details(e)
        fmt.add_error(
            StructuredError(
                code=ErrorCode.STATE_LOCK_LOST,
                message=safe_message,
                operation_id=error_operation_id,
            )
        )
        fmt.print_error(safe_message)
        fmt.flush()
        sys.exit(1)
    except StateBackendConflictError as e:
        safe_message = redact_sensitive_text(e)
        fmt.add_error(StructuredError(code=ErrorCode.STATE_CONFLICT, message=safe_message))
        fmt.print_error(safe_message)
        fmt.flush()
        sys.exit(1)
    except StateBackendUnknownCommitError as e:
        safe_message, error_operation_id = state_operation_error_details(e)
        fmt.add_error(
            StructuredError(
                code=ErrorCode.STATE_UNKNOWN_OUTCOME,
                message=safe_message,
                operation_id=error_operation_id,
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
        fmt.add_error(StructuredError(code=ErrorCode.STATE_INVALID, message=safe_message))
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
        try:
            operation_stack.close()
        finally:
            if lineage is not None:
                lineage.close()
