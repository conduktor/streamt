"""streamt plan command."""

from __future__ import annotations

import sys
from contextlib import ExitStack
from pathlib import Path
from typing import Optional

import click

from streamt.cli.connector_removal_guard import (
    enforce_connector_removal_plan_authorization,
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
    make_sr_deployer,
    redact_sensitive_text,
)
from streamt.compiler.connector_artifact import (
    CONNECTOR_REMOVAL_PLANNING_UNAVAILABLE_MESSAGE,
    ConnectorRemovalClusterReferenceError,
    ConnectorRemovalPreflightError,
    ConnectorRemovalRuntimeRequiredError,
    ConnectorRemovalStateAuthorityError,
)
from streamt.core.deployment_state import RemoteStateRequiredError
from streamt.core.errors import ErrorCode
from streamt.core.models import StreamtProject
from streamt.deployer.connect import ConnectorChange, secret_neutral_connector_changes
from streamt.deployer.gateway import GatewayRuleChange, secret_neutral_gateway_changes
from streamt.deployer.operation_actions import operation_actions_from_planned
from streamt.deployer.plan_file import (
    PLAN_FILE_VERSION,
    PlanFileError,
    ReviewedPlanFile,
    StateReference,
)
from streamt.deployer.state import (
    LOCAL_STATE_CI_WARNING,
    LocalState,
    StateError,
    local_state_path,
)
from streamt.deployer.state_backend import (
    StateBackendInvalidStateError,
    StateBackendLockLostError,
    StateBackendLockTimeoutError,
    StateBackendRecoveryRequiredError,
    StateBackendUnavailableError,
    make_deployment_state_service,
)
from streamt.output import StructuredError


def _connector_change_data(change: ConnectorChange) -> dict[str, object]:
    """Serialize a connector plan without carrying raw configuration values."""
    return {
        "type": "connector",
        "name": change.connector_name,
        "action": change.action,
        "changes": secret_neutral_connector_changes(change.changes),
    }


def _gateway_change_data(change: GatewayRuleChange) -> dict[str, object]:
    """Serialize a Gateway plan with exact secret-neutral aggregate evidence."""
    return {
        "type": "gateway_rule",
        "name": change.name,
        "action": change.action,
        "changes": secret_neutral_gateway_changes(change.changes),
    }


@click.command()
@click.option("--project-dir", "-p", type=click.Path(exists=True), help="Path to project directory")
@click.option(
    "--env", "-e", "environment", help="Target environment (reads from STREAMT_ENV if not set)"
)
@click.option(
    "--offline",
    is_flag=True,
    help="Plan without connecting to infrastructure (assumes fresh deploy)",
)
@click.option(
    "--out",
    "plan_output",
    type=click.Path(dir_okay=False, path_type=Path),
    help="Atomically save a deterministic reviewed plan file",
)
@click.pass_context
def plan(
    ctx: click.Context,
    project_dir: Optional[str],
    environment: Optional[str],
    offline: bool,
    plan_output: Optional[Path],
) -> None:
    """Show what would change on apply."""
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

    fmt = make_formatter(ctx, "plan")
    project_path = get_project_path(project_dir)
    connector_removal_workflow = False

    try:
        parser = ProjectParser(
            project_path,
            environment=environment,
            warn_callback=lambda msg: fmt.print(msg),
        )
        project = parser.parse()
        connector_removal_workflow = bool(
            isinstance(project, StreamtProject) and project.lifecycle.connector_removals
        )
        enforce_connector_removal_plan_authorization(
            (project.lifecycle.connector_removals if isinstance(project, StreamtProject) else []),
            fmt,
            offline=offline,
            plan_output=plan_output,
        )
        parsed_environment = parser.env_config.environment.name if parser.env_config else None
        effective_environment = (
            parsed_environment
            if isinstance(parsed_environment, str) and parsed_environment
            else "default"
        )

        validator = ProjectValidator(project)
        result = validator.validate()
        if not result.is_valid:
            for error in result.errors:
                fmt.add_error(StructuredError(code=ErrorCode.PARSE_ERROR, message=error.message))
                fmt.print_error(error.message)
            fmt.flush()
            sys.exit(1)

        compiler = Compiler(project)
        manifest = compiler.compile(dry_run=True)
        raw_connector_removals = manifest.artifacts.get("connector_removals", [])
        connector_removal_workflow = connector_removal_workflow or (
            type(raw_connector_removals) is not list or bool(raw_connector_removals)
        )
        require_connector_removal_postgres_state(
            raw_connector_removals,
            project.deployment_state,
        )
        prior_state: LocalState | None = None
        state_reference: StateReference | None = None
        reviewed_plan: ReviewedPlanFile | None = None
        operation_status: dict[str, object] = {
            "status": "unavailable",
            "operation_id": None,
            "kind": None,
            "failure_code": None,
            "last_completed_action_index": None,
        }

        if offline:
            planner = DeploymentPlanner(
                manifest,
                project=project,
                project_name=project.project.name,
                environment=effective_environment,
            )
            deployment_plan = planner.offline_plan()
            if plan_output:
                reviewed_plan = ReviewedPlanFile.create(
                    deployment_plan,
                    manifest,
                    project=project.project.name,
                    environment=effective_environment,
                    runtime=project.runtime,
                    state=None,
                    offline=True,
                    actions=(),
                )
                reviewed_plan.save(plan_output)

            fmt.print("[yellow]Offline plan — assumes no existing resources[/yellow]\n")
        else:
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
            with ExitStack() as state_stack:
                try:
                    state_operation = state_stack.enter_context(state_service.operation())
                    planning_snapshot = state_operation.observe()
                    state_operation.ensure_ready(planning_snapshot)
                except StateBackendInvalidStateError:
                    if connector_removal_workflow:
                        raise ConnectorRemovalStateAuthorityError(
                            "PostgreSQL-v2 Connector removal authority is invalid"
                        ) from None
                    raise
                operation_status = planning_snapshot.control.safe_status()
                prior_state = planning_snapshot.state.state
                state_reference = StateReference.from_observation(planning_snapshot.state)
                if project.deployment_state.backend == "local":
                    fmt.print_warning(
                        f"{LOCAL_STATE_CI_WARNING} State file: "
                        f"{local_state_path(project_path, environment=effective_environment)}",
                        code=ErrorCode.LOCAL_STATE_ONLY,
                    )

                if connector_removal_workflow:
                    resolve_connector_planning_targets(
                        manifest,
                        project,
                        environment=effective_environment,
                        prior_state=prior_state,
                        require_authoritative_state=True,
                    )
                    raise ConnectorRemovalPreflightError(
                        CONNECTOR_REMOVAL_PLANNING_UNAVAILABLE_MESSAGE
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

                # Create deployers only after provider-free identity validation.
                sr_deployer = make_sr_deployer(project, fmt)
                kafka_deployer = make_kafka_deployer(project, fmt)
                flink_deployer = make_flink_deployer(
                    project,
                    fmt,
                    state_dir=project_path / ".streamt",
                )
                connect_deployer = make_connect_deployer(project, fmt)
                gateway_deployer = make_gateway_deployer(project, fmt)

                if not check_required_deployers(
                    project,
                    kafka_deployer,
                    sr_deployer,
                    flink_deployer,
                    connect_deployer,
                    gateway_deployer,
                    fmt,
                ):
                    close_deployers(
                        sr_deployer,
                        kafka_deployer,
                        flink_deployer,
                        connect_deployer,
                        gateway_deployer,
                    )
                    fmt.flush()
                    sys.exit(1)

                try:
                    planner = DeploymentPlanner(
                        manifest,
                        schema_registry_deployer=sr_deployer,
                        kafka_deployer=kafka_deployer,
                        flink_deployer=flink_deployer,
                        connect_deployer=connect_deployer,
                        gateway_deployer=gateway_deployer,
                        project=project,
                        prior_state=prior_state,
                        project_name=project.project.name,
                        environment=effective_environment,
                    )
                    deployment_plan = planner.plan()
                    operation_actions = (
                        ()
                        if deployment_plan.is_apply_blocked
                        else operation_actions_from_planned(
                            planner.planned_actions(deployment_plan)
                        )
                    )
                finally:
                    close_deployers(
                        sr_deployer,
                        kafka_deployer,
                        flink_deployer,
                        connect_deployer,
                        gateway_deployer,
                    )

                if plan_output:
                    reviewed_plan = ReviewedPlanFile.create(
                        deployment_plan,
                        manifest,
                        project=project.project.name,
                        environment=effective_environment,
                        runtime=project.runtime,
                        state=state_reference,
                        offline=False,
                        actions=operation_actions,
                    )
                    reviewed_plan.save(plan_output)

        changes: list[dict[str, object]] = []
        for schema_change in deployment_plan.schema_changes:
            if schema_change.action != "none":
                changes.append(
                    {
                        "type": "schema",
                        "name": schema_change.subject,
                        "action": schema_change.action,
                        "changes": schema_change.changes,
                    }
                )
        for topic_change in deployment_plan.topic_changes:
            if topic_change.action != "none":
                changes.append(
                    {
                        "type": "topic",
                        "name": topic_change.topic,
                        "action": topic_change.action,
                        "changes": topic_change.changes,
                    }
                )
        for flink_change in deployment_plan.flink_changes:
            if flink_change.action != "none":
                changes.append(
                    {
                        "type": "flink_job",
                        "name": flink_change.job_name,
                        "action": flink_change.action,
                    }
                )
        for connector_change in deployment_plan.connector_changes:
            if connector_change.action != "none":
                changes.append(_connector_change_data(connector_change))
        for gateway_change in deployment_plan.gateway_changes:
            if gateway_change.action != "none":
                changes.append(_gateway_change_data(gateway_change))

        plan_data: dict[str, object] = {
            "offline": offline,
            "summary": deployment_plan.summary(),
            "creates": deployment_plan.creates,
            "updates": deployment_plan.updates,
            "deletes": deployment_plan.deletes,
            "has_changes": deployment_plan.has_changes,
            "is_apply_blocked": deployment_plan.is_apply_blocked,
            "risk_summary": deployment_plan.risk_summary,
            "change_risks": [risk.to_dict() for risk in deployment_plan.ordered_change_risks],
            "state_serial": prior_state.serial if prior_state is not None else None,
            "operation_status": operation_status,
            "changes": changes,
            "ownership_requirements": [
                requirement.to_dict() for requirement in deployment_plan.ownership_requirements
            ],
            "safety_blockers": [
                blocker.to_dict() for blocker in deployment_plan.ordered_safety_blockers
            ],
            "gateway_removal_assessments": [
                assessment.to_dict() for assessment in deployment_plan.gateway_removal_assessments
            ],
        }
        if plan_output and reviewed_plan is not None:
            fmt.print(f"[green]Saved reviewed plan to {plan_output.resolve()}[/green]")
            plan_data["plan_file"] = str(plan_output.resolve())
            plan_data["plan_checksum"] = reviewed_plan.checksum
            plan_data["manifest_checksum"] = reviewed_plan.manifest_checksum
            plan_data["plan_format_version"] = PLAN_FILE_VERSION
        fmt.set_data(plan_data)
        fmt.print(deployment_plan.details())
        fmt.flush()

    except (EnvVarError, ParseError, EnvironmentError) as e:
        handle_parse_error(fmt, e, ErrorCode.PARSE_ERROR)
    except PlanFileError as e:
        fmt.add_error(StructuredError(code=ErrorCode.PLAN_FILE_INVALID, message=str(e)))
        fmt.print_error(str(e))
        fmt.flush()
        sys.exit(1)
    except RemoteStateRequiredError as e:
        safe_message = redact_sensitive_text(e)
        fmt.add_error(StructuredError(code=ErrorCode.REMOTE_STATE_REQUIRED, message=safe_message))
        fmt.print_error(safe_message)
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
    except StateBackendLockTimeoutError as e:
        safe_message = redact_sensitive_text(e)
        fmt.add_error(StructuredError(code=ErrorCode.STATE_LOCK_TIMEOUT, message=safe_message))
        fmt.print_error(safe_message)
        fmt.flush()
        sys.exit(1)
    except StateBackendLockLostError as e:
        safe_message = redact_sensitive_text(e)
        fmt.add_error(StructuredError(code=ErrorCode.STATE_LOCK_LOST, message=safe_message))
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
        fmt.add_error(StructuredError(code=ErrorCode.CONNECTION_REFUSED, message=str(e)))
        fmt.print_error(str(e))
        fmt.flush()
        sys.exit(1)
