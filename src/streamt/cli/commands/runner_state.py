"""Exact-operation runner diagnostics and explicit reviewed continuation."""

from __future__ import annotations

import uuid
from pathlib import Path

import click

from streamt.cli.helpers import close_deployers, get_project_path, make_formatter
from streamt.core.deployment_state import RemoteStateRequiredError, enforce_remote_state_policy
from streamt.core.environment import EnvironmentError
from streamt.core.errors import ErrorCode
from streamt.core.models import StreamtProject
from streamt.core.parser import EnvVarError, ParseError, ProjectParser
from streamt.core.validator import ProjectValidator
from streamt.deployer.kafka_streams import KafkaStreamsDeployer
from streamt.deployer.kafka_streams_replacement_coordinator import (
    KafkaStreamsReplacementCoordinator,
)
from streamt.deployer.kafka_streams_replacement_executor import ReplacementExecutionState
from streamt.deployer.kafka_streams_replacement_observer import KafkaStreamsReplacementObserver
from streamt.deployer.plan_file import PlanFileError, ReviewedPlanFile, StalePlanError
from streamt.deployer.state import StateError
from streamt.deployer.state_backend import (
    StateBackendConflictError,
    StateBackendLockLostError,
    StateBackendLockTimeoutError,
    StateBackendRecoveryRequiredError,
    StateBackendReleaseAfterCommitError,
    StateBackendUnavailableError,
    StateBackendUnknownCommitError,
    make_deployment_state_service,
)
from streamt.output import StructuredError


def _canonical_operation_id(value: str) -> str:
    try:
        parsed = uuid.UUID(value)
        if str(parsed) != value or parsed.int == 0:
            raise ValueError("invalid")
    except (ValueError, AttributeError):
        raise PlanFileError("An exact original operation UUID is required") from None
    return value


def _error_code(error: Exception) -> str:
    for error_type, code in (
        (StalePlanError, ErrorCode.PLAN_STALE),
        (PlanFileError, ErrorCode.PLAN_FILE_INVALID),
        (ParseError, ErrorCode.PARSE_ERROR),
        (EnvVarError, ErrorCode.PARSE_ERROR),
        (RemoteStateRequiredError, ErrorCode.REMOTE_STATE_REQUIRED),
        (EnvironmentError, ErrorCode.ENVIRONMENT_ERROR),
        (StateBackendReleaseAfterCommitError, ErrorCode.STATE_RELEASE_FAILED_AFTER_COMMIT),
        (StateBackendUnknownCommitError, ErrorCode.STATE_UNKNOWN_OUTCOME),
        (StateBackendLockTimeoutError, ErrorCode.STATE_LOCK_TIMEOUT),
        (StateBackendLockLostError, ErrorCode.STATE_LOCK_LOST),
        (StateBackendUnavailableError, ErrorCode.STATE_BACKEND_UNAVAILABLE),
        (StateBackendConflictError, ErrorCode.STATE_CONFLICT),
        (StateBackendRecoveryRequiredError, ErrorCode.STATE_RECOVERY_REQUIRED),
        (StateError, ErrorCode.STATE_INVALID),
    ):
        if isinstance(error, error_type):
            return code
    return ErrorCode.DEPLOY_ERROR


def _run(
    ctx: click.Context, *, plan_path: Path, operation_id: str,
    project_dir: str | None, environment: str | None, resume: bool,
    timeout: float = 60, confirm_env: str | None = None,
) -> None:
    fmt = make_formatter(ctx, "state resume" if resume else "state runner-status")
    canonical_id = None
    plan = None
    holder = None
    deployer = None
    mutation_attempted = False
    try:
        canonical_id = _canonical_operation_id(operation_id)
        if not 0 < timeout <= 600:
            raise PlanFileError("Runner timeout must be bounded")
        plan = ReviewedPlanFile.load(plan_path)
        KafkaStreamsReplacementCoordinator._reviewed(plan)
        project_path = get_project_path(project_dir)
        parser = ProjectParser(project_path, environment=environment, warn_callback=lambda _message: None)
        initial_project = parser.parse()
        initial_environment = parser.env_config
        last_read_project = initial_project

        def read_project() -> StreamtProject:
            nonlocal last_read_project
            fresh_parser = ProjectParser(project_path, environment=environment, warn_callback=lambda _message: None)
            project = fresh_parser.parse()
            if (
                project.deployment_state != initial_project.deployment_state
                or fresh_parser.env_config != initial_environment
                or project.project.name != initial_project.project.name
                or project.environment_name != initial_project.environment_name
            ):
                raise PlanFileError("Replacement environment or state authority changed")
            enforce_remote_state_policy(
                project.deployment_state,
                required=bool(fresh_parser.env_config and fresh_parser.env_config.requires_remote_state),
            )
            if resume:
                effective = project.environment_name
                if confirm_env is not None and confirm_env != effective:
                    raise EnvironmentError("Explicit environment confirmation does not match")
                if fresh_parser.env_config and fresh_parser.env_config.requires_apply_confirmation and confirm_env != effective:
                    raise EnvironmentError("Runner resume requires --confirm-env for this environment")
            if not ProjectValidator(project).validate().is_valid:
                raise PlanFileError("The full current runner project is invalid")
            last_read_project = project
            return project

        project = read_project()
        service = make_deployment_state_service(
            project_path, project=project.project.name, environment=project.environment_name,
            config=project.deployment_state,
        )

        def observer_factory() -> KafkaStreamsReplacementObserver:
            nonlocal deployer
            # The coordinator invokes this factory only after validating and
            # compiling the exact instance returned by its context reader.
            # A new parse here would introduce an unchecked context before I/O.
            current = last_read_project
            if current.runtime.kafka_streams is None:
                raise PlanFileError("The reviewed runner runtime is not configured")
            deployer = KafkaStreamsDeployer(
                current.runtime.kafka_streams, current.runtime.kafka, state_dir=project_path / ".streamt",
            )
            return KafkaStreamsReplacementObserver(deployer)

        coordinator = KafkaStreamsReplacementCoordinator(None, read_project, observer_factory=observer_factory)
        with service.operation() as operation:
            holder = ReplacementExecutionState(operation.observe())
            report = coordinator.inspect(operation, holder, plan=plan, operation_id=canonical_id)
            if resume:
                if report["status"] == "blocked":
                    raise StateBackendRecoveryRequiredError("The exact runner frontier cannot safely continue")
                mutation_attempted = report["status"] != "completed"
                coordinator.resume(
                    operation, holder, plan=plan, operation_id=canonical_id,
                    actor="local-cli", timeout_seconds=timeout,
                )
                # Success requires the durable receipt and exact live candidate,
                # not merely an absent operation marker.
                report = coordinator.inspect(operation, holder, plan=plan, operation_id=canonical_id)
                report["read_only"] = not mutation_attempted
        fmt.set_data(report)
        fmt.print(f"Runner operation {canonical_id}: {report['status']}; next action: {report['next_action']}.")
        fmt.flush()
    except Exception as error:
        code = _error_code(error)
        # A failed read cannot establish whether an earlier invocation committed.
        committed: bool | None = True if isinstance(error, StateBackendReleaseAfterCommitError) else None
        phase = "unknown"
        if holder is not None:
            progress = holder.snapshot.control.control.progress
            phase = progress[-1].status if progress else "intent" if holder.snapshot.control.control.intent else "clear"
            if progress and progress[-1].kafka_streams_checkpoint is not None:
                phase = progress[-1].kafka_streams_checkpoint.phase
        data: dict[str, object] = {
            "operation_id": canonical_id, "plan_checksum": plan.checksum if plan is not None else None,
            "state_serial": holder.snapshot.state.state.serial if holder is not None else None,
            "committed": committed, "status": "blocked", "lifecycle_phase": phase,
            "lifecycle_phase_evidence": "last_acknowledged",
            "resumable": False, "next_action": "inspect_original_operation", "read_only": not mutation_attempted,
        }
        # Provider messages and paths never enter public structured output.
        message = {
            ErrorCode.PLAN_FILE_INVALID: "Original reviewed runner plan or current project is invalid or mismatched.",
            ErrorCode.ENVIRONMENT_ERROR: "Select the exact environment with --env; resume also requires --confirm-env when its policy demands confirmation.",
            ErrorCode.REMOTE_STATE_REQUIRED: "This environment requires PostgreSQL deployment state.",
            ErrorCode.STATE_UNKNOWN_OUTCOME: "Runner operation outcome is unknown; inspect the original operation before retrying.",
        }.get(code, "Runner operation could not be verified; retain its original plan and operation ID for diagnosis.")
        fmt.set_data(data)
        fmt.add_error(StructuredError(code=code, message=message, operation_id=canonical_id,
                                      suggestion="Use state runner-status with the original --plan and --operation-id; do not create a replacement plan."))
        fmt.print_error(message)
        fmt.flush()
        raise click.exceptions.Exit(1) from None
    finally:
        close_deployers(deployer)


@click.command("runner-status")
@click.option("--plan", "plan_path", type=click.Path(path_type=Path), required=True, help="Original reviewed format-6 plan file.")
@click.option("--operation-id", required=True, help="Exact original operation UUID.")
@click.option("--project-dir", "-p", type=click.Path(exists=True), default=None)
@click.option("--env", "-e", "environment", default=None)
@click.pass_context
def runner_status(ctx: click.Context, plan_path: Path, operation_id: str, project_dir: str | None, environment: str | None) -> None:
    """Read the exact runner frontier; never authorize or execute recovery."""
    _run(ctx, plan_path=plan_path, operation_id=operation_id, project_dir=project_dir, environment=environment, resume=False)


@click.command("resume")
@click.option("--plan", "plan_path", type=click.Path(path_type=Path), required=True, help="Original reviewed format-6 plan file.")
@click.option("--operation-id", required=True, help="Exact original operation UUID; no new plan is created.")
@click.option("--project-dir", "-p", type=click.Path(exists=True), default=None)
@click.option("--env", "-e", "environment", default=None)
@click.option("--confirm-env", default=None, help="Exact environment confirmation, mandatory when required by environment policy.")
@click.option("--timeout", type=click.FloatRange(min=0, max=600, min_open=True), default=60, show_default=True)
@click.pass_context
def runner_resume(ctx: click.Context, plan_path: Path, operation_id: str, project_dir: str | None, environment: str | None, confirm_env: str | None, timeout: float) -> None:
    """Continue the original reviewed runner operation under a fresh state lock."""
    _run(ctx, plan_path=plan_path, operation_id=operation_id, project_dir=project_dir, environment=environment,
         resume=True, confirm_env=confirm_env, timeout=timeout)


def register_runner_state_commands(state: click.Group) -> None:
    """Register without importing the parent state module or creating a cycle."""
    state.add_command(runner_status)
    state.add_command(runner_resume)
