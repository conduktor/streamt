"""Deployment ownership-state administrative commands."""

from __future__ import annotations

import sys
from typing import Optional

import click

from streamt.cli.helpers import (
    get_project_path,
    handle_parse_error,
    make_formatter,
    redact_sensitive_text,
)
from streamt.core.errors import ErrorCode
from streamt.deployer.postgres_state import (
    make_postgres_state_administration,
    make_postgres_state_initializer,
    make_postgres_state_lock_probe,
)
from streamt.deployer.state import StateError
from streamt.deployer.state_backend import (
    StateAddress,
    StateBackendInvalidStateError,
    StateBackendUnavailableError,
    StateBackendUnknownCommitError,
    make_deployment_state_service,
    state_checksum,
)
from streamt.output import StructuredError


@click.group()
def state() -> None:
    """Inspect or explicitly initialize deployment ownership state."""


@state.command("init")
@click.option(
    "--project-dir",
    "-p",
    type=click.Path(exists=True, file_okay=False),
    help="Path to project directory",
)
@click.option(
    "--env",
    "-e",
    "environment",
    help="Target environment (reads from STREAMT_ENV if not set)",
)
@click.option(
    "--confirm-project",
    help="Exact parsed project name required to authorize initialization",
)
@click.option(
    "--confirm-env",
    "confirm_environment",
    help="Exact effective environment required to authorize initialization",
)
@click.option(
    "--confirm-address",
    help="Exact canonical state address required to authorize initialization",
)
@click.pass_context
def state_init(
    ctx: click.Context,
    project_dir: Optional[str],
    environment: Optional[str],
    confirm_project: Optional[str],
    confirm_environment: Optional[str],
    confirm_address: Optional[str],
) -> None:
    """Explicitly initialize a configured PostgreSQL state store/address."""
    from streamt.core.environment import EnvironmentError
    from streamt.core.parser import EnvVarError, ParseError, ProjectParser

    fmt = make_formatter(ctx, "state init")
    project_path = get_project_path(project_dir)

    try:
        parser = ProjectParser(
            project_path,
            environment=environment,
            warn_callback=lambda message: fmt.print(message),
        )
        project = parser.parse()
        parsed_environment = (
            parser.env_config.environment.name if parser.env_config else None
        )
        effective_environment = (
            parsed_environment
            if isinstance(parsed_environment, str) and parsed_environment
            else "default"
        )
        if project.deployment_state.backend != "postgres":
            raise StateBackendUnavailableError(
                "PostgreSQL deployment state initialization is not configured"
            )

        address = StateAddress(
            namespace=project.deployment_state.namespace,
            project=project.project.name,
            environment=effective_environment,
        )
        if (
            confirm_project != project.project.name
            or confirm_environment != effective_environment
            or confirm_address != address.uri
        ):
            raise StateBackendInvalidStateError(
                "PostgreSQL deployment state initialization confirmation must "
                "exactly match the project, effective environment, and canonical address"
            )

        initializer = make_postgres_state_initializer(project.deployment_state)
        result = initializer.initialize(address)
        data = result.to_dict()
        fmt.set_data(data)
        fmt.print("[cyan]PostgreSQL deployment state initialization[/cyan]")
        fmt.print(f"  Outcome: {result.outcome}")
        fmt.print(f"  Store ID: {result.store_id}")
        fmt.print(f"  Address: {address.uri}")
        fmt.print("  Ownership: absent")
        fmt.print("  Operation: clear")
        fmt.print("  Ordinary state authority: disabled")
        fmt.flush()
    except (EnvVarError, ParseError, EnvironmentError) as error:
        handle_parse_error(fmt, error, ErrorCode.PARSE_ERROR)
    except (StateBackendUnavailableError, StateBackendUnknownCommitError) as error:
        safe_message = redact_sensitive_text(error)
        fmt.add_error(
            StructuredError(
                code=ErrorCode.STATE_BACKEND_UNAVAILABLE,
                message=safe_message,
            )
        )
        fmt.print_error(safe_message)
        fmt.flush()
        sys.exit(1)
    except StateError as error:
        safe_message = redact_sensitive_text(error)
        fmt.add_error(
            StructuredError(
                code=ErrorCode.STATE_INVALID,
                message=safe_message,
            )
        )
        fmt.print_error(safe_message)
        fmt.flush()
        sys.exit(1)


@state.command("lock-status")
@click.option(
    "--project-dir",
    "-p",
    type=click.Path(exists=True, file_okay=False),
    help="Path to project directory",
)
@click.option(
    "--env",
    "-e",
    "environment",
    help="Target environment (reads from STREAMT_ENV if not set)",
)
@click.pass_context
def state_lock_status(
    ctx: click.Context,
    project_dir: Optional[str],
    environment: Optional[str],
) -> None:
    """Probe the instantaneous PostgreSQL advisory-lock state."""
    from streamt.core.environment import EnvironmentError
    from streamt.core.parser import EnvVarError, ParseError, ProjectParser

    fmt = make_formatter(ctx, "state lock-status")
    project_path = get_project_path(project_dir)

    try:
        parser = ProjectParser(
            project_path,
            environment=environment,
            warn_callback=lambda message: fmt.print(message),
        )
        project = parser.parse()
        parsed_environment = (
            parser.env_config.environment.name if parser.env_config else None
        )
        effective_environment = (
            parsed_environment
            if isinstance(parsed_environment, str) and parsed_environment
            else "default"
        )
        if project.deployment_state.backend != "postgres":
            raise StateBackendUnavailableError(
                "PostgreSQL deployment state lock probing is not configured"
            )

        address = StateAddress(
            namespace=project.deployment_state.namespace,
            project=project.project.name,
            environment=effective_environment,
        )
        probe = make_postgres_state_lock_probe(project.deployment_state)
        result = probe.probe(address)
        fmt.set_data(result.to_dict())

        fmt.print("[cyan]PostgreSQL deployment state lock probe[/cyan]")
        fmt.print(f"  Address: {address.uri}")
        fmt.print(f"  Lock: {result.lock_status}")
        fmt.print("  Observation: instantaneous and racy")
        fmt.print("  Reservation: none")
        fmt.print("  Durable operation status: use `streamt state status`")
        fmt.flush()
    except (EnvVarError, ParseError, EnvironmentError) as error:
        handle_parse_error(fmt, error, ErrorCode.PARSE_ERROR)
    except StateBackendUnavailableError as error:
        safe_message = redact_sensitive_text(error)
        fmt.add_error(
            StructuredError(
                code=ErrorCode.STATE_BACKEND_UNAVAILABLE,
                message=safe_message,
            )
        )
        fmt.print_error(safe_message)
        fmt.flush()
        sys.exit(1)
    except StateError as error:
        safe_message = redact_sensitive_text(error)
        fmt.add_error(
            StructuredError(
                code=ErrorCode.STATE_INVALID,
                message=safe_message,
            )
        )
        fmt.print_error(safe_message)
        fmt.flush()
        sys.exit(1)


@state.command("status")
@click.option(
    "--project-dir",
    "-p",
    type=click.Path(exists=True, file_okay=False),
    help="Path to project directory",
)
@click.option(
    "--env",
    "-e",
    "environment",
    help="Target environment (reads from STREAMT_ENV if not set)",
)
@click.pass_context
def state_status(
    ctx: click.Context,
    project_dir: Optional[str],
    environment: Optional[str],
) -> None:
    """Show safe configured ownership and unfinished-operation status."""
    from streamt.core.environment import EnvironmentError
    from streamt.core.parser import EnvVarError, ParseError, ProjectParser

    fmt = make_formatter(ctx, "state status")
    project_path = get_project_path(project_dir)

    try:
        parser = ProjectParser(
            project_path,
            environment=environment,
            warn_callback=lambda message: fmt.print(message),
        )
        project = parser.parse()
        parsed_environment = (
            parser.env_config.environment.name if parser.env_config else None
        )
        effective_environment = (
            parsed_environment
            if isinstance(parsed_environment, str) and parsed_environment
            else "default"
        )

        if project.deployment_state.backend == "postgres":
            address = StateAddress(
                namespace=project.deployment_state.namespace,
                project=project.project.name,
                environment=effective_environment,
            )
            administration = make_postgres_state_administration(project.deployment_state)
            postgres_status = administration.status(address)
            postgres_data = postgres_status.to_dict()
            fmt.set_data(postgres_data)

            fmt.print("[cyan]PostgreSQL deployment state[/cyan]")
            fmt.print(f"  Store: {postgres_status.store_status}")
            fmt.print(f"  Address: {address.uri}")
            fmt.print(f"  Registration: {postgres_status.address_status}")
            fmt.print(f"  Ownership: {postgres_status.state_status}")
            postgres_operation_status = postgres_status.operation_status
            if postgres_operation_status is not None:
                fmt.print(f"  Operation: {postgres_operation_status.status}")
            fmt.print("  Mutation: disabled")
            fmt.flush()
            return

        service = make_deployment_state_service(
            project_path,
            project=project.project.name,
            environment=effective_environment,
            config=project.deployment_state,
        )
        observation = service.read()
        control = service.read_control()
        local_operation_status = control.safe_status()
        ownership_checksum = state_checksum(observation.state)
        state_status_value = (
            "absent" if observation.revision.is_absent else "present"
        )
        data: dict[str, object] = {
            "backend": observation.store.backend,
            "store_id": observation.store.store_id,
            "address": observation.address.uri,
            "state_status": state_status_value,
            "state_serial": observation.state_serial,
            "state_checksum": ownership_checksum,
            "operation_status": local_operation_status,
        }
        fmt.set_data(data)

        fmt.print("[cyan]Deployment state[/cyan]")
        fmt.print(f"  Backend: {observation.store.backend}")
        fmt.print(f"  Store ID: {observation.store.store_id}")
        fmt.print(f"  Address: {observation.address.uri}")
        fmt.print(
            f"  Ownership: {state_status_value} "
            f"(serial {observation.state_serial})"
        )
        fmt.print(f"  Checksum: {ownership_checksum}")
        fmt.print(f"  Operation: {local_operation_status['status']}")
        if local_operation_status["status"] != "clear":
            fmt.print(
                "[yellow]  Unfinished operation blocks apply/adopt. Retain the "
                "control sidecar as evidence; do not delete or edit it. "
                "Recovery is not implemented yet.[/yellow]"
            )
        fmt.flush()
    except (EnvVarError, ParseError, EnvironmentError) as error:
        handle_parse_error(fmt, error, ErrorCode.PARSE_ERROR)
    except StateBackendUnavailableError as error:
        safe_message = redact_sensitive_text(error)
        fmt.add_error(
            StructuredError(
                code=ErrorCode.STATE_BACKEND_UNAVAILABLE,
                message=safe_message,
            )
        )
        fmt.print_error(safe_message)
        fmt.flush()
        sys.exit(1)
    except StateError as error:
        safe_message = redact_sensitive_text(error)
        fmt.add_error(
            StructuredError(
                code=ErrorCode.STATE_INVALID,
                message=safe_message,
            )
        )
        fmt.print_error(safe_message)
        fmt.flush()
        sys.exit(1)
