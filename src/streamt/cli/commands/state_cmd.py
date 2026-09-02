"""Deployment ownership-state administrative commands."""

from __future__ import annotations

import sys
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Optional, cast

import click

from streamt.cli.helpers import (
    get_project_path,
    handle_parse_error,
    make_formatter,
    redact_sensitive_text,
    state_operation_error_details,
)
from streamt.core.errors import ErrorCode
from streamt.deployer.postgres_state import (
    POSTGRES_SCHEMA_V2_VERSION,
    make_postgres_state_administration,
    make_postgres_state_initializer,
    make_postgres_state_lock_probe,
    make_postgres_state_v2_migrator,
)
from streamt.deployer.state import StateError
from streamt.deployer.state_backend import (
    StateAddress,
    StateBackendConflictError,
    StateBackendInvalidStateError,
    StateBackendLockLostError,
    StateBackendLockTimeoutError,
    StateBackendRecoveryRequiredError,
    StateBackendReleaseAfterCommitError,
    StateBackendUnavailableError,
    StateBackendUnknownCommitError,
    make_deployment_state_service,
    make_recovery_state_service,
    state_checksum,
)
from streamt.output import StructuredError

if TYPE_CHECKING:
    from streamt.compiler.manifest import Manifest, TopicArtifact
    from streamt.core.models import StreamtProject
    from streamt.deployer.kafka import KafkaDeployer, TopicChange
    from streamt.deployer.recovery import (
        RecoveryResolution,
        RecoverySnapshotEvidence,
    )
    from streamt.deployer.recovery_service import (
        RecoveryLiveObservation,
        RecoveryProjectContext,
    )
    from streamt.deployer.state_backend import DeploymentStateService
    from streamt.output import OutputFormatter


_RECOVERY_RESOLUTIONS = (
    "observed",
    "rolled_back",
    "abandoned_before_mutation",
)
_RECOVERY_CHECKSUM_PREFIX = "sha256:"


@dataclass(frozen=True)
class _StrictRecoveryKafkaDeployer:
    """Make planner topic observations fail closed on incomplete config reads."""

    delegate: KafkaDeployer

    def plan_topic(self, artifact: TopicArtifact) -> TopicChange:
        from streamt.deployer.kafka import TopicChange

        current = self.delegate.get_topic_state(artifact.name, strict_config=True)
        if not current.exists:
            return TopicChange(
                topic=artifact.name,
                action="create",
                current=current,
                desired=artifact,
            )
        if (
            current.partitions is None
            or current.replication_factor is None
            or not isinstance(current.config, dict)
        ):
            raise ValueError("Strict Kafka recovery observation is incomplete")

        changes: dict[str, object] = {}
        if current.partitions != artifact.partitions:
            if artifact.partitions > current.partitions:
                changes["partitions"] = {
                    "from": current.partitions,
                    "to": artifact.partitions,
                }
            elif artifact.partitions < current.partitions:
                # Recovery only consumes the action verdict and exact live state.  It
                # must still preserve the planner's non-empty error marker here.
                changes["partitions_error"] = {
                    "from": current.partitions,
                    "to": artifact.partitions,
                }
        for key, value in artifact.config.items():
            current_value = current.config.get(key)
            if current_value is None or str(current_value).lower() != str(value).lower():
                changes[f"config.{key}"] = {"from": current_value, "to": value}
        for key, value in current.config.items():
            if key not in artifact.config:
                changes[f"config.{key}"] = {"from": value, "to": None}
        return TopicChange(
            topic=artifact.name,
            action="update" if changes else "none",
            current=current,
            desired=artifact,
            changes=changes,
        )

    def __getattr__(self, name: str) -> object:
        return getattr(self.delegate, name)


@dataclass(frozen=True)
class _RecoveryRuntime:
    """Reparse and re-observe project inputs only when the service holds its lock."""

    project_path: Path
    environment: str | None

    def _compile(self) -> tuple[StreamtProject, Manifest, str]:
        from streamt.compiler import Compiler
        from streamt.core.parser import ProjectParser
        from streamt.core.validator import ProjectValidator

        parser = ProjectParser(
            self.project_path,
            environment=self.environment,
            warn_callback=lambda _message: None,
        )
        project = parser.parse()
        validation = ProjectValidator(project).validate()
        if not validation.is_valid:
            raise ValueError("Current recovery project is invalid")
        manifest = Compiler(project).compile(dry_run=True)
        parsed_environment = (
            parser.env_config.environment.name if parser.env_config else None
        )
        effective_environment = (
            parsed_environment
            if isinstance(parsed_environment, str) and parsed_environment
            else "default"
        )
        return project, manifest, effective_environment

    def read_recovery_context(self) -> RecoveryProjectContext:
        from streamt.deployer.plan_file import (
            environment_fingerprint,
            manifest_checksum,
        )
        from streamt.deployer.recovery_service import RecoveryProjectContext

        project, manifest, effective_environment = self._compile()
        return RecoveryProjectContext(
            environment_fingerprint=environment_fingerprint(
                project.runtime,
                effective_environment,
            ),
            manifest_checksum=manifest_checksum(manifest),
        )

    def observe_recovery_targets(
        self,
        *,
        resolution: RecoveryResolution,
        snapshot: RecoverySnapshotEvidence,
    ) -> RecoveryLiveObservation:
        from streamt.cli.helpers import (
            check_required_deployers,
            close_deployers,
            make_connect_deployer,
            make_flink_deployer,
            make_gateway_deployer,
            make_kafka_deployer,
            make_sr_deployer,
        )
        from streamt.deployer.planner import DeploymentPlanner
        from streamt.deployer.recovery_observer import DeploymentPlanRecoveryObserver
        from streamt.output import OutputFormatter

        if resolution == "abandoned_before_mutation":
            raise ValueError("Abandoned-before-mutation recovery cannot observe live targets")
        project, manifest, effective_environment = self._compile()
        evidence = snapshot
        if (
            project.project.name != evidence.address.project
            or effective_environment != evidence.address.environment
        ):
            raise ValueError("Current recovery project identity changed")

        # Deployer construction failures are intentionally accumulated in a private,
        # quiet formatter.  RecoveryService exposes one generic sanitized evidence
        # error instead of provider exception text or runtime credentials.
        private_fmt = OutputFormatter("text", quiet=True)
        schema_registry = make_sr_deployer(project, private_fmt)
        kafka = make_kafka_deployer(project, private_fmt)
        flink = make_flink_deployer(
            project,
            private_fmt,
            state_dir=self.project_path / ".streamt",
        )
        connect = make_connect_deployer(project, private_fmt)
        gateway = make_gateway_deployer(project, private_fmt)
        try:
            if not check_required_deployers(
                project,
                kafka,
                schema_registry,
                flink,
                connect,
                gateway,
                private_fmt,
            ):
                raise ValueError("Required recovery provider is unavailable")
            strict_kafka = _StrictRecoveryKafkaDeployer(kafka) if kafka is not None else None
            planner = DeploymentPlanner(
                manifest,
                schema_registry_deployer=schema_registry,
                kafka_deployer=cast("KafkaDeployer | None", strict_kafka),
                flink_deployer=flink,
                connect_deployer=connect,
                gateway_deployer=gateway,
                project=project,
                prior_state=evidence.state,
                project_name=project.project.name,
                environment=effective_environment,
            )
            plan = planner.plan()
            return DeploymentPlanRecoveryObserver(
                planner=planner,
                plan=plan,
            ).observe_recovery_targets(
                resolution=resolution,
                snapshot=evidence,
            )
        finally:
            close_deployers(schema_registry, kafka, flink, connect, gateway)


@click.group()
def state() -> None:
    """Inspect, initialize, or migrate deployment ownership state."""


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
        parsed_environment = parser.env_config.environment.name if parser.env_config else None
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


@state.command("migrate-postgres-v2")
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
    "--confirm-store-id",
    help="Exact canonical PostgreSQL state store ID required to authorize migration",
)
@click.option(
    "--confirm-writer-role",
    help="Exact configured PostgreSQL writer role required to authorize migration",
)
@click.pass_context
def state_migrate_postgres_v2(
    ctx: click.Context,
    project_dir: Optional[str],
    environment: Optional[str],
    confirm_store_id: Optional[str],
    confirm_writer_role: Optional[str],
) -> None:
    """Explicitly migrate an exact PostgreSQL state catalog from v1 to v2."""
    from streamt.core.environment import EnvironmentError
    from streamt.core.parser import EnvVarError, ParseError, ProjectParser

    fmt = make_formatter(ctx, "state migrate-postgres-v2")
    project_path = get_project_path(project_dir)

    try:
        try:
            canonical_store_id = str(uuid.UUID(confirm_store_id or ""))
        except (ValueError, TypeError, AttributeError):
            canonical_store_id = ""
        writer_role_is_valid = False
        if (
            isinstance(confirm_writer_role, str)
            and confirm_writer_role
            and "\x00" not in confirm_writer_role
        ):
            try:
                writer_role_is_valid = (
                    len(confirm_writer_role.encode("utf-8")) <= 63
                )
            except UnicodeError:
                pass
        if (
            canonical_store_id != confirm_store_id
            or not writer_role_is_valid
        ):
            raise StateBackendInvalidStateError(
                "PostgreSQL deployment state migration requires exact store and "
                "writer-role confirmations"
            )
        assert isinstance(confirm_writer_role, str)

        parser = ProjectParser(
            project_path,
            environment=environment,
            warn_callback=lambda message: fmt.print(message),
        )
        project = parser.parse()
        if project.deployment_state.backend != "postgres":
            raise StateBackendUnavailableError(
                "PostgreSQL deployment state migration is not configured"
            )

        migrator = make_postgres_state_v2_migrator(project.deployment_state)
        result = migrator.migrate(
            confirmed_store_id=confirm_store_id,
            confirmed_writer_role=confirm_writer_role,
        )
        data = result.to_dict()
        data["mutation_status"] = "catalog_ready"
        fmt.set_data(data)
        fmt.print("[cyan]PostgreSQL deployment state migration[/cyan]")
        fmt.print(f"  Outcome: {result.outcome}")
        fmt.print(f"  Store ID: {result.store_id}")
        fmt.print(f"  Schema version: {POSTGRES_SCHEMA_V2_VERSION}")
        fmt.print("  Catalog mutation readiness: catalog_ready")
        fmt.print("  Ordinary state authority: disabled")
        fmt.flush()
    except (EnvVarError, ParseError, EnvironmentError) as error:
        handle_parse_error(fmt, error, ErrorCode.PARSE_ERROR)
    except StateBackendReleaseAfterCommitError as error:
        safe_message = redact_sensitive_text(error)
        fmt.set_data(
            {
                "committed": error.committed,
                "ordinary_state_authority": "disabled",
            }
        )
        fmt.add_error(
            StructuredError(
                code=ErrorCode.STATE_RELEASE_FAILED_AFTER_COMMIT,
                message=safe_message,
            )
        )
        fmt.print_error(safe_message)
        fmt.flush()
        sys.exit(1)
    except StateBackendLockTimeoutError as error:
        safe_message = redact_sensitive_text(error)
        fmt.add_error(
            StructuredError(code=ErrorCode.STATE_LOCK_TIMEOUT, message=safe_message)
        )
        fmt.print_error(safe_message)
        fmt.flush()
        sys.exit(1)
    except StateBackendUnknownCommitError as error:
        safe_message = redact_sensitive_text(error)
        fmt.add_error(
            StructuredError(code=ErrorCode.STATE_UNKNOWN_OUTCOME, message=safe_message)
        )
        fmt.print_error(safe_message)
        fmt.flush()
        sys.exit(1)
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
            StructuredError(code=ErrorCode.STATE_INVALID, message=safe_message)
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
            schema_version: int | str = (
                postgres_status.schema_version
                if postgres_status.schema_version is not None
                else "uninitialized"
            )
            fmt.print(f"  Schema version: {schema_version}")
            fmt.print(f"  Address: {address.uri}")
            fmt.print(f"  Registration: {postgres_status.address_status}")
            fmt.print(f"  Ownership: {postgres_status.state_status}")
            postgres_operation_status = postgres_status.operation_status
            if postgres_operation_status is not None:
                fmt.print(f"  Operation: {postgres_operation_status.status}")
            catalog_readiness = (
                "catalog_ready"
                if postgres_status.schema_version == POSTGRES_SCHEMA_V2_VERSION
                else "disabled"
            )
            fmt.print(f"  Catalog mutation readiness: {catalog_readiness}")
            fmt.print("  Ordinary state authority: disabled")
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
                "Create reviewed evidence with `streamt state recovery-plan`.[/yellow]"
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


def _recovery_service_and_runtime(
    project_path: Path,
    environment: str | None,
    fmt: OutputFormatter,
) -> tuple[DeploymentStateService, _RecoveryRuntime]:
    from streamt.core.parser import ProjectParser

    parser = ProjectParser(
        project_path,
        environment=environment,
        warn_callback=lambda message: fmt.print(message),
    )
    project = parser.parse()
    parsed_environment = parser.env_config.environment.name if parser.env_config else None
    effective_environment = (
        parsed_environment
        if isinstance(parsed_environment, str) and parsed_environment
        else "default"
    )
    service = make_recovery_state_service(
        project_path,
        project=project.project.name,
        environment=effective_environment,
        config=project.deployment_state,
    )
    return service, _RecoveryRuntime(project_path, environment)


def _require_recovery_confirmations(
    *,
    operation_id: str | None,
    resolution: str | None,
    evidence_checksum: str | None,
) -> tuple[str, str, str]:
    from streamt.deployer.recovery_plan import RecoveryPlanError

    try:
        parsed_operation_id = str(uuid.UUID(operation_id or ""))
    except (ValueError, TypeError, AttributeError):
        parsed_operation_id = ""
    checksum_is_valid = (
        isinstance(evidence_checksum, str)
        and len(evidence_checksum) == 71
        and evidence_checksum.startswith(_RECOVERY_CHECKSUM_PREFIX)
        and all(
            character in "0123456789abcdef"
            for character in evidence_checksum[len(_RECOVERY_CHECKSUM_PREFIX) :]
        )
    )
    if (
        parsed_operation_id != operation_id
        or resolution not in _RECOVERY_RESOLUTIONS
        or not checksum_is_valid
    ):
        raise RecoveryPlanError(
            "Recovery execution requires exact operation, resolution, and evidence "
            "checksum confirmations"
        )
    assert operation_id is not None
    assert resolution is not None
    assert evidence_checksum is not None
    return operation_id, resolution, evidence_checksum


def _recovery_error_code(error: Exception) -> str:
    from streamt.deployer.recovery_plan import RecoveryPlanError
    from streamt.deployer.recovery_service import RecoveryServiceError

    if isinstance(error, RecoveryPlanError):
        return ErrorCode.PLAN_FILE_INVALID
    if isinstance(error, StateBackendRecoveryRequiredError):
        return ErrorCode.STATE_RECOVERY_REQUIRED
    if isinstance(error, StateBackendUnavailableError):
        return ErrorCode.STATE_BACKEND_UNAVAILABLE
    if isinstance(error, StateBackendLockTimeoutError):
        return ErrorCode.STATE_LOCK_TIMEOUT
    if isinstance(error, StateBackendLockLostError):
        return ErrorCode.STATE_LOCK_LOST
    if isinstance(error, StateBackendConflictError):
        return ErrorCode.STATE_CONFLICT
    if isinstance(error, StateBackendUnknownCommitError):
        return ErrorCode.STATE_UNKNOWN_OUTCOME
    if isinstance(error, StateBackendReleaseAfterCommitError):
        return ErrorCode.STATE_RELEASE_FAILED_AFTER_COMMIT
    if isinstance(error, RecoveryServiceError):
        message = str(error)
        if "confirmation" in message:
            return ErrorCode.PLAN_FILE_INVALID
        if "active blocked operation" in message:
            return ErrorCode.STATE_RECOVERY_REQUIRED
        return ErrorCode.PLAN_STALE
    return ErrorCode.STATE_INVALID


def _handle_recovery_failure(fmt: OutputFormatter, error: Exception) -> None:
    code = _recovery_error_code(error)
    safe_message, operation_id = state_operation_error_details(error)
    data: dict[str, object] = {}
    if isinstance(error, StateBackendReleaseAfterCommitError):
        data["committed"] = error.committed
    if operation_id is not None:
        data["operation_id"] = operation_id
    if data:
        fmt.set_data(data)
    fmt.add_error(
        StructuredError(
            code=code,
            message=safe_message,
            operation_id=operation_id,
        )
    )
    fmt.print_error(safe_message)
    fmt.flush()
    raise click.exceptions.Exit(1)


@state.command("recovery-plan")
@click.option(
    "--resolution",
    type=click.Choice(_RECOVERY_RESOLUTIONS, case_sensitive=True),
    required=True,
    help="Reviewed recovery outcome to prove",
)
@click.option(
    "--out",
    "destination",
    type=click.Path(dir_okay=False, path_type=Path),
    required=True,
    help="Create a new recovery evidence file without overwriting",
)
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
def state_recovery_plan(
    ctx: click.Context,
    resolution: str,
    destination: Path,
    project_dir: str | None,
    environment: str | None,
) -> None:
    """Create strict, no-overwrite evidence for an unfinished operation."""
    from streamt.core.environment import EnvironmentError
    from streamt.core.parser import EnvVarError, ParseError
    from streamt.deployer.recovery_plan import RecoveryPlanError
    from streamt.deployer.recovery_service import RecoveryService, RecoveryServiceError

    fmt = make_formatter(ctx, "state recovery-plan")
    project_path = get_project_path(project_dir)
    try:
        state_service, runtime = _recovery_service_and_runtime(
            project_path,
            environment,
            fmt,
        )
        recovery = RecoveryService(state=state_service)
        needs_live_evidence = resolution != "abandoned_before_mutation"
        plan = recovery.create_plan(
            resolution=resolution,  # type: ignore[arg-type]
            destination=destination,
            observer=runtime if needs_live_evidence else None,
            context_reader=runtime if needs_live_evidence else None,
        )
        plan_path = str(destination.resolve())
        data: dict[str, object] = {
            "plan_file": plan_path,
            "blocked_operation_id": plan.blocked_operation_id,
            "recovery_operation_id": plan.recovery_operation_id,
            "resolution": plan.resolution,
            "evidence_checksum": plan.evidence_checksum,
        }
        fmt.set_data(data)
        fmt.print("[green]Saved reviewed recovery evidence[/green]")
        fmt.print(f"  Plan: {plan_path}")
        fmt.print(f"  Blocked operation: {plan.blocked_operation_id}")
        fmt.print(f"  Recovery operation: {plan.recovery_operation_id}")
        fmt.print(f"  Resolution: {plan.resolution}")
        fmt.print(f"  Evidence checksum: {plan.evidence_checksum}")
        fmt.flush()
    except (EnvVarError, ParseError, EnvironmentError) as error:
        safe_message = redact_sensitive_text(error)
        fmt.add_error(StructuredError(code=ErrorCode.PARSE_ERROR, message=safe_message))
        fmt.print_error(safe_message)
        fmt.flush()
        raise click.exceptions.Exit(1) from None
    except (RecoveryPlanError, RecoveryServiceError, StateError) as error:
        _handle_recovery_failure(fmt, error)


@state.command("recover")
@click.option(
    "--plan",
    "plan_path",
    type=click.Path(dir_okay=False, path_type=Path),
    required=True,
    help="Reviewed recovery evidence file",
)
@click.option(
    "--confirm-operation-id",
    help="Exact blocked operation UUID from the reviewed evidence",
)
@click.option(
    "--confirm-resolution",
    help="Exact recovery outcome from the reviewed evidence",
)
@click.option(
    "--confirm-evidence-checksum",
    help="Exact sha256 evidence checksum from the reviewed evidence",
)
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
def state_recover(
    ctx: click.Context,
    plan_path: Path,
    confirm_operation_id: str | None,
    confirm_resolution: str | None,
    confirm_evidence_checksum: str | None,
    project_dir: str | None,
    environment: str | None,
) -> None:
    """Execute an exact reviewed recovery after fresh evidence revalidation."""
    from streamt.core.environment import EnvironmentError
    from streamt.core.parser import EnvVarError, ParseError
    from streamt.deployer.recovery_plan import RecoveryPlanError, RecoveryPlanFile
    from streamt.deployer.recovery_service import RecoveryService, RecoveryServiceError

    fmt = make_formatter(ctx, "state recover")
    project_path = get_project_path(project_dir)
    try:
        operation_id, resolution, evidence_checksum = _require_recovery_confirmations(
            operation_id=confirm_operation_id,
            resolution=confirm_resolution,
            evidence_checksum=confirm_evidence_checksum,
        )
        reviewed_plan = RecoveryPlanFile.load(plan_path)
        state_service, runtime = _recovery_service_and_runtime(
            project_path,
            environment,
            fmt,
        )
        recovery = RecoveryService(state=state_service)
        needs_live_evidence = reviewed_plan.resolution != "abandoned_before_mutation"
        result = recovery.execute_plan(
            reviewed_plan,
            confirm_operation_id=operation_id,
            confirm_resolution=resolution,
            confirm_evidence_checksum=evidence_checksum,
            observer=runtime if needs_live_evidence else None,
            context_reader=runtime if needs_live_evidence else None,
        )
        changed = result.state.state != reviewed_plan.snapshot.state
        data: dict[str, object] = {
            "store": {
                "backend": result.state.store.backend,
                "store_id": result.state.store.store_id,
            },
            "address": result.address.uri,
            "state_serial": result.state.state_serial,
            "state_checksum": state_checksum(result.state.state),
            "control_status": result.control.control.status,
            "state_changed": changed,
            "blocked_operation_id": reviewed_plan.blocked_operation_id,
            "recovery_operation_id": reviewed_plan.recovery_operation_id,
            "resolution": reviewed_plan.resolution,
            "evidence_checksum": reviewed_plan.evidence_checksum,
        }
        fmt.set_data(data)
        fmt.print("[green]Deployment state recovery completed[/green]")
        fmt.print(f"  Address: {result.address.uri}")
        fmt.print(f"  State serial: {result.state.state_serial}")
        fmt.print(f"  State checksum: {data['state_checksum']}")
        fmt.print(f"  Operation: {result.control.control.status}")
        fmt.print(f"  State changed: {str(changed).lower()}")
        fmt.flush()
    except (EnvVarError, ParseError, EnvironmentError) as error:
        safe_message = redact_sensitive_text(error)
        fmt.add_error(StructuredError(code=ErrorCode.PARSE_ERROR, message=safe_message))
        fmt.print_error(safe_message)
        fmt.flush()
        raise click.exceptions.Exit(1) from None
    except (RecoveryPlanError, RecoveryServiceError, StateError) as error:
        _handle_recovery_failure(fmt, error)
