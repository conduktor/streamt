"""Fail-closed ownership adoption for existing Kafka topics."""

from __future__ import annotations

import json
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import click

from streamt.cli.helpers import (
    close_deployers,
    get_project_path,
    handle_parse_error,
    make_formatter,
    make_kafka_deployer,
    redact_sensitive_text,
)
from streamt.compiler.manifest import ArtifactOwnership, Manifest, TopicArtifact
from streamt.core.errors import ErrorCode
from streamt.deployer.kafka import TopicState
from streamt.deployer.state import (
    LOCAL_STATE_CI_WARNING,
    LocalState,
    ManagedResourceRecord,
    StateConflictError,
    StateError,
    artifact_checksum,
    load_local_state,
    local_state_path,
    resource_id,
)
from streamt.output import StructuredError

_SENSITIVE_KEY = re.compile(
    r"(^|[._-])(?:password|passwd|secret|token|api[_-]?key|authorization|credentials?"
    r"|basic[._-]auth[._-]user[._-]info|sasl[._-]jaas[._-]config)($|[._-])",
    re.IGNORECASE,
)
@dataclass(frozen=True)
class AdoptionError(ValueError):
    """A fail-closed adoption precondition was not met."""

    code: str
    message: str

    def __str__(self) -> str:
        return self.message


def _redact(value: object) -> object:
    """Remove credential-shaped values from adoption output."""
    if isinstance(value, dict):
        return {
            str(key): "<redacted>" if _SENSITIVE_KEY.search(str(key)) else _redact(item)
            for key, item in value.items()
        }
    if isinstance(value, (list, tuple)):
        return [_redact(item) for item in value]
    if isinstance(value, str):
        return redact_sensitive_text(value)
    return value


def _resolve_topic_artifact(
    manifest: Manifest,
    *,
    project: str,
    logical_name: str,
) -> TopicArtifact:
    """Resolve exactly one explicitly adopted topic by stable logical owner."""
    matches: list[TopicArtifact] = []
    for raw_artifact in manifest.artifacts.get("topics", []):
        ownership = ArtifactOwnership.from_dict(raw_artifact.get("ownership"))
        if ownership is None or ownership.owner_name != logical_name:
            continue
        physical_name = raw_artifact.get("name")
        partitions = raw_artifact.get("partitions")
        replication_factor = raw_artifact.get("replication_factor")
        config = raw_artifact.get("config", {})
        if (
            not isinstance(physical_name, str)
            or type(partitions) is not int
            or type(replication_factor) is not int
            or not isinstance(config, dict)
        ):
            raise AdoptionError(
                ErrorCode.ADOPTION_TARGET_INVALID,
                f"Compiled topic for logical name {logical_name!r} is malformed",
            )
        matches.append(
            TopicArtifact(
                name=physical_name,
                partitions=partitions,
                replication_factor=replication_factor,
                config=config,
                ownership=ownership,
            )
        )

    if not matches:
        raise AdoptionError(
            ErrorCode.ADOPTION_TARGET_INVALID,
            f"No compiled topic is owned by logical name {logical_name!r}",
        )
    if len(matches) != 1:
        physical_names = sorted(artifact.name for artifact in matches)
        raise AdoptionError(
            ErrorCode.ADOPTION_TARGET_INVALID,
            f"Logical name {logical_name!r} resolves to multiple topics: "
            f"{', '.join(physical_names)}",
        )

    artifact = matches[0]
    ownership = ArtifactOwnership.from_dict(artifact.ownership)
    if ownership is None or ownership.project != project:
        raise AdoptionError(
            ErrorCode.ADOPTION_TARGET_INVALID,
            "Compiled topic has no matching stable project ownership metadata",
        )
    if ownership.mode != "adopted":
        raise AdoptionError(
            ErrorCode.ADOPTION_TARGET_INVALID,
            f"Topic {artifact.name!r} declares ownership.mode {ownership.mode!r}; "
            "set it explicitly to 'adopted' before claiming an existing topic",
        )
    return artifact


def _pending_topic_diffs(
    current: TopicState,
    desired: TopicArtifact,
) -> dict[str, object]:
    """Return the same topic attributes a later plan may manage."""
    changes: dict[str, object] = {}
    if current.partitions != desired.partitions:
        if current.partitions is not None and desired.partitions < current.partitions:
            changes["partitions_error"] = {
                "current": current.partitions,
                "desired": desired.partitions,
                "message": "Kafka topic partitions cannot be reduced",
            }
        else:
            changes["partitions"] = {
                "from": current.partitions,
                "to": desired.partitions,
            }

    current_config = current.config or {}
    for key, desired_value in desired.config.items():
        current_value = current_config.get(key)
        if current_value is None or str(current_value).lower() != str(desired_value).lower():
            changes[f"config.{key}"] = {
                "from": current_value,
                "to": desired_value,
            }
    for key, current_value in current_config.items():
        if key not in desired.config:
            changes[f"config.{key}"] = {
                "from": current_value,
                "to": None,
            }
    return changes


def _records_match_for_idempotency(
    existing: ManagedResourceRecord,
    desired: ManagedResourceRecord,
) -> bool:
    """Treat an otherwise identical managed/adopted claim as already owned."""
    return (
        existing.physical_name == desired.physical_name
        and existing.artifact_checksum == desired.artifact_checksum
        and existing.backend == desired.backend
        and existing.ownership in ("managed", "adopted")
    )


def _confirmation_token(resource_uri: str, environment: str) -> str:
    return f"adopt {resource_uri} in {environment}"


def _stdin_is_interactive() -> bool:
    """Return whether an exact-token prompt is safe to display."""
    return sys.stdin.isatty()


def _require_confirmation(
    *,
    resource_uri: str,
    environment: str,
    confirm_resource: str | None,
    confirm_environment: str | None,
    allow_prompt: bool,
) -> None:
    """Require an exact resource and environment confirmation before writing."""
    if confirm_resource is not None or confirm_environment is not None:
        if confirm_resource != resource_uri or confirm_environment != environment:
            raise AdoptionError(
                ErrorCode.ADOPTION_CONFIRMATION_REQUIRED,
                "Adoption confirmation must include both the exact resource ID and "
                "effective environment",
            )
        return

    if not allow_prompt:
        raise AdoptionError(
            ErrorCode.ADOPTION_CONFIRMATION_REQUIRED,
            "Non-interactive adoption requires both "
            f"--confirm-resource {resource_uri} and --confirm-env {environment}",
        )

    token = _confirmation_token(resource_uri, environment)
    try:
        entered = click.prompt(
            f"Type {token!r} to confirm",
            default="",
            show_default=False,
        )
    except click.Abort as exc:
        raise AdoptionError(
            ErrorCode.ADOPTION_CONFIRMATION_REQUIRED,
            "Adoption aborted before confirmation",
        ) from exc
    if entered != token:
        raise AdoptionError(
            ErrorCode.ADOPTION_CONFIRMATION_REQUIRED,
            "Adoption confirmation did not match the resource and environment",
        )


def _adoption_data(
    *,
    resource_uri: str,
    logical_name: str,
    artifact: TopicArtifact,
    current: TopicState,
    state_path: Path,
    state_serial: int,
) -> dict[str, object]:
    return {
        "resource_id": resource_uri,
        "kind": "topic",
        "logical_name": logical_name,
        "physical_name": artifact.name,
        "observed": _redact(
            {
                "partitions": current.partitions,
                "replication_factor": current.replication_factor,
                "config": current.config or {},
            }
        ),
        "desired_managed_attributes": _redact(
            {
                "partitions": artifact.partitions,
                "config": artifact.config,
            }
        ),
        "desired_replication_factor": artifact.replication_factor,
        "pending_diffs": _redact(_pending_topic_diffs(current, artifact)),
        "state_file": str(state_path),
        "state_serial": state_serial,
    }


@click.command()
@click.option(
    "--project-dir",
    "-p",
    type=click.Path(exists=True, file_okay=False),
    required=True,
    help="Path to project directory",
)
@click.option(
    "--env",
    "-e",
    "environment",
    required=True,
    help="Target environment",
)
@click.option(
    "--kind",
    type=click.Choice(["topic"]),
    required=True,
    help="Runtime resource kind (topic only in this MVP)",
)
@click.option(
    "--name",
    "logical_name",
    required=True,
    help="Stable logical owner name from the compiled artifact",
)
@click.option(
    "--confirm-resource",
    help="Non-interactive confirmation using the full streamt resource ID",
)
@click.option(
    "--confirm-env",
    "confirm_environment",
    help="Non-interactive confirmation using the effective environment",
)
@click.pass_context
def adopt(
    ctx: click.Context,
    project_dir: str,
    environment: str,
    kind: str,
    logical_name: str,
    confirm_resource: Optional[str],
    confirm_environment: Optional[str],
) -> None:
    """Claim one declared, existing Kafka topic without mutating Kafka."""
    from streamt.compiler import Compiler
    from streamt.core.environment import EnvironmentError
    from streamt.core.parser import EnvVarError, ParseError, ProjectParser
    from streamt.core.validator import ProjectValidator

    fmt = make_formatter(ctx, "adopt")
    project_path = get_project_path(project_dir)
    kafka = None
    data: dict[str, object] = {}

    try:
        parser_environment = (
            None
            if environment == "default" and not (project_path / "environments").is_dir()
            else environment
        )
        parser = ProjectParser(
            project_path,
            environment=parser_environment,
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
        if environment != effective_environment:
            raise AdoptionError(
                ErrorCode.ADOPTION_TARGET_INVALID,
                f"Requested environment {environment!r} resolves to "
                f"{effective_environment!r}; use the effective environment explicitly",
            )

        validation = ProjectValidator(project).validate()
        if not validation.is_valid:
            message = "; ".join(error.message for error in validation.errors)
            raise AdoptionError(ErrorCode.ADOPTION_TARGET_INVALID, message)

        manifest = Compiler(project).compile(dry_run=True)
        artifact = _resolve_topic_artifact(
            manifest,
            project=project.project.name,
            logical_name=logical_name,
        )
        resource_uri = resource_id(
            project.project.name,
            effective_environment,
            kind,
            logical_name,
        )
        state_path = local_state_path(
            project_path,
            environment=effective_environment,
        )
        prior_state = load_local_state(
            project_path,
            project=project.project.name,
            environment=effective_environment,
        )
        fmt.print_warning(
            f"{LOCAL_STATE_CI_WARNING} State file: {state_path}",
            code=ErrorCode.LOCAL_STATE_ONLY,
        )

        desired_record = ManagedResourceRecord(
            physical_name=artifact.name,
            ownership="adopted",
            artifact_checksum=artifact_checksum(artifact.to_dict()),
            backend="direct-kafka",
        )

        existing = prior_state.resources.get(resource_uri)
        if existing is not None and not _records_match_for_idempotency(
            existing,
            desired_record,
        ):
            raise AdoptionError(
                ErrorCode.ADOPTION_STATE_CONFLICT,
                f"State already contains a different ownership record for {resource_uri}",
            )
        for other_uri, other_record in prior_state.resources.items():
            if (
                other_uri != resource_uri
                and other_record.backend == desired_record.backend
                and other_record.physical_name == desired_record.physical_name
            ):
                raise AdoptionError(
                    ErrorCode.ADOPTION_STATE_CONFLICT,
                    f"Physical topic {artifact.name!r} is already claimed by {other_uri}",
                )

        kafka = make_kafka_deployer(project, fmt)
        if kafka is None:
            raise AdoptionError(
                ErrorCode.ADOPTION_FAILED,
                "Kafka is not configured or reachable; adoption requires live observation",
            )
        try:
            current = kafka.get_topic_state(artifact.name, strict_config=True)
        except Exception as exc:
            raise AdoptionError(
                ErrorCode.ADOPTION_FAILED,
                f"Could not observe topic {artifact.name!r}: {_redact(str(exc))}",
            ) from exc
        if not current.exists:
            raise AdoptionError(
                ErrorCode.ADOPTION_LIVE_NOT_FOUND,
                f"Live topic {artifact.name!r} does not exist; adoption never creates it",
            )

        data = _adoption_data(
            resource_uri=resource_uri,
            logical_name=logical_name,
            artifact=artifact,
            current=current,
            state_path=state_path,
            state_serial=prior_state.serial,
        )
        fmt.set_data(data)
        fmt.print(f"[cyan]Adoption candidate:[/cyan] {resource_uri}")
        fmt.print(f"  Physical topic: {artifact.name}")
        fmt.print(f"  Observed: {json.dumps(data['observed'], sort_keys=True)}")
        fmt.print(
            "  Desired managed attributes: "
            f"{json.dumps(data['desired_managed_attributes'], sort_keys=True)}"
        )
        fmt.print(f"  Pending diffs: {json.dumps(data['pending_diffs'], sort_keys=True)}")

        if existing is not None:
            data["adopted"] = False
            data["already_owned"] = True
            fmt.print("[green]Topic already has an identical ownership record.[/green]")
            fmt.flush()
            return

        _require_confirmation(
            resource_uri=resource_uri,
            environment=effective_environment,
            confirm_resource=confirm_resource,
            confirm_environment=confirm_environment,
            allow_prompt=fmt.format == "text" and _stdin_is_interactive(),
        )

        resources = dict(prior_state.resources)
        resources[resource_uri] = desired_record
        next_state = LocalState(
            project=prior_state.project,
            environment=prior_state.environment,
            serial=prior_state.serial + 1,
            resources=resources,
        )
        try:
            next_state.save_if_serial(
                state_path,
                expected_serial=prior_state.serial,
            )
        except StateConflictError as exc:
            raise AdoptionError(
                ErrorCode.ADOPTION_STATE_CONFLICT,
                str(exc),
            ) from exc
        except OSError as exc:
            raise AdoptionError(
                ErrorCode.ADOPTION_FAILED,
                f"Could not atomically save adoption state: {_redact(str(exc))}",
            ) from exc

        data["adopted"] = True
        data["already_owned"] = False
        data["state_serial"] = next_state.serial
        next_command = [
            "streamt",
            "plan",
            "--project-dir",
            str(project_path),
        ]
        if parser_environment is not None:
            next_command.extend(["--env", effective_environment])
        reviewed_plan_path = (
            project_path
            / ".streamt"
            / "plans"
            / f"{effective_environment}-reviewed-plan.json"
        )
        next_command.extend(["--out", str(reviewed_plan_path)])
        data["next_command"] = next_command
        fmt.set_data(data)
        fmt.print("[green]Ownership adopted. Kafka was not modified.[/green]")
        fmt.print("Run a fresh reviewed plan before any apply.")
        fmt.flush()

    except (EnvVarError, ParseError, EnvironmentError) as exc:
        handle_parse_error(fmt, exc, ErrorCode.PARSE_ERROR)
    except AdoptionError as exc:
        if data:
            fmt.set_data(data)
        fmt.add_error(StructuredError(code=exc.code, message=exc.message))
        fmt.print_error(exc.message)
        fmt.flush()
        raise click.exceptions.Exit(1) from exc
    except StateError as exc:
        fmt.add_error(StructuredError(code=ErrorCode.STATE_INVALID, message=str(exc)))
        fmt.print_error(str(exc))
        fmt.flush()
        raise click.exceptions.Exit(1) from exc
    except KeyboardInterrupt as exc:
        fmt.print_error("Interrupted.")
        fmt.flush()
        raise click.exceptions.Exit(130) from exc
    except Exception as exc:
        message = f"Adoption failed unexpectedly: {redact_sensitive_text(exc)}"
        fmt.add_error(
            StructuredError(code=ErrorCode.ADOPTION_FAILED, message=message)
        )
        fmt.print_error(message)
        fmt.flush()
        raise click.exceptions.Exit(1) from exc
    finally:
        close_deployers(kafka)
