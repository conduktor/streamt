"""Fail-closed ownership adoption for existing runtime resources."""

from __future__ import annotations

import json
import re
import sys
import uuid
from contextlib import ExitStack
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import click

from streamt.cli.helpers import (
    close_deployers,
    get_project_path,
    handle_parse_error,
    make_connect_deployer,
    make_formatter,
    make_gateway_deployer,
    make_kafka_deployer,
    make_sr_deployer,
    redact_sensitive_text,
    state_operation_error_details,
)
from streamt.compiler.connector_artifact import parse_compiled_connector_artifact
from streamt.compiler.manifest import (
    ArtifactOwnership,
    ConnectorArtifact,
    ConnectorArtifactFormatError,
    Manifest,
    SchemaArtifact,
    TopicArtifact,
)
from streamt.core.deployment_state import (
    RemoteStateRequiredError,
    enforce_remote_state_policy,
)
from streamt.core.errors import ErrorCode
from streamt.core.models import StreamtProject
from streamt.deployer.connect import (
    ConnectClusterBinding,
    ConnectClusterBindingError,
    ConnectDeployer,
    ConnectManagedObservationError,
    ManagedConnectorObservation,
    bind_connector_artifact,
    secret_neutral_connector_config_diff,
)
from streamt.deployer.gateway import GatewayDeployer, ManagedGatewayRuleObservation
from streamt.deployer.gateway_adoption import (
    GatewayAdoptionDriftError,
    GatewayAdoptionError,
    GatewayAdoptionLiveNotFoundError,
    GatewayAdoptionTarget,
    build_gateway_adoption_action_evidence,
    build_gateway_adoption_review,
    require_unchanged_gateway_adoption_observation,
    resolve_gateway_adoption_target,
)
from streamt.deployer.kafka import TopicState
from streamt.deployer.planner import resolve_gateway_planning_targets
from streamt.deployer.schema_registry import SchemaReference, SchemaState
from streamt.deployer.state import (
    LOCAL_STATE_CI_WARNING,
    LocalState,
    ManagedResourceRecord,
    ResourceIdentity,
    StateConflictError,
    StateError,
    artifact_checksum,
    local_state_path,
    resource_id,
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

_SENSITIVE_KEY = re.compile(
    r"(^|[._-])(?:password|passwd|secret|token|api[_-]?key|authorization|credentials?"
    r"|basic[._-]auth[._-]user[._-]info|sasl[._-]jaas[._-]config)($|[._-])",
    re.IGNORECASE,
)
_SCHEMA_TYPES = frozenset({"AVRO", "JSON", "PROTOBUF"})
_COMPATIBILITY_LEVELS = frozenset(
    {
        "NONE",
        "BACKWARD",
        "BACKWARD_TRANSITIVE",
        "FORWARD",
        "FORWARD_TRANSITIVE",
        "FULL",
        "FULL_TRANSITIVE",
    }
)


def _resolve_provider_free_gateway_adoption_target(
    manifest: Manifest,
    project: StreamtProject,
    *,
    environment: str,
    logical_name: str,
) -> GatewayAdoptionTarget:
    """Resolve one exact Gateway target before state or provider construction."""
    try:
        targets = resolve_gateway_planning_targets(
            manifest,
            project,
            environment=environment,
            prior_state=None,
            require_authoritative_state=False,
        )
        matches = tuple(
            rule for rule in targets.desired_rules if rule.logical_owner == logical_name
        )
        if len(matches) != 1:
            raise GatewayAdoptionError(
                "Gateway adoption requires exactly one compiled rule for the logical owner"
            )
        rule = matches[0]
        return GatewayAdoptionTarget(
            resource_id=resource_id(
                manifest.project_name,
                environment,
                "gateway_rule",
                logical_name,
            ),
            logical_owner=logical_name,
            binding=targets.binding,
            artifact=rule.artifact,
            desired=rule.desired,
            desired_artifact_checksum=artifact_checksum(rule.artifact.to_dict()),
            existing_record=None,
        )
    except (GatewayAdoptionError, StateError, TypeError, ValueError):
        raise AdoptionError(
            ErrorCode.ADOPTION_TARGET_INVALID,
            "Gateway adoption target preflight rejected the manifest or runtime",
        ) from None


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


def _resolve_schema_artifact(
    manifest: Manifest,
    *,
    project: str,
    logical_name: str,
) -> SchemaArtifact:
    """Resolve exactly one explicitly adopted schema by stable logical owner."""
    matches: list[SchemaArtifact] = []
    for raw_artifact in manifest.artifacts.get("schemas", []):
        if not isinstance(raw_artifact, dict):
            raise AdoptionError(
                ErrorCode.ADOPTION_TARGET_INVALID,
                "Compiled schema artifact is malformed",
            )
        ownership = ArtifactOwnership.from_dict(raw_artifact.get("ownership"))
        if ownership is None or ownership.owner_name != logical_name:
            continue
        subject = raw_artifact.get("subject")
        schema = raw_artifact.get("schema")
        schema_type = raw_artifact.get("schema_type", "AVRO")
        compatibility = raw_artifact.get("compatibility")
        if (
            not isinstance(subject, str)
            or not subject
            or not isinstance(schema, dict)
            or not isinstance(schema_type, str)
            or not schema_type
            or (compatibility is not None and not isinstance(compatibility, str))
        ):
            raise AdoptionError(
                ErrorCode.ADOPTION_TARGET_INVALID,
                f"Compiled schema for logical name {logical_name!r} is malformed",
            )
        normalized_type = schema_type.upper()
        if normalized_type not in _SCHEMA_TYPES:
            raise AdoptionError(
                ErrorCode.ADOPTION_TARGET_INVALID,
                f"Compiled schema for logical name {logical_name!r} has unsupported "
                f"schema type {normalized_type!r}",
            )
        if compatibility is not None and compatibility.upper() not in _COMPATIBILITY_LEVELS:
            raise AdoptionError(
                ErrorCode.ADOPTION_TARGET_INVALID,
                f"Compiled schema for logical name {logical_name!r} has unsupported compatibility",
            )
        candidate = SchemaArtifact(
            subject=subject,
            schema=schema,
            schema_type=schema_type,
            compatibility=compatibility,
            ownership=ownership,
        )
        try:
            artifact_checksum(candidate.to_dict())
        except StateError as exc:
            raise AdoptionError(
                ErrorCode.ADOPTION_TARGET_INVALID,
                f"Compiled schema for logical name {logical_name!r} is malformed",
            ) from exc
        matches.append(candidate)

    if not matches:
        raise AdoptionError(
            ErrorCode.ADOPTION_TARGET_INVALID,
            f"No compiled schema is owned by logical name {logical_name!r}",
        )
    if len(matches) != 1:
        subjects = sorted(artifact.subject for artifact in matches)
        raise AdoptionError(
            ErrorCode.ADOPTION_TARGET_INVALID,
            f"Logical name {logical_name!r} resolves to multiple schema subjects: "
            f"{', '.join(subjects)}",
        )

    artifact = matches[0]
    ownership = ArtifactOwnership.from_dict(artifact.ownership)
    if ownership is None or ownership.project != project:
        raise AdoptionError(
            ErrorCode.ADOPTION_TARGET_INVALID,
            "Compiled schema has no matching stable project ownership metadata",
        )
    if ownership.mode != "adopted":
        raise AdoptionError(
            ErrorCode.ADOPTION_TARGET_INVALID,
            f"Schema {artifact.subject!r} declares ownership.mode {ownership.mode!r}; "
            "set it explicitly to 'adopted' before claiming an existing subject",
        )
    return artifact


def _resolve_connector_artifact(
    manifest: Manifest,
    *,
    project: str,
    logical_name: str,
    binding: ConnectClusterBinding,
) -> ConnectorArtifact:
    """Parse, bind, and resolve exactly one explicitly adopted connector."""
    resolved: list[ConnectorArtifact] = []
    provider_names: set[str] = set()
    logical_owners: set[str] = set()
    try:
        for raw_artifact in manifest.artifacts.get("connectors", []):
            parsed = parse_compiled_connector_artifact(raw_artifact)
            artifact = bind_connector_artifact(parsed, binding)
            if artifact.name in provider_names:
                raise AdoptionError(
                    ErrorCode.ADOPTION_TARGET_INVALID,
                    "Compiled connector artifacts contain a provider identity collision",
                )
            provider_names.add(artifact.name)
            ownership = ArtifactOwnership.from_dict(artifact.ownership)
            logical_owner = ownership.owner_name if ownership is not None else artifact.name
            if logical_owner in logical_owners:
                raise AdoptionError(
                    ErrorCode.ADOPTION_TARGET_INVALID,
                    "Compiled connector artifacts contain a logical ownership collision",
                )
            logical_owners.add(logical_owner)
            resolved.append(artifact)
    except AdoptionError:
        raise
    except (ConnectorArtifactFormatError, ConnectClusterBindingError, TypeError, ValueError):
        raise AdoptionError(
            ErrorCode.ADOPTION_TARGET_INVALID,
            "Compiled connector artifacts are malformed or target a non-default cluster",
        ) from None

    matches = []
    for artifact in resolved:
        ownership = ArtifactOwnership.from_dict(artifact.ownership)
        if ownership is not None and ownership.owner_name == logical_name:
            matches.append(artifact)
    if not matches:
        raise AdoptionError(
            ErrorCode.ADOPTION_TARGET_INVALID,
            f"No compiled connector is owned by logical name {logical_name!r}",
        )
    if len(matches) != 1:
        # The global logical-owner collision check should make this unreachable,
        # but retain a local fail-closed assertion at the adoption boundary.
        raise AdoptionError(
            ErrorCode.ADOPTION_TARGET_INVALID,
            f"Logical name {logical_name!r} resolves to multiple connectors",
        )

    artifact = matches[0]
    ownership = ArtifactOwnership.from_dict(artifact.ownership)
    if ownership is None or ownership.project != project:
        raise AdoptionError(
            ErrorCode.ADOPTION_TARGET_INVALID,
            "Compiled connector has no matching stable project ownership metadata",
        )
    if ownership.mode != "adopted":
        raise AdoptionError(
            ErrorCode.ADOPTION_TARGET_INVALID,
            "Compiled connector ownership mode must be explicitly 'adopted'",
        )
    return artifact


def _connect_binding_from_project(project: object) -> ConnectClusterBinding:
    """Resolve the effective default Connect binding without exposing its endpoint."""
    runtime = getattr(project, "runtime", None)
    connect = getattr(runtime, "connect", None)
    default = getattr(connect, "default", None)
    clusters = getattr(connect, "clusters", None)
    if (
        not isinstance(default, str)
        or not default
        or not isinstance(clusters, dict)
        or default not in clusters
    ):
        raise AdoptionError(
            ErrorCode.ADOPTION_TARGET_INVALID,
            "Connector adoption requires an effective default Connect cluster",
        )
    cluster = clusters[default]
    rest_url = getattr(cluster, "rest_url", None)
    if not isinstance(rest_url, str):
        raise AdoptionError(
            ErrorCode.ADOPTION_TARGET_INVALID,
            "Connector adoption requires a valid default Connect cluster",
        )
    try:
        return ConnectClusterBinding.from_endpoint(default, rest_url)
    except ConnectClusterBindingError:
        raise AdoptionError(
            ErrorCode.ADOPTION_TARGET_INVALID,
            "Connector adoption requires a valid default Connect cluster binding",
        ) from None


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


def _validate_live_topic_state(current: TopicState, *, topic: str) -> None:
    """Reject absence or an observation for a different Kafka target."""
    if current.name != topic:
        raise AdoptionError(
            ErrorCode.ADOPTION_FAILED,
            "Kafka returned state for a different topic",
        )
    if not current.exists:
        raise AdoptionError(
            ErrorCode.ADOPTION_LIVE_NOT_FOUND,
            f"Live topic {topic!r} does not exist; adoption never creates it",
        )


def _validate_live_schema_state(current: SchemaState, *, subject: str) -> None:
    """Reject incomplete or inconsistent Schema Registry observations."""
    if current.subject != subject:
        raise AdoptionError(
            ErrorCode.ADOPTION_FAILED,
            "Schema Registry returned state for a different subject",
        )
    if not current.exists:
        raise AdoptionError(
            ErrorCode.ADOPTION_LIVE_NOT_FOUND,
            f"Live schema subject {subject!r} does not exist; adoption never creates it",
        )
    if type(current.version) is not int or current.version < 1:
        raise AdoptionError(
            ErrorCode.ADOPTION_FAILED,
            f"Schema Registry returned no positive version for subject {subject!r}",
        )
    if type(current.schema_id) is not int or current.schema_id < 1:
        raise AdoptionError(
            ErrorCode.ADOPTION_FAILED,
            f"Schema Registry returned no valid schema ID for subject {subject!r}",
        )
    if current.schema is None:
        raise AdoptionError(
            ErrorCode.ADOPTION_FAILED,
            f"Schema Registry returned no schema content for subject {subject!r}",
        )
    if not isinstance(current.schema_type, str) or current.schema_type.upper() not in _SCHEMA_TYPES:
        raise AdoptionError(
            ErrorCode.ADOPTION_FAILED,
            f"Schema Registry returned an unsupported schema type for subject {subject!r}",
        )
    if (
        not isinstance(current.compatibility, str)
        or current.compatibility.upper() not in _COMPATIBILITY_LEVELS
    ):
        raise AdoptionError(
            ErrorCode.ADOPTION_FAILED,
            f"Schema Registry returned malformed compatibility for subject {subject!r}",
        )
    if not isinstance(current.references, list) or any(
        not isinstance(reference, SchemaReference)
        or not isinstance(reference.name, str)
        or not reference.name
        or not isinstance(reference.subject, str)
        or not reference.subject
        or type(reference.version) is not int
        or reference.version < 1
        for reference in current.references
    ):
        raise AdoptionError(
            ErrorCode.ADOPTION_FAILED,
            f"Schema Registry returned malformed references for subject {subject!r}",
        )

    try:
        _schema_content_checksum(
            schema=current.schema,
            schema_type=current.schema_type,
        )
    except StateError as exc:
        raise AdoptionError(
            ErrorCode.ADOPTION_FAILED,
            f"Schema Registry returned malformed schema content for subject {subject!r}",
        ) from exc


def _validate_live_connector_observation(
    current: object,
    *,
    artifact: ConnectorArtifact,
    binding: ConnectClusterBinding,
) -> ManagedConnectorObservation:
    """Require exact strict evidence for the configured connector binding."""
    if not isinstance(current, ManagedConnectorObservation):
        raise AdoptionError(
            ErrorCode.ADOPTION_FAILED,
            "Kafka Connect returned a partial managed observation",
        )
    if current.binding != binding:
        raise AdoptionError(
            ErrorCode.ADOPTION_FAILED,
            "Kafka Connect returned an observation for a different cluster binding",
        )
    if current.name != artifact.name:
        raise AdoptionError(
            ErrorCode.ADOPTION_FAILED,
            "Kafka Connect returned an observation for a different connector",
        )
    if not current.exists:
        raise AdoptionError(
            ErrorCode.ADOPTION_LIVE_NOT_FOUND,
            "The live connector does not exist; adoption never creates it",
        )
    return current


def _connector_config_checksum(config: object) -> str:
    """Hash one strict connector configuration without serializing its values."""
    if not isinstance(config, dict):
        raise AdoptionError(
            ErrorCode.ADOPTION_FAILED,
            "Connector config evidence is incomplete",
        )
    try:
        return artifact_checksum(config)
    except StateError as exc:
        raise AdoptionError(
            ErrorCode.ADOPTION_FAILED,
            "Connector config evidence is malformed",
        ) from exc


def _schema_content_checksum(
    *,
    schema: object,
    schema_type: str,
) -> str:
    """Hash schema-managed attributes without exposing schema content."""
    return str(
        artifact_checksum(
            {
                "schema": schema,
                "schema_type": schema_type.upper(),
            }
        )
    )


def _pending_schema_diffs(
    current: SchemaState,
    desired: SchemaArtifact,
) -> dict[str, object]:
    """Describe deterministic pending schema changes without schema content."""
    current_checksum = _schema_content_checksum(
        schema=current.schema,
        schema_type=current.schema_type or "",
    )
    desired_checksum = _schema_content_checksum(
        schema=desired.schema,
        schema_type=desired.schema_type,
    )
    changes: dict[str, object] = {}
    if current.schema_type and current.schema_type.upper() != desired.schema_type.upper():
        changes["schema_type"] = {
            "from": current.schema_type.upper(),
            "to": desired.schema_type.upper(),
        }
    if current_checksum != desired_checksum:
        changes["schema_checksum"] = {
            "from": current_checksum,
            "to": desired_checksum,
        }
    current_compatibility = current.compatibility.upper() if current.compatibility else None
    desired_compatibility = desired.compatibility.upper() if desired.compatibility else None
    if (
        desired_compatibility is not None
        and current_compatibility != desired_compatibility
    ):
        changes["compatibility"] = {
            "from": current_compatibility,
            "to": desired_compatibility,
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


def _topic_observation_fingerprint(current: TopicState) -> str:
    """Return an exact, non-revealing fingerprint of one Kafka observation."""
    try:
        return artifact_checksum(
            {
                "name": current.name,
                "exists": current.exists,
                "partitions": current.partitions,
                "replication_factor": current.replication_factor,
                "config": current.config,
            }
        )
    except StateError as exc:
        raise AdoptionError(
            ErrorCode.ADOPTION_FAILED,
            "Kafka returned malformed topic state that cannot be confirmed safely",
        ) from exc


def _schema_observation_fingerprint(current: SchemaState) -> str:
    """Return an exact, non-revealing fingerprint of one registry observation."""
    try:
        return artifact_checksum(
            {
                "subject": current.subject,
                "exists": current.exists,
                "version": current.version,
                "schema_id": current.schema_id,
                "schema": current.schema,
                "schema_type": current.schema_type,
                "compatibility": current.compatibility,
                "references": [
                    {
                        "name": reference.name,
                        "subject": reference.subject,
                        "version": reference.version,
                    }
                    for reference in current.references
                ],
            }
        )
    except (StateError, TypeError, AttributeError) as exc:
        raise AdoptionError(
            ErrorCode.ADOPTION_FAILED,
            "Schema Registry returned malformed state that cannot be confirmed safely",
        ) from exc


def _require_unchanged_observation(
    *,
    kind: str,
    confirmed_fingerprint: str,
    current_fingerprint: str,
) -> None:
    """Fail closed when the live evidence changed after confirmation."""
    if current_fingerprint != confirmed_fingerprint:
        raise AdoptionError(
            ErrorCode.ADOPTION_CONFIRMATION_REQUIRED,
            f"Live {kind} changed after confirmation; rerun adoption and confirm "
            "the fresh observation",
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
    state_path: Path | None,
    state_serial: int,
) -> dict[str, object]:
    data: dict[str, object] = {
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
        "state_serial": state_serial,
    }
    if state_path is not None:
        data["state_file"] = str(state_path)
    return data


def _schema_adoption_data(
    *,
    resource_uri: str,
    logical_name: str,
    artifact: SchemaArtifact,
    current: SchemaState,
    state_path: Path | None,
    state_serial: int,
) -> dict[str, object]:
    current_checksum = _schema_content_checksum(
        schema=current.schema,
        schema_type=current.schema_type or "",
    )
    desired_checksum = _schema_content_checksum(
        schema=artifact.schema,
        schema_type=artifact.schema_type,
    )
    data: dict[str, object] = {
        "resource_id": resource_uri,
        "kind": "schema",
        "logical_name": logical_name,
        "physical_name": _redact(artifact.subject),
        "observed": _redact(
            {
                "subject": current.subject,
                "schema_type": (current.schema_type or "").upper(),
                "version": current.version,
                "schema_id": current.schema_id,
                "compatibility": current.compatibility.upper() if current.compatibility else None,
                "schema_checksum": current_checksum,
            }
        ),
        "desired_managed_attributes": _redact(
            {
                "subject": artifact.subject,
                "schema_type": artifact.schema_type.upper(),
                "compatibility": artifact.compatibility.upper() if artifact.compatibility else None,
                "schema_checksum": desired_checksum,
                "artifact_checksum": artifact_checksum(artifact.to_dict()),
            }
        ),
        "pending_diffs": _pending_schema_diffs(current, artifact),
        "state_serial": state_serial,
    }
    if state_path is not None:
        data["state_file"] = str(state_path)
    return data


def _connector_adoption_data(
    *,
    resource_uri: str,
    artifact: ConnectorArtifact,
    current: ManagedConnectorObservation,
    binding: ConnectClusterBinding,
    state_path: Path | None,
    state_serial: int,
) -> dict[str, object]:
    """Build review evidence without carrying any raw connector config value."""
    serialized_artifact = artifact.to_dict()
    desired_config = serialized_artifact.get("config")
    if not isinstance(desired_config, dict):
        raise AdoptionError(
            ErrorCode.ADOPTION_TARGET_INVALID,
            "Compiled connector config evidence is incomplete",
        )
    current_config = current.config_dict()
    pending_diffs = secret_neutral_connector_config_diff(
        current_config,
        desired_config,
    )
    data: dict[str, object] = {
        "resource_id": resource_uri,
        "kind": "connector",
        "physical_name": artifact.name,
        "cluster_alias": binding.cluster_alias,
        "endpoint_fingerprint": binding.endpoint_fingerprint,
        "observed": {
            "name": current.name,
            "config_checksum": _connector_config_checksum(current_config),
        },
        "desired_managed_attributes": {
            "name": artifact.name,
            "cluster_alias": binding.cluster_alias,
            "config_checksum": _connector_config_checksum(desired_config),
            "artifact_checksum": artifact_checksum(serialized_artifact),
        },
        "changed_keys": sorted(pending_diffs),
        "has_pending_changes": bool(pending_diffs),
        "pending_diffs": pending_diffs,
        "state_serial": state_serial,
    }
    if state_path is not None:
        data["state_file"] = str(state_path)
    return data


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
    type=click.Choice(["topic", "schema", "connector", "gateway_rule"]),
    required=True,
    help="Runtime resource kind",
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
    """Claim one declared, existing resource without mutating its backend."""
    from streamt.compiler import Compiler
    from streamt.core.environment import EnvironmentError
    from streamt.core.parser import EnvVarError, ParseError, ProjectParser
    from streamt.core.validator import ProjectValidator

    fmt = make_formatter(ctx, "adopt")
    project_path = get_project_path(project_dir)
    kafka = None
    schema_registry = None
    connect: ConnectDeployer | None = None
    gateway: GatewayDeployer | None = None
    data: dict[str, object] = {}
    operation_stack = ExitStack()

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
        parsed_environment = parser.env_config.environment.name if parser.env_config else None
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

        enforce_remote_state_policy(
            project.deployment_state,
            required=bool(parser.env_config and parser.env_config.requires_remote_state),
        )

        validation = ProjectValidator(project).validate()
        if not validation.is_valid:
            message = "; ".join(error.message for error in validation.errors)
            raise AdoptionError(ErrorCode.ADOPTION_TARGET_INVALID, message)

        manifest = Compiler(project).compile(dry_run=True)
        topic_artifact: TopicArtifact | None = None
        schema_artifact: SchemaArtifact | None = None
        connector_artifact: ConnectorArtifact | None = None
        connector_binding: ConnectClusterBinding | None = None
        gateway_target: GatewayAdoptionTarget | None = None
        current_gateway: ManagedGatewayRuleObservation | None = None
        confirmed_gateway_observation: ManagedGatewayRuleObservation | None = None
        if kind == "topic":
            topic_artifact = _resolve_topic_artifact(
                manifest,
                project=project.project.name,
                logical_name=logical_name,
            )
            physical_name = topic_artifact.name
            backend = "direct-kafka"
            artifact_data = topic_artifact.to_dict()
        elif kind == "schema":
            schema_artifact = _resolve_schema_artifact(
                manifest,
                project=project.project.name,
                logical_name=logical_name,
            )
            physical_name = schema_artifact.subject
            backend = "schema-registry"
            artifact_data = schema_artifact.to_dict()
        elif kind == "connector":
            connector_binding = _connect_binding_from_project(project)
            connector_artifact = _resolve_connector_artifact(
                manifest,
                project=project.project.name,
                logical_name=logical_name,
                binding=connector_binding,
            )
            physical_name = connector_artifact.name
            backend = connector_binding.backend_identity
            artifact_data = connector_artifact.to_dict()
        else:
            gateway_target = _resolve_provider_free_gateway_adoption_target(
                manifest,
                project,
                environment=effective_environment,
                logical_name=logical_name,
            )
            physical_name = gateway_target.alias_name
            backend = gateway_target.binding.backend_identity
            artifact_data = gateway_target.artifact.to_dict()
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
        state_service = make_deployment_state_service(
            project_path,
            project=project.project.name,
            environment=effective_environment,
            config=project.deployment_state,
        )
        state_operation = operation_stack.enter_context(
            state_service.operation()
        )
        # Keep one provider operation across observation, confirmation, and
        # the final compare-and-swap boundary. Remote providers bind state and
        # control evidence in the same snapshot.
        prior_snapshot = state_operation.observe()
        prior_state = prior_snapshot.state.state
        state_operation.ensure_ready(prior_snapshot)
        local_state_output = project.deployment_state.backend == "local"
        if local_state_output:
            fmt.print_warning(
                f"{LOCAL_STATE_CI_WARNING} State file: {state_path}",
                code=ErrorCode.LOCAL_STATE_ONLY,
            )

        if gateway_target is not None:
            try:
                locked_gateway_target = resolve_gateway_adoption_target(
                    manifest,
                    project,
                    environment=effective_environment,
                    logical_name=logical_name,
                    prior_state=prior_state,
                )
            except GatewayAdoptionError as exc:
                raise AdoptionError(
                    ErrorCode.ADOPTION_STATE_CONFLICT,
                    str(exc),
                ) from None
            if (
                locked_gateway_target.resource_id != gateway_target.resource_id
                or locked_gateway_target.binding != gateway_target.binding
                or locked_gateway_target.rule_name != gateway_target.rule_name
                or locked_gateway_target.alias_name != gateway_target.alias_name
                or locked_gateway_target.desired != gateway_target.desired
                or locked_gateway_target.desired_artifact_checksum
                != gateway_target.desired_artifact_checksum
                or locked_gateway_target.artifact.to_dict() != gateway_target.artifact.to_dict()
            ):
                raise AdoptionError(
                    ErrorCode.ADOPTION_STATE_CONFLICT,
                    "Gateway adoption target changed during authoritative state preflight",
                )
            gateway_target = locked_gateway_target
            physical_name = gateway_target.alias_name
            backend = gateway_target.binding.backend_identity
            artifact_data = gateway_target.artifact.to_dict()
            desired_record = gateway_target.desired_record
        else:
            desired_record = ManagedResourceRecord(
                physical_name=physical_name,
                ownership="adopted",
                artifact_checksum=artifact_checksum(artifact_data),
                backend=backend,
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
            other_is_connector = False
            other_is_gateway = False
            if kind == "connector":
                try:
                    other_is_connector = ResourceIdentity.parse(other_uri).kind == "connector"
                except StateError:
                    # LocalState validation normally makes this unreachable.
                    other_is_connector = True
            elif kind == "gateway_rule":
                try:
                    other_is_gateway = ResourceIdentity.parse(other_uri).kind == "gateway_rule"
                except StateError:
                    # LocalState validation normally makes this unreachable.
                    other_is_gateway = True
            if (
                other_uri != resource_uri
                and (
                    other_record.backend == desired_record.backend
                    or (other_is_connector and other_record.backend == "kafka-connect")
                    or (other_is_gateway and other_record.backend == "conduktor-gateway")
                )
                and other_record.physical_name == desired_record.physical_name
            ):
                raise AdoptionError(
                    ErrorCode.ADOPTION_STATE_CONFLICT,
                    f"Physical {kind} {physical_name!r} is already claimed by {other_uri}",
                )

        if topic_artifact is not None:
            kafka = make_kafka_deployer(project, fmt)
            if kafka is None:
                raise AdoptionError(
                    ErrorCode.ADOPTION_FAILED,
                    "Kafka is not configured or reachable; adoption requires live observation",
                )
            try:
                current_topic = kafka.get_topic_state(
                    topic_artifact.name,
                    strict_config=True,
                )
            except Exception as exc:
                raise AdoptionError(
                    ErrorCode.ADOPTION_FAILED,
                    f"Could not observe topic {topic_artifact.name!r}: {_redact(str(exc))}",
                ) from exc
            _validate_live_topic_state(
                current_topic,
                topic=topic_artifact.name,
            )
            data = _adoption_data(
                resource_uri=resource_uri,
                logical_name=logical_name,
                artifact=topic_artifact,
                current=current_topic,
                state_path=state_path if local_state_output else None,
                state_serial=prior_state.serial,
            )
            confirmed_fingerprint = _topic_observation_fingerprint(current_topic)
        elif schema_artifact is not None:
            schema_registry = make_sr_deployer(project, fmt)
            if schema_registry is None:
                raise AdoptionError(
                    ErrorCode.ADOPTION_FAILED,
                    "Schema Registry is not configured or reachable; adoption requires "
                    "live observation",
                )
            try:
                current_schema = schema_registry.get_schema_state(schema_artifact.subject)
            except Exception as exc:
                raise AdoptionError(
                    ErrorCode.ADOPTION_FAILED,
                    f"Could not observe schema subject {schema_artifact.subject!r}: "
                    f"{_redact(str(exc))}",
                ) from exc
            _validate_live_schema_state(
                current_schema,
                subject=schema_artifact.subject,
            )
            data = _schema_adoption_data(
                resource_uri=resource_uri,
                logical_name=logical_name,
                artifact=schema_artifact,
                current=current_schema,
                state_path=state_path if local_state_output else None,
                state_serial=prior_state.serial,
            )
            confirmed_fingerprint = _schema_observation_fingerprint(current_schema)
        elif connector_artifact is not None:
            if connector_artifact is None or connector_binding is None:
                raise AdoptionError(
                    ErrorCode.ADOPTION_TARGET_INVALID,
                    "Compiled connector adoption target is missing",
                )
            connect = make_connect_deployer(project, fmt)
            if connect is None:
                raise AdoptionError(
                    ErrorCode.ADOPTION_FAILED,
                    "Kafka Connect is not configured or reachable; adoption requires "
                    "strict live observation",
                )
            try:
                factory_binding = connect.require_cluster_binding()
            except ConnectClusterBindingError:
                raise AdoptionError(
                    ErrorCode.ADOPTION_FAILED,
                    "Kafka Connect deployer has no valid cluster binding",
                ) from None
            if factory_binding != connector_binding:
                raise AdoptionError(
                    ErrorCode.ADOPTION_FAILED,
                    "Kafka Connect deployer binding does not match project configuration",
                )
            try:
                current_connector = connect.observe_managed_connector(connector_artifact.name)
            except (ConnectManagedObservationError, ConnectClusterBindingError) as exc:
                raise AdoptionError(
                    ErrorCode.ADOPTION_FAILED,
                    f"Could not strictly observe connector: {_redact(str(exc))}",
                ) from exc
            current_connector = _validate_live_connector_observation(
                current_connector,
                artifact=connector_artifact,
                binding=connector_binding,
            )
            data = _connector_adoption_data(
                resource_uri=resource_uri,
                artifact=connector_artifact,
                current=current_connector,
                binding=connector_binding,
                state_path=state_path if local_state_output else None,
                state_serial=prior_state.serial,
            )
            confirmed_fingerprint = current_connector.fingerprint
        else:
            if gateway_target is None:
                raise AdoptionError(
                    ErrorCode.ADOPTION_TARGET_INVALID,
                    "Compiled Gateway adoption target is missing",
                )
            gateway = make_gateway_deployer(project, fmt)
            if gateway is None:
                raise AdoptionError(
                    ErrorCode.ADOPTION_FAILED,
                    "Conduktor Gateway is not configured or reachable; adoption "
                    "requires strict live observation",
                )
            try:
                gateway_factory_binding = gateway.cluster_binding
            except Exception as exc:
                raise AdoptionError(
                    ErrorCode.ADOPTION_FAILED,
                    f"Conduktor Gateway deployer has no valid binding: {_redact(str(exc))}",
                ) from exc
            if gateway_factory_binding != gateway_target.binding:
                raise AdoptionError(
                    ErrorCode.ADOPTION_FAILED,
                    "Conduktor Gateway deployer binding does not match project configuration",
                )
            try:
                current_gateway = gateway.observe_managed_gateway_snapshot().rule(
                    gateway_target.rule_name,
                    gateway_target.alias_name,
                )
                review = build_gateway_adoption_review(
                    gateway_target,
                    current_gateway,
                )
            except GatewayAdoptionLiveNotFoundError as exc:
                raise AdoptionError(
                    ErrorCode.ADOPTION_LIVE_NOT_FOUND,
                    str(exc),
                ) from None
            except Exception as exc:
                raise AdoptionError(
                    ErrorCode.ADOPTION_FAILED,
                    f"Could not strictly observe Gateway alias: {_redact(str(exc))}",
                ) from exc
            data = {
                **review.to_dict(),
                "kind": "gateway_rule",
                "has_pending_changes": bool(review.pending_change_categories),
                "observation_fingerprint": review.observed_aggregate_fingerprint,
                "state_serial": prior_state.serial,
            }
            if local_state_output:
                data["state_file"] = str(state_path)
            confirmed_fingerprint = review.observed_aggregate_fingerprint
        data["observation_fingerprint"] = confirmed_fingerprint
        fmt.set_data(data)
        fmt.print(f"[cyan]Adoption candidate:[/cyan] {resource_uri}")
        if gateway_target is not None:
            fmt.print(f"  Alias key: {_redact(gateway_target.alias_name)}")
            fmt.print(f"  Effective vCluster: {gateway_target.binding.virtual_cluster}")
            fmt.print(f"  Endpoint fingerprint: {data['endpoint_fingerprint']}")
            fmt.print("  Physical cluster: main")
            fmt.print(f"  Observed mapping checksum: {data['observed_mapping_checksum']}")
            fmt.print(f"  Desired mapping checksum: {data['desired_mapping_checksum']}")
            fmt.print(f"  Desired artifact checksum: {data['desired_artifact_checksum']}")
            fmt.print(f"  Observation fingerprint: {confirmed_fingerprint}")
            fmt.print(
                "  Pending change categories: "
                f"{json.dumps(data['pending_change_categories'], sort_keys=True)}"
            )
        else:
            fmt.print(f"  Physical {kind}: {_redact(physical_name)}")
            fmt.print(f"  Observed: {json.dumps(data['observed'], sort_keys=True)}")
            fmt.print(f"  Observation fingerprint: {confirmed_fingerprint}")
            fmt.print(
                "  Desired managed attributes: "
                f"{json.dumps(data['desired_managed_attributes'], sort_keys=True)}"
            )
            fmt.print(f"  Pending diffs: {json.dumps(data['pending_diffs'], sort_keys=True)}")

        if existing is not None:
            data["adopted"] = False
            data["already_owned"] = True
            operation_stack.close()
            if connect is not None:
                close_deployers(connect)
                connect = None
            if gateway is not None:
                close_deployers(gateway)
                gateway = None
            fmt.print(f"[green]{kind.title()} already has an identical ownership record.[/green]")
            fmt.flush()
            return

        _require_confirmation(
            resource_uri=resource_uri,
            environment=effective_environment,
            confirm_resource=confirm_resource,
            confirm_environment=confirm_environment,
            allow_prompt=fmt.format == "text" and _stdin_is_interactive(),
        )

        # Confirmation approves one exact observation, not merely a resource
        # name. Re-observe the same target and require every observed field to
        # match without emitting sensitive values.
        if topic_artifact is not None:
            if kafka is None:
                raise AdoptionError(
                    ErrorCode.ADOPTION_FAILED,
                    "Kafka is not configured or reachable; adoption requires live observation",
                )
            try:
                confirmed_topic = kafka.get_topic_state(
                    topic_artifact.name,
                    strict_config=True,
                )
            except Exception as exc:
                raise AdoptionError(
                    ErrorCode.ADOPTION_FAILED,
                    f"Could not re-observe topic {topic_artifact.name!r}: {_redact(str(exc))}",
                ) from exc
            _validate_live_topic_state(
                confirmed_topic,
                topic=topic_artifact.name,
            )
            current_fingerprint = _topic_observation_fingerprint(confirmed_topic)
        elif schema_artifact is not None:
            if schema_registry is None:
                raise AdoptionError(
                    ErrorCode.ADOPTION_TARGET_INVALID,
                    "Compiled adoption target is missing",
                )
            try:
                confirmed_schema = schema_registry.get_schema_state(schema_artifact.subject)
            except Exception as exc:
                raise AdoptionError(
                    ErrorCode.ADOPTION_FAILED,
                    f"Could not re-observe schema subject {schema_artifact.subject!r}: "
                    f"{_redact(str(exc))}",
                ) from exc
            _validate_live_schema_state(
                confirmed_schema,
                subject=schema_artifact.subject,
            )
            current_fingerprint = _schema_observation_fingerprint(confirmed_schema)
        elif connector_artifact is not None:
            if connector_artifact is None or connector_binding is None or connect is None:
                raise AdoptionError(
                    ErrorCode.ADOPTION_TARGET_INVALID,
                    "Compiled connector adoption target is missing",
                )
            try:
                confirmed_connector = connect.observe_managed_connector(
                    connector_artifact.name
                )
            except (ConnectManagedObservationError, ConnectClusterBindingError) as exc:
                raise AdoptionError(
                    ErrorCode.ADOPTION_FAILED,
                    f"Could not strictly re-observe connector: {_redact(str(exc))}",
                ) from exc
            if (
                isinstance(confirmed_connector, ManagedConnectorObservation)
                and confirmed_connector.binding != connector_binding
            ):
                raise AdoptionError(
                    ErrorCode.ADOPTION_CONFIRMATION_REQUIRED,
                    "Live connector cluster binding changed after confirmation; "
                    "rerun adoption",
                )
            confirmed_connector = _validate_live_connector_observation(
                confirmed_connector,
                artifact=connector_artifact,
                binding=connector_binding,
            )
            current_fingerprint = confirmed_connector.fingerprint
        else:
            if gateway_target is None or gateway is None or current_gateway is None:
                raise AdoptionError(
                    ErrorCode.ADOPTION_TARGET_INVALID,
                    "Compiled Gateway adoption target is missing",
                )
            try:
                reobserved_gateway = gateway.observe_managed_gateway_snapshot().rule(
                    gateway_target.rule_name,
                    gateway_target.alias_name,
                )
                confirmed_gateway_observation = require_unchanged_gateway_adoption_observation(
                    gateway_target,
                    current_gateway,
                    reobserved_gateway,
                )
            except GatewayAdoptionDriftError as exc:
                raise AdoptionError(
                    ErrorCode.ADOPTION_CONFIRMATION_REQUIRED,
                    str(exc),
                ) from None
            except Exception as exc:
                raise AdoptionError(
                    ErrorCode.ADOPTION_FAILED,
                    f"Could not strictly re-observe Gateway alias: {_redact(str(exc))}",
                ) from exc
            current_fingerprint = confirmed_gateway_observation.fingerprint

        if gateway_target is None:
            _require_unchanged_observation(
                kind=kind,
                confirmed_fingerprint=confirmed_fingerprint,
                current_fingerprint=current_fingerprint,
            )

        # Bind the exact state and control revisions immediately before intent.
        # Any state/control drift requires a fresh observation and confirmation.
        final_snapshot = state_operation.observe()
        state_operation.ensure_ready(final_snapshot)
        if (
            final_snapshot.state.store != prior_snapshot.state.store
            or final_snapshot.state.revision != prior_snapshot.state.revision
            or final_snapshot.state.state != prior_snapshot.state.state
            or final_snapshot.control.revision != prior_snapshot.control.revision
            or final_snapshot.control.control != prior_snapshot.control.control
        ):
            raise AdoptionError(
                ErrorCode.ADOPTION_STATE_CONFLICT,
                "Deployment state changed after confirmation; rerun adoption",
            )
        final_state = final_snapshot.state.state
        resources = dict(final_state.resources)
        resources[resource_uri] = desired_record
        next_state = LocalState(
            project=final_state.project,
            environment=final_state.environment,
            serial=final_state.serial + 1,
            resources=resources,
        )
        gateway_evidence = None
        if gateway_target is not None:
            if confirmed_gateway_observation is None:
                raise AdoptionError(
                    ErrorCode.ADOPTION_FAILED,
                    "Confirmed Gateway adoption evidence is missing",
                )
            try:
                gateway_evidence = build_gateway_adoption_action_evidence(
                    gateway_target,
                    confirmed_gateway_observation,
                )
            except GatewayAdoptionError as exc:
                raise AdoptionError(
                    ErrorCode.ADOPTION_FAILED,
                    str(exc),
                ) from None
        operation_id = str(uuid.uuid4())
        intent = OperationIntent(
            operation_id=operation_id,
            kind="adopt",
            started_at=operation_timestamp(),
            actor="local-cli",
            prior_state_serial=final_state.serial,
            prior_state_checksum=state_checksum(final_state),
            reviewed_plan_checksum=None,
            actions=(
                OperationAction(
                    index=0,
                    resource_id=resource_uri,
                    action="adopt",
                    gateway_evidence=gateway_evidence,
                ),
            ),
        )
        active_snapshot: OperationSnapshot | None = None
        state_commit_attempted = False
        operation_finalized = False
        try:
            active_snapshot = state_operation.begin_operation(
                final_snapshot,
                intent,
            )
            state_operation.check_lock()
            active_snapshot = state_operation.record_progress(
                active_snapshot,
                OperationProgress(
                    operation_id=operation_id,
                    action_index=0,
                    resource_id=resource_uri,
                    action="adopt",
                    status="started",
                    succeeded=None,
                    recorded_at=operation_timestamp(),
                ),
            )
            active_snapshot = state_operation.record_progress(
                active_snapshot,
                OperationProgress(
                    operation_id=operation_id,
                    action_index=0,
                    resource_id=resource_uri,
                    action="adopt",
                    status="completed",
                    succeeded=True,
                    recorded_at=operation_timestamp(),
                ),
            )
            state_commit_attempted = True
            active_snapshot = state_operation.commit_operation(
                active_snapshot,
                next_state,
            )
            operation_finalized = True
        except StateBackendConflictError:
            raise
        except StateConflictError as exc:
            raise AdoptionError(
                (
                    ErrorCode.ADOPTION_FAILED
                    if state_commit_attempted
                    else ErrorCode.ADOPTION_STATE_CONFLICT
                ),
                (
                    "Could not confirm the atomic adoption state commit"
                    if state_commit_attempted
                    else str(exc)
                ),
            ) from exc
        except StateBackendUnknownCommitError:
            raise
        except OSError as exc:
            raise StateBackendUnknownCommitError(
                "Could not confirm the atomic adoption state commit",
                operation_id=operation_id,
            ) from exc
        finally:
            if active_snapshot is not None and not operation_finalized:
                if active_snapshot.control.control.progress:
                    try:
                        completed = [
                            progress.action_index
                            for progress in active_snapshot.control.control.progress
                            if progress.status == "completed" and progress.succeeded is True
                        ]
                        state_operation.mark_recovery_required(
                            active_snapshot,
                            RecoveryRecord(
                                operation_id=operation_id,
                                failure_code=(
                                    "adoption_state_commit_uncertain"
                                    if state_commit_attempted
                                    else "adoption_operation_interrupted"
                                ),
                                failed_at=operation_timestamp(),
                                last_completed_action_index=(max(completed) if completed else None),
                            ),
                        )
                    except BaseException:
                        # The existing in_progress marker remains blocking.
                        pass
                else:
                    try:
                        state_operation.clear_before_mutation(active_snapshot)
                    except BaseException:
                        # The existing in_progress marker remains conservative.
                        pass

        data["adopted"] = True
        data["already_owned"] = False
        data["state_serial"] = next_state.serial
        data["committed"] = True
        next_command = [
            "streamt",
            "plan",
            "--project-dir",
            str(project_path),
        ]
        if parser_environment is not None:
            next_command.extend(["--env", effective_environment])
        reviewed_plan_path = (
            project_path / ".streamt" / "plans" / f"{effective_environment}-reviewed-plan.json"
        )
        next_command.extend(["--out", str(reviewed_plan_path)])
        data["next_command"] = next_command
        fmt.set_data(data)

        # A provider may own a remote session or lease for the entire context.
        # Keep the verified result buffered until releasing that authority has
        # succeeded.
        operation_stack.close()

        if connect is not None:
            close_deployers(connect)
            connect = None
        if gateway is not None:
            close_deployers(gateway)
            gateway = None

        backend_name = {
            "topic": "Kafka",
            "schema": "Schema Registry",
            "connector": "Kafka Connect",
            "gateway_rule": "Conduktor Gateway",
        }[kind]
        fmt.print(f"[green]Ownership adopted. {backend_name} was not modified.[/green]")
        fmt.print("Run a fresh reviewed plan before any apply.")
        fmt.flush()

    except (EnvVarError, ParseError, EnvironmentError) as exc:
        handle_parse_error(fmt, exc, ErrorCode.PARSE_ERROR)
    except AdoptionError as exc:
        if data:
            fmt.set_data(data)
        safe_message = redact_sensitive_text(exc.message)
        fmt.add_error(StructuredError(code=exc.code, message=safe_message))
        fmt.print_error(safe_message)
        fmt.flush()
        raise click.exceptions.Exit(1) from exc
    except RemoteStateRequiredError as exc:
        safe_message = redact_sensitive_text(exc)
        fmt.add_error(
            StructuredError(
                code=ErrorCode.REMOTE_STATE_REQUIRED,
                message=safe_message,
            )
        )
        fmt.print_error(safe_message)
        fmt.flush()
        raise click.exceptions.Exit(1) from exc
    except StateBackendReleaseAfterCommitError as exc:
        safe_message, error_operation_id = state_operation_error_details(exc)
        data["committed"] = exc.committed
        fmt.set_data(data)
        fmt.add_error(
            StructuredError(
                code=ErrorCode.STATE_RELEASE_FAILED_AFTER_COMMIT,
                message=safe_message,
                operation_id=error_operation_id,
            )
        )
        fmt.print_error(safe_message)
        fmt.flush()
        raise click.exceptions.Exit(1) from exc
    except StateBackendLockTimeoutError as exc:
        safe_message = redact_sensitive_text(exc)
        fmt.add_error(
            StructuredError(code=ErrorCode.STATE_LOCK_TIMEOUT, message=safe_message)
        )
        fmt.print_error(safe_message)
        fmt.flush()
        raise click.exceptions.Exit(1) from exc
    except StateBackendLockLostError as exc:
        safe_message, error_operation_id = state_operation_error_details(exc)
        fmt.add_error(
            StructuredError(
                code=ErrorCode.STATE_LOCK_LOST,
                message=safe_message,
                operation_id=error_operation_id,
            )
        )
        fmt.print_error(safe_message)
        fmt.flush()
        raise click.exceptions.Exit(1) from exc
    except StateBackendConflictError as exc:
        safe_message = redact_sensitive_text(exc)
        fmt.add_error(
            StructuredError(code=ErrorCode.STATE_CONFLICT, message=safe_message)
        )
        fmt.print_error(safe_message)
        fmt.flush()
        raise click.exceptions.Exit(1) from exc
    except StateBackendUnknownCommitError as exc:
        safe_message, error_operation_id = state_operation_error_details(exc)
        fmt.add_error(
            StructuredError(
                code=ErrorCode.STATE_UNKNOWN_OUTCOME,
                message=safe_message,
                operation_id=error_operation_id,
            )
        )
        fmt.print_error(safe_message)
        fmt.flush()
        raise click.exceptions.Exit(1) from exc
    except StateBackendUnavailableError as exc:
        safe_message = redact_sensitive_text(exc)
        fmt.add_error(
            StructuredError(
                code=ErrorCode.STATE_BACKEND_UNAVAILABLE,
                message=safe_message,
            )
        )
        fmt.print_error(safe_message)
        fmt.flush()
        raise click.exceptions.Exit(1) from exc
    except StateBackendRecoveryRequiredError as exc:
        safe_message = redact_sensitive_text(exc)
        fmt.add_error(
            StructuredError(
                code=ErrorCode.STATE_RECOVERY_REQUIRED,
                message=safe_message,
            )
        )
        fmt.print_error(safe_message)
        fmt.flush()
        raise click.exceptions.Exit(1) from exc
    except StateError as exc:
        safe_message = redact_sensitive_text(exc)
        fmt.add_error(
            StructuredError(code=ErrorCode.STATE_INVALID, message=safe_message)
        )
        fmt.print_error(safe_message)
        fmt.flush()
        raise click.exceptions.Exit(1) from exc
    except KeyboardInterrupt as exc:
        fmt.print_error("Interrupted.")
        fmt.flush()
        raise click.exceptions.Exit(130) from exc
    except Exception as exc:
        message = f"Adoption failed unexpectedly: {redact_sensitive_text(exc)}"
        fmt.add_error(StructuredError(code=ErrorCode.ADOPTION_FAILED, message=message))
        fmt.print_error(message)
        fmt.flush()
        raise click.exceptions.Exit(1) from exc
    finally:
        try:
            close_deployers(schema_registry, kafka, connect, gateway)
        finally:
            operation_stack.close()
