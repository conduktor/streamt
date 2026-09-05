"""Fail-closed recovery evidence derived from fresh provider observations.

Ordinary and Gateway targets use the live state retained on an already-fresh plan.
A managed Connector deletion is the deliberate exception: its current manifest may
no longer contain either the desired artifact or tombstone, so its exact target is
resolved from durable version-3 action evidence and observed once through the
planner's already-bound Connect deployer.
"""

from __future__ import annotations

import hashlib
import json
import math
from dataclasses import dataclass, field
from typing import Literal, cast

from streamt.compiler.connector_artifact import parse_compiled_connector_artifact
from streamt.compiler.manifest import (
    ArtifactOwnership,
    ConnectorArtifactFormatError,
    GatewayRuleArtifact,
)
from streamt.deployer.connect import (
    ConnectClusterBinding,
    ConnectClusterBindingError,
    ManagedConnectorObservation,
    is_connect_backend_identity,
)
from streamt.deployer.gateway import (
    ManagedGatewayRuleObservation,
    build_desired_gateway_rule,
    is_gateway_backend_identity,
    plan_managed_gateway_rule,
)
from streamt.deployer.planner import (
    ConnectorRecoveryObservation,
    DeploymentPlan,
    DeploymentPlanner,
)
from streamt.deployer.recovery import (
    RecoveryResolution,
    RecoverySnapshotEvidence,
    RecoveryTargetEvidence,
)
from streamt.deployer.recovery_service import RecoveryLiveObservation
from streamt.deployer.state import (
    LocalState,
    ManagedResourceRecord,
    ResourceIdentity,
    StateError,
    artifact_checksum,
    desired_managed_records,
    resource_id,
)
from streamt.deployer.state_backend import (
    ConnectorActionEvidence,
    ConnectorActionSurfaceEvidence,
    GatewayActionSurfaceEvidence,
    OperationAction,
)


class RecoveryObservationError(ValueError):
    """Fresh provider evidence cannot prove the requested recovery outcome."""


_Kind = Literal["schema", "topic", "flink_job", "connector", "gateway_rule"]
_GenericKind = Literal["schema", "topic", "flink_job", "connector"]
_BACKENDS: dict[_Kind, str] = {
    "schema": "schema-registry",
    "topic": "direct-kafka",
    "flink_job": "flink",
    "gateway_rule": "conduktor-gateway",
}
_SUPPORTED_ACTIONS: dict[_Kind, frozenset[str]] = {
    "schema": frozenset({"register", "update", "delete", "adopt"}),
    "topic": frozenset({"create", "update", "delete", "adopt"}),
    "flink_job": frozenset({"submit", "update", "cancel"}),
    "connector": frozenset({"create", "update", "delete", "adopt"}),
    "gateway_rule": frozenset({"create", "update", "delete", "adopt"}),
}
_DELETE_ACTIONS = frozenset({"delete", "cancel"})
_CONNECTOR_RECOVERY_INVALID = "has invalid exact Connector recovery evidence"
_CONNECTOR_RECOVERY_OBSERVATION_FAILED = "Connector recovery live observation failed"


@dataclass(frozen=True)
class _MappedChange:
    kind: _GenericKind
    change: object = field(repr=False)
    current: object | None = field(repr=False)
    desired: object | None = field(repr=False)
    physical_name: str
    backend_identity: str | None


@dataclass(frozen=True)
class _ConnectorRecoveryTarget:
    """One provider-free validated Connector deletion recovery target."""

    action: OperationAction
    binding: ConnectClusterBinding
    prior_record: ManagedResourceRecord
    mutation_started: bool


def _target_error(action: OperationAction, reason: str) -> RecoveryObservationError:
    # OperationAction has already validated the text and RecoveryTargetEvidence later
    # validates its URI.  No provider value or exception text is ever interpolated.
    return RecoveryObservationError(f"Recovery target {action.resource_id} {reason}")


def _connector_recovery_target(
    *,
    action: OperationAction,
    snapshot: RecoverySnapshotEvidence,
) -> _ConnectorRecoveryTarget:
    """Validate durable Connector deletion authority without provider access."""
    evidence = action.connector_evidence
    intent = snapshot.control.intent
    try:
        if (
            snapshot.control.control_version != 3
            or intent is None
            or intent.kind != "apply"
            or type(action) is not OperationAction
            or type(action.index) is not int
            or type(action.resource_id) is not str
            or type(action.action) is not str
            or action.action != "delete"
            or type(evidence) is not ConnectorActionEvidence
            or type(evidence.version) is not int
            or type(evidence.backend_identity) is not str
            or type(evidence.connector_name) is not str
            or type(evidence.prior_artifact_checksum) is not str
            or type(evidence.current) is not ConnectorActionSurfaceEvidence
            or type(evidence.current.exists) is not bool
            or type(evidence.current.fingerprint) is not str
            or type(evidence.desired) is not ConnectorActionSurfaceEvidence
            or type(evidence.desired.exists) is not bool
            or type(evidence.desired.fingerprint) is not str
        ):
            raise ValueError
        binding = ConnectClusterBinding.from_backend_identity(evidence.backend_identity)
        if type(binding) is not ConnectClusterBinding:
            raise ValueError
        prior = snapshot.state.resources.get(action.resource_id)
        if type(prior) is not ManagedResourceRecord or (
            type(prior.physical_name) is not str
            or type(prior.ownership) is not str
            or type(prior.artifact_checksum) is not str
            or type(prior.backend) is not str
            or prior.ownership != "managed"
            or prior.physical_name != evidence.connector_name
            or prior.backend != evidence.backend_identity
            or prior.artifact_checksum != evidence.prior_artifact_checksum
        ):
            raise ValueError
        for other_resource_id, record in snapshot.state.resources.items():
            if type(other_resource_id) is not str:
                raise ValueError
            other_identity = ResourceIdentity.parse(other_resource_id)
            if other_identity.kind != "connector" or other_resource_id == action.resource_id:
                continue
            if (
                type(record) is not ManagedResourceRecord
                or type(record.physical_name) is not str
                or type(record.ownership) is not str
                or type(record.artifact_checksum) is not str
                or type(record.backend) is not str
            ):
                raise ValueError
            try:
                other_binding = ConnectClusterBinding.from_backend_identity(record.backend)
            except ConnectClusterBindingError:
                raise ValueError from None
            if (
                record.physical_name == evidence.connector_name
                and other_binding.endpoint_fingerprint == binding.endpoint_fingerprint
            ):
                raise ValueError
    except Exception:
        raise _target_error(action, _CONNECTOR_RECOVERY_INVALID) from None
    return _ConnectorRecoveryTarget(
        action=action,
        binding=binding,
        prior_record=prior,
        mutation_started=any(
            progress.action_index == action.index
            and progress.resource_id == action.resource_id
            and progress.action == action.action
            for progress in snapshot.control.progress
        ),
    )


def _canonical_fingerprint(value: object) -> str:
    try:
        payload = json.dumps(
            value,
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    except (TypeError, ValueError) as error:
        raise RecoveryObservationError(
            "Fresh recovery observation is not canonical JSON"
        ) from error
    return f"sha256:{hashlib.sha256(payload).hexdigest()}"


def _ownership(desired: object | None) -> ArtifactOwnership | None:
    return ArtifactOwnership.from_dict(getattr(desired, "ownership", None))


def _physical_name(kind: _GenericKind, change: object) -> str | None:
    if kind == "schema":
        value = getattr(change, "subject", None)
    elif kind == "topic":
        value = getattr(change, "topic", None)
    elif kind == "flink_job":
        value = getattr(change, "job_name", None)
    else:
        value = getattr(change, "connector_name", None)
    return value if isinstance(value, str) and value else None


def _normalized_gateway_change(
    change: object,
) -> tuple[
    ManagedGatewayRuleObservation,
    ManagedGatewayRuleObservation,
    str,
] | None:
    """Return one complete normalized Gateway surface, or identify legacy state."""
    current = getattr(change, "current", None)
    desired_managed = getattr(change, "desired_managed", None)
    backend_identity = getattr(change, "backend_identity", None)
    if current is None and desired_managed is None and backend_identity is None:
        return None
    if (
        getattr(change, "current_alias", None) is not None
        or getattr(change, "current_interceptors", None) is not None
    ):
        raise RecoveryObservationError(
            "Fresh normalized Gateway recovery observation contains legacy evidence"
        )
    desired = getattr(change, "desired", None)
    if (
        not isinstance(current, ManagedGatewayRuleObservation)
        or not isinstance(desired_managed, ManagedGatewayRuleObservation)
        or not isinstance(desired, GatewayRuleArtifact)
        or not isinstance(backend_identity, str)
        or not is_gateway_backend_identity(backend_identity)
    ):
        raise RecoveryObservationError(
            "Fresh normalized Gateway recovery observation is partial"
        )
    if (
        current.binding != desired_managed.binding
        or backend_identity != desired_managed.binding.backend_identity
        or getattr(change, "name", None) != desired.name
        or current.logical_name != desired.name
        or desired_managed.logical_name != desired.name
        or current.alias_name != desired.virtual_topic
        or desired_managed.alias_name != desired.virtual_topic
        or not desired_managed.exists
    ):
        raise RecoveryObservationError(
            "Fresh normalized Gateway recovery observation has mismatched identity"
        )
    try:
        reconstructed = build_desired_gateway_rule(desired, desired_managed.binding)
    except ValueError:
        raise RecoveryObservationError(
            "Fresh normalized Gateway recovery observation is incoherent"
        ) from None
    if reconstructed != desired_managed:
        raise RecoveryObservationError(
            "Fresh normalized Gateway recovery observation has mismatched desired state"
        )
    try:
        expected = plan_managed_gateway_rule(desired, desired_managed, current)
    except ValueError:
        raise RecoveryObservationError(
            "Fresh normalized Gateway recovery observation is incoherent"
        ) from None
    if (
        getattr(change, "action", None) != expected.action
        or getattr(change, "changes", None) != expected.changes
        or expected.backend_identity != backend_identity
    ):
        raise RecoveryObservationError(
            "Fresh normalized Gateway recovery observation is incoherent"
        )
    return current, desired_managed, backend_identity


def _current(change: object) -> object | None:
    return getattr(change, "current", None)


def _iter_changes(plan: DeploymentPlan) -> list[tuple[_GenericKind, object]]:
    return [
        *(("schema", change) for change in plan.schema_changes),
        *(("topic", change) for change in plan.topic_changes),
        *(("flink_job", change) for change in plan.flink_changes),
        *(("connector", change) for change in plan.connector_changes),
    ]


def preflight_recovery_intent(
    snapshot: RecoverySnapshotEvidence,
) -> tuple[OperationAction, ...]:
    """Validate the complete blocked intent before any provider observation.

    This check deliberately depends only on durable recovery evidence.  Callers can
    therefore reject malformed, cross-address, duplicate, or legacy Gateway targets
    before constructing a fresh plan or reading any provider.
    """
    intent = snapshot.control.intent
    if intent is None:  # pragma: no cover - RecoverySnapshotEvidence rejects this
        raise RecoveryObservationError("Recovery snapshot has no blocked operation intent")
    if (
        intent.prior_state_serial != snapshot.state.serial
        or intent.prior_state_checksum != snapshot.state_checksum
    ):
        raise RecoveryObservationError(
            "Recovery intent prior state evidence does not match the recovery snapshot"
        )
    action_ids: set[str] = set()
    for action in intent.actions:
        try:
            identity = ResourceIdentity.parse(action.resource_id)
        except StateError:
            raise _target_error(action, "has invalid canonical identity") from None
        if (
            identity.project != snapshot.address.project
            or identity.environment != snapshot.address.environment
        ):
            raise _target_error(action, "belongs to another state address")
        if action.resource_id in action_ids:
            raise _target_error(action, "is duplicated in the blocked intent")
        action_ids.add(action.resource_id)
        if identity.kind == "kafka_streams_job":
            raise _target_error(
                action,
                "Kafka Streams recovery is not yet supported; pending operation must remain intact",
            )
        if identity.kind not in _SUPPORTED_ACTIONS:
            raise _target_error(action, "has an unsupported resource kind")
        kind = cast(_Kind, identity.kind)
        if action.action not in _SUPPORTED_ACTIONS[kind]:
            raise _target_error(
                action,
                "has an action incompatible with its resource kind",
            )
        if intent.kind == "adopt":
            if action.action != "adopt" or kind not in (
                "schema",
                "topic",
                "connector",
                "gateway_rule",
            ):
                raise _target_error(action, "is not a representable adoption target")
        elif action.action == "adopt":
            raise _target_error(action, "is not part of an adoption intent")
        if kind == "gateway_rule" and action.gateway_evidence is None:
            raise _target_error(action, "has no exact Gateway action evidence")
        if kind == "connector" and action.action == "delete":
            _connector_recovery_target(action=action, snapshot=snapshot)
    return intent.actions


def preflight_connector_recovery_binding(
    snapshot: RecoverySnapshotEvidence,
    binding: ConnectClusterBinding,
) -> tuple[OperationAction, ...]:
    """Bind all durable Connector deletions to one runtime-derived cluster.

    The caller derives ``binding`` from current parsed project configuration, so
    this helper can run before a Connect deployer is constructed.  It performs no
    provider access and returns only the exact Connector delete actions.
    """
    actions = preflight_recovery_intent(snapshot)
    connector_actions = tuple(
        action
        for action in actions
        if ResourceIdentity.parse(action.resource_id).kind == "connector"
        and action.action == "delete"
    )
    if not connector_actions:
        return ()
    first = connector_actions[0]
    if type(binding) is not ConnectClusterBinding:
        raise _target_error(first, _CONNECTOR_RECOVERY_INVALID)

    locators: set[tuple[str, str]] = set()
    for action in connector_actions:
        target = _connector_recovery_target(action=action, snapshot=snapshot)
        evidence = action.connector_evidence
        if type(evidence) is not ConnectorActionEvidence or target.binding != binding:
            raise _target_error(action, _CONNECTOR_RECOVERY_INVALID)
        locator = (target.binding.endpoint_fingerprint, evidence.connector_name)
        if locator in locators:
            raise _target_error(action, _CONNECTOR_RECOVERY_INVALID)
        locators.add(locator)
    return connector_actions


def _identity_for_change(
    *,
    kind: _GenericKind,
    desired: object | None,
    physical_name: str,
    backend_identity: str | None,
    state: LocalState,
) -> str | None:
    ownership = _ownership(desired)
    if ownership is not None:
        if ownership.project != state.project:
            raise RecoveryObservationError(
                "Fresh recovery plan contains an ownership identity for another project"
            )
        try:
            return resource_id(
                state.project,
                state.environment,
                kind,
                ownership.owner_name,
            )
        except StateError as error:
            raise RecoveryObservationError(
                "Fresh recovery plan contains an invalid canonical ownership identity"
            ) from error

    matches: list[str] = []
    for prior_id, record in state.resources.items():
        try:
            identity = ResourceIdentity.parse(prior_id)
        except StateError as error:  # pragma: no cover - LocalState already validates this
            raise RecoveryObservationError(
                "Recovery state contains an invalid canonical ownership identity"
            ) from error
        if (
            identity.kind == kind
            and record.physical_name == physical_name
            and (kind != "connector" or record.backend == backend_identity)
        ):
            matches.append(prior_id)
    if len(matches) > 1:
        raise RecoveryObservationError(
            "Fresh recovery plan has an ambiguous canonical ownership identity"
        )
    return matches[0] if matches else None


def _mapped_changes(
    plan: DeploymentPlan,
    state: LocalState,
) -> dict[str, _MappedChange]:
    result: dict[str, _MappedChange] = {}
    for kind, change in _iter_changes(plan):
        current = _current(change)
        backend_identity = (
            getattr(change, "backend_identity", None) if kind == "connector" else _BACKENDS[kind]
        )
        physical_name = _physical_name(kind, change)
        if physical_name is None:
            continue
        desired = getattr(change, "desired", None)
        if kind == "connector" and not is_connect_backend_identity(backend_identity):
            backend_identity = None
        identity = _identity_for_change(
            kind=kind,
            desired=desired,
            physical_name=physical_name,
            backend_identity=backend_identity,
            state=state,
        )
        # Unowned, unrelated observations do not correspond to a durable action.
        if identity is None:
            continue
        if identity in result:
            raise RecoveryObservationError(
                "Fresh recovery plan contains duplicate canonical action identity"
            )
        result[identity] = _MappedChange(
            kind=kind,
            change=change,
            current=current,
            desired=desired,
            physical_name=physical_name,
            backend_identity=backend_identity,
        )
    return result


def _expected_backend(mapped: _MappedChange) -> str:
    backend = mapped.backend_identity
    if not isinstance(backend, str) or (
        mapped.kind == "connector" and not is_connect_backend_identity(backend)
    ):
        raise RecoveryObservationError(
            "Fresh recovery observation has no canonical backend identity"
        )
    return backend


def _require_bool(value: object) -> bool:
    if type(value) is not bool:
        raise RecoveryObservationError("Fresh recovery observation is partial")
    return value


def _normalize_schema(mapped: _MappedChange) -> tuple[str, dict[str, object]]:
    current = mapped.current
    if current is None:
        raise RecoveryObservationError("Fresh recovery observation is partial")
    exists = _require_bool(getattr(current, "exists", None))
    subject = getattr(current, "subject", None)
    if not isinstance(subject, str) or subject != mapped.physical_name:
        raise RecoveryObservationError("Fresh recovery observation has mismatched identity")
    if not exists:
        return "absent", {"kind": "schema", "presence": "absent"}
    version = getattr(current, "version", None)
    schema_id = getattr(current, "schema_id", None)
    schema_type = getattr(current, "schema_type", None)
    compatibility = getattr(current, "compatibility", None)
    schema = getattr(current, "schema", None)
    references = getattr(current, "references", None)
    if (
        type(version) is not int
        or version < 1
        or type(schema_id) is not int
        or schema_id < 1
        or schema is None
        or not isinstance(schema_type, str)
        or not schema_type
        or (compatibility is not None and not isinstance(compatibility, str))
        or not isinstance(references, list)
    ):
        raise RecoveryObservationError("Fresh recovery observation is partial")
    normalized_references: list[dict[str, object]] = []
    for reference in references:
        name = getattr(reference, "name", None)
        referenced_subject = getattr(reference, "subject", None)
        referenced_version = getattr(reference, "version", None)
        if (
            not isinstance(name, str)
            or not isinstance(referenced_subject, str)
            or type(referenced_version) is not int
            or referenced_version < 1
        ):
            raise RecoveryObservationError("Fresh recovery observation is partial")
        normalized_references.append(
            {
                "name": name,
                "subject": referenced_subject,
                "version": referenced_version,
            }
        )
    normalized_references.sort(
        key=lambda item: (
            str(item["name"]),
            str(item["subject"]),
            str(item["version"]),
        )
    )
    return "present", {
        "kind": "schema",
        "presence": "present",
        "subject": subject,
        "version": version,
        "schema_id": schema_id,
        "schema": schema,
        "schema_type": schema_type,
        "compatibility": compatibility,
        "references": normalized_references,
    }


def _normalize_topic(mapped: _MappedChange) -> tuple[str, dict[str, object]]:
    current = mapped.current
    if current is None:
        raise RecoveryObservationError("Fresh recovery observation is partial")
    exists = _require_bool(getattr(current, "exists", None))
    name = getattr(current, "name", None)
    if not isinstance(name, str) or name != mapped.physical_name:
        raise RecoveryObservationError("Fresh recovery observation has mismatched identity")
    if not exists:
        return "absent", {"kind": "topic", "presence": "absent"}
    partitions = getattr(current, "partitions", None)
    replication_factor = getattr(current, "replication_factor", None)
    config = getattr(current, "config", None)
    if (
        type(partitions) is not int
        or partitions < 1
        or type(replication_factor) is not int
        or replication_factor < 1
        or not isinstance(config, dict)
        or any(not isinstance(key, str) for key in config)
    ):
        raise RecoveryObservationError("Fresh recovery observation is partial")
    return "present", {
        "kind": "topic",
        "presence": "present",
        "name": name,
        "partitions": partitions,
        "replication_factor": replication_factor,
        "config": config,
    }


def _normalize_flink(mapped: _MappedChange) -> tuple[str, dict[str, object]]:
    current = mapped.current
    if current is None:
        raise RecoveryObservationError("Fresh recovery observation is partial")
    exists = _require_bool(getattr(current, "exists", None))
    name = getattr(current, "name", None)
    if not isinstance(name, str) or name != mapped.physical_name:
        raise RecoveryObservationError("Fresh recovery observation has mismatched identity")
    if not exists:
        return "absent", {"kind": "flink_job", "presence": "absent"}
    # FlinkJobState contains only runtime status and an opaque job id.  It cannot
    # prove the SQL or execution settings represented by the ownership checksum.
    raise RecoveryObservationError("Fresh Flink observation cannot prove managed artifact content")


def _normalize_connector(mapped: _MappedChange) -> tuple[str, dict[str, object]]:
    current = mapped.current
    if not isinstance(current, ManagedConnectorObservation):
        raise RecoveryObservationError("Fresh recovery observation is partial")
    backend_identity = _expected_backend(mapped)
    if current.binding.backend_identity != backend_identity:
        raise RecoveryObservationError("Fresh recovery observation has mismatched backend identity")
    if current.name != mapped.physical_name:
        raise RecoveryObservationError("Fresh recovery observation has mismatched identity")
    desired_cluster = getattr(mapped.desired, "cluster", None)
    if mapped.desired is not None and (
        not isinstance(desired_cluster, str)
        or not desired_cluster
        or desired_cluster != current.binding.cluster_alias
    ):
        raise RecoveryObservationError("Fresh recovery observation has mismatched backend identity")
    normalized: dict[str, object] = {
        "kind": "connector",
        "backend_identity": backend_identity,
        "cluster": current.binding.cluster_alias,
        "name": current.name,
        "presence": "present" if current.exists else "absent",
    }
    if not current.exists:
        return "absent", normalized
    normalized["config"] = current.config_dict()
    return "present", normalized


def _normalize_live(mapped: _MappedChange) -> tuple[str, dict[str, object]]:
    if mapped.kind == "schema":
        return _normalize_schema(mapped)
    if mapped.kind == "topic":
        return _normalize_topic(mapped)
    if mapped.kind == "flink_job":
        return _normalize_flink(mapped)
    return _normalize_connector(mapped)


def _effective_fresh_action(
    *,
    action: OperationAction,
    mapped: _MappedChange,
    plan: DeploymentPlan,
) -> str:
    requirements = [
        requirement
        for requirement in plan.ownership_requirements
        if requirement.resource_id == action.resource_id
    ]
    if len(requirements) > 1:
        raise _target_error(action, "has ambiguous ownership evidence")
    if requirements:
        # A successfully-created provider object is expected to look unowned while
        # the state commit is unresolved.  The requirement retains the planner's
        # pre-policy observation, which is the only action verdict used here.
        return requirements[0].observed_action
    value = getattr(mapped.change, "action", None)
    if not isinstance(value, str) or not value:
        raise _target_error(action, "has incomplete fresh action evidence")
    return value


def _direct_desired_record(
    *,
    action: OperationAction,
    mapped: _MappedChange,
    state: LocalState,
) -> ManagedResourceRecord:
    desired = mapped.desired
    ownership = _ownership(desired)
    if desired is None or ownership is None or not hasattr(desired, "to_dict"):
        raise _target_error(action, "has no exact desired ownership record")
    if (
        ownership.project != state.project
        or ownership.owner_name != ResourceIdentity.parse(action.resource_id).logical_name
        or ownership.mode not in ("managed", "adopted")
    ):
        raise _target_error(action, "has mismatched desired ownership evidence")
    try:
        raw = desired.to_dict()
        if not isinstance(raw, dict):
            raise TypeError
        return ManagedResourceRecord(
            physical_name=mapped.physical_name,
            ownership=ownership.mode,  # type: ignore[arg-type]
            artifact_checksum=artifact_checksum(raw),
            backend=_expected_backend(mapped),
        )
    except (StateError, TypeError, ValueError):
        raise _target_error(action, "has malformed desired ownership evidence") from None


def _desired_record(
    *,
    action: OperationAction,
    mapped: _MappedChange,
    state: LocalState,
    records: dict[str, ManagedResourceRecord],
) -> ManagedResourceRecord | None:
    if action.action in _DELETE_ACTIONS:
        return None
    record = records.get(action.resource_id)
    if record is not None:
        if record.backend != _expected_backend(mapped):
            raise _target_error(action, "has mismatched desired backend evidence")
        return record
    return _direct_desired_record(
        action=action,
        mapped=mapped,
        state=state,
    )


def _prior_artifact_checksum(
    *,
    mapped: _MappedChange,
    normalized: dict[str, object],
    prior: ManagedResourceRecord,
) -> str | None:
    ownership = _ownership(mapped.desired)
    if ownership is None or ownership.mode != prior.ownership:
        return None
    ownership_dict = ownership.to_dict()
    if mapped.kind == "schema":
        raw = {
            "subject": normalized["subject"],
            "schema": normalized["schema"],
            "schema_type": normalized["schema_type"],
            "compatibility": normalized["compatibility"],
            "ownership": ownership_dict,
        }
    elif mapped.kind == "topic":
        raw = {
            "name": normalized["name"],
            "partitions": normalized["partitions"],
            "replication_factor": normalized["replication_factor"],
            "config": normalized["config"],
            "ownership": ownership_dict,
        }
    elif mapped.kind == "connector":
        cluster = normalized.get("cluster")
        config = normalized["config"]
        if not isinstance(cluster, str) or not cluster or not isinstance(config, dict):
            return None
        connector_class = config.get("connector.class")
        topics = config.get("topics")
        if not isinstance(connector_class, str) or not isinstance(topics, str):
            return None
        raw = {
            "name": normalized["name"],
            "connector_class": connector_class,
            "topics": topics.split(","),
            "cluster": cluster,
            "config": config,
            "ownership": ownership_dict,
        }
    else:
        # Flink does not expose content.
        return None
    try:
        if mapped.kind == "connector":
            raw = parse_compiled_connector_artifact(raw).to_dict()
        return artifact_checksum(raw)
    except (ConnectorArtifactFormatError, StateError):
        return None


def _live_matches_desired(
    mapped: _MappedChange,
    presence: str,
    normalized: dict[str, object],
) -> bool:
    if presence != "present" or mapped.desired is None:
        return False
    if mapped.kind == "schema":
        return (
            getattr(mapped.desired, "subject", None) == mapped.physical_name
            and normalized.get("schema") == getattr(mapped.desired, "schema", None)
            and str(normalized.get("schema_type", "")).upper()
            == str(getattr(mapped.desired, "schema_type", "")).upper()
            and normalized.get("compatibility") == getattr(mapped.desired, "compatibility", None)
        )
    if mapped.kind == "topic":
        return (
            getattr(mapped.desired, "name", None) == mapped.physical_name
            and normalized.get("partitions") == getattr(mapped.desired, "partitions", None)
            and normalized.get("replication_factor")
            == getattr(mapped.desired, "replication_factor", None)
            and normalized.get("config") == getattr(mapped.desired, "config", None)
        )
    if mapped.kind == "flink_job":
        return False
    raw = mapped.desired.to_dict() if hasattr(mapped.desired, "to_dict") else None
    return (
        getattr(mapped.desired, "name", None) == mapped.physical_name
        and normalized.get("cluster") == getattr(mapped.desired, "cluster", None)
        and isinstance(raw, dict)
        and normalized.get("config") == raw.get("config")
    )


def _gateway_surface(
    observation: ManagedGatewayRuleObservation,
) -> GatewayActionSurfaceEvidence:
    return GatewayActionSurfaceEvidence(
        exists=observation.exists,
        fingerprint=observation.fingerprint,
        managed_interceptor_count=len(observation.interceptors),
    )


def _connector_recovery_targets(
    *,
    planner: DeploymentPlanner,
    actions: tuple[OperationAction, ...],
    snapshot: RecoverySnapshotEvidence,
) -> dict[str, _ConnectorRecoveryTarget]:
    """Bind every durable Connector deletion target before any live read."""
    connector_actions = tuple(
        action
        for action in actions
        if ResourceIdentity.parse(action.resource_id).kind == "connector"
        and action.action == "delete"
    )
    if not connector_actions:
        return {}

    first = connector_actions[0]
    try:
        configured_binding = planner._connect_binding_from_project()
        deployer = planner.connect_deployer
        if deployer is None:
            raise ValueError
        deployer_binding = deployer.require_cluster_binding()
        if (
            type(configured_binding) is not ConnectClusterBinding
            or type(deployer_binding) is not ConnectClusterBinding
            or deployer_binding != configured_binding
        ):
            raise ValueError
    except Exception:
        raise _target_error(first, _CONNECTOR_RECOVERY_INVALID) from None

    bound_actions = preflight_connector_recovery_binding(snapshot, configured_binding)
    targets: dict[str, _ConnectorRecoveryTarget] = {}
    for action in bound_actions:
        target = _connector_recovery_target(action=action, snapshot=snapshot)
        targets[action.resource_id] = target
    return targets


def _strict_connector_recovery_observation(
    *,
    action: OperationAction,
    value: object,
) -> ManagedConnectorObservation:
    """Detach one exact base observation without trusting subclasses or aliases."""
    try:
        if type(value) is not ManagedConnectorObservation:
            raise ValueError
        binding = value.binding
        if (
            type(binding) is not ConnectClusterBinding
            or type(binding.version) is not int
            or type(binding.cluster_alias) is not str
            or type(binding.endpoint_fingerprint) is not str
            or type(value.name) is not str
            or type(value.exists) is not bool
            or type(value.config) is not tuple
        ):
            raise ValueError
        config: list[tuple[str, str | bool | int | float]] = []
        for entry in value.config:
            if type(entry) is not tuple or len(entry) != 2:
                raise ValueError
            key, scalar = entry
            if type(key) is not str or type(scalar) not in (str, bool, int, float):
                raise ValueError
            if type(scalar) is float and not math.isfinite(scalar):
                raise ValueError
            config.append((key, scalar))
        return ManagedConnectorObservation(
            binding=ConnectClusterBinding(
                cluster_alias=binding.cluster_alias,
                endpoint_fingerprint=binding.endpoint_fingerprint,
                version=binding.version,
            ),
            name=value.name,
            exists=value.exists,
            config=tuple(config),
        )
    except Exception:
        raise _target_error(action, _CONNECTOR_RECOVERY_OBSERVATION_FAILED) from None


def _connector_reconstructed_checksum(
    *,
    action: OperationAction,
    observation: ManagedConnectorObservation,
    state: LocalState,
) -> str:
    """Reconstruct the prior managed artifact using only durable identity and live config."""
    evidence = action.connector_evidence
    try:
        if type(evidence) is not ConnectorActionEvidence or not observation.exists:
            raise ValueError
        identity = ResourceIdentity.parse(action.resource_id)
        config = observation.config_dict()
        connector_class = config.get("connector.class")
        topics = config.get("topics")
        if type(connector_class) is not str or type(topics) is not str:
            raise ValueError
        artifact = parse_compiled_connector_artifact(
            {
                "name": evidence.connector_name,
                "connector_class": connector_class,
                "topics": topics.split(","),
                "cluster": observation.binding.cluster_alias,
                "config": config,
                "ownership": ArtifactOwnership(
                    project=state.project,
                    owner_type="model",
                    owner_name=identity.logical_name,
                    mode="managed",
                ).to_dict(),
            }
        )
        return artifact_checksum(artifact.to_dict())
    except Exception:
        raise _target_error(action, _CONNECTOR_RECOVERY_OBSERVATION_FAILED) from None


def _observe_connector_recovery_target(
    *,
    action: OperationAction,
    target: _ConnectorRecoveryTarget,
    resolution: RecoveryResolution,
    raw_observation: object,
    prior_state: LocalState,
    candidate_resources: dict[str, ManagedResourceRecord],
) -> RecoveryTargetEvidence:
    """Classify one strict Connector observation already retained by the plan."""
    observation = _strict_connector_recovery_observation(
        action=action,
        value=raw_observation,
    )
    evidence = action.connector_evidence
    if type(evidence) is not ConnectorActionEvidence:  # pragma: no cover - preflight narrows this
        raise _target_error(action, _CONNECTOR_RECOVERY_INVALID)
    if (
        observation.binding != target.binding
        or observation.name != evidence.connector_name
    ):
        raise _target_error(action, _CONNECTOR_RECOVERY_OBSERVATION_FAILED)

    try:
        fingerprint = observation.fingerprint
    except Exception:
        raise _target_error(action, _CONNECTOR_RECOVERY_OBSERVATION_FAILED) from None
    if not observation.exists:
        if (
            fingerprint != evidence.desired.fingerprint
            or resolution == "rolled_back"
            or not target.mutation_started
        ):
            raise _target_error(action, "does not exactly match prior Connector surface")
        accepted_as: Literal["prior", "candidate"] = "candidate"
        candidate_resources.pop(action.resource_id, None)
    else:
        if fingerprint != evidence.current.fingerprint:
            raise _target_error(
                action,
                "matches neither exact current nor desired Connector surface",
            )
        checksum = _connector_reconstructed_checksum(
            action=action,
            observation=observation,
            state=prior_state,
        )
        if (
            checksum != evidence.prior_artifact_checksum
            or checksum != target.prior_record.artifact_checksum
        ):
            raise _target_error(action, "does not exactly match prior Connector state")
        accepted_as = "prior"

    return RecoveryTargetEvidence(
        action=action,
        presence="present" if observation.exists else "absent",
        accepted_as=accepted_as,
        fingerprint=fingerprint,
    )


def _connector_observation_map(
    plan: DeploymentPlan,
    actions: tuple[OperationAction, ...],
) -> dict[str, ManagedConnectorObservation]:
    """Require exactly one planner-retained observation per Connector target."""
    connector_action_ids = {
        action.resource_id
        for action in actions
        if ResourceIdentity.parse(action.resource_id).kind == "connector"
        and action.action == "delete"
    }
    raw_observations = getattr(plan, "connector_recovery_observations", ())
    if type(raw_observations) is not tuple:
        raise RecoveryObservationError(
            "Fresh Connector recovery observations must be an exact tuple"
        )

    observations: dict[str, ManagedConnectorObservation] = {}
    for entry in raw_observations:
        if (
            type(entry) is not ConnectorRecoveryObservation
            or type(entry.resource_id) is not str
            or type(entry.observation) is not ManagedConnectorObservation
        ):
            raise RecoveryObservationError(
                "Fresh Connector recovery observations are partial"
            )
        if entry.resource_id in observations:
            raise RecoveryObservationError(
                "Fresh Connector recovery observations contain a duplicate target"
            )
        observations[entry.resource_id] = entry.observation

    extras = set(observations) - connector_action_ids
    if extras:
        raise RecoveryObservationError(
            "Fresh Connector recovery observations contain an unrelated target"
        )
    missing = connector_action_ids - set(observations)
    if missing:
        missing_action = next(
            action for action in actions if action.resource_id in missing
        )
        raise _target_error(
            missing_action,
            "has no matching fresh Connector observation",
        )
    return observations


def _gateway_observation_map(
    plan: DeploymentPlan,
    actions: tuple[OperationAction, ...],
) -> dict[str, ManagedGatewayRuleObservation]:
    gateway_action_ids = {
        action.resource_id
        for action in actions
        if ResourceIdentity.parse(action.resource_id).kind == "gateway_rule"
    }
    raw_observations = getattr(plan, "gateway_recovery_observations", ())
    if not isinstance(raw_observations, tuple):
        raise RecoveryObservationError("Fresh Gateway recovery observations must be an exact tuple")

    observations: dict[str, ManagedGatewayRuleObservation] = {}
    for entry in raw_observations:
        identity = getattr(entry, "resource_id", None)
        observation = getattr(entry, "observation", None)
        if not isinstance(identity, str) or type(observation) is not ManagedGatewayRuleObservation:
            raise RecoveryObservationError("Fresh Gateway recovery observations are partial")
        if identity in observations:
            raise RecoveryObservationError(
                "Fresh Gateway recovery observations contain a duplicate target"
            )
        observations[identity] = observation

    extras = set(observations) - gateway_action_ids
    if extras:
        raise RecoveryObservationError(
            "Fresh Gateway recovery observations contain an unrelated target"
        )
    missing = gateway_action_ids - set(observations)
    if missing:
        missing_action = next(action for action in actions if action.resource_id in missing)
        raise _target_error(missing_action, "has no matching fresh Gateway observation")
    return observations


def _gateway_manifest_changes(
    plan: DeploymentPlan,
    state: LocalState,
) -> dict[str, object]:
    """Index only rules still present in the current manifest by logical owner."""
    result: dict[str, object] = {}
    for change in plan.gateway_changes:
        desired = getattr(change, "desired", None)
        if desired is None:
            continue
        if not isinstance(desired, GatewayRuleArtifact):
            raise RecoveryObservationError("Fresh Gateway manifest evidence is partial")
        ownership = ArtifactOwnership.from_dict(desired.ownership)
        if ownership is None or ownership.project != state.project:
            raise RecoveryObservationError("Fresh Gateway manifest ownership evidence is invalid")
        try:
            identity = resource_id(
                state.project,
                state.environment,
                "gateway_rule",
                ownership.owner_name,
            )
        except StateError as error:
            raise RecoveryObservationError(
                "Fresh Gateway manifest ownership identity is invalid"
            ) from error
        if identity in result:
            raise RecoveryObservationError(
                "Fresh Gateway manifest contains duplicate canonical ownership"
            )
        result[identity] = change
    return result


def _gateway_desired_record(
    *,
    action: OperationAction,
    change: object,
    observation: ManagedGatewayRuleObservation,
    state: LocalState,
) -> ManagedResourceRecord:
    evidence = action.gateway_evidence
    if evidence is None:  # pragma: no cover - preflight rejects this
        raise _target_error(action, "has no exact Gateway action evidence")
    desired = getattr(change, "desired", None)
    ownership = ArtifactOwnership.from_dict(getattr(desired, "ownership", None))
    if not isinstance(desired, GatewayRuleArtifact) or ownership is None:
        raise _target_error(action, "has no exact current Gateway manifest artifact")
    identity = ResourceIdentity.parse(action.resource_id)
    expected_adopted_ownership = ArtifactOwnership(
        project=state.project,
        owner_type="model",
        owner_name=identity.logical_name,
        mode="adopted",
    )
    if (
        ownership.project != state.project
        or ownership.owner_name != identity.logical_name
        or ownership.mode not in ("managed", "adopted")
        or desired.name != evidence.rule_name
        or desired.virtual_topic != evidence.alias_name
        or observation.logical_name != evidence.rule_name
        or observation.alias_name != evidence.alias_name
        or observation.binding.backend_identity != evidence.backend_identity
    ):
        raise _target_error(action, "has mismatched current Gateway manifest identity")
    if action.action == "adopt" and (
        ownership != expected_adopted_ownership or desired.interceptors != []
    ):
        raise _target_error(
            action,
            "requires exact adopted model ownership and no desired interceptors",
        )

    try:
        rebuilt = build_desired_gateway_rule(desired, observation.binding)
    except ValueError:
        raise _target_error(action, "has incoherent current Gateway manifest evidence") from None
    if action.action == "adopt":
        try:
            planned_current = getattr(change, "current", None)
            planned_desired = getattr(change, "desired_managed", None)
            backend_identity = getattr(change, "backend_identity", None)
            canonical = plan_managed_gateway_rule(
                desired,
                rebuilt,
                observation,
            )
            if (
                getattr(change, "current_alias", None) is not None
                or getattr(change, "current_interceptors", None) is not None
                or getattr(change, "action", None) not in {canonical.action, "none"}
                or getattr(change, "changes", None) != canonical.changes
            ):
                raise RecoveryObservationError
        except (RecoveryObservationError, ValueError):
            raise _target_error(
                action,
                "has incoherent current Gateway manifest evidence",
            ) from None
    else:
        try:
            normalized = _normalized_gateway_change(change)
        except RecoveryObservationError:
            raise _target_error(
                action,
                "has incoherent current Gateway manifest evidence",
            ) from None
        if normalized is None:
            raise _target_error(action, "has legacy current Gateway manifest evidence")
        planned_current, planned_desired, backend_identity = normalized
    if (
        backend_identity != evidence.backend_identity
        or planned_current != observation
        or planned_desired != rebuilt
        or _gateway_surface(rebuilt) != evidence.desired
    ):
        raise _target_error(action, "has mismatched current Gateway manifest evidence")
    try:
        return ManagedResourceRecord(
            physical_name=evidence.alias_name,
            ownership=ownership.mode,  # type: ignore[arg-type]
            artifact_checksum=artifact_checksum(desired.to_dict()),
            backend=evidence.backend_identity,
        )
    except (StateError, TypeError, ValueError):
        raise _target_error(action, "has malformed current Gateway manifest evidence") from None


def _observe_gateway_target(
    *,
    action: OperationAction,
    resolution: RecoveryResolution,
    observation: ManagedGatewayRuleObservation,
    manifest_changes: dict[str, object],
    prior_state: LocalState,
    candidate_resources: dict[str, ManagedResourceRecord],
) -> RecoveryTargetEvidence:
    evidence = action.gateway_evidence
    if evidence is None:  # pragma: no cover - preflight rejects this
        raise _target_error(action, "has no exact Gateway action evidence")
    if (
        observation.binding.backend_identity != evidence.backend_identity
        or observation.logical_name != evidence.rule_name
        or observation.alias_name != evidence.alias_name
    ):
        raise _target_error(action, "has mismatched fresh Gateway observation identity")

    current_surface = _gateway_surface(observation)
    accepted_as: Literal["prior", "candidate"]
    if action.action == "adopt":
        if current_surface != evidence.current:
            raise _target_error(
                action,
                "does not exactly match the reviewed Gateway adoption surface",
            )
        accepted_as = "prior" if resolution == "rolled_back" else "candidate"
    elif current_surface == evidence.current:
        accepted_as = "prior"
    elif current_surface == evidence.desired:
        if resolution == "rolled_back":
            raise _target_error(action, "does not exactly match prior Gateway surface")
        accepted_as = "candidate"
    else:
        raise _target_error(
            action,
            "matches neither exact current nor desired Gateway surface",
        )

    prior_record = prior_state.resources.get(action.resource_id)
    if action.action == "adopt" and prior_record is not None:
        raise _target_error(action, "requires absent prior Gateway ownership")
    if action.action in ("update", "delete") and prior_record is None:
        raise _target_error(action, "has no exact prior Gateway ownership record")
    if prior_record is not None and (
        prior_record.backend != evidence.backend_identity
        or prior_record.physical_name != evidence.alias_name
    ):
        raise _target_error(action, "has mismatched prior Gateway ownership evidence")
    if any(
        other_resource_id != action.resource_id
        and record.physical_name == evidence.alias_name
        and (
            record.backend == evidence.backend_identity
            or (action.action == "adopt" and record.backend == "conduktor-gateway")
        )
        for other_resource_id, record in prior_state.resources.items()
    ):
        raise _target_error(action, "has ambiguous prior Gateway alias ownership")

    manifest_change = manifest_changes.get(action.resource_id)
    if action.action == "delete":
        if manifest_change is not None:
            raise _target_error(action, "is still present in the current Gateway manifest")
        if accepted_as == "candidate":
            candidate_resources.pop(action.resource_id, None)
    else:
        if manifest_change is None:
            raise _target_error(action, "has no exact current Gateway manifest artifact")
        desired_record = _gateway_desired_record(
            action=action,
            change=manifest_change,
            observation=observation,
            state=prior_state,
        )
        if accepted_as == "candidate":
            candidate_resources[action.resource_id] = desired_record

    return RecoveryTargetEvidence(
        action=action,
        presence="present" if current_surface.exists else "absent",
        accepted_as=accepted_as,
        fingerprint=current_surface.fingerprint,
    )


@dataclass(frozen=True)
class DeploymentPlanRecoveryObserver:
    """Implement ``RecoveryTargetObserver`` using one already-fresh live plan.

    Constructing and invoking this object perform no provider calls. Connector and
    Gateway observations are retained on the already-fresh plan. The planner and
    plan must be bound to the supplied snapshot.
    """

    planner: DeploymentPlanner
    plan: DeploymentPlan

    def observe_recovery_targets(
        self,
        *,
        resolution: RecoveryResolution,
        snapshot: RecoverySnapshotEvidence,
    ) -> RecoveryLiveObservation:
        if resolution == "abandoned_before_mutation":
            raise RecoveryObservationError(
                "Abandoned-before-mutation recovery must not observe provider targets"
            )
        prior_state = snapshot.state
        actions = preflight_recovery_intent(snapshot)
        if (
            self.planner.project_name != prior_state.project
            or self.planner.environment != prior_state.environment
            or self.planner.prior_state != prior_state
        ):
            raise RecoveryObservationError(
                "Fresh recovery plan was not built against the recovery state snapshot"
            )
        connector_targets = _connector_recovery_targets(
            planner=self.planner,
            actions=actions,
            snapshot=snapshot,
        )
        connector_observations = _connector_observation_map(self.plan, actions)
        mapped_changes = _mapped_changes(self.plan, prior_state)
        gateway_observations = _gateway_observation_map(self.plan, actions)
        gateway_manifest_changes = _gateway_manifest_changes(self.plan, prior_state)
        try:
            managed_records = desired_managed_records(
                self.plan,
                project=prior_state.project,
                environment=prior_state.environment,
            )
        except StateError:
            raise RecoveryObservationError(
                "Fresh recovery plan has ambiguous desired ownership evidence"
            ) from None

        evidence: list[RecoveryTargetEvidence] = []
        candidate_resources = dict(prior_state.resources)
        for action in actions:
            identity = ResourceIdentity.parse(action.resource_id)
            kind = cast(_Kind, identity.kind)
            connector_target = connector_targets.get(action.resource_id)
            if connector_target is not None:
                evidence.append(
                    _observe_connector_recovery_target(
                        action=action,
                        target=connector_target,
                        resolution=resolution,
                        raw_observation=connector_observations[action.resource_id],
                        prior_state=prior_state,
                        candidate_resources=candidate_resources,
                    )
                )
                continue
            if kind == "gateway_rule":
                evidence.append(
                    _observe_gateway_target(
                        action=action,
                        resolution=resolution,
                        observation=gateway_observations[action.resource_id],
                        manifest_changes=gateway_manifest_changes,
                        prior_state=prior_state,
                        candidate_resources=candidate_resources,
                    )
                )
                continue

            mapped = mapped_changes.get(action.resource_id)
            if mapped is None:
                raise _target_error(action, "has no matching fresh observation")
            if mapped.kind != kind:
                raise _target_error(action, "has mismatched fresh observation kind")
            try:
                presence, normalized = _normalize_live(mapped)
                fingerprint = _canonical_fingerprint(normalized)
            except RecoveryObservationError as error:
                raise _target_error(action, str(error)) from None

            desired_record = _desired_record(
                action=action,
                mapped=mapped,
                state=prior_state,
                records=managed_records,
            )
            prior_record = prior_state.resources.get(action.resource_id)
            expected_backend = _expected_backend(mapped)
            if (
                kind == "connector"
                and prior_record is not None
                and prior_record.backend != expected_backend
            ):
                raise _target_error(
                    action,
                    "has legacy or mismatched prior backend evidence",
                )
            prior_matches = (
                presence == "absent"
                if prior_record is None
                else (
                    presence == "present"
                    and prior_record.physical_name == mapped.physical_name
                    and prior_record.backend == expected_backend
                    and _prior_artifact_checksum(
                        mapped=mapped,
                        normalized=normalized,
                        prior=prior_record,
                    )
                    == prior_record.artifact_checksum
                )
            )
            fresh_action = _effective_fresh_action(
                action=action,
                mapped=mapped,
                plan=self.plan,
            )
            candidate_matches = (
                presence == "absent"
                if desired_record is None
                else (
                    fresh_action == "none" and _live_matches_desired(mapped, presence, normalized)
                )
            )

            if prior_matches and candidate_matches and desired_record != prior_record:
                raise _target_error(action, "has ambiguous prior and candidate evidence")
            if resolution == "rolled_back":
                if not prior_matches:
                    raise _target_error(action, "does not exactly match prior state")
                accepted_as = "prior"
            elif prior_matches:
                accepted_as = "prior"
            elif candidate_matches:
                accepted_as = "candidate"
                if desired_record is None:
                    candidate_resources.pop(action.resource_id, None)
                else:
                    candidate_resources[action.resource_id] = desired_record
            else:
                raise _target_error(action, "matches neither prior nor candidate state")

            evidence.append(
                RecoveryTargetEvidence(
                    action=action,
                    presence=presence,  # type: ignore[arg-type]
                    accepted_as=accepted_as,  # type: ignore[arg-type]
                    fingerprint=fingerprint,
                )
            )

        if resolution == "rolled_back":
            candidate_state = None
        else:
            changed = candidate_resources != prior_state.resources
            candidate_state = LocalState(
                project=prior_state.project,
                environment=prior_state.environment,
                serial=prior_state.serial + (1 if changed else 0),
                resources=candidate_resources,
            )
        return RecoveryLiveObservation(
            targets=tuple(evidence),
            candidate_state=candidate_state,
        )
