"""Deterministic, integrity-checked files for reviewed deployment plans."""

from __future__ import annotations

import hashlib
import hmac
import json
import math
import os
import re
import tempfile
import uuid
from dataclasses import asdict, dataclass, field, is_dataclass, replace
from enum import Enum
from pathlib import Path
from typing import Optional, cast

import streamt.core.manifest_identity as _manifest_identity
from streamt import __version__
from streamt.compiler.manifest import Manifest
from streamt.deployer.connect import secret_neutral_connector_changes
from streamt.deployer.gateway import secret_neutral_gateway_changes
from streamt.deployer.kafka_streams_evidence import (
    KAFKA_STREAMS_CONTROL_VERSION,
    KafkaStreamsActionEvidence,
)
from streamt.deployer.planner import DeploymentPlan
from streamt.deployer.state import ResourceIdentity, StateError
from streamt.deployer.state_backend import (
    CURRENT_CONTROL_VERSION,
    OperationAction,
    StateAddress,
    StateObservation,
    state_checksum,
)

PLAN_FILE_KIND = "streamt.reviewed-plan"
PLAN_FILE_VERSION = 5
KAFKA_STREAMS_PLAN_FILE_VERSION = 6
_MAX_PLAN_FILE_BYTES = 10 * 1024 * 1024
_SENSITIVE_KEY = re.compile(
    r"(^|[._-])(?:password|passwd|secret|token|api[_-]?key|authorization|credentials?"
    r"|basic[._-]auth[._-]user[._-]info|sasl[._-]jaas[._-]config)($|[._-])",
    re.IGNORECASE,
)
_CREDENTIAL_URL = re.compile(r"://([^:@/\s]+):([^@/\s]+)@")
_INLINE_SENSITIVE_KV = re.compile(
    r"(password|passwd|secret|token|api[_-]?key|apikey)\s*[=:]\s*\S+",
    re.IGNORECASE,
)
_INLINE_SENSITIVE_AUTH = re.compile(
    r"(authorization|bearer)\s*[=:]\s*\S+(?:\s+\S+)?",
    re.IGNORECASE,
)
_BACKEND_KIND = re.compile(r"^[a-z][a-z0-9_-]*$")
_STATE_CHECKSUM = re.compile(r"^sha256:[0-9a-f]{64}$")


class PlanFileError(ValueError):
    """A reviewed plan file is invalid, stale, or does not match the project."""


class StalePlanError(PlanFileError):
    """A valid reviewed plan no longer matches project or live state."""


def _jsonable(value: object) -> object:
    """Convert supported model values to deterministic JSON-compatible values."""
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise PlanFileError("Plan data contains a non-finite number")
        return value
    if isinstance(value, Enum):
        return _jsonable(value.value)
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, dict):
        result: dict[str, object] = {}
        for key, item in value.items():
            if not isinstance(key, str):
                raise PlanFileError("Plan data contains a non-string object key")
            if _SENSITIVE_KEY.search(key):
                result[key] = "<redacted>"
            else:
                result[key] = _jsonable(item)
        return result
    if isinstance(value, (list, tuple)):
        return [_jsonable(item) for item in value]
    if isinstance(value, (set, frozenset)):
        normalized = [_jsonable(item) for item in value]
        return sorted(normalized, key=canonical_json)
    if is_dataclass(value) and not isinstance(value, type):
        return _jsonable(asdict(value))
    if hasattr(value, "model_dump"):
        return _jsonable(value.model_dump(mode="json"))
    raise PlanFileError(f"Plan data contains unsupported value type: {type(value).__name__}")


def _redact_inline_credentials(value: object) -> object:
    """Redact credentials embedded in otherwise non-sensitive string fields."""
    normalized = _jsonable(value)
    if isinstance(normalized, str):
        normalized = _INLINE_SENSITIVE_KV.sub(r"\1=<redacted>", normalized)
        normalized = _INLINE_SENSITIVE_AUTH.sub(r"\1=<redacted>", normalized)
        return _CREDENTIAL_URL.sub(r"://<redacted>:<redacted>@", normalized)
    if isinstance(normalized, list):
        return [_redact_inline_credentials(item) for item in normalized]
    if isinstance(normalized, dict):
        return {key: _redact_inline_credentials(item) for key, item in normalized.items()}
    return normalized


def canonical_json(value: object) -> str:
    """Return the canonical JSON representation used by all plan checksums."""
    return json.dumps(
        _jsonable(value),
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    )


def _checksum(value: object) -> str:
    digest = hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()
    return f"sha256:{digest}"


def _replacement_action(actions: tuple[OperationAction, ...]) -> OperationAction:
    """A v6 plan cannot authorize unrelated or additional state/runtime actions."""
    if (
        type(actions) is not tuple or len(actions) != 1
        or type(actions[0]) is not OperationAction or actions[0].index != 0
        or actions[0].action != "update"
        or type(actions[0].kafka_streams_evidence) is not KafkaStreamsActionEvidence
    ):
        raise PlanFileError("Reviewed plan format 6 requires one evidenced Kafka Streams update")
    return actions[0]


def _reviewed_checksum(
    unsigned: dict[str, object], *, format_version: int,
    actions: tuple[OperationAction, ...],
) -> str:
    if format_version == PLAN_FILE_VERSION:
        return _checksum(unsigned)
    if format_version != KAFKA_STREAMS_PLAN_FILE_VERSION:
        raise PlanFileError("Unsupported reviewed plan checksum version")
    action = _replacement_action(actions)
    # This narrow raw digest accepts ONLY the strict typed action. In addition
    # to the non-secret volume UUID, valid data schemas can contain keys such
    # as "token"; passing their preimages through generic credential redaction
    # would erase integrity. Keep all generic redaction/checksum paths intact.
    # The full action, including reviewed offsets, is bound here; its immutable
    # runtime fingerprint deliberately excludes those moving lower bounds.
    encoded = json.dumps(
        action.to_dict(control_version=KAFKA_STREAMS_CONTROL_VERSION), ensure_ascii=False, allow_nan=False,
        sort_keys=True, separators=(",", ":"),
    ).encode("utf-8")
    return _checksum({
        "domain": "streamt.reviewed-plan.v6",
        "envelope": unsigned,
        "typed_action_sha256": "sha256:" + hashlib.sha256(encoded).hexdigest(),
    })


def _validate_replacement_payload(
    plan: dict[str, object], actions: tuple[OperationAction, ...],
) -> None:
    """Validate the displayed v6 mutation set against its sole typed authority."""
    action = _replacement_action(actions)
    evidence = action.kafka_streams_evidence
    assert evidence is not None
    if type(plan) is not dict:
        raise PlanFileError("Reviewed replacement requires a complete plan payload")
    if evidence.progress.active_members != 1:
        raise PlanFileError("Reviewed replacement requires a running single-member group")
    resources, summary = plan.get("resources"), plan.get("summary")
    if type(resources) is not list or type(summary) is not dict:
        raise PlanFileError("Reviewed replacement requires complete resource and summary evidence")
    for field_name in ("safety_blockers", "connector_removal_assessments", "gateway_removal_assessments"):
        if plan.get(field_name) != [] or type(summary.get(field_name)) is not int or summary[field_name] != 0:
            raise PlanFileError("Reviewed replacement cannot contain blockers or removal assessments")
    requirements = plan.get("ownership_requirements")
    if type(requirements) is not list or any(
        type(item) is not dict or item.get("reason") != "external"
        or item.get("observed_action") != "none" or item.get("ownership_mode") != "external"
        for item in requirements
    ):
        raise PlanFileError("Reviewed replacement cannot require another ownership transition")
    if (
        type(summary.get("ownership_requirements")) is not int
        or summary["ownership_requirements"] != len(requirements)
        or summary.get("has_changes") is not True or summary.get("is_apply_blocked") is not False
        or any(type(summary.get(key)) is not int or summary[key] != expected
               for key, expected in (("creates", 0), ("updates", 1), ("deletes", 0)))
    ):
        raise PlanFileError("Reviewed replacement summary does not match its sole update")
    mutations: list[dict[str, object]] = []
    for resource in resources:
        if (
            type(resource) is not dict or set(resource) != {"kind", "name", "action", "changes"}
            or resource.get("kind") not in ("schema", "topic", "flink_job", "kafka_streams_job", "connector", "gateway_rule")
            or type(resource.get("name")) is not str or not resource["name"]
            or resource.get("action") not in ("none", "update")
            or type(resource.get("changes")) is not dict
        ):
            raise PlanFileError("Reviewed replacement contains another or malformed resource action")
        if resource["action"] != "none":
            mutations.append(resource)
    if len(mutations) != 1 or (mutations[0]["kind"], mutations[0]["name"]) != (
        "kafka_streams_job", evidence.desired_artifact.artifact.name,
    ):
        raise PlanFileError("Reviewed replacement resource does not match its typed action")
    changes = cast(dict[str, object], mutations[0]["changes"])
    current = {
        "exists": True, "container_id": evidence.prior_container_id, "status": "running",
        "artifact_hash": evidence.prior_artifact.checksum,
        "plan_hash": evidence.prior_artifact.plan_hash, "image_id": evidence.image_id,
        "input_topic_id": evidence.progress.input_topic_id,
        "output_topic_id": evidence.progress.output_topic_id, "network_id": evidence.network_id,
    }
    artifact = evidence.desired_artifact.artifact
    expected = {
        "application_id": evidence.application_id, "image_id": evidence.image_id,
        "topic_bindings": {
            str(artifact.plan["input_topic"]): evidence.progress.input_topic_id,
            str(artifact.plan["output_topic"]): evidence.progress.output_topic_id,
        },
        "initial_offset": artifact.initial_offset, "network_id": evidence.network_id,
        "desired_artifact_hash": evidence.desired_artifact.checksum,
        "backend_identity": evidence.backend_identity, "blocker": None, "current": current,
    }
    if (
        type(changes.get("current")) is not dict
        or cast(dict[str, object], changes["current"]).get("exists") is not True
        or any(key not in changes or changes[key] != value for key, value in expected.items())
    ):
        raise PlanFileError("Reviewed replacement displayed runtime differs from its typed evidence")


def manifest_checksum(manifest: Manifest) -> str:
    """Retain the deployer-facing PlanFileError compatibility boundary."""
    try:
        return _manifest_identity.manifest_checksum(manifest)
    except _manifest_identity.ManifestIdentityError:
        raise PlanFileError("Plan manifest content could not be checksummed") from None


def environment_fingerprint(runtime: object, environment: str) -> str:
    """Hash the non-secret runtime identity for environment freshness checks."""
    runtime_data = runtime.model_dump(mode="json") if hasattr(runtime, "model_dump") else runtime
    return _checksum(
        {
            "environment": environment,
            "runtime": _redact_inline_credentials(runtime_data),
        }
    )


def deployment_plan_payload(plan: DeploymentPlan) -> dict[str, object]:
    """Build the stable, secret-safe payload reviewed by plan-file consumers."""
    resources: list[dict[str, object]] = []

    def add(kind: str, name: str, action: str, changes: object = None) -> None:
        resources.append(
            {
                "kind": kind,
                "name": name,
                "action": action,
                "changes": _redact_inline_credentials(changes or {}),
            }
        )

    for schema_change in plan.schema_changes:
        add(
            "schema",
            schema_change.subject,
            schema_change.action,
            schema_change.changes,
        )
    for topic_change in plan.topic_changes:
        add("topic", topic_change.topic, topic_change.action, topic_change.changes)
    for flink_change in plan.flink_changes:
        add("flink_job", flink_change.job_name, flink_change.action)
    for runner_change in plan.kafka_streams_changes:
        current = runner_change.current
        add("kafka_streams_job", runner_change.job_name, runner_change.action, {
            **runner_change.changes,
            "backend_identity": runner_change.backend_identity,
            "blocker": runner_change.blocker,
            "current": None if current is None else {
                "exists": current.exists,
                "container_id": current.container_id,
                "status": current.status,
                "artifact_hash": current.artifact_hash,
                "plan_hash": current.plan_hash,
                "image_id": current.image_id,
                "input_topic_id": current.input_topic_id,
                "output_topic_id": current.output_topic_id,
                "network_id": current.network_id,
            },
        })
    for connector_change in plan.connector_changes:
        add(
            "connector",
            connector_change.connector_name,
            connector_change.action,
            secret_neutral_connector_changes(connector_change.changes),
        )
    for gateway_change in plan.gateway_changes:
        add(
            "gateway_rule",
            gateway_change.name,
            gateway_change.action,
            secret_neutral_gateway_changes(gateway_change.changes),
        )

    resources.sort(key=lambda item: (str(item["kind"]), str(item["name"]), str(item["action"])))
    impact: list[dict[str, object]] = []
    for entry in plan.impact_radius:
        normalized_exposures = sorted(
            (_redact_inline_credentials(exposure) for exposure in entry.exposures),
            key=canonical_json,
        )
        normalized_consumers = sorted(
            (_redact_inline_credentials(consumer) for consumer in entry.consumers),
            key=canonical_json,
        )
        normalized_owners = _redact_inline_credentials(sorted(entry.owners))
        identity_evidence = _redact_inline_credentials(entry.identity_evidence)
        graph_evidence = _redact_inline_credentials(entry.graph_evidence)
        consumer_evidence = _redact_inline_credentials(entry.consumer_evidence)
        impact.append(
            {
                "resource": entry.resource,
                "logical_type": entry.logical_type,
                "logical_name": entry.logical_name,
                "logical_resource": entry.logical_resource,
                "change_type": entry.change_type,
                "downstream_models": sorted(entry.downstream_models),
                "exposures": normalized_exposures,
                "owners": normalized_owners,
                "consumers": normalized_consumers,
                "identity_evidence": identity_evidence,
                "graph_evidence": graph_evidence,
                "consumer_evidence": consumer_evidence,
            }
        )
    impact.sort(key=lambda item: (str(item["resource"]), str(item["change_type"])))
    ownership_requirements = [
        {
            "resource_id": requirement.resource_id,
            "kind": requirement.kind,
            "logical_name": requirement.logical_name,
            "physical_name": requirement.physical_name,
            "reason": requirement.reason,
            "observed_action": requirement.observed_action,
            "ownership_mode": requirement.ownership_mode,
            "message": requirement.message,
        }
        for requirement in plan.ownership_requirements
    ]
    ownership_requirements.sort(
        key=lambda item: (
            str(item["resource_id"]),
            str(item["reason"]),
            str(item["observed_action"]),
        )
    )
    safety_blockers: list[dict[str, object]] = []
    for blocker in plan.ordered_safety_blockers:
        normalized = _redact_inline_credentials(blocker.to_dict())
        if not isinstance(normalized, dict):  # pragma: no cover - to_dict is fixed
            raise PlanFileError("Safety blocker payload must be an object")
        safety_blockers.append(normalized)

    connector_removal_assessments: list[dict[str, object]] = []
    for connector_assessment in plan.connector_removal_assessments:
        normalized = _redact_inline_credentials(connector_assessment.to_dict())
        if not isinstance(normalized, dict):  # pragma: no cover - to_dict is fixed
            raise PlanFileError("Connector removal assessment payload must be an object")
        connector_removal_assessments.append(normalized)

    gateway_removal_assessments: list[dict[str, object]] = []
    for gateway_assessment in plan.gateway_removal_assessments:
        normalized = _redact_inline_credentials(gateway_assessment.to_dict())
        if not isinstance(normalized, dict):  # pragma: no cover - to_dict is fixed
            raise PlanFileError("Gateway removal assessment payload must be an object")
        gateway_removal_assessments.append(normalized)

    change_risks: list[dict[str, object]] = []
    for risk in plan.ordered_change_risks:
        normalized = _redact_inline_credentials(risk.to_dict())
        if not isinstance(normalized, dict):  # pragma: no cover - to_dict is fixed
            raise PlanFileError("Change-risk payload must be an object")
        change_risks.append(normalized)
    risk_summary = _redact_inline_credentials(plan.risk_summary)
    if not isinstance(risk_summary, dict):  # pragma: no cover - property is fixed
        raise PlanFileError("Plan risk summary must be an object")

    return {
        "summary": {
            "creates": plan.creates,
            "updates": plan.updates,
            "deletes": plan.deletes,
            "has_changes": plan.has_changes,
            "ownership_requirements": len(ownership_requirements),
            "safety_blockers": len(safety_blockers),
            "connector_removal_assessments": len(connector_removal_assessments),
            "gateway_removal_assessments": len(gateway_removal_assessments),
            "is_apply_blocked": plan.is_apply_blocked,
            "risk": risk_summary,
        },
        "resources": resources,
        "impact": impact,
        "change_risks": change_risks,
        "risk_summary": risk_summary,
        "ownership_requirements": ownership_requirements,
        "safety_blockers": safety_blockers,
        "connector_removal_assessments": connector_removal_assessments,
        "gateway_removal_assessments": gateway_removal_assessments,
    }


def _impact_drift_payload(value: object) -> object:
    """Normalize impact evidence for drift checks while excluding volatile lag metrics."""
    normalized = _jsonable(value)
    if not isinstance(normalized, list):
        return normalized
    result: list[object] = []
    for entry in normalized:
        if not isinstance(entry, dict):
            result.append(entry)
            continue
        stable_entry = dict(entry)
        consumers = stable_entry.get("consumers")
        if isinstance(consumers, list):
            stable_consumers: list[object] = []
            for consumer in consumers:
                if isinstance(consumer, dict):
                    stable_consumer = dict(consumer)
                    stable_consumer.pop("lag", None)
                    stable_consumers.append(stable_consumer)
                else:
                    stable_consumers.append(consumer)
            stable_entry["consumers"] = stable_consumers
        result.append(stable_entry)
    return result


@dataclass(frozen=True)
class StateReference:
    """Portable exact-state evidence embedded in an online reviewed plan."""

    backend: str
    store_id: str
    address: str
    serial: int
    checksum: str

    def __post_init__(self) -> None:
        if not isinstance(self.backend, str) or not _BACKEND_KIND.fullmatch(self.backend):
            raise PlanFileError("Plan state backend must be a lowercase backend identifier")
        if not isinstance(self.store_id, str):
            raise PlanFileError("Plan state store_id must be a canonical UUID")
        try:
            parsed_store_id = uuid.UUID(self.store_id)
        except (ValueError, AttributeError) as error:
            raise PlanFileError("Plan state store_id must be a canonical UUID") from error
        if str(parsed_store_id) != self.store_id:
            raise PlanFileError("Plan state store_id must be a canonical UUID")
        try:
            StateAddress.parse(self.address)
        except StateError as error:
            raise PlanFileError(f"Plan state address is invalid: {error}") from error
        if type(self.serial) is not int or self.serial < 0:
            raise PlanFileError("Plan state serial must be a non-negative integer")
        if not isinstance(self.checksum, str) or not _STATE_CHECKSUM.fullmatch(self.checksum):
            raise PlanFileError("Plan state checksum must be a sha256:<64 lowercase hex> value")

    @property
    def parsed_address(self) -> StateAddress:
        """Return the already validated canonical address."""
        return StateAddress.parse(self.address)

    def to_dict(self) -> dict[str, object]:
        return {
            "backend": self.backend,
            "store_id": self.store_id,
            "address": self.address,
            "serial": self.serial,
            "checksum": self.checksum,
        }

    @classmethod
    def from_dict(cls, value: object) -> StateReference:
        if not isinstance(value, dict):
            raise PlanFileError("Plan file 'state' field must be an object or null")
        expected = {"backend", "store_id", "address", "serial", "checksum"}
        unknown = set(value) - expected
        missing = expected - set(value)
        if unknown:
            raise PlanFileError(f"Plan state has unknown field(s): {', '.join(sorted(unknown))}")
        if missing:
            raise PlanFileError(f"Plan state is missing field(s): {', '.join(sorted(missing))}")
        return cls(
            backend=value["backend"],
            store_id=value["store_id"],
            address=value["address"],
            serial=value["serial"],
            checksum=value["checksum"],
        )

    @classmethod
    def from_observation(cls, observation: StateObservation) -> StateReference:
        """Create portable evidence without exposing the provider revision."""
        return cls(
            backend=observation.store.backend,
            store_id=observation.store.store_id,
            address=observation.address.uri,
            serial=observation.state_serial,
            checksum=state_checksum(observation.state),
        )


@dataclass(frozen=True)
class ReviewedPlanFile:
    """Versioned envelope containing a deterministic reviewed-plan payload."""

    project: str
    environment: str
    environment_fingerprint: str
    manifest_checksum: str
    plan: dict[str, object]
    state: Optional[StateReference]
    actions: tuple[OperationAction, ...]
    offline: bool = False
    selection: Optional[dict[str, str]] = None
    streamt_version: str = __version__
    checksum: str = ""
    format_version: int = field(default=PLAN_FILE_VERSION, kw_only=True)

    def __post_init__(self) -> None:
        if type(self.format_version) is not int or self.format_version not in (
            PLAN_FILE_VERSION, KAFKA_STREAMS_PLAN_FILE_VERSION,
        ):
            raise PlanFileError("Unsupported reviewed plan format version")
        if type(self.actions) is not tuple or any(
            type(action) is not OperationAction for action in self.actions
        ):
            raise PlanFileError("Reviewed plan actions must be an exact immutable action tuple")
        if [action.index for action in self.actions] != list(range(len(self.actions))):
            raise PlanFileError("Reviewed plan action indexes must be contiguous from zero")
        resource_ids = [action.resource_id for action in self.actions]
        if len(set(resource_ids)) != len(resource_ids):
            raise PlanFileError("Reviewed plan actions contain a duplicate resource identity")
        try:
            action_identities = tuple(
                ResourceIdentity.parse(resource_id) for resource_id in resource_ids
            )
        except StateError:
            raise PlanFileError(
                "Reviewed plan action has an invalid canonical resource identity"
            ) from None
        if any(
            identity.project != self.project or identity.environment != self.environment
            for identity in action_identities
        ):
            raise PlanFileError("Reviewed plan action belongs to another project environment")
        if any(
            identity.kind == "gateway_rule"
            and action.action in ("create", "update", "delete")
            and action.gateway_evidence is None
            for identity, action in zip(action_identities, self.actions, strict=True)
        ):
            raise PlanFileError("Reviewed plan Gateway mutations require exact action evidence")
        if any(
            identity.kind == "connector"
            and action.action == "delete"
            and action.connector_evidence is None
            for identity, action in zip(action_identities, self.actions, strict=True)
        ):
            raise PlanFileError("Reviewed plan Connector deletions require exact action evidence")
        if self.offline and self.actions:
            raise PlanFileError("Offline reviewed plans must not contain actions")
        if self.offline and self.state is not None:
            raise PlanFileError("Offline reviewed plans must encode state as null")
        if not self.offline and self.state is None:
            raise PlanFileError("Online reviewed plans require an exact ownership-state reference")
        if self.state is not None:
            address = self.state.parsed_address
            if address.project != self.project or address.environment != self.environment:
                raise PlanFileError(
                    "Plan state address does not match the plan project and environment"
                )
        if self.format_version == KAFKA_STREAMS_PLAN_FILE_VERSION:
            if self.offline is not False or self.selection is not None:
                raise PlanFileError("Reviewed replacement requires an online full unselected plan")
            _validate_replacement_payload(self.plan, self.actions)
        elif any(action.kafka_streams_evidence is not None for action in self.actions):
            raise PlanFileError("Kafka Streams replacement requires reviewed plan format 6")

    @property
    def state_serial(self) -> Optional[int]:
        """Retain the existing CLI-facing serial accessor."""
        return self.state.serial if self.state is not None else None

    @classmethod
    def create(
        cls,
        deployment_plan: DeploymentPlan,
        manifest: Manifest,
        *,
        project: str,
        environment: str,
        runtime: object,
        state: Optional[StateReference],
        actions: tuple[OperationAction, ...],
        offline: bool = False,
        selection: Optional[dict[str, str]] = None,
    ) -> ReviewedPlanFile:
        """Create and checksum a reviewed plan envelope."""
        plan_file = cls(
            project=project,
            environment=environment,
            environment_fingerprint=environment_fingerprint(runtime, environment),
            manifest_checksum=manifest_checksum(manifest),
            plan=deployment_plan_payload(deployment_plan),
            state=state,
            actions=actions,
            offline=offline,
            selection=selection,
            format_version=(
                KAFKA_STREAMS_PLAN_FILE_VERSION
                if type(actions) is tuple and any(
                    type(action) is OperationAction and action.kafka_streams_evidence is not None
                    for action in actions
                ) else PLAN_FILE_VERSION
            ),
        )
        return replace(plan_file, checksum=_reviewed_checksum(
            plan_file._unsigned_dict(), format_version=plan_file.format_version,
            actions=plan_file.actions,
        ))

    def _unsigned_dict(self) -> dict[str, object]:
        return {
            "kind": PLAN_FILE_KIND,
            "format_version": self.format_version,
            "streamt_version": self.streamt_version,
            "project": self.project,
            "environment": self.environment,
            "environment_fingerprint": self.environment_fingerprint,
            "manifest_checksum": self.manifest_checksum,
            "state": self.state.to_dict() if self.state is not None else None,
            "selection": self.selection,
            "offline": self.offline,
            "plan": self.plan,
            "actions": [
                action.to_dict(control_version=(
                    KAFKA_STREAMS_CONTROL_VERSION if self.format_version == KAFKA_STREAMS_PLAN_FILE_VERSION
                    else CURRENT_CONTROL_VERSION
                )) for action in self.actions
            ],
        }

    def to_dict(self) -> dict[str, object]:
        """Return the complete plan-file envelope including its checksum."""
        return {**self._unsigned_dict(), "checksum": self.checksum}

    def save(self, path: Path) -> None:
        """Atomically write a deterministic plan file."""
        path = path.resolve()
        path.parent.mkdir(parents=True, exist_ok=True)
        content = (
            json.dumps(
                self.to_dict(), ensure_ascii=False, allow_nan=False, indent=2, sort_keys=True
            )
            + "\n"
        )
        temp_path: Optional[Path] = None
        try:
            with tempfile.NamedTemporaryFile(
                mode="w",
                encoding="utf-8",
                dir=path.parent,
                prefix=f".{path.name}.",
                suffix=".tmp",
                delete=False,
            ) as temp_file:
                temp_path = Path(temp_file.name)
                temp_file.write(content)
                temp_file.flush()
                os.fsync(temp_file.fileno())
            os.replace(temp_path, path)
        finally:
            if temp_path and temp_path.exists():
                temp_path.unlink()

    @classmethod
    def load(cls, path: Path) -> ReviewedPlanFile:
        """Load a plan file and reject malformed or modified content."""
        try:
            if path.stat().st_size > _MAX_PLAN_FILE_BYTES:
                raise PlanFileError("Plan file exceeds the 10 MiB size limit")
            raw = path.read_text(encoding="utf-8")
        except OSError as error:
            raise PlanFileError(f"Cannot read plan file '{path}': {error}") from error

        def reject_duplicates(pairs: list[tuple[str, object]]) -> dict[str, object]:
            result: dict[str, object] = {}
            for key, value in pairs:
                if key in result:
                    raise PlanFileError(f"Plan file contains duplicate field '{key}'")
                result[key] = value
            return result

        try:
            data = json.loads(raw, object_pairs_hook=reject_duplicates)
        except PlanFileError:
            raise
        except (json.JSONDecodeError, UnicodeError) as error:
            raise PlanFileError(f"Plan file is not valid UTF-8 JSON: {error}") from error
        if not isinstance(data, dict):
            raise PlanFileError("Plan file root must be a JSON object")

        if data.get("kind") != PLAN_FILE_KIND:
            raise PlanFileError(f"Unsupported plan file kind: {data.get('kind')!r}")
        format_version = data.get("format_version")
        if type(format_version) is int and format_version in (1, 2, 3, 4):
            raise PlanFileError(
                f"Reviewed plan format version {format_version} predates exact reviewed "
                "action binding and cannot authorize apply; regenerate it with the current "
                "'streamt plan --out <path>' command"
            )
        is_replacement = type(format_version) is int and format_version == KAFKA_STREAMS_PLAN_FILE_VERSION
        if format_version != PLAN_FILE_VERSION and not is_replacement:
            raise PlanFileError(
                f"Unsupported plan format version {format_version!r}; expected {PLAN_FILE_VERSION} "
                f"or {KAFKA_STREAMS_PLAN_FILE_VERSION}"
            )

        expected_fields = {
            "kind",
            "format_version",
            "streamt_version",
            "project",
            "environment",
            "environment_fingerprint",
            "manifest_checksum",
            "state",
            "selection",
            "offline",
            "plan",
            "actions",
            "checksum",
        }
        unknown = set(data) - expected_fields
        missing = expected_fields - set(data)
        if unknown:
            raise PlanFileError(f"Plan file has unknown field(s): {', '.join(sorted(unknown))}")
        if missing:
            raise PlanFileError(f"Plan file is missing field(s): {', '.join(sorted(missing))}")
        if not isinstance(data["plan"], dict):
            raise PlanFileError("Plan file 'plan' field must be an object")
        if not isinstance(data["actions"], list):
            raise PlanFileError("Plan file 'actions' field must be an array")
        if not isinstance(data["checksum"], str):
            raise PlanFileError("Plan file checksum must be a string")

        unsigned = {key: value for key, value in data.items() if key != "checksum"}
        replacement_actions: tuple[OperationAction, ...] | None = None
        if is_replacement:
            # Raw digest exceptions are available only after closed-schema
            # parsing. Arbitrary JSON never bypasses generic secret redaction.
            try:
                replacement_actions = tuple(
                    OperationAction.from_dict(action, control_version=KAFKA_STREAMS_CONTROL_VERSION)
                    for action in data["actions"]
                )
            except StateError as error:
                raise PlanFileError(f"Plan file action is invalid: {error}") from error
            actual_checksum = _reviewed_checksum(
                unsigned, format_version=KAFKA_STREAMS_PLAN_FILE_VERSION,
                actions=replacement_actions,
            )
        else:
            actual_checksum = _checksum(unsigned)
        if not hmac.compare_digest(data["checksum"], actual_checksum):
            raise PlanFileError(
                "Plan file checksum mismatch; the file was modified or is incomplete"
            )

        string_fields = (
            "streamt_version",
            "project",
            "environment",
            "environment_fingerprint",
            "manifest_checksum",
        )
        if any(not isinstance(data[field], str) or not data[field] for field in string_fields):
            raise PlanFileError("Plan file contains an invalid required string field")
        if not isinstance(data["offline"], bool):
            raise PlanFileError("Plan file 'offline' field must be boolean")
        if data["selection"] is not None and not isinstance(data["selection"], dict):
            raise PlanFileError("Plan file 'selection' field must be an object or null")
        state = None if data["state"] is None else StateReference.from_dict(data["state"])
        try:
            actions = replacement_actions if replacement_actions is not None else tuple(
                OperationAction.from_dict(
                    action,
                    control_version=CURRENT_CONTROL_VERSION,
                )
                for action in data["actions"]
            )
        except StateError as error:
            raise PlanFileError(f"Plan file action is invalid: {error}") from error

        return cls(
            project=data["project"],
            environment=data["environment"],
            environment_fingerprint=data["environment_fingerprint"],
            manifest_checksum=data["manifest_checksum"],
            plan=data["plan"],
            state=state,
            actions=actions,
            offline=data["offline"],
            selection=data["selection"],
            streamt_version=data["streamt_version"],
            checksum=data["checksum"],
            format_version=KAFKA_STREAMS_PLAN_FILE_VERSION if is_replacement else PLAN_FILE_VERSION,
        )

    def verify_context(
        self,
        manifest: Manifest,
        *,
        project: str,
        environment: str,
        runtime: object,
        state_observation: Optional[StateObservation],
    ) -> None:
        """Reject a plan created for different or changed project content."""
        if self.project != project:
            raise StalePlanError(
                f"Plan project '{self.project}' does not match current project '{project}'"
            )
        if self.environment != environment:
            raise StalePlanError(
                f"Plan environment '{self.environment}' does not match '{environment}'"
            )
        current_environment = environment_fingerprint(runtime, environment)
        if self.environment_fingerprint != current_environment:
            raise StalePlanError(
                "Plan environment fingerprint is stale; runtime endpoints or settings changed"
            )
        current_manifest = manifest_checksum(manifest)
        if self.manifest_checksum != current_manifest:
            raise StalePlanError(
                "Plan manifest checksum is stale; project content changed after planning"
            )
        self.verify_state(state_observation)

    def verify_state(
        self,
        state_observation: Optional[StateObservation],
    ) -> None:
        """Reject any backend, address, serial, or content drift."""
        current = (
            StateReference.from_observation(state_observation)
            if state_observation is not None
            else None
        )
        if self.state is None:
            if current is not None:
                raise StalePlanError(
                    "Offline plan state does not match an online ownership-state observation"
                )
            return
        if current is None:
            raise StalePlanError(
                "Reviewed plan requires an authoritative ownership-state observation"
            )
        if self.state.backend != current.backend:
            raise StalePlanError(
                f"Plan state backend {self.state.backend!r} does not match current backend "
                f"{current.backend!r}"
            )
        if self.state.store_id != current.store_id:
            raise StalePlanError("Plan state store does not match the current backend instance")
        if self.state.address != current.address:
            raise StalePlanError(
                f"Plan state address {self.state.address!r} does not match current address "
                f"{current.address!r}"
            )
        if self.state.serial != current.serial:
            raise StalePlanError(
                f"Plan state serial {self.state.serial!r} does not match current state serial "
                f"{current.serial!r}"
            )
        if self.state.checksum != current.checksum:
            raise StalePlanError(
                "Plan state checksum does not match current ownership-state content"
            )

    def verify_current_plan(
        self,
        current_plan: DeploymentPlan,
        *,
        actions: tuple[OperationAction, ...],
        state_observation: Optional[StateObservation],
    ) -> None:
        """Reject live-state drift that changes reviewed resource actions or diffs."""
        self.verify_state(state_observation)
        if type(actions) is not tuple or any(
            type(action) is not OperationAction for action in actions
        ):
            raise StalePlanError("Current planned actions are not an exact immutable action tuple")
        if self.format_version == KAFKA_STREAMS_PLAN_FILE_VERSION:
            try:
                reviewed_evidence = _replacement_action(self.actions).kafka_streams_evidence
                current_action = _replacement_action(actions)
                current_evidence = current_action.kafka_streams_evidence
                assert reviewed_evidence is not None
                assert current_evidence is not None
                current_evidence.progress.require_at_least(reviewed_evidence.progress)
                if current_evidence.progress.active_members != 1:
                    raise PlanFileError("Current replacement group is not a single-member group")
                # Exact equality outside moving progress retains every old/new
                # preimage and identity. require_at_least separately protects
                # cluster/topic/partition identities inside progress.
                comparable = replace(current_action, kafka_streams_evidence=replace(
                    current_evidence, progress=reviewed_evidence.progress,
                ))
                if (comparable,) != self.actions:
                    raise PlanFileError("Replacement immutable action evidence changed")
            except (StateError, PlanFileError) as error:
                raise StalePlanError(
                    "Reviewed replacement is stale; exact action identity or progress changed"
                ) from error
        elif self.actions != actions:
            raise StalePlanError(
                "Reviewed plan is stale; ordered action identity or evidence changed after planning"
            )
        current_payload = deployment_plan_payload(current_plan)
        if self.format_version == KAFKA_STREAMS_PLAN_FILE_VERSION:
            try:
                _validate_replacement_payload(self.plan, self.actions)
                _validate_replacement_payload(current_payload, actions)
            except PlanFileError as error:
                raise StalePlanError("Reviewed replacement no longer has its sole safe update") from error
        reviewed_live_state = {
            "resources": self.plan.get("resources"),
            "impact": _impact_drift_payload(self.plan.get("impact")),
            "change_risks": self.plan.get("change_risks"),
            "risk_summary": self.plan.get("risk_summary"),
            "ownership_requirements": self.plan.get("ownership_requirements"),
            "safety_blockers": self.plan.get("safety_blockers"),
            "connector_removal_assessments": self.plan.get("connector_removal_assessments"),
            "gateway_removal_assessments": self.plan.get("gateway_removal_assessments"),
        }
        current_live_state = {
            "resources": current_payload["resources"],
            "impact": _impact_drift_payload(current_payload["impact"]),
            "change_risks": current_payload["change_risks"],
            "risk_summary": current_payload["risk_summary"],
            "ownership_requirements": current_payload["ownership_requirements"],
            "safety_blockers": current_payload["safety_blockers"],
            "connector_removal_assessments": current_payload["connector_removal_assessments"],
            "gateway_removal_assessments": current_payload["gateway_removal_assessments"],
        }
        if canonical_json(reviewed_live_state) != canonical_json(current_live_state):
            raise StalePlanError(
                "Reviewed plan is stale; live resource actions, diffs, impact evidence, "
                "risk classification, ownership requirements, safety blockers, or "
                "Connector or Gateway removal assessments changed after planning"
            )

    def bind_current_actions(
        self,
        current_plan: DeploymentPlan,
        *,
        actions: tuple[OperationAction, ...],
        state_observation: Optional[StateObservation],
    ) -> tuple[OperationAction, ...]:
        """Verify freshness and return reviewed, never rebased, journal authority.

        The caller must use this tuple in its durable OperationIntent. A newer
        progress observation validates the approved lower bounds; it must not
        overwrite them or detach the journal from its reviewed-plan checksum.
        This method does not observe providers or activate a runtime action.
        """
        self.verify_current_plan(current_plan, actions=actions, state_observation=state_observation)
        return self.actions
