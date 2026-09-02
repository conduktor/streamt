"""Deterministic, integrity-checked files for reviewed deployment plans."""

from __future__ import annotations

import hashlib
import hmac
import json
import math
import os
import re
import tempfile
from dataclasses import asdict, dataclass, is_dataclass
from enum import Enum
from pathlib import Path
from typing import Optional

from streamt import __version__
from streamt.compiler.manifest import Manifest
from streamt.deployer.planner import DeploymentPlan

PLAN_FILE_KIND = "streamt.reviewed-plan"
PLAN_FILE_VERSION = 2
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


def manifest_checksum(manifest: Manifest) -> str:
    """Hash deployable project content while excluding compilation time."""
    manifest_data = manifest.to_dict()
    manifest_data.pop("compiled_at", None)
    return _checksum(manifest_data)


def environment_fingerprint(runtime: object, environment: str) -> str:
    """Hash the non-secret runtime identity for environment freshness checks."""
    runtime_data = (
        runtime.model_dump(mode="json") if hasattr(runtime, "model_dump") else runtime
    )
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
    for connector_change in plan.connector_changes:
        add(
            "connector",
            connector_change.connector_name,
            connector_change.action,
            connector_change.changes,
        )
    for gateway_change in plan.gateway_changes:
        add(
            "gateway_rule",
            gateway_change.name,
            gateway_change.action,
            gateway_change.changes,
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

    return {
        "summary": {
            "creates": plan.creates,
            "updates": plan.updates,
            "deletes": plan.deletes,
            "has_changes": plan.has_changes,
            "ownership_requirements": len(ownership_requirements),
            "safety_blockers": len(safety_blockers),
            "is_apply_blocked": plan.is_apply_blocked,
        },
        "resources": resources,
        "impact": impact,
        "ownership_requirements": ownership_requirements,
        "safety_blockers": safety_blockers,
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
class ReviewedPlanFile:
    """Versioned envelope containing a deterministic reviewed-plan payload."""

    project: str
    environment: str
    environment_fingerprint: str
    manifest_checksum: str
    plan: dict[str, object]
    offline: bool = False
    selection: Optional[dict[str, str]] = None
    state_serial: Optional[int] = None
    streamt_version: str = __version__
    checksum: str = ""

    @classmethod
    def create(
        cls,
        deployment_plan: DeploymentPlan,
        manifest: Manifest,
        *,
        project: str,
        environment: str,
        runtime: object,
        offline: bool = False,
        selection: Optional[dict[str, str]] = None,
        state_serial: Optional[int] = None,
    ) -> ReviewedPlanFile:
        """Create and checksum a reviewed plan envelope."""
        plan_file = cls(
            project=project,
            environment=environment,
            environment_fingerprint=environment_fingerprint(runtime, environment),
            manifest_checksum=manifest_checksum(manifest),
            plan=deployment_plan_payload(deployment_plan),
            offline=offline,
            selection=selection,
            state_serial=state_serial,
        )
        return cls(**{**plan_file.__dict__, "checksum": _checksum(plan_file._unsigned_dict())})

    def _unsigned_dict(self) -> dict[str, object]:
        return {
            "kind": PLAN_FILE_KIND,
            "format_version": PLAN_FILE_VERSION,
            "streamt_version": self.streamt_version,
            "project": self.project,
            "environment": self.environment,
            "environment_fingerprint": self.environment_fingerprint,
            "manifest_checksum": self.manifest_checksum,
            "state_serial": self.state_serial,
            "selection": self.selection,
            "offline": self.offline,
            "plan": self.plan,
        }

    def to_dict(self) -> dict[str, object]:
        """Return the complete plan-file envelope including its checksum."""
        return {**self._unsigned_dict(), "checksum": self.checksum}

    def save(self, path: Path) -> None:
        """Atomically write a deterministic plan file."""
        path = path.resolve()
        path.parent.mkdir(parents=True, exist_ok=True)
        content = json.dumps(
            self.to_dict(), ensure_ascii=False, allow_nan=False, indent=2, sort_keys=True
        ) + "\n"
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

        expected_fields = {
            "kind",
            "format_version",
            "streamt_version",
            "project",
            "environment",
            "environment_fingerprint",
            "manifest_checksum",
            "state_serial",
            "selection",
            "offline",
            "plan",
            "checksum",
        }
        unknown = set(data) - expected_fields
        missing = expected_fields - set(data)
        if unknown:
            raise PlanFileError(f"Plan file has unknown field(s): {', '.join(sorted(unknown))}")
        if missing:
            raise PlanFileError(f"Plan file is missing field(s): {', '.join(sorted(missing))}")
        if data["kind"] != PLAN_FILE_KIND:
            raise PlanFileError(f"Unsupported plan file kind: {data['kind']!r}")
        if data["format_version"] != PLAN_FILE_VERSION:
            raise PlanFileError(
                f"Unsupported plan format version {data['format_version']!r}; "
                f"expected {PLAN_FILE_VERSION}"
            )
        if not isinstance(data["plan"], dict):
            raise PlanFileError("Plan file 'plan' field must be an object")
        if not isinstance(data["checksum"], str):
            raise PlanFileError("Plan file checksum must be a string")

        unsigned = {key: value for key, value in data.items() if key != "checksum"}
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
        if data["state_serial"] is not None and (
            not isinstance(data["state_serial"], int) or isinstance(data["state_serial"], bool)
        ):
            raise PlanFileError("Plan file 'state_serial' field must be an integer or null")

        return cls(
            project=data["project"],
            environment=data["environment"],
            environment_fingerprint=data["environment_fingerprint"],
            manifest_checksum=data["manifest_checksum"],
            plan=data["plan"],
            offline=data["offline"],
            selection=data["selection"],
            state_serial=data["state_serial"],
            streamt_version=data["streamt_version"],
            checksum=data["checksum"],
        )

    def verify_context(
        self,
        manifest: Manifest,
        *,
        project: str,
        environment: str,
        runtime: object,
        state_serial: Optional[int] = None,
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
        if self.state_serial != state_serial:
            raise StalePlanError(
                f"Plan state serial {self.state_serial!r} does not match current state serial "
                f"{state_serial!r}"
            )

    def verify_current_plan(self, current_plan: DeploymentPlan) -> None:
        """Reject live-state drift that changes reviewed resource actions or diffs."""
        current_payload = deployment_plan_payload(current_plan)
        reviewed_live_state = {
            "resources": self.plan.get("resources"),
            "impact": _impact_drift_payload(self.plan.get("impact")),
            "ownership_requirements": self.plan.get("ownership_requirements"),
            "safety_blockers": self.plan.get("safety_blockers"),
        }
        current_live_state = {
            "resources": current_payload["resources"],
            "impact": _impact_drift_payload(current_payload["impact"]),
            "ownership_requirements": current_payload["ownership_requirements"],
            "safety_blockers": current_payload["safety_blockers"],
        }
        if canonical_json(reviewed_live_state) != canonical_json(current_live_state):
            raise StalePlanError(
                "Reviewed plan is stale; live resource actions, diffs, impact evidence, "
                "ownership requirements, or safety blockers changed after planning"
            )
