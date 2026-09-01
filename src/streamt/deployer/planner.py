"""Deployment planner for streamt projects."""

from __future__ import annotations

import logging
import re
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Optional, Protocol

from streamt.compiler.manifest import ArtifactOwnership, Manifest
from streamt.deployer.connect import ConnectDeployer, ConnectorChange
from streamt.deployer.flink import FlinkDeployer, FlinkJobChange
from streamt.deployer.gateway import GatewayDeployer, GatewayRuleChange
from streamt.deployer.kafka import KafkaDeployer, TopicChange
from streamt.deployer.schema_registry import SchemaChange, SchemaRegistryDeployer
from streamt.deployer.state import LocalState, StateIdentityError, resource_id

logger = logging.getLogger(__name__)


class _PlannedChange(Protocol):
    """Common mutable action carried by backend-specific change records."""

    action: str

_SENSITIVE_KV = re.compile(
    r"(password|passwd|secret|token|api_key|apikey)\s*[=:]\s*\S+",
    re.IGNORECASE,
)
_SENSITIVE_AUTH = re.compile(
    r"(authorization|bearer)\s*[=:]\s*\S+(?:\s+\S+)?",
    re.IGNORECASE,
)
_SENSITIVE_URL = re.compile(
    r"://([^:@/\s]+):([^@/\s]+)@",
)


def _sanitize_error(msg: str) -> str:
    """Strip credentials/tokens from error messages."""
    result = _SENSITIVE_KV.sub(r"\1=***", str(msg))
    result = _SENSITIVE_AUTH.sub(r"\1=***", result)
    return _SENSITIVE_URL.sub(r"://***:***@", result)


@dataclass
class ImpactEntry:
    """An entry in the impact radius for a planned change."""

    resource: str
    change_type: str
    downstream_models: list[str] = field(default_factory=list)
    consumers: list[dict] = field(default_factory=list)


@dataclass(frozen=True)
class OwnershipRequirement:
    """A resource that cannot be mutated without an ownership decision."""

    resource_id: str
    kind: str
    logical_name: str
    physical_name: str
    reason: str
    observed_action: str
    ownership_mode: str
    message: str

    def to_dict(self) -> dict[str, str]:
        """Return a stable machine-readable representation."""
        return {
            "resource_id": self.resource_id,
            "kind": self.kind,
            "logical_name": self.logical_name,
            "physical_name": self.physical_name,
            "reason": self.reason,
            "observed_action": self.observed_action,
            "ownership_mode": self.ownership_mode,
            "message": self.message,
        }


@dataclass(frozen=True)
class SafetyBlocker:
    """A deterministic policy decision that forbids applying one unsafe change."""

    code: str
    kind: str
    resource: str
    action: str
    message: str
    details: dict[str, object] = field(default_factory=dict)

    def to_dict(self) -> dict[str, object]:
        """Return a stable machine-readable representation."""
        return {
            "code": self.code,
            "kind": self.kind,
            "resource": self.resource,
            "action": self.action,
            "message": self.message,
            "details": dict(self.details),
        }


_SAFETY_KIND_ORDER = {"schema": 0, "topic": 1, "flink_job": 2}


def _safety_blocker_sort_key(blocker: SafetyBlocker) -> tuple[int, str, str, str]:
    """Order blockers in backend apply order, then by stable resource identity."""
    return (
        _SAFETY_KIND_ORDER.get(blocker.kind, 99),
        blocker.resource,
        blocker.code,
        blocker.action,
    )


@dataclass
class DeploymentPlan:
    """A deployment plan."""

    schema_changes: list[SchemaChange] = field(default_factory=list)
    topic_changes: list[TopicChange] = field(default_factory=list)
    flink_changes: list[FlinkJobChange] = field(default_factory=list)
    connector_changes: list[ConnectorChange] = field(default_factory=list)
    gateway_changes: list[GatewayRuleChange] = field(default_factory=list)
    impact_radius: list[ImpactEntry] = field(default_factory=list)
    ownership_requirements: list[OwnershipRequirement] = field(default_factory=list)
    safety_blockers: list[SafetyBlocker] = field(default_factory=list)

    def __post_init__(self) -> None:
        """Derive blockers for plans constructed with their changes up front."""
        if not self.safety_blockers:
            self.refresh_safety_blockers()

    def refresh_safety_blockers(self) -> None:
        """Rebuild blockers from final effective backend actions."""
        blockers: list[SafetyBlocker] = []

        for change in self.schema_changes:
            changes = change.changes or {}
            incompatible = changes.get("schema_incompatible")
            if change.action != "update" or incompatible is None:
                continue
            current = change.current
            desired = change.desired
            details: dict[str, object] = {}
            if isinstance(incompatible, dict):
                current_version = incompatible.get("current_version")
                if current_version is not None:
                    details["current_version"] = current_version
            compatibility = (
                getattr(current, "compatibility", None)
                or getattr(desired, "compatibility", None)
            )
            if compatibility is not None:
                details["compatibility"] = compatibility
            blockers.append(
                SafetyBlocker(
                    code="schema_incompatible",
                    kind="schema",
                    resource=change.subject,
                    action=change.action,
                    message=(
                        "Schema is incompatible with the subject's configured "
                        "compatibility policy; apply is blocked."
                    ),
                    details=details,
                )
            )

        for change in self.topic_changes:
            changes = change.changes or {}
            if change.action != "update" or "partitions_error" not in changes:
                continue
            details = {"field": "partitions"}
            current_partitions = getattr(change.current, "partitions", None)
            desired_partitions = getattr(change.desired, "partitions", None)
            if current_partitions is not None:
                details["current"] = current_partitions
            if desired_partitions is not None:
                details["desired"] = desired_partitions
            blockers.append(
                SafetyBlocker(
                    code="kafka_partition_reduction",
                    kind="topic",
                    resource=change.topic,
                    action=change.action,
                    message="Kafka topic partitions cannot be reduced; apply is blocked.",
                    details=details,
                )
            )

        for change in self.flink_changes:
            if change.action != "update":
                continue
            details = {}
            current_status = getattr(change.current, "status", None)
            if current_status is not None:
                details["current_status"] = current_status
            blockers.append(
                SafetyBlocker(
                    code="flink_update_requires_savepoint",
                    kind="flink_job",
                    resource=change.job_name,
                    action=change.action,
                    message=(
                        "Flink job updates are blocked until a savepoint-safe or "
                        "explicitly stateless upgrade workflow is implemented."
                    ),
                    details=details,
                )
            )

        self.safety_blockers = sorted(blockers, key=_safety_blocker_sort_key)

    @property
    def ordered_safety_blockers(self) -> list[SafetyBlocker]:
        """Return blockers in their canonical review and execution order."""
        return sorted(self.safety_blockers, key=_safety_blocker_sort_key)

    @property
    def has_changes(self) -> bool:
        """Check if there are any changes."""
        return (
            any(c.action != "none" for c in self.schema_changes)
            or any(c.action != "none" for c in self.topic_changes)
            or any(c.action != "none" for c in self.flink_changes)
            or any(c.action != "none" for c in self.connector_changes)
            or any(c.action != "none" for c in self.gateway_changes)
        )

    @property
    def creates(self) -> int:
        """Count of resources to create."""
        return (
            sum(1 for c in self.schema_changes if c.action == "register")
            + sum(1 for c in self.topic_changes if c.action == "create")
            + sum(1 for c in self.flink_changes if c.action == "submit")
            + sum(1 for c in self.connector_changes if c.action == "create")
            + sum(1 for c in self.gateway_changes if c.action == "create")
        )

    @property
    def updates(self) -> int:
        """Count of resources to update."""
        return (
            sum(1 for c in self.schema_changes if c.action == "update")
            + sum(1 for c in self.topic_changes if c.action == "update")
            + sum(1 for c in self.flink_changes if c.action == "update")
            + sum(1 for c in self.connector_changes if c.action == "update")
            + sum(1 for c in self.gateway_changes if c.action == "update")
        )

    @property
    def deletes(self) -> int:
        """Count of resources to delete."""
        return (
            sum(1 for c in self.schema_changes if c.action == "delete")
            + sum(1 for c in self.topic_changes if c.action == "delete")
            + sum(1 for c in self.flink_changes if c.action == "cancel")
            + sum(1 for c in self.connector_changes if c.action == "delete")
            + sum(1 for c in self.gateway_changes if c.action == "delete")
        )

    @property
    def has_ownership_requirements(self) -> bool:
        """Whether explicit ownership decisions are required before mutation."""
        return bool(self.ownership_requirements)

    @property
    def blocking_ownership_requirements(self) -> list[OwnershipRequirement]:
        """Requirements that must block apply until ownership is resolved.

        ``external`` is an explicit observe-only decision, so it neutralizes
        that resource without preventing unrelated managed creates or updates.
        Every other ownership requirement is an apply blocker.
        """
        return [
            requirement
            for requirement in self.ownership_requirements
            if requirement.reason != "external"
        ]

    @property
    def is_apply_blocked(self) -> bool:
        """Whether apply must refuse this plan for ownership or safety policy."""
        return bool(self.blocking_ownership_requirements or self.safety_blockers)

    def summary(self) -> str:
        """Get a summary of the plan."""
        summary = (
            f"Plan: {self.creates} to create, {self.updates} to update, "
            f"{self.deletes} to delete"
        )
        if self.ownership_requirements:
            summary += f", {len(self.ownership_requirements)} ownership requirement(s)"
        if self.safety_blockers:
            summary += f", {len(self.safety_blockers)} safety blocker(s)"
        return summary

    def details(self, color: bool = True) -> str:
        """Get detailed plan output with colored diff markers."""
        lines = [self.summary(), ""]

        def _add(prefix: str) -> str:
            return f"[green]{prefix}[/green]" if color else prefix

        def _upd(prefix: str) -> str:
            return f"[yellow]{prefix}[/yellow]" if color else prefix

        def _rm(prefix: str) -> str:
            return f"[red]{prefix}[/red]" if color else prefix

        for change in self.schema_changes:
            if change.action == "register":
                lines.append(_add(f"+ schema: {change.subject}"))
                if change.desired:
                    lines.append(f"    type: {change.desired.schema_type}")
            elif change.action == "update":
                lines.append(_upd(f"~ schema: {change.subject}"))
                for key, val in (change.changes or {}).items():
                    if key == "schema":
                        lines.append(f"    version: {val['from_version']} -> {val['to_version']}")
                    elif key == "compatibility":
                        lines.append(f"    compatibility: {val['from']} -> {val['to']}")
                    elif key == "schema_incompatible":
                        message = (
                            val.get("message", "schema is incompatible")
                            if isinstance(val, dict)
                            else "schema is incompatible"
                        )
                        lines.append(f"    blocked: {message}")
            elif change.action == "delete":
                lines.append(_rm(f"- schema: {change.subject}"))

        for change in self.topic_changes:
            if change.action == "create":
                lines.append(_add(f"+ topic: {change.topic}"))
                if change.desired:
                    lines.append(f"    partitions: {change.desired.partitions}")
                    lines.append(f"    replication_factor: {change.desired.replication_factor}")
            elif change.action == "update":
                lines.append(_upd(f"~ topic: {change.topic}"))
                for key, val in (change.changes or {}).items():
                    if key == "partitions_error":
                        message = (
                            val.get("message", "partitions cannot be reduced")
                            if isinstance(val, dict)
                            else "partitions cannot be reduced"
                        )
                        lines.append(f"    blocked: {message}")
                    elif isinstance(val, dict) and "from" in val and "to" in val:
                        lines.append(f"    {key}: {val['from']} -> {val['to']}")
            elif change.action == "delete":
                lines.append(_rm(f"- topic: {change.topic}"))

        for change in self.flink_changes:
            if change.action == "submit":
                lines.append(_add(f"+ flink_job: {change.job_name}"))
            elif change.action == "update":
                lines.append(_upd(f"~ flink_job: {change.job_name}"))
            elif change.action == "cancel":
                lines.append(_rm(f"- flink_job: {change.job_name}"))

        for change in self.connector_changes:
            if change.action == "create":
                lines.append(_add(f"+ connector: {change.connector_name}"))
            elif change.action == "update":
                lines.append(_upd(f"~ connector: {change.connector_name}"))
                for key, val in (change.changes or {}).items():
                    lines.append(f"    {key}: {val['from']} -> {val['to']}")
            elif change.action == "delete":
                lines.append(_rm(f"- connector: {change.connector_name}"))

        for change in self.gateway_changes:
            if change.action == "create":
                lines.append(_add(f"+ gateway_rule: {change.name}"))
            elif change.action == "update":
                lines.append(_upd(f"~ gateway_rule: {change.name}"))
                for key, val in (change.changes or {}).items():
                    lines.append(f"    {key}: {val['from']} -> {val['to']}")
            elif change.action == "delete":
                lines.append(_rm(f"- gateway_rule: {change.name}"))

        if not self.has_changes:
            lines.append("No changes detected.")

        if self.ownership_requirements:
            lines.append("")
            lines.append("Ownership Requirements:")
            for requirement in self.ownership_requirements:
                lines.append(f"  ! {requirement.kind}: {requirement.logical_name}")
                lines.append(f"    {requirement.message}")
                lines.append(f"    resource_id: {requirement.resource_id}")

        if self.safety_blockers:
            lines.append("")
            lines.append("Safety Blockers:")
            for blocker in self.ordered_safety_blockers:
                lines.append(f"  ! [{blocker.code}] {blocker.kind}: {blocker.resource}")
                lines.append(f"    {blocker.message}")

        if self.impact_radius:
            lines.append("")
            lines.append("Impact Analysis:")
            for entry in self.impact_radius:
                lines.append(f"  {entry.resource} ({entry.change_type})")
                if entry.downstream_models:
                    lines.append(f"    downstream: {', '.join(entry.downstream_models)}")
                if entry.consumers:
                    for c in entry.consumers:
                        declared = "" if c.get("declared", True) else " [undeclared]"
                        lines.append(
                            f"    consumer: {c['group_id']}{declared} lag={c.get('lag', 0)}"
                        )

        return "\n".join(lines)


class DeploymentPlanner:
    """Plans and executes deployments."""

    def __init__(
        self,
        manifest: Manifest,
        schema_registry_deployer: Optional[SchemaRegistryDeployer] = None,
        kafka_deployer: Optional[KafkaDeployer] = None,
        flink_deployer: Optional[FlinkDeployer] = None,
        connect_deployer: Optional[ConnectDeployer] = None,
        gateway_deployer: Optional[GatewayDeployer] = None,
        project: Optional[object] = None,
        prior_state: Optional[LocalState] = None,
        project_name: Optional[str] = None,
        environment: Optional[str] = None,
    ) -> None:
        """Initialize deployment planner."""
        self.manifest = manifest
        self.schema_registry_deployer = schema_registry_deployer
        self.kafka_deployer = kafka_deployer
        self.flink_deployer = flink_deployer
        self.connect_deployer = connect_deployer
        self.gateway_deployer = gateway_deployer
        self.project = project
        self.prior_state = prior_state
        self.project_name = project_name or manifest.project_name
        self.environment = environment or (prior_state.environment if prior_state else "default")
        if prior_state and prior_state.project != self.project_name:
            raise StateIdentityError(
                f"prior state belongs to project {prior_state.project!r}, "
                f"expected {self.project_name!r}"
            )
        if prior_state and prior_state.environment != self.environment:
            raise StateIdentityError(
                f"prior state belongs to environment {prior_state.environment!r}, "
                f"expected {self.environment!r}"
            )

    @staticmethod
    def _resource_exists(
        current: object,
        observed_action: str,
        create_actions: frozenset[str],
    ) -> bool:
        """Determine existence without confusing Flink re-submission with creation."""
        if current is not None and hasattr(current, "exists"):
            return bool(current.exists)  # type: ignore[attr-defined]
        return observed_action not in create_actions

    def _apply_ownership_policy(
        self,
        plan: DeploymentPlan,
        *,
        kind: str,
        logical_name: str,
        physical_name: str,
        ownership: ArtifactOwnership | dict[str, str] | None,
        change: _PlannedChange,
        current: object = None,
        create_actions: frozenset[str],
    ) -> None:
        """Neutralize changes that lack explicit authority over a live resource."""
        observed_action = str(change.action)
        parsed_ownership = ArtifactOwnership.from_dict(ownership)
        ownership_mode = parsed_ownership.mode if parsed_ownership else "managed"
        if parsed_ownership:
            logical_name = parsed_ownership.owner_name
        resource_uri = resource_id(
            self.project_name,
            self.environment,
            kind,
            logical_name,
        )

        reason: str | None = None
        message: str | None = None
        if parsed_ownership and parsed_ownership.project != self.project_name:
            reason = "ownership_mismatch"
            message = (
                f"Artifact declares project {parsed_ownership.project!r}, but this plan is for "
                f"{self.project_name!r}; mutation is blocked."
            )
        elif ownership_mode == "external":
            reason = "external"
            message = "Resource is declared external and is observe-only."
        elif ownership_mode not in ("managed", "adopted"):
            reason = "invalid_ownership"
            message = f"Unknown ownership mode {ownership_mode!r}; mutation is blocked."
        else:
            prior_record = (
                self.prior_state.resources.get(resource_uri) if self.prior_state else None
            )
            exists = self._resource_exists(current, observed_action, create_actions)
            if ownership_mode == "adopted" and prior_record is None:
                reason = "requires_adoption"
                message = (
                    "Declaring ownership.mode 'adopted' does not grant authority; "
                    "matching persisted ownership state is required before mutation."
                )
            elif exists:
                if prior_record is None:
                    reason = "requires_adoption"
                    message = (
                        "Live resource has no matching prior streamt ownership; "
                        "explicit adoption is required before mutation."
                    )
                elif prior_record.physical_name != physical_name:
                    reason = "state_mismatch"
                    message = (
                        f"Prior ownership points to {prior_record.physical_name!r}, not "
                        f"{physical_name!r}; explicit ownership reconciliation is required."
                    )

        if reason is None or message is None:
            return

        change.action = "none"
        plan.ownership_requirements.append(
            OwnershipRequirement(
                resource_id=resource_uri,
                kind=kind,
                logical_name=logical_name,
                physical_name=physical_name,
                reason=reason,
                observed_action=observed_action,
                ownership_mode=ownership_mode,
                message=message,
            )
        )

    def offline_plan(self) -> DeploymentPlan:
        """Create a plan assuming no current state (all creates).

        Useful when infrastructure is unavailable — shows what a fresh
        deployment would look like without connecting to Kafka/SR/Flink.
        """
        from streamt.compiler.manifest import (
            ConnectorArtifact,
            FlinkJobArtifact,
            GatewayRuleArtifact,
            TopicArtifact,
        )
        from streamt.deployer.schema_registry import SchemaArtifact as SRArtifact

        plan = DeploymentPlan()

        for schema_data in self.manifest.artifacts.get("schemas", []):
            try:
                artifact = SRArtifact(
                    subject=schema_data["subject"],
                    schema=schema_data["schema"],
                    schema_type=schema_data.get("schema_type", "AVRO"),
                    compatibility=schema_data.get("compatibility"),
                    ownership=ArtifactOwnership.from_dict(schema_data.get("ownership")),
                )
                change = SchemaChange(
                    subject=artifact.subject,
                    action="register",
                    desired=artifact,
                )
                self._apply_ownership_policy(
                    plan,
                    kind="schema",
                    logical_name=artifact.subject,
                    physical_name=artifact.subject,
                    ownership=artifact.ownership,
                    change=change,
                    create_actions=frozenset({"register"}),
                )
                plan.schema_changes.append(change)
            except KeyError:
                pass

        for topic_data in self.manifest.artifacts.get("topics", []):
            try:
                artifact = TopicArtifact(**topic_data)
                change = TopicChange(topic=artifact.name, action="create", desired=artifact)
                self._apply_ownership_policy(
                    plan,
                    kind="topic",
                    logical_name=artifact.name,
                    physical_name=artifact.name,
                    ownership=artifact.ownership,
                    change=change,
                    create_actions=frozenset({"create"}),
                )
                plan.topic_changes.append(change)
            except (KeyError, TypeError):
                pass

        for job_data in self.manifest.artifacts.get("flink_jobs", []):
            try:
                artifact = FlinkJobArtifact(**job_data)
                change = FlinkJobChange(
                    job_name=artifact.name,
                    action="submit",
                    desired=artifact,
                )
                self._apply_ownership_policy(
                    plan,
                    kind="flink_job",
                    logical_name=artifact.name,
                    physical_name=artifact.name,
                    ownership=artifact.ownership,
                    change=change,
                    create_actions=frozenset({"submit"}),
                )
                plan.flink_changes.append(change)
            except (KeyError, TypeError):
                pass

        for conn_data in self.manifest.artifacts.get("connectors", []):
            try:
                cfg = conn_data.get("config", {})
                artifact = ConnectorArtifact(
                    name=conn_data["name"],
                    connector_class=cfg.get("connector.class", ""),
                    topics=cfg.get("topics", "").split(","),
                    config={k: v for k, v in cfg.items() if k not in ["name", "connector.class", "topics"]},
                    ownership=ArtifactOwnership.from_dict(conn_data.get("ownership")),
                )
                change = ConnectorChange(
                    connector_name=artifact.name,
                    action="create",
                    desired=artifact,
                )
                self._apply_ownership_policy(
                    plan,
                    kind="connector",
                    logical_name=artifact.name,
                    physical_name=artifact.name,
                    ownership=artifact.ownership,
                    change=change,
                    create_actions=frozenset({"create"}),
                )
                plan.connector_changes.append(change)
            except KeyError:
                pass

        for rule_data in self.manifest.artifacts.get("gateway_rules", []):
            try:
                artifact = GatewayRuleArtifact(
                    name=rule_data["name"],
                    virtual_topic=rule_data["virtualTopic"],
                    physical_topic=rule_data["physicalTopic"],
                    interceptors=rule_data.get("interceptors", []),
                    ownership=ArtifactOwnership.from_dict(rule_data.get("ownership")),
                )
                change = GatewayRuleChange(
                    name=artifact.name,
                    action="create",
                    desired=artifact,
                )
                self._apply_ownership_policy(
                    plan,
                    kind="gateway_rule",
                    logical_name=artifact.name,
                    physical_name=artifact.virtual_topic,
                    ownership=artifact.ownership,
                    change=change,
                    create_actions=frozenset({"create"}),
                )
                plan.gateway_changes.append(change)
            except KeyError:
                pass

        plan.refresh_safety_blockers()
        return plan

    def plan(self) -> DeploymentPlan:
        """Create a deployment plan."""
        plan = DeploymentPlan()

        # Plan schemas first (before topics that may depend on them)
        if self.schema_registry_deployer:
            from streamt.deployer.schema_registry import SchemaArtifact as SRArtifact

            for schema_data in self.manifest.artifacts.get("schemas", []):
                try:
                    artifact = SRArtifact(
                        subject=schema_data["subject"],
                        schema=schema_data["schema"],
                        schema_type=schema_data.get("schema_type", "AVRO"),
                        compatibility=schema_data.get("compatibility"),
                        ownership=ArtifactOwnership.from_dict(schema_data.get("ownership")),
                    )
                    change = self.schema_registry_deployer.plan_schema(artifact)
                    self._apply_ownership_policy(
                        plan,
                        kind="schema",
                        logical_name=artifact.subject,
                        physical_name=artifact.subject,
                        ownership=artifact.ownership,
                        change=change,
                        current=change.current,
                        create_actions=frozenset({"register"}),
                    )
                    plan.schema_changes.append(change)
                except KeyError as e:
                    logger.error("Malformed schema artifact, missing key %s: %s", e, schema_data)

        # Plan topics
        if self.kafka_deployer:
            from streamt.compiler.manifest import TopicArtifact

            for topic_data in self.manifest.artifacts.get("topics", []):
                try:
                    artifact = TopicArtifact(**topic_data)
                    change = self.kafka_deployer.plan_topic(artifact)
                    self._apply_ownership_policy(
                        plan,
                        kind="topic",
                        logical_name=artifact.name,
                        physical_name=artifact.name,
                        ownership=artifact.ownership,
                        change=change,
                        current=change.current,
                        create_actions=frozenset({"create"}),
                    )
                    plan.topic_changes.append(change)
                except (KeyError, TypeError) as e:
                    logger.error("Malformed topic artifact: %s in %s", e, topic_data)

        # Plan Flink jobs
        if self.flink_deployer:
            from streamt.compiler.manifest import FlinkJobArtifact

            for job_data in self.manifest.artifacts.get("flink_jobs", []):
                try:
                    artifact = FlinkJobArtifact(**job_data)
                    change = self.flink_deployer.plan_job(artifact)
                    self._apply_ownership_policy(
                        plan,
                        kind="flink_job",
                        logical_name=artifact.name,
                        physical_name=artifact.name,
                        ownership=artifact.ownership,
                        change=change,
                        current=change.current,
                        create_actions=frozenset({"submit"}),
                    )
                    plan.flink_changes.append(change)
                except (KeyError, TypeError) as e:
                    logger.error("Malformed flink_job artifact: %s in %s", e, job_data)

        # Plan connectors
        if self.connect_deployer:
            from streamt.compiler.manifest import ConnectorArtifact

            for conn_data in self.manifest.artifacts.get("connectors", []):
                try:
                    cfg = conn_data.get("config", {})
                    artifact = ConnectorArtifact(
                        name=conn_data["name"],
                        connector_class=cfg.get("connector.class", ""),
                        topics=cfg.get("topics", "").split(","),
                        config={
                            k: v
                            for k, v in cfg.items()
                            if k not in ["name", "connector.class", "topics"]
                        },
                        ownership=ArtifactOwnership.from_dict(conn_data.get("ownership")),
                    )
                    change = self.connect_deployer.plan_connector(artifact)
                    self._apply_ownership_policy(
                        plan,
                        kind="connector",
                        logical_name=artifact.name,
                        physical_name=artifact.name,
                        ownership=artifact.ownership,
                        change=change,
                        current=change.current,
                        create_actions=frozenset({"create"}),
                    )
                    plan.connector_changes.append(change)
                except KeyError as e:
                    logger.error("Malformed connector artifact, missing key %s: %s", e, conn_data)

        # Plan gateway rules
        if self.gateway_deployer:
            from streamt.compiler.manifest import GatewayRuleArtifact

            for rule_data in self.manifest.artifacts.get("gateway_rules", []):
                try:
                    artifact = GatewayRuleArtifact(
                        name=rule_data["name"],
                        virtual_topic=rule_data["virtualTopic"],
                        physical_topic=rule_data["physicalTopic"],
                        interceptors=rule_data.get("interceptors", []),
                        ownership=ArtifactOwnership.from_dict(rule_data.get("ownership")),
                    )
                    change = self.gateway_deployer.plan(artifact)
                    self._apply_ownership_policy(
                        plan,
                        kind="gateway_rule",
                        logical_name=artifact.name,
                        physical_name=artifact.virtual_topic,
                        ownership=artifact.ownership,
                        change=change,
                        current=change.current_alias,
                        create_actions=frozenset({"create"}),
                    )
                    plan.gateway_changes.append(change)
                except KeyError as e:
                    logger.error(
                        "Malformed gateway_rule artifact, missing key %s: %s", e, rule_data
                    )

        # Compute impact radius for planned changes
        plan.refresh_safety_blockers()
        self._compute_impact_radius(plan)

        return plan

    def _compute_impact_radius(self, plan: DeploymentPlan) -> None:
        """Compute impact_radius for all planned topic creates/updates."""
        if not self.project:
            return

        # Find topics being created or updated
        changed_topics = [c.topic for c in plan.topic_changes if c.action in ("create", "update")]
        if not changed_topics:
            return

        # Build DAG to find downstream models
        try:
            from streamt.core.dag import DAGBuilder

            dag = DAGBuilder(self.project).build()  # type: ignore[arg-type]
        except Exception as e:
            logger.debug("Could not build DAG for impact analysis: %s", e)
            return

        # Declared consumer groups from project exposures
        declared_groups: set[str] = set()
        for exposure in getattr(self.project, "exposures", []):
            cg = getattr(exposure, "consumer_group", None)
            if cg:
                declared_groups.add(cg)

        for topic_name in changed_topics:
            # Find downstream models (those that depend on this topic)
            downstream: list[str] = []
            try:
                downstream = dag.get_downstream(topic_name)
            except Exception:
                pass

            change_type = next(
                (c.action for c in plan.topic_changes if c.topic == topic_name), "update"
            )
            change_type = "topic_create" if change_type == "create" else "topic_update"

            # Fetch live consumers if kafka_deployer available
            consumers: list[dict] = []
            if self.kafka_deployer:
                try:
                    groups = self.kafka_deployer.get_consumer_groups()
                    for group_id in groups:
                        lag = self.kafka_deployer.get_consumer_group_lag(group_id, topic_name)
                        if lag is not None:
                            consumers.append(
                                {
                                    "group_id": group_id,
                                    "lag": lag.total_lag,
                                    "declared": group_id in declared_groups,
                                }
                            )
                except Exception as e:
                    logger.debug("Could not fetch consumer groups for impact analysis: %s", e)

            plan.impact_radius.append(
                ImpactEntry(
                    resource=topic_name,
                    change_type=change_type,
                    downstream_models=downstream,
                    consumers=consumers,
                )
            )

    @staticmethod
    def _bucket_for(result: str, create_verb: str) -> str:
        """Map an apply-result string to a result-bucket key."""
        if result == create_verb:
            return "created"
        return "updated" if result == "updated" else "unchanged"

    def _apply_resource_changes(
        self,
        results: dict[str, object],
        deployer: Optional[object],
        changes: list[object],
        *,
        upsert_actions: tuple[str, ...],
        label_fn: Callable[[object], str],
        apply_fn: Callable[[object], str],
        create_verb: str,
        delete_action: str = "delete",
        delete_fn: Optional[Callable[[object], None]] = None,
    ) -> None:
        """Apply a homogeneous list of resource changes, recording outcomes into results."""
        if not deployer:
            return
        for change in changes:
            label = label_fn(change)
            if change.action in upsert_actions and getattr(change, "desired", None):
                try:
                    result = apply_fn(change.desired)
                    results[self._bucket_for(result, create_verb)].append(label)
                except Exception as e:
                    results["errors"].append(f"{label}: {_sanitize_error(e)}")
            elif change.action == delete_action and delete_fn is not None:
                try:
                    delete_fn(change)
                    results["deleted"].append(label)
                except Exception as e:
                    results["errors"].append(f"{label}: {_sanitize_error(e)}")

    def apply(self, plan: Optional[DeploymentPlan] = None) -> dict[str, object]:
        """Apply a deployment plan."""
        if plan is None:
            plan = self.plan()

        results: dict[str, object] = {
            "created": [],
            "updated": [],
            "deleted": [],
            "unchanged": [],
            "errors": [],
        }

        # Apply schemas first (before topics that may use them)
        sr = self.schema_registry_deployer
        self._apply_resource_changes(
            results,
            sr,
            plan.schema_changes,
            upsert_actions=("register", "update"),
            label_fn=lambda c: f"schema:{c.subject}",
            apply_fn=lambda desired: sr.apply_schema(desired),  # type: ignore[union-attr]
            create_verb="registered",
            delete_fn=lambda c: sr.delete_subject(c.subject),  # type: ignore[union-attr]
        )

        kd = self.kafka_deployer
        self._apply_resource_changes(
            results,
            kd,
            plan.topic_changes,
            upsert_actions=("create", "update"),
            label_fn=lambda c: f"topic:{c.topic}",
            apply_fn=lambda desired: kd.apply_topic(desired),  # type: ignore[union-attr]
            create_verb="created",
            delete_fn=lambda c: kd.delete_topic(c.topic),  # type: ignore[union-attr]
        )

        # Flink: "submitted" maps to created/updated based on action; delete is "cancel"
        if self.flink_deployer:
            for change in plan.flink_changes:
                label = f"flink_job:{change.job_name}"
                if change.action in ("submit", "update") and change.desired:
                    try:
                        result = self.flink_deployer.apply_job(change.desired)
                        if result == "submitted":
                            results["updated" if change.action == "update" else "created"].append(
                                label
                            )
                        else:
                            results["unchanged"].append(label)
                    except Exception as e:
                        results["errors"].append(f"{label}: {_sanitize_error(e)}")
                elif change.action == "cancel" and change.current and change.current.job_id:
                    try:
                        self.flink_deployer.cancel_job(change.current.job_id)
                        results["deleted"].append(label)
                    except Exception as e:
                        results["errors"].append(f"{label}: {_sanitize_error(e)}")

        cd = self.connect_deployer
        self._apply_resource_changes(
            results,
            cd,
            plan.connector_changes,
            upsert_actions=("create", "update"),
            label_fn=lambda c: f"connector:{c.connector_name}",
            apply_fn=lambda desired: cd.apply_connector(desired),  # type: ignore[union-attr]
            create_verb="created",
            delete_fn=lambda c: cd.delete_connector(c.connector_name),  # type: ignore[union-attr]
        )

        gd = self.gateway_deployer
        self._apply_resource_changes(
            results,
            gd,
            plan.gateway_changes,
            upsert_actions=("create", "update"),
            label_fn=lambda c: f"gateway_rule:{c.name}",
            apply_fn=lambda desired: gd.apply(desired),  # type: ignore[union-attr]
            create_verb="created",
            delete_fn=lambda c: gd.delete(c.name),  # type: ignore[union-attr]
        )

        # Track rollback candidates (newly created resources that could be undone)
        results["rollback_candidates"] = list(results["created"]) if results["errors"] else []
        results["summary"] = {
            "total": sum(len(v) for v in results.values() if isinstance(v, list)),
            "succeeded": len(results["created"])
            + len(results["updated"])
            + len(results["deleted"]),
            "failed": len(results["errors"]),
            "unchanged": len(results["unchanged"]),
        }

        return results

    def rollback(self, labels: list[str]) -> tuple[list[str], list[str]]:
        """Attempt to delete previously created resources.

        Returns (rolled_back, rollback_errors) lists.
        """
        rolled_back: list[str] = []
        errors: list[str] = []
        for label in labels:
            try:
                self._rollback_resource(label)
                rolled_back.append(label)
            except Exception as e:
                errors.append(f"{label}: {_sanitize_error(e)}")
        return rolled_back, errors

    def _rollback_resource(self, label: str) -> None:
        """Attempt to delete a resource by its apply label (e.g. 'topic:foo')."""
        kind, _, name = label.partition(":")
        if kind == "schema" and self.schema_registry_deployer:
            self.schema_registry_deployer.delete_subject(name)
        elif kind == "topic" and self.kafka_deployer:
            self.kafka_deployer.delete_topic(name)
        elif kind == "connector" and self.connect_deployer:
            self.connect_deployer.delete_connector(name)
        elif kind == "gateway_rule" and self.gateway_deployer:
            self.gateway_deployer.delete(name)
