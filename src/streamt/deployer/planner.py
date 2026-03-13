"""Deployment planner for streamt projects."""

from __future__ import annotations

import logging
import re
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Optional

from streamt.compiler.manifest import Manifest
from streamt.deployer.connect import ConnectDeployer, ConnectorChange
from streamt.deployer.flink import FlinkDeployer, FlinkJobChange
from streamt.deployer.gateway import GatewayDeployer, GatewayRuleChange
from streamt.deployer.kafka import KafkaDeployer, TopicChange
from streamt.deployer.schema_registry import SchemaChange, SchemaRegistryDeployer

logger = logging.getLogger(__name__)

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
class DeploymentPlan:
    """A deployment plan."""

    schema_changes: list[SchemaChange] = field(default_factory=list)
    topic_changes: list[TopicChange] = field(default_factory=list)
    flink_changes: list[FlinkJobChange] = field(default_factory=list)
    connector_changes: list[ConnectorChange] = field(default_factory=list)
    gateway_changes: list[GatewayRuleChange] = field(default_factory=list)

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

    def summary(self) -> str:
        """Get a summary of the plan."""
        return f"Plan: {self.creates} to create, {self.updates} to update, {self.deletes} to delete"

    def details(self) -> str:
        """Get detailed plan output."""
        lines = [self.summary(), ""]

        for change in self.schema_changes:
            if change.action == "register":
                lines.append(f"+ schema: {change.subject}")
                if change.desired:
                    lines.append(f"    type: {change.desired.schema_type}")
            elif change.action == "update":
                lines.append(f"~ schema: {change.subject}")
                for key, val in (change.changes or {}).items():
                    if key == "schema":
                        lines.append(f"    version: {val['from_version']} -> {val['to_version']}")
                    elif key == "compatibility":
                        lines.append(f"    compatibility: {val['from']} -> {val['to']}")
            elif change.action == "delete":
                lines.append(f"- schema: {change.subject}")

        for change in self.topic_changes:
            if change.action == "create":
                lines.append(f"+ topic: {change.topic}")
                if change.desired:
                    lines.append(f"    partitions: {change.desired.partitions}")
                    lines.append(f"    replication_factor: {change.desired.replication_factor}")
            elif change.action == "update":
                lines.append(f"~ topic: {change.topic}")
                for key, val in (change.changes or {}).items():
                    lines.append(f"    {key}: {val['from']} -> {val['to']}")
            elif change.action == "delete":
                lines.append(f"- topic: {change.topic}")

        for change in self.flink_changes:
            if change.action == "submit":
                lines.append(f"+ flink_job: {change.job_name}")
            elif change.action == "cancel":
                lines.append(f"- flink_job: {change.job_name}")

        for change in self.connector_changes:
            if change.action == "create":
                lines.append(f"+ connector: {change.connector_name}")
            elif change.action == "update":
                lines.append(f"~ connector: {change.connector_name}")
                for key, val in (change.changes or {}).items():
                    lines.append(f"    {key}: {val['from']} -> {val['to']}")
            elif change.action == "delete":
                lines.append(f"- connector: {change.connector_name}")

        for change in self.gateway_changes:
            if change.action == "create":
                lines.append(f"+ gateway_rule: {change.name}")
            elif change.action == "update":
                lines.append(f"~ gateway_rule: {change.name}")
                for key, val in (change.changes or {}).items():
                    lines.append(f"    {key}: {val['from']} -> {val['to']}")
            elif change.action == "delete":
                lines.append(f"- gateway_rule: {change.name}")

        if not self.has_changes:
            lines.append("No changes detected.")

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
    ) -> None:
        """Initialize deployment planner."""
        self.manifest = manifest
        self.schema_registry_deployer = schema_registry_deployer
        self.kafka_deployer = kafka_deployer
        self.flink_deployer = flink_deployer
        self.connect_deployer = connect_deployer
        self.gateway_deployer = gateway_deployer

    def plan(self) -> DeploymentPlan:
        """Create a deployment plan."""
        plan = DeploymentPlan()
        # Track successfully-planned names so orphan detection only considers
        # artifacts that were actually parsed. Malformed artifacts are excluded
        # to prevent accidental deletion of real resources.
        planned_subjects: set[str] = set()
        planned_topics: set[str] = set()
        planned_connectors: set[str] = set()

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
                    )
                    change = self.schema_registry_deployer.plan_schema(artifact)
                    plan.schema_changes.append(change)
                    planned_subjects.add(artifact.subject)
                except KeyError as e:
                    logger.error("Malformed schema artifact, missing key %s: %s", e, schema_data)

        # Plan topics
        if self.kafka_deployer:
            from streamt.compiler.manifest import TopicArtifact

            for topic_data in self.manifest.artifacts.get("topics", []):
                try:
                    artifact = TopicArtifact(**topic_data)
                    change = self.kafka_deployer.plan_topic(artifact)
                    plan.topic_changes.append(change)
                    planned_topics.add(artifact.name)
                except (KeyError, TypeError) as e:
                    logger.error("Malformed topic artifact: %s in %s", e, topic_data)

        # Plan Flink jobs
        if self.flink_deployer:
            from streamt.compiler.manifest import FlinkJobArtifact

            for job_data in self.manifest.artifacts.get("flink_jobs", []):
                try:
                    artifact = FlinkJobArtifact(**job_data)
                    change = self.flink_deployer.plan_job(artifact)
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
                    )
                    change = self.connect_deployer.plan_connector(artifact)
                    plan.connector_changes.append(change)
                    planned_connectors.add(artifact.name)
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
                    )
                    change = self.gateway_deployer.plan(artifact)
                    plan.gateway_changes.append(change)
                except KeyError as e:
                    logger.error(
                        "Malformed gateway_rule artifact, missing key %s: %s", e, rule_data
                    )

        # Detect orphaned resources (exist in cluster but not in manifest)
        self._detect_orphans(plan, planned_subjects, planned_topics, planned_connectors)

        return plan

    def _detect_orphans(
        self,
        plan: DeploymentPlan,
        planned_subjects: set[str],
        planned_topics: set[str],
        planned_connectors: set[str],
    ) -> None:
        """Detect resources in the cluster that are absent from the manifest.

        Uses the sets of successfully-planned artifact names (not raw manifest
        data) to avoid marking resources for deletion when their manifest entry
        was malformed and skipped during planning.
        """
        # Orphaned schemas
        if self.schema_registry_deployer:
            try:
                for subject in self.schema_registry_deployer.list_subjects():
                    if subject not in planned_subjects:
                        plan.schema_changes.append(SchemaChange(subject=subject, action="delete"))
            except Exception as e:
                logger.error("Failed to list subjects for orphan detection: %s", e)

        # Orphaned topics
        if self.kafka_deployer:
            try:
                for topic in self.kafka_deployer.list_topics():
                    if topic not in planned_topics:
                        plan.topic_changes.append(TopicChange(topic=topic, action="delete"))
            except Exception as e:
                logger.error("Failed to list topics for orphan detection: %s", e)

        # Orphaned connectors
        if self.connect_deployer:
            try:
                for connector in self.connect_deployer.list_connectors():
                    if connector not in planned_connectors:
                        plan.connector_changes.append(
                            ConnectorChange(connector_name=connector, action="delete")
                        )
            except Exception as e:
                logger.error("Failed to list connectors for orphan detection: %s", e)

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

        results["summary"] = {
            "total": sum(len(v) for v in results.values() if isinstance(v, list)),
            "succeeded": len(results["created"])
            + len(results["updated"])
            + len(results["deleted"]),
            "failed": len(results["errors"]),
            "unchanged": len(results["unchanged"]),
        }

        return results
