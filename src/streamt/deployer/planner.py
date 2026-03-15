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
class ImpactEntry:
    """An entry in the impact radius for a planned change."""

    resource: str
    change_type: str
    downstream_models: list[str] = field(default_factory=list)
    consumers: list[dict] = field(default_factory=list)


@dataclass
class DeploymentPlan:
    """A deployment plan."""

    schema_changes: list[SchemaChange] = field(default_factory=list)
    topic_changes: list[TopicChange] = field(default_factory=list)
    flink_changes: list[FlinkJobChange] = field(default_factory=list)
    connector_changes: list[ConnectorChange] = field(default_factory=list)
    gateway_changes: list[GatewayRuleChange] = field(default_factory=list)
    impact_radius: list[ImpactEntry] = field(default_factory=list)

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
                    lines.append(f"    {key}: {val['from']} -> {val['to']}")
            elif change.action == "delete":
                lines.append(_rm(f"- topic: {change.topic}"))

        for change in self.flink_changes:
            if change.action == "submit":
                lines.append(_add(f"+ flink_job: {change.job_name}"))
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
    ) -> None:
        """Initialize deployment planner."""
        self.manifest = manifest
        self.schema_registry_deployer = schema_registry_deployer
        self.kafka_deployer = kafka_deployer
        self.flink_deployer = flink_deployer
        self.connect_deployer = connect_deployer
        self.gateway_deployer = gateway_deployer
        self.project = project

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
                )
                plan.schema_changes.append(SchemaChange(subject=artifact.subject, action="register", desired=artifact))
            except KeyError:
                pass

        for topic_data in self.manifest.artifacts.get("topics", []):
            try:
                artifact = TopicArtifact(**topic_data)
                plan.topic_changes.append(TopicChange(topic=artifact.name, action="create", desired=artifact))
            except (KeyError, TypeError):
                pass

        for job_data in self.manifest.artifacts.get("flink_jobs", []):
            try:
                artifact = FlinkJobArtifact(**job_data)
                plan.flink_changes.append(FlinkJobChange(job_name=artifact.name, action="submit", desired=artifact))
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
                )
                plan.connector_changes.append(ConnectorChange(connector_name=artifact.name, action="create", desired=artifact))
            except KeyError:
                pass

        for rule_data in self.manifest.artifacts.get("gateway_rules", []):
            try:
                artifact = GatewayRuleArtifact(
                    name=rule_data["name"],
                    virtual_topic=rule_data["virtualTopic"],
                    physical_topic=rule_data["physicalTopic"],
                    interceptors=rule_data.get("interceptors", []),
                )
                plan.gateway_changes.append(GatewayRuleChange(name=artifact.name, action="create", desired=artifact))
            except KeyError:
                pass

        return plan

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

        # Compute impact radius for planned changes
        self._compute_impact_radius(plan)

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
