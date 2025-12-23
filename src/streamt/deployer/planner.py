"""Deployment planner for streamt projects."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

from streamt.compiler.manifest import Manifest
from streamt.deployer.connect import ConnectDeployer, ConnectorChange
from streamt.deployer.flink import FlinkDeployer, FlinkJobChange
from streamt.deployer.kafka import KafkaDeployer, TopicChange
from streamt.deployer.schema_registry import SchemaChange, SchemaRegistryDeployer


@dataclass
class DeploymentPlan:
    """A deployment plan."""

    schema_changes: list[SchemaChange] = field(default_factory=list)
    topic_changes: list[TopicChange] = field(default_factory=list)
    flink_changes: list[FlinkJobChange] = field(default_factory=list)
    connector_changes: list[ConnectorChange] = field(default_factory=list)

    @property
    def has_changes(self) -> bool:
        """Check if there are any changes."""
        return (
            any(c.action != "none" for c in self.schema_changes)
            or any(c.action != "none" for c in self.topic_changes)
            or any(c.action != "none" for c in self.flink_changes)
            or any(c.action != "none" for c in self.connector_changes)
        )

    @property
    def creates(self) -> int:
        """Count of resources to create."""
        return (
            sum(1 for c in self.schema_changes if c.action == "register")
            + sum(1 for c in self.topic_changes if c.action == "create")
            + sum(1 for c in self.flink_changes if c.action == "submit")
            + sum(1 for c in self.connector_changes if c.action == "create")
        )

    @property
    def updates(self) -> int:
        """Count of resources to update."""
        return (
            sum(1 for c in self.schema_changes if c.action == "update")
            + sum(1 for c in self.topic_changes if c.action == "update")
            + sum(1 for c in self.connector_changes if c.action == "update")
        )

    @property
    def deletes(self) -> int:
        """Count of resources to delete."""
        return (
            sum(1 for c in self.schema_changes if c.action == "delete")
            + sum(1 for c in self.topic_changes if c.action == "delete")
            + sum(1 for c in self.flink_changes if c.action == "cancel")
            + sum(1 for c in self.connector_changes if c.action == "delete")
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
    ) -> None:
        """Initialize deployment planner."""
        self.manifest = manifest
        self.schema_registry_deployer = schema_registry_deployer
        self.kafka_deployer = kafka_deployer
        self.flink_deployer = flink_deployer
        self.connect_deployer = connect_deployer

    def plan(self) -> DeploymentPlan:
        """Create a deployment plan."""
        plan = DeploymentPlan()

        # Plan schemas first (before topics that may depend on them)
        if self.schema_registry_deployer:
            from streamt.deployer.schema_registry import SchemaArtifact as SRArtifact

            for schema_data in self.manifest.artifacts.get("schemas", []):
                artifact = SRArtifact(
                    subject=schema_data["subject"],
                    schema=schema_data["schema"],
                    schema_type=schema_data.get("schema_type", "AVRO"),
                    compatibility=schema_data.get("compatibility"),
                )
                change = self.schema_registry_deployer.plan_schema(artifact)
                plan.schema_changes.append(change)

        # Plan topics
        if self.kafka_deployer:
            from streamt.compiler.manifest import TopicArtifact

            for topic_data in self.manifest.artifacts.get("topics", []):
                artifact = TopicArtifact(**topic_data)
                change = self.kafka_deployer.plan_topic(artifact)
                plan.topic_changes.append(change)

        # Plan Flink jobs
        if self.flink_deployer:
            from streamt.compiler.manifest import FlinkJobArtifact

            for job_data in self.manifest.artifacts.get("flink_jobs", []):
                artifact = FlinkJobArtifact(**job_data)
                change = self.flink_deployer.plan_job(artifact)
                plan.flink_changes.append(change)

        # Plan connectors
        if self.connect_deployer:
            from streamt.compiler.manifest import ConnectorArtifact

            for conn_data in self.manifest.artifacts.get("connectors", []):
                artifact = ConnectorArtifact(
                    name=conn_data["name"],
                    connector_class=conn_data["config"].get("connector.class", ""),
                    topics=conn_data["config"].get("topics", "").split(","),
                    config={
                        k: v
                        for k, v in conn_data["config"].items()
                        if k not in ["name", "connector.class", "topics"]
                    },
                )
                change = self.connect_deployer.plan_connector(artifact)
                plan.connector_changes.append(change)

        return plan

    def apply(self, plan: Optional[DeploymentPlan] = None) -> dict[str, list[str]]:
        """Apply a deployment plan."""
        if plan is None:
            plan = self.plan()

        results: dict[str, list[str]] = {
            "created": [],
            "updated": [],
            "unchanged": [],
            "errors": [],
        }

        # Apply schemas first (before topics that may use them)
        if self.schema_registry_deployer:

            for change in plan.schema_changes:
                if change.action in ["register", "update"] and change.desired:
                    try:
                        result = self.schema_registry_deployer.apply_schema(change.desired)
                        if result == "registered":
                            results["created"].append(f"schema:{change.subject}")
                        elif result == "updated":
                            results["updated"].append(f"schema:{change.subject}")
                        else:
                            results["unchanged"].append(f"schema:{change.subject}")
                    except Exception as e:
                        results["errors"].append(f"schema:{change.subject}: {e}")

        # Apply topics
        if self.kafka_deployer:
            for change in plan.topic_changes:
                if change.action in ["create", "update"] and change.desired:
                    try:
                        result = self.kafka_deployer.apply_topic(change.desired)
                        if result == "created":
                            results["created"].append(f"topic:{change.topic}")
                        elif result == "updated":
                            results["updated"].append(f"topic:{change.topic}")
                        else:
                            results["unchanged"].append(f"topic:{change.topic}")
                    except Exception as e:
                        results["errors"].append(f"topic:{change.topic}: {e}")

        # Apply Flink jobs
        if self.flink_deployer:
            for change in plan.flink_changes:
                if change.action == "submit" and change.desired:
                    try:
                        result = self.flink_deployer.apply_job(change.desired)
                        if result == "submitted":
                            results["created"].append(f"flink_job:{change.job_name}")
                        else:
                            results["unchanged"].append(f"flink_job:{change.job_name}")
                    except Exception as e:
                        results["errors"].append(f"flink_job:{change.job_name}: {e}")

        # Apply connectors
        if self.connect_deployer:
            for change in plan.connector_changes:
                if change.action in ["create", "update"] and change.desired:
                    try:
                        result = self.connect_deployer.apply_connector(change.desired)
                        if result == "created":
                            results["created"].append(f"connector:{change.connector_name}")
                        elif result == "updated":
                            results["updated"].append(f"connector:{change.connector_name}")
                        else:
                            results["unchanged"].append(f"connector:{change.connector_name}")
                    except Exception as e:
                        results["errors"].append(f"connector:{change.connector_name}: {e}")

        return results
