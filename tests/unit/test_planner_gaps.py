"""Gap tests for DeploymentPlanner: delete detection, partial failure,
plan properties, delegation, and apply behavior.

Groups covered:
  1. DELETE detection in planner (CRITICAL)
  5. Partial failure reporting in planner apply
  Plus: plan properties, delegation, apply action routing
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from streamt.compiler.manifest import (
    FlinkJobArtifact,
    GatewayRuleArtifact,
    Manifest,
    SchemaArtifact,
    TopicArtifact,
)
from streamt.deployer.connect import ConnectDeployer, ConnectorChange
from streamt.deployer.flink import FlinkDeployer, FlinkJobChange
from streamt.deployer.gateway import GatewayDeployer, GatewayRuleChange
from streamt.deployer.kafka import KafkaDeployer, TopicChange
from streamt.deployer.planner import DeploymentPlan, DeploymentPlanner
from streamt.deployer.schema_registry import SchemaChange, SchemaRegistryDeployer

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _manifest(**artifacts: list) -> Manifest:
    return Manifest(version="1.0", project_name="test", artifacts=artifacts)


def _mock_kafka() -> MagicMock:
    m = MagicMock(spec=KafkaDeployer)
    m.list_topics.return_value = []
    return m


def _mock_sr() -> MagicMock:
    m = MagicMock(spec=SchemaRegistryDeployer)
    m.list_subjects.return_value = []
    return m


def _mock_connect() -> MagicMock:
    m = MagicMock(spec=ConnectDeployer)
    m.list_connectors.return_value = []
    return m


def _mock_flink() -> MagicMock:
    return MagicMock(spec=FlinkDeployer)


def _mock_gw() -> MagicMock:
    return MagicMock(spec=GatewayDeployer)


# ===========================================================================
# GROUP 1: DELETE detection in planner
# ===========================================================================


class TestPlannerDeleteDetection:
    """Planner should detect orphaned resources in the cluster that are
    absent from the manifest, and plan DELETE actions."""

    def test_orphan_topic_detected_as_delete(self):
        """Manifest has NO topics, Kafka has 'orphan_topic'."""

        kafka = _mock_kafka()
        kafka.list_topics.return_value = ["orphan_topic"]
        planner = DeploymentPlanner(_manifest(topics=[]), kafka_deployer=kafka)
        plan = planner.plan()

        deletes = [c for c in plan.topic_changes if c.action == "delete"]
        assert len(deletes) == 1
        assert deletes[0].topic == "orphan_topic"

    def test_orphan_schema_detected_as_delete(self):
        """Manifest has NO schemas, SR has 'orphan-value'."""

        sr = _mock_sr()
        sr.list_subjects.return_value = ["orphan-value"]
        planner = DeploymentPlanner(_manifest(schemas=[]), schema_registry_deployer=sr)
        plan = planner.plan()

        deletes = [c for c in plan.schema_changes if c.action == "delete"]
        assert len(deletes) == 1
        assert deletes[0].subject == "orphan-value"

    def test_orphan_connector_detected_as_delete(self):
        """Manifest has NO connectors, Connect has 'orphan-sink'."""

        connect = _mock_connect()
        connect.list_connectors.return_value = ["orphan-sink"]
        planner = DeploymentPlanner(_manifest(connectors=[]), connect_deployer=connect)
        plan = planner.plan()

        deletes = [c for c in plan.connector_changes if c.action == "delete"]
        assert len(deletes) == 1
        assert deletes[0].connector_name == "orphan-sink"


# ===========================================================================
# GROUP 5: Partial failure reporting in planner apply
# ===========================================================================


class TestPlannerPartialFailure:
    """apply() should continue past individual errors and report both
    successes and failures."""

    def test_schema_failure_does_not_block_subsequent_schemas(self):
        """3 schemas: 1st succeeds, 2nd fails, 3rd succeeds."""
        sr = _mock_sr()
        sr.apply_schema.side_effect = [
            "registered",
            RuntimeError("Schema incompatible"),
            "registered",
        ]

        schemas = [
            SchemaArtifact(subject=f"s{i}-value", schema={"type": "record", "name": f"S{i}", "fields": []})
            for i in range(1, 4)
        ]
        plan = DeploymentPlan(
            schema_changes=[
                SchemaChange(subject=s.subject, action="register", desired=s) for s in schemas
            ],
        )

        planner = DeploymentPlanner(_manifest(), schema_registry_deployer=sr)
        results = planner.apply(plan)

        assert "schema:s1-value" in results["created"]
        assert any("s2-value" in e for e in results["errors"])
        assert "schema:s3-value" in results["created"]

    def test_topic_failure_does_not_block_subsequent_topics(self):
        """2 topics: 1st fails, 2nd succeeds."""
        kafka = _mock_kafka()
        kafka.apply_topic.side_effect = [RuntimeError("RF too large"), "created"]

        topics = [
            TopicArtifact(name="t1", partitions=3, replication_factor=5),
            TopicArtifact(name="t2", partitions=3, replication_factor=1),
        ]
        plan = DeploymentPlan(
            topic_changes=[
                TopicChange(topic=t.name, action="create", desired=t) for t in topics
            ],
        )

        planner = DeploymentPlanner(_manifest(), kafka_deployer=kafka)
        results = planner.apply(plan)

        assert any("t1" in e for e in results["errors"])
        assert "topic:t2" in results["created"]

    def test_mixed_resource_failure_isolation(self):
        """Schema ok, topic fails, flink ok -- errors isolated."""
        sr = _mock_sr()
        sr.apply_schema.return_value = "registered"
        kafka = _mock_kafka()
        kafka.apply_topic.side_effect = RuntimeError("Kafka down")
        flink = _mock_flink()
        flink.apply_job.return_value = "submitted"

        plan = DeploymentPlan(
            schema_changes=[SchemaChange(
                subject="s-value", action="register",
                desired=SchemaArtifact(subject="s-value", schema={"type": "record", "name": "S", "fields": []}),
            )],
            topic_changes=[TopicChange(
                topic="t", action="create",
                desired=TopicArtifact(name="t", partitions=1, replication_factor=1),
            )],
            flink_changes=[FlinkJobChange(
                job_name="j", action="submit",
                desired=FlinkJobArtifact(name="j", sql="SELECT 1"),
            )],
        )

        planner = DeploymentPlanner(
            _manifest(), schema_registry_deployer=sr,
            kafka_deployer=kafka, flink_deployer=flink,
        )
        results = planner.apply(plan)

        assert "schema:s-value" in results["created"]
        assert any("t" in e for e in results["errors"])
        assert "flink_job:j" in results["created"]

    def test_gateway_error_captured_in_results(self):
        """Planner.apply() wraps gateway errors into results['errors']."""
        gw = _mock_gw()
        gw.apply.side_effect = RuntimeError("Gateway connection refused")

        artifact = GatewayRuleArtifact(name="r1", virtual_topic="vt", physical_topic="pt")
        plan = DeploymentPlan(
            gateway_changes=[GatewayRuleChange(name="r1", action="create", desired=artifact)],
        )

        planner = DeploymentPlanner(_manifest(), gateway_deployer=gw)
        results = planner.apply(plan)

        assert len(results["errors"]) == 1
        assert "r1" in results["errors"][0]


# ===========================================================================
# DeploymentPlan properties
# ===========================================================================


class TestDeploymentPlanProperties:

    def test_empty_plan_has_no_changes(self):
        p = DeploymentPlan()
        assert not p.has_changes
        assert p.creates == 0
        assert p.updates == 0
        assert p.deletes == 0

    def test_none_actions_have_no_changes(self):
        p = DeploymentPlan(
            topic_changes=[TopicChange(topic="t1", action="none")],
            schema_changes=[SchemaChange(subject="s1", action="none")],
        )
        assert not p.has_changes

    def test_creates_counted(self):
        p = DeploymentPlan(
            topic_changes=[TopicChange(topic="t1", action="create"), TopicChange(topic="t2", action="create")],
            schema_changes=[SchemaChange(subject="s1", action="register")],
        )
        assert p.creates == 3
        assert p.has_changes

    def test_updates_counted(self):
        p = DeploymentPlan(
            topic_changes=[TopicChange(topic="t1", action="update")],
            connector_changes=[ConnectorChange(connector_name="c1", action="update")],
        )
        assert p.updates == 2

    def test_deletes_counted(self):
        p = DeploymentPlan(
            topic_changes=[TopicChange(topic="t1", action="delete")],
            schema_changes=[SchemaChange(subject="s1", action="delete")],
            flink_changes=[FlinkJobChange(job_name="f1", action="cancel")],
            connector_changes=[ConnectorChange(connector_name="c1", action="delete")],
            gateway_changes=[GatewayRuleChange(name="g1", action="delete")],
        )
        assert p.deletes == 5
        assert p.has_changes

    def test_summary_format(self):
        p = DeploymentPlan(topic_changes=[TopicChange(topic="t1", action="create")])
        assert "1 to create" in p.summary()

    def test_details_no_changes(self):
        assert "No changes detected." in DeploymentPlan().details()

    def test_details_delete_prefix(self):
        p = DeploymentPlan(topic_changes=[TopicChange(topic="old", action="delete")])
        assert "- topic: old" in p.details()


# ===========================================================================
# Planner delegation and apply routing
# ===========================================================================


class TestPlannerDelegation:

    def test_no_deployers_returns_empty(self):
        plan = DeploymentPlanner(_manifest()).plan()
        assert not plan.has_changes

    def test_delegates_to_kafka(self):
        kafka = _mock_kafka()
        kafka.plan_topic.return_value = TopicChange(
            topic="e", action="create",
            desired=TopicArtifact(name="e", partitions=3, replication_factor=1),
        )
        plan = DeploymentPlanner(
            _manifest(topics=[{"name": "e", "partitions": 3, "replication_factor": 1, "config": {}}]),
            kafka_deployer=kafka,
        ).plan()
        assert len(plan.topic_changes) == 1
        assert plan.topic_changes[0].action == "create"

    def test_delegates_to_sr(self):
        sr = _mock_sr()
        sr.plan_schema.return_value = SchemaChange(
            subject="e-value", action="register",
            desired=SchemaArtifact(subject="e-value", schema={"type": "record", "name": "E", "fields": []}),
        )
        plan = DeploymentPlanner(
            _manifest(schemas=[{"subject": "e-value", "schema": {}, "schema_type": "AVRO"}]),
            schema_registry_deployer=sr,
        ).plan()
        assert plan.schema_changes[0].action == "register"

    def test_delegates_to_flink(self):
        flink = _mock_flink()
        flink.plan_job.return_value = FlinkJobChange(
            job_name="p", action="submit",
            desired=FlinkJobArtifact(name="p", sql="SELECT 1"),
        )
        plan = DeploymentPlanner(
            _manifest(flink_jobs=[{"name": "p", "sql": "SELECT 1"}]),
            flink_deployer=flink,
        ).plan()
        assert plan.flink_changes[0].action == "submit"

    def test_delegates_to_gateway(self):
        gw = _mock_gw()
        gw.plan.return_value = GatewayRuleChange(
            name="r1", action="create",
            desired=GatewayRuleArtifact(name="r1", virtual_topic="vt", physical_topic="pt"),
        )
        plan = DeploymentPlanner(
            _manifest(gateway_rules=[{"name": "r1", "virtualTopic": "vt", "physicalTopic": "pt", "interceptors": []}]),
            gateway_deployer=gw,
        ).plan()
        assert plan.gateway_changes[0].action == "create"


class TestPlannerApplyRouting:

    def test_skips_none_actions(self):
        kafka = _mock_kafka()
        plan = DeploymentPlan(topic_changes=[TopicChange(topic="t1", action="none")])
        DeploymentPlanner(_manifest(), kafka_deployer=kafka).apply(plan)
        kafka.apply_topic.assert_not_called()

    def test_calls_plan_when_none_provided(self):
        kafka = _mock_kafka()
        kafka.plan_topic.return_value = TopicChange(topic="t1", action="none")
        DeploymentPlanner(
            _manifest(topics=[{"name": "t1", "partitions": 1, "replication_factor": 1, "config": {}}]),
            kafka_deployer=kafka,
        ).apply()
        kafka.plan_topic.assert_called_once()

    def test_unchanged_routed_correctly(self):
        kafka = _mock_kafka()
        kafka.apply_topic.return_value = "unchanged"
        plan = DeploymentPlan(topic_changes=[TopicChange(
            topic="t1", action="update",
            desired=TopicArtifact(name="t1", partitions=3, replication_factor=1),
        )])
        results = DeploymentPlanner(_manifest(), kafka_deployer=kafka).apply(plan)
        assert "topic:t1" in results["unchanged"]

    def test_updated_schema_routed_correctly(self):
        sr = _mock_sr()
        sr.apply_schema.return_value = "updated"
        plan = DeploymentPlan(schema_changes=[SchemaChange(
            subject="s-value", action="update",
            desired=SchemaArtifact(subject="s-value", schema={"type": "record", "name": "S", "fields": []}),
        )])
        results = DeploymentPlanner(_manifest(), schema_registry_deployer=sr).apply(plan)
        assert "schema:s-value" in results["updated"]
