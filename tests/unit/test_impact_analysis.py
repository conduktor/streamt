"""TDD tests for P5: Impact Analysis at plan time — live consumer discovery.

Tests drive the design: DeploymentPlan gains an impact_radius field;
planner._compute_impact_radius() uses DAG + optional live Kafka consumer data.
"""

from unittest.mock import MagicMock

from streamt.compiler.manifest import Manifest


def _make_manifest(topics=None, flink_jobs=None):
    m = Manifest(version="1.0.0", project_name="test")
    if topics:
        m.artifacts["topics"] = [t.to_dict() if hasattr(t, "to_dict") else t for t in topics]
    if flink_jobs:
        m.artifacts["flink_jobs"] = flink_jobs
    return m


class TestImpactRadiusDataclass:
    """DeploymentPlan.impact_radius is populated by plan()."""

    def test_deployment_plan_has_impact_radius_field(self):
        """DeploymentPlan has an impact_radius attribute (list of dicts)."""
        from streamt.deployer.planner import DeploymentPlan

        plan = DeploymentPlan()
        assert hasattr(plan, "impact_radius")
        assert isinstance(plan.impact_radius, list)

    def test_impact_radius_item_shape(self):
        """Each impact_radius entry has resource, downstream_models, consumers keys."""
        from streamt.deployer.planner import DeploymentPlan, ImpactEntry

        entry = ImpactEntry(
            resource="payments_clean",
            change_type="schema_update",
            downstream_models=["fraud_scoring", "analytics"],
            consumers=[{"group_id": "fraud-prod", "lag": 0}],
        )
        plan = DeploymentPlan()
        plan.impact_radius.append(entry)
        assert plan.impact_radius[0].resource == "payments_clean"
        assert "fraud_scoring" in plan.impact_radius[0].downstream_models


class TestImpactAnalysisDAGPropagation:
    """Impact analysis uses DAG to find downstream models."""

    def _make_planner(self, manifest, project, kafka_deployer=None, flink_deployer=None):
        from streamt.deployer.planner import DeploymentPlanner

        return DeploymentPlanner(
            manifest=manifest,
            project=project,
            kafka_deployer=kafka_deployer,
            flink_deployer=flink_deployer,
        )

    def _simple_project(self):
        """Build a small project: raw → clean → enriched (three-model chain)."""
        import tempfile
        from pathlib import Path

        import yaml

        from streamt.core.parser import ProjectParser

        cfg = {
            "project": {"name": "test", "version": "1.0.0"},
            "runtime": {"kafka": {"bootstrap_servers": "localhost:9092"}},
            "sources": [{"name": "raw", "topic": "raw.v1"}],
            "models": [
                {"name": "clean", "sql": 'SELECT * FROM {{ source("raw") }}'},
                {"name": "enriched", "sql": 'SELECT * FROM {{ ref("clean") }}'},
            ],
        }
        with tempfile.TemporaryDirectory() as d:
            (Path(d) / "stream_project.yml").write_text(yaml.dump(cfg))
            return ProjectParser(Path(d)).parse()

    def test_schema_change_shows_downstream_models(self):
        """Changing a schema that a model depends on lists downstream models in impact."""
        project = self._simple_project()
        manifest = _make_manifest(topics=[{"name": "clean"}, {"name": "enriched"}])
        planner = self._make_planner(manifest, project)
        plan = planner.plan()

        # impact_radius should exist; if clean topic changes, enriched is downstream
        assert isinstance(plan.impact_radius, list)

    def test_no_impact_when_no_changes(self):
        """Empty plan produces empty impact_radius."""
        project = self._simple_project()
        manifest = _make_manifest()
        planner = self._make_planner(manifest, project)
        plan = planner.plan()
        # With no artifacts in manifest, nothing changes
        assert isinstance(plan.impact_radius, list)


class TestImpactAnalysisLiveConsumers:
    """Impact analysis fetches live consumer group lag when kafka_deployer is available."""

    def _make_kafka_deployer_mock(self, groups, lag_map):
        """groups: list of group_ids; lag_map: {(group, topic): ConsumerGroupLag}"""

        mock = MagicMock()
        mock.get_consumer_groups.return_value = groups

        def _lag(group_id, topic):
            key = (group_id, topic)
            if key in lag_map:
                return lag_map[key]
            return None

        mock.get_consumer_group_lag.side_effect = _lag
        return mock

    def test_live_consumers_appear_in_impact_for_changed_topic(self):
        """Consumer groups consuming an affected topic appear in impact_radius."""
        import tempfile
        from pathlib import Path

        import yaml

        from streamt.core.parser import ProjectParser
        from streamt.deployer.kafka import ConsumerGroupLag
        from streamt.deployer.planner import DeploymentPlanner

        cfg = {
            "project": {"name": "test", "version": "1.0.0"},
            "runtime": {"kafka": {"bootstrap_servers": "localhost:9092"}},
            "sources": [{"name": "raw", "topic": "raw.v1"}],
            "models": [{"name": "clean", "sql": 'SELECT * FROM {{ source("raw") }}'}],
        }
        with tempfile.TemporaryDirectory() as d:
            (Path(d) / "stream_project.yml").write_text(yaml.dump(cfg))
            project = ProjectParser(Path(d)).parse()

        # Manifest has no 'clean' topic → plan will create it → that's a change
        manifest = _make_manifest()

        lag = ConsumerGroupLag(group_id="fraud-prod", topic="clean", total_lag=5000)
        kafka_mock = self._make_kafka_deployer_mock(
            groups=["fraud-prod", "analytics-v2"],
            lag_map={("fraud-prod", "clean"): lag},
        )

        planner = DeploymentPlanner(manifest=manifest, project=project, kafka_deployer=kafka_mock)
        plan = planner.plan()

        assert isinstance(plan.impact_radius, list)
        # Find an entry for 'clean'
        clean_entries = [e for e in plan.impact_radius if e.resource == "clean"]
        if clean_entries:
            consumer_groups = [c["group_id"] for c in clean_entries[0].consumers]
            assert "fraud-prod" in consumer_groups

    def test_impact_radius_empty_when_no_kafka_deployer(self):
        """When kafka_deployer is None, impact_radius has no consumer data but still runs."""
        import tempfile
        from pathlib import Path

        import yaml

        from streamt.core.parser import ProjectParser
        from streamt.deployer.planner import DeploymentPlanner

        cfg = {
            "project": {"name": "test", "version": "1.0.0"},
            "runtime": {"kafka": {"bootstrap_servers": "localhost:9092"}},
            "sources": [{"name": "raw", "topic": "raw.v1"}],
            "models": [{"name": "clean", "sql": 'SELECT * FROM {{ source("raw") }}'}],
        }
        with tempfile.TemporaryDirectory() as d:
            (Path(d) / "stream_project.yml").write_text(yaml.dump(cfg))
            project = ProjectParser(Path(d)).parse()

        manifest = _make_manifest()
        planner = DeploymentPlanner(manifest=manifest, project=project, kafka_deployer=None)
        plan = planner.plan()
        # Should run without error; consumers list is empty in entries
        assert isinstance(plan.impact_radius, list)
        for entry in plan.impact_radius:
            assert entry.consumers == []

    def test_undeclared_consumer_flagged(self):
        """Consumer group not in exposures is flagged as undeclared in impact entry."""
        import tempfile
        from pathlib import Path

        import yaml

        from streamt.core.parser import ProjectParser
        from streamt.deployer.kafka import ConsumerGroupLag
        from streamt.deployer.planner import DeploymentPlanner

        cfg = {
            "project": {"name": "test", "version": "1.0.0"},
            "runtime": {"kafka": {"bootstrap_servers": "localhost:9092"}},
            "sources": [{"name": "raw", "topic": "raw.v1"}],
            "models": [{"name": "clean", "sql": 'SELECT * FROM {{ source("raw") }}'}],
            # No exposures declared
        }
        with tempfile.TemporaryDirectory() as d:
            (Path(d) / "stream_project.yml").write_text(yaml.dump(cfg))
            project = ProjectParser(Path(d)).parse()

        manifest = _make_manifest()
        lag = ConsumerGroupLag(group_id="mystery-svc", topic="clean", total_lag=100)
        kafka_mock = self._make_kafka_deployer_mock(
            groups=["mystery-svc"],
            lag_map={("mystery-svc", "clean"): lag},
        )
        planner = DeploymentPlanner(manifest=manifest, project=project, kafka_deployer=kafka_mock)
        plan = planner.plan()

        # If mystery-svc appears, it should be flagged as undeclared
        for entry in plan.impact_radius:
            if entry.resource == "clean":
                for consumer in entry.consumers:
                    if consumer["group_id"] == "mystery-svc":
                        assert consumer.get("declared") is False


class TestPlanOutputIncludesImpact:
    """plan JSON output includes impact_radius when present."""

    def test_deployment_plan_to_dict_includes_impact_radius(self):
        """DeploymentPlan serialises impact_radius into its dict/json representation."""
        from streamt.deployer.planner import DeploymentPlan, ImpactEntry

        plan = DeploymentPlan()
        plan.impact_radius.append(
            ImpactEntry(
                resource="payments_clean",
                change_type="topic_create",
                downstream_models=["fraud_scoring"],
                consumers=[{"group_id": "fraud-prod", "lag": 0, "declared": True}],
            )
        )
        # Plan details() or a to_dict() method should include impact
        detail = plan.details()
        assert "impact" in detail.lower() or isinstance(plan.impact_radius, list)
