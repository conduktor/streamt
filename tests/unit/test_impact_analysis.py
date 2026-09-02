"""Canonical change-impact evidence tests."""

from __future__ import annotations

import json
from unittest.mock import MagicMock

import pytest

from streamt.compiler.manifest import ArtifactOwnership, Manifest, TopicArtifact
from streamt.core.models import Exposure, Model, ProjectInfo, Source, StreamtProject
from streamt.core.runtime import KafkaConfig, RuntimeConfig
from streamt.deployer.kafka import ConsumerGroupLag, TopicChange
from streamt.deployer.plan_file import (
    ReviewedPlanFile,
    StalePlanError,
    deployment_plan_payload,
)
from streamt.deployer.planner import DeploymentPlan, DeploymentPlanner, ImpactEntry

PHYSICAL_CLEAN = "prod.payments.clean.v2"


def _project() -> StreamtProject:
    """Build raw -> clean -> enriched -> scored -> fraud_service."""
    return StreamtProject(
        project=ProjectInfo(name="payments", version="1.0.0"),
        runtime=RuntimeConfig(kafka=KafkaConfig(bootstrap_servers="broker:9092")),
        sources=[Source(name="raw", topic="vendor.payments.v1", owner="ingest-team")],
        models=[
            Model(
                name="clean",
                owner="payments-platform",
                **{"from": [{"source": "raw"}]},
            ),
            Model(
                name="enriched",
                owner="feature-platform",
                **{"from": [{"ref": "clean"}]},
            ),
            Model(
                name="scored",
                owner="fraud-ml",
                **{"from": [{"ref": "enriched"}]},
            ),
        ],
        exposures=[
            Exposure(
                name="fraud_service",
                type="application",
                owners=[{"name": "risk-platform"}, {"name": "fraud-team"}],
                consumer_group="fraud-prod",
                consumes=[{"ref": "scored"}],
            )
        ],
    )


def _manifest(*, physical_name: str = PHYSICAL_CLEAN) -> Manifest:
    ownership = ArtifactOwnership(
        project="payments",
        owner_type="model",
        owner_name="clean",
        mode="managed",
    )
    topic = TopicArtifact(
        name=physical_name,
        partitions=6,
        replication_factor=3,
        ownership=ownership,
    )
    return Manifest(
        version="1.0.0",
        project_name="payments",
        artifacts={"topics": [topic.to_dict()]},
    )


def _changed_plan(*, physical_name: str = PHYSICAL_CLEAN) -> DeploymentPlan:
    return DeploymentPlan(
        topic_changes=[TopicChange(topic=physical_name, action="update")]
    )


def _impact(
    *,
    kafka: object | None = None,
    manifest: Manifest | None = None,
    project: StreamtProject | None = None,
) -> tuple[DeploymentPlan, DeploymentPlanner]:
    plan = _changed_plan()
    planner = DeploymentPlanner(
        manifest or _manifest(),
        project=_project() if project is None else project,
        kafka_deployer=kafka,  # type: ignore[arg-type]
    )
    planner._compute_impact_radius(plan)
    return plan, planner


def test_physical_topic_maps_to_logical_dag_with_transitive_exposure_and_consumers() -> None:
    kafka = MagicMock()
    kafka.get_consumer_groups.return_value = ["mystery-service", "fraud-prod"]
    kafka.get_consumer_group_lag.side_effect = lambda group, topic: ConsumerGroupLag(
        group_id=group,
        topic=topic,
        total_lag=11 if group == "fraud-prod" else 29,
    )

    plan, _ = _impact(kafka=kafka)

    assert len(plan.impact_radius) == 1
    entry = plan.impact_radius[0]
    assert entry.resource == PHYSICAL_CLEAN
    assert entry.logical_type == "model"
    assert entry.logical_name == "clean"
    assert entry.logical_resource == "model/clean"
    assert entry.change_type == "topic_update"
    assert entry.downstream_models == ["enriched", "scored"]
    assert entry.exposures == [
        {
            "name": "fraud_service",
            "owners": ["fraud-team", "risk-platform"],
            "consumer_group": "fraud-prod",
        }
    ]
    assert entry.owners == [
        "feature-platform",
        "fraud-ml",
        "fraud-team",
        "payments-platform",
        "risk-platform",
    ]
    assert entry.consumers == [
        {
            "group_id": "fraud-prod",
            "lag": 11,
            "declared": True,
            "declared_exposures": ["fraud_service"],
        },
        {
            "group_id": "mystery-service",
            "lag": 29,
            "declared": False,
            "declared_exposures": [],
        },
    ]
    assert entry.identity_evidence == {
        "status": "verified",
        "source": "manifest_artifact_ownership",
    }
    assert entry.graph_evidence == {
        "status": "verified",
        "source": "declared_project_dag",
    }
    assert entry.consumer_evidence == {
        "status": "verified",
        "source": "kafka_consumer_groups",
        "reason": None,
        "failures": [],
    }
    assert kafka.get_consumer_group_lag.call_args_list[0].args[1] == PHYSICAL_CLEAN


def test_no_kafka_access_is_explicitly_unavailable_not_clean() -> None:
    plan, _ = _impact()

    entry = plan.impact_radius[0]
    assert entry.consumers == []
    assert entry.consumer_evidence == {
        "status": "unavailable",
        "source": "kafka_consumer_groups",
        "reason": "kafka_not_configured",
        "failures": [],
    }
    assert "consumer evidence: unavailable (kafka_not_configured)" in plan.details(
        color=False
    )


def test_partial_consumer_query_failure_is_preserved_and_redacted() -> None:
    kafka = MagicMock()
    kafka.get_consumer_groups.return_value = ["z-broken", "a-observed"]

    def lag(group: str, topic: str) -> ConsumerGroupLag:
        if group == "z-broken":
            raise RuntimeError(
                "https://admin:super-secret@broker.test password=hunter2 token=abc"
            )
        return ConsumerGroupLag(group_id=group, topic=topic, total_lag=4)

    kafka.get_consumer_group_lag.side_effect = lag

    plan, _ = _impact(kafka=kafka)
    entry = plan.impact_radius[0]

    assert entry.consumers == [
        {
            "group_id": "a-observed",
            "lag": 4,
            "declared": False,
            "declared_exposures": [],
        }
    ]
    assert entry.consumer_evidence == {
        "status": "partial",
        "source": "kafka_consumer_groups",
        "reason": "consumer_queries_failed",
        "failures": [
            {
                "scope": "consumer_group/z-broken",
                "code": "consumer_group_lag_failed",
                "message": "https://***:***@broker.test password=*** token=***",
            }
        ],
    }
    serialized = json.dumps(deployment_plan_payload(plan))
    assert "super-secret" not in serialized
    assert "hunter2" not in serialized
    assert "token=abc" not in serialized


def test_consumer_group_listing_failure_is_unavailable_and_redacted() -> None:
    kafka = MagicMock()
    kafka.get_consumer_groups.side_effect = RuntimeError("authorization=Bearer top-secret")

    plan, _ = _impact(kafka=kafka)

    evidence = plan.impact_radius[0].consumer_evidence
    assert evidence["status"] == "unavailable"
    assert evidence["reason"] == "consumer_group_listing_failed"
    assert evidence["failures"] == [
        {
            "scope": "consumer_group_list",
            "code": "consumer_group_listing_failed",
            "message": "authorization=***",
        }
    ]
    assert "top-secret" not in plan.details(color=False)


def test_missing_manifest_ownership_does_not_guess_from_physical_name() -> None:
    manifest = Manifest(
        version="1.0.0",
        project_name="payments",
        artifacts={
            "topics": [
                TopicArtifact(
                    name=PHYSICAL_CLEAN,
                    partitions=1,
                    replication_factor=1,
                ).to_dict()
            ]
        },
    )

    plan, _ = _impact(manifest=manifest)

    entry = plan.impact_radius[0]
    assert entry.logical_resource is None
    assert entry.downstream_models == []
    assert entry.exposures == []
    assert entry.identity_evidence == {
        "status": "unavailable",
        "reason": "manifest_ownership_missing",
    }
    assert entry.graph_evidence == {
        "status": "unavailable",
        "reason": "logical_identity_unavailable",
    }


def test_missing_project_preserves_identity_and_marks_graph_unavailable() -> None:
    plan = _changed_plan()
    planner = DeploymentPlanner(_manifest(), project=None)

    planner._compute_impact_radius(plan)

    entry = plan.impact_radius[0]
    assert entry.logical_resource == "model/clean"
    assert entry.downstream_models == []
    assert entry.exposures == []
    assert entry.graph_evidence == {
        "status": "unavailable",
        "reason": "project_not_provided",
    }
    assert entry.consumer_evidence["status"] == "unavailable"


def test_impact_order_and_reviewed_payload_are_canonical() -> None:
    second = "aaa.physical"
    manifest = _manifest()
    manifest.artifacts["topics"].insert(
        0,
        TopicArtifact(
            name=second,
            partitions=1,
            replication_factor=1,
            ownership=ArtifactOwnership(
                project="payments",
                owner_type="source",
                owner_name="raw",
            ),
        ).to_dict(),
    )
    plan = DeploymentPlan(
        topic_changes=[
            TopicChange(topic=PHYSICAL_CLEAN, action="update"),
            TopicChange(topic=second, action="create"),
        ]
    )
    planner = DeploymentPlanner(manifest, project=_project())
    planner._compute_impact_radius(plan)

    assert [entry.resource for entry in plan.impact_radius] == [second, PHYSICAL_CLEAN]
    payload = deployment_plan_payload(plan)
    assert [entry["resource"] for entry in payload["impact"]] == [second, PHYSICAL_CLEAN]
    clean = payload["impact"][1]
    assert set(clean) == {
        "resource",
        "logical_type",
        "logical_name",
        "logical_resource",
        "change_type",
        "downstream_models",
        "exposures",
        "owners",
        "consumers",
        "identity_evidence",
        "graph_evidence",
        "consumer_evidence",
    }
    assert clean["logical_resource"] == "model/clean"
    assert clean["exposures"] == [
        {
            "consumer_group": "fraud-prod",
            "name": "fraud_service",
            "owners": ["fraud-team", "risk-platform"],
        }
    ]
    assert clean["owners"] == [
        "feature-platform",
        "fraud-ml",
        "fraud-team",
        "payments-platform",
        "risk-platform",
    ]
    assert clean["consumer_evidence"]["status"] == "unavailable"


def test_reviewed_plan_checks_impact_identity_and_evidence_drift_but_not_lag() -> None:
    kafka = MagicMock()
    kafka.get_consumer_groups.return_value = ["fraud-prod"]
    kafka.get_consumer_group_lag.return_value = ConsumerGroupLag(
        group_id="fraud-prod", topic=PHYSICAL_CLEAN, total_lag=10
    )
    original, planner = _impact(kafka=kafka)
    reviewed = ReviewedPlanFile.create(
        original,
        _manifest(),
        project="payments",
        environment="prod",
        runtime=_project().runtime,
        state=None,
        offline=True,
    )

    kafka.get_consumer_group_lag.return_value = ConsumerGroupLag(
        group_id="fraud-prod", topic=PHYSICAL_CLEAN, total_lag=999
    )
    lag_changed = _changed_plan()
    planner._compute_impact_radius(lag_changed)
    reviewed.verify_current_plan(lag_changed, state_observation=None)

    evidence_changed = _changed_plan()
    evidence_changed.impact_radius = [
        ImpactEntry(
            resource=PHYSICAL_CLEAN,
            change_type="topic_update",
            logical_type="model",
            logical_name="other",
            logical_resource="model/other",
        )
    ]
    with pytest.raises(StalePlanError, match="impact evidence"):
        reviewed.verify_current_plan(evidence_changed, state_observation=None)


def test_offline_plan_includes_graph_impact_with_unavailable_live_evidence() -> None:
    planner = DeploymentPlanner(_manifest(), project=_project())

    plan = planner.offline_plan()

    assert [(entry.resource, entry.logical_resource) for entry in plan.impact_radius] == [
        (PHYSICAL_CLEAN, "model/clean")
    ]
    assert plan.impact_radius[0].consumer_evidence["status"] == "unavailable"


def test_recomputing_impact_replaces_stale_evidence() -> None:
    planner = DeploymentPlanner(_manifest(), project=_project())
    plan = _changed_plan()

    planner._compute_impact_radius(plan)
    plan.impact_radius[0].logical_resource = "stale/value"
    planner._compute_impact_radius(plan)

    assert len(plan.impact_radius) == 1
    assert plan.impact_radius[0].logical_resource == "model/clean"
