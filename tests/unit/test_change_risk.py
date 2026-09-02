"""Canonical resource and plan-level change-risk tests."""

from __future__ import annotations

from pathlib import Path

import pytest

from streamt.compiler.manifest import Manifest
from streamt.deployer.connect import ConnectorChange
from streamt.deployer.flink import FlinkJobChange, FlinkJobState
from streamt.deployer.gateway import AliasTopicState, GatewayRuleChange
from streamt.deployer.kafka import TopicChange, TopicState
from streamt.deployer.plan_file import (
    ReviewedPlanFile,
    StalePlanError,
    deployment_plan_payload,
)
from streamt.deployer.planner import DeploymentPlan, ImpactEntry
from streamt.deployer.schema_registry import SchemaChange, SchemaState
from streamt.integrations.github_action import (
    ActionConfig,
    CommandExecution,
    render_summary,
)


def _manifest() -> Manifest:
    return Manifest(version="1.0", project_name="payments")


def test_change_risks_cover_every_primary_class_with_fail_closed_evidence() -> None:
    plan = DeploymentPlan(
        schema_changes=[
            SchemaChange(
                subject="payments-value",
                action="update",
                current=SchemaState(subject="payments-value", exists=True),
                changes={"schema_incompatible": {"current_version": 3}},
            )
        ],
        topic_changes=[
            TopicChange(
                topic="new-topic",
                action="create",
                current=TopicState(name="new-topic", exists=False),
            ),
            TopicChange(
                topic="payments-topic",
                action="update",
                current=TopicState(name="payments-topic", exists=True),
                changes={"partitions": {"from": 3, "to": 6}},
            ),
        ],
        flink_changes=[
            FlinkJobChange(
                job_name="score-payments",
                action="update",
                current=FlinkJobState(
                    name="score-payments", exists=True, status="RUNNING"
                ),
            )
        ],
        connector_changes=[ConnectorChange(connector_name="legacy", action="delete")],
        gateway_changes=[
            GatewayRuleChange(
                name="routing",
                action="update",
                current_alias=AliasTopicState(name="routing", exists=True),
                changes={},
            )
        ],
        impact_radius=[
            ImpactEntry(
                resource="payments-topic",
                change_type="topic_update",
                downstream_models=["scores"],
                identity_evidence={"status": "verified"},
                graph_evidence={"status": "verified"},
                consumer_evidence={"status": "verified"},
            )
        ],
    )

    risks = [risk.to_dict() for risk in plan.ordered_change_risks]

    assert [(risk["kind"], risk["resource"], risk["assessment"]) for risk in risks] == [
        ("schema", "payments-value", "schema_breaking"),
        ("topic", "new-topic", "safe"),
        ("topic", "payments-topic", "risky"),
        ("flink_job", "score-payments", "state_migration_required"),
        ("connector", "legacy", "destructive"),
        ("gateway_rule", "routing", "unknown"),
    ]
    assert risks[1]["evidence"] == {
        "status": "verified",
        "sources": ["live_resource_state"],
        "reasons": ["resource_absence_verified"],
    }
    assert risks[2]["risk_flags"] == ["consumer_impact"]
    assert risks[3]["risk_flags"] == [
        "live_state_unverified",
        "savepoint_required",
        "stateful_upgrade",
    ]
    assert plan.risk_summary == {
        "overall": "unknown",
        "counts": {
            "safe": 1,
            "risky": 1,
            "schema_breaking": 1,
            "state_migration_required": 1,
            "destructive": 1,
            "unknown": 1,
        },
        "risk_flags": [
            "consumer_impact",
            "destructive",
            "live_state_unverified",
            "policy_violation",
            "savepoint_required",
            "schema_breaking",
            "stateful_upgrade",
        ],
        "evidence_complete": False,
    }
    assert "Risk Classification:" in plan.details(color=False)


def test_compatible_schema_update_is_risky_not_claimed_safe() -> None:
    plan = DeploymentPlan(
        schema_changes=[
            SchemaChange(
                subject="payments-value",
                action="update",
                current=SchemaState(subject="payments-value", exists=True),
                changes={
                    "schema": {
                        "from_version": 3,
                        "to_version": 4,
                        "compatible": True,
                    }
                },
            )
        ]
    )

    assert plan.ordered_change_risks[0].to_dict() == {
        "kind": "schema",
        "resource": "payments-value",
        "action": "update",
        "assessment": "risky",
        "risk_flags": ["schema_impact_unverified"],
        "evidence": {
            "status": "partial",
            "sources": ["schema_registry_compatibility"],
            "reasons": [
                "downstream_contract_impact_unverified",
                "registry_compatibility_verified",
            ],
        },
    }


def test_offline_and_compound_resource_creates_remain_unknown() -> None:
    plan = DeploymentPlan(
        topic_changes=[TopicChange(topic="offline-topic", action="create")],
        gateway_changes=[GatewayRuleChange(name="alias-rule", action="create")],
    )

    assert [risk.to_dict() for risk in plan.ordered_change_risks] == [
        {
            "kind": "topic",
            "resource": "offline-topic",
            "action": "create",
            "assessment": "unknown",
            "risk_flags": ["live_state_unverified"],
            "evidence": {
                "status": "unavailable",
                "sources": [],
                "reasons": ["resource_absence_not_verified"],
            },
        },
        {
            "kind": "gateway_rule",
            "resource": "alias-rule",
            "action": "create",
            "assessment": "unknown",
            "risk_flags": ["live_state_unverified"],
            "evidence": {
                "status": "unavailable",
                "sources": [],
                "reasons": ["resource_absence_not_verified"],
            },
        },
    ]


def test_existing_flink_resubmit_requires_state_evidence_and_blocks_apply() -> None:
    plan = DeploymentPlan(
        flink_changes=[
            FlinkJobChange(
                job_name="score-payments",
                action="submit",
                current=FlinkJobState(
                    name="score-payments", exists=True, status="FAILED"
                ),
            )
        ]
    )

    assert plan.ordered_change_risks[0].assessment == "state_migration_required"
    assert [blocker.code for blocker in plan.ordered_safety_blockers] == [
        "flink_resubmit_requires_state_evidence"
    ]
    assert plan.is_apply_blocked is True


def test_reviewed_plan_checks_risk_classification_drift() -> None:
    original = DeploymentPlan(
        topic_changes=[
            TopicChange(
                topic="new-topic",
                action="create",
                current=TopicState(name="new-topic", exists=False),
            )
        ]
    )
    reviewed = ReviewedPlanFile.create(
        original,
        _manifest(),
        project="payments",
        environment="prod",
        runtime={},
    )
    payload = deployment_plan_payload(original)

    assert payload["risk_summary"] == original.risk_summary
    assert payload["change_risks"][0]["assessment"] == "safe"

    unverified_absence = DeploymentPlan(
        topic_changes=[TopicChange(topic="new-topic", action="create")]
    )
    with pytest.raises(StalePlanError, match="risk classification"):
        reviewed.verify_current_plan(unverified_absence)


def test_github_summary_renders_checksum_verified_risk(tmp_path: Path) -> None:
    plan_path = tmp_path / "reviewed-plan.json"
    reviewed = ReviewedPlanFile.create(
        DeploymentPlan(
            topic_changes=[
                TopicChange(
                    topic="new-topic",
                    action="create",
                    current=TopicState(name="new-topic", exists=False),
                )
            ]
        ),
        _manifest(),
        project="payments",
        environment="prod",
        runtime={},
    )
    reviewed.save(plan_path)
    config = ActionConfig(
        workspace=tmp_path,
        project_directory=tmp_path,
        environment="prod",
        offline=False,
        plan_path=plan_path,
        summary_path=None,
        output_path=None,
    )

    def execution(stage: str, data: dict[str, object]) -> CommandExecution:
        return CommandExecution(
            argv=("streamt", stage),
            returncode=0,
            stdout="",
            stderr="",
            payload={"status": "ok", "data": data},
        )

    summary = render_summary(
        config,
        execution("validate", {}),
        execution(
            "plan",
            {
                "creates": 1,
                "updates": 0,
                "deletes": 0,
                "is_apply_blocked": False,
            },
        ),
    )

    assert "Overall risk: `safe`" in summary
    assert "### Change risk" in summary
    assert "| topic | new-topic | create | safe | — | verified " in summary
