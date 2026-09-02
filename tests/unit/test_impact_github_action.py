"""GitHub rendering tests for canonical impact evidence."""

from pathlib import Path

from streamt.compiler.manifest import Manifest
from streamt.deployer.plan_file import ReviewedPlanFile
from streamt.deployer.planner import DeploymentPlan, ImpactEntry
from streamt.integrations.github_action import (
    ActionConfig,
    CommandExecution,
    render_summary,
)


def _execution(stage: str, data: dict[str, object]) -> CommandExecution:
    return CommandExecution(
        argv=("streamt", stage),
        returncode=0,
        stdout="",
        stderr="",
        payload={"status": "ok", "data": data},
    )


def test_summary_renders_checksum_verified_impact_evidence(tmp_path: Path) -> None:
    plan_path = tmp_path / "reviewed-plan.json"
    impact = ImpactEntry(
        resource="prod.payments.clean.v2",
        change_type="topic_update",
        logical_type="model",
        logical_name="clean",
        logical_resource="model/clean",
        downstream_models=["enriched"],
        exposures=[
            {
                "name": "fraud_service",
                "owners": ["risk-platform"],
                "consumer_group": "fraud-prod",
            }
        ],
        owners=["payments-platform", "risk-platform"],
        consumers=[
            {
                "group_id": "fraud-prod",
                "lag": 14,
                "declared": True,
                "declared_exposures": ["fraud_service"],
            },
            {
                "group_id": "unknown-service",
                "lag": 2,
                "declared": False,
                "declared_exposures": [],
            },
        ],
        identity_evidence={"status": "verified"},
        graph_evidence={"status": "verified"},
        consumer_evidence={
            "status": "partial",
            "source": "kafka_consumer_groups",
            "reason": "consumer_queries_failed",
            "failures": [
                {
                    "scope": "consumer_group/broken",
                    "code": "consumer_group_lag_failed",
                    "message": "password=do-not-render",
                }
            ],
        },
    )
    reviewed = ReviewedPlanFile.create(
        DeploymentPlan(impact_radius=[impact]),
        Manifest(version="1.0", project_name="payments"),
        project="payments",
        environment="prod",
        runtime={"kafka": {"bootstrap_servers": "broker:9092"}},
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

    summary = render_summary(
        config,
        _execution("validate", {}),
        _execution(
            "plan",
            {
                "creates": 0,
                "updates": 1,
                "deletes": 0,
                "is_apply_blocked": False,
            },
        ),
    )

    assert "### Impact evidence" in summary
    assert "prod.payments.clean.v2" in summary
    assert "model/clean" in summary
    assert "enriched" in summary
    assert "fraud_service (risk-platform)" in summary
    assert "payments-platform, risk-platform" in summary
    assert "fraud-prod (declared, lag=14)" in summary
    assert "unknown-service (undeclared, lag=2)" in summary
    assert "partial (consumer_queries_failed); 1 failure(s)" in summary
    assert "do-not-render" not in summary
