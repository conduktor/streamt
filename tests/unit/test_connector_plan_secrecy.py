"""Secret-neutral Kafka Connect plan evidence."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock

from streamt.cli.commands.plan import _connector_change_data
from streamt.compiler.manifest import ConnectorArtifact, Manifest
from streamt.deployer.connect import ConnectDeployer, ConnectorChange, ConnectorState
from streamt.deployer.plan_file import ReviewedPlanFile
from streamt.deployer.planner import DeploymentPlan


def _planned_secret_change() -> ConnectorChange:
    deployer = ConnectDeployer("http://connect.test:8083")
    deployer.get_connector_state = MagicMock(  # type: ignore[method-assign]
        return_value=ConnectorState(
            name="payments-sink",
            exists=True,
            config={
                "name": "payments-sink",
                "connector.class": "example.SinkConnector",
                "topics": "payments",
                "password": "old-password",
                "endpoint": "https://alice:old-url-secret@example.test/old-path",
                "mode": "old-mode",
                "removed.option": "removed-value",
            },
        )
    )
    artifact = ConnectorArtifact(
        name="payments-sink",
        connector_class="example.SinkConnector",
        topics=["payments"],
        config={
            "password": "new-password",
            "endpoint": "https://bob:new-url-secret@example.test/new-path",
            "mode": "new-mode",
            "added.option": "added-value",
        },
    )
    try:
        return deployer.plan_connector(artifact)
    finally:
        deployer.close()


def test_connector_plan_keeps_only_changed_keys_and_fingerprint_evidence() -> None:
    first = _planned_secret_change()
    second = _planned_secret_change()

    assert first.action == "update"
    assert first.changes == second.changes
    assert first.changes["password"]["change"] == "changed"
    assert first.changes["added.option"]["change"] == "added"
    assert first.changes["removed.option"]["change"] == "removed"
    assert first.changes["added.option"]["from_present"] is False
    assert first.changes["removed.option"]["to_present"] is False
    assert first.changes["mode"]["from_fingerprint"].startswith("sha256:")
    assert first.changes["mode"]["to_fingerprint"].startswith("sha256:")

    rendered = json.dumps(first.changes, sort_keys=True)
    for raw_value in (
        "old-password",
        "new-password",
        "old-url-secret",
        "new-url-secret",
        "old-path",
        "new-path",
        "old-mode",
        "new-mode",
        "removed-value",
        "added-value",
    ):
        assert raw_value not in rendered


def test_connector_plan_text_and_json_resanitize_legacy_raw_diffs(tmp_path: Path) -> None:
    change = ConnectorChange(connector_name="payments-sink", action="update")
    # Boundary serializers must remain safe even if a caller mutates the public
    # dataclass with a legacy raw diff after construction.
    change.changes = {
        "password": {"from": "legacy-old-secret", "to": "legacy-new-secret"},
        "endpoint": {
            "from": "https://alice:old-embedded@example.test/a",
            "to": "https://bob:new-embedded@example.test/b",
        },
        "mode": {"from": "copy", "to": "move"},
    }

    deployment_plan = DeploymentPlan(connector_changes=[change])
    text_plan = deployment_plan.details()
    json_plan = json.dumps(_connector_change_data(change), sort_keys=True)
    reviewed_plan = ReviewedPlanFile.create(
        deployment_plan,
        Manifest(version="1.0", project_name="payments", artifacts={}),
        project="payments",
        environment="test",
        runtime={},
        state=None,
        offline=True,
    )
    reviewed_plan_path = tmp_path / "reviewed-plan.json"
    reviewed_plan.save(reviewed_plan_path)
    saved_plan = reviewed_plan_path.read_text()

    for output in (text_plan, json_plan, saved_plan):
        assert "password" in output
        assert "endpoint" in output
        assert "mode" in output
        assert "sha256:" in output
        for raw_value in (
            "legacy-old-secret",
            "legacy-new-secret",
            "old-embedded",
            "new-embedded",
            "https://alice",
            "https://bob",
            "copy",
            "move",
        ):
            assert raw_value not in output


def test_connector_change_repr_excludes_current_and_desired_configs() -> None:
    change = _planned_secret_change()

    rendered = repr(change)

    assert "old-password" not in rendered
    assert "new-password" not in rendered
    assert "new-url-secret" not in rendered
    assert "sha256:" in rendered


def test_removed_connector_config_key_keeps_destructive_risk_flag() -> None:
    change = _planned_secret_change()

    risk = DeploymentPlan(connector_changes=[change]).ordered_change_risks[0]

    assert risk.assessment == "risky"
    assert risk.risk_flags == ("destructive",)


def test_connector_plan_comparison_preserves_case_and_json_types() -> None:
    deployer = ConnectDeployer("http://connect.test:8083")
    deployer.get_connector_state = MagicMock(  # type: ignore[method-assign]
        return_value=ConnectorState(
            name="payments-sink",
            exists=True,
            config={
                "name": "payments-sink",
                "connector.class": "example.SinkConnector",
                "topics": "Payments",
                "tasks.max": "1",
                "enabled": 1,
            },
        )
    )
    artifact = ConnectorArtifact(
        name="payments-sink",
        connector_class="example.SinkConnector",
        topics=["payments"],
        config={"tasks.max": 1, "enabled": True},
    )

    try:
        change = deployer.plan_connector(artifact)
    finally:
        deployer.close()

    assert change.action == "update"
    assert set(change.changes) == {"topics", "tasks.max", "enabled"}
    assert all(delta["change"] == "changed" for delta in change.changes.values())
