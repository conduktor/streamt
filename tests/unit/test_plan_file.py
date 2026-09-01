"""Tests for deterministic reviewed plan files and their CLI workflow."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

import pytest
import yaml
from click.testing import CliRunner

from streamt.cli import main
from streamt.compiler.manifest import Manifest, TopicArtifact
from streamt.deployer.connect import ConnectorChange
from streamt.deployer.kafka import TopicChange
from streamt.deployer.plan_file import (
    PlanFileError,
    ReviewedPlanFile,
    StalePlanError,
    deployment_plan_payload,
)
from streamt.deployer.planner import DeploymentPlan, OwnershipRequirement


def _manifest(*, compiled_at: str = "2026-01-01T00:00:00Z") -> Manifest:
    return Manifest(
        version="1.0",
        project_name="payments",
        compiled_at=compiled_at,
        artifacts={
            "topics": [
                TopicArtifact(
                    name="payments.clean.v1", partitions=3, replication_factor=1
                ).to_dict()
            ]
        },
    )


def _deployment_plan() -> DeploymentPlan:
    topic = TopicArtifact(name="payments.clean.v1", partitions=3, replication_factor=1)
    return DeploymentPlan(
        topic_changes=[
            TopicChange(topic=topic.name, action="create", desired=topic),
            TopicChange(topic="unchanged.v1", action="none"),
        ]
    )


def _reviewed_plan() -> ReviewedPlanFile:
    return ReviewedPlanFile.create(
        _deployment_plan(),
        _manifest(),
        project="payments",
        environment="prod",
        runtime={"kafka": {"bootstrap_servers": "broker:9092"}},
    )


def _ownership_requirement(reason: str = "requires_adoption") -> OwnershipRequirement:
    return OwnershipRequirement(
        resource_id="streamt://payments/prod/topic/payments_clean",
        kind="topic",
        logical_name="payments_clean",
        physical_name="payments.clean.v1",
        reason=reason,
        observed_action="update",
        ownership_mode="external" if reason == "external" else "managed",
        message="An explicit ownership decision is required.",
    )


def _write_project(path: Path) -> None:
    config = {
        "apiVersion": "streamt.dev/v1alpha1",
        "project": {"name": "plan-test"},
        "runtime": {"kafka": {"bootstrap_servers": "localhost:9092"}},
    }
    (path / "stream_project.yml").write_text(yaml.safe_dump(config))


def _json_output(result: object) -> dict[str, object]:
    return json.loads(result.stdout)


def test_plan_file_is_deterministic_and_excludes_compile_time(tmp_path: Path) -> None:
    first = ReviewedPlanFile.create(
        _deployment_plan(),
        _manifest(compiled_at="2026-01-01T00:00:00Z"),
        project="payments",
        environment="prod",
        runtime={"kafka": {"bootstrap_servers": "broker:9092"}},
    )
    second = ReviewedPlanFile.create(
        _deployment_plan(),
        _manifest(compiled_at="2026-09-01T12:34:56Z"),
        project="payments",
        environment="prod",
        runtime={"kafka": {"bootstrap_servers": "broker:9092"}},
    )
    first_path = tmp_path / "first.plan.json"
    second_path = tmp_path / "second.plan.json"
    first.save(first_path)
    second.save(second_path)

    assert first.checksum == second.checksum
    assert first.manifest_checksum == second.manifest_checksum
    assert first_path.read_bytes() == second_path.read_bytes()
    assert not list(tmp_path.glob(".*.tmp"))


def test_plan_file_load_detects_tampering(tmp_path: Path) -> None:
    path = tmp_path / "reviewed.plan.json"
    _reviewed_plan().save(path)
    data = json.loads(path.read_text())
    data["plan"]["resources"][0]["action"] = "delete"
    path.write_text(json.dumps(data))

    with pytest.raises(PlanFileError, match="checksum mismatch"):
        ReviewedPlanFile.load(path)


def test_plan_file_rejects_duplicate_json_fields(tmp_path: Path) -> None:
    path = tmp_path / "duplicate.plan.json"
    path.write_text('{"kind":"streamt.reviewed-plan","kind":"changed"}')

    with pytest.raises(PlanFileError, match="duplicate field 'kind'"):
        ReviewedPlanFile.load(path)


def test_context_verification_detects_project_environment_and_manifest_drift() -> None:
    reviewed = _reviewed_plan()
    runtime = {"kafka": {"bootstrap_servers": "broker:9092"}}

    reviewed.verify_context(
        _manifest(), project="payments", environment="prod", runtime=runtime
    )
    with pytest.raises(StalePlanError, match="does not match current project"):
        reviewed.verify_context(
            _manifest(), project="other", environment="prod", runtime=runtime
        )
    with pytest.raises(StalePlanError, match="does not match 'stage'"):
        reviewed.verify_context(
            _manifest(), project="payments", environment="stage", runtime=runtime
        )
    with pytest.raises(StalePlanError, match="runtime endpoints"):
        reviewed.verify_context(
            _manifest(),
            project="payments",
            environment="prod",
            runtime={"kafka": {"bootstrap_servers": "other:9092"}},
        )

    changed_manifest = _manifest()
    changed_manifest.artifacts["topics"][0]["partitions"] = 12
    with pytest.raises(StalePlanError, match="project content changed"):
        reviewed.verify_context(
            changed_manifest,
            project="payments",
            environment="prod",
            runtime=runtime,
        )


def test_context_verification_checks_optional_state_serial() -> None:
    reviewed = ReviewedPlanFile.create(
        _deployment_plan(),
        _manifest(),
        project="payments",
        environment="prod",
        runtime={"kafka": {"bootstrap_servers": "broker:9092"}},
        state_serial=7,
    )
    runtime = {"kafka": {"bootstrap_servers": "broker:9092"}}

    reviewed.verify_context(
        _manifest(),
        project="payments",
        environment="prod",
        runtime=runtime,
        state_serial=7,
    )
    with pytest.raises(StalePlanError, match="state serial 7"):
        reviewed.verify_context(
            _manifest(),
            project="payments",
            environment="prod",
            runtime=runtime,
            state_serial=8,
        )


def test_live_action_drift_is_rejected_but_impact_metrics_may_change() -> None:
    reviewed = _reviewed_plan()
    same_actions = _deployment_plan()
    same_actions.impact_radius = []
    reviewed.verify_current_plan(same_actions)

    drifted = DeploymentPlan(
        topic_changes=[TopicChange(topic="payments.clean.v1", action="none")]
    )
    with pytest.raises(StalePlanError, match="live resource actions"):
        reviewed.verify_current_plan(drifted)


def test_plan_payload_redacts_sensitive_change_evidence() -> None:
    plan = DeploymentPlan(
        connector_changes=[
            ConnectorChange(
                connector_name="sink",
                action="update",
                changes={
                    "config": {
                        "password": {"from": "old-secret", "to": "new-secret"},
                        "basic.auth.user.info": "alice:kafka-password",
                        "sasl.jaas.config": "username=alice password=jaas-password",
                        "url": "https://alice:super-secret@example.test/path",
                    }
                },
            )
        ]
    )

    serialized = json.dumps(deployment_plan_payload(plan))
    assert "old-secret" not in serialized
    assert "new-secret" not in serialized
    assert "kafka-password" not in serialized
    assert "jaas-password" not in serialized
    assert "super-secret" not in serialized
    assert "<redacted>" in serialized


def test_plan_payload_includes_sorted_ownership_requirements() -> None:
    plan = DeploymentPlan(
        ownership_requirements=[
            OwnershipRequirement(
                resource_id="streamt://payments/prod/topic/z",
                kind="topic",
                logical_name="z",
                physical_name="z.v1",
                reason="requires_adoption",
                observed_action="update",
                ownership_mode="managed",
                message="Adopt z first.",
            ),
            OwnershipRequirement(
                resource_id="streamt://payments/prod/topic/a",
                kind="topic",
                logical_name="a",
                physical_name="a.v1",
                reason="external",
                observed_action="none",
                ownership_mode="external",
                message="a is observe-only.",
            ),
        ]
    )

    payload = deployment_plan_payload(plan)

    assert payload["summary"]["ownership_requirements"] == 2
    assert [
        requirement["logical_name"] for requirement in payload["ownership_requirements"]
    ] == ["a", "z"]


def test_cli_saves_and_applies_reviewed_plan(tmp_path: Path) -> None:
    _write_project(tmp_path)
    plan_path = tmp_path / "reviewed.plan.json"
    runner = CliRunner()

    planned = runner.invoke(
        main,
        ["-o", "json", "plan", "-p", str(tmp_path), "--offline", "--out", str(plan_path)],
    )
    assert planned.exit_code == 0, planned.output
    planned_output = _json_output(planned)
    assert planned_output["data"]["plan_file"] == str(plan_path)
    assert plan_path.exists()

    applied = runner.invoke(
        main, ["-o", "json", "apply", "-p", str(tmp_path), "--plan", str(plan_path)]
    )
    assert applied.exit_code == 0, applied.output
    applied_output = _json_output(applied)
    assert applied_output["data"]["plan_checksum"] == ReviewedPlanFile.load(plan_path).checksum


def test_cli_rejects_tampered_and_stale_plan_files(tmp_path: Path) -> None:
    _write_project(tmp_path)
    plan_path = tmp_path / "reviewed.plan.json"
    runner = CliRunner()
    result = runner.invoke(
        main, ["plan", "-p", str(tmp_path), "--offline", "--out", str(plan_path)]
    )
    assert result.exit_code == 0, result.output

    original = plan_path.read_text()
    data = json.loads(original)
    data["offline"] = False
    plan_path.write_text(json.dumps(data))
    tampered = runner.invoke(
        main, ["-o", "json", "apply", "-p", str(tmp_path), "--plan", str(plan_path)]
    )
    assert tampered.exit_code == 1
    assert _json_output(tampered)["errors"][0]["code"] == "E408_PLAN_FILE_INVALID"

    plan_path.write_text(original)
    config = yaml.safe_load((tmp_path / "stream_project.yml").read_text())
    config["project"]["name"] = "renamed-project"
    (tmp_path / "stream_project.yml").write_text(yaml.safe_dump(config))
    stale = runner.invoke(
        main, ["-o", "json", "apply", "-p", str(tmp_path), "--plan", str(plan_path)]
    )
    assert stale.exit_code == 1
    assert _json_output(stale)["errors"][0]["code"] == "E409_PLAN_STALE"


def test_cli_rejects_selection_with_reviewed_plan(tmp_path: Path) -> None:
    _write_project(tmp_path)
    plan_path = tmp_path / "reviewed.plan.json"
    result = CliRunner().invoke(
        main, ["plan", "-p", str(tmp_path), "--offline", "--out", str(plan_path)]
    )
    assert result.exit_code == 0, result.output

    applied = CliRunner().invoke(
        main,
        [
            "-o",
            "json",
            "apply",
            "-p",
            str(tmp_path),
            "--plan",
            str(plan_path),
            "--target",
            "anything",
        ],
    )
    assert applied.exit_code == 1
    payload = _json_output(applied)
    assert payload["errors"][0]["code"] == "E408_PLAN_FILE_INVALID"


def test_cli_reports_missing_plan_file_as_structured_error(tmp_path: Path) -> None:
    _write_project(tmp_path)

    result = CliRunner().invoke(
        main,
        [
            "-o",
            "json",
            "apply",
            "-p",
            str(tmp_path),
            "--plan",
            str(tmp_path / "missing.plan.json"),
        ],
    )

    assert result.exit_code == 1
    assert _json_output(result)["errors"][0]["code"] == "E408_PLAN_FILE_INVALID"


def test_cli_rejects_live_plan_drift_before_apply(tmp_path: Path) -> None:
    _write_project(tmp_path)
    plan_path = tmp_path / "reviewed.plan.json"
    runner = CliRunner()
    result = runner.invoke(
        main, ["plan", "-p", str(tmp_path), "--offline", "--out", str(plan_path)]
    )
    assert result.exit_code == 0, result.output

    changed_live_plan = DeploymentPlan(
        topic_changes=[TopicChange(topic="new-live-topic", action="create")]
    )
    with (
        patch(
            "streamt.deployer.planner.DeploymentPlanner.plan",
            return_value=changed_live_plan,
        ),
        patch("streamt.deployer.planner.DeploymentPlanner.apply") as apply_plan,
    ):
        applied = runner.invoke(
            main,
            ["-o", "json", "apply", "-p", str(tmp_path), "--plan", str(plan_path)],
        )

    assert applied.exit_code == 1
    assert _json_output(applied)["errors"][0]["code"] == "E409_PLAN_STALE"
    apply_plan.assert_not_called()


def test_cli_saved_plan_fails_closed_on_blocking_ownership_requirement(
    tmp_path: Path,
) -> None:
    from streamt.compiler import Compiler
    from streamt.core.parser import ProjectParser

    _write_project(tmp_path)
    project = ProjectParser(tmp_path).parse()
    manifest = Compiler(project).compile(dry_run=True)
    blocked_plan = DeploymentPlan(
        ownership_requirements=[_ownership_requirement()]
    )
    reviewed = ReviewedPlanFile.create(
        blocked_plan,
        manifest,
        project=project.project.name,
        environment="default",
        runtime=project.runtime,
    )
    plan_path = tmp_path / "blocked.plan.json"
    reviewed.save(plan_path)

    with (
        patch(
            "streamt.deployer.planner.DeploymentPlanner.plan",
            return_value=blocked_plan,
        ),
        patch("streamt.deployer.planner.DeploymentPlanner.apply") as apply_plan,
    ):
        result = CliRunner().invoke(
            main,
            ["-o", "json", "apply", "-p", str(tmp_path), "--plan", str(plan_path)],
        )

    assert result.exit_code == 1
    payload = _json_output(result)
    assert payload["errors"][0]["code"] == "E410_OWNERSHIP_REQUIRED"
    assert payload["data"]["blocking_ownership_requirements"][0]["reason"] == (
        "requires_adoption"
    )
    apply_plan.assert_not_called()


def test_cli_external_ownership_visibility_does_not_block_other_apply(tmp_path: Path) -> None:
    _write_project(tmp_path)
    visible_plan = DeploymentPlan(
        ownership_requirements=[_ownership_requirement(reason="external")]
    )
    results = {
        "created": [],
        "updated": [],
        "deleted": [],
        "unchanged": [],
        "errors": [],
        "rollback_candidates": [],
        "summary": {"total": 0, "succeeded": 0, "failed": 0, "unchanged": 0},
    }

    with (
        patch(
            "streamt.deployer.planner.DeploymentPlanner.plan",
            return_value=visible_plan,
        ),
        patch(
            "streamt.deployer.planner.DeploymentPlanner.apply",
            return_value=results,
        ) as apply_plan,
    ):
        result = CliRunner().invoke(main, ["apply", "-p", str(tmp_path)])

    assert result.exit_code == 0, result.output
    apply_plan.assert_called_once_with(visible_plan)
