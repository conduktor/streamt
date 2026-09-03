"""Deterministic deployment safety blocker tests."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import yaml
from click.testing import CliRunner

from streamt.cli import main
from streamt.compiler.manifest import (
    FlinkJobArtifact,
    Manifest,
    SchemaArtifact,
    TopicArtifact,
)
from streamt.core.deployment_state import local_deployment_state_config
from streamt.deployer.flink import FlinkJobChange, FlinkJobState
from streamt.deployer.kafka import TopicChange, TopicState
from streamt.deployer.plan_file import (
    PLAN_FILE_VERSION,
    PlanFileError,
    ReviewedPlanFile,
    StalePlanError,
    StateReference,
    deployment_plan_payload,
)
from streamt.deployer.planner import (
    DeploymentPlan,
    OwnershipRequirement,
    SafetyBlocker,
)
from streamt.deployer.schema_registry import SchemaChange, SchemaState
from streamt.deployer.state_backend import make_deployment_state_service


def _topic_reduction() -> TopicChange:
    desired = TopicArtifact(
        name="orders.v1",
        partitions=3,
        replication_factor=1,
    )
    return TopicChange(
        topic=desired.name,
        action="update",
        current=TopicState(name=desired.name, exists=True, partitions=12),
        desired=desired,
        changes={"partitions_error": {"message": "cannot reduce 12 to 3"}},
    )


def _schema_incompatible() -> SchemaChange:
    desired = SchemaArtifact(
        subject="orders-value",
        schema={"type": "record", "name": "Order", "fields": []},
        compatibility="BACKWARD",
    )
    return SchemaChange(
        subject=desired.subject,
        action="update",
        current=SchemaState(
            subject=desired.subject,
            exists=True,
            version=4,
            compatibility="BACKWARD",
        ),
        desired=desired,
        changes={
            "schema_incompatible": {
                "message": "not backward compatible",
                "current_version": 4,
            }
        },
    )


def _flink_update() -> FlinkJobChange:
    desired = FlinkJobArtifact(name="orders_processor", sql="INSERT INTO sink SELECT 1")
    return FlinkJobChange(
        job_name=desired.name,
        action="update",
        current=FlinkJobState(
            name=desired.name,
            exists=True,
            job_id="job-123",
            status="RUNNING",
        ),
        desired=desired,
    )


def _blocked_plan() -> DeploymentPlan:
    return DeploymentPlan(
        schema_changes=[_schema_incompatible()],
        topic_changes=[_topic_reduction()],
        flink_changes=[_flink_update()],
    )


def _write_project(path: Path) -> None:
    (path / "stream_project.yml").write_text(
        yaml.safe_dump(
            {
                "apiVersion": "streamt.dev/v1alpha1",
                "project": {"name": "safety-test"},
                "runtime": {"kafka": {"bootstrap_servers": "localhost:9092"}},
            }
        )
    )


def _json(result) -> dict[str, object]:
    return json.loads(result.stdout)


class TestSafetyBlockerModel:
    def test_detects_all_unsafe_updates_in_canonical_backend_order(self) -> None:
        plan = DeploymentPlan(
            flink_changes=[_flink_update()],
            topic_changes=[_topic_reduction()],
            schema_changes=[_schema_incompatible()],
        )

        assert [blocker.code for blocker in plan.ordered_safety_blockers] == [
            "schema_incompatible",
            "kafka_partition_reduction",
            "flink_update_requires_savepoint",
        ]
        assert plan.is_apply_blocked
        assert plan.safety_blockers[0].details == {
            "current_version": 4,
            "compatibility": "BACKWARD",
        }
        assert plan.safety_blockers[1].details == {
            "field": "partitions",
            "current": 12,
            "desired": 3,
        }
        assert plan.safety_blockers[2].details == {"current_status": "RUNNING"}
        assert "3 safety blocker(s)" in plan.summary()
        assert "Safety Blockers:" in plan.details(color=False)

    def test_refresh_uses_final_effective_actions(self) -> None:
        plan = DeploymentPlan()
        reduction = _topic_reduction()
        plan.topic_changes.append(reduction)
        plan.refresh_safety_blockers()
        assert [blocker.code for blocker in plan.safety_blockers] == ["kafka_partition_reduction"]

        reduction.action = "none"
        plan.refresh_safety_blockers()
        assert plan.safety_blockers == []
        assert not plan.is_apply_blocked

    def test_ordering_is_stable_within_a_backend_kind(self) -> None:
        plan = DeploymentPlan(
            safety_blockers=[
                SafetyBlocker(
                    code="schema_incompatible",
                    kind="schema",
                    resource="z-value",
                    action="update",
                    message="blocked",
                ),
                SafetyBlocker(
                    code="schema_incompatible",
                    kind="schema",
                    resource="a-value",
                    action="update",
                    message="blocked",
                ),
            ]
        )

        assert [blocker.resource for blocker in plan.ordered_safety_blockers] == [
            "a-value",
            "z-value",
        ]

    def test_creates_and_noops_are_not_safety_blocked(self) -> None:
        plan = DeploymentPlan(
            schema_changes=[
                SchemaChange(
                    subject="new-value",
                    action="register",
                    changes={"schema_incompatible": {"message": "ignored for create"}},
                ),
                SchemaChange(subject="same-value", action="none"),
            ],
            topic_changes=[
                TopicChange(
                    topic="new-topic",
                    action="create",
                    changes={"partitions_error": {"message": "ignored for create"}},
                ),
                TopicChange(topic="same-topic", action="none"),
            ],
            flink_changes=[
                FlinkJobChange(job_name="new-job", action="submit"),
                FlinkJobChange(job_name="same-job", action="none"),
            ],
        )

        assert plan.safety_blockers == []
        assert not plan.is_apply_blocked

    def test_ownership_and_safety_both_contribute_to_apply_block(self) -> None:
        external_only = DeploymentPlan(
            ownership_requirements=[
                OwnershipRequirement(
                    resource_id="streamt://p/prod/topic/source",
                    kind="topic",
                    logical_name="source",
                    physical_name="source.v1",
                    reason="external",
                    observed_action="none",
                    ownership_mode="external",
                    message="observe only",
                )
            ]
        )
        assert not external_only.is_apply_blocked

        safety_only = DeploymentPlan(topic_changes=[_topic_reduction()])
        assert safety_only.is_apply_blocked


class TestReviewedPlanSafetyBlockers:
    def test_payload_round_trip_is_ordered_checksummed_and_versioned(
        self,
        tmp_path: Path,
    ) -> None:
        plan = _blocked_plan()
        manifest = Manifest(version="1", project_name="safety-test")
        reviewed = ReviewedPlanFile.create(
            plan,
            manifest,
            project="safety-test",
            environment="prod",
            runtime={"kafka": {"bootstrap_servers": "broker:9092"}},
            state=None,
            actions=(),
            offline=True,
        )
        path = tmp_path / "blocked.plan.json"
        reviewed.save(path)
        loaded = ReviewedPlanFile.load(path)

        assert PLAN_FILE_VERSION == 5
        assert loaded == reviewed
        assert [blocker["code"] for blocker in loaded.plan["safety_blockers"]] == [
            "schema_incompatible",
            "kafka_partition_reduction",
            "flink_update_requires_savepoint",
        ]
        assert loaded.plan["summary"]["safety_blockers"] == 3
        assert loaded.plan["summary"]["is_apply_blocked"] is True

        safe = ReviewedPlanFile.create(
            DeploymentPlan(),
            manifest,
            project="safety-test",
            environment="prod",
            runtime={"kafka": {"bootstrap_servers": "broker:9092"}},
            state=None,
            actions=(),
            offline=True,
        )
        assert safe.checksum != reviewed.checksum

    def test_old_v1_plan_is_explicitly_rejected(self, tmp_path: Path) -> None:
        reviewed = ReviewedPlanFile.create(
            DeploymentPlan(),
            Manifest(version="1", project_name="safety-test"),
            project="safety-test",
            environment="prod",
            runtime={},
            state=None,
            actions=(),
            offline=True,
        )
        path = tmp_path / "old.plan.json"
        reviewed.save(path)
        data = json.loads(path.read_text())
        data["format_version"] = 1
        path.write_text(json.dumps(data))

        with pytest.raises(PlanFileError, match="predates exact reviewed action binding"):
            ReviewedPlanFile.load(path)

    def test_verify_current_plan_detects_safety_blocker_drift(self) -> None:
        change = TopicChange(
            topic="orders.v1",
            action="update",
            changes={"config.cleanup.policy": {"from": "delete", "to": "compact"}},
        )
        reviewed_plan = DeploymentPlan(
            topic_changes=[change],
            safety_blockers=[
                SafetyBlocker(
                    code="manual_review",
                    kind="topic",
                    resource="orders.v1",
                    action="update",
                    message="review required",
                )
            ],
        )
        reviewed = ReviewedPlanFile.create(
            reviewed_plan,
            Manifest(version="1", project_name="safety-test"),
            project="safety-test",
            environment="prod",
            runtime={},
            state=None,
            actions=(),
            offline=True,
        )
        current = DeploymentPlan(topic_changes=[change])

        with pytest.raises(StalePlanError, match="safety blockers"):
            reviewed.verify_current_plan(current, actions=(), state_observation=None)

    def test_machine_payload_contains_no_blockers_for_safe_plan(self) -> None:
        payload = deployment_plan_payload(
            DeploymentPlan(
                topic_changes=[TopicChange(topic="new", action="create")],
                flink_changes=[FlinkJobChange(job_name="same", action="none")],
            )
        )

        assert payload["safety_blockers"] == []
        assert payload["summary"]["safety_blockers"] == 0
        assert payload["summary"]["is_apply_blocked"] is False


class TestCliSafetyBlockers:
    def test_plan_succeeds_and_serializes_blockers(self, tmp_path: Path) -> None:
        _write_project(tmp_path)
        blocked = _blocked_plan()
        plan_path = tmp_path / "blocked.plan.json"
        kafka = MagicMock()

        with (
            patch(
                "streamt.cli.commands.plan.make_kafka_deployer",
                return_value=kafka,
            ),
            patch(
                "streamt.deployer.planner.DeploymentPlanner.plan",
                return_value=blocked,
            ),
            patch(
                "streamt.deployer.planner.DeploymentPlanner.planned_actions",
                side_effect=AssertionError("blocked plan derived runtime actions"),
            ) as planned_actions,
        ):
            result = CliRunner().invoke(
                main,
                [
                    "-o",
                    "json",
                    "plan",
                    "-p",
                    str(tmp_path),
                    "--out",
                    str(plan_path),
                ],
            )

        assert result.exit_code == 0, result.output
        payload = _json(result)
        assert payload["status"] == "ok"
        assert payload["data"]["is_apply_blocked"] is True
        assert [blocker["code"] for blocker in payload["data"]["safety_blockers"]] == [
            "schema_incompatible",
            "kafka_partition_reduction",
            "flink_update_requires_savepoint",
        ]
        reviewed = ReviewedPlanFile.load(plan_path)
        assert reviewed.plan["summary"]["safety_blockers"] == 3
        assert reviewed.actions == ()
        planned_actions.assert_not_called()

    @pytest.mark.parametrize("use_reviewed_plan", [False, True])
    def test_apply_rejects_before_any_mutation(
        self,
        tmp_path: Path,
        use_reviewed_plan: bool,
    ) -> None:
        from streamt.compiler import Compiler
        from streamt.core.parser import ProjectParser

        _write_project(tmp_path)
        blocked = _blocked_plan()
        blocked.ownership_requirements.append(
            OwnershipRequirement(
                resource_id="streamt://safety-test/default/topic/source",
                kind="topic",
                logical_name="source",
                physical_name="source.v1",
                reason="requires_adoption",
                observed_action="update",
                ownership_mode="managed",
                message="explicit adoption required",
            )
        )
        args = ["-o", "json", "apply", "-p", str(tmp_path), "--force"]
        if use_reviewed_plan:
            project = ProjectParser(tmp_path).parse()
            manifest = Compiler(project).compile(dry_run=True)
            reviewed = ReviewedPlanFile.create(
                blocked,
                manifest,
                project=project.project.name,
                environment="default",
                runtime=project.runtime,
                state=StateReference.from_observation(
                    make_deployment_state_service(
                        tmp_path,
                        project=project.project.name,
                        environment="default",
                        config=local_deployment_state_config(),
                    ).read()
                ),
                actions=(),
            )
            plan_path = tmp_path / "reviewed.plan.json"
            reviewed.save(plan_path)
            args.extend(["--plan", str(plan_path)])

        kafka = MagicMock()
        with (
            patch(
                "streamt.cli.commands.apply.make_kafka_deployer",
                return_value=kafka,
            ),
            patch(
                "streamt.deployer.planner.DeploymentPlanner.plan",
                return_value=blocked,
            ),
            patch(
                "streamt.deployer.planner.DeploymentPlanner.planned_actions",
                side_effect=AssertionError("blocked plan derived runtime actions"),
            ) as planned_actions,
            patch("streamt.deployer.planner.DeploymentPlanner.apply") as apply_plan,
            patch(
                "streamt.cli.commands.apply.OperationIntent",
                side_effect=AssertionError("blocked plan created an operation intent"),
            ) as operation_intent,
        ):
            result = CliRunner().invoke(main, args)

        assert result.exit_code == 1
        payload = _json(result)
        assert payload["errors"][0]["code"] == "E417_SAFETY_BLOCKED"
        assert "3 unsafe change(s)" in payload["errors"][0]["message"]
        assert "1 resource(s) with unresolved ownership" in payload["errors"][0]["message"]
        assert len(payload["data"]["safety_blockers"]) == 3
        assert len(payload["data"]["blocking_ownership_requirements"]) == 1
        planned_actions.assert_not_called()
        operation_intent.assert_not_called()
        apply_plan.assert_not_called()
        for method in (
            "apply_topic",
            "create_topic",
            "update_topic",
            "delete_topic",
        ):
            getattr(kafka, method).assert_not_called()

    def test_safe_create_and_noop_reach_apply(self, tmp_path: Path) -> None:
        _write_project(tmp_path)
        safe_plan = DeploymentPlan(
            topic_changes=[
                TopicChange(topic="new-topic", action="create"),
                TopicChange(topic="same-topic", action="none"),
            ],
            flink_changes=[FlinkJobChange(job_name="new-job", action="submit")],
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
                "streamt.cli.commands.apply.make_kafka_deployer",
                return_value=MagicMock(),
            ),
            patch(
                "streamt.deployer.planner.DeploymentPlanner.plan",
                return_value=safe_plan,
            ),
            patch(
                "streamt.deployer.planner.DeploymentPlanner.apply",
                return_value=results,
            ) as apply_plan,
        ):
            result = CliRunner().invoke(main, ["apply", "-p", str(tmp_path)])

        assert result.exit_code == 0, result.output
        apply_plan.assert_called_once()
        assert apply_plan.call_args.args == (safe_plan,)
        assert set(apply_plan.call_args.kwargs) == {
            "before_action",
            "after_action",
            "stop_on_error",
        }
        assert apply_plan.call_args.kwargs["stop_on_error"] is True
