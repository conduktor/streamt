"""Atomic reviewed-action wiring across plan and apply."""

from __future__ import annotations

import json
from collections.abc import Iterator
from contextlib import ExitStack, contextmanager
from pathlib import Path
from unittest.mock import MagicMock, patch

import yaml
from click.testing import CliRunner

from streamt.cli import main
from streamt.compiler import Compiler
from streamt.compiler.manifest import ArtifactOwnership, TopicArtifact
from streamt.core.deployment_state import local_deployment_state_config
from streamt.core.parser import ProjectParser
from streamt.deployer.kafka import TopicChange
from streamt.deployer.operation_actions import operation_actions_from_planned
from streamt.deployer.plan_file import ReviewedPlanFile, StateReference
from streamt.deployer.planner import DeploymentPlan, PlannedAction
from streamt.deployer.state import LocalState, StateIdentityError, resource_id
from streamt.deployer.state_backend import (
    DeploymentStateOperation,
    OperationAction,
    OperationIntent,
    make_deployment_state_service,
)

_DEPLOYER_FACTORIES = (
    "make_sr_deployer",
    "make_kafka_deployer",
    "make_flink_deployer",
    "make_connect_deployer",
    "make_gateway_deployer",
)


def _write_project(path: Path, *, removal: bool = False) -> None:
    config: dict[str, object] = {
        "apiVersion": "streamt.dev/v1alpha1",
        "project": {"name": "plan-test"},
        "runtime": {
            "kafka": {"bootstrap_servers": "broker.invalid:9092"},
            "conduktor": {
                "gateway": {
                    "admin_url": "https://gateway.example.test/admin",
                    "virtual_cluster": "payments",
                }
            },
        },
    }
    if removal:
        config["lifecycle"] = {
            "gateway_rule_removals": [
                {
                    "logical_owner": "orders",
                    "prior_artifact": {
                        "name": "orders_rule",
                        "virtualTopic": "orders.public",
                        "physicalTopic": "orders.raw",
                        "interceptors": [],
                    },
                }
            ]
        }
    (path / "stream_project.yml").write_text(
        yaml.safe_dump(config, sort_keys=False),
        encoding="utf-8",
    )


def _topic_plan_and_action() -> tuple[DeploymentPlan, PlannedAction]:
    desired = TopicArtifact(
        name="orders.clean.v1",
        partitions=3,
        replication_factor=1,
        ownership=ArtifactOwnership(
            project="plan-test",
            owner_type="model",
            owner_name="orders",
            mode="managed",
        ),
    )
    plan = DeploymentPlan(
        topic_changes=[
            TopicChange(
                topic=desired.name,
                action="create",
                desired=desired,
            )
        ]
    )
    action = PlannedAction(
        resource_id=resource_id(
            "plan-test",
            "default",
            "topic",
            "orders",
        ),
        runtime_label="topic:orders.clean.v1",
        action="create",
    )
    return plan, action


def _patch_deployers(stack: ExitStack, command: str) -> None:
    for factory in _DEPLOYER_FACTORIES:
        stack.enter_context(patch(f"streamt.cli.commands.{command}.{factory}", return_value=None))
    stack.enter_context(
        patch(
            f"streamt.cli.commands.{command}.check_required_deployers",
            return_value=True,
        )
    )


def _reviewed_plan(
    project_path: Path,
    deployment_plan: DeploymentPlan,
    actions: tuple[OperationAction, ...],
) -> Path:
    project = ProjectParser(project_path).parse()
    manifest = Compiler(project).compile(dry_run=True)
    service = make_deployment_state_service(
        project_path,
        project=project.project.name,
        environment="default",
        config=local_deployment_state_config(),
    )
    reviewed = ReviewedPlanFile.create(
        deployment_plan,
        manifest,
        project=project.project.name,
        environment="default",
        runtime=project.runtime,
        state=StateReference.from_observation(service.read()),
        actions=actions,
    )
    path = project_path / "reviewed-plan.json"
    reviewed.save(path)
    return path


def test_operation_actions_from_planned_freezes_order_and_indexes() -> None:
    first = PlannedAction(
        resource_id="streamt://plan-test/default/topic/orders",
        runtime_label="topic:orders.clean.v1",
        action="create",
    )
    second = PlannedAction(
        resource_id="streamt://plan-test/default/topic/payments",
        runtime_label="topic:payments.clean.v1",
        action="update",
    )

    result = operation_actions_from_planned([first, second])

    assert result == (
        OperationAction(index=0, resource_id=first.resource_id, action="create"),
        OperationAction(index=1, resource_id=second.resource_id, action="update"),
    )


def test_plan_holds_operation_lock_through_action_freeze_and_atomic_save(
    tmp_path: Path,
) -> None:
    _write_project(tmp_path)
    deployment_plan, planned_action = _topic_plan_and_action()
    plan_path = tmp_path / "reviewed-plan.json"
    project = ProjectParser(tmp_path).parse()
    inner_service = make_deployment_state_service(
        tmp_path,
        project=project.project.name,
        environment="default",
        config=local_deployment_state_config(),
    )
    lock_held = False
    action_frozen = False

    class TrackingService:
        @contextmanager
        def operation(self) -> Iterator[DeploymentStateOperation]:
            nonlocal lock_held
            with inner_service.operation() as operation:
                lock_held = True
                try:
                    yield operation
                finally:
                    lock_held = False

    original_save = ReviewedPlanFile.save
    original_freeze = operation_actions_from_planned

    def freeze(actions: list[PlannedAction]) -> tuple[OperationAction, ...]:
        nonlocal action_frozen
        assert lock_held
        action_frozen = True
        return original_freeze(actions)

    def save(reviewed: ReviewedPlanFile, path: Path) -> None:
        assert lock_held
        assert action_frozen
        original_save(reviewed, path)

    def close(*_deployers: object) -> None:
        assert action_frozen

    with ExitStack() as stack:
        _patch_deployers(stack, "plan")
        stack.enter_context(
            patch(
                "streamt.cli.commands.plan.make_deployment_state_service",
                return_value=TrackingService(),
            )
        )
        stack.enter_context(
            patch(
                "streamt.deployer.planner.DeploymentPlanner.plan",
                return_value=deployment_plan,
            )
        )
        planned_actions = stack.enter_context(
            patch(
                "streamt.deployer.planner.DeploymentPlanner.planned_actions",
                return_value=[planned_action],
            )
        )
        freeze_actions = stack.enter_context(
            patch(
                "streamt.cli.commands.plan.operation_actions_from_planned",
                side_effect=freeze,
            )
        )
        stack.enter_context(patch("streamt.cli.commands.plan.close_deployers", side_effect=close))
        stack.enter_context(patch.object(ReviewedPlanFile, "save", new=save))
        result = CliRunner().invoke(
            main,
            ["plan", "-p", str(tmp_path), "--out", str(plan_path)],
        )

    assert result.exit_code == 0, result.output
    planned_actions.assert_called_once_with(deployment_plan)
    freeze_actions.assert_called_once()
    assert ReviewedPlanFile.load(plan_path).actions == (
        OperationAction(
            index=0,
            resource_id=planned_action.resource_id,
            action=planned_action.action,
        ),
    )


def test_plan_removal_preflight_fails_before_any_deployer_construction(
    tmp_path: Path,
) -> None:
    _write_project(tmp_path, removal=True)
    factories: list[MagicMock] = []
    with ExitStack() as stack:
        for factory in _DEPLOYER_FACTORIES:
            factories.append(
                stack.enter_context(
                    patch(
                        f"streamt.cli.commands.plan.{factory}",
                        side_effect=AssertionError("deployer constructed before preflight"),
                    )
                )
            )
        preflight = stack.enter_context(
            patch(
                "streamt.deployer.planner.resolve_gateway_planning_targets",
                side_effect=StateIdentityError("invalid Gateway removal evidence"),
            )
        )
        result = CliRunner().invoke(
            main,
            ["-o", "json", "plan", "-p", str(tmp_path)],
        )

    assert result.exit_code == 1, result.output
    assert json.loads(result.stdout)["errors"][0]["code"] == "E411_STATE_INVALID"
    assert preflight.call_count == 1
    assert preflight.call_args.kwargs["require_authoritative_state"] is True
    assert isinstance(preflight.call_args.kwargs["prior_state"], LocalState)
    for factory in factories:
        factory.assert_not_called()


def test_apply_removal_preflight_fails_before_any_deployer_construction(
    tmp_path: Path,
) -> None:
    _write_project(tmp_path, removal=True)
    plan_path = _reviewed_plan(tmp_path, DeploymentPlan(), ())
    factories: list[MagicMock] = []
    with ExitStack() as stack:
        for factory in _DEPLOYER_FACTORIES:
            factories.append(
                stack.enter_context(
                    patch(
                        f"streamt.cli.commands.apply.{factory}",
                        side_effect=AssertionError("deployer constructed before preflight"),
                    )
                )
            )
        preflight = stack.enter_context(
            patch(
                "streamt.deployer.planner.resolve_gateway_planning_targets",
                side_effect=StateIdentityError("invalid Gateway removal evidence"),
            )
        )
        result = CliRunner().invoke(
            main,
            [
                "-o",
                "json",
                "apply",
                "-p",
                str(tmp_path),
                "--plan",
                str(plan_path),
            ],
        )

    assert result.exit_code == 1, result.output
    assert json.loads(result.stdout)["errors"][0]["code"] == "E411_STATE_INVALID"
    assert preflight.call_count == 1
    assert preflight.call_args.kwargs["require_authoritative_state"] is True
    assert isinstance(preflight.call_args.kwargs["prior_state"], LocalState)
    for factory in factories:
        factory.assert_not_called()


def test_apply_reuses_one_exact_action_tuple_for_review_and_intent(
    tmp_path: Path,
) -> None:
    _write_project(tmp_path)
    deployment_plan, planned_action = _topic_plan_and_action()
    frozen_actions = operation_actions_from_planned([planned_action])
    plan_path = _reviewed_plan(tmp_path, deployment_plan, frozen_actions)
    verify_actions: list[tuple[OperationAction, ...]] = []
    intent_actions: list[tuple[OperationAction, ...]] = []
    original_verify = ReviewedPlanFile.verify_current_plan
    original_intent = OperationIntent

    def verify(
        reviewed: ReviewedPlanFile,
        current_plan: DeploymentPlan,
        *,
        actions: tuple[OperationAction, ...],
        state_observation: object,
    ) -> None:
        verify_actions.append(actions)
        original_verify(
            reviewed,
            current_plan,
            actions=actions,
            state_observation=state_observation,  # type: ignore[arg-type]
        )

    def make_intent(**kwargs: object) -> OperationIntent:
        actions = kwargs["actions"]
        assert isinstance(actions, tuple)
        intent_actions.append(actions)
        return original_intent(**kwargs)  # type: ignore[arg-type]

    def apply_plan(
        _plan: DeploymentPlan,
        *,
        before_action: object,
        after_action: object,
        stop_on_error: bool,
    ) -> dict[str, object]:
        assert stop_on_error is True
        before_action(planned_action.runtime_label, 0)  # type: ignore[operator]
        after_action(planned_action.runtime_label, 0, True)  # type: ignore[operator]
        return {
            "created": [planned_action.runtime_label],
            "updated": [],
            "deleted": [],
            "unchanged": [],
            "errors": [],
            "rollback_candidates": [],
        }

    with ExitStack() as stack:
        _patch_deployers(stack, "apply")
        stack.enter_context(
            patch(
                "streamt.deployer.planner.DeploymentPlanner.plan",
                return_value=deployment_plan,
            )
        )
        ordered_actions = [planned_action]
        planned_actions = stack.enter_context(
            patch(
                "streamt.deployer.planner.DeploymentPlanner.planned_actions",
                return_value=ordered_actions,
            )
        )
        freeze_actions = stack.enter_context(
            patch(
                "streamt.cli.commands.apply.operation_actions_from_planned",
                return_value=frozen_actions,
            )
        )
        stack.enter_context(patch.object(ReviewedPlanFile, "verify_current_plan", new=verify))
        stack.enter_context(
            patch("streamt.cli.commands.apply.OperationIntent", side_effect=make_intent)
        )
        stack.enter_context(
            patch(
                "streamt.deployer.planner.DeploymentPlanner.apply",
                side_effect=apply_plan,
            )
        )
        result = CliRunner().invoke(
            main,
            ["apply", "-p", str(tmp_path), "--plan", str(plan_path)],
        )

    assert result.exit_code == 0, result.output
    planned_actions.assert_called_once_with(deployment_plan)
    freeze_actions.assert_called_once_with(ordered_actions)
    assert len(verify_actions) == 2
    assert all(actions is frozen_actions for actions in verify_actions)
    assert intent_actions == [frozen_actions]
    assert intent_actions[0] is frozen_actions


def test_apply_rejects_reviewed_action_drift_before_mutation(tmp_path: Path) -> None:
    _write_project(tmp_path)
    deployment_plan, planned_action = _topic_plan_and_action()
    reviewed_actions = operation_actions_from_planned([planned_action])
    plan_path = _reviewed_plan(tmp_path, deployment_plan, reviewed_actions)
    drifted_action = PlannedAction(
        resource_id=planned_action.resource_id,
        runtime_label=planned_action.runtime_label,
        action="update",
    )

    with ExitStack() as stack:
        _patch_deployers(stack, "apply")
        stack.enter_context(
            patch(
                "streamt.deployer.planner.DeploymentPlanner.plan",
                return_value=deployment_plan,
            )
        )
        stack.enter_context(
            patch(
                "streamt.deployer.planner.DeploymentPlanner.planned_actions",
                return_value=[drifted_action],
            )
        )
        apply_plan = stack.enter_context(patch("streamt.deployer.planner.DeploymentPlanner.apply"))
        result = CliRunner().invoke(
            main,
            ["-o", "json", "apply", "-p", str(tmp_path), "--plan", str(plan_path)],
        )

    assert result.exit_code == 1, result.output
    assert json.loads(result.stdout)["errors"][0]["code"] == "E409_PLAN_STALE"
    apply_plan.assert_not_called()
