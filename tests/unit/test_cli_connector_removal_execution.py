"""Reviewed CLI execution boundary for exact managed Connector removals."""

from __future__ import annotations

import json
from contextlib import ExitStack
from pathlib import Path
from typing import cast
from unittest.mock import MagicMock, patch

import yaml
from click.testing import CliRunner, Result

from streamt.cli import main
from streamt.cli.connector_removal_guard import (
    CONNECTOR_REMOVAL_DRIFT_MESSAGE,
    emit_connector_removal_destructive_warning,
)
from streamt.compiler import Compiler
from streamt.compiler.manifest import ArtifactOwnership, TopicArtifact
from streamt.core.deployment_state import local_deployment_state_config
from streamt.core.parser import ProjectParser
from streamt.deployer.connect import (
    ConnectClusterBinding,
    ConnectorChange,
    ManagedConnectorObservation,
    managed_connector_absence_fingerprint,
)
from streamt.deployer.kafka import TopicChange
from streamt.deployer.operation_actions import operation_actions_from_planned
from streamt.deployer.plan_file import ReviewedPlanFile, StateReference
from streamt.deployer.planner import (
    ConnectorRemovalAssessment,
    DeploymentPlan,
    PlannedAction,
)
from streamt.deployer.state import (
    LocalState,
    ManagedResourceRecord,
    StateFormatError,
    local_state_path,
    resource_id,
)
from streamt.deployer.state_backend import (
    ConnectorActionEvidence,
    ConnectorActionSurfaceEvidence,
    DeploymentStateService,
    OperationAction,
    make_deployment_state_service,
)
from streamt.output import OutputFormatter

_ENDPOINT = "https://connect.example.test:8443/api/"
_CONNECTOR_NAME = "archive-orders-sink"
_RESOURCE_ID = "streamt://payments/default/connector/archive_orders"
_PRIOR_CHECKSUM = "sha256:" + "7" * 64
_BINDING = ConnectClusterBinding.from_endpoint("primary", _ENDPOINT)
_DEPLOYER_FACTORIES = (
    "make_sr_deployer",
    "make_kafka_deployer",
    "make_flink_deployer",
    "make_connect_deployer",
    "make_gateway_deployer",
)


def _write_project(path: Path) -> None:
    config = {
        "apiVersion": "streamt.dev/v1alpha1",
        "project": {"name": "payments"},
        "runtime": {
            "kafka": {"bootstrap_servers": "broker.invalid:9092"},
            "connect": {
                "default": "primary",
                "clusters": {"primary": {"rest_url": _ENDPOINT}},
            },
        },
        "deployment_state": {
            "backend": "postgres",
            "namespace": "test",
            "postgres": {
                "dsn_env": "STREAMT_TEST_ADMIN_DSN",
                "writer_dsn_env": "STREAMT_TEST_WRITER_DSN",
            },
        },
        "lifecycle": {
            "connector_removals": [
                {
                    "logical_owner": "archive_orders",
                    "name": _CONNECTOR_NAME,
                    "cluster": "primary",
                }
            ]
        },
    }
    (path / "stream_project.yml").write_text(
        yaml.safe_dump(config, sort_keys=False),
        encoding="utf-8",
    )


def _state_service(
    path: Path,
    *,
    include_connector_record: bool = True,
) -> DeploymentStateService:
    state = LocalState(
        project="payments",
        environment="default",
        resources=(
            {
                _RESOURCE_ID: ManagedResourceRecord(
                    physical_name=_CONNECTOR_NAME,
                    ownership="managed",
                    artifact_checksum=_PRIOR_CHECKSUM,
                    backend=_BINDING.backend_identity,
                )
            }
            if include_connector_record
            else {}
        ),
    )
    state.save(local_state_path(path, environment="default"))
    return make_deployment_state_service(
        path,
        project="payments",
        environment="default",
        config=local_deployment_state_config(),
    )


def _connector_plan_and_action() -> tuple[DeploymentPlan, PlannedAction]:
    current = ManagedConnectorObservation(
        binding=_BINDING,
        name=_CONNECTOR_NAME,
        exists=True,
        config=(
            ("connector.class", "com.example.ArchiveSink"),
            ("name", _CONNECTOR_NAME),
            ("topics", "orders.v1"),
        ),
    )
    plan = DeploymentPlan(
        connector_changes=[
            ConnectorChange(
                connector_name=_CONNECTOR_NAME,
                action="delete",
                current=current,
                backend_identity=_BINDING.backend_identity,
            )
        ]
    )
    action = PlannedAction(
        resource_id=_RESOURCE_ID,
        runtime_label=f"connector:{_CONNECTOR_NAME}",
        action="delete",
        connector_evidence=ConnectorActionEvidence(
            version=1,
            backend_identity=_BINDING.backend_identity,
            connector_name=_CONNECTOR_NAME,
            prior_artifact_checksum=_PRIOR_CHECKSUM,
            current=ConnectorActionSurfaceEvidence(
                exists=True,
                fingerprint=current.fingerprint,
            ),
            desired=ConnectorActionSurfaceEvidence(
                exists=False,
                fingerprint=managed_connector_absence_fingerprint(
                    _BINDING.backend_identity,
                    _CONNECTOR_NAME,
                ),
            ),
        ),
    )
    return plan, action


def _reviewed_plan(
    path: Path,
    service: DeploymentStateService,
    plan: DeploymentPlan,
    actions: tuple[OperationAction, ...],
) -> Path:
    project = ProjectParser(path).parse()
    manifest = Compiler(project).compile(dry_run=True)
    reviewed = ReviewedPlanFile.create(
        plan,
        manifest,
        project="payments",
        environment="default",
        runtime=project.runtime,
        state=StateReference.from_observation(service.read()),
        actions=actions,
    )
    plan_path = path / "connector-removal.plan.json"
    reviewed.save(plan_path)
    return plan_path


def _payload(result: Result) -> dict[str, object]:
    return cast(dict[str, object], json.loads(result.stdout))


def _patch_apply(
    stack: ExitStack,
    *,
    service: DeploymentStateService,
    plan: DeploymentPlan,
    actions: list[PlannedAction],
) -> None:
    stack.enter_context(
        patch(
            "streamt.cli.commands.apply.make_deployment_state_service",
            return_value=service,
        )
    )
    stack.enter_context(
        patch(
            "streamt.deployer.planner.resolve_connector_planning_targets",
            return_value=MagicMock(),
        )
    )
    for factory in _DEPLOYER_FACTORIES:
        stack.enter_context(
            patch(
                f"streamt.cli.commands.apply.{factory}",
                return_value=MagicMock(),
            )
        )
    stack.enter_context(
        patch("streamt.cli.commands.apply.check_required_deployers", return_value=True)
    )
    stack.enter_context(
        patch("streamt.deployer.planner.DeploymentPlanner.plan", return_value=plan)
    )
    stack.enter_context(
        patch(
            "streamt.deployer.planner.DeploymentPlanner.planned_actions",
            return_value=actions,
        )
    )


def test_aggregate_warning_counts_only_exact_connector_delete_actions() -> None:
    _plan, connector = _connector_plan_and_action()
    ordinary = PlannedAction(
        resource_id="streamt://payments/default/topic/orders",
        runtime_label="topic:orders.v1",
        action="create",
    )
    actions = operation_actions_from_planned([ordinary, connector])
    formatter = OutputFormatter("json", quiet=True)

    count = emit_connector_removal_destructive_warning(formatter, actions)

    assert count == 1
    assert [warning.to_dict() for warning in formatter.get_result().warnings] == [
        {
            "code": "W119_CONNECTOR_REMOVAL_DESTRUCTIVE",
            "message": "Planned Connector removal is destructive (1 delete(s))",
        }
    ]
    warning = formatter.get_result().warnings[0].message
    assert _CONNECTOR_NAME not in warning
    assert _RESOURCE_ID not in warning
    assert _ENDPOINT not in warning


def test_plan_emits_w119_before_atomic_reviewed_plan_save(tmp_path: Path) -> None:
    _write_project(tmp_path)
    service = _state_service(tmp_path)
    plan, action = _connector_plan_and_action()
    out = tmp_path / "planned.json"
    events: list[str] = []
    original_emit = emit_connector_removal_destructive_warning
    original_save = ReviewedPlanFile.save

    def emit(formatter: OutputFormatter, actions: tuple[OperationAction, ...]) -> int:
        events.append("warning")
        return original_emit(formatter, actions)

    def save(reviewed: ReviewedPlanFile, target: Path) -> None:
        events.append("save")
        original_save(reviewed, target)

    with ExitStack() as stack:
        stack.enter_context(
            patch(
                "streamt.cli.commands.plan.make_deployment_state_service",
                return_value=service,
            )
        )
        stack.enter_context(
            patch(
                "streamt.deployer.planner.resolve_connector_planning_targets",
                return_value=MagicMock(),
            )
        )
        for factory in _DEPLOYER_FACTORIES:
            stack.enter_context(
                patch(f"streamt.cli.commands.plan.{factory}", return_value=MagicMock())
            )
        stack.enter_context(
            patch("streamt.cli.commands.plan.check_required_deployers", return_value=True)
        )
        stack.enter_context(
            patch("streamt.deployer.planner.DeploymentPlanner.plan", return_value=plan)
        )
        stack.enter_context(
            patch(
                "streamt.deployer.planner.DeploymentPlanner.planned_actions",
                return_value=[action],
            )
        )
        stack.enter_context(
            patch(
                "streamt.cli.commands.plan.emit_connector_removal_destructive_warning",
                side_effect=emit,
            )
        )
        stack.enter_context(patch.object(ReviewedPlanFile, "save", new=save))
        result = CliRunner().invoke(
            main,
            ["-o", "json", "plan", "-p", str(tmp_path), "--out", str(out)],
        )

    assert result.exit_code == 0, result.output
    assert events == ["warning", "save"]
    assert out.exists()
    assert _payload(result)["warnings"] == [
        {
            "code": "W119_CONNECTOR_REMOVAL_DESTRUCTIVE",
            "message": "Planned Connector removal is destructive (1 delete(s))",
        }
    ]


def test_apply_refuses_actionable_removal_without_destructive_authorization(
    tmp_path: Path,
) -> None:
    _write_project(tmp_path)
    service = _state_service(tmp_path)
    prior = service.read().state
    plan, action = _connector_plan_and_action()
    reviewed_path = _reviewed_plan(
        tmp_path,
        service,
        plan,
        operation_actions_from_planned([action]),
    )

    with ExitStack() as stack:
        _patch_apply(stack, service=service, plan=plan, actions=[action])
        apply_plan = stack.enter_context(
            patch("streamt.deployer.planner.DeploymentPlanner.apply")
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
                str(reviewed_path),
            ],
        )

    assert result.exit_code == 1, result.output
    payload = _payload(result)
    assert payload["errors"][0]["code"] == "E503_ENVIRONMENT_ERROR"  # type: ignore[index]
    assert payload["warnings"] == []
    apply_plan.assert_not_called()
    assert service.read().state == prior
    assert service.read_control().control.status == "clear"


def test_already_absent_reviewed_removal_needs_no_destructive_override(
    tmp_path: Path,
) -> None:
    _write_project(tmp_path)
    service = _state_service(tmp_path, include_connector_record=False)
    plan = DeploymentPlan(
        connector_removal_assessments=(
            ConnectorRemovalAssessment(
                resource_id=_RESOURCE_ID,
                logical_owner="archive_orders",
                connector_name=_CONNECTOR_NAME,
                backend_identity=_BINDING.backend_identity,
                status="already_absent",
            ),
        )
    )
    reviewed_path = _reviewed_plan(tmp_path, service, plan, ())

    with ExitStack() as stack:
        _patch_apply(stack, service=service, plan=plan, actions=[])
        apply_plan = stack.enter_context(
            patch(
                "streamt.deployer.planner.DeploymentPlanner.apply",
                return_value={
                    "created": [],
                    "updated": [],
                    "deleted": [],
                    "unchanged": [],
                    "errors": [],
                    "rollback_candidates": [],
                },
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
                str(reviewed_path),
            ],
        )

    assert result.exit_code == 0, result.output
    payload = _payload(result)
    assert payload["warnings"] == []
    assert payload["data"]["connector_removal_assessments"] == [  # type: ignore[index]
        {
            "resource_id": _RESOURCE_ID,
            "logical_owner": "archive_orders",
            "connector_name": _CONNECTOR_NAME,
            "backend_identity": _BINDING.backend_identity,
            "status": "already_absent",
        }
    ]
    apply_plan.assert_called_once()
    assert service.read().state.serial == 0
    assert service.read().state.resources == {}
    assert service.read_control().control.status == "clear"


def test_successful_apply_projects_only_durably_completed_connector_delete(
    tmp_path: Path,
) -> None:
    _write_project(tmp_path)
    service = _state_service(tmp_path)
    plan, action = _connector_plan_and_action()
    reviewed_path = _reviewed_plan(
        tmp_path,
        service,
        plan,
        operation_actions_from_planned([action]),
    )

    def apply_plan(
        _plan: DeploymentPlan,
        *,
        before_action: object,
        after_action: object,
        stop_on_error: bool,
    ) -> dict[str, object]:
        assert stop_on_error is True
        before_action(action.runtime_label, 0)  # type: ignore[operator]
        after_action(action.runtime_label, 0, True)  # type: ignore[operator]
        return {
            "created": [],
            "updated": [],
            "deleted": [action.runtime_label],
            "unchanged": [],
            "errors": [],
            "rollback_candidates": [],
        }

    with ExitStack() as stack:
        _patch_apply(stack, service=service, plan=plan, actions=[action])
        stack.enter_context(
            patch(
                "streamt.deployer.planner.DeploymentPlanner.apply",
                side_effect=apply_plan,
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
                str(reviewed_path),
                "--force",
            ],
        )

    assert result.exit_code == 0, result.output
    payload = _payload(result)
    assert payload["warnings"] == [
        {
            "code": "W000_WARNING",
            "message": "--force used, allowing destructive ops on 'default'",
        },
        {
            "code": "W119_CONNECTOR_REMOVAL_DESTRUCTIVE",
            "message": "Planned Connector removal is destructive (1 delete(s))",
        },
    ]
    committed = service.read().state
    assert committed.serial == 1
    assert committed.resources == {}
    assert service.read_control().control.status == "clear"


def test_invalid_ordinary_state_projection_fails_before_any_provider_mutation(
    tmp_path: Path,
) -> None:
    _write_project(tmp_path)
    service = _state_service(tmp_path)
    plan, action = _connector_plan_and_action()
    reviewed_path = _reviewed_plan(
        tmp_path,
        service,
        plan,
        operation_actions_from_planned([action]),
    )

    with ExitStack() as stack:
        _patch_apply(stack, service=service, plan=plan, actions=[action])
        projection = stack.enter_context(
            patch(
                "streamt.cli.commands.apply.updated_local_state",
                side_effect=StateFormatError("invalid ordinary state projection"),
            )
        )
        apply_plan = stack.enter_context(
            patch("streamt.deployer.planner.DeploymentPlanner.apply")
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
                str(reviewed_path),
                "--force",
            ],
        )

    assert result.exit_code == 1, result.output
    assert _payload(result)["errors"][0]["code"] == "E411_STATE_INVALID"  # type: ignore[index]
    projection.assert_called_once()
    apply_plan.assert_not_called()
    assert service.read_control().control.status == "clear"
    retained = service.read().state
    assert retained.serial == 0
    assert _RESOURCE_ID in retained.resources


def test_uncertain_connector_delete_is_public_and_durable_e428_without_rollback(
    tmp_path: Path,
) -> None:
    _write_project(tmp_path)
    service = _state_service(tmp_path)
    connector_plan, connector_action = _connector_plan_and_action()
    topic = TopicArtifact(
        name="created-before-failure",
        partitions=1,
        replication_factor=1,
        ownership=ArtifactOwnership(
            project="payments",
            owner_type="model",
            owner_name="created_before_failure",
            mode="managed",
        ),
    )
    topic_action = PlannedAction(
        resource_id=resource_id(
            "payments",
            "default",
            "topic",
            "created_before_failure",
        ),
        runtime_label="topic:created-before-failure",
        action="create",
    )
    plan = DeploymentPlan(
        topic_changes=[TopicChange(topic=topic.name, action="create", desired=topic)],
        connector_changes=connector_plan.connector_changes,
    )
    actions = [topic_action, connector_action]
    reviewed_path = _reviewed_plan(
        tmp_path,
        service,
        plan,
        operation_actions_from_planned(actions),
    )

    def fail_connector(
        _plan: DeploymentPlan,
        *,
        before_action: object,
        after_action: object,
        stop_on_error: bool,
    ) -> dict[str, object]:
        assert stop_on_error is True
        before_action(topic_action.runtime_label, 0)  # type: ignore[operator]
        after_action(topic_action.runtime_label, 0, True)  # type: ignore[operator]
        before_action(connector_action.runtime_label, 1)  # type: ignore[operator]
        after_action(connector_action.runtime_label, 1, False)  # type: ignore[operator]
        return {
            "created": [topic_action.runtime_label],
            "updated": [],
            "deleted": [],
            "unchanged": [],
            "errors": ["connector leaked provider-token=do-not-print"],
            "rollback_candidates": [topic_action.runtime_label],
        }

    with ExitStack() as stack:
        _patch_apply(stack, service=service, plan=plan, actions=actions)
        stack.enter_context(
            patch(
                "streamt.deployer.planner.DeploymentPlanner.apply",
                side_effect=fail_connector,
            )
        )
        rollback = stack.enter_context(
            patch("streamt.deployer.planner.DeploymentPlanner.rollback")
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
                str(reviewed_path),
                "--force",
            ],
        )

    assert result.exit_code == 1, result.output
    payload = _payload(result)
    assert payload["errors"][0]["code"] == "E428_CONNECTOR_REMOVAL_DRIFT"  # type: ignore[index]
    assert payload["errors"][0]["message"] == CONNECTOR_REMOVAL_DRIFT_MESSAGE  # type: ignore[index]
    assert payload["data"]["errors"] == [CONNECTOR_REMOVAL_DRIFT_MESSAGE]  # type: ignore[index]
    assert "do-not-print" not in result.output
    rollback.assert_not_called()

    blocked = service.read_control().control
    assert blocked.status == "recovery_required"
    assert blocked.recovery is not None
    assert blocked.recovery.failure_code == "connector_removal_drift"
    assert blocked.recovery.last_completed_action_index == 0
    assert [
        (progress.action_index, progress.status, progress.succeeded)
        for progress in blocked.progress
    ] == [
        (0, "started", None),
        (0, "completed", True),
        (1, "started", None),
        (1, "completed", False),
    ]
    retained = service.read().state
    assert retained.serial == 0
    assert retained.resources[_RESOURCE_ID].artifact_checksum == _PRIOR_CHECKSUM
