"""Local CLI lifecycle coverage for explicit Gateway rule removals."""

from __future__ import annotations

import json
from contextlib import ExitStack
from pathlib import Path
from unittest.mock import MagicMock, patch

import yaml
from click.testing import CliRunner

from streamt.cli import main
from streamt.compiler import Compiler
from streamt.core.deployment_state import local_deployment_state_config
from streamt.core.parser import ProjectParser
from streamt.deployer.gateway import (
    GatewayBackendBinding,
    GatewayDeployer,
    ManagedGatewayRuleObservation,
    managed_gateway_absence_fingerprint,
)
from streamt.deployer.kafka import KafkaDeployer
from streamt.deployer.plan_file import PLAN_FILE_VERSION, ReviewedPlanFile
from streamt.deployer.state import (
    LocalState,
    ManagedResourceRecord,
    artifact_checksum,
    local_state_path,
    resource_id,
)
from streamt.deployer.state_backend import (
    OperationControlState,
    make_deployment_state_service,
)

_PROJECT = "removal-test"
_ENVIRONMENT = "default"
_OWNER = "orders_view"
_RULE = "orders_access_rule"
_ALIAS = "orders.public"
_PHYSICAL = "orders.raw"
_ADMIN_URL = "https://gateway.example.test/admin"
_VIRTUAL_CLUSTER = "payments"


def _write_project(path: Path) -> None:
    project = {
        "apiVersion": "streamt.dev/v1alpha1",
        "project": {"name": _PROJECT},
        "runtime": {
            "kafka": {"bootstrap_servers": "broker.invalid:9092"},
            "conduktor": {
                "gateway": {
                    "admin_url": _ADMIN_URL,
                    "password": "gateway-plan-secret",
                    "virtual_cluster": _VIRTUAL_CLUSTER,
                }
            },
        },
        "lifecycle": {
            "gateway_rule_removals": [
                {
                    "logical_owner": _OWNER,
                    "prior_artifact": {
                        "name": _RULE,
                        "virtualTopic": _ALIAS,
                        "physicalTopic": _PHYSICAL,
                        "interceptors": [],
                    },
                }
            ]
        },
    }
    (path / "stream_project.yml").write_text(
        yaml.safe_dump(project, sort_keys=False),
        encoding="utf-8",
    )


def _json(result: object) -> dict[str, object]:
    return json.loads(result.stdout)  # type: ignore[attr-defined, no-any-return]


def test_local_reviewed_gateway_removal_lifecycle(tmp_path: Path) -> None:
    _write_project(tmp_path)
    project = ProjectParser(tmp_path).parse()
    manifest = Compiler(project).compile(dry_run=True)
    raw_removals = manifest.artifacts["gateway_rule_removals"]
    assert len(raw_removals) == 1
    raw_removal = raw_removals[0]
    prior_artifact = raw_removal["priorArtifact"]
    assert raw_removal["logicalOwner"] == _OWNER
    assert isinstance(prior_artifact, dict)

    binding = GatewayBackendBinding.from_endpoint(
        _ADMIN_URL,
        virtual_cluster=_VIRTUAL_CLUSTER,
    )
    removed_resource_id = resource_id(
        _PROJECT,
        _ENVIRONMENT,
        "gateway_rule",
        _OWNER,
    )
    unrelated_resource_id = resource_id(
        _PROJECT,
        _ENVIRONMENT,
        "topic",
        "audit_log",
    )
    unrelated_record = ManagedResourceRecord(
        physical_name="audit.events.v1",
        ownership="managed",
        artifact_checksum=artifact_checksum({"name": "audit.events.v1"}),
        backend="direct-kafka",
    )
    initial_state = LocalState(
        project=_PROJECT,
        environment=_ENVIRONMENT,
        serial=7,
        resources={
            removed_resource_id: ManagedResourceRecord(
                physical_name=_ALIAS,
                ownership="managed",
                artifact_checksum=artifact_checksum(prior_artifact),
                backend=binding.backend_identity,
            ),
            unrelated_resource_id: unrelated_record,
        },
    )
    state_path = local_state_path(tmp_path, environment=_ENVIRONMENT)
    initial_state.save(state_path)

    provider_state = {"present": True}
    snapshot_observations: list[ManagedGatewayRuleObservation] = []
    durable_progress_history: list[OperationControlState] = []
    gateway = MagicMock(spec=GatewayDeployer)
    gateway.cluster_binding = binding
    kafka = MagicMock(spec=KafkaDeployer)

    def observe_snapshot() -> MagicMock:
        current = ManagedGatewayRuleObservation(
            binding=binding,
            logical_name=_RULE,
            alias_name=_ALIAS,
            exists=provider_state["present"],
            physical_name=_PHYSICAL if provider_state["present"] else None,
            physical_cluster="main" if provider_state["present"] else None,
        )
        snapshot_observations.append(current)
        snapshot = MagicMock()
        snapshot.binding = binding

        def rule(rule_name: str, alias_name: str) -> ManagedGatewayRuleObservation:
            assert (rule_name, alias_name) == (_RULE, _ALIAS)
            return current

        snapshot.rule.side_effect = rule
        return snapshot

    gateway.observe_managed_gateway_snapshot.side_effect = observe_snapshot
    state_service = make_deployment_state_service(
        tmp_path,
        project=_PROJECT,
        environment=_ENVIRONMENT,
        config=local_deployment_state_config(),
    )

    def delete_rule(current: ManagedGatewayRuleObservation) -> str:
        assert current == snapshot_observations[-1]
        assert current.logical_name == _RULE
        assert current.alias_name == _ALIAS
        assert current.exists is True
        durable_control = state_service.read_control().control
        durable_progress_history.append(durable_control)
        assert durable_control.status == "in_progress"
        assert durable_control.intent is not None
        assert durable_control.progress[-1].status == "started"
        assert durable_control.progress[-1].resource_id == removed_resource_id
        provider_state["present"] = False
        return "deleted"

    gateway.delete_managed_gateway_rule.side_effect = delete_rule
    reviewed_path = tmp_path / "gateway-removal.plan.json"
    completed_path = tmp_path / "gateway-removal-completed.plan.json"
    runner = CliRunner()

    with ExitStack() as stack:
        for command in ("plan", "apply"):
            stack.enter_context(
                patch(
                    f"streamt.cli.commands.{command}.make_kafka_deployer",
                    return_value=kafka,
                )
            )
            stack.enter_context(
                patch(
                    f"streamt.cli.commands.{command}.make_gateway_deployer",
                    return_value=gateway,
                )
            )

        planned = runner.invoke(
            main,
            ["-o", "json", "plan", "-p", str(tmp_path), "--out", str(reviewed_path)],
        )
        assert planned.exit_code == 0, planned.output
        planned_payload = _json(planned)
        assert planned_payload["data"]["deletes"] == 1  # type: ignore[index]
        assert planned_payload["data"]["gateway_removal_assessments"] == []  # type: ignore[index]

        reviewed = ReviewedPlanFile.load(reviewed_path)
        assert reviewed.to_dict()["format_version"] == PLAN_FILE_VERSION
        assert reviewed.plan["resources"] == [
            {
                "kind": "gateway_rule",
                "name": _RULE,
                "action": "delete",
                "changes": {
                    "categories": ["presence"],
                    "current": {
                        "exists": True,
                        "fingerprint": snapshot_observations[-1].fingerprint,
                        "managed_interceptor_count": 0,
                    },
                    "desired": {
                        "exists": False,
                        "fingerprint": managed_gateway_absence_fingerprint(
                            binding.backend_identity,
                            _RULE,
                            _ALIAS,
                        ),
                        "managed_interceptor_count": 0,
                    },
                },
            }
        ]
        assert len(reviewed.actions) == 1
        reviewed_action = reviewed.actions[0]
        assert reviewed_action.index == 0
        assert reviewed_action.resource_id == removed_resource_id
        assert reviewed_action.action == "delete"
        assert reviewed_action.gateway_evidence is not None
        assert reviewed_action.gateway_evidence.backend_identity == binding.backend_identity
        assert reviewed_action.gateway_evidence.rule_name == _RULE
        assert reviewed_action.gateway_evidence.alias_name == _ALIAS
        assert reviewed_action.gateway_evidence.current.exists is True
        assert reviewed_action.gateway_evidence.desired.exists is False
        reviewed_wire = reviewed_path.read_text(encoding="utf-8")
        for secret in (_ADMIN_URL, "gateway-plan-secret", _PHYSICAL):
            assert secret not in reviewed_wire

        blocked = runner.invoke(
            main,
            ["-o", "json", "apply", "-p", str(tmp_path), "--plan", str(reviewed_path)],
        )
        assert blocked.exit_code == 1
        blocked_payload = _json(blocked)
        assert blocked_payload["status"] == "error"
        assert "Destructive ops blocked" in blocked_payload["errors"][0]["message"]  # type: ignore[index]
        gateway.delete_managed_gateway_rule.assert_not_called()
        assert LocalState.load(state_path).to_dict() == initial_state.to_dict()
        assert state_service.read_control().control.status == "clear"

        applied = runner.invoke(
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
        assert applied.exit_code == 0, applied.output
        applied_payload = _json(applied)
        assert applied_payload["data"]["deleted"] == [f"gateway_rule:{_RULE}"]  # type: ignore[index]
        assert applied_payload["data"]["committed"] is True  # type: ignore[index]
        assert applied_payload["data"]["state_serial"] == initial_state.serial + 1  # type: ignore[index]

        gateway.delete_managed_gateway_rule.assert_called_once()
        assert len(durable_progress_history) == 1
        durable_control = durable_progress_history[0]
        assert durable_control.intent is not None
        assert durable_control.intent.reviewed_plan_checksum == reviewed.checksum
        assert durable_control.intent.actions == reviewed.actions
        assert [
            (entry.action_index, entry.action, entry.status, entry.succeeded)
            for entry in durable_control.progress
        ] == [(0, "delete", "started", None)]

        committed_state = LocalState.load(
            state_path,
            expected_project=_PROJECT,
            expected_environment=_ENVIRONMENT,
        )
        assert committed_state.serial == initial_state.serial + 1
        assert committed_state.resources == {unrelated_resource_id: unrelated_record}
        assert state_service.read_control().control.status == "clear"

        completed_plan = runner.invoke(
            main,
            ["-o", "json", "plan", "-p", str(tmp_path), "--out", str(completed_path)],
        )
        assert completed_plan.exit_code == 0, completed_plan.output
        completed_payload = _json(completed_plan)
        assert completed_payload["data"]["deletes"] == 0  # type: ignore[index]
        assert completed_payload["data"]["changes"] == []  # type: ignore[index]
        assert completed_payload["data"]["gateway_removal_assessments"] == [  # type: ignore[index]
            {
                "resource_id": removed_resource_id,
                "logical_owner": _OWNER,
                "rule_name": _RULE,
                "alias_name": _ALIAS,
                "backend_identity": binding.backend_identity,
                "status": "already_absent",
            }
        ]
        completed_reviewed = ReviewedPlanFile.load(completed_path)
        assert completed_reviewed.actions == ()
        assert completed_reviewed.plan["resources"] == []
        assert (
            completed_reviewed.plan["gateway_removal_assessments"]
            == completed_payload["data"]["gateway_removal_assessments"]
        )  # type: ignore[index]

        state_before_completed_apply = LocalState.load(state_path).to_dict()
        completed_apply = runner.invoke(
            main,
            ["-o", "json", "apply", "-p", str(tmp_path), "--plan", str(completed_path)],
        )
        assert completed_apply.exit_code == 0, completed_apply.output
        completed_apply_payload = _json(completed_apply)
        assert completed_apply_payload["data"]["deleted"] == []  # type: ignore[index]
        assert completed_apply_payload["data"]["committed"] is True  # type: ignore[index]
        assert LocalState.load(state_path).to_dict() == state_before_completed_apply
        assert state_service.read_control().control.status == "clear"

    gateway.delete_managed_gateway_rule.assert_called_once()
    gateway.apply_managed_gateway_rule.assert_not_called()
    assert len(snapshot_observations) == 5
    assert [observation.exists for observation in snapshot_observations] == [
        True,
        True,
        True,
        False,
        False,
    ]
