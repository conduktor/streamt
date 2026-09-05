"""Lossless planner evidence binding without enabling runner replacement."""

from __future__ import annotations

from dataclasses import FrozenInstanceError
from pathlib import Path
from types import SimpleNamespace

import pytest

from streamt.deployer.operation_actions import operation_actions_from_planned
from streamt.deployer.plan_file import PlanFileError, ReviewedPlanFile, canonical_json
from streamt.deployer.planner import PlannedAction
from streamt.deployer.state import StateFormatError, StateIdentityError
from streamt.deployer.state_backend import CURRENT_CONTROL_VERSION, OperationAction
from tests.unit.test_kafka_streams_operation_evidence import RESOURCE, TOKEN, _evidence
from tests.unit.test_kafka_streams_reviewed_plan import _plan, _review, _token_schema_evidence
from tests.unit.test_operation_control import _connector_action, _gateway_action
from tests.unit.test_planner_kafka_streams import _planner


def _planned(evidence=None, **changes):
    values = {
        "resource_id": RESOURCE,
        "runtime_label": "kafka_streams_job:filtered",
        "action": "update",
        "kafka_streams_evidence": evidence if evidence is not None else _evidence(),
    }
    values.update(changes)
    return PlannedAction(**values)


def test_conversion_keeps_exact_evidence_reference_order_and_v4_bytes() -> None:
    evidence = _evidence()  # Real SQL compiler and strict artifact/evidence constructors.
    runner = _planned(evidence)
    topic = PlannedAction("streamt://payments/prod/topic/input", "topic:input", "create")
    pending = [topic, runner]
    converted = operation_actions_from_planned(pending)
    pending.reverse()
    assert isinstance(converted, tuple)
    assert [item.resource_id for item in converted] == [topic.resource_id, runner.resource_id]
    assert [item.index for item in converted] == [0, 1]
    assert runner.kafka_streams_evidence is evidence
    assert converted[1].kafka_streams_evidence is evidence
    assert converted[1]._wire_version == 4
    expected = OperationAction(1, RESOURCE, "update", kafka_streams_evidence=evidence)
    assert canonical_json(converted[1].to_dict()) == canonical_json(expected.to_dict())
    assert converted[1].to_dict()["kafka_streams_evidence"] == evidence.to_dict()
    with pytest.raises(FrozenInstanceError):
        runner.kafka_streams_evidence = None
    with pytest.raises(FrozenInstanceError):
        converted[1].kafka_streams_evidence = None
    exported = converted[1].to_dict()
    exported["kafka_streams_evidence"]["desired_artifact"]["plan"]["predicates"].clear()
    assert converted[1].to_dict() == expected.to_dict()


def test_legacy_defaults_and_positional_fields_keep_old_serialization() -> None:
    ordinary = PlannedAction("streamt://payments/prod/topic/input", "topic:input", "create")
    durable = operation_actions_from_planned([ordinary])[0]
    assert ordinary.kafka_streams_evidence is None
    assert durable._wire_version == CURRENT_CONTROL_VERSION == 3
    assert durable.to_dict() == {
        "index": 0, "resource_id": ordinary.resource_id, "action": "create",
        "gateway_evidence": None, "connector_evidence": None,
    }
    for expected in (_gateway_action(), _connector_action()):
        planned = PlannedAction(
            expected.resource_id, "fixture:legacy", expected.action,
            expected.gateway_evidence, expected.connector_evidence,
        )
        result = operation_actions_from_planned([planned])[0]
        assert result.gateway_evidence is expected.gateway_evidence
        assert result.connector_evidence is expected.connector_evidence
        assert result.kafka_streams_evidence is None
        assert canonical_json(result.to_dict()) == canonical_json(expected.to_dict())


@pytest.mark.parametrize("bad", [{}, _evidence().to_dict(), object(), True, "credential-shaped-untrusted-value"])
def test_planned_action_rejects_untyped_evidence_without_echoing_it(bad) -> None:
    with pytest.raises(StateIdentityError, match="Kafka Streams evidence is invalid") as raised:
        _planned(kafka_streams_evidence=bad)
    assert "credential-shaped-untrusted-value" not in str(raised.value)


@pytest.mark.parametrize("action", ["create", "delete", "adopt", "none", "cancel"])
def test_evidence_only_authorizes_update_actions(action: str) -> None:
    with pytest.raises(StateIdentityError, match="exact replacement resource identity"):
        _planned(action=action)


@pytest.mark.parametrize("resource", [
    "streamt://payments/prod/topic/filtered",
    "streamt://payments/prod/flink_job/filtered",
    "streamt://payments/prod/kafka_streams_job/foreign",
    "streamt://foreign/prod/kafka_streams_job/filtered",
    "streamt://payments/foreign/kafka_streams_job/filtered",
])
def test_foreign_kind_name_project_or_environment_cannot_borrow_evidence(resource: str) -> None:
    with pytest.raises(StateIdentityError, match="exact replacement resource identity"):
        _planned(resource_id=resource)


@pytest.mark.parametrize(("field", "evidence"), [
    ("gateway_evidence", _gateway_action().gateway_evidence),
    ("connector_evidence", _connector_action().connector_evidence),
])
def test_mixed_provider_evidence_is_rejected_and_state_errors_are_translated(field, evidence) -> None:
    with pytest.raises(StateIdentityError, match="mutually exclusive") as raised:
        _planned(**{field: evidence})
    assert raised.value.__cause__ is None


def test_runner_update_without_evidence_remains_rejected() -> None:
    with pytest.raises(StateIdentityError, match="requires exact action evidence"):
        PlannedAction(RESOURCE, "kafka_streams_job:filtered", "update")


@pytest.mark.parametrize("change", [
    {"kafka_streams_evidence": {}},
    {"action": "create"},
    {"resource_id": "streamt://foreign/prod/kafka_streams_job/filtered"},
    {"gateway_evidence": _gateway_action().gateway_evidence},
])
def test_converter_revalidates_structural_protocol_inputs(change) -> None:
    values = {
        "resource_id": RESOURCE, "action": "update", "gateway_evidence": None,
        "connector_evidence": None, "kafka_streams_evidence": _evidence(),
    }
    values.update(change)
    with pytest.raises(StateFormatError):
        operation_actions_from_planned([SimpleNamespace(**values)])


def test_reviewed_v6_round_trip_keeps_raw_token_schema_and_complete_v4_binding(tmp_path: Path) -> None:
    evidence = _token_schema_evidence()
    converted = operation_actions_from_planned([_planned(evidence)])
    reviewed = _review(evidence, actions=converted)
    assert reviewed.format_version == 6
    assert reviewed.actions[0].kafka_streams_evidence is evidence
    expected_bytes = canonical_json(converted[0].to_dict(control_version=4))
    assert canonical_json(reviewed.to_dict()["actions"][0]) == expected_bytes
    path = tmp_path / "reviewed.json"
    reviewed.save(path)
    loaded = ReviewedPlanFile.load(path)
    assert loaded == reviewed
    assert canonical_json(loaded.actions[0].to_dict(control_version=4)) == expected_bytes
    assert loaded.actions[0].kafka_streams_evidence.volume.token == TOKEN
    assert loaded.actions[0].kafka_streams_evidence.desired_artifact.to_dict()["plan"]["schema"]["token"] == {
        "type": "STRING", "nullable": False,
    }


def test_typed_conversion_does_not_clear_reviewed_plan_replacement_blocker() -> None:
    evidence = _evidence()
    actions = operation_actions_from_planned([_planned(evidence)])
    plan = _plan(evidence, verified=False)
    assert plan.is_apply_blocked
    with pytest.raises(PlanFileError):
        _review(evidence, plan=plan, actions=actions)


@pytest.mark.parametrize("method", ["planned_actions", "operation_actions", "apply"])
def test_actual_planner_replacement_stays_blocked_without_provider_mutations(method: str) -> None:
    planner, kafka, runner, change = _planner(exists=True, owned=True)
    change.action = "update"
    change.blocker = "kafka_streams_replacement_not_verified"
    plan = planner.plan()
    assert plan.is_apply_blocked
    plan.safety_blockers.clear()  # Authoritative lifecycle preflight must still refuse.
    with pytest.raises(StateIdentityError, match="lifecycle"):
        getattr(planner, method)(plan)
    kafka.apply_topic.assert_not_called()
    runner.apply_job.assert_not_called()
