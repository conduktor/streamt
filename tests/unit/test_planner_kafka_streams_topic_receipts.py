"""New Kafka topics gain exact runner receipts before the next action."""

from __future__ import annotations

import copy
from unittest.mock import MagicMock

import pytest
import yaml
from click.testing import CliRunner

from streamt.cli import main
from streamt.compiler.manifest import TopicArtifact
from streamt.core.deployment_state import local_deployment_state_config
from streamt.deployer.kafka import TopicChange, TopicState
from streamt.deployer.state import StateIdentityError
from streamt.deployer.state_backend import make_deployment_state_service
from tests.unit.test_kafka_streams_compiler import _config
from tests.unit.test_planner_kafka_streams import _planner


def test_receipt_follows_acknowledged_creation_before_success_journal_or_next_topic() -> None:
    planner, kafka, runner, _change = _planner()
    plan = planner.plan()
    topic = plan.topic_changes[0].topic
    extra = TopicArtifact(name="unrelated", partitions=1, replication_factor=1)
    plan.topic_changes.append(TopicChange("unrelated", "create", TopicState("unrelated", False), extra))
    original = copy.deepcopy(plan.kafka_streams_changes[0].changes)
    events = []
    kafka.apply_topic.side_effect = lambda artifact: events.append(("acknowledged", artifact.name)) or "created"
    runner.record_created_topic.side_effect = lambda name: events.append(("receipt", name))
    runner.apply_job.side_effect = lambda _change: events.append(("runner",)) or "created"
    result = planner.apply(
        plan,
        before_action=lambda label, index: events.append(("started", label, index)),
        after_action=lambda label, index, success: events.append(("completed", label, index, success)),
    )
    assert result["errors"] == []
    assert events[:5] == [
        ("started", f"topic:{topic}", 0), ("acknowledged", topic), ("receipt", topic),
        ("completed", f"topic:{topic}", 0, True), ("started", "topic:unrelated", 1),
    ]
    assert events.index(("receipt", topic)) < events.index(("runner",))
    runner.record_created_topic.assert_called_once_with(topic)
    assert plan.kafka_streams_changes[0].changes == original


@pytest.mark.parametrize("failure", ["receipt", "provider", "unchanged", "updated", "invalid"])
def test_topic_receipt_or_provider_failure_stops_even_best_effort_direct_apply(failure: str) -> None:
    planner, kafka, runner, _change = _planner()
    plan = planner.plan()
    if failure == "receipt":
        runner.record_created_topic.side_effect = ValueError("topic identity unavailable")
    elif failure == "provider":
        kafka.apply_topic.side_effect = ValueError("broker creation failed")
    else:
        kafka.apply_topic.return_value = failure
    after = MagicMock()
    result = planner.apply(plan, after_action=after, stop_on_error=False)
    assert len(result["errors"]) == 1
    after.assert_called_once_with(f"topic:{plan.topic_changes[0].topic}", 0, False)
    runner.apply_job.assert_not_called()
    kafka.delete_topic.assert_not_called()
    if failure != "provider":
        assert result["rollback_candidates"] == []
    if failure != "receipt":
        runner.record_created_topic.assert_not_called()


def test_receipt_failure_prevents_next_provider_action_and_cannot_reach_runner() -> None:
    planner, kafka, runner, _change = _planner()
    plan = planner.plan()
    original_topic = plan.topic_changes[0].topic
    extra = TopicArtifact(name="unrelated", partitions=1, replication_factor=1)
    plan.topic_changes.append(TopicChange("unrelated", "create", TopicState("unrelated", False), extra))
    runner.record_created_topic.side_effect = ValueError("receipt verification failed")
    result = planner.apply(plan)
    assert result["errors"]
    assert [call.args[0].name for call in kafka.apply_topic.call_args_list] == [original_topic]
    assert result["rollback_candidates"] == []
    runner.apply_job.assert_not_called()


@pytest.mark.parametrize("malformation", ["missing", "duplicate", "update", "wrong_desired"])
def test_null_runner_binding_requires_one_reviewed_create_before_any_mutation(malformation: str) -> None:
    planner, kafka, runner, _change = _planner()
    plan = planner.plan()
    if malformation == "missing":
        plan.topic_changes.clear()
    elif malformation == "duplicate":
        plan.topic_changes.append(copy.deepcopy(plan.topic_changes[0]))
    elif malformation == "update":
        plan.topic_changes[0].action = "update"
    else:
        plan.topic_changes[0].desired.name = "other-topic"
    before = MagicMock()
    with pytest.raises(StateIdentityError, match="one exact reviewed topic creation"):
        planner.apply(plan, before_action=before)
    before.assert_not_called()
    kafka.apply_topic.assert_not_called()
    runner.record_created_topic.assert_not_called()
    runner.apply_job.assert_not_called()


def test_existing_managed_and_external_jobs_do_not_request_topic_creation_receipts() -> None:
    for mode, owned in (("managed", True), ("external", False)):
        planner, _kafka, runner, _change = _planner(mode=mode, exists=True, owned=owned)
        assert planner.apply(planner.plan())["errors"] == []
        runner.record_created_topic.assert_not_called()


@pytest.mark.parametrize("receipt_fails", [False, True])
def test_real_cli_path_records_receipt_before_success_and_retains_pending_on_failure(tmp_path, monkeypatch, receipt_fails) -> None:
    config = _config()
    (tmp_path / "stream_project.yml").write_text(yaml.safe_dump(config), encoding="utf-8")
    _planner_value, kafka, runner, _change = _planner()
    service = make_deployment_state_service(
        tmp_path, project="orders-project", environment="default", config=local_deployment_state_config(),
    )
    observed = []

    def receipt(topic):
        control = service.read_control().control
        assert control.status == "in_progress"
        assert control.progress[-1].resource_id.endswith("/topic/valuable_orders")
        assert control.progress[-1].status == "started"
        observed.append(topic)
        if receipt_fails:
            raise ValueError("topic identity unavailable")

    runner.record_created_topic.side_effect = receipt
    monkeypatch.setattr("streamt.cli.commands.apply.make_kafka_deployer", lambda *_args, **_kwargs: kafka)
    monkeypatch.setattr("streamt.cli.commands.apply.make_kafka_streams_deployer", lambda *_args, **_kwargs: runner)
    result = CliRunner().invoke(main, ["-o", "json", "apply", "-p", str(tmp_path)])
    assert observed == ["valuable_orders"], result.output
    control = service.read_control().control
    if receipt_fails:
        assert result.exit_code != 0, result.output
        assert control.status == "recovery_required"
        assert [(item.status, item.succeeded) for item in control.progress] == [("started", None), ("completed", False)]
        runner.apply_job.assert_not_called()
        kafka.delete_topic.assert_not_called()
        assert not service.read().state.resources
    else:
        assert result.exit_code == 0, result.output
        assert control.status == "clear"
        runner.apply_job.assert_called_once()
