"""Real CLI/state workflow keeps declaration-only Gateway resources unowned."""

from __future__ import annotations

import json
from contextlib import ExitStack
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import yaml
from click.testing import CliRunner

from streamt.cli import main
from streamt.compiler.manifest import TopicArtifact
from streamt.deployer.kafka import TopicChange, TopicState
from streamt.deployer.state import load_local_state, resource_id


def _write_project(path: Path, *, managed_topic: bool, collision: bool = False) -> None:
    models = [{
        "name": "external_view",
        "ownership": {"mode": "external"},
        "materialized": "virtual_topic",
        "sql": "SELECT id FROM {{ source('orders') }}",
        "gateway": {"virtual_topic": {"name": "orders.external_view"}},
    }]
    if managed_topic:
        models.append({"name": "managed_output", "materialized": "topic", "tags": ["chosen"]})
    if collision:
        models.append({
            "name": "managed_view",
            "materialized": "virtual_topic",
            "tags": ["chosen"],
            "sql": "SELECT id FROM {{ source('orders') }}",
            "gateway": {"virtual_topic": {"name": "orders.external_view"}},
        })
    (path / "stream_project.yml").write_text(yaml.safe_dump({
        "apiVersion": "streamt.dev/v1alpha1",
        "project": {"name": "gateway_scope"},
        "runtime": {
            "kafka": {"bootstrap_servers": "unreachable.invalid:9092"},
            "conduktor": {"gateway": {"admin_url": "https://unreachable.invalid/admin"}},
        },
        "sources": [{
            "name": "orders",
            "topic": "orders.raw",
            "columns": [{"name": "id", "type": "STRING"}],
        }],
        "models": models,
    }))


@pytest.mark.parametrize(("managed_topic", "selection"), [
    (False, []),
    (True, []),
    (True, ["--target", "managed_output"]),
    (True, ["--select", "tag:chosen"]),
])
def test_actual_apply_and_repeat_do_not_adopt_external_gateway(
    tmp_path: Path, managed_topic: bool, selection: list[str],
) -> None:
    _write_project(tmp_path, managed_topic=managed_topic)
    kafka = MagicMock()
    kafka.get_consumer_groups.return_value = []
    topic_created = False

    def plan_topic(artifact: TopicArtifact) -> TopicChange:
        return TopicChange(
            topic=artifact.name,
            action="none" if topic_created else "create",
            current=TopicState(
                name=artifact.name,
                exists=topic_created,
                partitions=artifact.partitions if topic_created else None,
                replication_factor=artifact.replication_factor if topic_created else None,
            ),
            desired=artifact,
        )

    def apply_topic(artifact: TopicArtifact) -> str:
        nonlocal topic_created
        assert artifact.name == "managed_output"
        topic_created = True
        return "created"

    kafka.plan_topic.side_effect = plan_topic
    kafka.apply_topic.side_effect = apply_topic
    with ExitStack() as stack:
        forbidden = [stack.enter_context(patch(
            f"streamt.cli.commands.apply.make_{provider}_deployer",
            side_effect=AssertionError(f"Unexpected {provider} provider construction"),
        )) for provider in ("gateway", "sr", "flink", "connect")]
        kafka_factory = stack.enter_context(patch(
            "streamt.cli.commands.apply.make_kafka_deployer",
            **({"return_value": kafka} if managed_topic else {
                "side_effect": AssertionError("External-only apply constructed Kafka"),
            }),
        ))
        for _ in range(2):
            result = CliRunner().invoke(
                main, ["-o", "json", "apply", "-p", str(tmp_path), *selection]
            )
            assert result.exit_code == 0, result.output
            payload = json.loads(result.stdout)
            assert payload["status"] == "ok"
            assert payload["data"]["committed"] is True
            state = load_local_state(tmp_path, project="gateway_scope", environment="default")
            assert state.serial == (1 if managed_topic else 0)
            assert set(state.resources) == (
                {resource_id("gateway_scope", "default", "topic", "managed_output")}
                if managed_topic else set()
            )
        assert all(factory.call_count == 0 for factory in forbidden)
        assert kafka_factory.call_count == (2 if managed_topic else 0)
    assert kafka.apply_topic.call_count == (1 if managed_topic else 0)


@pytest.mark.parametrize("selection", [["--target", "managed_view"], ["--select", "tag:chosen"]])
def test_selected_managed_gateway_cannot_hide_external_alias_collision(
    tmp_path: Path, selection: list[str],
) -> None:
    _write_project(tmp_path, managed_topic=False, collision=True)
    kafka = MagicMock()
    gateway = MagicMock()
    with (
        patch("streamt.cli.commands.apply.make_kafka_deployer", return_value=kafka),
        patch("streamt.cli.commands.apply.make_gateway_deployer", return_value=gateway),
    ):
        result = CliRunner().invoke(
            main, ["-o", "json", "apply", "-p", str(tmp_path), *selection]
        )
    assert result.exit_code == 1, result.output
    assert "duplicate canonical alias locator" in result.output
    gateway.observe_managed_gateway_snapshot.assert_not_called()
    gateway.apply_managed_gateway_rule.assert_not_called()
    kafka.plan_topic.assert_not_called()
    kafka.get_consumer_groups.assert_not_called()
    kafka.apply_topic.assert_not_called()
