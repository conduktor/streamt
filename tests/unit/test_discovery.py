"""Focused tests for the read-only infrastructure discovery boundary."""

from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

from streamt.cli import main
from streamt.deployer.kafka import DEFAULT_TIMEOUT, KafkaDeployer, TopicState
from streamt.deployer.schema_registry import SchemaState
from streamt.discovery import discover_topics, select_topic_names


class RecordingKafkaReader:
    """A Kafka reader with deliberately no admin or mutation methods."""

    def __init__(self, topic_names: list[str]) -> None:
        self.topic_names = topic_names
        self.calls: list[tuple[str, str | None]] = []

    def list_topic_names(self) -> list[str]:
        self.calls.append(("list_topic_names", None))
        return self.topic_names

    def get_topic_state(self, topic_name: str) -> TopicState:
        self.calls.append(("get_topic_state", topic_name))
        return TopicState(
            name=topic_name,
            exists=True,
            partitions=len(topic_name),
            replication_factor=2,
        )


class RecordingSchemaReader:
    def __init__(self, states: dict[str, SchemaState | Exception]) -> None:
        self.states = states
        self.calls: list[str] = []

    def get_schema_state(
        self,
        subject: str,
        *,
        include_compatibility: bool = True,
    ) -> SchemaState:
        self.calls.append(subject)
        result = self.states[subject]
        if isinstance(result, Exception):
            raise result
        return result


def test_select_topic_names_is_filtered_and_deterministic() -> None:
    topics = ["z.events", "_schemas", "a.events", "__consumer_offsets", "a.audit"]

    assert select_topic_names(topics, include="*.events", exclude="z.*") == ["a.events"]


def test_discovery_uses_only_public_reads_and_sorts_resources() -> None:
    kafka = RecordingKafkaReader(["z.events", "_confluent-control", "a.events"])

    first = discover_topics(kafka)
    second = discover_topics(RecordingKafkaReader(list(reversed(kafka.topic_names))))

    assert [resource.topic for resource in first] == ["a.events", "z.events"]
    assert first == second
    assert kafka.calls == [
        ("list_topic_names", None),
        ("get_topic_state", "a.events"),
        ("get_topic_state", "z.events"),
    ]
    assert first[0].source == {
        "name": "a_events",
        "topic": "a.events",
        "description": "Discovered from Kafka (8 partitions)",
    }


def test_discovery_extracts_avro_and_json_columns_but_not_protobuf() -> None:
    kafka = RecordingKafkaReader(["proto", "json", "avro"])
    registry = RecordingSchemaReader(
        {
            "avro-value": SchemaState(
                subject="avro-value",
                exists=True,
                schema_type="AVRO",
                schema={"fields": [{"name": "id", "type": "string", "doc": "Key"}]},
            ),
            "json-value": SchemaState(
                subject="json-value",
                exists=True,
                schema_type="JSON",
                schema={
                    "required": ["count"],
                    "properties": {"count": {"type": "integer"}},
                },
            ),
            "proto-value": SchemaState(
                subject="proto-value",
                exists=True,
                schema_type="PROTOBUF",
                schema='syntax = "proto3";',
            ),
        }
    )

    resources = discover_topics(kafka, registry)
    by_topic = {resource.topic: resource.source for resource in resources}

    assert by_topic["avro"]["columns"] == [{"name": "id", "type": "STRING", "description": "Key"}]
    assert by_topic["json"]["columns"] == [{"name": "count", "type": "INT", "required": True}]
    assert "columns" not in by_topic["proto"]
    assert registry.calls == ["avro-value", "json-value", "proto-value"]


def test_schema_read_failure_does_not_drop_topic() -> None:
    kafka = RecordingKafkaReader(["orders"])
    registry = RecordingSchemaReader({"orders-value": RuntimeError("registry unavailable")})

    resources = discover_topics(kafka, registry)

    assert [resource.topic for resource in resources] == ["orders"]
    assert "columns" not in resources[0].source


def test_malformed_json_required_entries_do_not_break_discovery() -> None:
    kafka = RecordingKafkaReader(["orders"])
    registry = RecordingSchemaReader(
        {
            "orders-value": SchemaState(
                subject="orders-value",
                exists=True,
                schema_type="JSON",
                schema={
                    "required": ["id", {"invalid": True}],
                    "properties": {"id": {"type": "string"}},
                },
            )
        }
    )

    resources = discover_topics(kafka, registry)

    assert resources[0].source["columns"] == [{"name": "id", "type": "STRING", "required": True}]


def test_kafka_public_topic_listing_is_sorted_and_checks_lifecycle() -> None:
    deployer = KafkaDeployer.__new__(KafkaDeployer)
    deployer._closed = False
    deployer.admin = MagicMock()
    deployer.admin.list_topics.return_value.topics = {"z": object(), "a": object()}

    assert deployer.list_topic_names() == ["a", "z"]
    deployer.admin.list_topics.assert_called_once_with(timeout=DEFAULT_TIMEOUT)

    deployer._closed = True
    with pytest.raises(RuntimeError, match="closed"):
        deployer.list_topic_names()


def test_kafka_strict_metadata_observation_never_reads_topic_config() -> None:
    deployer = KafkaDeployer.__new__(KafkaDeployer)
    deployer._closed = False
    deployer.admin = MagicMock()
    deployer.admin.list_topics.return_value = SimpleNamespace(
        topics={
            "orders": SimpleNamespace(
                error=None,
                partitions={
                    0: SimpleNamespace(error=None, replicas=[1, 2]),
                    1: SimpleNamespace(error=None, replicas=[1, 2]),
                },
            )
        }
    )

    state = deployer.get_topic_metadata_state("orders")

    assert state.exists is True
    assert state.partitions == 2
    assert state.replication_factor == 2
    deployer.admin.describe_configs.assert_not_called()


def test_kafka_strict_metadata_observation_rejects_topic_and_partition_errors() -> None:
    deployer = KafkaDeployer.__new__(KafkaDeployer)
    deployer._closed = False
    deployer.admin = MagicMock()
    deployer.admin.list_topics.return_value = SimpleNamespace(
        topics={"orders": SimpleNamespace(error=RuntimeError("denied"), partitions={})}
    )
    with pytest.raises(RuntimeError, match="invalid metadata"):
        deployer.get_topic_metadata_state("orders")

    deployer.admin.list_topics.return_value = SimpleNamespace(
        topics={
            "orders": SimpleNamespace(
                error=None,
                partitions={0: SimpleNamespace(error=RuntimeError("denied"), replicas=[])},
            )
        }
    )
    with pytest.raises(RuntimeError, match="invalid partition metadata"):
        deployer.get_topic_metadata_state("orders")


def test_init_discover_uses_the_public_reader_boundary(tmp_path: Path) -> None:
    kafka = RecordingKafkaReader(["z.events", "_schemas", "a.events"])
    kafka.close = MagicMock()  # type: ignore[attr-defined]

    with patch("streamt.deployer.kafka.KafkaDeployer", return_value=kafka):
        result = CliRunner().invoke(
            main,
            [
                "--output",
                "json",
                "init",
                "--discover",
                "--dry-run",
                "--kafka",
                "broker:9092",
                "--project-dir",
                str(tmp_path),
            ],
        )

    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert [item["topic"] for item in payload["data"]["discovered_topics"]] == [
        "a.events",
        "z.events",
    ]
    assert kafka.calls == [
        ("list_topic_names", None),
        ("get_topic_state", "a.events"),
        ("get_topic_state", "z.events"),
    ]
    kafka.close.assert_called_once()
