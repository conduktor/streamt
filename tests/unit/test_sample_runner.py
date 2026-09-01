"""Tests for bounded, authenticated Kafka sample tests."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from streamt.core.models import DataTest, DataTestType, Source, StreamtProject
from streamt.testing.runner import TestRunner as StreamtTestRunner


def _project() -> StreamtProject:
    return StreamtProject.model_validate(
        {
            "project": {"name": "sample-test"},
            "runtime": {
                "kafka": {
                    "bootstrap_servers": "broker:9093",
                    "security_protocol": "SASL_SSL",
                    "sasl_mechanism": "PLAIN",
                    "sasl_username": "streamt",
                    "sasl_password": "secret",
                    "ssl_ca_location": "/certs/ca.pem",
                }
            },
        }
    )


def _message(value: bytes) -> MagicMock:
    message = MagicMock()
    message.error.return_value = None
    message.value.return_value = value
    return message


def _project_with_source() -> StreamtProject:
    project = _project()
    project.sources.append(Source(name="events", topic="events.v1"))
    return project


def test_sampling_uses_confluent_client_and_runtime_auth() -> None:
    consumer = MagicMock()
    consumer.poll.side_effect = [_message(b'{"id": 1}'), _message(b'{"id": 2}')]

    with patch("confluent_kafka.Consumer", return_value=consumer) as consumer_class:
        messages = StreamtTestRunner(_project())._sample_messages_from_kafka(
            "events.v1", sample_size=2, timeout_ms=500
        )

    assert messages == [{"id": 1}, {"id": 2}]
    config = consumer_class.call_args.args[0]
    assert config["bootstrap.servers"] == "broker:9093"
    assert config["security.protocol"] == "SASL_SSL"
    assert config["sasl.mechanism"] == "PLAIN"
    assert config["sasl.username"] == "streamt"
    assert config["sasl.password"] == "secret"
    assert config["ssl.ca.location"] == "/certs/ca.pem"
    assert config["group.id"].startswith("streamt-sample-")
    assert config["auto.offset.reset"] == "earliest"
    assert config["enable.auto.commit"] is False
    consumer.subscribe.assert_called_once_with(["events.v1"])
    consumer.close.assert_called_once_with()


def test_sampling_stops_at_wall_clock_deadline() -> None:
    consumer = MagicMock()
    consumer.poll.return_value = None

    with (
        patch("confluent_kafka.Consumer", return_value=consumer),
        patch("streamt.testing.runner.time.monotonic", side_effect=[10.0, 10.0, 10.2]),
    ):
        messages = StreamtTestRunner(_project())._sample_messages_from_kafka(
            "events.v1", sample_size=10, timeout_ms=100
        )

    assert messages == []
    assert consumer.poll.call_args.kwargs["timeout"] == pytest.approx(0.1)
    consumer.close.assert_called_once_with()


def test_sampling_skips_partition_eof_and_reads_next_message() -> None:
    from confluent_kafka import KafkaError

    eof_error = MagicMock()
    eof_error.code.return_value = KafkaError._PARTITION_EOF
    eof_message = MagicMock()
    eof_message.error.return_value = eof_error
    consumer = MagicMock()
    consumer.poll.side_effect = [eof_message, _message(b'{"id": 1}')]

    with patch("confluent_kafka.Consumer", return_value=consumer):
        messages = StreamtTestRunner(_project())._sample_messages_from_kafka(
            "events.v1", sample_size=1, timeout_ms=500
        )

    assert messages == [{"id": 1}]
    eof_message.error.assert_called_once_with()
    consumer.close.assert_called_once_with()


def test_sampling_raises_real_kafka_error_and_closes_consumer() -> None:
    kafka_error = MagicMock()
    kafka_error.code.return_value = -1234
    error_message = MagicMock()
    error_message.error.return_value = kafka_error
    consumer = MagicMock()
    consumer.poll.return_value = error_message

    with (
        pytest.raises(RuntimeError, match="Kafka consumer error"),
        patch("confluent_kafka.Consumer", return_value=consumer),
    ):
        StreamtTestRunner(_project())._sample_messages_from_kafka(
            "events.v1", sample_size=1, timeout_ms=100
        )

    error_message.error.assert_called_once_with()
    consumer.close.assert_called_once_with()


def test_sampling_closes_consumer_when_payload_is_not_an_object() -> None:
    consumer = MagicMock()
    consumer.poll.return_value = _message(b'[1, 2, 3]')

    with (
        pytest.raises(ValueError, match="Expected a JSON object"),
        patch("confluent_kafka.Consumer", return_value=consumer),
        patch("streamt.testing.runner.time.monotonic", side_effect=[10.0, 10.0]),
    ):
        StreamtTestRunner(_project())._sample_messages_from_kafka(
            "events.v1", sample_size=1, timeout_ms=100
        )

    consumer.close.assert_called_once_with()


def test_malformed_assertion_config_fails_before_sampling() -> None:
    test = DataTest.model_construct(
        name="bad-assertion",
        model="events",
        type=DataTestType.SAMPLE,
        assertions=[{"accepted_values": 42}],
    )
    runner = StreamtTestRunner(_project_with_source())

    with patch.object(runner, "_sample_messages_from_kafka") as sample:
        result = runner.run([test])[0]

    assert result["status"] == "failed"
    assert "assertions → 0 → accepted_values" in str(result["errors"])
    sample.assert_not_called()


def test_valid_sample_assertions_keep_existing_behavior() -> None:
    test = DataTest(
        name="valid-assertions",
        model="events",
        type=DataTestType.SAMPLE,
        assertions=[
            {"not_null": {"columns": ["id"]}},
            {"accepted_values": {"column": "status", "values": ["active"]}},
            {"range": {"column": "amount", "min": 0, "max": 100}},
            {"unique_key": {"key": "id"}},
        ],
    )
    runner = StreamtTestRunner(_project_with_source())

    with patch.object(
        runner,
        "_sample_messages_from_kafka",
        return_value=[
            {"id": 1, "status": "active", "amount": 10},
            {"id": 2, "status": "active", "amount": 20},
        ],
    ):
        result = runner.run([test])[0]

    assert result["status"] == "passed"
    assert result["sample_size"] == 2
