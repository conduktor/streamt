"""Tests for bounded, authenticated Kafka sample tests."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from streamt.core.models import StreamtProject
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
