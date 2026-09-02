"""Consumer lag distinguishes absence from failed observation."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from streamt.deployer.kafka import (
    ConsumerGroupObservationError,
    KafkaDeployer,
)


def _deployer_with_topic() -> KafkaDeployer:
    with patch("streamt.deployer.kafka.AdminClient"):
        deployer = KafkaDeployer("broker:9092")

    partition_metadata = MagicMock()
    partition_metadata.error = None
    topic_metadata = MagicMock()
    topic_metadata.error = None
    topic_metadata.partitions = {0: partition_metadata}
    metadata = MagicMock()
    metadata.topics = {"orders": topic_metadata}
    deployer.admin.list_topics.return_value = metadata
    return deployer


def _offset_result(offset: int) -> MagicMock:
    partition = MagicMock()
    partition.topic = "orders"
    partition.partition = 0
    partition.offset = offset
    result = MagicMock()
    result.topic_partitions = [partition]
    future = MagicMock()
    future.result.return_value = result
    return future


def test_no_committed_offsets_returns_none_without_querying_watermarks() -> None:
    deployer = _deployer_with_topic()
    deployer.admin.list_consumer_group_offsets.return_value = {
        "analytics": _offset_result(-1001)
    }

    with patch("streamt.deployer.kafka.Consumer") as consumer:
        result = deployer.get_consumer_group_lag("analytics", "orders")

    assert result is None
    consumer.assert_not_called()


def test_committed_offsets_return_lag_and_close_consumer() -> None:
    deployer = _deployer_with_topic()
    deployer.admin.list_consumer_group_offsets.return_value = {
        "analytics": _offset_result(12)
    }
    consumer = MagicMock()
    consumer.get_watermark_offsets.return_value = (0, 20)

    with patch("streamt.deployer.kafka.Consumer", return_value=consumer):
        result = deployer.get_consumer_group_lag("analytics", "orders")

    assert result is not None
    assert result.total_lag == 8
    assert result.partitions[0].current_offset == 12
    consumer.close.assert_called_once_with()


def test_backend_failure_raises_typed_redacted_observation_error() -> None:
    deployer = _deployer_with_topic()
    future = MagicMock()
    future.result.side_effect = RuntimeError(
        "SASL authentication failed, password=supersecret, token=also-secret"
    )
    deployer.admin.list_consumer_group_offsets.return_value = {"analytics": future}

    with pytest.raises(ConsumerGroupObservationError) as raised:
        deployer.get_consumer_group_lag("analytics", "orders")

    error = raised.value
    assert error.operation == "committed-offset query"
    assert "<redacted>" in str(error)
    assert "supersecret" not in str(error)
    assert "also-secret" not in str(error)


def test_metadata_failure_is_not_misreported_as_no_offsets() -> None:
    deployer = _deployer_with_topic()
    deployer.admin.list_topics.side_effect = RuntimeError("broker unavailable")

    with pytest.raises(ConsumerGroupObservationError) as raised:
        deployer.get_consumer_group_lag("analytics", "orders")

    assert raised.value.operation == "topic metadata query"
    assert "broker unavailable" in str(raised.value)


def test_invalid_topic_metadata_is_not_misreported_as_no_offsets() -> None:
    deployer = _deployer_with_topic()
    deployer.admin.list_topics.return_value.topics["orders"].error = RuntimeError(
        "topic authorization failed"
    )

    with pytest.raises(ConsumerGroupObservationError) as raised:
        deployer.get_consumer_group_lag("analytics", "orders")

    assert raised.value.operation == "topic metadata query"
    assert "invalid topic metadata" in str(raised.value)


def test_missing_committed_offset_future_is_a_query_failure() -> None:
    deployer = _deployer_with_topic()
    deployer.admin.list_consumer_group_offsets.return_value = {}

    with pytest.raises(ConsumerGroupObservationError) as raised:
        deployer.get_consumer_group_lag("analytics", "orders")

    assert raised.value.operation == "committed-offset query"
    assert "no committed-offset future" in str(raised.value)
