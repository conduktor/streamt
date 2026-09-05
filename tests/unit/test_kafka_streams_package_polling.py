"""Bounded acceptance reads must not repeat writes or excuse bad evidence."""

from __future__ import annotations

import json
import runpy
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from streamt.deployer.kafka_streams_progress import (
    ApplicationProgress,
    KafkaStreamsProgressError,
    PartitionProgress,
)
from tests.unit.test_kafka_streams_operation_evidence import _evidence


def message(key, identifier, amount, offset):
    return SimpleNamespace(
        error=lambda: None, key=lambda: key,
        value=lambda: json.dumps({"id": identifier, "amount": amount}).encode(),
        offset=lambda: offset,
    )


@pytest.fixture
def probe(monkeypatch):
    path = Path(__file__).parents[1] / "package" / "kafka_streams_replacement_executor_probe.py"
    verify = runpy.run_path(str(path))["verify_updated_records"]
    elapsed = [0.0]
    outputs = [message(b"\x00\xff", "a", 120, 0), message(b"\x01\xfe", "after-high", 250, 1), message(None, "after-null-key", 300, 2)]
    consumer = Mock()

    def poll(delay):
        elapsed[0] += delay
        return outputs.pop(0) if outputs else None

    consumer.poll.side_effect = poll
    producer = Mock()
    producer.produce.side_effect = lambda *args, **kwargs: kwargs["on_delivery"](None, None)
    producer.flush.return_value = 0
    evidence = _evidence()
    ready = ApplicationProgress(
        evidence.progress.cluster_id, evidence.progress.input_topic_id, evidence.progress.output_topic_id,
        True, 1, (PartitionProgress(0, 0, 8, 8),),
    )
    observe = Mock(return_value=ready)
    runtime = SimpleNamespace(progress=SimpleNamespace(observe=observe))
    journey = SimpleNamespace(bootstrap="fixture:9092", token="fixture")
    monkeypatch.setitem(verify.__globals__, "Producer", Mock(return_value=producer))
    monkeypatch.setitem(verify.__globals__, "Consumer", Mock(return_value=consumer))
    monkeypatch.setitem(verify.__globals__, "time", SimpleNamespace(monotonic=lambda: elapsed[0]))

    def run():
        return verify(journey, runtime, evidence)

    yield SimpleNamespace(run=run, observe=observe, ready=ready, outputs=outputs, elapsed=elapsed, consumer=consumer, producer=producer)
    assert producer.produce.call_count == 3
    producer.flush.assert_called_once_with(15)
    consumer.close.assert_called_once_with()


def test_temporarily_unavailable_progress_retries_only_reads(probe):
    probe.observe.side_effect = [KafkaStreamsProgressError("unavailable"), KafkaStreamsProgressError("unavailable"), probe.ready]
    outputs, current = probe.run()
    assert len(outputs) == 3
    assert current is probe.ready
    assert probe.observe.call_count == 3


def test_permanently_unavailable_progress_times_out_without_false_success(probe):
    probe.observe.side_effect = KafkaStreamsProgressError("unavailable")
    with pytest.raises(AssertionError, match="did not reach"):
        probe.run()
    assert probe.elapsed[0] == 45


def test_old_successful_read_cannot_be_reused_after_new_reads_fail(probe):
    def observe(*args):
        if probe.observe.call_count == 1:
            return probe.ready
        raise KafkaStreamsProgressError("unavailable")

    probe.observe.side_effect = observe
    with pytest.raises(AssertionError, match="did not reach"):
        probe.run()
    assert probe.elapsed[0] == 45


@pytest.mark.parametrize("kind", ["missing_offset", "retention_loss", "wrong_uuid", "wrong_offset", "duplicate_output"])
def test_invalid_observed_evidence_and_output_are_not_excused(probe, kind):
    if kind == "missing_offset":
        probe.observe.return_value = replace(probe.ready, partitions=(PartitionProgress(0, 0, 8, None),))
    elif kind == "retention_loss":
        probe.observe.return_value = replace(probe.ready, partitions=(PartitionProgress(0, 9, 9, 8),))
    elif kind == "wrong_uuid":
        probe.observe.return_value = replace(probe.ready, output_topic_id="AAAAAAAAAAAAAAAAAAAAAw")
    elif kind == "wrong_offset":
        probe.observe.return_value = replace(probe.ready, partitions=(PartitionProgress(0, 0, 9, 9),))
    else:
        probe.outputs.append(message(b"\x00\xff", "a", 120, 3))
    with pytest.raises((AssertionError, KafkaStreamsProgressError)):
        probe.run()
    if kind in {"missing_offset", "retention_loss"}:
        assert probe.observe.call_count == 1


@pytest.mark.parametrize("surface", ["consumer", "observation"])
def test_unexpected_errors_are_not_swallowed(probe, surface):
    error = RuntimeError("fixture failure")
    if surface == "consumer":
        probe.consumer.poll.side_effect = error
    else:
        probe.observe.side_effect = error
    with pytest.raises(RuntimeError) as raised:
        probe.run()
    assert raised.value is error


def test_success_returning_after_deadline_is_not_accepted(probe):
    def observe(*args):
        probe.elapsed[0] += 50
        return probe.ready

    probe.observe.side_effect = observe
    with pytest.raises(AssertionError, match="deadline"):
        probe.run()
    assert probe.observe.call_count == 1


def test_continuous_unexpected_outputs_cannot_make_drain_infinite(probe):
    poll = probe.consumer.poll.side_effect

    def endless_poll(delay):
        return poll(delay) or message(None, "unexpected", 999, 3)

    probe.consumer.poll.side_effect = endless_poll
    with pytest.raises(AssertionError, match="deadline"):
        probe.run()
    assert 45 <= probe.elapsed[0] < 46
