"""Strict identity/offset observations and fresh-only initialization checks."""

from __future__ import annotations

from concurrent.futures import Future
from dataclasses import replace
from types import SimpleNamespace as Namespace
from unittest.mock import Mock

import pytest
from confluent_kafka import (
    ConsumerGroupState,
    ConsumerGroupTopicPartitions,
    ConsumerGroupType,
    KafkaError,
    KafkaException,
    TopicPartition,
    Uuid,
)

from streamt.deployer import kafka_streams_progress as progress

APP = "streamt-" + "a" * 32
INPUT = "orders.raw"
OUTPUT = "orders.clean"
INPUT_ID = str(Uuid(1, 2))
OUTPUT_ID = str(Uuid(3, 4))
SECRET = "client-private-password-never-print"


def _future(value: object = None, *, error: Exception | None = None) -> Future:
    result: Future = Future()
    if error is None:
        result.set_result(value)
    else:
        result.set_exception(error)
    return result


def _positions(*, exists: bool = False, committed: int | None = None) -> progress.ApplicationProgress:
    return progress.ApplicationProgress("cluster-test", INPUT_ID, OUTPUT_ID, exists, 0,
                                        (progress.PartitionProgress(0, 2, 8, committed),))


def _topic(name: str, identity: object, partitions: list[object] | None = None) -> Namespace:
    return Namespace(name=name, topic_id=identity,
              partitions=[Namespace(id=0)] if partitions is None else partitions)


@pytest.fixture
def admin(monkeypatch: pytest.MonkeyPatch) -> Mock:
    fake = Mock()
    fake.describe_cluster.return_value = _future(Namespace(cluster_id="cluster-test"))
    fake.describe_topics.return_value = {
        INPUT: _future(_topic(INPUT, Uuid(1, 2))),
        OUTPUT: _future(_topic(OUTPUT, Uuid(3, 4))),
    }
    fake.list_consumer_groups.return_value = _future(Namespace(valid=[], errors=[]))
    fake.list_consumer_group_offsets.return_value = {APP: _future(Namespace(group_id=APP, topic_partitions=[]))}
    fake.list_offsets.side_effect = [
        {TopicPartition(INPUT, 0): _future(Namespace(offset=2))},
        {TopicPartition(INPUT, 0): _future(Namespace(offset=8))},
    ]
    monkeypatch.setattr(progress, "AdminClient", lambda _config: fake)
    return fake


@pytest.fixture
def client(admin: Mock) -> progress.KafkaStreamsProgress:
    return progress.KafkaStreamsProgress({"bootstrap.servers": "unit-test.invalid"}, timeout=3)


def _existing(admin: Mock, *, offsets: list[object] | None = None) -> None:
    admin.list_consumer_groups.return_value = _future(Namespace(valid=[Namespace(group_id=APP)], errors=[]))
    admin.describe_consumer_groups.return_value = {APP: _future(Namespace(group_id=APP, members=[]))}
    admin.list_consumer_group_offsets.return_value = {APP: _future(Namespace(
        group_id=APP,
        topic_partitions=[TopicPartition(INPUT, 0, 5)] if offsets is None else offsets,
    ))}


def test_observe_fresh_metadata_does_not_join_or_modify_group(
    client: progress.KafkaStreamsProgress, admin: Mock,
) -> None:
    observed = client.observe(APP, INPUT, OUTPUT)
    assert observed == _positions()
    assert observed.initial_positions("earliest") == {0: 2}
    assert observed.initial_positions("latest") == {0: 8}
    admin.alter_consumer_group_offsets.assert_not_called()
    admin.list_consumer_group_offsets.assert_not_called()
    admin.describe_consumer_groups.assert_not_called()
    assert all(call.kwargs.get("request_timeout") == 3 for call in admin.mock_calls)


def test_existing_progress_is_observed_without_advancing_it(
    client: progress.KafkaStreamsProgress, admin: Mock,
) -> None:
    _existing(admin)
    observed = client.observe(APP, INPUT, OUTPUT)
    assert observed == _positions(exists=True, committed=5)
    observed.require_resumable()
    assert admin.list_consumer_group_offsets.call_args.kwargs["require_stable"] is True
    admin.alter_consumer_group_offsets.assert_not_called()


@pytest.mark.parametrize("committed", [None, -1, 1, 9, True, 2.5])
def test_missing_out_of_retention_or_wrongly_typed_positions_cannot_resume(committed: object) -> None:
    observed = _positions(exists=True, committed=committed)  # type: ignore[arg-type]
    with pytest.raises(progress.KafkaStreamsProgressError):
        observed.require_resumable()


@pytest.mark.parametrize("policy", ["", "reset", "none", "EARLIEST"])
def test_initial_offset_policy_must_be_explicit(policy: str) -> None:
    with pytest.raises(progress.KafkaStreamsProgressError):
        _positions().initial_positions(policy)


@pytest.mark.parametrize("observed", [
    _positions(exists=True), _positions(committed=2), replace(_positions(), active_members=1),
    replace(_positions(), partitions=()), replace(_positions(), group_exists=1),
    replace(_positions(), partitions=(progress.PartitionProgress(True, 2, 8, None),)),
    replace(_positions(), partitions=(progress.PartitionProgress(0, -1, 8, None),)),
    replace(_positions(), partitions=(progress.PartitionProgress(0, 8, 2, None),)),
    replace(_positions(), partitions=_positions().partitions * 2),
])
def test_only_well_formed_fresh_progress_can_be_initialized(observed: progress.ApplicationProgress) -> None:
    with pytest.raises(progress.KafkaStreamsProgressError):
        observed.initial_positions("earliest")


def test_constructor_failure_is_secret_neutral(monkeypatch: pytest.MonkeyPatch) -> None:
    def fail(_config: dict[str, str]) -> None:
        raise ValueError(SECRET)
    monkeypatch.setattr(progress, "AdminClient", fail)
    with pytest.raises(progress.KafkaStreamsProgressError) as caught:
        progress.KafkaStreamsProgress({"sasl.password": SECRET})
    assert SECRET not in str(caught.value)


@pytest.mark.parametrize("synchronous", [True, False])
def test_cluster_id_failures_are_secret_neutral(
    client: progress.KafkaStreamsProgress, admin: Mock, synchronous: bool,
) -> None:
    if synchronous:
        admin.describe_cluster.side_effect = ValueError(SECRET)
    else:
        admin.describe_cluster.return_value = _future(error=ValueError(SECRET))
    with pytest.raises(progress.KafkaStreamsProgressError) as caught:
        client.cluster_id()
    assert SECRET not in str(caught.value)


@pytest.mark.parametrize("identity", [None, "", " ", "cluster\nsecret", 123, False])
def test_cluster_identity_must_be_nonempty_valid_text(
    client: progress.KafkaStreamsProgress, admin: Mock, identity: object,
) -> None:
    admin.describe_cluster.return_value = _future(Namespace(cluster_id=identity))
    with pytest.raises(progress.KafkaStreamsProgressError):
        client.cluster_id()


@pytest.mark.parametrize("identity", [
    None, 123, False, {}, "", "bad-id", "AAAAAAAAAAAAAAAAAAAAAA",
    "AAAAAAAAAAAAAAAAAAAAAB", "x" * 23, "x" * 21, "00000000-0000-0000-0000-000000000001",
])
@pytest.mark.parametrize("method", ["observe", "topic_id"])
def test_topic_identity_requires_canonical_nonzero_kafka_uuid(
    client: progress.KafkaStreamsProgress, admin: Mock, identity: object, method: str,
) -> None:
    topics = {INPUT: _future(_topic(INPUT, identity))}
    if method == "observe":
        topics[OUTPUT] = _future(_topic(OUTPUT, Uuid(3, 4)))
    admin.describe_topics.return_value = topics
    with pytest.raises(progress.KafkaStreamsProgressError):
        client.observe(APP, INPUT, OUTPUT) if method == "observe" else client.topic_id(INPUT)
    admin.alter_consumer_group_offsets.assert_not_called()


def test_same_topic_uuid_cannot_be_both_input_and_output(
    client: progress.KafkaStreamsProgress, admin: Mock,
) -> None:
    admin.describe_topics.return_value[OUTPUT] = _future(_topic(OUTPUT, Uuid(1, 2)))
    with pytest.raises(progress.KafkaStreamsProgressError):
        client.observe(APP, INPUT, OUTPUT)


@pytest.mark.parametrize("identity", [Uuid(-1, -1), Uuid(42, -11), Uuid(-1, 123)])
def test_native_uuid_standard_base64_is_normalized_to_stable_kafka_encoding(
    client: progress.KafkaStreamsProgress, admin: Mock, identity: Uuid,
) -> None:
    admin.describe_topics.return_value = {INPUT: _future(_topic(INPUT, identity))}
    assert client.topic_id(INPUT) == str(identity).replace("+", "-").replace("/", "_")


@pytest.mark.parametrize("partitions", [[], [Namespace(id=0), Namespace(id=0)], [Namespace(id=True)],
                                       [Namespace(id="0")], [Namespace(id=-1)], [Namespace(id=0), Namespace(id=2)]])
def test_partition_metadata_must_be_complete_unique_and_integer(
    client: progress.KafkaStreamsProgress, admin: Mock, partitions: list[object],
) -> None:
    admin.describe_topics.return_value[INPUT] = _future(_topic(INPUT, Uuid(1, 2), partitions))
    with pytest.raises(progress.KafkaStreamsProgressError):
        client.observe(APP, INPUT, OUTPUT)


@pytest.mark.parametrize("listing", [
    Namespace(valid=[], errors=[ValueError(SECRET)]), Namespace(valid=None, errors=[]),
    Namespace(valid=[], errors=None), Namespace(valid=[Namespace(group_id=None)], errors=[]),
    Namespace(valid=[Namespace(group_id=APP), Namespace(group_id=APP)], errors=[]),
])
@pytest.mark.parametrize("method", ["observe", "fresh"])
def test_incomplete_or_ambiguous_group_inventory_is_never_absence(
    client: progress.KafkaStreamsProgress, admin: Mock, listing: Namespace, method: str,
) -> None:
    admin.list_consumer_groups.return_value = _future(listing)
    with pytest.raises(progress.KafkaStreamsProgressError) as caught:
        client.observe(APP, INPUT, OUTPUT) if method == "observe" else client.require_fresh_group(APP)
    assert SECRET not in str(caught.value)
    admin.alter_consumer_group_offsets.assert_not_called()


def test_existing_group_cannot_be_claimed_even_without_members(
    client: progress.KafkaStreamsProgress, admin: Mock,
) -> None:
    _existing(admin)
    with pytest.raises(progress.KafkaStreamsProgressError):
        client.require_fresh_group(APP)
    admin.describe_consumer_groups.assert_not_called()


@pytest.fixture
def clock(monkeypatch: pytest.MonkeyPatch) -> list[float]:
    current = [100.0]
    monkeypatch.setattr(progress.time, "monotonic", lambda: current[0])
    monkeypatch.setattr(progress.time, "sleep", lambda seconds: current.__setitem__(0, current[0] + seconds))
    return current


def _group_error(code: int) -> KafkaException:
    return KafkaException(KafkaError(code, SECRET))


def _dead_group(**changes: object) -> Namespace:
    fields = {"group_id": APP, "state": ConsumerGroupState.DEAD, "type": ConsumerGroupType.CLASSIC,
              "is_simple_consumer_group": True, "members": [], "partition_assignor": ""}
    return Namespace(**{**fields, **changes})


def test_legacy_dead_group_is_absent_only_with_exact_empty_stable_offsets_and_inventory(
    client: progress.KafkaStreamsProgress, admin: Mock, clock: list[float],
) -> None:
    admin.describe_consumer_groups.return_value = {APP: _future(_dead_group())}
    client.require_fresh_group(APP)
    assert admin.list_consumer_groups.call_count == 2
    request = admin.list_consumer_group_offsets.call_args
    assert request.kwargs == {"require_stable": True, "request_timeout": 3.0}
    assert len(request.args[0]) == 1
    assert request.args[0][0].group_id == APP
    assert request.args[0][0].topic_partitions is None
    admin.alter_consumer_group_offsets.assert_not_called()


@pytest.mark.parametrize("fields", [
    {"group_id": "foreign"}, {"state": ConsumerGroupState.EMPTY}, {"state": ConsumerGroupState.STABLE},
    {"state": ConsumerGroupState.UNKNOWN}, {"state": "DEAD"}, {"state": 5},
    {"type": ConsumerGroupType.CONSUMER}, {"type": ConsumerGroupType.UNKNOWN}, {"type": "CLASSIC"},
    {"is_simple_consumer_group": False}, {"is_simple_consumer_group": 1},
    {"members": [Namespace()]}, {"members": None}, {"members": ()},
    {"partition_assignor": "range"}, {"partition_assignor": None},
])
def test_nonempty_or_malformed_legacy_dead_group_is_not_absence(
    client: progress.KafkaStreamsProgress, admin: Mock, clock: list[float], fields: dict,
) -> None:
    admin.describe_consumer_groups.return_value = {APP: _future(_dead_group(**fields))}
    with pytest.raises(progress.KafkaStreamsProgressError):
        client.require_fresh_group(APP)
    admin.list_consumer_group_offsets.assert_not_called()
    admin.list_consumer_groups.assert_called_once()
    admin.alter_consumer_group_offsets.assert_not_called()


@pytest.mark.parametrize("response", [
    None, {}, {"foreign": _future()}, {APP: _future(), "foreign": _future()},
    {APP: _future(Namespace(group_id=APP, topic_partitions=None))},
    {APP: _future(Namespace(group_id="foreign", topic_partitions=[]))},
    {APP: _future(Namespace(group_id=APP, topic_partitions=()))},
    {APP: _future(Namespace(group_id=APP, topic_partitions=[TopicPartition(INPUT, 0, 0)]))},
    {APP: _future(Namespace(group_id=APP, topic_partitions=[TopicPartition("foreign", 0, -1)]))},
])
def test_dead_group_with_residual_or_ambiguous_offsets_is_never_claimed(
    client: progress.KafkaStreamsProgress, admin: Mock, clock: list[float], response: object,
) -> None:
    admin.describe_consumer_groups.return_value = {APP: _future(_dead_group())}
    admin.list_consumer_group_offsets.return_value = response
    with pytest.raises(progress.KafkaStreamsProgressError):
        client.require_fresh_group(APP)
    admin.list_consumer_group_offsets.assert_called_once()
    admin.list_consumer_groups.assert_called_once()
    admin.alter_consumer_group_offsets.assert_not_called()


@pytest.mark.parametrize("code", [KafkaError.GROUP_ID_NOT_FOUND, KafkaError.GROUP_AUTHORIZATION_FAILED,
                                  KafkaError.REQUEST_TIMED_OUT, KafkaError.NOT_COORDINATOR])
@pytest.mark.parametrize("synchronous", [True, False])
def test_offset_absence_accepts_only_not_found_and_never_retries_other_errors(
    client: progress.KafkaStreamsProgress, admin: Mock, clock: list[float], code: int, synchronous: bool,
) -> None:
    admin.describe_consumer_groups.return_value = {APP: _future(_dead_group())}
    if synchronous:
        admin.list_consumer_group_offsets.side_effect = _group_error(code)
    else:
        admin.list_consumer_group_offsets.return_value = {APP: _future(error=_group_error(code))}
    if code == KafkaError.GROUP_ID_NOT_FOUND:
        client.require_fresh_group(APP)
        assert admin.list_consumer_groups.call_count == 2
    else:
        with pytest.raises(progress.KafkaStreamsProgressError) as caught:
            client.require_fresh_group(APP)
        assert SECRET not in str(caught.value)
        admin.list_consumer_groups.assert_called_once()
    admin.list_consumer_group_offsets.assert_called_once()
    admin.describe_consumer_groups.assert_called_once()
    admin.alter_consumer_group_offsets.assert_not_called()


def test_fresh_group_requires_exact_coordinator_absence_and_rechecks_inventory(
    client: progress.KafkaStreamsProgress, admin: Mock, clock: list[float],
) -> None:
    admin.describe_consumer_groups.return_value = {APP: _future(error=_group_error(KafkaError.GROUP_ID_NOT_FOUND))}
    client.require_fresh_group(APP)
    assert [call[0] for call in admin.mock_calls] == [
        "list_consumer_groups", "describe_consumer_groups", "list_consumer_group_offsets", "list_consumer_groups",
    ]
    admin.describe_consumer_groups.assert_called_once_with([APP], request_timeout=3.0)
    admin.alter_consumer_group_offsets.assert_not_called()
    admin.list_consumer_group_offsets.assert_called_once()
    assert admin.list_consumer_group_offsets.call_args.kwargs["require_stable"] is True


@pytest.mark.parametrize("code", [KafkaError.NOT_COORDINATOR, KafkaError.COORDINATOR_LOAD_IN_PROGRESS,
                                  KafkaError.COORDINATOR_NOT_AVAILABLE])
@pytest.mark.parametrize("synchronous", [True, False])
def test_only_exact_transient_coordinator_errors_are_retried_read_only(
    client: progress.KafkaStreamsProgress, admin: Mock, clock: list[float], code: int, synchronous: bool,
) -> None:
    absent = {APP: _future(error=_group_error(KafkaError.GROUP_ID_NOT_FOUND))}
    failed = _group_error(code)
    admin.describe_consumer_groups.side_effect = [failed if synchronous else {APP: _future(error=failed)}, absent]
    client.require_fresh_group(APP)
    requests = admin.describe_consumer_groups.call_args_list
    assert len(requests) == 2
    assert all(call.args == ([APP],) for call in requests)
    assert requests[0].kwargs["request_timeout"] == 3.0
    assert requests[1].kwargs["request_timeout"] == pytest.approx(2.8)
    assert admin.list_consumer_groups.call_count == 2
    admin.alter_consumer_group_offsets.assert_not_called()


@pytest.mark.parametrize("failure", [
    _group_error(KafkaError.GROUP_AUTHORIZATION_FAILED),
    _group_error(KafkaError.CLUSTER_AUTHORIZATION_FAILED),
    _group_error(KafkaError.REQUEST_TIMED_OUT),
    _group_error(KafkaError._TIMED_OUT),
    _group_error(KafkaError._TRANSPORT),
    _group_error(KafkaError.UNKNOWN_MEMBER_ID),
    TimeoutError(SECRET), ValueError(SECRET), KafkaException(SECRET),
])
@pytest.mark.parametrize("synchronous", [True, False])
def test_uncertain_or_unauthorized_group_observation_never_retries_or_leaks(
    client: progress.KafkaStreamsProgress, admin: Mock, clock: list[float],
    failure: Exception, synchronous: bool,
) -> None:
    if synchronous:
        admin.describe_consumer_groups.side_effect = failure
    else:
        admin.describe_consumer_groups.return_value = {APP: _future(error=failure)}
    with pytest.raises(progress.KafkaStreamsProgressError) as caught:
        client.require_fresh_group(APP)
    assert SECRET not in str(caught.value)
    admin.describe_consumer_groups.assert_called_once()
    admin.list_consumer_groups.assert_called_once()
    admin.alter_consumer_group_offsets.assert_not_called()
    assert clock == [100.0]


@pytest.mark.parametrize("response", [
    None, {}, {"foreign": _future()}, {APP: _future(), "foreign": _future()},
    {APP: _future(Namespace(group_id=APP, members=[]))},
    {APP: _future(Namespace(group_id="foreign", members=[]))}, {APP: _future(None)},
])
def test_successful_or_wrongly_identified_description_is_not_fresh_group_evidence(
    client: progress.KafkaStreamsProgress, admin: Mock, clock: list[float], response: object,
) -> None:
    admin.describe_consumer_groups.return_value = response
    with pytest.raises(progress.KafkaStreamsProgressError):
        client.require_fresh_group(APP)
    admin.describe_consumer_groups.assert_called_once()
    admin.list_consumer_groups.assert_called_once()
    admin.alter_consumer_group_offsets.assert_not_called()


@pytest.mark.parametrize("listing", [
    Namespace(valid=[Namespace(group_id=APP)], errors=[]),
    Namespace(valid=[], errors=[ValueError(SECRET)]),
    Namespace(valid=None, errors=[]), Namespace(valid=[], errors=None),
    Namespace(valid=[Namespace(group_id="other"), Namespace(group_id="other")], errors=[]),
])
def test_group_appearing_during_probe_or_incomplete_recheck_blocks_fresh_claim(
    client: progress.KafkaStreamsProgress, admin: Mock, clock: list[float], listing: Namespace,
) -> None:
    admin.describe_consumer_groups.return_value = {APP: _future(error=_group_error(KafkaError.GROUP_ID_NOT_FOUND))}
    admin.list_consumer_groups.side_effect = [_future(Namespace(valid=[], errors=[])), _future(listing)]
    with pytest.raises(progress.KafkaStreamsProgressError) as caught:
        client.require_fresh_group(APP)
    assert SECRET not in str(caught.value)
    assert admin.list_consumer_groups.call_count == 2
    admin.alter_consumer_group_offsets.assert_not_called()


def test_coordinator_retries_share_one_deadline_and_stop_without_writes(
    client: progress.KafkaStreamsProgress, admin: Mock, clock: list[float],
) -> None:
    admin.describe_consumer_groups.return_value = {APP: _future(error=_group_error(KafkaError.NOT_COORDINATOR))}
    with pytest.raises(progress.KafkaStreamsProgressError, match="readiness timed out"):
        client.require_fresh_group(APP)
    assert clock[0] == pytest.approx(103.0)
    timeouts = [call.kwargs["request_timeout"] for call in admin.describe_consumer_groups.call_args_list]
    assert 1 < len(timeouts) <= 16
    assert all(0 < timeout <= 3 for timeout in timeouts)
    assert timeouts == sorted(timeouts, reverse=True)
    admin.list_consumer_groups.assert_called_once()
    admin.alter_consumer_group_offsets.assert_not_called()


@pytest.mark.parametrize("phase", ["inventory", "describe", "offsets", "recheck"])
def test_each_synchronous_call_and_future_use_remaining_global_budget(
    client: progress.KafkaStreamsProgress, admin: Mock, clock: list[float], phase: str,
) -> None:
    observations: list[tuple[str, float, float]] = []

    def request(label: str, request_timeout: float) -> Mock:
        clock[0] += 0.2
        future = Mock()

        def result(*, timeout: float) -> object:
            observations.append((label, request_timeout, timeout))
            clock[0] += 3 if label == phase else 0.2
            if label == "describe":
                raise _group_error(KafkaError.GROUP_ID_NOT_FOUND)
            if label == "offsets":
                return Namespace(group_id=APP, topic_partitions=[])
            return Namespace(valid=[], errors=[])

        future.result.side_effect = result
        return future

    inventories = iter(["inventory", "recheck"])
    admin.list_consumer_groups.side_effect = lambda *, request_timeout: request(next(inventories), request_timeout)
    admin.describe_consumer_groups.side_effect = lambda groups, *, request_timeout: {
        APP: request("describe", request_timeout),
    }
    admin.list_consumer_group_offsets.side_effect = lambda groups, *, request_timeout, require_stable: {
        APP: request("offsets", request_timeout),
    }
    with pytest.raises(progress.KafkaStreamsProgressError, match="readiness timed out"):
        client.require_fresh_group(APP)
    assert observations[-1][0] == phase
    assert all(timeout == pytest.approx(request_timeout - 0.2)
               for _, request_timeout, timeout in observations)
    assert len(observations) == ["inventory", "describe", "offsets", "recheck"].index(phase) + 1
    admin.alter_consumer_group_offsets.assert_not_called()


@pytest.mark.parametrize("synchronous", [True, False])
def test_group_inventory_failures_are_secret_neutral_without_coordinator_probe(
    client: progress.KafkaStreamsProgress, admin: Mock, clock: list[float], synchronous: bool,
) -> None:
    if synchronous:
        admin.list_consumer_groups.side_effect = ValueError(SECRET)
    else:
        admin.list_consumer_groups.return_value = _future(error=ValueError(SECRET))
    with pytest.raises(progress.KafkaStreamsProgressError) as caught:
        client.require_fresh_group(APP)
    assert SECRET not in str(caught.value)
    admin.describe_consumer_groups.assert_not_called()
    admin.alter_consumer_group_offsets.assert_not_called()


@pytest.mark.parametrize("offsets", [
    [TopicPartition(INPUT, 0, 3), TopicPartition(INPUT, 0, 4)],
    [TopicPartition("foreign", 0, 3)], [TopicPartition(INPUT, 1, 3)],
    [Namespace(topic=INPUT, partition=0, offset=True, error=None)],
    [Namespace(topic=INPUT, partition=0, offset=2.5, error=None)],
    [Namespace(topic=INPUT, partition=False, offset=3, error=None)],
    [Namespace(topic=INPUT, partition=0, offset=3, error=SECRET)],
])
def test_conflicting_or_malformed_group_offsets_are_not_silently_collapsed(
    client: progress.KafkaStreamsProgress, admin: Mock, offsets: list[object],
) -> None:
    _existing(admin, offsets=offsets)
    with pytest.raises(progress.KafkaStreamsProgressError) as caught:
        client.observe(APP, INPUT, OUTPUT)
    assert SECRET not in str(caught.value)


def test_missing_group_offsets_remain_unknown_and_block_resume(
    client: progress.KafkaStreamsProgress, admin: Mock,
) -> None:
    _existing(admin, offsets=[])
    result = client.observe(APP, INPUT, OUTPUT)
    assert result.partitions[0].committed is None
    with pytest.raises(progress.KafkaStreamsProgressError):
        result.require_resumable()


@pytest.mark.parametrize("bounds", [
    {}, {TopicPartition("foreign", 0): _future(Namespace(offset=2))},
    {TopicPartition(INPUT, 0): _future(Namespace(offset=2)), TopicPartition(INPUT, 1): _future(Namespace(offset=2))},
    {TopicPartition(INPUT, 0): _future(Namespace(offset=True))},
    {TopicPartition(INPUT, 0): _future(Namespace(offset=2.5))},
    {TopicPartition(INPUT, 0): _future(Namespace(offset=-1))},
])
def test_bounds_must_acknowledge_exact_requested_partitions_with_integer_offsets(
    client: progress.KafkaStreamsProgress, admin: Mock, bounds: dict,
) -> None:
    admin.list_offsets.side_effect = [bounds, {TopicPartition(INPUT, 0): _future(Namespace(offset=8))}]
    with pytest.raises(progress.KafkaStreamsProgressError):
        client.observe(APP, INPUT, OUTPUT)


def _initialization(client: progress.KafkaStreamsProgress, admin: Mock, monkeypatch: pytest.MonkeyPatch) -> Mock:
    observed = Mock(side_effect=[_positions(), _positions(exists=True, committed=2)])
    monkeypatch.setattr(client, "observe", observed)
    admin.alter_consumer_group_offsets.return_value = {
        APP: _future(ConsumerGroupTopicPartitions(APP, [TopicPartition(INPUT, 0, 2)])),
    }
    return observed


def test_initialization_reobserves_before_and_after_single_exact_write(
    client: progress.KafkaStreamsProgress, admin: Mock, monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed = _initialization(client, admin, monkeypatch)
    client.initialize(APP, INPUT, OUTPUT, _positions(), {0: 2})
    assert observed.call_count == 2
    admin.alter_consumer_group_offsets.assert_called_once()
    requested = admin.alter_consumer_group_offsets.call_args.args[0]
    assert len(requested) == 1
    assert requested[0].group_id == APP
    assert [(item.topic, item.partition, item.offset) for item in requested[0].topic_partitions] == [(INPUT, 0, 2)]


def test_successful_native_kafka_error_zero_acknowledgement_is_accepted(
    client: progress.KafkaStreamsProgress, admin: Mock, monkeypatch: pytest.MonkeyPatch,
) -> None:
    _initialization(client, admin, monkeypatch)
    admin.alter_consumer_group_offsets.return_value = {APP: _future(Namespace(
        group_id=APP, topic_partitions=[Namespace(topic=INPUT, partition=0, error=KafkaError(0))],
    ))}
    client.initialize(APP, INPUT, OUTPUT, _positions(), {0: 2})


@pytest.mark.parametrize("code", [KafkaError.NOT_COORDINATOR, KafkaError.COORDINATOR_LOAD_IN_PROGRESS,
                                  KafkaError.COORDINATOR_NOT_AVAILABLE, KafkaError.REQUEST_TIMED_OUT])
@pytest.mark.parametrize("synchronous", [True, False])
def test_offset_initialization_never_retries_even_coordinator_errors(
    client: progress.KafkaStreamsProgress, admin: Mock, monkeypatch: pytest.MonkeyPatch,
    code: int, synchronous: bool,
) -> None:
    observed = _initialization(client, admin, monkeypatch)
    if synchronous:
        admin.alter_consumer_group_offsets.side_effect = _group_error(code)
    else:
        admin.alter_consumer_group_offsets.return_value = {APP: _future(error=_group_error(code))}
    with pytest.raises(progress.KafkaStreamsProgressError) as caught:
        client.initialize(APP, INPUT, OUTPUT, _positions(), {0: 2})
    assert SECRET not in str(caught.value)
    admin.alter_consumer_group_offsets.assert_called_once()
    assert observed.call_count == 1
    admin.describe_consumer_groups.assert_not_called()


@pytest.mark.parametrize("expected", [_positions(exists=True), _positions(committed=2),
                                      replace(_positions(), active_members=1)])
def test_original_expected_progress_must_be_fresh_before_any_observation(
    client: progress.KafkaStreamsProgress, admin: Mock, monkeypatch: pytest.MonkeyPatch,
    expected: progress.ApplicationProgress,
) -> None:
    observed = _initialization(client, admin, monkeypatch)
    with pytest.raises(progress.KafkaStreamsProgressError):
        client.initialize(APP, INPUT, OUTPUT, expected, {0: 2})
    observed.assert_not_called()
    admin.alter_consumer_group_offsets.assert_not_called()


@pytest.mark.parametrize("positions", [{}, {False: 2}, {0: True}, {0: 3}, {0: 2, 1: 2}, {0: -1}])
def test_initialization_accepts_only_exact_original_policy_positions_before_io(
    client: progress.KafkaStreamsProgress, admin: Mock, monkeypatch: pytest.MonkeyPatch, positions: dict,
) -> None:
    observed = _initialization(client, admin, monkeypatch)
    with pytest.raises(progress.KafkaStreamsProgressError):
        client.initialize(APP, INPUT, OUTPUT, _positions(), positions)
    observed.assert_not_called()
    admin.alter_consumer_group_offsets.assert_not_called()


@pytest.mark.parametrize("current", [
    replace(_positions(), cluster_id="another-cluster"),
    replace(_positions(), input_topic_id=OUTPUT_ID, output_topic_id=INPUT_ID),
    _positions(exists=True), replace(_positions(), active_members=1),
    replace(_positions(), partitions=(progress.PartitionProgress(0, 3, 8, None),)),
])
def test_current_identity_group_and_retention_are_revalidated_before_write(
    client: progress.KafkaStreamsProgress, admin: Mock, monkeypatch: pytest.MonkeyPatch,
    current: progress.ApplicationProgress,
) -> None:
    observed = _initialization(client, admin, monkeypatch)
    observed.side_effect = [current]
    with pytest.raises(progress.KafkaStreamsProgressError):
        client.initialize(APP, INPUT, OUTPUT, _positions(), {0: 2})
    admin.alter_consumer_group_offsets.assert_not_called()


@pytest.mark.parametrize("ack", [
    Namespace(group_id=APP, topic_partitions=[]), Namespace(group_id=APP, topic_partitions=None),
    Namespace(group_id="foreign", topic_partitions=[TopicPartition(INPUT, 0, 2)]),
    Namespace(group_id=APP, topic_partitions=[TopicPartition(INPUT, 0, 2), TopicPartition(INPUT, 0, 2)]),
    Namespace(group_id=APP, topic_partitions=[TopicPartition(OUTPUT, 0, 2)]),
    Namespace(group_id=APP, topic_partitions=[Namespace(topic=INPUT, partition=0, error=SECRET)]),
])
def test_initial_offset_acknowledgements_cannot_omit_or_change_requested_identities(
    client: progress.KafkaStreamsProgress, admin: Mock, monkeypatch: pytest.MonkeyPatch, ack: object,
) -> None:
    _initialization(client, admin, monkeypatch)
    admin.alter_consumer_group_offsets.return_value = {APP: _future(ack)}
    with pytest.raises(progress.KafkaStreamsProgressError) as caught:
        client.initialize(APP, INPUT, OUTPUT, _positions(), {0: 2})
    assert SECRET not in str(caught.value)


@pytest.mark.parametrize("verified", [
    _positions(exists=True, committed=3), replace(_positions(exists=True, committed=2), active_members=1),
    replace(_positions(exists=True, committed=2), group_exists=False),
    replace(_positions(exists=True, committed=2), output_topic_id=INPUT_ID),
])
def test_initial_offsets_must_be_read_back_on_same_inactive_group_and_topics(
    client: progress.KafkaStreamsProgress, admin: Mock, monkeypatch: pytest.MonkeyPatch,
    verified: progress.ApplicationProgress,
) -> None:
    observed = _initialization(client, admin, monkeypatch)
    observed.side_effect = [_positions(), verified]
    with pytest.raises(progress.KafkaStreamsProgressError):
        client.initialize(APP, INPUT, OUTPUT, _positions(), {0: 2})
