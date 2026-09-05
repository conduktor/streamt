"""Explicit Kafka identity/progress checks for the stateless runner.

No runner is allowed to choose a reset policy on its own. Only a new, empty
application group can receive initial positions; existing offsets are read-only.
"""

from __future__ import annotations

import base64
import re
import time
from concurrent.futures import Future
from dataclasses import dataclass
from typing import TypeGuard, TypeVar

from confluent_kafka import (
    ConsumerGroupState,
    ConsumerGroupTopicPartitions,
    ConsumerGroupType,
    KafkaError,
    KafkaException,
    TopicCollection,
    TopicPartition,
    Uuid,
)
from confluent_kafka.admin import (  # type: ignore[attr-defined]  # public API, incomplete export typing
    AdminClient,
    OffsetSpec,
)

_ResultT = TypeVar("_ResultT")


class KafkaStreamsProgressError(ValueError):
    """Progress or Kafka identity could not be established safely."""


def _identity_text(value: object, message: str) -> str:
    if (
        not isinstance(value, str) or not value or value != value.strip()
        or any(ord(character) < 32 or ord(character) == 127 for character in value)
    ):
        raise KafkaStreamsProgressError(message)
    return value


def _topic_identity(value: object) -> str:
    """Accept Kafka's canonical, nonzero 128-bit URL-safe UUID encoding only."""
    # confluent-kafka's native Uuid string uses standard Base64. Normalize
    # that known native representation, not arbitrary provider objects/text.
    identity = str(value).translate(str.maketrans("+/", "-_")) if isinstance(value, Uuid) else value
    message = "Kafka topic has no stable canonical identity"
    if not isinstance(identity, str) or re.fullmatch(r"[A-Za-z0-9_-]{22}", identity) is None:
        raise KafkaStreamsProgressError(message)
    decoded = base64.urlsafe_b64decode(identity + "==")
    if (
        decoded == bytes(16)
        or base64.urlsafe_b64encode(decoded).decode("ascii").rstrip("=") != identity
    ):
        raise KafkaStreamsProgressError(message)
    return identity


def _offset(value: object) -> TypeGuard[int]:
    return type(value) is int and 0 <= value <= 2**63 - 1


def _partition_failed(value: object) -> bool:
    # Successful native Admin acknowledgements may carry KafkaError(0), not
    # None. Do not accept arbitrary falsey objects as successful evidence.
    return value is not None and not (isinstance(value, KafkaError) and value.code() == 0)


@dataclass(frozen=True)
class PartitionProgress:
    partition: int
    low: int
    high: int
    committed: int | None


@dataclass(frozen=True)
class ApplicationProgress:
    cluster_id: str
    input_topic_id: str
    output_topic_id: str
    group_exists: bool
    active_members: int
    partitions: tuple[PartitionProgress, ...]

    def _validate_metadata(self) -> None:
        _identity_text(self.cluster_id, "Kafka cluster did not provide a stable identity")
        if _topic_identity(self.input_topic_id) == _topic_identity(self.output_topic_id):
            raise KafkaStreamsProgressError("Input and output Kafka topic identities must differ")
        if (
            type(self.group_exists) is not bool
            or type(self.active_members) is not int or self.active_members < 0
            or not isinstance(self.partitions, tuple) or not self.partitions
        ):
            raise KafkaStreamsProgressError("Invalid application progress metadata")
        identifiers: list[int] = []
        for item in self.partitions:
            if (
                not isinstance(item, PartitionProgress)
                or type(item.partition) is not int or item.partition < 0
                or not _offset(item.low) or not _offset(item.high) or item.high < item.low
                or (item.committed is not None and not _offset(item.committed))
            ):
                raise KafkaStreamsProgressError("Invalid application partition progress")
            identifiers.append(item.partition)
        if sorted(identifiers) != list(range(len(identifiers))):
            raise KafkaStreamsProgressError("Input partition metadata is incomplete or ambiguous")

    def require_resumable(self) -> None:
        self._validate_metadata()
        if not self.group_exists or not self.partitions:
            raise KafkaStreamsProgressError("Existing application progress is missing; offset reset is blocked")
        for partition in self.partitions:
            if (
                partition.committed is None
                or not partition.low <= partition.committed <= partition.high
            ):
                raise KafkaStreamsProgressError(
                    "Committed input position is missing or outside retention; automatic reset is blocked"
                )

    def initial_positions(self, policy: str) -> dict[int, int]:
        if policy not in {"earliest", "latest"}:
            raise KafkaStreamsProgressError("Fresh application requires an explicit initial position policy")
        self._validate_metadata()
        if self.group_exists or self.active_members or any(
            partition.committed is not None for partition in self.partitions
        ):
            raise KafkaStreamsProgressError("Existing application group cannot be initialized or adopted implicitly")
        return {
            partition.partition: partition.low if policy == "earliest" else partition.high
            for partition in self.partitions
        }


class KafkaStreamsProgress:
    """Use bounded Admin requests without joining or advancing a consumer group."""

    def __init__(self, config: dict[str, str], *, timeout: int = 30) -> None:
        try:
            self.admin = AdminClient(dict(config))
        except Exception:
            raise KafkaStreamsProgressError("Kafka progress client initialization failed") from None
        self.timeout = timeout

    def _result(self, future: Future[_ResultT], *, timeout: float | None = None) -> _ResultT:
        try:
            return future.result(timeout=self.timeout if timeout is None else timeout)
        except Exception:
            # Provider exceptions can echo client configuration. Preserve only
            # the boundary that failed, not arbitrary broker/client messages.
            raise KafkaStreamsProgressError("Kafka identity/progress observation failed") from None

    def cluster_id(self) -> str:
        try:
            cluster = self._result(self.admin.describe_cluster(request_timeout=self.timeout))
            return _identity_text(getattr(cluster, "cluster_id", None),
                                  "Kafka cluster did not provide a stable identity")
        except KafkaStreamsProgressError:
            raise
        except Exception:
            raise KafkaStreamsProgressError("Kafka cluster identity observation failed") from None

    def _named_results(self, futures: object, names: set[str]) -> dict[str, object]:
        if not isinstance(futures, dict) or set(futures) != names:
            raise KafkaStreamsProgressError("Kafka response identities are incomplete or unexpected")
        return {name: self._result(futures[name]) for name in sorted(names)}

    @staticmethod
    def _remaining(deadline: float) -> float:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise KafkaStreamsProgressError("Kafka application group readiness timed out")
        return remaining

    def _group_ids(self, *, deadline: float | None = None) -> set[str]:
        timeout = self.timeout if deadline is None else self._remaining(deadline)
        future = self.admin.list_consumer_groups(request_timeout=timeout)
        listing = self._result(future, timeout=self.timeout if deadline is None else self._remaining(deadline))
        if deadline is not None:
            self._remaining(deadline)
        valid, errors = getattr(listing, "valid", None), getattr(listing, "errors", None)
        if not isinstance(valid, list) or not isinstance(errors, list) or errors:
            raise KafkaStreamsProgressError("Kafka application group inventory is incomplete")
        identifiers = [_identity_text(getattr(group, "group_id", None),
                                      "Kafka application group inventory is invalid") for group in valid]
        if len(set(identifiers)) != len(identifiers):
            raise KafkaStreamsProgressError("Kafka application group identity is ambiguous")
        return set(identifiers)

    @staticmethod
    def _described_topic_id(topic: object, name: str) -> str:
        if getattr(topic, "name", None) != name:
            raise KafkaStreamsProgressError("Kafka topic response identity changed")
        return _topic_identity(getattr(topic, "topic_id", None))

    def topic_id(self, name: str) -> str:
        try:
            topic = self._named_results(self.admin.describe_topics(
                TopicCollection([name]), request_timeout=self.timeout,
            ), {name})[name]
            return self._described_topic_id(topic, name)
        except KafkaStreamsProgressError:
            raise
        except Exception:
            raise KafkaStreamsProgressError("Kafka topic identity observation failed") from None

    def require_fresh_group(self, application_id: str) -> None:
        """Attest absence on a ready coordinator without joining or writing offsets.

        A broker can accept metadata requests before its group coordinator is
        ready. Retry only exact transient coordinator errors under one deadline;
        an ambiguous timeout or any other failure never means group absence.
        """
        try:
            _identity_text(application_id, "Invalid Kafka application group identity")
            deadline = time.monotonic() + self.timeout
            if application_id in self._group_ids(deadline=deadline):
                raise KafkaStreamsProgressError("Existing application group cannot be claimed implicitly")
            self._require_absent_coordinated_group(application_id, deadline)
            self._require_no_group_offsets(application_id, deadline)
            if application_id in self._group_ids(deadline=deadline):
                raise KafkaStreamsProgressError("Existing application group cannot be claimed implicitly")
        except KafkaStreamsProgressError:
            raise
        except Exception:
            raise KafkaStreamsProgressError("Kafka application group observation failed") from None

    def _require_absent_coordinated_group(self, application_id: str, deadline: float) -> None:
        transient = {
            KafkaError.NOT_COORDINATOR,
            KafkaError.COORDINATOR_LOAD_IN_PROGRESS,
            KafkaError.COORDINATOR_NOT_AVAILABLE,
        }
        while True:
            try:
                futures = self.admin.describe_consumer_groups(
                    [application_id], request_timeout=self._remaining(deadline),
                )
                if not isinstance(futures, dict) or set(futures) != {application_id}:
                    raise KafkaStreamsProgressError("Kafka application group response identity is invalid")
                group = futures[application_id].result(timeout=self._remaining(deadline))
            except KafkaException as error:
                # Inspect only native numeric codes, never provider messages or
                # generic retriable flags (which also cover unsafe ambiguity).
                if len(error.args) != 1 or not isinstance(error.args[0], KafkaError):
                    raise KafkaStreamsProgressError("Kafka application group observation failed") from None
                code = error.args[0].code()
                remaining = self._remaining(deadline)
                if code == KafkaError.GROUP_ID_NOT_FOUND:
                    return
                if code not in transient:
                    raise KafkaStreamsProgressError("Kafka application group observation failed") from None
                time.sleep(min(0.2, remaining))
            else:
                self._remaining(deadline)
                # DescribeGroups before v6 represents an absent classic group
                # as DEAD (KIP-1043). EMPTY is not absence. This narrow native
                # shape is only one part of the surrounding absence checks.
                if (
                    getattr(group, "group_id", None) != application_id
                    or getattr(group, "state", None) is not ConsumerGroupState.DEAD
                    or getattr(group, "type", None) is not ConsumerGroupType.CLASSIC
                    or getattr(group, "is_simple_consumer_group", None) is not True
                    or type(getattr(group, "members", None)) is not list or group.members
                    or type(getattr(group, "partition_assignor", None)) is not str
                    or group.partition_assignor != ""
                ):
                    raise KafkaStreamsProgressError("Existing or ambiguous application group cannot be claimed implicitly")
                return

    def _require_no_group_offsets(self, application_id: str, deadline: float) -> None:
        try:
            futures = self.admin.list_consumer_group_offsets(
                [ConsumerGroupTopicPartitions(application_id)], require_stable=True,
                request_timeout=self._remaining(deadline),
            )
            if not isinstance(futures, dict) or set(futures) != {application_id}:
                raise KafkaStreamsProgressError("Kafka application offset response identity is invalid")
            offsets = futures[application_id].result(timeout=self._remaining(deadline))
        except KafkaException as error:
            self._remaining(deadline)
            if (len(error.args) == 1 and isinstance(error.args[0], KafkaError)
                    and error.args[0].code() == KafkaError.GROUP_ID_NOT_FOUND):
                return
            raise KafkaStreamsProgressError("Kafka application offset absence could not be established") from None
        self._remaining(deadline)
        if (
            getattr(offsets, "group_id", None) != application_id
            or type(getattr(offsets, "topic_partitions", None)) is not list
            or offsets.topic_partitions
        ):
            raise KafkaStreamsProgressError("Existing or ambiguous application offsets cannot be claimed implicitly")

    def observe(self, application_id: str, input_topic: str, output_topic: str) -> ApplicationProgress:
        try:
            return self._observe(application_id, input_topic, output_topic)
        except KafkaStreamsProgressError:
            raise
        except Exception:
            raise KafkaStreamsProgressError("Kafka identity/progress observation failed") from None

    def _observe(self, application_id: str, input_topic: str, output_topic: str) -> ApplicationProgress:
        _identity_text(application_id, "Invalid Kafka application group identity")
        if input_topic == output_topic:
            raise KafkaStreamsProgressError("Input and output Kafka topic identities must differ")
        cluster_id = self.cluster_id()
        futures = self.admin.describe_topics(
            TopicCollection([input_topic, output_topic]), request_timeout=self.timeout,
        )
        topics = self._named_results(futures, {input_topic, output_topic})
        identifiers = {name: self._described_topic_id(topic, name) for name, topic in topics.items()}
        if identifiers[input_topic] == identifiers[output_topic]:
            raise KafkaStreamsProgressError("Input and output Kafka topic identities must differ")
        topic_partitions = getattr(topics[input_topic], "partitions", None)
        if not isinstance(topic_partitions, list) or not topic_partitions:
            raise KafkaStreamsProgressError("Invalid input topic partition metadata")
        raw_ids: list[int] = []
        for partition_metadata in topic_partitions:
            partition_id = getattr(partition_metadata, "id", None)
            if type(partition_id) is not int or partition_id < 0:
                raise KafkaStreamsProgressError("Invalid input topic partition metadata")
            raw_ids.append(partition_id)
        partition_ids = tuple(sorted(raw_ids))
        if partition_ids != tuple(range(len(partition_ids))):
            raise KafkaStreamsProgressError("Invalid input topic partition metadata")

        group_exists = application_id in self._group_ids()
        active_members = 0
        committed: dict[int, int] = {}
        if group_exists:
            group = self._named_results(self.admin.describe_consumer_groups(
                [application_id], request_timeout=self.timeout,
            ), {application_id})[application_id]
            members = getattr(group, "members", None)
            if getattr(group, "group_id", None) != application_id or not isinstance(members, list):
                raise KafkaStreamsProgressError("Kafka application group description is invalid")
            active_members = len(members)
            offsets = self._named_results(self.admin.list_consumer_group_offsets(
                [ConsumerGroupTopicPartitions(application_id)], require_stable=True,
                request_timeout=self.timeout,
            ), {application_id})[application_id]
            records = self._offset_records(offsets, application_id, input_topic, set(partition_ids))
            for partition_id, record in records.items():
                offset = getattr(record, "offset", None)
                if type(offset) is not int or offset > 2**63 - 1:
                    raise KafkaStreamsProgressError("Kafka application offsets have invalid types")
                if offset >= 0:
                    committed[partition_id] = offset

        bounds: dict[str, dict[int, int]] = {}
        offset_specs = (("low", OffsetSpec.earliest()), ("high", OffsetSpec.latest()))  # type: ignore[no-untyped-call]
        for label, spec in offset_specs:
            offset_futures = self.admin.list_offsets(
                {TopicPartition(input_topic, partition): spec for partition in partition_ids},
                request_timeout=self.timeout,
            )
            if not isinstance(offset_futures, dict) or len(offset_futures) != len(partition_ids):
                raise KafkaStreamsProgressError("Kafka input offset bounds are incomplete")
            values: dict[int, int] = {}
            for partition, future in offset_futures.items():
                if (
                    partition.topic != input_topic or type(partition.partition) is not int
                    or partition.partition not in partition_ids or partition.partition in values
                    or _partition_failed(partition.error)
                ):
                    raise KafkaStreamsProgressError("Kafka input offset identities are invalid")
                value = getattr(self._result(future), "offset", None)
                if not _offset(value):
                    raise KafkaStreamsProgressError("Kafka input offset bounds are invalid")
                values[partition.partition] = value
            bounds[label] = values
        partitions = tuple(
            PartitionProgress(partition, bounds["low"][partition], bounds["high"][partition], committed.get(partition))
            for partition in partition_ids
        )
        if any(item.low < 0 or item.high < item.low for item in partitions):
            raise KafkaStreamsProgressError("Kafka input offset bounds are invalid")
        result = ApplicationProgress(
            cluster_id, identifiers[input_topic], identifiers[output_topic],
            group_exists, active_members, partitions,
        )
        result._validate_metadata()
        return result

    @staticmethod
    def _offset_records(
        response: object, application_id: str, topic: str, partition_ids: set[int],
    ) -> dict[int, object]:
        records = getattr(response, "topic_partitions", None)
        if getattr(response, "group_id", None) != application_id or not isinstance(records, list):
            raise KafkaStreamsProgressError("Kafka application offset response is incomplete")
        result: dict[int, object] = {}
        for record in records:
            partition = getattr(record, "partition", None)
            if (
                _partition_failed(getattr(record, "error", True))
                or getattr(record, "topic", None) != topic
                or type(partition) is not int or partition not in partition_ids
                or partition in result
            ):
                raise KafkaStreamsProgressError("Kafka application offset identities are invalid")
            result[partition] = record
        return result

    def initialize(
        self, application_id: str, input_topic: str, output_topic: str,
        expected: ApplicationProgress, positions: dict[int, int],
    ) -> None:
        """Initialize only a fresh exact group, within an already journaled action.

        There is no Kafka compare-and-set for OffsetCommit. Streamt's operation
        lock and exclusive ownership of this application ID are prerequisites.
        An out-of-band group owner is never an authorized writer.
        """
        earliest = expected.initial_positions("earliest")
        latest = expected.initial_positions("latest")
        if (
            type(positions) is not dict
            or any(type(partition) is not int or not _offset(offset)
                   for partition, offset in positions.items())
            or positions not in (earliest, latest)
        ):
            raise KafkaStreamsProgressError("Initial positions do not match the fresh observed policy bounds")
        current = self.observe(application_id, input_topic, output_topic)
        current.initial_positions("earliest")
        if (
            (current.cluster_id, current.input_topic_id, current.output_topic_id)
            != (expected.cluster_id, expected.input_topic_id, expected.output_topic_id)
            or current.group_exists or current.active_members
            or set(positions) != {item.partition for item in current.partitions}
            or any(
                type(positions[item.partition]) is not int
                or not item.low <= positions[item.partition] <= item.high
                for item in current.partitions
            )
        ):
            raise KafkaStreamsProgressError("Fresh application identity or offset bounds changed before initialization")
        try:
            result = self._named_results(self.admin.alter_consumer_group_offsets(
                [ConsumerGroupTopicPartitions(application_id, [
                    TopicPartition(input_topic, partition, offset)
                    for partition, offset in sorted(positions.items())
                ])], request_timeout=self.timeout,
            ), {application_id})[application_id]
            acknowledged = self._offset_records(result, application_id, input_topic, set(positions))
            if set(acknowledged) != set(positions):
                raise KafkaStreamsProgressError("Initial application offsets were not fully acknowledged")
        except KafkaStreamsProgressError:
            raise
        except Exception:
            raise KafkaStreamsProgressError("Initial application offset write failed; inspect the durable operation") from None
        verified = self.observe(application_id, input_topic, output_topic)
        verified.require_resumable()
        if (
            verified.active_members
            or (verified.cluster_id, verified.input_topic_id, verified.output_topic_id)
            != (expected.cluster_id, expected.input_topic_id, expected.output_topic_id)
            or {item.partition: item.committed for item in verified.partitions} != positions
        ):
            raise KafkaStreamsProgressError("Initial application offsets could not be verified")
