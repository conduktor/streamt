"""Immutable, provider-free evidence for predicate-only runner replacement.

This is durable operation evidence, not a second lifecycle journal. Runtime
credentials and host input paths are deliberately absent from its wire format.
"""

from __future__ import annotations

import base64
import hashlib
import json
import re
import uuid
from dataclasses import dataclass, field
from typing import Literal, cast

from streamt.compiler.manifest import (
    KafkaStreamsJobArtifact,
    parse_compiled_kafka_streams_job_artifact,
)
from streamt.deployer.kafka_streams_time import parse_utc_timestamp
from streamt.deployer.state import StateFormatError, artifact_checksum

KAFKA_STREAMS_CONTROL_VERSION = 4
# These are admissible raw outcomes, not proof of a clean shutdown by themselves.
KAFKA_STREAMS_CLEAN_EXIT_CODES = frozenset({0, 143})
MAX_KAFKA_STREAMS_EVIDENCE_BYTES = 262144
MAX_KAFKA_STREAMS_ARTIFACT_BYTES = 65536
MAX_KAFKA_STREAMS_PARTITIONS = 1024
_ID = re.compile(r"[0-9a-f]{64}")
_BACKEND = re.compile(r"kafka-streams-docker:v1:[0-9a-f]{64}")
_APP = re.compile(r"streamt-[0-9a-f]{32}")
_CHECKSUM = re.compile(r"sha256:[0-9a-f]{64}")
_UTC = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,9})?Z")


def _object(value: object, keys: set[str], label: str) -> dict[str, object]:
    if type(value) is not dict or set(value) != keys:
        raise StateFormatError(f"Kafka Streams {label} has invalid fields")
    return cast(dict[str, object], value)


def _text(value: object, label: str, *, maximum: int = 256) -> str:
    if (
        type(value) is not str or not value or value != value.strip()
        or len(value) > maximum or any(ord(c) < 32 or ord(c) == 127 for c in value)
    ):
        raise StateFormatError(f"Kafka Streams {label} is invalid")
    return value


def _match(value: object, pattern: re.Pattern[str], label: str) -> str:
    if type(value) is not str or pattern.fullmatch(value) is None:
        raise StateFormatError(f"Kafka Streams {label} is not canonical")
    return value


def _uuid(value: object, label: str) -> str:
    try:
        if type(value) is not str or str(uuid.UUID(value)) != value or uuid.UUID(value).int == 0:
            raise ValueError
    except (ValueError, TypeError, AttributeError):
        raise StateFormatError(f"Kafka Streams {label} must be a nonzero canonical UUID") from None
    return value


def _topic_id(value: object) -> str:
    identity = _match(value, re.compile(r"[A-Za-z0-9_-]{22}"), "topic ID")
    decoded = base64.urlsafe_b64decode(identity + "==")
    if decoded == bytes(16) or base64.urlsafe_b64encode(decoded).decode().rstrip("=") != identity:
        raise StateFormatError("Kafka Streams topic ID is not canonical")
    return identity


def _integer(value: object, label: str) -> int:
    if type(value) is not int or not 0 <= value <= 2**63 - 1:
        raise StateFormatError(f"Kafka Streams {label} must be a non-negative int64")
    return value


def _json(value: object, *, maximum: int = MAX_KAFKA_STREAMS_EVIDENCE_BYTES) -> str:
    try:
        encoded = json.dumps(value, sort_keys=True, ensure_ascii=False, allow_nan=False, separators=(",", ":"))
        if len(encoded.encode("utf-8")) > maximum:
            raise ValueError
    except (TypeError, ValueError, UnicodeError, RecursionError):
        raise StateFormatError("Kafka Streams evidence is invalid or exceeds its size limit") from None
    return encoded


@dataclass(frozen=True)
class KafkaStreamsArtifactSnapshot:
    """Canonical bytes prevent mutable nested plans changing an existing intent."""

    canonical_json: str = field(repr=False)

    def __post_init__(self) -> None:
        try:
            if type(self.canonical_json) is not str or len(self.canonical_json.encode("utf-8")) > MAX_KAFKA_STREAMS_ARTIFACT_BYTES:
                raise ValueError
            artifact = parse_compiled_kafka_streams_job_artifact(json.loads(self.canonical_json))
            if _json(artifact.to_dict(), maximum=MAX_KAFKA_STREAMS_ARTIFACT_BYTES) != self.canonical_json:
                raise ValueError
        except (ValueError, TypeError, UnicodeError, RecursionError):
            raise StateFormatError("Kafka Streams artifact snapshot must be strict bounded canonical JSON") from None

    @classmethod
    def from_dict(cls, value: object) -> KafkaStreamsArtifactSnapshot:
        try:
            artifact = parse_compiled_kafka_streams_job_artifact(value)
        except (ValueError, TypeError):
            raise StateFormatError("Kafka Streams evidence requires an exact compiled artifact") from None
        return cls(_json(artifact.to_dict(), maximum=MAX_KAFKA_STREAMS_ARTIFACT_BYTES))

    @classmethod
    def from_artifact(cls, artifact: KafkaStreamsJobArtifact) -> KafkaStreamsArtifactSnapshot:
        if type(artifact) is not KafkaStreamsJobArtifact:
            raise StateFormatError("Kafka Streams evidence requires an exact compiled artifact")
        return cls.from_dict(artifact.to_dict())

    @property
    def artifact(self) -> KafkaStreamsJobArtifact:
        return parse_compiled_kafka_streams_job_artifact(json.loads(self.canonical_json))

    @property
    def checksum(self) -> str:
        return artifact_checksum(self.to_dict())

    @property
    def plan_hash(self) -> str:
        # Exact runner input bytes: ASCII JSON plus its terminating newline.
        payload = json.dumps(self.artifact.plan, sort_keys=True, ensure_ascii=True, allow_nan=False, separators=(",", ":")) + "\n"
        return "sha256:" + hashlib.sha256(payload.encode("ascii")).hexdigest()

    def to_dict(self) -> dict[str, object]:
        return cast(dict[str, object], json.loads(self.canonical_json))


@dataclass(frozen=True)
class KafkaStreamsPartitionEvidence:
    partition: int
    low: int
    high: int
    committed: int

    def __post_init__(self) -> None:
        for name in ("partition", "low", "high", "committed"):
            _integer(getattr(self, name), name)
        if not self.low <= self.committed <= self.high:
            raise StateFormatError("Kafka Streams committed offset is outside retention")

    def to_dict(self) -> dict[str, object]:
        return {name: getattr(self, name) for name in ("partition", "low", "high", "committed")}

    @classmethod
    def from_dict(cls, value: object) -> KafkaStreamsPartitionEvidence:
        data = _object(value, {"partition", "low", "high", "committed"}, "partition evidence")
        return cls(**cast(dict[str, int], data))


@dataclass(frozen=True)
class KafkaStreamsProgressEvidence:
    cluster_id: str
    input_topic_id: str
    output_topic_id: str
    group_exists: bool
    active_members: int
    partitions: tuple[KafkaStreamsPartitionEvidence, ...]

    def __post_init__(self) -> None:
        _text(self.cluster_id, "cluster ID")
        if _topic_id(self.input_topic_id) == _topic_id(self.output_topic_id):
            raise StateFormatError("Kafka Streams input/output topic IDs must differ")
        if self.group_exists is not True:
            raise StateFormatError("Kafka Streams replacement requires an existing group")
        _integer(self.active_members, "active members")
        if (
            type(self.partitions) is not tuple or not 0 < len(self.partitions) <= MAX_KAFKA_STREAMS_PARTITIONS
            or any(type(item) is not KafkaStreamsPartitionEvidence for item in self.partitions)
            or [item.partition for item in self.partitions] != list(range(len(self.partitions)))
        ):
            raise StateFormatError("Kafka Streams partition evidence must be complete, bounded and ordered")

    def require_at_least(self, previous: KafkaStreamsProgressEvidence, *, inactive: bool = False) -> None:
        if type(previous) is not KafkaStreamsProgressEvidence or (
            self.cluster_id, self.input_topic_id, self.output_topic_id, len(self.partitions)
        ) != (
            previous.cluster_id, previous.input_topic_id, previous.output_topic_id, len(previous.partitions)
        ):
            raise StateFormatError("Kafka Streams progress identity or partitions changed")
        if inactive and self.active_members != 0:
            raise StateFormatError("Kafka Streams closed checkpoint requires an inactive group")
        for before, after in zip(previous.partitions, self.partitions, strict=True):
            if after.committed < before.committed or after.low < before.low or after.high < before.high:
                raise StateFormatError("Kafka Streams progress regressed after its durable lower bound")

    def to_dict(self) -> dict[str, object]:
        return {
            "cluster_id": self.cluster_id, "input_topic_id": self.input_topic_id,
            "output_topic_id": self.output_topic_id, "group_exists": self.group_exists,
            "active_members": self.active_members, "partitions": [item.to_dict() for item in self.partitions],
        }

    @classmethod
    def from_dict(cls, value: object) -> KafkaStreamsProgressEvidence:
        data = _object(value, {"cluster_id", "input_topic_id", "output_topic_id", "group_exists", "active_members", "partitions"}, "progress evidence")
        partitions = data["partitions"]
        if type(partitions) is not list or len(partitions) > MAX_KAFKA_STREAMS_PARTITIONS:
            raise StateFormatError("Kafka Streams partition evidence must be a bounded array")
        return cls(
            cluster_id=cast(str, data["cluster_id"]), input_topic_id=cast(str, data["input_topic_id"]),
            output_topic_id=cast(str, data["output_topic_id"]), group_exists=cast(bool, data["group_exists"]),
            active_members=cast(int, data["active_members"]),
            partitions=tuple(KafkaStreamsPartitionEvidence.from_dict(item) for item in partitions),
        )


@dataclass(frozen=True)
class KafkaStreamsVolumeEvidence:
    name: str
    driver: str
    created_at: str
    application_id: str
    backend_identity: str
    token: str

    def __post_init__(self) -> None:
        _match(self.application_id, _APP, "volume application ID")
        _match(self.backend_identity, _BACKEND, "volume backend identity")
        _uuid(self.token, "volume token")
        if self.name != self.application_id + "-state" or self.driver != "local":
            raise StateFormatError("Kafka Streams volume requires an exact local owned identity")
        _match(self.created_at, _UTC, "volume created_at")
        try:
            parse_utc_timestamp(self.created_at)
        except ValueError:
            raise StateFormatError("Kafka Streams volume created_at is invalid") from None

    def to_dict(self) -> dict[str, object]:
        return {name: getattr(self, name) for name in ("name", "driver", "created_at", "application_id", "backend_identity", "token")}

    @classmethod
    def from_dict(cls, value: object) -> KafkaStreamsVolumeEvidence:
        return cls(**cast(dict[str, str], _object(value, {"name", "driver", "created_at", "application_id", "backend_identity", "token"}, "volume evidence")))


@dataclass(frozen=True)
class KafkaStreamsActionEvidence:
    version: int
    backend_identity: str
    prior_container_id: str
    prior_artifact: KafkaStreamsArtifactSnapshot
    desired_artifact: KafkaStreamsArtifactSnapshot
    image_id: str
    network_id: str
    volume: KafkaStreamsVolumeEvidence
    progress: KafkaStreamsProgressEvidence

    def __post_init__(self) -> None:
        if type(self.version) is not int or self.version != 1:
            raise StateFormatError("Unsupported Kafka Streams action evidence version")
        _match(self.backend_identity, _BACKEND, "backend identity")
        _match(self.prior_container_id, _ID, "prior container ID")
        _match(self.image_id, _CHECKSUM, "image ID")
        _match(self.network_id, _ID, "network ID")
        if (
            type(self.prior_artifact) is not KafkaStreamsArtifactSnapshot
            or type(self.desired_artifact) is not KafkaStreamsArtifactSnapshot
            or type(self.volume) is not KafkaStreamsVolumeEvidence
            or type(self.progress) is not KafkaStreamsProgressEvidence
        ):
            raise StateFormatError("Kafka Streams action requires exact typed evidence")
        before, after = self.prior_artifact.to_dict(), self.desired_artifact.to_dict()
        prior_plan, desired_plan = cast(dict[str, object], before["plan"]), cast(dict[str, object], after["plan"])
        prior_predicates, desired_predicates = prior_plan.pop("predicates"), desired_plan.pop("predicates")
        if before != after or prior_predicates == desired_predicates:
            raise StateFormatError("Kafka Streams replacement may change only predicates and must change them")
        ownership = cast(dict[str, object], before["ownership"])
        if ownership["mode"] not in {"managed", "adopted"}:
            raise StateFormatError("Kafka Streams replacement cannot mutate an external declaration")
        if self.volume.application_id != before["application_id"] or self.volume.backend_identity != self.backend_identity:
            raise StateFormatError("Kafka Streams volume evidence belongs to another application/backend")
        if cast(str, before["image"]).startswith("sha256:") and before["image"] != self.image_id:
            raise StateFormatError("Kafka Streams image ID disagrees with the pinned local image")
        _json(self.to_dict())

    @property
    def application_id(self) -> str:
        return self.prior_artifact.artifact.application_id

    @property
    def immutable_fingerprint(self) -> str:
        data = self.to_dict()
        # Offsets and watermarks are lower-bound evidence, not runtime identity.
        progress = self.progress.to_dict()
        data["progress"] = {
            **{name: progress[name] for name in ("cluster_id", "input_topic_id", "output_topic_id")},
            "partitions": [item.partition for item in self.progress.partitions],
        }
        return "sha256:" + hashlib.sha256(_json(data).encode("utf-8")).hexdigest()

    def to_dict(self) -> dict[str, object]:
        return {
            "version": self.version, "backend_identity": self.backend_identity,
            "prior_container_id": self.prior_container_id, "prior_artifact": self.prior_artifact.to_dict(),
            "desired_artifact": self.desired_artifact.to_dict(), "image_id": self.image_id,
            "network_id": self.network_id, "volume": self.volume.to_dict(), "progress": self.progress.to_dict(),
        }

    @classmethod
    def from_dict(cls, value: object) -> KafkaStreamsActionEvidence:
        data = _object(value, {"version", "backend_identity", "prior_container_id", "prior_artifact", "desired_artifact", "image_id", "network_id", "volume", "progress"}, "action evidence")
        _json(data)
        return cls(
            version=cast(int, data["version"]), backend_identity=cast(str, data["backend_identity"]),
            prior_container_id=cast(str, data["prior_container_id"]),
            prior_artifact=KafkaStreamsArtifactSnapshot.from_dict(data["prior_artifact"]),
            desired_artifact=KafkaStreamsArtifactSnapshot.from_dict(data["desired_artifact"]),
            image_id=cast(str, data["image_id"]), network_id=cast(str, data["network_id"]),
            volume=KafkaStreamsVolumeEvidence.from_dict(data["volume"]),
            progress=KafkaStreamsProgressEvidence.from_dict(data["progress"]),
        )


KafkaStreamsCheckpointPhase = Literal["old_closed", "old_removed", "replacement_created"]
KAFKA_STREAMS_CHECKPOINT_PHASES = ("old_closed", "old_removed", "replacement_created")


@dataclass(frozen=True)
class KafkaStreamsCheckpointEvidence:
    version: int
    phase: KafkaStreamsCheckpointPhase
    operation_id: str
    action_index: int
    prior_container_id: str
    replacement_container_id: str | None
    closed_plan_hash: str | None
    exit_code: int | None
    progress: KafkaStreamsProgressEvidence | None

    def __post_init__(self) -> None:
        if type(self.version) is not int or self.version != 1:
            raise StateFormatError("Unsupported Kafka Streams checkpoint evidence version")
        if self.phase not in KAFKA_STREAMS_CHECKPOINT_PHASES:
            raise StateFormatError("Kafka Streams checkpoint phase is invalid")
        _uuid(self.operation_id, "checkpoint operation ID")
        _integer(self.action_index, "checkpoint action index")
        _match(self.prior_container_id, _ID, "checkpoint prior container ID")
        if self.phase == "replacement_created":
            _match(self.replacement_container_id, _ID, "replacement container ID")
            if self.replacement_container_id == self.prior_container_id:
                raise StateFormatError("Kafka Streams replacement must have a new exact container ID")
        elif self.replacement_container_id is not None:
            raise StateFormatError("Kafka Streams replacement ID precedes its creation checkpoint")
        if self.phase == "old_closed":
            _match(self.closed_plan_hash, _CHECKSUM, "closed plan hash")
            if type(self.exit_code) is not int or self.exit_code not in KAFKA_STREAMS_CLEAN_EXIT_CODES:
                raise StateFormatError("Kafka Streams old container requires raw exit code 0 or 143")
            if type(self.progress) is not KafkaStreamsProgressEvidence or self.progress.active_members != 0:
                raise StateFormatError("Kafka Streams closed checkpoint requires inactive resumable progress")
        elif self.closed_plan_hash is not None or self.exit_code is not None or self.progress is not None:
            raise StateFormatError("Kafka Streams close evidence is allowed only at old_closed")

    def validate_action(self, action: KafkaStreamsActionEvidence) -> None:
        if self.prior_container_id != action.prior_container_id:
            raise StateFormatError("Kafka Streams checkpoint changed the prior container identity")
        if self.phase == "old_closed":
            if self.closed_plan_hash != action.prior_artifact.plan_hash:
                raise StateFormatError("Kafka Streams closed checkpoint has the wrong plan hash")
            assert self.progress is not None
            self.progress.require_at_least(action.progress, inactive=True)

    def to_dict(self) -> dict[str, object]:
        return {
            "version": self.version, "phase": self.phase, "operation_id": self.operation_id,
            "action_index": self.action_index, "prior_container_id": self.prior_container_id,
            "replacement_container_id": self.replacement_container_id,
            "closed_plan_hash": self.closed_plan_hash, "exit_code": self.exit_code,
            "progress": self.progress.to_dict() if self.progress is not None else None,
        }

    @classmethod
    def from_dict(cls, value: object) -> KafkaStreamsCheckpointEvidence:
        data = _object(value, {"version", "phase", "operation_id", "action_index", "prior_container_id", "replacement_container_id", "closed_plan_hash", "exit_code", "progress"}, "checkpoint evidence")
        return cls(
            version=cast(int, data["version"]), phase=cast(KafkaStreamsCheckpointPhase, data["phase"]),
            operation_id=cast(str, data["operation_id"]), action_index=cast(int, data["action_index"]),
            prior_container_id=cast(str, data["prior_container_id"]),
            replacement_container_id=cast(str | None, data["replacement_container_id"]),
            closed_plan_hash=cast(str | None, data["closed_plan_hash"]), exit_code=cast(int | None, data["exit_code"]),
            progress=None if data["progress"] is None else KafkaStreamsProgressEvidence.from_dict(data["progress"]),
        )
