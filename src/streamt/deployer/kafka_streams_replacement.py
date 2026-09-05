"""Pure decisions for the journaled predicate-only replacement protocol.

Nothing here observes a provider, mutates a runtime, advances a journal, or
clears an operation. The caller must hold operation authority, collect exact
read-only observations, durably record requested boundaries, then re-observe.
In particular, choosing ``resume`` is not itself a state-backend authorization.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from typing import Literal

from streamt.deployer.kafka_streams_evidence import (
    KAFKA_STREAMS_CLEAN_EXIT_CODES,
    KafkaStreamsActionEvidence,
    KafkaStreamsCheckpointEvidence,
    KafkaStreamsProgressEvidence,
    KafkaStreamsVolumeEvidence,
)
from streamt.deployer.state import ManagedResourceRecord, StateFormatError
from streamt.deployer.state_backend import OperationControlState

_ID = re.compile(r"[0-9a-f]{64}")
_CHECKSUM = re.compile(r"sha256:[0-9a-f]{64}")
_APP = re.compile(r"streamt-[0-9a-f]{32}")
_BACKEND = re.compile(r"kafka-streams-docker:v1:[0-9a-f]{64}")
_UUID = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")
ReplacementMode = Literal["execute", "resume", "recover"]
ReplacementStep = Literal[
    "record_started", "term_old", "wait_old_closed", "record_old_closed",
    "remove_old", "record_old_removed", "create_candidate", "record_replacement_created",
    "start_candidate", "wait_candidate_ready", "record_completed",
    "prior_verified", "candidate_verified", "blocked",
]


def _canonical(value: object, pattern: re.Pattern[str], field: str) -> None:
    if type(value) is not str or pattern.fullmatch(value) is None:
        raise StateFormatError(f"Replacement observation {field} is not canonical")


@dataclass(frozen=True)
class ReplacementGeneration:
    operation_id: str
    action_index: int
    evidence_fingerprint: str

    def __post_init__(self) -> None:
        _canonical(self.operation_id, _UUID, "operation_id")
        if self.operation_id == "00000000-0000-0000-0000-000000000000":
            raise StateFormatError("Replacement generation requires a nonzero operation UUID")
        if type(self.action_index) is not int or not 0 <= self.action_index <= 2**63 - 1:
            raise StateFormatError("Replacement generation action_index must be a non-negative int64")
        _canonical(self.evidence_fingerprint, _CHECKSUM, "evidence_fingerprint")

    def to_dict(self) -> dict[str, object]:
        return {
            "operation_id": self.operation_id, "action_index": self.action_index,
            "evidence_fingerprint": self.evidence_fingerprint,
        }


@dataclass(frozen=True)
class ReplacementContainerObservation:
    """An exact container, never a name-only or label-only observation.

    The adapter must verify fixed mounts/configuration, compute ``plan_hash``
    from actual mounted bytes, and compare label/status hashes with that hash.
    ``status_fresh`` proves the status belongs to this process start; an exited
    process also requires status time <= FinishedAt. ``forced_exit=False`` for
    an exited process requires complete non-OOM/non-dead/error-free evidence.
    Unknown reads are errors, not missing containers or clean exits.
    """

    container_id: str
    application_id: str
    backend_identity: str
    artifact_checksum: str
    plan_hash: str
    image_id: str
    network_id: str
    volume: KafkaStreamsVolumeEvidence
    process_state: Literal["created", "running", "exited"]
    exit_code: int | None
    forced_exit: bool | None
    runner_state: Literal["starting", "running", "closing", "closed", "failed"] | None
    status_fresh: bool
    generation: ReplacementGeneration | None = None

    def __post_init__(self) -> None:
        for value, pattern, field in (
            (self.container_id, _ID, "container_id"), (self.application_id, _APP, "application_id"),
            (self.backend_identity, _BACKEND, "backend_identity"),
            (self.artifact_checksum, _CHECKSUM, "artifact_checksum"),
            (self.plan_hash, _CHECKSUM, "plan_hash"), (self.image_id, _CHECKSUM, "image_id"),
            (self.network_id, _ID, "network_id"),
        ):
            _canonical(value, pattern, field)
        if type(self.volume) is not KafkaStreamsVolumeEvidence:
            raise StateFormatError("Replacement container requires an exact volume witness")
        if type(self.process_state) is not str or self.process_state not in {"created", "running", "exited"}:
            raise StateFormatError("Replacement container process state is unknown")
        if self.exit_code is not None and (type(self.exit_code) is not int or not 0 <= self.exit_code <= 255):
            raise StateFormatError("Replacement container exit code is invalid")
        if self.process_state != "exited" and self.exit_code is not None:
            raise StateFormatError("Replacement container cannot have an exit outcome before exit")
        if self.forced_exit is not None and type(self.forced_exit) is not bool:
            raise StateFormatError("Replacement forced-exit evidence must be boolean or unknown")
        if self.runner_state is not None and (
            type(self.runner_state) is not str
            or self.runner_state not in {"starting", "running", "closing", "closed", "failed"}
        ):
            raise StateFormatError("Replacement runner state is invalid")
        if type(self.status_fresh) is not bool or (self.status_fresh and self.runner_state is None):
            raise StateFormatError("Replacement status freshness requires a runner state")
        if self.process_state == "created" and (self.runner_state is not None or self.status_fresh):
            raise StateFormatError("An unstarted candidate cannot reuse status from its shared volume")
        if self.generation is not None and type(self.generation) is not ReplacementGeneration:
            raise StateFormatError("Replacement container generation is invalid")

    @property
    def ready(self) -> bool:
        return (
            self.process_state == "running" and self.runner_state == "running"
            and self.status_fresh and self.forced_exit is False
        )

    @property
    def cleanly_closed(self) -> bool:
        return (
            self.process_state == "exited" and self.runner_state == "closed"
            and self.status_fresh and self.exit_code in KAFKA_STREAMS_CLEAN_EXIT_CODES
            and self.forced_exit is False
        )

    @property
    def immutable_fingerprint(self) -> str:
        # Runtime state/timestamps and moving Kafka offsets are deliberately
        # not identity. Recovery must separately revalidate their predicates.
        value = {
            "container_id": self.container_id, "application_id": self.application_id,
            "backend_identity": self.backend_identity, "artifact_checksum": self.artifact_checksum,
            "plan_hash": self.plan_hash, "image_id": self.image_id, "network_id": self.network_id,
            "volume": self.volume.to_dict(),
            "generation": self.generation.to_dict() if self.generation is not None else None,
        }
        encoded = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        return "sha256:" + hashlib.sha256(encoded.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class ReplacementObservation:
    """One complete read-only snapshot for the action at the journal frontier.

    ``prior_container`` is an inspect by its durable old ID, not by name.
    ``candidate_container`` is the occupied application-name slot when its ID
    differs from the old ID. The adapter must exclude other matching/foreign
    containers and verify the application slot agrees with these observations.
    ``ownership_record`` comes from the locked protected state for this action.
    """

    backend_identity: str
    image_id: str
    network_id: str
    volume: KafkaStreamsVolumeEvidence
    progress: KafkaStreamsProgressEvidence
    prior_container: ReplacementContainerObservation | None
    candidate_container: ReplacementContainerObservation | None
    ownership_record: ManagedResourceRecord | None

    def __post_init__(self) -> None:
        _canonical(self.backend_identity, _BACKEND, "backend_identity")
        _canonical(self.image_id, _CHECKSUM, "image_id")
        _canonical(self.network_id, _ID, "network_id")
        if type(self.volume) is not KafkaStreamsVolumeEvidence or type(self.progress) is not KafkaStreamsProgressEvidence:
            raise StateFormatError("Replacement requires complete typed volume/progress evidence")
        if any(item is not None and type(item) is not ReplacementContainerObservation for item in (self.prior_container, self.candidate_container)):
            raise StateFormatError("Replacement container observations must be exact")
        if self.ownership_record is not None and type(self.ownership_record) is not ManagedResourceRecord:
            raise StateFormatError("Replacement ownership observation must be exact")


@dataclass(frozen=True)
class ReplacementDecision:
    step: ReplacementStep
    reason: str
    container_id: str | None = None
    checkpoint: KafkaStreamsCheckpointEvidence | None = None
    generation: ReplacementGeneration | None = None
    accepted_as: Literal["prior", "candidate"] | None = None
    immutable_fingerprint: str | None = None
    progress: KafkaStreamsProgressEvidence | None = None
    requires_resume_authority: bool = False

    @property
    def provider_mutation(self) -> bool:
        return self.step in {"term_old", "remove_old", "create_candidate", "start_candidate"}


def _container_matches(
    container: ReplacementContainerObservation, evidence: KafkaStreamsActionEvidence, *, candidate: bool,
) -> bool:
    artifact = evidence.desired_artifact if candidate else evidence.prior_artifact
    return (
        container.application_id == evidence.application_id
        and container.backend_identity == evidence.backend_identity
        and container.artifact_checksum == artifact.checksum and container.plan_hash == artifact.plan_hash
        and container.image_id == evidence.image_id and container.network_id == evidence.network_id
        and container.volume == evidence.volume
    )


def decide_replacement(
    control: OperationControlState, action_index: int, observed: ReplacementObservation,
    *, mode: ReplacementMode = "execute",
) -> ReplacementDecision:
    """Return one safe frontier step, or only a classification in recover mode.

    Progress is checked against immutable lower bounds, not exact dynamic
    offsets. This function never asserts the *whole* operation can be cleared:
    prior/candidate_verified classify only this resource for the coordinator.
    """
    if type(control) is not OperationControlState or type(observed) is not ReplacementObservation:
        raise StateFormatError("Replacement decisions require exact typed control and observation")
    if type(action_index) is not int or action_index < 0 or type(mode) is not str or mode not in {"execute", "resume", "recover"}:
        raise StateFormatError("Replacement decision action index or mode is invalid")
    resume_authority = mode == "resume" and control.status == "recovery_required"

    def blocked(reason: str) -> ReplacementDecision:
        return ReplacementDecision("blocked", reason, requires_resume_authority=resume_authority)

    intent = control.intent
    if control.control_version not in (4, 5) or intent is None or action_index >= len(intent.actions):
        return blocked("exact_v4_replacement_intent_required")
    if control.status not in {"in_progress", "recovery_required"}:
        return blocked("active_operation_required")
    if mode == "execute" and control.status != "in_progress":
        return blocked("explicit_same_operation_resume_required")
    action = intent.actions[action_index]
    evidence = action.kafka_streams_evidence
    if action.action != "update" or type(evidence) is not KafkaStreamsActionEvidence:
        return blocked("predicate_only_replacement_evidence_required")
    completed = {item.action_index for item in control.progress if item.status == "completed" and item.succeeded is True}
    if not set(range(action_index)) <= completed:
        return blocked("preceding_action_incomplete")
    if any(item.action_index > action_index for item in control.progress):
        return blocked("replacement_is_not_journal_frontier")
    boundaries = tuple(item for item in control.progress if item.action_index == action_index)
    terminal = next((item for item in boundaries if item.status == "completed"), None)
    if terminal is not None and terminal.succeeded is False:
        return blocked("terminal_failed_action_cannot_resume")
    checkpoints = tuple(item.kafka_streams_checkpoint for item in boundaries if item.kafka_streams_checkpoint is not None)
    stage = len(checkpoints)
    generation = ReplacementGeneration(intent.operation_id, action_index, evidence.immutable_fingerprint)
    if (
        observed.backend_identity != evidence.backend_identity or observed.image_id != evidence.image_id
        or observed.network_id != evidence.network_id or observed.volume != evidence.volume
    ):
        return blocked("backend_image_network_or_volume_changed")
    try:
        observed.progress.require_at_least(evidence.progress)
        if checkpoints:
            assert checkpoints[0].progress is not None
            observed.progress.require_at_least(checkpoints[0].progress)
    except StateFormatError:
        return blocked("progress_identity_retention_or_lower_bound_changed")
    owner = observed.ownership_record
    prior_ownership = evidence.prior_artifact.artifact.ownership
    assert prior_ownership is not None
    mode_value = prior_ownership["mode"] if isinstance(prior_ownership, dict) else prior_ownership.mode
    if owner is None or (owner.physical_name, owner.backend, owner.ownership) != (
        evidence.application_id, evidence.backend_identity, mode_value,
    ):
        return blocked("protected_ownership_identity_changed")
    if owner.artifact_checksum not in {evidence.prior_artifact.checksum, evidence.desired_artifact.checksum}:
        return blocked("protected_ownership_artifact_changed")
    if owner.artifact_checksum == evidence.desired_artifact.checksum and (terminal is None or terminal.succeeded is not True):
        return blocked("candidate_ownership_precedes_completed_action")
    old, candidate = observed.prior_container, observed.candidate_container
    if old is not None and (old.container_id != evidence.prior_container_id or not _container_matches(old, evidence, candidate=False)):
        return blocked("prior_container_identity_changed")
    if candidate is not None and (
        candidate.container_id == evidence.prior_container_id
        or not _container_matches(candidate, evidence, candidate=True) or candidate.generation != generation
    ):
        return blocked("candidate_container_or_generation_changed")
    if old is not None and candidate is not None:
        return blocked("old_and_candidate_both_present")
    if stage >= 2 and old is not None:
        return blocked("prior_container_reappeared_after_removal")
    if stage < 2 and candidate is not None:
        return blocked("candidate_created_before_durable_removal")
    if stage == 3 and candidate is not None and candidate.container_id != checkpoints[2].replacement_container_id:
        return blocked("journaled_candidate_container_changed")

    def decision(step: ReplacementStep, reason: str, *, container_id: str | None = None,
                 checkpoint: KafkaStreamsCheckpointEvidence | None = None,
                 accepted_as: Literal["prior", "candidate"] | None = None) -> ReplacementDecision:
        surface = candidate if accepted_as == "candidate" else old if accepted_as == "prior" else None
        return ReplacementDecision(
            step, reason, container_id=container_id, checkpoint=checkpoint, generation=generation,
            accepted_as=accepted_as, immutable_fingerprint=surface.immutable_fingerprint if surface else None,
            progress=observed.progress, requires_resume_authority=resume_authority,
        )

    candidate_ready = candidate is not None and candidate.ready and observed.progress.active_members == 1
    if mode == "recover":
        if not boundaries and old is not None and old.ready and observed.progress.active_members == 1:
            return decision("prior_verified", "unstarted_prior_runtime_verified", container_id=old.container_id, accepted_as="prior")
        if stage == 3 and candidate_ready:
            assert candidate is not None
            return decision("candidate_verified", "journaled_candidate_runtime_verified", container_id=candidate.container_id, accepted_as="candidate")
        return blocked("replacement_incomplete_or_signal_in_flight")
    if terminal is not None:
        if stage == 3 and candidate_ready:
            assert candidate is not None
            return decision("candidate_verified", "completed_candidate_runtime_verified", container_id=candidate.container_id, accepted_as="candidate")
        return blocked("completed_candidate_is_not_ready")
    if not boundaries:
        if old is None or not old.ready or candidate is not None or observed.progress.active_members != 1:
            return blocked("unstarted_prior_runtime_is_not_ready")
        return decision("record_started", "journal_before_first_runtime_mutation")
    if stage == 0:
        if old is None:
            return blocked("prior_absent_without_durable_clean_close")
        if old.ready and observed.progress.active_members == 1:
            return decision("term_old", "request_graceful_stop_of_exact_prior", container_id=old.container_id)
        if old.process_state == "running" and old.runner_state in {"closing", "closed"} and old.status_fresh and old.forced_exit is False:
            return decision("wait_old_closed", "graceful_close_or_process_exit_in_progress", container_id=old.container_id)
        if not old.cleanly_closed or observed.progress.active_members != 0:
            return blocked("prior_clean_close_or_inactive_group_not_proved")
        checkpoint = KafkaStreamsCheckpointEvidence(
            1, "old_closed", intent.operation_id, action_index, old.container_id, None,
            evidence.prior_artifact.plan_hash, old.exit_code, observed.progress,
        )
        return decision("record_old_closed", "persist_clean_close_before_removal", checkpoint=checkpoint)
    if stage == 1 and old is not None and (
        not old.cleanly_closed or old.exit_code != checkpoints[0].exit_code
    ):
        return blocked("prior_changed_after_durable_clean_close")
    # The original resume point must still be retained before a candidate ever
    # starts. A running/cleanly exited candidate may already have consumed it.
    candidate_has_started = candidate is not None and candidate.process_state != "created"
    if not candidate_has_started:
        barrier = checkpoints[0].progress
        assert barrier is not None
        if any(now.low > before.committed for now, before in zip(observed.progress.partitions, barrier.partitions, strict=True)):
            return blocked("clean_close_resume_point_lost_to_retention")
        # No owned process can commit in this interval: accepting even a
        # forward change would skip records before the replacement starts.
        # Producer high watermarks may still advance independently.
        if any(now.committed != before.committed for now, before in zip(observed.progress.partitions, barrier.partitions, strict=True)):
            return blocked("committed_offsets_changed_before_first_candidate_start")
        if observed.progress.active_members != 0:
            return blocked("inactive_group_required_before_runtime_transition")
    if stage == 1:
        if old is not None:
            return decision("remove_old", "remove_exact_cleanly_closed_prior_without_volume_deletion", container_id=old.container_id)
        checkpoint = KafkaStreamsCheckpointEvidence(1, "old_removed", intent.operation_id, action_index, evidence.prior_container_id, None, None, None, None)
        return decision("record_old_removed", "persist_verified_prior_absence", checkpoint=checkpoint)
    if stage == 2:
        if candidate is None:
            return decision("create_candidate", "create_exact_generation_with_existing_offsets")
        if candidate.process_state != "created" or candidate.forced_exit is not False:
            return blocked("candidate_started_before_creation_checkpoint")
        checkpoint = KafkaStreamsCheckpointEvidence(1, "replacement_created", intent.operation_id, action_index, evidence.prior_container_id, candidate.container_id, None, None, None)
        return decision("record_replacement_created", "persist_created_id_before_start", checkpoint=checkpoint)
    if candidate is None:
        return blocked("journaled_candidate_is_missing")
    if candidate.process_state == "created" and candidate.forced_exit is False:
        return decision("start_candidate", "start_exact_candidate_without_offset_initialization", container_id=candidate.container_id)
    if candidate_ready:
        return decision("record_completed", "persist_verified_candidate_readiness", container_id=candidate.container_id)
    if candidate.process_state == "running" and candidate.runner_state in {None, "starting"} and candidate.forced_exit is False:
        return decision("wait_candidate_ready", "candidate_has_not_proved_readiness", container_id=candidate.container_id)
    if mode == "resume" and candidate.cleanly_closed and observed.progress.active_members == 0:
        return decision("start_candidate", "resume_exact_cleanly_closed_candidate_without_offset_initialization", container_id=candidate.container_id)
    return blocked("candidate_clean_restart_or_readiness_not_proved")
