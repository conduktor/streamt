"""Provider-free replacement decisions at every durable/crash boundary."""

from __future__ import annotations

import socket
import subprocess
from dataclasses import FrozenInstanceError, replace
from typing import Any

import pytest

from streamt.deployer.kafka_streams_evidence import KafkaStreamsPartitionEvidence
from streamt.deployer.kafka_streams_replacement import (
    ReplacementContainerObservation,
    ReplacementGeneration,
    ReplacementObservation,
    decide_replacement,
)
from streamt.deployer.state import StateFormatError
from streamt.deployer.state_backend import (
    OperationAction,
    OperationControlState,
    OperationProgress,
    RecoveryRecord,
)
from tests.unit.test_kafka_streams_operation_evidence import (
    ADDRESS,
    APP,
    BACKEND,
    IMAGE,
    NETWORK,
    NEW_ID,
    OLD_ID,
    OPERATION,
    RESOURCE,
    STAMP,
    _boundaries,
    _boundary,
    _control,
    _desired_state,
    _evidence,
    _intent,
    _progress,
    _state,
)


def _generation(**changes: Any) -> ReplacementGeneration:
    return replace(ReplacementGeneration(OPERATION, 0, _evidence().immutable_fingerprint), **changes)


def _container(*, candidate: bool = False, state: str = "running", **changes: Any) -> ReplacementContainerObservation:
    evidence = _evidence()
    artifact = evidence.desired_artifact if candidate else evidence.prior_artifact
    runner_state = {"created": None, "running": "running", "exited": "closed"}[state]
    return replace(ReplacementContainerObservation(
        NEW_ID if candidate else OLD_ID, APP, BACKEND, artifact.checksum, artifact.plan_hash,
        IMAGE, NETWORK, evidence.volume, state, 0 if state == "exited" else None, False,
        runner_state, state != "created", _generation() if candidate else None,
    ), **changes)


def _observation(*, old: str | None = "running", candidate: str | None = None,
                 committed: int = 20, members: int | None = None, **changes: Any) -> ReplacementObservation:
    if members is None:
        members = int(old == "running" or candidate == "running")
    return replace(ReplacementObservation(
        BACKEND, IMAGE, NETWORK, _evidence().volume,
        _progress(committed=committed, active_members=members),
        _container(state=old) if old else None,
        _container(candidate=True, state=candidate) if candidate else None,
        _state().resources[RESOURCE],
    ), **changes)


def _recovering(prefix: int) -> OperationControlState:
    return replace(_control(_boundaries()[:prefix]), status="recovery_required", recovery=RecoveryRecord(
        OPERATION, "runtime_transition_uncertain", STAMP, 0 if prefix == 5 else None,
    ))


@pytest.mark.parametrize(("prefix", "old", "candidate", "expected"), [
    (0, "running", None, "record_started"),
    (1, "running", None, "term_old"),
    (1, "exited", None, "record_old_closed"),
    (2, "exited", None, "remove_old"),
    (2, None, None, "record_old_removed"),
    (3, None, None, "create_candidate"),
    (3, None, "created", "record_replacement_created"),
    (4, None, "created", "start_candidate"),
    (4, None, "running", "record_completed"),
    (5, None, "running", "candidate_verified"),
])
def test_each_fresh_execution_and_crash_frontier(prefix, old, candidate, expected) -> None:
    control, observed = _control(_boundaries()[:prefix]), _observation(old=old, candidate=candidate)
    before = control.to_dict()
    decision = decide_replacement(control, 0, observed)
    assert decision.step == expected
    assert decision.generation == _generation()
    assert decision.progress == observed.progress
    assert not decision.requires_resume_authority
    assert control.to_dict() == before  # Deciding never writes a boundary.
    assert decision.provider_mutation == (expected in {"term_old", "remove_old", "create_candidate", "start_candidate"})
    if decision.checkpoint is not None:
        checkpoint = OperationProgress(OPERATION, 0, RESOURCE, "update", "checkpoint", None, STAMP, decision.checkpoint)
        appended = replace(control, progress=(*control.progress, checkpoint))
        assert appended.progress[-1].kafka_streams_checkpoint == decision.checkpoint


@pytest.mark.parametrize(("prefix", "old", "candidate", "expected"), [
    (0, "running", None, "prior_verified"),
    (1, "running", None, "blocked"),  # TERM can be in flight even if still ready.
    (1, "exited", None, "blocked"),
    (2, "exited", None, "blocked"),
    (2, None, None, "blocked"),  # Never discard replacement intent in the gap.
    (3, None, None, "blocked"),
    (3, None, "created", "blocked"),
    (4, None, "created", "blocked"),
    (4, None, "running", "candidate_verified"),
    (5, None, "running", "candidate_verified"),
])
def test_read_only_recovery_only_classifies_proven_endpoints(prefix, old, candidate, expected) -> None:
    observed = _observation(old=old, candidate=candidate)
    decision = decide_replacement(_recovering(prefix), 0, observed, mode="recover")
    assert decision.step == expected
    assert not decision.provider_mutation
    assert decision.checkpoint is None
    assert not decision.requires_resume_authority
    assert decision.accepted_as == {"prior_verified": "prior", "candidate_verified": "candidate"}.get(expected)
    assert (decision.immutable_fingerprint is not None) == (expected != "blocked")


@pytest.mark.parametrize(("prefix", "old", "candidate", "expected"), [
    (1, "running", None, "term_old"),
    (2, None, None, "record_old_removed"),
    (3, None, None, "create_candidate"),
    (3, None, "created", "record_replacement_created"),
    (4, None, "created", "start_candidate"),
    (4, None, "running", "record_completed"),
    (4, None, "exited", "start_candidate"),
])
def test_explicit_same_operation_resume_requires_separate_backend_authority(prefix, old, candidate, expected) -> None:
    control, observed = _recovering(prefix), _observation(old=old, candidate=candidate)
    assert decide_replacement(control, 0, observed).reason == "explicit_same_operation_resume_required"
    decision = decide_replacement(control, 0, observed, mode="resume")
    assert decision.step == expected
    assert decision.generation == _generation()
    assert decision.requires_resume_authority
    assert control.status == "recovery_required"  # Caller still needs durable resume authorization.


@pytest.mark.parametrize("runner_state", ["closing", "closed"])
def test_term_is_not_removal_permission_while_old_process_has_not_exited(runner_state) -> None:
    old = _container(runner_state=runner_state)
    observed = _observation(prior_container=old)
    decision = decide_replacement(_control((_boundary(),)), 0, observed)
    assert decision.step == "wait_old_closed"
    assert not decision.provider_mutation
    assert decision.checkpoint is None


@pytest.mark.parametrize("changes", [
    {"status_fresh": False}, {"runner_state": None, "status_fresh": False},
    {"runner_state": "closing"}, {"runner_state": "failed"},
    {"exit_code": None}, {"exit_code": 1}, {"exit_code": 137}, {"exit_code": 130},
    {"forced_exit": True}, {"forced_exit": None},
])
@pytest.mark.parametrize("prefix", [1, 2])
@pytest.mark.parametrize("exit_code", [0, 143])
def test_allowed_exit_alone_stale_missing_or_forced_closure_never_permits_removal(prefix, changes, exit_code) -> None:
    old = replace(_container(state="exited", exit_code=exit_code), **changes)
    assert not old.cleanly_closed
    observed = _observation(old="exited", prior_container=old)
    decision = decide_replacement(_control(_boundaries(exit_code=exit_code)[:prefix]), 0, observed)
    assert decision.step == "blocked"
    assert decision.checkpoint is None


@pytest.mark.parametrize("exit_code", [0, 143])
def test_clean_close_checkpoint_preserves_exact_raw_exit_through_next_observation(exit_code) -> None:
    old = _container(state="exited", exit_code=exit_code)
    assert old.cleanly_closed
    observed = _observation(old="exited", prior_container=old)
    control = _control((_boundary(),))
    decision = decide_replacement(control, 0, observed)
    assert decision.step == "record_old_closed"
    assert not decision.provider_mutation
    assert decision.checkpoint.exit_code == exit_code
    checkpoint = OperationProgress(OPERATION, 0, RESOURCE, "update", "checkpoint", None, STAMP, decision.checkpoint)
    durable = replace(control, progress=(*control.progress, checkpoint))
    restored = OperationControlState.from_dict(durable.to_dict(), expected_address=ADDRESS)
    assert restored.progress[1].kafka_streams_checkpoint.exit_code == exit_code
    removal = decide_replacement(restored, 0, observed)
    assert removal.step == "remove_old"
    assert removal.container_id == OLD_ID


@pytest.mark.parametrize(("recorded_code", "observed_code"), [(0, 143), (143, 0)])
def test_durable_close_raw_exit_cannot_change_to_another_allowed_code(recorded_code, observed_code) -> None:
    old = _container(state="exited", exit_code=observed_code)
    assert old.cleanly_closed
    observed = _observation(old="exited", prior_container=old)
    control = _control(_boundaries(exit_code=recorded_code)[:2])
    decision = decide_replacement(control, 0, observed)
    assert decision.reason == "prior_changed_after_durable_clean_close"
    assert decision.step == "blocked"
    assert not decision.provider_mutation


@pytest.mark.parametrize("exit_code", [True, False, "143", "unknown", -1, 256])
def test_exit_observation_rejects_bool_string_and_out_of_range_code(exit_code) -> None:
    with pytest.raises(StateFormatError, match="exit code is invalid"):
        _container(state="exited", exit_code=exit_code)


@pytest.mark.parametrize("exit_code", [0, 143])
@pytest.mark.parametrize("prefix", [1, 2, 4])
def test_allowed_clean_exit_with_active_group_blocks_close_removal_and_resume(exit_code, prefix) -> None:
    is_candidate = prefix == 4
    container = _container(candidate=is_candidate, state="exited", exit_code=exit_code)
    assert container.cleanly_closed
    observed = _observation(
        old=None if is_candidate else "exited", candidate="exited" if is_candidate else None,
        prior_container=None if is_candidate else container,
        candidate_container=container if is_candidate else None, members=1,
    )
    control = _control(_boundaries(exit_code=exit_code)[:prefix])
    for mode in ("execute", "resume", "recover"):
        decision = decide_replacement(control, 0, observed, mode=mode)
        assert decision.step == "blocked"
        assert not decision.provider_mutation
        assert decision.checkpoint is None


@pytest.mark.parametrize("changes", [
    {"status_fresh": False}, {"runner_state": None, "status_fresh": False},
    {"runner_state": "running"}, {"runner_state": "failed"}, {"exit_code": None},
    {"exit_code": 1}, {"exit_code": 137}, {"exit_code": 130},
    {"forced_exit": True}, {"forced_exit": None},
])
@pytest.mark.parametrize("exit_code", [0, 143])
def test_candidate_resume_needs_exact_fresh_clean_closed_status(changes, exit_code) -> None:
    candidate = replace(_container(candidate=True, state="exited", exit_code=exit_code), **changes)
    assert not candidate.cleanly_closed
    observed = _observation(old=None, candidate="exited", candidate_container=candidate)
    for mode in ("execute", "resume", "recover"):
        decision = decide_replacement(_control(_boundaries()[:4]), 0, observed, mode=mode)
        assert decision.step == "blocked"


@pytest.mark.parametrize("exit_code", [0, 143])
def test_clean_candidate_restart_is_resume_only_and_never_reinitializes_offsets(exit_code) -> None:
    candidate = _container(candidate=True, state="exited", exit_code=exit_code)
    observed = _observation(old=None, candidate="exited", committed=70, candidate_container=candidate)
    # Candidate already consumed the original close point, now outside retention.
    observed = replace(observed, progress=replace(observed.progress, partitions=(KafkaStreamsPartitionEvidence(0, 60, 100, 70),)))
    control = _control(_boundaries()[:4])
    assert decide_replacement(control, 0, observed).step == "blocked"
    assert decide_replacement(control, 0, observed, mode="recover").step == "blocked"
    decision = decide_replacement(control, 0, observed, mode="resume")
    assert decision.step == "start_candidate"
    assert "without_offset_initialization" in decision.reason
    assert decision.container_id == NEW_ID
    assert decision.progress.partitions[0].committed == 70


@pytest.mark.parametrize("runner_state", [None, "starting"])
def test_started_candidate_waits_for_readiness_without_claiming_success(runner_state) -> None:
    candidate = _container(candidate=True, runner_state=runner_state, status_fresh=runner_state is not None)
    observed = _observation(old=None, candidate="running", candidate_container=candidate)
    assert decide_replacement(_control(_boundaries()[:4]), 0, observed).step == "wait_candidate_ready"
    assert decide_replacement(_control(_boundaries()[:4]), 0, observed, mode="recover").step == "blocked"


@pytest.mark.parametrize("prefix", [0, 1, 2, 3, 4, 5])
@pytest.mark.parametrize("mode", ["execute", "resume", "recover"])
def test_terminal_false_is_not_a_restart_or_finalization_escape_hatch(prefix, mode) -> None:
    # Successful completion is replaced by terminal failure at any allowed phase.
    count = max(1, min(prefix, 4))
    control = _control((*_boundaries()[:count], _boundary("completed", succeeded=False)))
    decision = decide_replacement(control, 0, _observation(old=None, candidate="running"), mode=mode)
    assert decision.step == "blocked"
    assert decision.reason == "terminal_failed_action_cannot_resume"


@pytest.mark.parametrize(("prefix", "old", "candidate", "reason"), [
    (0, None, None, "unstarted_prior_runtime_is_not_ready"),
    (1, None, None, "prior_absent_without_durable_clean_close"),
    (1, "running", "created", "old_and_candidate_both_present"),
    (2, None, "created", "candidate_created_before_durable_removal"),
    (3, "exited", None, "prior_container_reappeared_after_removal"),
    (3, None, "running", "candidate_started_before_creation_checkpoint"),
    (3, None, "exited", "candidate_started_before_creation_checkpoint"),
    (4, None, None, "journaled_candidate_is_missing"),
    (5, None, "created", "completed_candidate_is_not_ready"),
])
def test_ambiguous_or_out_of_order_runtime_states_fail_closed(prefix, old, candidate, reason) -> None:
    decision = decide_replacement(_control(_boundaries()[:prefix]), 0, _observation(old=old, candidate=candidate))
    assert (decision.step, decision.reason) == ("blocked", reason)


@pytest.mark.parametrize("changes", [
    {"container_id": "f" * 64}, {"application_id": "streamt-" + "f" * 32},
    {"backend_identity": "kafka-streams-docker:v1:" + "f" * 64},
    {"artifact_checksum": "sha256:" + "f" * 64}, {"plan_hash": "sha256:" + "f" * 64},
    {"image_id": "sha256:" + "f" * 64}, {"network_id": "f" * 64},
])
def test_exact_old_identity_and_actual_plan_bytes_are_not_only_name_or_labels(changes) -> None:
    observed = _observation(prior_container=_container(**changes))
    assert decide_replacement(_control((_boundary(),)), 0, observed).reason == "prior_container_identity_changed"


@pytest.mark.parametrize("changes", [
    {"container_id": OLD_ID}, {"application_id": "streamt-" + "f" * 32},
    {"backend_identity": "kafka-streams-docker:v1:" + "f" * 64},
    {"artifact_checksum": "sha256:" + "f" * 64}, {"plan_hash": "sha256:" + "f" * 64},
    {"image_id": "sha256:" + "f" * 64}, {"network_id": "f" * 64}, {"generation": None},
    {"generation": _generation(operation_id="00000000-0000-4000-8000-000000000009")},
    {"generation": _generation(action_index=1)},
    {"generation": _generation(evidence_fingerprint="sha256:" + "f" * 64)},
])
def test_candidate_must_match_this_exact_operation_action_evidence_generation(changes) -> None:
    observed = _observation(old=None, candidate="running", candidate_container=_container(candidate=True, **changes))
    assert decide_replacement(_control(_boundaries()[:4]), 0, observed).reason == "candidate_container_or_generation_changed"


def test_a_second_same_generation_container_cannot_replace_the_journaled_candidate() -> None:
    observed = _observation(old=None, candidate="running", candidate_container=_container(candidate=True, container_id="f" * 64))
    assert decide_replacement(_control(_boundaries()[:4]), 0, observed).reason == "journaled_candidate_container_changed"


@pytest.mark.parametrize("field", ["backend_identity", "image_id", "network_id", "volume_token", "volume_created_at"])
def test_provider_witnesses_remain_fixed_after_old_container_disappears(field) -> None:
    observed = _observation(old=None)
    if field == "volume_token":
        observed = replace(observed, volume=replace(observed.volume, token="00000000-0000-4000-8000-000000000009"))
    elif field == "volume_created_at":
        observed = replace(observed, volume=replace(observed.volume, created_at="2026-09-05T12:00:01Z"))
    else:
        value = {"backend_identity": "kafka-streams-docker:v1:" + "f" * 64, "image_id": "sha256:" + "f" * 64, "network_id": "f" * 64}[field]
        observed = replace(observed, **{field: value})
    decision = decide_replacement(_control(_boundaries()[:3]), 0, observed)
    assert decision.reason == "backend_image_network_or_volume_changed"


@pytest.mark.parametrize("field", ["missing", "physical_name", "ownership", "backend", "artifact_checksum"])
def test_protected_prior_ownership_cannot_be_missing_adopted_or_rebased(field) -> None:
    owner = _state().resources[RESOURCE]
    values = {"physical_name": "other", "ownership": "adopted", "backend": "different", "artifact_checksum": "sha256:" + "f" * 64}
    owner = None if field == "missing" else replace(owner, **{field: values[field]})
    observed = _observation(old=None, ownership_record=owner)
    assert decide_replacement(_control(_boundaries()[:3]), 0, observed).step == "blocked"


def test_state_commit_is_after_completion_and_remains_classifiable_after_commit() -> None:
    observed = _observation(old=None, candidate="running", ownership_record=_desired_state().resources[RESOURCE])
    assert decide_replacement(_control(_boundaries()[:4]), 0, observed).reason == "candidate_ownership_precedes_completed_action"
    for mode in ("execute", "resume", "recover"):
        decision = decide_replacement(_control(_boundaries()), 0, observed, mode=mode)
        assert decision.step == "candidate_verified"
    clear = OperationControlState(ADDRESS)
    assert decide_replacement(clear, 0, observed).step == "blocked"


@pytest.mark.parametrize("changes", [
    {"cluster_id": "different-cluster"}, {"input_topic_id": "AAAAAAAAAAAAAAAAAAAAAw"},
    {"output_topic_id": "AAAAAAAAAAAAAAAAAAAAAw"},
    {"partitions": (KafkaStreamsPartitionEvidence(0, 0, 100, 19),)},
    {"partitions": (KafkaStreamsPartitionEvidence(0, 0, 99, 20),)},
    {"partitions": (KafkaStreamsPartitionEvidence(0, 0, 100, 20), KafkaStreamsPartitionEvidence(1, 0, 100, 20))},
])
def test_progress_binding_or_lower_bound_changes_block_every_post_close_step(changes) -> None:
    observed = _observation(old=None, progress=replace(_progress(committed=20, active_members=0), **changes))
    decision = decide_replacement(_control(_boundaries()[:3]), 0, observed)
    assert decision.reason == "progress_identity_retention_or_lower_bound_changed"


def test_progress_may_advance_but_cannot_regress_from_reviewed_or_clean_close_lower_bound() -> None:
    before_started = _observation(committed=9)
    assert decide_replacement(_control(), 0, before_started).step == "blocked"
    for committed in (20, 21, 70, 100):
        observed = _observation(old=None, candidate="running", committed=committed)
        assert decide_replacement(_control(_boundaries()[:4]), 0, observed).step == "record_completed"
    with pytest.raises(StateFormatError):
        KafkaStreamsPartitionEvidence(0, 21, 100, 20)  # Current offset itself was lost.


@pytest.mark.parametrize(("prefix", "old", "candidate"), [
    (2, "exited", None), (2, None, None), (3, None, None), (3, None, "created"), (4, None, "created"),
])
def test_retention_cannot_skip_clean_close_point_before_first_candidate_start(prefix, old, candidate) -> None:
    observed = _observation(old=old, candidate=candidate, committed=30)
    observed = replace(observed, progress=replace(observed.progress, partitions=(KafkaStreamsPartitionEvidence(0, 21, 100, 30),)))
    assert decide_replacement(_control(_boundaries()[:prefix]), 0, observed).reason == "clean_close_resume_point_lost_to_retention"


@pytest.mark.parametrize(("prefix", "old", "candidate"), [
    (2, "exited", None), (2, None, None), (3, None, None), (3, None, "created"), (4, None, "created"),
])
def test_forward_offset_jump_cannot_skip_records_before_candidate_first_start(prefix, old, candidate) -> None:
    observed = _observation(old=old, candidate=candidate, committed=21)
    decision = decide_replacement(_control(_boundaries()[:prefix]), 0, observed)
    assert decision.reason == "committed_offsets_changed_before_first_candidate_start"
    assert not decision.provider_mutation


@pytest.mark.parametrize(("prefix", "old", "candidate", "expected"), [
    (2, "exited", None, "remove_old"), (2, None, None, "record_old_removed"),
    (3, None, None, "create_candidate"), (3, None, "created", "record_replacement_created"),
    (4, None, "created", "start_candidate"),
])
def test_producer_high_watermark_and_safe_retention_advance_without_committed_change(prefix, old, candidate, expected) -> None:
    observed = _observation(old=old, candidate=candidate)
    observed = replace(observed, progress=replace(observed.progress, partitions=(KafkaStreamsPartitionEvidence(0, 20, 150, 20),)))
    assert decide_replacement(_control(_boundaries()[:prefix]), 0, observed).step == expected


@pytest.mark.parametrize(("prefix", "old", "candidate", "members"), [
    (0, "running", None, 0), (0, "running", None, 2), (1, "running", None, 2),
    (1, "exited", None, 1), (2, "exited", None, 1), (3, None, None, 1),
    (3, None, "created", 1), (4, None, "created", 1),
    (4, None, "running", 0), (4, None, "running", 2), (4, None, "exited", 1),
])
def test_inactive_transition_and_single_member_readiness_are_required(prefix, old, candidate, members) -> None:
    observed = _observation(old=old, candidate=candidate, members=members)
    assert decide_replacement(_control(_boundaries()[:prefix]), 0, observed, mode="resume").step == "blocked"


def test_preceding_actions_must_succeed_and_no_later_action_may_have_started() -> None:
    topic = OperationAction(0, "streamt://payments/prod/topic/other", "create")
    runner = replace(_intent().actions[0], index=1)
    control = OperationControlState(ADDRESS, "in_progress", replace(_intent(), actions=(topic, runner)))
    assert decide_replacement(control, 1, _observation()).reason == "preceding_action_incomplete"
    started = OperationProgress(OPERATION, 0, topic.resource_id, "create", "started", None, STAMP)
    control = replace(control, progress=(started,))
    assert decide_replacement(control, 1, _observation()).reason == "preceding_action_incomplete"
    completed = replace(started, status="completed", succeeded=True)
    control = replace(control, progress=(started, completed))
    decision = decide_replacement(control, 1, _observation())
    assert decision.step == "record_started"
    assert decision.generation == _generation(action_index=1)

    later = replace(topic, index=1)
    later_started = replace(started, action_index=1)
    control = OperationControlState(ADDRESS, "in_progress", replace(_intent(), actions=(_intent().actions[0], later)), (*_boundaries(), later_started))
    for mode in ("execute", "resume", "recover"):
        assert decide_replacement(control, 0, _observation(old=None, candidate="running"), mode=mode).reason == "replacement_is_not_journal_frontier"


def test_dynamic_offsets_and_runtime_state_are_not_immutable_fingerprint_material() -> None:
    current = _container(candidate=True)
    closed = replace(current, process_state="exited", exit_code=0, runner_state="closed")
    assert current.immutable_fingerprint == closed.immutable_fingerprint
    decisions = [decide_replacement(_control(_boundaries()[:4]), 0, _observation(old=None, candidate="running", committed=offset), mode="recover") for offset in (20, 50)]
    assert decisions[0].immutable_fingerprint == decisions[1].immutable_fingerprint
    assert decisions[0].progress != decisions[1].progress
    assert current.immutable_fingerprint != replace(current, container_id="f" * 64).immutable_fingerprint
    assert current.immutable_fingerprint != replace(current, generation=_generation(action_index=1)).immutable_fingerprint


@pytest.mark.parametrize("changes", [
    {"operation_id": "short"}, {"operation_id": "00000000-0000-0000-0000-000000000000"},
    {"action_index": True}, {"action_index": -1}, {"action_index": 2**63},
    {"evidence_fingerprint": "redacted"},
])
def test_generation_labels_are_strict_and_canonical(changes) -> None:
    with pytest.raises(StateFormatError):
        _generation(**changes)


@pytest.mark.parametrize("changes", [
    {"container_id": "short"}, {"artifact_checksum": "redacted"}, {"image_id": "latest"},
    {"network_id": "bridge"}, {"volume": {}}, {"process_state": "paused"},
    {"process_state": {}}, {"exit_code": 0}, {"exit_code": True}, {"forced_exit": "false"},
    {"runner_state": {}}, {"runner_state": "unknown"}, {"status_fresh": 1},
    {"runner_state": None}, {"generation": {}},
])
def test_container_observations_never_silently_coerce_unknown_proofs(changes) -> None:
    with pytest.raises(StateFormatError):
        _container(**changes)


def test_created_container_cannot_reuse_closed_or_ready_status_from_shared_volume() -> None:
    for status in ("closed", "running"):
        with pytest.raises(StateFormatError, match="shared volume"):
            _container(candidate=True, state="created", runner_state=status, status_fresh=True)
    candidate = _container(candidate=True, state="created")
    assert not candidate.ready
    assert not candidate.cleanly_closed


@pytest.mark.parametrize("changes", [{"volume": {}}, {"progress": {}}, {"prior_container": {}}, {"candidate_container": {}}, {"ownership_record": {}}])
def test_complete_snapshot_requires_typed_local_proofs(changes) -> None:
    with pytest.raises(StateFormatError):
        _observation(**changes)


def test_decisions_are_immutable_provider_free_and_cannot_finish_an_operation(monkeypatch) -> None:
    def forbidden(*args, **kwargs):
        pytest.fail("pure replacement decision attempted provider or process access")

    control, observed = _control(_boundaries()[:3]), _observation(old=None)
    monkeypatch.setattr(socket, "socket", forbidden)
    monkeypatch.setattr(socket, "create_connection", forbidden)
    monkeypatch.setattr(subprocess, "Popen", forbidden)
    monkeypatch.setattr(subprocess, "run", forbidden)
    decision = decide_replacement(control, 0, observed)
    assert decision.step == "create_candidate"
    assert control.status == "in_progress"
    assert len(control.progress) == 3
    for target, field, value in ((decision, "step", "candidate_verified"), (observed, "prior_container", None), (_generation(), "action_index", 2)):
        with pytest.raises(FrozenInstanceError):
            setattr(target, field, value)
