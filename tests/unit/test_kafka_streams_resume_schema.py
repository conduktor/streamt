"""Same-operation resume authority: strict wire bytes and retained incidents."""

from __future__ import annotations

import copy
import json
from dataclasses import FrozenInstanceError, replace
from uuid import UUID

import pytest

from streamt.deployer.state import StateError, StateFormatError
from streamt.deployer.state_backend import (
    MAX_OPERATION_RESUMES,
    ControlObservation,
    OperationAction,
    OperationControlState,
    OperationResumeRecord,
    OperationSnapshot,
    RecoveryRecord,
    StateAddress,
    StateObservation,
    StateRevision,
    StateStoreIdentity,
    _control_revision,
    _validate_resume_transition_inputs,
    state_checksum,
)
from tests.unit.test_kafka_streams_operation_evidence import (
    ADDRESS,
    OPERATION,
    STAMP,
    _boundaries,
    _boundary,
    _desired_state,
    _intent,
    _state,
)

STORE = StateStoreIdentity("local", "00000000-0000-4000-8000-000000000003")
REVIEWED = "sha256:" + "9" * 64
RESUME = "00000000-0000-4000-8000-000000000004"


def snapshot(control: OperationControlState | None = None, *, prefix: int = 2) -> OperationSnapshot:
    state = _state()
    control = control or OperationControlState(
        ADDRESS, "recovery_required", replace(_intent(), reviewed_plan_checksum=REVIEWED),
        _boundaries(exit_code=143)[:prefix], RecoveryRecord(OPERATION, "lost_ack", STAMP, None),
    )
    return OperationSnapshot(
        StateObservation(STORE, ADDRESS, state, StateRevision(state_checksum(state))),
        ControlObservation(control, _control_revision(control)),
    )


def authority(observed: OperationSnapshot | None = None, *, number: int = 0) -> OperationResumeRecord:
    return OperationResumeRecord.create(
        observed or snapshot(), resume_id=str(UUID(int=UUID(RESUME).int + number)),
        actor="operator", resumed_at=STAMP,
    )


@pytest.mark.parametrize("prefix", range(5))
def test_resume_retains_exact_intent_progress_and_failure_at_every_frontier(prefix):
    observed = snapshot(prefix=prefix)
    before = observed.control.control
    record = authority(observed)
    resumed = _validate_resume_transition_inputs(observed, record)
    assert resumed.control_version == 5
    assert resumed.intent is before.intent
    assert resumed.progress is before.progress
    assert resumed.recovery is None
    assert resumed.status == "in_progress"
    assert resumed.resume_history == (record,)
    assert record.recovery is before.recovery
    assert record.source_control_checksum == observed.control.revision.value
    assert resumed.to_dict()["intent"] == before.to_dict()["intent"]
    assert resumed.to_dict()["progress"] == before.to_dict()["progress"]
    loaded = OperationControlState.from_dict(resumed.to_dict(), expected_address=ADDRESS)
    assert loaded == resumed
    assert loaded.intent._wire_version == 4
    assert loaded.intent.actions[0]._wire_version == 4
    assert loaded.to_dict() == resumed.to_dict()
    assert OperationResumeRecord.from_dict(record.to_dict()) == record
    assert OperationResumeRecord.from_dict(json.loads(json.dumps(record.to_dict()))) == record
    if prefix >= 2:
        assert loaded.progress[1].kafka_streams_checkpoint.exit_code == 143


def test_resume_records_are_deeply_immutable_and_serialization_has_no_aliases():
    record = authority()
    with pytest.raises(FrozenInstanceError):
        record.actor = "other"
    payload = record.to_dict()
    payload["store"]["store_id"] = RESUME
    payload["recovery"]["failure_code"] = "altered"
    assert record.store == STORE
    assert record.recovery.failure_code == "lost_ack"


@pytest.mark.parametrize(("key", "value"), [
    ("resume_id", "bad"), ("resume_id", OPERATION), ("operation_id", RESUME),
    ("address", "payments"), ("store", None), ("store", {"backend": "local", "store_id": "bad"}),
    ("prior_state_serial", True), ("prior_state_serial", -1),
    ("prior_state_checksum", None), ("source_control_checksum", "partial"),
    ("progress_count", True), ("progress_count", -1), ("progress_count", 5),
    ("progress_count", 1.0), ("actor", "password=secret"), ("actor", "a\nheader"),
    ("actor", "a" * 129), ("actor", ""), ("actor", "\ud800"), ("actor", "a\x7f"),
    ("actor", "https://user:secret@example.test"), ("resumed_at", "2026-99-05T12:00:00Z"),
    ("recovery", None), ("extra", True),
])
def test_resume_record_rejects_untyped_invalid_or_unsafe_fields(key, value):
    payload = authority().to_dict()
    payload[key] = value
    with pytest.raises(StateError):
        OperationResumeRecord.from_dict(payload)


@pytest.mark.parametrize("key", list(authority().to_dict()))
def test_resume_record_requires_every_field(key):
    payload = authority().to_dict()
    del payload[key]
    with pytest.raises(StateFormatError):
        OperationResumeRecord.from_dict(payload)


@pytest.mark.parametrize("key", ["operation_id", "failure_code", "failed_at", "last_completed_action_index", "mutation_may_have_succeeded"])
def test_resume_record_retains_strict_complete_recovery_record(key):
    payload = authority().to_dict()
    del payload["recovery"][key]
    with pytest.raises(StateFormatError):
        OperationResumeRecord.from_dict(payload)


@pytest.mark.parametrize(("key", "value"), [
    ("source_control_checksum", "sha256:" + "8" * 64), ("progress_count", 0),
    ("prior_state_serial", 2), ("prior_state_checksum", "sha256:" + "8" * 64),
    ("address", StateAddress("local", "elsewhere", "prod")),
    ("store", StateStoreIdentity("local", RESUME)),
    ("recovery", RecoveryRecord(OPERATION, "other_failure", STAMP, None)),
])
def test_transition_binds_exact_observed_control_state_store_and_incident(key, value):
    observed = snapshot()
    record = replace(authority(observed), **{key: value})
    with pytest.raises(StateError):
        _validate_resume_transition_inputs(observed, record)


def test_state_already_committed_must_be_recovered_never_resumed():
    observed = snapshot()
    changed = _desired_state()
    observed = replace(observed, state=replace(observed.state, state=changed, revision=StateRevision(state_checksum(changed))))
    with pytest.raises(StateError, match="interrupted operation"):
        authority(observed)


@pytest.mark.parametrize("succeeded", [True, False])
def test_terminal_completion_never_allows_resume(succeeded):
    observed = snapshot(prefix=4)
    control = replace(
        observed.control.control,
        progress=(*observed.control.control.progress, _boundary("completed", succeeded=succeeded)),
        recovery=RecoveryRecord(OPERATION, "interrupted_commit", STAMP, 0 if succeeded else None),
    )
    with pytest.raises(StateError):
        authority(snapshot(control))


@pytest.mark.parametrize("status", ["clear", "in_progress"])
def test_authority_requires_a_recorded_interruption(status):
    control = OperationControlState.clear(ADDRESS) if status == "clear" else replace(snapshot().control.control, status="in_progress", recovery=None)
    with pytest.raises(StateError, match="recorded interruption"):
        authority(snapshot(control))


def test_resume_requires_original_reviewed_sole_typed_runner_intent():
    for intent in (
        _intent(),
        replace(_intent(), reviewed_plan_checksum=REVIEWED, actions=(*_intent().actions, OperationAction(1, "streamt://payments/prod/topic/extra", "create"))),
    ):
        with pytest.raises(StateError, match="one reviewed v4 runner"):
            authority(snapshot(replace(snapshot().control.control, intent=intent)))


@pytest.mark.parametrize("version", [1, 2, 3, 4])
def test_legacy_controls_never_silently_accept_resume_history(version):
    payload = snapshot().control.control.to_dict()
    payload["control_version"] = version
    payload["resume_history"] = [authority().to_dict()]
    with pytest.raises(StateFormatError):
        OperationControlState.from_dict(payload, expected_address=ADDRESS)


@pytest.mark.parametrize("value", [None, [], {}, "", [None], [authority().to_dict()] * (MAX_OPERATION_RESUMES + 1)])
def test_v5_requires_bounded_strict_nonempty_history(value):
    payload = snapshot().control.control.to_dict()
    payload["control_version"] = 5
    payload["resume_history"] = value
    with pytest.raises(StateFormatError):
        OperationControlState.from_dict(payload, expected_address=ADDRESS)


@pytest.mark.parametrize("change", ["incident", "progress", "source", "intent", "duplicate", "reorder", "drop_first", "store", "address"])
def test_multiple_resume_history_rejects_edited_incident_progress_or_authority(change):
    observed = snapshot(prefix=2)
    first = _validate_resume_transition_inputs(observed, authority(observed))
    interrupted = replace(first, status="recovery_required", recovery=RecoveryRecord(OPERATION, "second_failure", STAMP, None), progress=_boundaries(exit_code=143)[:3])
    second = _validate_resume_transition_inputs(snapshot(interrupted), authority(snapshot(interrupted), number=1))
    payload = copy.deepcopy(second.to_dict())
    if change == "incident":
        payload["resume_history"][0]["recovery"]["failure_code"] = "other"
    elif change == "progress":
        payload["resume_history"][1]["progress_count"] = 1
    elif change == "source":
        payload["resume_history"][1]["source_control_checksum"] = "sha256:" + "f" * 64
    elif change == "intent":
        payload["intent"]["reviewed_plan_checksum"] = "sha256:" + "f" * 64
    elif change == "duplicate":
        payload["resume_history"][1]["resume_id"] = payload["resume_history"][0]["resume_id"]
    elif change == "reorder":
        payload["resume_history"].reverse()
    elif change == "drop_first":
        payload["resume_history"].pop(0)
    elif change == "store":
        payload["resume_history"][1]["store"]["store_id"] = RESUME
    elif change == "address":
        payload["resume_history"][0]["address"] = "streamt-state://local/other/prod"
    with pytest.raises(StateError):
        OperationControlState.from_dict(payload, expected_address=ADDRESS)


def test_repeated_interruptions_at_same_frontier_are_bounded_and_retain_original_action_bytes():
    observed = snapshot()
    original = observed.control.control.intent.to_dict()
    for number in range(MAX_OPERATION_RESUMES):
        control = _validate_resume_transition_inputs(observed, authority(observed, number=number))
        assert control.intent.to_dict() == original
        assert len(control.resume_history) == number + 1
        observed = snapshot(replace(control, status="recovery_required", recovery=RecoveryRecord(OPERATION, "again", STAMP, None)))
    loaded = OperationControlState.from_dict(control.to_dict(), expected_address=ADDRESS)
    assert loaded == control
    assert loaded.intent.to_dict() == original
    with pytest.raises(StateFormatError, match="bounded"):
        authority(observed, number=MAX_OPERATION_RESUMES)
