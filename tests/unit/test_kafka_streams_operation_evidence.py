"""Runner replacement journal: strict evidence, ordering and durable history."""

from __future__ import annotations

import copy
import hashlib
import json
import os
from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest

from streamt.compiler.kafka_streams import application_id, compile_plan
from streamt.compiler.manifest import KafkaStreamsJobArtifact
from streamt.core.deployment_state import local_deployment_state_config
from streamt.deployer.kafka_streams import runner_plan_hash
from streamt.deployer.kafka_streams_evidence import (
    KafkaStreamsActionEvidence,
    KafkaStreamsArtifactSnapshot,
    KafkaStreamsCheckpointEvidence,
    KafkaStreamsPartitionEvidence,
    KafkaStreamsProgressEvidence,
    KafkaStreamsVolumeEvidence,
)
from streamt.deployer.postgres_state import PrivatePostgresStateV2Migrator
from streamt.deployer.state import (
    LocalState,
    ManagedResourceRecord,
    StateFormatError,
    local_state_path,
)
from streamt.deployer.state_backend import (
    CURRENT_CONTROL_VERSION,
    OperationAction,
    OperationControlState,
    OperationIntent,
    OperationProgress,
    StateAddress,
    StateBackendConflictError,
    StateBackendInvalidStateError,
    StateBackendRecoveryRequiredError,
    StateBackendUnavailableError,
    StateBackendUnknownCommitError,
    make_deployment_state_service,
    state_checksum,
)
from tests.unit.test_operation_control import _connector_action, _gateway_action
from tests.unit.test_postgres_state_mutation import _operation
from tests.unit.test_postgres_state_v2 import _Cursor, _FakeSql

STAMP = "2026-09-05T12:00:00Z"
OPERATION = "00000000-0000-4000-8000-000000000001"
TOKEN = "00000000-0000-4000-8000-000000000002"
BACKEND = "kafka-streams-docker:v1:" + "b" * 64
OLD_ID, NEW_ID = "c" * 64, "d" * 64
IMAGE, NETWORK = "sha256:" + "a" * 64, "e" * 64
APP = application_id("payments", "prod", "filtered")
RESOURCE = "streamt://payments/prod/kafka_streams_job/filtered"
ADDRESS = StateAddress("local", "payments", "prod")
INPUT_ID, OUTPUT_ID = "AAAAAAAAAAAAAAAAAAAAAQ", "AAAAAAAAAAAAAAAAAAAAAg"


def _artifact(threshold: int = 50) -> KafkaStreamsJobArtifact:
    plan = compile_plan(
        f"SELECT id, amount FROM orders WHERE amount >= {threshold}",
        {"id": {"type": "STRING", "nullable": False}, "amount": {"type": "BIGINT", "nullable": False}},
        "orders", "orders.input", "orders.output",
    )
    return KafkaStreamsJobArtifact(
        "filtered", APP, IMAGE, "bridge", "earliest", plan,
        {"mode": "managed", "project": "payments", "type": "model", "name": "filtered"},
    )


def _progress(*, committed: int = 10, active_members: int = 1) -> KafkaStreamsProgressEvidence:
    return KafkaStreamsProgressEvidence(
        "cluster-a", INPUT_ID, OUTPUT_ID, True, active_members,
        (KafkaStreamsPartitionEvidence(0, 0, 100, committed),),
    )


def _evidence() -> KafkaStreamsActionEvidence:
    return KafkaStreamsActionEvidence(
        1, BACKEND, OLD_ID, KafkaStreamsArtifactSnapshot.from_artifact(_artifact()),
        KafkaStreamsArtifactSnapshot.from_artifact(_artifact(75)), IMAGE, NETWORK,
        KafkaStreamsVolumeEvidence(APP + "-state", "local", STAMP, APP, BACKEND, TOKEN),
        _progress(),
    )


def _state() -> LocalState:
    evidence = _evidence()
    return LocalState(
        project="payments", environment="prod", serial=1,
        resources={RESOURCE: ManagedResourceRecord(APP, "managed", evidence.prior_artifact.checksum, BACKEND)},
    )


def _desired_state() -> LocalState:
    state = _state()
    state.serial += 1
    state.resources[RESOURCE] = replace(state.resources[RESOURCE], artifact_checksum=_evidence().desired_artifact.checksum)
    return state


def _intent(state: LocalState | None = None) -> OperationIntent:
    state = state or _state()
    return OperationIntent(
        OPERATION, "apply", STAMP, "unit-test", state.serial, state_checksum(state), None,
        (OperationAction(0, RESOURCE, "update", kafka_streams_evidence=_evidence()),),
    )


def _boundary(status: str = "started", *, succeeded: bool | None = None) -> OperationProgress:
    return OperationProgress(OPERATION, 0, RESOURCE, "update", status, succeeded, STAMP)  # type: ignore[arg-type]


def _checkpoint(phase: str) -> OperationProgress:
    closed = phase == "old_closed"
    checkpoint = KafkaStreamsCheckpointEvidence(
        1, phase, OPERATION, 0, OLD_ID, NEW_ID if phase == "replacement_created" else None,
        _evidence().prior_artifact.plan_hash if closed else None, 0 if closed else None,
        _progress(committed=20, active_members=0) if closed else None,
    )
    return OperationProgress(OPERATION, 0, RESOURCE, "update", "checkpoint", None, STAMP, checkpoint)


def _boundaries() -> tuple[OperationProgress, ...]:
    return (
        _boundary(), _checkpoint("old_closed"), _checkpoint("old_removed"),
        _checkpoint("replacement_created"), _boundary("completed", succeeded=True),
    )


def _control(progress: tuple[OperationProgress, ...] = ()) -> OperationControlState:
    return OperationControlState(ADDRESS, "in_progress", _intent(), progress)


def test_exact_artifacts_and_progress_round_trip_without_aliasing_or_credentials() -> None:
    evidence = _evidence()
    assert KafkaStreamsActionEvidence.from_dict(evidence.to_dict()) == evidence
    assert evidence.prior_artifact.plan_hash == runner_plan_hash(_artifact().plan)
    payload = evidence.to_dict()
    payload["prior_artifact"]["plan"]["predicates"].clear()
    artifact_copy = evidence.prior_artifact.artifact
    artifact_copy.plan["predicates"].clear()
    assert evidence.prior_artifact.artifact.plan["predicates"]
    assert "client.properties" not in json.dumps(evidence.to_dict())
    assert "bootstrap" not in repr(evidence)
    assert evidence != replace(evidence, progress=_progress(committed=15))
    assert evidence.immutable_fingerprint == replace(evidence, progress=_progress(committed=15)).immutable_fingerprint
    partition_added = replace(_progress(), partitions=(*_progress().partitions, KafkaStreamsPartitionEvidence(1, 0, 100, 10)))
    assert evidence.immutable_fingerprint != replace(evidence, progress=partition_added).immutable_fingerprint


@pytest.mark.parametrize(("path", "value"), [
    (("version",), True), (("version",), 2), (("backend_identity",), "https://user:secret@example"),
    (("prior_container_id",), "short"), (("image_id",), "latest"), (("network_id",), "bridge"),
    (("volume", "token"), None), (("volume", "token"), "00000000-0000-0000-0000-000000000000"),
    (("volume", "driver"), "remote"), (("volume", "created_at"), "2026-99-05T12:00:00Z"),
    (("volume", "name"), APP + "-different"), (("volume", "backend_identity"), "kafka-streams-docker:v1:" + "f" * 64),
    (("progress", "group_exists"), False), (("progress", "active_members"), True),
    (("progress", "input_topic_id"), "AAAAAAAAAAAAAAAAAAAAAA"),
    (("progress", "input_topic_id"), OUTPUT_ID), (("progress", "partitions"), []),
    (("progress", "partitions", 0, "committed"), None),
    (("progress", "partitions", 0, "committed"), True),
    (("progress", "partitions", 0, "committed"), 101),
    (("progress", "partitions", 0, "partition"), 1),
    (("progress", "partitions", 0, "high"), 2**63),
    (("desired_artifact", "initial_offset"), "latest"),
    (("desired_artifact", "network"), "different"),
    (("desired_artifact", "image"), "sha256:" + "f" * 64),
    (("desired_artifact", "plan", "input_topic"), "orders.changed"),
    (("desired_artifact", "plan", "output_topic"), "orders.changed"),
    (("desired_artifact", "plan", "schema", "id", "nullable"), True),
    (("desired_artifact", "plan", "projection", 0, "as"), "changed"),
    (("desired_artifact", "ownership", "mode"), "external"),
])
def test_action_evidence_rejects_invalid_identity_progress_or_non_predicate_changes(path, value) -> None:
    payload: Any = _evidence().to_dict()
    target = payload
    for key in path[:-1]:
        target = target[key]
    target[path[-1]] = value
    with pytest.raises(StateFormatError) as captured:
        KafkaStreamsActionEvidence.from_dict(payload)
    assert "user:secret" not in str(captured.value)


@pytest.mark.parametrize("section", [None, "prior_artifact", "desired_artifact", "volume", "progress"])
def test_unknown_or_missing_evidence_fields_fail_closed(section) -> None:
    for missing in (False, True):
        payload: Any = _evidence().to_dict()
        target = payload if section is None else payload[section]
        if missing:
            target.pop(next(iter(target)))
        else:
            target["credentials"] = "password=not-for-the-journal"
        with pytest.raises(StateFormatError):
            KafkaStreamsActionEvidence.from_dict(payload)


def test_snapshot_rejects_noncanonical_duplicate_and_oversized_json() -> None:
    snapshot = _evidence().prior_artifact
    for raw in (json.dumps(snapshot.to_dict(), indent=2), snapshot.canonical_json[:-1] + ',"name":"filtered"}', " " * 65537):
        with pytest.raises(StateFormatError):
            KafkaStreamsArtifactSnapshot(raw)
    with pytest.raises(StateFormatError, match="must change"):
        replace(_evidence(), desired_artifact=snapshot)


@pytest.mark.parametrize(("resource", "action"), [
    (RESOURCE, "create"), (RESOURCE, "delete"), (RESOURCE, "adopt"),
    (RESOURCE.replace("/prod/", "/dev/"), "update"),
    (RESOURCE.replace("/filtered", "/other"), "update"),
    (RESOURCE.replace("/kafka_streams_job/", "/topic/"), "update"),
])
def test_action_rejects_other_kind_address_name_or_transition(resource, action) -> None:
    with pytest.raises(StateFormatError):
        OperationAction(0, resource, action, kafka_streams_evidence=_evidence())


def test_replacement_requires_evidence_and_explicit_version_four() -> None:
    with pytest.raises(StateFormatError, match="requires exact"):
        OperationAction(0, RESOURCE, "update")
    action = _intent().actions[0]
    assert OperationAction.from_dict(action.to_dict()) == action
    assert _control().control_version == 4
    assert OperationControlState.from_dict(_control().to_dict(), expected_address=ADDRESS) == _control()
    for version in (1, 2, 3):
        with pytest.raises(StateFormatError):
            action.to_dict(control_version=version)
        with pytest.raises(StateFormatError):
            OperationAction.from_dict(action.to_dict(), control_version=version)
        with pytest.raises(StateFormatError):
            _checkpoint("old_closed").to_dict(control_version=version)
        bad = _control((_boundary(), _checkpoint("old_closed"))).to_dict()
        bad["control_version"] = version
        with pytest.raises(StateFormatError):
            OperationControlState.from_dict(bad, expected_address=ADDRESS)
    with pytest.raises(StateFormatError, match="another state address"):
        replace(_control(), address=replace(ADDRESS, environment="dev"))


@pytest.mark.parametrize("version", [1, 2, 3])
def test_legacy_control_and_progress_bytes_survive_reconstruction(version: int) -> None:
    intent = replace(_intent(), actions=(OperationAction(0, "topic:orders", "create"),))
    original = OperationControlState(ADDRESS, "in_progress", intent, control_version=version).to_dict()
    raw = json.dumps(original, sort_keys=True, separators=(",", ":"))
    loaded = OperationControlState.from_dict(json.loads(raw), expected_address=ADDRESS)
    reconstructed = OperationControlState(ADDRESS, "in_progress", loaded.intent)
    assert reconstructed.to_dict() == original
    assert hashlib.sha256(json.dumps(reconstructed.to_dict(), sort_keys=True, separators=(",", ":")).encode()).hexdigest() == hashlib.sha256(raw.encode()).hexdigest()
    boundary = OperationProgress(OPERATION, 0, "topic:orders", "create", "started", None, STAMP)
    assert OperationProgress.from_dict(boundary.to_dict(control_version=version), control_version=version) == boundary
    assert CURRENT_CONTROL_VERSION == OperationControlState.clear(ADDRESS).control_version == 3


def test_existing_gateway_connector_evidence_stays_v3_and_cannot_mix_with_runner_evidence() -> None:
    for action in (_gateway_action(), _connector_action()):
        assert "kafka_streams_evidence" not in action.to_dict()
        with pytest.raises(StateFormatError, match="mutually exclusive"):
            replace(action, kafka_streams_evidence=_evidence())


def test_every_ordered_replacement_prefix_round_trips_and_only_full_success_completes() -> None:
    boundaries = _boundaries()
    for count in range(len(boundaries) + 1):
        control = _control(boundaries[:count])
        assert OperationControlState.from_dict(control.to_dict(), expected_address=ADDRESS) == control
        assert control.actions_completed is (count == len(boundaries))
    for count in range(1, len(boundaries)):
        failed = _control((*boundaries[:count], _boundary("completed", succeeded=False)))
        assert not failed.actions_completed
    with pytest.raises(StateFormatError, match="immutable ordered tuple"):
        _control(list(boundaries))


@pytest.mark.parametrize("indices", [
    (1,), (0, 2), (0, 3), (0, 4), (0, 1, 4), (0, 1, 2, 4),
    (0, 1, 1), (0, 1, 2, 2), (0, 1, 2, 3, 3),
    (0, 2, 1), (0, 1, 2, 3, 4, 1),
])
def test_checkpoint_gaps_reordering_duplicates_and_post_completion_progress_fail(indices) -> None:
    boundaries = _boundaries()
    with pytest.raises(StateFormatError):
        _control(tuple(boundaries[index] for index in indices))


@pytest.mark.parametrize(("field", "value"), [
    ("operation_id", TOKEN), ("action_index", 1), ("prior_container_id", NEW_ID),
    ("closed_plan_hash", "sha256:" + "f" * 64), ("exit_code", 1),
    ("exit_code", False), ("replacement_container_id", NEW_ID),
    ("version", True), ("phase", "unknown"),
])
def test_checkpoint_mismatched_generation_container_plan_and_close_proof_fail(field, value) -> None:
    with pytest.raises(StateFormatError):
        _control((_boundary(), replace(_checkpoint("old_closed"), kafka_streams_checkpoint=replace(_checkpoint("old_closed").kafka_streams_checkpoint, **{field: value}))))


@pytest.mark.parametrize("progress", [
    _progress(active_members=1), _progress(committed=9, active_members=0),
    replace(_progress(active_members=0), input_topic_id="AAAAAAAAAAAAAAAAAAAAAw"),
    replace(_progress(active_members=0), cluster_id="other-cluster"),
])
def test_close_checkpoint_requires_inactive_same_identity_and_monotonic_offsets(progress) -> None:
    with pytest.raises(StateFormatError):
        _control((_boundary(), replace(_checkpoint("old_closed"), kafka_streams_checkpoint=replace(_checkpoint("old_closed").kafka_streams_checkpoint, progress=progress))))


def test_checkpoint_cannot_attach_to_another_action_or_advance_past_failed_action() -> None:
    topic_action = OperationAction(0, "topic:orders", "create")
    intent = replace(_intent(), actions=(topic_action,))
    checkpoint = replace(_checkpoint("old_closed"), resource_id="topic:orders", action="create")
    started = replace(_boundary(), resource_id="topic:orders", action="create")
    with pytest.raises(StateFormatError, match="evidenced Kafka Streams"):
        OperationControlState(ADDRESS, "in_progress", intent, (started, checkpoint), control_version=4)
    with pytest.raises(StateFormatError):
        _control((_boundary(), _boundary("completed", succeeded=False), _checkpoint("old_closed")))


def _local(tmp_path: Path):
    _state().save(local_state_path(tmp_path, environment="prod"))
    return make_deployment_state_service(tmp_path, project="payments", environment="prod", config=local_deployment_state_config())


def test_local_durable_checkpoints_survive_reopen_and_old_artifact_removal(tmp_path: Path) -> None:
    service = _local(tmp_path)
    with service.operation() as operation:
        snapshot = operation.observe()
        active = operation.begin_operation(snapshot, _intent(snapshot.state.state))
        for boundary in _boundaries()[:3]:
            active = operation.record_progress(active, boundary)
        with pytest.raises(StateBackendRecoveryRequiredError):
            operation.commit_operation(active, None)
    # No source file, old container or runtime provider is needed to recover
    # the complete preimage and the clean-close offset barrier from the journal.
    control = service.read_control().control
    assert control.progress[-1].kafka_streams_checkpoint.phase == "old_removed"
    assert control.intent.actions[0].kafka_streams_evidence == _evidence()
    assert control.progress[1].kafka_streams_checkpoint.progress.partitions[0].committed == 20
    with service.operation() as operation, pytest.raises(StateBackendRecoveryRequiredError):
        operation.ensure_ready(operation.observe())


def test_local_failed_checkpoint_persistence_retains_last_durable_prefix(tmp_path: Path, monkeypatch) -> None:
    service = _local(tmp_path)
    with service.operation() as operation:
        initial = operation.observe()
        active = operation.begin_operation(initial, _intent(initial.state.state))
        active = operation.record_progress(active, _boundary())
        with monkeypatch.context() as patch:
            patch.setattr(os, "replace", lambda *_args: (_ for _ in ()).throw(OSError("private-error")))
            with pytest.raises(StateBackendUnknownCommitError):
                operation.record_progress(active, _checkpoint("old_closed"))
        assert service.read_control().control.progress == (_boundary(),)


def test_local_success_requires_exact_candidate_ownership_and_cannot_use_legacy_clear(tmp_path) -> None:
    service = _local(tmp_path)
    with service.operation() as operation:
        initial = operation.observe()
        active = operation.begin_operation(initial, _intent(initial.state.state))
        for boundary in _boundaries():
            active = operation.record_progress(active, boundary)
        with pytest.raises(StateBackendRecoveryRequiredError, match="legacy clear"):
            operation.clear_operation(active)
        with pytest.raises(StateBackendRecoveryRequiredError, match="legacy clear"):
            operation.clear_operation(active.control)
        with pytest.raises(StateBackendConflictError, match="result"):
            operation.commit_operation(active, None)
        final = operation.commit_operation(active, _desired_state())
    assert final.control.control.status == "clear"
    assert final.state.state == _desired_state()


def test_local_unstarted_replacement_can_clear_only_through_before_mutation_boundary(tmp_path) -> None:
    service = _local(tmp_path)
    with service.operation() as operation:
        initial = operation.observe()
        active = operation.begin_operation(initial, _intent(initial.state.state))
        assert operation.clear_before_mutation(active).control.control.status == "clear"


def test_local_legacy_begin_cannot_bypass_runner_preimage_validation(tmp_path) -> None:
    service = _local(tmp_path)
    with service.operation() as operation:
        observation = operation.observe()
        bad = replace(_intent(), prior_state_checksum="sha256:" + "f" * 64)
        with pytest.raises(StateBackendConflictError):
            operation.begin_operation(observation.control, bad)
        assert operation.observe().control.control.status == "clear"


@pytest.mark.parametrize(("field", "value"), [
    ("artifact_checksum", "sha256:" + "f" * 64), ("physical_name", "other-app"),
    ("backend", "kafka-streams-docker:v1:" + "f" * 64), ("ownership", "adopted"),
])
def test_begin_checks_preimage_against_protected_state_before_writing_intent(tmp_path, field, value) -> None:
    service = _local(tmp_path)
    state = _state()
    state.resources[RESOURCE] = replace(state.resources[RESOURCE], **{field: value})
    state.save(local_state_path(tmp_path, environment="prod"))
    with service.operation() as operation:
        snapshot = operation.observe()
        with pytest.raises(StateBackendConflictError, match="protected ownership"):
            operation.begin_operation(snapshot, _intent(state))
        assert operation.observe().control.control.status == "clear"


def _postgres(monkeypatch):
    operation, database, owner, driver = _operation(monkeypatch)
    database.state = _state()
    database.state_revision = 1
    raw = json.dumps(database.state.to_dict(), sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    database.state_history = [(1, 1, state_checksum(database.state), raw, None)]
    return operation, database, owner, driver


def test_postgres_records_all_checkpoint_prefixes_and_completes_atomically(monkeypatch) -> None:
    operation, database, _owner, _driver = _postgres(monkeypatch)
    initial = operation.observe()
    active = operation.begin_operation(initial, _intent(initial.state.state))
    for boundary in _boundaries():
        active = operation.record_progress(active, boundary)
    with pytest.raises(StateBackendConflictError, match="result"):
        operation.commit_operation(active, None)
    operation.commit_operation(active, _desired_state())
    assert database.control.status == "clear"
    assert [kind for _index, kind, _payload in database.operation_history] == [
        "intent", "progress_started", "progress_checkpoint", "progress_checkpoint", "progress_checkpoint", "progress_completed", "succeeded",
    ]
    for index, _kind, raw in database.operation_history[:-1]:
        parsed = OperationControlState.from_dict(json.loads(raw), expected_address=database.address)
        assert parsed.control_version == 4
        assert parsed.progress == _boundaries()[:index]


def test_postgres_checkpoint_dml_failure_retains_prior_checkpoint(monkeypatch) -> None:
    operation, database, owner, _driver = _postgres(monkeypatch)
    initial = operation.observe()
    active = operation.begin_operation(initial, _intent(initial.state.state))
    active = operation.record_progress(active, _boundary())
    owner.fail_dml_pattern = 'INSERT INTO "streamt"."operation_history"'
    with pytest.raises(StateBackendUnavailableError):
        operation.record_progress(active, _checkpoint("old_closed"))
    owner.fail_dml_pattern = None
    assert database.control.progress == (_boundary(),)
    assert len(database.operation_history) == 2


def test_postgres_prior_ownership_mismatch_blocks_before_intent_or_history_write(monkeypatch) -> None:
    operation, database, _owner, _driver = _postgres(monkeypatch)
    database.state.resources[RESOURCE] = replace(database.state.resources[RESOURCE], artifact_checksum="sha256:" + "f" * 64)
    initial = operation.observe()
    with pytest.raises(StateBackendConflictError, match="protected ownership"):
        operation.begin_operation(initial, _intent(initial.state.state))
    assert database.control.status == "clear"
    assert not database.operation_history


@pytest.mark.parametrize("schema_version", [1, 2])
def test_postgres_restore_history_validation_accepts_complete_v4_and_rejects_tampering(monkeypatch, schema_version) -> None:
    operation, database, _owner, _driver = _postgres(monkeypatch)
    initial = operation.observe()
    active = operation.begin_operation(initial, _intent(initial.state.state))
    for boundary in _boundaries():
        active = operation.record_progress(active, boundary)
    operation.commit_operation(active, _desired_state())
    address = database.address
    raw_state = json.dumps(database.state.to_dict(), sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    raw_prior = json.dumps(_state().to_dict(), sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    address_fields = (address.namespace, address.project, address.environment)
    records = [(*address_fields, OPERATION, index, kind, raw, len(raw.encode())) for index, kind, raw in database.operation_history]

    def responder(query, _params):
        if '"operation_history"' in query:
            return records
        if '"state_history"' in query:
            return [
                (*address_fields, 1, 1, state_checksum(_state()), raw_prior, None, len(raw_prior.encode())),
                (*address_fields, 2, 2, state_checksum(database.state), raw_state, OPERATION, len(raw_state.encode())),
            ]
        if '"current_state"' in query:
            return [(*address_fields, 2, 2, state_checksum(database.state), raw_state, len(raw_state.encode()))]
        raise AssertionError(query)

    migrator = PrivatePostgresStateV2Migrator(dsn="dbname=unit", schema="streamt", lock_timeout_seconds=3, writer_role="writer")
    def validate():
        migrator._validate_all_durable_rows(_Cursor(responder), _FakeSql(), expected_store_id=database.store_id, source_schema_version=schema_version)
    validate()
    original = copy.deepcopy(records)
    raw = json.loads(records[2][-2])
    raw["progress"][-1]["kafka_streams_checkpoint"]["progress"]["partitions"][0]["committed"] = 9
    changed = json.dumps(raw, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    records[2] = (*records[2][:-2], changed, len(changed.encode()))
    with pytest.raises(StateBackendInvalidStateError):
        validate()
    records[:] = original
    records[2] = (*records[2][:5], "progress_completed", *records[2][6:])
    with pytest.raises(StateBackendInvalidStateError):
        validate()
    records[:] = original
    database.state.resources[RESOURCE] = _state().resources[RESOURCE]
    raw_state = json.dumps(database.state.to_dict(), sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    with pytest.raises(StateBackendInvalidStateError, match="ownership history"):
        validate()
