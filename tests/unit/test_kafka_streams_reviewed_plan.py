"""Opt-in reviewed format 6 without activating replacement or changing format 5."""

from __future__ import annotations

import copy
import hashlib
import json
import socket
import subprocess
from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest

from streamt.compiler.manifest import Manifest
from streamt.deployer.kafka import TopicChange
from streamt.deployer.kafka_streams import KafkaStreamsJobChange, KafkaStreamsJobState
from streamt.deployer.kafka_streams_evidence import (
    KafkaStreamsActionEvidence,
    KafkaStreamsArtifactSnapshot,
    KafkaStreamsPartitionEvidence,
)
from streamt.deployer.plan_file import (
    KAFKA_STREAMS_PLAN_FILE_VERSION,
    PLAN_FILE_VERSION,
    PlanFileError,
    ReviewedPlanFile,
    StalePlanError,
    StateReference,
    _checksum,
    _jsonable,
    _reviewed_checksum,
    canonical_json,
)
from streamt.deployer.planner import DeploymentPlan, OwnershipRequirement
from streamt.deployer.state import ManagedResourceRecord
from streamt.deployer.state_backend import (
    CURRENT_CONTROL_VERSION,
    OperationAction,
    OperationControlState,
    OperationIntent,
    state_checksum,
)
from tests.unit.test_kafka_streams_operation_evidence import (
    ADDRESS,
    BACKEND,
    OPERATION,
    RESOURCE,
    STAMP,
    TOKEN,
    _evidence,
    _progress,
)
from tests.unit.test_plan_file import (
    _connector_delete_actions,
    _deployment_plan,
    _gateway_delete_actions,
    _state_observation,
)
from tests.unit.test_plan_file import (
    _reviewed_plan as _legacy_plan,
)


def _actions(evidence: KafkaStreamsActionEvidence | None = None) -> tuple[OperationAction, ...]:
    return (OperationAction(0, RESOURCE, "update", kafka_streams_evidence=evidence or _evidence()),)


def _observation(evidence: KafkaStreamsActionEvidence | None = None):
    evidence = evidence or _evidence()
    return _state_observation(serial=1, resources={RESOURCE: ManagedResourceRecord(
        evidence.application_id, "managed", evidence.prior_artifact.checksum, BACKEND,
    )})


def _plan(evidence: KafkaStreamsActionEvidence | None = None, *, verified: bool = True) -> DeploymentPlan:
    evidence = evidence or _evidence()
    artifact = evidence.desired_artifact.artifact
    current = KafkaStreamsJobState(
        artifact.name, True, evidence.prior_container_id, "running",
        evidence.prior_artifact.checksum, evidence.prior_artifact.plan_hash, evidence.image_id,
        evidence.progress.input_topic_id, evidence.progress.output_topic_id, evidence.network_id,
    )
    change = KafkaStreamsJobChange(artifact.name, "update", current, artifact, {
        "application_id": evidence.application_id, "image_id": evidence.image_id,
        "topic_bindings": {
            artifact.plan["input_topic"]: evidence.progress.input_topic_id,
            artifact.plan["output_topic"]: evidence.progress.output_topic_id,
        },
        "initial_offset": artifact.initial_offset, "network_id": evidence.network_id,
        "desired_artifact_hash": evidence.desired_artifact.checksum,
    }, evidence.backend_identity)
    plan = DeploymentPlan(kafka_streams_changes=[change])
    if verified:
        # Only this provider-free wire fixture models a future verified plan.
        # Production DeploymentPlan still blocks all replacements; the negative
        # below ensures adding a file format cannot bypass that lifecycle gate.
        plan.safety_blockers.clear()
    return plan


def _review(
    evidence: KafkaStreamsActionEvidence | None = None, *, plan: DeploymentPlan | None = None,
    **changes: Any,
) -> ReviewedPlanFile:
    evidence = evidence or _evidence()
    values = {
        "project": "payments", "environment": "prod", "runtime": {},
        "state": StateReference.from_observation(_observation(evidence)),
        "actions": _actions(evidence),
    }
    values.update(changes)
    return ReviewedPlanFile.create(
        plan or _plan(evidence),
        Manifest(version="1.0", project_name="payments", compiled_at=STAMP, artifacts={
            "kafka_streams_jobs": [evidence.desired_artifact.to_dict()],
        }), **values,
    )


def _write(path: Path, data: dict[str, Any]) -> Path:
    path.write_text(json.dumps(data), encoding="utf-8")
    return path


def _resign(data: dict[str, Any]) -> dict[str, Any]:
    actions = tuple(OperationAction.from_dict(item, control_version=4) for item in data["actions"])
    data["checksum"] = _reviewed_checksum(
        {key: value for key, value in data.items() if key != "checksum"},
        format_version=6, actions=actions,
    )
    return data


def _token_schema_evidence(column: str = "token") -> KafkaStreamsActionEvidence:
    evidence = _evidence()
    snapshots = []
    for snapshot in (evidence.prior_artifact, evidence.desired_artifact):
        artifact = snapshot.to_dict()
        artifact["plan"]["schema"][column] = {"type": "STRING", "nullable": False}
        snapshots.append(KafkaStreamsArtifactSnapshot.from_dict(artifact))
    return replace(evidence, prior_artifact=snapshots[0], desired_artifact=snapshots[1])


def test_v6_round_trip_deterministic_full_typed_preimages_and_raw_volume_uuid(tmp_path: Path) -> None:
    first, second = _review(_token_schema_evidence()), _review(_token_schema_evidence())
    first_path, second_path = tmp_path / "one.json", tmp_path / "two.json"
    first.save(first_path)
    second.save(second_path)
    assert first.format_version == KAFKA_STREAMS_PLAN_FILE_VERSION == 6
    assert first_path.read_bytes() == second_path.read_bytes()
    loaded = ReviewedPlanFile.load(first_path)
    assert loaded == first
    assert loaded.actions == first.actions
    assert loaded.actions[0].kafka_streams_evidence.volume.token == TOKEN
    assert TOKEN in first_path.read_text()
    assert set(loaded.to_dict()["actions"][0]) == {
        "index", "resource_id", "action", "gateway_evidence", "connector_evidence", "kafka_streams_evidence",
    }
    assert loaded.to_dict()["actions"][0]["kafka_streams_evidence"]["prior_artifact"]["plan"]["schema"]["token"] == {"type": "STRING", "nullable": False}


@pytest.mark.parametrize("location", ["volume_uuid", "hidden_schema_key", "reviewed_offset", "predicate"])
def test_v6_checksum_binds_even_values_hidden_by_generic_redaction(tmp_path: Path, location) -> None:
    reviewed = _review(_token_schema_evidence())
    before = reviewed.to_dict()
    data = copy.deepcopy(before)
    evidence = data["actions"][0]["kafka_streams_evidence"]
    if location == "volume_uuid":
        evidence["volume"]["token"] = "00000000-0000-4000-8000-000000000009"
    elif location == "hidden_schema_key":
        for key in ("prior_artifact", "desired_artifact"):
            evidence[key]["plan"]["schema"]["token"]["nullable"] = True
    elif location == "reviewed_offset":
        evidence["progress"]["partitions"][0]["committed"] += 1
    else:
        evidence["desired_artifact"]["plan"]["predicates"][0]["value"] += 1
    if location in ("volume_uuid", "hidden_schema_key"):
        assert _checksum(before) == _checksum(data)  # Reproduces the dangerous generic path.
    with pytest.raises(PlanFileError, match="checksum mismatch"):
        ReviewedPlanFile.load(_write(tmp_path / "tampered.json", data))


def test_checksum_domain_and_full_progress_cannot_be_replaced_with_legacy_or_immutable_hash(tmp_path: Path) -> None:
    reviewed = _review()
    unsigned = {key: value for key, value in reviewed.to_dict().items() if key != "checksum"}
    current = replace(_evidence(), progress=_progress(committed=11))
    assert current.immutable_fingerprint == _evidence().immutable_fingerprint
    assert _review(current).checksum != reviewed.checksum
    for checksum in (_checksum(unsigned), _evidence().immutable_fingerprint):
        data = {**unsigned, "checksum": checksum}
        with pytest.raises(PlanFileError, match="checksum mismatch"):
            ReviewedPlanFile.load(_write(tmp_path / "wrong-domain.json", data))


@pytest.mark.parametrize(("path", "value"), [
    (("volume", "token"), "<redacted>"), (("volume", "token"), None),
    (("volume", "token"), "00000000-0000-0000-0000-000000000000"),
    (("volume", "credentials"), "password=do-not-echo"),
    (("desired_artifact", "client_properties"), "password=do-not-echo"),
    (("progress", "partitions", 0, "committed"), True),
    (("progress", "group_exists"), False), (("version",), 2),
])
def test_raw_digest_never_accepts_untyped_missing_redacted_or_credential_fields(tmp_path: Path, path, value) -> None:
    data = _review().to_dict()
    target = data["actions"][0]["kafka_streams_evidence"]
    for key in path[:-1]:
        target = target[key]
    target[path[-1]] = value
    with pytest.raises(PlanFileError, match="action is invalid") as captured:
        ReviewedPlanFile.load(_write(tmp_path / "invalid.json", data))
    assert "do-not-echo" not in str(captured.value)


@pytest.mark.parametrize("removed", ["kafka_streams_evidence", "gateway_evidence", "connector_evidence"])
def test_v6_wire_requires_the_exact_version_four_action_schema(tmp_path: Path, removed) -> None:
    data = _review().to_dict()
    del data["actions"][0][removed]
    with pytest.raises(PlanFileError, match="action is invalid"):
        ReviewedPlanFile.load(_write(tmp_path / "missing.json", data))


@pytest.mark.parametrize("version", [True, 6.0, "6", 7])
def test_version_six_is_explicit_integer_not_a_coerced_or_future_version(tmp_path: Path, version) -> None:
    data = _review().to_dict()
    data["format_version"] = version
    with pytest.raises(PlanFileError, match="Unsupported plan format version"):
        ReviewedPlanFile.load(_write(tmp_path / "version.json", data))


def test_format_five_cannot_smuggle_version_four_runner_evidence(tmp_path: Path) -> None:
    reviewed = _review()
    with pytest.raises(PlanFileError, match="format 6"):
        replace(reviewed, format_version=5)
    data = reviewed.to_dict()
    data["format_version"] = 5
    unsigned = {key: value for key, value in data.items() if key != "checksum"}
    data["checksum"] = _checksum(unsigned)
    with pytest.raises(PlanFileError, match="action is invalid"):
        ReviewedPlanFile.load(_write(tmp_path / "downgrade.json", data))


@pytest.mark.parametrize("changes", [
    {"selection": {}}, {"selection": {"select": "filtered"}},
    {"selection": {"target": "filtered"}}, {"offline": True}, {"state": None},
])
def test_replacement_requires_online_exact_state_and_full_unselected_project(changes) -> None:
    with pytest.raises(PlanFileError):
        _review(**changes)


def test_existing_runtime_replacement_blocker_cannot_be_bypassed_by_file_format() -> None:
    plan = _plan(verified=False)
    assert plan.is_apply_blocked
    with pytest.raises(PlanFileError, match="blockers"):
        _review(plan=plan)


@pytest.mark.parametrize("other", ["topic", "schema", "flink_job", "connector", "gateway_rule", "kafka_streams_job"])
@pytest.mark.parametrize("action", ["create", "update", "delete", "adopt"])
def test_no_second_resource_mutation_may_hide_behind_the_one_action_tuple(other, action) -> None:
    reviewed = _review()
    payload = copy.deepcopy(reviewed.plan)
    payload["resources"].append({"kind": other, "name": "another", "action": action, "changes": {}})
    with pytest.raises(PlanFileError, match="resource"):
        replace(reviewed, plan=payload)


@pytest.mark.parametrize("extra", [
    OperationAction(1, "streamt://payments/prod/topic/another", "create"),
    OperationAction(1, "streamt://payments/prod/topic/another", "adopt"),
])
def test_v6_rejects_extra_durable_actions_even_when_the_display_omits_them(extra) -> None:
    reviewed = _review()
    with pytest.raises(PlanFileError, match="one evidenced"):
        replace(reviewed, actions=(*reviewed.actions, extra))


@pytest.mark.parametrize("actions", [(), _connector_delete_actions(), _gateway_delete_actions()])
def test_v6_cannot_authorize_an_unrelated_kind_or_empty_action_list(actions) -> None:
    with pytest.raises(PlanFileError, match="one evidenced"):
        replace(_review(), actions=actions)


def test_unchanged_managed_and_external_declarations_remain_in_full_plan() -> None:
    plan = _plan()
    plan.topic_changes = [TopicChange("orders.input", "none"), TopicChange("orders.output", "none")]
    plan.ownership_requirements = [OwnershipRequirement(
        "streamt://payments/prod/topic/existing", "topic", "existing", "orders.input",
        "external", "none", "external", "Declaration only; no provider drift checks.",
    )]
    reviewed = _review(plan=plan)
    assert len(reviewed.plan["resources"]) == 3
    assert reviewed.bind_current_actions(plan, actions=_actions(), state_observation=_observation()) is reviewed.actions


@pytest.mark.parametrize(("path", "value"), [
    (("summary", "creates"), 1), (("summary", "updates"), 2), (("summary", "updates"), True),
    (("summary", "deletes"), 1), (("summary", "has_changes"), False),
    (("summary", "is_apply_blocked"), True), (("summary", "ownership_requirements"), 1),
    (("safety_blockers",), [{}]), (("connector_removal_assessments",), [{}]),
    (("gateway_removal_assessments",), [{}]),
    (("ownership_requirements",), [{"reason": "requires_adoption"}]),
    (("resources", 0, "name"), "other"), (("resources", 0, "kind"), "topic"),
    (("resources", 0, "action"), "none"), (("resources", 0, "changes", "current"), None),
    (("resources", 0, "changes", "current", "exists"), 1),
    (("resources", 0, "changes", "current", "container_id"), "f" * 64),
    (("resources", 0, "changes", "current", "status"), "closed"),
    (("resources", 0, "changes", "desired_artifact_hash"), "sha256:" + "f" * 64),
    (("resources", 0, "changes", "topic_bindings"), {}),
])
def test_integrity_checked_scope_and_display_cannot_disagree_with_authoritative_action(tmp_path: Path, path, value) -> None:
    data = _review().to_dict()
    target = data["plan"]
    for key in path[:-1]:
        target = target[key]
    target[path[-1]] = value
    # Even a fresh integrity checksum cannot widen the supported scope.
    with pytest.raises(PlanFileError):
        ReviewedPlanFile.load(_write(tmp_path / "scope.json", _resign(data)))


@pytest.mark.parametrize("committed", [10, 11, 50, 100])
def test_progress_can_advance_but_returned_authority_keeps_exact_reviewed_preimages(committed) -> None:
    reviewed = _review()
    current = replace(_evidence(), progress=_progress(committed=committed))
    approved = reviewed.bind_current_actions(_plan(current), actions=_actions(current), state_observation=_observation())
    assert approved is reviewed.actions
    assert approved[0].kafka_streams_evidence.progress.partitions[0].committed == 10
    assert reviewed.checksum == _review().checksum
    intent = OperationIntent(
        OPERATION, "apply", STAMP, "unit-test", 1, state_checksum(_observation().state),
        reviewed.checksum, approved,
    )
    control = OperationControlState(ADDRESS, "in_progress", intent)
    assert control.control_version == 4
    restored = OperationControlState.from_dict(control.to_dict(), expected_address=ADDRESS)
    assert restored.intent.actions == reviewed.actions
    assert restored.intent.reviewed_plan_checksum == reviewed.checksum


def test_normal_watermarks_and_retention_may_move_while_review_is_pending() -> None:
    current = replace(_evidence(), progress=replace(_progress(committed=50), partitions=(KafkaStreamsPartitionEvidence(0, 40, 150, 50),)))
    assert _review().bind_current_actions(_plan(current), actions=_actions(current), state_observation=_observation()) == _actions()


@pytest.mark.parametrize("changes", [
    {"cluster_id": "another"}, {"input_topic_id": "AAAAAAAAAAAAAAAAAAAAAw"},
    {"output_topic_id": "AAAAAAAAAAAAAAAAAAAAAw"}, {"active_members": 0}, {"active_members": 2},
    {"partitions": (KafkaStreamsPartitionEvidence(0, 0, 100, 9),)},
    {"partitions": (KafkaStreamsPartitionEvidence(0, 0, 99, 10),)},
    {"partitions": (KafkaStreamsPartitionEvidence(0, 0, 100, 10), KafkaStreamsPartitionEvidence(1, 0, 100, 10))},
])
def test_current_progress_regression_membership_or_topic_identity_is_stale(changes) -> None:
    current = replace(_evidence(), progress=replace(_progress(), **changes))
    with pytest.raises(StalePlanError, match="identity or progress"):
        _review().bind_current_actions(_plan(current), actions=_actions(current), state_observation=_observation())


@pytest.mark.parametrize("field", ["prior_container_id", "volume_uuid", "volume_created_at", "network_id", "predicate"])
def test_immutable_replacement_evidence_drift_is_never_normal_progress(field) -> None:
    current = _evidence()
    if field == "volume_uuid":
        current = replace(current, volume=replace(current.volume, token="00000000-0000-4000-8000-000000000009"))
    elif field == "volume_created_at":
        current = replace(current, volume=replace(current.volume, created_at="2026-09-05T12:00:01Z"))
    elif field == "predicate":
        artifact = current.desired_artifact.to_dict()
        artifact["plan"]["predicates"][0]["value"] = 90
        current = replace(current, desired_artifact=KafkaStreamsArtifactSnapshot.from_dict(artifact))
    else:
        current = replace(current, **{field: "f" * 64})
    with pytest.raises(StalePlanError, match="identity or progress"):
        _review().bind_current_actions(_plan(current), actions=_actions(current), state_observation=_observation())


@pytest.mark.parametrize("scope", ["mixed", "blocked", "none", "display"])
def test_fresh_plan_must_still_have_the_same_single_safe_update(scope) -> None:
    plan = _plan(verified=scope != "blocked")
    if scope == "mixed":
        plan.topic_changes.append(TopicChange("other", "create"))
    elif scope == "none":
        plan.kafka_streams_changes[0].action = "none"
    elif scope == "display":
        plan.kafka_streams_changes[0].current.container_id = "f" * 64
    with pytest.raises(StalePlanError, match="sole safe update"):
        _review().bind_current_actions(plan, actions=_actions(), state_observation=_observation())


def test_v6_still_enforces_exact_protected_state_and_live_impact_risk_diff() -> None:
    reviewed = _review()
    drifted_state = replace(_observation(), state=replace(_observation().state, serial=2))
    with pytest.raises(StalePlanError, match="state serial"):
        reviewed.bind_current_actions(_plan(), actions=_actions(), state_observation=drifted_state)
    payload = copy.deepcopy(reviewed.plan)
    payload["risk_summary"]["overall"] = "changed"
    with pytest.raises(StalePlanError, match="risk classification"):
        replace(reviewed, plan=payload).bind_current_actions(_plan(), actions=_actions(), state_observation=_observation())


def test_format_five_wire_bytes_and_hash_algorithm_are_unchanged(tmp_path: Path) -> None:
    reviewed = _legacy_plan()
    assert PLAN_FILE_VERSION == 5
    assert CURRENT_CONTROL_VERSION == 3
    assert reviewed.format_version == 5
    unsigned = {
        "kind": "streamt.reviewed-plan", "format_version": 5,
        "streamt_version": reviewed.streamt_version, "project": reviewed.project,
        "environment": reviewed.environment, "environment_fingerprint": reviewed.environment_fingerprint,
        "manifest_checksum": reviewed.manifest_checksum, "state": reviewed.state.to_dict(),
        "selection": reviewed.selection, "offline": reviewed.offline, "plan": reviewed.plan,
        "actions": [{"index": 0, "resource_id": "streamt://payments/prod/topic/payments_clean", "action": "create", "gateway_evidence": None, "connector_evidence": None}],
    }
    checksum = "sha256:" + hashlib.sha256(canonical_json(unsigned).encode()).hexdigest()
    expected = json.dumps({**unsigned, "checksum": checksum}, ensure_ascii=False, allow_nan=False, indent=2, sort_keys=True) + "\n"
    path = tmp_path / "legacy.json"
    reviewed.save(path)
    assert path.read_bytes() == expected.encode()
    assert ReviewedPlanFile.load(path) == reviewed


def test_generic_redaction_has_no_new_token_or_nested_schema_exemptions() -> None:
    assert _jsonable({"token": TOKEN, "schema": {"token": {"type": "STRING"}}}) == {
        "token": "<redacted>", "schema": {"token": "<redacted>"},
    }
    reviewed = _review(runtime={"kafka": {"sasl_password": "secret-one"}})
    rotated = _review(runtime={"kafka": {"sasl_password": "secret-two"}})
    assert reviewed.checksum == rotated.checksum
    assert "secret-one" not in json.dumps(reviewed.to_dict())


@pytest.mark.parametrize("column", ["token", "password", "credentials", "api_key"])
def test_every_hidden_business_schema_key_is_bound_without_a_generic_allowlist(tmp_path: Path, column) -> None:
    data = _review(_token_schema_evidence(column)).to_dict()
    before = copy.deepcopy(data)
    for artifact_key in ("prior_artifact", "desired_artifact"):
        data["actions"][0]["kafka_streams_evidence"][artifact_key]["plan"]["schema"][column]["nullable"] = True
    assert _checksum(before) == _checksum(data)
    with pytest.raises(PlanFileError, match="checksum mismatch"):
        ReviewedPlanFile.load(_write(tmp_path / "hidden-key.json", data))


@pytest.mark.parametrize("actions", [_connector_delete_actions(), _gateway_delete_actions()])
def test_new_binding_api_preserves_format_five_connector_and_gateway_authority(actions) -> None:
    legacy = _legacy_plan()
    legacy = replace(legacy, actions=actions)
    observed = _state_observation()
    assert legacy.bind_current_actions(_deployment_plan(), actions=actions, state_observation=observed) is actions
    evidence = actions[0].connector_evidence or actions[0].gateway_evidence
    changed = replace(evidence, current=replace(evidence.current, fingerprint="sha256:" + "9" * 64))
    key = "connector_evidence" if actions[0].connector_evidence is not None else "gateway_evidence"
    changed_actions = (replace(actions[0], **{key: changed}),)
    with pytest.raises(StalePlanError, match="ordered action identity or evidence"):
        legacy.bind_current_actions(_deployment_plan(), actions=changed_actions, state_observation=observed)


@pytest.mark.parametrize("selection", [{}, {"select": "filtered"}, {"target": "filtered"}])
def test_loaded_format_six_cannot_authorize_a_checksummed_selection(tmp_path: Path, selection) -> None:
    data = _review().to_dict()
    data["selection"] = selection
    with pytest.raises(PlanFileError, match="full unselected"):
        ReviewedPlanFile.load(_write(tmp_path / "selected.json", _resign(data)))


def test_duplicate_json_keys_remain_rejected_inside_typed_evidence(tmp_path: Path) -> None:
    path = tmp_path / "duplicate.json"
    content = json.dumps(_review().to_dict())
    content = content.replace('"token": "' + TOKEN + '"', '"token": "' + TOKEN + '", "token": "' + TOKEN + '"')
    path.write_text(content, encoding="utf-8")
    with pytest.raises(PlanFileError, match="duplicate field"):
        ReviewedPlanFile.load(path)


def test_binding_is_provider_free_and_does_not_mutate_inputs(monkeypatch) -> None:
    reviewed = _review()
    current = replace(_evidence(), progress=_progress(committed=11))
    plan, actions, observed = _plan(current), _actions(current), _observation()
    before = copy.deepcopy(reviewed.to_dict())

    def forbidden(*args, **kwargs):
        pytest.fail("reviewed action binding attempted provider or process access")

    monkeypatch.setattr(socket, "socket", forbidden)
    monkeypatch.setattr(socket, "create_connection", forbidden)
    monkeypatch.setattr(subprocess, "Popen", forbidden)
    monkeypatch.setattr(subprocess, "run", forbidden)
    bound = reviewed.bind_current_actions(plan, actions=actions, state_observation=observed)
    assert bound is reviewed.actions
    assert actions[0].kafka_streams_evidence.progress.partitions[0].committed == 11
    assert reviewed.to_dict() == before
