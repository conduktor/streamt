"""Tests for strict, integrity-checked recovery plan files."""

from __future__ import annotations

import hashlib
import json
import os
import stat
from collections.abc import Callable
from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest

from streamt.deployer.connect import managed_connector_absence_fingerprint
from streamt.deployer.gateway import managed_gateway_absence_fingerprint
from streamt.deployer.recovery import (
    RecoverySnapshotEvidence,
    RecoveryTargetEvidence,
)
from streamt.deployer.recovery_plan import (
    MAX_RECOVERY_PLAN_FILE_BYTES,
    RECOVERY_PLAN_FILE_KIND,
    RECOVERY_PLAN_FILE_VERSION,
    RecoveryPlanError,
    RecoveryPlanFile,
)
from streamt.deployer.state import (
    LocalState,
    ManagedResourceRecord,
    artifact_checksum,
    resource_id,
)
from streamt.deployer.state_backend import (
    ConnectorActionEvidence,
    ConnectorActionSurfaceEvidence,
    ControlObservation,
    GatewayActionEvidence,
    GatewayActionSurfaceEvidence,
    OperationAction,
    OperationControlState,
    OperationIntent,
    OperationProgress,
    OperationSnapshot,
    StateAddress,
    StateObservation,
    StateRevision,
    StateStoreIdentity,
    state_checksum,
)

BLOCKED_OPERATION_ID = "00000000-0000-4000-8000-000000000011"
RECOVERY_OPERATION_ID = "00000000-0000-4000-8000-000000000012"
STORE_ID = "00000000-0000-4000-8000-000000000013"
CHECKSUM = "sha256:" + "a" * 64
CONNECTOR_BACKEND = "kafka-connect:v1:primary:sha256:" + "c" * 64
CONNECTOR_NAME = "archive-orders-sink"
CONNECTOR_RESOURCE = "streamt://payments/prod/connector/archive_orders"
CONNECTOR_CURRENT_FINGERPRINT = "sha256:" + "d" * 64
CONNECTOR_ABSENCE_FINGERPRINT = managed_connector_absence_fingerprint(
    CONNECTOR_BACKEND,
    CONNECTOR_NAME,
)


def _address() -> StateAddress:
    return StateAddress(namespace="platform", project="payments", environment="prod")


def _resource() -> str:
    return resource_id("payments", "prod", "topic", "payments_clean")


def _record(*, partitions: int) -> ManagedResourceRecord:
    return ManagedResourceRecord(
        physical_name="payments.clean.v1",
        ownership="managed",
        artifact_checksum=artifact_checksum(
            {"name": "payments.clean.v1", "partitions": partitions}
        ),
        backend="direct-kafka",
    )


def _state(*, serial: int = 1, partitions: int = 3) -> LocalState:
    return LocalState(
        project="payments",
        environment="prod",
        serial=serial,
        resources={_resource(): _record(partitions=partitions)},
    )


def _action() -> OperationAction:
    return OperationAction(index=0, resource_id=_resource(), action="update")


def _snapshot(
    *,
    with_progress: bool = False,
    control_version: int = RECOVERY_PLAN_FILE_VERSION,
) -> RecoverySnapshotEvidence:
    state = _state()
    action = _action()
    intent = OperationIntent(
        operation_id=BLOCKED_OPERATION_ID,
        kind="apply",
        started_at="2026-09-02T12:00:00Z",
        actor="operator",
        prior_state_serial=state.serial,
        prior_state_checksum=state_checksum(state),
        reviewed_plan_checksum=None,
        actions=(action,),
    )
    progress = (
        (
            OperationProgress(
                operation_id=BLOCKED_OPERATION_ID,
                action_index=0,
                resource_id=action.resource_id,
                action=action.action,
                status="started",
                succeeded=None,
                recorded_at="2026-09-02T12:01:00Z",
            ),
        )
        if with_progress
        else ()
    )
    control = OperationControlState(
        address=_address(),
        status="in_progress",
        intent=intent,
        progress=progress,
        control_version=control_version,
    )
    return RecoverySnapshotEvidence.from_operation_snapshot(
        OperationSnapshot(
            state=StateObservation(
                store=StateStoreIdentity(backend="postgres", store_id=STORE_ID),
                address=_address(),
                state=state,
                revision=StateRevision("provider-state-revision"),
            ),
            control=ControlObservation(
                control=control,
                revision=StateRevision("provider-control-revision"),
            ),
        )
    )


def _gateway_snapshot(
    *,
    with_prior_ownership: bool = False,
    action_name: str = "create",
) -> RecoverySnapshotEvidence:
    backend_identity = "conduktor-gateway:v1:p:sha256:" + "1" * 64
    gateway_resource = "streamt://payments/prod/gateway_rule/orders_owner"
    prior_resources = (
        {
            gateway_resource: ManagedResourceRecord(
                physical_name="orders.public",
                ownership="managed",
                artifact_checksum=CHECKSUM,
                backend=backend_identity,
            )
        }
        if with_prior_ownership
        else {}
    )
    state = LocalState(
        project="payments",
        environment="prod",
        resources=prior_resources,
    )
    absent = GatewayActionSurfaceEvidence(
        exists=False,
        fingerprint=managed_gateway_absence_fingerprint(
            backend_identity,
            "orders_rule",
            "orders.public",
        ),
        managed_interceptor_count=0,
    )
    current = (
        absent
        if action_name == "create"
        else GatewayActionSurfaceEvidence(
            exists=True,
            fingerprint="sha256:" + "2" * 64,
            managed_interceptor_count=0 if action_name == "adopt" else 1,
        )
    )
    desired = (
        absent
        if action_name == "delete"
        else GatewayActionSurfaceEvidence(
            exists=True,
            fingerprint="sha256:" + "3" * 64,
            managed_interceptor_count=0 if action_name == "adopt" else 1,
        )
    )
    action = OperationAction(
        index=0,
        resource_id=gateway_resource,
        action=action_name,
        gateway_evidence=GatewayActionEvidence(
            version=1,
            backend_identity=backend_identity,
            rule_name="orders_rule",
            alias_name="orders.public",
            current=current,
            desired=desired,
        ),
    )
    intent = OperationIntent(
        operation_id=BLOCKED_OPERATION_ID,
        kind="adopt" if action_name == "adopt" else "apply",
        started_at="2026-09-02T12:00:00Z",
        actor="operator",
        prior_state_serial=state.serial,
        prior_state_checksum=state_checksum(state),
        reviewed_plan_checksum=None,
        actions=(action,),
    )
    control = OperationControlState(
        address=_address(),
        status="in_progress",
        intent=intent,
    )
    return RecoverySnapshotEvidence.from_operation_snapshot(
        OperationSnapshot(
            state=StateObservation(
                store=StateStoreIdentity(backend="postgres", store_id=STORE_ID),
                address=_address(),
                state=state,
                revision=StateRevision("provider-state-revision"),
            ),
            control=ControlObservation(
                control=control,
                revision=StateRevision("provider-control-revision"),
            ),
        )
    )


def _connector_snapshot() -> RecoverySnapshotEvidence:
    state = LocalState(
        project="payments",
        environment="prod",
        serial=1,
        resources={
            CONNECTOR_RESOURCE: ManagedResourceRecord(
                physical_name=CONNECTOR_NAME,
                ownership="managed",
                artifact_checksum=CHECKSUM,
                backend=CONNECTOR_BACKEND,
            )
        },
    )
    action = OperationAction(
        index=0,
        resource_id=CONNECTOR_RESOURCE,
        action="delete",
        connector_evidence=ConnectorActionEvidence(
            version=1,
            backend_identity=CONNECTOR_BACKEND,
            connector_name=CONNECTOR_NAME,
            prior_artifact_checksum=CHECKSUM,
            current=ConnectorActionSurfaceEvidence(
                exists=True,
                fingerprint=CONNECTOR_CURRENT_FINGERPRINT,
            ),
            desired=ConnectorActionSurfaceEvidence(
                exists=False,
                fingerprint=CONNECTOR_ABSENCE_FINGERPRINT,
            ),
        ),
    )
    control = OperationControlState(
        address=_address(),
        status="in_progress",
        intent=OperationIntent(
            operation_id=BLOCKED_OPERATION_ID,
            kind="apply",
            started_at="2026-09-02T12:00:00Z",
            actor="operator",
            prior_state_serial=state.serial,
            prior_state_checksum=state_checksum(state),
            reviewed_plan_checksum=None,
            actions=(action,),
        ),
    )
    return RecoverySnapshotEvidence.from_operation_snapshot(
        OperationSnapshot(
            state=StateObservation(
                store=StateStoreIdentity(backend="postgres", store_id=STORE_ID),
                address=_address(),
                state=state,
                revision=StateRevision("provider-state-revision"),
            ),
            control=ControlObservation(
                control=control,
                revision=StateRevision("provider-control-revision"),
            ),
        )
    )


def _target(*, accepted_as: str = "candidate") -> RecoveryTargetEvidence:
    return RecoveryTargetEvidence(
        action=_action(),
        presence="present",
        accepted_as=accepted_as,  # type: ignore[arg-type]
        fingerprint=CHECKSUM,
    )


def _observed_plan() -> RecoveryPlanFile:
    return RecoveryPlanFile.create(
        resolution="observed",
        recovery_operation_id=RECOVERY_OPERATION_ID,
        snapshot=_snapshot(with_progress=True),
        targets=(_target(),),
        candidate_state=_state(serial=2, partitions=6),
        environment_fingerprint=CHECKSUM,
        manifest_checksum="sha256:" + "b" * 64,
    )


def _resign(data: dict[str, object]) -> None:
    unsigned = {key: value for key, value in data.items() if key != "evidence_checksum"}
    data["evidence_checksum"] = _object_checksum(unsigned)


def _object_checksum(value: object) -> str:
    canonical = json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    return "sha256:" + hashlib.sha256(
        canonical.encode("utf-8")
    ).hexdigest()


def test_observed_plan_is_deterministic_exact_and_omits_provider_revisions(
    tmp_path: Path,
) -> None:
    first = _observed_plan()
    second = _observed_plan()
    first_path = tmp_path / "first.recovery.json"
    second_path = tmp_path / "second.recovery.json"

    first.save(first_path)
    second.save(second_path)

    assert RECOVERY_PLAN_FILE_KIND == "streamt.recovery-plan"
    assert RECOVERY_PLAN_FILE_VERSION == 3
    assert first.evidence_checksum == second.evidence_checksum
    assert first_path.read_bytes() == second_path.read_bytes()
    serialized = first_path.read_text(encoding="utf-8")
    assert "provider-state-revision" not in serialized
    assert "provider-control-revision" not in serialized
    assert "dsn_env" not in serialized
    assert "writer_role" not in serialized
    assert RecoveryPlanFile.load(first_path) == first


def test_v1_recovery_plan_loads_and_reserializes_exactly(tmp_path: Path) -> None:
    original: dict[str, Any] = _observed_plan().to_dict()
    original["format_version"] = 1
    control = original["snapshot"]["control"]
    control["control_version"] = 1
    for action in control["intent"]["actions"]:
        action.pop("gateway_evidence")
        action.pop("connector_evidence")
    for target in original["targets"]:
        target["action"].pop("gateway_evidence")
        target["action"].pop("connector_evidence")
    original["snapshot"]["control_checksum"] = _object_checksum(control)
    _resign(original)
    path = tmp_path / "v1.recovery.json"
    path.write_text(json.dumps(original), encoding="utf-8")

    loaded = RecoveryPlanFile.load(path)

    assert loaded.format_version == 1
    assert loaded.snapshot.control.control_version == 1
    assert loaded.to_dict() == original


def test_v2_recovery_plan_preserves_v1_snapshot_and_uses_v2_targets(
    tmp_path: Path,
) -> None:
    current = RecoveryPlanFile.create(
        resolution="observed",
        recovery_operation_id=RECOVERY_OPERATION_ID,
        snapshot=_snapshot(with_progress=True, control_version=1),
        targets=(_target(),),
        candidate_state=_state(serial=2, partitions=6),
        environment_fingerprint=CHECKSUM,
        manifest_checksum="sha256:" + "b" * 64,
    )
    unsigned = replace(current, format_version=2, evidence_checksum="")
    plan = replace(
        unsigned,
        evidence_checksum=_object_checksum(unsigned._unsigned_dict()),
    )

    payload: dict[str, Any] = plan.to_dict()

    assert payload["format_version"] == 2
    assert payload["snapshot"]["control"]["control_version"] == 1
    assert set(payload["snapshot"]["control"]["intent"]["actions"][0]) == {
        "index",
        "resource_id",
        "action",
    }
    assert payload["targets"][0]["action"]["gateway_evidence"] is None
    assert "connector_evidence" not in payload["targets"][0]["action"]
    path = tmp_path / "v2.recovery.json"
    plan.save(path)
    assert RecoveryPlanFile.load(path).to_dict() == payload


def test_v3_recovery_plan_preserves_v2_snapshot_and_uses_v3_targets(
    tmp_path: Path,
) -> None:
    plan = RecoveryPlanFile.create(
        resolution="observed",
        recovery_operation_id=RECOVERY_OPERATION_ID,
        snapshot=_snapshot(with_progress=True, control_version=2),
        targets=(_target(),),
        candidate_state=_state(serial=2, partitions=6),
        environment_fingerprint=CHECKSUM,
        manifest_checksum="sha256:" + "b" * 64,
    )
    payload: dict[str, Any] = plan.to_dict()

    assert payload["format_version"] == 3
    assert payload["snapshot"]["control"]["control_version"] == 2
    assert set(payload["snapshot"]["control"]["intent"]["actions"][0]) == {
        "index",
        "resource_id",
        "action",
        "gateway_evidence",
    }
    assert set(payload["targets"][0]["action"]) == {
        "index",
        "resource_id",
        "action",
        "gateway_evidence",
        "connector_evidence",
    }
    path = tmp_path / "v3-with-v2-snapshot.recovery.json"
    plan.save(path)
    assert RecoveryPlanFile.load(path).to_dict() == payload


def test_v2_recovery_plan_rejects_v3_snapshot_at_construction_and_load(
    tmp_path: Path,
) -> None:
    current = _observed_plan()
    with pytest.raises(RecoveryPlanError, match="format version 2 requires control"):
        replace(current, format_version=2, evidence_checksum="")

    payload: dict[str, Any] = current.to_dict()
    payload["format_version"] = 2
    for target in payload["targets"]:
        target["action"].pop("connector_evidence")
    _resign(payload)
    path = tmp_path / "v2-smuggled-v3-snapshot.json"
    path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(RecoveryPlanError, match="format version 2 requires control"):
        RecoveryPlanFile.load(path)


def test_v3_abandoned_connector_recovery_round_trips_but_live_resolutions_fail_closed(
    tmp_path: Path,
) -> None:
    snapshot = _connector_snapshot()
    action = snapshot.control.intent.actions[0]  # type: ignore[union-attr]
    abandoned = RecoveryPlanFile.create(
        resolution="abandoned_before_mutation",
        recovery_operation_id=RECOVERY_OPERATION_ID,
        snapshot=snapshot,
        targets=(),
    )
    path = tmp_path / "abandoned-connector.recovery.json"
    abandoned.save(path)

    assert abandoned.format_version == 3
    assert RecoveryPlanFile.load(path) == abandoned

    live_cases = (
        (
            "rolled_back",
            RecoveryTargetEvidence(
                action=action,
                presence="present",
                accepted_as="prior",
                fingerprint=CONNECTOR_CURRENT_FINGERPRINT,
            ),
            None,
        ),
        (
            "observed",
            RecoveryTargetEvidence(
                action=action,
                presence="absent",
                accepted_as="candidate",
                fingerprint=CONNECTOR_ABSENCE_FINGERPRINT,
            ),
            LocalState(project="payments", environment="prod", serial=2),
        ),
    )
    for resolution, target, candidate in live_cases:
        with pytest.raises(RecoveryPlanError, match="not available in this build"):
            RecoveryPlanFile.create(
                resolution=resolution,  # type: ignore[arg-type]
                recovery_operation_id=RECOVERY_OPERATION_ID,
                snapshot=snapshot,
                targets=(target,),
                candidate_state=candidate,
                environment_fingerprint=CHECKSUM,
                manifest_checksum=CHECKSUM,
            )

        payload = abandoned.to_dict()
        payload["resolution"] = resolution
        payload["targets"] = [target.to_dict()]
        payload["candidate_state"] = None if candidate is None else candidate.to_dict()
        payload["environment_fingerprint"] = CHECKSUM
        payload["manifest_checksum"] = CHECKSUM
        _resign(payload)
        rejected = tmp_path / f"{resolution}-connector.recovery.json"
        rejected.write_text(json.dumps(payload), encoding="utf-8")
        with pytest.raises(RecoveryPlanError, match="not available in this build"):
            RecoveryPlanFile.load(rejected)


def test_v2_recovery_checksum_covers_action_evidence(tmp_path: Path) -> None:
    plan = RecoveryPlanFile.create(
        resolution="abandoned_before_mutation",
        recovery_operation_id=RECOVERY_OPERATION_ID,
        snapshot=_gateway_snapshot(),
        targets=(),
    )
    data: dict[str, Any] = plan.to_dict()
    action = data["snapshot"]["control"]["intent"]["actions"][0]
    assert action["gateway_evidence"]["rule_name"] == "orders_rule"
    action["gateway_evidence"]["desired"]["fingerprint"] = "sha256:" + "4" * 64
    path = tmp_path / "tampered-evidence.json"
    path.write_text(json.dumps(data), encoding="utf-8")

    with pytest.raises(RecoveryPlanError, match="checksum mismatch"):
        RecoveryPlanFile.load(path)


def test_v1_recovery_plan_rejects_gateway_evidence(tmp_path: Path) -> None:
    data: dict[str, Any] = _observed_plan().to_dict()
    data["format_version"] = 1
    data["snapshot"]["control"]["control_version"] = 1
    for action in data["snapshot"]["control"]["intent"]["actions"]:
        action.pop("gateway_evidence")
        action.pop("connector_evidence")
    data["snapshot"]["control_checksum"] = _object_checksum(data["snapshot"]["control"])
    # The target retains its v2-only member, proving v1 cannot acquire even null.
    _resign(data)
    path = tmp_path / "v1-with-evidence.json"
    path.write_text(json.dumps(data), encoding="utf-8")

    with pytest.raises(RecoveryPlanError, match="invalid"):
        RecoveryPlanFile.load(path)


def test_abandoned_with_nonempty_intent_requires_no_live_evidence() -> None:
    plan = RecoveryPlanFile.create(
        resolution="abandoned_before_mutation",
        recovery_operation_id=RECOVERY_OPERATION_ID,
        snapshot=_snapshot(),
        targets=(),
    )

    assert plan.snapshot.control.intent is not None
    assert plan.snapshot.control.intent.actions == (_action(),)
    assert plan.targets == ()
    assert plan.environment_fingerprint is None
    assert plan.candidate_state is None
    with pytest.raises(RecoveryPlanError, match="empty durable progress"):
        RecoveryPlanFile.create(
            resolution="abandoned_before_mutation",
            recovery_operation_id=RECOVERY_OPERATION_ID,
            snapshot=_snapshot(with_progress=True),
            targets=(),
        )


def test_rolled_back_requires_prior_classification_and_no_candidate() -> None:
    plan = RecoveryPlanFile.create(
        resolution="rolled_back",
        recovery_operation_id=RECOVERY_OPERATION_ID,
        snapshot=_snapshot(with_progress=True),
        targets=(_target(accepted_as="prior"),),
        environment_fingerprint=CHECKSUM,
        manifest_checksum=CHECKSUM,
    )

    assert plan.candidate_state is None
    with pytest.raises(RecoveryPlanError, match="accepted as prior"):
        RecoveryPlanFile.create(
            resolution="rolled_back",
            recovery_operation_id=RECOVERY_OPERATION_ID,
            snapshot=_snapshot(with_progress=True),
            targets=(_target(),),
            environment_fingerprint=CHECKSUM,
            manifest_checksum=CHECKSUM,
        )


def test_rolled_back_gateway_recreate_accepts_absent_current_with_prior_ownership() -> None:
    snapshot = _gateway_snapshot(with_prior_ownership=True)
    intent = snapshot.control.intent
    assert intent is not None
    action = intent.actions[0]
    gateway_evidence = action.gateway_evidence
    assert gateway_evidence is not None
    target = RecoveryTargetEvidence(
        action=action,
        presence="absent",
        accepted_as="prior",
        fingerprint=gateway_evidence.current.fingerprint,
    )

    plan = RecoveryPlanFile.create(
        resolution="rolled_back",
        recovery_operation_id=RECOVERY_OPERATION_ID,
        snapshot=snapshot,
        targets=(target,),
        environment_fingerprint=CHECKSUM,
        manifest_checksum=CHECKSUM,
    )

    assert action.resource_id in plan.snapshot.state.resources
    assert plan.targets[0].presence == "absent"
    assert plan.candidate_state is None


def test_rolled_back_gateway_adopt_requires_and_preserves_prior_absence() -> None:
    snapshot = _gateway_snapshot(action_name="adopt")
    intent = snapshot.control.intent
    assert intent is not None
    action = intent.actions[0]
    gateway_evidence = action.gateway_evidence
    assert gateway_evidence is not None
    target = RecoveryTargetEvidence(
        action=action,
        presence="present",
        accepted_as="prior",
        fingerprint=gateway_evidence.current.fingerprint,
    )

    plan = RecoveryPlanFile.create(
        resolution="rolled_back",
        recovery_operation_id=RECOVERY_OPERATION_ID,
        snapshot=snapshot,
        targets=(target,),
        environment_fingerprint=CHECKSUM,
        manifest_checksum=CHECKSUM,
    )

    assert action.resource_id not in plan.snapshot.state.resources
    assert plan.candidate_state is None

    with pytest.raises(RecoveryPlanError, match="absent prior ownership"):
        RecoveryPlanFile.create(
            resolution="rolled_back",
            recovery_operation_id=RECOVERY_OPERATION_ID,
            snapshot=_gateway_snapshot(
                with_prior_ownership=True,
                action_name="adopt",
            ),
            targets=(target,),
            environment_fingerprint=CHECKSUM,
            manifest_checksum=CHECKSUM,
        )


def test_observed_candidate_is_authoritative_and_preserves_unrelated_state() -> None:
    plan = _observed_plan()

    assert plan.candidate_state is not None
    assert plan.candidate_state.serial == plan.snapshot.state.serial + 1
    with pytest.raises(RecoveryPlanError, match="presence"):
        RecoveryPlanFile.create(
            resolution="observed",
            recovery_operation_id=RECOVERY_OPERATION_ID,
            snapshot=_snapshot(with_progress=True),
            targets=(
                RecoveryTargetEvidence(
                    action=_action(),
                    presence="absent",
                    accepted_as="candidate",
                    fingerprint=CHECKSUM,
                ),
            ),
            candidate_state=_state(serial=2, partitions=6),
            environment_fingerprint=CHECKSUM,
            manifest_checksum=CHECKSUM,
        )
    with pytest.raises(RecoveryPlanError, match="exactly one evidence target"):
        RecoveryPlanFile.create(
            resolution="observed",
            recovery_operation_id=RECOVERY_OPERATION_ID,
            snapshot=_snapshot(with_progress=True),
            targets=(),
            candidate_state=_state(serial=2, partitions=6),
            environment_fingerprint=CHECKSUM,
            manifest_checksum=CHECKSUM,
        )

    unrelated_id = resource_id("payments", "prod", "topic", "unrelated")
    with pytest.raises(RecoveryPlanError, match="outside the blocked intent"):
        RecoveryPlanFile.create(
            resolution="observed",
            recovery_operation_id=RECOVERY_OPERATION_ID,
            snapshot=_snapshot(with_progress=True),
            targets=(_target(),),
            candidate_state=LocalState(
                project="payments",
                environment="prod",
                serial=2,
                resources={
                    _resource(): _record(partitions=6),
                    unrelated_id: ManagedResourceRecord(
                        physical_name="unrelated.v1",
                        ownership="managed",
                        artifact_checksum=CHECKSUM,
                        backend="direct-kafka",
                    ),
                },
            ),
            environment_fingerprint=CHECKSUM,
            manifest_checksum=CHECKSUM,
        )


def test_observed_gateway_prior_retains_ownership_while_provider_is_absent() -> None:
    snapshot = _gateway_snapshot(with_prior_ownership=True)
    intent = snapshot.control.intent
    assert intent is not None
    action = intent.actions[0]
    gateway_evidence = action.gateway_evidence
    assert gateway_evidence is not None
    target = RecoveryTargetEvidence(
        action=action,
        presence="absent",
        accepted_as="prior",
        fingerprint=gateway_evidence.current.fingerprint,
    )

    plan = RecoveryPlanFile.create(
        resolution="observed",
        recovery_operation_id=RECOVERY_OPERATION_ID,
        snapshot=snapshot,
        targets=(target,),
        candidate_state=snapshot.state,
        environment_fingerprint=CHECKSUM,
        manifest_checksum=CHECKSUM,
    )

    assert plan.candidate_state == snapshot.state
    assert action.resource_id in plan.candidate_state.resources
    assert plan.targets[0].presence == "absent"


def test_observed_gateway_candidate_requires_exact_desired_ownership() -> None:
    snapshot = _gateway_snapshot()
    intent = snapshot.control.intent
    assert intent is not None
    action = intent.actions[0]
    gateway_evidence = action.gateway_evidence
    assert gateway_evidence is not None
    target = RecoveryTargetEvidence(
        action=action,
        presence="present",
        accepted_as="candidate",
        fingerprint=gateway_evidence.desired.fingerprint,
    )

    with pytest.raises(RecoveryPlanError, match="exact desired ownership"):
        RecoveryPlanFile.create(
            resolution="observed",
            recovery_operation_id=RECOVERY_OPERATION_ID,
            snapshot=snapshot,
            targets=(target,),
            candidate_state=snapshot.state,
            environment_fingerprint=CHECKSUM,
            manifest_checksum=CHECKSUM,
        )


def test_observed_gateway_adopt_candidate_requires_exact_adopted_ownership() -> None:
    snapshot = _gateway_snapshot(action_name="adopt")
    intent = snapshot.control.intent
    assert intent is not None
    action = intent.actions[0]
    gateway_evidence = action.gateway_evidence
    assert gateway_evidence is not None
    target = RecoveryTargetEvidence(
        action=action,
        presence="present",
        accepted_as="candidate",
        fingerprint=gateway_evidence.current.fingerprint,
    )
    candidate_record = ManagedResourceRecord(
        physical_name=gateway_evidence.alias_name,
        ownership="adopted",
        artifact_checksum=CHECKSUM,
        backend=gateway_evidence.backend_identity,
    )
    candidate = LocalState(
        project="payments",
        environment="prod",
        serial=snapshot.state.serial + 1,
        resources={action.resource_id: candidate_record},
    )

    plan = RecoveryPlanFile.create(
        resolution="observed",
        recovery_operation_id=RECOVERY_OPERATION_ID,
        snapshot=snapshot,
        targets=(target,),
        candidate_state=candidate,
        environment_fingerprint=CHECKSUM,
        manifest_checksum=CHECKSUM,
    )

    assert plan.candidate_state == candidate
    with pytest.raises(RecoveryPlanError, match="adopted ownership"):
        RecoveryPlanFile.create(
            resolution="observed",
            recovery_operation_id=RECOVERY_OPERATION_ID,
            snapshot=snapshot,
            targets=(target,),
            candidate_state=LocalState(
                project="payments",
                environment="prod",
                serial=snapshot.state.serial + 1,
                resources={
                    action.resource_id: ManagedResourceRecord(
                        physical_name=gateway_evidence.alias_name,
                        ownership="managed",
                        artifact_checksum=CHECKSUM,
                        backend=gateway_evidence.backend_identity,
                    )
                },
            ),
            environment_fingerprint=CHECKSUM,
            manifest_checksum=CHECKSUM,
        )


def test_gateway_update_requires_exact_prior_ownership_in_reviewed_plan() -> None:
    snapshot = _gateway_snapshot(action_name="update")
    intent = snapshot.control.intent
    assert intent is not None
    action = intent.actions[0]
    gateway_evidence = action.gateway_evidence
    assert gateway_evidence is not None
    target = RecoveryTargetEvidence(
        action=action,
        presence="present",
        accepted_as="prior",
        fingerprint=gateway_evidence.current.fingerprint,
    )

    with pytest.raises(RecoveryPlanError, match="exact prior ownership"):
        RecoveryPlanFile.create(
            resolution="rolled_back",
            recovery_operation_id=RECOVERY_OPERATION_ID,
            snapshot=snapshot,
            targets=(target,),
            environment_fingerprint=CHECKSUM,
            manifest_checksum=CHECKSUM,
        )


def test_observed_gateway_delete_candidate_must_remove_ownership() -> None:
    snapshot = _gateway_snapshot(
        with_prior_ownership=True,
        action_name="delete",
    )
    intent = snapshot.control.intent
    assert intent is not None
    action = intent.actions[0]
    gateway_evidence = action.gateway_evidence
    assert gateway_evidence is not None
    target = RecoveryTargetEvidence(
        action=action,
        presence="absent",
        accepted_as="candidate",
        fingerprint=gateway_evidence.desired.fingerprint,
    )

    with pytest.raises(RecoveryPlanError, match="must remove"):
        RecoveryPlanFile.create(
            resolution="observed",
            recovery_operation_id=RECOVERY_OPERATION_ID,
            snapshot=snapshot,
            targets=(target,),
            candidate_state=snapshot.state,
            environment_fingerprint=CHECKSUM,
            manifest_checksum=CHECKSUM,
        )


def test_observed_plan_accepts_mixed_prior_and_candidate_targets() -> None:
    prior_plan = RecoveryPlanFile.create(
        resolution="observed",
        recovery_operation_id=RECOVERY_OPERATION_ID,
        snapshot=_snapshot(with_progress=True),
        targets=(_target(accepted_as="prior"),),
        candidate_state=_state(),
        environment_fingerprint=CHECKSUM,
        manifest_checksum=CHECKSUM,
    )

    assert prior_plan.candidate_state == prior_plan.snapshot.state
    assert prior_plan.targets[0].accepted_as == "prior"

    with pytest.raises(RecoveryPlanError, match="retain its prior ownership record"):
        RecoveryPlanFile.create(
            resolution="observed",
            recovery_operation_id=RECOVERY_OPERATION_ID,
            snapshot=_snapshot(with_progress=True),
            targets=(_target(accepted_as="prior"),),
            candidate_state=_state(serial=2, partitions=6),
            environment_fingerprint=CHECKSUM,
            manifest_checksum=CHECKSUM,
        )


def test_observed_plan_rejects_credential_like_candidate_text() -> None:
    with pytest.raises(RecoveryPlanError, match="unsafe text"):
        RecoveryPlanFile.create(
            resolution="observed",
            recovery_operation_id=RECOVERY_OPERATION_ID,
            snapshot=_snapshot(with_progress=True),
            targets=(_target(),),
            candidate_state=LocalState(
                project="payments",
                environment="prod",
                serial=2,
                resources={
                    _resource(): ManagedResourceRecord(
                        physical_name="postgresql://owner:password@db/state",
                        ownership="managed",
                        artifact_checksum=CHECKSUM,
                        backend="direct-kafka",
                    )
                },
            ),
            environment_fingerprint=CHECKSUM,
            manifest_checksum=CHECKSUM,
        )


def test_plan_load_rejects_tamper_unknown_duplicate_and_nonfinite(
    tmp_path: Path,
) -> None:
    original = _observed_plan().to_dict()

    tampered = dict(original)
    tampered["resolution"] = "rolled_back"
    tampered_path = tmp_path / "tampered.json"
    tampered_path.write_text(json.dumps(tampered), encoding="utf-8")
    with pytest.raises(RecoveryPlanError, match="checksum mismatch"):
        RecoveryPlanFile.load(tampered_path)

    unknown = dict(original)
    unknown["owner_dsn"] = "secret"
    _resign(unknown)
    unknown_path = tmp_path / "unknown.json"
    unknown_path.write_text(json.dumps(unknown), encoding="utf-8")
    with pytest.raises(RecoveryPlanError, match="unknown field"):
        RecoveryPlanFile.load(unknown_path)

    duplicate_path = tmp_path / "duplicate.json"
    duplicate_path.write_text('{"kind":"a","kind":"b"}', encoding="utf-8")
    with pytest.raises(RecoveryPlanError, match="duplicate field"):
        RecoveryPlanFile.load(duplicate_path)

    nonfinite_path = tmp_path / "nonfinite.json"
    nonfinite_path.write_text('{"value":NaN}', encoding="utf-8")
    with pytest.raises(RecoveryPlanError, match="non-finite"):
        RecoveryPlanFile.load(nonfinite_path)


def test_plan_load_revalidates_nested_uuid_address_state_and_control(
    tmp_path: Path,
) -> None:
    mutations: tuple[tuple[str, Callable[[dict[str, Any]], None]], ...] = (
        ("uuid", lambda data: data.__setitem__("blocked_operation_id", "not-a-uuid")),
        (
            "address",
            lambda data: data["snapshot"].__setitem__(
                "address", "streamt-state://platform/other/prod"
            ),
        ),
        (
            "state",
            lambda data: data["snapshot"]["state"].__setitem__(
                "serial", -1
            ),
        ),
        (
            "control",
            lambda data: data["snapshot"]["control"].__setitem__(
                "status", "clear"
            ),
        ),
    )
    for label, mutate in mutations:
        data: dict[str, Any] = _observed_plan().to_dict()
        mutate(data)
        _resign(data)
        path = tmp_path / f"{label}.json"
        path.write_text(json.dumps(data), encoding="utf-8")
        with pytest.raises(RecoveryPlanError):
            RecoveryPlanFile.load(path)


def test_plan_write_is_0600_atomic_no_overwrite_and_no_symlink(
    tmp_path: Path,
) -> None:
    plan = _observed_plan()
    path = tmp_path / "recovery.json"
    plan.save(path)

    assert stat.S_IMODE(path.stat().st_mode) == 0o600
    assert not list(tmp_path.glob(".*.tmp"))
    with pytest.raises(RecoveryPlanError, match="already exists"):
        plan.save(path)

    target = tmp_path / "target.json"
    target.write_text("{}", encoding="utf-8")
    link = tmp_path / "linked.json"
    link.symlink_to(target)
    with pytest.raises(RecoveryPlanError, match="already exists"):
        plan.save(link)
    with pytest.raises(RecoveryPlanError, match="Cannot read"):
        RecoveryPlanFile.load(link)


def test_plan_rejects_oversize_before_json_parsing(tmp_path: Path) -> None:
    path = tmp_path / "large.json"
    file_descriptor = os.open(path, os.O_CREAT | os.O_WRONLY, 0o600)
    try:
        os.write(file_descriptor, b"{" + b" " * MAX_RECOVERY_PLAN_FILE_BYTES)
    finally:
        os.close(file_descriptor)

    with pytest.raises(RecoveryPlanError, match="10 MiB"):
        RecoveryPlanFile.load(path)


def test_resolution_record_is_derived_from_exact_plan() -> None:
    plan = _observed_plan()

    record = plan.make_resolution_record(resolved_at="2026-09-02T13:00:00Z")

    assert record.recovery_operation_id == plan.recovery_operation_id
    assert record.blocked_operation_id == plan.blocked_operation_id
    assert record.evidence_checksum == plan.evidence_checksum
    assert record.prior_state_serial == 1
    assert record.result_state_serial == 2
    assert record.state_changed is True
