"""Contract tests for provider-neutral recovery evidence models."""

from __future__ import annotations

import json

import pytest

from streamt.deployer.recovery import (
    RecoveryResolutionRecord,
    RecoverySnapshotEvidence,
    RecoveryTargetEvidence,
    control_checksum,
)
from streamt.deployer.state import (
    LocalState,
    ManagedResourceRecord,
    StateFormatError,
    artifact_checksum,
    resource_id,
)
from streamt.deployer.state_backend import (
    ControlObservation,
    OperationAction,
    OperationControlState,
    OperationIntent,
    OperationSnapshot,
    StateAddress,
    StateObservation,
    StateRevision,
    StateStoreIdentity,
    state_checksum,
)

BLOCKED_OPERATION_ID = "00000000-0000-4000-8000-000000000001"
RECOVERY_OPERATION_ID = "00000000-0000-4000-8000-000000000002"
STORE_ID = "00000000-0000-4000-8000-000000000003"
FINGERPRINT = "sha256:" + "a" * 64


def _address() -> StateAddress:
    return StateAddress(namespace="platform", project="payments", environment="prod")


def _resource() -> str:
    return resource_id("payments", "prod", "topic", "payments_clean")


def _record(*, partitions: int = 3) -> ManagedResourceRecord:
    return ManagedResourceRecord(
        physical_name="payments.clean.v1",
        ownership="managed",
        artifact_checksum=artifact_checksum(
            {"name": "payments.clean.v1", "partitions": partitions}
        ),
        backend="direct-kafka",
    )


def _state() -> LocalState:
    return LocalState(
        project="payments",
        environment="prod",
        serial=1,
        resources={_resource(): _record()},
    )


def _action() -> OperationAction:
    return OperationAction(index=0, resource_id=_resource(), action="update")


def _control(*, actor: str = "operator") -> OperationControlState:
    state = _state()
    return OperationControlState(
        address=_address(),
        status="in_progress",
        intent=OperationIntent(
            operation_id=BLOCKED_OPERATION_ID,
            kind="apply",
            started_at="2026-09-02T12:00:00Z",
            actor=actor,
            prior_state_serial=state.serial,
            prior_state_checksum=state_checksum(state),
            reviewed_plan_checksum=None,
            actions=(_action(),),
        ),
    )


def _snapshot(*, actor: str = "operator") -> OperationSnapshot:
    state = _state()
    control = _control(actor=actor)
    return OperationSnapshot(
        state=StateObservation(
            store=StateStoreIdentity(backend="postgres", store_id=STORE_ID),
            address=_address(),
            state=state,
            revision=StateRevision(
                "postgresql://state-owner:do-not-serialize@db/private-state-revision"
            ),
        ),
        control=ControlObservation(
            control=control,
            revision=StateRevision("opaque-control-revision-do-not-serialize"),
        ),
    )


def test_target_evidence_is_strict_and_round_trips() -> None:
    target = RecoveryTargetEvidence(
        action=_action(),
        presence="present",
        accepted_as="candidate",
        fingerprint=FINGERPRINT,
    )

    assert RecoveryTargetEvidence.from_dict(target.to_dict()) == target
    with pytest.raises(StateFormatError, match="unknown field"):
        RecoveryTargetEvidence.from_dict({**target.to_dict(), "raw_response": {}})
    with pytest.raises(StateFormatError, match="canonical resource identity"):
        RecoveryTargetEvidence(
            action=OperationAction(index=0, resource_id="not-a-resource", action="update"),
            presence="present",
            accepted_as="candidate",
            fingerprint=FINGERPRINT,
        )
    with pytest.raises(StateFormatError, match="fingerprint"):
        RecoveryTargetEvidence(
            action=_action(),
            presence="present",
            accepted_as="candidate",
            fingerprint="provider-token",
        )


def test_snapshot_evidence_is_exact_but_excludes_provider_revisions() -> None:
    evidence = RecoverySnapshotEvidence.from_operation_snapshot(_snapshot())
    serialized = json.dumps(evidence.to_dict())

    assert evidence.blocked_operation_id == BLOCKED_OPERATION_ID
    assert evidence.state_checksum == state_checksum(_state())
    assert evidence.control_checksum == control_checksum(_control())
    assert "do-not-serialize" not in serialized
    assert "revision" not in evidence.to_dict()
    assert RecoverySnapshotEvidence.from_dict(evidence.to_dict()) == evidence


def test_snapshot_rejects_tampered_content_and_credentials() -> None:
    evidence = RecoverySnapshotEvidence.from_operation_snapshot(_snapshot())
    with pytest.raises(StateFormatError, match="state checksum"):
        RecoverySnapshotEvidence(
            store=evidence.store,
            address=evidence.address,
            state=evidence.state,
            state_checksum="sha256:" + "b" * 64,
            control=evidence.control,
            control_checksum=evidence.control_checksum,
        )
    with pytest.raises(StateFormatError, match="credential-like"):
        RecoverySnapshotEvidence.from_operation_snapshot(
            _snapshot(actor="password=operator-secret")
        )


def test_resolution_record_enforces_monotonic_state_semantics() -> None:
    prior_checksum = state_checksum(_state())
    result_state = LocalState(
        project="payments",
        environment="prod",
        serial=2,
        resources={_resource(): _record(partitions=6)},
    )
    record = RecoveryResolutionRecord(
        address=_address(),
        recovery_operation_id=RECOVERY_OPERATION_ID,
        blocked_operation_id=BLOCKED_OPERATION_ID,
        resolution="observed",
        resolved_at="2026-09-02T12:05:00Z",
        evidence_checksum=FINGERPRINT,
        prior_state_serial=1,
        prior_state_checksum=prior_checksum,
        result_state_serial=2,
        result_state_checksum=state_checksum(result_state),
        state_changed=True,
    )

    assert RecoveryResolutionRecord.from_dict(record.to_dict()) == record
    with pytest.raises(StateFormatError, match="only observed"):
        RecoveryResolutionRecord(
            **{
                **record.__dict__,
                "resolution": "rolled_back",
            }
        )
    with pytest.raises(StateFormatError, match="differ"):
        RecoveryResolutionRecord(
            **{
                **record.__dict__,
                "recovery_operation_id": BLOCKED_OPERATION_ID,
            }
        )


@pytest.mark.parametrize(
    "resolution",
    ["rolled_back", "abandoned_before_mutation"],
)
def test_non_mutating_resolution_record_retains_state(
    resolution: str,
) -> None:
    checksum = state_checksum(_state())
    record = RecoveryResolutionRecord(
        address=_address(),
        recovery_operation_id=RECOVERY_OPERATION_ID,
        blocked_operation_id=BLOCKED_OPERATION_ID,
        resolution=resolution,  # type: ignore[arg-type]
        resolved_at="2026-09-02T12:05:00Z",
        evidence_checksum=FINGERPRINT,
        prior_state_serial=1,
        prior_state_checksum=checksum,
        result_state_serial=1,
        result_state_checksum=checksum,
        state_changed=False,
    )

    assert record.result_state_serial == record.prior_state_serial
    with pytest.raises(StateFormatError, match="retain"):
        RecoveryResolutionRecord(
            **{
                **record.__dict__,
                "result_state_serial": 2,
            }
        )
