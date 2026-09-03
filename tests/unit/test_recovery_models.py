"""Contract tests for provider-neutral recovery evidence models."""

from __future__ import annotations

import json
from dataclasses import replace

import pytest

from streamt.deployer.connect import managed_connector_absence_fingerprint
from streamt.deployer.gateway import (
    GatewayBackendBinding,
    managed_gateway_absence_fingerprint,
)
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
    ConnectorActionEvidence,
    ConnectorActionSurfaceEvidence,
    ControlObservation,
    GatewayActionEvidence,
    GatewayActionSurfaceEvidence,
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


def test_gateway_target_evidence_is_bound_to_the_accepted_surface() -> None:
    binding = GatewayBackendBinding.from_endpoint(
        "https://gateway.example.test",
        virtual_cluster="payments-prod",
    )
    absent_fingerprint = managed_gateway_absence_fingerprint(
        binding.backend_identity,
        "orders_rule",
        "orders.public",
    )
    desired_fingerprint = "sha256:" + "b" * 64
    action = OperationAction(
        index=0,
        resource_id=resource_id(
            "payments",
            "prod",
            "gateway_rule",
            "orders_owner",
        ),
        action="create",
        gateway_evidence=GatewayActionEvidence(
            version=1,
            backend_identity=binding.backend_identity,
            rule_name="orders_rule",
            alias_name="orders.public",
            current=GatewayActionSurfaceEvidence(
                exists=False,
                fingerprint=absent_fingerprint,
                managed_interceptor_count=0,
            ),
            desired=GatewayActionSurfaceEvidence(
                exists=True,
                fingerprint=desired_fingerprint,
                managed_interceptor_count=1,
            ),
        ),
    )
    target = RecoveryTargetEvidence(
        action=action,
        presence="present",
        accepted_as="candidate",
        fingerprint=desired_fingerprint,
    )

    assert RecoveryTargetEvidence.from_dict(target.to_dict()) == target
    with pytest.raises(StateFormatError, match="presence does not match"):
        RecoveryTargetEvidence(
            action=action,
            presence="absent",
            accepted_as="candidate",
            fingerprint=desired_fingerprint,
        )
    with pytest.raises(StateFormatError, match="fingerprint does not match"):
        RecoveryTargetEvidence(
            action=action,
            presence="present",
            accepted_as="candidate",
            fingerprint="sha256:" + "c" * 64,
        )


def test_gateway_adopt_candidate_is_bound_to_current_not_desired_surface() -> None:
    binding = GatewayBackendBinding.from_endpoint(
        "https://gateway.example.test",
        virtual_cluster="payments-prod",
    )
    current_fingerprint = "sha256:" + "d" * 64
    desired_fingerprint = "sha256:" + "e" * 64
    action = OperationAction(
        index=0,
        resource_id=resource_id(
            "payments",
            "prod",
            "gateway_rule",
            "orders_owner",
        ),
        action="adopt",
        gateway_evidence=GatewayActionEvidence(
            version=1,
            backend_identity=binding.backend_identity,
            rule_name="orders_rule",
            alias_name="orders.public",
            current=GatewayActionSurfaceEvidence(
                exists=True,
                fingerprint=current_fingerprint,
                managed_interceptor_count=0,
            ),
            desired=GatewayActionSurfaceEvidence(
                exists=True,
                fingerprint=desired_fingerprint,
                managed_interceptor_count=0,
            ),
        ),
    )

    target = RecoveryTargetEvidence(
        action=action,
        presence="present",
        accepted_as="candidate",
        fingerprint=current_fingerprint,
    )

    assert RecoveryTargetEvidence.from_dict(target.to_dict()) == target
    with pytest.raises(StateFormatError, match="fingerprint does not match"):
        RecoveryTargetEvidence(
            action=action,
            presence="present",
            accepted_as="candidate",
            fingerprint=desired_fingerprint,
        )


def test_connector_target_evidence_is_bound_to_exact_prior_or_candidate_surface() -> None:
    backend_identity = "kafka-connect:v1:primary:sha256:" + "4" * 64
    connector_name = "archive-orders-sink"
    current_fingerprint = "sha256:" + "5" * 64
    desired_fingerprint = managed_connector_absence_fingerprint(
        backend_identity,
        connector_name,
    )
    action = OperationAction(
        index=0,
        resource_id=resource_id(
            "payments",
            "prod",
            "connector",
            "archive_orders",
        ),
        action="delete",
        connector_evidence=ConnectorActionEvidence(
            version=1,
            backend_identity=backend_identity,
            connector_name=connector_name,
            prior_artifact_checksum="sha256:" + "6" * 64,
            current=ConnectorActionSurfaceEvidence(
                exists=True,
                fingerprint=current_fingerprint,
            ),
            desired=ConnectorActionSurfaceEvidence(
                exists=False,
                fingerprint=desired_fingerprint,
            ),
        ),
    )
    prior = RecoveryTargetEvidence(
        action=action,
        presence="present",
        accepted_as="prior",
        fingerprint=current_fingerprint,
    )
    candidate = RecoveryTargetEvidence(
        action=action,
        presence="absent",
        accepted_as="candidate",
        fingerprint=desired_fingerprint,
    )

    assert RecoveryTargetEvidence.from_dict(prior.to_dict()) == prior
    assert RecoveryTargetEvidence.from_dict(candidate.to_dict()) == candidate

    invalid = (
        ({**candidate.to_dict(), "presence": "present"}, "presence"),
        (
            {**candidate.to_dict(), "fingerprint": current_fingerprint},
            "fingerprint",
        ),
        (
            {
                **prior.to_dict(),
                "presence": "absent",
                "fingerprint": desired_fingerprint,
            },
            "presence",
        ),
        (
            {
                **candidate.to_dict(),
                "presence": "present",
                "fingerprint": current_fingerprint,
            },
            "presence",
        ),
    )
    for payload, message in invalid:
        with pytest.raises(StateFormatError, match=message):
            RecoveryTargetEvidence.from_dict(payload)


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


@pytest.mark.parametrize("mismatch", ["serial", "checksum"])
def test_snapshot_binds_blocked_intent_to_exact_prior_state(mismatch: str) -> None:
    snapshot = _snapshot()
    intent = snapshot.control.control.intent
    assert intent is not None
    changed_intent = replace(
        intent,
        prior_state_serial=(
            intent.prior_state_serial + 1
            if mismatch == "serial"
            else intent.prior_state_serial
        ),
        prior_state_checksum=(
            "sha256:" + "f" * 64
            if mismatch == "checksum"
            else intent.prior_state_checksum
        ),
    )
    changed = replace(
        snapshot,
        control=replace(
            snapshot.control,
            control=replace(snapshot.control.control, intent=changed_intent),
        ),
    )

    with pytest.raises(StateFormatError, match="intent does not match"):
        RecoverySnapshotEvidence.from_operation_snapshot(changed)


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
