"""Focused PostgreSQL recovery-history and retry semantics."""

from __future__ import annotations

import json
import uuid
from dataclasses import replace

import pytest

from streamt.deployer.postgres_state_backend import _PostgresStateReadOperation
from streamt.deployer.recovery import (
    RecoveryResolutionRecord,
    RecoverySnapshotEvidence,
)
from streamt.deployer.state import LocalState
from streamt.deployer.state_backend import (
    ControlObservation,
    OperationControlState,
    OperationIntent,
    OperationSnapshot,
    StateAddress,
    StateBackendConflictError,
    StateObservation,
    StateRevision,
    StateStoreIdentity,
    operation_timestamp,
    state_checksum,
)


def _evidence() -> RecoverySnapshotEvidence:
    address = StateAddress(
        namespace="platform",
        project="payments",
        environment="prod",
    )
    state = LocalState(project="payments", environment="prod")
    intent = OperationIntent(
        operation_id=str(uuid.uuid4()),
        kind="apply",
        started_at=operation_timestamp(),
        actor="postgres-recovery-unit",
        prior_state_serial=0,
        prior_state_checksum=state_checksum(state),
        reviewed_plan_checksum=None,
        actions=(),
    )
    return RecoverySnapshotEvidence.from_operation_snapshot(
        OperationSnapshot(
            state=StateObservation(
                store=StateStoreIdentity(
                    backend="postgres",
                    store_id=str(uuid.uuid4()),
                ),
                address=address,
                state=state,
                revision=StateRevision.absent(),
            ),
            control=ControlObservation(
                control=OperationControlState(
                    address=address,
                    status="in_progress",
                    intent=intent,
                ),
                revision=StateRevision("postgres-v1:1"),
            ),
        )
    )


def _resolution(evidence: RecoverySnapshotEvidence) -> RecoveryResolutionRecord:
    return RecoveryResolutionRecord(
        address=evidence.address,
        recovery_operation_id=str(uuid.uuid4()),
        blocked_operation_id=evidence.blocked_operation_id,
        resolution="rolled_back",
        resolved_at="2026-09-02T12:00:00Z",
        evidence_checksum="sha256:" + "e" * 64,
        prior_state_serial=evidence.state.serial,
        prior_state_checksum=evidence.state_checksum,
        result_state_serial=evidence.state.serial,
        result_state_checksum=evidence.state_checksum,
        state_changed=False,
    )


def _operation() -> _PostgresStateReadOperation:
    return object.__new__(_PostgresStateReadOperation)


def test_retry_history_identity_ignores_only_resolution_timestamp() -> None:
    operation = _operation()
    evidence = _evidence()
    original = _resolution(evidence)
    rows = operation._recovery_history_rows(evidence, original)
    later = replace(original, resolved_at="2099-01-01T00:00:00Z")

    assert operation._recovery_history_matches(
        rows,
        evidence=evidence,
        resolution=later,
        allow_resolution_timestamp_change=True,
    )
    assert not operation._recovery_history_matches(
        rows,
        evidence=evidence,
        resolution=later,
        allow_resolution_timestamp_change=False,
    )

    different = replace(later, evidence_checksum="sha256:" + "f" * 64)
    assert not operation._recovery_history_matches(
        rows,
        evidence=evidence,
        resolution=different,
        allow_resolution_timestamp_change=True,
    )
    different_operation = replace(later, recovery_operation_id=str(uuid.uuid4()))
    assert not operation._recovery_history_matches(
        rows,
        evidence=evidence,
        resolution=different_operation,
        allow_resolution_timestamp_change=True,
    )


def test_retry_history_rejects_noncanonical_or_secret_bearing_payload() -> None:
    operation = _operation()
    evidence = _evidence()
    resolution = _resolution(evidence)
    rows = operation._recovery_history_rows(evidence, resolution)
    raw = json.loads(rows[1][2])
    raw["resolved_at"] = "password=leaked"
    tampered = json.dumps(raw, sort_keys=True, separators=(",", ":"))
    rows[1] = (1, rows[1][1], tampered, len(tampered.encode("utf-8")))

    assert not operation._recovery_history_matches(
        rows,
        evidence=evidence,
        resolution=resolution,
        allow_resolution_timestamp_change=True,
    )


def test_retry_history_rejects_event_gaps_and_outcome_tag_mismatch() -> None:
    operation = _operation()
    evidence = _evidence()
    resolution = _resolution(evidence)
    rows = operation._recovery_history_rows(evidence, resolution)

    gap = [rows[0], (2, *rows[1][1:])]
    assert not operation._recovery_history_matches(
        gap,
        evidence=evidence,
        resolution=resolution,
        allow_resolution_timestamp_change=True,
    )
    wrong_kind = [rows[0], (1, "recovered_observed", *rows[1][2:])]
    assert not operation._recovery_history_matches(
        wrong_kind,
        evidence=evidence,
        resolution=resolution,
        allow_resolution_timestamp_change=True,
    )


def test_completed_retry_conflicts_when_same_operation_changes_evidence() -> None:
    operation = _operation()
    evidence = _evidence()
    resolution = _resolution(evidence)
    rows = operation._recovery_history_rows(evidence, resolution)

    with pytest.raises(StateBackendConflictError):
        operation._require_completed_recovery_history_match(
            rows,
            evidence=evidence,
            resolution=replace(
                resolution,
                evidence_checksum="sha256:" + "f" * 64,
            ),
        )


def test_completed_retry_conflicts_when_recovery_operation_is_unknown() -> None:
    operation = _operation()
    evidence = _evidence()
    resolution = replace(
        _resolution(evidence),
        recovery_operation_id=str(uuid.uuid4()),
    )

    with pytest.raises(StateBackendConflictError):
        operation._require_completed_recovery_history_match(
            [],
            evidence=evidence,
            resolution=resolution,
        )
