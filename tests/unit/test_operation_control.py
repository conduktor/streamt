"""Durable local operation-control protocol characterization tests."""

from __future__ import annotations

import json
import multiprocessing
import os
from copy import deepcopy
from dataclasses import replace
from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest

from streamt.core.deployment_state import local_deployment_state_config
from streamt.deployer.gateway import managed_gateway_absence_fingerprint
from streamt.deployer.state import LocalState, StateFormatError, local_state_path
from streamt.deployer.state_backend import (
    GatewayActionEvidence,
    GatewayActionSurfaceEvidence,
    OperationAction,
    OperationControlState,
    OperationIntent,
    OperationProgress,
    OperationSnapshot,
    RecoveryRecord,
    StateAddress,
    StateBackendConflictError,
    StateBackendRecoveryRequiredError,
    StateBackendUnknownCommitError,
    local_control_path,
    make_deployment_state_service,
    operation_timestamp,
    state_checksum,
)

GATEWAY_BACKEND = "conduktor-gateway:v1:p:sha256:" + "1" * 64
CURRENT_FINGERPRINT = "sha256:" + "2" * 64
DESIRED_FINGERPRINT = "sha256:" + "3" * 64
ABSENCE_FINGERPRINT = managed_gateway_absence_fingerprint(
    GATEWAY_BACKEND,
    "orders_rule",
    "orders.public",
)


def _intent(state: LocalState) -> OperationIntent:
    return OperationIntent(
        operation_id=str(uuid4()),
        kind="apply",
        started_at=operation_timestamp(),
        actor="test-runner",
        prior_state_serial=state.serial,
        prior_state_checksum=state_checksum(state),
        reviewed_plan_checksum=None,
        actions=(OperationAction(0, "topic:orders", "create"),),
    )


def _gateway_evidence(
    *,
    current_exists: bool = True,
    desired_exists: bool = True,
    current_fingerprint: str | None = None,
    desired_fingerprint: str | None = None,
) -> GatewayActionEvidence:
    return GatewayActionEvidence(
        version=1,
        backend_identity=GATEWAY_BACKEND,
        rule_name="orders_rule",
        alias_name="orders.public",
        current=GatewayActionSurfaceEvidence(
            exists=current_exists,
            fingerprint=(
                current_fingerprint
                if current_fingerprint is not None
                else CURRENT_FINGERPRINT if current_exists else ABSENCE_FINGERPRINT
            ),
            managed_interceptor_count=1 if current_exists else 0,
        ),
        desired=GatewayActionSurfaceEvidence(
            exists=desired_exists,
            fingerprint=(
                desired_fingerprint
                if desired_fingerprint is not None
                else DESIRED_FINGERPRINT if desired_exists else ABSENCE_FINGERPRINT
            ),
            managed_interceptor_count=1 if desired_exists else 0,
        ),
    )


def _gateway_action(*, action: str = "update") -> OperationAction:
    evidence = _gateway_evidence(
        current_exists=action != "create",
        desired_exists=action != "delete",
    )
    return OperationAction(
        index=0,
        resource_id="streamt://payments/dev/gateway_rule/orders_owner",
        action=action,
        gateway_evidence=evidence,
    )


def test_gateway_action_evidence_has_one_strict_secret_neutral_v2_shape() -> None:
    action = _gateway_action()

    serialized = action.to_dict()

    assert serialized == {
        "index": 0,
        "resource_id": "streamt://payments/dev/gateway_rule/orders_owner",
        "action": "update",
        "gateway_evidence": {
            "version": 1,
            "backend_identity": GATEWAY_BACKEND,
            "rule_name": "orders_rule",
            "alias_name": "orders.public",
            "current": {
                "exists": True,
                "fingerprint": CURRENT_FINGERPRINT,
                "managed_interceptor_count": 1,
            },
            "desired": {
                "exists": True,
                "fingerprint": DESIRED_FINGERPRINT,
                "managed_interceptor_count": 1,
            },
        },
    }
    assert OperationAction.from_dict(serialized, control_version=2) == action
    assert action.resource_id.endswith("/orders_owner")
    assert action.gateway_evidence is not None
    assert action.gateway_evidence.rule_name == "orders_rule"
    rendered = repr(action)
    assert "http" not in rendered
    assert "config" not in rendered


def test_gateway_action_evidence_rejects_noncanonical_or_unsafe_fields() -> None:
    original = _gateway_evidence().to_dict()

    def add_raw_config(data: dict[str, Any]) -> None:
        data["configuration"] = {"sql": "select secret"}

    def remove_rule_name(data: dict[str, Any]) -> None:
        data.pop("rule_name")

    def add_surface_config(data: dict[str, Any]) -> None:
        data["current"]["config"] = {"password": "secret"}

    def malformed_fingerprint(data: dict[str, Any]) -> None:
        data["current"]["fingerprint"] = "SHA256:" + "A" * 64

    def missing_surface_field(data: dict[str, Any]) -> None:
        data["desired"].pop("exists")

    def nonboolean_exists(data: dict[str, Any]) -> None:
        data["desired"]["exists"] = 1

    def boolean_count(data: dict[str, Any]) -> None:
        data["desired"]["managed_interceptor_count"] = True

    def negative_count(data: dict[str, Any]) -> None:
        data["desired"]["managed_interceptor_count"] = -1

    def absent_nonzero_count(data: dict[str, Any]) -> None:
        data["current"]["exists"] = False

    def wrong_absence_fingerprint(data: dict[str, Any]) -> None:
        data["current"] = {
            "exists": False,
            "fingerprint": CURRENT_FINGERPRINT,
            "managed_interceptor_count": 0,
        }

    def present_with_absence_fingerprint(data: dict[str, Any]) -> None:
        data["desired"]["fingerprint"] = ABSENCE_FINGERPRINT

    def malformed_backend(data: dict[str, Any]) -> None:
        data["backend_identity"] = "https://alice:secret@gateway.example"

    def unsafe_alias(data: dict[str, Any]) -> None:
        data["alias_name"] = "orders/secret"

    def unsafe_rule_name(data: dict[str, Any]) -> None:
        data["rule_name"] = "orders/rule"

    def unknown_version(data: dict[str, Any]) -> None:
        data["version"] = 2

    for mutate in (
        add_raw_config,
        remove_rule_name,
        add_surface_config,
        malformed_fingerprint,
        missing_surface_field,
        nonboolean_exists,
        boolean_count,
        negative_count,
        absent_nonzero_count,
        wrong_absence_fingerprint,
        present_with_absence_fingerprint,
        malformed_backend,
        unsafe_alias,
        unsafe_rule_name,
        unknown_version,
    ):
        payload = deepcopy(original)
        mutate(payload)
        with pytest.raises(StateFormatError) as captured:
            GatewayActionEvidence.from_dict(payload)
        assert "alice" not in str(captured.value)
        assert "secret" not in str(captured.value)


@pytest.mark.parametrize(
    ("resource_id", "action", "evidence"),
    [
        ("streamt://payments/dev/topic/orders", "update", _gateway_evidence()),
        ("gateway_rule:orders", "update", _gateway_evidence()),
        (
            "streamt://payments/dev/gateway_rule/orders",
            "noop",
            _gateway_evidence(),
        ),
        (
            "streamt://payments/dev/gateway_rule/orders",
            "create",
            _gateway_evidence(),
        ),
        (
            "streamt://payments/dev/gateway_rule/orders",
            "update",
            _gateway_evidence(desired_fingerprint=CURRENT_FINGERPRINT),
        ),
        (
            "streamt://payments/dev/gateway_rule/orders",
            "delete",
            _gateway_evidence(),
        ),
    ],
)
def test_gateway_action_evidence_rejects_kind_action_and_transition_mismatch(
    resource_id: str,
    action: str,
    evidence: GatewayActionEvidence,
) -> None:
    with pytest.raises(StateFormatError, match="Gateway"):
        OperationAction(
            index=0,
            resource_id=resource_id,
            action=action,
            gateway_evidence=evidence,
        )


def test_v1_control_roundtrips_exactly_and_rejects_gateway_evidence() -> None:
    address = StateAddress("local", "payments", "dev")
    state = LocalState(project="payments", environment="dev")
    intent = _intent(state)
    legacy = OperationControlState(
        address=address,
        status="in_progress",
        intent=intent,
        control_version=1,
    )
    payload = legacy.to_dict()
    canonical = json.dumps(payload, separators=(",", ":"), sort_keys=True)

    assert set(payload["intent"]["actions"][0]) == {
        "index",
        "resource_id",
        "action",
    }
    loaded = OperationControlState.from_dict(payload, expected_address=address)
    assert loaded.control_version == 1
    assert loaded.to_dict() == payload
    assert json.dumps(
        loaded.to_dict(), separators=(",", ":"), sort_keys=True
    ) == canonical

    gateway_intent = replace(intent, actions=(_gateway_action(),))
    with pytest.raises(StateFormatError, match="version 1"):
        OperationControlState(
            address=address,
            status="in_progress",
            intent=gateway_intent,
            control_version=1,
        )
    gateway_payload = OperationControlState(
        address=address,
        status="in_progress",
        intent=gateway_intent,
    ).to_dict()
    gateway_payload["control_version"] = 1
    with pytest.raises(StateFormatError, match="unknown field"):
        OperationControlState.from_dict(gateway_payload, expected_address=address)


def test_v2_control_requires_explicit_action_evidence_member() -> None:
    address = StateAddress("local", "payments", "dev")
    state = LocalState(project="payments", environment="dev")
    control = OperationControlState(
        address=address,
        status="in_progress",
        intent=_intent(state),
    )
    payload = control.to_dict()

    assert payload["control_version"] == 2
    assert payload["intent"]["actions"][0]["gateway_evidence"] is None
    del payload["intent"]["actions"][0]["gateway_evidence"]
    with pytest.raises(StateFormatError, match="missing field"):
        OperationControlState.from_dict(payload, expected_address=address)


def test_v2_control_requires_and_roundtrips_gateway_mutation_evidence() -> None:
    address = StateAddress("local", "payments", "dev")
    state = LocalState(project="payments", environment="dev")
    base_intent = _intent(state)
    missing_evidence = replace(
        base_intent,
        actions=(
            OperationAction(
                index=0,
                resource_id="streamt://payments/dev/gateway_rule/orders_owner",
                action="update",
            ),
        ),
    )
    with pytest.raises(StateFormatError, match="require action evidence"):
        OperationControlState(
            address=address,
            status="in_progress",
            intent=missing_evidence,
        )

    intent = replace(base_intent, actions=(_gateway_action(),))
    control = OperationControlState(
        address=address,
        status="in_progress",
        intent=intent,
    )
    payload = control.to_dict()

    loaded = OperationControlState.from_dict(payload, expected_address=address)
    assert loaded == control
    assert loaded.to_dict() == payload
    assert loaded.intent is not None
    assert loaded.intent.actions[0].gateway_evidence == _gateway_evidence()

    with pytest.raises(StateFormatError, match="another state address"):
        OperationControlState(
            address=StateAddress("local", "other", "dev"),
            status="in_progress",
            intent=intent,
        )


def _crash_after_mock_mutation(project_path: str, runtime_marker: str) -> None:
    service = make_deployment_state_service(
        Path(project_path),
        project="payments",
        environment="dev",
        config=local_deployment_state_config(),
    )
    with service.operation() as operation:
        control = operation.read_control()
        intent = _intent(operation.read().state)
        control = operation.begin_operation(control, intent)
        control = operation.record_progress(
            control,
            OperationProgress(
                operation_id=intent.operation_id,
                action_index=0,
                resource_id="topic:orders",
                action="create",
                status="started",
                succeeded=None,
                recorded_at=operation_timestamp(),
            ),
        )
        # This file write stands in for a runtime backend call that returned.
        Path(runtime_marker).write_text("mutated", encoding="utf-8")
        os._exit(23)


def test_control_lifecycle_is_strict_atomic_and_does_not_change_v1_state(
    tmp_path: Path,
) -> None:
    state_path = local_state_path(tmp_path, environment="dev")
    state = LocalState(project="payments", environment="dev")
    state.save(state_path)
    ownership_bytes = state_path.read_bytes()
    service = make_deployment_state_service(
        tmp_path,
        project="payments",
        environment="dev",
        config=local_deployment_state_config(),
    )

    with service.operation() as operation:
        initial = operation.read_control()
        operation.ensure_ready(initial)
        intent = _intent(state)
        active = operation.begin_operation(initial, intent)
        active = operation.record_progress(
            active,
            OperationProgress(
                operation_id=intent.operation_id,
                action_index=0,
                resource_id="topic:orders",
                action="create",
                status="started",
                succeeded=None,
                recorded_at=operation_timestamp(),
            ),
        )
        active = operation.record_progress(
            active,
            OperationProgress(
                operation_id=intent.operation_id,
                action_index=0,
                resource_id="topic:orders",
                action="create",
                status="completed",
                succeeded=True,
                recorded_at=operation_timestamp(),
            ),
        )
        cleared = operation.clear_operation(active)

    assert cleared.control == OperationControlState.clear(service.address)
    control_path = local_control_path(tmp_path, environment="dev")
    assert control_path.stat().st_mode & 0o777 == 0o600
    assert state_path.read_bytes() == ownership_bytes
    assert not list(control_path.parent.glob(f".{control_path.name}.*.tmp"))


def test_typed_snapshot_lifecycle_commits_state_before_clearing_control(
    tmp_path: Path,
) -> None:
    state_path = local_state_path(tmp_path, environment="dev")
    prior = LocalState(project="payments", environment="dev")
    prior.save(state_path)
    service = make_deployment_state_service(
        tmp_path,
        project="payments",
        environment="dev",
        config=local_deployment_state_config(),
    )

    with service.operation() as operation:
        snapshot = operation.observe()
        assert isinstance(snapshot, OperationSnapshot)
        assert snapshot.address == service.address
        intent = _intent(snapshot.state.state)
        active = operation.begin_operation(snapshot, intent)
        active = operation.record_progress(
            active,
            OperationProgress(
                operation_id=intent.operation_id,
                action_index=0,
                resource_id="topic:orders",
                action="create",
                status="started",
                succeeded=None,
                recorded_at=operation_timestamp(),
            ),
        )
        active = operation.record_progress(
            active,
            OperationProgress(
                operation_id=intent.operation_id,
                action_index=0,
                resource_id="topic:orders",
                action="create",
                status="completed",
                succeeded=True,
                recorded_at=operation_timestamp(),
            ),
        )
        replacement = LocalState(
            project="payments",
            environment="dev",
            serial=1,
        )
        committed = operation.commit_operation(active, replacement)

    assert committed.state.state == replacement
    assert committed.control.control == OperationControlState.clear(service.address)
    assert LocalState.load(state_path) == replacement


def test_snapshot_begin_rejects_intent_or_state_revision_drift(
    tmp_path: Path,
) -> None:
    service = make_deployment_state_service(
        tmp_path,
        project="payments",
        environment="dev",
        config=local_deployment_state_config(),
    )

    with service.operation() as operation:
        snapshot = operation.observe()
        mismatched = replace(
            _intent(snapshot.state.state),
            prior_state_serial=1,
        )
        with pytest.raises(StateBackendConflictError, match="prior state snapshot"):
            operation.begin_operation(snapshot, mismatched)
        checksum_mismatch = replace(
            _intent(snapshot.state.state),
            prior_state_checksum=state_checksum(
                LocalState(project="payments", environment="dev", serial=1)
            ),
        )
        with pytest.raises(StateBackendConflictError, match="prior state snapshot"):
            operation.begin_operation(snapshot, checksum_mismatch)

        changed = LocalState(
            project="payments",
            environment="dev",
            serial=1,
        )
        changed.save(local_state_path(tmp_path, environment="dev"))
        with pytest.raises(StateBackendConflictError, match="revision changed"):
            operation.begin_operation(snapshot, _intent(snapshot.state.state))

    assert service.read_control().control.status == "clear"


def test_commit_clear_failure_preserves_written_state_and_active_marker(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = make_deployment_state_service(
        tmp_path,
        project="payments",
        environment="dev",
        config=local_deployment_state_config(),
    )

    with service.operation() as operation:
        snapshot = operation.observe()
        intent = replace(_intent(snapshot.state.state), actions=())
        active = operation.begin_operation(snapshot, intent)
        backend = service.backend
        original_write = backend._write_control  # type: ignore[attr-defined]

        def fail_clear(
            path: Path,
            control: OperationControlState,
            *,
            operation_id: str | None,
        ) -> None:
            if control.status == "clear":
                raise StateBackendUnknownCommitError(
                    "local operation control state commit could not be confirmed",
                    operation_id=operation_id,
                )
            original_write(path, control, operation_id=operation_id)

        monkeypatch.setattr(backend, "_write_control", fail_clear)
        replacement = LocalState(
            project="payments",
            environment="dev",
            serial=1,
        )
        with pytest.raises(StateBackendUnknownCommitError) as captured:
            operation.commit_operation(active, replacement)

        assert captured.value.operation_id == intent.operation_id
        assert operation.read().state == replacement
        assert operation.read_control().control.status == "in_progress"


def test_commit_rejects_incomplete_action_without_state_write_or_clear(
    tmp_path: Path,
) -> None:
    service = make_deployment_state_service(
        tmp_path,
        project="payments",
        environment="dev",
        config=local_deployment_state_config(),
    )

    with service.operation() as operation:
        snapshot = operation.observe()
        intent = _intent(snapshot.state.state)
        active = operation.begin_operation(snapshot, intent)
        active = operation.record_progress(
            active,
            OperationProgress(
                operation_id=intent.operation_id,
                action_index=0,
                resource_id="topic:orders",
                action="create",
                status="started",
                succeeded=None,
                recorded_at=operation_timestamp(),
            ),
        )
        replacement = LocalState(
            project="payments",
            environment="dev",
            serial=1,
        )
        with pytest.raises(StateBackendRecoveryRequiredError, match="incomplete"):
            operation.commit_operation(active, replacement)

        assert operation.read().revision.is_absent is True
        assert operation.read_control().control.status == "in_progress"


def test_clear_before_mutation_rejects_started_progress(tmp_path: Path) -> None:
    service = make_deployment_state_service(
        tmp_path,
        project="payments",
        environment="dev",
        config=local_deployment_state_config(),
    )

    with service.operation() as operation:
        snapshot = operation.observe()
        intent = _intent(snapshot.state.state)
        active = operation.begin_operation(snapshot, intent)
        active = operation.record_progress(
            active,
            OperationProgress(
                operation_id=intent.operation_id,
                action_index=0,
                resource_id="topic:orders",
                action="create",
                status="started",
                succeeded=None,
                recorded_at=operation_timestamp(),
            ),
        )
        with pytest.raises(StateBackendRecoveryRequiredError, match="may have started"):
            operation.clear_before_mutation(active)

    assert service.read_control().control.status == "in_progress"


def test_clear_before_mutation_clears_intent_without_progress(tmp_path: Path) -> None:
    service = make_deployment_state_service(
        tmp_path,
        project="payments",
        environment="dev",
        config=local_deployment_state_config(),
    )

    with service.operation() as operation:
        snapshot = operation.observe()
        active = operation.begin_operation(snapshot, _intent(snapshot.state.state))
        cleared = operation.clear_before_mutation(active)

    assert cleared.state == snapshot.state
    assert cleared.control.control.status == "clear"
    assert not local_state_path(tmp_path, environment="dev").exists()


def test_commit_without_ownership_change_clears_completed_empty_intent(
    tmp_path: Path,
) -> None:
    service = make_deployment_state_service(
        tmp_path,
        project="payments",
        environment="dev",
        config=local_deployment_state_config(),
    )

    with service.operation() as operation:
        snapshot = operation.observe()
        intent = replace(_intent(snapshot.state.state), actions=())
        active = operation.begin_operation(snapshot, intent)
        committed = operation.commit_operation(active, None)

    assert committed.state == snapshot.state
    assert committed.control.control.status == "clear"
    assert not local_state_path(tmp_path, environment="dev").exists()


def test_recovery_record_is_sanitized_and_blocks_successor(tmp_path: Path) -> None:
    service = make_deployment_state_service(
        tmp_path,
        project="payments",
        environment="dev",
        config=local_deployment_state_config(),
    )
    with service.operation() as operation:
        state = operation.read().state
        intent = _intent(state)
        active = operation.begin_operation(operation.read_control(), intent)
        recovery = operation.mark_recovery_required(
            active,
            RecoveryRecord(
                operation_id=intent.operation_id,
                failure_code="runtime_action_failed",
                failed_at=operation_timestamp(),
                last_completed_action_index=None,
            ),
        )

    payload = json.loads(
        local_control_path(tmp_path, environment="dev").read_text(encoding="utf-8")
    )
    assert recovery.safe_status() == {
        "status": "recovery_required",
        "operation_id": intent.operation_id,
        "kind": "apply",
        "failure_code": "runtime_action_failed",
        "last_completed_action_index": None,
    }
    assert "message" not in json.dumps(payload)
    with (
        service.operation() as successor,
        pytest.raises(StateBackendRecoveryRequiredError, match="explicit recovery"),
    ):
        successor.ensure_ready(successor.read_control())


def test_control_parser_rejects_unknown_and_duplicate_fields(tmp_path: Path) -> None:
    service = make_deployment_state_service(
        tmp_path,
        project="payments",
        environment="dev",
        config=local_deployment_state_config(),
    )
    path = local_control_path(tmp_path, environment="dev")
    path.parent.mkdir(parents=True)
    clear = OperationControlState.clear(service.address).to_dict()
    clear["unexpected"] = True
    path.write_text(json.dumps(clear), encoding="utf-8")
    with pytest.raises(StateFormatError):
        service.read_control()

    path.write_text(
        '{"control_version":1,"control_version":1}',
        encoding="utf-8",
    )
    with pytest.raises(StateFormatError):
        service.read_control()


def test_atomic_control_failure_removes_temporary_file(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = make_deployment_state_service(
        tmp_path,
        project="payments",
        environment="dev",
        config=local_deployment_state_config(),
    )

    def fail_replace(_source: object, _target: object) -> None:
        raise OSError("token=do-not-leak")

    monkeypatch.setattr(os, "replace", fail_replace)
    with service.operation() as operation:
        state = operation.read().state
        with pytest.raises(
            StateBackendUnknownCommitError,
            match="commit could not be confirmed",
        ) as captured:
            operation.begin_operation(operation.read_control(), _intent(state))
    assert "do-not-leak" not in str(captured.value)
    control_path = local_control_path(tmp_path, environment="dev")
    assert not control_path.exists()
    assert not list(control_path.parent.glob(f".{control_path.name}.*.tmp"))


def test_cleanup_failure_cannot_mask_sanitized_unknown_commit_error(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = make_deployment_state_service(
        tmp_path,
        project="payments",
        environment="dev",
        config=local_deployment_state_config(),
    )

    def fail_replace(_source: object, _target: object) -> None:
        raise OSError("authorization=replace-secret")

    def fail_unlink(_path: Path, *, missing_ok: bool = False) -> None:
        del missing_ok
        raise OSError("password=cleanup-secret /sensitive/path")

    monkeypatch.setattr(os, "replace", fail_replace)
    monkeypatch.setattr(Path, "unlink", fail_unlink)
    with service.operation() as operation:
        state = operation.read().state
        with pytest.raises(StateBackendUnknownCommitError) as captured:
            operation.begin_operation(operation.read_control(), _intent(state))

    assert str(captured.value) == (
        "local operation control state commit could not be confirmed"
    )


def test_process_crash_after_first_mock_mutation_leaves_blocking_marker(
    tmp_path: Path,
) -> None:
    runtime_marker = tmp_path / "runtime-mutated"
    process = multiprocessing.get_context("spawn").Process(
        target=_crash_after_mock_mutation,
        args=(str(tmp_path), str(runtime_marker)),
    )
    process.start()
    process.join(timeout=15)

    assert process.exitcode == 23
    assert runtime_marker.read_text(encoding="utf-8") == "mutated"
    service = make_deployment_state_service(
        tmp_path,
        project="payments",
        environment="dev",
        config=local_deployment_state_config(),
    )
    with service.operation() as successor:
        observation = successor.read_control()
        assert observation.control.status == "in_progress"
        assert observation.control.progress[-1].status == "started"
        with pytest.raises(StateBackendRecoveryRequiredError):
            successor.ensure_ready(observation)
