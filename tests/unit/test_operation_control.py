"""Durable local operation-control protocol characterization tests."""

from __future__ import annotations

import json
import multiprocessing
import os
from dataclasses import replace
from pathlib import Path
from uuid import uuid4

import pytest

from streamt.core.deployment_state import local_deployment_state_config
from streamt.deployer.state import LocalState, StateFormatError, local_state_path
from streamt.deployer.state_backend import (
    OperationAction,
    OperationControlState,
    OperationIntent,
    OperationProgress,
    OperationSnapshot,
    RecoveryRecord,
    StateBackendConflictError,
    StateBackendRecoveryRequiredError,
    StateBackendUnknownCommitError,
    local_control_path,
    make_deployment_state_service,
    operation_timestamp,
    state_checksum,
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
