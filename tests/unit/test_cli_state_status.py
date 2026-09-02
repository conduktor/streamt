"""Read-only local deployment-state status CLI tests."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch
from uuid import UUID

import yaml
from click.testing import CliRunner

from streamt.cli import main
from streamt.deployer.state import (
    LocalState,
    ManagedResourceRecord,
    artifact_checksum,
    local_state_path,
    resource_id,
)
from streamt.deployer.state_backend import (
    LocalDeploymentStateBackend,
    OperationAction,
    OperationIntent,
    RecoveryRecord,
    local_control_path,
    make_deployment_state_service,
    operation_timestamp,
    state_checksum,
)


def _write_project(path: Path) -> None:
    (path / "stream_project.yml").write_text(
        yaml.safe_dump(
            {
                "apiVersion": "streamt.dev/v1alpha1",
                "project": {"name": "status-test"},
                "runtime": {
                    "kafka": {"bootstrap_servers": "unreachable.invalid:9092"}
                },
            }
        ),
        encoding="utf-8",
    )


def _write_multi_environment_project(path: Path) -> None:
    (path / "stream_project.yml").write_text(
        yaml.safe_dump(
            {
                "apiVersion": "streamt.dev/v1alpha1",
                "project": {"name": "status-test"},
            }
        ),
        encoding="utf-8",
    )
    environments = path / "environments"
    environments.mkdir()
    (environments / "dev.yml").write_text(
        yaml.safe_dump(
            {
                "environment": {"name": "dev"},
                "runtime": {
                    "kafka": {"bootstrap_servers": "unreachable.invalid:9092"}
                },
            }
        ),
        encoding="utf-8",
    )


def _json(result: object) -> dict[str, object]:
    return json.loads(result.stdout)


def test_missing_state_is_reported_without_files_locks_compiler_or_runtime(
    tmp_path: Path,
) -> None:
    _write_project(tmp_path)
    expected_state = LocalState(project="status-test", environment="default")

    with (
        patch(
            "streamt.deployer.state_backend.local_state_operation_lock",
            side_effect=AssertionError("state status acquired a mutation lock"),
        ),
        patch(
            "streamt.compiler.Compiler",
            side_effect=AssertionError("state status constructed the compiler"),
        ),
    ):
        result = CliRunner().invoke(
            main,
            ["-o", "json", "state", "status", "-p", str(tmp_path)],
        )

    assert result.exit_code == 0, result.output
    payload = _json(result)
    assert payload["command"] == "state status"
    data = payload["data"]
    assert data["backend"] == "local"
    UUID(data["store_id"])
    assert data["address"] == "streamt-state://local/status-test/default"
    assert data["state_status"] == "absent"
    assert data["state_serial"] == 0
    assert data["state_checksum"] == state_checksum(expected_state)
    assert data["operation_status"] == {
        "status": "clear",
        "operation_id": None,
        "kind": None,
        "failure_code": None,
        "last_completed_action_index": None,
    }
    assert not (tmp_path / ".streamt").exists()


def test_status_reads_selected_environment_without_creating_state(tmp_path: Path) -> None:
    _write_multi_environment_project(tmp_path)

    result = CliRunner().invoke(
        main,
        ["-o", "json", "state", "status", "-p", str(tmp_path), "-e", "dev"],
    )

    assert result.exit_code == 0, result.output
    data = _json(result)["data"]
    assert data["address"] == "streamt-state://local/status-test/dev"
    assert data["state_status"] == "absent"
    assert not (tmp_path / ".streamt").exists()


def test_present_state_reports_only_safe_metadata_without_modification(
    tmp_path: Path,
) -> None:
    _write_project(tmp_path)
    resource_uri = resource_id(
        "status-test",
        "default",
        "topic",
        "orders",
    )
    ownership = LocalState(
        project="status-test",
        environment="default",
        serial=7,
        resources={
            resource_uri: ManagedResourceRecord(
                physical_name="orders.internal.v1",
                ownership="managed",
                artifact_checksum=artifact_checksum({"name": "orders.internal.v1"}),
                backend="direct-kafka",
            )
        },
    )
    state_path = local_state_path(tmp_path, environment="default")
    ownership.save(state_path)
    before = state_path.read_bytes()

    result = CliRunner().invoke(
        main,
        ["-o", "json", "state", "status", "-p", str(tmp_path)],
    )

    assert result.exit_code == 0, result.output
    data = _json(result)["data"]
    assert data["state_status"] == "present"
    assert data["state_serial"] == 7
    assert data["state_checksum"] == state_checksum(ownership)
    assert "orders.internal.v1" not in result.output
    assert resource_uri not in result.output
    assert state_path.read_bytes() == before
    assert not local_control_path(tmp_path, environment="default").exists()


def test_unfinished_marker_is_read_only_and_text_directs_operator(
    tmp_path: Path,
) -> None:
    _write_project(tmp_path)
    service = make_deployment_state_service(
        tmp_path,
        project="status-test",
        environment="default",
    )
    with service.operation() as operation:
        ownership = operation.read().state
        intent = OperationIntent(
            operation_id="00000000-0000-4000-8000-000000000041",
            kind="apply",
            started_at=operation_timestamp(),
            actor="prior-runner",
            prior_state_serial=ownership.serial,
            prior_state_checksum=state_checksum(ownership),
            reviewed_plan_checksum=None,
            actions=(OperationAction(0, "topic:orders", "create"),),
        )
        active = operation.begin_operation(operation.read_control(), intent)
        operation.mark_recovery_required(
            active,
            RecoveryRecord(
                operation_id=intent.operation_id,
                failure_code="operation_interrupted",
                failed_at=operation_timestamp(),
                last_completed_action_index=None,
            ),
        )
    control_path = local_control_path(tmp_path, environment="default")
    before = control_path.read_bytes()

    with patch.object(
        LocalDeploymentStateBackend,
        "operation",
        side_effect=AssertionError("state status acquired a mutation lock"),
    ):
        result = CliRunner().invoke(
            main,
            ["state", "status", "-p", str(tmp_path)],
        )

    assert result.exit_code == 0, result.output
    normalized_output = " ".join(result.output.split())
    assert "Operation: recovery_required" in normalized_output
    assert "blocks apply/adopt" in normalized_output
    assert "Retain the control sidecar as evidence" in normalized_output
    assert "do not delete or edit it" in normalized_output
    assert "Recovery is not implemented yet" in normalized_output
    assert control_path.read_bytes() == before
    assert not local_state_path(tmp_path, environment="default").exists()


def test_malformed_ownership_fails_closed_without_creating_control(
    tmp_path: Path,
) -> None:
    _write_project(tmp_path)
    state_path = local_state_path(tmp_path, environment="default")
    state_path.parent.mkdir(parents=True)
    state_path.write_text(
        '{"state_version":1,"password":"ownership-secret"}',
        encoding="utf-8",
    )
    before = state_path.read_bytes()

    result = CliRunner().invoke(
        main,
        ["-o", "json", "state", "status", "-p", str(tmp_path)],
    )

    assert result.exit_code == 1
    payload = _json(result)
    assert payload["errors"][0]["code"] == "E411_STATE_INVALID"
    assert "ownership-secret" not in result.output
    assert state_path.read_bytes() == before
    assert not local_control_path(tmp_path, environment="default").exists()


def test_malformed_control_fails_closed_without_creating_ownership(
    tmp_path: Path,
) -> None:
    _write_project(tmp_path)
    control_path = local_control_path(tmp_path, environment="default")
    control_path.parent.mkdir(parents=True)
    control_path.write_text(
        '{"authorization":"control-secret"',
        encoding="utf-8",
    )
    before = control_path.read_bytes()

    result = CliRunner().invoke(
        main,
        ["-o", "json", "state", "status", "-p", str(tmp_path)],
    )

    assert result.exit_code == 1
    payload = _json(result)
    assert payload["errors"][0]["code"] == "E411_STATE_INVALID"
    assert "control-secret" not in result.output
    assert control_path.read_bytes() == before
    assert not local_state_path(tmp_path, environment="default").exists()


def test_unknown_environment_fails_before_state_service_construction(
    tmp_path: Path,
) -> None:
    _write_multi_environment_project(tmp_path)

    with patch(
        "streamt.cli.commands.state_cmd.make_deployment_state_service",
        side_effect=AssertionError("invalid environment reached state service"),
    ):
        result = CliRunner().invoke(
            main,
            ["-o", "json", "state", "status", "-p", str(tmp_path), "-e", "prod"],
        )

    assert result.exit_code == 1
    assert _json(result)["errors"][0]["code"] == "E501_PARSE_ERROR"
    assert not (tmp_path / ".streamt").exists()
