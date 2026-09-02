"""Real PostgreSQL command coverage for reviewed recovery."""

from __future__ import annotations

import json
import uuid
from pathlib import Path

import pytest
import yaml
from click.testing import CliRunner

from streamt.cli import main
from streamt.deployer.postgres_state import (
    PostgresStateInitializer,
    PrivatePostgresStateV2Migrator,
)
from streamt.deployer.postgres_state_backend import PrivatePostgresStateReadBackend
from streamt.deployer.state_backend import (
    DeploymentStateService,
    OperationControlState,
    OperationIntent,
    StateAddress,
    operation_timestamp,
    state_checksum,
)
from tests.postgres.conftest import PostgresCase, WriterIdentity

pytestmark = [pytest.mark.integration, pytest.mark.postgres]

_WRITER_DSN_ENV = "STREAMT_RECOVERY_COMMAND_WRITER_DSN"


def _address() -> StateAddress:
    return StateAddress(
        namespace="platform",
        project="recovery-command",
        environment="default",
    )


def _write_project(path: Path, case: PostgresCase) -> None:
    (path / "stream_project.yml").write_text(
        yaml.safe_dump(
            {
                "apiVersion": "streamt.dev/v1alpha1",
                "project": {"name": "recovery-command"},
                "runtime": {
                    "kafka": {"bootstrap_servers": "unreachable.invalid:9092"}
                },
                "deployment_state": {
                    "backend": "postgres",
                    "namespace": "platform",
                    "lock_timeout_seconds": 10,
                    "postgres": {
                        "dsn_env": "STREAMT_RECOVERY_COMMAND_OWNER_DSN",
                        "writer_dsn_env": _WRITER_DSN_ENV,
                        "schema": case.schema,
                    },
                },
            }
        ),
        encoding="utf-8",
    )


def _service(
    case: PostgresCase,
    writer: WriterIdentity,
) -> DeploymentStateService:
    initialized = PostgresStateInitializer(
        dsn=case.owner_dsn,
        schema=case.schema,
        lock_timeout_seconds=10,
    ).initialize(_address())
    PrivatePostgresStateV2Migrator(
        dsn=case.owner_dsn,
        schema=case.schema,
        lock_timeout_seconds=10,
        writer_role=writer.role,
    ).migrate(
        confirmed_store_id=initialized.store_id,
        confirmed_writer_role=writer.role,
    )
    return DeploymentStateService(
        backend=PrivatePostgresStateReadBackend(
            dsn=writer.dsn,
            schema=case.schema,
            lock_timeout_seconds=10,
        ),
        address=_address(),
    )


def test_postgres_abandoned_recovery_commands_use_writer_authority(
    tmp_path: Path,
    postgres_case: PostgresCase,
    postgres_writer: WriterIdentity,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_project(tmp_path, postgres_case)
    monkeypatch.setenv(_WRITER_DSN_ENV, postgres_writer.dsn)
    service = _service(postgres_case, postgres_writer)
    blocked_operation_id = str(uuid.uuid4())
    with service.operation() as operation:
        snapshot = operation.observe()
        operation.begin_operation(
            snapshot,
            OperationIntent(
                operation_id=blocked_operation_id,
                kind="apply",
                started_at=operation_timestamp(),
                actor="postgres-command-test",
                prior_state_serial=snapshot.state.state_serial,
                prior_state_checksum=state_checksum(snapshot.state.state),
                reviewed_plan_checksum=None,
                actions=(),
            ),
        )

    plan_path = tmp_path / "recovery.json"
    runner = CliRunner()
    planned = runner.invoke(
        main,
        [
            "-o",
            "json",
            "state",
            "recovery-plan",
            "-p",
            str(tmp_path),
            "--resolution",
            "abandoned_before_mutation",
            "--out",
            str(plan_path),
        ],
    )

    assert planned.exit_code == 0, planned.output
    plan_data = json.loads(planned.stdout)["data"]
    assert plan_data["blocked_operation_id"] == blocked_operation_id

    recovered = runner.invoke(
        main,
        [
            "-o",
            "json",
            "state",
            "recover",
            "-p",
            str(tmp_path),
            "--plan",
            str(plan_path),
            "--confirm-operation-id",
            blocked_operation_id,
            "--confirm-resolution",
            "abandoned_before_mutation",
            "--confirm-evidence-checksum",
            plan_data["evidence_checksum"],
        ],
    )

    assert recovered.exit_code == 0, recovered.output
    recovered_data = json.loads(recovered.stdout)["data"]
    assert recovered_data["store"]["backend"] == "postgres"
    assert recovered_data["address"] == _address().uri
    assert recovered_data["state_serial"] == 0
    assert recovered_data["state_changed"] is False
    assert recovered_data["control_status"] == "clear"
    assert service.read_control().control == OperationControlState.clear(_address())
