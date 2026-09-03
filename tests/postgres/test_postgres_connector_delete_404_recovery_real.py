"""Real PostgreSQL recovery gate for an ambiguous Connector DELETE 404."""

from __future__ import annotations

import json
import os
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import cast
from unittest.mock import MagicMock, patch

import pytest
import requests
import yaml
from click.testing import CliRunner

from streamt.cli import main
from streamt.deployer.connect import ConnectDeployer, ManagedConnectorObservation
from streamt.deployer.kafka import KafkaDeployer
from streamt.deployer.plan_file import ReviewedPlanFile
from streamt.deployer.recovery_plan import RecoveryPlanFile
from streamt.deployer.state import (
    LocalState,
    ManagedResourceRecord,
    artifact_checksum,
    local_state_path,
    resource_id,
)
from streamt.deployer.state_backend import (
    OperationControlState,
    OperationIntent,
    operation_timestamp,
    state_checksum,
)
from tests.postgres.conftest import PostgresCase, WriterIdentity
from tests.postgres.test_postgres_connector_removal_commands_real import (
    _CONNECT_PASSWORD,
    _CONNECT_URL,
    _CONNECTOR,
    _ENVIRONMENT,
    _LIVE_CONFIG_SECRET,
    _OWNER,
    _OWNER_DSN_ENV,
    _PROJECT,
    _WRITER_DSN_ENV,
    _address,
    _binding,
    _data,
    _durable_rows,
    _initialize_v2,
    _operation_rows,
    _payload,
    _prior_artifact,
    _verification_service,
    _write_project,
)

pytestmark = [pytest.mark.integration, pytest.mark.postgres]

_DELETE_404_BODY_SECRET = "connector-delete-404-body-secret"


class _Response:
    """Small requests-compatible response used below the production deployer."""

    def __init__(self, status_code: int, body: bytes = b"") -> None:
        self.status_code = status_code
        self.headers: dict[str, str] = {"Content-Length": str(len(body))}
        self._body = body
        self.closed = False

    @property
    def content(self) -> bytes:
        return self._body

    def iter_content(self, chunk_size: int) -> tuple[bytes, ...]:
        assert type(chunk_size) is int
        assert chunk_size > 0
        return (self._body,) if self._body else ()

    def close(self) -> None:
        self.closed = True

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise requests.HTTPError(response=cast(requests.Response, self))


@dataclass
class _Connect404Transport:
    """One present Connector that becomes absent while DELETE returns 404."""

    present: bool = True
    calls: list[tuple[str, str, object, object]] = field(default_factory=list)
    responses: list[_Response] = field(default_factory=list)

    def request(
        self,
        _session: requests.Session,
        method: str,
        url: str,
        **kwargs: object,
    ) -> _Response:
        self.calls.append(
            (
                method,
                url,
                kwargs.get("allow_redirects"),
                kwargs.get("stream"),
            )
        )
        expected_url = f"{_CONNECT_URL.rstrip('/')}/connectors/{_CONNECTOR}"
        assert url == expected_url
        assert kwargs.get("allow_redirects") is False
        assert kwargs.get("stream") is True
        assert set(kwargs) == {"timeout", "allow_redirects", "stream"}

        if method == "GET":
            if self.present:
                raw_config = _prior_artifact().to_dict()["config"]
                assert isinstance(raw_config, dict)
                response = _Response(
                    200,
                    json.dumps(
                        {"name": _CONNECTOR, "config": raw_config},
                        sort_keys=True,
                        separators=(",", ":"),
                    ).encode("utf-8"),
                )
            else:
                response = _Response(404)
        elif method == "DELETE":
            assert self.present is True
            self.present = False
            response = _Response(404, _DELETE_404_BODY_SECRET.encode("utf-8"))
        else:
            raise AssertionError(f"unexpected Kafka Connect method: {method}")

        self.responses.append(response)
        return response


def _remove_tombstone(project_path: Path) -> None:
    config_path = project_path / "stream_project.yml"
    raw = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    assert isinstance(raw, dict)
    assert raw.pop("lifecycle") == {
        "connector_removals": [
            {
                "logical_owner": _OWNER,
                "name": _CONNECTOR,
                "cluster": "primary",
            }
        ]
    }
    config_path.write_text(yaml.safe_dump(raw, sort_keys=False), encoding="utf-8")


def test_delete_404_requires_reviewed_exact_absence_recovery(
    tmp_path: Path,
    postgres_case: PostgresCase,
    postgres_writer: WriterIdentity,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A real managed DELETE 404 cannot silently commit ownership removal."""
    _write_project(tmp_path, postgres_case)
    store_id = _initialize_v2(postgres_case, postgres_writer)
    monkeypatch.delenv(_OWNER_DSN_ENV, raising=False)
    monkeypatch.setenv(_WRITER_DSN_ENV, postgres_writer.dsn)
    assert _OWNER_DSN_ENV not in os.environ

    binding = _binding()
    prior_checksum = artifact_checksum(_prior_artifact().to_dict())
    connector_resource_id = resource_id(
        _PROJECT,
        _ENVIRONMENT,
        "connector",
        _OWNER,
    )
    unrelated_resource_id = resource_id(
        _PROJECT,
        _ENVIRONMENT,
        "topic",
        "audit_log",
    )
    unrelated_record = ManagedResourceRecord(
        physical_name="audit.events.v1",
        ownership="managed",
        artifact_checksum=artifact_checksum({"name": "audit.events.v1"}),
        backend="direct-kafka",
    )
    initial_state = LocalState(
        project=_PROJECT,
        environment=_ENVIRONMENT,
        serial=1,
        resources={
            connector_resource_id: ManagedResourceRecord(
                physical_name=_CONNECTOR,
                ownership="managed",
                artifact_checksum=prior_checksum,
                backend=binding.backend_identity,
            ),
            unrelated_resource_id: unrelated_record,
        },
    )
    state_service = _verification_service(postgres_case, postgres_writer)
    with state_service.operation() as operation:
        observed = operation.observe()
        active = operation.begin_operation(
            observed,
            OperationIntent(
                operation_id=str(uuid.uuid4()),
                kind="adopt",
                started_at=operation_timestamp(),
                actor="postgres-connector-delete-404-test",
                prior_state_serial=observed.state.state_serial,
                prior_state_checksum=state_checksum(observed.state.state),
                reviewed_plan_checksum=None,
                actions=(),
            ),
        )
        operation.commit_operation(active, initial_state)

    transport = _Connect404Transport()
    kafka = MagicMock(spec=KafkaDeployer)
    managed_delete_calls: list[ManagedConnectorObservation] = []
    original_managed_delete = ConnectDeployer.delete_managed_connector

    def tracked_managed_delete(
        deployer: ConnectDeployer,
        current: ManagedConnectorObservation,
    ) -> str:
        managed_delete_calls.append(current)
        return original_managed_delete(deployer, current)

    def reject_legacy_delete(_deployer: ConnectDeployer, _name: str) -> None:
        raise AssertionError("legacy Connector delete path was used")

    def transport_request(
        session: requests.Session,
        method: str,
        url: str,
        **kwargs: object,
    ) -> _Response:
        return transport.request(session, method, url, **kwargs)

    monkeypatch.setattr(requests.Session, "request", transport_request)
    monkeypatch.setattr(
        ConnectDeployer,
        "delete_managed_connector",
        tracked_managed_delete,
    )
    monkeypatch.setattr(ConnectDeployer, "delete_connector", reject_legacy_delete)

    reviewed_path = tmp_path / "connector-delete-404.plan.json"
    recovery_path = tmp_path / "connector-delete-404.recovery.json"
    runner = CliRunner()
    with (
        patch("streamt.cli.commands.plan.make_kafka_deployer", return_value=kafka),
        patch("streamt.cli.commands.apply.make_kafka_deployer", return_value=kafka),
    ):
        planned = runner.invoke(
            main,
            ["-o", "json", "plan", "-p", str(tmp_path), "--out", str(reviewed_path)],
        )
        assert planned.exit_code == 0, planned.output
        reviewed = ReviewedPlanFile.load(reviewed_path)
        assert reviewed.state is not None
        assert reviewed.state.backend == "postgres"
        assert reviewed.state.store_id == store_id
        assert len(reviewed.actions) == 1
        reviewed_action = reviewed.actions[0]
        assert reviewed_action.resource_id == connector_resource_id
        assert reviewed_action.action == "delete"
        assert reviewed_action.connector_evidence is not None

        before_apply = _durable_rows(postgres_case)
        failed = runner.invoke(
            main,
            [
                "-o",
                "json",
                "apply",
                "-p",
                str(tmp_path),
                "--plan",
                str(reviewed_path),
                "--force",
            ],
        )

    assert failed.exit_code == 1
    failed_payload = _payload(failed)
    assert failed_payload["status"] == "error"
    failed_errors = failed_payload["errors"]
    assert isinstance(failed_errors, list)
    assert isinstance(failed_errors[0], dict)
    assert failed_errors[0]["code"] == "E428_CONNECTOR_REMOVAL_DRIFT"
    assert failed_errors[0]["message"] == (
        "Kafka Connect managed deletion could not prove exact absence"
    )
    assert transport.present is False
    assert len(managed_delete_calls) == 1
    assert managed_delete_calls[0].exists is True
    assert [call[0] for call in transport.calls] == ["GET", "GET", "GET", "DELETE"]
    delete_calls = [call for call in transport.calls if call[0] == "DELETE"]
    assert delete_calls == [
        (
            "DELETE",
            f"{_CONNECT_URL.rstrip('/')}/connectors/{_CONNECTOR}",
            False,
            True,
        )
    ]
    assert all(response.closed for response in transport.responses)

    blocked_control = state_service.read_control().control
    assert blocked_control.status == "recovery_required"
    assert blocked_control.intent is not None
    assert blocked_control.intent.reviewed_plan_checksum == reviewed.checksum
    assert blocked_control.intent.actions == reviewed.actions
    assert [
        (entry.action_index, entry.action, entry.status, entry.succeeded)
        for entry in blocked_control.progress
    ] == [(0, "delete", "started", None), (0, "delete", "completed", False)]
    assert blocked_control.recovery is not None
    assert blocked_control.recovery.failure_code == "connector_removal_drift"
    assert state_service.read().state == initial_state
    assert _durable_rows(postgres_case) != before_apply

    blocked_operation_id = blocked_control.intent.operation_id
    _control_row, operation_rows, _current_count = _operation_rows(postgres_case)
    blocked_history = [row for row in operation_rows if row[0] == blocked_operation_id]
    assert [(row[1], row[2]) for row in blocked_history] == [
        (0, "intent"),
        (1, "progress_started"),
        (2, "progress_completed"),
        (3, "recovery_required"),
    ]

    _remove_tombstone(tmp_path)
    before_recovery_plan = _durable_rows(postgres_case)
    with patch("streamt.cli.helpers.make_kafka_deployer", return_value=kafka):
        recovery_planned = runner.invoke(
            main,
            [
                "-o",
                "json",
                "state",
                "recovery-plan",
                "-p",
                str(tmp_path),
                "--resolution",
                "observed",
                "--out",
                str(recovery_path),
            ],
        )
        assert recovery_planned.exit_code == 0, recovery_planned.output
        assert _durable_rows(postgres_case) == before_recovery_plan

        recovery_plan = RecoveryPlanFile.load(recovery_path)
        assert recovery_plan.blocked_operation_id == blocked_operation_id
        assert recovery_plan.resolution == "observed"
        assert recovery_plan.snapshot.store.backend == "postgres"
        assert recovery_plan.snapshot.store.store_id == store_id
        assert recovery_plan.snapshot.address == _address()
        assert len(recovery_plan.targets) == 1
        target = recovery_plan.targets[0]
        assert target.action == reviewed_action
        assert target.presence == "absent"
        assert target.accepted_as == "candidate"
        expected_state = LocalState(
            project=_PROJECT,
            environment=_ENVIRONMENT,
            serial=initial_state.serial + 1,
            resources={unrelated_resource_id: unrelated_record},
        )
        assert recovery_plan.candidate_state == expected_state

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
                str(recovery_path),
                "--confirm-operation-id",
                blocked_operation_id,
                "--confirm-resolution",
                "observed",
                "--confirm-evidence-checksum",
                recovery_plan.evidence_checksum,
            ],
        )

    assert recovered.exit_code == 0, recovered.output
    recovered_data = _data(recovered)
    assert recovered_data["state_changed"] is True
    assert recovered_data["state_serial"] == expected_state.serial
    assert recovered_data["state_checksum"] == state_checksum(expected_state)
    assert recovered_data["control_status"] == "clear"
    assert state_service.read().state == expected_state
    assert state_service.read_control().control == OperationControlState.clear(_address())
    assert [call[0] for call in transport.calls] == [
        "GET",
        "GET",
        "GET",
        "DELETE",
        "GET",
        "GET",
    ]
    assert all(response.closed for response in transport.responses)

    _final_control, final_operation_rows, current_count = _operation_rows(postgres_case)
    assert current_count == 1
    recovery_history = [
        row for row in final_operation_rows if row[0] == recovery_plan.recovery_operation_id
    ]
    assert [(row[1], row[2]) for row in recovery_history] == [
        (0, "recovery_intent"),
        (1, "recovered_observed"),
    ]

    public_and_durable_wire = "\n".join(
        (
            planned.output,
            failed.output,
            recovery_planned.output,
            recovered.output,
            reviewed_path.read_text(encoding="utf-8"),
            recovery_path.read_text(encoding="utf-8"),
            *(str(row[3]) for row in blocked_history + recovery_history),
        )
    )
    writer_details = postgres_case.conninfo.conninfo_to_dict(postgres_writer.dsn)
    for forbidden in (
        _DELETE_404_BODY_SECRET,
        _CONNECT_URL,
        _CONNECT_PASSWORD,
        _LIVE_CONFIG_SECRET,
        postgres_case.owner_dsn,
        postgres_writer.dsn,
        cast(str, writer_details["password"]),
    ):
        assert forbidden not in public_and_durable_wire
    assert not local_state_path(tmp_path, environment=_ENVIRONMENT).exists()
    assert not (tmp_path / ".streamt").exists()
