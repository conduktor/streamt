"""Real PostgreSQL v2 composition gates for durable apply OpenLineage events.

The tests use the production deployment-state factory and synchronous File
transport. Only Kafka provider behavior and compiled deployment artifacts are
replaced, matching the installed-wheel ordinary-command gates.
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from unittest.mock import patch
from uuid import UUID

import pytest
import yaml
from click.testing import CliRunner, Result

from streamt.cli import main
from streamt.integrations.openlineage import validate_event_sequence
from tests.postgres.conftest import PostgresCase, WriterIdentity
from tests.postgres.test_postgres_ordinary_factory_commands_real import (
    _assert_no_local_state,
    _bind_writer_only,
    _expected_state,
    _initialize_v2,
    _verification_service,
    _write_project,
)
from tests.postgres.test_postgres_state_commands_real import (
    _assert_finalized,
    _kafka,
    _manifest,
    _operation_rows,
    _topic,
)

pytestmark = [pytest.mark.integration, pytest.mark.postgres]

_JOB_NAMESPACE = "postgres-apply-jobs"
_FAILURE_SECRET = "postgres-openlineage-provider-secret"
_OPENLINEAGE_ENVIRONMENT = (
    "OPENLINEAGE_CONFIG",
    "OPENLINEAGE_DISABLED",
    "OPENLINEAGE_URL",
    "OPENLINEAGE_API_KEY",
    "OPENLINEAGE_NAMESPACE",
    "OPENLINEAGE__TRANSPORT__TYPE",
    "OPENLINEAGE__TRANSPORT__LOG_FILE_PATH",
    "OPENLINEAGE__TRANSPORT__URL",
    "OPENLINEAGE__TRANSPORT__ENDPOINT",
    "STREAMT_OPENLINEAGE_KAFKA_NAMESPACE",
    "STREAMT_OPENLINEAGE_GATEWAY_NAMESPACE",
)


def _configure_file_transport(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[Path, Path]:
    for name in _OPENLINEAGE_ENVIRONMENT:
        monkeypatch.delenv(name, raising=False)
    events_path = tmp_path / "openlineage-events.jsonl"
    config_path = tmp_path / "openlineage-config.yml"
    config_path.write_text(
        yaml.safe_dump(
            {
                "transport": {
                    "type": "file",
                    "log_file_path": str(events_path),
                }
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("OPENLINEAGE_CONFIG", str(config_path))
    return config_path, events_path


def _events(path: Path) -> list[dict[str, object]]:
    events = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]
    assert all(isinstance(event, dict) for event in events)
    validate_event_sequence(events)
    return events


def _assert_event_surface(
    events: list[dict[str, object]],
    *,
    operation_id: str,
    started_at: str,
    terminal: str,
    forbidden: tuple[str, ...],
) -> None:
    assert [event["eventType"] for event in events] == ["START", terminal]
    assert events[0]["eventTime"] == started_at
    assert [event["run"] for event in events] == [
        {"runId": operation_id},
        (
            {
                "runId": operation_id,
                "facets": {
                    "errorMessage": {
                        "_producer": "https://github.com/conduktor/streamt",
                        "_schemaURL": (
                            "https://openlineage.io/spec/facets/1-0-1/"
                            "ErrorMessageRunFacet.json#/$defs/ErrorMessageRunFacet"
                        ),
                        "message": "streamt apply command did not complete successfully",
                        "programmingLanguage": "PYTHON",
                    }
                },
            }
            if terminal == "FAIL"
            else {"runId": operation_id}
        ),
    ]
    UUID(operation_id)
    for event in events:
        assert set(event) == {
            "eventTime",
            "eventType",
            "job",
            "producer",
            "run",
            "schemaURL",
        }
        assert "inputs" not in event
        assert "outputs" not in event
        job = event["job"]
        assert isinstance(job, dict)
        assert job["namespace"] == _JOB_NAMESPACE
        assert job["name"] == "streamt/command-e2e/commands/apply"
        assert job["facets"] == {
            "jobType": {
                "_producer": "https://github.com/conduktor/streamt",
                "_schemaURL": (
                    "https://openlineage.io/spec/facets/2-0-4/"
                    "JobTypeJobFacet.json#/$defs/JobTypeJobFacet"
                ),
                "integration": "STREAMT",
                "jobType": "COMMAND",
                "processingType": "BATCH",
            }
        }

    serialized = json.dumps(events, sort_keys=True)
    for value in forbidden:
        assert value not in serialized
    for forbidden_key in (
        '"actions"',
        '"artifacts"',
        '"plan"',
        '"state"',
        '"datasets"',
    ):
        assert forbidden_key not in serialized


def _persisted_intent(case: PostgresCase) -> tuple[str, dict[str, object]]:
    _control, history, _current_count = _operation_rows(case)
    operation_id = history[0][0]
    assert isinstance(operation_id, str)
    raw_control = history[0][3]
    assert isinstance(raw_control, str)
    control = json.loads(raw_control)
    intent = control["intent"]
    assert isinstance(intent, dict)
    return operation_id, intent


def _invoke_apply(tmp_path: Path, *, plan_path: Path | None = None) -> Result:
    arguments = [
        "-o",
        "json",
        "apply",
        "-p",
        str(tmp_path),
        "--emit-openlineage",
        "--openlineage-job-namespace",
        _JOB_NAMESPACE,
        "--openlineage-kafka-namespace",
        "kafka://explicit.example.test:9092",
        "--openlineage-gateway-namespace",
        "kafka://gateway.example.test:6969",
    ]
    if plan_path is not None:
        arguments.extend(["--plan", str(plan_path)])
    return CliRunner().invoke(main, arguments)


@pytest.mark.parametrize("reviewed", [False, True], ids=("direct", "reviewed"))
def test_postgres_apply_success_emits_exact_durable_openlineage_run(
    tmp_path: Path,
    postgres_case: PostgresCase,
    postgres_writer: WriterIdentity,
    monkeypatch: pytest.MonkeyPatch,
    reviewed: bool,
) -> None:
    _write_project(tmp_path, postgres_case)
    _initialize_v2(postgres_case, postgres_writer)
    _bind_writer_only(monkeypatch, dsn=postgres_writer.dsn)
    config_path, events_path = _configure_file_transport(tmp_path, monkeypatch)
    topic = _topic()
    manifest = _manifest(topic)
    plan_path: Path | None = None
    reviewed_checksum: str | None = None

    if reviewed:
        plan_path = tmp_path / "reviewed-plan.json"
        with (
            patch("streamt.compiler.Compiler.compile", return_value=manifest),
            patch(
                "streamt.cli.commands.plan.make_kafka_deployer",
                return_value=_kafka(exists=False),
            ),
        ):
            planned = CliRunner().invoke(
                main,
                [
                    "-o",
                    "json",
                    "plan",
                    "-p",
                    str(tmp_path),
                    "--out",
                    str(plan_path),
                ],
            )
        assert planned.exit_code == 0, planned.output
        assert not events_path.exists()
        plan_data = json.loads(plan_path.read_text(encoding="utf-8"))
        reviewed_checksum = plan_data["checksum"]
        assert isinstance(reviewed_checksum, str)

    with (
        patch("streamt.compiler.Compiler.compile", return_value=manifest),
        patch(
            "streamt.cli.commands.apply.make_kafka_deployer",
            return_value=_kafka(exists=False),
        ),
    ):
        applied = _invoke_apply(tmp_path, plan_path=plan_path)

    assert applied.exit_code == 0, applied.output
    service = _verification_service(postgres_case, postgres_writer)
    _assert_finalized(
        postgres_case,
        service,
        _expected_state(topic),
        kind="apply",
        reviewed_plan_checksum=reviewed_checksum,
    )
    operation_id, intent = _persisted_intent(postgres_case)
    assert intent["operation_id"] == operation_id
    assert intent["kind"] == "apply"
    assert intent["reviewed_plan_checksum"] == reviewed_checksum
    started_at = intent["started_at"]
    assert isinstance(started_at, str)
    _assert_event_surface(
        _events(events_path),
        operation_id=operation_id,
        started_at=started_at,
        terminal="COMPLETE",
        forbidden=(
            "payments.clean.v1",
            "broker.invalid",
            "explicit.example.test",
            "gateway.example.test",
            "direct-kafka",
            str(config_path),
            str(events_path),
            str(plan_path) if plan_path is not None else "reviewed-plan.json",
            reviewed_checksum or "reviewed-plan-checksum",
            postgres_case.schema,
            postgres_case.owner_role,
            postgres_writer.role,
            postgres_case.owner_dsn,
            postgres_writer.dsn,
        ),
    )
    _assert_no_local_state(tmp_path)


def test_postgres_apply_runtime_failure_emits_fail_after_durable_recovery_marker(
    tmp_path: Path,
    postgres_case: PostgresCase,
    postgres_writer: WriterIdentity,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_project(tmp_path, postgres_case)
    _initialize_v2(postgres_case, postgres_writer)
    _bind_writer_only(monkeypatch, dsn=postgres_writer.dsn)
    config_path, events_path = _configure_file_transport(tmp_path, monkeypatch)
    topic = _topic()
    manifest = _manifest(topic)
    kafka = _kafka(exists=False)
    kafka.apply_topic.side_effect = RuntimeError(
        f"provider password={_FAILURE_SECRET} topic=payments.clean.v1"
    )

    with (
        patch("streamt.compiler.Compiler.compile", return_value=manifest),
        patch(
            "streamt.cli.commands.apply.make_kafka_deployer",
            return_value=kafka,
        ),
    ):
        applied = _invoke_apply(tmp_path)

    assert applied.exit_code == 1, applied.output
    service = _verification_service(postgres_case, postgres_writer)
    assert service.read().state.resources == {}
    control_observation = service.read_control()
    control = control_observation.control
    assert control.status == "recovery_required"
    assert control.intent is not None
    assert control.recovery is not None
    assert control.recovery.failure_code == "runtime_action_failed"
    assert control.recovery.operation_id == control.intent.operation_id
    assert [(item.status, item.succeeded) for item in control.progress] == [
        ("started", None),
        ("completed", False),
    ]

    control_row, history, current_count = _operation_rows(postgres_case)
    operation_id = history[0][0]
    assert isinstance(operation_id, str)
    assert control_row[0:2] == (4, "recovery_required")
    assert current_count == 0
    assert [(row[0], row[1], row[2]) for row in history] == [
        (operation_id, 0, "intent"),
        (operation_id, 1, "progress_started"),
        (operation_id, 2, "progress_completed"),
        (operation_id, 3, "recovery_required"),
    ]
    raw_control = control_row[2]
    assert isinstance(raw_control, str)
    persisted_control = json.loads(raw_control)
    persisted_intent = persisted_control["intent"]
    persisted_recovery = persisted_control["recovery"]
    assert persisted_intent["operation_id"] == operation_id
    assert persisted_recovery["operation_id"] == operation_id
    started_at = persisted_intent["started_at"]
    assert isinstance(started_at, str)
    events = _events(events_path)
    _assert_event_surface(
        events,
        operation_id=operation_id,
        started_at=started_at,
        terminal="FAIL",
        forbidden=(
            _FAILURE_SECRET,
            "payments.clean.v1",
            "broker.invalid",
            "explicit.example.test",
            "gateway.example.test",
            "direct-kafka",
            str(config_path),
            str(events_path),
            postgres_case.schema,
            postgres_case.owner_role,
            postgres_writer.role,
            postgres_case.owner_dsn,
            postgres_writer.dsn,
        ),
    )
    failed_at = persisted_recovery["failed_at"]
    terminal_at = events[1]["eventTime"]
    assert isinstance(failed_at, str)
    assert isinstance(terminal_at, str)
    assert datetime.fromisoformat(failed_at.replace("Z", "+00:00")) <= datetime.fromisoformat(
        terminal_at.replace("Z", "+00:00")
    )
    assert _FAILURE_SECRET not in events_path.read_text(encoding="utf-8")
    kafka.apply_topic.assert_called_once_with(topic)
    kafka.delete_topic.assert_not_called()
    _assert_no_local_state(tmp_path)
