"""Regression tests for the runtime observe command and observer."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import yaml
from click.testing import CliRunner

from streamt.cli import main
from streamt.deployer.flink import FlinkJobState
from streamt.deployer.kafka import ConsumerGroupLag, ConsumerGroupObservationError


def _write_project(path: Path) -> None:
    config = {
        "project": {"name": "observe-test"},
        "runtime": {"kafka": {"bootstrap_servers": "broker:9092"}},
        "sources": [{"name": "raw", "topic": "raw.v1"}],
        "models": [
            {
                "name": "clean",
                "materialized": "topic",
                "sql": 'SELECT * FROM {{ source("raw") }}',
                "topic": {"name": "clean.events.v1"},
            }
        ],
    }
    (path / "stream_project.yml").write_text(yaml.safe_dump(config))


def _runtime_mocks() -> tuple[MagicMock, MagicMock]:
    kafka = MagicMock()
    kafka.get_consumer_groups.return_value = ["billing"]
    kafka.get_consumer_group_lag.return_value = ConsumerGroupLag(
        group_id="billing", topic="clean.events.v1", total_lag=12
    )

    flink = MagicMock()
    flink.get_job_state.return_value = FlinkJobState(
        name="clean_processor", exists=True, job_id="job-1", status="RUNNING"
    )
    flink._request.return_value = [
        {"id": "numRecordsInPerSecond", "value": "42.5"},
        {"id": "isBackPressured", "value": "false"},
    ]
    return kafka, flink


def test_observe_text_uses_current_cli_and_runtime_apis(tmp_path: Path) -> None:
    _write_project(tmp_path)
    kafka, flink = _runtime_mocks()

    with (
        patch("streamt.cli.commands.observe.make_kafka_deployer", return_value=kafka),
        patch("streamt.cli.commands.observe.make_flink_deployer", return_value=flink),
    ):
        result = CliRunner().invoke(main, ["observe", "-p", str(tmp_path)])

    assert result.exit_code == 0, result.output
    assert "Runtime Health" in result.output
    assert "clean.events.v1" in result.output
    assert "42.5 rec/s" in result.output
    assert "billing" in result.output
    kafka.get_consumer_group_lag.assert_called_once_with("billing", "clean.events.v1")
    flink.get_job_state.assert_called_once_with("clean_processor")
    kafka.close.assert_called_once_with()
    flink.close.assert_called_once_with()


def test_observe_json_has_structured_model_data(tmp_path: Path) -> None:
    _write_project(tmp_path)
    kafka, flink = _runtime_mocks()

    with (
        patch("streamt.cli.commands.observe.make_kafka_deployer", return_value=kafka),
        patch("streamt.cli.commands.observe.make_flink_deployer", return_value=flink),
    ):
        result = CliRunner().invoke(
            main, ["-o", "json", "observe", "-p", str(tmp_path), "--model", "clean"]
        )

    assert result.exit_code == 0, result.output
    payload = json.loads(result.stdout)
    assert payload["command"] == "observe"
    assert payload["data"]["models"] == [
        {
            "name": "clean",
            "topic": "clean.events.v1",
            "health": "ok",
            "total_lag": 12,
            "consumers": [{"group_id": "billing", "lag": 12, "state": None}],
            "consumer_evidence": {
                "status": "verified",
                "source": "kafka_consumer_groups",
                "reason": None,
                "failures": [],
            },
            "flink": {
                "job_id": "job-1",
                "state": "RUNNING",
                "records_in_per_second": 42.5,
                "is_backpressured": False,
            },
        }
    ]


def test_observe_unknown_model_returns_structured_model_not_found(
    tmp_path: Path,
) -> None:
    _write_project(tmp_path)
    kafka, flink = _runtime_mocks()

    with (
        patch("streamt.cli.commands.observe.make_kafka_deployer", return_value=kafka),
        patch("streamt.cli.commands.observe.make_flink_deployer", return_value=flink),
    ):
        result = CliRunner().invoke(
            main,
            ["-o", "json", "observe", "-p", str(tmp_path), "--model", "missing"],
        )

    assert result.exit_code == 1, result.output
    payload = json.loads(result.stdout)
    assert payload["status"] == "error"
    assert payload["errors"] == [
        {
            "code": "E102_MODEL_NOT_FOUND",
            "message": "Model 'missing' not found in manifest",
        }
    ]


def test_observe_missing_project_fails_without_traceback(tmp_path: Path) -> None:
    result = CliRunner().invoke(main, ["observe", "-p", str(tmp_path)])

    assert result.exit_code == 1
    assert "stream_project.yml" in result.output
    assert not isinstance(result.exception, TypeError)


def test_observe_reports_partial_redacted_consumer_evidence(tmp_path: Path) -> None:
    _write_project(tmp_path)
    kafka, flink = _runtime_mocks()
    kafka.get_consumer_group_lag.side_effect = ConsumerGroupObservationError(
        "committed-offset query",
        group_id="billing",
        topic="clean.events.v1",
        detail="authorization failed, password=supersecret",
    )

    with (
        patch("streamt.cli.commands.observe.make_kafka_deployer", return_value=kafka),
        patch("streamt.cli.commands.observe.make_flink_deployer", return_value=flink),
    ):
        result = CliRunner().invoke(main, ["-o", "json", "observe", "-p", str(tmp_path)])

    assert result.exit_code == 0, result.output
    assert "supersecret" not in result.output
    model = json.loads(result.output)["data"]["models"][0]
    assert model["health"] == "unknown"
    assert model["consumers"] == []
    assert model["consumer_evidence"] == {
        "status": "partial",
        "source": "kafka_consumer_groups",
        "reason": "consumer_group_lag_failed",
        "failures": [
            {
                "scope": "consumer_group/billing",
                "code": "consumer_group_lag_failed",
                "message": (
                    "Kafka committed-offset query failed for consumer group 'billing' "
                    "on topic 'clean.events.v1': authorization failed, <redacted>"
                ),
            }
        ],
    }
