"""External declarations are not implicitly inspected by status."""

from contextlib import ExitStack
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import yaml
from click.testing import CliRunner

from streamt.cli import main
from streamt.compiler.manifest import Manifest
from streamt.core.errors import ErrorCode
from streamt.deployer.flink import FlinkJobState
from streamt.deployer.kafka import TopicState
from streamt.deployer.schema_registry import SchemaState

FACTORIES = ("kafka", "sr", "flink", "connect", "gateway")


def project_file(path: Path, *, model_mode: str | None = None) -> None:
    config = {
        "apiVersion": "streamt.dev/v1alpha1",
        "project": {"name": "external-status"},
        "runtime": {
            "kafka": {"bootstrap_servers": "unreachable.invalid:9092"},
            "schema_registry": {"url": "http://unreachable.invalid:8081"},
        },
        "sources": [{
            "name": "orders", "topic": "existing.orders",
            "schema": {"registry": "confluent", "subject": "orders-value", "version": 2},
            "columns": [{"name": "id", "type": "STRING"}],
        }],
    }
    if model_mode is not None:
        config["models"] = [{
            "name": "clean_orders", "materialized": "topic",
            "ownership": {"mode": model_mode},
            "sql": 'SELECT id FROM {{ source("orders") }}',
        }]
    (path / "stream_project.yml").write_text(yaml.safe_dump(config))


def invoke_status(path: Path, *options: str, clients: dict | None = None):
    import json

    clients = clients or {}
    with ExitStack() as stack:
        factories = {
            kind: stack.enter_context(patch(
                f"streamt.cli.commands.status.make_{kind}_deployer",
                **({"return_value": clients[kind]} if kind in clients else {
                    "side_effect": AssertionError(f"unexpected {kind} connection"),
                }),
            )) for kind in FACTORIES
        }
        result = CliRunner().invoke(main, ["-o", "json", "status", "-p", str(path), *options])
    assert result.exception is None or isinstance(result.exception, SystemExit), result.output
    return result, json.loads(result.output), factories


@pytest.mark.parametrize("model_mode", [None, "external"])
def test_external_status_is_declaration_only(tmp_path: Path, model_mode: str | None) -> None:
    project_file(tmp_path, model_mode=model_mode)
    result, payload, factories = invoke_status(tmp_path, "--health", "--consumer-groups")
    assert result.exit_code == 0, result.output
    assert all(factory.call_count == 0 for factory in factories.values())
    data = payload["data"]
    assert data["observation_scope"] == "managed"
    assert data["source_topics"] == [{
        "name": "orders", "topic": "existing.orders", "exists": None,
        "partitions": None, "observation": "not_requested",
    }]
    assert data["schemas"] == data["topics"] == data["flink_jobs"] == []
    assert data["external_resources"]
    assert all(row["observation"] == "not_requested" for row in data["external_resources"])


@pytest.mark.parametrize("exists", [True, False])
def test_explicit_external_inspection_controls_health(tmp_path: Path, exists: bool) -> None:
    project_file(tmp_path)
    kafka = MagicMock()
    kafka.get_topic_state.return_value = TopicState(
        name="existing.orders", exists=exists, partitions=2, replication_factor=1,
    )
    registry = MagicMock()
    registry.get_schema_state.return_value = SchemaState(
        subject="orders-value", exists=True, version=2, schema_type="AVRO",
    )
    result, payload, factories = invoke_status(
        tmp_path, "--include-external", "--health", clients={"kafka": kafka, "sr": registry},
    )
    assert result.exit_code == (0 if exists else 1), result.output
    assert payload["data"]["observation_scope"] == "managed_and_external"
    source = payload["data"]["source_topics"][0]
    assert source["observation"] == "verified"
    assert source["exists"] is exists
    kafka.get_topic_state.assert_called_once_with("existing.orders")
    registry.get_schema_state.assert_called_once_with("orders-value")
    factories["kafka"].assert_called_once()
    kafka.close.assert_called_once()
    registry.close.assert_called_once()


def test_managed_status_keeps_live_checks(tmp_path: Path) -> None:
    project_file(tmp_path, model_mode="managed")
    kafka = MagicMock()
    kafka.get_topic_state.return_value = TopicState(
        name="clean_orders", exists=False,
    )
    flink = MagicMock()
    flink.get_job_state.return_value = FlinkJobState(
        name="clean_orders_processor", exists=True, status="RUNNING", job_id="job-1",
    )
    result, payload, factories = invoke_status(
        tmp_path, "--health", clients={"kafka": kafka, "flink": flink},
    )
    assert result.exit_code == 1, result.output
    kafka.get_topic_state.assert_called_once_with("clean_orders")
    flink.get_job_state.assert_called_once_with("clean_orders_processor")
    factories["sr"].assert_not_called()
    assert payload["data"]["topics"][0]["status"] == "MISSING"
    assert payload["data"]["source_topics"][0]["observation"] == "not_requested"


def test_filtered_status_does_not_open_unused_providers(tmp_path: Path) -> None:
    project_file(tmp_path, model_mode="managed")
    result, payload, factories = invoke_status(
        tmp_path, "--include-external", "--filter", "absent-*", "--health",
    )
    assert result.exit_code == 0, result.output
    assert all(factory.call_count == 0 for factory in factories.values())
    assert payload["data"]["source_topics"] == []


def test_explicit_external_observation_failure_is_not_health(tmp_path: Path) -> None:
    project_file(tmp_path)
    kafka = MagicMock()
    kafka.get_topic_state.side_effect = ConnectionError("unavailable")
    registry = MagicMock()
    registry.get_schema_state.return_value = SchemaState(subject="orders-value", exists=True)
    result, payload, _ = invoke_status(
        tmp_path, "--include-external", "--health", clients={"kafka": kafka, "sr": registry},
    )
    assert result.exit_code == 1, result.output
    assert payload["errors"]
    kafka.close.assert_called_once()


@pytest.mark.parametrize(
    ("kind", "artifact"),
    [
        ("topics", {}),
        ("schemas", {"subject": None}),
        ("flink_jobs", {"name": ""}),
        ("connectors", {"name": 17}),
        ("gateway_rules", {"name": ["https://reader:TOPSECRET@private.example.invalid"]}),
        ("gateway_rules", None),
    ],
)
@pytest.mark.parametrize("options", [[], ["--include-external"], ["--filter", "absent-*"]])
def test_malformed_artifact_prefilter_returns_secret_neutral_json_without_providers(
    tmp_path: Path, kind: str, artifact: object, options: list[str],
) -> None:
    project_file(tmp_path)
    manifest = Manifest(
        version="1", project_name="external-status", artifacts={kind: [artifact]},
    )
    with patch("streamt.compiler.Compiler.compile", return_value=manifest):
        result, payload, factories = invoke_status(tmp_path, "--health", *options)

    assert result.exit_code == 1, result.output
    assert payload["status"] == "error"
    assert payload["errors"] == [{
        "code": ErrorCode.PARSE_ERROR,
        "message": "Compiled status artifact metadata is invalid; recompile the project.",
    }]
    assert all(factory.call_count == 0 for factory in factories.values())
    assert "TOPSECRET" not in result.output
    assert "private.example.invalid" not in result.output


def test_malformed_artifact_prefilter_reports_text_error(tmp_path: Path) -> None:
    project_file(tmp_path)
    manifest = Manifest(
        version="1", project_name="external-status", artifacts={"topics": [{}]},
    )
    with patch("streamt.compiler.Compiler.compile", return_value=manifest):
        result = CliRunner().invoke(main, ["status", "-p", str(tmp_path)])

    assert result.exit_code == 1, result.output
    assert "Compiled status artifact metadata is invalid" in result.output


@pytest.mark.parametrize(
    "ownership",
    [
        None,
        {"mode": "external"},
        {"mode": "external", "project": "another-project", "type": "model", "name": "output"},
        {"mode": None, "project": "external-status", "type": "model", "name": "output"},
    ],
)
def test_untrusted_ownership_does_not_hide_live_health_evidence(
    tmp_path: Path, ownership: object,
) -> None:
    project_file(tmp_path)
    manifest = Manifest(
        version="1", project_name="external-status", artifacts={"topics": [{
            "name": "output", "partitions": 1, "replication_factor": 1,
            "ownership": ownership,
        }]},
    )
    kafka = MagicMock()
    kafka.get_topic_state.return_value = TopicState(name="output", exists=False)

    with patch("streamt.compiler.Compiler.compile", return_value=manifest):
        result, payload, factories = invoke_status(tmp_path, "--health", clients={"kafka": kafka})

    assert result.exit_code == 1, result.output
    assert payload["data"]["external_resources"] == []
    assert payload["data"]["topics"][0]["status"] == "MISSING"
    factories["kafka"].assert_called_once()
    kafka.get_topic_state.assert_called_once_with("output")
    kafka.close.assert_called_once()
