"""Reliability tests for the read-only status command."""

from __future__ import annotations

import json
from contextlib import ExitStack
from pathlib import Path
from unittest.mock import MagicMock, patch

from click.testing import CliRunner, Result

from streamt.cli import main
from streamt.compiler.manifest import Manifest
from streamt.deployer.flink import FlinkJobState
from streamt.deployer.gateway import GatewayDeployer
from streamt.deployer.kafka import ConsumerGroupLag, PartitionLag, TopicState


def _project() -> MagicMock:
    project = MagicMock()
    project.project.name = "status-test"
    project.sources = []
    project.runtime.schema_registry = None
    project.runtime.flink = None
    project.runtime.connect = None
    project.runtime.conduktor = None
    return project


def _invoke_status(
    tmp_path: Path,
    artifacts: dict[str, list[dict[str, object]]],
    *,
    status_args: list[str] | None = None,
    json_output: bool = True,
    project: MagicMock | None = None,
    kafka: object | None = None,
    schema_registry: object | None = None,
    flink: object | None = None,
    connect: object | None = None,
    gateway: object | None = None,
) -> Result:
    manifest = Manifest(version="1", project_name="status-test", artifacts=artifacts)
    with ExitStack() as stack:
        parser = stack.enter_context(patch("streamt.core.parser.ProjectParser"))
        compiler = stack.enter_context(patch("streamt.compiler.Compiler"))
        stack.enter_context(
            patch("streamt.cli.commands.status.make_kafka_deployer", return_value=kafka)
        )
        stack.enter_context(
            patch(
                "streamt.cli.commands.status.make_sr_deployer",
                return_value=schema_registry,
            )
        )
        stack.enter_context(
            patch("streamt.cli.commands.status.make_flink_deployer", return_value=flink)
        )
        stack.enter_context(
            patch("streamt.cli.commands.status.make_connect_deployer", return_value=connect)
        )
        stack.enter_context(
            patch("streamt.cli.commands.status.make_gateway_deployer", return_value=gateway)
        )
        parser.return_value.parse.return_value = project or _project()
        compiler.return_value.compile.return_value = manifest
        return CliRunner().invoke(
            main,
            [
                *(["-o", "json"] if json_output else []),
                "status",
                *(status_args or []),
                "-p",
                str(tmp_path),
            ],
        )


def test_health_treats_missing_job_with_no_status_as_unhealthy(tmp_path: Path) -> None:
    flink = MagicMock()
    flink.get_job_state.return_value = FlinkJobState(name="orders", exists=False)

    health_result = _invoke_status(
        tmp_path,
        {"flink_jobs": [{"name": "orders"}]},
        status_args=["--health"],
        flink=flink,
    )
    assert health_result.exit_code == 1
    assert json.loads(health_result.output)["data"]["flink_jobs"] == [
        {"name": "orders", "exists": False, "job_id": None, "status": None}
    ]


def test_health_treats_missing_schema_as_unhealthy(tmp_path: Path) -> None:
    schema_registry = MagicMock()
    schema_registry.get_schema_state.return_value.exists = False

    result = _invoke_status(
        tmp_path,
        {"schemas": [{"subject": "orders-value"}]},
        status_args=["--health"],
        schema_registry=schema_registry,
    )

    assert result.exit_code == 1
    assert json.loads(result.output)["data"]["schemas"][0]["exists"] is False


def test_health_fails_when_declared_resource_cannot_be_observed(tmp_path: Path) -> None:
    result = _invoke_status(
        tmp_path,
        {"flink_jobs": [{"name": "orders"}]},
        status_args=["--health"],
        flink=None,
    )

    assert result.exit_code == 1
    assert json.loads(result.output)["data"]["flink_jobs"] == []


def test_health_fails_on_redacted_backend_error(tmp_path: Path) -> None:
    kafka = MagicMock()
    kafka.get_topic_state.side_effect = RuntimeError(
        "connection refused, password=supersecret"
    )

    result = _invoke_status(
        tmp_path,
        {"topics": [{"name": "orders", "partitions": 1, "replication_factor": 1}]},
        status_args=["--health"],
        kafka=kafka,
    )

    assert result.exit_code == 1
    payload = json.loads(result.output)
    assert payload["status"] == "error"
    assert payload["errors"][0]["code"] == "E406_CONNECTION_REFUSED"
    assert "<redacted>" in payload["errors"][0]["message"]
    assert "supersecret" not in result.output


def test_gateway_interceptor_count_handles_found_payloads(tmp_path: Path) -> None:
    gateway = MagicMock(spec=GatewayDeployer)
    gateway.get_alias_topic.return_value = {"metadata": {"name": "orders"}}
    gateway.get_interceptor.side_effect = [
        {"metadata": {"name": "mask-orders"}},
        None,
    ]

    result = _invoke_status(
        tmp_path,
        {
            "gateway_rules": [
                {
                    "name": "orders-rule",
                    "virtualTopic": "orders",
                    "physicalTopic": "orders-v1",
                    "interceptors": [
                        {"name": "mask-orders"},
                        {"name": "filter-orders"},
                    ],
                }
            ]
        },
        gateway=gateway,
    )

    assert result.exit_code == 0, result.output
    rule = json.loads(result.output)["data"]["gateway_rules"][0]
    assert rule["interceptors_desired"] == 2
    assert rule["interceptors_found"] == 1


def test_health_fails_when_gateway_interceptor_is_missing(tmp_path: Path) -> None:
    gateway = MagicMock(spec=GatewayDeployer)
    gateway.get_alias_topic.return_value = {"metadata": {"name": "orders"}}
    gateway.get_interceptor.return_value = None

    result = _invoke_status(
        tmp_path,
        {
            "gateway_rules": [
                {
                    "name": "orders-rule",
                    "virtualTopic": "orders",
                    "physicalTopic": "orders-v1",
                    "interceptors": [{"name": "mask-orders"}],
                }
            ]
        },
        status_args=["--health"],
        gateway=gateway,
    )

    assert result.exit_code == 1
    rule = json.loads(result.output)["data"]["gateway_rules"][0]
    assert rule["exists"] is True
    assert rule["interceptors_found"] == 0


def test_consumer_group_partition_lag_is_json_serializable(tmp_path: Path) -> None:
    kafka = MagicMock()
    kafka.get_topic_state.return_value = TopicState(
        name="orders", exists=True, partitions=1, replication_factor=1
    )
    kafka.get_consumer_groups.return_value = ["analytics"]
    kafka.get_consumer_group_lag.return_value = ConsumerGroupLag(
        group_id="analytics",
        topic="orders",
        total_lag=7,
        partitions=[PartitionLag(partition=0, current_offset=3, end_offset=10, lag=7)],
    )

    result = _invoke_status(
        tmp_path,
        {"topics": [{"name": "orders", "partitions": 1, "replication_factor": 1}]},
        status_args=["--consumer-groups"],
        kafka=kafka,
    )

    assert result.exit_code == 0, result.output
    partitions = json.loads(result.output)["data"]["consumer_groups"][0]["topics"][0][
        "partitions"
    ]
    assert partitions == [
        {"partition": 0, "current_offset": 3, "end_offset": 10, "lag": 7}
    ]


def test_summary_does_not_count_drift_as_ok_or_missing_job_as_running(
    tmp_path: Path,
) -> None:
    kafka = MagicMock()
    kafka.get_topic_state.return_value = TopicState(
        name="orders", exists=True, partitions=1, replication_factor=1
    )
    flink = MagicMock()
    flink.get_job_state.return_value = FlinkJobState(name="orders-job", exists=False)

    result = _invoke_status(
        tmp_path,
        {
            "topics": [{"name": "orders", "partitions": 3, "replication_factor": 1}],
            "flink_jobs": [{"name": "orders-job"}],
        },
        json_output=False,
        kafka=kafka,
        flink=flink,
    )

    assert result.exit_code == 0
    assert (
        "Summary: Topics: 0 OK, 0 missing, 1 drift | Jobs: 0 running, 1 other"
        in result.output
    )
