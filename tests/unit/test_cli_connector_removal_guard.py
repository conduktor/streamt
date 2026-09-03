"""Temporary fail-closed CLI boundary for explicit Connector removals."""

from __future__ import annotations

import json
from contextlib import ExitStack
from pathlib import Path
from typing import cast
from unittest.mock import MagicMock, patch

import pytest
import yaml
from click.testing import CliRunner, Result

from streamt.cli import main
from streamt.cli.connector_removal_guard import (
    CONNECTOR_REMOVAL_REVIEW_MESSAGE,
    CONNECTOR_REMOVAL_REVIEW_SUGGESTION,
)

_RUNTIME_FACTORIES = (
    "make_deployment_state_service",
    "make_sr_deployer",
    "make_kafka_deployer",
    "make_flink_deployer",
    "make_connect_deployer",
    "make_gateway_deployer",
)

_COMMAND_CASES = [
    pytest.param("plan", ["--offline"], id="plan-offline"),
    pytest.param("plan", [], id="plan-online"),
    pytest.param("apply", [], id="apply-direct"),
    pytest.param("apply", ["--dry-run"], id="apply-dry-run"),
    pytest.param(
        "apply",
        ["--plan", "reviewed.json", "--dry-run"],
        id="apply-reviewed-dry-run",
    ),
    pytest.param("apply", ["--target", "orders"], id="apply-target"),
    pytest.param("apply", ["--select", "tag:critical"], id="apply-select"),
]

_SECRET = "connector-removal-secret-92f0"


def _write_project(path: Path, *, removal: bool) -> None:
    config: dict[str, object] = {
        "apiVersion": "streamt.dev/v1alpha1",
        "project": {"name": "payments", "version": "1.0.0"},
        "runtime": {
            "kafka": {"bootstrap_servers": "broker.invalid:9092"},
            "connect": {
                "default": "primary-connect",
                "clusters": {"primary-connect": {"rest_url": "https://connect.invalid"}},
            },
        },
    }
    if removal:
        config["lifecycle"] = {
            "connector_removals": [
                {
                    "logical_owner": "archive_orders",
                    "name": "archive-orders-sink",
                    "cluster": "primary-connect",
                }
            ]
        }
    (path / "stream_project.yml").write_text(
        yaml.safe_dump(config, sort_keys=False),
        encoding="utf-8",
    )


def _payload(result: Result) -> dict[str, object]:
    payload = json.loads(result.stdout)
    assert isinstance(payload, dict)
    return cast(dict[str, object], payload)


def _invoke_with_runtime_forbidden(
    path: Path,
    command: str,
    extra_args: list[str],
) -> tuple[Result, list[MagicMock]]:
    with ExitStack() as stack:
        factories = [
            stack.enter_context(
                patch(
                    f"streamt.cli.commands.{command}.{name}",
                    side_effect=AssertionError(
                        f"{name} was called before Connector removal authorization"
                    ),
                )
            )
            for name in _RUNTIME_FACTORIES
        ]
        planner = stack.enter_context(
            patch(
                "streamt.deployer.planner.DeploymentPlanner",
                side_effect=AssertionError(
                    "planner was constructed before Connector removal authorization"
                ),
            )
        )
        if command == "apply" and "--plan" in extra_args:
            stack.enter_context(
                patch(
                    "streamt.cli.commands.apply.ReviewedPlanFile.load",
                    return_value=MagicMock(offline=False),
                )
            )
        result = CliRunner().invoke(
            main,
            ["-o", "json", command, "-p", str(path), *extra_args],
        )
    return result, [*factories, planner]


@pytest.mark.parametrize(("command", "extra_args"), _COMMAND_CASES)
def test_connector_removal_requires_reviewed_workflow_before_state_or_planner(
    tmp_path: Path,
    command: str,
    extra_args: list[str],
) -> None:
    _write_project(tmp_path, removal=True)

    result, factories = _invoke_with_runtime_forbidden(
        tmp_path,
        command,
        extra_args,
    )

    assert result.exit_code == 1, result.output
    payload = _payload(result)
    assert payload["errors"] == [
        {
            "code": "E418_REVIEWED_PLAN_REQUIRED",
            "message": CONNECTOR_REMOVAL_REVIEW_MESSAGE,
            "suggestion": CONNECTOR_REMOVAL_REVIEW_SUGGESTION,
        }
    ]
    assert payload["data"] == {
        "policy": "connector_removal",
        "required_workflow": "reviewed_plan",
        "connector_removals": 1,
    }
    assert _SECRET not in result.output
    for factory in factories:
        factory.assert_not_called()


@pytest.mark.parametrize(
    ("command", "extra_args"),
    [
        pytest.param("plan", ["--out", "reviewed.json"], id="plan"),
        pytest.param("apply", ["--plan", "reviewed.json"], id="apply"),
    ],
)
def test_authorized_workflow_requires_postgres_before_state_or_runtime(
    tmp_path: Path,
    command: str,
    extra_args: list[str],
) -> None:
    _write_project(tmp_path, removal=True)

    result, factories = _invoke_with_runtime_forbidden(
        tmp_path,
        command,
        extra_args,
    )

    assert result.exit_code == 1, result.output
    payload = _payload(result)
    errors = payload["errors"]
    assert isinstance(errors, list)
    assert errors[0]["code"] == "E421_REMOTE_STATE_REQUIRED"
    assert "W106" not in result.output
    for factory in factories:
        factory.assert_not_called()
