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
    CONNECTOR_REMOVAL_UNAVAILABLE_DATA,
    CONNECTOR_REMOVAL_UNAVAILABLE_MESSAGE,
)
from streamt.compiler.manifest import Manifest

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
    pytest.param("apply", ["--plan", "reviewed.json"], id="apply-reviewed"),
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
                "clusters": {
                    "primary-connect": {"rest_url": "https://connect.invalid"}
                },
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


def _malformed_manifest() -> Manifest:
    return Manifest(
        version="1.0.0",
        project_name="payments",
        artifacts=cast(
            dict[str, list[dict[str, object]]],
            {"connector_removals": {"endpoint": _SECRET}},
        ),
    )


def _payload(result: Result) -> dict[str, object]:
    payload = json.loads(result.stdout)
    assert isinstance(payload, dict)
    return cast(dict[str, object], payload)


def _invoke_with_runtime_forbidden(
    path: Path,
    command: str,
    extra_args: list[str],
    *,
    malformed_manifest: bool,
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
        if malformed_manifest:
            stack.enter_context(
                patch(
                    "streamt.compiler.Compiler.compile",
                    return_value=_malformed_manifest(),
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
@pytest.mark.parametrize("malformed_manifest", [False, True], ids=["declared", "malformed"])
def test_connector_removal_is_fail_closed_before_state_provider_or_planner(
    tmp_path: Path,
    command: str,
    extra_args: list[str],
    malformed_manifest: bool,
) -> None:
    _write_project(tmp_path, removal=not malformed_manifest)

    result, factories = _invoke_with_runtime_forbidden(
        tmp_path,
        command,
        extra_args,
        malformed_manifest=malformed_manifest,
    )

    assert result.exit_code == 1, result.output
    payload = _payload(result)
    assert payload["errors"] == [
        {
            "code": "E418_REVIEWED_PLAN_REQUIRED",
            "message": CONNECTOR_REMOVAL_UNAVAILABLE_MESSAGE,
            "suggestion": (
                "Remove lifecycle.connector_removals before running plan or apply."
            ),
        }
    ]
    assert payload["data"] == CONNECTOR_REMOVAL_UNAVAILABLE_DATA
    assert _SECRET not in result.output
    errors = payload["errors"]
    assert isinstance(errors, list)
    first_error = errors[0]
    assert isinstance(first_error, dict)
    message = first_error["message"]
    assert isinstance(message, str)
    assert "reviewed" not in message.lower()
    assert "postgres" not in result.output.lower()
    for factory in factories:
        factory.assert_not_called()
