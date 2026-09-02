"""Early apply authorization for explicit Gateway lifecycle removals."""

from __future__ import annotations

import json
from contextlib import ExitStack
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import yaml
from click.testing import CliRunner, Result

from streamt.cli import main
from streamt.cli.commands.apply import _SELECTABLE_ARTIFACT_KINDS

_RUNTIME_FACTORIES = (
    "make_deployment_state_service",
    "make_sr_deployer",
    "make_kafka_deployer",
    "make_flink_deployer",
    "make_connect_deployer",
    "make_gateway_deployer",
)


def _write_project(path: Path, *, removal: bool) -> None:
    config: dict[str, object] = {
        "apiVersion": "streamt.dev/v1alpha1",
        "project": {"name": "payments", "version": "1.0.0"},
        "runtime": {"kafka": {"bootstrap_servers": "broker.invalid:9092"}},
    }
    if removal:
        config["lifecycle"] = {
            "gateway_rule_removals": [
                {
                    "logical_owner": "orders_view",
                    "prior_artifact": {
                        "name": "orders_rule",
                        "virtualTopic": "orders.public",
                        "physicalTopic": "raw.orders",
                        "interceptors": [],
                    },
                }
            ]
        }
    (path / "stream_project.yml").write_text(
        yaml.safe_dump(config, sort_keys=False),
        encoding="utf-8",
    )


def _payload(result: Result) -> dict[str, object]:
    return json.loads(result.stdout)


def _invoke_with_runtime_forbidden(
    path: Path,
    extra_args: list[str],
) -> tuple[Result, list[MagicMock]]:
    with ExitStack() as stack:
        factories = [
            stack.enter_context(
                patch(
                    f"streamt.cli.commands.apply.{name}",
                    side_effect=AssertionError(
                        f"{name} was called before Gateway removal authorization"
                    ),
                )
            )
            for name in _RUNTIME_FACTORIES
        ]
        result = CliRunner().invoke(
            main,
            ["-o", "json", "apply", "-p", str(path), *extra_args],
        )
    return result, factories


@pytest.mark.parametrize(
    "extra_args",
    [
        [],
        ["--force"],
        ["--dry-run"],
        ["--dry-run", "--force"],
    ],
)
def test_gateway_removal_requires_reviewed_plan_before_runtime_setup(
    tmp_path: Path,
    extra_args: list[str],
) -> None:
    _write_project(tmp_path, removal=True)

    result, factories = _invoke_with_runtime_forbidden(tmp_path, extra_args)

    assert result.exit_code == 1, result.output
    payload = _payload(result)
    assert payload["errors"][0]["code"] == "E418_REVIEWED_PLAN_REQUIRED"
    assert "cannot be applied directly" in payload["errors"][0]["message"]
    assert payload["data"]["policy"] == "gateway_rule_removal"
    assert payload["data"]["gateway_rule_removals"] == 1
    plan_command, apply_command = payload["data"]["next_steps"]
    assert "streamt plan" in plan_command
    assert "--out" in plan_command
    assert "streamt apply" in apply_command
    assert "--plan" in apply_command
    assert "--force" not in apply_command
    for factory in factories:
        factory.assert_not_called()


@pytest.mark.parametrize(
    "extra_args",
    [
        ["--target", "orders_view"],
        ["--select", "tag:critical"],
    ],
)
def test_gateway_removal_rejects_partial_selection_before_runtime_or_selection(
    tmp_path: Path,
    extra_args: list[str],
) -> None:
    _write_project(tmp_path, removal=True)

    result, factories = _invoke_with_runtime_forbidden(tmp_path, extra_args)

    assert result.exit_code == 1, result.output
    payload = _payload(result)
    assert payload["errors"][0]["code"] == "E418_REVIEWED_PLAN_REQUIRED"
    assert "cannot be combined with --target or --select" in payload["errors"][0][
        "message"
    ]
    assert payload["data"]["required_workflow"] == "reviewed_plan"
    assert "gateway_rule_removals" not in _SELECTABLE_ARTIFACT_KINDS
    for factory in factories:
        factory.assert_not_called()


def test_apply_without_gateway_removal_keeps_existing_dry_run_path(
    tmp_path: Path,
) -> None:
    _write_project(tmp_path, removal=False)
    kafka = MagicMock()
    with (
        patch("streamt.cli.commands.apply.make_sr_deployer", return_value=None) as make_sr,
        patch(
            "streamt.cli.commands.apply.make_kafka_deployer",
            return_value=kafka,
        ) as make_kafka,
        patch(
            "streamt.cli.commands.apply.make_flink_deployer",
            return_value=None,
        ) as make_flink,
        patch(
            "streamt.cli.commands.apply.make_connect_deployer",
            return_value=None,
        ) as make_connect,
        patch(
            "streamt.cli.commands.apply.make_gateway_deployer",
            return_value=None,
        ) as make_gateway,
    ):
        result = CliRunner().invoke(
            main,
            ["-o", "json", "apply", "-p", str(tmp_path), "--dry-run"],
        )

    assert result.exit_code == 0, result.output
    payload = _payload(result)
    assert payload["data"]["dry_run"] is True
    assert payload["data"]["has_changes"] is False
    for factory in (make_sr, make_kafka, make_flink, make_connect, make_gateway):
        factory.assert_called_once()
