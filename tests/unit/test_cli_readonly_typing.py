"""Regression tests for typed read-only CLI payload boundaries."""

import json
from unittest.mock import patch

import yaml
from click.testing import CliRunner

from streamt.cli import main
from streamt.cli.commands.test import _normalize_test_errors


def test_malformed_test_runner_errors_payload_has_stable_diagnostic() -> None:
    """A non-list runner payload must not crash while rendering failures."""
    assert _normalize_test_errors(42) == ["Malformed test result: 'errors' must be a list"]


def test_valid_test_runner_errors_are_preserved() -> None:
    assert _normalize_test_errors(["first", "second"]) == ["first", "second"]


def test_test_command_reports_malformed_runner_errors_without_crashing(tmp_path) -> None:
    config = {
        "project": {"name": "test"},
        "runtime": {"kafka": {"bootstrap_servers": "localhost:9092"}},
        "sources": [{"name": "raw", "topic": "raw.v1"}],
        "tests": [{"name": "quality", "model": "raw", "type": "schema", "assertions": []}],
    }
    (tmp_path / "stream_project.yml").write_text(yaml.safe_dump(config))

    with patch(
        "streamt.testing.TestRunner.run",
        return_value=[{"name": "quality", "status": "failed", "errors": 42}],
    ):
        result = CliRunner().invoke(main, ["-o", "json", "test", "-p", str(tmp_path)])

    assert result.exit_code == 1
    payload = json.loads(result.output)
    assert payload["data"]["results"][0]["errors"] == [
        "Malformed test result: 'errors' must be a list"
    ]
