"""Keep the published OpenLineage project and CLI example executable offline."""

from __future__ import annotations

import json
import re
import socket
from pathlib import Path

import pytest
from click.testing import CliRunner

from streamt.cli import main
from streamt.integrations.openlineage import validate_event

DOC_PATH = Path(__file__).parents[2] / "docs" / "reference" / "openlineage.md"
PROJECT_EXAMPLE = re.compile(
    r"```yaml\n(?P<project># streamt:openlineage-example\n.*?)```",
    re.DOTALL,
)


def test_published_project_and_cli_example_export_valid_openlineage_offline(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    match = PROJECT_EXAMPLE.search(DOC_PATH.read_text(encoding="utf-8"))
    assert match is not None, "published OpenLineage project example is missing"
    (tmp_path / "stream_project.yml").write_text(
        match.group("project"),
        encoding="utf-8",
    )

    def fail_network(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("published OpenLineage example attempted network access")

    monkeypatch.setattr(socket, "getaddrinfo", fail_network)
    monkeypatch.setattr(socket, "create_connection", fail_network)
    target = tmp_path / "events" / "payments.openlineage.jsonl"
    result = CliRunner().invoke(
        main,
        [
            "docs",
            "openlineage",
            "--project-dir",
            str(tmp_path),
            "--job-namespace",
            "https://lineage.example/namespaces/prod",
            "--output-file",
            str(target),
        ],
    )

    assert result.exit_code == 0, result.output
    assert result.stderr == ""
    events = [json.loads(line) for line in target.read_text(encoding="utf-8").splitlines()]
    assert len(events) == 3
    assert [event["dataset"]["name"] for event in events if "dataset" in event] == [
        "payments.clean.v1",
        "payments.raw.v1",
    ]
    assert [event["job"]["name"] for event in events if "job" in event] == [
        "streamt/payments-streams/models/payments_clean"
    ]
    assert {
        event["dataset"]["namespace"] for event in events if "dataset" in event
    } == {"kafka://broker.example:9092"}
    for event in events:
        validate_event(event)
    assert not (tmp_path / "generated").exists()
