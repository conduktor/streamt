"""Keep the published ODCS project example executable and offline-valid."""

from __future__ import annotations

import json
import re
import socket
from pathlib import Path

import pytest

from streamt.core.parser import ProjectParser
from streamt.docs.odcs import generate_odcs_document, validate_odcs_document

PROJECT_ROOT = Path(__file__).resolve().parents[2]
ODCS_REFERENCE = PROJECT_ROOT / "docs" / "reference" / "odcs.md"
ODCS_EXAMPLE_RE = re.compile(
    r"```yaml\n(?P<project># streamt:odcs-example\n.*?)```",
    re.DOTALL,
)


def test_published_project_example_parses_and_generates_valid_odcs_offline(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    reference = ODCS_REFERENCE.read_text(encoding="utf-8")
    matches = list(ODCS_EXAMPLE_RE.finditer(reference))
    assert len(matches) == 1
    (tmp_path / "stream_project.yml").write_text(
        matches[0].group("project"),
        encoding="utf-8",
    )

    def fail_network(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("published ODCS example attempted network access")

    monkeypatch.setattr(socket, "getaddrinfo", fail_network)
    monkeypatch.setattr(socket, "create_connection", fail_network)
    parser_warnings: list[str] = []
    project = ProjectParser(tmp_path, warn_callback=parser_warnings.append).parse()
    export = generate_odcs_document(
        project,
        contract_id="urn:acme:data-contract:payments",
        status="active",
    )

    assert parser_warnings == []
    assert export.warnings == ()
    assert [item["name"] for item in export.document["schema"]] == [
        "payments_raw",
        "payments_clean",
        "warehouse_archive",
    ]
    validate_odcs_document(export.document)
    rendered = json.dumps(export.document, sort_keys=True)
    assert "localhost:9092" not in rendered
    assert "analytics.payments" not in rendered
