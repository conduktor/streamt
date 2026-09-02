"""Focused safety tests for list command row rendering."""

from __future__ import annotations

import json
from types import SimpleNamespace

from click.testing import CliRunner

from streamt.cli import main
from streamt.core.parser import ProjectParser


def _project_with_model(*, name: object, upstream_source: object) -> SimpleNamespace:
    materialized = SimpleNamespace(value="virtual_topic")
    model = SimpleNamespace(
        name=name,
        sql=None,
        from_=[SimpleNamespace(source=upstream_source, ref=None)],
        description=None,
        tags=[],
        get_materialized=lambda: materialized,
    )
    return SimpleNamespace(sources=[], models=[model], tests=[], exposures=[])


def test_list_rejects_malformed_name_with_structured_error(
    tmp_path, monkeypatch
) -> None:
    (tmp_path / "stream_project.yml").write_text("project:\n  name: test\n")
    project = _project_with_model(name=object(), upstream_source="raw")
    monkeypatch.setattr(ProjectParser, "parse", lambda _self: project)

    result = CliRunner().invoke(
        main,
        ["--output", "json", "list", "models", "--project-dir", str(tmp_path)],
    )

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["status"] == "error"
    assert payload["errors"] == [
        {
            "code": "E501_PARSE_ERROR",
            "message": "List item field 'name' must be a string",
        }
    ]


def test_list_rejects_malformed_upstream_before_sorting(tmp_path, monkeypatch) -> None:
    (tmp_path / "stream_project.yml").write_text("project:\n  name: test\n")
    project = _project_with_model(name="clean", upstream_source=7)
    monkeypatch.setattr(ProjectParser, "parse", lambda _self: project)

    result = CliRunner().invoke(
        main,
        [
            "--output",
            "json",
            "list",
            "models",
            "--sort-by",
            "upstream",
            "--project-dir",
            str(tmp_path),
        ],
    )

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["errors"][0]["message"] == (
        "List item field 'upstream' must be a list of strings"
    )
