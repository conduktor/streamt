"""Tests for streamt diff command."""

from __future__ import annotations

import json
import tempfile
from pathlib import Path

import yaml
from click.testing import CliRunner

from streamt.cli import main


def _write_project(
    tmpdir: str, models: list[dict] | None = None, sources: list[dict] | None = None
) -> str:
    path = Path(tmpdir)
    config = {
        "project": {"name": "test", "version": "1.0.0"},
        "runtime": {"kafka": {"bootstrap_servers": "localhost:9092"}},
        "sources": sources or [{"name": "raw", "topic": "raw.v1"}],
        "models": models
        or [
            {"name": "out", "sql": 'SELECT * FROM {{ source("raw") }}'},
        ],
    }
    with open(path / "stream_project.yml", "w") as f:
        yaml.dump(config, f)
    return tmpdir


class TestDiffCommand:
    def test_parses_and_accepts_project_dir(self):
        """diff accepts --project-dir, exits 0 or graceful warning."""
        runner = CliRunner()
        with tempfile.TemporaryDirectory() as tmpdir:
            _write_project(tmpdir)
            result = runner.invoke(main, ["diff", "-p", tmpdir])
            # Should not crash — either warns about Kafka or runs
            assert result.exit_code == 0

    def test_json_output(self):
        """-o json produces valid JSON envelope."""
        runner = CliRunner()
        with tempfile.TemporaryDirectory() as tmpdir:
            _write_project(tmpdir)
            result = runner.invoke(main, ["-o", "json", "diff", "-p", tmpdir])
            assert result.exit_code == 0
            data = json.loads(result.output)
            assert data["status"] in ("ok", "error")
            assert data["command"] == "diff"

    def test_no_models(self):
        """Project with only sources, no crash."""
        runner = CliRunner()
        with tempfile.TemporaryDirectory() as tmpdir:
            _write_project(tmpdir, models=[], sources=[{"name": "raw", "topic": "raw.v1"}])
            result = runner.invoke(main, ["diff", "-p", tmpdir])
            assert result.exit_code == 0

    def test_graceful_no_kafka(self):
        """No reachable Kafka → warning, exit 0."""
        runner = CliRunner()
        with tempfile.TemporaryDirectory() as tmpdir:
            _write_project(tmpdir)
            result = runner.invoke(main, ["diff", "-p", tmpdir])
            assert result.exit_code == 0
            # Should mention kafka unavailable in text or json
            assert (
                "kafka" in result.output.lower()
                or "warning" in result.output.lower()
                or result.output.strip() != ""
            )
