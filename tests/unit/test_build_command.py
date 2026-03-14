"""Tests for streamt build command."""

from __future__ import annotations

import json
import tempfile
from pathlib import Path

import yaml
from click.testing import CliRunner

from streamt.cli import main


def _write_project(tmpdir: str) -> None:
    config = {
        "project": {"name": "test", "version": "1.0.0"},
        "runtime": {
            "kafka": {"bootstrap_servers": "localhost:9092"},
            "flink": {
                "default": "local",
                "clusters": {"local": {"type": "rest", "rest_url": "http://localhost:8082"}},
            },
        },
        "sources": [{"name": "raw", "topic": "raw.v1", "columns": [{"name": "id"}]}],
        "models": [{"name": "out", "sql": 'SELECT id FROM {{ source("raw") }}'}],
    }
    with open(Path(tmpdir) / "stream_project.yml", "w") as f:
        yaml.dump(config, f)


class TestBuildCommand:
    def test_produces_artifacts(self):
        runner = CliRunner()
        with tempfile.TemporaryDirectory() as tmpdir:
            _write_project(tmpdir)
            out_dir = str(Path(tmpdir) / "build")
            result = runner.invoke(main, ["build", "-p", tmpdir, "--output-dir", out_dir])
            assert result.exit_code == 0
            assert Path(out_dir, "manifest.json").exists()
            assert Path(out_dir, "checksums.sha256").exists()
            manifest = json.loads(Path(out_dir, "manifest.json").read_text())
            assert manifest["project"] == "test"

    def test_json_output(self):
        runner = CliRunner()
        with tempfile.TemporaryDirectory() as tmpdir:
            _write_project(tmpdir)
            result = runner.invoke(
                main,
                ["-o", "json", "build", "-p", tmpdir, "--output-dir", str(Path(tmpdir) / "build")],
            )
            assert result.exit_code == 0
            data = json.loads(result.output)
            assert data["command"] == "build"
            assert "files" in data["data"]

    def test_checksums_valid(self):
        runner = CliRunner()
        with tempfile.TemporaryDirectory() as tmpdir:
            _write_project(tmpdir)
            out_dir = Path(tmpdir) / "build"
            runner.invoke(main, ["build", "-p", tmpdir, "--output-dir", str(out_dir)])
            lines = (out_dir / "checksums.sha256").read_text().strip().split("\n")
            assert len(lines) > 0
            for line in lines:
                digest, name = line.split("  ", 1)
                assert len(digest) == 64  # sha256 hex
                assert (out_dir / name).exists()
