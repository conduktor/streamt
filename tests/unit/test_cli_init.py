"""Tests for streamt init command — scaffold mode (no infra needed)."""

import json
import os
import tempfile
from pathlib import Path

import yaml
from click.testing import CliRunner

from streamt.cli import main


def parse_json_output(output: str) -> dict:
    idx = output.find("{")
    if idx == -1:
        raise ValueError(f"No JSON found in output: {output!r}")
    return json.loads(output[idx:])


class TestInitScaffold:
    """Tests for streamt init (scaffold mode — creates empty project)."""

    def test_init_creates_project_structure(self):
        """init creates stream_project.yml and standard directories."""
        runner = CliRunner()
        with tempfile.TemporaryDirectory() as tmpdir:
            result = runner.invoke(main, ["init", "-p", tmpdir])

            assert result.exit_code == 0, result.output

            project_file = Path(tmpdir) / "stream_project.yml"
            assert project_file.exists()

            config = yaml.safe_load(project_file.read_text())
            assert config["project"]["name"] is not None
            assert config["project"]["version"] == "1.0.0"
            assert "runtime" in config

            assert (Path(tmpdir) / "sources").is_dir()
            assert (Path(tmpdir) / "models").is_dir()
            assert (Path(tmpdir) / "tests").is_dir()

    def test_init_uses_custom_project_name(self):
        """init --project-name sets the project name."""
        runner = CliRunner()
        with tempfile.TemporaryDirectory() as tmpdir:
            result = runner.invoke(main, ["init", "-p", tmpdir, "--project-name", "my-pipeline"])

            assert result.exit_code == 0, result.output

            config = yaml.safe_load((Path(tmpdir) / "stream_project.yml").read_text())
            assert config["project"]["name"] == "my-pipeline"

    def test_init_uses_directory_name_as_default(self):
        """init without --project-name uses the directory name."""
        runner = CliRunner()
        with tempfile.TemporaryDirectory() as tmpdir:
            project_dir = Path(tmpdir) / "cool-pipeline"
            project_dir.mkdir()
            result = runner.invoke(main, ["init", "-p", str(project_dir)])

            assert result.exit_code == 0, result.output

            config = yaml.safe_load((project_dir / "stream_project.yml").read_text())
            assert config["project"]["name"] == "cool-pipeline"

    def test_init_fails_if_project_exists(self):
        """init errors when stream_project.yml already exists."""
        runner = CliRunner()
        with tempfile.TemporaryDirectory() as tmpdir:
            (Path(tmpdir) / "stream_project.yml").write_text("existing: true")

            result = runner.invoke(main, ["init", "-p", tmpdir])

            assert result.exit_code == 1
            assert "already exists" in result.output.lower()

    def test_init_force_overwrites(self):
        """init --force overwrites existing project."""
        runner = CliRunner()
        with tempfile.TemporaryDirectory() as tmpdir:
            (Path(tmpdir) / "stream_project.yml").write_text("existing: true")

            result = runner.invoke(main, ["init", "-p", tmpdir, "--force"])

            assert result.exit_code == 0, result.output

            config = yaml.safe_load((Path(tmpdir) / "stream_project.yml").read_text())
            assert "project" in config

    def test_init_json_output(self):
        """init -o json returns structured output."""
        runner = CliRunner()
        with tempfile.TemporaryDirectory() as tmpdir:
            result = runner.invoke(main, ["-o", "json", "init", "-p", tmpdir])

            assert result.exit_code == 0, result.output
            data = parse_json_output(result.output)
            assert data["status"] == "ok"
            assert data["command"] == "init"
            assert "created_files" in data["data"]
            assert any("stream_project.yml" in f for f in data["data"]["created_files"])

    def test_init_creates_gitkeep_in_empty_dirs(self):
        """init creates .gitkeep in empty directories so git tracks them."""
        runner = CliRunner()
        with tempfile.TemporaryDirectory() as tmpdir:
            result = runner.invoke(main, ["init", "-p", tmpdir])

            assert result.exit_code == 0
            assert (Path(tmpdir) / "sources" / ".gitkeep").exists()
            assert (Path(tmpdir) / "models" / ".gitkeep").exists()
            assert (Path(tmpdir) / "tests" / ".gitkeep").exists()

    def test_init_project_validates(self):
        """The generated project passes streamt validate."""
        runner = CliRunner()
        with tempfile.TemporaryDirectory() as tmpdir:
            result = runner.invoke(main, ["init", "-p", tmpdir])
            assert result.exit_code == 0

            result = runner.invoke(main, ["validate", "-p", tmpdir])
            assert result.exit_code == 0, result.output
