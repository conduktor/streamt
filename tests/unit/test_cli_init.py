"""Tests for streamt init command — scaffold mode (no infra needed)."""

import json
import shlex
import socket
import tempfile
from contextlib import ExitStack
from pathlib import Path
from unittest.mock import patch

import pytest
import yaml
from click.testing import CliRunner

from streamt.cli import main


def parse_json_output(output: str) -> dict:
    idx = output.find("{")
    if idx == -1:
        raise ValueError(f"No JSON found in output: {output!r}")
    return json.loads(output[idx:])


class TestInitScaffold:
    """Tests for streamt init (scaffold mode — creates an offline example)."""

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
            assert "flink" not in config["runtime"]
            assert config["sources"][0]["ownership"] == {"mode": "external"}
            assert config["models"][0]["ownership"] == {"mode": "managed"}
            assert config["models"][0]["materialized"] == "topic"
            assert config["models"][0]["sql"] == (
                'SELECT id, event_type, payload, created_at FROM {{ source("raw_events") }}'
            )

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
            assert (Path(tmpdir) / "stream_project.yml").read_text() == "existing: true"
            assert sorted(path.name for path in Path(tmpdir).iterdir()) == ["stream_project.yml"]

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

    def test_init_project_validates_strictly(self):
        """The generated project passes the same strict validation used in CI."""
        runner = CliRunner()
        with tempfile.TemporaryDirectory() as tmpdir:
            result = runner.invoke(main, ["init", "-p", tmpdir])
            assert result.exit_code == 0

            result = runner.invoke(main, ["-o", "json", "validate", "--strict", "-p", tmpdir])
            assert result.exit_code == 0, result.output
            output = parse_json_output(result.output)
            assert output["status"] == "ok"
            assert output["warnings"] == []
            assert output["data"]["valid"] is True

    def test_init_explains_offline_steps_and_execution_prerequisites(self, tmp_path):
        runner = CliRunner()
        # Rich markup and shell metacharacters in paths must survive the printed cd command.
        project_dir = tmp_path / "my [bold] pipeline's"
        result = runner.invoke(
            main,
            ["init", "-p", str(project_dir), "--project-name", "onboarding"],
            env={"COLUMNS": "300"},
        )

        assert result.exit_code == 0, result.output
        assert f"cd {shlex.quote(str(project_dir.resolve()))}" in result.output
        assert "streamt validate --strict" in result.output
        assert "streamt lineage" in result.output
        assert "streamt compile --dry-run" in result.output
        assert "streamt plan --offline" in result.output
        assert "not a live diff" in result.output
        assert "runtime.flink" in result.output
        assert "No Flink runtime is configured" in result.output
        assert "Kafka alone cannot run it" in result.output
        assert "raw_events is external" in result.output
        assert "will not create or seed" in result.output
        assert "do not execute SQL or verify deployment" in result.output

    @pytest.mark.parametrize("existing_project", [False, True])
    def test_init_dry_run_does_not_write(self, tmp_path, existing_project):
        project_dir = tmp_path / "preview"
        if existing_project:
            project_dir.mkdir()
            (project_dir / "stream_project.yml").write_text("existing: true\n")

        result = CliRunner().invoke(main, ["init", "-p", str(project_dir), "--dry-run"])

        assert result.exit_code == 0, result.output
        if existing_project:
            assert (project_dir / "stream_project.yml").read_text() == "existing: true\n"
            assert sorted(path.name for path in project_dir.iterdir()) == ["stream_project.yml"]
        else:
            assert not project_dir.exists()

    @pytest.mark.parametrize("force", [False, True])
    def test_init_preserves_existing_files_in_scaffold_directories(self, tmp_path, force):
        for directory in ("sources", "models", "tests"):
            target = tmp_path / directory
            target.mkdir()
            (target / "user.yml").write_text("user_owned: true\n")
            (target / ".gitkeep").write_text("user-owned marker\n")
        if force:
            (tmp_path / "stream_project.yml").write_text("existing: true\n")

        result = CliRunner().invoke(
            main, ["init", "-p", str(tmp_path), *(["--force"] if force else [])]
        )

        assert result.exit_code == 0, result.output
        for directory in ("sources", "models", "tests"):
            assert (tmp_path / directory / "user.yml").read_text() == "user_owned: true\n"
            assert (tmp_path / directory / ".gitkeep").read_text() == "user-owned marker\n"

    def test_scaffold_offline_journey_never_constructs_runtime_clients(self, tmp_path, monkeypatch):
        def forbidden(*args, **kwargs):
            raise AssertionError("offline scaffold journey attempted runtime access")

        monkeypatch.setattr(socket, "getaddrinfo", forbidden)
        monkeypatch.setattr(socket, "create_connection", forbidden)
        runner = CliRunner()
        commands = (
            ["init"],
            ["validate", "--strict"],
            ["lineage"],
            ["compile", "--dry-run"],
            ["plan", "--offline"],
        )
        outputs = []
        with ExitStack() as stack:
            for target in (
                "streamt.deployer.kafka.KafkaDeployer",
                "streamt.deployer.schema_registry.SchemaRegistryDeployer",
                "streamt.deployer.gateway.GatewayDeployer",
                "streamt.deployer.flink.FlinkDeployer",
                "streamt.deployer.connect.ConnectDeployer",
                "streamt.deployer.state_backend.DeploymentStateService",
            ):
                stack.enter_context(patch(target, side_effect=forbidden))
            for command in commands:
                result = runner.invoke(main, ["-o", "json", *command, "-p", str(tmp_path)])
                assert result.exit_code == 0, result.output
                outputs.append(parse_json_output(result.output))

        assert outputs[1]["warnings"] == []
        assert outputs[3]["data"]["artifacts"]["topics"] == ["events_clean"]
        assert outputs[3]["data"]["artifacts"]["flink_jobs"] == ["events_clean_processor"]
        assert not (tmp_path / "generated").exists()
        assert not (tmp_path / ".streamt").exists()

    def test_strict_validation_still_rejects_user_added_select_star(self, tmp_path):
        runner = CliRunner()
        result = runner.invoke(main, ["init", "-p", str(tmp_path)])
        assert result.exit_code == 0, result.output
        project_file = tmp_path / "stream_project.yml"
        config = yaml.safe_load(project_file.read_text())
        config["models"][0]["sql"] = 'SELECT * FROM {{ source("raw_events") }}'
        project_file.write_text(yaml.safe_dump(config))

        result = runner.invoke(main, ["validate", "--strict", "-p", str(tmp_path)])

        assert result.exit_code == 1, result.output
        assert "SELECT *" in result.output
        assert "treated as errors" in result.output
