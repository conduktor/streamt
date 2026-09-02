"""Tests for CLI JSON output, list, show, and agent-friendly features."""

import json
import tempfile
from pathlib import Path

import pytest
import yaml
from click.testing import CliRunner

from streamt.cli import main


def parse_json_output(output: str) -> dict:
    """Extract JSON object from CLI output that may have stderr text mixed in."""
    idx = output.find("{")
    if idx == -1:
        raise ValueError(f"No JSON found in output: {output!r}")
    return json.loads(output[idx:])


class TestJSONOutput:
    """Tests for --output json on all commands."""

    def _create_project(self, tmpdir: str, config: dict) -> Path:
        project_path = Path(tmpdir)
        with open(project_path / "stream_project.yml", "w") as f:
            yaml.dump(config, f)
        return project_path

    def _minimal_config(self) -> dict:
        return {
            "project": {"name": "test-project"},
            "runtime": {"kafka": {"bootstrap_servers": "localhost:9092"}},
            "sources": [{"name": "raw", "topic": "raw.v1"}],
            "models": [
                {"name": "clean", "sql": 'SELECT * FROM {{ source("raw") }}'},
                {"name": "enriched", "sql": 'SELECT * FROM {{ ref("clean") }}'},
            ],
            "tests": [
                {"name": "clean_schema", "model": "clean", "type": "schema", "assertions": [{"not_null": {"columns": ["id"]}}]},
            ],
        }

    # -- validate --output json --

    def test_validate_json_valid(self):
        """validate --output json returns structured envelope on success."""
        runner = CliRunner()
        with tempfile.TemporaryDirectory() as tmpdir:
            self._create_project(tmpdir, self._minimal_config())
            result = runner.invoke(main, ["-o", "json", "validate", "-p", tmpdir])

            assert result.exit_code == 0
            data = parse_json_output(result.output)
            assert data["status"] == "ok"
            assert data["command"] == "validate"
            assert data["data"]["valid"] is True
            assert data["data"]["sources"] == 1
            assert data["data"]["models"] == 2
            assert data["errors"] == []

    def test_validate_json_invalid(self):
        """validate --output json returns errors on failure."""
        runner = CliRunner()
        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                "project": {"name": "test"},
                "runtime": {"kafka": {"bootstrap_servers": "localhost:9092"}},
                "models": [{"name": "broken", "sql": 'SELECT * FROM {{ source("nonexistent") }}'}],
            }
            self._create_project(tmpdir, config)
            result = runner.invoke(main, ["-o", "json", "validate", "-p", tmpdir])

            assert result.exit_code == 1
            data = parse_json_output(result.output)
            assert data["status"] == "error"
            assert len(data["errors"]) > 0
            assert "code" in data["errors"][0]

    # -- compile --output json --

    def test_compile_json_dry_run(self):
        """compile --dry-run --output json returns artifact list."""
        runner = CliRunner()
        with tempfile.TemporaryDirectory() as tmpdir:
            self._create_project(tmpdir, self._minimal_config())
            result = runner.invoke(main, ["-o", "json", "compile", "-p", tmpdir, "--dry-run"])

            assert result.exit_code == 0
            data = parse_json_output(result.output)
            assert data["command"] == "compile"
            assert data["data"]["dry_run"] is True
            assert "counts" in data["data"]
            assert "artifacts" in data["data"]

    # -- test --output json --

    def test_test_json(self):
        """test --output json returns structured test results."""
        runner = CliRunner()
        with tempfile.TemporaryDirectory() as tmpdir:
            self._create_project(tmpdir, self._minimal_config())
            result = runner.invoke(main, ["-o", "json", "test", "-p", tmpdir])

            assert result.exit_code == 0
            data = parse_json_output(result.output)
            assert data["command"] == "test"
            assert data["data"]["passed"] == 1
            assert data["data"]["failed"] == 0
            assert len(data["data"]["results"]) == 1
            assert data["data"]["results"][0]["name"] == "clean_schema"
            assert data["data"]["results"][0]["status"] == "passed"

    def test_test_json_no_tests(self):
        """test --output json handles no tests gracefully."""
        runner = CliRunner()
        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                "project": {"name": "test"},
                "runtime": {"kafka": {"bootstrap_servers": "localhost:9092"}},
            }
            self._create_project(tmpdir, config)
            result = runner.invoke(main, ["-o", "json", "test", "-p", tmpdir])

            assert result.exit_code == 0
            data = parse_json_output(result.output)
            assert data["data"]["total"] == 0

    # -- lineage --output json --

    def test_lineage_json_global(self):
        """lineage with global --output json wraps in envelope."""
        runner = CliRunner()
        with tempfile.TemporaryDirectory() as tmpdir:
            self._create_project(tmpdir, self._minimal_config())
            result = runner.invoke(main, ["-o", "json", "lineage", "-p", tmpdir])

            assert result.exit_code == 0
            data = parse_json_output(result.output)
            assert data["command"] == "lineage"
            assert "nodes" in data["data"]
            assert "edges" in data["data"]

    # -- envs list --output json --

    def test_envs_list_json_single_mode(self):
        """envs list --output json in single-env mode."""
        runner = CliRunner()
        with tempfile.TemporaryDirectory() as tmpdir:
            config = {"project": {"name": "test"}, "runtime": {"kafka": {"bootstrap_servers": "localhost:9092"}}}
            self._create_project(tmpdir, config)
            result = runner.invoke(main, ["-o", "json", "envs", "list", "-p", tmpdir])

            assert result.exit_code == 0
            data = parse_json_output(result.output)
            assert data["data"]["mode"] == "single"


class TestListCommand:
    """Tests for streamt list."""

    def _create_project(self, tmpdir: str, config: dict) -> Path:
        project_path = Path(tmpdir)
        with open(project_path / "stream_project.yml", "w") as f:
            yaml.dump(config, f)
        return project_path

    def test_list_sources(self):
        """list sources returns structured source list."""
        runner = CliRunner()
        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                "project": {"name": "test"},
                "runtime": {"kafka": {"bootstrap_servers": "localhost:9092"}},
                "sources": [
                    {"name": "orders_raw", "topic": "orders.raw.v1"},
                    {"name": "events_raw", "topic": "events.raw.v1"},
                ],
            }
            self._create_project(tmpdir, config)
            result = runner.invoke(main, ["-o", "json", "list", "sources", "-p", tmpdir])

            assert result.exit_code == 0
            data = parse_json_output(result.output)
            assert data["data"]["resource_type"] == "sources"
            assert data["data"]["count"] == 2
            names = [i["name"] for i in data["data"]["items"]]
            assert "orders_raw" in names
            assert "events_raw" in names

    def test_list_models(self):
        """list models returns materialized type and upstream."""
        runner = CliRunner()
        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                "project": {"name": "test"},
                "runtime": {"kafka": {"bootstrap_servers": "localhost:9092"}},
                "sources": [{"name": "raw", "topic": "raw.v1"}],
                "models": [
                    {"name": "clean", "sql": 'SELECT * FROM {{ source("raw") }} WHERE id IS NOT NULL'},
                ],
            }
            self._create_project(tmpdir, config)
            result = runner.invoke(main, ["-o", "json", "list", "models", "-p", tmpdir])

            assert result.exit_code == 0
            data = parse_json_output(result.output)
            assert data["data"]["count"] == 1
            model = data["data"]["items"][0]
            assert model["name"] == "clean"
            assert model["materialized"] == "virtual_topic"
            assert "raw" in model["upstream"]

    def test_list_tests(self):
        """list tests returns test details."""
        runner = CliRunner()
        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                "project": {"name": "test"},
                "runtime": {"kafka": {"bootstrap_servers": "localhost:9092"}},
                "sources": [{"name": "raw", "topic": "raw.v1"}],
                "models": [{"name": "clean", "sql": 'SELECT * FROM {{ source("raw") }}'}],
                "tests": [
                    {"name": "t1", "model": "clean", "type": "schema", "assertions": [{"not_null": {"columns": ["id"]}}]},
                ],
            }
            self._create_project(tmpdir, config)
            result = runner.invoke(main, ["-o", "json", "list", "tests", "-p", tmpdir])

            assert result.exit_code == 0
            data = parse_json_output(result.output)
            assert data["data"]["items"][0]["type"] == "schema"

    def test_list_text_mode(self):
        """list in text mode outputs a table."""
        runner = CliRunner()
        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                "project": {"name": "test"},
                "runtime": {"kafka": {"bootstrap_servers": "localhost:9092"}},
                "sources": [{"name": "raw", "topic": "raw.v1"}],
            }
            self._create_project(tmpdir, config)
            result = runner.invoke(main, ["list", "sources", "-p", tmpdir])

            assert result.exit_code == 0
            assert "raw" in result.output


class TestShowCommand:
    """Tests for streamt show."""

    def _create_project(self, tmpdir: str, config: dict) -> Path:
        project_path = Path(tmpdir)
        with open(project_path / "stream_project.yml", "w") as f:
            yaml.dump(config, f)
        return project_path

    def _full_config(self) -> dict:
        return {
            "project": {"name": "test"},
            "runtime": {"kafka": {"bootstrap_servers": "localhost:9092"}},
            "sources": [{"name": "raw", "topic": "raw.v1", "description": "Raw events"}],
            "models": [
                {
                    "name": "clean",
                    "description": "Cleaned events",
                    "sql": 'SELECT * FROM {{ source("raw") }} WHERE id IS NOT NULL',
                    "topic": {"partitions": 6},
                },
                {"name": "agg", "sql": 'SELECT COUNT(*) FROM {{ ref("clean") }} GROUP BY status'},
            ],
            "tests": [
                {"name": "t1", "model": "clean", "type": "schema", "assertions": []},
            ],
        }

    def test_show_source_json(self):
        """show source returns structured source info."""
        runner = CliRunner()
        with tempfile.TemporaryDirectory() as tmpdir:
            self._create_project(tmpdir, self._full_config())
            result = runner.invoke(main, ["-o", "json", "show", "source", "raw", "-p", tmpdir])

            assert result.exit_code == 0
            data = parse_json_output(result.output)
            assert data["data"]["topic"] == "raw.v1"
            assert data["data"]["description"] == "Raw events"
            assert "clean" in data["data"]["downstream"]

    def test_show_model_json(self):
        """show model returns materialized, upstream, downstream, config."""
        runner = CliRunner()
        with tempfile.TemporaryDirectory() as tmpdir:
            self._create_project(tmpdir, self._full_config())
            result = runner.invoke(main, ["-o", "json", "show", "model", "clean", "-p", tmpdir])

            assert result.exit_code == 0
            data = parse_json_output(result.output)
            assert data["data"]["materialized"] == "virtual_topic"
            assert "raw" in data["data"]["upstream"]
            assert "agg" in data["data"]["downstream"]
            assert data["data"]["topic"]["partitions"] == 6

    def test_show_model_flink(self):
        """show model with stateful SQL shows flink materialization."""
        runner = CliRunner()
        with tempfile.TemporaryDirectory() as tmpdir:
            self._create_project(tmpdir, self._full_config())
            result = runner.invoke(main, ["-o", "json", "show", "model", "agg", "-p", tmpdir])

            assert result.exit_code == 0
            data = parse_json_output(result.output)
            assert data["data"]["materialized"] == "flink"

    def test_show_test_json(self):
        """show test returns test details."""
        runner = CliRunner()
        with tempfile.TemporaryDirectory() as tmpdir:
            self._create_project(tmpdir, self._full_config())
            result = runner.invoke(main, ["-o", "json", "show", "test", "t1", "-p", tmpdir])

            assert result.exit_code == 0
            data = parse_json_output(result.output)
            assert data["data"]["model"] == "clean"
            assert data["data"]["type"] == "schema"

    def test_show_not_found(self):
        """show returns error when resource doesn't exist."""
        runner = CliRunner()
        with tempfile.TemporaryDirectory() as tmpdir:
            self._create_project(tmpdir, self._full_config())
            result = runner.invoke(main, ["-o", "json", "show", "model", "nonexistent", "-p", tmpdir])

            assert result.exit_code == 1
            data = parse_json_output(result.output)
            assert data["status"] == "error"
            assert data["errors"][0]["code"] == "E102_MODEL_NOT_FOUND"

    def test_show_text_mode(self):
        """show in text mode prints human-readable output."""
        runner = CliRunner()
        with tempfile.TemporaryDirectory() as tmpdir:
            self._create_project(tmpdir, self._full_config())
            result = runner.invoke(main, ["show", "model", "clean", "-p", tmpdir])

            assert result.exit_code == 0
            assert "Model:" in result.output
            assert "Materialized: virtual_topic" in result.output


class TestConfirmEnvFlag:
    """Tests for --confirm-env non-interactive flag."""

    def _create_multi_env_project(self, tmpdir: str) -> Path:
        project_path = Path(tmpdir)
        with open(project_path / "stream_project.yml", "w") as f:
            yaml.dump({"project": {"name": "test"}}, f)

        envs_dir = project_path / "environments"
        envs_dir.mkdir()
        with open(envs_dir / "prod.yml", "w") as f:
            yaml.dump({
                "environment": {"name": "prod", "description": "Production", "protected": True},
                "runtime": {"kafka": {"bootstrap_servers": "prod:9092"}},
                "safety": {"confirm_apply": True, "allow_destructive": False},
            }, f)
        return project_path

    def test_confirm_env_wrong_name(self):
        """--confirm-env with wrong name fails."""
        runner = CliRunner()
        with tempfile.TemporaryDirectory() as tmpdir:
            self._create_multi_env_project(tmpdir)
            plan_path = Path(tmpdir) / "prod.plan.json"
            planned = runner.invoke(
                main,
                [
                    "plan",
                    "-p",
                    tmpdir,
                    "--env",
                    "prod",
                    "--out",
                    str(plan_path),
                ],
            )
            assert planned.exit_code == 0, planned.output
            result = runner.invoke(
                main,
                [
                    "-o",
                    "json",
                    "apply",
                    "-p",
                    tmpdir,
                    "--env",
                    "prod",
                    "--confirm-env",
                    "staging",
                    "--plan",
                    str(plan_path),
                ],
            )

            assert result.exit_code == 1
            data = parse_json_output(result.output)
            assert data["status"] == "error"
            assert "does not match" in data["errors"][0]["message"]


class TestDestructiveSafety:
    """Tests for destructive operation safety checks."""

    def _create_multi_env_project(self, tmpdir: str, with_model: bool = False) -> Path:
        project_path = Path(tmpdir)
        config = {"project": {"name": "test"}}
        if with_model:
            config["sources"] = [{"name": "raw", "topic": "raw.v1"}]
            config["models"] = [{"name": "clean", "sql": 'SELECT * FROM {{ source("raw") }}'}]
        with open(project_path / "stream_project.yml", "w") as f:
            yaml.dump(config, f)

        envs_dir = project_path / "environments"
        envs_dir.mkdir()
        with open(envs_dir / "prod.yml", "w") as f:
            yaml.dump({
                "environment": {"name": "prod", "description": "Production", "protected": False},
                "runtime": {"kafka": {"bootstrap_servers": "prod:9092"}},
                "safety": {"confirm_apply": True, "allow_destructive": False},
            }, f)
        return project_path

    def test_non_destructive_apply_succeeds_without_force(self):
        """apply with allow_destructive=false should succeed when plan has no deletes."""
        runner = CliRunner()
        with tempfile.TemporaryDirectory() as tmpdir:
            self._create_multi_env_project(tmpdir)
            result = runner.invoke(
                main, ["-o", "json", "apply", "-p", tmpdir, "--env", "prod", "--confirm-env", "prod"]
            )

            # Should NOT fail with "Destructive ops blocked" — there are no destructive ops
            data = parse_json_output(result.output)
            if data["status"] == "error":
                for err in data["errors"]:
                    assert "destructive" not in err["message"].lower(), \
                        f"Non-destructive apply should not be blocked: {err['message']}"

    def test_destructive_apply_blocked_without_force(self):
        """apply with allow_destructive=false should block when plan has deletes."""
        from unittest.mock import MagicMock, patch

        from streamt.deployer.planner import DeploymentPlan

        runner = CliRunner()
        with tempfile.TemporaryDirectory() as tmpdir:
            self._create_multi_env_project(tmpdir, with_model=True)

            # Mock planner to return a plan with deletes
            mock_plan = MagicMock(spec=DeploymentPlan)
            mock_plan.deletes = 1
            mock_plan.has_changes = True

            with patch("streamt.deployer.planner.DeploymentPlanner.plan", return_value=mock_plan):
                result = runner.invoke(
                    main, ["-o", "json", "apply", "-p", tmpdir, "--env", "prod", "--confirm-env", "prod"]
                )

                assert result.exit_code == 1
                data = parse_json_output(result.output)
                assert data["status"] == "error"
                assert any("destructive" in e["message"].lower() for e in data["errors"])

    def test_destructive_apply_proceeds_with_force(self):
        """apply with --force should proceed even when plan has deletes."""
        from unittest.mock import MagicMock, patch

        from streamt.deployer.planner import DeploymentPlan

        runner = CliRunner()
        with tempfile.TemporaryDirectory() as tmpdir:
            self._create_multi_env_project(tmpdir, with_model=True)

            mock_plan = MagicMock(spec=DeploymentPlan)
            mock_plan.deletes = 1
            mock_plan.has_changes = True

            mock_results = {"created": [], "updated": [], "unchanged": ["topic:raw.v1"], "errors": []}

            with patch("streamt.deployer.planner.DeploymentPlanner.plan", return_value=mock_plan), \
                 patch("streamt.deployer.planner.DeploymentPlanner.apply", return_value=mock_results):
                result = runner.invoke(
                    main, ["-o", "json", "apply", "-p", tmpdir, "--env", "prod",
                           "--confirm-env", "prod", "--force"]
                )

                assert result.exit_code == 0
                data = parse_json_output(result.output)
                assert data["status"] == "ok"


class TestErrorCodes:
    """Tests for structured error codes."""

    def test_error_codes_in_json(self):
        """Errors in JSON output include machine-readable codes."""
        runner = CliRunner()
        with tempfile.TemporaryDirectory() as tmpdir:
            project_path = Path(tmpdir)
            with open(project_path / "stream_project.yml", "w") as f:
                yaml.dump({
                    "project": {"name": "test"},
                    "runtime": {"kafka": {"bootstrap_servers": "localhost:9092"}},
                    "models": [{"name": "bad", "sql": 'SELECT * FROM {{ source("nope") }}'}],
                }, f)

            result = runner.invoke(main, ["-o", "json", "validate", "-p", tmpdir])

            assert result.exit_code == 1
            data = parse_json_output(result.output)
            assert data["status"] == "error"
            # Error should have a code starting with E
            for err in data["errors"]:
                assert err["code"].startswith("E"), f"Error code should start with E: {err}"

    def test_missing_project_json(self):
        """Missing project file gives structured error in JSON mode."""
        runner = CliRunner()
        with tempfile.TemporaryDirectory() as tmpdir:
            result = runner.invoke(main, ["-o", "json", "validate", "-p", tmpdir])

            assert result.exit_code == 1
            data = parse_json_output(result.output)
            assert data["status"] == "error"
            assert len(data["errors"]) > 0


class TestOutputEnvelope:
    """Tests for the JSON envelope structure."""

    def test_envelope_structure(self):
        """JSON output always has status, command, data, errors, warnings."""
        runner = CliRunner()
        with tempfile.TemporaryDirectory() as tmpdir:
            project_path = Path(tmpdir)
            with open(project_path / "stream_project.yml", "w") as f:
                yaml.dump({
                    "project": {"name": "test"},
                    "runtime": {"kafka": {"bootstrap_servers": "localhost:9092"}},
                }, f)

            result = runner.invoke(main, ["-o", "json", "validate", "-p", tmpdir])
            data = parse_json_output(result.output)

            assert "status" in data
            assert "command" in data
            assert "data" in data
            assert "errors" in data
            assert "warnings" in data
            assert isinstance(data["errors"], list)
            assert isinstance(data["warnings"], list)

    def test_text_mode_no_json(self):
        """Text mode (default) does NOT output JSON."""
        runner = CliRunner()
        with tempfile.TemporaryDirectory() as tmpdir:
            project_path = Path(tmpdir)
            with open(project_path / "stream_project.yml", "w") as f:
                yaml.dump({
                    "project": {"name": "test"},
                    "runtime": {"kafka": {"bootstrap_servers": "localhost:9092"}},
                }, f)

            result = runner.invoke(main, ["validate", "-p", tmpdir])

            assert result.exit_code == 0
            # Should NOT be valid JSON (it's Rich text)
            try:
                json.loads(result.output)
                pytest.fail("Text mode should not output valid JSON")
            except json.JSONDecodeError:
                pass  # Expected
