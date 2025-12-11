"""Tests for the CLI module."""

import tempfile
from pathlib import Path

import pytest
import yaml
from click.testing import CliRunner

from streamt.cli import main


class TestCLI:
    """Tests for CLI commands."""

    def _create_project(self, tmpdir: str, config: dict) -> Path:
        """Helper to create a project."""
        project_path = Path(tmpdir)
        with open(project_path / "stream_project.yml", "w") as f:
            yaml.dump(config, f)
        return project_path

    def test_validate_minimal_project(self):
        """TC-VAL-001: Minimal project should validate successfully."""
        runner = CliRunner()

        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                "project": {"name": "minimal-project"},
                "runtime": {"kafka": {"bootstrap_servers": "localhost:9092"}},
            }
            project_path = self._create_project(tmpdir, config)

            result = runner.invoke(main, ["validate", "-p", str(project_path)])

            assert result.exit_code == 0
            assert "is valid" in result.output

    def test_validate_with_errors(self):
        """Validation should fail with errors."""
        runner = CliRunner()

        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                "project": {"name": "test"},
                "runtime": {"kafka": {"bootstrap_servers": "localhost:9092"}},
                "models": [
                    {
                        "name": "broken",
                        "sql": 'SELECT * FROM {{ source("nonexistent") }}',
                    }
                ],
            }
            project_path = self._create_project(tmpdir, config)

            result = runner.invoke(main, ["validate", "-p", str(project_path)])

            assert result.exit_code == 1
            assert "ERROR" in result.output

    def test_compile_dry_run(self):
        """TC-COMP-007: Compile dry run should show what would be generated."""
        runner = CliRunner()

        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                "project": {"name": "test", "version": "1.0.0"},
                "runtime": {"kafka": {"bootstrap_servers": "localhost:9092"}},
                "sources": [{"name": "raw", "topic": "raw.v1"}],
                "models": [
                    {
                        "name": "clean",
                        "sql": 'SELECT * FROM {{ source("raw") }}',
                        "advanced": {
                            "topic": {"partitions": 6}
                        }
                    }
                ],
            }
            project_path = self._create_project(tmpdir, config)

            result = runner.invoke(main, ["compile", "-p", str(project_path), "--dry-run"])

            assert result.exit_code == 0
            assert "Dry run" in result.output
            assert "clean" in result.output

    def test_compile_output_dir(self):
        """TC-COMP-008: Compile should use custom output directory."""
        runner = CliRunner()

        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                "project": {"name": "test", "version": "1.0.0"},
                "runtime": {"kafka": {"bootstrap_servers": "localhost:9092"}},
                "sources": [{"name": "raw", "topic": "raw.v1"}],
                "models": [
                    {
                        "name": "clean",
                        "sql": 'SELECT * FROM {{ source("raw") }}',
                    }
                ],
            }
            project_path = self._create_project(tmpdir, config)
            output_dir = Path(tmpdir) / "my-output"

            result = runner.invoke(
                main, ["compile", "-p", str(project_path), "-o", str(output_dir)]
            )

            assert result.exit_code == 0
            assert output_dir.exists()
            assert (output_dir / "manifest.json").exists()

    def test_lineage_ascii(self):
        """TC-LIN-001: Lineage should show ASCII DAG."""
        runner = CliRunner()

        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                "project": {"name": "test"},
                "runtime": {"kafka": {"bootstrap_servers": "localhost:9092"}},
                "sources": [{"name": "raw", "topic": "raw.v1"}],
                "models": [
                    {
                        "name": "clean",
                        "sql": 'SELECT * FROM {{ source("raw") }}',
                    },
                    {
                        "name": "enriched",
                        "sql": 'SELECT * FROM {{ ref("clean") }}',
                    },
                ],
            }
            project_path = self._create_project(tmpdir, config)

            result = runner.invoke(main, ["lineage", "-p", str(project_path)])

            assert result.exit_code == 0
            assert "raw" in result.output
            assert "clean" in result.output
            assert "enriched" in result.output

    def test_lineage_json(self):
        """TC-LIN-005: Lineage should support JSON format."""
        runner = CliRunner()

        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                "project": {"name": "test"},
                "runtime": {"kafka": {"bootstrap_servers": "localhost:9092"}},
                "sources": [{"name": "raw", "topic": "raw.v1"}],
                "models": [
                    {
                        "name": "clean",
                        "sql": 'SELECT * FROM {{ source("raw") }}',
                    }
                ],
            }
            project_path = self._create_project(tmpdir, config)

            result = runner.invoke(main, ["lineage", "-p", str(project_path), "--format", "json"])

            assert result.exit_code == 0
            assert '"nodes"' in result.output
            assert '"edges"' in result.output

    def test_test_schema(self):
        """TC-TEST-001: Schema test should pass."""
        runner = CliRunner()

        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                "project": {"name": "test"},
                "runtime": {"kafka": {"bootstrap_servers": "localhost:9092"}},
                "sources": [{"name": "raw", "topic": "raw.v1"}],
                "models": [
                    {
                        "name": "clean",
                        "sql": 'SELECT * FROM {{ source("raw") }}',
                    }
                ],
                "tests": [
                    {
                        "name": "clean_schema",
                        "model": "clean",
                        "type": "schema",
                        "assertions": [{"not_null": {"columns": ["id"]}}],
                    }
                ],
            }
            project_path = self._create_project(tmpdir, config)

            result = runner.invoke(main, ["test", "-p", str(project_path)])

            assert result.exit_code == 0
            assert "PASS" in result.output

    def test_test_filter_by_model(self):
        """TC-TEST-008: Test should filter by model."""
        runner = CliRunner()

        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                "project": {"name": "test"},
                "runtime": {"kafka": {"bootstrap_servers": "localhost:9092"}},
                "sources": [{"name": "raw", "topic": "raw.v1"}],
                "models": [
                    {
                        "name": "model1",
                        "sql": 'SELECT * FROM {{ source("raw") }}',
                    },
                    {
                        "name": "model2",
                        "sql": 'SELECT * FROM {{ ref("model1") }}',
                    },
                ],
                "tests": [
                    {
                        "name": "test1",
                        "model": "model1",
                        "type": "schema",
                        "assertions": [],
                    },
                    {
                        "name": "test2",
                        "model": "model2",
                        "type": "schema",
                        "assertions": [],
                    },
                ],
            }
            project_path = self._create_project(tmpdir, config)

            result = runner.invoke(main, ["test", "-p", str(project_path), "--model", "model1"])

            assert result.exit_code == 0
            assert "test1" in result.output
            # test2 should not be in output since it's for model2

    def test_test_filter_by_type(self):
        """TC-TEST-009: Test should filter by type."""
        runner = CliRunner()

        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                "project": {"name": "test"},
                "runtime": {"kafka": {"bootstrap_servers": "localhost:9092"}},
                "sources": [{"name": "raw", "topic": "raw.v1"}],
                "models": [
                    {
                        "name": "clean",
                        "sql": 'SELECT * FROM {{ source("raw") }}',
                    }
                ],
                "tests": [
                    {
                        "name": "schema_test",
                        "model": "clean",
                        "type": "schema",
                        "assertions": [],
                    },
                    {
                        "name": "sample_test",
                        "model": "clean",
                        "type": "sample",
                        "sample_size": 10,
                        "assertions": [],
                    },
                ],
            }
            project_path = self._create_project(tmpdir, config)

            result = runner.invoke(main, ["test", "-p", str(project_path), "--type", "schema"])

            assert result.exit_code == 0
            assert "schema_test" in result.output

    def test_docs_generate(self):
        """TC-LIN-006: Docs generate should create HTML."""
        runner = CliRunner()

        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                "project": {"name": "my-project"},
                "runtime": {"kafka": {"bootstrap_servers": "localhost:9092"}},
                "sources": [{"name": "raw", "topic": "raw.v1"}],
                "models": [
                    {
                        "name": "clean",
                        "description": "Cleaned data",
                        "sql": 'SELECT * FROM {{ source("raw") }}',
                    }
                ],
            }
            project_path = self._create_project(tmpdir, config)

            result = runner.invoke(main, ["docs", "generate", "-p", str(project_path)])

            assert result.exit_code == 0
            assert (project_path / "docs" / "index.html").exists()

    def test_version(self):
        """Version command should work."""
        runner = CliRunner()
        result = runner.invoke(main, ["--version"])

        assert result.exit_code == 0
        assert "0.1.0" in result.output

    def test_missing_project_file(self):
        """Should fail gracefully when project file is missing."""
        runner = CliRunner()

        with tempfile.TemporaryDirectory() as tmpdir:
            result = runner.invoke(main, ["validate", "-p", tmpdir])

            assert result.exit_code == 1
            assert "stream_project.yml" in result.output

    def test_env_var_error(self):
        """Should fail with helpful message when env var is missing."""
        runner = CliRunner()

        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                "project": {"name": "test"},
                "runtime": {"kafka": {"bootstrap_servers": "${UNDEFINED_VAR}"}},
            }
            project_path = self._create_project(tmpdir, config)

            result = runner.invoke(main, ["validate", "-p", str(project_path)])

            assert result.exit_code == 1
            assert "UNDEFINED_VAR" in result.output
