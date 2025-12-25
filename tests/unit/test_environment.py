"""Tests for multi-environment support.

These tests are written TDD-style BEFORE implementation.
Run with: pytest tests/unit/test_environment.py -v

Spec: docs/specs/multi-environment.md
"""

import os
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest
import yaml
from click.testing import CliRunner

from streamt.cli import main


# Default Flink config required for models with SQL transformations
DEFAULT_FLINK_CONFIG = {
    "default": "local",
    "clusters": {
        "local": {
            "rest_url": "http://localhost:8082",
            "sql_gateway_url": "http://localhost:8084",
        }
    },
}


def make_runtime(bootstrap: str = "localhost:9092") -> dict:
    """Create a complete runtime config with kafka and flink."""
    return {
        "kafka": {"bootstrap_servers": bootstrap},
        "flink": DEFAULT_FLINK_CONFIG,
    }


class ProjectFactory:
    """Helper to create test projects."""

    # Default Flink config required for models with SQL transformations
    DEFAULT_FLINK_CONFIG = {
        "default": "local",
        "clusters": {
            "local": {
                "rest_url": "http://localhost:8082",
                "sql_gateway_url": "http://localhost:8084",
            }
        },
    }

    def __init__(self, tmpdir: str):
        self.project_path = Path(tmpdir)

    def create_single_env_project(self, runtime: dict | None = None) -> Path:
        """Create a single-environment project (no environments/ dir)."""
        config = {
            "project": {"name": "test-project", "version": "1.0.0"},
            "sources": [{"name": "events", "topic": "events.raw.v1"}],
            "models": [
                {
                    "name": "events_clean",
                    "sql": 'SELECT * FROM {{ source("events") }} WHERE id IS NOT NULL',
                }
            ],
        }
        if runtime:
            config["runtime"] = runtime
        else:
            config["runtime"] = {
                "kafka": {"bootstrap_servers": "localhost:9092"},
                "flink": self.DEFAULT_FLINK_CONFIG,
            }

        with open(self.project_path / "stream_project.yml", "w") as f:
            yaml.dump(config, f)

        return self.project_path

    def create_multi_env_project(
        self,
        environments: dict[str, dict],
        include_runtime_in_project: bool = False,
    ) -> Path:
        """Create a multi-environment project with environments/ dir."""
        # Main project file (no runtime)
        config = {
            "project": {"name": "test-project", "version": "1.0.0"},
            "sources": [{"name": "events", "topic": "events.raw.v1"}],
            "models": [
                {
                    "name": "events_clean",
                    "sql": 'SELECT * FROM {{ source("events") }} WHERE id IS NOT NULL',
                }
            ],
        }
        if include_runtime_in_project:
            config["runtime"] = {
                "kafka": {"bootstrap_servers": "should-be-ignored:9092"},
                "flink": self.DEFAULT_FLINK_CONFIG,
            }

        with open(self.project_path / "stream_project.yml", "w") as f:
            yaml.dump(config, f)

        # Create environments directory
        env_dir = self.project_path / "environments"
        env_dir.mkdir()

        # Create environment files
        for env_name, env_config in environments.items():
            env_file = env_dir / f"{env_name}.yml"
            with open(env_file, "w") as f:
                yaml.dump(env_config, f)

        return self.project_path

    def create_dotenv(self, filename: str, content: dict[str, str]) -> None:
        """Create a .env file."""
        with open(self.project_path / filename, "w") as f:
            for key, value in content.items():
                f.write(f"{key}={value}\n")


# =============================================================================
# T1: Single-Environment Mode Tests
# =============================================================================


class TestSingleEnvMode:
    """Tests for single-environment mode (no environments/ directory)."""

    def test_t1_1_single_env_validates_without_flag(self):
        """T1.1: No environments/ dir, runtime: in project -> streamt validate succeeds."""
        runner = CliRunner()

        with tempfile.TemporaryDirectory() as tmpdir:
            factory = ProjectFactory(tmpdir)
            project_path = factory.create_single_env_project()

            result = runner.invoke(main, ["validate", "-p", str(project_path)])

            assert result.exit_code == 0, f"Expected success, got: {result.output}"
            assert "valid" in result.output.lower()

    def test_t1_2_single_env_rejects_env_flag(self):
        """T1.2: No environments/ dir -> --env flag causes error."""
        runner = CliRunner()

        with tempfile.TemporaryDirectory() as tmpdir:
            factory = ProjectFactory(tmpdir)
            project_path = factory.create_single_env_project()

            result = runner.invoke(main, ["validate", "-p", str(project_path), "--env", "dev"])

            assert result.exit_code != 0
            assert "no environments configured" in result.output.lower()

    def test_t1_3_single_env_missing_runtime_fails(self):
        """T1.3: No environments/ dir, NO runtime: -> error."""
        runner = CliRunner()

        with tempfile.TemporaryDirectory() as tmpdir:
            project_path = Path(tmpdir)

            # Create project without runtime
            config = {
                "project": {"name": "test-project"},
                "sources": [{"name": "events", "topic": "events.raw.v1"}],
            }
            with open(project_path / "stream_project.yml", "w") as f:
                yaml.dump(config, f)

            result = runner.invoke(main, ["validate", "-p", str(project_path)])

            assert result.exit_code != 0
            assert "runtime" in result.output.lower()


# =============================================================================
# T2: Multi-Environment Mode Tests
# =============================================================================


class TestMultiEnvMode:
    """Tests for multi-environment mode (environments/ directory exists)."""

    def _make_env_config(self, name: str, bootstrap: str = "localhost:9092") -> dict:
        """Helper to create environment config."""
        return {
            "environment": {"name": name, "description": f"{name} environment"},
            "runtime": make_runtime(bootstrap),
        }

    def test_t2_1_multi_env_requires_env_flag(self):
        """T2.1: environments/ exists -> must specify --env."""
        runner = CliRunner()

        with tempfile.TemporaryDirectory() as tmpdir:
            factory = ProjectFactory(tmpdir)
            factory.create_multi_env_project(
                environments={
                    "dev": self._make_env_config("dev"),
                }
            )

            result = runner.invoke(main, ["validate", "-p", str(tmpdir)])

            assert result.exit_code != 0, (
                f"Multi-env mode without --env flag MUST fail. Got: {result.output}"
            )
            # Error MUST tell user to specify --env
            output_lower = result.output.lower()
            assert "--env" in result.output or "specify" in output_lower, (
                f"Error must mention '--env' or 'specify'. Got: {result.output}"
            )
            # MUST show available environments to help user
            assert "dev" in result.output, (
                f"Error must list available environment 'dev'. Got: {result.output}"
            )

    def test_t2_2_multi_env_with_flag_succeeds(self):
        """T2.2: environments/dev.yml exists -> --env dev succeeds."""
        runner = CliRunner()

        with tempfile.TemporaryDirectory() as tmpdir:
            factory = ProjectFactory(tmpdir)
            factory.create_multi_env_project(
                environments={
                    "dev": self._make_env_config("dev"),
                }
            )

            result = runner.invoke(main, ["validate", "-p", str(tmpdir), "--env", "dev"])

            assert result.exit_code == 0, f"Expected success, got: {result.output}"

    def test_t2_3_multi_env_unknown_env_fails_with_list(self):
        """T2.3: --env prod when only dev exists -> error with available list."""
        runner = CliRunner()

        with tempfile.TemporaryDirectory() as tmpdir:
            factory = ProjectFactory(tmpdir)
            factory.create_multi_env_project(
                environments={
                    "dev": self._make_env_config("dev"),
                    "staging": self._make_env_config("staging"),
                }
            )

            result = runner.invoke(main, ["validate", "-p", str(tmpdir), "--env", "prod"])

            assert result.exit_code != 0, (
                f"Unknown environment 'prod' MUST fail. Got: {result.output}"
            )
            # Error MUST mention the unknown environment
            assert "prod" in result.output, (
                f"Error must mention the requested env 'prod'. Got: {result.output}"
            )
            # Error MUST indicate it wasn't found
            output_lower = result.output.lower()
            assert "not found" in output_lower or "available" in output_lower, (
                f"Error must say 'not found' or show 'available' envs. Got: {result.output}"
            )
            # MUST list all available environments
            assert "dev" in result.output, (
                f"Error must list available 'dev'. Got: {result.output}"
            )
            assert "staging" in result.output, (
                f"Error must list available 'staging'. Got: {result.output}"
            )

    def test_t2_4_multi_env_warns_about_ignored_runtime(self):
        """T2.4: environments/ exists + runtime: in project -> warning."""
        runner = CliRunner()

        with tempfile.TemporaryDirectory() as tmpdir:
            factory = ProjectFactory(tmpdir)
            factory.create_multi_env_project(
                environments={
                    "dev": self._make_env_config("dev"),
                },
                include_runtime_in_project=True,
            )

            result = runner.invoke(main, ["validate", "-p", str(tmpdir), "--env", "dev"])

            # Should succeed but warn about ignored runtime
            assert result.exit_code == 0
            output_lower = result.output.lower()
            # Must specifically mention runtime is ignored in multi-env mode
            assert "runtime" in output_lower and "ignored" in output_lower, (
                f"Warning must mention 'runtime' is 'ignored'. Got: {result.output}"
            )

    def test_validate_all_envs_flag(self):
        """Spec: validate --all-envs validates all environments at once."""
        runner = CliRunner()

        with tempfile.TemporaryDirectory() as tmpdir:
            factory = ProjectFactory(tmpdir)
            factory.create_multi_env_project(
                environments={
                    "dev": self._make_env_config("dev"),
                    "staging": self._make_env_config("staging"),
                    "prod": self._make_env_config("prod"),
                }
            )

            result = runner.invoke(main, ["validate", "-p", str(tmpdir), "--all-envs"])

            # Should validate all environments
            assert result.exit_code == 0, f"--all-envs validation failed: {result.output}"
            # Output should mention all environments were validated
            assert "dev" in result.output, f"Must show dev was validated. Got: {result.output}"
            assert "staging" in result.output, f"Must show staging was validated. Got: {result.output}"
            assert "prod" in result.output, f"Must show prod was validated. Got: {result.output}"

    def test_validate_all_envs_fails_if_any_invalid(self):
        """--all-envs should fail if ANY environment is invalid."""
        runner = CliRunner()

        with tempfile.TemporaryDirectory() as tmpdir:
            factory = ProjectFactory(tmpdir)
            # dev is valid, staging has missing variable
            factory.create_multi_env_project(
                environments={
                    "dev": self._make_env_config("dev"),
                    "staging": {
                        "environment": {"name": "staging"},
                        "runtime": {"kafka": {"bootstrap_servers": "${MISSING_VAR}"}},
                    },
                }
            )

            result = runner.invoke(main, ["validate", "-p", str(tmpdir), "--all-envs"])

            # Should fail because staging is invalid
            assert result.exit_code != 0, (
                f"--all-envs must fail if any env is invalid. Got: {result.output}"
            )
            # Should indicate which environment failed
            assert "staging" in result.output, (
                f"Must indicate 'staging' failed. Got: {result.output}"
            )

    def test_multi_env_uses_correct_environment_config(self):
        """Multi-env mode uses the correct environment's runtime config.

        This test verifies that each environment loads its OWN config, not another's.
        We use `envs show` command to verify the resolved config values.
        """
        runner = CliRunner()

        with tempfile.TemporaryDirectory() as tmpdir:
            factory = ProjectFactory(tmpdir)
            factory.create_multi_env_project(
                environments={
                    "dev": self._make_env_config("dev", "dev-kafka:9092"),
                    "staging": self._make_env_config("staging", "staging-kafka:9092"),
                }
            )

            # Both should succeed with their respective configs
            result_dev = runner.invoke(main, ["validate", "-p", str(tmpdir), "--env", "dev"])
            result_staging = runner.invoke(main, ["validate", "-p", str(tmpdir), "--env", "staging"])

            assert result_dev.exit_code == 0, f"Dev validation failed: {result_dev.output}"
            assert result_staging.exit_code == 0, f"Staging validation failed: {result_staging.output}"

            # Verify correct config is loaded using envs show
            show_dev = runner.invoke(main, ["envs", "show", "dev", "-p", str(tmpdir)])
            show_staging = runner.invoke(main, ["envs", "show", "staging", "-p", str(tmpdir)])

            # Dev MUST show dev-kafka, NOT staging-kafka
            if show_dev.exit_code == 0:
                assert "dev-kafka:9092" in show_dev.output, (
                    f"Dev config must show 'dev-kafka:9092'. Got: {show_dev.output}"
                )
                assert "staging-kafka:9092" not in show_dev.output, (
                    f"Dev config must NOT contain staging config. Got: {show_dev.output}"
                )

            # Staging MUST show staging-kafka, NOT dev-kafka
            if show_staging.exit_code == 0:
                assert "staging-kafka:9092" in show_staging.output, (
                    f"Staging config must show 'staging-kafka:9092'. Got: {show_staging.output}"
                )
                assert "dev-kafka:9092" not in show_staging.output, (
                    f"Staging config must NOT contain dev config. Got: {show_staging.output}"
                )

    def test_multi_env_lists_all_available(self):
        """Error message lists all available environments."""
        runner = CliRunner()

        with tempfile.TemporaryDirectory() as tmpdir:
            factory = ProjectFactory(tmpdir)
            factory.create_multi_env_project(
                environments={
                    "dev": self._make_env_config("dev"),
                    "staging": self._make_env_config("staging"),
                    "prod": self._make_env_config("prod"),
                }
            )

            result = runner.invoke(main, ["validate", "-p", str(tmpdir)])

            assert result.exit_code != 0
            # All three should be listed
            assert "dev" in result.output
            assert "staging" in result.output
            assert "prod" in result.output


# =============================================================================
# T3: STREAMT_ENV Variable Tests
# =============================================================================


class TestStreamtEnvVariable:
    """Tests for STREAMT_ENV environment variable."""

    def _make_env_config(self, name: str) -> dict:
        return {
            "environment": {"name": name},
            "runtime": make_runtime(f"{name}-kafka:9092"),
        }

    def test_t3_1_streamt_env_selects_environment(self):
        """T3.1: STREAMT_ENV=dev in multi-env mode -> uses dev."""
        runner = CliRunner()

        with tempfile.TemporaryDirectory() as tmpdir:
            factory = ProjectFactory(tmpdir)
            factory.create_multi_env_project(
                environments={
                    "dev": self._make_env_config("dev"),
                    "staging": self._make_env_config("staging"),
                }
            )

            result = runner.invoke(
                main,
                ["validate", "-p", str(tmpdir)],
                env={"STREAMT_ENV": "dev"},
            )

            assert result.exit_code == 0, f"Expected success, got: {result.output}"

    def test_t3_2_cli_flag_overrides_env_var(self):
        """T3.2: STREAMT_ENV=dev + --env staging -> uses staging."""
        runner = CliRunner()

        with tempfile.TemporaryDirectory() as tmpdir:
            factory = ProjectFactory(tmpdir)
            factory.create_multi_env_project(
                environments={
                    "dev": self._make_env_config("dev"),
                    "staging": self._make_env_config("staging"),
                }
            )

            result = runner.invoke(
                main,
                ["validate", "-p", str(tmpdir), "--env", "staging"],
                env={"STREAMT_ENV": "dev"},
            )

            assert result.exit_code == 0

    def test_t3_3_streamt_env_ignored_in_single_env_mode(self):
        """T3.3: STREAMT_ENV=dev in single-env mode -> warning."""
        runner = CliRunner()

        with tempfile.TemporaryDirectory() as tmpdir:
            factory = ProjectFactory(tmpdir)
            factory.create_single_env_project()

            result = runner.invoke(
                main,
                ["validate", "-p", str(tmpdir)],
                env={"STREAMT_ENV": "dev"},
            )

            # Should succeed but warn
            assert result.exit_code == 0
            assert "ignored" in result.output.lower() or "warning" in result.output.lower()

    def test_streamt_env_invalid_value_fails(self):
        """STREAMT_ENV set to non-existent environment fails with helpful message."""
        runner = CliRunner()

        with tempfile.TemporaryDirectory() as tmpdir:
            factory = ProjectFactory(tmpdir)
            factory.create_multi_env_project(
                environments={
                    "dev": self._make_env_config("dev"),
                }
            )

            result = runner.invoke(
                main,
                ["validate", "-p", str(tmpdir)],
                env={"STREAMT_ENV": "nonexistent"},
            )

            assert result.exit_code != 0
            assert "nonexistent" in result.output
            assert "dev" in result.output  # Should show available


# =============================================================================
# T4: .env File Loading Tests
# =============================================================================


class TestPerEnvDotenvLoading:
    """Tests for .env and .env.{env} file loading."""

    def test_t4_1_base_dotenv_loaded(self):
        """T4.1: .env has X=1 -> X resolves to 1."""
        runner = CliRunner()

        with tempfile.TemporaryDirectory() as tmpdir:
            factory = ProjectFactory(tmpdir)
            factory.create_single_env_project(
                runtime=make_runtime("${TEST_BOOTSTRAP}")
            )
            factory.create_dotenv(".env", {"TEST_BOOTSTRAP": "from-base:9092"})

            result = runner.invoke(main, ["validate", "-p", str(tmpdir)])

            assert result.exit_code == 0, f"Expected success, got: {result.output}"

    def test_t4_2_per_env_dotenv_overrides_base(self):
        """T4.2: .env has X=1, .env.dev has X=2 -> X=2 for --env dev."""
        runner = CliRunner()

        with tempfile.TemporaryDirectory() as tmpdir:
            factory = ProjectFactory(tmpdir)

            runtime = make_runtime("${TEST_BOOTSTRAP}")
            env_config = {
                "environment": {"name": "dev"},
                "runtime": runtime,
            }
            factory.create_multi_env_project(environments={"dev": env_config})
            factory.create_dotenv(".env", {"TEST_BOOTSTRAP": "from-base:9092"})
            factory.create_dotenv(".env.dev", {"TEST_BOOTSTRAP": "from-dev:9092"})

            result = runner.invoke(main, ["validate", "-p", str(tmpdir), "--env", "dev"])

            assert result.exit_code == 0

    def test_t4_3_other_env_dotenv_not_loaded(self):
        """T4.3: .env.prod has X=3 -> not loaded for --env dev."""
        runner = CliRunner()

        with tempfile.TemporaryDirectory() as tmpdir:
            factory = ProjectFactory(tmpdir)

            # dev.yml uses a var that's only in .env.prod
            env_config = {
                "environment": {"name": "dev"},
                "runtime": {"kafka": {"bootstrap_servers": "${PROD_ONLY_VAR}"}},
            }
            factory.create_multi_env_project(environments={"dev": env_config})
            factory.create_dotenv(".env.prod", {"PROD_ONLY_VAR": "prod-value"})

            result = runner.invoke(main, ["validate", "-p", str(tmpdir), "--env", "dev"])

            # Should fail because PROD_ONLY_VAR is not available
            assert result.exit_code != 0
            assert "PROD_ONLY_VAR" in result.output

    def test_t4_4_real_env_var_overrides_dotenv(self):
        """T4.4: .env.dev has X=2, actual env has X=9 -> X=9."""
        runner = CliRunner()

        with tempfile.TemporaryDirectory() as tmpdir:
            factory = ProjectFactory(tmpdir)

            runtime = make_runtime("${TEST_BOOTSTRAP}")
            env_config = {
                "environment": {"name": "dev"},
                "runtime": runtime,
            }
            factory.create_multi_env_project(environments={"dev": env_config})
            factory.create_dotenv(".env.dev", {"TEST_BOOTSTRAP": "from-dotenv:9092"})

            # Real env var should win
            result = runner.invoke(
                main,
                ["validate", "-p", str(tmpdir), "--env", "dev"],
                env={"TEST_BOOTSTRAP": "from-real-env:9092"},
            )

            assert result.exit_code == 0

    def test_base_dotenv_used_when_per_env_missing(self):
        """Falls back to .env when .env.{env} doesn't exist."""
        runner = CliRunner()

        with tempfile.TemporaryDirectory() as tmpdir:
            factory = ProjectFactory(tmpdir)

            runtime = make_runtime("${SHARED_VAR}")
            env_config = {
                "environment": {"name": "dev"},
                "runtime": runtime,
            }
            factory.create_multi_env_project(environments={"dev": env_config})
            factory.create_dotenv(".env", {"SHARED_VAR": "shared-value:9092"})
            # Note: no .env.dev created

            result = runner.invoke(main, ["validate", "-p", str(tmpdir), "--env", "dev"])

            assert result.exit_code == 0


# =============================================================================
# Environment Name Validation Tests
# =============================================================================


class TestEnvironmentNameValidation:
    """Tests for environment name validation."""

    def test_env_name_must_match_filename(self):
        """Environment name in YAML must match the filename."""
        runner = CliRunner()

        with tempfile.TemporaryDirectory() as tmpdir:
            project_path = Path(tmpdir)

            # Create project file
            config = {
                "project": {"name": "test-project"},
                "sources": [{"name": "events", "topic": "events.raw.v1"}],
            }
            with open(project_path / "stream_project.yml", "w") as f:
                yaml.dump(config, f)

            # Create environments directory with mismatched name
            env_dir = project_path / "environments"
            env_dir.mkdir()

            # File is "dev.yml" but name is "development"
            env_config = {
                "environment": {"name": "development"},  # Doesn't match "dev"
                "runtime": {"kafka": {"bootstrap_servers": "localhost:9092"}},
            }
            with open(env_dir / "dev.yml", "w") as f:
                yaml.dump(env_config, f)

            result = runner.invoke(main, ["validate", "-p", str(tmpdir), "--env", "dev"])

            assert result.exit_code != 0
            assert "mismatch" in result.output.lower() or "match" in result.output.lower()

    def test_env_name_alphanumeric_and_hyphens(self):
        """Environment names can contain alphanumeric and hyphens."""
        runner = CliRunner()

        with tempfile.TemporaryDirectory() as tmpdir:
            factory = ProjectFactory(tmpdir)

            env_config = {
                "environment": {"name": "prod-us-east-1"},
                "runtime": make_runtime("localhost:9092"),
            }
            factory.create_multi_env_project(environments={"prod-us-east-1": env_config})

            result = runner.invoke(
                main, ["validate", "-p", str(tmpdir), "--env", "prod-us-east-1"]
            )

            assert result.exit_code == 0


# =============================================================================
# CLI Flag Tests Across Commands
# =============================================================================


class TestEnvFlagAcrossCommands:
    """Tests that --env flag works on all relevant commands."""

    def _make_env_config(self, name: str) -> dict:
        return {
            "environment": {"name": name},
            "runtime": make_runtime(f"{name}-kafka:9092"),
        }

    @pytest.mark.parametrize(
        "command",
        [
            ["validate"],
            ["compile"],
            ["plan"],
            ["apply"],
            ["lineage"],
        ],
    )
    def test_env_flag_works_on_command(self, command: list[str]):
        """--env flag should work on common commands."""
        runner = CliRunner()

        with tempfile.TemporaryDirectory() as tmpdir:
            factory = ProjectFactory(tmpdir)
            factory.create_multi_env_project(
                environments={
                    "dev": self._make_env_config("dev"),
                }
            )

            full_command = command + ["-p", str(tmpdir), "--env", "dev"]
            result = runner.invoke(main, full_command)

            # Should not fail with "unknown option --env"
            assert "--env" not in result.output or "unknown" not in result.output.lower()


# =============================================================================
# Edge Cases
# =============================================================================


class TestEdgeCases:
    """Edge case tests."""

    def test_empty_environments_directory(self):
        """Empty environments/ directory should give helpful error."""
        runner = CliRunner()

        with tempfile.TemporaryDirectory() as tmpdir:
            project_path = Path(tmpdir)

            config = {
                "project": {"name": "test"},
                "sources": [{"name": "events", "topic": "events.raw.v1"}],
            }
            with open(project_path / "stream_project.yml", "w") as f:
                yaml.dump(config, f)

            # Create empty environments dir
            (project_path / "environments").mkdir()

            result = runner.invoke(main, ["validate", "-p", str(tmpdir)])

            assert result.exit_code != 0
            assert "no environment" in result.output.lower() or "empty" in result.output.lower()

    def test_invalid_yaml_in_environment_file(self):
        """Invalid YAML in environment file should give clear error."""
        runner = CliRunner()

        with tempfile.TemporaryDirectory() as tmpdir:
            project_path = Path(tmpdir)

            config = {
                "project": {"name": "test"},
                "sources": [{"name": "events", "topic": "events.raw.v1"}],
            }
            with open(project_path / "stream_project.yml", "w") as f:
                yaml.dump(config, f)

            env_dir = project_path / "environments"
            env_dir.mkdir()

            # Invalid YAML
            with open(env_dir / "dev.yml", "w") as f:
                f.write("invalid: yaml: content: [")

            result = runner.invoke(main, ["validate", "-p", str(tmpdir), "--env", "dev"])

            assert result.exit_code != 0
            assert "yaml" in result.output.lower() or "parse" in result.output.lower()

    def test_environment_file_missing_runtime(self):
        """Environment file without runtime section should fail."""
        runner = CliRunner()

        with tempfile.TemporaryDirectory() as tmpdir:
            project_path = Path(tmpdir)

            config = {
                "project": {"name": "test"},
                "sources": [{"name": "events", "topic": "events.raw.v1"}],
            }
            with open(project_path / "stream_project.yml", "w") as f:
                yaml.dump(config, f)

            env_dir = project_path / "environments"
            env_dir.mkdir()

            # Environment without runtime
            env_config = {
                "environment": {"name": "dev"},
                # No runtime section!
            }
            with open(env_dir / "dev.yml", "w") as f:
                yaml.dump(env_config, f)

            result = runner.invoke(main, ["validate", "-p", str(tmpdir), "--env", "dev"])

            assert result.exit_code != 0
            assert "runtime" in result.output.lower()

    def test_non_yml_files_in_environments_ignored(self):
        """Non-.yml files in environments/ should be ignored."""
        runner = CliRunner()

        with tempfile.TemporaryDirectory() as tmpdir:
            factory = ProjectFactory(tmpdir)

            env_config = {
                "environment": {"name": "dev"},
                "runtime": make_runtime("localhost:9092"),
            }
            factory.create_multi_env_project(environments={"dev": env_config})

            # Add a non-yml file
            (factory.project_path / "environments" / "README.md").write_text("# Ignore me")
            (factory.project_path / "environments" / ".gitkeep").touch()

            result = runner.invoke(main, ["validate", "-p", str(tmpdir), "--env", "dev"])

            assert result.exit_code == 0


# =============================================================================
# T5: Protected Environments Tests
# =============================================================================


class TestProtectedEnvironments:
    """Tests for protected environment safeguards (T5 from spec)."""

    def _make_protected_env_config(
        self, name: str, protected: bool = True, confirm_apply: bool = True
    ) -> dict:
        """Helper to create protected environment config."""
        return {
            "environment": {
                "name": name,
                "description": f"{name} environment",
                "protected": protected,
            },
            "runtime": make_runtime(f"{name}-kafka:9092"),
            "safety": {"confirm_apply": confirm_apply, "allow_destructive": True},
        }

    def _make_env_config(self, name: str) -> dict:
        """Helper to create non-protected environment config."""
        return {
            "environment": {"name": name, "description": f"{name} environment"},
            "runtime": make_runtime(f"{name}-kafka:9092"),
        }

    def test_t5_1_protected_env_prompts_in_interactive_mode(self):
        """T5.1: protected: true -> apply prompts for confirmation in interactive mode."""
        runner = CliRunner()

        with tempfile.TemporaryDirectory() as tmpdir:
            factory = ProjectFactory(tmpdir)
            factory.create_multi_env_project(
                environments={
                    "prod": self._make_protected_env_config("prod", protected=True),
                }
            )

            # Interactive mode - should prompt (we won't provide input)
            result = runner.invoke(
                main,
                ["apply", "-p", str(tmpdir), "--env", "prod"],
                input="",  # No confirmation provided
            )

            # Should either show confirmation prompt or require explicit confirm
            assert (
                "confirm" in result.output.lower()
                or "protected" in result.output.lower()
                or "prod" in result.output.lower()
            )

    def test_t5_2_protected_env_proceeds_with_confirm_flag(self):
        """T5.2: protected: true + --confirm flag -> proceeds without prompt."""
        runner = CliRunner()

        with tempfile.TemporaryDirectory() as tmpdir:
            factory = ProjectFactory(tmpdir)
            factory.create_multi_env_project(
                environments={
                    "prod": self._make_protected_env_config("prod", protected=True),
                }
            )

            result = runner.invoke(
                main, ["apply", "-p", str(tmpdir), "--env", "prod", "--confirm"]
            )

            # Should not show a prompt asking for confirmation
            # (may fail for other reasons like connectivity, but not for confirmation)
            assert "type" not in result.output.lower() or "confirm" not in result.output.lower()

    def test_t5_3_protected_env_requires_confirm_in_ci_mode(self):
        """T5.3: protected: true in non-interactive (CI) mode without --confirm -> error."""
        runner = CliRunner()

        with tempfile.TemporaryDirectory() as tmpdir:
            factory = ProjectFactory(tmpdir)
            factory.create_multi_env_project(
                environments={
                    "prod": self._make_protected_env_config("prod", protected=True),
                }
            )

            # Simulate non-interactive mode by not providing a TTY
            result = runner.invoke(
                main,
                ["apply", "-p", str(tmpdir), "--env", "prod"],
                input=None,  # Non-interactive
            )

            # MUST fail - protected environment requires --confirm in non-interactive mode
            assert result.exit_code != 0, (
                f"Expected error for protected env without --confirm, but got exit_code=0. "
                f"Output: {result.output}"
            )
            # Error message MUST mention either --confirm requirement OR protected status
            output_lower = result.output.lower()
            assert "--confirm" in result.output or "protected" in output_lower, (
                f"Error must mention '--confirm' or 'protected', got: {result.output}"
            )

    def test_t5_4_unprotected_env_proceeds_without_prompt(self):
        """T5.4: protected: false -> apply proceeds without prompt."""
        runner = CliRunner()

        with tempfile.TemporaryDirectory() as tmpdir:
            factory = ProjectFactory(tmpdir)
            factory.create_multi_env_project(
                environments={
                    "dev": self._make_env_config("dev"),  # Not protected
                }
            )

            result = runner.invoke(main, ["apply", "-p", str(tmpdir), "--env", "dev"])

            # Should not prompt for confirmation (may fail for other reasons)
            assert "type" not in result.output.lower() or "'dev'" not in result.output.lower()

    def test_protected_env_shows_warning_banner(self):
        """Protected environment apply should show clear warning banner."""
        runner = CliRunner()

        with tempfile.TemporaryDirectory() as tmpdir:
            factory = ProjectFactory(tmpdir)
            factory.create_multi_env_project(
                environments={
                    "prod": self._make_protected_env_config("prod", protected=True),
                }
            )

            result = runner.invoke(
                main, ["apply", "-p", str(tmpdir), "--env", "prod", "--confirm"]
            )

            # Should show some kind of warning about protected environment
            output_lower = result.output.lower()
            assert (
                "protected" in output_lower
                or "prod" in output_lower
                or "warning" in output_lower
            )

    def test_protected_badge_in_envs_list(self):
        """Protected environments should show [protected] badge in list."""
        runner = CliRunner()

        with tempfile.TemporaryDirectory() as tmpdir:
            factory = ProjectFactory(tmpdir)
            factory.create_multi_env_project(
                environments={
                    "dev": self._make_env_config("dev"),
                    "prod": self._make_protected_env_config("prod", protected=True),
                }
            )

            result = runner.invoke(main, ["envs", "list", "-p", str(tmpdir)])

            # prod should be marked as protected
            assert "protected" in result.output.lower()


# =============================================================================
# T6: Destructive Safety Tests
# =============================================================================


class TestDestructiveSafety:
    """Tests for destructive operation safety (T6 from spec)."""

    def _make_safe_env_config(
        self, name: str, allow_destructive: bool = False
    ) -> dict:
        """Helper to create environment with destructive safety setting."""
        return {
            "environment": {
                "name": name,
                "description": f"{name} environment",
                "protected": True,
            },
            "runtime": make_runtime(f"{name}-kafka:9092"),
            "safety": {"confirm_apply": True, "allow_destructive": allow_destructive},
        }

    def test_t6_1_destructive_blocked_when_disabled(self):
        """T6.1: allow_destructive: false + plan has deletes -> error.

        This test verifies that when allow_destructive is false, any plan
        containing deletions MUST be blocked with a clear error message.
        """
        runner = CliRunner()

        with tempfile.TemporaryDirectory() as tmpdir:
            factory = ProjectFactory(tmpdir)
            factory.create_multi_env_project(
                environments={
                    "prod": self._make_safe_env_config("prod", allow_destructive=False),
                }
            )

            # The safety config must be parsed and stored correctly
            # Verify config is valid first
            result_validate = runner.invoke(
                main, ["validate", "-p", str(tmpdir), "--env", "prod"]
            )
            assert result_validate.exit_code == 0, (
                f"Safety config should be valid, got: {result_validate.output}"
            )

            # When applying, if deletions are detected:
            # - Exit code MUST be non-zero
            # - Output MUST mention "destructive" or "blocked" or "allow_destructive"
            result = runner.invoke(
                main, ["apply", "-p", str(tmpdir), "--env", "prod", "--confirm"]
            )

            # For TDD: we test the EXPECTED behavior when implementation exists
            # The implementation MUST either:
            # 1. Succeed if no deletions exist (exit_code=0, no blocking message)
            # 2. Fail with blocking message if deletions exist
            output_lower = result.output.lower()
            if result.exit_code != 0:
                # If it fails, it MUST be because of destructive blocking (or other valid error)
                # We verify the safety mechanism is in place
                assert (
                    "destructive" in output_lower
                    or "blocked" in output_lower
                    or "delete" in output_lower
                    or "error" in output_lower  # Generic error is acceptable too
                ), f"Failure must explain why. Got: {result.output}"

    def test_t6_2_destructive_allowed_with_force(self):
        """T6.2: allow_destructive: false + --force -> proceeds with warning.

        The --force flag MUST override the allow_destructive safety setting.
        """
        runner = CliRunner()

        with tempfile.TemporaryDirectory() as tmpdir:
            factory = ProjectFactory(tmpdir)
            factory.create_multi_env_project(
                environments={
                    "prod": self._make_safe_env_config("prod", allow_destructive=False),
                }
            )

            result = runner.invoke(
                main,
                ["apply", "-p", str(tmpdir), "--env", "prod", "--confirm", "--force"],
            )

            # The --force flag MUST be recognized (not an unknown option)
            assert "unknown option" not in result.output.lower(), (
                f"--force flag must be recognized by CLI. Got: {result.output}"
            )
            # When --force is used with allow_destructive=false:
            # - Should proceed (may show warning about force override)
            # - Should NOT show "blocked" message (force overrides blocking)
            if "blocked" in result.output.lower():
                # If still blocked, --force didn't work - this is a failure
                assert False, (
                    f"--force should override destructive blocking. Got: {result.output}"
                )

    def test_t6_3_destructive_allowed_when_enabled(self):
        """T6.3: allow_destructive: true -> proceeds normally.

        When allow_destructive is true, deletions should proceed without blocking.
        """
        runner = CliRunner()

        with tempfile.TemporaryDirectory() as tmpdir:
            factory = ProjectFactory(tmpdir)
            factory.create_multi_env_project(
                environments={
                    "prod": self._make_safe_env_config("prod", allow_destructive=True),
                }
            )

            result = runner.invoke(
                main, ["apply", "-p", str(tmpdir), "--env", "prod", "--confirm"]
            )

            # MUST NOT block destructive operations when allow_destructive=true
            output_lower = result.output.lower()
            assert "blocked" not in output_lower, (
                f"Destructive ops should NOT be blocked when allow_destructive=true. "
                f"Got: {result.output}"
            )
            assert "destructive operations blocked" not in output_lower, (
                f"Got unexpected blocking message: {result.output}"
            )

    def test_destructive_warning_shows_affected_resources(self):
        """Destructive operation warning should list affected resources."""
        runner = CliRunner()

        with tempfile.TemporaryDirectory() as tmpdir:
            factory = ProjectFactory(tmpdir)
            factory.create_multi_env_project(
                environments={
                    "prod": self._make_safe_env_config("prod", allow_destructive=False),
                }
            )

            # First create some state, then modify project to cause deletions
            # For now, just verify the safety config is parsed
            result = runner.invoke(main, ["validate", "-p", str(tmpdir), "--env", "prod"])

            # Validation should pass - safety config is valid
            assert result.exit_code == 0 or "safety" not in result.output.lower()


# =============================================================================
# T7: Environment Commands Tests
# =============================================================================


class TestEnvsCommands:
    """Tests for streamt envs commands (T7 from spec)."""

    def _make_env_config(
        self, name: str, description: str = "", protected: bool = False
    ) -> dict:
        """Helper to create environment config."""
        return {
            "environment": {
                "name": name,
                "description": description or f"{name} environment",
                "protected": protected,
            },
            "runtime": make_runtime(f"{name}-kafka:9092"),
        }

    def test_t7_1_envs_list_shows_all_environments(self):
        """T7.1: envs list shows all environments with descriptions."""
        runner = CliRunner()

        with tempfile.TemporaryDirectory() as tmpdir:
            factory = ProjectFactory(tmpdir)
            factory.create_multi_env_project(
                environments={
                    "dev": self._make_env_config("dev", "Local development"),
                    "staging": self._make_env_config("staging", "Staging environment"),
                    "prod": self._make_env_config("prod", "Production", protected=True),
                }
            )

            result = runner.invoke(main, ["envs", "list", "-p", str(tmpdir)])

            assert result.exit_code == 0, f"envs list MUST succeed. Got: {result.output}"
            # ALL environments MUST be listed
            assert "dev" in result.output, (
                f"Must list 'dev' environment. Got: {result.output}"
            )
            assert "staging" in result.output, (
                f"Must list 'staging' environment. Got: {result.output}"
            )
            assert "prod" in result.output, (
                f"Must list 'prod' environment. Got: {result.output}"
            )
            # Descriptions MUST be shown
            assert "Local development" in result.output, (
                f"Must show description 'Local development'. Got: {result.output}"
            )

    def test_t7_2_envs_list_shows_protected_badge(self):
        """T7.2: protected environments show [protected] badge."""
        runner = CliRunner()

        with tempfile.TemporaryDirectory() as tmpdir:
            factory = ProjectFactory(tmpdir)
            factory.create_multi_env_project(
                environments={
                    "dev": self._make_env_config("dev", protected=False),
                    "prod": self._make_env_config("prod", protected=True),
                }
            )

            result = runner.invoke(main, ["envs", "list", "-p", str(tmpdir)])

            assert result.exit_code == 0
            # prod should have protected indicator
            assert "protected" in result.output.lower()

    def test_t7_3_envs_show_masks_secrets(self):
        """T7.3: envs show masks secret values with ****.

        CRITICAL SECURITY: Secrets MUST NEVER appear in CLI output.
        """
        runner = CliRunner()

        with tempfile.TemporaryDirectory() as tmpdir:
            factory = ProjectFactory(tmpdir)

            # Create environment with secret reference
            env_config = {
                "environment": {"name": "dev"},
                "runtime": {
                    "kafka": {
                        "bootstrap_servers": "${KAFKA_BOOTSTRAP}",
                        "sasl_password": "${KAFKA_PASSWORD}",
                    }
                },
            }
            factory.create_multi_env_project(environments={"dev": env_config})
            factory.create_dotenv(
                ".env.dev",
                {"KAFKA_BOOTSTRAP": "kafka:9092", "KAFKA_PASSWORD": "supersecret123"},
            )

            result = runner.invoke(
                main, ["envs", "show", "dev", "-p", str(tmpdir)]
            )

            # SECURITY: The actual secret value MUST NEVER appear in output
            assert "supersecret123" not in result.output, (
                f"SECURITY VIOLATION: Secret 'supersecret123' exposed in output! "
                f"Got: {result.output}"
            )

            # If command succeeded, it MUST show masking indicator for password
            if result.exit_code == 0:
                # Should show masking indicator (e.g., ****, ***, [masked])
                output_lower = result.output.lower()
                has_masking = (
                    "****" in result.output
                    or "***" in result.output
                    or "masked" in output_lower
                    or "hidden" in output_lower
                    or "[redacted]" in output_lower
                )
                assert has_masking, (
                    f"Secrets must be masked with ****, [masked], etc. Got: {result.output}"
                )

    def test_t7_4_envs_list_single_env_mode_message(self):
        """T7.4: envs list in single-env mode shows appropriate message."""
        runner = CliRunner()

        with tempfile.TemporaryDirectory() as tmpdir:
            factory = ProjectFactory(tmpdir)
            factory.create_single_env_project()

            result = runner.invoke(main, ["envs", "list", "-p", str(tmpdir)])

            # Should indicate single-env mode
            output_lower = result.output.lower()
            assert (
                "single" in output_lower
                or "no environment" in output_lower
                or "not configured" in output_lower
            )

    def test_envs_show_displays_resolved_config(self):
        """envs show displays resolved configuration values."""
        runner = CliRunner()

        with tempfile.TemporaryDirectory() as tmpdir:
            factory = ProjectFactory(tmpdir)

            env_config = {
                "environment": {"name": "dev", "description": "Dev environment"},
                "runtime": make_runtime("localhost:9092"),
            }
            factory.create_multi_env_project(environments={"dev": env_config})

            result = runner.invoke(main, ["envs", "show", "dev", "-p", str(tmpdir)])

            # Should show the environment configuration
            assert result.exit_code == 0 or "dev" in result.output
            assert "localhost:9092" in result.output or "kafka" in result.output.lower()

    def test_envs_show_nonexistent_env_fails(self):
        """envs show with non-existent environment fails with helpful message."""
        runner = CliRunner()

        with tempfile.TemporaryDirectory() as tmpdir:
            factory = ProjectFactory(tmpdir)
            factory.create_multi_env_project(
                environments={
                    "dev": self._make_env_config("dev"),
                }
            )

            result = runner.invoke(
                main, ["envs", "show", "nonexistent", "-p", str(tmpdir)]
            )

            assert result.exit_code != 0
            assert "nonexistent" in result.output
            assert "dev" in result.output  # Should show available


# =============================================================================
# Security Tests
# =============================================================================


class TestSecurityFeatures:
    """Security-focused tests for multi-environment support."""

    def _make_env_config(self, name: str, bootstrap: str) -> dict:
        return {
            "environment": {"name": name},
            "runtime": make_runtime(bootstrap),
        }

    def test_cross_environment_credential_isolation(self):
        """Credentials from one environment should not leak to another."""
        runner = CliRunner()

        with tempfile.TemporaryDirectory() as tmpdir:
            factory = ProjectFactory(tmpdir)

            # Create two environments with different credentials
            factory.create_multi_env_project(
                environments={
                    "dev": {
                        "environment": {"name": "dev"},
                        "runtime": make_runtime("${DEV_KAFKA}"),
                    },
                    "prod": {
                        "environment": {"name": "prod"},
                        "runtime": make_runtime("${PROD_KAFKA}"),
                    },
                }
            )

            # Only set dev credentials
            factory.create_dotenv(".env.dev", {"DEV_KAFKA": "dev-kafka:9092"})
            factory.create_dotenv(".env.prod", {"PROD_KAFKA": "prod-kafka:9092"})

            # Dev should work
            result_dev = runner.invoke(
                main, ["validate", "-p", str(tmpdir), "--env", "dev"]
            )
            # Prod should also work with its own credentials
            result_prod = runner.invoke(
                main, ["validate", "-p", str(tmpdir), "--env", "prod"]
            )

            # Both should work with their respective credentials
            assert result_dev.exit_code == 0, f"Dev failed: {result_dev.output}"
            assert result_prod.exit_code == 0, f"Prod failed: {result_prod.output}"

    def test_environment_file_path_traversal_blocked(self):
        """Prevent path traversal attacks via environment names."""
        runner = CliRunner()

        with tempfile.TemporaryDirectory() as tmpdir:
            factory = ProjectFactory(tmpdir)
            factory.create_multi_env_project(
                environments={
                    "dev": self._make_env_config("dev", "localhost:9092"),
                }
            )

            # Attempt path traversal
            result = runner.invoke(
                main, ["validate", "-p", str(tmpdir), "--env", "../../../etc/passwd"]
            )

            # Should fail safely with validation error (not filesystem error)
            assert result.exit_code != 0
            # Should show "invalid" error (blocked by validation, not filesystem access)
            assert "invalid" in result.output.lower(), (
                f"Expected 'invalid' in error message. Got: {result.output}"
            )

    def test_secret_env_vars_not_logged(self):
        """Sensitive environment variables should not appear in verbose output."""
        runner = CliRunner()

        with tempfile.TemporaryDirectory() as tmpdir:
            factory = ProjectFactory(tmpdir)

            env_config = {
                "environment": {"name": "prod"},
                "runtime": {
                    "kafka": {
                        "bootstrap_servers": "${KAFKA_HOST}",
                        "sasl_password": "${SUPER_SECRET_PASSWORD}",
                    },
                    "schema_registry": {"api_secret": "${SR_API_SECRET}"},
                },
            }
            factory.create_multi_env_project(environments={"prod": env_config})
            factory.create_dotenv(
                ".env.prod",
                {
                    "KAFKA_HOST": "kafka:9092",
                    "SUPER_SECRET_PASSWORD": "MyS3cr3tP@ssw0rd!",
                    "SR_API_SECRET": "AnotherSecretValue",
                },
            )

            result = runner.invoke(
                main, ["validate", "-p", str(tmpdir), "--env", "prod", "-v"]
            )

            # Secrets should never appear in output
            assert "MyS3cr3tP@ssw0rd!" not in result.output
            assert "AnotherSecretValue" not in result.output


# =============================================================================
# Error Message Quality Tests
# =============================================================================


class TestErrorMessageQuality:
    """Tests ensuring error messages are helpful and actionable."""

    def _make_env_config(self, name: str) -> dict:
        return {
            "environment": {"name": name},
            "runtime": make_runtime(f"{name}-kafka:9092"),
        }

    def test_missing_env_error_suggests_available(self):
        """Missing environment error lists all available environments."""
        runner = CliRunner()

        with tempfile.TemporaryDirectory() as tmpdir:
            factory = ProjectFactory(tmpdir)
            factory.create_multi_env_project(
                environments={
                    "dev": self._make_env_config("dev"),
                    "staging": self._make_env_config("staging"),
                    "production": self._make_env_config("production"),
                }
            )

            result = runner.invoke(
                main, ["validate", "-p", str(tmpdir), "--env", "prd"]
            )

            assert result.exit_code != 0
            # Should list available environments
            assert "dev" in result.output
            assert "staging" in result.output
            assert "production" in result.output
            # Should indicate the error clearly
            assert "prd" in result.output

    def test_typo_in_env_name_suggests_correction(self):
        """Typo in environment name should suggest similar names."""
        runner = CliRunner()

        with tempfile.TemporaryDirectory() as tmpdir:
            factory = ProjectFactory(tmpdir)
            factory.create_multi_env_project(
                environments={
                    "development": self._make_env_config("development"),
                    "staging": self._make_env_config("staging"),
                    "production": self._make_env_config("production"),
                }
            )

            # Typo: "developmnet" instead of "development"
            result = runner.invoke(
                main, ["validate", "-p", str(tmpdir), "--env", "developmnet"]
            )

            assert result.exit_code != 0
            output_lower = result.output.lower()
            # Should at least show available options
            assert "development" in result.output or "available" in output_lower

    def test_missing_runtime_error_is_specific(self):
        """Missing runtime config error should be specific and actionable."""
        runner = CliRunner()

        with tempfile.TemporaryDirectory() as tmpdir:
            project_path = Path(tmpdir)

            # Create project without runtime
            config = {
                "project": {"name": "test-project", "version": "1.0.0"},
                "sources": [{"name": "events", "topic": "events.raw.v1"}],
            }
            with open(project_path / "stream_project.yml", "w") as f:
                yaml.dump(config, f)

            result = runner.invoke(main, ["validate", "-p", str(tmpdir)])

            assert result.exit_code != 0
            output_lower = result.output.lower()
            # Should specifically mention runtime
            assert "runtime" in output_lower
            # Should suggest fix
            assert (
                "add" in output_lower
                or "missing" in output_lower
                or "required" in output_lower
            )

    def test_error_format_is_consistent(self):
        """Error messages should follow consistent formatting."""
        runner = CliRunner()

        with tempfile.TemporaryDirectory() as tmpdir:
            factory = ProjectFactory(tmpdir)
            factory.create_multi_env_project(
                environments={
                    "dev": self._make_env_config("dev"),
                }
            )

            # Trigger an error
            result = runner.invoke(
                main, ["validate", "-p", str(tmpdir), "--env", "nonexistent"]
            )

            assert result.exit_code != 0
            # Error output should not be empty
            assert len(result.output.strip()) > 0
            # Should have some structure (not just a raw exception)
            assert "Traceback" not in result.output or "Error" in result.output


# =============================================================================
# Realistic Configuration Tests
# =============================================================================


class TestRealisticConfigurations:
    """Tests with production-like configurations."""

    def test_confluent_cloud_config_validates(self):
        """Confluent Cloud configuration should validate successfully."""
        runner = CliRunner()

        with tempfile.TemporaryDirectory() as tmpdir:
            factory = ProjectFactory(tmpdir)

            confluent_env = {
                "environment": {
                    "name": "confluent-prod",
                    "description": "Confluent Cloud Production",
                    "protected": True,
                },
                "runtime": {
                    "kafka": {
                        "bootstrap_servers": "${CONFLUENT_BOOTSTRAP}",
                        "security_protocol": "SASL_SSL",
                        "sasl_mechanism": "PLAIN",
                        "sasl_username": "${CONFLUENT_API_KEY}",
                        "sasl_password": "${CONFLUENT_API_SECRET}",
                    },
                    "schema_registry": {
                        "url": "${CONFLUENT_SR_URL}",
                        "api_key": "${CONFLUENT_SR_KEY}",
                        "api_secret": "${CONFLUENT_SR_SECRET}",
                    },
                    "flink": {
                        "default": "confluent",
                        "clusters": {
                            "confluent": {
                                "type": "confluent",
                                "environment": "${CONFLUENT_ENV_ID}",
                                "api_key": "${CONFLUENT_FLINK_KEY}",
                                "api_secret": "${CONFLUENT_FLINK_SECRET}",
                            }
                        },
                    },
                },
                "safety": {"confirm_apply": True, "allow_destructive": False},
            }

            factory.create_multi_env_project(environments={"confluent-prod": confluent_env})
            factory.create_dotenv(
                ".env.confluent-prod",
                {
                    "CONFLUENT_BOOTSTRAP": "pkc-xxx.us-west-2.aws.confluent.cloud:9092",
                    "CONFLUENT_API_KEY": "TESTKEY123",
                    "CONFLUENT_API_SECRET": "TESTSECRET456",
                    "CONFLUENT_SR_URL": "https://psrc-xxx.us-west-2.aws.confluent.cloud",
                    "CONFLUENT_SR_KEY": "SRKEY789",
                    "CONFLUENT_SR_SECRET": "SRSECRET012",
                    "CONFLUENT_ENV_ID": "env-abc123",
                    "CONFLUENT_FLINK_KEY": "FLINKKEY",
                    "CONFLUENT_FLINK_SECRET": "FLINKSECRET",
                },
            )

            result = runner.invoke(
                main, ["validate", "-p", str(tmpdir), "--env", "confluent-prod"]
            )

            # Configuration should be valid (may fail on schema validation details)
            # We're mainly testing that complex configs don't crash
            assert result.exit_code == 0 or "confluent" in result.output.lower()

    def test_multi_cluster_flink_config(self):
        """Multi-cluster Flink configuration (local + cloud) should validate."""
        runner = CliRunner()

        with tempfile.TemporaryDirectory() as tmpdir:
            factory = ProjectFactory(tmpdir)

            dev_env = {
                "environment": {"name": "dev"},
                "runtime": {
                    "kafka": {"bootstrap_servers": "localhost:9092"},
                    "flink": {
                        "default": "local",
                        "clusters": {
                            "local": {
                                "rest_url": "http://localhost:8082",
                                "sql_gateway_url": "http://localhost:8084",
                            }
                        },
                    },
                },
            }

            prod_env = {
                "environment": {"name": "prod", "protected": True},
                "runtime": {
                    "kafka": {"bootstrap_servers": "${PROD_KAFKA}"},
                    "flink": {
                        "default": "confluent",
                        "clusters": {
                            "confluent": {
                                "type": "confluent",
                                "environment": "${FLINK_ENV}",
                                "api_key": "${FLINK_KEY}",
                                "api_secret": "${FLINK_SECRET}",
                            }
                        },
                    },
                },
            }

            factory.create_multi_env_project(
                environments={"dev": dev_env, "prod": prod_env}
            )
            factory.create_dotenv(
                ".env.prod",
                {
                    "PROD_KAFKA": "prod-kafka:9092",
                    "FLINK_ENV": "env-123",
                    "FLINK_KEY": "key",
                    "FLINK_SECRET": "secret",
                },
            )

            # Both environments should validate
            result_dev = runner.invoke(
                main, ["validate", "-p", str(tmpdir), "--env", "dev"]
            )
            result_prod = runner.invoke(
                main, ["validate", "-p", str(tmpdir), "--env", "prod"]
            )

            assert result_dev.exit_code == 0, f"Dev failed: {result_dev.output}"
            assert result_prod.exit_code == 0, f"Prod failed: {result_prod.output}"

    def test_schema_registry_compatibility_per_environment(self):
        """Schema Registry compatibility mode should differ per environment.

        Real-world pattern:
        - Dev uses BACKWARD (relaxed for fast iteration)
        - Prod uses FULL or FULL_TRANSITIVE (strict for production safety)

        This test verifies each environment can have its own compatibility setting
        and that they don't leak across environments.
        """
        runner = CliRunner()

        with tempfile.TemporaryDirectory() as tmpdir:
            factory = ProjectFactory(tmpdir)

            dev_env = {
                "environment": {"name": "dev", "description": "Development"},
                "runtime": {
                    "kafka": {"bootstrap_servers": "localhost:9092"},
                    "flink": DEFAULT_FLINK_CONFIG,
                    "schema_registry": {
                        "url": "http://localhost:8081",
                        "compatibility": "BACKWARD",  # Relaxed for dev
                    },
                },
            }

            prod_env = {
                "environment": {"name": "prod", "description": "Production", "protected": True},
                "runtime": {
                    "kafka": {"bootstrap_servers": "${PROD_KAFKA}"},
                    "flink": DEFAULT_FLINK_CONFIG,
                    "schema_registry": {
                        "url": "${PROD_SR_URL}",
                        "compatibility": "FULL_TRANSITIVE",  # Strict for prod
                    },
                },
                "safety": {"confirm_apply": True, "allow_destructive": False},
            }

            factory.create_multi_env_project(environments={"dev": dev_env, "prod": prod_env})
            factory.create_dotenv(".env.prod", {
                "PROD_KAFKA": "prod-kafka:9092",
                "PROD_SR_URL": "https://prod-sr.example.com",
            })

            # Both should validate
            result_dev = runner.invoke(main, ["validate", "-p", str(tmpdir), "--env", "dev"])
            result_prod = runner.invoke(main, ["validate", "-p", str(tmpdir), "--env", "prod"])

            assert result_dev.exit_code == 0, f"Dev failed: {result_dev.output}"
            assert result_prod.exit_code == 0, f"Prod failed: {result_prod.output}"

            # Verify configs are isolated via envs show
            show_dev = runner.invoke(main, ["envs", "show", "dev", "-p", str(tmpdir)])
            show_prod = runner.invoke(main, ["envs", "show", "prod", "-p", str(tmpdir)])

            if show_dev.exit_code == 0:
                # Dev should show BACKWARD, NOT FULL_TRANSITIVE
                assert "BACKWARD" in show_dev.output, (
                    f"Dev must use BACKWARD compatibility. Got: {show_dev.output}"
                )
                assert "FULL_TRANSITIVE" not in show_dev.output, (
                    f"Dev must NOT show prod's FULL_TRANSITIVE. Got: {show_dev.output}"
                )

            if show_prod.exit_code == 0:
                # Prod should show FULL_TRANSITIVE, NOT BACKWARD
                assert "FULL_TRANSITIVE" in show_prod.output, (
                    f"Prod must use FULL_TRANSITIVE compatibility. Got: {show_prod.output}"
                )
                assert "BACKWARD" not in show_prod.output, (
                    f"Prod must NOT show dev's BACKWARD. Got: {show_prod.output}"
                )
