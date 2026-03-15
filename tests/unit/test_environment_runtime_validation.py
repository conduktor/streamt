"""Tests for environment runtime Pydantic validation.

Ensures that EnvironmentManager.load_environment() validates the runtime
section through RuntimeConfig, catching typos and structural errors early.
"""

import pytest

from streamt.core.environment import EnvironmentManager


class TestEnvironmentRuntimeValidation:
    """Tests for environment runtime Pydantic validation."""

    def test_environment_runtime_validated_against_pydantic(self, tmp_path):
        """Environment runtime section with invalid field should raise."""
        project_dir = tmp_path
        envs_dir = project_dir / "environments"
        envs_dir.mkdir()

        # Write env file with typo in runtime key
        env_file = envs_dir / "prod.yml"
        env_file.write_text(
            "environment:\n  name: prod\n"
            "runtime:\n  kafka:\n    bootstrapp_servers: localhost:9092\n"
        )

        mgr = EnvironmentManager(project_dir)
        with pytest.raises(Exception) as exc_info:
            mgr.load_environment("prod")
        assert "bootstrapp_servers" in str(exc_info.value) or "kafka" in str(
            exc_info.value
        )

    def test_environment_valid_runtime(self, tmp_path):
        """Valid environment runtime should load without error."""
        project_dir = tmp_path
        envs_dir = project_dir / "environments"
        envs_dir.mkdir()

        env_file = envs_dir / "prod.yml"
        env_file.write_text(
            "environment:\n  name: prod\n"
            "runtime:\n  kafka:\n    bootstrap_servers: prod-kafka:9092\n"
        )

        mgr = EnvironmentManager(project_dir)
        env = mgr.load_environment("prod")
        assert env.environment.name == "prod"
