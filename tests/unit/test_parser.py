"""Tests for the parser module."""

import os
import tempfile
from pathlib import Path

import pytest
import yaml

from streamt.core.parser import ProjectParser, ParseError, EnvVarError


class TestProjectParser:
    """Tests for ProjectParser."""

    def test_minimal_project_valid(self):
        """TC-VAL-001: Minimal project should be valid."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_path = Path(tmpdir)

            # Create minimal project
            config = {
                "project": {"name": "minimal-project"},
                "runtime": {"kafka": {"bootstrap_servers": "localhost:9092"}},
            }
            with open(project_path / "stream_project.yml", "w") as f:
                yaml.dump(config, f)

            parser = ProjectParser(project_path)
            project = parser.parse()

            assert project.project.name == "minimal-project"
            assert project.runtime.kafka.bootstrap_servers == "localhost:9092"

    def test_project_with_source_valid(self):
        """TC-VAL-002: Project with source should be valid."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_path = Path(tmpdir)

            config = {
                "project": {"name": "test-project"},
                "runtime": {"kafka": {"bootstrap_servers": "localhost:9092"}},
                "sources": [{"name": "payments_raw", "topic": "payments.raw.v1"}],
            }
            with open(project_path / "stream_project.yml", "w") as f:
                yaml.dump(config, f)

            parser = ProjectParser(project_path)
            project = parser.parse()

            assert len(project.sources) == 1
            assert project.sources[0].name == "payments_raw"
            assert project.sources[0].topic == "payments.raw.v1"

    def test_model_with_sql_inline(self):
        """TC-VAL-004: Model with SQL inline should parse correctly."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_path = Path(tmpdir)

            config = {
                "project": {"name": "test-project"},
                "runtime": {"kafka": {"bootstrap_servers": "localhost:9092"}},
                "sources": [{"name": "payments_raw", "topic": "payments.raw.v1"}],
                "models": [
                    {
                        "name": "payments_clean",
                        "sql": 'SELECT * FROM {{ source("payments_raw") }}',
                    }
                ],
            }
            with open(project_path / "stream_project.yml", "w") as f:
                yaml.dump(config, f)

            parser = ProjectParser(project_path)
            project = parser.parse()

            assert len(project.models) == 1
            assert project.models[0].name == "payments_clean"
            assert "source" in project.models[0].sql

    def test_env_var_resolution(self):
        """TC-VAL-010/011: Environment variables should be resolved."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_path = Path(tmpdir)

            config = {
                "project": {"name": "test-project"},
                "runtime": {"kafka": {"bootstrap_servers": "${KAFKA_BOOTSTRAP}"}},
            }
            with open(project_path / "stream_project.yml", "w") as f:
                yaml.dump(config, f)

            # Without env var set, should fail
            parser = ProjectParser(project_path)
            with pytest.raises(EnvVarError) as exc_info:
                parser.parse()
            assert "KAFKA_BOOTSTRAP" in str(exc_info.value)

            # With env var set, should succeed
            os.environ["KAFKA_BOOTSTRAP"] = "localhost:9092"
            try:
                project = parser.parse()
                assert project.runtime.kafka.bootstrap_servers == "localhost:9092"
            finally:
                del os.environ["KAFKA_BOOTSTRAP"]

    def test_env_var_from_dotenv(self):
        """TC-VAL-011: Environment variables from .env should work."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_path = Path(tmpdir)

            config = {
                "project": {"name": "test-project"},
                "runtime": {"kafka": {"bootstrap_servers": "${KAFKA_BOOTSTRAP_TEST}"}},
            }
            with open(project_path / "stream_project.yml", "w") as f:
                yaml.dump(config, f)

            # Create .env file
            with open(project_path / ".env", "w") as f:
                f.write("KAFKA_BOOTSTRAP_TEST=localhost:9092\n")

            parser = ProjectParser(project_path)
            project = parser.parse()

            assert project.runtime.kafka.bootstrap_servers == "localhost:9092"

    def test_all_in_one_file(self):
        """TC-VAL-012: All-in-one YAML file should work."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_path = Path(tmpdir)

            config = {
                "project": {"name": "all-in-one"},
                "runtime": {"kafka": {"bootstrap_servers": "localhost:9092"}},
                "sources": [{"name": "payments_raw", "topic": "payments.raw.v1"}],
                "models": [
                    {
                        "name": "payments_clean",
                        "sql": 'SELECT * FROM {{ source("payments_raw") }}',
                    }
                ],
                "tests": [
                    {
                        "name": "payments_test",
                        "model": "payments_clean",
                        "type": "schema",
                        "assertions": [{"not_null": {"columns": ["payment_id"]}}],
                    }
                ],
                "exposures": [
                    {
                        "name": "consumer_app",
                        "type": "application",
                        "role": "consumer",
                        "consumes": [{"ref": "payments_clean"}],
                    }
                ],
            }
            with open(project_path / "stream_project.yml", "w") as f:
                yaml.dump(config, f)

            parser = ProjectParser(project_path)
            project = parser.parse()

            assert len(project.sources) == 1
            assert len(project.models) == 1
            assert len(project.tests) == 1
            assert len(project.exposures) == 1

    def test_sources_from_directory(self):
        """Sources should be loaded from sources/ directory."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_path = Path(tmpdir)

            # Main config
            config = {
                "project": {"name": "test-project"},
                "runtime": {"kafka": {"bootstrap_servers": "localhost:9092"}},
            }
            with open(project_path / "stream_project.yml", "w") as f:
                yaml.dump(config, f)

            # Sources in separate file
            sources_dir = project_path / "sources"
            sources_dir.mkdir()
            sources_config = {
                "sources": [
                    {"name": "payments_raw", "topic": "payments.raw.v1"},
                    {"name": "customers_raw", "topic": "customers.raw.v1"},
                ]
            }
            with open(sources_dir / "payments.yml", "w") as f:
                yaml.dump(sources_config, f)

            parser = ProjectParser(project_path)
            project = parser.parse()

            assert len(project.sources) == 2

    def test_extract_refs_from_sql(self):
        """SQL ref extraction should work correctly."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_path = Path(tmpdir)

            config = {
                "project": {"name": "test-project"},
                "runtime": {"kafka": {"bootstrap_servers": "localhost:9092"}},
            }
            with open(project_path / "stream_project.yml", "w") as f:
                yaml.dump(config, f)

            parser = ProjectParser(project_path)

            sql = """
            SELECT p.*, c.name
            FROM {{ source("payments_raw") }} p
            JOIN {{ ref("customers_clean") }} c ON p.customer_id = c.id
            WHERE {{ ref("filter_rules") }} = true
            """

            sources, refs = parser.extract_refs_from_sql(sql)

            assert sources == ["payments_raw"]
            assert set(refs) == {"customers_clean", "filter_rules"}

    def test_validate_jinja_syntax_valid(self):
        """Valid Jinja syntax should pass."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_path = Path(tmpdir)

            config = {
                "project": {"name": "test-project"},
                "runtime": {"kafka": {"bootstrap_servers": "localhost:9092"}},
            }
            with open(project_path / "stream_project.yml", "w") as f:
                yaml.dump(config, f)

            parser = ProjectParser(project_path)

            sql = 'SELECT * FROM {{ source("payments") }}'
            is_valid, error = parser.validate_jinja_sql(sql)

            assert is_valid
            assert error is None

    def test_validate_jinja_syntax_invalid(self):
        """Invalid Jinja syntax should be detected."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_path = Path(tmpdir)

            config = {
                "project": {"name": "test-project"},
                "runtime": {"kafka": {"bootstrap_servers": "localhost:9092"}},
            }
            with open(project_path / "stream_project.yml", "w") as f:
                yaml.dump(config, f)

            parser = ProjectParser(project_path)

            sql = 'SELECT * FROM {{ source("payments" }}'  # Missing closing paren
            is_valid, error = parser.validate_jinja_sql(sql)

            assert not is_valid
            assert error is not None

    def test_missing_project_file(self):
        """Missing stream_project.yml should raise error."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_path = Path(tmpdir)
            parser = ProjectParser(project_path)

            with pytest.raises(ParseError) as exc_info:
                parser.parse()
            assert "stream_project.yml" in str(exc_info.value)
