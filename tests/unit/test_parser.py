"""Tests for the parser module."""

import os
import tempfile
from pathlib import Path

import pytest
import yaml

from streamt.core.parser import EnvVarError, ParseError, ProjectParser


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
            assert project.api_version == "streamt.dev/v1alpha1"

    def test_explicit_api_version_valid(self, tmp_path: Path):
        """The current DSL version is accepted and retained on the parsed project."""
        config = {
            "apiVersion": "streamt.dev/v1alpha1",
            "project": {"name": "versioned-project"},
            "runtime": {"kafka": {"bootstrap_servers": "localhost:9092"}},
        }
        (tmp_path / "stream_project.yml").write_text(yaml.dump(config))

        project = ProjectParser(tmp_path).parse()

        assert project.api_version == "streamt.dev/v1alpha1"

    def test_unsupported_api_version_fails(self, tmp_path: Path):
        """A declared DSL version must be one streamt understands."""
        config = {
            "apiVersion": "streamt/v2",
            "project": {"name": "future-project"},
            "runtime": {"kafka": {"bootstrap_servers": "localhost:9092"}},
        }
        (tmp_path / "stream_project.yml").write_text(yaml.dump(config))

        with pytest.raises(ParseError, match=r"apiVersion.*streamt/v2.*streamt.dev/v1alpha1"):
            ProjectParser(tmp_path).parse()

    def test_missing_api_version_warns_during_alpha_migration(self, tmp_path: Path):
        """Legacy alpha projects remain readable but receive an actionable warning."""
        config = {
            "project": {"name": "legacy-project"},
            "runtime": {"kafka": {"bootstrap_servers": "localhost:9092"}},
        }
        (tmp_path / "stream_project.yml").write_text(yaml.dump(config))
        warnings: list[str] = []

        project = ProjectParser(tmp_path, warn_callback=warnings.append).parse()

        assert project.api_version == "streamt.dev/v1alpha1"
        assert any("no 'apiVersion'" in warning for warning in warnings)

    def test_unknown_top_level_field_fails(self, tmp_path: Path):
        """A root typo must not silently remove a complete declaration section."""
        config = {
            "project": {"name": "typo-project"},
            "runtime": {"kafka": {"bootstrap_servers": "localhost:9092"}},
            "modles": [],
        }
        (tmp_path / "stream_project.yml").write_text(yaml.dump(config))

        with pytest.raises(ParseError, match=r"field 'modles'.*Extra inputs"):
            ProjectParser(tmp_path).parse()

    def test_unknown_nested_field_reports_full_path(self, tmp_path: Path):
        """Nested runtime typos report their location instead of disappearing."""
        config = {
            "project": {"name": "typo-project"},
            "runtime": {
                "kafka": {"bootstrap_servers": "localhost:9092"},
                "schema_registry": {
                    "url": "http://localhost:8081",
                    "usernme": "misspelled",
                },
            },
        }
        (tmp_path / "stream_project.yml").write_text(yaml.dump(config))

        with pytest.raises(
            ParseError,
            match=r"schema_registry → usernme.*Extra inputs are not permitted",
        ):
            ProjectParser(tmp_path).parse()

    def test_unknown_model_field_reports_model_and_path(self, tmp_path: Path):
        """Unsupported model policy fields fail with model and nested field context."""
        config = {
            "project": {"name": "typo-project"},
            "runtime": {"kafka": {"bootstrap_servers": "localhost:9092"}},
            "models": [
                {
                    "name": "payments_clean",
                    "sql": "SELECT 1",
                    "security": {"classifiction": {"customer_id": "confidential"}},
                }
            ],
        }
        (tmp_path / "stream_project.yml").write_text(yaml.dump(config))

        with pytest.raises(
            ParseError,
            match=r"Invalid model 'payments_clean'.*security → classifiction.*Extra inputs",
        ):
            ProjectParser(tmp_path).parse()

    def test_unknown_declaration_file_key_fails(self, tmp_path: Path):
        """A misspelled collection key in a split file must not be ignored."""
        config = {
            "project": {"name": "split-project"},
            "runtime": {"kafka": {"bootstrap_servers": "localhost:9092"}},
        }
        (tmp_path / "stream_project.yml").write_text(yaml.dump(config))
        models_dir = tmp_path / "models"
        models_dir.mkdir()
        (models_dir / "payments.yml").write_text("modles: []\n")

        with pytest.raises(
            ParseError,
            match=r"Invalid models file 'payments.yml'.*field 'modles'.*Extra inputs",
        ):
            ProjectParser(tmp_path).parse()

    @pytest.mark.parametrize("root_value", ["not-a-project", ["not", "a", "mapping"]])
    def test_project_file_root_must_be_mapping(
        self, tmp_path: Path, root_value: object
    ) -> None:
        """Non-mapping YAML roots fail before section access."""
        (tmp_path / "stream_project.yml").write_text(yaml.dump(root_value))

        with pytest.raises(
            ParseError,
            match=r"YAML file '.*stream_project.yml'.*field 'root' must be a mapping",
        ):
            ProjectParser(tmp_path).parse()

    @pytest.mark.parametrize("section", ["project", "runtime", "defaults", "rules"])
    def test_object_sections_must_be_mappings(self, tmp_path: Path, section: str) -> None:
        """Object sections report their path instead of failing during unpacking."""
        config: dict[str, object] = {
            "project": {"name": "shape-test"},
            "runtime": {"kafka": {"bootstrap_servers": "localhost:9092"}},
        }
        config[section] = "wrong-shape"
        (tmp_path / "stream_project.yml").write_text(yaml.dump(config))

        with pytest.raises(
            ParseError,
            match=rf"stream_project.yml.*field '{section}' must be a mapping",
        ):
            ProjectParser(tmp_path).parse()

    def test_connections_section_must_be_mapping(self, tmp_path: Path) -> None:
        """The named-connections collection cannot be a sequence."""
        config = {
            "project": {"name": "shape-test"},
            "runtime": {"kafka": {"bootstrap_servers": "localhost:9092"}},
            "connections": [],
        }
        (tmp_path / "stream_project.yml").write_text(yaml.dump(config))

        with pytest.raises(
            ParseError,
            match=r"stream_project.yml.*field 'connections' must be a mapping",
        ):
            ProjectParser(tmp_path).parse()

    def test_connection_item_must_be_mapping(self, tmp_path: Path) -> None:
        """A connection value reports the connection name in its error path."""
        config = {
            "project": {"name": "shape-test"},
            "runtime": {"kafka": {"bootstrap_servers": "localhost:9092"}},
            "connections": {"warehouse": "wrong-shape"},
        }
        (tmp_path / "stream_project.yml").write_text(yaml.dump(config))

        with pytest.raises(
            ParseError,
            match=r"field 'connections → warehouse' must be a mapping",
        ):
            ProjectParser(tmp_path).parse()

    @pytest.mark.parametrize("section", ["sources", "models", "tests", "exposures", "udfs"])
    def test_declaration_sections_must_be_lists(
        self, tmp_path: Path, section: str
    ) -> None:
        """Mapping-shaped declaration sections are rejected, even when empty."""
        config = {
            "project": {"name": "shape-test"},
            "runtime": {"kafka": {"bootstrap_servers": "localhost:9092"}},
            section: {},
        }
        (tmp_path / "stream_project.yml").write_text(yaml.dump(config))

        with pytest.raises(
            ParseError,
            match=rf"stream_project.yml.*field '{section}' must be a list",
        ):
            ProjectParser(tmp_path).parse()

    @pytest.mark.parametrize("section", ["sources", "models", "tests", "exposures", "udfs"])
    def test_declaration_items_must_be_mappings(
        self, tmp_path: Path, section: str
    ) -> None:
        """Scalar declaration entries report their collection and index."""
        config = {
            "project": {"name": "shape-test"},
            "runtime": {"kafka": {"bootstrap_servers": "localhost:9092"}},
            section: ["wrong-shape"],
        }
        (tmp_path / "stream_project.yml").write_text(yaml.dump(config))

        with pytest.raises(
            ParseError,
            match=rf"field '{section} → 0' must be a mapping",
        ):
            ProjectParser(tmp_path).parse()

    @pytest.mark.parametrize(
        ("contents", "path", "expected"),
        [
            ("sources: {}\n", "sources", "a list"),
            ("sources:\n  - wrong-shape\n", "sources → 0", "a mapping"),
        ],
    )
    def test_split_declaration_shapes_include_file_and_path(
        self, tmp_path: Path, contents: str, path: str, expected: str
    ) -> None:
        """Split declaration shape errors retain their filename and nested path."""
        config = {
            "project": {"name": "shape-test"},
            "runtime": {"kafka": {"bootstrap_servers": "localhost:9092"}},
        }
        (tmp_path / "stream_project.yml").write_text(yaml.dump(config))
        sources_dir = tmp_path / "sources"
        sources_dir.mkdir()
        (sources_dir / "broken.yml").write_text(contents)

        with pytest.raises(
            ParseError,
            match=rf"sources file 'broken.yml'.*field '{path}' must be {expected}",
        ):
            ProjectParser(tmp_path).parse()

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
