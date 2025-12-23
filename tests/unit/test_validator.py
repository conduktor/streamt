"""Tests for the validator module."""

import tempfile
from pathlib import Path

import yaml

from streamt.core.models import StreamtProject
from streamt.core.parser import ProjectParser
from streamt.core.validator import ProjectValidator


class TestProjectValidator:
    """Tests for ProjectValidator."""

    def _create_project(self, tmpdir: str, config: dict) -> "StreamtProject":
        """Helper to create and parse a project."""
        project_path = Path(tmpdir)
        with open(project_path / "stream_project.yml", "w") as f:
            yaml.dump(config, f)
        parser = ProjectParser(project_path)
        return parser.parse()

    def test_duplicate_source_names(self):
        """TC-ERR-008: Duplicate source names should be detected."""
        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                "project": {"name": "test"},
                "runtime": {"kafka": {"bootstrap_servers": "localhost:9092"}},
                "sources": [
                    {"name": "payments_raw", "topic": "t1"},
                    {"name": "payments_raw", "topic": "t2"},  # Duplicate
                ],
            }
            project = self._create_project(tmpdir, config)
            validator = ProjectValidator(project)
            result = validator.validate()

            assert not result.is_valid
            assert any("DUPLICATE_SOURCE" in e.code for e in result.errors)

    def test_duplicate_model_names(self):
        """TC-ERR-008: Duplicate model names should be detected."""
        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                "project": {"name": "test"},
                "runtime": {"kafka": {"bootstrap_servers": "localhost:9092"}},
                "models": [
                    {"name": "payments_clean", "sql": "SELECT 1"},
                    {"name": "payments_clean", "sql": "SELECT 2"},  # Duplicate
                ],
            }
            project = self._create_project(tmpdir, config)
            validator = ProjectValidator(project)
            result = validator.validate()

            assert not result.is_valid
            assert any("DUPLICATE_MODEL" in e.code for e in result.errors)

    def test_source_not_found(self):
        """TC-VAL-007: Reference to non-existent source should fail."""
        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                "project": {"name": "test"},
                "runtime": {"kafka": {"bootstrap_servers": "localhost:9092"}},
                "models": [
                    {
                        "name": "payments_clean",
                        "sql": 'SELECT * FROM {{ source("nonexistent") }}',
                    }
                ],
            }
            project = self._create_project(tmpdir, config)
            validator = ProjectValidator(project)
            result = validator.validate()

            assert not result.is_valid
            assert any("SOURCE_NOT_FOUND" in e.code for e in result.errors)

    def test_model_not_found(self):
        """TC-VAL-008: Reference to non-existent model should fail."""
        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                "project": {"name": "test"},
                "runtime": {"kafka": {"bootstrap_servers": "localhost:9092"}},
                "models": [
                    {
                        "name": "enriched",
                        "sql": 'SELECT * FROM {{ ref("payments_clean") }}',
                    }
                ],
            }
            project = self._create_project(tmpdir, config)
            validator = ProjectValidator(project)
            result = validator.validate()

            assert not result.is_valid
            assert any("MODEL_NOT_FOUND" in e.code for e in result.errors)

    def test_cycle_detection(self):
        """TC-VAL-009: Cycles in DAG should be detected."""
        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                "project": {"name": "test"},
                "runtime": {"kafka": {"bootstrap_servers": "localhost:9092"}},
                "models": [
                    {
                        "name": "model_a",
                        "sql": 'SELECT * FROM {{ ref("model_b") }}',
                    },
                    {
                        "name": "model_b",
                        "sql": 'SELECT * FROM {{ ref("model_a") }}',
                    },
                ],
            }
            project = self._create_project(tmpdir, config)
            validator = ProjectValidator(project)
            result = validator.validate()

            assert not result.is_valid
            assert any("CYCLE_DETECTED" in e.code for e in result.errors)

    def test_virtual_topic_without_gateway(self):
        """TC-COMP-005: virtual_topic without Gateway should fail."""
        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                "project": {"name": "test"},
                "runtime": {"kafka": {"bootstrap_servers": "localhost:9092"}},
                "sources": [{"name": "payments_raw", "topic": "t1"}],
                "models": [
                    {
                        "name": "payments_filtered",
                        "sql": 'SELECT * FROM {{ source("payments_raw") }} WHERE amount > 100',
                        "gateway": {
                            "virtual_topic": {
                                "name": "payments.filtered",
                                "compression": "snappy"
                            }
                        },
                    }
                ],
            }
            project = self._create_project(tmpdir, config)
            validator = ProjectValidator(project)
            result = validator.validate()

            assert not result.is_valid
            assert any("GATEWAY_REQUIRED" in e.code for e in result.errors)

    def test_continuous_test_without_flink(self):
        """TC-TEST-006: Continuous test without Flink should fail."""
        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                "project": {"name": "test"},
                "runtime": {"kafka": {"bootstrap_servers": "localhost:9092"}},
                "sources": [{"name": "payments_raw", "topic": "t1"}],
                "models": [
                    {
                        "name": "payments_clean",
                        "sql": 'SELECT * FROM {{ source("payments_raw") }}',
                    }
                ],
                "tests": [
                    {
                        "name": "quality_test",
                        "model": "payments_clean",
                        "type": "continuous",
                        "assertions": [{"unique_key": {"key": "id", "window": "15 minutes"}}],
                    }
                ],
            }
            project = self._create_project(tmpdir, config)
            validator = ProjectValidator(project)
            result = validator.validate()

            assert not result.is_valid
            assert any("CONTINUOUS_TEST_REQUIRES_FLINK" in e.code for e in result.errors)

    def test_jinja_syntax_error(self):
        """TC-ERR-009: Invalid Jinja syntax should be detected."""
        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                "project": {"name": "test"},
                "runtime": {"kafka": {"bootstrap_servers": "localhost:9092"}},
                "sources": [{"name": "payments_raw", "topic": "t1"}],
                "models": [
                    {
                        "name": "broken",
                        "sql": 'SELECT * FROM {{ source("payments_raw" }}',  # Missing )
                    }
                ],
            }
            project = self._create_project(tmpdir, config)
            validator = ProjectValidator(project)
            result = validator.validate()

            assert not result.is_valid
            assert any("JINJA_SYNTAX_ERROR" in e.code for e in result.errors)

    def test_unused_from_warning(self):
        """TC-VAL-006: Declared but unused from should warn."""
        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                "project": {"name": "test"},
                "runtime": {"kafka": {"bootstrap_servers": "localhost:9092"}},
                "sources": [
                    {"name": "payments_raw", "topic": "t1"},
                    {"name": "customers_raw", "topic": "t2"},
                ],
                "models": [
                    {
                        "name": "payments_clean",
                        "from": [
                            {"source": "payments_raw"},
                            {"source": "customers_raw"},  # Declared but not used
                        ],
                        "sql": 'SELECT * FROM {{ source("payments_raw") }}',
                    }
                ],
            }
            project = self._create_project(tmpdir, config)
            validator = ProjectValidator(project)
            result = validator.validate()

            assert result.is_valid  # Should still be valid
            assert len(result.warnings) > 0
            assert any("UNUSED_FROM" in w.code for w in result.warnings)

    def test_rule_min_partitions_violation(self):
        """TC-RULE-001: Violation of min_partitions rule should fail."""
        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                "project": {"name": "test"},
                "runtime": {
                    "kafka": {"bootstrap_servers": "localhost:9092"},
                    "flink": {
                        "default": "local",
                        "clusters": {"local": {"type": "rest", "rest_url": "http://localhost:8081"}},
                    },
                },
                "rules": {"topics": {"min_partitions": 6}},
                "sources": [{"name": "payments_raw", "topic": "t1"}],
                "models": [
                    {
                        "name": "payments_clean",
                        # Use GROUP BY to ensure FLINK materialization
                        "sql": 'SELECT count(*) FROM {{ source("payments_raw") }} GROUP BY id',
                        "advanced": {
                            "topic": {"partitions": 3}  # Less than 6
                        }
                    }
                ],
            }
            project = self._create_project(tmpdir, config)
            validator = ProjectValidator(project)
            result = validator.validate()

            assert not result.is_valid
            assert any("RULE_MIN_PARTITIONS" in e.code for e in result.errors)

    def test_rule_require_description_violation(self):
        """TC-RULE-003: Violation of require_description rule should fail."""
        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                "project": {"name": "test"},
                "runtime": {"kafka": {"bootstrap_servers": "localhost:9092"}},
                "rules": {"models": {"require_description": True}},
                "models": [
                    {
                        "name": "payments_clean",
                        "sql": "SELECT 1",
                        # No description
                    }
                ],
            }
            project = self._create_project(tmpdir, config)
            validator = ProjectValidator(project)
            result = validator.validate()

            assert not result.is_valid
            assert any("RULE_REQUIRE_DESCRIPTION" in e.code for e in result.errors)

    def test_rule_require_tests_violation(self):
        """TC-RULE-004: Violation of require_tests rule should fail."""
        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                "project": {"name": "test"},
                "runtime": {"kafka": {"bootstrap_servers": "localhost:9092"}},
                "rules": {"models": {"require_tests": True}},
                "models": [
                    {
                        "name": "payments_clean",
                        "sql": "SELECT 1",
                    }
                ],
                # No tests defined
            }
            project = self._create_project(tmpdir, config)
            validator = ProjectValidator(project)
            result = validator.validate()

            assert not result.is_valid
            assert any("RULE_REQUIRE_TESTS" in e.code for e in result.errors)

    def test_valid_project_with_rules(self):
        """TC-RULE-006: Project respecting all rules should pass."""
        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                "project": {"name": "test"},
                "runtime": {"kafka": {"bootstrap_servers": "localhost:9092"}},
                "rules": {
                    "topics": {"min_partitions": 3},
                    "models": {"require_description": True},
                },
                "sources": [{"name": "payments_raw", "topic": "t1"}],
                "models": [
                    {
                        "name": "payments_clean",
                        "description": "Cleaned payments",
                        "sql": 'SELECT * FROM {{ source("payments_raw") }}',
                        "advanced": {
                            "topic": {"partitions": 6}
                        }
                    }
                ],
            }
            project = self._create_project(tmpdir, config)
            validator = ProjectValidator(project)
            result = validator.validate()

            assert result.is_valid

    def test_exposure_model_not_found(self):
        """Exposure referencing unknown model should fail."""
        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                "project": {"name": "test"},
                "runtime": {"kafka": {"bootstrap_servers": "localhost:9092"}},
                "exposures": [
                    {
                        "name": "app",
                        "type": "application",
                        "role": "consumer",
                        "consumes": [{"ref": "nonexistent_model"}],
                    }
                ],
            }
            project = self._create_project(tmpdir, config)
            validator = ProjectValidator(project)
            result = validator.validate()

            assert not result.is_valid
            assert any("EXPOSURE_MODEL_NOT_FOUND" in e.code for e in result.errors)

    def test_test_model_not_found(self):
        """Test referencing unknown model should fail."""
        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                "project": {"name": "test"},
                "runtime": {"kafka": {"bootstrap_servers": "localhost:9092"}},
                "tests": [
                    {
                        "name": "test1",
                        "model": "nonexistent",
                        "type": "schema",
                        "assertions": [],
                    }
                ],
            }
            project = self._create_project(tmpdir, config)
            validator = ProjectValidator(project)
            result = validator.validate()

            assert not result.is_valid
            assert any("TEST_MODEL_NOT_FOUND" in e.code for e in result.errors)

    def test_ml_predict_without_confluent_flink_errors(self):
        """TC-ML-001: ML_PREDICT without Confluent Flink should error."""
        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                "project": {"name": "test"},
                "runtime": {
                    "kafka": {"bootstrap_servers": "localhost:9092"},
                    # No Confluent Flink - only open-source Flink
                    "flink": {
                        "default": "local",
                        "clusters": {"local": {"type": "rest", "rest_url": "http://localhost:8081"}},
                    },
                },
                "sources": [{"name": "events", "topic": "events.v1"}],
                "models": [
                    {
                        "name": "predictions",
                        "sql": 'SELECT event_id, ML_PREDICT(fraud_model, amount) AS prediction FROM {{ source("events") }}',
                    }
                ],
            }
            project = self._create_project(tmpdir, config)
            validator = ProjectValidator(project)
            result = validator.validate()

            assert not result.is_valid
            assert any("CONFLUENT_FLINK_REQUIRED" in e.code for e in result.errors)
            error = next(e for e in result.errors if "CONFLUENT_FLINK_REQUIRED" in e.code)
            assert "ML_PREDICT" in error.message
            assert "Confluent" in error.message

    def test_ml_predict_without_ml_outputs_warns(self):
        """TC-ML-002: ML_PREDICT without ml_outputs should warn (with Confluent Flink)."""
        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                "project": {"name": "test"},
                "runtime": {
                    "kafka": {"bootstrap_servers": "localhost:9092"},
                    "flink": {
                        "default": "confluent",
                        "clusters": {"confluent": {"type": "confluent", "environment": "env-xxx"}},
                    },
                },
                "sources": [{"name": "events", "topic": "events.v1"}],
                "models": [
                    {
                        "name": "predictions",
                        "sql": 'SELECT event_id, ML_PREDICT(fraud_model, amount) AS prediction FROM {{ source("events") }}',
                        # No ml_outputs declared
                    }
                ],
            }
            project = self._create_project(tmpdir, config)
            validator = ProjectValidator(project)
            result = validator.validate()

            assert result.is_valid  # Should still be valid (warning, not error)
            assert any("ML_PREDICT_OPAQUE_OUTPUT" in w.code for w in result.warnings)
            warning = next(w for w in result.warnings if "ML_PREDICT_OPAQUE_OUTPUT" in w.code)
            assert "fraud_model" in warning.message
            assert "ml_outputs" in warning.message

    def test_ml_predict_with_ml_outputs_no_warning(self):
        """TC-ML-003: ML_PREDICT with proper ml_outputs should not warn."""
        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                "project": {"name": "test"},
                "runtime": {
                    "kafka": {"bootstrap_servers": "localhost:9092"},
                    "flink": {
                        "default": "confluent",
                        "clusters": {"confluent": {"type": "confluent", "environment": "env-xxx"}},
                    },
                },
                "sources": [{"name": "events", "topic": "events.v1"}],
                "models": [
                    {
                        "name": "predictions",
                        "sql": 'SELECT event_id, ML_PREDICT(fraud_model, amount) AS prediction FROM {{ source("events") }}',
                        "ml_outputs": {
                            "fraud_model": {
                                "columns": [
                                    {"name": "is_fraud", "type": "BOOLEAN"},
                                    {"name": "confidence", "type": "DOUBLE"},
                                ]
                            }
                        },
                    }
                ],
            }
            project = self._create_project(tmpdir, config)
            validator = ProjectValidator(project)
            result = validator.validate()

            assert result.is_valid
            # Should NOT have ML_PREDICT_OPAQUE_OUTPUT warning
            assert not any("ML_PREDICT_OPAQUE_OUTPUT" in w.code for w in result.warnings)

    def test_ml_evaluate_without_ml_outputs_warns(self):
        """TC-ML-004: ML_EVALUATE without ml_outputs should warn."""
        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                "project": {"name": "test"},
                "runtime": {
                    "kafka": {"bootstrap_servers": "localhost:9092"},
                    "flink": {
                        "default": "confluent",
                        "clusters": {"confluent": {"type": "confluent", "environment": "env-xxx"}},
                    },
                },
                "sources": [{"name": "events", "topic": "events.v1"}],
                "models": [
                    {
                        "name": "model_eval",
                        "sql": 'SELECT ML_EVALUATE(my_classifier, features) AS eval_result FROM {{ source("events") }}',
                    }
                ],
            }
            project = self._create_project(tmpdir, config)
            validator = ProjectValidator(project)
            result = validator.validate()

            assert result.is_valid
            assert any("ML_PREDICT_OPAQUE_OUTPUT" in w.code for w in result.warnings)
