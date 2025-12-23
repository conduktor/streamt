"""Edge case and error handling test scenarios.

Tests complex edge cases and error handling:
- Complex DAG structures
- Circular dependency detection
- Large-scale projects
- Invalid configurations
- Error recovery patterns
- Resource limits
"""

import tempfile
from pathlib import Path

import yaml

from streamt.compiler import Compiler
from streamt.core.dag import DAGBuilder
from streamt.core.parser import ProjectParser
from streamt.core.validator import ProjectValidator


class TestComplexDAGStructures:
    """Test complex DAG topologies."""

    def _create_project(self, tmpdir: str, config: dict) -> Path:
        project_path = Path(tmpdir)
        with open(project_path / "stream_project.yml", "w") as f:
            yaml.dump(config, f)
        return project_path

    def test_diamond_dependency(self):
        """
        SCENARIO: Diamond dependency pattern

        Story: Model D depends on both B and C, which both depend on A.
        This creates a diamond shape in the DAG.

              A
             / \\
            B   C
             \\ /
              D
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                "project": {"name": "diamond-dag", "version": "1.0.0"},
                "runtime": {
                    "kafka": {"bootstrap_servers": "localhost:9092"},
                    "flink": {
                        "default": "local",
                        "clusters": {
                            "local": {"type": "rest", "rest_url": "http://localhost:8082"}
                        },
                    },
                },
                "sources": [
                    {"name": "source_a", "topic": "events.a.v1"},
                ],
                "models": [
                    {
                        "name": "model_b",

                        "sql": "SELECT * FROM {{ source('source_a') }} WHERE type = 'B'",
                    },
                    {
                        "name": "model_c",

                        "sql": "SELECT * FROM {{ source('source_a') }} WHERE type = 'C'",
                    },
                    {
                        "name": "model_d",

                        "sql": """
                            SELECT b.*, c.extra_field
                            FROM {{ ref("model_b") }} b
                            JOIN {{ ref("model_c") }} c ON b.id = c.id
                        """,
                    },
                ],
            }

            project_path = self._create_project(tmpdir, config)
            parser = ProjectParser(project_path)
            project = parser.parse()

            validator = ProjectValidator(project)
            result = validator.validate()
            assert result.is_valid

            dag_builder = DAGBuilder(project)
            dag = dag_builder.build()

            # Verify diamond structure
            assert "source_a" in dag.get_upstream("model_b")
            assert "source_a" in dag.get_upstream("model_c")
            assert "model_b" in dag.get_upstream("model_d")
            assert "model_c" in dag.get_upstream("model_d")

            # Verify execution order
            order = dag.topological_sort()
            assert order.index("source_a") < order.index("model_b")
            assert order.index("source_a") < order.index("model_c")
            assert order.index("model_b") < order.index("model_d")
            assert order.index("model_c") < order.index("model_d")

    def test_wide_fan_out(self):
        """
        SCENARIO: Wide fan-out from single source

        Story: A single source feeds into many independent models.
        Common in event-driven architectures.

              S
          /||\\ \\
         A B C D E
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            models = [
                {
                    "name": f"model_{i}",

                    "sql": f"SELECT * FROM {{{{ source('events') }}}} WHERE category = '{i}'",
                }
                for i in range(10)
            ]

            config = {
                "project": {"name": "fan-out", "version": "1.0.0"},
                "runtime": {
                    "kafka": {"bootstrap_servers": "localhost:9092"},
                    "flink": {
                        "default": "local",
                        "clusters": {
                            "local": {"type": "rest", "rest_url": "http://localhost:8082"}
                        },
                    },
                },
                "sources": [
                    {"name": "events", "topic": "events.raw.v1"},
                ],
                "models": models,
            }

            project_path = self._create_project(tmpdir, config)
            parser = ProjectParser(project_path)
            project = parser.parse()

            validator = ProjectValidator(project)
            result = validator.validate()
            assert result.is_valid

            dag_builder = DAGBuilder(project)
            dag = dag_builder.build()

            # All 10 models should depend on events
            for i in range(10):
                upstream = dag.get_upstream(f"model_{i}", recursive=False)
                assert "events" in upstream

            # No cross-dependencies between models
            for i in range(10):
                upstream = dag.get_upstream(f"model_{i}", recursive=False)
                for j in range(10):
                    if i != j:
                        assert f"model_{j}" not in upstream

    def test_wide_fan_in(self):
        """
        SCENARIO: Wide fan-in to single model

        Story: Many sources are joined into a single enriched model.
        Common in data warehousing patterns.

        S1 S2 S3 S4 S5
          \\|/|/
            M
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            sources = [{"name": f"source_{i}", "topic": f"data.source{i}.v1"} for i in range(5)]

            config = {
                "project": {"name": "fan-in", "version": "1.0.0"},
                "runtime": {
                    "kafka": {"bootstrap_servers": "localhost:9092"},
                    "flink": {
                        "default": "local",
                        "clusters": {
                            "local": {"type": "rest", "rest_url": "http://localhost:8082"}
                        },
                    },
                },
                "sources": sources,
                "models": [
                    {
                        "name": "enriched_model",

                        "sql": """
                            SELECT
                                s0.*,
                                s1.field1,
                                s2.field2,
                                s3.field3,
                                s4.field4
                            FROM {{ source("source_0") }} s0
                            LEFT JOIN {{ source("source_1") }} s1 ON s0.id = s1.id
                            LEFT JOIN {{ source("source_2") }} s2 ON s0.id = s2.id
                            LEFT JOIN {{ source("source_3") }} s3 ON s0.id = s3.id
                            LEFT JOIN {{ source("source_4") }} s4 ON s0.id = s4.id
                        """,
                    },
                ],
            }

            project_path = self._create_project(tmpdir, config)
            parser = ProjectParser(project_path)
            project = parser.parse()

            validator = ProjectValidator(project)
            result = validator.validate()
            assert result.is_valid

            dag_builder = DAGBuilder(project)
            dag = dag_builder.build()

            # Model should depend on all 5 sources
            upstream = dag.get_upstream("enriched_model")
            for i in range(5):
                assert f"source_{i}" in upstream

    def test_deep_pipeline(self):
        """
        SCENARIO: Deep pipeline with many sequential stages

        Story: A pipeline with 10 sequential transformation stages.
        Tests execution order and long dependency chains.

        S -> M1 -> M2 -> M3 -> ... -> M10
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            models = []
            for i in range(1, 11):
                if i == 1:
                    sql = "SELECT *, 'stage_1' as stage FROM {{ source('events') }}"
                else:
                    sql = f"SELECT *, 'stage_{i}' as stage FROM {{{{ ref('stage_{i - 1}') }}}}"

                models.append(
                    {
                        "name": f"stage_{i}",

                        "sql": sql,
                    }
                )

            config = {
                "project": {"name": "deep-pipeline", "version": "1.0.0"},
                "runtime": {
                    "kafka": {"bootstrap_servers": "localhost:9092"},
                    "flink": {
                        "default": "local",
                        "clusters": {
                            "local": {"type": "rest", "rest_url": "http://localhost:8082"}
                        },
                    },
                },
                "sources": [
                    {"name": "events", "topic": "events.raw.v1"},
                ],
                "models": models,
            }

            project_path = self._create_project(tmpdir, config)
            parser = ProjectParser(project_path)
            project = parser.parse()

            validator = ProjectValidator(project)
            result = validator.validate()
            assert result.is_valid

            dag_builder = DAGBuilder(project)
            dag = dag_builder.build()

            # Verify chain
            assert "events" in dag.get_upstream("stage_1")
            for i in range(2, 11):
                assert f"stage_{i - 1}" in dag.get_upstream(f"stage_{i}")

            # Verify topological order
            order = dag.topological_sort()
            for i in range(1, 10):
                assert order.index(f"stage_{i}") < order.index(f"stage_{i + 1}")

class TestCircularDependencyDetection:
    """Test circular dependency detection."""

    def _create_project(self, tmpdir: str, config: dict) -> Path:
        project_path = Path(tmpdir)
        with open(project_path / "stream_project.yml", "w") as f:
            yaml.dump(config, f)
        return project_path

    def test_simple_circular_dependency(self):
        """
        SCENARIO: Simple circular dependency A -> B -> A

        Story: Two models incorrectly reference each other.
        Should be detected and rejected.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                "project": {"name": "circular-simple", "version": "1.0.0"},
                "runtime": {
                    "kafka": {"bootstrap_servers": "localhost:9092"},
                    "flink": {
                        "default": "local",
                        "clusters": {
                            "local": {"type": "rest", "rest_url": "http://localhost:8082"}
                        },
                    },
                },
                "sources": [
                    {"name": "events", "topic": "events.v1"},
                ],
                "models": [
                    {
                        "name": "model_a",

                        "sql": "SELECT * FROM {{ ref('model_b') }}",
                    },
                    {
                        "name": "model_b",

                        "sql": "SELECT * FROM {{ ref('model_a') }}",
                    },
                ],
            }

            project_path = self._create_project(tmpdir, config)
            parser = ProjectParser(project_path)
            project = parser.parse()

            validator = ProjectValidator(project)
            result = validator.validate()

            # Should detect circular dependency
            assert not result.is_valid
            error_messages = [e.message for e in result.errors]
            assert any(
                "circular" in msg.lower() or "cycle" in msg.lower() for msg in error_messages
            )

    def test_transitive_circular_dependency(self):
        """
        SCENARIO: Transitive circular dependency A -> B -> C -> A

        Story: Three models form a cycle through transitive dependencies.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                "project": {"name": "circular-transitive", "version": "1.0.0"},
                "runtime": {
                    "kafka": {"bootstrap_servers": "localhost:9092"},
                    "flink": {
                        "default": "local",
                        "clusters": {
                            "local": {"type": "rest", "rest_url": "http://localhost:8082"}
                        },
                    },
                },
                "sources": [
                    {"name": "events", "topic": "events.v1"},
                ],
                "models": [
                    {
                        "name": "model_a",

                        "sql": "SELECT * FROM {{ ref('model_c') }}",
                    },
                    {
                        "name": "model_b",

                        "sql": "SELECT * FROM {{ ref('model_a') }}",
                    },
                    {
                        "name": "model_c",

                        "sql": "SELECT * FROM {{ ref('model_b') }}",
                    },
                ],
            }

            project_path = self._create_project(tmpdir, config)
            parser = ProjectParser(project_path)
            project = parser.parse()

            validator = ProjectValidator(project)
            result = validator.validate()

            assert not result.is_valid
            error_messages = [e.message for e in result.errors]
            assert any(
                "circular" in msg.lower() or "cycle" in msg.lower() for msg in error_messages
            )

    def test_self_reference(self):
        """
        SCENARIO: Model references itself

        Story: A model incorrectly references itself.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                "project": {"name": "self-reference", "version": "1.0.0"},
                "runtime": {
                    "kafka": {"bootstrap_servers": "localhost:9092"},
                    "flink": {
                        "default": "local",
                        "clusters": {
                            "local": {"type": "rest", "rest_url": "http://localhost:8082"}
                        },
                    },
                },
                "sources": [
                    {"name": "events", "topic": "events.v1"},
                ],
                "models": [
                    {
                        "name": "recursive_model",

                        "sql": "SELECT * FROM {{ ref('recursive_model') }}",
                    },
                ],
            }

            project_path = self._create_project(tmpdir, config)
            parser = ProjectParser(project_path)
            project = parser.parse()

            validator = ProjectValidator(project)
            result = validator.validate()

            assert not result.is_valid

class TestLargeScaleProjects:
    """Test large-scale project handling."""

    def _create_project(self, tmpdir: str, config: dict) -> Path:
        project_path = Path(tmpdir)
        with open(project_path / "stream_project.yml", "w") as f:
            yaml.dump(config, f)
        return project_path

    def test_many_sources(self):
        """
        SCENARIO: Project with many sources (50+)

        Story: Enterprise project ingesting from many external systems.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            sources = [
                {"name": f"source_{i}", "topic": f"external.system{i}.v1"} for i in range(50)
            ]

            config = {
                "project": {"name": "many-sources", "version": "1.0.0"},
                "runtime": {
                    "kafka": {"bootstrap_servers": "localhost:9092"},
                    "flink": {
                        "default": "local",
                        "clusters": {
                            "local": {"type": "rest", "rest_url": "http://localhost:8082"}
                        },
                    },
                },
                "sources": sources,
                "models": [
                    {
                        "name": "all_sources_combined",

                        "sql": " UNION ALL ".join(
                            [
                                f"SELECT '{i}' as source_id, * FROM {{{{ source('source_{i}') }}}}"
                                for i in range(50)
                            ]
                        ),
                    },
                ],
            }

            project_path = self._create_project(tmpdir, config)
            parser = ProjectParser(project_path)
            project = parser.parse()

            assert len(project.sources) == 50

            validator = ProjectValidator(project)
            result = validator.validate()
            assert result.is_valid

    def test_many_models(self):
        """
        SCENARIO: Project with many models (100+)

        Story: Large analytics platform with many derived models.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            models = []

            # Create 10 base models from source
            for i in range(10):
                models.append(
                    {
                        "name": f"base_{i}",

                        "sql": f"SELECT *, {i} as base_id FROM {{{{ source('events') }}}}",
                    }
                )

            # Create 90 derived models (9 per base)
            for base in range(10):
                for derived in range(9):
                    models.append(
                        {
                            "name": f"derived_{base}_{derived}",

                            "sql": f"SELECT *, {derived} as derived_id FROM {{{{ ref('base_{base}') }}}}",
                        }
                    )

            config = {
                "project": {"name": "many-models", "version": "1.0.0"},
                "runtime": {
                    "kafka": {"bootstrap_servers": "localhost:9092"},
                    "flink": {
                        "default": "local",
                        "clusters": {
                            "local": {"type": "rest", "rest_url": "http://localhost:8082"}
                        },
                    },
                },
                "sources": [
                    {"name": "events", "topic": "events.raw.v1"},
                ],
                "models": models,
            }

            project_path = self._create_project(tmpdir, config)
            parser = ProjectParser(project_path)
            project = parser.parse()

            assert len(project.models) == 100

            validator = ProjectValidator(project)
            result = validator.validate()
            assert result.is_valid

            dag_builder = DAGBuilder(project)
            dag = dag_builder.build()

            # Should have 101 nodes (1 source + 100 models)
            assert len(dag.nodes) == 101

class TestInvalidConfigurations:
    """Test invalid configuration handling."""

    def _create_project(self, tmpdir: str, config: dict) -> Path:
        project_path = Path(tmpdir)
        with open(project_path / "stream_project.yml", "w") as f:
            yaml.dump(config, f)
        return project_path

    def test_missing_source_reference(self):
        """
        SCENARIO: Model references non-existent source

        Story: Model SQL references a source that doesn't exist.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                "project": {"name": "missing-source", "version": "1.0.0"},
                "runtime": {
                    "kafka": {"bootstrap_servers": "localhost:9092"},
                    "flink": {
                        "default": "local",
                        "clusters": {
                            "local": {"type": "rest", "rest_url": "http://localhost:8082"}
                        },
                    },
                },
                "sources": [
                    {"name": "events", "topic": "events.v1"},
                ],
                "models": [
                    {
                        "name": "broken_model",

                        "sql": "SELECT * FROM {{ source('nonexistent_source') }}",
                    },
                ],
            }

            project_path = self._create_project(tmpdir, config)
            parser = ProjectParser(project_path)
            project = parser.parse()

            validator = ProjectValidator(project)
            result = validator.validate()

            assert not result.is_valid
            error_messages = [e.message for e in result.errors]
            assert any("nonexistent_source" in msg for msg in error_messages)

    def test_missing_model_reference(self):
        """
        SCENARIO: Model references non-existent model

        Story: Model SQL references another model that doesn't exist.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                "project": {"name": "missing-model", "version": "1.0.0"},
                "runtime": {
                    "kafka": {"bootstrap_servers": "localhost:9092"},
                    "flink": {
                        "default": "local",
                        "clusters": {
                            "local": {"type": "rest", "rest_url": "http://localhost:8082"}
                        },
                    },
                },
                "sources": [
                    {"name": "events", "topic": "events.v1"},
                ],
                "models": [
                    {
                        "name": "valid_model",

                        "sql": "SELECT * FROM {{ source('events') }}",
                    },
                    {
                        "name": "broken_model",

                        "sql": "SELECT * FROM {{ ref('nonexistent_model') }}",
                    },
                ],
            }

            project_path = self._create_project(tmpdir, config)
            parser = ProjectParser(project_path)
            project = parser.parse()

            validator = ProjectValidator(project)
            result = validator.validate()

            assert not result.is_valid
            error_messages = [e.message for e in result.errors]
            assert any("nonexistent_model" in msg for msg in error_messages)

    def test_duplicate_names(self):
        """
        SCENARIO: Duplicate model names

        Story: Two models have the same name.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                "project": {"name": "duplicate-names", "version": "1.0.0"},
                "runtime": {
                    "kafka": {"bootstrap_servers": "localhost:9092"},
                    "flink": {
                        "default": "local",
                        "clusters": {
                            "local": {"type": "rest", "rest_url": "http://localhost:8082"}
                        },
                    },
                },
                "sources": [
                    {"name": "events", "topic": "events.v1"},
                ],
                "models": [
                    {
                        "name": "duplicate_model",

                        "sql": "SELECT * FROM {{ source('events') }} WHERE type = 'A'",
                    },
                    {
                        "name": "duplicate_model",  # Same name!

                        "sql": "SELECT * FROM {{ source('events') }} WHERE type = 'B'",
                    },
                ],
            }

            project_path = self._create_project(tmpdir, config)
            parser = ProjectParser(project_path)
            project = parser.parse()

            validator = ProjectValidator(project)
            result = validator.validate()

            assert not result.is_valid
            error_messages = [e.message for e in result.errors]
            assert any("duplicate" in msg.lower() for msg in error_messages)

    def test_invalid_advanced_config(self):
        """
        SCENARIO: Invalid configuration in advanced section

        Story: Model uses an invalid value in the advanced section.
        Note: With auto-inference, materialization is always valid.
              This test validates that invalid advanced config values are caught.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                "project": {"name": "invalid-config", "version": "1.0.0"},
                "runtime": {
                    "kafka": {"bootstrap_servers": "localhost:9092"},
                    "flink": {
                        "default": "local",
                        "clusters": {
                            "local": {"type": "rest", "rest_url": "http://localhost:8082"}
                        },
                    },
                },
                "sources": [
                    {"name": "events", "topic": "events.v1"},
                ],
                "models": [
                    {
                        "name": "model_with_invalid_ttl",
                        "sql": "SELECT * FROM {{ source('events') }} GROUP BY event_id",
                        "advanced": {
                            "flink": {
                                "state_ttl_ms": -1000,  # Invalid: negative TTL
                            }
                        },
                    },
                ],
            }

            project_path = self._create_project(tmpdir, config)
            parser = ProjectParser(project_path)
            project = parser.parse()

            validator = ProjectValidator(project)
            result = validator.validate()

            # Should fail validation due to invalid state_ttl_ms
            assert not result.is_valid
            error_codes = [e.code for e in result.errors]
            assert "INVALID_STATE_TTL" in error_codes

class TestErrorRecoveryPatterns:
    """Test error recovery and DLQ patterns."""

    def _create_project(self, tmpdir: str, config: dict) -> Path:
        project_path = Path(tmpdir)
        with open(project_path / "stream_project.yml", "w") as f:
            yaml.dump(config, f)
        return project_path

    def test_dlq_pattern(self):
        """
        SCENARIO: Dead Letter Queue pattern for error handling

        Story: Process valid events, route invalid events to DLQ.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                "project": {"name": "dlq-pattern", "version": "1.0.0"},
                "runtime": {
                    "kafka": {"bootstrap_servers": "localhost:9092"},
                    "flink": {
                        "default": "local",
                        "clusters": {
                            "local": {"type": "rest", "rest_url": "http://localhost:8082"}
                        },
                    },
                },
                "sources": [
                    {"name": "events", "topic": "events.raw.v1"},
                ],
                "models": [
                    {
                        "name": "events_validated",
                        "description": "Valid events",

                        "sql": """
                            SELECT *
                            FROM {{ source("events") }}
                            WHERE event_id IS NOT NULL
                              AND user_id IS NOT NULL
                              AND event_time IS NOT NULL
                        """,
                    },
                    {
                        "name": "events_dlq",
                        "description": "Invalid events for investigation",

                        "topic": {
                            "name": "events.dlq.v1",
                            "partitions": 1,
                            "config": {"retention.ms": "604800000"},  # 7 days
                        },
                        "sql": """
                            SELECT
                                *,
                                CASE
                                    WHEN event_id IS NULL THEN 'MISSING_EVENT_ID'
                                    WHEN user_id IS NULL THEN 'MISSING_USER_ID'
                                    WHEN event_time IS NULL THEN 'MISSING_EVENT_TIME'
                                    ELSE 'UNKNOWN'
                                END as error_reason,
                                CURRENT_TIMESTAMP as dlq_time
                            FROM {{ source("events") }}
                            WHERE event_id IS NULL
                               OR user_id IS NULL
                               OR event_time IS NULL
                        """,
                    },
                ],
            }

            project_path = self._create_project(tmpdir, config)
            parser = ProjectParser(project_path)
            project = parser.parse()

            validator = ProjectValidator(project)
            result = validator.validate()
            assert result.is_valid

            # Both valid and DLQ models should exist
            model_names = [m.name for m in project.models]
            assert "events_validated" in model_names
            assert "events_dlq" in model_names

    def test_retry_pattern(self):
        """
        SCENARIO: Retry pattern for transient failures

        Story: Track retry attempts and route to DLQ after max retries.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                "project": {"name": "retry-pattern", "version": "1.0.0"},
                "runtime": {
                    "kafka": {"bootstrap_servers": "localhost:9092"},
                    "flink": {
                        "default": "local",
                        "clusters": {
                            "local": {"type": "rest", "rest_url": "http://localhost:8082"}
                        },
                    },
                },
                "sources": [
                    {"name": "events", "topic": "events.raw.v1"},
                    {"name": "retry_queue", "topic": "events.retry.v1"},
                ],
                "models": [
                    {
                        "name": "events_with_retries",
                        "description": "Events with retry tracking",

                        "sql": """
                            SELECT
                                event_id,
                                payload,
                                COALESCE(retry_count, 0) as retry_count,
                                event_time,
                                CURRENT_TIMESTAMP as processed_at
                            FROM {{ source("events") }}
                            UNION ALL
                            SELECT
                                event_id,
                                payload,
                                retry_count + 1 as retry_count,
                                event_time,
                                CURRENT_TIMESTAMP as processed_at
                            FROM {{ source("retry_queue") }}
                            WHERE retry_count < 3
                        """,
                    },
                    {
                        "name": "events_max_retries_dlq",
                        "description": "Events that exceeded max retries",

                        "sql": """
                            SELECT
                                event_id,
                                payload,
                                retry_count,
                                'MAX_RETRIES_EXCEEDED' as failure_reason,
                                event_time,
                                CURRENT_TIMESTAMP as dlq_time
                            FROM {{ source("retry_queue") }}
                            WHERE retry_count >= 3
                        """,
                    },
                ],
            }

            project_path = self._create_project(tmpdir, config)
            parser = ProjectParser(project_path)
            project = parser.parse()

            validator = ProjectValidator(project)
            result = validator.validate()
            assert result.is_valid

class TestResourceLimits:
    """Test resource limit configurations."""

    def _create_project(self, tmpdir: str, config: dict) -> Path:
        project_path = Path(tmpdir)
        with open(project_path / "stream_project.yml", "w") as f:
            yaml.dump(config, f)
        return project_path

    def test_parallelism_configuration(self):
        """
        SCENARIO: Configure Flink parallelism per model

        Story: Different models need different parallelism based on load.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                "project": {"name": "parallelism-config", "version": "1.0.0"},
                "runtime": {
                    "kafka": {"bootstrap_servers": "localhost:9092"},
                    "flink": {
                        "default": "local",
                        "clusters": {
                            "local": {"type": "rest", "rest_url": "http://localhost:8082"}
                        },
                        "default_parallelism": 4,
                    },
                },
                "sources": [
                    {"name": "events", "topic": "events.v1"},
                ],
                "models": [
                    {
                        "name": "high_throughput_model",

                        "flink": {"parallelism": 16},  # High parallelism
                        "sql": "SELECT * FROM {{ source('events') }}",
                    },
                    {
                        "name": "low_throughput_model",

                        "flink": {"parallelism": 2},  # Low parallelism
                        "sql": "SELECT * FROM {{ ref('high_throughput_model') }} WHERE is_special = true",
                    },
                    {
                        "name": "default_model",

                        # Uses default parallelism (4)
                        "sql": "SELECT * FROM {{ source('events') }} WHERE type = 'default'",
                    },
                ],
            }

            project_path = self._create_project(tmpdir, config)
            parser = ProjectParser(project_path)
            project = parser.parse()

            validator = ProjectValidator(project)
            result = validator.validate()
            assert result.is_valid

            # Verify parallelism settings
            high_model = next(m for m in project.models if m.name == "high_throughput_model")
            low_model = next(m for m in project.models if m.name == "low_throughput_model")

            assert high_model.flink.parallelism == 16
            assert low_model.flink.parallelism == 2

    def test_topic_partition_configuration(self):
        """
        SCENARIO: Configure topic partitions per model

        Story: Different models produce to topics with different partition counts.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                "project": {"name": "partition-config", "version": "1.0.0"},
                "runtime": {
                    "kafka": {"bootstrap_servers": "localhost:9092"},
                    "flink": {
                        "default": "local",
                        "clusters": {
                            "local": {"type": "rest", "rest_url": "http://localhost:8082"}
                        },
                    },
                },
                "sources": [
                    {"name": "events", "topic": "events.v1"},
                ],
                "models": [
                    {
                        "name": "high_volume",

                        "topic": {"partitions": 48},  # Many partitions
                        "sql": "SELECT * FROM {{ source('events') }}",
                    },
                    {
                        "name": "low_volume",

                        "topic": {"partitions": 3},  # Few partitions
                        "sql": "SELECT * FROM {{ ref('high_volume') }} WHERE is_rare = true",
                    },
                    {
                        "name": "aggregate_output",

                        "topic": {"partitions": 1},  # Single partition for ordering
                        "sql": """
                            SELECT COUNT(*) as total
                            FROM {{ ref('low_volume') }}
                        """,
                    },
                ],
            }

            project_path = self._create_project(tmpdir, config)
            parser = ProjectParser(project_path)
            project = parser.parse()

            validator = ProjectValidator(project)
            result = validator.validate()
            assert result.is_valid

            # Compile and verify partition counts
            compiler = Compiler(project)
            manifest = compiler.compile()

            # Find topics from artifacts
            topic_artifacts = manifest.artifacts.get("topics", [])
            topic_partitions = {t["name"]: t["partitions"] for t in topic_artifacts}

            # Verify partition counts are captured (at least one of each)
            partition_values = list(topic_partitions.values())
            assert 48 in partition_values
            assert 3 in partition_values
            assert 1 in partition_values
