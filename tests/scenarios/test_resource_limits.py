"""Resource limit configuration test scenarios."""

import tempfile
from pathlib import Path

import yaml

from streamt.compiler import Compiler
from streamt.core.parser import ProjectParser
from streamt.core.validator import ProjectValidator


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
                    },
                },
                "sources": [
                    {"name": "events", "topic": "events.v1"},
                ],
                "models": [
                    {
                        "name": "high_throughput_model",
                        "flink": {"parallelism": 16},
                        "sql": "SELECT * FROM {{ source('events') }}",
                    },
                    {
                        "name": "low_throughput_model",
                        "flink": {"parallelism": 2},
                        "sql": "SELECT * FROM {{ ref('high_throughput_model') }} WHERE is_special = true",
                    },
                    {
                        "name": "default_model",
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

            high_model = next(m for m in project.models if m.name == "high_throughput_model")
            low_model = next(m for m in project.models if m.name == "low_throughput_model")

            assert high_model.get_flink_config().parallelism == 16
            assert low_model.get_flink_config().parallelism == 2

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
                        "topic": {"partitions": 48},
                        "sql": "SELECT * FROM {{ source('events') }}",
                    },
                    {
                        "name": "low_volume",
                        "topic": {"partitions": 3},
                        "sql": "SELECT * FROM {{ ref('high_volume') }} WHERE is_rare = true",
                    },
                    {
                        "name": "aggregate_output",
                        "topic": {"partitions": 1},
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

            compiler = Compiler(project)
            manifest = compiler.compile()

            topic_artifacts = manifest.artifacts.get("topics", [])
            topic_partitions = {t["name"]: t["partitions"] for t in topic_artifacts}

            partition_values = list(topic_partitions.values())
            assert 48 in partition_values
            assert 3 in partition_values
            assert 1 in partition_values
