"""End-to-end tests for the streamt status command.

These tests verify that the status command correctly:
- Shows deployed topics, jobs, connectors
- Filters resources by name pattern
- Outputs in text and JSON formats
- Shows consumer lag when requested
"""

import json
import tempfile
import uuid
from pathlib import Path

import pytest
import yaml
from click.testing import CliRunner

from streamt.cli import main
from streamt.compiler.manifest import TopicArtifact
from streamt.deployer.kafka import KafkaDeployer

from .conftest import INFRA_CONFIG


@pytest.mark.integration
@pytest.mark.kafka
class TestStatusCommand:
    """Test the streamt status command."""

    def _create_project(self, tmpdir: Path, config: dict) -> Path:
        """Create a project configuration file."""
        with open(tmpdir / "stream_project.yml", "w") as f:
            yaml.dump(config, f)
        return tmpdir

    def test_status_shows_topics(self, docker_services):
        """TC-STATUS-001: Status command should show topic status."""
        runner = CliRunner()
        topic_name = f"test_status_topic_{uuid.uuid4().hex[:8]}"
        deployer = KafkaDeployer(INFRA_CONFIG.kafka_bootstrap_servers)

        try:
            # Create a topic
            artifact = TopicArtifact(
                name=topic_name,
                partitions=3,
                replication_factor=1,
            )
            deployer.apply_topic(artifact)

            with tempfile.TemporaryDirectory() as tmpdir:
                config = {
                    "project": {"name": "test-status", "version": "1.0.0"},
                    "runtime": {
                        "kafka": {"bootstrap_servers": INFRA_CONFIG.kafka_bootstrap_servers}
                    },
                    "sources": [{"name": "raw", "topic": "raw.v1"}],
                    "models": [
                        {
                            "name": topic_name,
                            "sql": 'SELECT * FROM {{ source("raw") }}',
                            "advanced": {"topic": {"partitions": 3}},
                        }
                    ],
                }
                project_path = self._create_project(Path(tmpdir), config)

                result = runner.invoke(main, ["status", "-p", str(project_path)])

                assert result.exit_code == 0
                assert topic_name in result.output
                assert "OK" in result.output
                assert "partitions: 3" in result.output

        finally:
            try:
                deployer.delete_topic(topic_name)
            except Exception:
                pass

    def test_status_shows_missing_topics(self, docker_services):
        """TC-STATUS-002: Status should show MISSING for non-existent topics."""
        runner = CliRunner()
        topic_name = f"nonexistent_topic_{uuid.uuid4().hex[:8]}"

        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                "project": {"name": "test-missing", "version": "1.0.0"},
                "runtime": {
                    "kafka": {"bootstrap_servers": INFRA_CONFIG.kafka_bootstrap_servers}
                },
                "sources": [{"name": "raw", "topic": "raw.v1"}],
                "models": [
                    {
                        "name": topic_name,
                        "sql": 'SELECT * FROM {{ source("raw") }}',
                    }
                ],
            }
            project_path = self._create_project(Path(tmpdir), config)

            result = runner.invoke(main, ["status", "-p", str(project_path)])

            assert result.exit_code == 0
            assert topic_name in result.output
            assert "MISSING" in result.output

    def test_status_json_output(self, docker_services):
        """TC-STATUS-003: Status with --format json should output valid JSON."""
        runner = CliRunner()
        topic_name = f"test_json_topic_{uuid.uuid4().hex[:8]}"
        deployer = KafkaDeployer(INFRA_CONFIG.kafka_bootstrap_servers)

        try:
            # Create a topic
            artifact = TopicArtifact(
                name=topic_name,
                partitions=2,
                replication_factor=1,
            )
            deployer.apply_topic(artifact)

            with tempfile.TemporaryDirectory() as tmpdir:
                config = {
                    "project": {"name": "test-json", "version": "1.0.0"},
                    "runtime": {
                        "kafka": {"bootstrap_servers": INFRA_CONFIG.kafka_bootstrap_servers}
                    },
                    "sources": [{"name": "raw", "topic": "raw.v1"}],
                    "models": [
                        {
                            "name": topic_name,
                            "sql": 'SELECT * FROM {{ source("raw") }}',
                            "advanced": {"topic": {"partitions": 2}},
                        }
                    ],
                }
                project_path = self._create_project(Path(tmpdir), config)

                result = runner.invoke(
                    main, ["status", "-p", str(project_path), "--format", "json"]
                )

                assert result.exit_code == 0

                # Parse JSON output
                status_data = json.loads(result.output)
                assert "project" in status_data
                assert "topics" in status_data
                assert status_data["project"] == "test-json"

                # Find our topic
                topic = next(
                    (t for t in status_data["topics"] if t["name"] == topic_name), None
                )
                assert topic is not None
                assert topic["exists"] is True
                assert topic["partitions"] == 2

        finally:
            try:
                deployer.delete_topic(topic_name)
            except Exception:
                pass

    def test_status_filter_by_name(self, docker_services):
        """TC-STATUS-004: Status with --filter should only show matching resources."""
        runner = CliRunner()
        prefix = f"payments_{uuid.uuid4().hex[:4]}"
        topic1 = f"{prefix}_raw"
        topic2 = f"{prefix}_clean"
        other_topic = f"orders_{uuid.uuid4().hex[:8]}"
        deployer = KafkaDeployer(INFRA_CONFIG.kafka_bootstrap_servers)

        try:
            # Create topics
            for name in [topic1, topic2, other_topic]:
                artifact = TopicArtifact(name=name, partitions=1, replication_factor=1)
                deployer.apply_topic(artifact)

            with tempfile.TemporaryDirectory() as tmpdir:
                config = {
                    "project": {"name": "test-filter", "version": "1.0.0"},
                    "runtime": {
                        "kafka": {"bootstrap_servers": INFRA_CONFIG.kafka_bootstrap_servers}
                    },
                    "sources": [{"name": "raw", "topic": "raw.v1"}],
                    "models": [
                        {
                            "name": topic1,
                            "sql": 'SELECT * FROM {{ source("raw") }}',
                        },
                        {
                            "name": topic2,
                            "sql": 'SELECT * FROM {{ source("raw") }}',
                        },
                        {
                            "name": other_topic,
                            "sql": 'SELECT * FROM {{ source("raw") }}',
                        },
                    ],
                }
                project_path = self._create_project(Path(tmpdir), config)

                # Filter for payments topics only
                result = runner.invoke(
                    main,
                    ["status", "-p", str(project_path), "--filter", f"{prefix}*"],
                )

                assert result.exit_code == 0
                assert topic1 in result.output
                assert topic2 in result.output
                assert other_topic not in result.output

        finally:
            for name in [topic1, topic2, other_topic]:
                try:
                    deployer.delete_topic(name)
                except Exception:
                    pass

    def test_status_with_lag_flag(self, docker_services, kafka_helper):
        """TC-STATUS-005: Status with --lag should show message counts."""
        runner = CliRunner()
        topic_name = f"test_lag_topic_{uuid.uuid4().hex[:8]}"

        try:
            # Create topic and produce messages
            kafka_helper.create_topic(topic_name, partitions=2)

            # Produce some messages
            messages = [{"id": i, "value": f"msg_{i}"} for i in range(10)]
            kafka_helper.produce_messages(topic_name, messages, key_field="id")

            with tempfile.TemporaryDirectory() as tmpdir:
                config = {
                    "project": {"name": "test-lag", "version": "1.0.0"},
                    "runtime": {
                        "kafka": {"bootstrap_servers": INFRA_CONFIG.kafka_bootstrap_servers}
                    },
                    "sources": [{"name": "raw", "topic": "raw.v1"}],
                    "models": [
                        {
                            "name": topic_name,
                            "sql": 'SELECT * FROM {{ source("raw") }}',
                            "advanced": {"topic": {"partitions": 2}},
                        }
                    ],
                }
                project_path = self._create_project(Path(tmpdir), config)

                result = runner.invoke(
                    main, ["status", "-p", str(project_path), "--lag"]
                )

                assert result.exit_code == 0
                assert topic_name in result.output
                # Should show message count
                assert "msgs" in result.output

        finally:
            try:
                kafka_helper.delete_topic(topic_name)
            except Exception:
                pass

    def test_status_shows_summary(self, docker_services):
        """TC-STATUS-006: Status should show a summary at the end."""
        runner = CliRunner()
        topic_name = f"test_summary_topic_{uuid.uuid4().hex[:8]}"
        deployer = KafkaDeployer(INFRA_CONFIG.kafka_bootstrap_servers)

        try:
            # Create a topic
            artifact = TopicArtifact(
                name=topic_name,
                partitions=1,
                replication_factor=1,
            )
            deployer.apply_topic(artifact)

            with tempfile.TemporaryDirectory() as tmpdir:
                config = {
                    "project": {"name": "test-summary", "version": "1.0.0"},
                    "runtime": {
                        "kafka": {"bootstrap_servers": INFRA_CONFIG.kafka_bootstrap_servers}
                    },
                    "sources": [{"name": "raw", "topic": "raw.v1"}],
                    "models": [
                        {
                            "name": topic_name,
                            "sql": 'SELECT * FROM {{ source("raw") }}',
                        }
                    ],
                }
                project_path = self._create_project(Path(tmpdir), config)

                result = runner.invoke(main, ["status", "-p", str(project_path)])

                assert result.exit_code == 0
                # Should have summary line
                assert "Summary:" in result.output
                assert "Topics:" in result.output

        finally:
            try:
                deployer.delete_topic(topic_name)
            except Exception:
                pass


@pytest.mark.integration
@pytest.mark.kafka
class TestStatusCommandEdgeCases:
    """Test edge cases for the status command."""

    def _create_project(self, tmpdir: Path, config: dict) -> Path:
        """Create a project configuration file."""
        with open(tmpdir / "stream_project.yml", "w") as f:
            yaml.dump(config, f)
        return tmpdir

    def test_status_empty_project(self, docker_services):
        """TC-STATUS-007: Status for project with no models should work."""
        runner = CliRunner()

        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                "project": {"name": "empty-project", "version": "1.0.0"},
                "runtime": {
                    "kafka": {"bootstrap_servers": INFRA_CONFIG.kafka_bootstrap_servers}
                },
                "sources": [{"name": "raw", "topic": "raw.v1"}],
                # No models
            }
            project_path = self._create_project(Path(tmpdir), config)

            result = runner.invoke(main, ["status", "-p", str(project_path)])

            # Should not crash
            assert result.exit_code == 0

    def test_status_json_includes_all_fields(self, docker_services):
        """TC-STATUS-008: JSON output should include all expected fields."""
        runner = CliRunner()
        topic_name = f"test_fields_topic_{uuid.uuid4().hex[:8]}"
        deployer = KafkaDeployer(INFRA_CONFIG.kafka_bootstrap_servers)

        try:
            artifact = TopicArtifact(
                name=topic_name,
                partitions=3,
                replication_factor=1,
            )
            deployer.apply_topic(artifact)

            with tempfile.TemporaryDirectory() as tmpdir:
                config = {
                    "project": {"name": "test-fields", "version": "1.0.0"},
                    "runtime": {
                        "kafka": {"bootstrap_servers": INFRA_CONFIG.kafka_bootstrap_servers}
                    },
                    "sources": [{"name": "raw", "topic": "raw.v1"}],
                    "models": [
                        {
                            "name": topic_name,
                            "sql": 'SELECT * FROM {{ source("raw") }}',
                            "advanced": {"topic": {"partitions": 3}},
                        }
                    ],
                }
                project_path = self._create_project(Path(tmpdir), config)

                result = runner.invoke(
                    main, ["status", "-p", str(project_path), "--format", "json"]
                )

                assert result.exit_code == 0
                status_data = json.loads(result.output)

                # Check top-level fields
                assert "project" in status_data
                assert "topics" in status_data
                assert "schemas" in status_data
                assert "flink_jobs" in status_data
                assert "connectors" in status_data

                # Check topic fields
                topic = status_data["topics"][0]
                assert "name" in topic
                assert "exists" in topic
                assert "partitions" in topic
                assert "replication_factor" in topic

        finally:
            try:
                deployer.delete_topic(topic_name)
            except Exception:
                pass
