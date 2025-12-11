"""End-to-end tests for event time and watermark configuration.

These tests verify that:
- Event time configuration generates correct Flink SQL
- Watermark strategies are correctly applied
- Flink can parse and execute the generated SQL with watermarks
"""

import json
import tempfile
import time
import uuid
from pathlib import Path

import pytest
import yaml

from streamt.compiler import Compiler
from streamt.core.parser import ProjectParser
from streamt.deployer.flink import FlinkDeployer
from streamt.deployer.kafka import KafkaDeployer

from .conftest import INFRA_CONFIG, FlinkHelper


@pytest.mark.integration
@pytest.mark.flink
class TestEventTimeFlinkSQL:
    """Test event time configuration with Flink SQL execution."""

    def _create_project(self, tmpdir: Path, config: dict):
        """Create a project and return parsed project."""
        with open(tmpdir / "stream_project.yml", "w") as f:
            yaml.dump(config, f)
        parser = ProjectParser(tmpdir)
        return parser.parse()

    def test_event_time_sql_is_valid_flink_sql(
        self, docker_services, flink_helper: FlinkHelper, kafka_helper
    ):
        """TC-EVENTTIME-E2E-001: Generated SQL with watermarks should be valid Flink SQL."""
        topic_name = f"test_et_source_{uuid.uuid4().hex[:8]}"
        output_topic = f"test_et_output_{uuid.uuid4().hex[:8]}"

        try:
            # Create topics
            kafka_helper.create_topic(topic_name, partitions=2)
            kafka_helper.create_topic(output_topic, partitions=2)

            with tempfile.TemporaryDirectory() as tmpdir:
                config = {
                    "project": {"name": "test-event-time", "version": "1.0.0"},
                    "runtime": {
                        "kafka": {
                            "bootstrap_servers": INFRA_CONFIG.kafka_bootstrap_servers,
                            "bootstrap_servers_internal": "kafka:29092",
                        },
                        "flink": {
                            "default": "local",
                            "clusters": {
                                "local": {
                                    "type": "rest",
                                    "rest_url": INFRA_CONFIG.flink_rest_url,
                                    "sql_gateway_url": INFRA_CONFIG.flink_sql_gateway_url,
                                }
                            },
                        },
                    },
                    "sources": [
                        {
                            "name": "events",
                            "topic": topic_name,
                            "columns": [
                                {"name": "event_id"},
                                {"name": "event_timestamp"},
                                {"name": "value"},
                            ],
                            "event_time": {
                                "column": "event_timestamp",
                                "watermark": {
                                    "strategy": "bounded_out_of_orderness",
                                    "max_out_of_orderness_ms": 5000,
                                },
                            },
                        }
                    ],
                    "models": [
                        {
                            "name": output_topic,
                            "sql": f"""
                                SELECT event_id, event_timestamp, value
                                FROM {{{{ source("events") }}}}
                            """,
                        }
                    ],
                }
                project = self._create_project(Path(tmpdir), config)
                output_dir = Path(tmpdir) / "generated"
                compiler = Compiler(project, output_dir)
                manifest = compiler.compile(dry_run=False)

                # Read the generated SQL
                sql_path = output_dir / "flink" / f"{output_topic}.sql"
                assert sql_path.exists(), "SQL file should be generated"

                sql_content = sql_path.read_text()

                # Verify watermark is in the SQL
                assert "WATERMARK FOR `event_timestamp`" in sql_content
                assert "INTERVAL '5' SECOND" in sql_content
                assert "`event_timestamp` TIMESTAMP(3)" in sql_content

                # Try to execute the CREATE TABLE statements (not the INSERT)
                # Split into statements and execute DDL only
                statements = sql_content.split(";")
                ddl_statements = [s.strip() for s in statements if "CREATE TABLE" in s]

                for ddl in ddl_statements:
                    if ddl:
                        try:
                            flink_helper.execute_sql(ddl + ";")
                        except Exception as e:
                            pytest.fail(f"DDL failed: {e}\nSQL: {ddl}")

        finally:
            try:
                kafka_helper.delete_topic(topic_name)
                kafka_helper.delete_topic(output_topic)
            except Exception:
                pass

    def test_monotonously_increasing_watermark(
        self, docker_services, flink_helper: FlinkHelper, kafka_helper
    ):
        """TC-EVENTTIME-E2E-002: Monotonously increasing watermark should be valid."""
        topic_name = f"test_mono_source_{uuid.uuid4().hex[:8]}"
        output_topic = f"test_mono_output_{uuid.uuid4().hex[:8]}"

        try:
            kafka_helper.create_topic(topic_name, partitions=1)
            kafka_helper.create_topic(output_topic, partitions=1)

            with tempfile.TemporaryDirectory() as tmpdir:
                config = {
                    "project": {"name": "test-mono", "version": "1.0.0"},
                    "runtime": {
                        "kafka": {
                            "bootstrap_servers": INFRA_CONFIG.kafka_bootstrap_servers,
                            "bootstrap_servers_internal": "kafka:29092",
                        },
                        "flink": {
                            "default": "local",
                            "clusters": {
                                "local": {
                                    "type": "rest",
                                    "rest_url": INFRA_CONFIG.flink_rest_url,
                                    "sql_gateway_url": INFRA_CONFIG.flink_sql_gateway_url,
                                }
                            },
                        },
                    },
                    "sources": [
                        {
                            "name": "ordered_events",
                            "topic": topic_name,
                            "columns": [
                                {"name": "id"},
                                {"name": "ts"},
                            ],
                            "event_time": {
                                "column": "ts",
                                "watermark": {
                                    "strategy": "monotonously_increasing",
                                },
                            },
                        }
                    ],
                    "models": [
                        {
                            "name": output_topic,
                            "sql": f"""
                                SELECT id, ts
                                FROM {{{{ source("ordered_events") }}}}
                            """,
                        }
                    ],
                }
                project = self._create_project(Path(tmpdir), config)
                output_dir = Path(tmpdir) / "generated"
                compiler = Compiler(project, output_dir)
                compiler.compile(dry_run=False)

                sql_path = output_dir / "flink" / f"{output_topic}.sql"
                sql_content = sql_path.read_text()

                # Verify monotonous watermark (no INTERVAL)
                assert "WATERMARK FOR `ts` AS `ts`" in sql_content

                # Execute DDL to verify it's valid
                statements = sql_content.split(";")
                ddl_statements = [s.strip() for s in statements if "CREATE TABLE" in s]

                for ddl in ddl_statements:
                    if ddl:
                        flink_helper.execute_sql(ddl + ";")

        finally:
            try:
                kafka_helper.delete_topic(topic_name)
                kafka_helper.delete_topic(output_topic)
            except Exception:
                pass

    def test_event_time_with_different_delays(
        self, docker_services, flink_helper: FlinkHelper, kafka_helper
    ):
        """TC-EVENTTIME-E2E-003: Different watermark delays should generate correct SQL."""
        delays_to_test = [1000, 10000, 60000, 300000]  # 1s, 10s, 1min, 5min

        for delay_ms in delays_to_test:
            topic_name = f"test_delay_{delay_ms}_{uuid.uuid4().hex[:8]}"
            output_topic = f"test_delay_out_{delay_ms}_{uuid.uuid4().hex[:8]}"

            try:
                kafka_helper.create_topic(topic_name, partitions=1)
                kafka_helper.create_topic(output_topic, partitions=1)

                with tempfile.TemporaryDirectory() as tmpdir:
                    config = {
                        "project": {"name": f"test-delay-{delay_ms}", "version": "1.0.0"},
                        "runtime": {
                            "kafka": {
                                "bootstrap_servers": INFRA_CONFIG.kafka_bootstrap_servers,
                                "bootstrap_servers_internal": "kafka:29092",
                            },
                            "flink": {
                                "default": "local",
                                "clusters": {
                                    "local": {
                                        "type": "rest",
                                        "rest_url": INFRA_CONFIG.flink_rest_url,
                                        "sql_gateway_url": INFRA_CONFIG.flink_sql_gateway_url,
                                    }
                                },
                            },
                        },
                        "sources": [
                            {
                                "name": "events",
                                "topic": topic_name,
                                "columns": [
                                    {"name": "id"},
                                    {"name": "event_time"},
                                ],
                                "event_time": {
                                    "column": "event_time",
                                    "watermark": {
                                        "max_out_of_orderness_ms": delay_ms,
                                    },
                                },
                            }
                        ],
                        "models": [
                            {
                                "name": output_topic,
                                "sql": """
                                    SELECT id, event_time
                                    FROM {{ source("events") }}
                                """,
                            }
                        ],
                    }
                    project = self._create_project(Path(tmpdir), config)
                    output_dir = Path(tmpdir) / "generated"
                    compiler = Compiler(project, output_dir)
                    compiler.compile(dry_run=False)

                    sql_path = output_dir / "flink" / f"{output_topic}.sql"
                    sql_content = sql_path.read_text()

                    expected_seconds = delay_ms // 1000
                    assert f"INTERVAL '{expected_seconds}' SECOND" in sql_content, \
                        f"Expected {expected_seconds}s delay for {delay_ms}ms"

            finally:
                try:
                    kafka_helper.delete_topic(topic_name)
                    kafka_helper.delete_topic(output_topic)
                except Exception:
                    pass


@pytest.mark.integration
@pytest.mark.kafka
class TestEventTimeYAMLParsing:
    """Test YAML parsing of event time configuration."""

    def test_event_time_config_parsed_correctly(self):
        """TC-EVENTTIME-PARSE-001: Event time config should be parsed from YAML."""
        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                "project": {"name": "test-parse", "version": "1.0.0"},
                "runtime": {"kafka": {"bootstrap_servers": "localhost:9092"}},
                "sources": [
                    {
                        "name": "events",
                        "topic": "events.v1",
                        "columns": [{"name": "id"}, {"name": "ts"}],
                        "event_time": {
                            "column": "ts",
                            "watermark": {
                                "strategy": "bounded_out_of_orderness",
                                "max_out_of_orderness_ms": 15000,
                            },
                            "allowed_lateness_ms": 60000,
                        },
                    }
                ],
            }
            with open(Path(tmpdir) / "stream_project.yml", "w") as f:
                yaml.dump(config, f)

            parser = ProjectParser(Path(tmpdir))
            project = parser.parse()

            source = project.sources[0]
            assert source.event_time is not None
            assert source.event_time.column == "ts"
            assert source.event_time.watermark is not None
            assert source.event_time.watermark.max_out_of_orderness_ms == 15000
            assert source.event_time.allowed_lateness_ms == 60000

    def test_minimal_event_time_config(self):
        """TC-EVENTTIME-PARSE-002: Minimal event_time config (just column) should work."""
        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                "project": {"name": "test-minimal", "version": "1.0.0"},
                "runtime": {"kafka": {"bootstrap_servers": "localhost:9092"}},
                "sources": [
                    {
                        "name": "events",
                        "topic": "events.v1",
                        "columns": [{"name": "event_timestamp"}],
                        "event_time": {
                            "column": "event_timestamp",
                        },
                    }
                ],
            }
            with open(Path(tmpdir) / "stream_project.yml", "w") as f:
                yaml.dump(config, f)

            parser = ProjectParser(Path(tmpdir))
            project = parser.parse()

            source = project.sources[0]
            assert source.event_time is not None
            assert source.event_time.column == "event_timestamp"
            # Watermark should be None (will use default in compiler)
            assert source.event_time.watermark is None

    def test_event_time_in_manifest(self):
        """TC-EVENTTIME-PARSE-003: Event time config should appear in manifest."""
        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                "project": {"name": "test-manifest", "version": "1.0.0"},
                "runtime": {"kafka": {"bootstrap_servers": "localhost:9092"}},
                "sources": [
                    {
                        "name": "events",
                        "topic": "events.v1",
                        "columns": [{"name": "ts"}],
                        "event_time": {
                            "column": "ts",
                            "watermark": {
                                "max_out_of_orderness_ms": 10000,
                            },
                        },
                    }
                ],
            }
            with open(Path(tmpdir) / "stream_project.yml", "w") as f:
                yaml.dump(config, f)

            parser = ProjectParser(Path(tmpdir))
            project = parser.parse()
            compiler = Compiler(project)
            manifest = compiler.compile(dry_run=True)

            manifest_dict = manifest.to_dict()
            source_in_manifest = manifest_dict["sources"][0]

            assert "event_time" in source_in_manifest
            assert source_in_manifest["event_time"]["column"] == "ts"
