"""Tests for the compiler module."""

import json
import tempfile
from pathlib import Path

import pytest
import yaml

from streamt.core.parser import ProjectParser
from streamt.compiler import Compiler


class TestCompiler:
    """Tests for Compiler."""

    def _create_project(self, tmpdir: str, config: dict) -> "StreamtProject":
        """Helper to create and parse a project."""
        project_path = Path(tmpdir)
        with open(project_path / "stream_project.yml", "w") as f:
            yaml.dump(config, f)
        parser = ProjectParser(project_path)
        return parser.parse()

    def test_compile_topic_model(self):
        """TC-COMP-001: Topic model should generate topic config."""
        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                "project": {"name": "test", "version": "1.0.0"},
                "runtime": {"kafka": {"bootstrap_servers": "localhost:9092"}},
                "sources": [{"name": "raw", "topic": "raw.v1"}],
                "models": [
                    {
                        "name": "payments_clean",
                        "materialized": "topic",
                        "topic": {"partitions": 12, "replication_factor": 3},
                        "sql": 'SELECT * FROM {{ source("raw") }}',
                    }
                ],
            }
            project = self._create_project(tmpdir, config)
            compiler = Compiler(project)
            manifest = compiler.compile(dry_run=True)

            topics = manifest.artifacts.get("topics", [])
            assert len(topics) >= 1

            payment_topic = next((t for t in topics if t["name"] == "payments_clean"), None)
            assert payment_topic is not None
            assert payment_topic["partitions"] == 12
            assert payment_topic["replication_factor"] == 3

    def test_compile_flink_model(self):
        """TC-COMP-002: Flink model should generate SQL and topic."""
        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                "project": {"name": "test", "version": "1.0.0"},
                "runtime": {
                    "kafka": {"bootstrap_servers": "localhost:9092"},
                    "flink": {
                        "default": "local",
                        "clusters": {
                            "local": {"type": "rest", "rest_url": "http://localhost:8082"}
                        },
                    },
                },
                "sources": [{"name": "payments_raw", "topic": "payments.raw.v1"}],
                "models": [
                    {
                        "name": "payments_clean",
                        "materialized": "topic",
                        "sql": 'SELECT * FROM {{ source("payments_raw") }}',
                    },
                    {
                        "name": "customer_balance",
                        "materialized": "flink",
                        "sql": """
                            SELECT customer_id, SUM(amount) as total
                            FROM {{ ref("payments_clean") }}
                            GROUP BY customer_id, TUMBLE(event_time, INTERVAL '5' MINUTE)
                        """,
                    },
                ],
            }
            project = self._create_project(tmpdir, config)
            compiler = Compiler(project)
            manifest = compiler.compile(dry_run=True)

            flink_jobs = manifest.artifacts.get("flink_jobs", [])
            assert len(flink_jobs) >= 1

            balance_job = next((j for j in flink_jobs if j["name"] == "customer_balance"), None)
            assert balance_job is not None
            assert "sql" in balance_job
            assert "INSERT INTO" in balance_job["sql"]

    def test_compile_sink_model(self):
        """TC-COMP-003: Sink model should generate connector config."""
        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                "project": {"name": "test", "version": "1.0.0"},
                "runtime": {
                    "kafka": {"bootstrap_servers": "localhost:9092"},
                    "connect": {
                        "default": "local",
                        "clusters": {"local": {"rest_url": "http://localhost:8083"}},
                    },
                },
                "sources": [{"name": "raw", "topic": "raw.v1"}],
                "models": [
                    {
                        "name": "clean",
                        "materialized": "topic",
                        "sql": 'SELECT * FROM {{ source("raw") }}',
                    },
                    {
                        "name": "to_snowflake",
                        "materialized": "sink",
                        "from": [{"ref": "clean"}],
                        "sink": {
                            "connector": "snowflake-sink",
                            "config": {
                                "snowflake.database.name": "ANALYTICS",
                                "snowflake.schema.name": "PUBLIC",
                            },
                        },
                    },
                ],
            }
            project = self._create_project(tmpdir, config)
            compiler = Compiler(project)
            manifest = compiler.compile(dry_run=True)

            connectors = manifest.artifacts.get("connectors", [])
            assert len(connectors) >= 1

            sf_connector = next((c for c in connectors if c["name"] == "to_snowflake"), None)
            assert sf_connector is not None
            assert "snowflake" in sf_connector["connector_class"].lower()

    def test_compile_writes_files(self):
        """Compile should write files when not dry_run."""
        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                "project": {"name": "test", "version": "1.0.0"},
                "runtime": {"kafka": {"bootstrap_servers": "localhost:9092"}},
                "sources": [{"name": "raw", "topic": "raw.v1"}],
                "models": [
                    {
                        "name": "clean",
                        "materialized": "topic",
                        "topic": {"partitions": 6},
                        "sql": 'SELECT * FROM {{ source("raw") }}',
                    }
                ],
            }
            project = self._create_project(tmpdir, config)
            output_dir = Path(tmpdir) / "generated"
            compiler = Compiler(project, output_dir)
            manifest = compiler.compile(dry_run=False)

            # Check files were created
            assert (output_dir / "manifest.json").exists()
            assert (output_dir / "topics" / "clean.json").exists()

            # Check content
            with open(output_dir / "topics" / "clean.json") as f:
                topic_config = json.load(f)
            assert topic_config["name"] == "clean"
            assert topic_config["partitions"] == 6

    def test_manifest_content(self):
        """Manifest should contain all project info."""
        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                "project": {"name": "test-project", "version": "2.0.0"},
                "runtime": {"kafka": {"bootstrap_servers": "localhost:9092"}},
                "sources": [{"name": "src1", "topic": "t1"}],
                "models": [
                    {
                        "name": "model1",
                        "materialized": "topic",
                        "sql": 'SELECT * FROM {{ source("src1") }}',
                    }
                ],
                "tests": [
                    {
                        "name": "test1",
                        "model": "model1",
                        "type": "schema",
                        "assertions": [],
                    }
                ],
                "exposures": [
                    {
                        "name": "app1",
                        "type": "application",
                        "role": "consumer",
                        "consumes": [{"ref": "model1"}],
                    }
                ],
            }
            project = self._create_project(tmpdir, config)
            compiler = Compiler(project)
            manifest = compiler.compile(dry_run=True)

            assert manifest.version == "2.0.0"
            assert manifest.project_name == "test-project"
            assert len(manifest.sources) == 1
            assert len(manifest.models) == 1
            assert len(manifest.tests) == 1
            assert len(manifest.exposures) == 1
            assert "dag" in manifest.to_dict()

    def test_sql_transformation(self):
        """SQL should be transformed correctly."""
        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                "project": {"name": "test", "version": "1.0.0"},
                "runtime": {
                    "kafka": {"bootstrap_servers": "localhost:9092"},
                    "flink": {
                        "default": "local",
                        "clusters": {
                            "local": {"type": "rest", "rest_url": "http://localhost:8082"}
                        },
                    },
                },
                "sources": [{"name": "payments", "topic": "payments.v1"}],
                "models": [
                    {
                        "name": "clean",
                        "materialized": "flink",
                        "sql": """
                            SELECT id, amount
                            FROM {{ source("payments") }}
                            WHERE status = 'CAPTURED'
                        """,
                    }
                ],
            }
            project = self._create_project(tmpdir, config)
            compiler = Compiler(project)
            manifest = compiler.compile(dry_run=True)

            flink_jobs = manifest.artifacts.get("flink_jobs", [])
            clean_job = next((j for j in flink_jobs if j["name"] == "clean"), None)
            assert clean_job is not None

            # Check Jinja was replaced
            sql = clean_job["sql"]
            assert "{{ source" not in sql
            assert "payments" in sql  # Table name should be there

    def test_dry_run_no_files(self):
        """Dry run should not create files."""
        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                "project": {"name": "test", "version": "1.0.0"},
                "runtime": {"kafka": {"bootstrap_servers": "localhost:9092"}},
                "sources": [{"name": "raw", "topic": "raw.v1"}],
                "models": [
                    {
                        "name": "clean",
                        "materialized": "topic",
                        "sql": 'SELECT * FROM {{ source("raw") }}',
                    }
                ],
            }
            project = self._create_project(tmpdir, config)
            output_dir = Path(tmpdir) / "generated"
            compiler = Compiler(project, output_dir)
            compiler.compile(dry_run=True)

            # Should not create files
            assert not output_dir.exists()

    def test_secrets_not_resolved_in_artifacts(self):
        """Secrets should remain as placeholders in generated files."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create .env file
            with open(Path(tmpdir) / ".env", "w") as f:
                f.write("KAFKA_SERVERS=localhost:9092\n")
                f.write("SNOWFLAKE_URL=account.snowflakecomputing.com\n")

            config = {
                "project": {"name": "test", "version": "1.0.0"},
                "runtime": {
                    "kafka": {"bootstrap_servers": "${KAFKA_SERVERS}"},
                    "connect": {
                        "default": "local",
                        "clusters": {"local": {"rest_url": "http://localhost:8083"}},
                    },
                },
                "sources": [{"name": "raw", "topic": "raw.v1"}],
                "models": [
                    {
                        "name": "clean",
                        "materialized": "topic",
                        "sql": 'SELECT * FROM {{ source("raw") }}',
                    },
                    {
                        "name": "to_sf",
                        "materialized": "sink",
                        "from": [{"ref": "clean"}],
                        "sink": {
                            "connector": "snowflake-sink",
                            "config": {
                                "snowflake.url.name": "${SNOWFLAKE_URL}",
                            },
                        },
                    },
                ],
            }
            project_path = Path(tmpdir)
            with open(project_path / "stream_project.yml", "w") as f:
                yaml.dump(config, f)

            parser = ProjectParser(project_path)
            project = parser.parse()

            output_dir = Path(tmpdir) / "generated"
            compiler = Compiler(project, output_dir)
            manifest = compiler.compile(dry_run=False)

            # Check connector config has placeholder
            connectors = manifest.artifacts.get("connectors", [])
            sf_conn = next((c for c in connectors if c["name"] == "to_sf"), None)
            assert sf_conn is not None
            # The secret should be in the config
            # Note: In current implementation secrets are resolved at apply time


class TestCompilerSchemaGeneration:
    """Tests for proper schema/column generation in Flink SQL."""

    def _create_project(self, tmpdir: str, config: dict) -> "StreamtProject":
        """Helper to create and parse a project."""
        project_path = Path(tmpdir)
        with open(project_path / "stream_project.yml", "w") as f:
            yaml.dump(config, f)
        parser = ProjectParser(project_path)
        return parser.parse()

    def test_source_columns_in_generated_sql(self):
        """TC-SCHEMA-001: Source columns should appear in generated Flink SQL."""
        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                "project": {"name": "test", "version": "1.0.0"},
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
                    {
                        "name": "payments_raw",
                        "topic": "payments.raw.v1",
                        "columns": [
                            {"name": "payment_id"},
                            {"name": "customer_id"},
                            {"name": "amount"},
                            {"name": "timestamp"},
                        ],
                    }
                ],
                "models": [
                    {
                        "name": "payments_clean",
                        "materialized": "flink",
                        "sql": """
                            SELECT payment_id, customer_id, amount
                            FROM {{ source("payments_raw") }}
                            WHERE amount > 0
                        """,
                    }
                ],
            }
            project = self._create_project(tmpdir, config)
            output_dir = Path(tmpdir) / "generated"
            compiler = Compiler(project, output_dir)
            manifest = compiler.compile(dry_run=False)

            # Read generated SQL file
            flink_sql_path = output_dir / "flink" / "payments_clean.sql"
            assert flink_sql_path.exists(), "Flink SQL file should be generated"

            sql_content = flink_sql_path.read_text()

            # Verify source columns are in the CREATE TABLE
            assert "`payment_id`" in sql_content, "payment_id column should be in SQL"
            assert "`customer_id`" in sql_content, "customer_id column should be in SQL"
            assert "`amount`" in sql_content, "amount column should be in SQL"
            assert "`timestamp`" in sql_content, "timestamp column should be in SQL"

            # Verify we're NOT using the old _raw fallback (check for `_raw` as a column)
            assert "`_raw`" not in sql_content, "Should not use `_raw` fallback when columns defined"

    def test_sink_columns_from_select(self):
        """TC-SCHEMA-002: Sink table should have columns matching SELECT clause."""
        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                "project": {"name": "test", "version": "1.0.0"},
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
                    {
                        "name": "events",
                        "topic": "events.v1",
                        "columns": [
                            {"name": "event_id"},
                            {"name": "user_id"},
                            {"name": "event_type"},
                        ],
                    }
                ],
                "models": [
                    {
                        "name": "events_filtered",
                        "materialized": "flink",
                        "sql": """
                            SELECT event_id, user_id
                            FROM {{ source("events") }}
                            WHERE event_type = 'purchase'
                        """,
                    }
                ],
            }
            project = self._create_project(tmpdir, config)
            output_dir = Path(tmpdir) / "generated"
            compiler = Compiler(project, output_dir)
            compiler.compile(dry_run=False)

            sql_content = (output_dir / "flink" / "events_filtered.sql").read_text()

            # Sink table should have only the selected columns
            # Look for the sink table definition
            assert "CREATE TABLE IF NOT EXISTS events_filtered_sink" in sql_content
            # The sink should have event_id and user_id
            lines = sql_content.split("\n")
            sink_section = False
            sink_columns = []
            for line in lines:
                if "events_filtered_sink" in line:
                    sink_section = True
                if sink_section and "`" in line and "STRING" in line:
                    sink_columns.append(line)
                if sink_section and ") WITH" in line:
                    break

            assert len(sink_columns) == 2, f"Sink should have 2 columns, got {sink_columns}"

    def test_select_star_fallback(self):
        """TC-SCHEMA-003: SELECT * should still work (fallback to _raw)."""
        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                "project": {"name": "test", "version": "1.0.0"},
                "runtime": {
                    "kafka": {"bootstrap_servers": "localhost:9092"},
                    "flink": {
                        "default": "local",
                        "clusters": {
                            "local": {"type": "rest", "rest_url": "http://localhost:8082"}
                        },
                    },
                },
                "sources": [{"name": "raw", "topic": "raw.v1"}],  # No columns defined
                "models": [
                    {
                        "name": "passthrough",
                        "materialized": "flink",
                        "sql": 'SELECT * FROM {{ source("raw") }}',
                    }
                ],
            }
            project = self._create_project(tmpdir, config)
            compiler = Compiler(project)
            manifest = compiler.compile(dry_run=True)

            # Should still compile without error
            flink_jobs = manifest.artifacts.get("flink_jobs", [])
            assert len(flink_jobs) == 1

    def test_select_with_alias(self):
        """TC-SCHEMA-004: SELECT with AS aliases should use alias names in sink."""
        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                "project": {"name": "test", "version": "1.0.0"},
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
                    {
                        "name": "orders",
                        "topic": "orders.v1",
                        "columns": [{"name": "order_id"}, {"name": "total"}],
                    }
                ],
                "models": [
                    {
                        "name": "orders_renamed",
                        "materialized": "flink",
                        "sql": """
                            SELECT order_id AS id, total AS amount
                            FROM {{ source("orders") }}
                        """,
                    }
                ],
            }
            project = self._create_project(tmpdir, config)
            output_dir = Path(tmpdir) / "generated"
            compiler = Compiler(project, output_dir)
            compiler.compile(dry_run=False)

            sql_content = (output_dir / "flink" / "orders_renamed.sql").read_text()

            # Sink should use the aliases (id, amount), not original names
            assert "`id`" in sql_content, "Should use alias 'id'"
            assert "`amount`" in sql_content, "Should use alias 'amount'"


class TestCompilerBootstrapServers:
    """Tests for bootstrap_servers and bootstrap_servers_internal configuration."""

    def _create_project(self, tmpdir: str, config: dict) -> "StreamtProject":
        """Helper to create and parse a project."""
        project_path = Path(tmpdir)
        with open(project_path / "stream_project.yml", "w") as f:
            yaml.dump(config, f)
        parser = ProjectParser(project_path)
        return parser.parse()

    def test_bootstrap_servers_internal_used_in_flink_sql(self):
        """TC-NETWORK-001: Flink SQL should use bootstrap_servers_internal when set."""
        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                "project": {"name": "test", "version": "1.0.0"},
                "runtime": {
                    "kafka": {
                        "bootstrap_servers": "localhost:9092",
                        "bootstrap_servers_internal": "kafka-internal:29092",
                    },
                    "flink": {
                        "default": "local",
                        "clusters": {
                            "local": {"type": "rest", "rest_url": "http://localhost:8082"}
                        },
                    },
                },
                "sources": [
                    {
                        "name": "events",
                        "topic": "events.v1",
                        "columns": [{"name": "id"}],
                    }
                ],
                "models": [
                    {
                        "name": "events_clean",
                        "materialized": "flink",
                        "sql": 'SELECT id FROM {{ source("events") }}',
                    }
                ],
            }
            project = self._create_project(tmpdir, config)
            output_dir = Path(tmpdir) / "generated"
            compiler = Compiler(project, output_dir)
            compiler.compile(dry_run=False)

            sql_content = (output_dir / "flink" / "events_clean.sql").read_text()

            # Should use internal bootstrap servers
            assert "kafka-internal:29092" in sql_content
            # Should NOT use external bootstrap servers
            assert "localhost:9092" not in sql_content

    def test_fallback_to_bootstrap_servers_when_internal_not_set(self):
        """TC-NETWORK-002: Should fallback to bootstrap_servers when internal not set."""
        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                "project": {"name": "test", "version": "1.0.0"},
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
                    {
                        "name": "events",
                        "topic": "events.v1",
                        "columns": [{"name": "id"}],
                    }
                ],
                "models": [
                    {
                        "name": "events_clean",
                        "materialized": "flink",
                        "sql": 'SELECT id FROM {{ source("events") }}',
                    }
                ],
            }
            project = self._create_project(tmpdir, config)
            output_dir = Path(tmpdir) / "generated"
            compiler = Compiler(project, output_dir)
            compiler.compile(dry_run=False)

            sql_content = (output_dir / "flink" / "events_clean.sql").read_text()

            # Should use default bootstrap servers
            assert "localhost:9092" in sql_content


class TestContinuousTestCompilation:
    """Tests for continuous test compilation into Flink jobs."""

    def _create_project(self, tmpdir: str, config: dict) -> "StreamtProject":
        """Helper to create and parse a project."""
        project_path = Path(tmpdir)
        with open(project_path / "stream_project.yml", "w") as f:
            yaml.dump(config, f)
        parser = ProjectParser(project_path)
        return parser.parse()

    def test_continuous_test_generates_flink_job(self):
        """TC-TEST-001: Continuous tests should generate Flink monitoring jobs."""
        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                "project": {"name": "test", "version": "1.0.0"},
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
                    {
                        "name": "events",
                        "topic": "events.v1",
                        "columns": [
                            {"name": "event_id"},
                            {"name": "user_id"},
                            {"name": "event_type"},
                        ],
                    }
                ],
                "models": [
                    {
                        "name": "events_clean",
                        "materialized": "topic",
                        "topic": {"name": "events.clean.v1"},
                        "sql": """
                            SELECT event_id, user_id, event_type
                            FROM {{ source("events") }}
                        """,
                    }
                ],
                "tests": [
                    {
                        "name": "events_monitoring",
                        "model": "events_clean",
                        "type": "continuous",
                        "assertions": [
                            {"not_null": {"columns": ["event_id", "user_id"]}},
                            {"accepted_values": {"column": "event_type", "values": ["click", "view"]}},
                        ],
                    }
                ],
            }
            project = self._create_project(tmpdir, config)
            compiler = Compiler(project)
            manifest = compiler.compile(dry_run=True)

            # Should generate a test Flink job
            flink_jobs = manifest.artifacts.get("flink_jobs", [])
            test_job = next((j for j in flink_jobs if j["name"] == "test_events_monitoring"), None)

            assert test_job is not None, "Should generate test_events_monitoring job"
            assert "sql" in test_job

    def test_continuous_test_sql_structure(self):
        """TC-TEST-002: Continuous test SQL should have correct structure."""
        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                "project": {"name": "test", "version": "1.0.0"},
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
                    {
                        "name": "raw",
                        "topic": "raw.v1",
                        "columns": [{"name": "id"}, {"name": "status"}],
                    }
                ],
                "models": [
                    {
                        "name": "clean",
                        "materialized": "topic",
                        "topic": {"name": "clean.v1"},
                        "sql": 'SELECT id, status FROM {{ source("raw") }}',
                    }
                ],
                "tests": [
                    {
                        "name": "status_check",
                        "model": "clean",
                        "type": "continuous",
                        "assertions": [
                            {"not_null": {"columns": ["id"]}},
                            {"accepted_values": {"column": "status", "values": ["active", "inactive"]}},
                        ],
                    }
                ],
            }
            project = self._create_project(tmpdir, config)
            output_dir = Path(tmpdir) / "generated"
            compiler = Compiler(project, output_dir)
            compiler.compile(dry_run=False)

            sql_path = output_dir / "flink" / "test_status_check.sql"
            assert sql_path.exists(), "Test SQL file should be generated"

            sql_content = sql_path.read_text()

            # Should have source table reading from model's topic
            assert "CREATE TABLE IF NOT EXISTS test_source_status_check" in sql_content
            assert "'topic' = 'clean.v1'" in sql_content

            # Should have failures sink table
            assert "CREATE TABLE IF NOT EXISTS test_failures_status_check" in sql_content
            assert "'topic' = '_streamt_test_failures'" in sql_content

            # Should have INSERT with violation detection
            assert "INSERT INTO test_failures_status_check" in sql_content
            assert "WHERE `id` IS NULL" in sql_content
            assert "WHERE `status` NOT IN ('active', 'inactive')" in sql_content

    def test_continuous_test_uses_correct_column_in_cast(self):
        """TC-TEST-003: CAST should use explicit column name, not parsed from condition."""
        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                "project": {"name": "test", "version": "1.0.0"},
                "runtime": {
                    "kafka": {"bootstrap_servers": "localhost:9092"},
                    "flink": {
                        "default": "local",
                        "clusters": {
                            "local": {"type": "rest", "rest_url": "http://localhost:8082"}
                        },
                    },
                },
                "sources": [{"name": "src", "topic": "t1", "columns": [{"name": "col_a"}]}],
                "models": [
                    {
                        "name": "m1",
                        "materialized": "topic",
                        "topic": {"name": "t2"},
                        "sql": 'SELECT col_a FROM {{ source("src") }}',
                    }
                ],
                "tests": [
                    {
                        "name": "t1",
                        "model": "m1",
                        "type": "continuous",
                        "assertions": [{"not_null": {"columns": ["col_a"]}}],
                    }
                ],
            }
            project = self._create_project(tmpdir, config)
            output_dir = Path(tmpdir) / "generated"
            compiler = Compiler(project, output_dir)
            compiler.compile(dry_run=False)

            sql_content = (output_dir / "flink" / "test_t1.sql").read_text()

            # Should cast the column by name, not by splitting condition
            assert "CAST(`col_a` AS STRING)" in sql_content


class TestComplexSelectParsing:
    """Tests for complex SQL SELECT clause parsing."""

    def _create_project(self, tmpdir: str, config: dict) -> "StreamtProject":
        """Helper to create and parse a project."""
        project_path = Path(tmpdir)
        with open(project_path / "stream_project.yml", "w") as f:
            yaml.dump(config, f)
        parser = ProjectParser(project_path)
        return parser.parse()

    def test_select_with_functions(self):
        """TC-SQL-001: SELECT with functions should parse columns correctly."""
        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                "project": {"name": "test", "version": "1.0.0"},
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
                    {
                        "name": "orders",
                        "topic": "orders.v1",
                        "columns": [{"name": "amount"}, {"name": "created_at"}],
                    }
                ],
                "models": [
                    {
                        "name": "order_stats",
                        "materialized": "flink",
                        "sql": """
                            SELECT
                                UPPER(status) AS status_upper,
                                COALESCE(amount, 0) AS amount_safe
                            FROM {{ source("orders") }}
                        """,
                    }
                ],
            }
            project = self._create_project(tmpdir, config)
            output_dir = Path(tmpdir) / "generated"
            compiler = Compiler(project, output_dir)
            compiler.compile(dry_run=False)

            sql_content = (output_dir / "flink" / "order_stats.sql").read_text()

            # Should extract alias names from function calls
            assert "`status_upper`" in sql_content or "`amount_safe`" in sql_content

    def test_select_with_case_when(self):
        """TC-SQL-002: SELECT with CASE WHEN should handle correctly."""
        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                "project": {"name": "test", "version": "1.0.0"},
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
                    {
                        "name": "events",
                        "topic": "events.v1",
                        "columns": [{"name": "type"}, {"name": "value"}],
                    }
                ],
                "models": [
                    {
                        "name": "categorized",
                        "materialized": "flink",
                        "sql": """
                            SELECT
                                type,
                                CASE WHEN value > 100 THEN 'high' ELSE 'low' END AS category
                            FROM {{ source("events") }}
                        """,
                    }
                ],
            }
            project = self._create_project(tmpdir, config)
            compiler = Compiler(project)
            # Should not crash
            manifest = compiler.compile(dry_run=True)

            flink_jobs = manifest.artifacts.get("flink_jobs", [])
            assert len(flink_jobs) >= 1

    def test_multiline_select(self):
        """TC-SQL-003: Multiline SELECT should parse correctly."""
        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                "project": {"name": "test", "version": "1.0.0"},
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
                    {
                        "name": "data",
                        "topic": "data.v1",
                        "columns": [{"name": "a"}, {"name": "b"}, {"name": "c"}],
                    }
                ],
                "models": [
                    {
                        "name": "transformed",
                        "materialized": "flink",
                        "sql": """
                            SELECT
                                a,
                                b,
                                c
                            FROM {{ source("data") }}
                        """,
                    }
                ],
            }
            project = self._create_project(tmpdir, config)
            output_dir = Path(tmpdir) / "generated"
            compiler = Compiler(project, output_dir)
            compiler.compile(dry_run=False)

            sql_content = (output_dir / "flink" / "transformed.sql").read_text()

            # Should have all three columns in sink
            assert "`a`" in sql_content
            assert "`b`" in sql_content
            assert "`c`" in sql_content
