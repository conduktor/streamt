"""State TTL configuration scenario tests.

Tests covering State TTL configuration for Flink jobs:
- Valid TTL configurations
- TTL duration format conversion
- TTL validation errors
- TTL recommendations for stateful operations
"""

import tempfile
from pathlib import Path

import yaml

from streamt.compiler import Compiler
from streamt.core.parser import ProjectParser
from streamt.core.validator import ProjectValidator


class TestStateTtlConfiguration:
    """Tests for State TTL configuration in models."""

    def _create_project(self, tmpdir: str, config: dict) -> Path:
        """Helper to create a project."""
        project_path = Path(tmpdir)
        with open(project_path / "stream_project.yml", "w") as f:
            yaml.dump(config, f)
        return project_path

    def _create_base_config(self, models: list) -> dict:
        """Create a base config with the given models."""
        return {
            "project": {"name": "state-ttl-test", "version": "1.0.0"},
            "runtime": {
                "kafka": {"bootstrap_servers": "localhost:9092"},
                "flink": {
                    "default": "local",
                    "clusters": {
                        "local": {
                            "type": "rest",
                            "rest_url": "http://localhost:8082",
                            "sql_gateway_url": "http://localhost:8083",
                        }
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
                        {"name": "event_time", "type": "TIMESTAMP(3)"},
                    ],
                    "event_time": {
                        "column": "event_time",
                        "watermark": {"max_out_of_orderness_ms": 5000},
                    },
                }
            ],
            "models": models,
        }

    def test_state_ttl_24_hours(self):
        """
        SCENARIO: Configure 24-hour State TTL

        Standard configuration for daily aggregations.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            config = self._create_base_config([
                {
                    "name": "daily_user_counts",
                    "advanced": {
                        "flink": {
                            "state_ttl_ms": 86400000,  # 24 hours
                        },
                    },
                    "sql": """
                        SELECT user_id, COUNT(*) as event_count
                        FROM {{ source("events") }}
                        GROUP BY user_id
                    """,
                }
            ])

            project_path = self._create_project(tmpdir, config)
            parser = ProjectParser(project_path)
            project = parser.parse()

            validator = ProjectValidator(project)
            result = validator.validate()
            assert result.is_valid

            compiler = Compiler(project)
            manifest = compiler.compile(dry_run=True)

            job = manifest.artifacts["flink_jobs"][0]
            assert job["state_ttl_ms"] == 86400000

            # Verify SET statement in SQL
            assert "SET 'table.exec.state.ttl'" in job["sql"]
            assert "24 h" in job["sql"]

    def test_state_ttl_1_hour(self):
        """
        SCENARIO: Configure 1-hour State TTL

        Short TTL for real-time joins with limited lookback.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            config = self._create_base_config([
                {
                    "name": "recent_events",
                    "advanced": {
                        "flink": {
                            "state_ttl_ms": 3600000,  # 1 hour
                        },
                    },
                    "sql": """
                        SELECT user_id, COUNT(*) as event_count
                        FROM {{ source("events") }}
                        GROUP BY user_id
                    """,
                }
            ])

            project_path = self._create_project(tmpdir, config)
            parser = ProjectParser(project_path)
            project = parser.parse()

            validator = ProjectValidator(project)
            result = validator.validate()
            assert result.is_valid

            compiler = Compiler(project)
            manifest = compiler.compile(dry_run=True)

            job = manifest.artifacts["flink_jobs"][0]
            assert job["state_ttl_ms"] == 3600000
            assert "1 h" in job["sql"]

    def test_state_ttl_30_minutes(self):
        """
        SCENARIO: Configure 30-minute State TTL

        Short TTL for fast-expiring state.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            config = self._create_base_config([
                {
                    "name": "windowed_counts",
                    "advanced": {
                        "flink": {
                            "state_ttl_ms": 1800000,  # 30 minutes
                        },
                    },
                    "sql": """
                        SELECT user_id, COUNT(*) as event_count
                        FROM {{ source("events") }}
                        GROUP BY user_id
                    """,
                }
            ])

            project_path = self._create_project(tmpdir, config)
            parser = ProjectParser(project_path)
            project = parser.parse()

            validator = ProjectValidator(project)
            result = validator.validate()
            assert result.is_valid

            compiler = Compiler(project)
            manifest = compiler.compile(dry_run=True)

            job = manifest.artifacts["flink_jobs"][0]
            assert "30 min" in job["sql"]

    def test_state_ttl_7_days(self):
        """
        SCENARIO: Configure 7-day State TTL

        Longer TTL for weekly analytics patterns.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            config = self._create_base_config([
                {
                    "name": "weekly_patterns",
                    "advanced": {
                        "flink": {
                            "state_ttl_ms": 604800000,  # 7 days
                        },
                    },
                    "sql": """
                        SELECT user_id, COUNT(*) as event_count
                        FROM {{ source("events") }}
                        GROUP BY user_id
                    """,
                }
            ])

            project_path = self._create_project(tmpdir, config)
            parser = ProjectParser(project_path)
            project = parser.parse()

            validator = ProjectValidator(project)
            result = validator.validate()
            assert result.is_valid

            compiler = Compiler(project)
            manifest = compiler.compile(dry_run=True)

            job = manifest.artifacts["flink_jobs"][0]
            assert job["state_ttl_ms"] == 604800000
            # 604800000ms = 168 hours = 168h
            assert "168 h" in job["sql"]

    def test_state_ttl_with_seconds(self):
        """
        SCENARIO: Configure State TTL with seconds precision

        TTL that doesn't align to minutes.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            config = self._create_base_config([
                {
                    "name": "short_lived",
                    "advanced": {
                        "flink": {
                            "state_ttl_ms": 90000,  # 90 seconds
                        },
                    },
                    "sql": """
                        SELECT user_id, COUNT(*) as event_count
                        FROM {{ source("events") }}
                        GROUP BY user_id
                    """,
                }
            ])

            project_path = self._create_project(tmpdir, config)
            parser = ProjectParser(project_path)
            project = parser.parse()

            validator = ProjectValidator(project)
            result = validator.validate()
            assert result.is_valid

            compiler = Compiler(project)
            manifest = compiler.compile(dry_run=True)

            job = manifest.artifacts["flink_jobs"][0]
            assert "90 s" in job["sql"]

    def test_state_ttl_with_milliseconds(self):
        """
        SCENARIO: Configure State TTL with milliseconds precision

        TTL that doesn't align to seconds.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            config = self._create_base_config([
                {
                    "name": "precise_ttl",
                    "advanced": {
                        "flink": {
                            "state_ttl_ms": 5500,  # 5.5 seconds
                        },
                    },
                    "sql": """
                        SELECT user_id, COUNT(*) as event_count
                        FROM {{ source("events") }}
                        GROUP BY user_id
                    """,
                }
            ])

            project_path = self._create_project(tmpdir, config)
            parser = ProjectParser(project_path)
            project = parser.parse()

            validator = ProjectValidator(project)
            result = validator.validate()
            assert result.is_valid

            compiler = Compiler(project)
            manifest = compiler.compile(dry_run=True)

            job = manifest.artifacts["flink_jobs"][0]
            assert "5500 ms" in job["sql"]

    def test_state_ttl_combined_with_parallelism(self):
        """
        SCENARIO: Configure State TTL with parallelism

        Both state TTL and parallelism should be set correctly.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            config = self._create_base_config([
                {
                    "name": "aggregated",
                    "advanced": {
                        "flink": {
                            "parallelism": 8,
                            "state_ttl_ms": 86400000,
                            "checkpoint_interval_ms": 60000,
                        },
                    },
                    "sql": """
                        SELECT user_id, COUNT(*) as event_count
                        FROM {{ source("events") }}
                        GROUP BY user_id
                    """,
                }
            ])

            project_path = self._create_project(tmpdir, config)
            parser = ProjectParser(project_path)
            project = parser.parse()

            validator = ProjectValidator(project)
            result = validator.validate()
            assert result.is_valid

            compiler = Compiler(project)
            manifest = compiler.compile(dry_run=True)

            job = manifest.artifacts["flink_jobs"][0]
            assert job["parallelism"] == 8
            assert job["state_ttl_ms"] == 86400000
            assert job["checkpoint_interval_ms"] == 60000

            # Verify all SET statements
            assert "SET 'parallelism.default' = '8'" in job["sql"]
            assert "SET 'table.exec.state.ttl' = '24 h'" in job["sql"]
            assert "SET 'execution.checkpointing.interval' = '60000ms'" in job["sql"]

class TestStateTtlValidation:
    """Tests for State TTL validation."""

    def _create_project(self, tmpdir: str, config: dict) -> Path:
        project_path = Path(tmpdir)
        with open(project_path / "stream_project.yml", "w") as f:
            yaml.dump(config, f)
        return project_path

    def _create_base_config(self, models: list) -> dict:
        return {
            "project": {"name": "ttl-validation", "version": "1.0.0"},
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
            "models": models,
        }

    def test_negative_state_ttl_is_invalid(self):
        """
        SCENARIO: Negative State TTL should fail validation

        Negative values make no sense for TTL.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            config = self._create_base_config([
                {
                    "name": "invalid_ttl",
                    "advanced": {
                        "flink": {
                            "state_ttl_ms": -1000,
                        },
                    },
                    "sql": "SELECT * FROM {{ source(\"events\") }}",
                }
            ])

            project_path = self._create_project(tmpdir, config)
            parser = ProjectParser(project_path)
            project = parser.parse()

            validator = ProjectValidator(project)
            result = validator.validate()

            assert not result.is_valid
            assert any("INVALID_STATE_TTL" in e.code for e in result.errors)
            assert any("positive number" in e.message.lower() for e in result.errors)

    def test_zero_state_ttl_is_invalid(self):
        """
        SCENARIO: Zero State TTL should fail validation

        Zero TTL would expire state immediately.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            config = self._create_base_config([
                {
                    "name": "zero_ttl",
                    "advanced": {
                        "flink": {
                            "state_ttl_ms": 0,
                        },
                    },
                    "sql": "SELECT * FROM {{ source(\"events\") }}",
                }
            ])

            project_path = self._create_project(tmpdir, config)
            parser = ProjectParser(project_path)
            project = parser.parse()

            validator = ProjectValidator(project)
            result = validator.validate()

            assert not result.is_valid
            assert any("INVALID_STATE_TTL" in e.code for e in result.errors)

    def test_very_short_state_ttl_warns(self):
        """
        SCENARIO: Very short State TTL should produce a warning

        TTL under 1 second is suspiciously short.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            config = self._create_base_config([
                {
                    "name": "short_ttl",
                    "advanced": {
                        "flink": {
                            "state_ttl_ms": 500,  # 500ms - very short
                        },
                    },
                    "sql": "SELECT * FROM {{ source(\"events\") }}",
                }
            ])

            project_path = self._create_project(tmpdir, config)
            parser = ProjectParser(project_path)
            project = parser.parse()

            validator = ProjectValidator(project)
            result = validator.validate()

            # Should be valid but with warning
            assert result.is_valid
            assert any("STATE_TTL_TOO_SHORT" in w.code for w in result.warnings)

    def test_no_state_ttl_is_valid(self):
        """
        SCENARIO: No State TTL configuration is valid

        State TTL is optional.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            config = self._create_base_config([
                {
                    "name": "no_ttl",

                    "sql": "SELECT * FROM {{ source(\"events\") }}",
                }
            ])

            project_path = self._create_project(tmpdir, config)
            parser = ProjectParser(project_path)
            project = parser.parse()

            validator = ProjectValidator(project)
            result = validator.validate()

            assert result.is_valid

            compiler = Compiler(project)
            manifest = compiler.compile(dry_run=True)

            job = manifest.artifacts["flink_jobs"][0]
            assert job["state_ttl_ms"] is None
            assert "table.exec.state.ttl" not in job["sql"]

class TestStateTtlMultipleModels:
    """Tests for State TTL with multiple models."""

    def _create_project(self, tmpdir: str, config: dict) -> Path:
        project_path = Path(tmpdir)
        with open(project_path / "stream_project.yml", "w") as f:
            yaml.dump(config, f)
        return project_path

    def test_different_ttl_per_model(self):
        """
        SCENARIO: Different State TTL per model

        Each model can have its own TTL configuration.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                "project": {"name": "multi-model-ttl", "version": "1.0.0"},
                "runtime": {
                    "kafka": {"bootstrap_servers": "localhost:9092"},
                    "flink": {
                        "default": "local",
                        "clusters": {
                            "local": {
                                "type": "rest",
                                "rest_url": "http://localhost:8082",
                                "sql_gateway_url": "http://localhost:8083",
                            }
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
                            {"name": "event_time", "type": "TIMESTAMP(3)"},
                        ],
                        "event_time": {
                            "column": "event_time",
                            "watermark": {"max_out_of_orderness_ms": 5000},
                        },
                    }
                ],
                "models": [
                    {
                        "name": "hourly_metrics",

                        "advanced": {
                            "flink": {
                                "state_ttl_ms": 7200000,  # 2 hours for hourly
                            },
                        },
                        "sql": """
                            SELECT
                                TUMBLE_START(event_time, INTERVAL '1' HOUR) as hour_start,
                                COUNT(*) as event_count
                            FROM {{ source("events") }}
                            GROUP BY TUMBLE(event_time, INTERVAL '1' HOUR)
                        """,
                    },
                    {
                        "name": "daily_metrics",

                        "advanced": {
                            "flink": {
                                "state_ttl_ms": 172800000,  # 48 hours for daily
                            },
                        },
                        "sql": """
                            SELECT
                                TUMBLE_START(event_time, INTERVAL '1' DAY) as day_start,
                                COUNT(*) as event_count
                            FROM {{ source("events") }}
                            GROUP BY TUMBLE(event_time, INTERVAL '1' DAY)
                        """,
                    },
                    {
                        "name": "user_sessions",

                        "advanced": {
                            "flink": {
                                "state_ttl_ms": 3600000,  # 1 hour for session window
                            },
                        },
                        "sql": """
                            SELECT
                                user_id,
                                SESSION_START(event_time, INTERVAL '30' MINUTE) as session_start,
                                COUNT(*) as event_count
                            FROM {{ source("events") }}
                            GROUP BY user_id, SESSION(event_time, INTERVAL '30' MINUTE)
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
            manifest = compiler.compile(dry_run=True)

            flink_jobs = manifest.artifacts["flink_jobs"]
            assert len(flink_jobs) == 3

            # Find each job and verify its TTL
            hourly = next(j for j in flink_jobs if j["name"] == "hourly_metrics")
            daily = next(j for j in flink_jobs if j["name"] == "daily_metrics")
            sessions = next(j for j in flink_jobs if j["name"] == "user_sessions")

            assert hourly["state_ttl_ms"] == 7200000
            assert "2 h" in hourly["sql"]

            assert daily["state_ttl_ms"] == 172800000
            assert "48 h" in daily["sql"]

            assert sessions["state_ttl_ms"] == 3600000
            assert "1 h" in sessions["sql"]

    def test_mixed_ttl_and_no_ttl(self):
        """
        SCENARIO: Some models have TTL, others don't

        Models can choose to not configure TTL.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                "project": {"name": "mixed-ttl", "version": "1.0.0"},
                "runtime": {
                    "kafka": {"bootstrap_servers": "localhost:9092"},
                    "flink": {
                        "default": "local",
                        "clusters": {
                            "local": {
                                "type": "rest",
                                "rest_url": "http://localhost:8082",
                            }
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
                        "name": "with_ttl",
                        "sql": "SELECT id, COUNT(*) as count FROM {{ source(\"events\") }} GROUP BY id",
                        "advanced": {"flink": {"state_ttl_ms": 86400000}},
                    },
                    {
                        "name": "without_ttl",
                        "sql": "SELECT id, COUNT(*) as count FROM {{ source(\"events\") }} GROUP BY id",
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
            manifest = compiler.compile(dry_run=True)

            flink_jobs = manifest.artifacts["flink_jobs"]

            with_ttl = next(j for j in flink_jobs if j["name"] == "with_ttl")
            without_ttl = next(j for j in flink_jobs if j["name"] == "without_ttl")

            assert with_ttl["state_ttl_ms"] == 86400000
            assert "table.exec.state.ttl" in with_ttl["sql"]

            assert without_ttl["state_ttl_ms"] is None
            assert "table.exec.state.ttl" not in without_ttl["sql"]

class TestStateTtlRealWorldScenarios:
    """Tests for real-world State TTL scenarios."""

    def _create_project(self, tmpdir: str, config: dict) -> Path:
        project_path = Path(tmpdir)
        with open(project_path / "stream_project.yml", "w") as f:
            yaml.dump(config, f)
        return project_path

    def test_ecommerce_order_aggregation(self):
        """
        SCENARIO: E-commerce order aggregation with TTL

        Real-time order metrics with appropriate TTL for
        order lifecycle (24 hours).
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                "project": {"name": "ecommerce-ttl", "version": "1.0.0"},
                "runtime": {
                    "kafka": {"bootstrap_servers": "localhost:9092"},
                    "flink": {
                        "default": "prod",
                        "clusters": {
                            "prod": {
                                "type": "rest",
                                "rest_url": "http://flink:8082",
                                "sql_gateway_url": "http://flink:8083",
                            }
                        },
                    },
                },
                "sources": [
                    {
                        "name": "orders",
                        "topic": "orders.v1",
                        "columns": [
                            {"name": "order_id"},
                            {"name": "customer_id"},
                            {"name": "amount", "type": "DOUBLE"},
                            {"name": "order_time", "type": "TIMESTAMP(3)"},
                        ],
                        "event_time": {
                            "column": "order_time",
                            "watermark": {"max_out_of_orderness_ms": 30000},
                        },
                    },
                    {
                        "name": "customers",
                        "topic": "customers.v1",
                        "columns": [
                            {"name": "customer_id"},
                            {"name": "segment"},
                        ],
                    },
                ],
                "models": [
                    {
                        "name": "customer_order_counts",
                        "description": "Order counts per customer for join enrichment",
                        "advanced": {
                            "flink": {
                                "parallelism": 4,
                                "state_ttl_ms": 86400000,  # 24 hours
                            },
                        },
                        "sql": """
                            SELECT
                                customer_id,
                                COUNT(*) as order_count,
                                SUM(amount) as total_spent,
                                MAX(order_time) as last_order_time
                            FROM {{ source("orders") }}
                            GROUP BY customer_id
                        """,
                    },
                    {
                        "name": "customer_order_enriched",
                        "description": "Orders enriched with customer lifetime data",
                        "advanced": {
                            "flink": {
                                "parallelism": 4,
                                "state_ttl_ms": 3600000,  # 1 hour for join
                            },
                        },
                        "sql": """
                            SELECT
                                o.order_id,
                                o.customer_id,
                                c.segment,
                                co.order_count,
                                co.total_spent,
                                o.amount,
                                o.order_time
                            FROM {{ source("orders") }} o
                            LEFT JOIN {{ source("customers") }} c
                                ON o.customer_id = c.customer_id
                            LEFT JOIN {{ ref("customer_order_counts") }} co
                                ON o.customer_id = co.customer_id
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
            manifest = compiler.compile(dry_run=True)

            flink_jobs = manifest.artifacts["flink_jobs"]
            assert len(flink_jobs) == 2

            counts_job = next(j for j in flink_jobs if j["name"] == "customer_order_counts")
            enriched_job = next(j for j in flink_jobs if j["name"] == "customer_order_enriched")

            # Aggregation job has longer TTL
            assert counts_job["state_ttl_ms"] == 86400000  # 24h
            # Join job has shorter TTL
            assert enriched_job["state_ttl_ms"] == 3600000  # 1h

    def test_iot_sensor_aggregation(self):
        """
        SCENARIO: IoT sensor data aggregation with TTL

        Sensor readings aggregated with 1-week TTL for
        trend analysis.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                "project": {"name": "iot-sensors", "version": "1.0.0"},
                "runtime": {
                    "kafka": {"bootstrap_servers": "localhost:9092"},
                    "flink": {
                        "default": "local",
                        "clusters": {
                            "local": {
                                "type": "rest",
                                "rest_url": "http://localhost:8082",
                                "sql_gateway_url": "http://localhost:8083",
                            }
                        },
                    },
                },
                "sources": [
                    {
                        "name": "sensor_readings",
                        "topic": "iot.sensors.v1",
                        "columns": [
                            {"name": "sensor_id"},
                            {"name": "device_id"},
                            {"name": "temperature", "type": "DOUBLE"},
                            {"name": "humidity", "type": "DOUBLE"},
                            {"name": "reading_time", "type": "TIMESTAMP(3)"},
                        ],
                        "event_time": {
                            "column": "reading_time",
                            "watermark": {"max_out_of_orderness_ms": 60000},
                        },
                    }
                ],
                "models": [
                    {
                        "name": "sensor_hourly_stats",
                        "description": "Hourly sensor statistics",
                        "advanced": {
                            "flink": {
                                "parallelism": 2,
                                "state_ttl_ms": 172800000,  # 48 hours
                            },
                        },
                        "sql": """
                            SELECT
                                sensor_id,
                                device_id,
                                TUMBLE_START(reading_time, INTERVAL '1' HOUR) as hour_start,
                                AVG(temperature) as avg_temp,
                                MIN(temperature) as min_temp,
                                MAX(temperature) as max_temp,
                                AVG(humidity) as avg_humidity
                            FROM {{ source("sensor_readings") }}
                            GROUP BY
                                sensor_id,
                                device_id,
                                TUMBLE(reading_time, INTERVAL '1' HOUR)
                        """,
                    },
                    {
                        "name": "device_health",
                        "description": "Device health metrics",
                        "advanced": {
                            "flink": {
                                "parallelism": 2,
                                "state_ttl_ms": 604800000,  # 7 days
                            },
                        },
                        "sql": """
                            SELECT
                                device_id,
                                COUNT(DISTINCT sensor_id) as active_sensors,
                                MAX(reading_time) as last_reading,
                                COUNT(*) as total_readings
                            FROM {{ source("sensor_readings") }}
                            GROUP BY device_id
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
            manifest = compiler.compile(dry_run=True)

            flink_jobs = manifest.artifacts["flink_jobs"]

            hourly_job = next(j for j in flink_jobs if j["name"] == "sensor_hourly_stats")
            health_job = next(j for j in flink_jobs if j["name"] == "device_health")

            assert hourly_job["state_ttl_ms"] == 172800000
            assert "48 h" in hourly_job["sql"]

            assert health_job["state_ttl_ms"] == 604800000
            assert "168 h" in health_job["sql"]  # 7 days = 168 hours
