"""Tutorial scenario tests.

Tests covering the scenarios from the tutorials:
- Windowed Aggregations (tumbling, hopping windows)
- Stream Joins (temporal joins, interval joins)
- CDC Pipelines (Debezium, customer 360)

These tests validate that the patterns documented in the tutorials
compile correctly and produce valid Flink SQL.
"""

import tempfile
from pathlib import Path

import yaml

from streamt.compiler import Compiler
from streamt.core.dag import DAGBuilder
from streamt.core.parser import ProjectParser
from streamt.core.validator import ProjectValidator


class TestWindowedAggregationScenarios:
    """Tests for windowed aggregation patterns from the tutorial."""

    def _create_project(self, tmpdir: str, config: dict) -> Path:
        """Helper to create a project."""
        project_path = Path(tmpdir)
        with open(project_path / "stream_project.yml", "w") as f:
            yaml.dump(config, f)
        return project_path

    def test_tumbling_window_5_minute_aggregation(self):
        """
        SCENARIO: 5-minute tumbling window for order metrics

        From tutorial: Basic tumbling window for calculating order counts
        and revenue in non-overlapping 5-minute windows.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                "project": {
                    "name": "windowed-aggregations-tutorial",
                    "version": "1.0.0",
                    "description": "Tutorial: Windowed aggregations",
                },
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
                        "name": "orders_raw",
                        "topic": "orders.raw.v1",
                        "columns": [
                            {"name": "order_id"},
                            {"name": "customer_id"},
                            {"name": "amount", "type": "DOUBLE"},
                            {"name": "order_time", "type": "TIMESTAMP(3)"},
                        ],
                        "event_time": {
                            "column": "order_time",
                            "watermark": {
                                "strategy": "bounded_out_of_orderness",
                                "max_out_of_orderness_ms": 5000,
                            },
                        },
                    }
                ],
                "models": [
                    {
                        "name": "order_metrics_5min",
                        "description": "Order metrics in 5-minute windows",

                        "advanced": {
                            "flink": {
                                "parallelism": 4,
                                "state_ttl_ms": 3600000,  # 1 hour
                            },
                        },
                        "sql": """
                            SELECT
                                TUMBLE_START(order_time, INTERVAL '5' MINUTE) as window_start,
                                TUMBLE_END(order_time, INTERVAL '5' MINUTE) as window_end,
                                COUNT(*) as order_count,
                                SUM(amount) as total_revenue,
                                AVG(amount) as avg_order_value
                            FROM {{ source("orders_raw") }}
                            GROUP BY TUMBLE(order_time, INTERVAL '5' MINUTE)
                        """,
                    }
                ],
            }

            project_path = self._create_project(tmpdir, config)
            parser = ProjectParser(project_path)
            project = parser.parse()

            # Validate
            validator = ProjectValidator(project)
            result = validator.validate()
            assert result.is_valid, f"Validation errors: {[e.message for e in result.errors]}"

            # Compile
            compiler = Compiler(project)
            manifest = compiler.compile(dry_run=True)

            # Verify Flink job artifact
            flink_jobs = manifest.artifacts["flink_jobs"]
            assert len(flink_jobs) == 1

            job = flink_jobs[0]
            assert job["name"] == "order_metrics_5min"
            assert job["parallelism"] == 4
            assert job["state_ttl_ms"] == 3600000

            # Verify SQL contains windowing
            assert "TUMBLE" in job["sql"]
            assert "TUMBLE_START" in job["sql"]
            assert "TUMBLE_END" in job["sql"]

    def test_hopping_window_sliding_metrics(self):
        """
        SCENARIO: Hopping window for sliding metrics

        From tutorial: 1-minute slide over 5-minute windows for smoother
        real-time metrics.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                "project": {"name": "hopping-windows", "version": "1.0.0"},
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
                        "name": "page_views",
                        "topic": "analytics.pageviews.v1",
                        "columns": [
                            {"name": "view_id"},
                            {"name": "page_url"},
                            {"name": "user_id"},
                            {"name": "view_time", "type": "TIMESTAMP(3)"},
                        ],
                        "event_time": {
                            "column": "view_time",
                            "watermark": {
                                "strategy": "bounded_out_of_orderness",
                                "max_out_of_orderness_ms": 10000,
                            },
                        },
                    }
                ],
                "models": [
                    {
                        "name": "page_metrics_sliding",
                        "description": "Page metrics with sliding 5-minute window",

                        "flink": {"parallelism": 2},
                        "sql": """
                            SELECT
                                HOP_START(view_time, INTERVAL '1' MINUTE, INTERVAL '5' MINUTE) as window_start,
                                HOP_END(view_time, INTERVAL '1' MINUTE, INTERVAL '5' MINUTE) as window_end,
                                page_url,
                                COUNT(*) as view_count,
                                COUNT(DISTINCT user_id) as unique_visitors
                            FROM {{ source("page_views") }}
                            GROUP BY
                                HOP(view_time, INTERVAL '1' MINUTE, INTERVAL '5' MINUTE),
                                page_url
                        """,
                    }
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

            job = manifest.artifacts["flink_jobs"][0]
            assert "HOP(" in job["sql"]
            assert "HOP_START" in job["sql"]
            assert "HOP_END" in job["sql"]

    def test_session_window_user_sessions(self):
        """
        SCENARIO: Session windows for user activity

        From tutorial: Gap-based session windows that group user
        activity with 30-minute inactivity timeout.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                "project": {"name": "session-windows", "version": "1.0.0"},
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
                        "name": "user_events",
                        "topic": "user.events.v1",
                        "columns": [
                            {"name": "event_id"},
                            {"name": "user_id"},
                            {"name": "event_type"},
                            {"name": "event_time", "type": "TIMESTAMP(3)"},
                        ],
                        "event_time": {
                            "column": "event_time",
                            "watermark": {"max_out_of_orderness_ms": 60000},
                        },
                    }
                ],
                "models": [
                    {
                        "name": "user_sessions",
                        "description": "User sessions with 30-minute gap",

                        "flink": {"state_ttl_ms": 7200000},  # 2 hours
                        "sql": """
                            SELECT
                                user_id,
                                SESSION_START(event_time, INTERVAL '30' MINUTE) as session_start,
                                SESSION_END(event_time, INTERVAL '30' MINUTE) as session_end,
                                COUNT(*) as event_count,
                                COLLECT(DISTINCT event_type) as event_types
                            FROM {{ source("user_events") }}
                            GROUP BY
                                user_id,
                                SESSION(event_time, INTERVAL '30' MINUTE)
                        """,
                    }
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

            job = manifest.artifacts["flink_jobs"][0]
            assert "SESSION(" in job["sql"]
            assert "SESSION_START" in job["sql"]
            assert job["state_ttl_ms"] == 7200000

class TestStreamJoinScenarios:
    """Tests for stream join patterns from the tutorial."""

    def _create_project(self, tmpdir: str, config: dict) -> Path:
        project_path = Path(tmpdir)
        with open(project_path / "stream_project.yml", "w") as f:
            yaml.dump(config, f)
        return project_path

    def test_temporal_join_order_enrichment(self):
        """
        SCENARIO: Temporal join for order enrichment

        From tutorial: Join orders with customer data as of order time
        to get point-in-time customer information.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                "project": {
                    "name": "stream-joins-tutorial",
                    "version": "1.0.0",
                    "description": "Tutorial: Stream joins",
                },
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
                            "watermark": {"max_out_of_orderness_ms": 5000},
                        },
                    },
                    {
                        "name": "customers",
                        "topic": "customers.v1",
                        "columns": [
                            {"name": "customer_id"},
                            {"name": "customer_name"},
                            {"name": "segment"},
                            {"name": "update_time", "type": "TIMESTAMP(3)"},
                        ],
                        "event_time": {
                            "column": "update_time",
                            "watermark": {"max_out_of_orderness_ms": 5000},
                        },
                    },
                ],
                "models": [
                    {
                        "name": "orders_enriched",
                        "description": "Orders enriched with customer data",

                        "flink": {"state_ttl_ms": 86400000},  # 24 hours
                        "sql": """
                            SELECT
                                o.order_id,
                                o.customer_id,
                                c.customer_name,
                                c.segment,
                                o.amount,
                                o.order_time
                            FROM {{ source("orders") }} o
                            JOIN {{ source("customers") }} FOR SYSTEM_TIME AS OF o.order_time AS c
                                ON o.customer_id = c.customer_id
                        """,
                    }
                ],
            }

            project_path = self._create_project(tmpdir, config)
            parser = ProjectParser(project_path)
            project = parser.parse()

            validator = ProjectValidator(project)
            result = validator.validate()
            assert result.is_valid

            # Verify DAG has both sources as upstream
            dag_builder = DAGBuilder(project)
            dag = dag_builder.build()
            upstream = dag.get_upstream("orders_enriched")
            assert "orders" in upstream
            assert "customers" in upstream

            compiler = Compiler(project)
            manifest = compiler.compile(dry_run=True)

            job = manifest.artifacts["flink_jobs"][0]
            assert "FOR SYSTEM_TIME AS OF" in job["sql"]
            assert job["state_ttl_ms"] == 86400000

    def test_interval_join_order_payment_matching(self):
        """
        SCENARIO: Interval join for order-payment matching

        From tutorial: Match orders with payments that occur within
        1 hour of the order.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                "project": {"name": "interval-join", "version": "1.0.0"},
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
                            "watermark": {"max_out_of_orderness_ms": 5000},
                        },
                    },
                    {
                        "name": "payments",
                        "topic": "payments.v1",
                        "columns": [
                            {"name": "payment_id"},
                            {"name": "order_id"},
                            {"name": "payment_amount", "type": "DOUBLE"},
                            {"name": "payment_time", "type": "TIMESTAMP(3)"},
                        ],
                        "event_time": {
                            "column": "payment_time",
                            "watermark": {"max_out_of_orderness_ms": 5000},
                        },
                    },
                ],
                "models": [
                    {
                        "name": "order_payment_matched",
                        "description": "Orders matched with payments within 1 hour",

                        "flink": {"state_ttl_ms": 7200000},  # 2 hours
                        "sql": """
                            SELECT
                                o.order_id,
                                o.customer_id,
                                o.amount as order_amount,
                                p.payment_id,
                                p.payment_amount,
                                o.order_time,
                                p.payment_time
                            FROM {{ source("orders") }} o
                            JOIN {{ source("payments") }} p
                                ON o.order_id = p.order_id
                                AND p.payment_time BETWEEN o.order_time
                                    AND o.order_time + INTERVAL '1' HOUR
                        """,
                    }
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

            job = manifest.artifacts["flink_jobs"][0]
            assert "BETWEEN" in job["sql"]
            assert "INTERVAL '1' HOUR" in job["sql"]

    def test_multi_way_join_order_360(self):
        """
        SCENARIO: Multi-way join for complete order view

        From tutorial: Join orders with customers, products, and payments
        for a complete order 360 view.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                "project": {"name": "multi-join", "version": "1.0.0"},
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
                        "name": "orders",
                        "topic": "orders.v1",
                        "columns": [
                            {"name": "order_id"},
                            {"name": "customer_id"},
                            {"name": "product_id"},
                            {"name": "quantity", "type": "INT"},
                            {"name": "order_time", "type": "TIMESTAMP(3)"},
                        ],
                        "event_time": {
                            "column": "order_time",
                            "watermark": {"max_out_of_orderness_ms": 5000},
                        },
                    },
                    {
                        "name": "customers",
                        "topic": "customers.v1",
                        "columns": [
                            {"name": "customer_id"},
                            {"name": "name"},
                            {"name": "email"},
                        ],
                    },
                    {
                        "name": "products",
                        "topic": "products.v1",
                        "columns": [
                            {"name": "product_id"},
                            {"name": "product_name"},
                            {"name": "unit_price", "type": "DOUBLE"},
                        ],
                    },
                ],
                "models": [
                    {
                        "name": "order_360",
                        "description": "Complete order view with all dimensions",

                        "flink": {"state_ttl_ms": 86400000},
                        "sql": """
                            SELECT
                                o.order_id,
                                c.name as customer_name,
                                c.email as customer_email,
                                p.product_name,
                                o.quantity,
                                p.unit_price,
                                o.quantity * p.unit_price as line_total,
                                o.order_time
                            FROM {{ source("orders") }} o
                            LEFT JOIN {{ source("customers") }} c
                                ON o.customer_id = c.customer_id
                            LEFT JOIN {{ source("products") }} p
                                ON o.product_id = p.product_id
                        """,
                    }
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

            # Verify all three sources are upstream
            upstream = dag.get_upstream("order_360")
            assert "orders" in upstream
            assert "customers" in upstream
            assert "products" in upstream

class TestCDCPipelineScenarios:
    """Tests for CDC pipeline patterns from the tutorial."""

    def _create_project(self, tmpdir: str, config: dict) -> Path:
        project_path = Path(tmpdir)
        with open(project_path / "stream_project.yml", "w") as f:
            yaml.dump(config, f)
        return project_path

    def test_debezium_cdc_source(self):
        """
        SCENARIO: Debezium CDC source for database changes

        From tutorial: Configure a Debezium CDC source to capture
        changes from PostgreSQL.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                "project": {
                    "name": "cdc-pipeline-tutorial",
                    "version": "1.0.0",
                    "description": "Tutorial: CDC pipelines",
                },
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
                        "name": "customers_cdc",
                        "description": "Customer changes from PostgreSQL via Debezium",
                        "topic": "postgres.public.customers",
                        "columns": [
                            {"name": "id", "type": "INT"},
                            {"name": "name"},
                            {"name": "email"},
                            {"name": "created_at", "type": "TIMESTAMP(3)"},
                            {"name": "updated_at", "type": "TIMESTAMP(3)"},
                        ],
                    },
                    {
                        "name": "orders_cdc",
                        "description": "Order changes from PostgreSQL via Debezium",
                        "topic": "postgres.public.orders",
                        "columns": [
                            {"name": "id", "type": "INT"},
                            {"name": "customer_id", "type": "INT"},
                            {"name": "amount", "type": "DOUBLE"},
                            {"name": "status"},
                            {"name": "order_time", "type": "TIMESTAMP(3)"},
                        ],
                        "event_time": {
                            "column": "order_time",
                            "watermark": {"max_out_of_orderness_ms": 10000},
                        },
                    },
                ],
                "models": [
                    {
                        "name": "customer_orders_cdc",
                        "description": "Customer orders from CDC streams",

                        "flink": {"state_ttl_ms": 604800000},  # 7 days
                        "sql": """
                            SELECT
                                o.id as order_id,
                                c.id as customer_id,
                                c.name as customer_name,
                                c.email,
                                o.amount,
                                o.status,
                                o.order_time
                            FROM {{ source("orders_cdc") }} o
                            JOIN {{ source("customers_cdc") }} c
                                ON o.customer_id = c.id
                        """,
                    }
                ],
            }

            project_path = self._create_project(tmpdir, config)
            parser = ProjectParser(project_path)
            project = parser.parse()

            validator = ProjectValidator(project)
            result = validator.validate()
            assert result.is_valid

            # Verify sources are parsed correctly
            assert len(project.sources) == 2
            assert project.sources[0].name == "customers_cdc"
            assert project.sources[1].name == "orders_cdc"
            assert project.sources[1].event_time is not None

            compiler = Compiler(project)
            manifest = compiler.compile(dry_run=True)

            # Verify Flink job
            flink_jobs = manifest.artifacts["flink_jobs"]
            assert len(flink_jobs) == 1
            assert flink_jobs[0]["name"] == "customer_orders_cdc"
            assert flink_jobs[0]["state_ttl_ms"] == 604800000

    def test_customer_360_view_from_cdc(self):
        """
        SCENARIO: Customer 360 view from CDC streams

        From tutorial: Build a real-time customer 360 view combining
        customer profile, orders, and support tickets.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                "project": {"name": "customer-360", "version": "1.0.0"},
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
                        "name": "customers",
                        "topic": "postgres.public.customers",
                        "columns": [
                            {"name": "id", "type": "INT"},
                            {"name": "name"},
                            {"name": "segment"},
                        ],
                    },
                    {
                        "name": "orders",
                        "topic": "postgres.public.orders",
                        "columns": [
                            {"name": "id", "type": "INT"},
                            {"name": "customer_id", "type": "INT"},
                            {"name": "amount", "type": "DOUBLE"},
                            {"name": "order_time", "type": "TIMESTAMP(3)"},
                        ],
                        "event_time": {
                            "column": "order_time",
                            "watermark": {"max_out_of_orderness_ms": 60000},
                        },
                    },
                ],
                "models": [
                    {
                        "name": "customer_metrics",
                        "description": "Customer lifetime metrics",

                        "flink": {"state_ttl_ms": 2592000000},  # 30 days
                        "sql": """
                            SELECT
                                o.customer_id,
                                COUNT(*) as total_orders,
                                SUM(o.amount) as lifetime_value,
                                MAX(o.order_time) as last_order_time
                            FROM {{ source("orders") }} o
                            GROUP BY o.customer_id
                        """,
                    },
                    {
                        "name": "customer_360",
                        "description": "Complete customer 360 view",

                        "flink": {"state_ttl_ms": 86400000},
                        "sql": """
                            SELECT
                                c.id as customer_id,
                                c.name,
                                c.segment,
                                COALESCE(m.total_orders, 0) as total_orders,
                                COALESCE(m.lifetime_value, 0) as lifetime_value,
                                m.last_order_time
                            FROM {{ source("customers") }} c
                            LEFT JOIN {{ ref("customer_metrics") }} m
                                ON c.id = m.customer_id
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

            # Verify model dependency chain
            dag_builder = DAGBuilder(project)
            dag = dag_builder.build()

            # customer_360 depends on customer_metrics and customers
            upstream = dag.get_upstream("customer_360")
            assert "customer_metrics" in upstream
            assert "customers" in upstream

            # customer_metrics depends on orders
            assert "orders" in dag.get_upstream("customer_metrics")

            compiler = Compiler(project)
            manifest = compiler.compile(dry_run=True)

            # Verify both jobs have state TTL
            flink_jobs = manifest.artifacts["flink_jobs"]
            assert len(flink_jobs) == 2
            for job in flink_jobs:
                assert job["state_ttl_ms"] is not None

    def test_real_time_revenue_from_cdc(self):
        """
        SCENARIO: Real-time revenue tracking from CDC

        From tutorial: Calculate real-time revenue metrics from
        order changes.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                "project": {"name": "real-time-revenue", "version": "1.0.0"},
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
                        "name": "orders",
                        "topic": "postgres.public.orders",
                        "columns": [
                            {"name": "id", "type": "INT"},
                            {"name": "amount", "type": "DOUBLE"},
                            {"name": "status"},
                            {"name": "region"},
                            {"name": "order_time", "type": "TIMESTAMP(3)"},
                        ],
                        "event_time": {
                            "column": "order_time",
                            "watermark": {"max_out_of_orderness_ms": 30000},
                        },
                    }
                ],
                "models": [
                    {
                        "name": "hourly_revenue",
                        "description": "Hourly revenue by region",

                        "flink": {"state_ttl_ms": 172800000},  # 48 hours
                        "sql": """
                            SELECT
                                TUMBLE_START(order_time, INTERVAL '1' HOUR) as hour_start,
                                region,
                                COUNT(*) as order_count,
                                SUM(amount) as revenue,
                                SUM(CASE WHEN status = 'completed' THEN amount ELSE 0 END) as completed_revenue
                            FROM {{ source("orders") }}
                            GROUP BY
                                TUMBLE(order_time, INTERVAL '1' HOUR),
                                region
                        """,
                    },
                    {
                        "name": "daily_revenue_summary",
                        "description": "Daily revenue summary",

                        "flink": {"state_ttl_ms": 604800000},  # 7 days
                        "sql": """
                            SELECT
                                TUMBLE_START(order_time, INTERVAL '1' DAY) as day_start,
                                SUM(amount) as total_revenue,
                                COUNT(*) as total_orders,
                                AVG(amount) as avg_order_value
                            FROM {{ source("orders") }}
                            GROUP BY TUMBLE(order_time, INTERVAL '1' DAY)
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

            # Verify both have appropriate TTL
            hourly_job = next(j for j in flink_jobs if j["name"] == "hourly_revenue")
            daily_job = next(j for j in flink_jobs if j["name"] == "daily_revenue_summary")

            assert hourly_job["state_ttl_ms"] == 172800000
            assert daily_job["state_ttl_ms"] == 604800000
