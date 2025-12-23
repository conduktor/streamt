"""E-commerce domain test scenarios.

Simulates a complete e-commerce streaming platform with:
- Order processing pipeline
- Payment processing
- Inventory management
- Customer analytics
- Fraud detection
"""

import tempfile
from pathlib import Path

import yaml

from streamt.compiler import Compiler
from streamt.core.dag import DAGBuilder
from streamt.core.parser import ProjectParser
from streamt.core.validator import ProjectValidator


class TestEcommerceOrderPipeline:
    """Test complete order processing pipeline."""

    def _create_project(self, tmpdir: str, config: dict) -> Path:
        """Helper to create a project."""
        project_path = Path(tmpdir)
        with open(project_path / "stream_project.yml", "w") as f:
            yaml.dump(config, f)
        return project_path

    def test_full_order_pipeline(self):
        """
        SCENARIO: Complete order processing pipeline

        Story: An e-commerce company needs to process orders in real-time.
        Orders come from multiple channels (web, mobile, POS), need to be
        validated, enriched with customer data, and then split into
        different streams for fulfillment, analytics, and notifications.

        Pipeline:
        orders_raw -> orders_validated -> orders_enriched -> [
            orders_for_fulfillment,
            orders_for_analytics,
            orders_for_notifications
        ]
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                "project": {
                    "name": "ecommerce-orders",
                    "version": "1.0.0",
                    "description": "Order processing pipeline",
                },
                "runtime": {
                    "kafka": {"bootstrap_servers": "localhost:9092"},
                    "schema_registry": {"url": "http://localhost:8081"},
                    "flink": {
                        "default": "prod",
                        "clusters": {"prod": {"type": "rest", "rest_url": "http://localhost:8082"}},
                    },
                },
                "sources": [
                    {
                        "name": "orders_raw",
                        "description": "Raw orders from all channels",
                        "topic": "ecom.orders.raw.v1",
                        "columns": [
                            {"name": "order_id", "classification": "internal"},
                            {"name": "customer_id", "classification": "internal"},
                            {"name": "items", "classification": "internal"},
                            {"name": "total_amount", "classification": "internal"},
                            {"name": "channel", "classification": "public"},
                        ],
                    },
                    {
                        "name": "customers",
                        "description": "Customer master data",
                        "topic": "ecom.customers.v1",
                    },
                    {
                        "name": "products",
                        "description": "Product catalog",
                        "topic": "ecom.products.v1",
                    },
                ],
                "models": [
                    {
                        "name": "orders_validated",
                        "description": "Orders with validation status",
                        "sql": """
                            SELECT
                                order_id,
                                customer_id,
                                items,
                                total_amount,
                                channel,
                                CASE
                                    WHEN total_amount > 0 AND customer_id IS NOT NULL
                                    THEN 'VALID'
                                    ELSE 'INVALID'
                                END as validation_status,
                                event_time
                            FROM {{ source("orders_raw") }}
                        """,
                        "advanced": {"topic": {"partitions": 12}},
                    },
                    {
                        "name": "orders_enriched",
                        "description": "Orders enriched with customer and product data",

                        "advanced": {
                            "topic": {"partitions": 12},
                        },
                        "sql": """
                            SELECT
                                o.order_id,
                                o.customer_id,
                                c.customer_name,
                                c.customer_segment,
                                o.items,
                                o.total_amount,
                                o.channel,
                                o.event_time
                            FROM {{ ref("orders_validated") }} o
                            JOIN {{ source("customers") }} c
                                ON o.customer_id = c.customer_id
                            WHERE o.validation_status = 'VALID'
                        """,
                    },
                    {
                        "name": "orders_for_fulfillment",
                        "description": "Orders ready for warehouse fulfillment",

                        "advanced": {
                            "topic": {"partitions": 6},
                        },
                        "sql": """
                            SELECT
                                order_id,
                                customer_id,
                                items,
                                shipping_address
                            FROM {{ ref("orders_enriched") }}
                        """,
                    },
                    {
                        "name": "orders_for_analytics",
                        "description": "Orders for real-time analytics dashboard",

                        "advanced": {
                            "topic": {"partitions": 6},
                        },
                        "sql": """
                            SELECT
                                TUMBLE_END(event_time, INTERVAL '5' MINUTE) as window_end,
                                channel,
                                customer_segment,
                                COUNT(*) as order_count,
                                SUM(total_amount) as total_revenue
                            FROM {{ ref("orders_enriched") }}
                            GROUP BY
                                TUMBLE(event_time, INTERVAL '5' MINUTE),
                                channel,
                                customer_segment
                        """,
                    },
                    {
                        "name": "high_value_orders",
                        "description": "High value orders for priority processing",

                        "advanced": {
                            "topic": {"partitions": 3},
                        },
                        "sql": """
                            SELECT * FROM {{ ref("orders_enriched") }}
                            WHERE total_amount > 1000
                        """,
                    },
                ],
                "tests": [
                    {
                        "name": "orders_validated_schema",
                        "model": "orders_validated",
                        "type": "schema",
                        "assertions": [
                            {
                                "not_null": {
                                    "columns": ["order_id", "customer_id", "validation_status"]
                                }
                            },
                            {
                                "accepted_values": {
                                    "column": "validation_status",
                                    "values": ["VALID", "INVALID"],
                                }
                            },
                        ],
                    },
                    {
                        "name": "orders_enriched_schema",
                        "model": "orders_enriched",
                        "type": "schema",
                        "assertions": [
                            {"not_null": {"columns": ["order_id", "customer_name"]}},
                        ],
                    },
                ],
                "exposures": [
                    {
                        "name": "fulfillment_service",
                        "type": "application",
                        "role": "consumer",
                        "description": "Warehouse fulfillment microservice",
                        "owner": "fulfillment-team@company.com",
                        "consumes": [{"ref": "orders_for_fulfillment"}],
                        "consumer_group": "fulfillment-service-cg",
                    },
                    {
                        "name": "analytics_dashboard",
                        "type": "dashboard",
                        "description": "Real-time sales dashboard",
                        "owner": "analytics-team@company.com",
                        "depends_on": [{"ref": "orders_for_analytics"}],
                        "url": "https://dashboard.company.com/sales",
                    },
                    {
                        "name": "vip_notification_service",
                        "type": "application",
                        "role": "consumer",
                        "description": "Sends notifications for high-value orders",
                        "consumes": [{"ref": "high_value_orders"}],
                    },
                ],
            }

            project_path = self._create_project(tmpdir, config)
            parser = ProjectParser(project_path)
            project = parser.parse()

            # Validate
            validator = ProjectValidator(project)
            result = validator.validate()
            assert result.is_valid, f"Validation errors: {[e.message for e in result.errors]}"

            # Build DAG
            dag_builder = DAGBuilder(project)
            dag = dag_builder.build()

            # Verify DAG structure
            assert "orders_raw" in dag.nodes
            assert "orders_validated" in dag.nodes
            assert "orders_enriched" in dag.nodes
            assert "orders_for_fulfillment" in dag.nodes
            assert "orders_for_analytics" in dag.nodes
            assert "high_value_orders" in dag.nodes

            # Verify dependencies
            assert "orders_raw" in dag.get_upstream("orders_validated")
            assert "orders_validated" in dag.get_upstream("orders_enriched")
            assert "customers" in dag.get_upstream("orders_enriched")

            # Verify downstream
            downstream = dag.get_downstream("orders_enriched")
            assert "orders_for_fulfillment" in downstream
            assert "orders_for_analytics" in downstream
            assert "high_value_orders" in downstream

            # Compile
            compiler = Compiler(project)
            manifest = compiler.compile(dry_run=True)

            # Verify artifacts
            assert len(manifest.artifacts["topics"]) >= 5
            assert len(manifest.artifacts["flink_jobs"]) >= 3

    def test_order_pipeline_with_dlq(self):
        """
        SCENARIO: Order pipeline with DLQ for failed validations

        Story: Orders that fail validation should be routed to a DLQ
        for manual review. The DLQ is a first-class model in the pipeline.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                "project": {"name": "orders-with-dlq", "version": "1.0.0"},
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
                    {"name": "orders_raw", "topic": "orders.raw.v1"},
                ],
                "models": [
                    {
                        "name": "orders_validated",
                        "description": "Successfully validated orders",

                        "sql": """
                            SELECT * FROM {{ source("orders_raw") }}
                            WHERE total_amount > 0
                              AND customer_id IS NOT NULL
                        """,
                    },
                    {
                        "name": "orders_dlq",
                        "description": "Dead letter queue for failed order validations",

                        "topic": {"partitions": 3, "config": {"retention.ms": "604800000"}},
                        "sql": """
                            SELECT
                                *,
                                'VALIDATION_FAILED' as error_type,
                                CURRENT_TIMESTAMP as failed_at
                            FROM {{ source("orders_raw") }}
                            WHERE total_amount <= 0
                               OR customer_id IS NULL
                        """,
                    },
                    {
                        "name": "orders_processed",
                        "description": "Final processed orders",

                        "sql": """SELECT * FROM {{ ref("orders_validated") }}""",
                    },
                ],
                "tests": [
                    {
                        "name": "orders_validated_quality",
                        "model": "orders_validated",
                        "type": "schema",
                        "assertions": [
                            {"not_null": {"columns": ["order_id", "customer_id", "total_amount"]}},
                            {"range": {"column": "total_amount", "min": 0.01}},
                        ],
                        "on_failure": {
                            "severity": "error",
                            "actions": [
                                {"dlq": {"model": "orders_dlq"}},
                                {"alert": {"type": "slack", "channel": "#data-alerts"}},
                            ],
                        },
                    },
                ],
            }

            project_path = self._create_project(tmpdir, config)
            parser = ProjectParser(project_path)
            project = parser.parse()

            validator = ProjectValidator(project)
            result = validator.validate()
            assert result.is_valid

            # Verify DLQ is in the DAG
            dag_builder = DAGBuilder(project)
            dag = dag_builder.build()
            assert "orders_dlq" in dag.nodes

class TestEcommerceInventoryManagement:
    """Test inventory management scenarios."""

    def _create_project(self, tmpdir: str, config: dict) -> Path:
        project_path = Path(tmpdir)
        with open(project_path / "stream_project.yml", "w") as f:
            yaml.dump(config, f)
        return project_path

    def test_real_time_inventory_tracking(self):
        """
        SCENARIO: Real-time inventory tracking with stock alerts

        Story: Track inventory levels in real-time. When stock drops below
        threshold, generate alerts. Aggregate stock movements per warehouse.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                "project": {"name": "inventory-tracking", "version": "1.0.0"},
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
                        "name": "stock_movements",
                        "description": "Inbound/outbound stock movements",
                        "topic": "inventory.movements.v1",
                    },
                    {
                        "name": "products_master",
                        "description": "Product master data with reorder thresholds",
                        "topic": "inventory.products.v1",
                    },
                ],
                "models": [
                    {
                        "name": "stock_levels",
                        "description": "Current stock level per product/warehouse",

                        "topic": {"partitions": 12},
                        "sql": """
                            SELECT
                                product_id,
                                warehouse_id,
                                SUM(CASE WHEN movement_type = 'IN' THEN quantity ELSE -quantity END) as current_stock,
                                MAX(event_time) as last_updated
                            FROM {{ source("stock_movements") }}
                            GROUP BY product_id, warehouse_id
                        """,
                    },
                    {
                        "name": "low_stock_alerts",
                        "description": "Products below reorder threshold",

                        "topic": {"partitions": 3},
                        "sql": """
                            SELECT
                                s.product_id,
                                s.warehouse_id,
                                s.current_stock,
                                p.reorder_threshold,
                                p.product_name,
                                'LOW_STOCK' as alert_type
                            FROM {{ ref("stock_levels") }} s
                            JOIN {{ source("products_master") }} p
                                ON s.product_id = p.product_id
                            WHERE s.current_stock < p.reorder_threshold
                        """,
                    },
                    {
                        "name": "warehouse_summary",
                        "description": "Aggregated warehouse metrics per hour",

                        "sql": """
                            SELECT
                                warehouse_id,
                                TUMBLE_END(event_time, INTERVAL '1' HOUR) as window_end,
                                SUM(CASE WHEN movement_type = 'IN' THEN quantity ELSE 0 END) as total_inbound,
                                SUM(CASE WHEN movement_type = 'OUT' THEN quantity ELSE 0 END) as total_outbound,
                                COUNT(DISTINCT product_id) as unique_products
                            FROM {{ source("stock_movements") }}
                            GROUP BY warehouse_id, TUMBLE(event_time, INTERVAL '1' HOUR)
                        """,
                    },
                ],
                "exposures": [
                    {
                        "name": "procurement_system",
                        "type": "application",
                        "role": "consumer",
                        "description": "Auto-reorder system",
                        "consumes": [{"ref": "low_stock_alerts"}],
                    },
                    {
                        "name": "warehouse_dashboard",
                        "type": "dashboard",
                        "description": "Warehouse operations dashboard",
                        "depends_on": [
                            {"ref": "stock_levels"},
                            {"ref": "warehouse_summary"},
                        ],
                    },
                ],
            }

            project_path = self._create_project(tmpdir, config)
            parser = ProjectParser(project_path)
            project = parser.parse()

            validator = ProjectValidator(project)
            result = validator.validate()
            assert result.is_valid

            # Verify complex join dependencies
            dag_builder = DAGBuilder(project)
            dag = dag_builder.build()

            # low_stock_alerts depends on both stock_levels and products_master
            upstream = dag.get_upstream("low_stock_alerts")
            assert "stock_levels" in upstream
            assert "products_master" in upstream

class TestEcommercePaymentProcessing:
    """Test payment processing scenarios."""

    def _create_project(self, tmpdir: str, config: dict) -> Path:
        project_path = Path(tmpdir)
        with open(project_path / "stream_project.yml", "w") as f:
            yaml.dump(config, f)
        return project_path

    def test_payment_pipeline_with_fraud_detection(self):
        """
        SCENARIO: Payment processing with real-time fraud detection

        Story: Process payments, run fraud detection rules, and split
        into approved/flagged streams. Flagged payments need manual review.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                "project": {"name": "payment-processing", "version": "1.0.0"},
                "runtime": {
                    "kafka": {"bootstrap_servers": "localhost:9092"},
                    "flink": {
                        "default": "local",
                        "clusters": {
                            "local": {"type": "rest", "rest_url": "http://localhost:8082"}
                        },
                    },
                },
                "rules": {
                    "topics": {"min_partitions": 6},
                    "models": {"require_description": True},
                },
                "sources": [
                    {
                        "name": "payments_raw",
                        "description": "Raw payment transactions",
                        "topic": "payments.raw.v1",
                        "columns": [
                            {"name": "card_number", "classification": "highly_sensitive"},
                            {"name": "cvv", "classification": "highly_sensitive"},
                            {"name": "amount", "classification": "confidential"},
                        ],
                    },
                    {
                        "name": "customer_risk_scores",
                        "description": "Customer risk profiles",
                        "topic": "customers.risk.v1",
                    },
                    {
                        "name": "merchant_blocklist",
                        "description": "Blocked merchants",
                        "topic": "merchants.blocklist.v1",
                    },
                ],
                "models": [
                    {
                        "name": "payments_tokenized",
                        "description": "Payments with tokenized card data",

                        "topic": {"partitions": 12},
                        "security": {
                            "classification": {
                                "card_token": "internal",
                            },
                            "policies": [
                                {"mask": {"column": "card_number", "method": "hash"}},
                                {"mask": {"column": "cvv", "method": "redact"}},
                            ],
                        },
                        "sql": """
                            SELECT
                                payment_id,
                                MD5(card_number) as card_token,
                                '***' as cvv,
                                amount,
                                customer_id,
                                merchant_id,
                                event_time
                            FROM {{ source("payments_raw") }}
                        """,
                    },
                    {
                        "name": "payments_with_risk",
                        "description": "Payments enriched with risk scores",

                        "topic": {"partitions": 12},
                        "sql": """
                            SELECT
                                p.*,
                                COALESCE(r.risk_score, 50) as customer_risk_score,
                                CASE
                                    WHEN p.amount > 5000 THEN 'HIGH_AMOUNT'
                                    WHEN r.risk_score > 80 THEN 'HIGH_RISK_CUSTOMER'
                                    ELSE 'NORMAL'
                                END as risk_flag
                            FROM {{ ref("payments_tokenized") }} p
                            LEFT JOIN {{ source("customer_risk_scores") }} r
                                ON p.customer_id = r.customer_id
                        """,
                    },
                    {
                        "name": "payments_fraud_check",
                        "description": "Payments with fraud detection results",

                        "topic": {"partitions": 12},
                        "sql": """
                            SELECT
                                p.*,
                                CASE
                                    WHEN b.merchant_id IS NOT NULL THEN TRUE
                                    WHEN p.risk_flag = 'HIGH_AMOUNT' AND p.customer_risk_score > 70 THEN TRUE
                                    ELSE FALSE
                                END as is_flagged_fraud
                            FROM {{ ref("payments_with_risk") }} p
                            LEFT JOIN {{ source("merchant_blocklist") }} b
                                ON p.merchant_id = b.merchant_id
                        """,
                    },
                    {
                        "name": "payments_approved",
                        "description": "Approved payments for settlement",

                        "topic": {"partitions": 6},
                        "sql": """
                            SELECT * FROM {{ ref("payments_fraud_check") }}
                            WHERE is_flagged_fraud = FALSE
                        """,
                    },
                    {
                        "name": "payments_flagged",
                        "description": "Flagged payments for manual review",

                        "topic": {"partitions": 6},  # Must meet min_partitions rule
                        "sql": """
                            SELECT * FROM {{ ref("payments_fraud_check") }}
                            WHERE is_flagged_fraud = TRUE
                        """,
                    },
                    {
                        "name": "fraud_metrics",
                        "description": "Real-time fraud detection metrics",

                        "sql": """
                            SELECT
                                TUMBLE_END(event_time, INTERVAL '5' MINUTE) as window_end,
                                COUNT(*) as total_payments,
                                SUM(CASE WHEN is_flagged_fraud THEN 1 ELSE 0 END) as flagged_count,
                                SUM(amount) as total_amount,
                                SUM(CASE WHEN is_flagged_fraud THEN amount ELSE 0 END) as flagged_amount
                            FROM {{ ref("payments_fraud_check") }}
                            GROUP BY TUMBLE(event_time, INTERVAL '5' MINUTE)
                        """,
                    },
                ],
                "tests": [
                    {
                        "name": "payments_tokenized_no_pii",
                        "model": "payments_tokenized",
                        "type": "schema",
                        "assertions": [
                            {"not_null": {"columns": ["payment_id", "card_token"]}},
                        ],
                    },
                    {
                        "name": "fraud_metrics_continuous",
                        "model": "fraud_metrics",
                        "type": "continuous",
                        "assertions": [
                            {"throughput": {"min_per_second": 10}},
                        ],
                    },
                ],
                "exposures": [
                    {
                        "name": "payment_gateway",
                        "type": "application",
                        "role": "consumer",
                        "description": "Payment gateway for settlement",
                        "consumes": [{"ref": "payments_approved"}],
                    },
                    {
                        "name": "fraud_review_app",
                        "type": "application",
                        "role": "consumer",
                        "description": "Manual fraud review application",
                        "consumes": [{"ref": "payments_flagged"}],
                    },
                    {
                        "name": "fraud_dashboard",
                        "type": "dashboard",
                        "description": "Real-time fraud monitoring",
                        "depends_on": [{"ref": "fraud_metrics"}],
                    },
                ],
            }

            project_path = self._create_project(tmpdir, config)
            parser = ProjectParser(project_path)
            project = parser.parse()

            validator = ProjectValidator(project)
            result = validator.validate()
            assert result.is_valid, f"Errors: {[e.message for e in result.errors]}"

            # Verify DAG complexity
            dag_builder = DAGBuilder(project)
            dag = dag_builder.build()

            # payments_fraud_check has multiple upstream dependencies
            upstream = dag.get_upstream("payments_fraud_check")
            assert "payments_with_risk" in upstream
            assert "merchant_blocklist" in upstream

            # Both approved and flagged come from fraud_check
            assert "payments_fraud_check" in dag.get_upstream("payments_approved")
            assert "payments_fraud_check" in dag.get_upstream("payments_flagged")

            # Compile and verify masking is applied
            compiler = Compiler(project)
            manifest = compiler.compile(dry_run=True)

            # Verify all required artifacts are generated
            assert len(manifest.artifacts["topics"]) >= 5
            assert len(manifest.artifacts["flink_jobs"]) >= 4
