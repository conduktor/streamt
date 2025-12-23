"""Multi-team and governance test scenarios.

Tests enterprise governance patterns:
- Topic naming conventions
- Ownership and access control
- Cross-team dependencies
- Data quality enforcement
- Compliance and audit requirements

NOTE: These tests validate PARSING and STRUCTURAL validation of governance
configurations. Runtime ENFORCEMENT of governance rules (e.g., blocking
deployments, alerting on violations) is NOT YET IMPLEMENTED. Tests marked
with @pytest.mark.skip indicate features that require enforcement logic.
"""

import tempfile
from pathlib import Path

import pytest
import yaml

from streamt.compiler import Compiler
from streamt.core.dag import DAGBuilder
from streamt.core.parser import ProjectParser
from streamt.core.validator import ProjectValidator

class TestTopicNamingGovernance:
    """Test topic naming convention enforcement."""

    def _create_project(self, tmpdir: str, config: dict) -> Path:
        project_path = Path(tmpdir)
        with open(project_path / "stream_project.yml", "w") as f:
            yaml.dump(config, f)
        return project_path

    def test_valid_naming_convention(self):
        """
        SCENARIO: Enforce topic naming convention across organization

        Story: Organization requires topics to follow pattern:
        {domain}.{entity}.{event_type}.{version}
        e.g., orders.purchase.created.v1
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                "project": {
                    "name": "order-service",
                    "version": "1.0.0",
                    "description": "Order processing service",
                },
                "runtime": {
                    "kafka": {"bootstrap_servers": "localhost:9092"},
                    "flink": {
                        "default": "local",
                        "clusters": {
                            "local": {"type": "rest", "rest_url": "http://localhost:8082"}
                        },
                    },
                },
                "governance": {
                    "topic_rules": {
                        "naming_pattern": "^[a-z]+\\.[a-z_]+\\.[a-z]+\\.v\\d+$",
                        "naming_description": "{domain}.{entity}.{event_type}.v{version}",
                    },
                },
                "sources": [
                    {
                        "name": "order_events",
                        "description": "Raw order events",
                        "topic": "orders.purchase.raw.v1",  # Follows pattern
                    },
                ],
                "models": [
                    {
                        "name": "order_created",
                        "description": "Order created events",

                        "advanced": {
                            "topic": {"name": "orders.purchase.created.v1"},  # Follows pattern
                        },
                        "sql": """
                            SELECT * FROM {{ source("order_events") }}
                            WHERE event_type = 'CREATED'
                        """,
                    },
                    {
                        "name": "order_completed",
                        "description": "Order completed events",

                        "advanced": {
                            "topic": {"name": "orders.purchase.completed.v1"},  # Follows pattern
                        },
                        "sql": """
                            SELECT * FROM {{ source("order_events") }}
                            WHERE event_type = 'COMPLETED'
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

    def test_environment_based_naming(self):
        """
        SCENARIO: Different naming patterns per environment

        Story: Dev/staging use relaxed naming, production requires
        strict naming conventions.

        This test validates that environment-specific governance configurations
        are correctly parsed and accessible. The validator ensures the configuration
        structure is valid. Runtime enforcement (applying rules at deploy time)
        would use these parsed configs.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                "project": {
                    "name": "user-service",
                    "version": "1.0.0",
                },
                "runtime": {
                    "kafka": {"bootstrap_servers": "localhost:9092"},
                    "flink": {
                        "default": "local",
                        "clusters": {
                            "local": {"type": "rest", "rest_url": "http://localhost:8082"}
                        },
                    },
                },
                "governance": {
                    "environments": {
                        "dev": {
                            "topic_rules": {
                                "naming_pattern": "^dev\\..*",  # Relaxed
                                "min_partitions": 1,
                            },
                        },
                        "staging": {
                            "topic_rules": {
                                "naming_pattern": "^staging\\.[a-z]+\\.[a-z_]+\\.v\\d+$",
                                "min_partitions": 3,
                            },
                        },
                        "prod": {
                            "topic_rules": {
                                "naming_pattern": "^[a-z]+\\.[a-z_]+\\.[a-z]+\\.v\\d+$",
                                "min_partitions": 6,
                                "min_replication_factor": 3,
                            },
                        },
                    },
                },
                "sources": [
                    {"name": "user_events", "topic": "users.events.raw.v1"},
                ],
                "models": [
                    {
                        "name": "user_registered",

                        "topic": {"name": "users.registered.created.v1", "partitions": 6},
                        "sql": "SELECT * FROM {{ source('user_events') }} WHERE event_type = 'REGISTERED'",
                    },
                ],
            }

            project_path = self._create_project(tmpdir, config)
            parser = ProjectParser(project_path)
            project = parser.parse()

            validator = ProjectValidator(project)
            result = validator.validate()
            assert result.is_valid

class TestOwnershipAndAccessControl:
    """Test ownership and access control patterns."""

    def _create_project(self, tmpdir: str, config: dict) -> Path:
        project_path = Path(tmpdir)
        with open(project_path / "stream_project.yml", "w") as f:
            yaml.dump(config, f)
        return project_path

    def test_ownership_requirements(self):
        """
        SCENARIO: Require ownership on all models and sources

        Story: Every source and model must have an owner team
        for accountability and incident response.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                "project": {"name": "customer-data", "version": "1.0.0"},
                "runtime": {
                    "kafka": {"bootstrap_servers": "localhost:9092"},
                    "flink": {
                        "default": "local",
                        "clusters": {
                            "local": {"type": "rest", "rest_url": "http://localhost:8082"}
                        },
                    },
                },
                "governance": {
                    "require_owners": True,
                    "valid_owners": [
                        "platform-team",
                        "customer-team",
                        "analytics-team",
                        "data-engineering",
                    ],
                },
                "sources": [
                    {
                        "name": "customer_updates",
                        "description": "Customer profile updates",
                        "topic": "customers.updates.v1",
                        "owner": "customer-team",
                    },
                ],
                "models": [
                    {
                        "name": "customer_profiles",
                        "description": "Current customer profiles",

                        "owner": "customer-team",
                        "sql": "SELECT * FROM {{ source('customer_updates') }}",
                    },
                    {
                        "name": "customer_analytics",
                        "description": "Customer analytics aggregations",

                        "owner": "analytics-team",
                        "sql": """
                            SELECT
                                country,
                                COUNT(*) as customer_count
                            FROM {{ ref("customer_profiles") }}
                            GROUP BY country
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

            # Verify owners are assigned
            for source in project.sources:
                assert source.owner is not None

            for model in project.models:
                assert model.owner is not None

    def test_pii_access_control(self):
        """
        SCENARIO: Restrict access to PII data

        Story: Models containing PII must be tagged and access
        restricted to authorized teams only.

        This test validates that PII data classification, tagging, and access
        control configurations are correctly parsed. The validator warns when
        security policies require Gateway but Gateway is not configured.
        Runtime enforcement via Conduktor Gateway uses these parsed configs.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                "project": {"name": "user-data", "version": "1.0.0"},
                "runtime": {
                    "kafka": {"bootstrap_servers": "localhost:9092"},
                    "flink": {
                        "default": "local",
                        "clusters": {
                            "local": {"type": "rest", "rest_url": "http://localhost:8082"}
                        },
                    },
                },
                "governance": {
                    "require_owners": True,
                    "data_classification": {
                        "pii": {
                            "allowed_consumers": ["gdpr-compliance", "customer-support"],
                            "retention_max_days": 90,
                            "requires_masking": True,
                        },
                        "public": {
                            "allowed_consumers": ["*"],
                        },
                    },
                },
                "sources": [
                    {
                        "name": "user_events",
                        "topic": "users.events.v1",
                        "owner": "platform-team",
                    },
                ],
                "models": [
                    {
                        "name": "user_profiles_pii",
                        "description": "User profiles with PII",

                        "owner": "data-governance",
                        "tags": ["pii", "gdpr-sensitive"],
                        "topic": {
                            "config": {"retention.ms": "7776000000"},  # 90 days
                        },
                        "sql": """
                            SELECT
                                user_id,
                                email,
                                full_name,
                                phone_number,
                                event_time
                            FROM {{ source("user_events") }}
                        """,
                    },
                    {
                        "name": "user_profiles_anonymized",
                        "description": "User profiles with PII masked",

                        "owner": "data-governance",
                        "tags": ["anonymized", "public"],
                        "sql": """
                            SELECT
                                user_id,
                                CONCAT(SUBSTRING(email, 1, 2), '***@***') as email_masked,
                                'REDACTED' as full_name,
                                CONCAT('***-***-', SUBSTRING(phone_number, -4)) as phone_masked,
                                event_time
                            FROM {{ ref("user_profiles_pii") }}
                        """,
                    },
                ],
                "exposures": [
                    {
                        "name": "customer_support_app",
                        "type": "application",
                        "role": "consumer",
                        "description": "Customer support needs PII access",
                        "consumes": [{"ref": "user_profiles_pii"}],
                        "access_justification": "Required for customer identity verification",
                    },
                    {
                        "name": "analytics_dashboard",
                        "type": "dashboard",
                        "description": "Public analytics dashboard",
                        "depends_on": [{"ref": "user_profiles_anonymized"}],
                    },
                ],
            }

            project_path = self._create_project(tmpdir, config)
            parser = ProjectParser(project_path)
            project = parser.parse()

            validator = ProjectValidator(project)
            result = validator.validate()
            assert result.is_valid

            # Verify PII model has appropriate tags
            pii_model = next(m for m in project.models if m.name == "user_profiles_pii")
            assert "pii" in pii_model.tags

class TestCrossTeamDependencies:
    """Test cross-team dependency management."""

    def _create_project(self, tmpdir: str, config: dict) -> Path:
        project_path = Path(tmpdir)
        with open(project_path / "stream_project.yml", "w") as f:
            yaml.dump(config, f)
        return project_path

    def test_cross_team_data_contracts(self):
        """
        SCENARIO: Define data contracts between teams

        Story: Team A produces data that Team B depends on.
        A data contract defines the schema, SLAs, and responsibilities.

        This test validates that data contract configurations (schema references,
        SLAs, ownership, freshness) are correctly parsed and the DAG correctly
        captures cross-team dependencies. Contract metadata is available for
        documentation and runtime enforcement tools.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                "project": {"name": "cross-team-integration", "version": "1.0.0"},
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
                    # External source from another team
                    {
                        "name": "order_events",
                        "description": "Order events from Order Team",
                        "topic": "orders.events.v2",
                        "owner": "order-team",
                        "freshness": {"max_lag_seconds": 60, "warn_after_seconds": 30},
                        "contract": {
                            "version": "2.0.0",
                            "schema_registry_subject": "orders.events.v2-value",
                            "sla": {
                                "availability": "99.9%",
                                "max_latency_p99_ms": 1000,
                            },
                            "contact": "order-team@company.com",
                        },
                    },
                    # Another external source
                    {
                        "name": "product_catalog",
                        "description": "Product catalog from Catalog Team",
                        "topic": "products.catalog.v1",
                        "owner": "catalog-team",
                        "contract": {
                            "version": "1.0.0",
                            "sla": {"availability": "99.5%"},
                        },
                    },
                ],
                "models": [
                    {
                        "name": "enriched_orders",
                        "description": "Orders enriched with product details",

                        "owner": "analytics-team",
                        "sql": """
                            SELECT
                                o.order_id,
                                o.customer_id,
                                o.product_id,
                                p.product_name,
                                p.category,
                                o.quantity,
                                o.unit_price,
                                o.quantity * o.unit_price as total_price,
                                o.event_time
                            FROM {{ source("order_events") }} o
                            JOIN {{ source("product_catalog") }} p ON o.product_id = p.product_id
                        """,
                    },
                ],
                "exposures": [
                    {
                        "name": "sales_reporting",
                        "type": "dashboard",
                        "description": "Sales reporting dashboard",
                        "depends_on": [{"ref": "enriched_orders"}],
                        "owner": "bi-team",
                    },
                ],
            }

            project_path = self._create_project(tmpdir, config)
            parser = ProjectParser(project_path)
            project = parser.parse()

            validator = ProjectValidator(project)
            result = validator.validate()
            assert result.is_valid

            # Verify cross-team dependencies are captured
            dag_builder = DAGBuilder(project)
            dag = dag_builder.build()

            upstream = dag.get_upstream("enriched_orders")
            assert "order_events" in upstream
            assert "product_catalog" in upstream

    def test_internal_vs_external_sources(self):
        """
        SCENARIO: Distinguish internal vs external data sources

        Story: Some sources are owned by the project team, others are
        external dependencies. This affects change management.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                "project": {"name": "mixed-sources", "version": "1.0.0"},
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
                    # Internal source - we control this
                    {
                        "name": "internal_events",
                        "description": "Events generated by our service",
                        "topic": "myservice.events.v1",
                        "owner": "my-team",
                        "tags": ["internal", "owned"],
                    },
                    # External source - another team controls this
                    {
                        "name": "external_users",
                        "description": "User data from IAM team",
                        "topic": "iam.users.v3",
                        "owner": "iam-team",
                        "tags": ["external", "dependency"],
                        "freshness": {"max_lag_seconds": 300},  # More tolerant for external
                    },
                    # Third-party source - external vendor
                    {
                        "name": "vendor_data",
                        "description": "Data from external vendor API",
                        "topic": "vendor.ingestion.v1",
                        "owner": "integrations-team",
                        "tags": ["external", "third-party", "dependency"],
                        "freshness": {"max_lag_seconds": 600},  # Very tolerant
                    },
                ],
                "models": [
                    {
                        "name": "combined_view",

                        "owner": "my-team",
                        "sql": """
                            SELECT
                                i.event_id,
                                u.user_name,
                                v.vendor_attribute
                            FROM {{ source("internal_events") }} i
                            LEFT JOIN {{ source("external_users") }} u ON i.user_id = u.user_id
                            LEFT JOIN {{ source("vendor_data") }} v ON i.vendor_ref = v.ref_id
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

class TestDataQualityEnforcement:
    """Test data quality rule enforcement."""

    def _create_project(self, tmpdir: str, config: dict) -> Path:
        project_path = Path(tmpdir)
        with open(project_path / "stream_project.yml", "w") as f:
            yaml.dump(config, f)
        return project_path

    def test_mandatory_tests_for_critical_models(self):
        """
        SCENARIO: Require tests for critical models

        Story: Models tagged as critical must have associated tests
        before deployment.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                "project": {"name": "critical-data", "version": "1.0.0"},
                "runtime": {
                    "kafka": {"bootstrap_servers": "localhost:9092"},
                    "flink": {
                        "default": "local",
                        "clusters": {
                            "local": {"type": "rest", "rest_url": "http://localhost:8082"}
                        },
                    },
                },
                "governance": {
                    "model_rules": {
                        "require_tests": ["critical_model", "payment_events"],
                        "require_descriptions": True,
                    },
                },
                "sources": [
                    {"name": "raw_events", "topic": "events.raw.v1"},
                ],
                "models": [
                    {
                        "name": "critical_model",
                        "description": "Critical business model",

                        "tags": ["critical", "tier-1"],
                        "sql": """
                            SELECT * FROM {{ source("raw_events") }}
                            WHERE is_valid = true
                        """,
                    },
                    {
                        "name": "payment_events",
                        "description": "Payment processing events",

                        "tags": ["critical", "financial"],
                        "sql": """
                            SELECT * FROM {{ source("raw_events") }}
                            WHERE event_type = 'PAYMENT'
                        """,
                    },
                ],
                "tests": [
                    {
                        "name": "critical_model_not_null",
                        "model": "critical_model",
                        "type": "schema",
                        "assertions": [
                            {"not_null": {"columns": ["event_id", "user_id"]}},
                        ],
                    },
                    {
                        "name": "critical_model_freshness",
                        "model": "critical_model",
                        "type": "continuous",
                        "assertions": [
                            {"max_lag": {"column": "event_time", "max_seconds": 60}},
                        ],
                    },
                    {
                        "name": "payment_validation",
                        "model": "payment_events",
                        "type": "schema",
                        "assertions": [
                            {"not_null": {"columns": ["payment_id", "amount"]}},
                            {
                                "expression": {
                                    "sql": "amount > 0",
                                    "description": "Amount must be positive",
                                }
                            },
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

            # Verify tests exist for required models
            tested_models = {t.model for t in project.tests}
            assert "critical_model" in tested_models
            assert "payment_events" in tested_models

    def test_data_quality_monitoring(self):
        """
        SCENARIO: Continuous data quality monitoring

        Story: All production models must have data quality tests
        that run continuously and alert on failures.

        This test validates that continuous data quality test configurations
        (assertions, thresholds, distribution checks) are correctly parsed.
        The validator ensures continuous tests have Flink runtime configured.
        Continuous tests are compiled to Flink jobs that can be deployed.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                "project": {"name": "quality-monitoring", "version": "1.0.0"},
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
                    {"name": "transactions", "topic": "txn.events.v1"},
                ],
                "models": [
                    {
                        "name": "transaction_summary",

                        "sql": """
                            SELECT
                                txn_id,
                                amount,
                                currency,
                                status,
                                event_time
                            FROM {{ source("transactions") }}
                        """,
                    },
                ],
                "tests": [
                    # Schema validation
                    {
                        "name": "txn_schema",
                        "model": "transaction_summary",
                        "type": "schema",
                        "assertions": [
                            {"not_null": {"columns": ["txn_id", "amount", "currency", "status"]}},
                            {"unique": {"columns": ["txn_id"]}},
                            {
                                "accepted_values": {
                                    "column": "status",
                                    "values": ["PENDING", "COMPLETED", "FAILED"],
                                }
                            },
                            {
                                "accepted_values": {
                                    "column": "currency",
                                    "values": ["USD", "EUR", "GBP"],
                                }
                            },
                        ],
                    },
                    # Freshness monitoring
                    {
                        "name": "txn_freshness",
                        "model": "transaction_summary",
                        "type": "continuous",
                        "assertions": [
                            {"max_lag": {"column": "event_time", "max_seconds": 30}},
                            {"throughput": {"min_per_second": 100}},
                        ],
                    },
                    # Business logic validation
                    {
                        "name": "txn_business_rules",
                        "model": "transaction_summary",
                        "type": "schema",
                        "assertions": [
                            {
                                "expression": {
                                    "sql": "amount > 0",
                                    "description": "Amount must be positive",
                                }
                            },
                            {
                                "expression": {
                                    "sql": "amount < 1000000",
                                    "description": "Amount must be under $1M",
                                }
                            },
                        ],
                    },
                    # Distribution monitoring
                    {
                        "name": "txn_distribution",
                        "model": "transaction_summary",
                        "type": "continuous",
                        "assertions": [
                            {"distribution": {"column": "amount", "max_stddev": 1000}},
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

            # Verify multiple test types exist
            test_types = {t.type.value for t in project.tests}
            assert "schema" in test_types
            assert "continuous" in test_types

class TestComplianceAndAudit:
    """Test compliance and audit requirements."""

    def _create_project(self, tmpdir: str, config: dict) -> Path:
        project_path = Path(tmpdir)
        with open(project_path / "stream_project.yml", "w") as f:
            yaml.dump(config, f)
        return project_path

    def test_data_lineage_tracking(self):
        """
        SCENARIO: Track data lineage for compliance

        Story: For audit purposes, must be able to trace data from
        source to sink through all transformations.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                "project": {"name": "lineage-tracking", "version": "1.0.0"},
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
                        "name": "customer_data",
                        "description": "Customer PII data",
                        "topic": "customers.raw.v1",
                        "tags": ["pii", "gdpr"],
                    },
                    {
                        "name": "transaction_data",
                        "description": "Financial transactions",
                        "topic": "transactions.raw.v1",
                        "tags": ["financial", "sox"],
                    },
                ],
                "models": [
                    {
                        "name": "customer_cleaned",
                        "description": "Cleaned customer data",

                        "tags": ["pii", "gdpr"],
                        "sql": "SELECT * FROM {{ source('customer_data') }} WHERE is_valid = true",
                    },
                    {
                        "name": "transaction_cleaned",
                        "description": "Cleaned transaction data",

                        "tags": ["financial", "sox"],
                        "sql": "SELECT * FROM {{ source('transaction_data') }} WHERE status != 'INVALID'",
                    },
                    {
                        "name": "customer_transactions",
                        "description": "Customer transaction history",

                        "tags": ["pii", "financial", "gdpr", "sox"],
                        "sql": """
                            SELECT
                                c.customer_id,
                                c.name,
                                t.transaction_id,
                                t.amount
                            FROM {{ ref("customer_cleaned") }} c
                            JOIN {{ ref("transaction_cleaned") }} t ON c.customer_id = t.customer_id
                        """,
                    },
                    {
                        "name": "customer_summary",
                        "description": "Aggregated customer summary - no PII",

                        "tags": ["aggregated", "non-pii"],
                        "sql": """
                            SELECT
                                customer_id,
                                COUNT(*) as transaction_count,
                                SUM(amount) as total_amount
                            FROM {{ ref("customer_transactions") }}
                            GROUP BY customer_id
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

            # Build DAG and verify lineage
            dag_builder = DAGBuilder(project)
            dag = dag_builder.build()

            # Trace from source to final output
            # customer_data -> customer_cleaned -> customer_transactions -> customer_summary
            assert "customer_data" in dag.get_upstream("customer_cleaned")
            assert "customer_cleaned" in dag.get_upstream("customer_transactions")
            assert "customer_transactions" in dag.get_upstream("customer_summary")

            # Get full lineage path (recursive=True is default)
            full_upstream = dag.get_upstream("customer_summary", recursive=True)
            assert "customer_data" in full_upstream
            assert "transaction_data" in full_upstream
            assert "customer_cleaned" in full_upstream
            assert "transaction_cleaned" in full_upstream
            assert "customer_transactions" in full_upstream

    def test_retention_policy_enforcement(self):
        """
        SCENARIO: Enforce data retention policies

        Story: Different data types have different retention requirements.
        PII must be deleted after 90 days, financial data kept for 7 years.

        This test validates that retention policies are correctly configured
        in governance settings and that topic configurations include appropriate
        retention.ms values. The compiler generates Kafka topic configs with
        retention settings that can be validated against governance policies.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                "project": {"name": "retention-policies", "version": "1.0.0"},
                "runtime": {
                    "kafka": {"bootstrap_servers": "localhost:9092"},
                    "flink": {
                        "default": "local",
                        "clusters": {
                            "local": {"type": "rest", "rest_url": "http://localhost:8082"}
                        },
                    },
                },
                "governance": {
                    "retention_policies": {
                        "pii": {"max_days": 90},
                        "financial": {"min_days": 2555},  # 7 years
                        "default": {"max_days": 365},
                    },
                },
                "sources": [
                    {"name": "events", "topic": "events.raw.v1"},
                ],
                "models": [
                    {
                        "name": "user_pii",
                        "description": "User PII - short retention",

                        "tags": ["pii"],
                        "topic": {
                            "config": {
                                "retention.ms": "7776000000",  # 90 days
                            },
                        },
                        "sql": "SELECT user_id, email, name FROM {{ source('events') }}",
                    },
                    {
                        "name": "financial_records",
                        "description": "Financial records - long retention",

                        "tags": ["financial"],
                        "topic": {
                            "config": {
                                "retention.ms": "-1",  # Infinite (rely on external archival)
                            },
                        },
                        "sql": "SELECT txn_id, amount, timestamp FROM {{ source('events') }}",
                    },
                    {
                        "name": "operational_metrics",
                        "description": "Operational metrics - default retention",

                        "tags": ["operational"],
                        "topic": {
                            "config": {
                                "retention.ms": "31536000000",  # 1 year
                            },
                        },
                        "sql": "SELECT metric_name, value FROM {{ source('events') }}",
                    },
                ],
            }

            project_path = self._create_project(tmpdir, config)
            parser = ProjectParser(project_path)
            project = parser.parse()

            validator = ProjectValidator(project)
            result = validator.validate()
            assert result.is_valid

            # Compile and verify topic configs
            compiler = Compiler(project)
            manifest = compiler.compile()

            # Find topics and verify retention
            topic_artifacts = manifest.artifacts.get("topics", [])
            for topic in topic_artifacts:
                if "pii" in topic["name"].lower():
                    assert topic["config"].get("retention.ms") == "7776000000"
