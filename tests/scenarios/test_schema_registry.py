"""Schema Registry test scenarios.

Simulates real-world Schema Registry usage patterns:
- Schema registration with Avro/JSON schemas
- Schema evolution and compatibility
- Multi-source projects with schemas
- Schema auto-generation from columns
"""

import tempfile
from pathlib import Path

import yaml

from streamt.compiler import Compiler
from streamt.core.parser import ProjectParser
from streamt.core.validator import ProjectValidator


class TestSchemaRegistryBasics:
    """Test basic Schema Registry scenarios."""

    def _create_project(self, tmpdir: str, config: dict) -> Path:
        """Helper to create a project."""
        project_path = Path(tmpdir)
        with open(project_path / "stream_project.yml", "w") as f:
            yaml.dump(config, f)
        return project_path

    def test_source_with_avro_schema(self):
        """
        SCENARIO: Source with Avro schema reference

        Story: A payment processing system declares sources with explicit
        Avro schemas registered in Schema Registry. The pipeline should
        compile schema artifacts for registration.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                "project": {
                    "name": "payment-schemas",
                    "version": "1.0.0",
                },
                "runtime": {
                    "kafka": {"bootstrap_servers": "localhost:9092"},
                    "schema_registry": {"url": "http://localhost:8081"},
                },
                "sources": [
                    {
                        "name": "payments",
                        "topic": "payments.raw.v1",
                        "description": "Raw payment events with Avro schema",
                        "schema": {
                            "format": "avro",
                            "subject": "payments.raw.v1-value",
                            "definition": """{
                                "type": "record",
                                "name": "Payment",
                                "namespace": "com.example.payments",
                                "fields": [
                                    {"name": "payment_id", "type": "string"},
                                    {"name": "amount", "type": "double"},
                                    {"name": "currency", "type": "string"},
                                    {"name": "timestamp", "type": "long"}
                                ]
                            }""",
                        },
                    }
                ],
                "models": [
                    {
                        "name": "payments_clean",
                        "sql": "SELECT * FROM {{ source('payments') }}",
                    }
                ],
            }

            project_path = self._create_project(tmpdir, config)
            parser = ProjectParser(project_path)
            project = parser.parse()

            # Validate
            validator = ProjectValidator(project)
            result = validator.validate()
            assert result.is_valid

            # Compile
            compiler = Compiler(project)
            manifest = compiler.compile(dry_run=True)

            # Verify schema artifacts generated
            schemas = manifest.artifacts.get("schemas", [])
            assert len(schemas) == 1
            assert schemas[0]["subject"] == "payments.raw.v1-value"
            assert schemas[0]["schema_type"] == "AVRO"
            assert "Payment" in str(schemas[0]["schema"])

    def test_source_with_json_schema(self):
        """
        SCENARIO: Source with JSON schema

        Story: An IoT platform uses JSON schemas for sensor data.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                "project": {
                    "name": "iot-sensors",
                    "version": "1.0.0",
                },
                "runtime": {
                    "kafka": {"bootstrap_servers": "localhost:9092"},
                    "schema_registry": {"url": "http://localhost:8081"},
                },
                "sources": [
                    {
                        "name": "sensor_readings",
                        "topic": "iot.sensors.v1",
                        "schema": {
                            "format": "json",
                            "subject": "iot.sensors.v1-value",
                            "definition": """{
                                "$schema": "http://json-schema.org/draft-07/schema#",
                                "type": "object",
                                "properties": {
                                    "sensor_id": {"type": "string"},
                                    "value": {"type": "number"},
                                    "unit": {"type": "string"}
                                },
                                "required": ["sensor_id", "value"]
                            }""",
                        },
                    }
                ],
                "models": [
                    {
                        "name": "sensor_readings_clean",

                        "sql": "SELECT * FROM {{ source('sensor_readings') }}",
                    }
                ],
            }

            project_path = self._create_project(tmpdir, config)
            parser = ProjectParser(project_path)
            project = parser.parse()

            compiler = Compiler(project)
            manifest = compiler.compile(dry_run=True)

            schemas = manifest.artifacts.get("schemas", [])
            assert len(schemas) == 1
            assert schemas[0]["schema_type"] == "JSON"

    def test_schema_auto_generation_from_columns(self):
        """
        SCENARIO: Auto-generate schema from column definitions

        Story: A data platform declares sources with column definitions
        but no explicit schema. The compiler should auto-generate an
        Avro schema from the column definitions.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                "project": {
                    "name": "auto-schema",
                    "version": "1.0.0",
                },
                "runtime": {
                    "kafka": {"bootstrap_servers": "localhost:9092"},
                    "schema_registry": {"url": "http://localhost:8081"},
                },
                "sources": [
                    {
                        "name": "orders",
                        "topic": "orders.raw.v1",
                        "description": "Order events",
                        "schema": {
                            "format": "avro",
                            # No definition - should be auto-generated
                        },
                        "columns": [
                            {"name": "order_id", "description": "Unique order identifier"},
                            {"name": "customer_id", "description": "Customer reference"},
                            {"name": "amount", "description": "Order total"},
                            {"name": "status", "description": "Order status"},
                        ],
                    }
                ],
                "models": [
                    {
                        "name": "orders_clean",

                        "sql": "SELECT * FROM {{ source('orders') }}",
                    }
                ],
            }

            project_path = self._create_project(tmpdir, config)
            parser = ProjectParser(project_path)
            project = parser.parse()

            compiler = Compiler(project)
            manifest = compiler.compile(dry_run=True)

            schemas = manifest.artifacts.get("schemas", [])
            assert len(schemas) == 1

            # Schema should have all columns
            schema = schemas[0]["schema"]
            field_names = [f["name"] for f in schema.get("fields", [])]
            assert "order_id" in field_names
            assert "customer_id" in field_names
            assert "amount" in field_names
            assert "status" in field_names

class TestSchemaRegistryMultiSource:
    """Test multi-source Schema Registry scenarios."""

    def _create_project(self, tmpdir: str, config: dict) -> Path:
        project_path = Path(tmpdir)
        with open(project_path / "stream_project.yml", "w") as f:
            yaml.dump(config, f)
        return project_path

    def test_multiple_sources_with_schemas(self):
        """
        SCENARIO: Multiple sources with different schemas

        Story: An e-commerce platform has multiple data sources, each
        with their own schema. All schemas should be compiled.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                "project": {
                    "name": "ecommerce-schemas",
                    "version": "1.0.0",
                },
                "runtime": {
                    "kafka": {"bootstrap_servers": "localhost:9092"},
                    "schema_registry": {"url": "http://localhost:8081"},
                },
                "sources": [
                    {
                        "name": "orders",
                        "topic": "ecom.orders.v1",
                        "schema": {
                            "format": "avro",
                            "subject": "ecom.orders.v1-value",
                            "definition": """{
                                "type": "record",
                                "name": "Order",
                                "fields": [{"name": "order_id", "type": "string"}]
                            }""",
                        },
                    },
                    {
                        "name": "customers",
                        "topic": "ecom.customers.v1",
                        "schema": {
                            "format": "avro",
                            "subject": "ecom.customers.v1-value",
                            "definition": """{
                                "type": "record",
                                "name": "Customer",
                                "fields": [{"name": "customer_id", "type": "string"}]
                            }""",
                        },
                    },
                    {
                        "name": "products",
                        "topic": "ecom.products.v1",
                        "schema": {
                            "format": "avro",
                            "subject": "ecom.products.v1-value",
                            "definition": """{
                                "type": "record",
                                "name": "Product",
                                "fields": [{"name": "product_id", "type": "string"}]
                            }""",
                        },
                    },
                ],
                "models": [
                    {
                        "name": "orders_enriched",

                        "sql": """
                            SELECT o.order_id, c.customer_id
                            FROM {{ source('orders') }} o
                            JOIN {{ source('customers') }} c ON o.customer_id = c.customer_id
                        """,
                    }
                ],
            }

            project_path = self._create_project(tmpdir, config)
            parser = ProjectParser(project_path)
            project = parser.parse()

            compiler = Compiler(project)
            manifest = compiler.compile(dry_run=True)

            schemas = manifest.artifacts.get("schemas", [])
            assert len(schemas) == 3

            subjects = [s["subject"] for s in schemas]
            assert "ecom.orders.v1-value" in subjects
            assert "ecom.customers.v1-value" in subjects
            assert "ecom.products.v1-value" in subjects

    def test_sources_mixed_with_and_without_schemas(self):
        """
        SCENARIO: Some sources have schemas, some don't

        Story: A migration scenario where some sources have explicit
        schemas and others don't yet.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                "project": {
                    "name": "mixed-schemas",
                    "version": "1.0.0",
                },
                "runtime": {
                    "kafka": {"bootstrap_servers": "localhost:9092"},
                    "schema_registry": {"url": "http://localhost:8081"},
                },
                "sources": [
                    {
                        "name": "orders",
                        "topic": "orders.v1",
                        "schema": {
                            "format": "avro",
                            "definition": """{
                                "type": "record",
                                "name": "Order",
                                "fields": [{"name": "id", "type": "string"}]
                            }""",
                        },
                    },
                    {
                        "name": "events",
                        "topic": "events.v1",
                        # No schema defined
                    },
                    {
                        "name": "logs",
                        "topic": "logs.v1",
                        # No schema defined
                    },
                ],
                "models": [
                    {
                        "name": "orders_clean",

                        "sql": "SELECT * FROM {{ source('orders') }}",
                    }
                ],
            }

            project_path = self._create_project(tmpdir, config)
            parser = ProjectParser(project_path)
            project = parser.parse()

            compiler = Compiler(project)
            manifest = compiler.compile(dry_run=True)

            # Only one schema should be generated
            schemas = manifest.artifacts.get("schemas", [])
            assert len(schemas) == 1
            assert "orders" in schemas[0]["subject"]

class TestSchemaRegistryWithGovernance:
    """Test Schema Registry with governance rules."""

    def _create_project(self, tmpdir: str, config: dict) -> Path:
        project_path = Path(tmpdir)
        with open(project_path / "stream_project.yml", "w") as f:
            yaml.dump(config, f)
        return project_path

    def test_source_with_schema_and_classification(self):
        """
        SCENARIO: Source with schema and data classification

        Story: A financial services company requires both schema validation
        and data classification for compliance.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                "project": {
                    "name": "finserv-compliance",
                    "version": "1.0.0",
                },
                "runtime": {
                    "kafka": {"bootstrap_servers": "localhost:9092"},
                    "schema_registry": {"url": "http://localhost:8081"},
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
                        "name": "transactions",
                        "topic": "finserv.transactions.v1",
                        "description": "Financial transactions with PII",
                        "owner": "compliance-team",
                        "schema": {
                            "format": "avro",
                            "subject": "finserv.transactions.v1-value",
                            "definition": """{
                                "type": "record",
                                "name": "Transaction",
                                "namespace": "com.finserv",
                                "fields": [
                                    {"name": "transaction_id", "type": "string"},
                                    {"name": "account_number", "type": "string"},
                                    {"name": "amount", "type": "double"},
                                    {"name": "ssn", "type": ["null", "string"]}
                                ]
                            }""",
                        },
                        "columns": [
                            {"name": "transaction_id", "classification": "internal"},
                            {"name": "account_number", "classification": "confidential"},
                            {"name": "amount", "classification": "confidential"},
                            {"name": "ssn", "classification": "highly_sensitive"},
                        ],
                        "freshness": {"warn_after": "5m", "error_after": "15m"},
                    }
                ],
                "models": [
                    {
                        "name": "transactions_masked",
                        "description": "Transactions with PII masked",
                        "security": {
                            "policies": [
                                {"mask": {"column": "ssn", "method": "redact"}},
                                {"mask": {"column": "account_number", "method": "partial"}},
                            ]
                        },
                        "sql": """
                            SELECT
                                transaction_id,
                                account_number,
                                amount
                            FROM {{ source('transactions') }}
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

            # Verify schema artifact
            schemas = manifest.artifacts.get("schemas", [])
            assert len(schemas) == 1
            assert schemas[0]["subject"] == "finserv.transactions.v1-value"

            # Verify source has classification metadata
            source = manifest.sources[0]
            assert source["owner"] == "compliance-team"
            assert len(source["columns"]) == 4

class TestSchemaRegistrySubjectNaming:
    """Test Schema Registry subject naming conventions."""

    def _create_project(self, tmpdir: str, config: dict) -> Path:
        project_path = Path(tmpdir)
        with open(project_path / "stream_project.yml", "w") as f:
            yaml.dump(config, f)
        return project_path

    def test_default_subject_naming(self):
        """
        SCENARIO: Default subject naming uses topic-value convention

        Story: When no explicit subject is provided, the subject name
        should default to {topic}-value.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                "project": {
                    "name": "subject-naming",
                    "version": "1.0.0",
                },
                "runtime": {
                    "kafka": {"bootstrap_servers": "localhost:9092"},
                    "schema_registry": {"url": "http://localhost:8081"},
                },
                "sources": [
                    {
                        "name": "orders",
                        "topic": "my.orders.topic.v1",
                        "schema": {
                            "format": "avro",
                            # No explicit subject - should default
                            "definition": """{
                                "type": "record",
                                "name": "Order",
                                "fields": [{"name": "id", "type": "string"}]
                            }""",
                        },
                    }
                ],
                "models": [
                    {
                        "name": "orders_clean",

                        "sql": "SELECT * FROM {{ source('orders') }}",
                    }
                ],
            }

            project_path = self._create_project(tmpdir, config)
            parser = ProjectParser(project_path)
            project = parser.parse()

            compiler = Compiler(project)
            manifest = compiler.compile(dry_run=True)

            schemas = manifest.artifacts.get("schemas", [])
            assert len(schemas) == 1
            # Default subject should be topic-value
            assert schemas[0]["subject"] == "my.orders.topic.v1-value"

    def test_explicit_subject_naming(self):
        """
        SCENARIO: Explicit subject overrides default

        Story: When an explicit subject is provided, it should be used
        instead of the default topic-value convention.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                "project": {
                    "name": "custom-subject",
                    "version": "1.0.0",
                },
                "runtime": {
                    "kafka": {"bootstrap_servers": "localhost:9092"},
                    "schema_registry": {"url": "http://localhost:8081"},
                },
                "sources": [
                    {
                        "name": "orders",
                        "topic": "orders.raw.v1",
                        "schema": {
                            "format": "avro",
                            "subject": "custom-orders-schema",  # Custom subject
                            "definition": """{
                                "type": "record",
                                "name": "Order",
                                "fields": [{"name": "id", "type": "string"}]
                            }""",
                        },
                    }
                ],
                "models": [
                    {
                        "name": "orders_clean",

                        "sql": "SELECT * FROM {{ source('orders') }}",
                    }
                ],
            }

            project_path = self._create_project(tmpdir, config)
            parser = ProjectParser(project_path)
            project = parser.parse()

            compiler = Compiler(project)
            manifest = compiler.compile(dry_run=True)

            schemas = manifest.artifacts.get("schemas", [])
            assert len(schemas) == 1
            assert schemas[0]["subject"] == "custom-orders-schema"
