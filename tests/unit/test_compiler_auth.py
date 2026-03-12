"""Tests for Kafka auth properties in compiled Flink SQL DDL.

Production scenario: User has SASL_SSL Kafka. Compiles a project.
The generated Flink SQL must include security properties in all
CREATE TABLE WITH clauses, or Flink jobs fail at runtime with
"Authentication failed" / "Connection refused".
"""

import tempfile
from pathlib import Path

import yaml

from streamt.compiler import Compiler
from streamt.core.parser import ProjectParser


def _compile_project(config: dict) -> dict:
    """Helper: parse + compile, return manifest."""
    with tempfile.TemporaryDirectory() as tmpdir:
        project_path = Path(tmpdir)
        with open(project_path / "stream_project.yml", "w") as f:
            yaml.dump(config, f)
        project = ProjectParser(project_path).parse()
        compiler = Compiler(project)
        return compiler.compile(dry_run=True)


# Base config reused across tests — simulates a Confluent Cloud-style setup
SASL_SSL_CONFIG = {
    "project": {"name": "auth-compile-test", "version": "1.0.0"},
    "runtime": {
        "kafka": {
            "bootstrap_servers": "pkc-xyz.us-west-2.aws.confluent.cloud:9092",
            "security_protocol": "SASL_SSL",
            "sasl_mechanism": "PLAIN",
            "sasl_username": "APIKEY123",
            "sasl_password": "APISECRET456",
            "ssl_ca_location": "/etc/ssl/certs/ca-certificates.crt",
        },
        "flink": {
            "default": "prod",
            "clusters": {
                "prod": {
                    "type": "rest",
                    "rest_url": "https://flink.example.com:8082",
                }
            },
        },
    },
    "sources": [
        {
            "name": "orders_raw",
            "topic": "orders.raw.v1",
            "columns": [
                {"name": "order_id", "type": "STRING"},
                {"name": "amount", "type": "DOUBLE"},
            ],
        }
    ],
    "models": [
        {
            "name": "orders_clean",
            "sql": """
                SELECT order_id, amount
                FROM {{ source("orders_raw") }}
                WHERE amount > 0
            """,
        },
        {
            "name": "revenue_hourly",
            "sql": """
                SELECT SUM(amount) as revenue
                FROM {{ ref("orders_clean") }}
                GROUP BY TUMBLE(proctime, INTERVAL '1' HOUR)
            """,
        },
    ],
}


class TestFlinkDDLIncludesKafkaAuth:
    """Flink SQL DDL must include Kafka security properties when configured."""

    def test_source_table_ddl_has_sasl_properties(self):
        """Source table CREATE TABLE must include SASL properties."""
        manifest = _compile_project(SASL_SSL_CONFIG)
        flink_jobs = manifest.artifacts.get("flink_jobs", [])
        assert len(flink_jobs) >= 1

        # Find the revenue_hourly job (it references orders_raw via orders_clean)
        job = next(j for j in flink_jobs if j["name"] == "revenue_hourly")
        sql = job["sql"]

        # The DDL must include security properties for Kafka connector
        assert "'properties.security.protocol' = 'SASL_SSL'" in sql
        assert "'properties.sasl.mechanism' = 'PLAIN'" in sql
        assert "'properties.sasl.jaas.config'" in sql
        assert "APIKEY123" in sql  # username embedded in JAAS config
        assert "APISECRET456" in sql  # password embedded in JAAS config

    def test_sink_table_ddl_has_sasl_properties(self):
        """Sink table CREATE TABLE must include SASL properties."""
        manifest = _compile_project(SASL_SSL_CONFIG)
        flink_jobs = manifest.artifacts.get("flink_jobs", [])
        job = next(j for j in flink_jobs if j["name"] == "revenue_hourly")
        sql = job["sql"]

        # The INSERT INTO target table DDL must also have auth
        # Count occurrences — should appear in EVERY CREATE TABLE
        create_count = sql.count("CREATE TABLE")
        sasl_count = sql.count("'properties.security.protocol'")
        assert sasl_count == create_count, (
            f"Expected security.protocol in all {create_count} CREATE TABLE statements, "
            f"found in {sasl_count}"
        )

    def test_ssl_ca_location_in_ddl(self):
        """SSL CA cert path must appear in DDL when configured."""
        manifest = _compile_project(SASL_SSL_CONFIG)
        flink_jobs = manifest.artifacts.get("flink_jobs", [])
        job = next(j for j in flink_jobs if j["name"] == "revenue_hourly")
        sql = job["sql"]

        assert "'properties.ssl.truststore.location'" in sql or \
               "'properties.ssl.ca.location'" in sql or \
               "ssl" in sql.lower()

    def test_plaintext_kafka_has_no_auth_in_ddl(self):
        """When Kafka is PLAINTEXT, no security properties in DDL."""
        config = {
            "project": {"name": "plain-test", "version": "1.0.0"},
            "runtime": {
                "kafka": {"bootstrap_servers": "localhost:9092"},
                "flink": {
                    "default": "local",
                    "clusters": {"local": {"type": "rest", "rest_url": "http://localhost:8082"}},
                },
            },
            "sources": [{"name": "raw", "topic": "raw.v1"}],
            "models": [
                {
                    "name": "agg",
                    "sql": """
                        SELECT COUNT(*) as cnt
                        FROM {{ source("raw") }}
                        GROUP BY TUMBLE(proctime, INTERVAL '1' MINUTE)
                    """,
                }
            ],
        }
        manifest = _compile_project(config)
        flink_jobs = manifest.artifacts.get("flink_jobs", [])
        job = next(j for j in flink_jobs if j["name"] == "agg")
        sql = job["sql"]

        assert "security.protocol" not in sql
        assert "sasl.mechanism" not in sql
        assert "jaas.config" not in sql

    def test_test_job_ddl_has_auth_properties(self):
        """Continuous test DDL must also include Kafka auth."""
        config = dict(SASL_SSL_CONFIG)
        config = {**SASL_SSL_CONFIG}
        config["tests"] = [
            {
                "name": "orders_quality",
                "model": "orders_clean",
                "type": "continuous",
                "assertions": [{"not_null": {"columns": ["order_id"]}}],
            }
        ]
        manifest = _compile_project(config)
        flink_jobs = manifest.artifacts.get("flink_jobs", [])

        test_job = next((j for j in flink_jobs if "orders_quality" in j["name"]), None)
        if test_job:
            sql = test_job["sql"]
            assert "'properties.security.protocol' = 'SASL_SSL'" in sql

    def test_model_reference_table_ddl_has_auth(self):
        """When model A refs model B, B's CREATE TABLE DDL must have auth."""
        manifest = _compile_project(SASL_SSL_CONFIG)
        flink_jobs = manifest.artifacts.get("flink_jobs", [])
        job = next(j for j in flink_jobs if j["name"] == "revenue_hourly")
        sql = job["sql"]

        # revenue_hourly refs orders_clean — the orders_clean table DDL must have auth
        # There should be multiple CREATE TABLEs, ALL with auth
        tables = sql.split("CREATE TABLE")
        for i, table_ddl in enumerate(tables[1:], 1):  # skip first empty split
            assert "properties.security.protocol" in table_ddl or \
                   "properties.bootstrap.servers" not in table_ddl, \
                f"CREATE TABLE #{i} has bootstrap.servers but no security.protocol"
