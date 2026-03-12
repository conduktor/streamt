"""Round-trip test: YAML project with SASL auth → compile → DDL contains auth properties.

This test exercises the full pipeline without needing running services:
1. Create a project config with SASL_PLAINTEXT Kafka
2. Parse it
3. Compile it
4. Verify the Flink SQL DDL contains SASL properties and JAAS config
"""

import textwrap
from pathlib import Path

import yaml


def _write_project(tmp_path: Path) -> Path:
    """Create a minimal streamt project with SASL auth config."""
    project_dir = tmp_path / "auth_project"
    project_dir.mkdir()

    project_yaml = {
        "project": {"name": "auth_test"},
        "runtime": {
            "kafka": {
                "bootstrap_servers": "kafka.prod:9094",
                "security_protocol": "SASL_PLAINTEXT",
                "sasl_mechanism": "PLAIN",
                "sasl_username": "myuser",
                "sasl_password": "mypassword",
            },
        },
        "sources": [
            {
                "name": "raw_events",
                "topic": "events.raw.v1",
                "columns": [
                    {"name": "id", "type": "STRING"},
                    {"name": "value", "type": "INT"},
                ],
            },
        ],
        "models": [
            {
                "name": "filtered_events",
                "materialized": "flink",
                "sql": "SELECT id, value FROM {{ source('raw_events') }} WHERE value > 0",
            },
        ],
    }

    (project_dir / "stream_project.yml").write_text(yaml.dump(project_yaml))
    return project_dir


class TestAuthRoundTrip:
    """YAML → parse → compile → DDL contains auth."""

    def test_compiled_ddl_contains_sasl_properties(self, tmp_path):
        """Flink DDL WITH clause includes security.protocol and sasl.mechanism."""
        from streamt.compiler import Compiler
        from streamt.core.parser import ProjectParser

        project_dir = _write_project(tmp_path)
        parser = ProjectParser(project_dir)
        project = parser.parse()
        compiler = Compiler(project)
        manifest = compiler.compile(dry_run=True)

        # Find the source table DDL in flink_jobs
        flink_jobs = manifest.artifacts.get("flink_jobs", [])
        assert len(flink_jobs) > 0, "Expected at least one Flink job"

        # The DDL should be in the job SQL
        job_sql = flink_jobs[0].get("sql", "")
        assert "SASL_PLAINTEXT" in job_sql, f"DDL missing security.protocol. SQL:\n{job_sql}"
        assert "sasl.mechanism" in job_sql, f"DDL missing sasl.mechanism. SQL:\n{job_sql}"
        assert "sasl.jaas.config" in job_sql, f"DDL missing sasl.jaas.config. SQL:\n{job_sql}"

    def test_compiled_ddl_contains_jaas_credentials(self, tmp_path):
        """JAAS config string contains username and password."""
        from streamt.compiler import Compiler
        from streamt.core.parser import ProjectParser

        project_dir = _write_project(tmp_path)
        parser = ProjectParser(project_dir)
        project = parser.parse()
        compiler = Compiler(project)
        manifest = compiler.compile(dry_run=True)

        job_sql = manifest.artifacts["flink_jobs"][0]["sql"]
        assert 'username="myuser"' in job_sql, "JAAS missing username"
        assert 'password="mypassword"' in job_sql, "JAAS missing password"

    def test_compiled_ddl_contains_bootstrap_servers(self, tmp_path):
        """DDL WITH clause includes the correct bootstrap.servers."""
        from streamt.compiler import Compiler
        from streamt.core.parser import ProjectParser

        project_dir = _write_project(tmp_path)
        parser = ProjectParser(project_dir)
        project = parser.parse()
        compiler = Compiler(project)
        manifest = compiler.compile(dry_run=True)

        job_sql = manifest.artifacts["flink_jobs"][0]["sql"]
        assert "kafka.prod:9094" in job_sql, "DDL missing bootstrap.servers"

    def test_validate_passes_with_sasl_config(self, tmp_path):
        """Project with SASL + Flink config passes validation."""
        from streamt.core.parser import ProjectParser
        from streamt.core.validator import ProjectValidator

        project_dir = tmp_path / "valid_project"
        project_dir.mkdir()
        project_yaml = {
            "project": {"name": "valid_test"},
            "runtime": {
                "kafka": {
                    "bootstrap_servers": "kafka:9094",
                    "security_protocol": "SASL_PLAINTEXT",
                    "sasl_mechanism": "PLAIN",
                    "sasl_username": "user",
                    "sasl_password": "pass",
                },
                "flink": {
                    "default": "local",
                    "clusters": {
                        "local": {"rest_url": "http://localhost:8081"},
                    },
                },
            },
            "sources": [
                {"name": "src", "topic": "t.v1", "columns": [{"name": "id", "type": "STRING"}]},
            ],
            "models": [
                {"name": "m", "materialized": "flink", "sql": "SELECT id FROM {{ source('src') }}"},
            ],
        }
        (project_dir / "stream_project.yml").write_text(yaml.dump(project_yaml))

        parser = ProjectParser(project_dir)
        project = parser.parse()
        validator = ProjectValidator(project)
        result = validator.validate()
        assert result.is_valid, f"Validation failed: {[e.message for e in result.errors]}"

    def test_kafka_config_roundtrip_to_confluent(self):
        """KafkaConfig with SASL → to_confluent_config preserves all auth fields."""
        from streamt.core.models import KafkaConfig

        cfg = KafkaConfig(
            bootstrap_servers="kafka:9094",
            security_protocol="SASL_SSL",
            sasl_mechanism="SCRAM-SHA-256",
            sasl_username="user",
            sasl_password="pass",
            ssl_ca_location="/ca.pem",
        )
        confluent = cfg.to_confluent_config()
        assert confluent["bootstrap.servers"] == "kafka:9094"
        assert confluent["security.protocol"] == "SASL_SSL"
        assert confluent["sasl.mechanism"] == "SCRAM-SHA-256"
        assert confluent["sasl.username"] == "user"
        assert confluent["sasl.password"] == "pass"
        assert confluent["ssl.ca.location"] == "/ca.pem"

    def test_no_auth_ddl_has_no_sasl(self, tmp_path):
        """Project without auth config produces DDL without SASL properties."""
        from streamt.compiler import Compiler
        from streamt.core.parser import ProjectParser

        project_dir = tmp_path / "plain_project"
        project_dir.mkdir()
        project_yaml = {
            "project": {"name": "plain_test"},
            "runtime": {
                "kafka": {"bootstrap_servers": "kafka:9092"},
            },
            "sources": [
                {
                    "name": "raw",
                    "topic": "raw.v1",
                    "columns": [{"name": "id", "type": "STRING"}],
                },
            ],
            "models": [
                {
                    "name": "clean",
                    "materialized": "flink",
                    "sql": "SELECT id FROM {{ source('raw') }}",
                },
            ],
        }
        (project_dir / "stream_project.yml").write_text(yaml.dump(project_yaml))

        parser = ProjectParser(project_dir)
        project = parser.parse()
        compiler = Compiler(project)
        manifest = compiler.compile(dry_run=True)

        job_sql = manifest.artifacts["flink_jobs"][0]["sql"]
        assert "sasl" not in job_sql.lower(), f"Plain DDL should not have SASL. SQL:\n{job_sql}"
