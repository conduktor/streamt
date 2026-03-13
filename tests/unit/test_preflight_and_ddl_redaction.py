"""Tests for BUG-011 (pre-flight connectivity) and BUG-021 (DDL credential redaction)."""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

# ============================================================
# BUG-011: Pre-flight connectivity check
# ============================================================


class TestPreflightConnectivityPlan:
    """plan command should fail early when a configured deployer is unavailable."""

    def test_plan_fails_when_kafka_unavailable(self, tmp_path):
        from click.testing import CliRunner

        from streamt.cli import main

        runner = CliRunner()
        project = MagicMock()
        project.project.name = "test"
        project.runtime.kafka = MagicMock()  # kafka is configured
        project.runtime.schema_registry = None
        project.runtime.flink = None
        project.runtime.connect = None
        project.runtime.conduktor = None

        manifest = MagicMock()
        manifest.artifacts = {}

        with (
            patch("streamt.core.parser.ProjectParser") as mock_parser,
            patch("streamt.compiler.Compiler") as mock_compiler,
            patch("streamt.cli.commands.plan.make_kafka_deployer", return_value=None),
            patch("streamt.cli.commands.plan.make_sr_deployer", return_value=None),
            patch("streamt.cli.commands.plan.make_flink_deployer", return_value=None),
            patch("streamt.cli.commands.plan.make_connect_deployer", return_value=None),
            patch("streamt.cli.commands.plan.make_gateway_deployer", return_value=None),
        ):
            mock_parser.return_value.parse.return_value = project
            mock_compiler.return_value.compile.return_value = manifest

            result = runner.invoke(main, ["plan", "-p", str(tmp_path)])
            assert result.exit_code != 0

    def test_plan_succeeds_when_optional_sr_unavailable(self, tmp_path):
        """SR deployer is optional — plan should proceed if kafka is up."""
        from click.testing import CliRunner

        from streamt.cli import main
        from streamt.deployer.planner import DeploymentPlan

        runner = CliRunner()
        project = MagicMock()
        project.project.name = "test"
        project.runtime.kafka = MagicMock()
        project.runtime.schema_registry = None  # not configured
        project.runtime.flink = None
        project.runtime.connect = None
        project.runtime.conduktor = None

        manifest = MagicMock()
        manifest.artifacts = {}

        mock_kafka = MagicMock()
        empty_plan = DeploymentPlan()

        with (
            patch("streamt.core.parser.ProjectParser") as mock_parser,
            patch("streamt.compiler.Compiler") as mock_compiler,
            patch("streamt.cli.commands.plan.make_kafka_deployer", return_value=mock_kafka),
            patch("streamt.cli.commands.plan.make_sr_deployer", return_value=None),
            patch("streamt.cli.commands.plan.make_flink_deployer", return_value=None),
            patch("streamt.cli.commands.plan.make_connect_deployer", return_value=None),
            patch("streamt.cli.commands.plan.make_gateway_deployer", return_value=None),
            patch("streamt.deployer.planner.DeploymentPlanner.plan", return_value=empty_plan),
        ):
            mock_parser.return_value.parse.return_value = project
            mock_compiler.return_value.compile.return_value = manifest

            result = runner.invoke(main, ["plan", "-p", str(tmp_path)])
            assert result.exit_code == 0


class TestPreflightConnectivityApply:
    """apply command should fail early when a configured deployer is unavailable."""

    def test_apply_fails_when_kafka_unavailable(self, tmp_path):
        from click.testing import CliRunner

        from streamt.cli import main

        runner = CliRunner()
        project = MagicMock()
        project.project.name = "test"
        project.runtime.kafka = MagicMock()
        project.runtime.schema_registry = None
        project.runtime.flink = None
        project.runtime.connect = None
        project.runtime.conduktor = None

        manifest = MagicMock()
        manifest.artifacts = {}

        with (
            patch("streamt.core.parser.ProjectParser") as mock_parser,
            patch("streamt.compiler.Compiler") as mock_compiler,
            patch("streamt.cli.commands.apply.make_kafka_deployer", return_value=None),
            patch("streamt.cli.commands.apply.make_sr_deployer", return_value=None),
            patch("streamt.cli.commands.apply.make_flink_deployer", return_value=None),
            patch("streamt.cli.commands.apply.make_connect_deployer", return_value=None),
            patch("streamt.cli.commands.apply.make_gateway_deployer", return_value=None),
        ):
            mock_parser.return_value.parse.return_value = project
            mock_compiler.return_value.compile.return_value = manifest

            result = runner.invoke(main, ["apply", "-p", str(tmp_path)])
            assert result.exit_code != 0


class TestCheckRequiredDeployers:
    """Unit tests for the check_required_deployers helper."""

    def _make_project(
        self, has_kafka=True, has_sr=False, has_flink=False, has_connect=False, has_gateway=False
    ):
        project = MagicMock()
        project.runtime.kafka = MagicMock() if has_kafka else None
        project.runtime.schema_registry = MagicMock() if has_sr else None
        project.runtime.flink = MagicMock() if has_flink else None
        if has_flink:
            project.runtime.flink.clusters = {"default": MagicMock()}
        project.runtime.connect = MagicMock() if has_connect else None
        if has_connect:
            project.runtime.connect.clusters = {"default": MagicMock()}
        project.runtime.conduktor = MagicMock() if has_gateway else None
        if has_gateway:
            project.runtime.conduktor.gateway = MagicMock()
        return project

    def test_kafka_required_returns_false_when_none(self):
        from streamt.cli.helpers import check_required_deployers
        from streamt.output import OutputFormatter

        fmt = OutputFormatter("text")
        project = self._make_project(has_kafka=True)
        result = check_required_deployers(project, None, None, None, None, None, fmt)
        assert result is False

    def test_kafka_ok_returns_true(self):
        from streamt.cli.helpers import check_required_deployers
        from streamt.output import OutputFormatter

        fmt = OutputFormatter("text")
        project = self._make_project(has_kafka=True)
        result = check_required_deployers(project, MagicMock(), None, None, None, None, fmt)
        assert result is True

    def test_sr_configured_but_none_returns_false(self):
        from streamt.cli.helpers import check_required_deployers
        from streamt.output import OutputFormatter

        fmt = OutputFormatter("text")
        project = self._make_project(has_kafka=True, has_sr=True)
        result = check_required_deployers(project, MagicMock(), None, None, None, None, fmt)
        assert result is False

    def test_sr_not_configured_and_none_returns_true(self):
        from streamt.cli.helpers import check_required_deployers
        from streamt.output import OutputFormatter

        fmt = OutputFormatter("text")
        project = self._make_project(has_kafka=True, has_sr=False)
        result = check_required_deployers(project, MagicMock(), None, None, None, None, fmt)
        assert result is True

    def test_flink_configured_but_none_returns_false(self):
        from streamt.cli.helpers import check_required_deployers
        from streamt.output import OutputFormatter

        fmt = OutputFormatter("text")
        project = self._make_project(has_kafka=True, has_flink=True)
        result = check_required_deployers(project, MagicMock(), None, None, None, None, fmt)
        assert result is False


# ============================================================
# BUG-021: DDL credential redaction in manifest
# ============================================================


class TestDDLCredentialRedaction:
    """Credentials in Flink DDL SQL must be redacted when saved to disk."""

    def test_sasl_password_redacted_on_save(self, tmp_path):
        from streamt.compiler.manifest import Manifest

        sql_with_creds = (
            "CREATE TABLE t (id INT) WITH (\n"
            "    'connector' = 'kafka',\n"
            "    'properties.sasl.jaas.config' = "
            "'org.apache.kafka.common.security.plain.PlainLoginModule required "
            'username="admin" password="s3cr3t";\'\n'
            ")"
        )
        manifest = Manifest(version="1.0.0", project_name="test")
        manifest.artifacts = {"flink_jobs": [{"name": "job1", "sql": sql_with_creds}]}

        path = tmp_path / "manifest.json"
        manifest.save(path)

        saved = json.loads(path.read_text())
        saved_sql = saved["artifacts"]["flink_jobs"][0]["sql"]
        assert "s3cr3t" not in saved_sql
        assert "admin" not in saved_sql

    def test_ssl_key_password_redacted_on_save(self, tmp_path):
        from streamt.compiler.manifest import Manifest

        sql_with_creds = (
            "CREATE TABLE t (id INT) WITH (\n"
            "    'connector' = 'kafka',\n"
            "    'properties.ssl.key.password' = 'my-key-pass'\n"
            ")"
        )
        manifest = Manifest(version="1.0.0", project_name="test")
        manifest.artifacts = {"flink_jobs": [{"name": "job1", "sql": sql_with_creds}]}

        path = tmp_path / "manifest.json"
        manifest.save(path)

        saved = json.loads(path.read_text())
        saved_sql = saved["artifacts"]["flink_jobs"][0]["sql"]
        assert "my-key-pass" not in saved_sql

    def test_in_memory_sql_unchanged_after_save(self, tmp_path):
        """save() must NOT mutate the in-memory manifest (keeps credentials for deployment)."""
        from streamt.compiler.manifest import Manifest

        original_sql = (
            "CREATE TABLE t (id INT) WITH (\n"
            "    'properties.sasl.jaas.config' = "
            '\'PlainLoginModule required username="u" password="p";\'\n'
            ")"
        )
        manifest = Manifest(version="1.0.0", project_name="test")
        manifest.artifacts = {"flink_jobs": [{"name": "job1", "sql": original_sql}]}

        path = tmp_path / "manifest.json"
        manifest.save(path)

        # In-memory SQL must be intact
        assert manifest.artifacts["flink_jobs"][0]["sql"] == original_sql

    def test_non_sensitive_sql_unchanged(self, tmp_path):
        """SQL without credentials should be saved verbatim."""
        from streamt.compiler.manifest import Manifest

        clean_sql = (
            "CREATE TABLE t (id INT) WITH (\n    'connector' = 'kafka',\n    'topic' = 'events'\n)"
        )
        manifest = Manifest(version="1.0.0", project_name="test")
        manifest.artifacts = {"flink_jobs": [{"name": "job1", "sql": clean_sql}]}

        path = tmp_path / "manifest.json"
        manifest.save(path)

        saved = json.loads(path.read_text())
        saved_sql = saved["artifacts"]["flink_jobs"][0]["sql"]
        assert saved_sql == clean_sql

    def test_redact_ddl_credentials_function(self):
        """Unit test for the redact_ddl_credentials helper."""
        from streamt.compiler.flink_ddl import redact_ddl_credentials

        sql = (
            "'properties.sasl.jaas.config' = 'PlainLoginModule required "
            'username="u" password="secret";\'\n'
            "'properties.ssl.key.password' = 'keypass'"
        )
        result = redact_ddl_credentials(sql)
        assert "secret" not in result
        assert "keypass" not in result
        assert "'properties.sasl.jaas.config'" in result
        assert "'properties.ssl.key.password'" in result
