"""Gap tests for Flink deployer, status command, and cross-cutting concerns.

Groups covered:
  2.  Flink job SQL change detection
  10. Status command desired state verification
  11. Retry logic
  12. Flink session cleanup on error
  13. Status JSON mode error reporting
  15. Primary keys in DDL
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock, Mock, patch

import pytest
import requests

from streamt.compiler.manifest import FlinkJobArtifact
from streamt.deployer.flink import FlinkDeployer, FlinkJobState
from streamt.deployer.gateway import GatewayDeployer
from streamt.deployer.kafka import TopicState
from streamt.deployer.schema_registry import SchemaRegistryDeployer

# ===========================================================================
# GROUP 2: Flink job SQL change detection
# ===========================================================================


class TestFlinkJobSqlChangeDetection:
    """plan_job() should detect when a RUNNING job's SQL has changed."""

    @pytest.fixture
    def deployer(self):
        d = FlinkDeployer.__new__(FlinkDeployer)
        d.rest_url = "http://localhost:8082"
        d.sql_gateway_url = None
        d._http_session = MagicMock()
        return d

    def test_running_job_different_sql_should_plan_update(self, deployer):
        """RUNNING job with different SQL should not be 'none'."""
        pytest.xfail("plan_job doesn't compare SQL -- task #57")

        with patch.object(deployer, "get_job_state") as mock_state:
            mock_state.return_value = FlinkJobState(
                name="my_proc", exists=True, job_id="j-1", status="RUNNING",
            )
            artifact = FlinkJobArtifact(
                name="my_proc", sql="SELECT id, name, email FROM source",
            )
            change = deployer.plan_job(artifact)
            assert change.action != "none"

    def test_running_job_same_sql_is_none(self, deployer):
        """RUNNING job with same SQL should be 'none' (current behavior)."""
        with patch.object(deployer, "get_job_state") as mock_state:
            mock_state.return_value = FlinkJobState(
                name="my_proc", exists=True, job_id="j-1", status="RUNNING",
            )
            artifact = FlinkJobArtifact(name="my_proc", sql="SELECT id FROM source")
            assert deployer.plan_job(artifact).action == "none"

    def test_failed_job_resubmits(self, deployer):
        with patch.object(deployer, "get_job_state") as mock_state:
            mock_state.return_value = FlinkJobState(
                name="my_proc", exists=True, job_id="j-1", status="FAILED",
            )
            artifact = FlinkJobArtifact(name="my_proc", sql="SELECT 1")
            assert deployer.plan_job(artifact).action == "submit"

    def test_nonexistent_job_submits(self, deployer):
        with patch.object(deployer, "get_job_state") as mock_state:
            mock_state.return_value = FlinkJobState(name="new", exists=False)
            artifact = FlinkJobArtifact(name="new", sql="SELECT 1")
            assert deployer.plan_job(artifact).action == "submit"

    def test_cancelled_job_submits(self, deployer):
        with patch.object(deployer, "get_job_state") as mock_state:
            mock_state.return_value = FlinkJobState(
                name="dead", exists=True, job_id="j-1", status="CANCELED",
            )
            artifact = FlinkJobArtifact(name="dead", sql="SELECT 1")
            assert deployer.plan_job(artifact).action == "submit"

    def test_created_job_is_none(self, deployer):
        with patch.object(deployer, "get_job_state") as mock_state:
            mock_state.return_value = FlinkJobState(
                name="starting", exists=True, job_id="j-1", status="CREATED",
            )
            artifact = FlinkJobArtifact(name="starting", sql="SELECT 1")
            assert deployer.plan_job(artifact).action == "none"


# ===========================================================================
# GROUP 10: Status command desired state verification
# ===========================================================================


class TestStatusDesiredState:
    """Status should compare actual vs desired state, not just existence."""

    def test_topic_partition_drift_should_warn(self):
        """Topic has 3 partitions, manifest says 6 -> should show DRIFT."""
        pytest.xfail("status only checks existence -- task #98")

        from click.testing import CliRunner

        from streamt.cli import main

        runner = CliRunner()

        with patch("streamt.cli.commands.status.ProjectParser") as mock_parser, \
             patch("streamt.cli.commands.status.Compiler") as mock_compiler, \
             patch("streamt.cli.commands.status.make_kafka_deployer") as mock_kd, \
             patch("streamt.cli.commands.status.make_sr_deployer", return_value=None), \
             patch("streamt.cli.commands.status.make_flink_deployer", return_value=None), \
             patch("streamt.cli.commands.status.make_connect_deployer", return_value=None), \
             patch("streamt.cli.commands.status.make_gateway_deployer", return_value=None):

            project = MagicMock()
            project.project.name = "test"
            project.runtime.schema_registry = None
            project.runtime.flink = None
            project.runtime.connect = None
            project.runtime.conduktor = None
            mock_parser.return_value.parse.return_value = project

            manifest = MagicMock()
            manifest.artifacts = {
                "topics": [{"name": "events", "partitions": 6, "replication_factor": 1}],
            }
            mock_compiler.return_value.compile.return_value = manifest

            kd = MagicMock()
            kd.get_topic_state.return_value = TopicState(
                name="events", exists=True, partitions=3, replication_factor=1,
            )
            mock_kd.return_value = kd

            result = runner.invoke(main, ["status", "-p", "/tmp/test"])
            assert "DRIFT" in result.output or "mismatch" in result.output.lower()


# ===========================================================================
# GROUP 11: Retry logic
# ===========================================================================


class TestRetryLogic:
    """Deployer HTTP requests should retry transient failures."""

    def test_gateway_no_retry_on_transient_failure(self):
        """Gateway _request fails once then would succeed -- no retry."""
        pytest.xfail("no retry logic in deployers -- task #79")

        deployer = GatewayDeployer.__new__(GatewayDeployer)
        deployer.admin_url = "http://gw:8888"
        deployer.auth = None
        deployer.virtual_cluster = None
        deployer._session = MagicMock()
        deployer._closed = False

        ok = MagicMock()
        ok.status_code = 200
        ok.json.return_value = []
        deployer._session.request.side_effect = [
            requests.ConnectionError("refused"),
            ok,
        ]
        result = deployer._request("GET", "/interceptor")
        assert result == []

    def test_sr_no_retry_on_transient_failure(self):
        """Schema Registry _request fails once then would succeed."""
        pytest.xfail("no retry logic in deployers -- task #79")

        deployer = SchemaRegistryDeployer.__new__(SchemaRegistryDeployer)
        deployer.url = "http://localhost:8081"
        deployer.auth = None
        deployer.headers = {}
        deployer._http_session = MagicMock()

        ok = MagicMock()
        ok.status_code = 200
        ok.json.return_value = ["subject1"]
        ok.raise_for_status = MagicMock()
        deployer._http_session.request.side_effect = [
            requests.ConnectionError("Temporary"),
            ok,
        ]
        assert deployer.list_subjects() == ["subject1"]


# ===========================================================================
# GROUP 12: Flink session cleanup on error
# ===========================================================================


class TestFlinkSessionCleanup:
    """SQL Gateway session should be closed in finally when submit_sql fails."""

    def test_session_closed_after_statement_error(self):
        """2 statements: 1st FINISHED, 2nd ERROR -> session should close."""
        deployer = FlinkDeployer.__new__(FlinkDeployer)
        deployer.rest_url = "http://localhost:8082"
        deployer.sql_gateway_url = "http://localhost:8084"
        deployer.session_id = None
        deployer._http_session = MagicMock()

        responses = [
            Mock(json=Mock(return_value={"sessionHandle": "s1"}), content=b"{}", raise_for_status=Mock()),
            Mock(json=Mock(return_value={"operationHandle": "op1"}), content=b"{}", raise_for_status=Mock()),
            Mock(json=Mock(return_value={"status": "FINISHED"}), content=b"{}", raise_for_status=Mock()),
            Mock(json=Mock(return_value={"operationHandle": "op2"}), content=b"{}", raise_for_status=Mock()),
            Mock(json=Mock(return_value={"status": "ERROR", "error": "bad col"}), content=b"{}", raise_for_status=Mock()),
        ]
        deployer._http_session.request = Mock(side_effect=responses)

        with patch.object(deployer, "close_session") as mock_close, \
             patch("time.sleep"):
            with pytest.raises(RuntimeError):
                deployer.submit_sql("CREATE TABLE t1 (id INT); SELECT bad FROM t1")
            mock_close.assert_called_once()

    def test_session_closed_after_connection_error(self):
        """Session created, then connection lost -> session should close."""
        deployer = FlinkDeployer.__new__(FlinkDeployer)
        deployer.rest_url = "http://localhost:8082"
        deployer.sql_gateway_url = "http://localhost:8084"
        deployer.session_id = None
        deployer._http_session = MagicMock()

        responses = [
            Mock(json=Mock(return_value={"sessionHandle": "s1"}), content=b"{}", raise_for_status=Mock()),
            Mock(raise_for_status=Mock(side_effect=requests.ConnectionError("lost")), content=b"", status_code=0),
        ]
        deployer._http_session.request = Mock(side_effect=responses)

        with patch.object(deployer, "close_session") as mock_close:
            with pytest.raises((RuntimeError, requests.ConnectionError)):
                deployer.submit_sql("SELECT 1")
            mock_close.assert_called_once()


# ===========================================================================
# GROUP 13: Status JSON mode error reporting
# ===========================================================================


class TestStatusJsonErrorReporting:
    """status -o json should include error info when deployers fail."""

    def test_json_mode_includes_connection_error(self, tmp_path):
        from click.testing import CliRunner

        from streamt.cli import main

        runner = CliRunner()

        project = MagicMock()
        project.project.name = "test"
        project.runtime.schema_registry = None
        project.runtime.flink = None
        project.runtime.connect = None
        project.runtime.conduktor = None

        manifest = MagicMock()
        manifest.artifacts = {
            "topics": [{"name": "events", "partitions": 3, "replication_factor": 1}],
        }

        kd = MagicMock()
        kd.get_topic_state.side_effect = Exception("Connection refused to localhost:9092")
        kd.close = MagicMock()

        with patch("streamt.core.parser.ProjectParser") as mock_parser, \
             patch("streamt.compiler.Compiler") as mock_compiler, \
             patch("streamt.cli.commands.status.make_kafka_deployer", return_value=kd), \
             patch("streamt.cli.commands.status.make_sr_deployer", return_value=None), \
             patch("streamt.cli.commands.status.make_flink_deployer", return_value=None), \
             patch("streamt.cli.commands.status.make_connect_deployer", return_value=None), \
             patch("streamt.cli.commands.status.make_gateway_deployer", return_value=None):

            mock_parser.return_value.parse.return_value = project
            mock_compiler.return_value.compile.return_value = manifest

            result = runner.invoke(main, ["-o", "json", "status", "-p", str(tmp_path)])
            output = result.output.strip()
            assert output, f"No output. Exit code: {result.exit_code}, exception: {result.exception}"
            data = json.loads(output)
            assert data["errors"]
            assert data["status"] == "error"


# ===========================================================================
# GROUP 15: Primary keys in DDL
# ===========================================================================


class TestPrimaryKeysInDDL:
    """Models with primary_key should generate PRIMARY KEY in DDL."""

    def test_primary_key_not_supported(self):
        """Model has no primary_key attribute -- documents the gap."""
        pytest.xfail("primary_key not in model or DDL generation -- task #93")

        from streamt.core.models import Model
        model = Model(name="orders_deduped", sql="SELECT order_id, amount FROM orders")
        assert hasattr(model, "primary_key")
