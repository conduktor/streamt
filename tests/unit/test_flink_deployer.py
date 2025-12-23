"""Unit tests for FlinkDeployer.

Tests async operation handling, error surfacing, and API compatibility.
"""

from unittest.mock import Mock, patch

import pytest
import requests

from streamt.deployer.flink import FlinkDeployer


class TestFlinkDeployerInit:
    """Tests for FlinkDeployer initialization."""

    def test_init_with_both_urls(self):
        """Should accept both rest_url and sql_gateway_url."""
        deployer = FlinkDeployer(
            rest_url="http://localhost:8082",
            sql_gateway_url="http://localhost:8084",
        )
        assert deployer.rest_url == "http://localhost:8082"
        assert deployer.sql_gateway_url == "http://localhost:8084"
        deployer.close()

    def test_init_with_only_rest_url(self):
        """Should work with only rest_url (sql_gateway_url is None)."""
        deployer = FlinkDeployer(rest_url="http://localhost:8082")
        assert deployer.rest_url == "http://localhost:8082"
        assert deployer.sql_gateway_url is None
        deployer.close()

    def test_init_strips_trailing_slashes(self):
        """Should strip trailing slashes from URLs."""
        deployer = FlinkDeployer(
            rest_url="http://localhost:8082/",
            sql_gateway_url="http://localhost:8084/",
        )
        assert deployer.rest_url == "http://localhost:8082"
        assert deployer.sql_gateway_url == "http://localhost:8084"
        deployer.close()


class TestFlinkDeployerContextManager:
    """Tests for context manager protocol."""

    def test_context_manager_enters_and_exits(self):
        """Should support context manager protocol."""
        with FlinkDeployer(rest_url="http://localhost:8082") as deployer:
            assert deployer is not None
            assert deployer.rest_url == "http://localhost:8082"

    def test_close_clears_session_id(self):
        """Should clear session_id on close."""
        deployer = FlinkDeployer(
            rest_url="http://localhost:8082",
            sql_gateway_url="http://localhost:8084",
        )
        deployer.session_id = "test-session"
        deployer.close()
        assert deployer.session_id is None

    def test_close_session_handles_no_session(self):
        """Should handle close_session when no session exists."""
        deployer = FlinkDeployer(rest_url="http://localhost:8082")
        deployer.close_session()  # Should not raise
        deployer.close()


class TestFlinkDeployerSqlGateway:
    """Tests for SQL Gateway operations."""

    def test_get_session_requires_sql_gateway_url(self):
        """Should raise error if sql_gateway_url not configured."""
        deployer = FlinkDeployer(rest_url="http://localhost:8082")

        with pytest.raises(ValueError) as exc_info:
            deployer.get_session()

        assert "SQL Gateway URL not configured" in str(exc_info.value)
        deployer.close()

    def test_get_session_uses_session_handle(self):
        """Should extract sessionHandle from Flink 1.18+ response."""
        deployer = FlinkDeployer(
            rest_url="http://localhost:8082",
            sql_gateway_url="http://localhost:8084",
        )

        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.content = b'{"sessionHandle": "abc-123-session"}'
        mock_response.json.return_value = {"sessionHandle": "abc-123-session"}
        mock_response.raise_for_status = Mock()

        deployer._http_session.request = Mock(return_value=mock_response)
        session_id = deployer.get_session()

        assert session_id == "abc-123-session"
        deployer._http_session.request.assert_called_once()
        call_args = deployer._http_session.request.call_args
        assert "http://localhost:8084/v1/sessions" in call_args[0][1]
        deployer.close()

    def test_get_session_sends_empty_body(self):
        """Should send empty JSON body (Flink 1.18+ format)."""
        deployer = FlinkDeployer(
            rest_url="http://localhost:8082",
            sql_gateway_url="http://localhost:8084",
        )

        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.content = b'{"sessionHandle": "xyz"}'
        mock_response.json.return_value = {"sessionHandle": "xyz"}
        mock_response.raise_for_status = Mock()

        deployer._http_session.request = Mock(return_value=mock_response)
        deployer.get_session()

        # Verify the request body is empty
        call_args = deployer._http_session.request.call_args
        json_body = call_args[1].get("json", {})
        assert json_body == {}
        deployer.close()


class TestFlinkDeployerSubmitSql:
    """Tests for SQL submission with async operation handling."""

    def test_submit_sql_polls_for_completion(self):
        """Should poll operation status until FINISHED."""
        deployer = FlinkDeployer(
            rest_url="http://localhost:8082",
            sql_gateway_url="http://localhost:8084",
        )

        responses = [
            Mock(json=Mock(return_value={"sessionHandle": "s1"}), content=b'{}', raise_for_status=Mock()),
            Mock(json=Mock(return_value={"operationHandle": "op1"}), content=b'{}', raise_for_status=Mock()),
            Mock(json=Mock(return_value={"status": "RUNNING"}), content=b'{}', raise_for_status=Mock()),
            Mock(json=Mock(return_value={"status": "FINISHED"}), content=b'{}', raise_for_status=Mock()),
        ]
        deployer._http_session.request = Mock(side_effect=responses)

        with patch("time.sleep"):
            result = deployer.submit_sql("CREATE TABLE test (id INT)")

        assert len(result["results"]) == 1
        assert result["results"][0]["status"] == "FINISHED"
        deployer.close()

    def test_submit_sql_raises_on_error_status(self):
        """Should raise RuntimeError when operation status is ERROR."""
        deployer = FlinkDeployer(
            rest_url="http://localhost:8082",
            sql_gateway_url="http://localhost:8084",
        )

        responses = [
            Mock(json=Mock(return_value={"sessionHandle": "s1"}), content=b'{}', raise_for_status=Mock()),
            Mock(json=Mock(return_value={"operationHandle": "op1"}), content=b'{}', raise_for_status=Mock()),
            Mock(json=Mock(return_value={"status": "ERROR", "error": "Column not found"}), content=b'{}', raise_for_status=Mock()),
        ]
        deployer._http_session.request = Mock(side_effect=responses)

        with pytest.raises(RuntimeError) as exc_info:
            with patch("time.sleep"):
                deployer.submit_sql("SELECT foo FROM bar")

        assert "Flink SQL execution error" in str(exc_info.value)
        deployer.close()

    def test_submit_sql_raises_on_missing_operation_handle(self):
        """Should raise RuntimeError when no operationHandle returned."""
        deployer = FlinkDeployer(
            rest_url="http://localhost:8082",
            sql_gateway_url="http://localhost:8084",
        )

        responses = [
            Mock(json=Mock(return_value={"sessionHandle": "s1"}), content=b'{}', raise_for_status=Mock()),
            Mock(json=Mock(return_value={}), content=b'{}', raise_for_status=Mock()),
        ]
        deployer._http_session.request = Mock(side_effect=responses)

        with pytest.raises(RuntimeError) as exc_info:
            deployer.submit_sql("SELECT 1")

        assert "No operationHandle" in str(exc_info.value)
        deployer.close()

    def test_submit_sql_handles_multiple_statements(self):
        """Should execute multiple statements sequentially."""
        deployer = FlinkDeployer(
            rest_url="http://localhost:8082",
            sql_gateway_url="http://localhost:8084",
        )

        responses = [
            Mock(json=Mock(return_value={"sessionHandle": "s1"}), content=b'{}', raise_for_status=Mock()),
            Mock(json=Mock(return_value={"operationHandle": "op1"}), content=b'{}', raise_for_status=Mock()),
            Mock(json=Mock(return_value={"status": "FINISHED"}), content=b'{}', raise_for_status=Mock()),
            Mock(json=Mock(return_value={"operationHandle": "op2"}), content=b'{}', raise_for_status=Mock()),
            Mock(json=Mock(return_value={"status": "FINISHED"}), content=b'{}', raise_for_status=Mock()),
        ]
        deployer._http_session.request = Mock(side_effect=responses)

        with patch("time.sleep"):
            result = deployer.submit_sql("CREATE TABLE t1 (id INT); CREATE TABLE t2 (id INT)")

        assert len(result["results"]) == 2
        deployer.close()


class TestFlinkDeployerRestApi:
    """Tests for REST API operations (job management)."""

    def test_list_jobs_uses_rest_url(self):
        """Should use rest_url (not sql_gateway_url) for /jobs endpoint."""
        deployer = FlinkDeployer(
            rest_url="http://localhost:8082",
            sql_gateway_url="http://localhost:8084",
        )

        mock_response = Mock()
        mock_response.content = b'{"jobs": [{"id": "j1", "status": "RUNNING"}]}'
        mock_response.json.return_value = {"jobs": [{"id": "j1", "status": "RUNNING"}]}
        mock_response.raise_for_status = Mock()

        deployer._http_session.request = Mock(return_value=mock_response)
        jobs = deployer.list_jobs()

        assert len(jobs) == 1
        assert jobs[0]["id"] == "j1"

        # Verify REST URL was used
        call_args = deployer._http_session.request.call_args
        url = call_args[0][1]
        assert "8082" in url
        assert "8084" not in url
        deployer.close()

    def test_get_job_state_returns_not_exists(self):
        """Should return exists=False when job not found."""
        deployer = FlinkDeployer(rest_url="http://localhost:8082")

        mock_response = Mock()
        mock_response.content = b'{"jobs": []}'
        mock_response.json.return_value = {"jobs": []}
        mock_response.raise_for_status = Mock()

        deployer._http_session.request = Mock(return_value=mock_response)
        state = deployer.get_job_state("nonexistent-job")

        assert state.exists is False
        assert state.name == "nonexistent-job"
        deployer.close()

    def test_check_connection_returns_true(self):
        """Should return True when cluster is accessible."""
        deployer = FlinkDeployer(rest_url="http://localhost:8082")

        mock_response = Mock()
        mock_response.content = b'{"flink-version": "1.18.0"}'
        mock_response.json.return_value = {"flink-version": "1.18.0"}
        mock_response.raise_for_status = Mock()

        deployer._http_session.request = Mock(return_value=mock_response)
        assert deployer.check_connection() is True
        deployer.close()

    def test_check_connection_returns_false_on_error(self):
        """Should return False when cluster is not accessible."""
        deployer = FlinkDeployer(rest_url="http://localhost:8082")
        deployer._http_session.request = Mock(side_effect=requests.exceptions.ConnectionError())
        assert deployer.check_connection() is False
        deployer.close()


class TestFlinkDeployerJobNameMatching:
    """Tests for job name matching logic."""

    def test_matches_flink_insert_job_pattern(self):
        """Should match Flink's auto-generated INSERT job names."""
        deployer = FlinkDeployer(rest_url="http://localhost:8082")

        responses = [
            Mock(json=Mock(return_value={"jobs": [{"id": "j1", "status": "RUNNING"}]}), content=b'{}', raise_for_status=Mock()),
            Mock(json=Mock(return_value={"jid": "j1", "name": "insert-into_default_catalog.default_database.events_clean_sink", "state": "RUNNING"}), content=b'{}', raise_for_status=Mock()),
        ]
        deployer._http_session.request = Mock(side_effect=responses)

        state = deployer.get_job_state("events_clean_processor")

        assert state.exists is True
        assert state.status == "RUNNING"
        deployer.close()

    def test_strips_processor_suffix_for_matching(self):
        """Should strip _processor suffix when matching sink names."""
        deployer = FlinkDeployer(rest_url="http://localhost:8082")

        responses = [
            Mock(json=Mock(return_value={"jobs": [{"id": "j1", "status": "RUNNING"}]}), content=b'{}', raise_for_status=Mock()),
            Mock(json=Mock(return_value={"jid": "j1", "name": "insert-into_catalog.db.my_model_sink", "state": "RUNNING"}), content=b'{}', raise_for_status=Mock()),
        ]
        deployer._http_session.request = Mock(side_effect=responses)

        state = deployer.get_job_state("my_model_processor")

        assert state.exists is True
        assert state.name == "my_model_processor"
        deployer.close()


class TestFlinkDeployerJobNameCollision:
    """Tests for job name collision between models and tests."""

    def test_model_named_test_something_uses_processor_suffix(self):
        """Model named test_orders should match test_orders_processor_sink, not test_failures_orders."""
        deployer = FlinkDeployer(rest_url="http://localhost:8082")

        responses = [
            Mock(json=Mock(return_value={"jobs": [{"id": "j1", "status": "RUNNING"}]}), content=b'{}', raise_for_status=Mock()),
            Mock(json=Mock(return_value={"jid": "j1", "name": "insert-into_catalog.db.test_orders_sink", "state": "RUNNING"}), content=b'{}', raise_for_status=Mock()),
        ]
        deployer._http_session.request = Mock(side_effect=responses)

        state = deployer.get_job_state("test_orders_processor")
        assert state.exists is True
        deployer.close()

    def test_actual_test_job_matches_failures_pattern(self):
        """Test job test_my_check should match test_failures_my_check."""
        deployer = FlinkDeployer(rest_url="http://localhost:8082")

        responses = [
            Mock(json=Mock(return_value={"jobs": [{"id": "j1", "status": "RUNNING"}]}), content=b'{}', raise_for_status=Mock()),
            Mock(json=Mock(return_value={"jid": "j1", "name": "insert-into_catalog.db.test_failures_my_check", "state": "RUNNING"}), content=b'{}', raise_for_status=Mock()),
        ]
        deployer._http_session.request = Mock(side_effect=responses)

        state = deployer.get_job_state("test_my_check")
        assert state.exists is True
        deployer.close()


class TestSqlStatementSplitting:
    """Tests for SQL statement splitting."""

    def test_split_simple_statements(self):
        """Should split simple semicolon-separated statements."""
        from streamt.deployer.flink import _split_sql_statements

        sql = "CREATE TABLE t1 (id INT); CREATE TABLE t2 (id INT)"
        statements = _split_sql_statements(sql)

        assert len(statements) == 2
        assert "CREATE TABLE t1" in statements[0]
        assert "CREATE TABLE t2" in statements[1]

    def test_split_preserves_semicolon_in_string(self):
        """Should not split on semicolons inside string literals."""
        from streamt.deployer.flink import _split_sql_statements

        sql = "INSERT INTO t1 VALUES ('hello; world'); INSERT INTO t2 VALUES ('test')"
        statements = _split_sql_statements(sql)

        assert len(statements) == 2
        assert "hello; world" in statements[0]
        assert "test" in statements[1]

    def test_split_handles_escaped_quotes(self):
        """Should handle escaped quotes in strings."""
        from streamt.deployer.flink import _split_sql_statements

        sql = "INSERT INTO t1 VALUES ('it\\'s a test'); SELECT 1"
        statements = _split_sql_statements(sql)

        assert len(statements) == 2

    def test_split_handles_no_trailing_semicolon(self):
        """Should handle statements without trailing semicolon."""
        from streamt.deployer.flink import _split_sql_statements

        sql = "SELECT 1; SELECT 2"
        statements = _split_sql_statements(sql)

        assert len(statements) == 2

    def test_split_ignores_empty_statements(self):
        """Should ignore empty statements from multiple semicolons."""
        from streamt.deployer.flink import _split_sql_statements

        sql = "SELECT 1;; SELECT 2"
        statements = _split_sql_statements(sql)

        assert len(statements) == 2


class TestFlinkDeployerHttpErrors:
    """Tests for HTTP error handling."""

    def test_handles_500_server_error(self):
        """Should raise on 500 server error."""
        deployer = FlinkDeployer(
            rest_url="http://localhost:8082",
            sql_gateway_url="http://localhost:8084",
        )

        mock_response = Mock()
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError("500 Server Error")
        deployer._http_session.request = Mock(return_value=mock_response)

        with pytest.raises(requests.exceptions.HTTPError):
            deployer.get_session()
        deployer.close()

    def test_handles_connection_timeout(self):
        """Should raise on connection timeout."""
        deployer = FlinkDeployer(rest_url="http://localhost:8082")
        deployer._http_session.request = Mock(side_effect=requests.exceptions.Timeout())

        with pytest.raises(requests.exceptions.Timeout):
            deployer.list_jobs()
        deployer.close()

    def test_check_connection_handles_all_errors(self):
        """check_connection should return False for any error, not raise."""
        deployer = FlinkDeployer(rest_url="http://localhost:8082")
        deployer._http_session.request = Mock(side_effect=requests.exceptions.ConnectionError())

        result = deployer.check_connection()
        assert result is False
        deployer.close()


class TestFlinkDeployerTimeouts:
    """Tests for timeout handling."""

    def test_submit_sql_respects_statement_timeout(self):
        """Should accept configurable statement timeout."""
        deployer = FlinkDeployer(
            rest_url="http://localhost:8082",
            sql_gateway_url="http://localhost:8084",
        )

        responses = [
            Mock(json=Mock(return_value={"sessionHandle": "s1"}), content=b'{}', raise_for_status=Mock()),
            Mock(json=Mock(return_value={"operationHandle": "op1"}), content=b'{}', raise_for_status=Mock()),
            Mock(json=Mock(return_value={"status": "FINISHED"}), content=b'{}', raise_for_status=Mock()),
        ]
        deployer._http_session.request = Mock(side_effect=responses)

        with patch("time.sleep"):
            result = deployer.submit_sql("SELECT 1", statement_timeout=120)

        assert result["results"][0]["status"] == "FINISHED"
        deployer.close()
