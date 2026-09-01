"""Tests for structured auth/connection error codes in deployer factories."""


from streamt.cli.helpers import _classify_connection_error
from streamt.core.errors import ErrorCode


class TestClassifyConnectionError:
    """_classify_connection_error maps exceptions to structured error codes."""

    def test_ssl_error(self):
        e = Exception("SSL: CERTIFICATE_VERIFY_FAILED")
        code, msg = _classify_connection_error(e, "Schema Registry")
        assert code == ErrorCode.SSL_ERROR
        assert "SSL/TLS error" in msg
        assert "Schema Registry" in msg

    def test_tls_handshake_error(self):
        e = Exception("TLS handshake failed: timeout")
        code, msg = _classify_connection_error(e, "Flink")
        assert code == ErrorCode.SSL_ERROR
        assert "Flink" in msg

    def test_certificate_keyword(self):
        e = Exception("certificate has expired")
        code, _ = _classify_connection_error(e, "Kafka Connect")
        assert code == ErrorCode.SSL_ERROR

    def test_401_unauthorized(self):
        e = Exception("HTTP 401 Unauthorized")
        code, msg = _classify_connection_error(e, "Schema Registry")
        assert code == ErrorCode.AUTH_FAILED
        assert "Authentication failed" in msg

    def test_403_forbidden(self):
        e = Exception("403 Forbidden: insufficient permissions")
        code, _ = _classify_connection_error(e, "Flink")
        assert code == ErrorCode.AUTH_FAILED

    def test_sasl_failure(self):
        e = Exception("SASL authentication failed: bad credentials")
        code, msg = _classify_connection_error(e, "Kafka")
        assert code == ErrorCode.AUTH_FAILED
        assert "Kafka" in msg

    def test_connection_refused(self):
        e = Exception("[Errno 111] Connection refused")
        code, msg = _classify_connection_error(e, "Kafka")
        assert code == ErrorCode.CONNECTION_REFUSED
        assert "Cannot reach Kafka" in msg

    def test_dns_failure(self):
        e = Exception("Name or service not known")
        code, _ = _classify_connection_error(e, "Flink")
        assert code == ErrorCode.CONNECTION_REFUSED

    def test_connect_timeout(self):
        e = Exception("connect timeout after 5000ms")
        code, _ = _classify_connection_error(e, "Schema Registry")
        assert code == ErrorCode.CONNECTION_REFUSED

    def test_generic_error_no_code(self):
        e = Exception("something unexpected happened")
        code, msg = _classify_connection_error(e, "Kafka")
        assert code == ""
        assert "Cannot connect to Kafka" in msg

    def test_actionable_message_for_auth(self):
        e = Exception("401 Unauthorized")
        _, msg = _classify_connection_error(e, "Schema Registry")
        assert "username/password" in msg or "API key" in msg

    def test_actionable_message_for_ssl(self):
        e = Exception("SSL certificate verify failed")
        _, msg = _classify_connection_error(e, "Flink")
        assert "ssl_ca_location" in msg

    def test_connection_errors_redact_urls_key_values_and_jaas(self):
        cases = [
            (
                "failed at https://alice:url-secret@example.test",
                ("alice", "url-secret"),
            ),
            ("connection failed password=kv-secret", ("kv-secret",)),
            (
                'SASL failed sasl.jaas.config=required username="alice" '
                'password="jaas-secret";',
                ("alice", "jaas-secret"),
            ),
        ]

        for raw_message, secrets in cases:
            _, message = _classify_connection_error(Exception(raw_message), "Kafka")
            assert "<redacted>" in message
            for secret in secrets:
                assert secret not in message


class TestWarnDeployerError:
    """_warn_deployer_error adds structured error to formatter."""

    def test_auth_error_adds_structured_error(self):
        from unittest.mock import MagicMock
        fmt = MagicMock()
        fmt.print_warning = MagicMock()
        fmt.add_error = MagicMock()

        from streamt.cli.helpers import _warn_deployer_error
        _warn_deployer_error(fmt, Exception("401 Unauthorized"), "Schema Registry")

        fmt.add_error.assert_called_once()
        error_arg = fmt.add_error.call_args[0][0]
        assert error_arg.code == ErrorCode.AUTH_FAILED
        fmt.print_warning.assert_called_once()

    def test_generic_error_no_structured_error(self):
        from unittest.mock import MagicMock
        fmt = MagicMock()
        fmt.print_warning = MagicMock()
        fmt.add_error = MagicMock()

        from streamt.cli.helpers import _warn_deployer_error
        _warn_deployer_error(fmt, Exception("something unknown"), "Kafka")

        fmt.add_error.assert_not_called()
        fmt.print_warning.assert_called_once()


class TestErrorCodesExist:
    """Verify new error codes are defined."""

    def test_auth_failed_code(self):
        assert ErrorCode.AUTH_FAILED == "E404_AUTH_FAILED"

    def test_ssl_error_code(self):
        assert ErrorCode.SSL_ERROR == "E405_SSL_ERROR"

    def test_connection_refused_code(self):
        assert ErrorCode.CONNECTION_REFUSED == "E406_CONNECTION_REFUSED"
