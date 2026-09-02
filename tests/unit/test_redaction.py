"""Central user-visible diagnostic redaction tests."""

from streamt.core.redaction import redact_sensitive_text


def test_nested_structured_secret_values_are_redacted() -> None:
    value = {
        "outer": [
            {"password": "hunter2"},
            {"token": "token-value"},
            {"authorization": "Bearer nested-secret"},
        ]
    }

    result = redact_sensitive_text(value)

    assert "hunter2" not in result
    assert "token-value" not in result
    assert "nested-secret" not in result
    assert result.count("<redacted>") == 3


def test_postgres_dsn_hides_credentials_host_path_and_query() -> None:
    result = redact_sensitive_text(
        "provider failed for postgresql://alice:s3cret@db.internal:5432/state"
        "?sslmode=require&application_name=streamt"
    )

    assert result == "provider failed for postgresql://<redacted>"
    assert "alice" not in result
    assert "s3cret" not in result
    assert "db.internal" not in result
    assert "sslmode" not in result


def test_credential_url_hides_host_path_and_query() -> None:
    result = redact_sensitive_text(
        "request https://client:secret@api.internal/private?token=query-secret failed"
    )

    assert result == "request https://<redacted> failed"
    assert "api.internal" not in result
    assert "private" not in result
    assert "query-secret" not in result


def test_standalone_bearer_and_basic_authorization_are_redacted() -> None:
    result = redact_sensitive_text(
        "upstream returned Bearer eyJhbGciOiJIUzI1NiJ9.payload.signature "
        "then Basic YWxpY2U6c2VjcmV0"
    )

    assert "eyJhbGci" not in result
    assert "YWxpY2" not in result
    assert result.count("<redacted authorization>") == 2


def test_authorization_assignment_is_redacted_case_insensitively() -> None:
    result = redact_sensitive_text("AUTHORIZATION: Bearer top-secret")

    assert result == "<redacted>"
    assert "top-secret" not in result
