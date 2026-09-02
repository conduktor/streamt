"""Shared redaction helpers for user-visible diagnostic text."""

from __future__ import annotations

import re

_POSTGRES_DSN = re.compile(
    r"(?i)\bpostgres(?:ql)?://[^\s\"'<>]+"
)
_CREDENTIAL_URL = re.compile(
    r"(?i)\b([a-z][a-z0-9+.-]*://)[^:@/\s]+:[^@/\s]+@[^\s\"'<>]+"
)
_AUTHORIZATION_VALUE = re.compile(
    r"(?i)\b(?:bearer|basic)\s+[A-Za-z0-9._~+/=-]+"
)
_SENSITIVE_ASSIGNMENT = re.compile(
    r"(?i)[\"']?(?:password|passwd|secret|token|api[._-]?key|authorization|credentials?"
    r"|basic[._-]auth[._-]user[._-]info|sasl[._-]jaas[._-]config"
    r"|ssl[._-]key[._-]password)[\"']?\s*[:=]\s*"
    r"(?:\"[^\"]*\"|'[^']*'|[^\r\n,;]+)"
)


def redact_sensitive_text(value: object) -> str:
    """Redact credential URLs and assignment-shaped secrets from diagnostic text."""
    text = str(value)
    text = _POSTGRES_DSN.sub("postgresql://<redacted>", text)
    text = _CREDENTIAL_URL.sub(r"\1<redacted>", text)
    text = _SENSITIVE_ASSIGNMENT.sub("<redacted>", text)
    return _AUTHORIZATION_VALUE.sub("<redacted authorization>", text)
