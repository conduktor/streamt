"""Shared redaction helpers for user-visible diagnostic text."""

from __future__ import annotations

import re

_CREDENTIAL_URL = re.compile(r"://([^:@/\s]+):([^@/\s]+)@")
_SENSITIVE_ASSIGNMENT = re.compile(
    r"(?i)(?:password|passwd|secret|token|api[._-]?key|authorization|credentials?"
    r"|basic[._-]auth[._-]user[._-]info|sasl[._-]jaas[._-]config"
    r"|ssl[._-]key[._-]password)\s*[:=]\s*"
    r"(?:\"[^\"]*\"|'[^']*'|[^\r\n,;]+)"
)


def redact_sensitive_text(value: object) -> str:
    """Redact credential URLs and assignment-shaped secrets from diagnostic text."""
    text = str(value)
    text = _CREDENTIAL_URL.sub(r"://<redacted>:<redacted>@", text)
    return _SENSITIVE_ASSIGNMENT.sub("<redacted>", text)
