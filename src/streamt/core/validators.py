"""Shared Pydantic field validators for streamt models."""

from __future__ import annotations

import logging
import os
from pathlib import Path

logger = logging.getLogger(__name__)


def validate_ssl_path(v: str | None) -> str | None:
    """Validate that an SSL file path exists, unless it's an unresolved env var.

    Raises ValueError only when STREAMT_STRICT_SSL=1 is set (e.g. in CI).
    Otherwise logs a warning for missing files.
    """
    if v is None:
        return v
    # Skip for unresolved env var references
    if "${" in v:
        return v
    p = Path(v)
    if not p.exists():
        if os.environ.get("STREAMT_STRICT_SSL") == "1":
            raise ValueError(f"SSL file '{v}' does not exist")
        logger.warning("SSL file '%s' does not exist", v)
    return v
