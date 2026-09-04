"""Pure secret-neutral identity for compiled manifest content."""

from __future__ import annotations

import hashlib
import json
import math
import re
from dataclasses import asdict, is_dataclass
from enum import Enum
from pathlib import Path
from typing import Protocol

_SENSITIVE_KEY = re.compile(
    r"(^|[._-])(?:password|passwd|secret|token|api[_-]?key|authorization|credentials?"
    r"|basic[._-]auth[._-]user[._-]info|sasl[._-]jaas[._-]config)($|[._-])",
    re.IGNORECASE,
)
_FAILURE_MESSAGE = "Manifest content could not be checksummed"
_DECIMAL_CHUNK_BASE = 1_000_000_000
_DECIMAL_CHUNK_WIDTH = 9


class ManifestIdentityError(ValueError):
    """Manifest content cannot be converted to its secret-neutral identity."""


class _ManifestLike(Protocol):
    def to_dict(self) -> dict[str, object]: ...


def _jsonable(value: object) -> object:
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ManifestIdentityError(_FAILURE_MESSAGE)
        return value
    if isinstance(value, Enum):
        return _jsonable(value.value)
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, dict):
        result: dict[str, object] = {}
        for key, item in value.items():
            if not isinstance(key, str):
                raise ManifestIdentityError(_FAILURE_MESSAGE)
            if _SENSITIVE_KEY.search(key):
                result[key] = "<redacted>"
            else:
                result[key] = _jsonable(item)
        return result
    if isinstance(value, (list, tuple)):
        return [_jsonable(item) for item in value]
    if isinstance(value, (set, frozenset)):
        normalized = [_jsonable(item) for item in value]
        return sorted(normalized, key=_canonical_json)
    if is_dataclass(value) and not isinstance(value, type):
        return _jsonable(asdict(value))
    if hasattr(value, "model_dump"):
        return _jsonable(value.model_dump(mode="json"))
    raise ManifestIdentityError(_FAILURE_MESSAGE)


def _integer_decimal(value: int) -> str:
    """Render an integer without consulting the process-wide digit limit."""
    if value == 0:
        return "0"
    negative = value < 0
    remaining = -value if negative else value
    chunks: list[int] = []
    while remaining:
        remaining, chunk = divmod(remaining, _DECIMAL_CHUNK_BASE)
        chunks.append(chunk)
    rendered = str(chunks.pop())
    rendered += "".join(
        f"{chunk:0{_DECIMAL_CHUNK_WIDTH}d}" for chunk in reversed(chunks)
    )
    return f"-{rendered}" if negative else rendered


def _serialize_jsonable(value: object) -> str:
    """Serialize normalized content with the repository's canonical JSON bytes."""
    if value is None:
        return "null"
    if value is True:
        return "true"
    if value is False:
        return "false"
    if isinstance(value, str):
        return json.dumps(value, ensure_ascii=False)
    if isinstance(value, int):
        return _integer_decimal(value)
    if isinstance(value, float):
        return json.dumps(value, ensure_ascii=False, allow_nan=False)
    if isinstance(value, list):
        return f"[{','.join(_serialize_jsonable(item) for item in value)}]"
    if isinstance(value, dict):
        items = sorted(value.items(), key=lambda pair: pair[0])
        return "{" + ",".join(
            f"{json.dumps(key, ensure_ascii=False)}:{_serialize_jsonable(item)}"
            for key, item in items
        ) + "}"
    raise ManifestIdentityError(_FAILURE_MESSAGE)


def _canonical_json(value: object) -> str:
    return _serialize_jsonable(_jsonable(value))


def manifest_checksum(manifest: _ManifestLike) -> str:
    """Hash secret-neutral manifest content while excluding compilation time."""
    try:
        manifest_data = manifest.to_dict()
        if not isinstance(manifest_data, dict):
            raise ManifestIdentityError(_FAILURE_MESSAGE)
        if any(not isinstance(key, str) for key in manifest_data):
            raise ManifestIdentityError(_FAILURE_MESSAGE)
        stable_data = {
            key: value
            for key, value in manifest_data.items()
            if not (type(key) is str and key == "compiled_at")
        }
        canonical = _canonical_json(stable_data)
        digest = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
    except Exception:
        raise ManifestIdentityError(_FAILURE_MESSAGE) from None
    return f"sha256:{digest}"


__all__ = ["ManifestIdentityError", "manifest_checksum"]
