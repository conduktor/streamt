"""Type-preserving masking expression generation for Flink SQL."""

from __future__ import annotations

# Mapping from masking method to Flink SQL function (for string types)
_STRING_MASK_FUNCTIONS = {
    "hash": "MD5",
    "redact": "REGEXP_REPLACE",
    "partial": "REGEXP_REPLACE",
    "null": "NULLIF",
}


def _is_string_type(col_type: str) -> bool:
    upper = col_type.upper().strip()
    return upper in ("STRING",) or upper.startswith("VARCHAR") or upper.startswith("CHAR")


def build_mask_expression(column: str, method: str, col_type: str) -> str:
    """Build a masking expression that preserves the column's declared type.

    String types use the standard mask function (MD5, REGEXP_REPLACE).
    Non-string types use type-preserving alternatives:
      - hash  → numeric hash via HASH_CODE, cast back to original type
      - redact/partial → CAST(NULL AS type), since string redaction
        cannot produce a valid non-string value
      - null  → CAST(NULL AS type)
    """
    if method == "null":
        return f"CAST(NULL AS {col_type})"

    if _is_string_type(col_type):
        mask_fn = _STRING_MASK_FUNCTIONS.get(method, "MD5")
        return f"{mask_fn}({column})"

    # Non-string type: hash can produce a numeric value
    if method == "hash":
        return f"CAST(ABS(HASH_CODE(CAST({column} AS STRING))) AS {col_type})"

    # redact/partial on non-string: no meaningful representation, null out
    return f"CAST(NULL AS {col_type})"
