"""Type-preserving masking expression generation for Flink SQL."""

from __future__ import annotations

import logging
import re
from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, cast

if TYPE_CHECKING:
    from sqlglot import exp

logger = logging.getLogger(__name__)

# Mapping from masking method to Flink SQL function (for string types)
_STRING_MASK_FUNCTIONS: dict[str, str] = {
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


def apply_masking_to_sql(
    sql: str,
    masks: Sequence[Mapping[str, object]],
    schema: dict[str, str],
) -> str:
    """Apply masking policies to SQL using AST manipulation.

    Walks the SELECT expressions and replaces column references with
    masked versions.  Falls back to regex when sqlglot cannot parse.

    Args:
        sql: Transformed SQL (Jinja already resolved).
        masks: List of {"column": str, "method": str} dicts.
        schema: Column name → Flink SQL type mapping.
    """
    if not masks:
        return sql

    try:
        return _apply_masking_ast(sql, masks, schema)
    except Exception:
        logger.debug("sqlglot AST masking failed, falling back to regex")
        return _apply_masking_regex(sql, masks, schema)


def _apply_masking_ast(
    sql: str,
    masks: Sequence[Mapping[str, object]],
    schema: dict[str, str],
) -> str:
    """AST-based masking: only replaces columns in SELECT expressions."""
    import sqlglot
    from sqlglot import exp

    from streamt.compiler.flink_dialect import FlinkDialect

    mask_map = {
        cast(str, mask["column"]): cast(str, mask["method"])
        for mask in masks
    }
    parsed = sqlglot.parse_one(sql, dialect=FlinkDialect)

    select = parsed.find(exp.Select) if not isinstance(parsed, exp.Select) else parsed
    if not select:
        return sql

    new_expressions: list[exp.Expression] = []
    modified = False
    for expr in select.expressions:
        col_name = _get_select_column_name(expr)
        if col_name and col_name in mask_map:
            method = mask_map[col_name]
            col_type = schema.get(col_name, "STRING")
            mask_expr_str = build_mask_expression(col_name, method, col_type)
            mask_sql = f"{mask_expr_str} AS {col_name}"
            new_node = sqlglot.parse_one(mask_sql, dialect=FlinkDialect)
            new_expressions.append(new_node)
            modified = True
        else:
            new_expressions.append(expr)

    if not modified:
        return sql

    select.set("expressions", new_expressions)
    return parsed.sql(dialect=FlinkDialect)


def _get_select_column_name(expr: exp.Expression) -> str | None:
    """Extract the output column name from a SELECT expression."""
    from sqlglot import exp

    if isinstance(expr, exp.Alias):
        return expr.alias
    if isinstance(expr, exp.Column):
        return expr.name
    return None


def _apply_masking_regex(
    sql: str,
    masks: Sequence[Mapping[str, object]],
    schema: dict[str, str],
) -> str:
    """Regex fallback: replaces first occurrence of column name."""
    for mask in masks:
        column = cast(str, mask["column"])
        method = cast(str, mask["method"])
        col_type = schema.get(column, "STRING")
        mask_expr = build_mask_expression(column, method, col_type)
        sql = re.sub(
            rf"\b{re.escape(column)}\b",
            f"{mask_expr} AS {column}",
            sql,
            count=1,
        )
    return sql
