"""SQL-level validation checks for streamt models.

These checks use sqlglot AST analysis to catch Flink SQL errors
that would only surface at runtime.
"""

from __future__ import annotations

import re

from streamt.compiler.flink_dialect import FlinkDialect

_JINJA_REF_RE = re.compile(r'\{\{\s*(?:source|ref)\s*\(\s*["\']([^"\']+)["\']\s*\)\s*\}\}')

_WINDOW_COLUMNS = frozenset(
    {
        "window_start",
        "window_end",
        "window_time",
    }
)

_WINDOW_TVF_NAMES = frozenset(
    {
        "tumble",
        "hop",
        "session",
        "cumulate",
    }
)

_LEGACY_WINDOW_FUNCS = frozenset(
    {
        "tumble_start",
        "tumble_end",
        "tumble_rowtime",
        "tumble_proctime",
        "hop_start",
        "hop_end",
        "hop_rowtime",
        "hop_proctime",
        "session_start",
        "session_end",
        "session_rowtime",
        "session_proctime",
    }
)


def _render_jinja(sql: str) -> str:
    """Replace Jinja refs with plain table names for parsing."""
    return _JINJA_REF_RE.sub(r"\1", sql)


def check_window_group_by(sql: str) -> list[tuple[str, str]]:
    """Check that window columns in SELECT have a matching GROUP BY.

    Returns list of (code, message) tuples for violations.
    """
    try:
        import sqlglot
        from sqlglot import exp
    except ImportError:
        return []

    rendered = _render_jinja(sql)
    try:
        parsed = sqlglot.parse_one(rendered, dialect=FlinkDialect)
    except Exception:
        return []

    if not isinstance(parsed, exp.Select):
        selects = list(parsed.find_all(exp.Select))
        if not selects:
            return []
        parsed = selects[0]

    # Check if SELECT references window columns or legacy window functions
    has_window_ref = False
    for col in parsed.find_all(exp.Column):
        if col.name.lower() in _WINDOW_COLUMNS:
            has_window_ref = True
            break
    if not has_window_ref:
        for func in parsed.find_all(exp.Anonymous):
            if func.name.lower() in _LEGACY_WINDOW_FUNCS:
                has_window_ref = True
                break
    if not has_window_ref:
        return []

    # Window columns found — verify GROUP BY exists
    group = parsed.find(exp.Group)
    if not group:
        return [
            (
                "WINDOW_NO_GROUP_BY",
                "SELECT references window columns (window_start/window_end) "
                "but has no GROUP BY clause. Flink requires GROUP BY with "
                "window TVF columns.",
            )
        ]

    # GROUP BY exists — check it includes a window TVF or window columns
    has_window_in_group = False
    for col in group.find_all(exp.Column):
        if col.name.lower() in _WINDOW_COLUMNS:
            has_window_in_group = True
            break
    if not has_window_in_group:
        for func in group.find_all(exp.Anonymous):
            if func.name.lower() in _WINDOW_TVF_NAMES | _LEGACY_WINDOW_FUNCS:
                has_window_in_group = True
                break
    if not has_window_in_group:
        # Also check FROM for window TVF (TABLE(TUMBLE(...)))
        from_clause = parsed.find(exp.From)
        if from_clause:
            for func in from_clause.find_all(exp.Anonymous):
                if func.name.lower() in _WINDOW_TVF_NAMES:
                    has_window_in_group = True
                    break

    if not has_window_in_group:
        return [
            (
                "WINDOW_GROUP_BY_MISSING_TVF",
                "SELECT references window columns but GROUP BY does not include "
                "window_start/window_end or a window TVF. This may fail at runtime.",
            )
        ]

    return []


def check_having_aliases(sql: str) -> list[tuple[str, str]]:
    """Check that HAVING clause does not reference SELECT aliases.

    In Flink SQL, HAVING is evaluated before SELECT aliases are resolved,
    so referencing an alias in HAVING will fail at runtime.

    Returns list of (code, message) tuples for violations.
    """
    try:
        import sqlglot
        from sqlglot import exp
    except ImportError:
        return []

    rendered = _render_jinja(sql)
    try:
        parsed = sqlglot.parse_one(rendered, dialect=FlinkDialect)
    except Exception:
        return []

    if not isinstance(parsed, exp.Select):
        selects = list(parsed.find_all(exp.Select))
        if not selects:
            return []
        parsed = selects[0]

    having = parsed.find(exp.Having)
    if not having:
        return []

    # Collect SELECT aliases (case-insensitive)
    select_aliases: set[str] = set()
    for expr in parsed.expressions:
        if isinstance(expr, exp.Alias):
            select_aliases.add(expr.alias.lower())

    if not select_aliases:
        return []

    # Walk HAVING expression for column references matching aliases
    violations: list[str] = []
    for col in having.find_all(exp.Column):
        if col.table:
            continue  # Qualified column (table.col) — not an alias
        if col.name.lower() in select_aliases:
            violations.append(col.name)

    if violations:
        cols = ", ".join(sorted(set(violations)))
        return [
            (
                "HAVING_SELECT_ALIAS",
                f"HAVING references SELECT alias(es) ({cols}). "
                f"Flink evaluates HAVING before SELECT aliases are resolved — "
                f"use the original expression instead.",
            )
        ]

    return []
