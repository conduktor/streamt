"""Scope enrichment for subquery and CTE type propagation.

Walks a parsed sqlglot SELECT tree to find inline subqueries and CTEs,
infers their output column types, and merges them into the schema context
so the outer SELECT can resolve column references correctly.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from sqlglot import exp

if TYPE_CHECKING:
    from streamt.compiler.type_inference import TypeInferenceMixin


def _enrich_schema_from_scope(
    mixin: TypeInferenceMixin,
    select: exp.Select,
    schema: dict[str, str],
) -> dict[str, str]:
    """Enrich schema with columns from CTEs and FROM-clause subqueries."""
    # Process CTEs (WITH ... AS (...))
    root = select
    while root.parent:
        root = root.parent
    for cte in root.find_all(exp.CTE):
        cte_select = cte.find(exp.Select)
        if cte_select:
            cte_cols = _infer_subquery_columns(mixin, cte_select, schema)
            alias = cte.alias
            for col_name, col_type in cte_cols:
                schema[col_name] = col_type
                if alias:
                    schema[f"{alias}.{col_name}"] = col_type

    # Process FROM-clause subqueries
    from_clause = select.find(exp.From)
    if from_clause:
        _enrich_from_subqueries(mixin, from_clause, schema)

    # Process JOIN subqueries
    for join in select.find_all(exp.Join):
        _enrich_from_subqueries(mixin, join, schema)

    # Register table aliases so qualified refs (alias.col) resolve correctly
    _register_table_aliases(select, schema)

    return schema


def _enrich_from_subqueries(
    mixin: TypeInferenceMixin,
    node: exp.Expression,
    schema: dict[str, str],
) -> None:
    """Find subqueries under a FROM or JOIN node and add their columns to schema."""
    for subquery in node.find_all(exp.Subquery):
        inner_select = subquery.find(exp.Select)
        if not inner_select:
            continue
        sub_cols = _infer_subquery_columns(mixin, inner_select, schema)
        alias = subquery.alias
        for col_name, col_type in sub_cols:
            schema[col_name] = col_type
            if alias:
                schema[f"{alias}.{col_name}"] = col_type


def _register_table_aliases(
    select: exp.Select,
    schema: dict[str, str],
) -> None:
    """Map table aliases to qualified schema entries.

    For ``FROM orders o JOIN payments p``, copies ``orders.col``
    entries to ``o.col`` so qualified column references resolve.
    """
    for table in select.find_all(exp.Table):
        table_name = table.name
        alias = table.alias
        if not alias or alias == table_name:
            continue
        # Copy table_name.col → alias.col
        prefix = f"{table_name}."
        for key, val in list(schema.items()):
            if key.startswith(prefix):
                col = key[len(prefix):]
                schema[f"{alias}.{col}"] = val


def _infer_subquery_columns(
    mixin: TypeInferenceMixin,
    select: exp.Select,
    outer_schema: dict[str, str],
) -> list[tuple[str, str]]:
    """Infer column names and types from a subquery SELECT.

    Recursively enriches schema from nested subqueries/CTEs first.
    """
    inner_schema = _enrich_schema_from_scope(mixin, select, dict(outer_schema))
    columns: list[tuple[str, str]] = []
    for expr in select.expressions:
        if isinstance(expr, exp.Star):
            for col_name, col_type in inner_schema.items():
                if "." not in col_name:
                    columns.append((col_name, col_type))
            continue
        if isinstance(expr, exp.Column) and isinstance(expr.this, exp.Star):
            for col_name, col_type in inner_schema.items():
                if "." not in col_name:
                    columns.append((col_name, col_type))
            continue
        col_name = mixin._get_expression_alias(expr)
        col_type = mixin._infer_expression_type(expr, inner_schema)
        if col_name:
            columns.append((col_name, col_type))
    return columns
