"""Type inference gaps: schema propagation, DECIMAL, subqueries, CASE/COALESCE, JOINs.

Each test documents either:
- CORRECT current behavior (regular assertions) -- regression anchors
- BROKEN current behavior (``pytest.xfail``) -- spec for what SHOULD happen

Groups 1-5 live here. Groups 6-10 + schema gen + cross-cutting are in
``test_type_inference_gaps_extended.py``.
"""

from __future__ import annotations

import pytest

from streamt.compiler.compiler import Compiler
from streamt.compiler.type_inference import TypeInferenceMixin
from streamt.core.models import (
    ColumnDefinition,
    KafkaConfig,
    Model,
    ProjectInfo,
    RuntimeConfig,
    Source,
    StreamtProject,
)

# ---------------------------------------------------------------------------
# Helpers (shared contract with _extended module)
# ---------------------------------------------------------------------------

class StubTypeInference(TypeInferenceMixin):
    """Minimal host satisfying the mixin contract."""

    def _build_source_schema(self, model):
        return {}

    _current_model = None


def _infer(sql: str, schema: dict[str, str] | None = None) -> list[tuple[str, str]]:
    """Parse *sql* through the mixin and return inferred (name, type) pairs."""
    stub = StubTypeInference()
    return stub._extract_select_columns_with_types(sql, schema_context=schema or {})


def _project(sources: list[Source], models: list[Model] | None = None) -> StreamtProject:
    return StreamtProject(
        project=ProjectInfo(name="test_project"),
        runtime=RuntimeConfig(kafka=KafkaConfig(bootstrap_servers="localhost:9092")),
        sources=sources,
        models=models or [],
    )


def _infer_via_compiler(
    sources: list[Source],
    model_sql: str,
) -> list[tuple[str, str]]:
    """Full-stack inference through the Compiler with source resolution."""
    project = _project(
        sources=sources,
        models=[Model(name="test_model", sql=model_sql)],
    )
    compiler = Compiler(project)
    model = project.get_model("test_model")
    return compiler._extract_select_columns_with_types(model.sql, model=model)


# ===================================================================
# GROUP 1: Schema context propagation (SELECT *)
# ===================================================================

class TestGroup1SelectStarPropagation:
    """SELECT * should expand source columns from the schema context.
    Currently returns [] because Star is not handled by _get_expression_alias.
    """

    def test_select_star_propagates_source_columns(self):
        """SELECT * FROM src returns all source columns with types."""
        cols = _infer(
            "SELECT * FROM src",
            {"id": "BIGINT", "name": "STRING"},
        )
        assert len(cols) == 2
        type_map = dict(cols)
        assert type_map["id"] == "BIGINT"
        assert type_map["name"] == "STRING"

    def test_select_star_plus_expression_includes_all(self):
        """SELECT *, expr includes all source cols + the expression."""
        cols = _infer(
            "SELECT *, amount * 2 AS doubled FROM src",
            {"id": "BIGINT", "name": "STRING", "amount": "DOUBLE"},
        )
        type_map = dict(cols)
        assert "id" in type_map
        assert "name" in type_map
        assert type_map["doubled"] == "DOUBLE"
        assert len(cols) == 4  # id, name, amount, doubled

    def test_select_star_via_compiler_propagates(self):
        """Full-stack SELECT * propagates source columns through Compiler."""
        cols = _infer_via_compiler(
            sources=[
                Source(name="src", topic="src_topic", columns=[
                    ColumnDefinition(name="id", type="BIGINT"),
                    ColumnDefinition(name="name", type="STRING"),
                ])
            ],
            model_sql="SELECT * FROM {{ source('src') }}",
        )
        assert len(cols) == 2
        type_map = dict(cols)
        assert type_map["id"] == "BIGINT"
        assert type_map["name"] == "STRING"

    def test_select_star_from_join_should_propagate(self):
        """SELECT * from JOIN should propagate columns from both sources."""
        pytest.xfail("SELECT * does not propagate in JOINs")
        cols = _infer_via_compiler(
            sources=[
                Source(name="orders", topic="orders_topic", columns=[
                    ColumnDefinition(name="order_id", type="BIGINT"),
                    ColumnDefinition(name="customer_id", type="INT"),
                ]),
                Source(name="customers", topic="customers_topic", columns=[
                    ColumnDefinition(name="customer_id", type="INT"),
                    ColumnDefinition(name="name", type="STRING"),
                ]),
            ],
            model_sql=(
                "SELECT * FROM {{ source('orders') }} o "
                "JOIN {{ source('customers') }} c ON o.customer_id = c.customer_id"
            ),
        )
        assert len(cols) >= 3


# ===================================================================
# GROUP 2: DECIMAL precision preservation
# ===================================================================

class TestGroup2DecimalPrecision:
    """DECIMAL(p,s) should be preserved through expressions."""

    def test_decimal_column_reference_preserves_precision(self):
        """Direct column ref preserves DECIMAL(38,18)."""
        cols = _infer("SELECT amount FROM t", {"amount": "DECIMAL(38,18)"})
        assert cols == [("amount", "DECIMAL(38,18)")]

    def test_sum_decimal_widens_precision_to_38(self):
        """Per Flink rules, SUM(DECIMAL(10,2)) widens to DECIMAL(38,2)."""
        cols = _infer(
            "SELECT SUM(amount) AS total FROM t",
            {"amount": "DECIMAL(10,2)"},
        )
        assert cols[0][1] == "DECIMAL(38,2)"

    def test_decimal_division_preserves_decimal(self):
        """DECIMAL(10,2) / INT literal returns DECIMAL(10,2)."""
        cols = _infer(
            "SELECT amount / 3 AS divided FROM t",
            {"amount": "DECIMAL(10,2)"},
        )
        assert cols[0][1] == "DECIMAL(10,2)"

    def test_decimal_division_by_column(self):
        """DECIMAL(10,2) / INT column returns DECIMAL(10,2)."""
        cols = _infer(
            "SELECT amount / cnt AS avg FROM t",
            {"amount": "DECIMAL(10,2)", "cnt": "INT"},
        )
        assert cols[0][1] == "DECIMAL(10,2)"

    def test_cast_to_decimal_bare(self):
        """CAST(x AS DECIMAL) produces bare DECIMAL (no precision)."""
        cols = _infer("SELECT CAST(x AS DECIMAL) AS d FROM t", {"x": "STRING"})
        assert cols[0][1] == "DECIMAL"

    def test_decimal_addition_preserves_decimal(self):
        """DECIMAL + INT should promote to DECIMAL (highest precedence)."""
        cols = _infer(
            "SELECT amount + 1 AS incremented FROM t",
            {"amount": "DECIMAL(10,2)"},
        )
        assert cols[0][1] == "DECIMAL(10,2)"

    def test_decimal_multiply_preserves_decimal(self):
        """DECIMAL * DOUBLE promotes to DECIMAL (higher precedence)."""
        cols = _infer(
            "SELECT amount * rate AS product FROM t",
            {"amount": "DECIMAL(10,2)", "rate": "DOUBLE"},
        )
        assert cols[0][1] == "DECIMAL(10,2)"


# ===================================================================
# GROUP 3: Subquery type inference
# ===================================================================

class TestGroup3SubqueryTypeInference:
    """Columns from subqueries are resolved against the outer schema context
    which is empty -- inner expression types are not propagated.
    """

    def test_subquery_count_resolves_to_string_currently(self):
        """cnt from inner COUNT(*) resolves to STRING in outer SELECT."""
        cols = _infer("SELECT cnt FROM (SELECT COUNT(*) AS cnt FROM t) sub")
        assert cols == [("cnt", "STRING")]

    def test_subquery_count_should_resolve_to_bigint(self):
        """cnt from inner COUNT(*) should be BIGINT."""
        pytest.xfail("Subquery columns not propagated to outer query")
        cols = _infer("SELECT cnt FROM (SELECT COUNT(*) AS cnt FROM t) sub")
        assert cols == [("cnt", "BIGINT")]

    def test_subquery_literal_resolves_to_string_currently(self):
        """x from inner ``SELECT 42 as x`` resolves to STRING in outer SELECT."""
        cols = _infer("SELECT x FROM (SELECT 42 as x) sub")
        assert cols == [("x", "STRING")]

    def test_subquery_literal_should_resolve_to_int(self):
        """x from inner ``SELECT 42 as x`` should be INT."""
        pytest.xfail("Subquery columns not propagated to outer query")
        cols = _infer("SELECT x FROM (SELECT 42 as x) sub")
        assert cols == [("x", "INT")]

    def test_subquery_with_arithmetic_should_propagate(self):
        """Computed columns in subqueries should carry their inferred type."""
        pytest.xfail("Subquery expression types not propagated")
        cols = _infer(
            "SELECT doubled FROM (SELECT amount * 2 AS doubled FROM t) sub",
            {"amount": "DOUBLE"},
        )
        assert cols == [("doubled", "DOUBLE")]

    def test_nested_subquery_type_propagation(self):
        """Types should propagate through multiple subquery levels."""
        pytest.xfail("Nested subquery types not propagated")
        cols = _infer(
            "SELECT total FROM ("
            "  SELECT cnt AS total FROM ("
            "    SELECT COUNT(*) AS cnt FROM t"
            "  ) inner_q"
            ") outer_q",
        )
        assert cols == [("total", "BIGINT")]

    def test_subquery_with_schema_context_still_fails(self):
        """Even with a schema context, subquery inner types are not visible."""
        cols = _infer(
            "SELECT doubled FROM (SELECT amount * 2 AS doubled FROM t) sub",
            {"amount": "DOUBLE"},
        )
        assert cols == [("doubled", "STRING")]

    def test_cte_type_propagation(self):
        """CTE columns should carry their inferred types to the outer query."""
        pytest.xfail("CTE column types not propagated to outer query")
        cols = _infer(
            "WITH agg AS ("
            "  SELECT category, COUNT(*) AS cnt, SUM(amount) AS total FROM t"
            "  GROUP BY category"
            ") "
            "SELECT cnt, total FROM agg",
            {"category": "STRING", "amount": "DOUBLE"},
        )
        type_map = dict(cols)
        assert type_map["cnt"] == "BIGINT"
        assert type_map["total"] == "DOUBLE"


# ===================================================================
# GROUP 4: CASE / COALESCE type widening
# ===================================================================

class TestGroup4CaseCoalesceTypeWidening:
    """CASE and COALESCE should widen types across branches."""

    def test_case_int_int_returns_int(self):
        cols = _infer("SELECT CASE WHEN true THEN 1 ELSE 2 END as x FROM t")
        assert cols[0][1] == "INT"

    def test_case_int_double_widens_to_double(self):
        cols = _infer("SELECT CASE WHEN true THEN 1 ELSE 2.5 END as x FROM t")
        assert cols[0][1] == "DOUBLE"

    def test_coalesce_bigint_zero_returns_bigint(self):
        cols = _infer(
            "SELECT COALESCE(bigint_col, 0) as x FROM t",
            {"bigint_col": "BIGINT"},
        )
        assert cols[0][1] == "BIGINT"

    def test_case_mixed_string_int_returns_string(self):
        """CASE WHEN true THEN 'a' ELSE 1 END -- mixed types default to STRING."""
        cols = _infer("SELECT CASE WHEN true THEN 'a' ELSE 1 END as x FROM t")
        assert cols[0][1] == "STRING"

    def test_case_string_string_returns_string(self):
        cols = _infer("SELECT CASE WHEN x > 0 THEN 'high' ELSE 'low' END AS val FROM t")
        assert cols[0][1] == "STRING"

    def test_coalesce_int_null_preserves_int(self):
        """COALESCE(INT, NULL) preserves INT (NULL filtered from merge)."""
        cols = _infer("SELECT COALESCE(x, NULL) AS val FROM t", {"x": "INT"})
        assert cols[0][1] == "INT"

    def test_coalesce_double_null_preserves_double(self):
        cols = _infer("SELECT COALESCE(amount, NULL) AS val FROM t", {"amount": "DOUBLE"})
        assert cols[0][1] == "DOUBLE"

    def test_case_with_null_else_preserves_type(self):
        cols = _infer(
            "SELECT CASE WHEN x > 0 THEN x ELSE NULL END AS val FROM t",
            {"x": "INT"},
        )
        assert cols[0][1] == "INT"

    def test_case_null_branch_with_bigint(self):
        cols = _infer(
            "SELECT CASE WHEN flag THEN amount ELSE NULL END AS val FROM t",
            {"flag": "BOOLEAN", "amount": "BIGINT"},
        )
        assert cols[0][1] == "BIGINT"

    def test_case_boolean_branches(self):
        cols = _infer("SELECT CASE WHEN x > 0 THEN TRUE ELSE FALSE END AS val FROM t")
        assert cols[0][1] == "BOOLEAN"

    def test_nested_coalesce_in_case_with_null(self):
        cols = _infer(
            "SELECT CASE WHEN flag THEN COALESCE(amount, NULL) ELSE 0 END AS val FROM t",
            {"flag": "BOOLEAN", "amount": "DOUBLE"},
        )
        assert cols[0][1] == "DOUBLE"


# ===================================================================
# GROUP 5: Qualified column names in JOINs
# ===================================================================

class TestGroup5QualifiedColumnNamesInJoins:
    """Column references like ``a.id`` resolve by name only (ignoring table
    qualifier), so they work when the flat schema has the column name.
    """

    def test_qualified_col_resolves_from_flat_schema(self):
        cols = _infer(
            "SELECT a.id FROM a JOIN b ON a.id = b.id",
            {"id": "BIGINT"},
        )
        assert cols == [("id", "BIGINT")]

    def test_qualified_col_not_in_schema_defaults_to_string(self):
        cols = _infer(
            "SELECT a.id FROM a JOIN b ON a.id = b.id",
            {"name": "STRING"},
        )
        assert cols == [("id", "STRING")]

    def test_qualified_cols_both_resolve_by_name(self):
        cols = _infer(
            "SELECT a.id, b.name FROM a JOIN b ON a.id = b.id",
            {"id": "INT", "name": "STRING"},
        )
        assert cols == [("id", "INT"), ("name", "STRING")]

    def test_same_column_name_different_tables_loses_distinction(self):
        """a.id and b.id both resolve to the same flat schema entry."""
        cols = _infer(
            "SELECT a.id AS a_id, b.id AS b_id FROM a JOIN b ON a.id = b.id",
            {"id": "BIGINT"},
        )
        assert cols[0][1] == "BIGINT"
        assert cols[1][1] == "BIGINT"

    def test_different_types_same_name_should_resolve_per_table(self):
        pytest.xfail("Table qualifier ignored: flat schema cannot distinguish a.id vs b.id")
        cols = _infer_via_compiler(
            sources=[
                Source(name="orders", topic="orders_topic", columns=[
                    ColumnDefinition(name="id", type="INT"),
                ]),
                Source(name="payments", topic="payments_topic", columns=[
                    ColumnDefinition(name="id", type="BIGINT"),
                ]),
            ],
            model_sql=(
                "SELECT o.id AS order_id, p.id AS payment_id "
                "FROM {{ source('orders') }} o "
                "JOIN {{ source('payments') }} p ON o.id = p.id"
            ),
        )
        type_map = dict(cols)
        assert type_map["order_id"] == "INT"
        assert type_map["payment_id"] == "BIGINT"

    def test_same_column_name_different_types_amount(self):
        pytest.xfail("Table qualifier ignored in flat schema lookup")
        cols = _infer_via_compiler(
            sources=[
                Source(name="orders", topic="orders_topic", columns=[
                    ColumnDefinition(name="amount", type="DECIMAL(10,2)"),
                ]),
                Source(name="refunds", topic="refunds_topic", columns=[
                    ColumnDefinition(name="amount", type="DOUBLE"),
                ]),
            ],
            model_sql=(
                "SELECT o.amount AS order_amount, r.amount AS refund_amount "
                "FROM {{ source('orders') }} o "
                "JOIN {{ source('refunds') }} r ON o.id = r.order_id"
            ),
        )
        type_map = dict(cols)
        assert type_map["order_amount"] == "DECIMAL(10,2)"
        assert type_map["refund_amount"] == "DOUBLE"

    def test_unqualified_col_in_join_resolves_normally(self):
        cols = _infer(
            "SELECT id, name FROM a JOIN b ON a.id = b.id",
            {"id": "BIGINT", "name": "STRING"},
        )
        assert cols == [("id", "BIGINT"), ("name", "STRING")]
