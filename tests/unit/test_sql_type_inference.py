"""Unit tests for SQL type inference using sqlglot.

These tests validate that sqlglot can parse Flink-compatible SQL patterns
and that type inference works correctly for various expressions.
"""

import sqlglot
from sqlglot import exp


class TestSqlglotFlinkCompatibility:
    """Test sqlglot parsing of Flink-compatible SQL patterns."""

    def test_simple_select(self):
        """Test simple SELECT column parsing."""
        sql = "SELECT order_id, category, amount FROM orders WHERE amount >= 100"
        parsed = sqlglot.parse_one(sql)

        assert isinstance(parsed, exp.Select)
        assert len(parsed.expressions) == 3

        # All should be Column expressions
        for expr in parsed.expressions:
            assert isinstance(expr, exp.Column)

        # Check column names
        names = [expr.name for expr in parsed.expressions]
        assert names == ["order_id", "category", "amount"]

    def test_select_with_alias(self):
        """Test SELECT with AS alias."""
        sql = "SELECT order_id, amount * 2 AS doubled_amount FROM orders"
        parsed = sqlglot.parse_one(sql)

        assert len(parsed.expressions) == 2

        # First is simple column
        assert isinstance(parsed.expressions[0], exp.Column)
        assert parsed.expressions[0].name == "order_id"

        # Second is aliased expression
        assert isinstance(parsed.expressions[1], exp.Alias)
        assert parsed.expressions[1].alias == "doubled_amount"

    def test_case_when_boolean(self):
        """Test CASE WHEN with boolean result."""
        sql = "SELECT CASE WHEN amount >= 200 THEN TRUE ELSE FALSE END as is_premium FROM orders"
        parsed = sqlglot.parse_one(sql)

        assert len(parsed.expressions) == 1
        alias_expr = parsed.expressions[0]

        assert isinstance(alias_expr, exp.Alias)
        assert alias_expr.alias == "is_premium"
        assert isinstance(alias_expr.this, exp.Case)

        # Check that THEN clause contains Boolean
        case_expr = alias_expr.this
        ifs = case_expr.args.get("ifs", [])
        assert len(ifs) >= 1
        then_value = ifs[0].args.get("true")
        assert isinstance(then_value, exp.Boolean)

    def test_aggregate_count(self):
        """Test COUNT(*) aggregate parsing."""
        sql = "SELECT COUNT(*) as cnt FROM orders"
        parsed = sqlglot.parse_one(sql)

        alias_expr = parsed.expressions[0]
        assert isinstance(alias_expr, exp.Alias)
        assert alias_expr.alias == "cnt"
        assert isinstance(alias_expr.this, exp.Count)

    def test_aggregate_sum(self):
        """Test SUM() aggregate parsing."""
        sql = "SELECT SUM(amount) as total FROM orders"
        parsed = sqlglot.parse_one(sql)

        alias_expr = parsed.expressions[0]
        assert isinstance(alias_expr, exp.Alias)
        assert alias_expr.alias == "total"
        assert isinstance(alias_expr.this, exp.Sum)

    def test_aggregate_avg(self):
        """Test AVG() aggregate parsing."""
        sql = "SELECT AVG(amount) as avg_amount FROM orders"
        parsed = sqlglot.parse_one(sql)

        alias_expr = parsed.expressions[0]
        assert isinstance(alias_expr.this, exp.Avg)

    def test_string_functions(self):
        """Test string function parsing."""
        sql = "SELECT UPPER(category) as upper_cat, LOWER(name) as lower_name FROM orders"
        parsed = sqlglot.parse_one(sql)

        assert len(parsed.expressions) == 2
        assert isinstance(parsed.expressions[0].this, exp.Upper)
        assert isinstance(parsed.expressions[1].this, exp.Lower)

    def test_tumble_window_function(self):
        """Test TUMBLE window function parsing (Flink-specific).

        Note: sqlglot doesn't have native Flink support, so TUMBLE_START
        is parsed as Anonymous function.
        """
        sql = """SELECT
            category,
            TUMBLE_START(event_time, INTERVAL '1' HOUR) as window_start,
            COUNT(*) as event_count
        FROM events
        GROUP BY category, TUMBLE(event_time, INTERVAL '1' HOUR)"""

        parsed = sqlglot.parse_one(sql)

        # Find the TUMBLE_START expression
        found_tumble_start = False
        for expr in parsed.expressions:
            if isinstance(expr, exp.Alias) and expr.alias == "window_start":
                inner = expr.this
                # TUMBLE_START is parsed as Anonymous function
                if isinstance(inner, exp.Anonymous):
                    assert inner.name.upper() == "TUMBLE_START"
                    found_tumble_start = True

        assert found_tumble_start, "TUMBLE_START should be parsed as Anonymous function"

    def test_jinja_template_cleaning(self):
        """Test that Jinja templates are properly cleaned before parsing."""
        import re

        sql = "SELECT order_id, category FROM {{ source('orders') }} WHERE amount > 0"

        # Clean Jinja templates - same logic as compiler
        clean_sql = re.sub(r'\{\{\s*source\s*\(\s*["\'](\w+)["\']\s*\)\s*\}\}', r'\1', sql)
        clean_sql = re.sub(r'\{\{\s*ref\s*\(\s*["\'](\w+)["\']\s*\)\s*\}\}', r'\1', clean_sql)

        assert clean_sql == "SELECT order_id, category FROM orders WHERE amount > 0"

        parsed = sqlglot.parse_one(clean_sql)
        assert isinstance(parsed, exp.Select)
        assert len(parsed.expressions) == 2

    def test_numeric_literal(self):
        """Test numeric literal type detection."""
        sql = "SELECT 42 as int_val, 3.14 as float_val FROM dual"
        parsed = sqlglot.parse_one(sql)

        int_expr = parsed.expressions[0].this
        float_expr = parsed.expressions[1].this

        assert isinstance(int_expr, exp.Literal)
        assert int_expr.is_int

        assert isinstance(float_expr, exp.Literal)
        assert float_expr.is_number

    def test_cast_expression(self):
        """Test CAST expression parsing."""
        sql = "SELECT CAST(amount AS INT) as int_amount FROM orders"
        parsed = sqlglot.parse_one(sql)

        alias_expr = parsed.expressions[0]
        assert isinstance(alias_expr.this, exp.Cast)
        assert alias_expr.this.to.sql().upper() == "INT"

    def test_tumble_end_function(self):
        """Test TUMBLE_END window function parsing."""
        sql = """SELECT
            category,
            TUMBLE_END(event_time, INTERVAL '1' HOUR) as window_end
        FROM events"""
        parsed = sqlglot.parse_one(sql)

        # Find TUMBLE_END
        for expr in parsed.expressions:
            if isinstance(expr, exp.Alias) and expr.alias == "window_end":
                inner = expr.this
                assert isinstance(inner, exp.Anonymous)
                assert inner.name.upper() == "TUMBLE_END"

    def test_row_number_over_partition(self):
        """Test ROW_NUMBER() OVER (PARTITION BY ... ORDER BY ...) parsing."""
        sql = """SELECT
            event_id,
            ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY proc_time) as rn
        FROM events"""
        parsed = sqlglot.parse_one(sql)

        # Find ROW_NUMBER expression
        for expr in parsed.expressions:
            if isinstance(expr, exp.Alias) and expr.alias == "rn":
                inner = expr.this
                # ROW_NUMBER with OVER is parsed as Window expression
                assert isinstance(inner, exp.Window)
                assert isinstance(inner.this, exp.RowNumber)

    def test_proctime_function(self):
        """Test PROCTIME() function parsing (Flink-specific)."""
        sql = "SELECT event_id, PROCTIME() as proc_time FROM events"
        parsed = sqlglot.parse_one(sql)

        for expr in parsed.expressions:
            if isinstance(expr, exp.Alias) and expr.alias == "proc_time":
                inner = expr.this
                # PROCTIME is parsed as Anonymous function
                assert isinstance(inner, exp.Anonymous)
                assert inner.name.upper() == "PROCTIME"

    def test_join_expression(self):
        """Test JOIN expression parsing."""
        sql = """SELECT
            o.order_id,
            c.name as customer_name
        FROM orders o
        JOIN customers c ON o.customer_id = c.id"""
        parsed = sqlglot.parse_one(sql)

        assert isinstance(parsed, exp.Select)
        assert len(parsed.expressions) == 2

        # Check that we have a JOIN
        joins = list(parsed.find_all(exp.Join))
        assert len(joins) == 1

    def test_min_max_aggregates(self):
        """Test MIN/MAX aggregate parsing."""
        sql = "SELECT MIN(amount) as min_amt, MAX(amount) as max_amt FROM orders"
        parsed = sqlglot.parse_one(sql)

        assert len(parsed.expressions) == 2
        assert isinstance(parsed.expressions[0].this, exp.Min)
        assert isinstance(parsed.expressions[1].this, exp.Max)

    def test_arithmetic_expression(self):
        """Test arithmetic expression parsing."""
        sql = "SELECT amount * 2 as doubled, amount + tax as total FROM orders"
        parsed = sqlglot.parse_one(sql)

        assert len(parsed.expressions) == 2
        assert isinstance(parsed.expressions[0].this, exp.Mul)
        assert isinstance(parsed.expressions[1].this, exp.Add)

    def test_coalesce_function(self):
        """Test COALESCE function parsing."""
        sql = "SELECT COALESCE(name, 'unknown') as safe_name FROM orders"
        parsed = sqlglot.parse_one(sql)

        alias_expr = parsed.expressions[0]
        assert isinstance(alias_expr.this, exp.Coalesce)

    def test_hop_window_function(self):
        """Test HOP window function parsing (Flink sliding window)."""
        sql = """SELECT
            category,
            HOP_START(event_time, INTERVAL '5' MINUTE, INTERVAL '10' MINUTE) as hop_start
        FROM events"""
        parsed = sqlglot.parse_one(sql)

        for expr in parsed.expressions:
            if isinstance(expr, exp.Alias) and expr.alias == "hop_start":
                inner = expr.this
                assert isinstance(inner, exp.Anonymous)
                assert inner.name.upper() == "HOP_START"

    def test_session_window_function(self):
        """Test SESSION window function parsing (Flink session window)."""
        sql = """SELECT
            user_id,
            SESSION_END(event_time, INTERVAL '30' MINUTE) as session_end
        FROM events"""
        parsed = sqlglot.parse_one(sql)

        for expr in parsed.expressions:
            if isinstance(expr, exp.Alias) and expr.alias == "session_end":
                inner = expr.this
                assert isinstance(inner, exp.Anonymous)
                assert inner.name.upper() == "SESSION_END"

    def test_nested_case_when(self):
        """Test nested CASE WHEN expressions."""
        sql = """SELECT
            CASE
                WHEN amount >= 1000 THEN 'high'
                WHEN amount >= 100 THEN 'medium'
                ELSE 'low'
            END as tier
        FROM orders"""
        parsed = sqlglot.parse_one(sql)

        alias_expr = parsed.expressions[0]
        assert isinstance(alias_expr.this, exp.Case)
        # Should have 2 IFs (WHEN clauses)
        ifs = alias_expr.this.args.get("ifs", [])
        assert len(ifs) == 2

    def test_group_by_with_tumble(self):
        """Test GROUP BY with TUMBLE function."""
        sql = """SELECT
            category,
            COUNT(*) as cnt
        FROM events
        GROUP BY category, TUMBLE(event_time, INTERVAL '1' HOUR)"""
        parsed = sqlglot.parse_one(sql)

        # Check GROUP BY exists
        group = parsed.args.get("group")
        assert group is not None
        # Should have 2 group by expressions
        assert len(group.expressions) == 2


class TestTypeInferenceFromSchema:
    """Test type inference with schema context."""

    @staticmethod
    def _create_test_project(sources, models):
        """Helper to create a minimal test project with required fields."""
        from streamt.core.models import KafkaConfig, ProjectInfo, RuntimeConfig, StreamtProject
        return StreamtProject(
            project=ProjectInfo(name="test_project"),
            runtime=RuntimeConfig(
                kafka=KafkaConfig(bootstrap_servers="localhost:9092")
            ),
            sources=sources,
            models=models
        )

    def test_column_type_from_schema(self):
        """Test that column types are resolved from schema context."""
        from streamt.compiler.compiler import Compiler
        from streamt.core.models import ColumnDefinition, Model, Source

        # Create a minimal project with typed columns
        project = self._create_test_project(
            sources=[
                Source(
                    name="orders",
                    topic="orders_topic",
                    columns=[
                        ColumnDefinition(name="order_id", type="INT"),
                        ColumnDefinition(name="category", type="STRING"),
                        ColumnDefinition(name="amount", type="DOUBLE"),
                    ]
                )
            ],
            models=[
                Model(
                    name="test_model",
                    sql="SELECT order_id, category, amount FROM {{ source('orders') }}"
                )
            ]
        )

        compiler = Compiler(project)
        model = project.get_model("test_model")

        # Build schema from source
        schema = compiler._build_source_schema(model)

        assert schema["order_id"] == "INT"
        assert schema["category"] == "STRING"
        assert schema["amount"] == "DOUBLE"

    def test_type_inference_with_schema(self):
        """Test full type inference with schema context."""
        from streamt.compiler.compiler import Compiler
        from streamt.core.models import ColumnDefinition, Model, Source

        project = self._create_test_project(
            sources=[
                Source(
                    name="orders",
                    topic="orders_topic",
                    columns=[
                        ColumnDefinition(name="order_id", type="INT"),
                        ColumnDefinition(name="category", type="STRING"),
                        ColumnDefinition(name="amount", type="DOUBLE"),
                    ]
                )
            ],
            models=[
                Model(
                    name="test_model",
                    sql="SELECT order_id, category, amount FROM {{ source('orders') }} WHERE amount >= 100"
                )
            ]
        )

        compiler = Compiler(project)
        model = project.get_model("test_model")

        columns_with_types = compiler._extract_select_columns_with_types(
            model.sql, model=model
        )

        assert len(columns_with_types) == 3
        assert columns_with_types[0] == ("order_id", "INT")
        assert columns_with_types[1] == ("category", "STRING")
        assert columns_with_types[2] == ("amount", "DOUBLE")

    def test_aggregate_type_inference(self):
        """Test that aggregate functions get correct types."""
        from streamt.compiler.compiler import Compiler
        from streamt.core.models import ColumnDefinition, Model, Source

        project = self._create_test_project(
            sources=[
                Source(
                    name="orders",
                    topic="orders_topic",
                    columns=[
                        ColumnDefinition(name="category", type="STRING"),
                        ColumnDefinition(name="amount", type="DOUBLE"),
                    ]
                )
            ],
            models=[
                Model(
                    name="test_model",
                    sql="""SELECT
                        category,
                        COUNT(*) as order_count,
                        SUM(amount) as total_amount
                    FROM {{ source('orders') }}
                    GROUP BY category"""
                )
            ]
        )

        compiler = Compiler(project)
        model = project.get_model("test_model")

        columns_with_types = compiler._extract_select_columns_with_types(
            model.sql, model=model
        )

        # Find each column by name
        type_map = dict(columns_with_types)

        assert type_map["category"] == "STRING"
        assert type_map["order_count"] == "BIGINT"
        assert type_map["total_amount"] == "DOUBLE"

    def test_case_when_boolean_inference(self):
        """Test CASE WHEN with TRUE/FALSE infers BOOLEAN."""
        from streamt.compiler.compiler import Compiler
        from streamt.core.models import ColumnDefinition, Model, Source

        project = self._create_test_project(
            sources=[
                Source(
                    name="orders",
                    topic="orders_topic",
                    columns=[
                        ColumnDefinition(name="amount", type="DOUBLE"),
                    ]
                )
            ],
            models=[
                Model(
                    name="test_model",
                    sql="""SELECT
                        CASE WHEN amount >= 200 THEN TRUE ELSE FALSE END as is_premium
                    FROM {{ source('orders') }}"""
                )
            ]
        )

        compiler = Compiler(project)
        model = project.get_model("test_model")

        columns_with_types = compiler._extract_select_columns_with_types(
            model.sql, model=model
        )

        assert len(columns_with_types) == 1
        assert columns_with_types[0] == ("is_premium", "BOOLEAN")
