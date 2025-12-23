"""Unit tests for SQL type inference using sqlglot.

These tests validate that sqlglot can parse Flink-compatible SQL patterns
and that type inference works correctly for various expressions.
"""

import pytest
import sqlglot
from sqlglot import exp

from streamt.compiler.flink_dialect import FlinkDialect


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


class TestAdvancedFlinkSQLPatterns:
    """Battle-test sqlglot with complex real-world Flink SQL patterns.

    These tests ensure our parser handles advanced patterns found in
    production Flink deployments including temporal joins, interval joins,
    MATCH_RECOGNIZE (CEP), LAG/LEAD window functions, and JSON functions.

    Sources:
    - https://nightlies.apache.org/flink/flink-docs-master/docs/dev/table/sql/queries/joins/
    - https://nightlies.apache.org/flink/flink-docs-master/docs/dev/table/sql/queries/match_recognize/
    - https://docs.confluent.io/cloud/current/flink/reference/functions/json-functions.html
    """

    def test_temporal_join_for_system_time_as_of(self):
        """Test temporal join with FOR SYSTEM_TIME AS OF (versioned table join).

        Now supported via FlinkDialect which adds TIMESTAMP_SNAPSHOT token mapping.
        """
        sql = """SELECT
            order_id,
            price,
            orders.currency,
            conversion_rate,
            order_time
        FROM orders
        LEFT JOIN currency_rates FOR SYSTEM_TIME AS OF orders.order_time
        ON orders.currency = currency_rates.currency"""
        parsed = sqlglot.parse_one(sql, dialect=FlinkDialect)

        assert isinstance(parsed, exp.Select)
        assert len(parsed.expressions) == 5

        # Check for temporal join structure
        joins = list(parsed.find_all(exp.Join))
        assert len(joins) == 1

    def test_lookup_join_with_proctime(self):
        """Test lookup join using processing time.

        Now supported via FlinkDialect which adds TIMESTAMP_SNAPSHOT token mapping.
        """
        sql = """SELECT o.order_id, o.total, c.country, c.zip
        FROM Orders AS o
        JOIN Customers FOR SYSTEM_TIME AS OF o.proc_time AS c
        ON o.customer_id = c.id"""
        parsed = sqlglot.parse_one(sql, dialect=FlinkDialect)

        assert isinstance(parsed, exp.Select)
        joins = list(parsed.find_all(exp.Join))
        assert len(joins) == 1

    def test_interval_join(self):
        """Test interval join with BETWEEN time constraint."""
        sql = """SELECT *
        FROM Orders o, Shipments s
        WHERE o.id = s.order_id
        AND o.order_time BETWEEN s.ship_time - INTERVAL '4' HOUR AND s.ship_time"""
        parsed = sqlglot.parse_one(sql)

        assert isinstance(parsed, exp.Select)
        # Should have WHERE clause with BETWEEN
        where = parsed.args.get("where")
        assert where is not None

    def test_match_recognize_basic_pattern(self):
        """Test MATCH_RECOGNIZE for basic pattern matching (CEP).

        Now supported via FlinkDialect which adds MATCH_RECOGNIZE token.
        """
        sql = """SELECT T.aid, T.bid, T.cid
        FROM MyTable
        MATCH_RECOGNIZE (
            PARTITION BY userid
            ORDER BY proctime
            MEASURES
                A.id AS aid,
                B.id AS bid,
                C.id AS cid
            PATTERN (A B C)
            DEFINE
                A AS name = 'a',
                B AS name = 'b',
                C AS name = 'c'
        ) AS T"""
        parsed = sqlglot.parse_one(sql, dialect=FlinkDialect)

        assert isinstance(parsed, exp.Select)
        # MATCH_RECOGNIZE should be in the parsed tree
        match_recognize = list(parsed.find_all(exp.MatchRecognize))
        assert len(match_recognize) == 1

    def test_match_recognize_with_within_clause(self):
        """Test MATCH_RECOGNIZE with WITHIN time constraint for price drop detection.

        Now supported via FlinkDialect which adds MATCH_RECOGNIZE token.
        """
        sql = """SELECT *
        FROM Ticker
        MATCH_RECOGNIZE(
            PARTITION BY symbol
            ORDER BY rowtime
            MEASURES
                C.rowtime AS dropTime,
                A.price - C.price AS dropDiff
            ONE ROW PER MATCH
            AFTER MATCH SKIP PAST LAST ROW
            PATTERN (A B* C)
            DEFINE
                B AS B.price > A.price - 10,
                C AS C.price < A.price - 10
        )"""
        parsed = sqlglot.parse_one(sql, dialect=FlinkDialect)

        assert isinstance(parsed, exp.Select)
        match_recognize = list(parsed.find_all(exp.MatchRecognize))
        assert len(match_recognize) == 1

    def test_lag_window_function(self):
        """Test LAG() window function for accessing previous row values."""
        sql = """SELECT
            rowtime AS row_time,
            player_id,
            game_room_id,
            points,
            LAG(points, 1) OVER (PARTITION BY player_id ORDER BY rowtime) AS previous_points
        FROM gaming_player_activity"""
        parsed = sqlglot.parse_one(sql)

        assert len(parsed.expressions) == 5

        # Find the LAG expression
        lag_found = False
        for expr in parsed.expressions:
            if isinstance(expr, exp.Alias) and expr.alias == "previous_points":
                inner = expr.this
                # LAG is a Window function
                assert isinstance(inner, exp.Window)
                assert isinstance(inner.this, exp.Lag)
                lag_found = True

        assert lag_found, "LAG window function should be parsed"

    def test_lead_window_function(self):
        """Test LEAD() window function for accessing next row values."""
        sql = """SELECT
            event_id,
            amount,
            LEAD(amount, 1, 0) OVER (PARTITION BY user_id ORDER BY event_time) AS next_amount
        FROM events"""
        parsed = sqlglot.parse_one(sql)

        assert len(parsed.expressions) == 3

        # Find the LEAD expression
        for expr in parsed.expressions:
            if isinstance(expr, exp.Alias) and expr.alias == "next_amount":
                inner = expr.this
                assert isinstance(inner, exp.Window)
                assert isinstance(inner.this, exp.Lead)

    def test_json_value_function(self):
        """Test JSON_VALUE extraction from JSON strings."""
        sql = """SELECT
            event_id,
            JSON_VALUE(payload, '$.user.name') AS user_name
        FROM events"""
        parsed = sqlglot.parse_one(sql)

        assert len(parsed.expressions) == 2

        # JSON_VALUE is parsed as JSONExtract or Anonymous
        for expr in parsed.expressions:
            if isinstance(expr, exp.Alias) and expr.alias == "user_name":
                inner = expr.this
                # Could be JSONExtract, JSONExtractScalar, or Anonymous
                assert inner is not None

    def test_json_query_function(self):
        """Test JSON_QUERY for extracting complex JSON structures."""
        sql = """SELECT
            JSON_QUERY('{ "a": { "b": 1 } }', '$.a') AS nested_obj
        FROM events"""
        parsed = sqlglot.parse_one(sql)

        assert len(parsed.expressions) == 1
        assert parsed is not None

    def test_cumulate_window_tvf(self):
        """Test CUMULATE windowing table-valued function.

        Now supported via FlinkDialect which handles TABLE keyword in function args.
        """
        sql = """SELECT
            window_start,
            window_end,
            user_id,
            SUM(amount) AS total
        FROM TABLE(
            CUMULATE(TABLE orders, DESCRIPTOR(order_time), INTERVAL '1' HOUR, INTERVAL '1' DAY)
        )
        GROUP BY window_start, window_end, user_id"""
        parsed = sqlglot.parse_one(sql, dialect=FlinkDialect)

        assert isinstance(parsed, exp.Select)
        # Should have GROUP BY
        group = parsed.args.get("group")
        assert group is not None
        # Should have 3 group by expressions
        assert len(group.expressions) == 3

    def test_multiple_window_aggregates(self):
        """Test multiple window aggregates with different partitions."""
        sql = """SELECT
            order_id,
            category,
            amount,
            SUM(amount) OVER (PARTITION BY category ORDER BY order_time ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_total,
            AVG(amount) OVER (PARTITION BY category ORDER BY order_time ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS moving_avg,
            RANK() OVER (PARTITION BY category ORDER BY amount DESC) AS amount_rank
        FROM orders"""
        parsed = sqlglot.parse_one(sql)

        assert len(parsed.expressions) == 6

        # Count Window expressions
        window_count = 0
        for expr in parsed.expressions:
            if isinstance(expr, exp.Alias):
                if isinstance(expr.this, exp.Window):
                    window_count += 1

        assert window_count == 3, "Should have 3 window aggregates"

    def test_complex_case_with_in_clause(self):
        """Test CASE with IN clause and multiple conditions."""
        sql = """SELECT
            user_id,
            CASE
                WHEN status IN ('active', 'premium') AND region = 'US' THEN 'high_priority'
                WHEN status IN ('trial', 'basic') AND days_active > 30 THEN 'convert_target'
                WHEN status = 'churned' THEN 'win_back'
                ELSE 'standard'
            END AS segment
        FROM users"""
        parsed = sqlglot.parse_one(sql)

        assert len(parsed.expressions) == 2

        case_expr = parsed.expressions[1]
        assert isinstance(case_expr, exp.Alias)
        assert isinstance(case_expr.this, exp.Case)

        # Should have 3 WHEN clauses
        ifs = case_expr.this.args.get("ifs", [])
        assert len(ifs) == 3

    def test_subquery_in_select(self):
        """Test correlated subquery in SELECT clause."""
        sql = """SELECT
            o.order_id,
            o.customer_id,
            (SELECT MAX(amount) FROM orders o2 WHERE o2.customer_id = o.customer_id) AS max_order_amount
        FROM orders o"""
        parsed = sqlglot.parse_one(sql)

        assert len(parsed.expressions) == 3

        # Third expression should be a subquery
        subq_alias = parsed.expressions[2]
        assert isinstance(subq_alias, exp.Alias)
        assert isinstance(subq_alias.this, exp.Subquery)

    def test_cte_with_recursive_pattern(self):
        """Test Common Table Expression (WITH clause)."""
        sql = """WITH ranked_orders AS (
            SELECT
                order_id,
                customer_id,
                amount,
                ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY amount DESC) AS rn
            FROM orders
        )
        SELECT order_id, customer_id, amount
        FROM ranked_orders
        WHERE rn = 1"""
        parsed = sqlglot.parse_one(sql)

        assert isinstance(parsed, exp.Select)
        # WITH is accessible via find - it's part of the tree structure
        ctes = list(parsed.find_all(exp.CTE))
        assert len(ctes) == 1

    def test_union_all_streaming(self):
        """Test UNION ALL for combining multiple streams."""
        sql = """SELECT event_id, 'click' AS event_type, user_id, event_time
        FROM click_events
        UNION ALL
        SELECT event_id, 'view' AS event_type, user_id, event_time
        FROM view_events
        UNION ALL
        SELECT event_id, 'purchase' AS event_type, user_id, event_time
        FROM purchase_events"""
        parsed = sqlglot.parse_one(sql)

        # Should be a Union expression
        assert isinstance(parsed, exp.Union)

    def test_lateral_table_function(self):
        """Test LATERAL TABLE for table-generating functions."""
        sql = """SELECT
            user_id,
            tag
        FROM users,
        LATERAL TABLE(SPLIT(tags, ',')) AS T(tag)"""
        parsed = sqlglot.parse_one(sql)

        assert isinstance(parsed, exp.Select)
        assert len(parsed.expressions) == 2

    def test_distinct_on_streaming(self):
        """Test SELECT DISTINCT for deduplication."""
        sql = """SELECT DISTINCT
            user_id,
            FIRST_VALUE(event_type) OVER (PARTITION BY user_id ORDER BY event_time) AS first_event
        FROM events"""
        parsed = sqlglot.parse_one(sql)

        assert isinstance(parsed, exp.Select)
        # Check DISTINCT modifier
        assert parsed.args.get("distinct") is not None


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


class TestFlinkDialectTypeInference:
    """Test type inference using FlinkDialect for Flink-specific SQL patterns.

    These tests validate that the compiler correctly infers types when using
    FlinkDialect to parse advanced Flink SQL syntax.
    """

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

    def test_cast_to_timestamp(self):
        """Test CAST to TIMESTAMP type inference."""
        from streamt.compiler.compiler import Compiler
        from streamt.core.models import ColumnDefinition, Model, Source

        project = self._create_test_project(
            sources=[
                Source(
                    name="events",
                    topic="events_topic",
                    columns=[
                        ColumnDefinition(name="event_id", type="INT"),
                        ColumnDefinition(name="event_time_str", type="STRING"),
                    ]
                )
            ],
            models=[
                Model(
                    name="test_model",
                    sql="""SELECT
                        event_id,
                        CAST(event_time_str AS TIMESTAMP(3)) AS event_time
                    FROM {{ source('events') }}"""
                )
            ]
        )

        compiler = Compiler(project)
        model = project.get_model("test_model")
        columns_with_types = compiler._extract_select_columns_with_types(
            model.sql, model=model
        )

        type_map = dict(columns_with_types)
        assert type_map["event_id"] == "INT"
        assert type_map["event_time"] == "TIMESTAMP(3)"

    def test_cast_to_decimal(self):
        """Test CAST to DECIMAL with precision and scale."""
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
                        CAST(amount AS DECIMAL(10, 2)) AS precise_amount
                    FROM {{ source('orders') }}"""
                )
            ]
        )

        compiler = Compiler(project)
        model = project.get_model("test_model")
        columns_with_types = compiler._extract_select_columns_with_types(
            model.sql, model=model
        )

        type_map = dict(columns_with_types)
        assert type_map["precise_amount"] == "DECIMAL(10, 2)"

    def test_tumble_window_time_attributes(self):
        """Test TUMBLE_START and TUMBLE_END return TIMESTAMP(3)."""
        from streamt.compiler.compiler import Compiler
        from streamt.core.models import ColumnDefinition, Model, Source

        project = self._create_test_project(
            sources=[
                Source(
                    name="orders",
                    topic="orders_topic",
                    columns=[
                        ColumnDefinition(name="order_id", type="INT"),
                        ColumnDefinition(name="order_time", type="TIMESTAMP(3)"),
                        ColumnDefinition(name="amount", type="DOUBLE"),
                    ]
                )
            ],
            models=[
                Model(
                    name="test_model",
                    sql="""SELECT
                        TUMBLE_START(order_time, INTERVAL '10' MINUTE) AS window_start,
                        TUMBLE_END(order_time, INTERVAL '10' MINUTE) AS window_end,
                        SUM(amount) AS total
                    FROM {{ source('orders') }}
                    GROUP BY TUMBLE(order_time, INTERVAL '10' MINUTE)"""
                )
            ]
        )

        compiler = Compiler(project)
        model = project.get_model("test_model")
        columns_with_types = compiler._extract_select_columns_with_types(
            model.sql, model=model
        )

        type_map = dict(columns_with_types)
        assert type_map["window_start"] == "TIMESTAMP(3)"
        assert type_map["window_end"] == "TIMESTAMP(3)"
        assert type_map["total"] == "DOUBLE"

    def test_hop_window_time_attributes(self):
        """Test HOP_START and HOP_END return TIMESTAMP(3)."""
        from streamt.compiler.compiler import Compiler
        from streamt.core.models import ColumnDefinition, Model, Source

        project = self._create_test_project(
            sources=[
                Source(
                    name="orders",
                    topic="orders_topic",
                    columns=[
                        ColumnDefinition(name="order_time", type="TIMESTAMP(3)"),
                        ColumnDefinition(name="amount", type="DOUBLE"),
                    ]
                )
            ],
            models=[
                Model(
                    name="test_model",
                    sql="""SELECT
                        HOP_START(order_time, INTERVAL '5' MINUTE, INTERVAL '10' MINUTE) AS window_start,
                        HOP_END(order_time, INTERVAL '5' MINUTE, INTERVAL '10' MINUTE) AS window_end,
                        AVG(amount) AS avg_amount
                    FROM {{ source('orders') }}
                    GROUP BY HOP(order_time, INTERVAL '5' MINUTE, INTERVAL '10' MINUTE)"""
                )
            ]
        )

        compiler = Compiler(project)
        model = project.get_model("test_model")
        columns_with_types = compiler._extract_select_columns_with_types(
            model.sql, model=model
        )

        type_map = dict(columns_with_types)
        assert type_map["window_start"] == "TIMESTAMP(3)"
        assert type_map["window_end"] == "TIMESTAMP(3)"
        assert type_map["avg_amount"] == "DOUBLE"

    def test_coalesce_type_inference(self):
        """Test COALESCE inherits type from first non-null argument."""
        from streamt.compiler.compiler import Compiler
        from streamt.core.models import ColumnDefinition, Model, Source

        project = self._create_test_project(
            sources=[
                Source(
                    name="users",
                    topic="users_topic",
                    columns=[
                        ColumnDefinition(name="user_id", type="INT"),
                        ColumnDefinition(name="nickname", type="STRING"),
                        ColumnDefinition(name="full_name", type="STRING"),
                    ]
                )
            ],
            models=[
                Model(
                    name="test_model",
                    sql="""SELECT
                        user_id,
                        COALESCE(nickname, full_name, 'Anonymous') AS display_name
                    FROM {{ source('users') }}"""
                )
            ]
        )

        compiler = Compiler(project)
        model = project.get_model("test_model")
        columns_with_types = compiler._extract_select_columns_with_types(
            model.sql, model=model
        )

        type_map = dict(columns_with_types)
        assert type_map["user_id"] == "INT"
        assert type_map["display_name"] == "STRING"

    def test_if_function_type_inference(self):
        """Test IF function inherits type from THEN branch."""
        from streamt.compiler.compiler import Compiler
        from streamt.core.models import ColumnDefinition, Model, Source

        project = self._create_test_project(
            sources=[
                Source(
                    name="orders",
                    topic="orders_topic",
                    columns=[
                        ColumnDefinition(name="amount", type="DOUBLE"),
                        ColumnDefinition(name="discount", type="DOUBLE"),
                    ]
                )
            ],
            models=[
                Model(
                    name="test_model",
                    sql="""SELECT
                        IF(amount > 100, amount * 0.9, amount) AS final_price
                    FROM {{ source('orders') }}"""
                )
            ]
        )

        compiler = Compiler(project)
        model = project.get_model("test_model")
        columns_with_types = compiler._extract_select_columns_with_types(
            model.sql, model=model
        )

        type_map = dict(columns_with_types)
        assert type_map["final_price"] == "DOUBLE"

    def test_min_max_type_preserves_column_type(self):
        """Test MIN/MAX preserve the column type."""
        from streamt.compiler.compiler import Compiler
        from streamt.core.models import ColumnDefinition, Model, Source

        project = self._create_test_project(
            sources=[
                Source(
                    name="events",
                    topic="events_topic",
                    columns=[
                        ColumnDefinition(name="event_time", type="TIMESTAMP(3)"),
                        ColumnDefinition(name="value", type="INT"),
                    ]
                )
            ],
            models=[
                Model(
                    name="test_model",
                    sql="""SELECT
                        MIN(event_time) AS first_event,
                        MAX(event_time) AS last_event,
                        MIN(value) AS min_value,
                        MAX(value) AS max_value
                    FROM {{ source('events') }}"""
                )
            ]
        )

        compiler = Compiler(project)
        model = project.get_model("test_model")
        columns_with_types = compiler._extract_select_columns_with_types(
            model.sql, model=model
        )

        type_map = dict(columns_with_types)
        assert type_map["first_event"] == "TIMESTAMP(3)"
        assert type_map["last_event"] == "TIMESTAMP(3)"
        assert type_map["min_value"] == "INT"
        assert type_map["max_value"] == "INT"

    def test_concat_returns_string(self):
        """Test CONCAT always returns STRING."""
        from streamt.compiler.compiler import Compiler
        from streamt.core.models import ColumnDefinition, Model, Source

        project = self._create_test_project(
            sources=[
                Source(
                    name="users",
                    topic="users_topic",
                    columns=[
                        ColumnDefinition(name="first_name", type="STRING"),
                        ColumnDefinition(name="last_name", type="STRING"),
                    ]
                )
            ],
            models=[
                Model(
                    name="test_model",
                    sql="""SELECT
                        CONCAT(first_name, ' ', last_name) AS full_name
                    FROM {{ source('users') }}"""
                )
            ]
        )

        compiler = Compiler(project)
        model = project.get_model("test_model")
        columns_with_types = compiler._extract_select_columns_with_types(
            model.sql, model=model
        )

        type_map = dict(columns_with_types)
        assert type_map["full_name"] == "STRING"

    def test_arithmetic_type_promotion(self):
        """Test arithmetic operations follow type promotion rules."""
        from streamt.compiler.compiler import Compiler
        from streamt.core.models import ColumnDefinition, Model, Source

        project = self._create_test_project(
            sources=[
                Source(
                    name="metrics",
                    topic="metrics_topic",
                    columns=[
                        ColumnDefinition(name="int_val", type="INT"),
                        ColumnDefinition(name="bigint_val", type="BIGINT"),
                        ColumnDefinition(name="double_val", type="DOUBLE"),
                    ]
                )
            ],
            models=[
                Model(
                    name="test_model",
                    sql="""SELECT
                        int_val + int_val AS int_sum,
                        int_val + bigint_val AS bigint_sum,
                        int_val * double_val AS double_product
                    FROM {{ source('metrics') }}"""
                )
            ]
        )

        compiler = Compiler(project)
        model = project.get_model("test_model")
        columns_with_types = compiler._extract_select_columns_with_types(
            model.sql, model=model
        )

        type_map = dict(columns_with_types)
        assert type_map["int_sum"] == "INT"
        assert type_map["bigint_sum"] == "BIGINT"
        assert type_map["double_product"] == "DOUBLE"

    def test_window_rank_functions_return_bigint(self):
        """Test ROW_NUMBER, RANK, DENSE_RANK return BIGINT."""
        from streamt.compiler.compiler import Compiler
        from streamt.core.models import ColumnDefinition, Model, Source

        project = self._create_test_project(
            sources=[
                Source(
                    name="orders",
                    topic="orders_topic",
                    columns=[
                        ColumnDefinition(name="customer_id", type="INT"),
                        ColumnDefinition(name="amount", type="DOUBLE"),
                    ]
                )
            ],
            models=[
                Model(
                    name="test_model",
                    sql="""SELECT
                        customer_id,
                        amount,
                        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY amount DESC) AS rn,
                        RANK() OVER (PARTITION BY customer_id ORDER BY amount DESC) AS rnk,
                        DENSE_RANK() OVER (PARTITION BY customer_id ORDER BY amount DESC) AS dense_rnk
                    FROM {{ source('orders') }}"""
                )
            ]
        )

        compiler = Compiler(project)
        model = project.get_model("test_model")
        columns_with_types = compiler._extract_select_columns_with_types(
            model.sql, model=model
        )

        type_map = dict(columns_with_types)
        assert type_map["rn"] == "BIGINT"
        assert type_map["rnk"] == "BIGINT"
        assert type_map["dense_rnk"] == "BIGINT"

    def test_lag_lead_preserve_column_type(self):
        """Test LAG/LEAD preserve the column type."""
        from streamt.compiler.compiler import Compiler
        from streamt.core.models import ColumnDefinition, Model, Source

        project = self._create_test_project(
            sources=[
                Source(
                    name="events",
                    topic="events_topic",
                    columns=[
                        ColumnDefinition(name="user_id", type="INT"),
                        ColumnDefinition(name="value", type="DOUBLE"),
                        ColumnDefinition(name="event_time", type="TIMESTAMP(3)"),
                    ]
                )
            ],
            models=[
                Model(
                    name="test_model",
                    sql="""SELECT
                        user_id,
                        value,
                        LAG(value, 1) OVER (PARTITION BY user_id ORDER BY event_time) AS prev_value,
                        LEAD(value, 1) OVER (PARTITION BY user_id ORDER BY event_time) AS next_value
                    FROM {{ source('events') }}"""
                )
            ]
        )

        compiler = Compiler(project)
        model = project.get_model("test_model")
        columns_with_types = compiler._extract_select_columns_with_types(
            model.sql, model=model
        )

        type_map = dict(columns_with_types)
        assert type_map["prev_value"] == "DOUBLE"
        assert type_map["next_value"] == "DOUBLE"

    def test_temporal_join_type_inference(self):
        """Test type inference for temporal join with FOR SYSTEM_TIME AS OF."""
        # This tests that FlinkDialect can parse temporal joins
        sql = """SELECT
            o.order_id,
            o.amount,
            r.rate,
            o.amount * r.rate AS converted_amount
        FROM orders o
        LEFT JOIN currency_rates FOR SYSTEM_TIME AS OF o.order_time r
        ON o.currency = r.currency"""

        parsed = sqlglot.parse_one(sql, dialect=FlinkDialect)
        assert isinstance(parsed, exp.Select)
        assert len(parsed.expressions) == 4

        # Verify the join is parsed correctly
        joins = list(parsed.find_all(exp.Join))
        assert len(joins) == 1

    def test_cumulate_tvf_parsing_with_dialect(self):
        """Test CUMULATE TVF parsing with FlinkDialect."""
        sql = """SELECT
            window_start,
            window_end,
            user_id,
            SUM(amount) AS total
        FROM TABLE(
            CUMULATE(TABLE orders, DESCRIPTOR(order_time), INTERVAL '1' HOUR, INTERVAL '1' DAY)
        )
        GROUP BY window_start, window_end, user_id"""

        parsed = sqlglot.parse_one(sql, dialect=FlinkDialect)
        assert isinstance(parsed, exp.Select)
        assert len(parsed.expressions) == 4

    def test_match_recognize_parsing_with_dialect(self):
        """Test MATCH_RECOGNIZE parsing with FlinkDialect."""
        sql = """SELECT T.start_price, T.end_price
        FROM stock_ticks
        MATCH_RECOGNIZE (
            PARTITION BY symbol
            ORDER BY event_time
            MEASURES
                A.price AS start_price,
                LAST(B.price) AS end_price
            PATTERN (A B+)
            DEFINE
                A AS A.price > 10,
                B AS B.price > A.price
        ) AS T"""

        parsed = sqlglot.parse_one(sql, dialect=FlinkDialect)
        assert isinstance(parsed, exp.Select)

        # Verify MATCH_RECOGNIZE is in the tree
        match_recognize = list(parsed.find_all(exp.MatchRecognize))
        assert len(match_recognize) == 1

    def test_complex_nested_type_inference(self):
        """Test type inference with nested expressions."""
        from streamt.compiler.compiler import Compiler
        from streamt.core.models import ColumnDefinition, Model, Source

        project = self._create_test_project(
            sources=[
                Source(
                    name="events",
                    topic="events_topic",
                    columns=[
                        ColumnDefinition(name="event_type", type="STRING"),
                        ColumnDefinition(name="value", type="DOUBLE"),
                        ColumnDefinition(name="quantity", type="INT"),
                    ]
                )
            ],
            models=[
                Model(
                    name="test_model",
                    sql="""SELECT
                        CASE
                            WHEN event_type = 'sale' THEN value * quantity
                            WHEN event_type = 'refund' THEN -1 * value * quantity
                            ELSE 0.0
                        END AS net_amount,
                        COALESCE(
                            CASE WHEN value > 100 THEN 'high' ELSE NULL END,
                            CASE WHEN value > 50 THEN 'medium' ELSE 'low' END
                        ) AS value_tier
                    FROM {{ source('events') }}"""
                )
            ]
        )

        compiler = Compiler(project)
        model = project.get_model("test_model")
        columns_with_types = compiler._extract_select_columns_with_types(
            model.sql, model=model
        )

        type_map = dict(columns_with_types)
        assert type_map["net_amount"] == "DOUBLE"
        assert type_map["value_tier"] == "STRING"

    def test_to_timestamp_returns_timestamp(self):
        """Test TO_TIMESTAMP function returns TIMESTAMP type."""
        from streamt.compiler.compiler import Compiler
        from streamt.core.models import ColumnDefinition, Model, Source

        project = self._create_test_project(
            sources=[
                Source(
                    name="logs",
                    topic="logs_topic",
                    columns=[
                        ColumnDefinition(name="log_time_str", type="STRING"),
                        ColumnDefinition(name="message", type="STRING"),
                    ]
                )
            ],
            models=[
                Model(
                    name="test_model",
                    sql="""SELECT
                        TO_TIMESTAMP(log_time_str) AS log_time,
                        message
                    FROM {{ source('logs') }}"""
                )
            ]
        )

        compiler = Compiler(project)
        model = project.get_model("test_model")
        columns_with_types = compiler._extract_select_columns_with_types(
            model.sql, model=model
        )

        type_map = dict(columns_with_types)
        assert type_map["log_time"] == "TIMESTAMP(3)"
        assert type_map["message"] == "STRING"

    def test_string_functions_return_string(self):
        """Test string manipulation functions return STRING."""
        from streamt.compiler.compiler import Compiler
        from streamt.core.models import ColumnDefinition, Model, Source

        project = self._create_test_project(
            sources=[
                Source(
                    name="users",
                    topic="users_topic",
                    columns=[
                        ColumnDefinition(name="name", type="STRING"),
                        ColumnDefinition(name="email", type="STRING"),
                    ]
                )
            ],
            models=[
                Model(
                    name="test_model",
                    sql="""SELECT
                        UPPER(name) AS upper_name,
                        LOWER(email) AS lower_email,
                        SUBSTRING(name, 1, 10) AS short_name,
                        TRIM(name) AS trimmed_name
                    FROM {{ source('users') }}"""
                )
            ]
        )

        compiler = Compiler(project)
        model = project.get_model("test_model")
        columns_with_types = compiler._extract_select_columns_with_types(
            model.sql, model=model
        )

        type_map = dict(columns_with_types)
        assert type_map["upper_name"] == "STRING"
        assert type_map["lower_email"] == "STRING"
        assert type_map["short_name"] == "STRING"
        assert type_map["trimmed_name"] == "STRING"

    def test_comparison_returns_boolean(self):
        """Test comparison expressions return BOOLEAN."""
        from streamt.compiler.compiler import Compiler
        from streamt.core.models import ColumnDefinition, Model, Source

        project = self._create_test_project(
            sources=[
                Source(
                    name="orders",
                    topic="orders_topic",
                    columns=[
                        ColumnDefinition(name="amount", type="DOUBLE"),
                        ColumnDefinition(name="status", type="STRING"),
                    ]
                )
            ],
            models=[
                Model(
                    name="test_model",
                    sql="""SELECT
                        amount > 100 AS is_large,
                        status = 'completed' AS is_completed,
                        status IN ('pending', 'processing') AS is_in_progress
                    FROM {{ source('orders') }}"""
                )
            ]
        )

        compiler = Compiler(project)
        model = project.get_model("test_model")
        columns_with_types = compiler._extract_select_columns_with_types(
            model.sql, model=model
        )

        type_map = dict(columns_with_types)
        assert type_map["is_large"] == "BOOLEAN"
        assert type_map["is_completed"] == "BOOLEAN"
        assert type_map["is_in_progress"] == "BOOLEAN"
