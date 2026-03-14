"""Tests for final 3 migration findings: INIT-4, COMPILE-4, COMPILE-5."""

from __future__ import annotations

# ---------------------------------------------------------------------------
# INIT-4: JSON Schema column extraction
# ---------------------------------------------------------------------------


class TestJsonSchemaTypeToFlink:
    """Unit tests for _json_schema_type_to_flink."""

    def setup_method(self):
        from streamt.cli.commands.init import _json_schema_type_to_flink

        self.convert = _json_schema_type_to_flink

    def test_string(self):
        assert self.convert({"type": "string"}) == "STRING"

    def test_integer(self):
        assert self.convert({"type": "integer"}) == "INT"

    def test_number(self):
        assert self.convert({"type": "number"}) == "DOUBLE"

    def test_boolean(self):
        assert self.convert({"type": "boolean"}) == "BOOLEAN"

    def test_datetime_format(self):
        assert self.convert({"type": "string", "format": "date-time"}) == "TIMESTAMP(3)"

    def test_date_format(self):
        assert self.convert({"type": "string", "format": "date"}) == "DATE"

    def test_union_type_picks_first_non_null(self):
        assert self.convert({"type": ["null", "integer"]}) == "INT"

    def test_unknown_type_defaults_string(self):
        assert self.convert({"type": "object"}) == "STRING"

    def test_missing_type_defaults_string(self):
        assert self.convert({}) == "STRING"


class TestExtractColumnsFromJsonSchema:
    """Unit tests for _extract_columns_from_json_schema."""

    def setup_method(self):
        from streamt.cli.commands.init import _extract_columns_from_json_schema

        self.extract = _extract_columns_from_json_schema

    def test_basic_properties(self):
        schema = {
            "type": "object",
            "properties": {
                "user_id": {"type": "string"},
                "age": {"type": "integer"},
                "score": {"type": "number"},
                "active": {"type": "boolean"},
            },
        }
        cols = self.extract(schema)
        assert len(cols) == 4
        by_name = {c["name"]: c for c in cols}
        assert by_name["user_id"]["type"] == "STRING"
        assert by_name["age"]["type"] == "INT"
        assert by_name["score"]["type"] == "DOUBLE"
        assert by_name["active"]["type"] == "BOOLEAN"

    def test_format_hints(self):
        schema = {
            "type": "object",
            "properties": {
                "created_at": {"type": "string", "format": "date-time"},
                "birth_date": {"type": "string", "format": "date"},
            },
        }
        cols = self.extract(schema)
        by_name = {c["name"]: c for c in cols}
        assert by_name["created_at"]["type"] == "TIMESTAMP(3)"
        assert by_name["birth_date"]["type"] == "DATE"

    def test_required_fields_marked(self):
        schema = {
            "type": "object",
            "required": ["id"],
            "properties": {
                "id": {"type": "string"},
                "name": {"type": "string"},
            },
        }
        cols = self.extract(schema)
        by_name = {c["name"]: c for c in cols}
        assert by_name["id"].get("required") is True
        assert "required" not in by_name["name"]

    def test_description_preserved(self):
        schema = {
            "type": "object",
            "properties": {
                "email": {"type": "string", "description": "User email address"},
            },
        }
        cols = self.extract(schema)
        assert cols[0]["description"] == "User email address"

    def test_empty_properties(self):
        assert self.extract({"type": "object"}) == []
        assert self.extract({}) == []

    def test_non_dict_property_skipped(self):
        schema = {
            "type": "object",
            "properties": {
                "good": {"type": "string"},
                "bad": "not-a-dict",
            },
        }
        cols = self.extract(schema)
        assert len(cols) == 1
        assert cols[0]["name"] == "good"


class TestDiscoverySchemaTypeBranching:
    """Test that _init_discover branches correctly on schema_type."""

    def test_avro_schema_extracts_columns(self):
        from streamt.cli.commands.init import _extract_columns_from_avro

        schema = {"fields": [{"name": "id", "type": "string"}]}
        cols = _extract_columns_from_avro(schema)
        assert len(cols) == 1
        assert cols[0]["name"] == "id"

    def test_json_schema_extracts_columns(self):
        from streamt.cli.commands.init import _extract_columns_from_json_schema

        schema = {
            "type": "object",
            "properties": {"id": {"type": "string"}, "count": {"type": "integer"}},
        }
        cols = _extract_columns_from_json_schema(schema)
        assert len(cols) == 2

    def test_protobuf_returns_no_columns(self):
        """Protobuf should not extract columns (no extraction function)."""
        from streamt.cli.commands import init

        # Verify the function doesn't exist for protobuf — it's a debug log no-op
        assert not hasattr(init, "_extract_columns_from_protobuf")


# ---------------------------------------------------------------------------
# COMPILE-4: Window columns require GROUP BY
# ---------------------------------------------------------------------------


class TestCheckWindowGroupBy:
    """Tests for check_window_group_by."""

    def setup_method(self):
        from streamt.core.sql_checks import check_window_group_by

        self.check = check_window_group_by

    def test_window_columns_no_group_by_warns(self):
        sql = "SELECT window_start, window_end, COUNT(*) AS cnt FROM orders"
        results = self.check(sql)
        assert len(results) == 1
        assert results[0][0] == "WINDOW_NO_GROUP_BY"

    def test_window_columns_with_group_by_ok(self):
        sql = (
            "SELECT window_start, window_end, COUNT(*) AS cnt "
            "FROM orders GROUP BY window_start, window_end"
        )
        results = self.check(sql)
        assert results == []

    def test_no_window_columns_no_warning(self):
        sql = "SELECT id, name FROM users WHERE active = true"
        results = self.check(sql)
        assert results == []

    def test_window_end_alone_warns(self):
        sql = "SELECT window_end, SUM(amount) FROM orders"
        results = self.check(sql)
        assert len(results) == 1
        assert results[0][0] == "WINDOW_NO_GROUP_BY"

    def test_window_time_column(self):
        sql = "SELECT window_time, COUNT(*) FROM events"
        results = self.check(sql)
        assert len(results) == 1

    def test_group_by_without_window_cols_warns(self):
        sql = "SELECT window_start, window_end, user_id, COUNT(*) FROM orders GROUP BY user_id"
        results = self.check(sql)
        assert len(results) == 1
        assert results[0][0] == "WINDOW_GROUP_BY_MISSING_TVF"

    def test_jinja_refs_handled(self):
        sql = (
            "SELECT window_start, window_end, COUNT(*) AS cnt "
            'FROM {{ source("orders") }} GROUP BY window_start, window_end'
        )
        results = self.check(sql)
        assert results == []

    def test_flink_tvf_with_group_by_ok(self):
        """Real Flink TABLE(TUMBLE(...)) with GROUP BY — no warning."""
        sql = """
        SELECT window_start, window_end, COUNT(*) AS cnt
        FROM TABLE(TUMBLE(TABLE orders, DESCRIPTOR(ts), INTERVAL '1' HOUR))
        GROUP BY window_start, window_end
        """
        results = self.check(sql)
        assert results == []

    def test_flink_tvf_no_group_by_warns(self):
        """Real Flink TABLE(TUMBLE(...)) without GROUP BY — should warn."""
        sql = """
        SELECT window_start, window_end, COUNT(*) AS cnt
        FROM TABLE(TUMBLE(TABLE orders, DESCRIPTOR(ts), INTERVAL '1' HOUR))
        """
        results = self.check(sql)
        assert len(results) == 1
        assert results[0][0] == "WINDOW_NO_GROUP_BY"

    def test_case_insensitive(self):
        sql = "SELECT WINDOW_START, WINDOW_END, COUNT(*) FROM events"
        results = self.check(sql)
        assert len(results) == 1

    def test_unparseable_sql_returns_empty(self):
        sql = "THIS IS NOT SQL AT ALL %%% $$"
        results = self.check(sql)
        assert results == []


# ---------------------------------------------------------------------------
# COMPILE-5: HAVING with SELECT aliases
# ---------------------------------------------------------------------------


class TestCheckHavingAliases:
    """Tests for check_having_aliases."""

    def setup_method(self):
        from streamt.core.sql_checks import check_having_aliases

        self.check = check_having_aliases

    def test_having_with_alias_warns(self):
        sql = """
        SELECT user_id, COUNT(*) AS total
        FROM orders
        GROUP BY user_id
        HAVING total > 100
        """
        results = self.check(sql)
        assert len(results) == 1
        assert results[0][0] == "HAVING_SELECT_ALIAS"
        assert "total" in results[0][1]

    def test_having_with_expression_ok(self):
        sql = """
        SELECT user_id, COUNT(*) AS total
        FROM orders
        GROUP BY user_id
        HAVING COUNT(*) > 100
        """
        results = self.check(sql)
        assert results == []

    def test_no_having_no_warning(self):
        sql = "SELECT user_id, COUNT(*) AS total FROM orders GROUP BY user_id"
        results = self.check(sql)
        assert results == []

    def test_having_with_qualified_column_ok(self):
        sql = """
        SELECT o.user_id, COUNT(*) AS total
        FROM orders o
        GROUP BY o.user_id
        HAVING o.user_id IS NOT NULL
        """
        results = self.check(sql)
        assert results == []

    def test_having_with_non_alias_column_ok(self):
        sql = """
        SELECT user_id, COUNT(*) AS total
        FROM orders
        GROUP BY user_id
        HAVING user_id IS NOT NULL
        """
        results = self.check(sql)
        assert results == []

    def test_multiple_aliases_in_having(self):
        sql = """
        SELECT user_id, COUNT(*) AS cnt, SUM(amount) AS total_amt
        FROM orders
        GROUP BY user_id
        HAVING cnt > 5 AND total_amt > 1000
        """
        results = self.check(sql)
        assert len(results) == 1
        assert "cnt" in results[0][1]
        assert "total_amt" in results[0][1]

    def test_jinja_refs_handled(self):
        sql = """
        SELECT user_id, COUNT(*) AS total
        FROM {{ ref("orders_clean") }}
        GROUP BY user_id
        HAVING total > 100
        """
        results = self.check(sql)
        assert len(results) == 1
        assert results[0][0] == "HAVING_SELECT_ALIAS"

    def test_case_insensitive_alias_match(self):
        sql = """
        SELECT user_id, COUNT(*) AS Total
        FROM orders
        GROUP BY user_id
        HAVING total > 100
        """
        results = self.check(sql)
        assert len(results) == 1

    def test_unparseable_sql_returns_empty(self):
        sql = "NOT VALID SQL %%% $$$"
        results = self.check(sql)
        assert results == []

    def test_no_aliases_no_warning(self):
        sql = """
        SELECT user_id, COUNT(*)
        FROM orders
        GROUP BY user_id
        HAVING COUNT(*) > 10
        """
        results = self.check(sql)
        assert results == []


# ---------------------------------------------------------------------------
# FlinkDialect: WATERMARK parsing
# ---------------------------------------------------------------------------


class TestWatermarkParsing:
    """Test WATERMARK FOR col AS expr parsing via FlinkDialect."""

    def test_watermark_in_create_table_parses(self):
        import sqlglot
        from sqlglot import exp

        from streamt.compiler.flink_dialect import FlinkDialect

        sql = """CREATE TABLE t (
            id INT,
            ts TIMESTAMP(3),
            WATERMARK FOR ts AS ts - INTERVAL '5' SECOND
        )"""
        tree = sqlglot.parse_one(sql, dialect=FlinkDialect)
        assert isinstance(tree, exp.Create)
        wm = list(tree.find_all(exp.WatermarkColumnConstraint))
        assert len(wm) == 1
        assert wm[0].this.name == "ts"
