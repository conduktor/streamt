"""Tests for FlinkDialect — TVF, WATERMARK, functions, types, CEP, temporal joins."""

from __future__ import annotations

import pytest
import sqlglot
from sqlglot import exp

from streamt.compiler.flink_dialect import FlinkDialect

# ---------------------------------------------------------------------------
# TVF windowing
# ---------------------------------------------------------------------------


class TestTVFParsing:
    """TABLE(TUMBLE/HOP/CUMULATE(...)) parse and roundtrip."""

    @pytest.mark.parametrize("tvf", ["TUMBLE", "HOP", "CUMULATE"])
    def test_tvf_parses(self, tvf):
        interval = (
            "INTERVAL '1' HOUR, INTERVAL '1' DAY"
            if tvf in ("HOP", "CUMULATE")
            else "INTERVAL '1' HOUR"
        )
        sql = (
            f"SELECT window_start, COUNT(*) "
            f"FROM TABLE({tvf}(TABLE orders, DESCRIPTOR(ts), {interval})) "
            f"GROUP BY window_start"
        )
        tree = sqlglot.parse_one(sql, dialect=FlinkDialect)
        assert isinstance(tree, exp.Select)

    def test_tumble_tvf_roundtrip_contains_tumble(self):
        sql = (
            "SELECT window_start, COUNT(*) "
            "FROM TABLE(TUMBLE(TABLE orders, DESCRIPTOR(ts), INTERVAL '10' MINUTE)) "
            "GROUP BY window_start"
        )
        tree = sqlglot.parse_one(sql, dialect=FlinkDialect)
        generated = tree.sql(dialect=FlinkDialect)
        assert "TUMBLE" in generated.upper()

    def test_session_tvf_parses(self):
        sql = (
            "SELECT window_start, user_id, COUNT(*) "
            "FROM TABLE(SESSION(TABLE clicks, DESCRIPTOR(ts), INTERVAL '30' MINUTE)) "
            "GROUP BY window_start, user_id"
        )
        tree = sqlglot.parse_one(sql, dialect=FlinkDialect)
        assert isinstance(tree, exp.Select)


# ---------------------------------------------------------------------------
# WATERMARK
# ---------------------------------------------------------------------------


class TestWatermark:
    """WATERMARK FOR col AS expr in CREATE TABLE."""

    def test_basic_watermark(self):
        sql = """CREATE TABLE events (
            id INT,
            ts TIMESTAMP(3),
            WATERMARK FOR ts AS ts - INTERVAL '5' SECOND
        )"""
        tree = sqlglot.parse_one(sql, dialect=FlinkDialect)
        wm = list(tree.find_all(exp.WatermarkColumnConstraint))
        assert len(wm) == 1
        assert wm[0].this.name == "ts"

    def test_watermark_with_source_watermark_func(self):
        sql = """CREATE TABLE events (
            id INT,
            ts TIMESTAMP(3),
            WATERMARK FOR ts AS SOURCE_WATERMARK()
        )"""
        tree = sqlglot.parse_one(sql, dialect=FlinkDialect)
        wm = list(tree.find_all(exp.WatermarkColumnConstraint))
        assert len(wm) == 1

    def test_watermark_generate_roundtrip(self):
        sql = """CREATE TABLE t (
            id INT,
            ts TIMESTAMP(3),
            WATERMARK FOR ts AS ts - INTERVAL '5' SECOND
        )"""
        tree = sqlglot.parse_one(sql, dialect=FlinkDialect)
        generated = tree.sql(dialect=FlinkDialect)
        assert "WATERMARK" in generated.upper()


# ---------------------------------------------------------------------------
# Flink functions
# ---------------------------------------------------------------------------


class TestFlinkFunctions:
    """Verify Flink-specific functions parse without error."""

    @pytest.mark.parametrize(
        "func_call",
        [
            "JSON_VALUE(col, '$.key')",
            "JSON_QUERY(col, '$.arr')",
            "JSON_EXISTS(col, '$.key')",
            "TO_TIMESTAMP_LTZ(epoch_ms, 3)",
            "CONVERT_TZ(ts_str, 'UTC', 'US/Pacific')",
            "REGEXP_EXTRACT(s, '(\\w+)', 1)",
            "SPLIT_INDEX(s, ',', 0)",
            "LISTAGG(col, ',')",
            "CARDINALITY(arr_col)",
            "ML_PREDICT(model_ref, features)",
            "IFNULL(a, b)",
            "GREATEST(a, b, c)",
            "LOG2(x)",
            "PROCTIME()",
        ],
    )
    def test_function_parses(self, func_call):
        sql = f"SELECT {func_call} FROM t"
        tree = sqlglot.parse_one(sql, dialect=FlinkDialect)
        assert isinstance(tree, exp.Select)


# ---------------------------------------------------------------------------
# Type mappings
# ---------------------------------------------------------------------------


class TestTypeMappings:
    """Generator maps sqlglot types to Flink type names."""

    def test_varchar_to_string(self):
        node = exp.DataType(this=exp.DataType.Type.VARCHAR)
        gen = FlinkDialect.Generator()
        assert gen.generate(node) == "STRING"

    def test_varbinary_to_bytes(self):
        node = exp.DataType(this=exp.DataType.Type.VARBINARY)
        gen = FlinkDialect.Generator()
        assert gen.generate(node) == "BYTES"

    def test_timestampltz(self):
        node = exp.DataType(this=exp.DataType.Type.TIMESTAMPLTZ)
        gen = FlinkDialect.Generator()
        assert gen.generate(node) == "TIMESTAMP_LTZ(3)"


# ---------------------------------------------------------------------------
# Temporal joins
# ---------------------------------------------------------------------------


class TestTemporalJoin:
    """FOR SYSTEM_TIME AS OF temporal join syntax."""

    def test_temporal_join_parses(self):
        sql = (
            "SELECT o.id, r.rate "
            "FROM orders o "
            "JOIN rates FOR SYSTEM_TIME AS OF o.proc_time AS r "
            "ON o.currency = r.currency"
        )
        # Should not raise
        tree = sqlglot.parse_one(sql, dialect=FlinkDialect)
        assert isinstance(tree, exp.Select)


# ---------------------------------------------------------------------------
# SET statements
# ---------------------------------------------------------------------------


class TestSetStatement:
    """Flink SET key = value parsing."""

    def test_set_parses(self):
        sql = "SET 'execution.runtime-mode' = 'streaming'"
        tree = sqlglot.parse_one(sql, dialect=FlinkDialect)
        assert tree is not None


# ---------------------------------------------------------------------------
# PRIMARY KEY NOT ENFORCED
# ---------------------------------------------------------------------------


class TestPrimaryKeyNotEnforced:
    """PRIMARY KEY ... NOT ENFORCED in CREATE TABLE."""

    def test_pk_not_enforced_parses(self):
        sql = """CREATE TABLE t (
            id INT,
            name STRING,
            PRIMARY KEY (id) NOT ENFORCED
        )"""
        tree = sqlglot.parse_one(sql, dialect=FlinkDialect)
        assert isinstance(tree, exp.Create)
