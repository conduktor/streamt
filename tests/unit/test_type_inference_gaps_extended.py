"""Type inference gaps: literals, aggregates, UDFs, timestamps, REGEXP, schema gen.

Groups 6-10, Avro schema generation, and cross-cutting scenarios.
Groups 1-5 live in ``test_type_inference_gaps.py``.
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
    UDFDeclaration,
)

# ---------------------------------------------------------------------------
# Helpers (same contract as the main gaps module)
# ---------------------------------------------------------------------------

class StubTypeInference(TypeInferenceMixin):
    def _build_source_schema(self, model):
        return {}
    _current_model = None


def _infer(sql: str, schema: dict[str, str] | None = None) -> list[tuple[str, str]]:
    stub = StubTypeInference()
    return stub._extract_select_columns_with_types(sql, schema_context=schema or {})


def _project(sources: list[Source], models: list[Model] | None = None) -> StreamtProject:
    return StreamtProject(
        project=ProjectInfo(name="test_project"),
        runtime=RuntimeConfig(kafka=KafkaConfig(bootstrap_servers="localhost:9092")),
        sources=sources,
        models=models or [],
    )


def _infer_via_compiler(sources: list[Source], model_sql: str) -> list[tuple[str, str]]:
    project = _project(
        sources=sources,
        models=[Model(name="test_model", sql=model_sql)],
    )
    compiler = Compiler(project)
    model = project.get_model("test_model")
    return compiler._extract_select_columns_with_types(model.sql, model=model)


# ===================================================================
# GROUP 6: Literal types
# ===================================================================

class TestGroup6LiteralTypes:
    """Literal values should infer to appropriate types."""

    def test_null_literal_returns_null_type(self):
        cols = _infer("SELECT NULL as x FROM t")
        assert cols[0][1] == "NULL"

    def test_integer_literal_42(self):
        cols = _infer("SELECT 42 as x FROM t")
        assert cols[0][1] == "INT"

    def test_float_literal_3_14(self):
        cols = _infer("SELECT 3.14 as x FROM t")
        assert cols[0][1] == "DOUBLE"

    def test_string_literal_hello(self):
        cols = _infer("SELECT 'hello' as x FROM t")
        assert cols[0][1] == "STRING"

    def test_boolean_true(self):
        cols = _infer("SELECT TRUE as x FROM t")
        assert cols[0][1] == "BOOLEAN"

    def test_boolean_false(self):
        cols = _infer("SELECT FALSE as x FROM t")
        assert cols[0][1] == "BOOLEAN"

    def test_negative_integer(self):
        cols = _infer("SELECT -1 as x FROM t")
        assert cols[0][1] == "INT"

    def test_negative_float(self):
        cols = _infer("SELECT -3.14 as x FROM t")
        assert cols[0][1] == "DOUBLE"

    def test_zero_literal(self):
        cols = _infer("SELECT 0 as x FROM t")
        assert cols[0][1] == "INT"

    def test_large_integer_literal(self):
        """sqlglot treats all integer literals as INT regardless of magnitude."""
        cols = _infer("SELECT 9999999999 as x FROM t")
        assert cols[0][1] == "INT"

    def test_empty_string_literal(self):
        cols = _infer("SELECT '' as x FROM t")
        assert cols[0][1] == "STRING"


# ===================================================================
# GROUP 7: Aggregate precision
# ===================================================================

class TestGroup7AggregatePrecision:

    def test_sum_int_returns_bigint(self):
        cols = _infer("SELECT SUM(int_col) AS total FROM t", {"int_col": "INT"})
        assert cols[0][1] == "BIGINT"

    def test_sum_bigint_returns_bigint(self):
        cols = _infer("SELECT SUM(bigint_col) AS total FROM t", {"bigint_col": "BIGINT"})
        assert cols[0][1] == "BIGINT"

    def test_count_star_returns_bigint(self):
        cols = _infer("SELECT COUNT(*) AS cnt FROM t")
        assert cols[0][1] == "BIGINT"

    def test_count_column_returns_bigint(self):
        cols = _infer("SELECT COUNT(x) AS cnt FROM t", {"x": "STRING"})
        assert cols[0][1] == "BIGINT"

    def test_avg_int_returns_double(self):
        cols = _infer("SELECT AVG(int_col) AS avg_val FROM t", {"int_col": "INT"})
        assert cols[0][1] == "DOUBLE"

    def test_avg_bigint_returns_double(self):
        cols = _infer("SELECT AVG(bigint_col) AS avg_val FROM t", {"bigint_col": "BIGINT"})
        assert cols[0][1] == "DOUBLE"

    def test_avg_decimal_widens_to_38(self):
        cols = _infer("SELECT AVG(x) AS avg_val FROM t", {"x": "DECIMAL(10,2)"})
        assert cols[0][1] == "DECIMAL(38,2)"

    def test_sum_decimal_widens_to_38(self):
        cols = _infer("SELECT SUM(decimal_col) AS total FROM t", {"decimal_col": "DECIMAL(10,2)"})
        assert cols[0][1] == "DECIMAL(38,2)"

    def test_sum_double_returns_double(self):
        cols = _infer("SELECT SUM(x) AS total FROM t", {"x": "DOUBLE"})
        assert cols[0][1] == "DOUBLE"

    def test_sum_float_returns_double(self):
        cols = _infer("SELECT SUM(x) AS total FROM t", {"x": "FLOAT"})
        assert cols[0][1] == "DOUBLE"

    def test_sum_tinyint_returns_bigint(self):
        cols = _infer("SELECT SUM(x) AS total FROM t", {"x": "TINYINT"})
        assert cols[0][1] == "BIGINT"

    def test_sum_smallint_returns_bigint(self):
        cols = _infer("SELECT SUM(x) AS total FROM t", {"x": "SMALLINT"})
        assert cols[0][1] == "BIGINT"

    def test_sum_unknown_column_returns_double(self):
        cols = _infer("SELECT SUM(unknown_col) AS total FROM t")
        assert cols[0][1] == "DOUBLE"

    def test_min_preserves_type(self):
        cols = _infer("SELECT MIN(x) AS min_val FROM t", {"x": "BIGINT"})
        assert cols[0][1] == "BIGINT"

    def test_max_preserves_type(self):
        cols = _infer("SELECT MAX(x) AS max_val FROM t", {"x": "DOUBLE"})
        assert cols[0][1] == "DOUBLE"

    def test_min_string_preserves_string(self):
        cols = _infer("SELECT MIN(name) AS min_name FROM t", {"name": "STRING"})
        assert cols[0][1] == "STRING"

    def test_stddev_returns_double(self):
        cols = _infer("SELECT STDDEV_POP(x) AS sd FROM t", {"x": "INT"})
        assert cols[0][1] == "DOUBLE"

    def test_variance_returns_double(self):
        cols = _infer("SELECT VAR_SAMP(x) AS v FROM t", {"x": "INT"})
        assert cols[0][1] == "DOUBLE"


# ===================================================================
# GROUP 8: UDF return types
# ===================================================================

class TestGroup8UDFReturnTypes:

    def test_unknown_udf_returns_string(self):
        cols = _infer("SELECT MY_CUSTOM_UDF(x) AS result FROM t", {"x": "INT"})
        assert cols[0][1] == "STRING"

    def test_multiple_unknown_udfs_all_string(self):
        cols = _infer(
            "SELECT MY_UDF(x) AS a, ANOTHER_FUNC(y) AS b FROM t",
            {"x": "INT", "y": "DOUBLE"},
        )
        assert cols[0][1] == "STRING"
        assert cols[1][1] == "STRING"

    def test_unknown_udf_nested_still_string(self):
        cols = _infer("SELECT OUTER_UDF(INNER_UDF(x)) AS result FROM t", {"x": "INT"})
        assert cols[0][1] == "STRING"

    def test_udf_with_declared_return_type(self):
        stub = StubTypeInference()
        stub._udf_types = {"MY_CUSTOM_UDF": "BIGINT"}
        cols = stub._extract_select_columns_with_types(
            "SELECT MY_CUSTOM_UDF(x) AS result FROM t", schema_context={"x": "INT"},
        )
        assert cols[0][1] == "BIGINT"

    def test_udf_declaration_is_case_insensitive(self):
        stub = StubTypeInference()
        stub._udf_types = {"MY_FUNC": "DOUBLE"}
        cols = stub._extract_select_columns_with_types(
            "SELECT my_func(x) AS result FROM t", schema_context={"x": "INT"},
        )
        assert cols[0][1] == "DOUBLE"

    def test_udf_via_compiler_project_config(self):
        project = _project(
            sources=[Source(name="src", topic="src_topic", columns=[
                ColumnDefinition(name="x", type="INT"),
            ])],
            models=[Model(name="m", sql="SELECT MY_AGG(x) AS result FROM {{ source('src') }}")],
        )
        project.udfs = [UDFDeclaration(name="MY_AGG", return_type="BIGINT")]
        compiler = Compiler(project)
        model = project.get_model("m")
        cols = compiler._extract_select_columns_with_types(model.sql, model=model)
        assert cols[0][1] == "BIGINT"


# ===================================================================
# GROUP 9: Timestamp precision
# ===================================================================

class TestGroup9TimestampPrecision:

    def test_current_timestamp_returns_ltz3(self):
        cols = _infer("SELECT CURRENT_TIMESTAMP AS ts FROM t")
        assert cols[0][1] == "TIMESTAMP_LTZ(3)"

    def test_source_timestamp6_preserved(self):
        cols = _infer("SELECT ts FROM t", {"ts": "TIMESTAMP(6)"})
        assert cols[0][1] == "TIMESTAMP(6)"

    def test_source_timestamp0_preserved(self):
        cols = _infer("SELECT ts FROM t", {"ts": "TIMESTAMP(0)"})
        assert cols[0][1] == "TIMESTAMP(0)"

    def test_source_timestamp3_preserved(self):
        cols = _infer("SELECT ts FROM t", {"ts": "TIMESTAMP(3)"})
        assert cols[0][1] == "TIMESTAMP(3)"

    def test_source_timestamp_ltz_preserved(self):
        cols = _infer("SELECT ts FROM t", {"ts": "TIMESTAMP_LTZ(3)"})
        assert cols[0][1] == "TIMESTAMP_LTZ(3)"

    def test_to_timestamp_returns_timestamp3(self):
        cols = _infer("SELECT TO_TIMESTAMP(s) AS ts FROM t", {"s": "STRING"})
        assert cols[0][1] == "TIMESTAMP(3)"

    def test_current_date_returns_date(self):
        cols = _infer("SELECT CURRENT_DATE AS d FROM t")
        assert cols[0][1] == "DATE"

    def test_to_timestamp_ltz_returns_ltz3(self):
        cols = _infer(
            "SELECT TO_TIMESTAMP_LTZ(epoch_ms, 3) AS ts FROM t",
            {"epoch_ms": "BIGINT"},
        )
        assert cols[0][1] == "TIMESTAMP_LTZ(3)"


# ===================================================================
# GROUP 10: REGEXP_REPLACE on typed columns
# ===================================================================

class TestGroup10RegexpReplaceOnTypedColumns:

    def test_regexp_replace_on_bigint_returns_string(self):
        cols = _infer(
            "SELECT REGEXP_REPLACE(amount, '[0-9]', 'X') AS masked FROM t",
            {"amount": "BIGINT"},
        )
        assert cols[0][1] == "STRING"

    def test_regexp_replace_on_string_returns_string(self):
        cols = _infer(
            "SELECT REGEXP_REPLACE(name, 'secret', '***') AS masked FROM t",
            {"name": "STRING"},
        )
        assert cols[0][1] == "STRING"

    def test_regexp_replace_with_cast_returns_string(self):
        cols = _infer(
            "SELECT REGEXP_REPLACE(CAST(amount AS STRING), '123', '***') AS masked FROM t",
            {"amount": "BIGINT"},
        )
        assert cols[0][1] == "STRING"

    def test_masking_should_preserve_original_type(self):
        pytest.xfail("REGEXP_REPLACE always returns STRING, masking changes column type")
        cols = _infer_via_compiler(
            sources=[
                Source(name="src", topic="src_topic", columns=[
                    ColumnDefinition(name="id", type="BIGINT"),
                ])
            ],
            model_sql=(
                "SELECT REGEXP_REPLACE(CAST(id AS STRING), '\\d', '*') AS id "
                "FROM {{ source('src') }}"
            ),
        )
        assert cols[0][1] == "BIGINT"

    def test_regexp_replace_on_decimal_returns_string(self):
        cols = _infer(
            "SELECT REGEXP_REPLACE(price, '\\.', ',') AS formatted FROM t",
            {"price": "DECIMAL(10,2)"},
        )
        assert cols[0][1] == "STRING"


# ===================================================================
# Schema generation: Avro type mapping
# ===================================================================

class TestSchemaGenerationColumnTypes:
    """_generate_schema_from_columns hardcodes all Avro fields to
    ``["null", "string"]`` regardless of the declared Flink SQL column type.
    """

    def _schema_for(self, columns: list[ColumnDefinition]) -> dict:
        source = Source(name="src", topic="src_topic", columns=columns)
        project = _project(sources=[source])
        compiler = Compiler(project)
        return compiler._generate_schema_from_columns(source)

    def test_bigint_generates_long(self):
        schema = self._schema_for([ColumnDefinition(name="id", type="BIGINT")])
        assert schema["fields"][0]["type"] == ["null", "long"]

    def test_int_generates_int(self):
        schema = self._schema_for([ColumnDefinition(name="count", type="INT")])
        assert schema["fields"][0]["type"] == ["null", "int"]

    def test_double_generates_double(self):
        schema = self._schema_for([ColumnDefinition(name="amount", type="DOUBLE")])
        assert schema["fields"][0]["type"] == ["null", "double"]

    def test_float_generates_float(self):
        schema = self._schema_for([ColumnDefinition(name="ratio", type="FLOAT")])
        assert schema["fields"][0]["type"] == ["null", "float"]

    def test_boolean_generates_boolean(self):
        schema = self._schema_for([ColumnDefinition(name="active", type="BOOLEAN")])
        assert schema["fields"][0]["type"] == ["null", "boolean"]

    def test_timestamp_generates_logical_type(self):
        schema = self._schema_for([ColumnDefinition(name="ts", type="TIMESTAMP(3)")])
        avro_type = schema["fields"][0]["type"]
        assert isinstance(avro_type, list)
        assert avro_type[0] == "null"
        inner = avro_type[1]
        assert isinstance(inner, dict)
        assert inner.get("logicalType") in ("timestamp-millis", "timestamp-micros")

    def test_decimal_generates_decimal_logical_type(self):
        schema = self._schema_for([ColumnDefinition(name="price", type="DECIMAL(10,2)")])
        avro_type = schema["fields"][0]["type"]
        assert isinstance(avro_type, list)
        inner = avro_type[1]
        assert isinstance(inner, dict)
        assert inner.get("logicalType") == "decimal"
        assert inner.get("precision") == 10
        assert inner.get("scale") == 2

    def test_multiple_columns_typed_correctly(self):
        schema = self._schema_for([
            ColumnDefinition(name="id", type="BIGINT"),
            ColumnDefinition(name="amount", type="DOUBLE"),
            ColumnDefinition(name="active", type="BOOLEAN"),
            ColumnDefinition(name="name", type="STRING"),
        ])
        types = {f["name"]: f["type"] for f in schema["fields"]}
        assert types["id"] == ["null", "long"]
        assert types["amount"] == ["null", "double"]
        assert types["active"] == ["null", "boolean"]
        assert types["name"] == ["null", "string"]


# ===================================================================
# Cross-cutting scenarios
# ===================================================================

class TestCrossCuttingScenarios:

    def test_realistic_aggregation_query(self):
        cols = _infer(
            "SELECT "
            "  order_id, "
            "  COUNT(*) AS order_count, "
            "  SUM(amount) AS total, "
            "  AVG(amount) AS avg_amount, "
            "  MIN(amount) AS min_amount, "
            "  MAX(amount) AS max_amount, "
            "  CASE WHEN SUM(amount) > 1000 THEN TRUE ELSE FALSE END AS is_high "
            "FROM t GROUP BY order_id",
            {"order_id": "BIGINT", "amount": "DOUBLE"},
        )
        m = dict(cols)
        assert m["order_id"] == "BIGINT"
        assert m["order_count"] == "BIGINT"
        assert m["total"] == "DOUBLE"
        assert m["avg_amount"] == "DOUBLE"
        assert m["min_amount"] == "DOUBLE"
        assert m["max_amount"] == "DOUBLE"
        assert m["is_high"] == "BOOLEAN"

    def test_arithmetic_type_promotion_chain(self):
        cols = _infer(
            "SELECT "
            "  int_col + bigint_col AS sum_ib, "
            "  int_col * double_col AS mul_id, "
            "  bigint_col / int_col AS div_bi "
            "FROM t",
            {"int_col": "INT", "bigint_col": "BIGINT", "double_col": "DOUBLE"},
        )
        m = dict(cols)
        assert m["sum_ib"] == "BIGINT"
        assert m["mul_id"] == "DOUBLE"
        assert m["div_bi"] == "DOUBLE"

    def test_select_star_from_subquery(self):
        cols = _infer(
            "SELECT * FROM (SELECT id, name FROM users) sub",
            {"id": "INT", "name": "STRING"},
        )
        assert len(cols) == 2

    def test_full_pipeline_select_star_with_typed_schema(self):
        sources = [
            Source(name="events", topic="events_topic", columns=[
                ColumnDefinition(name="event_id", type="BIGINT"),
                ColumnDefinition(name="amount", type="DECIMAL(10,2)"),
                ColumnDefinition(name="created_at", type="TIMESTAMP(3)"),
                ColumnDefinition(name="is_valid", type="BOOLEAN"),
            ])
        ]
        project = _project(
            sources=sources,
            models=[Model(name="passthrough", sql="SELECT * FROM {{ source('events') }}")],
        )
        compiler = Compiler(project)
        model = project.get_model("passthrough")
        cols = compiler._extract_select_columns_with_types(model.sql, model=model)
        assert len(cols) == 4

    def test_cast_preserves_target_type(self):
        cols = _infer(
            "SELECT "
            "  CAST(x AS BIGINT) AS big, "
            "  CAST(y AS DOUBLE) AS dbl, "
            "  CAST(z AS STRING) AS str "
            "FROM t",
            {"x": "STRING", "y": "INT", "z": "DOUBLE"},
        )
        m = dict(cols)
        assert m["big"] == "BIGINT"
        assert m["dbl"] == "DOUBLE"
        # _normalize_cast_type now maps VARCHAR → STRING
        assert m["str"] == "STRING"

    def test_window_function_preserves_inner_type(self):
        cols = _infer(
            "SELECT SUM(amount) OVER (PARTITION BY id) AS running_total FROM t",
            {"amount": "DOUBLE", "id": "BIGINT"},
        )
        assert cols[0][1] == "DOUBLE"

    def test_lag_preserves_column_type(self):
        cols = _infer(
            "SELECT LAG(amount) OVER (ORDER BY ts) AS prev_amount FROM t",
            {"amount": "DECIMAL(10,2)", "ts": "TIMESTAMP(3)"},
        )
        assert cols[0][1] == "DECIMAL(10,2)"

    def test_coalesce_with_column_and_literal(self):
        cols = _infer(
            "SELECT COALESCE(bigint_col, 0) AS val FROM t",
            {"bigint_col": "BIGINT"},
        )
        assert cols[0][1] == "BIGINT"

    def test_mixed_aggregates_in_single_query(self):
        cols = _infer(
            "SELECT "
            "  COUNT(*) AS cnt, "
            "  SUM(int_col) AS sum_i, "
            "  SUM(double_col) AS sum_d, "
            "  AVG(int_col) AS avg_i, "
            "  MIN(string_col) AS min_s "
            "FROM t",
            {"int_col": "INT", "double_col": "DOUBLE", "string_col": "STRING"},
        )
        m = dict(cols)
        assert m["cnt"] == "BIGINT"
        assert m["sum_i"] == "BIGINT"
        assert m["sum_d"] == "DOUBLE"
        assert m["avg_i"] == "DOUBLE"
        assert m["min_s"] == "STRING"

    def test_nested_case_with_aggregates(self):
        cols = _infer(
            "SELECT CASE WHEN COUNT(*) > 10 THEN SUM(amount) ELSE 0 END "
            "AS conditional_sum FROM t",
            {"amount": "DOUBLE"},
        )
        # SUM(DOUBLE) is DOUBLE, 0 literal is INT, merge(DOUBLE, INT) -> DOUBLE
        assert cols[0][1] == "DOUBLE"
