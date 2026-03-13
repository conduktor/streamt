"""Tests for type-preserving masking expressions."""

from __future__ import annotations

from streamt.compiler.compiler import Compiler
from streamt.compiler.masking import apply_masking_to_sql, build_mask_expression
from streamt.core.models import (
    ColumnDefinition,
    FlinkClusterConfig,
    FlinkConfig,
    KafkaConfig,
    Model,
    ProjectInfo,
    RuntimeConfig,
    SecurityPolicies,
    Source,
    StreamtProject,
)


class TestBuildMaskExpression:
    """Unit tests for build_mask_expression."""

    # --- String types: use standard mask functions ---

    def test_hash_on_string_uses_md5(self):
        assert build_mask_expression("name", "hash", "STRING") == "MD5(name)"

    def test_redact_on_string_uses_regexp_replace(self):
        assert build_mask_expression("name", "redact", "STRING") == "REGEXP_REPLACE(name)"

    def test_partial_on_string_uses_regexp_replace(self):
        assert build_mask_expression("name", "partial", "STRING") == "REGEXP_REPLACE(name)"

    def test_null_on_string_casts_null(self):
        assert build_mask_expression("name", "null", "STRING") == "CAST(NULL AS STRING)"

    def test_hash_on_varchar(self):
        assert build_mask_expression("name", "hash", "VARCHAR(100)") == "MD5(name)"

    # --- Non-string types: type-preserving ---

    def test_hash_on_bigint_uses_hash_code(self):
        result = build_mask_expression("id", "hash", "BIGINT")
        assert result == "CAST(ABS(HASH_CODE(CAST(id AS STRING))) AS BIGINT)"

    def test_hash_on_decimal(self):
        result = build_mask_expression("amount", "hash", "DECIMAL(10,2)")
        assert result == "CAST(ABS(HASH_CODE(CAST(amount AS STRING))) AS DECIMAL(10,2))"

    def test_redact_on_bigint_nulls(self):
        result = build_mask_expression("id", "redact", "BIGINT")
        assert result == "CAST(NULL AS BIGINT)"

    def test_partial_on_int_nulls(self):
        result = build_mask_expression("count", "partial", "INT")
        assert result == "CAST(NULL AS INT)"

    def test_null_on_bigint(self):
        result = build_mask_expression("id", "null", "BIGINT")
        assert result == "CAST(NULL AS BIGINT)"

    def test_null_on_timestamp(self):
        result = build_mask_expression("ts", "null", "TIMESTAMP(3)")
        assert result == "CAST(NULL AS TIMESTAMP(3))"

    def test_redact_on_boolean_nulls(self):
        result = build_mask_expression("active", "redact", "BOOLEAN")
        assert result == "CAST(NULL AS BOOLEAN)"


class TestApplyMaskingToSql:
    """Tests for AST-based SQL masking."""

    def test_masks_column_in_select_not_where(self):
        sql = "SELECT name, age FROM users WHERE name = 'test'"
        masks = [{"column": "name", "method": "hash"}]
        schema = {"name": "STRING", "age": "INT"}
        result = apply_masking_to_sql(sql, masks, schema)
        # SELECT should have MD5(name), WHERE should still have plain name
        assert "MD5(name)" in result
        assert "WHERE name" in result or "WHERE `name`" in result

    def test_masks_column_in_select_not_string_literal(self):
        sql = "SELECT name, status FROM users WHERE status = 'name'"
        masks = [{"column": "name", "method": "hash"}]
        schema = {"name": "STRING", "status": "STRING"}
        result = apply_masking_to_sql(sql, masks, schema)
        assert "MD5(name)" in result
        # The string literal 'name' should NOT be replaced
        assert "'name'" in result

    def test_multiple_columns_masked(self):
        sql = "SELECT name, email, age FROM users"
        masks = [
            {"column": "name", "method": "hash"},
            {"column": "email", "method": "redact"},
        ]
        schema = {"name": "STRING", "email": "STRING", "age": "INT"}
        result = apply_masking_to_sql(sql, masks, schema)
        assert "MD5(name)" in result
        assert "REGEXP_REPLACE(email)" in result
        assert "age" in result

    def test_non_string_type_preserving_mask(self):
        sql = "SELECT user_id, name FROM users"
        masks = [{"column": "user_id", "method": "hash"}]
        schema = {"user_id": "BIGINT", "name": "STRING"}
        result = apply_masking_to_sql(sql, masks, schema)
        assert "HASH_CODE" in result
        assert "BIGINT" in result

    def test_no_masks_returns_original(self):
        sql = "SELECT name FROM users"
        result = apply_masking_to_sql(sql, [], {"name": "STRING"})
        assert result == sql

    def test_unmatched_column_leaves_sql_unchanged(self):
        sql = "SELECT name FROM users"
        masks = [{"column": "nonexistent", "method": "hash"}]
        schema = {"nonexistent": "STRING"}
        result = apply_masking_to_sql(sql, masks, schema)
        assert "name" in result
        assert "MD5" not in result

    def test_fallback_to_regex_on_unparseable_sql(self):
        # Garbage SQL that sqlglot can't parse - falls back to regex
        sql = "SELECT name %%% INVALID FROM users"
        masks = [{"column": "name", "method": "null"}]
        schema = {"name": "STRING"}
        result = apply_masking_to_sql(sql, masks, schema)
        # Regex fallback should still attempt replacement
        assert "CAST(NULL AS STRING)" in result


class TestCompilerMaskingTypePreservation:
    """Integration: compiler-generated DDL preserves types under masking."""

    def _compile_model(self, sources, model):
        project = StreamtProject(
            project=ProjectInfo(name="test"),
            runtime=RuntimeConfig(
                kafka=KafkaConfig(bootstrap_servers="localhost:9092"),
                flink=FlinkConfig(
                    default="local",
                    clusters={
                        "local": FlinkClusterConfig(
                            rest_url="http://localhost:8081",
                            sql_gateway_url="http://localhost:8083",
                        )
                    },
                ),
            ),
            sources=sources,
            models=[model],
        )
        compiler = Compiler(project)
        manifest = compiler.compile()
        # Find the Flink job artifact for this model
        for job in manifest.artifacts.get("flink_jobs", []):
            if model.name in str(job.get("name", "")):
                return str(job.get("sql", ""))
        return ""

    def test_hash_mask_on_bigint_preserves_type_in_ddl(self):
        source = Source(
            name="orders",
            topic="orders_topic",
            columns=[
                ColumnDefinition(name="order_id", type="BIGINT"),
                ColumnDefinition(name="amount", type="DECIMAL(10,2)"),
            ],
        )
        model = Model(
            name="masked_orders",
            sql='SELECT order_id, amount FROM {{ source("orders") }}',
            materialized="flink",
        )
        model.security = SecurityPolicies(
            policies=[{"mask": {"column": "order_id", "method": "hash"}}]
        )
        sql = self._compile_model([source], model)
        # The DDL should still have BIGINT for order_id, not STRING
        assert "`order_id` BIGINT" in sql
        # The INSERT should use HASH_CODE, not MD5
        assert "HASH_CODE" in sql
        assert "MD5" not in sql

    def test_hash_mask_on_string_uses_md5(self):
        source = Source(
            name="users",
            topic="users_topic",
            columns=[
                ColumnDefinition(name="user_id", type="BIGINT"),
                ColumnDefinition(name="email", type="STRING"),
            ],
        )
        model = Model(
            name="masked_users",
            sql='SELECT user_id, email FROM {{ source("users") }}',
            materialized="flink",
        )
        model.security = SecurityPolicies(
            policies=[{"mask": {"column": "email", "method": "hash"}}]
        )
        sql = self._compile_model([source], model)
        assert "MD5(email)" in sql
        assert "`email` STRING" in sql
