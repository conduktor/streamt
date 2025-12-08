"""Tests for rich error message formatting."""

import pytest

from streamt.core import errors


class TestFormatError:
    """Tests for the format_error helper."""

    def test_format_error_with_all_fields(self):
        """Test formatting with all optional fields."""
        result = errors.format_error(
            title="Test Error",
            explanation="Something went wrong.",
            suggestion="Try this fix.",
            example="some_code()",
            docs_path="reference/test",
        )

        assert "Test Error" in result
        assert "Something went wrong." in result
        assert "Try this fix." in result
        assert "    some_code()" in result  # indented
        assert f"{errors.DOCS_BASE_URL}/reference/test" in result

    def test_format_error_minimal(self):
        """Test formatting with only required fields."""
        result = errors.format_error(
            title="Test Error",
            explanation="Something went wrong.",
        )

        assert "Test Error" in result
        assert "Something went wrong." in result
        assert "Learn more:" not in result  # no docs link


class TestSuggestSimilar:
    """Tests for the suggest_similar helper."""

    def test_suggest_single_match(self):
        """Test suggestion with one close match."""
        result = errors.suggest_similar("ordres", ["orders", "products", "users"], "source")
        assert "Did you mean 'orders'?" in result

    def test_suggest_multiple_matches(self):
        """Test suggestion with multiple close matches."""
        result = errors.suggest_similar("order", ["orders", "order_items", "users"], "source")
        assert "Did you mean one of:" in result
        assert "'orders'" in result

    def test_suggest_no_close_matches_few_available(self):
        """Test when no close matches but few options available."""
        result = errors.suggest_similar("xyz", ["a", "b", "c"], "source")
        assert "Available sources:" in result
        assert "'a'" in result

    def test_suggest_no_close_matches_many_available(self):
        """Test when no close matches and many options."""
        result = errors.suggest_similar("xyz", [f"item_{i}" for i in range(10)], "source")
        assert "Available sources: 10 defined" in result

    def test_suggest_empty_available(self):
        """Test with empty available list."""
        result = errors.suggest_similar("test", [], "source")
        assert result == ""


class TestSourceNotFound:
    """Tests for source_not_found error."""

    def test_source_not_found_with_suggestion(self):
        """Test source not found with a similar name suggestion."""
        result = errors.source_not_found(
            source_name="ordres_raw",
            model_name="orders_clean",
            available_sources=["orders_raw", "payments_raw", "users_raw"],
        )

        assert "Source 'ordres_raw' not found" in result
        assert "Model 'orders_clean'" in result
        assert "orders_raw" in result  # suggestion
        assert "concepts/sources" in result  # docs link

    def test_source_not_found_without_close_match(self):
        """Test source not found when no similar names exist."""
        result = errors.source_not_found(
            source_name="xyz",
            model_name="my_model",
            available_sources=["orders", "payments"],
        )

        assert "Source 'xyz' not found" in result
        assert "Available sources:" in result


class TestModelNotFound:
    """Tests for model_not_found error."""

    def test_model_not_found_with_typo(self):
        """Test model not found with typo suggestion."""
        result = errors.model_not_found(
            model_name="ordres_clean",
            referencing_model="order_metrics",
            available_models=["orders_clean", "orders_raw", "payments"],
        )

        assert "Model 'ordres_clean' not found" in result
        assert "Model 'order_metrics'" in result
        assert "orders_clean" in result  # suggestion
        assert "concepts/models" in result


class TestGatewayRequired:
    """Tests for gateway_required error."""

    def test_gateway_required_includes_example(self):
        """Test gateway required error has config example."""
        result = errors.gateway_required("filtered_orders")

        assert "Gateway configuration required for 'filtered_orders'" in result
        assert "virtual_topic" in result
        assert "conduktor:" in result
        assert "gateway:" in result
        assert "url:" in result


class TestFlinkRequired:
    """Tests for flink_required error."""

    def test_flink_required_includes_config(self):
        """Test flink required error has config example."""
        result = errors.flink_required("order_aggregates")

        assert "Flink configuration required for 'order_aggregates'" in result
        assert "runtime:" in result
        assert "flink:" in result
        assert "rest_url:" in result
        assert "sql_gateway_url:" in result


class TestMissingSinkConfig:
    """Tests for missing_sink_config error."""

    def test_missing_sink_config_example(self):
        """Test missing sink config error has example."""
        result = errors.missing_sink_config("to_snowflake")

        assert "Sink configuration required for 'to_snowflake'" in result
        assert "sink:" in result
        assert "connector:" in result
        assert "reference/materializations#sink" in result


class TestJinjaSyntaxError:
    """Tests for jinja_syntax_error."""

    def test_jinja_syntax_error_with_sql(self):
        """Test Jinja error shows SQL snippet."""
        sql = "SELECT * FROM {{ source(\"orders\")"  # missing closing brace
        result = errors.jinja_syntax_error(
            model_name="bad_model",
            error="unexpected end of template",
            sql=sql,
        )

        assert "Jinja syntax error in model 'bad_model'" in result
        assert "unexpected end of template" in result
        assert "source(" in result  # SQL shown
        assert "Missing closing braces" in result  # suggestion

    def test_jinja_syntax_error_without_sql(self):
        """Test Jinja error without SQL snippet."""
        result = errors.jinja_syntax_error(
            model_name="bad_model",
            error="undefined variable",
        )

        assert "Jinja syntax error" in result
        assert "undefined variable" in result


class TestDuplicateName:
    """Tests for duplicate_name error."""

    def test_duplicate_name_basic(self):
        """Test duplicate name error."""
        result = errors.duplicate_name("model", "orders")

        assert "Duplicate model name 'orders'" in result
        assert "unique" in result.lower()

    def test_duplicate_name_with_locations(self):
        """Test duplicate name with file locations."""
        result = errors.duplicate_name(
            "source",
            "events",
            locations=["models/events.yml:5", "sources/events.yml:10"],
        )

        assert "Duplicate source name 'events'" in result
        assert "models/events.yml:5" in result
        assert "sources/events.yml:10" in result


class TestCycleDetected:
    """Tests for cycle_detected error."""

    def test_cycle_detected_shows_path(self):
        """Test cycle detected shows the cycle path."""
        result = errors.cycle_detected(["model_a", "model_b", "model_c", "model_a"])

        assert "Circular dependency detected" in result
        assert "model_a -> model_b -> model_c -> model_a" in result
        assert "Break the cycle" in result
        assert "concepts/dag" in result


class TestContinuousTestWithoutFlink:
    """Tests for continuous_test_without_flink error."""

    def test_continuous_test_flink_required(self):
        """Test continuous test requires Flink."""
        result = errors.continuous_test_without_flink("orders_monitoring")

        assert "Flink required for continuous test 'orders_monitoring'" in result
        assert "runtime:" in result
        assert "flink:" in result
        assert "concepts/tests#continuous-tests" in result


class TestCannotReducePartitions:
    """Tests for cannot_reduce_partitions error."""

    def test_cannot_reduce_partitions_explains_why(self):
        """Test partition reduction error explains the limitation."""
        result = errors.cannot_reduce_partitions(
            topic_name="orders.clean.v1",
            current=12,
            desired=6,
        )

        assert "Cannot reduce partitions for topic 'orders.clean.v1'" in result
        assert "12" in result
        assert "6" in result
        assert "Kafka does not support reducing" in result
        assert "ordering guarantees" in result.lower()
        assert "concepts/streaming-fundamentals" in result


class TestSchemaIncompatible:
    """Tests for schema_incompatible error."""

    def test_schema_incompatible_with_breaking_changes(self):
        """Test schema incompatibility with breaking changes listed."""
        result = errors.schema_incompatible(
            subject="orders.clean.v1-value",
            compatibility_mode="BACKWARD",
            breaking_changes=["Removed field 'customer_id'", "Changed type of 'amount'"],
        )

        assert "Schema incompatible for subject 'orders.clean.v1-value'" in result
        assert "BACKWARD" in result
        assert "Removed field 'customer_id'" in result
        assert "Changed type of 'amount'" in result
        assert "concepts/sources#schema-registry" in result

    def test_schema_incompatible_without_breaking_changes(self):
        """Test schema incompatibility without specific changes."""
        result = errors.schema_incompatible(
            subject="payments-value",
            compatibility_mode="FULL",
        )

        assert "Schema incompatible" in result
        assert "FULL" in result


class TestFlinkSqlError:
    """Tests for flink_sql_error."""

    def test_flink_sql_error_table_not_found(self):
        """Test Flink SQL error for missing table."""
        result = errors.flink_sql_error(
            error_msg="Table 'orders' not found in catalog",
            sql_snippet="SELECT * FROM orders",
        )

        assert "Flink SQL execution error" in result
        assert "Table 'orders' not found" in result
        assert "SELECT * FROM orders" in result
        assert "source/model is defined" in result.lower()

    def test_flink_sql_error_column_not_found(self):
        """Test Flink SQL error for missing column."""
        result = errors.flink_sql_error(
            error_msg="Column 'customer_id' not found in any table",
        )

        assert "Column 'customer_id' not found" in result
        assert "columns definition" in result.lower()

    def test_flink_sql_error_generic(self):
        """Test Flink SQL error with unknown error type."""
        result = errors.flink_sql_error(
            error_msg="Something unexpected happened",
        )

        assert "Flink SQL execution error" in result
        assert "Something unexpected happened" in result
        assert "Check your SQL syntax" in result


class TestSqlGatewayNotConfigured:
    """Tests for sql_gateway_not_configured error."""

    def test_sql_gateway_not_configured(self):
        """Test SQL Gateway not configured error."""
        result = errors.sql_gateway_not_configured()

        assert "SQL Gateway URL not configured" in result
        assert "sql_gateway_url" in result
        assert "rest_url" in result  # shows the distinction
        assert "reference/flink-options" in result


class TestTestModelNotFound:
    """Tests for test_model_not_found error."""

    def test_test_model_not_found_with_suggestion(self):
        """Test test model not found with suggestion."""
        result = errors.test_model_not_found(
            test_name="orders_quality",
            model_name="ordres_clean",
            available_models=["orders_clean", "orders_raw", "payments"],
        )

        assert "Model 'ordres_clean' not found for test 'orders_quality'" in result
        assert "orders_clean" in result  # suggestion
        assert "concepts/tests" in result


class TestExposureModelNotFound:
    """Tests for exposure_model_not_found error."""

    def test_exposure_model_not_found(self):
        """Test exposure model not found."""
        result = errors.exposure_model_not_found(
            exposure_name="billing_dashboard",
            model_name="billing_metrics",
            available_models=["orders_metrics", "payment_metrics"],
        )

        assert "Model 'billing_metrics' not found for exposure 'billing_dashboard'" in result
        assert "concepts/exposures" in result


class TestExposureSourceNotFound:
    """Tests for exposure_source_not_found error."""

    def test_exposure_source_not_found_with_suggestion(self):
        """Test exposure source not found with suggestion."""
        result = errors.exposure_source_not_found(
            exposure_name="data_producer",
            source_name="ordres_raw",
            available_sources=["orders_raw", "payments_raw"],
        )

        assert "Source 'ordres_raw' not found for exposure 'data_producer'" in result
        assert "orders_raw" in result  # suggestion
        assert "concepts/exposures" in result

    def test_exposure_source_not_found_no_match(self):
        """Test exposure source not found without close matches."""
        result = errors.exposure_source_not_found(
            exposure_name="data_producer",
            source_name="xyz",
            available_sources=["orders_raw", "payments_raw"],
        )

        assert "Source 'xyz' not found" in result
        assert "Available sources:" in result


class TestExposureDependencyNotFound:
    """Tests for exposure_dependency_not_found error."""

    def test_exposure_dependency_model_not_found(self):
        """Test exposure depends on non-existent model."""
        result = errors.exposure_dependency_not_found(
            exposure_name="analytics_dashboard",
            dependency_name="order_metrics",
            dependency_type="model",
            available=["orders_clean", "payments_clean"],
        )

        assert "Model 'order_metrics' not found for exposure 'analytics_dashboard'" in result
        assert "depends on model 'order_metrics'" in result
        assert "concepts/exposures" in result

    def test_exposure_dependency_source_not_found(self):
        """Test exposure depends on non-existent source."""
        result = errors.exposure_dependency_not_found(
            exposure_name="data_pipeline",
            dependency_name="events_raw",
            dependency_type="source",
            available=["orders_raw", "payments_raw"],
        )

        assert "Source 'events_raw' not found for exposure 'data_pipeline'" in result
        assert "depends on source 'events_raw'" in result


class TestInvalidStateTtl:
    """Tests for invalid_state_ttl error."""

    def test_invalid_state_ttl_negative(self):
        """Test invalid state TTL with negative value."""
        result = errors.invalid_state_ttl(
            model_name="my_aggregation",
            ttl_value=-1000,
            reason="must be a positive number",
        )

        assert "Invalid state TTL for model 'my_aggregation'" in result
        assert "-1000" in result
        assert "must be a positive number" in result
        assert "reference/flink-options#state-ttl" in result

    def test_invalid_state_ttl_includes_examples(self):
        """Test invalid state TTL error includes common configurations."""
        result = errors.invalid_state_ttl(
            model_name="test_model",
            ttl_value=0,
            reason="cannot be zero",
        )

        assert "86400000" in result  # 24 hours example
        assert "604800000" in result  # 7 days example
        assert "state_ttl_ms:" in result


class TestStateTtlRecommended:
    """Tests for state_ttl_recommended warning."""

    def test_state_ttl_recommended_for_groupby(self):
        """Test recommendation for GROUP BY operation."""
        result = errors.state_ttl_recommended(
            model_name="customer_counts",
            operation="GROUP BY",
        )

        assert "Consider adding state TTL for model 'customer_counts'" in result
        assert "GROUP BY" in result
        assert "unbounded" in result.lower()
        assert "state_ttl_ms:" in result

    def test_state_ttl_recommended_for_join(self):
        """Test recommendation for JOIN operation."""
        result = errors.state_ttl_recommended(
            model_name="order_enriched",
            operation="JOIN",
        )

        assert "JOIN" in result
        assert "concepts/streaming-fundamentals#state-management" in result


class TestAccessDenied:
    """Tests for access_denied error."""

    def test_access_denied_shows_groups(self):
        """Test access denied error shows group information."""
        result = errors.access_denied(
            referencing_model="public_dashboard",
            target_model="internal_metrics",
            target_group="finance",
        )

        assert "Access denied" in result
        assert "'public_dashboard' cannot reference 'internal_metrics'" in result
        assert "group 'finance'" in result
        assert "private" in result

    def test_access_denied_includes_options(self):
        """Test access denied error includes resolution options."""
        result = errors.access_denied(
            referencing_model="reporting",
            target_model="sensitive_data",
            target_group="security",
        )

        assert "Options:" in result
        assert "Move 'reporting' to group 'security'" in result
        assert "Change 'sensitive_data' access to 'protected'" in result
        assert "concepts/models#access-control" in result

    def test_access_denied_includes_example(self):
        """Test access denied error includes YAML examples."""
        result = errors.access_denied(
            referencing_model="my_model",
            target_model="private_model",
            target_group="internal",
        )

        assert "group: internal" in result
        assert "access: protected" in result
