"""Tests for the ColumnTypeChecker (feature #72)."""

from __future__ import annotations

import tempfile
from pathlib import Path

import pytest
import yaml

from streamt.core.models import (
    ColumnDefinition,
    FromRef,
    KafkaConfig,
    Model,
    ProjectInfo,
    RuntimeConfig,
    Source,
    StreamtProject,
)
from streamt.core.parser import ProjectParser
from streamt.core.type_checker import ColumnTypeChecker, TypeCheckResult
from streamt.core.validator import ProjectValidator


def _project(sources: list[Source], models: list[Model]) -> StreamtProject:
    return StreamtProject(
        project=ProjectInfo(name="test"),
        runtime=RuntimeConfig(kafka=KafkaConfig(bootstrap_servers="localhost:9092")),
        sources=sources,
        models=models,
    )


def _source(name: str, columns: list[tuple[str, str]]) -> Source:
    return Source(
        name=name,
        topic=f"{name}_topic",
        columns=[ColumnDefinition(name=n, type=t) for n, t in columns],
    )


def _model(name: str, sql: str, columns: list[tuple[str, str]] | None = None) -> Model:
    m = Model(name=name, sql=sql)
    if columns:
        m.columns = [ColumnDefinition(name=n, type=t) for n, t in columns]
    return m


class TestColumnTypeChecker:
    """Direct tests on ColumnTypeChecker."""

    def test_no_issues_when_columns_match(self):
        src = _source("orders", [("order_id", "BIGINT"), ("amount", "DECIMAL(10,2)")])
        model = _model(
            "clean",
            'SELECT order_id, amount FROM {{ source("orders") }}',
        )
        project = _project([src], [model])
        checker = ColumnTypeChecker(project)
        issues = checker.check_model(model)
        assert issues == []

    def test_missing_source_column_detected(self):
        src = _source("orders", [("order_id", "BIGINT"), ("amount", "DECIMAL(10,2)")])
        model = _model(
            "clean",
            'SELECT order_id, nonexistent_col FROM {{ source("orders") }}',
        )
        project = _project([src], [model])
        checker = ColumnTypeChecker(project)
        issues = checker.check_model(model)
        assert len(issues) == 1
        assert issues[0].column == "nonexistent_col"
        assert issues[0].issue == "missing_column"
        assert "orders" in issues[0].source_or_model

    def test_suggestion_for_typo(self):
        src = _source("orders", [("order_id", "BIGINT"), ("amount", "DECIMAL(10,2)")])
        model = _model(
            "clean",
            'SELECT ordr_id FROM {{ source("orders") }}',
        )
        project = _project([src], [model])
        checker = ColumnTypeChecker(project)
        issues = checker.check_model(model)
        assert len(issues) == 1
        assert issues[0].suggestion == "order_id"

    def test_select_star_skips_check(self):
        src = _source("orders", [("order_id", "BIGINT")])
        model = _model(
            "clean",
            'SELECT * FROM {{ source("orders") }}',
        )
        project = _project([src], [model])
        checker = ColumnTypeChecker(project)
        issues = checker.check_model(model)
        assert issues == []

    def test_qualified_column_different_table_ignored(self):
        src_a = _source("orders", [("order_id", "BIGINT")])
        src_b = _source("users", [("user_id", "BIGINT")])
        model = _model(
            "joined",
            'SELECT orders.order_id, users.user_id '
            'FROM {{ source("orders") }} JOIN {{ source("users") }} ON 1=1',
        )
        project = _project([src_a, src_b], [model])
        checker = ColumnTypeChecker(project)
        issues = checker.check_model(model)
        assert issues == []

    def test_no_columns_declared_skips_source(self):
        src = Source(name="raw", topic="raw_topic")  # No columns
        model = _model(
            "clean",
            'SELECT whatever FROM {{ source("raw") }}',
        )
        project = _project([src], [model])
        checker = ColumnTypeChecker(project)
        issues = checker.check_model(model)
        assert issues == []

    def test_virtual_columns_ignored(self):
        src = _source("orders", [("order_id", "BIGINT")])
        model = _model(
            "windowed",
            'SELECT order_id, WINDOW_START, WINDOW_END FROM {{ source("orders") }}',
        )
        project = _project([src], [model])
        checker = ColumnTypeChecker(project)
        issues = checker.check_model(model)
        assert issues == []

    def test_missing_ref_column_detected(self):
        src = _source("orders", [("order_id", "BIGINT"), ("amount", "DECIMAL(10,2)")])
        upstream = _model(
            "orders_clean",
            'SELECT order_id FROM {{ source("orders") }}',
            columns=[("order_id", "BIGINT")],
        )
        downstream = _model(
            "enriched",
            'SELECT order_id, missing_col FROM {{ ref("orders_clean") }}',
        )
        project = _project([src], [upstream, downstream])
        checker = ColumnTypeChecker(project)
        issues = checker.check_model(downstream)
        assert len(issues) == 1
        assert issues[0].column == "missing_col"
        assert "orders_clean" in issues[0].source_or_model

    def test_model_without_sql_returns_empty(self):
        model = Model(name="passthrough")
        project = _project([], [model])
        checker = ColumnTypeChecker(project)
        issues = checker.check_model(model)
        assert issues == []

    def test_multiple_missing_columns(self):
        src = _source("orders", [("order_id", "BIGINT")])
        model = _model(
            "bad",
            'SELECT fake_a, fake_b FROM {{ source("orders") }}',
        )
        project = _project([src], [model])
        checker = ColumnTypeChecker(project)
        issues = checker.check_model(model)
        assert len(issues) == 2
        names = {i.column for i in issues}
        assert names == {"fake_a", "fake_b"}


class TestEdgeCases:
    """Edge cases for ColumnTypeChecker."""

    def test_where_clause_column_checked(self):
        src = _source("orders", [("order_id", "BIGINT"), ("status", "STRING")])
        model = _model(
            "filtered",
            'SELECT order_id FROM {{ source("orders") }} WHERE bad_status = \'active\'',
        )
        project = _project([src], [model])
        checker = ColumnTypeChecker(project)
        issues = checker.check_model(model)
        assert any(i.column == "bad_status" for i in issues)

    def test_join_condition_column_checked(self):
        src_a = _source("orders", [("order_id", "BIGINT"), ("user_id", "BIGINT")])
        src_b = _source("users", [("user_id", "BIGINT"), ("name", "STRING")])
        model = _model(
            "joined",
            'SELECT orders.order_id, users.name '
            'FROM {{ source("orders") }} AS orders '
            'JOIN {{ source("users") }} AS users '
            'ON orders.user_id = users.user_id',
        )
        project = _project([src_a, src_b], [model])
        checker = ColumnTypeChecker(project)
        issues = checker.check_model(model)
        assert issues == []

    def test_aggregate_function_arg_checked(self):
        src = _source("orders", [("order_id", "BIGINT"), ("amount", "DECIMAL(10,2)")])
        model = _model(
            "agg",
            'SELECT order_id, SUM(bad_amount) FROM {{ source("orders") }} GROUP BY order_id',
        )
        project = _project([src], [model])
        checker = ColumnTypeChecker(project)
        issues = checker.check_model(model)
        assert any(i.column == "bad_amount" for i in issues)

    def test_from_based_model_no_sql_skipped(self):
        src = _source("orders", [("order_id", "BIGINT")])
        model = Model(name="passthrough")
        model.from_ = [FromRef(source="orders")]
        project = _project([src], [model])
        checker = ColumnTypeChecker(project)
        issues = checker.check_model(model)
        assert issues == []

    def test_table_star_skips_check(self):
        src = _source("orders", [("order_id", "BIGINT")])
        model = _model(
            "star",
            'SELECT orders.* FROM {{ source("orders") }} AS orders',
        )
        project = _project([src], [model])
        checker = ColumnTypeChecker(project)
        issues = checker.check_model(model)
        assert issues == []

    def test_upstream_model_inferred_columns(self):
        """When upstream model has no declared columns, infer from SQL."""
        src = _source("orders", [("order_id", "BIGINT"), ("amount", "DECIMAL(10,2)")])
        upstream = _model(
            "orders_clean",
            'SELECT order_id, amount FROM {{ source("orders") }}',
        )
        downstream = _model(
            "enriched",
            'SELECT order_id, amount FROM {{ ref("orders_clean") }}',
        )
        project = _project([src], [upstream, downstream])
        checker = ColumnTypeChecker(project)
        issues = checker.check_model(downstream)
        assert issues == []

    def test_nonexistent_source_reference_no_crash(self):
        """Referencing a source that doesn't exist shouldn't crash the checker."""
        model = _model(
            "bad",
            'SELECT col FROM {{ source("ghost") }}',
        )
        project = _project([], [model])
        checker = ColumnTypeChecker(project)
        issues = checker.check_model(model)
        # No crash; source not found is handled by the validator, not the type checker
        assert issues == []

    def test_nonexistent_ref_no_crash(self):
        """Referencing a model that doesn't exist shouldn't crash the checker."""
        model = _model(
            "bad",
            'SELECT col FROM {{ ref("ghost") }}',
        )
        project = _project([], [model])
        checker = ColumnTypeChecker(project)
        issues = checker.check_model(model)
        assert issues == []


class TestValidatorIntegration:
    """Test that ColumnTypeChecker is wired into ProjectValidator."""

    def _create_project(self, tmpdir: str, config: dict) -> StreamtProject:
        project_path = Path(tmpdir)
        with open(project_path / "stream_project.yml", "w") as f:
            yaml.dump(config, f)
        parser = ProjectParser(project_path)
        return parser.parse()

    def test_validator_reports_column_warnings(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                "project": {"name": "test"},
                "runtime": {"kafka": {"bootstrap_servers": "localhost:9092"}},
                "sources": [
                    {
                        "name": "orders",
                        "topic": "orders_topic",
                        "columns": [
                            {"name": "order_id", "type": "BIGINT"},
                            {"name": "amount", "type": "DECIMAL(10,2)"},
                        ],
                    }
                ],
                "models": [
                    {
                        "name": "clean",
                        "sql": 'SELECT order_id, bad_col FROM {{ source("orders") }}',
                    }
                ],
            }
            project = self._create_project(tmpdir, config)
            validator = ProjectValidator(project)
            result = validator.validate()
            column_warnings = [
                w for w in result.warnings if "COLUMN_TYPE_CHECK" in w.code
            ]
            assert len(column_warnings) >= 1
            assert "bad_col" in column_warnings[0].message

    def test_validator_no_column_warnings_when_valid(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                "project": {"name": "test"},
                "runtime": {"kafka": {"bootstrap_servers": "localhost:9092"}},
                "sources": [
                    {
                        "name": "orders",
                        "topic": "orders_topic",
                        "columns": [
                            {"name": "order_id", "type": "BIGINT"},
                        ],
                    }
                ],
                "models": [
                    {
                        "name": "clean",
                        "sql": 'SELECT order_id FROM {{ source("orders") }}',
                    }
                ],
            }
            project = self._create_project(tmpdir, config)
            validator = ProjectValidator(project)
            result = validator.validate()
            column_warnings = [
                w for w in result.warnings if "COLUMN_TYPE_CHECK" in w.code
            ]
            assert column_warnings == []
