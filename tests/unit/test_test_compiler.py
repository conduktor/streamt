"""TDD tests for P2: Streaming Tests → auto-compiled Flink assertion jobs.

Tests drive the design: continuous tests with not_null/range assertions compile
to Flink SQL jobs that filter violations into a __streamt_test_failures__ topic.
"""

import tempfile
from pathlib import Path

import pytest
import yaml

from streamt.core.models import StreamtProject
from streamt.core.parser import ProjectParser


def _parse(tmpdir: str, config: dict) -> StreamtProject:
    p = Path(tmpdir)
    (p / "stream_project.yml").write_text(yaml.dump(config))
    return ProjectParser(p).parse()


BASE = {
    "project": {"name": "test", "version": "1.0.0"},
    "runtime": {"kafka": {"bootstrap_servers": "localhost:9092"}},
    "sources": [{"name": "raw", "topic": "raw.v1"}],
    "models": [
        {
            "name": "payments_clean",
            "sql": 'SELECT payment_id, amount FROM {{ source("raw") }}',
        }
    ],
}


class TestTestCompilerSQL:
    """TestCompiler generates correct Flink SQL for assertions."""

    def _get_test_compiler(self):
        from streamt.compiler.test_compiler import TestJobCompiler

        return TestJobCompiler

    def test_not_null_single_column_generates_where_clause(self):
        """not_null on one column → WHERE `col` IS NULL filter in generated SQL."""
        TestJobCompiler = self._get_test_compiler()
        sql = TestJobCompiler.assertion_to_where_clause({"not_null": {"columns": ["payment_id"]}})
        assert "`payment_id` IS NULL" in sql

    def test_not_null_multiple_columns_joined_with_or(self):
        """not_null on multiple columns → multiple IS NULL conditions OR-joined."""
        TestJobCompiler = self._get_test_compiler()
        sql = TestJobCompiler.assertion_to_where_clause(
            {"not_null": {"columns": ["payment_id", "amount", "status"]}}
        )
        assert "`payment_id` IS NULL" in sql
        assert "`amount` IS NULL" in sql
        assert "`status` IS NULL" in sql
        assert " OR " in sql

    def test_range_min_only(self):
        """range with only min → WHERE col < min in generated SQL."""
        TestJobCompiler = self._get_test_compiler()
        sql = TestJobCompiler.assertion_to_where_clause({"range": {"column": "amount", "min": 0}})
        assert "amount" in sql
        assert "0" in sql

    def test_range_max_only(self):
        """range with only max → WHERE col > max."""
        TestJobCompiler = self._get_test_compiler()
        sql = TestJobCompiler.assertion_to_where_clause(
            {"range": {"column": "amount", "max": 1000000}}
        )
        assert "amount" in sql
        assert "1000000" in sql

    def test_range_min_and_max(self):
        """range with both min and max → compound condition."""
        TestJobCompiler = self._get_test_compiler()
        sql = TestJobCompiler.assertion_to_where_clause(
            {"range": {"column": "amount", "min": 0, "max": 1000000}}
        )
        assert "amount" in sql
        assert "0" in sql
        assert "1000000" in sql

    def test_multiple_assertions_combined_with_or(self):
        """Multiple assertions in a test → all conditions OR-joined in WHERE."""
        TestJobCompiler = self._get_test_compiler()
        assertions = [
            {"not_null": {"columns": ["payment_id"]}},
            {"range": {"column": "amount", "min": 0}},
        ]
        sql = TestJobCompiler.assertions_to_where_clause(assertions)
        assert "`payment_id` IS NULL" in sql
        assert "`amount`" in sql
        assert " OR " in sql

    def test_unknown_assertion_raises(self):
        """Unrecognised assertion type raises ValueError."""
        TestJobCompiler = self._get_test_compiler()
        with pytest.raises((ValueError, NotImplementedError)):
            TestJobCompiler.assertion_to_where_clause({"future_assertion": {"col": "x"}})

    def test_reserved_keyword_column_is_backtick_quoted(self):
        """Column names that are SQL reserved keywords are backtick-quoted."""
        TestJobCompiler = self._get_test_compiler()
        # 'value', 'status', 'timestamp' are Flink SQL reserved keywords
        sql = TestJobCompiler.assertion_to_where_clause(
            {"not_null": {"columns": ["value", "status", "timestamp"]}}
        )
        assert "`value` IS NULL" in sql
        assert "`status` IS NULL" in sql
        assert "`timestamp` IS NULL" in sql

    def test_range_column_is_backtick_quoted(self):
        """Range assertion column name is backtick-quoted in generated SQL."""
        TestJobCompiler = self._get_test_compiler()
        sql = TestJobCompiler.assertion_to_where_clause(
            {"range": {"column": "value", "min": 0, "max": 100}}
        )
        assert "`value` <" in sql or "`value` >" in sql

    def test_only_unsupported_assertions_returns_none(self):
        """compile_job returns None when all assertions are unsupported types."""
        from streamt.compiler.test_compiler import TestJobCompiler

        mock_test = type(
            "T",
            (),
            {
                "name": "t",
                "model": "m",
                "assertions": [{"custom_sql": {"name": "c", "where": "x > 1"}}],
            },
        )()
        result = TestJobCompiler.compile_job(mock_test)
        assert result is None

    def test_mixed_supported_unsupported_assertions_uses_supported_only(self):
        """compile_job filters out unsupported assertion types, uses supported ones."""
        from streamt.compiler.test_compiler import TestJobCompiler

        mock_test = type(
            "T",
            (),
            {
                "name": "t",
                "model": "m",
                "flink_cluster": None,
                "assertions": [
                    {"custom_sql": {"name": "c", "where": "x > 1"}},  # unsupported
                    {"not_null": {"columns": ["id"]}},  # supported
                ],
            },
        )()
        result = TestJobCompiler.compile_job(mock_test)
        assert result is not None
        assert "`id` IS NULL" in result.sql


class TestTestCompilerJobGeneration:
    """Compiler produces FlinkJobArtifact for continuous tests."""

    def test_continuous_test_produces_flink_job_artifact(self):
        """compile() generates a test job artifact for continuous tests with assertions."""
        from streamt.compiler.compiler import Compiler

        with tempfile.TemporaryDirectory() as d:
            cfg = {
                **BASE,
                "tests": [
                    {
                        "name": "check_payments",
                        "type": "continuous",
                        "model": "payments_clean",
                        "assertions": [
                            {"not_null": {"columns": ["payment_id"]}},
                            {"range": {"column": "amount", "min": 0, "max": 1000000}},
                        ],
                    }
                ],
            }
            project = _parse(d, cfg)
            compiler = Compiler(project)
            manifest = compiler.compile(dry_run=True)

            test_jobs = manifest.artifacts.get("test_jobs", [])
            assert len(test_jobs) >= 1
            job = test_jobs[0]
            name = job.get("name") if isinstance(job, dict) else job.name
            assert "check_payments" in name

    def test_test_job_sql_contains_violations_filter(self):
        """Generated test job SQL contains the assertion WHERE filter."""
        from streamt.compiler.compiler import Compiler

        with tempfile.TemporaryDirectory() as d:
            cfg = {
                **BASE,
                "tests": [
                    {
                        "name": "no_nulls",
                        "type": "continuous",
                        "model": "payments_clean",
                        "assertions": [{"not_null": {"columns": ["payment_id"]}}],
                    }
                ],
            }
            project = _parse(d, cfg)
            compiler = Compiler(project)
            manifest = compiler.compile(dry_run=True)

            test_jobs = manifest.artifacts.get("test_jobs", [])
            assert test_jobs
            job = test_jobs[0]
            sql = job.get("sql") if isinstance(job, dict) else job.sql
            assert "`payment_id` IS NULL" in sql

    def test_test_job_sql_inserts_into_failures_topic(self):
        """Generated SQL inserts violations into __streamt_test_failures__ topic."""
        from streamt.compiler.compiler import Compiler

        with tempfile.TemporaryDirectory() as d:
            cfg = {
                **BASE,
                "tests": [
                    {
                        "name": "no_nulls",
                        "type": "continuous",
                        "model": "payments_clean",
                        "assertions": [{"not_null": {"columns": ["payment_id"]}}],
                    }
                ],
            }
            project = _parse(d, cfg)
            compiler = Compiler(project)
            manifest = compiler.compile(dry_run=True)

            test_jobs = manifest.artifacts.get("test_jobs", [])
            assert test_jobs
            job = test_jobs[0]
            sql = job.get("sql") if isinstance(job, dict) else job.sql
            assert "__streamt_test_failures__" in sql

    def test_sample_test_produces_no_test_job(self):
        """type: sample tests are NOT compiled to Flink jobs (they're pull-based)."""
        from streamt.compiler.compiler import Compiler

        with tempfile.TemporaryDirectory() as d:
            cfg = {
                **BASE,
                "tests": [
                    {
                        "name": "sample_check",
                        "type": "sample",
                        "model": "payments_clean",
                        "assertions": [{"not_null": {"columns": ["payment_id"]}}],
                    }
                ],
            }
            project = _parse(d, cfg)
            compiler = Compiler(project)
            manifest = compiler.compile(dry_run=True)

            test_jobs = manifest.artifacts.get("test_jobs", [])
            assert len(test_jobs) == 0

    def test_continuous_test_without_assertions_produces_no_job(self):
        """continuous test with empty assertions list skips job generation."""
        from streamt.compiler.compiler import Compiler

        with tempfile.TemporaryDirectory() as d:
            cfg = {
                **BASE,
                "tests": [
                    {
                        "name": "empty_test",
                        "type": "continuous",
                        "model": "payments_clean",
                        "assertions": [],
                    }
                ],
            }
            project = _parse(d, cfg)
            compiler = Compiler(project)
            manifest = compiler.compile(dry_run=True)

            test_jobs = manifest.artifacts.get("test_jobs", [])
            assert len(test_jobs) == 0

    def test_test_job_name_derived_from_test_name(self):
        """The Flink job artifact name is derived from the test name."""
        from streamt.compiler.compiler import Compiler

        with tempfile.TemporaryDirectory() as d:
            cfg = {
                **BASE,
                "tests": [
                    {
                        "name": "my_quality_check",
                        "type": "continuous",
                        "model": "payments_clean",
                        "assertions": [{"not_null": {"columns": ["payment_id"]}}],
                    }
                ],
            }
            project = _parse(d, cfg)
            compiler = Compiler(project)
            manifest = compiler.compile(dry_run=True)

            test_jobs = manifest.artifacts.get("test_jobs", [])
            assert test_jobs
            job = test_jobs[0]
            name = job.get("name") if isinstance(job, dict) else job.name
            assert "my_quality_check" in name


class TestTestJobManifestSerialization:
    """test_jobs appear correctly in manifest JSON."""

    def test_test_jobs_key_in_manifest_artifacts(self):
        """manifest.artifacts has 'test_jobs' key when continuous tests exist."""
        from streamt.compiler.compiler import Compiler

        with tempfile.TemporaryDirectory() as d:
            cfg = {
                **BASE,
                "tests": [
                    {
                        "name": "check_1",
                        "type": "continuous",
                        "model": "payments_clean",
                        "assertions": [{"not_null": {"columns": ["payment_id"]}}],
                    }
                ],
            }
            project = _parse(d, cfg)
            manifest = Compiler(project).compile(dry_run=True)
            assert "test_jobs" in manifest.artifacts

    def test_manifest_to_dict_includes_test_jobs(self):
        """manifest.to_dict() serialises test_jobs."""
        from streamt.compiler.compiler import Compiler

        with tempfile.TemporaryDirectory() as d:
            cfg = {
                **BASE,
                "tests": [
                    {
                        "name": "check_1",
                        "type": "continuous",
                        "model": "payments_clean",
                        "assertions": [{"not_null": {"columns": ["payment_id"]}}],
                    }
                ],
            }
            project = _parse(d, cfg)
            manifest = Compiler(project).compile(dry_run=True)
            d_out = manifest.to_dict()
            assert "test_jobs" in d_out.get("artifacts", {})
