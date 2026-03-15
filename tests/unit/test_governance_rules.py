"""Tests for governance rules: max_replication_factor + forbidden_suffixes."""

from __future__ import annotations

import tempfile
from pathlib import Path

import yaml

from streamt.core.parser import ProjectParser
from streamt.core.validator import ProjectValidator


def _make_project(governance: dict, models: list[dict] | None = None):
    """Create a minimal project with governance rules."""
    with tempfile.TemporaryDirectory() as tmpdir:
        config = {
            "project": {"name": "test", "version": "1.0.0"},
            "runtime": {"kafka": {"bootstrap_servers": "localhost:9092"}},
            "rules": governance,
            "sources": [{"name": "raw", "topic": "raw.v1"}],
            "models": models
            or [
                {
                    "name": "orders",
                    "sql": 'SELECT * FROM {{ source("raw") }}',
                    "topic": {"partitions": 6, "replication_factor": 3},
                }
            ],
        }
        path = Path(tmpdir)
        with open(path / "stream_project.yml", "w") as f:
            yaml.dump(config, f)
        project = ProjectParser(path).parse()
        v = ProjectValidator(project)
        return v.validate()


class TestMaxReplicationFactor:
    def test_violation(self):
        """rf=5 exceeds max=3 → error."""
        result = _make_project(
            {"topics": {"max_replication_factor": 3}},
            [
                {
                    "name": "m",
                    "sql": 'SELECT * FROM {{ source("raw") }}',
                    "topic": {"partitions": 1, "replication_factor": 5},
                }
            ],
        )
        errors = [e for e in result.errors if e.code == "RULE_MAX_REPLICATION"]
        assert len(errors) == 1
        assert "<= 3" in errors[0].message
        assert "got 5" in errors[0].message

    def test_at_limit(self):
        """rf=3 with max=3 → valid."""
        result = _make_project(
            {"topics": {"max_replication_factor": 3}},
            [
                {
                    "name": "m",
                    "sql": 'SELECT * FROM {{ source("raw") }}',
                    "topic": {"partitions": 1, "replication_factor": 3},
                }
            ],
        )
        errors = [e for e in result.errors if e.code == "RULE_MAX_REPLICATION"]
        assert len(errors) == 0

    def test_not_set(self):
        """No max_replication_factor rule → valid regardless of rf."""
        result = _make_project(
            {"topics": {"min_partitions": 1}},
            [
                {
                    "name": "m",
                    "sql": 'SELECT * FROM {{ source("raw") }}',
                    "topic": {"partitions": 6, "replication_factor": 99},
                }
            ],
        )
        errors = [e for e in result.errors if e.code == "RULE_MAX_REPLICATION"]
        assert len(errors) == 0

    def test_no_rf_declared(self):
        """Rule set but model has no rf → valid (nothing to check)."""
        result = _make_project(
            {"topics": {"max_replication_factor": 3}},
            [{"name": "m", "sql": 'SELECT * FROM {{ source("raw") }}'}],
        )
        errors = [e for e in result.errors if e.code == "RULE_MAX_REPLICATION"]
        assert len(errors) == 0


class TestForbiddenSuffixes:
    def test_violation(self):
        """Topic ending in '_test' with forbidden suffix → error."""
        result = _make_project(
            {"topics": {"forbidden_suffixes": ["_test"]}},
            [{"name": "orders_test", "sql": 'SELECT * FROM {{ source("raw") }}'}],
        )
        errors = [e for e in result.errors if e.code == "RULE_FORBIDDEN_SUFFIX"]
        assert len(errors) == 1
        assert "_test" in errors[0].message

    def test_no_match(self):
        """Topic not ending in forbidden suffix → valid."""
        result = _make_project(
            {"topics": {"forbidden_suffixes": ["_test"]}},
            [{"name": "orders_prod", "sql": 'SELECT * FROM {{ source("raw") }}'}],
        )
        errors = [e for e in result.errors if e.code == "RULE_FORBIDDEN_SUFFIX"]
        assert len(errors) == 0

    def test_multiple_suffixes(self):
        """Both suffixes checked."""
        result = _make_project(
            {"topics": {"forbidden_suffixes": ["_test", "_dev"]}},
            [{"name": "orders_dev", "sql": 'SELECT * FROM {{ source("raw") }}'}],
        )
        errors = [e for e in result.errors if e.code == "RULE_FORBIDDEN_SUFFIX"]
        assert len(errors) == 1
        assert "_dev" in errors[0].message

    def test_empty_list(self):
        """Empty forbidden_suffixes → valid."""
        result = _make_project(
            {"topics": {"forbidden_suffixes": []}},
            [{"name": "orders_test", "sql": 'SELECT * FROM {{ source("raw") }}'}],
        )
        errors = [e for e in result.errors if e.code == "RULE_FORBIDDEN_SUFFIX"]
        assert len(errors) == 0


class TestExistingRulesRegression:
    """Ensure refactored range checks still work."""

    def test_min_partitions_violation(self):
        result = _make_project(
            {"topics": {"min_partitions": 12}},
            [
                {
                    "name": "m",
                    "sql": 'SELECT * FROM {{ source("raw") }}',
                    "topic": {"partitions": 3, "replication_factor": 1},
                }
            ],
        )
        assert any(e.code == "RULE_MIN_PARTITIONS" for e in result.errors)

    def test_max_partitions_violation(self):
        result = _make_project(
            {"topics": {"max_partitions": 6}},
            [
                {
                    "name": "m",
                    "sql": 'SELECT * FROM {{ source("raw") }}',
                    "topic": {"partitions": 12, "replication_factor": 1},
                }
            ],
        )
        assert any(e.code == "RULE_MAX_PARTITIONS" for e in result.errors)

    def test_min_replication_violation(self):
        result = _make_project(
            {"topics": {"min_replication_factor": 3}},
            [
                {
                    "name": "m",
                    "sql": 'SELECT * FROM {{ source("raw") }}',
                    "topic": {"partitions": 1, "replication_factor": 1},
                }
            ],
        )
        assert any(e.code == "RULE_MIN_REPLICATION" for e in result.errors)

    def test_forbidden_prefix_still_works(self):
        result = _make_project(
            {"topics": {"forbidden_prefixes": ["_internal"]}},
            [{"name": "_internal_orders", "sql": 'SELECT * FROM {{ source("raw") }}'}],
        )
        assert any(e.code == "RULE_FORBIDDEN_PREFIX" for e in result.errors)
