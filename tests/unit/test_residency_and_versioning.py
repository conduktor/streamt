"""Tests for data residency constraints and schema versioning validation."""

from __future__ import annotations

from streamt.core.models import (
    Model,
    ProjectInfo,
    Rules,
    Source,
    StreamtProject,
)
from streamt.core.runtime import FlinkClusterConfig, FlinkConfig, KafkaConfig, RuntimeConfig
from streamt.core.validator import ProjectValidator


def _runtime() -> RuntimeConfig:
    return RuntimeConfig(
        kafka=KafkaConfig(bootstrap_servers="localhost:9092"),
        flink=FlinkConfig(
            default="local",
            clusters={"local": FlinkClusterConfig(rest_url="http://localhost:8082")},
        ),
    )


def _project(
    models: list[dict] | None = None,
    sources: list[dict] | None = None,
    rules: dict | None = None,
) -> StreamtProject:
    src_list = [Source(name="raw", topic="raw.v1", **s) for s in (sources or [{}])]
    mdl_list = [
        Model(
            name=m.get("name", "out"),
            sql=m.get("sql", "SELECT 1"),
            **{k: v for k, v in m.items() if k not in ("name", "sql")},
        )
        for m in (models or [{"name": "out", "sql": "SELECT 1"}])
    ]
    return StreamtProject(
        project=ProjectInfo(name="test"),
        runtime=_runtime(),
        rules=Rules(**rules) if rules else None,
        sources=src_list,
        models=mdl_list,
    )


class TestDataResidency:
    def test_model_in_allowed_region(self):
        p = _project(
            models=[{"name": "out", "sql": "SELECT 1", "region": "EU"}],
            rules={"data_residency": {"allowed_regions": ["EU", "US"]}},
        )
        result = ProjectValidator(p).validate()
        assert not any("RESIDENCY" in e.code for e in result.errors)

    def test_model_violates_region(self):
        p = _project(
            models=[{"name": "out", "sql": "SELECT 1", "region": "APAC"}],
            rules={"data_residency": {"allowed_regions": ["EU", "US"]}},
        )
        result = ProjectValidator(p).validate()
        errors = [e for e in result.errors if "RESIDENCY" in e.code]
        assert len(errors) == 1
        assert "APAC" in errors[0].message

    def test_source_violates_region(self):
        p = _project(
            sources=[{"region": "APAC"}],
            rules={"data_residency": {"allowed_regions": ["EU"]}},
        )
        result = ProjectValidator(p).validate()
        errors = [e for e in result.errors if "RESIDENCY" in e.code]
        assert len(errors) == 1
        assert "raw" in errors[0].message

    def test_no_region_set_is_ok(self):
        p = _project(
            models=[{"name": "out", "sql": "SELECT 1"}],
            rules={"data_residency": {"allowed_regions": ["EU"]}},
        )
        result = ProjectValidator(p).validate()
        assert not any("RESIDENCY" in e.code for e in result.errors)

    def test_no_residency_rules(self):
        p = _project(models=[{"name": "out", "sql": "SELECT 1", "region": "APAC"}])
        result = ProjectValidator(p).validate()
        assert not any("RESIDENCY" in e.code for e in result.errors)

    def test_empty_allowed_regions_allows_all(self):
        p = _project(
            models=[{"name": "out", "sql": "SELECT 1", "region": "APAC"}],
            rules={"data_residency": {"allowed_regions": []}},
        )
        result = ProjectValidator(p).validate()
        assert not any("RESIDENCY" in e.code for e in result.errors)


class TestSchemaVersioning:
    def test_sequential_versions_ok(self):
        p = _project(
            models=[
                {"name": "users", "sql": "SELECT 1", "version": 1},
                {"name": "users", "sql": "SELECT 1, 2", "version": 2},
            ]
        )
        result = ProjectValidator(p).validate()
        assert not any("VERSION" in e.code for e in result.errors)
        assert not any("VERSION" in w.code for w in result.warnings)

    def test_duplicate_version_error(self):
        p = _project(
            models=[
                {"name": "users", "sql": "SELECT 1", "version": 1},
                {"name": "users", "sql": "SELECT 2", "version": 1},
            ]
        )
        result = ProjectValidator(p).validate()
        errors = [e for e in result.errors if "DUPLICATE_VERSION" in e.code]
        assert len(errors) == 1

    def test_version_gap_warning(self):
        p = _project(
            models=[
                {"name": "users", "sql": "SELECT 1", "version": 1},
                {"name": "users", "sql": "SELECT 3", "version": 3},
            ]
        )
        result = ProjectValidator(p).validate()
        warnings = [w for w in result.warnings if "VERSION_GAP" in w.code]
        assert len(warnings) == 1
        assert "version 3" in warnings[0].message
        assert "version 2" in warnings[0].message

    def test_no_version_is_ok(self):
        p = _project(models=[{"name": "out", "sql": "SELECT 1"}])
        result = ProjectValidator(p).validate()
        assert not any("VERSION" in e.code for e in result.errors)

    def test_single_version_1_ok(self):
        p = _project(models=[{"name": "out", "sql": "SELECT 1", "version": 1}])
        result = ProjectValidator(p).validate()
        assert not any("VERSION_GAP" in w.code for w in result.warnings)

    def test_version_2_without_1_warns(self):
        p = _project(models=[{"name": "out", "sql": "SELECT 1", "version": 2}])
        result = ProjectValidator(p).validate()
        warnings = [w for w in result.warnings if "VERSION_GAP" in w.code]
        assert len(warnings) == 1
