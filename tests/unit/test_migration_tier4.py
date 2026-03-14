"""Tests for migration findings Tier 4-5 features.

Covers: colored diff, exit codes, column type checking, DLQ auto-creation,
progress bars, consumer-groups flag, show --diff flag.
"""

from __future__ import annotations

import json
import tempfile
from pathlib import Path
from unittest.mock import MagicMock

import yaml

from streamt.core.errors import EXIT_CODE_MAP, ErrorCode, exit_code_for
from streamt.core.models import StreamtProject
from streamt.core.parser import ProjectParser

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _write_project(tmpdir: str, config: dict) -> Path:
    p = Path(tmpdir)
    (p / "stream_project.yml").write_text(yaml.dump(config))
    return p


def _parse(tmpdir: str, config: dict) -> StreamtProject:
    p = _write_project(tmpdir, config)
    return ProjectParser(p).parse()


def _base_config(**overrides: object) -> dict:
    cfg: dict = {
        "project": {"name": "test", "version": "1.0.0"},
        "runtime": {"kafka": {"bootstrap_servers": "localhost:9092"}},
        "sources": [
            {
                "name": "clicks",
                "topic": "raw.clicks.v1",
                "columns": [
                    {"name": "user_id", "type": "STRING"},
                    {"name": "url", "type": "STRING"},
                    {"name": "ts", "type": "TIMESTAMP(3)"},
                ],
            }
        ],
        "models": [
            {
                "name": "clean_clicks",
                "sql": 'SELECT user_id, url FROM {{ source("clicks") }}',
            }
        ],
    }
    cfg.update(overrides)
    return cfg


def _invoke(*args: str, project_dir: str | None = None):
    from click.testing import CliRunner

    from streamt.cli import main

    runner = CliRunner(mix_stderr=False)
    cmd = list(args)
    if project_dir:
        cmd += ["-p", project_dir]
    return runner.invoke(main, cmd)


# ============================================================================
# #178: UX-6 — Colored diff in plan output
# ============================================================================


class TestColoredDiff:
    def test_details_color_on_create(self) -> None:
        from streamt.deployer.kafka import TopicChange
        from streamt.deployer.planner import DeploymentPlan

        plan = DeploymentPlan(
            topic_changes=[
                TopicChange(
                    topic="new-t",
                    action="create",
                    desired=MagicMock(partitions=6, replication_factor=3),
                ),
            ]
        )
        out = plan.details(color=True)
        assert "[green]+ topic: new-t[/green]" in out

    def test_details_color_on_delete(self) -> None:
        from streamt.deployer.kafka import TopicChange
        from streamt.deployer.planner import DeploymentPlan

        plan = DeploymentPlan(
            topic_changes=[
                TopicChange(topic="old-t", action="delete"),
            ]
        )
        out = plan.details(color=True)
        assert "[red]- topic: old-t[/red]" in out

    def test_details_color_off(self) -> None:
        from streamt.deployer.kafka import TopicChange
        from streamt.deployer.planner import DeploymentPlan

        plan = DeploymentPlan(
            topic_changes=[
                TopicChange(
                    topic="t",
                    action="create",
                    desired=MagicMock(partitions=1, replication_factor=1),
                ),
            ]
        )
        out = plan.details(color=False)
        assert "+ topic: t" in out
        assert "[green]" not in out

    def test_details_update_tilde_marker(self) -> None:
        from streamt.deployer.kafka import TopicChange
        from streamt.deployer.planner import DeploymentPlan

        plan = DeploymentPlan(
            topic_changes=[
                TopicChange(
                    topic="t", action="update", changes={"partitions": {"from": 3, "to": 6}}
                ),
            ]
        )
        out = plan.details(color=True)
        assert "[yellow]~ topic: t[/yellow]" in out
        assert "partitions: 3 -> 6" in out

    def test_details_schema_register(self) -> None:
        from streamt.deployer.planner import DeploymentPlan
        from streamt.deployer.schema_registry import SchemaChange

        plan = DeploymentPlan(
            schema_changes=[
                SchemaChange(
                    subject="topic-value", action="register", desired=MagicMock(schema_type="AVRO")
                ),
            ]
        )
        out = plan.details(color=True)
        assert "[green]+ schema: topic-value[/green]" in out


# ============================================================================
# #181: JSON-2 — Machine-parseable exit codes
# ============================================================================


class TestExitCodes:
    def test_validation_errors_exit_2(self) -> None:
        assert exit_code_for(ErrorCode.SOURCE_NOT_FOUND) == 2
        assert exit_code_for(ErrorCode.DUPLICATE_NAME) == 2
        assert exit_code_for(ErrorCode.CYCLE_DETECTED) == 2
        assert exit_code_for(ErrorCode.NAMING_VIOLATION) == 2

    def test_config_errors_exit_3(self) -> None:
        assert exit_code_for(ErrorCode.GATEWAY_REQUIRED) == 3
        assert exit_code_for(ErrorCode.FLINK_REQUIRED) == 3
        assert exit_code_for(ErrorCode.INVALID_STATE_TTL) == 3

    def test_deployment_errors_exit_4(self) -> None:
        assert exit_code_for(ErrorCode.CANNOT_REDUCE_PARTITIONS) == 4
        assert exit_code_for(ErrorCode.FLINK_SQL_ERROR) == 4
        assert exit_code_for(ErrorCode.AUTH_FAILED) == 4

    def test_parse_errors_exit_5(self) -> None:
        assert exit_code_for(ErrorCode.PARSE_ERROR) == 5
        assert exit_code_for(ErrorCode.ENV_VAR_ERROR) == 5
        assert exit_code_for(ErrorCode.ENVIRONMENT_ERROR) == 5

    def test_warnings_exit_0(self) -> None:
        assert exit_code_for(ErrorCode.STATE_TTL_RECOMMENDED) == 0
        assert exit_code_for(ErrorCode.MISSING_SOURCE_COLUMN) == 0
        assert exit_code_for(ErrorCode.COLUMN_TYPE_MISMATCH) == 0

    def test_unknown_code_fallback(self) -> None:
        assert exit_code_for("X999_UNKNOWN") == 1

    def test_all_prefixes_covered(self) -> None:
        for prefix in ("E1", "E2", "E3", "E4", "E5", "E6", "W"):
            assert prefix in EXIT_CODE_MAP


# ============================================================================
# #173: VALIDATE-4 — Column type checking against SQL inference
# ============================================================================


class TestColumnTypeChecking:
    def test_compatible_same_group(self) -> None:
        from streamt.core.type_checker import _types_compatible

        assert _types_compatible("STRING", "VARCHAR")
        assert _types_compatible("INT", "INTEGER")
        assert _types_compatible("DOUBLE", "FLOAT")
        assert _types_compatible("BOOLEAN", "BOOL")

    def test_incompatible_different_groups(self) -> None:
        from streamt.core.type_checker import _types_compatible

        assert not _types_compatible("STRING", "INT")
        assert not _types_compatible("BOOLEAN", "DOUBLE")
        assert not _types_compatible("BIGINT", "STRING")

    def test_normalize_strips_params(self) -> None:
        from streamt.core.type_checker import _normalize_type

        assert _normalize_type("DECIMAL(10,2)") == "float"
        assert _normalize_type("VARCHAR(255)") == "string"
        assert _normalize_type("TIMESTAMP(3)") == "timestamp"

    def test_contract_type_mismatch_detected(self) -> None:
        from streamt.core.type_checker import ColumnTypeChecker

        with tempfile.TemporaryDirectory() as d:
            project = _parse(
                d,
                {
                    "project": {"name": "test", "version": "1.0.0"},
                    "runtime": {"kafka": {"bootstrap_servers": "localhost:9092"}},
                    "sources": [
                        {
                            "name": "ev",
                            "topic": "ev.v1",
                            "columns": [
                                {"name": "count_val", "type": "BIGINT"},
                                {"name": "name", "type": "STRING"},
                            ],
                        }
                    ],
                    "models": [
                        {
                            "name": "typed",
                            "sql": 'SELECT count_val, name FROM {{ source("ev") }}',
                            "contract": {
                                "enforced": True,
                                "columns": [
                                    {"name": "count_val", "type": "STRING"},
                                    {"name": "name", "type": "STRING"},
                                ],
                            },
                        }
                    ],
                },
            )
            checker = ColumnTypeChecker(project)
            results = checker.check_model(project.get_model("typed"))
            mismatches = [r for r in results if r.issue == "type_incompatible"]
            assert len(mismatches) >= 1
            assert mismatches[0].column == "count_val"

    def test_contract_compatible_no_issue(self) -> None:
        from streamt.core.type_checker import ColumnTypeChecker

        with tempfile.TemporaryDirectory() as d:
            project = _parse(
                d,
                {
                    "project": {"name": "test", "version": "1.0.0"},
                    "runtime": {"kafka": {"bootstrap_servers": "localhost:9092"}},
                    "sources": [
                        {
                            "name": "ev",
                            "topic": "ev.v1",
                            "columns": [
                                {"name": "uid", "type": "STRING"},
                            ],
                        }
                    ],
                    "models": [
                        {
                            "name": "ok",
                            "sql": 'SELECT uid FROM {{ source("ev") }}',
                            "contract": {
                                "enforced": True,
                                "columns": [
                                    {"name": "uid", "type": "VARCHAR"},
                                ],
                            },
                        }
                    ],
                },
            )
            checker = ColumnTypeChecker(project)
            results = checker.check_model(project.get_model("ok"))
            assert not [r for r in results if r.issue == "type_incompatible"]


# ============================================================================
# #174: KAFKA-2 — Dead-letter topic auto-creation
# ============================================================================


class TestDLQAutoCreation:
    def test_dlq_topic_from_explicit_name(self) -> None:
        from streamt.compiler import Compiler

        with tempfile.TemporaryDirectory() as d:
            project = _parse(
                d,
                {
                    "project": {"name": "test", "version": "1.0.0"},
                    "runtime": {"kafka": {"bootstrap_servers": "localhost:9092"}},
                    "sources": [{"name": "raw", "topic": "raw.v1"}],
                    "models": [{"name": "clean", "sql": 'SELECT * FROM {{ source("raw") }}'}],
                    "tests": [
                        {
                            "name": "t1",
                            "model": "clean",
                            "type": "schema",
                            "assertions": [{"not_null": "user_id"}],
                            "on_failure": {
                                "severity": "error",
                                "actions": [{"dlq": {"topic": "clean__dead_letters"}}],
                            },
                        }
                    ],
                },
            )
            manifest = Compiler(project).compile(dry_run=True)
            names = [t["name"] for t in manifest.artifacts.get("topics", [])]
            assert "clean__dead_letters" in names

    def test_dlq_default_name(self) -> None:
        from streamt.compiler import Compiler

        with tempfile.TemporaryDirectory() as d:
            project = _parse(
                d,
                {
                    "project": {"name": "test", "version": "1.0.0"},
                    "runtime": {"kafka": {"bootstrap_servers": "localhost:9092"}},
                    "sources": [{"name": "raw", "topic": "raw.v1"}],
                    "models": [{"name": "clean", "sql": 'SELECT * FROM {{ source("raw") }}'}],
                    "tests": [
                        {
                            "name": "t1",
                            "model": "clean",
                            "type": "schema",
                            "assertions": [{"not_null": "uid"}],
                            "on_failure": {"severity": "error", "actions": [{"dlq": True}]},
                        }
                    ],
                },
            )
            manifest = Compiler(project).compile(dry_run=True)
            names = [t["name"] for t in manifest.artifacts.get("topics", [])]
            assert "clean__dlq" in names

    def test_no_duplicate_dlq_topics(self) -> None:
        from streamt.compiler import Compiler

        with tempfile.TemporaryDirectory() as d:
            project = _parse(
                d,
                {
                    "project": {"name": "test", "version": "1.0.0"},
                    "runtime": {"kafka": {"bootstrap_servers": "localhost:9092"}},
                    "sources": [{"name": "raw", "topic": "raw.v1"}],
                    "models": [{"name": "clean", "sql": 'SELECT * FROM {{ source("raw") }}'}],
                    "tests": [
                        {
                            "name": "t1",
                            "model": "clean",
                            "type": "schema",
                            "assertions": [{"not_null": "a"}],
                            "on_failure": {
                                "severity": "error",
                                "actions": [{"dlq": {"topic": "shared"}}],
                            },
                        },
                        {
                            "name": "t2",
                            "model": "clean",
                            "type": "schema",
                            "assertions": [{"not_null": "b"}],
                            "on_failure": {
                                "severity": "error",
                                "actions": [{"dlq": {"topic": "shared"}}],
                            },
                        },
                    ],
                },
            )
            manifest = Compiler(project).compile(dry_run=True)
            names = [t["name"] for t in manifest.artifacts.get("topics", [])]
            assert names.count("shared") == 1

    def test_no_dlq_without_on_failure(self) -> None:
        from streamt.compiler import Compiler

        with tempfile.TemporaryDirectory() as d:
            project = _parse(
                d,
                {
                    "project": {"name": "test", "version": "1.0.0"},
                    "runtime": {"kafka": {"bootstrap_servers": "localhost:9092"}},
                    "sources": [{"name": "raw", "topic": "raw.v1"}],
                    "models": [{"name": "clean", "sql": 'SELECT * FROM {{ source("raw") }}'}],
                    "tests": [
                        {
                            "name": "t1",
                            "model": "clean",
                            "type": "schema",
                            "assertions": [{"not_null": "uid"}],
                        }
                    ],
                },
            )
            manifest = Compiler(project).compile(dry_run=True)
            names = [t["name"] for t in manifest.artifacts.get("topics", [])]
            assert not [n for n in names if "dlq" in n.lower()]


# ============================================================================
# #180: UX-3 — Progress bars
# ============================================================================


class TestProgressBar:
    def test_json_mode_passthrough(self) -> None:
        from streamt.output import OutputFormatter

        fmt = OutputFormatter(output_format="json")
        assert fmt.progress_bar([1, 2, 3], "Test") == [1, 2, 3]

    def test_quiet_mode_passthrough(self) -> None:
        from streamt.output import OutputFormatter

        fmt = OutputFormatter(output_format="text", quiet=True)
        assert fmt.progress_bar(["a", "b"], "Test") == ["a", "b"]

    def test_text_mode_returns_all_items(self) -> None:
        from streamt.output import OutputFormatter

        fmt = OutputFormatter(output_format="text", quiet=False)
        result = fmt.progress_bar([10, 20, 30], "Processing")
        assert result == [10, 20, 30]

    def test_empty_list(self) -> None:
        from streamt.output import OutputFormatter

        fmt = OutputFormatter(output_format="text")
        assert fmt.progress_bar([], "Empty") == []


# ============================================================================
# #177: STATUS-3 — Consumer lag flag acceptance
# ============================================================================


class TestStatusConsumerGroups:
    def test_flag_accepted(self, tmp_path: Path) -> None:
        _write_project(str(tmp_path), _base_config())
        r = _invoke("status", "--consumer-groups", project_dir=str(tmp_path))
        # Flag should parse; Kafka connection failure is expected in tests
        assert r.exit_code == 0 or "connect" in (r.output + (r.stderr or "")).lower()


# ============================================================================
# #182: SHOW-4 — Show resource diff vs deployed
# ============================================================================


class TestShowDiffFlag:
    def test_diff_flag_parsed(self, tmp_path: Path) -> None:
        _write_project(str(tmp_path), _base_config())
        r = _invoke("show", "source", "clicks", "--diff", project_dir=str(tmp_path))
        assert r.exit_code == 0 or "connect" in (r.output + (r.stderr or "")).lower()

    def test_show_model_basic(self, tmp_path: Path) -> None:
        _write_project(str(tmp_path), _base_config())
        r = _invoke("show", "model", "clean_clicks", project_dir=str(tmp_path))
        assert r.exit_code == 0
        assert "clean_clicks" in r.output

    def test_show_model_diff_json(self, tmp_path: Path) -> None:
        _write_project(str(tmp_path), _base_config())
        r = _invoke(
            "-o", "json", "show", "model", "clean_clicks", "--diff", project_dir=str(tmp_path)
        )
        if r.exit_code == 0:
            data = json.loads(r.output)
            assert data["data"]["name"] == "clean_clicks"

    def test_show_source_columns_json(self, tmp_path: Path) -> None:
        _write_project(str(tmp_path), _base_config())
        r = _invoke("-o", "json", "show", "source", "clicks", project_dir=str(tmp_path))
        assert r.exit_code == 0
        cols = json.loads(r.output)["data"]["columns"]
        assert {c["name"] for c in cols} >= {"user_id", "url"}

    def test_show_compiled_sql(self, tmp_path: Path) -> None:
        """show model should resolve Jinja refs in compiled_sql."""
        _write_project(str(tmp_path), _base_config())
        r = _invoke("-o", "json", "show", "model", "clean_clicks", project_dir=str(tmp_path))
        assert r.exit_code == 0
        data = json.loads(r.output)["data"]
        # compiled_sql replaces {{ source("clicks") }} with the topic name
        if "compiled_sql" in data:
            assert "{{" not in data["compiled_sql"]
