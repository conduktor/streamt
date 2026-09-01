"""Tests for migration findings Tier 3 features.

Covers: Mermaid/DOT lineage, column lineage, list --sort-by, data dictionary,
OpenAPI spec, envs diff, naming conventions.
"""

from __future__ import annotations

import csv
import io
import json
import tempfile
from pathlib import Path

import yaml

from streamt.core.models import StreamtProject
from streamt.core.parser import ProjectParser

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _write_project(tmpdir: str, config: dict) -> Path:
    p = Path(tmpdir)
    config.setdefault("apiVersion", "streamt.dev/v1alpha1")
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

    runner = CliRunner()
    cmd = list(args)
    if project_dir:
        cmd += ["-p", project_dir]
    return runner.invoke(main, cmd)


# ============================================================================
# #176: LINEAGE-2 — Mermaid / DOT export
# ============================================================================


class TestLineageMermaidDot:
    def _make_project(self, tmpdir: str) -> str:
        _write_project(
            tmpdir,
            {
                "project": {"name": "test", "version": "1.0.0"},
                "runtime": {"kafka": {"bootstrap_servers": "localhost:9092"}},
                "sources": [{"name": "events", "topic": "events.v1"}],
                "models": [
                    {"name": "enriched", "sql": 'SELECT * FROM {{ source("events") }}'},
                    {"name": "agg", "sql": 'SELECT * FROM {{ ref("enriched") }}'},
                ],
            },
        )
        return tmpdir

    def test_mermaid_output_has_graph_header(self, tmp_path: Path) -> None:
        d = self._make_project(str(tmp_path))
        r = _invoke("lineage", "--format", "mermaid", project_dir=d)
        assert r.exit_code == 0
        assert "graph LR" in r.output

    def test_mermaid_contains_all_nodes(self, tmp_path: Path) -> None:
        d = self._make_project(str(tmp_path))
        r = _invoke("lineage", "--format", "mermaid", project_dir=d)
        assert "events" in r.output
        assert "enriched" in r.output
        assert "agg" in r.output

    def test_mermaid_has_edges(self, tmp_path: Path) -> None:
        d = self._make_project(str(tmp_path))
        r = _invoke("lineage", "--format", "mermaid", project_dir=d)
        assert "-->" in r.output

    def test_mermaid_source_shape_is_cylinder(self, tmp_path: Path) -> None:
        d = self._make_project(str(tmp_path))
        r = _invoke("lineage", "--format", "mermaid", project_dir=d)
        assert '("' in r.output

    def test_dot_output_has_digraph(self, tmp_path: Path) -> None:
        d = self._make_project(str(tmp_path))
        r = _invoke("lineage", "--format", "dot", project_dir=d)
        assert r.exit_code == 0
        assert "digraph streamt" in r.output
        assert "rankdir=LR" in r.output

    def test_dot_contains_edges(self, tmp_path: Path) -> None:
        d = self._make_project(str(tmp_path))
        r = _invoke("lineage", "--format", "dot", project_dir=d)
        assert "->" in r.output

    def test_dot_source_shape_is_cylinder(self, tmp_path: Path) -> None:
        d = self._make_project(str(tmp_path))
        r = _invoke("lineage", "--format", "dot", project_dir=d)
        assert "shape=cylinder" in r.output

    def test_mermaid_with_upstream_filter(self, tmp_path: Path) -> None:
        d = self._make_project(str(tmp_path))
        r = _invoke("lineage", "--format", "mermaid", "-m", "enriched", "--upstream", project_dir=d)
        assert r.exit_code == 0
        assert "events" in r.output

    def test_dot_with_downstream_filter(self, tmp_path: Path) -> None:
        d = self._make_project(str(tmp_path))
        r = _invoke("lineage", "--format", "dot", "-m", "agg", "--downstream", project_dir=d)
        assert r.exit_code == 0
        assert "agg" in r.output


# ============================================================================
# #171: LINEAGE-3 — Column-level lineage
# ============================================================================


class TestColumnLineage:
    def test_column_lineage_traces_direct_columns(self, tmp_path: Path) -> None:
        _write_project(
            str(tmp_path),
            {
                "project": {"name": "test", "version": "1.0.0"},
                "runtime": {"kafka": {"bootstrap_servers": "localhost:9092"}},
                "sources": [
                    {
                        "name": "orders",
                        "topic": "orders.v1",
                        "columns": [
                            {"name": "order_id", "type": "STRING"},
                            {"name": "amount", "type": "DECIMAL(10,2)"},
                        ],
                    }
                ],
                "models": [
                    {"name": "totals", "sql": 'SELECT order_id, amount FROM {{ source("orders") }}'}
                ],
            },
        )
        r = _invoke("lineage", "--columns", "-m", "totals", project_dir=str(tmp_path))
        assert r.exit_code == 0

    def test_column_lineage_json_envelope(self, tmp_path: Path) -> None:
        _write_project(
            str(tmp_path),
            {
                "project": {"name": "test", "version": "1.0.0"},
                "runtime": {"kafka": {"bootstrap_servers": "localhost:9092"}},
                "sources": [
                    {
                        "name": "events",
                        "topic": "events.v1",
                        "columns": [{"name": "event_id", "type": "STRING"}],
                    }
                ],
                "models": [{"name": "f", "sql": 'SELECT event_id FROM {{ source("events") }}'}],
            },
        )
        r = _invoke("-o", "json", "lineage", "--columns", "-m", "f", project_dir=str(tmp_path))
        assert r.exit_code == 0
        data = json.loads(r.output)
        assert "column_lineage" in data["data"]

    def test_column_lineage_requires_model(self, tmp_path: Path) -> None:
        _write_project(str(tmp_path), _base_config())
        r = _invoke("lineage", "--columns", project_dir=str(tmp_path))
        assert r.exit_code != 0

    def test_select_star_expands_all_columns(self) -> None:
        from streamt.core.dag import ColumnLineageBuilder

        with tempfile.TemporaryDirectory() as d:
            project = _parse(
                d,
                {
                    "project": {"name": "test", "version": "1.0.0"},
                    "runtime": {"kafka": {"bootstrap_servers": "localhost:9092"}},
                    "sources": [
                        {
                            "name": "raw",
                            "topic": "raw.v1",
                            "columns": [
                                {"name": "a", "type": "STRING"},
                                {"name": "b", "type": "INT"},
                            ],
                        }
                    ],
                    "models": [{"name": "p", "sql": 'SELECT * FROM {{ source("raw") }}'}],
                },
            )
            lineage = ColumnLineageBuilder(project).build("p")
            assert {cl.column for cl in lineage} == {"a", "b"}

    def test_aliased_column_traced(self) -> None:
        from streamt.core.dag import ColumnLineageBuilder

        with tempfile.TemporaryDirectory() as d:
            project = _parse(
                d,
                {
                    "project": {"name": "test", "version": "1.0.0"},
                    "runtime": {"kafka": {"bootstrap_servers": "localhost:9092"}},
                    "sources": [
                        {
                            "name": "src",
                            "topic": "src.v1",
                            "columns": [
                                {"name": "old_name", "type": "STRING"},
                            ],
                        }
                    ],
                    "models": [
                        {"name": "r", "sql": 'SELECT old_name AS new_name FROM {{ source("src") }}'}
                    ],
                },
            )
            lineage = ColumnLineageBuilder(project).build("r")
            assert len(lineage) >= 1
            assert lineage[0].column == "new_name"
            assert ("src", "old_name") in lineage[0].upstream


# ============================================================================
# #170: LIST-4 — list --sort-by
# ============================================================================


class TestListSortBy:
    def _make_project(self, tmpdir: str) -> str:
        _write_project(
            tmpdir,
            {
                "project": {"name": "test", "version": "1.0.0"},
                "runtime": {"kafka": {"bootstrap_servers": "localhost:9092"}},
                "sources": [{"name": "s1", "topic": "s1.v1"}, {"name": "s2", "topic": "s2.v1"}],
                "models": [
                    {"name": "z_model", "sql": 'SELECT * FROM {{ source("s1") }}'},
                    {
                        "name": "a_model",
                        "sql": 'SELECT * FROM {{ source("s1") }}',
                        "materialized": "topic",
                    },
                    {
                        "name": "m_model",
                        "sql": 'SELECT * FROM {{ source("s1") }} UNION ALL SELECT * FROM {{ source("s2") }}',
                    },
                ],
            },
        )
        return tmpdir

    def test_sort_by_name_json(self, tmp_path: Path) -> None:
        d = self._make_project(str(tmp_path))
        r = _invoke("-o", "json", "list", "models", "--sort-by", "name", project_dir=d)
        assert r.exit_code == 0
        names = [i["name"] for i in json.loads(r.output)["data"]["items"]]
        assert names == sorted(names)

    def test_sort_by_type_json(self, tmp_path: Path) -> None:
        d = self._make_project(str(tmp_path))
        r = _invoke("-o", "json", "list", "models", "--sort-by", "type", project_dir=d)
        assert r.exit_code == 0
        mats = [i["materialized"] for i in json.loads(r.output)["data"]["items"]]
        assert mats == sorted(mats)

    def test_sort_by_upstream_json(self, tmp_path: Path) -> None:
        d = self._make_project(str(tmp_path))
        r = _invoke("-o", "json", "list", "models", "--sort-by", "upstream", project_dir=d)
        assert r.exit_code == 0
        counts = [len(i["upstream"]) for i in json.loads(r.output)["data"]["items"]]
        assert counts == sorted(counts, reverse=True)


# ============================================================================
# #169: DOCS-3 — Data dictionary export
# ============================================================================


class TestDocsDictionary:
    def _make_project(self, tmpdir: str) -> str:
        _write_project(
            tmpdir,
            {
                "project": {"name": "test", "version": "1.0.0"},
                "runtime": {"kafka": {"bootstrap_servers": "localhost:9092"}},
                "sources": [
                    {
                        "name": "users",
                        "topic": "users.v1",
                        "columns": [
                            {"name": "id", "type": "STRING", "description": "User identifier"},
                            {"name": "email", "type": "STRING", "classification": "sensitive"},
                        ],
                    }
                ],
                "models": [{"name": "active", "sql": 'SELECT id FROM {{ source("users") }}'}],
            },
        )
        return tmpdir

    def test_csv_format(self, tmp_path: Path) -> None:
        d = self._make_project(str(tmp_path))
        r = _invoke("docs", "dictionary", "--format", "csv", project_dir=d)
        assert r.exit_code == 0
        rows = list(csv.DictReader(io.StringIO(r.output)))
        assert len(rows) >= 2
        assert rows[0]["resource_type"] == "source"

    def test_csv_has_all_fields(self, tmp_path: Path) -> None:
        d = self._make_project(str(tmp_path))
        r = _invoke("docs", "dictionary", "--format", "csv", project_dir=d)
        for row in csv.DictReader(io.StringIO(r.output)):
            for f in ("resource_type", "column", "type", "classification"):
                assert f in row

    def test_json_format(self, tmp_path: Path) -> None:
        d = self._make_project(str(tmp_path))
        r = _invoke("docs", "dictionary", "--format", "json", project_dir=d)
        assert r.exit_code == 0
        entries = json.loads(r.output)
        assert any(e["resource"] == "users" for e in entries)

    def test_pii_classification(self, tmp_path: Path) -> None:
        d = self._make_project(str(tmp_path))
        r = _invoke("docs", "dictionary", "--format", "json", project_dir=d)
        entries = json.loads(r.output)
        assert next(e for e in entries if e["column"] == "email")["classification"] == "sensitive"


# ============================================================================
# #172: DOCS-1 — OpenAPI/AsyncAPI spec
# ============================================================================


class TestDocsOpenAPI:
    def _make_project(self, tmpdir: str) -> str:
        _write_project(
            tmpdir,
            {
                "project": {"name": "mystream", "version": "2.0.0"},
                "runtime": {"kafka": {"bootstrap_servers": "localhost:9092"}},
                "sources": [
                    {
                        "name": "clicks",
                        "topic": "raw.clicks",
                        "columns": [
                            {"name": "user_id", "type": "STRING"},
                            {"name": "page", "type": "STRING"},
                            {"name": "ts", "type": "TIMESTAMP"},
                        ],
                    }
                ],
                "models": [
                    {
                        "name": "page_views",
                        "sql": 'SELECT user_id, page FROM {{ source("clicks") }}',
                        "contract": {
                            "enforced": True,
                            "columns": [
                                {"name": "user_id", "type": "STRING"},
                                {"name": "page", "type": "STRING"},
                            ],
                        },
                    }
                ],
            },
        )
        return tmpdir

    def test_spec_structure(self, tmp_path: Path) -> None:
        d = self._make_project(str(tmp_path))
        r = _invoke("docs", "openapi", project_dir=d)
        assert r.exit_code == 0
        spec = json.loads(r.output)
        assert spec["asyncapi"] == "2.6.0"
        assert spec["info"]["title"] == "mystream"

    def test_source_channel_exists(self, tmp_path: Path) -> None:
        d = self._make_project(str(tmp_path))
        spec = json.loads(_invoke("docs", "openapi", project_dir=d).output)
        assert "raw.clicks" in spec["channels"]

    def test_schema_properties(self, tmp_path: Path) -> None:
        d = self._make_project(str(tmp_path))
        spec = json.loads(_invoke("docs", "openapi", project_dir=d).output)
        assert (
            spec["components"]["schemas"]["clicks_value"]["properties"]["user_id"]["type"]
            == "string"
        )

    def test_model_channel_from_contract(self, tmp_path: Path) -> None:
        d = self._make_project(str(tmp_path))
        spec = json.loads(_invoke("docs", "openapi", project_dir=d).output)
        assert "page_views_value" in spec["components"]["schemas"]

    def test_flink_type_mapping(self) -> None:
        from streamt.cli.commands.docs import _flink_to_json_type

        assert _flink_to_json_type("STRING") == "string"
        assert _flink_to_json_type("INT") == "integer"
        assert _flink_to_json_type("BIGINT") == "integer"
        assert _flink_to_json_type("DOUBLE") == "number"
        assert _flink_to_json_type("DECIMAL(10,2)") == "number"
        assert _flink_to_json_type("BOOLEAN") == "boolean"
        assert _flink_to_json_type("TIMESTAMP(3)") == "string"


# ============================================================================
# #179: ENV-3 — envs diff
# ============================================================================


class TestEnvsDiff:
    def _make_multi_env(self, tmpdir: str) -> str:
        p = Path(tmpdir)
        (p / "stream_project.yml").write_text(
            yaml.dump(
                {
                    "project": {"name": "test", "version": "1.0.0"},
                    "sources": [{"name": "raw", "topic": "raw.v1"}],
                }
            )
        )
        env_dir = p / "environments"
        env_dir.mkdir()
        (env_dir / "dev.yml").write_text(
            yaml.dump(
                {
                    "environment": {"name": "dev", "description": "Dev"},
                    "runtime": {"kafka": {"bootstrap_servers": "dev-kafka:9092"}},
                    "safety": {"confirm_apply": False, "allow_destructive": True},
                }
            )
        )
        (env_dir / "prod.yml").write_text(
            yaml.dump(
                {
                    "environment": {"name": "prod", "description": "Prod", "protected": True},
                    "runtime": {"kafka": {"bootstrap_servers": "prod-kafka:9092"}},
                    "safety": {"confirm_apply": True, "allow_destructive": False},
                }
            )
        )
        return tmpdir

    def test_detects_runtime_diffs(self, tmp_path: Path) -> None:
        d = self._make_multi_env(str(tmp_path))
        r = _invoke("envs", "diff", "dev", "prod", project_dir=d)
        assert r.exit_code == 0
        assert "kafka" in r.output.lower()

    def test_detects_safety_diffs(self, tmp_path: Path) -> None:
        d = self._make_multi_env(str(tmp_path))
        r = _invoke("envs", "diff", "dev", "prod", project_dir=d)
        assert "confirm_apply" in r.output or "allow_destructive" in r.output

    def test_detects_protected_flag(self, tmp_path: Path) -> None:
        d = self._make_multi_env(str(tmp_path))
        r = _invoke("envs", "diff", "dev", "prod", project_dir=d)
        assert "protected" in r.output

    def test_json_has_diffs_array(self, tmp_path: Path) -> None:
        d = self._make_multi_env(str(tmp_path))
        r = _invoke("-o", "json", "envs", "diff", "dev", "prod", project_dir=d)
        assert r.exit_code == 0
        assert len(json.loads(r.output)["data"]["diffs"]) > 0

    def test_identical_envs_no_diffs(self, tmp_path: Path) -> None:
        d = self._make_multi_env(str(tmp_path))
        r = _invoke("envs", "diff", "dev", "dev", project_dir=d)
        assert "No differences" in r.output


# ============================================================================
# #175: KAFKA-3 — Topic naming convention enforcement
# ============================================================================


class TestTopicNamingConvention:
    def test_valid_topic_passes(self) -> None:
        from streamt.core.validator import ProjectValidator

        with tempfile.TemporaryDirectory() as d:
            project = _parse(
                d,
                {
                    "project": {"name": "test", "version": "1.0.0"},
                    "runtime": {"kafka": {"bootstrap_servers": "localhost:9092"}},
                    "rules": {"topics": {"naming_pattern": r"^[a-z]+\.[a-z]+\.v\d+$"}},
                    "sources": [{"name": "clicks", "topic": "raw.clicks.v1"}],
                    "models": [{"name": "m", "sql": 'SELECT * FROM {{ source("clicks") }}'}],
                },
            )
            result = ProjectValidator(project).validate()
            assert not [
                e for e in result.errors if "naming" in e.message.lower() and "clicks" in e.message
            ]

    def test_invalid_topic_raises_error(self) -> None:
        from streamt.core.validator import ProjectValidator

        with tempfile.TemporaryDirectory() as d:
            project = _parse(
                d,
                {
                    "project": {"name": "test", "version": "1.0.0"},
                    "runtime": {"kafka": {"bootstrap_servers": "localhost:9092"}},
                    "rules": {"topics": {"naming_pattern": r"^[a-z]+\.[a-z]+\.v\d+$"}},
                    "sources": [{"name": "bad", "topic": "BAD_NAME"}],
                    "models": [{"name": "m", "sql": 'SELECT * FROM {{ source("bad") }}'}],
                },
            )
            result = ProjectValidator(project).validate()
            naming_errors = [e for e in result.errors if "naming" in e.message.lower()]
            assert len(naming_errors) >= 1
            assert "BAD_NAME" in naming_errors[0].message

    def test_model_topic_naming_enforced(self) -> None:
        from streamt.core.validator import ProjectValidator

        with tempfile.TemporaryDirectory() as d:
            project = _parse(
                d,
                {
                    "project": {"name": "test", "version": "1.0.0"},
                    "runtime": {"kafka": {"bootstrap_servers": "localhost:9092"}},
                    "rules": {"topics": {"naming_pattern": r"^[a-z]+\.[a-z]+\.v\d+$"}},
                    "sources": [{"name": "raw", "topic": "raw.data.v1"}],
                    "models": [
                        {
                            "name": "BAD_MODEL",
                            "materialized": "topic",
                            "sql": 'SELECT * FROM {{ source("raw") }}',
                        }
                    ],
                },
            )
            result = ProjectValidator(project).validate()
            assert [
                e
                for e in result.errors
                if "pattern" in e.message.lower() or "naming" in e.message.lower()
            ]


# ============================================================================
# DAG rendering unit tests
# ============================================================================


class TestDAGRendering:
    def _build_dag(self):
        from streamt.core.dag import DAG, DAGNode, NodeType

        dag = DAG()
        dag.add_node(DAGNode(name="src", type=NodeType.SOURCE))
        dag.add_node(DAGNode(name="model_a", type=NodeType.MODEL, materialized="flink"))
        dag.add_node(DAGNode(name="exp", type=NodeType.EXPOSURE))
        dag.add_edge("src", "model_a")
        dag.add_edge("model_a", "exp")
        return dag

    def test_mermaid_graph_lr(self) -> None:
        assert self._build_dag().render_mermaid().startswith("graph LR")

    def test_mermaid_source_cylinder(self) -> None:
        assert '("src")' in self._build_dag().render_mermaid()

    def test_mermaid_edge_syntax(self) -> None:
        assert "-->" in self._build_dag().render_mermaid()

    def test_mermaid_class_def_source(self) -> None:
        assert "classDef source" in self._build_dag().render_mermaid()

    def test_dot_digraph_header(self) -> None:
        assert "digraph streamt" in self._build_dag().render_dot()

    def test_dot_source_cylinder(self) -> None:
        assert "shape=cylinder" in self._build_dag().render_dot()

    def test_dot_exposure_hexagon(self) -> None:
        assert "shape=hexagon" in self._build_dag().render_dot()

    def test_dot_edge_arrow(self) -> None:
        out = self._build_dag().render_dot()
        assert '"src" -> "model_a"' in out
        assert '"model_a" -> "exp"' in out


# ============================================================================
# _flatten_dict unit test
# ============================================================================


class TestFlattenDict:
    def test_flatten_nested(self) -> None:
        from streamt.cli.commands.envs import _flatten_dict

        assert _flatten_dict({"a": {"b": {"c": 1}}, "d": 2}) == {"a.b.c": 1, "d": 2}

    def test_flatten_empty(self) -> None:
        from streamt.cli.commands.envs import _flatten_dict

        assert _flatten_dict({}) == {}

    def test_flatten_single_level(self) -> None:
        from streamt.cli.commands.envs import _flatten_dict

        assert _flatten_dict({"x": 1, "y": 2}) == {"x": 1, "y": 2}
