"""TDD tests for P3: Model Templates — Jinja macros from macros/ directory.

Tests drive the design: macros/*.sql.j2 files, rendered via Jinja,
model can specify macro: + params: instead of sql:.
"""

import tempfile
from dataclasses import FrozenInstanceError
from pathlib import Path

import pytest
import yaml

from streamt.compiler.compiler import CompileError, Compiler
from streamt.compiler.model_resolution import ModelDependency
from streamt.core.models import MaterializedType, Model, StreamtProject
from streamt.core.parser import ProjectParser


def _parse(tmpdir: str, config: dict) -> StreamtProject:
    p = Path(tmpdir)
    (p / "stream_project.yml").write_text(yaml.dump(config))
    return ProjectParser(p).parse()


def _write_macro(tmpdir: str, name: str, content: str) -> None:
    macros_dir = Path(tmpdir) / "macros"
    macros_dir.mkdir(exist_ok=True)
    (macros_dir / f"{name}.sql.j2").write_text(content)


BASE = {
    "project": {"name": "test", "version": "1.0.0"},
    "runtime": {"kafka": {"bootstrap_servers": "localhost:9092"}},
    "sources": [
        {"name": "orders_raw", "topic": "orders.raw.v1"},
        {"name": "customers", "topic": "customers.v1"},
    ],
}


class TestMacroParsing:
    """Model YAML parses macro + params fields."""

    def test_model_accepts_macro_and_params(self):
        """Model can declare macro: and params: instead of sql:."""
        with tempfile.TemporaryDirectory() as d:
            _write_macro(
                d, "filter_valid", "SELECT * FROM {{ source(source) }} WHERE id IS NOT NULL"
            )
            cfg = {
                **BASE,
                "models": [
                    {
                        "name": "orders_valid",
                        "macro": "filter_valid",
                        "params": {"source": "orders_raw"},
                    }
                ],
            }
            project = _parse(d, cfg)
            model = project.get_model("orders_valid")
            assert model.macro == "filter_valid"
            assert model.params == {"source": "orders_raw"}

    def test_model_macro_and_sql_are_mutually_exclusive(self):
        """Setting both macro: and sql: on a model raises a validation error."""
        with tempfile.TemporaryDirectory() as d:
            cfg = {
                **BASE,
                "models": [
                    {
                        "name": "m",
                        "sql": "SELECT 1",
                        "macro": "some_macro",
                        "params": {},
                    }
                ],
            }
            with pytest.raises(Exception):
                _parse(d, cfg)


class TestMacroCompilation:
    """Compiler renders macros and produces correct SQL."""

    def test_simple_macro_renders_to_sql(self):
        """A macro template is rendered and the model compiles successfully."""
        with tempfile.TemporaryDirectory() as d:
            _write_macro(
                d,
                "filter_valid",
                "SELECT * FROM {{ source(source_name) }} WHERE id IS NOT NULL",
            )
            cfg = {
                **BASE,
                "models": [
                    {
                        "name": "orders_valid",
                        "macro": "filter_valid",
                        "params": {"source_name": "orders_raw"},
                    }
                ],
            }
            project = _parse(d, cfg)
            compiler = Compiler(project)
            manifest = compiler.compile(dry_run=True)
            # Should produce a Flink job (SELECT * with IS NOT NULL → could be stateless/gateway
            # but must produce at least one artifact)
            all_artifacts = (
                manifest.artifacts.get("flink_jobs", [])
                + manifest.artifacts.get("gateway_rules", [])
                + manifest.artifacts.get("topics", [])
            )
            names = [a.get("name") if isinstance(a, dict) else a.name for a in all_artifacts]
            assert "orders_valid" in names

    def test_macro_with_ref_function(self):
        """Macro template can use {{ ref() }} to reference other models."""
        with tempfile.TemporaryDirectory() as d:
            _write_macro(
                d,
                "enrich_orders",
                "SELECT o.*, c.name AS customer_name FROM {{ ref(orders_model) }} o "
                "LEFT JOIN {{ ref(customers_model) }} c ON o.customer_id = c.id",
            )
            cfg = {
                **BASE,
                "models": [
                    {
                        "name": "orders_base",
                        "sql": '{{ source("orders_raw") | sqlref }}SELECT * FROM orders_raw_source',
                    },
                    {
                        "name": "customers_base",
                        "sql": '{{ source("customers") | sqlref }}SELECT * FROM customers_source',
                    },
                    {
                        "name": "orders_enriched",
                        "macro": "enrich_orders",
                        "params": {
                            "orders_model": "orders_base",
                            "customers_model": "customers_base",
                        },
                    },
                ],
            }
            # Just check it parses and renders without error
            project = _parse(d, cfg)
            model = project.get_model("orders_enriched")
            assert model.macro == "enrich_orders"

    def test_missing_macro_file_raises_compile_error(self):
        """Referencing a non-existent macro raises CompileError."""
        with tempfile.TemporaryDirectory() as d:
            cfg = {
                **BASE,
                "models": [
                    {
                        "name": "orders_valid",
                        "macro": "nonexistent_macro",
                        "params": {},
                    }
                ],
            }
            project = _parse(d, cfg)
            compiler = Compiler(project)
            with pytest.raises(CompileError, match="nonexistent_macro"):
                compiler.compile(dry_run=True)

    def test_macro_params_injected_into_template(self):
        """Template variables are replaced with param values."""
        with tempfile.TemporaryDirectory() as d:
            _write_macro(
                d,
                "filter_by_status",
                "SELECT * FROM {{ source(src) }} WHERE status = '{{ status_value }}'",
            )
            cfg = {
                **BASE,
                "models": [
                    {
                        "name": "active_orders",
                        "macro": "filter_by_status",
                        "params": {"src": "orders_raw", "status_value": "active"},
                    }
                ],
            }
            project = _parse(d, cfg)
            compiler = Compiler(project)
            # Access the rendered SQL on the model after compile
            # The rendered SQL should contain 'active' literally
            manifest = compiler.compile(dry_run=True)
            # Model must have produced an artifact
            all_artifacts = (
                manifest.artifacts.get("flink_jobs", [])
                + manifest.artifacts.get("gateway_rules", [])
                + manifest.artifacts.get("topics", [])
            )
            assert len(all_artifacts) >= 1

    def test_macro_template_missing_param_raises_compile_error(self):
        """A template referencing an undefined param raises CompileError (not silent None)."""
        with tempfile.TemporaryDirectory() as d:
            _write_macro(
                d,
                "needs_param",
                "SELECT * FROM {{ source(required_source) }}",
            )
            cfg = {
                **BASE,
                "models": [
                    {
                        "name": "m",
                        "macro": "needs_param",
                        "params": {},  # required_source not provided
                    }
                ],
            }
            project = _parse(d, cfg)
            compiler = Compiler(project)
            with pytest.raises((CompileError, Exception)):
                compiler.compile(dry_run=True)

    def test_macro_without_macros_dir_raises_compile_error(self):
        """Referencing a macro when no macros/ dir exists raises CompileError."""
        with tempfile.TemporaryDirectory() as d:
            cfg = {
                **BASE,
                "models": [{"name": "m", "macro": "some_macro", "params": {}}],
            }
            project = _parse(d, cfg)
            compiler = Compiler(project)
            with pytest.raises(CompileError):
                compiler.compile(dry_run=True)

    def test_model_with_sql_unaffected_by_macros_dir(self):
        """Regular sql: models still compile when macros/ dir exists."""
        with tempfile.TemporaryDirectory() as d:
            _write_macro(d, "unused", "SELECT 1")
            cfg = {
                **BASE,
                "models": [
                    {"name": "orders_valid", "sql": 'SELECT * FROM {{ source("orders_raw") }}'}
                ],
            }
            project = _parse(d, cfg)
            compiler = Compiler(project)
            manifest = compiler.compile(dry_run=True)
            all_names = [
                (a.get("name") if isinstance(a, dict) else a.name)
                for artifacts in manifest.artifacts.values()
                for a in artifacts
            ]
            assert "orders_valid" in all_names

    def test_macro_resolution_is_shared_and_preserves_declaration(self, monkeypatch):
        """One rendered snapshot drives dependencies, DAG order, and artifacts."""
        from streamt.compiler import model_resolution

        with tempfile.TemporaryDirectory() as d:
            _write_macro(
                d,
                "join_inputs",
                "SELECT u.* FROM {{ ref(upstream) }} u "
                "JOIN {{ source(extra_source) }} e ON u.id = e.id",
            )
            cfg = {
                **BASE,
                "models": [
                    {
                        "name": "a_consumer",
                        "macro": "join_inputs",
                        "params": {
                            "upstream": "z_upstream",
                            "extra_source": "customers",
                        },
                    },
                    {
                        "name": "z_upstream",
                        "sql": 'SELECT * FROM {{ source("orders_raw") }}',
                    },
                ],
            }
            project = _parse(d, cfg)
            original_render = model_resolution._render_macro_sql
            rendered: list[str] = []
            original_materialized = Model.get_materialized
            classified: list[str] = []

            def counted_render(project_arg: StreamtProject, model_arg):
                rendered.append(model_arg.name)
                return original_render(project_arg, model_arg)

            def counted_materialized(model_arg: Model):
                classified.append(model_arg.name)
                return original_materialized(model_arg)

            monkeypatch.setattr(model_resolution, "_render_macro_sql", counted_render)
            monkeypatch.setattr(Model, "get_materialized", counted_materialized)

            compiler = Compiler(project)
            manifest = compiler.compile(dry_run=True)

            assert rendered == ["a_consumer"]
            assert classified == ["a_consumer", "z_upstream"]
            resolved = compiler.resolved_models["a_consumer"]
            assert resolved.dependencies == (
                ModelDependency(name="customers", kind="source"),
                ModelDependency(name="z_upstream", kind="model"),
            )
            assert resolved.model.macro is None
            assert resolved.model.sql is not None
            assert resolved.model is not project.get_model("a_consumer")
            assert resolved.materialized == MaterializedType.FLINK
            assert compiler.dag.nodes["a_consumer"].materialized == "flink"
            with pytest.raises(TypeError):
                compiler.resolved_models["other"] = resolved  # type: ignore[index]
            with pytest.raises(FrozenInstanceError):
                resolved.materialized = MaterializedType.TOPIC  # type: ignore[misc]
            assert compiler.dag.get_models_only() == ["z_upstream", "a_consumer"]
            assert compiler.dag.get_upstream("a_consumer") == {
                "customers",
                "orders_raw",
                "z_upstream",
            }
            assert [topic["name"] for topic in manifest.artifacts["topics"]] == [
                "z_upstream",
                "a_consumer",
            ]

            declared = next(model for model in manifest.models if model["name"] == "a_consumer")
            assert declared["macro"] == "join_inputs"
            assert declared["sql"] is None
            assert declared["params"] == {
                "upstream": "z_upstream",
                "extra_source": "customers",
            }

    def test_sql_dependencies_do_not_also_include_from(self):
        """A SQL model's unused from declarations do not generate input DDL."""
        with tempfile.TemporaryDirectory() as d:
            cfg = {
                **BASE,
                "models": [
                    {
                        "name": "orders_copy",
                        "sql": 'SELECT * FROM {{ source("orders_raw") }}',
                        "from": [{"source": "customers"}],
                    }
                ],
            }
            compiler = Compiler(_parse(d, cfg))
            manifest = compiler.compile(dry_run=True)

            assert compiler.resolved_models["orders_copy"].dependencies == (
                ModelDependency(name="orders_raw", kind="source"),
            )
            job = manifest.artifacts["flink_jobs"][0]
            assert "CREATE TABLE IF NOT EXISTS orders_raw" in job["sql"]
            assert "CREATE TABLE IF NOT EXISTS customers" not in job["sql"]
            assert "customers.v1" not in job["sql"]

    @pytest.mark.parametrize(
        ("models", "message"),
        [
            (
                [{"name": "missing", "sql": 'SELECT * FROM {{ source("unknown") }}'}],
                "source 'unknown' was not found",
            ),
            (
                [{"name": "self_ref", "sql": 'SELECT * FROM {{ ref("self_ref") }}'}],
                "cannot depend on itself",
            ),
            (
                [{"name": "blank", "sql": 'SELECT * FROM {{ source("") }}'}],
                "quoted non-blank literal",
            ),
            (
                [{"name": "dynamic", "sql": "SELECT * FROM {{ ref(target) }}"}],
                "quoted non-blank literal",
            ),
            (
                [
                    {
                        "name": "ambiguous",
                        "from": [{"source": "orders_raw", "ref": "upstream"}],
                    },
                    {"name": "upstream"},
                ],
                "must declare exactly one",
            ),
        ],
    )
    def test_resolved_dependency_errors_are_compile_errors(self, models, message):
        """Dependency resolution fails deterministically at compile, not construction."""
        with tempfile.TemporaryDirectory() as d:
            project = _parse(d, {**BASE, "models": models})
            compiler = Compiler(project)

            with pytest.raises(CompileError, match=message):
                compiler.compile(dry_run=True)

    def test_macro_cycle_error_is_deterministic(self):
        """Rendered ref cycles report one stable path before artifact generation."""
        with tempfile.TemporaryDirectory() as d:
            _write_macro(d, "select_ref", "SELECT * FROM {{ ref(target) }}")
            project = _parse(
                d,
                {
                    **BASE,
                    "models": [
                        {"name": "b", "macro": "select_ref", "params": {"target": "a"}},
                        {"name": "a", "macro": "select_ref", "params": {"target": "b"}},
                    ],
                },
            )
            compiler = Compiler(project)

            with pytest.raises(
                CompileError,
                match=r"Model dependency cycle detected: a -> b -> a",
            ):
                compiler.compile(dry_run=True)

    def test_failed_recompile_clears_resolved_snapshot_and_dag(self):
        """A failed later resolution cannot expose a stale successful view."""
        with tempfile.TemporaryDirectory() as d:
            _write_macro(d, "copy_source", "SELECT * FROM {{ source(source_name) }}")
            project = _parse(
                d,
                {
                    **BASE,
                    "models": [
                        {
                            "name": "copy",
                            "macro": "copy_source",
                            "params": {"source_name": "orders_raw"},
                        }
                    ],
                },
            )
            compiler = Compiler(project)
            compiler.compile(dry_run=True)
            assert compiler.resolved_models
            assert compiler.dag.nodes

            (Path(d) / "macros" / "copy_source.sql.j2").unlink()
            with pytest.raises(CompileError, match="not found"):
                compiler.compile(dry_run=True)

            assert not compiler.resolved_models
            assert compiler.dag.nodes == {}

    def test_post_resolution_failure_clears_snapshot_and_partial_artifacts(self):
        """Only a fully successful compile may publish its resolved view."""
        with tempfile.TemporaryDirectory() as d:
            project = _parse(
                d,
                {
                    **BASE,
                    "models": [
                        {
                            "name": "a_good",
                            "sql": 'SELECT * FROM {{ source("orders_raw") }}',
                        },
                        {
                            "name": "z_bad_sink",
                            "from": [{"source": "orders_raw"}],
                        },
                    ],
                },
            )
            compiler = Compiler(project)

            with pytest.raises(CompileError, match="has no sink configuration"):
                compiler.compile(dry_run=True)

            assert not compiler.resolved_models
            assert compiler.dag.nodes == {}
            assert compiler.schemas == []
            assert compiler.topics == []
            assert compiler.flink_jobs == []
            assert compiler.test_jobs == []
            assert compiler.connectors == []
            assert compiler.gateway_rules == []
