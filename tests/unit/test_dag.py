"""Tests for the DAG module."""

import tempfile
from pathlib import Path

import pytest
import yaml

from streamt.core.parser import ProjectParser
from streamt.core.dag import DAGBuilder, NodeType


class TestDAGBuilder:
    """Tests for DAGBuilder."""

    def _create_project(self, tmpdir: str, config: dict) -> "StreamtProject":
        """Helper to create and parse a project."""
        project_path = Path(tmpdir)
        with open(project_path / "stream_project.yml", "w") as f:
            yaml.dump(config, f)
        parser = ProjectParser(project_path)
        return parser.parse()

    def test_simple_dag(self):
        """Simple source -> model DAG should be built correctly."""
        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                "project": {"name": "test"},
                "runtime": {"kafka": {"bootstrap_servers": "localhost:9092"}},
                "sources": [{"name": "payments_raw", "topic": "t1"}],
                "models": [
                    {
                        "name": "payments_clean",
                        "sql": 'SELECT * FROM {{ source("payments_raw") }}',
                    }
                ],
            }
            project = self._create_project(tmpdir, config)
            builder = DAGBuilder(project)
            dag = builder.build()

            assert "payments_raw" in dag.nodes
            assert "payments_clean" in dag.nodes
            assert dag.nodes["payments_raw"].type == NodeType.SOURCE
            assert dag.nodes["payments_clean"].type == NodeType.MODEL
            assert "payments_clean" in dag.nodes["payments_raw"].downstream
            assert "payments_raw" in dag.nodes["payments_clean"].upstream

    def test_chain_dag(self):
        """Chain of models should be built correctly."""
        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                "project": {"name": "test"},
                "runtime": {"kafka": {"bootstrap_servers": "localhost:9092"}},
                "sources": [{"name": "raw", "topic": "t1"}],
                "models": [
                    {
                        "name": "clean",
                        "sql": 'SELECT * FROM {{ source("raw") }}',
                    },
                    {
                        "name": "enriched",
                        "sql": 'SELECT * FROM {{ ref("clean") }}',
                    },
                    {
                        "name": "aggregated",
                        "sql": 'SELECT * FROM {{ ref("enriched") }}',
                    },
                ],
            }
            project = self._create_project(tmpdir, config)
            builder = DAGBuilder(project)
            dag = builder.build()

            # Check chain
            assert "enriched" in dag.nodes["clean"].downstream
            assert "aggregated" in dag.nodes["enriched"].downstream
            assert "clean" in dag.nodes["enriched"].upstream
            assert "enriched" in dag.nodes["aggregated"].upstream

    def test_topological_sort(self):
        """Topological sort should return correct order."""
        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                "project": {"name": "test"},
                "runtime": {"kafka": {"bootstrap_servers": "localhost:9092"}},
                "sources": [{"name": "raw", "topic": "t1"}],
                "models": [
                    {
                        "name": "clean",
                        "sql": 'SELECT * FROM {{ source("raw") }}',
                    },
                    {
                        "name": "enriched",
                        "sql": 'SELECT * FROM {{ ref("clean") }}',
                    },
                ],
            }
            project = self._create_project(tmpdir, config)
            builder = DAGBuilder(project)
            dag = builder.build()

            sorted_nodes = dag.topological_sort()

            # raw should come before clean, clean before enriched
            raw_idx = sorted_nodes.index("raw")
            clean_idx = sorted_nodes.index("clean")
            enriched_idx = sorted_nodes.index("enriched")

            assert raw_idx < clean_idx < enriched_idx

    def test_get_upstream(self):
        """get_upstream should return all upstream nodes."""
        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                "project": {"name": "test"},
                "runtime": {"kafka": {"bootstrap_servers": "localhost:9092"}},
                "sources": [
                    {"name": "src1", "topic": "t1"},
                    {"name": "src2", "topic": "t2"},
                ],
                "models": [
                    {
                        "name": "model1",
                        "sql": 'SELECT * FROM {{ source("src1") }}',
                    },
                    {
                        "name": "model2",
                        "sql": 'SELECT * FROM {{ source("src2") }}',
                    },
                    {
                        "name": "model3",
                        "sql": """
                            SELECT * FROM {{ ref("model1") }}
                            JOIN {{ ref("model2") }} ON 1=1
                        """,
                    },
                ],
            }
            project = self._create_project(tmpdir, config)
            builder = DAGBuilder(project)
            dag = builder.build()

            upstream = dag.get_upstream("model3")

            assert "model1" in upstream
            assert "model2" in upstream
            assert "src1" in upstream
            assert "src2" in upstream

    def test_get_downstream(self):
        """get_downstream should return all downstream nodes."""
        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                "project": {"name": "test"},
                "runtime": {"kafka": {"bootstrap_servers": "localhost:9092"}},
                "sources": [{"name": "raw", "topic": "t1"}],
                "models": [
                    {
                        "name": "clean",
                        "sql": 'SELECT * FROM {{ source("raw") }}',
                    },
                    {
                        "name": "agg1",
                        "sql": 'SELECT * FROM {{ ref("clean") }}',
                    },
                    {
                        "name": "agg2",
                        "sql": 'SELECT * FROM {{ ref("clean") }}',
                    },
                ],
            }
            project = self._create_project(tmpdir, config)
            builder = DAGBuilder(project)
            dag = builder.build()

            downstream = dag.get_downstream("raw")

            assert "clean" in downstream
            assert "agg1" in downstream
            assert "agg2" in downstream

    def test_exposure_edges(self):
        """Exposures should create correct edges."""
        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                "project": {"name": "test"},
                "runtime": {"kafka": {"bootstrap_servers": "localhost:9092"}},
                "sources": [{"name": "raw", "topic": "t1"}],
                "models": [
                    {
                        "name": "clean",
                        "sql": 'SELECT * FROM {{ source("raw") }}',
                    },
                ],
                "exposures": [
                    {
                        "name": "consumer_app",
                        "type": "application",
                        "role": "consumer",
                        "consumes": [{"ref": "clean"}],
                    }
                ],
            }
            project = self._create_project(tmpdir, config)
            builder = DAGBuilder(project)
            dag = builder.build()

            assert "consumer_app" in dag.nodes
            assert dag.nodes["consumer_app"].type == NodeType.EXPOSURE
            assert "consumer_app" in dag.nodes["clean"].downstream
            assert "clean" in dag.nodes["consumer_app"].upstream

    def test_models_only(self):
        """get_models_only should return only model nodes in order."""
        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                "project": {"name": "test"},
                "runtime": {"kafka": {"bootstrap_servers": "localhost:9092"}},
                "sources": [{"name": "raw", "topic": "t1"}],
                "models": [
                    {
                        "name": "clean",
                        "sql": 'SELECT * FROM {{ source("raw") }}',
                    },
                    {
                        "name": "enriched",
                        "sql": 'SELECT * FROM {{ ref("clean") }} GROUP BY id',
                    },
                ],
                "exposures": [
                    {
                        "name": "app",
                        "type": "application",
                        "role": "consumer",
                        "consumes": [{"ref": "enriched"}],
                    }
                ],
            }
            project = self._create_project(tmpdir, config)
            builder = DAGBuilder(project)
            dag = builder.build()

            models = dag.get_models_only()

            assert models == ["clean", "enriched"]
            assert "raw" not in models
            assert "app" not in models

    def test_to_dict(self):
        """to_dict should produce correct structure."""
        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                "project": {"name": "test"},
                "runtime": {"kafka": {"bootstrap_servers": "localhost:9092"}},
                "sources": [{"name": "raw", "topic": "t1"}],
                "models": [
                    {
                        "name": "clean",
                        "sql": 'SELECT * FROM {{ source("raw") }}',
                    },
                ],
            }
            project = self._create_project(tmpdir, config)
            builder = DAGBuilder(project)
            dag = builder.build()

            dag_dict = dag.to_dict()

            assert "nodes" in dag_dict
            assert "edges" in dag_dict
            assert len(dag_dict["nodes"]) == 2
            assert len(dag_dict["edges"]) == 1
