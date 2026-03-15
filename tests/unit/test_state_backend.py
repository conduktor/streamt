"""Tests for state_backend SET statement generation."""

from __future__ import annotations

import tempfile
from pathlib import Path

import yaml

from streamt.compiler import Compiler
from streamt.core.parser import ProjectParser


def _base_config(flink_config: dict) -> dict:
    return {
        "project": {"name": "test", "version": "1.0.0"},
        "runtime": {
            "kafka": {"bootstrap_servers": "localhost:9092"},
            "flink": {
                "default": "local",
                "clusters": {"local": {"type": "rest", "rest_url": "http://localhost:8082"}},
            },
        },
        "sources": [{"name": "src", "topic": "src.v1", "columns": [{"name": "id"}]}],
        "models": [
            {
                "name": "out",
                "sql": 'SELECT id FROM {{ source("src") }}',
                "flink": flink_config,
            }
        ],
    }


def _compile_sql(flink_config: dict) -> str:
    with tempfile.TemporaryDirectory() as tmpdir:
        path = Path(tmpdir)
        with open(path / "stream_project.yml", "w") as f:
            yaml.dump(_base_config(flink_config), f)
        project = ProjectParser(path).parse()
        output_dir = path / "gen"
        Compiler(project, output_dir).compile(dry_run=False)
        return (output_dir / "flink" / "out.sql").read_text()


class TestStateBackend:
    def test_rocksdb(self):
        sql = _compile_sql({"state_backend": "rocksdb"})
        assert "SET 'state.backend' = 'rocksdb';" in sql

    def test_hashmap(self):
        sql = _compile_sql({"state_backend": "hashmap"})
        assert "SET 'state.backend' = 'hashmap';" in sql

    def test_not_set(self):
        sql = _compile_sql({"parallelism": 4})
        assert "state.backend" not in sql

    def test_combined_with_parallelism_and_ttl(self):
        sql = _compile_sql(
            {
                "parallelism": 2,
                "state_backend": "rocksdb",
                "state_ttl_ms": 3600000,
            }
        )
        assert "SET 'parallelism.default' = '2';" in sql
        assert "SET 'state.backend' = 'rocksdb';" in sql
        assert "SET 'table.exec.state.ttl' = '1 h';" in sql
