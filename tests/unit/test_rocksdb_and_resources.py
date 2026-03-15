"""Tests for RocksDB tuning and resource configuration SET statements."""

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


class TestRocksDBConfig:
    def test_block_cache_size(self):
        sql = _compile_sql({"rocksdb": {"block_cache_size_mb": 256}})
        assert "SET 'state.backend.rocksdb.block.cache-size' = '256mb';" in sql

    def test_write_buffer_size(self):
        sql = _compile_sql({"rocksdb": {"write_buffer_size_mb": 128}})
        assert "SET 'state.backend.rocksdb.writebuffer.size' = '128mb';" in sql

    def test_predefined_options(self):
        sql = _compile_sql({"rocksdb": {"predefined_options": "SPINNING_DISK_OPTIMIZED"}})
        assert "SET 'state.backend.rocksdb.predefined-options' = 'SPINNING_DISK_OPTIMIZED';" in sql

    def test_full_rocksdb_config(self):
        sql = _compile_sql(
            {
                "state_backend": "rocksdb",
                "rocksdb": {
                    "block_cache_size_mb": 512,
                    "write_buffer_size_mb": 64,
                    "predefined_options": "FLASH_SSD_OPTIMIZED",
                },
            }
        )
        assert "SET 'state.backend' = 'rocksdb';" in sql
        assert "SET 'state.backend.rocksdb.block.cache-size' = '512mb';" in sql
        assert "SET 'state.backend.rocksdb.writebuffer.size' = '64mb';" in sql
        assert "SET 'state.backend.rocksdb.predefined-options' = 'FLASH_SSD_OPTIMIZED';" in sql

    def test_no_rocksdb_config(self):
        sql = _compile_sql({"parallelism": 2})
        assert "rocksdb" not in sql


class TestResourceConfig:
    def test_taskmanager_memory(self):
        sql = _compile_sql({"resources": {"taskmanager_memory_mb": 2048}})
        assert "SET 'taskmanager.memory.process.size' = '2048mb';" in sql

    def test_taskmanager_slots(self):
        sql = _compile_sql({"resources": {"taskmanager_slots": 4}})
        assert "SET 'taskmanager.numberOfTaskSlots' = '4';" in sql

    def test_jobmanager_memory(self):
        sql = _compile_sql({"resources": {"jobmanager_memory_mb": 1024}})
        assert "SET 'jobmanager.memory.process.size' = '1024mb';" in sql

    def test_full_resource_config(self):
        sql = _compile_sql(
            {
                "resources": {
                    "taskmanager_memory_mb": 4096,
                    "taskmanager_slots": 2,
                    "jobmanager_memory_mb": 2048,
                },
            }
        )
        assert "SET 'taskmanager.memory.process.size' = '4096mb';" in sql
        assert "SET 'taskmanager.numberOfTaskSlots' = '2';" in sql
        assert "SET 'jobmanager.memory.process.size' = '2048mb';" in sql

    def test_no_resource_config(self):
        sql = _compile_sql({"parallelism": 2})
        assert "taskmanager" not in sql
        assert "jobmanager" not in sql
