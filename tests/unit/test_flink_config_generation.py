"""Tests for advanced Flink SET statement generation: checkpointing, restart, watermark, changelog."""

from __future__ import annotations

import tempfile
from pathlib import Path

import yaml

from streamt.compiler import Compiler
from streamt.core.parser import ProjectParser


def _base_config(flink_config: dict | None = None, source_event_time: dict | None = None) -> dict:
    model: dict = {
        "name": "out",
        "sql": 'SELECT id FROM {{ source("src") }}',
    }
    if flink_config:
        model["advanced"] = {"flink": flink_config}
    source: dict = {"name": "src", "topic": "src.v1", "columns": [{"name": "id"}]}
    if source_event_time:
        source["event_time"] = source_event_time
    return {
        "project": {"name": "test", "version": "1.0.0"},
        "runtime": {
            "kafka": {"bootstrap_servers": "localhost:9092"},
            "flink": {
                "default": "local",
                "clusters": {"local": {"type": "rest", "rest_url": "http://localhost:8082"}},
            },
        },
        "sources": [source],
        "models": [model],
    }


def _compile_sql(flink_config: dict | None = None, source_event_time: dict | None = None) -> str:
    with tempfile.TemporaryDirectory() as tmpdir:
        path = Path(tmpdir)
        with open(path / "stream_project.yml", "w") as f:
            yaml.dump(_base_config(flink_config, source_event_time), f)
        project = ProjectParser(path).parse()
        output_dir = path / "gen"
        Compiler(project, output_dir).compile(dry_run=False)
        return (output_dir / "flink" / "out.sql").read_text()


class TestCheckpointConfig:
    def test_full_checkpoint_config(self):
        sql = _compile_sql(
            {
                "checkpoint_interval_ms": 60000,
                "checkpoint": {
                    "timeout_ms": 120000,
                    "min_pause_ms": 500,
                    "max_concurrent": 1,
                    "mode": "EXACTLY_ONCE",
                    "externalized": "RETAIN_ON_CANCELLATION",
                    "unaligned": True,
                    "incremental": True,
                },
            }
        )
        assert "SET 'execution.checkpointing.interval' = '60000ms';" in sql
        assert "SET 'execution.checkpointing.timeout' = '120000ms';" in sql
        assert "SET 'execution.checkpointing.min-pause' = '500ms';" in sql
        assert "SET 'execution.checkpointing.max-concurrent-checkpoints' = '1';" in sql
        assert "SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';" in sql
        assert (
            "SET 'execution.checkpointing.externalized-checkpoint-retention' = 'RETAIN_ON_CANCELLATION';"
            in sql
        )
        assert "SET 'execution.checkpointing.unaligned.enabled' = 'true';" in sql
        assert "SET 'state.backend.incremental' = 'true';" in sql

    def test_no_checkpoint_config(self):
        sql = _compile_sql({"parallelism": 2})
        assert "checkpointing.timeout" not in sql
        assert "state.backend.incremental" not in sql


class TestRestartStrategy:
    def test_fixed_delay(self):
        sql = _compile_sql(
            {
                "restart_strategy": {"type": "fixed-delay", "attempts": 3, "delay_ms": 10000},
            }
        )
        assert "SET 'restart-strategy.type' = 'fixed-delay';" in sql
        assert "SET 'restart-strategy.fixed-delay.attempts' = '3';" in sql
        assert "SET 'restart-strategy.fixed-delay.delay' = '10000ms';" in sql

    def test_failure_rate(self):
        sql = _compile_sql(
            {
                "restart_strategy": {
                    "type": "failure-rate",
                    "max_failures_per_interval": 5,
                    "failure_rate_interval_ms": 300000,
                    "delay_ms": 5000,
                },
            }
        )
        assert "SET 'restart-strategy.type' = 'failure-rate';" in sql
        assert "SET 'restart-strategy.failure-rate.max-failures-per-interval' = '5';" in sql
        assert "SET 'restart-strategy.failure-rate.failure-rate-interval' = '300000ms';" in sql
        assert "SET 'restart-strategy.failure-rate.delay' = '5000ms';" in sql

    def test_exponential_delay(self):
        sql = _compile_sql(
            {
                "restart_strategy": {
                    "type": "exponential-delay",
                    "initial_backoff_ms": 1000,
                    "max_backoff_ms": 60000,
                    "backoff_multiplier": 2.0,
                },
            }
        )
        assert "SET 'restart-strategy.type' = 'exponential-delay';" in sql
        assert "SET 'restart-strategy.exponential-delay.initial-backoff' = '1000ms';" in sql
        assert "SET 'restart-strategy.exponential-delay.max-backoff' = '60000ms';" in sql
        assert "SET 'restart-strategy.exponential-delay.backoff-multiplier' = '2.0';" in sql

    def test_no_restart_strategy(self):
        sql = _compile_sql({"parallelism": 4})
        assert "restart-strategy" not in sql


class TestCustomWatermark:
    def test_custom_watermark_expression(self):
        sql = _compile_sql(
            source_event_time={
                "column": "event_ts",
                "watermark": {
                    "strategy": "custom",
                    "expression": "CASE WHEN `event_ts` > CURRENT_TIMESTAMP THEN CURRENT_TIMESTAMP ELSE `event_ts` END",
                },
            },
        )
        assert "WATERMARK FOR `event_ts` AS CASE WHEN" in sql

    def test_bounded_still_works(self):
        sql = _compile_sql(
            source_event_time={
                "column": "ts",
                "watermark": {
                    "strategy": "bounded_out_of_orderness",
                    "max_out_of_orderness_ms": 10000,
                },
            },
        )
        assert "WATERMARK FOR `ts` AS `ts` - INTERVAL '10' SECOND" in sql


class TestChangelogMode:
    def test_upsert_kafka_connector(self):
        sql = _compile_sql({"changelog_mode": "upsert"})
        assert "'connector' = 'upsert-kafka'" in sql
        assert "'key.format' = 'json'" in sql

    def test_default_append(self):
        sql = _compile_sql({"parallelism": 1})
        assert "'connector' = 'kafka'" in sql
        assert "upsert-kafka" not in sql
