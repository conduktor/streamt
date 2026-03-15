"""Tests for global credentials/connections feature."""

from __future__ import annotations

import tempfile
from pathlib import Path

import yaml

from streamt.compiler import Compiler
from streamt.core.parser import ProjectParser


def _project_config(connections: dict | None = None, sink_connection: str | None = None) -> dict:
    sink: dict = {
        "connector": "snowflake",
        "config": {"topics": "enriched", "snowflake.topic2table.map": "enriched:USERS"},
    }
    if sink_connection:
        sink["connection"] = sink_connection

    config: dict = {
        "project": {"name": "test", "version": "1.0.0"},
        "runtime": {
            "kafka": {"bootstrap_servers": "localhost:9092"},
            "flink": {
                "default": "local",
                "clusters": {"local": {"type": "rest", "rest_url": "http://localhost:8082"}},
            },
        },
        "sources": [{"name": "enriched", "topic": "enriched.v1", "columns": [{"name": "id"}]}],
        "models": [
            {
                "name": "users_sink",
                "from": [{"source": "enriched"}],
                "sink": sink,
            }
        ],
    }
    if connections:
        config["connections"] = connections
    return config


def _parse(config: dict):
    with tempfile.TemporaryDirectory() as tmpdir:
        path = Path(tmpdir)
        with open(path / "stream_project.yml", "w") as f:
            yaml.dump(config, f)
        return ProjectParser(path).parse()


def _compile(config: dict):
    with tempfile.TemporaryDirectory() as tmpdir:
        path = Path(tmpdir)
        with open(path / "stream_project.yml", "w") as f:
            yaml.dump(config, f)
        project = ProjectParser(path).parse()
        output_dir = path / "gen"
        return Compiler(project, output_dir).compile(dry_run=True)


class TestGlobalConnections:
    def test_connection_config_merges_into_sink(self):
        """Connection base config is merged, sink-specific overrides take precedence."""
        connections = {
            "sf_prod": {
                "type": "snowflake",
                "config": {
                    "snowflake.url.name": "acme.snowflakecomputing.com",
                    "snowflake.user.name": "svc_user",
                    "snowflake.private.key": "${SF_KEY}",
                    "snowflake.database.name": "ANALYTICS",
                    "snowflake.schema.name": "PUBLIC",
                },
            },
        }
        manifest = _compile(_project_config(connections=connections, sink_connection="sf_prod"))
        connector = manifest.artifacts["connectors"][0]
        cfg = connector["config"]
        # Connection config merged
        assert cfg["snowflake.url.name"] == "acme.snowflakecomputing.com"
        assert cfg["snowflake.database.name"] == "ANALYTICS"
        # Sink-specific config preserved
        assert cfg["snowflake.topic2table.map"] == "enriched:USERS"

    def test_sink_overrides_connection(self):
        """Sink config takes precedence over connection config."""
        connections = {
            "sf_prod": {
                "type": "snowflake",
                "config": {
                    "snowflake.database.name": "DEFAULT_DB",
                },
            },
        }
        config = _project_config(connections=connections, sink_connection="sf_prod")
        config["models"][0]["sink"]["config"]["snowflake.database.name"] = "OVERRIDE_DB"
        manifest = _compile(config)
        connector = manifest.artifacts["connectors"][0]
        assert connector["config"]["snowflake.database.name"] == "OVERRIDE_DB"

    def test_no_connection_reference(self):
        """Sink without connection reference works as before."""
        manifest = _compile(_project_config())
        connector = manifest.artifacts["connectors"][0]
        assert connector["config"]["snowflake.topic2table.map"] == "enriched:USERS"

    def test_connection_parsed_on_project(self):
        """Connections are accessible on parsed project."""
        project = _parse(
            _project_config(
                connections={"s3": {"type": "s3", "config": {"s3.bucket": "my-bucket"}}}
            )
        )
        assert "s3" in project.connections
        assert project.connections["s3"].config["s3.bucket"] == "my-bucket"

    def test_empty_connections(self):
        """Empty connections dict is fine."""
        project = _parse(_project_config(connections={}))
        assert project.connections == {}
