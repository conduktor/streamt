"""Manifest for compiled streamt projects."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional


@dataclass
class TopicArtifact:
    """Compiled topic artifact."""

    name: str
    partitions: int
    replication_factor: int
    config: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "partitions": self.partitions,
            "replication_factor": self.replication_factor,
            "config": self.config,
        }


@dataclass
class FlinkJobArtifact:
    """Compiled Flink job artifact."""

    name: str
    sql: str
    cluster: Optional[str] = None
    parallelism: Optional[int] = None
    checkpoint_interval_ms: Optional[int] = None
    state_backend: Optional[str] = None
    state_ttl_ms: Optional[int] = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "sql": self.sql,
            "cluster": self.cluster,
            "parallelism": self.parallelism,
            "checkpoint_interval_ms": self.checkpoint_interval_ms,
            "state_backend": self.state_backend,
            "state_ttl_ms": self.state_ttl_ms,
        }


@dataclass
class ConnectorArtifact:
    """Compiled Connect connector artifact."""

    name: str
    connector_class: str
    topics: list[str]
    config: dict[str, Any] = field(default_factory=dict)
    cluster: Optional[str] = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "connector_class": self.connector_class,
            "topics": self.topics,
            "cluster": self.cluster,
            "config": {
                "name": self.name,
                "connector.class": self.connector_class,
                "topics": ",".join(self.topics),
                **self.config,
            },
        }


@dataclass
class GatewayRuleArtifact:
    """Compiled Gateway rule artifact."""

    name: str
    virtual_topic: str
    physical_topic: str
    interceptors: list[dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "virtualTopic": self.virtual_topic,
            "physicalTopic": self.physical_topic,
            "interceptors": self.interceptors,
        }


@dataclass
class SchemaArtifact:
    """Compiled schema artifact."""

    subject: str
    schema: dict[str, Any]
    schema_type: str = "AVRO"  # AVRO, JSON, PROTOBUF
    compatibility: Optional[str] = None  # BACKWARD, FORWARD, FULL, NONE

    def to_dict(self) -> dict[str, Any]:
        return {
            "subject": self.subject,
            "schema": self.schema,
            "schema_type": self.schema_type,
            "compatibility": self.compatibility,
        }


@dataclass
class Manifest:
    """Manifest of compiled streamt project."""

    version: str
    project_name: str
    compiled_at: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    )
    sources: list[dict[str, Any]] = field(default_factory=list)
    models: list[dict[str, Any]] = field(default_factory=list)
    tests: list[dict[str, Any]] = field(default_factory=list)
    exposures: list[dict[str, Any]] = field(default_factory=list)
    dag: dict[str, Any] = field(default_factory=dict)
    artifacts: dict[str, list[dict[str, Any]]] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "version": self.version,
            "project": self.project_name,
            "compiled_at": self.compiled_at,
            "sources": self.sources,
            "models": self.models,
            "tests": self.tests,
            "exposures": self.exposures,
            "dag": self.dag,
            "artifacts": self.artifacts,
        }

    def to_json(self, indent: int = 2) -> str:
        return json.dumps(self.to_dict(), indent=indent)

    def save(self, path: Path) -> None:
        """Save manifest to file."""
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w") as f:
            f.write(self.to_json())

    @classmethod
    def load(cls, path: Path) -> "Manifest":
        """Load manifest from file."""
        with open(path) as f:
            data = json.load(f)
        return cls(
            version=data["version"],
            project_name=data["project"],
            compiled_at=data["compiled_at"],
            sources=data.get("sources", []),
            models=data.get("models", []),
            tests=data.get("tests", []),
            exposures=data.get("exposures", []),
            dag=data.get("dag", {}),
            artifacts=data.get("artifacts", {}),
        )
