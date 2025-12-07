"""Test data factories for creating artifacts and test objects.

This module implements the Factory pattern to reduce Feature Envy and
Data Clumps code smells in tests. Instead of manually constructing
artifacts with repeated boilerplate, tests use these factory methods.
"""

import time
import uuid
from typing import Optional

from streamt.compiler.manifest import TopicArtifact, FlinkJobArtifact


class TopicFactory:
    """Factory for creating TopicArtifact instances in tests.

    Usage:
        # Simple topic with defaults
        topic = TopicFactory.create("my_topic")

        # Topic with custom settings
        topic = TopicFactory.create(
            "my_topic",
            partitions=6,
            config={"retention.ms": "86400000"}
        )

        # Unique topic name (for test isolation)
        topic = TopicFactory.create_unique("test_prefix")
    """

    DEFAULT_PARTITIONS = 1
    DEFAULT_REPLICATION_FACTOR = 1

    @classmethod
    def create(
        cls,
        name: str,
        partitions: int = DEFAULT_PARTITIONS,
        replication_factor: int = DEFAULT_REPLICATION_FACTOR,
        config: Optional[dict] = None,
    ) -> TopicArtifact:
        """Create a TopicArtifact with sensible defaults."""
        return TopicArtifact(
            name=name,
            partitions=partitions,
            replication_factor=replication_factor,
            config=config or {},
        )

    @classmethod
    def create_unique(
        cls,
        prefix: str = "test",
        partitions: int = DEFAULT_PARTITIONS,
        replication_factor: int = DEFAULT_REPLICATION_FACTOR,
        config: Optional[dict] = None,
    ) -> TopicArtifact:
        """Create a TopicArtifact with a unique name for test isolation."""
        unique_suffix = f"{int(time.time() * 1000)}_{uuid.uuid4().hex[:6]}"
        name = f"{prefix}_{unique_suffix}"
        return cls.create(name, partitions, replication_factor, config)

    @classmethod
    def create_compacted(
        cls,
        name: str,
        partitions: int = DEFAULT_PARTITIONS,
        replication_factor: int = DEFAULT_REPLICATION_FACTOR,
    ) -> TopicArtifact:
        """Create a compacted topic artifact."""
        return cls.create(
            name=name,
            partitions=partitions,
            replication_factor=replication_factor,
            config={"cleanup.policy": "compact"},
        )

    @classmethod
    def create_with_retention(
        cls,
        name: str,
        retention_ms: int,
        partitions: int = DEFAULT_PARTITIONS,
        replication_factor: int = DEFAULT_REPLICATION_FACTOR,
    ) -> TopicArtifact:
        """Create a topic with specific retention."""
        return cls.create(
            name=name,
            partitions=partitions,
            replication_factor=replication_factor,
            config={"retention.ms": str(retention_ms)},
        )


class FlinkJobFactory:
    """Factory for creating FlinkJobArtifact instances in tests.

    Usage:
        # Simple job
        job = FlinkJobFactory.create("my_model", "SELECT * FROM source")

        # Job with full SQL statements
        job = FlinkJobFactory.create(
            "my_model",
            sql="SELECT * FROM source",
            create_table_sql="CREATE TABLE ...",
        )
    """

    @classmethod
    def create(
        cls,
        model_name: str,
        sql: str,
        create_table_sql: Optional[str] = None,
        insert_sql: Optional[str] = None,
    ) -> FlinkJobArtifact:
        """Create a FlinkJobArtifact with sensible defaults."""
        return FlinkJobArtifact(
            model_name=model_name,
            sql=sql,
            create_table_sql=create_table_sql,
            insert_sql=insert_sql,
        )


def unique_name(prefix: str = "test") -> str:
    """Generate a unique name for test resources."""
    timestamp = int(time.time() * 1000)
    random_suffix = uuid.uuid4().hex[:6]
    return f"{prefix}_{timestamp}_{random_suffix}"
