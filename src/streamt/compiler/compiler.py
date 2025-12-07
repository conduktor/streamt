"""Compiler for streamt projects."""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Optional

from jinja2 import BaseLoader, Environment

from streamt.compiler.manifest import (
    ConnectorArtifact,
    FlinkJobArtifact,
    GatewayRuleArtifact,
    Manifest,
    SchemaArtifact,
    TopicArtifact,
)
from streamt.core.dag import DAGBuilder
from streamt.core.models import (
    MaterializedType,
    Model,
    Source,
    StreamtProject,
    Test,
    TopicDefaults,
)
from streamt.core.parser import ProjectParser

# Connector class mapping
CONNECTOR_CLASSES = {
    "snowflake-sink": "com.snowflake.kafka.connector.SnowflakeSinkConnector",
    "jdbc-sink": "io.confluent.connect.jdbc.JdbcSinkConnector",
    "s3-sink": "io.confluent.connect.s3.S3SinkConnector",
    "elasticsearch-sink": "io.confluent.connect.elasticsearch.ElasticsearchSinkConnector",
    "bigquery-sink": "com.wepay.kafka.connect.bigquery.BigQuerySinkConnector",
}


class CompileError(Exception):
    """Error during compilation."""

    pass


class Compiler:
    """Compiler for streamt projects."""

    def __init__(self, project: StreamtProject, output_dir: Optional[Path] = None) -> None:
        """Initialize compiler."""
        self.project = project
        self.output_dir = output_dir or (
            project.project_path / "generated" if project.project_path else Path("generated")
        )
        self.parser = ProjectParser(project.project_path) if project.project_path else None

        # Build DAG
        dag_builder = DAGBuilder(project)
        self.dag = dag_builder.build()

        # Jinja environment
        self.jinja_env = Environment(loader=BaseLoader())

        # Topic defaults
        self._topic_defaults = self._get_topic_defaults()

        # Artifacts
        self.schemas: list[SchemaArtifact] = []
        self.topics: list[TopicArtifact] = []
        self.flink_jobs: list[FlinkJobArtifact] = []
        self.connectors: list[ConnectorArtifact] = []
        self.gateway_rules: list[GatewayRuleArtifact] = []

    def _get_topic_defaults(self) -> TopicDefaults:
        """Get topic defaults from project config."""
        # Check project-level defaults.topic first
        if self.project.defaults and self.project.defaults.topic:
            return self.project.defaults.topic
        # Then check defaults.models.topic
        if self.project.defaults and self.project.defaults.models and self.project.defaults.models.topic:
            return self.project.defaults.models.topic
        # Return sensible defaults (1/1 works everywhere including local dev)
        return TopicDefaults()

    def compile(self, dry_run: bool = False) -> Manifest:
        """Compile the project."""
        # Clear previous artifacts
        self.schemas = []
        self.topics = []
        self.flink_jobs = []
        self.connectors = []
        self.gateway_rules = []

        # Compile schemas from sources with schema definitions
        for source in self.project.sources:
            self._compile_source_schema(source)

        # Compile models in topological order
        model_order = self.dag.get_models_only()
        for model_name in model_order:
            model = self.project.get_model(model_name)
            if model:
                self._compile_model(model)

        # Compile continuous tests as Flink jobs
        for test in self.project.tests:
            if test.type.value == "continuous":
                self._compile_continuous_test(test)

        # Create manifest
        manifest = self._create_manifest()

        # Write artifacts
        if not dry_run:
            self._write_artifacts()

        return manifest

    def _compile_source_schema(self, source: Source) -> None:
        """Compile schema artifact from a source with schema definition."""
        if not source.schema_:
            return

        # Generate subject name (topic-value is the convention)
        subject = source.schema_.subject or f"{source.topic}-value"

        # Get schema definition - either inline or reference
        if source.schema_.definition:
            try:
                schema = json.loads(source.schema_.definition)
            except json.JSONDecodeError:
                # Assume it's already a dict-like definition
                schema = {"type": "record", "name": source.name, "fields": []}
        else:
            # Generate basic schema from columns if available
            schema = self._generate_schema_from_columns(source)

        if schema:
            schema_type = source.schema_.format or "AVRO"
            self.schemas.append(
                SchemaArtifact(
                    subject=subject,
                    schema=schema,
                    schema_type=schema_type.upper(),
                )
            )

    def _generate_schema_from_columns(self, source: Source) -> dict | None:
        """Generate Avro schema from source columns."""
        if not source.columns:
            return None

        fields = []
        for col in source.columns:
            # Default to string type, can be enhanced with type mapping
            fields.append({
                "name": col.name,
                "type": ["null", "string"],
                "default": None,
                "doc": col.description or "",
            })

        return {
            "type": "record",
            "name": source.name.replace("-", "_").replace(".", "_"),
            "namespace": "com.streamt",
            "fields": fields,
        }

    def _compile_model(self, model: Model) -> None:
        """Compile a single model."""
        if model.materialized == MaterializedType.TOPIC:
            self._compile_topic_model(model)
        elif model.materialized == MaterializedType.VIRTUAL_TOPIC:
            self._compile_virtual_topic_model(model)
        elif model.materialized == MaterializedType.FLINK:
            self._compile_flink_model(model)
        elif model.materialized == MaterializedType.SINK:
            self._compile_sink_model(model)

    def _compile_topic_model(self, model: Model) -> None:
        """Compile a topic model (creates real Kafka topic)."""
        topic_name = model.topic.name if model.topic and model.topic.name else model.name
        partitions = (model.topic.partitions if model.topic and model.topic.partitions else None) or self._topic_defaults.partitions
        replication_factor = (model.topic.replication_factor if model.topic and model.topic.replication_factor else None) or self._topic_defaults.replication_factor
        config = model.topic.config if model.topic else {}

        self.topics.append(
            TopicArtifact(
                name=topic_name,
                partitions=partitions,
                replication_factor=replication_factor,
                config=config,
            )
        )

        # If there's SQL transformation, we need a Flink job to populate the topic
        if model.sql:
            self._compile_flink_job_for_topic(model, topic_name)

    def _compile_virtual_topic_model(self, model: Model) -> None:
        """Compile a virtual topic model (Gateway rule)."""
        virtual_topic_name = model.topic.name if model.topic and model.topic.name else model.name

        # Get the source topic
        source_topic = self._get_source_topic(model)
        if not source_topic:
            raise CompileError(
                f"Cannot determine source topic for virtual topic model '{model.name}'"
            )

        # Build interceptors
        interceptors = []

        # Add filter interceptor from SQL WHERE clause
        if model.sql:
            where_clause = self._extract_where_clause(model.sql)
            if where_clause:
                interceptors.append(
                    {
                        "type": "filter",
                        "config": {"where": where_clause},
                    }
                )

        # Add masking interceptors
        if model.security and model.security.policies:
            for policy in model.security.policies:
                if "mask" in policy:
                    mask_config = policy["mask"]
                    interceptors.append(
                        {
                            "type": "mask",
                            "config": {
                                "field": mask_config["column"],
                                "method": mask_config["method"],
                                "forRoles": mask_config.get("for_roles", []),
                            },
                        }
                    )

        self.gateway_rules.append(
            GatewayRuleArtifact(
                name=model.name,
                virtual_topic=virtual_topic_name,
                physical_topic=source_topic,
                interceptors=interceptors,
            )
        )

    def _compile_flink_model(self, model: Model) -> None:
        """Compile a Flink model."""
        # Create output topic
        topic_name = model.topic.name if model.topic and model.topic.name else model.name
        partitions = (model.topic.partitions if model.topic and model.topic.partitions else None) or self._topic_defaults.partitions
        replication_factor = (model.topic.replication_factor if model.topic and model.topic.replication_factor else None) or self._topic_defaults.replication_factor
        config = model.topic.config if model.topic else {}

        self.topics.append(
            TopicArtifact(
                name=topic_name,
                partitions=partitions,
                replication_factor=replication_factor,
                config=config,
            )
        )

        # Generate Flink SQL
        flink_sql = self._generate_flink_sql(model, topic_name)

        self.flink_jobs.append(
            FlinkJobArtifact(
                name=model.name,
                sql=flink_sql,
                cluster=model.flink_cluster,
                parallelism=model.flink.parallelism if model.flink else None,
                checkpoint_interval_ms=model.flink.checkpoint_interval_ms if model.flink else None,
                state_backend=model.flink.state_backend if model.flink else None,
            )
        )

    def _compile_sink_model(self, model: Model) -> None:
        """Compile a sink model (Kafka Connect)."""
        if not model.sink:
            raise CompileError(f"Sink model '{model.name}' has no sink configuration")

        # Get source topic(s)
        source_topics = self._get_source_topics(model)
        if not source_topics:
            raise CompileError(f"Cannot determine source topics for sink model '{model.name}'")

        # Get connector class
        connector_class = CONNECTOR_CLASSES.get(model.sink.connector, model.sink.connector)

        # Build connector config
        config = dict(model.sink.config)

        # Add masking transforms if needed
        if model.security and model.security.policies:
            transforms = []
            transform_configs = {}

            for i, policy in enumerate(model.security.policies):
                if "mask" in policy:
                    mask_config = policy["mask"]
                    transform_name = f"mask{i}"
                    transforms.append(transform_name)
                    transform_configs[f"transforms.{transform_name}.type"] = (
                        "org.apache.kafka.connect.transforms.MaskField$Value"
                    )
                    transform_configs[f"transforms.{transform_name}.fields"] = mask_config["column"]

            if transforms:
                config["transforms"] = ",".join(transforms)
                config.update(transform_configs)

        self.connectors.append(
            ConnectorArtifact(
                name=model.name,
                connector_class=connector_class,
                topics=source_topics,
                config=config,
                cluster=model.connect_cluster,
            )
        )

    def _compile_flink_job_for_topic(self, model: Model, output_topic: str) -> None:
        """Compile a Flink job for a topic model that has SQL."""
        flink_sql = self._generate_flink_sql(model, output_topic)

        self.flink_jobs.append(
            FlinkJobArtifact(
                name=f"{model.name}_processor",
                sql=flink_sql,
                cluster=model.flink_cluster,
            )
        )

    def _compile_continuous_test(self, test: Test) -> None:
        """Compile a continuous test as a Flink monitoring job.

        Generates a Flink job that:
        1. Reads from the model's output topic
        2. Filters rows that violate assertions
        3. Writes violations to _streamt_test_failures topic
        """
        # Get the model being tested
        model = self.project.get_model(test.model)
        if not model:
            # Could be testing a source directly
            source = self.project.get_source(test.model)
            if not source:
                return
            topic_name = source.topic
            columns = [col.name for col in source.columns] if source.columns else []
        else:
            topic_name = model.topic.name if model.topic and model.topic.name else model.name
            # Extract columns from model's SQL
            columns = self._extract_select_columns(model.sql or "")

        # Generate Flink SQL for monitoring
        flink_sql = self._generate_test_flink_sql(test, topic_name, columns)

        self.flink_jobs.append(
            FlinkJobArtifact(
                name=f"test_{test.name}",
                sql=flink_sql,
                cluster=test.flink_cluster,
            )
        )

    def _generate_test_flink_sql(
        self, test: Test, source_topic: str, columns: list[str]
    ) -> str:
        """Generate Flink SQL for a continuous test."""
        bootstrap = self._get_flink_bootstrap_servers()
        sql_parts = []

        # Generate column DDL
        if columns:
            columns_ddl = ",\n    ".join(f"`{col}` STRING" for col in columns)
        else:
            columns_ddl = "`_raw` STRING"

        # Source table (model output)
        sql_parts.append(f"""CREATE TABLE IF NOT EXISTS test_source_{test.name} (
    {columns_ddl}
) WITH (
    'connector' = 'kafka',
    'topic' = '{source_topic}',
    'properties.bootstrap.servers' = '{bootstrap}',
    'scan.startup.mode' = 'latest-offset',
    'format' = 'json'
);""")

        # Failures sink table
        sql_parts.append(f"""CREATE TABLE IF NOT EXISTS test_failures_{test.name} (
    `test_name` STRING,
    `violation_type` STRING,
    `violation_details` STRING,
    `record` STRING,
    `detected_at` TIMESTAMP(3)
) WITH (
    'connector' = 'kafka',
    'topic' = '_streamt_test_failures',
    'properties.bootstrap.servers' = '{bootstrap}',
    'format' = 'json'
);""")

        # Build WHERE clause from assertions
        # Each tuple: (condition_sql, violation_type, column_name)
        violation_conditions: list[tuple[str, str, str]] = []
        for assertion in test.assertions:
            assertion_type = list(assertion.keys())[0]
            config = assertion[assertion_type]

            if assertion_type == "not_null":
                for col in config.get("columns", []):
                    if col in columns:
                        violation_conditions.append(
                            (f"`{col}` IS NULL", f"not_null:{col}", col)
                        )

            elif assertion_type == "accepted_values":
                col = config.get("column")
                values = config.get("values", [])
                if col and col in columns and values:
                    values_str = ", ".join(f"'{v}'" for v in values)
                    violation_conditions.append(
                        (f"`{col}` NOT IN ({values_str})", f"accepted_values:{col}", col)
                    )

            elif assertion_type == "range":
                col = config.get("column")
                min_val = config.get("min")
                max_val = config.get("max")
                if col and col in columns:
                    if min_val is not None:
                        violation_conditions.append(
                            (f"CAST(`{col}` AS DOUBLE) < {min_val}", f"range_min:{col}", col)
                        )
                    if max_val is not None:
                        violation_conditions.append(
                            (f"CAST(`{col}` AS DOUBLE) > {max_val}", f"range_max:{col}", col)
                        )

        # Generate INSERT statement for each violation type
        if violation_conditions:
            union_parts = []
            for condition, violation_type, col_name in violation_conditions:
                union_parts.append(f"""SELECT
    '{test.name}' AS test_name,
    '{violation_type}' AS violation_type,
    CAST(`{col_name}` AS STRING) AS violation_details,
    '' AS record,
    CURRENT_TIMESTAMP AS detected_at
FROM test_source_{test.name}
WHERE {condition}""")

            sql_parts.append(
                f"INSERT INTO test_failures_{test.name}\n"
                + "\nUNION ALL\n".join(union_parts) + ";"
            )

        return "\n\n".join(sql_parts)

    def _generate_flink_sql(self, model: Model, output_topic: str) -> str:
        """Generate Flink SQL for a model."""
        sql_parts = []

        # Generate CREATE TABLE statements for sources
        dependencies = self._get_model_dependencies(model)

        for dep_name, dep_type in dependencies:
            if dep_type == "source":
                source = self.project.get_source(dep_name)
                if source:
                    sql_parts.append(self._generate_source_table_ddl(source, dep_name))
            else:
                dep_model = self.project.get_model(dep_name)
                if dep_model:
                    topic_name = (
                        dep_model.topic.name
                        if dep_model.topic and dep_model.topic.name
                        else dep_model.name
                    )
                    sql_parts.append(
                        self._generate_model_table_ddl(dep_model, dep_name, topic_name)
                    )

        # Generate CREATE TABLE for output
        sql_parts.append(self._generate_sink_table_ddl(model, output_topic))

        # Generate INSERT statement
        transformed_sql = self._transform_sql(model.sql or "")

        # Apply masking functions if needed
        if model.security and model.security.policies:
            for policy in model.security.policies:
                if "mask" in policy:
                    mask_config = policy["mask"]
                    column = mask_config["column"]
                    method = mask_config["method"]
                    mask_fn = self._get_flink_mask_function(method)
                    # Replace column reference with masked version
                    transformed_sql = re.sub(
                        rf"\b{column}\b",
                        f"{mask_fn}({column}) AS {column}",
                        transformed_sql,
                        count=1,
                    )

        sink_table = self._topic_to_table_name(output_topic)
        sql_parts.append(f"INSERT INTO {sink_table}\n{transformed_sql};")

        return "\n\n".join(sql_parts)

    def _get_flink_bootstrap_servers(self) -> str:
        """Get bootstrap servers for Flink (internal if available)."""
        kafka_config = self.project.runtime.kafka
        return kafka_config.bootstrap_servers_internal or kafka_config.bootstrap_servers

    def _generate_source_table_ddl(self, source: Source, alias: str) -> str:
        """Generate Flink CREATE TABLE DDL for a source."""
        bootstrap = self._get_flink_bootstrap_servers()

        # Generate columns from source definition
        if source.columns:
            columns = ",\n    ".join(
                f"`{col.name}` STRING" for col in source.columns
            )
        else:
            columns = "`_raw` STRING"

        return f"""CREATE TABLE IF NOT EXISTS {alias} (
    {columns}
) WITH (
    'connector' = 'kafka',
    'topic' = '{source.topic}',
    'properties.bootstrap.servers' = '{bootstrap}',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json'
);"""

    def _generate_model_table_ddl(self, model: Model, alias: str, topic_name: str) -> str:
        """Generate Flink CREATE TABLE DDL for a model reference."""
        bootstrap = self._get_flink_bootstrap_servers()

        # Try to infer columns from the upstream model's SQL SELECT clause
        columns = self._extract_select_columns(model.sql or "")
        if columns:
            columns_ddl = ",\n    ".join(f"`{col}` STRING" for col in columns)
        else:
            columns_ddl = "`_raw` STRING"

        return f"""CREATE TABLE IF NOT EXISTS {alias} (
    {columns_ddl}
) WITH (
    'connector' = 'kafka',
    'topic' = '{topic_name}',
    'properties.bootstrap.servers' = '{bootstrap}',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json'
);"""

    def _topic_to_table_name(self, topic_name: str) -> str:
        """Convert topic name to valid Flink SQL table name."""
        # Replace dots, dashes, and other special chars with underscores
        return re.sub(r"[.\-]", "_", topic_name) + "_sink"

    def _generate_sink_table_ddl(self, model: Model, topic_name: str) -> str:
        """Generate Flink CREATE TABLE DDL for the output sink."""
        bootstrap = self._get_flink_bootstrap_servers()
        table_name = self._topic_to_table_name(topic_name)

        # Extract columns from SELECT clause
        columns = self._extract_select_columns(model.sql or "")
        if columns:
            columns_ddl = ",\n    ".join(f"`{col}` STRING" for col in columns)
        else:
            columns_ddl = "`_raw` STRING"

        return f"""CREATE TABLE IF NOT EXISTS {table_name} (
    {columns_ddl}
) WITH (
    'connector' = 'kafka',
    'topic' = '{topic_name}',
    'properties.bootstrap.servers' = '{bootstrap}',
    'format' = 'json'
);"""

    def _extract_select_columns(self, sql: str) -> list[str]:
        """Extract column names from SELECT clause."""
        # Remove Jinja templates first
        clean_sql = re.sub(r'\{\{.*?\}\}', 'placeholder', sql)

        # Match SELECT ... FROM
        match = re.search(r'SELECT\s+(.+?)\s+FROM', clean_sql, re.IGNORECASE | re.DOTALL)
        if not match:
            return []

        select_clause = match.group(1)

        # Handle SELECT *
        if select_clause.strip() == '*':
            return []

        columns = []
        for part in select_clause.split(','):
            part = part.strip()
            # Handle "expr AS alias" - take the alias
            if ' AS ' in part.upper():
                alias_match = re.search(r'\s+AS\s+[`"]?(\w+)[`"]?\s*$', part, re.IGNORECASE)
                if alias_match:
                    columns.append(alias_match.group(1))
                    continue

            # Handle simple column reference (possibly with backticks)
            col_match = re.match(r'^[`"]?(\w+)[`"]?$', part)
            if col_match:
                columns.append(col_match.group(1))

        return columns

    def _transform_sql(self, sql: str) -> str:
        """Transform Jinja SQL to plain SQL."""
        # Replace {{ source("name") }} with table name
        sql = re.sub(
            r'\{\{\s*source\s*\(\s*["\']([^"\']+)["\']\s*\)\s*\}\}',
            r"\1",
            sql,
        )
        # Replace {{ ref("name") }} with table name
        sql = re.sub(
            r'\{\{\s*ref\s*\(\s*["\']([^"\']+)["\']\s*\)\s*\}\}',
            r"\1",
            sql,
        )
        return sql.strip()

    def _get_flink_mask_function(self, method: str) -> str:
        """Get Flink masking function for a method."""
        if method == "hash":
            return "MD5"
        elif method == "redact":
            return "REGEXP_REPLACE"  # Will need params
        elif method == "partial":
            return "REGEXP_REPLACE"  # Will need params
        elif method == "null":
            return "NULLIF"
        else:
            return "MD5"  # Default to hash

    def _get_source_topic(self, model: Model) -> Optional[str]:
        """Get the source topic for a model."""
        if model.sql and self.parser:
            sources, refs = self.parser.extract_refs_from_sql(model.sql)
            if sources:
                source = self.project.get_source(sources[0])
                if source:
                    return source.topic
            if refs:
                ref_model = self.project.get_model(refs[0])
                if ref_model:
                    return (
                        ref_model.topic.name
                        if ref_model.topic and ref_model.topic.name
                        else ref_model.name
                    )
        elif model.from_:
            for from_ref in model.from_:
                if from_ref.source:
                    source = self.project.get_source(from_ref.source)
                    if source:
                        return source.topic
                if from_ref.ref:
                    ref_model = self.project.get_model(from_ref.ref)
                    if ref_model:
                        return (
                            ref_model.topic.name
                            if ref_model.topic and ref_model.topic.name
                            else ref_model.name
                        )
        return None

    def _get_source_topics(self, model: Model) -> list[str]:
        """Get all source topics for a model."""
        topics = []

        if model.sql and self.parser:
            sources, refs = self.parser.extract_refs_from_sql(model.sql)
            for source_name in sources:
                source = self.project.get_source(source_name)
                if source:
                    topics.append(source.topic)
            for ref_name in refs:
                ref_model = self.project.get_model(ref_name)
                if ref_model:
                    topics.append(
                        ref_model.topic.name
                        if ref_model.topic and ref_model.topic.name
                        else ref_model.name
                    )
        elif model.from_:
            for from_ref in model.from_:
                if from_ref.source:
                    source = self.project.get_source(from_ref.source)
                    if source:
                        topics.append(source.topic)
                if from_ref.ref:
                    ref_model = self.project.get_model(from_ref.ref)
                    if ref_model:
                        topics.append(
                            ref_model.topic.name
                            if ref_model.topic and ref_model.topic.name
                            else ref_model.name
                        )

        return topics

    def _get_model_dependencies(self, model: Model) -> list[tuple[str, str]]:
        """Get model dependencies as (name, type) tuples."""
        dependencies = []

        if model.sql and self.parser:
            sources, refs = self.parser.extract_refs_from_sql(model.sql)
            for source_name in sources:
                dependencies.append((source_name, "source"))
            for ref_name in refs:
                dependencies.append((ref_name, "model"))
        elif model.from_:
            for from_ref in model.from_:
                if from_ref.source:
                    dependencies.append((from_ref.source, "source"))
                if from_ref.ref:
                    dependencies.append((from_ref.ref, "model"))

        return dependencies

    def _extract_where_clause(self, sql: str) -> Optional[str]:
        """Extract WHERE clause from SQL."""
        match = re.search(
            r"WHERE\s+(.+?)(?:GROUP BY|ORDER BY|LIMIT|$)", sql, re.IGNORECASE | re.DOTALL
        )
        if match:
            return match.group(1).strip()
        return None

    def _create_manifest(self) -> Manifest:
        """Create the manifest."""
        return Manifest(
            version=self.project.project.version or "0.0.0",
            project_name=self.project.project.name,
            sources=[s.model_dump() for s in self.project.sources],
            models=[m.model_dump() for m in self.project.models],
            tests=[t.model_dump() for t in self.project.tests],
            exposures=[e.model_dump() for e in self.project.exposures],
            dag=self.dag.to_dict(),
            artifacts={
                "schemas": [s.to_dict() for s in self.schemas],
                "topics": [t.to_dict() for t in self.topics],
                "flink_jobs": [f.to_dict() for f in self.flink_jobs],
                "connectors": [c.to_dict() for c in self.connectors],
                "gateway_rules": [g.to_dict() for g in self.gateway_rules],
            },
        )

    def _write_artifacts(self) -> None:
        """Write all artifacts to output directory."""
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # Write schemas
        if self.schemas:
            schemas_dir = self.output_dir / "schemas"
            schemas_dir.mkdir(exist_ok=True)
            for schema in self.schemas:
                # Write schema file
                path = schemas_dir / f"{schema.subject}.json"
                with open(path, "w") as f:
                    json.dump(schema.to_dict(), f, indent=2)

        # Write topics
        topics_dir = self.output_dir / "topics"
        topics_dir.mkdir(exist_ok=True)
        for topic in self.topics:
            path = topics_dir / f"{topic.name}.json"
            with open(path, "w") as f:
                json.dump(topic.to_dict(), f, indent=2)

        # Write Flink jobs
        flink_dir = self.output_dir / "flink"
        flink_dir.mkdir(exist_ok=True)
        for job in self.flink_jobs:
            # Write SQL file
            sql_path = flink_dir / f"{job.name}.sql"
            with open(sql_path, "w") as f:
                f.write(job.sql)
            # Write config file
            config_path = flink_dir / f"{job.name}.json"
            with open(config_path, "w") as f:
                json.dump(job.to_dict(), f, indent=2)

        # Write connectors
        connect_dir = self.output_dir / "connect"
        connect_dir.mkdir(exist_ok=True)
        for connector in self.connectors:
            path = connect_dir / f"{connector.name}.json"
            with open(path, "w") as f:
                json.dump(connector.to_dict(), f, indent=2)

        # Write gateway rules
        if self.gateway_rules:
            gateway_dir = self.output_dir / "gateway"
            gateway_dir.mkdir(exist_ok=True)
            for rule in self.gateway_rules:
                path = gateway_dir / f"{rule.name}.json"
                with open(path, "w") as f:
                    json.dump(rule.to_dict(), f, indent=2)

        # Write manifest
        manifest = self._create_manifest()
        manifest.save(self.output_dir / "manifest.json")
