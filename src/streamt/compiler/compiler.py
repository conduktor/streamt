"""Compiler for streamt projects."""

from __future__ import annotations

import json
import logging
import re
from pathlib import Path
from typing import Optional

import sqlglot
from jinja2 import BaseLoader, Environment
from sqlglot import exp

from streamt.compiler.flink_dialect import FlinkDialect, get_flink_function_type
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
    DataTest,
    EventTimeConfig,
    MaterializedType,
    Model,
    Source,
    StreamtProject,
    TopicDefaults,
    WatermarkStrategy,
)
from streamt.core.parser import ProjectParser

logger = logging.getLogger(__name__)

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
        materialized = model.get_materialized()

        # Handle VIRTUAL_TOPIC fallback to FLINK when Gateway is not available
        if materialized == MaterializedType.VIRTUAL_TOPIC:
            has_gateway = (
                self.project.runtime.conduktor
                and self.project.runtime.conduktor.gateway
            )
            is_explicit_virtual_topic = model.gateway and model.gateway.virtual_topic

            if not has_gateway and not is_explicit_virtual_topic:
                # Auto-detected stateless SQL but no Gateway → fallback to Flink
                materialized = MaterializedType.FLINK

        if materialized == MaterializedType.TOPIC:
            self._compile_topic_model(model)
        elif materialized == MaterializedType.VIRTUAL_TOPIC:
            self._compile_virtual_topic_model(model)
        elif materialized == MaterializedType.FLINK:
            self._compile_flink_model(model)
        elif materialized == MaterializedType.SINK:
            self._compile_sink_model(model)

    def _compile_topic_model(self, model: Model) -> None:
        """Compile a topic model (creates real Kafka topic)."""
        topic_name = model.get_topic_config().name if model.get_topic_config() and model.get_topic_config().name else model.name
        partitions = (model.get_topic_config().partitions if model.get_topic_config() and model.get_topic_config().partitions else None) or self._topic_defaults.partitions
        replication_factor = (model.get_topic_config().replication_factor if model.get_topic_config() and model.get_topic_config().replication_factor else None) or self._topic_defaults.replication_factor
        config = model.get_topic_config().config if model.get_topic_config() else {}

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
        virtual_topic_name = model.get_topic_config().name if model.get_topic_config() and model.get_topic_config().name else model.name

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
        topic_name = model.get_topic_config().name if model.get_topic_config() and model.get_topic_config().name else model.name
        partitions = (model.get_topic_config().partitions if model.get_topic_config() and model.get_topic_config().partitions else None) or self._topic_defaults.partitions
        replication_factor = (model.get_topic_config().replication_factor if model.get_topic_config() and model.get_topic_config().replication_factor else None) or self._topic_defaults.replication_factor
        config = model.get_topic_config().config if model.get_topic_config() else {}

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
                cluster=model.get_flink_cluster(),
                parallelism=model.get_flink_config().parallelism if model.get_flink_config() else None,
                checkpoint_interval_ms=model.get_flink_config().checkpoint_interval_ms if model.get_flink_config() else None,
                state_backend=model.get_flink_config().state_backend if model.get_flink_config() else None,
                state_ttl_ms=model.get_flink_config().state_ttl_ms if model.get_flink_config() else None,
            )
        )

    def _compile_sink_model(self, model: Model) -> None:
        """Compile a sink model (Kafka Connect)."""
        sink_config = model.get_sink_config()
        if not sink_config:
            raise CompileError(f"Sink model '{model.name}' has no sink configuration")

        # Get source topic(s)
        source_topics = self._get_source_topics(model)
        if not source_topics:
            raise CompileError(f"Cannot determine source topics for sink model '{model.name}'")

        # Get connector class
        connector_class = CONNECTOR_CLASSES.get(sink_config.connector, sink_config.connector)

        # Build connector config
        config = dict(sink_config.config)

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
                cluster=model.get_connect_cluster(),
            )
        )

    def _compile_flink_job_for_topic(self, model: Model, output_topic: str) -> None:
        """Compile a Flink job for a topic model that has SQL."""
        flink_sql = self._generate_flink_sql(model, output_topic)

        self.flink_jobs.append(
            FlinkJobArtifact(
                name=f"{model.name}_processor",
                sql=flink_sql,
                cluster=model.get_flink_cluster(),
            )
        )

    def _compile_continuous_test(self, test: DataTest) -> None:
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
            topic_name = model.get_topic_config().name if model.get_topic_config() and model.get_topic_config().name else model.name
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

    def _get_type_cast_expression(self, user_type: str) -> str:
        """Map user-friendly type names to Flink SQL type expressions.

        Args:
            user_type: User-provided type name (e.g., "string", "number", "timestamp")

        Returns:
            Flink SQL type expression, or empty string if unsupported
        """
        type_mapping = {
            "string": "STRING",
            "str": "STRING",
            "text": "STRING",
            "number": "DOUBLE",
            "numeric": "DOUBLE",
            "double": "DOUBLE",
            "float": "DOUBLE",
            "int": "INT",
            "integer": "INT",
            "bigint": "BIGINT",
            "long": "BIGINT",
            "boolean": "BOOLEAN",
            "bool": "BOOLEAN",
            "timestamp": "TIMESTAMP(3)",
            "datetime": "TIMESTAMP(3)",
            "date": "DATE",
            "time": "TIME",
        }
        return type_mapping.get(user_type.lower(), "")

    def _generate_test_flink_sql(
        self, test: DataTest, source_topic: str, columns: list[str]
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

            elif assertion_type == "accepted_types":
                types = config.get("types", {})
                for col, expected_type in types.items():
                    if col in columns:
                        # Generate type validation condition
                        # We try to cast to the expected type, and if it fails (returns NULL), it's a violation
                        type_cast = self._get_type_cast_expression(expected_type)
                        if type_cast:
                            violation_conditions.append(
                                (f"TRY_CAST(`{col}` AS {type_cast}) IS NULL AND `{col}` IS NOT NULL",
                                 f"accepted_types:{col}", col)
                            )

            elif assertion_type == "custom_sql":
                # Custom SQL assertion - user provides the WHERE condition
                name = config.get("name", "custom")
                where_clause = config.get("where")
                detail_column = config.get("detail_column", columns[0] if columns else "_raw")

                if where_clause and detail_column in columns:
                    violation_conditions.append(
                        (where_clause, f"custom_sql:{name}", detail_column)
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

        # Generate SET statements for Flink configuration
        set_statements = self._generate_flink_set_statements(model)
        if set_statements:
            sql_parts.append(set_statements)

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
                        dep_model.get_topic_config().name
                        if dep_model.get_topic_config() and dep_model.get_topic_config().name
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
        column_lines = []
        if source.columns:
            for col in source.columns:
                # Handle proctime columns (processing time attribute)
                if col.proctime:
                    column_lines.append(f"`{col.name}` AS PROCTIME()")
                # Determine column type - for event time columns, use TIMESTAMP(3)
                elif source.event_time and col.name == source.event_time.column:
                    column_lines.append(f"`{col.name}` TIMESTAMP(3)")
                # Use type from YAML if specified, otherwise default to STRING
                elif col.type:
                    column_lines.append(f"`{col.name}` {col.type}")
                else:
                    column_lines.append(f"`{col.name}` STRING")
        else:
            column_lines.append("`_raw` STRING")

        # Add watermark if event_time is configured
        watermark_ddl = ""
        if source.event_time:
            watermark_ddl = self._generate_watermark_ddl(source.event_time)
            if watermark_ddl:
                column_lines.append(watermark_ddl)

        columns = ",\n    ".join(column_lines)

        return f"""CREATE TABLE IF NOT EXISTS {alias} (
    {columns}
) WITH (
    'connector' = 'kafka',
    'topic' = '{source.topic}',
    'properties.bootstrap.servers' = '{bootstrap}',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json'
);"""

    def _generate_flink_set_statements(self, model: Model) -> str:
        """Generate SET statements for Flink job configuration."""
        statements = []

        if model.get_flink_config():
            # Parallelism
            if model.get_flink_config().parallelism:
                statements.append(f"SET 'parallelism.default' = '{model.get_flink_config().parallelism}';")

            # State TTL (table.exec.state.ttl)
            if model.get_flink_config().state_ttl_ms:
                # Convert milliseconds to Flink duration format (e.g., "24 h", "30 min", "5 s")
                ttl_ms = model.get_flink_config().state_ttl_ms
                if ttl_ms >= 3600000 and ttl_ms % 3600000 == 0:
                    ttl_str = f"{ttl_ms // 3600000} h"
                elif ttl_ms >= 60000 and ttl_ms % 60000 == 0:
                    ttl_str = f"{ttl_ms // 60000} min"
                elif ttl_ms >= 1000 and ttl_ms % 1000 == 0:
                    ttl_str = f"{ttl_ms // 1000} s"
                else:
                    ttl_str = f"{ttl_ms} ms"
                statements.append(f"SET 'table.exec.state.ttl' = '{ttl_str}';")

            # Checkpoint interval
            if model.get_flink_config().checkpoint_interval_ms:
                interval_ms = model.get_flink_config().checkpoint_interval_ms
                statements.append(f"SET 'execution.checkpointing.interval' = '{interval_ms}ms';")

        return "\n".join(statements)

    def _generate_watermark_ddl(self, event_time: EventTimeConfig) -> str:
        """Generate watermark DDL clause for event time configuration."""
        column = event_time.column

        if event_time.watermark:
            if event_time.watermark.strategy == WatermarkStrategy.MONOTONOUSLY_INCREASING:
                return f"WATERMARK FOR `{column}` AS `{column}`"
            else:
                # bounded_out_of_orderness (default)
                delay_ms = event_time.watermark.max_out_of_orderness_ms or 5000
                delay_seconds = delay_ms / 1000
                return f"WATERMARK FOR `{column}` AS `{column}` - INTERVAL '{int(delay_seconds)}' SECOND"
        else:
            # Default: 5 seconds out of orderness
            return f"WATERMARK FOR `{column}` AS `{column}` - INTERVAL '5' SECOND"

    def _generate_model_table_ddl(self, model: Model, alias: str, topic_name: str) -> str:
        """Generate Flink CREATE TABLE DDL for a model reference."""
        bootstrap = self._get_flink_bootstrap_servers()

        # Try to infer columns with types from the upstream model's SQL SELECT clause
        # Pass the model to enable schema resolution from source definitions
        columns_with_types = self._extract_select_columns_with_types(model.sql or "", model=model)
        if columns_with_types:
            columns_ddl = ",\n    ".join(f"`{col}` {col_type}" for col, col_type in columns_with_types)
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

        # Extract columns with inferred types from SELECT clause
        # Pass the model to enable schema resolution from source definitions
        columns_with_types = self._extract_select_columns_with_types(model.sql or "", model=model)
        if columns_with_types:
            columns_ddl = ",\n    ".join(f"`{col}` {col_type}" for col, col_type in columns_with_types)
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
        return [col for col, _ in self._extract_select_columns_with_types(sql)]

    def _build_source_schema(self, model: Model) -> dict[str, str]:
        """Build a schema dictionary from model dependencies.

        Returns dict mapping column_name → Flink SQL type.
        """
        schema: dict[str, str] = {}
        dependencies = self._get_model_dependencies(model)

        for dep_name, dep_type in dependencies:
            if dep_type == "source":
                source = self.project.get_source(dep_name)
                if source and source.columns:
                    for col in source.columns:
                        # Use the type from YAML, or default based on context
                        if col.proctime:
                            schema[col.name] = "TIMESTAMP_LTZ(3)"
                        elif source.event_time and col.name == source.event_time.column:
                            schema[col.name] = "TIMESTAMP(3)"
                        elif col.type:
                            schema[col.name] = col.type
                        else:
                            schema[col.name] = "STRING"
            else:
                # For model references, recursively build schema from upstream model
                dep_model = self.project.get_model(dep_name)
                if dep_model and dep_model.sql:
                    # Recursively build schema for the upstream model first
                    upstream_schema = self._build_source_schema(dep_model)
                    # Now extract column types using the upstream schema context
                    dep_columns = self._extract_select_columns_with_types(
                        dep_model.sql, schema_context=upstream_schema
                    )
                    for col_name, col_type in dep_columns:
                        schema[col_name] = col_type

        return schema

    def _extract_select_columns_with_types(
        self, sql: str, schema_context: Optional[dict[str, str]] = None, model: Optional[Model] = None
    ) -> list[tuple[str, str]]:
        """Extract column names and infer types from SELECT clause using sqlglot.

        Args:
            sql: The SQL query to parse
            schema_context: Optional pre-built schema context
            model: Optional model to build schema context from

        Returns list of (column_name, flink_type) tuples.
        """
        # Build schema context if not provided
        if schema_context is None and model is not None:
            schema_context = self._build_source_schema(model)
        elif schema_context is None:
            schema_context = {}

        # Store model for ML_PREDICT type inference (accessed in _infer_expression_type)
        self._current_model = model

        # Clean Jinja templates for parsing (replace with valid identifiers)
        clean_sql = re.sub(r'\{\{\s*source\s*\(\s*["\'](\w+)["\']\s*\)\s*\}\}', r'\1', sql)
        clean_sql = re.sub(r'\{\{\s*ref\s*\(\s*["\'](\w+)["\']\s*\)\s*\}\}', r'\1', clean_sql)

        try:
            # Parse SQL with FlinkDialect for proper Flink SQL support
            parsed = sqlglot.parse_one(clean_sql, dialect=FlinkDialect)
            if not isinstance(parsed, exp.Select):
                # Might be wrapped in other statements
                select = parsed.find(exp.Select)
                if not select:
                    self._current_model = None
                    return []
                parsed = select

            columns = []
            for expr in parsed.expressions:
                col_name = self._get_expression_alias(expr)
                col_type = self._infer_expression_type(expr, schema_context)
                if col_name:
                    columns.append((col_name, col_type))

            self._current_model = None
            return columns

        except Exception as e:
            logger.debug(f"sqlglot parse failed, falling back to regex: {e}")
            self._current_model = None
            # Fallback to regex-based extraction
            return self._extract_select_columns_with_types_regex(sql, schema_context)

    def _get_expression_alias(self, expr: exp.Expression) -> Optional[str]:
        """Get the output column name for an expression."""
        # If it has an alias, use that
        if isinstance(expr, exp.Alias):
            return expr.alias
        # If it's a column reference, use the column name
        if isinstance(expr, exp.Column):
            return expr.name
        # For other expressions without aliases, this is invalid SQL
        return None

    def _infer_expression_type(self, expr: exp.Expression, schema: dict[str, str]) -> str:
        """Infer Flink SQL type from a sqlglot expression.

        Uses the schema context to resolve column reference types.
        Uses _current_model (set during extraction) for ML_PREDICT type inference.
        """
        # Unwrap alias to get the actual expression
        if isinstance(expr, exp.Alias):
            expr = expr.this

        # Column reference - look up in schema
        if isinstance(expr, exp.Column):
            col_name = expr.name
            if col_name in schema:
                return schema[col_name]
            upper_name = col_name.upper()
            if upper_name == "$ROWTIME":
                return "TIMESTAMP_LTZ(3)"
            if upper_name == "ROWTIME":
                return "TIMESTAMP(3)"
            if upper_name in ("$PROCTIME", "PROCTIME"):
                return "TIMESTAMP_LTZ(3)"
            if upper_name in ("WINDOW_START", "WINDOW_END", "WINDOW_TIME"):
                return "TIMESTAMP(3)"
            return "STRING"

        # Aggregate functions
        if isinstance(expr, exp.Count):
            return "BIGINT"
        if isinstance(expr, exp.Sum):
            if expr.this is None:
                return "DOUBLE"
            input_type = self._infer_expression_type(expr.this, schema)
            base_type = input_type.split("(")[0].upper()
            if base_type in ("DECIMAL", "NUMERIC"):
                return input_type
            if base_type in ("FLOAT", "DOUBLE"):
                return "DOUBLE"
            if base_type in ("TINYINT", "SMALLINT", "INT", "INTEGER", "BIGINT"):
                return "BIGINT"
            return "DOUBLE"
        if isinstance(expr, exp.Avg):
            if expr.this is None:
                return "DOUBLE"
            input_type = self._infer_expression_type(expr.this, schema)
            base_type = input_type.split("(")[0].upper()
            if base_type in ("DECIMAL", "NUMERIC"):
                return input_type
            return "DOUBLE"
        if isinstance(expr, (exp.Min, exp.Max)):
            if expr.this is not None:
                return self._infer_expression_type(expr.this, schema)
            return "STRING"
        if isinstance(
            expr,
            (
                exp.Stddev,
                exp.StddevPop,
                exp.StddevSamp,
                exp.Variance,
                exp.VariancePop,
                exp.CumeDist,
                exp.PercentRank,
            ),
        ):
            return "DOUBLE"

        # Case expression - infer from THEN/ELSE branches
        if isinstance(expr, exp.Case):
            branch_types: list[str] = []
            for when in expr.args.get("ifs", []):
                then_expr = when.args.get("true")
                if then_expr is not None:
                    branch_types.append(self._infer_expression_type(then_expr, schema))
            else_expr = expr.args.get("default")
            if else_expr is not None:
                branch_types.append(self._infer_expression_type(else_expr, schema))
            return self._merge_types(branch_types)

        # IF function - infer type from THEN branch
        if isinstance(expr, exp.If):
            return self._merge_types(
                [
                    self._infer_expression_type(expr.args.get("true"), schema)
                    if expr.args.get("true") is not None
                    else "",
                    self._infer_expression_type(expr.args.get("false"), schema)
                    if expr.args.get("false") is not None
                    else "",
                ]
            )

        # Coalesce - infer type from all arguments
        if isinstance(expr, exp.Coalesce):
            return self._merge_types(
                [self._infer_expression_type(arg, schema) for arg in expr.iter_expressions()]
            )
        if isinstance(expr, exp.Nullif):
            if expr.this is not None:
                return self._infer_expression_type(expr.this, schema)
            return "STRING"

        # String functions
        if isinstance(expr, (exp.Upper, exp.Lower, exp.Concat, exp.ConcatWs, exp.Substring, exp.Trim)):
            return "STRING"

        # Numeric literals
        if isinstance(expr, exp.Literal):
            if expr.is_int:
                return "INT"
            if expr.is_number:
                return "DOUBLE"
            if expr.is_string:
                return "STRING"

        if isinstance(expr, exp.Extract):
            return "BIGINT"

        if isinstance(expr, exp.Round):
            value_type = (
                self._infer_expression_type(expr.this, schema)
                if expr.this is not None
                else "DOUBLE"
            )
            return value_type if self._is_numeric_type(value_type) else "DOUBLE"

        if isinstance(expr, exp.Rand):
            return "DOUBLE"

        if isinstance(expr, exp.Uuid):
            return "STRING"

        if isinstance(expr, exp.Array):
            return self._infer_array_literal_type(expr, schema)

        if isinstance(expr, exp.Bracket):
            if expr.this is None:
                return "STRING"
            container_type = self._infer_expression_type(expr.this, schema)
            element_type = self._extract_array_element_type(container_type)
            if element_type:
                return element_type
            key_value = self._extract_map_key_value_types(container_type)
            if key_value:
                return key_value[1]
            return "STRING"

        # Boolean literals and comparisons
        if isinstance(expr, exp.Boolean):
            return "BOOLEAN"
        if isinstance(
            expr,
            (
                exp.EQ,
                exp.NEQ,
                exp.GT,
                exp.GTE,
                exp.LT,
                exp.LTE,
                exp.And,
                exp.Or,
                exp.Not,
                exp.In,
                exp.Between,
                exp.Like,
                exp.ILike,
                exp.Is,
                exp.IsNullValue,
                exp.Exists,
                exp.RegexpLike,
                exp.RegexpILike,
                exp.RegexpFullMatch,
            ),
        ):
            return "BOOLEAN"

        # Arithmetic operations - use type promotion
        if isinstance(expr, exp.Div):
            left_type = self._infer_expression_type(expr.left, schema) if expr.left else "STRING"
            right_type = self._infer_expression_type(expr.right, schema) if expr.right else "STRING"
            return self._infer_division_type(left_type, right_type)
        if isinstance(expr, (exp.Add, exp.Sub, exp.Mul, exp.Mod)):
            left_type = self._infer_expression_type(expr.left, schema) if expr.left else "STRING"
            right_type = self._infer_expression_type(expr.right, schema) if expr.right else "STRING"
            return self._promote_numeric_types(left_type, right_type)

        # Window functions
        if isinstance(expr, exp.Window):
            inner = expr.this
            if inner:
                return self._infer_expression_type(inner, schema)
            return "STRING"

        # Ranking window functions
        if isinstance(expr, (exp.RowNumber, exp.Rank, exp.DenseRank, exp.Ntile)):
            return "BIGINT"

        # LAG/LEAD - preserve argument type
        if isinstance(expr, (exp.Lag, exp.Lead)):
            if expr.this is not None:
                return self._infer_expression_type(expr.this, schema)
            return "STRING"

        # FirstValue/LastValue - preserve argument type
        if isinstance(expr, (exp.FirstValue, exp.LastValue, exp.NthValue)):
            if expr.this is not None:
                return self._infer_expression_type(expr.this, schema)
            return "STRING"

        # Timestamp conversion functions
        if isinstance(expr, (exp.TsOrDsToTimestamp, exp.StrToTime, exp.UnixToTime)):
            return "TIMESTAMP(3)"
        if isinstance(expr, exp.TimeToUnix):
            return "BIGINT"
        if isinstance(expr, (exp.TimeToStr, exp.UnixToStr)):
            return "STRING"

        # Anonymous functions (like TUMBLE_START, PROCTIME, etc.)
        if isinstance(expr, exp.Anonymous):
            func_name = expr.name.upper()

            # Handle ML_PREDICT specially - use ml_outputs if declared
            if func_name in ("ML_PREDICT", "ML_EVALUATE"):
                return self._infer_ml_predict_type(expr)

            # Check Flink-specific window functions first
            flink_type = get_flink_function_type(func_name)
            if flink_type:
                return flink_type
            args = list(expr.expressions)
            if func_name in ("IFNULL", "NVL", "GREATEST", "LEAST"):
                return self._merge_types(
                    [self._infer_expression_type(arg, schema) for arg in args]
                )
            if func_name == "TIMESTAMPADD":
                if len(args) >= 3:
                    return self._infer_expression_type(args[2], schema)
                return "TIMESTAMP(3)"
            if func_name == "COLLECT":
                return self._infer_collect_type(args, schema)
            if func_name == "ELEMENT":
                return self._infer_element_type(args, schema)
            if func_name in (
                "ARRAY_CONCAT",
                "ARRAY_DISTINCT",
                "ARRAY_REMOVE",
                "ARRAY_REVERSE",
                "ARRAY_SLICE",
                "ARRAY_UNION",
            ):
                return self._infer_array_return_type(args, schema)
            if func_name == "MAP_KEYS":
                return self._infer_map_keys_type(args, schema)
            if func_name == "MAP_VALUES":
                return self._infer_map_values_type(args, schema)
            if func_name == "MAP_ENTRIES":
                return self._infer_map_entries_type(args, schema)
            if func_name == "MAP_UNION":
                return self._infer_map_union_type(args, schema)
            if func_name == "MAP_FROM_ARRAYS":
                return self._infer_map_from_arrays_type(args, schema)
            if func_name == "STR_TO_MAP":
                return "MAP<STRING, STRING>"
            # Timestamp conversion functions
            if func_name in ("TO_TIMESTAMP", "FROM_UNIXTIME"):
                return "TIMESTAMP(3)"
            # String functions
            if func_name in ("UPPER", "LOWER", "CONCAT", "CONCAT_WS", "SUBSTRING", "TRIM", "REGEXP_REPLACE"):
                return "STRING"
            # Window boundaries
            if func_name in ("WINDOW_START", "WINDOW_END"):
                return "TIMESTAMP(3)"
            # JSON functions usually return STRING
            if func_name in ("JSON_VALUE", "JSON_QUERY"):
                return "STRING"
            if func_name in ("NOW", "CURRENT_ROW_TIMESTAMP"):
                return "TIMESTAMP_LTZ(3)"

        # Current timestamp/time/date functions
        if isinstance(expr, (exp.CurrentTimestamp, exp.CurrentTimestampLTZ)):
            return "TIMESTAMP_LTZ(3)"
        if isinstance(expr, exp.Localtimestamp):
            return "TIMESTAMP(3)"
        if isinstance(expr, (exp.CurrentTime, exp.Localtime)):
            return "TIME(0)"
        if isinstance(expr, exp.CurrentDate):
            return "DATE"

        # Cast - use the target type
        if isinstance(expr, (exp.Cast, exp.TryCast)):
            if expr.to is None:
                return "STRING"
            target_type = expr.to.sql().upper()
            return self._normalize_cast_type(target_type)

        # Paren - unwrap
        if isinstance(expr, exp.Paren):
            return self._infer_expression_type(expr.this, schema)

        # Neg (unary minus) - preserve type
        if isinstance(expr, exp.Neg):
            return self._infer_expression_type(expr.this, schema)

        # Default to STRING for unknown expressions
        return "STRING"

    def _is_numeric_type(self, type_name: str) -> bool:
        base_type = type_name.split("(")[0].upper()
        return base_type in {
            "TINYINT",
            "SMALLINT",
            "INT",
            "INTEGER",
            "BIGINT",
            "FLOAT",
            "DOUBLE",
            "DECIMAL",
            "NUMERIC",
        }

    def _merge_types(self, types: list[str]) -> str:
        merged = [type_name for type_name in types if type_name]
        if not merged:
            return "STRING"
        if all(type_name.split("(")[0].upper() == "BOOLEAN" for type_name in merged):
            return "BOOLEAN"
        if all(self._is_numeric_type(type_name) for type_name in merged):
            result = merged[0]
            for next_type in merged[1:]:
                result = self._promote_numeric_types(result, next_type)
            return result
        base_types = {type_name.split("(")[0].upper() for type_name in merged}
        if len(base_types) == 1:
            return merged[0]
        return "STRING"

    def _infer_ml_predict_type(self, expr: exp.Anonymous) -> str:
        """Infer the return type for ML_PREDICT/ML_EVALUATE functions.

        Attempts to use declared ml_outputs for precise type inference.
        Falls back to opaque ROW type with a warning if not declared.

        Args:
            expr: The ML_PREDICT or ML_EVALUATE Anonymous expression

        Returns:
            ROW type string with field definitions if ml_outputs declared,
            otherwise generic "ROW" with a warning logged
        """
        # Try to extract the ML model name from the function arguments
        # ML_PREDICT syntax: ML_PREDICT(model_name, input_columns...)
        # The first argument is typically the model reference
        args = list(expr.expressions)
        ml_model_name = None

        if args:
            first_arg = args[0]
            # Model name could be a Column reference, Literal, or other expression
            if isinstance(first_arg, exp.Column):
                ml_model_name = first_arg.name
            elif isinstance(first_arg, exp.Literal) and first_arg.is_string:
                ml_model_name = first_arg.this
            elif isinstance(first_arg, exp.Anonymous) and first_arg.name.upper() == "TABLE":
                # TABLE(model_name) syntax
                inner_args = list(first_arg.expressions)
                if inner_args and hasattr(inner_args[0], "name"):
                    ml_model_name = inner_args[0].name

        # Check if we have ml_outputs declared for this model
        model = getattr(self, "_current_model", None)
        if model and model.ml_outputs and ml_model_name:
            ml_output = model.ml_outputs.get(ml_model_name)
            if ml_output and ml_output.columns:
                # Build ROW type from declared columns
                field_defs = []
                for col in ml_output.columns:
                    col_type = col.type or "STRING"
                    field_defs.append(f"{col.name} {col_type}")
                return f"ROW<{', '.join(field_defs)}>"

        # No ml_outputs declared - log warning and return opaque ROW
        func_name = expr.name.upper()
        model_name = model.name if model else "unknown"
        ml_model_ref = ml_model_name or "unknown"

        logger.warning(
            f"ML_PREDICT/ML_EVALUATE used in model '{model_name}' without ml_outputs declaration. "
            f"ML model '{ml_model_ref}' output schema is opaque. "
            "Declare ml_outputs in your model configuration to enable:\n"
            "  - Proper type inference for downstream consumers\n"
            "  - Lineage tracking through ML transformations\n"
            "  - Breaking change detection if the ML model schema changes\n"
            "Without ml_outputs, streamt cannot ensure schema compatibility."
        )
        return "ROW"

    def _normalize_cast_type(self, target_type: str) -> str:
        ltz_match = re.match(r"^TIMESTAMPLTZ(?:\((\d+)\))?$", target_type)
        if ltz_match:
            precision = ltz_match.group(1)
            return f"TIMESTAMP_LTZ({precision})" if precision else "TIMESTAMP_LTZ"
        tz_match = re.match(r"^TIMESTAMPTZ(?:\((\d+)\))?$", target_type)
        if tz_match:
            precision = tz_match.group(1)
            if precision:
                return f"TIMESTAMP({precision}) WITH TIME ZONE"
            return "TIMESTAMP WITH TIME ZONE"
        ntz_match = re.match(r"^TIMESTAMPNTZ(?:\((\d+)\))?$", target_type)
        if ntz_match:
            precision = ntz_match.group(1)
            return f"TIMESTAMP({precision})" if precision else "TIMESTAMP"
        return target_type

    def _normalize_type_whitespace(self, type_name: str) -> str:
        return " ".join(type_name.strip().split())

    def _split_type_params(self, type_body: str) -> list[str]:
        parts: list[str] = []
        current: list[str] = []
        depth = 0

        for char in type_body:
            if char == "<":
                depth += 1
                current.append(char)
                continue
            if char == ">":
                depth -= 1
                current.append(char)
                continue
            if char == "," and depth == 0:
                parts.append("".join(current).strip())
                current = []
                continue
            current.append(char)

        if current:
            parts.append("".join(current).strip())

        return parts

    def _extract_array_element_type(self, type_name: str) -> str | None:
        normalized = self._normalize_type_whitespace(type_name)
        if not normalized.upper().startswith("ARRAY<") or not normalized.endswith(">"):
            return None
        inner = normalized[6:-1].strip()
        return inner or None

    def _extract_map_key_value_types(self, type_name: str) -> tuple[str, str] | None:
        normalized = self._normalize_type_whitespace(type_name)
        if not normalized.upper().startswith("MAP<") or not normalized.endswith(">"):
            return None
        inner = normalized[4:-1].strip()
        parts = self._split_type_params(inner)
        if len(parts) != 2:
            return None
        return parts[0], parts[1]

    def _build_array_type(self, element_type: str) -> str:
        return f"ARRAY<{element_type}>"

    def _build_map_type(self, key_type: str, value_type: str) -> str:
        return f"MAP<{key_type}, {value_type}>"

    def _build_row_type(self, fields: list[tuple[str, str]]) -> str:
        field_defs = ", ".join(f"{name} {field_type}" for name, field_type in fields)
        return f"ROW<{field_defs}>"

    def _infer_array_literal_type(self, expr: exp.Array, schema: dict[str, str]) -> str:
        if not expr.expressions:
            return "ARRAY"
        element_type = self._merge_types(
            [self._infer_expression_type(item, schema) for item in expr.expressions]
        )
        return self._build_array_type(element_type)

    def _infer_array_return_type(self, args: list[exp.Expression], schema: dict[str, str]) -> str:
        if not args:
            return "ARRAY"
        input_type = self._infer_expression_type(args[0], schema)
        element_type = self._extract_array_element_type(input_type)
        return input_type if element_type else "ARRAY"

    def _infer_collect_type(self, args: list[exp.Expression], schema: dict[str, str]) -> str:
        if not args:
            return "ARRAY"
        element_type = self._infer_expression_type(args[0], schema)
        return self._build_array_type(element_type)

    def _infer_element_type(self, args: list[exp.Expression], schema: dict[str, str]) -> str:
        if not args:
            return "STRING"
        input_type = self._infer_expression_type(args[0], schema)
        element_type = self._extract_array_element_type(input_type)
        return element_type or "STRING"

    def _infer_map_keys_type(self, args: list[exp.Expression], schema: dict[str, str]) -> str:
        if not args:
            return "ARRAY"
        map_type = self._infer_expression_type(args[0], schema)
        key_value = self._extract_map_key_value_types(map_type)
        if key_value:
            return self._build_array_type(key_value[0])
        return "ARRAY"

    def _infer_map_values_type(self, args: list[exp.Expression], schema: dict[str, str]) -> str:
        if not args:
            return "ARRAY"
        map_type = self._infer_expression_type(args[0], schema)
        key_value = self._extract_map_key_value_types(map_type)
        if key_value:
            return self._build_array_type(key_value[1])
        return "ARRAY"

    def _infer_map_entries_type(self, args: list[exp.Expression], schema: dict[str, str]) -> str:
        if not args:
            return "ARRAY"
        map_type = self._infer_expression_type(args[0], schema)
        key_value = self._extract_map_key_value_types(map_type)
        if key_value:
            row_type = self._build_row_type([("key", key_value[0]), ("value", key_value[1])])
            return self._build_array_type(row_type)
        return "ARRAY"

    def _infer_map_union_type(self, args: list[exp.Expression], schema: dict[str, str]) -> str:
        if not args:
            return "MAP"
        map_type = self._infer_expression_type(args[0], schema)
        if self._extract_map_key_value_types(map_type):
            return map_type
        return "MAP"

    def _infer_map_from_arrays_type(self, args: list[exp.Expression], schema: dict[str, str]) -> str:
        if len(args) < 2:
            return "MAP"
        keys_type = self._infer_expression_type(args[0], schema)
        values_type = self._infer_expression_type(args[1], schema)
        key_element = self._extract_array_element_type(keys_type)
        value_element = self._extract_array_element_type(values_type)
        if key_element and value_element:
            return self._build_map_type(key_element, value_element)
        return "MAP"

    def _promote_numeric_types(self, left_type: str, right_type: str) -> str:
        """Promote numeric types following Flink SQL rules."""
        type_order = {
            "TINYINT": 1,
            "SMALLINT": 2,
            "INT": 3,
            "INTEGER": 3,
            "BIGINT": 4,
            "FLOAT": 5,
            "DOUBLE": 6,
            "DECIMAL": 7,
            "NUMERIC": 7,
        }

        # Extract base type (remove precision/scale)
        left_base = left_type.split("(")[0].upper()
        right_base = right_type.split("(")[0].upper()

        left_order = type_order.get(left_base, 0)
        right_order = type_order.get(right_base, 0)

        # If neither is numeric, return STRING
        if left_order == 0 and right_order == 0:
            return "STRING"

        # Return the higher precedence type
        if left_order >= right_order:
            return left_type if left_order > 0 else right_type
        return right_type

    def _infer_division_type(self, left_type: str, right_type: str) -> str:
        left_base = left_type.split("(")[0].upper()
        right_base = right_type.split("(")[0].upper()
        if left_base in ("DECIMAL", "NUMERIC"):
            return left_type
        if right_base in ("DECIMAL", "NUMERIC"):
            return right_type
        if left_base in ("FLOAT", "DOUBLE") or right_base in ("FLOAT", "DOUBLE"):
            return "DOUBLE"
        if left_base in ("TINYINT", "SMALLINT", "INT", "INTEGER", "BIGINT") or right_base in (
            "TINYINT",
            "SMALLINT",
            "INT",
            "INTEGER",
            "BIGINT",
        ):
            return "DOUBLE"
        return "STRING"

    def _extract_select_columns_with_types_regex(
        self, sql: str, schema: dict[str, str]
    ) -> list[tuple[str, str]]:
        """Fallback regex-based extraction when sqlglot fails."""
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
        parts = self._split_select_columns(select_clause)

        for part in parts:
            part = part.strip()
            column_name = None
            column_type = "STRING"

            if ' AS ' in part.upper():
                alias_match = re.search(r'\s+AS\s+[`"]?(\w+)[`"]?\s*$', part, re.IGNORECASE)
                if alias_match:
                    column_name = alias_match.group(1)
                    expr = part[:part.upper().rfind(' AS ')].strip()
                    column_type = self._infer_flink_type_regex(expr, schema)
            else:
                col_match = re.match(r'^[`"]?(\w+)[`"]?$', part)
                if col_match:
                    column_name = col_match.group(1)
                    column_type = schema.get(column_name, "STRING")

            if column_name:
                columns.append((column_name, column_type))

        return columns

    def _split_select_columns(self, select_clause: str) -> list[str]:
        """Split SELECT clause into columns, respecting nested parentheses."""
        parts = []
        current = []
        depth = 0

        for char in select_clause:
            if char == '(':
                depth += 1
                current.append(char)
            elif char == ')':
                depth -= 1
                current.append(char)
            elif char == ',' and depth == 0:
                parts.append(''.join(current).strip())
                current = []
            else:
                current.append(char)

        if current:
            parts.append(''.join(current).strip())

        return parts

    def _infer_flink_type_regex(self, expr: str, schema: dict[str, str]) -> str:
        """Infer Flink SQL type from an expression using regex (fallback)."""
        expr_upper = expr.upper().strip()

        # Boolean: CASE WHEN with TRUE/FALSE
        if 'CASE' in expr_upper and ('THEN TRUE' in expr_upper or 'THEN FALSE' in expr_upper):
            return "BOOLEAN"

        # Aggregate functions that return BIGINT
        if re.match(r'^COUNT\s*\(', expr_upper):
            return "BIGINT"

        sum_match = re.match(r'^SUM\s*\((.+)\)$', expr_upper)
        if sum_match:
            arg = sum_match.group(1).strip()
            col_match = re.match(r'^[`"]?(\w+)[`"]?$', arg)
            if col_match:
                col_type = schema.get(col_match.group(1), "DOUBLE")
                base_type = col_type.split("(")[0].upper()
                if base_type in ("DECIMAL", "NUMERIC"):
                    return col_type
                if base_type in ("FLOAT", "DOUBLE"):
                    return "DOUBLE"
                if base_type in ("TINYINT", "SMALLINT", "INT", "INTEGER", "BIGINT"):
                    return "BIGINT"
            return "DOUBLE"

        avg_match = re.match(r'^AVG\s*\((.+)\)$', expr_upper)
        if avg_match:
            arg = avg_match.group(1).strip()
            col_match = re.match(r'^[`"]?(\w+)[`"]?$', arg)
            if col_match:
                col_type = schema.get(col_match.group(1), "DOUBLE")
                base_type = col_type.split("(")[0].upper()
                if base_type in ("DECIMAL", "NUMERIC"):
                    return col_type
            return "DOUBLE"

        min_max_match = re.match(r'^(MIN|MAX)\s*\((.+)\)$', expr_upper)
        if min_max_match:
            arg = min_max_match.group(2).strip()
            col_match = re.match(r'^[`"]?(\w+)[`"]?$', arg)
            if col_match:
                return schema.get(col_match.group(1), "STRING")
            return "DOUBLE"

        # Window functions that return TIMESTAMP
        if re.match(
            r'^(TUMBLE_START|TUMBLE_END|TUMBLE_ROWTIME|HOP_START|HOP_END|HOP_ROWTIME|SESSION_START|SESSION_END|SESSION_ROWTIME|WINDOW_START|WINDOW_END)\s*\(',
            expr_upper,
        ):
            return "TIMESTAMP(3)"
        if re.match(r'^(TUMBLE_PROCTIME|HOP_PROCTIME|SESSION_PROCTIME)\s*\(', expr_upper):
            return "TIMESTAMP_LTZ(3)"

        # String functions
        if re.match(r'^(UPPER|LOWER|CONCAT|SUBSTRING|TRIM|LTRIM|RTRIM|REPLACE|REGEXP_REPLACE)\s*\(', expr_upper):
            return "STRING"

        # PROCTIME()
        if expr_upper.startswith('PROCTIME('):
            return "TIMESTAMP_LTZ(3)"

        if (
            expr_upper.startswith("CURRENT_TIMESTAMP")
            or expr_upper.startswith("NOW(")
            or expr_upper.startswith("CURRENT_ROW_TIMESTAMP(")
        ):
            return "TIMESTAMP_LTZ(3)"

        # Numeric literals
        if re.match(r'^-?\d+$', expr.strip()):
            return "INT"
        if re.match(r'^-?\d+\.\d*$', expr.strip()):
            return "DOUBLE"

        # Simple column reference - look up in schema
        col_match = re.match(r'^[`"]?(\w+)[`"]?$', expr.strip())
        if col_match:
            col_name = col_match.group(1)
            if col_name in schema:
                return schema[col_name]
            upper_name = col_name.upper()
            if upper_name == "$ROWTIME":
                return "TIMESTAMP_LTZ(3)"
            if upper_name == "ROWTIME":
                return "TIMESTAMP(3)"
            if upper_name in ("$PROCTIME", "PROCTIME"):
                return "TIMESTAMP_LTZ(3)"
            if upper_name in ("WINDOW_START", "WINDOW_END", "WINDOW_TIME"):
                return "TIMESTAMP(3)"
            return "STRING"

        # Default to STRING for unknown expressions
        return "STRING"

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
                        ref_model.get_topic_config().name
                        if ref_model.get_topic_config() and ref_model.get_topic_config().name
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
                            ref_model.get_topic_config().name
                            if ref_model.get_topic_config() and ref_model.get_topic_config().name
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
                        ref_model.get_topic_config().name
                        if ref_model.get_topic_config() and ref_model.get_topic_config().name
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
                            ref_model.get_topic_config().name
                            if ref_model.get_topic_config() and ref_model.get_topic_config().name
                            else ref_model.name
                        )

        return topics

    def _get_model_dependencies(self, model: Model) -> list[tuple[str, str]]:
        """Get model dependencies as (name, type) tuples."""
        dependencies = []

        if model.sql:
            # Try using parser if available, otherwise extract directly with regex
            if self.parser:
                sources, refs = self.parser.extract_refs_from_sql(model.sql)
            else:
                # Fallback: extract refs directly from SQL using regex
                sources, refs = self._extract_refs_from_sql(model.sql)

            for source_name in sources:
                dependencies.append((source_name, "source"))
            for ref_name in refs:
                dependencies.append((ref_name, "model"))

        if model.from_:
            for from_ref in model.from_:
                if from_ref.source:
                    dependencies.append((from_ref.source, "source"))
                if from_ref.ref:
                    dependencies.append((from_ref.ref, "model"))

        return dependencies

    def _extract_refs_from_sql(self, sql: str) -> tuple[list[str], list[str]]:
        """Extract source and ref names from SQL using regex.

        This is a fallback when parser is not available.
        """
        sources = []
        refs = []

        # Match {{ source('name') }} patterns
        source_pattern = r"\{\{\s*source\s*\(\s*['\"](\w+)['\"]\s*\)\s*\}\}"
        for match in re.finditer(source_pattern, sql):
            sources.append(match.group(1))

        # Match {{ ref('name') }} patterns
        ref_pattern = r"\{\{\s*ref\s*\(\s*['\"](\w+)['\"]\s*\)\s*\}\}"
        for match in re.finditer(ref_pattern, sql):
            refs.append(match.group(1))

        return sources, refs

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
