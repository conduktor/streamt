"""Compiler for streamt projects."""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Optional

from jinja2 import (
    BaseLoader,
    Environment,
    FileSystemLoader,
    StrictUndefined,
    TemplateNotFound,
    UndefinedError,
)

from streamt.compiler.manifest import (
    ConnectorArtifact,
    FlinkJobArtifact,
    GatewayRuleArtifact,
    Manifest,
    SchemaArtifact,
    TopicArtifact,
)
from streamt.compiler.sql_generator import SQLGeneratorMixin
from streamt.compiler.type_inference import TypeInferenceMixin
from streamt.core.dag import DAGBuilder
from streamt.core.models import (
    DataTest,
    MaterializedType,
    Model,
    Source,
    StreamtProject,
    TopicDefaults,
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


class Compiler(SQLGeneratorMixin, TypeInferenceMixin):
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

        self._topic_defaults = self._get_topic_defaults()
        self._udf_types: dict[str, str] = {
            udf.name.upper(): udf.return_type for udf in project.udfs
        }

        # Artifacts
        self.schemas: list[SchemaArtifact] = []
        self.topics: list[TopicArtifact] = []
        self.flink_jobs: list[FlinkJobArtifact] = []
        self.test_jobs: list[FlinkJobArtifact] = []
        self.connectors: list[ConnectorArtifact] = []
        self.gateway_rules: list[GatewayRuleArtifact] = []

    def _get_topic_defaults(self) -> TopicDefaults:
        """Get topic defaults from project config."""
        if self.project.defaults and self.project.defaults.topic:
            return self.project.defaults.topic
        if (
            self.project.defaults
            and self.project.defaults.models
            and self.project.defaults.models.topic
        ):
            return self.project.defaults.models.topic
        return TopicDefaults()

    def compile(self, dry_run: bool = False) -> Manifest:
        """Compile the project."""
        # Clear previous artifacts
        self.schemas = []
        self.topics = []
        self.flink_jobs = []
        self.test_jobs = []
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

        # Compile continuous tests as Flink jobs (DDL-style, backward compat)
        for test in self.project.tests:
            if test.type.value == "continuous":
                self._compile_continuous_test(test)

        # Compile test assertion jobs (simple INSERT-WHERE style)
        from streamt.compiler.test_compiler import TestJobCompiler

        for test in self.project.tests:
            if test.type.value == "continuous":
                job = TestJobCompiler.compile_job(test)
                if job:
                    self.test_jobs.append(job)

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

        subject = source.schema_.subject or f"{source.topic}-value"

        if source.schema_.definition:
            try:
                schema = json.loads(source.schema_.definition)
            except json.JSONDecodeError:
                schema = {"type": "record", "name": source.name, "fields": []}
        else:
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

    @staticmethod
    def _flink_type_to_avro(flink_type: str) -> str | dict:
        """Map a Flink SQL type to its Avro equivalent."""
        base = flink_type.split("(")[0].upper()
        simple = {
            "STRING": "string",
            "VARCHAR": "string",
            "CHAR": "string",
            "BOOLEAN": "boolean",
            "TINYINT": "int",
            "SMALLINT": "int",
            "INT": "int",
            "INTEGER": "int",
            "BIGINT": "long",
            "FLOAT": "float",
            "DOUBLE": "double",
            "BYTES": "bytes",
        }
        if base in simple:
            return simple[base]
        if base in ("DECIMAL", "NUMERIC"):
            import re

            m = re.match(r"(?:DECIMAL|NUMERIC)\((\d+),\s*(\d+)\)", flink_type, re.IGNORECASE)
            p, s = (int(m.group(1)), int(m.group(2))) if m else (38, 18)
            return {"type": "bytes", "logicalType": "decimal", "precision": p, "scale": s}
        if base in ("TIMESTAMP", "TIMESTAMP_LTZ"):
            return {"type": "long", "logicalType": "timestamp-millis"}
        if base == "DATE":
            return {"type": "int", "logicalType": "date"}
        if base == "TIME":
            return {"type": "int", "logicalType": "time-millis"}
        return "string"

    def _generate_schema_from_columns(self, source: Source) -> dict | None:
        """Generate Avro schema from source columns."""
        if not source.columns:
            return None

        fields = []
        for col in source.columns:
            avro_type = self._flink_type_to_avro(col.type) if col.type else "string"
            fields.append(
                {
                    "name": col.name,
                    "type": ["null", avro_type],
                    "default": None,
                    "doc": col.description or "",
                }
            )

        return {
            "type": "record",
            "name": source.name.replace("-", "_").replace(".", "_"),
            "namespace": "com.streamt",
            "fields": fields,
        }

    def _render_macro_sql(self, model: Model) -> str:
        """Render a Jinja2 macro template to SQL."""
        macro_name = model.macro
        project_path = self.project.project_path

        if project_path is None:
            raise CompileError(
                f"Model '{model.name}': cannot load macro '{macro_name}' without a project path"
            )

        macros_dir = project_path / "macros"
        if not macros_dir.exists():
            raise CompileError(
                f"Model '{model.name}': macro '{macro_name}' referenced but no macros/ directory found"
            )

        loader = FileSystemLoader(str(macros_dir))
        env = Environment(loader=loader, undefined=StrictUndefined)

        # source() and ref() produce Jinja2-style refs for downstream processing
        def source_fn(name: str) -> str:
            return f'{{{{ source("{name}") }}}}'

        def ref_fn(name: str) -> str:
            return f'{{{{ ref("{name}") }}}}'

        try:
            template = env.get_template(f"{macro_name}.sql.j2")
        except TemplateNotFound:
            raise CompileError(
                f"Model '{model.name}': macro file '{macro_name}.sql.j2' not found in {macros_dir}"
            ) from None

        try:
            return template.render(source=source_fn, ref=ref_fn, **model.params)
        except UndefinedError as e:
            raise CompileError(f"Model '{model.name}': macro template error: {e}") from e

    def _compile_model(self, model: Model) -> None:
        """Compile a single model."""
        # Resolve macro template to SQL before classification
        if model.macro:
            rendered_sql = self._render_macro_sql(model)
            model = model.model_copy(update={"sql": rendered_sql, "macro": None, "params": {}})

        materialized = model.get_materialized()

        # Handle VIRTUAL_TOPIC fallback to FLINK when Gateway is not available
        if materialized == MaterializedType.VIRTUAL_TOPIC:
            has_gateway = self.project.runtime.conduktor and self.project.runtime.conduktor.gateway
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
        tc = model.get_topic_config()
        topic_name = tc.name if tc and tc.name else model.name
        partitions = (
            tc.partitions if tc and tc.partitions else None
        ) or self._topic_defaults.partitions
        replication_factor = (
            tc.replication_factor if tc and tc.replication_factor else None
        ) or self._topic_defaults.replication_factor
        config = tc.config if tc else {}

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
        tc = model.get_topic_config()
        virtual_topic_name = tc.name if tc and tc.name else model.name

        source_topic = self._get_source_topic(model)
        if not source_topic:
            raise CompileError(
                f"Cannot determine source topic for virtual topic model '{model.name}'"
            )

        interceptors = []

        if model.sql:
            where_clause = self._extract_where_clause(model.sql)
            if where_clause:
                interceptors.append(
                    {
                        "type": "filter",
                        "config": {"where": where_clause},
                    }
                )

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
        tc = model.get_topic_config()
        topic_name = tc.name if tc and tc.name else model.name
        partitions = (
            tc.partitions if tc and tc.partitions else None
        ) or self._topic_defaults.partitions
        replication_factor = (
            tc.replication_factor if tc and tc.replication_factor else None
        ) or self._topic_defaults.replication_factor
        config = tc.config if tc else {}

        self.topics.append(
            TopicArtifact(
                name=topic_name,
                partitions=partitions,
                replication_factor=replication_factor,
                config=config,
            )
        )

        flink_sql = self._generate_flink_sql(model, topic_name)

        self.flink_jobs.append(
            FlinkJobArtifact(
                name=model.name,
                sql=flink_sql,
                cluster=model.get_flink_cluster(),
                parallelism=model.get_flink_config().parallelism
                if model.get_flink_config()
                else None,
                checkpoint_interval_ms=model.get_flink_config().checkpoint_interval_ms
                if model.get_flink_config()
                else None,
                state_backend=model.get_flink_config().state_backend
                if model.get_flink_config()
                else None,
                state_ttl_ms=model.get_flink_config().state_ttl_ms
                if model.get_flink_config()
                else None,
            )
        )

    def _compile_sink_model(self, model: Model) -> None:
        """Compile a sink model (Kafka Connect)."""
        sink_config = model.get_sink_config()
        if not sink_config:
            raise CompileError(f"Sink model '{model.name}' has no sink configuration")

        source_topics = self._get_source_topics(model)
        if not source_topics:
            raise CompileError(f"Cannot determine source topics for sink model '{model.name}'")

        connector_class = CONNECTOR_CLASSES.get(sink_config.connector, sink_config.connector)

        config = dict(sink_config.config)

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
        """Compile a continuous test as a Flink monitoring job."""
        model = self.project.get_model(test.model)
        if not model:
            source = self.project.get_source(test.model)
            if not source:
                return
            topic_name = source.topic
            columns = [col.name for col in source.columns] if source.columns else []
        else:
            topic_name = (
                model.get_topic_config().name
                if model.get_topic_config() and model.get_topic_config().name
                else model.name
            )
            columns = self._extract_select_columns(model.sql or "")

        flink_sql = self._generate_test_flink_sql(test, topic_name, columns)

        self.flink_jobs.append(
            FlinkJobArtifact(
                name=f"test_{test.name}",
                sql=flink_sql,
                cluster=test.flink_cluster,
            )
        )

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
                "test_jobs": [j.to_dict() for j in self.test_jobs],
                "connectors": [c.to_dict() for c in self.connectors],
                "gateway_rules": [g.to_dict() for g in self.gateway_rules],
            },
        )

    @staticmethod
    def _safe_filename(name: str, resource_type: str) -> str:
        """Reject names containing path separators or dotdot sequences."""
        if "/" in name or "\\" in name or ".." in name:
            raise ValueError(
                f"Unsafe {resource_type} name {name!r}: names may not contain '/', '\\', or '..'"
            )
        return name

    def _write_artifacts(self) -> None:
        """Write all artifacts to output directory."""
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # Write schemas
        if self.schemas:
            schemas_dir = self.output_dir / "schemas"
            schemas_dir.mkdir(exist_ok=True)
            for schema in self.schemas:
                safe = self._safe_filename(schema.subject, "schema subject")
                path = schemas_dir / f"{safe}.json"
                with open(path, "w") as f:
                    json.dump(schema.to_dict(), f, indent=2)

        # Write topics
        topics_dir = self.output_dir / "topics"
        topics_dir.mkdir(exist_ok=True)
        for topic in self.topics:
            safe = self._safe_filename(topic.name, "topic")
            path = topics_dir / f"{safe}.json"
            with open(path, "w") as f:
                json.dump(topic.to_dict(), f, indent=2)

        # Write Flink jobs
        flink_dir = self.output_dir / "flink"
        flink_dir.mkdir(exist_ok=True)
        for job in self.flink_jobs:
            safe = self._safe_filename(job.name, "flink job")
            sql_path = flink_dir / f"{safe}.sql"
            with open(sql_path, "w") as f:
                f.write(job.sql)
            config_path = flink_dir / f"{safe}.json"
            with open(config_path, "w") as f:
                json.dump(job.to_dict(), f, indent=2)

        # Write connectors
        connect_dir = self.output_dir / "connect"
        connect_dir.mkdir(exist_ok=True)
        for connector in self.connectors:
            safe = self._safe_filename(connector.name, "connector")
            path = connect_dir / f"{safe}.json"
            with open(path, "w") as f:
                json.dump(connector.to_dict(), f, indent=2)

        # Write gateway rules
        if self.gateway_rules:
            gateway_dir = self.output_dir / "gateway"
            gateway_dir.mkdir(exist_ok=True)
            for rule in self.gateway_rules:
                safe = self._safe_filename(rule.name, "gateway rule")
                path = gateway_dir / f"{safe}.json"
                with open(path, "w") as f:
                    json.dump(rule.to_dict(), f, indent=2)

        # Write manifest
        manifest = self._create_manifest()
        manifest.save(self.output_dir / "manifest.json")
