"""Compiler for streamt projects."""

from __future__ import annotations

import json
import logging
from collections.abc import Mapping
from pathlib import Path
from typing import Optional

from streamt.compiler.compiled_models import (
    CompiledModels,
    CompiledModelView,
    empty_compiled_models,
    freeze_compiled_models,
)
from streamt.compiler.gateway_artifact import (
    GatewayArtifactFormatError,
    parse_compiled_gateway_rule_artifact,
)
from streamt.compiler.manifest import (
    ArtifactOwnership,
    ConnectorArtifact,
    ConnectorRemovalArtifact,
    FlinkJobArtifact,
    GatewayRuleArtifact,
    GatewayRuleRemovalArtifact,
    Manifest,
    SchemaArtifact,
    TopicArtifact,
)
from streamt.compiler.model_resolution import (
    CompileError,
    ResolvedModel,
    ResolvedModels,
    empty_resolved_models,
    resolve_project_models,
)
from streamt.compiler.sql_generator import SQLGeneratorMixin
from streamt.compiler.type_inference import TypeInferenceMixin
from streamt.core.dag import DAG, DAGBuilder
from streamt.core.models import (
    DataTest,
    MaterializedType,
    Model,
    Source,
    StreamtProject,
    TopicDefaults,
)

logger = logging.getLogger(__name__)

# Connector class mapping
CONNECTOR_CLASSES = {
    "snowflake-sink": "com.snowflake.kafka.connector.SnowflakeSinkConnector",
    "jdbc-sink": "io.confluent.connect.jdbc.JdbcSinkConnector",
    "s3-sink": "io.confluent.connect.s3.S3SinkConnector",
    "elasticsearch-sink": "io.confluent.connect.elasticsearch.ElasticsearchSinkConnector",
    "bigquery-sink": "com.wepay.kafka.connect.bigquery.BigQuerySinkConnector",
}


def _mask_policy_values(config: object) -> tuple[str, str, list[str]]:
    """Return one strictly validated compiler-facing mask policy."""
    if not isinstance(config, Mapping):
        raise CompileError("Mask policy config must be a mapping")
    column = config.get("column")
    method = config.get("method")
    roles = config.get("for_roles", [])
    if not isinstance(column, str) or not isinstance(method, str):
        raise CompileError("Mask policy column and method must be strings")
    if not isinstance(roles, list) or not all(isinstance(role, str) for role in roles):
        raise CompileError("Mask policy for_roles must be a list of strings")
    return column, method, roles


class Compiler(SQLGeneratorMixin, TypeInferenceMixin):
    """Compiler for streamt projects."""

    def __init__(self, project: StreamtProject, output_dir: Optional[Path] = None) -> None:
        """Initialize compiler."""
        self.project = project
        self.output_dir = output_dir or (
            project.project_path / "generated" if project.project_path else Path("generated")
        )

        # Resolution intentionally happens at compile().  Before the first
        # successful compile there is no authoritative resolved DAG.
        self.resolved_models: ResolvedModels = empty_resolved_models()
        self.compiled_models: CompiledModels = empty_compiled_models()
        self.dag = DAG()

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
        self.connector_removals: list[ConnectorRemovalArtifact] = []
        self.gateway_rules: list[GatewayRuleArtifact] = []
        self.gateway_rule_removals: list[GatewayRuleRemovalArtifact] = []

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

    def _ownership(
        self,
        owner_type: str,
        owner_name: str,
        *,
        mode: Optional[str] = None,
    ) -> ArtifactOwnership:
        """Build lifecycle metadata from a declaration or an explicit output default."""
        if mode is None:
            declaration = (
                self.project.get_source(owner_name)
                if owner_type == "source"
                else self.project.get_model(owner_name)
            )
            if declaration is not None:
                mode = declaration.ownership.mode.value
            else:
                mode = "external" if owner_type == "source" else "managed"
        return ArtifactOwnership(
            project=self.project.project.name,
            owner_type=owner_type,
            owner_name=owner_name,
            mode=mode,
        )

    def compile(self, dry_run: bool = False) -> Manifest:
        """Compile the project."""
        self._reset_compilation_state()

        resolved_models = resolve_project_models(self.project)
        dag = DAGBuilder(self.project, resolved_models=resolved_models).build()
        model_order = dag.get_models_only()
        self.resolved_models = resolved_models
        self.dag = dag

        try:
            manifest, compiled_models = self._compile_resolved(
                resolved_models,
                model_order=model_order,
                dry_run=dry_run,
            )
            self.compiled_models = compiled_models
            return manifest
        except BaseException:
            # A public snapshot describes only a fully successful compile.  Do
            # not leave stale resolution or partially generated artifacts for
            # a caller that catches the original exception.
            self._reset_compilation_state()
            raise

    def _reset_compilation_state(self) -> None:
        """Clear every public result owned by one compiler invocation."""
        self.schemas = []
        self.topics = []
        self.flink_jobs = []
        self.test_jobs = []
        self.connectors = []
        self.connector_removals = []
        self.gateway_rules = []
        self.gateway_rule_removals = []
        self.resolved_models = empty_resolved_models()
        self.compiled_models = empty_compiled_models()
        self.dag = DAG()

    def _compile_resolved(
        self,
        resolved_models: ResolvedModels,
        *,
        model_order: list[str],
        dry_run: bool,
    ) -> tuple[Manifest, CompiledModels]:
        """Compile artifacts from one validated per-invocation model view."""
        # Compile schemas from sources with schema definitions
        for source in self.project.sources:
            self._compile_source_schema(source)

        # Compile models in topological order
        compiled_model_views = [
            self._compile_model(resolved_models[model_name])
            for model_name in model_order
        ]

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
                    job.ownership = self._ownership(
                        "model" if self.project.get_model(test.model) else "source",
                        test.model,
                        mode="managed",
                    )
                    self.test_jobs.append(job)

        # KAFKA-2: Auto-create dead-letter topics from test on_failure DLQ actions
        self._compile_dlq_topics()

        self._compile_connector_removals()
        self._compile_gateway_rule_removals()

        compiled_models = freeze_compiled_models(
            compiled_model_views,
            expected_model_names=resolved_models,
        )

        # Create manifest
        manifest = self._create_manifest()

        # Write artifacts
        if not dry_run:
            self._write_artifacts()

        return manifest, compiled_models

    def _compile_connector_removals(self) -> None:
        """Compile Connector tombstones without runtime, state, or provider access."""
        seen_owners: set[str] = set()
        seen_provider_targets: set[tuple[str, str]] = set()
        desired_owners: set[str] = set()
        desired_provider_targets: set[tuple[str, str]] = set()
        default_cluster = (
            self.project.runtime.connect.default
            if self.project.runtime.connect is not None
            else None
        )

        for connector in self.connectors:
            ownership = ArtifactOwnership.from_dict(connector.ownership)
            if ownership is not None and ownership.project == self.project.project.name:
                desired_owners.add(ownership.owner_name)
            effective_cluster = connector.cluster or default_cluster
            if effective_cluster is not None:
                desired_provider_targets.add((effective_cluster, connector.name))

        for declaration in self.project.lifecycle.connector_removals:
            provider_target = (declaration.cluster, declaration.name)
            if declaration.logical_owner in seen_owners:
                raise CompileError("Duplicate Connector removal logical_owner")
            if provider_target in seen_provider_targets:
                raise CompileError("Duplicate Connector removal (cluster, name) pair")
            if declaration.logical_owner in desired_owners:
                raise CompileError("Desired Connector and removal claim the same logical resource")
            if provider_target in desired_provider_targets:
                raise CompileError("Desired Connector and removal claim the same provider target")
            seen_owners.add(declaration.logical_owner)
            seen_provider_targets.add(provider_target)
            self.connector_removals.append(
                ConnectorRemovalArtifact(
                    logical_owner=declaration.logical_owner,
                    connector_name=declaration.name,
                    cluster_alias=declaration.cluster,
                )
            )

    def _compile_gateway_rule_removals(self) -> None:
        """Compile strict lifecycle tombstones without creating desired rules."""
        seen_owners: set[str] = set()
        seen_rule_names: set[str] = set()
        seen_alias_names: set[str] = set()
        for declaration in self.project.lifecycle.gateway_rule_removals:
            identities = (
                ("logical_owner", declaration.logical_owner, seen_owners),
                (
                    "prior_artifact.name",
                    declaration.prior_artifact.name,
                    seen_rule_names,
                ),
                (
                    "prior_artifact.virtualTopic",
                    declaration.prior_artifact.virtual_topic,
                    seen_alias_names,
                ),
            )
            for label, value, seen in identities:
                if value in seen:
                    raise CompileError(
                        f"Duplicate Gateway rule removal {label} {value!r}"
                    )
                seen.add(value)

            raw_prior = declaration.prior_artifact.model_dump(
                mode="json",
                by_alias=True,
            )
            raw_prior["ownership"] = self._ownership(
                "model",
                declaration.logical_owner,
                mode="managed",
            ).to_dict()
            try:
                prior_artifact = parse_compiled_gateway_rule_artifact(raw_prior)
            except GatewayArtifactFormatError as error:
                raise CompileError(
                    "Invalid prior Gateway rule artifact for lifecycle removal"
                ) from error
            self.gateway_rule_removals.append(
                GatewayRuleRemovalArtifact(
                    logical_owner=declaration.logical_owner,
                    prior_artifact=prior_artifact,
                )
            )

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
                    ownership=self._ownership("source", source.name),
                )
            )

    @staticmethod
    def _flink_type_to_avro(flink_type: str) -> str | dict[str, object]:
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

    def _generate_schema_from_columns(self, source: Source) -> dict[str, object] | None:
        """Generate Avro schema from source columns."""
        if not source.columns:
            return None

        fields: list[dict[str, object]] = []
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

    def _compile_model(self, resolved: ResolvedModel) -> CompiledModelView:
        """Compile a single model."""
        model = resolved.model
        materialized = resolved.materialized

        if materialized == MaterializedType.TOPIC:
            return self._compile_topic_model(model)
        if materialized == MaterializedType.VIRTUAL_TOPIC:
            return self._compile_virtual_topic_model(model)
        if materialized == MaterializedType.FLINK:
            return self._compile_flink_model(model)
        if materialized == MaterializedType.SINK:
            return self._compile_sink_model(model)
        raise CompileError(f"Unsupported materialization for model '{model.name}'")

    def _compile_topic_model(self, model: Model) -> CompiledModelView:
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
                ownership=self._ownership("model", model.name),
            )
        )

        # If there's SQL transformation, we need a Flink job to populate the topic
        if model.sql:
            self._compile_flink_job_for_topic(model, topic_name)

        return CompiledModelView(
            model_name=model.name,
            materialized=MaterializedType.TOPIC,
            process_kind="flink" if model.sql else None,
            output_kind="kafka",
            output_name=topic_name,
            gateway_physical_input=None,
            connector_inputs=(),
        )

    def _compile_virtual_topic_model(self, model: Model) -> CompiledModelView:
        """Compile a virtual topic model (Gateway rule)."""
        try:
            virtual_topic_name = model.get_virtual_topic_name()
        except ValueError as error:
            raise CompileError(str(error)) from error

        source_topic = self._get_source_topic(model)
        if not source_topic:
            raise CompileError(
                f"Cannot determine source topic for virtual topic model '{model.name}'"
            )

        interceptors: list[dict[str, object]] = []

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
                    column, method, roles = _mask_policy_values(policy["mask"])
                    interceptors.append(
                        {
                            "type": "mask",
                            "config": {
                                "field": column,
                                "method": method,
                                "forRoles": roles,
                            },
                        }
                    )

        self.gateway_rules.append(
            GatewayRuleArtifact(
                name=model.name,
                virtual_topic=virtual_topic_name,
                physical_topic=source_topic,
                interceptors=interceptors,
                ownership=self._ownership("model", model.name),
            )
        )

        return CompiledModelView(
            model_name=model.name,
            materialized=MaterializedType.VIRTUAL_TOPIC,
            process_kind="gateway",
            output_kind="gateway",
            output_name=virtual_topic_name,
            gateway_physical_input=source_topic,
            connector_inputs=(),
        )

    def _compile_flink_model(self, model: Model) -> CompiledModelView:
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
                ownership=self._ownership("model", model.name),
            )
        )

        flink_sql = self._generate_flink_sql(model, topic_name)
        flink_config = model.get_flink_config()

        self.flink_jobs.append(
            FlinkJobArtifact(
                name=model.name,
                sql=flink_sql,
                cluster=model.get_flink_cluster(),
                parallelism=flink_config.parallelism if flink_config else None,
                checkpoint_interval_ms=(
                    flink_config.checkpoint_interval_ms if flink_config else None
                ),
                state_backend=flink_config.state_backend if flink_config else None,
                state_ttl_ms=flink_config.state_ttl_ms if flink_config else None,
                ownership=self._ownership("model", model.name),
            )
        )

        return CompiledModelView(
            model_name=model.name,
            materialized=MaterializedType.FLINK,
            process_kind="flink",
            output_kind="kafka",
            output_name=topic_name,
            gateway_physical_input=None,
            connector_inputs=(),
        )

    def _compile_sink_model(self, model: Model) -> CompiledModelView:
        """Compile a sink model (Kafka Connect)."""
        sink_config = model.get_sink_config()
        if not sink_config:
            raise CompileError(f"Sink model '{model.name}' has no sink configuration")

        source_topics = self._get_source_topics(model)
        if not source_topics:
            raise CompileError(f"Cannot determine source topics for sink model '{model.name}'")

        connector_class = CONNECTOR_CLASSES.get(sink_config.connector, sink_config.connector)

        # Merge global connection config (connection base, sink overrides)
        config: dict[str, object] = {}
        if sink_config.connection and sink_config.connection in self.project.connections:
            config.update(self.project.connections[sink_config.connection].config)
        config.update(sink_config.config)

        if model.security and model.security.policies:
            transforms: list[str] = []
            transform_configs: dict[str, object] = {}

            for i, policy in enumerate(model.security.policies):
                if "mask" in policy:
                    column, _method, _roles = _mask_policy_values(policy["mask"])
                    transform_name = f"mask{i}"
                    transforms.append(transform_name)
                    transform_configs[f"transforms.{transform_name}.type"] = (
                        "org.apache.kafka.connect.transforms.MaskField$Value"
                    )
                    transform_configs[f"transforms.{transform_name}.fields"] = column

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
                ownership=self._ownership("model", model.name),
            )
        )

        return CompiledModelView(
            model_name=model.name,
            materialized=MaterializedType.SINK,
            process_kind="connect",
            output_kind=None,
            output_name=None,
            gateway_physical_input=None,
            connector_inputs=tuple(source_topics),
        )

    def _compile_flink_job_for_topic(self, model: Model, output_topic: str) -> None:
        """Compile a Flink job for a topic model that has SQL."""
        flink_sql = self._generate_flink_sql(model, output_topic)

        self.flink_jobs.append(
            FlinkJobArtifact(
                name=f"{model.name}_processor",
                sql=flink_sql,
                cluster=model.get_flink_cluster(),
                ownership=self._ownership("model", model.name),
            )
        )

    def _compile_continuous_test(self, test: DataTest) -> None:
        """Compile a continuous test as a Flink monitoring job."""
        declaration = self.project.get_model(test.model)
        if not declaration:
            source = self.project.get_source(test.model)
            if not source:
                return
            topic_name = source.topic
            columns = [col.name for col in source.columns] if source.columns else []
        else:
            resolved = self.resolved_models.get(declaration.name)
            model = resolved.model if resolved is not None else declaration
            topic_config = model.get_topic_config()
            topic_name = topic_config.name if topic_config and topic_config.name else model.name
            columns = self._extract_select_columns(model.sql or "")

        flink_sql = self._generate_test_flink_sql(test, topic_name, columns)

        self.flink_jobs.append(
            FlinkJobArtifact(
                name=f"test_{test.name}",
                sql=flink_sql,
                cluster=test.flink_cluster,
                ownership=self._ownership(
                    "model" if declaration else "source",
                    test.model,
                    mode="managed",
                ),
            )
        )

    def _compile_dlq_topics(self) -> None:
        """Auto-create dead-letter topics from test on_failure DLQ actions."""
        existing_names = {t.name for t in self.topics}
        for test in self.project.tests:
            if not test.on_failure:
                continue
            for action in test.on_failure.actions:
                if not isinstance(action, dict) or "dlq" not in action:
                    continue
                dlq_cfg = action["dlq"]
                dlq_topic = dlq_cfg.get("topic") if isinstance(dlq_cfg, dict) else None
                if not dlq_topic:
                    dlq_topic = f"{test.model}__dlq"
                if dlq_topic not in existing_names:
                    self.topics.append(
                        TopicArtifact(
                            name=dlq_topic,
                            partitions=self._topic_defaults.partitions,
                            replication_factor=self._topic_defaults.replication_factor,
                            ownership=self._ownership(
                                "model" if self.project.get_model(test.model) else "source",
                                test.model,
                                mode="managed",
                            ),
                        )
                    )
                    existing_names.add(dlq_topic)

    def _create_manifest(self) -> Manifest:
        """Create the manifest."""
        artifacts: dict[str, list[dict[str, object]]] = {
            "schemas": [s.to_dict() for s in self.schemas],
            "topics": [t.to_dict() for t in self.topics],
            "flink_jobs": [f.to_dict() for f in self.flink_jobs],
            "test_jobs": [j.to_dict() for j in self.test_jobs],
            "connectors": [c.to_dict() for c in self.connectors],
            "gateway_rules": [g.to_dict() for g in self.gateway_rules],
        }
        # Preserve checksums for projects that do not use the additive lifecycle
        # feature. Consumers already treat a missing removal collection as empty.
        if self.gateway_rule_removals:
            artifacts["gateway_rule_removals"] = [
                removal.to_dict() for removal in self.gateway_rule_removals
            ]
        if self.connector_removals:
            artifacts["connector_removals"] = [
                removal.to_dict() for removal in self.connector_removals
            ]
        return Manifest(
            version=self.project.project.version or "0.0.0",
            project_name=self.project.project.name,
            sources=[s.model_dump() for s in self.project.sources],
            models=[m.model_dump() for m in self.project.models],
            tests=[t.model_dump() for t in self.project.tests],
            exposures=[e.model_dump() for e in self.project.exposures],
            dag=self.dag.to_dict(),
            artifacts=artifacts,
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
