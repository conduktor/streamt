"""Pydantic models for streamt DSL."""

from __future__ import annotations

from enum import Enum
from pathlib import Path
from typing import Any, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

# ============================================================================
# Enums
# ============================================================================


class MaterializedType(str, Enum):
    """Types of model materialization.

    - TOPIC: Pure passthrough, no transformation (just topic aliasing)
    - VIRTUAL_TOPIC: Stateless transformations via Conduktor Gateway (filters, projections)
    - FLINK: Stateful transformations requiring Flink SQL (aggregations, joins, windows)
    - SINK: Connector sink (from: without sql:)
    """

    TOPIC = "topic"
    VIRTUAL_TOPIC = "virtual_topic"
    FLINK = "flink"
    SINK = "sink"


class DataTestType(str, Enum):
    """Types of data tests."""

    SCHEMA = "schema"
    SAMPLE = "sample"
    CONTINUOUS = "continuous"


class ExposureType(str, Enum):
    """Types of exposures."""

    APPLICATION = "application"
    DASHBOARD = "dashboard"
    ML_TRAINING = "ml_training"
    ML_INFERENCE = "ml_inference"
    API = "api"


class ExposureRole(str, Enum):
    """Roles for application exposures."""

    PRODUCER = "producer"
    CONSUMER = "consumer"
    BOTH = "both"


class AccessLevel(str, Enum):
    """Access control levels."""

    PRIVATE = "private"
    PROTECTED = "protected"
    PUBLIC = "public"


class Classification(str, Enum):
    """Data classification levels."""

    PUBLIC = "public"
    INTERNAL = "internal"
    CONFIDENTIAL = "confidential"
    SENSITIVE = "sensitive"
    HIGHLY_SENSITIVE = "highly_sensitive"


class MaskMethod(str, Enum):
    """Masking methods."""

    HASH = "hash"
    REDACT = "redact"
    PARTIAL = "partial"
    TOKENIZE = "tokenize"
    NULL = "null"


class Severity(str, Enum):
    """Alert severity levels."""

    ERROR = "error"
    WARNING = "warning"


# ============================================================================
# Runtime Configuration
# ============================================================================


class KafkaConfig(BaseModel):
    """Kafka cluster configuration."""

    bootstrap_servers: str
    # Internal bootstrap servers for Flink/Connect running in Docker
    bootstrap_servers_internal: Optional[str] = None
    security_protocol: Optional[str] = None
    sasl_mechanism: Optional[str] = None
    sasl_username: Optional[str] = None
    sasl_password: Optional[str] = None


class SchemaRegistryConfig(BaseModel):
    """Schema Registry configuration."""

    url: str
    username: Optional[str] = None
    password: Optional[str] = None


class FlinkClusterConfig(BaseModel):
    """Flink cluster configuration."""

    type: str = "rest"  # rest, docker, confluent, kubernetes
    rest_url: Optional[str] = None
    sql_gateway_url: Optional[str] = None  # Flink SQL Gateway URL for SQL submission
    version: Optional[str] = None
    environment: Optional[str] = None
    api_key: Optional[str] = None


class FlinkConfig(BaseModel):
    """Flink runtime configuration."""

    default: Optional[str] = None
    clusters: dict[str, FlinkClusterConfig] = Field(default_factory=dict)


class ConnectClusterConfig(BaseModel):
    """Connect cluster configuration."""

    rest_url: str


class ConnectConfig(BaseModel):
    """Connect runtime configuration."""

    default: Optional[str] = None
    clusters: dict[str, ConnectClusterConfig] = Field(default_factory=dict)


class GatewayConfig(BaseModel):
    """Conduktor Gateway configuration.

    Attributes:
        admin_url: Gateway Admin API URL (e.g., http://localhost:8888)
        proxy_bootstrap: Gateway proxy bootstrap servers for Kafka clients (e.g., localhost:6969)
        username: Admin API username (default: admin)
        password: Admin API password (default: conduktor)
        virtual_cluster: Optional virtual cluster for multi-tenant setups
    """

    admin_url: Optional[str] = None
    proxy_bootstrap: Optional[str] = None
    username: str = "admin"
    password: str = "conduktor"
    virtual_cluster: Optional[str] = None


class ConsoleConfig(BaseModel):
    """Conduktor Console configuration."""

    url: str
    api_key: Optional[str] = None


class ConduktorConfig(BaseModel):
    """Conduktor configuration (Gateway + Console)."""

    gateway: Optional[GatewayConfig] = None
    console: Optional[ConsoleConfig] = None


class RuntimeConfig(BaseModel):
    """Runtime configuration for all external systems."""

    kafka: KafkaConfig
    schema_registry: Optional[SchemaRegistryConfig] = None
    flink: Optional[FlinkConfig] = None
    connect: Optional[ConnectConfig] = None
    conduktor: Optional[ConduktorConfig] = None


# ============================================================================
# Governance Rules
# ============================================================================


class TopicRules(BaseModel):
    """Rules for topic creation."""

    min_partitions: Optional[int] = None
    max_partitions: Optional[int] = None
    min_replication_factor: Optional[int] = None
    required_config: list[str] = Field(default_factory=list)
    naming_pattern: Optional[str] = None
    forbidden_prefixes: list[str] = Field(default_factory=list)


class ModelRules(BaseModel):
    """Rules for model definitions."""

    require_description: bool = False
    require_owner: bool = False
    require_tests: bool = False
    max_dependencies: Optional[int] = None


class SourceRules(BaseModel):
    """Rules for source definitions."""

    require_schema: bool = False
    require_freshness: bool = False


class SecurityRules(BaseModel):
    """Rules for security."""

    require_classification: bool = False
    sensitive_columns_require_masking: bool = False


class Rules(BaseModel):
    """Governance rules."""

    topics: Optional[TopicRules] = None
    models: Optional[ModelRules] = None
    sources: Optional[SourceRules] = None
    security: Optional[SecurityRules] = None


# ============================================================================
# Defaults
# ============================================================================


class TopicDefaults(BaseModel):
    """Default values for topics."""

    partitions: int = 1
    replication_factor: int = 1


class ModelDefaults(BaseModel):
    """Default values for models."""

    cluster: Optional[str] = None
    topic: Optional[TopicDefaults] = None


class TestDefaults(BaseModel):
    """Default values for tests."""

    flink_cluster: Optional[str] = None


class Defaults(BaseModel):
    """Default values."""

    models: Optional[ModelDefaults] = None
    tests: Optional[TestDefaults] = None
    topic: Optional[TopicDefaults] = None


# ============================================================================
# Project
# ============================================================================


class ProjectInfo(BaseModel):
    """Project metadata."""

    name: str
    version: Optional[str] = None
    description: Optional[str] = None


class Project(BaseModel):
    """Project configuration (stream_project.yml)."""

    project: ProjectInfo
    runtime: RuntimeConfig
    defaults: Optional[Defaults] = None
    rules: Optional[Rules] = None


# ============================================================================
# Source
# ============================================================================


class SchemaRef(BaseModel):
    """Schema reference."""

    registry: Optional[str] = None
    subject: Optional[str] = None
    format: Optional[str] = None  # avro, json, protobuf
    definition: Optional[str] = None


class ColumnDefinition(BaseModel):
    """Column definition with classification."""

    name: str
    type: Optional[str] = None  # Flink SQL type (STRING, INT, DOUBLE, TIMESTAMP(3), etc.)
    classification: Optional[Classification] = None
    description: Optional[str] = None
    proctime: bool = False  # If true, this column is a processing time attribute


class MLModelOutput(BaseModel):
    """Output schema declaration for ML models used with ML_PREDICT.

    When using Confluent Flink's ML_PREDICT function, the output schema depends on
    the model's OUTPUT definition from CREATE MODEL. Since streamt cannot introspect
    the model registry, users should declare the expected output schema here to enable:
    - Proper type inference for downstream consumers
    - Lineage tracking through ML_PREDICT
    - Schema validation and breaking change detection

    Example:
        ml_outputs:
          fraud_detector:
            columns:
              - name: is_fraud
                type: BOOLEAN
              - name: confidence
                type: DOUBLE
    """

    columns: list[ColumnDefinition]
    description: Optional[str] = None


class FreshnessConfig(BaseModel):
    """Freshness SLA configuration."""

    max_lag_seconds: Optional[int] = None
    warn_after_seconds: Optional[int] = None


class WatermarkStrategy(str, Enum):
    """Watermark strategies for event time processing."""

    BOUNDED_OUT_OF_ORDERNESS = "bounded_out_of_orderness"
    MONOTONOUSLY_INCREASING = "monotonously_increasing"


class WatermarkConfig(BaseModel):
    """Watermark configuration for event time processing."""

    strategy: WatermarkStrategy = WatermarkStrategy.BOUNDED_OUT_OF_ORDERNESS
    max_out_of_orderness_ms: Optional[int] = 5000  # 5 seconds default


class EventTimeConfig(BaseModel):
    """Event time configuration for streaming processing."""

    column: str  # The column containing event time
    watermark: Optional[WatermarkConfig] = None
    allowed_lateness_ms: Optional[int] = None  # Allow late events within this window


class Source(BaseModel):
    """Source declaration."""

    model_config = ConfigDict(populate_by_name=True)

    name: str
    description: Optional[str] = None
    topic: str
    cluster: Optional[str] = None
    schema_: Optional[SchemaRef] = Field(default=None, alias="schema")
    owner: Optional[str] = None
    tags: list[str] = Field(default_factory=list)
    columns: list[ColumnDefinition] = Field(default_factory=list)
    freshness: Optional[FreshnessConfig] = None
    event_time: Optional[EventTimeConfig] = None


# ============================================================================
# Model
# ============================================================================


class TopicConfig(BaseModel):
    """Topic configuration for model output."""

    name: Optional[str] = None
    partitions: Optional[int] = None
    replication_factor: Optional[int] = None
    config: dict[str, Any] = Field(default_factory=dict)


class FlinkJobConfig(BaseModel):
    """Flink job configuration."""

    parallelism: Optional[int] = None
    checkpoint_interval_ms: Optional[int] = None
    state_backend: Optional[str] = None
    state_ttl_ms: Optional[int] = None  # Time-to-live for state entries


class SinkConfig(BaseModel):
    """Sink connector configuration."""

    connector: str
    config: dict[str, Any] = Field(default_factory=dict)


class MaskPolicy(BaseModel):
    """Masking policy."""

    column: str
    method: MaskMethod
    for_roles: list[str] = Field(default_factory=list)


class AllowPolicy(BaseModel):
    """Allow access policy."""

    roles: list[str]
    purpose: Optional[str] = None


class DenyPolicy(BaseModel):
    """Deny access policy."""

    roles: list[str]


class SecurityPolicies(BaseModel):
    """Security policies for a model."""

    classification: dict[str, Classification] = Field(default_factory=dict)
    policies: list[dict[str, Any]] = Field(default_factory=list)


class FromRef(BaseModel):
    """Reference in from clause."""

    source: Optional[str] = None
    ref: Optional[str] = None


class VirtualTopicConfig(BaseModel):
    """Virtual topic configuration for Gateway."""

    name: Optional[str] = None
    compression: Optional[str] = None


class ModelGatewayConfig(BaseModel):
    """Gateway configuration for a model."""

    virtual_topic: Optional[VirtualTopicConfig] = None


class AdvancedConfig(BaseModel):
    """Advanced configuration options for models (nested structure)."""

    flink: Optional[FlinkJobConfig] = None
    topic: Optional[TopicConfig] = None
    flink_cluster: Optional[str] = None
    connect_cluster: Optional[str] = None


class Model(BaseModel):
    """Model declaration."""

    model_config = ConfigDict(populate_by_name=True)

    # Top-level fields (user-facing)
    name: str
    description: Optional[str] = None
    sql: Optional[str] = None
    from_: Optional[list[FromRef]] = Field(default=None, alias="from")
    key: Optional[str] = None
    columns: Optional[list[ColumnDefinition]] = None
    owner: Optional[str] = None
    tags: list[str] = Field(default_factory=list)
    security: Optional[SecurityPolicies] = None

    # Optional: connector config (for sinks)
    connector: Optional[dict[str, Any]] = None
    sink: Optional[SinkConfig] = None

    # Optional: Gateway config (for virtual topics)
    gateway: Optional[ModelGatewayConfig] = None

    # Optional: ML model output schemas (for ML_PREDICT type inference)
    # Maps model name to its expected output schema
    ml_outputs: Optional[dict[str, MLModelOutput]] = None

    # Advanced section (optional, nested)
    advanced: Optional[AdvancedConfig] = None

    # Materialization is auto-inferred from SQL if not provided
    materialized: Optional[MaterializedType] = None

    # Other top-level fields
    access: AccessLevel = AccessLevel.PRIVATE
    group: Optional[str] = None
    version: Optional[int] = None

    @field_validator("sql")
    @classmethod
    def sql_required_for_non_sink(cls, v: Optional[str], info: Any) -> Optional[str]:
        """Validate that SQL is provided for non-sink models."""
        # Note: This validation is relaxed - sink models may not need SQL
        return v

    @model_validator(mode="after")
    def convert_connector_to_sink(self) -> "Model":
        """Convert connector dict to sink config if needed."""
        # If connector is a dict with 'type' and 'config', convert to SinkConfig
        if self.connector and isinstance(self.connector, dict):
            if "type" in self.connector:
                connector_type = self.connector["type"]
                connector_config = self.connector.get("config", {})
                self.sink = SinkConfig(connector=connector_type, config=connector_config)
                # Clear connector dict after conversion
                self.connector = None
        return self

    def get_materialized(self) -> MaterializedType:
        """Get materialization type, auto-inferring if not explicitly set.

        Detection logic:
        - SINK: has from: without sql:
        - VIRTUAL_TOPIC: explicit gateway.virtual_topic config OR stateless SQL
        - FLINK: stateful SQL (aggregations, joins, windows, ML_PREDICT)
        - TOPIC: pure passthrough (SELECT * FROM source)

        Stateless operations (can use Gateway):
        - Filters (WHERE)
        - Projections (SELECT a, b, c)
        - Simple expressions, CASE WHEN
        - Field renames

        Stateful operations (require Flink):
        - Aggregations (GROUP BY, COUNT, SUM, etc.)
        - Window functions (TUMBLE, HOP, SESSION, CUMULATE)
        - Joins (JOIN, LEFT JOIN, etc.)
        - DISTINCT
        - ORDER BY (needs all data)
        - ML_PREDICT, ML_EVALUATE (Confluent Flink specific)
        """
        import re

        # If explicitly set, use that
        if self.materialized is not None:
            return self.materialized

        # Check if it's a sink (has from: without sql:)
        if self.from_ and not self.sql:
            return MaterializedType.SINK

        # Check if it has explicit Gateway virtual_topic config
        if self.gateway and self.gateway.virtual_topic:
            return MaterializedType.VIRTUAL_TOPIC

        # Analyze SQL for stateful vs stateless operations
        if self.sql:
            sql_upper = self.sql.upper()

            # === STATEFUL OPERATIONS (require Flink) ===

            # Window TVFs
            window_patterns = [
                r'\bTUMBLE\s*\(',
                r'\bHOP\s*\(',
                r'\bSESSION\s*\(',
                r'\bCUMULATE\s*\(',
            ]
            for pattern in window_patterns:
                if re.search(pattern, sql_upper):
                    return MaterializedType.FLINK

            # Aggregations with GROUP BY
            if re.search(r'\bGROUP\s+BY\b', sql_upper):
                return MaterializedType.FLINK

            # Joins
            if re.search(r'\s+JOIN\s+', sql_upper):
                return MaterializedType.FLINK

            # DISTINCT (requires state to track uniqueness)
            if re.search(r'\bSELECT\s+DISTINCT\b', sql_upper):
                return MaterializedType.FLINK

            # ORDER BY (requires seeing all data)
            if re.search(r'\bORDER\s+BY\b', sql_upper):
                return MaterializedType.FLINK

            # Window functions (ROW_NUMBER, LAG, LEAD, RANK, etc.)
            window_funcs = [
                r'\bROW_NUMBER\s*\(',
                r'\bLAG\s*\(',
                r'\bLEAD\s*\(',
                r'\bRANK\s*\(',
                r'\bDENSE_RANK\s*\(',
                r'\bFIRST_VALUE\s*\(',
                r'\bLAST_VALUE\s*\(',
                r'\bNTH_VALUE\s*\(',
            ]
            for pattern in window_funcs:
                if re.search(pattern, sql_upper):
                    return MaterializedType.FLINK

            # Aggregate functions without GROUP BY (still stateful)
            agg_funcs = [
                r'\bCOUNT\s*\(',
                r'\bSUM\s*\(',
                r'\bAVG\s*\(',
                r'\bMIN\s*\(',
                r'\bMAX\s*\(',
                r'\bCOLLECT\s*\(',
                r'\bLISTAGG\s*\(',
            ]
            for pattern in agg_funcs:
                if re.search(pattern, sql_upper):
                    return MaterializedType.FLINK

            # ML functions (Confluent Flink specific)
            if re.search(r'\bML_PREDICT\s*\(', sql_upper):
                return MaterializedType.FLINK
            if re.search(r'\bML_EVALUATE\s*\(', sql_upper):
                return MaterializedType.FLINK

            # === PURE PASSTHROUGH (no transformation) ===
            is_simple_passthrough = bool(
                re.search(r'^\s*SELECT\s+\*\s+FROM\s+', sql_upper) and
                not re.search(r'\bWHERE\b', sql_upper) and
                not re.search(r'\bLIMIT\b', sql_upper)
            )

            if is_simple_passthrough:
                return MaterializedType.TOPIC

            # === STATELESS OPERATIONS (can use Gateway) ===
            # Filters (WHERE), projections, CASE WHEN, simple expressions
            # If we got here, it's a stateless transformation
            return MaterializedType.VIRTUAL_TOPIC

        # Default to topic for models without SQL
        return MaterializedType.TOPIC

    def get_flink_config(self) -> Optional["FlinkJobConfig"]:
        """Get Flink config from advanced section."""
        if self.advanced:
            return self.advanced.flink
        return None

    def get_topic_config(self) -> Optional["TopicConfig"]:
        """Get topic config from advanced section."""
        if self.advanced:
            return self.advanced.topic
        return None

    def get_flink_cluster(self) -> Optional[str]:
        """Get flink_cluster from advanced section."""
        if self.advanced:
            return self.advanced.flink_cluster
        return None

    def get_connect_cluster(self) -> Optional[str]:
        """Get connect_cluster from advanced section."""
        if self.advanced:
            return self.advanced.connect_cluster
        return None

    def get_gateway_config(self) -> Optional["ModelGatewayConfig"]:
        """Get gateway config."""
        return self.gateway

    def get_sink_config(self) -> Optional["SinkConfig"]:
        """Get sink config (connector dict is auto-converted to sink by model_validator)."""
        return self.sink


# ============================================================================
# Test
# ============================================================================


class NotNullAssertion(BaseModel):
    """Not null assertion."""

    columns: list[str]


class UniqueKeyAssertion(BaseModel):
    """Unique key assertion."""

    key: str
    window: Optional[str] = None
    tolerance: Optional[float] = None


class AcceptedValuesAssertion(BaseModel):
    """Accepted values assertion."""

    column: str
    values: list[Any]


class AcceptedTypesAssertion(BaseModel):
    """Accepted types assertion."""

    types: dict[str, str] = Field(default_factory=dict)


class RangeAssertion(BaseModel):
    """Range assertion."""

    column: str
    min: Optional[float] = None
    max: Optional[float] = None


class MaxLagAssertion(BaseModel):
    """Max lag assertion."""

    column: str
    max_seconds: int


class ThroughputAssertion(BaseModel):
    """Throughput assertion."""

    min_per_second: Optional[float] = None
    max_per_second: Optional[float] = None


class DistributionBucket(BaseModel):
    """Distribution bucket."""

    min: Optional[float] = None
    max: Optional[float] = None
    expected_ratio: Optional[float] = None
    max_ratio: Optional[float] = None
    tolerance: Optional[float] = None


class DistributionAssertion(BaseModel):
    """Distribution assertion."""

    column: str
    buckets: list[DistributionBucket]


class ForeignKeyAssertion(BaseModel):
    """Foreign key assertion."""

    column: str
    ref_model: str
    ref_key: str
    window: Optional[str] = None
    match_rate: Optional[float] = None


class CustomSqlAssertion(BaseModel):
    """Custom SQL assertion."""

    sql: str
    expect: Any


class AlertAction(BaseModel):
    """Alert action."""

    type: str  # slack, webhook
    channel: Optional[str] = None
    url: Optional[str] = None
    message: Optional[str] = None


class DlqAction(BaseModel):
    """DLQ action."""

    model: str
    topic: Optional[str] = None


class OnFailure(BaseModel):
    """On failure configuration."""

    severity: Severity = Severity.ERROR
    actions: list[dict[str, Any]] = Field(default_factory=list)


class DataTest(BaseModel):
    """Data test declaration."""

    name: str
    model: str
    type: DataTestType
    assertions: list[dict[str, Any]] = Field(default_factory=list)
    sample_size: Optional[int] = None
    flink_cluster: Optional[str] = None
    on_failure: Optional[OnFailure] = None


# ============================================================================
# Exposure
# ============================================================================


class SLAConfig(BaseModel):
    """SLA configuration."""

    availability: Optional[str] = None
    max_produce_latency_ms: Optional[int] = None
    max_end_to_end_latency_ms: Optional[int] = None
    max_lag_messages: Optional[int] = None
    max_error_rate: Optional[float] = None
    max_lag_minutes: Optional[int] = None


class ContractConfig(BaseModel):
    """Contract configuration for producers."""

    model_config = ConfigDict(populate_by_name=True)

    schema_: Optional[str] = Field(default=None, alias="schema")
    compatibility: Optional[str] = None


class AccessConfig(BaseModel):
    """Access configuration."""

    roles: list[str] = Field(default_factory=list)
    purpose: Optional[str] = None


class ExposureRef(BaseModel):
    """Reference in exposure."""

    source: Optional[str] = None
    ref: Optional[str] = None


class Exposure(BaseModel):
    """Exposure declaration."""

    name: str
    type: ExposureType
    role: Optional[ExposureRole] = None
    description: Optional[str] = None
    owner: Optional[str] = None
    url: Optional[str] = None
    repo: Optional[str] = None
    language: Optional[str] = None
    tool: Optional[str] = None
    produces: list[ExposureRef] = Field(default_factory=list)
    consumes: list[ExposureRef] = Field(default_factory=list)
    depends_on: list[ExposureRef] = Field(default_factory=list)
    consumer_group: Optional[str] = None
    sla: Optional[SLAConfig] = None
    contracts: Optional[ContractConfig] = None
    access: Optional[AccessConfig] = None
    freshness: Optional[FreshnessConfig] = None
    schedule: Optional[str] = None
    data_requirements: Optional[dict[str, Any]] = None


# ============================================================================
# Full Project
# ============================================================================


class StreamtProject(BaseModel):
    """Complete streamt project with all declarations."""

    project: ProjectInfo
    runtime: RuntimeConfig
    defaults: Optional[Defaults] = None
    rules: Optional[Rules] = None
    sources: list[Source] = Field(default_factory=list)
    models: list[Model] = Field(default_factory=list)
    tests: list[DataTest] = Field(default_factory=list)
    exposures: list[Exposure] = Field(default_factory=list)

    # Internal - set after parsing
    project_path: Optional[Path] = Field(default=None, exclude=True)

    def get_source(self, name: str) -> Optional[Source]:
        """Get source by name."""
        for source in self.sources:
            if source.name == name:
                return source
        return None

    def get_model(self, name: str) -> Optional[Model]:
        """Get model by name."""
        for model in self.models:
            if model.name == name:
                return model
        return None

    def get_test(self, name: str) -> Optional[DataTest]:
        """Get test by name."""
        for test in self.tests:
            if test.name == name:
                return test
        return None

    def get_exposure(self, name: str) -> Optional[Exposure]:
        """Get exposure by name."""
        for exposure in self.exposures:
            if exposure.name == name:
                return exposure
        return None
