"""Pydantic models for streamt DSL."""

from __future__ import annotations

from enum import Enum
from pathlib import Path
from typing import Any, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator

# ============================================================================
# Enums
# ============================================================================


class MaterializedType(str, Enum):
    """Types of model materialization."""

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

    admin_url: Optional[str] = Field(None, alias="url")  # alias for backward compatibility
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
    classification: Optional[Classification] = None
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


class DeprecationConfig(BaseModel):
    """Deprecation configuration for model versions."""

    sunset_date: Optional[str] = None
    message: Optional[str] = None


class Model(BaseModel):
    """Model declaration."""

    model_config = ConfigDict(populate_by_name=True)

    name: str
    description: Optional[str] = None
    materialized: MaterializedType
    topic: Optional[TopicConfig] = None
    key: Optional[str] = None
    from_: Optional[list[FromRef]] = Field(default=None, alias="from")
    sql: Optional[str] = None
    owner: Optional[str] = None
    tags: list[str] = Field(default_factory=list)
    access: AccessLevel = AccessLevel.PRIVATE
    group: Optional[str] = None
    version: Optional[int] = None
    deprecation: Optional[dict[str, DeprecationConfig]] = None
    security: Optional[SecurityPolicies] = None
    flink_cluster: Optional[str] = None
    flink: Optional[FlinkJobConfig] = None
    connect_cluster: Optional[str] = None
    sink: Optional[SinkConfig] = None

    @field_validator("sql")
    @classmethod
    def sql_required_for_non_sink(cls, v: Optional[str], info: Any) -> Optional[str]:
        """Validate that SQL is provided for non-sink models."""
        # Note: This validation is relaxed - sink models may not need SQL
        return v


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
