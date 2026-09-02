"""Pydantic models for streamt DSL."""

from __future__ import annotations

from enum import Enum
from pathlib import Path
from typing import Literal, Optional

from pydantic import (
    ConfigDict,
    Field,
    PositiveInt,
    ValidationInfo,
    field_validator,
    model_validator,
)

from streamt.core.base import StreamtBaseModel as BaseModel
from streamt.core.deployment_state import (
    DeploymentStateConfig,
    local_deployment_state_config,
)

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


class OwnershipMode(str, Enum):
    """Lifecycle authority for resources emitted from a declaration."""

    EXTERNAL = "external"
    MANAGED = "managed"
    ADOPTED = "adopted"


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


from streamt.core.runtime import ConduktorConfig as ConduktorConfig  # noqa: E402
from streamt.core.runtime import ConnectClusterConfig as ConnectClusterConfig  # noqa: E402
from streamt.core.runtime import ConnectConfig as ConnectConfig  # noqa: E402
from streamt.core.runtime import ConsoleConfig as ConsoleConfig  # noqa: E402
from streamt.core.runtime import FlinkClusterConfig as FlinkClusterConfig  # noqa: E402
from streamt.core.runtime import FlinkConfig as FlinkConfig  # noqa: E402
from streamt.core.runtime import GatewayConfig as GatewayConfig  # noqa: E402
from streamt.core.runtime import KafkaConfig as KafkaConfig  # noqa: E402
from streamt.core.runtime import RuntimeConfig as RuntimeConfig  # noqa: E402
from streamt.core.runtime import SchemaRegistryConfig as SchemaRegistryConfig  # noqa: E402

# ============================================================================
# Governance Rules
# ============================================================================


class TopicRules(BaseModel):
    """Rules for topic creation."""

    min_partitions: Optional[int] = None
    max_partitions: Optional[int] = None
    min_replication_factor: Optional[int] = None
    max_replication_factor: Optional[int] = None
    max_retention_ms: Optional[int] = None
    required_config: list[str] = Field(default_factory=list)
    naming_pattern: Optional[str] = None
    forbidden_prefixes: list[str] = Field(default_factory=list)
    forbidden_suffixes: list[str] = Field(default_factory=list)


class ModelRules(BaseModel):
    """Rules for model definitions."""

    require_description: bool = False
    require_owner: bool = False
    require_tests: bool = False
    valid_owners: list[str] = Field(default_factory=list)
    max_dependencies: Optional[int] = None


class SourceRules(BaseModel):
    """Rules for source definitions."""

    require_schema: bool = False
    require_freshness: bool = False


class SecurityRules(BaseModel):
    """Rules for security."""

    require_classification: bool = False
    sensitive_columns_require_masking: bool = False


class DataResidencyRules(BaseModel):
    """Data residency governance rules."""

    allowed_regions: list[str] = Field(default_factory=list)


class Rules(BaseModel):
    """Governance rules."""

    topics: Optional[TopicRules] = None
    models: Optional[ModelRules] = None
    sources: Optional[SourceRules] = None
    security: Optional[SecurityRules] = None
    data_residency: Optional[DataResidencyRules] = None


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


ApiVersion = Literal["streamt.dev/v1alpha1"]
CURRENT_API_VERSION: ApiVersion = "streamt.dev/v1alpha1"


class ProjectInfo(BaseModel):
    """Project metadata."""

    name: str
    version: Optional[str] = None
    description: Optional[str] = None


class Project(BaseModel):
    """Project configuration (stream_project.yml)."""

    model_config = ConfigDict(populate_by_name=True)

    api_version: ApiVersion = Field(default=CURRENT_API_VERSION, alias="apiVersion")
    project: ProjectInfo
    runtime: RuntimeConfig
    deployment_state: DeploymentStateConfig = Field(
        default_factory=local_deployment_state_config
    )
    defaults: Optional[Defaults] = None
    rules: Optional[Rules] = None


# ============================================================================
# Source
# ============================================================================


class LifecycleOwnership(BaseModel):
    """Lifecycle mode, distinct from the human/team `owner` field."""

    mode: OwnershipMode


class ColumnDefinition(BaseModel):
    """Column definition with classification."""

    name: str
    type: Optional[str] = None  # Flink SQL type (STRING, INT, DOUBLE, TIMESTAMP(3), etc.)
    classification: Optional[Classification] = None
    description: Optional[str] = None
    required: bool = False  # If true, column is NOT NULL
    proctime: bool = False  # If true, this column is a processing time attribute


class SchemaRef(BaseModel):
    """Schema reference.

    Supports both registry references (registry/subject/format) and inline field
    definitions via `fields:`. When `fields:` is set, Source.columns is populated
    automatically via a model_validator.
    """

    registry: Optional[str] = None
    subject: Optional[str] = None
    version: PositiveInt | Literal["latest"] = "latest"
    format: Optional[Literal["avro", "json", "protobuf"]] = None
    definition: Optional[str] = None
    fields: Optional[list[ColumnDefinition]] = None

    @field_validator("version", mode="before")
    @classmethod
    def validate_version(cls, value: object) -> object:
        if isinstance(value, bool) or (isinstance(value, int) and value < 1):
            raise ValueError("version must be a positive integer or 'latest'")
        return value

    @field_validator("format", mode="before")
    @classmethod
    def normalize_format(cls, value: object) -> object:
        return value.lower() if isinstance(value, str) else value


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
    CUSTOM = "custom"


class WatermarkConfig(BaseModel):
    """Watermark configuration for event time processing."""

    strategy: WatermarkStrategy = WatermarkStrategy.BOUNDED_OUT_OF_ORDERNESS
    max_out_of_orderness_ms: Optional[int] = 5000  # 5 seconds default
    expression: Optional[str] = None  # Raw SQL for strategy=custom


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
    ownership: LifecycleOwnership = Field(
        default_factory=lambda: LifecycleOwnership(mode=OwnershipMode.EXTERNAL),
        json_schema_extra={"default": {"mode": "external"}},
    )
    tags: list[str] = Field(default_factory=list)
    columns: list[ColumnDefinition] = Field(default_factory=list)
    freshness: Optional[FreshnessConfig] = None
    event_time: Optional[EventTimeConfig] = None
    region: Optional[str] = None

    @model_validator(mode="after")
    def populate_columns_from_schema_fields(self) -> Source:
        """Map schema.fields → columns when columns is empty."""
        if not self.columns and self.schema_ and self.schema_.fields:
            self.columns = list(self.schema_.fields)
        return self


# ============================================================================
# Model
# ============================================================================


class TopicConfig(BaseModel):
    """Topic configuration for model output."""

    name: Optional[str] = None
    partitions: Optional[int] = None
    replication_factor: Optional[int] = None
    config: dict[str, object] = Field(default_factory=dict)


class CheckpointConfig(BaseModel):
    """Advanced checkpointing configuration."""

    timeout_ms: Optional[int] = None
    min_pause_ms: Optional[int] = None
    max_concurrent: Optional[int] = None
    mode: Optional[str] = None  # EXACTLY_ONCE | AT_LEAST_ONCE
    externalized: Optional[str] = None  # RETAIN_ON_CANCELLATION | DELETE_ON_CANCELLATION
    unaligned: Optional[bool] = None
    incremental: Optional[bool] = None


class RestartStrategyConfig(BaseModel):
    """Restart strategy configuration."""

    type: str = "fixed-delay"  # fixed-delay | failure-rate | exponential-delay | none
    attempts: Optional[int] = None  # fixed-delay
    delay_ms: Optional[int] = None  # fixed-delay, failure-rate
    max_failures_per_interval: Optional[int] = None  # failure-rate
    failure_rate_interval_ms: Optional[int] = None  # failure-rate
    initial_backoff_ms: Optional[int] = None  # exponential-delay
    max_backoff_ms: Optional[int] = None  # exponential-delay
    backoff_multiplier: Optional[float] = None  # exponential-delay


class RocksDBConfig(BaseModel):
    """RocksDB state backend tuning."""

    block_cache_size_mb: Optional[int] = None
    write_buffer_size_mb: Optional[int] = None
    predefined_options: Optional[str] = (
        None  # DEFAULT, SPINNING_DISK_OPTIMIZED, FLASH_SSD_OPTIMIZED
    )


class ResourceConfig(BaseModel):
    """Flink resource configuration."""

    taskmanager_memory_mb: Optional[int] = None
    taskmanager_slots: Optional[int] = None
    jobmanager_memory_mb: Optional[int] = None


class FlinkJobConfig(BaseModel):
    """Flink job configuration."""

    parallelism: Optional[int] = None
    checkpoint_interval_ms: Optional[int] = None
    checkpoint: Optional[CheckpointConfig] = None
    state_backend: Optional[str] = None
    state_ttl_ms: Optional[int] = None
    restart_strategy: Optional[RestartStrategyConfig] = None
    changelog_mode: Optional[str] = None  # append | upsert
    rocksdb: Optional[RocksDBConfig] = None
    resources: Optional[ResourceConfig] = None


class ConnectionConfig(BaseModel):
    """Global connection/credential configuration."""

    type: str
    config: dict[str, object] = Field(default_factory=dict)


class SinkConfig(BaseModel):
    """Sink connector configuration."""

    connector: str
    connection: Optional[str] = None
    config: dict[str, object] = Field(default_factory=dict)


class LegacyConnectorConfig(BaseModel):
    """Legacy connector spelling retained during the alpha migration window."""

    type: str
    config: dict[str, object] = Field(default_factory=dict)


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
    policies: list[dict[str, object]] = Field(default_factory=list)

    @field_validator("policies", mode="before")
    @classmethod
    def validate_policy_shapes(cls, value: object) -> object:
        """Validate each tagged policy while preserving the compiler's dict API."""
        if not isinstance(value, list):
            return value
        policy_models: dict[str, type[BaseModel]] = {
            "mask": MaskPolicy,
            "allow": AllowPolicy,
            "deny": DenyPolicy,
        }
        for index, policy in enumerate(value):
            if not isinstance(policy, dict) or len(policy) != 1:
                raise ValueError(f"policy at index {index} must contain exactly one policy type")
            policy_type, config = next(iter(policy.items()))
            policy_model = policy_models.get(policy_type)
            if policy_model is None:
                raise ValueError(
                    f"policy at index {index} has unknown type '{policy_type}'; "
                    f"expected one of {sorted(policy_models)}"
                )
            policy_model.model_validate(config)
        return value


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


class ContractColumn(BaseModel):
    """Declared output column in a model contract."""

    name: str
    type: Optional[str] = None
    nullable: Optional[bool] = None
    description: Optional[str] = None


class ModelContract(BaseModel):
    """Schema contract for a model — declares expected output columns.

    When enforced=True, the validator errors if SQL-inferred types are
    incompatible with declared types, and warns if downstream exposures
    consume columns not present in the contract.
    """

    enforced: bool = True
    columns: list[ContractColumn] = Field(default_factory=list)


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
    primary_key: Optional[list[str]] = None
    owner: Optional[str] = None
    ownership: LifecycleOwnership = Field(
        default_factory=lambda: LifecycleOwnership(mode=OwnershipMode.MANAGED),
        json_schema_extra={"default": {"mode": "managed"}},
    )
    tags: list[str] = Field(default_factory=list)
    security: Optional[SecurityPolicies] = None

    # Optional: connector config (for sinks)
    connector: Optional[LegacyConnectorConfig] = None
    sink: Optional[SinkConfig] = None

    # Optional: Gateway config (for virtual topics)
    gateway: Optional[ModelGatewayConfig] = None

    # Optional: ML model output schemas (for ML_PREDICT type inference)
    ml_outputs: Optional[dict[str, MLModelOutput]] = None

    # Output topic configuration
    topic: Optional[TopicConfig] = None

    # Flink job configuration
    flink: Optional[FlinkJobConfig] = None
    flink_cluster: Optional[str] = None

    # Connect cluster override
    connect_cluster: Optional[str] = None

    # Materialization is auto-inferred from SQL if not provided
    materialized: Optional[MaterializedType] = None

    # Other top-level fields
    access: AccessLevel = AccessLevel.PRIVATE
    group: Optional[str] = None
    version: Optional[int] = None
    region: Optional[str] = None

    # Data contract (column-level breaking change enforcement)
    contract: Optional[ModelContract] = None

    # Macro templates (mutually exclusive with sql:)
    macro: Optional[str] = None
    params: dict[str, str] = Field(default_factory=dict)

    @field_validator("from_", mode="before")
    @classmethod
    def normalize_from(cls, v: object) -> object:
        """Accept string shorthand: `from: orders_clean` → `[FromRef(ref="orders_clean")]`."""
        if isinstance(v, str):
            return [{"ref": v}]
        return v

    @field_validator("sql")
    @classmethod
    def sql_required_for_non_sink(cls, v: Optional[str], _info: ValidationInfo) -> Optional[str]:
        """Validate that SQL is provided for non-sink models."""
        # Note: This validation is relaxed - sink models may not need SQL
        return v

    @model_validator(mode="after")
    def validate_macro_and_sql_exclusive(self) -> Model:
        """macro: and sql: are mutually exclusive."""
        if self.macro and self.sql:
            raise ValueError(
                f"Model '{self.name}': cannot set both 'macro' and 'sql'. Use one or the other."
            )
        return self

    @model_validator(mode="after")
    def convert_connector_to_sink(self) -> Model:
        """Convert connector dict to sink config if needed."""
        if self.connector:
            self.sink = SinkConfig(
                connector=self.connector.type,
                config=dict(self.connector.config),
            )
            # Clear the legacy spelling after conversion.
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
                r"\bTUMBLE\s*\(",
                r"\bHOP\s*\(",
                r"\bSESSION\s*\(",
                r"\bCUMULATE\s*\(",
            ]
            for pattern in window_patterns:
                if re.search(pattern, sql_upper):
                    return MaterializedType.FLINK

            # Aggregations with GROUP BY
            if re.search(r"\bGROUP\s+BY\b", sql_upper):
                return MaterializedType.FLINK

            # Joins
            if re.search(r"\s+JOIN\s+", sql_upper):
                return MaterializedType.FLINK

            # DISTINCT (requires state to track uniqueness)
            if re.search(r"\bSELECT\s+DISTINCT\b", sql_upper):
                return MaterializedType.FLINK

            # ORDER BY (requires seeing all data)
            if re.search(r"\bORDER\s+BY\b", sql_upper):
                return MaterializedType.FLINK

            # Window functions (ROW_NUMBER, LAG, LEAD, RANK, etc.)
            window_funcs = [
                r"\bROW_NUMBER\s*\(",
                r"\bLAG\s*\(",
                r"\bLEAD\s*\(",
                r"\bRANK\s*\(",
                r"\bDENSE_RANK\s*\(",
                r"\bFIRST_VALUE\s*\(",
                r"\bLAST_VALUE\s*\(",
                r"\bNTH_VALUE\s*\(",
            ]
            for pattern in window_funcs:
                if re.search(pattern, sql_upper):
                    return MaterializedType.FLINK

            # Aggregate functions without GROUP BY (still stateful)
            agg_funcs = [
                r"\bCOUNT\s*\(",
                r"\bSUM\s*\(",
                r"\bAVG\s*\(",
                r"\bMIN\s*\(",
                r"\bMAX\s*\(",
                r"\bCOLLECT\s*\(",
                r"\bLISTAGG\s*\(",
            ]
            for pattern in agg_funcs:
                if re.search(pattern, sql_upper):
                    return MaterializedType.FLINK

            # ML functions (Confluent Flink specific)
            if re.search(r"\bML_PREDICT\s*\(", sql_upper):
                return MaterializedType.FLINK
            if re.search(r"\bML_EVALUATE\s*\(", sql_upper):
                return MaterializedType.FLINK

            # === PURE PASSTHROUGH (no transformation) ===
            is_simple_passthrough = bool(
                re.search(r"^\s*SELECT\s+\*\s+FROM\s+", sql_upper)
                and not re.search(r"\bWHERE\b", sql_upper)
                and not re.search(r"\bLIMIT\b", sql_upper)
            )

            if is_simple_passthrough:
                return MaterializedType.TOPIC

            # === STATELESS OPERATIONS (can use Gateway) ===
            # Filters (WHERE), projections, CASE WHEN, simple expressions
            # If we got here, it's a stateless transformation
            return MaterializedType.VIRTUAL_TOPIC

        # Default to topic for models without SQL
        return MaterializedType.TOPIC

    def get_flink_config(self) -> Optional[FlinkJobConfig]:
        return self.flink

    def get_topic_config(self) -> Optional[TopicConfig]:
        return self.topic

    def get_flink_cluster(self) -> Optional[str]:
        return self.flink_cluster

    def get_connect_cluster(self) -> Optional[str]:
        return self.connect_cluster

    def get_gateway_config(self) -> Optional[ModelGatewayConfig]:
        return self.gateway

    def get_sink_config(self) -> Optional[SinkConfig]:
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
    values: list[object]


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

    name: str = "custom"
    where: str
    detail_column: Optional[str] = None


class AlertAction(BaseModel):
    """Alert action."""

    type: str  # slack, webhook
    channel: Optional[str] = None
    url: Optional[str] = None
    message: Optional[str] = None


class DlqAction(BaseModel):
    """DLQ action."""

    model: Optional[str] = None
    topic: Optional[str] = None


class OnFailure(BaseModel):
    """On failure configuration."""

    severity: Severity = Severity.ERROR
    actions: list[dict[str, object]] = Field(default_factory=list)

    @field_validator("actions", mode="before")
    @classmethod
    def validate_action_shapes(cls, value: object) -> object:
        """Reject unknown actions and fields instead of silently retaining them."""
        if not isinstance(value, list):
            return value
        action_models = {"dlq": DlqAction}
        for index, action in enumerate(value):
            if not isinstance(action, dict) or len(action) != 1:
                raise ValueError(f"action at index {index} must contain exactly one action type")
            action_type, config = next(iter(action.items()))
            action_model = action_models.get(action_type)
            if action_model is None:
                raise ValueError(
                    f"action at index {index} has unknown type '{action_type}'; "
                    f"expected one of {sorted(action_models)}"
                )
            if action_type == "dlq" and config is True:
                continue
            action_model.model_validate(config)
        return value


class DataTest(BaseModel):
    """Data test declaration."""

    name: str
    model: str
    type: DataTestType
    assertions: list[dict[str, object]] = Field(default_factory=list)
    sample_size: Optional[int] = None
    flink_cluster: Optional[str] = None
    on_failure: Optional[OnFailure] = None

    @field_validator("assertions", mode="before")
    @classmethod
    def validate_assertion_shapes(cls, value: object) -> object:
        """Validate tagged assertion configs while preserving their dict API."""
        if not isinstance(value, list):
            return value
        assertion_models: dict[str, type[BaseModel]] = {
            "not_null": NotNullAssertion,
            "unique_key": UniqueKeyAssertion,
            "accepted_values": AcceptedValuesAssertion,
            "accepted_types": AcceptedTypesAssertion,
            "range": RangeAssertion,
            "max_lag": MaxLagAssertion,
            "throughput": ThroughputAssertion,
            "distribution": DistributionAssertion,
            "foreign_key": ForeignKeyAssertion,
            "custom_sql": CustomSqlAssertion,
        }
        for index, assertion in enumerate(value):
            if not isinstance(assertion, dict) or len(assertion) != 1:
                raise ValueError(
                    f"assertion at index {index} must contain exactly one assertion type"
                )
            assertion_type, config = next(iter(assertion.items()))
            assertion_model = assertion_models.get(assertion_type)
            if assertion_model is None:
                raise ValueError(
                    f"assertion at index {index} has unknown type '{assertion_type}'; "
                    f"expected one of {sorted(assertion_models)}"
                )
            assertion_model.model_validate(config)
        return value


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
    freshness_minutes: Optional[int] = None

    @field_validator("max_error_rate", mode="before")
    @classmethod
    def validate_error_rate(cls, v: float | None) -> float | None:
        if v is not None and not 0 <= v <= 1:
            raise ValueError(f"max_error_rate must be between 0 and 1, got {v}")
        return v


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


class ExposureColumn(BaseModel):
    """Column declared on an exposure (for contract breaking-change detection)."""

    name: str
    type: Optional[str] = None


class Exposure(BaseModel):
    """Exposure declaration."""

    name: str
    type: ExposureType
    role: Optional[ExposureRole] = None
    description: Optional[str] = None
    owner: Optional[str] = None
    owners: Optional[list[dict[str, str]]] = None
    url: Optional[str] = None
    repo: Optional[str] = None
    language: Optional[str] = None
    tool: Optional[str] = None
    produces: list[ExposureRef] = Field(default_factory=list)
    consumes: list[ExposureRef] = Field(default_factory=list)
    depends_on: list[ExposureRef] = Field(default_factory=list)
    columns: list[ExposureColumn] = Field(default_factory=list)
    consumer_group: Optional[str] = None
    sla: Optional[SLAConfig] = None
    contracts: Optional[ContractConfig] = None
    access: Optional[AccessConfig] = None
    freshness: Optional[FreshnessConfig] = None
    schedule: Optional[str] = None
    data_requirements: Optional[dict[str, object]] = None

    @model_validator(mode="after")
    def populate_owner_from_owners_list(self) -> Exposure:
        """Map owners[0].name → owner when owner is not set."""
        if self.owner is None and self.owners:
            first = self.owners[0]
            self.owner = first.get("name") if isinstance(first, dict) else None
        return self


# ============================================================================
# UDF Declarations
# ============================================================================


class UDFDeclaration(BaseModel):
    """User-defined function type declaration."""

    name: str
    return_type: str


# ============================================================================
# Full Project
# ============================================================================


class StreamtProject(BaseModel):
    """Complete streamt project with all declarations."""

    model_config = ConfigDict(populate_by_name=True)

    api_version: ApiVersion = Field(default=CURRENT_API_VERSION, alias="apiVersion")
    project: ProjectInfo
    runtime: RuntimeConfig
    deployment_state: DeploymentStateConfig = Field(
        default_factory=local_deployment_state_config
    )
    defaults: Optional[Defaults] = None
    rules: Optional[Rules] = None
    connections: dict[str, ConnectionConfig] = Field(default_factory=dict)
    sources: list[Source] = Field(default_factory=list)
    models: list[Model] = Field(default_factory=list)
    tests: list[DataTest] = Field(default_factory=list)
    exposures: list[Exposure] = Field(default_factory=list)
    udfs: list[UDFDeclaration] = Field(default_factory=list)

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
