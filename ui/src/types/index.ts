// Core domain types matching streamt's Pydantic models

export type MaterializedType = 'topic' | 'flink' | 'virtual_topic' | 'sink'
export type AccessLevel = 'public' | 'protected' | 'private'
export type TestType = 'schema' | 'sample' | 'continuous'
export type ExposureType = 'application' | 'dashboard' | 'ml_model' | 'notebook' | 'other'
export type ExposureRole = 'producer' | 'consumer' | 'both'
export type Classification = 'public' | 'internal' | 'confidential' | 'sensitive' | 'highly_sensitive'

export interface Project {
  name: string
  version: string
  description?: string
}

export interface RuntimeConfig {
  kafka: {
    bootstrap_servers: string
  }
  schema_registry?: {
    url: string
  }
  flink?: {
    default: string
    clusters: Record<string, FlinkCluster>
  }
  connect?: {
    default: string
    clusters: Record<string, ConnectCluster>
  }
  conduktor?: {
    gateway?: {
      url: string
    }
  }
}

export interface FlinkCluster {
  type?: string
  rest_url: string
  sql_gateway_url?: string
}

export interface ConnectCluster {
  rest_url: string
}

export interface Column {
  name: string
  type?: string
  description?: string
  classification?: Classification
  masking?: string
}

export interface TopicConfig {
  name?: string
  partitions?: number
  replication_factor?: number
  retention_ms?: number
  config?: Record<string, string>
}

export interface FlinkConfig {
  parallelism?: number
  checkpoint_interval_ms?: number
  state_ttl_ms?: number
  allowed_lateness_ms?: number
  watermark_delay_ms?: number
}

export interface SinkConfig {
  connector: string
  config: Record<string, string>
}

export interface Source {
  name: string
  description?: string
  topic: string
  schema_?: {
    type: string
    fields?: Column[]
    registry_subject?: string
  }
  columns: Column[]
  freshness?: {
    warn_after_minutes?: number
    error_after_minutes?: number
  }
}

export interface Model {
  name: string
  description?: string
  materialized: MaterializedType
  sql?: string
  topic?: TopicConfig
  flink?: FlinkConfig
  sink?: SinkConfig
  access?: AccessLevel
  owner?: string
  group?: string
  from_?: Array<{ source?: string; ref?: string }>
  security?: {
    classification?: Record<string, Classification>
    policies?: Array<Record<string, unknown>>
  }
}

export interface Assertion {
  [key: string]: {
    columns?: string[]
    column?: string
    values?: unknown[]
    min?: number
    max?: number
    key?: string
  }
}

export interface DataTest {
  name: string
  model: string
  type: TestType
  description?: string
  assertions: Assertion[]
  sample_size?: number
  timeout_seconds?: number
  on_failure?: {
    actions: Array<Record<string, unknown>>
  }
}

export interface ExposureRef {
  source?: string
  ref?: string
}

export interface Exposure {
  name: string
  type: ExposureType
  role?: ExposureRole
  description?: string
  owner?: string
  produces: ExposureRef[]
  consumes: ExposureRef[]
  depends_on: ExposureRef[]
  consumer_group?: string
  sla?: {
    max_latency_ms?: number
    availability_percent?: number
  }
}

export interface Rules {
  topics?: {
    min_partitions?: number
    max_partitions?: number
    min_replication_factor?: number
    naming_pattern?: string
    forbidden_prefixes?: string[]
  }
  models?: {
    require_description?: boolean
    require_owner?: boolean
    require_tests?: boolean
    max_dependencies?: number
  }
  sources?: {
    require_schema?: boolean
    require_freshness?: boolean
  }
  security?: {
    require_classification?: boolean
    sensitive_columns_require_masking?: boolean
  }
}

export interface StreamtProject {
  project: Project
  runtime: RuntimeConfig
  rules?: Rules
  sources: Source[]
  models: Model[]
  tests: DataTest[]
  exposures: Exposure[]
}

// DAG types
export interface DAGNode {
  name: string
  type: 'source' | 'model' | 'exposure' | 'test'
  materialized?: MaterializedType
  upstream: string[]
  downstream: string[]
  description?: string
  status?: NodeStatus
}

export interface DAGEdge {
  from: string
  to: string
}

export interface DAG {
  nodes: DAGNode[]
  edges: DAGEdge[]
}

// Status types
export type NodeStatus = 'running' | 'pending' | 'failed' | 'stopped' | 'unknown'

export interface TopicStatus {
  name: string
  exists: boolean
  partitions?: number
  replication_factor?: number
  message_count?: number
  lag?: number
}

export interface FlinkJobStatus {
  name: string
  exists: boolean
  job_id?: string
  status?: string
  start_time?: string
  duration?: number
}

export interface ConnectorStatus {
  name: string
  exists: boolean
  state?: string
  worker_id?: string
  tasks?: Array<{ state: string; worker_id: string }>
}

export interface GatewayRuleStatus {
  name: string
  exists: boolean
  virtual_topic?: string
  physical_topic?: string
  interceptors?: number
}

export interface ProjectStatus {
  project_name: string
  timestamp: string
  topics: TopicStatus[]
  flink_jobs: FlinkJobStatus[]
  connectors: ConnectorStatus[]
  gateway_rules: GatewayRuleStatus[]
  health: 'healthy' | 'degraded' | 'unhealthy'
  health_score: number
}

// Deployment types
export interface DeploymentChange {
  type: 'schema' | 'topic' | 'flink_job' | 'connector' | 'gateway_rule'
  name: string
  action: 'create' | 'update' | 'delete' | 'none'
  details?: Record<string, unknown>
}

export interface DeploymentPlan {
  has_changes: boolean
  creates: number
  updates: number
  deletes: number
  changes: DeploymentChange[]
}

export interface DeploymentResult {
  success: boolean
  created: string[]
  updated: string[]
  unchanged: string[]
  errors: string[]
}

// Validation types
export interface ValidationMessage {
  level: 'error' | 'warning'
  code: string
  message: string
  location?: string
}

export interface ValidationResult {
  is_valid: boolean
  messages: ValidationMessage[]
}

// Test result types
export interface TestResult {
  name: string
  status: 'passed' | 'failed' | 'skipped'
  message?: string
  errors?: string[]
  warnings?: string[]
  sample_size?: number
  job_status?: string
  duration_ms?: number
}
