import type {
  StreamtProject,
  DAG,
  ProjectStatus,
  DeploymentPlan,
  DeploymentResult,
  ValidationResult,
  TestResult,
  Source,
  Model,
  DataTest,
  Exposure,
} from '@/types'

const API_BASE = '/api'

// Test mode: disable delays for browser automation testing
// Enable by adding ?testMode=true to URL or setting window.__TEST_MODE__ = true
const isTestMode = () => {
  if (typeof window !== 'undefined') {
    if ((window as any).__TEST_MODE__) return true
    if (new URLSearchParams(window.location.search).get('testMode') === 'true') return true
  }
  return false
}

// Simulated delay for demo purposes (skipped in test mode)
const delay = (ms: number) => isTestMode() ? Promise.resolve() : new Promise((resolve) => setTimeout(resolve, ms))

// Error handling wrapper
async function fetchApi<T>(
  endpoint: string,
  options?: RequestInit
): Promise<T> {
  const response = await fetch(`${API_BASE}${endpoint}`, {
    ...options,
    headers: {
      'Content-Type': 'application/json',
      ...options?.headers,
    },
  })

  if (!response.ok) {
    const error = await response.json().catch(() => ({ message: 'Unknown error' }))
    throw new Error(error.message || `HTTP error ${response.status}`)
  }

  return response.json()
}

// API functions
export const api = {
  // Project
  async getProject(): Promise<StreamtProject> {
    return fetchApi<StreamtProject>('/project')
  },

  async parseYaml(yaml: string): Promise<StreamtProject> {
    return fetchApi<StreamtProject>('/project/parse', {
      method: 'POST',
      body: JSON.stringify({ yaml }),
    })
  },

  // DAG
  async getDag(): Promise<DAG> {
    return fetchApi<DAG>('/dag')
  },

  // Validation
  async validate(): Promise<ValidationResult> {
    return fetchApi<ValidationResult>('/validate')
  },

  // Status
  async getStatus(): Promise<ProjectStatus> {
    return fetchApi<ProjectStatus>('/status')
  },

  // Deployment
  async plan(): Promise<DeploymentPlan> {
    return fetchApi<DeploymentPlan>('/plan')
  },

  async apply(): Promise<DeploymentResult> {
    return fetchApi<DeploymentResult>('/apply', { method: 'POST' })
  },

  // Tests
  async runTests(testNames?: string[]): Promise<TestResult[]> {
    const body = testNames ? { tests: testNames } : undefined
    return fetchApi<TestResult[]>('/test', {
      method: 'POST',
      body: body ? JSON.stringify(body) : undefined,
    })
  },

  // Connectivity
  async checkConnectivity(url: string): Promise<boolean> {
    try {
      // Try to fetch with a short timeout
      const controller = new AbortController()
      const id = setTimeout(() => controller.abort(), 2000)

      // We use no-cors because we just want to know if it's reachable (network-wise),
      // we don't care about the content and don't want CORS errors to throw immediately
      // unless the network is down.
      // Note: In a real app, this should be proxied by the backend to avoid mixed-content/CORS issues.
      await fetch(url, {
        method: 'HEAD',
        mode: 'no-cors',
        signal: controller.signal,
      })
      clearTimeout(id)
      return true
    } catch (e) {
      return false
    }
  },

  // === CRUD Operations ===

  // Sources
  async createSource(source: Source): Promise<Source> {
    return fetchApi<Source>('/sources', {
      method: 'POST',
      body: JSON.stringify(source),
    })
  },

  async updateSource(name: string, source: Partial<Source>): Promise<Source> {
    return fetchApi<Source>(`/sources/${encodeURIComponent(name)}`, {
      method: 'PUT',
      body: JSON.stringify(source),
    })
  },

  async deleteSource(name: string): Promise<void> {
    return fetchApi<void>(`/sources/${encodeURIComponent(name)}`, {
      method: 'DELETE',
    })
  },

  // Models
  async createModel(model: Model): Promise<Model> {
    return fetchApi<Model>('/models', {
      method: 'POST',
      body: JSON.stringify(model),
    })
  },

  async updateModel(name: string, model: Partial<Model>): Promise<Model> {
    return fetchApi<Model>(`/models/${encodeURIComponent(name)}`, {
      method: 'PUT',
      body: JSON.stringify(model),
    })
  },

  async deleteModel(name: string): Promise<void> {
    return fetchApi<void>(`/models/${encodeURIComponent(name)}`, {
      method: 'DELETE',
    })
  },

  // Tests
  async createTest(test: DataTest): Promise<DataTest> {
    return fetchApi<DataTest>('/tests', {
      method: 'POST',
      body: JSON.stringify(test),
    })
  },

  async updateTest(name: string, test: Partial<DataTest>): Promise<DataTest> {
    return fetchApi<DataTest>(`/tests/${encodeURIComponent(name)}`, {
      method: 'PUT',
      body: JSON.stringify(test),
    })
  },

  async deleteTest(name: string): Promise<void> {
    return fetchApi<void>(`/tests/${encodeURIComponent(name)}`, {
      method: 'DELETE',
    })
  },

  // Exposures
  async createExposure(exposure: Exposure): Promise<Exposure> {
    return fetchApi<Exposure>('/exposures', {
      method: 'POST',
      body: JSON.stringify(exposure),
    })
  },

  async updateExposure(name: string, exposure: Partial<Exposure>): Promise<Exposure> {
    return fetchApi<Exposure>(`/exposures/${encodeURIComponent(name)}`, {
      method: 'PUT',
      body: JSON.stringify(exposure),
    })
  },

  async deleteExposure(name: string): Promise<void> {
    return fetchApi<void>(`/exposures/${encodeURIComponent(name)}`, {
      method: 'DELETE',
    })
  },
}

// Mock data for development/demo
export const mockData = {
  project: {
    project: {
      name: 'payments-pipeline',
      version: '1.0.0',
      description: 'Real-time payments processing pipeline with fraud detection',
    },
    runtime: {
      kafka: {
        bootstrap_servers: 'localhost:9092',
      },
      schema_registry: {
        url: 'http://localhost:8081',
      },
      flink: {
        default: 'local',
        clusters: {
          local: {
            type: 'rest',
            rest_url: 'http://localhost:8082',
            sql_gateway_url: 'http://localhost:8084',
          },
        },
      },
      connect: {
        default: 'local',
        clusters: {
          local: {
            rest_url: 'http://localhost:8083',
          },
        },
      },
    },
    rules: {
      topics: {
        min_partitions: 3,
        min_replication_factor: 2,
      },
      models: {
        require_description: true,
        require_owner: true,
      },
    },
    sources: [
      {
        name: 'payments_raw',
        description: 'Raw payment events from payment gateway',
        topic: 'payments.raw.v1',
        columns: [
          { name: 'payment_id', type: 'STRING', description: 'Unique payment identifier' },
          { name: 'customer_id', type: 'STRING', description: 'Customer identifier' },
          { name: 'amount', type: 'DECIMAL(10,2)', description: 'Payment amount' },
          { name: 'currency', type: 'STRING', description: 'ISO currency code' },
          { name: 'status', type: 'STRING', description: 'Payment status' },
          { name: 'merchant_id', type: 'STRING', description: 'Merchant identifier' },
          { name: 'event_timestamp', type: 'TIMESTAMP', description: 'Event time' },
        ],
      },
      {
        name: 'customers',
        description: 'Customer master data from CRM',
        topic: 'customers.cdc.v1',
        columns: [
          { name: 'customer_id', type: 'STRING' },
          { name: 'name', type: 'STRING' },
          { name: 'email', type: 'STRING', classification: 'sensitive' },
          { name: 'risk_score', type: 'INT' },
          { name: 'created_at', type: 'TIMESTAMP' },
        ],
      },
      {
        name: 'merchants',
        description: 'Merchant reference data',
        topic: 'merchants.v1',
        columns: [
          { name: 'merchant_id', type: 'STRING' },
          { name: 'name', type: 'STRING' },
          { name: 'category', type: 'STRING' },
          { name: 'country', type: 'STRING' },
        ],
      },
    ],
    models: [
      {
        name: 'payments_validated',
        description: 'Validated and enriched payment events',
        materialized: 'topic',
        owner: 'payments-team@company.com',
        topic: {
          partitions: 6,
          replication_factor: 3,
        },
        sql: `SELECT
  p.payment_id,
  p.customer_id,
  p.amount,
  p.currency,
  p.status,
  p.merchant_id,
  p.event_timestamp
FROM {{ source("payments_raw") }} p
WHERE p.status IS NOT NULL
  AND p.amount > 0`,
      },
      {
        name: 'payments_enriched',
        description: 'Payments enriched with customer and merchant data',
        materialized: 'flink',
        owner: 'data-platform@company.com',
        topic: {
          partitions: 12,
        },
        flink: {
          parallelism: 4,
          checkpoint_interval_ms: 60000,
          state_ttl_ms: 86400000,
        },
        sql: `SELECT
  p.payment_id,
  p.customer_id,
  c.name AS customer_name,
  c.risk_score,
  p.amount,
  p.currency,
  m.name AS merchant_name,
  m.category AS merchant_category,
  p.event_timestamp
FROM {{ ref("payments_validated") }} p
LEFT JOIN {{ source("customers") }} c ON p.customer_id = c.customer_id
LEFT JOIN {{ source("merchants") }} m ON p.merchant_id = m.merchant_id`,
      },
      {
        name: 'fraud_scores',
        description: 'Real-time fraud risk scoring',
        materialized: 'flink',
        owner: 'fraud-team@company.com',
        topic: {
          partitions: 6,
        },
        flink: {
          parallelism: 2,
          checkpoint_interval_ms: 30000,
        },
        sql: `SELECT
  payment_id,
  customer_id,
  amount,
  CASE
    WHEN risk_score > 80 AND amount > 1000 THEN 'HIGH'
    WHEN risk_score > 50 OR amount > 5000 THEN 'MEDIUM'
    ELSE 'LOW'
  END AS fraud_risk,
  event_timestamp
FROM {{ ref("payments_enriched") }}`,
      },
      {
        name: 'customer_daily_totals',
        description: 'Aggregated daily totals per customer',
        materialized: 'flink',
        owner: 'analytics-team@company.com',
        topic: {
          partitions: 12,
        },
        flink: {
          parallelism: 4,
          state_ttl_ms: 604800000, // 7 days
        },
        sql: `SELECT
  customer_id,
  TUMBLE_END(event_timestamp, INTERVAL '1' DAY) AS window_end,
  SUM(amount) AS daily_total,
  COUNT(*) AS transaction_count,
  AVG(amount) AS avg_amount
FROM {{ ref("payments_enriched") }}
GROUP BY customer_id, TUMBLE(event_timestamp, INTERVAL '1' DAY)`,
      },
      {
        name: 'high_risk_payments',
        description: 'Virtual topic filtering high-risk payments only',
        materialized: 'virtual_topic',
        sql: `SELECT * FROM {{ ref("fraud_scores") }} WHERE fraud_risk = 'HIGH'`,
      },
      {
        name: 'to_snowflake',
        description: 'Sink payments to Snowflake data warehouse',
        materialized: 'sink',
        sink: {
          connector: 'snowflake-sink',
          config: {
            'snowflake.url.name': '${SNOWFLAKE_URL}',
            'snowflake.database.name': 'ANALYTICS',
            'snowflake.schema.name': 'PAYMENTS',
          },
        },
        from_: [{ ref: 'payments_enriched' }],
      },
    ],
    tests: [
      {
        name: 'payments_not_null',
        model: 'payments_validated',
        type: 'schema',
        description: 'Ensure required fields are not null',
        assertions: [
          { not_null: { columns: ['payment_id', 'customer_id', 'amount'] } },
        ],
      },
      {
        name: 'valid_status_values',
        model: 'payments_validated',
        type: 'sample',
        sample_size: 1000,
        assertions: [
          {
            accepted_values: {
              column: 'status',
              values: ['PENDING', 'CAPTURED', 'FAILED', 'REFUNDED'],
            },
          },
        ],
      },
      {
        name: 'amount_range',
        model: 'payments_validated',
        type: 'sample',
        sample_size: 500,
        assertions: [
          { range: { column: 'amount', min: 0.01, max: 100000 } },
        ],
      },
      {
        name: 'fraud_monitoring',
        model: 'fraud_scores',
        type: 'continuous',
        description: 'Continuous monitoring for fraud detection anomalies',
        assertions: [
          { not_null: { columns: ['fraud_risk'] } },
        ],
      },
    ],
    exposures: [
      {
        name: 'fraud_alerting_service',
        type: 'application',
        role: 'consumer',
        description: 'Microservice that sends alerts for high-risk payments',
        owner: 'fraud-team@company.com',
        depends_on: [{ ref: 'high_risk_payments' }],
        consumes: [{ ref: 'high_risk_payments' }],
        produces: [],
        consumer_group: 'fraud-alerting-consumer',
        sla: {
          max_latency_ms: 5000,
          availability_percent: 99.9,
        },
      },
      {
        name: 'payments_dashboard',
        type: 'dashboard',
        role: 'consumer',
        description: 'Real-time payments analytics dashboard',
        owner: 'analytics-team@company.com',
        depends_on: [
          { ref: 'customer_daily_totals' },
          { ref: 'payments_enriched' },
        ],
        consumes: [
          { ref: 'customer_daily_totals' },
          { ref: 'payments_enriched' },
        ],
        produces: [],
      },
      {
        name: 'payment_gateway',
        type: 'application',
        role: 'producer',
        description: 'External payment gateway that produces payment events',
        owner: 'external-integrations@company.com',
        produces: [{ source: 'payments_raw' }],
        consumes: [],
        depends_on: [],
      },
    ],
  } as StreamtProject,

  dag: {
    nodes: [
      { name: 'payments_raw', type: 'source', upstream: [], downstream: ['payments_validated'], status: 'running' },
      { name: 'customers', type: 'source', upstream: [], downstream: ['payments_enriched'], status: 'running' },
      { name: 'merchants', type: 'source', upstream: [], downstream: ['payments_enriched'], status: 'running' },
      { name: 'payments_validated', type: 'model', materialized: 'topic', upstream: ['payments_raw'], downstream: ['payments_enriched'], status: 'running' },
      { name: 'payments_enriched', type: 'model', materialized: 'flink', upstream: ['payments_validated', 'customers', 'merchants'], downstream: ['fraud_scores', 'customer_daily_totals', 'to_snowflake', 'payments_dashboard'], status: 'running' },
      { name: 'fraud_scores', type: 'model', materialized: 'flink', upstream: ['payments_enriched'], downstream: ['high_risk_payments'], status: 'running' },
      { name: 'high_risk_payments', type: 'model', materialized: 'virtual_topic', upstream: ['fraud_scores'], downstream: ['fraud_alerting_service'], status: 'running' },
      { name: 'customer_daily_totals', type: 'model', materialized: 'flink', upstream: ['payments_enriched'], downstream: ['payments_dashboard'], status: 'running' },
      { name: 'to_snowflake', type: 'model', materialized: 'sink', upstream: ['payments_enriched'], downstream: [], status: 'running' },
      { name: 'fraud_alerting_service', type: 'exposure', upstream: ['high_risk_payments'], downstream: [], status: 'running' },
      { name: 'payments_dashboard', type: 'exposure', upstream: ['customer_daily_totals', 'payments_enriched'], downstream: [], status: 'running' },
      { name: 'payment_gateway', type: 'exposure', upstream: [], downstream: ['payments_raw'], status: 'running' },
    ],
    edges: [
      { from: 'payment_gateway', to: 'payments_raw' },
      { from: 'payments_raw', to: 'payments_validated' },
      { from: 'payments_validated', to: 'payments_enriched' },
      { from: 'customers', to: 'payments_enriched' },
      { from: 'merchants', to: 'payments_enriched' },
      { from: 'payments_enriched', to: 'fraud_scores' },
      { from: 'payments_enriched', to: 'customer_daily_totals' },
      { from: 'payments_enriched', to: 'to_snowflake' },
      { from: 'payments_enriched', to: 'payments_dashboard' },
      { from: 'fraud_scores', to: 'high_risk_payments' },
      { from: 'high_risk_payments', to: 'fraud_alerting_service' },
      { from: 'customer_daily_totals', to: 'payments_dashboard' },
    ],
  } as DAG,

  status: {
    project_name: 'payments-pipeline',
    timestamp: new Date().toISOString(),
    topics: [
      { name: 'payments_validated', exists: true, partitions: 6, replication_factor: 3, message_count: 1234567 },
      { name: 'payments_enriched', exists: true, partitions: 12, replication_factor: 3, message_count: 987654 },
      { name: 'fraud_scores', exists: true, partitions: 6, replication_factor: 3, message_count: 456789 },
      { name: 'customer_daily_totals', exists: true, partitions: 12, replication_factor: 3, message_count: 12345 },
    ],
    flink_jobs: [
      { name: 'payments_enriched_processor', exists: true, job_id: 'abc123', status: 'RUNNING', duration: 3600000 },
      { name: 'fraud_scores_processor', exists: true, job_id: 'def456', status: 'RUNNING', duration: 3600000 },
      { name: 'customer_daily_totals_processor', exists: true, job_id: 'ghi789', status: 'RUNNING', duration: 3600000 },
    ],
    connectors: [
      { name: 'to_snowflake', exists: true, state: 'RUNNING', tasks: [{ state: 'RUNNING', worker_id: 'worker-1' }] },
    ],
    gateway_rules: [
      { name: 'high_risk_payments', exists: true, virtual_topic: 'high_risk_payments', physical_topic: 'fraud_scores', interceptors: 1 },
    ],
    health: 'healthy',
    health_score: 100,
  } as ProjectStatus,

  validation: {
    is_valid: true,
    messages: [
      { level: 'warning', code: 'SLA_NOT_ENFORCED', message: "Exposure 'fraud_alerting_service' has SLA configuration. SLAs are metadata-only and not actively monitored/enforced.", location: "exposure 'fraud_alerting_service'" },
    ],
  } as ValidationResult,
}

// Use mock data when API is not available
export async function loadProject(): Promise<StreamtProject> {
  try {
    return await api.getProject()
  } catch {
    await delay(500)
    return mockData.project
  }
}

export async function loadDag(): Promise<DAG> {
  try {
    return await api.getDag()
  } catch {
    await delay(300)
    return mockData.dag
  }
}

export async function loadStatus(): Promise<ProjectStatus> {
  try {
    return await api.getStatus()
  } catch {
    await delay(400)
    return mockData.status
  }
}

export async function loadValidation(): Promise<ValidationResult> {
  try {
    return await api.validate()
  } catch {
    await delay(200)
    return mockData.validation
  }
}
