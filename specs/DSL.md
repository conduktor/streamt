# DSL Specification

## Project Structure

```
my-streaming-project/
├── stream_project.yml      # Project config + runtime connections
├── sources/
│   └── *.yml               # Source declarations
├── models/
│   └── *.yml               # Model declarations (can include SQL inline)
├── tests/
│   └── *.yml               # Test declarations
└── exposures/
    └── *.yml               # Exposure declarations
```

All declarations can also live in a single YAML file if preferred.

---

## stream_project.yml

```yaml
project:
  name: fraud-detection
  version: "1.0.0"
  description: "Fraud detection streaming pipeline"

# Runtime connections (can use env vars)
runtime:
  kafka:
    bootstrap_servers: ${KAFKA_BOOTSTRAP_SERVERS}
    security_protocol: SASL_SSL  # optional
    sasl_mechanism: PLAIN        # optional
    sasl_username: ${KAFKA_USER}
    sasl_password: ${KAFKA_PASSWORD}

  schema_registry:              # optional - enables schema management
    url: ${SCHEMA_REGISTRY_URL}
    username: ${SR_USER}        # Basic auth username
    password: ${SR_PASSWORD}    # Basic auth password
    # Deployment behavior:
    # 1. Schemas are registered BEFORE topics are created
    # 2. Compatibility is checked against existing versions
    # 3. Incompatible changes fail deployment with error
    # 4. Schema artifacts written to generated/schemas/
    #
    # Supported schema types: AVRO, JSON, PROTOBUF
    # Compatibility modes: NONE, BACKWARD, FORWARD, FULL,
    #                      BACKWARD_TRANSITIVE, FORWARD_TRANSITIVE, FULL_TRANSITIVE

  flink:                        # optional, for stateful models & tests
    default: prod-flink         # default cluster to use
    clusters:
      prod-flink:
        type: rest              # rest | kubernetes | yarn
        rest_url: ${FLINK_REST_URL}
      local-flink:
        type: docker            # will spin up Flink in docker
        version: "1.18"
      confluent-flink:
        type: confluent         # Confluent Cloud Flink
        environment: ${CONFLUENT_ENV}
        api_key: ${CONFLUENT_API_KEY}

  connect:                      # optional, for sink models
    default: prod-connect
    clusters:
      prod-connect:
        rest_url: ${CONNECT_REST_URL}

  conduktor:                    # optional, enables enhanced features
    gateway:
      url: ${GATEWAY_URL}
      # enables: virtual_topic, masking at read
    console:
      url: ${CONSOLE_URL}
      api_key: ${CONDUKTOR_API_KEY}
      # enables: advanced RBAC, monitoring integration

# Global defaults
defaults:
  models:
    cluster: prod-kafka         # default Kafka cluster for topics
  tests:
    flink_cluster: prod-flink   # default Flink for running tests

# Governance rules (validated at compile time)
rules:
  topics:
    min_partitions: 6
    max_partitions: 128
    min_replication_factor: 3
    required_config:
      - retention.ms
    naming_pattern: "^[a-z]+\\.[a-z]+\\.[a-z0-9]+\\.v[0-9]+$"  # domain.team.name.v1
    forbidden_prefixes:
      - "test."
      - "tmp."

  models:
    require_description: true
    require_owner: true
    require_tests: true          # at least one test per model
    max_dependencies: 10         # avoid spaghetti

  sources:
    require_schema: true         # must have schema defined
    require_freshness: true      # must define SLA

  security:
    require_classification: true              # all columns must be classified
    sensitive_columns_require_masking: true   # if classified sensitive, must have mask policy
```

---

## Sources

Sources represent external data entering the pipeline (produced by apps, CDC, etc.)

```yaml
# sources/payments.yml

sources:
  - name: card_payments_raw
    description: "Raw payment events from payments-api"

    # Physical location
    cluster: prod-kafka           # optional, uses default
    topic: payments.raw.v1

    # Schema (optional but recommended)
    # Option 1: Reference existing schema from registry
    schema:
      registry: schema_registry   # reference to runtime config
      subject: payments.raw.v1-value
      version: latest             # or specific version (1, 2, etc.)

    # Option 2: Provide inline schema definition
    # schema:
    #   format: avro | json | protobuf
    #   subject: payments.raw.v1-value  # optional, defaults to {topic}-value
    #   compatibility: BACKWARD         # optional, default varies by registry
    #   definition: |
    #     { "type": "record", "name": "Payment", ... }

    # Option 3: Auto-generate from columns (format required)
    # schema:
    #   format: avro
    #   # Schema auto-generated from columns below

    # Metadata
    owner: team-payments
    tags:
      - payments
      - pii

    # Data classification (for governance)
    columns:
      - name: card_number
        classification: highly_sensitive
      - name: customer_email
        classification: sensitive
      - name: amount_cents
        classification: confidential

    # SLA expectations
    freshness:
      max_lag_seconds: 60
      warn_after_seconds: 30

  - name: customers_cdc
    description: "CDC from CRM database"
    topic: crm.customers.cdc
    schema:
      registry: schema_registry
      subject: crm.customers.v3-value
    owner: team-crm
```

---

## Models

Models are transformations that produce new streams.

### Materialization Types (Auto-Inferred)

Materialization type is automatically inferred from SQL patterns:

| SQL Pattern | Inferred Type | Runtime | Creates |
|-------------|---------------|---------|---------|
| Simple SELECT/WHERE | `topic` | Kafka | Real topic |
| TUMBLE/HOP/SESSION | `flink` | Flink | Real topic + Flink job |
| GROUP BY (non-windowed) | `flink` | Flink | Real topic + Flink job |
| JOIN | `flink` | Flink | Real topic + Flink job |
| `from:` without `sql:` | `sink` | Connect | External system |
| `gateway:` rules | `virtual_topic` | Gateway | Virtual topic (no storage) |

**Note:** You can override auto-inference by explicitly setting `materialized:` in the `advanced:` section.

### Model Examples

```yaml
# models/payments.yml

models:
  # --- Stateless transformation ---
  - name: card_payments_clean
    description: "Cleaned and filtered payment events"
    owner: team-payments
    tags:
      - payments
      - pii

    key: payment_id

    # Dependencies
    from:
      - source: card_payments_raw

    # Transformation (SQL with Jinja)
    sql: |
      SELECT
        payment_id,
        customer_id,
        amount_cents,
        currency,
        status,
        event_time,
        card_network
      FROM {{ source("card_payments_raw") }}
      WHERE status IN ('AUTHORIZED', 'CAPTURED')

    # Materialized auto-inferred as 'topic' from simple SELECT/WHERE

    # Access control
    access: private               # private | protected | public
    group: finance                # only finance group can reference this

    # Security policies (applied at sink or Gateway)
    security:
      policies:
        - mask:
            column: card_number
            method: hash          # hash | redact | tokenize | partial
            for_roles:
              - support
              - analytics
        - allow:
            roles:
              - fraud-engine
              - payments-core
            purpose: fraud_detection

    # Advanced configuration (optional)
    advanced:
      # Output topic config
      topic:
        name: payments.clean.v1     # optional, defaults to model name
        partitions: 12
        replication_factor: 3
        config:
          retention.ms: 604800000   # 7 days

  # --- Stateful transformation (Flink) ---
  - name: customer_risk_profile
    description: "Aggregated risk profile per customer"
    owner: team-risk

    key: customer_id

    from:
      - source: customers_cdc

    sql: |
      SELECT
        customer_id,
        risk_band,
        kyc_status,
        updated_at
      FROM {{ source("customers_cdc") }}
      WHERE kyc_status IN ('APPROVED', 'REVIEW')

    # Materialized auto-inferred as 'flink' (could be inferred from downstream usage or explicit GROUP BY)
    # In this case, it's a CDC stream that may need stateful processing

    # Advanced configuration (optional)
    advanced:
      # Override auto-inference if needed
      # materialized: flink

      flink_cluster: prod-flink     # optional, uses default

      topic:
        name: customers.risk.profile.v1

      # Flink-specific config
      flink:
        parallelism: 4
        checkpoint_interval_ms: 60000
        state_backend: rocksdb

  # --- Windowed aggregation (Flink) ---
  - name: customer_balance_5m
    description: "Customer balance over 5-minute tumbling windows"
    owner: team-analytics

    key: customer_id

    from:
      - ref: card_payments_clean

    sql: |
      SELECT
        customer_id,
        TUMBLE_START(event_time, INTERVAL '5' MINUTE) AS window_start,
        TUMBLE_END(event_time, INTERVAL '5' MINUTE) AS window_end,
        SUM(amount_cents) AS total_amount_cents,
        COUNT(*) AS transaction_count
      FROM {{ ref("card_payments_clean") }}
      GROUP BY
        customer_id,
        TUMBLE(event_time, INTERVAL '5' MINUTE)

    # Materialized auto-inferred as 'flink' from TUMBLE windowing function

    # Advanced configuration (optional)
    advanced:
      flink:
        parallelism: 8
        checkpoint_interval_ms: 30000

  # --- Join (Flink) ---
  - name: card_payments_enriched
    description: "Payments enriched with customer risk profile"
    owner: team-fraud

    key: payment_id

    from:
      - ref: card_payments_clean
      - ref: customer_risk_profile

    sql: |
      SELECT
        p.payment_id,
        p.customer_id,
        p.amount_cents,
        p.currency,
        p.event_time,
        c.risk_band,
        c.kyc_status,
        CASE
          WHEN c.risk_band = 'HIGH' AND p.amount_cents > 100000 THEN 'REVIEW'
          WHEN c.risk_band = 'HIGH' THEN 'FLAG'
          ELSE 'PASS'
        END AS fraud_decision
      FROM {{ ref("card_payments_clean") }} p
      LEFT JOIN {{ ref("customer_risk_profile") }} c
        ON p.customer_id = c.customer_id

    # Materialized auto-inferred as 'flink' from JOIN operation

    # Advanced configuration (optional)
    advanced:
      flink:
        parallelism: 6
      topic:
        partitions: 24

  # --- Sink to external system ---
  - name: payments_to_snowflake
    description: "Export enriched payments to Snowflake"
    owner: team-data-platform

    from:
      - ref: card_payments_enriched

    # No sql: means sink-only model
    # Materialized auto-inferred as 'sink' from absence of sql: with from: present

    # Advanced configuration
    advanced:
      connect_cluster: prod-connect  # optional, uses default

      sink:
        connector: snowflake-sink    # connector type
        config:
          snowflake.url.name: ${SNOWFLAKE_URL}
          snowflake.user.name: ${SNOWFLAKE_USER}
          snowflake.private.key: ${SNOWFLAKE_KEY}
          snowflake.database.name: FRAUD
          snowflake.schema.name: PROD
          snowflake.topic2table.map: "payments.enriched:PAYMENTS_ENRICHED"
          key.converter: org.apache.kafka.connect.storage.StringConverter
          value.converter: io.confluent.connect.avro.AvroConverter
```

---

## Auto-Inference Logic

### How Materialization is Inferred

The system analyzes your SQL to determine the optimal materialization strategy:

```yaml
# Example 1: Simple SELECT/WHERE → topic
- name: filtered_payments
  sql: SELECT * FROM {{ source("payments") }} WHERE amount > 100
  # Inferred: materialized: topic

# Example 2: Window function → flink
- name: hourly_revenue
  sql: |
    SELECT TUMBLE_START(ts, INTERVAL '1' HOUR) as hour, SUM(amount)
    FROM {{ ref("payments") }}
    GROUP BY TUMBLE(ts, INTERVAL '1' HOUR)
  # Inferred: materialized: flink

# Example 3: JOIN → flink
- name: enriched_events
  sql: |
    SELECT e.*, u.name
    FROM {{ ref("events") }} e
    JOIN {{ ref("users") }} u ON e.user_id = u.id
  # Inferred: materialized: flink

# Example 4: GROUP BY (non-windowed) → flink
- name: customer_totals
  sql: |
    SELECT customer_id, COUNT(*) as total
    FROM {{ ref("orders") }}
    GROUP BY customer_id
  # Inferred: materialized: flink

# Example 5: No SQL, only from → sink
- name: export_to_warehouse
  from:
    - ref: enriched_events
  # Inferred: materialized: sink
  advanced:
    sink:
      connector: snowflake-sink
```

### Auto-Inference Rules

| Pattern Detected | Inferred Type | Reason |
|-----------------|---------------|---------|
| `TUMBLE()`, `HOP()`, `SESSION()` | `flink` | Window functions require stateful processing |
| `JOIN` keyword | `flink` | Stream joins require Flink's join operators |
| `GROUP BY` (non-window) | `flink` | Stateful aggregation requires Flink |
| Simple `SELECT`, `WHERE`, `CASE` | `topic` | Stateless transformation, direct topic-to-topic |
| `from:` present, `sql:` absent | `sink` | Sink-only model, no transformation |
| `advanced.gateway.virtual_topic: true` | `virtual_topic` | Explicit Gateway virtual topic |

### Overriding Auto-Inference

You can override the inferred materialization:

```yaml
models:
  - name: simple_transform
    sql: SELECT * FROM {{ source("raw") }} WHERE valid = true
    # Would normally infer 'topic', but we want virtual_topic:

    advanced:
      materialized: virtual_topic  # Explicit override
      gateway:
        virtual_topic: true
```

### Schema-Level Configuration

Top-level fields focus on **what** the model does:

| Field | Purpose | Example |
|-------|---------|---------|
| `name` | Identifier | `card_payments_clean` |
| `description` | Documentation | "Cleaned payment events" |
| `owner` | Ownership | `team-payments` |
| `tags` | Organization | `[pii, payments]` |
| `sql` | Transformation logic | `SELECT * FROM ...` |
| `from` | Dependencies | `[ref: payments_raw]` |
| `key` | Partitioning key | `payment_id` |
| `columns` | Data structure | Column definitions |
| `security` | Data policies | Masking, access control |

`advanced:` section contains **how** it's implemented:

| Field | Purpose | Example |
|-------|---------|---------|
| `materialized` | Override inference | `virtual_topic` |
| `topic.*` | Kafka topic config | partitions, retention |
| `flink.*` | Flink job config | parallelism, checkpointing |
| `sink.*` | Connector config | Connector type, properties |
| `gateway.*` | Gateway config | Virtual topic, interceptors |
| `cluster` | Target cluster | `prod-kafka` |

### Benefits of Auto-Inference

1. **Cleaner YAML**: Focus on business logic, not implementation details
2. **Less error-prone**: No need to remember which SQL patterns need Flink
3. **Easier refactoring**: Change SQL, materialization adapts automatically
4. **Better defaults**: System chooses the most efficient runtime
5. **Flexibility**: Override when needed via `advanced:` section

---

## Tests

Tests validate data quality in streaming context.

### Test Types

| Type | Execution | Use case |
|------|-----------|----------|
| `schema` | Compile-time | Validate schema compatibility |
| `sample` | On-demand | Check N messages against assertions |
| `continuous` | Flink job | Real-time monitoring with alerts |

```yaml
# tests/payments_tests.yml

tests:
  # --- Schema validation ---
  - name: payments_clean_schema
    model: card_payments_clean
    type: schema
    assertions:
      - not_null:
          columns: [payment_id, customer_id, amount_cents]
      - accepted_types:
          payment_id: string
          amount_cents: long
          event_time: timestamp

  # --- Sample-based test (on-demand) ---
  - name: payments_clean_sample
    model: card_payments_clean
    type: sample
    sample_size: 1000
    assertions:
      - not_null:
          columns: [payment_id, customer_id]
      - accepted_values:
          column: status
          values: ["AUTHORIZED", "CAPTURED"]
      - range:
          column: amount_cents
          min: 0
          max: 10000000  # 100k max

  # --- Continuous test (Flink job) ---
  - name: payments_clean_quality
    model: card_payments_clean
    type: continuous
    flink_cluster: prod-flink

    assertions:
      - unique_key:
          key: payment_id
          window: "15 minutes"
          tolerance: 0.001        # allow 0.1% duplicates

      - max_lag:
          column: event_time
          max_seconds: 300

      - throughput:
          min_per_second: 100
          max_per_second: 10000

      - distribution:
          column: amount_cents
          buckets:
            - { min: 0, max: 10000, expected_ratio: 0.7, tolerance: 0.1 }
            - { min: 10000, max: 100000, expected_ratio: 0.25, tolerance: 0.1 }
            - { min: 100000, max: null, expected_ratio: 0.05, tolerance: 0.02 }

      - foreign_key:
          column: customer_id
          ref_model: customer_risk_profile
          ref_key: customer_id
          window: "1 hour"
          match_rate: 0.99        # 99% must match

    on_failure:
      severity: error             # error | warning
      actions:
        - alert:
            type: slack
            channel: "#streaming-alerts"
            message: "Data quality issue on {{ model.name }}: {{ failure.description }}"
        - alert:
            type: webhook
            url: ${ALERT_WEBHOOK_URL}
        - pause_model: true       # stop the upstream model
        - dlq:                    # send bad records to DLQ
            topic: "dlq.{{ model.name }}"
```

---

## Exposures

Exposures document downstream consumers for lineage and impact analysis.

```yaml
# exposures/fraud.yml

exposures:
  # --- Producer application ---
  - name: payments_api
    type: application
    role: producer

    description: "Java service that produces payment events"
    owner: team-payments

    repo: https://github.com/company/payments-api
    language: java

    produces:
      - source: card_payments_raw

    contracts:
      schema: strict              # reject events not matching schema
      compatibility: backward     # schema evolution mode

    sla:
      availability: "99.9%"
      max_produce_latency_ms: 50

  # --- Consumer application ---
  - name: fraud_scoring_service
    type: application
    role: consumer

    description: "Python ML service for real-time fraud scoring"
    owner: team-fraud

    repo: https://github.com/company/fraud-scoring
    language: python

    consumes:
      - ref: card_payments_enriched

    consumer_group: fraud-scoring-v3

    access:
      roles: [fraud-engine, fraud-ml]
      purpose: fraud_detection

    sla:
      max_lag_messages: 10000
      max_end_to_end_latency_ms: 800
      max_error_rate: 0.001

  # --- Dashboard ---
  - name: fraud_analyst_dashboard
    type: dashboard

    description: "Tableau dashboard for fraud analysts"
    owner: team-fraud-analytics
    tool: tableau
    url: https://tableau.internal/fraud/overview

    depends_on:
      - ref: payments_to_snowflake

    freshness:
      max_lag_minutes: 5

    access:
      roles: [fraud-analytics, risk-management]
      purpose: reporting

  # --- ML Training ---
  - name: fraud_model_training
    type: ml_training

    description: "Weekly fraud model retraining pipeline"
    owner: team-ml

    depends_on:
      - ref: payments_to_snowflake

    schedule: "weekly"

    data_requirements:
      min_rows: 1000000
      max_age_days: 90
```

---

## Jinja Functions

Available in SQL blocks:

| Function | Description | Example |
|----------|-------------|---------|
| `{{ source("name") }}` | Reference a source | `FROM {{ source("payments_raw") }}` |
| `{{ ref("name") }}` | Reference a model | `FROM {{ ref("payments_clean") }}` |
| `{{ env("VAR") }}` | Environment variable | `{{ env("KAFKA_TOPIC_PREFIX") }}` |
| `{{ config("key") }}` | Project config value | `{{ config("project.name") }}` |

---

## CLI Commands

```bash
# Validate project syntax
stream validate

# Show the DAG
stream lineage
stream lineage --model card_payments_enriched  # focus on one model

# Compile without deploying
stream compile
stream compile --output ./generated/
stream compile --dry-run  # just show what would be generated

# Show what would change (like terraform plan)
stream plan

# Deploy everything
stream apply
stream apply --target card_payments_clean  # deploy single model
stream apply --select "tag:payments"       # deploy by tag

# Run tests
stream test
stream test --model card_payments_clean
stream test --type sample                   # only sample tests

# Check status of deployed resources
stream status

# Teardown
stream destroy --target payments_to_snowflake
```
