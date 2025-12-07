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

### Materialization Types

| Type | Runtime | Creates | Use case |
|------|---------|---------|----------|
| `topic` | Kafka | Real topic | Stateless transform, persistent output |
| `virtual_topic` | Gateway | Virtual topic (no storage) | Stateless transform, read-time only |
| `flink` | Flink | Real topic + Flink job | Stateful, windowing, joins |
| `sink` | Connect | External system | Export to warehouse, search, etc. |

### Model Examples

```yaml
# models/payments.yml

models:
  # --- Stateless transformation ---
  - name: card_payments_clean
    description: "Cleaned and filtered payment events"

    materialized: topic           # or virtual_topic (requires Gateway)

    # Output topic config
    topic:
      name: payments.clean.v1     # optional, defaults to model name
      partitions: 12
      replication_factor: 3
      config:
        retention.ms: 604800000   # 7 days

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

  # --- Stateful transformation (Flink) ---
  - name: customer_risk_profile
    description: "Aggregated risk profile per customer"

    materialized: flink
    flink_cluster: prod-flink     # optional, uses default

    topic:
      name: customers.risk.profile.v1

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

    # Flink-specific config
    flink:
      parallelism: 4
      checkpoint_interval_ms: 60000
      state_backend: rocksdb

  # --- Windowed aggregation (Flink) ---
  - name: customer_balance_5m
    description: "Customer balance over 5-minute tumbling windows"

    materialized: flink

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

  # --- Join (Flink) ---
  - name: card_payments_enriched
    description: "Payments enriched with customer risk profile"

    materialized: flink

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

  # --- Sink to external system ---
  - name: payments_to_snowflake
    description: "Export enriched payments to Snowflake"

    materialized: sink
    connect_cluster: prod-connect  # optional, uses default

    from:
      - ref: card_payments_enriched

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
