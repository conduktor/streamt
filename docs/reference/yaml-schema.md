---
title: YAML Schema Reference
description: Complete reference for all YAML configuration options in streamt
---

# YAML Schema Reference

Complete reference for all YAML configuration in streamt projects.

## Project Configuration

The main project configuration file (`stream_project.yml`) defines project metadata, runtime connections, defaults, and governance rules.

### Project Metadata

```yaml
project:
  name: my-pipeline              # Required: Project identifier
  version: "1.0.0"               # Optional: Semantic version
  description: "Pipeline desc"   # Optional: Human-readable description
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `name` | string | Yes | Unique project identifier |
| `version` | string | No | Semantic version string |
| `description` | string | No | Project description |

---

## Runtime Configuration

### Kafka

```yaml
runtime:
  kafka:
    bootstrap_servers: kafka:9092           # Required
    bootstrap_servers_internal: kafka:29092 # Optional: For Flink/Connect in Docker
    security_protocol: SASL_SSL             # Optional
    sasl_mechanism: PLAIN                   # Optional
    sasl_username: ${KAFKA_USER}            # Optional
    sasl_password: ${KAFKA_PASSWORD}        # Optional
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `bootstrap_servers` | string | Yes | Kafka broker address(es) |
| `bootstrap_servers_internal` | string | No | Internal address for Flink/Connect in Docker |
| `security_protocol` | string | No | `PLAINTEXT`, `SSL`, `SASL_PLAINTEXT`, `SASL_SSL` |
| `sasl_mechanism` | string | No | `PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512` |
| `sasl_username` | string | No | SASL username |
| `sasl_password` | string | No | SASL password |

### Schema Registry

```yaml
runtime:
  schema_registry:
    url: http://schema-registry:8081  # Required
    username: ${SR_USER}               # Optional
    password: ${SR_PASSWORD}           # Optional
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `url` | string | Yes | Schema Registry URL |
| `username` | string | No | Basic auth username |
| `password` | string | No | Basic auth password |

### Flink

```yaml
runtime:
  flink:
    default: local                    # Optional: Default cluster name
    clusters:
      local:
        type: rest                    # Cluster type
        rest_url: http://localhost:8082
        sql_gateway_url: http://localhost:8084
        version: "1.18"               # Optional
        environment: dev              # Optional
        api_key: ${FLINK_API_KEY}     # Optional
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `default` | string | No | Name of default cluster |
| `clusters` | map | No | Named cluster configurations |

**Cluster Configuration:**

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `type` | string | No | `rest`, `docker`, `kubernetes`, `confluent` |
| `rest_url` | string | No | Flink REST API URL |
| `sql_gateway_url` | string | No | Flink SQL Gateway URL |
| `version` | string | No | Flink version |
| `environment` | string | No | Environment identifier |
| `api_key` | string | No | API key for managed services |

### Connect

```yaml
runtime:
  connect:
    default: local
    clusters:
      local:
        rest_url: http://localhost:8083
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `default` | string | No | Default cluster name |
| `clusters.<name>.rest_url` | string | Yes | Connect REST API URL |

### Conduktor Gateway (Optional)

Required for virtual topic models (auto-inferred from `gateway:` configuration). See [Gateway Guide](../guides/gateway.md).

```yaml
runtime:
  conduktor:
    gateway:
      admin_url: http://localhost:8888       # Required: Gateway Admin API
      proxy_bootstrap: localhost:6969         # Required: Gateway proxy for clients
      username: ${GATEWAY_USER}               # Optional (default: admin)
      password: ${GATEWAY_PASSWORD}           # Optional (default: conduktor)
      virtual_cluster: default                # Optional: For multi-tenant setups
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `admin_url` | string | Yes | Gateway Admin API URL |
| `proxy_bootstrap` | string | Yes | Gateway proxy for Kafka clients |
| `username` | string | No | Admin API username (default: admin) |
| `password` | string | No | Admin API password (default: conduktor) |
| `virtual_cluster` | string | No | Virtual cluster for multi-tenancy |

---

## Defaults

Set project-wide default values:

```yaml
defaults:
  topic:
    partitions: 1
    replication_factor: 1

  models:
    cluster: production
    topic:
      partitions: 6
      replication_factor: 3

  tests:
    flink_cluster: production
```

| Section | Field | Type | Description |
|---------|-------|------|-------------|
| `topic` | `partitions` | int | Default partition count |
| `topic` | `replication_factor` | int | Default replication factor |
| `models` | `cluster` | string | Default Flink cluster |
| `models.topic` | `partitions` | int | Default for model output topics |
| `models.topic` | `replication_factor` | int | Default for model output topics |
| `tests` | `flink_cluster` | string | Default cluster for tests |

---

## Governance Rules

### Topic Rules

```yaml
rules:
  topics:
    min_partitions: 3
    max_partitions: 128
    min_replication_factor: 2
    required_config:
      - retention.ms
    naming_pattern: "^[a-z]+\\.[a-z]+\\.v[0-9]+$"
    forbidden_prefixes:
      - "_"
      - "test"
```

| Field | Type | Description |
|-------|------|-------------|
| `min_partitions` | int | Minimum allowed partitions |
| `max_partitions` | int | Maximum allowed partitions |
| `min_replication_factor` | int | Minimum replication factor |
| `required_config` | list | Required topic config keys |
| `naming_pattern` | string | Regex pattern for topic names |
| `forbidden_prefixes` | list | Disallowed topic name prefixes |

### Model Rules

```yaml
rules:
  models:
    require_description: true
    require_owner: true
    require_tests: true
    max_dependencies: 10
```

| Field | Type | Description |
|-------|------|-------------|
| `require_description` | bool | Models must have description |
| `require_owner` | bool | Models must have owner |
| `require_tests` | bool | Models must have associated tests |
| `max_dependencies` | int | Max upstream dependencies |

### Source Rules

```yaml
rules:
  sources:
    require_schema: true
    require_freshness: true
```

| Field | Type | Description |
|-------|------|-------------|
| `require_schema` | bool | Sources must define schema |
| `require_freshness` | bool | Sources must define freshness SLA |

### Security Rules

```yaml
rules:
  security:
    require_classification: true
    sensitive_columns_require_masking: true
```

| Field | Type | Description |
|-------|------|-------------|
| `require_classification` | bool | Columns must have classification |
| `sensitive_columns_require_masking` | bool | Sensitive columns require masking policy |

---

## Sources

Sources declare external Kafka topics that your project consumes.

```yaml
sources:
  - name: orders_raw                    # Required: Unique identifier
    topic: orders.raw.v1                # Required: Kafka topic name
    description: "Raw order events"     # Optional
    cluster: production                 # Optional: Kafka cluster
    owner: orders-team                  # Optional
    tags:                               # Optional
      - orders
      - raw

    schema:                             # Optional: Schema definition
      registry: confluent               # Schema registry type
      subject: orders-raw-value         # Subject name
      format: avro                      # avro, json, protobuf
      definition: |                     # Inline schema (alternative to registry)
        {
          "type": "record",
          "name": "Order",
          "fields": [...]
        }

    columns:                            # Optional: Column metadata
      - name: order_id
        description: "Unique order ID"
        classification: internal

      - name: customer_email
        description: "Customer email"
        classification: sensitive

    freshness:                          # Optional: Freshness SLA
      max_lag_seconds: 300
      warn_after_seconds: 120
```

### Source Fields

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `name` | string | Yes | Unique source identifier |
| `topic` | string | Yes | Kafka topic name |
| `description` | string | No | Human-readable description |
| `cluster` | string | No | Kafka cluster name |
| `owner` | string | No | Team or person responsible |
| `tags` | list | No | Categorization tags |
| `schema` | object | No | Schema definition |
| `columns` | list | No | Column metadata |
| `freshness` | object | No | Freshness SLA configuration |

### Schema Definition

| Field | Type | Description |
|-------|------|-------------|
| `registry` | string | `confluent` or custom registry type |
| `subject` | string | Schema Registry subject name |
| `format` | string | `avro`, `json`, `protobuf` |
| `definition` | string | Inline schema definition |

### Column Definition

| Field | Type | Description |
|-------|------|-------------|
| `name` | string | Column name |
| `description` | string | Column description |
| `classification` | string | `public`, `internal`, `confidential`, `sensitive`, `highly_sensitive` |

### Freshness Configuration

| Field | Type | Description |
|-------|------|-------------|
| `max_lag_seconds` | int | Maximum acceptable lag (error) |
| `warn_after_seconds` | int | Lag threshold for warning |

### Event Time Configuration

Configure event time semantics for streaming processing. This enables proper watermark generation for windowed aggregations.

**Simple case** (just specify the column):

```yaml
sources:
  - name: events
    topic: events.raw.v1
    event_time:
      column: event_timestamp           # Simple: just the column name
```

**Advanced case** (custom watermark settings):

```yaml
sources:
  - name: events
    topic: events.raw.v1
    columns:
      - name: event_id
      - name: user_id
      - name: event_timestamp  # Will be TIMESTAMP(3) in Flink

    # Top-level: just the column name
    event_time:
      column: event_timestamp

    # Advanced section: watermark details
    advanced:
      event_time:
        watermark:
          strategy: bounded_out_of_orderness
          max_out_of_orderness_ms: 5000   # Allow 5 seconds of late data
        allowed_lateness_ms: 60000        # Accept data up to 1 minute late
```

This generates proper Flink SQL watermark declarations:
```sql
CREATE TABLE events (
  ...
  `event_timestamp` TIMESTAMP(3),
  WATERMARK FOR `event_timestamp` AS `event_timestamp` - INTERVAL '5' SECOND
) WITH (...)
```

**Event Time Fields:**

| Field | Location | Type | Description | Required |
|-------|----------|------|-------------|----------|
| `column` | Top-level | string | Column name containing event time | Yes |
| `watermark.strategy` | Advanced | string | `bounded_out_of_orderness` or `monotonously_increasing` | No (default: bounded) |
| `watermark.max_out_of_orderness_ms` | Advanced | int | Max delay in ms | No (default: 5000) |
| `allowed_lateness_ms` | Advanced | int | Accept late events within window | No |

**Watermark Strategies:**

| Strategy | Use Case |
|----------|----------|
| `bounded_out_of_orderness` | Events can arrive out of order (most common) |
| `monotonously_increasing` | Events always arrive in order (rare) |

See [Streaming Fundamentals](../concepts/streaming-fundamentals.md) for more on watermarks.

---

## Models

Models define transformations that create new data streams.

### Auto-Inferred Materialization

**IMPORTANT:** The `materialized` field is now **auto-inferred** from your SQL and configuration. You no longer need to specify it explicitly.

**Inference Rules:**

| SQL Pattern | Inferred Materialization |
|-------------|-------------------------|
| Contains `TUMBLE`, `HOP`, `SESSION`, or `CUMULATE` | `flink` |
| Contains `GROUP BY` without window functions | `flink` |
| Contains `JOIN` | `flink` |
| Has `gateway:` configuration | `virtual_topic` |
| Has `from:` without `sql:` | `sink` |
| Simple `SELECT`, `WHERE`, projection | `topic` |

### Basic Example (No Advanced Config)

```yaml
models:
  - name: orders_validated
    description: "Validated orders"
    owner: orders-team
    tags: [orders, validated]

    sql: |
      SELECT
        order_id,
        customer_id,
        amount,
        CASE WHEN amount > 1000 THEN 'high' ELSE 'normal' END as tier
      FROM {{ source("orders_raw") }}
      WHERE order_id IS NOT NULL
        AND amount > 0

    # materialized: topic (auto-inferred from simple SELECT)
```

### Example With Advanced Overrides

```yaml
models:
  - name: hourly_revenue
    description: "Hourly revenue aggregation"
    owner: analytics-team

    sql: |
      SELECT
        TUMBLE_START(order_time, INTERVAL '1' HOUR) as window_start,
        SUM(amount) as revenue
      FROM {{ ref("orders_validated") }}
      GROUP BY TUMBLE(order_time, INTERVAL '1' HOUR)

    # materialized: flink (auto-inferred from TUMBLE)

    # Only specify when overriding defaults:
    advanced:
      flink:
        parallelism: 8
        checkpoint_interval_ms: 60000
        state_backend: rocksdb
        state_ttl_ms: 86400000

      flink_cluster: production

      topic:
        name: analytics.revenue.v1
        partitions: 12
        replication_factor: 3
        config:
          retention.ms: 604800000
          cleanup.policy: delete

    columns:
      - name: window_start
        description: "Start of the hourly window"
      - name: revenue
        description: "Total revenue in the window"
        classification: confidential
```

### Full Example (All Options)

```yaml
models:
  - name: orders_validated
    description: "Validated orders"     # Optional but recommended
    owner: orders-team                  # Optional
    tags: [orders, validated]           # Optional
    access: protected                   # Optional: private, protected, public
    group: orders                       # Optional: Logical grouping
    version: 1                          # Optional: Model version
    key: order_id                       # Optional: Partition key

    sql: |                              # Required (except for sink models)
      SELECT
        order_id,
        customer_id,
        amount,
        CASE WHEN amount > 1000 THEN 'high' ELSE 'normal' END as tier
      FROM {{ source("orders_raw") }}
      WHERE order_id IS NOT NULL
        AND amount > 0

    # Advanced configuration (optional - only when overriding defaults)
    advanced:
      # Flink job settings (for flink materialization)
      flink:
        parallelism: 4
        checkpoint_interval_ms: 60000
        state_backend: rocksdb
        state_ttl_ms: 86400000

      # Target Flink cluster
      flink_cluster: production

      # Output topic configuration
      topic:
        name: orders.validated.v1
        partitions: 12
        replication_factor: 3
        config:
          retention.ms: 604800000
          cleanup.policy: delete

      # Connect cluster (for sink materialization)
      connect_cluster: production

      # Event time configuration (advanced)
      event_time:
        watermark:
          strategy: bounded_out_of_orderness
          max_out_of_orderness_ms: 5000
        allowed_lateness_ms: 60000

    # Column metadata (stays at top level)
    columns:
      - name: customer_id
        description: "Customer identifier"
        classification: internal
      - name: amount
        description: "Order amount"
        classification: confidential

    security:                           # Optional
      classification:
        customer_id: internal
        amount: confidential
      policies:
        - mask:
            column: customer_email
            method: hash
            for_roles: [analyst]

    deprecation:                        # Optional: Version deprecation
      v1:
        sunset_date: "2025-06-01"
        message: "Use v2 instead"
```

### Model Fields (Top Level)

**Top-level fields** (always visible, business-focused):

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `name` | string | Yes | Unique model identifier |
| `description` | string | No | Human-readable description |
| `owner` | string | No | Team or person responsible |
| `tags` | list | No | Categorization tags |
| `access` | string | No | `private`, `protected`, `public` |
| `group` | string | No | Logical grouping |
| `version` | int | No | Model version number |
| `key` | string | No | Partition key column |
| `sql` | string | Conditional | SQL transformation (required except for sink) |
| `from` | string | Conditional | Source model (required for sink) |
| `columns` | list | No | Column metadata and documentation |
| `security` | object | No | Security policies and classification |
| `deprecation` | object | No | Version deprecation info |

**Note:** The `materialized` field is **no longer specified** - it's auto-inferred from your SQL and configuration.

### Model Fields (Advanced Section)

**Advanced section fields** (optional, infrastructure/tuning):

| Field | Type | Description |
|-------|------|-------------|
| `advanced.flink` | object | Flink job configuration (see below) |
| `advanced.flink_cluster` | string | Target Flink cluster name |
| `advanced.topic` | object | Output topic configuration (see below) |
| `advanced.connect_cluster` | string | Target Kafka Connect cluster |
| `advanced.event_time` | object | Event time watermark configuration |
| `advanced.gateway` | object | Gateway-specific settings |

### Advanced: Topic Configuration

Nested under `advanced.topic:`:

| Field | Type | Description |
|-------|------|-------------|
| `name` | string | Explicit topic name (default: model name) |
| `partitions` | int | Partition count |
| `replication_factor` | int | Replication factor |
| `config` | map | Kafka topic configuration |

### Advanced: Flink Configuration

Nested under `advanced.flink:`:

| Field | Type | Description | Status |
|-------|------|-------------|--------|
| `parallelism` | int | Job parallelism | Supported |
| `checkpoint_interval_ms` | int | Checkpoint interval in milliseconds | Supported |
| `state_backend` | string | `hashmap` or `rocksdb` | Parsed only |
| `state_ttl_ms` | int | State time-to-live in milliseconds | Supported |

**Planned for Future:**

```yaml
# PLANNED - Not yet supported
advanced:
  flink:
    parallelism: 4
    checkpoint_interval_ms: 60000
    state_backend: rocksdb
    state_ttl_ms: 86400000

    # Advanced checkpointing (planned)
    checkpoint_timeout_ms: 600000
    checkpoint_min_pause_ms: 500
    max_concurrent_checkpoints: 1
```

See [Flink Options Reference](flink-options.md) for complete Flink configuration.

### Security Policies

| Field | Type | Description |
|-------|------|-------------|
| `classification` | map | Column name → classification level |
| `policies` | list | Masking and access policies |

### Masking Methods

| Method | Description |
|--------|-------------|
| `hash` | SHA-256 hash of value |
| `redact` | Replace with `***` |
| `partial` | Show partial value (e.g., `***-1234`) |
| `tokenize` | Replace with reversible token |
| `null` | Replace with null |

---

## Sink Models

Sink models export data to external systems via Kafka Connect.

```yaml
models:
  - name: orders_snowflake
    description: "Export orders to Snowflake"

    # materialized: sink (auto-inferred from having 'from' without 'sql')
    from: orders_validated              # Source model or topic

    sink:
      connector: snowflake-sink
      config:
        snowflake.url.name: ${SNOWFLAKE_URL}
        snowflake.user.name: ${SNOWFLAKE_USER}
        snowflake.private.key: ${SNOWFLAKE_KEY}
        snowflake.database.name: ANALYTICS
        snowflake.schema.name: RAW
        snowflake.table.name: ORDERS
        tasks.max: 4

    # Only when overriding defaults:
    advanced:
      connect_cluster: production
```

### Sink Configuration

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `connector` | string | Yes | Connector type/class |
| `config` | map | Yes | Connector configuration |

### Common Connector Types

| Type | Use Case |
|------|----------|
| `snowflake-sink` | Snowflake data warehouse |
| `bigquery-sink` | Google BigQuery |
| `s3-sink` | Amazon S3 |
| `gcs-sink` | Google Cloud Storage |
| `jdbc-sink` | PostgreSQL, MySQL, etc. |
| `elasticsearch-sink` | Elasticsearch/OpenSearch |
| `http-sink` | HTTP/REST APIs |

---

## Tests

Tests validate data quality in your streaming pipelines.

```yaml
tests:
  - name: orders_schema_test            # Required
    model: orders_validated             # Required: Model to test
    type: schema                        # Required: schema, sample, continuous
    flink_cluster: production           # Optional

    assertions:                         # Required
      - not_null:
          columns: [order_id, customer_id, amount]

      - accepted_values:
          column: status
          values: [pending, confirmed, shipped, delivered]

      - accepted_types:
          types:
            order_id: STRING
            amount: DOUBLE
            created_at: TIMESTAMP

      - range:
          column: amount
          min: 0
          max: 1000000

      - unique_key:
          key: order_id
          window: "1 HOUR"
          tolerance: 0.01

    on_failure:                         # Optional
      severity: error                   # error, warning
      actions:
        - alert:
            type: slack
            channel: "#data-alerts"
        - dlq:
            topic: test_failures
```

### Test Fields

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `name` | string | Yes | Unique test identifier |
| `model` | string | Yes | Model to test |
| `type` | string | Yes | `schema`, `sample`, `continuous` |
| `assertions` | list | Yes | Test assertions |
| `sample_size` | int | No | Rows to sample (for sample tests) |
| `flink_cluster` | string | No | Flink cluster for execution |
| `on_failure` | object | No | Failure handling |

### Test Types

| Type | Description |
|------|-------------|
| `schema` | Validate schema structure and types |
| `sample` | Test assertions on sampled data |
| `continuous` | Ongoing streaming validation |

### Assertion Types

| Assertion | Description | Fields |
|-----------|-------------|--------|
| `not_null` | Columns must not be null | `columns` |
| `accepted_values` | Column values must be in set | `column`, `values` |
| `accepted_types` | Column types must match | `types` (map) |
| `range` | Numeric column within range | `column`, `min`, `max` |
| `unique_key` | Key uniqueness within window | `key`, `window`, `tolerance` |
| `max_lag` | Max lag from event time | `column`, `max_seconds` |
| `throughput` | Messages per second bounds | `min_per_second`, `max_per_second` |
| `distribution` | Value distribution buckets | `column`, `buckets` |
| `foreign_key` | Referential integrity | `column`, `ref_model`, `ref_key` |
| `custom_sql` | Custom SQL assertion | `sql`, `expect` |

---

## Exposures

Exposures document downstream consumers of your data.

```yaml
exposures:
  - name: checkout-service              # Required
    type: application                   # Required
    role: consumer                      # Optional: producer, consumer, both
    description: "Checkout microservice"
    owner: checkout-team
    url: https://github.com/org/checkout
    repo: org/checkout
    language: java

    consumes:                           # Topics/models consumed
      - ref: orders_validated
      - source: inventory_updates

    produces:                           # Topics/models produced
      - ref: checkout_events

    consumer_group: checkout-service-cg

    sla:
      availability: "99.9%"
      max_lag_messages: 1000
      max_lag_minutes: 5
      max_end_to_end_latency_ms: 500

    contracts:
      schema: orders-validated-value
      compatibility: BACKWARD

    access:
      roles: [checkout-team, platform-team]
      purpose: "Process customer checkouts"
```

### Exposure Types

| Type | Description |
|------|-------------|
| `application` | Microservice or application |
| `dashboard` | BI dashboard or reporting |
| `ml_training` | ML training pipeline |
| `ml_inference` | ML inference service |
| `api` | External API |

### Exposure Fields

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `name` | string | Yes | Unique exposure identifier |
| `type` | string | Yes | Exposure type |
| `role` | string | No | `producer`, `consumer`, `both` |
| `description` | string | No | Description |
| `owner` | string | No | Owner team/person |
| `url` | string | No | Documentation URL |
| `repo` | string | No | Repository |
| `language` | string | No | Programming language |
| `tool` | string | No | BI tool (for dashboards) |
| `consumes` | list | No | Consumed sources/refs |
| `produces` | list | No | Produced sources/refs |
| `consumer_group` | string | No | Kafka consumer group |
| `sla` | object | No | SLA configuration |
| `contracts` | object | No | Schema contracts |
| `access` | object | No | Access configuration |
| `schedule` | string | No | Cron schedule (for batch) |

### SLA Configuration

| Field | Type | Description |
|-------|------|-------------|
| `availability` | string | Uptime target (e.g., "99.9%") |
| `max_lag_messages` | int | Max message lag |
| `max_lag_minutes` | int | Max time lag |
| `max_produce_latency_ms` | int | Max producer latency |
| `max_end_to_end_latency_ms` | int | Max E2E latency |
| `max_error_rate` | float | Max error rate (0.0-1.0) |

---

## SQL Macros

Use Jinja-style macros in SQL to reference sources and models.

### source()

Reference a source topic:

```sql
SELECT * FROM {{ source("orders_raw") }}
```

### ref()

Reference another model:

```sql
SELECT * FROM {{ ref("orders_validated") }}
```

### Example with Multiple References

```yaml
models:
  - name: order_enriched
    description: "Orders enriched with customer data"

    # materialized: flink (auto-inferred from JOIN)
    sql: |
      SELECT
        o.order_id,
        o.amount,
        c.name as customer_name,
        c.tier as customer_tier
      FROM {{ ref("orders_validated") }} o
      JOIN {{ source("customers") }} c
        ON o.customer_id = c.id
        AND o.order_time BETWEEN c.update_time - INTERVAL '1' HOUR
                             AND c.update_time + INTERVAL '1' HOUR

    # Only when overriding defaults:
    advanced:
      flink:
        parallelism: 4
```

---

## Environment Variables

Use `${VAR_NAME}` syntax to reference environment variables:

```yaml
runtime:
  kafka:
    bootstrap_servers: ${KAFKA_BOOTSTRAP_SERVERS}
    sasl_password: ${KAFKA_PASSWORD}

  schema_registry:
    url: ${SCHEMA_REGISTRY_URL}
```

Variables can be set via:

1. System environment: `export KAFKA_PASSWORD=secret`
2. `.env` file in project root
3. CI/CD secrets

```bash title=".env"
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_PASSWORD=secret
SCHEMA_REGISTRY_URL=http://localhost:8081
```

---

## File Organization

### Single File (Small Projects)

```yaml title="stream_project.yml"
project:
  name: simple-pipeline

runtime:
  kafka:
    bootstrap_servers: localhost:9092

sources:
  - name: events
    topic: events.raw

models:
  - name: events_clean
    description: "Cleaned event stream"
    # materialized: topic (auto-inferred from simple SELECT)
    sql: |
      SELECT * FROM {{ source("events") }}
      WHERE event_id IS NOT NULL
```

### Multi-File (Large Projects)

```
my-pipeline/
├── stream_project.yml          # Project config + runtime
├── sources/
│   ├── orders.yml
│   └── customers.yml
├── models/
│   ├── staging/
│   │   ├── stg_orders.yml
│   │   └── stg_customers.yml
│   └── marts/
│       ├── order_metrics.yml
│       └── customer_360.yml
├── tests/
│   ├── orders_tests.yml
│   └── customers_tests.yml
└── exposures/
    └── applications.yml
```

Each YAML file can contain its respective type:

```yaml title="sources/orders.yml"
sources:
  - name: orders_raw
    topic: orders.raw.v1
    # ...
```

```yaml title="models/staging/stg_orders.yml"
models:
  - name: stg_orders
    description: "Staging orders with basic validation"
    # materialized: topic (auto-inferred)
    sql: |
      SELECT * FROM {{ source("orders_raw") }}
      WHERE order_id IS NOT NULL
```
