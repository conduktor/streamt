---
title: Models
description: Transformations that create new data streams
---

# Models

Models are the heart of streamt. They define transformations that produce new data streams from sources and other models.

## What is a Model?

A **model** is a named transformation that:

- Takes input from sources or other models
- Applies SQL transformations
- Produces output to a new stream
- Is deployed as infrastructure (topic, Flink job, or connector)

## Basic Model

```yaml title="models/orders_clean.yml"
models:
  - name: orders_clean
    description: Cleaned and validated orders
    sql: |
      SELECT order_id, customer_id, amount
      FROM {{ source("orders_raw") }}
      WHERE amount > 0
```

The `materialized` property is automatically inferred from your SQL. Simple transforms become topics; windowed aggregations become Flink jobs.

## Materializations

Materializations are **automatically inferred** from your SQL:

- **Stateless** SQL (`WHERE`, projections) → **virtual_topic** (if Gateway configured) or **flink** (fallback)
- **Stateful** SQL (`GROUP BY`, `JOIN`, windows) → **flink** materialization
- `ML_PREDICT`, `ML_EVALUATE` → **flink** (Confluent Cloud only)
- `from:` only (no SQL) → **sink** materialization

You can override auto-inference by explicitly specifying `materialized:`.

### Topic Materialization

Creates a Kafka topic with stateless transformations:

```yaml
- name: high_value_orders
  sql: |
    SELECT *
    FROM {{ source("orders") }}
    WHERE amount > 10000

  # Advanced configuration (optional)
  advanced:
    topic:
      name: orders.high-value.v1
      partitions: 12
      config:
        retention.ms: 604800000
```

**Best for:** Filtering, field selection, simple transformations

### Virtual Topic Materialization

Creates a Gateway rule for read-time filtering (requires Conduktor):

```yaml
- name: orders_europe
  materialized: virtual_topic  # Must be explicit
  from: orders_clean
  sql: |
    SELECT *
    FROM {{ ref("orders_clean") }}
    WHERE region = 'EU'
```

**Best for:** Multi-tenant views, access control, no additional storage

### Flink Materialization

Deploys a Flink SQL job for stateful processing:

```yaml
- name: hourly_revenue
  sql: |
    SELECT
      TUMBLE_START(event_time, INTERVAL '1' HOUR) as window_start,
      SUM(amount) as total_revenue,
      COUNT(*) as order_count
    FROM {{ ref("orders_clean") }}
    GROUP BY TUMBLE(event_time, INTERVAL '1' HOUR)

  # Advanced configuration (optional)
  advanced:
    flink:
      parallelism: 8
      checkpoint_interval: 60000
    topic:
      name: analytics.revenue.v1
      partitions: 6
```

The `TUMBLE` window automatically triggers Flink materialization.

**Best for:** Windowed aggregations, joins, complex state

### Sink Materialization

Creates a Kafka Connect connector to export data:

```yaml
- name: orders_snowflake
  from: orders_clean  # No SQL = sink materialization
  advanced:
    connector:
      type: snowflake-sink
      config:
        snowflake.url.name: ${SNOWFLAKE_URL}
        snowflake.database.name: ANALYTICS
        snowflake.schema.name: ORDERS
```

Using `from:` without `sql:` automatically triggers sink materialization.

**Best for:** Exporting to data warehouses, databases, S3

### ML Inference (Confluent Flink)

Use `ML_PREDICT` and `ML_EVALUATE` for real-time ML inference:

```yaml
- name: fraud_scores
  sql: |
    SELECT
      transaction_id,
      amount,
      ML_PREDICT('FraudModel', amount, merchant_id) as prediction
    FROM {{ ref("transactions") }}

  # Declare ML output schema for proper type inference
  ml_outputs:
    FraudModel:
      fraud_score: DOUBLE
      confidence: DOUBLE
```

**Requirements:**
- Confluent Cloud Flink cluster
- Model registered in Confluent's model registry

**Best for:** Real-time fraud detection, recommendations, anomaly detection

## Complete Model Reference

```yaml
models:
  - name: customer_metrics
    description: |
      Real-time customer metrics aggregated by hour.
      Used for the customer dashboard and recommendations.

    # Ownership and organization
    owner: analytics-team
    tags: [analytics, customer, kpi]
    access: protected  # public, protected, private

    # Partitioning key
    key: customer_id

    # SQL transformation
    sql: |
      SELECT
        customer_id,
        email,
        phone,
        TUMBLE_START(event_time, INTERVAL '1' HOUR) as window_start,
        COUNT(*) as event_count,
        SUM(CASE WHEN event_type = 'purchase' THEN amount ELSE 0 END) as revenue
      FROM {{ source("customer_events") }}
      GROUP BY
        customer_id,
        email,
        phone,
        TUMBLE(event_time, INTERVAL '1' HOUR)

    # Advanced configuration (optional)
    advanced:
      # Output topic configuration
      topic:
        name: analytics.customer-metrics.v1
        partitions: 12
        replication_factor: 3
        config:
          retention.ms: 2592000000  # 30 days
          cleanup.policy: delete

      # Flink job configuration
      flink:
        parallelism: 8
        checkpoint_interval: 60000
        state_backend: rocksdb
        cluster: production

      # Security policies
      security:
        masking:
          - column: email
            policy: hash
          - column: phone
            policy: partial
            config:
              visible_chars: 4
```

## SQL Syntax

### Referencing Sources

Use `{{ source("name") }}` to reference a source:

```sql
SELECT * FROM {{ source("orders_raw") }}
```

### Referencing Other Models

Use `{{ ref("name") }}` to reference another model:

```sql
SELECT *
FROM {{ ref("orders_clean") }} o
JOIN {{ ref("customers") }} c ON o.customer_id = c.id
```

### Jinja Templating

Use Jinja for dynamic SQL:

```sql
SELECT
  order_id,
  amount,
  {% if var("include_pii", false) %}
  customer_email,
  {% endif %}
  created_at
FROM {{ source("orders") }}
WHERE created_at > '{{ var("start_date") }}'
```

### Reserved Keywords

When using SQL reserved words as column names, escape them:

```sql
SELECT
  `timestamp`,  -- Reserved keyword
  `value`,      -- Reserved keyword
  user_id
FROM {{ source("events") }}
```

## Topic Configuration

Configure the output Kafka topic using the `advanced:` section:

```yaml
advanced:
  topic:
    name: orders.clean.v1        # Topic name
    partitions: 12               # Number of partitions
    replication_factor: 3        # Replication factor
    config:                      # Topic configs
      retention.ms: 604800000    # 7 days
      cleanup.policy: delete     # or compact
      min.insync.replicas: 2
```

## Flink Configuration

Configure Flink job settings using the `advanced:` section:

```yaml
advanced:
  flink:
    parallelism: 8                  # Job parallelism
    checkpoint_interval: 60000      # Checkpoint interval (ms)
    checkpoint_timeout: 300000      # Checkpoint timeout (ms)
    state_backend: rocksdb          # hashmap or rocksdb
    restart_strategy: fixed-delay   # Restart strategy
    cluster: production             # Target Flink cluster
```

## Connector Configuration

Configure Kafka Connect sinks using the `advanced:` section:

```yaml
advanced:
  connector:
    type: snowflake-sink           # Connector type
    tasks_max: 4                   # Number of tasks
    config:
      # Connector-specific configuration
      snowflake.url.name: ${SNOWFLAKE_URL}
      snowflake.user.name: ${SNOWFLAKE_USER}
      snowflake.database.name: ANALYTICS
```

Supported connector types:

| Type | Target |
|------|--------|
| `snowflake-sink` | Snowflake |
| `bigquery-sink` | BigQuery |
| `s3-sink` | Amazon S3 |
| `jdbc-sink` | PostgreSQL, MySQL, etc. |
| `elasticsearch-sink` | Elasticsearch |

## Access Control

Control who can read your model's output:

```yaml
access: protected  # or public, private

# With group-based access
access:
  level: protected
  allowed_groups:
    - analytics-team
    - data-science
```

| Level | Description |
|-------|-------------|
| `public` | Anyone can consume |
| `protected` | Only specified groups |
| `private` | Only within this project |

## Security & Masking

Apply masking policies to sensitive columns:

```yaml
security:
  masking:
    - column: email
      policy: hash      # MD5 hash

    - column: phone
      policy: partial   # Show last 4 digits
      config:
        visible_chars: 4

    - column: ssn
      policy: redact    # Replace with ***

    - column: credit_card
      policy: tokenize  # Replace with token
```

## Dependencies

Dependencies are automatically extracted from your SQL:

```yaml
- name: enriched_orders
  sql: |
    SELECT o.*, c.name as customer_name
    FROM {{ ref("orders_clean") }} o        -- Depends on orders_clean
    JOIN {{ source("customers") }} c         -- Depends on customers source
    ON o.customer_id = c.id
```

This model depends on both `orders_clean` and the `customers` source. streamt builds a DAG from these dependencies.

## Best Practices

### 1. One Transformation Per Model

```yaml
# Good: Single responsibility
- name: orders_validated
  sql: SELECT * FROM {{ source("orders") }} WHERE valid = true

- name: orders_enriched
  sql: |
    SELECT o.*, c.tier
    FROM {{ ref("orders_validated") }} o
    JOIN {{ source("customers") }} c ON o.customer_id = c.id

# Bad: Too much in one model
- name: orders_all_in_one
  sql: |
    WITH validated AS (...),
         enriched AS (...),
         aggregated AS (...)
    SELECT * FROM aggregated
```

### 2. Use Meaningful Names

```yaml
# Good
- name: daily_revenue_by_region
- name: high_risk_transactions
- name: customer_lifetime_value

# Bad
- name: orders_v2
- name: temp_table
- name: test_model
```

### 3. Document Everything

```yaml
- name: fraud_scores
  description: |
    Real-time fraud risk scores for transactions.
    Score ranges from 0 (safe) to 100 (high risk).
    Updated every 5 seconds based on recent patterns.
  owner: fraud-team
  tags: [fraud, ml, real-time]
```

### 4. Set Appropriate Partitions

```yaml
# High-volume: more partitions for parallelism
advanced:
  topic:
    partitions: 24

# Low-volume: fewer partitions
advanced:
  topic:
    partitions: 3
```

### 5. Use Keys for Ordering

```yaml
# Ensures customer events stay ordered
- name: customer_events_clean
  key: customer_id  # Partition by customer
```

## Example: Multi-stage Pipeline

```yaml title="models/pipeline.yml"
models:
  # Stage 1: Clean raw data
  - name: events_clean
    sql: |
      SELECT event_id, user_id, event_type, created_at
      FROM {{ source("events_raw") }}
      WHERE event_id IS NOT NULL

  # Stage 2: Enrich with user data
  - name: events_enriched
    sql: |
      SELECT e.*, u.tier, u.country
      FROM {{ ref("events_clean") }} e
      JOIN {{ source("users") }} u ON e.user_id = u.id

  # Stage 3: Aggregate metrics (auto-inferred as Flink due to TUMBLE)
  - name: hourly_metrics
    sql: |
      SELECT
        country,
        TUMBLE_START(created_at, INTERVAL '1' HOUR) as hour,
        COUNT(*) as event_count
      FROM {{ ref("events_enriched") }}
      GROUP BY country, TUMBLE(created_at, INTERVAL '1' HOUR)

  # Stage 4: Export to warehouse (auto-inferred as sink)
  - name: metrics_warehouse
    from: hourly_metrics
    advanced:
      connector:
        type: snowflake-sink
```

## Next Steps

- [Materializations Reference](../reference/materializations.md) — Detailed materialization docs
- [Tests](tests.md) — Test your models
- [DAG & Lineage](dag.md) — Understand dependencies
