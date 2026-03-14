---
title: Flink Options Reference
description: Complete reference for Flink configuration in streamt
---

# Flink Options Reference

This page documents all Flink-related configuration options in streamt.

## Current Status

| Category | Status | Notes |
|----------|--------|-------|
| Basic job submission | Supported | Via REST API and SQL Gateway |
| Parallelism | Supported | Per-job configuration |
| Checkpointing | Partial | Interval only, no advanced options |
| State backend | Partial | Type selection only |
| MATCH_RECOGNIZE (CEP) | Supported | Complex event processing patterns |

---

## Model Flink Configuration

Configure Flink jobs in your model definitions using the `advanced` section:

```yaml
models:
  - name: my_aggregation
    description: "Hourly aggregation"

    # materialized: flink (auto-inferred from GROUP BY)
    sql: |
      SELECT
        customer_id,
        COUNT(*) as order_count
      FROM {{ ref("orders") }}
      GROUP BY customer_id

    # Only when overriding defaults:
    advanced:
      flink:
        parallelism: 4
        checkpoint_interval_ms: 60000
        state_backend: rocksdb
        state_ttl_ms: 86400000

      flink_cluster: production    # Which cluster to deploy to
```

### Supported Options

All Flink options are nested under `advanced.flink`:

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `parallelism` | int | 1 | Job parallelism (number of parallel tasks) |
| `checkpoint_interval_ms` | int | 60000 | Checkpoint interval in milliseconds |
| `state_ttl_ms` | int | none | State TTL in milliseconds (see [State TTL](#state-ttl)) |

### State TTL

State TTL (Time-To-Live) controls how long Flink keeps state entries before expiring them. This is **critical** for preventing unbounded state growth in streaming jobs.

```yaml
models:
  - name: customer_counts
    description: "Customer order counts"

    # materialized: flink (auto-inferred from GROUP BY)
    sql: |
      SELECT customer_id, COUNT(*)
      FROM {{ ref("orders") }}
      GROUP BY customer_id

    advanced:
      flink:
        state_ttl_ms: 86400000  # 24 hours
```

**When to use State TTL:**

| Operation | State Growth | Recommendation |
|-----------|--------------|----------------|
| `GROUP BY` without window | Unbounded | Add TTL |
| `JOIN` without time bounds | Unbounded | Add TTL |
| `DISTINCT` | Unbounded | Add TTL |
| Windowed aggregations | Bounded by window | TTL optional |
| Stateless transforms | No state | TTL not needed |

**Common configurations:**

| Use Case | TTL Value | Duration |
|----------|-----------|----------|
| Short-lived joins | 3600000 | 1 hour |
| Daily aggregations | 86400000 | 24 hours |
| Weekly patterns | 604800000 | 7 days |
| Monthly analytics | 2592000000 | 30 days |

**Trade-offs:**

- **TTL too short**: State expires before it's needed → incorrect results for returning entities
- **TTL too long**: State grows too large → memory pressure, longer recovery times
- **No TTL**: State grows forever → eventual job failure

### state_backend (Parsed Only)

The `state_backend` option is parsed from YAML but not yet applied to Flink jobs. The state backend is currently determined by your Flink cluster configuration.

---

### Watermark Strategy

```yaml
# SUPPORTED - Use in sources or models
sources:
  - name: events
    topic: events.raw.v1

    # Top-level: column name
    event_time:
      column: event_timestamp

    # Advanced section: watermark details
    advanced:
      event_time:
        watermark:
          strategy: bounded_out_of_orderness
          max_out_of_orderness_ms: 5000
        # OR
        watermark:
          strategy: monotonous
```

| Option | Location | Type | Description |
|--------|----------|------|-------------|
| `event_time.column` | Top-level | string | Event time column |
| `event_time.watermark.strategy` | Advanced | string | `bounded_out_of_orderness` or `monotonous` |
| `event_time.watermark.max_out_of_orderness_ms` | Advanced | int | Max out-of-orderness for bounded strategy |

---

## Runtime Flink Cluster Configuration

Configure Flink clusters in your project's runtime section:

```yaml
runtime:
  flink:
    default: production
    clusters:
      local:
        type: rest
        rest_url: http://localhost:8082
        sql_gateway_url: http://localhost:8084

      production:
        type: rest
        rest_url: http://flink-jobmanager:8081
        sql_gateway_url: http://flink-sql-gateway:8083
        version: "1.18"
```

### Cluster Types

| Type | Description | Status |
|------|-------------|--------|
| `rest` | Connect via REST API | Supported |
| `confluent` | Confluent Cloud for Flink | Supported |

### REST Cluster Configuration

```yaml
clusters:
  my-cluster:
    type: rest
    rest_url: http://flink-jobmanager:8081
    sql_gateway_url: http://flink-sql-gateway:8083
    version: "1.18"
    environment: production
    api_key: ${FLINK_API_KEY}
```

| Option | Type | Required | Description |
|--------|------|----------|-------------|
| `type` | string | Yes | Cluster type (`rest`) |
| `rest_url` | string | Yes | Flink REST API URL |
| `sql_gateway_url` | string | Yes | SQL Gateway URL |
| `version` | string | No | Flink version |
| `environment` | string | No | Environment identifier |
| `api_key` | string | No | API key for authentication |

---

## Flink SQL Features Reference

### Supported Window Functions

#### TVF Syntax (Recommended, Flink 1.15+)

The Table-Valued Function (TVF) syntax is the modern Flink window API. It appends `window_start`, `window_end`, and `window_time` columns automatically.

| Function | Description | Example |
|----------|-------------|---------|
| `TUMBLE` | Fixed-size, non-overlapping | `TABLE(TUMBLE(TABLE t, DESCRIPTOR(ts), INTERVAL '5' MINUTE))` |
| `HOP` | Fixed-size, overlapping | `TABLE(HOP(TABLE t, DESCRIPTOR(ts), INTERVAL '1' MINUTE, INTERVAL '5' MINUTE))` |
| `SESSION` | Gap-based sessions | `TABLE(SESSION(TABLE t, DESCRIPTOR(ts), INTERVAL '10' MINUTE))` |
| `CUMULATE` | Cumulative windows | `TABLE(CUMULATE(TABLE t, DESCRIPTOR(ts), INTERVAL '1' MINUTE, INTERVAL '1' HOUR))` |

```sql
-- TVF example: tumbling window
SELECT window_start, window_end, COUNT(*) AS cnt
FROM TABLE(TUMBLE(TABLE orders, DESCRIPTOR(ts), INTERVAL '1' HOUR))
GROUP BY window_start, window_end
```

#### Legacy GROUP BY Syntax

Still supported but deprecated. Does not provide `window_start`/`window_end` columns directly — use accessor functions instead.

| Function | Description | Example |
|----------|-------------|---------|
| `TUMBLE` | Fixed-size, non-overlapping | `GROUP BY TUMBLE(ts, INTERVAL '5' MINUTE)` |
| `HOP` | Fixed-size, overlapping | `GROUP BY HOP(ts, INTERVAL '1' MINUTE, INTERVAL '5' MINUTE)` |
| `SESSION` | Gap-based sessions | `GROUP BY SESSION(ts, INTERVAL '10' MINUTE)` |

### Window Accessors (Legacy Syntax)

These functions are used with the legacy `GROUP BY` window syntax to extract window boundaries:

| Function | Description |
|----------|-------------|
| `TUMBLE_START(ts, size)` | Window start timestamp |
| `TUMBLE_END(ts, size)` | Window end timestamp |
| `TUMBLE_ROWTIME(ts, size)` | Window rowtime attribute |
| `HOP_START(...)` | Hopping window start |
| `HOP_END(...)` | Hopping window end |
| `SESSION_START(...)` | Session window start |
| `SESSION_END(...)` | Session window end |

With TVF syntax, use `window_start`, `window_end`, `window_time` columns directly instead of these accessor functions.

### Supported Join Types

| Join Type | Description | Time Constraint Required |
|-----------|-------------|--------------------------|
| Regular join | Stream-stream join | Yes (prevents state explosion) |
| Interval join | Time-bounded join | Yes (explicit BETWEEN) |
| Temporal join | Point-in-time lookup | Yes (FOR SYSTEM_TIME AS OF) |
| Lookup join | External table lookup | No |

### Join Examples

```sql
-- Interval join (recommended for stream-stream)
SELECT o.*, c.name
FROM orders o, customers c
WHERE o.customer_id = c.id
  AND o.order_time BETWEEN c.update_time - INTERVAL '1' HOUR
                       AND c.update_time + INTERVAL '1' HOUR

-- Temporal join (point-in-time lookup)
SELECT o.*, p.price
FROM orders o
JOIN products FOR SYSTEM_TIME AS OF o.order_time AS p
  ON o.product_id = p.id
```

### Complex Event Processing (MATCH_RECOGNIZE)

`MATCH_RECOGNIZE` enables pattern matching on event streams - detecting sequences of events that match a specified pattern. This is useful for fraud detection, user behavior analysis, and anomaly detection.

| Feature | Description |
|---------|-------------|
| Pattern matching | Detect sequences like "A followed by B then C" |
| Quantifiers | Match zero or more (`*`), one or more (`+`), optional (`?`) |
| Measures | Extract values from matched events |
| Row pattern output | `ONE ROW PER MATCH` or `ALL ROWS PER MATCH` |

**Example: Detect Price Increases**

```sql
SELECT *
FROM stock_prices
MATCH_RECOGNIZE (
  PARTITION BY symbol
  ORDER BY ts
  MEASURES
    A.price AS start_price,
    LAST(B.price) AS end_price,
    A.ts AS start_time,
    LAST(B.ts) AS end_time
  ONE ROW PER MATCH
  AFTER MATCH SKIP PAST LAST ROW
  PATTERN (A B+)
  DEFINE
    A AS A.price > 0,
    B AS B.price > LAST(price)
) AS m
```

**Example: User Session Pattern**

```sql
-- Detect users who browse, add to cart, then purchase
SELECT *
FROM user_events
MATCH_RECOGNIZE (
  PARTITION BY user_id
  ORDER BY event_time
  MEASURES
    FIRST(A.event_time) AS session_start,
    C.event_time AS purchase_time,
    C.amount AS purchase_amount
  ONE ROW PER MATCH
  PATTERN (A+ B+ C)
  DEFINE
    A AS A.event_type = 'browse',
    B AS B.event_type = 'add_to_cart',
    C AS C.event_type = 'purchase'
) AS matched
```

!!! tip "Pattern Quantifiers"
    - `A` — Exactly one A
    - `A*` — Zero or more A
    - `A+` — One or more A
    - `A?` — Zero or one A
    - `A{3}` — Exactly 3 A
    - `A{2,4}` — Between 2 and 4 A

---

## SQL Generation

streamt generates Flink SQL from your YAML definitions. Understanding the generated SQL helps with debugging.

### Example: Simple Filter

**Input YAML:**
```yaml
models:
  - name: orders_valid
    description: "Valid orders only"
    # materialized: topic (auto-inferred from simple SELECT)
    sql: |
      SELECT * FROM {{ source("orders_raw") }}
      WHERE amount > 0
```

**Generated Flink SQL:**
```sql
-- Create source table
CREATE TABLE orders_raw (
  `order_id` STRING,
  `customer_id` STRING,
  `amount` DOUBLE,
  `created_at` TIMESTAMP(3)
) WITH (
  'connector' = 'kafka',
  'topic' = 'orders.raw.v1',
  'properties.bootstrap.servers' = 'kafka:29092',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json'
);

-- Create sink table
CREATE TABLE orders_valid_sink (
  `order_id` STRING,
  `customer_id` STRING,
  `amount` DOUBLE,
  `created_at` TIMESTAMP(3)
) WITH (
  'connector' = 'kafka',
  'topic' = 'orders_valid',
  'properties.bootstrap.servers' = 'kafka:29092',
  'format' = 'json'
);

-- Execute transformation
INSERT INTO orders_valid_sink
SELECT * FROM orders_raw
WHERE amount > 0;
```

### Example: Windowed Aggregation

**Input YAML:**
```yaml
models:
  - name: hourly_revenue
    description: "Hourly revenue aggregation"

    # materialized: flink (auto-inferred from window TVF)
    sql: |
      SELECT
        window_start,
        window_end,
        SUM(amount) as revenue
      FROM TABLE(TUMBLE(TABLE {{ ref("orders_valid") }}, DESCRIPTOR(order_time), INTERVAL '1' HOUR))
      GROUP BY window_start, window_end

    # Only when overriding defaults:
    advanced:
      flink:
        parallelism: 4
```

**Generated Flink SQL:**
```sql
SET 'parallelism.default' = '4';

CREATE TABLE orders_valid (
  `order_id` STRING,
  `customer_id` STRING,
  `amount` DOUBLE,
  `order_time` TIMESTAMP(3),
  WATERMARK FOR order_time AS order_time - INTERVAL '5' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'orders_valid',
  'properties.bootstrap.servers' = 'kafka:29092',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json'
);

CREATE TABLE hourly_revenue_sink (
  `window_start` TIMESTAMP(3),
  `window_end` TIMESTAMP(3),
  `revenue` DOUBLE
) WITH (
  'connector' = 'kafka',
  'topic' = 'hourly_revenue',
  'properties.bootstrap.servers' = 'kafka:29092',
  'format' = 'json'
);

INSERT INTO hourly_revenue_sink
SELECT
  window_start,
  window_end,
  SUM(amount) as revenue
FROM TABLE(TUMBLE(TABLE orders_valid, DESCRIPTOR(order_time), INTERVAL '1' HOUR))
GROUP BY window_start, window_end;
```

---

## Troubleshooting

### Common Issues

| Issue | Cause | Solution |
|-------|-------|----------|
| Job fails to start | SQL Gateway not running | Ensure `sql_gateway_url` is correct |
| State grows unbounded | No TTL configured | Add `state_ttl_ms` in advanced.flink |
| Late data dropped | Watermark too aggressive | Increase `max_out_of_orderness_ms` |
| OOM errors | State too large for heap | Use `rocksdb` state backend |
| Checkpoint failures | Timeout too short | Tune checkpoint interval |

### Debugging Tips

1. **Check Flink UI**: Access at `rest_url` to view job status, exceptions, and metrics

2. **View generated SQL**: Run `streamt plan --show-sql` to see generated Flink SQL

3. **Test SQL locally**: Copy generated SQL to Flink SQL CLI for testing

4. **Check logs**: Look at TaskManager logs for detailed error messages

---

## Version Compatibility

| streamt Version | Flink Versions | SQL Gateway Required |
|-----------------|----------------|---------------------|
| 0.1.x (current) | 1.17, 1.18, 1.19 | Yes |

### Flink Version Notes

- **Flink 1.17+**: SQL Gateway is required for SQL submission
- **Flink 1.18+**: Recommended for best SQL features
- **Flink 1.19+**: Full support, recommended for production
