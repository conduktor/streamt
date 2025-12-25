---
title: Flink Options Reference
description: Complete reference for Flink configuration in streamt
---

# Flink Options Reference

This page documents all Flink-related configuration options in streamt, including what's currently supported and what's planned for future releases.

## Current Status

| Category | Status | Notes |
|----------|--------|-------|
| Basic job submission | Supported | Via REST API and SQL Gateway |
| Parallelism | Supported | Per-job configuration |
| Checkpointing | Partial | Interval only, no advanced options |
| State backend | Partial | Type selection only |
| MATCH_RECOGNIZE (CEP) | Supported | Complex event processing patterns |
| Savepoints | Planned | Not yet implemented |
| Kubernetes operator | Planned | Currently REST API only |

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

### Parsed But Not Yet Applied

These options are parsed from YAML but not yet used when deploying jobs:

| Option | Location | Type | Description | Status |
|--------|----------|------|-------------|--------|
| `state_backend` | `advanced.flink` | string | State backend type (`hashmap`, `rocksdb`) | Parsed only |

!!! warning "state_backend not applied"
    The `state_backend` option is currently parsed but **not applied** to Flink jobs. The state backend is determined by your Flink cluster configuration. Support for setting this via streamt is planned.

---

## Planned Options (Not Yet Implemented)

The following options are planned for future releases:

### Checkpointing (Advanced)

```yaml
# PLANNED - Not yet supported
advanced:
  flink:
    checkpoint:
      interval_ms: 60000
      timeout_ms: 600000
      min_pause_ms: 500
      max_concurrent: 1
      mode: exactly_once          # exactly_once, at_least_once
      externalized:
        enabled: true
        cleanup: retain_on_cancellation
      unaligned:
        enabled: false
      incremental: true           # For RocksDB
```

| Option | Type | Description | Status |
|--------|------|-------------|--------|
| `checkpoint.interval_ms` | int | Checkpoint interval | Supported |
| `checkpoint.timeout_ms` | int | Checkpoint timeout | Planned |
| `checkpoint.min_pause_ms` | int | Min pause between checkpoints | Planned |
| `checkpoint.max_concurrent` | int | Max concurrent checkpoints | Planned |
| `checkpoint.mode` | string | `exactly_once` or `at_least_once` | Planned |
| `checkpoint.externalized.enabled` | bool | Enable externalized checkpoints | Planned |
| `checkpoint.externalized.cleanup` | string | Cleanup policy on cancellation | Planned |
| `checkpoint.unaligned.enabled` | bool | Enable unaligned checkpoints | Planned |
| `checkpoint.incremental` | bool | Incremental checkpoints (RocksDB) | Planned |

### State Backend (Advanced)

```yaml
# PLANNED - Not yet supported
advanced:
  flink:
    state:
      backend: rocksdb
      rocksdb:
        block_cache_size_mb: 256
        write_buffer_size_mb: 64
        max_write_buffer_number: 4
        predefined_options: SPINNING_DISK_OPTIMIZED_HIGH_MEM
      ttl_ms: 86400000              # State TTL
      incremental_cleanup:
        enabled: true
        records_per_cleanup: 1000
```

| Option | Type | Description | Status |
|--------|------|-------------|--------|
| `state.backend` | string | State backend type | Supported |
| `state.ttl_ms` | int | State TTL in milliseconds | Planned |
| `state.rocksdb.*` | various | RocksDB tuning options | Planned |
| `state.incremental_cleanup.*` | various | Incremental cleanup config | Planned |

### Restart Strategy

```yaml
# PLANNED - Not yet supported
advanced:
  flink:
    restart:
      strategy: fixed_delay        # fixed_delay, failure_rate, exponential_delay, none
      fixed_delay:
        attempts: 3
        delay_ms: 10000
      failure_rate:
        max_failures_per_interval: 3
        failure_interval_ms: 60000
        delay_ms: 10000
      exponential_delay:
        initial_delay_ms: 1000
        max_delay_ms: 60000
        multiplier: 2.0
```

| Option | Type | Description | Status |
|--------|------|-------------|--------|
| `restart.strategy` | string | Restart strategy type | Planned |
| `restart.fixed_delay.*` | various | Fixed delay restart config | Planned |
| `restart.failure_rate.*` | various | Failure rate restart config | Planned |
| `restart.exponential_delay.*` | various | Exponential backoff config | Planned |

### Resource Configuration

```yaml
# PLANNED - Not yet supported
advanced:
  flink:
    resources:
      task_manager:
        memory_mb: 4096
        cpu_cores: 2
        slots: 4
      job_manager:
        memory_mb: 2048
        cpu_cores: 1
```

| Option | Type | Description | Status |
|--------|------|-------------|--------|
| `resources.task_manager.memory_mb` | int | TM memory | Planned |
| `resources.task_manager.cpu_cores` | float | TM CPU cores | Planned |
| `resources.task_manager.slots` | int | TM task slots | Planned |
| `resources.job_manager.memory_mb` | int | JM memory | Planned |
| `resources.job_manager.cpu_cores` | float | JM CPU cores | Planned |

### Savepoint Management

```yaml
# PLANNED - Not yet supported
advanced:
  flink:
    savepoint:
      enabled: true
      path: s3://my-bucket/savepoints
      on_upgrade: trigger_and_restore
      on_cancel: trigger
```

| Option | Type | Description | Status |
|--------|------|-------------|--------|
| `savepoint.enabled` | bool | Enable savepoint management | Planned |
| `savepoint.path` | string | Savepoint storage path | Planned |
| `savepoint.on_upgrade` | string | Behavior on job upgrade | Planned |
| `savepoint.on_cancel` | string | Behavior on job cancel | Planned |

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
        # OR (planned)
        watermark:
          strategy: custom
          expression: "event_timestamp - INTERVAL '5' SECOND"
```

| Option | Location | Type | Description | Status |
|--------|----------|------|-------------|--------|
| `event_time.column` | Top-level | string | Event time column | Supported |
| `event_time.watermark.strategy` | Advanced | string | Watermark strategy | Supported |
| `event_time.watermark.max_out_of_orderness_ms` | Advanced | int | Allowed lateness | Supported |
| `event_time.watermark.expression` | Advanced | string | Custom watermark SQL | Planned |

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
| `docker` | Local Docker deployment | Planned |
| `kubernetes` | Kubernetes Flink Operator | Planned |

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

### Kubernetes Cluster Configuration (Planned)

```yaml
# PLANNED - Not yet supported
clusters:
  k8s-cluster:
    type: kubernetes
    namespace: flink-jobs
    service_account: flink-sa
    image: flink:1.18
    image_pull_policy: IfNotPresent
    image_pull_secrets:
      - flink-registry-secret
    pod_template:
      spec:
        tolerations:
          - key: "streaming"
            operator: "Equal"
            value: "true"
            effect: "NoSchedule"
```

---

## Flink SQL Features Reference

### Supported Window Functions

| Function | Description | Example |
|----------|-------------|---------|
| `TUMBLE` | Fixed-size, non-overlapping | `GROUP BY TUMBLE(ts, INTERVAL '5' MINUTE)` |
| `HOP` | Fixed-size, overlapping | `GROUP BY HOP(ts, INTERVAL '1' MINUTE, INTERVAL '5' MINUTE)` |
| `SESSION` | Gap-based sessions | `GROUP BY SESSION(ts, INTERVAL '10' MINUTE)` |
| `CUMULATE` | Cumulative windows | `GROUP BY CUMULATE(ts, INTERVAL '1' MINUTE, INTERVAL '1' HOUR)` |

### Window Accessors

| Function | Description |
|----------|-------------|
| `TUMBLE_START(ts, size)` | Window start timestamp |
| `TUMBLE_END(ts, size)` | Window end timestamp |
| `TUMBLE_ROWTIME(ts, size)` | Window rowtime attribute |
| `HOP_START(...)` | Hopping window start |
| `HOP_END(...)` | Hopping window end |
| `SESSION_START(...)` | Session window start |
| `SESSION_END(...)` | Session window end |

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

    # materialized: flink (auto-inferred from TUMBLE)
    sql: |
      SELECT
        TUMBLE_START(order_time, INTERVAL '1' HOUR) as window_start,
        SUM(amount) as revenue
      FROM {{ ref("orders_valid") }}
      GROUP BY TUMBLE(order_time, INTERVAL '1' HOUR)

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
  `revenue` DOUBLE
) WITH (
  'connector' = 'kafka',
  'topic' = 'hourly_revenue',
  'properties.bootstrap.servers' = 'kafka:29092',
  'format' = 'json'
);

INSERT INTO hourly_revenue_sink
SELECT
  TUMBLE_START(order_time, INTERVAL '1' HOUR) as window_start,
  SUM(amount) as revenue
FROM orders_valid
GROUP BY TUMBLE(order_time, INTERVAL '1' HOUR);
```

---

## Troubleshooting

### Common Issues

| Issue | Cause | Solution |
|-------|-------|----------|
| Job fails to start | SQL Gateway not running | Ensure `sql_gateway_url` is correct |
| State grows unbounded | No TTL configured | Add state TTL (coming soon) |
| Late data dropped | Watermark too aggressive | Increase `max_out_of_orderness` (coming soon) |
| OOM errors | State too large for heap | Use `rocksdb` state backend |
| Checkpoint failures | Timeout too short | Increase checkpoint timeout (coming soon) |

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

---

## Roadmap

### Completed

- [x] **Event time configuration** — `event_time.column`, `event_time.watermark` in source/model YAML
- [x] **`streamt status` command** — Show running jobs with health, lag, checkpoint status
- [x] **State TTL configuration** — `state_ttl_ms` to prevent unbounded state growth
- [x] **Watermark strategies** — bounded out-of-orderness, monotonous
- [x] **Confluent Cloud for Flink** — `type: confluent` cluster with ML_PREDICT/ML_EVALUATE support

### Soon

- [ ] **Advanced checkpoint options** — timeout, min pause, externalized checkpoints
- [ ] **Custom watermark expressions** — User-defined watermark SQL

### Later

- [ ] **Savepoint management** — Trigger savepoints on upgrade, restore from savepoint
- [ ] **Kubernetes Flink Operator integration** — Deploy via K8s CRDs instead of REST API
- [ ] **Prometheus/OpenTelemetry metrics** — Export job metrics for observability
- [ ] **Resource configuration** — Memory, CPU, task slots per job

### Deferred

- [ ] Changelog mode configuration (append, upsert, retract)

See [GitHub Issues](https://github.com/conduktor/streamt/issues) for the latest roadmap updates.
