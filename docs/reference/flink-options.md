---
title: Flink Options Reference
description: Complete reference for Flink configuration in streamt
---

# Flink Options Reference

This page documents all Flink-related configuration options in streamt.

## Job Lifecycle on Apply

When you run `streamt apply`, each Flink job goes through a planning phase that
inspects the cluster and decides what action to take. This section documents the
exact behavior for every scenario.

### Decision Flow

```
┌─────────────────────┐
│  Does the job exist  │
│  on the cluster?     │
└──────┬──────┬────────┘
       │      │
      No     Yes
       │      │
       ▼      ▼
   SUBMIT   ┌──────────────────┐
   new job  │ Is it RUNNING or │
            │ CREATED?         │
            └──┬───────┬───────┘
              Yes      No (FAILED, CANCELED, …)
               │       │
               ▼       ▼
         ┌──────────┐  SUBMIT
         │ Has SQL   │  (re-submit as new job)
         │ changed?  │
         └──┬────┬───┘
           Yes   No
            │     │
            ▼     ▼
        CANCEL   SKIP
        + SUBMIT (unchanged)
```

### 1. New Job (no existing job on cluster)

The job is submitted for the first time. The generated SQL is executed as a
sequence of statements through the Flink SQL Gateway:

1. `SET` statements (parallelism, checkpointing, state backend, etc.)
2. `CREATE TABLE` for source and sink tables (Kafka connectors)
3. `INSERT INTO` to start the streaming pipeline

The SQL hash is recorded to local state (`.streamt/flink_hashes.json`) for
future change detection.

### 2. SQL Changed (existing RUNNING job)

streamt detects SQL changes by comparing a SHA-256 hash of the full generated
SQL against the hash recorded during the previous apply. The generated SQL
includes `SET` statements, so **any** configuration change — SQL logic,
parallelism, checkpointing, state backend, state TTL — produces a different
hash.

When a change is detected:

1. The running job is **cancelled** via the Flink REST API (`PATCH /jobs/{id}` with state `cancelled`).
2. The new SQL is submitted through the SQL Gateway.
3. The new hash is saved to local state.

**There is no savepoint taken before cancellation.** The job is hard-cancelled
and restarted from scratch (or from the latest externalized checkpoint, if
configured). If you need to preserve state across redeployments, configure
externalized checkpoint retention:

```yaml
flink:
  checkpoint:
    externalized: RETAIN_ON_CANCELLATION
```

With this setting, Flink retains checkpoints after cancellation, and the new
job can resume from the latest checkpoint automatically (depending on your Flink
cluster configuration).

### 3. Config Changed (parallelism, checkpointing, etc.)

Config-only changes are **not treated differently** from SQL logic changes.
Because all Flink options are compiled into `SET` statements within the
generated SQL, changing any option (parallelism, checkpoint interval, state
backend, etc.) changes the SQL hash. This triggers the same cancel + resubmit
path described above.

Examples of changes that trigger redeployment:

- `parallelism: 4` to `parallelism: 8`
- `checkpoint_interval_ms: 60000` to `checkpoint_interval_ms: 30000`
- `state_backend: hashmap` to `state_backend: rocksdb`
- Any modification to the SQL query itself

### 4. Job Failed or Stopped

If the job exists on the cluster but is not in `RUNNING` or `CREATED` state
(e.g., `FAILED`, `CANCELED`, `FINISHED`), streamt treats it as a fresh
submission. The job is submitted without attempting to cancel it first.

This means `streamt apply` is self-healing: if a job crashes, re-running apply
will restart it.

### 5. No Changes Detected

If the job is `RUNNING` and the SQL hash matches the previously recorded hash,
the job is left untouched. The plan shows `action: none` and apply reports it
as `unchanged`.

!!! note "First Apply After Import"
    On the very first apply (or if the local state file is missing), there is no
    prior hash to compare against. In this case, a running job is reported as
    `unchanged` and left alone. The hash is recorded for future comparisons. To
    force redeployment, delete `.streamt/flink_hashes.json` and stop the job
    manually, or change the SQL.

### 6. Rollback on Failure

If the new SQL fails to submit after the running job was cancelled:

1. The SQL hash for that job is **cleared** from local state.
2. A `CRITICAL` log message is emitted: `PIPELINE DOWN: job '<name>' was cancelled but resubmit failed.`
3. The error is re-raised, causing `apply` to report the job under `errors`.

**The old job is not automatically restored.** The pipeline is down until the
issue is fixed and `streamt apply` is run again. On the next apply, the missing
hash and non-running state will cause the job to be submitted as new.

To minimize this risk:

- Use `streamt plan` to preview changes before applying.
- Test SQL changes locally with the Flink SQL CLI first.
- Configure `checkpoint.externalized: RETAIN_ON_CANCELLATION` so that even
  after a failed redeploy, manually resubmitting the old SQL can resume from
  the last checkpoint.

!!! warning "No Savepoints"
    streamt does **not** trigger savepoints before cancelling jobs. This is a
    known limitation. If you require zero-data-loss redeployments with state
    continuity, take a savepoint manually via the Flink REST API before running
    `streamt apply`, or configure externalized checkpoint retention.

---

## Current Status

| Category | Status | Notes |
|----------|--------|-------|
| Basic job submission | Supported | Via REST API and SQL Gateway |
| Parallelism | Supported | Per-job configuration |
| Checkpointing | Supported | Full: interval, timeout, mode, externalized, unaligned, incremental |
| State backend | Supported | Type + RocksDB tuning |
| Restart strategy | Supported | fixed-delay, failure-rate, exponential-delay |
| Resources | Supported | TM/JM memory and slots |
| Changelog mode | Supported | append or upsert |
| Custom watermark | Supported | Arbitrary SQL expression |
| MATCH_RECOGNIZE (CEP) | Supported | Complex event processing patterns |

---

## Model Flink Configuration

Configure Flink jobs in your model definitions using the `flink:` and `flink_cluster:` fields:

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
    flink:
      parallelism: 4
      checkpoint_interval_ms: 60000
      state_backend: rocksdb
      state_ttl_ms: 86400000

    flink_cluster: production    # Which cluster to deploy to
```

### Supported Options

All Flink options are nested under `flink:`:

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `parallelism` | int | 1 | Job parallelism (number of parallel tasks) |
| `checkpoint_interval_ms` | int | 60000 | Checkpoint interval in milliseconds |
| `state_ttl_ms` | int | none | State TTL in milliseconds (see [State TTL](#state-ttl)) |
| `state_backend` | string | none | State backend type (`hashmap`, `rocksdb`) |
| `checkpoint` | object | none | Advanced checkpoint config (see [Advanced Checkpointing](#advanced-checkpointing)) |
| `restart_strategy` | object | none | Restart strategy config (see [Restart Strategy](#restart-strategy)) |
| `rocksdb` | object | none | RocksDB tuning (see [RocksDB Tuning](#rocksdb-tuning)) |
| `resources` | object | none | TM/JM resource config (see [Resource Configuration](#resource-configuration)) |
| `changelog_mode` | string | append | Changelog mode: `append` or `upsert` |

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

### State Backend

The `state_backend` option is applied as `SET 'state.backend'` in the generated Flink SQL.

```yaml
flink:
  state_backend: rocksdb  # generates: SET 'state.backend' = 'rocksdb'
```

| Value | Description |
|-------|-------------|
| `hashmap` | In-memory state backend (default Flink behavior) |
| `rocksdb` | RocksDB-based state backend for large state; combine with `rocksdb` tuning options |

### Advanced Checkpointing

```yaml
flink:
  checkpoint_interval_ms: 60000
  checkpoint:
    timeout_ms: 120000
    min_pause_ms: 500
    max_concurrent: 1
    mode: EXACTLY_ONCE
    externalized: RETAIN_ON_CANCELLATION
    unaligned: true
    incremental: true
```

| Field | Type | SET statement | Description |
|-------|------|---------------|-------------|
| `timeout_ms` | int | `execution.checkpointing.timeout` | Checkpoint timeout |
| `min_pause_ms` | int | `execution.checkpointing.min-pause` | Min pause between checkpoints |
| `max_concurrent` | int | `execution.checkpointing.max-concurrent-checkpoints` | Max concurrent checkpoints |
| `mode` | string | `execution.checkpointing.mode` | `EXACTLY_ONCE` or `AT_LEAST_ONCE` |
| `externalized` | string | `execution.checkpointing.externalized-checkpoint-retention` | `RETAIN_ON_CANCELLATION` or `DELETE_ON_CANCELLATION` |
| `unaligned` | bool | `execution.checkpointing.unaligned.enabled` | Enable unaligned checkpoints |
| `incremental` | bool | `execution.checkpointing.incremental` | Enable incremental checkpoints (RocksDB) |

### Restart Strategy

Three restart strategy types are supported via `flink.restart_strategy`:

**fixed-delay** — restart a fixed number of times with a delay between attempts:

```yaml
flink:
  restart_strategy:
    type: fixed-delay
    attempts: 3
    delay_ms: 10000
```

| SET statement | Value |
|---------------|-------|
| `restart-strategy` | `fixed-delay` |
| `restart-strategy.fixed-delay.attempts` | `attempts` |
| `restart-strategy.fixed-delay.delay` | `delay_ms` (converted to duration) |

**failure-rate** — restart as long as failure rate stays below threshold:

```yaml
flink:
  restart_strategy:
    type: failure-rate
    max_failures_per_interval: 3
    failure_rate_interval_ms: 300000
    delay_ms: 10000
```

**exponential-delay** — restart with exponentially increasing delay:

```yaml
flink:
  restart_strategy:
    type: exponential-delay
    initial_backoff_ms: 1000
    max_backoff_ms: 60000
    backoff_multiplier: 2.0
```

### RocksDB Tuning

When using `state_backend: rocksdb`, tune RocksDB performance:

```yaml
flink:
  state_backend: rocksdb
  rocksdb:
    block_cache_size_mb: 256
    write_buffer_size_mb: 64
    predefined_options: FLASH_SSD_OPTIMIZED
```

| Field | SET statement | Description |
|-------|---------------|-------------|
| `block_cache_size_mb` | `state.backend.rocksdb.block.cache-size` | Block cache size |
| `write_buffer_size_mb` | `state.backend.rocksdb.writebuffer.size` | Write buffer size |
| `predefined_options` | `state.backend.rocksdb.predefined-options` | `DEFAULT`, `SPINNING_DISK_OPTIMIZED`, `SPINNING_DISK_OPTIMIZED_HIGH_MEM`, `FLASH_SSD_OPTIMIZED` |

### Resource Configuration

Configure TaskManager and JobManager resources:

```yaml
flink:
  resources:
    taskmanager_memory_mb: 4096
    taskmanager_slots: 2
    jobmanager_memory_mb: 2048
```

| Field | SET statement | Description |
|-------|---------------|-------------|
| `taskmanager_memory_mb` | `taskmanager.memory.process.size` | TM process memory |
| `taskmanager_slots` | `taskmanager.numberOfTaskSlots` | Slots per TM |
| `jobmanager_memory_mb` | `jobmanager.memory.process.size` | JM process memory |

### Changelog Mode

Controls the connector type for sink tables:

```yaml
flink:
  changelog_mode: upsert  # switches to upsert-kafka connector
```

| Value | Connector | Use case |
|-------|-----------|----------|
| `append` | `kafka` | Append-only streams (default) |
| `upsert` | `upsert-kafka` | Tables with retractions/updates (requires key) |

---

### Watermark Strategy

```yaml
# SUPPORTED - Use in sources or models
sources:
  - name: events
    topic: events.raw.v1

    event_time:
      column: event_timestamp
      watermark:
        strategy: bounded_out_of_orderness
        max_out_of_orderness_ms: 5000
```

**Custom watermark expression:**

```yaml
event_time:
  column: event_ts
  watermark:
    strategy: custom
    expression: "CASE WHEN `event_ts` > CURRENT_TIMESTAMP THEN CURRENT_TIMESTAMP ELSE `event_ts` END"
```

| Option | Location | Type | Description |
|--------|----------|------|-------------|
| `event_time.column` | Top-level | string | Event time column |
| `event_time.watermark.strategy` | Advanced | string | `bounded_out_of_orderness`, `monotonous`, or `custom` |
| `event_time.watermark.max_out_of_orderness_ms` | Advanced | int | Max out-of-orderness for bounded strategy |
| `event_time.watermark.expression` | Advanced | string | SQL expression for `custom` strategy |

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

### Example: Windowed Aggregation with Parallelism

**Input YAML:**
```yaml
models:
  - name: hourly_revenue
    sql: |
      SELECT window_start, window_end, SUM(amount) as revenue
      FROM TABLE(TUMBLE(TABLE {{ ref("orders_valid") }}, DESCRIPTOR(order_time), INTERVAL '1' HOUR))
      GROUP BY window_start, window_end
    flink:
      parallelism: 4
```

**Generated Flink SQL:**
```sql
SET 'parallelism.default' = '4';

CREATE TABLE orders_valid ( ... ) WITH ('connector' = 'kafka', ...);
CREATE TABLE hourly_revenue_sink ( ... ) WITH ('connector' = 'kafka', ...);

INSERT INTO hourly_revenue_sink
SELECT window_start, window_end, SUM(amount) as revenue
FROM TABLE(TUMBLE(TABLE orders_valid, DESCRIPTOR(order_time), INTERVAL '1' HOUR))
GROUP BY window_start, window_end;
```

Use `streamt plan --show-sql` to see the full generated SQL for any model.

---

## Troubleshooting

### Common Issues

| Issue | Cause | Solution |
|-------|-------|----------|
| Job fails to start | SQL Gateway not running | Ensure `sql_gateway_url` is correct |
| State grows unbounded | No TTL configured | Add `state_ttl_ms` in `flink:` |
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
