---
title: "Tutorial: Windowed Aggregations"
description: Build real-time metrics with tumbling and hopping windows
---

# Tutorial: Windowed Aggregations

Learn to build real-time metrics using time-based windows. By the end, you'll have a pipeline that computes hourly order statistics with proper event-time handling.

**Prerequisites:**

- [Streaming Fundamentals](../concepts/streaming-fundamentals.md) (understand event time, watermarks, windows)
- streamt installed and Docker running

**What you'll build:**

```
orders_raw → orders_clean → hourly_order_stats → order_trends_15min
   (source)     (filter)     (tumbling window)    (hopping window)
```

---

## Step 1: Set Up the Project

Create a new project directory:

```bash
mkdir windowed-metrics && cd windowed-metrics
```

Create `stream_project.yml`:

```yaml
project:
  name: windowed-metrics
  version: "1.0.0"

runtime:
  kafka:
    bootstrap_servers: localhost:9092
  schema_registry:
    url: http://localhost:8081
  flink:
    default: local
    clusters:
      local:
        rest_url: http://localhost:8082
        sql_gateway_url: http://localhost:8084

sources:
  - name: orders_raw
    description: Raw order events from the order service
    topic: orders.raw.v1

    # Define the event time field - critical for correct windowing
    event_time:
      column: order_timestamp
      watermark:
        strategy: bounded_out_of_orderness
        max_out_of_orderness_ms: 5000  # Allow 5 seconds late

    schema:
      format: json

    columns:
      - name: order_id
        type: STRING
        description: Unique order identifier
      - name: customer_id
        type: STRING
        description: Customer who placed the order
      - name: amount
        type: DECIMAL(10,2)
        description: Order total in USD
      - name: status
        type: STRING
        description: Order status (pending, confirmed, shipped, delivered)
      - name: region
        type: STRING
        description: Geographic region
      - name: order_timestamp
        type: TIMESTAMP(3)
        description: When the order was placed
```

---

## Step 2: Clean the Data

Before aggregating, filter out invalid orders. Create `models/orders_clean.yml`:

```yaml
models:
  - name: orders_clean
    description: Validated orders with null checks
    # Auto-inferred as flink (simple SELECT defaults to topic, but can override)

    sql: |
      SELECT
        order_id,
        customer_id,
        amount,
        status,
        region,
        order_timestamp
      FROM {{ source("orders_raw") }}
      WHERE order_id IS NOT NULL
        AND amount > 0
        AND status IN ('pending', 'confirmed', 'shipped', 'delivered')

    advanced:  # Optional: override defaults
      topic:
        name: orders.clean.v1
        partitions: 6
```

This creates a cleaned stream that downstream models can rely on.

---

## Step 3: Tumbling Window - Hourly Stats

Now the interesting part: aggregate orders into hourly buckets. Create `models/hourly_order_stats.yml`:

```yaml
models:
  - name: hourly_order_stats
    description: |
      Hourly order statistics by region.
      Uses a tumbling window to compute non-overlapping hourly aggregates.
    # Auto-inferred as flink due to TUMBLE window function

    sql: |
      SELECT
        region,
        TUMBLE_START(order_timestamp, INTERVAL '1' HOUR) AS window_start,
        TUMBLE_END(order_timestamp, INTERVAL '1' HOUR) AS window_end,
        COUNT(*) AS order_count,
        SUM(amount) AS total_revenue,
        AVG(amount) AS avg_order_value,
        MIN(amount) AS min_order,
        MAX(amount) AS max_order
      FROM {{ ref("orders_clean") }}
      GROUP BY
        region,
        TUMBLE(order_timestamp, INTERVAL '1' HOUR)

    advanced:  # Optional: tune Flink behavior
      topic:
        name: orders.stats.hourly.v1
        partitions: 3
      flink:
        parallelism: 4
        checkpoint_interval_ms: 60000  # Checkpoint every minute
```

**How it works:**

1. `TUMBLE(order_timestamp, INTERVAL '1' HOUR)` creates 1-hour non-overlapping windows
2. Orders are grouped by region AND window
3. When the watermark passes the window end, results are emitted
4. Each order belongs to exactly one window

**Example output:**

| region | window_start | window_end | order_count | total_revenue |
|--------|--------------|------------|-------------|---------------|
| US-EAST | 2025-01-15 10:00:00 | 2025-01-15 11:00:00 | 1,234 | 98,765.43 |
| US-WEST | 2025-01-15 10:00:00 | 2025-01-15 11:00:00 | 987 | 76,543.21 |

---

## Step 4: Hopping Window - Trend Detection

For smoother trend lines, use overlapping windows. Create `models/order_trends_15min.yml`:

```yaml
models:
  - name: order_trends_15min
    description: |
      Rolling order trends updated every 15 minutes.
      Uses a hopping window: 1-hour windows sliding every 15 minutes.
      Good for dashboards that need smooth trend lines.
    # Auto-inferred as flink due to HOP window function

    sql: |
      SELECT
        region,
        HOP_START(order_timestamp, INTERVAL '15' MINUTE, INTERVAL '1' HOUR) AS window_start,
        HOP_END(order_timestamp, INTERVAL '15' MINUTE, INTERVAL '1' HOUR) AS window_end,
        COUNT(*) AS order_count,
        SUM(amount) AS total_revenue,
        -- Calculate order velocity (orders per minute)
        COUNT(*) / 60.0 AS orders_per_minute
      FROM {{ ref("orders_clean") }}
      GROUP BY
        region,
        HOP(order_timestamp, INTERVAL '15' MINUTE, INTERVAL '1' HOUR)

    advanced:  # Optional: tune for larger state
      topic:
        name: orders.trends.15min.v1
        partitions: 3
      flink:
        parallelism: 4
        state_ttl_ms: 7200000  # 2 hours - keep state for trend calculation
```

**How it works:**

1. `HOP(..., INTERVAL '15' MINUTE, INTERVAL '1' HOUR)` creates:
   - 1-hour windows
   - Sliding every 15 minutes
   - Each order appears in 4 windows (60min / 15min = 4)
2. Results update every 15 minutes with a 1-hour lookback

**Window overlap visualization:**

```
Time:     10:00   10:15   10:30   10:45   11:00   11:15
          |----Window 1 (10:00-11:00)----|
                  |----Window 2 (10:15-11:15)----|
                          |----Window 3 (10:30-11:30)----|
                                  |----Window 4 (10:45-11:45)----|
```

---

## Step 5: Add Data Quality Tests

Ensure your aggregations produce valid results. Create `tests/stats_tests.yml`:

```yaml
tests:
  - name: hourly_stats_quality
    description: Validate hourly stats output
    model: hourly_order_stats
    type: sample
    sample_size: 100

    assertions:
      - not_null:
          columns: [region, window_start, order_count]
      - positive:
          column: order_count
      - positive:
          column: total_revenue
      - range:
          column: avg_order_value
          min: 0
          max: 100000  # Sanity check
```

---

## Step 6: Deploy and Monitor

Validate your configuration:

```bash
streamt validate
```

Expected output:

```
✓ Project 'windowed-metrics' is valid

  Sources:   1
  Models:    3
  Tests:     1
  Exposures: 0
```

View the lineage:

```bash
streamt lineage
```

```
orders_raw (source)
    └── orders_clean (flink)
            ├── hourly_order_stats (flink)
            └── order_trends_15min (flink)
```

Deploy:

```bash
streamt apply
```

Check status:

```bash
streamt status --lag
```

---

## Understanding the Results

### Why might results be delayed?

Results only emit when the watermark passes the window end:

```
Window: 10:00 - 11:00
Watermark delay: 5 seconds
Last event seen: 11:00:03 (event_time)

Watermark = 11:00:03 - 5s = 10:59:58
Window end = 11:00:00

Watermark < Window end → Window still open!

Next event: 11:00:10 (event_time)
Watermark = 11:00:10 - 5s = 11:00:05

Watermark > Window end → Window closes, results emitted!
```

### Why might counts be lower than expected?

Late events (arriving after the watermark passed) are dropped by default:

```yaml
# To accept late events, configure allowed_lateness (coming soon)
event_time:
  column: order_timestamp
  watermark:
    max_out_of_orderness_ms: 5000
  allowed_lateness_ms: 60000  # Accept up to 1 minute late
```

---

## Common Patterns

### Pattern 1: Multiple Time Granularities

```yaml
# Hourly
- name: stats_hourly
  sql: |
    SELECT ...
    FROM {{ ref("orders_clean") }}
    GROUP BY TUMBLE(order_timestamp, INTERVAL '1' HOUR)

# Daily (aggregates hourly)
- name: stats_daily
  sql: |
    SELECT
      region,
      CAST(window_start AS DATE) AS day,
      SUM(order_count) AS order_count,
      SUM(total_revenue) AS total_revenue
    FROM {{ ref("stats_hourly") }}
    GROUP BY region, CAST(window_start AS DATE)
```

### Pattern 2: Late Data Handling

If you can't afford to drop late data:

1. Increase watermark delay (trades latency for completeness)
2. Use allowed_lateness (emits updates when late data arrives)
3. Reprocess from Kafka when you detect gaps

### Pattern 3: Dimension Enrichment

Join windowed aggregates with dimension tables:

```sql
SELECT
  s.region,
  r.region_name,
  r.timezone,
  s.window_start,
  s.order_count
FROM {{ ref("hourly_order_stats") }} s
JOIN {{ ref("regions") }} FOR SYSTEM_TIME AS OF s.window_start AS r
  ON s.region = r.region_code
```

---

## Troubleshooting

### "Results never appear"

1. Check if events are flowing: `streamt status --lag`
2. Check watermark progress in Flink UI
3. Ensure event_time column has valid timestamps

### "Some events missing from aggregates"

1. Events arriving after watermark are dropped
2. Increase `max_out_of_orderness_ms`
3. Check producer clock skew

### "Job runs out of memory"

1. Add `state_ttl_ms` for unbounded aggregations
2. Reduce parallelism if over-provisioned
3. Use RocksDB state backend for large state

---

## Next Steps

- [Stream Joins Tutorial](./stream-joins.md) - Join orders with customers
- [CDC Pipelines Tutorial](./cdc-pipelines.md) - Capture database changes
- [Flink Options Reference](../reference/flink-options.md) - Tune performance
