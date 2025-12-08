---
title: "Tutorial: CDC Pipelines"
description: Capture database changes and stream them through Flink
---

# Tutorial: CDC Pipelines

Learn to capture changes from databases and process them in real-time. By the end, you'll have a complete Change Data Capture (CDC) pipeline from PostgreSQL to analytics.

**Prerequisites:**

- [Stream Joins Tutorial](./stream-joins.md) (understand temporal joins)
- Docker with PostgreSQL and Debezium

**What you'll build:**

```
PostgreSQL ──→ Debezium ──→ Kafka ──→ Flink ──→ Analytics
  (source)     (CDC)       (topics)   (transform)  (output)

Tables:
  customers ──→ customers.cdc ──→ customer_360
  products  ──→ products.cdc  ──→ product_analytics
  orders    ──→ orders.cdc    ──┬→ order_enriched
                                └→ real_time_revenue
```

---

## What is CDC?

**Change Data Capture** streams database changes as events:

```
Database:                    Kafka Topic:
┌─────────────┐              ┌─────────────────────────┐
│ UPDATE      │    CDC       │ {"op": "u",             │
│ customers   │ ──────────→  │  "before": {...},       │
│ SET tier=   │              │  "after": {"tier":"gold"│
│   'gold'    │              │  }}                     │
└─────────────┘              └─────────────────────────┘
```

Every INSERT, UPDATE, DELETE becomes an event you can process in real-time.

### Why CDC?

| Traditional ETL | CDC |
|-----------------|-----|
| Batch (hourly/daily) | Real-time (seconds) |
| Full table scans | Only changed rows |
| Heavy database load | Reads transaction log |
| Stale analytics | Fresh analytics |

---

## Step 1: Set Up the Database

First, configure PostgreSQL for logical replication. Add to `postgresql.conf`:

```ini
wal_level = logical
max_replication_slots = 4
max_wal_senders = 4
```

Create the example tables:

```sql
-- Create tables
CREATE TABLE customers (
    id VARCHAR(50) PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(100) NOT NULL,
    tier VARCHAR(20) DEFAULT 'bronze',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE products (
    id VARCHAR(50) PRIMARY KEY,
    name VARCHAR(200) NOT NULL,
    category VARCHAR(50),
    price DECIMAL(10, 2),
    inventory_count INT DEFAULT 0,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE orders (
    id VARCHAR(50) PRIMARY KEY,
    customer_id VARCHAR(50) REFERENCES customers(id),
    product_id VARCHAR(50) REFERENCES products(id),
    quantity INT NOT NULL,
    total_amount DECIMAL(10, 2),
    status VARCHAR(20) DEFAULT 'pending',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create publication for Debezium
CREATE PUBLICATION dbz_publication FOR ALL TABLES;
```

---

## Step 2: Configure Debezium Connector

Create a Debezium PostgreSQL connector. Save as `connectors/postgres-source.json`:

```json
{
  "name": "postgres-source",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "database.hostname": "postgres",
    "database.port": "5432",
    "database.user": "debezium",
    "database.password": "${POSTGRES_PASSWORD}",
    "database.dbname": "analytics",
    "database.server.name": "dbserver1",
    "topic.prefix": "cdc",
    "table.include.list": "public.customers,public.products,public.orders",
    "publication.name": "dbz_publication",
    "slot.name": "debezium_slot",
    "plugin.name": "pgoutput",
    "transforms": "unwrap",
    "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
    "transforms.unwrap.drop.tombstones": "false",
    "transforms.unwrap.delete.handling.mode": "rewrite",
    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter"
  }
}
```

Deploy via Kafka Connect:

```bash
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @connectors/postgres-source.json
```

This creates topics:
- `cdc.public.customers`
- `cdc.public.products`
- `cdc.public.orders`

---

## Step 3: Create the streamt Project

Create `stream_project.yml`:

```yaml
project:
  name: cdc-analytics
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
  connect:
    default: local
    clusters:
      local:
        rest_url: http://localhost:8083

sources:
  # Customer CDC stream
  - name: customers_cdc
    description: |
      Customer changes from PostgreSQL via Debezium.
      Each message represents an INSERT, UPDATE, or DELETE.
    topic: cdc.public.customers

    event_time:
      column: updated_at
      watermark:
        max_out_of_orderness_ms: 1000  # CDC is usually low-latency

    columns:
      - name: id
        type: STRING
        description: Customer primary key
      - name: name
        type: STRING
      - name: email
        type: STRING
      - name: tier
        type: STRING
        description: bronze, silver, gold, platinum
      - name: created_at
        type: TIMESTAMP(3)
      - name: updated_at
        type: TIMESTAMP(3)
      - name: __deleted
        type: BOOLEAN
        description: Debezium delete marker (true if row was deleted)

  # Product CDC stream
  - name: products_cdc
    description: Product catalog changes
    topic: cdc.public.products

    event_time:
      column: updated_at
      watermark:
        max_out_of_orderness_ms: 1000

    columns:
      - name: id
        type: STRING
      - name: name
        type: STRING
      - name: category
        type: STRING
      - name: price
        type: DECIMAL(10, 2)
      - name: inventory_count
        type: INT
      - name: updated_at
        type: TIMESTAMP(3)
      - name: __deleted
        type: BOOLEAN

  # Order CDC stream
  - name: orders_cdc
    description: Order changes (new orders, status updates)
    topic: cdc.public.orders

    event_time:
      column: updated_at
      watermark:
        max_out_of_orderness_ms: 2000  # Orders may have slight delay

    columns:
      - name: id
        type: STRING
      - name: customer_id
        type: STRING
      - name: product_id
        type: STRING
      - name: quantity
        type: INT
      - name: total_amount
        type: DECIMAL(10, 2)
      - name: status
        type: STRING
      - name: created_at
        type: TIMESTAMP(3)
      - name: updated_at
        type: TIMESTAMP(3)
      - name: __deleted
        type: BOOLEAN
```

---

## Step 4: Build a Customer 360 View

Create a denormalized customer view that updates in real-time. Create `models/customer_360.yml`:

```yaml
models:
  - name: customer_360
    description: |
      Real-time customer 360 view.
      Combines customer profile with aggregated order history.
      Updates whenever customer data or orders change.
    materialized: flink

    topic:
      name: analytics.customer360.v1
      partitions: 6

    flink:
      parallelism: 4
      state_ttl_ms: 604800000  # 7 days - keep customer state for a week

    sql: |
      SELECT
        c.id AS customer_id,
        c.name,
        c.email,
        c.tier,
        c.created_at AS customer_since,
        -- Aggregate order history
        COUNT(o.id) AS total_orders,
        COALESCE(SUM(o.total_amount), 0) AS lifetime_value,
        COALESCE(AVG(o.total_amount), 0) AS avg_order_value,
        MAX(o.created_at) AS last_order_at,
        -- Calculate days since last order
        TIMESTAMPDIFF(
          DAY,
          MAX(o.created_at),
          CURRENT_TIMESTAMP
        ) AS days_since_last_order
      FROM {{ source("customers_cdc") }} AS c
      LEFT JOIN {{ source("orders_cdc") }} AS o
        ON c.id = o.customer_id
        AND o.__deleted = FALSE
      WHERE c.__deleted = FALSE
      GROUP BY
        c.id, c.name, c.email, c.tier, c.created_at
```

**How it works:**

1. Each customer change triggers an update
2. Orders are aggregated per customer
3. Deleted records are filtered out (`__deleted = FALSE`)
4. Result is a continuously updating view

---

## Step 5: Build Real-Time Revenue Dashboard

Create `models/real_time_revenue.yml`:

```yaml
models:
  - name: real_time_revenue
    description: |
      Real-time revenue metrics updated every minute.
      Powers the executive dashboard.
    materialized: flink

    topic:
      name: analytics.revenue.realtime.v1
      partitions: 1  # Single partition for ordered revenue timeline

    flink:
      parallelism: 2

    sql: |
      SELECT
        TUMBLE_START(o.updated_at, INTERVAL '1' MINUTE) AS minute,
        COUNT(DISTINCT o.id) AS order_count,
        COUNT(DISTINCT o.customer_id) AS unique_customers,
        SUM(o.total_amount) AS revenue,
        AVG(o.total_amount) AS avg_order_value,
        -- Revenue by status
        SUM(CASE WHEN o.status = 'completed' THEN o.total_amount ELSE 0 END) AS completed_revenue,
        SUM(CASE WHEN o.status = 'pending' THEN o.total_amount ELSE 0 END) AS pending_revenue,
        SUM(CASE WHEN o.status = 'cancelled' THEN o.total_amount ELSE 0 END) AS cancelled_revenue
      FROM {{ source("orders_cdc") }} AS o
      WHERE o.__deleted = FALSE
      GROUP BY TUMBLE(o.updated_at, INTERVAL '1' MINUTE)
```

---

## Step 6: Enriched Orders with Product Details

Create `models/orders_enriched.yml`:

```yaml
models:
  - name: orders_enriched
    description: |
      Orders enriched with customer and product details.
      Uses temporal joins to get the correct version at order time.
    materialized: flink

    topic:
      name: analytics.orders.enriched.v1
      partitions: 6

    flink:
      parallelism: 4
      state_ttl_ms: 86400000  # 24 hours

    sql: |
      SELECT
        o.id AS order_id,
        o.customer_id,
        o.product_id,
        o.quantity,
        o.total_amount,
        o.status,
        o.created_at AS order_timestamp,
        -- Customer details at order time
        c.name AS customer_name,
        c.tier AS customer_tier,
        -- Product details at order time
        p.name AS product_name,
        p.category AS product_category,
        p.price AS unit_price,
        -- Calculated fields
        o.quantity * p.price AS expected_amount,
        o.total_amount - (o.quantity * p.price) AS discount_amount
      FROM {{ source("orders_cdc") }} AS o
      LEFT JOIN {{ source("customers_cdc") }}
        FOR SYSTEM_TIME AS OF o.updated_at AS c
        ON o.customer_id = c.id
      LEFT JOIN {{ source("products_cdc") }}
        FOR SYSTEM_TIME AS OF o.updated_at AS p
        ON o.product_id = p.id
      WHERE o.__deleted = FALSE
```

---

## Step 7: Product Analytics

Track product performance in real-time. Create `models/product_analytics.yml`:

```yaml
models:
  - name: product_analytics
    description: |
      Real-time product performance metrics.
      Updated with each order.
    materialized: flink

    topic:
      name: analytics.products.performance.v1
      partitions: 3

    flink:
      parallelism: 2
      state_ttl_ms: 604800000  # 7 days

    sql: |
      SELECT
        p.id AS product_id,
        p.name AS product_name,
        p.category,
        p.price AS current_price,
        p.inventory_count AS current_inventory,
        -- Sales metrics
        COUNT(o.id) AS total_orders,
        SUM(o.quantity) AS total_units_sold,
        SUM(o.total_amount) AS total_revenue,
        AVG(o.total_amount) AS avg_order_value,
        -- Inventory velocity (units sold per day)
        SUM(o.quantity) / GREATEST(
          TIMESTAMPDIFF(DAY, MIN(o.created_at), CURRENT_TIMESTAMP),
          1
        ) AS daily_velocity,
        -- Days of inventory remaining
        CASE
          WHEN SUM(o.quantity) > 0 THEN
            p.inventory_count / (
              SUM(o.quantity) / GREATEST(
                TIMESTAMPDIFF(DAY, MIN(o.created_at), CURRENT_TIMESTAMP),
                1
              )
            )
          ELSE NULL
        END AS days_of_inventory
      FROM {{ source("products_cdc") }} AS p
      LEFT JOIN {{ source("orders_cdc") }} AS o
        ON p.id = o.product_id
        AND o.__deleted = FALSE
        AND o.status != 'cancelled'
      WHERE p.__deleted = FALSE
      GROUP BY
        p.id, p.name, p.category, p.price, p.inventory_count
```

---

## Step 8: Sink to Data Warehouse

Export the analytics to Snowflake for BI tools. Create `models/revenue_to_snowflake.yml`:

```yaml
models:
  - name: revenue_to_snowflake
    description: Export real-time revenue to Snowflake
    materialized: sink

    from:
      - ref: real_time_revenue

    sink:
      connector: snowflake-sink
      config:
        snowflake.url.name: ${SNOWFLAKE_URL}
        snowflake.user.name: ${SNOWFLAKE_USER}
        snowflake.private.key: ${SNOWFLAKE_KEY}
        snowflake.database.name: ANALYTICS
        snowflake.schema.name: REALTIME
        snowflake.topic2table.map: "analytics.revenue.realtime.v1:REVENUE_METRICS"
        buffer.count.records: 1000
        buffer.flush.time: 60
```

---

## Step 9: Add Monitoring

Create `tests/cdc_tests.yml`:

```yaml
tests:
  - name: cdc_freshness
    description: Ensure CDC data is flowing
    model: orders_enriched
    type: sample
    sample_size: 10
    assertions:
      - freshness:
          column: order_timestamp
          max_age_minutes: 5  # Data should be < 5 minutes old

  - name: customer_360_quality
    description: Validate customer 360 view
    model: customer_360
    type: sample
    sample_size: 100
    assertions:
      - not_null:
          columns: [customer_id, name, email]
      - positive:
          column: total_orders
          allow_zero: true
      - range:
          column: lifetime_value
          min: 0
          max: 10000000
```

---

## Handling CDC Edge Cases

### Deletes

Debezium marks deleted rows with `__deleted = TRUE`. Filter them:

```sql
WHERE __deleted = FALSE
```

Or track deletions:

```sql
SELECT
  id,
  CASE WHEN __deleted THEN 'deleted' ELSE 'active' END AS status
FROM {{ source("customers_cdc") }}
```

### Schema Evolution

When the database schema changes:

1. Debezium captures the new schema
2. Add new columns to your source definition
3. Handle missing columns with `COALESCE`:

```sql
COALESCE(new_column, 'default_value') AS new_column
```

### Out-of-Order Updates

CDC events are ordered per row, but not globally. Use event time:

```yaml
event_time:
  column: updated_at
  watermark:
    max_out_of_orderness_ms: 5000
```

### Initial Snapshot

Debezium takes an initial snapshot of existing data. Handle with:

```sql
-- Distinguish snapshot from live changes
CASE
  WHEN source_timestamp IS NULL THEN 'snapshot'
  ELSE 'live'
END AS record_type
```

---

## Complete Lineage

```bash
streamt lineage
```

```
customers_cdc (source)
    ├── customer_360 (flink)
    └── orders_enriched (flink)

products_cdc (source)
    ├── product_analytics (flink)
    └── orders_enriched (flink)

orders_cdc (source)
    ├── customer_360 (flink)
    ├── product_analytics (flink)
    ├── orders_enriched (flink)
    └── real_time_revenue (flink)
            └── revenue_to_snowflake (sink)
```

---

## Troubleshooting

### "No CDC events appearing"

1. Check Debezium connector status: `curl localhost:8083/connectors/postgres-source/status`
2. Verify PostgreSQL WAL level: `SHOW wal_level;` → should be `logical`
3. Check replication slot: `SELECT * FROM pg_replication_slots;`

### "Temporal join returns NULL"

1. CDC stream may not have the record yet
2. State TTL may have expired historical versions
3. Check if join key matches exactly

### "Duplicate records in output"

1. CDC may replay during connector restart
2. Add deduplication window:
   ```sql
   SELECT DISTINCT ON (id, updated_at) *
   ```
3. Use upsert mode in sink

### "State growing too large"

1. Add `state_ttl_ms` to all models
2. Reduce join window sizes
3. Filter unnecessary columns early

---

## Next Steps

- [Windowed Aggregations](./windowed-aggregations.md) - Add time-based analytics
- [Stream Joins](./stream-joins.md) - More join patterns
- [Flink Options Reference](../reference/flink-options.md) - Tune checkpoints and state
