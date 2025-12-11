---
title: "Tutorial: Stream Joins"
description: Join streaming data with temporal and interval joins
---

# Tutorial: Stream Joins

Learn to join streams together and enrich streaming data with dimension tables. By the end, you'll understand the different join types and when to use each.

**Prerequisites:**

- [Streaming Fundamentals](../concepts/streaming-fundamentals.md) (understand state, event time)
- [Windowed Aggregations Tutorial](./windowed-aggregations.md)

**What you'll build:**

```
orders_raw ──────┬─────→ orders_enriched ───→ order_payment_matched
                 │            (temporal join)       (interval join)
customers_cdc ───┘                │
                                  │
payments_raw ─────────────────────┘
```

---

## The Challenge of Streaming Joins

In batch SQL, joins are straightforward:

```sql
SELECT o.*, c.name
FROM orders o
JOIN customers c ON o.customer_id = c.id
```

In streaming, this is problematic:

1. **Orders never stop arriving** - you can't wait for all of them
2. **Customers change over time** - which version do you join with?
3. **State grows unbounded** - keeping all unmatched events forever

Streaming joins solve this with **time bounds** and **versioned tables**.

---

## Step 1: Project Setup

Create `stream_project.yml`:

```yaml
project:
  name: stream-joins-demo
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
  # Orders stream
  - name: orders_raw
    description: Order events from the order service
    topic: orders.raw.v1
    event_time:
      column: order_timestamp
      watermark:
        max_out_of_orderness_ms: 5000
    columns:
      - name: order_id
        type: STRING
      - name: customer_id
        type: STRING
      - name: amount
        type: DECIMAL(10,2)
      - name: order_timestamp
        type: TIMESTAMP(3)

  # Payments stream
  - name: payments_raw
    description: Payment events from the payment processor
    topic: payments.raw.v1
    event_time:
      column: payment_timestamp
      watermark:
        max_out_of_orderness_ms: 10000  # Payments can be more delayed
    columns:
      - name: payment_id
        type: STRING
      - name: order_id
        type: STRING
      - name: payment_method
        type: STRING
      - name: payment_status
        type: STRING
      - name: payment_timestamp
        type: TIMESTAMP(3)

  # Customer CDC stream (changes from the customer database)
  - name: customers_cdc
    description: Customer changes via CDC (Debezium format)
    topic: customers.cdc.v1
    event_time:
      column: updated_at
      watermark:
        max_out_of_orderness_ms: 1000
    columns:
      - name: id
        type: STRING
      - name: name
        type: STRING
      - name: email
        type: STRING
      - name: tier
        type: STRING
        description: Customer tier (bronze, silver, gold, platinum)
      - name: updated_at
        type: TIMESTAMP(3)
```

---

## Step 2: Temporal Join - Enrich Orders with Customer Data

A **temporal join** looks up the customer state **as of the order time**. This ensures you get the customer's tier at the moment they placed the order, not their current tier.

Create `models/orders_enriched.yml`:

```yaml
models:
  - name: orders_enriched
    description: |
      Orders enriched with customer information.
      Uses a temporal join to get customer state as of order time.

      Important: This join uses the customer's state at order_timestamp,
      not their current state. If a customer upgrades from Silver to Gold
      after placing an order, that order still shows Silver.
    # Auto-inferred as flink due to temporal JOIN (FOR SYSTEM_TIME AS OF)

    sql: |
      SELECT
        o.order_id,
        o.customer_id,
        o.amount,
        o.order_timestamp,
        c.name AS customer_name,
        c.email AS customer_email,
        c.tier AS customer_tier,
        -- Derive discount based on tier at order time
        CASE c.tier
          WHEN 'platinum' THEN 0.20
          WHEN 'gold' THEN 0.15
          WHEN 'silver' THEN 0.10
          ELSE 0.0
        END AS tier_discount
      FROM {{ source("orders_raw") }} AS o
      LEFT JOIN {{ source("customers_cdc") }} FOR SYSTEM_TIME AS OF o.order_timestamp AS c
        ON o.customer_id = c.id

    advanced:  # Optional: tune state management
      topic:
        name: orders.enriched.v1
        partitions: 6
      flink:
        parallelism: 4
        state_ttl_ms: 86400000  # 24 hours - keep customer versions for a day
```

**How temporal joins work:**

```
Timeline:
─────────────────────────────────────────────────→ time
     │                    │                │
  Customer               Order           Customer
  tier=silver           placed           tier=gold
  (10:00)               (10:30)          (11:00)

Temporal join at 10:30 → tier = 'silver' (correct!)
Regular join           → tier = 'gold'   (wrong!)
```

**Key points:**

- `FOR SYSTEM_TIME AS OF o.order_timestamp` - look up customer as of order time
- The CDC stream must be keyed by the join key (`id`)
- State stores historical versions of customers
- `state_ttl_ms` limits how far back we keep versions

---

## Step 3: Interval Join - Match Orders with Payments

An **interval join** matches events from two streams within a time window. Use this when both sides are unbounded streams.

Create `models/order_payment_matched.yml`:

```yaml
models:
  - name: order_payment_matched
    description: |
      Orders matched with their payments.
      Uses an interval join: payments must occur within 24 hours of the order.

      Orders without payment within 24h are emitted as unmatched (LEFT JOIN).
      This helps identify abandoned orders.
    # Auto-inferred as flink due to interval JOIN with BETWEEN time bounds

    sql: |
      SELECT
        o.order_id,
        o.customer_id,
        o.amount AS order_amount,
        o.order_timestamp,
        p.payment_id,
        p.payment_method,
        p.payment_status,
        p.payment_timestamp,
        -- Calculate time to payment
        TIMESTAMPDIFF(MINUTE, o.order_timestamp, p.payment_timestamp) AS minutes_to_payment,
        -- Flag if payment was received
        CASE WHEN p.payment_id IS NOT NULL THEN TRUE ELSE FALSE END AS is_paid
      FROM {{ ref("orders_enriched") }} AS o
      LEFT JOIN {{ source("payments_raw") }} AS p
        ON o.order_id = p.order_id
        AND p.payment_timestamp BETWEEN o.order_timestamp
                                    AND o.order_timestamp + INTERVAL '24' HOUR

    advanced:  # Optional: tune parallelism
      topic:
        name: orders.payment.matched.v1
        partitions: 6
      flink:
        parallelism: 4
```

**How interval joins work:**

```
Orders:    [O1 10:00] [O2 10:30] [O3 11:00]
Payments:  [P1 10:15] [P2 11:45] [P3 35:00]

Match window: order_time to order_time + 24h

O1 (10:00) matches P1 (10:15) ✓ - within 24h
O2 (10:30) matches P2 (11:45) ✓ - within 24h
O3 (11:00) matches nothing    ✗ - P3 is 24h+ later
```

**Key points:**

- Time bounds are **required** - unbounded joins are dangerous
- Both streams must have event time configured
- State holds events within the join window
- Results emit when a match is found OR when the window expires (for LEFT JOIN)

---

## Step 4: Build an Abandoned Orders Alert

Use the matched data to identify orders that weren't paid. Create `models/abandoned_orders.yml`:

```yaml
models:
  - name: abandoned_orders
    description: |
      Orders that haven't been paid within 1 hour.
      These are candidates for reminder emails or cart recovery.
    # Auto-inferred as topic (simple SELECT with WHERE, no aggregation/window/join)

    sql: |
      SELECT
        order_id,
        customer_id,
        customer_email,
        order_amount,
        customer_tier,
        order_timestamp,
        -- For targeting, high-value customers get priority
        CASE
          WHEN customer_tier IN ('gold', 'platinum') AND order_amount > 100 THEN 'high'
          WHEN order_amount > 500 THEN 'high'
          ELSE 'normal'
        END AS priority
      FROM {{ ref("order_payment_matched") }}
      WHERE is_paid = FALSE
        -- Only alert for orders older than 1 hour
        AND order_timestamp < CURRENT_TIMESTAMP - INTERVAL '1' HOUR

    advanced:  # Optional: configure output topic
      topic:
        name: orders.abandoned.v1
        partitions: 3
```

---

## Step 5: Add Monitoring Tests

Create `tests/join_tests.yml`:

```yaml
tests:
  - name: enriched_orders_quality
    description: Ensure customer enrichment is working
    model: orders_enriched
    type: sample
    sample_size: 100
    assertions:
      - not_null:
          columns: [order_id, customer_id, order_timestamp]
      # Customer name should be present for most orders
      # NULL is acceptable for new customers not yet in CDC
      - custom_sql:
          sql: |
            SELECT COUNT(*) as failures
            FROM {{ model }}
            WHERE customer_name IS NULL
          max_failures: 10  # Allow up to 10% missing

  - name: payment_matching_latency
    description: Monitor payment matching timing
    model: order_payment_matched
    type: sample
    sample_size: 100
    assertions:
      - range:
          column: minutes_to_payment
          min: 0
          max: 1440  # 24 hours max
```

---

## Join Types Reference

### Regular Join (INNER)

```sql
FROM orders o
JOIN payments p ON o.order_id = p.order_id
```

**Warning**: Unbounded state! Both sides buffer all unmatched events forever. Only use with time bounds or TTL.

### Left/Right/Full Outer Join

```sql
FROM orders o
LEFT JOIN payments p ON o.order_id = p.order_id
  AND p.payment_timestamp BETWEEN ...
```

Emits unmatched orders when the time window expires.

### Temporal Join (Versioned Table Lookup)

```sql
FROM orders o
JOIN customers FOR SYSTEM_TIME AS OF o.order_timestamp AS c
  ON o.customer_id = c.id
```

Looks up the table state at a specific point in time. The table side must be a CDC/changelog stream.

### Interval Join

```sql
FROM orders o
JOIN payments p ON o.order_id = p.order_id
  AND p.event_time BETWEEN o.event_time - INTERVAL '1' HOUR
                       AND o.event_time + INTERVAL '1' HOUR
```

Matches events within a symmetric or asymmetric time window.

---

## State Management for Joins

Joins are the most state-intensive operations in streaming:

| Join Type | State Per Side | Growth |
|-----------|----------------|--------|
| Unbounded inner | All unmatched events | Infinite! |
| Interval join | Events within window | Bounded |
| Temporal join | Table versions | Bounded by TTL |

### Configuring State TTL

```yaml
advanced:
  flink:
    state_ttl_ms: 86400000  # 24 hours
```

For temporal joins, TTL controls how far back you can look up historical versions. For interval joins, the join condition itself bounds the state.

### Memory Estimation

```
State size ≈ (events/second) × (window size in seconds) × (avg event size)

Example:
- 1,000 orders/second
- 24-hour join window
- 500 bytes per order

State = 1,000 × 86,400 × 500 = 43.2 GB per side!
```

**Tips:**

1. Reduce window size when possible
2. Filter early (before the join)
3. Use RocksDB for large state
4. Project only needed columns

---

## Troubleshooting

### "Join produces no results"

1. Check watermark progress on both sides
2. Ensure join keys match exactly (case, type)
3. Verify time bounds overlap

### "Missing matches that should exist"

1. Event may have arrived after watermark passed
2. Increase `max_out_of_orderness_ms` on both sources
3. Check for clock skew between producers

### "State grows unbounded"

1. Add time bounds to join condition
2. Add `state_ttl_ms` to flink config
3. Switch to RocksDB state backend

### "Temporal join returns NULL"

1. The lookup table had no entry at that timestamp
2. State TTL may have expired the historical version
3. Check if CDC events are reaching Kafka

---

## Complete Pipeline

Your final lineage:

```bash
streamt lineage
```

```
orders_raw (source)
    └── orders_enriched (flink)
            └── order_payment_matched (flink)
                    └── abandoned_orders (flink)

customers_cdc (source)
    └── orders_enriched (flink)

payments_raw (source)
    └── order_payment_matched (flink)
```

Deploy everything:

```bash
streamt validate && streamt apply
```

---

## Next Steps

- [CDC Pipelines Tutorial](./cdc-pipelines.md) - Set up Debezium for the customers_cdc source
- [State TTL Configuration](../reference/flink-options.md#state-ttl) - Fine-tune state management
- [Streaming Fundamentals](../concepts/streaming-fundamentals.md) - Review watermarks and state
