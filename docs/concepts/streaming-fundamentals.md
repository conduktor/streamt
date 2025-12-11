---
title: Streaming Fundamentals
description: Essential concepts for understanding real-time data processing
---

# Streaming Fundamentals

This guide covers the essential concepts you need to understand when working with streaming data. If you're coming from a batch/SQL background (like dbt), this will help you understand why streaming behaves differently.

## Why Streaming Is Different

In batch processing, you work with **complete datasets**. The data is static, you transform it, done.

In streaming, data **never stops arriving**. This creates fundamental challenges:

| Batch | Streaming |
|-------|-----------|
| Data is complete | Data is infinite |
| Process once | Process continuously |
| "Now" is when you run the job | "Now" is ambiguous |
| Results are final | Results may update |
| Failures restart from scratch | Failures must resume |

Understanding these differences is key to building reliable streaming pipelines.

---

## Event Time vs Processing Time

The most important concept in streaming is understanding **two different notions of time**.

### Processing Time

**When the system sees the event.** This is the wall-clock time on your Flink server when it processes a message.

```
Event created: 10:00:00
Network delay: 5 seconds
Kafka lag: 10 seconds
Flink processes it: 10:00:15  ← Processing time
```

### Event Time

**When the event actually happened.** This is a timestamp embedded in the event itself, set by the producer.

```yaml
# The event payload
{
  "order_id": "12345",
  "amount": 99.99,
  "event_timestamp": "2025-01-15T10:00:00Z"  ← Event time
}
```

### Why Does This Matter?

Imagine you're counting orders per hour. Using processing time:

- An order from 9:55 AM arrives at 10:05 AM (network delay)
- It gets counted in the 10:00-11:00 window
- Your hourly counts are **wrong**

Using event time:

- The same order has `event_timestamp: 09:55:00`
- It gets counted in the 9:00-10:00 window
- Your hourly counts are **correct**

**Rule of thumb**: Always use event time for business logic. Processing time is only useful for monitoring the system itself.

### In streamt (Coming Soon)

```yaml
sources:
  - name: orders
    topic: orders.raw.v1

    # Tell Flink which field is the event time
    event_time:
      column: event_timestamp
```

---

## Watermarks

If events can arrive late, how does Flink know when it's "safe" to compute results?

**Watermarks** are Flink's answer. A watermark is a declaration: "I believe all events with timestamp ≤ W have arrived."

### How Watermarks Work

```
Time →

Events:    [09:58] [09:55] [10:02] [09:59] [10:01]
                     ↑
              This arrived late!

Watermark: ────────────────[09:55]──────────────→
           "All events ≤ 09:55 should have arrived"
```

When the watermark passes 10:00, Flink can safely close the 9:00-10:00 window and emit results.

### Watermark Strategies

**Bounded Out-of-Orderness** (most common):

Assume events can be up to N seconds late.

```sql
-- Flink SQL
WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
```

This means: "Events can arrive up to 5 seconds after their event time."

**Monotonously Increasing**:

Assume events always arrive in order (rare in practice).

```sql
WATERMARK FOR event_time AS event_time
```

### What Happens to Late Data?

Events that arrive after the watermark has passed are called **late data**. By default, Flink drops them.

You can configure **allowed lateness** to accept late events and update results:

```yaml
# Coming soon in streamt
event_time:
  column: event_timestamp
  watermark:
    max_out_of_orderness_ms: 5000   # 5 seconds
  allowed_lateness_ms: 60000         # Accept up to 1 minute late
```

### Choosing Watermark Delay

| Scenario | Suggested Delay |
|----------|-----------------|
| Local/same datacenter | 1-5 seconds |
| Cross-region | 10-30 seconds |
| Mobile/IoT devices | 1-5 minutes |
| Batch uploads | Hours or disable |

**Trade-off**: Longer delay = more complete results, but higher latency before results are available.

---

## Windows

Windows group events for aggregation. Without windows, aggregations over infinite streams would never complete.

### Tumbling Windows

Fixed-size, non-overlapping. Most common for hourly/daily aggregations.

```
|----Window 1----|----Window 2----|----Window 3----|
   10:00-11:00      11:00-12:00      12:00-13:00
```

```sql
SELECT
  TUMBLE_START(event_time, INTERVAL '1' HOUR) as window_start,
  COUNT(*) as order_count
FROM orders
GROUP BY TUMBLE(event_time, INTERVAL '1' HOUR)
```

### Hopping (Sliding) Windows

Fixed-size, overlapping. Useful for moving averages.

```
|----Window 1----|
     |----Window 2----|
          |----Window 3----|
```

```sql
-- 1-hour windows, sliding every 15 minutes
SELECT
  HOP_START(event_time, INTERVAL '15' MINUTE, INTERVAL '1' HOUR) as window_start,
  AVG(amount) as avg_amount
FROM orders
GROUP BY HOP(event_time, INTERVAL '15' MINUTE, INTERVAL '1' HOUR)
```

### Session Windows

Dynamic size based on activity gaps. Useful for user sessions.

```
|--Session 1--|  gap  |----Session 2----|  gap  |--Session 3--|
  (activity)          (activity)                  (activity)
```

```sql
-- New session after 10 minutes of inactivity
SELECT
  user_id,
  SESSION_START(event_time, INTERVAL '10' MINUTE) as session_start,
  COUNT(*) as events_in_session
FROM user_events
GROUP BY user_id, SESSION(event_time, INTERVAL '10' MINUTE)
```

### When Do Windows Emit Results?

Windows emit results when the **watermark passes the window end time**.

```
Window: 10:00 - 11:00
Watermark delay: 5 seconds

Events arrive...
Watermark reaches 11:00:05 → Window closes, results emitted
```

---

## State Management

Streaming jobs maintain **state** to remember things across events.

### What Creates State?

| Operation | State Required |
|-----------|---------------|
| `COUNT(*)` | Counter per group |
| `SUM(amount)` | Running total per group |
| `JOIN` | Buffer of unmatched events |
| `DISTINCT` | Set of seen values |
| Windows | Events waiting for window to close |

### The State Problem

State grows over time. Without management, your job will eventually run out of memory.

**Example**: Counting orders per customer

```sql
SELECT customer_id, COUNT(*)
FROM orders
GROUP BY customer_id
```

If you have 1 million customers, you're storing 1 million counters. Forever growing.

### State TTL (Time-To-Live)

State TTL automatically expires old state. Coming soon in streamt:

```yaml
models:
  - name: customer_order_counts
    sql: |
      SELECT customer_id, COUNT(*)
      FROM {{ ref("orders") }}
      GROUP BY customer_id
    advanced:
      flink:
        state_ttl_ms: 86400000  # Expire state after 24 hours
```

**Trade-off**: TTL too short = incorrect results for returning users. TTL too long = memory pressure.

### State Backends

| Backend | Best For |
|---------|----------|
| `hashmap` | Development, small state |
| `rocksdb` | Production, large state |

RocksDB spills to disk, allowing state larger than memory.

---

## Exactly-Once Semantics

In distributed systems, failures happen. The question is: what happens to your data?

### Delivery Guarantees

| Guarantee | Meaning | Risk |
|-----------|---------|------|
| At-most-once | Events may be lost | Data loss |
| At-least-once | Events may be duplicated | Double-counting |
| Exactly-once | Events processed exactly once | None (but complex) |

### How Flink Achieves Exactly-Once

1. **Checkpoints**: Periodically snapshot the entire job state
2. **Kafka Transactions**: Atomic writes to output topics
3. **Two-Phase Commit**: Coordinate between Flink and Kafka

When a failure occurs:
1. Job restarts from the last checkpoint
2. Replays events from Kafka (which retains history)
3. State is restored exactly as it was
4. No duplicates in output (transactional writes)

### Requirements for Exactly-Once

- Kafka as source AND sink
- Checkpointing enabled
- Transactional sink configured
- Source must be replayable (Kafka retains messages)

### In streamt

Checkpointing is configured per job:

```yaml
models:
  - name: revenue_aggregation
    sql: |
      SELECT ...
    advanced:
      flink:
        checkpoint_interval_ms: 60000  # Checkpoint every minute
```

---

## Joins in Streaming

Joining streams is fundamentally different from joining tables.

### The Challenge

Tables are static: you can look up any row at any time.

Streams are infinite: you can't wait forever for a matching event.

### Interval Joins

Join events within a time window of each other:

```sql
SELECT o.order_id, p.payment_id
FROM orders o, payments p
WHERE o.order_id = p.order_id
  AND p.event_time BETWEEN o.event_time
                       AND o.event_time + INTERVAL '1' HOUR
```

"Match orders with payments that occur within 1 hour."

### Temporal Joins (Lookup Joins)

Join a stream with the latest version of a table:

```sql
SELECT o.order_id, c.customer_name
FROM orders o
JOIN customers FOR SYSTEM_TIME AS OF o.event_time AS c
  ON o.customer_id = c.id
```

"For each order, look up the customer as of the order time."

### State Implications

Joins hold unmatched events in state, waiting for their match. Without bounds:

- Interval join: State grows with the interval size
- Regular join: State grows unbounded (avoid!)

**Rule**: Always use time bounds or TTL for streaming joins.

---

## Backpressure

What happens when your job can't keep up with incoming data?

### The Symptom

```
Kafka consumer lag: 500,000 messages (and growing)
Flink: All tasks at 100% busy
```

### The Cause

Your pipeline's slowest stage limits throughput:

```
Source → Transform → Aggregate → Sink
 1000/s    1000/s      500/s      1000/s
                         ↑
                    Bottleneck!
```

### Solutions

| Approach | When to Use |
|----------|-------------|
| Increase parallelism | CPU-bound operations |
| Optimize SQL | Inefficient queries |
| Add resources | Underprovisioned cluster |
| Sample/filter early | Don't need all data |
| Async I/O | External lookups blocking |

### In streamt

```yaml
models:
  - name: heavy_aggregation
    sql: |
      SELECT ...
    advanced:
      flink:
        parallelism: 8  # Increase parallel tasks
```

---

## Quick Reference

### Mental Model Checklist

Before building a streaming pipeline, ask:

- [ ] What is my event time field?
- [ ] How late can events arrive? (watermark strategy)
- [ ] What windows do I need?
- [ ] Will state grow unbounded? (need TTL?)
- [ ] What happens on failure? (checkpointing)
- [ ] Can my job keep up? (parallelism)

### Common Gotchas

| Gotcha | Solution |
|--------|----------|
| Results seem wrong | Check event time vs processing time |
| Missing late events | Increase watermark delay |
| Job runs out of memory | Add state TTL, use RocksDB |
| Results never appear | Watermark stuck (no events flowing) |
| Duplicates in output | Enable exactly-once, check idempotency |
| Lag keeps growing | Increase parallelism, optimize query |

### Further Reading

- [Flink Documentation: Event Time](https://nightlies.apache.org/flink/flink-docs-stable/docs/concepts/time/)
- [Flink Documentation: Windows](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/operators/windows/)
- [Streaming 101 - Tyler Akidau](https://www.oreilly.com/radar/the-world-beyond-batch-streaming-101/)
- [Streaming 102 - Tyler Akidau](https://www.oreilly.com/radar/the-world-beyond-batch-streaming-102/)

---

## Next Steps

Now that you understand the fundamentals:

1. [Build your first pipeline](../getting-started/first-pipeline.md)
2. [Learn about materializations](../reference/materializations.md)
3. [Explore Flink options](../reference/flink-options.md)
