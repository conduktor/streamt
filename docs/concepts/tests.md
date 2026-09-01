---
title: Tests
description: Validate data quality with schema, sample, and continuous tests
---

# Tests

Tests validate that your streaming data meets quality expectations. streamt supports three test types for different validation needs.

## Test Types

| Type | When | How | Use Case |
|------|------|-----|----------|
| `schema` | Compile time | Validates structure | Schema constraints |
| `sample` | On demand | Consumes N messages | Data quality checks |
| `continuous` | Always running | Flink monitoring job | Real-time alerting |

## Schema Tests

Schema tests validate at compile time without consuming messages:

```yaml
tests:
  - name: orders_schema
    model: orders_clean
    type: schema
    assertions:
      - not_null:
          columns: [order_id, customer_id, amount]

      - accepted_values:
          column: status
          values: [pending, confirmed, shipped]
```

Run with:

```bash
streamt test --type schema
```

## Sample Tests

Sample tests consume actual messages and validate assertions:

```yaml
tests:
  - name: orders_quality
    model: orders_clean
    type: sample
    sample_size: 1000        # Consume 1000 messages

    assertions:
      - range:
          column: amount
          min: 0
          max: 1000000

      - unique_key:
          key: order_id
          tolerance: 0.01    # Allow 1% duplicates
```

Run with:

```bash
streamt test --type sample
```

## Continuous Tests

Continuous tests deploy as Flink jobs for real-time monitoring:

```yaml
tests:
  - name: orders_freshness
    model: orders_clean
    type: continuous

    assertions:
      - max_lag:
          column: order_time
          max_seconds: 300   # Alert if lag > 5 minutes

      - throughput:
          min_per_second: 2  # ~120/minute

```

Deploy with:

```bash
streamt test --deploy
```

## Assertions Reference

### not_null

Check that columns are never null:

```yaml
- not_null:
    columns: [order_id, customer_id, amount]
```

### accepted_values

Check that a column only contains allowed values:

```yaml
- accepted_values:
    column: status
    values: [pending, confirmed, shipped, delivered, cancelled]
```

### accepted_types

Validate column data types:

```yaml
- accepted_types:
    types:
      order_id: string
      amount: number
      created_at: timestamp
```

**Supported types:** `string`, `number`, `int`, `bigint`, `boolean`, `timestamp`, `date`, `time`

### range

Check numeric values are within bounds:

```yaml
- range:
    column: amount
    min: 0
    max: 1000000
```

### unique_key

Validate uniqueness of a key:

```yaml
- unique_key:
    key: order_id
    tolerance: 0.0      # 0% duplicates allowed
```

Or with tolerance:

```yaml
- unique_key:
    key: transaction_id
    tolerance: 0.01     # Allow 1% duplicates (for exactly-once issues)
```

### custom_sql

Write a SQL predicate that identifies invalid rows in a continuous test:

```yaml
- custom_sql:
    name: negative_amount
    where: amount < 0
    detail_column: order_id
```

`where` is compiled into the monitoring query; `detail_column` identifies the
value included with a violation.

## Failure Actions

Define what happens when tests fail:

### Route to DLQ

Declare a dead-letter topic for failed records:

```yaml
on_failure:
  severity: error
  actions:
    - dlq:
        topic: orders.dlq.v1
```

## Complete Test Example

```yaml title="tests/orders_comprehensive.yml"
tests:
  # Schema validation (compile-time)
  - name: orders_schema_validation
    model: orders_clean
    type: schema

    assertions:
      - not_null:
          columns: [order_id, customer_id, amount, status]

      - accepted_values:
          column: status
          values: [pending, confirmed, shipped, delivered, cancelled]

      - accepted_values:
          column: currency
          values: [USD, EUR, GBP, JPY]

  # Sample test (on-demand)
  - name: orders_data_quality
    model: orders_clean
    type: sample
    sample_size: 5000

    assertions:
      - range:
          column: amount
          min: 1
          max: 10000000

      - unique_key:
          key: order_id
          tolerance: 0.001

      - distribution:
          column: amount
          buckets:
            - min: 0
              max: 100
              expected_ratio: 0.3
              tolerance: 0.1
            - min: 100
              max: 1000
              expected_ratio: 0.5
              tolerance: 0.1
            - min: 1000
              max: 100000
              expected_ratio: 0.2
              tolerance: 0.1

  # Continuous monitoring (always-on)
  - name: orders_monitoring
    model: orders_clean
    type: continuous

    assertions:
      - max_lag:
          column: created_at
          max_seconds: 180

      - throughput:
          min_per_second: 1          # ~60/minute
          max_per_second: 100        # ~6000/minute

```

## Running Tests

### Run All Tests

```bash
streamt test
```

### Filter by Type

```bash
streamt test --type schema
streamt test --type sample
streamt test --type continuous
```

### Filter by Model

```bash
streamt test --model orders_clean
```

### Deploy Continuous Tests

```bash
streamt test --deploy
```

### Check Test Status

```bash
streamt test --coverage
```

## Test Output

```bash
$ streamt test

Running tests...

Schema Tests:
  ✓ orders_schema_validation (3 assertions)
  ✓ customers_schema_validation (2 assertions)

Sample Tests:
  ✓ orders_data_quality (1000 messages, 4 assertions)
  ✗ orders_uniqueness - FAILED
    → unique_key: order_id has 2.3% duplicates (tolerance: 0.1%)

Continuous Tests:
  ○ orders_monitoring - DEPLOYED (Flink job: running)
  ○ revenue_alerting - DEPLOYED (Flink job: running)

Results: 3 passed, 1 failed, 2 deployed
```

## Best Practices

### 1. Layer Your Tests

```yaml
# Layer 1: Schema (fast, always run)
- type: schema
  assertions: [not_null, accepted_values]

# Layer 2: Sample (medium, run periodically)
- type: sample
  assertions: [range, unique_key, distribution]

# Layer 3: Continuous (slow, always-on monitoring)
- type: continuous
  assertions: [max_lag, throughput]
```

### 2. Set Meaningful Thresholds

```yaml
# Consider your data characteristics
- unique_key:
    key: event_id
    tolerance: 0.001   # 0.1% for high-volume with rare duplicates

- throughput:
    min_per_second: 2     # Based on actual expected volume
```

### 3. Test Critical Paths

Focus tests on business-critical data:

```yaml
# Test payment data rigorously
- name: payments_critical
  model: payments_validated
  type: schema
  assertions:
    - not_null: { columns: [payment_id, amount, status] }
    - range: { column: amount, min: 1 }
    - unique_key: { key: payment_id, tolerance: 0 }
```

### 4. Use DLQ for Bad Data

```yaml
on_failure:
  - dlq:
      topic: payments.dlq.v1
      include_error: true
      include_metadata: true
```

### 5. Alert Appropriately

```yaml
# Alert integrations are not implemented yet. Route failures to a DLQ and
# connect your existing monitoring system to that topic.
```

## Next Steps

- [Exposures](exposures.md) — Document downstream consumers
- [Governance Rules](../reference/governance.md) — Enforce test requirements
- [CLI Reference](../reference/cli.md) — Test command options
