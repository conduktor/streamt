---
title: Conduktor Gateway
description: Virtual topics, filtering, and data masking with Conduktor Gateway
---

# Conduktor Gateway Integration

[Conduktor Gateway](https://www.conduktor.io/gateway/) is a Kafka proxy that enables virtual topics, data masking, and read-time transformations without modifying your Kafka cluster.

## When to Use Gateway

- **Virtual topics**: Logical views over physical topics
- **Data masking**: PII protection without copying data
- **Multi-tenancy**: Role-based access to the same topic
- **Migration**: Alias topic names during transitions
- **Filtering**: Read-time filtering without storage cost

## Prerequisites

Gateway must be configured in your project:

```yaml
# stream_project.yml
runtime:
  kafka:
    bootstrap_servers: localhost:9092

  conduktor:
    gateway:
      admin_url: http://localhost:8888
      proxy_bootstrap: localhost:6969
      username: admin
      password: ${GATEWAY_PASSWORD}
```

## Virtual Topics

Virtual topics are logical topics that map to physical topics with transformations applied at read time.

### Basic Virtual Topic

```yaml
models:
  - name: orders_europe
    sql: |
      SELECT * FROM {{ source("orders") }}
      WHERE region = 'EU'

    advanced:
      # Explicit configuration for virtual topics via Gateway
      virtual_topic: true
```

What happens:

1. No physical topic is created
2. Gateway creates an alias `orders_europe` -> `orders`
3. A filter interceptor is applied
4. Consumers connecting via Gateway (port 6969) see only EU orders

### Virtual Topic vs Topic

| Aspect | Physical topic | Virtual topic (Gateway) |
|--------|----------------|-------------------------|
| Storage | Creates real Kafka topic | No storage (alias only) |
| Latency | Pre-computed | Computed on read |
| CPU | At write time (Flink) | At read time (Gateway) |
| Data freshness | Point-in-time | Always current |
| Role-based masking | No | Yes |
| Gateway required | No | Yes |

**Use virtual_topic when:**

- You need different views for different consumers
- Storage cost is a concern
- Data must always be current
- You need role-based masking

**Use physical topic when:**

- High read volume (pre-compute is cheaper)
- Low latency is critical
- Gateway is not available

## Data Masking

Apply column-level masking based on consumer roles:

```yaml
models:
  - name: customers_view
    from:
      - source: customers_raw

    advanced:
      virtual_topic: true
      security:
        policies:
          # Hash email for analytics team
          - mask:
              column: email
              method: hash
              for_roles: [analytics]

          # Redact SSN for support team
          - mask:
              column: ssn
              method: redact
              for_roles: [support]

          # Partial mask phone for everyone else
          - mask:
              column: phone
              method: partial
```

### Masking Methods

| Method | Input | Output | Use Case |
|--------|-------|--------|----------|
| `hash` | `john@example.com` | `a1b2c3d4e5...` | Unique but anonymous |
| `redact` | `secret123` | `***` | Complete hiding |
| `partial` | `555-123-4567` | `***-***-4567` | Keep some context |
| `null` | `any value` | `null` | Remove entirely |

## Topic Aliasing

Alias topics enable transparent migrations:

```yaml
# Old topic name -> new topic name
models:
  - name: orders_v1
    from:
      - source: orders_v2

    advanced:
      virtual_topic: true
```

Consumers using `orders_v1` through Gateway will read from `orders_v2`.

## SQL-Based Filtering

Apply SQL WHERE clauses at read time:

```yaml
models:
  - name: high_value_orders
    sql: |
      SELECT * FROM {{ source("orders") }}
      WHERE amount > 10000 AND status = 'completed'

    advanced:
      virtual_topic: true
```

The WHERE clause becomes a Gateway filter interceptor.

### Supported SQL Syntax

| Supported | Not Supported |
|-----------|---------------|
| `=`, `<>` | `IN (...)` |
| `>`, `<`, `>=`, `<=` | `OR` |
| `REGEXP` | `IS NULL`, `IS NOT NULL` |
| `AND` | `LIKE`, `BETWEEN` |

**Workaround for IN**: Use `REGEXP` pattern matching:
```yaml
# Instead of: WHERE region IN ('US', 'EU')
# Use:
WHERE region REGEXP 'US|EU'
```

## Multi-Tenancy

Create tenant-specific views of shared data:

```yaml
models:
  - name: orders_tenant_a
    sql: |
      SELECT * FROM {{ source("orders") }}
      WHERE tenant_id = 'tenant_a'

    advanced:
      virtual_topic: true

  - name: orders_tenant_b
    sql: |
      SELECT * FROM {{ source("orders") }}
      WHERE tenant_id = 'tenant_b'

    advanced:
      virtual_topic: true
```

## Configuration Reference

### Project Configuration

```yaml
runtime:
  conduktor:
    gateway:
      admin_url: http://localhost:8888      # Admin API (required)
      proxy_bootstrap: localhost:6969        # Kafka proxy (for clients)
      username: admin                        # Admin API user
      password: conduktor                    # Admin API password
      virtual_cluster: default               # Optional: for multi-cluster setups
```

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `GATEWAY_ADMIN_URL` | - | Gateway Admin API URL |
| `GATEWAY_PROXY_BOOTSTRAP` | - | Gateway proxy for Kafka clients |
| `GATEWAY_USERNAME` | `admin` | Admin API username |
| `GATEWAY_PASSWORD` | `conduktor` | Admin API password |

## Connecting Through Gateway

Kafka clients must connect through Gateway's proxy port (default: 6969) to see virtual topics:

```python
# Python (confluent-kafka)
from confluent_kafka import Consumer

consumer = Consumer({
    'bootstrap.servers': 'localhost:6969',  # Gateway proxy, not Kafka directly
    'group.id': 'my-consumer',
})
consumer.subscribe(['orders_europe'])  # Virtual topic name
```

```java
// Java
Properties props = new Properties();
props.put("bootstrap.servers", "localhost:6969");  // Gateway proxy
props.put("group.id", "my-consumer");

KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
consumer.subscribe(Arrays.asList("orders_europe"));
```

## Deployment

Virtual topics are deployed with:

```bash
streamt apply
```

This creates:

1. **Alias topic mapping** in Gateway
2. **Filter interceptors** from SQL WHERE clauses
3. **Masking interceptors** from security policies

## Troubleshooting

### Gateway Not Configured

```
Error: virtual_topic requires Conduktor Gateway.
Configure runtime.conduktor.gateway or remove advanced.virtual_topic
```

Add Gateway configuration to your project.

### Cannot Connect to Gateway

```
Error: Cannot connect to Gateway at http://localhost:8888
```

Ensure Gateway is running:

```bash
docker compose up -d conduktor-gateway
curl http://localhost:8888/health
```

### Consumer Can't See Virtual Topic

Ensure consumer is connecting through Gateway proxy (port 6969), not directly to Kafka (port 9092).

## Example: Analytics Data Pipeline

```yaml
sources:
  - name: orders_raw
    topic: orders.raw.v1
    columns:
      - name: order_id
      - name: customer_id
      - name: customer_email
      - name: amount
      - name: region

models:
  # Full access for data engineers
  - name: orders_full
    from:
      - source: orders_raw

    advanced:
      virtual_topic: true
      access:
        allowed_groups: [data-engineering]

  # Masked view for analytics (US region only)
  - name: orders_analytics
    sql: |
      SELECT * FROM {{ source("orders_raw") }}
      WHERE region REGEXP 'US.*'

    advanced:
      virtual_topic: true
      security:
        policies:
          - mask:
              column: customer_email
              method: hash
          - mask:
              column: customer_id
              method: hash
      access:
        allowed_groups: [analytics]

  # Regional view for EU team
  - name: orders_eu
    sql: |
      SELECT * FROM {{ source("orders_raw") }}
      WHERE region = 'EU'

    advanced:
      virtual_topic: true
      access:
        allowed_groups: [eu-team]
```

This creates three virtual topics from one physical topic, each with different filtering and masking rules.
