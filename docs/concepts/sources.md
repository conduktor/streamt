---
title: Sources
description: Declaring external data entry points in streamt
---

# Sources

Sources represent external Kafka topics that your pipeline consumes. They're the entry points for data flowing into your streaming system.

## What is a Source?

A **source** is a Kafka topic that:

- Is produced by another system (not your streamt project)
- Your models read from using `{{ source("name") }}`
- You don't create or modify — only declare

Think of sources as contracts: "This topic exists, has this schema, and is owned by this team."

## Basic Definition

```yaml title="sources/events.yml"
sources:
  - name: user_events
    topic: events.users.v1
    description: User activity events from the web application
```

The `name` is how you reference it in SQL. The `topic` is the actual Kafka topic name.

## Complete Source Definition

```yaml
sources:
  - name: orders_raw
    topic: orders.raw.v1
    description: |
      Raw order events from the checkout service.
      Contains all order attempts including failed ones.

    # Ownership
    owner: checkout-team
    tags: [orders, checkout, critical]

    # Freshness SLA
    freshness:
      warn_after: 5m      # Warn if no messages for 5 minutes
      error_after: 15m    # Error if no messages for 15 minutes

    # Schema reference
    schema:
      registry: confluent
      subject: orders-raw-value
      version: latest     # or specific version number

    # Column definitions
    columns:
      - name: order_id
        description: Unique order identifier (UUID)
        tests:
          - not_null
          - unique

      - name: customer_id
        description: Customer who placed the order
        classification: internal

      - name: email
        description: Customer email address
        classification: sensitive

      - name: total_amount
        description: Order total in cents
        classification: internal

      - name: status
        description: Order status
        tests:
          - accepted_values:
              values: [pending, confirmed, shipped, delivered, cancelled]
```

## Properties Reference

### Required Properties

| Property | Type | Description |
|----------|------|-------------|
| `name` | string | Unique identifier for the source |
| `topic` | string | Kafka topic name |

### Optional Properties

| Property | Type | Description |
|----------|------|-------------|
| `description` | string | Human-readable description |
| `owner` | string | Team/person responsible |
| `tags` | list | Labels for organization |
| `freshness` | object | SLA monitoring settings |
| `schema` | object | Schema Registry reference |
| `columns` | list | Column definitions |

## Freshness Monitoring

Track how fresh your source data is:

```yaml
freshness:
  warn_after: 5m      # Duration without messages before warning
  error_after: 15m    # Duration without messages before error
```

Durations support: `s` (seconds), `m` (minutes), `h` (hours), `d` (days)

Freshness is checked during `streamt test` when configured.

## Schema Integration

Define schemas for your sources to enable Schema Registry integration:

### Explicit Schema Definition

Provide a complete schema definition:

```yaml
schema:
  format: avro                 # avro | json | protobuf
  subject: orders-raw-value    # Subject name (defaults to {topic}-value)
  definition: |
    {
      "type": "record",
      "name": "Order",
      "namespace": "com.example",
      "fields": [
        {"name": "order_id", "type": "string"},
        {"name": "amount", "type": "double"},
        {"name": "customer_id", "type": "string"}
      ]
    }
```

### Schema Registry Reference

Reference an existing schema in Schema Registry:

```yaml
schema:
  registry: confluent           # Schema Registry type
  subject: orders-raw-value     # Subject name
  version: latest               # or specific version (1, 2, etc.)
```

### Auto-Generated Schema

When you define columns but no explicit schema, streamt auto-generates an Avro schema:

```yaml
sources:
  - name: orders
    topic: orders.v1
    schema:
      format: avro              # Just specify format
    columns:
      - name: order_id
        description: Unique order identifier
      - name: amount
        description: Order total
      - name: status
        description: Order status
```

This generates:

```json
{
  "type": "record",
  "name": "orders",
  "namespace": "com.streamt",
  "fields": [
    {"name": "order_id", "type": ["null", "string"], "default": null},
    {"name": "amount", "type": ["null", "string"], "default": null},
    {"name": "status", "type": ["null", "string"], "default": null}
  ]
}
```

### Schema Types

| Format | Use Case | Schema Registry Type |
|--------|----------|---------------------|
| `avro` | Structured data, evolution support | AVRO |
| `json` | Flexible schemas, web APIs | JSON |
| `protobuf` | High performance, gRPC | PROTOBUF |

### Deployment Behavior

When you run `streamt apply`:

1. Schemas are registered **before** topics are created
2. Compatibility is checked against existing versions
3. If incompatible, deployment fails with an error
4. Schema artifacts are written to `generated/schemas/`

When `--check-schemas` is passed to `streamt validate`, the schema is fetched and column definitions are validated.

## Column Definitions

Document your source schema:

```yaml
columns:
  - name: order_id
    description: Unique identifier
    classification: public      # Data classification

  - name: customer_email
    description: Customer email
    classification: sensitive   # Will require masking

  - name: amount
    description: Transaction amount
    tests:                      # Column-level tests
      - not_null
      - range:
          min: 0
```

### Data Classification Levels

| Level | Description | Typical Handling |
|-------|-------------|------------------|
| `public` | Can be shared freely | No restrictions |
| `internal` | Internal use only | No external exposure |
| `confidential` | Business sensitive | Limited access |
| `sensitive` | PII, requires protection | Masking required |
| `highly_sensitive` | Regulated data (PCI, HIPAA) | Encryption + audit |

## Using Sources in Models

Reference sources in your SQL with the `source()` function:

```yaml title="models/orders_clean.yml"
models:
  - name: orders_clean
    sql: |
      SELECT
        order_id,
        customer_id,
        total_amount
      FROM {{ source("orders_raw") }}
      WHERE status IS NOT NULL
```

The `{{ source("orders_raw") }}` is replaced with the actual topic name during compilation.

## Multiple Sources

You can define multiple sources in one file:

```yaml title="sources/all.yml"
sources:
  - name: orders_raw
    topic: orders.raw.v1
    owner: checkout-team

  - name: payments_raw
    topic: payments.raw.v1
    owner: payments-team

  - name: users_raw
    topic: users.raw.v1
    owner: identity-team
```

Or organize by domain:

```
sources/
├── orders.yml       # Order-related sources
├── payments.yml     # Payment-related sources
└── users.yml        # User-related sources
```

## Source vs Model

| Aspect | Source | Model |
|--------|--------|-------|
| Created by | External system | Your project |
| Modifiable | No | Yes |
| SQL | None | Required |
| Topic | Pre-existing | Created by streamt |
| Owner | External team | Your team |

## Best Practices

### 1. Always Add Descriptions

```yaml
# Good
- name: orders_raw
  description: Raw order events from checkout, including failed attempts

# Bad
- name: orders_raw
  topic: orders.raw.v1
```

### 2. Define Ownership

```yaml
- name: orders_raw
  owner: checkout-team
  tags: [orders, critical, tier-1]
```

### 3. Set Freshness SLAs

```yaml
freshness:
  warn_after: 5m
  error_after: 15m
```

### 4. Document Columns

```yaml
columns:
  - name: order_id
    description: UUID, globally unique
  - name: amount
    description: Total in cents (USD)
```

### 5. Classify Sensitive Data

```yaml
columns:
  - name: email
    classification: sensitive
  - name: credit_card
    classification: highly_sensitive
```

## Validation Rules

With governance rules enabled, sources can be validated:

```yaml title="stream_project.yml"
rules:
  sources:
    require_schema: true       # Must have schema reference
    require_freshness: true    # Must have freshness SLA
    require_columns: true      # Must document columns
```

## Example: E-commerce Sources

```yaml title="sources/ecommerce.yml"
sources:
  - name: products
    topic: catalog.products.v1
    description: Product catalog updates
    owner: catalog-team
    freshness:
      warn_after: 1h
      error_after: 4h
    columns:
      - name: product_id
        description: Unique product SKU
      - name: price
        description: Current price in cents
        classification: internal

  - name: inventory
    topic: inventory.updates.v1
    description: Real-time inventory changes
    owner: warehouse-team
    freshness:
      warn_after: 5m
      error_after: 15m
    columns:
      - name: product_id
      - name: warehouse_id
      - name: quantity_available

  - name: orders
    topic: orders.created.v1
    description: New order events
    owner: checkout-team
    freshness:
      warn_after: 1m
      error_after: 5m
    schema:
      registry: confluent
      subject: orders-created-value
```

## Next Steps

- [Models](models.md) — Transform source data
- [Tests](tests.md) — Validate source data quality
- [DAG & Lineage](dag.md) — Track data flow
