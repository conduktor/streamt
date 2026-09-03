<div align="center">

# streamt

**dbt for streaming** — Declarative streaming pipelines with Kafka, Flink, and Connect

[![Python 3.10–3.14](https://img.shields.io/badge/python-3.10--3.14-blue.svg)](https://www.python.org/downloads/)
[![License](https://img.shields.io/badge/license-Apache%202.0-green.svg)](https://github.com/conduktor/streamt/blob/main/LICENSE)
[![CI](https://github.com/conduktor/streamt/actions/workflows/ci.yml/badge.svg)](https://github.com/conduktor/streamt/actions)
[![Status](https://img.shields.io/badge/status-alpha-orange.svg)]()

[Documentation](https://conduktor.github.io/streamt/) • [Getting Started](#quick-start) • [Examples](https://github.com/conduktor/streamt/tree/main/examples) • [Local Development](https://github.com/conduktor/streamt/blob/main/LOCAL_DEVELOPMENT.md) • [Community](https://conduktor.io/slack)

</div>

---

## What is streamt?

**streamt** is declarative infrastructure for streaming data products. Not just ETL tooling — a policy-as-code layer that treats Kafka topics, Flink jobs, and data governance rules with the same rigor that Terraform brought to compute and dbt Mesh brought to the warehouse.

The dbt workflow (sources, models, tests, lineage, plan/apply) is the surface. The deeper ambition: a single YAML project that encodes *what your data means* — who owns it, how it's classified, which fields must be masked, what retention limits apply, which teams consume it downstream. Today most platform teams track this across Conduktor Console, DataHub, and spreadsheets. streamt makes it a first-class artifact next to the SQL.

```yaml
sources:
  - name: payments_raw
    topic: payments.raw.v1

models:
  - name: payments_clean
    owner: team-payments
    sql: |
      SELECT payment_id, customer_id, amount
      FROM {{ source("payments_raw") }}
      WHERE amount > 0 AND status IS NOT NULL
    security:
      classification:
        customer_id: confidential
      policies:
        - mask:
            column: customer_id
            method: hash
    topic:
      config:
        retention.ms: 2592000000  # 30 days
```

`streamt apply` doesn't just create a Kafka topic and a Flink job — it enforces the policy, validates the schema, and records the lineage. `streamt plan` shows the diff before anything touches production.

## Features

| Feature | Description |
|---------|-------------|
| **Declarative** | Define what you want, not how to build it |
| **Lineage** | Automatic dependency tracking from SQL |
| **Policy-as-code** | Classification, masking, retention, and owner rules enforced at compile time |
| **Testing** | Schema, sample, and continuous tests |
| **Plan/Apply** | Review changes before deployment — like Terraform for streaming |
| **Agent-Friendly** | Structured JSON output for LLM/CI integration |
| **Documentation** | Auto-generated docs with lineage diagrams |

## How It Works

streamt compiles your YAML definitions into deployable artifacts:

1. **Sources** → Metadata only (external topics you consume)
2. **Models with SQL** → Flink SQL jobs that read from sources/models and write to output topics
3. **Sinks** → Kafka Connect connector configurations

**All SQL transformations run on Flink.** streamt generates Flink SQL with CREATE TABLE statements for your sources, your transformation query, and INSERT INTO for the output topic.

## Materializations

Materializations are **automatically inferred** from your SQL:

| SQL Pattern | Inferred Type | Creates |
|-------------|---------------|---------|
| Stateless (`WHERE`, projections) | `virtual_topic` | Gateway rule (if available) |
| Stateless (no Gateway) | `flink` | Flink job (fallback) |
| Stateful (`GROUP BY`, `JOIN`, windows) | `flink` | Flink job + Kafka topic |
| `ML_PREDICT`, `ML_EVALUATE` | `flink` | Confluent Flink job* |
| `from:` only (no SQL) | `sink` | Kafka Connect connector |
| Explicit `materialized: virtual_topic` | `virtual_topic` | Conduktor Gateway rule** |

> *ML functions require Confluent Cloud Flink.
> **`virtual_topic` requires [Conduktor Gateway](https://www.conduktor.io/gateway/).

### Simple Surface, Full Control

Most models only need `name` and `sql`. Infrastructure fields like `topic:` and `flink:` are optional top-level overrides:

```yaml
# Simple: just the essentials
- name: valid_orders
  sql: SELECT * FROM {{ source("orders") }} WHERE status = 'valid'

# Full control: tune performance when needed
- name: hourly_stats
  sql: |
    SELECT window_start, window_end, COUNT(*)
    FROM TABLE(TUMBLE(TABLE {{ ref("valid_orders") }}, DESCRIPTOR(ts), INTERVAL '1' HOUR))
    GROUP BY window_start, window_end

  flink:
    parallelism: 4
    checkpoint_interval_ms: 60000
  topic:
    partitions: 12
```

## Quick Start

### Installation

The immutable first-alpha installation is:

```bash
python -m pip install "streamt==0.1.0a1"
```

Until that exact version is visible on PyPI, install the candidate preview
from the repository:

```bash
python -m pip install "git+https://github.com/conduktor/streamt.git@main"
```

Pin `main` to an immutable commit SHA for CI or other reproducible preview
installs.

### Create a Project

```bash
# Scaffold an empty project
streamt init

# Or discover from existing Kafka infrastructure
streamt init --discover --kafka localhost:9092 --schema-registry http://localhost:8081

# Discover from Confluent Cloud
streamt init --discover \
  --kafka $CC_BOOTSTRAP \
  --security-protocol SASL_SSL \
  --sasl-mechanism PLAIN \
  --sasl-username $CC_API_KEY \
  --sasl-password $CC_API_SECRET \
  --schema-registry $CC_SR_URL \
  --sr-username $CC_SR_KEY \
  --sr-password $CC_SR_SECRET
```

```yaml
# stream_project.yml
apiVersion: streamt.dev/v1alpha1

project:
  name: my-pipeline
  version: "1.0.0"

runtime:
  kafka:
    bootstrap_servers: localhost:9092
    # For Confluent Cloud, add:
    # security_protocol: SASL_SSL
    # sasl_mechanism: PLAIN
    # sasl_username: ${CC_API_KEY}
    # sasl_password: ${CC_API_SECRET}
  flink:
    default: local
    clusters:
      local:
        rest_url: http://localhost:8082
        sql_gateway_url: http://localhost:8084

defaults:
  topic:
    partitions: 6
    replication_factor: 3

sources:
  - name: events
    topic: events.raw.v1

models:
  - name: events_clean
    sql: |
      SELECT event_id, user_id, event_type
      FROM {{ source("events") }}
      WHERE event_id IS NOT NULL
```

### CLI Commands

```bash
# Validate configuration
streamt validate

# See what will change
streamt plan

# Deploy to infrastructure
streamt apply

# Run tests
streamt test

# View lineage
streamt lineage

# Inspect resources
streamt list models
streamt show model order_metrics

# Structured JSON output (for agents/CI)
streamt -o json validate
streamt -o json list sources
streamt -o json show model order_metrics
```

## Multi-Environment Support

streamt supports managing multiple environments (dev, staging, prod) with different configurations.

### Setup

Create an `environments/` directory with YAML files for each environment:

```
my-project/
├── stream_project.yml      # No runtime section needed
├── environments/
│   ├── dev.yml
│   ├── staging.yml
│   └── prod.yml
└── models/
```

Each environment file defines its runtime configuration:

```yaml
# environments/prod.yml
environment:
  name: prod
  description: Production environment
  protected: true  # Requires a reviewed plan and confirmation for apply

runtime:
  kafka:
    bootstrap_servers: ${PROD_KAFKA_SERVERS}
  schema_registry:
    url: ${PROD_SR_URL}
  flink:
    default: prod-cluster
    clusters:
      prod-cluster:
        rest_url: ${PROD_FLINK_URL}

safety:
  confirm_apply: true
  allow_destructive: false  # Block destructive operations
  require_reviewed_plan: true  # Also available for unprotected shared envs
```

### CLI Usage

```bash
# Target a specific environment
streamt validate --env dev
streamt plan --env prod --out prod.plan.json
streamt apply --env staging

# Use STREAMT_ENV environment variable
export STREAMT_ENV=prod
streamt validate

# Protected environment apply after reviewing prod.plan.json
streamt apply --env prod --plan prod.plan.json --confirm-env prod

# Override destructive safety
streamt apply --env prod --plan prod.plan.json --confirm-env prod --force

# Validate all environments at once
streamt validate --all-envs

# List available environments
streamt envs list

# Show resolved config (secrets masked)
streamt envs show prod
```

### .env File Loading

Environment variables are loaded with precedence:

1. `.env` (base, always loaded)
2. `.env.{environment}` (if exists, e.g., `.env.prod`)
3. Actual environment variables (highest priority)

## Examples

### Source with Schema Registry

```yaml
sources:
  - name: orders_raw
    topic: orders.raw.v1
    ownership:
      mode: external              # Sources are observe-only by default
    schema:
      registry: confluent          # Pull schema from Schema Registry
      subject: orders-raw-value    # SR subject name
      version: latest              # Or a positive, pinned version number
      format: avro                 # Must match the registered schema type
    columns:
      - name: order_id
        description: Unique order identifier
      - name: customer_id
        classification: internal
```

Inline schemas are also supported when Schema Registry isn't available:

```yaml
sources:
  - name: orders_raw
    topic: orders.raw.v1
    ownership:
      mode: managed               # Explicitly lifecycle-manage this schema artifact
    schema:
      format: avro
      definition: |
        {
          "type": "record",
          "name": "Order",
          "fields": [
            {"name": "order_id", "type": "string"},
            {"name": "amount", "type": "double"},
            {"name": "customer_id", "type": "string"}
          ]
        }
```

Run `streamt validate --check-schemas` to verify external subjects, selected
versions, declared formats, and version-pinned reference graphs using read-only
Schema Registry requests. Avro and JSON Schema documents are decoded as JSON;
Protobuf content remains raw text, so this check does not infer columns from
Protobuf or validate message semantics.

### Simple Transform (Auto-Inferred as Topic)

```yaml
- name: high_value_orders
  sql: |
    SELECT * FROM {{ source("orders_raw") }}
    WHERE amount > 10000
```

### Windowed Aggregation (Auto-Inferred as Flink)

```yaml
- name: hourly_revenue
  sql: |
    SELECT
      window_start,
      window_end,
      SUM(amount) as revenue
    FROM TABLE(TUMBLE(TABLE {{ ref("orders_clean") }}, DESCRIPTOR(ts), INTERVAL '1' HOUR))
    GROUP BY window_start, window_end
```

Window TVF syntax (`TABLE(TUMBLE(...))`) is the recommended Flink SQL pattern. Legacy `GROUP BY TUMBLE(ts, ...)` is also supported.

### ML Inference (Confluent Flink)

```yaml
- name: fraud_predictions
  sql: |
    SELECT
      transaction_id,
      amount,
      ML_PREDICT('FraudModel', amount, merchant_category) as fraud_score
    FROM {{ ref("transactions") }}

  # Declare ML output schema for type inference
  ml_outputs:
    FraudModel:
      columns:
        - name: fraud_score
          type: DOUBLE
        - name: confidence
          type: DOUBLE
```

`ML_PREDICT` and `ML_EVALUATE` require Confluent Cloud Flink.

### Export to Warehouse (Auto-Inferred as Sink)

```yaml
- name: orders_snowflake
  from: orders_clean  # No SQL = sink
  connector:
    type: snowflake-sink
    config:
      snowflake.database.name: ANALYTICS
```

### Data Quality Tests

```yaml
tests:
  - name: orders_quality
    model: orders_clean
    type: sample
    assertions:
      - not_null: { columns: [order_id, amount] }
      - range: { column: amount, min: 0, max: 1000000 }
```

## Architecture

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│    YAML     │────▶│   Compile   │────▶│  Artifacts  │
│  + SQL      │     │  & Validate │     │   (JSON)    │
└─────────────┘     └─────────────┘     └──────┬──────┘
                                               │
                    ┌──────────────────────────┼──────────────────────────┐
                    ▼                          ▼                          ▼
             ┌─────────────┐           ┌─────────────┐           ┌─────────────┐
             │    Kafka    │           │    Flink    │           │   Connect   │
             │   Topics    │           │    Jobs     │           │ Connectors  │
             └─────────────┘           └─────────────┘           └─────────────┘
```

## License

Apache 2.0

---

<div align="center">

**[Documentation](https://conduktor.github.io/streamt/)** • **[Examples](https://github.com/conduktor/streamt/tree/main/examples)** • **[Local Development](https://github.com/conduktor/streamt/blob/main/LOCAL_DEVELOPMENT.md)** • **[Community](https://conduktor.io/slack)**

</div>
