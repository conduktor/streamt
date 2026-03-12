<div align="center">

# streamt

**dbt for streaming** — Declarative streaming pipelines with Kafka, Flink, and Connect

[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![License](https://img.shields.io/badge/license-Apache%202.0-green.svg)]()
[![Tests](https://img.shields.io/badge/tests-277%20passed-brightgreen.svg)]()
[![CI](https://github.com/conduktor/streamt/actions/workflows/ci.yml/badge.svg)](https://github.com/conduktor/streamt/actions)
[![Status](https://img.shields.io/badge/status-alpha-orange.svg)]()

[Documentation](docs/) • [Getting Started](#quick-start) • [Examples](#examples) • [Local Development](LOCAL_DEVELOPMENT.md) • [Community](https://conduktor.io/slack)

</div>

---

## What is streamt?

**streamt** brings the beloved dbt workflow to real-time streaming. Define your streaming pipelines declaratively using YAML and SQL, then let streamt handle compilation, validation, and deployment to Kafka, Flink, and Kafka Connect.

```yaml
sources:
  - name: payments_raw
    topic: payments.raw.v1

models:
  - name: payments_validated
    sql: |
      SELECT payment_id, customer_id, amount
      FROM {{ source("payments_raw") }}
      WHERE amount > 0 AND status IS NOT NULL
```

That's it! The model is automatically materialized as a topic or Flink job based on your SQL.

## Features

| Feature | Description |
|---------|-------------|
| 🎯 **Declarative** | Define what you want, not how to build it |
| 🔗 **Lineage** | Automatic dependency tracking from SQL |
| 🛡️ **Governance** | Enforce naming conventions, partitions, tests |
| 📊 **Testing** | Schema, sample, and continuous tests |
| 🔄 **Plan/Apply** | Review changes before deployment |
| 🤖 **Agent-Friendly** | Structured JSON output for LLM/CI integration |
| 📖 **Documentation** | Auto-generated docs with lineage diagrams |

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

### Simple Surface, Advanced Control

Most models only need `name` and `sql`. Framework details go in the optional `advanced:` section:

```yaml
# Simple: just the essentials
- name: valid_orders
  sql: SELECT * FROM {{ source("orders") }} WHERE status = 'valid'

# Advanced: tune performance when needed
- name: hourly_stats
  sql: |
    SELECT TUMBLE_START(ts, INTERVAL '1' HOUR), COUNT(*)
    FROM {{ ref("valid_orders") }}
    GROUP BY TUMBLE(ts, INTERVAL '1' HOUR)

  advanced:
    flink:
      parallelism: 4
      checkpoint_interval: 60000
    topic:
      partitions: 12
```

## Quick Start

### Installation

```bash
pip install streamt
```

### Create a Project

```yaml
# stream_project.yml
project:
  name: my-pipeline
  version: "1.0.0"

runtime:
  kafka:
    bootstrap_servers: localhost:9092
  flink:
    default: local
    clusters:
      local:
        rest_url: http://localhost:8082
        sql_gateway_url: http://localhost:8084

sources:
  - name: events
    topic: events.raw.v1

models:
  - name: events_clean
    sql: |
      SELECT event_id, user_id, event_type
      FROM {{ source("events") }}
      WHERE event_id IS NOT NULL

    # Optional: only if you need custom settings
    advanced:
      topic:
        partitions: 6
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
  protected: true  # Requires confirmation for apply

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
```

### CLI Usage

```bash
# Target a specific environment
streamt validate --env dev
streamt plan --env prod
streamt apply --env staging

# Use STREAMT_ENV environment variable
export STREAMT_ENV=prod
streamt validate

# Protected environment apply
streamt apply --env prod --confirm              # Interactive or CI
streamt apply --env prod --confirm-env prod     # Non-interactive (agents/CI)

# Override destructive safety
streamt apply --env prod --confirm --force

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

### Source with Schema

```yaml
sources:
  - name: orders_raw
    topic: orders.raw.v1
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
    columns:
      - name: order_id
        description: Unique order identifier
      - name: customer_id
        classification: internal
```

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
      TUMBLE_START(ts, INTERVAL '1' HOUR) as hour,
      SUM(amount) as revenue
    FROM {{ ref("orders_clean") }}
    GROUP BY TUMBLE(ts, INTERVAL '1' HOUR)
```

The `TUMBLE` window automatically triggers Flink materialization.

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
      fraud_score: DOUBLE
      confidence: DOUBLE
```

`ML_PREDICT` and `ML_EVALUATE` require Confluent Cloud Flink.

### Export to Warehouse (Auto-Inferred as Sink)

```yaml
- name: orders_snowflake
  from: orders_clean  # No SQL = sink
  advanced:
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

## Current Status

**Alpha** — Core functionality works, but not production-tested yet.

| Component | Status | Notes |
|-----------|--------|-------|
| YAML parsing & validation | ✅ Stable | Pydantic models, governance rules |
| DAG & lineage | ✅ Stable | Automatic from SQL refs |
| SQL parsing & type inference | ✅ Stable | sqlglot-based with custom Flink dialect |
| Automatic backend selection | ✅ Stable | Auto-detects stateless vs stateful SQL |
| Kafka topic deployment | ✅ Stable | Create, update partitions, config |
| Schema Registry | ✅ Stable | Avro/JSON/Protobuf, compatibility checks |
| Flink job generation | ✅ Works | SQL generation, REST API deployment |
| Flink job upgrades | ⚠️ Basic | No savepoint handling yet |
| Connect deployment | ✅ Works | Connector CRUD via REST |
| Testing framework | ✅ Works | Schema, sample, continuous tests |
| Continuous tests | ✅ Works | Flink-based monitoring, real-time violations |
| ML_PREDICT/ML_EVALUATE | ✅ Works | Confluent Cloud Flink only |
| CI/CD pipeline | ✅ Works | GitHub Actions for tests and linting |
| Multi-environment | ✅ Stable | Dev/staging/prod profiles, protected envs |

### What's Missing for Production

- **SQL injection mitigation** — Add input validation for identifiers in SQL generation
- **Planner module tests** — Add test coverage for deployment logic
- **HTTP response validation** — Add checks before `.json()` calls
- **Input validation** — Pydantic validators for URLs, topic names, bootstrap servers

## Roadmap

### High Value

- [x] Basic test assertions — `not_null`, `accepted_values`, `range`, `accepted_types`, `custom_sql` (continuous tests)
- [x] Hide implementation details — Simple YAML surface; `advanced:` section for framework control
- [x] Multi-environment support — dev/staging/prod profiles with protected environments
- [ ] Advanced test assertions — `unique_key`, `foreign_key`, `distribution`, `max_lag`, `throughput` (require windowing/aggregation)
- [ ] Test failure handlers — `on_failure` actions (alert to Slack/PagerDuty, pause model, route to DLQ, block deployment)
- [ ] DLQ support — Dead Letter Queue for failed messages
- [ ] Flink savepoint handling — Graceful upgrades without data loss
- [ ] Global credentials/connections — Define Snowflake, S3, etc. once and reference everywhere

### Operational

- [ ] Build artifacts — `streamt build` generates deployable artifacts for debugging, auditing, air-gapped deployments ([spec](docs/specs/build-artifacts.md))
- [ ] Prometheus/OpenTelemetry integration — Metrics and alerting
- [ ] Kubernetes Flink operator support — Native K8s deployment
- [ ] CI/CD GitHub Actions templates — Automation for deploy pipelines
- [ ] Curated connector library — Tested configs for Postgres, Snowflake, S3
- [ ] CLI: `streamt init` — Initialize new project from template
- [ ] CLI: `streamt init --discover` — Bootstrap config from existing Kafka/SR infrastructure ([spec](docs/specs/init-command.md))
- [ ] CLI: `streamt diff` — Show diff between local and deployed state
- [ ] CLI: `streamt rollback` — Rollback to previous deployment

### Vision

- [ ] External app support — Register "blackbox" applications (Java, Go) with input/output models for lineage
- [ ] High-level intent mode — "I want X" and streamt builds the entire pipeline
- [ ] KStreams runtime — `materialized: kstreams` for users without Flink; SQL→topology conversion via `ksqlDBContext`; K8s auto-scaling
- [ ] RisingWave runtime — Streaming SQL database alternative to Flink; PostgreSQL-compatible SQL
- [ ] Materialize runtime — Incremental view maintenance; PostgreSQL-compatible streaming SQL

### Deferred

- [ ] VS Code extension
- [ ] Additional streaming substrates (Pulsar, Kinesis)
- [ ] Cloud/SaaS version

## License

Apache 2.0

---

<div align="center">

**[Documentation](docs/)** • **[Examples](examples/)** • **[Local Development](LOCAL_DEVELOPMENT.md)** • **[Community](https://conduktor.io/slack)**

</div>
