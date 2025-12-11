<div align="center">

# streamt

**dbt for streaming** — Declarative streaming pipelines with Kafka, Flink, and Connect

[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![License](https://img.shields.io/badge/license-Apache%202.0-green.svg)](LICENSE)
[![Tests](https://img.shields.io/badge/tests-186%20passed-brightgreen.svg)]()
[![Status](https://img.shields.io/badge/status-alpha-orange.svg)]()

[Documentation](docs/) • [Getting Started](#quick-start) • [Examples](#examples) • [Community](https://conduktor.io/slack)

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
| Simple `SELECT` | `topic` | Kafka topic + Flink job |
| `TUMBLE`, `HOP`, `JOIN` | `flink` | Kafka topic + Flink job |
| `from:` only (no SQL) | `sink` | Kafka Connect connector |
| Explicit `materialized: virtual_topic` | `virtual_topic` | Conduktor Gateway rule* |

> *`virtual_topic` requires [Conduktor Gateway](https://www.conduktor.io/gateway/) (commercial)

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
```

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

## Local Development

Start the infrastructure:

```bash
docker compose up -d
```

Services:
- **Kafka** (KRaft): localhost:9092
- **Schema Registry**: localhost:8081
- **Flink UI**: localhost:8082
- **Connect**: localhost:8083
- **Conduktor Console**: localhost:8080 (admin@localhost / Admin123!)

Run tests:

```bash
pytest tests/ -v
```

## Documentation

```bash
# Install docs dependencies
pip install -e ".[docs]"

# Serve locally
mkdocs serve
```

## Project Structure

```
streamt/
├── src/streamt/
│   ├── cli.py              # CLI commands
│   ├── core/
│   │   ├── models.py       # Pydantic models
│   │   ├── parser.py       # YAML parser
│   │   ├── validator.py    # Validation rules
│   │   └── dag.py          # DAG builder
│   ├── compiler/           # Artifact generation
│   ├── deployer/           # Kafka, Flink, Connect
│   └── testing/            # Test runner
├── docs/                   # Documentation site
├── tests/                  # Test suite
└── examples/               # Example projects
```

## Current Status

**Alpha** — Core functionality works, but not production-tested yet.

| Component | Status | Notes |
|-----------|--------|-------|
| YAML parsing & validation | ✅ Stable | Pydantic models, governance rules |
| DAG & lineage | ✅ Stable | Automatic from SQL refs |
| Kafka topic deployment | ✅ Stable | Create, update partitions, config |
| Schema Registry | ✅ Stable | Avro/JSON/Protobuf, compatibility checks |
| Flink job generation | ✅ Works | SQL generation, REST API deployment |
| Flink job upgrades | ⚠️ Basic | No savepoint handling yet |
| Connect deployment | ✅ Works | Connector CRUD via REST |
| Testing framework | ✅ Works | Schema, sample tests |
| Continuous tests | 🚧 Planned | Flink-based monitoring |
| Multi-environment | 🚧 Planned | Dev/staging/prod profiles |

### What's Missing for Production

- **State management** — ~~State TTL~~, savepoint handling for job upgrades
- **Kubernetes Flink operator** — Currently REST API only
- **CI/CD templates** — GitHub Actions, etc.
- **Metrics integration** — Prometheus/OpenTelemetry for alerting

## Roadmap

### High Value

- [ ] Multi-environment support — dev/staging/prod profiles
- [ ] DLQ support — Dead Letter Queue for failed messages
- [ ] Flink savepoint handling — Graceful upgrades without data loss
- [ ] Global credentials/connections — Define Snowflake, S3, etc. once and reference everywhere
- [x] Hide implementation details — Simple YAML surface; `advanced:` section for framework control

### Operational

- [ ] Prometheus/OpenTelemetry integration — Metrics and alerting
- [ ] Kubernetes Flink operator support — Native K8s deployment
- [ ] CI/CD GitHub Actions templates — Automation for deploy pipelines
- [ ] Curated connector library — Tested configs for Postgres, Snowflake, S3

### Vision

- [ ] External app support — Register "blackbox" applications (Java, Go) with input/output models for lineage
- [ ] High-level intent mode — "I want X" and streamt builds the entire pipeline
- [ ] KStreams runtime — `materialized: kstreams` for users without Flink; SQL→topology conversion via `ksqlDBContext`; K8s auto-scaling

### Deferred

- [ ] VS Code extension
- [ ] Additional streaming substrates (Pulsar, Kinesis)
- [ ] Cloud/SaaS version

## License

Apache 2.0 - See [LICENSE](LICENSE) for details.

---

<div align="center">

**[Documentation](docs/)** • **[Examples](examples/)** • **[Community](https://conduktor.io/slack)** • **[Contributing](CONTRIBUTING.md)**

</div>
