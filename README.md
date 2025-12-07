<div align="center">

# streamt

**dbt for streaming** — Declarative streaming pipelines with Kafka, Flink, and Connect

[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![License](https://img.shields.io/badge/license-Apache%202.0-green.svg)](LICENSE)
[![Tests](https://img.shields.io/badge/tests-186%20passed-brightgreen.svg)]()
[![Status](https://img.shields.io/badge/status-alpha-orange.svg)]()

[Documentation](docs/) • [Getting Started](#quick-start) • [Examples](#examples)

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
    materialized: flink
    sql: |
      SELECT payment_id, customer_id, amount
      FROM {{ source("payments_raw") }}
      WHERE amount > 0 AND status IS NOT NULL
```

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

| Type | Use Case | Creates |
|------|----------|---------|
| `topic` | Simple transforms, filtering | Kafka topic + Flink job |
| `virtual_topic` | Read-time filtering (no storage) | Conduktor Gateway rule* |
| `flink` | Stateful: windows, joins, aggregations | Kafka topic + Flink job |
| `sink` | Export to external systems | Kafka Connect connector |

> *`virtual_topic` requires [Conduktor Gateway](https://www.conduktor.io/gateway/) (commercial)

### When to use `topic` vs `flink`?

Both create Flink jobs when SQL is present. The difference is semantic:

- **`topic`**: Focus is on the output topic. Simple transforms, filtering, field selection.
- **`flink`**: Focus is on the processing. Use for windows, joins, aggregations, or when you need Flink-specific config (parallelism, checkpoints, state backend).

```yaml
# Use topic: simple filter, output is what matters
- name: valid_orders
  materialized: topic
  sql: SELECT * FROM {{ source("orders") }} WHERE status = 'valid'

# Use flink: windowed aggregation, processing is complex
- name: hourly_stats
  materialized: flink
  flink:
    parallelism: 4
    checkpoint_interval_ms: 60000
  sql: |
    SELECT TUMBLE_START(ts, INTERVAL '1' HOUR), COUNT(*)
    FROM {{ ref("valid_orders") }}
    GROUP BY TUMBLE(ts, INTERVAL '1' HOUR)
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
    materialized: topic
    topic:
      partitions: 6
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

### Simple Transform (Topic)

```yaml
- name: high_value_orders
  materialized: topic
  sql: |
    SELECT * FROM {{ source("orders_raw") }}
    WHERE amount > 10000
```

### Windowed Aggregation (Flink)

```yaml
- name: hourly_revenue
  materialized: flink
  sql: |
    SELECT
      TUMBLE_START(ts, INTERVAL '1' HOUR) as hour,
      SUM(amount) as revenue
    FROM {{ ref("orders_clean") }}
    GROUP BY TUMBLE(ts, INTERVAL '1' HOUR)
```

### Export to Warehouse (Sink)

```yaml
- name: orders_snowflake
  materialized: sink
  from: orders_clean
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

- **Flink savepoint management** — Job upgrades don't preserve state yet
- **Kubernetes Flink operator** — Currently REST API only
- **CI/CD templates** — GitHub Actions, etc.
- **Observability** — Metrics, alerting integration

## Roadmap

- [x] Schema Registry integration
- [ ] Conduktor Gateway virtual topics
- [ ] Kubernetes Flink operator support
- [ ] Flink savepoint handling for upgrades
- [ ] CI/CD GitHub Actions templates
- [ ] VS Code extension

## License

Apache 2.0 - See [LICENSE](LICENSE) for details.

---

<div align="center">

**[Documentation](docs/)** • **[Examples](examples/)** • **[Contributing](CONTRIBUTING.md)**

</div>
