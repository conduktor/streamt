---
title: Quick Start
description: Walk through a local Kafka and Flink project
---

# Quick Start

For Kafka without Flink, use the [Kafka Streams starter](kafka-streams.md).
It creates a projection/filter topology and documents a downstream custom
application. Its deployment support is create/no-op only; updates are blocked.

This manual walkthrough uses Kafka and Flink SQL. It does not yet include
automatic event seeding or a
supported update walkthrough; those are acceptance requirements for the planned
[developer workflow](../specs/developer-workflow.md).

## Prerequisites

- [streamt installed](installation.md)
- Docker running (for local Kafka)
- A checkout of the [streamt repository](https://github.com/conduktor/streamt)
  for the Compose file used below; run the infrastructure commands from its root

## 1. Start Local Infrastructure

```bash
# Start Kafka, Flink, and supporting services
docker compose up -d

# Wait for services to be healthy
docker compose ps
```

## 2. Create Your Project

```bash
mkdir my-streaming-project
cd my-streaming-project
streamt init
```

This creates `stream_project.yml`, `sources/`, `models/`, and `tests/` directories.

!!! tip "Already have Kafka topics?"
    Use `streamt init --discover --kafka localhost:9092` to auto-generate sources from existing topics. Add `--schema-registry http://localhost:8081` to extract column definitions from Avro schemas.

Edit the generated configuration file:

```yaml title="stream_project.yml"
project:
  name: my-first-pipeline
  version: "1.0.0"
  description: My first streaming pipeline with streamt

runtime:
  kafka:
    bootstrap_servers: localhost:9092
  flink:
    default: local
    clusters:
      local:
        type: rest
        rest_url: http://localhost:8082
        sql_gateway_url: http://localhost:8084
```

> **Using Confluent Cloud?** Add authentication to your runtime config:
> ```yaml
> runtime:
>   kafka:
>     bootstrap_servers: pkc-abc12.us-east-1.aws.confluent.cloud:9092
>     security_protocol: SASL_SSL
>     sasl_mechanism: PLAIN
>     sasl_username: my-api-key
>     sasl_password: my-api-secret
>   schema_registry:
>     url: https://psrc-xyz99.us-east-1.aws.confluent.cloud
>     username: sr-api-key
>     password: sr-api-secret
> ```
> Store credentials in `.env` (gitignored) and reference with `${VAR}` syntax.

## 3. Define a Source

Create a source representing incoming data:

```yaml title="sources/events.yml"
sources:
  - name: raw_events
    topic: events.raw.v1
    description: Raw user events from the web application
    owner: platform-team
    columns:
      - name: event_id
        description: Unique event identifier
      - name: user_id
        description: User who triggered the event
      - name: event_type
        description: Type of event (click, view, purchase)
      - name: timestamp
        description: When the event occurred
```

## 4. Create Your First Model

Create a model that transforms the raw events:

```yaml title="models/events_clean.yml"
models:
  - name: events_clean
    description: Cleaned and validated events
    sql: |
      SELECT
        event_id,
        user_id,
        event_type,
        `timestamp`
      FROM {{ source("raw_events") }}
      WHERE event_id IS NOT NULL
        AND user_id IS NOT NULL

    # Optional: customize topic settings
    topic:
      name: events.clean.v1
      partitions: 6
```

The model is automatically materialized as a topic since it's a simple SELECT statement.

## 5. Validate Your Project

Check that everything is configured correctly:

```bash
streamt validate
```

You should see:

```
✓ Project 'my-first-pipeline' is valid

  Sources:  1
  Models:   1
  Tests:    0
  Exposures: 0
```

## 6. View the Lineage

See how data flows through your pipeline:

```bash
streamt lineage
```

Output:

```
raw_events (source)
    └── events_clean (topic)
```

## 7. Plan the Deployment

See what will be created:

```bash
streamt plan
```

Output:

```
Plan: 1 to create, 0 to update, 0 to delete

Topics:
  + events.clean.v1 (6 partitions, replication: 1)
```

## 8. Deploy!

Apply your pipeline to the infrastructure:

```bash
streamt apply
```

Output:

```
Applying changes...

Topics:
  + events.clean.v1 ............... created

Applied: 1 created, 0 updated, 0 unchanged
```

## 9. Verify in Conduktor

Open [http://localhost:8080](http://localhost:8080) and log in with:

- **Email:** admin@localhost
- **Password:** Admin123!

You should see the `events.clean.v1` topic in the Topics view.

## 10. Add a Test

Create a test to validate data quality:

```yaml title="tests/events_test.yml"
tests:
  - name: events_schema_validation
    model: events_clean
    type: schema
    assertions:
      - not_null:
          columns: [event_id, user_id, event_type]
      - accepted_values:
          column: event_type
          values: [click, view, purchase, signup]
```

Run the test:

```bash
streamt test
```

---

## Project Structure

Your project should now look like this:

```
my-streaming-project/
├── stream_project.yml      # Main configuration
├── sources/
│   └── events.yml          # Source definitions
├── models/
│   └── events_clean.yml    # Model definitions
└── tests/
    └── events_test.yml     # Test definitions
```

### Single-File vs Multi-File

streamt supports both layouts:

| Layout | When to use |
|--------|-------------|
| **Single-file** (`stream_project.yml` with everything) | Small projects, quick prototyping, < 5 models |
| **Multi-file** (separate `sources/`, `models/`, `tests/` dirs) | Team projects, > 5 models, better git diffs |

Both are equivalent. streamt auto-discovers YAML files in subdirectories. You can also mix: keep sources inline in `stream_project.yml` and split models into `models/`. Subdirectory nesting works too (`models/payments/orders.yml`).

## Bonus: Inspect Your Pipeline

Use `list` and `show` to explore what you've built:

```bash
# List all models
streamt list models

# Show details of a specific model
streamt show model events_clean

# Get JSON output (for scripting or LLM agents)
streamt -o json list sources
streamt -o json show model events_clean
```

## What's Next?

Congratulations! You've created your first streaming pipeline with streamt.

- [Build a complete pipeline](first-pipeline.md) — Add stateful processing with Flink
- [Learn about concepts](../concepts/overview.md) — Understand sources, models, tests
- [Explore materializations](../reference/materializations.md) — Topics, Flink jobs, sinks
- [See examples](../examples/payments.md) — Real-world pipeline examples
- [CI/CD Integration](../guides/ci-cd.md) — GitHub Actions, validation in PRs
- [CLI Reference](../reference/cli.md) — All commands and options
