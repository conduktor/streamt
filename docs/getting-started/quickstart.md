---
title: Quick Start
description: Get up and running with streamt in 5 minutes
---

# Quick Start

Get up and running with streamt in 5 minutes. By the end of this guide, you'll have a working streaming pipeline.

## Prerequisites

- [streamt installed](installation.md)
- Docker running (for local Kafka)

## 1. Start Local Infrastructure

```bash
# Start Kafka, Flink, and supporting services
docker compose up -d

# Wait for services to be healthy
docker compose ps
```

## 2. Create Your Project

Create a new directory for your streamt project:

```bash
mkdir my-streaming-project
cd my-streaming-project
```

Create the main configuration file:

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
    advanced:
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

## What's Next?

Congratulations! You've created your first streaming pipeline with streamt.

- [Build a complete pipeline](first-pipeline.md) — Add stateful processing with Flink
- [Learn about concepts](../concepts/overview.md) — Understand sources, models, tests
- [Explore materializations](../reference/materializations.md) — Topics, Flink jobs, sinks
- [See examples](../examples/payments.md) — Real-world pipeline examples
