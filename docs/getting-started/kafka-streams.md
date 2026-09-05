---
title: Kafka Streams starter
description: Create a managed streaming topology with Kafka, SQL, and a declared custom application
---

# Kafka Streams starter

This starter creates a raw input topic, a SQL projection/filter, and its output
topic. A downstream custom application appears in the same project with its
consumed columns. You can review those dependencies before deploying anything.

```text
raw_orders (managed topic)
  -> eligible_orders (managed topic + Kafka Streams runner)
    -> fraud_app (external application declaration; metadata only)
```

The SQL runner does not need Flink or ksqlDB. It executes a restricted plan with
STRING, BOOLEAN, and signed 64-bit BIGINT fields in raw JSON records. Joins,
windows, aggregations, nested JSON, and arbitrary functions are not supported.
The [Flink walkthrough](quickstart.md) remains available, and `streamt init`
without an executor choice keeps its existing Flink-oriented starter.

!!! warning "Create/no-op only"

    The supported CLI path creates a new topology and observes an unchanged
    deployment. Updates to a running model are blocked pending replacement and
    recovery support. A custom application declaration does not deploy its code.

## Choose the local runtime explicitly

[Install streamt](installation.md), then inspect the packaged build contract:

```bash
streamt -o json runtime build --dry-run
```

This command needs neither a project nor running infrastructure. To build the
runner image, start local Docker Engine with Buildx and run:

```bash
streamt -o json runtime build
```

The build downloads pinned base images and Maven dependencies. It builds on
the local Docker daemon and returns an immutable `data.image` SHA-256 ID. It
does not publish or tag an image. The build inputs are included in the installed
package; a repository checkout and a host Java installation are not required.

## Generate the project

Use the returned image ID in this command. Replace the example broker addresses
and network name with your own existing local infrastructure:

```bash
streamt init --project-dir orders --project-name orders \
  --executor kafka_streams \
  --runner-image 'sha256:<64 hexadecimal characters from data.image>' \
  --kafka localhost:9092 \
  --kafka-internal broker:19092 \
  --docker-network streaming-local \
  --initial-offset earliest
cd orders
```

`--runner-image` is required and must be an immutable image ID or repository
digest. Init checks its format but does not inspect, build, or pull the image.
It writes `stream_project.yml`, `README.md`, `sample_events.jsonl`, and the
existing `sources/`, `models/`, and `tests/` directories. Add `--dry-run` to
preview filenames without writes. Existing starter files are protected unless
you explicitly pass `--force`.

No Docker/Kafka resources are created during init. The two broker addresses
describe different clients: your CLI connects from the host; the runner connects
from its container. The configured network must be a local Docker bridge
network. If the internal address uses a container name such as `broker`, use a
user-defined bridge with that name resolvable there. Docker's default `bridge`
does not provide service-name DNS. See
[Docker bridge networking](https://docs.docker.com/engine/network/drivers/bridge/).

The starter is plaintext and uses one partition with replication factor one.
Those settings are for local testing. A single-broker fixture also needs Kafka's
transaction and consumer-offset internal-topic replication settings configured
for that broker. TLS/SASL broker acceptance and production hardening are not
established by this walkthrough.

## Read and validate the topology

The input is a managed model without SQL. Creating a new topic does not create
a producer or seed any data. The SQL model contains:

```yaml
name: eligible_orders
ownership:
  mode: managed
materialized: topic
executor: kafka_streams
sql: |
  SELECT id, amount
  FROM {{ ref("raw_orders") }}
  WHERE amount >= 100 AND paid = TRUE
```

`fraud_app` consumes this model and declares that it needs `id: STRING` and
`amount: BIGINT`. A projected-column change can therefore surface a broken
application declaration in offline validation. The custom application's code,
scheduling, and health remain outside streamt's control.

```bash
streamt validate --strict
streamt lineage
streamt compile --dry-run
streamt plan --offline
```

The offline plan assumes the managed resources are absent: two topics and one
Kafka Streams job. It does not contact Kafka/Docker or compare live state.
Compilation produces no Flink job and needs no Schema Registry for this path.

## Create and verify

After the local runner image, broker, and network are ready, review a live plan:

```bash
streamt plan --out reviewed-plan.json
streamt apply --plan reviewed-plan.json
streamt status --health
streamt plan
```

An unchanged second plan should be a no-op. On first creation, streamt records
operation intent before initializing source partitions at the explicit starting
position. The runner uses `auto.offset.reset=none`; missing or out-of-range offsets fail
instead of silently replaying or skipping data.

Preflight waits for a fresh broker's group coordinator with read-only requests
under one deadline. If readiness or group absence cannot be established, creation
fails before runtime mutation. Streamt does not retry an uncertain offset write.

The generated README contains producer/consumer commands for the four records
in `sample_events.jsonl`. Only `{"id":"large-paid","amount":150}` should reach
`orders.eligible.orders.v1` from that fresh sample. Read the output with
`isolation.level=read_committed`. Producing the file again appends new records;
it is not a deduplicated seed operation.

Raw values must contain exactly `id`, `amount`, and `paid`. A nullable amount
is explicit JSON null. Missing/extra fields, malformed JSON, wrong types, or
out-of-range integers stop the runner. Kafka keys are preserved; tombstones
are dropped. These are append-stream semantics, not table/upsert semantics.

## Start from an existing topic

The other entry point is [discovery and import](../reference/cli.md). Imported
sources remain external; discovery does not adopt their lifecycle or infer an
unknown raw-JSON contract. Review their topic names and add matching declared
columns before creating a SQL consumer.

The generated README includes this variant: replace the managed `raw_orders`
input with an imported external source, then use `source()` instead of `ref()`.
The managed output and SQL runner remain in the same topology. Streamt does
not create, reconfigure, seed, or continuously reconcile the external source.
Runner preflight still checks the source identity and offsets needed by the
managed consumer.
