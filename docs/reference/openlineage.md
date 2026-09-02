# OpenLineage static export

`streamt docs openlineage` emits deterministic OpenLineage 1.53.0 design
metadata for one successfully compiled streamt project. It writes one
`DatasetEvent` or `JobEvent` JSON object per line and validates the complete
event sequence before any event bytes are written.

This is a static metadata export. It does not connect to Kafka, Schema
Registry, Flink, Connect, Gateway administration, deployment state, an
OpenLineage backend, or any other network service.

## Complete project example

This example is parsed and exported by the same production CLI in the test
suite, with network access disabled:

```yaml
# streamt:openlineage-example
apiVersion: streamt.dev/v1alpha1
project:
  name: payments-streams
  version: 2.3.0
  description: Payment processing project
runtime:
  kafka:
    bootstrap_servers: broker.example:9092
sources:
  - name: payments_raw
    topic: payments.raw.v1
    description: Raw payment events
    owner: payments-platform
    columns:
      - name: payment_id
        type: STRING
        description: Stable payment identifier
models:
  - name: payments_clean
    materialized: topic
    sql: |
      SELECT payment_id
      FROM {{ source("payments_raw") }}
    description: Validated payment stream
    owner: payments-platform
    topic:
      name: payments.clean.v1
    contract:
      enforced: true
      columns:
        - name: payment_id
          type: STRING
          nullable: false
```

Export it with an explicit job namespace:

```bash
streamt docs openlineage \
  --job-namespace https://lineage.example/namespaces/prod \
  --output-file payments.openlineage.jsonl
```

The Kafka dataset namespace in this example is safely derived as
`kafka://broker.example:9092`. Use `--kafka-namespace` when the runtime address
is not the stable catalog identity or when Kafka has multiple bootstrap
endpoints.

## Exported metadata

Every declared source and every output-bearing model becomes one
`DatasetEvent`. The dataset identity is the exact physical topic name paired
with its Kafka namespace. Virtual-topic outputs use the separate Gateway
namespace. Sink connectors do not have a normalized output dataset.

Schema fields preserve declaration order, name, type, and description. Sources
use `source.columns`. Models use `contract.columns` whenever a contract exists,
including an explicitly empty contract; otherwise they use `model.columns`.
When no fields are available, the dataset remains in the export, the schema
facet is omitted, and streamt reports
`W110_OPENLINEAGE_SCHEMA_INCOMPLETE`.

Descriptions and human owners become documentation and ownership facets.
streamt does not invent an ownership role. Dataset events also identify the
dataset type as `TOPIC`.

Each compiled model process becomes one `JobEvent` with job type `MODEL`,
integration `STREAMT`, and processing type `STREAMING`:

- a topic model with SQL and a `flink` model produce Flink processing jobs;
- a virtual-topic model produces a Gateway processing job;
- a sink model produces a Connect processing job;
- a topic declaration without SQL is a dataset only and has no job event.

Job inputs are the model's direct resolved dependencies, not the transitive
closure. A Gateway job consumes its exact ordinary-Kafka physical input and
produces the virtual dataset in the Gateway namespace. Chained Gateway virtual
topics are not supported. A sink job has inputs but no normalized output and
reports `W111_OPENLINEAGE_SINK_OUTPUT_OMITTED`.

The exporter fails closed on ambiguous or colliding dataset and job identities,
duplicate schema fields, unsupported dependency shapes, and any input/output
identity collision.

## Namespace selection

Namespaces use this precedence, with the first selected value winning:

| Identity | Command option | Environment value | Narrow derivation |
| --- | --- | --- | --- |
| Jobs | `--job-namespace` | `OPENLINEAGE_NAMESPACE` | None; one value is required |
| Kafka datasets | `--kafka-namespace` | `STREAMT_OPENLINEAGE_KAFKA_NAMESPACE` | One unambiguous `runtime.kafka.bootstrap_servers` endpoint |
| Gateway datasets | `--gateway-namespace` | `STREAMT_OPENLINEAGE_GATEWAY_NAMESPACE` | One unambiguous `runtime.conduktor.gateway.proxy_bootstrap` endpoint |

Project and environment-specific `.env` files are loaded before the namespace
environment values are selected. An invalid higher-precedence value is an
error; streamt does not silently fall through to a lower-precedence value.
Explicit dataset namespace values are validated even when that namespace is
not needed by the project.

Dataset namespaces must be absolute `kafka://host:port` URIs with exactly one
authority and no user information, path, query, or fragment. A comma-separated
bootstrap list is intentionally not converted into a catalog identity. Job
namespaces must be non-blank and are never inferred from a path, user name, or
project name.

## Output behavior

Without `--output-file`, text-mode stdout is canonical UTF-8 JSON Lines:

1. `DatasetEvent` records sorted by dataset namespace and name;
2. `JobEvent` records sorted by job namespace and name;
3. compact JSON with recursively sorted keys and one final newline.

Warnings go to stderr, leaving stdout directly pipeable. With
`--output-file`, streamt validates and serializes everything first, stages the
file in the target directory, flushes and fsyncs it, then atomically replaces
the target. A failure preserves an existing target and removes the staging
file.

Global JSON mode returns the normal streamt envelope instead of JSONL:

```bash
streamt --output json docs openlineage \
  --job-namespace https://lineage.example/namespaces/prod
```

`data` contains `standard`, `release`, `core_schema`, canonical `events`, and
total/dataset/job `counts`; it also contains `output_file` when requested.
Warnings remain in the envelope's top-level `warnings` array and are not
duplicated under `data`.

## Validation and security boundary

Every generated event validates offline against the bundled official
OpenLineage 1.53.0 core schema and every standard facet schema streamt emits.
streamt also checks identity, field, event-shape, and ordering invariants that
the official schemas do not fully express. The packaged schemas are pinned by
exact size and SHA-256 and the validator has no network retrieval fallback.

The exporter compiles once in dry-run mode and consumes only the safe compiled
identity projection. It does not traverse or serialize SQL, macro source,
runtime credentials, connector configuration, reviewed plans, deployment
state, request headers, or environment variables. Kafka and Gateway host/port
namespaces are intentional dataset identities. Any mapping, validation,
serialization, or output failure uses `E506_OPENLINEAGE_INVALID` with a safe
location when available; no partial event stream is emitted.

## Intentional non-support

Static export does not emit `RunEvent` records. Ordinary `compile`, `plan`,
`apply`, and `test` commands do not emit OpenLineage telemetry, and streamt does
not claim lifecycle telemetry for deployed Flink, Gateway, Kafka Connect, or
topic processes. Transport configuration, backend delivery, field lineage,
live schema enrichment, connector-specific sink datasets, catalog sync, and
round-trip editing remain unsupported.

See the [normative integration contract](../specs/openlineage-integration.md)
for the complete event, facet, identity, and future-runtime design.
