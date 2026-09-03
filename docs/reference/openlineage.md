# OpenLineage integration

`streamt docs openlineage` emits deterministic OpenLineage 1.53.0 design
metadata for one successfully compiled streamt project. It writes one
`DatasetEvent` or `JobEvent` JSON object per line and validates the complete
event sequence before any event bytes are written.

The static command does not connect to Kafka, Schema Registry, Flink, Connect,
Gateway administration, deployment state, an OpenLineage backend, or any other
network service. Separately, explicitly enabled `streamt apply` and `streamt
test` commands can send validated finite command lifecycles through an
explicitly configured bounded File or HTTP transport.

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

## Finite test-command run events

Runtime emission requires an explicit flag; transport environment variables
alone never enable it:

```bash
streamt test --emit-openlineage \
  --openlineage-job-namespace https://lineage.example/namespaces/prod
```

The four runtime options are `--emit-openlineage`,
`--openlineage-job-namespace`, `--openlineage-kafka-namespace`, and
`--openlineage-gateway-namespace`. Namespace options override the same
environment values used by static export. Project `.env` files are applied
first. Sample tests consume `runtime.kafka`, so their dataset identity always
uses the Kafka namespace, never the Gateway namespace.

One non-empty selected invocation creates one aggregate job named
`streamt/{project-segment}/commands/test`, one random UUIDv4 run, and job type
`BATCH` / `STREAMT` / `TEST`. Its inputs are the sorted unique physical topics
actually consumed by the selected sample tests. Schema and continuous tests
may participate in the aggregate run but add no dataset inputs. Coverage, an
empty selection, and the reserved `--deploy` path open no transport and claim
no run.

streamt validates START and every possible terminal shape before constructing
the test runner, then attempts START immediately before execution. Every result
passing produces COMPLETE; a non-passing result or uncaught execution exception
produces FAIL; interruption produces ABORT. The pair keeps the same run, job,
facets, and inputs. FAIL contains only a fixed generic error-message facet—test
assertion details, SQL, configuration, and credentials are never copied.

## Durable apply-command run events

Apply telemetry is strictly opt-in. Transport environment variables alone do
not enable it:

```bash
streamt apply --env prod \
  --plan prod.plan.json \
  --confirm-env prod \
  --emit-openlineage \
  --openlineage-job-namespace https://lineage.example/namespaces/prod
```

The four runtime options are `--emit-openlineage`,
`--openlineage-job-namespace`, `--openlineage-kafka-namespace`, and
`--openlineage-gateway-namespace`. A namespace option overrides its matching
environment value. Project and environment-specific `.env` files are loaded
before those values are selected. The job namespace falls back to
`OPENLINEAGE_NAMESPACE` and has no inferred default. Explicit Kafka and Gateway
namespaces fall back to `STREAMT_OPENLINEAGE_KAFKA_NAMESPACE` and
`STREAMT_OPENLINEAGE_GATEWAY_NAMESPACE`; supplied values are validated, but an
apply run does not derive or emit dataset identities.

One durable apply operation produces a job named
`streamt/{project-segment}/commands/apply` with job type `BATCH` / `STREAMT` /
`COMMAND`. Its OpenLineage run ID is exactly the UUIDv4 already persisted in
the deployment `OperationIntent`, and START uses that intent's exact durable
`started_at` time. START is attempted only after the intent is durably written
and before the first planner action or provider mutation.

COMPLETE is attempted immediately after ownership state is successfully
committed and the durable operation marker is cleared. A verified commit whose
state-authority release subsequently fails still truthfully emits COMPLETE;
the CLI separately returns `E426_STATE_RELEASE_FAILED_AFTER_COMMIT` with
`committed: true`, and the operation must not be replayed. A runtime failure,
unknown commit, authority loss before confirmed commit, or recovery-required
outcome produces FAIL after streamt makes its existing best effort to persist
the conservative recovery marker. `KeyboardInterrupt` after START produces
ABORT. FAIL contains only the fixed generic message `streamt apply command did
not complete successfully`.

Parse, validation, confirmation, review, safety, planning, existing-recovery,
final state-drift, OpenLineage-preflight, and `--dry-run` exits that occur
before `begin_operation` emit no RunEvent. A zero-action apply does begin and
complete a durable operation, so it emits a normal START/COMPLETE pair. Each
later apply has a fresh durable operation and run UUID.

Apply RunEvents describe only the finite streamt control-plane command. They
contain no inputs or outputs and no action, plan, artifact, SQL, provider,
state-location, runtime-configuration, or credential data. COMPLETE does not
claim that a submitted Flink job or another deployed streaming workload
finished, reached RUNNING, or processed any data.

## Runtime transport configuration

Runtime emission requires either an explicit UTF-8 YAML file named by
`OPENLINEAGE_CONFIG` or the exact supported nested environment fields. This
local append-only File configuration writes one durably synchronized JSON event
per line:

```yaml
# streamt:skip
transport:
  type: file
  log_file_path: /var/log/streamt/openlineage.jsonl
```

```bash
OPENLINEAGE_CONFIG=/etc/streamt/openlineage.yml \
  streamt apply --env prod --plan prod.plan.json --confirm-env prod \
  --emit-openlineage \
  --openlineage-job-namespace https://lineage.example/namespaces/prod
```

An HTTP transport can instead be supplied entirely by the environment:

```bash
export OPENLINEAGE__TRANSPORT__TYPE=http
export OPENLINEAGE__TRANSPORT__URL=https://lineage.example
export OPENLINEAGE__TRANSPORT__ENDPOINT=api/v1/lineage
export OPENLINEAGE__TRANSPORT__TIMEOUT=5
export OPENLINEAGE__TRANSPORT__VERIFY=true
export OPENLINEAGE__TRANSPORT__RETRY__TOTAL=1

streamt apply --env prod --plan prod.plan.json --confirm-env prod \
  --emit-openlineage \
  --openlineage-job-namespace https://lineage.example/namespaces/prod
```

Optional API-key authentication requires HTTPS plus
`OPENLINEAGE__TRANSPORT__AUTH__TYPE=api_key` and
`OPENLINEAGE__TRANSPORT__AUTH__APIKEY` supplied by the process secret manager.
The URL must contain no credentials. Certificate verification cannot be
disabled; plain HTTP is accepted only for loopback; redirects, proxy and
`.netrc` inheritance, and adapter-level retries are disabled. Timeout is at
most five seconds, and total retry count is zero or one.

Only File and synchronous HTTP are supported. Console, Kafka, composite,
asynchronous, remote-filesystem, and custom Python transports are rejected, as
are legacy `OPENLINEAGE_URL` and `OPENLINEAGE_API_KEY`. An explicit
`OPENLINEAGE_DISABLED=true` conflicts with `--emit-openlineage`.

Namespace, event, and transport preflight failures are fatal
`E506_OPENLINEAGE_INVALID` errors before samples are consumed or apply can
write its durable intent and mutate a provider. After START is attempted,
delivery and close are best effort: failures add only the bounded,
secret-neutral `W112_OPENLINEAGE_EMIT_FAILED` warning without changing a real
test/apply result, rollback, recovery marker, exit code, or original exception.
There is no required-delivery mode or durable outbox.

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

Static export does not emit `RunEvent` records. Ordinary `compile`, `plan`, and
`apply` without `--emit-openlineage` do not emit OpenLineage telemetry. Runtime
command events are limited to explicitly enabled finite `apply` and `test`
invocations. streamt does not claim lifecycle telemetry for deployed Flink,
Gateway, Kafka Connect, or topic processes. Field lineage, live schema
enrichment, connector-specific sink datasets, catalog sync, round-trip editing,
required delivery, and transports beyond File/HTTP remain unsupported.

See the [normative integration contract](../specs/openlineage-integration.md)
for the complete event, facet, identity, and future-runtime design.
