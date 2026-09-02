# OpenLineage integration

## Status

The design-time `DatasetEvent` and `JobEvent` export is supported against its
executable acceptance gates. The remaining normative contract for narrowly
scoped streamt command telemetry is proposed and must not be described as
supported until its separate acceptance requirements are executable.

The first implementation is static export. Runtime command events are separate
later slices. Neither static export nor command telemetry is deployed Apache
Flink runtime telemetry.

## Purpose

streamt already has authoritative facts about declared Kafka datasets, compiled
model processes, direct dependencies, schemas, documentation, and human owners.
It also has authoritative lifecycle boundaries for its own finite `apply` and
`test` commands.

The integration exposes only those facts:

- design-time `DatasetEvent` records for declared Kafka datasets;
- design-time `JobEvent` records for declared streamt model processes;
- later, finite `RunEvent` pairs for streamt's own `apply` and `test` commands.

The integration does not infer deployed Flink run identity, completion, record
counts, connector destination identities, or data-processing facts that streamt
does not observe.

## Normative language

The words **must**, **must not**, **should**, and **may** are normative. An
omission is preferable to an invented value. An ambiguity in an identity or a
field that the exporter claims to support is an error unless this specification
defines an explicit warning boundary.

## Pinned OpenLineage release and schemas

The first supported target is the official **OpenLineage 1.53.0** release:

- Release: <https://github.com/OpenLineage/OpenLineage/releases/tag/1.53.0>
- Signed release commit:
  [`8ad5c14c63fbab63fedd8ff42f9a208d86ad07fe`](https://github.com/OpenLineage/OpenLineage/commit/8ad5c14c63fbab63fedd8ff42f9a208d86ad07fe)
- Object model at the release commit:
  <https://github.com/OpenLineage/OpenLineage/blob/8ad5c14c63fbab63fedd8ff42f9a208d86ad07fe/website/docs/spec/object-model.md>
- Naming conventions at the release commit:
  <https://github.com/OpenLineage/OpenLineage/blob/8ad5c14c63fbab63fedd8ff42f9a208d86ad07fe/website/docs/spec/naming.md>
- Run cycle at the release commit:
  <https://github.com/OpenLineage/OpenLineage/blob/8ad5c14c63fbab63fedd8ff42f9a208d86ad07fe/website/docs/spec/run-cycle.md>
- Upstream license: Apache License 2.0.

The release version and the core JSON Schema version are different identifiers.
OpenLineage 1.53.0 continues to publish the core Draft 2020-12 schema with `$id`
`https://openlineage.io/spec/2-0-2/OpenLineage.json`.

The implementation must pin the following immutable upstream bytes. Checksums
and sizes apply to the uncompressed JSON bytes.

| Artifact | Official schema `$id` | Bytes | SHA-256 |
| --- | --- | ---: | --- |
| [`OpenLineage.json`](https://raw.githubusercontent.com/OpenLineage/OpenLineage/8ad5c14c63fbab63fedd8ff42f9a208d86ad07fe/spec/OpenLineage.json) | `https://openlineage.io/spec/2-0-2/OpenLineage.json` | 9155 | `69f68bee00b9beac88a87059c0102410e7bb05f3f43c46d02a0409831eceb0d2` |
| [`JobTypeJobFacet.json`](https://raw.githubusercontent.com/OpenLineage/OpenLineage/8ad5c14c63fbab63fedd8ff42f9a208d86ad07fe/spec/facets/JobTypeJobFacet.json) | `https://openlineage.io/spec/facets/2-0-4/JobTypeJobFacet.json` | 3072 | `11c12cab95a411ca31066c80d2bb4aefd37bbcadbda5e4d343d2069853b907d5` |
| [`SchemaDatasetFacet.json`](https://raw.githubusercontent.com/OpenLineage/OpenLineage/8ad5c14c63fbab63fedd8ff42f9a208d86ad07fe/spec/facets/SchemaDatasetFacet.json) | `https://openlineage.io/spec/facets/1-2-0/SchemaDatasetFacet.json` | 1687 | `50236a779aa64baa0bad0055391838bd22fcb36ce667c41d60ada80915e899b6` |
| [`DocumentationDatasetFacet.json`](https://raw.githubusercontent.com/OpenLineage/OpenLineage/8ad5c14c63fbab63fedd8ff42f9a208d86ad07fe/spec/facets/DocumentationDatasetFacet.json) | `https://openlineage.io/spec/facets/1-1-0/DocumentationDatasetFacet.json` | 1031 | `bad5c041d679e73b2faea43506860428f48d80710ad1354a2d2393475143285f` |
| [`DocumentationJobFacet.json`](https://raw.githubusercontent.com/OpenLineage/OpenLineage/8ad5c14c63fbab63fedd8ff42f9a208d86ad07fe/spec/facets/DocumentationJobFacet.json) | `https://openlineage.io/spec/facets/1-1-0/DocumentationJobFacet.json` | 944 | `b5823685c20c712d9ee1e3b310ad6ca426be7b46db60acd9f23248811af3c8d4` |
| [`DatasetTypeDatasetFacet.json`](https://raw.githubusercontent.com/OpenLineage/OpenLineage/8ad5c14c63fbab63fedd8ff42f9a208d86ad07fe/spec/facets/DatasetTypeDatasetFacet.json) | `https://openlineage.io/spec/facets/1-0-1/DatasetTypeDatasetFacet.json` | 1008 | `1a7d5106877151d52c4d3967f77b92788ea42ebf9843c6573d776a63c9a7d157` |
| [`OwnershipDatasetFacet.json`](https://raw.githubusercontent.com/OpenLineage/OpenLineage/8ad5c14c63fbab63fedd8ff42f9a208d86ad07fe/spec/facets/OwnershipDatasetFacet.json) | `https://openlineage.io/spec/facets/1-0-1/OwnershipDatasetFacet.json` | 1368 | `9a18a508746627eff18fa84ca1694e1a9f2d556ac0052dbdb6970d8a35f75231` |
| [`OwnershipJobFacet.json`](https://raw.githubusercontent.com/OpenLineage/OpenLineage/8ad5c14c63fbab63fedd8ff42f9a208d86ad07fe/spec/facets/OwnershipJobFacet.json) | `https://openlineage.io/spec/facets/1-0-1/OwnershipJobFacet.json` | 1344 | `c54ae771b1183efbe007a778eb5bf05e9e3f39f3d48f22a4beceea00950abdad` |
| [`ErrorMessageRunFacet.json`](https://raw.githubusercontent.com/OpenLineage/OpenLineage/8ad5c14c63fbab63fedd8ff42f9a208d86ad07fe/spec/facets/ErrorMessageRunFacet.json) | `https://openlineage.io/spec/facets/1-0-1/ErrorMessageRunFacet.json` | 1501 | `b11b4ee8b0f99f6846264f87cad48390255c7ef142ce623216660c7097be0ea6` |

The initial bundle is 21110 uncompressed bytes. Every artifact must be stored as
a gzip-compressed, base64-encoded package resource beneath
`src/streamt/docs/schemas/`. Loading must verify the decoded size and checksum
before JSON parsing. Wheels and source distributions must contain the resources.
Validation must never fetch a schema or reference from the network.

OpenLineage 1.53.0 also introduced `LineageFacet.json`. It is not used by the
initial table-level exporter because `JobEvent.inputs` and `JobEvent.outputs`
already express the declared relationships without duplication. If the later
field-lineage slice is implemented, it must separately pin this artifact:

- Immutable source:
  <https://raw.githubusercontent.com/OpenLineage/OpenLineage/8ad5c14c63fbab63fedd8ff42f9a208d86ad07fe/spec/facets/LineageFacet.json>
- `$id`: `https://openlineage.io/spec/facets/1-0-0/LineageFacet.json`
- Uncompressed size: `8691` bytes.
- SHA-256:
  `a60708365707faaa3d430f0f1465991abda3ed0dfd562d8eb614d38a603acf13`.

## Capability boundaries by command

| streamt activity | OpenLineage event | Boundary |
| --- | --- | --- |
| `docs openlineage` | `DatasetEvent`, `JobEvent` | Explicit design-time export only |
| ordinary `compile` | None | Compilation remains local and side-effect free |
| `plan` | None | A desired/live diff is not a data-processing run |
| `apply --emit-openlineage` | Later `RunEvent` pair | The finite streamt control-plane command, not deployed Flink |
| `test --emit-openlineage` | Later `RunEvent` pair | The finite streamt test invocation |
| `observe` and `status` | None | Flink status and IDs do not establish an OpenLineage run cycle |
| deployed Flink, Gateway, or Connect process | None | streamt does not observe a correlated runtime lifecycle |

The support matrix and roadmap must distinguish static export from command-run
telemetry. Neither may be used to claim deployed streaming runtime telemetry.

## Static export command

The initial command surface is:

```text
streamt docs openlineage \
  --job-namespace <NAMESPACE> \
  [--kafka-namespace <KAFKA-URI>] \
  [--gateway-namespace <KAFKA-URI>] \
  [--output-file <PATH>] \
  [--project-dir <PATH>] \
  [--env <ENVIRONMENT>]
```

`--job-namespace` is semantically required unless `OPENLINEAGE_NAMESPACE`
supplies it. It must be checked inside the command rather than declared as a
Click-required option so global structured JSON mode receives a normal streamt
error.

Namespace precedence is:

1. the corresponding command option;
2. `OPENLINEAGE_NAMESPACE` for jobs,
   `STREAMT_OPENLINEAGE_KAFKA_NAMESPACE` for Kafka datasets, or
   `STREAMT_OPENLINEAGE_GATEWAY_NAMESPACE` for Gateway datasets;
3. the narrow automatic derivation defined below for dataset namespaces only.

There is no default job namespace and no OpenLineage-specific project DSL in
the first release. In particular, local project paths, user names, and a bare
project name must not become implicit namespaces.

Any OpenLineage mapping, validation, serialization, or output failure uses the
planned stable code `E506_OPENLINEAGE_INVALID`, exits non-zero, and includes a
safe streamt field or JSON path when available.

## Producer and schema URLs

Every event uses the producer URI:

```text
https://github.com/conduktor/streamt
```

Every standard facet uses the same value in `_producer`. Event `schemaURL`
values are exact:

```text
https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/DatasetEvent
https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/JobEvent
https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunEvent
```

Facet `_schemaURL` values are their official `$id` followed by the exact `$defs`
fragment for the emitted facet. Mutable Git branches and release-documentation
URLs must not appear as event or facet schema URLs.

## Dataset identity

An OpenLineage dataset identity is the pair `(namespace, name)`.

For an ordinary Kafka topic:

- `namespace` is `kafka://{bootstrap-host}:{port}`;
- `name` is the exact physical topic name.

An explicit Kafka namespace must be an absolute `kafka://` URI with one
authority, an explicit port, and no user information, path, query, or fragment.
The exporter preserves its exact spelling after validation. It must not expose
credentials in a namespace.

Automatic derivation is allowed only when `runtime.kafka.bootstrap_servers`
contains exactly one unambiguous `host:port` endpoint. Whitespace around that
endpoint is discarded and `kafka://` is prepended. The internal bootstrap
address is not used as a catalog identity.

A multi-broker list does not define a stable cluster identity. Without an
explicit namespace it is an error. The exporter must not select the first
broker, sort or hash the broker list, use a Schema Registry URL, or invent a
cluster alias.

A Gateway virtual topic is addressed through the Gateway Kafka proxy, not the
backing broker endpoint. Its output dataset therefore uses the explicit Gateway
namespace or a namespace derived from exactly one unambiguous
`runtime.conduktor.gateway.proxy_bootstrap` endpoint. Its physical backing input
retains the ordinary Kafka namespace. A downstream model reference to a virtual
topic uses the Gateway dataset identity.

`Source.cluster` does not currently resolve to multiple Kafka runtime objects,
so it does not alter dataset identity in this release.

## Stable job identity

An OpenLineage job identity is the pair `(job namespace, job name)`. The job
namespace is the explicit value resolved above. Callers are responsible for
using different namespaces when two environments must remain distinct.

Job names are stable and do not include a timestamp, run ID, filesystem path,
project version, backend URL, or compiler artifact suffix:

```text
streamt/{project-segment}/models/{model-segment}
streamt/{project-segment}/commands/apply
streamt/{project-segment}/commands/test
```

Each segment is UTF-8 percent-encoded independently using only `-._~`, ASCII
letters, and ASCII digits as unescaped characters. Hexadecimal escapes are
uppercase. Case in the original value is preserved. This prevents `/`, `%`, and
separator characters in names from creating collisions.

The exporter fails closed if two declarations or commands resolve to the same
job identity.

## Dataset inventory

One `DatasetEvent` is emitted for each unique declared dataset:

- every source topic;
- every compiled `topic` model output;
- every compiled `flink` model output topic;
- every compiled Gateway virtual-topic output.

Physical output names come from compiled artifacts so explicit topic names and
effective materialization fallbacks match deployment behavior. Sink models do
not get an invented output dataset.

If two distinct declarations resolve to the same dataset identity, export
fails. The exporter does not merge their descriptions, schemas, or owners.
Repeated references to one already resolved declaration are deduplicated.

Dataset events are design metadata. They do not assert that a topic exists,
contains records, uses a particular serializer, or has been deployed.

## Dataset facet mapping

Static dataset metadata appears on `DatasetEvent.dataset.facets`. Dataset
references in `JobEvent.inputs` and `JobEvent.outputs` contain only `namespace`
and `name`; the exporter does not duplicate static facets on every job.

### Schema

The selected declared column source is:

1. `source.columns` for a source;
2. `model.contract.columns` when a contract exists, including an explicitly
   empty contract;
3. otherwise `model.columns` when present;
4. otherwise no schema facet.

No SQL-inferred, generated Avro, registry-fetched, or fallback string schema is
used. An explicit empty contract is authoritative and does not fall back to
model columns.

Each non-empty selected column list becomes a `schema` facet. Fields preserve
declaration order and map as follows:

| OpenLineage field | streamt field | Rule |
| --- | --- | --- |
| `name` | column `name` | Exact, required, non-blank |
| `type` | column `type` | Exact declared Flink type string when present and non-blank |
| `description` | column `description` | Exact when present |
| `ordinal_position` | declaration position | One-based |

The exporter does not parse nested Flink types into nested OpenLineage fields.
Nullability, `required`, processing-time flags, primary keys, classifications,
and masking are not encoded in the schema facet.

Missing or explicitly empty columns omit the facet and report
`W110_OPENLINEAGE_SCHEMA_INCOMPLETE`. Duplicate emitted field names or malformed
declared values are errors.

### Documentation

A non-empty declared source or model description becomes a `documentation`
facet with the exact `description`. `contentType` is omitted because the DSL
does not declare it. A model description is attached to both its output dataset
and its job when both exist.

### Dataset type

Every emitted dataset receives a `datasetType` facet whose `datasetType` is
`TOPIC`. `subType` is omitted. The exporter does not invent `VIRTUAL`,
`MATERIALIZED`, or `EXTERNAL` subtypes.

### Ownership

A declared human `owner` becomes one `ownership.owners` entry whose `name` is
copied exactly. The optional OpenLineage ownership `type` is omitted because
streamt does not declare an ownership role.

For a source, the source owner applies to its dataset. For a model, the model
owner applies to both the model job and its output dataset. The separate
streamt lifecycle `ownership.mode` field is never mapped as a human owner.

## Static job mapping

One `JobEvent` is emitted per model that compiles to an actual processing
artifact:

- a Flink job, including a SQL `topic` model that needs a processor;
- a Gateway rule, including a policy-only virtual topic;
- a Kafka Connect sink connector.

A no-SQL topic-provisioning model has a dataset but no process and therefore no
job. Continuous test artifacts are not model jobs and are excluded from static
export.

Each model is one semantic streamt job even when compilation also creates its
output topic. Compiler-specific artifact names such as `_processor` do not
change the OpenLineage job identity.

The job has these facets:

- `jobType.processingType`: `STREAMING`;
- `jobType.integration`: `STREAMT`;
- `jobType.jobType`: `MODEL`;
- `documentation`: exact model description when present;
- `ownership`: exact model owner name without a role when present.

The first release does not emit `emissionPattern`: a design-time event does not
establish how an unobserved deployed process emits runtime events.

## Direct inputs and outputs

Job inputs are direct declared source and model dependencies, not transitive
ancestors. Logical dependencies are resolved to the physical dataset identities
defined above. Input ordering is canonical by `(namespace, name)`.

Job outputs are:

- the compiled Kafka topic for `topic` and `flink` outputs;
- the compiled virtual topic for Gateway outputs;
- absent for generic sink connectors.

A generic sink connector's external destination is connector-specific config,
not a normalized dataset identity. Its `JobEvent` contains the known Kafka
inputs, omits `outputs`, and reports `W111_OPENLINEAGE_SINK_OUTPUT_OMITTED`.
That warning is the only supported incomplete output relationship in the first
release.

The current DAG builder does not resolve references inside rendered macro SQL.
The exporter must reuse or expose compiler-resolved macro SQL before extracting
dependencies. It must not silently emit an empty input list for an unresolved
macro. A failure to resolve compiled macro dependencies is an export error.

`inputs` and `outputs` are omitted when empty. Duplicate references within one
list are deduplicated. A dataset appearing as both input and output for one job,
or two distinct logical dependencies resolving ambiguously to one identity, is
an error in the first release.

## Event time, run IDs, and determinism

All events from one static export use one timezone-aware UTC event time. When a
dry-run manifest is compiled for export, its `compiled_at` is the authoritative
time. The exact value is expected to differ across invocations.

Static job and dataset identities, event ordering, array ordering, facet
selection, and serialized key ordering are deterministic for the same parsed
project, namespaces, and event time. Tests inject or freeze time. The exporter
must not create deterministic content-derived timestamps.

Runtime command runs use random UUIDv4 identifiers. The `apply` event reuses the
UUID already persisted in `OperationIntent.operation_id`; `test` creates one
UUID per non-empty invocation. A repeated command is a distinct run and must
not use a checksum-derived UUID. UUIDv4 is valid under the pinned schema even
though current OpenLineage documentation recommends UUIDv7 for new clients.

Runtime event times are the actual UTC transition times. `apply` START uses the
durable operation's `started_at` value.

## Canonical event serialization

Text-mode static export is UTF-8 JSON Lines:

1. `DatasetEvent` records sorted by `(dataset.namespace, dataset.name)`;
2. `JobEvent` records sorted by `(job.namespace, job.name)`;
3. one compact JSON object per line with recursively sorted keys;
4. no insignificant spaces;
5. one final newline.

Without `--output-file`, stdout contains only those JSONL bytes. Warnings go to
stderr and must not corrupt the event stream.

`--output-file` atomically replaces the explicit path using a staging file in
the same directory, flushes and fsyncs it, and cleans every staging file on
error. Parent directories may be created. A failed export never leaves a
partial target.

Global `--output json` preserves the normal streamt envelope rather than
printing JSONL. `data` contains:

- `standard: "OpenLineage"`;
- `release: "1.53.0"`;
- `core_schema: "2-0-2"`;
- `events`, in canonical order;
- total, dataset, and job event counts;
- `output_file` when one was requested.

Structured warnings remain in the normal envelope's top-level `warnings`
array. They are not duplicated under `data`.

No progress text or warning line may precede or follow the structured envelope.

## Runtime command events

Runtime support is opt-in and is implemented only after static export and the
transport boundary pass their own gates.

The shared options are:

```text
--emit-openlineage
--openlineage-job-namespace <NAMESPACE>
--openlineage-kafka-namespace <KAFKA-URI>
--openlineage-gateway-namespace <KAFKA-URI>
```

The namespace options use the same environment fallbacks and identity rules as
static export. Merely setting transport environment variables does not enable
emission. `--emit-openlineage` is required.

### Apply

`apply` emits lifecycle events for the finite streamt deployment command:

- job name: `streamt/{project-segment}/commands/apply`;
- job type: `BATCH`, integration `STREAMT`, job type `COMMAND`;
- inputs and outputs: omitted, because infrastructure resources are not data
  consumed or produced by the command;
- run ID: exact durable `OperationIntent.operation_id`.

No event is emitted for parse, validation, review, safety, confirmation, or
dry-run exits that occur before a durable operation begins.

The START boundary is after `begin_operation` succeeds and before the first
runtime mutation. COMPLETE is created only after the state compare-and-swap
succeeds and the durable operation marker is cleared. Execution failure or a
recovery-required path produces FAIL. `KeyboardInterrupt` after START produces
ABORT. All events for the run use the same run and job identities.

The event describes streamt's control-plane execution only. It does not mean
that a submitted streaming job completed or even reached a running state.

### Test

`test` emits one aggregate run for one non-empty selected invocation:

- job name: `streamt/{project-segment}/commands/test`;
- job type: `BATCH`, integration `STREAMT`, job type `TEST`;
- run ID: one random UUIDv4;
- inputs: the canonical union of topics actually consumed by selected sample
  tests;
- outputs: omitted.

Schema tests currently perform structural assertion checks and do not read
Schema Registry data. Continuous tests currently poll Flink status and do not
execute the compiled test job. Neither contributes a dataset input. Coverage,
an empty selection, and the unimplemented `--deploy` path emit no run.

START is emitted immediately before the runner begins. COMPLETE means every
selected result passed. Any failed result or uncaught execution exception means
FAIL. `KeyboardInterrupt` means ABORT. Known sample inputs appear on START and
the terminal event so each event is useful independently.

### Failure facet

FAIL events may include `errorMessage` with:

- `message`: shared credential redaction applied, bounded to 4096 Unicode code
  points, with a generic fallback if redaction leaves it blank;
- `programmingLanguage`: `PYTHON`;
- no `stackTrace` in the first release.

Test assertion details, SQL, plan contents, connector config, request headers,
and credentials must not be copied into this facet.

## Transport boundary

Static stdout and atomic file export do not require the OpenLineage client.

Runtime support may add the optional dependency
`openlineage-python>=1.53,<1.54`. It must instantiate an explicit transport and
must not accept the Python client's implicit console fallback.

Runtime emission requires an explicit modern official configuration through
`OPENLINEAGE_CONFIG` or `OPENLINEAGE__TRANSPORT...`. The first release reads
only the transport section and rejects facet enrichment, tags, filters, dataset
normalization, and legacy `OPENLINEAGE_URL`/`OPENLINEAGE_API_KEY` aliases. Those
features could mutate an already validated event or broaden secret exposure.

The initially supported runtime transports are:

- a local append-only File transport that flushes each event;
- the synchronous HTTP transport, which posts one JSON event to the configured
  OpenLineage endpoint.

Console, Kafka, composite, asynchronous, remote-filesystem, and custom Python
transports are unsupported initially. Runtime stdout is never a transport.

HTTP rules are conservative:

- the base URL and endpoint are explicit configuration, never generated;
- user information in a URL is rejected;
- TLS certificate verification must remain enabled;
- the effective per-request timeout is at most five seconds;
- the effective total retry count is at most one;
- transport configuration and authorization never appear in an event,
  structured result, or normal diagnostic.

`OPENLINEAGE_DISABLED=true` conflicts with explicit `--emit-openlineage` and is
an error before execution begins.

## Failure policy

Static generation, validation, and output are fail-closed. Invalid output is
never emitted or written.

For runtime commands, event construction, offline validation, namespace
resolution, and transport configuration are completed before the command can
mutate runtime infrastructure or consume samples. Those preflight failures are
fatal.

After a command run starts, delivery is best effort. A file or HTTP delivery
failure produces a redacted, bounded `W112_OPENLINEAGE_EMIT_FAILED` warning but
does not change the apply/test result, trigger rollback, or replace the command's
real error. A terminal event can fail after successful work and cannot
truthfully undo that work.

There is no required-delivery mode in the first release. Such a mode requires a
durable outbox written before execution and acknowledged independently of the
business operation.

## Offline validation boundary

The implementation uses `jsonschema.Draft202012Validator` with a
`FormatChecker` and a closed offline `referencing.Registry` containing the
pinned resources. `Draft202012Validator.check_schema` must pass for every
vendored schema. An unresolved reference is an error; the registry has no
network retrieval fallback.

Every event is validated against the pinned core schema. Every standard facet
the exporter emits is also validated directly against its pinned facet `$defs`
schema. Core validation alone is insufficient because the core deliberately
allows arbitrary facet bodies through `BaseFacet`.

streamt additionally enforces semantics that the official JSON Schema does not
fully express:

- the root `schemaURL` fragment matches the event kind;
- generated objects contain only the fields permitted for that kind;
- `DatasetEvent` and `JobEvent` contain neither `run` nor `eventType`;
- generated `RunEvent` records have an explicit `eventType`;
- event, job, dataset, field, and namespace values are non-blank;
- standard facet keys match their exact `_schemaURL` and entity location;
- generated job and dataset identities are unique;
- field names and input/output identities are unique within their containers;
- finite runs have exactly one START and one terminal COMPLETE, FAIL, or ABORT
  event with identical run and job identity;
- event times are timezone-aware RFC 3339 values;
- run IDs pass UUID format validation.

The truthful validation claim is:

> streamt validates generated events offline against the pinned official
> OpenLineage 1.53.0 core schema and every standard facet schema it emits, plus
> streamt identity and lifecycle invariants.

The implementation must not claim universal OpenLineage semantic or backend
conformance.

## Secret and sensitive-data boundary

Event builders consume only the minimum declared metadata needed by this
specification. They must not traverse or serialize runtime configuration,
compiled SQL, deployment artifacts, reviewed plans, connector maps, HTTP
headers, environment variables, or authentication objects.

Kafka and Gateway host/port namespaces are intentional dataset identities.
User information and embedded credentials are forbidden. User-authored names
and descriptions are copied as metadata; diagnostic exceptions are separately
redacted and bounded.

Transport exceptions must not be serialized with an unrestricted `repr`.
Normal diagnostics should expose a stable warning, the exception class when
useful, and a sanitized message only.

## Explicit omissions and non-goals

The first release intentionally omits:

- automatic emission from ordinary compile or plan;
- deployed Flink, Gateway, Kafka Connect, or topic runtime lifecycle events;
- standalone events for tests, exposures, schemas, consumer groups, or
  deployment resources during static export;
- inferred schemas, serializer claims, Schema Registry lookup, and live-topic
  existence;
- SQL, macro source, source-code location, processing-engine, tags, access,
  classification, primary-key, lifecycle, data-quality, and metrics facets;
- connector-specific external sink datasets;
- current heuristic column lineage;
- the OpenLineage 1.53 explicit `lineage` facet until field lineage is proven;
- parent/root run propagation and externally supplied run context;
- OpenLineage ingestion, round-trip editing, deletion, or catalog sync;
- a server URL, API key, credential, or console transport default;
- durable delivery guarantees.

## Acceptance requirements

Static export is supportable only when tests prove:

1. all packaged schemas match the pinned size and checksum in an installed
   wheel and source tree;
2. no validation path performs network I/O;
3. generated events pass official core, used-facet, and local semantic
   validation;
4. every materialization and the sink omission boundary map as specified;
5. macro dependencies resolve from compiled SQL rather than disappearing;
6. single-endpoint derivation works and multi-broker ambiguity fails closed;
7. dataset, job, and field collisions fail deterministically;
8. fixed-time export is byte-for-byte stable under declaration reordering;
9. text JSONL, structured JSON, and atomic file behavior remain distinct;
10. secret-bearing runtime and connector fixtures do not leak into events or
    diagnostics;
11. strict documentation examples and the existing CLI output contract pass.

Runtime command telemetry additionally requires tests proving paired lifecycle
events, exact durable boundaries, sample-only dataset inputs, interrupt and
failure mapping, transport bounds, and the rule that emission failures never
change the underlying apply/test outcome.
