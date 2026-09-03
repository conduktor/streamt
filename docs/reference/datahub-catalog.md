---
title: DataHub Catalog Export
description: Export deterministic simplified DataHub MCP metadata files offline
---

# DataHub catalog export

`streamt docs datahub` compiles one project offline and emits a deterministic
JSON metadata file containing simplified DataHub Metadata Change Proposals
(MCPs). The artifact targets the file format and generated types pinned from
DataHub `v1.7.0`; every proposal has `changeType` set to `UPSERT`.

This is a design-metadata export, not a DataHub connection or ingestion
command. It does not contact Kafka, Gateway, DataHub, a Generalized Metadata
Service (GMS), or any other network service, and it does not prove that an
entity, platform, or platform instance exists remotely.

A separate release gate does exercise the shipped artifact against a disposable,
exact DataHub v1.7.0 quickstart. Both Kafka identity variants are ingested twice;
all five emitted aspect types are read back exactly, and the five representative
direct `Consumes`/`Produces` Dataset relationships are verified after each
ingestion. This is pinned test evidence, not a production publisher or general
DataHub compatibility claim.

## Export a metadata file

Supply every catalog identity explicitly on the command line:

```bash
streamt docs datahub \
  --catalog-id payments-prod \
  --fabric PROD \
  --kafka-platform-instance main \
  --gateway-platform-id conduktor-gateway \
  --gateway-platform-instance edge \
  --output-file payments.datahub.json
```

The command surface is exactly:

| Option | Meaning |
| --- | --- |
| `--catalog-id ID` | Required exact DataFlow flow ID owned by the caller |
| `--fabric FABRIC` | Required exact uppercase DataHub v1.7.0 FabricType |
| `--kafka-platform-instance ID` | Optional Kafka ingestion `platform_instance` |
| `--gateway-platform-id ID` | Gateway DataPlatform ID; required with the Gateway instance when Gateway assets exist |
| `--gateway-platform-instance ID` | Gateway platform instance; required with the Gateway platform ID when Gateway assets exist |
| `--output-file PATH` | Atomically write canonical raw JSON instead of raw stdout |
| `--project-dir PATH`, `-p PATH` | Project directory; defaults to the current directory |
| `--env ENVIRONMENT`, `-e ENVIRONMENT` | Select the normal streamt environment |

The only accepted fabric values are:

```text
DEV TEST QA UAT EI PRE STG NON_PROD PROD CORP RVW PRD TST SIT SBX SANDBOX CERT
```

Catalog ID, fabric, and transport identities are CLI-only: streamt does not
read DataHub identity defaults from environment variables or project runtime
configuration. `--env` selects a streamt project environment; it never selects
or implies a DataHub fabric. `--catalog-id` and `--fabric` are validated before
the project is parsed, compiled, or written.

## Identity alignment

The single DataFlow uses orchestrator `streamt`, the exact catalog ID as its
flow ID, and the exact fabric as its cluster. Each actual compiled process uses
its exact streamt model name as a DataJob ID beneath that flow. IDs are not
trimmed, case-folded, slugged, or hashed. Invalid and overlong identities fail
closed, as do distinct declarations that resolve to the same final URN within
one export.

The pinned DataHub encoder escapes comma, parentheses, and U+241F, but it does
not escape literal percent text. Raw values such as `a,b` and `a%2Cb` can
therefore produce the same encoded component. streamt detects these aliases
within one export, but it cannot detect a collision with a separate file or a
remote catalog. Caller-owned identities must account for that cross-export
boundary.

Ordinary datasets always use the official `kafka` platform and the exact
physical topic name. `--kafka-platform-instance` is optional, but its presence
is identity-significant: with `main`, physical topic `payments.raw.v1` becomes
dataset name `main.payments.raw.v1` and receives a `dataPlatformInstance`
aspect. Without the option, the dataset name is `payments.raw.v1` and that
aspect is omitted. The choice must exactly match the DataHub Kafka ingestion
source's `platform_instance`; offline streamt cannot verify the source
configuration.

Gateway virtual-topic datasets require both Gateway options. DataHub v1.7.0
does not provide or bootstrap an official Conduktor Gateway platform, so the
operator owns these exact values. streamt neither invents nor creates a
DataPlatform or DataPlatformInstance. A project without Gateway assets does
not require the pair.

Changing fabric changes every entity URN. Catalog ID determines the DataFlow
and DataJob URNs but not Dataset URNs. Physical names and transport platform/
instance values determine Dataset URNs. Except for fabric, a raw identity
change can preserve the same final URN through the percent-text alias above.
Align all values with existing DataHub ingestion before combining metadata
files.

## Complete project example

The following complete project is parsed, compiled, and exported by the
production CLI in the test suite with network and subprocess access disabled:

```yaml
# streamt:datahub-example
apiVersion: streamt.dev/v1alpha1
project:
  name: payments-streams
  version: 2.3.0
  description: Payment processing catalog
runtime:
  kafka:
    bootstrap_servers: kafka.private.example:9092
    sasl_password: ${DATAHUB_DOC_KAFKA_PASSWORD}
  conduktor:
    gateway:
      proxy_bootstrap: gateway.private.example:6969
sources:
  - name: payments_raw
    topic: Payments.Raw.v1
    description: Raw payment events
    owner: payments-platform
    tags: [payments, restricted]
    columns:
      - name: payment_id
        type: STRING
        classification: confidential
models:
  - name: payments_clean
    materialized: topic
    description: Validated payment events
    owner: payments-platform
    tags: [payments, curated]
    sql: |
      SELECT payment_id
      FROM {{ source("payments_raw") }}
    topic:
      name: payments.clean.v1
    contract:
      enforced: true
      columns:
        - name: payment_id
          type: STRING
          nullable: false
  - name: payments_public
    materialized: virtual_topic
    description: Gateway payment view
    from: payments_clean
    sql: |
      SELECT payment_id
      FROM {{ ref("payments_clean") }}
    gateway:
      virtual_topic:
        name: payments.public.v1
  - name: warehouse_archive
    materialized: sink
    description: Warehouse archive connector
    from: payments_clean
    sink:
      connector: jdbc-sink
      config:
        connection.url: jdbc:postgresql://warehouse.private.example/payments
        password: ${DATAHUB_DOC_CONNECTOR_PASSWORD}
exposures:
  - name: finance_dashboard
    type: dashboard
    description: Internal finance dashboard definition
    consumes:
      - ref: payments_clean
```

For this example, catalog ID `payments-prod`, fabric `PROD`, Kafka instance
`main`, and Gateway platform/instance `conduktor-gateway` / `edge` produce
native identities such as:

```text
urn:li:dataFlow:(streamt,payments-prod,PROD)
urn:li:dataset:(urn:li:dataPlatform:kafka,main.Payments.Raw.v1,PROD)
urn:li:dataset:(urn:li:dataPlatform:conduktor-gateway,edge.payments.public.v1,PROD)
urn:li:dataJob:(urn:li:dataFlow:(streamt,payments-prod,PROD),payments_clean)
```

## Entity mapping and direct lineage

| Compiled streamt fact | DataHub entity and aspects |
| --- | --- |
| Project plus catalog ID/fabric | One `dataFlow` with `dataFlowInfo` |
| Source or compiled topic output | `dataset` with `datasetProperties` and conditional `dataPlatformInstance` |
| Actual Flink, Gateway, or Connect process | `dataJob` with `dataJobInfo` and `dataJobInputOutput` |
| Process-free topic model | Dataset only; no invented DataJob |
| Connect sink | DataJob and input edges; no destination Dataset |
| Exposure | Omitted with a warning |

Each DataJob's input edges are exactly its direct compiled Dataset
dependencies. Output edges contain its one existing output Dataset; a Connect
sink has an empty output. The exporter does not add transitive,
process-to-process, inferred, exposure, or self-output edges. Both deprecated
DataHub dataset arrays remain empty while the preferred input/output edge
arrays carry these relationships.

The only contract metadata is `streamt.contract.status` in
`datasetProperties.customProperties`, with value `declared` or `enforced`.
This is descriptive streamt state, not a native DataHub DataContract,
assertion, schema, or proof of runtime enforcement.

## Canonical output, warnings, and errors

Without `--output-file`, text mode writes only the canonical UTF-8 top-level
MCP array to stdout: two-space indentation, sorted object keys, unescaped
Unicode, LF line endings, and one final newline. Warnings go to stderr. With an
output file, streamt validates and serializes the complete array before a
private same-directory write, flush, `fsync`, and atomic replacement; text
stdout stays empty. `--quiet` suppresses stdout and warning text while still
allowing an explicitly requested file write.

Global JSON mode uses the normal streamt envelope:

```bash
streamt --output json docs datahub \
  --catalog-id payments-prod \
  --fabric PROD \
  --kafka-platform-instance main
```

Its `data` object contains `standard`, pinned `release`, `api_version`, the
same ordered `proposals`, exact proposal/entity/aspect `counts`, and
`output_file`. Warnings appear once in the envelope and are not printed to
stderr. Identical inputs and streamt versions produce byte-identical raw JSON
and structured proposals.

Warnings report metadata that was deliberately omitted without echoing its
contents:

| Code | Once per | Meaning |
| --- | --- | --- |
| `W115_DATAHUB_SINK_OUTPUT_OMITTED` | Sink model | The DataJob remains, but no provider-specific destination Dataset is invented |
| `W116_DATAHUB_EXPOSURE_OMITTED` | Exposure occurrence | No exposure entity or relationship is emitted |
| `W117_DATAHUB_TAGS_OMITTED` | Declaration with tags | Tag mapping is deferred, regardless of tag count |
| `W118_DATAHUB_OWNER_OMITTED` | Declaration with an owner | Ownership mapping is deferred |

Invalid input, identity, mapping, collision, validation, serialization, or
file output fails without a partial artifact under
`E508_DATAHUB_INVALID`. Error messages are bounded and do not forward raw
configuration or SDK exceptions.

## Privacy and unsupported operations

Treat the generated file as sensitive. It intentionally contains catalog ID,
fabric, platform and instance IDs, project and logical names, physical topics
and aliases, descriptions, contract state, and direct topology. In structured
mode, it may also contain the explicit output path.

The artifact excludes Kafka and Gateway endpoints and credentials,
environment variables, SQL and macros, local paths, connector configuration
and destinations, schema and columns, classifications and policies, tests and
freshness data, tag values, owner labels, exposure bodies, deployment state,
plans, operations, recovery evidence, timestamps, run IDs, and host/user/
process data.

The production command and validator have no `acryl-datahub` runtime or
optional dependency and do not import the DataHub SDK. Exact
`acryl-datahub==1.7.0` is used only in isolated release-oracle tests for
constructor, wrapper, and metadata-file reader compatibility.

The command does not publish, ingest, read, patch, delete, soft-delete,
reconcile, synchronize, or roll back DataHub metadata. It has no URL, token,
profile, recipe, emitter, or GMS client. In particular, `datahub ingest mcps`
is a publishing command and is not an offline validation step for this
artifact; only the isolated release gate invokes it against disposable GMS.

Native DataHub ownership, GlobalTags, DataContracts, assertions, schema,
columns, field-level lineage, destinations, exposures, domains, glossary
terms, containers, status, subtypes, and live synchronization are unsupported.
They are omitted rather than fabricated. See the
[offline specification](../specs/datahub-catalog-export.md) and
[GMS acceptance specification](../specs/datahub-gms-v170-acceptance.md) for the
normative boundaries. The corresponding records are the
[offline implementation plan](../plans/2026-09-03-datahub-catalog-export.md)
and [GMS acceptance plan](../plans/2026-09-03-datahub-gms-v170-acceptance.md).
