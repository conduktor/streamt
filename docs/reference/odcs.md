# Open Data Contract Standard export

`streamt docs odcs` emits one deterministic Open Data Contract Standard (ODCS)
3.1.0 document for one parsed streamt project. The contract contains every
declared source and model as a schema object. This is a portable metadata
export: it does not connect to Kafka, Schema Registry, Flink, Connect, Gateway,
deployment state, or a catalog.

Contract identity and lifecycle are explicit because streamt cannot truthfully
infer them:

```bash
streamt docs odcs \
  --contract-id urn:acme:data-contract:payments \
  --status active > payments.odcs.yaml
```

`--contract-id` and `--status` are required semantic inputs and are copied
exactly. Root `version` uses `--contract-version` when supplied, otherwise the
exact `project.version`; missing or blank metadata fails with
`E505_ODCS_INVALID`. streamt does not default identity, status, or version.

## Complete project example

This complete example is parsed by the same production parser used by the CLI
and is exercised against the bundled ODCS validator in the test suite:

```yaml
# streamt:odcs-example
apiVersion: streamt.dev/v1alpha1
project:
  name: payments-streams
  version: 2.3.0
  description: Payment processing project
runtime:
  kafka:
    bootstrap_servers: localhost:9092
sources:
  - name: payments_raw
    topic: payments.raw.v1
    description: Raw payment events
    tags: [payments, raw]
    columns:
      - name: payment_id
        type: VARCHAR(64)
        required: true
        classification: confidential
      - name: amount
        type: DECIMAL(12, 2)
models:
  - name: payments_clean
    materialized: topic
    topic:
      name: payments.clean.v1
    primary_key: [payment_id]
    contract:
      enforced: true
      columns:
        - name: payment_id
          type: STRING
          nullable: false
        - name: amount
          type: DECIMAL(12, 2)
          nullable: true
  - name: warehouse_archive
    materialized: sink
    from: payments_clean
    sink:
      connector: jdbc
      config:
        table: analytics.payments
    columns:
      - name: payment_id
        type: STRING
```

The resulting document has the exact root shape `apiVersion: v3.1.0`,
`kind: DataContract`, explicit `id`, project `name`, explicit or project
`version`, explicit `status`, and a non-empty `schema` array. It is one contract
for the project—not separate contracts for each model, topic, or environment.

## Mapped schema facts

Sources map their logical name, exact physical topic, descriptions, tags, and
declared columns. Models prefer `contract.columns` whenever a contract exists,
including an explicitly empty contract; otherwise they use explicit `columns`.
Kafka-output models map their explicit topic name or the established model-name
default. Sink models remain schema objects, but connector-specific destinations
are not presented as a normalized physical identity.

Properties can include the exact declared physical type, a conservative ODCS
logical type, description, classification, required/nullability semantics, and
declared model primary-key position. A model contract records its exact
`enforced` value as a streamt custom property. Unknown, complex, binary,
malformed, or out-of-range types retain `physicalType` and omit `logicalType`;
they never fall back to `string`.

Schema-object and property IDs are deterministic UUIDv5 values derived from
the explicit contract ID, resource kind, resource name, and property name.
Sources and models are canonically ordered. Duplicate logical names, duplicate
properties, shared physical topics, generated-ID collisions, missing primary
key properties, and conflicting classifications fail instead of being merged
or overwritten.

## Output behavior

YAML is the default raw serialization. JSON is explicit:

```bash
streamt docs odcs \
  --contract-id urn:acme:data-contract:payments \
  --status active \
  --format json
```

Without `--output-file`, text-mode stdout contains only the raw document.
Parser notices and incomplete-schema warnings are written to stderr, so stdout
is directly pipeable. A resource without declared columns remains in the
contract, omits `properties`, and reports `W109_ODCS_SCHEMA_INCOMPLETE`.

With `--output-file`, streamt validates and serializes before staging a file in
the target directory and atomically replacing the explicit target. A failure
does not emit a partial document. Global JSON mode retains the normal streamt
envelope and keeps the complete document as an object under `data.document`:

```bash
streamt --output json docs odcs \
  --contract-id urn:acme:data-contract:payments \
  --status active \
  --format yaml
```

The local `--format` value is reported in `data.serialization`; it does not
replace or nest a second raw document inside the global JSON envelope.

## Offline validation boundary

Every emitted document validates before serialization against the official
ODCS 3.1.0 Draft 2019-09 companion schema pinned to
[`b9d3ffc5aabe9e058afe4469cabe5a218fe9946d`](https://github.com/bitol-io/open-data-contract-standard/blob/b9d3ffc5aabe9e058afe4469cabe5a218fe9946d/schema/odcs-json-schema-v3.1.0.json).
The packaged bytes are integrity-checked by exact size and SHA-256. A second
semantic pass enforces textual-standard and streamt project-boundary rules that
the companion schema cannot prove, including a present, non-empty `schema`
array. Validation uses only the bundled local schema and never resolves network
references.

This proves the exported document shape and streamt's deterministic mapping. It
does not prove that topics or schemas exist at runtime, that a serializer
matches the conceptual contract, or that a catalog has accepted the document.

## Intentional omissions

The first supported boundary deliberately excludes:

- data tests, quality rules, failure actions, exposures, freshness, and SLAs;
- teams, roles, support contacts, price, ownership, and server definitions;
- runtime endpoints, credentials, registry subjects, deployment observations,
  SQL, lineage inference, and live schema enrichment;
- ODCS import, round-trip synchronization, and per-model or per-topic output;
- catalog publication to Conduktor Console, Backstage, DataHub, or another
  remote service;
- multiple ODCS versions, mutable `latest` schemas, and ODCS-specific project
  DSL fields.

These fields are omitted, not emitted as `null`, fabricated defaults, or vendor
extensions. See the [normative export contract](../specs/odcs-export.md) for the
field-by-field mapping and collision algorithm.
