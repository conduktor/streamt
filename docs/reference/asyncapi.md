# AsyncAPI export

`streamt docs asyncapi` emits a deterministic AsyncAPI 3.1.0 JSON document for
the Kafka channels declared by a project. Sources become `receive` operations;
non-sink models become `send` operations. Sink models do not declare Kafka
output channels and are therefore excluded.

```bash
streamt docs asyncapi > asyncapi.json
```

The command does not connect to Kafka or Schema Registry. It intentionally does
not emit `servers`, credentials, content types, consumer groups, message keys,
or Schema Registry serialization settings because the streamt project does not
provide enough portable information to make those claims.

## Generated identities and schemas

Channel, message, schema, and operation identifiers are derived from the
logical source or model name. For a source named `orders`, the identifiers are:

- channel: `source.orders`
- message: `source.orders.message`
- payload schema: `source.orders.payload`, when columns are declared
- operation: `receive.source.orders`

Model identifiers use the `model` and `send.model` prefixes. Invalid or
colliding normalized identifiers fail generation instead of overwriting an
earlier declaration.

Source payload schemas come from `columns`. Model payload schemas prefer the
declared `contract.columns`, then `columns`. A model contract with
`enforced: true` produces `additionalProperties: false`. When a resource has no
declared columns or contract, its message remains in the document but no
payload schema is invented. A declared column without a type remains an
unconstrained JSON Schema property; streamt preserves the column metadata but
does not guess a wire type.

Scalar Flink SQL types, `ARRAY`, string-keyed `MAP`, and `ROW` are converted to
AsyncAPI Schema Objects. Unsupported, malformed, or representation-dependent
types such as `MULTISET` fail closed; streamt never falls back to `string`.

Kafka channel bindings use binding version 0.5.0 and are emitted only for
explicit model topic metadata supported by that binding: partitions,
replication factor, and recognized topic configuration fields. Unknown Kafka
configuration remains absent from the document rather than being reinterpreted.
Virtual-topic settings are not presented as physical Kafka topic settings.

## Validation boundary

Generation succeeds only after two offline validation passes:

1. The complete document validates with `jsonschema` against the official
   AsyncAPI 3.1.0 JSON Schema vendored from the
   [`asyncapi/spec-json-schemas`](https://github.com/asyncapi/spec-json-schemas/blob/61cc6add7cf3467f56d1fbb55b1a2b78b4ae6323/schemas/3.1.0-without-%24id.json)
   repository. The packaged schema is pinned by upstream commit and SHA-256.
2. A semantic pass verifies local channel, message, payload-schema, and
   operation references. It also checks the AsyncAPI 3 rule that an operation's
   messages are a unique subset of the referenced channel's messages.

This validates the exported document. It does not prove that a broker contains
the topics, that a deployed serializer matches the conceptual payload schema,
or that external AsyncAPI tooling supports every 3.1 feature.

## Structured output and compatibility alias

With the global `--output json` option, the AsyncAPI document is returned under
`data.document` together with channel, operation, and schema counts:

```bash
streamt --output json docs asyncapi
```

`streamt docs openapi` remains a deprecated compatibility alias. It emits the
same AsyncAPI 3.1 document and does not emit or claim to emit an OpenAPI
document. New scripts should use `docs asyncapi`.
